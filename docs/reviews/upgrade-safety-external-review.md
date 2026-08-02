Fail

# upgrade-safety 独立外部审查

日期：2026-08-01
审查者：独立外部审查者
基线：`HEAD=a31634814aff`

## 审查结论

**Fail，不建议合入或发布。** 当前暂存区外实现仍有 **1 个 Blocker、5 个 Major、1 个 Minor**。
最严重的问题是升级状态被绑定到共享二进制目录，却用单个 `Agent` 对象内的 mutex 和一个不带
sid/nid/upgrade-id 的全局 marker 管理。项目明确支持同一物理机运行多个 agent；在这个受支持拓扑下，
未启动新映像的兄弟进程可以替目标进程提交升级，多个进程也可以并发改写同一个 prev/marker/dst。
这会解除真正目标进程的回退保护，具备把 NAT 后节点永久留在坏版本上的路径。

此外，boot budget 在 Cobra 参数解析、session/YAML 读取和 logger 创建之后才运行，因此若新版本在这些
阶段失败，重启多少次都不会消耗预算或触发回退；`--all` 的 same-tag 路径会在没有 re-register 证据时
直接放行整个车队；baseline 读取失败会把旧 `node.list` 行误判为 COMMITTED；冒烟门不校验候选二进制
的 ProtoVersion；声称覆盖断电的文件事务也缺少必要的 metadata/目录 fsync。

本报告不采信内审的“已采纳/已修复”标签。内审材料仅作为反例索引；结论来自逐行实现审计、独立红测、
wire 变异、全量门禁和实际 simcluster 尝试。完整执行清单见
`docs/reviews/upgrade-safety-external-review-tasklist.md`。

## 审查边界

开始审查时 index 为空；工作树包含 29 个 tracked 修改、7 个 untracked 文件，tracked diff 约
`+1279/-131`，SHA-256 为
`55413bfe04d46bd48af061292106214e8f6299b06a8751446d266a1ab2209f76`。
审查覆盖 requirements/architecture/usage/broker-ops、agent 安装与状态机、ctl wait/fleet、broker/wire、
测试与发布门禁。审查者没有修改产品实现，只新增 tasklist、本文，以及 4 个失败即证明缺陷的回归用例。

## Findings

### F1 — Blocker：共享二进制使用 host-global 状态，却只有 process-local 所有权

位置：`docs/requirements.md:112`、`docs/usage.md:1667`、`internal/agent/agent.go:272-286`、
`internal/agent/upgrade_state.go:48-86,392-418,437-478`、`internal/agent/upgrade.go:77-89,493-554`。

requirements 把 node 定义为 agent 实例，并明确同一物理机可运行多个 agent。正常安装时这些进程共享
同一个 `tether` 路径，因此也共享 `<binary>.prev` 和目录级 `.tether-upgrade.json`。marker 没有 sid、
nid、目标实例或 upgrade nonce；`upgradeInstallMu`、`upgradeMu` 和 watchdog cancel 则只存在于各自
进程的 `Agent` 对象中。

失败路径一：目标进程 A flip 后启动新映像并把全局 `boot_count` 加到 1，但在 register 前失败。仍运行
旧映像的兄弟进程 B 恰好重连。B 对“路径上的文件”求 SHA，看到的是 NEW，而不是 B 当前运行 inode 的
OLD；全局 `boot_count>0` 也被当成 B 自己启动过新映像的证明。B 随即报告并写入 committed。A 的
watchdog 此后看到终态而 no-op，坏版本失去自动回退。

失败路径二：两个不同 nid 的 agent 同时收到升级请求。两个 process-local install mutex 都能成功，随后
交错执行 `Remove(prev) / Link-or-copy / write marker / Rename(dst)`；commit/watchdog 的 mutex 同样不能
跨进程串行化。最终 prev SHA、marker 和 dst 可以分别来自不同事务，回退收敛为
`rollback_failed`，甚至错误提交。

独立证据：`TestUpgradeCommitRequiresPerInstanceBootProofOnSharedBinary` 当前稳定失败：未启动新映像的
兄弟实例返回 `committed`。这不是测试构造出的非支持拓扑，而是 requirements/FAQ 明示支持的部署。

建议：二选一并写成强约束。

1. 每个 agent 实例使用独立、被强制验证的二进制/marker/prev 路径；或
2. 把升级建模为 host/install transaction：使用跨进程文件锁覆盖 install、commit、watchdog、rollback
   的完整读改写周期；marker 带 upgrade-id 和目标实例集合；`BootUpgradeCheck` 产出不可被兄弟进程借用
   的 process-local boot proof；提交必须证明本进程运行 inode/upgrade-id/实例身份匹配。并补多进程
   并发 install、目标 crash + sibling reconnect、不同 sid/nid 的真实进程测试。

### F2 — Major：boot 回退检查晚于会阻止 `RunE` 执行的启动步骤

位置：`cmd/tether/agent.go:261-309`、`cmd/tether/main.go:63-90`、
`docs/distributed-broker-architecture.md:863-879`。

`BootUpgradeCheck` 虽然位于 `agent.New/Run` 前，却已经晚于 Cobra 的命令/flag 解析，也晚于 `--session`
检查、strict YAML 解码、nid 解析和 logger 创建。以下新版本故障都会在调用它之前退出：删除/改名旧 unit
仍传入的 flag、无法解析 N-1 的 YAML、新增严格配置验证、logger flag/初始化回归。systemd 会重复启动
同一个坏二进制，但每次都停在相同前置错误，`boot_count` 永远不增加，预算 3 和 deadline watchdog 都
不会工作。

冒烟门只运行 `<candidate> version`，完全不覆盖 `tether agent <现网参数和配置>`。因此架构文档把
“早于启动检查”的窗口称为“极窄”并以静态 Go 二进制已过 exec 冒烟为依据并不成立；这里是普通 CLI/
配置兼容面，不是 runtime 极早崩溃窗口。

建议：在通用 Cobra 解析和可变配置之前增加最小、稳定的 agent boot shim，只在真实 agent daemon
启动时消费 marker；或让 supervisor 执行一个稳定的 upgrade bootstrap 子命令，再由它 exec 正常 agent。
新增黑盒测试：候选版本的 `version` 成功，但 agent 的旧 flag 或现存 YAML 被拒；重复启动 3 次必须恢复
prev。仅直接调用 `bootUpgradeCheck` helper 的单测不能证明生产入口顺序。

### F3 — Major：same-tag 金丝雀没有健康证明仍放行整个车队

位置：`cmd/tether/node.go:260-320,392-410`、`cmd/tether/node.go:150-172`、
`docs/usage.md:1189`。

代码和用户契约都说 `--all` 的第一台必须 staged **且重新 register** 后才触碰其余节点。但
`newVersion == startRelease` 时，`waitForUpgradeCommit` 打印“降级为 staged+smoke”后返回 nil；
`runUpgradeAll` 随即向所有剩余节点扇出。`version` 成功只证明候选能执行一个无配置子命令，不能证明
`Agent.Run`、连接、register 或 watchdog 正常。同 tag 重装又被注释称为标准恢复动作，因此不是可忽略
的怪异输入。

独立证据：`TestUpgradeAllSameTagCannotReleaseFleetWithoutCommitProof` 当前失败，观察到 3 次 dispatch，
预期只有 canary 1 次。

建议：在没有 registration generation/upgrade nonce 的当前 wire 下 fail closed；单节点可返回明确的
UNCONFIRMED 结果，`--all` 必须停止。长期以 additive 字段携带 upgrade-id/最近成功注册代数，让 same-tag
也有可验证提交点，不能用 stale release 字符串代替。

### F4 — Major：baseline 获取失败会把 stale ONLINE 行当作 COMMITTED

位置：`cmd/tether/node.go:217-232,383-478`。

`captureReleaseBaseline` 失败只打印 warning 并返回空字符串。旧 agent 的 `NewVersion` 会被规范化为空，
fallback 条件随后是 `entry.ReleaseVersion != startRelease`；于是任何旧的、非空 ONLINE 行在第一次 poll
就等于“release changed”，即使本次请求之后没有发生 register。对新 agent 也存在同类窗口：baseline
为空会绕过 same-tag guard，而 stale 行若已等于目标 tag，仍会立即满足精确相等分支。

独立证据：`TestWaitForUpgradeCommitMissingLegacyBaselineCannotCommitStaleRow` 当前失败，并输出
`release changed — upgrade COMMITTED`。

建议：需要 `--wait` 或 canary 时，baseline 获取失败必须在 dispatch 前返回错误；至少让函数返回
`(string, error)`，不得用空字符串同时表达“真实未知 release”和“lookup 失败”。更稳妥的终局仍是以
register generation/upgrade-id 判定，而不是比较容易陈旧的 node-list 值。

### F5 — Major：冒烟门接受不同 ProtoVersion 的候选产物

位置：`internal/agent/upgrade.go:650-676`、`docs/requirements.md:318-324`、
`docs/distributed-broker-architecture.md:850-852`。

`smokeVersion` 只要求首个非空行的前两个字段为 `tether <release>`，对 `(proto vN)` 完全不解析。
因此运行 v2 的 agent 会接受并安装输出 `tether v1.2.3 (proto v3)` 的 artifact。请求里的
`UpgradeReq.ProtoVersion` 只证明 ctl/broker/当前 agent 在同一纪元，不能证明下载字节属于该纪元。新进程
随后会被 v2 broker 精确拒绝，绕过“ProtoVersion bump 必须重装、不得 node upgrade”的硬契约。

独立证据：`TestSmokeVersionTable/different_proto_epoch` 当前失败，返回 `v1.2.3` 而不是错误。

建议：严格解析冻结格式 `tether <release> (proto v<N>)`，要求 N 等于当前
`proto.ProtoVersion`，并在改写 prev/marker/dst 前返回稳定的
`proto_bump_requires_reinstall`。保留 wrong/missing/malformed/future/previous epoch 的表驱动反例。

### F6 — Major：文件事务只有运行时原子性，没有所承诺的掉电持久性

位置：`internal/agent/upgrade.go:469-554,592-642`、
`internal/agent/upgrade_state.go:107-132,178-216,465-490`、
`docs/distributed-broker-architecture.md:848-858`。

候选文件在 mode=0600 时 `Sync`，随后 `Chmod(0755)` 后没有再次 fsync；marker tmp 虽然 sync 后 rename，
但父目录未 sync；prev 的 Remove/Link、候选到 dst 的 Rename、rollback Rename、终态 marker Rename/Remove
也都没有目录 fsync。`rename` 的可见性/原子性不等于断电后目录项与 metadata 已持久化。

可能结果包括：重启后新 dst 存在但 executable bit 未持久化；dst 已翻转而 pending marker/prev link 丢失；
rollback 的 dst 恢复或终态 marker 丢失。代码把 corrupt marker 当 idle，这会进一步把部分落盘状态变成
无回退的坏二进制。因计划/架构明确把“任意断点/断电”列为安全目标，这不是文档措辞上的小误差。

建议：定义并测试有序持久化协议：写候选→chmod→file fsync；建立/复制 prev→file/dir fsync；写并 rename
pending marker→dir fsync；rename dst→dir fsync；rollback 和 marker terminal/remove 同样 sync 目录。
用可注入 filesystem ops 做每一步掉电/错误矩阵，并明确哪些文件系统语义受支持。

### F7 — Minor：部署覆盖与终端观测文档陈述已失真

位置：`docs/reviews/simcluster-coverage-inventory.md:129,329,352,395`、
`docs/broker-ops.md:723-725`。

本轮修改的 coverage inventory 仍称成功路径和 `--wait` 被 gotcha #28 阻断；但
`test/simcluster/drills/31-node-upgrade-fleet.sh` 已明确写 #28 FIXED，并称成功路径另属 dedicated drill。
当前 drills 中又不存在该 dedicated upgrade-success drill。账本因此同时错报阻断原因和 coverage owner。

broker-ops 还说 ctl 会“直接”打印 ROLLED BACK；当前 ctl 不消费 broker 持久 upgrade state，只在轮询
超时时打印 `likely ROLLED BACK`。这两种语义对自动化和事故诊断差别很大。

建议：把 inventory/README/31 drill 三方统一为真实状态：#28 已修，控制面覆盖存在，真
re-exec/register/rollback deploy-tier 仍 NOT-COVERED 且尚无 owner；在 dedicated drill 落地前不要宣称
已归属。ops 文档改为“COMMITTED 基于 release 轮询推断；超时仅提示 likely rollback；权威状态查 agent/
broker log”，或交付可查询的持久状态。

## 疑惑与建议

1. 当前 `node upgrade` 的目标是 nid，但被替换的是安装目录中的共享 executable。产品需要明确：同宿主
   多 agent 到底是“一个升级域”还是“多个升级域”。在这个问题定稿前，任何 per-node canary 语义都不完整。
2. `smokeVersion` 使用 `exec.CommandContext(...).Output()`，stdout 无上限，且没有设置 `WaitDelay` 或
   进程组清理。候选若大量输出会耗尽内存；候选派生进程继承 pipe 时，5s context 未必能及时让
   `Output` 返回。建议加输出 cap、`WaitDelay`/进程组终止和相应反例测试。
3. wire inventory 的独立 tag 变异确实失败，说明 append-only 账本能抓住已登记字段的删除/改 tag；但它
   仍只覆盖 `internal/proto`，不会机械检查零值语义或包外 JSON。这个边界在架构中已诚实写出，可接受，
   但 release review 不能把“inventory green”等同于“N-1 行为已证明”。
4. `31-node-upgrade-fleet` 因服务器 `/etc/resolv.conf` 不满足 DNS fidelity 前置门而 SETUP-FAIL。
   该 drill 含 dead-node 断言，按 simcluster mandate 不能用 `SIM_ALLOW_FAKE_DNS=1` 绕过。建议修服务器
   resolver 后重跑，并新增真正拥有 PID/version/re-register/rollback oracle 的成功/失败 drill。

## 验证结果

通过：

- `git diff --check`
- wire 独立变异：临时把 `NodeRegisterReq.UpgradeState` 的 JSON tag 改名，
  `TestWireFieldInventoryAppendOnly` 按预期变红；原样恢复后 proto 测试通过
- 受影响包中除 4 个 reviewer 反例外均通过：agent、ctl、broker、proto、p10、security
- `go test -race ./internal/agent ./cmd/tether -count=1`：无 race detector 报告；仅 4 个 reviewer 反例失败
- `make e2e-parallel`：PASS，15/15 coverage、99 units、总耗时约 3m25s
- `make lint`：PASS，0 issues；受限环境只产生默认 lint cache 不可写 warning
- simcluster 演练前后 `status` 均为空，无容器残留

按预期失败并证明本报告问题：

- `TestUpgradeCommitRequiresPerInstanceBootProofOnSharedBinary` → F1
- `TestUpgradeAllSameTagCannotReleaseFleetWithoutCommitProof` → F3
- `TestWaitForUpgradeCommitMissingLegacyBaselineCannotCommitStaleRow` → F4
- `TestSmokeVersionTable/different_proto_epoch` → F5
- `make test`：仅上述 4 个 reviewer tests 失败，其余全量包通过
- `make gates`：前置 vet-tags、Darwin build、architecture、determinism、auth、concurrency、proto 均通过，
  到普通测试阶段因上述 reviewer tests 停止
- `31-node-upgrade-fleet`：**SETUP-FAIL，不是产品 verdict**；DNS fidelity gate 失败，0 个 product failure，
  未使用 fake-DNS bypass

## 放行条件

至少修复 F1-F6，并让 4 个独立反例、`make gates`、`make test`、`make e2e-parallel` 和 lint 全绿；
F1 必须在真实多 agent/同 binary 的进程级测试中闭合，F2 必须由真实 CLI 启动失败测试证明，F6 必须有
可注入断电顺序测试。修复 sim 服务器 DNS 后补跑相关 deploy-tier drill，并同步 F7 台账，方可复审。

---

## 主进程逐条回复（2026-08-01，修复轮）

> 结论先行：**F1–F7 全部采纳并已修复**；4 个外审反例全部翻绿，另按各 finding 的"建议"补齐了
> 对应的结构性测试。逐条如下（编号后是处置与落点）。

**F1（Blocker）— 采纳，按建议方案 2（host/install transaction）实施。** 四件套落地：
① **跨进程 flock**（`<二进制目录>/.tether-upgrade.lock`）：install 全段非阻塞持有（loser 回
`upgrade_in_progress`），boot/commit/watchdog/exec-failure 四个 RMW 短临界区阻塞持有（进程死亡自动释放）；
② **marker 带目标实例**（`target_sid/target_nid/upgrade_id`）：commit/上报/watchdog 武装都要求目标匹配；
③ **process-local boot proof**：提交证明从"磁盘路径 sha"改为 **`/proc/self/exe` 运行映像 sha**——
flip 后磁盘路径对全宿主都是 NewSHA，但只有真正 re-exec 过的进程运行着它，兄弟进程无法借用
（外审失败路径一的根因）；叠加既有 `boot_count>0` 门；
④ **升级域裁决**（回应"疑惑 1"）：同宿主共享二进制 = **一个升级域**，一次一个在途升级，
提交/回退只由被点名的 nid 驱动（写入 architecture §21.3 与 broker-ops §8.7）。
测试：外审反例 `TestUpgradeCommitRequiresPerInstanceBootProofOnSharedBinary` 翻绿；新增失败路径二回归
`TestUpgradeConcurrentInstallAcrossAgentsRejected`（两个不同 nid 的 Agent 实例共享二进制并发 install，
in-process 锁互不可见，flock 是唯一拦截者；变异删 flock 实测 `2 OK/0 busy` 红）。
关于"真实多进程"：反例与回归都用同进程内多 Agent 实例 + 独立 fd 的 flock（flock 按 fd 排斥，
语义与跨进程一致）；真多进程版本由 F2 的黑盒测试（真实二进制子进程）部分覆盖，完整的
multi-process drill 归入 F7 的 dedicated deploy-tier drill 立项。

**F2（Major）— 采纳。** boot 检查移至 `main()` **Cobra 解析之前**（`isAgentDaemonInvocation`
按 argv 形状识别守护调用，help/install/uninstall 排除；RunE 内原调用点删除，防双计数）。
黑盒钉子 `cmd/tether/agent_boot_shim_test.go`：`go build` 真实二进制 + marker/prev 沙箱 +
必被 Cobra 拒绝的 flag 连续启动 ×4，断言预算逐次消耗（1/2/3）并在耗尽时 dst 恢复为 prev、
marker=rolled_back——正是"候选 version 成功但 agent 启动失败"的类别。architecture §21.3 的
"极窄窗口"论断按外审订正（shim 前置**之前**该说法不成立，已如实改写）。

**F3（Major）— 采纳（推翻本进程内审轮 S4 的"警告放行"裁决）。** same-tag 无提交证明一律
fail-closed：`waitForUpgradeCommit` 返回 **UNCONFIRMED**（ExitError, exit 64），单节点非零退出并
指向日志，`--all` 视为金丝雀失败、其余节点不动。外审反例
`TestUpgradeAllSameTagCannotReleaseFleetWithoutCommitProof` 翻绿（canary 1 次 dispatch 后 abort）；
本进程原 S4 测试改写为 `TestWaitForUpgradeCommitSameTagFailsClosedAsUnconfirmed`。
长期修（wire 级 registration generation / upgrade-id 回传）记入 §21.3 待办与本报告存档。

**F4（Major）— 采纳。** `captureReleaseBaseline` 改为 `(string, error)`：需要确认（--wait/金丝雀）
时 baseline 失败**拒绝 dispatch**（错误信息给出 `--wait=false` 逃生门）；legacy fallback 判据加
双非空守卫（`startRelease != "" && entry.ReleaseVersion != ""`），空 baseline 永远走不到
"release changed"。外审反例 `TestWaitForUpgradeCommitMissingLegacyBaselineCannotCommitStaleRow` 翻绿。
代价如实记录：pre-release-report 时代的节点行（release 恒空）在 fallback 下只能等到超时的
likely-ROLLED-BACK——fail-closed 方向，接受。

**F5（Major）— 采纳。** `smokeVersion` 严格解析冻结格式 `tether <release> (proto v<N>)` 并要求
N == 本 agent 纪元（`proto.SubjectVersionToken`），不符 → `proto_bump_requires_reinstall`
（磁盘零变更、`--all` config-abort 类）；wrong/missing/malformed/cross-epoch 反例入
`TestSmokeVersionTable`（外审加的 `different_proto_epoch` 臂翻绿）。同时落"疑惑 2"加固：
stdout 64KiB cap + `WaitDelay=2s`。

**F6（Major）— 采纳。** 有序持久化协议落地：候选文件**同 fd** chmod→fsync（executable bit 持久化）；
prev 槽建立后目录 fsync；marker tmp fsync → rename → 目录 fsync；dst flip 后目录 fsync；
rollback 恢复 rename 与终态转换补目录 fsync；install 路径 sync 失败 → `install_failed` fail-closed。
注入点 `upgradeSyncObserver` + `upgrade_durability_test.go`：顺序断言（file→dir(prev)→file(marker)→
dir(marker)→dir(flip)）与 sync 失败注入断言。支持语义边界（POSIX fsync 的目录项持久化）写入 §21.3。

**F7（Minor）— 采纳。** coverage inventory 行改为真实状态（#28 已修〔R15〕；success/`--wait` 的
deploy-tier 臂 NOT-COVERED **且无 owner**，待 dedicated drill 立项）；broker-ops §8.7 改为
"COMMITTED 基于 release 轮询**推断**、超时仅 likely、权威状态查 agent/broker 日志"，并补
same-tag 与升级域两条运维语义；usage.md §5.19 同步三条硬语义。

**疑惑 1/2/3/4 回应**：①升级域已裁决（一个域，见 F1④）；②已修（见 F5）；③同意——
"inventory green ≠ N-1 行为已证明"，账本只是机械半，行为半靠条文+审查，§21.2 已写明边界；
④DNS fidelity 与 dedicated success drill：`/etc/resolv.conf` 属服务器系统状态，本轮不擅动
（simcluster 铁律之外的宿主变更需要用户确认）；修复 resolver 与新 drill 的立项一并列入
复审前置清单，由用户裁决执行时机。

### 复审轮（F8–F13，外审者直接修复）的主进程审查

复审判 Fail（2 Major + 4 Minor，`upgrade-safety-external-rereview.md`）后外审者**直接修改了实现**并出
最终 Pass（`upgrade-safety-external-final-review.md`）。主进程逐块审查了该 unstaged 修复层，**全部合格、
无改动**，要点：F8 的锁序修复（install 只持 flock，marker 文件读写靠 rename 原子语义）环消除成立，
子进程反例 `upgrade_lock_order_test.go` 用 4s 超时隔离验证；F9 用 128-bit `crypto/rand` UpgradeID +
pre-Cobra boot proof 补上了我方案里 darwin 路径 sha 可借用的洞（四件套证明），persist 失败→无 proof→
watchdog 回退是安全方向；F11 的 UpgradeID 精确匹配正确区分了 tmp 写失败（marker 属上一笔，不可删）与
rename 后 dir-fsync 失败（本笔，须删）；F12 在锁不可得时保活唯一旧进程（对 ReExecOnly exit-nonzero
合同的偏离仅发生在锁异常路径，保守可辩护）；F13 三方台账对齐无追溯篡改（M4 行以"当时状态"注记 +
指向权威行）。

**验证**（修复轮，全部以单独退出码取数）：外审 4 反例 + F2 黑盒（`TestAgentBootShimConsumesBudgetOnPreRunEFailures`）
+ F6 顺序对（`TestInstallSyncProtocolOrder` / `TestInstallSyncFailureFailsClosed`）+ 跨 Agent flock 回归
（`TestUpgradeConcurrentInstallAcrossAgentsRejected`）全绿；新增守卫变异实测（删 flock → `2 OK/0 busy` 红；
epoch/baseline/same-tag/运行映像由外审反例自身作载体）。终验：**`make gates`=0（lint 0 issues）、
`make test`=0、`make e2e-parallel`=0（ALL PASS）**。修复轮附带两处账本变更（提交时在 commit message
说明）：结构预算 `Agent` 方法数 115→118（F1 新增 runningImageSHA / markerTargetsThisAgent /
handleUpgradeInstallLocked）；`//nolint` 计数 30→34（4 条 nilerr，flock 闭包语义，line2-plan §14.1 已枚举）。
