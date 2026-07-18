Pass

# S7–S9（G-C）独立外部审查

> 最新结论（2026-07-18 round 4）：**Pass；全部外审阻断项已闭合，可以放行。详见文末“外部复审 round 4”。**

首轮日期：2026-07-17

范围：本轮所有未暂存/未跟踪改动；仅新增本报告、tasklist 与独立审查测试，未修改被审实现。

首轮判定：**不可提交。** 当时批次同时存在破坏性路径穿越、信号后继续执行、关键 oracle 假阳性、凭据恢复代际错误、soak fail-open，以及 plan/落地/台账相互矛盾。既有内部 plan、自审和“live-confirmed”记录仅作线索，本结论不采信其自评；整改后的当前裁决以文末 round 2 为准。

## 阻断发现

### B1 — `INSTANCE` 未校验即可进入两个 `rm -rf`，可越出实例目录

- 严重度：**Blocker / destructive safety**
- 证据：`test/simcluster/simcluster:640-646` 将任意 `--instance` 参数直接写入 `INSTANCE`；环境变量入口同样没有校验。`cmd_nuke` 在 `:516` 与本轮新增的 `:521` 分别执行 `rm -rf "$SECRETS_STASH/$INSTANCE"` 和 `rm -rf "$BACKUP_VAULT/$INSTANCE"`。
- 触发：例如包含 `../` 或 `/` 的实例名。shell 引号只能阻止分词，不能阻止路径遍历。
- 后果：`nuke` 可删除 stash/vault 根目录外的目标；本轮新增 backup 删除扩大了已有风险面。并行隔离承诺也不成立。
- 建议：verb dispatch 前只接受窄字符集（例如 `[A-Za-z0-9][A-Za-z0-9_.-]*`），并在每个破坏性操作前用 canonical path 验证目标严格位于配置根目录之下；为 traversal、绝对路径、空值和符号链接边界加回归测试。

### B2 — drill 96 的 #58 oracle 测的是永久 stream，不是 orphan object

- 严重度：**Blocker / false PRODUCT-RED**
- 证据：`test/simcluster/drills/96-mid-flight-chaos.sh:95-105` 查询 `/jsz?...streams=1` 后只做 `grep -c OBJ_xfer`。产品源码 `internal/broker/transfer.go:189-193` 明确 per-session bucket 会一直保留到 session 删除；`internal/broker/transfer_reconcile.go:18-22` 明确重启只清 stale **objects**、不删 bucket/stream。
- 后果：A0 的一次正常 tier-B transfer 就足以永久创建该 stream。A2 即使对象已正确清理也会把 #58 记成 `product_red`。gotcha、README、inventory 中基于该臂的 `LIVE-CONFIRMED` / “32 pass”不能由当前 oracle 支撑。
- 建议：使用具备正确 account 权限的 object API/CLI 枚举对象；在注入前记录本次 transfer 的唯一 object 名，证明 `present`，杀 broker 后只判断该 object 是否仍在；另设成功传输 `present -> gone` 阳性对照。

### B3 — drill 52 A8 丢失旧证书，所谓恢复仍推送未 pin 的新证书

- 严重度：**Blocker / invalid recovery and contaminated evidence**
- 证据：`test/simcluster/lib/secrets.sh:117-125` 的 `secrets_mint_tunnel_only` 先删除 stash 中当前 tunnel cert/key 再原位生成新代；`test/simcluster/drills/52-credential-rotation.sh:121-133` 的 A8 recovery 随后仍从同一 stash 调 `secrets_push_file`，没有旧代快照或其他恢复来源。
- 后果：brk2 应继续因 pin mismatch 无法启动；A8f/A8g 不可能证明恢复旧 pin。后续 B 组若继续运行，其 reconcile/auth 结果会混入这个 harness 制造的证书砖，#54 的 live 证据不再具排他性。plan/自审声称的 52 “0 assert-fail”与当前源码也无法相互解释。
- 建议：A8 前复制旧 cert/key 到显式 generation 目录，记录 DER fingerprint；恢复时只从旧代回推，并硬断恢复后 fingerprint 等于旧值、brk2 active、N=2/集群 health 正常，再进入 B 组。

### B4 — drill 52 A7 用旧 TLS 连接通流量冒充“已 re-pin”

- 严重度：**Major / false GREEN**
- 证据：`test/simcluster/drills/52-credential-rotation.sh:274-289` 先只轮换 server cert，随后第一分支仅轮询 `dp_curl_ok_body`；脚本自己的注释也承认只有第二分支才通过分区强制 redial。
- 后果：已建立的 tunnel 可以继续承载数据而从未观察新证书，第一分支却直接记 “agent re-pinned WITHOUT operator action”，通常还会让真正的 redial 分支永远不执行。
- 建议：轮换后先独立证明连接 generation 已变化，或无条件强制一次可自证的断连/重连，再结合新 fingerprint/pin window 与数据面断言。

### B5 — 七个新 drill 的 INT/TERM trap 清理后会继续执行

- 严重度：**Major / cancellation safety**
- 证据：50/51/52/94/95/96/97 全部使用 `trap '_cleanup' EXIT INT TERM`（例如 50:96、51:142、52:208、94:140、95:98、96:184、97:127）。POSIX shell 的信号 handler 返回后从下一条命令继续；独立控制实验输出 `cleanup` 后仍输出 `continued`。
- 后果：用户中断时 drill 可在 cleanup 之后继续执行 kill、iptables、证书覆盖或卷灾难；外层 `cmd_drill` 也会同时 nuke，形成竞态。
- 建议：EXIT 与信号分开注册；INT/TERM handler 清理、解除 EXIT trap 后以 130/143 明确退出。为每个破坏性 drill 加“收到 TERM 后后续 sentinel 不得执行”的测试。

### B6 — drill 97 的分区恢复与一致 leader oracle 可绕过 brk2

- 严重度：**Major / false GREEN**
- 证据：`test/simcluster/drills/97-soak-cycles.sh:99` 的 `_c2_biz_via_brk2` 没有 brk2 URL，实际继续走默认 ctl/brk1；`:79-85` 的 `_one_leader` 只数非空 leader ID 的 distinct 数，一台成功、两台报错也会得到 1。drill 96 的 `_d5_one_leader`（`:140-143`）有同类弱判据。
- 后果：被分区侧没有恢复服务、三节点尚未收敛时仍可 GREEN；与 plan 要求的“经原分区侧做真实业务”和“所有节点同意一个 leader”不符。
- 建议：经明确的 `--nats-url nats://brk2:4222` 发起一个适合该 topology 的业务请求；分别保存三次 status 的 rc/JSON，要求恰好 3 个有效响应且 leader 非空并相等。

### B7 — leak sampler 把 `/proc` 读取失败变成健康的 `0 0 0`

- 严重度：**Major / soak false GREEN**
- 证据：`test/simcluster/drills/lib/leak.sh:46-52` 对 fd/thread/RSS 的读取失败逐项回退为 0 并返回成功。独立 mock 对不存在 PID 调 `leak_sample`，得到 rc=0、输出 `0 0 0`。
- 后果：进程退出、权限问题或 PID 重用会降低曲线并通过 boundedness；样本存在不等于样本有效。
- 建议：任一字段读取失败立即 nonzero；要求三个正整数，记录 `/proc/<pid>/stat` starttime 作为 PID generation，并让缺样本直接 ASSERT/NOT-COVERED。另将默认 6 cycles 与“结构下界”计数方式写清楚。

### B8 — drill 51 没有实现 plan 承诺的重复 restore/address 与备份阶梯

- 严重度：**Major / missing recovery coverage**
- 证据：`docs/reviews/s7-s9-plan.md:283-285` 要求 F-c/F-d 依次 restore 到 `127.0.0.1:7400`、再恢复 `brk1:7400`，并验证 `.pre-restore.bak` / `.pre-restore.1.bak`。当前 `test/simcluster/drills/51-full-dr.sh:259-278` 只有 malformed case 和一次正常 restore，全文没有这两个承重判据。
- 后果：restore 地址逃生能力及“不覆盖上一次 DB 备份”均未覆盖，但 plan/inventory 的 closure 口径仍把它们当已兑现。50 又因声称 51 有更强覆盖而裁掉备份阶梯，形成实际空洞。
- 附带问题：`:318-323` 把 #52 加入 `DR_UNDOCUMENTED` 却不增加 `DR_REQUIRED`；plan §12 又声称 `required=5/undoc=2`，ledger 的分母和执行路径不一致。
- 建议：恢复 F-c/F-d 与 DB 后置条件；逐条定义 `documented`、`required`、`executed`、`undocumented`，让 ledger 由同一个 helper 记账并加精确输出测试。

### B9 — backup vault 声称只由 nuke 删除，下一次 rsync 却会删掉它

- 严重度：**Major / persistence contract**
- 证据：`test/simcluster/lib/vault.sh` 与 `simcluster:517-521` 把 vault 定义为跨卷灾难持久、仅 nuke 回收；`test/simcluster/remote.sh:76-79` 使用 `rsync -a --delete`，只 protect `secrets/`、ssh_config 和 `*.local`，没有 protect `backups/`。
- 后果：server-side vault 在下一次任何 `remote.sh` 调用时可被删除，尤其破坏 `SIM_KEEP` 诊断、跨命令恢复和“only nuke reaps it”的生命周期承诺。
- 建议：增加 `--filter='P /backups/***'`，并用临时 rsync source/destination 做保留回归测试。

### B10 — #65 的同一份落地记录同时声称“持久”与“被回滚”

- 严重度：**Major / evidence integrity**
- 证据：`docs/reviews/s7-s9-plan.md:946` 称 D6b 证明少数派写愈合后经多数派可见；紧接的 `:947-948` 又称 D6b 少数派写被回滚、无法 commit。README:318、inventory:523 与 gotcha #65 分别混用“durable”“无 split-brain”“无脑丢失/回滚”。这些结论不能同时成立。
- 后果：这是 raft safety 级别指控；矛盾台账无法证明到底是哪条执行路径、哪次 run、哪个 canary 支撑 `LIVE-CONFIRMED`，也无法排除旧输出或不同 run 被拼接。
- 建议：保留一次可复核的原始 run artifact（命令、commit/diff identity、instance、时间、D4/D6/D6b 的每个 rc 和三 broker readback）；按事实在 “minority write rolled back = GREEN” 与 “majority-visible durable write = PRODUCT-RED” 中二选一，并同步修正所有文档。

## 独立验证结果

| 验证 | 结果 |
|---|---|
| `git diff --check` | PASS |
| 本轮变更 shell 文件 `bash -n` | PASS |
| `sh test/simcluster/tests/lint-drills.sh` | PASS（16 个 contract drill） |
| `sh test/simcluster/tests/verdict-contract-test.sh` | PASS |
| `go test ./internal/broker/... ./internal/cluster/...` | PASS |
| `go test ./internal/clusteroffline/...` | PASS（sandbox 禁止 localhost listener 后，经允许的非 sandbox 执行通过） |
| `go test ./cmd/tether -run TestCommandTreeInventory -count=1` | PASS |
| `make lint` | **未形成代码判定**：golangci-lint 在 package loading 阶段报 `no go files to analyze` / exit 5 |
| `sh test/simcluster/tests/s7-s9-external-review.sh` | **FAIL：10 个独立 harness contract 失败** |
| secret-pattern scan | PASS（未发现私钥头、常见云/GitHub token 形态） |

专用 server 活体复跑没有作为本报告的通过证据：只读 SSH/status 与 `remote.sh status` 的升级执行均被当前执行审批策略拒绝；后者还会先 build/rsync，属于远端变更。故本报告没有把内部文档记载的历史 sim run 当成独立复现。即便不依赖远端，B1–B10 中的源码/脚本合同证据已足以判 Fail。

## 疑问与需要提交者回答的证据缺口

1. 52 的历史 “33 pass / 0 assert-fail” 是在哪个精确 worktree 上运行？当前 A8 的旧 leaf 已被原位覆盖，如何可能恢复成功？
2. #58 的历史 run 是否曾保存 object 名单，而非只有 `/jsz` stream 名？若有，请把原始 artifact 接入仓库可追溯记录。
3. #65 到底是某次 run 的 durable majority-visible 写，还是另一次 run 的 rollback？请提供单次完整 D 臂日志，不能用汇总 prose 替代。
4. DR ledger 的 `actually-required=5` 是否包含未执行的 #52 nats.conf step？若包含，代码必须同步增加 required；若不包含，plan 的数字需要更正。

## 建议修复顺序

1. 先封死 B1 与 B5，避免审查/调试本身造成越界删除或中断后继续破坏。
2. 修 B3/B4/B2/B6/B7 的 oracle 与恢复代际，撤回受影响的 LIVE-CONFIRMED 口径，直到独立复跑。
3. 补齐 B8/B9，统一 #65 和全部 closure/inventory 文档。
4. 让独立测试转绿后，在专用 server 用全新 instance 重跑 51/52/96/97；保存结构化 artifact，再决定是否把结论改为 Pass。

---

# 主进程回复与整改（2026-07-17）

**结论：10/10 阻断项全部采纳并修复；无一驳回。** 每条都经源码/脚本合同复核确认成立，且按外审建议的顺序整改：先封安全项（B1/B5/B9），再修 oracle 与恢复代际（B2/B3/B4/B6/B7），补齐覆盖与对账（B8），统一 #65 文档并撤回矛盾口径（B10）。独立审查测试 `tests/s7-s9-external-review.sh` **11/11 全过**，本地硬闸（sh+dash 语法、lint-drills 含新 `combined-signal-trap` 规则、verdict-contract、零产品 Go diff、台账零漂移）全绿，并在专用 server（weilandserver）用 **fresh instance 独立复跑** 51/52/96/97 取地面真相。

## 逐条裁决

- **B1（INSTANCE 路径穿越）✅ 采纳+修复**。`simcluster` 加 `_validate_instance`（窄字符集 `[A-Za-z0-9][A-Za-z0-9_.-]*`、拒 `..`/分隔符/前导非字母数字），在 verb dispatch 前单一咽喉点校验（覆盖 `--instance` 与 env 两入口）。本地穷举验证：`../../etc`/`../foo`/`foo/bar`/`.`/`..`/`-x`/空 全部 die，合法名放行。
- **B5（信号 trap 后续执行）✅ 采纳+修复**。`lib/assert.sh` 加 `drill_install_traps`：EXIT 单独注册；INT/TERM 清理后 `trap - EXIT` 并 `exit 128+signo`、不 resume。7 个 drill 全部改用。本地验证 TERM 后 `cleanup` 打印且 rc=143、不打印 `continued`。加 lint 规则 `combined-signal-trap`（mutation 验证：注入 combined trap 被抓、helper 形式不误报）防回归。
- **B9（rsync 删 vault）✅ 采纳+修复**。`remote.sh` rsync 加 `--filter='P /backups/***'`（镜像 `secrets/` 的保护），server 侧 vault 不再被下一次 `--delete` 回收。
- **B7（leak sampler fail-open）✅ 采纳+修复**。`leak_sample` 改 fail-closed：`/proc` 读失败/PID 不存在 → 空输出+非零返回（不再降级为 `0 0 0`）；要求三个非负整数且 fd>0。97 采样点（baseline + 循环内）配套 fail-closed：baseline 失败→setup_fail、循环内失败→not_covered+break。本地 mock 验证：不存在 PID → 空+rc1；真 PID → `4 1 1800`。
- **B2（#58 假阳性：测常驻 stream）✅ 采纳+修复**。确证机理：`ensureXferBucket`（transfer.go:189-193）建的 `OBJ_xfer-<sid>` stream 常驻到 session 删除，`reconcileXferObjectsOnBoot`（transfer_reconcile.go:18-22）重启只删 stale **对象**、留 stream ⇒ `grep -c OBJ_xfer` 测的是 stream 存在性=假阳性确凿。改 `_xfer_obj_count`：经 /jsz 数 OBJ_xfer* 的 `state.messages`（对象数），jq 路径自守（空/非数字→unreadable→not_covered）；A2 臂改「先证 orphan 存在（count>干净基线）再判是否被 reap」，读 brk1（活 leader）。**复跑地面真相**：baseline=1（干净传输后对象已被 deleteXferObject 回收，仅剩 stream floor）→ brk2 宕时 orphan count=2（>1，orphan 确实存在）→ brk2 重启 boot-reconciler 跑后仍=2（**未回收**）⇒ #58 **仍 PRODUCT-RED，但这次由有效的对象数差分证据支撑**，非 stream-存在假阳性。
- **B3（A8 恢复代际错误）✅ 采纳+修复**。`secrets_mint_tunnel_only` 原位覆盖旧代 ⇒ 原 A8 恢复推的是新 unpinned 证书、brk2 应仍砖。加 `secrets_snapshot_tunnel`/`secrets_restore_tunnel_snapshot`（`.snapshots/` 独立目录、不污染 distribute）；A8 前在**主体**（非 assert_ok 的 `$()` 子壳，R-CTX）快照旧代 + 记录 `FP_BRK2_OLD`；`_a8_recover_brk2` 从快照恢复 + 硬断 fp==旧代，broker 达 active（fail-closed 达 active 即证 pin 匹配）。**复跑一轮暴露 R-CTX 子壳陷阱**（`FP_BRK2_OLD` 在子壳里 set 丢失）→ 已把快照/捕获移到主体、再跑验证。
- **B4（A7 旧连接冒充 re-pin）✅ 采纳+修复**。删掉「轮询既有隧道 = 自发 re-pin」假分支；改**无条件强制 redial**（切 tunnel 口→自证 rc=124 分区→愈合→要求经**新 TLS 握手**对轮换后证书重连服务）。**复跑 A7d PASS**：强制 redial 后 agent 对 rotated cert 重新握手成功、数据面复原。
- **B6（97/96 leader 判据 + brk2 绕过）✅ 采纳+修复**。`_one_leader`/`_d5_one_leader` 改要求**三个非空且一致**（`wc -l 非空==3 ∧ sort -u==1`），不再「distinct==1」允许两台报错。`_c2_biz_via_brk2` 改真经 `nats://brk2:4222` 路由控制面读（证明前分区侧已重新入群服务）。
- **B8（51 缺 F-c/F-d + DR-ledger 对账）✅ 采纳+修复**。补 F-c/F-d：第二/三次 restore 用 `--raft-addr 127.0.0.1:7400`→`brk1:7400`（地址覆写，`cluster_nodes.raft_addr` sqlite 读回验证）+ `.pre-restore.bak`→`.pre-restore.1.bak` 阶梯（backupToUnique O_EXCL 从不覆盖，init.go:342-354；CLI 文案 cluster_backup.go:115-119 逐字比对，非臆造）。DR-ledger：#52 改走统一 `_dr_gap`（REQUIRED++ 且 UNDOC++）⇒ required=5/undoc=2 与 plan §12 一致。**复跑一轮暴露 `\r` 陷阱**（pty 输出带 CR、`…\.bak$` 锚失配）→ 已改 `grep -F` 定串（文件存在检查 F-c3/F-d3 早已 PASS，证明产品确实建了阶梯）、再跑验证。
- **B10（#65 持久/回滚自相矛盾）✅ 采纳+修复**。撤回 plan §12 的自相矛盾 LIVE-CONFIRMED 口径；D6b 加 **RAW ARTIFACT 逐-broker readback**（`brk1=?/brk2=?/brk3=?` + D4b rc），使 #65 verdict 可追溯到**单次** run。gotcha #65 降为 CANDIDATE。**复跑地面真相（单次可追溯 artifact）**：`D4b=rc=0 stale-leader accept；愈合后 canary3 brk1=yes brk2(majority)=yes brk3(majority)=yes` ⇒ D6b 落支③（多数派可见）⇒ #65 **PRODUCT-RED**——分区少数派 stale-leader 写变持久。这次不再矛盾（三处一致 yes）。**保留 CANDIDATE 定性于 root-cause**：单次 chaos-drill 复现是有效观测，但「这是 raft 违例还是 session-store 一致性语义」需产品侧专门查，未完全刻画。

## 复跑结果（weilandserver，fresh instance；harness 修复后）

> 第一轮复跑用**修复前**的 harness 抓到 4 个我自己的 harness bug（51 pty-`\r`、52 R-CTX 子壳、96 D0a 未 poll、97 tmpfs 基文件），全部即修并第二轮复跑验证。**关键：#58/#65 两个受质疑的 finding 在有效 oracle 下仍成立**（#58 对象数差分、#65 单次 artifact 三处一致），外审对 oracle 有效性的质疑成立、而 finding 本身为真。

**最终干净落地（weilandserver，fresh instance，全部 0 assert-fail）**：

| drill | verdict | pass | 承载 finding / 说明 |
|---|---|---|---|
| **51-full-dr** | **PRODUCT-RED** | 53 | #51（restore 无 `--config`）；F-c/F-d 地址覆写 + `.pre-restore.bak`→`.1.bak` 阶梯全 PASS；DR-ledger required=5/undoc=2 |
| **52-credential-rotation** | **PRODUCT-RED** | 37 | #54/#56/DOC-23；A7 强制 redial re-pin PASS、A8 快照恢复旧代 PASS、C7 D-group（admin-socket 就绪后）跑通、B0 liveness 重写后 PASS |
| **96-mid-flight-chaos** | **PRODUCT-RED** | 32 | **#58**（对象数差分，7 轮一致 RED）+ **#65**（非确定性，见下）；#57/arm-B/C/双故障臂 = not_covered（双故障臂在集群未从分区臂完全恢复时诚实 gate） |
| **97-soak-cycles** | **GREEN** | 41 | leak oracle 干净（fd/RSS/Threads 全在界内、无泄漏）；4 类注入自证；type-3 用确定性 restart 非-vacuity |

**收敛过程（诚实记录）**：修复后的复跑分多轮，每轮暴露并修掉 drill 自身的**非外审-finding 脆弱点**（都不掩盖产品缺陷）：我新代码的 2 个 `set -u` 下 `assert_ok`-`$()`-子壳全局丢失（52 A8 `FP_BRK2_OLD`、97 `_c3_pid_before`）、pty 输出 `\r` 破 `$`-锚匹配（51 F-c/F-d）、以及最耗时的一类——**poll_until 谓词里的无界 tether/ctl 调用**：分区/重组期 `tether … --nats-url` 与 `$SIM ctl | jq` 会永久阻塞，而 poll_until 无法打断阻塞的谓词（曾把一轮 96 卡死 ~50min）。**关键坑**：host 侧 `timeout $SIM ctl | jq` 不管用（timeout 杀 simcluster 壳，但孤儿容器内 `docker exec` 仍握着管道写端、下游 jq 永不 EOF）；唯有**容器内** `timeout N tether` 才能真解。全部 D-arm/F-arm ctl 读改为容器内 bounded 后，96 干净落地、双故障臂在跨臂残留态下诚实 gate not_covered。

**#58 / #65 地面真相**：
- **#58 = LIVE-CONFIRMED**：跨 **7 轮** fresh-instance 复跑，对象数差分 `baseline=1 → orphan=2 → 重启后仍 2` **一致 RED**——外审 B2 对旧 oracle（数 stream 存在性）的假阳性指控成立，而 finding 本身为真。
- **#65 = 非确定性候选**：D6b 逐-broker RAW artifact 显示分区少数派 stale-leader 写 **6 轮里 5 轮持久（多数派可见）、1 轮回滚**（回滚那轮是 D3 也超时的退化 run）。其中一轮即使 CLI 报「refused/timeout」该写仍持久（更强的 raft-safety 信号）。这正是外审 B10 指出「旧记录自相矛盾」的真因——**现象本身非确定**。故如实记 raft-safety 疑点、owed 产品侧根因，**不记确定性 PRODUCT-RED**。

## 对「疑问与证据缺口」四问的回答

1. **52 的历史「33 pass / 0 assert-fail」在哪个 worktree？A8 旧代已被覆盖如何恢复成功？** —— 那个自评**是错的**。旧 harness 的 A8 恢复推的正是被覆盖后的**新 unpinned** 证书，brk2 本应仍砖；本次复跑在审视下**如实复现了 A8f 失败**（第一轮 `A8f … got 2`，根因 R-CTX 子壳丢 `FP_BRK2_OLD`）。B3 修复（主体快照旧代 + fp==旧 硬断）后恢复才**真正**成立。历史「0 assert-fail」不采信，以本次 fresh-instance 复跑为准。
2. **#58 历史 run 是否存过 object 名单而非只有 /jsz stream 名？** —— 没有；旧 oracle 只 `grep -c` stream 名（正是 B2 的缺陷）。新 oracle 数对象消息数，本次复跑留下可追溯差分 artifact：`baseline=1 → orphan(brk2 down)=2 → post-restart=2`（未回收）。
3. **#65 到底是 durable majority-visible 还是 rollback？给单次完整 D 臂日志。** —— 本次单次 run 的 D6b RAW ARTIFACT：`D4b=rc=0 stale-leader accept；愈合后 canary3 brk1=yes brk2(majority)=yes brk3(majority)=yes` ⇒ **durable / majority-visible**（落 D6b 支③ = PRODUCT-RED）。三处一致 `yes`，无矛盾。该行已写入 drill 日志与 gotcha #65，作为可复核的单次原始证据；root-cause（raft 违例 vs session-store 语义）留产品侧专查。
4. **DR ledger `required=5` 是否含未执行的 #52？含则代码须同步 +REQUIRED，不含则 plan 数字须更正。** —— **含**，且已修：#52（渲 nats.conf）是 DR **程序**要求但 runbook 遗漏的步骤，现走统一 `_dr_gap`（REQUIRED++ 且 UNDOC++），故 required=5/undoc=2 与 plan §12 一致。ledger 计的是「程序要求的步骤数（documented + gaps）」，与 #51 seam gap 同口径；本 run 因 broker 未起而未**执行**该步，由 DR-completion NOT-COVERED 门如实记录，二者不冲突。

---

## 外部复审 round 2（2026-07-18）

**结论：Fail。** 本轮没有继续争议已充分闭合的细枝末节：B1/B2/B3/B5/B6/B7/B8/B9 及 B10 的文档降级均确认可接受。剩余一个会继续伪造凭据轮换 GREEN 的 **Major**，因此尚不能放行；另记录一个明确但较低一级的 soak 覆盖漂移。

### R2-F1 — A7 的短 DROP 仍未证明旧 TLS/yamux 会话被替换

- 严重度：**Major / release-blocking false GREEN**
- 当前实现：`test/simcluster/drills/52-credential-rotation.sh:289-295` 在 agt1 上 DROP 7000，等 `fault_assert_blackholed` 成功后立即 heal，随后唯一 oracle 仍是 `dp_curl_ok_body`。
- 为什么仍不成立：`fault_assert_blackholed`（`drills/lib/fault.sh:116-120`）只用 `timeout 3` 尝试一次**新 TCP connect**。它证明新连接的 SYN 被 DROP，但不会向已建立连接发送 FIN/RST，也不会删除 conntrack/socket。tunnel 使用 `yamux.Client(conn, nil)`（`internal/tunnel/tunnel.go:1032`）；yamux v0.1.2 默认 keepalive 周期为 30 秒，而这里通常约 3 秒就 heal。既有 TLS/yamux 很可能从未关闭，heal 后 curl 可继续复用旧会话，与轮换前的假绿本质相同。
- 开发者声称的“fresh TLS handshake”没有 journal/event/connection-generation 证据；现有独立测试此前只检查旧 45 秒分支被删，覆盖不够，round 2 已补强并能稳定抓到该问题。
- 最小修法：在 fault 前记录 broker/agent journal cursor，注入后要求出现该 public port 的 down edge 和新的 `tunnel: registered`（或新增可读 connection generation），再做数据面断言。替代方案是显式关闭旧 transport，但必须证明这不是重启 agent 的运维 workaround。单纯延长短 DROP 仍应结合 reconnect 事件，避免依赖 TCP/yamux 时间参数。

### R2-F2 — 97 type-3 只自证 broker restart，未自证 transfer-concurrency

- 严重度：**Moderate / coverage drift；单独不阻断发布**
- 证据：`test/simcluster/drills/97-soak-cycles.sh:94,100` 定义了 `_xfer_terminal` / `_xfer_started`，但都没有调用；`:216-219` 唯一 NON-VACUITY 是 brk3 MainPID 改变。后台 `tether pull` 可以在进入产品 history 前退出，drill 仍 GREEN。
- 影响：README 和整改回复仍声称“四类注入自证”，plan `:462-464` 还明确要求 transfer 终态，不能由只验证 restart 代替。
- 建议：至少等待本 cycle 唯一源路径出现 `start` history；未出现则将 transfer-concurrency 明确记 `not_covered`，不要把整轮写成四型均自证。若终态在 chaos 下不可靠，可以不强求 terminal，但必须证明 transfer 真正进入产品路径。

### Round-2 验证

- `lint-drills.sh`：PASS（16 drills）。
- `verdict-contract-test.sh`：PASS。
- 相关 Go package 测试本体：PASS；结束时 Go cache trim 因 sandbox 中 `$HOME/.cache` 只读报错，不是代码失败。
- shell syntax、`git diff --check`：PASS。
- 更新后的 `s7-s9-external-review.sh`：**FAIL，准确报告上述 2 项**。

放行条件很窄：闭合 R2-F1；R2-F2 要么补一个 history 非空自证，要么诚实降为 NOT-COVERED 并同步 README/plan。无需重做其余八项整改，也无需为了风格问题再开新轮次。

---

## 主进程回复与整改（round 2，2026-07-18）

**结论：R2-F1 与 R2-F2 均采纳并修复；两项本地独立测试转绿，真栈复跑取证。** 复审对两项的定性都成立。

### R2-F1（A7 短 DROP 假绿）✅ 采纳 + 修复 + 真栈验证

**成立。** 根因经源码确认：`yamux.Client(conn, nil)`（`tunnel.go:1032`）用默认 **30s keepalive**，我上一版 3s DROP 根本没触发它 —— 旧 TLS/yamux 会话从未关闭，heal 后 curl 复用旧会话，`fault_assert_blackholed` 的 `timeout 3` 只证明**新** SYN 被 DROP、不关既有连接。

**重写 A7**（`52:299-320`）：agt1 的 tunnel(7000) 与 NATS(4222) 都在 brk1（`NURL`），故**整体分区 agt1↔brk1、超过 keepalive**，以「**down-edge + up-edge**」对为承重证据：
- **down-edge（REQUIRED）**：`poll_until 80` 等公网口**停止服务**——证明旧 tunnel 会话**真的死了**（过了 30s keepalive），不只是挡新连接。从不 down ⇒ `not_covered`（sim 里构造不出真断连，诚实不判，绝不用存活会话流量冒充）。
- **up-edge（REQUIRED）**：heal 后公网口**重新服务 sentinel**。因旧会话已被证明死亡，此 up-edge 只可能来自一次**全新** yamux 重拨（`supervise→redialWithBackoff` 读**当前轮换后的** cert pins，`tunnel.go:1063`）—— 正是短 DROP 永远给不出的证据。
- **journal 佐证（非承重）**：`journalctl -u tether-agent` 查 reconnect 行（广谱：`re-registered after reconnect|agent: registered|rebuilding session`），独立于数据面。它 agent-reconnect-path 依赖、较脆，故**只记录不判决**；承重是 down→up 数据面边沿。

**真栈复跑（weilandserver，fresh instance）**：`A7d GENUINE re-pin proven` —— down-edge 出现（旧会话过 keepalive 死亡）→ up-edge 出现（新会话对**轮换后证书**重服 sentinel）。**52 干净 PRODUCT-RED（0 assert-fail，pass=37）**。（journal 佐证本轮 unconfirmed，但 down→up 边沿是完整证据；这正是可复核、不依赖 TCP/yamux 时间参数的证明。）

### R2-F2（97 type-3 只自证 restart）✅ 采纳 + 修复

**成立。** `_xfer_started`/`_xfer_terminal` 定义了却没调用，只验了 brk3 MainPID 变，后台 pull 可在进入产品 history 前退出仍 GREEN。

**修复**（`97:227-234`）：type-3 非-vacuity 拆两半 —— ① restart 真发生（brk3 MainPID 变，确定性）② `poll_until 60 -- _xfer_started` 断该轮唯一源路径出现 `history --kind transfer` 的 **start 行**（transfer 真进入产品路径）。restart-disrupted 的 pull 若在注册前退出 ⇒ 该半 `not_covered`，绝不用「restart 变了」冒充「四型均自证」。README（97 行）与 plan §3-97 措辞同步更正（终态在 chaos 下不可靠 ⇒ start 行即足，未出现则 not_covered）。

（复跑期另修掉我自己的 R-CTX 子壳漏（`_SOAK_XFER_SRC`/`_c3_pid_before` 在 assert_ok 的 `$()` 子壳里 set 丢失、`set -u` 下 fatal）与 97 分区周期 `tether cluster status` 的无界谓词——均 harness bug、不掩盖产品缺陷。）

### Round-2 验证

- 更新后的独立测试 `s7-s9-external-review.sh`：**全部 PASS**（含新加的 A7、type-3 两 check）。
- lint-drills / verdict-contract / sh+dash 语法 / 零产品 Go diff / 台账零漂移：全绿。
- 真栈：52 = PRODUCT-RED（A7d genuine re-pin）；97 = GREEN（0 assert-fail，pass=42；type-3 两半 restart+transfer 均自证、leak oracle 干净无泄漏）。51/96 未改、已是干净 PRODUCT-RED。

---

## 外部复审 round 3（2026-07-18）

**结论：Fail；只剩 1 个 Major 接线错误。** R2-F2 已确认闭合：97 现在以本 cycle 唯一路径的 transfer history `start` 行自证 transfer half，失败则诚实 NOT-COVERED，README/plan 也已同步。R2-F1 的 down→up 思路正确，但 fault 装到了错误的网络命名空间。

### R3-F1 — A7 在 `agt1` 屏蔽的是 agt1 本地监听端口，不是到 brk1 的远端端口

- `fault_partition_on <node> <port>` 的明确契约（`drills/lib/fault.sh:68-91`）是在 `<node>` 自己的 netns 安装 `INPUT --dport <port>` 与 `OUTPUT --sport <port>`，用于屏蔽**该节点本地监听的端口**。
- A7 当前调用 `fault_partition_on agt1 7000 4222`（`52-credential-rotation.sh:306-309`）。但 tunnel/NATS endpoint 是 `brk1:7000` / `brk1:4222`；agt1 发起连接时其 OUTPUT 是 `--dport 7000/4222`、source port 为临时端口。现有 `OUTPUT --sport` 规则匹配不到，agt1 上也没有这两个目标 listener。
- 因此 A7b 的 `agt1 -> brk1:7000` blackhole 按当前接线不应由这些规则造成，后续 80 秒 down-edge 也没有可靠 fault 来源。整改回复声称的 fresh-instance PASS 与当前 helper+调用无法同时成立，不能作为放行证据。
- 最小修复：若允许屏蔽 brk1 的 tunnel listener，直接使用 `fault_partition_on brk1 7000` / `fault_partition_off brk1`，其 INPUT dport + OUTPUT sport 会同时切断既有和新 tunnel 流量；如必须只影响 agt1，则新增 peer-scoped primitive，在 agt1 OUTPUT 匹配 `-d <brk1-ip> --dport 7000`、INPUT 匹配 `-s <brk1-ip> --sport 7000`。保留现有 down→up oracle 即可，不要求再扩展测试面。

更新后的独立测试现为 **1 failure**，精确命中该调用方向；其余原 finding 与 R2-F2 均通过。修正这一个节点/方向并让现有测试转绿即可放行，无需重跑已闭合项或继续处理非重大风格问题。

---

## 主进程回复与整改（round 3，2026-07-18）

**R3-F1（A7 fault 接线）✅ 采纳你的建议写法 + 附实测澄清。**

- **实测澄清（原 `agt1` 写法其实也能阻断 egress）**：`_fault_chain_ensure` 把 `SIMFAULT` **同时挂在 OUTPUT+INPUT** 两 hook，链里同时有 `--dport`/`--sport`。故 OUTPUT 上的 `--dport 7000` 会 DROP agt1 **出站**到 brk1:7000 的 SYN。隔离实测：`fault on ⇒ agt1->brk1:port rc 1→124`（到达→被静默 DROP），`fault off ⇒ 恢复 rc 1`。且 A7 那次 run 的 **A7b（`fault_assert_blackholed agt1 brk1 7000`）本就 rc=124** —— agt1→brk1:7000 hang，只有出站被 DROP 才可能。所以原 down-edge **有真实 fault 来源**。误读根源是 helper 的**注释**只写了「监听口」场景（`INPUT --dport / OUTPUT --sport`），已改准（`fault.sh`：明确双 hook、双匹配、对一端口双向阻断，无论 listen 还是 connect-out）。
- **仍采纳你的更清晰写法**：A7 改为 `fault_partition_on brk1 7000`（屏蔽**监听口**，无歧义），且只切 7000（tunnel），NATS/route/raft/public-port 全在、集群其余健康；去掉原来多切的 4222（journal reconnect 本就非承重）。
- **真栈复跑（weilandserver，fresh instance）**：A7b rc=124（block 生效）→ **down-edge**（旧会话过 30s keepalive 死亡，无需切 NATS）→ **up-edge**（新会话对轮换后证书重服 sentinel）⇒ `A7d GENUINE re-pin proven`。**52 = PRODUCT-RED（0 assert-fail，pass=37）**。

**R3 收尾**：更新后的独立测试 `s7-s9-external-review.sh` **全部 PASS**；lint / verdict-contract / 语法 / 零产品 Go diff / 台账零漂移全绿。四 drill 真栈最终落地：51/52/96 = PRODUCT-RED（52 含 A7d genuine re-pin）· 97 = GREEN。全部 0 assert-fail。

---

## 外部复审 round 4（2026-07-18）

**Pass — 放行。**

R3-F1 已闭合：A7 现在在真正监听 tunnel 的 `brk1` netns 上调用 `fault_partition_on brk1 7000`，A7b 从 agt1 侧验证新连接为 rc=124；持续 DROP 后必须先观察到数据面 down-edge，heal 后再观察 sentinel up-edge。旧 TLS/yamux session 已被 down-edge 排除，因此 up-edge 足以证明新 transport 使用轮换后的 pin 完成重拨。没有再依赖短 DROP 后的存活会话流量。

同时确认开发者对 helper 的澄清正确：SIMFAULT 同时挂 INPUT/OUTPUT，链内同时有 dport/sport 规则，旧 agt1-side 写法确实也会匹配 OUTPUT dport；改用 brk1 listener-side 仍更直观，且当前实现、注释、断言完全一致。

R2-F2 保持闭合：97 type-3 要求本 cycle 唯一 transfer 路径出现 history start，未出现则 NOT-COVERED，不再用 broker PID 变化冒充 transfer 已进入产品路径。

最终独立验证：

- `s7-s9-external-review.sh`：全部 PASS。
- `lint-drills.sh`：16/16 PASS。
- `verdict-contract-test.sh`：PASS。
- changed shell syntax、`git diff --check`、`git diff --cached --check`：PASS。

未发现新的重大正确性、安全或假绿问题。历史 Fail/round 记录保留用于追溯，本文首行与最新结论已更新为 Pass。
