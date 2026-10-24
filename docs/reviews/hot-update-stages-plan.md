# 全栈热更新 · stage 分治计划（主进程定稿）

日期：2026-09-20。状态：**定稿；作为 [hot-update-plan.md](hot-update-plan.md) 的执行分解，不替代它。** 冲突时以 hot-update-plan 的合同为准，本文只决定"先做哪块、做到哪算一块"。

## §-1 主进程定稿裁决

本文由主进程在 4 路草拟（runtime / transport / persistence / orchestration）→ 4 路交叉批评 → 1 路综合的 workflow 产物上定稿
（原始产物归档于仓库外证据目录 `~/tether-hot-update-handoff-20260920T2356Z/macro-plan/`）。综合稿引用的代码事实
（`reexec.go` leader 门与 single-mode 未接线、`fsm.go` poison-skip、`datadirlock.go` OFD 语义、`dataplane_lifetime_test.go` 的 ssproxy 无 ctx 规则、
`main.go` BootUpgradeCheck 在 Cobra 前、`handoff.Restored` 只有 TakeFile/Close、`install.sh` caddy.service 为 root + `Restart=on-failure`、
`cluster-runbook.md §3a` standalone JS 不可并入 clustered meta、`workload.OutputState` 的 FIN/ReadError 分离、`ctl_connect.go` 单次连接、
`broker/exec.go` 转发重编码 JSON）已由综合者逐条复核；主进程抽查了 reexec.go / fsm.go / ctl_connect.go 三处，属实。

**工作模式（用户 2026-09-20 授权）**：每个 stage 独立跑 plan workflow → 主进程定稿 `docs/reviews/hot-update-<slug>-plan.md` →
主进程实现 → 内审 workflow → 主进程修复 → `git add -A`。**stage 内无外审**；stage 只是推进方式，不具流程效力。
全部 stage 完成后做最终内审并停在外审门；不 commit、不 push、不部署现网。用户已两次裁定**维持全范围**。

**§4 六条 open question 的裁决**（综合稿把它们交主进程；下面是定论，各 stage plan 不再重开）：

| OQ | 裁决 | 理由 |
|---|---|---|
| 1 · N=1 broker 的产品定义 | **(b)：single mode 与 1-voter 集群模式都必须可热更新，single mode 是主 lane。** S6 升为 XL，drill 37 三 lane（single / 1-voter / N=3）。 | 现网唯一 broker racknerd 就是 **纯 single mode**（`cluster status` → "cluster mode not enabled"，2026-08-11 重建后无 raft 层）。只交付 1-voter 集群等于让真实部署永远拿不到热更新——这是缩范围，不是取舍。 |
| 2 · N=1 NATS | **主路径 = `--signal stop` + `Restart=always RestartSec=2` 原地重启（不用 LDM）+ S1/S5/S7 续接层吸收，以 10/s 注入实测 ≤5s、传输 10s 进展为闸。** 临时 sibling / JS R1→R2 只在实测超预算时作为增补，且须先 spike。 | plan §3.5 允许「临时冗余**或**可验证的本机交接路径」二选一；`cluster-runbook.md §3a` 与 `test/d9 GrowInPlaceOrphansStreams` 已证 standalone JS store 不能就地并入 clustered meta，sibling 在源码事实上既省不掉重启也搬不走 R1 store。这是在 plan 两个许可选项间按证据选择，不是缩范围；两条路径都不得用 deferred 冲抵 N=1 成功臂。 |
| 3 · Caddy 特权面与入口预热 | **采纳**：install.sh 改 `caddy.service` 为 `Restart=always` + `User=tether` + `AmbientCapabilities=CAP_NET_BIND_SERVICE`（带存量 retrofit）；配置 reload 走 localhost admin API；binary 换代前用产品管理的临时第二入口预热（SO_REUSEPORT 同址 vs alt-port transient unit 在 S9 plan spike 后二选一）。 | tether 从不驱动 systemctl；root unit + `on-failure` 下 graceful stop 不会被拉起，这是 install.sh 层面必须改的现状。 |
| 4 · ctl 进程退出/重启后按同一 handle 恢复 | **非目标，写入 requirements §6.8 作为明确边界**：§6.8 承诺的是升级不迫使用户重启/重连/重提交；用户自己杀掉 ctl 进程不是升级事件。句柄在 ctl 进程生命周期内内存保留；失败信息附 handle 仅作诊断。 | 这不是缩热更新范围——"持久 CLI 会话恢复"是另一个 feature。若用户裁定纳入，作为 S1 追加子项（S1 升 XL）。**本条向用户明示，可被推翻。** |
| 5 · F2 异常流合同 | **批准「aborted」语义**：`ProcessOutput.Error != ""` = 该流在 `Offset` 处 aborted（已读字节保留、不再有后续字节），`FIN` 保持 false、不调用 `Replay.Finish()`；terminal-ack 逐流接受 `(FIN && ack==End) || (aborted && ack==End)`；execclient 把 aborted 呈现为错误但仍完成 settle；registry 终态加 additive `stream_error`。驳回「异常 FIN=true+Error」。**S0 精确化**：「在 `Offset` 处」= `Offset+len(Data)`，且 Error 只出现在到达 End 的那一批（与 FIN 同形、互斥）；`stream_error` 在 S0 判定不需要，流属性由 `task.abort` / `OutputState.ReadError` 承载，S2 映像必须导出 `task.abort`。 | 让 `Replay.FIN` 同时表示真 EOF 与异常终结正是 handoff-failures §1 F2 禁止的伪造 EOF，且与 `workload.OutputState` 注释冲突。同一语义在 S4 复用于非 started-PTY 域 EIO、S5 复用于流错误。 |
| 6 · legacy（v0.6.0）首次迁入 | **采纳**：busy → `legacy_busy` 拒绝；idle → 仍 defer；新增显式 `node upgrade --interrupting` 走既有 §21.3 中断式路径，事务记 `legacy_interrupting`（非热、不计 GREEN），usage/runbook 写明这是现网 8 台 v0.6.0 agent 的一次性入口；capable→capable 的 idle 迁入在 S8 用 gen-N artifact 证明。 | plan §3.6/§10 已裁定无 quiescence 能力的 legacy 只能 defer；不给显式入口就等于现网永远无法升级，给了隐式入口就等于假装无损。 |

**对综合稿的其他修改**：S6 size L→XL 并加 single-mode lane（OQ1）；drill 编号 35/36/37/38/39/63/75/85/86/99 已核对 `expected-verdicts.tsv`（现有 00–98 中这些号均空闲）；
每个 stage 的 plan/review 文件名固定为 `docs/reviews/hot-update-<slug>-plan.md` / `-review.md`（多轮 `-review-round2.md`）；
每个 stage 的真实 drill **实跑前在该 stage plan 里预告成本**（用户 2026-09-18 裁定）。

## §0 总览

**一句话**：11 个 stage（S0–S10），按资源域切分：稳定树 → 可靠 exec（ctl 端 + 控制面 carrier 可换）→ agent 映像 ABI（同 PID harness）→ agent bootstrap/`node upgrade` 接线（**drill 34 正向 oracle 在 S3 转绿**）→ PTY → 逻辑流 + SS → broker 映像 → 传输连续性 → 升级事务/回滚/N-1/legacy 首次迁入 → NATS/Caddy/拓扑 → 全栈五代验收并停外审门。中间 stage 对尚不可迁移的资源域一律 **`deferred(<reason>)` 并保原业务**，原因码集合逐 stage 收缩，S10 断言为空。

**为什么是 11 而不是 ≤10**：四份批评一致要求把 broker 映像与传输拆开、把 agent 映像与 broker 骨架拆开，三份要求把状态 ABI 与 bootstrap 接线拆开；保留这三处拆分后，唯一能压回 10 的办法是再造一个跨两个资源域的 XL。本稿宁可多一个 stage 也不再造"两域一审"的 XL；SS 并入逻辑流 stage 是本稿**唯一**接受的两域合并，理由写在 S5。

| idx | name | slug | size | 真实 drill |
|---|---|---|---|---|
| 0 | 稳定交接树 | `stable-tree` | M | 否（仅 `make gates` 内 hermetic 集） |
| 1 | 可靠 exec 产品路径与 carrier 可换 | `reliable-exec` | L | **是**：新 35 + 34 重钉签名（首次 restage） |
| 2 | agent 运行层状态 ABI | `agent-image-abi` | L | 否（同 PID harness） |
| 3 | agent bootstrap 与 node upgrade 接线 | `agent-bootstrap` | L | **是**：34 → GREEN（红→绿）+ 33 回归 |
| 4 | PTY 双工资源 actor 与可靠 run | `pty-terminal` | L | **是**：新 36 + 34 回归 |
| 5 | 逻辑流 carrier 与 SS 状态 | `flow-carrier-ss` | XL | **是**：新 75 + 70/72 回归 |
| 6 | broker 映像交接（含 N=1 single / 1-voter 路径） | `broker-image` | XL | **是**：新 37（single / 1-voter / N=3 三 lane） |
| 7 | 传输连续性 | `transfer-continuation` | XL | **是**：新 63 + 61 回归 |
| 8 | 升级事务、回滚、N-1 与首次迁入 | `upgrade-orchestration` | XL | **是**：新 38/39 + 31/33 回归 |
| 9 | NATS/Caddy 产品路径与拓扑 | `infra-nats-caddy` | XL | **是**：新 85/86 + 32/73/98 回归 |
| 10 | 全栈验收、文档与最终内审 | `fullstack-acceptance` | L | **是**：新 99 + 全套复跑 + `--live-grow` 一轮 |

drill 编号为建议（已避开 40/41/42 等占用号），最终由 inventory / `expected-verdicts.tsv` 分配。

---

## §1 逐 stage

### S0 · 稳定交接树（`stable-tree`，M）

**goal**：把 Codex 交接的 169 文件暂存树变成三硬闸全绿、登记齐全、两增量边界清楚、可复现的基线；不新增任何产品能力。

**scope_in**
- `internal/execsession/runtime.go`：F1（closed runtime 下迟到 Start 成功/失败的唯一最终清场 owner，与正常保留 terminal replay 的路径分开；并发双 Close 无双 waiter）；F2（分离"停原 source/pipe/pump"与"保留未交付结果"）。
- **F2 合同（本稿定稿，主进程 §4-OQ5 批准）**：`ProcessOutput.Error != ""` 表示该流在 `Offset` 处 **aborted**（已读字节保留，不再有后续字节）；**`FIN` 保持 false，不调用 `Replay.Finish()`**（`workload.OutputState` 注释已规定 FIN 只表示真 EOF，`ReadError` 单独承载异常）；terminal-ack 逐流接受 `(FIN && ack==End) || (aborted && ack==End)`；execclient 把 aborted 作为错误结果呈现并保留真实 exit/signal，但仍完成 settle；registry 终态**不**加 `stream_error`（S0 判定：流属性由 `task.abort` / `OutputState.ReadError` 承载，见 §-1 OQ5 精确化）。同一 aborted 语义在 S4 复用于非 started-PTY 域 EIO、S5 复用于流错误。
- `internal/execclient/client.go` + `internal/proto/process_resume.go`：与上述合同一致的接收侧（Error 批次不当 FIN；**零新 wire 字段**——S0 实际只改注释，并删掉了 `ProcessResumeReply` 顶层从未被读写的 `Stream/Offset/Data/FIN` 四个死字段，账本手改收缩 4 行，理由记在 S0 review）。
- 登记：test inventory（+45 交接 + S0 新增测试）、wire inventory（+49 −4）、三个 leak anchor（`TestMasterPollableLifecycleReleasesResources→exerciseMasterReadDeadline`、`TestRegistryRepeatedLifecycleReleasesResources→exerciseRegistryLifecycle`、`TestRuntimeRepeatedExecutionReleasesResources→exerciseRuntimeExecution`）、`TestLeakRiskMatrix` 加入 `./internal/procrequest/... ./internal/execsession/... ./internal/execclient/...` 并只增更新 matrix golden；新增 `context.Background()` 站点标注或入账本。
- `__pycache__`：**只核验**（`git ls-files | grep -c __pycache__` = 0，`.gitignore:93-94` 已含）；若主进程环境中仍在 index 才 `git rm --cached`。
- 两增量边界：在 `docs/reviews/hot-update-stages-plan.md` 与 evidence 记录 control-connection-recovery（46 文件 / 43 测试，两轮内审已闭合、外审未做）与 hot-update 的文件级归属；后续每个 stage 不得让其测试变红；evidence 分栏。

**scope_out**：Freeze/Export/Import（S2）；真实 CLI e2e（S1）；任何 restage / 真实 drill（S1 首次）；任何 golden/账本放宽（永不）。

**entry**：HEAD `cab90d1` + 169 文件已暂存；`go build ./...`、`make vet-tags`、test/architecture、cmd/tether、test/e2e/parallel 绿；已知红仅 F1/F2 + 三本清单。

**exit（机械判定）**
1. `go test -race ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure|PumpSetupFailureRetainsObservableTerminal|PartialAllocationFailureSettlesAndReleasesBudget|CloseReapsChildWithUnacknowledgedFullOutput)$' -count=3` 四条 PASS，断言未削弱；F1/F2 各有一处 overlay 变异证明能再红（留 rc）。
2. `make test`、`make e2e-parallel`（并行全绿即通过）、单份 `make lint`、`make gates`（含 `sh test/simcluster/tests/run-all.sh`）全绿，退出码直接取。
3. `GOOS=darwin GOARCH=arm64 go build ./...` 绿。
4. test/wire/matrix inventory 新增行数 = 预期（交接 45 + S0 新增测试 / 49 − 4 死字段 / 3 单元），`TestEveryLeakAssertionExercisesItsSubjectRepeatedly` 绿；structural golden 未放宽。
5. `git ls-files | grep -c __pycache__` = 0；`git add -A` 后 `git status --short | grep -v '^[AMDR] '` 为空（无未暂存、无未跟踪；暂存项本身仍会显示）。
6. stages-plan 落盘（本文定稿版）；evidence 追加 S0 段（命令/rc/时长/HEAD/`git write-tree`）。

**tests**：F1/F2 原测试转绿 + 「aborted 流可 resume 且 terminal-ack 结清」「把 aborted 改写成正常 FIN → 红」「未创建 child 的 allocate 失败路径仍是两条空流真 FIN」；execclient aborted 呈现测试；proto 零值/omitempty 测试。
**drills**：无。
**docs**：`hot-update-stages-plan.md`（新）、evidence S0 段、tasklist 执行记录（不勾功能项）、INDEX 一行、architecture §22 追加 aborted 语义一句。
**coverage**：handoff §5 F1/F2；handoff §7 第 1–4、11 项；plan §3.3 第 8 条（异常流不伪造 EOF）落地；tasklist H-6 的首次登记（每 stage 重复，S10 收口）。
**risks**：F2 用伪造 FIN 换绿；updater 匹配不到测试名 exit 0 假成功（核对 golden 行数）；首轮 e2e-parallel 暴露交接树包间冲突，只能 `make e2e-one T=` 定位。
**plan_lenses**：runtime 失败路径状态机 / 协议接收端一致性（aborted 合同两端）/ 闸门与登记 / 两增量边界与证据归属。
**review_lenses**：并发与资源释放（race×3、五轮）/ 协议一致性（runtime↔execclient 逐条对照）/ 闸门变更正当性 / 文档-账本-行为一致。

---

### S1 · 可靠 exec 产品路径与 carrier 可换（`reliable-exec`，L）

**goal**：把 procrequest/execsession/execclient/process-resume 原语接成真实 `tether` 二进制 ctl→broker→agent→child 的产品路径；证明控制面 carrier（agent NATS session 重建、broker 重启、nats-server 重启、ctl 断线）可替换而任务不丢、不重、退出码准确——这是后面每一次 exec/LDM/Caddy 切换的地基。不含任何跨 exec 状态保留。

**scope_in**
- ctl：`internal/execclient` + `cmd/tether/exec.go`（无新文件）：真实 CLI 全链路（短写、lost create/terminal reply、AttachEpoch、`ErrExpired` 收敛=仅持有双 FIN|aborted + terminal 时视为完成、no-responder ≤500ms 可取消退避、`--timeout` 语义、exit/signal/aborted 呈现）；**连接级有界重试**：对已在 `~/.tether` 持久身份且曾认证成功的 actor，把 `Authorization Violation`/no responder 视为 transient，在 ≤5s 预算内退避重试（前提"本身份此前认证过"并记原因，因为同串也是错凭据）；legacy fallback 只在 create 前明确 unsupported 时触发，权限/会话错误不降级。
- broker（`exec.go` 尾部，不新增文件）：每 op 重查当前 ACTIVE/member/node；CallID 三臂错误关联；私有 reply 校验；**转发改为 raw overlay**（`map[string]json.RawMessage` 覆写 ActorFP 再 marshal），加「未知字段穿过 broker 不丢」测试——消除 handoff §3.3 #1 对新家族的同一告警。
- agent：`Started` 回调改到真实 `Start` 之后并带 OS PID/start ticks；**legacy runChild 与 reliable runtime 的任务表合一**（ps/kill/DropProcesses/G.1 快照对 reliable 任务有效）；**C-5** 联合 starting/courier/register 准入屏障（starting 记录进联合快照；register 回复只 settle 携带的事件集）；**C-10 exec 半**：session_destroyed/kick/revoke → Runtime revoke（kill 进程组与后代、tombstone 结构在此定义、拒绝后续 ACK/resume）；freeze 与删除竞争的定义先写进 plan（S3 实现导入半）。
- procrequest/execsession：容量满前拒绝、settled+expired GC、过期未知拒绝重执行、reply 丢失→切代→GC→晚重试窗（C-6 的本 stage 半）；**abandoned-terminal 的有界保留**（S0 内审 protocol 核验者 missed：ctl 在 terminal-ack 前离开的 task 今天永不回收，两条输出 replay 预算保留到 `Runtime.Close`，默认 8 MiB/流、256 MiB/节点下约 15 个被抛弃的 task 就让该 agent 后续所有可靠 exec `ErrBackpressure`）——lease 到期 + grace 后释放 replay、registry 留可查询的 tombstone（"output discarded"），plan §3.1「终态至少保留到租约过期」的"至少"在此封顶；带"抛弃 N 个 task 后新任务仍可创建"的负控制。
- hermetic N-1 四象限：`go test` 内只用「旧端不订阅新 verb / legacy 路径模拟」，**不在 go test 里构建 v0.6.0**。
- simcluster 供给：`lib/stage.sh` 新增 `vendor/tether-prev` = `git worktree add … v0.6.0` 源码构建（`CGO_ENABLED=0`、toolchain 钉住、记录 tag+SHA）；`tether-next` 只用于 exec 机制/运行 SHA 反例；两张旧镜像不可复用，本 stage 首次 `./local.sh build` restage 并记录镜像 ID / vendor SHA / HEAD+dirty 摘要。
- drill 34 重跑并**重钉 legacy 失败签名**（ctl 已走可靠路径，ps/EPIPE/rc 形状可能变；旧签名收据保留），作为 S3 red→green 的红半。

**scope_out**：Freeze/Export/Import 与 same-PID 交接（S2/S3）；PTY/run（S4）；legacy-wire 会话在场的激活拒绝码（S3）；真实 v0.6.0 四象限 drill 臂（S8）；ctl 进程重启后的句柄恢复（§4-OQ4）。

**entry**：S0 出口。

**exit**
1. `test/cli_e2e/exec_reliable_test.go`（真实构建二进制 + 嵌入 NATS + broker + 真 agent 进程）：48KiB stdin、双流、rc 23、signal→128+sig、短写、丢 create/terminal 回复、agent NATS 会话中断续接、broker 重启续接、legacy fallback、`--timeout` 本地取消不远端 kill 全绿。
2. 嵌入 nats-server 上「callout responder 缺席 1.5s 再回来」确定性测试：ctl 连接级重试在预算内成功；负控制：从未认证过的身份不重试。
3. broker：CallID 三臂绿；撤 member 中途 → ctl 得权限错误（非 unsupported）、agent task SIGKILL 并有 terminal；未知字段穿透测试绿；`make fuzz FUZZ='^FuzzPrivateProcessReply$' FUZZTIME=30s` 复验（若改规则）。
4. agent：spawn 成功/started 未持久化窗口下 G.1 快照不误杀（真实 child，race×3）；session_destroyed 后真实 exec 子树被杀且 tombstone 拒绝复活；ps 显示 reliable task RUNNING/EXITED。
5. drill 35 臂 A（nats-server 重启中途）/臂 B（broker 重启中途）GREEN：原 child PID/nonce 不变、ctl 收齐全部字节与真实退出码、无重复执行；**新请求时延只作校准记录（NOT a pass criterion）**；臂 C：v0.6.0 ctl 对新 agent 走 legacy exec，oracle 判「不承诺连续」。
6. drill 34 重跑 PRODUCT-RED（新签名），`expected-verdicts.tsv/log` 注记更新、旧收据保留。
7. 三硬闸 + `make gates` 绿；Darwin 绿；inventory/wire/matrix 只增；CLI golden 若变更写理由；Agent 方法 ≤126、broker 文件 =70。

**tests**：上述 cli_e2e；execclient 真 child 草稿转正（`TestRunRealChildSurvivesLostCreateAndSettlementReplies` 等）；broker 授权/CallID/未知字段；agent 准入 census/清场/任务表合一；procrequest 容量/GC/晚重试；hermetic 四象限。
**drills**：35-hot-update-exec-carrier（新，≈4 min + restage ≈3 min）；34 重钉（≈2 min）。
**docs**：architecture §22「可靠进程请求协议与准入」（把 handoff-protocol 9 条裁决搬成活契约）；usage.md（exec 可靠语义、退出码、fallback、ctl 中断=放弃）；testing-standards + test/README（真实 binary e2e 层、tether-prev 供给法）；simcluster README（35、tether-prev）；evidence；tasklist 勾 C-5、B-5。
**coverage**：tasklist **C-5、B-5**（owner）；C-2/C-4/C-6/C-10 的 exec 半；plan §5 行 1 的 unit/component 半；plan §3.1 裁决 1–9；handoff §8 步骤 2。
**risks**：两套任务表不合一 → S2 导出漏任务；fallback 判定过宽把权限错误当 unsupported 造成双执行；连接级重试把真错凭据当 transient（必须以曾认证成功为前提）。
**plan_lenses**：协议/幂等/CallID（ctl 端）/ broker 授权与 reply 注入 / agent 准入屏障与任务表合一 / drill 35 与 tether-prev 供给。
**review_lenses**：重复执行/双 spawn 反例 / 授权撤销与迟到消息 / 短写-断链-迟到回复 / N-1 fallback 与 legacy exec 不退化。

---

### S2 · agent 运行层状态 ABI（`agent-image-abi`，L）

**goal**：把 execsession Runtime 变成带版本化状态 ABI 的可交接运行层：统一准入 fence（接纳/spawn/wait/signal/输入/cancel/reaper 全部来源）、联合 Freeze（procrequest.State + workload.ProcessState + 3 replay + Input/Output + pending startup + terminal + tombstone + 预算）、导出为 handoff bundle、导入为 frozen→全域就绪才 Thaw；courier outbox 与 spawnsafe 状态导出；用测试 harness 的**真实同 PID exec** 证明原 child/pipe/字节/退出码连续。不接产品路径。

**scope_in**
- `internal/execsession`：Freeze/Export/Import/Thaw；fence；image schema v1（版本、instance、boot ID、每 task 的 handle/pid/start ticks/replay 三份 FD + 精简元数据/input/output 状态/phase/terminal/aborted）；starting 存在时 ErrBusy 有界等待；导入校验失败无部分安装、不 reap/不 signal。
- 新包 `internal/agentimage`：`Bundle{Version, Instance, Lineage, Runtime, Courier, SpawnSafe}` 组合 + `Prepare/Restore` 适配 `internal/handoff`；每域独立校验、任一失败整体拒绝；预算守恒（导入后节点 256MiB/流 8MiB 记账等于导出前）。**接口用 deadline/`<-chan struct{}` 而非 ctx**（为 S5 的 ssproxy 域预留，`dataplane_lifetime` 门禁止 ssproxy 方法接受 ctx）。
- `internal/agent/proc_delivery.go`：courier outbox State 导出/导入（包级函数）；`internal/spawnsafe`：Policy 健康判定/wedge 计数导出，in-flight helper handshake → `spawn_in_flight` defer 判定。
- `internal/workload`：核对 ReapHeld zombie 随 ProcessState 导出、Output/Input Restore 缺口。
- 归档本 stage 出口二进制 + SHA 到仓库外证据目录，**标注为「ABI 代际机制证明（dev gen-1）」**，不是 release 级 N-1。
- 秘密（registry HMAC secret 等）只进 sealed memfd；manifest JSON 只含范围。

**scope_out**：bootstrap/upgrade.go/marker/G.1 联合快照/session_destroyed 导入半（S3）；PTY（S4）；流/SS/传输资源类（S5/S7）；broker（S6）；wire 无变更。

**entry**：S1 出口（ACK/FIN/aborted/terminal-ack 合同固定；任务表合一；tombstone 结构已定）；`internal/handoff` 契约不变。

**exit**
1. `go test -race -count=3 ./internal/execsession ./internal/agentimage ./internal/workload ./internal/procrequest ./internal/spawnsafe` 全绿。
2. 真实同 PID exec 测试：运行中 child（stdout 部分 ACK、stdin 未写完、stderr warning prefix、后代持 pipe）跨 exec 后 execclient 得到精确字节/顺序/rc；原 PID/start ticks/pipe inode 不变；TargetInstance/lease key 原值恢复。
3. Freeze 在 starting/in-flight spawn 存在时 ErrBusy 或有界 defer；导入校验拒绝表（schema/boot ID/PID 复用/FD 指纹/预算超限/重复 handle）无部分安装。
4. 变异：漏导出 stderr replay / 漏 courier exit 事件 / 导入后重复预留预算 / 漏 tombstone → 各自红（留 rc）。
5. 五轮 freeze/export/import 资源门（goroutine/FD/memfd 容差 0）；三硬闸绿；Darwin 绿（`_unsupported.go`）；inventory/anchor/矩阵登记（agentimage 进某个 -race 单元）。
6. 同 PID exec 测试 helper 复用 handoff/procrequest 的 dispatch 形状（精确 `-test.run=^…$`、mode env 双向剥离、helper 内不二次 fork）。

**tests**：`internal/execsession/runtime_image_exec_test.go`（`TestRuntimeImagePreservesLiveChildAcrossExec`、`…RestoresPartiallyAckedOutputs`、`…FreezeRefusesWhileStarting`、`…ImportRejectsInvalidBundle` 表驱动、`…ImageRoundTripReleasesResources`）；`internal/agentimage`（`TestBundleRoundTrip`、`TestCourierOutboxSurvivesImage`、`TestSpawnInFlightDefersFreeze`、`TestBudgetConservedAcrossImport`）；`TestRuntimeFreezeLatencySample`（本地样本，非达标声明）。
**drills**：无。
**docs**：architecture §22 运行层 image ABI/schema/fence 六段屏障；testing-standards（同 PID harness 形状）；evidence；tasklist 勾 C-2、C-6。
**coverage**：tasklist **C-2、C-6**（owner）；C-3/C-7 的 ABI 半；plan §3.2 进程域、§4.2 裁决 1/2/6；handoff §8 步骤 3；handoff-process §8 项 2/3/4。
**risks**：fence 只锁 registry 不锁 pump/wait/spawn/cancel；Thaw 顺序错（先网络后本地泵、临时 pause deadline 当业务 deadline）；256MiB replay 进 JSON manifest（必须留在匿名 FD）。
**plan_lenses**：状态机/fence（六段屏障与读写/wait 权）/ FD 与内核资源（memfd/pipe/zombie/pidfd/CLOEXEC）/ schema 版本化与 fail-closed 校验 / 测试 harness（同 PID exec、五轮、变异清单）。
**review_lenses**：安全点正确性（n>0+err 先记账、不 Close 业务 FD）/ 导入校验完备性（伪造 bundle、错 PID、预算注入）/ 预算与资源守恒 / 与 S1 协议一致（跨代后 ACK/aborted/terminal-ack 仍成立）。

---

### S3 · agent bootstrap 与 node upgrade 接线（`agent-bootstrap`，L）

**goal**：把 S2 的 ABI 接到产品路径：pre-Cobra bootstrap、`node upgrade`/ReExecOnly 的 freeze→export→Prepare+Exec→import→thaw、exec 失败原地解冻、导入/健康失败经保 FD 的同 PID 再派发回 prev、首个 register 前恢复本地泵与 courier、G.1 联合快照含导入任务、session_destroyed tombstone 导入、预检 **deferred 原因码集合**、三种部署形态；**drill 34 正向 oracle 转绿**并做首次 ≤500ms / 10/s 校准。

**scope_in**
- `cmd/tether/main.go`（只加调用，逻辑在 internal）：**顺序裁决**——`agent.BootUpgradeCheck` 先跑（它在预算耗尽/过期分支会直接 exec prev）；仅当决议为 proceed/continue 才 `agentimage.Restore`；校验失败则**从仍冻结的状态重新 Prepare 再 exec prev**；`Restore` → control-ready 之间的每条退出路径（含 Cobra/strict-YAML 失败）都经再派发；handoff 层自身校验失败（sealed memfd 非法、env 已消费）如实定义为 unknown/error 崩溃窗并进 S8 注入矩阵。
- `internal/handoff`：新增「从 Restored 文件 + 同一 State 重新 Prepare 并 Exec(prev)」再派发 API（真实 exec 测试）。
- `internal/agent/upgrade.go` / `upgrade_state.go`：reExecInPlace/handleReExecOnly → 安全点等待（有界）→ Freeze/Export → `handoff.Prepare+Exec`（execEnv + dispatch env）→ 失败恢复 CLOEXEC/Thaw；marker additive 字段（`resource_phase/generation/imported_resources`）；watchdog 回退前重新导出当前态；bundle 在场 + 预算耗尽 → prev 收到同一 bundle。
- `internal/agent/agent.go`（包级函数/新文件，不加 Agent 方法）：New 接收 restored bundle；首个 register 前 Thaw 本地泵与 courier；G.1 联合快照含导入任务（无 EXITED(-1)）；禁止空表 register；handleSessionDestroyed 的 tombstone 导入不复活（C-10 导入半）。
- **预检 deferred 集合（首版）**：`pty_busy`、`flow_busy`（expose/proxy 流）、`ss_busy`、`transfers_in_flight`（prepare cache/在途 push/pull）、`legacy_session_busy`（旧 ctl 的 legacy-wire exec/run 会话）、`spawn_in_flight`、`remote_fs_probe_in_flight`。存在即 deferred 并保原业务、报告具体原因；后续 stage 各自移除。
- remote-fs/控制任务：freeze 前有界 drain；D-state 探测未完成即 defer；只读请求 ctl 有界重试。
- 新包 `internal/upgradebudget`：plan §7 数值作为**单一常量源**（≤500ms/≤5s/10s/120s/15min/8MiB/256MiB/两代），测试与 drill 引用；deadline 一律 CLOCK_BOOTTIME 携带。本 stage 只产出进程域首次校准样本，不宣称冻结。
- 部署单位：systemd / user unit / setsid-nohup 三形态下同 PID exec 与清场（不停 unit、MainPID 不变、KillMode 不动）；gen1→gen2→gen3 连续两代（runtime 自身升级机械证明）。
- drill 34：`_continuity` 升为 `assert_ok`；新臂：两连续升级、user unit、no-systemd、稳定负载校准臂（无升级）、10/s 混合探针（读 `ps`/`node ls` + 写 `exec` 带副作用 + 短命令，分类记 p50/p95/p99/max，区分 CONNECT 失败与请求失败计数）、旧映像退出（`/proc/<pid>/exe` SHA、无残留 memfd/pidfd）、导入失败回滚臂、remote-fs 臂（复用 62 的 fuse.hangfs 供给）、**负控制臂用 `vendor/tether-prev` 真实旧 binary**（不用测试开关）；hermetic 五份基线同步（assert-identity、kept-sites、drill-costs、expected-verdicts+log、coverage inventory）。

**scope_out**：PTY（S4）、流/SS（S5）、broker（S6）、传输（S7）、完整事务状态机/两代限额/故障注入矩阵/真实 N-1（S8）。

**entry**：S2 出口（Bundle 往返与同 PID harness 绿）；S1 的 tether-prev 供给与重钉签名。

**exit**
1. **drill 34 GREEN**：原 child PID/boot/startticks/nonce 不变、stdout/stderr 序号完整、ps RUNNING、原 ctl 收到 rc 23、agent 同 PID 换新 SHA + committed marker；`expected-verdicts` PRODUCT-RED→GREEN，红收据（S1 重钉）保留成对。
2. 34 新臂全绿；tether-prev 负控制臂 PRODUCT-RED；导入失败臂：原映像继续、原 child 输出连续。
3. drill 33 各臂原判词不劣化（INCOMPLETE 带 #73 不变）；B 臂 120s watchdog 在有 bundle 时行为明确。
4. hermetic：`TestAgentBootstrapRunsBootCheckBeforeRestore`（真实构建 binary）、`TestBundlePresentAndBudgetExhaustedRedispatchesToPrev`、`TestImportFailureNeverRegistersEmptyTable`、`TestReconcileSnapshotIncludesImportedTasks`（变异：漏导入→G.1 -1→红）、`TestSessionDestroyedTombstoneSurvivesImport`、`TestPreflightDefersOnEachReasonCode`（表驱动）、no-systemd 路径。
5. freeze→新泵可用 ≤500ms 有测量样本（写 evidence，标"校准数据"）。
6. 三硬闸 + `make gates`（drill 与 go test/lint 串行）；Darwin；agent 代码行 <8000、方法 ≤126。

**tests**：如上 + `TestUpgradeExecFailureThawsOriginalBusiness`、`TestRegisterReceiptSettlesOnlyCarriedEvents`（导入 outbox 身份）、`TestGenerationChainExportImportExport`。
**drills**：34（GREEN + 臂，≈6 min）、33 回归（≈5 min）；每次前 restage。
**docs**：architecture §21.3/§22（bootstrap 顺序、bundle 与 marker/watchdog 关系、再派发、resource-active vs control-ready、deferred 原因码）；usage.md（node upgrade 行为、deferred 处置）；simcluster README 34 行 + expected-verdicts；deploy-tier-gotchas；tasklist 勾 C-3、C-7、C-9、C-10。
**coverage**：tasklist **C-3、C-7（含子项）、C-9、C-10**（owner）；C-4 的「旧 ctl 会话 → legacy_session_busy 激活拒绝」半；plan §5 行 1 drill 半、**行 2 整行**、行 7（预检/排空/wedged 半）；plan §4.2 裁决 3/4/5/7；handoff §8 步骤 4；B-4 新请求半、A-6 首次样本。
**risks**：Restore/BootUpgradeCheck 顺序错让回退 exec 丢原 child 的 pipe/replay FD；导入顺序错→先 register 后导入→G.1 EXITED(-1)（H0 红的机制换位置）；drill 与 go test 并发假红或 restage 遗漏跑旧镜像。
**plan_lenses**：启动序与回退（boot shim × bundle × marker × watchdog 状态表 + 再派发 API）/ agent 会话/G.1/courier / 部署单位（systemd/user/nohup、cgroup、共享 binary sibling、spawnexec 租约跨 exec）/ drill oracle 与校准（deferred 集合、混合探针、tether-prev 负控制）。
**review_lenses**：回退路径反例（导入失败/exec 失败/watchdog/bundle 损坏/Cobra 失败）/ 并发窗口（register vs 新建 vs 导入 vs session_destroyed）/ 撤权与清场 / drill 证据真实性（运行 SHA/PID/nonce/四日志流/不与 go test 并发）。

---

### S4 · PTY 双工资源 actor 与可靠 run（`pty-terminal`，L）

**goal**：PTY 成为可交接的双工资源：唯一 master owner actor（读写 adapter 借用）；输入 FIN≠关 master、不注入 0x04、不自动 SIGHUP；resize/信号/KA/Hangup 序号化进同一 fence 且 Hangup 的 Signal→Close 不跨快照；**仅 started+身份验证 PTY 域**的 last-slave EIO=输出 FIN（其他域按 S0 aborted 语义）；200ms 尾流替换为后代 slave 持有感知；ctl `run` 走可靠输出/输入 seq/ACK；legacy `run` wire 原样保留；原 PTY 贯穿 agent 升级与一次回滚。H1 在此收口。

**scope_in**
- `internal/pty`：master owner；EIO 语义；termios/winsize/前台 pgrp/KA 与授权绝对期限导出（同 boot 时钟）；resize 走 RawConn 不碰 `File.Fd`；`prepareMaster` 对运行中 FD 交接安全。
- 新包 `internal/ptysession`（import pty，自带泄漏门）：双工 actor、输入 seq/ACK 有界队列、输出 replay（`workload.Replay`）、Freeze/Export/Import 走 S2 ABI；execsession 通过接口接纳第二 task kind。
- wire：**同 process-resume verb 家族 additive**（`Kind=pty`、`Run *RunReq`、`Input/InputSeq`、`Resize`、`Signal`、`Hangup` op），复用 lease/handle/CallID/AttachEpoch/授权；**broker `exec.go` 尾部 op 白名单同步扩展**（不是零改动）；wire inventory 只增。
- `internal/agent/run.go`：handleRunForwarded 可靠路径；`.in/.resize/.ka` 进 fence；legacy 双发布去重；kill/ps 对两类任务统一；ctlLivenessReaper 期限跨代不重发预算。
- `internal/execclient` pty 模式 + `cmd/tether/run.go` reliable 分支 + legacy fallback（无新文件）。
- 预检移除 `pty_busy`。

**scope_out**：SSH/TCP/SS（S5）；broker 转发逻辑（除白名单）；legacy ctl 的 run 会话仍是 `legacy_session_busy`。

**entry**：S3 出口（bootstrap + 34 GREEN + deferred 集合框架）。

**exit**
1. 真 PTY 测试：后代持 slave >200ms 的尾输出完整；阻塞 raw slave 下 partial write→同 PID exec→原 PTY 身份/双向字节无重无丢；canonical/raw 下逻辑 FIN 与真实 Ctrl-D 分离；resize/前台组/SIGINT/Hangup 与 exec 交错；process freeze 下 Hangup 拒绝且 master 保留；非 started 域 EIO 仍是 aborted。
2. `test/cli_e2e/run_reliable_test.go`（真实 binary + 伪终端）绿；legacy run 仍可用。
3. **drill 36 GREEN**：原交互 PTY + resize + 长前台作业跨 agent 升级 + 一次回滚；无 EOF/HUP、双向序号完整、原前台 PID、结束无 zombie/FD 残留；负控制臂（tether-prev 旧路径 → 红）。
4. 34 回归 GREEN；变异：恢复 200ms 截断 → 红；resize 改回 `File.Fd` → 红；EIO 统一吞 EOF → 红。
5. 三硬闸 + gates；Darwin（`master_other` 路径）；ptysession 在 -race 矩阵且有 leak 锚；C-11 收口收据齐全。

**tests**：`internal/pty` + `internal/ptysession` race×3 + 五轮；`TestPtyTailOutlivesLeaderExit`、`TestPtyEIOOnlyInStartedDomain`、`TestPtyActorSurvivesSamePIDExecWithBlockedSlave`、`TestHangupSequenceNeverSplitsAcrossSnapshot`；execclient pty 模式（输入 seq 重放/去重、resize 序号、断链续接）；agent legacy 双发布去重回归；broker 白名单扩展 + 未知 op 拒绝。
**drills**：36-hot-update-pty（新，≈4 min）、34 回归（≈2 min）。
**docs**：architecture §22 PTY 域 ABI/EIO/FIN/KA 期限；usage.md run 可靠路径与终端语义；simcluster README + expected-verdicts；tasklist 勾 C-4、C-8、C-11。
**coverage**：plan §5 **行 3**；tasklist **C-4、C-8、C-11**（owner）；B-2 PTY 子项（B-2 owner 在 S5）；C-10 PTY 变体；handoff §8 步骤 5；handoff-failures §4 1–6。
**risks**：读/写 actor 共享 master FD 的 deadline/O_NONBLOCK 互扰；EIO 误判（后代仍持 slave 时提前 FIN）；输入 seq/ACK 与 legacy 双发布重复注入；freeze 丢半个 escape 序列（pending suffix 必须保留）。
**plan_lenses**：终端内核语义（termios/EIO/pgrp/信号/HUP 序列）/ 可靠双向协议（seq/ACK/replay/resize 序号）/ owner/fence（Hangup/resize/KA 序列化、撤权期限导入）/ ctl 体验与 N-1（legacy run 保留、fallback）。
**review_lenses**：尾流/EOF/EIO 反例 / 双工 FD 并发与 deadline / 权限与撤权（KA 绝对期限不延长、tombstone）/ drill 真实性（无 HUP/EOF、原前台 PID、残留）。

---

### S5 · 逻辑流 carrier 与 SS 状态（`flow-carrier-ss`，XL）

**冻结细化**：[binding S5 plan](hot-update-flow-carrier-plan.md)，顶部 lead Q1–Q3/M 优先；
architecture §22.8 是活契约（§22.7 保留 S4）。本节原概述与该冻结合同冲突处由其覆盖；
[六出口收据](hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index) 当前 NOT_RUN pending P29。

**goal**：在可替换 TLS/yamux carrier 之上实现有序逻辑流（FlowID/双向 offset/ACK/FIN/重放/背压/OPEN 窗口）；TLS ALPN `tether-flow/1` 协商，REGISTER 六字段与精确 OK 原样，未协商即 legacy raw bridge；**broker 侧 Endpoint 生命周期与 serverSession 解耦**（listener/pubConn/流表在 carrier 重建时存活并受围栏）；agent 端 raw socket owner 接入 image；SS AEAD 全状态导出导入；原 SSH/TCP/SS 长流贯穿 agent 升级 + 强制 carrier 换代 + 迁移中撤销。

**XL 不可拆理由**：流协议两端（agent Client / broker Server）不同时落地就没有可判定 oracle；Endpoint 解耦是 RESUME 成立的前提，不是"加一个 grace"；SS 两端是 agent 进程内 loopback 对，其 FD 复原就是 flow 的 agent raw socket 机制，单列只会为一个 M 级域再付一轮 restage/drill；本稿以 4 条 review lens（含密码学）、独立子包与独立 drill 臂/fuzz 补偿。

**scope_in**
- 新包 `internal/flow`：帧（OPEN/OPEN-ACK/RESUME/RESUME-OK/RESUME-REJECT/DATA/ACK/FIN/CLOSE）；FlowID 由 opener 铸造（128bit）；**ACK = 对端 raw socket `Write` 实际接纳的 applied offset**，去重在写业务 socket 前；FIN→`CloseWrite` 而非 Close；replay 复用 `workload.Replay` memfd（8MiB/流、256MiB/节点，满即背压停读）；socket 经 `SyscallConn` dup + `net.FileConn` 复原；恢复票据绑定 (sid,nid,publicPort,tokenHash,epoch)+当前授权+handoff 决议；`FuzzFlowFrame`。
- `internal/tunnel`：server/client `NextProtos`；**N-1 纪律**：客户端永远提供所有仍支持的版本并按新→旧排序，服务端选最高交集，无交集时**仍必须走 legacy**（不得只靠 NextProtos 判定）；**Endpoint{listener, pubConns, flow 表}独立于 serverSession 存活并受 `killGen*/forgotten/closedAllocation` 围栏**；同 allocation 的 REGISTER/RESUME 先 attach Endpoint、bind 只在无 Endpoint 时；bridge 替换为可暂停 pump；detached 保活 grace 为产品常量且 **`resume grace < proxyRehomeDwell(3×5s)`**（或计划维护态不计 unready）并加回归；RESUME 重跑 tokenLookup + fence；OPEN 幂等；CloseProxy/CloseSession/ForgetSession 对 detached 流生效；旧 epoch/迟到 activate 拒绝。
- `internal/agent/tunnel_adapter.go`、`proxy.go`：流域进 image；复原顺序 raw 泵→重拨 carrier→RESUME→再 NATS register；session-state hook 在计划内 freeze 期不报 `proxy_ready=false`。
- `internal/agent/ssproxy`：relay 改为可冻结 actor（chunk 对齐、pending 已 seal 未写密文、未转发明文）；per-conn {keyID, 读/写盐, nonce 计数}、saltRing、密钥表、listener FD 导出（全部 sealed memfd）；**方法不接受 ctx（dataplane 门）**，适配器放 `internal/agent` 侧；单次使用 Server 由 Restore 构造新实例；nonce 不复用显式断言；撤权与 freeze 竞争；无法安全表示的状态 → `ss_state_unrepresentable` defer；`FuzzSSFrame`。
- 预检移除 `flow_busy`、`ss_busy`。

**scope_out**：broker 自身 exec 时的 Endpoint/fence 表导出（S6）；transfer（S7）；NATS/Caddy 换代触发的主动迁移（S9，本 stage 只证 carrier 可换）。

**entry**：S4 出口；`internal/tunnel` 现有 OPEN replay/fence/pin 测试全绿作回归基线。

**exit**
1. 真 TLS+yamux 测试：多流双向 seq/hash 跨 carrier 重建零丢零重；OPEN 中/空闲/活跃/慢接收/half-close/多流/迁移中撤权/目标已关/旧 epoch 拒绝各有断言；ALPN 四象限（含"无交集仍 legacy"）；legacy 端 raw stream 前无 header。
2. agent 同 PID exec 跨升级：sshd 侧与 broker 侧原 socket 均不关闭；重放去重在写入业务 socket 前；无 ACK 不无限占内存。
3. broker：`TestEndpointSurvivesSessionLossAndResumes`、`TestResumeReRunsTokenLookupAndFence`、`TestReaperDoesNotRehomeDuringPlannedCarrierRebuild`。
4. SS：AEAD 状态跨导出/导入 nonce 连续、截断/半帧、盐重放表跨代、撤销竞态、密钥轮换后原流不断；`make fuzz FUZZ='^Fuzz(FlowFrame|SSFrame)$' FUZZ_MIN=2 FUZZTIME=30s` 无 crasher；秘密不进 manifest JSON（变异→红）。
5. **drill 75 GREEN**：原 SSH + 4 条双向 TCP 长流 + 1 条 OPEN 中 + 1 条 half-close + 1 条原 SS 长连接 + 并发新流，贯穿 agent 升级 + 强制 carrier 换代 + 升级期间撤销一个 SS 订阅；禁止应用重连；原 socket/session nonce 不变；非慢接收最大停顿 ≤5s（p50/p95/max）；负控制臂（提前关原 socket / 错 offset / tether-prev 旧路径 → 红）；legacy 臂（v0.6.0 broker 无 ALPN ↔ 新 agent raw bridge 互通，判「不承诺连续」）。
6. 70/72 回归判词不变；三硬闸 + gates；Darwin；`internal/flow`（不 import yamux/tunnel，仍显式列入 risk）与 ssproxy 在 -race 矩阵、leak 锚齐；wire additive；ctx 站点标注。

**tests**：`internal/flow` 表驱动/属性/fuzz；tunnel Endpoint/ALPN/RESUME/fence；ssproxy 状态/fuzz；`test/hotupdate`（新主题目录，登记 test/README 地图）集成：agent 同 PID exec 持 4 条 TCP 流 + SS。
**drills**：75-hot-update-flows 六臂 A/C/R/B/W/L（estimate 总1290s，declared worst3060s，非实测）；
39.F 真旧 exporter 原生 PRODUCT-RED；early-close/wrong-offset 独立 ASSERT-FAIL；70/72 完整回归；
36.A/W/C/L GREEN 与39.P PRODUCT-RED 同候选复验。全套 serial deploy estimate60–90min，实际wall仅取COMPLETE receipt。
**docs**：architecture §7/§22（逻辑流层、ALPN 与 N-1 纪律、Endpoint 生命周期、恢复票据、SS ABI；替换「tunnel reconnect 只恢复新流」）；usage/broker-ops（expose/proxy 升级期行为、resume grace 与公网端口占用）；simcluster README + expected-verdicts；gotchas；tasklist 仅据 P29 同候选 raw receipts 勾 B-2/D-1..D-6 对应子项；D-2/D-4/D-6 的 broker 责任留 S6，不总勾。
**coverage**：plan §5 **行 4**（agent 层 + carrier 换代；broker 层臂 S6、infra 层臂 S9、各层贯穿 S10）、**行 5**；tasklist **B-2、D-1、D-2（agent 侧）、D-3、D-4、D-5、D-6**（owner；D-2/D-4/D-6 broker 侧子项在 S6 闭合）；plan §3.3 全段。
**risks**：ACK 取"carrier 已投递"→重放窗失效；Endpoint 与 fence 表不一致→双绑/端口丢失；agent 重建窗超 rehome dwell→leader 重铸公网口；yamux 窗口与流 ACK 互相背压死锁；nonce/半 chunk 未导出→静默断流。
**plan_lenses**：流协议/背压/ACK 语义 / tunnel 双端所有权、Endpoint 生命周期与围栏票据 / SS AEAD 状态与秘密 / agent 复原顺序与 drill 探针。
**review_lenses**：字节级反例（丢/重/乱序/half-close/慢接收）/ 死锁/背压/资源上限 / 安全与密码学（夺权/epoch/撤权、nonce、fuzz、秘密不落盘）/ drill 真实性与 legacy 互通。

---

### S6 · broker 映像交接（`broker-image`，XL）

**XL 不可拆理由（§-1 OQ1）**：single mode 与 1-voter 集群是同一个 broker 进程的两种持久化形态，资源图（tunnel Endpoint/listener/admin socket/SQLite 句柄）共用一份 `brokerimage`，只有 Raft/锁那一域按形态分支；拆成两个 stage 会让同一资源图被导出两次、drill 37 的业务 oracle 跑两遍。

**goal**：broker 整体同 PID 可恢复 exec：控制/隧道/持久化分立生命周期；tunnel Server 的 Endpoint 表/公网 listener/pubConn/流 replay/fence 表导出复原（listener FD 跨 exec，升级窗内 agent 重拨落 backlog）；data-dir flock FD 继承；Raft/SQLite 有序关闭→exec→重开（单 writer、不回旧快照）；auth_callout 黑窗实测；**N=1 有产品路径**；ctl 读类/稳定 ID 动词有界重试；在途传输 `deferred(transfers_in_flight)` 诚实过渡。

**scope_in**
- 新包 `internal/brokerimage`：资源图（tunnel 域、SQLite 句柄域、Raft 域、admin socket、metrics/manifest/sub-http listener、**`${DataDir}/tether.lock` flock FD 白名单继承**）；freeze 顺序（停 Accept 不关 listener→排空 in-flight handleAgent→冻结流 owner→导出）；导入失败 → 保 FD 再派发回 prev（复用 S3 的 handoff 再派发 API）。
- `internal/broker/reexec.go` + `broker.go` Run：**exec 在 Run 内部的显式出口执行，先 `Node.Shutdown()`（`node.go:523` 一次性关 raft/transport/store/ro/db）再 `handoff.Exec`，Run 的 defer 链（含 `defer release()` 的锁释放）永不跑到**；候选预检永不打开 store/不跑 migration（bbolt flock 同 PID 二次 open 会挂死，负控制断言"第二次 open 在有界时间内不返回"）；SQLite WAL checkpoint→close→reopen；断言 applied_index 单调、已 ACK session-create 可读；引用 `RecoverToSelfOnline` 先例定义顺序与断言点。
- **N=1（§-1 OQ1 裁决 (b)）**：两种形态都必须可热更新。**single mode（install.sh 默认渲染、现网 racknerd 的实际形态）是主 lane**：`requestReExec` 在 single mode 接线（今天返回 `cluster_not_enabled`），SQLite 由 `serve.go` 打开的路径走 checkpoint→close→exec→reopen，无 Raft 参与，`broker upgrade reload` and `broker upgrade status` are the single-mode broker entries; `node upgrade` remains agent-only (S6 lead L3)；**1-voter 集群模式**：`handleBrokerUpgradeReload` 的 leader 门对 `voters==1` 改走同 PID 有界交接（写停顿计入 ≤5s），`clusterupgrade/plan.go` 为 N=1 增加诚实标志（类似 N2WriteFence）；N≥3 保持 transfer-leader-first。drill 37 三 lane：single / 1-voter / N=3。
- `cmd/tether/serve.go`（只加调用）：pre-Cobra Restore（serve argv 形状）。
- `internal/tunnel`：Server Freeze/Export/Import（sessions/Endpoint、`killGen*/forgotten/closedAllocation`、inflight==0 证明；regLog damper 可丢）。
- 控制面：NATS 重订阅顺序 **auth_callout 最先**；实测 freeze→exec→callout 就绪黑窗；`internal/cli` 共享有界重试 helper（幂等只读 `ps`/`node ls` / 带稳定 ID 动词 → 有界退避；非幂等无 ID → 明确报错），drill 探针混入读动词并分别计数 CONNECT 失败与请求失败。
- 集群：proxy reaper/auto-rebalance 对「计划内升级中的 home」不 rehome（维护态）；agent `connected_broker_id`/silence 逃逸不把 broker 升级窗当 rehome 依据（回归）。
- 预检：tracker 有在途传输 → `deferred(transfers_in_flight)`（S7 移除），带负控制臂。

**scope_out**：tracker/watchdog/GC 导出（S7）；事务状态机/两代限额（S8）；NATS/Caddy（S9）。

**entry**：S5 出口（Endpoint 可导出形状、flow 两端）；S3 的再派发 API；S1 的 ctl 连接级重试。

**exit**
1. `test/clusterharness` 真 Raft+SQLite（N=1 与 N=3）：exec 前已 ACK 写在 exec 后可读；exec 全程并发 `AcquireDataDirLock` 始终失败（把 exec 挪回 Run 之后 → 探针抢到锁 → 红）；applied_index 单调；回滚不恢复旧 DB 快照；候选双开挂死断言。
2. 真 tunnel：公网 TCP 客户端字节跨 broker exec 连续（agent carrier 重连续接）；导出期落下的 CloseProxy 在复原后仍生效（fence 表漏导出→变异红）；公网端口零 refused（listener 继承）。
3. auth_callout 黑窗实测值写 evidence；exec 窗口内 `node ls`/`ps`/`exec` 经 helper 全部 ≤5s 完成、无重复副作用（hermetic + drill）。
4. N=1 reload 不再 `CodeIsLeader`（hermetic 正控制）；`TestReloadRefusesOnLeader` 改为 voters>1 语义。
5. **drill 37 GREEN**：臂 A（N=1 **single mode**，现网形态）、臂 A′（N=1 1-voter 集群）与臂 B（N=3 leader transfer 后逐个 follower/原 leader/原 home）：与 75 同一批原 SSH/TCP/SS 流 + 运行中 reliable exec + 10/s 混合探针贯穿 broker 升级；原 socket 不重建、字节零缺失、新请求终态失败=0、运行 SHA 变更、旧映像退出、不触发 rehome/rebalance；「有在途传输→deferred 且原传输完成」负控制臂；导入失败回滚臂；tether-prev 旧 reexec（Run unwind）→ 红。drill 30 只作参考回归、不进出口。
6. 三硬闸 + gates；broker 文件 =70、方法 ≤285、代码行 <18000（新逻辑在 brokerimage/tunnel/cli）；Darwin。

**tests**：brokerimage 资源图/锁继承/store 顺序（fake clock + sync observer 注入点）；cluster 锁继承 + 双开负控制；tunnel Server 导出导入；cli 重试 helper 表驱动；`test/p13` broker_restart 扩为 same-PID exec 版；broker 维护态/reaper 回归；callout 黑窗测试。
**drills**：37-hot-update-broker（新，三 lane，≈15 min）；30 参考（可选）。
**docs**：architecture §3/§21.3/§22（broker 资源图、listener 继承、单 writer、锁继承、维护态、N=1 写停顿边界、single mode 裁定）；broker-ops / cluster-runbook（reload 新语义、N=1 前提、deferred 处置）；simcluster README + expected-verdicts；tasklist 勾 E-4、D-2/D-4/D-6 broker 侧。
**coverage**：tasklist **E-4**（owner）；D-2/D-4/D-6 broker 侧子项；plan §5 **行 8** store 半、行 4 broker 层臂；plan §2 表「broker reexec 先取消 Run」行、§3.4 单 writer 段；B-4 broker 半。
**risks**：Run defer 链在 unwind 时关闭导出 FD/释放锁；N=3 下 reaper 把升级中 home 判 unreachable→rehome；auth_callout 黑窗被算成"请求失败"训练人抬预算；结构预算撞墙诱发把逻辑塞进 broker 包。
**plan_lenses**：broker 生命周期分立与 Run 出口 / 持久化单 writer 与锁（Raft/SQLite/JS/flock）/ 集群面（leader transfer、维护态、N=1 门改造、single mode 裁定）/ 控制面黑窗与 ctl 重试 + drill 37 双 lane。
**review_lenses**：FD/所有权与 listener 继承反例 / 单 writer/崩溃窗/双开 / 集群 rehome/围栏竞态 / ≤5s 与黑窗测量诚实性。

---

### S7 · 传输连续性（`transfer-continuation`，XL）

**goal**：同一用户调用、同一 transfer ID 贯穿 agent 升级、broker 升级与 NATS/carrier 重连：对象版本固定（NUID）、分块 stream-seq 收据、agent 传输 worker 状态与终态 courier、broker tracker/watchdog/commit/GC/reaper 责任随事务交接、prepare/commit/finalize 幂等 + `transfer-status` 查询；SHA/长度完整、一次原子 rename、审计唯一终态；≥66,549,440B 与 1GiB 双向并行贯穿逐层升级。

**XL 不可拆理由**：同 transferID oracle 需要 ctl writer、agent worker checkpoint、broker tracker 三端同一版本同时落地；tracker 搬包与 Export/Import 不可分；任一端缺失都没有可判定的连续性收据。

**scope_in**
- `internal/jsstream/object_writer.go`（新，ctl 与 agent pull 共用）：prepare 时固定 NUID（`PushPrepareResp/PullPrepareResp` 加 omitempty `ObjectVersion`）；chunk 发布带 `Nats-Msg-Id`（窗内二次保护）；**以 idx→stream seq 收据为唯一续传依据**；meta 最后写且 wire 与 SDK 相同（N-1 读端 `Get` 可读）；broker 在 `ensureXferBucket` 后对 `OBJ_<bucket>` `UpdateStream` 设 Duplicates ≥ 恢复地平线（ACL 放行 `$JS.API.STREAM.UPDATE` 并登记双向账本；旧 bucket 记限制）。
- `object_reader.go`：按最后消费 chunk 的 stream seq 续读；sha256 状态 Marshal/Unmarshal；取消仍 UNSUB+PONG 排空；同名新 Put 不被采纳为续传。
- 新包 `internal/xferworker`（agent）：prepare cache（boot 期限）、在途接收 checkpoint（tmp FD + 父目录 FD + offset + hash 状态 + next seq + NUID/size/sha）、在途 pull 发送状态、终态 courier（`transfer-terminal` verb 带 reply；N-1 broker 退回 fire-and-forget+重发去重）；作为 S2 ABI 资源。
- 新包 `internal/xfertrack`：`transferTracker/transferEntry/watchdog` 从 `internal/broker/transfer.go` **整段搬出（注释整段搬运）**，加 Export/Import（immutable 字段 + committed + startedAt + 剩余预算 boot clock）；导入条目视为 live（`finalizeStrandedXfers/ledgerProtectedObjects`）；`deleteXferObject` 只由 finalizer 触发；`claimFinalize` CAS 与 `xfer_inflight` 精确终态优先规则原样保留；`transfer_unknown` 不得因交接出现。
- `internal/broker/transfer.go` 薄层：prepare/commit/finalize 幂等；`transfer-status` 三态 verb；ACL 双向账本 + wire 错误码↔exit class 表登记。
- `cmd/tether/transfer.go`：push/pull 用可续传 writer/reader；重连后先 status 再决定；`commit_lost` 改同 ID 重试；同一调用绝不第二个 transferID。
- 预检移除 `transfers_in_flight`（agent 与 broker 两侧）。
- 崩溃边界如实：计划内 freeze 证连续；kill-9 落在 rename 与终态之间沿用 ledger/outbox 语义，文档写清不属热更新承诺。
- B-6：校核 simcluster 真实 Caddy/WSS 供给（本 stage 首个必须 WSS 的 drill）。

**scope_out**：真实 nats-server 二进制切换（S9；本 stage 用嵌入式 server 重启证 JS 重连续传）；跨纪元 schema 拒绝（S8）。

**entry**：S6 出口（tracker 作为 brokerimage 资源导入）；S2 agent ABI；S1 CallID/私有 reply 模式。

**exit**
1. jsstream：嵌入 nats-server 同 store_dir 重启中 Put/Get 续传，NUID 不变、chunk 无重复、SHA 一致；负控制：关掉 seq 收据 → 重复/缺块红；Duplicates 改回默认后超窗重发仍不产生重复 chunk（靠 seq 收据）。
2. xferworker：同 PID exec 携真实 tmp/父目录 FD/hash 状态续写并一次 rename；courier 在 broker ACK 前重发、ACK 后停止。
3. xfertrack + broker：导出/导入后 watchdog 剩余预算正确；禁用导入 → reaper 合成 failed → 红；prepare/commit/finalize 重放各恰一终态；status 三态；broker 代码行因搬迁**下降**（golden 可 -update 收紧）。
4. cli_e2e 真 binary push/pull tier A/B，在 Put/Get/commit/finalize 前后分别做 agent exec、broker exec、NATS 重启，同 transferID 完成、审计单终态、无半文件。
5. **drill 63 GREEN**（Caddy/WSS profile）：≥4 session/bucket、2 push + 2 pull、66,549,440B 与 1GiB 双向并行 + tier A，逐层 agent→broker（N=1 与 N=3）→回滚，一个取消、一个撤权，慢消费者臂用**预登记生产限速预算**；**非空性守卫**：升级触发前探针证明 transferID live 且已确认块 < 总块，否则记 `not_covered` 而非 GREEN；每 10s 持久进展；同 transferID、SHA/长度、审计单终态、无半文件；负控制臂（丢 checkpoint/错 offset/先 failed 后 complete → 红）。61 回归 GREEN。
6. 三硬闸 + gates；wire inventory 只增；xferworker/xfertrack 在 -race 矩阵；Darwin。

**tests**：如上 + `test/security` transfer-status/terminal ACL 双向；`internal/broker/xfer_inflight` 与导入条目交互。
**drills**：63-hot-update-transfer（新，≈10 min）；61 回归（≈4 min）。
**docs**：architecture §9/§22（对象版本固定、checkpoint、courier、tracker 交接、幂等 verb、崩溃边界；逐项替换旧 hard-cutover/rehome 在途不保条款，保留非计划故障边界）；usage.md（push/pull 升级期、status）；broker-ops（reaper/ledger 与导入条目）；testing-standards（JS 重启型测试）；simcluster README；tasklist 勾 B-3、B-6、E-1/2/3/5/6。
**coverage**：plan §5 **行 6**、行 8 tracker/GC 半、§5.1 transfer 写/sync/rename/终态窗；tasklist **B-3、B-6、E-1、E-2、E-3、E-5、E-6**（owner）；plan §3.4 传输段、§2 表「Put 每次新 NUID」「xfer_inflight 合成 home_broker_restart」、§10 裁决「Put 同 name」「只保 object 不保 tracker」。
**risks**：用 SDK Put 重试冒充续传（头号红线）；`ev.transfer` fire-and-forget 在 exec 窗丢失→先 failed 后 complete；搬包漏 committed/markReaped/claimFinalize CAS→双 finalize；1GiB 在 88 vCPU 宿主上升级前就完成（非空性守卫）。
**plan_lenses**：对象版本与可续传 writer/reader（NUID/seq 收据/去重窗）/ agent worker 状态与终态 courier / broker tracker/GC 事务交接与幂等 verb / ctl 续接、唯一终态呈现与 drill 63 非空性设计。
**review_lenses**：终态唯一性反例（先 failed 后 complete、双 rename、reaper 误收、双 finalize）/ 崩溃窗与 ledger/outbox 一致 / JS/NATS 重连、慢消费者、去重窗边界 / 大文件 drill 证据与预算。

---

### S8 · 升级事务、回滚、N-1 与首次迁入（`upgrade-orchestration`，XL）

**goal**：升级成为可查询事务：`requested→staged→ready→activating→serving_new/migrating→completed | deferred/rejected/rolled_back/rollback_blocked`，三个完成点分开、运行 SHA 证明；候选预检全集、升级域/共址/共享 binary sibling/两代限额；逐交接窗口故障注入（旧 owner 在/亡两种语义）；接管前/部分迁移后/新任务执行后回滚无双执行不丢任务；真实 v0.6.0 artifact N-1 矩阵与跨纪元拒绝；legacy busy 安全拒绝 + defer + 显式 `--interrupting`；no-systemd/user unit/共址；A-6 正式冻结。

**XL 不可拆理由**：故障注入矩阵只有在全部资源域都可迁移后才有意义；事务状态必须跨 agent/broker 一次定义（S9 复用）；回滚三时点依赖传输资源（S7）。若拆 N-1/legacy 出去，两半都要跑 31/33 回归与 tether-prev 供给。

**scope_in**
- 新包 `internal/upgradetx`：状态机；**节点本地持久**（agent marker v2 additive 字段 / broker datadir `upgrade-state.json` tmp+rename+fsync）+ additive 查询 verb 与 register/ClusterHealth omitempty 字段；**零新 Raft op、零 SQLite migration、commandVersion 不动**（`fsm.go` poison-skip 会在旧副本静默吞新 op）；三完成点；deferred 原因码汇总；两代限额（ABI 仅相邻一代可导入 + 集群同时 ≤2 release）；运行 SHA 证明（`/proc/self/exe`）；新枚举登记进 15 家族表（或 string 常量）。
- `internal/agent/upgrade*.go`、`internal/broker/upgrade.go`、`cmd/tether/cluster_upgrade_drive.go`、`node upgrade --wait`：薄接线；候选预检全集（schema/能力/限额/legacy-wire 会话/spawnsafe/资源不足→defer）；升级域 flock、sibling 拒绝、共址 agent ReExecOnly 纳入事务。
- 故障注入（测试构建 hook 只在 hermetic；drill 用真实手段）：artifact/smoke、ready、停收、freeze 前后、snapshot、FD 发出未 ACK、ACK 未持久、激活决议、首个新请求、旧 FD 回收、reap 未落盘、退场 × candidate 退出/断连或截断/超时/磁盘满或只读/慢消费者/撤权/NATS 断/leader 切换/并发升级；每臂预先声明连续/可恢复/明确失败/未知；旧 owner 在→恢复、已死→unknown/error 不伪造成功；handoff 层校验失败崩溃窗进表。
- 回滚三时点：不重跑已执行请求、不抛弃新代接纳的工作、以当前态重新导出、不恢复旧 SQLite 快照；跨纪元/未知 manifest 字段/不可逆 schema 激活前拒绝；回滚到 N+1 导出的 bundle 需 prev 能导入——不支持→`rollback_blocked` 保业务。
- 真实 N-1：`vendor/tether-prev`（S1 供给）做 wire 四象限（exec/run/kill/expose/transfer）+ 首次迁入；S2 归档 gen-1 做 ABI 代际机制臂（标注非 release N-1）；wire/manager ABI/manifest/DB/NATS/Caddy 配置 N-1 矩阵总表。
- **legacy 首次迁入（§4-OQ6）**：broker 侧探测 ReleaseVersion/能力位；busy → `legacy_busy` 拒绝不伤业务；idle → **仍 defer**（v0.6.0 无 quiescence 能力，且 ctl 直发 agent subject 使 broker 侧接纳冻结不可证明）；显式 `node upgrade --interrupting` 才走既有 §21.3 路径，事务记 `legacy_interrupting`（非热，不计 GREEN）；capable→capable 的 idle 迁入（先禁新接纳→完整闲置证明→保持到激活）用 gen-N artifact 证明，附「idle 观察后并发新任务被 fence 拒绝」负控制；capable→legacy 回滚明确拒绝或同样需 `--interrupting`。
- **session authority incarnation（S4 定稿裁决 L1 移交）**：S4 证明了 session_destroyed 在事件 / Freeze 前后 / Take 后 Activate 前 /
  active / register-gone 五窗口的撤权，但「finalPrepare 之后丢了删除事件、且同 SID 已被硬删重建」时新映像的首个 register 会成功——
  session 没有不可复用的 incarnation，本地无从判别。S8 给 session 加不可复用 incarnation（从既有 session 行的创建身份派生、不新增
  SQLite migration——与本 stage「零 migration」约束一致；register/lease 上是 additive omitempty 字段），agent 在 image 里携带 cut 时的 incarnation，register 发现不一致即按撤权处理；`TestPTYSessionRecreationBoundary`
  （S4 记录现象）在 S8 改为要求拒绝旧 incarnation 的回归测试；C-10 PTY 变体在此勾选。
- **A-6 正式冻结**：以完整 profile（4 TCP/SSH、2 PTY、4 进程、4 tier-B、SS、remote-fs）跑稳定负载校准，冻结 `internal/upgradebudget` 数值并记环境/资源；不为失败样本抬门。
- 机械断言：`git diff` 不触及 `internal/cluster/command.go` 的 OpType 列表与 `internal/storage` migrations。

**scope_out**：NATS/Caddy 组件进入事务（S9）；五代混载（S10）。

**entry**：S1–S7 全部域可迁移；tether-prev 与 gen-1 归档件。

**exit**
1. upgradetx 表驱动：全部合法转换、非法拒绝、两代限额、损坏文件 fail-closed、旧 marker 零值合法；`TestCompletedRequiresRunningImageProof`（假 SHA→红）；机械断言 Raft op/schema 无 diff。
2. 故障注入矩阵每臂有测试且与预声明一致；至少一臂证明「旧 owner 已死不写成自动回旧」；变异：双代激活/只换磁盘不换映像/永留旧 worker/漏记录新请求 → 各红。
3. 真实 CLI 回滚三臂无双执行/丢任务/假成功；`TestRollbackExportsCurrentStateNotStaleSnapshot`。
4. **drill 38 GREEN**（逐窗口注入 + 三时点回滚 + 共址/sibling/两代 + no-systemd/user unit + 混载）；**drill 39**：v0.6.0 真实四象限互通 GREEN；legacy busy 拒绝 GREEN（业务不损）、idle → deferred（记安全拒绝）、`--interrupting` 记非热、capable→capable idle 迁入 GREEN + 并发新任务负控制红、capable→legacy 明确拒绝；31/33 回归不劣化。
5. A-6 校准报告 + 冻结常量落盘；三硬闸 + gates；CLI golden/wire inventory 带理由。

**tests**：如上 + `test/cli_e2e` 驱动器黑盒；预检拒绝表；域锁/sibling；legacy fence 单测。
**drills**：38-hot-update-rollback-compat（新，≈12 min）、39-hot-update-legacy-n1（新，≈6 min）、31/33 回归（≈8 min）。
**docs**：requirements §6.8（事务/完成点/首次迁入措辞校准，WHAT 层）；architecture §21.3 状态机 v2 + §22 事务/回滚窗/N-1 矩阵表/两代限额；usage/broker-ops/cluster-runbook（状态查询、deferred/rejected/rollback_blocked 处置、`--interrupting` 一次性入口、无隐藏手工步骤）；gotchas；simcluster README；tasklist 勾 A-6、B-4、F-1..F-7。
**coverage**：plan §5 **行 11**、§5.1 全窗（transfer 窗引用 S7）、§7 冻结、§3.6/§1.2/§6 第 3 项、§10 后四行；tasklist **A-6、B-4、F-1、F-2、F-3、F-4、F-5、F-6、F-7**（owner）；C-4 的「旧 ctl 能力不足激活拒绝」总表；C-9 no-systemd drill 半。
**risks**：事务状态做成 Raft op/schema→毁滚动前提；legacy 用假 idle 判据放行竞态；回滚倒回旧快照抹掉已 ACK 写；注入臂靠 fixture 重连/手搬状态制造绿。
**plan_lenses**：事务/状态机/持久化与可观测（三完成点、运行 SHA、旧 owner 计数）/ 故障注入与回滚窗语义 / N-1/ABI/纪元矩阵与 legacy 首次迁入 / 部署形态（systemd/user/nohup/共址/共享 binary）与 A-6 校准。
**review_lenses**：非法转换与假 completed 反例 / 双执行/丢任务/回滚抹写 / N-1 真实性（tag 构建、SHA、不用 tether-next 冒充）/ 文档一致性（状态词汇、无隐藏步骤）。

---

### S9 · NATS/Caddy 产品路径与拓扑（`infra-nats-caddy`，XL）

**goal**：外置 nats-server/JetStream 与 Caddy 的**产品**升级/回滚路径（非 harness 代劳）：N≥3 逐节点 LDM/readiness/JS 副本/运行 SHA；**N=1 可验证本机交接路径实际实现**；N=2/quorum 不足/replica 落后 fail-closed；拒双开同一 JS store；临时节点不绕 join 门；Caddy 配置 reload 与 binary 换代分臂、内部 WSS 主动迁移、新入口进程自身可升级并退出；真实相邻版本 artifact。

**XL 不可拆理由**：NATS 与 Caddy 共用 `internal/infraupgrade`、同一特权面模型、同一 `install.sh` retrofit 与 unit 模板、同一 drill 供给与「旧进程退出 + 运行 SHA 变更 + 业务连续」oracle；拆开会让特权面被定义两次。

**scope_in**
- 新包 `internal/infraupgrade`：artifact 校验（版本允许表与 `install.sh` pin 单源；nats 用**上一 patch release 作旧、仓库 pin 作新**，回滚反向，终态回到 `nats_server_pin` 三方一致；Caddy 用固定 2.7.6 与相邻版本，checksum 钉死）；readiness 探针（JS meta leader 不在本机、stream/consumer replicas current、quorum、auth_callout 就绪、WSS 握手、varz 版本/SHA）；LDM 触发与每步 barrier；事务上报（复用 S8 组件类型 nats/caddy）；四层依赖顺序。
- NATS N≥3：staged binary 校验（`--version`、`-t`）→ readiness → `--signal ldm` → 客户端迁移（S1/S5/S7 续接层）→ `--signal stop` → `Restart=always` 拉起新 binary → `/proc/<pid>/exe` SHA 复核；readiness 不达标保旧服务；进入 LDM 后失败停止后续滚动并证明仍有健康路径。
- **NATS N=1（§4-OQ2）**：主路径 = `--signal stop` + `Restart=always RestartSec=2` 原地重启（**不用 LDM**，其 2 分钟默认会把黑窗拉到分钟级），停顿由续接层吸收，10/s 注入实测 ≤5s / 传输 10s 进展；JS R1 store 内在途对象/审计不丢（GetInfo NUID 不变、reader 续读）；**临时 sibling/JS R1→R2 迁移**只在实测超预算后作为增补，且必须先做 spike 证明 standalone JS→clustered meta 可无损（`cluster-runbook.md §3a` + `test/d9 GrowInPlaceOrphansStreams` 已证会孤儿化流）。
- N=2/quorum 不足/replica 落后 → defer（记安全拒绝，不冲抵 N=1 成功臂）；同一 JS 目录双开拒绝；临时 broker 受 join 精确 release 门约束。
- Caddy：config reload（localhost admin API）与 binary 换代分臂；**unit 变更（§4-OQ3）**：`caddy.service` 现为 root + `Restart=on-failure`（graceful stop exit 0 不会被拉起）→ 改 `Restart=always` + `User=tether` + `AmbientCapabilities=CAP_NET_BIND_SERVICE`（或 sudoers/polkit 二选一），带存量 retrofit；切换前预热新 TLS/WSS 路径（临时第二入口：SO_REUSEPORT 同址或 alt-port 的产品管理 transient unit，二选一在 S9 plan spike 后定）→ 内部 WSS 主动迁移（broker 推送入口提示 / connect_urls）→ 旧 Caddy graceful stop → 新 binary 起 → 迁回 → 临时入口退出；坏证书/坏配置拒绝不伤业务；ACME 数据目录/证书锁竞争与 `/sub/*` 路由顺序在 plan 里裁定。
- `scripts/install.sh`：unit 模板/特权面渲染（含 retrofit）；`nats_server_pin`/`nats_ping_defaults` 门保持单源；`sh test/simcluster/tests/lint-install` 绿。
- `cmd/tether`：`cluster upgrade --component nats|caddy`（既有文件内加 flag，逻辑全在新包；CLI golden 带理由）；broker admin socket 上的 readiness/JS 副本报告（包级函数）。
- simcluster 供给：镜像携带两版 nats-server 与两版 Caddy（SHA 钉死）；WSS/tunnel 地址建模核验（agentyaml 正负例）；B-6 复核。

**scope_out**：五代/混载全栈（S10）；现网部署（不在范围）。

**entry**：S8 事务状态机；S1/S5/S7 续接层；S6 broker 维护态；§4-OQ2/OQ3 已裁定。

**exit**
1. infraupgrade 表驱动：readiness fail-closed、顺序、LDM 后不假设旧服务、双开拒绝、joiner 版本门、每步失败回滚（继任未就绪不 LDM 旧；旧未退出不 completed）各有正负控制。
2. 嵌入 nats-server：LDM 下 publish/reply 丢 ACK、consumer 迁移、auth_callout 重订阅顺序、副本不足；JS 503 期间传输 checkpoint 等待而非失败。
3. **drill 85 GREEN**：N=3 真实 nats-server 升级+回滚（原 SSH/TCP/PTY/exec/transfer 持续、10/s 逐请求成功、旧 server 退出、运行 SHA 变更、每步副本 current）；N=1 原地重启臂 GREEN 且预算内；N=2/quorum 不足/replica 落后臂 = deferred（记拒绝）；负控制：只换磁盘不换运行进程 → 红；旧 server 未退出却 completed → 红。
4. **drill 86 GREEN**：Caddy reload 臂、binary 臂（原业务 socket 不重建、内部 WSS 迁移完成可观测、新 TLS 握手持续成功、旧 Caddy 退出才 completed、临时入口退出）；坏证书/配置负控制；永留旧 Caddy → 红。
5. 32/73/98 回归不劣化；三硬闸 + gates；`nats_server_pin` 三方一致门终态绿；Darwin。

**tests**：如上 + install.sh 渲染/retrofit 单元与 unit 对账（新增门带正负控制）；simcluster hermetic lint-install/lint-drills 覆盖新脚本。
**drills**：85-hot-update-nats（新，N3/N1/N2 臂，≈12 min）、86-hot-update-caddy（新，≈8 min）、32/73/98 回归（≈10 min）。
**docs**：broker-ops（infra 升级操作/前提/特权面/N=1 方案/deferred 处置）；cluster-runbook（N≥3/N=1/N=2 行为矩阵）；architecture §1/§3.5/§6/§22；install.sh 注释；gotchas；simcluster README（供给、地址建模）；devices.md 仅当真实资源变化；tasklist 勾 G-1..G-6。
**coverage**：plan §5 **行 9、行 10**、行 4 infra 层臂；plan §3.5 全段、§6 WSS 地址建模、§10「原 socket 与内部 WSS」「N=3 不代替 N=1」；tasklist **G-1、G-2、G-3、G-4、G-5、G-6**（owner）；B-6 复核。
**risks**：特权面缺失→只能靠 harness 代劳（Mandate 假绿）；N=1 JS 恢复超预算被抬门或 fixture 掩盖；LDM 顺序错一步（callout 随旧 server 走）整集群 503；临时入口跑在 unit cgroup 外、重启主机后回旧 binary。
**plan_lenses**：NATS/JS 运维语义（LDM/副本/quorum/双开/auth_callout）/ Caddy 入口生命周期（预热、迁移、退出、证书、ACME 锁）/ 特权面与 install.sh retrofit（最小、可审计）/ N=1/N=2 拓扑与预算 + drill 供给。
**review_lenses**：旧进程退出/运行 SHA 反例 / 业务连续 oracle（内部重连≠业务中断）/ 权限最小化与凭据 / drill 供给真实性（真实相邻版本、不把 hermetic 当部署）。

---

### S10 · 全栈验收、文档与最终内审（`fullstack-acceptance`，L）

**goal**：真实 CLI 入口完整四层升级再回滚，持续混合业务，≥5 代换代（每代真实不同运行 SHA），资源回基线；预算 p50/p95/p99/max；§5.1 变异逐条红；deferred 集合为空；每能力 Test/drill/预算/身份/命令/rc/收据/活文档映射齐全；固定三路内审并修复；停外审门。只修 bug 与整合。

**scope_in**
- drill 99：N=1 与 N=3 各一 lane；agent→broker→NATS→Caddy 顺序升级、逆序回滚、再升级共 ≥5 代；2 agent、4 条原双向 TCP/SSH、2 PTY、4 普通进程、4 tier-B（4 独立 session/bucket）、SS 与 remote-fs 独立臂、10/s 混合请求；逐代读资源基线（goroutine/FD/进程/consumer/temp/旧映像/锁）；先跑无升级校准臂；soak 臂复用 97 形状。
- 变异清单执行（overlay 或真改磁盘再还原，各留原 rc）：恢复旧 reexec、丢 checkpoint、错 offset、提前关原 socket、漏记录新请求、双代激活、只换磁盘不换运行映像、永留旧 worker、假运行 SHA、漏探针、双 owner。
- **deferred 集合断言**：在全客户端为当前版本的混载 profile 下集合为空；`legacy_session_busy` 是唯一被文档化保留的原因且有 39 臂证明不伤业务。
- 文档总同步（requirements §6.8 改为实现事实 + 未覆盖边界；architecture §22 定稿并逐项替换 §21 hard-cutover、保留非计划故障边界；usage/broker-ops/cluster-runbook/testing-standards/test README/simcluster README/coverage inventory）；tasklist 全勾选附收据；evidence 最终候选 HEAD+dirty 摘要与镜像/vendor SHA、无 NOT_RUN；INDEX 追加（内审通过≠外审 Pass）；过程文档归档判据。
- 最终同一候选：`make test` + 完整 `make e2e-parallel` + 单份 `make lint` + `make gates`；全套 `./run-drills.sh` + `--live-grow` 一轮（预计 70–90 min，与 go test 严格串行）；`git add -A`（排除运行产物/凭据）；固定三路内审（任务/传输、连接/基础服务、身份/状态机/测试文档）+ 部署证据审计；报告 `docs/reviews/hot-update-fullstack-acceptance-review.md` 首行 Pass/Fail；不 commit、不 push、不部署现网。

**scope_out**：外审本身；现网车队；任何新能力。

**entry**：S0–S9 全部出口断言仍成立。

**exit**
1. drill 99 两 lane GREEN：≥5 代、零重启/重连/EOF/缺失重复字节/重复副作用/矛盾终态；所有旧映像/owner/consumer/temp/锁退出；资源回基线；预算表齐全无临时放宽。
2. 变异每条红（原 rc）；deferred 集合断言绿。
3. plan §5 全部 12 行与 tasklist A–H 全部项均有映射；evidence 无 NOT_RUN。
4. 三硬闸 + gates 绿；全套 drill 判词 = expected-verdicts；inventory/golden 只增、ledger 只减、命名门/docs_layout 门绿。
5. 内审报告首行 Pass，主进程逐条回复；全部暂存、无未跟踪、未 commit。

**tests**：`test/hotupdate`：`TestFiveGenerationResourceBaseline`（五轮跨代资源门）、`TestMixedWorkloadSurvivesFullStackRollout`（hermetic 缩比）；变异 overlay 集；全量闸门。
**drills**：99-hot-update-fullstack（新，≈25 min）；34/36/75/37/63/38/39/85/86 终态复跑 + 全套 + `--live-grow`。
**docs**：见 scope_in。
**coverage**：plan §5 **行 12**、行 4「各层贯穿」合取、行 7 混载臂；§5.1 变异、§7 全部预算、§8、§9；tasklist **H-1..H-10**（owner；H-6/H-7 每 stage 已重复）。
**risks**：五代 drill 时长与本机资源（与任何 go test 串行、两 lane 各独占 INSTANCE/卷/端口）；为失败样本抬预算或筛样本；文档把设计写成已实现（A3）。
**plan_lenses**：全栈编排与代际（顺序、锁依赖、五代、资源基线）/ 预算与性能采样 / 变异与证据纪律 / 文档-台账-闸门一致性。
**review_lenses**：任务/进程/PTY 域 / 连接/传输/基础服务域 / 身份/状态机/兼容 / 测试与文档诚实性 + 部署证据审计（镜像身份、判词、四条日志流）。

---

## §2 覆盖对账表

### 2.1 requirements §6.8 逐句

| # | 原文（摘要） | owner stage（贡献） |
|---|---|---|
| R1 | 计划升级须覆盖 agent、broker、外置 NATS、Caddy 及新增运行层 | agent S3；broker S6；NATS/Caddy S9；运行层自身 S3（gen1→2→3）+ S8（两代限额） |
| R2 | 升级期间原有业务进程、PTY/exec 输入输出与退出状态、文件传输、SSH/TCP/proxy 连接继续工作 | 进程/exec S3；PTY S4；SSH/TCP/proxy(SS) S5；传输 S7 |
| R3 | 同时持续服务新控制请求 | S1（ctl 可靠 + 连接级重试）；S3（agent 窗口探针）；S6（broker 窗口 + 读动词 helper + 黑窗）；S9（infra 窗口） |
| R4 | 用户无需重启业务、重新连接或重新提交传输 | S3/S4（不重启）；S5（不重连）；S7（不重提交） |
| R5 | 字节不丢不重，命令不重复执行，文件保持一次原子提交和唯一终态 | S2/S5（字节）；S1（命令不重复）；S7（原子提交/唯一终态） |
| R6 | 权限撤销、取消、ps/kill 和可观测性在升级期间仍有效 | S1（session_destroyed/kill/ps）；S3（tombstone 导入）；S5（迁移中撤权）；S7（取消）；S8（事务可观测） |
| R7 | 区分"已接管控制/活业务已迁移/所有组件完成" | S8 |
| R8 | 只有全部旧运行映像退出…才能报告完成；无限期保留旧 worker 不算完成 | S8（状态机与运行 SHA 证明）；S9（旧 nats/caddy 退出）；S10（五代退出） |
| R9 | 不兼容或资源不足时须在破坏业务前延期/拒绝并说明原因，不得强制杀掉活业务 | S3（deferred 原因码集合）；S8（预检全集/两代/跨纪元）；S9（N=2/quorum） |
| R10 | 连续性具有预先固定、可测试的延迟与背压预算 | S3（常量单源 + 首次样本）；S8（A-6 冻结）；S10（复核） |
| R11 | 允许内部承载连接更换，不能据此关闭原有业务连接 | S5（agent carrier）；S6（broker exec）；S9（NATS/Caddy carrier） |
| R12 | 同协议纪元/N-1、首次迁入、无 systemd、单节点与 HA 拓扑分别有明确能力和验证范围 | S8（N-1/首次迁入/no-systemd）；S6（N=1 broker）；S9（N=1/N=2/N≥3 nats、caddy） |
| R13 | 不能把 HA 的通过外推到单节点，或追溯声称旧版本已有热更新能力 | S6/S9（N=1 独立 lane）；S8（legacy 只证首次迁入） |
| R14 | 整机断电/重启和业务程序自身升级另属故障/应用契约 | S8（崩溃窗定义）；S10（文档边界） |
| R15 | 每项能力必须完成自动测试、真实 drill、失败/回滚验证、文档、原始证据 | 每 stage 出口仪式；S10 总收口 |
| R16 | 原 PID/原连接/同一传输、新请求连续性及旧版本实际退出都要独立验证 | S3（PID）；S5（连接）；S7（传输）；S1/S6（新请求）；S8/S10（退出） |

### 2.2 plan §5 逐行 + §5.1/§7/§8/§9

| 行 | 能力 | owner（贡献） |
|---|---|---|
| 1 | 唯一 owner/接纳 | unit/component 半 S1；drill 半 S3 |
| 2 | 普通进程/exec | S3（ABI S2） |
| 3 | PTY | S4 |
| 4 | SSH/TCP 既有流 | S5（agent 层 + carrier 换代）；S6（broker 层臂）；S9（infra 层臂）；S10（各层贯穿） |
| 5 | SS proxy | S5 |
| 6 | tier A/B push/pull | S7 |
| 7 | remote-fs/控制任务 | S3（预检/排空/wedged + 34 remote-fs 臂）；S10（混载臂） |
| 8 | broker/持久化 | S6（store/单 writer）；S7（tracker/GC 围栏） |
| 9 | NATS/JS | S9 |
| 10 | Caddy/入口 | S9 |
| 11 | 回滚/兼容/首次迁入 | S8 |
| 12 | 全栈/多代/清理 | S10 |
| §5.1 故障注入矩阵 | S8（全窗；transfer 窗 S7 以引用计入） |
| §5.1 变异/反例 | 各 stage 自带本域变异；S10 总执行 |
| §7 预算 | S3 常量；S8 冻结；S10 复核 |
| §8 文档硬闸 | 每 stage；S10 总同步 |
| §9 最终闸门与停点 | S10 |

### 2.3 tasklist 逐个未勾选项

| 项 | owner（贡献） |
|---|---|
| A-6 校准/artifact 清单/冻结预算 | **S8**（S3 首次样本；S10 复核） |
| B-2 SSH/TCP/PTY/SS 序号/无重连/half-close/静默预算 oracle | **S5**（PTY 子项 S4） |
| B-3 同 transferID/SHA/原子提交/唯一终态 oracle | **S7** |
| B-4 每请求新控制时延/副作用 + 旧运行映像退出 oracle | **S8**（新请求半 S3/S6） |
| B-5 真实旧 binary reexec 失败基线；确定性负控制；非空性守卫 | **S1**（tether-prev + 34 重钉；各 stage 自加负控制；非空性 S7） |
| B-6 校核 simcluster Caddy/WSS 供给与独立资源 | **S7**（S9 复核） |
| C-2 稳定任务/资源 ID、唯一 owner/epoch、已接纳请求与退出日志 | **S2**（S1 贡献） |
| C-3 版本化 manager 接口、完整资源所有权、候选无执行权、入口身份检查 | **S3**（S2 接口半） |
| C-4 ctl 稳定 requestID/幂等 + 输出 seq/ACK/resume；旧 ctl 激活拒绝 | **S4**（exec 半 S1；legacy_session_busy S3；总表 S8） |
| C-5 联合 starting/courier/register 准入屏障 | **S1** |
| C-6 去重身份/容量/保留期、跨代及回滚保留、过期拒绝、晚重试窗 | **S2**（S1 半） |
| C-7 同 PID 可恢复 exec、安全点、白名单 FD、状态校验（含子项） | **S3**（ABI 半 S2） |
| C-8 exec pipe、PTY、stdin/resize/KA、ps/kill、子树/退出码完整接线 | **S4** |
| C-9 runtime 自身可升级；systemd/user/no-systemd 生命周期与清场 | **S3**（S8 no-systemd drill 臂复核） |
| C-10 session_destroyed 清场缺口；exec/PTY/后代与删除/freeze 竞争；tombstone 不复活 | **S3**（exec 半 S1；PTY 变体 S4） |
| C-11 H1 收据齐全 | **S4** |
| D-1 stableID/offset/ACK/重放/FIN/限额 | **S5** |
| D-2 两端 raw socket 可交接、carrier 可替换、原外部 socket 不关 | **S5**（broker 端 S6） |
| D-3 OPEN 中/空闲/活跃/慢接收/half-close/多流/迁移中撤权 | **S5** |
| D-4 home/pins/port 与旧 epoch 围栏 | **S5**（fence 表导出 S6） |
| D-5 SS AEAD 状态/nonce/盐/replay filter、秘密、fuzz | **S5** |
| D-6 真协议测试、race/资源、真实 SSH/TCP/SS drill、退场证据、文档 | **S5**（broker 升级 drill S6） |
| E-1 稳定版本/NUID、prepare checkpoint、可信 temp/parent | **S7** |
| E-2 持久进度、摘要恢复、同操作续传、取消/预算、唯一 rename/终态 | **S7** |
| E-3 tracker/watchdog/commit/GC/reaper 同事务交接 | **S7** |
| E-4 Raft/SQLite/JS 单 writer、已 ACK 写不丢、不回旧快照 | **S6** |
| E-5 prepare/读写/sync/rename/终态前后、重复消息、权限撤销 | **S7** |
| E-6 ≥66,549,440B 与 1GiB、双向并行、WSS/慢消费者、逐层升级 drill | **S7** |
| F-1 可查询事务状态、三完成点、运行 SHA 证明 | **S8** |
| F-2 候选预检、升级域/共址/共享 binary sibling/两代限额 | **S8** |
| F-3 逐交接窗故障注入，旧 owner 在/亡语义 | **S8** |
| F-4 wire/manager ABI/manifest/DB/第三方配置 N-1 矩阵、跨纪元拒绝 | **S8** |
| F-5 接管前/部分迁移后/新任务后回滚 | **S8** |
| F-6 legacy busy 拒绝、idle 清单+冻结接纳、无能力则 defer、并发新任务反例 | **S8** |
| F-7 单测/真 binary/资源/真实 drill、状态与操作文档 | **S8** |
| G-1 NATS/JS 滚动路径、readiness、版本/SHA | **S9** |
| G-2 Caddy reload 与 binary 分别完成、新入口、WSS 迁移、旧映像退出 | **S9** |
| G-3 N≥3/共址、N=1、N=2/quorum 不足、replica 落后行为 | **S9**（N=1 broker S6） |
| G-4 单实例可用路径实际实现；defer 只记安全拒绝 | **S9**（broker 侧 S6） |
| G-5 无双开 store、临时节点不绕 join 门、新增入口可升级 | **S9** |
| G-6 外置 NATS/真实 Caddy 升回滚 drill、业务/新请求/资源证据、文档 | **S9** |
| H-1 四层升级/回滚、混合业务、≥5 代/资源回收 | **S10** |
| H-2 延迟/吞吐/FD/内存/consumer/磁盘限额，撤销/取消并发 | **S10** |
| H-3 变异全红 | **S10**（各 stage 自带） |
| H-4 每能力 Test/drill/预算/身份/命令/rc/收据/文档映射齐全 | **S10** |
| H-5 活文档同步 | **S10**（每 stage 增量） |
| H-6 test/wire/CLI/inventory 按规则更新，账本不放宽，新门有正负控制 | **S10**（S0 起每 stage） |
| H-7 同一最终候选三硬闸 + gates | **S10**（每 stage） |
| H-8 固定三路内审、逐条回复、复审收据 | **S10**（每 stage 各一轮） |
| H-9 最终报告 Pass/Fail、INDEX | **S10** |
| H-10 核验未提交/部署，停外审门 | **S10** |

---

## §3 贯穿纪律（cross-cutting）

1. **包布局与结构预算**：`internal/broker` 70 文件/285 方法/16000 量子（≈17.4k，余 ≈600 行）、`Agent` 126 方法/6000 量子（≈6.8k）、`cmd/tether` 57 文件/12000 量子（≈12.7k）全部钉死。新逻辑一律进新包（`agentimage`、`ptysession`、`flow`、`brokerimage`、`xferworker`、`xfertrack`、`upgradetx`、`infraupgrade`、`upgradebudget`、`jsstream/object_writer.go`）；broker/agent/cmd 只放包级 adapter 或既有文件内薄包装；需要空间时整段搬职责（`transferTracker→xfertrack`）并整段搬注释；golden 只许 `-update` 收紧，任何放宽手改并写 commit 理由；每 stage plan 预告新增行数与落点。
2. **N-1 wire 四象限**：一切 wire 变更 additive/omitempty/零值合法；新能力用新 verb/新 op 串；ProtoVersion 不 bump；**零新 Raft op、零 SQLite migration、commandVersion 不动**（FSM poison-skip 会在旧副本静默吞新 op）；事务状态节点本地；broker 转发用 raw overlay 保未知字段；wire inventory 只增并核对新增行数；`go test` 内 N-1 只用 legacy 模拟，真实 artifact 象限只在 simcluster 用 `vendor/tether-prev`（v0.6.0 tag 源码构建），`tether-next` 只证机制/运行 SHA 反例，S2 归档件标注为代际机制证明。ALPN：客户端提供全部仍支持版本、服务端无交集仍走 legacy；REGISTER 六字段与精确 OK 一字不动。
3. **同 PID exec 与 FD 纪律**：每个 FD 唯一 owner；dup 只经 `SyscallConn.Control`；导出前非白名单 FD 皆 CLOEXEC；秘密只进 sealed memfd，manifest JSON 只含范围；freeze 屏障含接纳/spawn/wait/signal/输入/resize/KA/cancel/reaper/accept/dial/commit；resource-active 与 control-ready 分开；回滚导出当前态；到不了安全点就 defer 保原业务，绝不 Close 业务 FD 或 kill 任务凑期限；broker exec 在 Run 内、defer 链前，先 `Node.Shutdown()`；data-dir flock FD 白名单继承。
4. **deferred 集合**：S3 定义原因码集合；每个后续 stage 出口的一条断言是「移除本 stage 承接的原因码」；S10 断言集合为空（`legacy_session_busy` 为唯一文档化保留项）。任何中间 stage 不得对不可迁移域"沿用旧行为"。
5. **Darwin**：memfd/pidfd/CLOCK_BOOTTIME/SCM_RIGHTS/ioctl 代码走 `_linux.go` + `_unsupported.go` 返回 `ErrUnsupported`；每 stage 出口 `GOOS=darwin GOARCH=arm64 go build ./...` + `make vet-tags`。
6. **ctx 站点**：新增 `context.Background()` 同行/上一行标 `ctx-root:`/`ctx-none:`，账本只减；**ssproxy 带 receiver 方法不得接受 ctx**（dataplane 门），资源接口用 deadline/`<-chan struct{}`。
7. **既有闸门触发清单**（每个 stage 出口显式核对）：新 iota 枚举登记 15 家族表且 switch 不写非穷举 `default:`；新 verb/subject 进 ACL↔订阅表双向账本与 auth_callout 权限（含 `_TINBOX.>`）；新错误码/原因码进 `cmd/tether` wire 错误码↔exit class 双向表；新 leak 测试有 anchor、本地 `const rounds ≥ 5`、断言在循环外；import raft/yamux/tunnel/pty 的新包进某个 -race 无 -run 矩阵单元；同 PID exec 测试复用 handoff/procrequest 的 dispatch 形状（精确 `-test.run`、mode env 双向剥离、helper 内不二次 fork）。
8. **测试命名与溯源**：文件/函数按被测单元命名，禁 stage/round 命名；`// origin: hot-update <slug> plan §x / review Fn`；test/wire/matrix inventory 只增；新测试目录按主题（`test/hotupdate`）并登记 test/README 地图，p*/d* 冻结 18；每条新守卫注入它声称能抓的缺陷并确认变红（扫源码的门真改磁盘再还原）。
9. **simcluster**：每次真实 drill 前 `./local.sh build` restage 并记录镜像 ID/vendor SHA/HEAD+dirty；本机即 weilandserver 用 `./local.sh drill`；drill 与 go test/lint/gates 严格串行、不留残留容器；实跑前在 stage plan 预告成本征得同意；新增/改动断言必须真跑（predicate 用 shell 函数）；四条日志流只经 `drills/lib/logs.sh`；**改 drill 即同步五份基线**（`tests/assert-identity.baseline.tsv`、`tests/kept-sites.baseline.tsv`、`drill-costs.tsv`、`expected-verdicts.tsv`+log、coverage inventory）并跑 `sh test/simcluster/tests/run-all.sh`；新 `tests/*.sh` 进 run-all.sh 循环；每个能力 drill 自带确定性负控制（**用 tether-prev 真实旧 binary，不用测试开关**）与非空性守卫；verdict 用原生词表，deferred 不计成功，未跑记 NOT_RUN；drill 30 只作参考不进出口；编号先查 expected-verdicts。
10. **预算单源**：`internal/upgradebudget` 提供 ≤500ms/≤5s/≤5s 停顿/10s 进展/120s/15min/8MiB/256MiB/两代，测试与 drill 引用；deadline 一律 CLOCK_BOOTTIME；不为失败样本抬门；改数先改 plan 写用户可观察影响；探针分类计数（CONNECT 失败 vs 请求失败；读/写/短命令）。
11. **审计/ledger 规则不可被热更新打破**：导入 tracker 条目视为 live；`xfer_inflight` 精确终态优先规则原样；ledger 是 outbox 不是活 checkpoint；`transfer_unknown` 不得因交接出现；`ev.transfer` 丢失由带 reply 的 terminal verb + status 补齐。
12. **秘密处理**：registry secret、lease MAC key、SS 密钥/盐、tunnel token 只在 sealed memfd；不进日志/普通临时文件/JSON 报告；handoff 私有 env 用 `CleanEnvironment` 从业务 child 剥离；失败日志不 dump State。
13. **证据纪律**：evidence 只追加不改写；原始 rc/日志保留；red→green 收据成对；runner 原生 verdict 不改写；control-connection-recovery 与 hot-update 分栏。
14. **每 stage 收尾仪式**：plan `docs/reviews/hot-update-<slug>-plan.md` 与 review `hot-update-<slug>-review.md`；活文档同步（requirements 只写 WHAT，architecture §22 每 stage 增 HOW，usage/ops/runbook 随命令与状态，testing-standards/test README/simcluster README 随测试与 drill）；evidence 段；tasklist 只勾有收据项；INDEX 一行；三硬闸（`make test` / `make e2e-parallel` 非管道取 rc、并行全绿即通过 / 单份 `make lint`）+ 动闸门或登记时 `make gates` + Darwin；`git add -A`（排除运行产物/凭据）不 commit/不 push/不部署现网；docs/ 顶层不放过程产物。
15. **Workflow 约束**：每 stage 的 plan/review workflow 固定 3–4 路、数量静态、无条件 spawn、`agent()` 省略 `model`（禁 Haiku/Sonnet）；lint 收敛到单个 agent（全局锁）；专家只读实现/可加测试，主进程唯一改实现与定稿。
16. **注释是资产**：搬动 upgrade.go/run.go/exec.go/tunnel.go/transfer.go/reexec.go 任何段落整段搬其「为什么」注释；生产文件按职责命名；`.golangci.yml` 不动。

---

## §4 综合稿提交的 open questions（原文保留；**裁决见 §-1**，本节不再是待办）

1. **N=1 broker 的产品定义：只承诺 1-voter 集群模式，还是同时接线 install.sh 默认的 single mode？** 现状：leader 门无 `voters==1` 例外；single mode 的 `requestReExec` 未接线（`cluster_not_enabled`），SQLite 由 `serve.go` 打开、无 data-dir 锁。**推荐 (a)**：N=1 热更新在 1-voter 集群模式交付（simcluster `init brk1` 形态，也是 Raft/SQLite/JS 单 writer 论证所在）；single mode reload 返回明确 `deferred(single_mode_unsupported)` 并在 usage/broker-ops 写明一次性前提 `cluster init --from-existing`；先核对现网 racknerd 实际处于哪种模式。选 (b) 则 S6 升 XL、新增 serve.go 级 DB close/reopen 路径与 single-mode drill lane。必须在 S6 plan 前裁定。
2. **N=1 NATS：原地重启 + 续接层吸收，还是产品临时 sibling（路由集群 + JS 迁移）？** 用户措辞含「N=1 冗余」，plan §3.5 允许「临时冗余**或**可验证的本机交接路径」。**推荐**：主路径 = `--signal stop` + `Restart=always RestartSec=2` 原地重启（不用 2 分钟 LDM）+ S1/S5/S7 续接层，10/s 注入实测 ≤5s、传输 10s 进展；sibling 只在实测超预算后作增补，且先做 spike——`cluster-runbook.md §3a` 与 `test/d9 GrowInPlaceOrphansStreams` 已证 standalone JS store 不能就地并入 clustered meta（会孤儿化所有 stream，含在途 tier-B 对象），sibling 在源码事实上省不掉重启也搬不走 R1 store。两条路径都不得用 deferred 冲抵 N=1 成功臂。**这是范围口径，需用户确认**。
3. **Caddy 二进制换代的特权面与入口预热形式。** 现状 caddy.service 为 root、`Restart=on-failure`（graceful stop exit 0 不拉起），tether 从不驱动 systemctl。**推荐**：install.sh 改 unit 为 `Restart=always` + `User=tether` + `AmbientCapabilities=CAP_NET_BIND_SERVICE`（存量 retrofit，走 lint-install + drill 32），配置 reload 走 localhost admin API；binary 换代前用产品管理的临时第二入口预热（SO_REUSEPORT 同址 vs alt-port transient unit，S9 plan spike 后二选一），内部 WSS 主动迁移，旧 Caddy graceful stop，临时入口最终退出。备选 sudoers/polkit 只允许 `systemctl restart caddy`。
4. **ctl 进程退出/重启后是否需要按同一 handle 恢复输出与终态（`exec --resume`、句柄持久到 `~/.tether`）？** 四份草案一致推荐非目标，但主交接 §3.3 写「仍须裁定和实现」，用户两次拒绝缩范围，不能由 stage plan 单方面定。**推荐**：裁定为非目标并写进 `requirements.md §6.8`（WHAT 层）：§6.8 的承诺是升级不迫使用户重启/重连/重提交，ctl 进程被用户杀掉不是升级事件；句柄在 ctl 进程生命周期内内存保留；失败信息附 handle 仅作诊断。若用户裁定纳入，则作为 S1 追加子项（句柄持久化 + resume 子命令），S1 升 XL。
5. **F2 异常流合同批准**：本稿采用「Error 非空 = 流在 Offset 处 aborted、FIN 保持 false、terminal-ack 逐流接受 FIN|aborted 且 ack==End、ctl 呈现为错误但完成 settle、registry 加 `stream_error`」，驳回「异常 FIN=true+Error」（会让 `Replay.FIN` 同时表示真 EOF 与异常终结，正是 handoff-failures §1 F2 禁止的伪造 EOF，且与 `workload.OutputState` 注释冲突）。它决定 S0 修法与 S4 EIO / S5 流错误的统一语义，**请在 S0 plan 定稿时批准**。
6. **legacy（v0.6.0）首次迁入的产品入口**：**推荐**：busy → `legacy_busy` 拒绝；idle → 仍 defer（v0.6.0 无 quiescence 能力；ctl 直发 agent subject 使 broker 侧接纳冻结不可证明，与 plan §10「无此能力的 legacy 一律 defer」一致）；新增显式 `node upgrade --interrupting` 走既有 §21.3 中断式路径，事务记 `legacy_interrupting`（非热、不计 GREEN），usage/runbook 写明这是现网 8 台 v0.6.0 agent 的一次性入口；capable→capable 的 idle 迁入（先禁新接纳→完整闲置证明→保持到激活 + 并发新任务负控制）在 S8 用 gen-N artifact 证明。这是新增 CLI 面，需主进程确认。

---

## §5 批评处置表

无批评标 BLOCKER；下表按 MAJOR 逐条，MINOR 择要合并。

| # | target | 批评（摘要） | 处置 | 落点 |
|---|---|---|---|---|
| M1 | runtime（3 位批评者） | 中间 stage「沿用 §21 现有行为 + hot_update_partial」违反 plan §4.2.7 / §3.1 / req §6.8 | **采纳** | S3 定义 deferred 原因码集合，S4–S7 各自移除，S10 断言为空（§3-4） |
| M2 | runtime（2 位） | S1 出口依赖 C-5/C-10（S3 才做）；Started 先于真实 Start；两套任务表 | **采纳** | S1 scope：Started 后置带 PID、任务表合一、C-5 屏障、C-10 exec 半 |
| M3 | runtime（4 位） | S6 把 broker 映像与传输合成一个 XL 无理由 | **采纳** | 拆为 S6 broker-image（`deferred(transfers_in_flight)` 诚实过渡 + 负控制臂）与 S7 transfer |
| M4 | runtime | S7 legacy 首次迁入无产品路径（v0.6.0 恒 defer 且拒绝新 CLI 面） | **采纳** | S8 + §4-OQ6：`--interrupting` 记 `legacy_interrupting` |
| M5 | transport / orchestration / persistence（4 位） | N=1 broker 被 leader 门拒绝、single mode 未接线，各草案 N=1 drill 臂结构上不可达 | **采纳** | S6 scope：voters==1 走同 PID 交接、plan.go 诚实标志、single mode 按 §4-OQ1 |
| M6 | runtime（all） | 读类动词（`ps` 遇

## S6 implementation snapshot and claim authority (2026-10-11)

The [frozen S6 plan](hot-update-broker-image-plan.md) lead L1–L7 and §12/§14 refine the earlier stage sketch.
Canonical arm names are `37.A` (single), `37.A1` (sole voter), `37.B` (N3), `37.T` (real transfer defer),
`37.R` (valid post-Take/ACTIVE rollback); genuine old N3 Run-unwind is separate `39.BR`.37 parent/children
must be GREEN0,39.BR PRODUCT-RED3, INCOMPLETE4. Legacy signature is pending independent run/lead pin.
Single uses broker upgrade reload/status; no node upgrade --broker alias. Maintenance capability plus
installed agent ACK is S6 work; broad legacy first migration remains S8. Transfers must defer, not export
an invented S7 tracker image. Normal maximum stall5s is hard; complete freeze500ms is calibration.

Source implementations P02/P03/P05/P07–P10/P12–P21/P24/P25/P31–P33 are present. P01/P04/P06/P11/P22/P23/P29
are pending, including Run-internal exec/current rollback and live collector. Existing S5 receipts do not
sign S6, and carried ENDPOINT-RESUME remains open. P35 has supplied no S6 drill/gate receipts yet.
[P34's claim index](hot-update-tasklist.md#s6-broker-image-receipt-boundary) therefore has no checked S6 item.
Only E-4 and broker subitems of D-2/D-4/D-6/B-4 and coverage rows4/5/8 may be signed by S6 evidence;
B-4/D-6 totals and cross-layer rows stay unchecked for later owners. No whole-stack completion follows.
The three hard gates are test/e2e-parallel/lint, with gates separate and Darwin-native separately recorded;
S6 requires the native platform receipt to close exit6. Costs are NOT_MEASURED until P35 records actual wall.
