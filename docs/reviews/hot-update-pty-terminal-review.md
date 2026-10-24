Pass（处置后）—— 内审原判 **Fail**：去重后 BLOCKER 1、MAJOR 16、MINOR 1、NOTE 1，见 §1。

全部处置见 §2/§3。收尾硬闸与 drill 又揪出 1 个恢复交接死等、1 个客户端契约问题，以及 4 个内审与单元测试都够不到的产品缺陷（见 §3/§4），均已修复。

最终同候选树上的验证结果：drill NO DEVIATIONS；`make test`、`make e2e-parallel`、`make gates` 全部 rc=0，见证据 §15.8。

# S4 · PTY 终端热更新 —— 内审报告与主进程处置

日期：2026-10-02 至 10-03。范围：S4 相对 S3 最终候选树 `f366a046b56ed827f6dcd7e7ef19c28df88c28e1` 的全部改动（plan：[hot-update-pty-terminal-plan.md](hot-update-pty-terminal-plan.md)；证据：[hot-update-evidence.md](hot-update-evidence.md) 的 S4 节）。

**形式**：从 S4 起，按用户裁定，开发与审查改由 Codex（`gpt-6.1-sol`）承担，Astra 只留给每个 stage 的 0–2 个最难单点。本次内审共 4 路审查：

| lane | 关注面 |
|---|---|
| rv-eof | 尾流 / EOF / EIO |
| rv-fd | 双工 FD 并发与 deadline |
| rv-auth | 权限、撤权与 N-1 |
| rv-drill | drill 与 real-binary harness 真实性 |

审查与核验的分工：

- 每路审查都配一个独立的对抗性核验者，任务是逐条推翻 finding，并在同一 lane 内补报最多 5 条。
- 审查者与核验者都只能新增测试，不得修改实现。
- 报告原文（含证据、触发条件与建议）归档在仓库外的 `~/tether-hot-update-handoff-20260920T2356Z/s4-review/`，本文件只列定位与处置。

## 1. 结论

审查者共报 **18 条** finding（rv-eof 2、rv-fd 4、rv-auth 3、rv-drill 9），核验者另补报 **2 条**（rv-fd-V1、rv-drill-V1）。核验结果：

- 18 条全部 **CONFIRMED**；
- 唯一的严重度调整是 RV-AUTH-3，由 NOTE（未证实疑点）**升为 MAJOR**：核验者构造出了 proof 与 generation 错位的确定性触发。

跨 lane 的独立重复发现只有一处：**rv-eof-1 = rv-fd-1**（同一缺陷，按一处修复）。去重后共 19 个缺陷：

- BLOCKER 1：rv-fd-4
- MAJOR 16：
  - 产品 7 个：rv-eof-1、rv-eof-2、rv-fd-2、rv-fd-3、rv-fd-V1、RV-AUTH-1、RV-AUTH-3；
  - drill / harness 9 个：DRILL-1 至 DRILL-8、rv-drill-V1。
- MINOR 1：RV-AUTH-2
- NOTE 1：DRILL-9

横贯的根因有三条：

**(a) 「承担了责任」与「完成了」被合成同一个状态。**

- 部分激活失败时，Run 直接退出（rv-fd-4）。
- 重试时，已经激活的 actor 被当作失败拒绝（rv-fd-2）。
- 冻结回滚失败时，入口照样打开（rv-fd-V1）。
- 已结束的 PTY 被撤权时，重新背上已经完成的清场责任（RV-AUTH-1）。

**(b) 「发布终态」早于「终态的最后一笔记账」。**

- 输入源结束时，在飞 write 的 `n` 尚未记账，最终 InputEnded 已经发布（rv-eof-1）。
- 已提交的 replay abort 被包装成请求拒绝，ctl 因而永远等不到 settle（rv-eof-2）。

**(c) drill 判官要求的收据，driver 从未产生。**

- 判官要求 resize 收据，但 W 与 L 从未执行 resize（DRILL-1、V1）。
- 已声明的 25 秒断连被 5 秒停滞判据拒绝（DRILL-2）。
- 产品有意保留的 zombie leader 被判为残留（DRILL-3）。
- 健康校准反而要求只有故障时才写的标记（DRILL-7）。
- L 的故障注入让被测的准入 handler 根本不可达（DRILL-6）。

这类判官在真实部署上要么永远红，要么在错误的理由下才绿。

## 2. finding 逐条处置

（修复轮进行中：FX-A 激活/冻结事务、FX-B 输入终结与 replay abort、FX-C 授权与撤权、FX-D drill 与 harness。完成后在此逐条填写处置、回归测试与变异收据。）

| # | lane | 核验 | 严重度 | 内容 | 修复包 |
|---|---|---|---|---|---|
| rv-fd-4 | fd | CONFIRMED | BLOCKER | 部分激活失败直接退出，绕过当前态恢复 | FX-A |
| rv-fd-2 | fd | CONFIRMED | MAJOR | 一次部分激活后，重试永久拒绝已 active actor | FX-A |
| rv-fd-3 | fd | CONFIRMED | MAJOR | 完整 Freeze deadline 漏掉预检与超时恢复的锁获取 | FX-A |
| rv-fd-V1 | fd（核验补报） | CONFIRMED | MAJOR | 冻结回滚失败时打开准入，而更早的 actor 仍处冻结 | FX-A |
| rv-eof-1 = rv-fd-1 | eof / fd | CONFIRMED（两路独立） | MAJOR | 真 source end 在 writer 记账前发布最终 InputEnded，并提前发布 terminal | FX-B |
| rv-eof-2 | eof | CONFIRMED | MAJOR | input replay abort 被包装成永久拒绝，真实 ctl 不会继续 drain / settle | FX-B |
| RV-AUTH-1 | auth | CONFIRMED | MAJOR | 已结束 PTY 的活动态撤权重新制造清场责任，破坏 import 和本地 Thaw | FX-C |
| RV-AUTH-3 | auth | 由 NOTE 升级 | MAJOR | 健康 proof 等待 control token 后缺少对当前连接 generation 的复核 | FX-C |
| RV-AUTH-2 | auth | CONFIRMED | MINOR | 非零独立授权已到期时，下一次 Tick 前仍可执行 control 并续 KA | FX-C |
| DRILL-1 | drill | CONFIRMED | MAJOR | W 从不产生其连续性判官要求的 resize 收据 | FX-D |
| rv-drill-V1 | drill（核验补报） | CONFIRMED | MAJOR | L 同样无法满足 resize 判据 | FX-D |
| DRILL-2 | drill | CONFIRMED | MAJOR | C1 声明的 25 秒断连被 5 秒停滞判据拒绝 | FX-D |
| DRILL-3 | drill | CONFIRMED | MAJOR | C2 拒绝产品在 draining 期间有意保留的 zombie leader | FX-D |
| DRILL-4 | drill | CONFIRMED | MAJOR | 39.P 在判决分类前丢掉 legacy 所有权丢失证据 | FX-D |
| DRILL-5 | drill | CONFIRMED | MAJOR | P15/P15b 非空性控制不再跑通正例（r9d 7 FAILED） | FX-D |
| DRILL-6 | drill | CONFIRMED | MAJOR | L 的 pending-gate 故障让 handler 不可达，rc75 不是准入收据 | FX-D |
| DRILL-7 | drill | CONFIRMED | MAJOR | A 的严格校准要求健康运行从不写的故障标记 | FX-D |
| DRILL-8 | drill | CONFIRMED | MAJOR | 意外 HUP 抵达原前台后 T35 仍绿 | FX-D |
| DRILL-9 | drill | CONFIRMED | NOTE | -race 下完整冻结超过 500 ms 校准目标（按 lead 裁决 L4 只报告） | 报告 |

### 2.1 FX-A —— 激活 / 冻结事务（已合入）

**rv-fd-4（BLOCKER）**

- **根因**：Run 在承担 active 责任之后、装上 watchdog 与退出恢复之前，就可能返回；同时 `PhaseActive` 被误当作「所有方向都已激活」。
- **修复**：
  - `internal/agent/bootstrap.go`：激活最多重试三次；仍失败时保留 active owners，进入现有的当前态 cut 与 `rollback_blocked` 恢复。
  - `internal/agent/agent.go`：在激活之前装好 watchdog 与退出责任。
  - `internal/agentboot/image.go`：`Image.Activated` 区分「承担责任」与「全部激活」。
  - `internal/agent/upgrade_state.go`：提前装上的 watchdog 不得把已导入的映像当作普通 bare-exec 回滚。
- **回归**：
  - `internal/agent/bootstrap_activation_test.go: TestRunRetainsRecoveryAfterPartialPTYActivation`
  - `internal/agent/bootstrap_recovery_test.go: TestRunActivationFailureKeepsCurrentStateRecoveryReachable`：持续失败，并验证新的当前态 cut generation。

**rv-fd-2**

- **根因**：重试时重新处理已完成的 owner；actor 在已 active 后拒绝合法的激活调用。
- **修复**：
  - `internal/execsession/activation.go`：记录每个 task 的完成进度，只准备 / 激活未完成的 owner。
  - `internal/ptysession/fence.go`：已 active 时的 prepare / activate 幂等，不停泵、不清 replay、不重授期限或租约。
  - 撤权仍先于输入恢复。
- **回归**：
  - `internal/execsession/activation_retry_test.go: TestPartialActivationRetryKeepsAlreadyActiveActors`
  - `internal/execsession/activation_progress_test.go: TestActivationRetryResumesOnlyUnfinishedActors`

**rv-fd-3**

- **根因**：所有权快照与超时回滚使用普通 mutex，没有纳入绝对 deadline；同步恢复还可能无界等待泵的记账。
- **修复**：
  - Manager / runtime 快照改为感知 deadline 的版本，并保留 legacy adoption 预检的顺序。
  - 外层 cut guard 与 liveness 读取共用同一 deadline。
  - actor 恢复持有该 generation 的 control token，直到旧泵完成记账。
  - runtime 恢复保持准入关闭并保留确认；调用方以 `ErrRollbackPending` 有界返回，后续操作等待已确认的恢复。
- **回归**：
  - `internal/agentimage/freeze_admission_test.go: TestFreezeDeadlineIncludesProcessOwnershipPrecheck`：合并审查者与核验者的两种触发（30 ms / 50 ms），保留较强的 250 ms 返回界。
  - `internal/ptysession/fence_recovery_test.go: TestFreezeDeadlineIncludesTimeoutRecoveryLock`
  - `internal/execsession/activation_progress_test.go: TestFreezeDeadlineIncludesRuntimeOwnershipSnapshot`、`TestFreezeRollbackBoundsRuntimeAccountingLock`
  - `internal/agent/bootstrap_recovery_test.go: TestActivationCutBoundsOwnershipGuards`

**rv-fd-V1**

- **根因**：回滚丢弃了激活失败，在更早的 actor 仍冻结时重新打开准入。
- **修复**：
  - `internal/execsession/fence.go` 与恢复 finalizer 在失败后保留部分进度，并保持准入关闭。
  - 组合回滚向上传播错误；`Frozen.Thaw` 只在成功恢复后才算完成。
- **回归**：
  - `internal/agentimage/freeze_admission_test.go: TestFreezeRollbackIncludesManagerActivationGuard`
  - `internal/execsession/activation_progress_test.go: TestFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor`

**变异**：13 条全红，且都是断言失败而非编译失败：

| 变异 | 回注的缺陷 |
|---|---|
| run_once | 只试一次即返回 |
| skip_active | active 即视为完成 |
| redo_completed | 重做已完成的 owner |
| reject_active | 拒绝已 active 的 actor 重试 |
| manager_precheck_lock / runtime_precheck_lock / live_precheck_lock | 三处普通锁 |
| actor_recovery_lock | 同步 actor 恢复 |
| runtime_recovery_lock | 无界回滚记账锁 |
| composed_drops_pending | 丢弃组合回滚错误 |
| cut_manager_lock / cut_guard_lock | 外层 cut 普通锁 |
| rollback_opens_admission | 回滚失败仍开准入 |

其中 liveness 变异最初没有变红，原因是测试漏调了 liveness 读取；R7 补上后转红。

**计划偏差**：S3「先激活、后 defer」的顺序被最小修订，以覆盖 S4 的部分激活责任。wire 与 image ABI 不变，方法预算不增。

**结构预算后果**：FX-A 合入后，`internal/agent` 的剥注释代码从 7988 行升到 8078 行，越过 8000 硬线。按 CLAUDE.md「新增状态机留子包，不以删注释换预算」，另派 AGENT-SLIM-2 把内聚逻辑整段（连同注释）迁往子包，目标 ≤7300 行，为 S5 的接线留余量（见 §3）。

**合入后发现的追加缺陷：过期 thaw 丢失恢复交接（产品缺陷，已修）**

AGENT-SLIM-2 的组合 -race 运行中，FX-A 新增的 `TestFreezeRollbackIncludesManagerActivationGuard` 卡住 15 分钟。SIGQUIT 栈显示，测试 goroutine 停在 `execsession.(*Runtime).Handle` 的 `cond.Wait`，没有任何人 broadcast。沿 FX-A 线程（sol）追根因的结果如下。

- **根因**：Manager 期限过期后，`ThawUntil` 只尝试一次 `Runtime.mu.TryLock()`；一旦失败就直接返回 `ErrRollbackPending`，没有排程恢复。runtime 因此永远停在冻结态，`Handle` 无限等待。
  - 修复前复现：`GOMAXPROCS=8 -count=200` 挂到 6 分钟超时；用持锁屏障可确定性复现。
- **修复**（`internal/execsession/activation.go`、`admission.go`、`runtime.go`、`fence.go`）：
  - 在取 runtime 锁之前，先发布一个按 generation 区分的恢复 owner。
  - 同 generation 的重试共享这个 owner；当前 generation 的恢复替换掉过期的排队工作；过期作业不能 thaw 更新的 cut，也不能清除它的 owner。
  - 空 cut 回滚成功后清除 `partialActive`。
  - `Handle` 的每一个准入等待（含取 mutex）都以 `upgradebudget.RequestLatency`（5 s）为上限；超时返回 `ErrBusy` + `ErrRollbackPending`，同时保留恢复责任与资源。
- **回归**：
  - `internal/execsession/activation_handoff_test.go`：
    - `TestExpiredThawRetainsRecoveryThroughRuntimeLock`
    - `TestDelayedThawCannotActivateLaterCut`
  - `internal/execsession/admission_test.go: TestHandleBoundsEveryAdmissionWait`：覆盖五个准入谓词与入口锁争用。
- **变异**：10 条全红：
  - 恢复「无交接即拒绝」；
  - 去掉 broadcast；
  - 去掉超时；
  - 无界取锁；
  - 去掉 stale-generation 守卫；
  - 复用 cut generation；
  - 空 cut 保留 `partialActive`；
  - 保留已完成的交接槽；
  - 去掉恢复确认；
  - join 过期交接而非替换它。
- **压测**：同样的压测在修复后 rc=0/0/0。

**这个上限带来了一个客户端契约问题**：超时映射成 wire 码 `process_busy`。

| 客户端 | 对 `process_busy` 的处理 |
|---|---|
| PTY | 视为瞬时错误 |
| exec | 视为终态错误 |

S1 计划 D1 写的是「计划内切换不发 `process_busy`」。于是 runtime 内恢复超过 5 s 时，已 attach 的 exec 会失败，而不是等恢复后继续。这一点并入收尾包 S4-CLOSE 处理（见 §3）。

### 2.2 FX-B —— 输入终结与 replay abort（已合入）

**rv-eof-1 = rv-fd-1**

- **根因**：source EOF 在 writer 仍持有一笔未记账的内核 write 时，就发布了 `InputEnded` 并释放 reap 责任。于是最终 A 仍可能变化，完整送达的 FIN 也可能被误判为 abort。
- **修复**（`internal/ptysession/actor.go`、`input.go`、`output.go`）：把「关闭输入入口」与「发布最终状态」拆成两步。
  - writer generation 先记完最后一笔 `n`，再定稿输入状态。
  - source 结束时，在 actor 锁与 Manager guard 之外唤醒并 join writer，然后才 close / reap。
  - 真实错误或撤权优先于 clean FIN。
  - replay abort 与撤权走同一道记账屏障。
- **回归**：`internal/ptysession/input_finality_test.go` 中的三个测试：
  - `TestPtyInputEndedWaitsForInflightWriteReceipt`：已读 / 未读两种 FIN 触发；
  - `TestInputEndedWaitsForInFlightWriteAccounting`：非 FIN 触发；
  - `TestPtyInputAbortWaitsForInflightWriteReceipt`：replay 失败、撤权、先 source 结束后撤权三种情形。

  审查者与核验者重复的 oracle 已合并，保留各自不同的触发形状。
- **变异**：5 条全红，分别回注：
  - 立即发布 `InputEnded`；
  - 去掉 source join 与 reap 屏障；
  - ACK 丢掉已写的 `n`；
  - 跳过 replay-abort 的 join；
  - source 关闭覆盖后续撤权。

**rv-eof-2**

- **根因**：首次 replay Append 失败时 abort 已经提交，却返回普通 error，agent 因而把 status 替换成拒绝信封，client 随之放弃 drain 与 settle。
- **修复**：
  - `internal/ptysession/contracts.go` 新增 typed `InputAbortError`；
  - `internal/execsession/pty.go` 只把已提交的 abort 转成完整 status 回复，非法序列仍按拒绝处理；
  - T12 改为拒绝一切顶层 input error。
- **回归**：
  - `internal/execsession/pty_input_test.go: TestPTYCommittedReplayAbortIsAStatusReply`；
  - `internal/execclient/pty_replay_test.go: TestPTYReplayPrefixAbortDrainsThroughAgentAndSettles`。真实 `agent.Run`、NATS、PTY/replay syscall 加原 client 全链路，断言：保留 2 字节前缀、输出恰为 `STARTREADYTAIL`、真实 rc0 终态、terminal ACK、`SettledError`、replay 预算归零。
- **变异**：2 条全红：
  - 关闭 typed abort 转 status：三处回归同时变红；
  - 把一切 ended-input error 都转成 status：非法序列的拒绝被吞，回归变红。

**合入后检查**：`go test -race` 覆盖 ptysession、execsession、execclient 三包，rc=0；determinism 与 architecture rc=0。

### 2.3 FX-C —— 授权与撤权（已合入）

**RV-AUTH-1**

- **根因**：撤权时无条件重新设置 cleanup pending，即使保留中的 terminal 已经退休了自己的 Manager 行。
- **修复**：
  - `internal/execsession/revoke.go` 对已完成的 terminal（含冻结态本地恢复）改走 `ptysession.RevokeTerminal`；
  - `internal/ptysession/control.go` 保留未完成的 cleanup 与已捕获的身份；
  - 缺失归属仍返回 `ErrUnknown`。
- **回归**：
  - `internal/execsession/pty_terminal_authority_test.go: TestPTYTerminalAuthorityRevocationPreservesRestorability`，覆盖活动、本地、import、冻结 import、冻结本地恢复五条路径；
  - `internal/ptysession/control_revocation_test.go: TestPTYRevocationRetainsUnfinishedCleanup`。

**RV-AUTH-2**

- **根因**：control 只检查已记录的撤权，在 Tick 之前，独立授权已到期的 control 仍可执行。
- **修复**：`ApplyControl` 持有 control token 时，按 boot clock 复核授权期限；到期先进入不可逆撤权，再拒绝新的副作用。
- **保持不变**：authority=0 的语义、重复收据的幂等性、重复 KA 不续期。
- **回归**：`internal/ptysession/control_authority_deadline_test.go`：
  - `TestPTYControlRequiresUnexpiredImportedAuthority`，覆盖四种 control；
  - `TestPTYDuplicateControlDoesNotRenewExpiredAuthority`。

**RV-AUTH-3**

- **根因**：agent 成功 probe 后，proof 可能在 actor 消费之前就已过期。
- **修复**：
  - `internal/ptysession/contracts.go` 新增 `HealthProofVerifier`；
  - `internal/agent/run.go` 提供 `ptyHealthConnectionProof.Current`。
  - actor 取得 token 后，复核 connection、reconnect 计数与 generation；proof 失效时同时清除先前的 strike。
  - ptysession 不 import agent/NATS，分层门保持绿。
- **回归**：
  - `internal/agent/pty_health_generation_test.go: TestPTYHealthProofCannotOutliveCurrentConnectionGeneration`：真实 NATS 往返，加 probe 后屏障；
  - `internal/ptysession/control_health_test.go: TestPTYHealthProofRechecksGenerationAfterControlWait`：token 真实被占用。

**变异**：11 条全红，分别回注：

| 变异 | 回注的缺陷 |
|---|---|
| terminal-rearm | 无条件 cleanup pending |
| terminal-runtime-selection | 改回普通撤权 |
| terminal-frozen-mark | fence 下不标 terminal |
| terminal-closed-guard | 去掉 closed-master 前提 |
| terminal-pending-guard | 去掉未完成清场保护 |
| cleanup-unknown | 吞掉缺失归属 |
| control-authority | 去掉期限检查 |
| control-zero-authority | 把 0 当作过期 |
| duplicate-ka-renewal | 重复收据续 KA |
| proof-consumption | 去掉消费时复核 |
| proof-producer | agent 不提供 verifier |

**合入后检查**：`go test -race` 覆盖 execsession、ptysession、agent 三包，rc=0（agent 574 s）；determinism 与 architecture rc=0。Agent 方法数仍为 125，Runtime 仍为 39。

### 2.4 FX-D —— drill 36 / 39.P / T35 真实性（已合入）

每条 finding 都在 `test/cli_e2e` 中有对应的回归测试，或给出了非空性控制；整轮未改产品代码。

| finding | 根因 → 修复 | 回归 |
|---|---|---|
| DRILL-1 | W 要求 resize 收据却从不 resize。driver 现在在 blackhole 之前与恢复之后各执行一次真实 checkpoint resize。 | `pty_probe_test.go: TestPTYProbeWatchdogDriverProvidesRequiredResizeReceipts` |
| rv-drill-V1 | L 只在拒绝之后 resize。现在用真实 resize 把拒绝窗口前后包住。 | `TestPTYProbeLegacyDriverBracketsRefusalWithResizeReceipts` |
| DRILL-2 | 停滞豁免只认 watchdog。carrier 恢复现在按「观测到的窗口 + 5 s 边沿 + 原 180 s 期限」界定，无法解释的停滞仍判失败。 | `TestPTYProbeCarrierOutageBudgetAllowsDeclaredFault` |
| DRILL-3 | 一切 zombie 都判失败。现在只放行原 leader 的身份，且只在测得的 live-tail 区间内；无关、晚到或已 settle 的 zombie 仍判失败。 | `TestPTYProbeDrainingReapHeldLeaderIsAllowedBeforeSettlement` |
| DRILL-4 | P 要求 bare exec 必然丢失的所有权记录。`runtime_records`、`assemble_observations`、`verify_legacy_fault` 在证明真实供给、替换、日志可读以及原 ctl 结局之后，保留这份「缺失」本身作为证据；ctl 超时时记录测得的 timeout 与真实 waitpid 状态。 | `TestPTYProbeLegacyExporterLossRemainsBusinessEvidence` |
| DRILL-5 | W/C/P 非空性 fixture 用的是旧 schema 或旧子进程边界。改用当前收据、原始文件采集边界与 stderr 签名。 | r9d：审查时 326 证成 / 7 失败 → 合并后 **399 证成 / 0 失败** |
| DRILL-6 | L 在重连仍被阻断时发出准入请求，并把任意 rc75 当作证据。driver 现在只对 commit 持久化注入故障、准入前恢复传输，并要求产品的 `agent_upgrading` 原因、pending candidate 不变、零 spawn/effect。 | `hot_update_pty_driver_test.go: TestPTYLegacyPendingGateRejectsNoResponderReceipt` |
| DRILL-7 | 严格校准去找不存在的故障标记。改用完整的健康运行区间做校准，同时保留升级窗口的要求。 | `hot_update_mixed_probe_test.go: TestPTYMixedProbeStrictCalibrationNeedsNoFaultMarkers` |
| DRILL-8 | 前台 fixture 记下了信号收据，但 T35 从不读取。现在在往返期间与 settle 之后检查意外的 HUP / INT / TERM 收据。 | `run_reliable_test.go: TestRunReliableDuplexAcrossUpgradeAndRollback` |
| DRILL-9 | 只作校准。T35 样本最大值：<br>- 各次运行合计最大 933 ms；<br>- 最后三次 -count=3 分别为 839 / 842 / 867 ms；<br>- 整包运行 807 ms。<br>均超 500 ms 目标，按 L4 只报告。 | — |

**变异**：13 条全红，另有两条对照：

- 去掉两处信号检查后，再注入审查者的 HUP，结果 rc0。这证实了原先的 oracle 漏洞，加回检查即转红。
- 恢复旧的 W 正例 schema 后，r9d 为 334 证成 / 2 失败。

**并发写者合并**：FX-D 与 P16-SIM 从同一基线并行改动了 simcluster 账本，共 6 个文件冲突。由单独的 FX-D-MERGE（sol）取两边控制的并集；因 probe 语义合法变化而过时的控制，改写成「正例通过、反例失败」的新形式。

- 合并后 run-all 为 ALL PASS，r9d 为 399 证成 / 0 失败。
- 两本 baseline 经扫描器重新生成，计数如下：

| drill | 各臂计数 | 共享 | 合计 |
|---|---|---:|---:|
| 36 | A=0、W=19、C=22、L=9 | 38 | 88 |
| 39 | X=6、Y=5、P=0 | 17 | 28 |

- FX-D-MERGE 的变异：每边删一条控制，结果都是 rc1。

**合入后检查**：

- `go test -race ./test/cli_e2e/`：rc=0（856 s）；
- run-all、architecture、determinism：rc=0；
- `bash -n`、lint-drills：通过。

**部署层**：drill 尚未实跑（NOT_RUN）；39.P 仍未钉判（UNPINNED），首次实跑应保持 ASSERT-FAIL 并保留诊断，钉判之后才出原生 PRODUCT-RED。

## 3. 收尾：结构预算、lint 与准入上限的客户端契约

### 3.1 AGENT-SLIM-2：结构预算（已合入）

FX-A 让 `internal/agent` 的剥注释代码达到 8078 行（硬线 8000）。按「新增状态机留子包、不以删注释换预算」，由 sol 把内聚逻辑整段（连同注释）迁出：

| 迁到 | 内容 | 行数 |
|---|---|---:|
| `internal/binaryupgrade` | marker 类型与持久化、启动决策与回滚、安装事务、URL allowlist 与有界下载、pre-control-ready 恢复 | 541 |
| `internal/transferpath` | 路径校验、目录钉住的原子操作 | 409 |

- **结果**：`internal/agent` 从 8078 降到 **7291**，为 S5 的 P14/P15/P16 接线留出约 700 行余量。
- **不变量**：Agent 恰好 125 个方法，生产文件 19 个，`execsession.Runtime` 39 个方法，结构 golden 未放宽。
- **分层**：两个新包在分层表里各加一行，禁止 import agent / broker / NATS。
- **error-code 闸门**：`cmd/tether/error_code_coverage_test.go` 的扫描树加入 `internal/transferpath`。没有这一行，闸门会看不见 8 个迁走的发射点；去掉它已验证会变红。
- **测试身份**：31 个测试原名迁移（断言不变）。测试清单的旧行已按规则手工删除、新行由 updater 追加；commit message 须写明理由。
- **变异**：29 条断言失败。另有 4 条单点变异被冗余防线挡下，组合变异之后全红。
- **合入后检查**：`go test -race ./internal/agent ./internal/binaryupgrade ./internal/transferpath ./internal/agentboot ./cmd/tether`，rc=0。

### 3.2 lint 收尾（主进程）

`make lint` 在合入后报 28 条，已修 25 条：

- 15 处测试 `defer x.Close()` 改为仓库惯用写法 `defer func() { _ = x.Close() }()`；
- 瘦身遗留的死代码：
  - `upgradeMaxTarballBytes` 的来由注释整段搬到 `binaryupgrade.MaxTarballBytes` 后，删除 agent 侧别名与 `extractTetherBinary`、`syncDirE`；
  - 删除测试里重复的 `canonDir` 与未用的 `natsRelay.cutHappened`；
- `initPTYUpgradeAdmission` 的 nilerr：语义不变，改写为「能力足够才解除限制」；
- 两处 staticcheck 改写。

剩余 3 条都位于 S4-CLOSE 正在修改的文件中，并入该包处理：

- `Handle`（CC 68）与 `handlePTYLocked`（CC 67）的 maintidx：按职责拆成包级函数，**不**进 maintidx 豁免账本；
- `fence.go` 中 `thawTask` 的 errcheck：需要明确它在激活事务中的语义。

### 3.3 S4-CLOSE：准入上限的客户端契约（已合入）

**wire 码**

- 新增 additive wire 码 `process_recovering`。准入超过 `RequestLatency` 时，agent 发出它，而不是 `process_busy`。
- 已在 `cmd/tether` 的四处 error-code ↔ exit-class 闸门中归类为临时失败。

**客户端处理**

| 客户端 | 收到 `process_recovering` | 收到真正的 `process_busy` |
|---|---|---|
| exec | 在调用方 ctx 内用同一 CallID / handle 重发，不重复执行 | 终态 |
| PTY | 在既有恢复窗口内重试 | 终态 |
| N-1 | 把未知拒绝原样报出，不 fallback，也不产生第二个子进程 | — |

**测试与变异**

- **端到端**：真实 NATS + agent + Runtime，准入被扣住超过 5 s 后恢复。断言输出恰为 `readydone`、exit 23、原唯一子进程 settle。
- **回归测试**：
  - `internal/agent/process_recovery_test.go`：
    - `TestProcessClientsContinueAfterBoundedAdmissionRecovery`
    - `TestProcessRecoveryRefusalPrecedesBusyMapping`
  - `internal/execclient/admission_test.go`：
    - `TestExecClientRecoveryRefusalKeepsOriginalTransaction`
    - `TestProcessClientsKeepGenuineBusyFinal`
    - `TestRecoveryRefusalPreservesNMinusOneSafety`
  - `internal/proto/process_resume_test.go: TestProcessRecoveryRefusalWireCompatibility`
- **变异**：11 条全红：
  - exec 把恢复当终态；
  - PTY 把恢复当终态；
  - agent 把恢复映射成普通 busy；
  - exec 重试真正的 busy；
  - PTY 重试真正的 busy；
  - agent 把真正的 busy 映射成恢复；
  - 忽略 thaw 错误；
  - thaw 失败后打开准入；
  - 重命名 wire 码；
  - 允许 N-1 fallback；
  - 在磁盘上删除 CLI 归类。

**god-function 拆分**：按职责拆成包级函数，检查顺序、锁定与注释原样保留。

| 函数 | CC（前 → 后） | MI（前 → 后） |
|---|---|---|
| `Handle` | 68 → 18 | 16 → 36 |
| `handlePTYLocked` | 67 → 13 | 15 → 42 |

所有拆出的 helper MI > 20，未进 maintidx 豁免账本。

**thaw errcheck**：激活循环里的 thaw 失败现在保留部分进度、关闭的准入、恢复所有权与资源。回归测试为 `internal/execsession/activation_progress_test.go: TestExecThawFailureRetainsPartialActivationResponsibility`。

**架构文档**：§22.5 第 3、12 项与相关 §22.4 / §22.7 句子已同步。

**合入后（主进程）**：lint 新报 1 条 staticcheck De Morgan，改为具名谓词 `admissionBlocked` 后通过；`make lint` 与 `make vet-tags` 均 rc=0。

### 3.4 硬闸收尾与测试侧时序缺陷（主进程 + sol）

- **`make test` 首轮 3 个失败，均为测试侧时序问题。**
  - realStack 的 `stopBroker` 只等到 `broker.Run` 返回。但 `nc.Drain()` 是异步的，NATS 随后关闭时，drain goroutine 要等满 5 s 的 FlushTimeout，于是两个泄漏门报泄漏。现在改为等 broker 连接离开 server 连接表。
  - S4-CLOSE 的 N-1 测试缺少 `Flush`，SUB 到达 server 之前就有 publish。
  - spawnsafe 的扰动非空性守卫在负载下偶发失败。它在 S3 树与 S4 树上的失败率相同（约 1.5%），不是回退。改为至少 3200 次 consult 且直到 5 代 re-arm，以 5 s 为上限；修复后 300 次零失败，非空性不变。
- **`make e2e-parallel` 唯一失败：LeakRisk 中 internal/agent -race 超过 300 s。** 历史：S2 为 1m19s，S3 为 5m0s（已贴线）。sol 包 AGENT-LATENCY 先查根因后降耗，未放宽预算，整包从 601 s 降到 164 s（限 2 核时 165 s）。
  - **产品侧**：rollback 先探测 `.prev`、后检查 busy owner，可能白白占住升级锁 5 s。现在先 defer busy，回归测试为 `TestWatchdogDefersBusyCutBeforePreviousImageProbe`。
  - **测试侧**：
    - -race 运行时给每个子进程加 1 s 退出 sleep，S4 新增的 prev 探测把它成倍放大。只对子进程关闭这段 sleep，race 报告保留，并以 `TestRaceSubprocessRetainsDiagnostics` 证明子进程的 race 仍会报告并失败。
    - 固定等待改为等可观测条件。
    - 无共享全局的测试改为并行；有进程全局状态的 6 个 fixture 改在独立子进程中运行。

## 4. 部署层 drill 实跑（S4 第一次真正跑通 drill 36）

drill 36 与 39.P 在内审期间从未真实运行过。5 轮实跑（证据在仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run{1..5}`）逐层暴露了 drill 侧与**产品侧**缺陷。产品缺陷全部是内审与单元 / real-binary 测试结构上够不到的：持续混合负载、真实的 `/proc`、真实的 v0.6.0 客户端、真实 NATS 断连下的终端字节流。

| 轮次（镜像） | 结果 | 暴露 | 处置 |
|---|---|---|---|
| 1（595ae2bb） | 34/35/39.X/39.Y/33 符合预期；36 四臂 + 39.P INFRA-ABORT | `assert_*` 用 `$("$@")` 在子 shell 里执行谓词，`artifact_up`、`pty_fixture_start`、`_pty_legacy_quadrants` 建立的全局与后台 job 全部丢失（`ART_SHA` 未设）；drill 94 有同类缺陷（`_A0_PIDS`） | sol DRILL-SUBSHELL：64 处改为在父 shell 执行、断言身份不变。新 lint `tests/assert-shell-state.py` 由主进程扩到**全套** drill，并以磁盘变异验证；drill 94 修好后实跑 GREEN |
| 2（ae082fc7） | 36 / 39.P SETUP-RED | 探针 `census()` 在真实 `/proc` 上遇到 pid 1 必然抛出 `missing process identity` | 主进程：先写回归测试（红），在保留的实例现场复现，修复后转绿 |
| 3（同上 + 修复） | 36.A/W/C、39.P 失败 | **产品**：spawnsafe 把按时在飞的 spawn 计为 wedged，持续请求的节点几乎每次升级都被当场 `spawn_in_flight` 推迟；drill 侧：`node ls` 隐藏离线节点、回滚 phase 判据、C1/C2 共 nonce 的 join、旧 broker 的 fixture 顺序、P 的记账 | sol SPAWN-INFLIGHT（区分已遗弃与在飞；cut 内在同一期限下排空，`Policy.mu` 串行化防 TOCTOU；23 条变异全红；架构 §22.5 同步）；sol DRILL-R3 |
| 4（ab9568ac） | 34/35/94 全绿；36 四臂与 39.P 仍有失败 | **产品**：跨映像切换后 PTY 退出，保留的 terminal 在 Manager 行退休后丢失，原 ctl 收到 `workload: unknown process` 而 exit 74。**产品**：C1 断连期间 ctl 把 `error: read tcp …: i/o timeout` 打进 raw 终端。**产品 / N-1**：升级中的 agent 对 legacy 线路的拒绝码 `agent_upgrading` 不在 v0.6.0 的分类表中，旧 ctl exit 70 而非 75（run / exec / expose / transfer 七处）。drill 侧：W 的未 ACK 输出前提、判官静默失败 | sol PRODUCT-R4（retained 回复改用 registry terminal、完成 / 撤权共用 task token；run 连接的 NATS 异步回调不写 stderr，最终失败在恢复终端后打印）；sol N1-REFUSAL（统一以 `try_again` 开头、`agent_upgrading` 保留在详情，新客户端给出升级提示，冻结 v0.6.0 分类规则做表测试，16 条变异全红）；sol DRILL-R4；主进程审阅后**钉判 39.P** |
| 5（78f2c62d） | **NO DEVIATIONS**：36 A/W/C/L GREEN（123 pass），39.P 原生 PRODUCT-RED（钉判命中），34/35/94 GREEN，39.X GREEN，39.Y PRODUCT-RED，33 INCOMPLETE | — | 实测耗时写入 `drill-costs.tsv`；`hot-update-pty-ledger-test` 转为钉住实跑后的状态（两条变异均红） |

DRILL-9（-race 下冻结窗超过 500 ms）按 L4 只报告：T35 的冻结窗样本为 807–933 ms。
