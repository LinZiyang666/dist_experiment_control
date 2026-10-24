# 热更新交接：runtime 失败路径、PTY master 与共用 exec 启动

状态：**停止实现，交接 Claude；F1/F2 已证实且未修复**。这份文档覆盖 tunnel/transport lane 最近的具体实现与审查，不代表完整功能内审或外审通过。用户要求停止后，本 lane 只读源码、归档文档，没有再改生产/测试/登记，也没有运行新测试。

## 1. 当前已证实的两个阻断项

冻结候选 `internal/execsession/runtime.go` SHA256：`b25f9b7481f4cea5f9aadc72b436c646376f1e69e6459fb93e7d25183fcf3943`。交接时仍是产生两条红测试的原实现。完整英文审查和原始 hash 见 [failure review](hot-update-execsession-failure-review.md)。两项都经过另一 lane 的只读交叉确认。

### F1：Close 已取消后，迟到的 Start 失败会永久保留 owner 与输出 replay（Medium）

位置：`internal/execsession/runtime.go:173,187–189,234–247,381–389`。

真实调度顺序：

1. create 建立三个真实 Replay memfd；Start hook 在实际创建子进程前被门控阻塞。
2. `Close` 收到已经取消的 ctx，先把 runtime 标为 closed，然后因 task.done 未结束返回 context.Canceled。
3. 释放 Start，让它返回普通启动错误；`failStart` 仅关闭 stdin replay，对两个输出 Finish，随后 done 关闭。
4. 此时没有第二次 Close：任务表仍有一个 owner，两个输出 Read 返回 EOF 而不是 os.ErrClosed，完整预算再次预留失败 ErrBackpressure。

这是生产所有权路径缺陷，测试没有伪造 replay/budget。测试断言之后才调用第二次 Close 做 fixture 清理；它不参与通过条件。Close 文档称停滞的启动“保留 owner 直到返回”，但当前代码缺少它返回后的最终清理。

**可选修复方向，尚未落地/验证：** 在 start/observe 的唯一最终结束路径中，于 runtime.mu 下检查 closed，关闭资源并移除 task，然后发布 done；不依赖仍有 Close 调用者等待。开放状态下的失败终态仍应保留到客户端结清。处理正常 Start 迟到成功、失败、部分 allocate、重复/并发 Close，避免重复终态或双 waiter。仅加“后台再次调用 Close”若无唯一所有权与截止条件证明，不是现有证据确认过的修法。

### F2：已启动子进程的泵初始化失败，会在协议结清前关闭输出日志（Medium）

位置：`internal/execsession/runtime.go:195–216,325–329,354–367`。

实际反例：合法 ReplayCapacity=8，Start 返回 16 字节 warning。真实 `/bin/sh` 子进程以独立原 PGID 与显式 stdio 启动，Manager.AdoptDraining 已接管；stdout Output 创建成功；stderr NewOutputWithPrefix 因 warning 超容量拒绝。Runtime SIGKILL 并由唯一 Manager reaper 记录原进程 signal=9/code=-1，随后记录 registry terminal。但 setup 错误分支调用 closeTask，把仍需协议交付的所有日志关掉。下一次 resume 在 Ack 时返回 `file already closed`，没有可用的输出响应，因而不能正常 settle。

范围限定：这是用有效小容量配置触发的真实后启动失败路径；**没有**声称正常默认容量的普通 warning 经常超限。单纯限制 warning 长度不能替代所有后启动 actor 初始化失败的生命周期处理。

**可选修复方向，尚未落地/验证：** 分开“终止原进程/原管道 actor”和“保留已接管的字节及最终错误状态”，后者要保留到协议确认。异常输出必须有明确的终结与 ACK 状态，source EOF、read error、真实 exit、terminal ACK 不可混同；调整 agent/runtime 与 execclient 的读取/结清规则应一致，不能只让 runtime 通过测试。

**禁止伪造 EOF：** 当前 `proto.ProcessOutput` 明确写着 Error 不伪造成功 EOF；`terminal-ack` 又要求两个 FIN。被 teardown 打断的输出不能直接 Finish 成功来消除卡住。可以设计独立的 error-terminal 确认，或在确能证明保全尾流的路径继续 drain；具体方案需先定契约。未创建任何子进程的 allocate/Start 失败，两条真实空输出 FIN 是不同情况，不要破坏该现有正确路径。

## 2. 可直接复现的测试与原始收据

独占测试文件 `internal/execsession/runtime_failure_test.go`，当前 SHA256：`345dc35bee4c9ea600d411135c9b3d67b2ef2dade615bda6c4479ae9a03e3abb`。在原始收据之后只加了 linux build tag，与 Replay memfd 的支持范围一致；没有改断言，也未再次运行测试。

```sh
go test -race ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure|PumpSetupFailureRetainsObservableTerminal|PartialAllocationFailureSettlesAndReleasesBudget|CloseReapsChildWithUnacknowledgedFullOutput)$' -count=1 -v
```

| TestRuntime 后缀 | 原始结果 | 实际观察 |
|---|---|---|
| CloseCanceledStartupReclaimsLateFailure | FAIL | done 已关；task=1；两个 replay 未关；预算被占 |
| PumpSetupFailureRetainsObservableTerminal | FAIL | 真进程已 sole-owner reap；resume 返回 file already closed |
| PartialAllocationFailureSettlesAndReleasesBudget | PASS | 第三份分配失败；不 Start；两空流 FIN；terminal ACK 结清并释放完整预算 |
| CloseReapsChildWithUnacknowledgedFullOutput | PASS | stdout 实际填满 8B、没有 ACK；Close 杀原 child 并 reap，manager/task 清空、预算可重用 |

- `/tmp/tether-control-recovery/execsession-failure-before.log`：前三条，原 rc=1，FAIL 0.062s。
- `/tmp/tether-control-recovery/execsession-failure-confirmed.log`：四条，原 rc=1，FAIL 0.095s；两红两绿，无 race 诊断。
- 第二次运行前只把预算 probe 改为“首次使用前固定 Limit”，避免违反 ReplayBudget 配置契约；没有放宽失败期望，也没有生产修改。两份日志都保留。
- 新增四个 Test 的 inventory 是否已登记须由总交接核对；本 lane 没有改 golden/清单，交接阶段亦未补登记。
- system lane 成功主线在 `runtime_test.go`：真实 duplicate-create 单次物理 spawn、256KiB 输入背压/原管道 inode 重连回放、身份/epoch/双 ACK、5 轮资源回收。本 lane 已源读确认其关键 oracle 有效；它们通过不能覆盖本节两红。该 lane 原始报告负责其最终收据。

修复后应先原样复验这四条，再检查 error-stream 的接收/终态确认和成功主线。这里是后续执行建议，不是已执行的测试或保证。

## 3. PTY master：已应用的有限改动

Root 已审核应用以下文件，逐文件 SHA 与原提案一致：`internal/pty/pty.go`、`master_linux.go`、`master_other.go`。完整说明与 SHA 在 [PTY master 测试报告](hot-update-pty-master-test-report.md)。原提案路径 `/tmp/tether-control-recovery/pty-master-poll-proposed/proposed.patch`，SHA `d3f0dc70e3c6fea87ab49e4f204a127b171b32c98d02b17b4341f9517fd86766`。

- `pty.go:71`：Allocate 在 Start/I/O 前独占 master/slave 时调用 prepareMaster。
- `master_linux.go:18`：RawConn.Control 内 F_DUPFD_CLOEXEC；共享 open-file description 上设置 O_NONBLOCK；用 os.NewFile 建新的 poller wrapper；检查两向 deadline；关闭旧 Go File 引用。保留同一内核 PTY，不重开 /dev/ptmx。
- `pty.go:309` / `master_linux.go:58`：resize 使用 RawConn.Control + unix.IoctlSetWinsize，避开 creack/pty v1.1.24 的 File.Fd 行为；后者会把旧 os.OpenFile wrapper 关联的 fd 改回阻塞，并影响 duplicate。
- 该准备函数只用于尚无并行 I/O 的 Allocate，**不是**任意运行中 FD 转交接口。输入输出共享 master 的后续 handoff owner 仍未接线。
- 非 Linux 延用原 master/resize 行为，Darwin fallback 的 overlay 构建已通过；不声称 Darwin 实现了 Linux 热更新能力。

原始 focused 收据（都是 root apply 前的 overlay/基线证据，没有转写为 apply 后全栈 Pass）：5 新 Test 基线 FAIL 0.034s；提案 overlay 5×3 `-race` PASS 1.051s；6 相邻已有 PTY 测试 PASS 1.108s；只把 resize 改回 File.Fd 的负控制 FAIL 0.033s；Darwin overlay build PASS。日志均在 `/tmp/tether-control-recovery/pty-master-poll-*.log`，测试文件是 `internal/pty/master_poll_test.go`，包含五轮真实 FD/goroutine 回收。

## 4. PTY 尚未完成的具体接线

1. **Started PTY 专用 EIO 语义。** 真子进程退出 23 后，完整 tail 仍可读，最后 master.Read 给 EIO，master FD 仍打开。只在“已经 Start 且身份验证的 PTY master”模式把最后 slave 关闭 EIO 解释为输出 FIN；先保留 n>0；snapshot 保存该 source-kind。通用 Output 的 EIO 仍是错误，不能统一吞成 EOF。
2. **PTY writer 不能复用 pipe Input 的 FIN Close。** 一个 terminal owner 唯一拥有双向 master，读写 adapter 借用，导出一次。逻辑 stdin FIN 仅终止输入源，不关闭 master、不自动 SIGHUP、不注入 0x04。真实 Ctrl-D 按当前 ICANON/VEOF 处理；raw 模式下不是通用 EOF。
3. **Session 的控制面要进入同一屏障。** 当前 Adopt 主要移交 wait/signal；WriteInput、SetSize、Signal、Hangup、Close 还需 terminal-resource 的 admission/freeze 控制。Hangup 的整个 Signal→Close 序列不能跨快照；resize/信号保存序号与已应用进度，旧 generation 不能迟到重放。
4. **替换 agent/run 的直接 NATS 泵和 200ms 尾流收尾。** 进程退出不等于后代 slave 输出 EOF。保原 PGID 的 AdoptDraining 要在真输出结束后 AllowReap，不能先 Wait 再等待需 AllowReap 才能完成的条件。输入使用 seq/ACK 与有界队列，不能回调直接堵在 master.Write。
5. **保留原内核终端状态与绝对期限。** 不重开 PTY、不 reset termios/TCSAFLUSH，不把初始 shell pgrp 当当前前台任务组；保留 winsize、实际前台作业、授权/liveness 到期时间。外层 carrier 换代不创建新 terminal 或延长权限。
6. **缺少的真实验收。** 堵塞 raw slave 下 partial write→同 PID exec→原 PID/PTY identity/双向字节无重无丢；canonical/raw 的逻辑 FIN 与真实 Ctrl-D 分离；后代持有 slave 超过旧 200ms；resize/前台作业/SIGINT/Hangup 与 exec 交错。现有 pollability 测试不能证明这些，也不能证明 500ms 全映像 freeze 预算。

详细只读草案仍在 `/tmp/tether-control-recovery/hot-update-pty-stream-wiring.md`。Linux EIO 来源是 N_TTY 的 TTY_OTHER_CLOSED 分支；[Linux 源码](https://github.com/torvalds/linux/blob/master/drivers/tty/n_tty.c)、[creack/pty v1.1.24 ioctl](https://github.com/creack/pty/blob/v1.1.24/ioctl.go#L8)、[termios](https://www.man7.org/linux/man-pages/man3/termios.3.html)、[TIOCSWINSZ](https://man7.org/linux/man-pages/man2/TIOCSWINSZ.2const.html)已在此前调研核对，本次归档没有发起新网络查询。

## 5. startExecPipes：已应用的提取与唯一所有权

Root 已应用共用启动提取，位于 `internal/agent/exec.go:169`，仍在原文件，没有新增生产文件。API：

```go
startExecPipes(a *Agent, pid string, req *proto.ExecReq) (*workload.Stdio, string, error)
```

调用者先做请求/argv 验证、稳定任务身份预留和 admission 协调。helper 执行原有 buildExecCmd、NewStdio/Bind、可选 spawnexec.Prepare、startBounded/Start、CloseChild、registerExecChild→Manager.AdoptDraining。

- **成功：**返回原 parent pipe ends 和 warning；Manager 已是唯一 wait/signal owner。调用者负责 stdin/EOF、两个 output actor、输出责任结束后的 AllowReap/Wait、courier/终态、stdio/资源清理。不得再 cmd.Wait 或创建第二 waiter。
- **失败：**返回 nil Stdio，关闭未转移管道；adoption 失败仍由启动方 SIGKILL 原 group + cmd.Wait，因为此时 Manager 未接管。safe spawn abandon 仍是原 Cancel→关闭 reader→reapAbandonedExecChild→wedge-slot 生命周期；helper 没有把可能内核 D-state 的 target 直接放回 Go goroutine。
- **Warning：**只在 safe Start 成功后取得，后续 adoption 失败仍保留。legacy runChild 发原 stderr chunk+换行，再处理错误或开始输出泵；相对旧实现，warning 发送时机由 Start 与 registration 之间移到了 helper 完成之后。新 reliable runtime 使用 stderr replay；这正是 F2 超容量反例的合法注入点。
- legacy runChild 的 courier defer、stdin writer、两个 stream goroutine、AllowReap/Wait 与 signal note 继续存在；提取不声称这些旧直接泵本身已经支持 exec handoff。

提案 `/tmp/tether-control-recovery/exec-pipes-proposed/proposed.patch` SHA `6e6d8fdaae980abc1ef52f557637669cab0f38a637e6de4eab9744cefb3cb723`；提案完整 README 与 delivery-sha256.json 同目录。提案 exec.go SHA `6a4255cae461603828838510b3a8a06804e5d0b972eca587b213d71b4399a1be`。Root 后续在 exec.go 加 reliable adapter，故交接时全文件 SHA `4fb2b8d9beefe1114a793f6461b73691aaa721a148edaa91163476ee9376e464` 与提案不同是已知事实；原 overlay 收据不能覆盖后续整个文件增量。

原始现有 14 测试 overlay `-race` **PASS rc0 14.741s**，日志 `/tmp/tether-control-recovery/exec-pipes-overlay.log` SHA `24653136549c5123827d28602dce6a78e9885bfbe32a48706621e0f823521dda`。覆盖 explicit stdio bytes/EOF、原 task 管理/控制 kill、后代、reconciliation、safe helper、环境/cwd、abandoned child。没有新测真实 kernel D-state，没有跑整个矩阵。

可复用原命令（仍引用提案 overlay，评估当前生产时应有意识地去掉 overlay，而非拿旧文件覆盖新 adapter）：

```sh
go test -race -overlay=/tmp/tether-control-recovery/exec-pipes-proposed/overlay.json ./internal/agent -run '^Test(ExecExplicitStdioPreservesInputAndEOF|ExecOwnershipSnapshotAndControlKill|ExecDescendantsRemainKillable|ReconciliationDropsOrdinaryExecWithBoundedEscalation|RunChildActivePathUsesSpawnHelper|BuildExecCmd_.*|StartBounded_abandonClosesPipesAndReaps|AnAbandonedExecChildIsKilledNotJustWaitedOn|ExecChildEnvCarriesTheRequestedCwdAsPWD)$' -count=1 -timeout=90s -v
```

## 6. 本 lane 运行会话与资源清单

- 当前运行中的 exec/Go/Docker 会话：**无**。
- 最近 `functions.exec` 产生的测试 PTY session `64724` 与 `50768` 都已完成并取得 raw rc=1；不存在等待继续写 stdin 的会话。
- 四条 runtime 测试真实子进程由测试 cleanup/Manager 回收；没有保留后台进程或外部服务。F1 的测试残留在断言后第二次 Close 清理，不能误称缺陷已被产品自行修复。
- 本 lane 此前真实 H0/Caddy smoke 资源已按原报告 teardown；本次任务没有再次启动 Docker。镜像由 root 保留作已授权复用，不能把它们视为当前源码镜像。
- 本交接之后不再等待修复/自动复验；后续执行由 Claude 或 root 新的明确工作安排接续。

## S5 current contract addendum (2026-10-03)

The dated stop-point observations above remain historical. S0–S4 follow their own plans/receipts;
this addendum does not overwrite earlier hashes or turn historical NOT_RUN into a current result.
The [binding S5 plan](hot-update-flow-carrier-plan.md) and
[live architecture §22.8](../distributed-broker-architecture.md#228-s5-flow-and-ss-domains-binding-contract)
define flow/SS ownership. [Final S5 receipts](hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index)
are NOT_RUN pending P29.

Endpoint owns public listener/socket/journal/fence, rawsocket owns original kernel sockets,
flow owns replay/offset/FIN, SS actor owns parser/nonce/cipher suffix/material/authority;
carrier TLS/yamux is replaceable and not image state. Each image resource has one owner;
Prepared duplicates are separate cut-owned descriptors, and all private state uses sealed memfd.
PREPARE occurs before global freeze. Final Prepare re-exports every domain, including late revoke/fired/cleanup.
Restore takes all resources frozen with no workers, then Manager/revoke → raw pumps (SS AuthorityPending)
→ TLS/yamux REGISTER/exact OK → fresh bounded local-applied RESUME → SS.Authorize/data bind → NATS/register.
Any partial activation creates current-state responsibility; rollback cuts current offsets/nonces/retirement
at generation+1, never Detaches the pristine bundle. Exec failure Thaws originals and reacquires SS authority.

Flow ACK is original raw Write's n, with dedupe before Write; received≠applied, short suffix retained.
Source EOF fixes FIN End and CloseWrite once at Applied=End, preserving reverse tail. Abort preserves actual
prefix/final receipt without fake EOF. Retired OPEN never redials after receipt GC; SO_ERROR has one consumer.
Revoke irreversibly shuts originals/dup and bound public flow, including silent victims; legitimate survivors
are checked separately. SS uses actual-material salt history and an independent subkey+nonce ledger.

S4 failures §4's tail/partial/control/deadline/identity/EOF responsibilities require same-S5-candidate
36.A/W/C/L and39.P regression plus their units/mutations. PTY finalPrepare lost-event + same-SID recreation
remains S8. Broker same-PID exec/Endpoint export remains S6; NATS/Caddy switching S9; final mixed rollout S10.
500 ms full required-domain freeze is calibration with max/excesses, normal business max5 s is hard.
Darwin native lifecycle is NOT_RUN without a runner, distinct from cross-build and nonblocking under S4 L6.
Completion still requires original test/drill/docs/review steps and actual receipts; this addendum signs none.
