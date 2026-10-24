Pass（仅 Output actor 基础；未接 agent 升级，不代表完整热更新）

只新增 owned `internal/workload/output_test.go`、`output_exec_test.go`，未修改 production/gate。origin：`docs/reviews/hot-update-plan.md §4.2`。

## 8 项独立测试

- `TestOutputBackpressureFreezeRetainsEveryByte`：真实 writer 写量超过实际 kernel pipe 容量+log；满 log 确实堵住原 writer；Freeze 不关闭 source；Thaw/ACK 后全部 bytes 精确相同。
- `TestOutputBlockedReadRepeatedFreezeAndOriginalEOF`：5 轮 blocked-read Freeze/Thaw；原管道每轮持续可用；原 EOF 到达后 Replay.Read 同时返回 n>0+EOF 的最后字节不能丢失。
- `TestOutputCanceledFreezeKeepsOriginalOwner`：提前取消；另将真实 actor 的首次调度延迟，使取消确定发生于 Freeze 等 done 的路径。只控制 goroutine 调度，字节、FD、Freeze/thaw/run 都为真实实现；返回取消后原 owner 继续原 pipe。
- `TestOutputStorageFailureRetainsBoundedPendingSuffix`：paused owner 的私有 replay FD 故障注入为同 memfd 的只读打开；真实 source n>0 后 Append 失败。验证已存 prefix、Pending 与原 pipe 未读余字节拼接等于全部原数据；Pending 原始字节≤32KiB；snapshot/restore 深拷贝且保持 ReadError，不伪造 FIN。候选恢复前重新以 RW 打开 memfd 并通过 RestoreReplay，未绕过合法 FD 校验。
- `TestOutputRestoreRejectsInvalidStateWithoutTakingResources`：6 个错误 metadata 臂，均不接管/关闭/读取 caller source，不夺 caller replay。
- `TestOutputRejectsInvalidDescriptorWithoutChangingFlags`：New/Restore 各覆盖 write-end、blocking、inheritable；失败不改变 flags，原 caller 仍可读写 source 和 replay。
- `TestOutputRepeatedLifecycleReleasesResources`：warmup+5 轮真实 producer/consumer/Freeze/Thaw/Close，内建 g/FD 检查，FD 容差0。
- `TestOutputExecRetainsOriginalPipeAndUnacknowledgedBytes`：真 syscall.Exec，同 reader PID和原pipe inode；原外部 writer 在 exec 前被堵住，跨代继续；1024B 已读未 ACK prefix+其余原流合计256KiB，完整 SHA和原 EOF一致。正常 metadata 小于4KiB，流数据留在 memfd。

资源锚点：`output_test.go:353` Test → `:363` exerciseOutputLifecycle；`:356` 显式五轮，`:359/:360` 内建 g/FD。exec helper：`output_exec_test.go:119/:174` before/after。

## 发现及处置

已向 root 指出 `SetReadDeadline` 不能证明 source 可读：pipe 写端也支持 deadline。root 在 stdio.go 增 validateStreamFile，用 SyscallConn 验 access、O_NONBLOCK、CLOEXEC，失败不改 flags/不夺权；本路正例和负控制已验证。

首次 exec fixture 把 raw pipe 经 os/exec.ExtraFiles 传入，File.Fd 使其变 blocking，被该新校验正确拒绝。只修初始 fixture bootstrap：先 SetNonblock+CloseOnExec，再 os.NewFile 登记 poller；真正 handoff 不做 flags 修补，仍要求原样保持。此红为 fixture 问题，不计产品缺陷。

## 原始收据

- 全部8项：`go test -race -count=3 -timeout=180s -run '^TestOutput' ./internal/workload`，PASS 4.546s，`workload-output-race-final.log`。
- 按 root lint反馈修检查receipt写入错误、简化context声明后，仅复验exec一次：PASS 2.113s，`workload-output-exec-final.log`。之后仅把错误路径的 `%v` owner诊断改 `%p`，避免负控制里reflect读取运行中的mutex。
- `/tmp` overlay删除Pending保存：期望 FAIL 0.036s，真实source一次读出32768B、memfd写0B报错，Pending=0被抓到；`workload-output-pending-negative.log`。
- `/tmp` overlay跳过source校验，仅write-end New臂：期望 FAIL 0.032s，返回非nil owner且无err被抓到；最终干净负控日志 `workload-output-invalid-fd-negative.log`。初次负控额外揭示上述测试诊断自身竞态，已修并重跑；未归因为生产竞态。

日志均在 `/tmp/tether-control-recovery/`。未跑全套Go/lint/Docker；root统一登记inventory、leak anchors与最终门。

## 边界

Pending上限指原始字节32KiB；JSON会Base64膨胀，加ReadError后不能声称整个OutputState JSON≤32KiB。错误Pending由后续domain恢复决策处理，测试不声称actor自动重试修复存储。取消与资源测试不证明包含 mutex 等待、Go init、全部业务域的500ms/5s门；未覆盖agent接线、PTY、TLS/yamux、N-1控制协议、四层滚动升级或真实业务无中断drill。与tunnel lane已做源码交叉，未确认其他阻断。

## 最终SHA-256

- output_test.go: `4b1861d1ba3cfa558e8373160eadf5e19102b46c90f676dfac8ef9df20dbf018`
- output_exec_test.go: `2efebd06e1671758463cb3b3dc63b844992d5dadebab6a789aafb4e908e87e26`
- output.go: `ba91dab927f788f51923e56147294e8c0f0dd1ec3663e5aeda3852a801039187`
- replay.go: `ace564d8bc652c8dce0550a5caf0d04900052cb40b6c673abccd2fe19545c495`
- replay_linux.go: `9b5cd66cba6487daa5f99ecc7077e53b54620b41df79dda5fbfe45b7f193c06a`
- stdio.go: `a8eb7466d02fff07cc07bb5512a6632a75ed9c6b85af7183047c25e2290cfe44`
