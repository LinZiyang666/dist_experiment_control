Pass（仅 process-table ExportProcesses/RestoreProcesses 基础，不是完整热更新验收）

## 本路所有权与执行边界

只新增 `internal/workload/process_state_test.go`、`internal/workload/process_state_exec_test.go`。生产、agent 接线、gate/inventory 都由 root 管理。真实 drill 34 结束、root 明确允许后才运行新增定向测试；未运行 Docker、完整矩阵或 lint。origin 为 `docs/reviews/hot-update-plan.md §4.2`。

## 独立测试：6 个 Test

| Test | 文件行 | 验证的实际行为 |
|---|---|---|
| TestProcessStateRejectsInvalidImportWithoutSideEffects | process_state_test.go:26 | 17 个非法版本/owner/child/terminal 状态臂；其中真实同组成员验证 PGID 条件、真实 waitable zombie 验证未消费退出；valid-first/bad-second 拒绝后表仍空、未冻结且可重新导入；原活 child 最终仍精确 rc31，zombie rc37，同组成员 rc43。 |
| TestProcessStateExportRequiresFrozenOriginalIdentity | process_state_test.go:122 | unfrozen 导出拒绝，缺失身份只在真实 child 校验后补齐，已有错误 boot/ticks 不得覆盖；导出记录不反向别名 manager。 |
| TestProcessStateTerminalRecordsNeverOwnReusedPID | process_state_test.go:164 | 历史终态收据的 OSPID 故意指向当前真实活 child；不打开 wait/signal 权限；保留 WaitError、Exit 原值与深拷贝；SignalAll 后该 child 仍自行以 rc41 结束。 |
| TestProcessStateThawAndAdoptKeepOneWaitOwner | process_state_test.go:214 | Restore 成功仍 frozen；二次 Restore 拒绝；8 个并发 Thaw 与额外 Adopt，多个观察者得到同一真实状态；不会消费非 owned child 的 rc43。 |
| TestProcessStateRepeatedRestoreReleasesResources | process_state_test.go:218 | warmup 后显式 5 轮同上真实 import/wait/forget；内建 goroutine/FD 检查，FD 容差 0。 |
| TestProcessStateExecPreservesOriginalChildren | process_state_exec_test.go:44 | 真 syscall.Exec 保持原父 PID；活 child 跨代沿原 pipe 收输入、原 zombie 保 ReapHeld、真实终态 status/Rusage/EndedAt 不变、WaitError 收据保留；导入冻结时拒 Signal/AllowReap；Thaw 后 held child 仍 waitable，显式 AllowReap 才得 rc37；非 owned child 仍由原父独立取 rc41。 |

终态复用 PID 用“历史记录指向当前真实进程”的可控 fixture，不强行制造内核 PID 回卷。WaitError 是明确标注的既有错误收据 fixture，不冒充一次新发生的内核 wait 错误。same-PID exec 的正常 terminal 状态来自真实 child 的 Wait4。

## 最终正例收据

命令：`go test -race -count=3 -timeout=150s -run '^TestProcessState' ./internal/workload`

结果：PASS，4.861s。日志 `/tmp/tether-control-recovery/workload-process-state-race-final.log`。

首次正例（尚未补独立 PGID 反例臂）PASS 4.838s，`workload-process-state-race.log`；最终以上述 final 日志为准。

资源锚点：`process_state_test.go:218` Test → `:229` exerciseProcessRestore；显式 5 轮在 `:222`，内建 g/FD 断言在 `:225/:226`。辅助 same-PID exec 函数：`process_state_exec_test.go:79` before、`:147` after。

## /tmp Go overlay 负控制

1. 导入时强行清掉 ReapHeld：仅跑真实 exec Test，期望 FAIL，0.116s；新的 reaper 消耗原 held child，WNOWAIT 实测 `ECHILD`。日志 `workload-process-state-hold-negative.log`，overlay `process-state-drop-hold-overlay.json`。
2. 导入终态时不深拷贝 Exit：仅跑 terminal/reused-PID Test，期望 FAIL，0.033s；修改 caller snapshot 后 manager.Wait 实际返回篡改的 rc99。日志 `workload-process-state-alias-negative.log`，overlay `process-state-alias-exit-overlay.json`。

所有负控制仅替换 Go build 输入的 `/tmp` 文件，生产未改。第二项避免向复用 PID 发送危险信号。

## 源审意见与未覆盖面

- root 实现已覆盖先前反馈：原 owner PID/startticks/boot；live 的原 parent/group/startticks；原子全表校验后才分配句柄；unused receiver；terminal 不读其可能已复用的 PID；ReapHeld 不丢；导入冻结、Thaw 单 poller；失败不 Wait/kill/部分安装。
- 本机 Go 的 FindProcess 在 Unix 可对不存在 PID 返回 nil error，并可在 pidfd 失败时退化为 PID 模式，因此不能取代真实 /proc 身份验证；当前实现没有这么用。
- 额外 non-consuming Waitid 验证 wait 权可作加固，但在“旧 image 已消失、无其他 waiter、manager frozen”现有调用前提下，未证实需要它才能保证正确。本报告不声称承受绕过 sole-owner 前提的任意并发裸 Wait4。
- 未模拟 procfs 不可读、FindProcess 资源错误、跨 PID namespace、agent 权限变更；未跑不支持平台、完整 fd 白名单/业务 I/O 恢复、courier 状态迁移、自动回滚或四层升级。
- 本组进程表测试没有测量 500ms/5s 产品预算；没有用基础 PASS 抵消真实 drill 的产品失败。

## 文件 SHA-256

- process_state_test.go: `78fdaaca10b12411115a34957a58c6d475f12a2c12a089b842db718f294b65a2`
- process_state_exec_test.go: `952b2d276f1428f1effa5f385c352bacd0dad7279d798e697c1375caf7ec3ba1`
- process.go: `316d203109cbc3ee610870fde9d30a43711ed89bfc1cd9879b91f35d574782cb`
- process_state.go: `53d5549f7b16c5dfe39064fa443b332d1c93e2cca6f221127164baa95a458e87`
- process_state_linux.go: `df4e52be92ebd3570d11956a07fba28f643d306e81e8a7878138e165d37a0f59`
- process_state_unsupported.go: `166ddce239d2a11af8111ed2b8498656daa6faafd45e499294384ced2223c0fe`

无新增生产阻断 finding；本阶段报告到此停止扩测。
