Pass — 新 PTY ownership 委派的独立真实测试；不代表 I/O freeze 或热更新通过。

## 范围

仅新增 `internal/pty/process_owner_test.go`，生产与公共测试/gate未改。
真实 Linux PTY、/bin/sh、stty、终端输入/输出/resize、Session.Adopt/Wait/Signal/Hangup 与真实workload回收；无 NATS、agent、Docker、image switch或import。
新增3个Test（其中拒绝测试有3个子例），origin为docs/reviews/hot-update-plan.md §4.2。

| 锚点 | 实际覆盖 |
| --- | --- |
| :41 TestOwnedSessionPreservesPTYIOAndResizeAcrossFreeze | 原terminal输入回显协议；freeze后Signal(SIGTERM)返回ErrFrozen、Hangup不关master；仍可resize至137×43并由child stty读出43 137；thaw后同terminal继续输入，Session.Wait精确rc37且owner保留结果一致 |
| :96 TestOwnedSessionKeepsPIDAndMasterAfterProcessRelease | 确认真Process.Release使原Process.Pid=-1，Session.OSPID仍等于原正PID；Signal(0)返回ErrExited、Hangup不关master、原输出尾部可读取、第二次owner委派Wait仍精确rc17 |
| :137 TestSessionAdoptRejectsCompetingAndPriorWaitOwners | 同owner同ID、同owner别名、不同owner都拒绝重复；未委派Wait已完成后拒绝Adopt；真实未委派Wait进入且child仍阻塞时拒绝并发Adopt；原waiter仍得到rc29而不被抢reap |

回收后信号检查使用Signal(0)，避免错误实现把Process.Pid=-1取负后误向PID1发真实信号。
fixture保存master局部指针供reader，cleanup先解冻/结束child、唯一Wait完成再关闭master并join reader；已Wait不再发raw signal。

## 实际运行

`go test -race -count=3 -timeout=90s -run '^(TestOwnedSession|TestSessionAdoptRejectsCompetingAndPriorWaitOwners)' ./internal/pty`

PASS **1.155s**；原始收据 `/tmp/tether-control-recovery/pty-process-owner-race.log`。
未重跑旧PTY/agent/P5套件、全量gates、Docker；未新增overlay或声称这些测试已覆盖完整runtime迁移。
本范围未发现生产缺陷。Adopt拒绝正在执行的unmanaged Wait通过真实Wait进入状态屏障观察，未用sleep猜测ready。

## 快照

- pty.go：b2f98052337eb49504002894e352acf7014c5a643cda5f7ef9707eab086fedc9
- process_owner_test.go：f63f4aa174ef9e400d5481e709e6d136abe66a5d349bd226626e814620b7d53c
- workload/process.go（含Usage）：f736a951b51d4ae5c1e7cb226b74c1aa63aa7052a15a82fd48f9c10b60e703bc

root负责3个Test的inventory/矩阵登记；本次没有另造资源门或修改此前process_test.go。
