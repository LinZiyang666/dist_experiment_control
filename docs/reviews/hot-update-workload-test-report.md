Pass — 仅 process registry 的独立测试与子屏障；不代表业务 I/O freeze、agent 接线或热更新完成。

## 范围与变更

- 只新增 `internal/workload/process_test.go`；生产 `process.go` 由 root 所有，本路未修改。
- Linux，真实 `/bin/sh` child、显式stdin pipe、独立process group、真实Wait4/Waitid(WNOWAIT)；无NATS、Docker、完整exec/handoff或业务stdio adapter。
- 8 个 Test；共享资源门用同一 Manager 预热后 5 轮真实 adopt→freeze→child退出→WNOWAIT→thaw→wait→forget。
- 对外部child保持调用者Wait责任；专门的进程组测试用两个直接child同组，避免留下孙进程或改变全进程subreaper策略。

## 测试清单与锚点

| 行 | Test | 实际断言 |
| --- | --- | --- |
| 42 | TestManagerReapsExitForEveryObserver | 正常rc23/SIGKILL rc-1；8观察者同一保留结果与EndedAt；原kernel状态已被唯一回收；终态Signal拒绝 |
| 106 | TestManagerFreezeRetainsWaitableChildAndRejectsMutation | 冻结后正常退出，WNOWAIT证明waitable；Wait不提前完成；拒绝重复Freeze/Adopt/Signal/SignalAll/Forget；Thaw后准确rc37；拒收child仍归caller |
| 163 | TestManagerFreezePreservesAlreadyRecordedExit | freeze前已reap的rc41仍完整；冻结中可观察终态，不能Forget |
| 191 | TestManagerSnapshotCopiesRecordsAndRejectsDuplicateOwners | 重复task ID/live OSPID拒绝，拒忘live；有序snapshot；修改Identity/Exit与Freeze结果不反改Manager；Forget后Unknown |
| 258 | TestManagerDoesNotReapUnownedChild | unowned child先成为zombie，强制manager处理owned退出后，caller仍能取得原外部rc47 |
| 279 | TestManagerSignalAllStopsOwnedGroups | 两个独立owner均收SIGKILL；保留终态不导致SignalAll再次发信号 |
| 301 | TestManagerSignalReachesWholeProcessGroup | Signal必须影响整个group，单杀leader不能通过；同组非manager owner的child仍由caller Wait |
| 330 | TestManagerRepeatedLifecycleReleasesResources | 335行显式5轮，342行exerciseProcessLifecycle；内建goroutine/FD helper，FD tolerance=0 |

Test文件 origin 为 `docs/reviews/hot-update-plan.md §4.2`。root负责inventory、race matrix、leak anchor登记；建议重复门anchor指向 `exerciseProcessLifecycle`。

## 实际运行与负控制

1. 最终 `go test -race -count=3 -timeout=90s ./internal/workload`：PASS **1.846s**。
   收据：`/tmp/tether-control-recovery/workload-process-race-final.log`。
2. `/tmp` Go overlay 把reapLoop的`if !m.frozen`改成无条件reap：定向freeze测试预期FAIL **0.045s**。
   真实失败是child rc37被提前reap，`Wait completed before Thaw`（测试140行），不是编译失败或测试超时。
   收据：`workload-process-ignore-freeze-negative.log`，overlay：`workload-process-ignore-freeze-overlay.json`。
3. `/tmp` Go overlay 去除snapshot Exit深拷贝：定向snapshot测试预期FAIL **0.045s**。
   真实失败是测试写入的Code88/WaitError=forged污染保留结果（测试241行）；未更改Signal终态检查，未向复用PID发信号。
   收据：`workload-process-alias-snapshot-negative.log`，overlay：`workload-process-alias-snapshot-overlay.json`。

两项负控制仅替换Go构建输入，未写生产文件；它们没有覆盖最终源码或放宽断言。
此前7项初版整包race×3 PASS1.493s不作为最终8项的替代收据。

## 已知边界

- 无restore/import、完整exec、stdio pump/PTY/session destroy/remote FS helper整合测试，均仍由后续接线工作包验证。
- Wait为现API的无context观察；用测试侧deadline防卡死，未宣称Manager新增等待超时或自动kill语义。
- 多观察者与真实child并发使用-race；未证明所有scheduler交错或任意崩溃exactly-once。
- cleanup对已Forget的ErrUnknown不再裸阻塞Wait；仅在错误terminal留下仍属本测试的child时，以WNOHANG验证后kill+wait兜底，避免负控制遗留进程。
- 未发现本次覆盖内的生产缺陷；兼容接线风险另见 `/tmp/hot-update-workload-compatibility.md`。

## 最终源码快照

- process.go：`bb19f31ba99ccaffd91f9e2b401c54af540faff0933510cc697a2095b917c684`
- process_test.go：`4935240f576a4ddb43c00d15e9f52e286b9ef5f0f4271abed061430e77877d38`
