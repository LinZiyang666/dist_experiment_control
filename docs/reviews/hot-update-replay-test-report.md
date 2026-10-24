Pass — 限于 Replay 单资源与真实 same-PID handoff；不代表应用热更新已完成。

## 范围与收据

本路仅新增 `internal/workload/replay_test.go`、`replay_exec_test.go`，未改生产/公共测试/清单。生产审查覆盖 replay.go、replay_linux.go、replay_unsupported.go；与 transport 路交叉后无已证阻断。

- `go test -race ./internal/workload -run '^TestReplay' -count=3`：PASS 24.577s，`/tmp/tether-control-recovery/replay-race-final.log`。
- 最后补强 exec 后原 memfd 的全部 dev/inode FD 引用计数后，仅重跑受影响组：`go test -race ./internal/workload -run '^TestReplayExec' -count=3`：PASS 23.644s，`/tmp/tether-control-recovery/replay-exec-final.log`。
- 基础组与真实 exec 首轮分别 PASS 1.281s / 8.541s；后者收据 `replay-exec-initial.log`。最后两条命令为当前测试内容的正式收据；没有跑 Docker、整包、矩阵或 lint。
- 初次未提升权限调用因 Go cache 只读在 setup 阶段失败；其后获得工具权限，实际执行上述测试。不是产品失败。

## 独立 oracle / 负控制

1. 5 种容量（1、2、7、64、4097）的固定种子随机模型，各 1400 步；模型只保线性 retained bytes，不复制环索引实现。覆盖 append 部分成功+背压、ACK 旧值/未来值、跨环 Read、FIN 前后、未写区域未改变，且每容量实际跨至少10圈。
2. 真实 memfd grow/shrink seal、保持可写、16种状态/FD拒绝臂，失败时不偷走 FD 或容量；零内容流仍预留全部8MiB，32条填满256MiB，ACK不释放预留、Close恰好释放一次；24个并发申请争9份容量。
3. `uint64` 末端不环绕；真实 producer 与 Freeze 竞争，返回的快照必须包含每个已确认 Append 字节，冻结后无遗漏写入。
4. exec前两条流均实际跨20圈左右且保非空未ACK后缀，一条带FIN。通过真实 `handoff.Prepare → Exec → Restore → TakeFile → RestoreReplay` 后核对原PID、dev/inode、字节绝对序列、Base/End/FIN、起始冻结、只可TakeFile一次；bundle关闭不关闭已转移FD；恢复后继续跨至少10圈并验证部分成功的每字节。
5. 两个负控制仅篡改测试导出包/测试文件，不改生产：a. 导出Base+1，b. 实际memfd改坏一个未ACK字节。二者均必须在真实exec并成功导入handoff后触发原始独立oracle，返回失败且没有成功receipt。前者在 Replay restore 前发现元数据损失，后者在 Replay restore 后读取时发现字节损失。上层测试明确将这两种预期拒绝判为通过。

## 资源 anchors

- `TestReplayRepeatedRestoreReleasesResources`：预热后5轮实际 freeze/dup/restore/old close/new thaw/续写/FIN/close，函数内直接调用共享 `AssertNoGoroutineLeak`、`AssertNoFDLeak(...,0)`。
- `TestReplayExecRepeatedlyReleasesResources`：预热后5轮真实exec，外部测试函数内同样直接调用两个共享泄漏门。每轮新映像还核对导入FD总数关闭后恰减2、原两个memfd的全部dev/inode引用从2变0、原预算324字节可完整重新申请。不会仅以子进程退出后的OS回收充当内部Close证明。
- `TestReplayExecSubprocess` 只是上述真实 exec 的入口，正常无环境分派时无动作；不得把它单独计成独立行为证明。

## 给 root 的12个 Test 清单

`internal/workload/replay_test.go`:

- TestReplayMatchesLinearModelAcrossWraps
- TestReplayFreezePartialAppendAndFIN
- TestReplayRestoreRejectsInvalidStateAndDescriptors
- TestReplayBudgetReservesEmptyStreamsAndReleasesOnce
- TestReplayConcurrentReservationIsBounded
- TestReplayOffsetExhaustionDoesNotWrap
- TestReplayConcurrentFreezePreservesAcceptedPrefix
- TestReplayRepeatedRestoreReleasesResources

`internal/workload/replay_exec_test.go`:

- TestReplayExecSubprocess
- TestReplayExecPreservesUnackedBytes
- TestReplayExecOracleRejectsLostData
- TestReplayExecRepeatedlyReleasesResources

## 冻结 SHA256

```text
ace564d8bc652c8dce0550a5caf0d04900052cb40b6c673abccd2fe19545c495  internal/workload/replay.go
9b5cd66cba6487daa5f99ecc7077e53b54620b41df79dda5fbfe45b7f193c06a  internal/workload/replay_linux.go
e64e6fc91922178cf83a53ab0472d451a6823342d701ff107b46e36f91cd5487  internal/workload/replay_unsupported.go
ca7723bf3d42b8e2d7cd5c02725a4daa1d9b6a3282e0d21d908d1e4affc34890  internal/workload/replay_test.go
af6f02c8ff9f4c50ac08360834818536e523c691772c2fff303bdb9b62b247e2  internal/workload/replay_exec_test.go
```

## 覆盖边界

- Replay遵守显式单owner API：调用者全局freeze后借FD，成功导入后只激活一个owner。没有声称 RestoreReplay 能识别调用者恶意dup后同时Thaw两个对象；handoff.TakeFile防重复转移，域层屏障仍必需。
- 未测非Linux平台、agent/broker/ctl接线、远端ACK真实性、原source pipe/pump（其他lane负责）、重连/授权/多代版本、kill-9持久性、完整500ms/5s产品预算。
- 容量门证明逻辑预留与固定memfd大小，不声称构成整个进程RSS/cgroup限制；不会以这些局部测试抵扣完整真实业务drill。

测试内容已冻结；后续整包/门禁与正式清单归 root。
