Pass — internal/handoff 基础层的独立真实 exec 测试与两项负控制通过；不是完整热更新能力的验收。

# Handoff 真实进程测试报告

日期：2026-09-20。本人只新增 `internal/handoff/image_exec_test.go`，未修改生产、root的image_test.go或公共测试清单。最初草稿 `/tmp/handoff_exec_test.go` 已同步为最终版本。root确认API后才放入repo；未运行全量矩阵/lint，未操作现网。

## 1. 新增测试

| Test | 独立行为 oracle |
|---|---|
| TestExecPreservesChildAndOriginalTCPStream | before/after为同一个实际测试子进程的两次执行映像；Exec返回则立即失败。原父PID保持，exec前启动的/bin/sh业务在同一stdin/stdout pipe上继续交换带nonce字节，exec后原父Wait4收到同一child的exit23，再Wait得到ECHILD。父测试端只Accept一次并持有同一TCP Conn，前后双向挑战与最后字节均成功。 |
| TestExecFailureKeepsOriginalFilesAndFlags | 同一Prepared连续两次执行不存在路径均报错；调用者原FD的F_GETFD/F_GETFL逐位不变，非阻塞pipe仍收发。每次失败后Prepared尚活着时启动真实audit子进程，按dev/inode检查其dup没有泄漏。Prepared.Close后原文件仍可用。 |
| TestHandoffRepeatedExecReleasesResources | warmup后5轮上述真实exec生命周期，另5轮Prepare/两次失败exec/Close；内建goroutine基线及FD零容差门。每轮包含child reaping和真实TCP关闭。 |
| TestHandoffExecSubprocess | 隔离的before/after/audit模式入口，正常测试进程直接返回；不是mock Exec。Restore后private env消耗，第二次Restore应nil,nil；新业务子进程Restore也nil,nil且无导入FD。 |

成功exec还独立验证：

- 原本CLOEXEC的非白名单secret文件未继承，按dev/inode而非数字FD判断，避免Go启动时复用fd造成假失败/假通过。
- 导入文件立即恢复CLOEXEC；Resource.Files元数据不暴露第二份owner。
- 未Take的文件在import后确有一份，Restored.Close后消失；已Take的pipe/socket仍可使用。
- 业务audit子进程不通过ExtraFiles接收任何资源；fixture仅移除自己测试mode键，保留生产私有env，因此不能通过测试主动删除生产marker来制造环境清理假绿。
- 所有helper都由原父测试的30s控制预算和独立进程组约束；失败时关闭/杀死并Wait，记录helper完整输出，不留下阻塞shell。测试时间预算属于本机race fixture等待，不能据此声称产品已达到500msfreeze或5s控制SLO。

## 2. 真实命令与结果

1. `go test -race -count=1 -timeout=3m -run '^(TestExecPreservesChildAndOriginalTCPStream|TestExecFailureKeepsOriginalFilesAndFlags|TestHandoffRepeatedExecReleasesResources|TestHandoffExecSubprocess)$' ./internal/handoff`
   - PASS 28.182s；日志 `/tmp/tether-control-recovery/handoff-exec-first.log`。
2. 临时overlay把Exec改为错误地清除全部低位FD的CLOEXEC，仅跑TestExecPreservesChildAndOriginalTCPStream。
   - **期望FAIL，实得FAIL 0.095s**；helper明确报 `inherited forbidden open-file identity ...`，主测试原流随后EOF。
   - overlay `/tmp/tether-control-recovery/handoff-exec-broad-inheritance-overlay.json`；日志 `handoff-exec-broad-inheritance-negative.log`。
3. 临时overlay把Restore导入FD的保护改为F_SETFD(0)，同一Test。
   - **期望FAIL，实得FAIL 0.096s**；helper明确报 `handoff-process-stdin is missing FD_CLOEXEC`。
   - overlay `/tmp/tether-control-recovery/handoff-exec-unprotected-import-overlay.json`；日志 `handoff-exec-unprotected-import-negative.log`。
4. `go test -race -count=1 -timeout=3m ./internal/handoff`
   - **当前正式源码整包PASS 28.232s**（包括root已落的image_test.go）；日志 `/tmp/tether-control-recovery/handoff-exec-package-final.log`。

负控制均在/tmp副本和go-overlay完成，未修改生产源码。整个包最终复验在两条negative后运行，防止只留错误版本测试结果。快照SHA见 `/tmp/tether-control-recovery/handoff-exec-reviewed-sha256.txt`。

## 3. 覆盖边界

- 本层假设调用者已经quiesce；没有证明如何停止活跃goroutine/阻塞syscall、序列化任意Go堆或构造完整业务manifest。
- 使用同一真实测试binary执行两次，证明实际exec和FD/父关系机制；不是两个正式release的N-1 ABI兼容矩阵，也不是daemon早期boot shim接线。
- TCP保留为raw字节流，不含TLS/yamux状态、PTY、SS、broker/Raft/SQLite、NATS/Caddy、file-transfer checkpoint/终态或ctl可靠协议。没有真实simcluster热更新drill。
- 5轮资源检查包含父测试进程的Prepare失败/Close资源与完整子进程生命周期；没有宣称同一个长寿命runtime连续5代升级的整体资源门已完成。Restored未取资源释放有单次明确身份oracle。
- 非白名单测试按已确认契约只要求原本CLOEXEC资源不被库打开继承。调用者主动清CLOEXEC的其他资源不由此基础层扫描封堵；后续runtime需自己的完整资源政策。
- 不证明任意kill-9/断电、exec成功后新映像崩溃的业务保活；本次只验证exec返回失败时旧映像和原FD仍可工作。
- syscall.ForkLock与并发fork的线性化、损坏manifest/identity/配额等更广泛场景不由本文件完整覆盖；root的单元测试及后续独立审查负责相应面。

## 4. root整合事项

新Test共4个。leak门锚点登记：`internal/handoff/image_exec_test.go:TestHandoffRepeatedExecReleasesResources` → `exerciseHandoffExec`（显式5次循环内调用），同循环还调用exerciseHandoffExecFailure。测试清单/矩阵归属由root统一登记；本lane未改公共清单。
