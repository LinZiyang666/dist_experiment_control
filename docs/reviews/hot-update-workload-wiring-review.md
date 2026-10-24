# 有界实现复核：W1/W2 已提交修复，独立证据与边界如下

范围：本次 `internal/workload`、agent 普通 exec/PTY 接线、进程快照/退出 courier、`pty.Session.Adopt`。这不是完整热更新内审 Pass，不证明 image switch、500ms freeze、5s 新控制请求、四层升级或 simcluster drill 已完成。初始只读审查未运行 Go/服务；root 后续明确授权 W1 独立测试后，仅运行本文新增测试。生产修改、公共测试与 gate 均由 root 负责。

## W1 · High · 提前 reap 普通 exec leader 会失去仍活后代的 kill/evict 权限

**发现时链路**：`internal/workload/process.go` 原 249/266 行无条件 Wait4+记录 Exit；`internal/agent/exec.go:245` 仍先等 stdout/stderr EOF；manager 的 Signal/SignalAll 拒绝或跳过 Exit 已存在的任务；`internal/agent/run.go` 原 577 行已把 evict 委派给 manager。

**实际可达例子**：普通 exec 启动 `sh -c 'sleep 60 & exit 37'`。shell 先退出，sleep 仍属原 PGID 并持 stdout/stderr 管道。新 reaper 提前消耗 shell 的 wait 状态，handler 继续等 EOF；kill 返回 ErrExited，evict 跳过，sleep 可越过 agent 自退出继续存活。旧实现先排空管道再 cmd.Wait，期间保留 zombie leader，因此旧 evict 能杀该组。这是新增回归，不是任意 daemonize 后代的新增管理承诺。

**root 修法，已复核**：`process.go:80–85` 的 AdoptDraining 在同一锁内创建 ReapHeld；`run.go:561` 使用它；`exec.go:245–249` 在输出排空后 AllowReap；`process.go:260` 只有未 freeze 且无 hold 才消费退出。Signal 继续拒绝已 reap 的任务，不用裸已回收 PGID 弥补遗漏。Record 导出 ReapHeld。此法保留普通 exec 原有“输出生命周期内可管理整组”的边界。

**独立运行闭合**：新增 `internal/agent/process_descendants_test.go:33` 的 `TestExecDescendantsRemainKillable`，kill/evict 两臂。每臂在私有 test subprocess 中启用 subreaper，真实 shell/后台 sleep，不改变全包的 reaper；先验证 shell 为 zombie、sleep 已归该 helper 且与 shell 同 PGID、fd 1/2 仍为管道，再用非消费 WNOWAIT 反复证明原 child 仍 waitable。实际 KillReq 走 NATS handler；evict 臂走实际 reapManagedChildren hook。要求后代真收 SIGKILL，原 leader rc 精确为 37，manager 退役且 courier snapshot 保 37。helper 用精确 PID Wait4 回收其唯一后代，避免留给容器 PID 1。

- 最终命令：`go test -race -count=3 -timeout=120s -run '^TestExecDescendantsRemainKillable$' ./internal/agent`
- 最终结果：PASS，8.278s。
- 原始日志：`/tmp/tether-control-recovery/workload-descendants-race-final.log`。
- 负控制：仅在 `/tmp` Go overlay 把 `registerExecChild` 的 AdoptDraining 换回 Adopt；原生产文件不变，仅运行新增 Test 的 kill 臂。预期 FAIL，0.140s，真实 WNOWAIT 返回 `ECHILD`：`leader no longer waitable while descendant holds output`。
- 负控制日志：`/tmp/tether-control-recovery/workload-descendants-no-hold-negative.log`；overlay：`descendants-no-hold-overlay.json`，同目录。
- 新增 Test 数：1（两个子用例）。最终文件 SHA-256：`c1a8623ddbdb4a9c24dc36632cd65389848c062be803c3c11701508b9597d645`。
- 测试不声称覆盖 broker 身份认证、外部 evict 事件分发、远端文件系统 spawn 超时、刻意 setsid 脱离原组的后代，或完整 image switch。

## W2 · High · G.1 新纳入普通 exec，但 DropProcesses 仍只处理 PTY

**发现时链路**：`run.go` 原 624 行将普通 exec 纳入 LocalProcesses；`agent.go` 原 2151–2154 行的 killOrphanProcess 仅 lookupProc，普通 exec 命令直接返回。`broker/reconcile.go:310–349` 在有节点历史时会对真正无对应 row 的 running task 下发 DropProcesses。恢复旧 broker bundle、而任务是在该 bundle 后启动，是仓库现有的真实支持场景（该处 gotcha #87 / drill 94 注释），不需要恶意本地文件或假造身份。

**影响**：broker 的孤儿清理决定被普通 exec 静默忽略；其重新上报已启用、管理动作未接通，导致 task ownership 与 authority 不一致。

**root 修法，源码复核认可**：`run.go:512–516` 新 signalManagedProcess 先委派 PTY owner、否则 manager；public Kill 与 `agent.go:2147–2168` orphan 路径共用。SIGTERM 与 5s SIGKILL 都重新按稳定 task ID 寻 owner；unknown/exited 不再信号，升级协程保留 ctx 取消。没有从快照抽出裸 OS PID 后延迟 kill。

**运行证据边界**：本路没有宣称 W2 的真实 DropProcesses、拒 TERM 后 5s 升级、ctx 取消已经测试通过。root 负责其定向用例和最终 gates。建议该门至少包含真实普通 exec 收 TERM 退出、忽略 TERM 后 SIGKILL、取消时不再升级，另保留 PTY 兼容臂。

## 其余已核对的调用链

1. 成功 Adopt 后，正常 exec/PTY 只由 manager 消费内核退出；多 Wait 观察者读取 retained result。失败 Start、remote-FS helper abandon、失败 adoption 仍由原调用者 Wait，不与成功 adoption 重叠。
2. 显式 Stdio 先把 child 文件赋给 Cmd，再 Start；Start 成功后关闭父进程持有的 child 端；stdin writer 与 stdout/stderr pumps 的父端由 adapter 收束。不再依赖 os/exec 的隐式 stdin copier 和 Cmd.Wait 关闭父端。
3. PTY master/KA 仍归 Session/agent；Adopt 前 Wait 排斥并发转移，Signal/Wait 委派 manager，OSPID 保存不可变数值，避免 Process.Release 把指针上的 Pid 置 -1 后误用。相关本路既有 3 个独立 PTY owner Tests 的 race×3 收据为 `/tmp/tether-control-recovery/pty-process-owner-race.log`（1.155s，针对该时点文件，不冒称这次重跑）。
4. enqueueExit 在退休 task 记录前，联合表用 terminal 状态覆盖 running，可消除已登记 task 的该段退出交接缺口；读取顺序是 PTY表→manager→courier。若 courier 已被真实 ACK 清掉，broker 已有终态，不需要以该已确认项证明缺失。
5. 本次 Signal/reap 共锁；Snapshot 深拷贝 Exit；精确 Wait4 只等自己已 adopt 的 child，不使用 P_ALL 窃取 spawn helper/candidate 的退出状态。Freeze 仅覆盖 process table，不能独自代表 stdio/命令接纳已冻结。

## 必须保留的既有与未实现边界（不冒充本次新增缺陷）

- **启动窗口未完成**：`exec.go:134/139` 仍先发 started/enqueueStarted，再 build/Start/Adopt。starting、abandoned spawn 尚未纳入完整 admission/联合资源表。safe-spawn quota 的旧隔离行为应保持，但这个子步不能称为已实现全局 ownership census。
- **旧 register 回复的 courier 清理窗口**：`agent.go:1556` 在本次 register 起始只取一次快照，`proc_delivery.go:395–403` 却结算返回时的全部 pending。快照之后才接纳并完成的任务可以被不包含它的旧回复误清 exit；这是既有机制，后续必须绑定本次 snapshot 的对象/版本或由完整 admission gate 排除。enqueue-before-Forget 不能证明此窗口已关闭。
- **PTY 子树边界**：PTY 当前仍是 leader Wait 后最多 200ms 尾部排空；W1 的普通 exec hold 不等于 PTY I/O 迁移完成。raw master、KA、reaper、Wait 结果与授权绝对期限仍须在真正 image cut 联合导出。
- **计划 freeze 未接业务闭环**：manager 冻结时的 AllowReap/Forget/Signal 拒绝，必须由将来的入口/I/O/admission 协调处理；不能直接把 ErrFrozen 当普通业务结束后给 courier 编造 rc。
- **既有 session_destroyed 清理差异**：前置兼容清单已记录 requirements 的 session rm 清场要求与现有 agent 仅处理 agent_evicted 的差异；本次没有修也没有以此冒充新回归。

## 最终快照（本报告取样时）

| 文件 | SHA-256 |
|---|---|
| internal/workload/process.go | f0011f418905173d958f9bf1514a211bf8141c6ad27acafbcbcda30e12bfd3bb |
| internal/workload/stdio.go | 8461d84cea59a9342c5d91e192d85115679c86a38c215a47e505d8539f5a3f2a |
| internal/agent/exec.go | 0d2b441117a9e37b01cf448eb7a2de0145fd9c64af4cd0c0e45ffbd1be44f58f |
| internal/agent/run.go | 483983761538ab3c6f86bbb7e8704078cdf5f48adc6afc1e1e296835b8353324 |
| internal/agent/proc_delivery.go | ee9774faa91f89fe512f1c6fd370c50bfefdfbff240c723ca3a53e2e6616e668 |
| internal/agent/agent.go | 5c6233992c33c7af7fafb769e013c1ff6557c0d5ce07506b13c83f2adc61d61f |
| internal/pty/pty.go | 7ff8287bf963e66c5373fda93896303783d78d7465580bfb501d6179a94aedf1 |

未运行完整 Go 矩阵、lint、Docker 或 simcluster，未修改 production/gate、未 stage/commit/deploy。本文不能替代后续固定三路完整热更新内审与独立外审门。
