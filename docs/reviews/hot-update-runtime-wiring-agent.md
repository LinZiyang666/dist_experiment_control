# H1 agent runtime 接线裁决

2026-09-20；只读建议，未改生产、未运行新测试；不是能力 Pass。范围为现有 plan 全契约。

## 1. 裁决

**同意先在同一 agent 进程引入独立 `workload.Manager`，整体同 PID 可恢复 exec；不必先拆 controller 子进程/IPC。**
同 PID 保留任务父关系；显式 FD、协议状态和 owner 状态导入后，目标映像可接管无限期任务并退出全部旧映像。
进程拆分有故障隔离价值，但不会自动解决原 socket、Wait、半包、权限或 owner 迁移；当前契约没有强制多进程的理由。
这里的“独立”必须是职责/状态所有权独立：控制 handler 仅调用 Manager，不能再私藏业务 FD、进度或终态。
该裁决没有删除文件传输、PTY、SSH/proxy、remote-fs、新请求或 runtime 自身升级；未接入的资源类必须阻止激活，后续仍须补齐。
多个 controller/旧 worker 暂时存活不能成为 completed 条件；本方案成功 exec 后本 agent 的旧映像已退出。

## 2. 必须先接的边界

1. 新 `internal/workload` 拥有 task 表、raw FD、pump、wait/reap、信号、输入/输出日志、退出 outbox、freeze 屏障和版本化 export/import。
2. controller 拥有 NATS/session/auth/路由；只交付经过认证且具有稳定 ID 的请求，并作为 Manager 的可替换 transport adapter。
3. Manager 根生命周期独立于 `loadRunCtx()`；重建 carrier 只解绑 adapter，不触发 task cancel、Hangup、关闭 master/pipe 或终态失败。
4. `dispatchForwarded` (`internal/agent/exec.go:35`) 的所有命令、PTY `.in/.resize/.ka` (`run.go:858`)、cancel/kill/reaper 都进入统一 admission/owner 屏障。
5. 请求先登记 request ID、payload hash、权限身份、执行状态，再产生副作用；freeze 中到达的新请求由有界可靠 ctl/broker 队列接纳，不可用 NATS Publish 成功冒充接纳。
6. `cmd/tether/main.go:68` 加最早的 handoff bootstrap；恢复资源表/本地 pumps 先于 `agent.New`、会话注册和 G.1。禁止以空表 register。
7. `upgrade.go:301` 的 sleep+裸 Exec 换为 Manager prepare/freeze/export/exec；失败原地 unfreeze，成功新代 import；普通 stop/evict 的清理语义保留。
8. 当前 `internal/handoff` 只运送已静止 FD/JSON（`handoff.go:1`），不提供上面任何屏障、业务可靠性或回滚接线。

## 3. exec/run/PTY 最小改线

| 现有接线 | H1 必须改成 |
| --- | --- |
| `exec.go:159` 的 `runChild` 栈持有整个任务 | controller 提交 Start，Manager 返回稳定 task/stream ID；handler 结束不结束任务 |
| `exec.go:165` bytes.Reader Stdin，`181` StdoutPipe/StderrPipe | 显式 stdin writer、stdout/stderr reader；初始 stdin 全量/已写 offset/EOF 自持，避免 os/exec 隐式 copier |
| `exec.go:227` execChildren 只有 *os.Process | 与 PTY 统一完整任务表：逻辑 PID、OS 身份、生命周期、owner、输入输出与 wait 状态 |
| `exec.go:230` 两个 stream goroutine+wg.Wait+cmd.Wait | 可停在明确 read/write 边界的 pumps 和独立 wait actor；导出已读但未交付的字节 |
| `run.go:259` procRec、`300` PTY pump、`307` sess.Wait | master 和任务状态由 Manager 持有；PTY Session 不再以旧 *exec.Cmd 作为恢复依据 |
| `run.go:322` 等 pump 最多 200ms 然后 Close | child exit 与流 EOF/尾部 ACK 分开；保留退出后仍未交付尾部，达到队列限额应背压而非截断 |
| `run.go:469` kill 只查 PTY 表 | kill/ps 对 exec 与 PTY 都查统一 owner；信号、wait、回收使用同一身份校验与序列 |
| `agent.go:1790` PTY 表+分离的 pendingExitSnapshot | 单次联合快照含普通 exec、PTY、starting、exit-pending；无“暂时缺席”被 G.1 当 -1 |
| `proc_delivery.go:119` unregister→enqueueExit 空窗 | 同一 owner 事务从 running 转 exit-pending；broker 对 courier/register 两路使用同一事件 ID 去重 |

明确区分 OS child 已退出、输出已 EOF、输出已 ACK、退出事件已 ACK；这四个事实不能合成一个 `done` 位。
子进程自己 fork 后留下继承 stdout 的后代时，原 child exit 不代表 pipe EOF；保留流 owner，不能为 upgrade 强制关管道。
`pty.go:93/199/304` 的正常和 abandoned-start Wait、`spawnexec/helper.go:106` 的错误回收也必须纳入；不能只替换成功路径。

## 4. Wait 如何冻结

- 当前 `cmd.Wait()`/`sess.Wait()` 不支持 context 停止，并会回收内核退出状态；外层取消 goroutine 不会暂停它。
- **最小可交付选项**：Manager 是唯一 wait owner，定时或 SIGCHLD 唤醒后对已登记 child 做 `wait4(WNOHANG)`；不得用 P_ALL 偷收其他 helper。
- reap→保存原始 wait status、rc/signal、rusage、endedAt→产生稳定 exit-pending，是 freeze 屏障内不可分割的有界事务；freeze 只在事务外成功。
- **可选更清楚的两阶段选项**：`waitid(P_PID/P_PIDFD, WEXITED|WNOHANG|WNOWAIT)` 先观察，保存 exit-observed，再显式 reap；导入时可接着处理未 reap zombie。
- WNOWAIT 自身不会冻结任何旧 Wait；两条 wait 路不能并存。Linux wrapper 要处理 EINTR/ECHILD、无事件、信号退出及支持架构的 siginfo 表示。
- 观察退出后也可在 sole-owner 约束下用 Process.Wait 取精确状态，但不得用包含隐式 copier/closer 的 Cmd.Wait；优先保持真正非阻塞 reap 接口。
- 受控 freeze 的 WNOHANG 原子事务已足够，不为 H1 额外宣称任意 kill-9 的 reap→journal 崩溃窗口 exactly-once。
- 保留 PID/boot/startticks/pgid 和可用时的 pidfd；全部 wait/signal/KA-reaper 串行，退出后禁止向可能已复用的 pgid 发信号。
- `spawnexec` 尚在 handshake 的任务要显式导出 spec/status FD、部分解码/发送进度、helper 身份、watchdog/slot 状态；初期可在预备阶段等其 settle，不能以杀任务达到 freeze。
- 若有永不返回的远端 FS 操作或不可导出的 in-flight syscall，报告 deferred 并继续原业务；多进程也不能保证这种旧 worker 立即升级退出。
- 全进程 exec 还受其他线程退出进度影响；仅用 memfd/本地 artifact 不能证明 500ms。预检与资源 registry 必须覆盖控制侧文件探测等非业务线程，避免带着不可中止调用进入切换。

## 5. 必须迁移的精确状态

| 域 | 内容 |
| --- | --- |
| 身份与权限 | sid、实际 routing nid/lease、instance lineage、目标 generation、授权 actor、request/client lineage、payload hash、task/flow/transfer ID；generation 不进入用户请求去重 scope |
| 子进程 | 逻辑 PID、OS PID/pgid、boot ID/startticks、argv/cwd/必要环境元数据、startedAt、spawn phase、helper/父关系、观察/回收状态、wait status/rc/signal/rusage |
| 父端 FD | stdin 写端、stdout/stderr 读端、PTY master、待启动时的 slave/handshake FD、pidfd；名称/用途和唯一 ownership；子进程自己的 FD 原样存于内核 |
| 输入 | stream ID、recv/accepted/applied offset、未写字节和当前 partial-write offset、输入 EOF/half-close、resize 序号与最后尺寸、待执行信号及其 request ID |
| 输出 | stdout/stderr/PTY 各自 stream ID、read/send/ACK offset、已读未 ACK 字节、正在发送 frame 和偏移、EOF/terminal 标志；不承诺 stdout/stderr 跨流原本不存在的总序 |
| 生命周期 | started/exit 的稳定 event ID、原 payload/时间、ACK 与重试状态、已回收但未通知、输出尾部未完、最终 ctl reply；同一 task 不从表中消失后再加入 outbox |
| 活性和控制 | KA grace/最后有效 receipt/两阶段 probe/reaped 状态、cancel/revoke 的单调决议、原绝对 deadline及剩余预算、未完成命令/结果去重表与 tombstone |
| carrier adapter | 逻辑订阅/回复目标与恢复票据、双方 ACK 水位和 owner fence；旧 *nats.Conn、goroutine、channel、锁不序列化，导入后重建 |
| 其他资源 | 文件 source/目标临时 FD、hash/offset/checkpoint、commit决议、对象/consumer/GC责任；raw socket/half-close/协议半包与密钥相位由 flow/transfer manager 导出 |

时间不能直接依赖序列化后失效的 Go monotonic 部分；使用同机可比较时间基准/保存 anchor，deadline 不因升级重置；KA 按已定义 carrier 不健康策略恢复。
`proc_delivery.go:60/138/225` 现有溢出/24h 丢事件不能作新契约的可靠 outbox；要有有界 admission/backpressure、持久水位和可解释回收条件。
`handoff` JSON 上限 64MiB，plan 每节点 replay 256MiB：重放字节走受额度管理的 memfd/日志 FD 与范围索引，不能硬塞 JSON 或冻结中复制全部 backlog。

## 6. freeze 与恢复顺序

1. 活态阶段完成 artifact/ABI/权限/容量预检、只读候选检查、日志分段准备；所有副作用仍归旧 owner。预检不能夺数据锁或提前迁库。
2. 发布切换 fence；关闭新副作用 admission；已接纳请求保留队列与去重状态；等待当前有界状态变更完成，停止新 read/write/reap。
3. 各 pump 到安全点：没有读出但未记账的 byte、没有 write 完但 offset 未记录；blocking I/O 用非阻塞 FD+poll/唤醒点，不以 Close 原业务 FD 解阻塞。
   Read/Write 返回 `n>0,err!=nil` 必须先记录 n、buffer/offset、EOF/FIN；临时 pause deadline 不替换原业务 deadline。禁止隐式 io.Copy/ReaderFrom/WriterTo/splice 遗留未登记中间字节。
4. 冻结最后 delta、显式白名单 FD/owner 清单，Prepare+Exec；失败恢复 CLOEXEC、释放副本、恢复原 pumps/admission。
5. 新映像 bootstrap 核验完整包、建立唯一 Manager，先恢复撤销 tombstone、原授权/fail-closed 绝对期限，再恢复本地 pumps；carrier、会话认证、订阅随后恢复，再开放新命令执行并发送 backlog。
6. **500ms 从旧业务 pumps 停止到新 pumps 实际运行，包含 Prepare 尾部、exec、Go init、import；不是只计 Freeze() 调用。**
7. 新请求端到端 ≤5s 另计整个排队/切换/重试；旧业务活流无进展 ≤5s。默认 NATS 重连退避/5s RPC timeout 不能直接相加后冒称达标。
8. raw pumps 不等网络 register/远端存储成功才启动；暂时无 carrier 用 bounded replay/backpressure。内核/应用原有短 deadline 不能悄悄延长。
9. 不能及时到安全点时在旧映像尚可运行阶段撤销激活并解冻；成功 exec 后无法“取消 exec”，靠兼容 bootstrap 受控回退。

## 7. 回滚与单进程的实际边界

`main.go:78` BootUpgradeCheck、`upgrade_state.go:382` realExec、普通 main os.Exit 必须进入同一 handoff-aware boot coordinator。
当前 Restore 消耗 env 并在验证后关闭 manifest（`image_linux.go:264/320`）；不能在 domain import 失败后裸 exec prev，或者先 Close 全部继承资源。
bootstrap 必须保留可再导出的完整 bundle/未消费 FD、域状态和兼容版本，预先验证再 TakeFile；回退同 PID 到兼容 prev，成功导入前不注册空 owner。
`resource-active` 与 `control-ready` 分开：只要 raw pumps/reap 已推进，初始 snapshot 已过时；即使控制尚未 ready，回滚也必须重新 freeze/export 当前字节、任务和退出状态。
如果新映像在 bootstrap 前 fatal 或唯一 FD holder 被 kill-9，单进程不能承诺业务无损；这是计划已区分的非受控崩溃边界，不能把普通 supervisor restart 写成连续回滚。
额外 guardian 若只复制 FD 也不能让新进程恢复原父关系；若未来要求任意控制进程崩溃隔离，可再拆进程，但仍需升级 guardian/runtime 的完整迁移协议。
首轮 legacy 二进制无这些状态：只能在证明全资源静止且所有入口被冻结后迁入；普通活跃 legacy reexec 仍不能视作首轮热更新成功。

## 8. H1 可交付顺序与硬闸

A. 先统一 exec/PTY 表、显式 stdio、single wait-owner、联合 snapshot/outbox；不接 upgrade 时验证原功能不回归。
B. 用真实子进程和 PTY 穿过同 PID exec：原 PID/boot/startticks/nonce，双流连续、剩余 stdin、resize/信号、精确 rc、ps/kill、child 在各 exit/reap 窗口退出。
C. 接 ctl/broker request ID+ACK/replay/恢复票据：真实 carrier 掉线时原调用不失败，迟到请求/ACK不重执行；没有此项不开放活跃任务热升级。
D. 同一 candidate 接 upgrade/boot rollback；导入失败、Exec 返回失败、回退失败、满 replay/FD、blocked pump、正在启动/退出、反复升级与旧 SHA 退场全部独立 oracle。
E. 将 flow/transfer/SS/remote-fs 的资源清单接到同一顶层 fence；各能力真实 simcluster 与 test/docs 完备后，才能声明全契约完成。
现有真实 exec/rawFD 通过仅证明基础运送机制；H0 进程基线红和普通 continuity 正向 oracle 应沿用，不能以 os.ProcessAlive 替换输出/管理断言。

## 9. 三路交叉

已亲读 transport 121 行稿与 system 114 行稿：raw endpoint/flow 显式 owner 足够，不需提前拆进程，旧 handler 栈不可迁移。
采纳 system 的约束：500ms 覆盖 exec/import、业务 pumps 早于 register、bundle-aware 受控回退、禁止空 G.1、所有请求 5s 含排队和重连。
采纳 tunnel 的 `n>0+err` 必先记账与禁隐式 copy 快路径，采纳两路的 resource-active 后回滚须导出当前态、权限期限先于泵恢复。
两路均已亲读本稿并反馈认可、无新增阻断；system 认可 WNOHANG 的计划内原子事务，tunnel 补充全进程 exec 的非业务线程/远端 FS 安全点也须检查。
本建议不声称已通过 H1 集成或真实升级验证。
