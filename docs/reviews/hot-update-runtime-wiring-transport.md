# H1 接线裁决：同进程资源 manager 与同 PID exec（连接 lane）

2026-09-20；只读生产，未改源码/启服务。结论为架构建议，不是实现或预算通过。

## 1. 裁决

**可选同进程独立 workload/flow manager + agent/broker 各自整体同 PID 可恢复 exec，作为首选最小路线。连接层没有要求先引入常驻 controller 子进程/IPC 的硬约束。**
前提是“独立”落实到所有权、入口围栏、可序列化状态和生命周期；不是把现有对象换个名字后直接 Exec。
多进程能隔离普通 controller 失败、让部分控制更新无需暂停泵，但最终 flow owner 自身升级仍需原 FD/用户态状态接管，不能消除 H2/SS 改造。
同进程无需本机 IPC，但保留 typed/versioned manager API、唯一 owner/generation、请求去重和完整资源清单；跨节点身份/恢复票据仍必须认证。
新旧兼容、ctl可靠请求/输出、原业务socket、内部carrier重建、≤500ms freeze、≤5s每请求、旧映像有限退场全部保留，不以“一个进程”降低门槛。

## 2. 现有代码需改变的真实边界

| 代码 | 当前问题 | 同进程必须改造 |
| --- | --- | --- |
| `internal/tunnel/tunnel.go:947–954,962–971` | yamux关闭直接关闭public listener并注销session | Endpoint生命周期独立于Carrier，计划暂停不调用业务Close |
| `tunnel.go:974–993` | Accept返回的pubConn先藏在goroutine参数，再Open yamux | Accept→登记Endpoint/Flow必须受同一入口围栏保护；Open成为可恢复状态 |
| `tunnel.go:2046–2053` | 5s DialTimeout结果只在handler栈 | 登记Dial状态和已创建FD，不能等待5s完成才freeze，也不能丢栈后重dial |
| `tunnel.go:2061–2074` | io.Copy持有局部缓冲，任何方向结束关闭两端 | 显式分方向pump、partial read/write记账、FIN状态和可暂停循环 |
| `internal/agent/tunnel_adapter.go:27–50,146–149` | op锁/ApplyHome等待不提供快照屏障 | 纳入统一generation/intake gate，冻结不能无限等旧op锁 |
| `internal/agent/ssproxy/crypto.go:96–120,147–169` | partial密文与写进度在局部栈，nonce先递增 | AEAD parser/writer全部状态化，任意分片位置可暂停 |
| `ssproxy/server.go:657–669,686–708,740–758` | 握手/target dial/response salt局部状态 | 从accept起登记，握手/连接中/盐部分写入同属资源 |
| `internal/agent/agent.go:1082–1096` | forwarded callback先读msg再派发后台handler | 在副作用前将已接纳reqID/payload/阶段归manager，callback返回不表示工作已交清 |

不能迁移任意Go goroutine/栈、mutex等待者、TLS/yamux对象或NATS回调；它们必须收敛为“已完成”或“由manager显式接管的续行状态”。
不可取消/不可归集handler在预检期导致defer；不能先关闭业务socket去迫使WaitGroup清零。
永久依赖旧handler不退场的方案不满足完成契约；多进程亦不能靠永久旧worker豁免。

## 3. Flow adapter 最小接口

下列是职责接口草案，不预占正式函数名或Test名称：

```go
type FlowAdapter interface {
    Preflight(ctx context.Context, target ABI) (Estimate, error)
    Freeze(ctx context.Context, cut CutToken) (Snapshot, error)
    Thaw(cut CutToken) error // 仅exec尚未成功且旧对象完整
}
type RestoredFlow interface {
    Validate(authority Decision) error // 无业务读写、无重新dial
    Activate(ctx context.Context) error
    Close(reason TerminalReason) error
}
// 每协议注册 Restore(snapshot, ownedFiles) -> Dormant RestoredFlow。
// Restore/Validate失败保留FD供受支持rollback，不能自动执行Close。
```

Manager提供唯一`Admit/Lookup/Cancel/Revoke`、全量资源枚举、冻结屏障和owner代际；adapter不能旁路创建不登记的socket/goroutine。
Preflight在业务仍运行时完成目标ABI/容量/权限/代码/依赖检查和可提前完成的大块拷贝；Freeze只取一致切面，不进行远端拨号、DNS或文件下载。
Freeze返回必须代表所有有副作用的读写已计账、原FD仍持有且无第二writer；失败返回后Thaw恢复原截止时间与调度。
Import先构造Dormant对象，全部身份/状态/文件校验后才Activate；旧callback/迟到carrier不得再次激活同FlowID。

### 必需快照

| 层 | 明确字段 |
| --- | --- |
| Endpoint | 稳定ID、sid/nid/allocation、原FD类型与identity、public/local方向、listener或accepted socket、目标addr、ownerGen、接纳/连接阶段 |
| LogicalFlow每方向 | tx已读/已发送/已ACK offset，rx连续接纳/向原socket已写offset，未ACK数据/未写tail，FIN最终offset/已传播位、RESET/终态 |
| Authorization | token/allocation/home epoch/pins、当前已证明peer约束、取消/撤权tombstone、fail-closed/授权期限；绝不把generation当凭据 |
| Carrier意图 | 对端identity/可用地址、协商版本、已绑定FlowID、resume ticket与generation；不保存TLS/yamux对象或它们的TCP连接作为可续旧会话 |
| SS | keyID/keygeneration、子密钥或足够派生输入、双向nonce、盐/length/payload阶段及已收字节、解密未消费明文、已封装待写密文+wire offset、replay salt集合 |
| 调度/限制 | 原读/写/握手/idle/操作deadline、重放容量、背压状态、统计高水位、是否因人工慢消费者暂停 |

256MiB总重放不能直接塞进handoff的64MiB JSON上限；用独立sealed payload/chunk FD+小型manifest，或先明确受支持迁移容量。
可提前封存的不可变chunk不在freeze中重拷贝；在写尾块/元数据切面有明确上界。仅把JSON上限调大不能证明500ms。

## 4. 安全点与 Go netpoll 恢复

1. **接纳屏障**：先拒绝/排队新manager操作并归集已接纳请求；Accept与登记同一cut generation。Accept恰在屏障返回的新FD必须进入快照或留内核accept队列，不得藏在旧栈。
2. **唤醒不关闭**：对原TCPConn/TCPListener用临时deadline唤醒阻塞Read/Write/Accept，pump识别freeze原因。任何`n>0,err!=nil`先提交n及buffer位置，再报告暂停；不把暂停timeout当业务错误。
   `net.Conn.Write`可在内部多次poll；不允许在syscall成功到offset记账之间宣布safe。避免io.Copy的隐式ReaderFrom/WriterTo/splice中间缓冲，先使用完全受manager记录的Read/Write循环。
3. **Dial窗口**：连接尚未创建前可安全重启无副作用准备；一旦raw socket/connect已创建，需登记可恢复非阻塞connect状态/FD并正确处理SO_ERROR。不能假设取消DialContext等于远端从未accept，不能对已建立目标重新dial。
4. **非阻塞标志**：通过SyscallConn.Control/F_DUPFD_CLOEXEC取副本；原FD须CLOEXEC。不能File().Fd()清共享O_NONBLOCK。基础层已加非CLOEXEC输入拒绝，整体清单仍由runtime闭合。
5. **新poller**：`os.NewFile`/`net.FileConn`/`net.FileListener`建立新Go netpoll对象；成功交付后关闭中间文件副本，确认只剩预期owner，避免隐藏引用延迟EOF。
6. **deadline**：原Go ReadDeadline/WriteDeadline/ListenerDeadline存在用户态，FD不会携带它们。快照每类原截止值/剩余预算，并选择跨exec不重置的单调时钟表示（同boot的clock基准）；过期就按原语义处理，不能统一清零或“再给5s”。
7. **临时pause deadline**不进入业务快照；Thaw/Restore恢复原期限，原零值确实代表无期限。握手、SS auth、fail-closed与idle计时器同样不重置。
8. **缓冲/FIN**：kernel未读bytes和TCP半关闭能随原socket存活；bufio、CopyBuffer、AEAD和frame parser已读出的字节只能靠显式状态。先恢复缓存再继续socket读，先续原待写密文再生成下一帧。
9. **秘密/权限**：匿名FD不等于同UID恶意进程隔离；沿用明确威胁模型。SS不能重用nonce封装不同内容，revoke先更新manager tombstone并关闭受影响原流，不被freeze漏记。

内部TLS/yamux/NATS carrier可以关闭重建，前提是关闭路径只影响carrier、逻辑层原socket保持且可靠seq/ACK恢复；旧代码的defer Close不能保留。
新模式ALPN、原REGISTER身份验证、既有allocation attach/resume与N-1拒绝路径仍按正式plan执行；legacy活流不能被追溯变成可迁移。

## 5. 同 PID exec 的阶段与失败语义

`预检/准备 → 停接纳/归集 → 全部adapter freeze → 封存manifest/决议 → 只继承白名单exec → 最早Restore → 校验/构造Dormant managers → Activate → carrier恢复/可靠ctl续行 → complete`。
此处“最早”包括依赖init不能先spawn应用子进程；所有原业务fd先恢复CLOEXEC，才允许新请求启动子进程。
不能走当前普通Run取消→Tunnel/SS Close→再exec路径；计划暂停、正常stop、权限撤销三种操作职责必须分离。
exec返回error：恢复CLOEXEC、Thaw旧对象，旧PID/业务仍在；不能fallback成spawn+exit而破坏父子Wait关系。
exec成功之后：旧Go映像已不存在，不能声称“unfreeze旧worker”。新旧需共享最小restore/rollback bootstrap，保持所有原FD，遇返回型初始化失败可同PID回到受支持prev artifact/快照。
当前handoff.Restore会消费dispatch并关闭manifest；不能原样接main就声称有自动rollback。runtime须在可信校验后保留可回退envelope/状态与FD所有权，或在任何副作用前从Restored重新构造受支持旧schema快照。
该bootstrap在验证完成前不能恢复旧DB快照、开放第二writer或发新业务副作用；若已接纳新工作，rollback也必须交接这些工作。
即使控制入口尚未ready，只要raw泵/Wait已推进，最初manifest就已过时；rollback必须重新freeze/export当前offset/缓冲/退出记录。资源owner激活与controlReady分开命名，不能用`!controlReady`误判可重用旧包。
任意panic/kill9造成进程死亡/原fd关闭不是该架构可普遍无损恢复的事件，沿用计划已明确的崩溃窗口，不伪造“自动回旧owner”。
多进程可减轻controller崩溃影响，但最终owner升级仍有此边界，因此不是为了满足计划内换代必须先引入的依赖。

## 6. 预算和新请求服务不能靠进程划分推导

500ms本地freeze必须测到新映像恢复原pump能力，不能只统计旧进程进入syscall.Exec前耗时；Go启动/init、import、大状态验证均纳入停顿观察。
已验证原raw I/O应尽早恢复，不等待NATS/SQLite/Raft全部ready；但必须先恢复授权tombstone、原fail-closed绝对期限和唯一owner，不能为快恢复跳过鉴权/撤权。
目标预检子进程的热堆不会搬进同PID新映像；不能把smoke ready当真正新runtime已预热。昂贵初始化须前移/缓存或从关键路径分离，并用实际artifact实测。
ctl/broker保存同reqID结果并可靠重试；NATS重连不可把成功请求变成重复执行，5s是整个原request的budget，不按尝试重置。
单broker升级期间短暂无handler可由已认证可靠客户端/产品接纳层等待≤5s，不能返回终态失败后让drill换reqID重发。入口容量不足须升级前defer。
NATS/Caddy的HA/N=1产品路径照常需要；把agent/broker变同进程manager不会替代外部服务连续性。
若支持profile在500ms/5s内实测不能达到，应优化关键路径/降低明确容量上限并先改plan复审，不能临时放宽断言。

## 7. 必须保留的验收与裁决状态

本lane前次真实TCP实验已证明samePID exec能保原socket inode、kernel排队数据、半关闭；它未证明TLS/yamux/SS/应用deadline自动恢复。
新增验收必须卡住：Accept后登记前、Dial已connect但handler未接管、Read n>0+err、Write部分成功、SS半个长度/密文帧/盐、FIN已收未传播、撤权与freeze竞争。
每臂同时验原SSH/TCP/SS双方seq/nonce、未重dial、每新请求延迟/唯一副作用、实际新映像和旧映像退出；500ms/5s预算与≥5轮资源不退让。
架构若采纳，应doc-first将plan中的“常驻额外runtime/认证本机IPC”改为“进程内manager typed API + 同PIDexec；可选后续进程隔离”，保持全部跨节点认证和owner/fence要求。
不能因此提前勾选H1/H2；正式实现及真实simcluster门仍未完成。

## 8. 固定交叉意见（已收到）

- agent/transfer lane同意无必须多进程的硬约束；stdin隐式copier、stdout/stderr pump、PTY Wait与后台cmd.Wait必须先归并为明确FD/offset/state owner；sole waiter的observe/reap/journal与freeze协调，不能仅等待全部handler自然结束。
- 本lane回质：nonblocking本身不构成safe point，partial n必须记账后再冻结，隐式Copy/ReadFrom/splice不得留下未登记中间字节；不能用Close pipe诱使pump退出。
- system lane同意返回型启动失败可持原FD同PID回prev；指出当前Restore消费manifest，runtime还需rollback envelope。500ms包括exec/init，raw恢复不可被NATS/持久层完整readiness拖住；已采纳。
- 已亲读system稿110行并认可其main/BootUpgradeCheck/普通os.Exit/空表register风险；补充尚未controlReady但泵已推进的回滚必须取当前态，双方一致。
- 已亲读agent完整稿，认可child-exit/EOF/outputACK/eventACK分离及spawn handshake归表；远端FS不可中断syscall须预检defer，执行目标用memfd也不代表进程所有线程能在500ms内完成exec。
- 两lane均认可整体同PIDexec方向；具体plan变更、API/预算和正式实现由root doc-first统一裁决。本稿不代表H1/H2已通过。
