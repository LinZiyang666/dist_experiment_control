# 全栈不中断热更新：系统契约与验收可行性草案

日期：2026-09-20。角色：固定并行规划中的系统／身份／持久化审查 lane。

**性质：只读可行性和计划建议，非实现通过报告。** 本文只阅读本地文档、实现和测试，没有运行新的测试、simcluster drill 或现网操作，没有修改生产。所有下述新增能力、gate、drill 均为**待设计／待实现／待验证**。正式 plan 由主进程定稿到 `docs/reviews/hot-update-plan.md`，本建议不改变正在收尾的 control-connection-recovery 修复范围。

## 1. 结论与范围

用户最新范围已经确定：**agent、broker、NATS、Caddy 全部纳入；原业务进程、文件传输、SSH／隧道不中止，升级期间新控制请求持续可用；每项能力必须有测试、真实 simcluster drill 和文档收据。** 不再把 agent-only 或“升级后能重新连上”当作交付范围。

可以把这一目标建设为**对明确支持的版本组合和部署形态成立的产品能力**，但当前实现尚不具备；这是进程与资源归属架构增量，不能由修改 `syscall.Exec` 参数完成。最终方案应支持原始业务 socket／PTY／pipe 的安全点 FD＋自有状态移交，以及内部 TLS／yamux carrier 重建后的有序逻辑流续接，让长寿命任务也能脱离旧 runtime；排空只能作阶段交付或不兼容时的安全状态，不能作为用户最彻底目标的全部答案。必须区分三个结论：

1. **业务连续性可优先保证**：保留已有资源的执行进程、为新请求提供新的有效控制 owner，通过有围栏的交接和可恢复的内部通信隐藏升级动作。
2. **通用的有限时全代码替换不能无条件保证**：任意长寿命的 TLS／yamux／第三方运行时可能携带无法导出的会话状态。只传监听 FD 或已连接 socket FD 不会自动传递 TLS 密钥与序号、yamux 流状态、协议缓冲、Go 对象和任务状态。保留旧执行者可以保业务，但旧代码仍在运行。
3. **不满足连续性或兼容前提就 defer／拒绝激活**：不得为了按时退出全部旧代码杀掉长连接、重启业务或丢弃任务。跨纪元、不能回退的 DB 迁移、第三方版本不支持的状态交接都必须清楚呈现原因。最早安装具备热更新能力的“桥接版本”也不能追溯保证更老二进制所持有的资源无缝交接。

必须把“不中断”解释为业务可观测契约，允许内部 NATS 连接更换或内部重试，但前提是原业务 socket／进程／操作继续，调用者不用重新执行命令或重开连接，字节和副作用不丢、不重复，且延迟仍在承诺预算内。**内部 reconnect 成功既不是必要的零断开指标，也不是充分的业务连续性证明。**

## 2. 现有契约和实现：可以复用什么，不能扩大解释什么

| 来源（行号以当前工作树为准） | 已有事实 | 对新目标的约束 |
|---|---|---|
| `docs/requirements.md:308`、`:310`、`:325`、`:327` | 控制组件崩溃后托管进程继续、网络断连后重连；不假设 systemd，支持手工／nohup 等启动方式 | 已有韧性要求未定义全栈计划升级时原 PTY、流和新请求的连续性；新 supervisor 不能只在 systemd 模式成立 |
| `docs/requirements.md:335`、`:338`、`:342`、`:348` | 相邻 release 在线兼容，broker 先升／后回；跨 ProtoVersion 需重装；新 broker 加入集群的 release 要精确匹配 | 不能借热更新绕过纪元门或 join 门；临时增加一个新版本 broker 不能假称现有 join 已支持 |
| `docs/distributed-broker-architecture.md:875`、`:884`、`:904`、`:915` | 下载／校验／smoke／prev／marker／rename／exec；commit 只需目标实例运行新 SHA 并 register；有序 fsync | 原子安装、身份化提交、互斥和回退可复用；register 不足以证明控制所有权接管、业务继续或旧代码退出 |
| `internal/agent/upgrade.go:236`、`:291`、`:314` | 直接替换同 PID 的映像，连接关闭后重新注册；进程内升级测试跳过真实 exec | 同 PID 不等于同运行时、同资源所有者。不得沿用当前 reexec 宣称零中断 |
| `internal/agent/run.go:283`、`:300`、`:307`；`internal/pty/pty.go:43`、`:126`、`:194` | PTY master、会话映射、泵、Wait 由 agent 运行时持有；NATS 重连时已支持当前连接上的收发 | setsid 只解决一部分进程生命周期问题；PTY master、输入、窗口调整、信号、退出状态都需要跨控制版本继续有人持有 |
| `internal/agent/exec.go:181`、`:230` | stdout／stderr pipe reader 与 Wait 在 agent 内 | 业务 PID 未变仍可能失去输出、受 pipe 关闭影响；验收必须同时测原 PID、持续输出和最终退出状态 |
| `docs/requirements.md:455`、`:460`、`:465`、`:470`；`internal/agent/instance.go:73`、`:102`、`:181` | nid 是单持有者槽位；instanceID 是路由正确性 token，非认证；exec 传递 lineage；租约族远程升级当前受限 | 两个新旧 controller 同时用相同 instanceID 并不能产生安全 handoff，会重新引入双订阅／双执行；不得顺带放开租约族升级限制 |
| `docs/distributed-broker-architecture.md:347`、`:351`；`internal/broker/transfer_home.go:48` | 在途 transfer 始终由原 tracker 所在 broker 终结；home 死亡时在途重启是现有边界 | 新增“计划升级不丢在途”契约必须明确区别于崩溃恢复；rehome 本身不能实现任务转移 |
| `internal/broker/transfer.go:248`、`:264`、`:273`、`:1336`；`internal/broker/xfer_inflight_test.go:30`、`:69` | tracker／watchdog／committed 在内存；durable ledger 可在重启后补 `failed/home_broker_restart` 审计 | 该 ledger 不是可恢复的活任务 checkpoint，不能据此声称迁移成功；升级重放不能合成失败后再补完成 |
| `internal/broker/reexec.go:21`、`:47`；`cmd/tether/serve.go:326`、`:399` | broker 当前先有序关闭再同 PID exec；有 leader 拒绝、SHA 与操作锁门 | 新计划必须保留这些门的目的，同时把任务／连接 owner 与控制进程关闭解耦 |
| `docs/distributed-broker-architecture.md:13`、`:31`、`:34`、`:37`、`:111` | Raft＋SQLite；外置 NATS＋JS；Caddy WSS；部分实时字节 best-effort；SQLite 保单写约束 | 全栈目标还要补消息与本地交接层可靠性；不能同时启动两个持同一 Raft nodeID、store 或 SQLite 状态机的活 owner |

历史 `docs/reviews/upgrade-safety-plan.md:29`、`:32`、`:35` 明确未做 DB 迁移、行为级健康和 broker 升级变更；`:184` 明确当时进程内测试的 exec 边界。历史 plan 的启动检查位置已经由当前架构 §21 更新，不能用历史文字覆盖当前 HOW。

## 3. 可验证的“不打扰”和升级完成状态

### 3.1 每次激活必须同时保持的业务标准

- **业务进程**：原 OS PID＋boot ID＋start ticks 保持；未因升级收到终止／HUP；持续有输出且输入、resize、信号控制仍可用；退出只由业务或用户决定，退出码完整上报。禁止只比逻辑 proc ID 或升级前后各一次 `ps`。
- **文件传输**：同一用户调用与 transfer ID 继续，源和目标数据摘要／长度完全一致；不要求用户重传；目标 rename 仍原子，不能暴露半文件；同一任务最多一个有效终态，不出现先 failed 后 complete、双写目标、重复执行副作用。
- **SSH／TCP 隧道**：升级前已经建立的业务 socket 持续双向收发连续序号字节，客户端不重连、不重开 session，无升级导致的 EOF／RST；half-close、背压、空闲长连接、多个并发流均覆盖。只测试升级后新建 SSH 成功不合格。
- **新控制请求**：整个升级窗口持续发起有唯一请求 ID 的读、写和业务命令；无升级导致的终态失败／无响应、无重复副作用；每个请求在冻结的延迟上限内完成。不能停发探针、删除切换窗口样本，或让测试外层无限重试掩盖失败。
- **安全与身份**：sid／nid、home／port binding、证书／ACL 不被偷换；一个控制槽位同一时刻最多一个有效命令 owner。旧数据 worker 只允许继续既有授权操作，不能因控制版本失效仍接受新管理命令；用户 kick／撤权／取消仍必须能到达它。

“持续可用”必须在正式计划冻结一个**绝对单请求上限和持续业务最大停顿**，并记录 p50／p95／p99／max。现有 `requirements.md:313` 明言性能预期不是 SLA，不能偷用 `<100ms` 当已承诺硬门。建议默认不修改任何业务 deadline，不给升级增加无限容忍；数值由根计划按目标环境实测并定稿。严格零耗时／零抖动不是可执行验收标准。

### 3.2 状态机和两个完成点（建议，待实现）

`requested → staged → ready → activating → serving_new/draining_old → completed`；旁路为 `deferred`、`rejected`、`rolled_back`、`rollback_blocked`。

- `staged`：只是文件存在且校验通过，不能报升级完成。
- `ready`：候选运行映像 SHA 已验、兼容性证明和必要依赖就绪，尚无命令 authority。
- **控制切换成功**：新控制 owner 已唯一激活、原任务都仍有明确 owner、新请求与持续业务探针成立；可进入 `serving_new/draining_old`。
- **全量升级完成**：agent／broker／NATS／Caddy 及新增 supervisor／worker／shim 所有计划内运行实例均为目标映像；旧实例已无任务、流、listener、租约和写权限并退出；无旧业务 owner 隐藏残留；必要 DB／IPC／wire 兼容状态已确认。正在运行旧 worker 时必须列组件、版本、SHA、持有资源数及等待原因，不得标 `completed`。
- 若旧流无限持续且没有受支持的状态迁移方法，则允许长期 `draining_old`／`deferred`；继续提供服务。有限时间强制退出旧代码会触及用户的“不打扰”要求，不作为默认降级策略。

## 4. 最小完整产品方案边界（全四层，分阶段实现）

“最小”只限制首个支持集合，例如 Linux、同协议纪元、相邻支持 release、可回退的 schema／IPC；不减少四层或业务连续性的最终范围。原业务进程自己的程序映像不属于tether热更新目标；其PID与运行状态必须保留。新增guardian／runtime仍属tether组件，不能排除其更新责任。

1. **agent**：引入可保留活资源的执行边界（例如独立 workload worker／运行时托管进程），控制版本更换时 PTY、pipe、Wait、传输、TLS／yamux 保留在原执行者，或经专门证明的协议转移。控制 owner 切换采用可校验 epoch／generation、带身份的本机握手、激活 ACK 和旧 owner fencing；候选预热阶段禁止订阅会执行命令的入口。新控制面可控制旧资源，并能取消／回收它们。REGISTER／G.1必须报告所有活runtime的联合snapshot，不能以新controller空表把原进程对账成EXITED(-1)。runtime与controller的unit／cgroup生命周期必须明确分离；只fork到默认systemd control-group里不能抵抗controller unit stop，kick／evict仍须能完整清场。仅凭继承相同 instanceID 或 NATS 重注册不能替代围栏。
2. **broker**：控制权／Raft／SQLite 写 owner、原 transfer tracker owner、隧道 endpoint owner分别列出；升级交接期间每份资源只有一个负责者。可先保留原任务执行者，或定义显式 checkpoint＋所有权交换；必须包含 committed／watchdog／取消／终态去重／对象 GC 责任。不得复制 map 后让新旧实例都 finalize，也不得新进程启动时把仍活着的旧任务当孤儿清理。候选ready健康检查不得打开同一权威写store自动跑migration／夺data-dir锁；预热只读检查与激活后的独占写路径必须分开。
3. **NATS**：真实服务升级需要保消息与请求语义。产品内重试必须使用稳定reqID／结果查询和副作用去重；drill不得用新reqID重新执行一次来掩盖切换失败。多节点可以利用滚动、客户端迁移和副本准备，但这些手段本身不证明实时消息不丢；读／写／publish ACK／reply／consumer／auth_callout 的在途边界逐项验。N=1 若无法保留老服务并引入受支持替代路径，必须 defer，不能停旧后宣称“很快重连所以没影响”。禁止两个 server 同时打开同一个 JetStream store。上游兼容条件须在正式计划按实际版本源码／官方文档核实。保持旧服务的承诺仅适用于尚未退出服务的预检阶段；进入drain/LDM等不可逆步骤后失败应停止后续滚动，并由仍健康的服务路径承担流量，不能假称已关闭的旧连接／server仍在服务。
4. **Caddy 和入口**：分别设计配置 reload 与二进制更新；前者成功不能推导后者支持既有 WSS 保留。新连接需要稳定地址和接受者；旧连接需要其 TLS／WebSocket runtime 继续处理或明确支持的迁移机制。推荐主动把tether内部WSS迁往新入口，由请求／transfer续接协议屏蔽断连；内部连接全部迁出后旧Caddy可真正退出，无需序列化其TLS状态。禁止重建的是用户外部SSH／业务TCP，不是所有内部WSS。保留旧 Caddy 直到已有连接结束可以作过渡，但必须如实显示旧版本仍在；新增入口／supervisor 自身的更新也在全量完成清单内，不能用“永不更新的中间层”暗中绕过用户范围。
5. **拓扑**：N≥3 健康多数派先设计可验证切换；N=1 必须有独立可达方案或显式暂缓，N=2 按当前过渡态契约不能假称具备 quorum HA。用户要求全栈并不等于允许同一时刻关闭全栈；应逐组件、逐节点升级并全程保留请求路径。

### 4.1 原子性和回滚

继续使用 artifact SHA、同目录原子替换、prev、fsync、目标实例证明、宿主升级域锁；增加**资源归属 manifest 与激活事务**。需要持久化哪些字段、由哪个 owner 写、何时 fsync、重启如何判断“旧仍活／新已激活／二者均未知”均须成表，不能让 marker 与实际 authority 分离。

激活前失败：销毁候选、恢复安装指针／marker，原负载与老 control owner 继续服务。激活后失败：先证明旧版本能解析当前 IPC／任务状态／数据库，再通过同一围栏协议切回；候选已经接收的新任务不得凭“回滚二进制”抛弃。**数据回滚不等于恢复旧 SQLite 快照**，否则会丢掉升级期间已经 ACK 的合法写入。

N-1 wire、IPC、worker handoff schema、持久化 marker、Raft op、SQLite migration 和 NATS／Caddy 配置兼容是不同表面，全部进入兼容矩阵。旧worker若跨多代驻留，需扩大并真实测试该IPC兼容窗口，或在下一次升级前先完成迁移；相邻release兼容不能推导任意多代共存。`internal/proto` 字段 inventory 只覆盖其包内导出结构（架构 `:868`），不能替代 IPC／磁盘版本检查。不可逆迁移应延期到旧代码和回滚窗口退出后，或在激活前明确拒绝；不得破坏负载以强行跨纪元。

### 4.2 计划升级与故障的边界

计划升级有预热、旧owner停收新任务、交接、ACK、回滚机会；系统的新请求仍必须由有效owner接纳或在约定预算内排队，不能停全局入口。候选进程启动崩溃属于计划内失败注入，应保原服务。安全点冻结必须等待／打断可恢复的内部I/O并精确记录部分写入；遇阻塞write、D-state等不能形成安全点时延期升级，禁止以关闭业务FD／kill任务强行达期限。普通TCP／pipe write已成功而offset未记录、wait4已reap而退出码未journal的任意kill-9窗口，不因增加owner日志就自动变为无损；必须单独证明或保留故障边界。旧资源 worker 意外崩溃、物理机断电、所有副本失联属于另一故障域：不能保证原 TCP 或已失电业务继续，但须保证已确认持久数据、无双 owner／重复副作用／虚假成功，并按已公开恢复语义收敛。`requirements.md:577` 明列整机宕机“全死”；不能把这项故障伪装成升级连续性的一部分已解决。

容器 kill 或 `kill -9` 只能证明相应崩溃恢复，不能作为物理断电后的持久化证据。fsync 顺序的单测与故障注入需保留；断电保证如要升级，必须有单独存储／VM 断电层的可验证方案及限制说明。

## 5. 可执行验收矩阵（全部新增项待实现）

每行都必须关联正式 Test 名、drill owner 和原始收据。下列场景名是职责描述，**不预占 drill 编号，不表示现有 drill 已覆盖或已运行**。正式立项时由根检查 inventory 后分配。

| 能力／失败边界 | unit／component／integration | race、资源、N-1、真实进程门 | deploy-tier simcluster 必须实际做的事与成功 oracle |
|---|---|---|---|
| 候选预热与唯一控制 owner | 各状态转换、重复／乱序／迟到 ACK、旧 generation 请求、candidate 超时、交接重放；同一 reqID 只执行一次 | 真双进程并发启动同 sid/nid/lineage；N-1 IPC／缺字段；activation 与回滚竞态；拒绝候选提前订阅执行入口 | 实际旧／新 artifact；持续带唯一 ID 的读写／命令探针贯穿切换，记录每次响应与副作用；无双 owner、无租约改名，无无响应窗口 |
| 进程／exec 输出连续 | worker 资源引用、取消、PID 复用、Wait once、stdout／stderr 背压、完整退出码 | 真 subprocess 的原 PID＋bootID＋start ticks；升级控制进程而非 stub reexec；N-1 控制旧 worker；≥5轮资源 exercise | 启动跨越升级的持续计数业务，同时升级各组件；原 PID 不变、输出序列完整、最终 code 准确；同时新命令持续可用 |
| PTY 连续 | stdin／resize／信号路由转移、ctl keepalive 与旧 owner 竞态、终态清理 | 真 PTY，连续输入回显＋SIGWINCH确认；移除某个 worker 保留步骤的负控制必须红 | 原 PTY session 不重开、不 HUP；升级窗口持续交互和 resize；原前台 PID 存活；结束后无 zombie／fd 残留 |
| push／pull 同一操作继续 | transfer owner、commit前后、超时／cancel／finalize竞态、长度／摘要／temp rename、不重复终态 | tier A／B，真实 SDK consumer、slow TLS WSS、大于单 chunk 和原现场量级数据；N-1，多个同时取消；race 与 consumer／fd 清理 | 升级前开始同一传输，agent／home broker／NATS／Caddy 各层分别及顺序升级；同一 transferID 完成，SHA／长度正确，用户没有重试，目标无半文件，审计单终态 |
| SSH／TCP／yamux 旧流 | OPEN replay、stream ID、缓冲／窗口、half-close、空闲／活跃、pins、旧 epoch fencing | 真 TLS＋yamux，多流持续双向 seq/hash；N-1 peers；不能只 mock Conn；旧会话提前 Close 的负控制红 | 真实 SSH 长连接及 TCP echo／bulk；客户端从未重连，原连接持续收发且无 EOF/RST；流不串线；原 endpoint 更新后继续建立新连接 |
| broker tracker／SQLite／Raft owner | checkpoint完整性、唯一 finalize／GC owner；拒绝候选启动重扫活任务；提交幂等；读写并发 | 真 Raft＋SQLite；两个候选不能共写本地 store；迁移注入失败、旧 reader兼容、已ACK写不回退；race | 持续真实 Raft 写＋文件任务，分别升 follower／leader并合法移交领导；检查全部已ACK业务写和审计；注入 candidate失败后原任务不被 orphan reaper 删除 |
| NATS 服务升级 | 消息／请求／消费者在途状态、重复 publish ACK、auth_callout／重订阅顺序 | 真外置 NATS，实际相邻版本；JS数据目录拒双开；N=1／N≥3；只读副本不足时 fail-closed | 新旧真实 nats-server 二进制切换；持续控制请求、PTY、transfer、SSH；内部 reconnect 数可非零但业务 oracle 零中断；进入LDM前readiness不达标保旧服务；进入后失败停止后续滚动并证明仍有健康服务路径 |
| Caddy 二进制与配置更新 | 入口切换规则、连接／listener owner、认证配置失败、目标证书匹配 | 真 Caddy＋TLS/WSS；配置 reload 与 binary 升级两臂；相邻支持版本；活动旧流与新TLS握手同时存在 | 持续原外部SSH／业务TCP，同时主动让内部旧WSS退场并续接原任务；新TLS/WSS握手持续成功；真实更新Caddy运行SHA，旧实例退出才completed；原业务socket的probe禁止重新dial |
| 回滚及不兼容拒绝 | artifact错误、smoke失败、协议／IPC／DB不兼容、marker坏／prev坏、并发升级锁 | 真旧／新版本 exec与候选启动；切换前／中／后故障；同路径共享 binary sibling；N-1各中间态；race | 候选起不来／首请求失败／迟到 ACK／失联后回滚；原流与原PID连续；新请求继续；跨纪元和不可回退迁移准确拒绝且旧服务保持 |
| 旧代码排空与全量完成 | 资源清单计数、超时不强杀、completed前全组件身份证明、GC | 真长期worker＋短任务；升级不执行的负控制不能过版本证明；≥5轮无旧进程／fd／goroutine／consumer增长 | 分两臂：过渡排空能力需准确显示draining；最终支持集合必须在原SSH仍连续收发、业务进程仍存活时完成runtime状态/FD迁移并退出全部旧映像，不许等任务结束；新增supervisor也必须列入 |
| 崩溃／断电恢复边界 | manifest／marker每个落盘点故障，持久化顺序与幂等恢复 | 各进程kill与启动顺序；存储故障注入独立于race；不用恢复前快照抹掉ACK写 | 真kill候选／旧controller／worker分臂，清楚区分业务是否应存活；持久卷重启后无双owner与假成功；物理断电若未模拟必须记 not_covered |
| 全栈组合与权限部署 | 四层状态／锁依赖死锁、同机agent共享升级域、lease family拒绝语义 | 用户态agent／systemd user／system service／nohup，real process；N-1全部支持组合；整体资源门 | 原进程＋PTY＋文件＋SSH同时活跃，新请求持续；四层按生产方式整轮升级再回滚；无手工脚本替产品完成handoff；最终运行身份、原业务和资源清理全部证明 |

### 5.1 测试执行和证据纪律

每一能力须走完整链：

`requirements条文 → architecture状态与owner表 → unit/component → integration → -race与资源 → 实际N-1二进制／真实进程 → simcluster持续负载drill → 原始收据 → 活文档与review归档`。

- 真实二进制至少区分**同源码改版本戳的生命周期机制验证**和**真实上一 release 与候选 release 的兼容验证**，两者不能互换。必须证明运行 SHA 确实改变，防止“没升级所以业务没断”的假绿。
- 每个 probe 在升级前建立非空成功基线；整个升级窗口不停止、不丢样本；升级后继续足够时间观察旧 owner 迟到动作；记录请求开始／结束时间、seq／reqID、失败、最大停顿。要求原 socket 的 probe 不允许 reconnect。
- 每个重要 gate 至少一个负控制：旧 owner提早退出、两个owner都获权、丢一段字节、漏一条终态、把draining误标completed等必须被对应 oracle 抓到。不能只断言 helper 返回值或日志中的“成功”。
- 资源门覆盖fd、goroutine、child/zombie、PTY、consumer、临时文件／对象和旧worker；按现有测试规范至少5轮相关exercise；有限等待由实际进度／事件驱动，`-race`时间预算不能用生产不可能的lease常量缩短。
- simcluster 使用真实 systemd／外置 NATS／磁盘／TLS／Caddy／权限安装路径；配置环境和产品步骤分开标注，禁止测试脚本代产品转移状态。依据 `test/simcluster/README.md:12` 的 Mandate；分层归属依据 `docs/testing-standards.md:15`，真实deploy层不冒充 `make test`覆盖。
- 每次收据包含源码 SHA／dirty tree摘要、所有binary SHA与release/proto/依赖版本、拓扑／unit／配置摘要、命令／环境／时间、完整stdout/stderr与退出码、每次probe数据、业务PID三元组、原连接ID、transferID及SHA、升级manifest、必要journal、故障注入和清理结果。敏感值脱敏，保留复核所需关联ID。
- 每行状态只能是 `待实现`、`已实现待验证`、`通过（收据链接）`、`失败` 或 `受阻（原因）`；未跑不填Pass。原始收据建议目录由正式plan确定，drill编号等inventory核对后分配。

## 6. 已有测试可复用基础及诚实覆盖边界

以下只表示读到源码，不表示本可行性阶段重新跑过：

- `internal/agent/upgrade_state_test.go:29`、`:465`、`:589`、`:721`：boot状态表、exec失败恢复、commit/watchdog竞态、共享binary目标实例证明；`upgrade_durability_test.go:59`、`:97`：sync顺序和失败闭合。它们可以继续保护安装事务，不能替代新控制权交接。
- `cmd/tether/agent_boot_shim_test.go:26` 是真实构建二进制、坏flag导致的启动预算／回退测试；应保留。它未建立活跃PTY／transfer／SSH。
- `test/p10/upgrade_e2e_test.go:94` 明确 `UpgradeNoExit:true`；`:102` 注入运行映像路径。`test/p10/review_risk_test.go:87` 只读源码字符串证明重启路径存在。不能把这些说成真正不中断reexec的端到端证明。
- `internal/agent/session_generation_test.go:285`、`:311` 保护同一agent运行时的PTY intake／exec当前连接；不是跨进程更新资源保留。
- `internal/agent/lease_shared_state_test.go:103`、`:132`、`:218` 保护租约共享state和回退lineage；新handoff需新增真实双进程authority测试。
- `test/simcluster/drills/33-node-upgrade-success.sh:8`、`:16`、`:26` 已定义真实reexec、运行SHA、watchdog回退、共享升级域；`:40` 明确某回退窗口agent会OFFLINE。它有价值，但不能扩称现有原业务不中断。
- `test/simcluster/drills/30-rolling-upgrade.sh:34`、`:41`、`:43` 有滚动期间真实Raft写与版本／PID证据；不能推导在途transfer和原SSH连续。`31-node-upgrade-fleet.sh:9` 主要验证allowlist／SHA拒绝；其旧头注释关于“无success owner”应以33实际存在为准，不能引用旧注释否定33。

## 7. 固定三路专家规划／交叉审查建议

遵循 `CLAUDE.md:31` 起的阶段流程及固定宽度要求；**本lane没有spawn**。正式规划固定三路无条件并行，不按发现数量临时扩缩：

1. **执行与资源专家**：agent／broker进程边界、原PID、PTY／pipe／Wait、worker生命周期、exec／supervisor与资源限制；提出真实进程反例和部署矩阵。
2. **传输与入口专家**：transfer、SSH／TLS／yamux、NATS／JS、Caddy、listener、在途ACK与背压；核实依赖实际版本和升级接口，证明原连接与新请求并存。
3. **系统与安全专家**：lease／instance／authority epoch、任务owner、Raft／SQLite／migration、原子激活／回滚／兼容窗口、权限与证据充分性。

固定交叉：执行专家挑战系统方案的崩溃／reexec可达性；传输专家挑战执行方案的在途字节／owner存活；系统专家挑战传输方案的重复副作用／兼容／完成点；三路均输出覆盖边界。主进程综合定稿并唯一修改生产；实施后同三维内审与反例复验，再独立外审；不能因新增feature而跳过当前fix的外审门。

## 8. 文档交付与尚需定稿的产品偏好

正式plan须逐行包含矩阵owner、Test/drill名称、预期oracle、负控制、证据路径、完成状态和文档落点；后续每一个热更新功能都走同一链。

- **requirements**：最强范围、业务连续性、可用性预算、计划升级与故障边界、完成状态；如需扩大lease族升级或跨纪元支持，先明确改WHAT，不能暗改。
- **architecture**：组件／resource／authority owner表、状态机、IPC和wire版本、持久化和回滚顺序、失败矩阵；修正§9在途任务针对“计划升级”的新例外，不抹去真实崩溃边界。
- **usage／broker-ops／cluster-runbook**：`staged/serving_new/draining_old/completed`的用户含义，等待原因、查看旧代码、回滚／取消、需要权限的真实动作、兼容不满足时的处理。
- **testing-standards／test README／simcluster inventory**：按实际新增门更新所有归属和owner，drill说明真实／stub／不覆盖；不能机械填一个“全覆盖”。
- **reviews**：定稿plan、tasklist、各轮内审、独立外审、逐项回复与收据链接归档，INDEX登记；下一次实施会读的契约保留在活文档。

用户已决定全栈范围和不中断目标，无需再次询问范围。剩余核心偏好建议默认如下并由根plan明确：

1. **无限长旧连接是否允许长期保留旧代码**：对于不支持的迁移组合，按“不打扰”默认defer并明确显示原因；这仅是安全拒绝姿态。正式支持集合必须实现活跃状态迁移，原无限长SSH和进程继续时退出全部旧代码，不能用无限draining通过最终门。若额外要求任意未支持版本也限时清除旧代码，则不能在尚无迁移能力时同时承诺不打断。
2. **切换期间可接受的最大业务停顿／新请求延迟**：制定可测数字与目标环境；默认不放宽现有业务deadline，拒绝无限等待。数值尚待定稿，不假装已经承诺。
3. **首次桥接与不兼容更新的处置**：默认保服务、defer并报告缺的前提；任何需维护窗口／业务结束才能完成的更新单独显式呈现，不作为热更新成功。

综上，建议正式立项为全栈热更新架构增量，并以本矩阵约束分阶段交付。只有四层真实运行版本切换与所有业务oracle都有收据，才可以说该支持集合已实现“全栈不中断”；仅完成agent worker设计或若干升级后恢复测试不能结项。


## 9. 固定三路交叉意见（2026-09-20，仅规划交叉）

- 已亲读 `/tmp/tether-control-recovery/hot-update-agent-feasibility.md`：认可同lineage双订阅风险、G.1联合snapshot、父PID／Wait、systemd cgroup、首跳bootstrap、多代IPC与最终旧runtime／协调器退出。补充候选ready不可带写库migration副作用；冻结遇阻塞I/O应defer，不能Close业务FD。对方已经明确区分计划内freeze与任意kill-9的write/offset、reap/journal歧义窗口。
- 已亲读 `/tmp/tether-control-recovery/hot-update-transport-feasibility.md`：认可同机raw业务FD移交＋内部TLS/yamux重建／逻辑续接，所有权／单写库／独立NATS目录约束，以及HA新请求与最终退场的分层。补充N=1临时冗余仍受join精确release门约束；产品同reqID重试不能由测试外层重执行代替；候选不能与老owner同开权威写store。
- 传输专家反向发现本稿Caddy矩阵对“原socket”措辞可能误指内部WSS，已修为外部SSH／业务TCP保持、内部WSS主动更换并证原任务续接。也明确NATS退出服务后失败不能承诺该旧实例继续服务，只能halt后续动作并使用仍健康路径。
- 三路没有提出当前规划的额外不可行阻断；可行性以正式支持集合、显式迁移协议和逐项真实验收为条件。本文不签发功能Pass，不声称任何新drill已执行。


## 10. 建议预注册的数值预算与只读部署资源核查

### 10.1 默认数字建议（待根plan采用；不是既有SLA或已测成绩）

这些数字针对已预热、正常可达的计划升级；有意断电／失多数派／慢盘至无法进度等故障臂按其独立恢复契约判，不把故障臂通过改名为计划升级成功。正式plan应在实现前锁定，之后放宽需重新审查并保留原失败证据。`-race`组件测试可放宽fixture等待上限，**不能**据此放宽真实deploy业务预算。

| 项目 | 建议初始硬门／采样 | 解释 |
|---|---|---|
| 正常计划升级业务失败 | 0次业务重启、0外部socket重连、0 EOF/RST、0缺失/重复字节、0重复副作用、0矛盾终态 | 不用百分比掩盖少量真实中断 |
| 新控制请求 | 连续20 req/s（10读＋10带唯一ID的轻量写/命令）；每个请求≤5s，p99≤1s，0终态失败 | 同ID产品内重试记入总耗时；业务本身运行时不计作控制接纳延迟，另验执行结果；不对任意长业务命令套5s |
| 原SSH／PTY交互 | 双向每100ms一条带序号探针；最大升级相关无响应间隔≤1s | 不把用户未输入／业务本身沉默计作升级停顿；独立echo/probe在同业务连接内确认 |
| 单次计划内FD／状态冻结 | ≤500ms；到期尚无安全点则延期本次交接并继续有效服务 | 此为迁移设计目标；未完成迁移不能标completed。不能用强制关FD实现预算 |
| 文件传输 | 任一健康、可写收端无进度窗口≤5s；每10s传输进度至少同形态无升级基线的50%；最终字节和SHA全等 | 基线同数据/同限速/同并发；故意不读端另验背压与资源上限，不能要求它持续进度 |
| 预热及完整组件升级 | artifact已在本机后ready≤120s；ready后单组件完全退出旧映像≤180s；四层一次计划≤600s | 下载耗时单独报告；持续业务probe从下载前已开始。永久SSH仍活时也须完成支持集合的最终迁移；超预算记deferred/失败而非成功 |
| 升级失败回控制旧owner | 可回退前提成立时≤5s恢复接纳且在途业务无终态失败 | 已不可回退的状态切换不得开始；非同一承诺的worker意外死亡单独分类 |
| 每个正向／回滚臂观测 | 升级前60s非空基线＋覆盖全部切换窗口＋全量完成后120s继续业务/新请求 | 原业务至少覆盖整个四层整轮，不把几个各自新建的短连接拼成连续证据 |
| 重复与资源 | 关键handoff/rollback竞态race×3；资源≥5整轮；每轮活动业务终结后30s内task/stream/consumer/旧worker为0 | fd/goroutine由现有共享资源helper加新外进程计数验收；不以RSS高水位单独推导泄漏 |

5s请求预算是候选验收默认值，不是让产品覆盖现有更短调用方deadline；调用方若已有更短期限，仍服从它。正式环境若需要WAN限速，必须另列可复现实验配置和预算，不能混入同一成绩。上述吞吐／延迟门要在正式实现前做无升级baseline可达性检查；若baseline本身不满足，记环境／预算待裁定而非修改产品结果。

### 10.2 2026-09-20只读资源现状

实际只读执行了hostname、nproc、free、df、inotify读取、vendor列表以及经只读提权的docker ps/images；未启动、停止、重建或删除任何容器／服务。

- hostname=`weilandserver`；可见CPU数88。内存total257792MiB、available231663MiB；repo挂载剩余3.5TiB，/tmp剩余806GiB。这是瞬时可用量，不能当执行期资源预留。
- `fs.inotify.max_user_instances=8192`、`max_user_watches=1048576`；与README中实例上限前检需求匹配。
- docker ps成功且无运行容器；镜像有`tether-sim:dev`（358MB）、`tether-sim:base-a3431a1`（361MB）。无运行容器不等于不存在他人的卷／网络，清理必须按owner限制。
- vendor已有tether、tether-next、nats-server、nats、nk、install.sh；未对这些缓存签署当前源码／目标版本身份，也未将其当已可用的N-1 artifact。`lib/stage.sh:37`明确tether-next只是同源码版本戳变化，真N-1仍需另建。
- **真实Caddy/WSS部署尚不能称现成**：`Dockerfile:57`起仅烘焙tether/NATS等；`drills/lib/ingress.sh:12`明确入口为Python sidecar替代真实Caddy。热更新drill必须建立真实Caddy binary/unit/config及WSS链路，不能复用该替身后声称Caddy升级通过。
- 本地沙箱首次docker查询permission denied；只读提升查询成功，未发生auto-review拒绝。正式执行仍应通过获授权的simcluster路径，不把这一查询权限扩成任意现网操作。

### 10.3 合法并行建议（未启动）

- 固定两个部署执行槽：一条N=1（broker＋agent＋ctl，3容器）和一条N≥3（3broker＋agent＋ctl，5容器）；真实Caddy可在对应broker内用真实unit，蓝绿或专门入口需要额外容器时计入该槽。两槽不是上线能力限制，只是首轮证据可归因的调度方案。
- 每槽独占唯一`INSTANCE`、network、volumes、容器label、日志目录和artifact清单；使用`lib/docker.sh:14`的namespace机制。禁止操作默认`sim`、别人的实例或既有报告目录；不得共享业务state／SQLite／JS store。
- cluster grow／JS重新形成阶段同时最多1条；不把`-j 2`本身当family约束。当前runner已有grow-cap等参数（`run-drills.sh:41`），正式命令显式设定所用策略并存regime receipt。README`:330`记录grow并发是独立于inotify的时序风险。
- root独占共享vendor编译/staging和镜像构建，先校验SHA并冻结唯一image tag／digest，再允许两槽只读消费；并行agent不得改同一个`tether-sim:dev`或vendor/tether-next。
- CPU/内存足够的静态检查不自动证明延迟可比。第一轮延迟签署让一个完整全栈drill独占测量窗；另槽可做静态fixture验证或停留ready，但不同时制造grow/磁盘压力。随后另跑规定的竞争场景，保留独立regime而不把竞争失败抹去。
- 完整race矩阵与关键部署延迟签署不要同时满载主机；定向race可在独立构建缓存/端口上并行，但root调度全门。清理仅本人INSTANCE与带所有权标记的日志；不使用全局prune/nuke。

### 10.4 单点与首次迁入的出口门

N=1单broker/单Caddy必须有自己的真实drill：原业务保持、唯一控制owner切换期间请求在≤5s内完成，并在原永久任务还活时全旧映像退出。若实现借临时冗余，要由产品显式编排、独立store、遵守同release join门，并在drill如实记录；测试脚本偷偷补第二个服务不算产品通过。若该支持形态尚不能做到，应标该矩阵行未交付，不能用HA结果替代后把全功能写completed。

首次legacy monolith迁入另列：在没有旧版本导出协议时，只能等原业务自然结束后安装桥接版本，或研发并证明具体旧版本迁入机制。不能只在测试前预装新runtime来声称“legacy首跳无中断已覆盖”。正式产品需在旧任务存在时可观测defer／拒绝危险exec；此安全拒绝可单独验收，但不等于旧任务迁移能力已实现。


### 10.5 消费端能力门与本路最终交叉结果

agent lane补充的ctl能力前置已核对：`internal/proto/messages.go:555`、`:802`、`:894` 的ExecReq/RunReq/KillReq没有稳定request ID。严格不中断还需要ctl或等效可靠边缘层参与reqID、输出seq/ACK/resume及缺回复结果查询；四类被升级daemon的范围不能使正式实现漏掉消费端改动。N-1普通在线兼容与新增严格连续性的能力集合分别登记，缺能力先defer，不在已经断掉旧内部连接后才发现旧ctl无法补收。

已读agent稿§10的有限架构建议：Linux小runtime作为真实MainPID与业务父进程，controller可替换；runtime本身以同PID受控exec保留白名单rawFD和有限manifest，重建自有逻辑流carrier。此方向保父关系并避免复制Go/TLS堆，适合作为第一项真实进程原型；待证明导入早于G.1/孤儿清理、错误导入回退也保全部FD、partial-write/wait状态闭合，不能仅因设计合理就跳过真实原型和全栈drill。主升级及watchdog/boot rollback两个exec入口均必须同协议，否则回滚重新引入断流。

三路规划交叉已完成。支持按固定矩阵进入正式plan与实施；本lane无已确认的额外架构阻断，但所有功能门仍待实现/验证。
