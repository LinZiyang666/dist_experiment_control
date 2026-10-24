# 全栈热更新 plan

日期：2026-09-20。状态：**实现中；基础交接包已验证，业务接线与全栈验收未完成；不是热更新 Pass 报告**。

**会话交接（2026-09-20）：** 用户要求因时间限制转交 Claude。原目标和本计划保留；最新实现、两条未修复 Runtime 失败、未执行测试、证据/源码快照和接续顺序，统一从[主交接文档](hot-update-handoff.md)进入。当前没有完成全栈热更新，也没有外审放行。

## 0. 授权、范围与工作流

用户选择“最彻底的”热更新：agent、broker、外置 NATS、Caddy，以及为实现热更新新增的运行层，都进入范围。升级期间原有业务进程、PTY、exec 输出、文件传输、SSH/TCP/proxy 和新控制请求持续工作。用户另明确要求：**此后的每项热更新能力必须经过完整 test、真实 simcluster drill，并落入文档**。本计划把它作为逐能力放行硬闸，不作为可选后续工作。

这是独立于 control-connection-recovery 的叶子增量。起点为 HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025` 加该修复的未暂存候选；修复收据见 [第二轮内审](control-connection-recovery-review-round2.md)。其 43 个新增测试和全绿闸门**不证明热更新**。实现、测试、文档保持暂存区外，不提交、不部署现网；完成内审及修改后停在独立外审门。

遵守 CLAUDE.md：固定三路并行草拟 → 三路互相质疑 → 主进程定稿 → 主进程实现 → 固定三路独立内审和交叉质疑 → 主进程修复、复验 → 外审。使用会话原生 agents 代替 Claude Workflow，继承主模型。专家负责设计建议、审查和独立测试；仅主进程修改生产实现及定稿。三个规划视角是：任务/进程与传输、连接/隧道及基础服务、身份/持久化及验收。

配套 [tasklist](hot-update-tasklist.md) 逐项记录真实状态；新增问题先补计划，不在实现中偷偷缩小范围。
实际测试/部署收据统一登记到 [证据索引](hot-update-evidence.md)，所有热更新能力初始为NOT_RUN。

独立设计原稿：[任务/进程](hot-update-design-agent.md)、[连接/基础服务](hot-update-design-transport.md)、[系统/验收](hot-update-design-system.md)。原稿保留各路建议及修订，本文的主进程裁决优先。

## 1. 业务契约与完成判据

### 1.1 不打扰的可观察定义

1. **进程**：原业务 PID、boot ID、start ticks 和业务 nonce 保持；不因升级重启、收到 HUP/终止或失去管理。stdout/stderr、stdin、PTY resize、信号、ps/kill 和退出码仍有效；不能只证明 OS 进程还存在。
2. **字节流**：原外部 SSH/TCP socket 和 agent 到业务端的 socket 保持；双向字节按序、不丢不重，half-close 正确，客户端无需重新连接或重开 session。升级后能新建连接不等于原连接连续。
3. **传输**：同一用户调用、同一 transfer ID 继续；长度和 SHA 完整，目标一次原子提交，终态和审计唯一；取消/撤权仍生效。禁止先报 failed、后台重传后再报 complete，或把另一次用户调用算作续传。
4. **新请求**：整个升级窗口持续接纳并完成新的读写/业务请求，在预定延迟预算内，无升级导致的终态失败、无重复副作用。客户端内部有界重试必须绑定稳定请求 ID；测试外层无限重试不算成功。
5. **权限与归属**：sid/nid、home/port、凭据权限不变；每项资源始终有唯一有效 owner。旧代不能重新获得命令执行/提交权；撤销、kick、cancel 优先于继续搬运业务。

允许替换内部 NATS/WSS/TLS/yamux carrier，但必须由可靠续接机制使上述业务无感。零业务中断不是零调度耗时；明确的延迟/背压预算见 §7。宿主重启、断电、跨主机迁移任意原 TCP 和升级业务程序自身的内存不是本次计划升级契约。

### 1.2 三个不同的完成点

- **控制接管**：目标版本唯一接纳新请求，全部已接纳请求和业务有可查 owner。
- **业务迁移**：旧代持有的活任务、原 socket、PTY、传输状态已交给目标代，原业务验证仍成立。
- **全量完成**：范围内所有组件的运行映像达到目标 SHA；旧映像、旧资源 owner、订阅/consumer/租约、临时凭据与升级锁退出或回收。新增 guardian/runtime 也在清单中。

拟定状态：`requested → staged → ready → activating → serving_new/migrating → completed`，另有 `deferred`、`rejected`、`rolled_back`、`rollback_blocked`。`staged`、register 成功、磁盘文件已替换都不是 completed。旧 worker 承载业务时必须显示版本、SHA、资源数和原因；无限期保留旧 worker **不能满足最终交付**。迁移失败应保业务、报告未完成，不以杀业务强行收敛。

## 2. 当前实现与不能复用的假设

| 当前事实 | 热更新必须补齐的能力 |
| --- | --- |
| agent `upgrade.go` 直接 `syscall.Exec`；同 PID 保 lineage，丢 Go 堆、泵、PTY/pipe 与会话 | 显式资源清单、安全点、FD/状态 ABI、接管恢复；进程存在不等于输出和管理连续 |
| proc/execChildren、退出 courier、prepare cache 在内存 | 活任务联合 snapshot、可靠退出与结果日志、唯一 waiter/committer |
| broker reexec 先取消 Run 并关闭 tunnel/Raft/SQLite | 控制、隧道、transfer continuation 的分立生命周期与可恢复所有权 |
| `xfer_inflight` 重启补 `home_broker_restart` 并清理 | 可恢复 checkpoint 与有围栏的 reaper，不能把审计 ledger 当活任务 checkpoint |
| 同 instanceID 双 agent 可同时订阅同 NID | 唯一接纳入口/代际 fence；预热候选不得执行业务 |
| 规划基线的 legacy raw tunnel reconnect 只恢复新流；S5 v1 合同保留原 socket | 保两端业务 raw socket，增加有序逻辑流续接、ACK/重放/背压 |
| TLS/yamux/WS 协议状态不由底层 TCP FD 表达 | 内部 carrier 重建；不导出 Go TLS 或 yamux 私有堆 |
| ObjectStore.Put 每次生成新 NUID，成功会清旧版本 chunks | 版本固定与 reader adoption；同名重新 Put 不能直接充当续传 |
| systemd 默认 control-group stop 会杀同组子进程 | 产品部署生命周期与资源所有权一致，兼容 user unit/无 systemd 启动 |
| NATS lame duck 关闭客户端；Caddy reload 默认可关闭旧 WSS | 产品可恢复请求/流 + 准备好的备用路径；配置 reload 不等于 binary 热换 |
| drill 30/33 验证滚动/真实 reexec 的一部分 | 原 PID、原流、同 transfer、新请求、全旧版本退场的独立连续性 oracle |

源码事实与三路质疑归档到相应设计记录；上游行为按仓库实际 pin 核对。实现若升级依赖，重新查官方源码/文档与本地 go directive/toolchain，不沿用旧版本结论。

## 3. 定稿架构与不变量

### 3.1 可恢复运行层和唯一控制入口

将命令接纳与业务资源持有分开。首选同进程的 workload/flow/transfer manager 与 control 职责接口，agent/broker 各自整体同 PID exec；不要求先引入常驻 controller 子进程或私有 IPC。运行层独占 task→owner、代号、已接纳请求结果、活任务和待确认退出记录；控制逻辑只提交经认证的请求，不能在 handler 栈私藏资源。接口及导出状态有明确版本，一个 NID 同时只有一个执行入口，候选预检不能订阅可执行命令的入口、占活 store 写锁或迁移 schema。全部命令、PTY intake、信号、取消、reaper、accept/dial 都受同一准入与所有权屏障保护。generation 本身不是凭据，跨节点/本地管理入口仍核验身份与权限。若将来拆为 IPC，另补 peer/UID、随机实例能力和安全路径认证。

任务父进程优先采用**同 PID、显式状态可恢复 exec**，保留业务父子关系和白名单 FD。候选预检在激活前完成；实际 exec 前暂停读写/Wait 到可恢复安全点，保存用户态缓存、任务标识、退出/取消、FD 类型与归属，新的映像验证后恢复。无法到达安全点则 defer 并继续原业务。禁止清空所有 CLOEXEC、继承不相关秘密 FD，或仅用 SCM_RIGHTS 假称新进程能 wait 非子进程。

同进程 manager 保留类型化接口和完整状态 ABI，不能把旧对象改名后直接 exec。运行层及协调器也必须升级；不能把永不升级的新守护进程从完成清单排除。部署单位/cgroup、正常 stop、kick/evict 的清场责任明确；仅把 runtime fork 到同一会被 stop 杀掉的 unit 无效。保留 no-systemd/setsid-nohup 支持。

**ctl 是可靠链路的一端，也必须改造**：现有 ExecReq/RunReq/KillReq 无稳定请求ID，ExecChunk/PTY实时输出不能靠agent单边恢复。增加请求幂等、输出seq/ACK/resume和能力协商，覆盖正运行中的旧ctl。N-1仍须普通互通，但没有续接能力的旧ctl不能被宣称满足新增严格连续性；激活前检查受影响会话，保持旧路径或defer并说明具体不支持者。不能只升级服务端后把客户端输出缺口排除统计。

可靠进程操作采用独立`process-resume`请求入口，类型化区分probe/create/resume/ack/terminal-ack。不能只给旧ExecReq添加RequestID后重试：N-1 broker会解码并重编码旧结构、丢新字段，而旧agent仍执行命令；一次probe通过后版本或leader变化也会击穿。新入口缺少handler时没有执行副作用，旧exec入口保持原兼容语义。broker继续做真实session/member/node授权并重新stamp ActorFP；稳定ID/参数hash、字节offset和terminal ACK由新的运行层处理，迟回复不能改变其他调用的责任。此项先定协议、再接三端和N-1反例测试，不把capability声明当实现证据。

请求去重键绑定sid/nid、已认证actor、客户端lineage、agent签发的leaseID和requestID，跨runtime generation不变；同ID异payload必须拒绝。创建租约的HMAC绑定目标实例、boot ID、身份与CLOCK_BOOTTIME期限；只限制首次接纳，已知任务过期后仍可续接。ctl发送前保存完整handle，自动重试不可换leaseID或requestID。热更新保留实例、签名key、活任务和必要tombstone；冷启动没有原表时必须换实例/key。终态至少保留到租约过期且业务结果责任已结清，GC后原租约不能重执行；新租约复用requestID属于新调用，不能当作原调用恢复。表有硬容量上限，满额在副作用前拒绝，不驱逐活任务或必要tombstone。验证reply丢失→切代→结果回收→旧请求晚重试窗口，区分正常升级保证与崩溃后无法判定的结果。

新入口必须在任何错误响应前用subject里的完整User NKey验证actor私有reply前缀及具体subject语法，拒绝跨actor、通配符、旧_INBOX或业务subject；ActorFP不可反推该前缀。agent只接受broker盖章身份并以实际forwarded subject的SID/NID核对租约。旧exec路径不受这一新增协议限制。

### 3.2 资源交接协议

每一资源的 manifest 包含：稳定资源 ID、sid/nid、owner generation、协议/状态 schema、相关原 PID/start ticks、FD 清单、读写偏移与待发 buffer、权限/取消状态、deadline、终态和确认进度。握手阶段必须有可观测屏障：准备 → 停收并归集已接纳请求 → freeze → snapshot/持久决议 → import 校验 → 唯一激活 → retire。

传 FD 采用白名单 exec 继承或经认证 Unix socket 的 SCM_RIGHTS，按资源实际需要选择；共享 FD 不意味着允许两方同时读写。每个阶段说明谁持有读/写/rename/wait 权，超时恢复哪一方，哪些决议先 fsync。候选失败且旧 owner/完整状态仍在时，旧方恢复；旧方已死不能写成“自动回到旧方”。

显式覆盖 wait/reap 与 journal、write 与 offset 的崩溃歧义。计划内 freeze 可证明连续；任意 SIGKILL 恰在 syscall 成功而记账未完成时，普通 FD+日志不能泛保恰一次和完整退出码。每个崩溃窗分别规定可证明恢复或明确 unknown/error，禁止伪造成功。

### 3.3 可靠逻辑流和已有业务 socket

S5 的精确合同已冻结于 [flow-carrier-ss plan](hot-update-flow-carrier-plan.md)（lead Q1–Q3 覆盖正文建议），
活契约在 architecture §22.8。下述总范围保留；执行状态以 [evidence §16](hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index)
为准，当前 NOT_RUN pending P29。broker exec/S6 与 NATS/Caddy/S9 不由 S5 代签。


在可替换内部 carrier 之上定义逻辑 stream ID、方向、连续 offset、ACK、重放窗口和 FIN 状态。两端 socket owner 在 carrier 重建/切换时存活或可恢复移交；重连按已确认 offset 续接，去重在向业务 socket 写入前执行。S5 ACK 精确表示接收端原业务 socket Write 返回的实际 n；received 与 applied 分开，去重先于 Write。
不能把 carrier 投递、对端接收或应用已处理当作 ACK。每主机/每 flow 一个 8 MiB send log，与 exec/PTY 共用 256 MiB budget pointer。

同时覆盖 OPEN/连接建立中、空闲流、双向高吞吐、单向 half-close、慢接收、拒绝旧 epoch、目标已关和权限撤销。重放缓存有上下界与反压，无 ACK 不无限占内存。迁移中切换 pins/home 仍须遵守现有 peer proof、fence 和 public port 所有权。

首选TLS ALPN协商续接模式；旧REGISTER六字段及精确OK应答保持原样，只有双方选中新能力才使用新逻辑帧。legacy模式继续互通，不能在旧raw stream前偷偷插header。RESUME只接回原FlowID的原socket，禁止重新dial本地sshd冒充原流。恢复票据绑定原allocation、当前授权和已确认的handoff决议；来访方自报更大generation不能夺权，过期manifest/迟到activate拒绝。

SS proxy 自己终止 AEAD，不能用 SSH 原连接通过推导其通过：须显式保存盐/子密钥派生输入、nonce、帧解析状态、待写密文/明文和 replay filter，并验证移交前后密码序号不复用；秘密不进日志或明文普通临时文件。自写解析器要 fuzz。无法安全表示的协议状态必须在激活前拒绝，作为交付未完成项保留。

### 3.4 传输和 broker continuation

稳定 transfer ID 绑定对象 NUID/版本、长度/SHA、方向、源/目标授权及可信 parent-dir/temp inode；持久 checkpoint 记录可恢复偏移/块确认、预算、取消、摘要恢复策略、commit 决议和终态。prepare 已确认尚未 commit 同样属于活工作。续写必须证明源版本一致，不能悄悄采纳同名新对象。

broker 迁移同时交接 tracker、watchdog、commit/terminal 权、deadline、reaper/GC 资格。旧任务未交清时，新启动不得合成失败或删对象。目标 rename 前后丢 ACK 可查询确定结果；晚到旧代消息不能 finalize 新任务。拉取、上传、tier A/B、取消、同步/重命名期间都要覆盖。

Raft/SQLite/JetStream store 各自遵守单 writer：禁止双进程打开同一 store 并承担写入。控制面升级依赖健康 quorum；data owner 交接不能通过回滚旧数据库快照抹掉已经 ACK 的新写。

### 3.5 NATS、Caddy 与拓扑

四层按依赖顺序逐节点、逐组件迁移，不同时停全栈。至少验证 N≥3 的 Raft quorum、JetStream stream/consumer 副本和外部入口实际可达；启动 candidate/readiness 在旧服务退出前完成。NATS LDM 之后不能仍假设旧服务接受新请求或提供本机 JS。

Caddy 配置 reload 与二进制替换分开实现/验证。切入口之前预热新的 TLS/WSS 路径，使内部连接主动迁移；业务 socket 由 tether 的连续流层保持，避免无限保留旧 Caddy。新地址必须对所有受支持客户端可用、认证/证书正确；新增入口进程自身也须能退出旧映像。

单 broker/单入口需要产品提供的临时冗余或可验证的本机交接路径；不能把 N≥3 的通过外推到 N=1，也不能让脚本替产品手动搭接。资源不足时升级前 defer，继续原业务；这只能证明安全拒绝，**不能作为 N=1 热更新已实现的证明**。N=2 过渡态、quorum 不足、replica 落后必须独立验证。

临时加 broker 仍须满足现有 release 精确 join 门，不能用新版本 joiner 绕过。NATS/Caddy 选择具体升级/回滚版本后，固定 artifact SHA、配置兼容和官方支持条件。

### 3.6 兼容、回滚与第一次安装

首个支持集合是 Linux、同协议纪元、相邻受支持 release、可回退的状态/schema；兼容矩阵分别列 wire、manager 状态 ABI（若有 IPC 也纳入）、磁盘 manifest、Raft op、SQLite、NATS 与 Caddy 配置。N-1 wire additive 不等于资源状态可跨任意多代。最多两代同时活跃；第三代升级须先迁清前代或 defer，不杀原业务腾位置。

保留既有 SHA/smoke/原子安装/prev/fsync/升级域锁，增加资源交接与运行映像证明。回滚也是一次有围栏的接管：不得重跑已执行请求，不得抛弃新版本接纳的工作，不恢复陈旧 SQLite 快照。不可逆 schema 变化、未知状态字段/类型、跨纪元在激活前拒绝。

**首次 legacy→可交接架构是独立入口**：当前旧二进制没有导出活 Go 状态的接口，不能追溯承诺所有已存在的流可搬走。实现能力探测与 busy/deferred 提示，证明拒绝不损伤业务；允许自然业务结束后进入支持版本。若要对原有无限业务实现首次无损，须另有真实旧版本适配器及独立证据，不能用预先部署新 runtime 的测试掩盖此入口。

首次迁入的idle判据也必须完整：单次ps/节点状态不能证明没有PTY、prepare、传输或连接，且检查后可能立即接纳新业务。必须先证明所有相关入口已禁止新接纳，再取得完整资源闲置证明，保持到激活结束；无清单/无quiescence能力的legacy只能defer，不能推断idle后直接exec。真正无旧进程/资源的初次安装另走可证明的安装路径。增加“观察idle后新任务/连接并发进入”的负控制，拒绝有竞态的首次迁入。

## 4. 实现顺序与责任边界

### 4.1 H1 首个基础模块的实现合同

`internal/handoff` 提供Linux同PIDexec的显式状态/FD包：`Prepare(State, []Resource)`复制调用者白名单FD并创建sealed memfd；`Prepared.Exec`仅在Go ForkLock保护的exec窗口开放这些复制FD的继承，失败恢复CLOEXEC；`Restore`先消耗私有环境变量、核验schema/PID/EUID/实例包身份和FD指纹，再恢复CLOEXEC并导入；`TakeFile`移交单项所有权，`Close`只清理基础模块自己的副本。调用者的原FD/状态flags始终保留，socket通过SyscallConn而非会切换blocking模式的File/Fd路径复制。

此模块只保计划内exec的白名单状态和内核资源，不自行冻结业务、不证明完整资源清单、不提供崩溃持久journal或远端权限；运行层必须在安全点调用，并给所有非业务秘密FD设置CLOEXEC。manifest秘密只在匿名内存中传递，不写普通磁盘/日志；总大小与资源数有界，未知schema/缺FD/指纹不符拒绝导入。非Linux返回明确不支持。接入正常升级前先以真实子进程exec验证父子Wait、原pipe/TCP、白名单隔离、失败恢复与≥5轮资源门；单包通过不等于整个hotupdate完成。

基础层还拒绝原本非CLOEXEC的输入FD，且不修改调用者的flags；否则原FD与其manifest副本会同时继承，隐藏引用会拖延EOF并破坏资源退场。并不因此宣称已扫描所有无关FD，运行层完整清单仍是必要前提。Go netpoll的deadline、bufio和泵已读未写缓存也须由adapter显式恢复，不能从FD保留推导自动恢复。

### 4.2 H1 业务接线裁决

三路完整建议与交叉记录：[任务](hot-update-runtime-wiring-agent.md)、[连接](hot-update-runtime-wiring-transport.md)、[系统](hot-update-runtime-wiring-system.md)。root 采纳同进程职责分离路线，全部 §1、§5–§9 合同保持。

1. 先统一普通 exec/PTY 的任务身份与 sole wait-owner。运行层针对已登记 child 做 `wait4(WNOHANG)`；reap→保存真实 status/退出码→exit-pending 与 freeze 互斥。不使用可失落的 `cmd.Wait` 栈或 `os/exec` 隐式 stdio copier 作为恢复依据；信号与 reap 同步，避免向已复用 pgid 发信号。starting/abandoned spawn 也必须归集，未完成时不能激活。
2. 逐步把显式 stdin/stdout/stderr/PTY、部分读写字节及 seq/ACK、四种终态（child exited、流 EOF、输出 ACK、退出事件 ACK）归运行层。进程退出不证明后代持有的 pipe 已 EOF；取消也不能用关闭原 FD 代替暂停。 普通exec在输出仍可能由后代持有时，以原子AdoptDraining/ReapHeld保留leader的wait权与PID占用，输出EOF后AllowReap；不能提前reap后因ErrExited跳过对仍活子树的kill/evict，也不能对已回收裸PGID盲发信号。
3. pre-Cobra bootstrap 先识别包、恢复 CLOEXEC、校验完整状态并保留回滚所有权；恢复授权 tombstone/原绝对期限和原本地泵先于网络注册。联合快照不得暂时遗漏任务并触发 G.1 的 EXITED(-1)。
4. `resource-active` 与 `control-ready` 分开：任何 raw I/O 或 Wait 已推进即使控制未 ready，初始 snapshot 也已过时。回滚必须重新导出当前态。Exec 返回错误可解冻原映像；成功后旧堆已消失，返回型初始化失败经保 FD 的同 PID bootstrap 回到兼容版本，不能普通 exit/restart。
5. 任意新映像 fatal/唯一 owner 被 kill-9 仍受 §3.2 崩溃边界限制；不声称常规 supervisor restart 保原 socket。remote-fs/控制探测的不可中止 syscall 也进入预检，整进程 exec 不能仅凭本地 artifact 就宣称满足 500ms。
6. 256MiB 重放不能塞入 64MiB JSON manifest；大块数据使用受额度管理的匿名payload FD/范围索引。不可变片段可完全seal；循环重放日志则固定size并seal grow/shrink，由唯一owner的freeze屏障排除写入，恢复后仍由唯一owner续写。原FD不经File.Fd改flags；完整资源包绑定其身份，元数据只含连续base/end/FIN和容量，不复制全部backlog。默认每流8MiB、每节点256MiB，容量预留也计入节点上限；满时背压。freeze只收最后有界delta。原deadline使用同boot可比较时钟，不能升级后重新给完整预算。
7. ctl 可靠请求/输出与所有其他资源的登记、屏障未完整之前，不开放活跃业务热升级。模块接线的局部测试不冲抵实际 agent/broker/四层 drill。

| 工作包 | 生产职责 | 出口要求 |
| --- | --- | --- |
| H0 契约/传感器 | 能力/资源清单、事务状态、独立业务探针和旧实现失败基线 | 证明探针会抓当前 reexec 的真实中断，冻结预算/版本/拓扑 |
| H1 运行层 | 进程/PTY/pipe/Wait/事件与可恢复 exec、版本化 manager 接口/唯一 owner | 原 PID、输入/输出、ps/kill、退出码，candidate 失败和五轮资源门 |
| H2 连续流 | raw socket 交接、可靠 carrier resume、SS 状态 | 原双向 SSH/TCP/SS 流、half-close、撤权，旧执行映像退场 |
| H3 传输连续性 | 稳定对象版本、持久 checkpoint/commit、broker continuation/reaper | 同 transfer ID 完成、SHA、唯一终态、所有关键窗口 |
| H4 升级编排 | staged/ready/activate/migrate/complete、升级域、N-1/rollback | 不兼容拒绝、失败恢复、共址与共享 binary sibling、多代 |
| H5 基础服务 | NATS/JS、Caddy/入口更新、N=1 与 HA 产品路径 | 真实基础服务 SHA 换代、业务与新请求持续、旧版本全部退出 |
| H6 联测/文档/内审 | 组合、压力、安全、故障、完整闸门及三路对抗内审 | §5–§9 全部闭合，停独立外审门 |

工作包是工程顺序，不能把 H1 或保旧 worker 通过称为完整热更新。具体新增文件按职责命名，先利用现有模块；新增测试函数只在实现时命名登记，避免写成不存在的守卫承诺。

## 5. 每项能力的强制测试矩阵

**每一行都要同时完成自动测试、部署级 drill、证据和活文档；任一缺项，该能力不得标 completed/Pass。** 下表现在全部待实现/待运行。描述的是测试责任，不冒充已有测试或预占 drill 编号。

| 能力 | unit/component/integration 与负控制 | 必须运行的真实 simcluster drill | 独立验收 oracle |
| --- | --- | --- | --- |
| 唯一 owner/接纳 | 乱序/迟到 ACK、重复 request ID、双候选、旧代重放、manager 身份/所有权越权及管理入口路径攻击 | 原/新真实进程争用同 NID，升级全程发带副作用的命令 | 每请求一次副作用、无双订阅执行、节点不改名、读写逐请求收据 |
| 普通进程/exec | 真 subprocess、PID 复用、Wait once、pipe 背压、子树、退出与交接竞态 | 原进程持续计数，升级 control 与 runtime 后操作原任务 | PID+bootID+start_ticks+nonce不变，stdout/stderr完整，ps/kill和退出码准确 |
| PTY | 真 PTY、输入/resize/KA/信号、HUP 敏感前台、退出移交 | 原 PTY 持续交互及 resize，跨 controller/runtime 升级与回滚 | 无 EOF/HUP、双向序号完整、原前台 PID、结束无 zombie/FD 残留 |
| SSH/TCP 既有流 | 真 TLS/yamux/carrier 换代，多流、OPEN窗口、FIN/half-close、offset错位 | 原 SSH 与双向 TCP 长流贯穿各层升级，同时新建流 | 禁止应用自动重连，原 socket/session nonce，双向字节/顺序/最大停顿 |
| SS proxy | 真 AEAD 分帧、nonce/盐/重放表、截断、迁移与撤销竞态、fuzz | 原 SS 长连接与新连接并发，升级期间撤销一个订阅 | 其他原流不断、被撤销流及时停，无密钥复用/串流/秘密泄漏 |
| tier A/B push/pull | 真 JS/ACL/WSS，版本锁定、慢接收、cancel、sync/rename前后、终态丢ACK | 至少66,549,440B和1GiB，双向并行；逐层升级与回滚 | 同调用/transferID、源目标SHA+长度、一次原子提交/终态，无假失败 |
| remote-fs/控制任务 | 运行中查找/打开/读取、取消、挂载停滞/超时、跨代能力 | 活跃 remote-fs 与进程/transfer混载升级 | 原请求不重复副作用、不无期限挂起，进度与资源有界 |
| broker/持久化 | 真 Raft+SQLite、ACK写恢复、tracker/GC围栏、store双开拒绝 | follower/leader/原home升级，连续真实写与传输 | 所有ACK写保留，活任务不被reaper误收，单writer/终态 |
| NATS/JS | 外置真实版本，publish/reply丢ACK、consumer迁移、副本不足/旧目录拒双开 | 实际 nats-server 升/回滚，N=1/N≥3，auth_callout 与 WSS | 运行SHA变更，业务oracle连续，新控制逐请求成功，旧server退出 |
| Caddy/入口 | 真 TLS/WSS，配置reload与binary换代分臂，坏证书/配置 | 升/回滚实际Caddy，持续内外连接与新TLS握手 | 原业务socket不重建；内部WSS可迁移；入口可用且旧Caddy退出 |
| 回滚/兼容/首次迁入 | 错SHA/纪元/manifest、candidate启动失败、部分接管、新请求后回滚、N-1 | 真实新旧artifact、legacy busy/idle、共享bin/共址agent、无systemd | 原业务保留、无重复命令、运行映像与状态一致；不支持则明确拒绝 |
| 全栈/多代/清理 | 依赖锁顺序、容量、两代上限、取消与升级、五轮race/leak | 顺序完整升级/回滚及至少五次换代，持有不结束的业务 | 有限期限所有旧映像/owner退出，活业务仍连续，资源回到基线 |

### 5.1 故障注入矩阵必须覆盖所有交接阶段

阶段至少包括：artifact/smoke、candidate ready、停收新请求、freeze前后、snapshot写/同步、FD发出/收到未ACK、ACK未持久化、激活决议、首个新请求、旧FD回收、退出reap/日志未落盘、transfer写/sync/rename/终态、旧版本退场。

每阶段分别注入 candidate退出、可恢复carrier/交接消息断连或截断、超时、磁盘满/只读、慢消费者、权限撤销；适用时加旧owner退出、NATS断连、leader切换、入口失败和并发升级。预先说明该臂期望原业务连续、可恢复还是明确失败/未知。kill-9 与物理断电不是同一证据，未跑存储断电就不声明其覆盖。

必须有反例/变异证明 oracle 有效：恢复旧reexec、丢checkpoint、错offset、提前关原socket、漏记录新请求、双代激活、只换磁盘不换运行映像、永留旧worker，各自必须被对应断言抓红。不能靠 fixture 自动重连、手工搬状态或重启产品漏掉的服务制造绿灯。

## 6. simcluster 的部署真实性与执行账本

1. 使用 `test/simcluster/README.md` 的 Mandate：真实 Docker/systemd/sshd、独立 NATS、真实 Caddy TLS/WSS、真实 tether binaries、真实安装权限/持久卷与 CLI 升级入口。只读核查发现现有 `ingress.sh` 用Python sidecar，Dockerfile未供给真实Caddy；必须先作为**机器供给**补齐，再运行能力drill，不得记为已有覆盖。
2. 由 inventory/owner manifest/expected-verdicts 分配实际 drill 和臂；与现有30/31/32/33逐项对应旧覆盖，保留旧oracle，新增完整业务oracle。未实现前不写假 Test 名、不预占既有编号、不把安全defer称成热更新GREEN。
3. 基准身份必须包括旧/新 tether、NATS、Caddy artifact SHA 与实际 `/proc/.../exe` 身份，HEAD+dirty diff摘要、构建参数、拓扑、副本数、权限、配置摘要；不把“磁盘新二进制”当运行新版本。
   现有stage的`tether-next`只改版本字符串，可用于exec机制/运行SHA反例，不能代替真实相邻release的N-1兼容臂；后者必须使用各自源码构建的真实版本。
4. 本机若为weilandserver直接 `./local.sh drill <name>`；使用独立instance/卷/端口，遵守contention registry、调度器和锁，不把并行钻测施加到现网。部署动作由主进程协调，固定agent可并行读证据/审测试。首次可用两条部署lane（N=1、HA），grow最多一条；共享vendor/image由root串行冻结，不与本机全量Go测试争资源。启动前重新确认空闲资源，不能把规划时的空闲读数当长期保证。
5. 证据含UTC时刻、每请求/每流序号与时延、PID/nonce、transferID和哈希、真实故障屏障、运行身份、资源时序、四条日志流及verdict。先验证传感器非空、有负控制，禁止筛掉失败样本再统计。
6. 执行状态与runner verdict分栏记录；未执行为`NOT_RUN`，已运行保留原生`GREEN / PRODUCT-RED / SETUP-RED / ASSERT-FAIL / INCOMPLETE`及实际runner其他状态，不改写原始判词。环境未就绪只能记未验证；修环境后重跑。全部必要臂GREEN才可写能力Pass。

WSS供给还须修正地址建模：现有`agentyaml.sh`只剥`nats://`，直接传`wss://...`会生成非法tunnel地址。fixture必须在首次供给时分别生成NATS/WSS URL与纯host:port的tunnel地址，覆盖解析正负控制；不能在产品失败后临时改配置掩盖原因。

## 7. 验收预算与资源限额

预算在正式首轮验收前冻结；以下是**本增量目标，不是当前产品已达标的声明**。生产用到的值由单一常量/配置提供，测试引用，不在drill里私自放宽。

| 项 | 首个验收profile |
| --- | --- |
| 拓扑/并发 | N=1 与 N=3分别；至少2 agent、1 ctl容器；4条原双向TCP/SSH、2 PTY、4普通进程；4个tier-B使用至少4个独立session/bucket和独立ctl上下文并发，保留现有同session串行准入门；SS及remote-fs另有独立臂 |
| 持续控制 | 每秒至少10次新请求，涵盖读/写/短业务命令；升级导致的终态失败=0、重复副作用=0；每次端到端≤5s（含排队/freeze/重试），并保留p50/p95/p99/max。长run/exec须≤5s真实接纳并有原业务PID/started证明，随后流与最终退出另验，不要求长任务5s完成 |
| 活流停顿 | 非主动慢接收臂原流探针最大无进展≤5s；双向字节零缺失/重复/乱序；业务自己的短deadline不能被升级悄悄延长 |
| 文件进度 | 非故障/非限速臂每10s有可证的持久进展；同操作完成且SHA一致；限速臂使用预登记生产预算，仍受有界资源约束 |
| 激活/收敛 | artifact已就绪后，每组件交接≤120s、基准全栈滚动≤15min；无限业务必须完成迁移，不能靠等任务结束达标 |
| 本地freeze | 从旧业务泵暂停到新映像原泵可用每次≤500ms，包含exec、Go init与import；准备/大状态复制在freeze前完成；不能把120s交接预算全部变成停收窗口。到不了安全点则延期并保原业务 |
| 重放/代数 | 默认每逻辑流≤8MiB未确认字节、每节点总重放≤256MiB；最多两代；达到限额背压或延期升级，不杀活业务 |
| 资源回收 | ≥5个真实exercise周期；goroutine/FD采用仓库内建基线门；另核对进程/consumer/temp/旧映像/锁，无单调累积 |

先跑稳定负载校准并记录环境/资源，证明发生故障时不是传感器饿死；不能为已失败样本临时涨预算。若设计证据要求修改数值，先更新plan、说明用户可观察影响、三路复核，再对完整适用臂重新验收；测试超时余量与产品预算分开。

## 8. 文档与收据硬闸

每个工作包完成时同步更新真实行为相关文档，不等最后口头补充：

- `docs/requirements.md`：业务连续性、全栈范围和完成/延期的需求。
- `docs/distributed-broker-architecture.md`：运行层、单owner/事务、协议与持久化、真实恢复/崩溃边界；逐项替换旧hard-cutover契约，保留适用于非计划故障的边界。
- `docs/usage.md`、`docs/broker-ops.md`、`docs/cluster-runbook.md`：命令、状态、前提、首次迁入、回滚、容量不足/不兼容时的操作；无隐藏手工步骤。
- `docs/testing-standards.md`、`test/README.md`、`test/simcluster/README.md`：新增测试层/命令、drill职责、原socket/原PID/逐请求/旧映像退出oracle、实际覆盖与未覆盖。
- `docs/devices.md` 或本地设备操作说明：仅在真实部署资源/依赖变化时更新，不提交秘密。
- `docs/reviews/hot-update-tasklist.md`、本plan、设计/内审报告和证据索引：每项链接实际测试/臂/命令/rc/日志/版本，保留red→green与变异收据。
- `docs/reviews/INDEX.md`：追加当前状态，内审通过不代写外审Pass。

过程记录放 `docs/reviews/`，活文档不能把设计写成已实现；临时原始日志必须有可核验的归档路径与摘要，最终报告写清存储位置。**没有测试、没有真实drill、没有落文档，三者任一缺失都不算功能完成。**

## 9. 最终闸门与停点

迭代先跑相关测试；最终同一候选必须过 `make test`、完整 `make e2e-parallel`、单份 `make lint`。并发/隧道/PTY/传输/Raft变更另过 `-race` 与内建≥5轮goroutine/FD泄漏门；手写解析器增加有界fuzz。inventory只追加、结构/ctx/豁免账本只收紧；改闸门后跑 `make gates`，其lint与其他lint串行。

`make gates` 中的simcluster **hermetic自检不等于真实部署drill**；§5真实drill必须另跑。不得把并行矩阵通过后再全量串行当作必要确认；只定位具体红项。

固定三路内审分别覆盖任务/传输、连接/基础服务、身份/状态机/测试文档，并互相审查修法；root逐条回复采纳/驳回证据、修复并复验。tasklist所有必需项有真实完成证据、无阻断finding、所有能力映射齐全后，形成以Pass/Fail开头的内审报告，**停在独立外审门**。未完成工作不靠状态标签、保留旧进程或更换验收口径消失。

## 10. 固定三路交叉裁决

| 质疑 | 主进程裁决 |
| --- | --- |
| FD可传但新进程不能wait旧子进程 | 采纳；任务父runtime选同PID可恢复exec，非父收养方案需额外独立证明 |
| ctl实时输出仍best-effort/请求无ID，服务端单边修复不够 | 采纳；ctl请求幂等/流续接纳入实现、能力前置与N-1矩阵 |
| 有限去重日志回收后，旧请求是否会再执行 | 禁止；跨代身份键不变、payload冲突拒绝，过期未知结果不作为新请求执行 |
| 同NID并行controller会fan-out执行 | 采纳；预热无执行权，唯一入口与代号围栏不可省 |
| Put同name改变NUID且purge旧chunk | 采纳；绑定对象版本、协调reader和commit，不以整对象重Put代替续传 |
| 只保object不保tracker，reaper仍会误杀 | 采纳；tracker/终态/GC资格作为同一迁移责任 |
| 原socket与内部WSS混为一谈会迫使永留Caddy | 采纳；保原业务socket，允许可靠carrier换代，真实旧Caddy必须退出 |
| FD+日志不能覆盖任意kill-9的write/reap缝隙 | 采纳；逐窗定义连续/恢复/未知，禁止泛保恰一次 |
| fork runtime但systemd仍杀整个cgroup | 采纳；部署单位/停止/清场归属进入产品和真实drill |
| 排空无限SSH能否算全升级 | 否；必须迁移并证旧映像退出；draining/deferred只是诚实中间态 |
| 首次旧monolith无export仍宣称无损 | 否；能力探测、busy拒绝与idle迁入独立验证，未有适配器不作追溯承诺 |
| 单次ps为空可否安全首次迁入 | 否；完整资源清单+冻结新接纳持续到激活，无此能力的legacy一律defer |
| N=3成功能否代替N=1/Caddy独立验证 | 否；分别实现和验证，defer只证明安全拒绝 |

## S5 frozen resource and receipt refinement

The S5 resource graph is agentimage → runtime/Manager, tunnel Client → flow Manager → raw/replay,
and SS Server → actors/listener/upstream/salt history; broker Endpoint retains public listener/flows/fences.
TLS/yamux is rebuilt, never serialized. PREPARE precedes freeze, final Prepare re-exports every retained domain.
Restore validates/takes all resources while frozen, then Manager/revoke → raw → REGISTER/RESUME → fresh SS grant/bind
→ NATS/register. First active resource makes rollback a new current-state cut; pristine Detach is no longer valid.
SS keys/salts/nonces/grants/parser state remain in sealed memfd, not Bundle/Resource.Data/marker/logs/argv/env.

Lead Q1 permits bounded local-applied authority with known lag refused; it does not promise global immediate
revocation at leader commit. Q2 keeps idle legacy listeners/retained slots busy. Q3 retains material histories
at64/node, removing revoked authority before refusing new material/authentication. Domain gates cover actual
candidate/prev SHA at pre-mutation, frozen bundle and final re-export; pending old reader admission stays closed
until durable commit. ABI1 alone is insufficient. The full 500 ms freeze interval remains calibration,
normal survivor/new-request max5 s hard; native Darwin and cross-build receipts are separate.

Tasklist B-2 and D-1..D-6 follow plan §11.2: receipt-backed agent subitems only; D-2/D-4/D-6 broker responsibilities
remain S6, PTY same-SID recreation remains S8 and infrastructure switching S9. No new completion is signed here.
