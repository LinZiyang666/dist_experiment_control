# 全栈热更新可行性：broker / data plane / NATS / TLS / yamux

日期：2026-09-20。只读可行性草案；与本轮 control-connection-recovery fix 分开，未改生产、未新增测试、未提交/部署。本文基于当前工作区与锁定依赖源审，不是实现承诺或已完成的连续性证明。

## 结论与目标

可以工程化实现“升级 agent、broker、NATS、Caddy 时，已有业务不因升级失败，新控制请求继续得到服务”。最强可落地路线是：**控制与执行/连接所有权分离 + 可续接的逻辑流协议 + 计划内同机 socket/状态交接 + 冗余控制/NATS/入口滚动切换**。不能只把当前 re-exec 改成 graceful shutdown。

必须分别定义：

- 外部 SSH/TCP：同一条用户连接不出现升级导致的 EOF/RST，不重复、不漏字节；保留用户端与本地 sshd 端两条 TCP socket。内部 tunnel 的 TLS/yamux可以重建。
- 文件传输：同一 transfer ID 自动跨内部重连继续，最终文件 SHA/长度正确、只提交一次、终态一致；允许短时吞吐停顿，不要求同一个 NATS socket 不变。
- 控制请求：新请求可被冗余入口接收并在预算内完成，非幂等请求在回复丢失后的重试不能执行两次。仅把请求积压到单 broker 重启后再处理，不等于任意延迟预算下持续可用。
- 升级完成：所有目标组件的执行代码已到目标版、旧 worker 已退出、没有旧版数据会话宿主残留。无限保留旧 worker 只能报告 draining；旧进程留到会话自然结束与有界完成必须区分。

正常计划内升级可用短暂停读/背压保住字节流；“严格零时延变化”、任意网络分区/掉电、跨不兼容任意版本、任意第三方协议自己的超时不受影响，均不属于可以无条件承诺的性质。应先定义 `L_flow_pause` / `L_rpc` / 最大可用内存与磁盘，再用原型量出能承诺的值。

## 1. 当前实现的实际生命周期

| 层 | 实现证据 | 当前升级影响 |
|---|---|---|
| agent 升级 | `internal/agent/upgrade.go:230` 安装后回包，`:301–314` 延迟100ms后直接 `syscall.Exec`；回滚 `upgrade_state.go:382–388` 同样 exec | 同PID避免systemd看到进程退出，但旧Go heap/goroutine消失；网络FD通常CLOEXEC，TLS/yamux、NATS client、transfer hash/offset都不保留。不能因PID不变就断言SSH流不变。 |
| agent tunnel | `cmd/tether/agent.go:377–379` 在agent进程内构造并Start adapter；`internal/agent/tunnel_adapter.go:16–18` 每进程一个Client；`tunnel.go:1479–1487` 生命周期终止关闭conn/yamux | 本轮修复让**同一个活agent进程**的控制session重建不误拆活隧道，不能跨进程映像替换保存它。 |
| broker 升级 | `internal/broker/reexec.go:37–40` cancel Run；`cmd/tether/serve.go:312–330` 等Run退出再exec；`:399`执行exec | 先执行实际关闭，再重启原PID。 |
| broker tunnel | `broker.go:1104–1108` tunnel.Server挂在broker Run ctx；`tunnel.go:419–421` ctx取消即Close；`:1254–1276` 关闭控制listener及全部session | 该broker承载的公网listener、TLS/yamux会话与存量流会被拆除。`:986–993` 每公网TCP经该session.Open；`:2058–2073` 任一方向结束就关闭两端。自动redial恢复的是新流可用性。 |
| broker NATS/Raft | `broker.go:1072–1079` Drain本进程NATS连接；`clusterwrite.go:151–181` 顺序审计清理、停loops、退订；`broker.go:1037` shutdown Raft节点 | 外置NATS进程不会被broker re-exec自动重启，但本broker的subscriptions、内存tracker和连接会消失。 |
| cluster rollout | `cluster_upgrade_drive.go:96–151` transfer-leader、reload broker、等版本/追平、reexec同机agent；`:330–332` 明言每步barrier未检查JS副本ready；`:438–455` 尾门为agent home确认 | 保quorum与恢复收敛；没有移交存量TCP，也没有等待所有transfer完成后才切换。`docs/distributed-broker-architecture.md:263/:351` 明确hard cutover、rehome在途传输不保。 |

运行进程可能在原PID exec时继续存在，但管道、PTY、wait/事件投递与控制能力是否完整需runtime职责一并设计，不能据“子进程未被kill”判整体成功。`internal/agent/agent.go:654–656` 还明确当前systemd默认control-group的退出风险；拆runtime必须独立unit/cgroup生命周期，不能把它留在会随controller stop一起被杀的单元里。

## 2. 当前依赖版本/配置与官方核查

### 2.1 项目实际默认值（未查询现场主机）

- `go.mod:12/14/15`：yamux v0.1.2、nats-server v2.14.6、nats.go v1.52.0。
- `scripts/install.sh:1115–1116`：NATS默认v2.14.6、Caddy默认2.7.6，均有环境变量override；这是仓库安装pin，不把它冒称每台现场已运行版本。
- `install.sh:1024–1030`：443 Caddy把 `/sub/*` 代理到127.0.0.1:8090，其余到127.0.0.1:8222；没有配置 `stream_close_delay`。
- `install.sh:1240–1248`：JetStream外部进程持久store；NATS WebSocket为loopback/no_tls，WSS TLS由Caddy终止。tunnel的独立TCP/TLS入口及公网expose端口由tether承载，SSH流不经过这段Caddy WSS。
- `install.sh:1323–1334`：Caddy单个systemd进程，ExecStart run；当前cluster升级只编排broker/agent，未实现NATS/Caddy二进制滚动升级。

### 2.2 Caddy：reload与binary replacement要分清

直接核对官方**v2.7.6**：Handler.Cleanup调用cleanupConnections；StreamCloseDelay零值立即closeConnections，关闭记录的前后两端hijacked连接；非零只是定时推迟关闭。当前生成配置用零值。因此不能用“Caddy reload是graceful”推导WSS不断。来源：[v2.7.6 Handler](https://github.com/caddyserver/caddy/blob/v2.7.6/modules/caddyhttp/reverseproxy/reverseproxy.go)、[v2.7.6 streaming cleanup](https://github.com/caddyserver/caddy/blob/v2.7.6/modules/caddyhttp/reverseproxy/streaming.go)。

当前官方文档同样说明默认reload会关闭WebSocket；stream_close_delay只延迟关闭。这个开关也不把TLS状态转移到新binary。全栈方案允许tether内部WSS换连接，由逻辑请求/transfer恢复屏蔽影响；不能把延迟关闭称作通用活连接迁移。[Caddy reverse_proxy Streaming](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#streaming)

### 2.3 NATS：LDM会断开客户端

官方v2.14.6源码与本地模块一致：`server.go:4436–4452`先关client及WS listener，`:4543–4544`逐个 `closeConnection(ServerShutdown)`。LDM用于领导权让渡和分散重连，不保同一个client socket。官方滚动升级说明要求逐台、stream replica追平后才升级下一台；应用仍走重连。[v2.14.6 server.go](https://github.com/nats-io/nats-server/blob/v2.14.6/server/server.go)、[NATS rolling upgrades](https://docs.nats.io/learn/deployment/rolling-upgrades)

因此NATS/Caddy可用正常冗余滚动替换完成升级，但tether必须先具备“内部断线无业务失败”的能力；现有短暂重连能力和对象持久化本身不足证明它。

## 3. 为什么现有TLS/yamux不能只交一个FD

本地编译工具链Go1.26.8源码：`crypto/tls/conn.go:101–107`有已读/待发缓冲，`:171–184`有cipher/MAC、record seq、TLS1.3 trafficSecret。`ConnectionState`只暴露观察状态，不能导出/导入整套运行态。Go官方亦说明直接读写底层NetConn会破坏TLS会话。[tls.Conn.NetConn](https://pkg.go.dev/crypto/tls#Conn.NetConn)

锁定的yamux v0.1.2 `session.go:21–79`保存nextStreamID、bufRead、stream/inflight表、队列和关闭状态；`stream.go:27–38`保存每流send/recv window、状态与接收buffer。丢掉这些，仅对继承TCP socket新建tls.Conn/yamux.Session，不会恢复旧session。[yamux v0.1.2 session](https://github.com/hashicorp/yamux/blob/v0.1.2/session.go)

FD handoff适用范围：

1. **listener交接**：新连接无绑定空窗，旧worker保留旧连接；可做第一步，但长会话不结束时升级不能完成。
2. **同机原始业务TCP交接**：在明确暂停旧读写、冻结用户态buffer/字节位置后，经Unix socket传FD，序列化自有逻辑流状态，再由新worker接管；可保留外部SSH TCP和本地sshd TCP。SSH加密由用户/sshd承担，转发器看到的只是字节，不必迁移SSH密钥。
3. **内部TLS/yamux**：推荐重建，然后用新的可恢复逻辑流协议续接旧业务socket。无需fork Go TLS、序列化第三方私有内存。
4. **跨主机外部TCP迁移**：普通SCM_RIGHTS不能跨kernel。应让稳定入口继续拥有用户socket，后端broker滚动；若也升级入口软件，用上述同机raw socket交接。VIP/DNS变化只解决新连接，不自动把旧TCP状态搬到新机器。

如tether自身终止第三方加密会话（例如SS代理）也要保旧加密状态；需给其独立可交接状态设计，或会话结束后升级终止器。不能把raw SSH转发的可交接性泛化到任何TLS/SS终止器。

## 4. 文件传输：对象还在不等于任务还在

`internal/jsstream/object_reader.go:59–74`为每次读取建立随机name consumer，AckNone、DeliverAll、InactiveThreshold；`:148–167`的batch、seq、hash、size等全在进程heap；`:264–299` Close取消/退订/删consumer。没有跨升级保存读取检查点与任务adoption。

agent push prepare cache本身是内存，`internal/agent/transfer.go:245–251`找不到即transfer_unknown；`:256–263`接收任务绑定当前session ctx和nc；`:329–364`读对象、写临时文件、校验后原子rename。exec不会跑这些defer，不能自动继续原临时文件。broker continuation又由持有tracker的源home响应（架构`:347`）；durable inflight ledger用于恢复后审计/回收，`internal/broker/xfer_inflight.go:879–897`可以写home_broker_restart并删对象，并非传输续接日志。

最小持续方案是transfer worker及其原NATS client不随controller替换；要覆盖NATS/Caddy滚动，最终仍需要：

- 固定transfer ID、对象NUID/版本及size/hash；持久化prepare权限/目标父目录identity、临时文件、已验证/已持久化offset、上传分块receipt；durable成功终态。
- consumer重建按已提交检查点重放，不能把AckNone服务端“已投递”当接收者已写稳。目标写入/哈希/rename保持幂等，取消仍能终止恢复。
- 上传中断需要分块manifest/去重receipt或经过证明的整对象重试，现成ObjectStore.Put不等于任意offset续传。锁定SDK `jetstream/object.go:651–655` 每次Put生成新NUID，`:802–805` 成功后清旧NUID chunks；因此同name自动重Put会破坏仍绑定旧NUID的reader。必须固定对象版本并协调reader adoption/旧版本回收，不能把“同名重试”当安全续传。
- home/controller变更用唯一owner generation原子接管tracker、commit/terminal归属、deadline与reaper资格；旧worker还活时新reaper不能合成失败或删它的对象。`internal/broker/transfer.go:1524–1534`只有持sourcehome tracker者回应，`:1556–1560`在forward前转交terminal权；只延长TTL或保对象不足以迁移任务。
- NATS在途RPC回复丢失要有request ID与结果查询/去重；Core NATS自动重连不能把业务exactly-once凭空补出来。

## 5. 推荐最强架构与最小实施顺序

### P0：先定义可测承诺与不升级的底座

本目标覆盖四类应用进程，OS/kernel/主机保持运行；为计划内升级定义最大停顿、故障时halt/rollback及wire/state兼容窗口。为所有版本/worker公开staged、serving-new、draining、fully-upgraded四种状态，禁止把新binary落盘或新controller ready报告为全栈完成。初次从当前不支持handoff的版本迁入，需要等现有活业务自然结束或维护窗口；不能追溯给现有heap补一个已部署的迁移协议。

### P1：先拆连接与任务所有权

- agent runtime独立持有tunnel、本地业务socket、transfer worker与运行任务/PTY；controller经版本化Unix IPC管理。broker dataplane独立持有公网listener、raw业务socket与内部transport。controller更新不触及它们。
- controller/data worker互认实例与generation、单写owner、有限权限IPC；撤销、token/NID/home epoch、代理fail-closed依然生效。不能为不中断绕过安全撤销。
- 仅允许一个权威SQLite/Raft写owner；旧controller不能和新controller并发打开同一写库。已有`broker.go:994` data-dir锁和Raft所有权不能跳过。通过HA滚动控制节点保持新请求服务。
- 单靠这一步已显著减少日常控制逻辑升级扰动，但明确**尚不满足全部runtime代码也升级完成**。

### P2：逻辑流恢复与transfer续接

- 在可重建TLS/yamux之上定义稳定stream ID、两向byte offset、重放/去重、FIN半关闭状态、flow control、鉴权续接票据与owner generation。
- 两端保留已承诺但未确认的字节，重连只补缺失区间；ACK语义必须与“已安全保存或已按顺序写入业务socket”严格对应。有限buffer/磁盘quota+背压，不能靠无限内存实现“无损”。
- 计划内移交时暂停旧读写并确认无在途syscall，冻结buffer和偏移；socket FD与状态移交后新owner确认，旧owner才关闭副本/退出。失败恢复旧owner，generation阻止双reader/双writer。进程在write成功与offset记账之间被kill的崩溃精确续接另有难点；不能把计划内freeze证明延伸为任意kill-9无损。
- 文件按上一节实现checkpoint、重新绑定consumer和幂等finalize。该层是NATS/Caddy重连时业务无感的关键。

### P3：全栈滚动与runtime自身升级

- 至少三节点broker/Raft与健康的NATS/JetStream副本、冗余WSS入口；先验证副本健康、准备新进程，逐个切换，完成NATS LDM+client恢复后再下一台。切换可短暂停顿，API通过幂等重试在预算内成功。
- Caddy蓝绿/多入口切新连接，旧WSS可以主动重连至新Caddy，由P2遮蔽；全部内部连接迁出后旧Caddy退出，即可真正完成其binary升级。仅设stream_close_delay不够。
- 两端raw socket owner用P2的同机handoff把存量SSH流交给新runtime，内部TLS/yamux重新建通道；旧worker资源清零并退出后才记fully-upgraded。
- 入口如增加独立L4 acceptor，它自己也须支持listener/raw连接与buffer交接；不能把“永远不升级的代理”悄悄加成隐藏前提。内核TCP状态仍留在同一主机。
- 混版先部署可理解新恢复协议的一代，能力全满足后激活；既有安全协议/raft commandVersion门和数据库迁移门继续约束发布顺序。新旧进程状态schema必须有明确N/N-1转换，不能承诺任意版本组合迁移。

## 6. 部署边界

| 场景 | 可以达到的最强承诺及前置 |
|---|---|
| 单broker、单NATS、单Caddy、无冗余 | P1/P2可保已有SSH socket并让transfer自动继续；唯一权威执行进程替换时新写请求会排队，延迟由交接决定。若要求持续有活的写服务，必须加临时/永久控制冗余或拆出不同时替换的权威状态服务。 |
| 单物理机、多实例 | 可在不同端口/独立store目录运行冗余进程并滚动；同机FD交接可行，仍有主机/内核故障单点。禁止两NATS实例共开同一JetStream目录或两broker抢同一写库。 |
| HA三节点 + 多入口 | 可以逐台把agent/broker/NATS/Caddy全部换到新版；新请求走健康节点，内部重连由任务/流协议吸收。公网既有TCP仍驻留原入口kernel，入口软件用同机handoff升级。 |
| 换主机/重启内核/入口掉电 | 普通FD handoff不够；需要TCP状态迁移、虚机迁移或客户端可恢复协议等额外技术。没有这些前置时，不能对不配合的任意SSH/TCP客户端保证原socket不断。 |
| 永不结束的会话 | 只有已实现并兼容的socket+逻辑状态交接才能有界完全升级；否则必须等待自然结束，并诚实保持draining。 |

## 7. 分期验收（必须观察原业务，不只看重连成功）

1. **P1**：两端各升级controller，同时维持一个从测试开始前就建立的真实SSH会话和双向连续编号流；零EOF/RST、零丢/重字节，同一sshd会话/任务持续；后台文件任务完成SHA；新请求持续注入并计分位/最大延迟。不是升级后另开HTTP请求算通过。
2. **P2**：在每个数据读写/ACK/落盘/rename/终态边界暂停并迁移或断NATS；同transfer ID、精确长度/hash、终态唯一，错误时不出现部分目标文件；64并发取消/慢盘/背压仍有界；revoke/evict与升级交错仍按现有安全契约生效。
3. **P3**：真实版本N→N+1二进制与真实systemd unit，逐个升级所有agent、broker、NATS2.14.6基线、Caddy2.7.6基线；长SSH会话贯穿，主动触发内部WSS与NATS重连；验证旧PID/旧binary映射/旧worker FD全部消失。持续新控制请求无重复副作用且都在声明的延迟预算内完成。
4. **负控制**：保留原re-exec、去掉checkpoint、回退字节offset、提前kill旧worker、重复activate owner，各自必须导致测试红。回滚失败应halt在可观测状态，不能默默中断业务后宣称成功。
5. **容量/边界**：长达数日的SSH模拟、不读接收端、大文件、有限磁盘、入口短不可达；给出实际buffer/停顿上界。跨主机/主机断电与协议不兼容单独列为条件，不能由正常滚动绿灯推导。

本路建议root对用户简答：**可以做成，但这是一次运行架构与传输协议升级；最彻底版本应保留外部业务socket、允许内部连接恢复，并把所有旧worker最终退出作为验收条件。** 当前fix改善同进程恢复，既不实现也不应被描述成这个热更新功能。

## 8. 必须落入 test/simcluster 的正式计划门

用户要求该功能经完整simcluster drill并文档化；下列是正式plan应登记的独立职责。由root分配不冲突的drill编号，不能用本地组件内restart替代真实部署验收。

| drill职责 | 真实操作 | 不可弱化的oracle |
|---|---|---|
| agent单层 | 实际staging目标binary、node upgrade、新controller/runtime交接，再rollback | 预先建立的SSH客户端进程不重启、不重连；同一用户TCP与sshd连接持续双向seq；file push/pull保持transfer ID与检查点；进程/PTY持续；旧agent worker退出。 |
| broker单层 | 单台reload及HA全队列滚动，含leader让渡、不同home、broker-only和同机agent | 原公网连接持续；跨home只是内部通道切换，不另建用户连接；新命令持续成功；唯一writer/owner与撤销边界保持；所有旧broker worker退出。 |
| NATS单层 | 仓库pin的真实server binary，通过真实unit/LDM更换，覆盖连接所在节点、JS stream leader、meta leader | 必须观察到内部NATS连接确实换代；同一外部SSH不EOF；同transfer跨消费者重建完整提交；每步JS实际副本current且有quorum，不能只有broker版本门。 |
| Caddy单层 | pin的真实Caddy binary、真实Caddyfile/unit，reload与binary蓝绿分别测，单/多入口分开 | 在强制旧WSS退场后，任务自动续接；外部业务SSH仍是原连接；新TLS/WSS握手有健康入口，旧Caddy进程全部退出；stream_close_delay单独不是通过条件。 |
| 全栈升级/回滚 | 按发布plan逐层升级agent/broker/NATS/Caddy到N+1，再按受支持schema顺序回N；不支持回滚要在切换前拒绝 | 一条从开始持续到最后的SSH会话及双向seq日志、一批贯穿全程的large transfer；逐层新请求统计不清零；全部旧binary进程与旧generation流归零。 |
| 混合故障 | 移交Prepare后新worker死、Activate前controller死、Activate后旧worker迟回包；升级中断NATS/Caddy、慢盘、half-close、取消/撤销；一次只扰动与同时扰动组合 | halt/rollback不能双writer，有限buffer/FD/goroutine/磁盘；正确保留已接受任务或明确可恢复状态；不得静默丢字节、重复副作用、写半目标或假complete。计划外kill-9的合同独立验收，不能与可安全freeze的计划内移交混记。 |

**网络证据**：用一个不带重连逻辑的真实SSH客户端，长运行远端任务持续带序号输出，同时输入另一方向独立seq，记录准确起止和每个缺口；测试进程持有同一个net.Conn，禁止失败后重建连接继续计数。保留连接四元组/内核socket身份及sshd侧会话佐证，必要时pcap证明未出现FIN/RST；四元组相同自身不排除重连，不能单独作oracle。延迟取同一持续连接上的应用级probe；慢收端证明背压而不是丢包。

**文件证据**：逐chunk记录对象identity、源offset、接收者持久offset和digest；在每个chunk中部/落盘后/commit响应丢失点升级；重复读取与重发可以出现，但最终目标只能有一次可见commit和一个一致终态；上游重复ACK/晚terminal不改变结果。已有SHA-only成功收据不足证明未重新从零开始或没有重复业务提交。

**新请求证据**：在整个drill持续以唯一request ID发新请求，覆盖读与真实有副作用的命令；记录总量、成功/失败、重试数、p50/p95/p99/max、升级阶段和服务generation；失去回复后用同ID重试并核对实际副作用计数。请求失败或超时不得在汇总时丢弃；给出具体流暂停/请求预算后才可签“无感”。

**完成证据**：收集各unit MainPID、`/proc/<pid>/exe`实际映像/sha、runtime API报告版本/generation、旧worker清单和FD所有权；re-exec可同PID，因此只看PID变化不够。`staged`、新版本上线、draining、fully-upgraded分开输出；超时仍有旧版本必须非成功/保持明确等待态，不准以无限留存兑现零中断。

**真实部署约束**：至少两份实际不同构建的release artifact，使用项目实际install/staging/upgrade和systemd/reexec路径；网络路径包括公网模拟入口→真实Caddy→真实NATS WS/JS，及真实raw expose/tunnel→本地sshd。组件mock、test-only回调、Go内嵌server Stop/Start可以留作快测试，不能替代这些签署drill。最终plan、drill清单、版本兼容表、部署/回滚手册、资源及延迟收据均需归档。

## 9. 三路交叉质疑记录

- 已亲读 `/tmp/tether-control-recovery/hot-update-agent-feasibility.md`；认可父PID/wait语义、PTY/pipe所有权、同NID双订阅风险、G.1联合snapshot、bootstrap首次迁移以及旧代码退场要求。指出故障drill不能把计划内freeze证明泛化为任意kill-9：普通TCP/pipe写成功但offset未记账、wait4已reap但退出journal未写的窗口仍有歧义。agent lane已采纳并收窄合同；“旧方已死后halt让旧方继续”不能是fallback。
- transfer/agent lane反向指出ObjectStore.Put换NUID并purge旧chunks，以及sourcehome tracker/terminal/reaper的原子归属问题。我亲读锁定SDK和production确认，已补到§4。对应drill需让旧sourcehome与新home的迟到terminal、creator finalize、startup reaper在adoption窗口交错，证明不重复commit/审计且不删活对象。
- 已亲读 `/tmp/tether-control-recovery/hot-update-system-feasibility.md`，认可单owner、HA/单节点边界、已ACK写不能通过回滚旧快照抹除、桥接版本及全旧代码退出定义。指出其Caddy drill须把“原socket不可重建”限定到外部SSH/业务TCP；内部NATS WSS应允许并主动触发换代，业务层续接成功后旧Caddy可完全退出。另NATS readiness须在进入LDM前验证；LDM后旧本机JS/clients可能已停，失败处置应依靠健康多数派并halt后续节点，不能假设原本机服务仍在。已同步system与root，供正式plan纳入。

三路讨论是对可行性、边界与验收的交叉质疑，不是执行通过收据；本阶段没有热更新实现或drill执行结果。

系统lane反向补充已采纳：API重试必须是产品内同reqID查询/去重，drill不能外层换ID重跑；N=1如先加临时broker冗余，受 `requirements.md:348` 的join release精确相等门约束，应先加同版节点再滚动，不能让新版本直接join后拿N-1互通作理由。candidate ready不得提前跑权威DB migration或同时打开写store。

## 10. 给实现者的连接层最小顺序（追加只读设计）

### 10.1 M1：先做状态分层和真实停顿点，保持旧协议

在现有tunnel内先明确三个对象：`Endpoint`拥有公网listener/本地与用户raw socket；`LogicalFlow`拥有两向byte位置、缓存与half-close；`Carrier`仅拥有TLS/yamux及其generation。名称为建议，不是已新增类型。当前 `tunnel.go:947–954` 的yamux关闭→关公网listener，以及`:962–971` accept-loop defer关闭全session，是首先需要在新模式中解开的接缝。旧协议支路保持原语义。

当前 `bridge` 的两个io.Copy没有可导出缓冲，不能直接围上一个锁就迁移。新bridge要有显式、可暂停pump：每次read/write返回的n先计入实体状态，再处理err/停顿；partial write剩余bytes明确保留。实现pause barrier并确认两向pump、accept与carrier回调都已停止改状态。禁止在持全局锁等待网络，停顿不能用Close触发，否则原业务socket已坏。慢socket阻塞时可用受控deadline唤醒并保留部分n，清deadline后恢复；此行为需要真实TCP反例，停不稳则在触碰旧服务前延期本次升级。

### 10.2 M2：独立协商恢复协议，避免破坏N-1

现有REGISTER是严格六字段（`:2081–2084`），client也只接受精确 `OK`（`:1846`）。不能加第七字段、改成 `OK caps=...`，也不能向旧的raw yamux stream插magic，它会被写入业务sshd。

推荐新增TLS ALPN，例如 `tether-tunnel-resume/1`，与产品ProtoVersion分开命名；新client/server共同选中才进入新模式，空NegotiatedProtocol走逐字兼容legacy REGISTER6/OK/raw-stream路径。初始实际TLS pin与token校验保持；ALPN只选协议，不提供额外授权。new→old、old→new、new→new与未选协议的拒降级策略必须真实TLS测试。控制面可加additive capability供upgrade planner预检，但实际流模式取此次TLS协商，不能只信一个旧cache capability。

双方选新模式后仍完成原身份REGISTER门，再建立专用yamux控制流做HELLO/ATTACH/RESUME。server新模式须能把已授权同allocation REGISTER附着到**原Endpoint**，不再次bind同公网口，也不借“恢复”放宽token/NID/home epoch/revoke围栏；新carrier经过resume handshake ready后才原子替换carrierGen。普通legacy slot不能被自动标记可恢复。

ownerGen/恢复票据必须绑定原allocation、当前授权decision和有效交接决议，不能接受来访方自报更大generation就提升owner；持同token的候选也不能直接抢存量Endpoint。拒绝旧manifest/旧generation迟到activate。必须先验证两向prefix可续接再替换carrier，验证失败保留仍有效的旧owner，不能先破坏旧流再发现状态不兼容（系统lane追加交叉准则）。

N-1策略：第一代桥接版本接受legacy+resume，新模式只在双方支持时激活；已有legacy流自然结束或明确延期runtime替换，不能无证据转换其TLS/yamux heap。后续相邻resume版本必须兼容frame/state schema，协商双方交集；降级到旧版没有恢复能力时只能拒绝/延期无损激活，不用静默断流“fallback”。保持现有ProtoVersion/raft commandVersion边界；若选非additive wire方案，必须按项目纪元规则走，不能自行绕过。

ctl也在能力链中：当前 `proto.ExecReq/RunReq/KillReq` 没有统一request ID，旧ExecChunk/PTY输出为best-effort（agent lane源码复核）。因此全栈NATS/Caddy换代的命令去重与输出续接必须包含新版ctl或等效可靠客户端代理；仅升级agent/broker不能补回旧ctl已丢失的字节。桥接代需对ctl请求ID/输入输出seq能力握手并形成支持集合，N-1可互通不等于N-1旧端自动获得新增严格连续性保证。此gate须在接受“无中断升级”前判明，不到中途才发现旧客户端不能恢复。

### 10.3 M3：在yamux stream之上实现可重放字节流

建议每logical flow沿用一个yamux stream，额外用控制流传carrier附着与全局resume摘要，避免把整个隧道改造成一个大单流。新流前置有界header绑定随机FlowID、allocation identity、ownerGen、协议版本；`OPEN`第一次且只一次dial本地端口，`RESUME`只查原FlowID与原socket，找不到必须明确失败，不新dial伪装续接。

最小frame集合：OPEN/OPEN_ACK、RESUME/RESUME_ACK、DATA(offset,len,bytes)、ACK(deliveredOffset)、FIN(finalOffset)、RESET(reason)。严格限制frame size、流数、缓存、resume摘要大小及算术溢出；未知版本/type fail closed。两向各自维护`txRead/txAcked/txRetained`、`rxDelivered/rxPending`、FIN/FIN_ACK；它们与本地socket关联，不随yamux stream ID改变。

接收方每次业务socket Write的n先推进rxDelivered，ACK只确认该连续前缀；相同/更旧前缀重放丢弃，gap拒绝或明确请求缺段；未写完buffer保留。发送方在ACK前保存原bytes，carrier断开只停发并背压，不关闭业务socket。恢复双方交换已交付前缀与FIN状态，补缺失区间，旧carrierGen的ACK/DATA不得越权推进新owner。FIN按字节位置传递并只执行一次CloseWrite；一向EOF不能让另向响应被截断。控制ACK需独立有界队列/调度，不可被bulk send window饿死。

这里`rxDelivered`仅表示原业务socket的Write已接纳的连续字节，不能宣称远端应用已经处理或落盘；正常TCP重传仍由保留的原kernel socket负责。因此handoff必须保同一socket、同一buffer/offset原子状态；文件任务另外使用自己的持久化checkpoint与commit语义，不能复用此ACK当文件写稳证明。

这个模型解决carrier切换与**计划内可确认freeze**，并不自动解决任意kill-9在Write完成与状态记账间的歧义。若未来要扩到后者，需接收端去重/另层可靠endpoint等额外方案和独立合同。

### 10.4 M4：raw socket owner的可恢复exec/双进程交接

先实现独立runtime与受认证Unix控制口，保留当前实例、FlowID/Endpoint/ownerGen/取消状态。新binary预热与schema验证成功后：stop新接纳→两向pump安全点→冻结manifest→持有/复制FD白名单→持久激活决议→exec或SCM_RIGHTS接管→重建Go net.Conn与pump→恢复新carrier→确认→回收旧引用。允许新连接在稳定listener/backlog或已经ready的新acceptor等待，但必须有明确上限与成功探针。

same-PID恢复exec可保持子进程父关系且不触发systemd cgroup stop；只清业务raw FD/PTY/pipe、listener与移交manifest FD的CLOEXEC，TLS/yamux/NATS FD保持CLOEXEC后重建。manifest应经私有匿名FD/受认证本机IPC传递，敏感key不进argv/env/日志。启动shim必须在普通agent/broker初始化、migration或重新注册之前识别恢复入口，导入资源、恢复authority并阻止正常Start另绑listener/另dial本地目标。

成功exec后进程已换像，失败回滚必须能读取同一handoff schema和FD清单；现有upgrade marker的boot/register提交门不足，需跟整个升级事务结合。syscall.Exec返回错误时原映像必须解除freeze继续服务；exec成功而新初始化失败不能把已继承FD全Close后才考虑rollback。两进程移交则要求旧方保留raw FD副本直到新owner确认，但旧方不能再读写或调用CloseRead/CloseWrite；其退出只释放副本，不发送终止业务的协议动作。

### 10.5 M5：ssproxy AEAD可以设计导出，但当前不能直接交接

源码已亲读：`ssproxy/crypto.go:84–90`保存reader nonce、解密buf/off；`:96–120`读length/payload的中间buffer在局部栈；`:137–169` writer nonce先递增再写，而ciphertext和部分write进度也在栈。`server.go:668`在handshake后构造reader，`:740–756`创建另方向salt/subkey/writer；`:761–774`两个io.Copy。当前没有snapshot/import或pause接口，仅传accepted+upstream两个FD会丢AEAD framing状态。

与Go TLS不同，这里加密实现是项目自有、锁定单一ChaCha20-Poly1305，AEAD原语可由每方向32-byte session subkey重建，不需要导出第三方巨大heap。可实现的最小state ABI应包含：

- 两向subkey、下一nonce及nonce是否已被本条length/payload使用；client/response salt与发送salt进度。
- reader phase（salt/length/payload/relay）、已收ciphertext和目标长度、解密但未消费plaintext及off；writer phase、已封装ciphertext、发送wireOffset、已接受plaintext及其完成语义。
- 原accepted FD、原upstream FD、target与已完成的安全校验、keyID/keyset generation、两向half-close、deadline/取消状态。
- `Server.saltSeen`有界anti-replay集合（`:64–66/:103–142`）、key→两端FD归属及当前密钥撤销状态；恢复后仍能按`:432–440`同时关闭被撤销subscriber的两端。

必须先把partial read/write显式实体化，不能只等“下一个完整AEAD chunk”——对方停在半chunk会令无限长会话永远迁不走。迁移时不得新生成salt、把nonce归零、重复Seal不同plaintext到同key/nonce；对已生成的未完整发出ciphertext只能续发原bytes。candidate需先验state长度/phase/nonce，再成为唯一sender。恢复不重dialupstream、不重做产生第二连接的目标握手；未完成DNS/dial/认证的操作也须建phase或在有界窗口内由旧owner完成，否则升级延期。

先用真实SS客户端保持加密会话、分片length/payload、partial write、half-close、revoke与salt replay做跨exec测试，再放入simcluster全栈drill。只测完成chunk处恢复不能覆盖上述状态；只保salt/nonce也不足。此工作应与ssproxy职责owner独立实现，不把它隐含在普通tunnel raw FD能力里。

### 10.6 顺序裁定

M1显式状态/安全点 → M2双协议桥接 → M3同进程carrier重建不断原socket → M4跨runtime真实exec接管 → M5 SS状态导出/接管 → 与可靠transfer/RPC、HA及真实NATS/Caddy升级整合。每一步有真实反例和独立门，再逐能力simcluster；最终全部实现且全部旧代码退出后才能结束全栈目标。
