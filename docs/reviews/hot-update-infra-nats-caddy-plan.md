# S9 · infra-nats-caddy 定稿计划（XL）

状态：**lead 冻结（2026-10-12）**。下方「lead 定稿裁决」覆盖正文与候选「定稿裁决」表中的任何冲突；正文其余部分即为绑定计划。

## lead 定稿裁决（冻结；覆盖正文）

| # | 裁决 | 落点 |
|---|---|---|
| LQ1 | **采纳 Q1 推荐。** 首发 infra hot cut 承诺 linux/amd64 + systemd + pidfd/cgroup/nft；缺能力在 before-effect 明确 `unsupported`，保留冷路径与其它平台的安装/构建；不以编译通过外推平台。 | §3、§6、P02–P07 |
| LQ2 | **采纳 Q2 推荐。** Caddy 首发支持面：stock 两版、filesystem ACME、tether 自有 HTTP/WSS/TCP 路由与可验证的有限普通 HTTP；未知/不支持的活责任（活 QUIC、第三方长连接、自定义 module、外部 storage）先 defer 并说明原因，绝不强断后签成功。 | §5、P08–P10 |
| LQ3 | **采纳 Q3。** S7 已冻结生产 `transfer --rate-limit`（S7 LQ2）；85 的四条 transfer 以启动时固定速率贯穿 N3 九 hop，禁止 harness 暂停 producer 或故障后降速。 | §9、P19、P32 |
| L22 | **测试进程监督复用共享 helper。** P27 的 `test/infraharness` 进程监督必须建立在 `internal/testharness` 已落地的共享 subreaper + CLOEXEC lease + pidfd 监督（S6 FIX-ORPHAN）之上，只补 infra 特有部分（cgroup/unit 登记、deathwatch 适配），不得另起一套并行实现。 | P27 |
| L23 | **与 S7/S8 并行时的开工顺序。** 现在开工：P00，随后与 S7/S8 无文件重叠的叶子包（P01–P17、P27，及 P19/P20 的中立接口合同测试部分）。与 S7/S8 包拥有同一文件的包——P18、P21–P26、P33–P38（共 70 个重叠文件：install.sh、Makefile/goreleaser/simcluster 供给、wire/ACL/admin、broker/agent/ctl 接线、CLI 与错误码、gates/账本/矩阵、活文档）——等对应 S7/S8 包合并后以合并树为基线启动；其余按 DAG。 | §8、§11、§12 |
| L24 | 同 S7 L16–L18：重型套件按测试分区、单元 600s 不变；包只跑授权的定向检查，三硬闸/`make gates`/whole-race/drill 只由 lead 在同一候选上跑，drill 与 go test 严格串行。 | 全部包 |

## 候选稿原状态行

状态（合成时）：**候选，待 lead review/freeze**；不是实施、测试或部署收据。核对快照为 `e0a218b1ee2b1b372933d653ce8be5210af7e472`，只读 `git status --short` 为空。四份草稿、四份 critique、指定契约、安装脚本与相关源码已读；本轮没有运行 build/test/vet/make/lint/Docker/drill/ssh/installed tether，没有触及 `~/.tether`。官方四个 linux/amd64 archive 仅在内存读取、校验、读取 tar member 并计算 SHA，未落盘、未执行。全部运行验收仍为 `NOT_RUN`；成本为 `ESTIMATE / NOT_MEASURED`。

权威链：stages-plan §S9及已定 OQ2/OQ3 → CLAUDE、architecture §21/§22.*、gotchas、testing-standards → 冻结 S5/S6 合同。S0–S5已关闭；S6源码已经实现 single / sole voter / N=3 的同 PID 真 cut，最后 gates/drill 尚待关闭。不能用冻结 S6 文中的旧“尚未实现”重写当前事实，也不能替 S6补签收据。S7候选 `/home/weiland/s7wt/drafts/synth-plan.md` 已存在，所列 API、rate-limit、fixture、receipt 均是**提案**；本稿只冻结 S9侧 adapter 合同，等实际交还后核对。

## 定稿裁决（候选；lead 冻结后覆盖正文）

| # | 裁决与理由 | 机械落点 |
|---|---|---|
| L1 | 保留两个 N1 成功出口：default single、tether sole-voter + standalone JS；二者均原目录 stop/restart，无 LDM。NATS拓扑不能由 tether voter数推导。 | §4；85.A/A1，原4条transfer、ACK前缀、5s/10s |
| L2 | N3先完成 successor安装、checkpoint及可迁出的客户端迁移，再 LDM；LDM后只确认/收尾已准备的迁移。实际 LDM先关闭 client/WS listener并关闭JS，不能此时首次发 hint。 | §4.2；T29/T34/T59 |
| L3 | N2 NATS、任一实际组无 surviving quorum/current、lag/未知资产均 defer。readiness只读，不隐式把 R1/R2升副本；healthy N2 Caddy另有正臂。 | §3；85.D/Q/L，86.H |
| L4 | Caddy选 **alt-port transient + 产品 nft canonical redirect**；不用 SO_REUSEPORT，不把首次 TLS失败解释成重试成功。只改客户端8443不能覆盖无 hint 的新443连接。 | §5；86.B/I，raw TLS zero failures与≤5s logical请求分账 |
| L5 | 旧 root/on-failure Caddy必须带原负载完成首次 retrofit；新主为 tether/always/bind capability。旧root不 load caller配置，以 exact pidfd graceful SIGTERM退休。 | §5/§6；86.I |
| L6 | 独立、可更新、薄 root helper与 exec launcher；控制 DTO下沉 neutral leaf，root不链接业务 bootstrap/NATS。相比共用 tether hidden mode，可机械排除 agentboot/init和业务依赖。 | §2/§6；T07–T15、依赖闭包门 |
| L7 | Caddy admin用受权限保护的 per-invocation Unix socket；旧 root loopback端口由产品先隔离普通UID。localhost/Origin/token不是 UID权限边界。 | §5.1；T19/T72 |
| L8 | ACME warm copy只用于只读预热；旧writer退出后最终当前态同步/fsync/owner/selector提交，才释放持久 startup fence。回滚也用当前树。 | §5.3；T20/T21/86.I |
| L9 | 120s是正常组件完成上限，不能给仍承载业务的 shadow设置自动 kill期限。失败 halt、保健康路径、阻下一步；recover最后退休。 | §5.4/§7；T23/86.F |
| L10 | 迁移 ACK必须绑定真实 raw socket incarnation、新 cid、serverID、指定 Caddy accepted socket→PID→exe。URL、证书、HTTP200或伪 generation均不足。 | §2.3/§5；M04/M09/M17 |
| L11 | 普通 NATS reconnect新增 auth-first gate及正常queue探针。S6 image局部handler探针保留原用途，不代签实际认证服务。 | §4.4；T38/T39 |
| L12 | planned NC取消在 DNS/TCP/TLS/WSS最底层生效并 join全部worker；沿用旧20s/10s+10s梯子不能签5s。业务owner不绑NC ctx。 | §4.5；T32/T33 |
| L13 | S9独立 signed old→new route proof桥接真实健康代际，原8s到期不延长；不放宽 S6 exact-route票据。 | §4.6；T36/T37 |
| L14 | pre-repair ACK-prefix验证是产品恢复门，统一挡住 S7 writer、audit replay、reconcile/rederive及GC；观察器不能暂停producer代办。 | §4.3；T40/T41 |
| L15 | 新独立 infra subject + 严格 request codec；不扩展旧 reload签名，不让旧broker误走 reexec。缺能力显式拒绝，timeout不降级。 | §7；T46/T47 |
| L16 | NATS2.14.5↔2.14.6、Caddy2.7.5↔2.7.6；默认pin不另升。helper/launcher也有真实两代archive、运行身份、更新/回滚与退出账。 | §3.1/§6.5；P01/P20/P26 |
| L17 | S8只交通用事务/域合同；nats/caddy注册、rootjournal/status/external-runtime adapter全部归S9。S7交continuation，S9交NC迁移/cohort。 | §8；P19/P20 |
| L18 | 新代码从第一天落最终 leaf；P00永久合同+独立失败桩先落，每个文件恰一个owner。全部 legacy文件 ceiling均递减，不预支S7将来腾出的行数。 | §10/§12；41包、精确文件清单 |
| L19 | 重集成从第一天逐顶层Test分片；whole-race覆盖集合不能缩。每个spawn在Start前接父死监督，不能只靠Pdeathsig或t.Cleanup。 | §9.5；P27/P36 |
| L20 | 正85/86父判词只包含正臂；普通mutant原生ASSERT-FAIL1，PRODUCT-RED3只认独立钉住的历史损失签名。 | §9.3；原生计数不翻译 |

## §0 范围与五出口

五出口是 AND；safe defer、磁盘换件、配置校验或诚实halt都不能冲抵成功臂。每条实际证据绑定同一最终 source/artifact/运行映像及 oracle，不拼接不同候选的绿灯。

| exit | 必须交付的行为与证据 | 唯一集成 owner |
|---|---|---|
| E1 | infraupgrade逐转换表：ready fail-closed、LDM前后不同恢复语义、同store双开拒绝、exact-release临时broker join门、每步故障/丢回复恢复、旧未退不得completed；真实进程负控补纯状态表盲点。 | P28；T24–T31/T59–T63 |
| E2 | pinned embedded NATS实发/实际effect/回复被截断的publish与request lost ACK；consumer退役/恢复、ordinary auth重订阅、replica不足、JS503时原checkpoint等待与恢复。 | P28/P30；T38–T43/T59–T63 |
| E3 | 85.A/A1/B升级→回滚→再升级：原SSH/TCP/SS/PTY/exec/四transfer保持；每hop实际current、旧server退出、运行SHA；每秒精确10slot全部≤5s、持久进展≤10s；N2/quorum/lag拒绝独立列。disk-only/oldalive mutant必须被同oracle杀死。 | P33，物理夹具P32 |
| E4 | 86.R reload与B binary分臂；I首迁、H healthy N2、bad config/cert；内部WSS真迁出/迁回、canonical fresh TLS无失败、原业务socket不重建、旧主与temporary退场；ACME最终态与有限grace；永留旧主/假main-ready负控。 | P29/P33 |
| E5 | 32/73/98原native期望不劣化；三硬闸+gates；pin/ping、所有结构/库存/enum/error/ctx/race/leak/tag、hermetic lint-install、Darwin；支持制品和供给闭环。 | P34–P39 |

scope_in逐项定位：artifact/catalog→§3.1；readiness/JS→§3.2；N3→§4.2；两个N1→§4.3；N2/双开/join→§3.2/§4.1；Caddy reload/binary/retrofit/ACME→§5；installer/特权面→§6；CLI/admin→§7；S8组件/四层次序→§7/§8；双版本simcluster/B-6/address核验→§9。B-6只按这些真实WSS/transfer/地址子项勾选；G类各条按事务/退场/支持制品实际证据签。S10五代/15min全混载、现网部署不在S9；不改变Raft OpType、commandVersion、SQL migration、ProtoVersion、REGISTER六字段、S5 flow/SS wire。

## §1 源码事实裁判

行号均以本快照为准。`NS`=`github.com/nats-io/nats-server/v2@v2.14.6`，`NC`=`github.com/nats-io/nats.go@v1.52.0`，均从本机module cache读取。表内“未证明”不等于已运行失败。下表broker/agent/natsconf/proto等短路径均相对internal/；带cmd/test/scripts前缀的路径相对仓库根。

| 分歧/草稿前提 | 代码证据与裁定 | 设计结果 |
|---|---|---|
| S6还只取消Run再exec | 当前 `broker/reexec.go:73–137`，`broker.go:1642–1655` 已在Run内显式shutdown/cut；旧文档描述过时。 | 复用现实现，等待最终收据；不重造S6。 |
| S7在途transfer已能切 | 当前 `brokerimage/cut.go:103–108`、`agent/upgrade_image.go:248–264` 仍拒在飞transfer。 | S7候选不算实现；P19/P30为真实WAIT。 |
| sole-voter就是clustered-JS N1 | priv错误；`natsconf/render.go:68–73`、`preflight.go:449–452` 明定standalone；runbook §3a同义。 | B/S/JS分别建模，85.A1不能增加隐藏peer。 |
| LDM只是温和通知，还能先在旧NC协商 | `NS/server/server.go:4450–4452,4474–4478` 先关client/WS listener及JS，:4527才发INFO，随后渐关既有clients，:4564退出。exit/business批评正确；关闭listener不等于此时全部clients已退出。 | 先prepare/move，再LDM；默认2min不作业务预算。 |
| stop是graceful | `NS/server/signal.go:149–160` stop=SIGKILL，ldm=SIGUSR2。 | N1证明same-boot已ACK数据，不声称掉电持久性。 |
| sync always可reload建立资格 | caddy稿错误；`NS/server/reload.go:1977–1985` 未支持差异拒绝，`opts.go:484–485,2696–2701` 是解析而非reload。 | 不加隐藏restart；两版R1默认同步写路径实测。 |
| 缺SYS只能任意换SystemAccount | `NS/server/server.go:2371–2373` 默认建立$SYS；`reload.go:1744–1745,2200–2214` 可重装users/accounts；:1950–1953拒任意SystemAccount变化。 | 使用**已存在默认$SYS**加窄user、保持system_account配置不变；不是新增/改名账号的承诺。两版live reload必须过T27。 |
| broker已有$JS.API.>所以能meta stepdown | `auth/permissions.go:572–596` 是subject权限；broker属$G（`natsconf/render.go:99,182–205`）；`NS/server/jetstream_api.go:2970–2972` 要system account。 | 专用SYS principal，真权限测试，不让fixture添全权user。 |
| 旧single可热加monitor再ready | `natsconf/takeover.go:81–85`、`reconcile.go:136–138` 拒这种假定；installer :1230–1248没有http monitor。 | 用INFO/JS API/本地proc/SYS请求观察旧服务；monitor仅可在本次合法继任启动时启用。 |
| ReconnectHandler里装auth就可保证先后 | `NC/nats.go:3379–3401,5876–5903` 自动重发所有SUB后才调用handler；broker现handler只日志（`authcallout.go:74–76`）。 | 新auth-only NC与business dial gate，普通重连有独立owner。 |
| S6认证probe证明生产queue | `brokercontrol/authcallout.go:228–243` 临时非queue订阅应答；正常queue在 `broker/authcallout.go:229`。critique正确。 | 正常activated member/agent无辅助responder探针。 |
| URL/serverID能签Caddy迁移 | `NC/nats.go:6462–6465` 可直接改current，:2510–2513读该值；:6487–6501才是server cid。Caddy共用上游serverID。 | raw socket+cid与Caddy PID的物理归属联合证明。 |
| ForceReconnect可用ctx包成5s | `NC/nats.go:2465,2492,6412` 同一mu/flush；`ws.go:655` Write无自身deadline；`agent/conn_teardown.go:3–30,52–57` 已记10+10与DNS例外。 | 先解除原代底层阻塞再API/join，不弃goroutine。 |
| broker可迁到peer保持连接 | `broker/authcallout.go:49–68` 与gotcha #89明确本机服务责任。 | broker auth/业务NC坚持local-only；远端业务client迁走。 |
| 原S6 hold可覆盖新路由 | `agent/roster.go:779–806,817–821,898–909` 绑定URL/serverID/healthGeneration/digest。 | 独立S9 proof，不改S6匹配。 |
| Caddy有admin live connectionID census | stock admin没有该接口；access log是请求结束的结果；[v2.7.6 admin源码](https://github.com/caddyserver/caddy/blob/v2.7.6/admin.go)、[server源码](https://github.com/caddyserver/caddy/blob/v2.7.6/modules/caddyhttp/server.go)。 | P02提供实际kernel producer，无日志推零。 |
| WSS=0可保证/stop有界 | [v2.7.6 app.go](https://github.com/caddyserver/caddy/blob/v2.7.6/modules/caddyhttp/app.go#L117) 的grace=0为无限；[streaming.go](https://github.com/caddyserver/caddy/blob/v2.7.6/modules/caddyhttp/reverseproxy/streaming.go#L300) 有upgraded关闭路径。 | 先全TCP/HTTP责任封口归零，再exact graceful signal；未知长请求在stop前defer。 |
| installer已经换运行NATS、Caddy权限已合格 | `scripts/install.sh:1134–1168` 换盘，:1265–1273 NATS always/2s，:1331–1334 Caddy仍root/on-failure。 | stock存量是强制正控，不在READY前修掉。 |
| release自动带新helper | `Makefile:16–18`、`build/goreleaser.yaml:29–57` 只build tether；installer :393–402只提取tether。 | P26/P25明列真实构建/归档/提取/回滚。 |
| admin wire不在库存 | 三稿错误；`proto/wire_inventory_test.go:122–146` 已含admin Request/Response/RuntimeReport。 | 追加原账本；root IPC/journal另账本。 |
| package总行数下降就可在小文件加行 | `structural_budget_test.go:978–984,1019–1032` 所有存量文件都有ceiling；:1063–1069还有精确计数。 | 每文件净Δ≤0，新增≤700；不做事后slim。 |
| ClusterHealth定义在messages.go | `proto/alerts.go:21–42` 才是真定义。 | P21唯一owner；移动需P34更新真实scanner。 |
| 新arm可带下划线/透传negative-control | `arm-manifest-lint.sh:89–92` 只准alnum；`simcluster:719–739` 只传ARM，没有透传argv。 | 原生--arm，无自创参数；多余参数明确拒绝。 |
| image guard已经覆盖供给 | `simcluster:693–697` 只比tether SHA；新helper/Caddy/installer变化会漏。 | 完整COPY/运行文件manifest，逐输入stale负控。 |
| lint-install已有可靠全扫描 | `tests/lint-install.sh:98` greedy取最后<<，:131只验总数≥10。 | 精确targetset+纯parser多heredoc负控，P37实接run-all。 |

辅助上游证据只用于锁定版本的设计，不代运行收据。systemd有效capability/Exec前缀语义以[v256 systemd.exec](https://github.com/systemd/systemd/blob/v256/man/systemd.exec.xml)和[systemd.service](https://github.com/systemd/systemd/blob/v256/man/systemd.service.xml)核对；实际宿主支持版本仍须86记录。

## §2 P00先落：冻结合同、状态和观察闭包

### 2.1 包依赖与精确签名

`internal/infracontract` 只依赖stdlib及 `internal/controlbudget`；不得import proto、infraupgrade、broker、agent、NATS、systemd adapter。privileged可执行闭包不经过业务入口。实现包按接口互相调用，factory在P22/P23/P24组合，不能让leaf互相import形成环。

以下为P00绑定签名；HostRequest/HostReceipt/RouteProof及其可达类型的私有codec使用下列无tag字段的Go原名、固定声明顺序，不加omitempty；内存接口不编码。空slice规范为null，禁止双拼写；需要改变编码先改version。所有跨进程codec的字段顺序/类型/长度/枚举值另由同包descriptor与独立golden双向冻结；不得用 `any`、任意map、path/argv字符串逃避闭合合同。

~~~go
// internal/infracontract/types.go
type Deadline = controlbudget.Deadline
type Stop <-chan struct{}
type Digest [32]byte
type ID [16]byte
type Component uint8
const ( NATS Component = 1; Caddy Component = 2; HostSupport Component = 3 ) // 3仅root support/policy，不是公开CLI组件
type Phase uint8
const (
    Prepared Phase = 1; Staged Phase = 2; Ready Phase = 3
    Moving Phase = 4; Cutting Phase = 5; Verifying Phase = 6
    Returning Phase = 7; Retiring Phase = 8; Completed Phase = 9
    Deferred Phase = 10; Halted Phase = 11
)
type Action uint8
const ( Upgrade Action = 1; Rollback Action = 2; Reload Action = 3; Recover Action = 4 )
type Process struct {
    BootID string; PID int32; StartTicks uint64; InvocationID string
    ExeDevice uint64; ExeInode uint64; ExeSHA256 Digest; Cgroup string
}
type Socket struct {
    NetNS uint64; Device uint64; Inode uint64; Cookie uint64
    Local string; Remote string; Owner Process
}
type Attachment struct {
    Participant ID; Generation uint64; ServerID string; CID uint64
    URL string; Raw Socket; Ingress Process; HealthGeneration uint64
}
type Store struct { Device uint64; Inode uint64; MountID uint64; LockDevice uint64; LockInode uint64 }
type ArtifactKind uint8
const ( NATSBinary ArtifactKind = 1; CaddyBinary ArtifactKind = 2; HelperBinary ArtifactKind = 3; LauncherBinary ArtifactKind = 4 )
type Artifact struct {
    Kind ArtifactKind; Version string; GOOS string; GOARCH string
    ArchiveSHA256 Digest; BinarySHA256 Digest; CatalogSHA256 Digest
}
type Plan struct {
    Tx ID; Step uint32; Domain ID; Component Component; Action Action
    Candidate Artifact; Previous Artifact; Before Process
    ConfigSHA256 Digest; Store Store; PolicyRevision uint64
}
type Census struct {
    Complete bool; Generation uint64; Processes []Process; Sockets []Socket
    LiveBusiness uint32; OldOwnerPending uint32; Digest Digest
}
type Status struct {
    Tx ID; Step uint32; Phase Phase; Revision uint64; Code string; Detail string
    Before Process; Current Process; Census Census; Proof Digest
}
type AssetSeal struct { Revision uint64; Accounts Digest; Assets Digest; Participants Digest; Complete bool }
type Readiness struct {
    Topology Digest; Seal AssetSeal; Auth Digest; WSS Digest; Running Process
    MetaLeader string; Groups Digest; Complete bool
}
type RoutePurpose uint8
const ( RoutePrepare RoutePurpose = 1; RouteBind RoutePurpose = 2 )
type RouteDirection uint8
const ( RouteOutbound RouteDirection = 1; RouteReturn RouteDirection = 2 )
type RouteTarget struct {
    URL string; NodeID string; Component Component
    Before Process; Candidate Artifact; Ingress Process
}
type MoveResult struct { Census Census; Attachments []Attachment }
type RouteProof struct {
    Purpose RoutePurpose; Direction RouteDirection; Target RouteTarget
    Version uint16; Tx ID; Participant ID; Old Attachment; New Attachment
    HomeDigest Digest; AuthorityRevision uint64; PolicyRevision uint64
    RemainingNS uint64; Nonce ID; KeyID string; Signature []byte
}
type TransferSeal struct {
    Version uint16; SourceSHA256 Digest; SchemaSHA256 Digest
    InvocationSet Digest; ReceiptSet Digest; ConsumerSet Digest; Complete bool
}
type Checkpoint struct { Cohort ID; Assets AssetSeal; Transfers TransferSeal; Proof Digest }
type Observation struct {
    Version uint16; Tx ID; Step uint32; Kind string; BootID string; BootNS int64
    Subject ID; Generation uint64; Evidence Digest
}

// internal/infracontract/host.go
type HostOp uint8
const (
    Inspect HostOp = 1; Stage HostOp = 2; FenceStart HostOp = 3
    Promote HostOp = 4; SignalLDM HostOp = 5; SignalStop HostOp = 6
    SignalGraceful HostOp = 7; StartWarm HostOp = 8; SwitchIngress HostOp = 9
    FinalizeStorage HostOp = 10; ReleaseStart HostOp = 11; RetireWarm HostOp = 12
    RecoverHost HostOp = 13; RotatePolicy HostOp = 14; UpdateSupport HostOp = 15
    LaunchClaim HostOp = 16; LaunchAck HostOp = 17
)
type HostRequest struct {
    Version uint16; Op HostOp; Component Component; Tx ID; Step uint32; Domain ID; Sequence uint64
    PolicyRevision uint64; KeyID string; HostBootID string; Nonce ID
    Expected Process; Candidate Artifact; Store Store
    ExpectedJournal Digest; Config Digest; BeforeCensus Digest
    WarmCensus Digest; ReturnProof Digest; Payload Digest
    RemainingNS uint64; Signature []byte
}
type HostReceipt struct {
    Tx ID; Sequence uint64; Revision uint64; Op HostOp; Applied bool
    RequestDigest Digest; Journal Digest; Current Process; Census Census; Code string
}
type Peer struct { PID int32; UID uint32; GID uint32; BootID string; StartTicks uint64 }
func EncodeRequest(HostRequest) ([]byte, error)
func DecodeRequest([]byte) (HostRequest, error)
func SigningBytes(HostRequest) ([]byte, error)
func VerifyRequest(HostRequest, []byte) error

// internal/infracontract/interfaces.go
var (
    ErrContractStub = errors.New("infra_contract_stub")
    ErrUnsupported = errors.New("infra_unsupported")
    ErrIncomplete = errors.New("infra_inventory_incomplete")
)
type Host interface {
    Do(HostRequest, Deadline, Stop) (HostReceipt, error)
    Inspect(ID, Deadline) (Census, error)
}
type HostBindings [18]Host // index=HostOp，0禁用；实际表必须完整且不可重注册
type HostFactory func(HostBindings) (Host, error)
type Artifacts interface {
    Resolve(ArtifactKind, string, string, string) (Artifact, error)
    Verify(Artifact, Deadline, Stop) error
    Compatible(Artifact, Artifact, Digest, Store, Deadline) error
}
type ReadyProbe interface { Read(Plan, Deadline, Stop) (Readiness, error) }
type Cohort interface {
    Seal(Plan, Deadline) (Checkpoint, error)
    Install(Checkpoint, RouteProof, Deadline) error
    Move(Checkpoint, Deadline, Stop) (MoveResult, error)
    Return(Checkpoint, Deadline, Stop) (MoveResult, error)
    Bind(Checkpoint, []RouteProof, Deadline) (Census, error)
    Release(Checkpoint, Deadline) error
}
type Routes interface {
    Prepare(Plan, Checkpoint, RouteDirection, Deadline) ([]RouteProof, error)
    Bind(Plan, MoveResult, RouteDirection, Deadline) ([]RouteProof, error)
}
type Transfers interface {
    Fence(ID, Deadline) (TransferSeal, error)
    Rebind(TransferSeal, Deadline) error
    Release(TransferSeal, Deadline) error
}
type Recovery interface {
    Seal(Plan, Deadline) (Checkpoint, error)
    VerifyBeforeReplay(Checkpoint, Deadline, Stop) error
    Release(Checkpoint, Deadline) error
}
type Transaction interface {
    Acquire(Plan, Deadline) (Status, error)
    CompareAndSwap(Status, Phase, Digest, Deadline) (Status, error)
    Load(ID, Deadline) (Status, error)
}
type Observer interface { Record(Observation) error }
type Dependencies struct {
    Host Host; Artifacts Artifacts; Ready ReadyProbe; Cohort Cohort; Routes Routes
    Transfers Transfers; Recovery Recovery; Tx Transaction; Observe Observer
}
type Driver interface {
    Advance(Plan, Deadline, Stop) (Status, error)
    Recover(Plan, Deadline, Stop) (Status, error)
}
type DriverFactory func(Dependencies) (Driver, error)
type Service interface {
    Start(Plan, Deadline, Stop) (Status, error)
    Resume(ID, Deadline, Stop) (Status, error)
    Status(ID, Deadline) (Status, error)
}
type ServiceFactory func(Dependencies, Driver, Driver) (Service, error)
~~~

公开Plan.Component只许NATS/Caddy；HostSupport只供root的policy/support/host census操作，不能经CLI触发whole-host更新。只读初始Inspect和Stage允许尚无Expected进程；所有作用于既有进程的mutation必须有完整Expected。按HostOp的required/forbidden字段表固定在P00 codec测试，零值不代表已退出。

root factory另冻为 `infrahost.NewWorker(HostBindings) (Host,error)`；P40负责最终可执行组装，P04核心不import具体nft/systemd/ACME实现，漏任一required backend只返回ErrContractStub。具体factory冻结：`infraupgrade.NewNATS/NewCaddy(Dependencies) (Driver,error)`，`infraupgrade.NewService(Dependencies,Driver,Driver) (Service,error)`；各实现加相应 `var _ infracontract.DriverFactory = NewNATS` 等编译断言。P00的 `contractstub.NewDependencies() Dependencies`、`NewDriver(Dependencies) (Driver,error)`、`NewService(Dependencies,Driver,Driver) (Service,error)` 永久独立，全部动作返回ErrContractStub、零文件/进程/goroutine副作用。生产传递import stub即门红；nil factory/未接S7/S8不广告capability，不可激活或completed。后包不覆盖P00文件，API修改须P00单一owner改同份合同并重验consumer。

跨主机只传剩余预算；接收方扣排队/往返，不比较不同host的BOOTTIME。HostRequest被root接纳时根据信任socket上的本机时钟生成**不可延长**deadline，重复请求不能重盖时钟。每个PID操作先持pidfd并复验BootID/startticks/invocation；Process中的任一未知身份导致拒绝，空集合不是“已退出”。

### 2.2 codec、资源与签名

根请求最大64KiB、单控制连接一请求一回复、64个census分页、每页≤64KiB、整份≤4MiB；超限/丢页/重复冲突页返回incomplete，不截断后Complete=true。codec为严格JSON struct固定字段顺序，拒重复key/unknown字段/尾字节/非规范UTF-8/负数溢出；签名输入为 `tether-infra-host-v1\n` + **所有字段（Signature清空）的规范bytes**。接收方重编码相等才验证，不签若干选中字段。Payload只引用root已经验证、不可变的catalog/config/storage记录，不是任意shell输入。请求、回执、rootjournal、route proof各自version/purpose，不能交叉使用。

rootjournal持久字段覆盖host/domain/Tx/step、holder/key/policy revision、请求摘要/nonce/sequence、每个实际副作用前后、before/expected-after identities、active/prev/pending slots、启动围栏、warm/redirect/current ACME selector、支持制品代际、retirement proof；先fsync intent，再副作用，再fsync结果。丢回复按同digest/sequence查询并返回同receipt；不同digest重用序号拒绝。无法判定effect就Halted，不重发对继任有害的signal/unflip。root授权是当前domain CAS与有效policy的合取，不信broker自己宣称持锁。

public wire/marker/status/log只含摘要、计数、脱敏身份；不包含seed、JWT、ACME account key、served key、cert私有目录快照、原始私网tuple。完整物理trace按既有evidence保密目录保存，分享版投影。秘密canary检测raw/hex/base64及fmt/error路径；禁止靠字段名叫不叫secret判保密。

### 2.3 三个完成点

`files_installed`只签磁盘；`runtime_ready`签新进程、真实auth/TLS/JS能力；`Completed`必须再签**本步before集合旧责任全退 + temporary全退 + return proof + root/S8 journal一致**。支持正常的live_business_count>0，必须old_owner_pending=0；不等长exec/PTY/transfer自然结束。

比较以每步 `Tx/Step/BootID/PID/startticks/InvocationID/exe inode+SHA/listener/socket` 为键。回滚目标合法地等于最初SHA，不能“所有oldSHA都必须消失”。launcher原地exec后PID/startticks仍相同，证明旧launcher映像消失；NATS/Caddy换代则证明旧server进程退出。枚举未知/权限不足/异常空census使完成拒绝。每一步重新观察，不复用上一跳OldExited或从产品Status直接抄观察字段。

P00 的 `Observation.Kind` 封闭词表为 `artifact_verified, asset_sealed, successor_installed, attachment_changed, auth_usable, business_ready, signal_applied, old_exit, storage_finalized, startup_released, return_verified, owner_retired, completed, halted`；未知值、空生产者与仅状态投影均不能签 observation。每项均须有实际 emitter，P35反查死项。


## §3 制品、权限身份与全资产 readiness

### 3.1 唯一允许表与运行映像

本次只读验证的linux/amd64数据如下；archive来自官方release，二进制SHA由内存中读取唯一tar regular-file member计算。它们是**内容身份收据**，没有运行 `--version/-t/adapt/validate`，不能签兼容或停顿。

| artifact | archive SHA256 | binary SHA256 |
|---|---|---|
| nats-server-v2.14.5-linux-amd64.tar.gz | `5e3b603d47c447bda1f77f9ac16dbf91c90aac4ff3681f8fbbc7201e4ed99355` | `e1a2f9ba25077f4cf753bee829483bd68fbf0a4eec9b6645a1e5785a6de0c0d1` |
| nats-server-v2.14.6-linux-amd64.tar.gz | `61c3d55f69f61ec616b75782250936445f2819e9e5f2ae6159b10a31abd2200c` | `2d0dc83e0547185a6820377114215d6a7c645f607aeb31728fffaf3d84e77c10` |
| caddy_2.7.5_linux_amd64.tar.gz | `13653e34d194d6737268e391c40a72a5d55e9f741e2ebbeebfa675e91a27223a` | `f7706787b843d375d1326cc451d53f48900b7a14f3a9c606d49d7751b390609f` |
| caddy_2.7.6_linux_amd64.tar.gz | `99587cf77c595f0bf62cc23c9ea101f9879fa016c7b689d498ce054904821f22` | `db3bfc85bb93160f60fa6df9c3ebf2340dc11740e9a52c717d88a14c0430f229` |

官方校验清单：[NATS2.14.5](https://github.com/nats-io/nats-server/releases/download/v2.14.5/SHA256SUMS)、[NATS2.14.6](https://github.com/nats-io/nats-server/releases/download/v2.14.6/SHA256SUMS)、[Caddy2.7.5](https://github.com/caddyserver/caddy/releases/download/v2.7.5/caddy_2.7.5_checksums.txt)、[Caddy2.7.6](https://github.com/caddyserver/caddy/releases/download/v2.7.6/caddy_2.7.6_checksums.txt)。清单本身SHA256依次为 `2e842f2670eb0eed2c65f2a58ac3f0c439256d607e9654ebd6999859c18ed0e0`、`cc6e36a66bd91aead06d5a1bfcf49ea90354917eb35ae44319b92925279d4523`、`a2667595ce868bfee693413f6b4f1e463cc8d856a99a743b7dbc1b06dae020c8`、`0ca6538623d93747a104ce7c135c7f5261f08c155cd80187c2918d390e14830d`。Caddy官方使用SHA512，catalog同时保存原算法/原值，不能拿重新计算的archive SHA256代官方核验：
- 2.7.5：`4afdf50ccf3a8f32344dbac46006ca2b5d90c2ef656c53e8617f1c3b81c5f9e44bd3a9e0b62975f73c85451c354d3d9b5292f5247d18a62d95ab19c8b0a5dba7`。
- 2.7.6：`b74311ec8263f30f6d36e5c8be151e8bc092b377789a55300d5671238b9043de5bd6db2bcefae32aa1e6fe94c47bbf02982c44a7871e5777b2596fdb20907cbf`。

`scripts/install.sh` 的NATS/Caddy pin仍是版本SSOT；catalog读取并核对，不另写漂移默认。NATS新pin同时与go.mod、Makefile一致；Caddy终态2.7.6；每个cycle最后回新pin。arm64只在取得同等archive/binary/兼容收据后加入catalog，不拿amd64收据授权另一平台。Darwin拒特权infra动作，保普通CLI。

普通用户下载→检查官方allowlist/唯一checksum/大小/ELF平台/tar traversal/symlink/device→root私有dirfd创建immutable slot→复制完成后**重新hash最终bytes**→fsync文件及目录→去写引用→原子选择。只验caller FD不安全，SCM_RIGHTS只可作为输入；可写同inode/祖先替换不能改变已验证slot。可信官方候选的 `--version/-t/adapt/validate` 在非root服务UID和隔离运行目录、无live ACME/admin环境下执行，全部经监督器。此UID能读业务seeds，**不是沙箱**；任意第三方插件/任意URL执行不受本catalog授权。

两版NATS必须真证当前非空store的新代写入→旧代读回→再升级，包含stream/consumer/OBJ metadata、S7receipt和审计；Caddy两版必须真证同config/schema/当前ACME树的双向兼容。不能拿空store smoke、同源改version字符串或某release能启动代签。

### 3.2 三种拓扑、分页和资产封口

`B`为tether Raft voter配置，`S`为实际NATS server集合，`J`为JS meta及逐stream/consumer实际placement。readiness保存三份来源与hash。N3不是 `len(brokers)==3`；每个待停host从每个组的实际成员集合移除，剩余**不同host**上的current、online、lag=0成员须达到该组configured majority。meta leader须在目标外，实际leader与成员身份来自独立响应；目标上的R1拒绝，目标外R1可保留；目标参与R2通常无剩余多数，拒绝；不存在/unknown/无leader的consumer也拒绝。

枚举所有启用JS的账号、stream分页、consumer分页、每组成员及placement。SYS只负责账号/服务器层只读信息，$G broker权限读业务组；额外账号没有授权读取则 `infra_inventory_incomplete`，不能悄悄跳过。请求过期、分页重复/缺失、游标不进、account list不完整、资产删除重建、未知server/同host伪多副本均fail-closed；给asset附账号、name、创建身份/配置digest、stream/consumer组身份，不仅名字。

`AssetSeal` 的producer是P18管理的真实JS变更入口；broker cold ensure、topology reconcile、history/events创建、S7 bucket coordinator、consumer创建/删除、replica raise、GC全部登记。P22接全现有producer，P19接S7；P34逆向扫实际Create/Update/Delete/Ensure/Purge emitter验证集合。先预读库存；临cut在同domain下短暂关配置准入、join已发请求、再完整读回seal，窗口计入5s，不把全部120s变成新业务停机。sent-unknown的配置请求未获retirement不能seal。新consumer/dataNC须先注册cohort；latejoin转向survivor并触发generation复核。切后开放配置只能选择已ready surviving placement。

受支持部署的配置writer必须经过产品门；未经登记的operator/外部账号配置写入不在热切并发支持集合，检测到即defer，文档要求避开事务。不能从“两次相同LIST”声称能排除任意恶意外部writer。foreign asset可只读保留并按实际placement拒绝，绝不自动raise/recreate/delete；缺观测权限是阻断理由。dry-run只读，连副本/凭据也不安装。

### 3.3 SYS principal与真实ready

默认部署已有内部$SYS；P12只在这个同名账号加入两枚独立user nkey，不搬$G、不改store布局、不切SystemAccount、不热加monitor。读身份 `infra-sys-read.nk` 仅授确切 `$SYS.REQ.SERVER.*.JSZ`、`$SYS.REQ.SERVER.*.VARZ`、`$SYS.REQ.ACCOUNT.*.INFO` 所需只读集合及自己的私有reply root；是否实际使用某subject以实现descriptor为准，没调用的授权不得保留。动作身份 `infra-sys-step.nk` 只额外授 `$JS.API.META.LEADER.STEPDOWN` 及自己的reply；不授 `$JS.API.>`、purge/delete、account变更、任意SYS发布。枚举协议选择及真实server响应分别用两版测试钉住。

root bootstrap生成并安装seeds，root:tether 0640、父目录root所有且不可由tether替换；只给负责查询/stepdown的非rootadapter读取，不给roothelper链接NATS。重跑不重新生成；丢失/坏mode拒绝，显式rotate才换。natsconf Parse→Owned→Render→BuildMergedConf→RenderDesired→reconcile保持$SYS block与最窄permissions，未知operator accounts/options照旧拒绝。保持顶层authorization及$G现状；用stock两版reload验证不触发server重启、原流/连接/认证仍活。若该精确reload路径在任一版失败，P12仍未完成，不能退成“fixture预置SYS”或“先重启一次”；需回到lead报告上位所需迁入路径尚缺，不能签E3。

meta stepdown实请求必须由当前leader处理并观察新leader在off-host；拒绝/503/丢reply后读实际状态，不把nil effect视成功。broker静态nkey、member、agent三类普通身份请求必须失败。default single的ready不依赖meta stepdown。

ready还要求目标/存活入口各自真实activated member及provisioned agent完成CONNECT/PONG、私有inbox和允许的真实RPC；正常auth queue responder记录同serverID/connection generation/request digest。不新增特殊probe responder；本机与survivor正常queue分别验证，断route防peer掩盖本机遗漏。WSS握手校验SNI/pins/HTTP101/NATS INFO/实际cid；`varz.version`与本地`/proc/exe`相互核对，但没有monitor的旧实例用INFO+本地进程+SYS VARZ请求形成等价来源，不能以磁盘version代运行。readiness报告由broker admin包级函数和sanitized ClusterHealth字段投影。

## §4 NATS 产品切换、业务恢复与锁

### 4.1 lifetime store所有权

固定root-owned锁目录 `/var/lib/tether-infra/locks`，key由canonical store实际dev/ino/mount身份形成；目录与锁inode永久，uninstall/cleanup不unlink活动或历史锁。alias/symlink/bind mount必须归同store domain；不能仅hash路径。root journal另持upgrade-domain lock，不与store lifetime lock混称。

launcher也负责Caddy的启动围栏/当前storage selector校验，stock Caddy运行后同样无驻留launcher映像；不导入NATS SDK。新NATS unit经 `tether-infra-launch` 验unit/catalog/start fence后**原地exec** stock nats-server；同一non-CLOEXEC flock OFD进入server，直到实际server退出才释放。launcher无长期parent，无旧Go映像常驻；无关child不得继承该FD。两版真stock必须验证FD未被关闭、父worker退出/被杀不释放最后引用、第二launcher无法开store。若stock关闭FD，此方案尚未验收，不能偷偷切parent模型；替代方案须显式证明child也持锁及旧guardian退场后再冻结合同。

legacy direct NATS没有锁：首次retrofit在同root域枚举全部进程/打开store责任，并取lifetime锁作为外部guard；安装后所有受支持launch入口都检查legacy incarnation。旧owner仍活时禁止启动第二server，不能把“我已flock成功”当无旧owner。guard锁仅在旧pidfd退出已证之后交给唯一launcher（SCM_RIGHTS传同OFD，sender保留至receiver ACK），不经close→重新lock窗口；rootworker死亡导致启动围栏继续闭合，recover重新确认旧owner，不并开。任意恶意root直接运行裸binary不在权限威胁模型，但产品service/CLI/recover/bootstrap/reinstall不能绕门。T14/T15覆盖全部入口、父死、锁inode被换、继承泄漏、legacy仍活、同目录第二启动。

### 4.2 N≥3逐节点算法

每节点独立120s组件deadline，滚动顺序与B的leader-last和当前健康门相容；不靠固定sleep等待选举。每步：

1. Acquire S8域及root CAS，保存完整before；stage immutable candidate/actual prev、两向兼容及 `--version/-t`。配置不得迁store/账号/cluster名。
2. 验B/S/J真实状态，meta leader必要时窄权限stepdown到off-host；完成每组surviving quorum/current、普通认证、survivor WSS。任何不可逆动作前失败均defer，旧server继续服务。
3. 封JS资产与NC cohort；S7 writers/readers真实checkpoint、待ACK责任固定；安装signed successor与route bridge并取得每个affected control/data NC的Installed ACK。N3优先把可迁client移到survivor，真实socket/cid+业务进展证明Attached；新加入者必须登记且只能去survivor。broker本机NC不漫游。
4. 再核seal，提交root expected-incarnation的LDM动作（pidfd发SIGUSR2，语义等stock `--signal ldm`；不用存在PID重用窗的裸kill）。记录真正LDM观测，确认迁移收尾、JS健康survivor与transfer进展。尚依赖旧NC第一份hint/未checkpoint者绝不进入本步。
5. 取最终旧责任集合；pidfd SIGKILL（stock `--signal stop`）只命中本步旧server。LDM可能已经自行退出：观察旧pidfd后跳过signal，不能对新MainPID补发。pre-LDM已把唯一candidate selector/start fence写稳，always/2s通过launcher起继任；store锁连续移交。
6. 新server实际exe SHA/version、同store、原JS资产身份/current逐个读回。产品pre-repair门先验cut前ACK multiset，再放audit/transfer/reconcile；普通auth-first完成，客户端按策略回本地/原canonical，观察新control/data NC实际generation。
7. 后置全组current、普通业务、oldserver exit、support/warm零旧责任，S8与rootjournal一致，才completed及推进下一节点。整个过程默认reaper/roster/home CAS正常运行。

LDM之后任何失败→Halted，**不再声称旧服务活着**，不推进下一host。观察并保留已证healthy survivor；允许从当前store切回actual prev，重走before/expected-after，不restore旧JS备份。若survivor也不健康，记录实际失效；禁止伪造“安全回滚成功”。每个动作前/后及丢reply都有恢复表，见§7。

### 4.3 两个N1：真实默认配置、同boot ACK与pre-repair

85.A是真default single，85.A1只增加tether sole voter而JS仍standalone。旧版2.14.5由原installer形态建立非空$G JS，原审计与4条transfer已在进展；无monitor、无fixture加sync always、无额外restart。产品安装权限、stage文件不算业务切换，也不得预先完成首次切换。

主路径固定SIGKILL + Restart=always RestartSec=2，不LDM、不临时NATS/sibling、不改cluster{}、不reset store。上游当前R1默认flush-in-place（`NS/server/filestore.go:473–474,771–803,5336–5355`）提供可验证的same-boot进程重启前提，**不是电源失效fsync保证**。两版FileStorage/default persist的实际ACK-before-process-kill路径须T60读回；MemoryStorage、AsyncPersistMode、非支持持久设置拒绝，不能通过热改SyncAlways假装合格。

P18 `Recovery.Seal` 固定每项已ACK `account/stream/subject/NUID/idx/sequence/header+payload digest`、reader已Sync前缀、consumer delivered/ack floor、audit START/TERMINAL canonical receipt、publisher cursor与S7未决attempt。切前关并join所有可修复这些资料的producer，保留持久责任；重启后**最先**以只读管理连接逐项读取原store。校验完成前，普通broker reconnect/cold repair、S7writer、audit publisher、Ensure/reconcile/rederive/GC都受产品gate约束。不能先补齐再读，也不能collector自行暂停业务代gate。

验证期间保原任务/FD/NUID；JS503只使worker在同checkpoint有界等待，不新Put/新NUID，不报假failed。通过后释放同generation，原reader继续、未决ACK按S7协议解析。inject删除一个已ACKchunk再让正常replay可补的mutant，必须在补之前被发现。全局LastSeq不下降/GetInfo NUID不变/最终文件SHA正确均不足。

从固定slot计划时间到完成的所有新请求≤5s，RestartSec=2包含在内；每条健康transfer最大持久进展空窗≤10s，均计故障首尾。原对象NUID、原ctl PID/invocation、source/tmp/parent OFD、exact审计均保持。首个支持lane不能在data warmup结束后才切。若真实N1超5s/10s，E3失败；只有随后另做无损standalone→clustered spike才可提出sibling补案，未过spike不改主路径、不把defer当成功。

### 4.4 ordinary auth-first生产路径

P17引入独立auth-only NC与business NC。auth-only承载正常 `tether-authcallout` queue及认证所需最小authority responder集合；business NC拨号/重绑定受auth/recovery generation gate约束。所有broker NC仍local-only/IgnoreDiscoveredServers，普通client可使用survivor。冷启动、S6 hot restore、planned cut、普通NATS重连用同一binding descriptor，不保留一条“只在image时正确”的旁路。

断开即关business admission；auth-only重新接本地、真实queue SUB+Flush/PONG、正常activated member/agent CONNECT成功后记auth_usable。business NC直到这时才允许建立或自动重发业务SUB；同代minimal authority服务先于可能依赖它的认证，不能形成“先业务RPC才能证明auth、先auth才能挂RPC”的环。随后重绑业务表、通过真实private-inbox RPC记control_ready并开放请求。S6临时observer在普通ready探针之前已经退出；删normalqueue、坏issuer/ACL、断route、本机queue缺而peer好各有独立负控。auth_usable/control_ready记录发送端BOOTTIME和实际generation，不是后置日志排序。

### 4.5 有界planned transport与cohort

P14在NATS CustomDialer之下拥有每次dial的可取消DNS resolver、TCP/proxy握手、TLS和WSS原raw socket。SetDeadline覆盖实际handshake/write，取消发生时黏性禁止旧generation后续dial，并Close该代raw连接；然后才等待持nc.mu的ForceReconnect/SetServerPool/Close worker退出。所有新dial在接入registry前验代际，不能晚到复活旧pool。代理DNS/CONNECT/SOCKS路径同样可取消，不能将ignore-ctx的inner.Dial包一层就宣称有界。

正常Move总预算≤5s，不调用旧20s redial或10+10 escalation来凑；不得self-exec丢业务、不得放弃worker。无法证明原dial可打断的legacy/third-party dialer在signal前defer。P23将这个owner接到ctl、agent control、S7独立transfer NC；普通NATS会话ctx结束只换transport，绝不Close SS/flow/exec/PTY/source FD。

cohort key=`participant identity + raw attachment generation`；能力与Installed ACK从真实正常REGISTER/连接创建入口安装，不从farewell或布尔配置猜。每个dataNC/consumer创建先登记，seal期间晚加入转向successor并增加revision；未知NC/丢prepare ACK不signal。ACK须由该NC实际新CONNECT取得cid，再PONG/真实请求、独立物理入口归属核验；不能通过另一条NC或转发mailbox替它确认。两次Move/Return都重取census、锁定generation，旧ACK不继承。

### 4.6 S9路由证明与预算

独立purpose `infra-route-v1`，分**Prepare与Bind两种签名消息**，不能在切换前虚构未来cid。Prepare签domain/Tx/participant、实际Old attachment、受限RouteTarget（URL/node/component、已运行survivor/warm的Ingress identity，或N1当前服务的Before+已验candidate）、agent instance/home/token/epoch摘要、authority/policy revision、nonce及remaining；此时New必须全零。P15安装/ACK只证明successor意图已接纳，不证明迁移已完成。

`Cohort.Move/Return` 返回真实MoveResult，里面的每个Attachment由raw socket/cid/serverID/实际accepted ingress重新取证；P16 `Routes.Bind` 据该结果签完整Old→New，P15 `Cohort.Bind` 核验同prepare nonce/目标/participants、正常REGISTER/heartbeat及原home基线后才给migration ACK。N1尚未启动的继任只能在root实际runtime receipt和ordinary auth均成立后签Bind，Prepare不能把candidate磁盘身份当服务事实。先后固定为Routes.Prepare→Install/全部ACK→Move→Routes.Bind→Cohort.Bind；返程同理。丢Bind reply查询同代实际receipt，不再次重连铸新generation。

准备阶段仅保护仍匹配Old的owner；迁移中的短 `pending-target` 由S9专用bridge记录真实dial generation+受限Target，允许一次已授权转换，并在Bind后替换为完整New证据。它不是S6旧ACK对新route继续有效，不能匹配其它URL/host/worker或未知来源；任何失败到期恢复正常reaper。S6 maintenance bytes和严格函数完全不放松；迁移期间正常roster/home CAS继续，CAS已变就拒Bind，不能压住合法撤权。

第一次Install形成≤8s绝对到期，Prepare/Bind/重试/换generation/回消息均引用同nonce并夹紧原deadline，不能续8s；返程是已有正常稳定阶段、真实新准备的另一步，不连续自动重发把120s变成维护豁免。过期、撤权、home CAS变化或未知source立即停止保护。T36/T37/T69覆盖Prepare假填未来CID、Bind改raw/health、pending-target晚到dial、迁前/迁后/8s之后及60s Offline门之后的真实reaper与home CAS；同时证正常不误迁和故障不被永久遮蔽。

## §5 Caddy：reload、binary、root首迁与当前ACME

### 5.1 支持配置与权限

支持官方2.7.5/2.7.6、tether安装器路由、filesystem ACME storage、TLS over TCP的HTTP1/2与NATS WSS。保完整route顺序：`/sub/*`、well-known/manifest及其他具体匹配先于WSS catch-all；不简单复制成一个reverse_proxy丢功能。shadow渲染相同host/SNI/pins/upstream/路由，端口固定policy允许的8443，admin为per-invocation Unix socket，使用当前证书只读快照，自动HTTPS/ACME writer关闭。validate不对live目录申请/续期、不bind生产listener。

新main/admin位于tether可访问的0700 runtime父目录，socket0600（仅确需独立group时0660且显式root policy），client验SO_PEERCRED并固定已连接socket，不自动重拨。同UID是可信产品域，不承诺隔离持有同UID/account seed的恶意代码；普通member、其他本机UID、同UID越catalog分别测试。旧root的2019即使仍listen，也由root固定规则只准root本地管理流量，禁止任意UID绕helper POST /load读取canary seed。旧root不接caller config，唯一退休动作是pidfd SIGTERM；信号是graceful，不是SIGKILL。新Unix admin /load同一已验证invocation连接，丢reply先query配置digest，不能无条件重放。

未知custom module、外部ACME storage、无法枚举的其他入口/长请求、额外root Exec hooks在动作前拒绝；本稿不承诺任意HTTP3/QUIC或任意第三方HTTP服务原连接续接。旧stock可能有空闲UDP监听器，记录其关闭；有活动不受支持协议时不给热切成功资格。此支持面是§开放问题Q2的产品裁决，不允许实施时自行扩大或把强杀称graceful。

### 5.2 canonical连续与实际全连接封口

先预热shadow，证明实际shadow PID/exe、TLS/WSS/member/agent、/sub和well-known。P07由roothelper原子提交固定nft表的v4/v6 PREROUTING与OUTPUT跳转，新canonical443 TCP流转向shadow8443；覆盖外部、同机、容器hairpin与本机域名解析路径，保留已有conntrack流。管理规则只改自有table/handle，不flush宿主规则；检查既有drop/reject与IPv6支持。必须在产品bootstrap做环境检测/安装自有规则，fixture只能提供标准nft/kernel/IPv6环境，不能自己开8443/改规则填产品缺口。

new main探针使用root限制的SO_MARK bypass，不能靠临时删全局规则；accepted socket→listener inode→PID/startticks/cgroup→runningSHA独立证明命中main。main与shadow证书、NATS ServerID相同仍能区分。负控main坏/shadow好时canonical可绿、main-ready必须红。

redirect只挡新TCP，**不能证明旧keepalive上不会late Upgrade**。因此产品封口以完整kernel集合为准：提交规则后纳入旧main全部accepted TCP、TLS未完成连接、HTTP/2/普通/sub response、WSS、LISTEN backlog/SYN_RECV及指向旧入口的未完成conntrack。所有已知product NC迁移并关闭原内部socket；旧HTTP请求自然完成、旧keepalive关闭，持续新raw探针每次新TCP并主动正常关闭。直到封口后旧入口的全部既存可升级/可服务TCP责任归零，才允许stop；不能只读WSS计数，不能把两次瞬时0当封口。root旧主禁止load时也用同一全TCP封口，不声称nft是L7门。

未知/慢HTTP责任无法在本步有限预算内退净，则**在stop前**defer/halt并保已证健康入口；不主动截断未完成HTTP来凑零。支持正臂必须包含有限慢/sub、HTTP2、idle keepalive的退出与lateUpgrade竞态，并在120s内真退出旧主。idle正臂的客户端按启动前固定的有限工作负载自然完成并关闭，不由fixture在barrier临时强关；永久keepalive另作诚实defer负例，不能假定stock idle timeout短于120s。旧进程默认grace=0不构成无限等待，因为SIGTERM前所有可阻塞的服务连接已实际归零。新main/shadow配置固定 `shutdown_delay=0`、有界grace（5s），但不能依赖grace到期强断业务；upgraded close-control堵塞同样要在stop前解开并join。返回切换对shadow执行完全相同的封口；剩余从canonical建立且由conntrack仍指shadow的WSS也必须迁回，不能删规则后假定它们已经走main。

canonical observer固定10次/s fresh TCP/TLS，禁TLS session reuse/预seedhint/只拨8443，每个attempt记录首尝试成功及耗时，任何ECONNREFUSED/握手失败粘性红。可另跑NATS WSS101/逻辑请求≤5s，但逻辑重试不擦raw失败。没有监听黑窗才签E4。

### 5.3 binary与首次root retrofit的顺序

1. 原root/on-failure进程、真实当前ACME树和原业务在READY前已经存在。之后**产品命令**显式bootstrap管理socket、验证effective unit；不由H0换UID/重启/搬最终storage。
2. root写新drop-in、launcher启动围栏、always/2s策略并daemon-reload；当前旧PID仍root。capability先空赋值清累加，再设唯一CAP_NET_BIND_SERVICE；User/Group/SupplementaryGroups、全部ExecCondition/StartPre/Post/Reload/Stop/Post、prefix、EnvironmentFile/Environment、路径/capbounding均读有效值核验，未知override拒绝，不只看模板或MainPID。
3. 从当前树读只读warm snapshot，预热shadow、redirect、迁出、全责任封口。旧writer仍可续期，warm不算最终态。
4. exact旧pidfd SIGTERM，等待旧进程退出及全部旧writer/lock释放；systemd自动restart即使已触发也被持久startup fence挡住，不能在2s后抢读旧copy。candidate已验证及selector预备，不代表允许start。
5. **old-exit之后**从最终当前ACME树做完整delta，包含account key、cert/key、metadata、目录、锁处理与mode/owner；trusted dirfd拒symlink越界，temp tree→fsync→selector CAS→fsync。只有证明原writer死亡的陈旧锁可处理，不能按年龄抢活锁。策略只迁入批准filesystem树，不将任意caller路径chown为tether。
6. 提交`FinalizeStorage` receipt与启动围栏revision，`ReleaseStart`才允许一个tether主writer启动。startup launcher有真实等待/取消和BootBudget，过期保fence/健康shadow，不自动跳过验证。main执行当前目录、实际新SHA、UID/caps、Unixadmin、TLS/WSS与普通认证readiness。
7. main bypass成功后原子切canonical新流回main；所有shadow已有NC真Return ACK，全责任归零后RetireWarm。旧主/旧配置generation/临时unit/旧helper worker退出，才completed。原无限业务及至少四在飞transfer仍未被fixture cleanup结束。

ACME测试在warm复制后、oldexit前通过真实本地测试CA续期/当前writer更新cert/account metadata，故意让final copy超过RestartSec=2；必须新main和回滚都加载B态，不能重新issue掩盖缺文件。test CA是供给，不是fixture代迁移；TLS static cert臂另列，不能代续期/锁收据。

### 5.4 reload、失败与临时入口自升级

reload不是binary升级：main PID/startticks/exeSHA保持，配置digest/generation真正变化，/load成功后旧配置generation责任实际退休。为防reload断WSS，先走同样warm→redirect→迁出→旧责任退净，再通过permissioned Unix admin加载已验证配置，然后main bypass→迁回→retire warm。坏config/cert在redirect/stop前发现，旧generation和业务不变；不以HTTP503挡lateUpgrade，因为NC将其折成 `invalid websocket connection`，可能是终态。

main启动失败、controller失联、reply丢失、final ACME复制失败、return失败都Halted并保最后健康入口；shadow**无RuntimeMaxSec**。控制worker有有界寿命，不等于杀服务。120s正常完成失败保持红收据，后续recover成功也不擦原超时。recover通过rootjournal和独立census选择安全下一步；不会“假定旧主活着”“无条件unflip”“拿旧ACME快照回退”。

temporary入口必须自己可升级：当main已独立健康且业务回main后，产品创建目标SHA的新warm incarnation并验证，退休旧warm，再做一次迁入/迁回/退休；support artifact也同样不能长期固定旧映像。正常completed不留监听、unit、FD、worker、旧映像或未交清迁移责任。长业务仍活时观察退场，fixture关任务后进程才退出不算成功。

## §6 权限、安装器与支持制品生命周期

### 6.1 固定root产品面

`tether-infra-helper` 独立main，不调用agentboot.Begin、不读业务handoff env、不链接broker/agent/NATS/infraupgrade orchestrator。root只做IPC/policy/immutable slots/journal/systemd/nft/pidfd/storage/launcher交接；业务readiness由非rootadapter提供且root重新验证属于自己的物理事实。清除继承env、关闭不在0/1/2+systemd socket activation+本次批准FD集合中的FD；没有shell、任意unit名、路径或argv动作。固定绝对路径调用nft/systemctl时argv由closed op构造，不执行caller文本。

socket activation `Accept=yes`、root服务每请求一个worker；用Socket MaxConnections、全局domain锁及应用内并发cap=4共同约束，read header/body≤2s、单host动作≤10s、完整worker≤15s，所有I/O包括D-Bus/flock支持取消且join。worker RuntimeMaxSec仅作为最后清理保障；必须实际验证半包/空包/锁竞争/D-Bus丢reply后无遗留。长ACME final同步分有journal checkpoint的有界动作，不占一个无限worker；业务shadow不受该15s kill。关闭时对实际启动的子进程组kill/reap，不能把exec成功等同读请求结束。

P04的journal锁只覆盖短CAS/intent/result，不跨D-Bus等待、socket回复或launcher握手持有；StartWarm/ReleaseStart提交固定的非阻塞systemd job（等价systemctl --no-block），记录job/permit后返回，后续Inspect验证运行状态。否则start job等待launcher的LaunchClaim，而Claim等待发起worker的锁会自锁。LaunchClaim/Ack仅能续同Tx/permit，不新取另一业务domain；T09/T13/T91必须真实命中该互相等待反例并证明无锁跨等待，不能用fake即时start规避。

### 6.2 可审计verb-effect表

| verb | 必要当前事实/票据 | 副作用与丢reply处理 |
|---|---|---|
| Inspect/Stage | policy有效、catalog/hash、受控FD | Inspect只读；Stage写root私有slot，重复digest幂等，不改active |
| FenceStart/Promote | 当前domain/sequence/CAS、expected旧incarnation、兼容报告 | durable启动围栏、active/prev selector；不启动；恢复先读journal和slot |
| SignalLDM/SignalStop/SignalGraceful | pidfd绑定expected、readiness/cohort/资产证明仍有效；op允许该component/phase | 只信号指定旧进程；old gone视已完成该effect，绝不重找PID再杀 |
| StartWarm/SwitchIngress | 验证warm bytes/config/只读key、完整物理warm ready与root policy的端口/mark | 创建固定transient，规则原子CAS；丢reply查实际unit/rule generation |
| FinalizeStorage/ReleaseStart | oldwriter真退出、final树hash/owner/fsync及expected journal | 当前树/selector durable后才解围栏；故意延迟不能偷跑 |
| RetireWarm/RecoverHost | ReturnProof、旧TCP/consumer/迁移责任0；或精确恢复phase | zero proof才停止warm；recover只执行当前journal许可下一步 |
| LaunchClaim/LaunchAck | 固定unit启动的launcher、SO_PEERCRED/cgroup/MainPID、local launch key、start fence与BootBudget | Claim经socket取得只读启动permit和同OFD锁；Ack绑定继任exec身份，root在ACK前保最后锁引用；不授任意路径启动 |
| RotatePolicy/UpdateSupport | 管理root权限、当前policy CAS、无相冲mutation | key/revision撤销旧票据；支持制品受同domain两代约束，busy只stage |

bootstrap是root手动显式管理入口 `/usr/local/libexec/tether-infra-helper bootstrap`，只认root且只使用root-owned本机policy，不开放给socket普通请求；初始化policy/journal/seeds、安装固定管理面、daemon-reload并启动管理socket，**不启动/重启NATS/Caddy/broker**。客户端未bootstrap报 `infra_not_enabled` 并给该具体指引。这是产品启用步骤，86.I在原业务live窗口调用；installer仍never-start，不靠fixture预先systemctl start helper。disable/masked/非systemd/不支持nft/错误权限均真实拒绝并保原业务。

### 6.3 凭据与轮换

| 资料 | owner/mode/用途 | 生命周期 |
|---|---|---|
| account public key、policy revision/key_id | root-owned policy，非秘密 | 根授权固定issuer；与账户轮换CAS更新，不从caller自报接受 |
| 既有account seed/broker nkey、route/tunnel key | 沿原root:tether管理，helper不读取业务seed | 不借新helper扩大读取者；rotation跟随已有流程并先失效旧infra票据 |
| SYS read/step user seeds | root:tether0640，只给指定非rootadapter | bootstrap一次、explicit rotate；重装不生新key，普通member不可取得 |
| local launch/infra签名seed（每host） | root:tether0640、与host/domain绑定 | launcher只获固定launch verbs；single另获本机infra管理域；不创建任意authority discovery；丢失fail-closed，root显式轮换 |
| ACME account/TLS keys、warm cert快照 | 主目录tether0700、文件0600；旧root目录直至退出 | warm只读受控副本，zero责任后删；final当前态唯一writer |
| rootjournal、lock、immutable slots | root私有写权限，journal0600，executable slot0755，lock inode永久 | 保current+prev及pending；不把秘密bytes放journal |

账户/CA/local authority轮换先关闭infra mutation准入、确认无不可判副作用，再更新root policy revision与服务凭据并实测ordinary auth。旧key签的新请求、旧未消费ticket、跨boot/旧revision票据全部拒；已pending事务仅允许read/query，必须root显式reconcile/re-authorize到新policy后继续，不能自动以旧票据recover变更。rotation与upgrade互斥；测试已签但未发、已发丢reply、旧key仍能签、policy部分更新各分支。

### 6.4 install.sh的五态与seam

安装状态严格 `files_installed → socket_active → helper_version_verified → effective_unit_verified → runtime_ready`；只有实际能力与全部guards接线后才广告InfraUpgradeVersion=1。fresh install只交文件/enable策略，不启动任何tether、helper、launcher、NATS、Caddy、nft动作或keygen。rootbootstrap交管理启用，后续driver交runtime_ready；launcher通过socket的LaunchClaim读取root生成的只读启动permit/接收锁FD，不直接读取0600 rootjournal或依赖tether可写selector；普通reinstall在active host默认stage-only，和retrofit/promote/uninstall同一upgrade-domain/policy epoch互斥；能力未知/事务busy不能覆盖active/prev/unit选择器。

`TETHER_INSTALL_ROOT`、`--dry-run`、`--skip-download` 全对称覆盖新libexec/socket/service/drop-in/catalog/policy/journal/lock/secret/warm/staging路径以及D-Bus/socket/nft/keygen/执行候选等host副作用。dry-run连测试根创建、chmod/chown、时间戳变化都不允许。skip-download只免下载，不免helper/launcher存在、hash/platform/权限与必要文件验证，不能假成功。uninstall只删有内容ownership marker且当前domain允许退休的自有文件；operator同名文件保留，busy先拒绝；永久锁inode不随普通cleanup重建。

32的content+metadata manifest覆盖所有新增根和数值uid/gid/type/mode/size/hash/link-target；never-start从仅pgrep tether扩成全部支持binary/unit/cgroup/process及网络动作集合。保现有ctl/agent真实archive下载/安装/卸载和manual cold broker升级旧臂，不挪作S9热升级证明。

### 6.5 helper/launcher的build→运行退场

P26在真实Makefile/goreleaser添加两个Linux supporting executable和平台归档清单，同release archive/SHA256SUMS交付；installer真实提取/校验/immutable stage，不只Docker COPY。主tether既有Linux/Darwin产物不缩减。每支持件有source/binary SHA、协议version、allowed previous reader、running identity；root catalog不能从普通用户目录追随可写symlink。

helper是有限worker，新请求版本由selector决定；更新先取得排他的support mutation lease、停止接纳旧代mutation、join其它旧worker，durable写意图并CAS新selector，再让本次旧worker结束lease/退出；后续新worker确认全部旧pidfd退出才签退休。处理lostreply时查selector/journal，不能等待本worker自己退出。业务事务忙只stage，不能第三代挤掉rollback件。launcher已经exec成stock NATS而无驻留Go映像，更新其文件不能声称NATS也换代；下一合法launch校验目标launcher SHA并记录exec映像退出。两代helper/launcher更新及回滚有真实archive/运行收据；不让固定旧support parent长期躲在“基础设施”名下。S8仅提供通用两代/域能力，实际支持件的接线和验收归S9。

支持件升级不允许当前worker `Wait` 自己：旧worker先持久写切换意图并CAS新selector，随后结束mutation lease/退出；固定socket服务的新启动选择器读durable slot，**后续新worker**依据旧pidfd与journal完成旧代退场确认。旧worker不能给自己签 `owner_retired`。launcher在原地exec前发送LaunchAck意图并保锁FD；root handler只在独立census看见同PID/startticks的stock目标exe及继承FD后提交ACK receipt并释放自身锁引用，不能要求已经exec消失的Go代码返回，也不能把exec前ACK当完成。等待超过host动作预算则保pending/fence，下一worker查询真实incarnation；根worker死亡不释放server最后锁引用。P40组装该闭环，P31/P28分别验两代与stock锁。

轮换沿既有人工账户流程增加显式产品 `admin infra rotate-policy`（仅本机root授权、固定policy输入、同域CAS），在已有rotation guide中给确切先后顺序。隧道CA轮换仅更新相应policy trust引用，不扩大业务私钥读者；新revision提交前不得接受新infra请求。P24拥有guide与CLI接线，P03拥有验证，P31验旧票据撤销，P38同步runbook；不宣称当前手册式seed轮换已自动完成。


## §7 CLI、事务、错误码与恢复

### 7.1 唯一入口与wire

`cluster upgrade --component nats|caddy` 在既有cmd文件加flag；空值保持当前whole-host语义，`--broker-only`保持S6含义。infra分支在旧broker/agent planner之前分派，不能先reexec broker再升级Caddy。infra分派还必须先于当前`cluster_upgrade.go:59–61`的to-version校验、backup与N2 ack-writefence分支：后两者不能授权infra中断。已有CLI URL、身份、签名、预算原则不变；新driver在 `internal/infraupgrade` / `internal/infracli`，cmd只解析/调用/显示。

single无需cluster init：在既有 `admin` 入口提供 `admin infra upgrade --component nats|caddy`、`admin infra status --tx <id>`、`admin infra recover --tx <id>`，同一Service/Driver/root域，不复制单机状态机。cluster CLI对应 `cluster upgrade --component ... --status <tx>` / `--recover <tx>`；status/recover与新计划参数互斥，不允许覆盖pending记录。config reload通过同一component请求 `--reload-config`（只适用caddy、不能同时要求binary版本），单独展示配置generation与同PID。以上flags/commands的Hidden=false、互斥规则、帮助文案进入两个CLI golden；没有任意“执行root命令”的CLI。


二进制正向使用既有 `--to-version` 与 `--expect-sha256`，在infra分支解释为所选组件的catalog版本与**binary** SHA（非archive SHA）；缺少或与catalog不一致即64。`--rollback` 使用每host实际prev/current-state兼容记录，与to-version/expect-sha256互斥。`--reload-config --config-sha256 <hex>` 仅Caddy，摘要必须对应产品已验证的受管staged配置；不接受任意root配置路径。status/recover不要求to-version；component指定时拒绝broker-only、ack-writefence、backup-taken这些broker-only语义，保空component旧行为。single admin复用相同flag验证和状态机，不另写协议。

支持制品由同产品域管理：root-only `tether-infra-helper update-support` / `rollback-support` 只选择installer已stage且root校验的本release/actual-prev slots，无任意path/argv/下载参数；本机admin `infra status` 显示其pending/retiring结果。bootstrap、update-support、rollback-support三个固定root入口均在P40 main闭合；P24给具体指引，P31/86.U实跑，不能要求操作者手动换symlink或杀旧worker。

新NATS subjects用本仓prefix SSOT生成：
`tether.v2.cluster.infra.<node_id>.req` 与 `tether.v2.ctrl.s.<sid>.node.<nid>.infra-move.req`；组件request仅account授权管理端/本机admin，move仅broker→该agent/连接owner，reply用真实 `_TINBOX`绑定。P21冻结builder/parser及正常SUB/ACL双向descriptor；直连owner单答，不用queue随机命中别host。payload为严格编码、独立version/purpose的infra request，不能把opaque bytes绕过codec校验。cluster-health/admin只加 `omitempty` 的能力/报告字段，旧零值明确unsupported/unknown，不是ready。

旧 `ClusterUpgradeReq.SigningBytes` 的zero-extension bytes完全不变；新subject不复用旧reload op。旧broker无能力时以事前真实probe判拒绝；no responders/timeout只是不知道，不能fallback旧reexec。真旧artifact四象限分别测普通互通与升级能力拒绝，不能用fake“old verifier”代替。

### 7.2 状态图与故障语义

`Prepared→Staged→Ready→Moving→Cutting→Verifying→Returning→Retiring→Completed`；reload、N1没有的步骤也须明确no-op evidence，不能用默认空proof跳过。任一前置拒绝进入Deferred且before服务不变；Cutting之后不明/失败进入Halted。Recover从根journal读实际phase继续，不能把Halted直接标Completed。phase是S9 adapter的细节；S8通用外层status词表与两代限制保持原义。

| 故障窗 | 允许的恢复 | 必须保持/禁止 |
|---|---|---|
| stage/ready/cohort未完整 | 释放短seal、恢复原准入，保旧服务；候选slot可留待重试 | 禁LDM/stop、禁默认Complete=true |
| LDM之前丢prepare ACK | 查同participant/generation receipt或超时defer | 不以capability=true猜安装 |
| LDM已生效但stop reply丢 | exact pidfd+census查旧已退与健康survivor，再决定本步 | 不signal当前新MainPID，不滚下一台 |
| promote已落盘但未start | root start fence/journal确定目标；当前store启动候选/prev | 不从磁盘SHA推runtime，不打开第二store |
| N1新server起、ACK验证失败 | 保恢复gate与证据，明确halt；当前store诊断/兼容回退 | 不先replay洗白、不restore旧snapshot |
| Caddy旧主已退、final ACME未完 | shadow继续，fence仍关，重试当前态有界分段同步 | 不在2s后偷跑main，不到期杀shadow |
| main ready但return缺ACK | 保入口与当前责任表，继续同代return/query | 不删仍在用的warm，不以任务完成替迁移 |
| controller死亡/轮换policy | 新管理者按S8域认领后query；旧policy不能新增mutation | 不手删marker/lock、不超两代、不盲unflip |
| 支持件pending | stage-only；旧worker必须实际join，selector CAS一次 | 不覆盖prev、不留旧guardian常驻 |

每次不可逆effect各有before、actual-applied、reply-lost、recover-confirmed的观测点。lost ACK必须证明真实请求已发送、目标副作用已发生、返回路径实际被截，三者digest同一；只关TCP而没命中reply窗不签该测试。重复/乱序/迟到请求通过root CAS与S7稳定ID去重，不重新铸造业务请求。

### 7.3 封闭错误码和exit

Code用下表封闭family，Phase/HostOp等enum进入真实enum family表；Detail不含秘密或用作机器分类。所有实际emitter→wire→leaf classifier→cmd exit/hint双向登记，无死code，无unknown默退0/70。

| code | exit | 产品含义 |
|---|---:|---|
| `infra_unsupported`, `infra_artifact_invalid`, `infra_config_invalid`, `infra_incompatible` | 64 | 平台/能力、来源/bytes、配置、回读不支持；不靠重试绕门 |
| `infra_permission_denied`, `infra_policy_stale` | 77 | UID/签名/权限或已撤销policy；无副作用 |
| `infra_not_enabled`, `infra_busy`, `infra_not_ready`, `infra_inventory_incomplete`, `infra_budget_exceeded` | 75 | 管理入口待显式启用、域忙、健康/库存/预算门；给具体未过barrier |
| `infra_result_unknown`, `infra_recovery_required` | 74 | 已发effect不明或cut后需恢复；保Tx/phase及只读status指引 |
| `infra_internal` | 70 | 已定位内部不变量/IO错误，不能冒作retryable成功 |

completed=0；safe-defer的CLI75与drill验证“正确拒绝”的GREEN0不同层。Halted不自动exit0；无ack的请求不宣称failed已回滚。保既有认证/网络错误class，不为S9新造同义的通用network69。未知外部host错误通过 `infra_internal`明确归类与诊断，不靠catch-all静默丢码。

S9操作与agent/broker image cut在同upgrade domain互斥：从Prepare持有到所有cohort/worker责任交清；正常结束只留durable terminal记录。P22/P23在全部image入口及watchdog入口接检查，pending infra拒image cut；不把活S9 owner塞进S5/S6 image。故public/private image应为**零新增S9运行态**，必须scanner证实没有可达漏项；P19使用S7已有state，不扩其codec。拒并发并不让Caddy永久shadow成为完成态。

## §8 可立即实现 vs 依赖 S6/S7 交付物

| 项目 | 现在可做 | 真实依赖与验收门 |
|---|---|---|
| P00、artifact、物理身份、IPC/policy、unit、lock、nft、Caddy config/ACME、readiness表、SYS配置 | 可在最终leaf完成合同/纯逻辑/独立假端口测试；不能在本只读规划轮实际实施 | 各自真实kernel/stock外置测试再签；P0失败桩不签行为 |
| NC raw取消、cohort、route bridge、auth-only/gate、rootjournal | 可先实现typed接口与隔离测试 | 接实际agent/broker/ctl全部dial与SUB时需宿主交还SHA；仅fake接口成功不广告能力 |
| S6接缝 | 当前Run内cut、maintenance、identity/lock可读可对照 | 要single/sole/N3 final source SHA、三完成点、37/39BR与gate receipts；保S5原socket回归缺口如实状态，不重新宣称pending实现 |
| S7 continuation | P19先按中立Transfers接口写adapter合同测试；P32先写schema validator/空供应拒绝 | 等实际worker/tracker/writer/reader/courier API、原NUID/consumer retirement、JS503同checkpoint、audit确证、normalREGISTER能力、真实live fixture**可执行文件+schema+源码SHA+正负收据** |
| S7候选的Freeze/Thaw与rate-limit | 候选有 `xferworker.Manager.Freeze/Thaw`、`xfertrack.Manager.Freeze/Bind` 等，可作为实现对照 | 不是当前事实；P19逐签名映射，不要求S7先实现S9的Move/cohort；如果实际合同不同只改P19映射/P00审查，不改S7已冻语义 |
| S8通用事务 | P20先对中立Transaction写所有状态/两代/域/超时测试，compile stub可并行 | 等真实domain Acquire/CAS/Load、启动恢复、两代max、support-artifact合同、authority incarnation能力与收据；S9自己注册nats/caddy和root状态 |
| 宿主薄接线与CLI | P22/P23/P24可先冻结old→new responsibility映射 | S7修改中的同文件按交还SHA串行rebase；不在私有快照预支未来slim、创建两个实现 |
| 85/86及完整跨层验收 | P26/P32/P33可先交真实供给、fixture自检、合法arms、NOT_RUN行 | 只有S6/S7/S8真实依赖齐备才跑完整成功；缺供应SETUP-RED，真实未覆盖cell INCOMPLETE，均不算E3/E4绿 |

能力安装顺序与capable演练顺序分开：首次把续接能力装到旧车队沿broker receiver-first，再agent/ctl（broker-ops §8.4）；全部capable后的四层事务依赖固定 `agent→broker→NATS→Caddy`，回滚反向按实际当前态和兼容门执行。S9测试dependency planner，并只执行NATS/Caddy阶段；不借此启动S10五代全栈。临时broker仍走现 `brokercontrol/clusterstatus.go:863–866` 的exact release hard rejection，不能把runbook旧WARN文案或相同ProtoVersion当许可；本方案不需要增加broker，仍有T31负控证明任何备用路径不能绕join门。

## §9 测试、演练、负控与成本

### 9.1 分层与真实非空条件

L0纯codec/状态/权限/拓扑表；L1 pinned embedded NATS；L2非特权外置官方两版NATS及高端口Caddy；L3真实tether/agent/ctl/S7混合业务；L6 systemd/nft/IPv6/多UID/ACME/live root retrofit的85/86。L0/L1不签真实exe/权限；L2不签systemd/cap/nft；L6不以Docker provision绕产品。外置binary由P26 **预先供应且验证**，测试进程不能下载、现编历史release、以当前binary改version冒充旧件；缺对应指定供应时专门integration job失败为setup，不能Skip变绿。默认hermetic gates不需要这些外部binary。

共同requiredset在首个cut前固定：原SSH连接、4条双向TCP（含idle/half-close）、SS两个订阅alice/bob、可靠PTY、长exec、四条tier-B transfer。SSalice是独立撤权victim，bob及其余原socket必须继续；PTY真resize/signal/KA lost ACK/input/output、ps/kill对独立victim有实际效果，不只观测ONLINE。所有TCP/SS/SSH以原FD/socket cookie/tuple/application nonce、target Accept=1、redial=0和双向seq/hash/FIN终点签；longexec/PTY保原child/ctl PID和任务ID、输出账及最终native rc。

NATS/WSS NC变代与数据面tunnel carrier分账。仅切infra的正常profile传 `--carrier-transition unchanged` 给既有flow oracle；专门session rebuild profile可以预声明允许内部carrier replacement，仍不得改变原业务socket。不能因旧 `hot_update_flow_probe.py:344,510` 默认replaced而强行重建tunnel；P32用适配器显式选口径，不改旧S5 judge默认。

四transfer为四独立session/bucket，固定2push+2pull，66,549,440B与1GiB各双向；每hop、每方向、每phase都取原ctlPID/transfer_id/NUID/0<durable<total。同一个cohort穿过整个upgrade→rollback→upgrade，N3每个host都需要四条仍活。原调用早完则该cell INCOMPLETE，不能补开另一笔、不暂停producer延长窗口。采用S7**已交且lead接受**的生产rate-limit或在开始前冻结支持的限速profile；若无可用rate机制，P32必须报告夹具前提未满足，不用test-only服务暂停补偿。fixture预算按原start/size/rate计算并保存，不在故障后改变。

writer publish（含ACK已丢）、reader partial-write/已Sync、commit/rename、terminal/courier各至少真实跨一个cut。不能要求同一transfer同时处于互斥phase：四条live payload是每hoprequiredset，额外独立phase lanes覆盖commit/terminal，结束后不冒充四条仍live。NUID、逐idx原始JS数据、consumer retirement、exactSTART/TERMINAL审计与原S7 receipt互相校验；最终SHA只是合取一项。

10/s来自独立observer monotonic排程，slot时间 `t0+k*100ms`；每秒exact10、每slot记录scheduled/dispatched/attempts/terminal/effect，deadline从scheduled计算，排队/迟发/漏slot都失败。禁止用≥9/s或只从真正开始计时掩掉排队。node ls、ps、只读、真实write、短exec按预声明轮转，write/exec用稳定ID且each effect exactly once；每个请求最终成功且≤5s，所有attempt包括auth/connect失败均入账。p50/p95仅描述，硬门看max及故障首尾silence；flows≤5s、transfer持久进展≤10s、组件120s，500ms全image freeze是前阶段校准不伪作S9新门。

### 9.2 测试名字与反例冻结

T名为新增的责任名，不按评审轮次命名；既有Test/Fuzz不删除不重命名。文件及变异owner见§12；下表给每个编号唯一名字和要击穿的接缝。L标记的lifecycle tests在§12另列，均≥5轮真实exercise，不能用test退出回收资源。

| IDs | 精确Test/Fuzz名字 | 决定性断言 |
|---|---|---|
| T01/T02 | TestInfraContractFactories / TestInfraRequestCanonicalBinding；FuzzInfraRequest | 全consumer签名；每字段篡改/重复key/尾字节/未知enum |
| T03/T04 | TestInfraArtifactBindsFinalBytes / TestInfraPreviousReadsCurrentState | archive≠binary；真正新代写后旧版读，不执行未验bytes |
| T05/T06 | TestInfraProcessIncarnationCensus / TestIngressProofRequiresAcceptedSocket | 同SHA合法回滚、同PID exec；URL假变而socket未变必须拒 |
| T07/T08 | TestInfraPeerAndPolicyAuthorization / TestInfraPartialRequestDeadline | 普通UID/同UID越权/跨boot/key撤销；半包无worker泄漏 |
| T09/T10/T11 | TestInfraHostEffectCAS / TestInfraPromotionCopiesBeforeHash / TestInfraSupportRetiresOldWorker | 迟到unflip/丢reply；原FD写竞态；两代真运行与旧退 |
| T12/T13 | TestInfraEffectiveUnitRejectsOverrides / TestInfraBootstrapLeavesBusinessRunning | 累加cap、+Exec、env注入；socket初始inactive、原业务PID不变 |
| T14/T15 | TestNATSExecInheritsLifetimeLock / TestLegacyStoreOwnerBlocksSecondLaunch | exec/OFD机制、父死/最后引用/替换inode/alias/legacy；stock两版由T63实证 |
| T16/T17 | TestCanonicalIngressAtomicSwitch / TestPrimaryBypassCannotHitShadow | v4/v6/output/hairpin/lateSYN；main死shadow好必须拒return |
| T18/T19 | TestCaddyConfigPreservesSpecificRoutes / TestCaddyAdminBindsInvocation | /sub/manifest/WSS；普通UID不可root load；旧退重拨不误停继任 |
| T20/T21 | TestACMEFinalDeltaFollowsOldExit / TestACMEStartupFenceOutlivesRestartDelay | warm后真晚写/续期、final>2s、新回滚均读最新树 |
| T22/T23 | TestCaddyDriverRetiresAllOldResponsibilities / TestCaddyDeadlineKeepsServingShadow | 原业务live时old/temp真退；过120s服务仍活且不completed |
| T24/T25/T26 | TestInfraTopologyUsesActualPlacement / TestInfraInventoryRequiresAllPages / TestInfraReadinessUsesOrdinaryAuth | target R1/R2/同host/consumer lag；漏页/重建；peer不能掩本机缺queue |
| T27/T28 | TestSystemPrincipalReloadPreservesGlobalAccount / TestSystemPrincipalStepdownIsLeastPrivilege | 两版无restart、$G流不搬；普通身份拒、窄SYS真stepdown |
| T29/T30/T31 | TestNATSCutWaitsForInstalledSuccessors / TestNATSSingleUsesStopWithoutLDM / TestNATSHaltedRollAndJoinGate | 迟到NC/丢ACK不signal；N1无隐藏重启；LDM后不滚下一台/不同release拒join |
| T32/T33 | TestPlannedMoveInterruptsNATSMutexWait / TestPlannedMoveJoinsBlockedWebSocketWrite | 真mu/Write阻塞、DNS取消、旧代lateworker不能改继任；≤5s |
| T34/T35 | TestIngressCohortSealsLateConnections / TestIngressReturnRequiresEveryAttachment | 独立dataNC必登记；原cid/forwarded ACK不能签新连接 |
| T36/T37 | TestInfraRouteProofBindsBothGenerations / TestInfraRouteHoldExpiresWithoutRenewal | full home/health identity、原8s；8/60s后真reaper不被遮 |
| T38/T39 | TestOrdinaryReconnectAuthenticatesBeforeBusiness / TestOrdinaryAuthProbeNeedsNormalQueue | 记录真实SUB/PONG/RPC；删除正常queue必须红，无临时responder |
| T40/T41 | TestACKPrefixIsReadBeforeRepair / TestAssetSealFencesEveryProducer | 删已ACKchunk后可replay也仍红；create/delete/recreate与sent-unknown |
| T42/T43 | TestTransferAdapterKeepsOriginalReceipts / TestTransferAdapterRetiresConsumerBeforeReturn | NUID/idx/原budget，503等待不failed，consumer UNSUB/PONG/删除receipt |
| T44/T45 | TestInfraComponentsRegisterWithTransaction / TestInfraImageAndSupportDomainsExcludeOverlap | stub无法广告、S8/root状态一致、第三代拒、image并发拒 |
| T46/T47 | TestInfraWireRejectsOldReloadConfusion / TestInfraACLMatchesLiveSubscriptions | 真旧binary不误reexec；普通member/agent/管理者权限四象限 |
| T48/T49 | TestBrokerInfraColdHotReconnectBindings / TestBrokerInfraGatesAllJSProducers | 同descriptor三入口、local-only；GC/audit/cold修复都在恢复门后 |
| T50/T51 | TestAgentInfraMovePreservesDataOwners / TestCLIInfraMoveRetainsOneInvocation | 原SS/flow/PTY不随NC取消；正常REGISTER能力与原调用ID |
| T52/T53 | TestInfraCLISelectsComponentBeforeHostPlan / TestInfraCLIReportsUnknownAndDeferred | 默认whole-host不变、single可达；64/74/75/77与Tx指引 |
| T54 | TestInfraInstallerNeverStartsOrMutatesDryRun | 3角色/allroots/全进程/effective metadata，真实archive非skip分支 |
| T55/T56 | TestInfraReleaseContainsSupportingBinaries / TestInfraImageGuardChecksEveryCopiedInput | source/archive/running闭合，逐COPY变动必须stale |
| T57/T58 | TestInfraSupervisorReapsAfterTestParentDeath / TestInfraObservationRejectsMissingProducer | kill测试父/timeout/setsid孙/Start竞态全退；空字段/伪generation拒 |
| T59/T60 | TestNATSLostACKAndConsumerContinuation / TestNATSSingleAdjacentCycle | 真effect+丢reply+fault-hit；两N1当前store原ACK/NUID |
| T61/T62/T63 | TestNATSThreeServerAdjacentCycle / TestNATSQuorumAndReplicaRefusals / TestNATSProcessLockFailureWindows | 每host current；N2/lag故障后原窗保红；stock锁/旧alive负控 |
| T64/T65 | TestCaddyReloadKeepsOriginalBusiness / TestCaddyAdjacentBinaryCycle | reload同PID新config、binary真SHA；原socket+迁出/迁回 |
| T66/T67 | TestCaddyRetirementHandlesLateUpgrade / TestCaddyCurrentStorageRollback | 旧keepalive在零WSS后Upgrade/慢HTTP/close堵；当前ACME回滚 |
| T68/T69/T70/T71 | TestInfraOriginalMixedCohort / TestInfraRouteReaperIntegration / TestInfraAuthReplayIntegration / TestInfraFourTransferEveryHop | 四真实phase/原requiredset/精确slots；认证恢复门与S7实际业务 |
| T72/T73/T74/T75 | TestInfraPrivilegeCanaryBoundary / TestInfraActiveInstallerInterlock / TestInfraSupportingRuntimeCycle / TestInfraAuthorityRotationRevokesTickets | 三身份、live reinstall、两代退场、已签旧key拒绝 |
| T76/T77 | test_infra_complete_trace_passes / test_infra_missing_slot_or_original_fails | Python独立oracle：空set/漏slot/redial/只盘SHA/错pid/repair污染 |
| T78/T79/T80/T81 | hot-update-infra-drill-test.sh 的 nats_manifest/caddy_manifest/native_controls/regression_scope cases | 合法arms/五headers/正负父分离/32 73 98原claim |
| T82/T83 | TestInfraContractGateControls / TestInfraRuntimeGateControls | 701行、legacy+1、包环、root依赖业务、ctx/data lifetime、坏scanner根 |
| T84/T85 | TestInfraInventoryMatchesRuntimeCodecs / TestInfraErrorEnumAndLeakInventoryControls | 真codec/loader/descriptor三向；漏enum、code、anchor各红 |
| T86/T87 | TestInfraMatrixPartitionsEveryHeavyTest / TestInfraLedgerRequiresNativeEvidence | whole-race覆盖与逐Test执行集合相等，600s单元不聚合 |
| T88/T89 | lint-install-selftest.sh / hot-update-infra-supply-test.sh | 多heredoc A含substitution+B quoted、exact targetset、stale guard |
| T90 | TestInfraDocumentationClaimsNeedReceipts | 路径/命令与scope对账，无收据勾选拒绝 |
| T91/T92 | TestInfraRootBindsEveryHostOperation / TestInfraRootExecutableHasNoBusinessBootstrap | 实际helper入口→全部backend，无stub/缺verb；真实依赖闭包无agentboot/NATS |

T54–T56的Go测试分别落P31的installer_test/support_test，hermetic输入控制在P37；T82/T83落P34两个新门文件；T84/T85落P35新inventory文件；T86落P36 inventory_test，T87落P37 simcluster_gate_set_test并核对其ledger脚本消费P36账本；T90由P34 infra_contract_test消费P38文档。producer和观察器分文件，不让被测receipt自身生成“独立”oracle预期。

### 9.3 85/86原生arms与负控

以下五headers为合同；worst是初始调度上界估计，不是实测耗时。P33给每个arm独立拓扑和READY/live/FINAL边界；两个agent容器明确为agt1/agt2，加ctl，不能写“一个agent”却跑双收发。

~~~sh
# 85-hot-update-nats.sh
# arms: A A1 B D Q L S J F
# fixture: A=N1 A1=N1 B=N3-live D=N2-live Q=N3-live L=N3-live S=N1 J=N3-live F=N3-live
# grows: A=0 A1=0 B=2 D=1 Q=2 L=2 S=0 J=2 F=2
# worst: A=720 A1=720 B=1200 D=300 Q=360 L=360 S=300 J=360 F=540
# forgoes: A=- A1=- B=- D=- Q=- L=- S=- J=- F=-
# 86-hot-update-caddy.sh
# arms: R B I H C K F U
# fixture: R=N1 B=N3-live I=N1 H=N2-live C=N1 K=N1 F=N1 U=N1
# grows: R=0 B=2 I=0 H=1 C=0 K=0 F=0 U=0
# worst: R=600 B=1200 I=840 H=720 C=300 K=300 F=600 U=720
# forgoes: R=- B=- I=- H=- C=- K=- F=- U=-
~~~

85.A/A1/B均原cohort三向cycle并回2.14.6；D=N2拒绝，Q=实际失quorum拒绝，L=stream/consumer lag拒绝；这三条都包含修复后新事务成功，但修复不抹原拒绝/失败窗口。S=真实double-open/锁/legacy guards；J=临时broker不同release真实拒绝，healthy exact-release控制；F=LDM后失败→halt保健康survivor→产品recover，原业务继续。85父每个正arm必须GREEN0、nc_gap=0；F的预期故障以明确安全断言为绿，不能把正常120s超限样本洗绿。

86.R=reload，B=三host adjacent binary cycle；I=旧root/on-failure + 当前ACME真晚写首次retrofit，全负载内完成；H=healthy N2 Caddy成功；C=坏cert/config before-effect拒且原业务不变；K=UID/override/policy/lateUpgrade/全责任门部署判定；F=main失败/控制器死后跨120s保shadow、明确未完成，再产品recover；U=warm自身与helper/launcher真实两代切换回滚及旧映像退场。I/B/U不能用已经retrofit好的READY代供给。

原生每child恰一行 `DRILL-ARM arm=<A> of=<drill>` 及恰一行：
~~~text
DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=<nonzero> -- <detail>
~~~
其它类别同原validator字段与顺序，不发自创VERDICT行。rc0=全部assert绿；新断言或人工mutant失败1；prerequisite/供应/采样custody坏2；已独立钉signature真实历史损失3；真实未覆盖cell4。exit75是产品safe defer，不是native drill码。

M01–M36按§12实际产品diff逐一独立restage、通过完整image guard、跑对应未修改oracle，期望native1；保存baseline绿、diff、source/binary/running SHA、真实fault-hit、原生红和恢复绿。不把预期红child合入85/86正parent；每个mutant在独立instance/evidence run用同合法--arm，parent汇总单独保存。删除/伪造Status字段的judge单测只是补充，必须另有旧PID真留、磁盘真换运行没换、main死shadow好的物理负控。无 `--negative-control` 透传假入口。

39新增NI/CI为真正历史tether无infra capability的**安全拒绝**负控，正常业务持续，原生GREEN0证明拒绝，不能冒称历史断流PRODUCT-RED。如以后另要历史损失签名，必须独立建立并由lead钉住再登记3；现有39.X/Y/P/F/BR语义与native期望全保留。各正负run不共享parent或互相改expected抹红。

### 9.4 供给、地址、五份基线与回归

P26供应两NATS、两Caddy、tether current/next及真正prev、helper/launcher两代、installer/catalog/CA fixture和source receipts；支持制品两代必须实际不同source/bytes且兼容，不用空改version充代。Docker构建/每次drill独立guard核完整COPY输入的path/type/mode/hash、Dockerfile/installer/provision/scripts/catalog、最终image文件及目标制品。只改任一个输入但忘build必须拒；receipt本身不能自包含其hash形成循环。source-manifest由外部读取完整bytes计算，validator从实际COPY集合反查无漏项，不能只信自己生成的列表。

镜像提供systemd、nft、iproute2/conntrack观察能力、IPv6 enabled和网络双栈；不能依赖当前Dockerfile恰有sudo/nft或docker网络默认v6。product bootstrap检查可执行文件绝对路径/版本/内核hooks，缺条件before-effect拒；fixture不得写产品nft规则或提前activate socket。B-6地址测试实际解析agentyaml：NATS WSS endpoint为443/受控8443，tunnel=7000；正/负例走真实config parse与connect，不能把7000当WSS而只验URL字符串。

五份基线均由P36唯一writer闭合：`expected-verdicts.tsv`、`expected-verdicts-log.md`、`tests/assert-identity.baseline.tsv`、`tests/kept-sites.baseline.tsv`、`docs/reviews/simcluster-coverage-inventory.md`。P36另交infra-ledger与drill-costs；只按实际predicate/source集合更新，不凭计划数生成一片GREEN。既有四日志均经logs.sh读fresh cursor；不加inline journal豁免。

32扩全roots、真实support archive安装、never-start全进程集合，同时保原ctl/agent下载/卸载/manual cold-path claim。73保真实recovery原scope：它的fresh SS/curl及Python TLS front不能签原SS socket连续，86另验。98保当前原native expectation及#72的WSS缺口；S9 planned raw-wedge成功不能替98长故障梯子关闭gotcha。若扩98真实WSS fault，需单独同缺陷负控/多轮收据后才改ledger，非本计划默认顺便勾绿。

### 9.5 heavy matrix与进程父死监督

所有Go外置child在cmd.Start之前交P27统一监督：独立subreaper/进程组、父生存管道、启动nonce/握手、原测试PID/startticks/pidfd（可用时）和bounded reap。watcher在test timeout/SIGKILL/parent先于握手死亡时kill整棵受管子树并Wait；setsid后代仍由subreaper/cgroup登记追踪，不能只kill(-PGID)。从来不把agent runtime置为subreaper抢业务child。外层matrix超时同样通过该监督模型清理测试进程组及其后代；L6容器/unit最后由独立宿主drill teardown回收。T57真杀父测试进程并检查零残留，不能只测试context.Cancel。版本probe、NATS/Caddy、helper、openssl/测试CA、ctl/workload每个spawn都登记，禁止旁路exec.Command.Start；所有产品child另有对应产品stop/join所有权。

L0/leaf package整包-race无-run；重集成局部tag：
`test/infranats:infranats_integration`、
`test/infracaddy:infracaddy_integration`、
`test/infrabusiness:infrabusiness_integration`、
`test/infraprivilege:infraprivilege_integration`。
tag仅出现在各自test目录，Makefile tag列表、gate count、矩阵golden共同更新；不在internal藏tag测试。每个whole declaration保无-run，runner按相同race/tags枚举每个顶层Test，实际unit固定 `-run ^TestName$ -timeout 600s`，每unit一个test；无法枚举/零test/集合不一致即失败，不能fallback巨型suite。直接whole-matrix入口也走同一分片逻辑，不能只有parallel模式分片。

五轮leak使用真实短生命周期与错误窗；120s deadline真实部署验证独立臂一次，不能把5×120s塞进600s矩阵。任何单个真实Test仍超过600s，应按不同责任拆**新测试**并保旧名字/覆盖映射，或在冻结前明确独立L6单位，不能靠整套提高timeout。所有-risk包（即使没有raft/yamux import）显式-race列入：本稿全部新增runtime叶子、两个cmd、受影响broker/agent/cli/proxydial/connrecovery/natsconf/jsstream/auth/adminsock/brokercontrol，以及infraharness；既有S6whole tuple不删。每包至少一个Test函数体直接调用共享goroutine+FD leak helper≥5轮，test→exercise anchor精确到文件/函数；查资源在product责任退休后、fixture cleanup前。

### 9.6 成本与最终执行

估计每个L0新门边际≤2s、全部新机械门≤30s（待实测）；非特权外置单Test目标≤180s，实上限600s；85/86 worst见manifest，source/tag编译、真实archive冷下载、image build、warm-cache、N3 grows、两agent+ctl、各positive/negative、ACME和support cycle分别计wall/CPU/RSS/FD/残留。85.B/86.B以三个host×三方向计九hop，不能只预算一次换代。没有实跑值时drill-costs不写measured。

未来实施收据顺序：供给+所有controls→leaf/embedded→逐Test外置→跨包→85/86及32/73/98→同候选三硬闸 `make test`、`make e2e-parallel`、`make lint`，另 `make gates`（含vet-tags/hermetic/lint）→Darwin。正常闸通过后不无理由重跑；任何生产/配置/judge变化使受影响收据失效。最终lint由一个owner串行避免共享缓存争抢。该段是未来实施命令合同，本只读合成轮未执行。


## §10 全部门禁、结构预算与账本闭合

### 10.1 在首个接线包之前归还空间

S6的三轮事后slim不能重演。下表是本快照只读词法计数：逐token起始行计代码、去comment/semicolon、raw string只计起始行，AllOS包含平台twins；不是执行Go门的收据。正式P34必须以仓库同一AST/scanner重测并输出entry/after/delta；与只读数不同先解释，不能改门适配本表。S7/S8交还SHA重新取entry，不把其尚未交付slim记为可用空间。

| 宿主 | 本快照 production files / code | 类型/不可突破的实际门 | 本stage分配 |
|---|---|---|---|
| broker | 70 / 16952 | 文件**恰70**，Broker≤285（当前285），code<18000 | P22不加production文件/方法；既有文件先移控制实现到infraattach再加薄调用，每文件净Δ≤0 |
| agent | 19 / 7694 | ≤19，Agent**恰125**，code<8000 | P23不加production文件/方法；不靠306行包余量突破单文件帽 |
| cmd/tether | 57 / 13225；nonCLI867 | 文件**恰57**，code<14000；nonCLI现golden800档，即不增长到900；direct1300不是可花余额 | P23只改ctl_connect；P24先抽cluster-drive/CLI非解析逻辑，再加flags/errors/rotation guide；不用新import cobra伪装逻辑 |
| cluster | 32 / 3765 | 文件**恰32**，code<4000 | Δ0；事务不扩Raft schema |
| brokercontrol | 17 / 4548 | ≤17，code<5000；ClusterAdmin当前86且不增 | P18仅audit publisher薄门；先归还该文件空间，不加宿主文件 |
| execsession | 10 / 3440 | Runtime**恰39**，code<4000 | Δ0；不塞S9状态 |
| tunnel | 22 / 5790 | code<6000，既有文件ledger不增 | Δ0；正常infra cut不重建carrier |
| cli | 13 / 2472 | 现code2000档不提高 | P23连接薄调用；逻辑由infratransport/infraattach持有 |

`structural_budget_test.go:1063–1069` 实际强于CLAUDE部分“≤”文案，明确钉住上述恰值；`:1019–1032` 对**每个**legacy文件单独比帽，不只oversized文件。P34同时保direct caps与quantized ratchet，P35只收紧golden，不能提高既有债务。

本stage所有已存在production文件采用“entry不增”合同；其中ledger已有的还必须≤原ledger。以下是本次会动的既有文件代码行基线（精确文件manifest见§12，列数为上表只读口径；gate解析为最终裁判）：

| owner / 文件组 | entry code，逐文件不得借别的文件下降抵销 |
|---|---|
| P12 natsconf | preflight280、render132、takeover218、render_desired69、reconcile139 |
| P14 connrecovery/proxydial | teardown129；proxydial124、budget474、socks5176、httpconnect54 |
| P18 JS/audit | jsstream285、replicas109、audit_publisher481 |
| P21 proto/auth/admin | subjects258、alerts79、codes41、permissions170、protocol417、server550 |
| P22 broker | broker1487、reexec1223、authcallout224、cluster_upgrade_trigger534、cluster_health179、admin135、topology_reconcile275、reconcile_passes172、transfer1204、transfer_reconcile214 |
| P23 agent/cli/ctl | agent1292、roster749、conn_teardown169、upgrade_image675、transfer140；natsconn234；ctl_connect432 |
| P24 cmd | cluster_upgrade408、cluster_upgrade_drive383、serve447、admin461、exitcode64、error_hints290、cluster_rotation85 |

每个owner先交“原symbol→最终leaf→调用者→tagged test→source scanner→inventory→matrix tuple”机械移动表，保注释/测试名字与错误语义；再交语义接线diff。新行为从第一笔就写入最终leaf。P21将ClusterHealth定义移动至cluster_health、admin消息至infra_protocol、ACL builder至infra_permissions；P24将infra error分类/文案放infracli并从现有表提取等量通用代码，不能将single长行、删注释或压缩switch当slim。若不能满足既有文件Δ≤0，包未完成，不能交给P39善后。

所有新production文件≤700 **code** lines，Linux/Darwin/other各自计入，700不是原始行数；新类型<100 methods，S6 image/type已有更紧<40 methods/<100 fields保持。本稿所有新owner类型目标≤40 methods，默认新包≤20 production files、<2000 code；`infrahost`、`infraupgrade`的封闭系统动作/两组件状态机允许**首次登记**2000档并另钉<4000，不提高任何既有包债务。首次登记必须实测、手工写理由，不能由update偷加未知债务。P00冻结后如职责无法装入这些最终leaf，先修改候选manifest再freeze，不能实施中留下临时大文件等待末尾拆。精确生产文件数不靠挪走一个空文件/改成test绕门。

### 10.2 不可遗漏的机械门

| 门族 | S9增量与正负控制 | writer / 真执行点 |
|---|---|---|
| layering/root closure | contracts只stdlib/controlbudget；root实际transitive imports不含agentboot、broker、agent、infraupgrade、NATS SDK或testharness；只非rootdriver链接业务。包级adapter不可import宿主形成环；注入一个间接业务import必须红。固定root工具env/FD白名单是可信执行边界，不把签名等同OS sandbox。 | P34扫描、P40实际入口；Makefile由P26接gates |
| 结构/legacy/701 | AllOS实际测量、全部legacy entry、main真实cobra import解析；+1到已满legacy、701新文件、假comment隐藏logic、错平台twins分别红。 | P34；P35单一golden writer |
| wire/ACL/CLI | public新增omitempty；字段名/tag/type append-only，旧SigningBytes不变；真实SUB↔ACL双向；adminRequest/Response/RuntimeReport原roots已经可达，不重复建第二wire账本；command/flag/Hidden双golden。 | P21/P24代码，P35账本 |
| private/IPC/credentials | HostRequest/Receipt及可达对象、rootjournal、policy/key loader、route proof、cohort/checkpoint、observer schema、runtime producer/codec/loader三向；实际字段删改/遗漏或credential路径多一读者必红。私有协议有自己的version，不能借proto root掩盖。 | P35 infra_inventory；各包提供真实producer行 |
| image/state零Δ | 从实际agent/broker image可达root、私有descriptor/encode/decode扫描；infra互斥而无活对象进入image才可记零Δ。若S7真实adapter需要新域，先解冻实际账本/版本，不写“零”掩漏。既有flow/SS/broker frame库存保持。 | P35与P22/P23；三向控件 |
| enum | Component/ArtifactKind/Phase/Action/HostOp/RoutePurpose/RouteDirection及实现新增auth/recovery/support closed families全部登记实际成员；枚举反向AST遍历，不沿用文档“15家族”旧数字。default只有显式列全成员才可存在。Observation.Kind及错误词表另做string闭合。 | P35 enum_switch_default；P00字段owner |
| error coverage | 每个§7.3 emitter↔namespace↔exit class↔hint双向，缺emitter的死code也红；分别覆盖permission、unknown、defer与unsupported，不将drill native码混入CLI分类。 | P35四error gates；P24真实呈现 |
| ctx/lifetime | 新Background/TODO每个站点写ctx-root或ctx-none及reason进入ledger；control ctx不能成为SS/flow/PTY/transfer存活根。新infra数据owner导出API用Deadline/Stop、不接受context；私有DNS/TLS/action ctx有唯一cancel+join owner；旧豁免不能扩大。 | P34 ctx/data lifetime；P35 ledger；P14/P19/P22/P23行为 |
| budget/pin/security | 热预算继续读upgradebudget SSOT：5s request/flow、8s maintenance、10s transfer、120s组件、8MiB/256MiB replay等不复制数字；新增helper子预算只在infra.go及shell对应项定义，缺/重/非整数红。NATS模块/installer/catalog终态2.14.6、ping全部dial profile一致；Go1.26.0/toolchain1.26.8与NATS安全下限不改。 | P36常量/脚本；P34 pin/ping；P35 budget_test |
| TLS/secret | 继续四对TLS校验配套门，新增dial用当前pin/verified-chain helper，不加InsecureSkipVerify孤点；config/admin/ACME/票据canary覆盖fmt/error/hex/base64，运行证据只按既有私密策略落地。 | P14/P03实现，P34 layering/TLS扫描现门 |
| leak/race/进程 | §9.5每个新runtime leaf与实际宿主整包-race；每危险包Test体直接双shared assert、≥5轮且exercise在loop内；新增没有raft/pty import的root/IPC/NC同样显式登记，无例外增长。child父死监督是独立于goroutine/FD的第三条门。 | 各owner供anchor；P35 exact表，P36 tuple，P27父死监督 |
| tags/source移动 | 四integration tag仅各自test目录；Makefile vet-tags、tag计数、矩阵source map同步；旧S6测试/Fuzz名字、原tuple全保留。移动代码须令旧scanner跟真实新symbol走，不保扫描空旧路径的绿。alias/import/跨文件调用负控红。 | P34扫描，P26 Makefile，P35测试库存，P36矩阵 |
| hermetic/install lint | 所有tests/*.sh按真实shebang双向列入run-all；新Python自检由显式shell wrapper调用并对账，不靠glob偶遇。lint-install按实际所有shell renderer target精确集合、所有heredoc而非greedy最后一个；A未quoted含substitution+B quoted必须红，Go renderer生成内容另测。无root/network/Docker/真实安装/下载。 | P37唯一脚本/targetset owner |
| matrix | whole声明无-run保留；四重套件按**每个顶层Test**列举600s单位，race/tag闭包同flags，零枚举/漏Test红；唯一fork go test在test/e2e，库/harness不另开go test。运行dedupe仍只按相同-race依赖闭包，不为去重删源码tuple。 | P36；all_phases与direct/parallel runner同时闭合 |
| simcluster | 五基线、合法arm、五headers、原生字段/rc、完整COPYguard、fresh四日志、无inline日志豁免；所有新shell有hermetic入口；32/73/98旧claim如实不升级。 | P26供给、P33drill、P36ledger、P37controls |
| 既有门与发布 | 选举测试ObserveLeader不Sleep猜leader；private inbox/session绑定、cfgdb/raft timing和产品sleep门不弱化；三层lint/nolint递减、8个seedSession转发、spawnexec统一init/64租约/四spawn-stall证据及remoteFS实际行为、docs_layout、origin/promised-guard、CI release needs三闸+tag/contents权限、fuzz schedule可达且不进PR，均不削弱。根独立binary不能借用业务main绕init门。 | P34注册/原门、P26release；P38文档 |
| fuzz与命名 | 新手写严格codec的FuzzInfraRequest确定性语料随make test，真正变异未来按需make fuzz；每目录≤64/全仓≤256KiB/header不变，不测stdlib JSON冒充自研解析。Test/Fuzz按责任命名、只增库存，不用review轮次名。 | P00/P35；不把随机fuzz塞硬闸 |
| 文档/关闭 | 每个“已实现/已覆盖/已勾”必须有同候选source/artifact/runtime/native receipt；新门CLAUDE registry与实际make gates双向。Darwin编译/既有功能检查真实执行；Linux专属driver在other平台明确unsupported，不伪成功。 | P38文档；P34文档门；P39收据 |

每道门必须在**同文件**有正负control，证明真scanner与生产源同谓词；gate自身变动后集中make gates，不靠手动跑某个Test代签。包作者不得自行写共享golden；P35汇合真实diff，重复/缺行/死路径即红。独立支持binary的race不是从外层继承：P26交race版本构建flags/source/binary，P27验证内层确为指定件；L6生产归档仍用正式静态构建，不把race与release SHA混认。


### 10.3 冻结matrix tuples与HostOp绑定

P36新增一个整包leaf矩阵 `TestHotUpdateInfraMatrix`：下面每个pkg独立声明一个 `(matrix,pkg,tags="",race=true,run="")` tuple，不是用一个窄run拼覆盖；unit timeout600s。所有原S6/普通矩阵tuple不删不改。目录列表是membership，不是文件编辑归属：

~~~text
internal/infracontract
internal/infracontract/contractstub
internal/infraartifact
internal/infrahost
internal/infrapriv
internal/infralaunch
internal/ingressswitch
internal/caddyconfig
internal/infraupgrade
internal/infrareadiness
internal/infratransport
internal/ingressmove
internal/inframaint
internal/infraauth
internal/infrarepair
internal/infratransfer
internal/infraattach
internal/infracli
internal/infraroot
internal/connrecovery
internal/proxydial
internal/natsconf
internal/jsstream
internal/auth
internal/adminsock
internal/proto
internal/brokercontrol
internal/broker
internal/agent
internal/cli
cmd/tether
cmd/tether-infra-helper
cmd/tether-infra-launch
test/infraharness
~~~

四个外置矩阵声明固定：
`(TestHotUpdateInfraNATSMatrix,test/infranats,infranats_integration,true,"")`、
`(TestHotUpdateInfraCaddyMatrix,test/infracaddy,infracaddy_integration,true,"")`、
`(TestHotUpdateInfraBusinessMatrix,test/infrabusiness,infrabusiness_integration,true,"")`、
`(TestHotUpdateInfraPrivilegeMatrix,test/infraprivilege,infraprivilege_integration,true,"")`。
这些**whole声明仍留golden**；P36 runner展开相同flags发现的每个顶层Test为独立执行单位。原始tuple、实际发现集合、逐Test单位、native结果四表双向相等；平台不支持的真实root/kernel动作在L6格明确验证，不能用L2假backend签L6。四套默认无tag时只含hermetic/embedded可运行部分，不下载、不Skip专门要求的供给。

P40实际HostBindings的唯一backend分配：Inspect=P02；Stage/Promote/UpdateSupport=P04；FenceStart/StartWarm/ReleaseStart/RetireWarm=P05；SignalLDM/SignalStop/SignalGraceful=P02；SwitchIngress=P07；FinalizeStorage=P09；RecoverHost=P04；RotatePolicy=P03；LaunchClaim/LaunchAck=P06。P04持有共同journal顺序，backend不能绕它直接接socket。P10/P13/P20是非root计划/业务证明consumer，不进入root依赖闭包。T91必须从真实main建立同一表并命中每一op，逐个漏bind/错handler/连stub都红，不只检查一个声明数组。P05停warm必须由P02验证精确pidfd与P04当前零责任receipt后执行，不能另按当前unit name误停继任。

## §11 接缝只有一个责任人，DAG与合流顺序

“owner”是交付与修复责任，调用方/validator可以不同；单行不能由两个包各等对方。下表artifact主owner唯一，§12的文件manifest是编辑权威。

| 接缝artifact | 唯一producer owner | consumer / 独立签字者 |
|---|---|---|
| P0 signatures、strict codec、失败桩与HostOp表 | P00 | P34 compile/scanner；各实际factory |
| immutable archive/binary catalog与actual prev兼容输入 | P01 | P26供给，P28/P29真实跨代写后读 |
| before/after PID/socket/cgroup/exe identity | P02 | P32独立observer；P10/P13不能用自己status代签 |
| root ticket/policy/key loader与撤销 | P03 | P40实际root，P31权限反例 |
| root intent/effect/result journal与worker stop/join | P04 | P40组装，P31丢reply/父死 |
| startup许可与effective systemd闭合 | P05 | P06 exec入口，P40注册，P31有效配置 |
| permanent lifetime OFD/legacy guard/exec退出 | P06 | P28 stock进程与锁反例 |
| canonical nft generation及root-mark bypass | P07 | P32从外部/本机/hairpin双栈观察 |
| Caddy route/admin配置 | P08 | P29真实load，P31权限 |
| old-exit后ACME final tree/fsync/selector | P09 | P40暴露FinalizeStorage，P29/86.I晚写 |
| Caddy/NATS phase顺序与完成判定 | P10 / P13，各自一组件 | P20注册；P29/P28独立验证 |
| paginated asset readiness reader | P11 | P18提供真实seal；P28并发资产反例 |
| SYS principal配置roundtrip/最小permissions | P12 | P03 keypolicy；P28两版reload与stepdown |
| 每代raw dial取消与mutex worker join | P14 | P15迁移；P30真WSS write-wedge |
| 全NC registry/successor/latejoin/return barrier | P15 | P22/P23注册全部创建点；P30漏dataNC反例 |
| signed old→new route/original8s bridge | P16 | P23 agent执行；P30真reaper |
| ordinary auth gate及normalqueue generation | P17 | P22冷/热/普通reconnect接线；P30删正常queue |
| pre-repair与全JS mutation seal实际producer表 | P18 | P22所有audit/GC/reconcile入口；P19 transfer入口；P28独立原ACK读回 |
| S7具体API→中立Transfers映射 | P19 | P30原NUID/consumer责任；不能要求S7完成S9迁移 |
| S8通用domain→S9组件/status/support映射 | P20 | P22/P24运行入口；P31旧worker退休 |
| public wire/subscription/ACL/admin投影 | P21 | P22/P23真实绑定；P35原wire root账本 |
| broker生产composition与全部调用根 | P22 | P34 runtime gate；P28/P30 |
| agent/ctl生产composition与所有NC | P23 | P34 runtime gate；P30 |
| CLI flags/error/rotate-policy/rotation guide | P24 | P35 golden；P31真实权限 |
| 所有installer目标和never-start/dry-run语义 | P25 | P31行为；P37实际renderer lint |
| 三binary release + 四第三方件 + image guard | P26 | P31真实安装；P37逐COPYstale controls |
| 每个test child/孙进程父死与reap | P27 | 各integration包调用；P36matrix超时集成 |
| 活业务fixture executable/schema/observation字段 | P32 | P30/P33；P19只交S7映射，不代写夹具 |
| 85/86/legacy控件及32/73/98部署断言 | P33 | P36原生ledger；P37hermetic自检 |
| scanner路径/调用根/结构/文档门 | P34 | P35库存，P39实际收据 |
| 所有共享wire/image/private/enum/error/leak/Test golden行 | P35 | 各producer提行，不拥有golden编辑权 |
| 每个-race矩阵单位、预算、五基线、成本行 | P36 | P37ledger闭合；P39运行 |
| lint targetset/run-all/script↔registry接线 | P37 | P34gate registry；P39集中gates |
| 活文档/支持范围/tasklist具体claim | P38 | P39提供事实；P34拒无receipt勾选 |
| 最终source→运行→oracle→native收据索引 | P39 | lead最终审查；无收据不close |
| 真实root main→17个HostOp backend | P40 | P31集成；T91/T92拒任何漏bind/stub |

### 11.1 编译DAG与测试DAG

先落P00唯一合同commit与可编译失败桩；接着每个leaf可针对P00 stub编译/验证拒绝，不因上游未完成阻塞所有开发。编译通过不等于支持能力可开启。多个包属于同Go package时仍只编辑自身列出的文件，通过P0 interface注入，禁止各自添加同名类型/全局单例。

~~~mermaid
flowchart LR
  P00 --> Pure["P01 P02 P03 P08 P11 P14 P16 P20 P21 P27"]
  P00 --> Gates["P34 scanner schema"]
  Pure --> Root["P04 → P05 P06 P07 P09"]
  Pure --> Move["P15 P17 P18 → P19"]
  Root --> Drivers["P10 P13"]
  Move --> Drivers
  Root --> Supply["P25 P26"]
  Drivers --> Host["P22 P23 → P24"]
  Supply --> P40
  Host --> Integration["P28 P29 P30 P31"]
  P40 --> Integration
  P27 --> P32
  P32 --> Integration
  Integration --> P33
  P33 --> P36
  P36 --> P37
  P37 --> P35
  Gates --> P35
  P35 --> P38
  P38 --> P39
~~~

箭头只画主干，§12逐包deps才是精确集合。P20可先写通用adapter测试但依赖S8实际合同才签生产；P19/P22/P23等S7交还；P32等S7真实fixture才签live；P28–P31等实际运行件，不能靠stub跑“integration”绿。

同一包的**unit/codec测试**在其deps已落即可通过；依赖真实kernel/systemd/跨版本/活业务的证明，必须等表内指定integration包及供给到齐。P25/P26不拥有P31测试，因此不存在“installer先等root integration、root integration又等installer”的实施环：先交文件/纯渲染，再P40组装，再P31行为验收。P34先落scanner及controls，扫描全树的验收当然等实际生产接线；P35是最后共享库存writer，不要求leaf先伪造golden才能编译。

P36/P37/P35按矩阵→runner/lint→库存顺序完成，P38先写NOT_RUN文档，P39将真实结果回交P38单一writer签claim；P39不越权改其文件。任何生产/配置/oracle改动后由责任owner和P39列出失效收据，局部重验后再同候选三硬闸/gates，不能用旧绿覆盖新树。


## §12 并行工作包与唯一文件manifest

共有41包、308个唯一文件路径（含测试、golden、文档）；文件数不是production-file预算。下列列表是**未来实施**的编辑分配，本规划只写指定候选文件。未列文件默认Δ0；不得为快过编译擅改别人的文件，发现遗漏由P00/lead改同一manifest并转移唯一owner。deps表示代码/合同必须落地，不要求前包所有跨层验收已签；测试先决条件另列，避免把最后集成的绿作为早期接口落地条件。

I=实际inventory row；L=直接shared goroutine+FD leak anchor（≥5轮、真实exercise）；R=原始ledger/收据行。每个owner交自己的行内容，P35/P36是共享文件唯一writer。M号是变异责任族，各族每个反例都须独立执行、保存diff和原生rc；重复引用M号表示另一层重验，**产品变异实现owner仍是首次定义该族的包**。部署故障/负控由P33执行，不授权改变独立judge或expectations。

| 包 | 难度 | 文件数 | 代码/合同依赖 |
|---|---|---:|---|
| P00 合同与失败桩 | HARD | 8 | — |
| P01 制品与版本目录 | NORMAL | 7 | P00 |
| P02 物理进程与连接观察 | HARD | 6 | P00 |
| P03 权限 IPC 与凭据策略 | HARD | 8 | P00 |
| P04 root worker 与持久事务 | HARD | 7 | P00,P02,P03 |
| P05 systemd 与首次启用 | HARD | 5 | P00,P02,P03,P04 |
| P06 stock exec launcher | HARD | 7 | P00,P02,P04 |
| P07 canonical 入口切换 | HARD | 5 | P00,P02,P04 |
| P08 Caddy 配置与 Unix admin | HARD | 5 | P00 |
| P09 ACME 当前态迁移 | HARD | 4 | P00,P02,P04,P08 |
| P10 Caddy 组件状态机 | HARD | 5 | P00,P01,P02,P04,P05,P07,P08,P09,P15,P16 |
| P11 JS 全资产 readiness | HARD | 6 | P00 |
| P12 SYS principal 与配置保留 | HARD | 8 | P00,P03 |
| P13 NATS 组件状态机 | HARD | 5 | P00,P01,P02,P04,P05,P06,P11,P12,P15,P16,P17,P18 |
| P14 有界 NC transport | HARD | 12 | P00 |
| P15 NC cohort 与迁移 | HARD | 5 | P00,P02,P14 |
| P16 S9 route bridge | HARD | 4 | P00 |
| P17 普通重连 auth-first | HARD | 5 | P00,P14 |
| P18 ACK 恢复与资产写门 | HARD | 11 | P00,P11 |
| P19 S7 continuation 适配 | HARD | 4 | P00,P15,P18；S7交还 |
| P20 S8 事务与组件注册 | HARD | 5 | P00；S8通用合同交还 |
| P21 wire、ACL 与 admin 接口 | HARD | 14 | P00 |
| P22 broker 生产接线 | HARD | 13 | P00,P11–P21；S6/S7宿主交还 |
| P23 agent/ctl 连接接线 | HARD | 11 | P00,P14–P19,P21；S7宿主交还 |
| P24 CLI 与错误呈现 | NORMAL | 11 | P00,P20,P21,P22 |
| P25 installer 文件与 retrofit 模板 | HARD | 1 | P00,P03,P05,P06,P08,P09,P12；真实安装测试等P26/P40 |
| P26 release 与双版本供给 | HARD | 12 | P00,P01,P04,P06 |
| P27 测试进程监督器 | HARD | 8 | P00 |
| P28 NATS 集成 | HARD | 4 | P01–P06,P11–P24,P27,P40；S6/S7/S8收据 |
| P29 Caddy 集成 | HARD | 4 | P01–P10,P14–P24,P27,P40；S6/S7/S8收据 |
| P30 业务与跨层接缝集成 | HARD | 4 | P19,P22,P23,P27,P32；S6/S7/S8收据 |
| P31 权限、安装与支持制品集成 | HARD | 4 | P01–P09,P12,P20,P25,P26,P27,P40 |
| P32 活业务 drill 夹具 | HARD | 5 | P00,P02,P27；S7 fixture交还 |
| P33 85/86 与既有部署回归 | HARD | 6 | P25,P26,P28–P32；登记闭合由P35–P37验 |
| P34 结构、分层与接缝扫描门 | HARD | 15 | P00；接线扫描待P01–P33 |
| P35 库存、enum、error 与 leak 账本 | HARD | 21 | P00–P34,P36,P37,P40 |
| P36 race 矩阵与部署 ledger | HARD | 16 | P00,P27–P33,P40 |
| P37 hermetic 与 installer lint | HARD | 7 | P00,P25,P26,P32,P33,P36 |
| P38 正式文档 | NORMAL | 14 | P00,P33–P37 |
| P39 最终候选收据与关闭 | HARD | 2 | P00–P38,P40；S6/S7/S8正式close |
| P40 root 可执行接缝集成 | HARD | 4 | P00–P09,P12,P20,P25,P26,P27 |

### P00 · 合同与失败桩 · HARD

目标：先落永久合同、严格 codec、失败桩；不临时占有下游文件。\
独占测试：T01/T02；FuzzInfraRequest。\
测试通过前置：P00；合同/codec/无副作用桩独立绿，不等实现。\
变异：M01/M02；stub 被接入生产必须红。\
库存/资源/收据：I:Request/Receipt/Plan/Status/codec；L:TestInfraContractStubLifecycle→exerciseContractStub；R:P0签名冻结。

文件（只此包可编辑）：
~~~text
internal/infracontract/types.go
internal/infracontract/host.go
internal/infracontract/interfaces.go
internal/infracontract/codec.go
internal/infracontract/contracts_test.go
internal/infracontract/codec_test.go
internal/infracontract/contractstub/stub.go
internal/infracontract/contractstub/stub_test.go
~~~

### P01 · 制品与版本目录 · NORMAL

目标：真实相邻版本、最终 bytes、兼容报告；版本命令只交 supervisor。\
独占测试：T03/T04（兼容接口/失败控制；真正回读由P28/P29独立T60/T65/T67签）。\
测试通过前置：P00；T04只签兼容验证接口，真实current-state回读另等P28/P29。\
变异：M03 archive当binary/坏prev/重复checksum。\
库存/资源/收据：I:Artifact/Compatibility；L:TestInfraArtifactLifecycle→exerciseArtifact；R:官方清单/两版SHA。

文件（只此包可编辑）：
~~~text
internal/infraartifact/catalog.go
internal/infraartifact/verify.go
internal/infraartifact/probe.go
internal/infraartifact/catalog_test.go
internal/infraartifact/verify_test.go
internal/infraartifact/probe_test.go
internal/infraartifact/testdata/catalog.json
~~~

### P02 · 物理进程与连接观察 · HARD

目标：pidfd、/proc exe、sock_diag/conntrack 的完整责任集合；不依赖产品 status。\
独占测试：T05/T06。\
测试通过前置：P00,P27；Linux物理观察用真实短child，非Linux拒绝；部署cgroup全集等P31。\
变异：M04 disk-only/空census/同SHA误判/URL假迁移。\
库存/资源/收据：I:Process/Socket/Census；L:TestInfraIdentityLifecycle→exerciseIdentity；R:每步before/after。

文件（只此包可编辑）：
~~~text
internal/infrahost/identity_linux.go
internal/infrahost/identity_other.go
internal/infrahost/census_linux.go
internal/infrahost/census_other.go
internal/infrahost/identity_test.go
internal/infrahost/census_test.go
~~~

### P03 · 权限 IPC 与凭据策略 · HARD

目标：SO_PEERCRED、全请求签名、key revision、有限读与并发；neutral leaf。\
独占测试：T07/T08。\
测试通过前置：P00,P27；实际root/多UID授权等P31。\
变异：M05 任一未签字段/跨boot/旧key/半包。\
库存/资源/收据：I:IPC/credential loader/policy；L:TestInfraIPCLifecycle→exerciseIPC；R:authority epoch。

文件（只此包可编辑）：
~~~text
internal/infrapriv/ipc.go
internal/infrapriv/client_linux.go
internal/infrapriv/client_other.go
internal/infrapriv/policy.go
internal/infrapriv/credentials.go
internal/infrapriv/ipc_test.go
internal/infrapriv/policy_test.go
internal/infrapriv/credentials_test.go
~~~

### P04 · root worker 与持久事务 · HARD

目标：root worker核心、闭合verbs、CAS/replay、不可变promotion；最终root可执行组装归P40。\
独占测试：T09/T10/T11。\
测试通过前置：P00,P02,P03,P27；worker/CAS可独立；支持两代真入口等P31/P40。\
变异：M06 迟到unflip/TOCTOU/worker留存/越权路径。\
库存/资源/收据：I:root journal/verb-effect表；L:TestInfraHostLifecycle→exerciseHost；R:root每次副作用。

文件（只此包可编辑）：
~~~text
internal/infrahost/worker.go
internal/infrahost/journal.go
internal/infrahost/promote_linux.go
internal/infrahost/promote_other.go
internal/infrahost/worker_test.go
internal/infrahost/journal_test.go
internal/infrahost/promote_test.go
~~~

### P05 · systemd 与首次启用 · HARD

目标：有效 unit 全量验证、inactive socket bootstrap、启动失败预算；不动活业务。\
独占测试：T12/T13。\
测试通过前置：P00,P02,P03,P04；纯effective-unit表；真实socket bootstrap等P31/P40。\
变异：M07 累加cap/+Exec/EnvironmentFile/未激活假ready。\
库存/资源/收据：I:effective unit/policy targets；L:TestInfraUnitLifecycle→exerciseUnit；R:安装至ready五态。

文件（只此包可编辑）：
~~~text
internal/infrahost/unit_linux.go
internal/infrahost/unit_other.go
internal/infrahost/bootstrap.go
internal/infrahost/unit_test.go
internal/infrahost/bootstrap_test.go
~~~

### P06 · stock exec launcher · HARD

目标：原地exec stock NATS/Caddy，NATS永久锁OFD、legacy唯一owner及两组件启动围栏。\
独占测试：T14/T15。\
测试通过前置：P00,P02,P04,P27；exec/OFD机制；stock两版及legacy真启动由P28/T63验证。\
变异：M08 最后锁引用/父死/锁替换/alias双开。\
库存/资源/收据：I:store identity/FD allowlist；L:TestInfraLaunchLifecycle→exerciseLaunch；R:锁连续性与映像退出。

文件（只此包可编辑）：
~~~text
cmd/tether-infra-launch/main.go
cmd/tether-infra-launch/main_test.go
internal/infralaunch/launch.go
internal/infralaunch/store_linux.go
internal/infralaunch/store_other.go
internal/infralaunch/launch_test.go
internal/infralaunch/store_test.go
~~~

### P07 · canonical 入口切换 · HARD

目标：产品 nft 原子规则、v4/v6 PREROUTING/OUTPUT、SO_MARK bypass、入站封口。\
独占测试：T16/T17。\
测试通过前置：P00,P02,P04；规则/CAS与bypass判据；真实nft原子切换等P29/P31/86。\
变异：M09 main坏shadow好/漏IPv6/hairpin/lateSYN。\
库存/资源/收据：I:rule generation/census seal；L:TestIngressSwitchLifecycle→exerciseIngress；R:canonical逐次TLS。

文件（只此包可编辑）：
~~~text
internal/ingressswitch/switch.go
internal/ingressswitch/rules_linux.go
internal/ingressswitch/rules_other.go
internal/ingressswitch/switch_test.go
internal/ingressswitch/rules_test.go
~~~

### P08 · Caddy 配置与 Unix admin · HARD

目标：固定模块/路由保序、无ACME shadow配置、同socket凭据绑定reload。\
独占测试：T18/T19。\
测试通过前置：P00,P27；配置/admin机制；stock load及旧root权限等P29/P31。\
变异：M10 catch-all抢/sub/坏cert/root任意load/重拨误停。\
库存/资源/收据：I:config/secret path/admin协议；L:TestCaddyAdminLifecycle→exerciseCaddyAdmin；R:配置digest。

文件（只此包可编辑）：
~~~text
internal/caddyconfig/config.go
internal/caddyconfig/routes.go
internal/caddyconfig/admin.go
internal/caddyconfig/config_test.go
internal/caddyconfig/admin_test.go
~~~

### P09 · ACME 当前态迁移 · HARD

目标：warm只读与old-exit最终delta、owner/fsync/selector、唯一续期writer。\
独占测试：T20/T21。\
测试通过前置：P00,P02,P04,P08,P27；树/fence失败窗；真实CA及always启动竞态等P29/P31/86.I。\
变异：M11 复制后晚写/自动启动抢跑/回滚旧snapshot。\
库存/资源/收据：I:ACME树/秘密引用/锁；L:TestACMELifecycle→exerciseACME；R:最终delta及renewal。

文件（只此包可编辑）：
~~~text
internal/infrahost/acme.go
internal/infrahost/acme_linux.go
internal/infrahost/acme_other.go
internal/infrahost/acme_test.go
~~~

### P10 · Caddy 组件状态机 · HARD

目标：reload/binary/live retrofit/迁回/旧及临时入口退休；过期保健康入口。\
独占测试：T22/T23。\
测试通过前置：全部实施deps；leaf表可注入，Caddy真实退场/超期保业务等P29/86.F。\
变异：M12 错invocation/仅WSS零/到期杀shadow/提前completed。\
库存/资源/收据：I:Transition/Phase/old obligations；L:TestCaddyDriverLifecycle→exerciseCaddyDriver；R:Caddy各barrier。

文件（只此包可编辑）：
~~~text
internal/infraupgrade/caddy.go
internal/infraupgrade/caddy_reload.go
internal/infraupgrade/caddy_return.go
internal/infraupgrade/caddy_test.go
internal/infraupgrade/caddy_reload_test.go
~~~

### P11 · JS 全资产 readiness · HARD

目标：B/S/JS三拓扑、分页/账号闭合、placement current/quorum、真实auth/WSS证据。\
独占测试：T24/T25/T26。\
测试通过前置：P00,P27；embedded当前pin，真实两版/多host由P28。\
变异：M13 count=3/漏页/忽略consumer/错host/R2。\
库存/资源/收据：I:Account/Group/AssetSeal；L:TestInfraReadinessLifecycle→exerciseReadiness；R:全资产快照。

文件（只此包可编辑）：
~~~text
internal/infrareadiness/jetstream.go
internal/infrareadiness/topology.go
internal/infrareadiness/assets.go
internal/infrareadiness/auth.go
internal/infrareadiness/readiness_test.go
internal/infrareadiness/assets_test.go
~~~

### P12 · SYS principal 与配置保留 · HARD

目标：最窄$SYS读/stepdown凭据；parse/render/merge/reconcile闭合，旧配置无隐藏restart。\
独占测试：T27/T28。\
测试通过前置：P00,P03,P27；embedded当前pin及roundtrip；两版无隐藏restart/真stepdown由P28/85。\
变异：M14 $G冒充SYS/凭据被render抹掉/改SystemAccount/always伪reload。\
库存/资源/收据：I:principal/config/key路径；L:TestSystemPrincipalLifecycle→exerciseSystemPrincipal；R:两版reload和真stepdown。

文件（只此包可编辑）：
~~~text
internal/natsconf/preflight.go
internal/natsconf/render.go
internal/natsconf/takeover.go
internal/natsconf/render_desired.go
internal/natsconf/reconcile.go
internal/natsconf/system_principal.go
internal/natsconf/system_principal_test.go
internal/natsconf/system_reload_test.go
~~~

### P13 · NATS 组件状态机 · HARD

目标：N3预迁移→LDM/stop、N1无LDM同store、逐步current、失败halt/current rollback。\
独占测试：T29/T30/T31。\
测试通过前置：全部实施deps；状态表；N1/N3/quorum真实出口等P28/85。\
变异：M15 LDM早于准备/N1用LDM/N2强行/oldalive/后续继续roll。\
库存/资源/收据：I:NATS phase/signal/health；L:TestNATSDriverLifecycle→exerciseNATSDriver；R:N3/N1/N1V。

文件（只此包可编辑）：
~~~text
internal/infraupgrade/nats.go
internal/infraupgrade/nats_single.go
internal/infraupgrade/nats_recover.go
internal/infraupgrade/nats_test.go
internal/infraupgrade/nats_single_test.go
~~~

### P14 · 有界 NC transport · HARD

目标：DNS/TCP/TLS/WSS全部可取消；原代raw打断后join，业务owner不随NC死。\
独占测试：T32/T33。\
测试通过前置：P00,P27；真实DNS/proxy/TLS/WSS阻塞短测试，生产NC接线等P30。\
变异：M16 nc.mu真堵/req.Write真堵/lateworker/10+10伪5s。\
库存/资源/收据：I:attachment/raw generation；L:TestInfraTransportLifecycle→exerciseTransport；R:5s真实上界。

文件（只此包可编辑）：
~~~text
internal/infratransport/dial.go
internal/infratransport/connection.go
internal/infratransport/move.go
internal/infratransport/dial_test.go
internal/infratransport/move_test.go
internal/connrecovery/teardown.go
internal/connrecovery/teardown_test.go
internal/proxydial/proxydial.go
internal/proxydial/budget.go
internal/proxydial/socks5.go
internal/proxydial/httpconnect.go
internal/proxydial/proxydial_test.go
~~~

### P15 · NC cohort 与迁移 · HARD

目标：所有ctl/agent/transfer NC注册、seal latejoin、Installed/Attached/Returned真ACK。\
独占测试：T34/T35。\
测试通过前置：P00,P02,P14,P27；registry/admission；全部实际dial根等P30。\
变异：M17 漏独立dataNC/丢prepareACK/零census后latejoin/仅pool。\
库存/资源/收据：I:participant/ACK generation；L:TestIngressMoveLifecycle→exerciseMove；R:迁出/迁回双ledger。

文件（只此包可编辑）：
~~~text
internal/ingressmove/cohort.go
internal/ingressmove/move.go
internal/ingressmove/admission.go
internal/ingressmove/cohort_test.go
internal/ingressmove/move_test.go
~~~

### P16 · S9 route bridge · HARD

目标：独立signed old→new purpose，8s原deadline不续期，home/epoch保持与正常reaper。\
独占测试：T36/T37。\
测试通过前置：P00；proof/时钟；真实reaper集成等P30。\
变异：M18 旧S6票据复用/每hop续8s/过期仍护航。\
库存/资源/收据：I:RouteProof/hold purpose；L:TestInfraMaintenanceLifecycle→exerciseMaintenance；R:8s和60s后真实判词。

文件（只此包可编辑）：
~~~text
internal/inframaint/proof.go
internal/inframaint/hold.go
internal/inframaint/proof_test.go
internal/inframaint/hold_test.go
~~~

### P17 · 普通重连 auth-first · HARD

目标：auth-only+受门控businessNC，普通queue的member/agent新CONNECT/PONG和真实RPC。\
独占测试：T38/T39。\
测试通过前置：P00,P14,P27；真实embedded正常queue；生产cold/hot/ordinary同根等P30。\
变异：M19 删normalqueue/辅助probe冒充/autoSUB早于auth/漫游peer。\
库存/资源/收据：I:auth phase/generation/SUB集合；L:TestInfraAuthLifecycle→exerciseAuth；R:auth→business时序。

文件（只此包可编辑）：
~~~text
internal/infraauth/owner.go
internal/infraauth/gate.go
internal/infraauth/probe.go
internal/infraauth/owner_test.go
internal/infraauth/probe_test.go
~~~

### P18 · ACK 恢复与资产写门 · HARD

目标：产品统一JS变更入口、短asset seal、ACK-prefix-before-replay与audit读取。\
独占测试：T40/T41。\
测试通过前置：P00,P11,P27；真实embedded原始ACK/资产竞态；S7多writer等P30。\
变异：M20 先repair后验/仅LastSeq/漏producer/create-delete-recreate。\
库存/资源/收据：I:ACK multiset/asset generation/producer roots；L:TestInfraRecoveryLifecycle→exerciseRecovery；R:pre-repair原始读回。

文件（只此包可编辑）：
~~~text
internal/infrarepair/gate.go
internal/infrarepair/receipts.go
internal/infrarepair/assets.go
internal/infrarepair/gate_test.go
internal/infrarepair/assets_test.go
internal/jsstream/jsstream.go
internal/jsstream/replicas.go
internal/brokercontrol/audit_publisher.go
internal/brokercontrol/infra_recovery_test.go
internal/infrarepair/legacy_streams.go
internal/infrarepair/legacy_streams_test.go
~~~

### P19 · S7 continuation 适配 · HARD

目标：将实际S7 worker/tracker/writer/reader/courier接迁移门；不冒称S7已实现。\
独占测试：T42/T43。\
测试通过前置：P00,P15,P18,S7实际API；真实原consumer/四业务等P30。\
变异：M21 新NUID/reader未退役/失ACK先失败/重置budget。\
库存/资源/收据：I:S7→S9映射/ObservationSchema；L:TestInfraTransferLifecycle→exerciseTransfer；R:S7 source/API/fixture收据。

文件（只此包可编辑）：
~~~text
internal/infratransfer/adapter.go
internal/infratransfer/checkpoint.go
internal/infratransfer/adapter_test.go
internal/infratransfer/checkpoint_test.go
~~~

### P20 · S8 事务与组件注册 · HARD

目标：S9注册nats/caddy、状态/恢复/rootjournal映射、support制品两代、image互斥。\
独占测试：T44/T45。\
测试通过前置：P00,S8通用API；真实broker/权限/support接线等P22/P31/P40。\
变异：M22 stub可激活/错domain/第三代/旧worker未退。\
库存/资源/收据：I:S8 adapter/status projection；沿TestNATSDriverLifecycle和TestCaddyDriverLifecycle；R:三完成点。

文件（只此包可编辑）：
~~~text
internal/infraupgrade/service.go
internal/infraupgrade/transaction.go
internal/infraupgrade/support.go
internal/infraupgrade/service_test.go
internal/infraupgrade/transaction_test.go
~~~

### P21 · wire、ACL 与 admin 接口 · HARD

目标：独立infra subject、严格签名、sanitized health/admin包级函数、默认旧语义。\
独占测试：T46/T47。\
测试通过前置：P00；wire/ACL纯测；真实旧artifact和生产SUB表等P26/P30。\
变异：M23 旧reload夹Infra/普通member/root越权/无subscriber授权。\
库存/资源/收据：I:wire/SUB/ACL/admin既有roots；L:TestInfraAdminLifecycle→exerciseInfraAdmin；R:N-1四象限。

文件（只此包可编辑）：
~~~text
internal/proto/infra_upgrade.go
internal/proto/subjects.go
internal/proto/alerts.go
internal/proto/codes.go
internal/proto/infra_upgrade_test.go
internal/auth/permissions.go
internal/auth/acl_reconcile_test.go
internal/auth/infra_permissions_test.go
internal/adminsock/protocol.go
internal/adminsock/server.go
internal/adminsock/infra_test.go
internal/proto/cluster_health.go
internal/adminsock/infra_protocol.go
internal/auth/infra_permissions.go
~~~

### P22 · broker 生产接线 · HARD

目标：冷/热/普通NC重连同表，local-only，所有JS producer入门，host状态移最终leaf。\
独占测试：T48/T49。\
测试通过前置：实施deps+S6/S7宿主交还；实际caller行为绿；live业务总验收等P28/P30。\
变异：M24 漏cold/hot/reconnect/镜像与infra重叠/漏GC。\
库存/资源/收据：I:接线descriptor/runtime roots；L:TestBrokerInfraLifecycle→exerciseBrokerInfra；R:全部真实调用点。

文件（只此包可编辑）：
~~~text
internal/broker/broker.go
internal/broker/reexec.go
internal/broker/authcallout.go
internal/broker/cluster_upgrade_trigger.go
internal/broker/cluster_health.go
internal/broker/admin.go
internal/broker/topology_reconcile.go
internal/broker/reconcile_passes.go
internal/broker/transfer.go
internal/broker/transfer_reconcile.go
internal/broker/infra_wiring_test.go
internal/infraattach/broker.go
internal/infraattach/broker_test.go
~~~

### P23 · agent/ctl 连接接线 · HARD

目标：普通control/dataNC同cohort、route bridge/健康代际、不杀原data owner，capability真安装。\
独占测试：T50/T51。\
测试通过前置：实施deps+S7宿主交还；P27进程监督；完整cohort等P30。\
变异：M25 farewell假capability/NC rebuild停SS/过期hold/漏ctl。\
库存/资源/收据：I:route/normalREGISTER/ctx；L:TestAgentInfraLifecycle→exerciseAgentInfra；TestCLIInfraLifecycle→exerciseCLIInfra；R:客户端原身份。

文件（只此包可编辑）：
~~~text
internal/agent/agent.go
internal/agent/roster.go
internal/agent/conn_teardown.go
internal/agent/upgrade_image.go
internal/agent/infra_wiring_test.go
internal/cli/natsconn.go
internal/cli/infra_connection_test.go
internal/infraattach/client.go
internal/infraattach/client_test.go
internal/agent/transfer.go
cmd/tether/ctl_connect.go
~~~

### P24 · CLI 与错误呈现 · NORMAL

目标：既有cmd文件加component、single本地入口/status/bootstrap指引，逻辑在leaf；rotation guide接显式rotate-policy。\
独占测试：T52/T53。\
测试通过前置：实施deps；command/error纯测试；真实权限由P31。\
变异：M26 默认改语义/infra仍跑brokerleg/错exit/状态当completed。\
库存/资源/收据：I:CLI/exit/hint/emitter；L:TestInfraCLILifecycle→exerciseInfraCLI；R:命令表。

文件（只此包可编辑）：
~~~text
cmd/tether/cluster_upgrade.go
cmd/tether/cluster_upgrade_drive.go
cmd/tether/serve.go
cmd/tether/admin.go
cmd/tether/exitcode.go
cmd/tether/error_hints.go
cmd/tether/infra_upgrade_test.go
internal/infracli/command.go
internal/infracli/render.go
internal/infracli/command_test.go
cmd/tether/cluster_rotation.go
~~~

### P25 · installer 文件与 retrofit 模板 · HARD

目标：fresh/reinstall/stage-only/内容ownership卸载、全部seam、never-start；shell唯一owner。\
独占测试：T54由P31/P37验收。\
测试通过前置：不拥有测试函数；P37纯渲染/lint先验，P26/P40落后由P31/T54真安装。\
变异：M27 dryrun写盘/启动任意新进程/覆盖busy selector/删operator文件。\
库存/资源/收据：I:managed targetset/metadata；无常驻L；R:32生命周期。

文件（只此包可编辑）：
~~~text
scripts/install.sh
~~~

### P26 · release 与双版本供给 · HARD

目标：helper/launcher真实archive，两NATS两Caddy，完整COPY/image guard，IPv6/nft环境供应。\
独占测试：T55/T56由P31/P37验收。\
测试通过前置：不拥有Go测试函数；P37纯供给控件先验，P31/T55/T56+P40真归档/运行。\
变异：M28 每个COPY输入单独stale/skip-download漏件/同源伪旧。\
库存/资源/收据：I:release/COPY/source/target manifest；无常驻L；R:冷/热供应成本。

文件（只此包可编辑）：
~~~text
Makefile
build/goreleaser.yaml
test/simcluster/Dockerfile
test/simcluster/simcluster
test/simcluster/remote.sh
test/simcluster/image/provision-node.sh
test/simcluster/lib/docker.sh
test/simcluster/lib/stage-caddy.sh
test/simcluster/lib/caddy-artifact.py
test/simcluster/lib/stage-infra.sh
test/simcluster/lib/infra-artifact.py
test/simcluster/lib/infra-image-manifest.py
~~~

### P27 · 测试进程监督器 · HARD

目标：所有外置binary经父死管道+独立subreaper监督，超时/启动竞态kill/reap。\
独占测试：T57/T58。\
测试通过前置：P00；真实杀父/timeout/setsid孙及start竞态，不依赖任何S9产品实现。\
变异：M29 仅Pdeathsig/test-timeout留孙/丢Wait/父死窗口。\
库存/资源/收据：I:child/FD/pipe责任；L:TestInfraHarnessLifecycle→exerciseHarness；R:父测试被杀后的零后代。

文件（只此包可编辑）：
~~~text
test/infraharness/process.go
test/infraharness/process_linux.go
test/infraharness/process_other.go
test/infraharness/process_test.go
test/infraharness/artifact.go
test/infraharness/observe.go
test/infraharness/observe_test.go
test/infraharness/deathwatch.go
~~~

### P28 · NATS 集成 · HARD

目标：真实两版/N3/N1/sole、lostACK/503/auth/replicas/当前store回滚接缝。\
独占测试：T59–T63。\
测试通过前置：全部实施deps+P26已校验两NATS供给+S6/S7/S8实际收据；root/kernel项在明确L6 cell。\
变异：M04/M08/M13–M21真实进程变异。\
库存/资源/收据：I:全输入/输出映射；L:TestInfraNATSLifecycle→exerciseNATSLifecycle（≥5真实短轮）；R:exit1/2/3。

文件（只此包可编辑）：
~~~text
test/infranats/embedded_test.go
test/infranats/external_test.go
test/infranats/recovery_test.go
test/infranats/lock_test.go
~~~

### P29 · Caddy 集成 · HARD

目标：高端口stock进程/Unixadmin/原WSS；部署层unit/nft/ACME另有86。\
独占测试：T64–T67。\
测试通过前置：全部实施deps+P26两Caddy/测试CA供给+S6/S7/S8实际收据；nft/systemd项在86。\
变异：M09–M12/M17；lateUpgrade/慢HTTP/旧代自然退出。\
库存/资源/收据：I:listener/socket/config/ACME联合收据；L:TestInfraCaddyLifecycle→exerciseCaddyLifecycle（≥5真实短轮）；R:exit4。

文件（只此包可编辑）：
~~~text
test/infracaddy/reload_test.go
test/infracaddy/binary_test.go
test/infracaddy/retirement_test.go
test/infracaddy/acme_test.go
~~~

### P30 · 业务与跨层接缝集成 · HARD

目标：原SSH/TCP/SS/PTY/exec/4transfer与10/s；四层依赖选择，不执行S10五代。\
独占测试：T68–T71。\
测试通过前置：P19,P22,P23,P26,P27,P32+S6/S7/S8交还；B-6地址正负例在此包。\
变异：M16–M21/M24/M25；业务redial/10slot漏计。\
库存/资源/收据：I:requiredset/slot/effect/原NUID；L:TestInfraBusinessLifecycle→exerciseBusinessLifecycle（≥5轮）；R:B-6和三向cycle。

文件（只此包可编辑）：
~~~text
test/infrabusiness/continuity_test.go
test/infrabusiness/migration_test.go
test/infrabusiness/auth_test.go
test/infrabusiness/transfer_test.go
~~~

### P31 · 权限、安装与支持制品集成 · HARD

目标：非root测试精确syscall seam；真实UID/systemd/nft能力由86交收据。\
独占测试：T54/T55/T56/T72–T75。\
测试通过前置：全部实施deps+真实archive/固定多UID fixture；root/systemd/nft收据由86相应cell补齐。\
变异：M05–M11/M14/M22/M27/M28；越权canary/active-reinstall。\
库存/资源/收据：I:credential-loader-policy/支持两代；L:TestInfraPrivilegeLifecycle→exercisePrivilegeLifecycle（≥5轮）；R:安装权限出口。

文件（只此包可编辑）：
~~~text
test/infraprivilege/authority_test.go
test/infraprivilege/installer_test.go
test/infraprivilege/support_test.go
test/infraprivilege/rotation_test.go
~~~

### P32 · 活业务 drill 夹具 · HARD

目标：唯一可执行fixture owner：原requiredset/固定slots/迁移物理证据/S7 schema适配。\
独占测试：T76/T77。\
测试通过前置：P00,P02,P27即可schema/judge绿；真实live还等P19及S7 executable/schema/source receipt。\
变异：M30 空set/漏slot/repair掩盖/自动redial/半trace。\
库存/资源/收据：I:observation每字段producer→reader；L:进程经P27/部署cgroup；R:fixture本身先正负自检。

文件（只此包可编辑）：
~~~text
test/simcluster/drills/lib/hot_update_infra.sh
test/simcluster/drills/lib/hot_update_infra_probe.py
test/simcluster/drills/lib/hot_update_infra_workload.py
test/simcluster/tests/hot-update-infra-probe-test.py
test/simcluster/tests/hot-update-infra-fixture-test.sh
~~~

### P33 · 85/86 与既有部署回归 · HARD

目标：原生合法arms、正父全绿、真旧能力拒绝、32全部roots；73/98仅保原claim。\
独占测试：85/86/39NI/CI/32/73/98部署断言；T78–T81由P37独占。\
测试通过前置：P25,P26,P28–P32；P35–P37登记后部署才可签；不拥有T78–T81脚本自检。\
变异：M04/M12/M28/M30真实restage；不得将rc1洗3。\
库存/资源/收据：I:arm→scope→receipt；L:cleanup前产品退场；R:85/86/32/73/98。

文件（只此包可编辑）：
~~~text
test/simcluster/drills/85-hot-update-nats.sh
test/simcluster/drills/86-hot-update-caddy.sh
test/simcluster/drills/39-hot-update-legacy-n1.sh
test/simcluster/drills/32-install-lifecycle.sh
test/simcluster/drills/73-proxy-cluster-ha.sh
test/simcluster/drills/98-stuck-redial-recovery.sh
~~~

### P34 · 结构、分层与接缝扫描门 · HARD

目标：实际AST调用根/701行/全部legacy ceiling/依赖闭包/安装输出/预算pin控制。\
独占测试：T82/T83/T90。\
测试通过前置：P00即可controls；T82/T83扫已落P01–P33/P40；T90等P38真实文档，不挡提前落scanner。\
变异：M31 每门删除生产调用/错误路径/漏tag/空root正负。\
库存/资源/收据：I:scanner roots和origin映射；无runtime L；R:逐门边际秒数。

文件（只此包可编辑）：
~~~text
test/architecture/infra_contract_test.go
test/architecture/infra_runtime_test.go
test/architecture/structural_budget_test.go
test/architecture/layering_test.go
test/architecture/ctx_background_ledger_test.go
test/architecture/dataplane_lifetime_test.go
test/architecture/build_tags_test.go
test/architecture/upgrade_budget_pin_test.go
test/architecture/nats_server_pin_test.go
test/architecture/nats_ping_defaults_test.go
test/architecture/gate_registry_test.go
test/architecture/ci_workflow_test.go
test/architecture/infra_supply_test.go
test/architecture/broker_image_contract_test.go
test/architecture/testdata/broker_image_gate_rows.tsv
~~~

### P35 · 库存、enum、error 与 leak 账本 · HARD

目标：所有共享golden唯一writer；三向真实codec/loader/runtime库存，不放宽预算。\
独占测试：T84/T85。\
测试通过前置：P00–P34,P36,P37/P40代码已落；全部producer表到齐；无需等P39执行收据才更新实测库存。\
变异：M32 删字段/漏enum/死error/隐藏helper/少于5轮/旧scanner路径。\
库存/资源/收据：I:全部共享库存；L:逐package精确Test→exercise行；R:Δ和零Δ反向扫描。

文件（只此包可编辑）：
~~~text
internal/proto/wire_inventory_test.go
internal/proto/testdata/wire_field_inventory.txt
test/architecture/infra_inventory_test.go
test/architecture/image_field_inventory_test.go
test/architecture/broker_state_inventory_test.go
test/architecture/testdata/infra_inventory.tsv
test/architecture/testdata/image_field_inventory.txt
test/architecture/testdata/broker_state_inventory.txt
test/architecture/testdata/structural_budget_golden.txt
test/architecture/testdata/legacy_file_code_ceilings.tsv
test/architecture/legacy_ctx_background_sites.go
test/determinism/enum_switch_default_test.go
test/determinism/leak_assert_shape_test.go
test/determinism/testdata/test_function_inventory.txt
cmd/tether/error_code_coverage_test.go
cmd/tether/exit_class_family_test.go
cmd/tether/wire_code_namespaces_test.go
cmd/tether/error_hints_test.go
cmd/tether/testdata/command_tree_golden.txt
cmd/tether/testdata/command_tree_golden_runtime.txt
internal/upgradebudget/budget_test.go
~~~

### P36 · race 矩阵与部署 ledger · HARD

目标：whole-race声明+重套件逐Test分片，五部署资产闭合，不改native语义。\
独占测试：T86；T87账本由P37验。\
测试通过前置：P27–P33/P40代码已落即可tuple/partition/ledgercontrols；实际完整matrix由P39执行。\
变异：M33 漏tuple/窄run/少一Test/父判词吞红/预算漂移。\
库存/资源/收据：I:matrix/arm/ledger/budget；无runtime L；R:每unit≤600s和所有结果。

文件（只此包可编辑）：
~~~text
test/e2e/all_phases_test.go
test/e2e/parallel/shard.go
test/e2e/parallel/split.go
test/e2e/parallel/inventory_test.go
test/e2e/parallel/deadline_test.go
test/e2e/parallel/testdata/matrix_units_golden.txt
test/simcluster/expected-verdicts.tsv
test/simcluster/expected-verdicts-log.md
test/simcluster/tests/kept-sites.baseline.tsv
test/simcluster/hot-update-infra-ledger.tsv
test/simcluster/drills/lib/hot_update_budget.sh
internal/upgradebudget/infra.go
test/e2e/parallel/main.go
test/simcluster/tests/assert-identity.baseline.tsv
docs/reviews/simcluster-coverage-inventory.md
test/simcluster/drill-costs.tsv
~~~

### P37 · hermetic 与 installer lint · HARD

目标：exact模板targetset、多heredoc fail-closed、所有新脚本按shebang进入run-all。\
独占测试：T78–T81/T87/T88/T89。\
测试通过前置：P25,P26,P32,P33,P36代码/manifest；hermetic只读fixture，不依赖S6/S7 live环境。\
变异：M34 greedy吞A/伪quoted/漏新模板/漏run-all/stale输入。\
库存/资源/收据：I:target/script/source集合；无runtime L；R:hermetic单项边际。

文件（只此包可编辑）：
~~~text
test/simcluster/tests/lint-install.sh
test/simcluster/tests/lint-install-selftest.sh
test/simcluster/tests/hot-update-infra-supply-test.sh
test/simcluster/tests/hot-update-infra-drill-test.sh
test/simcluster/tests/hot-update-infra-ledger-test.sh
test/simcluster/tests/run-all.sh
test/architecture/simcluster_gate_set_test.go
~~~

### P38 · 正式文档 · NORMAL

目标：绑定状态机/权限/支持边界、纠正过时S6描述，过程文件归reviews；状态链接P39，不抢S10总勾。\
独占测试：文档交付；T90由P34独占。\
测试通过前置：不拥有测试函数；P34/T90在文档落后运行；P39回交真实结果前保NOT_RUN。\
变异：M35 无receipt勾选/旧路径/文档扩大支持面。\
库存/资源/收据：I:scope→claim→receipt；无runtime L；R:未验证诚实标注。

文件（只此包可编辑）：
~~~text
CLAUDE.md
docs/distributed-broker-architecture.md
docs/deploy-tier-gotchas.md
docs/broker-ops.md
docs/cluster-runbook.md
docs/usage.md
docs/testing-standards.md
test/README.md
test/simcluster/README.md
docs/reviews/hot-update-tasklist.md
docs/reviews/hot-update-stages-plan.md
docs/reviews/INDEX.md
docs/reviews/hot-update-infra-nats-caddy-plan.md
test/infraharness/README.md
~~~

### P39 · 最终候选收据与关闭 · HARD

目标：唯一最终收据汇总者；同候选 gates/drills/全部mutant/成本/platform，不代做产品。\
独占测试：五出口逐格AND。\
测试通过前置：P00–P38,P40+S6/S7/S8正式close；每个实际leaf/integration/drill/gate/platform receipt同候选。\
变异：任一缺source/running/oracle/native/mutation/原业务非空则拒签。\
库存/资源/收据：I:最终receipt index；无runtime L；R:全部NOT_RUN→实测后状态。

文件（只此包可编辑）：
~~~text
docs/reviews/hot-update-infra-evidence.md
docs/reviews/hot-update-infra-validation.md
~~~

### P40 · root 可执行接缝集成 · HARD

目标：组装真实root policy/worker/unit/nft/storage/launch/support，所有HostOp恰一backend；薄main与非Linux拒绝。\
独占测试：T91/T92；TestInfraRootLifecycle。\
测试通过前置：P00–P09,P12,P20,P25,P26,P27代码已落；17verbs/实际闭包绿后才可运行P31真入口。\
变异：M36 漏verb/连stub/业务init进入root/错handler。\
库存/资源/收据：I:root实际依赖闭包/HostOp→backend；L:TestInfraRootLifecycle→exerciseRoot；R:root服务端端到端。

文件（只此包可编辑）：
~~~text
cmd/tether-infra-helper/main.go
cmd/tether-infra-helper/main_test.go
internal/infraroot/bind.go
internal/infraroot/bind_test.go
~~~

P00/P02/P03/P04/P05/P06/P07/P08/P09/P11/P12/P14–P19/P21–P24/P27/P40各L行在该owner现有列出的_test.go内实现，不另外增文件。P10/P13/P20共享infraupgrade包但各自Test生命周期函数/consumer明确，P20不重复写P10/P13文件。新cmd/helper和launcher包自身也需Test体直接shared assert，不借internal包的L行代签；分别使用P40 main_test与P06 main_test。P22/P23宿主及infraattach是不同package，每个风险package各有直接anchor；P35反向发现漏包即拒签。

T03–T53的leaf tests只证明其可在默认环境执行的真实机制；fake backend的表不冒充L6动作。真实stock/权限/ACME/kernel收据由上列独立integration Test与drill负责；禁止在internal加integration build tag或让默认make test下载外部binary。T78–T81唯一测试源码owner是P37，P33只交被检drill；T87唯一Go入口位于P37的simcluster_gate_set_test，消费其ledger脚本与P36的实际行；T90唯一测试源码owner是P34，P38只交文档。此分工覆盖最容易被双方省掉的测试接线。

B-6地址路径已经有 `test/simcluster/drills/lib/agentyaml.sh`：它对非nats URL要求独立tunnel_addr，并会stop/start agent。因此P32只在**建立原业务之前**调用它配置 `broker_url=wss://host:443` 与 `tunnel_addr=host:7000`，live窗口禁止重新调用以掩盖NC问题。T68/T69在P30对实际config/连接跑正确URL、7000冒充WSS、443冒充tunnel、IPv6 bracket与缺独立地址负例；P37验证供给调用位置/显式第六参数，P33部署真连。该既有helper无需修改，不列为双owner文件。


## §13 文档和最终签收边界

P38逐项同步architecture §1/§3.5/§6/§21.3/§22（infra状态/权限/NC迁移和三完成点）、broker-ops（single/sole/N3/N2操作、bootstrap、rotation、status/recover、N-1能力安装）、cluster-runbook（真实B/S/J矩阵、ACK门与当前态rollback）、gotchas（明确机制/真实守卫/未覆盖边界）、usage（CLI精确flags/错误码）、testing-standards/test README（新层次/tag/matrix/监督器）、simcluster README（artifact、双栈/地址、原生arms/预算/供给）、CLAUDE registry、stages/tasklist/INDEX。过程产物在docs/reviews；不覆盖历史source/NOT_RUN为本候选的事实。没有真实设备资源变化时devices.md不改；若实施发现必须增配资源，先补唯一文件owner再动文档，不把估计写成已配。

| tasklist项 | 可勾所需合取 | claim writer / receipt writer |
|---|---|---|
| G-1 | §3/4实际NATS路径、全部readiness/版本/SHA、T24–T31/T59–T63、85.B | P38 / P39 |
| G-2 | §5 reload和binary分别、T64–T67、86.R/B/I、原内部NC/业务socket两账及old/temp退出 | P38 / P39 |
| G-3 | B/S/J矩阵、single/sole/N3/共址agent、N2/quorum/lag原生拒绝、healthy N2 Caddy | P38 / P39 |
| G-4 | 85.A/A1真实同store stop/always/2s正臂、原ACK/NUID、10/s≤5s/进展≤10s；defer不能勾 | P38 / P39 |
| G-5 | T14/T15/T31/T63、85.S/J、86.U；同store拒双开、join exact release、新入口/support自身可升级且旧代退场 | P38 / P39 |
| G-6 | 两版外置件升回再升、85/86全部原业务和资源、32/73/98、三硬闸+gates+Darwin、完整文档 | P38 / P39 |
| B-6复核 | P26全供给身份、agentyaml正负/真实WSS+独立7000 tunnel、两agent+ctl资源、P32独立oracle非补偿 | P38 / P39 |

P39收据每格最少 `source_head, dirty_patch_sha, source_manifest_sha, build_flags, archive_sha, binary_sha, running_identity, topology, Tx/Step, command, native_rc, verdict, evidence_path, oracle_inputs_sha, mutations, wall_s, status`；before/after进程集合、原业务requiredset、slot原始账、S7输入、rootjournal/effect证明为可追溯附件。dirty包含真实未跟踪输入内容，不能只写clean或文件数。每package row与每cell只有一个producer，不以最终summary“ALL GREEN”代替原始证据。

候选freeze后先做P00+最早的真实可行性门：默认两N1预算/ACK、两版$SYS reload、stock继承OFD、canonical双栈切换、root首迁ACME fence、真实WSS取消。它们失败时回到具体owner修方案、保失败证据；不等四十包完成才发现主路径不可行。没有达成相应成功出口，S9仍未完成；不能用scope外sibling/维护停机/重试掩盖替代。

## §开放问题（仅lead产品/范围裁决；建议不改变五出口）

1. **首发热切平台矩阵**：是否批准S9首先承诺linux/amd64 + systemd + 所需pidfd/cgroup/nft双栈能力？建议批准，缺能力在before-effect明确unsupported；保原Linux其它架构和Darwin安装/构建能力，Darwin真实回归照交，不宣称已验证这些平台的infra hot cut。扩大矩阵须另补官方archive/binary SHA及同样真实kernel收据，不能靠编译通过外推。
2. **Caddy的责任边界**：是否批准首发只承诺stock两版、filesystem ACME、tether自有HTTP/WSS/TCP路由与可验证有限普通HTTP，不承诺第三方永久HTTP/H3流、自定义module或外部storage插件热迁移？建议批准；已有未知/不支持的活责任先defer，不强断后签成功。stock旧root/on-failure、canonical新TCP/TLS和原tether业务仍是不可删的86.I正臂；不能将“root旧实例”本身列为unsupported。若需覆盖活QUIC/第三方长HTTP，必须扩产品入口/责任观察方案，不能以本稿TCP census代签。
3. **四条同一transfer贯穿N3九hop所需限速能力**：是否接纳S7候选的正式transfer rate-limit作为可发布能力和fixture前提？建议由S7 lead确认后接纳，以启动时固定速率保持小/大、push/pull四条原调用全程live；不允许harness暂停producer/服务或故障后降速。若不接纳，lead需重新冻结足够大payload与成本/requiredset，使每hop确实仍活；不能事后把过早完成改成通过。

其余选择已在定稿裁决冻结为本候选建议，不把socket取消、SYS权限、校验来源、launcher锁模型、gates归属、artifact pair或S9注册工作抛给lead作实现设计。以上问题未获裁决之前，P00/所有独立叶子仍可准备；候选不是已批准扩大/缩小scope的凭据。


## §附录 A · 四份批评逐项处置

编号按各critique §6最终must-fix列表，EX=crit-exit，BU=crit-business，PR=crit-priv，SU=crit-supply。**共67条BLOCKER/MAJOR，全部采纳（含PR-07的路径精化）；无搁置、无凭口头保证关闭项。**“采纳”表示具体修订已进入绑定候选，不表示运行验证已完成。原文D/F/M等展开项由对应must-fix覆盖；下表保留代码争议的裁判与可定位测试。

| critique-ID | 严重度 | 处置 | 具体修订与事实依据 | 唯一实现责任及独立验收 |
|---|---|---|---|---|
| EX-01 | BLOCKER | 采纳 | §2.3/§5把每步socket/cid/accepted PID/exe联结，main用产品root-mark bypass；拒SetServerPool伪变化。NC nats.go:6462–6465与6487–6501证明URL不是物理重连。 | P02/P07/P15；T06/T17/T35；M04/M09/M17 |
| EX-02 | BLOCKER | 采纳 | §4.5底层raw DNS/proxy/TLS/WSS取消先于mu等待，全部worker join≤5s。agent/conn_teardown.go:3–30,52–57的既有梯子不够。 | P14；T32/T33；M16 |
| EX-03 | BLOCKER | 采纳 | §5以expected pidfd SIGTERM停止旧invocation，不重新拨复用admin endpoint；/stop没有expected PID条件。 | P02/P08/P10；T19/T66；M04/M10 |
| EX-04 | BLOCKER | 采纳 | §5.2全TCP/HTTP/upgraded及pending accepts归零才stop；新配置grace5s/shutdown0，旧无限grace必须先实证责任已空；slowHTTP/close-write反例。Caddy2.7.6 app.go/streaming.go见§1。 | P10/P29；T22/T66；M10/M12 |
| EX-05 | BLOCKER | 采纳 | §5.3持久startup fence挡always/2s；oldwriter退出后current ACME delta/fsync/selector提交再ReleaseStart。 | P05/P09/P40；T20/T21/T67；M11 |
| EX-06 | BLOCKER | 采纳 | §5/86.I保首尝试canonical TCP/TLS连续成功与旧root首迁；stages-plan:417不允许5s重试代签。 | P07/P25/P29/P33；T16/T65；M09 |
| EX-07 | BLOCKER | 采纳 | §5.4取消shadow RuntimeMaxSec；120s只halt推进，跨期限继续承载，recover凭零责任退场。 | P10；T23/86.F；M12 |
| EX-08 | MAJOR | 采纳 | §4.1选exec+同OFD继承、永久锁inode、legacy census及启动围栏；Claim/Ack独立看stock exe，父死不释放最后server引用。 | P06/P28/P40；T14/T15/T63；M08 |
| EX-09 | MAJOR | 采纳 | §4.6独立signed old→new proof保原8s expiry与home baseline；agent/roster.go:779–806,817–821,898–909不允许复用旧route ACK。 | P16/P23；T36/T37/T69；M18 |
| EX-10 | MAJOR | 采纳 | §4.3 sole-voter指tether Raft、JS仍standalone；natsconf/render.go:68–73、preflight.go:449–452反证priv稿。 | P13/P33；T30/T60/85.A1；M15 |
| EX-11 | MAJOR | 采纳 | 不热改sync always，不偷偷重启；NS reload.go:1977–1985无该reload支持；§4.3两版默认R1 same-boot ACK实测，失败不能defer抵成功。 | P18/P28；T40/T60；M20 |
| EX-12 | MAJOR | 采纳 | §4.4普通auth gate与真实normalqueue；brokercontrol/authcallout.go:228–243是临时nonqueue探针，只可签S6 handler。 | P17/P22/P30；T38/T39/T70；M19 |
| EX-13 | MAJOR | 采纳 | §2.3按本步before/after incarnation而非全局oldSHA过滤；同SHA回滚合法、samePID exec需证明旧映像消失。 | P02/P32；T05/T58/T74；M04/M30 |
| EX-14 | MAJOR | 采纳 | §5.2redirect只分流新TCP；旧keepalive可lateUpgrade，故全socket/accept/conntrack责任封口及完整复盘，返回对shadow相同，未知不算0。 | P15/P10/P29；T34/T66；M17 |
| EX-15 | MAJOR | 采纳 | §3.2/P18实际gate所有managed JS mutation并取seal，不虚构NATS AssetGeneration API；分页/跨account/同名重建和foreign writer未封门拒绝。 | P11/P18/P22/P19；T25/T41；M13/M20 |
| EX-16 | MAJOR | 采纳 | §8分S8通用事务、S9组件注册与physical proof；S7续接与S9全部NC；S6 exact hold与S9 route bridge，各自唯一owner。 | P16/P19/P20/P22/P32；T42/T44；M21/M22 |
| EX-17 | MAJOR | 采纳 | §8区分receiver-first安装与全capable agent→broker→NATS→Caddy cut；S9只执行两infra leg。broker-ops:693–694与cluster_upgrade_drive.go:141–172支持该区分。 | P20/P24/P30；T45/T52；M26 |
| EX-18 | MAJOR | 采纳 | §6.5 build/archive/checksum/install/两代runtime/旧worker真退；Makefile:16–18、goreleaser:29–57目前只有tether，必须产品补齐。 | P26/P25/P40/P31；T55/T74；M28/M36 |
| EX-19 | MAJOR | 采纳 | §10–12逐enum/ctx/wire/private/IPC/error/race/leak/五基线明owner；精确宿主counts+全部legacy ceiling，不借package净下降。 | P34/P35/P36/P37；T82–T89；M31–M34 |
| BU-01 | BLOCKER | 采纳 | §5.1/5.2独立fresh443首尝试oracle+raw物理入口，logical retry另账；旧root仍正臂。 | P07/P32/P33；T16/T17/86.B/I；M09 |
| BU-02 | BLOCKER | 采纳 | §4.4 nats.go:3379在callback前自动SUB，因此用auth-only NC与business dial gate阻auto replay；普通身份不走临时probe。 | P17/P22；T38/T39/T70；M19 |
| BU-03 | BLOCKER | 采纳 | §4.5打断真实raw连接及DNS，join锁持有worker；不将SetServerPool/Goroutine包装看成5s返回。NC:2465,6412与ws.go:655为真实阻塞点。 | P14/P30；T32/T33；M16 |
| BU-04 | BLOCKER | 采纳 | §4.6 proof绑定old/new URL/serverID/cid/health、原home与expiry；不放宽S6 exact身份校验。 | P16/P23；T36/T37/T69；M18 |
| BU-05 | BLOCKER | 采纳 | §4.2 LDM先关JS/WS（NS:4450,4474），所以先seal全NC、checkpoint、successor ACK和可迁客户端；latejoin同generation登记，不在LDM后首次协调。 | P15/P18/P19/P22/P23；T29/T34；M17 |
| BU-06 | BLOCKER | 采纳 | §5.4保仍服务shadow至实际Return零责任，不为component120s安装自动kill。 | P10；T23/86.F；M12 |
| BU-07 | BLOCKER | 采纳 | §4.3/P18产品pre-repair gate逐chunk/history/consumer/audit读取，覆盖自动reconcile/GC；harness不能先暂停producer或仅查最终SHA。 | P18/P19/P22/P28；T40/T49/T60；M20 |
| BU-08 | MAJOR | 采纳 | §4.3两个default standalone非空store正臂；不依赖SyncAlways reload/monitor热加，资格失败就是E3未交。 | P12/P13/P28；T27/T30/T60；M14/M15 |
| BU-09 | MAJOR | 采纳 | stages-plan:372通用事务不包含S9实现；§8/P20自己注册nats/caddy/status/root映射，等待的只是上游真实通用合同与close。 | P20/P22/P24；T44/T45；M22 |
| BU-10 | MAJOR | 采纳 | §9.1固定每hop requiredset与exact10 slots，业务socket/carrier与NC两本ledger；PTY实际控制、ps/kill、SS双订阅独立撤权及四原transfer非空。 | P30/P32/P33；T68–T71/T76/T77；M30 |
| BU-11 | MAJOR | 采纳 | §5.3/86.I在LIVE窗口由产品完成旧root/on-failure迁入；installer never-start，H0不预迁storage/UID。 | P05/P25/P31/P33；T13/T54/86.I；M27 |
| BU-12 | MAJOR | 采纳 | §5.1 main-ready经过root-mark物理bypass，accepted socket连到指定main PID；shadow成功不能签main健康。 | P02/P07/P32；T06/T17；M09 |
| BU-13 | MAJOR | 采纳 | §2.3分live_business>0与old_owner_pending=0，retire不等原无限任务/transfer结束，也不忽略未移交责任。 | P10/P15/P19/P32；T22/T35/T43；M17/M21 |
| BU-14 | MAJOR | 采纳 | 86.H healthy N2 Caddy正臂；85.D/Q/L各实际NATS拒绝与后续恢复分别记账，不擦原失效窗。 | P13/P29/P33；T62/T65；M15 |
| BU-15 | MAJOR | 采纳 | §7.2每lost ACK绑定真实sent/effect/reply-cut三证；实际产品diff restage被同oracle击红，单关TCP/伪Status不代签。 | P28/P30/P33；T59/T70/T71；各M族 |
| BU-16 | MAJOR | 采纳 | §10逐root/codec/ctx/error/enum/race/leak精确表，run-all/lint-install是真接线，image零Δ有反向扫描条件。 | P34–P37；T82–T89；M31–M34 |
| PR-01 | BLOCKER | 采纳 | §5.1 main/warm各permissioned Unixadmin；旧root2019产品隔离普通UID，禁止旧root/load任意caller配置。Origin/localhost不作UID证明。 | P03/P07/P08/P31；T19/T72；M05/M10 |
| PR-02 | BLOCKER | 采纳 | §5.3 oldexit后final当前ACME→fsync→selector→ReleaseStart，持久fence跨always restart；warm snapshot不成为最终主目录。 | P05/P09/P40；T20/T21/T67；M11 |
| PR-03 | BLOCKER | 采纳 | §2.2签完整规范HostRequest（包括expected/warm/policy/boot/sequence）；helper当前domain CAS重验，迟到unflip/跨boot/改字段均拒。 | P00/P03/P04；T02/T07/T09；M01/M05/M06 |
| PR-04 | BLOCKER | 采纳 | §6.2 root显式bootstrap激活管理socket不动业务，86.I原业务上验证；install.sh:413现never-start约束保留，fixture不得预systemctl代签。 | P05/P25/P40/P31；T13/T54；M27/M36 |
| PR-05 | BLOCKER | 采纳 | §7/8 S9组件/status/journal/support binding归P20，S8仅通用事务/domain/两代；stubs不能广告能力。 | P20/P22/P24/P40；T44/T45/T91；M22/M36 |
| PR-06 | BLOCKER | 采纳 | §5.3明确stock root/on-failure必须原负载连续首迁，非维护窗口和READY前setup；86.I独立正臂。 | P10/P25/P29/P33；T65/86.I；M12/M27 |
| PR-07 | BLOCKER | 采纳并精化路径 | §3.3在默认已有$SYS加read/step窄users，保持SystemAccount/$G；Parse→Owned→Render→Merge链真保留，两版reload及最窄stepdown必须实测。NS:1950–1953不许随意切SystemAccount。 | P03/P12/P28；T27/T28；M14 |
| PR-08 | MAJOR | 采纳 | §10 lint-install目前:98 greedy last heredoc、:131≥10地板不够；P37 exact targets+每个heredoc+quoted/substitution反例并run-all真跑。 | P37；T88/T89；M34 |
| PR-09 | MAJOR | 采纳 | §6.5完整release/install/busy-stage/idle切换/两代rollback；旧worker退出由新worker独立核实，无常驻旧launcher parent。 | P26/P25/P20/P40/P31；T55/T74；M22/M28/M36 |
| PR-10 | MAJOR | 采纳 | §6.1 host worker2s读取/10s动作/15s总寿命/concurrency4、全部child join；§5.4 shadow无timer。不同责任不共用RuntimeMaxSec。 | P04/P10/P27；T08/T23；M06/M12 |
| PR-11 | MAJOR | 采纳 | §6.3/6.5账户/CA/local policy revision+key_id同步、旧ticket撤销、pending仅读后显式re-authorize；rotation guide增加产品rotate-policy。 | P03/P24/P31；T07/T75；M05 |
| PR-12 | MAJOR | 采纳 | §5.3先空赋值清累加caps再唯一bind；读取effective所有Exec/prefix/env/override，未审计拒绝。systemd v256 exec/service文档支持累加/+prefix反例。 | P05/P25/P31；T12/T72；M07 |
| PR-13 | MAJOR | 采纳 | §2/6薄root独立binary+neutral DTO，排除cmd/tether/main.go:84–117 pre-Cobra agentboot；env/FD白名单与真实依赖闭包门。 | P00/P03/P34/P40；T91/T92；M31/M36 |
| PR-14 | MAJOR | 采纳 | §3/6先复制callerFD到root私有最终immutable inode再hash，可信dirfd逐祖先/不可替换slot；拒可写SCM_RIGHTS和检查后换路径。 | P01/P04/P31；T03/T10；M03/M06 |
| PR-15 | MAJOR | 采纳 | §6.4 active reinstall/retrofit/uninstall同域CAS、stage-only及ownership marker；TETHER_INSTALL_ROOT/dry-run所有新文件/进程/kernel副作用全对称，永久锁不unlink。 | P25/P31/P33；T54/T73/32；M27 |
| PR-16 | MAJOR | 采纳 | §10 credentials loader/codec/policy真实三向row，root和launcher自己的race/leak anchor，ctx/tag/enum/error逐站精确owner。 | P34/P35/P36；T84/T85/T86；M31–M33 |
| PR-17 | MAJOR | 采纳 | §5/9保443 raw handshake独立oracle、产品nft8443/双栈/OUTPUT/hairpin/bypass，H0仅供内核工具不预写规则。 | P07/P26/P32/P33；T16/T17/86.B/I；M09/M28 |
| PR-18 | MAJOR | 采纳 | §4.3/P13修sole-voter→standalone JS；§8分别列receiver-first安装与全capable四层cut，依据natsconf/render.go:68–73。 | P13/P20/P24；T30/T45/T52；M15/M26 |
| SU-01 | BLOCKER | 采纳 | §5 canonical每次首TLS成功，main/shadow/bypass物理联结；不将5s logical retries签为连续入口。 | P07/P32/P33；T16/T17/86.B；M09 |
| SU-02 | BLOCKER | 采纳 | §9.3合法alnum arms/原生DRILL-ARM与完整VERDICT/正负parent分开；simcluster:719–739只吃--arm，禁止虚设被丢弃negative参数。 | P33/P36/P37；T78–T81；M34 |
| SU-03 | BLOCKER | 采纳 | §9.4完整COPY输入+最终image/source清单每次独立guard；现simcluster:693–697只验tether不足，每一输入单改stale必须拒。 | P26/P31/P37；T56/T89；M28 |
| SU-04 | BLOCKER | 采纳 | §6.5 helper/launcher build→archive/SHA→真实installer/skip-download→runtime两代/rollback→旧worker退出，不是fixture内孤立二进制。 | P26/P25/P20/P40/P31；T55/T74；M22/M28 |
| SU-05 | BLOCKER | 采纳 | §5.3首迁保原root负载；晚ACME写+final复制>2s打穿快照/自动restart竞态，产品fence承担。 | P09/P10/P33；T21/T67/86.I；M11 |
| SU-06 | BLOCKER | 采纳 | §4.3两N1独立成功，旧stock无monitor、defaultpersist、非空store；natsconf/preflight.go:449–452不允许孤儿化迁cluster。 | P12/P13/P28/P33；T27/T30/T60/85.A/A1；M15 |
| SU-07 | MAJOR | 采纳 | §9.4显式供应systemd/nft/iproute2/conntrack/IPv6及外部本机hairpin拓扑，权限动作归产品；不假设当前image有sudo/nft或v6。 | P26/P07/P31/P33；T16/T72/86；M28 |
| SU-08 | MAJOR | 采纳 | §9.1/9.5二版外置件由预供manifest校验；default hermetic不下载/no root，指定integration缺件setup失败；四tag每Test600s/真实race构建链。 | P26/P27/P36/P37；T55/T57/T86；M28/M29/M33 |
| SU-09 | MAJOR | 采纳 | §6.4/9.4 32全root content+metadata/所有process never-start；73新SS恢复不代原socket，98#72不被plannedMove越界关闭。 | P25/P33/P36；T54/T81/32/73/98；M27 |
| SU-10 | MAJOR | 采纳 | §8/9.1等S7 executable/schema/source正负收据；每hop四原transfer/NUID/ctlPID/live checkpoint，完整三向cycle早完为INCOMPLETE，不重开补齐。 | P19/P30/P32/P33；T42/T43/T71；M21/M30 |
| SU-11 | MAJOR | 采纳 | §9.3普通mutant native1；只有真实历史已钉signature用3；39.NI/CI真旧artifact证明拒绝用GREEN，与85/86成功分账。assert.sh:14–22为词表。 | P26/P33/P36/P37；T78–T81；M28/M34 |
| SU-12 | MAJOR | 采纳 | §10–12五基线P36唯一writer；alerts定义P21、exit呈现P24、error/enum/inventoryP35；admin已有root不另建wire岛；逐ctx/tag/layering接线。 | P21/P24/P34–P37；T84–T89；M31–M34 |
| SU-13 | MAJOR | 采纳 | §10.1全部legacy单文件ceilings及恰70/57/32、Agent125/Runtime39当前事实；entry测量与每文件Δ≤0，不借S7未来slim或包净下降。 | P12/P14/P18/P21–P24/P34/P35；T82；M31 |
| SU-14 | MAJOR | 采纳 | §1/8依据真实S6 Run cut，S7/S8候选不当事实；交还SHA/API/fixture/race/inventory正式映射齐前禁止广告/激活/completed。 | P19/P20/P22/P23/P32/P39；T42/T44/T91；M21/M22/M36 |

MINOR亦采用：EX-20/BU-18/PR-19/SU-14统一当前快照及最终rebase SHA，不沿用旧“尚未实现”/不存在文件/“15家族”/默认native3；BU-17/PR-18分安装与cut顺序；PR-20/SU-15固定2.7.5↔2.7.6回pin、NATS2.14.5↔2.14.6回pin；SU-16成本按两agent+ctl、全部arms/九hop/ACME/支持件/冷供给分别估算，未实跑不写measured。具体实现事实争议以§1文件:行裁决为准，官方第三方源码链接仅佐证版本语义，不代本项目测试。

本计划不以“所有critique已采纳”代替完成：P39只有在E1–E5每格、所有mutation、S6/S7/S8实际交还和结构/供给/独立observer闭合后才提出stage close；当前全部实施/运行项仍NOT_RUN。
