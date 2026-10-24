# S6 · broker 映像交接定稿计划（broker-image，XL）

## lead 定稿裁决（覆盖正文）

日期 2026-10-03。来源：四份草案（lifecycle / store / cluster / ctlplane）、四份对抗批评（fd / writer / cluster / measure）与综合稿，原文归档于 `~/tether-hot-update-handoff-20260920T2356Z/s6-drafts/`。本节冻结，正文中与本节冲突处以本节为准。

| # | 裁决 |
|---|---|
| L1 | 正文「定稿裁决」表整体采纳，作为冻结值。 |
| L2 | Q1：**是**。S6 交付 agent 侧 maintenance capability + ACK 接线（P17 与 additive wire），planned hot-cut 以「已安装该能力」为入口；缺能力的旧 fleet 保持普通 wire，planned hot-cut 拒绝；首次迁入留 S8。 |
| L3 | Q2：**不加** `node upgrade --broker` 别名；single 只提供 `broker upgrade reload` 入口与状态，`node upgrade` 仍只作用于 agent。 |
| L4 | INCOMPLETE 的原生 rc 保持 **4**（沿用 S5 L15 的 validator/runner 契约）。 |
| L5 | 吸取 S5 教训：每个跨包产物（live drill fixture/collector、private-state descriptor、observer、ledger 行、leak anchor）只有一个 owner，以 §12 为准；任何包报告「需要别的包实现」时，由 lead 当场指定 owner，不得悬空。 |
| L6 | 实现全部用 `gpt-6.1-sol`；正文 HARD 标记只表示投入更高 effort，不指定 Astra。 |
| L7 | S6 实现在 S5 收口（三硬闸 + drill 75 同候选树）之后开工；P00 合同可在 S5 收口期间先行起草，但不落树。 |
| D-C (2026-10-22) | Single mode follows §7.1 literally: retain a local-equivalent maintenance owner in the same image, with the same CAS, fixed TTL and signed challenge proof semantics. Every currently reachable affected agent must advertise the S6 capability and ACK its installed hold before any planned cut step, including S8 arm. Missing capability refuses before the cut (runbook: “维护缺capability切前拒绝”); ordinary legacy traffic remains supported. No new OpType. |

状态：**已冻结（见上方 lead 定稿裁决）**。本稿基于只读快照 `7fc0be99e61c8414f3d1341cf15789ee34d1f33f`，以 live contract architecture §22.8 为准；四稿中的旧行号不作为当前实现证据。只进行了读取和静态核对，没有运行任何构建、测试、drill 或部署；下列预算均为估算，验收状态均为 `NOT_RUN`。

## 定稿裁决

| 议题 | 裁决 | 理由 / 落点 |
|---|---|---|
| N=1 的产品范围 | single 为主 lane；真实 1-voter 另列 A′；二者均同 PID exec、写停顿 ≤5s | stages §S6 与已裁定 OQ1；不以 `cluster init` 改造 single fixture。§0/§5/§10。 |
| exec 的位置 | 在 `Broker.Run` 内完成显式 `Node.Shutdown` 后调用 `Prepared.Exec`；不 cancel Run、不返回到 `serve` 后 exec | 当前取消路径会运行所有 defer。§1/§4。 |
| 数据库与锁 | 当前 SQLite 文件唯一 writer；single/cluster/offline 同一 canonical lock domain；原 flock OFD 跨 exec | 不接受不同 DataDir 把同一 DB 分成两个锁域。§3。 |
| preflight | candidate/prev 纯 smoke、ABI/域/配置/schema 集合检验；从不打开 store | 即使 read-only 打开 Bolt 也可能挂住。§3.1/T06。 |
| SQLite 交接 | 阻止生产者、真实 join、取最终 cursor、WAL checkpoint、关闭全部连接、最终 seal、exec、`OpenCurrent` | 采用 writer/FD critique 的真实关闭收据；拒绝 P 稿先 Prepare 再关闭。§3.2。 |
| carrier 与 raw freeze | attachment fence → 关闭底层 TLS → yamux/worker/monitor join → `Manager.Freeze` | live §22.8 与现 `Client.FreezeFlows` 一致；拒绝 L/S 的 raw-first 顺序。§6。 |
| listener | 独立 keeper 与 Accept view；Freeze 关闭 view 的准入，不关闭 keeper OFD | TCP/UNIX/HTTP/admin/tunnel 共用一个 leaf 合同。§6.1。 |
| importer 与 rollback | 一次 validated Role dispatch；partial Take 返回完整 salvage；第一潜在 store/transport 副作用前置 ACTIVE | pristine Repack 与 current cut 两条路径；永不恢复旧 DB snapshot。§4。 |
| snapshot | 保持 `NoSnapshotRestoreOnStart=true`；新增真实 runtime InstallSnapshot 动态 floor 拒绝 | C 稿 startup Restore 测试不可达；writer 所指 runtime 覆盖风险真实存在。§3.4。 |
| maintenance | ≤8s 固定到期、replicated record + holder/CAS + 全部 observer 明确确认；S6 增 agent capability/ACK 接线 | 不假设 S5 已实现，不用 20s roster backoff 证明安全；保历史 dwell。§7。 |
| scope | 独立 `PlanScope`，默认 `ScopeWholeHost`；`--broker-only` 选 `ScopeBrokerDaemon` | `Step.BrokerOnly` 仍只表示 host 无 agent，不改它的语义。§5。 |
| ctl 重试 | 一个绝对 5s admission budget；只对 S1 允许的身份与安全动词重试，固定 CallID/RequestID | 不扩大首次 PIN/auth 失败的权限边界；所有尝试都入 observer。§8。 |
| drill verdict | `37.A/A1/B/T/R` 原生全 GREEN；真旧 Run-unwind 负控独立 `39.BR` 原生 PRODUCT-RED | 不用子 rc 翻译把 parent 洗绿；`INCOMPLETE=4`。§10。 |
| ABI / 状态库存 | 一个 `broker-image-v1` envelope；broker private descriptor 独立闭合词表；public/wire append-only | 不向所有 S5 sealed-state reader 的闭合 kind 集合塞 broker-only kind。§2/§9。 |
| 并行拆包 | P0 先冻结合同并落编译 stub；每个文件只有一个 owner；显式 store/flow/control/rollback/drill integration 包 | S5 的 22 个确认 finding 表明 seam 必须有独立 owner。§12。 |

## §0 出口、覆盖与验收收据

六出口是 AND。safe defer 只证明拒绝正确，不能替代 A/A′/B 成功。证据必须来自同一 source/artifact SHA、同一 native run；失败不重跑洗绿。

| 出口 | binding 行为 | 必要收据 / 负控 | 实现与验收 owner |
|---|---|---|---|
| 1 · Raft/SQLite/锁与回退 | single/N1/N3真store；Run内NodeShutdown→Exec不unwind；单writer/连续flock/ACKprefix/applied_index/currentrollback/preflight不开store | T01–T17/T37/T49/T51/T52；actualdriver0、replay前ACK、旧库/WAL/unlock/Boltsecondopen负控 | P01–P05/P11/P22；P26/P28 |
| 2 · 真tunnel交接 | 原publicraw/flow/Endpoint/fences/listeners跨exec；导出期CloseProxy仍生效；新connect零refused | T18–T28/T48；fullFDidentity与seq/hash/FINtail、latealiasrevoke与lateAcloseB负控 | P06–P10/P12；P28 |
| 3 · auth与新控制请求 | auth_calloutfirst；符合S1身份的CONNECT/node ls/ps/reliable shortexec从RunE入口到真实结果≤5s，无duplicateeffect | T29–T36/T50；10/s所有slot/attempt、truePIDstarted、2.1sprobe不能legacy、measuredblackout | P12/P18–P23/P27；P29 |
| 4 · N=1产品正控与N≥3安全门 | single主lane、solevoterleader≤5s且不CodeIsLeader；TestReloadRefusesOnLeader只对voters>1；N≥3freshtransfer、维护不rehome/escape | T37–T45；freshconfig/term/roleedge、holderCAS/fixedTTL、agentACK、fullidentitywriterguard | P13–P17/P24；P26/P27 |
| 5 · drill37GREEN | A/A1/B原SSH/TCP/SS与longexec持续；T真transferdefer及negativearm；R合法afterTake/ACTIVEcurrentrollback；39BR旧unwind原生红 | 全部nativechild/parentrc、originalsockets/PID/startticks/runningSHA退出、attempt/effect、homehistory；无fixture补偿 | P26–P30/P33/P35 |
| 6 · gates与Darwin/docs | 三硬闸+gates；70/285/<18000及所有inventory/leak/race/budget/layering/enum/error/tag/ctx；Darwin独立receipt | T46–T47/T53–T58，所有gatecontrols与wholematrix；tasklist只据sameSHAreceipts | P25/P31–P35 |


覆盖：stage coverage 行 8（broker/持久化）、行 4 broker 臂（SSH/TCP）、行 5 broker carrier；tasklist E-4、D-2/D-4/D-6 **broker 子项**、B-4 的新控制请求与旧映像退出子项。B-4 总项、D-6 总项和跨层行不在 S6 总勾。S7 tracker/GC、S8 两代限额与真实 fleet N-1/首次迁入/session reincarnation、S9 NATS/Caddy、S10 全栈 15min 均保留原 owner。

## §1 已核代码事实与资源图

### 1.1 事实纠正

| 分歧 | 当前代码证据 | 本稿判定 |
|---|---|---|
| reload 已在 Run 内 exec | `internal/broker/reexec.go:31–42,91–95` flag+cancel；`cmd/tether/serve.go:315–330` 等 Run 返回后 exec | 尚未实现。P11/P22 必须改接线。 |
| single 已有锁/触发器 | `internal/broker/broker.go:995–1008,1385–1395` 两项均在 cluster 分支；`cmd/tether/serve.go:197–202` 单机 Open+captured defer | 尚未实现。single 必须共用 image store owner 与本地 admin reload。 |
| leader 一律不能 reload | `internal/broker/reexec.go:55–62` same-release skip 在 SHA 前、IsLeader 一律拒绝 | 改为 running SHA 判断；只有经 fresh config 证实 sole voter 才可 leader cut。 |
| `s.wg` 已 join 全 carrier | `internal/tunnel/tunnel.go:416,535–540` 只覆盖 accepted agent handler；carrier 在建立后独立运行 | FD critic 正确，L/C 事实错误。需独立 worker/monitor/accept/timer 账。 |
| raw Export 自己 dup / listener 可复用 raw validator | `internal/rawsocket/image_linux.go:11–28` 返回 borrowed file；`socket_linux.go:35–55` 要 `SO_ACCEPTCONN=0` | FD critic 正确；keeper 显式来源与专用 `SO_ACCEPTCONN=1` validator。 |
| Stop timer 等于 callback 退出 | `internal/tunnel/flow_endpoint.go:186–191,214–215` callback 可已在执行；:261–359 accept/slot/stop 会关 pending raw | FD critic 正确；不能把 terminal stop 或 deadline loop 当 pause。 |
| current constructor 错误一定泄漏 injected transport | `internal/cluster/node.go:118–125` 对 New 任意失败关闭 injected transport | writer 的这一事实归因拒绝；transport accepted handler 的未 join 仍真实，见 module `net_transport.go:333–342,606,701–707`。 |
| Open 可用于无副作用 hot reopen | `internal/storage/storage.go:63–69,87–93,193–226` 自动 migration；migration version 是完整 filename TEXT | writer 正确；不得用整数 SchemaVersion 或 Open 校验热库。 |
| Shutdown 收据已含 checkpoint | `internal/cluster/node.go:523–543` 顺序 close raft/transport/Bolt/RO/DB，错误 join，无 checkpoint/逐对象 phase | 四稿均须补；不能据一次返回 nil 假定全部实际 driver 0。 |
| startup 会恢复旧 snapshot | `internal/cluster/node.go:303` 设置 `NoSnapshotRestoreOnStart=true`；raft v1.7.3 `api.go:677–679` 跳过启动 payload Restore | C 错、FD/writer 正确；runtime `fsm.go:420` → `snapshot.go:139–155` 仍会 in-place Restore。 |
| `applied_index` 等于所有 Raft index | `internal/cluster/fsm.go:301–329,397–406` 是 SQL command cursor；barrier/config entry 不等价 | SQL floor 与 Raft metadata/config 分别验证，不用一个整数混判。 |
| 原 factory 可重用 | `internal/broker/expose.go:533–544` cold 随机 incarnation/instance 并建 budget；`broker.go:937–946` cold advance generation | hot 必须跳过并采用 sealed 原 issuer/key 与同一个 budget pointer。 |
| epoch CAS 足够 | `internal/port/plan.go:273–295` 未完整约束 SID/NID/tokenHash/home；`proxy_rebalance.go:163–169` 先 started 再 Propose | cluster critic 正确；在 applyMu 内 full identity + maintenance 决策，commit/readback 后发事件。 |
| lock 时间戳是 holder | `internal/broker/cluster_upgrade_trigger.go:223–288` 时间戳 readback、无 holder renew/release；`cluster/membership_ops.go:491–499` 无条件清除 | cluster critic 正确；新 holder/lease CAS。 |
| roster backoff ≥20s，可不用 agent 改动 | `internal/agent/roster.go:287,308–318` topology wake 可在 silent=2 立即第三次失败；:435–456 随后 rebuild | 拒绝该证明；S6 必须明确 route-hold capability/ACK。 |
| probe timeout 等于不支持 | `internal/execclient/client.go:143–156` 将 timeout/no responders 当 ErrUnsupported；`cmd/tether/exec.go:94–125` 因此 legacy fallback | measure 正确；空窗必须同预算重试，不能重复启动 legacy。 |
| CallID 每次 attempt 可变 | `internal/execclient/client.go:255–268` 已在循环外固定 CallID | writer 正确；保留，新增 helper 不改稳定身份。 |
| Fresh receipt 可由 fixture 预种 | `cmd/tether/ctl_connect.go:126–169` 只允许非 login 且有效 historical receipt 重试；`cli/auth_receipt.go:28–40` 只是身份历史 | 遵从 architecture §22.4 S1 item 10；拒绝 FD8 扩大首次 auth/PIN 策略。 |
| READY / Flush 已证明 auth | `broker.go:1283–1288` 忽略 Flush 错且早于 :1305–1331 JS/late wiring | measure/FD 正确；单独 auth-usable 与 full-ready 两事件，真 callout JWT response/CONNECT。 |
| 75 已有 live fixture | `test/simcluster/drills/75-hot-update-flows.sh:54,119–180,264` 要求 `flow_fixture` 命令，当前无实现 | S5 review 正确；P29 独占实现与 collector，P30 只接线。 |
| INCOMPLETE=3 / legacy 放37 | `drills/lib/assert.sh:18–22` 为 4；`tests/validate-verdicts.sh:226–304` 聚合所有 declared children | measure/writer 正确；37 全 GREEN，legacy 独立 39.BR。 |

上述 line 为当前快照；实施收据需同时附新 SHA 的定位，不能把这张表当测试通过证据。

### 1.2 图、owner 与生命周期

`Run` 的唯一 `RuntimeSlot` 持当前 `{StoreOwner, Node, NC, Auth, Control, Server, Listeners, Maintenance, Boot}`。`serve` 不再捕获 DB；Run 的普通退出只有一次 `RuntimeSlot.StopCurrent()`。所有 defer、ctx watcher、closed listener view cleanup 都读取当前 slot，不能捕获旧 `nc/db/node/admin/server`。首次 exec 成功不会返回，原 defer 链完全不执行；exec 返回只重开当前库、替换 slot、恢复当前 control roots 后继续同一个 Run。

| 资源 | terminal owner / image owner | Freeze / 继承 / 关闭 |
|---|---|---|
| `DataDirLock` | store owner；cut 仅借原 OFD，Prepared 持 dup | keeper 不 `LOCK_UN`；image Adopt 不 reopen/re-flock；正常退出唯一 Release |
| SQLite writer / RO / backup readers | storage driver registry + Node/single store owner | 排空、checkpoint、实际 Close；**不继承** SQLite FD/pool/statement |
| Raft / network transport / Bolt | Node + clusterio tracked transport | 取消可取消 RPC、join、Shutdown、Close；**不继承** raft/TLS/Bolt FD |
| HTTP/admin/tunnel/public listener | listenerimage keeper；每代 Accept view | Pause view、handler join；原 LISTEN OFD + pathname 留存，dup 继承 |
| original public raw / replay / Endpoint / pending | tunnel Server + S5 flow owner | carrier join 后 safe point；raw/replay FD 与 private cursor/slots/fences 入 image |
| carrier TLS/yamux / agent REGISTER | 当代 tunnel control root | 不继承；关闭并 join；new carrier 重连后 fresh authority 验证 |
| NATS / auth subscription / business handlers / loops | 当前 ControlOwner | admission fence、DB handler/loop join、回复已准入请求；重建，auth first |
| JS object/stream/audit | 外置 NATS 的磁盘 owner | 仅 witness；S6 不 close/recreate/reset，也不启动第二 server |
| sealed private state / prepared dup / taken salvage | 一个 `CutResources` registry，逐 ID/purpose/OFD owner | 替换/失败显式 close 仅本 cut 的 FD；pristine detach 不 shutdown 原业务 |
| S7 transfer tracker | current broker tracker | S6 必须证明 census=0；非零只 deferred，无 tracker image 假实现 |

控制 roots 的 ctx 仅管当前 admission/loops/CONNECT；dataplane exports 全 ctx-free，使用 absolute deadline、明确 owner stop/condition。所有新 root 逐站标 `ctx-root:`，不用 caller ctx 锚定 raw/listener/Endpoint；内建 Background 站标 `ctx-none:` 并进入精确账本。锁序：cut admission → server tables → Endpoint → flow latch；阻塞 TLS Close、socket Shutdown、wait、store Close 都在表锁外。回调不可等自身；final seal 不持正在 join 的 callback 所需 mutex。

## §2 P0 合同、命名与 image ABI

P0 先落下列合同文件与独立 `contractstub`，compile probes 消费所有签名；stub 只返回 `ErrNotImplemented`，不能返回伪收据。生产 wiring 不可 import stub，P11 最后接真实现。定义归 P0、方法体归后续 owner；合同变更必须由 lead 解冻 P0 后串行修改，不允许实现包自行新增字段。所有合同 file 永久只由 P0 编辑。

### 2.1 frozen signatures

以下所有 image/leaf API 无 caller context；`Deadline` 是同机 `CLOCK_BOOTTIME` absolute ns，零值非法，wall time 不参与 cut/lease 保护。跨机 proof 见 §7。

```go
// internal/controlbudget: 类型与 observation 合同，不 import cli/nats。
type Deadline int64
type AttemptPhase string // 冻结闭合值见§8.3，不用任意string逃family gate
type Attempt struct {
    LogicalID string; Ordinal uint32; Phase AttemptPhase
    StartedMonoNS int64; FinishedMonoNS int64
    Sent bool; Class string; BackoffNS int64
}
type Observer interface {
    Begin(logicalID string, phase AttemptPhase) (Attempt, error)
    Record(Attempt) error
    Close() error
}

// internal/storage: 以下是 image 字段；json 名称同 snake_case 字段语义。
type DBIdentity struct { Path string; DataDir string; LockPath string; Device uint64; Inode uint64 }
type SchemaStamp struct { Applied []MigrationDigest; Supported []MigrationDigest; Digest string; ObjectDigest string }
type MigrationDigest struct { Name string; SHA256 string }
type StoreStamp struct {
    Identity DBIdentity; Schema SchemaStamp; Journal string; Synchronous int
    AppliedIndex uint64; AppliedTerm uint64
    RaftLastIndex uint64; RaftConfigIndex uint64; RaftConfigDigest string
}
type ClosePhase uint8 // CloseOpen, CloseStopped, CloseClosed, CloseUnknown
// closed receipt 与 stamp 分开，禁止构造“预期将关闭”的 receipt。
type StoreSeal struct {
    Stamp StoreStamp; Writer ClosePhase; ReadOnly ClosePhase; Bolt ClosePhase
    Transport ClosePhase; Raft ClosePhase
    Role RoleReceipt
    DriverWriters uint32; DriverReaders uint32; ActiveRPCs uint32; ActiveBackups uint32
    CheckpointBusy int; CheckpointLog int; CheckpointDone int
}
type RoleReceipt struct {
    EdgeSeq uint64; Term uint64; ConfigIndex uint64; VoterDigest string
    LastRunningIsLeader bool; ObserverClosed bool
}
type OpenImageConfig struct { Stamp StoreStamp; Registry DriverRegistry }
func OpenCurrent(OpenImageConfig) (*sql.DB, error)
// DriverRegistry 是按实际 driver connection 与 DBIdentity 统计的接口。
type DriverRegistry interface {
    Connections(DBIdentity) (writers uint32, readers uint32)
    AssertExclusive(DBIdentity) error
}

// internal/listenerimage: 永久 keeper 与短命 Accept view 分开。
type Descriptor struct {
    ID string; Network string; Address string; Path string
    Device uint64; Inode uint64; Cookie uint64; UnixPathInode uint64
    Family int; SockType int; Nonblock bool; CloseOnExec bool
}
type Keeper interface {
    Descriptor() Descriptor
    View() net.Listener
    Pause(controlbudget.Deadline) error
    Export() (Descriptor, handoff.Resource, error)
    Resume() error
    Detach() (*os.File, error) // pristine，只交所有权，不能 Shutdown/unlink
    Close() error // normal owner；Unix 只删除匹配原 pathname inode
}
func Adopt(Descriptor, *os.File) (Keeper, error)
func Capture(net.Listener, string) (Keeper, error)

// internal/brokermaint: wire/private 公用纯类型；codec 不 import broker/agent。
type Phase uint8 // MaintPrepared, MaintActive, MaintEnded
type Record struct {
    CutID string; Holder string; Home string; InstanceID string; IssuerBootID string
    Mode string; IngressServerID string
    Generation uint64; Phase Phase; IssuedBootNS int64; ExpiresBootNS int64
    Term uint64; ConfigIndex uint64; VoterDigest string
    BaselineDigest string; RequiredCapability string
}
type Proof struct {
    CutID string; Nonce string; IssuerBootID string
    ResponseBootNS int64; ExpiresBootNS int64; RecordDigest string; AppliedIndex uint64
    Sig string
}
type LocalLease struct { Record Record; ExpiresLocalBootNS int64; Proof Proof }

// internal/tunnel: Server image 不复用 agent Bundle。
type ServerImage struct {
    Schema string; SchemaVersion uint32; Generation uint64
    IssuerBootID string; InstanceID string; OwnerGeneration uint64
    ReplayLimit int64; ReplayUsed int64
    Listeners []listenerimage.Descriptor
    StateResource string; Endpoints []EndpointImage
    Flows flow.Image // 现 internal/flow/contracts.go:171 的实际类型
}
type ServerImportConfig struct {
    PublicHost string; Lookup TokenLookup; Certificate *tls.Certificate
    Flow FlowConfig; Logger *slog.Logger
}
type FileSource interface { TakeFile(string) (*os.File, error) }
type Salvage struct { Resources []handoff.Resource }
// P06运行态safe-point与P07codec的私有接缝，定义只由P00写。
type serverStateSnapshot struct {
    Records []sealedstate.Record; Resources []handoff.Resource
}
type serverRuntimeCut interface {
    snapshot() (serverStateSnapshot, error)
    flowExport() (flow.Image, *sealedstate.Snapshot, error)
    thaw(controlbudget.Deadline) error
    discard() error
}
func freezeServerRuntime(*Server, controlbudget.Deadline) (serverRuntimeCut, error)
type ServerFrozen interface {
    Export() (ServerImage, []handoff.Resource, error)
    Thaw(controlbudget.Deadline) error
    Discard() error
}
type ServerImported interface {
    Server() *Server
    Activate(controlbudget.Deadline) error
    DetachPristine() (Salvage, error)
}
func (s *Server) FreezeServer(controlbudget.Deadline) (ServerFrozen, error)
func ImportServer(ServerImportConfig, ServerImage, FileSource) (ServerImported, Salvage, error)

// internal/brokerimage: 编排 leaf，不 import internal/broker 或 cmd。
type Mode uint8 // ModeSingle, ModeSoleVoter, ModeQuorum
// ImageRole 固定 broker；所有 slice 规范排序；secret 仅在 sealed private record。
type Bundle struct {
    Schema string; SchemaVersion uint32; Role string; Mode Mode; Generation uint64
    ExporterSHA256 string; CandidateSHA256 string; PreviousSHA256 string
    InstanceID string; BootID string; Args []string; ConfigDigest string
    RequiredDomains []string; StateResource string
    Store storage.StoreStamp; Seal storage.StoreSeal
    LockResource string; Listeners []listenerimage.Descriptor
    Tunnel tunnel.ServerImage; Maintenance []brokermaint.LocalLease
}
type StoreOwner interface {
    Quiesce(controlbudget.Deadline) error
    CloseForImage(controlbudget.Deadline) (storage.StoreSeal, error)
    ReopenCurrent(storage.StoreStamp, controlbudget.Deadline) (StoreOwner, error)
    Stamp() (storage.StoreStamp, error)
    LockResource() (handoff.Resource, error)
}
type ControlOwner interface {
    FenceTransfers(controlbudget.Deadline) (uint32, error)
    ResumeTransfers() error
    Fence(controlbudget.Deadline) error
    JoinStoreUsers(controlbudget.Deadline) error
    StopTransport(controlbudget.Deadline) error
    StartAuth(controlbudget.Deadline) error
    StartBusiness(controlbudget.Deadline) error
    StopCurrent(controlbudget.Deadline) error
}
type Config struct {
    Mode Mode; Store StoreOwner; Control ControlOwner
    Server *tunnel.Server; Listeners []listenerimage.Keeper
    FreezeServer func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error)
    ImportServer func(tunnel.ServerImportConfig, tunnel.ServerImage, tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error)
    Observe controlbudget.Observer; Deadline controlbudget.Deadline
    NewStore func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error)
    NewControl func(StoreOwner, controlbudget.Deadline) (ControlOwner, error)
    TunnelConfig tunnel.ServerImportConfig
}
type Cut interface {
    Export(storage.StoreSeal) (Bundle, []handoff.Resource, error)
    Prepare(Bundle, []handoff.Resource) (*handoff.Prepared, error)
    Thaw(controlbudget.Deadline) error
    Discard() error
}
func Freeze(Config) (Cut, error)
func Import(Config, Bundle, tunnel.FileSource) (Imported, tunnel.Salvage, error)
type Imported interface {
    Activate(RuntimeSlot, controlbudget.Deadline) error
    DetachPristine() (tunnel.Salvage, error)
}
// RuntimeSlot 实现体 P01；每代 replace 与 StopCurrent 串行，旧代不能再次成为 current。
type RuntimeSlot interface {
    Install(StoreOwner, ControlOwner, tunnel.ServerImported) error
    StopCurrent(controlbudget.Deadline) error
}

// internal/cli: deadline 在 RunE 入口一次建立，既有 CallID/RequestID 保持原类型。
type RetryFamily uint8 // RetryReadOnly, RetryStableProcess, RetryUnsafe
type RetryConfig struct {
    Deadline controlbudget.Deadline; Family RetryFamily
    LogicalID string; HasAuthReceipt bool; Observer controlbudget.Observer
}
func DoWithin(RetryConfig, func(time.Duration) error) error
```

`ServerImage.Flows`直接使用 `flow.Image`（`internal/flow/contracts.go:171–177`），Endpoint projection使用现`tunnel.EndpointImage`，不另造FlowImage/第二flowcodec；Server private `permit/receipt/retirement`复用现S5编码。image struct所有导出字段加固定snake_case json tag（SHA256整体为`sha256`、ID整体为`id`、NS为`ns`，不omitempty；Args/RequiredDomains/Applied/Supported无nil与empty歧义）。P0编译stub与taggolden逐field钉住，不能等实施包自己选tag。`DBIdentity`是当前库身份；nonclusterphase用CloseClosed和显式Mode，不能伪造曾运行Raft。Schema digest为Name UTF-8 + NUL + contentSHA256，按Name升序；Applied的contentSHA是依据已存在filename解析的当前embedded支持内容，不伪称旧SQL账本保存过hash。ObjectDigest另外对sqlite_master及table_info/index_list/index_info规范排序编码，校验真实对象结构；同名字内容不同、Applied缺早期记录、账本不变但删/改table都拒绝。

以下接缝**参数与返回类型同样冻结**。函数声明由各实现owner落指定文件，P0用函数类型/假adapter compile probe，不在别人的文件先放以后要改的body。

```go
// brokerimage/contracts.go：cold single也必须在开DB前持同一真实cluster lock。
type SingleStoreConfig struct {
    DBPath string; DataDir string; Registry storage.DriverRegistry
}
func OpenSingleStore(SingleStoreConfig) (StoreOwner, *sql.DB, error)
type PreflightRequest struct {
    CandidatePath string; PreviousPath string; Config []byte
    AllowedArgs []string; Stamp storage.StoreStamp; RequiredDomains []string
    Deadline controlbudget.Deadline
}
type ReaderProof struct { SHA256 string; ABI uint32; Domains []string; MigrationDigest string }
func Preflight(PreflightRequest) (candidate ReaderProof, previous ReaderProof, err error)

// internal/cluster：声明在现node.go/production.go/datadirlock.go，原API不破坏。
func NewForImage(Config, storage.StoreStamp) (*Node, error)
func NewProductionForImage(ProductionConfig, storage.StoreStamp) (*Node, error)
func PrepareImageShutdown(*Node, controlbudget.Deadline) error
func ClosedImageStore(*Node) (storage.StoreSeal, error)
func ReadImageRole(*Node) (storage.RoleReceipt, error)
func AcquireImageDataDirLock(string) (*DataDirLock, error)
func AdoptDataDirLock(string, *os.File) (*DataDirLock, error)
func (l *DataDirLock) Export() (handoff.Resource, error)
func (l *DataDirLock) Detach() (*os.File, error)
func (l *DataDirLock) Release() error
// 现AcquireDataDirLock(string)(func(),error)保留，cold/offline兼容wrapper。

// internal/clusterio：只用net/io/primitive，绝不import raft或NATS。
type SnapshotTicket struct {
    RPCID uint64; Index uint64; Term uint64; ConfigIndex uint64
    ConfigDigest string; PayloadBytes int64
}
type Tracker interface {
    Track(net.Conn) (net.Conn, error)
    Fence(controlbudget.Deadline) error
    Join(controlbudget.Deadline) error
    Counts() (connections uint32, readers uint32)
}
// raft.Consumer/RPC绑定留cluster/transport.go；ticket交给cluster FSM最终验证。

// agentboot：取的是Begin已验证的handoff envelope，不造第二Boot/Restore。
func TakeRole(role string) (*handoff.Restored, error)
// 匹配held Role后单次转交ownership；agent Role不匹配返回nil,nil且留给Claim；
// 未知Role返回error按W1处理。brokerimage Boot随后验证其bundle与sealed private records。

// cli/natsinbox/proxydial：旧API保留，newcaller必须用Within。
func ConnectWithin(url string, id *Identity, deadline controlbudget.Deadline,
    observer controlbudget.Observer, opts ...nats.Option) (*nats.Conn, error)
func ConnectWithin(url, pubKey string, opts []nats.Option,
    deadline controlbudget.Deadline, observer controlbudget.Observer) (*nats.Conn, bool, error)
func OptionsWithin(env Env, deadline controlbudget.Deadline,
    observer controlbudget.Observer) ([]nats.Option, error)
// 上面三行依次属于cli、natsinbox、proxydial，不是一个package里重载。

// execclient/admission_contracts.go：不import cli，实现调用中性budget合同。
type AdmissionConfig struct {
    Deadline controlbudget.Deadline; LogicalID string
    LegacyConfirmed bool; Observer controlbudget.Observer
}
func ProbeWithin(ctx context.Context, nc *nats.Conn, subject string,
    req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error)
func CreateWithin(ctx context.Context, nc *nats.Conn, subject string,
    req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error)
func RunWithin(ctx context.Context, nc *nats.Conn, subject string, spec proto.ExecReq,
    stdout, stderr io.Writer, cfg AdmissionConfig) (Result, error)
// execclient控制客户端ctx可取消admission；预算不绑长任务/raw owner lifetime。

// test/clusterharness：唯一真实process/store fixture接口，不进生产import图。
type BrokerImageFixtureConfig struct {
    Topology string // single | sole-voter | quorum
    CandidatePath string; PreviousPath string; FaultArtifactPath string
}
type BrokerImageReceipt struct { Label string; JSON []byte; Digest string }
type BrokerImageFault struct { Point string; Ordinal uint32; Once bool }
type BrokerImageFixtureAPI interface {
    Checkpoint(string) (BrokerImageReceipt, error)
    ArmFault(BrokerImageFault) error
    Close() error
}
type BrokerImageFixture struct { api BrokerImageFixtureAPI }
func StartBrokerImageFixture(*testing.T, BrokerImageFixtureConfig) (*BrokerImageFixture, error)
func (f *BrokerImageFixture) Checkpoint(string) (BrokerImageReceipt, error)
func (f *BrokerImageFixture) ArmFault(BrokerImageFault) error
func (f *BrokerImageFixture) Close() error
```

`StoreOwner.LockResource`返回borrowed原lockFD，Prepare负责dup；P03的single_store与P04的cluster_store adapter都实现同一StoreOwner，不能藏第二opener或锁。single `serve`仅调用OpenSingleStore、把currentowner交Run；cluster coldRun在第一Open前用相同canonicalbinding。`Config.NewStore`显式取得已Take的原lockfile并负责Adopt/closephase；`Config.NewControl`取得currentStore；二者只在Import的ACTIVE边界后调用；pristineimport不构造store/transport。fixtureReceipt.JSON的shape与§14固定且由P26实现，P27/P28独立检查，不把JSON是否存在当oracle。

资源上界不另放大：brokerBundle metadata≤16MiB（参照agentimage.MaxBundleBytes），handoff外层≤64MiB、resources/files各≤16384（`handoff.go:23–29`），每sealedprivateFD≤32MiB（`sealedstate/contracts.go:14`）；flowlive32/pending32/retirementholes1024/terminalmetadata4096、Endpoint128、每stream8MiB/node256MiB沿现S5。FinalPrepare重验所有上界与requiredreader，不仅preflight验一次。非Linux拒绝发生在任何Take/store/lock副作用前。


P00的最后三处adapter合同也固定，防止listener/SQLguard两边只各写interface。以下是API签名说明；P00用function-type aliases/interface与contractstub编译，不在合同文件写没有body的外部func声明；实际自由函数与receiver body只在§12的唯一实现文件定义。

```go
// internal/httplisten/image_contracts.go；metrics/manifest/sub-http只转交同Owner。
type ImageOwner interface {
    Fence(controlbudget.Deadline) error
    Join(controlbudget.Deadline) error
    Resume() error
    Stop(controlbudget.Deadline) error
}
func StartImage(listenerimage.Keeper, http.Handler, string,
    controlbudget.Deadline) (ImageOwner, error)
// internal/adminsock/image_contracts.go；old New/Start API保留。
func (s *Server) StartInherited(listenerimage.Keeper, controlbudget.Deadline) error
func (s *Server) FenceImage(controlbudget.Deadline) error
func (s *Server) JoinImage(controlbudget.Deadline) error
func (s *Server) DetachImage() (listenerimage.Keeper, error)

// internal/port/image_contracts.go：guard只能在Node.Propose的live db回调内建立。
type MutationGuard struct {
    Expected *Allocation; ExpectAbsent bool
    FromHome string; ToHome string
    ExpectedMaintenance map[string]string // home→canonical current record；空value=明确不存在
    Proofs map[string]brokermaint.LocalLease
    NowBootNS int64
}
type AllocationIntent struct {
    SID string; NID string; Name string; LocalPort int; DesiredPort int
    CreatedByFP string; HomeBroker string; RebuildOff bool; Config *Config
}
func PlanReassignHomeGuarded(*sql.DB, MutationGuard, string, time.Time) (int64, *cluster.Command, error)
func PlanAllocateGuarded(*sql.DB, MutationGuard, AllocationIntent) (*Allocation, *cluster.Command, error)
func PlanRotateAllocationGuarded(*sql.DB, MutationGuard, AllocationIntent, time.Time) (*Allocation, *cluster.Command, error)
func PlanFreeAllocationGuarded(*sql.DB, MutationGuard, time.Time) (*cluster.Command, error)
```

P14在上述方法内再读取并校验fullidentity/maintenance当前record与proof资格，guard不能仅相信调用方bool；unknownproof拒绝plannedcut，maintenanceexpired按原故障策略。rotation的free+insert是**同一Command/transaction**、共享fullidentity条件，不能第一Propose free后第二Propose重新挑target绕维护；返回allocation是expectedprojection，P13必须commit/readback后确认才发started/directive。旧PlanReassignHome等cold/testAPI保持，P31扫描production home-mutator不得绕过Guarded方法；permissionrevoke独立fullidentity路径始终可执行，不被维护抑制。HTTP/admin新Image API全部ctx-free，由owner内部建立标注controlroot，Stop关闭当前view/handlers，最终keeper关/Unixunlink仍只由currentterminalowner执行。

### 2.2 broker private-state inventory（闭合 descriptor）

固定 namespace `broker-image-v1`、private record `broker-state-v1` 与 `broker-tunnel-state-v1`；resource ID 只 `[A-Za-z0-9._-]`，不用冒号。`brokerimage` 自己解析自身 private FD 中的闭合 kind 集合；tunnel 自己解析自己的集合；现全局 `sealedstate` 的 S2–S5 reader kind 词表不扩展，避免旧 agent 因 broker kind 失配而被假冒“不兼容”。共享底层 sealed FD 验证、S5 flow codec 与 S3 handoff API 原样复用。

| record | 精确状态字段 / 守恒 | descriptor owner |
|---|---|---|
| `broker-runtime` | Role/Mode/Args/ConfigDigest/Generation/BootID/InstanceID；exporter/candidate/previous SHA；RequiredDomains；ACTIVE boundary；当前 cut id | P02 |
| `store` | canonical DB/lock identity；exact applied/supported migration digest；journal/effective synchronous；SQL cursor+term；Raft log/config index+digest；逐对象 phase / driver counts / checkpoint triple | P03，public 类型 P0 |
| `control` | admission generation；必需 handler/loop names；auth/control ready observations；当前 maintenance proof cache、历史 dwell/planned-return baseline；**无 NATS conn** | P01/P15，codec 唯一 P02 |
| `server` | sessions 完整 metadata、flow cfg/issuer identity、ticket key sealed secret、一个 shared replay budget、owner/carrier generation、next seq；无 heap TLS/yamux | P07 |
| `endpoint` | EndpointID、full allocation `{Port,SID,NID,TokenHash,Home,HomeEpoch}`、listener ref、phase、deadline、Fired/CleanupPending、pending raw FIFO/slots/waiters、per-IP policy、accept/open cursor | P07 |
| `permit/receipt` | requestID、cutID、owner/nextOwner、deadline、digest、consumed/abort；原 response 与原 retirement 语义 | P07（复用 S5 codec） |
| `fences` | `killGenPort/killGenSID/killGenNID/forgotten/closedAllocation`；每项 generation；Closing owner refs 为 flow+permit+receipt+pending+cleanup 并集 | P07 |
| `resources` | ID→kind/purpose/FD provenance/OFD identity→唯一 terminal owner；Taken/untaken/salvage/prepared aliases；private FD 当前 cut owner | P01/P02 |

每条 descriptor 有版本、字段名、类型、编码、最大长度、是否 secret、兼容 reader；append-only golden 更新拒删/改类型。P07 对私有 map/heap cursor 逐项作 reflect 或显式 descriptor 漂移闸，不能只盘点 `Bundle`。新字段必须同时有 runtime→Export→Import→inventory 测试；故意删 runtime 映射或 decoder 字段能变红。ticket key/token/JWT/PSK 不进日志/public/wire；collector 仅 digest。

## §3 store、writer、flock 与耐久

### 3.1 唯一开库与纯 preflight

canonical DB 的 parent DataDir 作为唯一锁域，锁文件沿用实际 `tether.lock`。plain path 与允许的 local `file:` URI 必须归一到同一绝对 pathname/DSN；production hot path 拒绝 in-memory、未知 query、任意 cache/locking pragma、symlink 或 hardlink alias、DB 与 cfg DataDir 不匹配。cold production single/cluster、offline writable tools 同样先校验 binding、AcquireDataDirLock 再打开；embedding/in-memory 测试仍可 cold，用明确 `image-ineligible` 标记。不能为了避免测试工作而把所有 `storage.Open` 改成 production 文件限制。

锁继承是原 open-file-description：Export 只借原 lock file，Prepare dup 保持锁；original CLOEXEC；Adopt 接管该 inherited FD 验证 inode/device/owner，不 `open`/`Flock`/`LOCK_UN`。cut/old wrappers 的 Detach 不能解锁；normal current owner Release 才解锁。独立 subprocess 使用真实 `AcquireDataDirLock` 全窗并发探测，另在 **store closed、exec 尚未调用** barrier 保持足够 probe 完成一轮；任何成功即产品红。unlock mutant 必须在 barrier 确实 Acquire 成功，证明 probe 能抓缺口；normal Stop 后成功为正控。连续 polling 不声称能观测每个纳秒，逻辑 OFD 守恒加确定性交错共同证明。

candidate smoke 用自己的短生命 binary 验证编译 ABI/RequiredDomains、platform、配置纯 decode、embedded supported migration filenames/content SHA、prev reader 能读当前 image 域；读取 candidate artifact bytes/当前 running `/proc/self/exe` hash。它不得 import/open Node/SQLite/Bolt/JS、启动 broker或获取 production lock；纯 schema 判断依赖 exporter 提供的 stamp，不“read-only 看一下真实库”。double-open hang 负控为独立 helper：entry barrier 证明已开始真实 Bolt second-open，监督 deadline 到后 kill+wait；旧 owner 释放后 helper 能成功作为正控，不能将没启动 helper 的 timeout 当通过。

### 3.2 closing phase 与实际 writer 证明

storage 在 driver connection 的 Open/Close 层按 `(device,inode,mode)` 注册 writer/read-only connection，含 pool、backup、临时 snapshot reader；lazy `sql.Open` 要显式 Ping/注册后才进入 census。`MaxOpenConns(1)` 只约束一个 pool，`ProbeWriterLock` 只作辅助冲突探测，二者不能代替 registry。正常 cut 实际 live driver writers ≤1，FinalSeal 必须 writer/RO/backup=0，reopen 后又唯一 1。instrumentation 不新增持久 schema，不将计数写入 SQL。

绑定同一 cut deadline 的顺序：

1. pure preflight、running-SHA/配置/域、legacy/live resources/transfer census；fresh topology；必要 maintenance observer ACK；原业务此时继续 pump。
2. fence 当前业务 NATS/admin/HTTP SQL admission、transfer creation、new tunnel REGISTER/public Accept；保 listener keeper。冻结驱动 loops 的准入，取消 **可取消** 的 producer/read/backup RPC，真实 join；已 admitted 副作用完成 commit/reply 账后 join。
3. clusterio 阻新 inbound/outbound Raft work；关闭 tracked stalled snapshot payload/connection，使真正 `io.Copy` 退出；snapshot persist reader、备份 reader、accepted transport handler 逐个 join。已 committed FSM Apply 不能因 cut cancellation 跳过；等待其 durable commit 或 fail-stop，无法有界收敛则拒绝 cut并继续合法 owner，不能遗弃 `Shutdown().Error()` goroutine。
4. `Node.Shutdown` 显式在 Run 内；Node 保存逐对象 phase，shutdown Raft 后 join role observer；close transport/Bolt；取最后 SQL cursor/term/config/log receipt。所有产生 SQL work 的 callbacks/loops 已结束。
5. cluster WAL 执行 `wal_checkpoint(TRUNCATE)`，检查 busy=0、log/done 的成功语义；然后 Close RO/backup/writer，registry 验证 0。single 保原 `DELETE`/`WAL` journal：WAL 才 checkpoint；DELETE 写 `not_applicable` mode receipt 并证无活跃 transaction。不得自动把 single 改成 WAL。effective `synchronous=FULL` 在真实连接读取并校验，DSN 不能覆盖它。
6. 合并实际 close errors/phase，所有要求对象 `CloseClosed` 后生成 `StoreSeal`。再取 **最终** server/late-revoke/FD export，重验 reader domains/size/FD count；这时才 `handoff.Prepare`，绝不引用 preliminary export 的关闭声明。
7. admin reload 结果是 `armed`，通过不访问 DB 的 responder 发实际 reply、checked Flush / Unix write；已 admitted reply 的 sender join。随后停止 NATS transport/auth 并 join，无 DB 用户仍在；核实际 inherit whitelist，再在 Run 调用 Exec。

阶段收据是按 object 的 `open/stopped/closed/unknown`，Stop 与 Close 不混。partial New/Close 报 nonnil owner + status；所有返回对象立即入当前 owner registry，包括 `file != nil && err != nil`。unknown writer、未 join transport/reader、checkpoint busy 或 conn count>0 **禁止**另一次 reopen/redispatch。不能把 generic error 都当“已关”并造第二 pool。旧 constructor 已关闭 injected transport 的事实保留；新 constructor 额外保证被接受 RPC 全 join。

exec 返回 error 时 Prepared 恢复每个修改过的 CLOEXEC 位并检查恢复错误；关闭 cut-owned prepared/private dup；原 lock/raw/listener 仍归 current slot。确认 old writer closed 后 `OpenCurrent` 当前 pathname，不 restore snapshot，不 rerun migrations；替换 Node/DB/authority/NC/root，auth→flow→business 顺序 Thaw。若未知 close 状态，保持 fenced 单 owner并报告明确失败；不可在后台超时后放弃已开始的 Close。


Run的承重调用顺序冻结如下；`Node.Shutdown()`这次实际调用不能藏到Run退出defer。clusteradapter的CloseForImage只收集已完成关闭收据；singleadapter在同位置完成自己的checkpoint/close。

```go
cut, err := brokerimage.Freeze(cfg) // 已join当前control/store用户与carrier
if err != nil { returnToCurrentRun(err) }
if currentNode != nil {
    err = cluster.PrepareImageShutdown(currentNode, deadline)
    if err == nil { err = currentNode.Shutdown() }
    // Shutdown热路径内：finalcursor → checkpoint → RO/writerClose；记录每phase。
}
seal, err := currentStore.CloseForImage(deadline) // 不重复关已closedNode
bundle, resources, err := cut.Export(seal)       // 校验err/knownclosed后才调用
prepared, err := cut.Prepare(bundle, resources)
// checked armed reply、DB-free sender join、StopTransport、actualFD census；
// cut latch内ExecCommit，dirtyrevoke必须废弃prepared并重Export。
err = prepared.Exec(candidatePath, bundle.Args, sanitizedEnv)
// 返回error：checked flagrestore、currentStore.ReopenCurrent、slot replace、Thaw；
// 原Run继续，不cancel，不return-to-serve；真正正常停止才StopCurrent。
```

伪码每个error点按§4.2分支处理，不允许在err非nil后继续Export/Prepare。P01控制编排；P11负责Run唯一出口；P04负责Shutdown实际热关闭；P26独立验证调用顺序与storeclose事实。

### 3.3 `OpenCurrent` / durability oracle

`OpenCurrent` 只允许已存在文件，校验 canonical dev/ino、exact migration set+digest、journal/synchronous 与 expected stamp；不 CREATE、不 ApplyMigrations、不 Bootstrap、不切换 schema。`NewForImage/NewProductionForImage` 复用该 opener、现 Bolt/Raft path，不调用 cold `New` 的 migration/bootstrap。同机锁仍 held；先获取 current SQL cursor和 ACK witness，再允许 Raft replay。manifest 不携数据库 payload，只携身份、floor、schema/close receipt；rollback 输入始终同一当前 disk。

独立 writer fixture 为每个收到成功 ACK 的 RequestID 写唯一 nonce/bytes，ACK日志在客户端。首个 hot SQL reader **在首次 Raft replay 前**核 ACK rows、SQL cursor ≥sealed floor、term 合法；停 replay barrier，覆盖旧 DB/丢 WAL mutant 必须在此红，不能让 Raft重放补行洗绿。replay 后再验证 prefix + dedup ledger + 新 ACK、ReqID 恰一次效果。snapshot/config index 与 SQL command floor 分别记；合法新 raft barrier 不要求 SQL applied_index 同步上涨。crash-window 单独标 fail-closed/OS crash边界，不拿 SIGKILL 的 restart 等价于 planned hot cut。

### 3.4 真 runtime InstallSnapshot floor

保 `NoSnapshotRestoreOnStart=true` 与 restart 幂等 replay 测试，删除 C 稿不可达 startup Restore mutation。runtime 网络 InstallSnapshot 则必须保护：clusterio 包装当前 transport Consumer，在交给 Raft 前绑定该 RPC 的 snapshot index/term/config metadata、reader 与 request identity；完整 bounded stage payload 先做完整 schema/integrity/SQL cursor 校验，失败返回 error。FSM 在同一 mutation serialization 下、in-place restore 前读取 **当时** current command floor；`payload.applied_index < currentFloor` 必须 error。已 Apply 新 ACK 后再 Restore旧 snapshot 的强制交错不能用“初始 sealed floor”放过。

拒绝会让 Raft install future 非 nil error，不能返回 nil/no-op，否则 raft v1.7.3 `raft.go` 的 InstallSnapshot 成功分支仍更新 lastApplied/config。验证 metadata/config/log-prefix 与 payload，拒绝后 DB inode/rows/cursor、Raft configuration/index/term 全不变。合法 ≥current command floor 且 matching metadata 的新 snapshot/catch-up 必须成功；不要把 all-entry Raft index 与 SQL command cursor作相等判断。Snapshot Persist/Restore staging与取消 token均归 P05，实际 socket join归 P04/clusterio；P26 独立 transport→FSM seam test，不能两边各说对方会检查 metadata。

## §4 Run 内 exec、boot 与 current-state failure

### 4.1 一次 claim 与验证

保 `cmd/tether/main.go` 已有唯一 `agentboot.Begin` collector；增加 `TakeRole("broker")`，在现非-agent `Abandon` 前把经过 manifest/private validation 的 broker dispatch 转给 broker Boot。不能按 argv/hint 决定是否读唯一 envelope；hint 仅 chooser/诊断，sealed Role才权威；不调用 Restore 两次。合法 broker image 同时 wrong argv+missing hint仍由 broker Finish 使用 sealed correct args redispatch；未知/bad manifest遵守 S3 W1 的 foreign FD census/lost cleanup，不 cold-open生产库。

验证 PID/EUID/boot、generation、Role、exact config digest/argv、candidate/previous **具体 running SHA** 与 RequiredDomains、大小/count、resource ID、sealed FD、raw/listener identity与 flags。第三 artifact SHA不可读；磁盘 staged SHA 与正在运行 SHA 分开。manifest 掩不住真实 `/proc/self/fd`：Exec 前除明确 0/1/2 外，实际所有 non-CLOEXEC FD 必须与 Prepared typed whitelist 精确相等；未知 FD拒绝。Import 以 actual FD identity multiset 查 cross-ID alias；Prepare/Merge 的 filename/FD number 校验不足以证明唯一 OFD owner。intentional keeper/prepared aliases有不同 purpose、同一最终 owner；禁止两个最终 owner分配同一 OFD。Linux socket cookie / devino / kcmp 可用处联合 provenance；无法证明身份则 fail-closed，不偷偷读 bytes 作验证。

### 4.2 importer salvage 与 ACTIVE

每个 TakeFile 返回即先登记 nonnil file再看 err；raw/state/listener/lock及成功 Import 后外层失败均返回全部 taken salvage，Restored 只留 untaken。`DetachPristine` 使 sleeping owner relinquish，不 Shutdown raw，不 Release flock，不 unlink Unix；MergeResources 逐 ID与OFD ownership去重，不忽略 merge error。所有新 sealed FD都有当前 cut owner，replacement先转移再close旧FD，Thaw/Discard/失败都有显式 close。

ACTIVE latch 在**调用第一 store/transport 潜在副作用 constructor 之前**置位；Open失败也不能回 pristine。它不等 `Imported.Activate` 成功、first pump、first ACK。pristine decode/Take/validation失败可 Repack同一 generation到 sealed previous；open后任意 failure只能取 current runtime、真实 close receipts、新 server offsets/fences、新 ACK库、新 generation+1 的 `FreezeCurrent`，再 exec previous。partialconstructor没有 known writer closed则不做第二 opener，按 §3 failure 合同收回并保唯一 owner。

| failure 点 | 唯一合法动作 | 禁止动作 / 必测 mutation |
|---|---|---|
| preflight / admission fence 前 | current继续运行，关闭新 private/dup，返回 typed deferred/error | 不 cancel Run、不重建 flow |
| 部分 fence / store仍open | 同 cut Thaw原 graph，maintenance release CAS | 不拿旧 preliminary image覆盖 late revoke |
| old store已close / Exec返回 | 校验 closed seal→重开当前库→current slot rebind→auth/flow/control Thaw | 不让 captured old nc/db defer关 current；不 rerun migration |
| held pristine partial Take / constructor尚未调用 | taken+untaken完整 merge→Repack原 generation→prev | 不 Detach成 Close、不吞 nonnil-file error |
| 首constructor已开始 / 尚无业务启动 | ACTIVE，收敛当前 partial owners→current close/new cut | 不重用初 cut、不假称未激活 |
| candidate已有新 ACK/新 bytes/撤权 | 当前 SQL cursor/flow offsets/fences完整新 cut→generation+1→prev | 不恢复任何旧库/初始 flow cursor |
| previous不支持当前域 | 留可运行 current owner；不能exec不可读prev；明确失败 | 不循环 redispatch，不把资源丢失叫 rollback成功 |
| close unknown | 单 owner fenced、收敛真实关闭；无法完成则明确 fail-closed | 不 parallel reopen、不能丢弃后台 shutdown |

普通 SIGTERM/Run return/ctx cancellation 通过当前 slot真实 StopCurrent；新代 NC/Node/listener/raw/backup皆关闭，probe此后能获锁，原Unix path正确删。P28 必须组合“Exec error→reopen→new ACK→normal stop”，以及“ACTIVE new ACK+offset→prev→normal stop”，不能只分别测组件Close。

## §5 产品入口、scope 与拓扑

single 以新的本地 `broker upgrade reload` 调当前 admin `OpReload`，校验 installed candidate SHA/prev/readers后同PID交接；不改 `node upgrade` 的 agent 对象。实现放现有 command 文件，新增逻辑在 internal/cli，不能新增 cmd production file透支57。cluster `cluster upgrade --broker-only` 是产品 scope，默认旧 whole-host。新 `PlanScope` 独立于 `Step.BrokerOnly`；B lane native product drive不得发 `reexec-agent`，共址agent PID/startticks/runningSHA以及SS connection必须不变。

sole voter 每次 fresh Raft configuration恰一个 voter且是self，local caught-up，无 membership/op in-flight，使用生产选举/lease timer，同PID Node关闭/reopen，写停顿≤5s。plan 输出 `N1WriteStall=true`，诚实解释短暂写拒绝/重试；健康1-voter不返回 `CodeIsLeader`。N=2继续当前 quorum/write fence，不在S6承诺hot可用路径。

N≥3 每 STEP 重新读当前 leader/config，不沿用 plan时 staticTransferTo；若目标此刻为leader，先向仍留在线、caught-up voter transfer，验证新leader VerifyLeader和旧target follower，再cut。constructor接并持续消费 `NotifyCh`，保持 role-edge序号+term/config index/voter digest；cut前后检查 edge序号，leader→follower ABA 或membership变化都使cut失败，不能两次IsLeader false洗掉中间升主。observer直到真实raft shutdown后join并出最终 receipt。

reload `armed/Reloading` 不是完成。local `RuntimeReport`、authenticated remote health统一新增 `BrokerImageStatus`；`RunningSHA256`来自正在运行的 `/proc/self/exe`，含 generation/phase/cut/mode。driver用同operation deadline观察预期candidate运行、当前generation、ready/维护结束；unknown结果只query，不能重发unsafe reload。same release比较实际SHA，相同Version但不同SHA必须执行；staged SHA一样而running旧仍不算完成。

S6在transfer创建与reload准入共享gate下通过ControlOwner.FenceTransfers关闭新transfer创建并取 live census；gate包括所有prepare/provision/create入口，busy或readerror返回前ResumeTransfers，成功cut一直持fence至exec/newcontrolready，Exec返回在当前Thaw才恢复。既有live transfer继续progress，不能停它来制造census0。任何NonTerminal/读错都诚实拒绝：成功读且非零→`deferred(transfers_in_flight)`；读错→明确 error不能忽略。T lane先证真实tier-B同transferID、bytes<total，reload deferred、runningSHA不变、原 transfer完整finish，terminal后retry才成功；无transfer的negative arm必须成功进入cut。tracker import/midflight续传留S7。

## §6 Server Freeze/Export/Import 与原 listener

### 6.1 keeper / view / handler join

`listenerimage` 唯一实现TCP/UNIX listener OFD keeper与可暂停Accept view。View.Close只唤醒/停止本代Accept，keeper仍LISTEN；Freeze不是关闭原listener再`net.FileListener`复活。listener validator检查stream、SO_ACCEPTCONN=1、family/localaddr、nonblock、CLOEXEC、devino/cookie；raw validator仍要求0。明确FileListener会dup的来源，导入后不同时留未登记wrapper。对Unix在建立keeper前设置`SetUnlinkOnClose(false)`，hot `StartInherited`不执行admin stale-path probe/remove/bind，normalowner只unlink匹配原pathname inode，不能删新代/别人的socket。

HTTP使用可追踪ServerOwner：Fence停止accepted request新业务/DB准入，取消读/备份work，tracked handler count归零后才能seal；`http.Server.Serve`返回或`Shutdown`返回本身不能代替handler/keepalive真实join。admin已有accepted Unix handler也同样计数；reloadreply保一个明确DB-free路径。tunnel控制Accept可关闭尚未完成TLS/REGISTER的controlsocket使agent有界重拨；不可保存已消费TLS header的rawFD假装重做TLS。public原socket不是控制socket，不可如此关闭。

测试至少一个原业务socket和一个cut中freshconnect；内核backlog成功只证明TCP握手，freshconnect须接到预期业务响应≤5s，`ECONNREFUSED=0`。同时检查 listener pathname/inode/原OFD及target原 acceptID，不能用connected代理补偿原FD断裂。

### 6.2 freeze 与最终撤权

跟踪五种真实对象：REGISTER handler、carrier pump/monitor、Endpoint accept、timer callback、cleanup callback；counter在真正创建/接收处记账，不按固定goroutine数。Endpoint新增 `Paused` safe point独立terminal stop：保pendingRaw、pending slot数、FIFO waiters、nextSeq与accept→OPEN cursor；不能触发当前 stop branch关闭pending。`AfterFunc.Stop=false`等待该callback显式done；timer=nil但Fired/CleanupPending仍须导出；callbacks在锁外join。deadline wake后不能循环重新Accept逃过pause。

顺序固定：server admission/attachment fence → Close underlying TLS以取消yamux阻塞（不是等streamdeadline）→ carrier worker和monitor全部join → flow Manager safe point / Freeze。S5 `n>0,err`先计真实offset/ACK/credit，再做终态决定；不允许关闭carrier后丢已写字节。首次Detached保持原S5 8s absolute deadline，已Detached绝不刷新；剩余不足完成cut则defer，而不是升级豁免延长。

Export保存§2.2全server/Endpoint/fences和S5flow/permit/receipt/retirement。sessions不是live carrier认证；legacy有FD/active pump用既有 `legacy_session_busy`诚实拒绝，空/closingmetadata仍保留。broker所有flow共享同一个budget pointer；agent的exec/pty/flow/SS总预算仍由其S5Runtime持有，不把两个进程预算互相覆盖，验证本域ownedcapacity与总sum，不把Used覆盖或每flow新建budget。hot配置保issuerBootID/InstanceID/ticketkey/owner代，不调用cold随机factory/advanceProxyGeneration/stranded cleanup。

最终cut与revoke在线性化点互斥。seal前已提交revoke必入最终fence/Closing owner refs并清理所有victimaliases。Prepare之后、ExecCommit之前，已准入的late hard revoke若先取得cut latch：标dirty→raw同OFD `SHUT_RDWR`→枚举close全部victimlistener/rawprepared/salvage/originalaliases→**Close整个Prepared及本次封存副本**→从当前fences/owners与同一已closed StoreSeal重新Export/Prepare。禁止修改sealedmemfd或假设S3有RemoveResource API；超出remainingdeadline就currentreopen/Thaw仍合法survivors，victim撤权不逆转。正常timer/cleanup必须在此前join，只有未完成的localrevoke能够赢这一线性化点。

ExecCommit在同cut latch下确认dirty=false、finaldomain/FD/role/maintenance收据仍有效并调用Prepared.Exec；以后旧admission不ACK新mutation，等cut释放或返回temporary error。不得在持该latch时join需要它的callback；所有callbacks先排空，syscall.Exec返回error后release再处理pendingrevoke、重新授权Thaw。listener不能用Shutdown代替枚举close，全部aliases移除后才真停止该revokedlistener。fullallocation identity防late Close(A)误杀同port新B；fenceprune按flow/permit/receipt/pending/cleanup引用并集，import不能reset retainedrefs。所谓inflight=0仅指实际admission/RPC/worker/callback，不是把Closing refs写0。P28在Prepare后/ExecCommit前barrier强制已ACK撤权，验证旧RESUME拒绝且survivor继续，再从下一Prepared真实exec。

### 6.3 import 激活

验证sealed/private与FD→adopt lock→前置ACTIVE→OpenCurrent+pre-replayACK→建立current authority/leader apply responder→auth usable→恢复budget/fences/Endpoint/pending/slot/timers→freshTLS/REGISTER/RESUME逐allocation查询当前SQLauthority→允许原raw delivery→business/admin/HTTP/publicAccept→fullcontrolready→维护结束CAS。SQL authority含fullidentity、epoch、pins/port、permission、expiry与allfences；exportedsession表不能替代fresh授权。ticket/Boot/Instance保持，carrier generation递增，旧owner/oldpermit不能抢。brokerBundle.Generation每次daemonexec+1；ServerImage.OwnerGeneration保原FlowConfig值，**Endpoint.ownerGen是agent owner fence，broker-only cut不能+1**。`flow_endpoint.go:483–520`只在agent的Prepare/Activate推进该值，`flow_client.go:359–410`的PrepareHandoff也属于agent迁移；S6走普通carrier detach/reattach、只按现attach规则推进carrierGen，已consumed permit/receipt不重造。T28加“错误把broker generation写入Endpointowner”mutation，确保newcarrier不被当成第二agentowner。SS AEAD/key/nonce仍在agentS5 owner；broker不得重放plaintext或新建AEADheader。

## §7 ≤8s maintenance、rolling holder 与 home 决策

### 7.1 durable record、proof 与明确 entry

使用既有 `cluster_meta` 与 `OpClusterMetaSet/Clear` 写规范literal record，固定key prefix `broker_image_maintenance.`；不新增OpType/commandVersion/migration。single本地等价owner状态（由同image保留）；cluster记录由currentleadercommit。rolling lock与短维护不同：保原15minrollinglease，新增随机Holder，acquire/readback/renew/expire/release均比较markerholder+expectedlease；旧无holder请求可兼容只读/无锁的旧普通流程，但不得操作新holder，返回既有`CodeBadRequest`。A失去租约后迟到release/renew不能伤B。newmarker使用规范`broker-roll-v1:`prefix+holder/leasepayload；现无参数PlanClearUpgradeActive/PlanRenewUpgradeLease/PlanExpireUpgradeLease保兼容但SQLguard拒该prefix，不能删newholder。新Expected变体在applyMu内读取并对marker+lease双值CAS，lease/marker同transaction清除（材料化eligible CTE或等价一次条件delete，不能第一条删marker导致第二条丢CAS）。reconcile_upgrade_lock的旧ROexpired观察仅hint，Propose内重新读currentholder/lease/expiry、commit/readback确认后才released日志；新rollcrash后的15min lease也能用currentexpectedholder清理，不留下永不reap的marker。

maintenance `prepared→active→ended` CAS绑CutID/Holder/Home/InstanceID/IssuerBootID/generation/currentterm/config/voterdigest；TTl从首次issuer创建固定≤8s，不能续期或因新leader/重新读延长。创建TTL之前，先真probe目标home健康、proxy_ready与currentinstance匹配、当前route/heartbeat可信；有现真实故障或未知healthy不启动maintenance，不清dwell造健康。cut前全current mutation voter已apply，以及所有当前可达且受该home影响agent完成 **新增 S6 capability + signed challenge proof ACK**。每个ACK附签名`remaining_ns`（安装后的localexpiry减localacknow）；issuer用对应challenge发出t0+remaining作保守保护下界，RTT再消耗寿命。最终取issuerexpiry和所有ACK保护下界的min，确认该下界尚够5scut；不能只看issuer还有5s而忽略某agent/voterproof更早到期。缺 capability/缺ACK不进入cut，明确`CodeBadRequest`带`maintenance_observer_unsupported` detail；普通旧wire仍工作，不能当busy伪装。维护窗口需剩余够正常5scut；最终校验角色/config/holders仍相同。不把S5agent指定为“当然懂维护”。agent先装S6能力可作为明确entry，见开放问题1。

跨机不可比较issuer BootNS与local BootNS。observer在发nonce时记local t0，收到签名proof时记t1；issuer回固定record的`ResponseBootNS/ExpiresBootNS/BootID/CutID/digest/appliedcursor`。保守localexpiry=`t0 + max(0,issuerExpires-issuerResponseNow)`，检查t1<expiry、nonce/record匹配；RTT与排队消耗寿命，后续proof只能取min，不重新给8s。所有voters在cut前缓存proof，cache同image导出；leader变更使用原localexpiry与原dwell基线，不“now+TTL”。迟到首次response仍用原nonce t0；unknown/changingBootID/无proof不能保护并拒绝plannedcut。相同内核boot的samePIDexec保持issuerBootID，不随机重造。clock wall±30s不影响；crash后至多原≤8s到期，正常故障恢复继续，不能120s屏蔽死home。

### 7.2 所有 mutator 在 writer critical section 重验

维护覆盖 **from 和 to** home：reaper healthy/unready两branch、free+insert rotation、初allocation/handsoff、manual rebalance、auto return rebalance、leader-on-return、普通reconcile。决策在 `Node.Propose` 的applyMu内读取currentmaintenance与fullallocation identity；不能在前面挑完target后只在SQLWHERE比epoch。bakedSQLWHERE绑 `{Port,SID,NID,TokenHash,Home,HomeEpoch,State}`、proof/CutID expectedrecord；维护record更新也同writer串行。commit后readback实际新identity/epoch确认更新，Propose nil+0rows不等成功。`started`事件、home directive仅在commit/readback后发；对stale A→新B的mutation影响0。控制ACL撤权/permissionrevoke不被维护挡住。

active期间暂停matchinghome的故障dwell增长，保存此前真实dwell和planneddown/returnbaseline，不清零，不全局暂停reaper/auto-rebalance，不把ready强写true；其他home照常处理。ended后plannedreturn沿baseline识别，不触发故障migration/rotation；crash/expiry按保留历史恢复普通故障动作。baseline durable digest+每home本地proofcache，不因leaderchange或observerrestart得到新期限；无cache不可重新声称stillmaintained。manualmove在plan返回typeddefer并保未执行状态；globalauto pending不清掉其他home。

### 7.3 agent silence-escape 与本地 route

新增 signed maintenance command/reply（已有forwardedcommand的新verb，明确auth/ACL）和NodeRegister capability additivefield。agent绑定当前connectedbrokeridentity、route order、homeinstance、CutID、proof固定expiry；cluster只用actualconnected_broker_id映射，single的该旧field本来为空（`broker/home.go:39–46`），因此signedRecord额外绑Mode=single与IngressServerID（源NC实测NATS server ID），agent核当前nc.ConnectedServerId与同一routeorder/instance，不能把空ID或hostname当matching。不改变single空SQLhome、不铸clusterroster；外置NATS在S6保持身份；silence=2且topologywake再失败时matchingcut不escape到另一home，不重选/repin，维护到期恢复现normalrebuild。newroute/retire/revoke/permission更高优先级且fullidentity验证，maintenance不能保已撤权socket。ACK必须真正装入hold并有receipt，不是收到msg就回复。N≥3 fixture `AUTO_REBALANCE=on`，普通reaper与rosterwake真实运行；baseline tests涵盖healthy/unready、historydwell、oldleader首跳、新leaderreturn、from/to选择、silent2+immediatewake与expiry。

## §8 auth first、一次请求预算与 attempt 观测

### 8.1 auth 无循环依赖

旧owner在storeclose前 fence/auth/PIN handlers并join；new先currentDB/forwarder、leaderPIN/apply responder、纯permission/capability服务，再`QueueSubscribe`callout并checkedFlush，再确认真实authresponse签名/JWT与一条隔离的真calloutCONNECT。peerstaticauth不能证明callout可用。provisioned read身份可在writeleader尚待就绪时认证，但PIN/create仍经过currentleadercommit，不绕ACL。authusable事件之后才能启agent/session/courier/reliableexec/business，fullReady在existingJSidentity读校验与所有latewiring后发，旧ReadyCh不再早发。

观测 `{freeze_begin, auth_off, exec_enter, boot_claim, store_open, replay_gate, auth_sub_flush, auth_reply, auth_usable, control_ready, maintenance_end}` 同localbootmonotonic/ID；calloutblackout从old真实lastusable至new真实usable，不能把Flush→Flush或日志两点称认证黑窗。drill freshCONNECT cohort带有效receipt只改变client可重试资格，不预认证全部连接回避window；独立no-receipt/PIN/ACL错误仍验证terminal边界。

### 8.2 统一 helper

ctl读动词及stable-process admission的每RunE **入口**建deadline=`min(userctxdeadline, operation_start + RequestLatency5s)`，banner/health/discovery、receiptrefresh、DNS、多URLconnect、proxy/SOCKS/HTTP CONNECT、TLS/WSS、privateinbox探测与fallback、reliableprobe/create、readrequest、backoff都传sameabsolute。不是connect后再给request5s。长运行任务在true_started后沿原userctx继续；cluster滚动总操作沿组件/用户deadline，每个logicalcontrolRPC各自5s，不把整个多hostroll硬塞5s也不让它的新请求脱离5s。best-effort banner/refresh在剩余不足时明确省略并留attempt，不在主结果后拖进程退出超5s。底层`nats.SkipHostLookup()`+CustomDialer接context-boundResolver/DialContext（pinnednats.go v1.52.0有该API），TLS/WSS handshake用sticky conn deadline贯穿CONNECT；handshake成功移除deadline并join取消hook，不能5s后杀长exec/run。proxy选项不能固定10s，inboxfallback不能第二次重置全额，nohostlookuplookup不能落回无ctx的net.LookupHost；禁止goroutine+timeout遗弃真正dial/handshake。

三个闭合family：read-only可retry；stableprocess保同CallID/handle/RequestID/leaseintent/canonicalpayload，最多一次真实启动；unsafe在sent后结果未知只query可用receipt或报`control_result_unknown` detail（既有errorcode），不能自动重发mutation。temporary pre-send connection、noresponders/timeout在deadline内可retry；明确ACL/revoke/invalidsig/PIN/authidentity不符/explicitunsupported等terminal即时返回；eligible CONNECT的authviolation无法区分blackout与revoked时，只在同绝对deadline内按S1重试，耗尽保留原最终autherror，不能改写为普通timeout。只有S1 historicalreceipt合格且非首次login可重试authcallout空窗。无receipt首次authviolation不变成5s权限试探。

`ProbeWithin` timeout/noresponders不是ErrUnsupported；同budgetretry直到成功/耗尽。只有真实explicitunsupported或先前authenticatedversion/capability证明legacy才允许原S1legacy路径；未知server空窗不能猜legacy。2.1s blackout的可靠shortexec必须reliablecreate=1、legacyPUB=0、effects=1；explicitunsupportedcontrol要求legacyPUB=1/effects=1。aftercreate sent任何fallback禁止。固定CallID与transportcorrelation各有目的，不因重试生成第二logicalID。

### 8.3 observer 与 collector

neutral `controlbudget`只定义预算/Attempt；P18helper、P19dial、P20inbox、P21execclient、P23RunE都使用同Observer。每个attempt通过Observer.Begin分配同LogicalID的全局ordinal与startedtime，Record完成；phase闭合值为`dns,proxy,tls,ws,connect,inbox_probe,inbox_fallback,discovery,banner,probe,create,request,backoff,cancel,result,freeze,exec,import,auth,ready,maintenance`，不得自造phase。记录phase/ordinal/start/end/sent/class/backoff，包含最终成功前全部失败以及banner/discovery/connect/privatefallback。secret-free；每logicalID最多128attempt、queue4096、drill持久sink128MiB/lane，overflow显式红、无静默drop；CLI由可配置的独立attempt-receipt FD sink发JSONL（与stdout/PTYbytes隔离，当前CLIexit显式Close/join，FD设CLOEXEC），P18实现sink、P23接入口、P29独立收集并核完整ID。drill必须启该production通道，不能从stderrregex推断CONNECT；必须输出失败最终请求与迟到effects。每逻辑operation从**计划slot**到validatedread/truePIDstarted或finalresult≤5s，accepted不是执行成功；长run只量admission到真实started，持续业务独立量原socket。

drill10/s绝对slot不能等上一请求结束才发下一请求，全部expectedIDs预登记，schedulerdelay算时延，skippedslot/missingattempt/duplicateeffect均红。cut前后每class至少20个读/稳定write/shortexec；每hop freeze/auth/controlwindow每class至少一个**新的logicaloperation真实start位于该window内**，另记已有inflightoperation的区间交集；不能只靠cut前已启动请求的overlap遮盖cut内漏发。基线绝对10/s slots不断，P29收到本地phaseevent后额外发三class targetedfreshprobes、单独登记expectedIDs与schedule/start/finish；monitor与CLI调度延迟也算5s。没有freshstart覆盖的window即INCOMPLETE而非0错误，包括极短窗口；不人为延长产品冻结或删短窗口求绿。继续收lateeffecttail以抓deadline后第二进程，不延长5s成功预算。

## §9 wire、N-1 四象限与机械 gates

### 9.1 additive wire 唯一命名

`BrokerImageStatus`固定publicfields `running_sha256,generation,phase,cut_id,mode`（omitempty additive）；localRuntimeReport与remotehealth都引用同shape，local admin_runtime schema_version不抬；remote ClusterHealthSchemaVersion从当前7（alerts.go:16）记账至8，仅shape记录不作为兼容门。`ClusterUpgradeReq`只加omitempty `holder,scope,cut_id,maintenance`；canonical签名在任何新field非零时增加版本化extensionblock并覆盖全部新字段，零值旧bytes严格相等。oldverb的新无holder请求不能操作newheldlock，noexactproof不能进入newmaintenance，不silentdowngrade。NodeRegisterReq/Resp加`broker_image_maintenance` capability/accepted状态；新增forwardedmaintenance request/ack/proof纯type、newverb明确ACL，不重用ambiguous旧字段。

不提升S5冻结flowwireepoch、九个hotdeferreason或全局protoepoch；复用`proto.CodeBadRequest/adminsock.CodeIsLeader`，为broker channel新增`adminsock.CodeUpgradeDeferred="upgrade_deferred"`（现agent/upgrade.go:178已有同值）和稳定detail，detail闭合表包括 `maintenance_observer_unsupported`, `maintenance_expired`, `store_close_unknown`, `control_result_unknown`；store_close_unknown归adminsock.CodeStoreError，control_result_unknown归proto.CodeAgentNoResponders且CLI视sent后终态；所有namespace registry/exitmap同步，禁止自行改名加第二相近enum。每个返回点被producer/classifier/CLIexitmap覆盖，`phase/mode/ClosePhase/RetryFamily/MaintPhase` exhaustive switchfamily有正负controls。

wireinventory scanner明确加入actual `adminsock.RuntimeReport/BrokerImageStatus` roots，不能只扫proto。更新现NodeRegister与其他受影响golden，旧wire删除/tag/type变更都红；旧N-1bytesnonzero新扩展的签名负控红。imageinventory增加`brokerimage.Bundle/StoreStamp/Seal/tunnel.ServerImage/listenerimage.Descriptor`反射roots，保持旧agentreachable行不删；private库存另§2.2。namingP0freeze在field/type/domain/resource/error/test/drill/matrix每层完全一致。


P00永久拥有proto新puretypes，P25只消费它们并修改现wire承载字段；不得P25另造一套Status/maintenancebody。

| 新type / existing carrier | exact fields（全部new field为omitempty；body required字段无omitempty） |
|---|---|
| `proto.BrokerImageStatus` | `RunningSHA256 string→running_sha256`; `Generation uint64→generation`; `Phase string→phase`; `CutID string→cut_id`; `Mode string→mode`；phase只`serving/preparing/frozen/importing/auth_usable/control_ready/rolling_back/failed`，mode只`single/sole-voter/quorum` |
| `proto.BrokerMaintenanceReq` | `Record brokermaint.Record→record`; `Proof brokermaint.Proof→proof`; `NID string→nid`; `ConnectedBrokerID string→connected_broker_id`; `IngressServerID string→ingress_server_id`; `RouteDigest string→route_digest`; `Sig string→sig`；existingforwardedcommand verb=`broker-maintenance` |
| `proto.BrokerMaintenanceAck` | `CutID,Nonce,NID,ConnectedBrokerID,IngressServerID,InstanceID,RouteDigest,ProofDigest string`分别snake_case；`Accepted bool→accepted`; `RemainingNS int64→remaining_ns`; `Sig string→sig`；ACK签全部fields并绑定currentroute，remaining不是跨机absolute时间 |
| `ClusterUpgradeReq` | `Holder string→holder`; `Scope string→scope`（空=旧defaultwhole-host）；`CutID string→cut_id`; `Maintenance *brokermaint.Record→maintenance`；现oldfields不改 |
| `ClusterUpgradeResp` / adminsock.Response | `BrokerImage *proto.BrokerImageStatus→broker_image`; `NextGeneration uint64→next_generation`；armed后给sealednextgen，completed需真实newStatus |
| `ClusterHealthResp` / adminsock.RuntimeReport | `BrokerImage *proto.BrokerImageStatus→broker_image`；只在当前可测时有值，remoteHealthschema记8，localruntime保持原schema |
| NodeRegisterReq / NodeRegisterResp | `BrokerImageMaintenance string→broker_image_maintenance`；仅`broker-maint-v1`表示支持 / 已接受，missing/unknown都unsupported，不猜bool默认值 |
| `clusterupgrade.Plan` | `Scope PlanScope`; `N1WriteStall bool`；P00新`broker_contracts.go`定义`type PlanScope string`，值`ScopeWholeHost="whole-host"/ScopeBrokerDaemon="broker-daemon"`；P24现plan.go新增`ComputeWithScope(nodes []Node,target string,scope PlanScope) Plan`，旧Compute调用default |

`CanonicalUpgradeReqBytes`：newfields全zero时返回现七段oldbytes逐字相同；否则 `oldbytes + "\nbroker-image-v1\n" + Holder + "\n" + Scope + "\n" + CutID + "\n" + base64.RawStdEncoding(canonicalMaintenanceJSON)`。JSON按P00 Record声明fieldorder固定snake_case且显式零值；nil为`null`；全部identifier拒绝newline/未知scope；Maintenance.CutID必须等外层CutID。newop对oldreader不能签删field后downgrade。maintenanceReq/Ack canonical为同schema固定fieldorder、签名字段排除，序列含RecordDigest/ProofNonce/route/NID/剩余值；P15codec实现，P25golden按字节固定。

proto.ImageDomainsLine仅追加`broker-image-v1`，ImageABIVersion仍1、最终image-abi行不变；每brokerBundle始终required该域，有retainedflow再requiredflow-v1，broker不携SScrypto时不假称需解ss-v1。source/prev都必须已具brokerimage能力，37为capableA→capableB真实不同SHA；真正pre-S6首次迁入S8、39BR独立负控，不能拿不具reader的prev称安全rollback。

### 9.2 N-1 quadrants

| server / client | 正常wire要求 | hot-cut入口与证据 |
|---|---|---|
| new broker / new agent+ctl | 正常注册/ctl/flow，maintenancecapACK；A/A′/B完整契约 | 37三lane、同发行capableA→capableB、S5 flows原socket |
| new broker / old agent+ctl | additivewire、旧exec/read/REGISTER按原能力运行；不读未知字段造成失败 | oldlivelegacyflow或缺maintenanceACK诚实拒绝cut；不能外推fullhot连续 |
| old broker / new agent+ctl | oldbytes签名、explicitlegacyfallback、安全重试边界；真旧artifact正常可用 | 独立39.BR N3旧follower/home触发其已有reload，原Rununwind应红；oldN1leader拒绝不是负控 |
| old broker / old agent+ctl | 原wire/普通业务baseline | 真实artifactbaseline；无S6能力不宣称热交接 |

N-1wiretests在S6落地；S8负责两releasefleet完整升级/legacy第一次迁入、长窗/跨gen/两代上限。S6 `NO_NEW_OPTYPE/MIGRATION` exactdiff gate验证无OpType/commandVersion/migration集合变化，go/toolchain/dependencydirectives不趁机升级。JS独立witness记录objectGetInfo.NUID、chunkbytes、streamconfigdigest与persistedconsumerseq/auditterminalhistory，pre/post相等且currentACK可读；hotreadiness只验证现stream，不调用coldensure改replica/reset/recreate。

### 9.3 所有闸守恒

broker生产files**=70**、Brokermethods≤285、codelines<18000；现packagegolden16000不随便抬到18000。cluster32、cmd57生产filesΔ=0；new逻辑放brokerimage/tunnel/cli/leaf，clusteronlyexistingnode/production/transport/snapshot seams，cmdonlyexistingwiring。cmdmain非CLI<1300（当前量化golden1000，S5 CMD-SLIM后已收紧）、Agentmethods≤125/productionfiles≤19/codelines<8000、execsession.Runtime methods保持39，既有债务caps不抬。每filecode≤700、新增type receiver methods<100（既有debt exceptions仍受专门caps），新增image struct字段亦<100，newpackage直接cap+currentvalue；AllOS AST总并集，不只Linux构建。budget更新只能既有quantum与ceiling内，有receipt不能先raisegolden再实现。

layering：brokerimage不能importbroker/cmd/agent；clusterio只追踪net/io，不import raft或NATS，RaftRPC wrapper只在cluster/transport.go；brokermaint/controlbudget纯leaf；listenerimage不importtunnel；tunnel只leaf/raw/flow/handoff；CLI不能importbrokerimageimpl；test/clusterharness仅testimport。新exporteddataplane函数/方法ctx-free，ctxroot/none精确账本无新增泛例外；newOSsyscall仅叶子`*_linux.go/*_other.go`，其余common不buildtag，只在合法platform边界有notavailable明确行为。Darwin编译扫描与原生回归按testingstandards，未有runner记NOT_RUN，不能以Linuxloopback替代。

新增leaktest必须在真实Test函数内baseline→≥5轮exercise→共享AssertNoGoroutineLeak **和** AssertNoFDLeak；每assertion有exactanchor，actualFDidentitymultiset防close旧/open新计数相等假绿。包含partialTake/partialconstructor/partialClose/latealiasrevoke/execerrorreopen/normalStop/currentrollback。whole-package-race tuple `TestHotUpdateBrokerMatrix` 的 `run=""`, `race=true`, deadline600s；覆盖brokerimage/contractstub、brokermaint/controlbudget/listenerimage/clusterio、storage/cluster/tunnel/adminsock/httplisten/subhttp/brokermetrics/clustermanifest/broker/agent/agentboot/cli/natsinbox/proxydial/execclient/clusterupgrade/cmd及test/clusterharness/test/hotupdate/test/p13/test/cli_e2e whole-package闭包。保现Flow/Exec矩阵，不把新路径靠regex选两test漏掉。testfunctioninventory、matrixunitsgolden与riskpackage/leakanchor双向扫描；负控删除一个package/anchor/testrow必须红。

## §10 drill 37、39.BR、fixture 与 costs

### 10.1 唯一 fixture / collector

P29唯一拥有可调用 `flow_fixture`（兼容75现command约定）和brokercollector：fixture供给、原socket保持、referenceSS、absolute10/smixedctlattemptsink、ACKwriter、FDidentity与JSwitness、rawlogs。P30仅stage/arm/产品命令/manifest/ledger引用，不再实现一个私有oracle。P26是hermeticprocess/storefixture的唯一owner，与P29livefixture分开并明确职责，二者不复制pendingcursor/decoder判据。收日志只用现`lib/logs.sh`一份四流映射并source；新增collector不能内联猜路径。

每lane至少2agent/1ctl，真实install/productinit/grow、systemd、外置NATS/Caddy/持久磁盘/mTLS；candidate/source/previous commit或dirtytreehash、artifactSHA/buildflags、每进程running `/proc/PID/exe` SHA/PID/startticks/ExecStart、NATS/Caddyversion+SHA全部保存。旧current真实退出、newrunningSHA不同；user业务不应用reconnect，只有TLS/yamuxcarrier可替换。Bnativebroker-onlyscope证明agentPID/startticks/exeSHA不变；不以AgentAbsent或sameVersion跳步替代。

原cohort：前台SSH命令持续双向、四条TCP（真实connecting OPEN、idle、active、slow/half-close反向tail）、bob/alice真实SS、长reliableexec原PID/startticks/stdout/stderr/result。双向字节seq/hash/预登记总量/FIN与tailreceipt、clientoriginalsocket/tuple、targetacceptID/原socketcookie、agentloopback/upstream、公网listenerdevino/port、flowopen_seq/ID均独立采集；不只相信productcodec。SQL pre/mid/post home/epoch/tokenhash/pins不变、no unexpectedrehome/rotation/rebalanceevent；密钥只有digest。每hop新listenerconnect无refused且业务结果≤5s，每class覆盖window；没有真实停留在half-close/connecting/active状态算不覆盖。

### 10.2 arms 与 native verdict

37固定五个真实manifestheader（`arms/fixture/grows/worst/forgoes`），六列expected-verdictrow遵现parser；机器arm `A1`，正文A′。manifestchildren全列入，不能声明A但漏joinR。39保现X/Y/P/F及它们原owner/fixture/expected；只追加BR，BR在commonN1setup前dispatch到独立N3供给。

37manifest冻结为 `arms: A A1 B T R`、`fixture: A=install A1=N1 B=N3-live T=N3-live R=N1`、`grows: A=0 A1=0 B=2 T=2 R=0`、`worst: A=420 A1=420 B=720 T=240 R=360`、`forgoes: A=- A1=- B=- T=- R=-`；T的samearm第二singlefixture由P29具名供给，不能借别arm状态。R两substeps不是额外declaredchildren；headerR=N1供给capablesingle亦须验证ACTIVEcurrentstore（Raft形态的currentrollback另P26N1/N3真测试）。39追加`BR=N3-live / grows=2 / worst=300 / forgoes=-`，保X/Y/P/F原tuple逐字不动。simbudget单位ceiling按现`2×worst`与全局cap，不新增parser词表。


| child | native操作 / oracle | 负控与 verdict | normal / worst 秒（估算） |
|---|---|---|---|
| 37.A | 默认single，不clusterinit；A→B同PID，originalcohort/ACK/新ctl/新connect | GREEN；reader丢字段/关keeper/丢ACK mutation本地必红 | 180 / 420 |
| 37.A1（A′） | 产品init真实1-voterleader；N1WriteStall真实；生产timer；最后ACK到firstvalidatedwrite ≤5s | GREEN；错误保旧CodeIsLeader、重置requestbudget都红 | 180 / 420 |
| 37.B | 产品growN3，followerfirst/leaderlast，每stepfreshleadertransfer；auto-rebalance on，normalreaper/rosterwake | GREEN；删maintenanceguard/holder/CAS或错误scope升级agent红 | 360 / 720 |
| 37.T | freshsingle与N3home真tier-B限速transfer，先证bytes>0且<total；同IDfinish，defer时SHA不变；terminal后reload成功 | GREEN；census恒0必须实际cut误入而红，census恒busy负arm进不了cut也红 | 90 / 240 |
| 37.R | capable真实binary fault：R.held与R.active两子阶段，均过purepreflight且进入指定barrier；samePIDprev恢复 | GREEN；R.held验证Take nonnil+err salvage；R.active newACK+flowoffset后currentcut回prev，原库/bytes不回退 | 120 / 360 |
| 39.BR | 真legacyartifact先建立N3follower/home原cohort；调用旧版本已有signedreload触发原Rununwind | PRODUCT-RED=3；独立oracle必须命中已pin旧断流/拒连/lockrelease事实；不以旧1-voterCodeIsLeader作红 | 90 / 300 |

37normal930s（15.5min）、worst2160s（36min）；加入BR为1020/2460s，fixture/build/deploy coldcost另记，计划全组18–24min。stagewall ceiling4320s留收据/失败收敛空间，不改变5s产品窗/8sdetach。T需供给两拓扑的额外成本由首receipt核，超过240s不得吞样本；更新估算与runbudget须同owner落receipt。所有时间都从artifactready的runwall计，build不藏在cutlatency。

R故障不能用不兼容ABI/domain（会被preflight拦下）。以harness生成的临时sourceoverlay制品，保持合法compiledreader/schema/version，指定一次性nthTake或ACTIVE后ACK/offset barrier返回结构错误；记overlay/source/artifactSHA、faultentryreceipt、taken/untaken/writers。生产源码无持久环境变量后门，不新增productionbuildtag；faultcode仅testfixture的artifactoverlay。R.active在candidate产生独立新ACK并真实推进原flowbytes后注错，再走真实FreezeCurrent→prev；不能只测没启动pump的importerror。

BRartifact由供应工具真build/vendoredpreviousrelease，pinpeeledtag/sourceSHA/binarySHA和preflightcapability，不能把新binary标old或故意loadererror。其原生旧断流signature必须由首次独立run建立具体expectation并lead签收；未pin是SETUP-RED，不猜某EOF/RST范围。既有39旧PTY/flownegativecontracts不改expected。`validate-verdicts`父37=GREEN0、39含BR维持nativePRODUCT-RED3；INCOMPLETE4、ASSERT-FAIL1、SETUP-RED2都原样保留；没有翻译“预期红所以返回0”的arm。

manifest/expected、blocked_by、contention、candidate-stamp、legacy-owner row同步更新。旧owner逐字保留`hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration`、`hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration`、`hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration`（Y/P/F；`expected-verdicts.tsv:84,90,98`）；BR新增owner固定 `hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration`。blockedrow只在本候选nativeGREEN收据之后解除，不能以hermetictest解除liveblocked；lint/ledger/selftest的fixturetuple与预算逐arm对账。

## §11 测试库存：每项都有能使它变红的 mutation

以下T编号是本计划receipt索引，Test名冻结。同文件可多个测试；唯一文件owner见§12。所有强制交错用barrier/fakebootclock/realRPC，不靠sleep碰运气。mutation只用于test/harnessoverlay，记录patch+红输出，修复后正控绿；不能把mutant加入生产背门。

| ID / Test | 核心 oracle | 必须杀死的 mutation | owner；tests需先land |
|---|---|---|---|
| T01 `TestBrokerExecStaysInsideRun` | helper真execsamePID，Nodeclose先于Exec，olddefercounter=0 | cancelRun→serveexec | P11；P01/P03/P04/P12/P22 |
| T02 `TestCurrentRuntimeStopAfterExecError` | Exec error→currentopen/newACK→Stop，NC/DB/raw全关，锁可获 | defer捕获oldnc/DB | P28；P11/P22/P26 |
| T03 `TestBrokerBootRoleOnce` | validbrokerwrongargv+missinghint同PIDsalvageprev；坏image清场无coldopen | argvchooser先Abandon / Restore二次 | P22；P02/P25 |
| T04 `TestBrokerPartialTakeSalvage` | 各NthTake返回nonnil+err，全部原FD survives，owner唯一 | err先判丢File / pristineShutdown | P02；P07/P08/P22 |
| T05 `TestBrokerCanonicalStoreLock` | single/cluster/offline同DB同锁；URIalias/hardlink/symlink/错DataDir拒绝 | DataDir独立于DB / 只clean不身份核 | P03；P04 |
| T06 `TestBrokerPreflightNeverOpensStore` | 真Boltsecondopenhelper证hang；preflight禁止所有opener；kill+wait清零 | purepreflight调用New/OpenRO | P26；P02/P03/P04 |
| T07 `TestOpenCurrentExactSchema` | appliedfilename+content集合、unknown/missingearly/null集合拒绝且无写 | MAX/count/user_version整数判断 / migrate | P03；P0 |
| T08 `TestBrokerWriterRegistryExclusive` | 实际driver同inode livewriters≤1，close0/reopen1；lazyRO真实计 | 第二pool不登记 / MaxConns代替registry | P03；P0 |
| T09 `TestBrokerShutdownReceiptOrder` | producers0→raft/transport/Boltclose→cursor→checkpoint→RO/writerclose→seal→Prepare | seal提前 / 忽略Close错误 | P26；P01/P03/P04 |
| T10 `TestBrokerPartialCloseBlocksReopen` | 每objecterror/unknown禁止新open，nonnilpartialconstructorowner闭合 | error当closed / backgroundShutdown丢弃 | P04；P03 |
| T11 `TestBrokerJournalPreserved` | singleDELETE/WAL不改；真实FULL；busycheckpoint拒cut | single自动WAL / busy忽略 / DSN降低FULL | P03；P0 |
| T12 `TestBrokerLockHeldAcrossExec` | realcontender连续+closedstorebarrier不可Acquire；normalStop后可Acquire | oldwrapperLOCK_UN；mutant实际Acquire成功 | P26；P04/P08/P11 |
| T13 `TestBrokerACKBeforeReplay` | currentOpen后首replay前ACKrows/cursor独立读完整 | restoreoldDB/删除WAL，让replay补行 | P26；P03/P04/P05 |
| T14 `TestBrokerAppliedIndexMonotonic` | ACKprefix、ReqID效果恰一次；含config/barrierentries区分SQLcursor | resetappliedindex / 混同raftlast | P26；P05 |
| T15 `TestInstallSnapshotCurrentFloor` | 新ACK后staleinstall error、rows/config/term/index不改；freshcatchup成功 | 初始floor / nil-noop / metadata不绑定 | P05；P04/P03 |
| T16 `TestRaftSnapshotRPCJoinWithinCut` | 真实mTLSstalledpayload被关闭，io.Copy/acceptedhandlers/snapshotreaders均join | 只Close listener / 不关acceptedconn | P26；P04/P05 |
| T17 `TestBrokerRestartSkipsStartupSnapshot` | NoSnapshotRestoreOnStart与durablecursor幂等replay正控 | 设false并用stalesnapshot | P05；P04 |
| T18 `TestListenerKeeperViewPause` | viewClose后originalOFD stillLISTEN，freshbusinessreply≤5s无refused | viewClose实际closekeeper | P08；P0 |
| T19 `TestUnixListenerHotNoUnlink` | 路径inode原样，import不probe/remove，normalstop只删matchinginode | SetUnlinkOnClose默认 / adminStartcold | P10；P08 |
| T20 `TestHTTPFreezeJoinsStoreHandlers` | acceptedkeepaliveDBhandler0后seal，取消joined，keeper未关 | Serve/Shutdown返回就declare0 | P09；P08/P03 |
| T21 `TestEndpointPauseKeepsPendingFIFO` | raw/slots/waiterFIFO/nextSeq在pause/export/import保持 | terminalstopclosepending / deadline循环Accept | P06；P08 |
| T22 `TestEndpointTimerCallbackJoin` | AfterFunc已fire且timer=nil仍join/exportcleanup，回调不selfjoin | Stop()当join / nil忽略 | P06；P0 |
| T23 `TestServerCarrierJoinBeforeRawFreeze` | stalledyamux含partialn>0err，TLSclose、pump+monitorjoin后Freeze，offset正确 | rawfirst / 只streamdeadline / 不joinmonitor | P06；P0 |
| T24 `TestBrokerEndpointFenceRoundTrip` | fivefencemaps+refs并集/Closing/permits/receipts/FIFO完整 | inflightreset0 / 漏closedAllocation | P07；P06 |
| T25 `TestBrokerPreparedListenerRevokeCannotResurrectEndpoint` | Prepare前后latehardrevoke所有dups不可收发，revokedlistener无alias | 只关original / 导出preliminaryfence | P28；P01/P07/P08 |
| T26 `TestBrokerLateCloseDoesNotKillReplacement` | sameportA staleclose不能杀newB全identity | 只port/epoch匹配 | P07；P06/P14 |
| T27 `TestBrokerActualFDWhitelist` | 额外nonCLOEXEC/不同ID同OFD/failflagrestore被拒且无漏 | 只验manifest / CLOEXEC恢复错误忽略 | P02；P08/P22 |
| T28 `TestBrokerIssuerAndBudgetPreserved` | ticket旧flow能resume，boot/instance/key/budgetpointer保存、本域reservedsum合法 | callcoldfactory / 每flow新budget / globalUsed冒域值 | P12；P07/P03 |
| T29 `TestBrokerAuthFirstReadyBarrier` | 真calloutJWT/CONNECT先于业务；PIN走commit；JS/latewire后ready | uncheckedFlush / peerstaticauth当callout / Ready过早 | P27；P12/P26 |
| T30 `TestControlAdmissionAbsoluteBudget` | DNS→proxy→TLS/WSS→inbox→banner→request总≤5s，所有attempt输出 | 每阶段reset5s / natsnet.LookupHost / 固定proxy10s | P27；P18–P23/P26 |
| T31 `TestAdmissionProbeTimeoutDoesNotDowngrade` | 2.1s空窗create1/legacyPUB0/effects1；explicitunsupportedlegacy1 | timeout→ErrUnsupported | P21；P18/P20 |
| T32 `TestRetryStableLogicalIdentity` | retrycanonicalCallID/RequestID/payload不变、真process1 | attempt新CallID / handle重建 | P21；P18 |
| T33 `TestRetryAuthReceiptBoundary` | eligiblehistoricalreceipt空窗≤5s；freshlogin/PIN/ACLterminal无副作用 | 首次auth也retry / stalefingerprint当授权 | P23；P18/P12 |
| T34 `TestBrokerProxyAbsoluteDeadline / TestBrokerInboxAbsoluteDeadline` | proxy/WSS/privatefallback同remaining，连接可用后取消hookjoined不杀longrun | fallback全额 / 留sticky5s杀长任务 | P19/P20各own独立Test（suffixProxy/Inbox）；P18 |
| T35 `TestAttemptObserverNoSilentLoss` | success前failures和sent/timeout/backoff完整；overflow红 | 成功时清失败记录 / 丢slot | P18；P0 |
| T36 `TestControlResultUnknownNoReplay` | unsafe已sent只查/报unknown、不duplicateeffect | sent后genericretry | P23；P18/P21 |
| T37 `TestSoleVoterReloadWithinBudget` | productiontimers、samePID、ACKprefix、firstwrite≤5s | 仍CodeIsLeader / N1tests缩timer | P26；P04/P11/P24 |
| T38 `TestTransferLeaderEveryStep` | plan后leader变化，target必须transfer；NotifyABA/config变化拒cut | 静态TransferTo / 两次IsLeader false | P24；P04/P16 |
| T39 `TestMaintenanceFixedCrossClockExpiry` | nonceproof固定≤8s，RTT消费、wall±30s、新leader/迟到reply不延长 | 外机BootNS直比 / now+TTL / 120sTTL | P15；P0 |
| T40 `TestUpgradeHolderCAS` | Alease失去→B获得→A迟到renew/release不得动B | stamp当holder / unconditionalclear | P16；P15/P25 |
| T41 `TestMaintenanceEveryHomeMutator` | from/to/healthy/unready/rotation/manual/auto均受guard，otherhome照常 | 漏manual/to/rotation一个guard | P13；P14/P15 |
| T42 `TestRehomeFullAllocationCAS` | applyMu内维护与fullidentity，0rows不started，staleA不能改B | epoch-only / 先publishstarted | P14；P15 |
| T43 `TestMaintenancePreservesDwellAndReturn` | historicaldwell不清，plannedreturn不rotate；expirycrash继续真实恢复 | 清dwell / 清globalpending / expiry无限保护 | P13；P15 |
| T44 `TestAgentSilenceTwoWakeMaintenance` | silent2+即时topologywake不escape，fixedexpiry后正常escape；revoke优先 | 只信20sbackoff / 未installhold即ACK | P17；P15/P25 |
| T45 `TestBrokerScopeAndTransfersDeferred` | BagentPID/SHA不变；transferbusydefer/terminal成功/读errorfail | AgentAbsent伪造scope / 忽略censuserr / census恒0 | P24；P11/P25 |
| T46 `TestBrokerWireNMinusOneQuadrants` | 四象限真实oldbytes、newadditive、localadminroots、canonical签名 | 删除wirefield / extension未签 / 无holder操作newlock | P25；P0 |
| T47 `TestBrokerImageInventoryControls` | public/private/type/decoder闭合库存appendonly | 删field、改type/tag、漏privateexportkind | P32；P02/P07/P25 |
| T48 `TestBrokerExecOriginalFlowCohort` | 原socket/targetaccept、SSH/TCP/SS、halfclose/tail、newconnect≤5s | appredial / 重新AEADheader / listenerclose | P28；P11/P12/P22/P26 |
| T49 `TestBrokerActiveRollbackKeepsNewACK` | ACTIVE newACK+newbyteoffset→currentgeneration+1→prev，newstate可读 | pristine初cut / oldDBsnapshot | P28；P01/P02/P11/P26 |
| T50 `TestBrokerControlBlackoutMixed` | absolute10/s、truePIDstarted、效果1、全部attempt≤5s、非空windows | serializedsampler / accepted当started / 丢失败样本 | P27；P18–P23/P29 |
| T51 `TestBrokerJSCurrentWitness` | NUID/chunks/config/consumerseq/auditterminal原对象不变 | recreate同名object / resetJS / 临时第二NATS | P26；P12 |
| T52 `TestBrokerCurrentRollbackAndNormalStop` | rawlocklistener当前owner闭合、FDmultiset、probe可获、Unixpath删 | oldcaptureddefer / leakedTakenstate | P28；P11/P22/P26 |
| T53 `TestBrokerStructuralAndLayeringControls` | 70/285/<18000、57/32/125、AllOS/ctx/buildtag/layering | 新brokerfile / aliasctx / syscallcommon / ceilingraise | P31；P0及全部impl |
| T54 `TestBrokerLeakGateAndRaceCoverageControls` | riskpackages/wholetuples/anchors/testinventory双向 | omitcmd/proxydial/clusterupgrade / deadhelper断言 | P33；全部unit与P26–P28 |
| T55 `TestBrokerNodeLeakCycles / TestBrokerStoreFixtureLeakCycles` | ≥5partialclose/constructor/execerror真实cycles，共享goroutine+FD断言 | 未joinRPC / 第二ROreader漏close | P04及P26各suffix独立Test；P03/P05 |
| T56 `TestBrokerServerImageLeakCycles / TestBrokerLifecycleSeamLeakCycles` | ≥5take/timer/latealias/currentcutcycle且FDmultiset稳定 | privateFD不close / monitor未join | P07及P28各suffix独立Test；P06/P08 |
| T57 `TestBrokerCLIRetryLeakCycles / TestBrokerProxyLeakCycles / TestBrokerInboxLeakCycles / TestBrokerExecClientLeakCycles / TestBrokerBootLeakCycles / TestBrokerControlIntegrationLeakCycles` | ≥5DNS/proxy/inboxcancel/auth/normalstop cycles | 遗弃dialgoroutine / 旧cancelhook | P18/P19/P20/P21/P22/P27各suffix独立Test；相应实现 |
| T58 `TestBrokerDrillNativeVerdictControls` | 37allchildrenGREEN、39BR真红、INCOMPLETE4、missingarm/slot/fixture/selftest红 | expectedredtranslate0 / wrongoldN1 / R被preflight拦 | P30；P29/P33 |

P08/P09/P10/P11/P12/P15/P16/P17/P23/P24有资源或riskimport的包再在它们既有unit文件内增加suffix唯一的`TestBroker*LeakCycles`，每条≥5真实exercise与两个直接共享断言；各owner给P33精确站点和exercise名，P33不代写死helper。测试矩阵必须执行所有这些后补Test；P0 receipt列出最后exactnames，testinventory拒未登记Test。测试文件不以“integration owner以后补”代替上述明确producer/consumer seam。

## §12 严格 disjoint 的并行工作包

下表是**唯一文件归属清单**，每个物理文件只列一次；`files count` 含生产/测试/golden/docs，生产新增量另由P31检查。不要因某文件里有另一包需要的调用就去改它：用P0合同/compile-adapter，integration将缺陷发回唯一owner。未列文件默认不改；确需触及须lead先更新这张清单并做重复路径检查。P00先land，其后可按stub合同并行；`depends-on`是合同/实现land的DAG；每包Tests依赖另外明确列出，早期可用P0stub编译，未满足真实测试依赖不能签PASS。只有P01/P06标HARD。

| 包 | 名称 | 难度 | files count | depends-on |
|---|---|---|---:|---|
| P00 | contracts / compile-stubs | NORMAL | 18 | — |
| P01 | cut / current runtime | HARD | 5 | P00 |
| P02 | boot import / pure preflight / codec | NORMAL | 9 | P00 |
| P03 | storage current-open / identity / driver registry | NORMAL | 9 | P00 |
| P04 | Node shutdown / transport cancellation seam | NORMAL | 10 | P00,P03 |
| P05 | FSM / runtime snapshot safety | NORMAL | 3 | P00,P03,P04 |
| P06 | tunnel pause / worker joins | HARD | 6 | P00,P08 |
| P07 | tunnel Server image / private descriptor | NORMAL | 4 | P00,P06,P08 |
| P08 | listener keeper / Accept view leaf | NORMAL | 4 | P00 |
| P09 | HTTP accepted-handler seam | NORMAL | 9 | P00,P08 |
| P10 | admin inherited Unix seam | NORMAL | 3 | P00,P08 |
| P11 | Broker Run / local reload wiring | NORMAL | 11 | P01–P04,P07–P10,P12,P25 |
| P12 | authority / auth first / current factories | NORMAL | 5 | P00 |
| P13 | all home mutators / dwell / planned return | NORMAL | 7 | P11,P14,P15,P16 |
| P14 | port full-identity / maintenance CAS planner | NORMAL | 3 | P00,P15 |
| P15 | replicated maintenance / clock proof leaf | NORMAL | 5 | P00 |
| P16 | rolling holder / maintenance command / cluster meta seam | NORMAL | 7 | P00,P15,P25 |
| P17 | agent maintenance capability / route hold | NORMAL | 3 | P00,P15,P25 |
| P18 | CLI shared retry / absolute budget / observer | NORMAL | 8 | P00,P25 |
| P19 | proxy / direct DNS / TLS-WSS budget seam | NORMAL | 5 | P00,P18 |
| P20 | private inbox budget seam | NORMAL | 3 | P00,P18,P19 |
| P21 | reliable probe / stable create seam | NORMAL | 3 | P00,P18,P20 |
| P22 | sole bootstrap / serve current store wiring | NORMAL | 5 | P02,P11,P24,P25 |
| P23 | ctl RunE call sites / surface golden | NORMAL | 13 | P18–P21,P24,P25 |
| P24 | scope / rolling driver / local broker command | NORMAL | 7 | P04,P11,P15,P16,P18,P25 |
| P25 | wire / signature / public runtime schema | NORMAL | 15 | P00 |
| P26 | store / Raft / process integration fixture | NORMAL | 4 | P01–P12,P22,P25 |
| P27 | auth / CLI / maintenance integration | NORMAL | 3 | P12,P15–P23,P25,P26,P29 |
| P28 | flow / current rollback / normal-stop integration | NORMAL | 3 | P01–P12,P14,P22,P26 |
| P29 | ONE live flow_fixture / broker collector | NORMAL | 6 | P00 |
| P30 | 37 activation / 39.BR / 75 fixture wiring | NORMAL | 5 | P11–P29 |
| P31 | contracts / structural / layering / ctx / tags gates | NORMAL | 8 | P00 |
| P32 | public + private image inventories gates | NORMAL | 4 | P00 |
| P33 | leak / race / enum / test / sim ledgers gates | NORMAL | 19 | P00 |
| P34 | docs / tasklist receipt-only tick | NORMAL | 11 | P00（最终tick需P35） |
| P35 | final integration / receipts / marginal costs | NORMAL | 1 | P01–P33 |

### 12.1 每包交付与唯一 ownership

每包收据格式固定：`source_sha, artifact_sha, owned_files, tests, mutation_patch_digest, mutant_native_rc, positive_native_rc, inventory_rows, leak_anchor_rows, observation_rows, test_dependencies, marginal_seconds`。P31/P32/P33是账本文件唯一写者；各生产owner交精确row manifest，不直接修改别人golden。下列测试编号引用§11，必要测试依赖是每行最后的“须”；缺依赖只能`NOT_RUN`，不能以fake签真实出口。

#### P00 · contracts / compile-stubs（NORMAL）

目标：精确类型/参数/tag/enum/domain/error/fixture合同与compile-stubs；所有消费者先编译；生产不可达stub。

Files（18）：`internal/brokerimage/contracts.go`; `internal/brokerimage/private_contracts.go`; `internal/brokerimage/contractstub/stub.go`; `internal/brokerimage/contractstub/stub_test.go`; `internal/tunnel/server_image_contracts.go`; `internal/listenerimage/contracts.go`; `internal/storage/image_contracts.go`; `internal/brokermaint/contracts.go`; `internal/controlbudget/contracts.go`; `internal/cli/retry_contracts.go`; `internal/port/image_contracts.go`; `internal/clusterio/contracts.go`; `internal/execclient/admission_contracts.go`; `test/clusterharness/broker_image_contracts.go`; `internal/proto/broker_image.go`; `internal/clusterupgrade/broker_contracts.go`; `internal/httplisten/image_contracts.go`; `internal/adminsock/image_contracts.go`。

Tests / mutations：Compile probes；mutation为改签名/fieldtype/enum、productionimportstub。

Inventory / leak / observation / test dependencies：类型根、closedfamily及新包caps申报给P31/P32/P33；stub无资源，无leak例外。

#### P01 · cut / current runtime（HARD）

目标：统一resourcegraph/freeze/finalseal/currentowner/Exec返回Thaw；所有alias/FD有唯一owner。

Files（5）：`internal/brokerimage/cut.go`; `internal/brokerimage/runtime.go`; `internal/brokerimage/resources.go`; `internal/brokerimage/cut_test.go`; `internal/brokerimage/runtime_test.go`。

Tests / mutations：T02/T09/T25/T49模拟合同；真实测试P28拥有；mutation提前seal/oldslot/alias漏。

Inventory / leak / observation / test dependencies：broker-runtime/resources字段与phase observations；TestBrokerCutLeakCycles→exerciseCutFailure（≥5）；实际store/flow后须P03/P04/P06/P07/P08。

#### P02 · boot import / pure preflight / codec（NORMAL）

目标：纯reader/schema gate、held/taken/salvage、pristineRepack、actualFDwhitelist；修Prepared flagrestore错误路径。

Files（9）：`internal/brokerimage/boot.go`; `internal/brokerimage/import.go`; `internal/brokerimage/preflight.go`; `internal/brokerimage/codec.go`; `internal/brokerimage/boot_test.go`; `internal/brokerimage/import_test.go`; `internal/brokerimage/codec_test.go`; `internal/handoff/image_linux.go`; `internal/handoff/image_test.go`。

Tests / mutations：T03/T04/T06/T27；FuzzBrokerPrivateState seed+malformedmutant；Take非nilerr/alias/unknownFD反例。

Inventory / leak / observation / test dependencies：broker-state-v1 descriptor producer；TestBrokerImportLeakCycles→exerciseBrokerPartialTake；stub tests可先跑，真实须P01/P07/P08/P22。

#### P03 · storage current-open / identity / driver registry（NORMAL）

目标：canonicalbinding/exactschema/journal/drivercounts、OpenCurrent无migration；clusteroffline四个现文件全部六处Acquire先核DB-lockbinding；single_store adapter由P11集成owner接P03/P04，避免storage↔newclusterlock编译依赖环。

Files（9）：`internal/storage/storage.go`; `internal/storage/current.go`; `internal/storage/connection_registry.go`; `internal/storage/image_test.go`; `internal/clusteroffline/init.go`; `internal/clusteroffline/offline.go`; `internal/clusteroffline/restore.go`; `internal/clusteroffline/backup.go`; `internal/clusteroffline/image_identity_test.go`。

Tests / mutations：T05/T07/T08/T11；缺earlymigration/同集合不同内容/账本不变但删table/第二pool/busycheckpoint。

Inventory / leak / observation / test dependencies：StoreStamp/Seal/SchemaStamp全部inventory；TestBrokerStorageLeakCycles→exerciseStorageCloseFailure；与cluster/lock整合测试需P04。

#### P04 · Node shutdown / transport cancellation seam（NORMAL）

目标：现clusterfiles内NewForImage/phase/NotifyCh/Shutdown；cluster_store adapter唯一持Node/lock；clusterio只net/io追踪真正acceptedRPC、snapshotpayload取消join；lockAdopt不reflock。

Files（10）：`internal/cluster/node.go`; `internal/cluster/production.go`; `internal/cluster/transport.go`; `internal/cluster/datadirlock.go`; `internal/cluster/testhooks.go`; `internal/cluster/broker_image_test.go`; `internal/clusterio/transport.go`; `internal/clusterio/quiesce.go`; `internal/clusterio/transport_test.go`; `internal/brokerimage/cluster_store.go`。

Tests / mutations：T10/T12/T16/T55Node；mutation丢RPC/unknown当closed/ABA/LOCK_UN。

Inventory / leak / observation / test dependencies：roleedge/RPC/closephase observation；TestBrokerNodeLeakCycles→exerciseNodePartialClose，TestBrokerTransportLeakCycles→exerciseStalledRPC；runtimefloor seam须P05，prodcut须P11。

#### P05 · FSM / runtime snapshot safety（NORMAL）

目标：动态commandfloor+RPCmetadata绑定、boundedstage、restore拒绝语义；保startupskip与幂等replay。

Files（3）：`internal/cluster/fsm.go`; `internal/cluster/snapshot.go`; `internal/cluster/snapshot_image_test.go`。

Tests / mutations：T15/T17；staleafternewACK/nilnoop/config污染/validhigher正控；FuzzBrokerSnapshotStamp。

Inventory / leak / observation / test dependencies：snapshotstaging/restorelinearization/backupreader rows；TestBrokerSnapshotLeakCycles→exerciseSnapshotCancel；真实网络ACK须P26。

#### P06 · tunnel pause / worker joins（HARD）

目标：Endpointpause与terminalstop分开、FIFO/pending、timer/monitor/cleanup/REGISTER tracking与carrier-firstfreeze。

Files（6）：`internal/tunnel/tunnel.go`; `internal/tunnel/flow_endpoint.go`; `internal/tunnel/flow_fence.go`; `internal/tunnel/server_lifecycle.go`; `internal/tunnel/server_pause.go`; `internal/tunnel/server_pause_test.go`。

Tests / mutations：T21/T22/T23；terminalstop/dropnpositive/未joinmonitor/AfterFunc并发。

Inventory / leak / observation / test dependencies：endpoint/timer/pending/callback真实runtime映射；TestBrokerServerPauseLeakCycles→exerciseServerPauseFailure；keeper测试须P08。

#### P07 · tunnel Server image / private descriptor（NORMAL）

目标：独立ServerImage codec/import/flowreuse/fivefences/refunion/salvage；不重造issuerbudget。

Files（4）：`internal/tunnel/server_image.go`; `internal/tunnel/server_image_codec.go`; `internal/tunnel/server_import.go`; `internal/tunnel/server_image_test.go`。

Tests / mutations：T24/T26/T56Tunnel；FuzzBrokerTunnelPrivateState；漏fenceref/dropprivatefield/portonlyclose。

Inventory / leak / observation / test dependencies：broker-tunnel-state-v1的唯一descriptor；TestBrokerServerImageLeakCycles→exerciseServerPartialImport；方法体先基于P00编译，实际测试须P06/P08。

#### P08 · listener keeper / Accept view leaf（NORMAL）

目标：TCP/Unixkeeper/view/pause/adopt/provenance/validator与normalunlink。

Files（4）：`internal/listenerimage/keeper.go`; `internal/listenerimage/listener_linux.go`; `internal/listenerimage/listener_other.go`; `internal/listenerimage/keeper_test.go`。

Tests / mutations：T18；wrongSO_ACCEPTCONN/family/flags/alias/viewClosekeeper。

Inventory / leak / observation / test dependencies：Descriptor字段/FDpurpose rows；TestBrokerListenerLeakCycles→exerciseKeeperPauseAdopt；独立测试无需broker。

#### P09 · HTTP accepted-handler seam（NORMAL）

目标：HTTP ServerOwner与accepted/keepalivehandler admission/join；subscriptionHTTP/metrics/manifest三ServeListener薄adapter都交当前可跟踪Owner，不能只joinRun里Serve返回。

Files（9）：`internal/httplisten/httplisten.go`; `internal/httplisten/image.go`; `internal/httplisten/image_test.go`; `internal/subhttp/subhttp.go`; `internal/subhttp/image_test.go`; `internal/brokermetrics/metrics.go`; `internal/brokermetrics/image_test.go`; `internal/clustermanifest/manifest.go`; `internal/clustermanifest/image_test.go`。

Tests / mutations：T20；Serve返回假drain/DBclosurecapturedold；subhttp真实请求。

Inventory / leak / observation / test dependencies：http-handler/metrics/manifest/sub-http listener rows；TestBrokerHTTPLeakCycles→exerciseHTTPKeepaliveFreeze；TestBrokerMetricsLeakCycles→exerciseMetricsInheritedHTTP、TestBrokerManifestLeakCycles→exerciseManifestInheritedHTTP、TestBrokerSubHTTPLeakCycles→exerciseSubHTTPInheritedHTTP，均≥5直接双assert；SQLcancel测试须P03。

#### P10 · admin inherited Unix seam（NORMAL）

目标：StartInherited无staleunlink、acceptedSQLhandlersjoin、DB-freearmedreply；normalcurrentownerclose。

Files（3）：`internal/adminsock/server.go`; `internal/adminsock/image.go`; `internal/adminsock/image_test.go`。

Tests / mutations：T19；Unixunlink默认/coldStart/老handler未join。

Inventory / leak / observation / test dependencies：admin-listener/admin-handler rows；TestBrokerAdminLeakCycles→exerciseAdminInheritedClose；reply协议测试须P25。

#### P11 · Broker Run / local reload wiring（NORMAL）

目标：只在现files接currentRuntimeSlot、Run显式NodeShutdown/Exec、single_store adapter先持P04原lock再用P03coldOpen/CloseForImage；singlelocaladmintrigger、SHA/transfercreationgate+census/phase/remoteClusterHealthStatus；loopSet真实join。

Files（11）：`internal/broker/broker.go`; `internal/broker/reexec.go`; `internal/broker/runtime_introspect.go`; `internal/broker/loopset.go`; `internal/broker/reexec_test.go`; `internal/broker/broker_image_run_test.go`; `internal/broker/cluster_health.go`; `internal/broker/transfer.go`; `internal/broker/xfer_inflight.go`; `internal/broker/xfer_provision.go`; `internal/brokerimage/single_store.go`。

Tests / mutations：T01；保TestReloadRefusesOnLeader改voters>1，新增N1正控；colddefer/censuserr/samereleaseSHA。

Inventory / leak / observation / test dependencies：Runphase/runtimeStatus/loopjoins rows；TestBrokerRunLeakCycles→exerciseBrokerExecReturned；编译P00可先做，最终tests须P01–P04/P07–P10/P12/P22/P25。

#### P12 · authority / auth first / current factories（NORMAL）

目标：hot保issuerBoot/instance/ticket/sharedbudget，currentSQLauthority与auth/PIN依赖、freshclusterforwarder/NC/responder重绑定，skipcoldreconcile，JS仅witness接口。

Files（5）：`internal/broker/expose.go`; `internal/broker/authcallout.go`; `internal/broker/tunnel_reconcile.go`; `internal/broker/broker_image_authority_test.go`; `internal/broker/cluster_forward.go`。

Tests / mutations：T28/T29unit；coldfactory/keyreset/budgetvaluecopy/uncheckedFlush/把daemonGen写入agentEndpointowner。

Inventory / leak / observation / test dependencies：issuer/authusable/currentauthority/factory rows；TestBrokerAuthorityLeakCycles→exerciseAuthorityRebind；真测试须P03/P07/P11/P25。

#### P13 · all home mutators / dwell / planned return（NORMAL）

目标：所有from/to/manual/auto/rotation/allocationwriter决策、commit后events、历史dwell与plannedreturn；不新增Broker方法。

Files（7）：`internal/broker/proxy_reconcile.go`; `internal/broker/proxy_rebalance.go`; `internal/broker/proxy_auto_rebalance.go`; `internal/broker/observability.go`; `internal/broker/clusterwrite.go`; `internal/broker/home.go`; `internal/broker/broker_image_maintenance_test.go`。

Tests / mutations：T41/T43；漏manual/to/unreadyguard、clearhistory/globalpending、started早发。

Inventory / leak / observation / test dependencies：maintbaseline/dwell/event rows；TestBrokerHomeLeakCycles→exerciseMaintainedHomeTick；测试须P14/P15/P16。

#### P14 · port full-identity / maintenance CAS planner（NORMAL）

目标：在writerplanning内current维护/fullallocation条件，fullidentityCAS与0row结果；无schema。

Files（3）：`internal/port/plan.go`; `internal/port/image_guard.go`; `internal/port/image_guard_test.go`。

Tests / mutations：T26/T42；epochonly/PlanoutsideapplyMu/freeinsert绕guard。

Inventory / leak / observation / test dependencies：AllocationGuard字段与SQLguard inventory；无goroutine/FD，不造leak例外；tests须P15。

#### P15 · replicated maintenance / clock proof leaf（NORMAL）

目标：prepared/active/ended权威、固定8sproof/cache/baseline与全observerACK聚合；cluster_meta existingliteral。

Files（5）：`internal/brokerimage/maintenance.go`; `internal/brokerimage/maintenance_test.go`; `internal/brokermaint/codec.go`; `internal/brokermaint/proof.go`; `internal/brokermaint/proof_test.go`。

Tests / mutations：T39；外机BootNS直比/迟到proof延长/leaderfreshTTL/unknownboot。

Inventory / leak / observation / test dependencies：Record/Proof/LocalLease与baseline descriptor rows；TestBrokerMaintenanceLeakCycles→exerciseMaintenanceExpiry；integratedapply/holder须P16/P17/P26。

#### P16 · rolling holder / maintenance command / cluster meta seam（NORMAL）

目标：全lockverbholderCAS、maintenance原command+keyprefix验证、apply/readback/ACL/observerchallenge交付；不加OpType。

Files（7）：`internal/broker/cluster_upgrade_trigger.go`; `internal/broker/reconcile_upgrade_lock.go`; `internal/cluster/lock_lease.go`; `internal/cluster/membership_ops.go`; `internal/cluster/clustermeta.go`; `internal/broker/upgrade_holder_test.go`; `internal/cluster/upgrade_holder_test.go`。

Tests / mutations：T40；A迟到release伤B/unsignedextension/旧无holder抢锁。

Inventory / leak / observation / test dependencies：holder/lease/appliedrecord/ACKrows；TestBrokerHolderLeakCycles→exerciseHolderLostLease；tests须P15/P25/P17。

#### P17 · agent maintenance capability / route hold（NORMAL）

目标：明确S6capability/ACK、当前routeproofhold、silent2拓扑wake、revokeprecedence；prodfile19不增。

Files（3）：`internal/agent/agent.go`; `internal/agent/roster.go`; `internal/agent/broker_maintenance_test.go`。

Tests / mutations：T44；ACK未安装/20sbackoff假证明/hold重置expiry。

Inventory / leak / observation / test dependencies：routehold/capability/ACK rows；TestBrokerAgentHoldLeakCycles→exerciseAgentMaintenanceHold；tests须P15/P25。

#### P18 · CLI shared retry / absolute budget / observer（NORMAL）

目标：RunEbudget/closedpolicy/secretfreeboundedAttempt与health/bannerhelpers；clock纯leaf，无NATS进neutralcontracts。

Files（8）：`internal/cli/retry.go`; `internal/cli/control_health.go`; `internal/cli/retry_test.go`; `internal/controlbudget/clock.go`; `internal/controlbudget/clock_linux.go`; `internal/controlbudget/clock_other.go`; `internal/controlbudget/observer_test.go`; `internal/cli/attempt_sink.go`。

Tests / mutations：T35/T57CLI；resetdeadline/成功抹失败/overflowdrop；context取消真实join。

Inventory / leak / observation / test dependencies：RetryFamily/Attemptphase/clock/observerrows；TestBrokerCLIRetryLeakCycles→exerciseCLIRetryCancel；deepconnecttests须P19/P20。

#### P19 · proxy / direct DNS / TLS-WSS budget seam（NORMAL）

目标：OptionsWithin alwaysboundedresolver/dialer、proxyremoteDNS/NoProxy、多URLremaining、stickydeadline与成功detach。

Files（5）：`internal/proxydial/proxydial.go`; `internal/proxydial/httpconnect.go`; `internal/proxydial/socks5.go`; `internal/proxydial/budget.go`; `internal/proxydial/budget_test.go`。

Tests / mutations：T34Proxy/T57Proxy；fixed10s/无ctxlookup/遗弃handshakegoroutine。

Inventory / leak / observation / test dependencies：deepdial/handshake/cancelhook rows；TestBrokerProxyLeakCycles→exerciseProxyDeadlineCancel；controlhelpertests须P18。

#### P20 · private inbox budget seam（NORMAL）

目标：ConnectWithin真实privateprefix/marker不变，refusal才fallback，两个dial共享剩余与observer。

Files（3）：`internal/natsinbox/natsinbox.go`; `internal/natsinbox/budget.go`; `internal/natsinbox/budget_test.go`。

Tests / mutations：T34Inbox/T57Inbox；第二Connect重置budget/flusherr当refusal。

Inventory / leak / observation / test dependencies：inboxattempt/fallback rows；TestBrokerInboxLeakCycles→exerciseInboxFallbackCancel；tests须P18/P19。

#### P21 · reliable probe / stable create seam（NORMAL）

目标：RunWithin/ProbeWithin/CreateWithin贯穿同budget、probe不timeout降legacy、stablelogicalID、无sent后fallback。

Files（3）：`internal/execclient/client.go`; `internal/execclient/admission.go`; `internal/execclient/admission_test.go`。

Tests / mutations：T31/T32/T57Exec；probe2stimeoutunsupported/newCallID/重建handle。

Inventory / leak / observation / test dependencies：execattempt/probe/create/sent rows；TestBrokerExecClientLeakCycles→exerciseAdmissionBlackout；tests须P18/P20。

#### P22 · sole bootstrap / serve current store wiring（NORMAL）

目标：Begin单claim/TakeRole、serve纯wiring、single不capturedDBdefer、imageerror无coldopen；保S3W1。

Files（5）：`cmd/tether/main.go`; `cmd/tether/serve.go`; `cmd/tether/serve_test.go`; `internal/agentboot/image.go`; `internal/agentboot/broker_role_test.go`。

Tests / mutations：T03/T57Boot；wrongargv+missinghint/双Restore/earlyAbandon。

Inventory / leak / observation / test dependencies：BootRole/heldsalvage/servephase rows；TestBrokerBootLeakCycles→exerciseBrokerRoleRedispatch；tests须P02/P11/P25。

#### P23 · ctl RunE call sites / surface golden（NORMAL）

目标：cli/natsconn.ConnectWithin消费P19/P20真正deepdeadline；入口budget/receiptguard/exec-runadmission/psnodehelper、移alertlogic到cli、不resettimeout提示；唯一commandgolden写者。

Files（13）：`cmd/tether/ctl_connect.go`; `cmd/tether/exec.go`; `cmd/tether/run.go`; `cmd/tether/node.go`; `cmd/tether/node_versions.go`; `cmd/tether/ps.go`; `cmd/tether/alert_gate.go`; `cmd/tether/error_hints.go`; `cmd/tether/ctl_budget_test.go`; `cmd/tether/command_tree_inventory_test.go`; `cmd/tether/testdata/command_tree_golden.txt`; `cmd/tether/testdata/command_tree_golden_runtime.txt`; `internal/cli/natsconn.go`。

Tests / mutations：T33/T36/T57Cmd；wholeRunretry/首次authretry/读超5s仍hint加timeout。

Inventory / leak / observation / test dependencies：CLIerror/exitmap/commandtree rows；TestBrokerCommandLeakCycles→exerciseCtlAdmissionCancel；tests须P18–P21/P24/P25。

#### P24 · scope / rolling driver / local broker command（NORMAL）

目标：独立PlanScope、N1flag、fresh每steptransfer、brokerupgrade本地入口、remoteSHAphasequery；clusterdrivehelpers迁cli。

Files（7）：`internal/clusterupgrade/plan.go`; `internal/clusterupgrade/broker_scope_test.go`; `cmd/tether/cluster_upgrade.go`; `cmd/tether/cluster_upgrade_drive.go`; `cmd/tether/cluster_upgrade_scope_test.go`; `internal/cli/cluster_roll.go`; `internal/cli/cluster_roll_test.go`。

Tests / mutations：T38/T45；scopeAgentAbsent/静态leader/sameversion假完成/顺带升级agent。

Inventory / leak / observation / test dependencies：PlanScope/N1WriteStall/statusquery/driverattempt rows；TestBrokerRollLeakCycles→exerciseRollLeaderChanged；tests须P04/P11/P15/P16/P25。

#### P25 · wire / signature / public runtime schema（NORMAL）

目标：消费P00的proto新puretypes，additiveHolder/Scope/Cut/Maint/cap/status、canonicalextension、ImageDomains追加broker-image-v1且ABI仍1、Healthschemaledger7→8、adminactualroots扫描；不增epoch。

Files（15）：`internal/proto/cluster_upgrade.go`; `internal/proto/alerts.go`; `internal/proto/messages.go`; `internal/proto/broker_image_test.go`; `internal/proto/wire_inventory_test.go`; `internal/proto/testdata/wire_field_inventory.txt`; `internal/proto/testdata/golden/node_register_req.json`; `internal/proto/testdata/golden/node_register_resp.json`; `internal/proto/testdata/golden/node_register_resp_empty_slices.json`; `internal/proto/testdata/golden/node_register_resp_proto_mismatch.json`; `internal/adminsock/protocol.go`; `internal/adminsock/broker_image_protocol_test.go`; `internal/proto/version.go`; `internal/proto/codes.go`; `internal/proto/codes_registry_test.go`。

Tests / mutations：T46；oldzeroexactbytes/tagtype删除/newextensionunsigned；closederror namespaces覆盖。

Inventory / leak / observation / test dependencies：wiregolden唯一writer；closedfamily/code/detailmanifest交P33，publicimageroot交P32；无新resourceowner。

#### P26 · store / Raft / process integration fixture（NORMAL）

目标：唯一hermeticrealprocess/RaftSQLite/ACK/lock/barrier/faultartifactfixture；single/N1/N3、pre-replaystore、JSwitness。

Files（4）：`test/clusterharness/broker_image.go`; `test/clusterharness/broker_image_test.go`; `test/hotupdate/broker_store_exec_test.go`; `test/hotupdate/broker_store_helpers_test.go`。

Tests / mutations：T06/T09/T12–T14/T16/T37/T51/T55Fixture；双开hang/watchdogWait/旧库恢复/transport不join。

Inventory / leak / observation / test dependencies：storephase/barrier/preReplayACK/JSwitness receipt；TestBrokerStoreFixtureLeakCycles→exerciseStoreExecRollback；tests须P01–P05/P08–P12/P22/P25。

#### P27 · auth / CLI / maintenance integration（NORMAL）

目标：真实NATSauthblackout/CONNECT+RunEdeepbudget/attempt/trueprocess与维护deliveryseams，不实现第二storefixture。

Files（3）：`test/hotupdate/broker_auth_test.go`; `test/cli_e2e/broker_control_test.go`; `test/cli_e2e/broker_control_helpers_test.go`。

Tests / mutations：T29/T30/T50/T57Integration；READY早发/丢CONNECT样本/receipt补偿。

Inventory / leak / observation / test dependencies：authblackout/slot/sideeffect/attempt rows；TestBrokerControlIntegrationLeakCycles→exerciseRealCalloutCut；tests须P11–P12/P15–P23/P25/P26/P29。

#### P28 · flow / current rollback / normal-stop integration（NORMAL）

目标：真exec原socket/SSH/TCP/SS/Endpoint/latealiasrevocation、ACTIVE currentcut/newACK→prev→normalstop。

Files（3）：`test/p13/broker_exec_test.go`; `test/hotupdate/broker_lifecycle_seam_test.go`; `test/hotupdate/broker_rollback_test.go`。

Tests / mutations：T02/T25/T48/T49/T52/T56Integration；初cutrollback/alias未shutdown/olddefer。

Inventory / leak / observation / test dependencies：原socket/FDmultiset/rollbackboundary/currentACK rows；TestBrokerLifecycleSeamLeakCycles→exerciseActiveRollbackStop；tests须P01–P12/P14/P22/P26。

#### P29 · ONE live flow_fixture / broker collector（NORMAL）

目标：兼容75flow_fixture可调用协议、原socketSS/半关闭/OPEN、10/sslotattempt/ACK/JSwitnesscollector、raw四流唯一映射。

Files（6）：`test/simcluster/drills/lib/flow_fixture`; `test/simcluster/drills/lib/hot_update_flow_fixture.py`; `test/simcluster/drills/lib/hot_update_broker_probe.py`; `test/simcluster/drills/lib/hot_update_mixed_probe.py`; `test/simcluster/tests/hot-update-broker-probe-test.py`; `test/simcluster/tests/hot-update-broker-probe-test.sh`。

Tests / mutations：collectorhermeticselftests；missingfixture/slot/attempt/targetaccept/lateeffects/bytes截断。

Inventory / leak / observation / test dependencies：fixturecustody/schema/oracle/phasecoverage rows；collectorchildsocket/childprocess资源闭合selftest；Go接纳格式tests需P18/P21/P25。

#### P30 · 37 activation / 39.BR / 75 fixture wiring（NORMAL）

目标：37五armsnativeGREEN；BRcommonN1前独立N3；R合法overlay真afterTake/ACTIVE；75只定位P29wrapper。

Files（5）：`test/simcluster/drills/37-hot-update-broker.sh`; `test/simcluster/drills/39-hot-update-legacy-n1.sh`; `test/simcluster/drills/75-hot-update-flows.sh`; `test/simcluster/drills/lib/hot_update_broker.sh`; `test/simcluster/tests/hot-update-broker-drill-test.sh`。

Tests / mutations：T58；parentjoin/missingarm/wronglegacyN1/unreachableR/agentchangedscope。

Inventory / leak / observation / test dependencies：五headerarmtuple/cost/legacyclaim供P33；childprocessWait/fixturecustodyselftest；live须P11–P29/P33。

#### P31 · contracts / structural / layering / ctx / tags gates（NORMAL）

目标：P0stubproduction不可达、storeprematureseal、noOp/migrationdiff、caps/AllOS/layering/ctx-freealias/buildtaglocality。

Files（8）：`test/architecture/broker_image_contract_test.go`; `test/architecture/structural_budget_test.go`; `test/architecture/layering_test.go`; `test/architecture/dataplane_lifetime_test.go`; `test/architecture/build_tags_test.go`; `test/architecture/ctx_background_ledger_test.go`; `test/architecture/legacy_ctx_background_sites.go`; `test/architecture/upgrade_budget_pin_test.go`。

Tests / mutations：T53；每gate同文件positive/negative/realnegativefamily；自写parserfuzzownership核。

Inventory / leak / observation / test dependencies：新gate registry名/cap/ctx站点唯一producer；无执行runtime资源。tests需各impl，不能只P0stub假绿。

#### P32 · public + private image inventories gates（NORMAL）

目标：public多rootappendonly、brokerprivatepartition闭合kind/descriptor/runtimeencoderdecoder三向一致。

Files（4）：`test/architecture/image_field_inventory_test.go`; `test/architecture/broker_state_inventory_test.go`; `test/architecture/testdata/image_field_inventory.txt`; `test/architecture/testdata/broker_state_inventory.txt`。

Tests / mutations：T47；删字段/改type/tag、privatecodec漏项、全局kind污染旧reader。

Inventory / leak / observation / test dependencies：image/privategolden唯一writer；消费P02/P03/P07/P15/P25fieldmanifest；tests须这些owner。

#### P33 · leak / race / enum / test / sim ledgers gates（NORMAL）

目标：所有精确leakanchor/closedenum/whole-race/test/gateinventory及simbudget/owner/blocked/cost唯一写者；新hot-update-broker-ledger.tsv固定candidate_sha/arm/claim/status/blocked_by/legacy_owner/marginal_seconds/reader_digest/receipt列；保原所有rows。

Files（19）：`test/determinism/leak_assert_shape_test.go`; `test/determinism/enum_switch_default_test.go`; `test/determinism/test_inventory_test.go`; `test/determinism/testdata/test_function_inventory.txt`; `test/e2e/all_phases_test.go`; `test/e2e/parallel/inventory_test.go`; `test/e2e/parallel/testdata/matrix_units_golden.txt`; `test/architecture/testdata/structural_budget_golden.txt`; `test/architecture/gate_registry_test.go`; `test/architecture/simcluster_gate_set_test.go`; `test/simcluster/expected-verdicts.tsv`; `test/simcluster/expected-verdicts-log.md`; `test/simcluster/contention-sensors.tsv`; `test/simcluster/drill-costs.tsv`; `test/simcluster/tests/run-all.sh`; `test/simcluster/tests/hot-update-broker-ledger-test.sh`; `internal/upgradebudget/budget.go`; `test/simcluster/drills/lib/hot_update_budget.sh`; `test/simcluster/hot-update-broker-ledger.tsv`。

Tests / mutations：T54/T58ledger；删除package/anchor/gate/child、错误rc/owner/预算值。

Inventory / leak / observation / test dependencies：全局golden唯一writer；输入逐pkgmanifest、用P31caps/P30arms/P29fixture；最终tests须P01–P32。

#### P34 · docs / tasklist receipt-only tick（NORMAL）

目标：维护entry/N1single/Run内exec/store/currentrollback/auth/retry/verdict/fixture/gate文档，纠正旧hotrollbackbackup建议；仅有receipts后勾子项。

Files（11）：`CLAUDE.md`; `docs/distributed-broker-architecture.md`; `docs/deploy-tier-gotchas.md`; `docs/broker-ops.md`; `docs/cluster-runbook.md`; `docs/usage.md`; `docs/reviews/hot-update-tasklist.md`; `docs/reviews/hot-update-stages-plan.md`; `docs/testing-standards.md`; `test/simcluster/README.md`; `test/hotupdate/README.md`。

Tests / mutations：documentationaudit/链接sourcecode/命令tree核；无receipt总勾mutation红。

Inventory / leak / observation / test dependencies：覆盖claim与evidence索引唯一writer；可先写正文，最终签字需P35receipts。

#### P35 · final integration / receipts / marginal costs（NORMAL）

目标：唯一最终验证/同候选receipt汇编；不代改其他owner源码、不重跑洗绿；证据和tasklisttick供P34。

Files（1）：`docs/reviews/hot-update-broker-evidence.md`。

Tests / mutations：三硬闸+gates+parallelmatrix+Darwin+37/39BR、mutations原始rc及wall。

Inventory / leak / observation / test dependencies：每exit/claim/cost/sourceSHA receipt唯一汇编；发现seam缺陷送回文件owner，复测只因新fix。

### 12.2 dependency DAG 与接缝裁决

```text
P00 contracts/stubs
  ├─ P01/P02/P03/P06/P07/P08/P13–P25/P29/P31–P34（合同内可并行开工）
  ├─ P03 → P04 → P05
  ├─ P08 → P06/P09/P10；P06+P08 → P07 codec/wrapper land
  ├─ P15+P25 → P16/P17；P15 → P14；P11+P14+P16 → P13 全home决策
  ├─ P25 wire → P18 retry/clock → P19 → P20 → P21；P24+这些producer → P23 connect/RunE wiring
  ├─ P01–P04+P07–P10+P12+P25 → P11；P24+P02+P11 → P22 main/serve wiring
  ├─ P04+P11+P15+P16+P18+P25 → P24 driver land；真实RunE tests再等P23
  ├─ P01–P12+P22+P25 → P26 real store/process fixture
  ├─ P26+P11/P12+P15–P23+P25+P29 → P27 control seam
  ├─ P26+P01–P12+P14+P22 → P28 flow/rollback seam
  ├─ P29+P11–P28 → P30 live activation
  ├─ 全runtime/fieldmanifest → P31/P32/P33 final gates/ledgers
  └─ P01–P33 receipts → P35 final evidence → P34 receipt-only tick
```

P11/P22的生产接线可先编译，真实tests在各实现合流后跑；P07从P00合同实现codec，P06仅定义freezeServerRuntime与safe-point实现；P07定义Server.FreezeServer薄wrapper、ServerFrozen/ImportServer与codec，最终land需P06/P08，二者不互改文件。P01/P02只调用Config中P0冻结的FreezeServer/ImportServer function-typed callbacks，不引用P07尚未land的symbol；P11填真实method/function，niladapter只返回明确unsupported，生产不可达compile-stub。P26入口fixture接口P00冻结，测试中partialconstructor/ACK/replay/FDbarrier只有P26一份。P29livefixture不依赖Gointegration的内部codec当唯一oracle。P31/P32/P33的早期gatecontrols可并行，最终rows待producerland后统一生成；P34先写文档，tick延后不形成实现DAG循环。

| seam artifact | 唯一owner | producer / consumer 与明确交付 |
|---|---|---|
| P0接口、compile-stubs、field/tag/enum/domain命名 | P00 | 各impl只交具体方法体；不私自修改合同 |
| producer0 / reader0 / actualclose / finalPrepare receipts | P01（编排）；P26（独立integration oracle） | P03registry、P04Node、P05snapshot→P01；最终seal不能由consumer提前造 |
| runtime snapshot metadata→FSM floor | P05 | P04clusterio传已绑定RPCticket；P26强制newACK在Restore前interleave |
| listenerkeeper / view / Unixpath | P08 | P09HTTP/P10admin/P06tunnel→P08同合同；P28真exec独立oracle |
| broker private descriptor / codec | P02 | P03/P15runtimeprojection→P02；P32盘点；没有第二privatecodecowner |
| Server private descriptor / callback safe point | P07 | P06产runtimehooks→P07；P28测latecallback/alias；P32盘点 |
| actualFDmultiset / aliasregistry / finalrevoke | P01 | P02验证，P06/P07撤权，P08keeper；P28跨Preparebarrier真oracle |
| maintenanceproof/expiry/baseline | P15 | P16replicate+CAS、P17真实agentACK、P13insidewriterguard、P24driver消费 |
| capability wire / RuntimeReport / remotehealthshape | P25 | P11/P12/P17接生产；P23/P24消费同schema；P32/P33登记 |
| absolutebudget / attempt通道 | P18 | P19/P20/P21/P23各emit；P27验证，P29collector收全部attempt |
| hermeticrealprocessfixture / faultartifactcustody | P26 | P27/P28复用，不各造私有DB/barrierfixture |
| liveflow_fixture / brokercollector / 原socket oracle | P29 | P30只供给/stage/productcall；75复用；缺命令/无样本直接setup/incomplete |
| wiregolden | P25 | producer申报，不自己append本表以外golden |
| image/privateinventorygolden | P32 | P02/P03/P07/P15/P25申报，反射+runtimecodeccontrols |
| test/leak/race/enum/sim/structuralledger | P33 | 每包交exactrow+exerciseanchor；P31只写gatecode，P30只写drillcode |
| exit/claim/nativeverdict/cost receipts | P35 | P34仅凭这些tick；不会替未实现fixture签覆盖 |

“编排owner”和“独立oracleowner”是不同artifact，不能互相推卸：P01必须产生可检验的finalseal，P26必须自己核真实顺序。若P0合同缺某项，暂停该依赖接线、由P00串行补合同后所有消费包重新编译；其他独立包继续。不是让两包各补一个不一致字段。

## §13 ledgers、验证顺序与边际成本

所有行由唯一goldenowner实际落盘，不接受“包名已在某矩阵出现”代替新路径whole-package tuple。P33给每生产owner一份最终`file:Test→exercise→sharedassertsite→raceunit→inventoryrow→receipt`对账；P35把row与同SHA输出绑定。

| 闸 / row | 文件owner / 精确新增内容 | 边际秒数估算（不是实测） |
|---|---|---:|
| `BrokerImageContracts` | P31：P0签名、productionstub不可达、seal-before-close、无Op/schema、candidate不开store；positive/negative controls | 0.3–1 |
| public image roots | P32：既有imagegoldenappend`brokerimage.Bundle/StoreStamp/Seal/tunnel.ServerImage/listenerimage.Descriptor`可达字段，旧roots不删 | 0.2–0.6 |
| broker private partition | P32：两私有record词表+codec/runtime/descriptor三向、旧sealed全局12kind不变 | 0.3–1 |
| wire + canonical / admin roots | P25：proto/adminactualroots、旧bytes、新signedextension、NodeRegistergoldens、Health7→8 | 0.2–0.6 |
| structural / layering / ctx / tags | P31代码，P33golden：70/285/<18000、57/32、AllOS、leafNATS/Raft禁入、新roots、ctx-none精确减少/登记 | 0.5–1.5 |
| leak shape / risk package / anchors | P33：各§12Test的direct双assert/≥5exercise、riskpackage包含新owner，不加例外 | 0.3–1（真实cycle另计） |
| enum / detail producer-classifier-exit | P33：Mode/Phase/ClosePhase/RetryFamily/MaintPhase/AttemptPhase/ListenerRole显式families；P25/P23覆盖emit与exit | 0.2–0.7 |
| test inventory / whole matrix | P33：testfunctiongolden+`TestHotUpdateBrokerMatrix`tuple+matrixunitsgolden双向；旧Flow/Exec不删 | 0.3–0.8（race运行另计） |
| budget pins | P33常量/shell，P31gate：`BrokerMaintenanceTTL=RequestLatency+3s=8s`、RequestLatency5s/ProbeRate10/LiveFlowStall5s/LocalFreeze500ms与现值相等 | 0.1–0.3 |
| sim broker hermetic selftests | P29/P30源，P33run-all/ledger：missingfixture/arm/slot/attempt/nativejoin/五header/BRearlydispatch/Rstage非空 | 1–3 |
| docs audit / gate registry | P31/P33接现gate路径；P34正文/claimlinks无deadTest/命令，README与ledger一致 | 0.1–0.5 |

新增Go门预计总2.5–8s；shellselftest1–3s，集中`make gates`边际约3.5–11s，按CLAUDE现3m32s基线估，不承诺无cold-cache增长。`TestHotUpdateBrokerMatrix`估120–360s（deadline600s），≥5cycles真实资源测试预计合计20–60s，由parallel矩阵和unit按唯一owner执行；drill预算见§10。P35每项记baseline同runner/同cache条件、afterwall、marginalwall和compiler/testruntime分账；不能把build十分钟或缺真实drill装成0s。

新增包directcaps冻结：brokerimage生产files≤12（`contractstub`独立pkg≤1）、code<4000、各新增type methods<40；listenerimagefiles≤6/code<1200，brokermaint≤4/<900，controlbudget≤4/<500，clusterio≤4/<900。超过globaldebtthreshold时P33以实测量首次注册golden，值只允许当前quantum且不突破这些caps；低于threshold由P31直接cap控制，不能写一行假golden宣称门已有。既有tunnel4000意味着必须<6000，cluster2000意味着<4000，cmd12000意味着<14000；不抬这些debtrows。cmdmain现golden1000为量化下界；新命令先移alert/clusterdrivehelper，目标不跨当前1100量化边界，额外全局<1300同守。实际缩小后按gate收紧，不把历史1200重新恢复为宽松值。

验证由P35在可写实施环境执行，**本只读综合轮不执行**：

1. entry收据：S5 final-fix的22findings逐项关闭、actual live §22.8一致；P29先交可运行75 fixture供S5关闭live缺口，再取得S5lead出口签收。P0合同/compile-stubs可准备；S6productionassembly、finaldrill不能在S5出口未闭合时宣称开始验收。
2. 每包先appropriateunit、同predicate三向control与声明mutation红→还原正控绿；手写broker/private/snapshot/maintenancecanonical parser分别真实fuzz30s，seed在普通test确定性回放，随机fuzz不加PR/tag门。所有childhelper必须kill+wait/正常wait，不能跨test剩进程。
3. P26/P27/P28真实seams合流，执行whole-package`-race`与五轮FD/goroutine测试。先集中`make gates`（含vet-tags/hermetic/run-all/lint）；lint唯一执行owner，禁止与另一lint并行触发全局锁rc3。
4. 三硬闸是且仅是`make test`、`make e2e-parallel`、`make lint`；`make gates`作为改闸的集中自检另列。parallel全绿后不再串行全矩阵复核，只定位并行失败的那个`make e2e-one T=...`；race矩阵membership不能靠全量串行掩漏。S6不另增一个stage外审作为出口条件。
5. Linux与Darwin正常编译/gates/platform边界检查；Darwin原生资源回归有runner才出PASS，无runner显式NOT_RUN不得关闭出口6。不支持liveimage的平台在Take/store副作用前明确拒绝，不能以Linux原生收据外推。
6. 37.A/A1/B/T/R与39.BR真实artifactnative执行、原始父子verdict+rc/counters+wall/source/artifactSHA/marker/阶段coverage归档；37missingarm、R没到barrier、BR没进旧Rununwind、wrongSHA、panic、emptycohort都不是已知负控成功。失败修复后仅重测受改动项与必要合流；不重跑直到绿、不重分类历史红。
7. P35签每exit与coverageclaim；P34最后勾tasklist子项与blockedrow对应索引，未有receipt保持未勾。lead逐行冻结计划后才成为binding实施计划，本稿不伪签S6完成。

500ms `LocalFreezeWindow`照S0/S3只作calibration分布：记录从旧最后required-domainpause到新**全部required-domain**可用，600ms但request/flow≤5s不会被写成错误的产品5s失败；5.1s请求/原flowstall必红。ComponentHandover120s只界组件artifactready后整体收敛，不得拿它替控制请求或maintenance TTL；15minFullStackRollout仍由S10验收。

## §14 docs、覆盖签收与 evidence 形状

| 文档 / claim | 必须改的事实与关联receipt | tick条件 |
|---|---|---|
| architecture §3/§7/§21/§22.4–§22.8 | SQLcommandfloor与Raftmeta分开；Runtime图/Run内exec/closephase/Serverimage；S1receipt边界；维护cap/proof/TTL；N-1四象限与currentrollback | 设计先写，行为结论只据T01–T52和nativearms |
| gotchas / broker-ops / usage | single主入口、N1boundedwrite、N3transfer/scope、stageSHAvsrunningSHA、transfersdefer、freshlogin不无限retry；正常stop与hotcut明确行为 | 原commandtree与实际candidatecommands receipts |
| cluster-runbook | 维护缺capability切前拒绝/rollingholder、安全expiry；移除“热回退恢复旧备份”的建议（backup restore仍是独立DR操作）；joinWARN不暗示自动可热升级 | T37–T45、37B、39BR |
| testing-standards / CLAUDE | newgate/matrix/leak/directcaps/ctxexports/actualfiles、三硬闸名称、fuzz和platform收据；不把未实测成本写成事实 | P31–P33同SHAcontrols/gates |
| simcluster README / cost / expected | 37五arm三laneGREEN与39BRN3native红、Rheld/active、真实fixture命令和custody、原slot/attempt/bytesoracles、5s/500ms区别 | P29selftests + P30native + P35cost |
| E-4 / plan coverage row8 | currentDB ACKprefix/replay前floor、singlewriter/flock/JSindependentwitness、currentcutrollback | T05–T17/T37/T49/T51；37A/A1/B/R |
| D-2 broker / plan row4 | listener/raw/OFD原socket、carrier可替换、freshzero refused、halfclose/slowtail | T18–T28/T48；37A/A1/B/R |
| D-4 broker / plan row4 | home/epoch/tokenhash/pins、fullidentity/五fences/refunion/lateclose、maintenance真实observer | T24–T28/T38–T44；37B/R |
| D-6 broker / plan row4/5 | trueprotocol/byteoracle、race/leak/mutation、37GREEN、39BR旧退出负控、docs | T46–T58；全部nativearms与gates |
| B-4新控制请求/旧映像退出子项 | 10/s所有slot/attempt、CONNECT与request分账、truePID/sideeffect恰1、≤5s、runningSHA确切变更 | T29–T36/T50；37A/A1/B每hop |
| stage主plan §2 / §3.4 | 单writer/控制连续/原socket/真实退出与scopeboundary逐claim链接 | 上述交叉claim全部齐，不总勾S7/S8/S9/S10 |

每receipt至少 `{claim, test_or_arm, command, native_rc, verdict, counters, source_sha, artifact_sha, previous_sha, pid, startticks, wall_seconds, phase_intersections, oracle_inputs, mutation_digest, stdout_path, stderr_path, log_mapping}`；flow附双向seq/hash/FINtail/accept/socketcookie，store附currentdevino/journal/FULL/drivercounts/cursor/ACKIDs，control附expectedslots/allattempts/truestartedPID/effects/blackout各端点，maintenance附record/proofnonce/expiry/CAS/appliedACK/roleedges，rollback附ACTIVEentry/newACK/newoffset/newgeneration。禁止secret、exportedticketkey、rawtoken/nkey/JWT进入receipt。所有路径相对archivedrunroot，raw文件SHA与collector版本存档；collector缺logsource/时间相位/expectedID即不签。

## §开放问题（仅产品 / 范围，推荐值已用于本候选）

| # | lead裁定事项 | 推荐 | 影响 |
|---|---|---|---|
| Q1 | S6是否明确把maintenancecapability+ACK的agent接线与先行安装能力作为hot-cutentry交付？ | **是**。S6拥有P17和additivewire；37三lane先部署capableagent，B窗口不升级agent；缺cap的旧fleet普通wire保持、plannedhotcut拒绝，首次迁入留S8。 | 不假称S5已经实现；若否，需缩承诺或重新设计可证明silence-escape方案，不能只删ACK测试。 |
| Q2 | single是否同时提供`node upgrade --broker`别名？ | **不加**，只提供`broker upgrade reload`明确入口与状态；`node upgrade`保持agent对象。 | 减少两个表面的帮助/错误/commandgolden负担；若lead要别名，P23/P24在现files接同handler，不能新增cmdfile。 |

其余存储顺序、scope、TTL、snapshotfloor、bootrole、retryeligibility与verdict都是依据合同/代码已裁决的工程选择，不作为开放问题延期实现。

## §附录 A · 四评 disposition（逐项，不遗漏 BLOCKER / MAJOR）

编号取各 critique 最后“must-fix”清单，`FD/WR/CL/ME`分别对应crit-fd/crit-writer/crit-cluster/crit-measure；正文D/F反例由该条落点一并覆盖，不与各draft的T编号混用。`采纳（局部拒绝）`表示问题保留，具体事实/策略分歧以右栏代码证据裁决；没有“以后再考虑”的BLOCKER。minor亦登记。

| critique item / 严重度 | disposition | concrete amended design / 本稿落点 |
|---|---|---|
| FD1 BLOCKER（D2/D3/D13） | 采纳 | keeper/view分离、Unix不unlink、HTTP/adminacceptedhandler真实join；§6.1/P08–P10/T18–T20。 |
| FD2 BLOCKER（D4/D5/D6） | 采纳 | Endpointpause保raw/slots/FIFO；callback/monitor真实join；§6.2/P06/T21–T23。 |
| FD3 BLOCKER（D8/D9） | 采纳 | nonnilTake先登记、successfulimport外层失败也DetachPristine全salvage；§4.2/P02/P07/T04。 |
| FD4 BLOCKER（D11/D12） | 采纳 | allclosephase+actual0之后finalPrepare；OpenCurrent；unknown禁止第二open；§3.1–§3.3/P03/P04/P26/T09–T12。 |
| FD5 BLOCKER（D7） | 采纳 | finalcut与revoke线性化、rawSHUT_RDWR全部dup、listener全alias移除；§6.2/P01/P28/T25。 |
| FD6 BLOCKER（D10/D14） | 采纳 | RuntimeSlot当前owner、Run内exec、Execerrorcurrentreopen、ACTIVEcurrentcut；§1.2/§4/P01/P11/P22/P28/T02/T49/T52。 |
| FD7 BLOCKER（D1/D9/D13） | 采纳 | actualnonCLOEXECwhite与OFDaliasmultiset、坏manifest早census、flagrestoreerrors；§4.1/P02/T27。 |
| FD8 BLOCKER | **拒绝fresh无receipt也retry的策略；采纳绝对budget/真实空窗测试** | `ctl_connect.go:126–169`与architecture§22.4 S1item10固定eligiblehistoricalreceipt、首次login一拨；扩大freshPIN权限无授权。§8/T30/T33，不用预种receipt掩盖CONNECTsample。 |
| FD9 BLOCKER | 采纳 | 独立PlanScope、nativebroker-onlydrive与agentPID/startticks/SHA连续；§5/P24/T45/37B。 |
| FD10 BLOCKER | 采纳 | 37原生GREEN五arms，旧Rununwind39BRN3，INCOMPLETE4；§10/P30/T58。 |
| FD11 MAJOR | 采纳 | `tunnel.go:416,535–540`wg不是carrierjoin；rawExportborrow、rawvalidatorSO_ACCEPTCONN0；§1.1/§6/P06/P08。 |
| FD12 MAJOR | **采纳startup纠正，拒绝一并删真实runtimefloor** | `node.go:303`确为skipstartup；`fsm.go:420/snapshot.go:139–155`runtimeinplacerestore真实。保T17、实现T15动态floor；§3.4/P05。 |
| FD13 MAJOR | 采纳durable/CAS/短TTL/dwell；**拒绝“不新增agentwire也保证全部窗”** | `agent/roster.go:287,311–318`silent2即时wake可escape，backoff无保证；S6明确扩entry/P17而非假称S5。§7/Q1/T39–T44。 |
| FD14 MAJOR | 采纳 | 唯一Begin→validatedRoleTakeRole、wrongargv+missinghint同测；§4.1/P22/T03。 |
| FD15 MAJOR | 采纳 | `expose.go:533–544`coldrandomissuer/budget被hot绕开；explicitfactoryowner/freshSQL；§6.3/P12/T28。 |
| FD16 MAJOR | 采纳 | actualrunningSHA/gen/phase统一local与remote、真实wiregolden；§5/§9.1/P11/P24/P25。 |
| FD17 MAJOR | 采纳 | currentDB/forwarder/PINapply→checkedcalloutSUBFlush+真JWT/CONNECT→business；§8.1/P12/P27/T29。 |
| FD18 MAJOR | 采纳 | 单命名/contracts，localprivatepartition，多rootwire/image、enum/error/ctx/tag/leak/race精确owner；§2/§9/§12/§13。 |
| FD19 MAJOR | 采纳 | 强制交错+FDidentitymultiset、5真实cycles、claimreceipt逐项绑定；§11/P26–P28/§14。 |
| FD20 MINOR | 采纳 | 三硬闸名称与57/32/70、AllOS、belowthresholddirectcaps；§9.3/§13/P31/P33。 |
| WR1 BLOCKER（D1） | 采纳 | OpenCurrent/NewForImage/NewProductionForImage无migration/CREATE/bootstrap；§3.3/P03/P04/T07。 |
| WR2 BLOCKER（D5） | 采纳真实取消/join；拒绝raw-first建议 | committedApply不被cut丢弃，真实payloadRPCclose/join，TLSclosejoin→ManagerFreeze；§3.2/§6.2/P04–P06/T16/T23。 |
| WR3 BLOCKER（D3） | 采纳 | 最终store全部closedreceipt之后才seal/Prepare；§3.2/T09/P01/P26。 |
| WR4 BLOCKER（D2） | 采纳 | canonicalDB决定唯一lockdomain，cold/hot/offline统一binding、拒alias；§3.1/P03/P04/T05。 |
| WR5 BLOCKER（D4） | 采纳partialphase；**拒绝currentNew必泄漏injectedtransport事实** | `node.go:118–125`已关errorinjectedtransport；补acceptedRPCjoin和partialunknown责任仍必要。§1.1/§3.2/P04/T10/T55。 |
| WR6 BLOCKER（F1/D7） | 采纳 | startupskip事实纠正、真runtimeRPCmetadata/config/动态floor拒绝、freshcatchup；§3.4/P05/T15/T17。 |
| WR7 BLOCKER（D8） | 采纳 | replay前独立ACKrows+cursororacle，旧库覆盖/WAL丢失不能replay洗绿；§3.3/P26/T13。 |
| WR8 MAJOR（D10） | 采纳 | firstpotentialconstructor之前ACTIVE，错误也currentcut新seal；§4.2/P01/P02/T49。 |
| WR9 MAJOR（F6/D6） | 采纳 | actualdriverconnregistry+independentOFDcontender确定性交错，不reflock；§3.1/§3.2/T08/T12。 |
| WR10 MAJOR（D13/D14） | 采纳 | nativebroker-onlyscope、actualRunningSHA/gen/phase统一完成查询；§5/P24/P25/T45。 |
| WR11 MAJOR（D9） | 采纳 | single保DELETE/WAL、effectiveFULL、checkpointN/A/失败合同；§3.2/P03/T11。 |
| WR12 MAJOR（D12） | 采纳 | replicated≤8s、manual/auto/from/to、历史dwell/plannedreturn真实owner；§7/P13–P17/T39–T44。 |
| WR13 MAJOR（F2/exit3） | 采纳 | 原CallID/handle/payload、RunE绝对budget贯穿deepdial/inbox；§8.2/P18–P23/T30–T34。 |
| WR14 MAJOR（D15） | 采纳 | 合法过preflightafterTake/ACTIVEartifact、真旧N3follower/homeBR；§10/P26/P30/T49/T58。 |
| WR15 MAJOR（D16） | 采纳 | Role一次dispatch，wrongargv与missinghint同时反例；§4.1/P22/T03。 |
| WR16 MAJOR（F3/exit5） | 采纳 | 37parentGREEN、39BRnative红、INCOMPLETE4、五真实manifestheaders；§10/P30/P33。 |
| WR17 MAJOR（D11） | 采纳 | independentcurrentJSwitness NUID/chunks/config/consumerseq/audit，无reset/recreate/第二server；§9.2/P26/T51。 |
| WR18 MAJOR | 采纳 | actualwiregolden/adminroots/localprivatepartition闭合kind唯一owner；§2.2/§9/P25/P32。 |
| WR19 MAJOR | 采纳 | cmd/clusterupgrade/proxydial/natsinbox whole-race+≥5partialconstructor/closeanchors；§9.3/§12/P33/T54–T57。 |
| WR20 MAJOR | 采纳 | cluster32/cmd57Δ0、broker70/285/<18000、newlogicleaf/cli；§9.3/§12/§13/P31。 |
| WR21 MAJOR（D17） | 采纳 | fullfilename+contentSHA exactsets SchemaStamp纯映射，无整数schema谎言；`storage.go:193–226`；§2.1/§3.1/T07。 |
| WR22 MINOR | 采纳 | 只有test/e2e-parallel/lint三硬闸，gates另集中跑，不加外审出口；§13。 |
| WR23 MINOR | 采纳 | exactowner/filetest/mutation/race/leak/receipt，Health7→8、enum/detail/ctx/tags与claimlinks；§11–§14。 |
| CL1 BLOCKER | 采纳 | 全mutator writerapplyMu内currentmaintenance/from-to/事件决议、commit/readback；§7.2/P13/P14/T41/T42。 |
| CL2 BLOCKER | 采纳 | fullallocationidentityCAS，staleA不能更新复用portB；`port/plan.go:273–295`实缺；§7.2/T26/T42。 |
| CL3 BLOCKER | 采纳 | 不以oldleader零dwell+5<10保证；缺actualobserver/capACK拒cut、ordinaryN-1不变；§7.1/§9.2/Q1。 |
| CL4 BLOCKER | 采纳 | signednonce/issuerfixedexpiry、localt0映射/年龄、leadercache、crash≤8s；§7.1/P15/T39。 |
| CL5 BLOCKER | 采纳 | acquire/renew/expire/release/localreloadholder全signedCAS；§7.1/P16/T40。 |
| CL6 MAJOR | 采纳 | scope独立不偷换AgentAbsent，B真实共址agent不升级；§5/P24/T45。 |
| CL7 MAJOR | 采纳 | everySTEPfreshconfig/leader、constructorNotifyCh、roleedgeABA/term/configobserver到Shutdownjoin；§5/P04/P24/T38。 |
| CL8 MAJOR | 采纳 | silent2+即时topologywake、currentroute身份/hold安装ACK、expiry/controlescape与SQLhome分账；§7.3/P17/T44。 |
| CL9 MAJOR | 采纳 | planneddown/return来源与每home真实baseline保存，ended后仍不误auto-return；§7.2/P13/P15/T43。 |
| CL10 MAJOR | 采纳 | Boot/Instance/key/fivefences/refunion/finalcut，seal后不ACK新mutation；§6.2/§6.3/P07/P28/T24–T28。 |
| CL11 MAJOR | 采纳 | 保NoSnapshotRestoreOnStart，startup幂等replay、真runtimecatchupfloor；§3.4/P05/T15/T17。 |
| CL12 MAJOR | 采纳 | 39BR真N3旧unwind、37原生GREEN、INCOMPLETE4、RvalidafterTake/active；§10/P30/T58。 |
| CL13 MAJOR | 采纳 | S1receipt边界+absolute5sdeepdial与成功清deadline/joinhook；§8/P18–P23/T30/T33/T34。 |
| CL14 MAJOR | 采纳 | actualSHA/gen/ready、signedholder/maintenancewire、localprivatepartition避免全局kind扩散；§9.1/P25/P32。 |
| CL15 MAJOR | 采纳 | clusterupgradewhole-race、全newriskexact≥5anchor、ctx/enum/error/tag/testinventory/caps；§9.3/§12/§13/P31/P33。 |
| CL16 MAJOR | 采纳 | singlecanonicalDB-lock、no-migrationopen、全部actualclosed后finalimage；§3/P03/P04/P26。 |
| CL17 MINOR | 采纳 | 一套A1/T/phase命名、三硬闸与Health8、runbookWARN/backup误导、claimreceipt后tick；§10/§13/§14。 |
| ME1 BLOCKER | 采纳 | 真RunEbanner/health/discovery→DNS/multiURL/proxy/TLS/WSS/inbox统一5s及取消join；§8.2/P18–P23/T30。 |
| ME2 BLOCKER | 采纳 | probe2stimeout不是unsupported，2.1s黑窗create1/legacy0/effects1；§8.2/P21/T31。 |
| ME3 BLOCKER | 采纳 | 底层全部Attempt sink、logicalID固定、success保失败/overflow红；§8.3/P18/P29/T35/T50。 |
| ME4 BLOCKER | 采纳 | 37GREEN/39native旧红、parentjoin不改、INCOMPLETE4；§10/P30/P33/T58。 |
| ME5 BLOCKER | 采纳 | BR真previousN3follower/home、39commonN1之前dispatch、原旧unwindbarrier；§10.2/P30。 |
| ME6 BLOCKER | 采纳 | Rcompatible合法image、afterTakefaultSHA固定、真ACTIVEnewACK+newbytes currentcut；§4.2/§10/P26/P28/T49。 |
| ME7 MAJOR | 采纳 | absolute10/sslots全部登记、每class每hopwindow真实相交、missing/仅precutoverlap为incomplete；§8.3/P29/T50。 |
| ME8 MAJOR | 采纳 | localcallouthandlerfence/checkedSUBFlush/真实validatedJWT端点；peerprobe与采样区间另记；§8.1/P12/P27/T29。 |
| ME9 MAJOR | 采纳 | 保historicalreceipt与首次一拨，eligibleauthviolation不可辨revoked/blackout最多到绝对deadline，最终原autherror；§8.2/P23/T33。 |
| ME10 MAJOR | 采纳 | 产品broker-onlyscope+原agentPID/startticks/exe连续；§5/P24/37B。 |
| ME11 MAJOR | 采纳 | 固定≤8sTTL不拿120swatchdog；historicaldwell与expiry后真实failover反例；§7/P13/P15/T39/T43。 |
| ME12 MAJOR | 采纳 | 删除虚构startupRestore，NoSnapshotRestoreOnStart保持；§3.4/T17。 |
| ME13 MAJOR | 采纳 | actualallclosed之后Seal/Prepare；partialunknown不能以provisionalClosed恢复；§3.2/T09/T10。 |
| ME14 MAJOR | 采纳 | attachmentfence→TLSclose/yamuxworker+monitorjoin→ManagerFreeze，partialnpositive全账；§6.2/P06/T23。 |
| ME15 MAJOR | 采纳 | 确定性LOCK_UNpark+独立contender+normalStop正控，不reflock；§3.1/P26/T12。 |
| ME16 MAJOR | 采纳 | singlejournal保持、canonical共享lock，无额外DELETE→WAL迁移；§3.1/§3.2/T05/T11。 |
| ME17 MAJOR | 采纳明确扩scope/entry的分支 | S6拥有P17capACK交付，S5不被假定实现；普通oldwire仍兼容，缺capcut前拒绝；§7.1/§13entry/Q1。 |
| ME18 MAJOR | 采纳 | publicmulti-root+privateactualcodecpartition、proto/adminscanner与golden唯一owner；§2.2/§9/P25/P32。 |
| ME19 MAJOR | 采纳 | cmd/clusterupgradewhole-race、每newriskowner≥5真实exercise/direct双assert；§9.3/§12/P33/T54–T57。 |
| ME20 MAJOR | 采纳 | 展开244文件唯一归属、broker/57/32硬预算与belowthresholddirectcaps；§9.3/§12/§13/P31。 |
| ME21 MAJOR | 采纳 | P29独占missinglivefixture/collector，供S5finalexit；custody/barrier先于37；§10.1/§12.2/§13entry。 |
| ME22 MAJOR | 采纳 | E-4/D-2/D-4/D-6broker/B-4/row4/8与主plan§2/§3.4逐claim同SHAreceipt；§0/§14/P35。 |
| ME23 MINOR | 采纳 | 三硬闸名称、ps不提示无限加timeout；失败样本只受控classfixture必须命中，短window每class≥1freshstart非每窗20；§8/§10/§13/P23/P29。 |

FD critic的锁/OFD与partialTake反例、writer的durablefloor/closing与schema反例、cluster的writercriticalsection/proof/ABA反例、measure的slot/attempt/nativeverdict反例均进入具体Test与owner；被拒绝的只限上述有现代码/冻结S1证据的归因或策略，不删除真实负控。
