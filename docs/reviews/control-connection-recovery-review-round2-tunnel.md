Pass — 第二轮内部隧道/恢复审查完成；T2-F1 与修复期 T2-F2 均由真实反例、修后复验及交叉质疑闭合。此结论不代替独立外审门。

范围：HEAD cab90d1；index 干净；全部未暂存和未跟踪修改为增量，文件摘要见 round2-start-sha256.txt。身份为内部隧道/恢复审查专家，只读生产实现，不暂存/提交/部署。

独立粗读后建立的审查面（此段先于阅读首轮报告落盘）：

1. OpenHome 的同 slot 判据必须覆盖 sid/nid、token、public/local port、有效 home 地址、epoch；空 pins 的保留规则与显式 named home 安全边界不得相互绕过。
2. pin 证明必须来自所安装 transport；desired pins 更新、无 pin fallback 升级、正在进行的 unpinned/pinned redial、失败后继续 supervisor、旧/当前证书宽限期分别核验。
3. OpenHome/Close/SetNID/Start cancel/ApplyHome/redial swap 的线性化；旧 generation 不安装、不删除新 slot，不复活已撤销端口；同配置重放只留一个 supervisor。
4. adapter 的 mapping、real local dial 和 Client desired localPort 一致；成功、失败、旧 epoch、取消、rename、并发替换不能把旧流路由错或留下错误 cache。
5. replayExposes/applyHome/applyReconciliation 对 live、down/recovering、终止、需要 pins 的 slot 分支必须既保持已有流又恢复缺失会话；proxy ready 信号必须对应真实转发。
6. REGISTER bind-before-replace 的服务端语义：活重放避免自撞，真正 token/home/epoch 差异仍到达原有验证，失败不破坏已有桥。
7. 完整 broker/SQLite/NATS/agent 重启测试是否覆盖同生命周期自主恢复、原 token/allocation、恢复后真实 payload；单 tunnel.Server 证据边界、历史 #4 未采样 payload 边界。
8. 清理与资源：取消握手/关闭旧 transport/terminal deny/端口撤销后有界退出，superseded transport 不泄漏；复用已有泄漏门，必要时新增独占职责测试。
9. 全增量交叉面：新 ConnectedBrokerID 的 N-1 additive wire 与实际 ingress 来源、同路由迟到回复，bounded object reader 与控制连接生命周期、history exact number/trailing JSON、测试与账本/架构表述一致性。

建立审查面时已读：CLAUDE.md 角色和流程、testing-standards.md、requirements §6.4/10、当前 distributed-broker-architecture §6.5/7/控制连接补充、control-connection-recovery-plan.md；粗读全部 tracked diff、新生产 object_reader.go、新测试职责/主要恢复测试。当时首轮报告尚未读取；建立此面后才读取既有报告与交叉材料，不沿用先前 Pass。

以下记录独立证据、finding、修复过程与最终覆盖限制；中间候选的失败/通过收据均保留。

## T2-F1 — Major / 已证实：同 epoch ApplyHome 旁路真实 peer 升级检查

起点位置：internal/tunnel/tunnel.go:1728–1736（ApplyHome 同 epoch 分支）。此前仅更新 sess.certPins，未核验 sess.conn 的真实 TLS peer、未更新 sess.pinned、未保存 named home identity。OpenHome 新 replay 检查并没有覆盖这条生产入口。

可达链：internal/clusteroffline/init.go:302–303 的 --from-existing 对 ALLOCATED 行只回填 home_broker、不增 epoch；初始 epoch 仍为 0。internal/broker/home.go:184–186 原样下发该 epoch、named addr 与当前 pins。internal/agent/agent.go:2023–2035 对已有 session 调用 ApplyHome；2038–2055 将 nil 视为应用成功，持久化并 ACK。故数据面 fallback supervisor 跨控制会话存活/恢复后，可由实际注册对账进入同 epoch 升级。未要求生产产生不合法 epoch。

真实反例：新增独占 internal/tunnel/open_replay_lifecycle_test.go，TestApplyHomeUpgradeVerifiesExistingFallback，epoch=0、真实 TLS tunnel.Server、真实 HTTP payload。foreign-peer 分支给另一张真实证书的 pin，ApplyHome 错误返回 nil。matching-peer 分支虽返回 nil，session 没记录实际验证/named identity；再次 ApplyHome("", same epoch, empty pins) 仍 nil，并清空下次 redial 的验证要求。原样 -race 红收据：round2-apply-home-pin-before.log，0.121s。没有修改生产制造红灯。

影响：可将尚未证明 peer 的 fallback session 经控制面标成已应用的 named home，原有 unverified 流仍可持续转发；当前代码又允许清掉其未来重拨 pins。其安全影响有明确状态机证据，但未声称现场已经有 MITM 或真实泄漏事件。

建议：OpenHome 与同 epoch ApplyHome 共用基于真实 tls.Conn.ConnectionState 的升级校验；只有成功后写 pin proof、desired pins 和显式 named identity；已验证 transport 的正常 pure-pin rotation 仍保留已有流。已 named 的 slot 在同 epoch 变更地址必须拒绝；首次命名迁移需要独立处理（见后文 T2-F2），不能把 session desired 地址与 supervisor 实际重拨目标分叉。

主进程采纳并实现共用 verifySessionPeer + same-epoch address guard；审查者新增第二条 TestApplyHomeSameEpochRejectsDifferentAddress。首轮修后测试新反例不再报错，旧人工 pure-pin fixture 因没有 conn/pinned 前提而红，主进程随后修正 fixture。该次失败记录为 round2-apply-home-pin-after.log，不冒报整组通过。此 finding 已由下述复验关闭。

## T2-F1 修后独立闭合

当前修后位置：tunnel.go:1702 verifySessionPeer；:1741–1763 ApplyHome same-epoch 分支；OpenHome:1564–1568 共用证明逻辑。

`verifySessionPeer` 仅对尚未经过 pin 证明的 transport 检查实际 `*tls.Conn`，空/非 TLS peer fail closed；真实 slot 只在 DialContext 的完整 TLS 握手与 REGISTER 后安装。已经验证的 transport 允许正常 pure-pin rotation，下一次握手使用 desired pins。same-epoch 地址守卫在修改任何状态前拒绝已 named slot 的不同有效地址；首次命名迁移允许实际 peer 证明后的新 advertised endpoint，后续 redial 在 c.mu/gen 内读取它（T2-F2 修后规则）。保存显式地址后，后续空 fallback directive 无法清掉 named home 的 pins。swapTransport 仍独立验证被升级超越的在途 unpinned redial，失败继续同 supervisor。

主进程仅给既有 TestD6ReviewPurePinUpdateSameEpoch 的人工 rotation fixture 补 pinned:true 与前提注释，未修改断言。我独立查看 diff，接受这是修复夹具前提；新增真实 TLS 正反例承担升级安全验证。

原样复验：`go test -race -v ./internal/tunnel -run '^Test(ApplyHome|OpenHome|D6(ReviewPurePinUpdateSameEpoch|SameEpoch.*ClusterPins)|TunnelClientRecoversAcrossServerRestarts|SetNIDRetiresSessionsRegisteredUnderTheOldName|RedialAfterSetNIDIsTerminallyDeniedUnderTheNewName|TunnelReconnectCtxCancelInterruptsBackoff|TunnelReconnectClosesPredecessorConn)' -count=3`。

exit 0，23 个顶层 Test 每个真实执行 3 次，PASS 11.257s。日志 round2-tunnel-regressions.log。包括新升级正反例/不同地址、完整 OpenHome deadline/install 生命周期、保留已有/新流、正在恢复的唯一 supervisor、foreign pin、pin rotation、在途不可信重拨、SetNID、取消退避、旧连接清理，以及每次 5 轮真实 tunnel service restart 的 goroutine/fd 门。

审阅/执行时身份：
- internal/tunnel/tunnel.go SHA256 6b82f3ea86c5c1083ce3c37981b928078f15f8063cef476f7d303af0964f5e4d
- 新测试 SHA256 21fe5190f229deebb499021031f065bc435db637ce4014b864518ced65fb1793
- register_and_fence_test.go SHA256 11e445122b14406f1f5892ee0f9f54b9b3526bd7742e5697a96f8beb31a7014d

T2-F1 阻断已关闭；同 epoch 不同地址是同一状态机守卫的负控制，不另将不合法 directive 夸大为已发生线上故障。

## 独立审查面逐项处置

1. 身份/配置：Client sid 构造后不可变；publicPort map key + slot nid/local/token/epoch/effective addr 比较完整。fast replay 在 c.mu 内且复查 caller/Start ctx 与 nidGen。不扩大 token/home/epoch 授权。差异配置由真实 rejecting REGISTER 负例验证，失败保留旧真实 payload。
2. pin 证明：发现并闭合 T2-F1；初始不可信、在途不可信、重拨后 proof 与 normal rotation 三种状态分开。非空新 pins 更新、persisted 空 pins 保留均有真实测试。
3. lifecycle/generation：fast replay 不生成 ctx/goroutine、不增 gen；Close/SetNID 删除 slot 后旧 swap/drop 均受 gen 检查；Open 初始 handshake 完成后仍受 caller cancel、Start cancel、nidGen、epoch 守卫。定向测试覆盖可观测关闭/不晚装与改名拒绝，未宣称穷尽所有调度。
4. adapter：opSem 串行 AddProxy/ApplyHome/RemoveProxy，失败恢复 prior mapping；成功/epoch-no-op读 Client.LocalPort 同步，absence则清。数据面当前使用 supervisor 捕获的 localPort，不使用 LocalPortLookup。对这些新增 cache 分支进行了锁序与调用链静态审查，未新增 adapter 专用测试，也没有把 cache 修复夸大为已证实流量错路由。
5. reconciliation/proxy：已有 expose 经 HasSession→ApplyHome、缺失经 openHomeFromState→AddProxy，deferred pins可重试；T2-F1 曾使 ApplyHome nil 被persist+ACK，因此是实际调用链问题。正在 redial 的同配置 slot被保留表示desired state接受，不表示当刻数据面up；replay本身不发新的up hook，proxy/drop/redial原回调语义保留。proxy full start会先RemoveProxy，故不会把同配置 recovering expose replay的nil直接当成新的proxy transport ready。本轮未发现新增可复现虚假proxy-ready路径。
6. 服务端 bind：REGISTER继续真实授权、bind先于替换；fast replay只对已安装同授权slot跳过额外REGISTER，从而避免对自己监听的端口误bindfail。真实冲突仍失败。
7. 完整重启证据：独立读 test/p13/broker_restart_test.go：同一个 Agent.Run、文件SQLite关闭重开、稳定证书、broker-only与NATS-before-broker两窗口；每次禁keepalive新HTTP验证payload，恢复后token hash/allocation不变。此lane未重复执行完整broker测试；自身只执行tunnel service restart。历史#4仍仅有秒级REGISTER，历史payload未知；不把新回归回填历史。
8. 资源与退出：fast path无新资源；升级拒绝不关闭旧合法fallback；无效redial结果关闭并继续唯一supervisor；原取消/terminal deny/Close及旧transport清理保持。三轮定向race/五周期goroutine-fd门已真实运行；无部署/WAN/内核黑洞场景新增验证。
9. 跨模块：见下方固定三路交叉质疑。本文只追加自己的三个 Test，生产/旧测试/inventory由root统一修改；未stage/commit/deploy，未运行完整矩阵或lint。

## 固定三路交叉质疑（已完成，保留首修失败过程）

system：亲读 round2-system-review.md、roster_route_identity_test.go 与当前 production。认可S1：boot仅VerifyAt通过后填cachedRoster，持久pin/gen高水位保留；rosterCurrentAt只约束当前地址/身份推断。认可S2：uniqueRosterNodeAtHost对共享host的第二不同NodeID返回空；明确reply ID/同路由已证明ID仍优先。反例分别有真实cache加载/签名refresh/实际session cancel，正控保留真正retired→唯一survivor。未重复执行该lane矩阵；其最终race日志另由该lane负责。追问same-nc A→B→A：name/URL通常仍对应同稳定broker，未找到实际错误反例；order仅处理调用顺序，不声称request generation，保留确定性锁调度未覆盖边界。

transfer：亲读当前 object_reader.go、SDK FlushWithContext/Unsubscribe/subscribe/ConsumerInfo/getJSContextOpts 及其独立 constructor/bind测试。未找到same-nc reconnect本身绕过barrier：旧socket丢弃或pending flush失败，继承needsBarrier阻止下一批。提出Draining时Unsubscribe可拒绝但PONG仍成功的泛用API疑点，transfer专家核实当前agent/ctl生产都用Close、非Drain；未证明生产可达，不列阻断，未来改Drain应重新审。

交叉发现首修不闭合：root尝试 `JetStream(nats.Context(ctx))` 解决Bind内部ConsumerInfo取消，但SDK v1.52.0 `jsm.go:getJSContextOpts` 不继承 defs.ctx，只继承 wait/pre；subscribe调用ConsumerInfo无opts，因此根本不受新默认ctx取消。亲读 round2-transfer-lifecycle-after.log 三轮仍红，及时同步root/transfer。随后第二修与等价行为回归闭合（见文末），没有把首修日志写成PASS。

## T2-F2 — Major / 修复过程中引入并关闭：地址守卫阻断合法首次命名迁移

来源：主进程在最终风险复核中提出反问，我重新独立读取迁移实现、CLI与运维基线后确认，并增加真实反例。不能把前一轮“首版guard通过”当最终结论。

契约证据：internal/clusteroffline/init.go:46–49 明确 TunnelAddr 由运维提供，:149 只校验 host:port；cmd/tether/cluster.go:839/866–869 直接传独立 --tunnel-addr；docs/cluster.md:130–133、:173 说明它是 agent 应拨入的 public endpoint，没有要求等于每个 agent 已有 fallback 字符串。init.go:302–303 回填 home_broker 不增 epoch。因此当前同协议纪元的 single→cluster 迁移可在 epoch=0 把旧IP fallback命名为新的DNS/控制listener。CLI的历史v1→v2重装警告描述跨纪元边界，不使当前同纪元模式切换变成非法输入。

首版新增 ApplyHome guard（tunnel.go:1741–1747）一律拒绝不同 effective address，把这条合法迁移挡住；影响为 agent 无法应用合法 home 指令，不能切换后续恢复所需的 advertised endpoint。独立 TestApplyHomeAdoptsFallbackAddress 真实建立 epoch0 unpinned tunnel，然后启动同一证书、另一控制地址的服务；首次命名必须不发新REGISTER、不换supervisor，关闭旧listener后必须实际通过新地址恢复原公共端口HTTP payload。原样对首版guard红：round2-fallback-address-before.log，FAIL0.067s，明确拒绝合法首次命名。

建议并已落实的最小修复：只有已经 named 的 slot 在同 epoch 变更地址被拒（tunnel.go:1745）；未 named 的初次命名先通过实际peer证明，保存 desired addr/pins（:1754–1763）。redial 在原有 c.mu+gen 检查中一次性抓取 addr 与 pins 局部快照（:1922–1928），不无锁读 map，不取 successor 配置。旧 transport与流保留；正在进行的旧fallback握手继续由 swap 当前pins复核保护。正常更高epoch rehome仍是新generation。对应架构/三处R13注释由root先更新，避免保留“地址永远仅spawn值”的假描述。

已 named 的不同地址负例仍原样保留，不弱化它。第三条新测试覆盖first naming正例及真实redial目的地；新增函数合计3个，root统一登记inventory。

## 跨模块最终修复复核

transfer第二修：公开 Bind(stream, consumer)+SkipConsumerLookup()+AckNone()，保留订阅ctx。独立核对 SDK subscribe：准确stream/name避免StreamNameBySubject；skipCInfo且consumerBound直接break，不进行额外INFO或Create；AckNone与刚完成Create的权威配置一致。因此去除不可取消的重复查询，不放宽认证或对象scope。新版dual-path oracle允许无需INFO的本地绑定，但旧实现须在实际INFO发布后才取消，亲读修前仍红1.129s，排除了“仅加前置ctx检查即可假绿”。最终constructor/绑定取消两Test三轮race PASS5.112s；完整reader+正常/连续取消真WSS race PASS70.980s，亲读日志 round2-transfer-lifecycle-final.log / round2-transfer-reader-wss-final.log：正常66,549,440B、191RPC零失败、max277.405ms、零断重连；64 reader取消、127RPC零失败、max277.369ms、零断线。测试由transfer lane执行，我没有冒称自行重跑。

system最终修复：亲读 round2-system-identity-final.log PASS6.755s，确认该lane已对S1/S2与explicit ID/unique mapping/真实sole-survivor正控跑三轮race。源码交叉结论见前节，未发现额外阻断。两lane报告均已独立改签，本路仍以真实源码与收据判断。

## 最终候选收据与结论

本路实际执行前述完整定向命令，新增第三条迁移恢复测试后，24 个顶层 Test 各执行 3 次，`-race` PASS 11.274s，日志 `/tmp/tether-control-recovery/round2-tunnel-final.log`。三个新增 Test 位于独占 `internal/tunnel/open_replay_lifecycle_test.go:13/:79/:101`，职责分别为实际peer升级正反例、已named同epoch不同地址拒绝、epoch0首次命名采用不同地址及真实HTTP恢复。

恢复反例有效性负控制：仅通过 Go `-overlay` 将 `redialWithBackoff` 的一次 `dialAndRegister(... addr ...)` 改回 `dialAndRegister(... brokerAddr ...)`；其余生产逻辑未变，工作区生产文件未修改。命令 `go test -race -overlay /tmp/tether-control-recovery/round2-tunnel-address-overlay.json ./internal/tunnel -run '^TestApplyHomeAdoptsFallbackAddress$' -count=1` 按预期 FAIL 8.080s，原公共端口 HTTP 在关闭旧服务后无法恢复（connect refused），日志 `round2-fallback-address-mutant.log`。因此正例不是只检查desired字段的实现镜像，确能检出恢复仍拨旧地址的缺陷。

最终收据之后只补了 production ApplyHome godoc 和本路已named负例的前置注释，不改运行代码/断言。最新文件身份：

- `internal/tunnel/tunnel.go` SHA256 `f7cddc03ae1111c16be6aacf3c295cf38e230b1ee60abaf827a63b8403984738`
- `internal/tunnel/open_replay_lifecycle_test.go` SHA256 `9977128fa269a6bbdc645fe9faf57f1d963c17455da8110f774399d5caee3a73`
- `internal/tunnel/register_and_fence_test.go` SHA256 `11e445122b14406f1f5892ee0f9f54b9b3526bd7742e5697a96f8beb31a7014d`

`git diff --check` 最终通过。transfer/system 两路均对最后首次命名地址例外再次独立源审认可：实际peer证明先于adoption；addr/pins同锁同gen抓取；在途旧fallback仍受swap的当前pins检查。system同时亲读负控制失败日志，transfer亲读24×3正例日志。未发现尚未闭合的隧道/恢复阻断问题。

限制：这是一轮内部审查；不代替root最终固定make gates或独立外审。没有stage/commit/deploy，没有重复跑完整矩阵/lint，没有宣称穷尽调度、广域网或黑洞退出。历史#4真实payload仍未知，不能将当前恢复回归作为历史取证。后续生产运行逻辑若变化，需要按变化重新审查并绑定新收据。
