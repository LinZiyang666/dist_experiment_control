Pass — 第二轮内部传输审查通过；T2-F1 已由根进程修复并经原行为反例与真实 WSS 复验。本结论不代替独立外审或项目最终闸门。

基线：HEAD cab90d1，index 干净；范围为当前未暂存增量与未跟踪文件。身份：内部传输全链路专家，只读生产代码，不 stage/commit/deploy，不创建子代理。起点指纹：/tmp/tether-control-recovery/round2-start-sha256.txt。

## 独立审查面（先落盘）

1. 目标 #1 的真实约束：66,549,440 B 完整传输，标准对象 wire 格式，真实 TLS WSS 默认 max_payload/frame 上限，受限带宽、RPC 延迟/失败计数、连接身份/断连计数；最终摘要正确不足以放行。
2. 元数据与对象内容：bucket/name/NUID、link/deleted、零值/空对象、非法 SHA 算法、chunks/size 不一致、缺 chunk/多 chunk、乱序/重复、异步修改或删除对象、旧对象 chunk 大于降低后的 MaxPayload、metadata 缺失或谎报 ChunkSize。
3. 拉取约束：SDK legacy Fetch 的实际 request batch+max_bytes 编码；服务端的 max_bytes 是否含协议 overhead、首条 oversize 行为；真实每批实际消费情况、短批/状态消息/超时语义；整数溢出/异常 MaxPayload；旧版本 SDK/服务端兼容及权限。
4. 共享连接窗口：同 reader 串行 Read、不同 reader/重复 commit 并发、空对象和 constructor 失败引用释放、最后 reader Close 与新 constructor 交错、map key 生命周期、全连接对象总字节量与控制流量竞争。
5. 取消及 drain barrier：等 gate、Fetch 前/中/后、batch 部分到达、校验失败、Read 后 close、重复/并发 Close；UNSUB 与 PING 顺序、Flush 失败/迟到 PONG、NATS 重连/旧 server 数据及 routed delivery；失败 barrier 是否被清除/绕过。
6. 清理与预算：Create 成功但 reply 丢失、Bind 错误/ctx timeout、临界区阻塞、Close 的 2+2 秒开销、inactive 与整次 budget 一致、stream/consumer 已删、断网错误、per-reader memory、goroutine/fd，5 轮生命周期泄漏断言质量。
7. agent 接线：push prepare cache/重复 commit 身份、nc 与 js 实际归属、run/session context 生命周期、Stop/rebuild/rename/kick/cancellation fence、异步任务退出、写入错误不发 complete。
8. 文件原子性：临时文件打开/上限/校验/Sync/Close/rename，ctx 取消前后 boundary，force/no-force 与并发相同路径，不合法对象不落盘，准备时 size/hash 与最终内容绑定，临时文件/目录 fd 清理。
9. ctl pull：合法 bucket/key、session/nkey ACL、原始请求 expected size/hash、cancel/finalize 顺序、错误分类、Close 前后终态、timeout 与 producer/broker budget 相容。
10. broker/ACL：新 pull consumer/Create/INFO/NEXT/DELETE subjects 权限在真 auth_callout 下，拒绝跨 session；tracker home 路由与事件收敛，重复 commit/terminal 去重，生命周期（session rm/kick/rehome）不会跨身份写入。
11. 回归边界：publish 仍标准 ObjectStore.Put，tier A 与 N-1 混配可用；整个对象 reader 更改对非传输读取没有扩散；无新控制面 blocking handler。
12. 证据可信性：覆盖以断言行为为准，先读实现独立推导再读首轮报告；必要疑点通过定向反例，不借用整体 gates 全绿替代审查；无现网 canary、组件与端到端证据分别标识。

## 状态

- 已读：CLAUDE.md、testing-standards.md 的相关规范、requirements.md 的传输/韧性/性能/N-1、当前架构 §9/§20.2/控制连接补充、当前定稿 plan。
- 尚未读取首轮报告。
- 待完成：实现深查、必要定向反例、首轮主张复核、与其他专家交叉质疑、最终结论。

## T2-F1 — Medium — 新引入的 consumer Bind INFO 请求不受 transfer ctx 取消（已证，已修复并独立闭合）

起点位置：`internal/jsstream/object_reader.go:82–85`；固定依赖 nats.go v1.52.0 `js.go:1813–1814`、`jsm.go:634–648` 与 `:1769–1797`。

`PullSubscribe(..., nats.Context(ctx))` 把 ctx 放在 subscription options；它在真实绑定前调用 `js.ConsumerInfo(stream, consumer)`，没有把这份 ctx 传给查询。查询的 `getJSContextOpts` 只从 JS defaults 继承 wait/pre，甚至不继承 defaults.ctx。因此 transfer deadline/cancel 恰好在绑定 INFO 已发、响应未到时，无法中断这条请求，要额外等待 legacy 默认 5 秒才继续清理。影响是构造与清理不遵守本次 transfer 的取消/预算，不是已证控制 WSS 再次超帧，也不声称会错误提交文件。

独立反例 `TestTransferObjectCancellationInterruptsConsumerBinding`（独占新增 `internal/jsstream/object_reader_lifecycle_test.go`）：真实 NATS + 真实 consumer Create，创建回复后在 TCP transport 扣留入站，另一真实连接观测 Consumer INFO 确已发布，再取消 ctx；一秒内仍未发出 Consumer DELETE。原候选 `-race` FAIL 1.119s（`round2-transfer-bind-before.log`）。恢复网络后 constructor 返回，Read 确实取消、Close 最终回收，故断言不把暂时停顿冒报永久泄漏。

第一修复尝试是给 `nc.JetStream(nats.Context(ctx))` 传 ctx，静态分析起初误以为 SDK 会继承 defaults.ctx；原样反例连续三轮仍红（`round2-transfer-lifecycle-after.log`，FAIL 7.351s）。传输/隧道两路进一步共同核对 `getJSContextOpts` 后明确这个 SDK 细节，未把首次修改算闭合，也未改测试制造绿。

建议第二修复：使用公开 `nats.Bind` + `nats.SkipConsumerLookup`，直接绑定已由相同 reader 的真实 `CreateConsumer` 创建并已得到 CachedInfo 的 consumer，明确 `nats.AckNone()`；不允许 SDK 另建 consumer。`js.go:1835` skipCInfo 分支在 consumerBound 时 break，证明这个组合不会 upsert/覆盖 consumer。继续保留 subscription/FETCH ctx、真实 Fetch count/max_bytes 与取消 drain barrier。

为使同一行为测试同时接受“可取消 INFO”与“无需 INFO 的 Bind”，反例增强为两个公开可观测分支：constructor 若先返回则取消其 Read/Close；否则一定等到真实 INFO 已发后才 cancel。这保留对“只在 Bind 前加 ctx.Err 检查”的反例判别力。增强后的测试在尚无 Skip 的生产代码上仍 `-race` FAIL 1.129s（`round2-transfer-bind-dual-path-before.log`）；中间的创建返回前取消版也曾 FAIL 1.121s（`round2-transfer-bind-create-barrier-before.log`），但最终没有只依赖这条较弱调度。

## 其他独立处置

1. Fetch 真正双限：逐行核对固定 SDK `Subscription.Fetch` 的 nextRequest 编码，batch=4 与 PullMaxBytes 同时上 wire；SDK 只在无任何用户消息且 no-wait 返回404/408时补等候请求，不会在已收部分数据后加第二批。SDK 会吞掉 partial batch 后面的错误，helper 的 len(batch)!=wanted→UnexpectedEOF 补足，且维持 held permit 等 Close 排空。
2. 元数据/内容：bucket/name/deleted/link/NUID、SHA-256 digest语法、空对象、chunks/size、consumer连续序号、stream递增序号、NumPending与剩余chunks一致，均有实现检查和真实对象反例。对象被并发改写/删块只允许错误结束，没有自动续读保证。旧ChunkSize超限正常metadata明确拒绝，missing/伪小metadata仍受实际request byte cap控制。未新增对象wire或权限域。
3. 共享窗口与取消：map由nc指针索引、refs从constructor入口保留；needsBarrier由gate保护，channel释放/取得形成跨reader同步。完整Fetch批次的数据确实全部已到本机，全部消费后才放permit；partial/error/取消在Close先UNSUB再同nc PONG，失败保留needsBarrier。最后引用归零后的新reader仍须完成真实GetInfo（GetLastMsg）与Create往返，发生在旧UNSUB入队后；因此无需依赖随后可被跳过的Bind INFO往返。构造已开始的reader必须继承旧window，这是入口retain的意义。
4. 生命周期新增验证：`TestTransferObjectConstructorCancellationReleasesResources` 每个卡点六轮：真实metadata回复后暂停、第一位持批次reader Close，再取消constructor；另一臂真实Create成功后cancel，SDK仍可能合法返回已取消reader，首次Read必须零字节取消。每轮consumer归零、window无残留，下一reader完整读取正确内容；最终subscriptions回基线、仓库goroutine/fd门。修Bind前此测试已经 `-race` PASS 2.349s（`round2-transfer-constructor-race.log`）。该测试补 constructor 取消/最后Close交错，不冒称证明任意调度。
5. 测试误判保留：初次资源测试曾强制“Create成功后cancel，constructor必须报错”，SDK合法返回已取消reader使其红。这个强要求不是接口安全契约，改为“Read不能给字节，Close必须释放”。原红日志保留 `round2-transfer-constructor-initial-oracle.log`，不登记为产品finding。
6. 预算与资源：inactive至少5min且覆盖deadline+1min；生产两调用点都有总budget。正常Close主动删consumer，断网/服务端已接收Create但回复丢失时只能靠inactive回收，属于可观测受限清理边界；Close忽略上层defer错误是现有best-effort形状。Read/Close互斥、Close先cancel能唤醒Fetch或等gate者，重复Close不重复减ref。
7. agent/ctl落盘：两处均保留prepare给出的独立expectedSize/hash、expectedSize+1硬读取上限、tmp→Sync→Close→验证→ctx.Err→原子commit，失败有tmp/目录fd回收。force/no-force沿用既有pinned-parent或link/rename实现。ctx检查不可能追溯撤销随后同时发生的rename；报告不承诺该性质。重复commit在broker仍可重新forward，但共用gate不会叠加批次；同一tid相同内容重复写入仍沿用既有语义，非本次引入去重机制。
8. 会话/身份代：新reader基于收到commit的nc创建JS，后续不取可变a.js。与system lane交叉核对sessionFinalizer：新runCtx发布前旧nc已Close；因此旧handler如果迟到才读新runCtx，其旧nc的GetInfo/Create不能获取successor的数据；如果已经取得数据，其ctx在旧session下且rename前会检查取消。没有证实跨代成功写入。原a.js!=nil入口guard仍会碰可变字段，root已删这条push-commit冗余依赖；其他pull既有a.js链路不在此补强的范围。
9. broker/ACL/N-1：实际权限模板已提供sid范围内CREATE/INFO/NEXT/DELETE/STREAM.INFO/MSG.GET，新代码无需扩权；完整同sidauth_callout推拉和跨sid拒绝测试仍适用。broker的home/tracker与finalize绑定不变；相邻release对象格式不变，但旧receiver仍用旧Get，不能声称未升级agent自动修好。
10. WSS证据：亲读正式测试实现，确实使用TLS校验证书、默认1MiB MaxPayload、66,549,440字节、实际socket限速、同nc上RPC、disconnect/reconnect计数与CID；摘要不是唯一判据。64取消reader预先构造，通过独立TCP观察server投递后取消，避免constructor往返掩盖积压。组件实验不覆盖broker/Caddy/现网WAN；pending采样不作为严格上限。待第二修复影响legacy配置后复跑有关正例。
11. 跨lane Drain疑问：SDK Unsubscribe在nc.Draining时会返回ErrConnectionDraining，随后单独Flush不能证明实际UNSUB已排；但目前agent finalizer使用nc.Close，ctl也defer nc.Close，两生产调用链不进入Drain。泛用helper未来若改接Drain应重新审查此分支；本增量未建立可达反例，不列确认缺陷。

## 固定交叉质疑

- system S1：已读报告和当前源码，boot只镜像VerifyAt通过cache，derive与silence均检查expiry，保留durable pin/generation；已确认同route身份可跨后来roster成员删除保留，这是retired→sole survivor所需。未发现影响传输window/cancel的新路径。
- system S2：当前uniqueRosterNodeAtHost对不同NodeID共享host拒绝猜测，显式ConnectedBrokerID/同route已确认proof优先，修法符合保守路由。另提出same URL/custom server name复用到全新brokerID的跨重建边界：生产默认server_name=node_id，未证实允许该在线身份替换；当前字段不是volatile server instance fence，保留说明而非扩成猜测finding。
- tunnel T2-F1：亲读ApplyHome同epoch修法，共用verifySessionPeer只对未验证transport校验真实peer，已验证连接仍允许正常pin rotation；不同effective addr先拒绝，成功后写named identity使空pin不能降级。c.brokerAddr仅构造时设定，没有setter，fallback显式命名后supervisor捕获空串仍指同一有效地址，未发现恢复目标分叉。epoch0由from-existing回填导致真实可达，报告应保留。
- 其他两lane独立质疑并确认T2-F1第一次修法无效，共同定位SDK忽略defs.ctx；第二修复后的收据才可闭合。


## 最终复验与结论

根进程第二次修复采用公开 `Bind + SkipConsumerLookup + AckNone + subscription Context`，移除没有效果的 JS defaults Context 配置。当前实现 `object_reader.go:82–89` 不再执行不受 ctx 控制的冗余 ConsumerInfo；数据读取依旧使用真实 Fetch count/bytes限制。T2-F1 已闭合，无剩余已确认阻断 finding。

独立命令/收据：

- `go test -race ./internal/jsstream -run '^TestTransferObject(ConstructorCancellationReleasesResources|CancellationInterruptsConsumerBinding)$' -count=3 -timeout=60s`：PASS 5.112s，`/tmp/tether-control-recovery/round2-transfer-lifecycle-final.log`。同一双路径回归在无 Skip 时红，在第二修后连续三轮绿。
- `go test -race -v ./internal/jsstream -run '^TestTransferObject' -count=1 -timeout=4m`：全组 PASS 70.980s，`/tmp/tether-control-recovery/round2-transfer-reader-wss-final.log`。
- 正常真实 WSS：66,549,440 B，读取38.265776471s，191次RPC零失败，最大277.404671ms，断线0/重连0，长度/SHA/CID断言均通过。
- 64次取消真实 WSS：16.307263422s，127次RPC零失败，最大277.369139ms，断线0，CID/consumer清理断言通过；sampled pending最大393,794B仅作为诊断样本。
- 全组还包含共享reader总窗口、等候Close、不同连接、失败PONG继承、历史oversized实际bytes、坏metadata/缺尾/额外块、空对象及原资源门；未把部分子测试通过当整体通过。
- `git diff --check -- internal/jsstream/object_reader_lifecycle_test.go`：通过。此专家未执行全矩阵/lint，没有stage/commit/deploy或修改生产。

最终传输审查文件指纹：

```
cd8ea793418c2bbf636fc95a97e7c434671aef7738ad81e3fb3ad90afd6241ee  internal/jsstream/object_reader.go
7487b223a85c2147485411273f203e2f467efb0e30a000012c9fdeb6b5ead762  internal/jsstream/object_reader_lifecycle_test.go
ebd84925aed9c8b538a10ae6dc48055a082f8cdc70c1febe2a67331a8d770090  internal/agent/transfer.go
cfaf6916c856d190375914ff426ac727ca1b41e7df97229b3bb039b5ecc5fe7f  cmd/tether/transfer.go
```

新增独占测试函数清单已交 root 登记：`TestTransferObjectConstructorCancellationReleasesResources`、`TestTransferObjectCancellationInterruptsConsumerBinding`。初次源码/测试失败、第一修无效与每次修后的实际结果全部保留，没有以首轮内审结论代替本轮证据。

### 独立审查面最终处置

| 初始项 | 处置 |
| --- | --- |
| 1 真实WSS/控制延迟 | 正常原大小和64取消真实TLS WSS两测试独立重跑，摘要/CID/RPC/断重连同时通过。未做现网canary。 |
| 2 元数据/内容 | 源码逐项核验，全部对应真实对象reader反例复跑通过；并发修改不保证成功，允许明确失败。 |
| 3 Fetch实际bytes | SDK与server源码均核对，真实历史bytes三臂通过；服务端预算含数据/header/原subject/ack reply，不含所有transport/framing，5×相对8×留余量。 |
| 4 共享窗口/构造 | 源码锁/引用证明，加正常共享20 reader、等待者close、构造取消/last-close资源边界真实回归。未证明所有任意调度。 |
| 5 取消/barrier | blocked Fetch、failed PONG继承、64连续取消WSS通过；新增T2-F1在真实Bind INFO卡点证实并闭合。 |
| 6 清理/预算 | constructor六循环、现有资源门与consumer inactive检验通过；server接收Create但回复丢失的残留靠inactive限期回收，未注入该丢包路径。 |
| 7 agent lifecycle | captured nc调用链、finalizer先close后新ctx源码/跨lane确认；删冗余a.js nil读；未新增真实跨代落盘测试，不声称严格接收代快照。 |
| 8 原子落盘 | 双重length/hash、大小限制、Sync/Close/ctx/commit顺序及现有force/no-force路径静态核查。未重跑所有文件系统故障注入。 |
| 9 ctl pull | 接线/期望值/ctx/finalize与临时文件清理源码核查；本轮root全项目门负责CLI完整回归。 |
| 10 broker/ACL | tracker home/重复commit与当前sid permissions源码核查；新Skip少用一个已授权INFO，不加权限。未独立重跑完整auth矩阵。 |
| 11 兼容/控制面 | 对象wire/Put无变，target接收方共享nc有界读；没有发布版N-1混部矩阵或旧receiver自动修复承诺。 |
| 12 证据可信性 | 初始审查面先落盘后看首轮报告；三路交叉质疑；本轮初红/无效首修/最终正反例收据全部留档。 |

独立外审仍由用户或其指定独立审查者执行，根进程负责最终测试门和交付文档；本内审Pass不授权提交、暂存或部署。

## 最终小增量交叉复核：合法首次命名的地址迁移

根进程在本路完成后补充了合法迁移反例。此前本报告“同epoch不同effective addr先拒绝”的交叉评价仅针对第一版补丁；它过宽，不能作为最终候选的契约。已重新只读核查 `internal/tunnel/tunnel.go:1741–1764`、`:1919–1930`、`:1956–1974` 与 `TestApplyHomeAdoptsFallbackAddress`，同意当前最小例外，维持 Pass。

依据与组合语义：

1. `InitFromExisting` 的 TunnelAddr 是独立运维输入，并非必须等于旧fallback地址；它只回填已有allocation的home身份而不增加旧epoch。首次同epoch命名可以合法把旧IP/端口表述替换成新的DNS/控制endpoint。
2. 当前guard仅在 `sess.brokerAddr` 已非空时拒绝同epoch不同地址；未命名slot首次采用非空地址仍要求非空pins，并在写desired addr/pins前执行 `verifySessionPeer`。因此没有恢复首轮“任意unpinned peer可被标成已验证home”的缺陷。
3. 原supervisor每次redial在同一个 `c.mu` 和 `cur.gen == gen` 判断内，同时快照当前addr与pins，不会组合新addr和旧pins；不重建supervisor、不丢已有转发。被Close/替换的generation仍由原ctx取消与swap的gen检查阻止安装。
4. 首次命名若越过一个已在途的unpinned旧地址握手，`swapTransport`仍用当前pins检查该握手的真实TLS peer；不匹配则关闭结果并由同一supervisor下一轮读到新addr/pins。匹配则可以保留同一已授权broker的已有transport，后续redial采用保存的新地址。这与首次命名保留活流的约束一致，不声称地址更新会强制立即切断旧transport。
5. 已验证transport的正常pin rotation仍沿用既定行为；这个delta没有改变其信任宽限或要求所有已有连接立即改证书。

证据边界：本路没有重复运行测试，亲读 `round2-tunnel-final.log` 的24项测试三轮race全通过（11.274s）及新增测试源码。该测试真实使用同证书、不同控制endpoint，首次命名时检验无新REGISTER/无supervisor替换及原payload可用，再关闭旧服务并检验新endpoint恢复真实payload；没有把单纯desired字段更新当完成。旧addr负控制由隧道专家负责收尾，未在此提前冒报其结果。

本次复核的隧道生产指纹：`8a38181a551a53a47c5e9ad547d0b4dc91effab7e237313a4fd722ece33a71a4  internal/tunnel/tunnel.go`。传输生产与本路测试未变，本路没有新增执行命令、生产修改或暂存。
