Pass

# proxy-lifecycle 独立外部审查报告

> 日期：2026-08-29
>
> 身份与边界：本报告是对当前暂存区外增量的独立外审。既有 plan 和内审报告只用于建立待证伪假说，未把其中的采纳、驳回或绿色结果当作证据。外审未修改生产实现，只增加了三项可重复的反例测试、本 tasklist 和本报告。
>
> **当前结论以文末“独立外部复审”覆盖首轮结论。** 首轮 Fail 与 findings 保留为审计轨迹，不代表修复后的当前状态。

## 首轮结论（历史，已被复审取代）

当前增量不能上线。原始事故的直接根因——把 SS server 锚在 per-session `runCtx` 上——确实已被移除，cluster HA drill 也在本次构建上通过；但自愈和 teardown 的新契约仍有四个 Major 缺口：

1. `Stop` 不能取消已经进入的 DNS 解析，且在 agent 中持 `proxyRuntime.mu` 等待；
2. hard revoke 只关闭 subscriber 半边，silent upstream 会永久保留两条连接和 handler；
3. corpse rebuild 复用持久化的本地端口，一次 `EADDRINUSE` 就能让自愈永久变暗；
4. single-broker 在「READY=true、序对相等」时明确不下发 keyset，因此非 session-rebuild 原因造成的 accept-loop corpse 没有任何触发 `reapProxyCorpseLocked` 的边。内审对这条路径的驳回建立在与 broker 源码相反的前提上。

三项独立反例均稳定失败；因此即使 `make gates`、完整 E2E 和 simcluster drill 为绿，总体 verdict 仍是 **Fail**。

## 审查范围与契约

- 外审介入前的候选面共 25 个未暂存/未跟踪路径：20 个 tracked 修改和 5 个 untracked 文件；第 26 个路径是外审先行建立的 tasklist。生产改动集中于 `internal/agent/proxy.go`、`internal/agent/ssproxy/server.go` 与 `internal/agent/agent.go`；其余为测试、计划/审查文档、运维 gotcha、设备清单及一个备份文件。
- 权威链按 `CLAUDE.md` 执行：requirements 定义 WHAT，`distributed-broker-architecture.md` 与 `deploy-tier-gotchas.md` 定义当前 HOW，历史 architecture 只作理由来源。关键不变量是数据面不得随控制面 session 重建退出、READY 必须诚实、OFF/fail-closed/agent-exit 必须 fail closed、单节点故障不得无故轮换公网端口、teardown 不得无限阻塞 heartbeat/rehome。
- 当前增量没有修改 proto/wire、broker、CLI、`go.mod` 或 `go.sum`；N-1 的 wire 四象限没有新增协议表面。`ssproxy.Start` 是 agent 进程内 API，签名变化随同一二进制发布。
- `docs/devices.md` 中 GPU/NAT 等修改与 proxy-lifecycle 无关，但仍在用户指定的“全部暂存区外内容”审查面内。`docs/devices.md.bak.20260820` 与 `HEAD:docs/devices.md` 的 SHA-256 完全相同，属于未跟踪备份工件，不是有效产品变更。

## Findings

### F1 — Major — `Stop` 无法取消 in-flight DNS，持 proxy 锁期间可阻断 heartbeat 与 rehome

`handleConn` 在 upstream 被纳入 `allConns` 之前，先调用 `destAllowed` 和 `dialTarget`（`internal/agent/ssproxy/server.go:605-635`）。其中 `destAllowed` 使用无调用方 context/deadline 的 `net.LookupIP`（`:216-239`）；`shutdown` 只能关闭 listener 与已经登记的连接（`:421-434`），所以它无法触及正在解析的 handler。`Stop` 随后无条件 `wg.Wait()`（`:405-410`）。agent 又在持有 `p.mu` 的 `proxyTeardownLocked` 中调用这个 `Stop`（`internal/agent/proxy.go:742-754`）。

独立测试 `TestStopCancelsInFlightDestinationResolution` 用一个确实进入 resolver、并服从其 context 的解析器构造阻塞。500 ms 后 `Stop` 仍未返回；释放测试 resolver 后它才结束。该测试不是靠睡眠猜路径：`entered` 证明查询已进入，fixture 也在失败后显式释放，避免把测试进程本身挂死。

影响：一个订阅者只需请求域名目标，就可能让 OFF、rebuild 或 agent-exit 在 `p.mu` 下等待系统 resolver；期间 heartbeat 的 generation/epoch/ProxyBound 读取也拿不到同一把锁。内审已实测另一个 pre-track 窗口中的拨号可耗时 9.547s，而 DNS 受系统 resolver 策略控制、没有这里的 10s `Dialer.Timeout` 保证。这违反“有界 teardown”，并可能跨过 3 次 heartbeat/rehome dwell，扩大为公网端口轮换。

建议：由 `Server` 自己创建并拥有 shutdown context/cancel，在 `shutdown` 中 cancel；用 `Resolver.LookupIPAddr(ctx, ...)` 和 `Dialer.DialContext(ctx, ...)` 取消每个 operation。这个 context 不能由 agent/session 传入，因此不会重引入原事故。不要用 package-wide context 禁令阻止 operation-scoped cancellation。

### F2 — Major — hard revoke 没有回收 revoked key 的 upstream 半边

`keyConns` 只在 `bindKeyConn(chosenID, c)` 中登记 accepted/subscriber 连接（`internal/agent/ssproxy/server.go:492-516,583-585`）。upstream 虽随后进入 `allConns`，却没有进入该 key 的集合（`:613-635`）。`SetKeys` 撤销 key 时只遍历 `keyConns` 关闭 subscriber 半边（`:365-395`）。如果 upstream 不写数据也不响应 half-close，relay 的 remote-to-client copy 会继续阻塞在 `remote.Read`；handler 和 accepted/upstream 两个 map entry 都要等整个 server `Stop` 才消失。

独立测试 `TestRevocationReclaimsSilentUpstreamConnections` 先等待 upstream Accept，再读取 server response salt；后者只会在 remote 已 track、relay 即将开始后写出，因此 fixture 不会误落到 pre-relay 清理路径。撤销 key 500 ms 后仍精确观察到 2 条 tracked connection。

影响：数据字节层面虽因 subscriber socket 被关闭而 fail closed，但“force-close in-flight connection”的资源契约不成立。已授权 key 可预先建立大量 silent upstream，再被撤销；连接、goroutine 和 fd 会一直留到整个 proxy server 重建/关闭，形成可放大的资源耗尽路径。

建议：把 accepted 与 upstream 作为同一 key-owned session 管理，或在 remote 建立后也通过带撤销竞态检查的 `bindKeyConn` 登记；撤销必须原子地关闭两半。补充精确的 fd/goroutine 终态断言，而不只检查 subscriber 收到 EOF。

### F3 — Major — persisted `LocalPort` 冲突可把 corpse 自愈变成永久暗循环

corpse 被收割后，`bootstrapProxyFromFootprintLocked` 原样把持久化的 `ps.LocalPort` 交给 `proxyStartLocked`（`internal/agent/proxy.go:438-475`），后者只尝试一次 `srv.Start(wantLocal, keys)`（`:573-585`）。失败清理把运行时清空并 ACK unready，但保留 footprint；下次相同 keyset 仍使用同一个被占端口，并受 dial failure backoff 逐步延迟。这个本地端口只是 SS listener 与 tunnel adapter 之间的实现细节，公网 `PublicPort`、token 与 home 都无需变化。

独立测试 `TestCorpseRebuildSurvivesPersistedLocalPortCollision` 停止旧 server、真实占用其 local port，再发送同序对 keyset。当前实现稳定保持不 serving；预期是保留 public port 14000，并选择新的 OS free local port。

影响：新自愈路径在常见的端口竞争条件下仍永久变暗，恢复依赖占用者退出或 operator `proxy off/on`；后者正是本增量要避免的全 session 端口重排。

建议：仅在 footprint bootstrap 且错误明确为 address-in-use 时，使用新的 `Server`/`Start(0, ...)` 重试一次，并以新 local port 更新 footprint 与 `AddProxy`；其它 listen 错误仍 fail closed，不应被泛化掩盖。

### F4 — Major — single-broker 的非重连 corpse 没有自愈触发边，且会持续假 READY

`Serving` 新增 `acceptExited` 是正确的：accept loop 遇到任意 Accept error 都直接返回并置闩（`internal/agent/ssproxy/server.go:321-340,437-465`）。但 corpse 只在 `applyProxyDirective` 入口被收割。对不伴随 NATS session rebuild 的 accept-loop 退出，例如 fd exhaustion 后的非临时 Accept error，不会自然产生 register reply。

single-broker heartbeat 也不能补上这条边：

- `HeartbeatPayload.ProxyBound` 只在 cluster mode 写本地 `proxy_ready`；single mode明确忽略该字段并进入 `repairProxy`（`internal/broker/broker.go:2442-2463`）。
- single broker 的 DB 此时仍是 `ready=true`，agent 报告的 generation/epoch 仍是已应用序对；`repairProxy` 在这个组合上立即 return（`internal/broker/proxy.go:712-718`），不会推送 keyset。
- 即使某条路径先把 ready 改成 false，同序对的 `!ready` 也被明确抑制（`:748-763`），仍不会推送。

因此 `internal/agent/proxy.go:194-203` 关于“broker 仍认为 ready=true，所以 guard 不触发、keyset push arrives”的注释，以及内审报告对相同 finding 的驳回，均把“不触发 suppress guard”错误等价成了“必然 push”；实际源码在更早的 healthy/exact return 就已经退出。`reapProxyCorpseLocked` 的“whatever kills the server, the runtime converges”注释（`:430-455`）也超出实现可保证范围。

影响：原始 runCtx/session-rebuild 事故因为新 session 的 register reply 确实能触发 directive，直接路径已修；但增量声称的 cause-independent self-heal 和 single-mode 诚实 READY 并未闭合。一个失去 accept loop 的 single 节点可无限期保持 DB `proxy_ready=true` 且不再收到修复 directive。

建议：增加 agent-owned 的 accept-loop exit 通知/恢复状态机，或让 heartbeat 路径在本地发现 `!Serving` 时安全调度 reap/rebuild；不要简单地让 broker 对所有 `ProxyBound=false` 推 keyset，因为 tunnel-only outage 与 authoritative terminal 状态已有防 flapping 语义。需要一项 single-broker 端到端测试：不重建 NATS session，只杀 accept loop，随后必须先 UNREADY、再自动恢复且公网端口不变。

### F5 — Minor — 架构门把“不能接收 session lifetime”误写成“整个包不能使用 context”

`TestSSProxyPackageTakesNoContext` 只扫描 `ssproxy` 顶层非测试 `.go` 文件，并禁止任何 `context` import（`test/architecture/dataplane_lifetime_test.go:30-80`）；注释还明确要求用 `time.Duration`/`net.Dialer` 代替 cancellation。原事故的不变量是 server lifetime 不能由调用方 session context 所有，而不是数据面 operation 永远不能取消。F1 正说明内部、server-owned cancellation 是正确缺失机制。

此外，顶层 `os.ReadDir` 会让未来子包逃逸。建议保留对 `applyProxyDirective`、`proxyStartLocked` 和 `Server.Start` 参数/调用图的窄门，允许 ssproxy 内部持有自己的 cancel；若仍做 package scan，应递归并对目标语义而非 import 名称断言。

### F6 — Minor — 生产包导出了纯测试控制面

`CloseListenerForTest` 是 production `.go` 中的 exported API，注释承认 production 永不调用（`internal/agent/ssproxy/server.go:343-356`）。唯一使用点是同仓测试。它扩大了包表面，也允许生产调用者制造 `closed=false && acceptExited=true` 的异常态。

建议：测试移入 `ssproxy` 包后使用 unexported helper，或通过可注入 listener/accept seam 构造错误；不要在生产 API 中保留 `ForTest` 方法。

### F7 — Minor — 上线文档结论过早，且包含应移除的备份工件

`docs/deploy-tier-gotchas.md:834-867` 把 #80 标为“已修”并称 Stop 无界次生缺陷已“一并修复”。这对原始 runCtx 锚和 established upstream 的 Stop 路径成立，但对 F1 的 pre-track DNS/dial、F3 的 rebuild collision、F4 的 generalized corpse 不成立。应把结论收窄为“原始 session-context 根因已修，外审剩余项开放”，在三项红测试与 single-mode recovery 闭合后再 flip。

`docs/devices.md.bak.20260820` 大小 19,895 bytes，与 `HEAD:docs/devices.md` SHA-256 均为 `fca59ec2066c92b9f96c7ba9a1e6adcbf4ed917f721900396595d8ceea9c5cf2`。它没有审计价值，且会因用户要求的 `git add -A` 被暂存。提交前应从工作树/index 删除，并视需要把编辑器/日期备份模式加入 ignore。

## 独立验证结果

| 验证 | 结果 | 说明 |
|---|---|---|
| focused F1/F2 | **FAIL（预期反例）** | `go test ./internal/agent/ssproxy -run 'TestStopCancelsInFlightDestinationResolution|TestRevocationReclaimsSilentUpstreamConnections' -count=1 -v`；两项均红 |
| focused F3 | **FAIL（预期反例）** | `go test ./internal/agent -run TestCorpseRebuildSurvivesPersistedLocalPortCollision -count=1 -v` |
| affected race | **FAIL（仅三项反例）** | `go test -race ./internal/agent/... -count=1`；没有 race detector 报告，其余 affected tests 通过 |
| `make gates` | **PASS** | all-tags vet、darwin cluster build、architecture/determinism/cmd/auth/concurrency/proto、pinned lint（0 issues）及 gofmt 全通过；首轮只因外审测试未 gofmt 失败，修正后完整重跑通过 |
| `make test` | **FAIL（仅三项反例）** | 全仓其余包通过，包括 `internal/broker` 334.040s；失败不是 setup error |
| `make e2e-parallel` | **PASS** | coverage self-check 15/15，scheduled/reported 99/99，`ALL PASS`，wall clock 3m56.638s |
| `make build` | **PASS** | `CGO_ENABLED=0` 完成；Go 尝试写只读 module stat cache打印 warning，但命令 exit 0、产物成功生成在 ignored `bin/` |
| `git diff --check` | **PASS** | 报告/任务单完成后又重跑一次，见交付节 |

所有 Go 命令使用 `GOCACHE=/tmp/tether-review-gocache`；需要本地 listener 的测试在允许访问 host network namespace 的环境运行。没有把沙箱 socket 拒绝当成产品失败。

## simcluster

- 已阅读 `test/simcluster/README.md` 的 Mandate 和本地设备/运维信息。持久实例 `sim` 的只读状态为 `{"error":"no leader in instance sim"}`；它不是本次隔离 drill 的 fixture，未据此给产品 verdict。
- 先执行 `./local.sh --build build`，确保 vendor/tether 与镜像来自当前源码；随后运行 `./local.sh drill 73-proxy-cluster-ha`。
- 结果：`DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=46`，direct poll wait 24s。杀死 agt1 的 home broker 后，控制面约 19s rehome+ready，数据面约 22s 自动恢复；同时覆盖 non-tunnel home、revoke 与 quorum freeze。
- 清理核验：`sim.instance=drill-73-proxy-cluster-ha` 下容器、network、volume 均为空，无遗留。
- 这一次 GREEN 只说明 #33 的该次 cluster crash 形状未复现 stranded data plane，支持但不能证明 #80 是 #33 的唯一根因；文档继续标 CANDIDATE 是正确的。

## NOT-COVERED、疑惑与残余风险

- simcluster 没有 DNS resolver stall、local-port theft 或 accept-loop error 注入 seam，因此 F1/F3/F4 未由 drill 覆盖；F1/F3 已有确定性本地红测试，F4 由 single-broker 分支源码和既有 broker“exact pair 不 push”测试共同证明。建议 owner 为 agent/proxy lifecycle，并补专门 fault seam，而不是用 fake harness 声称通过。
- plan §6.2/§6.3 许诺同一次恢复中恰好一次 UNREADY 再一次 READY，但现有 agent 测试均传 `nil` NATS connection，没有捕获 publish sequence。当前 cluster drill 只观察最终态。该时序仍需可观察的发布 fixture 与重复/乱序断言。
- 疑问 1：`LocalPort` 是否存在未写入权威文档的必须稳定契约？从当前 `AddProxy(PublicPort, LocalPort, Token)` 结构看，它是可更新的内部目标；若 adapter 有隐藏约束，需要先补契约再决定 F3 的 fallback 位置。
- 疑问 2：“hard revoke”是否只要求 subscriber 不再传输数据，还是要求释放该 key 的全部资源？源码注释和既有测试使用“force-close in-flight”，本报告按后者裁定；即便产品选择前者，当前 silent-upstream fd/goroutine 可被放大，仍需资源上界。
- 疑问 3：accept loop 对所有错误立即永久退出是否本来就是策略？若是，则必须有本地恢复/进程失败策略；若不是，应对可恢复错误退避重试并对永久错误 fail visibly。当前“退出但进程和 READY 继续”不是合法第三种状态。

## 上线前必须完成

1. 修复 F1-F4，使三项外审测试转绿，并补 F4 single-broker 无重连恢复测试与 READY 序列断言。
2. 收窄 architecture context gate，去掉或内化 `CloseListenerForTest`。
3. 订正文档 #80 的 flip/有界 Stop 口径，移除 `docs/devices.md.bak.20260820`。
4. 重跑 `go test -race ./internal/agent/... -count=1`、`make gates`、`make test`、`make e2e-parallel`；若生产修复改变 deploy 路径，再重跑 drill 73。

## 交付状态

tasklist 全部执行；最终 `git diff --check` 通过。按用户明确要求，本报告、三项外审测试、tasklist、既有修改以及已指出不应提交的 `.bak` 文件均由 `git add -A` 加入暂存区。暂存不代表审查通过；本报告首行 verdict 仍为 **Fail**。

---

# 主进程回复（step 6 处置）

> 全部 7 条 **采纳并已修**，无驳回。每条修复各带一个变异验证（注入该修复要防的缺陷、确认对应测试变红）。
> 外审新增的三条反例测试全部由修复转绿，未被改写或放宽。

## F1 — 采纳并已修

`Server` 现在**自己创建并拥有** `stopCtx`/`stopCancel`（`Start` 中 `context.WithCancel(context.Background())`，标 `ctx-root`），
`shutdown()` **在关闭 socket 之前**先 cancel；`destAllowed`/`dialTarget` 改用
`net.DefaultResolver.LookupIPAddr(ctx,…)` 与 `Dialer.DialContext(ctx,…)`，两段 pre-track 阻塞由同一个 opCtx 覆盖。
`TestStopCancelsInFlightDestinationResolution` 由红转绿。变异：让 `shutdown` 不再 cancel → 该测试重新变红。

**这条指出的是我的一个判断错误，值得写下来**：我在 plan §3 把「SS server 不接受任何 ctx」当成了核心设计决策，理由是
"换锚只是换一根绳子"。那个论证对**生命周期所有权**成立，我却把它推广成了对**取消能力**的禁令——两者是不同的东西。
caller-owned 的 ctx 说的是"请求你的那个 session 结束了"，server-owned 的说的是"你正在被停止"，语义相反，只有前者是 bug。
禁令的代价是具体的：它把**完全没有 deadline** 的 `net.LookupIP` 逼进了 handler。修复一个 hang 的机制，被我用一道闸门挡在了门外。

## F2 — 采纳并已修

upstream 建立后经 `bindKeyConn(chosenID, remote)` 绑到**同一个 key**，撤销时两半原子关闭；
新增 `unbindKeyConn` 在 relay 结束时解绑（否则 `keyConns` 会按完成的 relay 数无界增长，与 `allConns` 同类）。
`bindKeyConn` 自带 active 复查，顺带关掉「拨号期间 key 被撤销」的竞态：绑定失败即由 defer 关闭 remote，不启动 relay。
`TestRevocationReclaimsSilentUpstreamConnections` 转绿；既有的 `TestRevokeForceClosesInflight` 与 drain 测试未受影响。
变异：去掉 upstream 的 bind → 反例重新变红。

## F3 — 采纳并已修

`proxyStartLocked` 在 `wantLocal != 0` **且** `errors.Is(err, syscall.EADDRINUSE)` 时，用 `Start(0, keys)` 重试**一次**；
footprint 随后按实际绑定的端口持久化，公网端口/token/home 均不变。按外审要求**刻意收窄**：其它 listen 错误
（EACCES、EMFILE…）仍 fail closed——换端口不解决它们，掩盖只会藏起真实故障。同一个 `Server` 重试是安全的：
失败的 `Start` 未设置 `ln`、未 latch `closed`，没有消耗任何东西。
`TestCorpseRebuildSurvivesPersistedLocalPortCollision` 转绿；变异：去掉重试分支 → 重新变红。

## F4 — 采纳并已修；**这条推翻了我在内审中的驳回，错在我**

修法：新增 `reapProxyCorpseOnHeartbeat`，在 `heartbeatLoop` 每次 tick **发布之前**运行。它使这次心跳同时携带
`ProxyBound=false` **和** `(0,0)`——这个组合 `repairProxy` 的两道早退都不匹配（CONVERGENCE-FIRST 要求
`on && ready && 序对相同`，Fix D 要求 `!ready && 序对相同`），修复推送因而得以发出，随后 bootstrap 重建、公网端口不变。

**我的错误**：内审有三个独立 lane 报告「single 模式下修复无效」，我全部驳回，理由是"broker 仍认为 ready=true，
所以 Fix D 的 `!ready` 不成立、不抑制、推送会到达"。我读了 Fix D，却**没有读它上面那道 CONVERGENCE-FIRST 早退**
（`internal/broker/proxy.go`）——而 corpse 恰好完全匹配它：DB 里 `ready` 仍为 true，且 `p.srv != nil` 时
`proxyGenEpoch` 报的正是真实序对。于是根本走不到 Fix D。
我不仅据此驳回了三位 reviewer，还把这个错误结论写进了 `proxyBound` 的注释和内审报告。两处均已按事实改写，
`reapProxyCorpseLocked` 那句 "whatever kills the server, the runtime converges" 也收窄为「cause-independent 但依赖调用边」。
教训是具体的：**我在一条早退链上只读了自己预期会命中的那一段**，而这正是我用来否决多人一致意见的依据。

变异发现的附带问题：我为 F4 写的行为测试**直接调用** `reapProxyCorpseOnHeartbeat`，因此把 `heartbeatLoop` 里的调用删掉
它照样通过——与外审 F15 指出的「测函数不测接线」完全同形，而且是在同一轮里立刻重犯。已补
`TestHeartbeatLoopWiresTheCorpseReap`（AST 接线闸门），变异验证通过。

## F5 — 采纳并已修

闸门重写为 `TestSSProxyEntryPointsAcceptNoCallerContext`：断言的是**导出方法的签名不得接收 `context.Context`**
（"不能接收 caller 的生命周期"），而不再是包级 import 禁令；扫描改为 `filepath.WalkDir` **递归**，子包无法逃逸；
保留"`Start` 必须仍存在"的反空转断言。变异：给 `Server` 加一个接收 ctx 的导出方法 → 闸门变红并点名该方法。

## F6 — 采纳并已修

`CloseListenerForTest` 降级为包内 `closeListenerForTest`，使用它的测试
（`TestServingIsFalseWhenAcceptLoopDiesWithoutClose`）从 `internal/agent` 迁入 `internal/agent/ssproxy`。
生产 API 表面不再含 `ForTest` 方法，外部调用者也无法再制造 `closed==false && acceptExited==true` 的异常态。

## F7 — 采纳并已修

gotcha #80 的结论口径改写：明确区分「原始 session-context 根因已修」与「外审 F1–F4 已在同一增量内修复并各有反例转绿」，
并如实列出**仍未闭合**项（`Stop` 在 `p.mu` 下的**总**上界尚未做成显式契约；#33 仍是 CANDIDATE，转 FIXED 需重跑 drill 73
见 STRANDED 臂消失）。`docs/devices.md.bak.20260820` 经 SHA-256 核对与 `HEAD:docs/devices.md` **完全相同**、无审计价值，
已用 `git rm -f` 从工作树与 index 移除；`.gitignore` 增加 `*.bak.[0-9]{8}` 模式并实测生效——
故意不用宽泛的 `*.bak`，以免连带隐藏某个有意提交、经过评审的产物。

---

# 独立外部复审结论（step 7）

## Verdict

**Pass。** 按用户本轮明确口径，只有发现重大问题才判 Fail。本轮没有发现 Blocker 或 Major；首轮 F1–F7 均已由源码、独立反例、真实接线测试和运行证据闭合。保留两个 Minor 与一个非阻断建议，均不改变 Pass。

开发者回复没有被直接采信：复审以首轮已暂存树为基线，只审查随后 12 个路径、约 542 行新增/修改的 developer delta，并重新走完整门禁与 deploy drill。外审仍未修改生产实现；仅增加真实 heartbeat 顺序测试、复审 tasklist 和本节报告。

## 首轮 findings 复核

### F1 — Closed

`Server` 在成功 Start 后创建自己的 `stopCtx`，不接收 caller/session context；`shutdown` 在关闭 sockets 前 cancel。DNS 改为 `LookupIPAddr(ctx, ...)`，拨号改为 `DialContext`。Start/Stop 共享 `s.mu`，所以 Stop 不会观察到“listener 已发布但 cancel 尚未安装”的中间态。

首轮 `TestStopCancelsInFlightDestinationResolution` 未被放宽，连续 20 次通过；affected `-race` 也通过。server-owned cancellation 与原事故的 caller-owned lifetime 已正确分离。

### F2 — Closed（有 Minor R2）

upstream 在进入 relay 前同时加入 `allConns` 和对应 `keyConns`；`bindKeyConn` 在锁内复查 key 仍 active，revoke/shutdown 竞态均会关闭 remote，`unbindKeyConn` 又保证正常完成后不残留 key map entry。

首轮 `TestRevocationReclaimsSilentUpstreamConnections` 未被改写，连续 20 次通过；drain、旧 revoke 测试和 affected `-race` 均通过。原来的长期 socket/goroutine/fd 泄漏已闭合。

### F3 — Closed

只有 `wantLocal != 0` 且 error chain 匹配 `syscall.EADDRINUSE` 时才以 `Start(0, keys)` 重试一次；其它 listen 错误仍 fail closed。失败的第一次 Start 尚未设置 listener/context/closed，复用同一 Server 安全。成功后的实际 local port 会随正常路径写回 footprint，public port/token/home 不变。

首轮 `TestCorpseRebuildSurvivesPersistedLocalPortCollision` 未被放宽，连续 20 次通过。

### F4 — Closed

`heartbeatLoop` 在读取 generation/epoch/ProxyBound 并 Publish **之前**调用 `reapProxyCorpseOnHeartbeat`。corpse 被 teardown 后 `p.srv=nil`，首个 heartbeat 报 `(0,0,false)`：single broker 的 CONVERGENCE-FIRST 和 exact-pair-unready 两道早退都不再匹配，真实 heartbeat broker 测试证明 keyset 会被推送；cluster mode 则由现有 reconcile 在 unready 后以原 allocation nudge，保持公网端口。

开发者新增的 helper 行为测试和 AST 接线门不足以独立证明调用顺序，因此复审增加 `TestHeartbeatPublishesReapedCorpseStateOnItsFirstTick`：启动真实 NATS、只 corpse SS server、不制造 directive/session rebuild，捕获**第一条**生产 heartbeat 并断言 `(0,0,false)`。该测试 `-race -count=20` 通过，且会在把 reap 移到 Publish 后时变红。

### F5 — Closed（有 Minor R1）

package-wide context import 禁令已移除。当前 production API 没有接收 `context.Context`，`Start` 仍无 caller lifetime；递归扫描也消除了子包直接逃逸。守卫精度的剩余问题见 R1，但当前实现本身符合契约。

### F6 — Closed

`CloseListenerForTest` 已改为 unexported `closeListenerForTest`，测试迁入 `ssproxy` 包；production exported API 不再包含测试控制面。

### F7 — Closed

gotcha #80 已区分原始事故、F1–F4 修复和仍未闭合的总 teardown budget。`docs/devices.md.bak.20260820` 已从工作树/index 删除；实际 `.gitignore` 使用八个 `[0-9]` glob（不是正则量词），`git check-ignore -v --no-index docs/example.bak.20260820` 命中 line 92。#33 继续标 CANDIDATE 是诚实口径。

## 非阻断 findings

### R1 — Minor — context/heartbeat AST 门仍有可绕过的语义盲区

`TestSSProxyEntryPointsAcceptNoCallerContext` 只检查 `Recv != nil` 的 exported methods（`test/architecture/dataplane_lifetime_test.go:75-102`），所以 exported constructor/free function 接收 context 不会被看到；它还只认 selector 的包名恰为 `context`，import alias/type alias 可逃逸，并把任意 receiver 的 `Start` 当成 `Server.Start` 的反空转证据。

`TestHeartbeatLoopWiresTheCorpseReap` 只检查函数体中出现调用名（`:248-258`），不检查调用在 snapshot/Publish 前。当前 production 顺序正确，复审的真实 NATS 测试已覆盖当前行为，所以这是 guard fidelity 问题，不是当前产品故障。

建议：使用 `go/types`/`go/packages` 按真实类型身份检查全部 exported funcs/methods，并精确确认 `(*Server).Start`；heartbeat 以本次新增的行为测试为承重门，AST 门只作快速错误提示。

### R2 — Minor — revoke-during-dial 仍可能完成一次短暂 outbound connect

remote 只有在 `dialTarget` 成功返回后才执行 `bindKeyConn(chosenID, remote)`（`internal/agent/ssproxy/server.go:686-726`）。若 key 在 resolver/dial 阻塞期间被撤销，accepted socket 会立即关闭，但 dial 的 context 只由整个 Server.Stop cancel，不由 key revoke cancel；TCP connect 仍可能成功一次，随后 active 复查失败并立即关闭 remote。不会启动 relay、不会持续泄漏，故不构成首轮 F2 的 Major 资源问题，但源码注释“关掉拨号期间 key 被撤销的竞态”略有过度表述。

建议：若 hard revoke 契约包含“撤销后不得再产生任何 outbound connect side effect”，为 key/session 建立可撤销的 op context；否则收窄注释为“阻止 revoked key 进入 relay，并立即回收 dial 结果”。

## 验证账本

| 验证 | 复审结果 |
|---|---|
| F1/F2/Serving/drain focused `-count=20` | PASS |
| F3/F4/stopper/exit focused `-count=20` | PASS |
| single-broker heartbeat repair tests `-count=10` | PASS |
| 新增真实 heartbeat first-tick `-race -count=20` | PASS |
| `go test -race ./internal/agent/... -count=1` | PASS |
| `make gates` | PASS；all-tags vet、darwin cluster build、architecture/determinism/concurrency、lint 0 issues、gofmt clean |
| `make test` | PASS；全仓通过，`internal/broker` 331.198s |
| `make e2e-parallel` | PASS；coverage 15/15，99/99，`ALL PASS`，3m59.937s |
| current-source simcluster build | PASS；静态 tether/tether-next 与 `tether-sim:dev` 重建，nats-server 2.10.22 pin 一致 |
| drill `73-proxy-cluster-ha` | GREEN；46 assertions、0 gaps、direct poll wait 24s |

drill 73 在真实 non-tunnel home 上杀死 agt1 home broker：同一数据面先证实流量、故障后证实 black-hole，控制面约 25s rehome+ready，数据面约 26s **AUTO-RECOVERED**，没有 manual off/on 或 STRANDED；revoke、quorum freeze 和独立 survivor leg 也均通过。`sim.instance=drill-73-proxy-cluster-ha` 的容器、network、volume 清理后均为空。

## 残余风险与建议

- 当前所有已知 blocking spans 已被 accepted/upstream socket close 或 server-owned cancel 覆盖，但 `Stop` 在 `p.mu` 下的总 wall-clock budget 尚未形成统一 deadline/metric；维持 gotcha 的“未闭合”标记是正确的。建议后续独立增量定义预算，而不是在本轮再扩 scope。
- simcluster 仍没有直接注入 DNS stall、local-port theft、accept-loop spontaneous error；前三者分别由确定性本地测试覆盖，drill 只证明真实 cluster crash/rehome 形状。没有把这些未注入场景写成 sim PASS。
- #33 曾表现为 per-run 非确定性；本次修复后 drill 为 GREEN 是必要证据，不足以证明 #80 是历史 #33 的唯一根因，因此继续 CANDIDATE。

## 复审交付

复审 tasklist 全部执行，最终 diff check 通过。依用户要求，复审报告、独立 heartbeat 测试、开发者修复及全部其它工作树变更均重新 `git add -A`；最终无未暂存路径。顶部 verdict 为 **Pass**。
