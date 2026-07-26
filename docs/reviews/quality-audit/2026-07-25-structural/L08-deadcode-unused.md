# Lane 8 — 死代码 / 未使用导出 / 只服务于测试的生产代码

> 结构性质量审计 · 2026-07-25 · lane key = `deadcode-unused`
> 范围：全仓生产代码（`internal/` + `cmd/`，68,328 行，5,101 个顶层声明）
> 只读审计。未修改任何实现代码。

---

## 结论

**净判断：死代码不是 tether 臃肿的原因，一点都不是。这个仓库在"删干净"这件事上做得比绝大多数同体量 Go 项目好。**

用 `golang.org/x/tools/cmd/deadcode` 做 RTA 全程序可达性分析（从 `cmd/tether` 唯一 main 出发），
全仓只有 **51 个函数不可达，合计 392 行 = 生产代码的 0.57%**。35 个 `internal` 包里
**只有 1 个**（`internal/testharness`，189 行）不在主二进制的 import closure 内，其余 34 个全部真正装进了发布二进制。
生产代码里 **0 处注释掉的代码块**、**2 处空方法体**（都是合法的 `noopTransport.Close` 与 raft `SnapshotSink.Release`）、
**1 处** 上一轮 audit 点名的 `var _ = X` 占位符残留（且在测试文件里）。

**bloat 打分（仅就本 lane 的维度）：2 / 10。**
理由：可安全净删的生产行数估计 **140–160 行，占 0.2%**。把"只服务于测试的生产代码"全算上也不到 1.5%。
按"死代码堆积"这个指标，这不是屎山，甚至不是普通工程债。

**但本 lane 找到的问题不在体量，在于"活下来的死符号在误导人"。** 四条最有价值的发现全是这个形状：

1. `internal/auth/jwt.go` 整个 `AccountSigner`（95 行、4 个导出符号、两套测试）**从未被生产接线**——
   而它专门要执行的那个安全校验（"account.nk 必须真的是 account key"）生产路径上**确实没有**。
   测试还在断言这个校验存在，给了假的信心。
2. `port.Revoke` / `port.PlanRevoke` 是被 race-safe 版本取代的旧孪生，godoc 却还写着"broker reconciler 用它"——
   下一个人照着 godoc 选，就把 port-reuse race 重新引回来。
3. 5 条 wire subject 在 JWT permission 模板里**仍然被授权**，但全仓没有任何 publisher / subscriber。
4. `schema.AuditCall.ActorName` / `AuditProc.DurationMs` / `AuditPort.ActorNkey` 三个字段的注释声称"会被填"，
   生产从来不填——而上一轮 audit（shard 03 F2）刚刚修好**同一个 struct 的另外两个同类字段**。

Verdict: **minor-debt**。体量健康，但有 2 条 high 值得立刻处理。

---

## 范围与方法

三条互相独立、互相交叉验证的证据链：

**(A) RTA 全程序可达性** — `golang.org/x/tools/cmd/deadcode v0.42.0`，
入口 `./cmd/...`，`-filter github.com/LinZiyang666/tether`。
它正确处理接口动态派发（`raft.FSM.Apply`、`ExposeAdapter.AddProxy` 这类不会被误报）。
产出：51 个 unreachable func，392 行。

**(B) 自写 AST/类型引用扫描** — `golang.org/x/tools/go/packages` 加载 `./...`，`Tests: true`，
**并带上全部 8 个 build tag**（`d9_integration,d5_integration,d8_integration,d6_integration,d7_integration,c7_integration,phasefluidity_integration,e2e_matrix`）。
> 这一步是必须的：不带 tag 时 `Broker.ClusterStateForTest` 等 5 个方法会被误判为"零引用"，
> 带上 tag 后它们正确地落进"只被 `test/d9` 用"。任何不带 tag 的死代码扫描在本仓都会产生假阳性。

统计每个生产声明（func / method / type / const / var / struct field）的引用来源，
区分"来自非测试文件"与"来自 `_test.go`"。产出：
- **58 个零引用声明**（其中 28 个是接口实现方法 → 假阳性，真死 30 个）
- **86 个只被 `_test.go` 引用的生产声明**

**(C) 定向 grep + 真读代码** — 对 (A)(B) 给出的每一条候选，读原文件确认语义。
重复实现一类完全靠读代码确认（grep 只用来定位候选）。

**分母**：生产代码 68,328 行 / 5,101 个顶层声明（其中 2,873 个导出）。
零引用率 = 30/5101 = **0.59%**；测试专用率 = 86/5101 = **1.7%**。

**上一轮 `06-deadcode-drift.md` 的核实结果**（13 条）：

| 上轮 finding | 现状 |
|---|---|
| F1 `natsconn.go` AgentName godoc 反了 | ✅ 已修（`DeniedUntilP4` 全仓 0 命中） |
| F2 `broker/expose.go` 指向不存在的 `internal/frpmgr` | ✅ 已修（`.go` 里 0 命中） |
| F3 `agent/expose.go` "P6-6 ships frp adapter" | ✅ 已修 |
| F4 `var _ = errors.New` 占位符 | ✅ 已修 |
| F5 `var _ = cobra.Command{}` 占位符 | ⚠️ **仍在**，迁到了 `cmd/tether/cli_ux_test.go:928` |
| F6/F7/F8/F9/F11 "lands in P\<N\>" 相位标签漂移 | ✅ 已修（`lands in P[0-9]` 全仓 0 命中） |
| F10 `tunnel.hashToken` 重复 `port.HashToken` | ❌ **恶化**：当时决定"用注释解决而非提取"，此后又长出**第三份**（`proxysub.HashToken`），见 F6 |
| F12 `unparam` 3 处 | 未复查（不在本 lane） |
| F13 `architecture.md` 提 `internal/frpmgr` | ✅ 已加注（`architecture.md:1391/2124` 显式说明被 `internal/tunnel` 替换） |

11/13 已闭合，1 条部分残留，**1 条因为"决定不修、只加注释"而繁殖了**。这本身是个信号：F10 那种处理方式在这个仓库不 work。

---

## Findings

### F1 — high — `internal/auth/jwt.go` 整套 `AccountSigner` 从未接线；它要守的那条不变量生产路径上真的没有

**证据**
- `internal/auth/jwt.go:12-93` — `AccountSigner` 类型 + `LoadAccountSigner` / `IssueUserJWT` / `AccountPublicKey` / `DecodeUserJWT`，共 95 行（整个文件）。
- 包 godoc 明写：`"tetherd holds exactly one and uses it from inside its auth_callout response handler (architecture E.5)"` — **这句话是假的**。
- deadcode RTA：4 个函数全部 unreachable from main。AST 扫描：只被 `internal/auth/jwt_test.go` 和 `test/p1/foundation_risk_test.go` 引用。
- 生产真实路径：`internal/broker/authcallout.go:70` 直接 `nkeys.FromSeed(b.cfg.AuthCallout.AccountSeed)`；
  签 JWT 在 `internal/authcallout/handler.go:427-455`（`jwt.NewUserClaims(userPub)` + `uc.Encode(h.AccountKp)`）——
  与 `IssueUserJWT` 逐行同构，但**少了 `IsValidPublicUserKey(userPub)` 前置校验**。
- seed 加载点 `cmd/tether/serve.go:437-451` `loadAuthCalloutSeeds`：只读文件，**不做任何 key kind 校验**。

**为什么是债（它让什么变难/变危险）**
`LoadAccountSigner` 存在的**唯一理由**写在它自己的 godoc 里：拒绝把 user seed（`SU…`）当 account seed 用，
因为那会"silently produce JWTs with a wrong-kind issuer"。这条校验在生产路径上不存在。
后果：运维在 `/etc/tether/account.nk` 里放错一个 seed（比如从 `keys/default.nk` 复制），
broker **会正常启动**，`installAuthCallout` 成功，然后每一个客户端连接在 auth_callout 阶段被 NATS 静默拒绝——
一个本该在 boot 时用一行明确错误挡掉的配置错误，变成了"服务起来了但没人能连"的现网事故。
更糟的是 `test/p1/foundation_risk_test.go:35` **正在断言这个校验存在**，
所以任何人 review 时看到"有 risk test 覆盖"都会认为这条路已经守住了。
这正是"测试覆盖了一条产品不走的路"的教科书案例。

**建议**
不要删，**要接线**：`loadAuthCalloutSeeds` 里把 `accountSeed` 过一遍 `auth.LoadAccountSigner`（丢弃返回的 signer，只取校验），
或让 `AuthCalloutConfig` 直接持有 `*auth.AccountSigner`。
顺带把 `authcallout/handler.go:427` 的 `uc.Encode` 换成 `signer.IssueUserJWT`，消掉 JWT 签发的第二份实现。

**量化 / 风险**：净增 ~3 行、消掉 95 行的"平行实现"状态。changeRisk **low**——不动 wire、不动 subject、
只在 boot 路径加一次校验；唯一的行为变化是"错误的 account.nk 现在会 fail fast"，这正是想要的。
不碰 `ProtoVersion`，不需要重装现网。

---

### F2 — high — `port.Revoke` / `port.PlanRevoke` 是被 race-safe 版本取代的旧孪生，godoc 还在给新调用者指错路

**证据**
- `internal/port/port.go:399-425` `Revoke(db, port, now)` — 25 行，按 **public port 单键** 做 `ALLOCATED → REVOKED`。
- `internal/port/port.go:374-378` `RevokeAllocation(db, a Allocation, now)` — 生产实际使用的版本，
  谓词是 `port AND sid AND nid AND name AND token_hash AND state='ALLOCATED'` 的**整行身份**。
  它的 godoc 明说理由：*"so a delayed revoke cannot affect a later allocation that reused the same port"*。
- `internal/port/plan.go:74-77` `PlanRevoke` vs `internal/port/plan.go:41-44` `PlanRevokeAllocation` — 同一对关系。
- deadcode：`Revoke`、`PlanRevoke` 均 unreachable from main。
  AST：`Revoke` 只被 `internal/port/port_test.go` + `test/cluster/equiv_test.go` 引用；`PlanRevoke` 只被 `test/cluster/` 两个文件引用。
- 而 `Revoke` 的 godoc 写着：*"used by the broker reconciler when the owning node has been OFFLINE long enough (architecture D.4 / F.3, default 15min)"* —— **reconciler 用的是 `RevokeAllocation`，不是它**。

**为什么是债**
这不是"多了 28 行"的问题，是**陷阱**。仓库里现在并排放着两个同名同义的 API，
一个安全、一个有已知的 port-reuse race，而**有 race 的那个的 godoc 声称自己是 reconciler 在用的那个**。
下一个要加"revoke 一个 port"的人（比如给 `expose rm --force` 或 cluster drain 加路径）grep `Revoke`，
读到那句 godoc，会理所当然地选 `port.Revoke`——然后 15 分钟 OFFLINE 窗口里被重新分配的端口会被延迟到达的 revoke 打掉，
表现为"别人的 expose 莫名其妙被撤销"。这类 bug 在测试里抓不到（需要精确的时序 + 端口复用），只在现网出现。
同时 `test/cluster/equiv_test.go` 还在用它做 single/cluster 等价性断言，
意味着**等价性证明覆盖的是一个产品不走的路径**。

**建议**
删 `Revoke` 与 `PlanRevoke`，把 `test/cluster/equiv_test.go` / `port_test.go` 里的调用改到 `RevokeAllocation` / `PlanRevokeAllocation`。
如果为了保住"port 单键"的等价性断言，至少把 `Revoke` 改成 `//lint:ignore` 之外的显式标注：
`Deprecated: race-prone port-keyed form; production uses RevokeAllocation. Kept only for the single/cluster equivalence test.`

**量化 / 风险**：净删 **28 行生产代码** + 改 3 个测试文件的调用点。
changeRisk **low**——纯删除一个无生产调用者的函数，不动 schema（`REVOKED` 状态本身照旧），不动 wire。

---

### F3 — medium — 5 条 wire subject 没有任何 publisher / subscriber，却仍在 JWT permission 模板里被授权

**证据**

| subject | subject builder | 授权点 | 生产 pub | 生产 sub |
|---|---|---|---|---|
| `tether.v2.ctrl.version.announce` | `proto/subjects.go:10` `SubjVersionAnnounce`（const，只被 `proto_test.go` 用） | `auth/permissions.go:35`（ctl-unactivated sub）、`:136`（ctl-activated sub）、`:227`（agent sub） | 无 | 无 |
| `…ctrl.s.<sid>.node.<nid>.unregister.req` | `proto/subjects.go:74` `SubjNodeUnregister`（只被测试用） | `permissions.go:170`（agent pub）、`:250`（broker sub） | 无 | 无 |
| `…s.<sid>.ev.node.<nid>.state` | `proto/subjects.go:82` `SubjEvNodeState`（只被测试用） | 被 `permissions.go:137` `ev.>` / `:172` `ev.node.<nid>.>` 通配覆盖 | 无 | 无 |
| `…s.<sid>.pty.<pid>.ready` | `proto/subjects.go:203` `SubjPtyReady`（只被测试用） | `permissions.go:140`（ctl sub）、`:174`（agent pub） | 无 | 无 |
| `…cluster.>` 里的 `SubjClusterWildcard` | `proto/subjects.go:27`（只被 `auth/permissions_test.go` 用） | — | — | — |

补充证据：
- PTY 的 "ready" 实际是 `pty.<pid>.out` 上的一个 chunk kind（`internal/agent/run.go:131` `Kind: "ready"`，`cmd/tether/run.go:152` `case "ready":`），**不是独立 subject**。
- `internal/broker/audit.go:37` 自己就写着 `agent_unregistered` *"have NO producer in v1"* —— 缺口在一个地方被承认了，但授权模板没跟着收。
- `git log -S SubjNodeUnregister(` 显示它在 P2 有过真实调用者，后来在 `55b1451` 被移走；授权留下了。
- `git log -S SubjVersionAnnounce` 只有 P1 一次提交 —— 定义了 13 个 phase，从未接线。

**为什么是债**
`internal/auth/permissions.go` 是这个产品的**授权边界**，是安全审查唯一要看的那张表。
表里有 5 条没有消费者的 subject，代价有三层：
(a) 安全审查面比真实协议面大——每次做 permission review 都要重新判断这 5 条是不是活的；
(b) 任何被授权的 ctl / agent 现在可以往 `ctrl.version.announce` 上无限 publish，没有 rate limit、没有消费者、没有 schema 校验。
今天无害；哪天有人给这条 subject 加了 subscriber，它**继承的是一个从没被有意设计过的授权**，而不是一个新写的授权；
(c) 反过来，想加"版本广播"功能的人会发现 subject 和授权都已经在了，于是跳过设计直接用——这正是最容易出授权错误的路径。

**建议**
从 `permissions.go` 的 5 个模板里摘掉这 4 条（`SubjClusterWildcard` 只是个未用常量，删常量即可），
同时删掉 `SubjVersionAnnounce` / `SubjNodeUnregister` / `SubjEvNodeState` / `SubjPtyReady` 四个 builder。
如果 `unregister` 是有意保留的未来能力，就在 `docs/architecture.md` 里明确登记为"已保留未接线"，并**保留 subject 但撤掉授权**——
授权可以在接线那天再加，这才是 least-privilege。

**量化 / 风险**：净删 ~14 行（4 个 builder + 1 个 const）+ 6 行授权。
changeRisk **medium**——授权模板变化会体现在新签发的 user JWT 上。但历史上从没有任何 agent/ctl 版本 publish 过这些 subject
（`SubjNodeUnregister` 的最后一个生产调用者在 `55b1451` 就被删了，而那早于 v0.1.0），
所以现网不会有连接因此被拒。不涉及 `ProtoVersion`，**不需要重装**。

---

### F4 — medium — `schema` 里三个字段的注释声称"会被填"，生产从不填；上一轮刚修过同一 struct 的同类问题

**证据**
- `internal/schema/audit.go:24` `AuditCall.ActorName string \`json:"actor_name,omitempty"\` // display only`
  → 全仓唯一的 `schema.AuditCall{}` 构造点 `internal/broker/exec.go:428-435` **不设这个字段**。
- `internal/schema/audit.go:45` `AuditProc.DurationMs *int64 // nil unless exit`
  → 构造点 `internal/broker/exec.go:447-457`（`pubAuditProc`）在 `kind == "exit"` 分支里**只设 `RC`**；
  另一个构造点 `internal/proc/plan.go:238-242` 也不设。**"unless exit" 的那个 exit 永远不发生。**
- `internal/schema/audit.go:61` `AuditPort.ActorNkey string // populated when user-triggered`
  → 构造点 `internal/broker/expose.go:523-527` `pubAuditPort` 的签名根本没有 `actorNkey` 参数，只有 `actorFP`；
  `internal/proc/plan.go:245-249` 同样不设。
- AST 扫描：三个字段都只在 `internal/schema/audit_test.go` 里被引用。
- **同一个 struct 的 `ReqID` / `Target` 在上一轮 audit（03 F2）被发现"defined in schema but never populated by the broker"并修好了**——
  修复注释就写在 `internal/broker/exec.go:419-422`。三个兄弟字段漏网。

**为什么是债**
`schema` 包的 godoc 把自己定义为 *"append-only contract"*：一旦写进 JetStream `history-<sid>` 就必须永远可解码。
消费者（`tether history`、任何外部 audit pipeline）会照着这个 struct 写解码逻辑，然后发现
`actor_name` 永远空、`duration_ms` 永远 nil、port 审计的 `actor_nkey` 永远空。
`DurationMs` 尤其要命：进程执行时长是审计最常被问的字段之一，schema 说"exit 时有"，
运维查 `history` 发现全是 nil，只能自己拿 start/exit 两条记录去减——而这个 join 恰恰是上一轮 F2 修 `ReqID`/`Target` 想解决的问题。
更结构性的一点：**同一个缺陷类型在同一个文件上复发**，说明"schema 字段 ↔ 构造点"之间没有任何机械约束
（既没有 linter，也没有一个"每个非 omitempty 字段至少被某个构造点写过"的测试）。

**建议**
两条路二选一，不要留中间态：
(a) **填上** —— `pubAuditProc` 增加 `startedAt` 参数在 exit 分支算 `DurationMs`；`pubAuditPort` 增加 `actorNkey` 参数
（调用点已经有 `actor`，见 `broker/expose.go` 里 `pubAuditCall(sid, fp, actor, …)`）；`ActorName` 若无来源就删。
(b) **删字段** —— 但 `schema` 是 append-only 契约，删已发布字段需要 bump `AuditSchemaVersion`，代价高于填上。
所以实际推荐 (a)。
另外加一条便宜的机械约束：在 `internal/schema/audit_test.go` 里加一个"每个字段至少被一个生产构造点写过"的反射断言，
把这条复发路径钉死。

**量化 / 风险**：0 净删行，净增 ~10 行 + 一个约束测试。changeRisk **low**——
纯 additive 的字段填充，JSON 是 `omitempty`，老消费者不受影响，不动 `AuditSchemaVersion`。

---

### F5 — medium — `ExposeReq` / `ExposeRmReq` / `UpgradeReq` 的 `ActorFP` 声明了一条 broker 从不执行的契约

**证据**
- `internal/proto/messages.go:577-579`：`// ActorFP — broker-stamped at forward time; same convention as ExecReq.ActorFP. ctl-supplied value is discarded.`
- `internal/proto/messages.go:670`：`ActorFP string \`json:"actor_fp,omitempty"\` // broker-stamped`
- `internal/proto/messages.go:715-717`：同样的措辞。
- 真实情况：broker 只 stamp **forwarded 类型**，不 stamp 请求类型——
  `internal/broker/expose.go:277` 设的是 `proto.ExposeForwardedReq{… ActorFP: fp}`；
  `internal/broker/upgrade.go:115` 设的是 `proto.UpgradeForwardedReq{… ActorFP: fp}`。
- 唯一真的做了 in-place stamp 的是 `ExecReq`（`internal/broker/exec.go:97` `req.ActorFP = fp`）和
  `RunReq`（`run.go:83`/`:170`）、`transfer` 三处（`transfer.go:631/754/864`）——所以那句 *"same convention as ExecReq.ActorFP"* 读起来完全可信。
- AST 扫描：这三个字段在生产代码里**既不写也不读**，只被 `internal/proto/proto_invariants_test.go` 等 wire 形状测试引用。

**为什么是债**
"ctl-supplied value is discarded" 是一条**安全语义**：它宣称 ctl 无法伪造 actor 身份。
今天这条宣称是空的（因为没人读这三个字段，所以伪造也没用），但它是一颗定时炸弹：
下一个人给 expose 加"记录发起人"的功能时，读到这行注释会直接 `req.ActorFP` 拿来用——
拿到的是 **ctl 自己填的、未经 broker 覆盖的值**，也就是一个可伪造的 actor 身份进了审计记录。
这不是理论风险：`ExecReq` 的同名字段确实是 broker 覆盖的，两者在同一个文件里相隔 200 行，
措辞逐字相同，只有实现不同。

**建议**
二选一：
(a) 让 broker 在 expose / expose-rm / upgrade 的 handler 里也做 in-place stamp（与 exec/run/transfer 对齐，各 1 行），
    这样注释变真，且未来的读者拿到的是可信值；
(b) 删掉这三个字段并把注释改成"actor fp 只在 `*ForwardedReq` 上；ctl→broker 的请求不携带 actor"。
推荐 (a)——它把三条不同的约定收敛成一条，代价 3 行。

**量化 / 风险**：(a) 净增 3 行、消掉 3 处语义分叉；(b) 净删 3 行 wire 字段。
changeRisk **low**（(a) 完全 additive，字段是 `omitempty`，老 ctl 发不发都一样）；
(b) 严格说是 wire 字段删除，虽然没人读，仍建议走 (a) 避免任何 wire 讨论。**不触碰 `ProtoVersion`**。

---

### F6 — medium — 三份逐字节相同的 `hashToken`；上一轮决定"用注释解决"，之后长出了第三份

**证据**
- `internal/port/port.go:466-473` — `HashToken` / `hashToken`，godoc：*"Public so callers outside this package … can compute the lookup key … without re-implementing the hash choice."*
- `internal/tunnel/tunnel.go:1269-1277` — `hashToken`，godoc：*"Kept locally (duplicated with internal/port.HashToken) to keep tunnel a dep-graph leaf … Audit shard 06 F10 — flagged as low, resolved by comment, not extraction."*
- `internal/proxysub/proxysub.go:204-208` — `HashToken`，godoc：*"same scheme as port tokens"*。
- 三份实现完全一致：`sum := sha256.Sum256([]byte(raw)); return hex.EncodeToString(sum[:])`。
- 三份服务于三个不同的 bearer-token 命名空间：expose port token（DB 查表键）、tunnel REGISTER token（数据面认证）、
  proxy subscription token（`/sub/<token>` HTTP 端点的查表键，`internal/subhttp` 用）。

**为什么是债**
这三处是**同一个安全决策的三个副本**：raw bearer token 的存储/查表表示。
它们必须永远一致，否则不是编译错误、不是测试失败，而是**静默的 fail-closed 全网故障**：
比如把 hash 换成加 pepper 的形式（很合理的加固，因为 DB 泄露就等于 token 泄露），
改了 `port` 忘了 `tunnel`，结果是**每一个 agent 的 tunnel REGISTER 都被 broker 拒绝**——
6 台 agent 的隧道同时断，且错误信息是"token 不匹配"，排查方向完全指向 token 分发而不是 hash 实现。
上一轮 audit 已经识别出这个风险并评为 low，选择了"加注释说明是故意的"。
**结果是：注释生效了（`tunnel` 那份现在有很清楚的说明），但一年后 `proxysub` 又独立写了第三份，
而第三份的注释只写了 "same scheme as port tokens"，没有提到还有第二份。**
这说明"用注释固定重复"在这个仓库里不是稳定解——重复会继续繁殖，而每一份新副本的注释都只知道它自己那一份。

**建议**
提一个真正的叶子包 `internal/tokenhash`（无依赖，仅 `crypto/sha256` + `encoding/hex`，约 10 行），
三处都 import 它。`tunnel` 保持 dep-graph leaf 的诉求成立——`tokenhash` 本身就是 leaf，不会拉进 SQLite。
如果坚持不提取，至少让三份 godoc 互相点名（每份都列出另外两份的路径），让下一个改 hash 的人一次就能找全。

**量化 / 风险**：净删 ~8 行、新增一个 10 行的包（净 +2 行），但把 3 个必须同步的点收敛成 1 个。
changeRisk **low**——纯重构，无行为变化，`hex(sha256(x))` 不变，DB 里已有的 hash 全部继续匹配。

---

### F7 — medium — `clusterspec.NodeSpec` 的 4 个 yaml 字段解析后被丢弃，而生成的命令行恰好缺的就是这几个值

**证据**
- `internal/clusterspec/spec.go:28-35` `NodeSpec` 有 6 个 yaml 字段：`node_id` / `raft_addr` / `nats_route` / `tunnel_addr` / `cert_fp` / `desired`。
- `internal/clusterspec/spec.go:89-160` `Diff()` —— 唯一的消费者 —— **只读 `NodeID` 和 `Desired`**。
- AST 扫描：`RaftAddr` / `NatsRoute` / `TunnelAddr` / `CertFP` 四个字段全仓零引用（包括测试）。
- 而 `Diff` 生成的 add step 是：
  `internal/clusterspec/spec.go:111` — `"tether cluster join prepare --node-id %s …   # on %s, then on the leader: …"`
  —— 那个字面量 `…` 需要的正是 raft addr / nats route / tunnel addr / cert fp。
- `yaml.Unmarshal` 非 strict（`spec.go:56`，未设 `KnownFields`），所以这些 key 不是"为了让解析不报错而存在"。

**为什么是债**
`tether cluster apply` 的整个价值主张是"把 roster.yaml 翻译成运维要敲的命令"。
运维在 yaml 里认真填了 `raft_addr: 10.0.0.5:7300`，工具解析了它、然后扔掉，
最后打印一条带着字面 `…` 的命令让运维**再去别处翻一遍同样的地址**。
这不是"少了 4 行代码"，是这个命令的核心用途只完成了一半，而且从 struct 定义上看不出来——
字段在那儿、yaml 在那儿、文档在那儿，只有实现没有。
更麻烦的是包 godoc 写着 *"The YAML schema is kept additive so a future executor can consume the same file"*，
这句话把"没用上"合理化成了"为将来准备"，于是没人会去补。

**建议**
把这 4 个值插进生成的 verb 字符串（`Diff` 里改 1 处 `Sprintf`），字段立刻从死变活。
如果 `join prepare` 的真实参数形状不是这 4 个，那就把 yaml schema 改成真实形状——
但绝不能留"解析了、丢弃了、还在文档里"。

**量化 / 风险**：净增 ~4 行（或净删 4 个字段 + 5 行 yaml 文档）。changeRisk **low**——
`cluster apply` 是 plan-only、不执行任何变更（包 godoc 明说），改的只是打印出来的文本。

---

### F8 — low — `internal/` 包给"不可能存在的外部调用者"保留 back-compat / convenience API

**证据**
- `internal/subhttp/subhttp.go:147-150` `LiveProxyNodes` —— godoc：*"is the single-mode (P13) exit-node query, **kept exported for back-compat**"*。
  全仓零引用（deadcode + AST 双确认）。3 行。
- `internal/subhttp/subhttp.go:304-317` `Serve(ctx, addr, cfg)` —— godoc：*"Binds and serves in one call … **Retained for callers/tests that want the combined form**"*。
  生产走的是 `Bind` + `ServeListener` 两步（`internal/broker/broker.go:886-895`）；`Serve` 只被 `internal/subhttp/p13_external_review_test.go` 调用。10 行。

**为什么是债**
`internal/` 是 Go 的**封闭世界**：编译器保证仓库外没有任何 import 者。
所以"back-compat"和"retained for callers"在 `internal/` 包里是**语义上不可能成立的**理由——
不存在需要兼容的调用者，只有仓库里这些文件。
写下这两句 godoc 的人是把公共库的习惯带进了 internal 包。
代价：这两个符号会永远活着（因为注释给了它们存在理由），
下一次有人清理 `subhttp` 时会看到"kept for back-compat"然后跳过它们。
一个 317 行的包里有 13 行（4%）是靠一句不成立的理由活着的。

**建议**
删掉这两个函数，把 `p13_external_review_test.go` 里的 `Serve` 调用改成 `Bind` + `ServeListener`（2 行）。
更重要的是把这个判据写进 review checklist：**`internal/` 包不存在 back-compat 义务**；
任何以"外部兼容"为由保留的 internal 符号都应该被质疑。

**量化 / 风险**：净删 **13 行**。changeRisk **low**。

---

### F9 — low — 30 个真·零引用的叶子符号（其中一条还声称有个不存在的 guard test 守着它）

deadcode RTA + AST 双确认、排除全部接口实现方法后的完整清单：

| 符号 | 位置 | 行数 | 备注 |
|---|---|---|---|
| `session.Member` + 5 个字段 | `internal/session/session.go:51-57` | 7 | 整个类型死；`session.Member` 全仓 0 命中 |
| `proto.RehomeDirective` | `internal/proto/messages.go:192-206` | 3 (+12 行 godoc) | **见下方专门说明** |
| `natsconf.Ownership.ServerName` | `internal/natsconf/preflight.go:209-214` | 6 | |
| `spawnsafe.Policy.IsPathDead` | `internal/spawnsafe/spawnsafe.go:885-891` | 4 (+3 doc) | |
| `agent.IsPathValidationError` | `internal/agent/transfer.go:569-574` | 4 (+2 doc) | |
| `ssproxy.Server.LocalPort` | `internal/agent/ssproxy/server.go:283-288` | 5 (+1 doc) | `Start()` 直接返回 `s.localPort`，访问器无人用 |
| `xferaudit.TransferReqID` | `internal/xferaudit/plan.go:29-33` | 4 (+2 doc) | godoc 自称 *"legacy coarse key"*，被 `TransferRecordReqID` 取代 |
| `subhttp.LiveProxyNodes` | `internal/subhttp/subhttp.go:147-150` | 3 | 见 F8 |
| `proto.ProxySubListReq` | `internal/proto/messages.go:1076-1077` | 2 | 空 struct；ctl 直接发 `{}` |
| `proto.ClusterGrowSchemaVersion` | `internal/proto/cluster_grow.go:10-11` | 2 | 定义了 schema 版本但从未 stamp 进 `ClusterGrowReq` |
| `adminsock.CodeAlreadyVoter` | `internal/adminsock/protocol.go:127` | 1 | |
| `serveconf.NATSSection.WssListen` / `.WSInternal` | `internal/serveconf/serveconf.go:112-113` | 2 | 见下 |
| `agent.ProxyState.CertPins` | `internal/agent/state.go:63` | 1 | `json:"-"`；`SetProxy` 的构造点（`agent/proxy.go:311-314`）不设它，pins 实际走 `PortToken.CertPins` |
| `broker.xferProvisionErr.Error` | `internal/broker/xfer_provision.go:143` | 1 | 该类型全程以 `*xferProvisionErr` 具体指针传递，从不进 `error` 接口 |
| `cluster` 的 5 个 `applied*.index` 字段 | `internal/cluster/fsm.go:74-78` | 0 净行 | 全部以位置字面量构造（`appliedOK{l.Index}`），字段**只写不读** |

**`serveconf.WssListen` / `WSInternal` 值得单独说**：`scripts/install.sh:547-548` **确实会往 broker.yaml 里写这两个 key**，
`docs/broker-ops.md:112-113` 和 `docs/architecture.md:78-79` 也都文档化了它们。
但 Go 侧解析后从不读（Caddy 是独立配置的）。
即：运维在 broker.yaml 里改 `wss_listen`，重启 broker，**什么都不会发生**——一个静默无效的配置旋钮。
`yaml.Unmarshal` 非 strict（`serveconf.go:166`），所以删字段不会让已有 broker.yaml 解析失败。

**`proto.RehomeDirective` 的额外问题**：它的 godoc（`messages.go:192-203`）明确说这是有意未接线的
D7 备份 rehome 触发器，理由写得很清楚——**这部分是好的**（见反证 §8）。
但同一段 godoc 的最后一句写着：*"a guard test asserts it has no live publisher so a half-wiring is caught (review A5 M5)"*。
**这个 guard test 不存在**：`grep -rn RehomeDirective --include=*.go` 在全仓只有 3 个命中，全部在 `messages.go` 自己的注释和类型定义里，
没有任何测试引用这个类型。所以"有测试守着半接线"这句保证是假的。

**为什么是债**
单看每一条都是 nit。合起来的问题是：**这 30 条里有 3 条带着积极的误导性文本**——
"legacy coarse key"（暗示还有非 legacy 用途）、"kept for back-compat"（暗示有外部调用者）、
"a guard test asserts…"（暗示有安全网）。剩下 27 条是纯噪声。
`ClusterGrowSchemaVersion` 尤其值得注意：定义了 wire schema 版本却从未写进消息，
意味着 `ClusterGrowReq` 在线上**没有版本判别位**——如果将来要改 grow 协议形状，
没有办法让老 broker 识别并拒绝新形状的请求。这是个真正会咬人的缺口，虽然只有 1 行。

**建议**
按上表逐条删。三条特殊处理：
- `ClusterGrowSchemaVersion`：不要删，**要 stamp**——给 `ClusterGrowReq` 加 `SchemaVersion int \`json:"schema_version,omitempty"\`` 并在构造点填上（additive，不动 `ProtoVersion`）。
- `RehomeDirective`：删掉 godoc 里那句关于 guard test 的假保证，或者真把 guard test 写出来（一个 `grep`-style 的 AST 断言即可）。
- `serveconf` 两个字段：删 Go 字段，同时决定 `install.sh` 还写不写这两个 key（写就纯注释性，最好在 yaml 里加 `# informational only, read by Caddy setup not by tetherd`）。

**量化 / 风险**：净删 **~45 行代码 + ~25 行 godoc ≈ 70 行**。changeRisk **low**——
全是零引用叶子，删除不可能改变任何运行行为；`ClusterGrowSchemaVersion` 那条是 additive。

---

### F10 — low — `proc.ulidLower` + `bytePos` 用 26 行手写实现了 `strings.ToLower` 一行的事；同仓另一处已经这么写了

**证据**
- `internal/proc/proc.go:62-78` `ulidLower(u ulid.ULID) string` —— 手写 Crockford base32 大写→小写查表映射，17 行。
- `internal/proc/proc.go:79-86` `bytePos(s string, c byte) int` —— 手写 `strings.IndexByte`，8 行，只被 `ulidLower` 用。
- `internal/proxysub/proxysub.go:218-220` `newSubID()` —— 同样的需求（lowercase ULID），实现是
  `return strings.ToLower(ulid.Make().String())`，**1 行**。
- 两者语义等价：Crockford base32 字母表 `0123456789ABCDEFGHJKMNPQRSTVWXYZ` 经 `strings.ToLower` 得到的正是 `ulidLower` 里的 `enc` 常量，逐字符一一对应。

**为什么是债**
影响很小（PID 生成不在热路径），但它是"同一个需求在同一个仓库里有 26 行版和 1 行版"的直接证据。
真正的成本是阅读成本：读 `internal/proc` 的人会停下来想"为什么不能用 `strings.ToLower`？是不是有 Unicode 陷阱？"——
答案是没有陷阱，只是先写的那份没想到。

**建议**
`ulidLower` 改为 `strings.ToLower(u.String())`，删 `bytePos`。
`internal/proc/proc_test.go` 若有针对映射表的测试，保留它作为等价性回归。

**量化 / 风险**：净删 **25 行**。changeRisk **low**——有现成的 `proxysub` 实现作为等价性证明，
且 `NewPID` 的输出格式完全不变（可用一个 table test 钉住）。

---

### F11 — low — 86 个只被测试引用的生产声明；其中 8 个是纯粹为"让测试非空转"而导出的计数器

**证据（分类统计）**

| 类别 | 数量 | 说明 |
|---|---|---|
| 只被**同包** `_test.go` 引用 | 47 | 理论上可以降为不导出，或搬进 `export_test.go` |
| 只被**外部** test 包引用（`test/pN`、`test/dN`、跨 internal 包） | 24 | **结构上被迫导出**——`export_test.go` 只对本包测试二进制可见 |
| 两者都有 | 15 | |

**"为测试非空转而生"的计数器**（8 个，共约 10 行）：
`broker.AuditPublisher.TruncationLossCount` (`audit_publisher.go:149`)、`.LagExceededCount` (`:268`)、
`.DeletedStreamLossCount` (`:421`)、`broker.webhookPoster.Drops` (`alert_webhook.go:192`)、
`cluster.Node.DedupCount` (`node.go:422`)、`spawnsafe.Policy.WedgedCount` (`:976`)、
`cmd/tether.lockKeeper.Renewals` (`cluster_lock_keeper.go:154`)、`broker.homeDeliveryStats` (`home_delivery.go:340`)。
它们的存在理由在源码里写得很直白，例如 `internal/cluster/fsm.go:60-70`：
*"so the forwarding-idempotency tests can assert the dedup branch actually fired (non-vacuity — the in-scope writes are SQL-idempotent so a row-count assertion alone is vacuous)"*。

**真正的测试替身住在生产文件里**：
- `internal/cli/completion_transport.go:290-315` —— `NewTestNATSTransport` + 一个 stub transport（4 个 1 行方法），共 **~26 行测试替身在非测试文件里**。只被 `test/cli_e2e/completion_test.go` 用。
- `internal/cli/completion.go:151-156` `ClearCompletionCacheForTest`（6 行）。
- `internal/broker` 的 5 个 `*ForTest` 方法（`ClusterAdminForTest` / `ClusterStateForTest` / `AppliedIndexForTest` / `RODBForTest` / `TunnelTokenLookupForTest`，共 26 行）——
  被 `test/d9/` 与 `test/d6/` 的 gated integration 测试使用。

**为什么是债（以及为什么大部分不是）**
`*ForTest` 这一组**不是债**，是 `test/pN` 外部测试树布局的必然代价：跨包测试拿不到 `export_test.go` 的可见性，
只能要求生产类型导出钩子。在 `internal/` 里这几乎没有成本（外界看不到）。这条要如实记为架构取舍而非缺陷。

真正的债是 `internal/cli` 的那 26 行**测试替身住在生产文件里**：
`NewTestNATSTransport` 与它的 stub 会被编进发布的 `tether` 二进制（虽然 RTA 显示不可达，链接器多半会剔除，但源码层面它们是生产文件的一部分）。
更实际的影响：读 `completion_transport.go` 的人会以为这个包有两种 transport 实现需要维护，
实际只有一种是产品的。这个文件 362 行里有 26 行（7%）是测试脚手架。

`NewTestNATSTransport` 只被 `test/cli_e2e` 用，同样受"跨包不可见"约束，
但正确的做法是把这个替身放到 `test/cli_e2e` 自己的 helper 里（`Transport` 是导出接口，外部包可以自己实现）。

**建议**
- 把 `NewTestNATSTransport` + stub 搬进 `test/cli_e2e/`（它只需要实现导出的 `Transport` 接口，不需要包内可见性）：生产减 26 行。
- `ClearCompletionCacheForTest`：同包测试也在用，搬进 `internal/cli/export_test.go`：生产减 6 行。
- 8 个计数器：**保留**。它们服务于一条明确的测试质量原则（断言非空转），成本每个 1 行。
  但建议统一加 `// test-observability accessor` 标记，让下一个 auditor 一眼分类，不必每次重新判断。
- 5 个 `*ForTest`：**保留**，它们的 godoc 已经写明用途和调用者。

**量化 / 风险**：净删 **32 行生产代码**（搬到测试）。changeRisk **low**。

---

## 反证：做得好的地方

这一节是净判断的另一半。以下都是具体的、可核验的：

1. **死代码率极低，且是硬数字。** RTA 全程序分析：51 个不可达函数 / 392 行 = **生产代码的 0.57%**。
   零引用声明 30 个 / 5,101 个顶层声明 = **0.59%**。
   我审过的多数同体量 Go 服务这两个数字在 3–8%。**tether 在这个维度是 top decile。**

2. **35 个 internal 包里 34 个真的进了发布二进制。** `go list -deps ./cmd/tether` 与 `go list ./...` 的差集
   只有 `internal/testharness` 一个。没有"为将来准备的空包"、没有"实验后忘了删的包"。
   对一个走了 P0→P11 + 十几个 post-1.0 增量的项目，这个纪律非常罕见。

3. **`internal/testharness` 是正确的抽象，不是债。** 189 行、6 个函数（`StartNATS` / `StartJSNATS` / `OpenDB` /
   `SilentLog` / `WaitNodeOnline` / `WaitConnect` / `FreshUserPub`），被 **15 个测试包**引用
   （`test/p4`…`test/p13`、`test/chaos`、`test/cli_e2e`、`test/security`、`cmd/tether`、`internal/broker`、`internal/jsstream`）。
   替代方案是 15 份拷贝。它作为一个不进主二进制的独立包存在，是唯一正确的做法。

4. **生产代码里 0 处注释掉的代码块。** 我用
   `grep -rnE "^\s*//\s*(if |for |return |go |defer |switch )"` 扫了全部生产文件，
   66 个命中**全部是以这些词开头的英文散文注释**（"for the replayed-JSON byte-identity…"、"return would duplicate…"），
   没有一处是被注释掉的代码。这在长期演进的项目里几乎没见过。

5. **只有 2 个空方法体**，都是合法的接口实现：`cli.noopTransport.Close`（显式 no-op transport）和
   `cluster.fsmSnapshot.Release`（`raft.SnapshotSink` 要求但本实现无需释放）。没有"待实现"的空壳。

6. **386 处 `_ = f()` 全部是有意的 errcheck 消音**，抽样检查（`rows.Close` / `msg.Respond` / `srv.Shutdown` /
   `conn.SetReadDeadline` / `rollbackExposeAllocation`）没有发现吞掉业务错误的情况。
   在一个开了 errcheck 的仓库里这是正常且必要的显式标注，不是懒惰。

7. **接口没有被滥用。** 我原以为会找到一堆"只有一个实现的接口"（过度抽象的经典症状）。
   实际情况：AST 扫描报出的 28 个"零引用方法"**全部**是真接口实现
   （`raft.FSM` / `raft.SnapshotSink` / `raft.StreamLayer` / `error` / `io.Reader` / `cluster.Applier` /
   `agent.ExposeAdapter` + 三个可选能力接口 `sessionStateHookSetter` / `homeApplier` / `homeSessionChecker` /
   `cli.Transport`）。
   而且 `internal/cluster` 用 `var _ Applier = clusterNodeUpsertApplier{}`（`membership_ops.go:168`）、
   `var _ raft.FSM = (*fsm)(nil)`（`fsm.go:409`）等 4 处编译期接口断言把契约钉死——这是正确的 Go 习惯。

8. **"故意不接线"和"忘了删"被明确区分开了，而且写下了理由。**
   `proto.RehomeDirective`（`messages.go:192-203`）用 12 行说明为什么它现在没有 publisher、
   主路径是什么（agent-self-driven rehome）、将来 D7 接线时为什么 wire 已经稳定。
   `clusterupgrade.AgentUnknown`（`plan.go:24-27`）零引用但是 `iota` 零值且**fail-closed 语义是载荷**——
   删掉它会静默改变后面所有枚举值。
   `cluster.fsm.go:80-89` 有一段 10 行的 INVARIANT 注释，禁止给 `applied*` 结果类型加 `Error()` 方法，
   并记录了曾经有人加过、导致 D7 forged-sig poison-skip 路径静默失效。
   **一个能把"为什么这段代码看起来该删但不能删"写下来的仓库，比一个没有死代码的仓库更难得。**

9. **上一轮 audit 的清理是真的落地了。** 06-deadcode-drift.md 的 13 条里 11 条完全闭合：
   `internal/frpmgr` 悬空指针没了、`lands in P<N>` 相位标签全仓 0 命中、`var _ = errors.New` 占位符删了、
   `architecture.md:1391/2124` 加上了"已被 `internal/tunnel` 替换"的注解。
   这说明 audit → 修复的闭环在这个项目里是有效的（这也是为什么 F6 的"决定不修"反例值得记下来）。

10. **`internal/proto` 的 wire 面很克制。** 2,042 行生产代码承载 3 个角色 × ~40 个动词的完整协议，
    死掉的消息类型只有 2 个单行（`ProxySubListReq` 空 struct、`RehomeDirective`）。
    对比同类项目里常见的"每个版本留一份旧 message 类型"，这里一份都没有。

---

## 本质 vs 偶然复杂度拆解

**本 lane 能给出的定量结论：偶然复杂度中"死代码 / 重复 / 测试专用"这一类，占生产代码的 1–1.5%。**

| 类别 | 行数 | 占 68,328 的比例 |
|---|---|---|
| RTA 不可达函数（死 + 仅测试可达） | 392 | 0.57% |
| 仅测试用的独立包 `internal/testharness` | 189 | 0.28% |
| 零引用的类型/常量/字段（函数以外） | ~45 | 0.07% |
| 语义重复的实现（`hashToken` ×3、`ulidLower`、`port.Revoke` 孪生、`subhttp.Serve`、`auth.AccountSigner` 平行实现） | ~160 | 0.23% |
| 住在生产文件里的测试脚手架（`internal/cli`） | 32 | 0.05% |
| **合计** | **~818** | **~1.2%** |

去掉重叠计数（`port.Revoke` 等已计入 RTA 不可达），实际约 **700 行 / 1.0%**。

**估计 essential ≈ 97%。** 拆解如下：

- **~97%** 是活代码：从 `main` 可达、有生产调用者、承担真实职责。它是否"本质"取决于问题域——
  NAT 穿透 + Raft HA + auth_callout + 文件传输 + PTY + 隧道数据面 + 集群生命周期，
  这七个子系统每一个都是分布式系统里的重活。**这部分的 essential/accidental 判定不在本 lane 的证据范围内**，
  由 concurrency / broker-decomposition 等 lane 回答。我只能说：它们不是死的。
- **~1.2%** 是本 lane 能确证的偶然复杂度：死代码、重复实现、测试脚手架。
- **~1.8%** 是灰色地带：86 个测试专用声明中那些"结构上被迫导出"的部分（`*ForTest`、外部测试树的可见性代价）。
  这是 `test/pN` 外部测试树布局带来的偶然复杂度，但**换掉它的代价（把 e2e 测试全搬进 internal 包）远大于收益**，
  所以实际上应该算作已付清的、合理的架构税。

**一句话回答用户的原问题（就本 lane 而言）**：
16 万行里的 6.8 万行生产代码，**只有约 700 行是可以删掉的**。
如果这个项目臃肿，原因不在"堆了没用的代码"——它在这方面异常干净。
本 lane 唯一值得担心的不是体量，是那几个**活得比自己的用途更久、还带着误导性文档的幸存者**（F1–F5）。
它们加起来不到 200 行，但每一条都能让下一个改这块的人做出错误决定。

---

## 附：完整清单产出

本报告的原始扫描输出（不入库）：
- RTA 不可达函数全表（51 项，含精确行跨度）
- 零引用声明全表（58 项，含 28 项接口实现的假阳性标注）
- 测试专用声明全表（86 项，按"仅同包测试 / 仅外部测试 / 两者"三分）

扫描方法可复现：`golang.org/x/tools/cmd/deadcode@v0.42.0` + 一个基于 `go/packages` 的引用计数脚本
（`Tests: true` + 全部 8 个 build tag）。**重跑时务必带 build tag**，否则 `internal/broker` 的 5 个
`*ForTest` 方法会被误报为完全死代码。

---

## 核验附记（补位 agent）

> 2026-07-25 · 本节由独立的补位 agent 追加。原报告的撰写 agent 在写完全文后、返回结构化摘要前因上下文耗尽而崩溃，
> 其结论未经任何人核验、也未进入下游综合阶段的输入。本节补上核验，**只追加、未改动上文任何一个字**。
> 手段：`golang.org/x/tools/cmd/deadcode@v0.42.0`（离线重编，同版本）+ 自写 `go/packages` 引用计数器
> （`Tests: true` + 全部 8 个 build tag，且**修正了一个原报告可能也踩过的坑**：`packages.Load` 会为带包内测试的包
> 生成 `pkg [pkg.test]` 变体，`types.Object` 身份不同——按对象身份统计会漏掉全部包内测试引用，必须把同 path 的
> 所有包实例的对象都登记为目标）。未跑任何测试、未改任何实现代码。

### 一、量化结论：逐位复现，无一虚构

原报告的所有硬数字都独立重算过，**全部精确命中**：

| 断言 | 原报告 | 复核 | 结论 |
|---|---|---|---|
| 生产代码行数（`internal/`+`cmd/`，排除 `_test.go`） | 68,328 | 68,328 | ✅ 精确 |
| RTA 不可达函数数 | 51 | 51 | ✅ 精确 |
| 不可达函数合计行数 | 392 | 392 | ✅ 精确（按 `FuncDecl` 起止行逐个求和） |
| 生产代码空方法体 | 2 | 2（`cli.noopTransport.Close`、`cluster.fsmSnapshot.Release`） | ✅ 精确 |
| `_ = f()` errcheck 消音 | 386 | 386 | ✅ 精确 |
| 不在 `cmd/tether` import closure 内的 internal 包 | 仅 `internal/testharness` | 仅 `internal/testharness` | ✅ |
| `internal` 包总数 | 35 | **36** | ⚠️ 差 1 |
| `internal/testharness` 行数 | 189 | **239** | ⚠️ 少算 50 行 |

对一个"上下文将崩"的 agent，这个命中率本身就是它没有编数字的证据。两处偏差都是小数点后的事，
但**"偶然复杂度合计"表要跟着修**：`189 → 239` ⇒ 合计 `818 → 868` 行（1.27%），去重后 `~700 → ~750` 行（**~1.1%**）。
净判断（"死代码不是臃肿的原因"、bloatScore 2/10、verdict minor-debt）不变。

### 二、逐条核验结果

**F1 — UPHELD。** `auth.LoadAccountSigner` / `IssueUserJWT` / `AccountPublicKey` / `DecodeUserJWT` 引用计数
`prod=0, test=8`（`internal/auth/jwt_test.go` ×6、`test/p1/foundation_risk_test.go` ×2）；deadcode 也把 4 个全部列为 unreachable。
生产路径的两处对照证据都成立：`internal/broker/authcallout.go:70` 直接 `nkeys.FromSeed`；
`cmd/tether/serve.go:438 loadAuthCalloutSeeds` 只读文件不校验 key kind。
关键的差异断言也成立——`internal/auth/jwt.go:49` 有 `nkeys.IsValidPublicUserKey(userPub)` 前置校验，
生产签发路径 `internal/authcallout/handler.go:427-448 (*Handler).allow` **没有**。
*订正*：`internal/auth/jwt.go` 是 **92** 行不是 95 行（`wc -l`）。

**F2 — UPHELD，但建议里有个不能机械照做的地方。**
`port.Revoke` `prod=0, test=6`（`internal/port/port_test.go:150/180/232/342/370` + `test/cluster/equiv_test.go:422`）；
`port.PlanRevoke` `prod=0, test=3`。生产侧确实只用带整行身份的孪生：`RevokeAllocation` ← `internal/broker/clusterwrite.go:897`，
`PlanRevokeAllocation` ← `internal/broker/clusterwrite.go:906` + `internal/broker/cluster_forward.go:618`。
`Revoke` 的 godoc（`internal/port/port.go:401-403`）确实写着 "used by the broker reconciler …" 而 reconciler 不用它——**陷阱成立**。
*订正 1*：行号是 `port.go:401-428`（不是 399-425）与 `plan.go:73-77`（不是 74-77）；净删应是 **~33 行**（Revoke 28 + PlanRevoke 5），不是 28。
*订正 2*：`PlanRevoke` 删掉后 `planPortStateChange`（`plan.go:298`）仍被 `PlanFree`（`plan.go:28`）用，**不能一起删**。
*补充风险（原报告未提）*：两者语义**不等价**——`Revoke` 在 `RowsAffected==0` 时会回查该 port 是否存在，
存在则返回 `nil`（幂等）；`RevokeAllocation` 直接返回 `ErrNotFound`。所以"把测试调用点改到 `RevokeAllocation`"
不是纯机械替换，`port_test.go:180/232/342/370` 那几处依赖的正是幂等语义。建议照做时需逐点确认，不能 sed 全替。

**F3 — 4 条 UPHELD，第 5 条（`SubjClusterWildcard`）DOWNGRADED。**
`SubjVersionAnnounce` / `SubjNodeUnregister` / `SubjEvNodeState` / `SubjPtyReady` 四个引用计数全部 `prod=0`；
按**字面 subject 串**（不只按符号）复扫也没有任何 publisher/subscriber：
`ctrl.version.announce` 全仓只出现在 `internal/auth/permissions.go:35/136/227` 与 `internal/proto/subjects.go:10`；
`unregister.req` 只出现在 `permissions.go:170/250` 与 `subjects.go:75`；
`internal/broker/broker.go:960-1013` 的订阅表里确认没有 `.ev.node.*.state`（只有 `proc.*`/`transfer.*`/`proxy.*`），
也没有 `.pty.*.ready`（只有 `.pty.*.failed`）。
"PTY ready 是 `RunChunk` 的 chunk kind、走 `msg.Reply` 而非独立 subject"也核实无误
（`internal/agent/run.go:130-132` 发 `proto.RunChunk{Kind:"ready"}`，`cmd/tether/run.go:152` `case "ready":`）。
git 溯源两条也精确：`git log -S "SubjNodeUnregister("` 命中 `ecacde6`/`d7241a9`/`55b1451`（最后一条即移除），
`SubjVersionAnnounce` 只有 `ecacde6` 一条。
**DOWNGRADED 的那条**：`proto.SubjClusterWildcard` **不是"未用常量"**——它被
`internal/auth/permissions_test.go:246-247` 用来断言 `proto` 侧 SSOT 与 `internal/auth/permissions.go:232-233`
的 ACL 字面量不漂移。按原报告"删常量即可"执行会**删掉一条活的 drift guard**。正确做法是保留该常量，
或反过来让 `permissions.go` 直接引用它（那才是真正消除重复）。
*订正*：要摘的授权行是 **7** 行（`35/136/227` + `170/250` + `140/174`），不是 6 行。

**F4 — UPHELD。** 三个字段引用计数全部 `prod=0, test=1`。构造点逐一读过：
`internal/broker/exec.go:428-435 pubAuditCall` 不设 `ActorName`；
`internal/broker/exec.go:446-457 pubAuditProc` 在 `kind=="exit"` 分支只设 `RC`；
`internal/broker/expose.go:523-527 pubAuditPort` 签名只有 `actorFP`，写的是 `ActorFp` 而非 `ActorNkey`。
上一轮修 `ReqID`/`Target` 的注释也确实就在 `internal/broker/exec.go:419-422`。
*措辞订正*：原文"『unless exit』的那个 exit 永远不发生"不准确——`kind=="exit"` 分支是会走的（它设了 `RC`），
真实情况是 `DurationMs` **在任何分支都没被赋过值**。结论不变，但表述会误导下一个读者去查"为什么 exit 事件不发"。

**F5 — UPHELD。** `ExposeReq.ActorFP` / `ExposeRmReq.ActorFP` / `UpgradeReq.ActorFP` 三个字段
在生产代码里既不写也不读（`prod=0`，仅 `internal/proto/proto_invariants_test.go` 等形状测试引用）；
`internal/broker/expose.go:277` / `internal/broker/upgrade.go:115` 设的确实是 `*ForwardedReq` 上的同名字段。

**F6 — UPHELD，而且"逐字节相同"是可以钉死的。** 三份函数体一字不差，全是同样两行：
`sum := sha256.Sum256([]byte(raw))` / `return hex.EncodeToString(sum[:])`
（`internal/port/port.go:470-473`、`internal/tunnel/tunnel.go:1274-1277`、`internal/proxysub/proxysub.go:205-208`）。
"tunnel 那份的 godoc 提到 shard 06 F10 决定用注释解决、proxysub 那份只写 same scheme as port tokens 而不知道第二份存在"
也逐字核实。这条是本报告里证据最硬的一条。

**F7 — UPHELD。** `clusterspec.NodeSpec` 的 `RaftAddr`/`NatsRoute`/`TunnelAddr`/`CertFP` 四个字段引用计数**全部为 0**
（含测试、含全部 build tag）；`internal/clusterspec/spec.go:111` 生成的 Verb 串确实仍带着字面 `…`。

**F8 / F9 — UPHELD。** 抽查的 14 个符号引用计数全部为 0：
`session.Member`、`proto.RehomeDirective`、`natsconf.Ownership.ServerName`、`spawnsafe.Policy.IsPathDead`、
`agent.IsPathValidationError`、`ssproxy.Server.LocalPort`、`xferaudit.TransferReqID`、`subhttp.LiveProxyNodes`、
`proto.ProxySubListReq`、`proto.ClusterGrowSchemaVersion`、`adminsock.CodeAlreadyVoter`、
`serveconf.NATSSection.WssListen`、`.WSInternal`、`agent.ProxyState.CertPins`。
`subhttp.Serve` 为 `prod=0, test=1`（`internal/subhttp/p13_external_review_test.go:19`），与 F8 描述一致。
**`RehomeDirective` 那句假保证也确认为假**：`grep -rn RehomeDirective --include=*.go` 全仓 3 个命中，
全在 `internal/proto/messages.go`（163 注释 / 192 godoc / 204 类型定义），**没有任何测试引用它**，
所以 `messages.go:202-203` 那句 "a guard test asserts it has no live publisher so a half-wiring is caught (review A5 M5)" 是空头支票。
`clusterupgrade.AgentUnknown` 引用计数也确为 0，原报告把它判为"必须保留的 iota 零值"是对的。

**F10 — UPHELD。** `ulidLower`/`bytePos` 各有 1 个生产引用（互相调用），语义等价性成立：
`ulid.String()` 用 Crockford 大写表 `0123456789ABCDEFGHJKMNPQRSTVWXYZ`，`strings.ToLower` 后逐字符等于
`internal/proc/proc.go:63` 的 `enc` 常量。净删应为 **~23 行**（`ulidLower` 16 + `bytePos` 8 − 替换 1），
原报告的 25 略高一点点，量级正确。

**F11 — 主体 UPHELD，但其中一条具体建议 REFUTED。**
主体核实无误：`cli.NewTestNATSTransport` `prod=0, test=1`（`test/cli_e2e/completion_test.go:35`）；
5 个 `*ForTest` 方法全部 `prod=0` 且只被 `test/d9/` `test/d6/` 引用；
`cluster.Node.DedupCount`（5 test）、`spawnsafe.Policy.WedgedCount`（9 test）等计数器确为测试可观测性钩子。
**REFUTED 的是这一句**：*"`ClearCompletionCacheForTest`：同包测试也在用，搬进 `internal/cli/export_test.go`：生产减 6 行"*。
实测引用为 `prod=0, test=31`，且分布在**两个不同的测试二进制**：`internal/cli/completion_test.go`（17 处，包内）
**与 `test/cli_e2e/completion_test.go`（14 处，外部测试树）**。`export_test.go` 只对包内测试二进制可见——
**照此执行会让 `test/cli_e2e` 编译不过**。讽刺的是原报告在同一条 finding 里刚刚正确陈述过这条可见性规则
（"`export_test.go` 只对本包测试二进制可见"），随后自己违反了它。该符号的正确处置与 `NewTestNATSTransport` 相同：
要么保持导出（付 6 行的架构税），要么让 `test/cli_e2e` 走公开 API 自己清缓存——**不能搬进 `export_test.go`**。

### 三、原报告漏报的一类（本节最有价值的补充）

原报告用 51 个不可达函数撑起"死代码率 0.57%"的反证，然后 F9 只挑了"零引用叶子"来讲。
但把 51 条逐个做引用计数后会看到：**其中至少 6 条正是本报告自己的头号主题——"活得比自己的用途更久 + godoc 在误导人"——
却一条都没进 F1–F11**。这不是数字错误，是取样偏差：报告挑了最小的那些，漏掉了最大的和最像 F1/F2 的那些。

| 符号 | 位置 | 引用 | 为什么它就是 F1/F2/F8 的同一形状 |
|---|---|---|---|
| `session.BumpProxyEpoch` | `internal/session/session.go:297-303` | prod=0, test=10 | godoc 写 *"used after enable / sub create / sub revoke"* —— **生产一个都没有**，已被同文件 `SetProxyEnabledAndBumpEpoch` 取代。**与 F2 逐点同构**（被取代的旧孪生 + godoc 指错路），严重度不低于 F2 |
| `spawnsafe.Policy.IsPathDead` | `internal/spawnsafe/spawnsafe.go:885-891` | 0 | godoc 写 *"Exposed for the agent-liveness guard (Component I): the agent checks its own Home before a blocking state.json read"* —— agent 从不调它。原报告只把它当 F9 表里一行无名 nit，**没看出它带着一句假的生产用途声明** |
| `spawnsafe.Policy.SafeDir` / `.RunStart` | `spawnsafe.go:876` / `:909` | prod=0, test=13 | `RunStart` 是 `RunStartWithCleanup`（生产在用）的 1 行便利孪生；`SafeDir` 只有测试读。F8 同类 |
| `broker.Broker.repairProxyEpoch` | `internal/broker/proxy.go:676-678` | prod=0, test=2 | godoc 自陈 *"kept for direct callers / tests"* —— **`internal/` 包里"为直接调用者保留"的又一例，正是 F8 点名要杀的措辞**，原报告只在 `subhttp` 里抓到两处，没做全仓扫 |
| `broker.newAutoRebalanceArm` | `internal/broker/proxy_auto_rebalance.go:40` | prod=0, test=5 | 死构造器：同文件 `tick()` 的注释自己写着 *"zero-value arm (a Broker struct field) is usable without newAutoRebalanceArm"* —— 机制是活的，构造器是死的 |
| `broker.ReconcileReqID` + `rcKey` + `writeSeg` | `internal/broker/cluster_forward.go:309-380` | `ReconcileReqID` prod=0/test=11；后两者只被前者调 | **~40 行、3 个函数的仅测试可达簇，整体大于 F9 表里任何一条**，却完全没被点名 |

这 6 条合计约 **75 行**，且其中 `BumpProxyEpoch` 的严重度（被取代的孪生 + 撒谎的 godoc）**与 F2 同级**。
含义：报告"这个仓库异常干净"的判断依然站得住（75 行仍是 0.1%），但它对**自己发现的那条规律**
（"问题不在体量，在于活下来的死符号在误导人"）的**覆盖是不完整的**——真实数量比 F1–F5 列出的多一半。
下一次修这块时，应该以"51 条不可达 × 逐条读 godoc"为清单，而不是以 F9 那张零引用表为清单。

### 四、净结论

- 原报告**没有幻觉**。所有可精确复现的数字（68,328 / 51 / 392 / 2 / 386 / 1 个包在闭包外）**逐位命中**，
  崩溃前的上下文压力没有污染它的结论。
- **11 条 finding 中 9 条完整 upheld，1 条部分 downgraded（F3 的 `SubjClusterWildcard` 行），
  1 条的具体建议被 refuted（F11 的 `ClearCompletionCacheForTest` → `export_test.go`，照做会编译失败）。**
  另有若干行号/行数的小订正（jwt.go 92 行、`Revoke` 401-428、净删 ~33、testharness 239 行、internal 包 36 个、授权 7 行）。
- **"可删行数"从 ~700 修正为 ~750**（testharness 189→239 行），再加上第三节漏报的 ~75 行 ⇒ **~825 行 / 1.2%**。
  但要区分两个桶：这 ~825 行里真正"直接删掉即可"的约 **260 行**（F2 33 + F3 ~20 + F8 13 + F9 ~70 + F10 23 + F11 32 + 漏报 75），
  其余是 `internal/testharness`（239 行，报告自己论证过是**正确抽象**，不该删）与 `AccountSigner`（95 行，
  建议是**接线**而不是删）。任何"能删 800 行"的复述都是对本报告的误读。
- 对总问题的贡献：**这一 lane 是"不是屎山"最硬的一条反证，且经复核成立。** 死代码率 0.57%、
  36 个 internal 包只有 1 个不进二进制、0 处注释掉的代码、2 处空方法体——这些是可复现的硬指标，
  在同体量长演进 Go 服务里属 top decile。tether 若臃肿，原因必须到别的 lane 去找（职责分解 / 抽象层数 / 测试树布局），
  **不在"堆了没用的代码"**。
