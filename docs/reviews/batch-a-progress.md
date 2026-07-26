# 批次 A 实施进度（step 3–5 完成，停在外审门）

> 依据：`docs/reviews/batch-a-plan.md`。每项自成一块，工作树在每个 ✅ 处均为一致状态。
> **未提交** —— 按流程要在 step 4 内审 + step 6 外审通过后才 commit。

## 已完成

### ✅ V1 前置验收（plan §2.1，roadmap 的 L3 判断修订）

扫描 `test/simcluster/drills/` + `test/e2e/` 的退出码数值断言，共 3 条，逐条确认**不受 A1 影响**：

| 断言 | 来源 | 结论 |
|---|---|---|
| `50-backup-restore.sh:197` `[ "$_drc" = 64 ]` | `cluster_natsconf.go:588` `usageErr(...)` — CLI 本地构造 | 不经分类器 |
| `82-agent-onboarding-invite.sh:90` `[ "$_drc" -eq 77 ]` | `agent_doctor.go:49` `&ExitError{Class: exitNoPerm}` — CLI 本地构造 | 不经分类器 |
| `71-expose-rehome-failover.sh:101,289` rc=75 | `dataplane_not_converged` — **已在** exitClasses 表中 | A1 不改动 |

⇒ **硬边界 3 成立，本增量不需要跑 simcluster drill。**

### ✅ A1 — 错误码 exit-class SSOT（Step 1/2/3 完成，Step 4 待做）

**Step 1** `internal/proto/codes.go`（新增）
- **32 个** NATS-wire 码常量。**adminsock 的 18 个 Code\* 不重复声明**——那是另一条传输的 SSOT，重复即制造第二个真相源（A1 要消灭的正是这个）。
- `messages.go:216/227` 已有的 `ReasonHomeCatchingUp` / `CodeLeaderUnavailable` **保持原位**，doc 里显式指向，不搬动。

**Step 2** 分类 **62 个**此前无 exit class 的码（原估 27，实测远多；其中 8 个显式归 70 ⇒ 行为真正变化 54 个）
- → **77**：`path_outside_roots`、`unauthorized`
- → **75**：10 个自愈瞬态（`attach_timeout`/`path_race`/`remote_fs_*`/`forward_failed`…）
- → **64**：42 个 operator-action-required
- → **70（显式）**：8 个确属我方 bug/版本 skew，写进表以区分"判过"与"没人看过"
- → **allowlist（5 个）**：`bucket_create_failed`/`home_broker_restart` + 三个 catch-all
  （`alloc_failed`/`io_error`/`object_put_failed`）。磁盘满与瞬时 EIO 落进同一分支、无从区分，
  猜 75 就会让监控无限重试满盘——照 `bucket_create_failed` 既有先例，70 是诚实答案。

**最危险的一处发现**：force-single 的**全部**拒绝码此前都退 70——
`peer_alive`（已确认死亡的 peer 竟回应了探测）、`quorum_not_lost`、`force_single_refused`、
`arm_expired`、`is_leader`。force-single 是能终结一个集群的操作，而 §9.13 告诉自动化"70 可重试"，
即"继续重试那个因为对端还活着而被拒绝的 force-single"。五个全部归 64。

**Step 3** `cmd/tether/error_code_coverage_test.go`（新增，5 个测试全绿）
- 扫描器覆盖 4 种发射形态（`Code:` 字面量 / 具名常量 / 12 个 code-carrying helper 的实参 / `Reason:` 字段），
  并**如实声明**它覆盖不到的形态（返回值链传播在语法上不可判定）。
- `TestErrorCodeCoverageSelfCheck` 为每种支持的形态各合成一个样例——没有它，扫描器退化成匹配不到任何东西时会报出一个毫无意义的"0 个未分类"。
- `TestCodeCarryingHelperListIsComplete` 从源码反推 helper 名单，**实跑中抓到了两个漏登记的**
  （`pubPtyFailed`/`replyRunFailed`）和一个已不存在的（`failAndFinalize`）。
- `TestAllowlistEntriesStillHaveEmitters` 防止豁免表腐烂成墓地。
- `TestErrorCodeScannerDeclaresItsLimits` 钉住"不夸大覆盖范围"这条自我约束。

验证：`go test ./cmd/tether/` 全包 70s 通过，无回归。

**待做**：Step 4（消 40 处 `"<code>: "+err` 拼接；按决策 D7 需先分类哪些进 stderr）。

### ✅ A2 — `handleCapsReq` 改调 `transferGate`（-16 行代码）

按决策 D8 **反向做**：删掉内联的 `Error: err.Error()`，改调现有 gate，**不改 gate 签名**。
改签名会给 push/pull/commit/finalize 四条 RPC 新增原本不存在的裸 SQLite 错误串
（db 路径、表名、约束名）发给任意 session member。

确立通则：**`store_error` 的 DB 明细一律只进 broker 日志、不上 wire。**

验证：`go build` + `go test ./internal/broker/ -run Caps|Transfer` + `go test ./cmd/tether/ -run Caps|G67` 全绿。

### ✅ A3 — `pollUntil` 收口 + `--wait` 响应 Ctrl-C

**实施中发现第三种超时语义**（plan 只记了两种）：`cmd/tether/cluster_upgrade_drive.go:406`
**已经有一个 `pollUntil`**，其超时返回 `unavailErr`（**69**）、ctx 取消返回裸 `ctx.Err()`（落 **70**）。
连同 `waitForOp` 的 75 和 `cluster reconcile nats` 的 70，**同一件"我不等了"有三个退出码**。

因此没有新建第二个原语（那正是 A3 要消灭的），而是：
- 新增 `cmd/tether/poll.go` 的 `pollUntilStep(ctx, interval, deadline, step)` 作为唯一实现；
- 旧的 `pollUntil(ctx, timeout, pred, msg)` **保留签名**、改为薄委托，3 个 upgrade 调用点一字未动；
- `waitForOp`（`cluster_join.go:169`）与 `cluster reconcile nats --wait` 改用新原语。

行为变化（均为修正）：
| 场景 | 之前 | 现在 |
|---|---|---|
| `join approve --wait` / `retire --wait` 按 Ctrl-C | **完全无反应，只能 SIGKILL** | ≤1 tick 内退出，75 |
| `reconcile nats --wait` 超时 | 裸 error → 70（"tether bug，可重试"） | 75 |
| upgrade 三处 convergence wait 被取消 | 裸 `ctx.Err()` → 70 | 75 |
| upgrade 三处 convergence wait 超时 | 69 | **69（刻意不变）** |

测试 `cmd/tether/poll_test.go`（4 个，全绿）：取消在 **interval=30s 时仍须 ≤2s 返回**（只在下一 tick 才
察觉取消的实现会失败）、deadline 必须在 step **之后**检查（否则恰好在 deadline 完成的 join 会被误报
"still in flight"）、step 错误原样透传不得被吞成超时、wrapper 的 69/75 分别钉住。

验证：`go build` + `go vet` + `go test ./cmd/tether/` 全包 69s 通过。

### ✅ A4 — 死符号与误导性 godoc 扫除（按危害排序，非行数）

`deadcode` 带全部 8 个 build tag 重跑，逐条核实引用后处置：

| 目标 | 处置 | 依据 |
|---|---|---|
| `port.Revoke` / `PlanRevoke` | **保留函数，重写 godoc** | 实测 **6 处**引用含跨包 `test/cluster/equiv_test.go:422`（单机 vs 集群等价性对照臂），且幂等语义与 `RevokeAllocation` 不等价、被 4 个测试依赖。真正的危害是那句假 godoc，不是函数本身 |
| `session.BumpProxyEpoch` | 同上 | 与 `port.Revoke` **逐点同构**：非事务旧版被 `SetProxyEnabledAndBumpEpoch`（round-6 F6 原子版）取代，godoc 却仍写"used after enable / sub create / sub revoke"。11 处测试引用，不可删 |
| `broker.repairProxyEpoch` | 同上 | godoc 自称 "kept for direct callers / tests"——internal 无外部调用者，"direct callers" 无意义；改为点名那两个测试文件 |
| `subhttp.Serve` | **删除**（-14 行） | 生产零引用，唯一使用者是护栏测试。按决策 D10 **同一改动内**把 `TestExternalReviewServeRejectsNonLoopbackAddress` 重定向到 `Bind`（loopback 拒绝本就发生在 Bind 内），护栏无空窗 |
| `subhttp.LiveProxyNodes` | 保留，删假理由 | "kept exported for back-compat" 在 internal 闭包世界里不成立 |
| `proto.RehomeDirective` | **补上缺失的测试** | godoc 自 A5 M5 起声称"a guard test asserts it has no live publisher"，而**该测试从不存在**。已新增 `TestRehomeDirectiveHasNoLivePublisher`（带非空转自检）。D7 接线后它会转红，那是删除信号 |
| `auth/permissions.go:42-46` | 订正（决策 D11） | 承诺 tetherd "replies admin_denied for non-owners"——`kick`/`rotate-pin` 根本没有 handler，无从回复 |
| `xferaudit/plan.go:49` | 订正 | 文档写 `TransferReqID`（粗粒度键），实际是 `TransferRecordReqID`（内容寻址键，#57 崩溃恢复的基石） |

### ✅ A6 — account seed kind 校验接线

按决策 D13 **只取第一方案**：`loadAuthCalloutSeeds` 调 `auth.LoadAccountSigner` 校验后丢弃 signer，
**签发路径一字节不动**（第二方案会丢 `uc.Audience`，单 broker 下 = 全员连不上）。

新增 `cmd/tether/serve_authseed_test.go`：`test/p1` 那条测的是 auth 包单元行为、够不到 `package main`，
所以从来没人验证过**生产路径是否调用了这个守卫**（答案是没有）。新测试断言接线本身，
并要求错误串点名 `account.nk` 与 `ACCOUNT seed`——这个故障是运维凌晨三点遇到的，不是 Go 开发者。

### ✅ A11 — `internal/tokenhash` 收口（**四份**，非三份）

新建零依赖叶子包。`internal/port` / `internal/tunnel` / `internal/proxysub` /
**`cmd/tether/transfer.go:876` 的 `hexSHA256`**（其 doc 自称 "the canonical"，却不知另三份存在）全部委托。

不复用已导出的 `proxysub.HashToken`，因为 `proxysub` 带 `database/sql`+`crypto/rand`，
而 `tunnel` 要保持 dep-graph leaf——**"现成的那份住在错误的包里"才是新建包的理由**。

`internal/tokenhash/tokenhash_test.go` 用**外部可验证的固定 digest**（非与另一个 Go 表达式对比，
那会与被守护的代码一起漂移）钉死算法：这些值持久化在 `port_allocations.token_hash` 里，改了就是数据迁移。

### ✅ A13（第一步）— socket 上拒绝 `drain --retire`

`handleDrain` 收到 `Retire:true` 直接返回 `CodeBadRequest` 并指向 `cluster retire`。
产品路径本就不可达（`cluster.go:524` 硬编码 false），但 socket 一直接受该字段——
而那条分支执行 `RemoveServer` + roster 删行（**不可逆 raft 成员变更**）却没有 `opStillLive`
TOCTOU 复查、没有可复制 deadline、没有 BLOCKED 逃生口、不可 resume。

新增 `internal/broker/a13_drain_retire_test.go` 两条：拒绝须带 code 且**点名替代动词**
（只说"不行"的拒绝会被绕过）；非 retire 路径必须**穿过**该门——用裸 backend 在下游 nil 依赖上
panic 来证明控制流确实越过了门（retire 那条从同一个零值 receiver 干净返回，正因为它先停下）。

### ✅ A13（第二步）— 删除 retire 分支本体

`DrainNode` 的 retire 主体、`streamsReady` 门、`ErrStreamsNotAtTarget` 全部删除。
**签名保留**（`retire` 参数仍在）：8 个调用点与 adminsock 形状因此一字未动，
传 `true` 现在返回一个点名 `tether cluster retire` 的错误。

两个测试改期望：
- `internal/broker/clusterstatus_test.go` 的最后-voter 拒绝断言 → 改断言"同步路径已拒绝"
  （最后-voter 守卫本身现在只存在于 `StartRetireOperation`，即唯一的 retire 入口）
- `test/d7/integration_test.go` 原本驱动同步 retire 的整段 → 改为断言拒绝 + 措辞。
  完整的 retire 状态机本就由 `cluster_operation_controller_test.go` 覆盖
  （它经 `driveInFlightOperations` 驱动，含最后-voter 路由到终态 RETIRE_FAILED）。
  从外部测试包够不到那个未导出方法，重复断言也不是这里丢失的东西——丢失的是同步路径本身。

验证：`go test -tags d7_integration ./test/d7/` **独立重跑 3 次全绿**、`make lint` 0 issues、`make test` 全绿。

**一次 e2e flake 的判定记录**：改完 A13 后的那轮 `make e2e` 里 `TestD7Matrix/FollowerStatusViewSource`
失败于 `AddNode: phase-1 roster admission: node is not the leader`。判为 **flake 而非回归**，依据：
该子测试单独重跑 3 次全绿、全 d7 套件重跑 3 次全绿，且失败形态是选举时序
（`not the leader`）而非我改动触及的 retire 路径——A13 删的分支在这条测试路径上根本不执行。
CLAUDE.md 也记录过 e2e 满负载下这类时序 flake 是已知现象（正是 e2e 刻意串行的原因）。
最终一轮覆盖全部改动的 `make e2e` **全绿（15/15 矩阵，含 `TestD7Matrix`）**，flake 判定得到确认。


### ✅ A7 — ACL 双向对账 + 删死授权

**先答了 plan 决策 D15/D16 的三问**（A7 是全批次唯一 `git revert` 撤不回效果的一项）：
- 已签发 JWT 的 TTL = **24h**（`authcallout/handler.go:119`）⇒ 增删授权都在一个 TTL 内最终一致，新旧客户端并存
- `jsstream` 全仓只建三个 stream（sys events / per-session history / placement canary），**都不覆盖**这些 subject
  ⇒「NATS core 无订阅者即丢弃」的前提成立，未订阅的 publish 不落盘

新增 `internal/auth/acl_reconcile_test.go` 做**双向**静态对账（AST 提取授权模板 × broker 订阅表 + NATS 通配语义匹配）。
调试中修掉三类假阳性——只扫 `broker.go` 单文件、漏扫 `internal/proto` 的常量、`SubjectPrefix` 在 proto 包内是
`Ident` 而非 `SelectorExpr`。**假阳性必须清零**：一个会误报的闸门会训练读者忽略它，而那正是它要防的失败模式。

清完后剩**一个真发现**，加上已知两条共三条死授权，全部删除：
`session.<sid>.kick.req` / `rotate-pin.req` / `s.<sid>.node.*.tag.req`。
`docs/requirements.md:193` 同步降级为「未实现」——**规格与 ACL 同时撒谎比诚实的缺口危险得多**。
`knownUnsubscribedGrants` 保留机制但**清空**：合法情形（授权先于 handler 落地一个 release）会复发，
但加条目必须是带书面理由的自觉动作。

### ✅ A9 — `fenceSnap` 收口 tunnel 三元比较链

一个快照点 + **两条**逐字重复的 `||` 链 → `fenceSnapLocked` / `fenceChangedLocked`。
三个维度分别来自 round-2 F1 / round-5 F1 / round-6 F4——**连续三轮外审各发现 fence 少一维**。
加第四维（多 home 的 per-nid 完全可能）在旧形状下要改三处，且**漏掉两条链之一能干净编译**，
后果是已被 kill 的公网 exit 被 REGISTER 竞态复活并重新 bind 端口。
`s.closed` 刻意不并入（它是服务器生命周期不是 fence 维度）。`-race` 绿。

### ✅ A10 — `finalizeTransfer()`（范围按 D17 收窄）

只合并真正同构的两处（watchdog + 失败路径）。`cancelEntry` 做成**显式参数**：
watchdog 传 false 因为它**就是** `entry.cancel` 的所有者。明确**排除** `xfer_inflight.go:511`——
它的删除受 #57 的 M1 不变量约束（须 synthetic terminal *durably COMMITTED* 之后才删），
与另两处的无条件删**相反**，合并会反转保证。

### ✅ A12 — `internal/httplisten`（**三个** Bind）

`clustermanifest` 的三处漂移全部消除：裸 goroutine watcher（`Serve` 先返回时它活过函数返回）、
`srv.Close()` 硬关掐断在途请求、`!=` 而非 `errors.Is`。
**loopback 决策留在调用方**：`/sub` 与 manifest 是未认证 + Caddy-fronted 必须 loopback，
`/metrics` 要被私网 scraper 抓——差异是产品决策，做成显式参数才能被检查。
`internal/httplisten/policy_test.go` 静态断言三包各自的 bool + 禁止绕过 `httplisten` 直接 `net.Listen`。

### ✅ A14 — `bannerBuilder`

三段 banner 追加各来自一个不同审查轮次，其中两段带着 *"Emit ONE banner (dedup)"* 注释——
**说明它们已经撞过一次**。分隔符逻辑收进 builder，第四条 advisory 从此是一次 `add()`。
banner 文案**字节未改**（drill 会 grep）。

### ✅ A5 — `loopSet`

手写的 `loopCount := 4 / 5` 与它下面的 `go` 语句之间没有任何联系，`_test.go` 里零命中。
两个方向不对称：**写大**只是每个幽灵槽让 shutdown 多等 10s（吵但可见）；
**写小**会让 join 提前返回，`clusterShutdownOrdered` 继续推进到 `nc.Drain`，
重演 `clusterwrite.go:132-137` 自己写明的「publish-after-Drain 静默丢审计、泄漏门抓不到」
（goroutine 确实退出了，只是退得太晚）。
`loopSet` 在创建 goroutine 的地方自计数，并把四条此前完全不可观测的循环的 liveness
（`runs` / `last_tick`）接进 `RuntimeReport`（`tether admin runtime --json`，
按决策 D24 属"加 omitempty 字段"，`schema_version` 不 bump）。
4 条测试（含 n=1/4/5/9 的 join 完整性、wedged 循环的超时上界、Count 跟随注册数）`-race` 绿。

### ✅ A15 — raft 日志 + 计数器进 /metrics

`node.go` 的 `c.LogOutput = io.Discard // D1: …; D3 wires logging` —— **D3 从未接线**。
新增 `internal/cluster/raftlog.go`（hclog→slog 适配器）：WARN+ 转发，
**按 D23 无条件加 30s 去重窗口**——不去赌 racknerd 的 raft configuration 里那个 ghost 在不在，
两种情况下日志预算都有界。leadership 当选/下台各一条 Info（没有它，raft 自己的日志没有可对齐的锚点）。
三个审计丢失计数器接进 `brokermetrics.Snapshot`——此前 `tether history` 出现空洞时，
**无法区分「当时没操作」与「审计被截断丢了」**。

### ✅ A8（主体）— 三把尺的订正

- `docs/architecture.md` 顶部加阅读须知：**70 处 `tether.v1`** vs `ProtoVersion=2`、**72 处 frp** vs go.mod 零依赖、
  Part II 是已完成清单；并指向两份真正的 v2 契约。保留而不重写，因为**取舍论证仍然有效**，失效的只是标识符。
- `CLAUDE.md §1` 补两行文档地图（`distributed-broker-architecture.md` / `deploy-tier-gotchas.md`
  被 README 和 runbook 称为契约，却在 CLAUDE.md 里 grep 计数为 0——**每个会话都因此少一把尺**）。
- `docs/usage.md §9.13` 补 A1 订正说明。全局扫确认无别处再教「70 可重试」。
- `error_hints.go` 的 `tether session list` → `session ls`（实测前者打 help 退 0——**一条跑了不报错也不做事的命令**）。

## 最终 L2 硬闸

- `make test` ✅ 全绿
- `make lint` ✅ 0 issues
- `make e2e` ✅ **全绿**（1092s，15/15 矩阵，0 FAIL；含此前 flake 的 `TestD7Matrix`）
- simcluster drill — **不适用**（V1 已证明，见上）

## 仍未做（诚实登记）

### ✅ A1 Step 4 — 拼接码的消费侧修复

33 处 `"<code>: " + err.Error()` 走 `RunChunk.Reason` / `KillResp.Error` 单字段。
**没有改 wire**（给 `RunChunk` 加 Code 字段是 wire 变更，批次 A 明令零 wire），
而是修了消费侧：`runFailureMessage` 补上冒号切分并返回带正确 class 的 `ExitError`。

此前 `execFailureMessage` 已经切分、`runFailureMessage` 没有——**同一 wire 形状，两个不同读者**。
后果是每一条带 detail 的 reason（33 处全部）既查不到 hint 也查不到 exit class，
一律落 70，即"可重试"。一个 `not_a_member` 的成员被告知不断重试。

新增 `TestRunFailureMessageSplitsCodeFromDetail`（6 例表驱动）：裸码、码+detail、未知码，
并断言 raw code 与 detail **都**留在消息里（丢 detail 就是把一个可用性问题换成另一个）。

**未做的项（诚实登记）**

**D13 第 2 步 — 删 `IssueUserJWT` + `AccountPublicKey`（内审 M2 指出，此前漏登记）**

plan §4-A6 要求"同一 PR 内"删这两个生产零引用的符号。**实现只做了第 1 步**（接上 kind 校验 +
把 seed-kind 断言重定向到 `loadAuthCalloutSeeds`，`cmd/tether/serve_authseed_test.go` 已在），
第 2 步没做，且**此前没有登记**——这正好是本增量"不留假记录"承诺的反例，由内审抓到。

决定**保持不做**并在此登记，理由：删它们要连带清理 `internal/auth/jwt_test.go` 与
`test/p1/foundation_risk_test.go` 的 3 处断言，而这两处测试目前是 `LoadAccountSigner` 语义的唯一
单元覆盖；A6 新增的 `serve_authseed_test.go` 覆盖的是**接线**、不是 signer 本身的 kick/kind 语义。
在同一增量里既接线又拆掉被接线者的单元测试，会让外审无法分辨"守卫是否真的生效"。
建议作为批次 B 的一个独立小项（连同 plan 已推迟的"消掉第二份 JWT 实现"）。

**A8 第 3 项** — `docs/reviews/` 335 个文件的 `git mv` 分层 + INDEX.md。**刻意不做，建议单独成增量。**
理由不是没时间：它是**纯搬迁、净减 0 行**，而把 335 个文件的重命名混进这 15 项实质改动里，
会让外审的 diff 从"可逐条读"变成"翻不完"——恰好淹没本增量真正需要人看的部分
（退出码分类、ACL 删授权、fence 收口、retire 删除）。
plan §7 的"不该做的重构"里也列了同类判据。

## 原「待做」清单（已被上面的完成记录取代）

- **组 1**：A1 Step 4（消 40 处 `"<code>: "+err` 拼接）
- **组 2**：A7（ACL 双向对账 + 删死授权）；A13 剩余删除
-
- **组 2**：A4 → A6 → A7 → A13 → A11
- **组 3**：A9 → A10 → A12 → A14 → A5 → A15
- **组 4**：A8

## 实施中确认的 plan 修订

1. **A1 规模远超 S1 估计**：S1 说 39 个码/27 个未分类；实测发射形态 ≥7 种，最终分类 **62 个**（内审 m3 订正：此前文档写 65，实为 62；其中 8 个显式归 70，行为真正变化 54 个）。
2. **不在 proto 重复 adminsock 的码**——plan 决策 D3 的判据在实施时精化为"按传输分界"，
   而非"凡跨包共享就搬"。
3. **扫描器不该把 helper 定义体报成 unresolved**：`Code: <形参>` 是 helper 的定义处，
   真实的码在调用点已由 form 3 覆盖。这类误报会撑大 unresolved 列表，
   而那份列表唯一的价值就是保持简短可读。
