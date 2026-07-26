# 批次 A 内审报告（CLAUDE.md §3 step 4）

> 6 个视角审查 + 6 个对抗性复核，本文是综合裁决版。
> **重要**：审查期间主进程仍在修改工作树。本报告的**全部结论按当前工作树复核**（我逐条重新取证），
> 已被修复的 finding 移入 §6 并标注 `[STALE]`，避免主进程重复处置。
> 我方只读：未修改任何实现代码。

---

## 0. 结论

**ship-with-fixes** —— 无未决 blocker（审查期间抓到的 1 条现网可触发安全回归已由主进程修复并补上行为测试）；
剩余 13 条 major 集中在**「闸门宣称的覆盖面 > 实际覆盖面」**这一类，正是本批次自己的立项命题在自己产出上的残留。

单条最需要先看的：**A15 的 raft 日志桥在唯一的生产构造路径上完全不输出**（`ProductionConfig` 没有 `Logger` 字段），
而 `internal/cluster/node.go:279` 的新注释写着 "raft's own logging, finally wired"。

---

## 1. Blocker

**未决 0 条。**

以下 1 条在审查期间由主进程修复，记录在此供确认（这是本轮最有价值的产出，3 个视角独立命中）：

### B1 [已修复，需确认] A12 收口 `Bind` 时反转了空 host 的 loopback 判定，`:port` 曾被放行到全网卡

- **回归点**：`internal/httplisten/httplisten.go` 的 `isLoopbackHost` 曾写作 `if host == "localhost" || host == "" { return true }`。
  被它取代的两份都拒绝空 host —— `git show HEAD:internal/clustermanifest/manifest.go` 是
  `if host == "localhost" || host == "" { return host == "localhost" }`（这个看似冗余的形状**存在的唯一理由**就是让 `""` 答 false），
  `git show HEAD:internal/subhttp/subhttp.go` 走 `net.ParseIP("") == nil` → error。
- **后果面**：`--sub-http-listen :8090` / `broker.sub.listen` 会静默绑到 `[::]`。`/sub` 是**未鉴权**的 session token + Clash PSK 分发口，
  明文 HTTP；cluster manifest 是 C2 发现文档。现网 racknerd 是公网单 broker。
- **为什么现有护栏抓不到**：`internal/subhttp/p13_external_review_test.go:21` 与
  `internal/clustermanifest/manifest_test.go:55-56` 都只喂 `0.0.0.0:0`（该形态新旧都拒绝）；
  `internal/httplisten/policy_test.go` 两个测试全是 AST 断言，只看调用点传的 bool，从不调用 `Bind`。
- **当前状态（我复核）**：`httplisten.go:73-83` 已改为 `if host == "" { return false }` 并附了「为什么那个冗余形状不冗余」的注释；
  新增 `internal/httplisten/bind_test.go` 覆盖 `":0"` / `"0.0.0.0:0"` / `"[::]:0"` 拒绝 + `127.0.0.1:0` / `localhost:0` / `[::1]:0` 接受
  + `requireLoopback=false` 时 `":0"` 仍可绑（防「全拒绝」式蒙混）。`go test ./internal/httplisten/` 绿。
- **仍建议做**（两项，成本各 5 分钟）：
  1. `internal/clustermanifest/manifest_test.go:55` 与 `internal/subhttp/p13_external_review_test.go:21` 的地址表各加 `":0"` ——
     这两个包各自的护栏至今仍只认 `0.0.0.0`，同一 fail-open 当时是**同时**逃过两套互不相关的护栏，只加固 httplisten 一处仍留假安全感。
  2. plan `docs/reviews/batch-a-plan.md:68` 白纸黑字写着「A12 若改动 /sub 的 loopback 语义，相关 drill 是 **72-proxy-subscription**」。
     A12 确实改动了该语义（就是本条），而 `batch-a-progress.md:18/239` 判为「不需要跑 drill」。
     本条已修复不改变判断：plan 的触发条件当时命中了。发版前跑一次
     `cd test/simcluster && ./local.sh drill 72-proxy-subscription`。

---

## 2. Major

### M1 — A15 的 raft 日志桥在生产上是黑洞，而注释宣称 "finally wired"

- `internal/cluster/node.go:282`：`c.Logger = NewRaftLogger(cfg.Logger, cfg.Logger != nil && ...)` —— 传的是 **原始 `cfg`**。
  `node.go:132` 那句 `logger := cfg.Logger; if logger == nil { logger = slog.Default() }` 只喂 `n.logger`，
  `node.go:180` 的 `raftConfig(cfg)` 绕开它。
- `internal/cluster/production.go:26-42` 的 `ProductionConfig` **没有 Logger 字段**；`production.go:76-92` 的 `New(Config{...})` 也从不设 `Logger`。
- `internal/broker/cutover.go:195` 是 tetherd 唯一的生产构造点。
- `internal/cluster/raftlog.go:82-84`：`if b.logger == nil { return }` —— 静默丢弃。
- 全仓 `slog.SetDefault` 零调用，所以即便兜底到 `slog.Default()` 也不会走 `cmd/tether/logging.go:20-27` 的 handler/level。
- **范围订正**（复核者的攻击成立，我核实）：A15 有三件产出，**只有 bridge 死**。
  `internal/broker/observability.go:252-261` 的 leadership 边沿 Info 走 `b.cfg.Logger`，生产正常；
  `internal/brokermetrics/metrics.go:93-95` 的三条计数器也正常。所以修法是补一个字段，不是 A15 返工。
- **修法**：`ProductionConfig` 加 `Logger *slog.Logger`，`cutover.go:195` 传 `b.cfg.Logger`，`New` 里透传到 `Config.Logger`；
  同时 `raftConfig` 改收已 default 的 logger（或内部同样兜底），并留一条断言「`Config.Logger==nil` 时产出的 logger 仍能吐字」。

### M2 — 错误码扫描器在自己声称 `EXACTLY` 覆盖的 form 3 上有一整扇门：非常量实参被静默丢弃，且**不记 unresolved**

- `cmd/tether/error_code_coverage_test.go:47-48` 宣称 "covers forms 1-3 and 8 **EXACTLY**"。
- 实现 `:274-289` 的 CallExpr 分支只有 `BasicLit` / `Ident`(查常量表) / `SelectorExpr`(查常量表) 三个 case，
  **没有 default，`Ident`/`SelectorExpr` 查不到常量时也没有 else**。对比同文件 `:241-256` 的 KeyValueExpr 分支：
  它有 `else` + `default` 写 `unresolved[rel]`。两条路径不对称。
- 22 处 call site 现处于该状态（`internal/broker/run.go:37,43,53,68,71,80,86,96` 的 `"<code>: "+err.Error()`、
  `expose.go:315`、`upgrade.go:131`、`transfer.go` 7 处、`agent/run.go:170,213`、`agent/transfer.go` 3 处）。
  **我核实这 22 处今天全部已分类**，所以不是现网缺陷。
- **规模比初审所述更大**（复核者的攻击成立，我核实源码确认）：undeclared-file 兜底只对 `Code:`/`Reason:` 的 KeyValueExpr 生效。
  form 3 的实参走局部变量时，**即使在一个全新的、未登记的文件里**也照样静默通过——初审把这条当成「被兜底接住」写进了 wellDone，是错的。
- 风险在 A1 Step 4 之后**变大**：`error_hints.go:309-320` 的 `runFailureMessage` 现在按 `:` 切分并对前缀查表，
  `"<code>: "+detail` 已是一等公民的 class 载体 —— 恰好是闸门唯一看不见的形状。
- **修法**：(1) form 3 的 switch 补 `default` + `Ident`/`SelectorExpr` 的 else，写进一个**按 file:line 计**的
  `declaredOpaqueCodeArgs` 豁免表；(2) 新增一条对 `"<code>: " + x` 形态的抽取规则（取冒号前缀并要求分类），
  `"agent_rejected:"+ar.Code` 两处按已有理由登记豁免；(3) 若 (2) 不做，把文件头的 `EXACTLY` 改成如实描述。
- **附带**：`TestErrorCodeScannerDeclaresItsLimits`（`:547-563`）钉的三段散文里**不包含**那句 `EXACTLY`，
  所以这个「诚实性闸门」既拦不住覆盖面退化、也拦不住夸大措辞。

### M3 — `unresolvedCodeSites` 是**按文件**的一揽子豁免，10 个已登记文件从此免检

- `cmd/tether/error_code_coverage_test.go:108-131`：map 的 key 是文件路径（实测 **10 条**，非初审所写的 11 条）。
- `scanTree` 记录时也按文件去重（`:245-246` / `:250-251` / `:254-256` 的 `if _, dup := unresolved[rel]; !dup`），只留每文件第一处；
  断言侧 `:367` 只检查 `unresolvedCodeSites[file]` 是否存在。
- 每条豁免的 reason 都精确描述**一个站点**（如 transfer.go 那条指的是 watchdog `transfer.go:421-431` 那处），
  但豁免覆盖**整个文件** —— 粒度差一个量级，而这 10 个正是 broker/agent 最热的 reply 路径。
- **修法**：key 改成 `"internal/broker/transfer.go:421"` 这类 file:line，`scanTree` 收集全部站点，断言逐站点比对。
  行号漂移本身就是「这处代码动了，重新看一眼」的正确信号，与 `TestCodeCarryingHelperListIsComplete` 已用的自校正范式一致。

### M4 — A7 的 ACL 对账在三处（含**生产源码**）宣称「双向」，实际只有 grant→subscriber 一向

- `internal/auth/acl_reconcile_test.go:15`："in BOTH directions."
- `internal/auth/permissions.go:52-54`（**生产源码 doc**）："...reconciles this template against the broker's live subscription table in both directions so the pair cannot drift apart again."
- `internal/auth/permissions.go:100`："(TestACLGrantsHaveSubscribers now reconciles both directions)"
- `docs/reviews/batch-a-progress.md:165`：「做**双向**静态对账」
- 文件内实际只有 `TestACLGrantsHaveSubscribers`（:203）与 `TestACLReconcilerIsNotVacuous`（:261）。
  `:30` 的第二方向只是一句 "would be the opposite failure"，**无任何断言**。全仓反向断言 = 0。
- **降级理由**（初审记 blocker，我判 major）：初审的标题事实主张「删掉一条活授权，全包依然绿」**是假的** ——
  复核者在干净副本上删 `alert.ack` 授权后 `go test ./internal/auth/` FAIL（`TestD8bMemberAlertACLCarveOut`），
  且反向探测 18 条被订阅的 `ctrl.by.*` subject 全部有匹配授权、零漂移。所以已实现那一向**确实守得住**，
  这是**三处 doc 不实**（其中一处在生产源码），不是空转闸门。
- **修法（二选一，必须选）**：(a) 补第二方向测试（推荐，proposed-tests 里已有可用实现，实测 baseline 全绿）；
  (b) 把四处「双向 / BOTH directions」改成「grant→subscriber 单向」并在测试头显式登记未覆盖的方向。
  留现状不行 —— 这正是 A4 刚修掉的 `RehomeDirective` 失败模式（doc 承诺一个不存在的 guard），而这次假承诺写进了生产源码。

### M5 — ACL 对账的「订阅表」抽取器把任何 `SubjectPrefix + "字面量"` 都当订阅，声明一个 proto 常量就能洗白死授权

- `internal/auth/acl_reconcile_test.go:157-190` 的 `subjectLiterals` 抓的是任意 `BinaryExpr(SubjectPrefix + STRING)`，
  与是否出现在 `nc.Subscribe(...)` 无关。
- 闸门认为存在的 38 条「订阅」里，`SubjVersionAnnounce`（proto/subjects.go:10，broker 只 publish）、
  `SubjSysEvents`（:11）、`SubjClusterPrefix`（:22）都不是 broker 的订阅。
- 变异实测（复核者复现一致）：加回 kick 授权 + 在 `internal/proto/subjects.go` 加一行
  `SubjCtrlSessionKickWildcard = SubjectPrefix + ".ctrl.by.*.session.*.kick.req"`（**无人订阅**）→ 测试 **ok**。
  对照组（只加授权不加常量）正确翻红。
- 这恰是该测试头 `:24-28` 自己写下的威胁模型：「下一个人建 node tagging 时发现 subject 和 grant 都在，认定设计已完成」——
  而在本仓约定里「找到 subject」的标准写法**就是**在 `internal/proto/subjects.go` 声明常量。
  **本轮抓到三条死授权是运气**（kick/rotate-pin/tag 恰好没有 proto 常量），不是设计。
- **修法**：抽取器收窄到真实订阅点 —— `nc.Subscribe(...)` / `QueueSubscribe(...)` 的第一实参，
  以及 `broker.go:957-1013` 那张 `{subj, handler}` 表的 `subj` 字段（含 proto 常量一层解引用）。
  同时在测试头登记「只覆盖 `ctrl.by.*` 前缀的授权，agent 模板的 `s.*` 授权不在范围内」（`:118` 的现有限制未声明）。

### M6 — `/metrics` 服务 goroutine 无同步读 `b.cl.auditPub`，与 `wireClusterLate` 的写构成数据竞争（`-race` 可复现）

- 读：`internal/broker/metrics_wire.go:25-28`（A15 新增）。
- 写：`internal/broker/clusterwrite.go:385` `b.cl.auditPub = pub`，由 `internal/broker/broker.go:1134` 的 `wireClusterLate` 触发。
- metrics listener 的 goroutine 在 `broker.go:916` 就已起来 —— **早于 :1134**。窗口内夹着 JetStream 探测（1s）、
  `EnsureEventsStream`（5s）、`reconcileHistoryStreamsOnBoot`、`reconcileXferObjects`，数百毫秒到数秒量级，
  15s 间隔的 Prometheus scrape 完全可能落进去。
- `metricsSnapshot` 触碰的其它字段全部要么在 listener 之前写定（`b.cl` 本体 `broker.go:817`）、
  要么自带同步（`lastObserveMu` / atomic）——`auditPub` 是**第一个**「listener 起来之后才写」的字段。
  仓库自己在 `broker.go` 三处把「写在 goroutine 起之前 ⇒ race-free」立成明文纪律。
- amd64 上指针读不会撕裂，所以后果良性；但本仓并发纪律不接受「这个 ISA 上大概没事」。
- **修法（推荐 ①）**：① `auditPub` 改 `atomic.Pointer[AuditPublisher]`（读侧一行）；
  ② 三个计数器提到 `Broker` 上做 `atomic.Uint64`；③ 把三个 listener 的启动挪到 `wireClusterLate` 之后。

### M7 — A15 的 167 行 raft 日志桥零测试；去重键只有 msg，会吞掉多 peer 的区分度

- `grep -rln "raftLogBridge\|NewRaftLogger\|raftLogDedup" --include=*_test.go .` → **0 命中**（我复核）。
- 决策 D23 的全部论证就是「无条件加 30s 去重 + 速率上限，两种情况下日志预算都有界」。
  `allow()`（`raftlog.go:62-79`）实现了窗口，但没有任何测试证明它在窗口内抑制、窗口后带 `suppressed_since_last` 放行。
- `allow()` 的 key 只有 `msg`，不含 args。raft@v1.7.3 `raft.go:1064` 是
  `r.logger.Warn("failed to contact", "server-id", server.ID, "time", diff)` —— 每个 follower 同一条 msg、
  身份只在 args。两个不可达 peer 时 30s 窗口内只有其中一个的 server-id 进日志，另一个只体现为计数。
  这直接损害 A15 自己的立项目的（事故复盘最需要知道**哪个 peer** 联系不上）。
- `raftlog.go:71-76` 的抑制计数只在**下次放行时**才带出，故障停止后残留计数无 flush 路径：
  一次持续 25s 后恢复的抖动，最终报出 1 条、抑制数 0，读者以为只发生过一次。
- `b.seen` 无上限、无淘汰（raft 的 msg 集合有限，实践上有界，但没有断言）。
- **顺带订正一条 review 内的错误论据**：`raftlog.go:129-152` 的 `With`/`Named`/`ResetNamed` 各建新 `seen` map，
  所以预算是 per-derived-logger 而非全局；但 raft@v1.7.3 全仓只有 3 处 `.With()`（fsm.go:197、api.go:364、api.go:682，
  均在 snapshot restore / RecoverCluster 路径），`.Named()`/`.ResetNamed()` 零调用，**实践上不构成问题**。
- **修法**：补 `internal/cluster/raftlog_test.go`（纯单元、注入收集型 slog.Handler）：
  (a) 同一 msg 窗口内一条、窗口后带 `suppressed_since_last=N`；(b) `debug=false` 时 DEBUG/INFO 被丢、`true` 时透出；
  (c) `logger==nil` 不 panic。dedup key 改为 `msg` + args 里 `server-id`/`peer`/`id` 类字段的拼接（未命中退回纯 msg），
  并给 `seen` 加上限。

### M8 — A5 的 loop 行违反 `RuntimeReport` 自己的书面契约，长跑 broker 的 `admin runtime --json` 会稳定出现 4–5 个「按文档判据 = 已 stall」的假阳性

- `internal/adminsock/protocol.go:370-372`（未改动的契约）：
  "Reconcilers is one entry per registered R7 periodic reconciliation pass, carrying the last-tick the registry recorded.
  **A pass whose LastTick stops advancing while the process is up is a stalled reconciler**"；
  `:339-340`："Every field is measured FROM THIS PROCESS at request time"。
- `internal/broker/runtime_introspect.go:76-89`：loop 行的 `LastTick: st.StartedAt` —— 按构造**永不推进**。
- 这是**机器可读载荷**，不只是渲染：`export-incident` 包（`protocol.go:296` `Runtime *RuntimeReport`）也带它；
  `IntervalMS`/`LeaderOnly`/`Skips` 都**不带 omitempty**，零值原样进 JSON，
  而 `cmd/tether/admin.go:186-187` 会把 INTERVAL 渲染成 `0s`、SKIPS 渲染成 `0`。
- 代码已用 `Name: "loop:"+name+" (started_at)"` 做人眼区分（这个判断是对的），但 JSON 消费者不能假定解析名字，
  且 `adminsock/protocol.go` 的契约文本一个字没改。
- 决策 D24 只论证了「加 omitempty 字段不 bump」，**从未评估「往既有数组里加异质行」**这一种兼容性问题。
- **附带**：`markStarted` 在 goroutine **内部**执行（`loopset.go:86`），`Go` 在 `:88` 就返回，
  所以启动窗口内 `Snapshot()` 可能返回 `StartedAt` 零值 → `admin.go:183-185` 渲染成 LAST_TICK `never`。
- **修法（三选一）**：(a) 更新 `adminsock/protocol.go` 的 Reconcilers 契约，明写「`loop:` 前缀的行 LastTick 是 start-time、
  按定义不推进」；(b) 给 loop 行的 `IntervalMS`/`Skips` 填 `-1` sentinel 并在 `renderAdminRuntime` 里按前缀走独立渲染分支；
  (c) 不塞进 `Reconcilers`，另开一个 `Loops []LoopRow` 字段（带 omitempty，按 D24 不 bump）。
  另：要么给 `loopStat.LastErr` 真正写入（`Go` 的 defer 里 recover 并记录），要么删掉它 ——
  全仓零写入点，一个永远为空的 LAST_ERR 会让人以为「这个 loop 没出过错」。

### M9 — `internal/proto/codes.go` 自称 SSOT，32 个常量里 30 个全仓零引用，且无任何闸门比对常量值与发射点

- `internal/proto/codes.go:15-16`："This file is the SSOT for the NATS-wire half; TestErrorCodeCoverage reconciles emitters against the tables."
- 实测：32 个常量里只有 `CodeLeaderUnavailable` 有生产引用（`internal/agent/agent.go:1022` + `internal/broker/broker.go:1384`），
  **其余 30 个零引用**。发射点仍是裸字面量（如 `internal/broker/expose.go:175` 的 `"actor_invalid"`），
  与 `codes.go:46` 的 `CodeActorInvalid` 之间没有任何编译期链接。
- `TestErrorCodeCoverage`（`error_code_coverage_test.go:329-378`）只把**发射到的码**与 `brokerCodeExitClasses`/allowlist 对账，
  **从不读 codes.go 的常量值**。plan D2 承诺「常量值与原字面量的一致性由 Step 3 的守门测试保证」—— 这个保证不存在。
- **我独立核实：今天零漂移**（脚本对 32 个值逐个 grep 排除 codes.go 与 `_test.go`，无一为 0 命中）。所以这是**潜伏风险**，不是现网缺陷。
  把 `codes.go:36` 改成 `"not_owner_typo"`：包级未使用常量合法、编译通过、`make test` 全绿。
- **另**：`TestWireCodeNamespacesAgree`（初审报「不存在」）**现已存在**（`cmd/tether/wire_code_namespaces_test.go:28`，带非空转自检）——
  那半已修，`codes.go:29`/`:129` 的两处引用现在诚实了。剩下的是 `:15` 的「SSOT」。
- **顺带：决策 D4 未兑现且反向恶化**。D4 说 `dataplaneNotConvergedCode` 可以搬进 proto。
  现状是**三份声明**：`internal/broker/home_convergence.go:55`、`cmd/tether/error_hints.go:231`、`internal/proto/codes.go:123`，
  且 proto 那份零引用。比批次 A 之前多了一份。
- **修法（二选一，别留中间态）**：
  (A) 落地 SSOT —— 把发射点改成引用常量（这是 wire 面的机械改动，属批次 B），并补
  `TestProtoCodeConstantsAreActuallyEmitted`（codes.go 声明的每个值必须真有发射点；proposed-tests 里已有实现，实测绿）；
  (B) 本批次不接线（合理）—— 把 `codes.go:15` 的「SSOT」改成「REFERENCE 清单，尚未与发射点接线，见批次 B」，
  并把 proto 那份 `CodeDataplaneNotConverged` 删掉或在 D4 处登记为未采纳。

### M10 — `exec_failed`（`tether run` 最常见的失败）有 hint 无 exit class，且它的豁免理由是假的

- `cmd/tether/error_hints.go:296` 给了 hint（"check argv (typo? not in PATH? not executable?)"），
  但 `brokerCodeExitClasses` 里没有该键（我 awk 抽表实测 0 命中）→ `brokerCodeExitClass("exec_failed")` 落 70。
- 发射点 `internal/agent/run.go:204` 是 `reason := "exec_failed"`（AssignStmt），
  而扫描器**完全没有 AssignStmt/ValueSpec 扫描**（`grep AssignStmt cmd/tether/error_code_coverage_test.go` = 0）。
- 因此 `unresolvedCodeSites["internal/agent/run.go"]` 的豁免理由 ——
  "the local variable is assigned from literals a few lines up (run.go:160-168) and re-emitted;
  **those literals are scanned at their assignment sites**" —— 两句都不成立
  （既没有 assignment-site 扫描，另一支 `remoteFSFailReason(ferr)` 还是函数返回值）。
  这条假理由正是该文件头部自己批判的形态：「a gate which claims to cover every code ... reviewers stop looking」。
- **重要的因果订正**（复核者攻击成立）：A1 **之前** `runFailureMessage` 返回裸 `fmt.Errorf`，
  冒泡到 sink 由 `classifyExit`（`exitcode.go`）落 70。A1 之后是显式 `Class:70`。
  **前后完全等价** —— A1 没让它变糟，这是「承诺分类却没分到位」的遗漏，不是新造缺陷。
  `download_failed`(75) / `state_write_failed`(70) 三条同理（且 usage.md 下 70 与 75 对自动化等价）。
- **修法**：(1) 定 `"exec_failed"` 的 class 并写下理由 —— 注意这里有真实取舍：
  `sess.Start()` 的兜底 err 除 ENOENT/EACCES（argv 打错）外还含 EAGAIN/ENOMEM（主机资源压力），
  与 `pty_alloc_failed` 同族。要么归 64 并说明「打错 argv 占主导」，要么留 70/75 并在行内注释写下这个权衡；
  (2) 把 `internal/agent/run.go` 的豁免理由改写成事实；
  (3) 加一条「有 hint 就必须有 class」的闸门（proposed-tests 里已有，实测当前 FAIL 且只报 `exec_failed`）。

### M11 — `session create` 的 `Error:`-as-code 形态完全在守门规格之外，且 A1 的冒号切分没给 `brokerErrorMessage`

- `proto.SessionCreateResp` 没有 `Code` 字段，broker 把码写进 `Error`：
  `internal/broker/sessions.go:24` `"subject_malformed"`、`:44` `"name_required"`、`:57` `"already_exists"`、
  以及 `:28` `"actor_invalid: "+err`、`:33` `"actor_decode: "+err`、`:39` `"json_parse: "+err`、`:48` `"pin_invalid: "+err`。
- `cmd/tether/session.go:67` 把整个 `resp.Error` 当 code 传给 `brokerErrorMessage`，
  而 `brokerErrorMessage`（`error_hints.go:247-264`）**只剥 `agent_rejected:` 前缀，不做 A1 给 `runFailureMessage` 加的冒号切分**。
- 结果：`tether session create --pin <非法>` 今天仍退 70（文档说可重试）；`pin_invalid` / `actor_decode` 连 `brokerCodeExitClasses` 都没进。
- 扫描器只认 `Code:`/`Reason:` 两个 key（`error_code_coverage_test.go:226`），所以这条路径**既不计 missing 也不计 unresolved**。
- 这是「同一 wire 形状两个读者」这一 A1 立项理由的最大残留实例，且发生在一等用户命令上。
- **修法**：把 `runFailureMessage` 的冒号切分逻辑提出来给 `brokerErrorMessage` 复用（人类字符串保持字节不变），
  并给 `Error:` 这个 key 在扫描器里加一条规格（或显式登记为已知未覆盖形态）。

### M12 — 批次 A 的旗舰闸门 `TestRehomeDirectiveHasNoLivePublisher` 只扫 `CompositeLit`，var-decl / `new()` / 转发型 publisher 全部逃逸

- `internal/proto/rehome_directive_test.go:50-52`：`cl, ok := n.(*ast.CompositeLit); if !ok { return true }`。
- 复核者的实测（我核对了扫描逻辑，成立）：在 `internal/broker/zz_rehome.go` 写
  `var d proto.RehomeDirective; d.Name = name; d.Epoch = 7; b, _ := json.Marshal(&d); return nc.Publish("tether.v2.rehome", b)`
  → `go build` 通过，`go test ./internal/proto/ -run TestRehomeDirective` → **ok**；
  换成 `d := proto.RehomeDirective{}` 则正确翻红。
- `internal/proto/messages.go` 那句「asserts it has no live publisher, so a half-wiring is caught」仍只兑现了一部分 ——
  而 A4 的整个立项理由就是「doc 承诺存在 guard test 而它不存在」。
- **修法**：扫描扩到 `*ast.ValueSpec`（`var x proto.RehomeDirective`）与 `*ast.CallExpr` 的 `new(proto.RehomeDirective)`；
  或换一个更粗但更不可逃的判据：全仓（除 proto 包与本测试外）不得出现 `RehomeDirective` 这个标识符。
  后者对「decode-then-republish」也有效，且失败信息更直白。

### M13 — A2 承诺的「`store_error` 明细只进 broker 日志」未落地：`transferGate` 三处丢弃 err 且无任何日志

- `internal/broker/transfer.go:988-1017`：三处 `if err != nil { return "store_error" }`，**全函数无任何 `b.cfg.Logger` 调用**。
- plan D8（`batch-a-plan.md:139`）、progress（`:59`）以及 A2 新写的代码注释都以**既成事实**的口吻写着这条通则。
- **降级理由**（初审记 major「净信息丢失」，我判 major 但性质不同）：`transferGate` 函数体**本批次一行未改**（git diff 确认），
  plan D8 自己也写着「transferGate 现有 4 个调用点今天全部丢弃 err 明细」——
  即这个盲区**先于批次 A 存在**，A2 只是没顺手补上；且被删的 `Error: err.Error()` 的接收者是**任意 session member**（外泄面），不是运维。
  所以真实缺陷是「一条 plan 通则被写成了既成事实而实际是待办」。
- **修法**：三个 `if err != nil` 各加一行结构化日志（不上 wire，不触碰硬边界 1），
  例如 `b.cfg.Logger.Error("transfer gate: store lookup failed", "sid", sid, "fp", fp, "stage", "is_active", "err", err)`。
  `b.cfg.Logger` 在该文件已被使用（`transfer.go:437`），无需改签名。

---

## 3. Minor

| # | 位置 | 问题 | 修法 |
|---|---|---|---|
| m1 | `internal/broker/clusterdrain.go:117-121` | `if retire && proj.Voters < 1 {...}` 在 `:103` 的 `if retire { return }` 之后**恒不可达**，读起来像安全网。`:116` 的 `ProjectQuorum(voters, false)` 已有注释说明。 | 删掉不可达分支。last-voter 保护在活路径上完好（`cluster_operation_controller.go:201` + `:1019`）。 |
| m2 | `internal/broker/clusterstatus.go:573/596/598` | `clusterAdminBackend.streamsReady` 已成**只写字段**（唯一读点随 handleDrain 改写消失；全仓零 `.streamsReady` 读取）。`d9_external_review_test.go:17` 的失败文案仍宣称守着 "caughtUp/**streamsReady**"。`make lint` 抓不到结构体死字段。 | 删字段+形参并收窄 `d9_external_review_test.go:17` 文案到 caughtUp；或加注释登记「A13 后无生产读者」。**注意不要连坐** `ClusterAdmin.streamsReadyFn`（仍被 `cluster_operation_controller.go:911,948` 消费）。 |
| m3 | `internal/broker/loopset.go:80-82` | `l.done` 的惰性初始化在 `l.mu.Unlock()`（:79）**之后**，`Join` 的 `case <-l.done`（:112+）同样无锁读；而 `names`/`stats`/`started` 全在锁内。`Go` 的 doc 只说 "must be called before Join"，没说不可并发调用。生产 `clusterwrite.go:432-441` 顺序调用故今天不可达。 | 把 `done: make(chan struct{}, 64)` 挪进 `newLoopSet()`；把 64 提成具名常量并在 `Go` 里对超限 panic。 |
| m4 | `cmd/tether/poll.go:51` | `t := time.NewTicker(interval)` 在第一次 `step()` **之前**创建，故 `step()` 耗时 > interval 时下一轮立即触发（背靠背轮询）。旧 `pollUntil` 用 `time.After` 每轮新建，是「step 完再等一个完整 interval」。 | 语义变化很小（Ticker 丢弃错过的 tick），但未进 progress 的行为变化表。要么改回 `case <-time.After(interval)`，要么在 godoc + 行为变化表里登记。 |
| m5 | `internal/broker/metrics_wire.go:21-24` | 注释宣称「Read BEFORE the cluster-mode early return —— 单模式 broker 静默丢审计同样不可调查」。但 `internal/brokermetrics/metrics.go:84` 的 `if !s.ClusterMode { return }` 在三条 `c(...)` **之前**，且 `AuditPublisher` 只在 `clusterwrite.go:343` 构造（cluster-only），单模式恒 0。注释描述的保护两头都不存在。 | 改注释（如实说「cluster-only；单模式没有 AuditPublisher」），或把三条 `c(...)` 移到早退之前（值恒 0，但序列始终存在，符合 `Render` 自己 "never omit a series a scraper expects" 的规矩）。 |
| m6 | `internal/httplisten/policy_test.go:113,130-133` | `TestNoDirectHTTPListenOutsideHTTPListen` 只扫 3 个**硬编码**包，且只匹配 `sel.Sel.Name=="Listen" && sel.X=="net"` —— `net.ListenConfig{}.Listen` / `tls.Listen` 漏过。名字承诺全仓。plan D20（`batch-a-plan.md:231`）要求的也是全仓断言。 | 改名 + 注释写明范围 + 加一条列表长度自检；或改成全仓扫描 + 带书面理由的豁免表（tunnel 数据面、adminsock）。**另**：D20 明写三个**具名入口** `BindLoopback`/`BindAny`，实现改成了 bool 参数 `Bind(name, addr, requireLoopback)` —— 这个偏离既是本条的根因，也是 B1 有机会发生的结构前提，progress §A12 记了改动但未标注为对 D20 的偏离。 |
| m7 | `cmd/tether/error_hints.go:204-214` | 注释块标题是 "The online force-single anti-split-brain gates" 并称 "All five"，但 `is_leader` 的唯一发射点是 `internal/broker/reexec.go:58`（broker upgrade reload 的 leader 门），与 force-single 无关。分类 64 本身正确。（该行行内已写 `(reexec.go:58)`，属半自曝。） | 把 `is_leader` 从该块拆出单列。**`docs/usage.md` 无需改** —— 新增块（1544-1556）只列了 peer_alive / quorum_not_lost / arm_expired 三个，从未提 is_leader。 |
| m8 | `internal/httplisten/httplisten.go:85-88` | 注释 "AfterFunc's registration is released when ctx is done OR when the returned stop func runs, **so nothing outlives Serve**" 与 `context.AfterFunc` 语义不符：`stop` 不等待 f 完成（`/usr/local/go/src/context/context.go:316-322`），跑 `srv.Shutdown(3s)` 的 goroutine 可活过 Serve 返回最多 `shutdownGrace`。改动本身仍是净改善。 | 改注释；或真做到：`done := make(chan struct{})`，`stop` 返回 false 时 `<-done`（本函数只调一次 stop，不会死锁）。这也关系到泄漏门在 cancel 后立刻采样时的容差。 |
| m9 | `internal/broker/clusterstatus_test.go:128` | `TestD7RetireLastVoterHardRefused` **保留了名字、换掉了断言**：现在测的是 drain 的拒绝措辞（与 `a13_drain_retire_test.go:22` 重复），名字仍向 grep 的人承诺守着 last-voter 门。连带：`:148` 仍钉 `{"cannot retire the last voter" → CodeNotAVoter}`，但活路径的生产者 `cluster_operation_controller.go:201` **无任何测试断言它会产生这个串**。 | 改名（如 `TestA13DrainRetireRefusedNamesTheOperation`），并给 `StartRetireOperation` 的 last-voter 门补一条生产者侧断言。 |
| m10 | `internal/broker/bannerbuilder.go` | 零测试（`grep -rln bannerBuilder --include=*_test.go` → 0），而自述风险是「drill 会 grep 的运维可见字符串」。**我逐行核过 `clusterstatus.go:355-376`：`bb.add(rep.Banner)` 与 `rep.Banner = bb.String()` 之间只有 healthExitCode/healthLabel/certExpiryAdvisory，无一写 `rep.Banner`；`strings.Join(parts," ")` 与旧的逐段 `if != "" { += " " }` 在 0/1/2/3 段全部组合下逐字节等价 —— 本次无回归。** 但「by construction」是论证不是闸门：下一个人若在两者之间插一处 `rep.Banner = ...`，会被静默丢弃（旧的 `+=` 形状不会丢）。 | 补 12 组合的表驱动字节断言 + 一条 AST 断言（`StatusReport` 内 `bb.add(rep.Banner)` 之后不得再出现 `rep.Banner` 的写入）。 |
| m11 | `internal/broker/clusterdrain.go:211` | `RemoveNode` godoc 仍写 "Use `drain --retire` to remove a live node" —— 一个在 CLI 层（`cmd/tether/cluster.go`）、socket 层（`clusterstatus.go:800`）、admin 层（`clusterdrain.go:103`）三重拒绝的动词。同文件 `:230` 的错误串已改口 `cluster retire`，godoc 落后于自己的代码。`:289` 的 NOTE 理由也过时。（**`DrainNode` 的 godoc 已修好** ——`:74-87` 现在如实描述删除。） | 改成 "Use `tether cluster retire <node>`"，顺手更新 `:289` 的 NOTE。 |
| m12 | `docs/architecture.md:231-235 / :257 / :1219` | A7 删了三条授权只同步了 `requirements.md`。`:257`（B.2 note，现在时规范句）仍写 `session.<S>.{rm,kick,rotate-pin}` 非 owner「立即 reply `admin_denied`」—— 这是 A4/D11 判定为**双重假话**并已在代码注释里订正掉的那句；`:231-235` 的 JWT pub 模板仍逐字列出三条被删授权；`:1219` 仍写「权限层预留了 `.kick.req`/`.rotate-pin.req` 的 pub 许可」。**而 `internal/auth/permissions.go:45` 新写的文本主动把读者指向 "see architecture B.2 note"。** 缓解：A8 新增的顶部须知覆盖 §A–§K，`:257`（§B，122-366）与 `:1219`（§H，1179-1373）**都在覆盖范围内**。 | 优先级最高的是 `permissions.go:45` 的那个指针（代码指向一份已知失真的段落）；其次 `:1219`（DOC-12 是「当前事实」段落，A7 刚把它变成假的）。`:231-235` / `:144-148` 属 v1 历史段，可选。 |
| m13 | `cmd/tether/error_hints.go:222` `state_write_failed` → 70 | 发射点 `internal/agent/expose.go:78` 是写 agent 的 `state.json`（盘满/只读/权限），被归入 "our bug / version skew" 块；而同一 handler 下 20 行的 `frpc_failed`（`:93`）被归 **64**（`error_hints.go:190`）。两者同属「这台 agent 主机做不了这件事」。行为上 70 与 75 对自动化等价，故无行为差异；但「我们判过了：这是 tether 的 bug」会让运维去查 issue 而不是 `df -h`。 | 归 64（与相邻的 `frpc_failed` 一致），或按 `io_error` 先例进 allowlist 并写理由。同类账目问题还有：`cutover_revival_failed`→70 但其 Error 串就是一条 systemctl 运维命令（`cluster_grow_cutover.go:226-228`；今天不可达，经 `cluster_add_drive.go:555`→69）；`free_failed`→70 而镜像项 `alloc_failed` 进了 allowlist；`signal_failed`→70 而同 handler 的 `pid_unknown`→64（且 `sendKill` 丢弃回复体，不可达）；`path_outside_roots`→77 而同一个 `allow_roots` 旋钮的 `transfer_disabled`→64。**建议一次性把这一族的取舍写进行内注释，而不是逐条改类。** |

---

## 4. 建议新增的测试

专家写的测试在 `/home/weiland/.claude/jobs/cda1899e/tmp/batch-a-review/proposed-tests/`（未进仓库）。
按价值排序，标注目标路径与守护对象：

| 文件 | 目标路径 | 守什么 | 状态 |
|---|---|---|---|
| `acl_reconcile_reverse_test.go` | `internal/auth/acl_reconcile_reverse_test.go` | **M4** 的第二方向：subscriber→grant。同包复用 `tokenMatch`/`grantSubjects`/`subscribedSubjects`。 | 实测 baseline 绿；删 `alert.ack` 授权 → 红并点名 `PREFIX.ctrl.by.*.alert.ack.req`；同时删 `proxy.sub.revoke` → 报两条 |
| `error_code_concat_coverage_test.go` | `cmd/tether/error_code_concat_coverage_test.go` | **M2**：`"<code>: "+detail` 形态的抽取 + 按 **file:line** 的 `declaredOpaqueCodeArgs` 豁免表（同时是 **M3** 的参考实现） | 实测 baseline 绿；`run.go:43` 改新码 → 红并报出 file:line；新增未登记不透明实参 → 红 |
| `raftlog_production_wiring_test.go` | `internal/cluster/raftlog_wiring_test.go` | **M1**（`Config.Logger==nil` 时 raft 仍能吐字）+ **M7**（窗口内抑制 / 第二个 peer 不被吞） | `TestRaftLoggerSurvivesANilConfigLogger`、`TestRaftLoggerDedupDoesNotHideASecondPeer` 当前 FAIL；`TestRaftLoggerDedupesIdenticalMessages` 当前 PASS，可一并收作窗口回归 |
| `metrics_auditpub_race_test.go` | `internal/broker/metrics_auditpub_race_test.go` | **M6**：启动窗口内 scrape /metrics 与 `wireClusterLate` 写 `auditPub` 的竞态 | `go test -race` 当前 FAIL（`Read at metrics_wire.go:25 / Previous write at ...`） |
| `exec_exit_class_test.go` + `error_code_semantics_review_test.go` | `cmd/tether/` | **M10**：「有 hint 就必须有 exit class」；`runFailureReasons` 一侧的反向对账 | 当前 FAIL，输出恰为 `exec_failed`（其余 9 个 reason 全部已有 class） |
| `proto_codes_ssot_review_test.go` | `internal/proto/` 或 `cmd/tether/` | **M9**：`codes.go` 声明的每个值必须真有发射点（把已写下的承诺变成事实） | 实测当前**绿** —— 收下它是为了把今天的零漂移钉住 |
| `loopset_concurrent_go_test.go` / `loopset_join_test.go` | `internal/broker/` | **m3**：并发 `Go` 的 `-race`；Join 的多 loop 超时路径（后者对应主进程已做的修复，收下作回归） | concurrent 版 `-race` 当前 FAIL |
| `code_scanner_file_exemption_test.go` | `cmd/tether/` | **M3**：豁免粒度必须是 file:line 而非整文件 | — |
| `promised_guard_tests_exist_test.go` | 仓库级 | 元闸门：凡源码注释里写「a guard test asserts X」，那个测试名必须真的存在。**直接对应 A4/M4/M9 这一整类失败模式**，价值高于单条修复 | — |
| `fence_snap_test.go` | `internal/tunnel/` | A9 的 fence 快照/比较等价性 | — |
| `httplisten_*_test.go`（5 份） | `internal/httplisten/` | **B1**。主进程已自行落地 `bind_test.go`，这 5 份可作交叉核对，其中「`requireLoopback=false` 时 `":0"` 必须仍能绑」的反向断言值得确认已覆盖（`bind_test.go:60` 有） | 已被覆盖 |
| `tokenhash_pinned_digest_test.go` / `wire_code_namespaces_test.go` | — | 对应的问题已修复，仅供交叉核对 | STALE |

**建议补写但无人提交的**（M8 / m10 / m5）：`internal/adminsock` 契约与 loop 行的一致性断言、
`bannerBuilder` 的 12 组合字节断言 + 「`bb.add` 之后不得再写 `rep.Banner`」的 AST 断言。

---

## 5. 做得好的地方

具体点名，都是我或复核者独立验证过的：

1. **A1 的三条自检骨架真有效，不是装饰。** 四次变异实测：新增未分类字面量码 → 红；`Code: <局部变量>` 出现在未登记文件 → 红（undeclared 兜底）；
   新增 code-carrying helper 未注册 → `TestCodeCarryingHelperListIsComplete` 精确报出 `helper "replyNewErrM7" takes a reply code at arg #0`。
   **undeclared-file 兜底是整个设计里最强的一环** —— 它让「扫描器看不见」必须变成一次写下理由的自觉动作。
   （其局限见 M2：这条兜底只覆盖 KeyValueExpr。）
2. **`internal/httplisten/policy_test.go` 抓住了不显而易见的规避手法**：把 bool 洗成具名常量 →
   报 `requireLoopback must be a literal true/false so this policy is statically checkable`。大多数 AST 策略测试会在第二种手法上失守。
3. **`tokenMatch` 的 NATS 通配语义实现是对的**，逐条验证：`*` 恰好一 token、`>` 至少一个尾 token（`len(s) > i`，故 `foo.>` 不匹配 `foo`）、
   其余长度必须相等。已实现的那一向确实守得住（加回 kick 授权 → 翻红，两位复核者独立复现）。
4. **A7 的删除是真安全的**：三个 subject 既无 publisher（CLI 里根本没有 `kick`/`rotate-pin`/`tag` 动词）也无 subscriber，
   `pin_hash` 零 UPDATE。加上 24h JWT TTL + 不落任何 JS stream filter，删除与 revert 都能在一个 TTL 内收敛。D15/D16 的三问答得扎实。
5. **A11 的四份 hash 收口值不变已验证**：`tokenhash.Sum(raw)` 与三处旧实现逐字节相同；测试用外部可验证的固定 digest
   （`e3b0c442…` / `ba7816bf…` / `51643eac…`）而非与另一个 Go 表达式对比 —— 这个选择是对的，存量 `port_allocations.token_hash` 不会失配。
   `hexSHA256` 的 "canonical" 假措辞也一并订正了（D19 兑现），且新注释诚实标注了「这里哈希的是文件内容不是 bearer token，不应假定联动」。
6. **A9 的锁纪律逐点核实全部正确**：`tunnel.go:353-360`（快照）、`:401-403`、`:435-449`（if 分支与 fallthrough 各自 Unlock）三处都在 `s.mu` 内，
   且 `fenceSnapLocked(...) != snap` 与旧的三段 `||` 链逐位等价。结构体 `!=` 在将来引入不可比较字段时编译失败也是正确的 fail-loud 方向。
   刻意不把 `s.closed` 并进去（它是生命周期不是 fence 维度）—— 这个判断是对的。
7. **A4 删 `subhttp.Serve` 后把护栏重定向到 `Bind` 是守护强度不降反升**：loopback 拒绝完全发生在 `Bind` 内，
   原测试有可能因 `ServeListener` 先失败而「以错误的理由通过」，新测试不会。
8. **`TestRunFailureMessageSplitsCodeFromDetail` 同时断言 exit class 和 raw code + detail 都留在消息里。**
   第二半才是关键 —— 只断言 class 的版本会放行「把 detail 丢掉换 exit code」这种拆东墙补西墙的修法。
9. **`cmd/tether/poll_test.go:38` 用 interval=30s / grace=2s** —— 这是能真正杀死「只在下一 tick 才察觉取消」实现的参数选择
   （很多同类测试用 interval≈grace，等于空转）。
10. **`TestDrainWithoutRetireIsNotRefusedByTheRetireGate` 带了真正的非空转半边**（用零值 receiver 在下游 nil 依赖上 panic 来证明控制流确实越过了门），
    比常见的「只断言拒绝串」强得多。（判据略宽 —— 它证明的是「panic 了」，宣称的是「越过了门」；可用 `debug.Stack()` 断言含 `ClusterAdmin` 收紧。）
11. **A15 bridge 的并发形状是对的**：`allow()` 持锁、`emit()` 在锁外调 `b.logger.Log`，不会把慢 handler 的时延带进 raft 的锁。
    `b.logger.Log(nil, ...)` 也确实不 panic（Go 1.25 log/slog 三处都有 `if ctx == nil` 兜底），那条 `//nolint:staticcheck` 的理由属实。
12. **A2 的 `handleCapsReq` → `transferGate` 替换逐分支等价**：`store_error`/`session_not_found_or_deleting`/`not_a_member` 同码同序，
    `nid=""` 使其在 member 检查后提前返回，且确实去掉了发给任意 member 的裸 SQLite 串。
13. **硬边界 1（零 wire 变更）我做了独立扫描**：`git diff` 中被删除的错误码字面量全部在同一 diff 中被重新加入
    （仅剩 context/localhost/node_id/time 四个非错误码），**没有任何错误码的字符串值发生改变**。`proto.ProtoVersion` 未动。
14. **硬边界 3（不碰部署面）**：`git status` 中无 `scripts/install.sh` / nats.conf 渲染 / systemd unit 的改动。
15. **审查期间主进程的响应速度值得记一笔**：B1（安全回归）、`loopSet.Join` 首次超时即 return、tokenhash 伪造 digest、
    metric `_total` 渲染成 gauge、码数 65→62、`DrainNode` godoc、`TestWireCodeNamespacesAgree` 缺失 —— 7 项在审查窗口内就已修复。

---

## 6. 被驳回的 finding

这一节是本报告可信度的凭证。分两类：**经核实不成立**（含 1 条被复核者标 fatal 的伪造证据链），
以及 **`[STALE]`：审查期间已被主进程修复**（主进程不必再处置）。

### 6.1 经核实不成立

| # | 主张 | 我的核实 | 判定 |
|---|---|---|---|
| R1 | deletion-safety 声称「`grep OpStateDrainConfirmed\|DRAIN_CONFIRMED --include=*_test.go` 零命中，故 retire 移除步骤无测试到达」 | **这两个标识符在本仓根本不存在**（我 `grep -rn` 全仓零命中，非测试文件也零）。真正的常量在 `internal/cluster/operation_ops.go:27-47`（`OpStateRaftRemoved="RAFT_REMOVED"` 等），执行 `RemoveServer` 的是 `cluster_operation_controller.go:939` 的 `case cluster.OpStateRaftRemoved`。**对一个不存在的名字 grep 出零命中，再把这个零当作覆盖缺失的证据，是伪造的证据链。** | **驳回（fatal，采纳复核者）** |
| R2 | 同上 finding：「A13 把 §8.1 retire 移除顺序的唯一端到端覆盖删掉了」「最不可逆的两步现在没有任何测试跑过」 | 被删的 d7 断言驱动的是 `c.admin.DrainNode(c.ids[2], true, ...)` —— 即 A13 删掉的**同步**路径自身。删除针对已删代码的测试，逻辑上不可能减少幸存代码（`cluster_operation_controller.go:939-980`）的覆盖。且 `grep -rn OpKindRetire --include=*_test.go` 命中 **6 个文件**：`js_placement_gate_test.go:252` 直接 `driveRetire(rop, substrate{phase:"VOTER", inRaft:true, isVoter:true, numVoters:2})`、`g3_seed_helper_test.go:111-115` 断言到达终态 `OpStateRetired`、`cluster_phase_fluidity_external_test.go:111` seed 在 `OpStateRaftRemoved`。「只有两个测试、都只覆盖拒绝分支」作为事实陈述是错的。 | **驳回**（残值仅一句：`batch-a-progress.md:144-146` 的覆盖声明措辞可收紧为「同步路径的覆盖随代码一起消失」） |
| R3 | deletion-safety 声称留下了孤儿函数 `d7RetireRosterRemovalLegacy`（全仓零调用、godoc 残句） | `grep -rn d7RetireRosterRemovalLegacy` **全仓零命中**，该符号不存在。`git diff test/d7/integration_test.go` 显示只做了断言替换，没有新增任何函数。 | **驳回（伪造）** |
| R4 | deletion-safety 声称「A6 漏做 D13-2 且**没有登记**，progress 的『唯一未做项』只列了 A8-3」 | `docs/reviews/batch-a-progress.md:256-268` 有一整段标题为「**未做的项（诚实登记）**」的 `D13 第 2 步 — 删 IssueUserJWT + AccountPublicKey（内审 M2 指出，此前漏登记）`，明确写下决定与理由。**「登记不实」的指控是错的**，而这正是该 finding 判 major 的主要依据。另：plan D13 的第 2 条（同 PR 删）与第 3 条（推迟到批次 B，前置条件是给 `IssueUserJWT` 补 audience）**内部互斥** —— 一个已删的函数无法被「补 audience 并证明等价」。且这两个符号在 `internal/auth/jwt_test.go` 与 `test/p1/foundation_risk_test.go` 有 7 处活测试引用（含 panic 回归防护）。 | **驳回**（残值：可在 plan D13 处标注 2/3 冲突及所取读法） |
| R5 | gate-vacuity blocker#2 标题「A7 的 ACL 对账……删掉一条活授权，**全包依然绿**」 | 复核者在干净副本上删 `alert.ack` 授权后 `go test ./internal/auth/` → **FAIL**（`TestD8bMemberAlertACLCarveOut`，`permissions_test.go:115`）。审查只跑了 `-run TestACL` 却把结论推广成「全包依然绿」。且反向探测 18 条被订阅的 `ctrl.by.*` subject 全部有匹配授权、**零漂移**。 | **降级 blocker→major**（见 M4，只保留 doc 不实那半） |
| R6 | gate-vacuity 的 wellDone：「把 helper 参数经局部变量洗一道仍被 undeclared-file 规则接住」 | **错的**。`error_code_coverage_test.go:274-289` 的 CallExpr 分支从头到尾不写 `unresolved`（无 default、`Ident`/`SelectorExpr` 无 else）；undeclared-file 兜底只对 `:241-256` 的 KeyValueExpr 生效。这条褒扬反而**掩盖了 M2 的真实规模**（新文件里的局部变量实参也照样逃逸）。 | **驳回褒扬，M2 相应扩大** |
| R7 | error-semantics 把「`internal/proto/codes.go` 是第二个无人执行的真相源」判为 **blocker** | 三条 blocker 判据都不满足：(1) 我逐值核实 **32 个常量今天全部与活发射点字面一致，零 wire 漂移**；(2) 零引用 = 零行为，无现网可触发缺陷；(3) 出问题的是 doc 注释，真正的 gate 不空转。且它自己的 why 段写着「今天没有现网后果」。另：它点名「不存在」的 `TestWireCodeNamespacesAgree` **现已存在**（`cmd/tether/wire_code_namespaces_test.go:28`）。它给的修法 (A)「把发射点改成引用常量」还与 plan D3 相冲突。 | **降级 blocker→major**（见 M9，只保留「SSOT」措辞 + D4 三份声明） |
| R8 | error-semantics findings #2/#4/#6 的框架：「A1 让监控无限退避重试 / A1 修好一个又新造一个」 | **因果系统性错误。** `git diff cmd/tether/error_hints.go` 证明：A1 之前 `runFailureMessage` 返回裸 `fmt.Errorf`，冒泡到 sink 由 `classifyExit` 落 70；A1 之后是显式 `Class:70`。**前后完全等价**。`download_failed`(75) / `state_write_failed`(70) 同理，且该报告自己的 finding #12 已论证「70 与 75 在 usage.md 的重试规则下完全等价」。三条描述的现象在批次 A **之前**就是现状。 | **驳回定性**（缺陷本体保留为 M10/m13，性质改为「遗漏」而非「新造」） |
| R9 | error-semantics #5：「§9.13 与 `exitcode.go:22-24` 的保留区间条款**被 A1 变成了假话**」 | `cmd/tether/run.go:155` 的 `return runFailureMessage(first.Reason)` 是从 RunE 返回的，A1 之前它返回裸 error，照样进 sink 退 70。所以「exec/run never reach the sink」**先于本批次就是假话**；A1 只是把可观测值域从 {70} 扩到 {64,70,75,77}。 | **驳回定性**（文档确实该写两段语义，但不是本批次造成的；建议顺手改，不计入 A1 债务） |
| R10 | error-semantics 多处引 `docs/usage.md:1546` 作为「健壮重试规则」 | 行号系统性漂移：`保留区间` 在 1537、`exec/run 透传` 在 1540、`健壮重试规则` 在 **1542**。1546 已落在本批次新增的订正块里。引文**内容**正确，结论不受影响。（该报告对 `.go` 文件的行号我抽查 20 余处全部准确。） | **仅订正锚点** |
| R11 | deletion-safety：「A8 的须知掩护不了 `:257` 和 `:1219`」；复核者反称「只有 `:1219` 逃出覆盖」 | **两边都错。** `grep -n "^## " docs/architecture.md`：§B=122、§H=1179、§I=1373。`:257` 在 §B 内，`:1219` 在 §H 内 —— **两者都在 §A–§K 覆盖范围内**。 | **两边均订正**（m12 相应降级，只保留 `permissions.go:45` 的指针问题为核心） |
| R12 | deletion-safety：「两处代码注释已经在指着不存在的 release note」 | `grep -rn "release note" --include=*.go .` **唯一命中** `internal/broker/clusterstatus.go:799`。且本增量尚未 commit、未打 tag，仓库无 CHANGELOG 载体（release note 走 goreleaser 在发版时产出）——「文件不存在」在 step 4 属正常状态。 | **降为发版提醒**（见 §7 的 D22） |
| R13 | error-semantics #3：「`internal/broker/run.go:37/43/53/68/80/86/96` 共 **9 处**」 | 实测该文件的冒号拼接实参站点是 **8 处**：37/43/53/68/**71**/80/86/96（`:71` 是 `"node_offline: status="+string(status)`，被漏列），而列出的行号只有 7 个却写作 9 处。结论（这些站点对 gate 不可见）不受影响。 | **仅订正计数**（M2 已用正确数字） |
| R14 | finding「`unresolvedCodeSites` 共 11 条」 | `awk '/^var unresolvedCodeSites/,/^}/' \| grep -c '^\t"internal/'` → **10**。 | **仅订正计数**（M3 已用 10） |
| R15 | concurrency 视角标题「A15 在生产上**完全**不生效」/「A15 的全部产出就是给 raft 一个声音」 | A15 有三件产出，**只有 bridge 死**：`internal/broker/observability.go:252-261` 的 leadership 边沿 Info 走 `b.cfg.Logger`、`runObserveLoop` 无 leader-gate 提前 continue，生产正常；`brokermetrics/metrics.go:93-95` 的三条计数器也正常（该报告自己的 finding 3 就在攻击后者的读取时机）。误判会把分诊导向「A15 整项返工」。 | **保留缺陷，收窄范围**（见 M1） |
| R16 | concurrency 视角 wellDone：「A5 Join 首次超时即 return 是**净改善**（有界预算）」 | **方向讲反了。** 旧 `for i:=0;i<cap;i++ { select{...case <-time.After(10s): Warn} }` 不 return 的含义是**每个还没回来的 loop 都还能再拿一个完整 10s 窗口**；新 Join 首次超时立刻 return，剩余 loop 一个都不再等 → `clusterShutdownOrdered` 推进到 `nc.Drain`，正是 `loopset.go:25-33` 自己描述的 publish-after-Drain hazard。**主进程已按此修复**（现 `loopset.go:104-119` 为 per-loop 预算 + continue，注释引 "batch-A review M1"）。 | **驳回褒扬；缺陷已修** |
| R17 | concurrency #6：JSON 下会渲染成 `{"time":"…", …, "time":"3s"}` | slog 的 JSONHandler 把 `time.Duration` 序列化成**纳秒整数**：实测 `{"time":"2026-…","level":"WARN","msg":"failed to contact","server-id":"n2","time":3000000000,"component":"raft"}`。重复 key 的结论对，具体后果描述错（不是 duration 字符串，是裸整数 3000000000）。TextHandler 那半（`time=3s`）写对了。 | **保留（收进 M7），订正取证** |
| R18 | concurrency #4（`loopSet.Go` 的 `done` 锁外初始化）标 major | 我核实**今天零条可达路径**：唯一调用方 `clusterwrite.go:432-441` 是 5 次顺序调用；`wireClusterLate`（Run 的 goroutine）与 `clusterShutdownOrdered`（`broker.go` 的 defer）在同一 goroutine 上。纯锁协议卫生问题。 | **降级 major→minor**（见 m3） |
| R19 | deletion-safety #4 把「不可达的 last-voter 硬拒绝」计入 major | `DrainNode` 共 8 个调用点，**生产只有一个**（`internal/broker/clusterstatus.go:810`）且已硬编码 `false`，其余 7 个全在 `_test.go`。故 `:117-121` 不可达、`:103` 的 retire 拒绝在生产也不可达，无任何行为面；而 last-voter 保护在活路径上双重存在（`cluster_operation_controller.go:201` + `:1019`，后者有 `TestC4RetireGateLastVoter` 覆盖）。 | **降级 major→minor**（见 m1；同 finding 的 godoc 半保留为 m11） |
| R20 | error-semantics #7（`cutover_revival_failed`→70）标 major | 该 finding 自己登记「现网影响为零……账目错误，不是行为缺陷」，且实际退出码来自 `cluster_add_drive.go:555 → haltAdd → unavailErr` = 69，表项从不被查。 | **降级 major→minor**（并入 m13） |

### 6.2 `[STALE]` —— 审查期间已由主进程修复，主进程不必再处置

| 主张 | 当前树核实 |
|---|---|
| A12 空 host fail-open（**blocker**） | `httplisten.go:73-83` 已改 `if host == "" { return false }` + 新增 `internal/httplisten/bind_test.go` 行为测试（6 个方向 + 反向）。`go test ./internal/httplisten/` 绿。见 B1（仍有 2 项加固建议）。 |
| `loopSet.Join` 首次超时即 return | 已改为 per-loop 预算 + `timedOut` 计数（`loopset.go:104-125`），注释引 "batch-A review M1"。 |
| `tokenhash_test.go:38` 是伪造 digest 且永不被比较 | 已改为真值 `51643eac9777b63a7b268174d1fd4276daedec9bc9ea0bc6e5abf69047bc54f6`，早退特例已删，三条走同一路径。 |
| `TestWireCodeNamespacesAgree` 不存在（codes.go 点名一个不存在的测试） | 已存在于 `cmd/tether/wire_code_namespaces_test.go:28`，带非空转自检（`len(shared)==0` 硬失败 + 每条必须有 exit class）。**残留的只有 `codes.go:15` 的「SSOT」措辞**，见 M9。 |
| 三条 audit metric 名带 `_total` 却渲染成 gauge | 已加 `c()` counter helper（`internal/brokermetrics/metrics.go:78-81`），三条改用 `c(...)`，注释注明 "Batch-A review m5"。 |
| 码数 65 与实测 62 不符、progress 分项相加=66 | 已全部订正为 62（`docs/usage.md:1544`/`:1550`、`batch-a-progress.md:26`/`:287`），progress 还留了订正记录。 |
| `DrainNode` godoc 仍描述已删的 retire/AllAtTarget/streamsReady 全序 | 已重写（`clusterdrain.go:74-87`）。**`RemoveNode` 的 godoc 仍未改**，见 m11。 |
| A6 的 D13-2 漏做且未登记 | 「漏做」属实但**已登记**（`batch-a-progress.md:256-268`），见 R4。 |

---

## 7. plan 决策 D1–D24 兑现核对表

| # | 决策 | 状态 | 依据 |
|---|---|---|---|
| D1 | 扫描器规格分三段，每段各配合成自检样例；helper 名单本身有断言 | **部分** | 三段自检存在且实测有效；`TestCodeCarryingHelperListIsComplete` 实跑抓到过 2 个漏登记。**但第 3 段「`Code:` 指向变量/函数返回值 ⇒ 硬失败并要求显式豁免」在 form 3 上未实现**（`error_code_coverage_test.go:274-289` 无 default）→ **M2** |
| D2 | Step 1 验收 = build + `go test ./cmd/tether/ ./internal/broker/` + 人工 diff；常量值一致性由 Step 3 守门保证 | **❌ 后半未兑现** | 守门从不读 `codes.go` 的常量值；改一个常量值编译通过、测试全绿 → **M9** |
| D3 | 只把跨包共享的码搬进 proto（判据：同时出现在 broker 与 error_hints.go） | ✅ | progress:290 记录判据精化为「按传输分界」；`codes.go:18-30` 的 scope boundary 写清了 proto/adminsock 两个命名空间 |
| D4 | `dataplaneNotConvergedCode` 可以搬进 proto | **❌ 反向恶化** | 现有**三份**声明：`internal/broker/home_convergence.go:55`、`cmd/tether/error_hints.go:231`、`internal/proto/codes.go:123`（后者零引用）。D4 承诺的「比今天更强」未发生 → 并入 **M9** |
| D5 | `home_broker_restart` 进豁免白名单并注明 audit-only；白名单每条带理由 | ✅ | `error_code_coverage_test.go:86-87` 条目 + 理由；`TestAllowlistEntriesStillHaveEmitters` 防腐烂 |
| D6 | 不写「所有退出码来自分类器」式全局断言，排除两个保留区间 | ✅（附带债务） | 守门未写该断言。但 A1 Step 4 之后 `run` 的**启动期失败**已进分类器（可出 64/70/75/77），而 `docs/usage.md:1540` 与 `cmd/tether/exitcode.go:22-24` 仍写「不入分类器」—— 该漂移**先于本批次存在**（R9），建议顺手改文档说明两段语义 |
| D7 | Step 4 的拼接改动中运维可见字符串一律字节不变 | ✅ | git diff 独立扫描：无任何错误码字符串值改变；`transferRefusalErr` 豁免保留 |
| D8 | 反向做，不改 `transferGate` 签名；`store_error` 明细只进 broker 日志 | **部分** | 签名未改 ✅；`handleCapsReq` 已收口 ✅；**「明细进 broker 日志」未落地** —— `transferGate` 三处仍静默丢 err → **M13** |
| D9 | `port.Revoke` 改为「删假 godoc、保留函数」 | ✅ | `internal/port/port.go:405-416` 的新 godoc 如实说明它是 pre-fix 版本、`port=?` 单条件匹配就是那个 race，并写明保留理由（`test/cluster/equiv_test.go:422` 用它做对照） |
| D10 | `subhttp.Serve` 的删除必须同 commit 重定向护栏测试 | ✅ | `internal/subhttp/p13_external_review_test.go:14-25` 已重定向到 `Bind` 并说明理由（守护强度不降反升） |
| D11 | `permissions.go:42-46` 的误导性 godoc 订正 | **部分** | 代码侧已订正（`permissions.go:47-54` 写明「两半都是假的」）✅；**但订正后的文本把读者指向 `see architecture B.2 note`，而 `docs/architecture.md:257` 正是那句原话且未改** → **m12** |
| D12 | `deadcode` 重跑须带全部 8 个 build tag + 处理 `pkg [pkg.test]` 变体 | 未验证 | 属工具执行细节，`tools/` 未纳入本次审查范围（未 commit） |
| D13 | 只取第一方案；签发路径一字不动；同 PR 删 `IssueUserJWT`+`AccountPublicKey`；「消掉第二份 JWT 实现」推迟批次 B | **部分（已诚实登记）** | 第 1 步 ✅（`cmd/tether/serve.go:464` 接上 `LoadAccountSigner` kind 校验、丢弃 signer；`serve_authseed_test.go` 已在）；**第 2 步未做**，`batch-a-progress.md:256-268` 明确登记并给了理由。另：D13 第 2 与第 3 条互斥（见 R4），建议在 plan 标注 |
| D14 | 落地后静态确认 `test/simcluster/lib/secrets.sh` 产出的是 account seed | ✅ | `test/simcluster/lib/secrets.sh:28` `"$NK_BIN" -gen account > account.nk`；`:10` 注释也写明；`:109` 的 gen-N 铸造同理。drill 不会因 A6 起不来 |
| D15 | 删授权前回答三个问题，答案写进实施记录 | ✅ | `acl_reconcile_test.go:33-42` + progress §A7 记录了 revert 语义（24h JWT TTL）、无 JS stream filter、零 publisher/subscriber |
| D16 | 验证「NATS core 即刻丢弃」的前提 | ✅ | 同上，且我独立核实三个 subject 无 publisher 无 subscriber、`pin_hash` 零 UPDATE |
| D17 | A10 范围收窄到 2 处 | ✅ | `internal/broker/transfer.go` 的 `finalizeTransfer` 两条路径逐行等价（watchdog: emit→delete→remove；失败路径: emit→delete→cancel→remove），`cancelEntry` 做成显式布尔 |
| D18 | 删掉两条经证伪的「风险」，不写进 plan | ✅ | plan 中未见该两条 |
| D19 | 是四份不是三份；`hexSHA256` 的 "canonical" 措辞必须订正 | ✅ | 四处全部收口到 `internal/tokenhash`；`cmd/tether/transfer.go:877-885` 的 "canonical" 已订正，且新注释诚实标注「这里哈希文件内容不是 bearer token，不应假定联动」 |
| D20 | 三个 Bind；AST 断言覆盖三个包的具名入口（`BindLoopback`/`BindAny`）；断言 httplisten 之外无直接 `net.Listen` HTTP 监听点 | **部分（偏离未登记）** | 三个包全部收口 ✅；AST 断言存在 ✅。**但实现改成了 bool 参数 `Bind(name, addr, requireLoopback)` 而非具名入口** —— 这个偏离既是 **m6** 的根因、也是 **B1** 的结构前提（具名入口下不存在「把两份 `isLoopbackHost` 合并成一个」这一步）；progress §A12 记了改动但未标注为对 D20 的偏离。「httplisten 之外无 `net.Listen`」的断言只扫 3 个硬编码包 → **m6** |
| D21 | 删掉「A12 会把 shutdown 推过 `nc.Drain`」这条编造的耦合 | ✅ | plan 中已删；实现也确认三个 listener 跑在无人 join 的裸 goroutine 中 |
| D22 | release note 的取证方法订正；须写明 `a0704c3..9b99c0e` 自建二进制的残余面 | **❌ 未做** | `grep -rn "release note" --include=*.go .` 唯一命中 `internal/broker/clusterstatus.go:799`（指向一份不存在的文档）；仓库无 CHANGELOG 载体。**属发版时的动作**（本增量尚未 commit），但需登记 → 见 R12 |
| D23 | 无条件加 30s 去重 + 速率上限，理由写成「便宜的保险」 | **部分** | 窗口已实现（`raftlog.go:62-79`）✅，理由措辞也按 D23 写 ✅；**但零测试、去重键只有 msg 会吞掉 peer 身份** → **M7**。且整条链在生产上不通电 → **M1** |
| D24 | `admin runtime --json` / `export-incident` 按「加 omitempty 字段不 bump」处理，`schema_version` 不动 | **部分** | A15 的三个计数器按此处理 ✅；**A5 往既有 `Reconcilers` 数组里加了异质行（LastTick 永不推进、`IntervalMS`/`Skips` 无 omitempty），D24 从未评估这一种兼容性问题，且 `internal/adminsock/protocol.go:370-372` 的契约文本未改** → **M8** |

**汇总**：✅ 完全兑现 12 条（D3/D5/D7/D9/D10/D14/D15/D16/D17/D18/D19/D21），
部分兑现 8 条（D1/D6/D8/D11/D13/D20/D23/D24），未兑现 3 条（D2 后半、D4、D22），未验证 1 条（D12）。
其中 **D13 与 D22 已被 progress 或流程阶段合理解释**，真正需要处置的是 D2/D4（→M9）、D1/D8/D20/D23/D24（→M2/M13/m6/M7/M8）。
