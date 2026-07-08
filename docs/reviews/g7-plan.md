# G7 — 数据面再均衡 + 可观测/告警（#2 / #18 / #16 / #20③）— Implementation Plan

> **状态：FINALIZED（主进程，Stage-A step 2）。** 由 6 drafter + 3 adversarial critic 的对抗性 Workflow 综合，经主进程现场 file:line 核验 + 裁决锁定。Post-1.0 叶子增量；覆盖 roadmap G7 的 gotcha **#2 / #18 / #16 / #20③**。走 3 阶段 7 步。
> **打包决策（用户拍板 OQ-A）：不拆分——一份 `g7-plan.md`，内部按 `G7a(#2→#18) → G7b(#16+#20③)` 顺序实现，末尾一次合并外审。** 内部分段保留只为实现/回归分档，不产生第二个外审门。
> **核验地基**：全部 load-bearing 代码事实已现场核实（下标 [已核实]），推翻了 3 份 draft 的关键误判（尤其 #16 exit-3 与 #2 的 residual 范围）。

---

## 0. 主进程定稿裁决（keystone decisions）

### 0.0 已核实的代码事实（定稿地基）
- **[已核实] #2 residual = 仅默认视图**：`proxyStatusNodes`（`internal/broker/proxy.go:989-1017`）在 `pport!=0` 时无条件 `e.PublicHost = b.publicHostFor()`（:1012），且 **SELECT 里根本没有 `pa.home_broker`**。而 `--cluster` 路径 `proxyStatusNodesCluster`（proxy.go:405-425）**已是 N3-正确**（`clusternodes.LookupByNodeID(alloc.HomeBroker).PublicHost`，gated on `proxyHomeHealthy` + `proxyInSubRender`）；`/sub` body（`internal/subhttp/subhttp.go:190-206`）**已 per-home 正确**（JOIN `cluster_nodes cn ON cn.node_id=pa.home_broker`，`cn.phase='VOTER' AND cn.public_host!=''`）。→ #2 只需修默认视图这一处。
- **[已核实] #16 exit-3 全链已 wired**：`cluster_health.go:72` `ForceSingleActive: forceSingleActive(db)` → `cluster_status_nats.go:67-68` 聚合 → `ctlExitCode:124-125 return 3`，测试 `cluster_status_nats_test.go:77` 已锁。Draft 4/5 的「broker 端没 stamp」诊断是错的——2026-06-29 的 exit-0 是过时 `v0.0.0-dev` 笔记本二进制（gotcha #17），非 HEAD bug。#16 exit-3 = **verify-and-lock only**。
- **[已核实] #20③ 吞错点**：`alert_reconcile.go:185` 把 Observe 错误 log-drop。`ObserveReplicas`（audit_publisher.go:512）在 503 时经 `CollectStreamState` **返回 error**（`oerr!=nil` 分支）——**不是** `rep.Observed==false`。而 `replication_degraded` raise 门在 `else if rep.Observed`（:187）→ **503 期间永不触发**。→ 复用 `replication_degraded`（Option A）**结构性无效**，从选项集删除。
- **[已核实] 新 alerts.kind = FSM-poison 轴**：`alert_ops.go:28-31` + `PlanAlertRaise`（:69-73）自校验——out-of-enum kind 在**每个 replica** fail 掉 0009 CHECK（走 `genericExecApplier`、非 poison-skip）→ fail-stop、混版 replay 砖化。ProtoVersion=2 **不覆盖此轴**。下一个空闲 migration = **0018**（`0015_cluster_operations.sql` 已占）。
- **[已核实] G2 已有 JS-degraded banner，但 socket-only + config-推断**：`clusterstatus.go:331` 在 `forceSingle && natsconf.Preflight().IsClusteredJetStream()` 时拼 "DATA-PLANE DEGRADED" 进 `rep.Banner`。它 (a) 只在 socket status、(b) 只覆盖「force-single + clustered conf」一种 cause、(c) 靠 conf 推断而非实测 503、(d) `--remote` 看不到。#20③ 需要**运行时实测、cause-agnostic、`--remote` 可见**的信号。
- **[已核实] `ClusterHealthResp` 已带合成 bool 先例**：`internal/proto/alerts.go:12` 的 `ForceSingleActive bool` 就是「broker 自报 + ctl 合成进 verdict」模式；加 `JetStreamUnavailable bool`（omitempty）**byte-additive**、老 broker 省略→false→graceful。
- **[已核实] #18 机器全在**：`rebalanceProxyHomes(dryRun)`（proxy_rebalance.go:31，leader-gated、`len(voters)<2` no-op）、`movableProxyAllocs`（:225 仅 `pa.name='__proxy__' AND state='ALLOCATED'`——regular/rebuild-OFF expose **结构性**不可选）、`eligibleProxyHomes`（:195 VOTER+cert+public_host+`homeReachable`）、`NonTerminalOperations`（operation_read.go:68）、`forceSingleActive`（cluster_health.go:167）、`last_rehome_at`（migration 0017）。observeOnce（observability.go:146-166）已建 `prevDown`，decide-loop 与 orphan-clear pass（:170-188）是两段。
- **[已核实] rehome = break-before-make**：`moveProxyHomeTo`（proxy_rebalance.go:161-190）Propose `PlanReassignHome` → 发 `proxy_node_unready` → re-push Home directive 让 agent **重建**在新 home。**不 hold 旧 home 的 public listener** → 缓存客户端指旧 host 会 black-hole 直到 refetch `/sub`。**这是 OQ-D showstopper 的根**。
- **[已核实] staleness lever 效力未知**：`subhttp.go:131 Header.Set("Profile-Update-Interval","1")`。单位（1h vs 秒）、以及 Clash Verge/mihomo 作为 **proxy-provider 消费**时是否 honor 该 header，**均未验证**（mihomo 控制器是不可编程命名管道）。**Stage-B 必先核实。**

### 0.1 逐 keystone 裁决（KD）

| KD | 裁决（ADOPTED） | 理由 / 驳回 |
|---|---|---|
| **KD-1 打包** | **不拆——一份 g7-plan.md，内部 `G7a(#2→#18)→G7b(#16+#20③)` 顺序实现 + 一次合并外审**（用户 OQ-A 拍板）。内部分段仅用于实现/回归分档。 | 用户定夺。外审在末尾一次覆盖两种风险模型（数据面 rebalance-flap/black-hole vs 观测 FSM-poison/ACL/exit-truth），报告须显式分节分档。**驳回**：拆两独立叶子两次外审（用户不选）。 |
| **KD-2 #2 render** | **只改默认 `proxyStatusNodes`**：SELECT 加 `pa.home_broker`、JOIN `cluster_nodes` 取 home 的 `public_host`、**镜像 `--cluster` 路径的 `proxyHomeHealthy` 健康门**；single-mode（`home_broker=''`）→ `publicHostFor()` fallback、列 byte-identical。**不碰** `/sub` 与 `--cluster`（已正确）。 | **驳回**：绕进 `proxyStatusNodesCluster`（改列形态、破解析器）；纯 host-VALUE 改而不加健康门（会给 `/sub` 排除的 exit 显示 host）。render-equivalence 测试**须 scoped 到「`/sub` 实际渲染的 exit」**，非集合相等。 |
| **KD-3 #18 trigger+debounce** | 钉 observeOnce decide-loop 里 `broker_down` **true→false CLEAR 边**（`prevDown[node] && !decision.Active`，仍是 current voter，与 orphan-clear pass 天然分离）；复用 `rebalanceProxyHomes(false)` verbatim；纯 `planAutoRebalanceDecision` 做 per-node return-dwell（**> `proxyRehomeDwell`**）+ 全局 cooldown；gates = `downNow` 空 ∧ `NonTerminalOperations` 空 ∧ `!forceSingleActive` ∧ 无 `__proxy__ last_rehome_at`∈quiet-window。 | **驳回**：任意 `broker_down` clear 触发（会打到 orphan-clear/departed 节点）；AddNode/join 触发（那是 GROW=G4）；边到即发（thrash）；replicated/cron dwell（leader-local in-memory 即可）。**硬约束见 KD-3b。** |
| **KD-3b #18 安全门（OQ-D/E 定稿）** | **#18 机制随 G7 落地，但 auto-fire 受三条硬约束**：(1) **#2 先落**（per-home 渲染正确后才谈 voluntary move）；(2) **cooldown ≥ 有效客户端刷新周期**，且**绝不制造无界 black-hole 窗**（inv-11）；(3) auto-fire **默认姿态由 Stage-B staleness 核实结论定**——若 header 有效/provider interval 可界定 black-hole 曝露 → default-ON；若无效 → cooldown 取保守大值界定曝露 + honest doc，必要时经 **ENV opt-out**（**不加 broker.yaml key**，保 G7 off deploy-tier）。`cluster rebalance proxy --dry-run` 作 manual preview。 | 采 synth OQ-D(1)+OQ-E(1) 的骨架，但**收紧**为「default 姿态是 Stage-B 决策、以 black-hole 有界为不变量」。**驳回** OQ-E 无条件 default-ON（与 black-hole showstopper 冲突）、OQ-D(3) 无 gate、make-before-break 本批（较大数据面改造，DEFERRED）。 |
| **KD-4 #20③ 检测** | un-swallow `oerr`；leader-local **wall-clock `jsDownSince`**；`classifyJSUnavailable(err)` 仅认 `jetstream.APIError` code **10008**（`errors.As`，本地 const；**Stage-B 先 trace `ObserveReplicas` 实际 503 error 形态**再定分类器）；持续 dwell **≥60s**（保守，防 grow/meta-reform 误报）且未已 signal 才 raise；首个 positive Observe clear；injectable clock。cause-agnostic（force-single remedy 进 message、不进 trigger）。 | **驳回**：单 tick 503 即报（每次选举/reconfig 误报）；N-consecutive-tick 计数（脆、依赖 cadence）；gate 在 `force_single_active`（耦合单一 cause）。threshold 保守取 ≥60s。 |
| **KD-5 #20③ 告警接线（OQ-B 定稿）** | **Option C：合成 `ClusterHealthResp.JetStreamUnavailable bool`**（broker 端 sustained-gated、ctl 合成进 banner/verdict，如 `ForceSingleActive`）。零 migration、零 poison 轴、ProtoVersion=2、老 broker 省略→false→graceful、level-triggered auto-clear，**且顺带补上 `--remote` 看不到 JS-503 的缺口**。与 G2 socket banner 协调统一。 | 采 synth OQ-B(1)。**驳回**：Option A 复用 `replication_degraded`（结构性无效）；单-release 新 kind + migration（FSM-poison）。**注**：`alert ls` 看不到 JS-503 行（它经 status/banner surfacing）；若日后硬需 ack-able 持久行 → 唯一安全路径是**两-release rollout**（rel N widen 0009 CHECK 无 writer、rel N+1 加 writer），**本批不做**。 |
| **KD-6 #16 --remote + exit（OQ-C 定稿）** | exit-3 = **verify-and-lock**（+ signature-guarded 回归 + flag help 对齐，**不 reimplement**）；`seeds show --remote` = **复用 G3 roster-pull manifest**（`fetchManifestOverNATS→m.Seeds`，零新 subject/ACL），DATA 契约 0/69（force-single 下 seed 可服务、**不** exit-3）；`homes --remote` = **aggregate-only per-broker counts**（OQ-C(1)，ACL-clean、正中 #18「分布是否均衡」运维需求），**绝不** wholesale `buildHomesReport()`（无 sid 过滤→跨会话泄漏）。删 `MarkFlagsMutuallyExclusive("homes","remote")`（cluster.go:172）、保 offline/watch 互斥。 | 采 synth OQ-C(1) aggregate-only。**驳回**：broker 端 exit-3「stamp」修（no-op、破 precedence）；homes wholesale 给 member（跨会话 ACL 洞）；sid-scoped homes（独立 nice-to-have，DEFERRED）；`--homes --remote` 带 0/2/3（与 socket `--homes` 无 exit 契约冲突）。 |

### 0.2 OQ 定稿汇总（open to 外审 veto）
- **OQ-A 打包** → 用户拍板：**不拆，合并一个 G7**，内部 G7a→G7b、一次外审。
- **OQ-B #20③ 接线** → **Option C 合成 bool**（新 kind/migration DEFERRED，仅硬需 ack-able 行时两-release）。
- **OQ-C homes --remote** → **aggregate-only counts**（sid-scoped DEFERRED）。
- **OQ-D #18 前提** → **Stage-B 先验 staleness lever + break-before-make（已核实）；auto-fire 受 cooldown + #2-先落约束，black-hole 有界为不变量**。
- **OQ-E #18 默认/kill-switch** → **默认姿态 = Stage-B staleness 结论；无 broker.yaml key；必要时 ENV opt-out；`--dry-run` 作 manual preview**。

---

## 1. 逐 gotcha 工作项（file:function 落点；实现顺序 G7a→G7b）

### 【G7a·先】#2 — proxy home-host 渲染 + 订阅可用性 + staleness

- **W2-1｜默认 `proxy status` 修 host 错标**（核心）：`proxyStatusNodes`（proxy.go:989-1017）——SELECT 加 `pa.home_broker`；cluster 模式每 exit 走 `clusternodes.LookupByNodeID(home).PublicHost` 并**加 `proxyHomeHealthy` 健康门**（镜像 :410-416）；single/空-home → `publicHostFor()` fallback，列 byte-identical。*风险中。*
- **W2-2｜verify-and-guard `/sub` 与 `--cluster` 已正确**：subhttp.go:190-206 + proxy.go:405-425——**只加 golden/equivalence 测试、不改逻辑**；render-equivalence 断言 scoped 到「`/sub` 实际渲染的 (sid,nid)」。*风险低。*
- **W2-3｜SubURL/SubURLPrefix 去单点 + SPOF marker**（次要、低优先）：`subURLBase()`/`subURL()`/`SubURLPrefix`（proxy.go:1029/1036/355）+ leader-minted SubURL（proxy_cluster_wire.go:138）——统一从 operator-配置的 `SubURLBase` 渲染；cluster 模式未配置时追加显式 **broker-local SPOF marker**（不静默用 per-broker host）；doc 要求多-broker DNS + 各 Caddy 同名。**不重写 URL host**（客户端已缓存）。`--json` schema 不受 marker 影响。*风险中（排 W2-1/#18 之后）。*
- **W2-4｜staleness 信号（best-effort + 诚实 doc）**：`subhttp.go:131` Profile-Update-Interval 改可配（`subhttp.Config`），cluster 默认调低——**但仅当 Stage-B 证实 Clash-provider 消费模式 honor 该 header（OQ-D）**；否则退化为 doc-honest（如 G3 #11）+ 用 #18 cooldown 界定 churn。落 `docs/cluster.md`/`docs/usage.md`。*风险中。*

### 【G7a·后，依赖 #2】#18 — broker 回归后自动再均衡

- **W18-1｜纯决策函数**（新 `internal/broker/proxy_auto_rebalance.go`）：常量 `autoRebalanceReturnDwellTicks`（默认 6≈30s，> `proxyRehomeDwell=3`）/`autoRebalanceCooldown`（默认 5m，**Stage-B 据 staleness 结论校准 ≥ 有效刷新周期**）/`autoRebalanceQuietWindow`（30s）；`planAutoRebalanceDecision(...) (fire, nextPending, cleared)`——DB-free 纯逻辑。*风险低。*
- **W18-2｜leader-gated driver**（同文件）：`driveAutoRebalanceOnReturn(returnedEdges, downNow, now)`——`eligibleUp` via `proxyHomeHealthy`；`gatesClear = downNow 空 ∧ NonTerminalOperations 空 ∧ !forceSingleActive ∧ 无 __proxy__ last_rehome_at∈quiet-window`；fire → `rebalanceProxyHomes(false)`，`rep.Planned>0` 时 emit 汇总事件 + `markFired`。*风险中。*
- **W18-3｜接线进 observeOnce**（observability.go:146-166）：decide-loop 内累加 `returnedEdges`（`prevDown[node] && !d.Active && kind==broker_down`）+ `downNow`；loop 后调 driver；`aerr!=nil` 时整段跳过（fail-safe）。*风险中（勿扰 broker_down/raft_lag/orphan-clear）。*
- **W18-4｜Broker 字段 + leadership-loss 重置**：`broker.go` 加 `autoRebalanceArm`（近 `proxyDwell`）；`runObserveLoop`（observability.go:215-264）wasLeader true→false 时清 pending。*风险低。*
- **W18-5｜secret-free 汇总事件**：`b.pubSysEvent('proxy_auto_rebalanced', {reason:'broker_return', returned_nodes, voters, planned})` 仅 `planned>0`（idle-zero-writes）；**不改** `moveProxyHomeTo` 的 per-move `reason='rebalance'`。additive 事件类型、不 bump ProtoVersion。*风险低。*
- **W18-6｜default 姿态 + opt-out（KD-3b）**：auto-fire 默认由 Stage-B staleness 核实定；若需关，读 **ENV**（如 `TETHER_AUTO_REBALANCE=off`，broker 启动读，**不进 broker.yaml**）。doc 记明 black-hole 曝露上界 = cooldown 期内单次迁移窗。*风险中。*

### 【G7b】#16 — 远程可观测 + exit codes

- **W16-1｜exit-3 verify-and-lock**：cluster_status_nats.go:120-133 + cluster_health.go:72——加 named 回归（枚举所有 summary → exit∈{0,2,3}、force_single→3、no-reply→0）；对齐 cluster.go:160 flag help。**无实现改动。** *风险极低。*
- **W16-2｜flag 互斥修复 + 路由**：cluster.go:114-176——删 `MarkFlagsMutuallyExclusive("homes","remote")`（:172）；RunE 先 `if homes && remote`，再 homes(socket)、remote、offline/watch；**保** homes+offline、homes+watch、remote+offline、remote+watch 互斥。*风险低。*
- **W16-3｜`seeds show --remote`**（复用 G3）：cluster_seeds.go:75-95 加 `--remote/--home/--nats-url`；`clusterSeedsShowRemote` → `fetchManifestOverNATS(ctx,nc,actor)` → 渲 `m.Seeds.{Generation,BootstrapURL,AccountPub,Endpoints}`；nil/未 publish → exit 69。**零新 subject/ACL**。DATA 契约 0/69。doc ≥30s manifest-cache staleness。*风险低。*
- **W16-4｜`homes --remote`（aggregate-only，OQ-C(1)）**：`homes.go` 加 per-broker count 投影（**无 per-SID**）+ 新 responder（镜像 `SubscribeClusterHealth`）+ actor-scoped hyphen-leaf subject（`internal/proto/subjects.go`，保 §13.8 绿）+ `PermissionsForActivatedMember` pub-allow。**绝不** wholesale `buildHomesReport()`。老 broker → ErrNoResponders → graceful。descriptive、无 exit-3。*风险中（ACL 是活雷）。*
- **W16-5｜exit-code 契约文档 + ordering 不变量**：`cmd/tether/exitcode.go:19-24` 注释扩展；每个新 --remote 路径 preflight 错误（无 session/dial fail）**在 `os.Exit(ctlExitCode)` 前**返回 classified ExitError（64/69/75），绝不撞 0..3 或发 1。`docs/cluster.md`/`docs/broker-ops.md`。*风险低。*

### 【G7b】#20③ — 持续 JS-503 告警

- **W20-1｜分类器**：`classifyJSUnavailable(err) bool`（新 `internal/broker/js_health.go`）——`errors.As(err,&apiErr) && apiErr.ErrorCode==10008`（本地 const `jsErrCodeUnavailable=10008`）。**Stage-B 先 trace** `ObserveReplicas`/`CollectStreamState` 真 clustered-JS-no-quorum 503 下实际返回（typed APIError vs ErrNoResponders vs stringified）。*风险中（10008 非 named const）。*
- **W20-2｜sustained 检测 + 合成 bool**（Option C）：`alert_reconcile.go` 加 leader-local `jsDownSince`——首次 503 置 `now`，`now.Sub(jsDownSince)>=JSDownThreshold`（默认 ≥60s）→ 标 broker 自身 `jetStreamUnavailable=true`；首个 positive Observe reset；leadership-loss 重置；injectable `cfg.Now`。*风险中。*
- **W20-3｜合成进 ClusterHealthResp + 渲染**：alerts.go:12 加 `JetStreamUnavailable bool json:",omitempty"`；responder 填该 broker sustained 判定；cluster_status_nats.go 聚合（any true → summary true）+ 进 verdict/banner；ctl 端渲染。**与 G2 socket banner（clusterstatus.go:331）协调**（runtime 检测更广）。exit-code 不改 0/2/3。*风险中。*

---

## 2. 待保护不变量（invariants）

1. **ProtoVersion=2 SSOT 不动 + 老 broker graceful degrade**：全部 additive（新 subject 字符串、`ClusterHealthResp.JetStreamUnavailable` additive bool、`proxy_auto_rebalanced` additive 事件、复用 `PlanReassignHome`/既有 op）；老 broker 无 responder → `ErrNoResponders` → fallback，**绝不** fail-closed/poison；`CanonicalSeedBytes` 等公式零字节改。
2. **无单-release 新 alerts.kind**：0009 CHECK + `genericExecApplier` fail-stop 会砖化混版老 follower（已核实）。优先合成 bool；硬需持久行则两-release rollout（migration 0018）。
3. **ACL / 会话隔离（§5）**：`homes --remote` 绝不经 member-auth 服务 wholesale `buildHomesReport()`（无 sid 过滤→跨会话拓扑泄漏）；用 aggregate-only；新 subject actor-scoped hyphen-leaf 保 §13.8 负测绿。
4. **R3 不静默 de-cluster**：#18 复用 `rebalanceProxyHomes` 只移 `__proxy__` home、从不碰 membership/nats.conf；#20③ 只检测/告警。
5. **do-not-move pinned/rebuild-OFF exposes**：`movableProxyAllocs` 仅 `pa.name='__proxy__'`——regular/rebuild-OFF expose **结构性**不可被 #18 选中（加断言钉死）。
6. **告警 auto-clear**：JS-503 信号首个 positive Observe 即 clear、transition-gated、无 stuck-ACTIVE；若走 store-backed 新 kind 则 clear 须 key on 已提交 ACTIVE 集（跨 failover 存活、新 leader 不 double-raise）。
7. **idle-zero-writes（D5）**：#18 already-balanced return → 0 `PlanReassignHome`；#20③ steady-healthy ∧ steady-down-after-first-signal → 每 tick 0 raft 写。
8. **#2 single-mode byte-identical**：空 `home_broker` → `publicHostFor()` fallback、无 HOME 列、golden byte-stable。
9. **exit-code taxonomy**：0/2/3 专属 status-family、force_single→3、no-reply→0；seeds-remote 数据契约 0/69；preflight 错误在 `os.Exit(ctlExitCode)` 前 classify 到 64/69/75，绝不发 exit 1。
10. **#16 exit-3 既正确链 verify-and-lock、不 reimplement**（Draft 4/5 broker 端 stamp 修是 no-op 且破 precedence）。
11. **#18 black-hole 有界（KD-3b 硬约束）**：break-before-make 已核实 → auto-fire 须在 #2 staleness 界定 + cooldown≥有效刷新周期之后、且 #2 先落；**auto-fire 绝不制造无界 black-hole 窗**（曝露上界 = 单次迁移窗，doc 记明）。
12. **render-equivalence 测试 scoped 到「/sub 实际渲染的 (sid,nid)」** 而非集合相等。

---

## 3. 测试面（per-assertion → gotcha）

### hermetic Go（`make test`；触碰 reconcile/传输/rehome/ctl-refresh 面带 `-race` + 内建 NumGoroutine/fd 泄漏门）

*#2*
1. 默认 `proxyStatusNodes`：B-homed exit（A 应答）→ `PublicHost==B.public_host`；un-homed fallback；non-VOTER/空-public_host home 与 `/sub` 一致（健康门）→ #2
2. render-equivalence（scoped 到 `/sub` 实际渲染的 (sid,nid)）：默认 status / `--cluster` / `/sub` host 一致；`/sub` 排除的 exit 不参与断言 → #2
3. single-mode byte-identical golden（空 home → publicHostFor，无 HOME 列）→ #2
4. SubURL/SubURLPrefix：shared base 下 leader-minted host == status prefix host；未配置 cluster 才现 SPOF marker；single 无 marker → #2

*#18（纯 `planAutoRebalanceDecision` 表驱动 + DB/raft 集成）*
5. flap：dwell 中途 down → 永不 fire；steady return → 恰 fire 一次后 cleared、无 re-fire → #18
6. cooldown 抑制第二次 return；down-during-dwell（另一节点）阻塞；in-flight op 阻塞；force-single 阻塞；recent-rehome(quiet-window) 阻塞；dwell 未达；eligibility-loss-mid-dwell（cert 缺）reset → #18
7. 集成：堆全 `__proxy__` 于一 voter → 模拟回归 → tick dwellTicks → 分布收敛 max−min≤1 + 恰一 `proxy_auto_rebalanced` → #18
8. rebuild-OFF **regular** expose co-homed → auto-rebalance 后**不动**（`__proxy__`-only 断言）→ #18/inv-5
9. idle-zero-writes：already-balanced + return edge → dwell 后 0 Propose/0 事件 → #18
10. orphan-clear **不**触发：retire/force-single 踢出节点**不** auto-rebalance → #18
11. leadership-flap-while-returning：`-race` + NumGoroutine/fd 泄漏门 → #18

*#20③*
12. 分类器：`APIError{10008}`→true；10039/10076/timeout/ctx-cancel/ErrNoResponders/nil→false；wrapped 10008 via errors.As→true → #20③
13. sustained：单 blip<threshold→无 signal；持续>threshold→恰一次；持续后→每 tick 0 raft 写；首 positive→clear 一次 → #20③
14. flap：down→up→down<threshold → `jsDownSince` 在 healthy tick reset、无早发 → #20③
15. leadership-change-mid-outage：合成 bool level-recompute 不依赖 in-memory 跨 leader → #20③/inv-6
16. injectable `cfg.Now`（无 real sleep）→ #20③
17. 合成 bool 进 --remote：force-single + clustered conf 场景 `JetStreamUnavailable==true`、聚合进 `--remote` verdict（补 socket-only 缺口）→ #20③/#16

*#16*
18. exit-3 named 回归：枚举所有 summary → exit∈{0,2,3} never 1、force_single→3、no-reply→0 → #16
19. flag-parse：`status --homes --remote` PARSES；`--homes --offline`/`--remote --offline`/`--homes --watch`/`--remote --watch`/triple 仍互斥 error → #16
20. seeds-remote：manifest 有 → 渲 4 字段；nil → exit 69；**不对称**：force_single 下 `seeds show --remote` 仍 exit 0 → #16
21. homes-remote ACL（load-bearing）：aggregate wire payload **无 per-SID 行**；malformed subject 拒；non-member 拒；§13.8 保绿 → #16 ACL
22. exit-ordering：preflight → classified 64/69/75 **先于** `os.Exit(ctlExitCode)`，never 1 → #16

### e2e 矩阵（`test/e2e/all_phases_test.go`，串行）
- G7a：hermetic proxy-rebalance-on-return 子测试（`-race`+泄漏门，仿 `TestProxyDialMatrix`）——**不**把 clustered-JS-重路径塞进 gated 串行 e2e（routed-JS flake）。#18 dwell/threshold 定时逻辑归**纯单测（injectable clock）**，非 e2e。
- G7b：`status --homes --remote` / `seeds show --remote` / `status --remote` exit-code 子测试。

### 新 sim deploy-tier drill（`test/simcluster/drills/`，按需单跑、不 loop；§5 铁律：暴露缺陷不代劳）
- **G7a drill `NN-g7a-rebalance-return.sh`**：真 N=3，堆 proxy（重启一 broker）→ 断言分布倾斜（RED-first）→ broker 回归 → 断言 **无 operator rebalance** 下 `cluster status --homes` 分布收敛（#18）+ 默认 `proxy status` 每 exit 标真 home host + `curl /sub/<token>` 每出口指真 home（#2）。修好翻普通 GREEN。
- **G7b drill `NN-g7b-js503-alert.sh`**：复用 `20-forcesingle-natsconf` 造 JS-503 → 断言 `cluster status --remote` 现 JS-unavailable hint（#20③，原 socket-only）+ `--to-standalone` 恢复后 auto-clear + `cluster status --remote` force_single exit 3（#16）+ `--homes/seeds --remote` 从 laptop-container 无 SSH 可用（#16）。RED→GREEN。

---

## 4. Scope 边界（合并 G7，内部顺序 G7a→G7b）

### IN-scope
- **G7a（先）**：#2（默认 `proxyStatusNodes` per-home 修 + `/sub`/`--cluster` verify-guard + SubURL SPOF marker + best-effort staleness signal/doc）+ #18（return-edge auto-rebalance，debounced/gated，复用 `rebalanceProxyHomes`，black-hole 有界）。**先 #2 后 #18。**
- **G7b（后）**：#16（seeds/homes `--remote` responder + exit-3 verify-lock + flag 互斥修）+ #20③（sustained-503 检测 + 合成 `ClusterHealthResp.JetStreamUnavailable`）。

### DEFERRED / OUT
- 新 alerts.kind + migration 0018（除非硬需 ack-able 持久行 → 两-release rollout，OQ-B）。
- make-before-break rehome（保旧 listener grace 窗）——较大数据面改造，若 header lever 无效再评估（OQ-D）；本批用 cooldown + honest doc + black-hole 有界约束。
- GROW-触发 rebalance（新空 voter）→ G4 `cluster add`（非 return edge）。
- homes `--remote` per-SID/per-port 全表（ACL，socket-only）；per-follower JS-503 forwarding（leader-observed 足够）。
- 任何新 `broker.yaml` deploy-rendered key（保 G7 off deploy-tier；#18 opt-out 用 ENV）；expose（非 `__proxy__`）rebalance；ProtoVersion bump；G2 force-single 去集群化本体（#20 主体已 G2-done，G7 只告警）。

### 外审分档提示（合并外审须分节）
G7a（数据面：rebalance-flap/black-hole/rehome-vs-reaper race）与 G7b（观测：FSM-poison/ACL/exit-truth）风险模型差一个量级——外审报告须**显式分节分档**，逐 finding 标属 G7a 还是 G7b。
