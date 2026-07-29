# 批次 C 实施 plan（定稿）

> **状态**：阶段 A 步骤 2 定稿。主进程是唯一定稿人。
> **来源**：`docs/reviews/quality-audit/2026-07-25-structural/S1-refactor-roadmap.md` §6（524-570）、§3（106-108）、§7（572-640）。
> **产出方式**：6 条 lane 的多专家对抗性草拟 workflow（起草 → 互评 → 综合 → 完备性稽查）
> + C2 lane 补跑 workflow（原 agent 死于 `server_error`）+ 主进程独立勘察。冲突处一律由主进程裁决，理由记录在 §2。
> **基线**：`HEAD = 0f26330`。**roadmap 写于 `52d3b80`，中间隔四次提交，其引用行号多处已漂移**——本文件的行号全部按 HEAD 复核。

---

## 0. 范围与操作者指令

**操作者指令（本轮）**：批 C 的 **C1 / C2 / C3 全部**在**同一个流程**里并行推进，**不得延后、推迟任何剩余工作**。
若判定某项不该做，允许作出该决定，但**这意味着它以后永不做**——必须记为**带理由的永久决策**，
不允许写成 "TODO" / "后续增量" / "以后再说"。

因此本 plan 有两类条目，且**只有这两类**：

- **§4–§7 的交付项**：本轮必须落地，逐条可由 diff 判定；
- **§3 的永久非目标**：本轮不做，且**以后也不做**，每条带理由与出处。

**停止点**：实现 + 内审 + 采纳内审 finding 全部完成后，**停在外审**（CLAUDE.md §3 步骤 6）。
不 commit、不 `git add`（暂存是外部审查者的工作）。

---

## 1. roadmap 与代码不符之处（实现者以代码为准）

| # | roadmap 说法 | HEAD 上的事实 | 影响 |
|---|---|---|---|
| **R-1** | §6:539「`cluster_operation_controller.go:1152-1166` 有整段 `WHAT IS DELIBERATELY NOT FIXED`」 | 该块在 **`:1194-1208`**；`:1140-1173` 是 `jsPlacementAdvance` 的 timeline-splice 论证，与 force-single 无关 | 按 roadmap 行号 grep 会读到不相干的段落，误以为论证已消失 |
| **R-2** | §6:528「后 4 步（marker / epoch / prune / seeds）」 | 实际是 **5 个动作**：`WaitForLeader`(:263)、`PlanSetForceSingle`(:268)、`newRecoveryEpoch`+`PlanForceSingleEpoch`(:273-283)、`PlanClusterNodePrune`(:294-299)、`deriveAndConvergeSeedsFromRoster`(:300-306)。epoch 是 **mint + persist 两步**，mint 不幂等 | 照「4 步」建 4 个 state ⇒ epoch 每次重试重新 mint 出不同值，split-brain detector 的 durable epoch 变成随机数 |
| **R-3** | §6:551「看门狗改成 `max(transferTimeoutTierB, size/minThroughput)`」 | `transferTimeoutTierB` 有 **3 个其他消费者**（见 §6.1）。只改看门狗 ⇒ cross-home GC 在旧期限删掉仍在传的对象 | **短期半自己造出一个新的数据删除缺陷**，而既有关系断言仍然绿 |
| **R-4** | §6:562「两个渲染器」 | 「拓扑是否收敛」有 **4 个判决实现 + 2 个应判未判的镜像**（见 §4.1） | 只改 2 个 = 修好今天这一次分歧，把**产生分歧的机制**原封留下 |
| **R-5** | §6:541「顺带（低风险，可先单独做）」的愈合 pass | 按字面实现「phase=DRAINING 且无 active op ⇒ 记 INCONSISTENT」会把**每一次成功的 drain** 判成 `cluster doctor` FATAL（exit 64） | 见 §5.1 的订正谓词 |
| **R-6** | §6:530「后 4 步完全可以崩溃可续、prune 失败可重试」 | 方向对，但**异步化 prune 会打断 runbook 的下一步**（§5.2 的证据链） | 决定了 C1 的最终形状：prune 保持同步，op 只做失败重试 |

另有一条 roadmap 完全没提、但决定 C1 生死的事实：**force-single ghost 会 fence 住数据面恢复**。见 §5.2。

---

## 2. 主进程裁决台账（专家意见冲突处）

| # | 冲突 | 裁决 | 理由 |
|---|---|---|---|
| **D1** | C1-arch lane 主张 marker/epoch/prune/seeds 全进 op 梯子；C1-adversary lane 的 DC-1 主张 marker/epoch 必须同步 | **marker/epoch 同步且 LOUD；prune/seeds 同步先试、失败才建 op** | destructive gate 是 `Blocked() = QuorumLost \|\| ForceSingleActive`（`internal/proto/alerts.go:179`）。`RecoverToSelfOnline` 一返回本节点即可写 leader ⇒ `QuorumLost=false`；marker 未落盘 ⇒ 门**全开**，`session rm`/`expose`/`push` 全部放行到一台零冗余、JS 仍 503 的单点上 |
| **D2** | 穷尽预算用 attempt counter（C1-arch §2.6）vs 用复制式 deadline | **复制式 deadline（复用 `catchup_deadline` 列）**；attempt counter 只作补充 | `opAttempts`（`cluster_operation_controller.go:837-844`）是 leader-local map，leader 翻转/进程重启即归零；**且只在 step 返回 error 时 ++**，而 advance-after-observe 最重要的失败类是「propose 返回 nil 但观测恒假」（`RowsAffected==0`、poison-skip、bounded-stale 落后）——那一类**一次也不计数**。仓内正解就在隔壁：`jsPlacementAdvance:1148` |
| **D3** | 愈合 pass 用「进程内认领 + deadline + phase 闸」清 marker | **收缩到只清可证明的孤儿；半完成的 drain 只报不清** | `DrainNode` 的正常失败出口（`home_convergence.go:79-80` `ErrDataPlaneNotConverged`）**明文让运维再跑一次**，期间 marker 在、phase 仍 VOTER、无 op 行、进程内认领已释放。这是**两次运维尝试之间的常驻状态**，不是 30s 窗口。而此时 marker + `broker_draining` 告警是那次半完成 drain **唯一**的 status-visible 证据（phase 还是 VOTER） |
| **D4** | C1-arch lane 主张愈合 pass 是独立叶子增量、不该混进 C1 | **必须做**，作为 C1 的第三项交付 | 操作者指令：批 C 全部在本流程完成。且 C1-adversary 的 DC-5 证明它**不是顺带**——substrate 推导是 C1 覆盖崩溃窗口的必要条件 |
| **D5** | C3 只改 2 个渲染器 vs 6 处一起改 | **6 处一起改**（IV 除外，见 §4.1） | 只改 2 处等于修掉今天这一次分歧、留下产生分歧的机制 |
| **D6** | C2 是否要改 agent 侧 | **必须改** | 只抬 broker watchdog ⇒ broker 抱着 bucket+goroutine 多等几十分钟等一个 agent 已在 5 分钟放弃的传输——**比今天更糟** |

---

## 3. 永久非目标（本轮不做，且以后不做）

每条都是**带理由的永久决策**，不是延后。

| # | 非目标 | 理由 | 出处 |
|---|---|---|---|
| **N1** | **不**让 online force-single 自动做 nats.conf 脱簇 | `cluster_operation_controller.go:1194-1208` 整段 `WHAT IS DELIBERATELY *NOT* FIXED (and must not be)` 论证；drill 41 断言这个形状 | §6:538-541 |
| **N2** | **不**自动 JS reset | 自动 reset 一个 data-bearing 的 JetStream store = 静默丢 audit/history；运维 ack 是**特性**不是缺口 | §6:540-541 |
| **N3** | **不**做 `ev.transfer.<id>.progress` 进度续期 | 需要 agent 侧升级才生效；收益要等到真有慢链路 agent 才兑现。**本轮的短期半已经把「看门狗删掉在传对象」这个最坏后果消掉** | §6:553-558 |
| **N4** | **不**把 `agent_no_responders` 改名成 `no_progress` | 同 N3。但**归因错误本身在本轮修**——见 §6.5：新增 `transfer_budget_exceeded` 而非改名，因为 `agent_no_responders` 在 `expose.go:270`/`upgrade.go:93`/`cluster_upgrade_trigger.go:151` 三处是**真的** no-responders，不能一起改 | §6:553-558 |
| **N5** | **不**给 pull 路径加 size | `proto.PullPrepareReq` 无 size 字段、pull 的 `transferEntry` 不设 size。补它要动 proto + agent + ctl 三端，属 N3 的中期半范畴。**后果：pull 的 tier-B 预算仍是固定值**，由 §6.6 的回归测试钉住，防止误以为已修 | 代码事实 |
| **N6** | **不**改 `topoConvergedForOp`（`cluster_operation_controller.go:1076`）的 fail-closed 语义 | `:1072-1076` 注释明写 "Deliberately differs from computeHealth's inlined predicate, which excludes unreachable voters"；op ladder 靠它不 false-green SERVING/RETIRED | §7.3:596-604 |
| **N7** | **不**在半完成 drain 的形状上清 marker（`marker ∧ phase==VOTER ∧ 无 op`） | 它是运维**唯一**的可见证据（phase 还是 VOTER，`clusterdrain.go:85` 的 "Each step's failure leaves a status-visible stuck phase" 在这个窗口里靠的就是 marker 派生的告警）。清掉 = 把半完成 drain 变静默，且 `pickProxyRehomeTarget` 会**优先**挑中刚被搬空的那台 | 见 §5.1 |
| **N8** | **不**拆 `internal/cluster`、**不**合并双写路径、**不**做 `cmd/tether` 大搬迁、**不**删 outbox、**不**换 goleak、**不**把 `adminsock` union 改 typed payload、**不**合并 40 个具名 subject builder | §7 全节 | §3:110、§7 |
| **N9** | **不**合并 `clustermanifest`/`clusterupgrade`/`xferaudit`/`testharness` 四个叶子包，**不**合并 `internal/proc` 与 `internal/spawnsafe` | 判据是 import 面不是行数。本轮实测：`internal/adminsock` 与 `internal/proto` 的内部 import **恰为 0** ——这正是 §7.1 要保护的形状，所以 §4.2 只给它们加 `string` 字段、**不加任何 import** | §7.1、§7.2 |
| **N10** | **不**删任何高密度注释；重构必须**整段搬运**论证 | 在"wire 破坏 = 现网必须重装"的工具里，这些注释比测试耐久 | §7.4 |
| **N11** | **不**在 e2e 矩阵上去重；**不**做日志级别大调整；**不**改测试/文档总量 | §7.5 / §7.6 / §7.7 | §7 |
| **N12** | **不**动 `internal/proto.ProtoVersion` | 批 C 三项全部是 additive / 源码级；bump 它 = 现网 6 个 agent 必须**重装而非升级** | §6:534、:554、:566 |
| **N13** | **不**改 `computeHealth` 里 `FaultTolerance == 0` 早退于拓扑 banner 的优先级 | 该早退在 voters ≤ 2 时抢先于**所有**拓扑 banner（既有的 Stuck/Behind 也一样），所以这不是 C3 引入的问题而是既有的 banner 优先级选择。改它会动到与拓扑无关的健康裁决排序，超出 C3 范围。拓扑判决在 TOPO 列 / `--wait` / `doctor` / status card 四处仍然完整可见 | 见 §4.2 |
| **N14** | **不**抬高全局 `xferCrossHomeReapAge` / `serveconf.MinXferCrossHomeReapAge` | 后者是生产 YAML 的**硬拒下限**，抬高会让显式设过该 knob 的现网 broker 升级后拒绝启动，且让小盘 broker 的孤儿对象多留一小时。改用逐对象下限即可精确保持不变量 | 见 §6.3 |
| **N15** | **不**修 orphan reaper 在 broker 重启后误删在传对象 | `transfer_reconcile.go` 的 orphan reaper 宽限期是 `xferReapMinObjectAge = 2 min`，而 broker 重启后 in-memory tracker 为空，于是一个仍在传的 tier-B 对象活过 2 分钟就会被当 orphan 删除。修它需要"对象归属"的**持久**证据，而那正是被判缓的中期半（progress/ledger 面，N3）。⇒ 本轮**不修**，但加一条**断言其存在**的回归测试，防止 §6 的"最坏后果已消除"被读成全称命题 | C2 补跑 lane §0-b |

---

## 4. C3 — topo `Action` 上 wire

### 4.1 缺陷全貌（实证）

「拓扑是否收敛」在 HEAD 上有 **4 个判决实现 + 2 个应判未判的镜像**，谓词两两不等价：

| # | 位置 | 谓词 | 本轮处置 |
|---|---|---|---|
| **I** | `internal/broker/clusterstatus.go:599-608`（`computeHealth`） | 先 `observed<desired`，**再**在其内部按 3 个 reason 子串分 STUCK/BEHIND | **改分类器；STUCK 无条件** |
| **II** | `cmd/tether/cluster.go:433-446`（`topoCell`） | 先 reason 子串（**只有 2 个，缺 `render`**），后 `observed>=desired` | **改同一个分类器** |
| **III** | `cmd/tether/cluster_reconcile.go:171-189`（`topoLaggards`，喂 `--wait`） | `observed<desired \|\| reason!=""`，**完全不分 STUCK/BEHIND** | **改分类器**（误判方向决定 `--wait` 是假绿还是永挂） |
| **IV** | `internal/broker/cluster_operation_controller.go:1076-1098`（`topoConvergedForOp`） | 只比 generation，完全不看 reason，**故意 fail-closed** | **不动**（N6） |
| **V** | `cmd/tether/cluster_status_card.go:104-137`（`cardTopReason`） | 自称 "a pure CLI mirror of the computeHealth degraded=true triggers"，**却完全没有 topo 分支** | **加 topo 分支** |
| **VI** | `cmd/tether/cluster_doctor_online.go:20-90` | per-node 检查有 8 项，**没有 topology** | **加 topology check** |

另记录一处非 Go 的第七面：`test/simcluster/drills/41-shrink-to-standalone.sh` 的 `_topo_converged()` 是 jq 版镜像，同样只比 generation。**本轮不改它**（drill 是部署层的独立观察者，不应与产品共享判据），但在 §8 里记录。

**两条实证过的真缺陷**（一次性 probe test，已删除；`make test` 里会补成正式测试）：

```
health=HEALTHY_HA banner=""
  ← voter TopoObserved=7=desired，Reason="…unrecognized directive…"
```
> **(a) STUCK 判断被关在 `observed < desired` 的 if 里**（`clusterstatus.go:599`）。
> `ActionUnknownDirective` 分支（`natsconf/reconcile.go:117-118`）返回 `AppliedGen: lastApplied, ObservedGen: lastObserved`，
> 所以一个**已收敛后**被手工加进 `include` 指令而永久 fail-closed 的 reconciler，`observed == desired` 成立，
> 整段子串判断根本不执行 ⇒ **报 HEALTHY_HA**。而 `topoCell` 是子串在前，同一状态它渲染 STUCK ——
> **这是两端的第二处不一致，roadmap 只写了子串集合那一处。**

```
held: health=DEGRADED banner="a broker's NATS topology reconcile is STUCK … cannot be rendered/validated"
      next="fix that broker's nats.conf, or run `tether cluster reconcile nats --manual` on it"
  ← ActionAwaitingClusteredCutover
```
> **(b) `ActionAwaitingClusteredCutover` 的 Reason 含 "**render**ed"**（`natsconf/reconcile.go:187-188`），
> 命中 `computeHealth` 的 `strings.Contains(reason, "render")` ⇒ 被判 STUCK。
> 而它是首次 standalone→clustered grow 的**故意 WITHHOLD**，conf 完全正常。
> 更糟的是 next-step 里那句 `cluster reconcile nats --manual` ——
> `natsconf/reconcile.go:182-185` 明写它会造成 **G4 #10（clustered-alone JS meta）/ #4（孤儿 standalone store）**。
> ⇒ **产品今天在这个状态下主动把运维推向数据损坏。**

**(c) `ActionRejected` 的第三个来源两端全漏**：`reconcile.go:193` 的 `Reason: "apply: " + err`
（原子换装失败：磁盘满 / `.bak` 写不下 / rename EXDEV）既不含 `render`、也不含 `nats-server -t`、
也不含 `unrecognized directive` ⇒ 两端都渲染成"还在追赶"。

**(d) 覆盖现状**：`topoCell` **零测试**（全仓 grep 无命中）；
`internal/broker/topology_health_test.go:13` 的 `TestComputeHealthTopologyGate` 存在但
**从不设置 `TopoReconcileReason`** ⇒ STUCK/BEHIND 分类在两端都零覆盖。缺陷正好长在没测试的地方。

### 4.2 设计：一个共享分类器 + 加法式字段链

**分类器落点 `internal/natsconf/topostate.go`（新文件）**。实测 import 图（`go list`）：

```
internal/natsconf  -> internal/auth（唯一内部依赖）
internal/adminsock -> （零内部依赖）
internal/proto     -> （零内部依赖）
cmd/tether         -> 已 import internal/natsconf
internal/broker    -> 已 import internal/natsconf（topology_reconcile.go）
```
⇒ 放 `natsconf` **零新 import 边、结构上不可能成环**；`Action*` 常量的 SSOT 也在这里。
`adminsock` / `proto` 各自只加一个 `string` 字段、**不加任何 import**（N9）。

新增符号：

```go
type TopoState int
const (
    TopoUnreported TopoState = iota // 不报拓扑（老 broker / desired==0）
    TopoConverged
    TopoBehind                      // 会自愈，等即可
    TopoHeld                        // 故意扣住的 standalone→clustered 换装：不会自愈，但 conf 没毛病
    TopoStuck                       // reconcile 卡死，需要人改 conf
    TopoUnknownAction               // 对端报了本二进制不认识的 action（读者比写者旧）
)

func ClassifyTopo(action, reason string, observed, desired uint64, reported bool) TopoState
func (s TopoState) Cell() string      // TOPO 列 token
func (s TopoState) Banner() string
func (s TopoState) NextStep() string
func (s TopoState) Degrades() bool
func WorstTopoState(...TopoState) TopoState  // Stuck > Held > UnknownAction > Behind > Converged > Unreported
func AllActions() []string                   // 7 个常量，供穷尽门
```

**7 个 Action 的映射**（每条带依据）：

| Action | 状态 | 依据 |
|---|---|---|
| `ActionNoop` `:38` | 按世代：`observed>=desired`→Converged，否则 Behind | `:89`（desired==0，渲染器已因 desired==0 短路成 `-`）与 `:163`（真收敛） |
| `ActionReloaded` `:39` | 同上 | `:169`/`:206`，probe 已确认加载 |
| `ActionSwappedReloadPending` `:40` | **Behind** | 三个 producer 都有既定自愈路径（`topology_reconcile.go:120-147` 的 staggered hard-restart + 每 tick 重探）。**不改今天的行为** |
| `ActionRejected` `:41` | **Stuck（无条件，不看世代）** | 三个 producer：render 失败 `:151`、`nats-server -t` 失败 `:177`、**apply 失败 `:193`**。这一条同时修掉 (a) 和 (c) |
| `ActionUnresolvable` `:42` | **Behind** | `:95`/`:102`/`:110` 全是"peer 身份还没复制过来"，reason 自带 `(converging)`；`topology_reconcile.go:154` 的 sys.event 变更门也把它和 noop 一起豁免。**不改行为** |
| `ActionUnknownDirective` `:43` | **Stuck（无条件）** | `:117` fail-closed 的 conf 解析拒绝。无条件正是 (a) 的修法 |
| `ActionAwaitingClusteredCutover` `:49` | **Held** | 见下 |

**`Held` 为什么单列**（(b) 的修法）：

- **不能判 Stuck**：Stuck 的 next-step 会叫运维跑 `cluster reconcile nats --manual`，
  而那正是 `reconcile.go:182-185` 写明会造成 G4 #10 / #4 的动作。
- **不能判 Behind**（今天两端都是）：它**永远不会自愈**——自治 reconciler 是 SIGHUP-only，
  只有编排的 `cluster add` 才做协调重启。
- **降级位不变**：Held 时 Applied/Observed 原地不动 ⇒ `observed < desired` 已成立 ⇒
  `computeHealth` 今天已置 `degraded=true`。所以引入 Held **不改 HEALTHY_HA/DEGRADED 的极性**。
- **诚实限制（内审综合稿指出，已核实）**：Held 的 banner/next-step 在 `computeHealth` **侧不可达**。
  `computeHealth` 在 `proj.FaultTolerance == 0` 时**先于**拓扑 banner return，
  而 `ProjectQuorum` 在 voters ≤ 2 时 FaultTolerance 恒为 0；
  Held 只发生在首次 standalone→clustered grow（voters 从 1 变 2）。
  ⇒ **不得声称"引入 Held 改善了集群健康裁决"**。它的价值兑现在另外四处：
  **TOPO 列**（`topoCell`）、**`reconcile nats --wait`**（不再空等到超时）、
  **`cluster doctor`**（新增的 topology check）、**status card 头条**（`cardTopReason`）。
  注：这条限制对既有的 topoStuck/topoBehind banner **同样成立**（N≤2 时也被 FaultTolerance 抢先），
  是既有的 banner 优先级选择，**本轮不改**——改它会动到与拓扑无关的健康裁决优先级，超出 C3 范围。
  记为**永久决策 N13**。
- Held 的 next-step **必须带否定子句**：
  `run 'tether cluster add <that-broker>' to perform the coordinated restart — do NOT run 'cluster reconcile nats --manual' on it: that would apply a clustered conf under a running standalone nats-server (G4 #10/#4)`。

**混版 fallback**：`TopoAction == ""`（老 broker）走 `classifyLegacyReason(reason)`，
**与分类器同文件、只有一份实现、两端共用**。今天两端 fallback 不一致正是原始缺陷，
所以必须有一条测试钉住"两端对每个 Action 与每条 legacy reason 都给出同一分类"。

### 4.3 字段链（additive omitempty，不动 ProtoVersion）

1. `internal/broker/clusterwrite.go:131-135` `topoSelfReport` 加 `Action string`（同步改 `:130` 的注释）。
2. `internal/broker/topology_reconcile.go:149` 存入 `Action: out.Action`。
   **这是全仓唯一的 `topoSelf.Store` 点**（已 grep 确认）。
3. `internal/proto/alerts.go` `ClusterHealthResp` 加 `TopoAction string \`json:"topo_action,omitempty"\``；
   `ClusterHealthSchemaVersion` 6→7 并在 `:6-13` 的账本注释追加一句。
   该常量自陈 *"No consumer gates on this value — decoding is omitempty-additive — so it is a documentation ledger, not a compat switch"*（`:11-13`），
   实测唯一写者是 `cluster_health.go:82`，**无读者 gate**。`ProtoVersion` **不动**。
4. `internal/broker/cluster_health.go:98-104` 填充 `resp.TopoAction`。
5. `internal/broker/clusterstatus.go:352` 附近（peer 行，health-echo）与 `:376` 附近（self 行，权威覆盖）
   **两处都要**传播 `ns.TopoAction`。漏 `:376` ⇒ leader 自己那行永远走 fallback，
   而 STUCK 最常发生在自己身上。
6. `internal/adminsock/protocol.go:767-770` `ClusterNodeStatus` 加 `TopoAction string \`json:"topo_action,omitempty"\``。

### 4.4 六处渲染/判决的改法

- **I `computeHealth`**：用 `natsconf.ClassifyTopo(...)` 替换 3 个子串；
  **STUCK/Held 的判断移出 `observed < desired` 的 if**（修 (a)）；
  banner/next-step 取自 `TopoState`；跨节点折叠用 `WorstTopoState`。
- **II `topoCell`**：同一个分类器，`Cell()` 出 token（新增 `HOLD`、`?`）。
- **III `topoLaggards`**：同一个分类器；`--wait` 的退出条件改为「无节点处于 `Degrades()==true`」，
  且遇到 `TopoStuck`/`TopoHeld` 时**立即返回可诊断错误而不是继续轮询**（今天它会一直等到超时）。
- **IV `topoConvergedForOp`**：**不动**（N6）。在其注释里加一行指向 `natsconf.ClassifyTopo`，
  说明"此处故意不用共享分类器"，防止下一轮审查再发现一次。
- **V `cardTopReason`**：在 phase 分支之后、disk/ports 之前插入 topo 分支，
  返回 `"broker <id> topology " + state.String()`；这样 STUCK 时头条不再是
  今天那句错误的 `"fault tolerance reduced — see the table"`（`:136`）。
- **VI `cluster doctor`**：新增一项 `topology`，`TopoStuck` ⇒ FATAL，`TopoHeld` ⇒ ADVISORY 并给
  `cluster add` 指引，`TopoBehind` ⇒ ADVISORY。

### 4.5 C3 交付清单（原子项）

| # | 交付 |
|---|---|
| C3-1 | 新文件 `internal/natsconf/topostate.go`：`TopoState` + 6 个常量 + `ClassifyTopo` + `classifyLegacyReason` + `Cell/Banner/NextStep/Degrades` + `WorstTopoState` + `AllActions` |
| C3-2 | `topoSelfReport.Action`（`clusterwrite.go`）+ 写入点（`topology_reconcile.go:149`） |
| C3-3 | `proto.ClusterHealthResp.TopoAction` + `ClusterHealthSchemaVersion` 6→7 + 账本注释 |
| C3-4 | `cluster_health.go` 填充 |
| C3-5 | `clusterstatus.go` **两处**传播（peer 行 + self 行） |
| C3-6 | `adminsock.ClusterNodeStatus.TopoAction` |
| C3-7 | I `computeHealth` 改分类器 + STUCK 无条件化 |
| C3-8 | II `topoCell` 改分类器 |
| C3-9 | III `topoLaggards` 改分类器 + `--wait` 遇 Stuck/Held 立即返回 |
| C3-10 | V `cardTopReason` 加 topo 分支 |
| C3-11 | VI `cluster doctor` 加 topology check |
| C3-12 | IV `topoConvergedForOp` 加"故意不共享"的一行注释（**不改逻辑**） |
| C3-13 | drill `93-metrics-observability.sh` 断言 `cluster status --json` 带 `topo_action`，且 render 失败时 TOPO 列**极性**为 STUCK（不能只断言列存在） |
| C3-14 | `docs/distributed-broker-architecture.md` 记录新 wire 字段与"老 broker 不带此字段"的混版惯例 |

---

## 5. C1 — force-single 后半段收进 op 机器

### 5.1 伴生项：drain marker 的愈合与报告（roadmap §6:541-542）

**订正谓词**（R-5）。roadmap 的字面写法有两处会立刻炸：

- 「phase=DRAINING 且无 active op ⇒ 记 INCONSISTENT」：`DrainNode` **全程不创建 op 行**
  （`clusterdrain.go` 只有 4 个 Propose，没有一个是 `PlanClusterOpStart`），
  它的成功出口就是 `setPhase(nodeID, phaseDraining, …)` 然后 `return nil`。
  所以「phase=DRAINING ∧ 无 op 行」**正是 `tether cluster drain <node>` 成功后的正常稳态**，
  可以合法持续数天。而 `cluster_doctor_online.go` 把 `roster_consistency` 判 FATAL（exit 64）
  ⇒ 字面实现 = 每个正常 drain 完的节点让 `cluster doctor` 退 64。
  **正确谓词是「phase=DRAINING ∧ marker 缺失 ∧ 无非终态 op」**——
  三条产生路径（`DrainNode` 先 marker 后 phase、`driveRetire` 先 marker 后 phase、
  `AbortDrain` 先还原 phase 后清 marker）都保证 `DRAINING ∧ ¬marker` **不是任何合法瞬态**。
- 「记 INCONSISTENT」：`Inconsistent` 是**渲染期派生**（`clusterstatus.go:317-320` + `:396-404`），
  **没有持久化列**。所以这一半 pass **一个字节都不写**，只在同一处派生式上加第三个 disjunct。

**清 marker 的范围（D3 裁决后收缩）**：

| 形状 | 处置 | 理由 |
|---|---|---|
| `draining:<node>` 存在但 `cluster_nodes` **根本没有该 node 的行** | **清** | 节点都不存在了，没有任何 drain 可能在跑。产生源：`cluster_operation_controller.go:1028-1030` 清 marker 的 Propose **错误被 `_` 吃掉**，随后 op 进终态 `RETIRED`、roster 行已删 ⇒ 一条指向不存在节点的永久 `broker_draining` 告警 |
| `phase == DRAINING` ∧ marker 缺失 ∧ 无非终态 op | **报（渲染期 `Inconsistent`），不清** | 不是合法瞬态；没有任何机器会推进它，必须人工 `cluster drain --abort` |
| `marker ∧ phase == VOTER ∧ 无 op` | **不碰**（N7） | 这正是半完成 raw drain 的形状，marker 是运维唯一的可见证据 |

**pass 的形状**：注册进 `reconcile_passes.go`，`authorityLeader`（它经 raft 写），
interval 走**新配置字段**（不复用 `GrowLockReapInterval`——一个字段两语义是 B3 的病），
只调既有幂等命令路径 `cluster.PlanClusterDrainSet(node, nil)`
（`AbortDrain` / `driveRetire` 已在用，`DELETE … WHERE key=` 天然幂等），
并加 `reaperCaughtUp()` 闸（照 `reconcile_grow_lock.go` 的形状）。
`AbortOp` 的注释（`cluster_operation_controller.go:285-287` 的 "reconcile/doctor heals"）
改为**点名这个 pass 的名字**——让承诺变事实。

### 5.2 主体：形状裁决

**决定性证据链**（评审提出、主进程已逐条核实）：

1. ghost roster 行不在 raft config ⇒ `clusterstatus.go:315` 的 `role[r.nodeID]` 取不到 ⇒ `Role == ""`。
2. `cmd/tether/cluster_natsconf.go:180-189`：`--to-standalone` 逐节点 `switch nd.Role`，
   `default: return fmt.Errorf("… unrecognized raft role %q — cannot prove N=1, refusing")`。
3. `docs/cluster-runbook.md:461` 原文：
   > *"The abandoned peers are already pruned (below), so the N=1 voter tally passes and `--to-standalone` is unlocked."*
4. `--to-standalone` 是 force-single 之后恢复数据面的**唯一**途径。

⇒ **ghost 残留 = JetStream 永久 503**，正是项目记忆里 racknerd「JS 503-rotted for 5 days」那起事故本身。
⇒ 同时也说明：**把 prune 改成"commit 返回后异步做"会打断 runbook 的下一步**，
运维照做会撞上 `unrecognized raft role ""` ——灾难现场毫无指向性的报错。

**最终形状**：

| 步骤 | 归属 | 依据 |
|---|---|---|
| `RecoverToSelfOnline` | 同步（不变） | 不可逆，本来就是成功边界 |
| `WaitForLeader` + marker + epoch | **同步、LOUD（不变）** | D1 |
| `prune` | **同步先试**（不变）；**失败才建 op** | 上面的证据链 |
| `seeds` | **同步先试**（不变）；不进梯子 | `driveLeaderMaintenance:371` 每 leader tick + `clusteradmin.go:402` 每 leader edge 都重跑，**已自愈** |

**成功路径与今天逐字相同，不建 op 行、CLI 文案不变** ⇒ 零新失败模式。
op 只出现在**今天是永久死路**的那条分支上。

**崩溃窗口**（崩在 raft rewrite 之后、任何后续步骤之前）：
`ReconcileMembershipOnLeadership`（`clusteradmin.go:389-396`）那条**只报不修**的分支升级为
「四条全中才建 finalize op」：

① `cluster_meta[force_single_active]` 已置 ② `NumVoters() == 1`
③ 该 roster 行 `phase == VOTER` 且不在 `RaftConfiguration()` 中 ④ 无以 self 为 target 的非终态 finalize op

缺任何一条**保持今天的 log-only**。③ 用 `VOTER` 而非任意 live phase 是关键：
正在 join 的行是 `PENDING`/`CATCHING_UP`，**短暂满足"不在 config 中"**，
用 `VOTER` 收紧后结构上不可能误清。

> 这一条是 C1 覆盖崩溃窗口的**必要条件**——只做"commit 里插 op 行"的方案覆盖不到它，
> 反而多了一层「看起来有机制其实没覆盖」的假象。

### 5.3 op 梯子

新 kind `OpKindForceSingleFinalize = "force_single_finalize"`
（`operation_ops.go`，`ValidOpKind` 改三值；`0015_cluster_operations.sql` 的 `kind TEXT` 无 CHECK ⇒ **零 migration**）。

| state | 含义 | terminal |
|---|---|---|
| `FS_PRUNE_PENDING` | 初态：raft 已 {self}、marker+epoch 已落盘、prune 尚未观测到完成 | 否 |
| `FS_FINALIZED` | 终态·成功：`params.abandoned` 的 roster 行已**观测到**全部消失，seeds 已收敛 | **是** |
| `FS_GHOST_LEFT` | 终态·带残留：预算内 prune 未成功 | **是** |

- `TargetNode = selfID`（**不能是 ghost**：否则 `cluster recovery node remove <ghost>` 的
  `assertNoActiveOp(nodeID)` 会被自己刚建的 op 拒绝 = 为了修 ghost 而废掉清 ghost 的工具）。
- `Params` 烤入 `{abandoned, epoch, markedAt}`，**driver 不得重新读 roster 推导 abandoned**
  （第二次 tick 时 roster 已被部分 prune，abandoned 集合会缩小，剩下的 ghost 永远轮不到）。
- **advance-after-observe**：prune 步的推进条件是
  `SELECT COUNT(*) FROM cluster_nodes WHERE node_id IN (params.abandoned)` == 0，不是 "Propose 返回 nil"。
- **prune 提交前重新确认** ghost 仍不在 `RaftConfiguration()` 中（不信 params 快照），
  且**只删 `params.abandoned` 里列出的 id**，绝不实现成"删所有不在 raft config 里的 roster 行"。
- **永不 BLOCKED**：预算耗尽进终态 `FS_GHOST_LEFT`，`last_error` 必须写明
  「`--to-standalone` 会以 `unrecognized raft role ""` 拒绝；先逐个 `tether cluster recovery node remove <id>`」。
- **预算用复制式 deadline**（D2）：在 op 创建时 bake 进 `catchup_deadline` 列，
  照 `jsPlacementAdvance:1148` 的 `op.CatchupDeadline == 0 ⇒ 视为已过期` 语义。
- `driveOne` 的 `switch op.Kind` **必须加 case**；同时加 `default:` 分支把未知 kind 转终态，
  让未来的新 kind 不再复现"旧二进制永久非终态"的陷阱。
- `driveInFlightOperations` 开头的 `upgradeActive()` 冻结：**finalize 豁免**
  （force-single 常由 rolling upgrade 滚死 quorum 触发，那意味着 upgrade lock 大概率持有且无法续期/删除；
  冻结 ⇒ op 永远停在初态 = 比今天更糟）。**同 commit 必须配一条反向测试**锁住
  「upgrade lock 持有时 join/retire 仍然被冻结」，否则这个豁免会顺手把 B2 的保护也开了。
- `ConfirmOp`（`:242-251`）必须在 `clearOpAttempts(opID)` **之前**对 finalize 早退——
  今天 `clearOpAttempts` 在 kind 判断之前无条件执行，反复 `cluster ops confirm` 能把预算清零。
- `AbortOp` 改成不依赖 `FromState` 枚举（照 `PlanClusterOpConfirm:203-210` 只 guard `terminal = 0` 的形状）。
  **理由**：`PlanClusterOpTransition:166` 今天 **连 FromState 一起校验**，
  所以回滚到旧二进制后 `cluster ops abort` 会因"未知 FromState"直接报错——逃生口不存在。

### 5.4 单活闸的反向

`PlanClusterOpStart` 的 `WHERE NOT EXISTS (… target_node = <target> AND terminal = 0)`：
若 self 身上已挂着一条半途的 retire op（N=3 上 `cluster retire <self>` 走到一半另两台挂了），
建 op 会 no-op。因为 D1 已把 op 降级成**失败分支才用**，这里退回今天的行为
（Warn + `cluster recovery node remove` 是确定性终结器）即可。
仍需一条测试钉住「self 上预置一条非终态 retire op，force-single commit 仍然成功」。

### 5.5 运维文案（契约扫描面）

`cmd/tether/cluster_offline.go:389-396` 今天打印：
> *"…then a FULL `systemctl restart nats-server` (**the abandoned peers are already pruned**, so the N=1 proof now passes)"*

这不是过时描述，是**因果指引**。必须按分支分文案：

- prune 成功（绝大多数）⇒ 今天的文案不变；
- prune 失败/建了 op ⇒ **不说 already pruned**，改为点名 `op_id` +
  「先 `tether cluster ops show <op_id>` 到终态，再做 de-cluster」。

同时 `internal/adminsock/protocol.go` 的 `Abandoned` 字段注释
（"node_ids removed from the new {self} config"）从事实变成意图，要改。

### 5.6 C1 交付清单

| # | 交付 |
|---|---|
| C1-1 | `OpKindForceSingleFinalize` + 3 个 state 常量 + `validOpStates` + `ValidOpKind` 三值 |
| C1-2 | `driveOne` 加 case **和** `default:` 未知 kind 转终态 |
| C1-3 | `driveForceSingleFinalize`：advance-after-observe、prune 前重确认 raft config、只删 params 列出的 id |
| C1-4 | 复制式 deadline 预算（`catchup_deadline`），耗尽 ⇒ `FS_GHOST_LEFT`，**永不 BLOCKED** |
| C1-5 | `handleForceSingleCommit`：prune/seeds 同步先试，**失败才建 op**；成功路径逐字不变 |
| C1-6 | `ReconcileMembershipOnLeadership` 的 log-only 分支升级为"四条全中建 op"（崩溃窗口） |
| C1-7 | `driveInFlightOperations` 的 `upgradeActive` 冻结对 finalize 豁免 + 反向测试 |
| C1-8 | `ConfirmOp` 在 `clearOpAttempts` 之前对 finalize 早退 |
| C1-9 | `AbortOp` 不再依赖 `FromState` 枚举 |
| C1-10 | `clusterops.go` `opEntryFromOperation` 为新 state 渲染 `State`/`Resume` |
| C1-11 | CLI 分支文案（§5.5）+ `adminsock` `Abandoned` 注释订正 |
| C1-12 | 愈合 pass（§5.1）：清可证明的孤儿 + 注册 + `AbortOp` 注释点名 |
| C1-13 | 渲染期 `Inconsistent` 加第三个 disjunct（`DRAINING ∧ ¬marker ∧ 无非终态 op`） |
| C1-14 | `docs/distributed-broker-architecture.md` 增补新 op kind + ladder 状态表 |
| C1-15 | `docs/cluster-runbook.md` 的 online force-single 段落改为「commit 返回后按分支处理」 |
| C1-16 | drills：`22-forcesingle-online.sh` 断言成功路径**不建 op**；`12-ghost-voter.sh` 加 prune 失败注入分支；`20`/`41` 回归 |

---

## 6. C2 — 传输预算（只做短期半）

### 6.1 `transferTimeoutTierB` 的全部消费者（R-3）

| # | 消费者 | 位置 | 不同改的后果 |
|---|---|---|---|
| 1 | 看门狗 | `transfer.go:402-406` | —（这是要改的那个） |
| 2 | `xferCrossHomeReapAge = 3 * transferTimeoutTierB` | `transfer.go:62` | leader 跨 home GC 在旧期限删掉**仍在传**的对象；`reconcile_passes_test.go:1119` 仍然绿（它钉的是旧关系） |
| 3 | `transferTimeoutFor(tier)` | `xfer_inflight.go:138-143` | stranded 判定与 synthetic terminal 的时基；崩溃恢复按 5m 判一个活着的大传输为 stranded |
| 4 | agent 侧 `commitCtx`（push 下载腿） | `internal/agent/transfer.go:186` | agent 在 5 分钟放弃，broker 却多等几十分钟 |
| 5 | agent 侧 `putCtx`（pull 上传腿） | `internal/agent/transfer.go:408` | 同上 |
| 6 | ctl `--timeout` 默认 10m | `cmd/tether/transfer.go:72`、`:386` | 客户端与 broker 期限不一致（今天已然） |

**只改 #1 是净倒退**（D6）。

### 6.1b C2 补跑 lane + 其评审带来的裁决（主进程逐条定）

| # | 事实 / 冲突 | 裁决 |
|---|---|---|
| **D-C2-1** | `xferMinThroughput` 取值 | **2 MiB/s**（≈16.8 Mbit/s，单腿口径）。推导：今天的 (5m, 2 GiB, 2 腿) 隐含断言 **13.65 MiB/s ≈ 114 Mbit/s 端到端**（还要穿 JetStream 分块 + fsync），连 roadmap §6:547 自己举的 100 Mbit/s 裸链路都够不到。取其 ~1/7 作为保守下限；且 2 GiB / 2 MiB/s = **恰好 1024 s**（2 的幂，手算无取整误差）。⇒ `XferTierBMaxBudget = 2 × 1024 s = 2048 s = 34m08s`。**否决 1 MiB/s**（上限 68 min，代价见 D-C2-2） |
| **D-C2-2** | 我原先写的"小盘 broker 的最坏 **bucket 占用**"**是错的** | `deleteXferObject` 只删**对象**、从不删 bucket（`transfer.go:376-378` 逐字 "The bucket itself survives — it's per-session, not per-transfer"）。bucket 的 `MaxBytes` 预留与看门狗预算**无关**。真实代价是三项：① 对象字节在盘上多待 6.8×（racknerd 的 per-session 天花板下，**3 个并发 2 GiB 在飞对象就会让第 4 个 `Put` 撞 10047**）；② tracker 槽位/goroutine/timer 的**耗尽窗口**从 5 min 变 34 min（内存上限不变）；③ 悬空 start 审计行与 ledger 文件多活 29 min。plan §6.2 的措辞按此订正 |
| **D-C2-3** | **第五个消费者**：`internal/agent/transfer.go:54` `pushCommitCacheTTL = 6 * time.Minute` | 今天 6m > 5m（tier-B 看门狗）——**那 1 分钟余量就是 "6" 的全部含义，从未被写下来**。broker 预算抬到 34m 而不动它 ⇒ ctl 传 2 GiB 花 17 min，push-commit 到达时缓存已被扫掉 ⇒ `transfer_unknown`。**必须一起改** |
| **D-C2-4** | 但**单纯抬高 TTL 会让缺陷更容易发生**（评审 BLOCKER 3） | `pushCommitCache` 有**两条**淘汰路径：TTL 惰性清扫 **与容量满时删 `added` 最早的那条**（`agent/transfer.go:502-522`）。TTL 抬 3.2× ⇒ 条目滞留更久 ⇒ 1024 上限更早触及 ⇒ 容量淘汰**恰好先杀掉那个等了十几分钟的大 push**。⇒ 容量淘汰必须改成"只淘汰**已过期**条目；无过期条目则拒新 prepare（可重试 code）"。**且测试必须是行为测试**（填满 1024 条新条目、断言 19 分钟前的大条目仍在），常量不等式对这条路径完全盲 |
| **D-C2-5** | 评审 BLOCKER 1/2：草稿提议的关系式测试是**恒等式** | `XferTierBMaxBudget = L × legBudget` 时，`maxBytes×L/(L×legBudget)` 里的 `L` 被约掉；`floor + 2×floor == 3×floor` 在任何世界都真。⇒ **承重断言一律用手算字面量**（`2048*time.Second`、`1024*time.Second`、`10*time.Minute`），只保留一条真正承重的不等式 `crossHomeFloor(maxBytes) > XferTierBMaxBudget`。这条评审同时证明**草稿没有真跑过自己的变异清单**——本轮所有新守卫必须实跑变异 |
| **D-C2-6** | 迟到的 `ev.transfer.complete` **不是**在 `!claimed` 处被丢的 | `finalizeTransfer` 已 `b.transfers.remove(...)`，所以迟到事件命中的是 `transfer.go:896-899` 的 `preview == nil` 裸 return（`L09-transfer-subsystem.md:206` 已独立记载）。`!claimed` 分支只在亚微秒窗口可达 ⇒ **Warn 必须加在 `preview == nil` 分支**，且要能区分"从没见过"与"看门狗刚终结过"（短 TTL 的近期终结集） |
| **D-C2-7** | 遗漏消费者 | 补三处：`internal/broker/cluster_observe_budget_test.go:31` 的注释写着即将作废的关系式；`internal/broker/broker.go:227-230` 的 `XferCrossHomeReapAge` 注入点注释；`xfer_inflight.go:209-211` 是 `xferInflightRecord` 的**第二个**写入点（`stageXferInflightTerminal` 合成自足记录） |
| **D-C2-8** | 评审 BLOCKER 6（serveconf 的三段运维文案）**在本 plan 的设计下不成立** | 那条 finding 假设我们**抬高** `MinXferCrossHomeReapAge`。§6.3 已裁决**不抬**（改逐对象下限），所以 `serveconf.go:221-224/:248-250/:253-254` 那三段 "3x the tier-B transfer timeout" **保持为真**、无需改动。这是选择逐对象方案的又一条收益 |
| **D-C2-9** | 草稿把 `transfer_budget_exceeded` 改名成 `xfer_deadline_exceeded`、并绕过 `codes.go` | **驳回改名**，沿用本 plan 已定的 `transfer_budget_exceeded`；**驳回绕过**，必须进 `internal/proto/codes.go`（`codes_registry_test.go` 的 `TestEveryDeclaredCodeHasAProductionEmitter` 有配套要求） |
| **D-C2-10** | 草稿要求 synthetic terminal 的 `Ts` **保持** tier 下限（推翻本 plan §6.4） | **采纳其技术论证并改判**：`Ts` 是 dedup reqID 的载体；若 `Ts` 随新 budget 变化，回滚后新旧二进制对同一 transfer 算出**不同** `Ts` ⇒ 同一终态可能写两条。⇒ **`Ts`/`DurationMs` 继续用 tier 下限，逐字不变**；只有 stranded **判定阈值**改用新 budget。原 §6.4 该条作废，以此为准 |
| **D-C2-11** | 残留（**永久决策 N15**） | orphan reaper（`transfer_reconcile.go`，宽限 `xferReapMinObjectAge = 2 min`）在 **broker 重启后 tracker 为空**时，仍会把一个正在传的 2 GiB 对象当 orphan 删掉。修它需要"对象归属"的持久证据 = 被判缓的中期半。**本轮不修**，写成 N15 并加一条断言其存在的回归测试，防止"最坏后果已消除"被当成全称命题 |

### 6.2 预算函数

```go
// xferMinThroughput is the SLOWEST link the tier-B budget promises to cover.
const xferMinThroughput = 1 * 1024 * 1024 // bytes/sec == 8 Mbit/s
// xferBudgetLegs: a tier-B transfer crosses the object store TWICE (sender Put, receiver Get).
const xferBudgetLegs = 2
func transferBudget(tier string, size int64) time.Duration
func transferLegBudget(size int64) time.Duration   // agent 侧单腿
const transferBudgetMax = /* transferBudget("b", transferMaxBytes) */
```

- tier a 保持 `transferTimeoutTierA` 不变（8 MiB 上限，30s 足够）。
- tier b：`max(transferTimeoutTierB, xferBudgetLegs * size / xferMinThroughput)`。
- agent 单腿：`max(transferTimeoutTierB, size / xferMinThroughput)`。
- **上界确定**：admission gate（`transfer.go:586`）已拒 `size > transferMaxBytes`，
  所以 `transferBudgetMax` 是有限常量。**必须显式命名它**——无上界的 budget 等于取消看门狗。
- size 取 `req.Size`（客户端声明）。声明值受 admission gate 夹住，
  所以一个过度声明的客户端最多把预算撑到 `transferBudgetMax`；这一点要写进注释并有测试。

`xferMinThroughput` 的取值理由与代价，以及 `xferCrossHomeReapAge` 的新关系式，
由 C2 补跑 lane 的产出 + 内审共同定稿（见 §9 的开放项处置规则）——
**但取值必须有推导、必须在小盘 broker（现网 racknerd）上把最坏情况的 bucket 占用说清楚**。

### 6.3 关系式重推 —— **改成按对象大小的逐对象下限，而不是抬高全局常量**

`xferCrossHomeReapAge` 的语义是"一个还活在别的 home 上的传输一定会在一个 tier-B 期限内
被自己的看门狗终结"。预算变成 size 的函数之后，这条不变量也必须变成 size 的函数。

**先看一条会把整件事引爆的事实**（内审综合稿指出、我已核实）：

```go
// internal/serveconf/serveconf.go:221-225
// MinXferCrossHomeReapAge is the SAFE FLOOR for the cross-home GC age (external review F2): 3x the
// tider-B transfer timeout, i.e. the same value broker.New derives when the knob is unset.
const MinXferCrossHomeReapAge = 15 * time.Minute
// :246-250
if d < MinXferCrossHomeReapAge { return 0, fmt.Errorf(... "only RAISE the floor" ...) }
```
`internal/broker/every_started_attempt_test.go:410` 又把 `serveconf.MinXferCrossHomeReapAge`
与 `xferCrossHomeReapAge` 钉成**相等**。

⇒ 若把全局 `xferCrossHomeReapAge` 抬到 `transferBudgetMax`（1 MiB/s + 2 腿 ⇒ ~68 分钟），
则 `MinXferCrossHomeReapAge` 也得跟着抬，而它是**生产 YAML 的硬拒下限**：
任何显式设了 `broker.cluster.xfer_cross_home_reap_age` 且落在 [旧下限, 新下限) 的现网 broker
**升级后会拒绝启动**。同时小盘 broker（现网 racknerd）的孤儿对象要多留一个多小时。
**这不是"零部署面"的改动**——roadmap 说 C2 零 wire 是对的，但没看到这条。

**裁决：不抬全局常量，改成逐对象。**

- `xferCrossHomeReapAge` **保持 `3 * transferTimeoutTierB`（15m）不变**，
  `serveconf.MinXferCrossHomeReapAge` 也**不动** ⇒ **零部署面变更、现网 YAML 不受影响**。
- 跨 home GC 的年龄判据改成**逐对象**：
  `floor(obj) = max(xferCrossHomeReapAge, transferBudget("b", obj.Size) + xferReapBudgetMargin)`。
  对象大小从 ObjectStore 的 `ObjectInfo.Size` 直接拿（GC 本来就在遍历它们）。
- 于是不变量「GC 绝不删掉一个仍被活看门狗覆盖的对象」**逐对象精确成立**，
  而不是靠一个必须覆盖最坏情况的全局常量——后者既过度惩罚小对象，又碰部署面。
- 若 GC 拿不到某对象的 size（列举失败/旧路径），**回落到今天的 15m**（fail-safe 方向：
  只会更保守地保留，不会更早删）。

`reconcile_passes_test.go:1119` 的 `TestXferCrossHomeReapAgeDerivation` **保持绿且不改**
（它钉的关系仍然成立）；新增一条 `TestCrossHomeFloorCoversPerObjectBudget` 钉住逐对象关系。

### 6.4 崩溃恢复路径

- `transferTimeoutFor(tier)` 改签名接受 size，内部转调 `transferBudget`。
- `xferInflightRecord` 加 `Size int64 \`json:"size,omitempty"\``，`writeXferInflight` **两条分支都要填**。
- **旧 ledger 兼容**：v0.4.7 写下的记录没有 size ⇒ `Size == 0` ⇒ **回落固定 `transferTimeoutTierB`**。
  必须显式实现（`max(...)` 包裹，否则 `0/throughput = 0` 会让老记录**立即**被判 stranded）+ 显式测试。
- synthetic terminal 的 `Ts: rec.StartedAt.Add(timeout)` 与 `DurationMs` 改用新 budget，
  **并保持确定性**——`xfer_inflight.go` 那段"deterministic `Ts` 是 dedup reqID 的载体"的论证必须整段搬运（N10）。

### 6.5 归因错误

看门狗超时写 `agent_no_responders` 是**错误归因**（agent 可能一直在传，只是预算不够）。
`agent_no_responders` 在 `expose.go:270`、`upgrade.go:93`、`cluster_upgrade_trigger.go:151` 三处是**真的**
no-responders，不能一起改（N4）。
⇒ **本轮新增一个 code `transfer_budget_exceeded`**，只在看门狗超时的 push 路径使用，
并接进 A1 的错误码 registry（`internal/proto/codes.go` + `cmd/tether/error_hints.go` 的分类表），
让 `cmd/tether/error_code_coverage_test.go` 保持绿。
`cmd/tether/node.go:205`、`:303` 的 `agent_no_responders` 保留集合要同步评估。

### 6.6 三端 tier 常量提到 `internal/proto`

`XferTierAMaxBytes`（8 MiB）与 `XferMaxBytes`（2 GiB）。六个引用点全部改：
`internal/broker/transfer.go:52`、`:67`；`internal/agent/transfer.go:47`、`:51`；
`cmd/tether/transfer.go:683`、`:690`。
守卫测试用 **AST 扫描**（仿 `error_code_coverage_test.go` 的 scanner 形状），
断言仓内不再出现第二处**tier 语义**的 `8 * 1024 * 1024` / `2 * 1024 * 1024 * 1024` 字面量，
且**必须放行**语义不同的 `xferEventsHistoryReserve` / `xferBucketCap`（`transfer.go:74`、`:76`）。

**手抄文案全局扫**：`internal/broker/transfer.go:587` 的 `"(2 GiB)"`、`:592` 的 `"(8 MiB)"`、
`internal/agent/transfer.go`、`cmd/tether/transfer.go` 的长帮助、`internal/schema/audit.go`、
`internal/proto/messages.go`、`docs/usage.md:948`（`--timeout` 表格行）、
`docs/usage.md:974`（逐字写着「tier A 30s / tier B 5min」）。
每处要么改成从常量派生，要么在本 plan 里列为"纯散文、不派生"并说明。

**N5 的回归测试**：断言 pull 的 `transferEntry.size == 0`，防止读者误以为 pull 也被修了。

### 6.7 C2 交付清单

| # | 交付 |
|---|---|
| C2-1 | `xferMinThroughput` / `xferBudgetLegs` / `transferBudget` / `transferLegBudget` / `transferBudgetMax`（含推导注释） |
| C2-2 | `startTransferWatchdog` 改用 `transferBudget` |
| C2-3 | `xferCrossHomeReapAge` 改从 `transferBudgetMax` 推导（加性余量） |
| C2-4 | `reconcile_passes_test.go` + `every_started_attempt_test.go` + `serveconf.MinXferCrossHomeReapAge` 同步钉新关系 |
| C2-5 | `transferTimeoutFor` 接受 size；`xferInflightRecord.Size`；`writeXferInflight` 两分支都填 |
| C2-6 | 旧 ledger `Size==0` 回落固定值（`max` 包裹）+ 测试 |
| C2-7 | synthetic terminal 的 `Ts`/`DurationMs` 改用新 budget，保持确定性，论证整段搬运 |
| C2-8 | agent 侧 `commitCtx` / `putCtx` 改用 `transferLegBudget`（老 agent 落回 5m = 今天行为，安全方向；零 wire） |
| C2-9 | 新 code `transfer_budget_exceeded` + 接进 A1 registry + `error_hints.go` 分类 |
| C2-10 | `internal/proto` 的 `XferTierAMaxBytes` / `XferMaxBytes` + 六个引用点 |
| C2-11 | AST 守卫测试（单一来源，且不误杀不同语义的同字面量） |
| C2-12 | 手抄文案全局扫（含 `docs/usage.md:948`、`:974`） |
| C2-13 | N5 的 pull-size 回归测试 |

---

## 7. 实施顺序

`C3` → `C2 常量提升` → `C2 budget` → `C1 愈合/报告 pass` → `C1 finalize op`

理由（真实耦合，不是口味）：

1. **风险与回滚代价单调递增**。C3 是纯 additive 字段 + 渲染；C2 常量提升是编译期机械改动；
   C2 budget 改运行期时序；C1 碰灾难恢复路径。
2. **C1 排最后**是 roadmap §6:526 的原话，且它是唯一有**回滚不对称**的一项
   （旧二进制既驱动不了新 state 也 abort 不了它——`PlanClusterOpTransition:166` 连 FromState 一起校验）。
3. C2 常量提升先于 C2 budget：前者动的是 `transferMaxBytes` 的**引用点**，
   后者的 `transferBudgetMax` 从它推导，反序会产生一次无谓的返工。
4. C1 的两项之间没有代码耦合，愈合 pass 先做是因为它风险更低、且它交付的
   `DrainingMarkers` 读侧与渲染期 `Inconsistent` 谓词是 finalize op 的测试脚手架能复用的。

---

## 8. 闸门与 drill 矩阵

**每次编辑中途不跑全量闸**（它编译的是一棵不存在的树）。全部编辑完成后：

```
PATH=/usr/local/go/bin:$PATH make test
PATH=/usr/local/go/bin:$PATH make lint          # golangci-lint v2.5.0，必须 0 issue
PATH=/usr/local/go/bin:$PATH make e2e-parallel  # 唯一的全矩阵闸；并行全绿即通过，严禁再串行复核
PATH=/usr/local/go/bin:$PATH go vet -tags phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix ./...
PATH=/usr/local/go/bin:$PATH go test -race ./internal/broker/ ./internal/agent/ ./internal/cluster/ ./internal/natsconf/ ./test/determinism/
```

并发改动（愈合 pass）另过 `-race` + 仓库内建 NumGoroutine/fd 泄漏门
（`test/concurrency/helpers_test.go`，**刻意不用 goleak**）。

**simcluster drill（按需，跑在 weilandserver，本机即是）**：

```
cd test/simcluster && PATH=/usr/local/go/bin:$PATH ./local.sh --build build
cd test/simcluster && ./local.sh drill 93-metrics-observability     # C3
cd test/simcluster && ./local.sh drill 22-forcesingle-online        # C1
cd test/simcluster && ./local.sh drill 12-ghost-voter               # C1
cd test/simcluster && ./local.sh drill 20-forcesingle-natsconf      # C1
cd test/simcluster && ./local.sh drill 41-shrink-to-standalone      # C1 回归
```

C2 不碰部署栈 ⇒ **不跑 drill**（跑全套 `run-drills.sh` 违反"按需运行、非必要绝不运行"）。

**drill 铁律**：忠实复现真实部署环境、如实暴露缺陷，**绝不替 tether 弥补**。
靠复杂脚本才"成功"的操作是缺陷不是成就，标 `[GAP #N]`。

---

## 9. 测试与变异验证

**纪律**：每条新增守卫都要**注入它声称能抓的那个缺陷并确认变红**。
批 B 曾在此真实翻车（产出恒等式测试）。

| 测试 | 注入的变异 |
|---|---|
| `TestTopoClassCoversEveryAction` | 在 `natsconf` 加第 8 个 `Action*` 常量而不加分类 ⇒ 必红 |
| `TestTopoRenderersAgreeOnEveryAction` | 只改 broker 侧、CLI 侧保留旧 substring ⇒ 必红 |
| `TestTopoStuckIsNotGatedOnGeneration` | 把 STUCK 判断搬回 `observed < desired` 的 if 内 ⇒ 必红（缺陷 (a)） |
| `TestAwaitingCutoverIsHeldNotStuck` | 让 cutover 落回通用 STUCK 文案 ⇒ 必红（缺陷 (b)） |
| `TestApplyFailureIsStuck` | 把 `ActionRejected` 的 apply 来源分出去当 Behind ⇒ 必红（缺陷 (c)） |
| `TestStatusCardNamesTopologyStuck` | 删掉 `cardTopReason` 的 topo 分支 ⇒ 必红 |
| `TestTransferBudgetRelations` | 把 budget 改回常量 `transferTimeoutTierB` ⇒ 必红。**必须钉关系不钉数值** |
| `TestCrossHomeReapExceedsMaxBudget` | 把 `xferCrossHomeReapAge` 改回 `3 * transferTimeoutTierB` ⇒ 必红 |
| `TestStrandedXferUsesSameBudgetAsWatchdog` | 让 `transferTimeoutFor` 忽略 size ⇒ 必红 |
| `TestLegacyLedgerWithoutSizeFallsBackToFixedBudget` | 去掉 `max()` 包裹 ⇒ Size=0 得 budget=0 ⇒ 必红 |
| `TestTierConstantsHaveSingleSource` | 在 agent 侧塞回一个 `2 * 1024 * 1024 * 1024` 字面量 ⇒ 必红 |
| `TestPullEntryCarriesNoSize` | 给 pull entry 填上 size ⇒ 必红（钉住 N5 是**已知**裂口而非疏漏） |
| `TestForceSingleCommitSuccessPathCreatesNoOp` | 让 commit 无条件建 op ⇒ 必红 |
| `TestForceSingleFinalizeRetriesFailedPrune` | 把重试改回 best-effort log ⇒ 必红 |
| `TestForceSingleFinalizeDeadlineGoesTerminalNotBlocked` | 把终态失败改成 `OpStateBlocked` ⇒ 必红 |
| `TestForceSingleFinalizeAdvancesOnObservationNotPropose` | 让推进条件改成"Propose 返回 nil" ⇒ 必红（D2 的第二类失败） |
| `TestForceSingleFinalizeSurvivesLeadershipChange` | 用进程内 attempt counter 代替复制式 deadline ⇒ 必红 |
| `TestLeadershipEdgeCreatesFinalizeOpForGhost` | 去掉四条判据中任意一条 ⇒ 必红（尤其去掉 `phase==VOTER` ⇒ 正在 join 的节点被误当 ghost） |
| `TestUpgradeLockStillFreezesJoinAndRetire` | 把 finalize 豁免写成对所有 kind 生效 ⇒ 必红 |
| `TestConfirmOpDoesNotResetFinalizeBudget` | 把早退移到 `clearOpAttempts` 之后 ⇒ 必红。**断言副作用，不是返回值** |
| `TestForceSingleCommitSucceedsWithStaleRetireOpOnSelf` | 让 commit 在建不出 op 时返回失败 ⇒ 必红 |
| `TestDrainMarkerHealerClearsOnlyRosterlessOrphan` | 放宽到 `marker ∧ phase==VOTER` ⇒ 必红（N7） |
| `TestDrainMarkerHealerIdleZeroWrites` | 让 pass 无条件 propose ⇒ 必红 |
| `TestDrainingWithoutMarkerIsInconsistent` | 去掉"无非终态 op"这一合取项 ⇒ 必红 |

**测试文件命名**：一律按**被测单元**命名（`topostate_test.go`、`transfer_budget_test.go`、
`force_single_finalize_test.go`、`drain_marker_healer_test.go`）。
注意 `test/determinism/test_naming_test.go` 的 `processNamedPattern` 会拦下 `c1_*` / `c2_*` / `c3_*`；
**绝不往 `legacy_process_named_list.go` 加新行**（那是递减账本，不是允许列表）。
审查 finding 写成测试函数上方一行 `// origin: batch-c internal review F<N>`。

---

## 10. 回滚代价

| 单元 | 回滚方式 | 代价 |
|---|---|---|
| C3 | 纯 revert | **极低**。additive omitempty：老 broker 不发 ⇒ 新 ctl 走 legacy fallback；新 broker 发 ⇒ 老 ctl 忽略未知 JSON 字段。前提是 fallback 真的存在且两端一致 |
| C2 常量提升 | 纯机械 revert | **极低**（编译期，无运行期语义） |
| C2 budget | 纯 revert | **低**。`Size` 是 `omitempty`，旧二进制读新记录会忽略它 ⇒ 回落固定 5m（安全方向）。唯一注意：`xferCrossHomeReapAge` 变小会让回滚后的 leader 更早删对象，回滚窗口内不要有大文件在传 |
| C1 愈合 pass | 纯 revert | **低**。它只清"节点行都不存在"的孤儿 marker，回滚后那些 marker 会重新长回来（噪音，非损坏） |
| C1 finalize op | **不对称** | 一旦在某节点执行过 force-single 且**恰好**留下过非终态 finalize op，回滚后的旧二进制既驱动不了它（`driveOne` 无 case）也 abort 不了它（`PlanClusterOpTransition:166` 连 FromState 一起校验）。**缓解**：(a) op 只在 prune 失败时创建（罕见）；(b) 有复制式 deadline ⇒ 在新二进制下是秒~分钟级的短命对象；(c) 本轮同时把 `AbortOp` 改成不依赖 FromState 枚举，让未来的新 kind 不再复现。**永久决策**：回滚前须先 `cluster ops show` 确认无非终态 finalize op |

---

## 11. 完成度自查（内审的审计基线）

内审 workflow 必须包含一个**完成度稽查 agent**，逐条核对下表，
任何 PARTIAL / MISSING 都是 BLOCKER。规避清单来自 plan workflow 的 scope-guard lane，
是"赶时间的实现者最可能的半成品做法"：

| 交付 | 最可能的规避 |
|---|---|
| C3-1/8 | 不建共享分类器，两端各写一个 switch ⇒ 今天的分歧修好了，**产生分歧的机制原样保留** |
| C3-1 | 分类器写成 `default: return behind` ⇒ 新增 Action 静默落进"假绿"方向，无测试变红 |
| C3-2 | `topoSelfReport` 加了字段但 `topology_reconcile.go:149` 的 `Store` 漏传（结构体字面量按字段名写，漏一个不报错）⇒ 全链路恒为 ""，"已上 wire"而零效果 |
| C3-5 | 只改 peer 分支、漏掉 self 行 ⇒ 自己这台的 TOPO 列永远走 fallback，而 STUCK 最常发生在自己身上 |
| C3-7/8 | broker 侧改 switch，CLI 侧只把 `topoCell` 的子串列表**补上 `render`** ⇒ 字段上了 wire 但没有渲染器用它 |
| C3-9/10/11 | III/V/VI 完全不提，清单打勾"两个渲染器已改" |
| C3-13 | drill 加一行 `grep -q TOPO` ⇒ 断言列存在、不断言**极性** |
| C2-2 | 测试写成 `if budget != 5*time.Minute*6` ⇒ **断言公式而非关系**，5m/2GiB 下次照样各自漂移 |
| C2-1 | 不加上界 ⇒ 对 tier-B 等于取消超时保护，audit 层面看不出变化 |
| C2-3 | `xferCrossHomeReapAge` 不动（"关系还在啊"）⇒ **亲手造出新的数据删除缺陷**，而既有断言仍绿 |
| C2-5 | 只改 `transfer.go` 的看门狗，`xfer_inflight.go` 不动 ⇒ 崩溃恢复按 5m 判 stranded，测试全在另一侧所以全绿 |
| C2-5 | `Size` 字段加了但 `writeXferInflight` 只在 push 分支填 ⇒ 恒为 0，"已实现"而零效果 |
| C2-8 | agent 侧不改（"风险大"）⇒ 只改 broker 是净倒退 |
| C2-10 | 只把 broker 端改成引用常量 ⇒ 三份拷贝变成"一份 SSOT + 两份拷贝" |
| C2-11 | 守卫测试误杀 `xferEventsHistoryReserve` ⇒ 加 file:line 豁免 ⇒ 豁免列表开始腐化 |
| C2-12 | 常量提了，`transfer.go:587` 的 `"(2 GiB)"` 不动 ⇒ 运维在错误串里读到的还是旧值 |
| C1-2 | `driveOne` 忘了加 case（或加进 `default` 里 no-op）⇒ 测试因直接调 `driveForceSingleFinalize` 而绿，**现网 op 永久非终态** |
| C1-3 | driver 重新读 roster 推导 abandoned ⇒ 第二 tick 集合缩小，剩下的 ghost 永远轮不到 |
| C1-4 | 失败进 `OpStateBlocked`（"等运维 confirm 更安全"）⇒ 灾难现场运维手上只有一个孤节点，换个形状的死路 |
| C1-5 | 新增 op 机器但 commit 里的四步**没删也没分支**，"作为快速路径保留" ⇒ 两个执行者，新机器从未被真正走过 |
| C1-6 | 只做"commit 里插 op"，不升级 leadership-edge ⇒ **崩溃窗口仍是永久 ghost**，还多一层"看起来有机制"的假象 |
| C1-7 | `upgradeActive` 冻结原样保留、不写注释 ⇒ 灾难恢复时 finalize op 永不驱动 |
| C1-12 | 愈合 pass 放宽到 `marker ∧ phase==VOTER` ⇒ 清掉运维正在重试的那次 drain 的 marker（N7 的反面） |
| C1-12 | pass 自己写一条新的 `UPDATE cluster_meta` 而不走 `PlanClusterDrainSet` ⇒ 违反 one-vote-veto |
| C1-13 | 只 `logger.Warn("inconsistent")` 就算交付 ⇒ 运维在 `cluster status` 里看不到任何东西 |
| C1-16 | drill 只加一行注释说"prune 失败路径由单测覆盖" ⇒ 用 hermetic 层回答只有部署层能回答的问题 |
| 跨批 | 为了让某个 additive 字段"干净"顺手 bump `ProtoVersion` ⇒ 现网 6 个 agent 必须**重装** |
| 跨批 | 新测试文件叫 `c1_*_test.go`，被冻结门拦下后**往 `legacy_process_named_list.go` 加一行**绕过 |
| 跨批 | 把需要裁决的项写成 "TODO / 后续增量" ⇒ **操作者指令直接禁止**；不做必须是带理由的永久决策 |
| 跨批 | 编辑中途跑 `make e2e-parallel` 然后乱归因；或并行绿了再手工串行"复核一遍"（CLAUDE.md §5 明令严禁，串行 target 已从 Makefile 删除） |

---

## 12. 开放项处置规则

本 plan 定稿时仍有一处取值待定：§6.2 的 `xferMinThroughput` 与 §6.3 的
`xferCrossHomeReapAge` 新关系式的具体形式。

**处置规则（不是延后）**：实现阶段由主进程按 §6.2 的约束（必须有推导、
必须说清小盘 broker 的最坏 bucket 占用）直接定值并写进代码注释与关系式测试；
内审的完成度稽查 agent 必须把"该常量有无推导、推导是否站得住"作为一条独立 finding 核验。
**它不会被跳过，也不会被写成 TODO。**
