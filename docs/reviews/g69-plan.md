# G69 — `cluster add` 必须先证明 JS meta 能放置 R=N 资产，再宣告 SERVING（#67 sub-face 4）— PLAN

> Status: **Stage A finalized**（主进程是唯一定稿人）。由 9 agent Opus 对抗性 workflow 起草
> （4 lens 起草 → 4 对抗互评 → 1 综合），主进程**逐条对源码复核了全部承重结论**并做出 §0 决定。
> 叶子增量。按 CLAUDE.md §3（Stage B 实现 → Stage C 内审 → 外审门）。
>
> 上游：`docs/deploy-tier-gotchas.md` `### #67` open **sub-face 4**；G67 内审称其为
> "the item most likely to bite a real operator on the first push after a grow"。
> 基线：main `b602fc7` + **同树未提交的 R16 与 G67**（两者停在外审门；本增量叠在其上、共用同一道门）。

---

## 0. 定稿人决定（binding）

1. **推翻我自己的选型稿。** `docs/reviews/g69-subface4-scoping.md` 的核心主张——「对称复用
   `clusterStreamsReady`」——**经复核不成立，作废**：
   - `ObserveReplicas` 枚举 `events` + 每个 `history-<sid>` + **每个活的 `OBJ_xfer-*`**
     （`audit_publisher.go:519-545`）；
   - `AllAtTarget` 要求每条流 `Ready`，而 `Ready` 基于 `ActualReplicas`，后者**只数
     `p.Current && !p.Offline` 的 peer**（`jsstream/replicas.go:78`）。
   ⇒ 照选型稿实现，**每次 grow 都要等到所有流（含多 GiB 对象存储）字节复制完**；`audit.go` 自己就记着
   滞留的 under-target `OBJ_xfer-*` 会「blocks the D7 retire gate (AllAtTarget) forever」。
   **这会做出一个让正常扩容卡死的门。** 对抗流程在这里救了一次。
2. **判据改为「**放置**」而非「**追平**」，且只看 `events` 一条流。** 新增
   `jsstream.AssignedReplicas`：数 JS **meta 已指派**给该流 raft group 的 peer 数，**不要求 `Current`**。
   理由：一个刚被指派、仍在拷贝字节的 peer，**已经证明了 meta 能在 R=N 放置一个组**——这正是新建空资产
   所需的全部。`events` 是 `ReconcileOnce` **无条件且最先**抬升的规范流（`audit_publisher.go:481`）。
   `Ready` 与 retire 的全部调用方**一个字节都不动**。
3. **OQ-1 — 到期后 ADVANCE，不 escalate 到 `OpStateBlocked`。** 已源码复核：
   `--auto-confirm-catchup` 默认 **0**（`cluster_add.go:132`），而
   `blockedConfirmDecision(0,0,false)` 首行 `confirms >= budget` 即命中 ⇒ **第一次 BLOCKED 轮询就报预算耗尽**
   ⇒ `cluster add` 立刻非零退出、且给出**错误的成因串**（"joiner 没在追赶"），op 保持非终态，
   `assertNoActiveOp` 随即围死成员变更面；部署层上约 12 个会 grow 的 drill 全部 SETUP-RED。
   而 pre-fix 状态**没有任何数据不安全**：grow 物理上已完成，G67 已让那次 push 失败变诚实可重试。
   ⇒ **到期即推进到 SERVING**（退化成与今天完全一致），并留下 timeline 证据。
4. **OQ-5 — 不加 sys event，只留 timeline 条目。** 已复核 `appendTimeline` 是**追加**、
   终态 transition 不会抹除，故 timeline 是持久且可 grep 的证据（drill 经
   `cluster ops show --json` 读取）。在一棵已压着两个未外审增量的树上，**少加一份 wire 面**。
   若日后测量表明需要车队级告警，再单独加。
5. **OQ-2/3/4/6 — 采纳综合稿建议**：共用现有 `CatchupDeadline`（不加列）；用专用 2 次往返探针
   （不从 `StatusReport` 里穿线）；**不**把 `drills/lib/setup-forcesingle.sh` 的接受-重试分支翻成
   product_red（那 6 个 witness 都期望 GREEN 且无台账行，会造出无主的 PRODUCT-RED 通道）；
   **接受并文档化 N≥4 时本门为 no-op**（`ReplicasFor` 封顶 3 是架构钉，#67 实测发生在 1→2）。
6. **文案诚实约束（硬性）**：探针注释**不得**声称「meta 把资产放到了 joiner 上」——它没有证明这一点，
   在 N≥4 时更是对 joiner 一无所证。

---

## 1. 根因

`driveJoin` 的 `case cluster.OpStateNatsRolledOut:` 在终态 transition 前**只有一道门**：
`topoAdvance(op, sub, true)` → `topoConvergedForOp`，它断言的是每个 voter 都报告
`TopoObserved >= op.TopoTargetGen`——即 nats.conf 已 rollout 且活进程已加载。
**join 阶梯上没有任何合取项过问 JS meta 能否在 `jsstream.ReplicasFor(NumVoters)` 放置资产。**
`waitJoinServing` 一见 `SERVING` 即返回 ⇒ `cluster add` 在「第一条
`CreateObjectStore(Replicas: 2)` 仍可能失败」的时刻就 rc=0。

**明确不是本缺陷**：push 路径（G67 正确，8s 预算不得放宽）；`topoConvergedForOp` 本身
（它正确回答了自己的问题，只是作为**终态**门欠一个合取项）；N=1 去集群化那道**故意无界**的
两阶段边界（retire-only，drill 41 + `TestTopoAdvanceN1BoundaryStaysNonterminal` 钉住）；
retire 的 `clusterStreamsReady`（那是**数据安全**问题，join 问的是**放置能力**问题）；
单 broker 模式（`wireClusterLate` 根本不跑）；`/jsz` 判定机（R16 A5 已刻意推迟，JS 客户端本就能回答）。

**证据诚实性**：实测签名是 `create_bucket: context deadline exceeded`——一个**超时**，
不是 `no suitable peer` 式的放置拒绝。grow 侧的门消除的是**结构性窗口**，消除不了宿主机饱和。
验收主张必须据此收窄（§5）。

---

## 2. 设计

三个源文件、一个 seam、一个合取项。**无新 op 状态、无 schema 变更、无新列、无新 goroutine、无新循环。**

- **`internal/jsstream/replicas.go`**：新增 `AssignedReplicas(info) int`（nil⇒0；`Cluster==nil`⇒1；
  否则 `1+len(Cluster.Replicas)`）；`StreamReplicaState` **增量**加 `Assigned`/`Configured` 两字段，
  由 `CollectStreamState` 填充。**`Ready` 逐字节不变。**
- **`internal/broker/clusterwrite.go`**：
  - `jsPlaceableFrom(target int, st jsstream.StreamReplicaState, obsErr error) (bool, string)` —— **纯函数、
    无 error 返回**，一切不确定性折叠成 `(false, detail)`。顺序：`target<=1`⇒`(true,"")`；
    `obsErr!=nil`⇒false；`st.Configured<target`⇒false（副本抬升尚未落地）；`st.Assigned<target`⇒false；
    否则 true。
  - `clusterJSPlaceable() (bool, string)` —— 取 `NumVoters` → `ReplicasFor` → `target<=1` 时**不发任何 JS 调用**
    直接返回 → 3s 超时内 `CollectStreamState(events)` → 交给纯函数。
  - `wireClusterLate` 里一行接线，紧挨现有 `streamsReadyFn`。
- **`internal/broker/clusteradmin.go`**：`jsPlaceableFn func() (bool, string)` seam，**nil ⇒ 视为可放置**
  （fail-open，遵循本仓库既有 nil-skip 约定；正因如此源码接线 pin 是**强制项**）。
- **`internal/broker/cluster_operation_controller.go`**：新增 `jsPlacementAdvance(op) bool`，
  在 `driveJoin` 的 `OpStateNatsRolledOut` 分支中、**紧跟 `topoAdvance` 之后、在其余三个阻塞步骤之前**调用。
  机制：seam 为 nil ⇒ true；探针 true ⇒ true；否则若 `CatchupDeadline==0` 或已过期 ⇒
  `recordOpError`（写明「未能证明放置即宣告 SERVING……首推可能被判 `jetstream_not_ready` 需重试一次」）
  后返回 true；否则 `recordOpError(detail)` 返回 false（本 tick 不推进）。

**合取项必须放在 `driveJoin` 里，绝不能塞进共享的 `topoAdvance`**——后者被 retire 复用，且
`topo_advance_test.go` 钉着它现有语义（含 N=1 carve-out）。**若实现迫使你去改 `topo_advance_test.go`，
说明位置放错了**（验收判据，非偏好）。

---

## 3. 为什么这不会变成 #45（机械式论证）

#45 = *fail-closed 门 + 不可靠信号 + 无计数/无 deadline/无看门狗 ⇒ op 永久钉在 `NATS_ROLLED_OUT`
⇒ `assertNoActiveOp` 拒绝下一次成员变更 ⇒ 脊柱 wedge 且运维无状态可操作*。

1. **这不是 fail-closed 门**，是**有界等待且到期结果为 true**——那一 tick 即进终态。
   **不存在任何输入**能把**本合取项**拖过 `op.CatchupDeadline`。
   > **⚠ 内审 G-1 更正（BLOCKER，主进程已发出后才被抓到）**：上面这句**原本写的是对整条阶梯的断言，那是假的**。
   > 把 op 留在 `NATS_ROLLED_OUT` 度过剩余窗口，等于让**先跑的** `topoAdvance`（fail-**closed**、超时分支是
   > `OpStateBlocked`）每 tick 重新求值；修复前 join 在拓扑收敛的第一个 tick 即终态、此后免疫。
   > 且相关性真实：`topoConvergedForOp` 需要每个 voter 的 `TopoReported`，那只在该节点应答了**当次 tick 的
   > 健康扫描**时才置位，而**同一台饱和主机既让 JS 放置变慢、也会丢健康回复**。落进 BLOCKED 后
   > `blockedConfirmDecision(0,0,false)` 首轮即报预算耗尽 ⇒ `cluster add` 带错误成因串失败 + 成员变更面被围死
   > ——**正是 §0.3 拒绝的那个后果**。
   > **修复**：`jsGateExpiryReserve = 30s`（≥2 个 observe tick），保证降级 tick 严格早于超时 tick；
   > pin：`TestJoinDegradesBeforeTopoAdvanceCanBlock` + `TestJSGateReserveExceedsTwoObserveTicks`。
2. **界来自已有的复制列**：`toNatsRolledOut` 入场时就写 `CatchupDeadline`；本合取项读同一个值。
   kill-9 / 换主后按同一绝对纳秒续算。`NATS_ROLLED_OUT` 总驻留时长**不变**。
3. **零 deadline 是推进而非卡死**（遗留/手工种的行）。
4. **界之前没有提前返回**：seam **无 error 返回**，「探针每 tick 都出错从而跳过 deadline 检查」这种洞
   **在签名上不可表达**。
5. **等待期间零 raft 写**：合取项排在两个 `Propose` 之前；`recordOpError` 按 `op.LastError` 变化门控。
6. **无新 op 状态 ⇒ 无混版风险**：旧 leader 接手只看到 `NATS_ROLLED_OUT`，退化成今天的行为。
7. **retire 不可达**：只在 `driveJoin` 调用；且 `ReplicasFor(nv)<=1` 时判据自禁。
8. **半接线是惰性而非致命**：nil seam ⇒ 可放置 ⇒ 今天的行为。

**这一选择的代价，直说**：到期时我们发出一个**没能证明**的 SERVING——那**恰好就是今天的行为**，
外加一条写明原因的 timeline 条目。

---

## 4. 代价

- **节律**：`driveLeaderMaintenance` 内联在 `runObserveLoop`，`observeTickInterval = 5s`，leader-only。
  仅在 `topoAdvance` 已返回 true 的 tick 上求值；无在飞 join 时**为零**。
- **基线诚实更正**（综合稿推翻了它自己某条 lane 的论证，已复核）：`topoConvergedForOp` →
  `StatusReport("ctl-nats")` → **无条件** `streamObserve()`（整轮 `ObserveReplicas`，
  `clusterstatus.go:231`）+ `healthPoll()`。所以该 tick **本来就**在跑完整的 `ObserveReplicas`——
  「join 每 tick、retire 低频」这个反对理由**是空的**；§1 对 `clusterStreamsReady` 的否决**纯粹是语义的**，
  并不依赖代价。
- **新探针边际代价**：恰好 **2 次** JS 往返（`js.Stream` + `s.Info`），≤3s。典型 grow 共 1–2 次。
- **3s 上限是卫生而非保护**（同一内联调用里已有 2s `healthPoll` 和无界 `ObserveReplicas`）。
- **健康 grow 增加的延迟：未测量，如实标注。** 台账里所有数字测的是 `CreateObjectStore`，不是 meta 指派；
  **不得**挪用 G67 的 1.66s/>8s。timeline 条目就是为了让第一次带载运行把它测出来。

---

## 5. 风险与守卫（摘承重项）

| # | 风险 | 守卫 |
|---|---|---|
| R1 | **#45 复发** | §3 全部八条；pin P1/P2/P4/P6 |
| R2 | **误等健康 grow** | 判据是**指派**不是追平，且**只看 events**；pin 断言「assigned+configured 达标但 actual 未达标」必须判**可放置** |
| R3 | 等待期空转 raft 写 | 合取项排在两个 `Propose` 前；`recordOpError` 变更门控；pin 断言等待 tick 间 `AppliedIndex` 不变 |
| R4 | 序列化窗口 | 合取项排在 `PlanClearGrowActive` **之前** |
| R6 | **retire 回归 / drill 41** | 只在 `driveJoin`；pin 用**会 panic 的**探针驱动 retire（调用次数须为 0）；`topo_advance_test.go` **不得需要修改** |
| R8 | **N≥4 本门为 no-op** | 接受并文档化（`ReplicasFor` 封顶 3；#67 实测在 1→2）。注释**不得**声称证明了 joiner |
| R10 | events 流缺失（`--reset-former-js` 切换路径） | `CollectStreamState` 出错 ⇒ `(false,detail)` ⇒ 有界等待 ⇒ 到期推进。`ReconcileOnce` 以 100ms 轮询无条件最先抬升 events，构造上自愈 |
| R12 | **G-13（内审，接受并登记）**：合取项排在 `PlanClearForceSingle` 之前，故等待期间一个 force-single **恢复型** grow 会让 `cluster status` 维持 exit 3 + 严重横幅，并让 `d8_alerts.go` 继续硬门破坏性操作。受同一 deadline 约束、且仅影响 force-single 回扩这一窄场景。**不改顺序**：`PlanClearForceSingle` 是无条件 `DELETE`，把它提到合取项之前会破坏「等待期零 raft 写」这条更重要的性质 |
| R13 | **G-17（内审，pre-existing 非 G69）**：`driveCatchingUp` → `boundCatchingUp` 仍带着 Stage-B 那个形状（`recordOpError` 真写库 → 随后 `transition` 用**陈旧**内存 timeline 重建、抹掉刚写的条目）。`Broker.clusterCaughtUp` 从不返回非 nil，故探针分支生产上是死的；活体变体是 `nodeRaftAddr`/`setPhase` 在跨越 deadline 那一 tick 失败，以及遗留零 deadline 行。**本增量不修**（不在 G69 面），登记以便下一个碰这文件的增量结构性处理：**同一 tick 内绝不跨两次写去读/改 `op.Timeline`** |
| R11 | drill 67 翻转后的 oracle 在 `-j` 饱和下误触 | 它是 **product_red 而非 assert_fail**：响亮、可计数、可归因，drill 仍可判定；且在验收跑之前先在 tsv 预登记 |

---

## 6. 测试与部署层证据

**已核实的地基**：`grep OpStateServing internal/broker/*_test.go` 为**空**——今天**没有任何** hermetic 测试
把 join 驱动到终态 SERVING。所以 P1/P3 是首创，也正因如此 nil seam 必须 fail-open 且源码接线 pin 是强制项。
夹具用 `g3AdminWithSelf`（**不是**裸 `NewClusterAdmin`——终态臂会调
`deriveAndConvergeSeedsFromRoster`，裸夹具下它出错会让每条 pin 因**错误的理由**通过）。

关键 pin（每条配「它杀死哪个变异」）：

- **P1（RED-first）**：探针恒 false、时钟在 deadline 内 ⇒ 3 个 tick 后仍 `NATS_ROLLED_OUT`、`terminal==false`。
  **杀死：什么都不做**（今天 tick 1 就 SERVING），以及「只记录不把关」的伪合取项。
- **P2**：同上但时钟**越过** deadline ⇒ 进终态 SERVING **且** timeline 含未证明放置的条目；
  等待 tick 间 `AppliedIndex` 不变。**杀死：把有界等待改成 #45 式卡死；把合取项放到 `PlanClearGrowActive` 之后。**
- **P3**：探针 false,false,true ⇒ 第 3 tick 进终态、grow marker 已清、`last_error==""`。**杀死：门永不打开／判据反相。**
- **P6（retire 回归）**：用**会 panic 的**探针驱动 retire ⇒ 调用次数必须为 0；
  且 `topo_advance_test.go` / `g3_seed_helper_test.go` **不得修改**即通过。
- **P8/P10**：纯判据表驱动（含 R2 那一行）；`AssignedReplicas` 忽略 `Current`。
- **P9**：源码接线 pin（`wireClusterLate` 必须赋值 `jsPlaceableFn`）——本仓库已两次栽在半接线上。

**部署层**：drill 67 的 `CONTROL(before)` 现有那条无条件 `not_covered … "#67 sub-face 4"` gap
**就是本增量的具名 oracle**——修好后它应当**自然消失**。翻 tsv **只在部署层跑绿之后**。
回归集：42 / 51 / 11 / 10（grow 家族）+ 41（retire N=1 边界）。

---

## 7. 我最没把握的（如实登记）

1. ~~**充分性**：「meta 给 events 指派了 N 个 peer」是「能放置新 R=N 组」的**强证据**但非证明；~~
   > **已闭合（外审 F3 + round-3，2026-07-23）**：判据不再只是代理。门现在**先**过便宜的 events
   > `Configured/Assigned` 前置（其中 `nil`/`Offline` 已按 F3 排除，杀掉 corpse 假证），**再用空 canary
   > 直接测量**——以目标副本数建一个空流并立即删除，问的正是 CLI 契约承诺的那个问题「此刻能否创建
   > R=N 新资产」。空 canary 没有历史数据，因此不引入 gating-on-catch-up 那种字节拷贝等待。
   > 真 JetStream 上的行为钉：`TestPlacementCanaryMeasuresRatherThanInfers`（R=1 成功且零残留；
   > R=3 在单节点上必须**报告失败**——代理推断做不到这一半）。
   > **仍欠**：`3→2(不 peer-remove)→3` 的实机 differential。
   原文保留如下：**「meta 给 events 指派了 N 个 peer」是「能放置新 R=N 组」的强证据但非证明**；
   实测失败是 `CreateObjectStore` 上的**超时**，而对象存储另有 storage/`MaxBytes` 约束
   （已单独登记为 `tier_b_store_too_small`）。**我没有验证「流放置成功 ⇒ 对象存储放置成功」。**
   带载的 drill 67 是唯一证伪者。
2. **指派 vs 追平**：选指派是基于源码证据（1 GiB events 上限 × `Current` 门控 = 真实的 WAN 误等），
   但它**更弱**：一个已指派却在颠簸的 meta 仍可能拒绝 create。若带载运行显示太弱，
   下一根杠杆在 push 侧，不是更强的 grow 侧判据。
3. **共享且可能被饿死的窗口**：`CatchupDeadline` 在入场时一次性写死、`topoAdvance` 先花；
   拓扑慢的 grow 可能只留给 JS 合取项接近零的窗口。有了「到期推进」这是**良性**的
   （退化成今天），但也意味着**本修复在最需要它的负载下最弱**。
4. **健康 grow 增加的延迟未测量**——timeline 条目就是为了让第一次带载运行测出来。

---

## 8. 实现顺序

0. **P1 先写、必须 RED**（只加 `jsPlaceableFn` 字段让其编译，**不接线**）——今天 `driveJoin` tick 1 就进 SERVING，
   这条红灯即根因裁决。
1. `jsstream`：`AssignedReplicas` + 增量字段 ⇒ P10 绿；确认 `Ready` 与全部 retire 调用方逐字节不变。
2. `clusterwrite.go`：`jsPlaceableFrom` 纯函数 ⇒ P8 绿（不需要 broker、不需要 JS）。
3. `cluster_operation_controller.go`：`jsPlacementAdvance` + 两行调用点 ⇒ **P1 转绿**，随后 P2/P3。
4. `clusterJSPlaceable` + `wireClusterLate` 接线 ⇒ P9。
5. **P6 retire spy**；确认 `topo_advance_test.go` 与 `g3_seed_helper_test.go` **未经修改**仍通过——
   若任一需要改，**停下**：合取项放错了函数。
6. 硬闸 + 部署层（drill 67 验 oracle 消失；42/51/11/10/41 回归）。
