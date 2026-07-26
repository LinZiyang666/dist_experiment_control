# L11 — reconcile 引擎与集群操作编排的抽象质量（结构性质量审计）

- lane key: `reconcile-engine`
- 审计日期：2026-07-25
- 审计对象版本：`main` @ 84bf030（v0.4.7 已发布）
- 性质：**只读结构审计**。不找 bug（那是 `quality-audit/01..06` 的活），找冗余 / 重复 / 抽象错位 / 演进阻力。
  过程中撞见的真缺陷记了 1 条（F4 的 TOPO 列渲染不一致），其余全部是结构判断。

---

## 结论

**净判断：这块不是屎山。6,203 行里只有 3,830 行是代码（33% 是注释），而这 3,830 行要承担
"raft 命令域 + NATS JetStream meta 两个独立共识域，经由一个只有进程重启才能完全生效的磁盘文件
（nats.conf）保持互相一致" —— 这是问题域本身强加的复杂度，抽象总体选对了。真正的结构债是
四条具体的："收敛调度有两套"、"retire 有两套实现且不可达的那套更危险"、"一列 deadline 被四种
语义分时复用"、"『拓扑是否收敛』有四个实现、三种失败极性"。**

bloat 打分：**4 / 10**（1=精炼，5=正常工程债，10=屎山）。

打 4 而不是 5 的理由：

1. **核心抽象站得住**。`reconcileRegistry`（189 行代码）是一个不持有 goroutine、不持有 timer、
   锚定 deadline、带指数退避、可在假时钟下微秒级证明等价性的调度器 —— 这是我在本次审计里读到的
   最干净的一块。`internal/natsreconcile`（150 行代码）是一个纯 step machine，import 面干净
   （只 natsconf + natscluster，无 nats / 无 raft / 无 broker），reload 与 probe 都是注入的 seam。
2. **op 状态机是这个问题域的正确答案**。`driveJoin` / `driveRetire` 每 tick 只推进一步、每步前
   从 substrate（`cluster_nodes.phase` + raft config + topology_generation）重新推导、每个不可逆
   副作用前再查一次 `opStillLive` —— 这是可 resume 分布式操作该有的样子，不是脚本。
3. **团队会去重**。`natsconf.MoveAsideJSStore` 被显式提取一次给 4 个调用方共享
   （`cluster_grow_cutover.go:280-282` 的 R16 A0 注释）；`startLockKeeper` 被 grow 和 upgrade 共享；
   `migrateExposes` / `pendingRetireConvergence` 被同步 drain 和 op retire 共享。
   这不是一个"每次都重写一遍"的代码库。

打 4 而不是 2/3 的理由：下面 F1–F3 是真结构债，不是洁癖。

verdict：**minor-debt**。

---

## 范围与方法

### 范围（生产代码，不含 `_test.go`）

| 文件 | total | 注释 | 空行 | **代码** |
|---|---:|---:|---:|---:|
| `internal/broker/cluster_operation_controller.go` | 1248 | 441 | 45 | 762 |
| `internal/broker/clusterdrain.go` | 959 | 299 | 45 | 615 |
| `internal/broker/proxy_reconcile.go` | 373 | 86 | 18 | 269 |
| `internal/broker/topology_reconcile.go` | 367 | 84 | 19 | 264 |
| `internal/broker/cluster_grow_cutover.go` | 367 | 103 | 29 | 235 |
| `internal/broker/reconcile_registry.go` | 342 | 127 | 26 | 189 |
| `internal/broker/reconcile.go` | 325 | 115 | 16 | 194 |
| `internal/broker/alert_reconcile.go` | 322 | 90 | 19 | 213 |
| `internal/broker/force_single_online.go` | 320 | 72 | 19 | 229 |
| `internal/broker/cluster_upgrade_trigger.go` | 259 | 76 | 14 | 169 |
| `internal/natsreconcile/reconcile.go` | 246 | 77 | 19 | 150 |
| `internal/broker/cluster_grow_trigger.go` | 244 | 50 | 19 | 175 |
| `internal/broker/reconcile_passes.go` | 226 | 134 | 15 | 77 |
| `internal/broker/transfer_reconcile.go` | 213 | 93 | 8 | 112 |
| `internal/broker/reconcile_grow_lock.go` | 198 | 103 | 14 | 81 |
| `internal/broker/reconcile_upgrade_lock.go` | 133 | 81 | 8 | 44 |
| `internal/broker/tunnel_reconcile.go` | 61 | 4 | 5 | 52 |
| **合计** | **6203** | **2035 (33%)** | **338** | **3830** |

（用一次性 `go/ast` 脚本统计，写在 `/home/weiland/.claude/jobs/…/tmp/`，未进仓库。）

关联但不在 lane 内、为回答 Q4 必须读的：`cmd/tether/cluster_add_drive.go`(886)、
`cmd/tether/cluster_upgrade_drive.go`(451)、`internal/cluster/operation_ops.go`、
`internal/broker/clusteradmin.go`、`internal/broker/clusterstatus.go`、`internal/broker/observability.go`。

### 方法

- 核心文件全文读入（`reconcile_registry.go` / `reconcile_passes.go` / `cluster_operation_controller.go`
  全 1248 行 / `clusterdrain.go` 前 420 行 / `topology_reconcile.go` / `natsreconcile/reconcile.go`
  / `force_single_online.go` 关键段 / `cluster_grow_cutover.go` 关键段）。
- 调用图靠 `grep` 交叉验证（哪些 pass 真被注册、哪些循环自己起 ticker、哪个 CLI 动词打到哪个 handler）。
- `go build ./...` 通过（确认读的是能编译的代码）。
- **未运行任何测试**，未碰 `test/simcluster/`。

---

## Findings

### F1（high）收敛调度实际上有**两套**：registry 的 9 个 pass，和 `runObserveLoop` 这个"非正式 registry"上的 6 个职责 —— 而这不是遗忘，是 registry 执行模型装不下慢 pass

**证据**

- `internal/broker/reconcile_registry.go:11-21` 明确声明 registry 存在的理由：
  > "every convergence duty that was NOT one of those three inline bodies had to invent its own cadence"
- 实际注册的只有 9 个（`internal/broker/reconcile_passes.go:57,82,96,116,162,177,193,201,224`）。
- 另外**四个收敛职责各自起 ticker**：
  - `internal/broker/topology_reconcile.go:61-79`（5s，per-broker）
  - `internal/broker/alert_reconcile.go:101-114`（500ms，leader + fence 门）
  - `internal/broker/proxy_reconcile.go:28`（挂在 observe loop 上）
  - `internal/broker/cluster_operation_controller.go:357-393`（挂在 observe loop 上）
  - 启动点集中在 `internal/broker/clusterwrite.go:439-447`（4–5 个 goroutine）。
- `internal/broker/observability.go:236-278` 一个 5s ticker 上塞了 **6 个互不相干的职责**：
  ① `fsArm.observeLeadership`（每 tick，非 leader-gated）② `ReconcileMembershipOnLeadership`（leader 上升沿）
  ③ `autoRebalanceArm.reset`（leader 下降沿）④ `driveLeaderMaintenance`（= 驱动所有在飞 op + seed 收敛）
  ⑤ `driveProxyReconcile` ⑥ `observeOnce`（告警 + auto-rebalance）。

**为什么不是"他们忘了注册"**：registry 的执行模型结构上装不下慢 pass。
`reconcile_registry.go:135-137` 的 `runMu` 在**整个 sweep 期间持有，包括 fn 调用**；
`internal/broker/broker.go:1281-1289` 的驱动循环是一个裸 `select`，**没有 per-pass 超时、没有并发**。
所以任何会阻塞的 pass 都会拖住整个 sweep —— `observeOnce` 有 2s 的 scatter-gather 窗口
（`observability.go:219`），`waitNatsLoaded` 会阻塞 30s（`topology_reconcile.go:164-182`），
两者放进 1s granularity 的串行 registry 会让 `node-states`/`ports`/`tunnel-sessions` 掉 tick。
**这个限制在 registry 那 60 行"不可协商"的接口说明里一个字都没提。**

顺带：那个"冻结的 tuple `(name, interval, leaderOnly, lastTick, fn)`"也 under-model 了权限空间。
**9 个 pass 里有 6 个在函数体里自己再做一遍门控**，因为 `leaderOnly bool` 表达不了：

| pass | 体内额外门控 |
|---|---|
| `proc-gc` | `if b.clusterMode { return nil }`（模式门，非 leader 门） |
| `xfer-orphan-reap` | `b.js != nil` + `reaperCaughtUp()` + `homeOwnsXferBucket`（home 分区，非 leader） |
| `xfer-inflight-finalize` | `xferInflightDir() != ""` |
| `grow-lock` | `clusterMode && cl && node` + `reaperCaughtUp()` |
| `upgrade-lock` | 同上 |
| `home-delivery` | `clusterMode && selfID != ""` + `nc != nil` |

只有 `node-states` / `ports` / `tunnel-sessions` 是"纯 tuple"。
也就是说 registry 宣称的"exactly one writer 由中心保证"实际上是**九种方式各保证一次**。

**这让什么未来改动变难/变危险**

1. **可观测性是断裂的。** `internal/broker/runtime_introspect.go:57-69` 的 `admin runtime` 报告
   9 个注册 pass 的 `runs/skips/lastErr/lastTick`，对另外 4 个循环**一个字段都没有**。
   其中包括 operation controller —— 全项目最容易出运维事故的收敛职责。
   op 的**状态**能从 `cluster ops show` 看到，但**驱动器是否还活着**看不到：
   observe loop 若卡住（比如 `observeOnce` 的 2s 窗口被一个不回包的 peer 反复吃满），
   在飞 op 会停在某个 state 带一条陈旧 `last_error`，运维没有任何信号能区分"正在等"和"没人推了"。
2. **加一个新收敛职责时，要在两套机制之间做一个没有文档的选择**，而两者的 leader 门
   （registry 的 `IsLeader()` vs AlertReconciler 的 `IsLeader() && !LeaderContactStale()`）、
   失败处理（registry 有指数退避，其余四个只 log）、可观测性都不同。选错不会报错，只会在事故时才发现。

**建议**

- 给 `reconcilePass` 加两样东西：（a）执行模式（`inline` / `own-goroutine-with-timeout`），
  （b）一个 per-pass state slot（见 F1a）。然后把 `driveProxyReconcile` 和 `driveLeaderMaintenance`
  移进去 —— 它们本来就是 `func()` 形状、本来就 leader-gated、本来就 idempotent。
- 若嫌上面动得大，**最小闭合**：把那 4 个循环的 `lastTick/runs/lastErr` 也塞进 `RuntimeReport`。
  这单独就能把"op 驱动器卡住"从不可观测变成可观测。
- 顺手把 `leaderOnly bool` 换成 `authority func() bool`，让 6 个体内门控上浮到注册处，
  registry 的 skip 计数才对得上真实语义。

**量化**：4 个游离循环、6 个职责挤在一个 ticker、9/13 个收敛职责可观测。
移动 2 个职责进 registry 净变化约 −40 / +30 行；最小闭合方案约 +45 行。
**风险：medium**（改的是线上收敛节奏；`make e2e` + 相关 simcluster drill 必跑）。

---

### F1a（附属）`reconcileTopologyOnce` 用 6 个返回值手搓 pass 状态

`internal/broker/topology_reconcile.go:81`：

```go
func (b *Broker) reconcileTopologyOnce(ctx context.Context, lastApplied, lastObserved uint64,
    lastReloadMtime, lastRestartMtime int64, lastEventKey string) (uint64, uint64, int64, int64, string)
```

五个循环状态（applied gen / observed gen / 上次 reload 的 conf mtime / 上次 restart 的 mtime /
上次发过 event 的 key）在函数签名里穿进穿出，因为 `reconcilePassFn` 是 `func(ctx, now) error`，
**没有状态位**。这就是 F1"接口 under-model"的直接产物。
修法：pass 携带一个 `any` state slot（或干脆让 pass 是个 interface 而非裸 func）。
**量化**：这一个签名就能从 6 返回值降到 1（`error`），约 −12 行、−1 处 5 元组手抄。

---

### F2（high）`DrainNode` 的 retire 分支是**产品不可达的第二套 retire 实现**，而且是没有保护的那套

**证据**

- 实现在 `internal/broker/clusterdrain.go:85-206`；retire 专属段是 `:124-132`（streamsReady 门）
  与 `:182-205`（`setPhase RETIRING` → `RemoveServer` → `PlanClusterNodeRemove` → 清 drain marker → 收敛 seeds）。
- **CLI 把 `Retire` 硬编码成 false**：`cmd/tether/cluster.go:524`
  `req := adminsock.Request{Op: adminsock.OpClusterDrain, NodeID: node, Retire: false, ...}`。
- `docs/cluster.md:64` 明确记为旧动词：`tether cluster drain <n> --retire` → `tether cluster retire <n>`。
- 全仓 `Retire: true` / `DrainNode(x, true, …)` 的调用方只有测试：
  `internal/broker/clusterstatus_test.go:132`、`test/d7/integration_test.go:532`。
- `ErrStreamsNotAtTarget`（`clusterdrain.go:61`）**唯一引用点**就在这段不可达分支里（`:130`）。

**为什么是债，不是"留着也没坏处"**

这段代码做的是 `RemoveServer` + roster 删行 —— 不可逆的 raft 成员变更 —— 而它**没有** op 状态机的
任何一层保护：没有 `opStillLive` 的 TOCTOU 复查（对比 `cluster_operation_controller.go:900,928,956`
三处），没有 `boundRehomeConvergence` 的可复制 deadline，没有 BLOCKED 逃生口，不可 resume
（中途 leader 换届就永久半成品）。它离"可达"只差一个手写 adminsock 请求 —— socket 至今接受 `retire:true`。

更实际的伤害是**同一条不变量被写了两遍、且两遍的界限机制不同**：

| 不变量 | op 机器（`driveRetire`） | 同步 `DrainNode` |
|---|---|---|
| F==0 需手输确认 | `StartRetireOperation:203` **+ 每次 drive 重跑** `retireGatePasses:1022` **+ RAFT_REMOVED 前再跑** `:945` | `clusterdrain.go:100` 一次 |
| 最后一个 voter 硬拒 | `:199-202` + `retireGatePasses:1015` | `:95-99` |
| streams 达标 | `STREAMS_AT_TARGET:911` **+ 移除前复查** `:948` | `:124-132` 一次 |
| 数据面收敛 | `pendingRetireConvergence` + **可复制 deadline** `boundRehomeConvergence` | `pendingRetireConvergence` + **墙钟 deadline** `awaitHomeConvergence` |

op 机器那侧的"重复检查"是**刻意的**（"never trust stale consent"，注释在 `:833`）——那是好设计。
坏的是第四列：一套用可复制的 `catchup_deadline`、崩溃可续，一套用调用方传进来的墙钟 deadline、
进程一死就没了。谁改了 retire 的安全语义，必须记得改两处，而只有一处有测试网。

**建议**

1. 删掉 retire 分支 + `ErrStreamsNotAtTarget`；`DrainNode` 去掉 `retire` 参数。
2. adminsock 收到 `Retire: true` 时返回明确拒绝（"use `cluster_retire`"）——
   这对老版本 CLI 是**改善**（原来是静默走无保护路径），但确实是一次跨版本行为变化，需在 release note 写明。
3. 两个测试改指向 `StartRetireOperation`。

**量化**：−约 55 行生产代码、−1 个 sentinel error、−1 条隐藏的不可逆路径；
`clusterdrain.go` 从 615 行代码降到约 560。
**风险：low**（CLI 不可达；adminsock 是 root-only 本地 socket）。不触碰 NATS wire。

---

### F3（high）`catchup_deadline` 一列被**四种语义分时复用**，`SetBarrier` 一个 bool 网关三列 → 四处手抄 read-modify-write，漏抄的失败模式是 **fail-OPEN 的拓扑门**

**证据**

`internal/cluster/operation_ops.go` 的 `PlanClusterOpTransition`：

```go
if in.SetBarrier {
    set += `, barrier = ` + LitInt(...) +
        `, catchup_deadline = ` + LitInt(in.CatchupDeadline) +
        `, topo_target_gen = ` + LitInt(...)
}
```

一个 bool 同时决定三列写不写。于是每个只想改**其中一列**的调用方必须把另外两列原样回抄。
`SetBarrier = true` 共 5 处（`cluster_operation_controller.go:258, 532, 723, 754, 1212`），
其中 **4 处**带着 `in.Barrier = op.Barrier; in.TopoTargetGen = op.TopoTargetGen` 的回抄。

同一列 `catchup_deadline` 承担四种语义：

| 语义 | 位置 | 窗口 |
|---|---|---|
| join catch-up 窗口 | `:530` `adaptiveCatchupDeadline()` | 2min 起，按 DB 大小放大到 30min |
| retire rehome 数据面收敛窗口 | `:750` `opRehomeConvergeTimeout` | 10min |
| 拓扑收敛窗口 | `:1214` `opTopoConvergeTimeout` | 5min |
| JS placement 窗口 | `:1106` `op.CatchupDeadline - jsGateExpiryReserve` | 上一条减 30s |

四种语义的安全性各靠一段"这两个窗口不会重叠"的**人工论证**：
`:741-744`（"a retire never used it for CATCHING_UP"）、
`:1196-1201`（"nothing reads the catch-up meaning past CATCHING_UP"）、
`jsGateExpiryReserve` 那 17 行（`:1059-1075`，一个已承认发过一次的 BLOCKER：
"a BLOCKER I shipped and did not see"）。

**已经咬过一次**：`ConfirmOp:251-262`（F2 external re-review）——
一个 BLOCKED 的 retire 被 confirm 后带着**上一阶段已过期**的 deadline 重新入梯，
下一 tick 立刻又 BLOCK，`cluster ops confirm` 成了 no-op。修法是手动 `in.CatchupDeadline = 0`。

**这让什么未来改动变难/变危险**

- 加**第五个**需要 deadline 的状态（例如给 force-single 的后半段建 op、或给 "JS store 移置" 加个等待），
  必须对已有四种用法逐一重证不重叠 —— 而**没有任何测试会在证错时失败**。
- 漏抄 `topo_target_gen` 的后果是 **fail-OPEN**：`topoConvergedForOp:1035-1037`
  `if op.TopoTargetGen == 0 { return true, "" }` —— 直接判定"已收敛"。
  代码自己在 `:1204-1208`（C4-M5）警告过这一点。也就是说，这个 API 形状**主动邀请**一个
  会把 SERVING/RETIRED 在拓扑未收敛时宣布出去的错误。

**建议**

把 `OpTransitionInput` 的 `barrier / catchup_deadline / topo_target_gen` 从
"一个 bool 门三列"改成三个指针（`*uint64` / `*int64` / `*uint64`），nil = 不动。
`PlanClusterOpTransition` 按非 nil 逐列拼 SET。四处回抄全部消失，
`ConfirmOp` 的"手动清零"变成"传一个 nil"。
更彻底的话把 `catchup_deadline` 拆成 `(deadline_kind TEXT, deadline_ns INTEGER)`，
让"哪一相的窗口"显式化，三段不重叠论证退休。

**量化**：4 处 read-modify-write → 0；约 −15 行；3 段人工不变量论证退休；
fail-open 触发面从"任何忘记回抄的新调用方"降到 0。
**风险：medium**。是**可复制 SQLite schema** 变更（需 migration，且必须在所有 voter 上一致落地），
但**不触碰 NATS wire**（`proto.ProtoVersion` 无关）。指针化那一步甚至可以不动 schema，只改 Go 侧。

---

### F4（medium）"拓扑是否收敛"有**四个实现、三种失败极性**；STUCK 分类靠对 Reason 字符串 substring 匹配，且两端匹配集不一致（**当前真实存在的渲染错误**）

**证据**

`internal/natsreconcile/reconcile.go:21-34` 定义了**类型化的** `Action` 常量
（`ActionNoop / Reloaded / SwappedReloadPending / Rejected / Unresolvable / UnknownDirective /
AwaitingClusteredCutover`）。

但 `internal/broker/clusterwrite.go:117-121`：

```go
type topoSelfReport struct {
    Applied  uint64
    Observed uint64
    Reason   string
}
```

**`Action` 被丢掉了**（`topology_reconcile.go:149` 只 Store 这三个字段）。
于是下游只能对 `Reason` 做 substring 匹配，一共四个消费点：

| # | 位置 | 语义 | 失败极性 |
|---|---|---|---|
| 1 | `cluster_operation_controller.go:1034-1057` `topoConvergedForOp` | op 终态门 | **fail-CLOSED**（unreachable 算未收敛） |
| 2 | `clusterstatus.go:490-502` `computeHealth` 内联 | 健康裁决 | **排除 unreachable**（注释在 `:1031-1033` 说这是"deliberately differs"） |
| 3 | `cmd/tether/cluster.go:431-444` `topoCell` | TOPO 列渲染 | 第三套 |
| 4 | `cmd/tether/cluster_reconcile.go:175-178` | reconcile 输出 | 第四套 |

而 #2 匹配 **三个**子串（`"unrecognized directive"` / `"nats-server -t"` / `"render"`，`:495-497`），
#3 只匹配 **两个**（缺 `"render"`，`cluster.go:437`）。

**当前真实后果（这是本 lane 唯一记的真 bug）**：
`natsreconcile` 在 render/merge 失败时返回 `ActionRejected` + reason
`"render (nats.conf could not be assembled …)"`（`reconcile.go:149-150`）。
→ broker 侧 `topoStuck=true`，健康裁决正确；
→ CLI 的 TOPO 列却渲染 `…`（"还在追赶"），**告诉运维去等一个永远不会来的自愈**。
同理 `Apply` 失败的 `"apply: …"`（`reconcile.go:191`）两端都不匹配，两边都渲染 `…`。

**为什么是结构债**：分类词汇是**跨 adminsock 边界重复的字符串字面量**。
每加一个 `Action`（比如 G4 加的 `ActionAwaitingClusteredCutover`）都要在两个二进制里改 2+ 个
substring 列表，编译器一点忙都帮不上。

**建议**

把 `Action` 一路带下去：`topoSelfReport` 加 `Action string` →
`proto.ClusterHealthResp` / `adminsock.ClusterNodeStatus` 加 `topo_action,omitempty` →
两个渲染器 switch 类型值。

**量化**：4 个谓词 → 1 个类型字段 + 2 个薄渲染器；消除 5 处重复字符串字面量；约 20 行。
**风险：low-medium**。是 **additive** 字段（`omitempty`），沿用 `TopoReported` 已有的
"老 broker 不带此字段"惯例，无需 `ProtoVersion` 跨版本路径；但混版窗口内渲染器要保留 substring 回退。
**触碰 wire：是（仅新增字段）。**

---

### F5（medium）编排范式有**四种**；只有 join/retire 进了可 resume 的 op 机器，而 force-single 这个最危险的操作是 6 步非原子且**结构上不可重试**；`AbortOp` 承诺的"reconcile/doctor 会愈合"不存在

**证据 — 四种范式**

| 操作 | 范式 | 代码路径 | 行数 |
|---|---|---|---|
| `cluster add`（grow） | ③ CLI 侧命令式多阶段 driver（P0–P9）经签名 NATS trigger + broker 侧 dispatcher + ① op 机器 | `cmd/tether/cluster_add_drive.go` + `cluster_grow_trigger.go` + `cluster_grow_cutover.go` + `driveJoin` + `reconcile_grow_lock.go` | ~1924 |
| `cluster upgrade` | ③ 同上（无 op 机器） | `cmd/tether/cluster_upgrade_drive.go` + `cluster_upgrade.go` + `cluster_upgrade_trigger.go` + `reconcile_upgrade_lock.go` | ~1287 |
| `cluster retire` | ① 可复制 op 日志状态机 | `StartRetireOperation` + `driveRetire` + 共享 helper | ~600 |
| `cluster drain`（非 retire） | ② broker 内同步命令式 | `DrainNode` | ~120 |
| `recovery force-single --online` | ④ 两阶段 arm/commit token，**无 op 行** | `force_single_online.go:187-309` | ~230 |

`internal/cluster/operation_ops.go:17-18`：`OpKind` 只有 `join` 和 `retire`。

**范式 ③ 是正当的**，必须先说清楚：grow 的编排必须从**集群外**（运维机）发起 ——
joiner 还不在集群里、upgrade 会把 leader 自己重启掉 —— 所以它不可能是一个 broker 内的 op。
这是本质复杂度。而且 grow/upgrade 已经共享了 `startLockKeeper`
（`cmd/tether/cluster_lock_keeper.go:86`），锁续租机制**没有**重复实现。

**范式 ④ 才是问题**。`handleForceSingleCommit`（`force_single_online.go:227-309`）是一条
6 步非原子序列：`RecoverToSelfOnline` → `WaitForLeader` → `PlanSetForceSingle` →
`PlanForceSingleEpoch` → `PlanClusterNodePrune` → `deriveAndConvergeSeedsFromRoster`。
每一步失败都返回一句"re-run to retry"。**但代码自己在 `:291-293` 承认这个 re-run 是结构上被拒的**：

> "a re-run to retry a failed prune is REFUSED by the dwell gate (the node now HAS leader
> contact → CodeQuorumNotLost), so a LOUD fail here would be an unreachable dead-end"

于是 prune 失败 = 永久 ghost roster 行。这正是现网 racknerd 上那个删不掉的 pc732 的形状。
（另一半 —— force-single 完全不碰 nats.conf，把一个 clustered conf 留在 N=1 幸存者上 ——
产品后来是补上了的：`topoAdvance:1152-1166` 的 N=1 去集群化 carve-out +
`cluster reconcile nats --to-standalone` +
`cmd/tether/cluster_status_nats.go:168` 的 banner。**这条算已闭合，见反证。**）

**证据 — 承诺的愈合器不存在**

`AbortOp`（`cluster_operation_controller.go:275-290`）注释：

> "freeing the per-node active slot WITHOUT touching the substrate (the membership stays
> whatever the gates left it; **reconcile/doctor heals**)"

但一个在 `REHOME_EXPOSES` 之后被 abort 的 retire 留下：`broker_draining` marker 已抬起、
phase 可能是 `DRAINING`、exposes 已迁走。而：

- `ReconcileMembershipOnLeadership`（`clusteradmin.go:319-389`）只处理
  `PENDING`/`VOTER_ADD_FAILED` vs raft-voter 两种，**不碰 DRAINING**；
  它的第二个循环（`:371-379`）还**刻意把 DRAINING/RETIRING 排除在 INCONSISTENT 之外**。
- `PlanClusterDrainSet(node, nil)` 的调用点只有三处
  （`cluster_operation_controller.go:987,1018`、`clusterdrain.go:196,400`），全部在正常完成路径或
  手动 `AbortDrain` 里，**没有任何周期性 pass 会清一个孤儿 marker**。
- `clusteroffline.Doctor`（`internal/clusteroffline/doctor.go:48`）也不查这个。

所以运维要**两条命令**（`cluster ops abort` 然后 `cluster drain --abort`）才能撤销一次 abort，
而没有任何地方告诉他第二条。

**关于"边界非原子"的结构判断**（任务问的 Q5）：
op 机器内部的边界**不是**非原子的 —— advance-after-observe + 每 tick 从 substrate 重推导，
这是结构上正确的。非原子的是**op 机器之外**的操作。所以这不是"抽象缺失的必然结果"，
而是"抽象没有被应用到最需要它的地方"。
（注：`docs/deploy-tier-gotchas.md:574` 的 `#71` 是 drill 观测边界的取样歧义，与此不同，不要混淆。）

**建议**

1. **最小、低风险、立刻能做**：加一个 registry pass ——
   "某节点 `broker_draining` 已抬起、但该节点没有非终态 retire op、且 phase 是 VOTER" ⇒ 清 marker；
   "phase 是 DRAINING 但没有 active op" ⇒ 记 INCONSISTENT。
   这让 `AbortOp` 那句注释从"承诺"变成"事实"。
2. **force-single**：`RecoverToSelfOnline` **之前**确实不能建 op 行（那时没有 quorum，写不进 raft）——
   这是它被排除在 op 机器外的**正当理由**，必须承认。但 `RecoverToSelfOnline` 成功**之后**
   节点已是可写单 voter，后 4 步（marker / epoch / prune / seeds）完全可以是一个
   `OpKindForceSingleFinalize` 的 op 行，由 controller 驱到终态、崩溃可续、prune 失败可重试。

**量化**：(1) 约 +60 行，闭合一个已文档化但不存在的愈合器；
(2) 约 +150 行状态机 / −80 行 handler 内联序列，把一个已知的永久 ghost 来源变成可 resume。
**风险**：(1) low；(2) **high**（改的是 quorum-lost 应急路径，只能靠 simcluster deploy-tier drill 验证）。

---

### F6（medium）grow cutover 重实现了 `natsreconcile` 的 render，靠一句注释声称"byte-identical"

**证据**

- `internal/broker/cluster_grow_cutover.go:233-235` 的声明：
  > "renders the clustered nats.conf via the SAME path the reconciler uses … so the applied
  > conf is **byte-identical** to the one the reconciler DryRun-validated then withheld."
- 实现却是 `:236-274` 的一段独立 38 行组装（`natscluster.Config{...}` + `BuildMergedConf`），
  对照 `internal/natsreconcile/reconcile.go:102-143` 的同一段逻辑。
- **两者已经不同了**：cutover 强制 `MonitorListen: topoMonitorListen`（`:263-267`），
  而 reconciler 是从 live conf 里 harvest 的（`reconcile.go:126-128`）。
- `natsreconcile` **没有导出 render seam** —— render 内联在 `ReconcileOnce` 里，
  所以 cutover 想复用也复用不了。这是"抽象不足"（缺一次提取），不是"抽象过度"。

**这让什么未来改动变难/变危险**

两份 render 必须保持 byte-identical，否则 cutover 硬重启后的**下一个 reconcile tick**
会看到 `current != merged`，于是执行一次计划外的 swap + SIGHUP —— 在一台刚重启完的 broker 上。
今天它们碰巧收敛（cutover 写进去的 MonitorListen 会被下一 tick 的 harvest 读回来），
但这个耦合**没有任何测试钉住**，只有一句注释。而这正是 racknerd 那次事故所在的那个文件。

**建议**

从 `natsreconcile` 导出 `RenderDesired(in Inputs, own *natsconf.Ownership, override RenderOverride) (string, error)`，
`ReconcileOnce` 的第 4 步和 cutover 都调它；cutover 把 MonitorListen 作为显式 override 传入。
那句"byte-identical"从注释变成一次编译期调用。

**量化**：cutover −约 30 行，+1 个导出 seam；1 条无测试的文字声明 → 1 次编译期调用。
**风险：low**（纯重构，无 wire 面），但因为改的是部署面文件，合并前需跑一次相关 simcluster drill。

---

### F7（low）注释体量与"review 轮次标签"的可解码性

**证据**

- lane 6,203 行里 **2,035 行是注释（33%）**；`reconcile_passes.go` 59%、
  `reconcile_upgrade_lock.go` 60%、`reconcile_grow_lock.go` 52%。
- lane 内 **288 处** review 轮次标签（`R7a` / `R7b` / `C4-M8` / `G69 (#67 sub-face 4)` /
  `round-5 S5-15` / `mega-audit MAJ-6` / `F1` / `m1` / `D9 round-1 BLOCKER` …）。
  全 broker 包统计：`external review` 37 次、`internal review` 16、`self-review` 16、
  `round-1/2` 各 16、`round-5` 12、`round-6` 10。

**我的判断：这些注释绝大多数是负载性的，不该删。** 每一道门旁边都写着它挡住的是哪次事故 ——
对于一个"能不能安全删掉这个 gate"决定成败的系统，这是极高价值的。
`jsGateExpiryReserve` 那 17 行、`topoAdvance` 的 "WHAT IS DELIBERATELY NOT FIXED" 那段、
`reconcile_upgrade_lock.go` 的 "WHY A LEASE AND NOT A DEADLINE"，删掉任何一段都会让下一个人
把正确的设计当 bug 改掉。

**但寻址方案是债。** 标签是一套**私有索引**，指向 `docs/reviews/` 66,836 行 / 335 个文件。
新维护者拿到 `round-5 S5-15` 无法解码到具体那条 finding。而且注释与行为之间**没有任何 pin**——
以后改了代码，那 20 行 rationale 会静默变成谎言。

**建议（明确不是删除建议）**

保留全部散文，把标签词汇换成稳定锚点：`// [inv:topo-fail-closed]`、`// [inv:one-vote-veto]`，
配一页 `docs/reviews/quality-audit/invariant-index.md` 做 锚点 → 建立它的那次 review 的映射。
锚点可以被 grep、可以被测试名引用（`TestInvTopoFailClosed`），从而把"注释 ↔ 行为"钉住。

**量化**：288 处标签；**0 行删除**；+1 页索引。
**风险：low**。

---

## 反证：做得好的地方

1. **`reconcileRegistry` 调度器本身（`reconcile_registry.go:189` 行代码）是全 lane 最干净的一块。**
   锚定 deadline（`advanceLocked:293-300`，`nextDue += interval` 而不是重采样墙钟）让"R7 重写前后
   在假时钟下逐 tick 等价"可被证明；落后太多时**丢弃**错过的槽而不是补发 burst（level-triggered
   收敛补发是有害的，这一点被显式论证了）；指数退避带 shift 上界防溢出（`:305-320`）；
   **不起 goroutine、不持有 timer**，因此对仓库自建的 NumGoroutine/fd 泄漏门是隐形的。
   这不是能碰运气写出来的。

2. **`internal/natsreconcile` 是一个真正的纯 step machine。** 150 行代码，import 面
   只有 `natsconf` + `natscluster`（无 nats / 无 raft / 无 broker，注释里叫 "L-2 clean"），
   reload 与 probe 都是注入的 seam。它把"渲染—校验—原子换—reload—**真探测**"这条链
   完整表达成 7 个显式步骤，且每个 Outcome 都带类型化 Action。
   （F4 抱怨的是下游把 Action 丢了，不是这个引擎本身。）

3. **op 状态机每个不可逆副作用前都再查一次 `opStillLive`。**
   `cluster_operation_controller.go:551`（AddNonvoter 前）、`:688`（AddVoter 前）、
   `:900`（phase→DRAINING 前）、`:928`（transferLeadership 前）、`:956`（RemoveServer 前）——
   五处，且注释 `:398-402` 精确说明了为什么 driveOne 入口的重读不够（abort 可能在 tick 中途 commit）。
   这是对 TOCTOU 的系统性而非零散的处理。

4. **`growLockDecision` / `upgradeLockDecision` 被提取成不依赖 raft 的纯函数**
   （`reconcile_grow_lock.go:91`、`reconcile_upgrade_lock.go:67`），只吃 `*sql.DB` + `now` +
   一个 `isVoter` 回调，因此可以对普通 SQLite fixture 穷举测试。
   这个模式在两处一致应用，是对的做法。

5. **锁续租机制没有重复实现。** `startLockKeeper`（`cmd/tether/cluster_lock_keeper.go:86`）
   被 grow（`cluster_add_drive.go:108`）和 upgrade（`cluster_upgrade_drive.go:58`）共享；
   两处的 `renewXLease` 只是各自的 trigger 封装。lease 而非 deadline 的选择
   （"把『花太久了吗？』这个无法回答的问题，换成『还有人握着吗？』这个持有者自己在持续回答的问题"）
   是一个真正对的设计判断。

6. **步骤原语在两套 retire 之间是共享的。** 即使 F2 指出 `DrainNode` 重复了 retire 的**顺序**，
   它调用的 `migrateExposes` / `pendingRetireConvergence` / `setPhase` / `PlanClusterDrainSet`
   都是同一批函数 —— 重复的是编排，不是实现。这把 F2 的修复成本从"重写"降到"删一段"。

7. **团队证明过自己会去重。** `cluster_grow_cutover.go:280-282` 的 R16 A0 注释记录了
   `natsconf.MoveAsideJSStore` 被提取一次、给 4 个调用方（grow cutover / grow joiner reset /
   offline force-single / reconcile-nats --to-standalone）共享。F6 建议的 render 提取
   是同一个动作在同一个文件上的又一次应用，不是引入新范式。

8. **racknerd 那次事故的产品侧闭合是真的做了。** N=1 去集群化边界在
   `topoAdvance:1152-1166` 被显式建模成"故意不修的两阶段边界"并写进 drill 41；
   `filterGhostPeers:254-282` 的 fail-SAFE（读 raft config 失败时只保留 self，而不是 fail-open 到全部 peer）
   直接防的就是"把一个手工去集群化的幸存者重新集群化"；
   `cluster reconcile nats --to-standalone --confirm-single --reset-js` 是产品动词而不是运维脚本；
   `cluster_status_nats.go:168` 会打 banner 指向它。
   **这符合 simcluster 的"暴露而非弥补"铁律 —— 缺口是用产品动词补的，不是用脚本绕的。**

9. **`reconcile.go`（G.1 register 收敛）里的 `resolveReconcile` 纯分类器**（`:68-140`）
   是"先写纯函数、再证明它与线上内联实现等价（marks + audit 作为集合比较，
   `reconcile_marks_test.go`）、最后才切换"的教科书做法 —— 对一条 e2e 覆盖的热路径，
   这是把重构风险降到最低的正确顺序。

---

## 本质 vs 偶然复杂度拆解

**结论：lane 内 3,830 行代码里，约 85% 是问题域本质要求的。**

### 为什么本质复杂度这么高（这部分不可能被抽象掉）

这块代码要维持**两个独立复制域**的互相一致：

1. **raft 命令域**（hashicorp raft + 复制 SQLite），成员变更是在线 config change；
2. **NATS JetStream meta 域**，route mesh 与 JS meta 的成员关系**只由磁盘上的 `nats.conf` 定义**。

而 (2) 的施加介质是**文件系统**，NATS 对"你重载了吗"的唯一回答是
`/varz` 的 `config_load_time` 时间戳。这直接推导出：

- 必须有 `topology_generation`（一个第三方计数器，让"desired"可被复制、可被每个 broker 独立比对）；
- 必须有 applied（磁盘 conf）/ observed（活进程）两个不同的量，且 observed 必须**真探测**而非
  "signal 返回 nil"（`reconcile.go:196` 的注释就是这么写的）；
- 必须有 reload 与 hard-restart 两条路径，且 hard-restart 必须**按 roster 排名错峰**
  （`topologyRestartDue:186-200`），否则所有 voter 同时 bounce；
- 必须有一个"每个 voter 都观察到了新 gen"的门（`topoConvergedForOp`），且它必须 fail-closed；
- 必须有 N=1 去集群化这个**结构上不可自动跨越**的边界（lone-self clustered render 非法），
  于是 retire 的最后一步必须能无限期停在非终态。

join/retire 的 8 + 10 个状态，**每一个都对应一个必须被独立证明的分布式事实**：
raft config 已提交 / roster phase / catch-up barrier 已达 / stream replica 达标 /
leadership 已转移 / 拓扑代已被每个 voter 观察到 / JS meta 可放置。
把它们合并会让"崩在哪一步"不可推导，而崩溃后可推导正是 op 机器存在的全部理由。

同理，**范式 ③（CLI 侧 driver）不是懒惰**：grow 的编排主体必须在集群外，因为 joiner 还不在集群里；
upgrade 的编排主体必须在集群外，因为它会把 leader 自己重启掉。

### 那 15% 偶然复杂度是什么

| 项 | 位置 | 可净减代码行 |
|---|---|---:|
| `DrainNode` 不可达的 retire 分支（F2） | `clusterdrain.go:124-132,182-205` + `ErrStreamsNotAtTarget` | ~55 |
| grow cutover 重实现 render（F6） | `cluster_grow_cutover.go:236-274` | ~30 |
| `SetBarrier` 四处 read-modify-write 回抄（F3） | `cluster_operation_controller.go:258,532,723,754` | ~15 |
| 四个"拓扑是否收敛"谓词合并成一个类型字段（F4） | 跨 broker + cmd | ~20 |
| `reconcileTopologyOnce` 6 返回值状态穿线（F1a） | `topology_reconcile.go:81` | ~12 |
| **合计** | | **~132（占 lane 代码 3.4%）** |

另外两项是**结构风险而非行数**，修了行数几乎不变：
- 两套调度机制（F1）—— 移动职责，净 ±10 行，但把 4/13 个收敛职责从不可观测变成可观测；
- force-single 后半段进 op 机器 + drain marker 愈合 pass（F5）—— 净 **+130 行**，
  但把一个已在现网留下永久 ghost 的不可重试序列变成可 resume。

**所以"这块 6,000 行是不是臃肿"的直接回答是：不是。**
真实代码量是 3,830 行，其中约 3,250 行是两个共识域互相收敛这件事本身要求的，
可删的约 132 行（3.4%），另有约 130 行**应该被加上**去闭合两个已知的结构缺口。
剩下 2,035 行注释是这块代码最有价值的资产之一 ——
它唯一的问题是寻址方案（F7），不是体量。

---

## 附：五个集群操作的代码路径与行数对照（Q4 答案）

| 操作 | 编排范式 | 主要路径 | 行数（含注释） |
|---|---|---|---:|
| `cluster init` | offline，本地 | `internal/clusteroffline/*`（不在 lane） | 2,268 |
| `cluster add`（grow） | ③ CLI driver + ① op 机器 | `cmd/tether/cluster_add_drive.go`(886) + `cluster_add.go`(232) + `cluster_grow_trigger.go`(244) + `cluster_grow_cutover.go`(367) + `driveJoin`(~155) + `reconcile_grow_lock.go`(198) | ~2,082 |
| `cluster upgrade` | ③ CLI driver | `cmd/tether/cluster_upgrade_drive.go`(451) + `cluster_upgrade.go`(446) + `cluster_upgrade_trigger.go`(259) + `reconcile_upgrade_lock.go`(133) | ~1,289 |
| `cluster retire` | ① op 机器 | `StartRetireOperation`(51) + `driveRetire`(175) + 共享 helper(~190) + `migrateExposes`/`pendingRetireConvergence`(~210) | ~626 |
| `cluster drain`（非 retire） | ② broker 内同步 | `DrainNode`(122，其中 ~55 不可达) + `AbortDrain`(17) | ~139 |
| `recovery force-single --online` | ④ arm/commit token | `force_single_online.go`(320) | 320 |

**共享的编排骨架有多少？**
"前置检查 → 广播 → 等 quorum → 写配置 → reload → 校验 → 回滚"这条骨架里，
**已被共享的**是：锁续租（`startLockKeeper`）、签名 trigger 的 verify+dispatch 形状
（`cluster_grow_trigger.go` 与 `cluster_upgrade_trigger.go` 是刻意的镜像，各自 ~170 行代码，
但请求类型/op 集合不同，抽公共骨架的收益低于抽象成本 —— 这个不合并我认为是对的）、
JS store 移置（`natsconf.MoveAsideJSStore`）、step 原语（`migrateExposes` 等）。
**未被共享的**是：op 梯子本身（join 与 retire 各写一个 switch —— 这是对的，它们的状态集不同）、
以及 F2 指出的 retire 顺序被写了两遍（这是不对的）。

**"能否抽出统一的 `clusterOperation` 骨架"**：不建议。
join 与 retire 的 `driveX` 已经共享了 `transition` / `recordOpError` / `blockAfterAttempts` /
`opStillLive` / `readSubstrate` / `topoAdvance` / `toNatsRolledOut` 这 7 个真正共通的部件，
剩下的 switch 是状态图本身。再往上抽一层泛型骨架，只会把"哪个状态做什么"这个唯一重要的信息
埋进一层间接。**该做的不是加骨架，是把还没进这个骨架的操作（F5）挪进来。**

---

## 回滚 / 中断安全（Q5 答案，摘要）

- **op 机器内：没有回滚，是 forward-only + 有界 BLOCKED —— 这是正确的设计选择。**
  失败路径统一走 `blockAfterAttempts`（`opMaxAttempts=5`）→ `OpStateBlocked`，
  一个 `cluster ops confirm/abort` 能作用的**响亮**状态。
  梯子上每个可能永久等待的门都被显式加了界：`boundCatchingUp`(#47)、
  `boundRehomeConvergence`(self-review high)、`topoAdvance`(#45)、`jsPlacementAdvance`(G69)。
  这四处都记录了"无界 fail-closed 门 + 不可靠信号 = 成员面永久 wedge"这条教训。**做得对。**
- **`AbortOp` 不做补偿动作，把 substrate 留在原地 —— 设计上说得通，但它委托的愈合器不存在（F5）。**
- **op 机器外：force-single 的 6 步序列非原子且第 5 步结构上不可重试（F5）**，
  这是 lane 内唯一一处"失败即留下需要人工介入的永久残留"。

---

*（本报告为只读审计产物，未修改任何实现代码。）*
