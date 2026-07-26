# L01 — `*Broker` God Object 解剖（结构性质量审计 · 2026-07-25）

> lane key: `broker-god-object`
> 范围：`internal/broker/` 全部 65 个生产文件（21,618 行 span）
> 定位：**不是 bug hunt**。找的是冗余、重复、过度/不足抽象、职责错位、演进阻力。
> 只读审计，未修改任何实现代码。

---

## 结论

**`*Broker` 是"命名空间型 God Object"，不是"共享可变状态型 God Object"。** 263 个方法里只有 **6 个**碰 ≥6 个字段，只有 `Run` 一个碰 ≥10 个；**104 个方法（40%、2,555 行）除 `cfg.DB` / `cfg.Logger` / `cfg.Now` 外不碰任何 broker 状态**，11 个方法根本不引用 receiver。也就是说：这个类型不是"一坨互相纠缠的可变状态"，而是"一个把 263 个几乎互不耦合的函数挂在同一个前缀下的命名空间"。经典 God Object 的两大症状——锁粒度粗化、改一处牵动全身——**在这里都不成立**（8 把 mutex 每把只护 1–2 个字段、无嵌套、无全局 `b.mu`；导出面只有 16 个方法、包外 `&Broker{}` 构造点为 **0**）。

所以真正的债**不在 struct 大小**，而集中在三条明确的缝：

1. **加一个 raft 写动词 = 5 处散弹式手术**，其中两份 plan 闭包必须逐字语义等价，靠人眼维持；
2. **`C.1 §6` ingress 准入不变量靠 13 处复制粘贴维持**，历史上已经漏过一次（register），并且已经长出两个签名互不兼容的半成品门；
3. **`b.cfg.DB` 一个字段两种语义**（单机=可写池 / 集群=只读池），89 个方法读它，三个 handle 全是 `*sql.DB`，编译器分不出来——已经产生过一次运行期 bug。

**bloat 打分：4 / 10。** 理由：注释占 29.3%（6,336 / 21,618），`Broker` struct 231 行 span 只有 **47 行代码**（67% 是文档）、`Run` 529 行 span 只有 **338 行代码**；包内 60%（13,080 行）已经不在 `Broker` 上，而在 `ClusterAdmin` / `AuditPublisher` / `AlertReconciler` / `transferTracker` / `reconcileRegistry` 这 5 次**成功的**组件抽取里；`reconcile_registry.go` 是教科书级的抽取（带不变量文档 + 假时钟等价性证明）。这不是屎山，是**抽取做了一半就停了**——而且停的位置有迹可循（见 Finding 5：文件按 phase 切，不按职责切，git 可证）。

**可净删的行数很小（约 500–900 行 / 占本包代码的 4–6%）。这条 lane 的债不是体积债，是形状债。** 任何以"砍行数"为目标的重构在这里都会失败；正确的目标是把上面三条缝各收口成一个点。

---

## 范围与方法

- **读**：`broker.go`（全）、`clusterwrite.go`（全）、`cluster_forward.go` dispatch 段、`clusterstatus.go` StatusReport 段、`cluster_operation_controller.go` 大纲、`clusteradmin.go` struct、`expose.go` handleExposeReq、`proxy.go`、`transfer.go` gate 段、`reconcile_registry.go`、`reconcile_passes.go`、`upgrade.go`、`home.go`/`homes.go`/`js_health.go`/`alert_id.go`/`testhooks.go`。
- **AST 分析**（一次性只读脚本，`go/parser`，跑在 `/home/weiland/.claude/jobs/.../tmp/`，未写进仓库）：统计 `Broker` 的字段数、每个方法的**字段足迹**（`b.<field>` 与 `b.cfg.<field>` 去重集合）、方法行数、按 receiver 归类的方法数、按文件归类。
- **git 考古**：`git log --diff-filter=A` 取每个生产文件的**首次引入 commit**，用来判定文件划分逻辑。
- **注释密度**：逐行分类 code / comment / blank（区分块注释）。
- **未运行**任何测试（按约束）；只跑 `go build ./...`（通过）。

关键量化基线（本包，生产文件）：

| 指标 | 数值 |
|---|---|
| span 行数 / 实际代码行 / 注释行 / 空行 | 21,618 / **14,051** / 6,336 / 1,231 |
| `Broker` 字段数 | **45** |
| `Config` 字段数 | 43 |
| `Broker` 方法数 / 方法 span 总行 | **263 / 8,538** |
| 其余（`ClusterAdmin` 67 / `clusterAdminBackend` 17 / `AuditPublisher` 15 / 包级 func 153 / …） | 13,080 行 |
| `Broker` **导出**方法 | 16（其中 6 个是 `*ForTest` 测试访问器） |
| 包外 `broker.Broker{}` 构造点 | **0** |
| 测试内 `&Broker{...}` 字面量 | 126 处 / 41 个文件 |

`Broker` 方法的**字段耦合分布**（这是全审计最关键的一张表）：

| 桶 | 方法数 | span 行 |
|---|---|---|
| 完全不引用 receiver（伪方法） | **11** | 121 |
| 引用 receiver 但不碰任何字段（只调其它方法） | 34 | 362 |
| 只碰 `cfg` | **79** | 2,537 |
| 碰 1–2 个字段 | 100 | 2,419 |
| 碰 3–5 个字段 | 33 | 2,117 |
| 碰 ≥6 个字段 | **6** | 982 |
| 合计 | 263 | 8,538 |

`Broker` **字段的**使用分布（长尾极长）：`cfg` 166 个方法 / 42 个文件；`cl` 47；`clusterMode` 36；`selfID` 26；`js` 18；`nc` 13；`tunnelSrv` 9；`transfers` 9；**其余 37 个字段全部 ≤4 个方法，且 30 个被限制在 1–2 个文件内**。

---

## Findings

### F1 [high] 一个 raft 写动词 = 5 处散弹式手术，其中两份 plan 闭包必须逐字语义等价，靠人眼维持

**证据**

- `internal/broker/cluster_forward.go:507-671` — `dispatchForward` 是 **17 个 `case` 的 switch，171 行 span / 155 行代码、注释只占 9%**（全包注释率 29%），是本包密度最高的函数之一。
- `internal/broker/clusterwrite.go:694-997` — 10 个形状完全相同的路由方法（`createSession` / `allocatePort` / `registerNode` / `recordProc` / `freePortAllocation` / `revokePortAllocation` / `markProcExited` / `tombstoneSession` / `dropSession` / `evictNode`），模板一律是：

  ```
  if !b.clusterMode { return <domain>.<Direct>(b.cfg.DB, …) }
  payload, err := json.Marshal(<X>Payload{…}); if err != nil { return err }
  return b.proposeOrForward(Verb<X>, "", payload, func(db *sql.DB) (*cluster.Command, error) {
      return <domain>.Plan<X>(db, …)
  })
  ```

- 于是新增一个动词要同时改 **5 个地方**：`Verb<X>` 常量（`cluster_forward.go:53-122`）、`<X>Payload` 类型、领域包里的 `Plan<X>`、`clusterwrite.go` 的路由方法、`dispatchForward` 的 `case`。
- **最危险的一点**：同一个 `Plan<X>` 闭包被写了**两遍**——leader 本地路径一份（`clusterwrite.go`），follower 转发后 leader 侧一份（`dispatchForward`）。两份必须语义等价，否则同一个逻辑写操作在"打到 leader"和"打到 follower"两种情况下行为不同。**已有一处只是巧合正确**：
  - `clusterwrite.go:833-846` `freePortAllocation` 把**完整的** `port.Allocation` 传给 `PlanFreeAllocation`；
  - `cluster_forward.go:592-604` 的 `VerbPortFreeAllocation` / `VerbPortRevoke` 分支只用 5 个字段 `{Port, SID, NID, Name, TokenHash}` **重建**一个 `port.Allocation`，其余字段（`Epoch`、`HomeBroker`、`RebuildOnFailure` …）全是零值；
  - 今天不出问题，纯粹因为 `internal/port/plan.go:45-70` 的 `planAllocationStateChange` 恰好只读那 5 个字段。**这条不变量没有任何测试或类型在守。**

**为什么是债（它让什么具体的未来改动变难/变危险）**
让 `PlanFreeAllocation` 多读一个字段（例如加一条 "只在 epoch 匹配时才 free" 的 fencing 条件——这正是本项目已经在 `PlanAllocate` 里做过的事）成为一个**静默的、只在 follower 路径触发的**正确性回归：leader 直连的所有测试全绿，只有"ctl 打到 follower + 该字段非零"的组合才错。仓库现有的 e2e 矩阵按 phase 组织、大量走 leader 路径，结构上难以稳定命中这个组合。

**建议**
把 17 个动词收成一张**表**，让"注册一个动词"变成一处：

```go
type writeVerb struct {
    verb    string
    decode  func([]byte) (any, error)
    plan    func(db *sql.DB, arg any, now time.Time) (*cluster.Command, error)
    allowReqID bool
}
var writeVerbs = map[string]writeVerb{ … }
```

`dispatchForward` 退化成 `decode → plan → Propose`（约 25 行）；`clusterwrite.go` 的 10 个路由方法改成调用同一个泛型 helper `routeWrite[T](b, verb, arg, direct)`，**plan 闭包只剩一份**，leader/follower 路径按构造即等价。**verb 字符串与 payload JSON 必须逐字保持不变**（滚动 broker 升级期间 broker↔broker apply 总线是跨版本 wire，见 G5）。

**量化 / 风险**：净减约 **120 行**；把"两份闭包必须等价"从人肉不变量变成类型不变量。changeRisk = **medium**（触碰所有写路径，但不改 wire 字节，且 `internal/broker` 有 19,895 行包内测试托底）。触碰 wire：**是**（broker↔broker apply 总线；重构必须保持 verb/payload 字节兼容）。

---

### F2 [high] `C.1 §6` ingress 准入不变量靠 13 处复制粘贴维持，历史上已经漏过一次，且已长出两个互不兼容的半成品门

**证据**

- `session.IsActive` 在生产代码里有 **13 处 ingress 调用点**：`broker.go:1327`、`exec.go:49/210/276`、`run.go:41/134`、`expose.go:178/386`、`upgrade.go:47`、`proxy.go:327/553`、`transfer.go:969/1048`。
- 配套的链条同样复读：`auth.FingerprintFromActor` **19 处**、`session.IsMember` **10 处**、`node.LookupStatus` **7 处**、`proto.ParseCmdBy` **9 处**。
- **已经漏过一次**——`broker.go:1320-1326` 的注释是自陈：
  > "C.1 §6 — every session-scoped ingress must reject DELETING / missing sessions before mutating any state. … **Without this**, a tombstoned session could get a fresh `nodes` row, a forced ONLINE transition, an `agent_registered` sys.event, and reconcile side effects while H.3 cleanup is supposed to be the only writer."

  也就是说 register 这条 ingress 曾经**没有**这个门，是后补的。
- **抽象开了个头就停了**：包里已经有**两个**门 helper，签名和回错方式互不兼容——
  - `transfer.go:968-997` `transferGate(sid, fp, nid) string`：**返回** code，不回消息，`nid==""` 表示跳过 node 检查；
  - `proxy.go:552-573` `proxyActiveOwnerGate(sid, fp, actor, verb string, msg *nats.Msg) string`：**自己回消息 + 自己发 audit**，检查的是 `IsOwner` 而不是 `IsMember`。
  - 剩下 6 个 handler（`handleExecReq`、`handlePsReq`、`handleNodeListReq`、`handleRunReq`、`handleKillReq`、`handleExposeReq`、`handleExposeRmReq`、`handleUpgradeReq`）**仍然逐行手写**这条链。`handleExposeReq`（`expose.go:159-345`，187 行 span / 152 行代码）里 1–6 步全是这条链。

**为什么是债**
每加一个 session-scoped 动词，作者都要**凭记忆**复读 6 段门（subject 解析 → actor→fp → IsActive → IsMember/IsOwner → LookupStatus → 各自的 audit）。漏掉任何一段：**不会编译失败，不会被现有测试抓到**（除非专门为新动词写一条 tombstone 竞态测试），只会在 `session rm` 的 DELETING 窗口里写脏数据——正是 `broker.go:1320` 注释描述的那个已发生过的故障类。这条不变量在 `docs/architecture.md` 里是**硬约束**，而它的执行方式是复制粘贴。

**建议**
收成一个**必经的 admission 类型**，而不是又一个可选 helper：

```go
type ingress struct{ SID, NID, Actor, FP string; Msg *nats.Msg }
// admit 一次性做：ParseCmdBy → FingerprintFromActor → IsActive → 权限（member|owner）→ 可选 node-online
func (b *Broker) admit(msg *nats.Msg, want verbSpec) (*ingress, bool)
```

每个 handler 的第一行变成 `in, ok := b.admit(msg, specExpose); if !ok { return }`。把 `transferGate` / `proxyActiveOwnerGate` 合并进去（`want` 里带 `requireOwner` / `requireNode` 两个 bool 即可覆盖现有全部差异）。

**量化 / 风险**：13 处 ingress × 约 25 行 → 1 个 `admit`（约 60 行）+ 13 × 约 5 行，**净减约 200 行**；更重要的是把"新动词是否装了门"变成签名层面的强制。changeRisk = **medium**（错误码字符串是 ctl 可见契约，重构必须逐个动词保持 code 字符串不变——项目已有 `feedback-contract-change-sweep` 的教训）。触碰 wire/不变量：**是**（错误 code 是 ctl↔broker 契约的一部分）。

---

### F3 [high] `b.cfg.DB` 一个字段两种语义（单机可写池 / 集群只读池），89 个方法读它，三个 handle 全是 `*sql.DB`，编译器分不出来

**证据**

- `broker.go:818` — `Run` 里**唯一**一处 `b.cfg.*` 的运行期赋值：`b.cfg.DB = cl.node.RODB()`。单机模式下 `cfg.DB` 是 storage.Open 出来的**可读写**池；集群模式下同一个字段变成 cluster.Node 的**只读**池。
- **89 个方法**读 `b.cfg.DB`（AST 统计），分布在 42 个文件里。
- 补救办法是又加了一个访问器而不是消歧：`clusterwrite.go:583-589` `livenessDB()` —— 集群模式返回 `b.cl.node.DB()`（FSM 写池），单机返回 `b.cfg.DB`。于是同时存在**三个** `*sql.DB`：`b.cfg.DB`（读）、`b.livenessDB()`（liveness 列直写）、`b.cl.node` 的 Propose 路径（复制列）。三者类型完全相同。
- **已经产生过一次运行期 bug**——`clusterwrite.go:961-963` 的注释是自陈：
  > "D9 round-2 BLOCKER: route `admin evict` through raft (**else the direct tx hits the RODB handle and fails**)."

  即：有人在集群模式下用 `b.cfg.DB` 做了写，运行期才炸。
- 同类补丁还有 `broker.go:660-665`：`New` 里要靠一条运行期检查（"cluster mode must be constructed with a nil cfg.DB"）来防止**第二个可写池**被打开。

**为什么是债**
每一次新增数据访问，作者都要先回答一个**不在类型里、只在注释里**的问题："这是复制列还是 liveness 列？这条路径在集群模式下会不会执行？" 答错的代价是运行期 `attempt to write a readonly database`，而且**只在集群模式**触发——hermetic 单测大量用单机模式的零值 `Broker` 字面量（包内 126 处），结构上不覆盖这个分支。这直接抬高了"给 broker 加任何一张新表 / 新写路径"的风险，而 HA 之后的每个叶子增量几乎都要加。

**建议**
用类型消歧，一次性把它变成编译期问题：

```go
type readDB struct{ db *sql.DB }            // 只暴露 Query / QueryRow
func (b *Broker) read() readDB              // 替代 89 处 b.cfg.DB
func (b *Broker) liveness() *sql.DB         // 保留，语义收窄到 liveness 列
```

`Config.DB` 在 `Run` 之后不再被任何业务代码直接读。这样"在集群模式下误写"从运行期错误变成**编译错误**。

**量化 / 风险**：89 个调用点的机械替换，**不减行**（约 +15 行包装类型）；换来的是一整类 bug 的消失。changeRisk = **low**（纯机械、编译器全程护航、无 wire 影响）。触碰 wire/不变量：**否**（反而是把 architecture §3.5 的 liveness/replicated 列分离从注释变成类型）。

---

### F4 [medium] `*Broker` 是命名空间而非对象：45 字段 / 263 方法，但 40% 的方法只需要 `(db, logger, clock)`

**证据**

- 45 个字段中，**30 个**被 ≤3 个方法触碰且限制在 ≤2 个文件内（AST 全量统计）。典型：`proxyGen`/`proxyGenMu`/`proxyOpMu`/`proxyDwell`/`proxyEvtCounts` 只出现在 `proxy*.go`；`transfers`/`xferReapMinAge`/`xferReplicasFn`/`transferAudit{Sink,ForwardSync,WG,Draining,Mu}`/`xferUnreapableBuckets` 只出现在 `transfer*.go` + `xfer_*.go`；`homeDeliveryState`/`homeDeliveryOnce` 只出现在 `home_delivery.go`。
- **104 个方法（2,555 行）**不碰任何 broker 可变字段，只用 `cfg.DB` / `cfg.Logger` / `cfg.Now`。
- **11 个方法（121 行）根本不引用 receiver**——它们今天就是纯函数，只是挂在 `Broker` 上：`expose.go:359` `replyExposeErr`、`expose.go:472` `replyExposeRmErr`、`broker.go:1477` `replyErr`、`topology_reconcile.go:310` `probeNatsConfigLoadTime`、`proxy_cluster_wire.go:38` `proxyDegradedCode`、`upgrade.go:148` `replyUpgradeErr`、`cluster_grow_cutover.go:351` `probeNatsClusterName`、`xfer_inflight.go:132` `writeLedgerRecord`、`run.go:107` `replyRunFailed`、`run.go:189` `replyKillFailed`、`exec.go:125` `replyExecErr`。

**为什么是债**
不是"锁竞争"或"改一处炸全身"——那两条**在这里都不成立**（见反证）。真实成本是**导航**与**测试构造**：
- 要理解 `proxy` 子系统，必须在一个 45 字段的 struct 里辨认哪 7 个字段属于它（靠读注释，没有类型边界）；
- 包内测试有 **126 处 `&Broker{...}` 字面量**，每一处都要作者判断"这个测试要填哪几个字段才够"。新增字段时，"哪些测试字面量需要跟着填"没有任何工具能回答——只能靠跑测试看它挂不挂。

**具体切分方案（可照做）**

| 组件 | 从 `Broker` 拿走的字段 | 方法数 / span 行 | 依赖（构造参数） |
|---|---|---|---|
| `proxyController` | `proxyGen, proxyGenMu, proxyOpMu, proxyDwell, proxyEvtCounts, rehomeEvt, autoRebalanceArm`（7） | 约 56 / 约 1,460 | `readDB, logger, now, nc, tunnelSrv, writeGateway` |
| `transferService` | `transfers, xferReapMinAge, xferReplicasFn, transferAuditSink, transferAuditForwardSync, transferAuditWG, transferAuditDraining, transferAuditMu, xferUnreapableBuckets`（9） | 约 46 / 约 1,518 | `readDB, logger, now, js, selfID, forwarder` |
| `homeDirector` | `homeDeliveryState, homeDeliveryOnce, tunnelCert`（3；`selfID`/`tunnelSrv` 共享只读） | 约 34 / 约 600 | `readDB, logger, now, nc, selfID` |
| `writeGateway`（见 F1） | `cl, clusterMode` 的写侧用法 | 约 13 / 约 330 | 两个实现：`directGateway{db, now}` / `raftGateway{node, forwarder, now}` |
| `metricsCache` | `lastObserve, lastObserveMu, lastReplicaActual, lastReplicaTarget, lastReplicaMu, jsUnavail`（6） | 约 6 / 约 150 | 无 |

**切完后 `Broker` 还剩什么**：`cfg, nc, js, admin, runCtx, bootAt, reconcilers, clusterMode, cl, reExecRequested, reloadTrigger, clusterAdminMu, clusterAdminHandle, manifestCache, manifestMu, rosterStaleMu, rosterStaleWarned` 约 **17 个字段**，加上 ingress handlers（register/heartbeat/session/exec/run/ps/expose/upgrade）与 `Run` 的启动编排，约 **100 个方法 / 3,000 行**。那还是个大类型，但它有了一个能一句话说清的职责："NATS 订阅入口 + 启动编排 + 把请求分派给上面 5 个组件"。

组件间接口保持**贫瘠**（只暴露 handler 与 reconcile pass 需要的动词），组件之间**不互相持有**——它们今天就不互相依赖（字段足迹表证明了这点），所以拆分不会产生新的循环。

**量化 / 风险**：**不减行**（约 +150 行构造/接线），把 263 → 约 100 个方法、45 → 约 17 个字段。changeRisk = **low**：包外 `broker.Broker{}` 构造点为 **0**，导出方法仅 16 个，blast radius 完全落在包内的 126 处测试字面量上，且全部是机械改写。触碰 wire/不变量：**否**。

> **诚实的取舍**：这一条**不紧急**。因为方法-字段耦合极稀疏（263 个里只有 6 个碰 ≥6 字段），今天的痛感主要是阅读成本而非事故成本。建议把它当作 F1/F2/F3 落地时**顺手完成**的重组，而不是单独立项。

---

### F5 [medium] 65 个生产文件按"哪个 phase / review 加的"切，不按职责切——git 可证

**证据**（`git log --diff-filter=A` 每个文件的首次引入 commit）

- 一次 commit 同时新建的文件 = 一个 phase 的产出，而非一个内聚的概念：
  - `D7` 一次建 `clusteradmin.go` / `clusterdrain.go` / `clusterstatus.go`；
  - `D9` 一次建 `clusterwrite.go` / `cutover.go` / `observability.go`；
  - `v2-usability C1–C8` 一次建 **10 个**文件（`cluster_manifest.go`、`cluster_operation_controller.go`、`homes.go`、`proxy_cluster.go`、`proxy_cluster_wire.go`、`proxy_rebalance.go`、`proxy_reconcile.go`、`rehome_events.go`、`roster_stale.go`、`topology_reconcile.go`）；
  - `R16/G67/G69` 一次建 `xfer_inflight.go` / `xfer_provision.go`。
- 后果一：**单文件多职责**。`clusterwrite.go`（1,010 行）实际装了 4 件不相干的事——① 集群早/晚期接线与有序关闭（`wireClusterEarly/Late`、`clusterShutdownOrdered`）；② tunnel 证书加载与热轮换（`loadStableTunnelCert`、`prepareTunnelCertRotate`、`tunnelCertMatchesPinned`）；③ JS 放置探针与 reaper 闸（`jsPlaceableFrom`、`reaperMayDelete`、`reaperCaughtUp`）；④ **真正内聚的那一块**——全部授权写路由（`proposeOrForward` + 10 个 mutator，`:666-997`）。文件名只描述了 ④。
- 后果二：**职责放错文件**。`clusterdrain.go` 里放着 `RotateTunnelCert`（`:410`）、`SetRaftAddr`（`:513`）、`SetNatsRoute`（`:578`）——和"drain"毫无关系，只是同一个 D7 commit 的产物。`clusterstatus.go` 里放着 **adminsock 请求分发器** `clusterAdminBackend`（`:573-901`，17 个方法，含 `handleDrain` / `handleAdd`）——把"状态报告渲染"和"admin 请求路由"塞进同一个文件。
- 后果三：**名字碰撞**。`home.go`(180) 是 D6 seam attach + `selfNodeID`；`homes.go`(120) 是 `cluster status --homes` 的报告构建器。差一个字母，两件事。同类还有 `proxy_cluster.go` vs `proxy_cluster_wire.go`、`clusterops.go` vs `clusteradmin.go` vs `cluster_operation_controller.go`、`alert_{forward,webhook,reconcile,id}.go` + `alertadmin.go`。
- 后果四：**碎片**。3 个 <35 行的文件——`testhooks.go`(11)、`alert_id.go`(19，只有一个 `newAlertID`)、`js_health.go`(32，只有一个 `classifyJSUnavailable`)；20 个 <150 行的文件。

**为什么是债**
"我要改 X，该打开哪个文件"这个问题在这个包里**答不出来**——文件名编码的是**来历**而不是**内容**。具体后果：新增功能时的默认动作变成"再开一个新文件"（因为找不到该并进去的地方），于是文件数继续增长、每个文件更薄、导航更难——这是一个**自我强化的循环**，git 记录显示它已经跑了 4 轮（D7 → D9 → C1-C8 → R16）。

**建议**
不必大搬家。做三件低风险的事：
1. 把 `clusterwrite.go` 的 ④（`:666-997`，约 330 行）单独切成 `writegateway.go`——它同时是 F1 的落点；
2. 把 `clusterstatus.go` 的 `clusterAdminBackend`（`:573-901`，约 330 行）切成 `clusteradmin_backend.go`；
3. 把 3 个 <35 行的文件并入它们唯一的调用者所在文件（`alert_id.go` → `alert_reconcile.go`，`js_health.go` → `alert_reconcile.go`，`testhooks.go` 保留——它是刻意的包级测试缝，有先例 `internal/cluster/testhooks.go`）。

并在 `CLAUDE.md` §5 补一条：**新代码优先并入职责匹配的既有文件；新建文件必须能用一个名词短语说清它的职责，不得以 phase / review 轮次命名。**

**量化 / 风险**：纯移动，**不减行**；文件数 65 → 约 62。changeRisk = **low**（`git mv` 级别）。触碰 wire/不变量：**否**。

---

### F6 [medium] 22 个 nil-默认的函数 seam 靠一个 166 行的 `wireClusterLate` 手工接线；漏接 = 静默降级，只能靠"一个 seam 一个反漏接测试"来防

**证据**

- `clusteradmin.go:47-175` — `ClusterAdmin` 有 **13 个 `func` 类型字段**，全部 nil-默认、nil 即静默跳过：`healthPoll`、`homesReport`、`streamObserve`、`topoSelf`、`caughtUpFn`、`streamsReadyFn`、`jsPlaceableFn`、`homeAppliedFn`、`homeDeliverFn`、`prepareTunnelCertRotate`、`emitEvent`、`jsUnavail`、`now`。
- `Broker` 上另有 6 个：`transferAuditSink`、`transferAuditForwardSync`、`xferReplicasFn`、`alertSink`、`clusterAdminHandle`、`reloadTrigger`。`clusterAdminBackend` 上还有 3 个（`rebalanceProxy`、`fsArm`、`requestReExec`，`broker.go:1200-1203` 通过类型断言塞进去）。
- `clusterwrite.go:284-449` — `wireClusterLate` 一口气接 14 个 seam，**166 行 span / 115 行代码**。
- 代码自陈这个模式已经出过两次事故：
  - `clusteradmin.go:100-104`：`nil ⇒ 收敛等待被 SKIP`（本包大量单测构造裸 `ClusterAdmin`）。"That nil-skip is **the classic half-wiring trap this project has already been bitten by twice**, so it is guarded from both ends: `TestWireClusterLateWiresHomeConvergence` pins the wiring, and `TestDrainRefusesRcZeroWhenDataPlaneStale` pins the behaviour."
  - `clusterwrite.go:420-423` 同样挂着一条 "anti-half-wiring guard" 注释。
- 也就是说：**防线是"每个 seam 手写一条专门的接线测试"**——一个 O(seam 数) 的人肉义务。

**为什么是债**
"nil = 跳过"让**漏接与合法的单机模式无法区分**。新增一个 seam 时，忘记在 `wireClusterLate` 里接上：编译通过、单测通过（单测本来就构造裸 `ClusterAdmin`）、单机 e2e 通过（本来就该 nil）、集群 e2e 里表现为"某个门不生效"——`cluster drain` 在数据面未收敛时返回 rc=0，正是 `clusteradmin.go:96-99` 描述的那个契约破坏。这个 failure mode **只在集群实跑**暴露，而集群实跑是最贵的一层（simcluster drill，2–10 分钟/次）。

**建议**
把"生产必需"和"单测可省"两类 seam 分开：
- 生产必需的（`caughtUpFn`、`streamsReadyFn`、`homeAppliedFn`、`homeDeliverFn`、`jsPlaceableFn`、`healthPoll`、`streamObserve`、`topoSelf`）移进一个 **`ClusterAdminDeps` 结构体，作为 `NewClusterAdmin` 的必填参数**——漏接变成编译错误；
- 单测继续用 `NewClusterAdminForTest(node, logger, ClusterAdminDeps{…部分填充…})`，缺省用**显式的、名字自陈的** stub（`alwaysCaughtUp` / `neverPlaceable`）而不是 nil；
- 真正可选的（`emitEvent`、`prepareTunnelCertRotate`、webhook）留 nil 语义，但注释里注明"可选"是产品语义而非接线状态。

这样能删掉一批纯粹为了钉接线而存在的测试（`TestWireClusterLateWires*` 系列）。

**量化 / 风险**：`wireClusterLate` 从 115 行代码降到约 60；删掉约 8 条纯接线测试（估计 150–250 行测试）。changeRisk = **medium**（`NewClusterAdmin` 签名变更会波及包内所有构造点，但**全部在包内**）。触碰 wire/不变量：**否**。

---

### F7 [medium] `StatusReport` 是"每轮审查往末尾追加一段"的累积函数：216 行代码，尾部 3 个 banner 追加块各来自一个不同的审查轮次，带手搓分隔符与 dedup

**证据**

- `clusterstatus.go:127-401` — `StatusReport`，**275 行 span / 216 行代码**（注释仅 19%），是本包**代码密度最高的单个函数**。
- 尾部结构（`:332-381`）是三段来历不同的 banner 追加，每段都自带 `if rep.Banner != "" { rep.Banner += " " }`：
  - `:332` `computeHealth` 产出基础 banner；
  - `:352-369` **G2 #20** 的 DATA-PLANE-DEGRADED（force-single + clustered conf）；
  - `:370-376` **External-review m2 / round2 M1** 的 sustained-503 —— 注释里明写 "Emit ONE banner (**dedup**)"，说明这两段**已经撞过一次**；
  - `:373-381` **B5 OPS#7** 的证书过期 advisory。
- 同一函数里还混着 4 个抽象层次：SQL 行扫描（`:137-172`）、statfs 磁盘探测（`:181-188`）、raft 配置读取（`:193-200`）、per-node 结构体拼装、健康判定、文案渲染。

**为什么是债**
再加第 4 条 advisory 时，作者必须先读懂**前三条之间的 dedup 交互**（第二条和第三条是 `if A { …A文案… } else { …B文案… }` 的互斥关系，第四条是无条件追加）——这个交互没有名字、没有测试直接覆盖"四条同时成立"的组合。这正是 `feedback-contract-change-sweep` 描述的那类缺陷：同一个缺陷被反复发现，因为它的成因（无收口的追加点）从未被修掉。

**建议**
提取 `type bannerBuilder []string`（`add(cond bool, text string)` + `String()` 负责分隔符），把三段追加改成三次 `bb.add(...)`；把 `:137-188` 的数据采集抽成 `readStatusSubstrate() (statusSubstrate, error)`，`StatusReport` 退化为 `substrate → assemble → health → banner → verdict` 五步。

**量化 / 风险**：`StatusReport` 216 → 约 120 行代码；新增 advisory 从"读懂三段交互"变成一行。changeRisk = **low**（banner 文案字节需逐条保持，包内已有 status 文案测试）。触碰 wire/不变量：**否**（banner 是运维可见文案，重构须保持字节不变）。

---

### F8 [low] `Broker.Run` 是 20 段启动 DAG，**不是垃圾桶**——但混进了约 55 行诊断文案和 3 段几乎逐字相同的可选 HTTP listener 装配

**证据 / 逐段拆解**（`broker.go:762-1290`，529 行 span / **338 行代码** / 166 行注释）

| # | 行 | 职责 |
|---|---|---|
| 1 | 763-772 | 记录 `bootAt`（必须早于任何 goroutine） |
| 2 | 773-795 | `${ClusterDataDir}/tether.lock` 全进程 flock（与离线恢复工具互锁） |
| 3 | 796-804 | 单机模式可选 stable tunnel cert |
| 4 | 806-829 | 构造 `cluster.Node`、把读句柄指到 `RODB`、`wireClusterEarly` |
| 5 | 831-848 | NATS connect + drain-before-clear 的 defer |
| 6 | 850-861 | forwarder（必须早于 authcallout）+ `installAuthCallout` |
| 7 | 863-877 | tunnel server |
| 8 | 879-902 | subhttp listener |
| 9 | 904-920 | metrics listener |
| 10 | 922-937 | cluster manifest listener |
| 11 | 939-1037 | register/heartbeat + 24 条订阅表（含 cluster 模式的 queue-vs-broadcast 判定） |
| 12 | 1039-1050 | Flush + `ReadyCh` |
| 13 | 1052-1086 | JetStream 探测 + events stream + boot 期 history/OBJ_xfer 收敛 |
| 14 | 1088-1142 | `wireClusterLate` + **JS 不可用时的三段长诊断文案** |
| 15 | 1144-1150 | `tetherd_restarted` 事件 + disk monitor |
| 16 | 1155-1160 | 构造 reconcile registry（必须早于 admin socket 的 accept goroutine） |
| 17 | 1163-1206 | admin socket + backend 组装（含 3 个 seam 的类型断言注入） |
| 18 | 1208-1224 | `finalizeStrandedXfers`（**必须晚于** admin socket——注释标了 "PLACEMENT IS LOAD-BEARING"） |
| 19 | 1226-1245 | boot 期 node states / ports / tunnel sessions 收敛 |
| 20 | 1247-1289 | registry 起点 + 单一驱动 ticker 主循环 |

**判定：这是必要的启动编排，不是垃圾桶。** 顺序真的是承重的，且有三处显式的 "PLACEMENT IS LOAD-BEARING" / "MUST run before/after" 注释在守（`:1208-1216` 记录了一次真实的部署层故障：这段跑在 admin socket 之前会让 `cluster add` 在 start-joiner 边界 HALT）。把它拆成 20 个小函数只会把"顺序"这条唯一重要的信息藏起来。

**但有两处是纯粹的噪声**：
- `:1088-1142` 约 55 行是**诊断文案**（三段长 `fmt.Errorf`，讲 N=1 clustered-JS / force-single 被踢出 / routes mesh 未成型的差异诊断），与启动编排无关。项目**已经做对过一次**——`n1ClusteredJetStreamFatal()`（`broker.go:748-760`）就是从 `Run` 里抽出来的，理由写在注释里："so the guidance can be asserted at RUNTIME"。剩下两段没有跟进。
- `:879-937` 三段可选 HTTP listener（subhttp / metrics / manifest）形状逐字相同：`if addr != "" { ln, err := X.Bind(addr); if err != nil { return fmt.Errorf(...) }; log; go func(){ if err := X.ServeListener(...); err != nil { log } }() }`，每段约 17 行。

**建议**：把剩下两段诊断抽成 `clusterJSUnavailableError(voters int, peers []string, selfID string) error`（与 `n1ClusteredJetStreamFatal` 并列，可单测断言文案）；三段 listener 收成 `b.startOptionalHTTP(name, addr, bind, serve)`。之后 `Run` 约 **280 行代码 / 20 段**，每一行都是编排。

**量化 / 风险**：净减约 **50 行**，`Run` 338 → 约 280 行代码。changeRisk = **low**。触碰 wire/不变量：**否**。

---

### F9 [low] 13 个 `reply*Err` 包装函数 + 38 处 `replyErr + pubAuditCall` 成对复读

**证据**：`broker.go:1477` `replyErr`、`exec.go:125` `replyExecErr`、`expose.go:359/472` `replyExposeErr`/`replyExposeRmErr`、`upgrade.go:148` `replyUpgradeErr`、`proxy.go:36` `proxyErr`、`run.go:107/189` `replyRunFailed`/`replyKillFailed`、`sessions.go:206` `replyJSON`、`transfer.go:1002/1005/1008/1208` `replyPushErr`/`replyPullErr`/`replyCommitErr`/`replyFinalize` —— 13 个，每个 3–9 行，都在做"marshal 一个各自的 resp 类型 + `msg.Respond`"。`b.pubAuditCall(` 共 **38 处**，绝大多数紧跟在一个 `reply*Err` 之后，参数逐字重复 `(sid, fp, actor, verb, nid, false, code, msg.Reply, nil)`。`handleExposeReq` 的 allocate 错误映射（`expose.go:232-267`）里连续 6 个 `case` 全是这个二连。

**为什么是债**：轻微但真实——"回错必须同时发 audit"是个约定，靠成对复读维持；`expose.go:250-252`（`port.ErrPortExhausted`）与 `expose.go:264-266`（`err != nil` 兜底）的差异是**有没有发 audit**，而这个差异没有任何东西在守。

**建议**：`replyJSON` 已经泛化了一半（`sessions.go:206`）。把 13 个包装收成 `b.fail(msg, resp any, code string, aud *auditCall)`，让"回错 + 记 audit"成为一次调用。**量化**：净减约 60 行。changeRisk = **low**（错误码字符串必须逐条保持）。

---

## 反证：做得好的地方

1. **`reconcile_registry.go`（341 行）是教科书级抽取。** 它有：显式的存在理由（"Runs once at boot, behind a gate that is structurally false at boot, is not convergence; it is a no-op with a comment" —— 指名了 #58/P10 那个真实缺陷）、一条**可执行的准入不变量**（one-vote-veto：pass 只能"读期望 / 比实际 / 调已存在的幂等命令路径"，不得发明策略，并给出了算术论证——一次性错误摧毁状态一次，30s 周期化摧毁 2880 次/天）、一个**行为等价性证明**（anchored deadline 使改写前后在假时钟下逐 tick 相同）、以及"不起 goroutine、不持有 timer"的设计以避开仓库的 NumGoroutine/fd 泄漏门。9 个 pass 用同一个 `(name, interval, leaderOnly, lastTick, fn)` 元组表达。**这是本包里"如何正确地抽一个组件"的样板**——F1/F2 的建议本质上是把同样的手法用到写路径和 ingress 上。

2. **锁做得干净——God Object 并没有导致锁粒度粗化。** 8 把 mutex（`rosterStaleMu`、`manifestMu`、`proxyGenMu`、`proxyOpMu`、`clusterAdminMu`、`transferAuditMu`、`lastObserveMu`、`lastReplicaMu`），**每把只保护 1–2 个字段、只在 ≤4 处获取、无任何嵌套获取、不存在全局 `b.mu`**。热路径用 `atomic.Pointer`（`nc`、`tunnelSrv`、`manifestCache`）和 `atomic.Bool/Int64`（`jsUnavail`、`xferUnreapableBuckets`、`reExecRequested`、`transferAuditDraining`）而非加锁。`transferAuditMu` 把 `{检查 draining + WaitGroup.Add}` 做成不可分割对（`clusterwrite.go:147-152` + `transfer_audit_forward.go:88`），并在注释里论证了"裸 `atomic.Bool` 做不到 check+Add 原子"——这是对 `WaitGroup.Add` 与 `Wait` 竞态这一经典误用的正确处理。唯一可议的是 `proxyOpMu`（`proxy.go:68`）是**全 broker 而非 per-session** 的 proxy 变更锁，但 proxy set 是运维级低频操作，这个取舍正当。

3. **方法-字段耦合极稀疏，这从根本上限制了 God Object 的危害。** 263 个方法中只有 6 个碰 ≥6 个字段，只有 `Run` 碰 ≥10 个。经典 God Object 的核心风险是"任何改动都可能影响任何其他方法"——**在这里不成立**，因为绝大多数方法之间没有共享可变状态。这既是"它没那么糟"的证据，也是"拆起来很便宜"的证据。

4. **封装边界守得住。** `*Broker` 只导出 16 个方法，其中 6 个是 `*ForTest` 访问器；**包外 `broker.Broker{}` 构造点为 0**。也就是说这个 God Object 完全是**包内私事**，没有任何外部消费者被它的形状绑架。重构的 blast radius 有确切上界：包内 126 处 `&Broker{}` 测试字面量，全部是机械改写。

5. **包内 60% 的代码已经不在 `Broker` 上了。** `ClusterAdmin`(67 方法)、`clusterAdminBackend`(17)、`AuditPublisher`(15)、`AlertReconciler`(4)、`reconcileRegistry`(5)、`transferTracker`(5)、`Forwarder`、`webhookPoster`(4)、`forceSingleArm`(4) —— 13,080 行 / 60% 已经被抽出去了。这不是一个"从没做过分解"的包，而是一个"分解到一半"的包。

6. **注释密度异常高且质量高。** 全包 29.3% 是注释（6,336 / 21,618）；`Broker` struct 231 行 span 里 155 行是注释（67%），`Config` 66%，`registerCoreReconcilePasses` 60%。这些注释绝大多数不是"重述代码"，而是**记录为什么**——`clusterwrite.go:611-651` 的 `reaperMayDelete` 用 30 行论证"caught-up 必须在 raft 域而非 command 域度量，因为 SQLite command cursor 在 LogNoop 上不前进，跨域比较结构上永不为真——这曾静默禁用了整个闸门"。这类注释是本项目**最有价值的资产之一**，它把每次生产事故的根因钉在了代码旁边。**任何重构都必须整段搬运这些注释，不得当作"清理"删掉。**

7. **发现文案漂移就立刻收口的正确反射。** `n1ClusteredJetStreamFatal`（`broker.go:740-760`）的注释："the remedy literal is the shared `natsconf` SSOT — the same sentence the DATA-PLANE-DEGRADED status banner and `cluster recovery restore`'s completion text emit. **Those were three hand-copied copies, which is how the late one gets fixed and the early one rots.**" —— 这正是 F1/F2/F7 想要的那种反射，只是还没有系统化地施加到写路径和 ingress 上。

8. **`proposeOrForward` 本身是个好收口。** `clusterwrite.go:665-685` 把"leader 本地 Propose / follower 转发"和"leadership race 统一映射为可重试的 `cluster.ErrForwardNotLeader`"收在一个 20 行函数里，并解释了不这样做的后果（raw raft error 漏出去被 agent 当成永久拒绝而**退出进程**）。F1 批评的是它上面那 10 个模板方法，不是它本身。

9. **全仓 TODO/FIXME 仅 1 处。** 对一个 6.8 万行、经历过 4 轮大改的生产工具，这说明"发现问题就修或就登记到 `docs/reviews/`"的纪律是真的在执行，而不是靠代码里堆注释欠着。

---

## 本质 vs 偶然复杂度拆解

**本包 span 21,618 行 = 代码 14,051 + 注释 6,336 + 空行 1,231。下面的比例针对 14,051 行实际代码。**

### 本质复杂度（约 75%，约 10,500 行）

这个包一个人扛着 **10 个子系统**，每一个都是独立的分布式/系统编程问题：

| 子系统 | 本质难度来源 |
|---|---|
| NAT 穿透控制面 | 反向注册 + 心跳状态机（ONLINE/STALE/OFFLINE）+ G.1/G.2 双向 reconcile |
| 反向 TCP 隧道数据面 | 按需开公网端口、token 一次性泄露、cert pinning、跨 broker home 迁移 |
| auth_callout | 每连接签发 JWT、nkey 身份、PIN 验证、per-IP 限流 |
| Raft HA | 单 WAL owner、写转发、apply lag、leadership race、快照/恢复 |
| **双模式**（single + cluster） | 已发布产品必须保持单机部署字节等价 |
| 文件传输两档 | tier-A inline / tier-B JetStream Object Store + 跨 broker home + 崩溃后 ledger 收敛 |
| PTY 交互 | 两阶段 attach、Ctrl-C、失败事件 |
| proxy 订阅（P13） | generation/epoch 双计数器 fencing、keyset 推送、rehome hysteresis |
| 告警 + 可观测 | 复制式 alert store、webhook、Prometheus、JS-503 sustained 检测 |
| 集群生命周期运维 | init/grow/retire/force-single/upgrade 的两阶段 operation 状态机 |

**45 个 `Broker` 字段里，至少 32 个是这些子系统各自不可消除的运行期状态**（连接、JS 句柄、raft runtime、home 投递账本、proxy generation、transfer tracker、reconcile registry、各类缓存）。**43 个 `Config` 字段**同样：其中 20 个是真实的运维旋钮（监听地址、端口段、超时、保留期），15 个是测试注入点（`Now`、`DiskUsageFn`、`ReadyCh`、各类 interval override）——后者是**这个项目测试纪律的代价，而且是划算的代价**（它让整套 reconcile 逻辑能在假时钟下微秒级验证，不用起 Docker）。

`Run` 的 20 段编排、8 把细粒度锁、`reaperMayDelete` 的 raft-域 vs command-域论证、`readCommittedSession` 的 3s apply-lag 容忍窗口——这些**全部是本质的**。删掉任何一个都会重新引入一个已经在生产上发生过的故障。

### 偶然复杂度（约 25%，约 3,500 行）

| 来源 | 估算行数 | 可净删 |
|---|---|---|
| 双模式分支复读（47 处 `b.clusterMode`，其中 11 处是写路由的机械二选一） | 约 400 | 约 80 |
| 写动词 5 点散弹（17 verb × 双份 plan 闭包 + payload 解码，F1） | 约 500 | 约 120 |
| ingress 准入门 13 处复制粘贴 + 2 个不兼容的半成品门（F2） | 约 350 | 约 200 |
| 13 个 `reply*Err` 包装 + 38 处 reply/audit 二连（F9） | 约 200 | 约 60 |
| `StatusReport` 尾部三段 banner 追加 + 数据采集内联（F7） | 约 100 | 约 90 |
| `Run` 里的诊断文案 + 3 段同形 listener 装配（F8） | 约 90 | 约 50 |
| 22 个 nil-seam 的手工接线 + 配套反漏接测试（F6） | 约 120 | 约 55 |
| 文件碎片化 / 按 phase 切（F5） | 0（纯移动） | 0 |
| **合计** | **约 1,760** | **约 655** |

剩下的偶然复杂度（约 1,700 行）不是"多余代码"，而是**分散**——`proxy` 的 7 个字段、`transfer` 的 9 个字段散在一个 45 字段的 struct 里，它们的行数是本质的，位置是偶然的。这部分**重排能改善，删不掉**。

### 净判断

**"16 万行是不是屎山"这个问题，在本 lane 的答案是：不是，而且这个包的行数几乎全部有主。** 68,328 行"生产代码"里实际只有 45,832 行是代码（26.7% 是注释、6.2% 是空行）；本包 21,618 行里只有 14,051 行是代码。**真正冗余、可以净删的量级是本包代码的 4–6%（约 655 行）**——这个数字太小，不足以支撑"臃肿"的指控。

真正值得投入的是**形状**而非**体积**：F1（写动词收表）、F2（ingress 收门）、F3（`readDB` 类型消歧）三条加起来约 400 行的改动，能把三类**只在集群模式 / 只在 follower 路径 / 只在 DELETING 窗口**触发的静默故障类，从"靠人肉复读维持"变成"靠类型和单一收口维持"。以这个项目的历史看（三条缝各自都已经至少出过一次事故并被记在注释里），这个投入的回报是明确的。

F4（组件切分）建议**搭车做**，不单独立项——它的收益是阅读与测试构造成本，风险极低（包外 0 依赖），但也不解决任何正在流血的问题。
