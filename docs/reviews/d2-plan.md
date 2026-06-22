# D2 计划（主进程定稿）— "全 mutator 走 FSM Plan/Apply；N=1 功能等价"

> **来源**：Stage A 多专家对抗 workflow（5 drafter + 4 critic + 1 synth，**全 Opus 4.8**，~1.01M tok）→ 候选 synth plan。本文件是**主进程逐条裁定后的定稿**（唯一实现尺）。Stage A 原始 draft/critique 在 workflow transcript（`wf_c8b18225-2b1`）。
> **里程碑**：D2 出口 = **N=1 集群与今天单点 broker 功能等价**（§0 北极星、§19 硬节点）——一切后续分布式行为的回归基线（§19「❌ D2 未过就上多节点」）。

---

## 0. 主进程定稿裁定（5 个 open decision + 承重事实亲手验证）

### 裁定 D2-R1 — broker 接线范围：**ops-only，不切线上 broker（采纳 synth，推翻我先前直觉）**
D2 **只**交付 op 集 + 每 mutator `Plan*`/`Apply` 拆分 + lint 全开 + 等价 harness。**运行中的 broker 不动**（保留直连 mutator）。等价由 **DIFF-1 差分 harness**（今天直连 mutator vs FSM Plan/Apply 路径，同输入 → 逻辑内容哈希相等）证明，**而非**把生产 broker 切到 FSM 上。
- **依据**：§3.8 line 109 明确「真实 mutator + FSM 合到单 WAL 库是 **D9 一次性迁移**（journal_mode=WAL 落库头）」；§19-D9 列了「`cluster init --from-existing` + 删 `broker.New` 启动期写」。把 broker 切到 FSM 会强行解决 journal-mode（rollback-journal vs WAL）、`fsm.Snapshot` 在非 WAL handle 上、单 WAL 库合并——**全是 D9 工作**，拉进 D2 即违反先父后子。
- **我先前的直觉（「YES 接进 broker」）被纠正**：出口门「N=1 等价」由差分 harness 证明 FSM 路径产出与今天直连路径**逻辑内容字节一致**即满足，无需切生产。3/4 critic + synth + 架构正文一致指向 ops-only。
- **后果**：D2 不引入 `clusterApply` 生产接缝、不把 `cluster.Node` 塞进 `broker.New`、不碰 authcallout 跨包 routing（那些是 D3/D9 形状）。等价靠**测试里直接驱动 `cluster.Node`**。

### 裁定 D2-R2 — audit 去交织深度：**最小切 + 抽共享分类器（采纳 synth 的最小切，加一条澄清）**
ReconcileBatch 把 4 处 `proc.MarkExited` 收成**一条有序 op**、把全序元组烤进 entry（供 D5 重导）；**不建** D5 的 post-commit 单写者/dedup/term-fence/sweep。
- **澄清（我加）**：因 R1 是 ops-only，**线上 `reconcileOnRegister` 行为不变**。做法 = 抽一个**纯分类器** `resolveReconcile()`（无副作用、无 `db.Exec`、无 `pubAudit*`、所有 map 派生切片排序），**被两条路共用**：(a) 线上路 = resolve→直连 `MarkExited`+inline audit（行为与今天逐字节相同）；(b) op 路 = resolve→烤 batch。共用分类器使两路**不会漂移**，差分测试比较二者。这是**行为保持的重构**，不违反 ops-only。

### 裁定 D2-R3 — `OpNodeEvict`：**现在就加（采纳 synth）**
`adminsock/server.go:337/346 handleEvict` 在一个 tx 里 `DELETE agent_provisioning` + `DELETE nodes`（经 admin.go:149 OpEvict 实连，over `b.cfg.DB`）是**真实权威写**（survey + 全部 drafter 都漏了）。它就是 §5 的 `NodeRegister/Remove` 的 Remove 写者。**现在定义 + 差分测试**它（即便 ops-only 下只被 harness 驱动）——否则 D2「op 集覆盖今天所有权威写」的完整性有洞。

### 裁定 D2-R4 — `PlanCreate` 的 PIN-hash 位置：**留在 broker 边界（采纳 synth）**
`auth.HashPIN`（crypto/rand 盐）继续在 broker 边界算（`sessions.go` 在调 `session.Create` 前已 hash），hash 作为入参传进 `Plan`。这样 crypto/rand **永不进 `internal/session`**、保持今天分层。确定性满足：pin_hash 由 leader 侧一次铸成、作为固定字面量进 op。
- **差分 harness 注意**：HashPIN 不确定（每次盐不同），故 harness **算一次 pin_hash 喂两臂**（driveDirect 的 `session.Create` 与 driveFSM 的 `PlanCreate` 都收同一 pin_hash 作输入）。

### 裁定 D2-R5 — 旧 `TestDeterminismBannedImportBaseline`：**reachability lint 落地后改非对称/退役（采纳 synth）**
旧包级 baseline 在「新增 banned import」**和**「移除一个 baselined 命中」时都会红——Plan 侧搬迁会移除命中而**误红**、逼出 silence-edit。reachability 版落地后把它改成**只对新增报红**（非对称）或退役。

### 承重事实——亲手验证（不靠 agent 转述）
- **litTime 格式 = `t.String()`（不强制 UTC）**：我亲手 spike（modernc v1.50.0，`internal/storage` 临时 test 已跑后删）。modernc 把绑定 `time.Time` 存为**精确 `time.Time.String()`**——**忠实时区甚至 monotonic**：UTC→`… +0000 UTC`、`+0800`→`… +0800 CST`（不转 UTC）、`time.Now()`→带 `m=+…` 后缀。**RFC3339 不同**（`...T...Z`）。**关键纠正**：`LitTime(t)=t.String()`，**调用方必须传与对应 live mutator 完全相同的 `time.Time`**——port/proc/node/agentprov 绑 `now.UTC()`、**session.\* 绑 raw `now`（含 monotonic，是 live 既有怪癖，严格等价须忠实复现）**。传错时区会静默破坏等价 + 线上 `port.ListAllocatedForOfflineNodes` 字典序 `last_heartbeat_at < cutoff` 比较（node.go:77 裸串比较）。已锚成 frozen 字节门 `TestLitTimeMatchesBoundParam`（UTC/+0800/monotonic 三例全绿）。
- **`Args []any` int64 精度丢失属实**：`int64(1750512345123456789)` 过 JSON→float64→`1750512345123456768`（差 21）。`processes.start_time_ticks` 是 int64 且 `pidReused()` 按精确相等比——损坏即破坏 PID-reuse 防御 + 副本发散。→ **D2 op 一律禁 `Statement.Args`**，全走 leader 烤的 SQL 字面量（一个审计过的 `lit()`）。

### Stage B 第一步要落的 doc-first 修正（先改 §0–§18 正文再写代码）
1. **§3.4**：proc pid 的 ULID 是 **agent 铸**（不是 leader 烤）——纠正表中「ulid.Make pid」行；leader 对 proc pid 不烤任何东西；banned-import lint **reachability-scoped**，故 `oklog/ulid` 合法留在 `internal/proc`（`NewPID`，非 Apply 可达）。
2. **§3.4/§5**：litTime **driver-coupled** 到 modernc `time.Time.String()`（记确切格式 + spike 门；driver 升级必重验）。
3. **§5**：D2 in-scope op 集定稿（MemberJoin 提升、NodeEvict 新增、MemberKick/RotatePin/PortReassignHome/Alert*/ClusterNode* 推迟，逐条注明无 live writer）；arg 编码 = **全 leader 烤字面量、禁 `Args []any`**。
4. **§3.5**：`proxy_ready=0` reset 是**活性、且每次 re-register 无条件**触发（F8 安全复位）。
5. **§3.8/§19-D2**：明确 D2 = **build+prove** FSM 写路径（cutover-ready），**不切线上 broker**；cutover + 单 WAL 库合并 = D9。§13.1「禁 FSM 外 INSERT」lint 是 **reachability-scoped（Apply 面）+ 分级**（直连 mutator grandfathered 到 D9 cutover 前）。
6. **§13.1**：determinism lint 用 `golang.org/x/tools/go/callgraph/cha`（引入 `golang.org/x/tools` 作 **test 依赖**）——登记（go.mod 增量，升级前验 go directive）。

---

## 1. 范围与出口门

### D2 做
1. **窄类型 op 集**（§5，无 `GenericRowMutate`）——覆盖**今天有 live 单 broker 调用者**的权威写，由 **`internal/` 全量 grep 每个对 Apply-owned 表的 INSERT/UPDATE/DELETE** 推出（见 §2），不照搬 §5 目录。
2. **Plan/Apply 拆分**：`internal/{port,proc,node,session,agentprov}` 每 mutator + survey 漏的两个写者（`session.AddMember` join、`adminsock` evict）。leader-only `Plan*` 读 leader DB、烤每个 PK/唯一索引/fence/时间值；副本 `Apply*` 执行 leader 渲染的 SQL、绑 `*sql.Tx`。
3. **确定性雷区**（§3.4）：leader 烤 time/token/pin-hash/sub-id；**ULID 留 agent 侧**（已验，§3）；ReconcileBatch 全序；**保 AUTOINCREMENT**（leader 省 `row_id`）。
4. **ProcGC 与全部活性写留 leader-local**（非 Raft op）。
5. **测试** = §13.1 lint 全开 + §13.2 多 FSM 等价 + **DIFF-1 差分 harness**（真正的出口门工件）+ `-race`/内建泄漏门 + `TestD2Matrix` e2e。

### D2 不做（先父后子 carve-out，均已验）
- **不切线上 broker DB**（裁定 R1）。cutover + 单 WAL 库合并 = D9（§3.8 line 109）。
- **不做 `apply.*` 转发 / not-leader 路由**（D4）。N=1 = self 永远 leader；`Plan` 直读 `n.db`。`IsLeader` 守卫与 not-leader typed error 留**惰性**，同 `ReqID`（保留+惰性，无 dedup；dedup-by-ReqID 是 D4）。
- **不做多节点 / NATS 集群传输**（D3）。测试驱单 `cluster.Node`；§13.2 的「≥2 FSM」是**喂同一 op 流的独立单节点 FSM**（离线），非复制集群。
- **不做 D5 审计重导管线**（term-fence / dedup-by-raft-index / 单写者选举）。D2 的 reconcile 止于「确定性全序 DB-mutation 集 + 不交织 `db.Exec`」。
- **不做 D6 rehome/per-expose home/server_id 桥接/cert-pins；不做 D7 membership 两阶段；不做 D8 alerts。**
- **不做 P13 proxy**（`AllocateProxy`、`proxy_enabled`、`proxy_epoch`、`BumpProxyEpoch`）——留直连 `db.Exec`，lint **证其非 Apply 可达**，不迁移。
- **不再 bump proto**（已 v2）；`commandVersion` 保 1，与 `proto.ProtoVersion` 解耦。

### 出口门（硬里程碑）
**非 vacuous 差分测试 DIFF-1** 证明：对取自真实调用点的同一 op 序列，**今天直连 mutator**（`port.Allocate`/`proc.Insert`/`node.Register`/`session.Create`… on `storage.Open` DB）产出的 DB 与 **`Plan*`→`Node.Apply`→`ApplyTx` 路径 on 1-node cluster** 产出的 DB **逻辑内容一致**（per-table 排序哈希，明确排除集），含**负向对照**（故意烤错一个值 → DIFF-1 变红）。叠加 §13.2 多 FSM 绿、§13.1 lint 硬开、`-race`/泄漏门绿、`make test`/`make e2e`/`make lint` 绿。

> **为何 DIFF-1 是承重门、不是 §13.2**（critic 3 crux）：§13.2（同流 → ≥2 FSM）对**确定性但错误**的烤值（错时间格式、漏活性写、错列）**结构性失明**——两副本共享同一 bug。只有 DIFF-1（旧路 vs 新路）能抓。本计划**反转**先前重心：§13.2 证*跨副本收敛*，DIFF-1 证*与今天等价*。

---

## 2. Op 集

### 权威写清单（全量 grep，已验）
| 表 | 语句 | 位点 | live 调用者 |
|---|---|---|---|
| sessions | INSERT | session.go:82 | sessions.go (Create) |
| members | INSERT (owner) | session.go:93 | sessions.go (Create) |
| **members** | **INSERT OR IGNORE** | **session.go (AddMember)** | **authcallout JoinWithPIN** ← *survey 漏* |
| sessions | UPDATE state=DELETING | session.go:183 | sessions.go (Tombstone) |
| {6 表} | DELETE by sid | audit.go:103-109 | dropSessionRows (HardDelete) |
| nodes | UPSERT (身份+活性) | node.go:104 | broker.go (Register) |
| **nodes + agent_provisioning** | **DELETE by sid,nid** | **adminsock/server.go:337,346** | **adminsock handleEvict** ← *全 drafter 漏* |
| processes | INSERT | proc.go:118 | exec.go (Insert) |
| processes | UPDATE status=EXITED | proc.go:147 | exec.go + reconcile.go ×4 |
| port_allocations | INSERT | port.go:182 | expose.go (Allocate) |
| port_allocations | UPDATE FREED/REVOKED | port.go:300,330 | expose.go + proxy.go (Free/Revoke) |
| agent_provisioning | INSERT OR IGNORE | agentprov.go | authcallout (Provision) |

**Leader-local（非 op）**：`node.Heartbeat`(node.go:142)、`ReconcileStates`(:198)、`SetProxyReady`(:263)、`GCExited`(proc.go:273)、`RebuildLiveness`(liveness.go:21)。**P13/out（留直连、证非 Apply 可达）**：session proxy_enabled/proxy_epoch(session.go:244/282/310)、`AllocateProxy`(port.go:559)、proxy.go:512。

### D2 in-scope OpType 集（定稿）
```
OpSessionCreate, OpSessionTombstone, OpSessionHardDelete,
OpMemberJoin,                       ← 提升（AddMember INSERT 是 live 写；critic 1 blocker）
OpNodeRegister (identity-only),
OpNodeEvict,                        ← 新增（adminsock DELETE nodes+agent_provisioning；裁定 R3）
OpProcCreate, OpProcMarkExited, OpReconcileBatch,
OpPortAllocate, OpPortFree, OpPortRevoke,
OpAgentProvision,
OpClusterMetaSet                    ← D1 保留（cursor/test 接缝）
```
**推迟（无 live writer，已验 grep 零写者）**：`MemberKick`（members 的唯一 DELETE 折进 HardDelete）、`PortReassignHome`(D6)、`RotatePin`（只有 NATS 权限串、无 `pin_hash` UPDATE 写者）、`Alert*`(D8)、`ClusterNode*`(D3)。逐条理由：今天无权威写者，加了就是等价测试无法 exercise 的死代码——non-vacuity 是里程碑。

> **`OpProcMarkExited` vs `OpReconcileBatch`**：两类 MarkExited 调用点（exec.go 单退出 + reconcile.go ×4）共用**一个 canonical SQL 渲染器**（单一出处），保证两路吐字节相同 `UPDATE`。exec.go 单退出 → `OpProcMarkExited`（一条 Statement）；reconcile.go → `OpReconcileBatch`（Body = 同渲染器的 Statement 列表）。lint 断言两路用同一渲染器、不漂移。

### Arg 编码：**全 leader 烤 SQL 字面量（一个审计过的 `lit()`）；D2 op 一律禁 `Statement.Args []any`**
裁定 R1 的承重事实小节已证 int64 精度丢失。`Args []any` 比「INSERT 失败」更危险：精度损坏 `start_time_ticks` 静默破坏 PID-reuse 防御 + 发散副本。一个审计路（`lit*`）严格优于两个字节稳定性证明（litText + JSON-string）。

**`internal/cluster/sqlbake.go`（新）**：
```go
litText(s string)  // '' doubling；拒内嵌 NUL；唯一 text→SQL 路
litInt(int64)
LitTime(t time.Time) string   // = t.String()（不强制 UTC；caller 传与 mutator 一致的时间，已亲手验，见裁定 R1）
litNull()
```
- **LitTime 已 pin + 门控**：`LitTime(t)=t.String()`（不强制 UTC；caller 传与 mutator 一致的时间）；frozen 字节相等 spike（绑定参数路 vs `LitTime` 写两库、断 `CAST(col AS TEXT)` 字节相等，UTC/+0800/monotonic 三例）作 blocking 前置门 `TestLitTimeMatchesBoundParam`。doc-first §3.4 注明 driver-coupled。
- **注入安全**：`litText` 是唯一 text→字面量点（`''` doubling + 拒 NUL），配对抗单测（名含 `'`、`--`、`;`、NUL、unicode）。一条 AST 规则禁 `sqlbake.go` 外 `fmt.Sprintf`-of-SQL，堵串接逃逸。
- `Statement.Args` 对 D2 op **禁用**（只留给惰性的 D1 `ClusterMetaSet`）。`Command`/`Statement` 字段与 `commandVersion` 不变。

---

## 3. 每 mutator 的 Plan/Apply 拆分

**形状（解 import-cycle）**：SQL 留在 `internal/{port,proc,node,session,agentprov}`；每个加 `Plan*` 返回**每 op typed 结果 + `*cluster.Command`**（§5 secret-return）。`internal/cluster` **不**得 import mutator 包。`Apply` = **一个共享 `genericExecApplier`**（从 `clusterMetaApplier` 抽：`for st := range cmd.Body { tx.Exec(st.SQL) }`，无 Args），按 OpType 注册进 `defaultAppliers`（可读 + 未来分化）。

> **lint 节必须自认（critic 2/3）**：共享 applier **切断调用图**——从 Applier 根的 import-reachability lint 因此**对 Plan 侧确定性是 tautologically clean、无法证明**。真正的确定性保证是 **§13.2 多 FSM 等价**；lint 是**绊线、非证明**。计划明说这点、不夸称 reachability lint 证了 Apply 确定性。

**port.Allocate** → `PlanAllocate`（leader）：名唯一性 SELECT + `findFreePort` 扫描（或 `desiredPort` 门）+ `genToken`(crypto/rand) + `hashToken`，**在 propose 前**返 `ErrNameTaken`/`ErrPortExhausted`/`ErrPortOutOfBand`；渲染 `INSERT … 省 row_id`，`port=litInt`、`token_hash=litText`、`created_at=litTime`。**保 AUTOINCREMENT**（§3.6，单 leader 串行 Apply 下 SQLite 确定性赋 rowid，partial-unique idx 兜底）。raw token 经 typed 结果返回（§5）。`PlanFree`/`PlanRevoke` 烤 `revoked_at=litTime`。

**proc** → `PlanInsert`：烤 agent 给的 `pid`(litText)、`argv`-JSON(litText)、`started_at`/`start_time_ticks`(litInt)。**ULID 是 agent 侧——已验**（`NewPID`/`ulid.Make` 在 agent/exec.go + run.go；broker 收到成形 pid）。故 §3.4「leader 烤 ULID」**doc 纠正**：leader 对 proc pid 不烤；ULID 非 Apply 可达，banned-import lint **reachability-scoped** 使 `oklog/ulid` 合法留 `proc`。`PlanMarkExited` 烤 `exit_code=litInt`、`ended_at=litTime`；leader 解析器拥有 not-found 决策，故 Apply 的 `UPDATE … WHERE pid=… AND status='RUNNING'` 故意无 check（收敛副本上幂等 no-op）。

**node.Register — 身份/活性拆分（§3.5，crux）**。已验 node.go:104 UPSERT 混身份 {boot_id,release_version,proto_version,proxy_capable,registered_at} + 活性 {status='ONLINE',last_heartbeat_at,proxy_ready=0}，两时间戳同 `now` + F8 `proxy_ready=0` reset。
- `OpNodeRegister` Apply 写**身份-only**；`registered_at=litTime`；`ON CONFLICT` **不**碰 status/last_heartbeat_at/proxy_ready。
- 活性半（`status='ONLINE'`、`last_heartbeat_at`、**`proxy_ready=0` 无条件**）是 **leader-local**，post-Apply（测试里；线上保持现有原子写——见下）。
- **F8 安全复位必须无条件**（critic 2/3 blocker）：leader-local 活性写在**每次** re-register 触发 `proxy_ready=0`，即便身份是 content no-op，否则重启 agent 在 `/sub` 里假 proxy-ready。专测：同身份 payload re-register 断 `proxy_ready` 1→0。
- **线上原子性（裁定 R1 化解）**：因 ops-only，**线上 `node.Register` 保持单条原子 UPSERT（直连 `db.Exec`）不变**。身份/活性拆分是 **op 定义**的属性（供 lint + DIFF-1 exercise 身份-only Apply），**非线上写**的属性。D2 里「两写之间崩溃窗口」隐患**不存在**。

**session.Create** → `PlanCreate`：PIN 在 broker 边界**预 hash**（裁定 R4）。Body = `[INSERT sessions(…,pin_hash,created_at litTime), INSERT members(owner, joined_at litTime)]` 作**一条 Command**（Apply 在 FSM txn 内执行两 INSERT → 两-INSERT 原子性保持）。`PlanTombstone`/`PlanHardDelete` 同理；HardDelete = `dropSessionRows`(audit.go:103-109) 的 6 条有序 DELETE 作一 Body。**JetStream `history-<sid>` 拆除留 leader-local**（JS 非 FSM 态）。

**MemberJoin** → `PlanJoinWithPIN`：`verifyPIN`(crypto/subtle) 在 `Plan` 跑 → `ErrInvalidPIN` 在 propose 前；Apply = `INSERT OR IGNORE members(joined_at litTime)`。

**agentprov** → `PlanProvisionWithPIN`：session `pin_hash`+state 读 + `verifyPIN` + 幂等 re-read 全在 Plan 解析成 leader 决策（`ErrSessionMissing`/`ErrSessionDeleting`/`ErrInvalidPIN`/`ErrAlreadyProvisioned`）；吐 `INSERT OR IGNORE agent_provisioning(joined_at litTime)` op 或无 op。Apply = 裸 INSERT。

**NodeEvict** → `PlanEvict`：Body = `[DELETE agent_provisioning WHERE sid,nid; DELETE nodes WHERE sid,nid]`（匹配 server.go:337/346 顺序）。

**cfgWithDefaults fallback 删除（critic 2 minor 范围化）**：删 port.go cfgWithDefaults 的 `time.Now()` fallback；时钟做每个 `Plan*` 的**必填显式参数**。**但**小心范围：`AllocateProxy`(P13,留直连) 共用该 helper 且有测试传 `nil` cfg——给 `AllocateProxy` 自己的显式时钟管线（仍直连 `db.Exec`），删前审计所有 nil-cfg 调用者，免 lint 清理改了 P13 时间戳行为。

**Plan 串行化 / 竞态（critic 2 major 加固）**：复合 RMW（Allocate 名+findFreePort、Create 两-INSERT、ProvisionWithPIN verify-then-bind）一旦读(Plan)/写(Apply) 跨 `raft.Apply` 即丢单 txn 原子性。引入 **leader `applyMu`** 横跨 `{Plan 读 leader DB} + {n.Apply propose} + {await future}`。**不死锁前置条件（critic 2 补的关键）**：`Plan` 必须在返回 Command 前**完全 materialize 并关闭每个 `*sql.Rows`、不持任何开着的 `*sql.Tx`**，否则 Apply 的 txn 在单池连接(`SetMaxOpenConns(1)`)上永久阻塞。活性/GC 写**不**取 `applyMu`（碰不相交列/表——**表-所有权不相交论证必须明写进计划**：GC 只碰 EXITED processes；活性只碰 nodes 活性列；都不碰 mutexed Plan 读的 port_allocations/sessions/members）。配并发测试（§6）。

---

## 4. ReconcileBatch

已验：`reconcileOnRegister` 只有**一类**复制 DB 写——`proc.MarkExited` ×4(reconcile.go:93/108/119/129)，端口半**零** DB 写（keep/revoke 是回包指令 + `pubAuditPort`）。故：
- **DB-mutation 集 = 仅 proc MarkExited 行**。端口项**无 Statement**（烤幻影端口写会偏离今天）。
- **全序**：解析器产全序项列表——proc 项按 `pid`(ULID 串) ASC、port/orphan 项按 `port`(int) ASC，proc-组-then-port-组（组序固定、永不 map 派生）。消除 reconcile.go 的 Go-map 不确定（agentByPID、portByHash）。
- **烤请求-only 字段**（name/local_port/rc——已验只活在 live `NodeRegisterReq`）进每项，使副本 Apply / 未来 D5 重导永不重读请求。rc 处置按现码：pidReuse=-1、exited=`*lp.RC` 或 -1、unknown=-1、missed-exit=-1、killed_orphan=0。
- **一个烤时间戳**：解析顶端取 `now` 一次；`ended_at=litTime` 进每条 MarkExited；同一刻进 audit 项。
- **排序每个 map 派生输出切片（critic 2 major），不只 audit/DB 元组**：`dropProcesses`（孤儿杀列表、返回 **agent** 作 SIGTERM/SIGKILL 指令）建于 Go map，必须**按 pid 排序**；`revokePorts`/`accepted` 同理。否则 §13.2 字节相等断言会 flake（Go 每次 range 随机化 map）且 agent 面指令 replay 不稳。

**D2/D5 audit 边界（裁定 R2，最小切 + 共享分类器）**：D2 **停止交织直连 `db.Exec`**（4 处 MarkExited 走一条有序 `OpReconcileBatch`）+ **把全序元组集烤进 entry** 供 D5 重导。**D2 不建** post-commit 单写者发布器。抽**纯分类器 `resolveReconcile()`** 共用于线上路（行为不变）与 op 路（见裁定 R2）。**零 term-fence/dedup/单写者选举**（那些 D5，承重注释标记）。N=1 self==leader，audit 集行为与今天相同，只是顺序变确定。

**解析器位置**：`resolveReconcile()` 在 `internal/broker`（读 leader DB + 消费 live 请求 + 拥 G.1 决策逻辑），输出全序 `ReconcileBatchReq`。`internal/cluster` 的 `reconcileBatchPlanner` 是**纯渲染器**（项→烤 Statement），保 `internal/cluster` 无 broker/proto/crypto/ulid import。

---

## 5. Broker N=1 接线

### 中心裁定：**ops-only，不切线上 broker**（裁定 R1）
D2 交付 op 集 + 每 op typed `Plan*` + Apply + lint + 等价 harness。**线上 broker 不变**（保留直连 mutator）。这：honors §3.8 D9 WAL-合并边界（不在线上 rollback-journal DB 上跑 FSM、不改 journal-mode、无 `fsm.Snapshot`-nil-`ro` panic）；保 DIFF-1 **非 vacuous**（旧直连 mutator 活着 = golden 臂）；推迟 `cluster.Node`-in-`broker.New`+inmem transport+authcallout-seam（D3/D9 形状）出 D2。**故 D2 无 `clusterApply` 生产接缝**；等价靠**测试直驱 `cluster.Node`**。

### Secret-return 契约（critic 4 blocker — drafted API 不存在）
已验：`Node.Apply(cmd) error` 只返 error；`Planner.Plan` 只返 `(*Command, error)`。故**带参 op 不走 generic `Planner` 接口**。每个是**每 op typed leader 函数**：
```go
PlanAllocate(db, …) (*port.Allocation, *cluster.Command, error)  // Allocation.Token = raw secret，仅内存
```
**调用者持 typed 结果**（raw token / raw PIN），`Plan` 在调 `n.Apply(cmd)` **前**返回它；`*Command` 只带 `token_hash`。`n.Apply` 保持 error-only。不变式测试：raw token 与 Plan 返回字节相同 **且** grep-断言 raw secret 串**绝不出现在任何 encoded `Command`/`Statement`**。

### Leader-local carve-out（lint 例外清单，本节拥有）
`GCExited`、`Heartbeat`、`ReconcileStates`、`SetProxyReady`、`RebuildLiveness`（列-lint 白名单）+ 全部 P13 proxy 写留直连 `db.Exec`、**证非 Apply 可达**。`ProcGC` 从 §5 op 集与 §3.4 烤时间/lint 表移除。

### 存储 journal（WAL vs frozen Open）张力——**显式推迟 D9**
因不切线上，broker 留 `storage.Open`（rollback-journal，P0–P13 冻结），FSM 在**测试里跑自己的 `OpenWAL` 文件**（D1 路、`synchronous=FULL`，kill-9 矩阵验过的耐久模式）。D2 不引入注入-`*sql.DB` 的 `cluster.Node` 构造（会拉 D9 前移 + FSM 跑在非测试 journal 模式）。单 WAL 库合并 = **D9，按 §3.8 line 109**——此处记为显式推迟、非 open question。

---

## 6. 测试

**§13.1 lint — 全开、sound、配对负向对照（D1 self-check 先例对每个新门强制）**：
- **用 `golang.org/x/tools/go/callgraph/cha`（或 `rta`）建 Apply-reachability 调用图**、seed 于 `fsm.Apply`——**非**手卷 `TypesInfo.Uses` BFS。Applier 是 **map 里的接口值**（`defaultAppliers() map[OpType]Applier`），静态 BFS 不遍历接口/map dispatch → 整个 Apply 子树不可达 → **vacuously 绿**。CHA/RTA sound 地过近似动态 dispatch（critic 3 blocker）。
- `TestDeterminismBannedImportBaseline` 翻成 **reachability-scoped 硬失败**（crypto/rand、math/rand、oklog/ulid 对 Apply-可达码）。包级 import 在 Plan/agent 侧合法（port 留 crypto/rand 给 Plan 的 `genToken`；proc 留 ulid 给 agent 侧 `NewPID`）。旧包-baseline 退役/非对称（裁定 R5）。
- 实现 `TestApplyReachabilityDeterminismLint`（现 `t.Skip`）：(a) **禁 Applier 外任何对 Apply-owned 表的 `*sql.DB`.Exec INSERT/UPDATE/DELETE**——锚在 `*sql.DB.Exec` **调用**(reachability)、非 SQL 字面量出现（Plan 合法在渲染字面量里提表名）；(b) 禁 Apply→`*sql.DB`-bound mutator；(c) 列级活性断言（Apply 永不写 status/last_heartbeat_at/proxy_ready）配 **`RebuildLiveness` 例外**，+ 反向（leader-local 码永不写身份列）。
- **每条规则经同一 dispatch 路的负向对照**：test-only `defaultAppliers` 变体里注册一个 ApplyTx 传递性 import `math/rand` 的 poison Applier **必须被抓**；Plan-only helper import crypto/rand **必须不被抓**；Applier 外 `db.Exec(INSERT INTO processes …)` **必须报**；Plan 把同 INSERT 渲染进 `Statement` **必须不报**。

**§13.2 多 FSM 逻辑内容等价**：泛化 `hashClusterMeta` → `perTableSortedHash(db, table, excludeCols)` + `equivHashFull(db)`。同 op 流喂 **≥2 个独立新 FSM**，断两两相等。**排除集明确且权威**：
- **排除**：`schema_migrations`（各副本自盖）、活性列 `{status, last_heartbeat_at, proxy_ready}`。
- **纳入（必须收敛——这些正是里程碑保证的 leader 烤值）**：`created_at`/`registered_at`/`ended_at`/`token_hash`/`pid`、**`row_id`**（`port_allocations` 的投影**与** ORDER BY 都含，使 AUTOINCREMENT 场景非 vacuous——critic 3 major）、**`sqlite_sequence`**（AUTOINCREMENT 物化、落 `.dump`；同流必须匹配，证 rowid 确定性——critic 2 blocker）。
- **两个具名比较器（critic 3 minor）**：`equivHashFull`（含 cursor 行 applied_index/applied_term；§13.2 同流须收敛）vs `equivHashAuthoritative`（排 cursor 行 + schema_migrations + 活性 + sqlite_sequence；给 DIFF-1，直连臂无 FSM cursor）。单一比较器对两者都对是不可能的。

**§13.2 对抗场景**：(a) Allocate×N → Free → 再 Allocate（token + AUTOINCREMENT 不复用）；(b) ProcCreate 定/烤时钟；(c) Allocate → SessionHardDelete(FK 级联) → 再 Allocate 同名；(d) **ReconcileBatch 打乱 Plan-输入 map**（注入种子打乱 agentByPID/portByHash 构建序；断**吐出的 `Command.Body` 字节**跨 N 次有序稳定 **且** post-Apply DB 哈希相等）——这是全序的**唯一**非 vacuous 证明（排序后的 DB 哈希对序失明）。每场景配**发散负向对照**（注入非烤值/省排序）必须使哈希/字节不同。

**DIFF-1 N=1 差分 harness（出口门工件 — critic 4 blocker 化解）**：新 `test/cluster/differential_*_test.go`。`driveDirect(db, ops)` = **今天 mutator on `storage.Open` DB**（D2 不删故活着）；`driveFSM(node, ops)` = `Plan*`→`node.Apply` on 1-node。断 `equivHashAuthoritative(directDB) == equivHashAuthoritative(fsmDB)`，op 序取自现有 per-phase fixture(p3/p4/p6)。**vacuity 化解**：旧直连 mutator 活着 → 两臂是真不同的码（非自比）。**负向对照**：烤错一个值（错 port、错时间格式）→ DIFF-1 红。

**活性拆分非排除 oracle（critic 3 major — 哈希唯一失明处）**：因 §13.2/DIFF-1 排活性列，加**直接行读**断言：register op + leader-local 活性写后，`nodes.status='ONLINE' AND proxy_ready=0`；+ re-register-content-no-op 断 `proxy_ready` 1→0。仅一个测试拥有此。

**TOCTOU + 并发测试**：并发 `SessionTombstone(sid)` + `ProvisionWithPIN(sid)`（provision 须在 DELETING session 上失败或串行其后——匹配今天单 tx）；N 并发 `PlanAllocate`→Apply 同 (sid,name)/band 断恰一胜、`ErrNameTaken`/`ErrPortExhausted`、`SetMaxOpenConns(1)` 下**无死锁**（证 `applyMu` + no-open-handle 前置条件）。

**门控**：全部 FSM/Apply 测试在 `-race` + **内建 `runtime.NumGoroutine` poll-with-tolerance + fd 基线**（刻意非 goleak），含 Apply-churn 泄漏 case（N op → Shutdown）。新 `TestD2Matrix`（克隆 `TestD1Matrix`）：`go test -race -count=1 -timeout 240s ./internal/cluster/... ./test/cluster/... ./test/determinism/...`，把等价+差分+lint 折进 `make e2e`。**等价/差分测试先写成 FAIL**（对故意烤错值）证非 vacuous，再宣里程碑达成。

---

## 7. 排序

1. **DOC-FIRST**（裁定 R1 小节的 6 条 §0–§18 修正）——改 lint/test 设计，先落。
2. **Pin spike + 地基**：`litTime` 字节相等 spike(blocking 门) → `sqlbake.go`(`lit*` + 对抗测试) → `command.go` OpType/knownOps 增长(无 struct/version 改) → 抽 `genericExecApplier` + 按 OpType 注册。建**比较器 + CHA/RTA reachability helper**（配 D1 FSM self-check，ops 前就绪）。
3. **简单单语句 op 先**（低风险验字面量/等价管线）：`PortFree`/`PortRevoke`、`ProcMarkExited`、`SessionTombstone`、`AgentProvision`。每 op 落时加其 §13.2 场景 + 开其匹配 lint 规则（co-evolve、不大爆炸）。
4. **复合 RMW op**：`PortAllocate`(token 铸进 Plan；AUTOINCREMENT 省；`applyMu`+no-open-handle 前置+并发-allocate 竞态测试)、`SessionCreate`(两-INSERT Body)、`ProcCreate`(FK check 进 Plan、agent pid 烤)、`MemberJoin`/`ProvisionWithPIN`(verify 进 Plan + TOCTOU 测试)、`NodeEvict`。
5. **NodeRegister** 身份/活性拆分(只 op 定义；线上写不变) + 非排除活性 oracle。
6. **ReconcileBatch 最后**(最难)：`resolveReconcile` 纯分类器(无 `db.Exec`、无 `pubAudit*`、map 派生切片全排序) → `reconcileBatchPlanner`/Applier → 打乱-输入全序测试。
7. **翻 banned-import 门为硬失败 / 退役 legacy baseline**(全部 Plan/Apply 拆完后，否则半迁移包误红)。**DIFF-1 差分 harness** + `TestD2Matrix` + `make e2e` 接线**最后**。终门：DIFF-1 绿+负向对照、§13.2 绿、lint 硬开、`-race`/泄漏绿。

**PR 形状**：单人 repo 直提 main（memory `feedback_main_only_no_branches`），但建议**3 个逻辑提交段**——(C1) doc-first + spike + 地基；(C2) op 迁移 + 每 op 测试（按上序）；(C3) ReconcileBatch + DIFF-1 + lint 硬翻 + matrix。commit/push 是 phase 收尾 step 7，非计划。

---

## 8. 风险登记

| 风险 | 级 | 缓解 |
|---|---|---|
| **错时间格式/时区**静默破坏 DIFF-1 + 线上字典序 OFFLINE→REVOKE，而 §13.2 vacuously 过 | Critical | `LitTime=t.String()`（不强制 UTC；caller 传与 mutator 一致时间，**已亲手验 UTC/+0800/monotonic 字节相等**）；blocking 字节门 `TestLitTimeMatchesBoundParam`；doc 注 driver-coupled |
| **`Args []any` int64 精度丢失**损坏 `start_time_ticks` → 破坏 PID-reuse + 发散 | Critical | D2 op 禁 `Args`；全字面量经一个审计 `lit*`；「int 破 INSERT」论据本身错(affinity 存活)——别让审查者复活 hybrid |
| **reachability lint vacuously 绿**（接口/map dispatch 未遍历；generic applier 切图） | High | 用 `callgraph/cha`/`rta` seed `fsm.Apply`；经同一 map dispatch 的配对负向对照；lint 定位为绊线、Plan 侧确定性靠 §13.2 |
| **DIFF-1 自比**（删旧路 → 比自己 → 证 nothing） | High | D2 保留旧直连 mutator(不切线上)；两臂真不同；烤坏值的负向对照逼红 |
| **漏 live writer** 绕过 FSM（survey 漏 members join INSERT + adminsock evict DELETE） | High | op 集由**全量 grep**(§2)推出、非 §5 目录；MemberJoin+NodeEvict 提升；sweep 测试断 `internal/{adminsock,authcallout,broker,…}` 无 Applier 外的 Apply-owned 表 `db.Exec` |
| **F8 `proxy_ready=0` reset 丢**于 no-op re-register → stale 节点假 proxy-ready(安全回归) | High | 无条件 leader-local 活性写；非排除直接行 oracle 测试(同身份 re-register 1→0) |
| **`applyMu` 死锁** under `SetMaxOpenConns(1)` 若 Plan 持开着的 `*sql.Rows`/`*sql.Tx` 跨 `raft.Apply` | High | 显式 Plan 契约(返回 Command 前全 drain/close)；明写表-所有权不相交论证(哪些写者跳 mutex)；并发-allocate -race 测试断无死锁 + 正确败者错误 |
| **ReconcileBatch 序经 DB 哈希不可测**(排序哈希对序失明；map 派生指令切片不稳) | Medium | 排序每个 map 派生输出切片(含 agent 面 `dropProcesses`)；打乱 Plan-输入 map 断吐出 Command 字节相等；省排序的负向对照逼红 |
| **范围蔓延入 D9**(FSM over 线上 rollback-journal；`fsm.Snapshot` nil-`ro` panic；线上自动快照) | Medium | ops-only 不切线上；FSM 测试里跑自己 `OpenWAL` 文件；推迟记于 §5 引 §3.8 line 109 |
| **`sqlite_sequence` 发散** 于 DIFF-1 若两臂跑不同 op 数 | Low | 两具名比较器；§13.2 同流纳入 sqlite_sequence、DIFF-1 排除(或两臂从空跑同数) |
| **cfgWithDefaults fallback 删除**静默改 P13 `AllocateProxy` 时间戳 | Low | 给 `AllocateProxy` 自己显式时钟；删前审计 nil-cfg 调用者 |
