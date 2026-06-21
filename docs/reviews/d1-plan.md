# D1 Plan — 状态层：Raft FSM + SQLite Apply + 快照/恢复（N=1）

> **定稿人 = 主进程**（CLAUDE.md §3 step 2）。Stage A：4 专家对抗草拟 + 4 对抗互评 + 1 综合（workflow `wf_dee13cbf`）→ 本文为定稿。
> 实现尺 = `docs/distributed-broker-architecture.md` §0–§18 正文 + §19 D1。所有承重机制已对 pin 源（hashicorp/raft@v1.7.3、modernc.org/sqlite@v1.50.0）与 D0 代码核验。
> **本 phase 已先改正文**：§3.8/§3.5/§3.1/§5/§13/§19 已落 D1 实现修正（先改正文再改代码，见各节「D1 实现修正」块）。

## 0. 主进程定稿决定（对 9 个未决项 + 6 处正文修正）

| 决定 | 裁定 |
|---|---|
| 泄漏门 goleak？ | **不引入 goleak**（已核验：仓库 3 处明确拒之、go.mod/go.sum 无）；用内建 `NumGoroutine` poll-with-tolerance + **fd 基线门**。**正文已改**（§13 泄漏门约定 + §19 checklist）。 |
| DB 拓扑 / WAL 作用域 | **D1 的 cluster FSM 开自己独立的 WAL 库文件**（可测构造、对冻结 broker 库零爆炸半径），经新 `storage.OpenWAL`/`WithWAL`，**不翻**共享 `storage.Open` DSN；单库合一 = **D9**。**正文已改**（§3.8）。 |
| 恢复机制 | **采 modernc `NewRestore` 就地拷**（`FSM.Restore` 在 `NewRaft` 内重入，rename 盖开着的 inode 会留 stale `-wal/-shm`）。**正文已改**（§3.8）。 |
| `synchronous` | **`FULL`**（保 `applied_index` 在 §3.7 下持久）；并明示 SIGKILL 测不到掉电区分。**正文已改**（§3.8）。 |
| Poison entry | **结构非法的已提交 entry → 大声 log 并把 `applied_index` 推过去当 no-op**（已提交 entry 是持久的；程序/版本 bug 不得把 FSM 卡死在永久重投）。kill-9 矩阵含 poison case 断言此行为。 |
| §3.5 恢复后活性基线 | **`status=OFFLINE`、`proxy_ready=0`、`last_heartbeat_at=NULL`**。**正文已改**（§3.5）。 |
| 命令编码 | **D1 raft-log `Data` 用 `encoding/json`**；记 `Args []any` 非类型稳定 → 值烤 SQL 字面量；raft-log wire 与 proto-v2 subject SSOT 解耦。**正文已改**（§5）。 |
| 快照节奏（N=1） | 留 `raft.DefaultConfig()` 阈值，测试里 `r.Snapshot()` 强制；§8.2 阈值在后续多节点/catch-up phase pin。 |
| e2e 接线 | **专用 `TestD1Matrix`（带 `-race`），非 `allPhases` 项**（`runPhase` 不带 `-race`/tag 隔离，会重入 kill-9 子进程自 fork）。 |

## 1. 目标与范围（D1-only）

**目标**：在 `internal/cluster` 建**单节点共识心脏**——单节点 hashicorp/raft，其 FSM 把 leader 渲染的 SQL **在同一 txn 里连同 `applied_index`/`applied_term` 一起落 SQLite**（复制写路径**零直连 `db.Exec`**），带快照（modernc online-backup）与恢复（integrity_check + 前向 migrations + 就地 `NewRestore`），并用**真 SIGKILL kill-9 矩阵**证 §3.7 两不变式。D1 只发**一个最小代表 op**（`ClusterMetaSet`）来驱动 FSM；**不迁任何真实 mutator**（D2）、**不做转发/多节点/auth_callout**（D3+）。

**D1 是可测构造的 `internal/cluster` 包——不接 `tetherd`/`broker.New`**。现有真实 mutator 仍直接 `db.Exec` 写；D1 加的是**并行**的 FSM + 其崩溃一致性证明，不改任何现写路径。这是「先父后子」的正确读法。

### D1 / D2 / D3+ 边界表（拒绝 scope creep 的尺）

| 关注点 | **D1（现在）** | **D2（不现在）** | **D3+（不现在）** |
|---|---|---|---|
| op 集 | 仅 `ClusterMetaSet`（test 保留 key 前缀） | §5 全窄类型集 | — |
| Plan/Apply | 骨架接口 + **1** 具体实现 | 全 mutator 迁移 | — |
| 真实 mutator 上 FSM | **无** | 迁移 + N=1 等价（§13.2） | — |
| 确定性雷区（§3.4） | 不碰（op 是烤字面量、零 rand/ulid/time） | leader 烤 time/token/ULID/seq | — |
| 确定性 lint | raft-confinement L-2 翻 live + cluster Apply-import + 活性列自检 | 全 5 包 Apply-可达 + 禁 FSM 外 INSERT + 禁 Apply 调 `*sql.DB` | — |
| 活性列（§3.5） | lint 自检 + 恢复基线重置（证非平凡） | 真 node mutator 守 + 全 G.2 重建 | — |
| applied_index/快照/恢复/kill-9 | **是——心脏** | 仅回归 | — |
| `apply.*` 转发 / `not_leader` / ReqID dedup | 否（字段保留、**惰性**） | 否 | **D4** |
| 多节点/membership/AddVoter/真 transport | 否（InmemTransport、`BootstrapCluster({self})`） | 否 | **D3/D7** |
| auth_callout 本地读 + T_fence | 否 | 否 | **D3** |
| home_broker/epoch/rehome（0010 列） | 否（列在、不用） | 否 | **D6** |
| `tetherd`/`broker.New` 集成 | **无**（仅可测构造；FSM 开自己的 WAL 库文件） | 起步接线 | D9 单库迁移 |

## 2. 设计

### 2.1 Raft node（`node.go`）
- `Node` 包 `*raft.Raft` + 写池 `*sql.DB` + **专用只读** `*sql.DB`（独立 handle，§3.8）+ backup/restore 助手。`New(cfg)` 收 `LocalID`、`DataDir`（`raft/{logs,snapshots}`）、写池、`raft.Transport`、logger。
- Stores：`raftboltdb.New`（LogStore+StableStore，`<DataDir>/raft/raft.db`）；`raft.NewFileSnapshotStore(<DataDir>/raft/snapshots, retain≥1, …)`——D0 pin 的三构造器。
- 仅首启（`raft.HasExistingState`==false）：`BootstrapCluster(conf, …, {Suffrage:Voter, ID:LocalID, Address:self})`。`Config.PreVoteDisabled` 留零值 `false`（D3 前向不变式；**非 D1 测试断言**——N=1 处 vacuous）。
- **Transport 接缝**：测试注入 `raft.NewInmemTransport("")`（D0 `prevote_test.go` 先例）。真 `NetworkTransport`/mTLS StreamLayer 是 **D3**——现在建 listener 是死/未测代码。`Shutdown()` 按确定序关 raft → BoltStore → 只读 handle → 写池（fd/goroutine 卫生）。

### 2.2 FSM.Apply + 同 txn applied_index + 幂等（`fsm.go`/`apply.go`）
`Apply(l *raft.Log) any`：
1. 解码信封；解码/未知 op → **typed error**（poison 策略见 §2.8）。
2. 写池 `BeginTx`（该 txn **就是**单写者）。
3. **txn 内**读本地 `applied_index`（`sql.ErrNoRows ⇒ 0`——D0 发的 `cluster_meta` 空）。
4. **§3.7 不变式 #2 — 已验证幂等 no-op**：若 `l.Index ≤ appliedIndex`，空提交/回滚、返回哨兵 `appliedNoOp{l.Index}`，**不执行 op SQL**。机制：raft 重启**无条件重投** `log[lastSnapshot+1 .. commit]`（无设 raft 内存 lastApplied 的钩子）；FSM 靠**读自己 SQLite 的 `applied_index`** 自跳，绝不信 raft 跳。
5. 否则逐条 `tx.ExecContext` 烤好的 SQL，再**同 txn UPSERT** `cluster_meta` `applied_index=l.Index`、`applied_term=l.Term`。**必须 UPSERT，绝不裸 UPDATE**——D0 空表上裸 UPDATE 写 0 行、`applied_index` 静默永不前进（看着像过的灾难）。对抗测试：空库 apply 1 op，断言 `applied_index` 行**存在且==1**。
6. `tx.Commit()` **完全同步**：commit 返回早于 Apply 返回——这是硬测不变式（§2.4），也是 §3.7 不变式 #1 的保证。
- 路径上**零直连 `db.Exec`**：由 `*sql.Tx`-绑定的 `Applier` 接口编译期强制（§2.7）。

### 2.3 命令编码（`command.go`）
- 信封 `{t:OpType, v:int, r:ReqID, b:[]Statement}`（§5），`encoding/json`。仅注册一个 `OpClusterMetaSet`；未知 OpType → typed `errUnknownOp`。
- 该 op 的 `b` 是一条烤字面量 `INSERT INTO cluster_meta(key,value) VALUES(...) ON CONFLICT(key) DO UPDATE SET value=excluded.value`；key 用 **test 保留前缀**（如 `t:`），不撞保留的 `applied_index`/`applied_term`/`bootstrapped` 行、也不撞真表。
- **`r:ReqID` D1 保留且惰性**——FSM 不查它去重。测试钉惰性：两个**不同 raft index** 带**同 ReqID** ⇒ 都 apply（无 ReqID dedup）；同一 **index** 重投 ⇒ 跳过（index dedup）。§4.1 ReqID dedup 是 **D4**，不得半建。

### 2.4 快照（`snapshot.go`，modernc online-backup）
- 详见正文 §3.8 D1 修正。要点：`Persist` 两段（`NewBackup(临时文件)` + `Step` 循环 **`!more` 才 break** + `defer Finish`；再 `io.Copy(sink, 临时文件)`）；快照文件是非 WAL 独立库；raft v1.7.3 `Persist` 无法设快照 meta.Index，「snapshot.Index ≤ applied_index」**仅靠 §3.7 同步提交时序**。测保证器：`node.Apply(cmd)` 返回后磁盘 `applied_index == entry.Index`（无异步缝）；commit 延后到 Apply-return 之后的对照必须令该不等式**失败**。

### 2.5 恢复（`restore.go`，就地 `NewRestore`）
- 序：① 流 `rc` → 同盘临时**源**文件（`os.CreateTemp(filepath.Dir(dbPath), …)`）；② throwaway handle 跑 `integrity_check`+`foreign_key_check`，非 `ok`/有违 ⇒ 返错、**不恢复**（§13.3 撕裂门）；③ `storage.ApplyMigrations` 前向迁移临时文件；④ 经写池 `Raw` conn `NewRestore(tempSrc)` 就地拷；⑤ `RebuildLiveness` 基线重置（§2.9）。

### 2.6 WAL + 单写者（`internal/storage`）
- 加 `storage.OpenWAL(dsn)`（或 `Open(dsn, WithWAL())`）：DSN 追加 `_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)`；**不翻**共享 `storage.Open`。`SetMaxOpenConns(1)` 写池**原样保留**（load-bearing）。只读 backup handle = **独立 `*sql.DB`**，`?mode=ro`，绝非放松写池。
- 导出 `storage.ApplyMigrations(db *sql.DB) error`（现私有 storage.go:95）供 `internal/cluster` 调真 runner，不复制。

### 2.7 Plan/Apply 骨架（`planapply.go`）
- `Planner interface { Plan(ctx, db *sql.DB, req any) (Op, error) }`（**仅 leader**：读 leader 库、烤字面量）。
- `Applier interface { Apply(tx *sql.Tx, op Op) error }`——**构造上绑 `*sql.Tx`**，类型系统禁 `*sql.DB` Apply 路径（编译期强制 §3.2「零直连」+ §3.7 同 txn）。`var _ Applier = clusterMetaApplier{}`。

### 2.8 Apply 错误分类 / poison entry
FSM.Apply 返回的 typed error 经 `ApplyFuture.Response()` 出（`Error()` 留 nil → `applied_index` **不前进**）。故结构非法的已提交 entry（解码失败/未知 OpType）是 **poison**：返错会把 FSM 永久卡在重投。**D1 策略**：大声 log + 把 `applied_index` 推过去当 no-op（已提交 entry 持久，程序/版本 bug 不得死锁集群）。瞬时基础设施错（Begin/Commit 失败）返错、靠重启+重投自愈。`node.Apply` 同时查 `Response()`（typed FSM 错）与 `Error()`（`ErrLeadershipLost`/`ErrNotLeader`，原样透传给 D4 类型匹配）。kill-9 矩阵含 poison case 断言此行为。

### 2.9 读一致性 + 活性接缝（`read.go`/`liveness.go`）
- 读契约**仅接缝**（§3.2）：`VerifyLeaderRead(ctx, fn)`（先 `raft.VerifyLeader()` 再读）给正确性敏感读；`BoundedStaleRead(fn)`+`AppliedIndex()` 给 ps/history/status。D1 不接任何真消费者（auth_callout=D3、catch-up barrier=D6、force-single=D7）。N=1 处 VerifyLeader 平凡成功——意义在**命名**两条路径让 D2+ 正确挂。单测：N=1 leader VerifyLeader 成功；AppliedIndex 跨 Applies 单调。
- `RebuildLiveness(db)` 在 Restore 末无条件跑基线重置（§3.5，基线 = OFFLINE/0/NULL）。

### 2.10 最小代表 op — 论证
`ClusterMetaSet`（已在 §5 op 集；表 `cluster_meta` 0009 就有；无 FK、无活性列、无 AUTOINCREMENT）是驱动**整条 Apply 路径**（exec op SQL + 同 txn `applied_index` UPSERT + commit）的最小物，不碰任何真实 mutator；纯烤字面量 upsert（零 rand/ulid/time ⇒ 平凡满足 §3.4，给崩溃测确定性 post-Apply 值）。**拒**真窄 op（`PortAllocate` 读 `crypto/rand`+`time.Now()` → 启动 D2）。明标为 D1 测试脚手架、D2 被 §5 类型集取代。

## 3. 文件 / 模块布局

```
internal/cluster/
  doc.go            # 去掉 "NO product code here yet"（D1 LIVE）
  node.go fsm.go apply.go command.go planapply.go clustermeta.go
  snapshot.go restore.go read.go liveness.go testhooks.go
  backup_pin_test.go   # D0 式：Raw conn 满足 NewBackup 接口 + 一次 Step+Finish 回环
internal/storage/
  storage.go        # 加 OpenWAL/WithWAL；导出 ApplyMigrations
  storage_wal_test.go
test/cluster/
  fsm_apply_test.go snapshot_test.go wal_concurrency_test.go liveness_invariant_test.go
test/cluster/kill9/
  kill9_test.go child_main_test.go harness_test.go
test/determinism/
  lint_skeleton_test.go   # L-2 翻 live + 非平凡自检；README 更新
test/e2e/all_phases_test.go   # 加 TestD1Matrix（专用 func，非 allPhases）
```

## 4. 确定性 lint 骨架改动（§13.1）
- L-2 `TestRaftConfinedToClusterPackage` 白名单已含 `internal/cluster`；D1 令其 **live**（首个产品 raft import）。加正向断言：**同一扫描器**确实访问并放行了新产品文件（数它遍历的文件，非独立 walk，使坏遍历不能让正/负向同时 vacuous 绿）。
- 非平凡自检（仿 `TestNoStrayVersionLiteralSelfCheck`）：合成的 `internal/port` raft import **必被旗**；合成的 cluster-path import **必被放**。
- 新 `TestClusterApplyNoNondeterministicImports`：扫 `internal/cluster` 产品文件传递 import，禁 `{crypto/rand, math/rand, oklog/ulid/v2}`（D1 一个不 import ⇒ 真主体绿）。
- 活性列 lint = **静态**非平凡自检（合成 `UPDATE nodes SET status='ONLINE'` 必被旗），非脆的运行期 SQL 串 grep；真 typed 列级守在 D2。
- `TestApplyReachabilityDeterminismLint` 留 `t.Skip`（D2）。更新 `test/determinism/README.md`。

## 5. 测试计划（非平凡是硬要求）

### 5.1 §13.4 kill-9 崩溃一致性矩阵 — 承重门（净新 SIGKILL 设施）
今天无真 SIGKILL harness（`test/chaos` 是干净 cancel）。设计：`TestMain` 里 env-gated 子进程派发（`TETHER_KILL9_CHILD` set 才跑子逻辑并 `os.Exit`）；子进程开**真盘上** WAL 库（非 `:memory:`——崩溃一致性要文件存活）、构 Node、驱动确定性 `ClusterMetaSet` 序列、在受控点被 SIGKILL；父 SIGKILL 后**同 dir 进程内重开**并断言。
- **危险窗口确定性是硬要求**（非 stdout race）：test-only `applyCommitGate` 包变量（prod nil）阻 SQLite commit，使 kill **可证地**落在窗口内；子进程写 breadcrumb 记 "raft committed K、sqlite 未 commit K"。
- **故障点**：FP1（BoltDB 超前 SQLite：raft 已持久 K、SQLite txn 未提交即 kill → 重启 raft 重投 K，首次真 apply）；FP2（SQLite 已提交 `applied_index==K`、快照前 kill → 重投 K 被 §3.7#2 短路为 no-op）；FP3（online-backup `Step` 中 kill → 活库不动，临时 dst 文件重启清）；poison entry。
- **非平凡断言（全部必须）**：(1) 无丢已提交 entry（驱动序 ⇒ 精确期望集，非"有些行在"）；(2) `applied_index` 单调且 `== 最后 COMMAND index`（注意 raft commit index 与之差**索引 1 的 bootstrap config entry**——config entry 不调 FSM.Apply；断言对最后 **command** index，绝不对 raft-last-index；并测 bootstrap-后-首 op 前崩）；(3) **窗口存在证**：FP1 重启前直接读 `BoltStore.LastIndex()` vs `cluster_meta.applied_index`，断 `boltIdx > appliedIdx`；(4) FSM 重投计数器 `>0` 且重投窗口内 per-table 排序哈希不变（证跳过生效且没改内容）；(5) **判别力反向对照（必须）**：一个把 `applied_index` 写在**分离 txn**（违 §3.2）且在 data-commit/index-commit 间崩的对照 FSM **必须让 harness 检出发散（FAIL）**——光计数器不够非平凡，判别力来自坏不变式对照产出可检的错结果；(6) **双 apply 检测器用烤字面量 append**（test-only `UNIQUE(index)` 行表，双 apply 报错或一 index 出 2 行），**不用 `value=value+1` 读改写**（违 §3.4 确定性）。

### 5.2 §13.3 快照-恢复-重放 + 撕裂拒
- happy：驱 N op → `r.Snapshot().Error()` → 新 node Restore → 断**逻辑内容**==源（per-table 排序 SELECT 哈希，§3.6，**不比字节**）；断 FileSnapshotStore 读出的 meta.Index `≤ applied_index`；重放快照后 entry 收敛。
- 撕裂（表）：损坏**承重区**（page-1 头 magic、`sqlite_master` b-tree、有数据的 leaf page），断**每个变体都被拒**（Restore 返错、活库哈希逐字节不变）+ **正对照**（未损 `integrity_check` 返 `ok`）+ **负对照**（free-page/slack 损坏**不该被拒**，证非偏执 always-reject）。**快照文件 size ≥ 活主库 size** 守（抓 `Step` 反相截断）。
- 前向迁移：合成旧 schema 快照（删最新 `schema_migrations` 行 + 0010 列）断 Restore 前向迁移（廉价则做，否则记 D9 真测）。

### 5.3 §13.5 WAL 并发（`-race` + 内建泄漏门）
- 断 cluster 库 `journal_mode=='wal'` 且新 conn `foreign_keys==1`。Goroutine A：紧 `ClusterMetaSet` Apply 环（单写）；B：经只读 handle 反复分页 backup。
- 断言：(a) **写者绝不见 SQLITE_BUSY**（单写池+busy_timeout 覆盖）——**不**断"backup_step 永不 BUSY"（并发 checkpoint 下 `Step` 可合法 BUSY 重启；契约是每个**完成的** backup integrity_check 干净且是真提交前缀）；(b) **交错证**：有 `Step` 在飞时提交的 Apply 计数 `≥` 下限（~N/2），否则"零 BUSY"vacuous；(c) **具体延迟天花板**：p99 Apply `< 100ms`（多秒停顿=被 busy_timeout 吸收的静默串行=fail）；(d) 每完成 backup integrity_check 干净且 `applied_index` 是某真提交前缀；(e) WAL 下 FK 仍强制（违 FK insert 被拒）；(f) 只读 handle 真只读（经它写**失败**，证无第二写者潜入）、写池 `SetMaxOpenConns(1)` 仍 1。
- **泄漏门 = 内建（非 goleak）**：backup 前后 `runtime.NumGoroutine` 回基线（poll-with-tolerance，仿 `test/concurrency/helpers_test.go`）+ **fd 基线门**（`NewBackup` 的无人管 dst conn 仅 Finish 关，漏 Finish 泄 fd，NumGoroutine 抓不到）。`-race` 必带。

### 5.4 支撑单测
空 `cluster_meta` 首 Apply UPSERT seed（断 `applied_index` 行存在且==1）；ReqID-惰性；同步提交（post-`node.Apply` 磁盘 `applied_index==index`）+ 延后提交对照令不等式失败；VerifyLeader/AppliedIndex 接缝；backup-API pin 测（Raw 满足 `NewBackup` 接口 + `Step` 极性 `!more`==done + Finish 回环）；stale `-wal/-shm` 对抗恢复。

## 6. 合并门
- `make test`：cluster 单测（FSM/幂等/快照逻辑等价/VerifyLeader/ReqID-惰性/首 Apply seed）用 InmemTransport + 临时 WAL 库、无 nats-server、快。**meta-test 断裸 `go test ./...` fork 零 kill-9 子进程**（env-gated 派发、非 import-time）。
- `make e2e`：**专用 `TestD1Matrix(t)`**（仿 `TestProxyTunnelReconnectMatrix`，子进程 `-race` + `-run` 过滤跑 cluster+kill-9+快照+WAL 套件），**非 `allPhases` 项**。pin 具体 per-d1 timeout（注释写明、不够再调）。D1 只加 `d1`，不声称补 p11/file-transfer/ps/P12 既有洞。
- `make lint`（golangci-lint v2）新包绿。
- `-race` 覆盖所有并发面 cluster 测。
- 收尾更新 `CLAUDE.md §7` 状态。

## 7. 风险
- **modernc backup/restore API 漂移**：`NewBackup`/`NewRestore` 在未导出 `*conn`，经 `(*sql.Conn).Raw`+接口断言到（`*sqlite.Backup` 已导出，接口可表达）。缓解：D0 式 `backup_pin_test.go` 断接口+一次 Step+Finish 回环；`!ok` 返 typed 错绝不 panic；pin v1.50.0。
- **dst conn fd 泄漏**：`defer Finish/Cancel` 每路径；§13.5 fd 基线门是闸。
- **WAL synchronous vs 崩溃模型**：SIGKILL 是进程死非掉电——矩阵证 OS 可见已提交前缀崩溃一致性、非 fsync/掉电持久；`synchronous=FULL` pin 在复制写路径；不过度声称。
- **e2e 自 fork**：kill-9 子派发仅靠 harness 设的 `TETHER_KILL9_CHILD`；meta-test 确认 `make test` 不 fork 子。
- **恢复 fs**：临时源同 fs；`NewRestore` 就地无 rename，溶解 EXDEV/stale-inode；仍断恢复后同 `*sql.DB` 读到恢复内容。
