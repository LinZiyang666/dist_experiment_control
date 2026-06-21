# D0 plan — distributed-broker HA epic（proto v2 地基）

> **流程状态**：阶段 A 定稿。来源 = 8-agent 对抗 workflow（4 视角起草 → 3 红队对抗互评 → 1 综合）的综合候选 plan，经主进程逐条评估、采纳/驳回、并对 open decisions 拍板。**主进程是唯一定稿人；本文件是 D0 实现的唯一实现尺**（契约细节以 `docs/distributed-broker-architecture.md` 正文为准，§ 引用内联）。
>
> **本 plan 触发的「先改文档再改代码」修正已先行落盘**（见 §0）：架构正文 §3.1/§19/§18.2.14（PreVote 字段名）、§4.2/§8.1（phase 枚举大小写）已改。
>
> 语言：中文叙述；代码/标识符/subject/表名/配置键英文（CLAUDE.md §5）。

---

## §0. 已落盘的文档前置修正（doc-first，本 plan 触发）

阶段 A 实测发现两处正文与真实代码/自洽性冲突，按 checklist「实现中发现设计问题先改文档再改代码」**已先改架构正文**：

1. **PreVote 字段名修正**（§3.1 line 64、§19 line 485、§18.2.14 line 399）：hashicorp/raft v1.7.x **无 `Config.PreVote` 字段**；真实字段是 `Config.PreVoteDisabled bool`，`DefaultConfig()` 不设它（零值 false）→ pre-vote **缺省启用**。原文 `确认 Config.PreVote 字段在` 会写出一个**编译不过**的断言。已改为 `Config.PreVoteDisabled`，并补「transport 不实现 `WithPreVote` 时 raft 静默关闭 pre-vote」这一隐患（实测门须同时核验 transport 能力）。

2. **phase 枚举大小写归一**（§4.2 line 119、§8.1 line 204）：原文 `phase` 列混用大写（`VOTER_ADD_FAILED`）与小写（`draining`/`retiring`），单条 SQL `CHECK` 无法同时匹配。已归一为**全大写** canonical 6 值：`JOIN_VERIFIED_PENDING_VOTER` / `CATCHING_UP` / `VOTER` / `VOTER_ADD_FAILED` / `DRAINING` / `RETIRING`。（注：alert kind `broker_draining`/`broker_down` 是**另一命名空间**，仍小写 snake_case，不动。）

---

## §1. D0 范围与显式不做（先父后子边界）

**目标（§19）**：依赖 + wire/schema 地基就位，**零运行期行为变化**（N=1 单点 broker 与 v0.3.5 功能等价）。

**D0 做（仅这 5 个面）**：
1. **依赖** pin `hashicorp/raft` + `raft-boltdb/v2`，核验 CGO-free 静态编译 + `FileSnapshotStore` 编译可达。
2. **wire 常量** proto SSOT 翻 v2（`ProtoVersion=2`、`SubjectPrefix="tether.v2"`），**消息结构体形状逐字节不动**。
3. **schema** migrations 0008/0009/0010——纯 DDL，无 Apply/backfill/writer。
4. **lint 骨架** §13.1 确定性 lint 脚手架（可跑、warn-not-fail 基线）——**非**全 Apply-可达 lint。
5. **依赖验证测试** PreVote 实测合并门（inmem，无产品 FSM）。

**D0 不做（越界即返工）**：
- ❌ Raft FSM / `cn.Apply` / Apply 路径 / 写转发 / `applied_index` 写逻辑 → **D1**（§3.2）。
- ❌ 数据面 home/rehome、`tunnelTokenLookup` home==self 变体、epoch 比对 → **D6**（§7.2）。
- ❌ REGISTER 第 6 字段（epoch）、`HomeDirective`/`cert_pins` 结构体 → **D6**（§7.2b/§15）。
- ❌ `cluster.apply.*` / `cluster.*` 新 subject builder → D3/D4。**D0 的"v2 grammar SSOT" = 把现有 v1 subject 树逐字移到 v2 前缀，零新 subject 形状**。
- ❌ 双栈（v1+v2 并存）——是 flip 不是 add（§4）。
- ❌ `BoltStore`/`FileSnapshotStore` 产品接线、Raft node 产品封装 → D1。`internal/cluster/` 在 D0 **只**放 PreVote 测试 + doc.go。
- ❌ determinism-lint 的 Apply-可达图（D0 无 FSM/Apply 根）→ **D2**（§13.1）。
- ❌ alert/cluster_meta 的 writer 或 seed 行 → D1（cluster_meta upsert）/ D8b（alerts writer）/ D9（`--from-existing` seed）。

---

## §2. 依赖 pin + CGO-free 编译核验

**pin（workflow 红队已实编译核验：CGO_ENABLED=0、Go 1.25.0）**：
- `github.com/hashicorp/raft v1.7.3`
- `github.com/hashicorp/raft-boltdb/v2 v2.3.1`
- 传递：`go.etcd.io/bbolt v1.3.5`（**纯 Go，零 `import "C"`**；`unsafe.go` 是 `unsafe.Pointer` 算术非 cgo）。**D0 不 bump bbolt**（YAGNI，最小面；若 D1 用 BoltStore 撞 Go 1.25 问题再 bump 并登记为 D1 偏离）。

引入的 indirect require：`hashicorp/go-hclog`、`hashicorp/go-metrics` + `armon/go-metrics`、`hashicorp/go-msgpack/v2`、`hashicorp/go-immutable-radix`、`hashicorp/golang-lru`、`bbolt`、`fatih/color` + `mattn/go-colorable`。`mattn/go-isatty` 保持 v0.0.20（MVS 高者胜，不降级）。
> **实现偏离登记（内审 n1）**：`hashicorp/raft` 与 `raft-boltdb/v2` 最终落在 go.mod 的 **direct** require 块（非本节原预期的 `// indirect`）——这是 §3/§19 强制的「依赖验证编译引用」的必然结果：`internal/cluster/deps_test.go` **直接 import** 二者（以钉 `FileSnapshotStore`/`BoltStore` 并让 `go mod tidy` 保住 raft-boltdb/v2）。`go mod tidy` 干净、无漂移。

**依赖审计纪律（采纳红队 R1，重要）**：`go get raft-boltdb/v2@v2.3.1` 会在 `go mod graph` 留 **陈旧图节点** `boltdb/bolt v1.3.1` + `go-msgpack v0.5.5`，但 `go build` 永不编译它们。**判定 CGO-free 一律用 `CGO_ENABLED=0 go build ./...` + `go list -deps`，绝不用 `go mod graph`**（避免审查者扫 go.sum 时虚惊）。

**编译/提交门（红队确认的缺口）**：
- `make build` **只 build `./cmd/tether`，不是 `./...`**——故 §19 出口的「`go build ./...` 绿」**不被 `make build` 覆盖**。D0 提交门须显式跑 **`CGO_ENABLED=0 go build ./...`**（覆盖新增 `internal/cluster` 测试依赖 + raft）。
- `go.mod` + `go.sum` 同 PR 提交；`go mod tidy` 须干净（列为 D0 闸项）。

---

## §3. PreVote 实测合并门设计（D0 最重门，§3.1/§18.2.14）

**前提**：§0 的字段名修正已落盘。门的实现尺以修正后的 §3.1/§18.2.14 为准。

**放置**：`internal/cluster/prevote_test.go`（+ `internal/cluster/doc.go` 标注「D0 依赖验证专用、无产品 FSM」，并承载 `FileSnapshotStore` 编译引用，见下）。理由：纯 inmem 依赖验证**单测**（无 nats、无磁盘）→ 归依赖属主旁、跑在 `make test` 快道；`test/cluster/` 留给 D7 集群 e2e，不预占。

**fixture（红队 3 实跑过：`-race`、~2s、不 flaky）**：
- 3 节点 a/b/c：`raft.NewInmemTransport(addr)` + `raft.MockFSM{}` + `raft.NewInmemStore()` + `raft.NewInmemSnapshotStore()`（不用 BoltStore/FileSnapshotStore——D0 证 PreVote 不需要持久化）。
- 自洽超时：`HeartbeatTimeout=ElectionTimeout=LeaderLeaseTimeout=50ms`、`CommitTimeout=5ms`（亚秒选举）。
- `BootstrapCluster`，**轮询**（非定时 sleep）拿 leader（3s deadline），记 `termBefore = leader.CurrentTerm()`。
- 隔离一个 follower F：**双向** `Disconnect`（每 peer 对 `F.Disconnect(p)` **且** `p.Disconnect(F)`），sleep ~1.5s（数个选举超时）让 F 的 pre-vote campaign 失败（`tally=1 refused=2 votesNeeded=2`）。
- 重连 F（双向 `Connect`），sleep ~500ms 收敛。
- **判别断言（D0 实测修正，详见 §18.2.14 改文档）**：断言 **`f.CurrentTerm()==termBefore`**——pre-vote 拦住被隔离 follower 的 term 自增，**这是 pre-vote 的直接效果、是判别信号**。次级断言：`leader.State()==raft.Leader` 且 `leader.CurrentTerm()==termBefore`（不下台、不被扰动）。**注**：判别信号取**被隔离节点自身**的 term，**非** leader 的 term——inmem 实测下 leader-term 在启用/禁用两种配置都不被扰动（rejoin 扰动不在此 harness 内确定性复现），故 leader-term 不变虽真但无判别力。

**三件强制控制断言（§18.2.14「字段在≠行为生效」的具体化）**：
1. **transport 能力编译断言**：`var _ raft.WithPreVote = (*raft.InmemTransport)(nil)` + 注释。raft v1.7.3 `api.go` 会 `preVoteDisabled = conf.PreVoteDisabled || !transportSupportPreVote`——transport 不实现 `WithPreVote` 则 pre-vote **静默 OFF**、门假过。已核验 `InmemTransport` 实现了它（`RequestPreVote`）。
2. **反向对照子测试**：兄弟用例 `cfg.PreVoteDisabled = true`，断言**被隔离 follower 的 term 反而抬高**（`f.CurrentTerm() > termBefore`，inmem 实测抬到 20+）——证明门真在测 PreVote、非恒真 no-op。
3. **`FileSnapshotStore` 编译引用**：§19 把 `FileSnapshotStore` 列入 3 个 pin 项，但 PreVote 测试用 Inmem。**实现落点（内审 n1）**：编译引用放在 `internal/cluster/deps_test.go`（而非 `doc.go`——`doc.go` 保持 import-free），含**类型钉** `var _ *raft.FileSnapshotStore`（D1 消费的就是该 struct）**+ 构造器钉** `var _ = raft.NewFileSnapshotStore`（§19 字面点名、抓 API rename），并一并钉 `*raftboltdb.BoltStore`/`Options` 以让 `go mod tidy` 保住 raft-boltdb/v2。

**不用 goleak（驳回起草建议）**：goleak **非**本项目依赖（go.sum 无；`test/concurrency/helpers_test.go` 明写「deliberately avoid the goleak library」）。在最小依赖地基 phase 引入它违反项目惯例 + CLAUDE.md §5（leak 门只圈 tunnel/PTY/reconcile/transport，非 D0 inmem）。改用 `t.Cleanup` → 每节点 `Shutdown().Error()` + 关 transport；跑 `-race`（raft 多 goroutine，已核验过）。

---

## §4. proto v2 SSOT —— flip + derive-from-SSOT（主进程裁定）

**裁定：flip（不是 add）+ 把版本 token 收敛成真 SSOT**。`ProtoVersion=2`、`SubjectPrefix="tether.v2"`，**消息结构体形状逐字节不变**；破坏性 wire 字段（REGISTER epoch、`HomeDirective.cert_pins`）留 **D6**。

**为何 flip 而非 add**：§16.2 / 需求 §10.1 命定「proto v1→v2 **硬升级**、不兼容老 agent、协调全机群重装」——add 的唯一价值是 v1/v2 滚动并存，而需求基线**明确拒绝**（全机群重装）。add 是纯负债；且 add 需版本协商分支 = **净新增运行期逻辑** = 违反「无运行期行为变化」。flip 后 broker/agent/ctl 同为 2、握手 strict same-version 自洽。

**为何 flip 满足「无运行期行为变化」**：§0 是**功能等价**非字节等价。flip 只改两个标签串（版本整数 1→2、subject 根）；69 个 wire 结构体的字段集/JSON tag/编解码语义全同。新建 v2 二进制、broker↔自家 v2 agent 两端皆 2，握手与行为逐功能等价于今天 v1↔v1。

**SSOT 收敛裁定（采纳红队 3，强于综合者的"minimal literal-flip"）**：D0 的官方任务字面是「**建 tether.v2 subject grammar SSOT**」。真 SSOT = 版本 token 只活在**一处**；让 7 个 parser 各硬编码 `"v2"` 不是 SSOT、只是改名。故 D0 **做收敛重构**（行为保持）：
- `internal/proto/version.go` 引入 `SubjectVersionToken = "v2"`，`SubjectPrefix = "tether." + SubjectVersionToken`、`ProtoVersion = 2`。
- `internal/proto/subjects.go` 的 **7 个 `Parse*` 的 `parts[1] != "v1"` 改为 `parts[1] != SubjectVersionToken`**（ParseTransferFinalize/ParseEvTransfer/ParseEvProc/ParseCmdBy/ParseCtrlBy/ParseSidNidFromCtrl/ParseCtrlProxy）；doc-comment 例子里的 `tether.v1.*` 同步更新（注释，不影响行为）。
- `internal/agent/agent.go:477` 的 `fmt.Sprintf("tether.v1.s.%s.cmd.node.%s.*.req.forwarded", …)` **改为调用 builder** `proto.SubjCmdForwarded(sid, nid, "*")`（彻底消除该 off-SSOT 字面量 + 永久消除「漏改前缀致 agent 订错 subject」运行期致命脚枪）。

**唯一保留的 off-SSOT 字面量**：`internal/auth/permissions.go:9` 的 `const subjectPrefix = "tether.v1"`——这是**故意的 import-cycle 规避副本**（注释明示），不能 collapse 进 proto。改为 `"tether.v2"`，靠该包内 **static-guard 测试**（`TestSubjectPrefixInSyncWithProto`，断言 `subjectPrefix == proto.SubjectPrefix`）保持同步。**这是 D0 唯一合法重复点。**

> **proxy parser 说明**：`ParseCtrlProxy` 在 7 个里——它随前缀移植翻 v2（机械、无行为），**不是**把 proxy 重纳入 v1 HA（proxy 仍按 §0/§3.3 在 v1 HA 外、不进 lint 目标集）。

**必须同 PR 翻的测试（已核验）**：
- `internal/proto/proto_test.go` `TestSubjectPrefixStable`（硬断言 `=="tether.v1"`，flip 后**真失败**）+ subject golden 表 `tether.v1`→`tether.v2`。
- `internal/proto/proto_invariants_test.go` `TestProtoVersionStillPositive`：现守卫 `!HasSuffix(.v1) && ProtoVersion==1`，flip 后 `&& ProtoVersion==1` 转 false → **整体 vacuously PASS、不报警**（综合者纠正了"一跑就红"的错判）。**必须主动重写**为版本无关不变式：`wantSuffix := ".v" + strconv.Itoa(ProtoVersion); if !strings.HasSuffix(SubjectPrefix, wantSuffix) { t.Fatalf(...) }`——任何不匹配即红、未来 bump 不用再改。再加 `SubjectPrefix == "tether." + SubjectVersionToken` 与 `SubjectVersionToken == "v"+strconv.Itoa(ProtoVersion)` 两条 SSOT 自洽断言。
- `internal/auth/permissions_test.go` `TestSubjectPrefixInSyncWithProto`（两侧同 PR 翻 v2 才保持绿）+ 注释里 `"tether.v1"` 更新。
- 4 处 `ProtoVersion: 1` 字面量（3 在 proto_invariants_test.go、1 在 node_test.go）：roundtrip 版本对称，留着**不**破 `TestRoundtripAllMessages`——翻 2 是为清晰，**非**正确性门。
- ~17 个 `_test.go` 含硬编码 `tether.v1` subject 串（`internal/proto`、`internal/auth`、`internal/authcallout`、`internal/broker`、`test/concurrency`、`test/security`、`test/p3`）——机械 `tether.v1`→`tether.v2` 扫；grep 收集；收口 = `make test` + `make e2e` 绿。

**proto v2 golden JSON 回环（§19）**：新 `internal/proto/golden_test.go` + `internal/proto/testdata/golden/*.json`。目的 = 钉「v2 形状 == 今天 v1 形状」（证 flip 没夹带 wire 改动 + 给 D6 基线 diff）。表驱动、每 wire 结构一行、**确定性 fixture**（固定 `time.Date(...,UTC)`、固定 `[]byte`、固定 int——无 `time.Now()`/rand/map 迭代），`-update` flag 再生。双向：`Marshal(fixture)==golden` **且** `Unmarshal(golden)→reMarshal==golden`。**不**含 `HomeDirective`/epoch（D6，避超前）。对抗 fixture：非 UTF-8 `[]byte`、NUL 字节、`[]string{}` vs `nil`（JSON `[]` vs `null`、omitempty）；一条负向：v1 前缀 subject 喂 v2 parser 返回 `ok=false`。

---

## §5. migrations 0008 / 0009 / 0010 —— 完整 DDL

> 引擎不变（`storage.go`：embed + `sort.Strings` + 每文件独立 txn + 多语句 `tx.Exec`，均已核验）。**cluster/alert 系表零 `CURRENT_TIMESTAMP` 默认**（§4.2：leader 烤时间字面量保确定性）；新 ALTER 列只用**确定性常量**默认。**D0 只建 schema、零 writer/seed**（先父后子）。

### 0008_cluster_nodes.sql
```sql
-- Migration 0008 — cluster_nodes roster (distributed-broker §4.2).
-- Raft voting set is authoritative; `phase` is a DERIVED display state (§8.1).
-- NO column carries a CURRENT_TIMESTAMP default: every value is leader-baked
-- at Apply time so all replicas converge byte-for-byte (§3.4/§4.2).
-- D0 only creates schema; first writer is D7 ClusterNodeUpsert. Empty until
-- `cluster init --from-existing` (D9). Applies cleanly on a pre-cluster DB.
CREATE TABLE cluster_nodes (
    node_id              TEXT      PRIMARY KEY,   -- == Raft ServerID (stable)
    name                 TEXT      NOT NULL,
    node_ident_pub       TEXT      NOT NULL,      -- node-identity nkey pub (≠ bus nkey)
    nats_server_id       TEXT,                    -- deterministic nats-server id; agent self-reports to bridge home (§6.5); NULL until D3
    raft_addr            TEXT      NOT NULL,
    nats_route           TEXT      NOT NULL,
    tunnel_addr          TEXT      NOT NULL,
    public_host          TEXT      NOT NULL,
    cert_fp              TEXT      NOT NULL,       -- current tunnel-cert fp (§15 RF3)
    cert_fp_prev         TEXT,                     -- non-null only during rotation
    cert_fp_valid_until  TIMESTAMP,                -- leader-baked literal; rotation window only
    phase                TEXT      NOT NULL
                                   CHECK (phase IN (
                                       'JOIN_VERIFIED_PENDING_VOTER',
                                       'CATCHING_UP', 'VOTER', 'VOTER_ADD_FAILED',
                                       'DRAINING', 'RETIRING'
                                   )),
    added_at             TIMESTAMP NOT NULL,       -- leader-baked literal (NO default)
    UNIQUE (name)
);
CREATE INDEX idx_cluster_nodes_phase ON cluster_nodes(phase);
```
phase CHECK = §0 归一后的 canonical 6 值（全大写）。

### 0009_cluster_meta_alerts.sql
```sql
-- Migration 0009 — cluster_meta KV + replicated alerts (single cluster-level ack).
-- cluster_meta = Raft FSM KV sidecar (applied_index/applied_term/bootstrapped, §11).
-- Per §18.3: NO cluster_revoked_identities table; alert ack is a SINGLE
-- cluster-level ack (PK = dedup_key alone; acked_by is display-only).
-- NO CURRENT_TIMESTAMP defaults (leader bakes literals, §3.4). D0 = schema only,
-- NO seed rows: cluster_meta(applied_index,bootstrapped) is written by D1 Apply
-- (upsert, §3.2) / D9 --from-existing seed (§11); alerts writer arrives in D8b.
-- alerts.kind CHECK is the REPLICATION-STORE-BACKED set only: the client-synthesized
-- severe gating kinds quorum_lost/force_single_active are NEVER written here (§10.4).
CREATE TABLE cluster_meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
CREATE TABLE alerts (
    id          TEXT      PRIMARY KEY,            -- ULID, leader-baked (§3.4)
    kind        TEXT      NOT NULL
                          CHECK (kind IN (
                              'manual','below_quorum','broker_draining',
                              'broker_down','replication_degraded',
                              'disk_pressure','raft_lag'
                          )),
    severity    TEXT      NOT NULL CHECK (severity IN ('severe','info')),
    dedup_key   TEXT      NOT NULL,
    state       TEXT      NOT NULL DEFAULT 'ACTIVE'
                          CHECK (state IN ('ACTIVE','CLEARED')),
    message     TEXT      NOT NULL,
    raised_at   TIMESTAMP NOT NULL,               -- leader-baked literal (NO default)
    cleared_at  TIMESTAMP
);
CREATE UNIQUE INDEX idx_alerts_dedup_active ON alerts(dedup_key) WHERE state = 'ACTIVE';
CREATE TABLE alert_acks (
    dedup_key  TEXT      NOT NULL,
    acked_by   TEXT      NOT NULL,                -- display only (§18.3), not per-identity
    acked_at   TIMESTAMP NOT NULL,               -- leader-baked literal (NO default)
    PRIMARY KEY (dedup_key)
);
```
**alerts.kind 裁定（采纳红队，§10.4）**：CHECK = **7 个复制存储承载 kind**：`{manual, below_quorum, broker_draining, broker_down, replication_degraded}`（§10.4 line 258 复制存储集）+ `{disk_pressure, raft_lag}`（§10.2 目录 info kind，D8b 可能晋升进存储——前向预留）。**排除 `quorum_lost`/`force_single_active`**：它们是客户端合成（quorum 丢时无法 Raft 写，§10.4），复制存储里**无 writer**。`severity` CHECK = `{severe, info}`（§10.2 目录）。

**alerts.id 裁定**：`id` = ULID TEXT PK（leader-baked，§3.4）+ `dedup_key` partial-unique-active——镜像 0003 port_allocations 的 history-rows 模型；§4.2 line 121 显式给了 `idx_alerts_dedup_active`。

**cluster_meta seed 裁定（解红队 open item）**：D0 建**空** KV 表、**不**预 seed 任何行。§3.2 每次 Apply（D1）在同 txn upsert `applied_index`（INSERT-on-first-Apply）；§11 `--from-existing`（D9）seed `applied_index=0,bootstrapped`。故 D0 空表对 D1/D9 都够（D1 upsert、D9 seed），先父后子正确。

### 0010_port_cluster_columns.sql
```sql
-- Migration 0010 — port_allocations cluster columns (§4.2, §7.1–7.3).
-- ALTER ADD COLUMN, NOT a table rebuild: preserves row_id AUTOINCREMENT (§3.6/§4.2)
-- and leaves every historical default untouched (§4.2).
-- New-column defaults are deterministic CONSTANTS (§3.4): home_broker='' (D9
-- --from-existing backfills =self for live rows; D0 must NOT backfill — 先父后子),
-- rebuild_on_failure=1 (default ON, §7.3), epoch=0 (ReassignHome advances, §7.2).
-- No writer fills home_broker/epoch in D0; the tunnelTokenLookup home==self/epoch
-- variant lands in D6, the one-time backfill in D9 (§11).
ALTER TABLE port_allocations ADD COLUMN rebuild_on_failure INTEGER NOT NULL DEFAULT 1
    CHECK (rebuild_on_failure IN (0, 1));
ALTER TABLE port_allocations ADD COLUMN home_broker TEXT NOT NULL DEFAULT '';
ALTER TABLE port_allocations ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0;
-- Serves tunnelTokenLookup's home==self filter (§7.1–7.2). NON-UNIQUE: port
-- uniqueness already held by 0003/0004's active-port unique index.
CREATE INDEX idx_port_alloc_home_active
    ON port_allocations(home_broker, port) WHERE state = 'ALLOCATED';
```
`home_broker DEFAULT ''`（**非** `self`——D7 前无 "self" 概念；D9 backfill）。新索引**非唯一**（port 唯一性已由既有 active-port 唯一索引持有；实现时核验既有唯一索引确切名再写注释）。

---

## §6. §13.1 确定性 lint 骨架边界

**D0 = 脚手架 + 静态可判定子集；非全 Apply-可达 lint**（§13.1 列 4 子检查，全 lint 是 D2；line 568 它从 D1/D2 起持续生效）。D0 无 FSM/Apply 根可遍历。

| §13.1 子检查 | D0 形态 | 理由 |
|---|---|---|
| 解析目标包 `{port,proc,node,session,agentprov}` | **build、跑绿** | 包今天就在、AST 可解析 |
| 禁用 import `{crypto/rand, math/rand, oklog/ulid/v2}` 探测 | **扫描 + 断言 == 已知白名单（warn-not-fail）** | 真命中：`port/port.go` crypto/rand、`proc/proc.go` oklog/ulid/v2；今天合法（无 Plan/Apply 拆分）；强制 fail 会误杀 = 越界 |
| Apply-可达谓词 | **stub、`t.Skip`** | D0 无 FSM/Apply 根（§13.1 line 77/295）；命名 D2 接续点 |
| 禁 FSM 外 INSERT Apply-owned 表 / 禁 Apply→`*sql.DB` mutator / 列级断言 | **不实现** | Apply-owned 集 D2（op 集 §5）定 |

**D0 能落的两个有牙真检查**（骨架非 100% TODO）：
1. **禁用 import 白名单基线（L-1）**：断言违规集 `== {(port, crypto/rand), (proc, oklog/ulid/v2)}` 精确相等。新越界 import → 红；移除 → 红。`// TODO(D2): tighten to t.Fatal once Plan/Apply split lands`。单调收紧、非"D0 空→D2 从零"。
2. **raft-confinement 护栏（L-2）**：扫 `internal/...`+`cmd/...`，断言**无产品包** import `hashicorp/raft`/`raft-boltdb`——**白名单 `internal/cluster` 的 `_test.go`** 为唯一合法 raft importer（否则撞 §3 的 PreVote 测试）。机械执行 §19「❌ D1 之前碰 apply.*」、抓 D0 里把 raft 接进 broker/port 的人。

**tripwire `TestNoStrayVersionLiteral`**（配合 §4 SSOT 收敛后）：禁 SSOT 外的 `tether.v1`/`tether.v2` **字符串字面量**。因 §4 做了 derive-from-SSOT 重构（7 parser + agent.go 都不再含字面量），非测试字面量点收敛为 **2 个**：`internal/proto/version.go`（SSOT）+ `internal/auth/permissions.go`（import-cycle 副本）。**只扫 string-literal AST 节点、排除注释**（故 `subjects.go`/`jsstream.go` 的注释例子不误报）。白名单 = `{version.go, auth/permissions.go}`。

**技术选型**：`test/determinism/lint_skeleton_test.go`（新目录，与 `test/concurrency/` 平级）。标准库 `go/ast`+`go/parser`+`go/token`，`parser.ParseDir(..., parser.ImportsOnly)`——**不引** `golang.org/x/tools/go/packages`（今天无此依赖；D0 只需 import 级判定；不预支 D2 调用图依赖决策）。`targetPkgs`/`bannedImports` 为硬编码常量逐字对齐 §13.1/§3.3（proxysub 按 §0/§3.3 排除；math/rand 当前零命中、为 D2 前向）。`make lint`（golangci-lint v2）不受影响——determinism-lint 是 Go 测试、非 golangci 自定义插件（YAGNI，§18.3「lint 只是 tripwire」）。

---

## §7. 测试门清单

| 门 | 交付物 | 通过判据 | 锚 |
|---|---|---|---|
| determinism-lint 骨架可跑 | `test/determinism/lint_skeleton_test.go` | 绿；L-1 白名单 + L-2 raft-confinement 命中真代码；Apply-可达 stub `t.Skip` 注册 D2 接续点 | §13.1/§19 |
| migrations 0008–0010 前向幂等 | `internal/storage/cluster_migrations_test.go`（白盒 `package storage`） | 引擎按名只跑一次；重复应用无错 | §4.2/§19 |
| migrations introspection/CHECK/FK/无-CURRENT_TIMESTAMP/无-cluster_revoked_identities | 同上 | 见下对抗清单 | §4.2/§18.3 |
| proto v2 golden 回环 | `internal/proto/golden_test.go` + testdata | 字节稳定、确定性 fixture | §19 |
| proto 不变量重写 | `TestProtoVersionStillPositive` → 版本无关 | 任何版本/后缀不匹配即红 | §16.2 |
| PreVote 实测生效 | `internal/cluster/prevote_test.go` | leader term 不涨/不下台；transport-cap 断言；反向对照涨 term；`-race` | §3.1/§18.2.14 |
| `go build ./...` 绿 CGO-free | 显式 `CGO_ENABLED=0 go build ./...`（**非** `make build`） | 过 | §3.1/§19 |
| **现有 e2e 矩阵回归** | 跑 `make e2e`（**不**加新 phase） | p1–p13 绿 = v2-flip 行为等价证明 | §9 |

**对抗 migration 测试（采纳红队强化）**：
- 负向：`cluster_revoked_identities` **不**存在；cluster/alert 表 DDL **无** `CURRENT_TIMESTAMP`（扫 `sqlite_master` 子串）；对照 `port_allocations.created_at` **有**它（别误统一）。
- CHECK：`phase='BOGUS'` 拒、6 canonical 值过；`alerts.kind='not_a_kind'`/`severity='critical'` 拒、7 kind + 2 severity 过；**`alerts.kind IN ('quorum_lost','force_single_active')` 被拒**（证客户端合成 kind 非存储承载）。
- partial-unique：两条 ACTIVE 同 `dedup_key` → 第二条拒；ACTIVE+CLEARED 同 key → 允许。
- `alert_acks`：两 INSERT 同 `dedup_key`（即便不同 `acked_by`）→ PK 冲突（证非 per-identity）。
- NOT NULL 无默认：插 `cluster_nodes` 漏 `added_at` → 失败（证无默认兜底、值须 leader-baked）；`alerts.raised_at`、`alert_acks.acked_at` 同。
- ALTER 默认：插 `port_allocations` 漏新列 → `rebuild_on_failure==1, home_broker=='', epoch==0`（§7.3/§4.2 良性默认门）。**更强变体（采纳红队 3）**：白盒驱动引擎只跑 0001–0007 → 插一条真行 → 再跑 0008–0010 → 断言既有行 backfill 成 `1/''/0`（真·生产升级路径）。引擎 `applyMigrations` 按排序名迭代，白盒可按文件子集逐条应用构造此 0007→0010 时序；若 API 不便子集应用则**显式文档化残留 gap**（不假称等价）。
- post-0010 回归：`port.Allocate`（或其 INSERT 列表）仍工作；`PRAGMA foreign_key_check` 空；`integrity_check==ok`；既有 active-port 唯一索引 + FK-to-nodes 在已有 0003 行的库上仍完好。

**migration 测试放置裁定**：白盒 `internal/storage/cluster_migrations_test.go`（`package storage`，可调未导出 `applyMigrations`，循 `p13_generation_migration_test.go` 先例）承载 introspection/CHECK/FK-negative/无-CURRENT_TIMESTAMP/子集-backfill；如需 restart-idempotency（file DB 重开）可另在黑盒 `test/storage/`——**合并不重复**。

---

## §8. 文件改动清单

**新增**：
- `internal/storage/migrations/0008_cluster_nodes.sql`、`0009_cluster_meta_alerts.sql`、`0010_port_cluster_columns.sql`
- `internal/storage/cluster_migrations_test.go`（白盒 `package storage`）
- `internal/proto/golden_test.go` + `internal/proto/testdata/golden/*.json`
- `internal/cluster/prevote_test.go` + `internal/cluster/doc.go`（含 `FileSnapshotStore` 编译引用）
- `test/determinism/lint_skeleton_test.go`（+ `README.md` 注 D0-build / D2-full 边界）

**修改**：
- `docs/distributed-broker-architecture.md` — **已先行落盘**（§0：PreVote 字段名 §3.1/§19/§18.2.14；phase 枚举大小写 §4.2/§8.1）。
- `internal/proto/version.go`（`ProtoVersion=2`、`SubjectVersionToken="v2"`、`SubjectPrefix="tether."+SubjectVersionToken` + doc 注释）
- `internal/proto/subjects.go`（7 个 `Parse*` 的 `parts[1]` 比对改 `SubjectVersionToken`；doc-comment 例子 v1→v2）
- `internal/auth/permissions.go`（`subjectPrefix` → `"tether.v2"`）
- `internal/agent/agent.go`（:477 改调 `proto.SubjCmdForwarded(sid, nid, "*")`）
- `internal/jsstream/jsstream.go`（注释 `tether.v1`→`v2`）
- `internal/proto/proto_test.go`（`TestSubjectPrefixStable` + golden 表）
- `internal/proto/proto_invariants_test.go`（`TestProtoVersionStillPositive` 版本无关重写 + `ProtoVersion:1`→`2`）
- `internal/auth/permissions_test.go`（`TestSubjectPrefixInSyncWithProto` + 注释）
- `internal/node/node_test.go`（`ProtoVersion:1`→`2`）
- ~17 个 `_test.go`：机械 `tether.v1`→`tether.v2` 扫（grep 收集）
- `go.mod` / `go.sum`（raft v1.7.3 + raft-boltdb/v2 v2.3.1 + indirects；`go mod tidy` 干净）

**不动**：`Makefile`（`./...` 自动覆盖新单测；显式 `CGO_ENABLED=0 go build ./...` 是提交门命令非 Makefile 改动，除非要加 target）；`internal/proto/messages.go`（69 wire 结构体——「无行为变化」的本体）。

---

## §9. 出口断言核对表（§19）

| §19 D0 出口 | 由谁满足 | 证据 |
|---|---|---|
| raft 依赖 CGO-free 编译过 | §2 pin + 显式 `CGO_ENABLED=0 go build ./...` | build 绿（红队已实证） |
| **PreVote 实测生效**（分区+重连：健康 leader term 不涨/不下台） | §3 `internal/cluster/prevote_test.go` | 红队实跑 `-race` PASS；transport-cap + 反向对照证判别力 |
| `Config.PreVoteDisabled` 字段在（§0 修正后） | §3 编译期 + DefaultConfig | 字段在 v1.7.3 `config.go` |
| `FileSnapshotStore` pin/编译 | §3 编译引用 | `var _ = raft.NewFileSnapshotStore` |
| proto v2 SSOT + `ProtoVersion=2` | §4 flip + SSOT 收敛 + tripwire | golden + 版本无关不变式 |
| 0008–0010 内存库前向跑通 | §7 | introspection + 幂等 + 子集-backfill 测试 |
| **无运行期行为变化（N=1 等价）** | §9 e2e 矩阵 p1–p13 用 v2 二进制全绿 | 同二进制、两端 v2、same-version 握手 = 功能等价证明 |

**e2e 裁定**：D0 **不加新 e2e 矩阵 phase**（地基 phase，同 P0「scaffold only, no e2e」），**但** D0 提交门**必须跑现有 `make e2e`** 作回归网——因 v2 flip + off-SSOT 硬编码（`auth/permissions.go`、`agent.go`、`subjects.go` parser）是 `make test` 看不见的**运行期 ACL/parser 失步**，只在真 broker↔agent 路径暴露；这次 e2e 跑也是「无行为变化」的具体等价证据。

**提交硬闸**：`make test` + `make lint` + `CGO_ENABLED=0 go build ./...` + `make e2e` 全绿；PreVote 测试过 `-race`；`go mod tidy` 干净。

> **出口门语义（原则，外审 F1 后回写）**：「全绿」原则上指 **D0 引入零新失败（HEAD 对照证实）+ 所有 D0 相关门绿**——一个 foundation phase 不应被全仓无关 pre-existing 失败永久阻塞。**但本轮按用户要求把发现的两个 pre-existing gate 失败一并修绿，故 D0 的门此刻字面全绿、不依赖该豁免**：
> - **`test/p6`（已修，真生产 bug）**：`internal/node` 的 `Register`/`Heartbeat` 把 `last_heartbeat_at`/`registered_at` 写入前归一 `now.UTC()`——原本地 TZ 值令 `port.ListAllocatedForOfflineNodes` 的 raw-SQL `last_heartbeat_at < <UTC cutoff>` 比较在非 UTC broker 上恒不命中、offline 端口永不撤销。配 TZ 无关回归测试 `internal/node.TestLastHeartbeatStoredUTC`（已证非 vacuous）。
> - **`internal/spawnsafe`（已修，test-only）**：`spawnsafe_test.go:435` 把 resolve-PATH 的 tmp 放首位，去掉「`/usr/bin/echo` 不存在」的环境假设。
> - **`make lint` 可跑性（已修）**：`Makefile` `tools`→`go install …/v2@v2.5.0`、`lint` 找 `GOPATH/bin`。
> 三项均超出 D0 原始范围，按用户「确定修完才能走」明确要求一并完成，并在 `d0-external-review.md` 主进程回复透明登记。

---

## §10. open decisions 主进程裁定（已锁定）

1. **phase 枚举大小写** → **全大写**（§0 已改文档；CHECK 用 6 大写值）。
2. **alerts.kind CHECK 宽度** → **7 个存储承载 kind**（排除客户端合成 `quorum_lost`/`force_single_active`）。
3. **alerts.id 类型/PK** → ULID TEXT PK + `idx_alerts_dedup_active` partial-unique-active。
4. **subjects.go parser：literal-flip vs derive-from-SSOT** → **derive-from-SSOT**（引 `SubjectVersionToken`，更忠于「建 SSOT」、tripwire 白名单收敛为 2 点；agent.go:477 改调 builder）。
5. **`TestNoStrayVersionLiteral` 白名单** → 扫 string-literal AST 节点（排除注释），白名单 `{version.go, auth/permissions.go}`。
6. **migration 测试放置** → 白盒 `internal/storage/cluster_migrations_test.go` 为主（含子集-backfill）；黑盒 `test/storage/` 仅 restart-idempotency，合并不重复。
7. **PreVote 测试目录** → `internal/cluster/prevote_test.go`（快道；`test/cluster/` 留 D7）；L-2 lint 白名单 `internal/cluster/_test.go` 为唯一合法 raft importer。
8. **goleak** → **不用**（非项目依赖、显式避免）；`-race` + `Shutdown/Cleanup`。
9. **依赖 pin** → raft v1.7.3 + raft-boltdb/v2 v2.3.1，bbolt v1.3.5 传递、D0 不 bump，全 `// indirect` 到 D1。
10. **build 门** → 显式 `CGO_ENABLED=0 go build ./...`（`make build` 只 build cmd/tether，不够）。

---

## §11. 实现顺序（阶段 B）

1. **deps**：`go get` raft@v1.7.3 + raft-boltdb/v2@v2.3.1；`go mod tidy`；`CGO_ENABLED=0 go build ./...` 绿。
2. **PreVote 门**：`internal/cluster/{doc.go,prevote_test.go}`——先把承重门立起来、`-race` 过。
3. **migrations**：0008/0009/0010 + `cluster_migrations_test.go`——`make test` 过。
4. **proto v2 SSOT**：version.go（引 token）→ subjects.go（7 parser）→ agent.go:477（调 builder）→ permissions.go → jsstream.go 注释 → golden_test.go + testdata → 改不变量/同步/golden 测试 → ~17 测试文件机械扫。逐步 `make test`。
5. **lint 骨架**：`test/determinism/lint_skeleton_test.go`（L-1 + L-2 + Apply-可达 stub）。
6. **全门**：`make test` + `make lint` + `CGO_ENABLED=0 go build ./...` + `make e2e` + PreVote `-race` + `go mod tidy` 干净。

> 阶段 B 实现中若再发现设计问题，**先改 §0–§18 正文再改代码**（§18 为审计轨迹、正文唯一实现尺）。
