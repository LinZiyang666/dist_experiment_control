# B4 plan — 需求一致性补缺（expose flags / ExposeResp home 字段 / alert raise-clear）

> Stage A：9×Opus 对抗规划（4 drafter 视角 wire-proto-expose / alert-raise-clear / observability-explain / test-risk-scope → 4 critic correctness/ux/scope/test → 1 synth）。synth 现场核验全部 load-bearing claim（file:line）。主进程定稿采纳。
>
> **范围**：3 个已核验缺失的需求一致性项。底层机制（`rebuild_on_failure`/`home_broker`/`epoch` 列、D6 rehome、D8 `OpAlertRaise/Clear/Ack` + appliers、admin backend `HandleCluster` propose-or-forward）**都已存在**——B4 = 把用户输入/输出接到这套机制。**非集群字节等价 + proto 不 bump** 是硬约束。

## 0. Ground truth（本轮核验）

- `proto.ExposeReq` = `{Name, LocalPort, RemotePort omitempty, ActorFP omitempty}`（messages.go:526）。`ExposeResp` = `{Port, PublicHost, Name, Code, Error}` 全 omitempty（messages.go:553）。`PsPortEntry` = `{Port, Name, NID, LocalPort, State, CreatedByFP omitempty, CreatedAt}` — **Port/Name/NID/LocalPort/State/CreatedAt 非 omitempty**（messages.go:406-414）。
- `port_allocations` 有 `home_broker DEFAULT ''`、`epoch DEFAULT 0`、`rebuild_on_failure DEFAULT 1`（migration 0010）。`rebuild_on_failure` 有 reader（`clusterdrain.migrateExposes`）但**无 writer** → 生产中恒 1。
- `PlanAllocate`（plan.go:156-171）用**条件列技巧**：default/un-homed 烤出与 pre-D6 完全相同的列集；仅 `homeBroker != ""` 才 append `home_broker, epoch`。`Allocate`（port.go:191）完全不写 `rebuild_on_failure` → 取 DEFAULT 1。
- `allocatePort`（clusterwrite.go:415-436）返回 Propose 后的 `captured *Allocation`（home/epoch 已被 PlanAllocate 烤入；allocate 时 epoch 恒 0）。
- Home 解析（`resolveHomeForAgent`/`homeForExpose`/`homeForRegister`，home.go）读 `b.cfg.DB`，cluster 模式下 broker.go:516 已把它重指向 `cl.node.RODB()`（committed read）。validator 谓词 = `LookupBy*(...).Eligible() && CertFP != ""`。`SetMaxOpenConns(1)` ⇒ 嵌套查询 under open-rows 死锁隐患（home.go:125-128 drain-then-resolve 教训）。
- `ListBySession`（port.go:290）用 `scanRow`（无 home/epoch/rebuild）。`scanRowWithHome`（port.go:577）+ `scanOneWithHome`（port.go:536）扫 home/epoch；caller = `LookupByName`（264）/`LookupByTokenHash`（278）/`ListAllocatedForOfflineNodes`（417）。
- `cluster.PlanAlertRaise/Clear` 已存在、自校验 enum、经 `LitText`/`LitTextAll` 烤（**拒 NUL + 非 UTF-8**，sqlbake.go:38,46）。`AlertReconciler` 只管 replication_degraded/below_quorum/broker_draining:* — **manual 永不被自动 clear**（alert_reconcile.go:50-51,88-93）。
- `Node.Propose`（node.go:357-372）：leader-gate → applyMu → `plan(db)`；**Plan 错误在 `Apply` 之前返回**（无 raft append）。`HandleCluster`（clusterstatus.go:354-397）：status leader-agnostic；所有 mutator gated `!IsLeader()` → `CodeNotLeader`+`LeaderHost`。adminsock server：`clusterOps` + nil `Backend.Cluster` → `CodeClusterNotEnabled`。
- `EvalDestructiveGate`（alerts.go:62-99）：只 key 在 client-synth `QuorumLost`/`ForceSingleActive`。**store-backed severe（含 manual）→ banner only，NOT `--ack-alerts` 硬门**。

---

## ITEM 1 — `expose --no-rebuild` + `--on-broker <node>`

### 1a. proto 字段（ExposeReq）— additive，NOT 版本 bump

`internal/proto/messages.go`，append 到 `ExposeReq`：
```go
// RebuildOff (B4) is the INVERTED rebuild flag for `--no-rebuild`. Product DEFAULT
// is rebuild ON (the expose auto-rehomes to a survivor when its home dies, D6).
// Encoding the NON-default (OFF) case makes an OMITTED field decode to today's ON:
//   absent / RebuildOff=false  => rebuild ON   (today's behavior, byte-identical)
//   "rebuild_off":true         => rebuild OFF
// Mirror of ExecReq.Safe / RunReq.Safe. An old broker ignores the unknown key and
// keeps rebuild-ON (DEFAULT 1) — the SAFE direction (a dropped --no-rebuild fails ON).
RebuildOff bool `json:"rebuild_off,omitempty"`

// OnBroker (B4) pins the expose's home to a NAMED cluster node (cluster_nodes.node_id
// == raft ServerID), the `--on-broker <node>` flag. Empty/omitted = default home
// resolution (agent's connected broker via the server-id bridge). CLUSTER-ONLY:
// a single broker has no roster, so non-empty is REJECTED (on_broker_single_mode).
// In cluster mode the broker validates the target is a real ELIGIBLE (VOTER) node
// with a usable cert pin, else on_broker_unknown. omitempty => absent on default path.
OnBroker string `json:"on_broker,omitempty"`
```

**4-way old/new peer 字节兼容（load-bearing）：**

| ctl | broker | wire body | decode | behavior |
|---|---|---|---|---|
| old | new | 无 `rebuild_off`/`on_broker` | `RebuildOff=false, OnBroker=""` | rebuild ON, default home — **今天行为** ✓ |
| new, 无 flag | new | omitempty 丢两者 | 同 old ctl — **字节相同** ✓ |
| new, `--no-rebuild` | old | `"rebuild_off":true` | old broker 忽略未知键、无 rebuild-OFF 代码、列 DEFAULT 1 | rebuild ON（**SAFE** — OFF 失败朝 ON）✓ |
| new, `--on-broker X` | old | `"on_broker":"X"` | old broker 忽略未知键 | default home（pin 静默丢 — SAFE：pin 是 best-effort 意图）✓ |
| old | old | — | — | 不变 ✓ |

**为何 `RebuildOff` 而非 `Rebuild`**：`Rebuild bool omitempty`（true=ON）会 (a) 每个 default expose 都序列化键 → 破坏字节等价；(b) `--no-rebuild` 发 `rebuild:false` 被 omitempty 丢 → 与 default 不可区分 → `--no-rebuild` 静默 no-op。tri-state `*bool` 给同一 ON 意图加第三种线上形态。倒置 `RebuildOff` 严格正确（Draft 1 推理，逐字采纳）。

**NOT 版本 bump**：additive optional omitempty、absent 时 decode-to-today — 与 RemotePort/Safe 同类（messages.go 已记不 bump）。`proto.ProtoVersion` 不变。guard 测试断言。

### 1b. 持久化 — 把 `rebuildOff`/`onBroker` 穿过 allocate

`internal/port/port.go` — `Allocate`：加 `rebuildOff bool` 参数。**条件列**（镜像 PlanAllocate）：`!rebuildOff` 时 INSERT 字节相同（不写 `rebuild_on_failure` → DEFAULT 1）；仅 `rebuildOff` 时 INSERT 命名 `rebuild_on_failure` 并置 0。这让**默认单节点行与 pre-B4 字节相同**（DEFAULT 1 == 显式省略）。在返回里置 `Allocation.RebuildOff`。

`internal/port/plan.go` — `PlanAllocate`：加 `rebuildOff bool` 参数。**条件 all-literal bake**：仅当 `rebuildOff==true` 时给 home 与 no-home 两个列集 append `, rebuild_on_failure)` / `, <cluster.LitInt(0)>)`（与既有 home 条件组成 2×2）。`!rebuildOff` 时该 home-state 烤出的 SQL 与 pre-B4 字节相同。（Critic B / test-skeptic §5：Draft 1 的"两分支恒写"是错的；条件 bake 是唯一真字节等价策略。）返回里置 `Allocation.RebuildOff`。

`internal/port/port.go` — `Allocation`：加 `RebuildOff bool` 字段。

### 1c. `--on-broker` 校验 + home-pin（broker）

`internal/broker/clusterwrite.go` — 扩 `allocatePort(sid, nid, name, localPort, remotePort int, rebuildOff bool, onBroker, fp string)`：
- **单模**（`!b.clusterMode`）：`onBroker != ""` 在 `port.Allocate` 前返回 `errOnBrokerSingleMode`（broker 包新 sentinel）。`rebuildOff` 透传给 `port.Allocate`（接受、inert — 写 `rebuild_on_failure=0`，为将来 `cluster init --from-existing` 留诚实元数据）。
- **集群模**：解析 `homeBroker`：
  - `onBroker != ""`：经 `home, err := clusternodes.LookupByNodeID(b.cfg.DB, onBroker)` 校验 — **用 `b.cfg.DB`，与 `homeForExpose` 同一 handle**（cluster 模 = RODB，broker.go:516；已核验非猜）。要求 `err == nil && home.Eligible() && home.CertFP != ""`（镜像 homeForExpose，home.go:101）。**并拒 draining VOTER**（correctness-skeptic §E）：节点带 draining 标记 → `errOnBrokerUnknown`（pin 到正在 drain 的 home 是立刻需迁移的 foot-gun）。任何失败 → `errOnBrokerUnknown`。置 `homeBroker = onBroker`。
  - 否则：保持 `b.resolveHomeForAgent(sid, nid)` → `home.NodeID`。
  - 把 `homeBroker, rebuildOff` 传入 Propose closure → `port.PlanAllocate(db, ..., homeBroker, rebuildOff, cfg)`。

`internal/broker/expose.go` — `handleExposeReq`：把 `req.RebuildOff, req.OnBroker` 传给 `allocatePort`。在 `port.ErrPortTaken` switch 臂后加：
```go
case errors.Is(err, errOnBrokerSingleMode):
    b.replyExposeErr(msg, "on_broker_single_mode",
        "--on-broker requires a clustered broker; this broker is single-node (every expose is homed locally, nothing to pin to)")
    return
case errors.Is(err, errOnBrokerUnknown):
    b.replyExposeErr(msg, "on_broker_unknown",
        req.OnBroker+": not a known eligible (VOTER, non-draining, cert-pinned) cluster node")
    return
```
**安全**：named broker 经 live roster（`LookupByNodeID` + `Eligible() && CertFP != "" && !draining`）校验 — 无任意串进 `port_allocations.home_broker`；坏目标不写任何行（校验在 Propose closure 的 Plan，`Node.Propose` 在任何 raft append 前中止）。把 `on_broker_single_mode`/`on_broker_unknown` 加进 `ExposeResp.Code` 文档注释。

### 1d. CLI flags（`cmd/tether/expose.go`）

```go
cmd.Flags().Bool("no-rebuild", false,
  "pin this expose to its home broker — do NOT auto-move it to a survivor if the home dies "+
  "(default: it auto-rehomes; with --no-rebuild it goes DOWN with its home and `cluster drain` will refuse to migrate it)")
cmd.Flags().String("on-broker", "",
  "pin the expose's home to a named cluster node (cluster mode only; default: the agent's connected broker)")
```
RunE：marshal 进 `ExposeReq{..., RebuildOff: noRebuild, OnBroker: onBroker}`。
- **单模 `--no-rebuild` note**（UX critic §6）：成功 ExposeResp 的 `HomeBroker` 为空 AND `--no-rebuild` 被设时，stderr 打 `note: --no-rebuild has no effect on a single broker (no other broker to move to).`
- **doc caveat**（correctness-skeptic §A）：help 文本 + `docs/usage.md` 注明 `--no-rebuild`/`--on-broker` 对老 broker 静默 no-op（fail-safe，但老 broker 无法信号它丢了 flag）。

---

## ITEM 2 — `ExposeResp.HomeBroker/Epoch`、`ps` 列、`expose explain`

### 2a. proto 字段 — additive omitempty（NOT 版本 bump）

`ExposeResp`（messages.go:553）append：
```go
HomeBroker string `json:"home_broker,omitempty"` // D6 home node_id; '' single/un-homed
Epoch      int64  `json:"epoch,omitempty"`        // per-port monotone reassign counter; 0 baseline
```
`PsPortEntry`（messages.go:406）append：
```go
HomeBroker string `json:"home_broker,omitempty"`
Epoch      int64  `json:"epoch,omitempty"`
RebuildOff bool   `json:"rebuild_off,omitempty"` // surfaces rebuild policy to `expose explain`; same inverted encoding (omitted=rebuild ON=default)
```

**字节兼容**：单 broker → `alloc.HomeBroker==""`、`Epoch==0`、`RebuildOff==false` → 三者全省 → ExposeResp/PsPortEntry 线上与 pre-B4 **字节相同**（对真实 populated 行，非仅 zero struct — 见 test §F.1）。`Epoch` 仅 D6 rehome 后非零（集群）。`RebuildOff` 仅显式 `--no-rebuild` 时 true。老 ctl 忽略未知键；老 broker 永不发它们。

**PsPortEntry 不加 PublicHost**（correctness-skeptic §J、scope-skeptic §B1、test-skeptic §7）：`b.publicHostFor()` 单 broker 上**非空**，故 `public_host omitempty` 不会丢 → 给每个单 broker ps 行加键，**破坏字节等价**。Deferred。`expose explain` 渲染 `public_port: <port>` + 一行 "(combine with your broker host)" 提示，而非完整 URL。

### 2b. 让 `ps` 携带 home/epoch/rebuild — 共享 scanner 的 lockstep 编辑（最高机械风险）

`internal/port/port.go`：
1. 给 `scanRowWithHome`（581）+ `scanOneWithHome`（540）的 Scan 加 `&a.RebuildOff`，并给**所有 caller 同步** append `rebuild_on_failure` 到 SELECT 列表（correctness-skeptic §G、scope-skeptic §C1 — 头号机械地雷）：`LookupByName`（264）/`LookupByTokenHash`（280）/`ListAllocatedForOfflineNodes`（419）。列序：各处 `home_broker, epoch` 后 append `, rebuild_on_failure`，两 scanner 里 `&a.HomeBroker, &a.Epoch` 后 append `&a.RebuildOff`。**列序锁定测试必备**（§F）。
2. `ListBySession`（290）改 SELECT `..., home_broker, epoch, rebuild_on_failure` 并用 `scanRowWithHome` 替 `scanRow`。（`scanRow` 留给 `LookupProxyByNode` 等非 home caller — 不碰它们。）

`internal/broker/exec.go` — `handlePsReq` PsPortEntry 构造：加 `HomeBroker: pa.HomeBroker, Epoch: pa.Epoch, RebuildOff: pa.RebuildOff`。

### 2c. ExposeResp 填充（broker）

`internal/broker/expose.go:303` — 从 captured `alloc`（`allocatePort` Propose 后返回、home/epoch 已烤，clusterwrite.go:430 核验）填：
```go
resp := proto.ExposeResp{
    Port: alloc.Port, PublicHost: b.publicHostFor(), Name: req.Name,
    HomeBroker: alloc.HomeBroker, Epoch: alloc.Epoch,
}
```
无额外 DB 读。单模 → 两者零 → 省 → 字节相同。（test 断言 `--on-broker` 后 `HomeBroker` 匹配持久化行，不止"单模省略" — correctness-skeptic §F。）

### 2d. CLI 渲染

`cmd/tether/ps.go` — PORTS 表：**仅当响应携带任一非空 `HomeBroker`** 时渲染 `HOME` 列（保持单 broker stdout 对脚本字节相同；UX critic §2.2 + scope-skeptic §C3）。渲染值：`epoch==0` → `home=<broker>`；`epoch>0` → `home=<broker> (moved)`（UX critic §2.1 — 人类输出不漏裸 `@<epoch>`；epoch 留 `--json`/`explain`）。集群表里 un-homed 行显 `-`。`--json` 恒带 omitempty 字段（免费；按 jsonout.go 策略不 bump schema_version）。

`cmd/tether/expose.go` 成功行：`resp.HomeBroker != ""` 时 append `  home=<broker>`（+ ` (moved)` 若 `Epoch>0`）。`exposeJSON`（jsonout.go:90）：加 `HomeBroker omitempty` + `Epoch omitempty` + `RebuildOff omitempty`（不 bump schema_version）。

### 2e. `expose explain <name>` — 新子命令，诚实字段集

`cmd/tether/expose.go` — `cmd.AddCommand(newExposeExplainCmd())`。**传输：薄客户端复用既有 `ps` RPC** — 发同一 `PsReq`（member-readable，经已 ACL 的 `ps.req` 命名空间），在**完整未过滤集**上按 `Name` 过滤 `PsResp.Ports`（故能解释 REVOKED/rehomed expose，不论 state — scope-skeptic §B3）。**无新 subject、无新 proto 类型、无 ACL 改**（最小爆炸半径；遵守"不扩 ACL"）。

字段集：

| field | source | status |
|---|---|---|
| name, nid, local_port, state, created_by_fp, created_at | PsPortEntry | **AVAILABLE** |
| home_broker | PsPortEntry.HomeBroker (B4) | **AVAILABLE** — 空时渲染 `(single broker / un-homed)` |
| epoch + `moved` flag | PsPortEntry.Epoch (B4) | **AVAILABLE**（机器 epoch 在此显） |
| rebuild | `!PsPortEntry.RebuildOff` (B4) | **AVAILABLE** |
| public_port | PsPortEntry.Port | **AVAILABLE**（仅 port；完整 URL DEFERRED — PublicHost 不在 ps 行，§2a） |
| last_error / reconnects / ready_reason / last_rehome_at | — | **DEFERRED → B5/DOC#5** |

**诚实规则**（解 Draft 3 vs 4 分歧 — test-skeptic §8、UX critic §3）：人类输出只渲染 available 字段 + 单条 **footer note**：`# rehome events / last_error / reconnects / ready_reason: not yet recorded (planned, B5)` — 不打空 labeled placeholder 字段（杂乱 + 暗示坏掉）。对"出问题"场景，`expose explain` 区分 (a) name 未找到 → 干净 "no such expose" error vs (b) 找到、state=ALLOCATED、`home_broker=<node>` 离线/非 VOTER → 渲染 home + 提示 home 可能不可达（UX critic §2.3）。

`expose explain --json`：新 `exposeExplainJSON{schema:"expose_explain", schema_version:1, ...}` 只带 available 字段；deferred 字段**不在 schema**（B5 干净 bump schema_version 加入 — test-skeptic §8 禁 null 占位）。

---

## ITEM 3 — operator `alert raise` / `alert clear`

### 3a. 传输：broker-local admin Unix socket（operator-only）— 零 ACL 改

同 `cluster add/remove/drain/...`：CLI → `callAdmin(socket, adminsock.Request)` → `clusterAdminBackend.HandleCluster` → leader-gate → `node.Propose(PlanAlert*)`。理由：(1) raise/clear 集群级告警是 operator 动作（与 `cluster *` 同信任档）；(2) **不加 NATS subject、不碰 `PermissionsForBroker`/member-JWT** → D8 member carve-out（`alert.ls`/`alert.ack`）不变、`cluster.apply.*` 仍 broker-only — 最强安全姿态，直接满足"不扩 ACL"并使"NATS perm 改须本地可证"约束**变空**（无 perm 改）。`alert ls`/`alert ack` 仍 member NATS RPC（读 + team-ack）；raise/clear 仅 operator。member NATS write RPC for raise 显式拒（会为铸造集群级 banner 的写扩 ACL）。

### 3b. adminsock + raft routing

`internal/adminsock/protocol.go`：
```go
OpClusterAlertRaise = "cluster_alert_raise"
OpClusterAlertClear = "cluster_alert_clear"
```
两者加进 `clusterOps`。新 Request 字段（additive omitempty、字节兼容 — 同 D7 args 纪律）：
```go
AlertKind     string `json:"alert_kind,omitempty"`
AlertSeverity string `json:"alert_severity,omitempty"`
AlertMessage  string `json:"alert_message,omitempty"`
AlertLabel    string `json:"alert_label,omitempty"`     // optional disambiguator -> dedup manual:<label>
AlertDedupKey string `json:"alert_dedup_key,omitempty"` // alert-clear positional key
```
（用专用 `AlertLabel` 而非复用 `NodeID` — UX critic §7：manual 维护告警非 node-scoped；`--label` 诚实。）

`internal/broker/clusterstatus.go` — `HandleCluster`：在 leader-gate 后加两 case（follower 快失败 `CodeNotLeader`+`LeaderHost`）：
```go
case adminsock.OpClusterAlertRaise:
    return b.handleAlertRaise(req)
case adminsock.OpClusterAlertClear:
    return b.handleAlertClear(req)
```

`internal/broker/clusteradmin.go`（或新 `alertadmin.go`）：
- `handleAlertRaise`：校验 `req.AlertKind == cluster.AlertKindManual`（CLI 限 manual；broker 复核 — raise system kind 会与 reconciler 打架）→ 否则 `CodeBadRequest`。校验 `ValidAlertSeverity` → `CodeBadRequest`。校验 `AlertMessage` 非空。算 key：`req.AlertLabel == ""` → `DedupKeyGlobal("manual")`（=`"manual"`）；否则 `DedupKeyNode("manual", req.AlertLabel)`（=`"manual:<label>"`）。经既有 `newAlertID(key, now)` 铸 id。`b.admin.node.Propose(func(db){ return cluster.PlanAlertRaise(id, "manual", sev, key, msg, now) })`。`cluster.IsNotLeader(err)` → `CodeNotLeader`（mid-Propose 失 leadership，TOCTOU）；其他 → `CodeStoreError`。**OK 响应回显 dedup_key** 让 CLI 印精确 clear 命令（UX critic §4）。
- `handleAlertClear`：要求 `req.AlertDedupKey` 非空 → `CodeBadRequest`。`b.admin.node.Propose(func(db){ return cluster.PlanAlertClear(req.AlertDedupKey, now) })`。**无 pre-read**（scope-skeptic §D3、test-skeptic §4）：`PlanAlertClear` 幂等；若 Apply 路径廉价返回 `RowsAffected` 则用，否则仅 OK — 彻底避开 `MaxOpenConns(1)` 嵌套查询死锁。（"already inactive" note 是 nice-to-have，不值 read-then-propose；drop。）

**Poison-safety**（correctness-skeptic §C — 头号缺项，已进 test §F）：alert ops 走 `genericExecApplier`（非 poison-skip）。`PlanAlertRaise` 自校验 kind/severity AND 经 `LitText`/`LitTextAll` 拒 NUL + 非 UTF-8（sqlbake.go:38,46 核验）。`Node.Propose` 在 `n.Apply` 前返回 Plan 错误（node.go:362-364 核验）→ 无 raft append。坏 kind/severity/message → `CodeBadRequest`、零 raft entry、集群活。纵深：CLI 限 kind、broker 复校、Plan 自校验 + LitText 拒。

### 3c. CLI（`cmd/tether/alert.go`）

所有 alert verb 留在 `tether alert` 下（可发现性；UX critic §5、scope-skeptic §D2、遵命名）— 但 raise/clear 经 `callAdmin(socket)` 实现（非 session/NATS），help：`# operator-only; runs on the broker host via the admin socket (not a session)`。复用 cluster-cmd `--socket` flag + 既有 non-leader-hint 渲染器。
```
tether alert raise --kind manual --severity {info|severe} --message "<text>" [--label <tag>]
tether alert clear <dedup_key>
```
- client 侧：限 `--kind` 为 `manual`（其他给 usage error 指向自动 writer）；校验 `--severity ∈ {info,severe}`；`--message` 非空。
- **`--severity severe` help 须述爆炸半径**（UX critic Finding 0，must-fix）：`severe = shown in the always-on ps/node stderr banner; it does NOT block writes (only quorum_lost / force_single_active hard-gate destructive ops).` 防 operator 误以为会强制。
- **`raise` 成功输出回显精确 clear 命令**（UX critic §4）：`raised: manual (severe) "<msg>" — clear with: tether alert clear manual`。**dedup no-op 时 warn**：raise 是 `WHERE NOT EXISTS` no-op（该 key 已有活 manual）→ 打 `note: a manual alert is already active for key "manual" (not overwritten); clear it first or use --label`。（经 Apply 的 RowsAffected==0 检测，优先于二次往返。）
- `clear <dedup_key>`：positional；help 述形态（`manual` 或 `manual:<label>`）。幂等：key 活与否都 OK。

### 3d. 单模行为 — cluster-only、诚实、不崩

两 op 在 `clusterOps`；单模 nil `Backend.Cluster`（核验：`wireClusterLate` 仅 cluster 模构造）→ adminsock server 返 `CodeClusterNotEnabled`（核验 server 路径）。**不崩、无 NATS、无 Raft、单模 wiring 字节等价**（无新构造）。CLI 消息：`alert raise/clear requires a clustered broker (alerts are Raft-replicated); this broker is single-node.` 诚实：单 broker 无 raft 可 Propose，single-WAL-owner 不变量禁非 raft 告警写。

### 3e. manual-alert 不变量（保 + 测）

reconciler clear-pass 只管三 derived kind（alert_reconcile.go:50-51,88-93 核验）— operator-raised `manual` 持续到 operator clear。B4 依赖此并测（§F）。

---

## F. 对抗 TEST 清单（映射到套件）

### `make test` — 单元（嵌入式 nats-server；无 gated harness）

**proto 编码（`internal/proto`），用真实 populated fixture（非 zero struct — test-skeptic §0）：**
1. `ExposeReq{Name:"x",LocalPort:8080}`（真实、无 flag）marshal 与 pre-B4 fixture 字节相同；`{...,RebuildOff:false,OnBroker:""}` ≡ 同（omitempty default-ON 证）。
2. `{...,RebuildOff:true}` → 含 `"rebuild_off":true`，round-trip；`OnBroker:"X"` → `"on_broker":"X"`。
3. **老 broker 忽略（decode 侧，两 flag）**：new-ctl body 含 `rebuild_off:true,on_broker:"X"` unmarshal 进 legacy struct（仅老字段）→ 无错、extras 丢（证 new-ctl→old-broker 兼容）。
4. **`ExposeResp`/`PsPortEntry` 真实行 omitempty**：populated 行（Port/Name/NID/LocalPort/State/CreatedAt 已设）`HomeBroker=""`/`Epoch=0`/`RebuildOff=false` marshal 与同行 pre-B4 marshal 字节相同；置 home/epoch/rebuild → 键现。
5. guard：`proto.ProtoVersion` 不变。

**port 层（`internal/port`）：**
6. `Allocate(rebuildOff=false)` → 行 `rebuild_on_failure=1`，且存储行与 pre-B4 allocate 字节/语义相同（DEFAULT 1 == 省略）。`Allocate(rebuildOff=true)` → `=0`、`Allocation.RebuildOff==true`。
7. **`PlanAllocate` 条件 bake golden**：default expose（`rebuildOff=false`、`homeBroker=""`）烤出的 INSERT 串 **等于捕获的 pre-B4 golden**（无 `rebuild_on_failure` 列）。`rebuildOff=true` append literal `, rebuild_on_failure) ... , 0)`。测全 4 combo（home × rebuild）。**DIFF-1**：`--on-broker brk-b, --no-rebuild` 在 UTC 与非 UTC `Now` 烤出字节相同。
8. **`scanRowWithHome`/`scanOneWithHome` 列序锁定**（头号机械地雷）：插 DISTINCT `home_broker`/`epoch`/`rebuild_on_failure` 值的行；断言经全 caller（`ListBySession`/`LookupByName`/`LookupByTokenHash`/`ListAllocatedForOfflineNodes`）各映对字段 — 防 SELECT/Scan 错位。
9. `ListBySession` 返回 `HomeBroker`/`Epoch`/`RebuildOff` 填充（homed + 单模 `""`/`0`/`false` 行）。

**broker wiring（`internal/broker`）：**
10. **`TestOldShapedExposeReqRebuildsOn`**（test-skeptic 必备 #1）：字面 pre-B4 字节 `{"name":"x","local_port":8080}` → decode → 驱 `handleExposeReq`（单模 in-mem DB）→ 断言持久化 `rebuild_on_failure==1`（端到端，非仅 marshal）。
11. **`--on-broker` 4-cause 表**（correctness/test-skeptic）：(a) `LookupByNodeID` miss → `on_broker_unknown`；(b) VOTER-phase 节点 → ok / pin 写；(c) 非 VOTER（`JOIN_VERIFIED_PENDING_VOTER`/`CATCHING_UP`/`VOTER_ADD_FAILED`）→ `on_broker_unknown`；(d) **VOTER 但 `CertFP==""`** → `on_broker_unknown`；(e) draining VOTER → `on_broker_unknown`；(f) 单模 `--on-broker X` → `on_broker_single_mode`。**每个拒绝断言无行写**（validation-before-Propose）。
12. ExposeResp 从 captured alloc 填：单模 → home/epoch 省；`--on-broker` → `resp.HomeBroker` 等于持久化行 `home_broker`（correctness-skeptic §F）。

**alert ops（`internal/cluster` + `internal/broker`）：**
13. `PlanAlertRaise("manual","severe",...)` / `("manual","info",...)` 烤出精确 `INSERT…WHERE NOT EXISTS`；坏 kind/severity → error；DIFF-1（UTC/非 UTC 同 literal）。
14. **Poison-safety**（correctness-skeptic #1，头号缺测）：`handleAlertRaise` message 含 NUL / 非 UTF-8 / 1 MiB → `CodeBadRequest`、**零 raft entry（断言无 Apply）**、集群活。坏 kind/severity 同。
15. **`TestReconcilerNeverClearsManual`**（test-skeptic 必备 #5）：raise `manual`，健康 quorum 下跑 `ReconcileAlertsOnce`（会 clear 它自己的 key），断言 manual 仍 ACTIVE。
16. 双 raise 同 key → 第二个是 committed no-op（`WHERE NOT EXISTS`），恰一 ACTIVE 行；raise RowsAffected==0 浮出 "already active" warn。
17. **`TestAlertClearLeadershipLostMidPropose`**（test-skeptic 必备 #6）：fake node `IsLeader()==true` 但 `Propose→raft.ErrNotLeader` → handler 返 `CodeNotLeader`（非 `CodeStoreError`）。
18. clear absent key → OK（幂等、无错、无额外读 → 无死锁）。

**adminsock + CLI：**
19. 新 Request 字段 omitempty round-trip；`OpClusterAlertRaise/Clear` 在 `clusterOps`；nil `Backend.Cluster` → `CodeClusterNotEnabled`（不 panic、不 Propose）。
20. **`TestPsPortsStdoutSingleBroker_ByteStable`**（test-skeptic 必备 #8）：所有 port `HomeBroker==""` → 渲染 → golden stdout 等于 pre-B4（无 HOME 列、无尾 tab）。集群：一 homed + 一 un-homed → HOME 列现、epoch>0 `(moved)`、un-homed `-`。
21. `expose explain`：匹配 name 渲染 available 字段 + footer note、无 labeled `last_error:`/`reconnects:`；name-not-found → 干净 error；`exposeExplainJSON`/`exposeJSON`/`psJSON` golden — 无 deferred 键、schema_version 留 1。

### `test/d6`（`-tags d6_integration`，`TestD6Matrix -race`）— home/rehome

22. `--on-broker brk-b` → `port_allocations.home_broker=="brk-b"`、`homeForExpose` 骑指向 brk-b 的 HomeDirective、agent 拨 brk-b。
23. **rehome → ExposeResp/ps/explain 反映新 home**（payoff）：allocate home A（epoch 0），杀 A，agent rehome 到 B（`OpPortReassignHome`，epoch 1）；`ps`/`expose explain` 显 `home=B (moved)` / epoch 1。断言读经 committed RODB（epoch 跨两读单调非降 — 不会 B@1 然后 A@0）。
24. **双 rehome A→B→A**（test-skeptic 必备 #4）：epoch 0→1→2，home 回 A；断言 `ps` 显 A epoch 2（非 0）— 防仅按 `home_broker` 键 home-identity。

### `test/d7`（`-tags d7_integration`）— drain refusal（首次真演练）

25. **`--no-rebuild` drain refusal**（scope-skeptic §E3，证 item-1 持久化抵达消费者 — B4 前无 expose 能 rebuild-OFF，故 `clusterdrain.go` reader 恒 1）：`--no-rebuild` expose homed 某节点 → `cluster drain` 该节点 → 精确 D7 契约（读 `clusterdrain.go` 锁定：typed refusal `ErrRebuildOffExposes` vs 枚举 vs skip-migrate）— 断言不被静默 auto-migrate。

### `test/d8`（`-tags d8_integration`，`TestD8Matrix -race`）— alert raise/clear

26. operator `alert raise --kind manual --severity severe` 经 leader socket → Raft-replicated（FOLLOWER 上 `ActiveAlerts` 可见）→ 现于 member 侧 `ps`/read 的 severe banner → leader `alert clear manual` → 全集群消失。
27. 非 leader raise（follower socket）→ `CodeNotLeader` + 正确 `LeaderHost`；leader 重跑 → 成功。
28. operator raise SYSTEM kind → broker guard 拒（仅 `manual`），不能与 reconciler 打架。
29. **爆炸半径断言**（UX Finding 0）：raised `manual/severe` 让 member `ps` 渲染 banner 但**不**触发 `expose`/`session rm` 的 `--ack-alerts` 硬门（硬门 key 在 client-synth `quorum_lost`/`force_single_active`，alerts.go:62 核验）。锁定文档语义。

### e2e

B4 触碰 D6/D7/D8 已在矩阵的面 — 无新矩阵条目。提交前硬闸：`make test` + `make e2e` + `make lint`；gated 套件 under `-race`。无新 goroutine/tunnel（rehome 是既有 D6；alert raise 是一次性 Propose）→ 无新 leak-gate 工作，但 gated 测试跑在既有 `-race` 矩阵。

---

## G. 实现顺序 + 依赖

1. **proto**（messages.go：ExposeReq +2、ExposeResp +2、PsPortEntry +3；adminsock/protocol.go：2 op + 5 字段 + 加进 clusterOps）+ §F test 1-5,19。（additive；无 dependent 破。guard：ProtoVersion 不变。）
2. **port 层**（Allocation.RebuildOff；**条件**穿 rebuildOff 进 Allocate/PlanAllocate；lockstep `scanRowWithHome`/`scanOneWithHome` + 4-caller 列编辑；ListBySession swap）+ test 6-9。**最高机械风险 — 列序锁定测试(8)须随之落地**。（依赖 1 取字段名。）
3. **broker wiring**（allocatePort 签名 + on-broker 校验门经 `b.cfg.DB`/RODB + draining check + sentinel；handleExposeReq 调用点 + error case + ExposeResp 填充；handlePsReq 构造）+ test 10-12,22-25。（依赖 2。）
4. **CLI expose**（`--no-rebuild`/`--on-broker` flag + 单模 note；ps HOME 列带 `(moved)`；exposeJSON 字段；`expose explain` 子命令 + exposeExplainJSON）+ test 20-21。（依赖 1,3。）
5. **operator alert**（adminsock 字段在 1 完成；clusteradmin handleAlertRaise/Clear；HandleCluster case；CLI `alert raise`/`clear` 在 `tether alert` 经 callAdmin，带 severe-爆炸半径 help + clear-命令回显 + 单模消息）+ test 13-19,26-29。**独立于 1-4**（除 proto 无共享文件）；可同 PR 后落。
6. **Docs**：`docs/usage.md`（expose flag、explain、alert raise/clear operator 行 — 注 operator-only/admin-socket/cluster-only + severe=banner-not-gate）；`docs/requirements.md`（标满足）；`docs/cluster-runbook.md`（一行：维护期 operator manual 告警 + `--on-broker` pin）。
7. 提交前硬闸（§F e2e）。

---

## H. DEFERRED → B5 / DOC#5（NOT in B4）

- **rehome EVENT 流 / `last_error` / `reconnects` / `ready_reason` / `last_rehome_at`** 带真数据 — 需新 agent→broker telemetry 通道（DOC#5）。B4 只加结构字段（home/epoch/rebuild）+ `expose explain` 壳；explain 显 available 列 + 其余 footer note。
- **`PsPortEntry.PublicHost`**（explain 里完整 `public_url`）— deferred 因单 broker 非空、会破 ps 字节等价；explain 只渲 `public_port`。
- **独立 `expose ls`** + `exposeLsJSON`（scope-skeptic §A1）— `ps` + `expose explain` 覆盖 item 2；从 B4 砍。
- **人类 `ps` 表的 REBUILD 列**（scope-skeptic §A3）— rebuild 策略住 `expose explain`，非 ps 列。
- **operator "紧急 clear system 告警"** 框（scope-skeptic §A2）— `alert clear` 实践限 manual-kind key；drop emergency-system-clear 承诺（reconciler 会重 raise）。
- **manual-severe 接入 destructive 硬门** — D8 故意只门 client-synth `quorum_lost`/`force_single_active`；manual-severe 设计上 banner-only（扩门是单独决定）。
- **manual-alert TTL/自动过期、`--kind` 超 manual、0009 enum 自定义 kind/label、per-identity manual-alert ack、`ps --watch`/metrics、multi-home 二级 tunnel** — 出 B4。

---

## 硬约束 CONFIRMATION

- **NOT proto 版本 bump**：全新线上字段（ExposeReq ×2、ExposeResp ×2、PsPortEntry ×3）additive `omitempty`、absent decode-to-today — 同 RemotePort/Safe/ServerID（记不 bump）。adminsock 字段是 host-local socket（非 `tether.v2.*` 线），additive omitempty 同 D7 args。`proto.ProtoVersion` 不变（guard test）。✅
- **非集群字节等价成立**：单模 `RebuildOff=false`→`rebuild_on_failure=1`（== 老 DEFAULT、条件 bake 字节相同）；`home_broker=''`/`epoch=0`/`rebuild_off` absent → ExposeResp/PsPortEntry 线上字节相同（真实行 golden test 4,7,20）；ps HOME 列不渲；`--on-broker` 拒（不写）；`PublicHost` 不加 ps 行（会破）；`alert raise/clear` → `cluster_not_enabled`（无新构造）；D6 `tunnelTokenLookup`/home ladder 仍 `HomeBroker != ""`-gated → inert。✅
- **安全**：`--on-broker` 经 `clusternodes.LookupByNodeID` + `Eligible() && CertFP != "" && !draining`（真 eligible VOTER、无任意串；拒绝时不写行）。`alert raise/clear` operator-only 经 host-local admin socket。✅
- **不扩 ACL / 本地可证**：`expose explain` 复用 member-readable `ps` RPC；`alert raise/clear` 不加 NATS subject/permission — 零 auth_callout/perm 改 → 本地可证约束变空。✅
- **FSM poison-safe（item 3）**：Plan 自校验 + `LitText` 拒 NUL/非 UTF-8；`Node.Propose` 在任何 raft append 前返回 Plan 错误（node.go:362-364）→ 坏输入 `CodeBadRequest` 零 raft entry。test 14 证。✅
