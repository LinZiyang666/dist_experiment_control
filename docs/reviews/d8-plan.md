# D8 plan — 文件传输分布式 ‖ 告警系统（两并行叶子）

> **流程**：Stage A 定稿（本文件）。多专家对抗 workflow（4 drafter 不同视角 → 4 critic 四风险镜头对抗互评 → 1 synth；全 Opus 4.8、静态 fan-out）产出候选，**主进程为唯一定稿人**：亲手核验全部承重事实、逐条采纳/驳回 critic finding、裁决 5 个残留 OQ。synth 轨迹见任务输出，本文件是最终实现尺。
>
> **范围（架构 §19-D8）**：D8a transfer 补成 cluster-distributed（终结于 home + audit 经 leader Apply 可重导 + tier-B 副本重配 + retire 门）；D8b alerts 补成 HA（leader 复制 store + cluster 级 ack + 客户端合成 gating + banner + destructive 闸）。**build-and-prove，不切线上**（cutover=D9，同 D2–D7）。
>
> **承重事实已核验**（主进程亲读，非采信 agent）：① transfer 全部 subject 走 plain `nc.Subscribe`（`broker.go:532` 订阅表，含 513–522 push/pull/push-commit/ev/finalize）→ 集群下 N 路 fan-out，**home-gate 必需**；② `validReqID` = `<=64 lowercase hex`（`node.go:342`）→ 派生 hex reqID 合法；③ reqID ledger 写仅按 `cmd.ReqID!=""`（`fsm.go:169/234`）、与 applier SQL 无关 → 空-Body op 可用 ledger；④ `VerifyLeaderRead` 存在（`read.go:66`）→ health 响应可确认 writable leader；⑤ `claimFinalize` 是全部终态路径的唯一原子认领点（callers `transfer.go:318/686/720/932`）→ 单 broker 每 transfer 只发一个终态；⑥ `Degraded=Observed&&len>0&&!AllAtTarget`、`AllAtTarget` 在 `!Observed||empty` 时 false（`audit_publisher.go:357/371`）→ clear 用 `Observed && !Degraded()`（Stage C m5：覆盖 `AllAtTarget` 与 empty-set；Observed 门下不 transient false-clear）；⑦ `disk.go` 无下降沿（recover 仅 `emitted=false`，`disk.go:85-87`）→ 需新增 clear-edge；⑧ leader 读不到 follower 游标（`clusterstatus.go:60-61/352`）→ raft_lag per-follower 推 D9；⑨ member Pub 全 `ctrl.by.<actor>.*`、`cluster.*` broker-only（`permissions.go:55-77`）→ ACL carve-out 用 actor-scoped ctrl 子主题；⑩ 0009 已带 `alerts`/`alert_acks`、0011 已带 `cluster_reqid_ledger` → **D8 无需新 migration**。

---

## 1. 范围与三层可证性

D8 两叶子**非完全独立**：D8b 的 ctl 侧 `gateDestructive` 必须包住 D8a 的 push/pull。**裁定**：D8a 先落；D8b 的 gate/banner 是纯 ctl 侧 helper，D8a push/pull 经一行 hook 调用（接缝先约定）。一个 plan、一次 Stage C、一次外审、phase 末一次 commit（同 D7 体例）。

三层划分（同 D5–D7）：

| 层 | D8a | D8b |
|---|---|---|
| **(A) build-and-prove，不 cutover**（harness-only，需 `cluster.Node`） | home-routing gate、`VerbTransferAudit` 转发、`OpTransferAudit` op + publisher 重导、clustered `ensureXferBucket(ReplicasFor(nVoters))`、retire 门 xfer 枚举、home-gated orphan reaper 过滤 | `OpAlertRaise/Clear/Ack` op + applier、leader `alertReconcile` loop、`VerbAlertSignal`/`VerbAlertAck` 转发、broker 侧 health/alert-ls 响应器、disk-bridge 转发 |
| **(B) genuinely-LIVE**（发布，但 N=1 构造性 inert） | **无**——transfer 分布式全部 inert until D9 | **ctl 侧** `gateDestructive`/`withBanner` helper（live 在 `cmd/tether`，N=1 无 broker 应答 probe → 不 gate/不 banner）；`disk.go` clear-edge 新信号 `alertSink`-gated（生产 nil） |
| **(C) inert-until-D9**（serve.go 接线） | `serve.go` 字节不动；生产 `Broker` 不构造 `cluster.Node`；`transferAuditSink`/`alertSink` nil；`xferTargetReplicas→ReplicasSingle` | 同 |

**生产不变式（脆点保证）**：N=1 今天零行为变化。无 transfer audit 转发（sink nil → `pubAuditTransfer` 字节等价）；永不写 alert（无 `cluster.Node` → alert loop 永不启动）；ctl 的 gate/banner probe 得 `ErrNoResponders` → 判为"非集群"静默（OQ9-C）。guard `TestD8ProductionWiresNoCluster` 锁死。

---

## 2. D8a transfer

### 2.1 Home 路由（OQ1）— broadcast-SUB + home-keyed gate，**数据面不经 §4.1 转发**

**根因（已核验）**：push/pull/push-commit/ev.transfer/finalize 全是 plain subscribe → 集群下每 broker 都收每份 → cutover 时 N broker 各跑 handler（N 个 bucket、N 条 tracker、N 个 forward-to-agent、N 条 audit）。这是 D8a 头号正确性问题。

**机制**：build-and-prove `transferHomeGate(sid, nid) (proceed bool)` 插在每个 handler 体顶部，**在** `transferGate`（node-online/actor-member）**之后**、任何 bucket/tracker 工作**之前**：
- `selfID == ""`（生产）→ `true` **无条件** —— 生产字节路径不变（inert 接缝）。
- home 解析 == self → `true`（我是 home，处理）。
- home 解析 != self → `false`（**静默返回、不应答**；home 处理）。
- home 未解析/不合格 → `false`（**静默返回、不应答**，见下）。

node→home 复用 expose-independent `resolveHomeForAgent(sid,nid)`（`home.go:63`，读 `nodes.nats_server`→`clusternodes.LookupByNatsServer`→合格 VOTER）。无 expose 的 node 也有 home（D6 server-id 桥接）。

**home-未解析 = 静默重试，非合成错误**（采纳 critic 1/2/3 BLOCKER；驳回 drafter 1 的"agent 当前 server 应答"——未解析恰由缺绑定定义，无 broker 能匹配 → 零应答 → ctl 挂死）。**裁定**：未解析/不合格路径**所有非 home broker 静默**，ctl 超时 → backoff 重试（绑定收敛/rehome 落定后命中真 home）。无 consensus-free single-writer 宣称、无 `ConnectedServerName()` 依赖。代价仅 ctl 侧对真不可解析目标的"超时重试"——可接受（贴 `transferGate` 现有 `node_offline` 重试姿态，ctl 本就把 transfer 应答超时当 best-effort）。

**finalize.req（subject 无 nid）**（已核验 `ParseTransferFinalize`→`(actor,sid,transferID)`，无 nid）：按 **tracker 持有** 路由——仅持 tracker entry 的 broker 应答；`entry==nil` → 静默（ctl 本就 `_ = sendFinalize` 忽略应答）。**无需 home-epoch 围栏**（实现裁定，修正 critic 1 MAJOR 的担忧）：每次 push/pull 调用铸新 transfer_id、对象按 transfer_id 键、tracker entry 只存在于单个源 broker，故并发 rehome 不产生跨 broker 双绑——旧 home 只 finalize/删自己 tid 的对象（正确），rehome 后的传输是新 tid 走新 home。与 §16(f) 一致（claimFinalize 单-broker + home-gate 单写 + per-invocation tid）。**不把"finalize 超时"烤进测试当契约**——测试断言"客户端重启成功"，非"finalize 必超时"。

**§9 doc-first（BLOCKER）**：§9 现写"push.req/pull.req 经 §4.1 转发"，与本设计冲突（broadcast SUB 已送达 home，多一跳是浪费）。§9 必改为"数据面 push/pull 经 broadcast-SUB + home-keyed gate（home==self 才处理，余静默）；仅 transfer audit 行经 §4.1 leader Apply"。**gate 不能在 §9 仍说"经 §4.1 转发"时落地。**

### 2.2 Transfer audit 可重导 + 幂等（OQ2）— `OpTransferAudit`，**reqID-ledger 锚定**（非 JS-窗口）

**`OpTransferAudit`** = 每事件、**纯-Aux 空-Body** op（无持久 audit DB 行）。**六个** `pubAuditTransfer` 调用点（已核验：`331/473/580/705/724/961`）各成**一条** committed `OpTransferAudit`。publisher 纯从 Aux 重导（同 `publishReconcile`）。

**幂等（中心 BLOCKER 修正）**（采纳 critic 1/2/4；驳回 drafter 2 的"无 reqID、JS dedup 吸收 raft 重复"——`Duplicates` 窗口有限，选后 sweep / 延迟重试可在窗外重发 → 两条 `complete`）：`OpTransferAudit` **必带派生 reqID** `reqID = hex(sha256("xferaudit:"||transfer_id||":"||kind))`（取 64 hex），经 0011 `cluster_reqid_ledger` 压第二次 commit（唯一窗口无关的修法，同 D4 reconcile）。ULID↔`<=64 lowercase hex` 冲突由 sha256-hex 派生化解（transfer_id 走 Aux，派生 hex 走 `cmd.ReqID`）。空-Body 经 `genericExecApplier`（0 语句、确定性 no-op）；ledger 写在 applier 后、仅按 `ReqID!=""` 门（`fsm.go:234`，已核验），故首 commit 注册 reqID、重试走 `appliedDedup` 跳 SQL 推 applied_index。

**Publisher 重导**（采纳 critic 1/4 头号 false-green：`PublishOnce` 默认 `advanced=idx`（`audit_publisher.go:239`）静默丢未知 op）：在 `PublishOnce` switch 加 `case cmd.Op==cluster.OpTransferAudit`（紧挨 `OpReconcileBatch`），发到 `proto.SubjAuditTransfer(rec.Session)`、msg-id `q<reqID>:xfer:0`（现有 reqID-bearing 形）。**强制带真空对照的反 false-green 测试**：一个缺 case 的变体断言行被静默丢——证测试真走重导路径（对真 `PublishOnce` 控制流，非伪造 cmd）。

**矛盾 complete/failed（OQ9-B，主进程裁定：无需 terminal-state guard）**：已核验 `claimFinalize` 是全部终态路径（watchdog 318 / ev 686,720 / finalize 932）的唯一原子认领点——单 broker 每 transfer 只一个终态被发。配合 §2.1 home-gate（每 transfer_id 单 broker）+ 每次 push/pull 调用铸**新 transfer_id**（客户端重启=新 transfer），complete/failed 矛盾**根本不出现**：A 的 `failed`(tid=X) 与重启后 B 的 `complete`(tid=Y) 是不同 transfer。reqID ledger 处理 raft 重试。**裁定**：不加 terminal-state guard、不加新 SQL 表（比 synth 两选项都干净）；§16 登记此推理（claimFinalize 单 broker 终态串行 + home-gate 单写 + per-invocation transfer_id）。

**start 延迟（OQ9-A，主进程采纳）**：start/complete/failed 三者**全经 leader Apply**（保 audit-pair 完整：可重导 `complete` 无 `start` 是完整性洞），但 forward **异步、不阻塞 agent-forward**（audit 可重导、最终一致即可；不给数据面加 raft commit 延迟）。§9 prose 注明"start 异步不阻塞 agent-forward"。

### 2.3 Tier-B 副本 + retire 门（OQ3）

D5 已建 `XferReplicaState`（`transfer.go:266`）、`raiseXferReplicas`（只升）、`ReconcileOnce` 经 `XferState` hook 把 `OBJ_xfer-<sid>` 折进 `AllAtTarget`（`audit_publisher.go:455`）。D8a 补完：

1. **live 调用点脱离 `ReplicasSingle`**经 inert 接缝：`b.xferTargetReplicas()` = `node==nil`（生产）→ `ReplicasSingle`（字节等价），else `ReplicasFor(NumVoters())`。替 `transfer.go:439,539`。
2. **retire 门枚举读 JetStream 实际 `OBJ_xfer-*` 流列表，非 DB `ListSIDs`**（采纳 critic 1/2/4 BLOCKER：`ListSIDs` 生产无 provider、bucket 可活过 session 行 → purged-session 孤儿不可见 → retire 对 1-副本对象 false-green → 丢数据）。retire 门副本枚举用 `js.ListStreams` 过滤 `OBJ_xfer-*`（boot reconciler 已这么做，`transfer_reconcile.go:32`），唯一 fail-closed 源。DB `ListSIDs` 仍可用于 **raise** pass（漏 bucket 只延迟 raise、不丢数据）。
3. **拆只读 `ObserveReplicas()` 与升配 `ReconcileOnce()`**（采纳 critic 1/2/4：`ReconcileOnce` 升配有副作用；retire readiness 探针若 MUTATE 拓扑会掩盖卡住的重配）。retire 门消费**只读** `ObserveReplicas()`；后台 loop 保留升配 `ReconcileOnce()`。

### 2.4 In-flight best-effort + orphan reaper（OQ4）

best-effort 确认：tracker+watchdog home-local；home 死丢在途；rehome 不保在途；**完成的 tier-B 对象存活**（在 JS-quorum 的 `OBJ_xfer-<sid>` 流，R=`ReplicasFor(3)`=3，不在 broker）。

**boot orphan reaper（采纳 critic 4 BLOCKER 修正）**：危害是 OBJECT 级（已核验 `reconcileXferObjectsOnBoot` 调 `store.Delete(obj.Name)`、从不 `DeleteStream`）：broker-B boot reaper 把共享复制 bucket 里 broker-A 的 live 在途对象当孤儿（B 的 tracker 空）→ 删 A 的活对象。**裁定：home-ownership 过滤，非整体禁用**（采纳 critic 1/2/3：禁用会永久泄漏 8 GiB bucket 且 wedge retire 门）：clustered 模式下对象 reaper 仅收 home==self 且 committed `OpTransferAudit` 状态为终态的 session 对象；永死 home 的孤儿由新 home 接管后收（或 leader-driven GC 按 committed 终态键）。生产（`selfID==""`）保今天字节等价。

---

## 3. D8b alerts

### 3.1 Store op（OQ5）— 三个 FSM op 走 `genericExecApplier`，**无新 migration**

0009 已带 `alerts(id,kind,severity,dedup_key,state,message,raised_at,cleared_at)`+`idx_alerts_dedup_active(dedup_key) WHERE state='ACTIVE'`、`alert_acks(dedup_key PK,acked_by,acked_at)`，CHECK 恰列 store-backed 集（`quorum_lost`/`force_single_active` 故意缺——client-synth）。**无 schema 改**。三 op 全 leader-Plan、全字面 SQL、`genericExecApplier`：

- `OpAlertRaise` → `INSERT INTO alerts(...) SELECT <lits> WHERE NOT EXISTS(SELECT 1 FROM alerts WHERE dedup_key=<lit> AND state='ACTIVE')`（冲突 no-op，避开 unique-index Apply 错致 fork）。
- `OpAlertClear` → `UPDATE alerts SET state='CLEARED', cleared_at=<lit> WHERE dedup_key=<lit> AND state='ACTIVE'`。
- `OpAlertAck` → `INSERT ... ON CONFLICT(dedup_key) DO UPDATE`（last-writer-wins、幂等）。

**确定性证明重新落地**（采纳 critic 1/2：SQL 安全但 drafter 3 理由"任何副本都不报错"是错的、会诱发 fork "优化"）：正确性靠**严格有序 Apply + committed-state 谓词**——每副本按 committed 序对相同 committed 态评 `WHERE NOT EXISTS(ACTIVE)` → 相同 `RowsAffected`。§16 登记此证明。

### 3.2 raise/clear 归属 — leader-only level-triggered 独立 loop，**不折进 audit-publisher tick**

（采纳 BLOCKER：critic 1/3/4。驳回 drafter 3 的"折进 `AuditPublisher.tick`"，三个已核验理由：① **liveness 反转**——`PublishOnce` 可在未 ACK publish 时早 `return` → alert raise 恰在集群退化时 wedge（"degraded"告警发不出）；② **audit 耐久饥饿**——alert `node.Propose` 在 applyMu 下阻塞会卡 audit-publish drain；③ **idle-zero-writes 破坏**——`WHERE NOT EXISTS` no-op 仍是 committed raft entry，level-triggered 每 tick 重 propose 重破 D5 F1 idle-zero-writes）。

**裁定**：独立、常数计数、leader-gated goroutine `alertReconcile.Run`（自己的 `IsLeader()&&!LeaderContactStale` 门、自己的 ctx-child——leak 门干净，同 publisher loop 形）。仅在**真状态跃迁**时 propose `OpAlertRaise/Clear`（diff desired-vs-current ACTIVE 行，无条件重 propose 禁止）。可**读**缓存 `ReplicaReport` 算 `replication_degraded`、但不发 JS 写。

**per-kind 归属 + clear-条件修正**（采纳 critic 2：clear 必须 `Observed && AllAtTarget`，绝非 `!Degraded()`——后者在 `Observed=false` 时为真 → 对 JS meta-not-ready 瞬态 false-clear/flap）：

| kind | sev | raise | clear（仅正观测） |
|---|---|---|---|
| `replication_degraded` | severe | `report.Degraded()` | `report.Observed && !report.Degraded()`（覆盖 AllAtTarget + empty-set，Stage C m5） |
| `below_quorum` | info | serving-set F==0（一个投影、与 `broker_down` 共享） | F>=1 |
| `broker_draining` | info | `cluster_meta draining:<node>` 存在 | key 清 |
| `broker_down` | info | roster contact-loss（同 below_quorum 投影） | 联系恢复/已 retire |
| `disk_pressure` | info | follower 转发的 level（§3.3） | follower 转发的 clear（§3.3） |
| `raft_lag` | info | **推 D9、writerless**（§3.4） | — |
| `manual` | info | 运维 | 运维 |

### 3.3 disk_pressure 桥接 — level-triggered re-assert + disk.go 新 clear-edge

（采纳 BLOCKER：critic 3/4。已核验 `disk.go` 无下降沿——recover 只 `emitted=false`；drafter 3 的"dip-below 清"无 hook → disk_pressure 会 raise 永不自动 clear）。两改：
1. **disk.go 加 clear-edge** `alertSink`-gated 信号（`else` 重置分支在 emitted→recovered 跃迁时调 `b.alertSink.SignalDisk(active=false)`）。`pubSysEvent` raise 调用字节不变；新信号 `alertSink!=nil` 门（生产 nil）。guard 扫 `disk.go` 允许 inert `if b.alertSink != nil` **读**、禁 `b.alertSink =`/`alertSink:` **写**。
2. **level-triggered 转发避免丢 clear 致 stranded-ACTIVE**：follower 每 tick 向 leader 重断言当前 disk 态（leader 幂等 raise/clear），dedup_key=`disk_pressure:<node_id>`，自愈丢失的 clear（此为唯一 edge-forwarded kind、否则无 reconciler）。转发 verb `VerbAlertSignal` 走现有 §4.1 forwarder（一个 ACL 面）。

### 3.4 raft_lag — **推 D9**（OQ，主进程裁定）

已核验 `clusterstatus.go:60-61/352`：leader 读不到 follower 游标（per-follower applied-lag 是 D9 follower-cursor transport；status 只算 self lag `CommitIndex-AppliedIndex`，leader-meaningful）。drafter 3 的"leader-self audit-publish-lag"是**类别错误**（测 JS-publisher backlog 非 follower 复制 lag）且**误导运维**（看到 raft_lag 疑 follower、实因 leader-local JS）。**裁定**：`raft_lag` 留在 0009 CHECK 目录 **writerless**（同 D5 留 `replication_degraded`），§16 登记"per-follower raft_lag writer 推 D9"。架构 §10.2"不静默推后"由**显式登记**满足；**doc-first 修 §10.2** 那条"trivial 桥接"裁定（桥接非 trivial——per-follower lag 在 D9 前不可得）。

### 3.5 Ack（OQ6）— cluster 级、display-only、永久；重现纯客户端侧

`OpAlertAck` 写 `alert_acks(dedup_key PK, acked_by, acked_at)`。单 cluster 级 ack（非 per-identity）；`acked_by`=ctl nkey、**display-only**；leader 烤 `acked_at`（follower/ctl 不得烤——非确定性）。ack 经 `VerbAlertAck` 转发（无需 reqID——`ON CONFLICT DO UPDATE` 幂等）。ack **仅压内联 ack-prompt、不压 banner**。

**"severe 每新会话重现"**（与 §18.3 删 `session_nonce` 一致）：**无**存储 per-session 态、**无** client session-nonce。ack 行永久；banner **总是**渲染 severe ACTIVE alert（每次 ctl 调用）；ack 仅翻 inline-prompt 位。ack 时 ctl 打印"将于新会话重现"。`alert ls` 显示 `acked_by`+何时（LEFT JOIN、filter `state='ACTIVE'`；回归测试钉 reader 不跨 CLEARED 历史 GROUP-BY）。

### 3.6 客户端合成 gating（OQ7）— VerifyLeader-confirmed、零应答不 gate、advisory 框定

已核验今天无 NATS cluster-health RPC（`clusterstatus.go` 是 adminsock-local）。新增最小只读 NATS RPC `cluster-health`，每 broker 应答（broadcast、无 queue-group），返 `{writable_leader_confirmed bool, leader_id string, force_single_active bool, schema_version int}`。

**leadership 信号必须 VerifyLeader-confirmed、非裸 `State()==Leader`**（采纳 BLOCKER critic 4：分区 ex-leader 在 `LeaderLeaseTimeout` 内仍报 `State()==Leader` → "任意应答声称 leadership→不 gate"恰在丢数据窗口不触发）。health 响应仅在 `VerifyLeaderRead`（已核验存在 `read.go:66`）barrier 通过后答 `writable_leader_confirmed=true`；分区 ex-leader VerifyLeader 失败。follower 答 `false`+已知 `leader_id`（仅 banner 文案）。

**gating 规则**（采纳修正 critic 3/4：丢弃坏的"leader_id 须空"合取）：
- `quorum_lost` 闸触发 iff：**≥1 应答到达** AND **无应答 `writable_leader_confirmed=true`** AND（server 佐证）follower 报 `LeaderContactStale>T_fence`（10s）。任一确认 writable leader → 不 gate；brief 选举（<T_fence）不 gate。
- `force_single_active` 闸触发 iff **≥1 应答**报其为真（读 D7 本地持久事实、非合成）。
- **零应答**（OQ9-C，主进程裁定）→ **不 gate**：零应答 = ctl 自身网络问题/非集群，恰合 §10.4"纯抖动不阻断"；闸需 server 佐证（≥1 应答）。生产 N=1 无 health 响应器 → 永零应答 → 静默（无回归）。

**权威保护是 server 侧写拒绝、client-synth 是 advisory UX**（采纳 critic 1/4：VerifyLeader 仍有 lease 窗）。§10.4 prose 注明：ctl 闸是 best-effort 预检，真安全是 broker 拒绝无法 quorum-serve 的写（loud fail）。不过卖。纯 client 抖动/单 follower 滞后不 gate（另一应答确认 writable leader 即放行）。

### 3.7 Banner（OQ8）— 客户端组装、alert-ls queue-group、corroboration broadcast

（采纳 critic 1/4：驳回 drafter 4 的"每 ps/ls 两 broadcast RPC"——thundering-herd + 固定延迟税）。**拆两需求**：
- **banner alert 集**（reads ps/ls + writes）：一个 **queue-group** `alert.ls` 请求由**任一 broker** bounded-stale 读应答（alert 复制、任 broker 可服务）。一次往返、best-effort（超时 → 照常渲染命令输出、跳 banner）。
- **destructive corroboration**（仅 writes）：**broadcast** `cluster-health` probe（罕见、延迟可接受）。

banner **客户端组装、渲染到 stderr**（stdout 保脚本可解析；测试断言 `ps` stdout 带/不带 active alert 字节一致）。**无** per-Resp-struct `Banner` 字段（驳回 prompt 提示：~12 结构加字段 = wire churn + 每命令 leader 读）。**`--json` 命令抑制 stderr banner**（`cluster status --json` 已在 JSON 携 banner）。

**`below_quorum` 在 N=2 不刷屏**（采纳 critic 3/4：常驻 INFO 每 ps/ls 刷 = alert 疲劳、废掉 severe 闸价值）：**常驻 banner 仅渲 SEVERE**；INFO kind（`below_quorum`/`broker_draining` 等）只在 `alert ls`（按需拉）、不进 banner。

**ACL carve-out**（采纳 BLOCKER critic 1/2/3/4：`cluster.*` broker-only → member 够不到 → banner-for-everyone 不可建）。**裁定（主进程细化）**：两只读 RPC 置于 member-reachable **actor-scoped** `ctrl.by.<actor>.*`（非 broker-only `cluster.*`）：`ctrl.by.<actor>.cluster-health.req` / `.alert.ls.req` / `.alert.ack.req`，narrow `Pub` 加进 `PermissionsForActivatedMember`（贴现有 `session.create.req`/`list.req` 的 actor-scoped、session-无关 *subject* 结构（授权在 activated-member JWT、命令在 session 上下文内运行）；`_INBOX.>` 已在）。加**正向** ACL 测试（member CAN 达这些 ctrl 子主题）+ 保**负向** §13.8（member 仍 CANNOT 达 `cluster.apply.*`）。broker 用 queue-group 订 `ctrl.by.*.alert.ls.req`（一 broker 答 banner）、broadcast 订 `ctrl.by.*.cluster-health.req`（全 broker 答 corroboration）。这是有意、带测的 D3-面 ACL 改。

---

## 4. Doc-first 回写（OQ10）— 精确 prose 编辑（**先落**）

全在 `docs/distributed-broker-architecture.md`：

1. **§10.1 ~331 行 "三条"→"两条"**（外科式，不重写已对的 store-set 列表）：仅改"三条…客户端合成"→"两条…客户端合成（`quorum_lost`/`force_single_active`）；`replication_degraded` store-backed（0009 CHECK、写者=D8b）虽 severe 不硬闸"。
2. **§9 ~305 行 数据面路由 override**（BLOCKER）：改"经 §4.1 转发"→"数据面 push/pull 经 broadcast-SUB + home-keyed gate（home==self 才处理，余静默）；仅 audit 行经 §4.1 leader Apply" + boot-reaper home-gated 注。
3. **§9 ~318 audit 行**：命名 `OpTransferAudit`（纯-Aux 空-Body、`reqID=hex(sha256(transfer_id:kind))` 经 0011 ledger、`q<reqID>:xfer` 去重）+ start/complete/failed 全经 leader Apply（**start 异步、不阻塞 agent-forward**）+ 矛盾终态由 claimFinalize 单-broker 串行 + home-gate 单写 + per-invocation transfer_id 化解（无 terminal-guard）。
4. **§6.3 ~183**：可重导集 = `OpReconcileBatch` **+ OpTransferAudit**。
5. **§10.2 目录脚注**：disk_pressure 经 follower level-triggered re-assert + `VerbAlertSignal` 转发；**raft_lag per-follower writer 推 D9 writerless（桥接非 trivial：leader 读不到 follower 游标，`clusterstatus.go:60`）**；`broker_down` ok/failed 计数 D9（D8b 报 contact-loss、message 不 over-promise）。
6. **§10.3**：banner 客户端组装（非挂回包）：reads 经 queue-group `alert.ls`（severe-only banner、stderr、best-effort）；writes 经 broadcast `cluster-health` 兼 §10.4 闸；`--json` 抑制 stderr banner。
7. **§10.4**：client-synth=两条；闸=VerifyLeader-confirmed + ≥1 应答 + follower `LeaderContactStale>T_fence`；**advisory 预检，权威保护=broker 拒无法 quorum-serve 的写**；零应答/纯抖动/单 follower 滞后不 gate；force-single 永不由 client-synth 单独驱动。
8. **§6.2 / §13.8**：carve-out `ctrl.by.<actor>.cluster-health.req`/`.alert.ls.req`/`.alert.ack.req` member-reachable（非 broker-only `cluster.*`）；positive+negative ACL 测试。
9. **§16 偏离登记 — 新 D8 块**：(a) 两条 client-synth + replication_degraded store-backed；(b) raft_lag/broker_down-计数 推 D9；(c) banner 客户端组装 stderr/severe-only；(d) alert raise/clear=独立 leader-gated loop（非折进 publisher tick）；(e) ack 永久 display-only、重现纯客户端侧；(f) transfer audit 可重导（committed 后）+ 矛盾终态由 claimFinalize+home-gate+per-invocation-tid 化解（无 terminal-guard）；(g) alert-SQL 确定性=有序 Apply + committed 谓词；(h) ACL carve-out ctl health/alert 子主题。
10. **§19-D8 状态 ~632**："三条"→"两条 client-synth + replication_degraded store-backed"；phase-done 翻 checkbox。

---

## 5. Build-and-prove guard & harness（OQ9）

**guard `TestD8ProductionWiresNoCluster`**（镜像 test/d7 token-scan：剥 `//` 注释、self-check `TestD8GuardSelfCheck`）扫 `cmd/tether/serve.go`+生产 broker/agent。

`d8BannedTokens`（cutover **形式**，非裸符号——bans 针对 `alertSink:`/`b.alertSink =`/`transferAuditSink:`/`b.transferAuditSink =`/struct-literal field-write，因 inert `if b.alertSink != nil` **读**在被扫文件里）：`transferAuditSink:`、`b.transferAuditSink =`、`alertSink:`、`b.alertSink =`、`NewAlertReconciler`、`startAlertReconcile(`、`SubscribeClusterHealth(`、`SubscribeAlertLs(`、`INSERT INTO alerts`、`UPDATE alerts`、`INSERT INTO alert_acks`、production 处 propose `OpAlertRaise/Clear/Ack`、`node:`（serve.go/broker struct-literal）。
**允许（live ctl 路径）**：`proto.Subj*` **publish** 在 `cmd/tether`——self-check fixture 证 client `nc.Request(...cluster-health...)` 不被旗、broker `SubscribeClusterHealth(` 被旗。**生产零响应器保证**（Stage C m4 落地修正）：分三道——(i) token-absence 扫描禁 `SubscribeClusterHealth(`/`SubscribeAlertLs(`/`SubscribeAlertAck(` 出现在被扫生产文件；(ii) 这些 responder 构造器**形参要 `*cluster.Node`/`*Forwarder`**，生产从不构造，故即便漏扫也无法 wire；(iii) `TestD8GuardExclusionsJustified` 钉每个排除文件确含 ≥1 banned token（防排除清单悄悄过宽）。**运行时"数订阅==0"的断言需起一个全 production-config broker + 嵌入 NATS 内省其订阅集，推后**（重 harness、收益边际——上三道已闭合攻击面）。

`d8ExcludedFiles`（机制文件）：`transfer_home.go`、`transfer_audit_forward.go`、`alert_ops.go`、`alert_reconcile.go`、`alert_forward.go`、`cluster_health.go` + 继承 `home.go`、`audit_publisher.go`、`cluster_forward.go`。**每个 inert 默认由正向单测钉**（`transferAuditSink==nil`、`xferTargetReplicas()==ReplicasSingle`、`alertSink==nil`），exclusion 不真空。

**分层 guard**：扩 D5 "internal/cluster no-NATS" 扫到新文件；`OpTransferAudit` Aux 在 `internal/cluster` 保 `json.RawMessage`，把 `schema.AuditTransfer` 限到新 leaf `internal/xferaudit`（已核验 cluster 不 import schema）。`internal/clusternodes` 保纯 SQL。

**harness `test/d8/`**（`//go:build d8_integration`、`TestD8Matrix -race`）建在 `startRoutedJS`(d5)+`newHomeBroker`(d6)：N 真 routed broker + health/alert-ls 响应器 + alert loop 订阅。**`TestD8Matrix` 接进 `test/e2e/all_phases_test.go` 带 `-tags d8_integration`**。

---

## 6. 文件级改动清单（**无新 migration**：0011 ledger + 0009 alerts 复用）

**新 leaf `internal/xferaudit/`**：`plan.go`（`PlanTransferAudit`、`ReplayTransferAudit`、`xferAuxV1`、`transferReqID`）；schema 仅此。
**新 `internal/cluster/`**：`alert_ops.go`（`OpAlertRaise/Clear/Ack`、`PlanAlert*`、`DedupKey*`、注册 `genericExecApplier`）；`alert_read.go`（`ActiveAlerts`、`AckedKeys`）。
**新 `internal/broker/`**：`transfer_home.go`（`transferHomeGate`、home-gated reaper 过滤）；`transfer_audit_forward.go`（`emitTransferAudit` 派发、`TransferAuditPayload`、`transferAuditSink` 接缝）；`alert_reconcile.go`（独立 leader-gated `Run`）；`alert_forward.go`（`VerbAlertSignal`/`VerbAlertAck` + payload）；`cluster_health.go`（`SubscribeClusterHealth` VerifyLeader-confirmed、`SubscribeAlertLs` queue-group）。
**新 `cmd/tether/`**：`cluster_health.go`（`probeClusterHealth`）、`banner.go`（`fetchAlerts` queue-group、`renderBanner` stderr severe-only、`withBanner`）、`gate.go`（`gateDestructive`、`--ack-alerts`）。
**改**：`internal/broker/transfer.go`（6× `pubAuditTransfer`→`emitTransferAudit`；`xferTargetReplicas`；home-gate 调用；finalize tracker-presence 路由）；`internal/broker/disk.go`（clear-edge `alertSink` 信号 + level re-assert）；`internal/broker/audit_publisher.go`（`OpTransferAudit` 重导 case；拆 `ObserveReplicas`）；`internal/broker/cluster_forward.go`（`VerbTransferAudit`/`VerbAlertSignal`/`VerbAlertAck` case）；`internal/cluster/command.go`+`clustermeta.go`（注册 4 新 op）；`internal/auth/permissions.go`（member `ctrl.by.<actor>.cluster-health.req`/`.alert.ls.req`/`.alert.ack.req` Pub）；`internal/proto/messages.go`+`subjects.go`（subject SSOT + req/resp 类型）；`cmd/tether/{session,transfer,expose,run}.go`（`gateDestructive`+`--ack-alerts`——最终 NATS-侧 gated 集 session rm/push/pull/expose/expose-rm/run+kill；外审 F2 落地，非 node/proxy/cluster——后者走 adminsock 或有 D7 门）；`cmd/tether/{ps}.go`（`withBanner`）；`cmd/tether/{alert.go,d8_alerts.go}`（strict `alert ls`/`ack` fail-closed，外审 F3/F4）。

---

## 7. 测试计划（映射两出口门；-race + leak 门）

**EXIT-A "tier-B 对象杀 home 存活 N≥3"**：`TestD8TierBSurvivesHomeKill`——必须杀**那个 home broker**（跑 prepare 的、显式辨识），非任意 broker（防真空逃逸）。
**EXIT-B "severe 正确 gate destructive、不误伤"**：drill {全 stale 无 confirmed leader→gate；1 fresh-confirmed leader→不 gate；stale ex-leader VerifyLeader 失败→gate；单滞后 follower→不 gate；force_single→gate；零应答→不 gate}。

**单测（`make test`）**：`OpTransferAudit` plan/replay 字节一致 + forged-Aux poison-skip（与 D7 `errAppliedRejected` 平价）；reqID 派生稳定/hex；**publisher-replays-transfer 带真空对照**（缺 case→静默丢）；`xferTargetReplicas` inert/clustered；`transferAuditSink==nil` 默认；alert raise 幂等 / clear-re-raise history 行 / ack last-writer-wins / 确定性 DIFF-1（两时区）/ clear 需 `Observed&&AllAtTarget`（非 `!Degraded`）；disk clear-edge 触发 + `pubSysEvent` 不变；`ActiveAlerts` 滤 CLEARED；client-synth corroboration 表（EXIT-B 各例）；banner stderr-非-stdout + `ps` stdout 字节稳；guard self-check（client-pub 允许、broker-sub 禁）；ACL 正向（member 达 ctrl health/alert）+ 负向（member 拒 cluster.apply）。

**gated harness（`TestD8Matrix -race`）**：home-routing 无 fan-out（恰一 broker 处理 prepare、真 routed NATS 非 shared-DB sim）；audit 跨选举可重导（幂等、无双行）；EXIT-A；in-flight 重启（无假 complete 行）；rehome 不保在途（tracker-presence finalize、无跨 broker 双 finalize）；假孤儿不被无关 broker boot 收；retire 门计 xfer bucket（含 session 缺于 ListSIDs 的 bucket、JS 枚举证）；alert 复制 raise/clear 经**真** `ReconcileOnce`→`Degraded`（非罐装 report）；alert 跨 leader flap 存活；disk_pressure follower-forward + 丢-clear 自愈；cluster 级 ack 全 broker 可见；EXIT-B gating drill；alert-loop 不被卡住的 publish wedge（liveness 反转回归）。

**-race + 内建 NumGoroutine/fd leak 门**（每个新并发面：watchdog-under-rehome、audit-forward、独立 alert-reconcile loop、`withBanner` 每调用 goroutine 须 cancel+join）。per CLAUDE.md §5，`-race` 单独不够。

---

## 8. 风险排序实现序（riskiest-first）

1. **Doc-first §4**（门一切；尤 §9 数据面 override + §10.1 + ACL carve-out + §16）。prose 不一致前不 build。
2. **`OpTransferAudit` reqID-ledger + publisher 重导 case + 真空测试**（最高 false-green + 幂等风险；端到端验空-Body ledger 写）。
3. **retire 门 JS 枚举 + `ObserveReplicas` 拆**（retire 丢数据风险）。
4. **home routing gate + 未解析静默重试 + finalize tracker-presence 路由**（fan-out 正确性）。
5. **home-gated orphan reaper**（跨 broker 对象删腐败）。
6. **alert store op + 独立 leader-gated reconcile loop + clear-条件**（liveness 反转 + idle-zero-writes 风险）。
7. **client-synth gating VerifyLeader-confirmed + ACL carve-out**（EXIT-B；分区 ex-leader 洞）。
8. **disk 桥接 clear-edge + level re-assert**；**banner queue-group + severe-only**。
9. **guard + harness + e2e 接线 + leak 门**。

---

## 9. 主进程已裁决的残留 OQ（记录）

- **OQ9-A（audit fidelity vs 延迟）= 采纳**：start/complete/failed 全经 leader Apply，但 forward 异步不阻塞 agent-forward；harness 断言"start 不阻塞 agent-forward"。
- **OQ9-B（矛盾终态）= 无 terminal-state guard**：claimFinalize（单 broker 终态原子认领、已核验 callers 318/686/720/932）+ home-gate（每 tid 单 broker）+ per-invocation transfer_id → 矛盾不出现；reqID ledger 管 raft 重试；§16 登记。比 synth 两选项（加 SQL 表 / 接受 benign race）都干净。
- **OQ9-C（零应答边界）= 不 gate**：零应答=ctl 网络抖动/非集群，合 §10.4"纯抖动不阻断"；闸需 ≥1 server 佐证。生产 N=1 永零应答→静默无回归。无需 cluster-mode hint。
- **OQ9-D（force_single_active live 面）= 接受 build-and-prove-only**：force_single_active 仅经 Layer-A health 响应器暴露（D9 live）；N=1 今天运维自己跑 force-single 知情。
- **OQ9-E（ConnectedServerName）= moot**：选静默重试路径，不依赖 self-name。

**净评**：两个最有价值的 draft 发现（broadcast-SUB fan-out、publisher silent-skip）已核验为真且中心化。最危险的 draft 提案（drafter 2 无-reqID JS-窗口幂等、drafter 3 折进 publisher-tick、drafter 4 裸-`State()` quorum 规则 + `cluster.*` member RPC）全驳回 + 给出修正机制。边界在两条 critic 旗标的战线上诚实：member-ACL carve-out（actor-scoped ctrl 子主题 + 正负测试）、force-single live-vs-gated 拆（build-and-prove-only）。
