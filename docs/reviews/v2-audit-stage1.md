# v2 大版本彻底审计 — 阶段一定稿（代码审查）

> 范围：整个 **distributed-broker HA epic（proto v2，D0–D9 + "v2 plish"）**，diff 基线 `f484938..HEAD(8eb7f68)` = 323 文件 / +44,536 行（其中 ~28k 行非测试代码，跨 16 个包）。
> 方法：18 个专家切片并行对抗性审查（每切片 finder + 独立 skeptic verifier，全 Opus 4.8），主进程逐条确认并定稿。1 切片（sqlbake-meta-ops）+ 2 个 finder 占位失败（cli-lifecycle / xx-security）已补跑（见 §7）。
> 三阶段流程：**阶段一（本报告）= 审查定稿 → 阶段二 = 主进程修复+测试 → 阶段三 = 独立 agent 闭合核验。**

## 0. 结果概览

| 维度 | 数 |
|---|---|
| 原始 findings | 99（+ 补跑 + 基线） |
| workflow verifier CONFIRMED | 93（0 BLOCKER / 9 MAJOR / 41 MINOR / 43 NIT） |
| REJECTED（主进程同意驳回） | 6 |
| **主进程定稿后净 MAJOR（功能问题）** | **7** + 1 潜伏 footgun（proc INSERT） |
| 基线硬闸 | `go vet` 净 · `make test` 净 · **`make lint` 红**（3 死函数） |

**核心结论**：代码已过 D0–D9 多轮内/外审，**无 BLOCKER**，符合预期。但本轮审计抓到一个关键规律——**多个 MAJOR 正是 D9 cutover / "v2 plish" 接线时引入的集成 bug**：此前 D2–D8 接缝是 inert（build-and-prove），per-phase 审查无法触达；D9 把它们接到生产后，集成层的缺陷无人复审。这正是本轮全量审计的价值所在。

## 1. 主进程定稿：MAJOR（功能问题，阶段二必修）

> 说明：workflow 报了 9 个 final_severity=MAJOR。主进程独立再研判后：**7 个确认 MAJOR 功能问题**；**proc-INSERT** 降为"潜伏 footgun"（今天不可达，但假"幂等"注释会诱发未来 BLOCKER，必修）；**reconcile-dead-code** 从 MAJOR **降级**（经 d9-review-round2 §25 确认是评审通过的设计决定，非回归——真实残留是死代码+失效注释，见 §3）。

### M1 [membership] `ReconcileMembershipOnLeadership` 无生产调用方 — §8.1 no-silent-fork 保证从不在真实 leader 切换时运行
- 文件：`internal/broker/clusteradmin.go:202-277`（定义）；缺失的接线点：`clusterwrite.go:wireClusterLate`
- 全仓 grep：该函数只有 3 个**测试**调用方（clusteradmin_test.go、test/d7）。生产无任何 leadership-transition hook 调它（read.go:30-31 明确放弃 LeaderCh，audit_publisher.go:110 用 IsLeader() 轮询）。
- 后果：leader 在 `raft.AddVoter` committed 后、`setPhase(CATCHING_UP)` 落地前崩溃 → 新 leader 继承一个 `JOIN_VERIFIED_PENDING_VOTER`/`VOTER_ADD_FAILED` 卡死的 roster 行（而该 node 已是活 raft voter）。`clusternodes.Eligible()` 要求 `phase==VOTER`，故 D6 故障切换永远无法 home 到这个长出来的节点。可经运维重跑 `cluster add` 手工恢复 → MAJOR 非 BLOCKER。
- **修法**：在 `wireClusterLate` 起一个 IsLeader()-edge 触发器（或并入 `runObserveLoop`），每次取得 leadership 调一次 `b.cl.admin.ReconcileMembershipOnLeadership()`，失败 log+retry。加 test/d7 drill：AddVoter 与 phase bump 之间崩 leader → 选新 leader → 断言**生产接线**（非手工调用）把行愈合到 VOTER。

### M2 [write-forward] `proposeOrForward` leader 分支泄漏 raw `ErrLeadershipLost` → agent 在常规 leader failover 时**进程退出**
- 文件：`internal/broker/clusterwrite.go:346-359`；`broker.go:946-958`（handleRegister）；`internal/agent/agent.go:771-776`（register 循环）
- 端到端确认：`proposeOrForward` leader 分支直接 `return node.Propose(plan)`，**不映射** `cluster.IsNotLeader`（对比 PIN seam cluster_forward.go:664-668 显式映射）。leadership 在 IsLeader() 检查后、Apply 期间丢失 → Propose 返回 `raft.ErrLeadershipLost` → handleRegister 落到 `replyErr("store_error")` → agent register 循环 default 分支把**任何 OK=false**（含 store_error，注释明列为 "permanent"）当永久拒绝、返回错误**不重试** → Agent.Run 传播 → **agent 进程在一次常规 raft leader failover 时退出**。queue-grouped 的 session/port 动词同样把 `ErrForwardNotLeader` 逐字当终态错误回给 ctl，无重试映射。
- **修法（两侧协同）**：(a) `proposeOrForward` leader 分支 `if cluster.IsNotLeader(err) { return cluster.ErrForwardNotLeader }`（镜像 PIN seam）；(b) handlers（handleRegister/sessions/expose 等）把 `ErrForwardNotLeader`/IsNotLeader 映射成**新的 transient reply code**（如 `not_leader`/`leader_unavailable`）而非 store_error；(c) agent register 循环识别该 transient code → 重试（continue+backoff）而非退出。加 leader-failover-during-register 回归测试。

### M3 [agent-rehome] `HasSession` 生产 adapter 未实现 → `openHomeFromState` 恢复分支在生产中是死代码（clustered expose 重启后静默 DOWN）
- 文件：`internal/agent/tunnel_adapter.go`（缺 `HasSession`）；`internal/agent/agent.go:1075-1086`（type-assert 分支）；`internal/tunnel/tunnel.go:937-943`
- **由 commit 8eb7f68("v2 plish") 引入**：该分支 + `tunnel.Client.HasSession` 是 plish 加的，但生产 `TunnelExposeAdapter` 从未实现 `HasSession(int) bool`，故 `applier.(homeSessionChecker)` 断言在生产中**恒为 false**，`!HasSession → openHomeFromState` 恢复分支死。仅测试 fake 实现它（掩盖 CI）。
- 真实触发（boot-ordering race）：`applyReconciliation`→`applyHomeDirectives`（agent.go:576）在 `replayPortsFromState`（agent.go:583）**之前**跑；directive worker 取非 deferred 分支 → `ApplyHome` → 此时无 session（replay 未跑）→ 返回 nil no-op → **假成功**（持久 epoch、log "rehomed"）→ worker 退出。replay 随后置 deferred=true 但无 worker 再 spawn，直到下次 NATS reconnect。稳定连接下 clustered expose 重启后**一直 DOWN**，agent 却报成功。
- **修法**：`tunnel_adapter.go` 实现 `func (a *TunnelExposeAdapter) HasSession(publicPort int) bool { return a.client.HasSession(publicPort) }`；加编译期断言 `var _ homeSessionChecker = (*TunnelExposeAdapter)(nil)`；加非 fake 集成断言防 test/prod 分叉复发。

### M4 [transfer] `OBJ_xfer-<sid>` object store 永不删 → 孤儿桶永久阻塞 retire（actual<target 时）
- 文件：`internal/broker/audit.go:73-90`（finalizeSessionRm 只删 history 流）；`audit_publisher.go:457-496`（ObserveReplicas 枚举 OBJ_xfer-* 实际流）；`clusterwrite.go:263-291`（ReconcileOnce 只 raise ListSIDs=ACTIVE）
- 全仓无 `DeleteObjectStore`。session rm cascade 删 history 流 + SQL 行，但留 `OBJ_xfer-<sid>`。retire 门 `clusterStreamsReady→ObserveReplicas` 枚举到该孤儿桶 actual<target → `AllAtTarget()==false` → **retire 永久 REFUSED**；同时 `Degraded()` 恒 true → `replication_degraded` alert 永不清。
- verifier 纠正：该 alert **不** gate destructive 命令（gateDestructive 只查 quorum_lost/force_single_active）；且仅当桶 actual<target（建桶时 nVoters 小、后扩容，或丢副本）才触发。
- **修法**：session-rm cascade 加 `DeleteObjectStore(OBJ_xfer-<sid>)`（容忍 NotFound）；boot orphan reaper 删无 session 行的整桶（镜像 history 孤儿删除）；`ReconcileOnce` 从 JS 实际流列表（ListXferStreams）而非仅 ListSIDs raise，使瞬态孤儿收敛而非 wedge retire。

### M5 [transfer] audit publisher 在 history 流被 racing session-rm 删后对该 index **永久 wedge** → 全集群审计停发
- 文件：`internal/broker/audit_publisher.go:240-245,334-354`；`transfer_audit_forward.go`（async 转发与 session 生命周期解耦）
- `OpTransferAudit` 可在 session 转 DELETING + `DeleteHistoryStream` 之后才 committed（async 转发）；publisher 发到 `tether.v2.s.<sid>.audit.transfer`（无流捕获）→ JS publish 报错 → R-22"不推进未 ACK 的 index"规则下**永久重试该 index** → 后续所有 session 审计停发。窗口窄（publisher 落后期 + rm 同刻）但爆炸半径 = 全审计丢失。
- **修法**：`publishTransferAudit` 把"no stream / no responders"类 publish 错误归类为**有界 loud-loss**（loud log + 返回 nil 推进 cursor），与 snapshot-truncation 同姿态（audit_publisher.go:189-206）。

### M6 [natsconf-init] `BuildMergedConf` 静默丢弃 operator 的 jetstream/websocket 子指令（违反 fail-closed 契约）
- 文件：`internal/natsconf/takeover.go:60-105`；`cmd/tether/cluster_natsconf.go:75`；`internal/natscluster/config.go:91-98`
- preflight 只按**顶层 key** 分类（jetstream/websocket=InstallSafe ACCEPT），不查子键。`JSStoreDir()` 只取 `store_dir`，`websocketBlock` 只重发 host/port/no_tls。手工加的 `jetstream { domain; max_file_store }` / `websocket { compression }` 过 preflight 但被 takeover **静默截断**——`domain:` 甚至**总是被丢**（cluster_natsconf.go 根本没把 JSDomain 传进 Config）。包文档承诺"REFUSES rather than silently dropping a hand-tuned conf"，被打破。dry-run 抓不到（截断后仍语法合法）。
- **修法（倾向 fail-closed）**：preflight 对 jetstream/websocket 内的**未识别子键 fail-closed 拒绝**（只放行 install.sh 已知子集），而非静默截断；common install.sh 路径只有 store_dir 不受影响。（备选：generic map walk 逐字重发——更复杂、风险更高。）

### M7 [xx-concurrency] transfer-audit shutdown drain 的 `WaitGroup.Add` 与 `Wait` 竞态（裸 atomic 未配 mutex）→ 泄漏 goroutine + 丢审计
- 文件：`internal/broker/transfer_audit_forward.go:76-84`；`clusterwrite.go:124-125`（clusterShutdownOrdered）
- 经典"Add 与零计数 Wait 竞态"：sink 读 `draining.Load()==false`（76）后被抢占；shutdown `Store(true)+WaitTransferAudit()`（计数 0 返回）；sink 恢复执行 `Add(1)+go`（80-84）→ 未跟踪 goroutine 在 Wait 后运行 → 泄漏 + forward 可能活过 nc.Drain → 丢 terminal 审计（正是 round-1 MAJOR 注释声称已修的丢失）。agent 的 proxyDrainMu 是正确范式，broker 漏了。post-D9 seam 已 live，窗口可达。
- **修法**：用一个 mutex 把 draining-check + Add(1) 配成原子（镜像 agent proxyDrainMu）；clusterShutdownOrdered 设 draining=true 前取同一 mutex。同时修该文件失效的"INERT in production"头注释。

### M8（潜伏 footgun，必修）[port-plan/fsm] proc replicated INSERT 裸写 pid PK 无幂等 → 重复 OpProcCreate fail-stop **panic 全副本**
- 文件：`internal/proc/plan.go:25-54`（PlanInsert）；`fsm.go:199-234`；`clusterwrite.go:440-453`；`cluster_forward.go:90-92`
- `PlanInsert` bake 裸 `INSERT INTO processes(pid,...)`（无 OR IGNORE / 无 pid 存在性预检，只有 nodes FK 预检）。`VerbProcInsert` 带 reqID=""，0011 ledger 不去重。第二个 OpProcCreate(同 pid) 在新 index commit → genericExecApplier 返回 PK 违反的**普通错误**（非 errAppliedRejected）→ fsm.Apply 重试 3× 后 **panic 全副本** = 全集群 crash-loop。`cluster_forward.go:91` 注释"a forwarder retry is idempotent (PlanInsert keys on pid)"是**假的、且危险**。
- **今天不可达**（pid 是 agent-mint ULID 唯一、proc.started 仅发一次、exec.go 只 log 不 retry），但 Forwarder 契约（"retriable 错误用同 reqID 重试"）一旦被 proc 路径采用即变 BLOCKER。
- **修法**：`PlanInsert` 改 `INSERT OR IGNORE`（或加 leader 侧 pid 存在性预检返回 no-op command）；修正两处假"幂等"注释；加测试：同 OpProcCreate 在两个不同 raft index apply 断言不 panic（现有双 apply 测试只覆盖同 index 的 appliedNoOp）。

## 2. MINOR（确认；阶段二修，去重后）

> 去重说明：proc-INSERT（fsm F1 / write-forward F1 / port-plan F1）→ 已并入 §1 M8。reconcile-dead-code（write-forward F4 / port-plan F2 / broker-core F2 / audit-publish F2）→ 见 §3 R1。

按文件/主题分组（完整证据见 workflow 输出 `tasks/w1gbiz7ea.output` 与 scratchpad/minor_nit_detail.txt）：

**home / dataplane（home.go 一组）**
- dataplane F1：`homeForExpose` 从 nats_server 重解析 home 而非读 leader baked 的 `home_broker` → directive 可与权威态分叉并 TERMINAL-deny expose。**修**：读committed `home_broker`。
- dataplane F3：`homeForExpose` 在 epoch 读瞬态错误时返回 nil（无 directive）→ 静默生出死 clustered expose。**修**：瞬态错误返回 transient deny 而非 nil。
- dataplane F4：`homeForRegister` 漏 `Eligible()`（VOTER-phase）检查 → 可下发指向 draining/retiring/非-VOTER home 的 rehome directive。**修**：加 Eligible() 门。

**write-forward / forward 错误映射**
- write-forward F2：`forwardErrKind` 漏 `node.ErrSessionMissing`/`node.ErrSessionNotActive`/`proc.ErrNodeMissing` → typed sentinel 身份跨转发边界丢失。**修**：补这些 kind 映射。
- write-forward F6（NIT→实为隐患）：`evictNode` 缺 `!b.clusterMode` 守卫（其他 proposeOrForward router 都有）→ 单模式 nil-deref footgun。**修**：加守卫。

**audit / observability**
- audit-publish F1：node 离开 voter 集时 orphaned broker_down/raft_lag alert 永不清（stuck-ACTIVE）。**修**：node 离集时清其 alert。
- audit-publish F5 / xx-concurrency F2：leader 自身选举后 applied-lag 误标 cluster DEGRADED；raft_lag 比较 leader CommitIndex vs follower command-domain AppliedIndex（不同计数器）→ 选举史上误报 raft_lag。**修**：用同域计数器比较 / 给 leader 自身豁免窗口。

**membership / status**
- membership F2：leader-local 单用 join-nonce 在并发 admin 连接下 TOCTOU（nonceKnown→AddNode→consumeJoinNonce 非原子）。**修**：原子化 nonce 消费。

**snapshot / recover**
- snapshot F2：offline `recover` wipe 无 SelfID/cluster-state 前置检查 → 可 dump+wipe 错的（或从未 clustered 的 v1）DB。**修**：加前置断言。
- snapshot F4：`restoreFrom` 在 integrity/FK 校验**之后**跑 forward-migrations，且不复验迁移结果。**修**：迁移后复验。

**transfer / jsstream / natscluster**
- transfer F3：无界 `context.Background()` JS object delete 在 NATS handler/watchdog 内 → JS 不可用时卡 handler。**修**：加超时 context。
- transfer F5：`handlePushCommitReq`/`handleFinalizeReq` 不交叉校验 entry.sid==subject sid（与 handleEvTransfer 不一致）。**修**：加交叉校验。
- natscluster F1：重复 peer NkeyPub → Render 发重复 static nkey user → nats-server FATAL（--skip-dry-run 时 brick）。**修**：dedup peers（见 natsconf F5）。
- jsstream F2：`reconcileReplicas`/`raiseXferReplicas` 全配置 UpdateStream 在升副本时静默重置 operator 编辑的 stream limits。**修**：只改 Replicas 字段，保留其余。
- jsstream F3：`IsMetaGroupNotReady` 把"insufficient storage"（盘满 peer）当 transient → reconcile 无限循环、无硬错/日志。**修**：盘满归类为硬错+loud。
- jsstream F4：raise-only-never-shrink 在 retire 后留永久过配（degraded）无自愈。**修**：retire 后允许降配/记录。

**natsconf-init**
- natsconf F2：`JSStoreDir` 解析为空时 takeover 静默禁用 JetStream。**修**：空 store_dir fail-closed。
- natsconf F3：idle-writer interlock 检测不到 idle DELETE-mode v1 daemon（正是它声称覆盖的 case）。**修**：补 DELETE-mode 探测。
- natsconf F4：`--route-url`/`--peer` 无 scheme/format 校验（只靠 dry-run）。**修**：加 URL 校验。

**broker-core / agent**
- broker-core F4：cluster 模式下 replicated proc-insert 失败仍发 `audit.proc{start}`。**修**：失败则不发 start 审计。
- broker-core F5：boot 时 DELETING-session finalize 在 cluster 模式静默失败、无进程内重试。**修**：加重试/告警。
- agent-rehome F2：rehome 持久 `UpdatePortHome` 按 directive Name 键，而 open 路径按 Port 匹配 → name drift 跳过单调持久。**修**：统一按 Port 键。
- agent-rehome F3 / xx-concurrency F4：`runCtx` 在 NATS reconnect goroutine 上读、与 Run 写未同步；`onNATSReconnect` 每次 reconnect spawn 无界 goroutine 无 single-flight → reconnect 风暴扇出。**修**：同步 runCtx + single-flight reconnect。

**tunnel**
- tunnel F4：bufio.Reader 被丢弃、裸 conn 交给 yamux（两侧）→ peer 若 pipeline 握手后字节即静默丢数据。**修**：把 bufio 缓冲交给 yamux 或断言空。
- xx-concurrency F3：`handleAgent` 先 bind 公网口再关同口旧 session → 同口 re-REGISTER 第一次必败。**修**：先关旧再 bind。

## 3. 主进程再研判降级 / 设计确认

### R1 reconcile re-derivable 机制在生产是死代码（write-forward F4 / port-plan F2 / broker-core F2 / audit-publish F2）— **从 MAJOR 降级为 cleanup**
- 依据：`d9-review-round2.md:25` 白纸黑字——register 改 leader-only 后 `reconcileOnRegister` 只在 leader 跑一次（单写者），reconcile audit **故意走 live**（与 proc start/exit audit 一致的 best-effort）。§4.1 的"可重导"是 D4 build-and-prove 机制，**评审决定不在 live register 路径采用**。故"未达成 §4.1 不变量"**不是回归**。
- 真实残留：`VerbReconcile` dispatch / `ReconcileReqID` / `PlanReconcileBatch` / `ReplayReconcileAudit` / publisher 的 `OpReconcileBatch` 分支在 leader-only 设计下成了**静态可达但运行期不走的死代码**（lint 不报，因经 dispatchForward switch 静态可达）；且多处注释仍宣称强可重导不变量。
- **修法（低风险）**：修正失效注释，诚实说明"reconcile audit 走 live best-effort（同 proc start/exit）；VerbReconcile/OpReconcileBatch 是保留但当前 leader-only 路径未接线的 D4 机制"。**不做有风险的删除/重构**（OpReconcileBatch 织入 commandVersion=2 + FSM + publisher + codec）。

## 4. NIT（确认）— 重点是 cutover 后的 stale-comment 群

**stale build-and-prove 注释群（D9 cutover 后失效，系统性清理）**：post-D9 把 D2–D8 接缝接到生产后，大量文件头/函数注释仍写"INERT in production / serve.go never wires this / Production NEVER calls it / 本文件 EXCLUDED from TestDxProductionWiresNoCluster guard"，现在**全部失效误导**。涉及：`transfer_audit_forward.go`(头)、`home.go`(头+多函数：dataplane F2、port-plan F5、broker-core F3)、`alert_forward.go`(alerts F2)、`cluster_health.go`(alerts F3、audit-publish F7)、`transfer.go`(audit-publish F6)、`cluster_forward.go`(write-forward F5)、`audit_publisher.go`(audit-publish F2)、`disk.go`/`d8_alerts.go`(alerts F3)。**修**：统一改写为反映 live cutover 状态，并复核相关 `TestDxProductionWiresNoCluster` guard 测试 post-cutover 是否仍有意义（可能已 vacuous）。

**死代码 / 冗余（含基线 lint，见 §6）**：
- tunnel-cert F3 = 基线 B-LINT-3：`tls.go` 死的自由函数 `serverTLSConfig`（被同名方法取代）。**删**。
- dataplane F5：`RehomeDirective` 类型定义+文档为"leader-pushed backup rehome trigger"但全仓无发布/订阅 — 死 wire 类型。**删或接线**。
- port-plan F3：`PlanReassignHome` 死状态检查（SELECT 已过滤 state='ALLOCATED'）。**删**。
- port-plan F6：`PlanAllocate` 声明恒零的未用 `epoch` 变量。**删**。
- transfer F4：死的粗键函数 `TransferReqID` + reqID 注释与实际 mint 的键不符。**删+修注释**。
- dataplane F7：clustered expose 每次多余的第二次 `resolveHomeForAgent` + 额外 DB 往返（allocatePort 已解析捕获）。**复用已捕获值**。

**其他 NIT（清单见 scratchpad/minor_nit_detail.txt）**：fsm F4（final 失败 attempt 后多睡一次再 panic）、fsm F5（commit-gate seam 三处重复）、tunnel-cert F5（token 未在 Open-replace/Close/cleanup 清零）、tunnel-cert F6（pinned dial 接受过期 home 证书，InsecureSkipVerify 绕过 NotAfter）、tunnel-cert F8（CertPins.Previous/ValidUntil 轮换窗后不清、broker 一直发死 Previous pin）、natscluster F6（auth_callout 无 xkey，callout 请求含 token/PIN 明文走 system account）、alerts F6（cluster 模式 disk pressure 双重出现）、agent-rehome F4（deferredReplay map 永不过期）、port-plan F4（cluster 模式 re-register 不复位 proxy_ready，因 proxy 在 cluster 禁用而 inert）、natsconf F5（--peer 不 dedup self/peers）、natsconf F6（InitFromExisting 必填检查漏 SecretsDir）、natsconf F8（seed self-row added_at 用 RFC3339Nano 与 FSM LitTime 格式分歧、benign-by-construction 但潜陷阱）、xx-concurrency F5（clusterShutdownOrdered loop-join 不验证 loop 已启动→30s stall 若 wireClusterLate 在建 loopDone 后出错）、xx-concurrency F6（publisher/alert/observe loop 全经 applyMu Propose、register 风暴下吞吐悬崖）、write-forward F7（alert sink/ack 即便 leader-local 也总走 NATS self-round-trip）、cluster F4（VerifyLeaderRead 闭包忽略传入 *sql.DB）等。

## 5. REJECTED（主进程复核同意驳回，6 条）
- snapshot F1（follower Restore 竞争 RODB 读）：WAL 快照隔离保证读到 pre/post 一致态、非撕裂 → 驳回；残留仅 Restore‖RODB-read 的 test-gap（可补测）。
- tunnel-cert F1（rehome-following-drop proxy_ready 永久 unready）：被 onNATSReconnect re-ACK 恢复 → 驳回；残留窄瞬态。
- tunnel-cert F2（redial-superseded down-edge 无 up-edge）：teardown 清 readiness filter 过滤 → 降为 NIT。
- cli-lifecycle F1 / xx-security F1：finder 返回**占位符模板**（非真 finding）→ verifier 独立复读确认 6 类不变量均成立 → 驳回（并已补跑，见 §7）。
- broker-core F1（reconcile dead = crash-consistency 回归）：经 d9-round2 §25 确认是设计决定 → 驳回 framing，并入 §3 R1。

## 6. 基线硬闸 findings（主进程独立确认，阶段二必修）
当前 HEAD `make lint` **红**（exit 2），违反 CLAUDE.md §5 硬闸。3 个 unused 死函数（均"v2 plish"遗留）：
- **B-LINT-1** `(*Broker).freePort`（clusterwrite.go:459）零生产调用（test 的 `freePort(t)` 是另一函数）→ 删。
- **B-LINT-2** `(*Broker).revokePort`（clusterwrite.go:551）零调用，注释称"retained"但无调用方 → 删（或确认缺失的 revoke 接线）。
- **B-LINT-3** = tunnel-cert F3：`tls.go:60` 自由函数 `serverTLSConfig` → 删。

## 7. 补跑切片结果

### 7.1 sqlbake-meta-ops（切片状态：优秀，核心不变量全 hold）
- **S1 [MINOR/security] 采纳**：`PlanAlertRaise`（alert_ops.go:68-82）不自校验 kind/severity 枚举——只靠调用方预检。未来某 raise 调用方传 out-of-enum severity → `OpAlertRaise` 经 genericExecApplier bake 的 INSERT 触 `CHECK(severity IN(...))` → 普通错误 → fsm.Apply panic 全副本 brick。**修**：`PlanAlertRaise` 顶部 `if !ValidAlertKind(kind)||!ValidAlertSeverity(severity) { return nil, err }`，把"绝不 bake 违反 CHECK 的 alert"收进唯一 bake 面。
- **S2 [MINOR/test-gap] 采纳**：无多副本测试证明 constraint-violating alert/membership op poison-skip（现有 alert 测试都走 `ExecCommand` 单 DB，绕过 fsm.Apply panic 路径）。**修**：加表测试——out-of-enum `OpAlertRaise` 直经 fsm.Apply 断言不 panic；多-FSM 等价 harness 覆盖 Alert*/Drain/Checkpoint/MetaClear/CertRotate。
- **S3 [NIT/security] 采纳**：test-only `OpClusterMetaSet` 的 key+value 经 `Args []any` 不过 LitText（NUL/非 UTF-8 会被 json 转 U+FFFD）。仅测试脚手架。**修**：加 NUL/UTF-8 运行期守卫或改走 LitText。
- **S4 [NIT/clarity] 采纳**：`clusterPhases`（membership_ops.go:215）注释写成不存在的 `clusterPhaseRank`"orders the phases"，实为无序 set。**修**：改注释。
- **S5 [NIT/redundancy] 并入 S1**：`LitTextAll` 对常量 enum 输入与外部输入一视同仁，掩盖信任边界。随 S1 一起收敛。

### 7.2 xx-security（安全态：强，无 BLOCKER）
所有 fail-closed 控制确认正确实现且互相加固（LeaderContactStale 读路从不 VerifyLeader、errAppliedRejected poison-skip 永不 brick honest replica、positional Aux↔Body 交叉校验防 name-aliasing splice、broker-only `cluster.*` ACL、cert-pin VerifyConnection 含 TLS1.3 resumption、sqlbake NUL/UTF-8 拒绝、ProposeWithReqID leader-gate）。可执行新项：
- **CC-2 [MINOR/security] 采纳**：`checkPeersDead`（offline.go:118-141）force-single peer 探活只 TCP-dial `raft_addr`——raft 口被防火墙/挂但 NATS(:6222/:4222)/tunnel(:7000) 仍服务客户端的 peer 被判死 → 数据面脑裂。**修**：探 peer 所有服务口（tunnel+nats+raft），任一 TCP 完成即 HARD-REFUSE。（运维 `--confirm-peers-dead` 仍是主控，故 MINOR。）
- **CC-4 [MINOR/logic] 采纳**：`validReqID("")`=true → provision/join 的"无转发 ReqID"契约全靠 dispatchForward 里两处手写 `env.ReqID!=""`；未来新动词忘写即复活 external-F1 stale-ledger 假成功。**修**：改数据驱动——`verbForbidsReqID` 表（或 allow-list），未分类动词默认拒非空 ReqID。
- **CC-5 [NIT/security] 采纳**：`forwardErrKind` 未知错误回 `ErrKind:"" + ErrMsg:err.Error()`（原始 leader 错误串跨 broker 总线，受信域内信息披露）。**修**：generic 永久错误分支回固定 ErrMsg（镜像本地 canonical deny）。
- **CC-1/CC-3/CC-6/CC-7 记录但暂不改（设计接受 / 已知 D9 follow-up / 安全实用主义）**：CC-1 ProbeWriterLock WAL idle 假阴性（runbook 主控、daemon 端 flock 是已登记的 D9 follow-up）；CC-3 partitioned ex-leader 有界 fail-open 窗（§3.2/§8.4 已接受；24h JWT TTL 残留是理论硬化，不为理论攻击改）；CC-6 PIN 经 mTLS routes 明文（trust model 接受、Render 强制 verify:true，可选 startup TLS 断言）；CC-7 join nonce leader-local in-memory（设计正确、仅文档澄清）。

### 7.3 cli-lifecycle-status（补跑完成）
- **F1 [MAJOR] = M1 双重确认**：`ReconcileMembershipOnLeadership` 无生产调用方（第二个独立 finder 复现）。补充：卡死行使 `cluster status` 永久 INCONSISTENT/DEGRADED，运维只能手工 `cluster doctor`。修法同 M1（raft.RegisterObserver LeaderObservation 或 runObserveLoop leader-edge）。
- **F2 [clarity] 采纳（并入 stale-comment 群）**：clusteradmin.go 头注释假称"production 从不构造 cluster.Node/ClusterAdmin"并引用 **D9 已删除的 `TestD7ProductionWiresNoCluster` guard 测试**（test/d7/regression_test.go:11-13 + cutover.go:8-10 证实 guard 已删、生产合法构造）。**关键发现**：§4 的 stale 注释群引用的多是**已删测试**，清理时需一并核对。
- **F3 [MINOR/clarity] 采纳**：`NewClusterAdminBackend` 注释称 nil-caughtUp 有"leader-applied proxy"回退，实际代码 REFUSES（clusterstatus.go:424-430）。改注释。
- **F4 [MINOR/clarity] 采纳（重要）**：`DrainNode` doc "Order:" 把 transfer-leader 列在 migrate **之后**，代码在**之前**（正确，per review B5）。维护者照错注释改会复活 B5 半-drain bug。改注释对齐代码。
- **F5 [MINOR/bug] 采纳（新）**：`cluster status --offline`（cluster.go:94-176）exit code 只编码 FORCE_SINGLE(3)，quorum loss/全 peer down 时返回 **exit 0** → cron `status --offline || alert` 在全停机时静默 OK。修：从 ping 结果派生粗 exit code（0 peer 可达且 >1 roster → exit 2），或在 --help 明确"roster 快照非 verdict、不可作监控门"。
- **F6 [MINOR/concurrency] 采纳（= membership F2）**：join-nonce peek-then-consume 非原子，adminsock 每连接 `go s.handle` → 并发 `cluster add` step-2 都过 nonceKnown。`applyMu`+幂等 AddVoter 防腐败，但单用不变量破、冗余 raft churn。修：原子 `claimJoinNonce`。
- **F7 [NIT/redundancy] 采纳**：`cluster drain` 的 `confirmed` var 无 flag 绑定、恒 false（cluster.go:255）。删声明、首调传字面 false。
- **F8 [NIT/smell] 采纳**：`cluster status` 在 cobra RunE 内 os.Exit 跳过清理；online 总 exit、offline 仅非零 exit，不对称。修：把 exit code 上抛 main 统一 os.Exit / 对称化。

---

## 9'. 阶段一定稿结论（Stage 1 COMPLETE）
全 18 切片到齐（含 3 补跑）。**主进程定稿**：
- **MAJOR 功能问题 7 个**（M1 双重确认）+ **1 潜伏 footgun**（M8 proc INSERT）。
- **MINOR 约 30 个**（§2 + S1/S2/CC-2/CC-4/F5/F6）。
- **NIT / 死代码 / stale-comment 群** 一大批（§4 + S3/S4/CC-5/F2/F3/F4/F7/F8）。
- **基线硬闸**：`make lint` 红（3 死函数，§6）必先修绿。
- **设计确认**：R1 reconcile-dead-code 降为 cleanup（非回归）。安全态强、无 BLOCKER。
进入阶段二修复。

## 8. 阶段二修复计划（按文件/批次组织）
1. **硬闸先绿**：删 3 个死函数（B-LINT-1/2/3）→ `make lint` 绿。
2. **MAJOR 修复**（M1–M8，逐条带回归测试）。
3. **MINOR 修复**（§2，按主题批次）。
4. **R1 + stale-comment 群清理**（§3 + §4，系统性改注释 + 复核 guard 测试）。
5. **NIT/死代码清理**（§4）。
6. 每批后跑相关单测；触并发面带 `-race` + 内建 NumGoroutine/fd 泄漏门；最终 `make test` + `make lint` 全绿（+ 视改动跑相关 gated `-race` 矩阵）。
