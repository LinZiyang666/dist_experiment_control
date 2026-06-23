# D8 review — 文件传输分布式 ‖ 告警系统（Stage C 内审 + 主进程处置）

> **流程**：Stage C 多专家对抗审查（6 reviewer 不同风险镜头 → 1 synth；全 Opus 4.8、静态 fan-out）。专家**只读实现、可新增测试条目、绝不改实现**；**主进程**为唯一裁决+修复人。synth 报告原文见任务输出（`w2dcavkgf`）。本文件记录每条 finding 的处置。
>
> **总评（synth）**：D8 核心机制**全部 SOUND，复制状态机无 BLOCKER**——可重导 transfer audit（`PublishOnce` 显式 `OpTransferAudit` case、非 silent-skip）、派生-reqID 经 0011 ledger 去重、alert 确定性（有序 Apply + committed-state 谓词 + UTC/NUL/UTF-8 烤守）、idle-zero-writes（nil-command no-op）、`Observed && AllAtTarget` clear 谓词、home-gate 反 fan-out、client-synth gate（VerifyLeader-confirmed）、L-2 分层、build-and-prove 无 cluster.Node 保证。计 **1 BLOCKER（doc 调和）+ 4 MAJOR + 6 MINOR**，全部处置如下。

## BLOCKER

### B1 — finalize epoch-fence doc-first 矛盾 → **采纳（prose 调和）**
§9 prose 写"finalize 按 tracker 持有路由 **+ D6 home-epoch 围栏**"，但代码（transfer_home.go）+ §16(f) 说**无 fence**（claimFinalize + home-gate + per-invocation tid）。synth 正确指出 §9 与代码/§16(f) 冲突，且这是 phase #1 doc-first 门。
**主进程裁决**：实现期（task #33）选了更干净的 **tracker-presence 路由 + per-invocation transfer_id**——epoch-fence 确实不需要：finalize 只被持该 tid tracker entry 的源 broker 处理（claimFinalize），对象按 tid 键，并发 rehome 不产生跨 broker 双绑（旧 home 只 finalize/删自己 tid 的对象=正确；rehome 后是新 tid 走新 home）。§9 的"+ home-epoch 围栏"是 Stage-A synth 残留 stale 措辞。**修**：改 §9（删 fence 子句、写实际 rationale、与 §16(f) 一致 + 区分 START=home-gate / continuation=tracker-presence）；改 plan §2.1/§6/§7/§8 的 4 处 fence 措辞。**纯 prose、无代码改**（威胁模型已亲手核验）。

## MAJOR

### M1 — gateDestructive 在 N=1 强加 ~600ms 延迟（破坏"byte-identical at N=1"墙钟）→ **采纳**
`probeClusterHealth` 手卷 `SubscribeSync`+`PublishRequest`、循环到固定 600ms window，无 `Conn.Request` 的 no-responders 快path → N=1 下 `session rm` 每次阻塞 600ms。
**修**：probe 检测服务器 503 no-responders 哨兵（`len(data)==0 && Header["Status"]=="503"`）→ 立即返回（cmd/tether/d8_alerts.go）。harness `probeHealth` 同改。**测试**：testClusterHealthGate 加 no-responder 快path 断言（订阅前 probe → <300ms 返回 + 零 reply + 不 gate）。

### M2 — ACL carve-out session-依赖，与"session-independent/for everyone"prose 矛盾 → **采纳（prose 澄清）**
三 carve-out subject 只在 `PermissionsForActivatedMember`，无 session 的 ctl auth-DENY。synth 指出 prose 过度声称。
**主进程裁决**：subject 确是 cluster-wide（actor-scoped、无 sid），但授权落在 activated-member——**破坏性命令（session rm / expose rm…）本就需 session**，故 activated-member scope 充分且更紧（不授无 session CLI）。§10.3"所有人"是原架构"无 role 脱敏分层"之意（无关、正确）。**修**：澄清 permissions.go 注释 + plan §3.7（subject session-无关、授权在 activated-member、命令在 session 上下文）。**测试**：ACL 测试加负向——`PermissionsForUnactivated` 不含三 subject（钉死 activated-only scope）。

### M3 — 跨选举 re-derive drill 真空（声称证 dedup-collapse，实际新 leader 继承复制游标、跳过条目、不重发）→ **采纳**
synth 正确：游标 `audit_published_index` 复制，新 leader 从 `cur+1` 起、跳过 transfer-audit 条目→不重发，`==1` 只证"没第二次发布"非"dedup 收拢"。
**修**：drill 改为选举后**二次 forward 同 (tid,complete) 记录**（新 raft index、同 content-derived reqID）→ 断言 **(1) 0011 ledger dedup（`totalDedup` 跨活 node 求 `DedupCount` 之和增长）+ (2) JS msg-id dedup（history 行数仍恰 1）**。两个幂等锚都真被 exercise。

### M4 — async transfer-audit forward 面零覆盖、承诺的 leak gate 定义但从不调用（违 CLAUDE.md §5 并发面 leak 门纪律）→ **采纳**
synth 正确：集成套件经 `Forwarder.Forward` 直发、绕过 `AttachTransferAuditSink`；async goroutine + `WaitTransferAudit` + leak gate 全未调用。（reviewer 1 已确认 goroutine **终止**（bounded attempts×backoff）、非运行时泄漏——M4 是**覆盖**缺口。）
**修**：抽 `attachTransferAuditSinkWith(forward func([]byte) error)` 为可测核心。**测试**（broker 包、fake forward）：(a) emitTransferAudit 非阻塞（forward 阻塞时调用立即返回）；(b) `WaitTransferAudit` 后 NumGoroutine 回基线（**leak 门**）；(c) `ErrForwardNotLeader`-then-ok 重试到第 3 次、permanent error 单发不重试。

## MINOR

### m1 — leader 侧 VerbAlertSignal 不校验 forwarded Kind/Severity；越界值会 fail 0009 CHECK → FSM fail-stop（alert op 无 poison-skip）→ **采纳（防御加固）**
当前不可达（唯一写者 `AttachAlertSink` 硬编码 disk_pressure/info、bus broker-only），但防未来 verb caller/沦陷 peer。**修**：抽 `planAlertSignal` 加 `cluster.ValidAlertKind`/`ValidAlertSeverity` 校验——越界值返 nil command（不写、不 poison）。**测试**：`TestD8PlanAlertSignalRejectsBadEnum`（quorum_lost kind / bad severity / 空 kind → nil command）。

### m2 — disk_pressure VerbAlertSignal transition 门（raise/clear/idle-zero-write）无 e2e 覆盖 → **采纳**
disk_pressure 是唯一走 forward 路径的 kind；其 idle-zero-writes（§16 D8(d) 拆分目的）是独立代码路径，翻转的 switch 臂会过其它所有测试。**修**：`planAlertSignal` 抽出后，加 `TestD8PlanAlertSignalTransition`（active+无行→raise；active+有行→nil 无写；!active+有行→clear；!active+无行→nil）。

### m3 — clustered（selfID!=""）home-gate/reaper 零覆盖、且测试注释假称已覆盖 → **采纳**
home-gate 是承重反 fan-out 机制，唯一测试是 inert `selfID==""` 路径。**修**：`TestD8TransferHomeGateClustered`（seeded sessions+nodes+cluster_nodes：home==self→proceed、home!=self→拒、unresolved→拒）+ `TestD8HomeOwnsXferBucketClustered`（单 node homed-self→可收、混合 home→拒）。改正 transfer_home_test.go 假注释指向新测试。

### m4 — guard 弱于 plan §5：缺正向订阅计数断言；`d8ExcludedFiles` 过宽 → **采纳 (b)，(a) 注记**
**(b) 修**：删 6 个零-D8-token 的继承排除项（home.go/audit_publisher.go/cluster_forward.go/clusteradmin/drain/status.go 现重新被扫）、只留 5 个真 D8 机制文件；加 `TestD8GuardExclusionsJustified`（每排除文件须含 ≥1 banned token、防过宽）。**(a) 注记**：运行时"数订阅==0"断言需全 production-config broker + 嵌入 NATS 内省，推后——已被三道（token-absence + responder 构造器要 `*cluster.Node`/`*Forwarder`（生产不构造）+ exclusion-justified self-check）闭合（plan §5 已注）。

### m5 — replication_degraded 在 `Observed && len(Streams)==0` 时可 wedge ACTIVE（latent、当前不可达）→ **采纳（廉价防御）**
当前 `ObserveReplicas` 总含 events 流（len≥1）故不可达，但防未来/替代 Observe。**修**：clear 臂从 `rep.AllAtTarget()` 改 `!rep.Degraded()`——覆盖 AllAtTarget **与** len==0；仍在 `else if rep.Observed` 内、不重引入 transient false-clear。

### m6 — stale LeaderID 渲染无 provenance（当前无害）→ **驳回（无需修）**
reviewer 自认 `EvalDestructiveGate` 忽略它、`renderBanner` 不打印它、纯 banner 文案 best-effort。已是 best-effort 文档化（wire 注释"banner text only"）。无需改。

## 新增/修改测试（专家提议、主进程整合）
1. `cmd/tether` + `test/d8` no-responder 503 快path（M1）。
2. `internal/auth` ACL 负向 unactivated（M2）。
3. `test/d8` 真跨选举 re-derive（M3，`totalDedup` + JS 行数）。
4. `internal/broker/transfer_audit_forward_test.go` async/leak/retry（M4）。
5. `internal/broker/alert_forward_test.go` planAlertSignal transition + enum 拒绝（m1+m2）。
6. `internal/broker/transfer_home_cluster_test.go` clustered home-gate + reaper（m3）。
7. `internal/broker/alert_reconcile_test.go` reconciler Run-cancel leak（早补）。
8. `test/d8/regression_test.go` `TestD8GuardExclusionsJustified`（m4）。

## 门（全绿）
`go build ./...` 0 · `make test` exit 0 · `make lint` 0 issues · gated `TestD8Matrix -race`（5 drill：alert 复制 raise/clear+transient 不误清、cluster 级 ack 带认证 actor 复制、真跨选举 re-derive（ledger dedup + JS 行数 1）、VerifyLeader-confirmed health gate + no-responder 快path、EXIT-A tier-B 杀 home 存活）· `make e2e`（TestAllPhases + TestD1–D8Matrix）。

### 另修 3 处满负载 flake（"有问题就修掉不要留"；非 D8 回归，均隔离通过——D8 给 e2e 矩阵加了 ~36s clustered-JS 工作量、抬高总竞争触发已有 timing-敏感测试）
- **D5 harness setup-timeout**：`startRoutedJS` 的 `ReadyForConnections`/route-mesh/JS-meta 就绪超时 10s/10s/20s → 30s/20s/30s（满负载下嵌入 NATS 启动慢，"routed JS server not ready"）；d5+d8 共用模式同改。
- **D7 ForceSingleRecover "leadership lost while committing log"**：seed `AddNode` 在 transient raft leadership-change 下重试（`addNodeRetry` helper + `d7TransientAddErr`，membership 两阶段幂等）；应用到全部 5 个 seed 站点（ghost/half-state 期望错误的直调不变）。
- **P13 ProxyFalseOnlineRecoversAfterTunnelDrop**：`proxy_ready` clear 等待 5s→15s（满负载下 tunnel-drop 检测 + /sub re-advertise 更慢；隔离 ~0.5s 清）。

**结论**：核心机制 SOUND；B1 doc 调和 + M1–M4 + m1-m5 全采纳修复并加回归测试；m6 驳回（无害）。待外审。
