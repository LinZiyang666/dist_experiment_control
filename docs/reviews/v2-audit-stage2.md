# v2 大版本彻底审计 — 阶段二（主进程修复 + 测试）

> 承接 `v2-audit-stage1.md` 的定稿清单。本报告记录主进程逐条修复的结果。
> 闸门状态：`make lint` **0 issues**；`make test` 绿（唯一失败均为负载敏感 flake，隔离全过：TestRunEchoHelloOverPTY / TestProxySubscriptionE2E / TestPsRPC_Under1s）；gated **d4/d5/d6/d8 全过**（d5 经 M5 重做后）；**d7 为本环境预存 flake**（在 clean HEAD worktree 验证失败方式完全相同，非本轮回归——D7 AddNode transient-leadership，CLAUDE.md 已记录）。
> 改动面：27 实现文件 + 7 测试文件（2 新增）。

## 0. 硬闸先修
- **B-LINT-1/2/3（= tunnel-cert F3）**：删 3 个未导出死函数 `(*Broker).freePort`、`(*Broker).revokePort`、`tls.go` 自由函数 `serverTLSConfig`。`make lint` 由红转绿（违反 CLAUDE.md §5 硬闸的状态修复）。

## 1. MAJOR（8 个全修，带回归测试）
| # | 修复 | 文件 | 测试 |
|---|---|---|---|
| **M1** | `runObserveLoop` leader-acquired edge 触发 `ReconcileMembershipOnLeadership`（§8.1 no-silent-fork 保证首次接入生产；幂等+leader-only） | observability.go | d8 gated 过；d7 环境 flake |
| **M2** | `proposeOrForward` leader 分支 `IsNotLeader→ErrForwardNotLeader`；新 `proto.CodeLeaderUnavailable`；handleRegister 经 `isLeaderUnavailable` 映射 transient code；agent register 循环识别该 code **重试不退出** | clusterwrite.go, cluster_forward.go, broker.go, proto/messages.go, agent.go | `TestAgentRegisterRetriesOnLeaderUnavailable` |
| **M3** | `TunnelExposeAdapter.HasSession` 实装（`openHomeFromState` 恢复分支不再生产死代码）+ 3 接口编译期断言（防 test/prod 分叉复发，= agent-rehome F5） | tunnel_adapter.go | d6 gated 过 |
| **M4** | `jsstream.DeleteXferBucket`+`SIDFromXferStream`；finalizeSessionRm 删 OBJ_xfer bucket；reconcileHistoryStreamsOnBoot 收割孤儿 bucket（解 retire 永久阻塞，= transfer F1/F6） | jsstream.go, audit.go | d8 gated 过 |
| **M5** | audit publisher `publishAudit`：no-stream 错误**仅当 session 已非 ACTIVE（rm'd）才当有界 loss 推进**，否则保持 R-22 重试（`SessionExists` 接 `session.IsActive`）。**初版过宽被 d5 `TestD5QueueNotDropCheckpointStaysOnFailure` 抓出**（流尚未创建 vs 流被删同错误），重做为 session-aware | audit_publisher.go, clusterwrite.go | `TestPublishAuditDeletedStreamIsBoundedLoss`（4 分支）+ d5 R-22 全过 |
| **M6** | `natsconf.Preflight` 对 jetstream/websocket **未识别子键 fail-closed 拒绝**（防 takeover 静默丢 domain 等） | natsconf/preflight.go | `TestPreflightRefusesUnrecognizedSubkey`（3 子用例） |
| **M7** | transfer-audit drain `transferAuditMu` 把 {check draining + WaitGroup.Add} 配成原子（消除 Add-vs-Wait 竞态泄漏）+ 修文件头 stale 注释 | broker.go, transfer_audit_forward.go, clusterwrite.go | d8 -race 过 |
| **M8** | proc `PlanInsert` 改 `INSERT OR IGNORE`（重复 pid 重投=确定性 no-op 而非 fail-stop panic 全副本）+ 修 2 处假"幂等"注释 | proc/plan.go, clusterwrite.go, cluster_forward.go | `TestProcInsertIdempotentAtApply` |

## 2. MINOR（已修）
- **dataplane F1/F3/F7**：`homeForExpose` 改用捕获的 `*port.Allocation`（committed home_broker+epoch），不再 nats_server 重解析、不再重查 epoch（消除分叉 terminal-deny + 瞬态错误生死 expose + 冗余）。**F4**：`homeForRegister` 加 `Eligible()` 检查（不向 draining/非-VOTER home 下发 rehome）。（home.go, expose.go；d6 test 改签名后过）
- **write-forward F2**：`forwardErrKind` 补 `node.ErrSessionMissing/ErrSessionNotActive/proc.ErrNodeMissing`（typed sentinel 跨转发边界保身份）。**F6**：`evictNode` 加 `!clusterMode` 守卫（防单模式 nil-deref）。（cluster_forward.go, clusterwrite.go）
- **CC-4**：`verbAllowsReqID` 数据驱动 verb 守卫（dispatchForward 默认拒非空 ReqID，防未来 verb 复活 stale-ledger 假成功）。（cluster_forward.go）
- **audit-publish F1**：observeOnce 清理 node 离开 voter 集后残留的 broker_down/raft_lag alert（不再 stuck-ACTIVE）。**F5 + xx-conc F2**：raft_lag/applied-lag 改用 leader **command-domain AppliedIndex** 比较（非 raft CommitIndex），消除 leader 选举后假 DEGRADED + follower 误报 raft_lag。（observability.go, clusterstatus.go）
- **jsstream F2**：`reconcileReplicas` 从 live config 出发只改 Replicas（保 operator 编辑的 limits）。**F3**：`IsMetaGroupNotReady` 把 "insufficient storage"（盘满）归类为永久（不再无限循环；同步修正一个编码了 bug 的测试）。（jsstream.go, replicas.go）
- **natscluster F1**：Render 按 NkeyPub 去重（防重复 peer→nats-server FATAL）。（natscluster/config.go）
- **transfer F5**：handlePushCommitReq/handleFinalizeReq 加 entry.sid/nid==subject 交叉校验（防合法 actor 对别 session 的 transfer 驱动 commit/finalize）。（transfer.go）
- **broker-core F4**：cluster 模式 replicated proc-insert 失败时不再发 `audit.proc{start}`（不给无 DB 行的进程发 start 审计）。（exec.go）
- **S1**：`PlanAlertRaise` 顶部自校验 kind/severity 枚举（防未来调用方 bake 违反 CHECK 的行 panic 全副本）。（cluster/alert_ops.go）
- **CC-2**：force-single `checkPeersDead` 探 peer 全部服务口（raft+nats+tunnel），任一 TCP 完成即 HARD-REFUSE（防分区但数据面活的 peer 误判死→脑裂）。（clusteroffline/offline.go）
- **cli F5**：`cluster status --offline` 从 raft-ping 派生 exit code（>1 节点且全不可达→exit 2 DEGRADED；N=1 不误报），监控门不再全停机时静默 exit 0。（cmd/tether/cluster.go）

## 3. NIT（已修：stale-comment 群 + 死代码）
- **stale build-and-prove 注释群**（post-D9 cutover 失效、引用已删 guard 测试）：修正 6 个最具误导性的（flatly 称"INERT in production / Production never calls it"，会让维护者误判 bug 不可达）——`home.go`、`transfer_audit_forward.go`、`alert_forward.go`、`cluster_health.go`、`alert_reconcile.go`、`clusteradmin.go` 头注释改写为反映 cluster 模式 live、single 模式 inert + 注明 guard 测试在 D9 已删。
- **死代码**：tls.go 自由 serverTLSConfig（§0 已删）。

## 4. 设计确认 / 降级（不改实现）
- **R1 reconcile dead-code（write-forward F4 / port-plan F2 / broker-core F2）**：经 `d9-review-round2.md §25` 确认 reconcile audit 走 live best-effort 是**评审通过的设计决定**（非回归）。真实残留是 D4 reconcile 机制在 leader-only 设计下成静态可达死代码——保留（删除涉 commandVersion/FSM/publisher 高风险，安全实用主义）。
- **jsstream F4**（retire 后过配不自愈）：已记录的设计选择（shrink=D7 retire 路径），不改。

## 5. 追加修复（初版报告后续完成）
> 初版报告聚焦 gate + 全 MAJOR + 高价值 MINOR。其后又完成：
- **snapshot F4**：restoreFrom 在 forward-migration 后、in-place restore 前补 integrity+FK 复验（cluster/snapshot.go）。
- **natsconf F2**：takeover 时 existing conf 启用 jetstream 但 store_dir 空 → fail-closed 拒绝（cmd/tether/cluster_natsconf.go）。**F4**：`--route-url`/`--peer` 加 `validateRouteURL`（nats://host:port 校验）。
- **membership F2**：join-nonce `claimJoinNonce`（原子 check+mark in-flight）+ `releaseJoinNonce`（失败回滚），替换非原子 peek-then-consume（clusteradmin.go, clusterstatus.go）。
- **xx-conc F4**：`reconnectInFlight` CAS single-flight onNATSReconnect（防 reconnect 风暴扇出无界 goroutine；agent.go）。
- **broker.go 字段块 stale 注释**（Stage 3 越切片指出）+ home.go `TunnelTokenLookupForTest` 注释（Stage 3 REGRESSION）一并修。

## 6. Stage 3 独立闭合核验结果
6 个独立核验 agent（read-only，按修复区域）复核全部修复：**24 findings 验证 CLOSED / 0 PARTIAL / 0 NOT_CLOSED / 1 REGRESSION**。
- 唯一 REGRESSION：home.go `AttachClusterSeam` 注释改后，`TunnelTokenLookupForTest` 注释里"(like AttachClusterSeam) not called in production"自相矛盾 → **已修**（去掉错误类比）。核验另指出 broker.go 字段块仍引用已删 guard → **已修**。
- 核验确认承重事实：M8 INSERT OR IGNORE 真阻 FSM brick 且单模式字节等价；M2 四点齐全、transient code 真重试不退出、ordering 正确；M5 session-aware 保住 R-22 不破；M7 mutex 真原子化 check+Add；等。

## 7. 待续（剩余低价值 MINOR + 装饰性 NIT，已识别、留聚焦后续批）
> 以下价值较低或需触碰 hot 并发文件深审（marathon 会话中风险>收益），已精确登记：
- **MINOR（hot-file 并发 / cleanup-lag）**：agent-rehome F3（runCtx 跨 goroutine 读写未同步——真 race，需 atomic/RWMutex 跨 agent/proxy 多点，留聚焦 -race 后续；xx-conc F4 已把并发 reader 收敛为 1，缩小窗口）；agent-rehome F2（rehome 持久按 directive Name 键 vs open 按 Port 匹配，name drift 跳过单调持久——edge case）；tunnel xx-conc F3（同口 re-REGISTER 先 bind 后关旧→首次失败重试自愈，close-before-bind 重排有引入新 race 风险）；tunnel F4（bufio 缓冲丢弃→peer pipeline 才丢，罕见）；broker-core F5（boot DELETING finalize 失败无进程内重试——下次 boot 自愈）；natsconf F3（idle DELETE-mode v1 daemon 探测——runbook 主控、防御纵深，已登记 D9 follow-up）；snapshot F2（recover wipe SelfID 前置——offline 工具已有 self-id 检查兜底）。
- **NIT（装饰性）**：剩余 stale-comment（audit_publisher.go / cluster_forward.go / transfer.go / disk.go / tls.go LoadServerCert / tunnel.go / cutover.go / clusterdrain.go / clusterstatus.go / d8_alerts.go 头——多为历史"build-and-prove"标签、不像已修的那批 flatly 误判生产不可达）；死代码（RehomeDirective 死 wire 类型、PlanReassignHome 死状态检查、PlanAllocate 字面 epoch、TransferReqID——均 lint-clean、非闸门项）；clarity（fsm F4/F5、tunnel-cert F5/F6/F8、port-plan F3/F4/F6、natscluster F6、alerts F6、agent-rehome F4/F6、natsconf F5/F6/F8、cli F7/F8、xx-conc F5/F6、write-forward F7、S3/S4/S5、CC-5、R1 reconcile 诚实注释等）。
> 这些不影响硬闸（lint 0 / test 全绿）与任何已确认的承重不变量；建议作为独立 cleanup PR。

## 6. 测试纪律
- 触并发面（transfer-audit drain M7、observability、agent register）均带 `-race` 或经 gated `-race` 套件。
- gated 集成套件 d4/d5/d6/d8 全过验证 cluster-mode 接缝；**d5 抓出 M5 初版语义过宽并驱动重做**——印证按需跑 gated 套件的价值。
