# Fail - G5/G7 external review

结论：Fail。当前未暂存增量还不能放行，主要原因在 G5 `cluster upgrade` 的 quorum 安全模型仍然把
"本次 health probe 有回复的 broker"当作完整 voter roster。这个假设在真实 HA 事故/维护窗口下不成立：
缺席 voter 会被静默丢出计划，N=3 可被误判成 N=2，最终既可能在滚动升级中打掉 quorum，也可能打印
complete 但漏升真实 voter。另有一个启动窗口 race 会让刚 re-exec 回来的 broker 已能回答 health，却还不能处理
upgrade trigger。G7 本体没有同等级的数据破坏问题，但有几个混版/观测缺口需要修。

## Tasklist / review surface

- [x] 复读 `CLAUDE.md`、核心架构、cluster 手册和既有 review/tasklist 习惯。
- [x] 以当前空暂存区为基线重建未暂存/未跟踪 review 边界。
- [x] 粗读 G5/G7 plan 与内部 Stage-C review，但不把其结论当作权威。
- [x] 审查 G5 signed trigger、planner、driver、broker/agent re-exec、version correlation、docs。
- [x] 审查 G7 proxy home render、remote homes/seeds/status、auto-rebalance gates、JS health。
- [x] 审查 wire/ACL/concurrency/lifecycle/CLI/docs。
- [x] 添加一个独立外审 regression test。
- [x] 运行聚焦测试与 D8 build-tag 套件。

## Findings

### B1 - `cluster upgrade` 仍然从“有回复者”推断完整 voter roster，会在缺席 voter 时破坏 quorum 安全

Anchors: `cmd/tether/cluster_upgrade.go:145`, `cmd/tether/cluster_upgrade.go:160`, `internal/clusterupgrade/plan.go:70`

`buildUpgradeNodes` 只遍历 `probeClusterHealth` 的 replies，并把这些回复折成 planner 输入。新增的 `IsVoter` 只说明“这个回复者是不是 voter”，没有告诉 ctl “配置中还有哪些 voter 没回复”。因此真实 N=3 `{A,B,C}` 中 C 短暂不可达时，计划只看到 `{A,B}`。若 operator 带 `--ack-writefence` 继续，升级会重启 A/B 中的一台；真实 raft config 仍是 3 voters，此时 live voters 可能只剩 1-of-3，直接失 quorum。更糟的是 C 不在 `Steps` 中，roll 可以对 A/B 打印 `rolling upgrade complete`，但 C 从未升级。

这也会误导 N=2 honesty：真实 N=4 掉 1 台会被看成 N=3，无需 `--ack-writefence`；真实 N=3 掉 1 台会被看成 N=2，只要 ack 就会继续，实际风险不同。

Fix direction: 从 leader 的 authoritative raft configuration/cluster status 获取 configured voter set，并要求每个 configured voter 都在 probe 中回答且达到 caught-up/stream-ready 条件；缺一个就 fail closed，不能从 responder 集合推导 voter 集合。

### B2 - 没有 cluster-scoped upgrade lock，per-reload in-flight check 不能阻止并发 membership op

Anchors: `cmd/tether/cluster_upgrade.go:21`, `internal/broker/reexec.go:61`

内部审查的 single-active-op finding 只被部分处理：`handleBrokerUpgradeReload` 在每台 reload 前查一次 `NonTerminalOperations`，但 roll 本身没有持有 cluster-wide reservation。并发 `retire`/`join` 可以在两次 reload 之间启动，也可以在某台 broker 已经停机但下一台尚未检查前进入。这个窗口正是滚动升级最危险的时刻：一个 voter 正在重启，另一个 operator 发起 retire/remove，quorum 可以从“临时少一台”变成“永久少一台 + 正在重启一台”。

Fix direction: roll start 时经 leader 创建 cluster-scoped `cluster_operations` reservation（例如 `cluster_upgrade` sentinel），成功/失败/HALT 都释放；membership op 必须把该 reservation 当作阻塞条件。仅靠每台 reload 前的读检查不构成互斥。

### M1 - upgrade trigger subscription 早于 `clusterAdminHandle` 赋值，存在数据 race 和真实启动窗口 HALT

Anchors: `internal/broker/clusterwrite.go:287`, `internal/broker/broker.go:964`, `internal/broker/broker.go:1013`, `internal/broker/cluster_upgrade_trigger.go:73`

`SubscribeClusterUpgradeTrigger` 在 `wireClusterLate` 内注册；`b.clusterAdminHandle = cab.HandleCluster` 要等 `wireClusterLate` 返回后、admin socket backend 构造时才赋值。NATS callback 在另一个 goroutine 中读这个普通 func 字段，没有同步，race detector 会认为这是读写竞争。

功能上也会触发：broker re-exec 后，health responder 已经上线，`waitVersion` 可以通过；随后 driver 立即发送 `reexec-agent` 或下一步 trigger，此时 `clusterAdminHandle` 仍可能是 nil，目标 broker 返回 `cluster_not_enabled`，roll HALT。这个窗口会在每台 broker 重载后重复出现。

Fix direction: upgrade trigger responder 不应在 admin backend 可用前订阅。把 subscription 移到 `clusterAdminHandle` 赋值之后并纳入 ordered shutdown；或至少用 atomic pointer 加 readiness gate，但顺序修复仍然需要。

### M2 - planner 对多 leader 快照 fail-open，外审 regression 已复现

Anchors: `internal/clusterupgrade/plan.go:78`, `internal/clusterupgrade/plan.go:85`, `internal/clusterupgrade/external_review_test.go:9`

`probeClusterHealth` 收集 600ms 窗口内的广播回复。一次 leadership transfer/election 横跨该窗口时，旧 leader 与新 leader 都可能自报 `WritableLeaderConfirmed=true`。`Compute` 当前只保留最后一个 leader，前一个 leader 既不进入 `Steps`，也不进入 `Refused`，会造成漏升和错误 transfer target。

我添加了 `TestExternalReviewComputeRefusesMultipleWritableLeaders`，当前失败：

```text
ambiguous multiple-leader snapshot must refuse, got steps=[{Kind:upgrade NodeID:c TransferTo:} {Kind:upgrade NodeID:b TransferTo:a}]
```

Fix direction: planner 输入中 leader count 必须恰好 0/1；大于 1 时 fail closed，提示重跑等待 leadership 稳定。

### M3 - `waitVersion` 仍不是 plan 要求的 converge barrier

Anchor: `cmd/tether/cluster_upgrade_drive.go:120`

barrier 只要求目标 broker 版本等于 target，且 `AppliedIndex` 不落后 writable leader 超过 64。它没有确认目标仍是 configured voter、role/phase 正确、reachable、JetStream stream replicas 达 target、无 inconsistent。这个门比 G5 plan 中的 “Phase==VOTER && role voter/leader && AppliedLag==0 && Reachable && StreamActual==StreamTarget && !Inconsistent” 低很多。结合 B1 的缺席 voter 问题，roll 很容易把“进程能回答 health”误当成“下一台可以安全重启”。

Fix direction: 用 authoritative status/converge predicate；至少要求目标在 configured voter set 中、leader 视图无 inconsistent、AppliedLag==0。stream readiness 若 ClusterHealthResp 当前没有字段，需要新增 additive report 或在 leader status 路径查询。

### m1 - `ProxyHomeCount` 没有 reported discriminator，混版 `--homes --remote` 会把 unknown 当 0

Anchors: `internal/proto/alerts.go:33`, `cmd/tether/cluster_status_nats.go:213`

`cluster status --homes --remote` 复用 `cluster-health.req`。老 broker 会正常回复，只是 JSON 没有 `proxy_home_count`，ctl 解码为 0。`foldProxyHomeCounts` 无法区分“该 broker 真的有 0 个 proxy home”和“该 broker 太旧，根本没报告这个字段”。滚动/混版窗口下会静默低估分布，正好削弱 G7 #16/#18 想解决的“分布是否均衡”可观测性。

Fix direction: 像 `TopoReported` 一样加 `ProxyHomeReported bool`；未报告时 render `?` 或标注 unknown，total 也不能把 unknown 当 0 求和。

### m2 - socket `cluster status` 没有接入 runtime `JetStreamUnavailable`

Anchors: `internal/broker/clusterstatus.go:324`, `internal/broker/clusterstatus.go:330`

新的 sustained JS-503 信号只进入 `cluster status --remote`。broker host 上的 socket `cluster status` 仍只在 `forceSingle && nats.conf clustered` 时追加旧 banner。非 force-single 原因导致的 JS-503 会在 laptop `--remote` 里显示，但 operator SSH 到 broker host 后反而看不到同一个 runtime 信号。这和 G7 plan “remote/socket 协调统一”的目标不一致。

Fix direction: socket status 已经有 health poll map，可 OR 聚合 `ClusterHealthResp.JetStreamUnavailable` 并追加同类 banner，注意与旧 force-single banner 去重。

### m3 - 生成的备份文件被留在工作区

Anchor: `test/d8/integration_test.go.bak_orig`

工作区有一个未跟踪 `.bak_orig` 文件。它不是 Go test 文件，但按用户要求最终会被 `git add`，容易把本地备份噪声提交进仓库。若确实只是手工备份，应在主进程处理阶段删除；如果保留，应解释用途并改成正式 fixture 名称。

## Doubts / residual risk

- `reexec-agent` 通过 `proto.SubjCmdForwarded(req.SID, colocatedAgentNID, "upgrade")` 转发，而 `SID` 来自 ctl 当前 active session。cluster upgrade 是 account-seed 授权的集群级操作，但 agent re-exec 实际依赖某个 session 下存在同机 agent。文档没有明确“必须登录包含每台 colocated agent 的 session”，配置里也没有 `colocated_agent_sid`。如果 operator 的 active session 不含这些 agent，broker 会先升级，然后 agent leg HALT，形成半升级。
- `handleBrokerUpgradeReload` 仍允许本地/admin signed caller 省略 `ExpectSHA256` 直接 reload；CLI 会强制 `--expect-sha256`，但 broker primitive 本身不是 fail-closed。鉴于 account seed / local socket 都是高权限，这不是主要 blocker，但它和“reload 总验 on-disk 二进制”的注释不一致。
- `TETHER_AUTO_REBALANCE=on` 只写进 `docs/cluster.md` 新段落；`docs/usage.md` 的环境变量表和 broker ops/runbook没有同步。默认关是合理的，发现性还不够。

## Verification

Passing:

- `go test ./cmd/tether -run 'Test(G7|Correlate|RenderUpgradePlan|SignUpgradeTrigger)' -count=1`
- `go test ./internal/broker -run 'Test(G5|G7|Reload|UpgradeTrigger|RequestReExec)' -count=1`
- `go test ./internal/agent -run 'TestG5|TestUpgrade|TestSha' -count=1`
- `go test ./cmd/tether ./internal/broker ./internal/agent ./internal/proto -run 'Test(G5|G7|Reload|UpgradeTrigger|RequestReExec|Correlate|RenderUpgradePlan|SignUpgradeTrigger|ClusterHealth|Proto)' -count=1`
- `go test -tags d8_integration ./test/d8 -count=1` (rerun outside sandbox because local TCP listen is blocked inside sandbox)
- `git diff --check`

Failing external-review regression:

- `go test ./internal/clusterupgrade -count=1`

Failure is intentional evidence for M2 until planner fail-closes on ambiguous multiple-leader snapshots.

Simcluster not run: the review already has deterministic G5 fail findings in the hermetic code path. A deploy-tier drill would be useful after B1/B2/M1 are fixed, especially for the PID-preserving broker re-exec and N=3 no-write-fence claim, but it would not change this review verdict.

---

## 主进程回复（逐条采纳/修改）

全部 8 条 finding + 3 条 doubt **均采纳并修复**。三硬闸复跑全绿：`make lint` = 0 issues、`make test` 全包 ok、`make e2e` 串行矩阵 PASS（537s）。逐条如下（anchor 为修复后的当前行号）。

### B1 — ✅ 采纳并修复：不再从 responder 推断 voter roster，缺席 voter fail-closed

`buildUpgradeNodes`（`cmd/tether/cluster_upgrade.go:131`）改为先经 NATS 拉 leader 的 **account-signed manifest**（`fetchManifestOverNATS`，与 discovery 同一权威 roster），从 `m.Roster.Brokers` 里挑 `Phase == proto.RosterPhaseVoter` 得到**配置态 voter 全集**；再要求每个 configured voter 都在本次 health probe 里回答，**缺一个就 fail-closed**：

```
cluster_upgrade.go:172-183
  configured voter(s) %v did not answer the cluster-health probe — cannot verify a quorum-safe
  roll (restore them or wait); refusing to infer the voter set from responders
```

这样真实 `{A,B,C}` 中 C 缺席时不会被看成 `{A,B}`——roll 直接拒、绝不"漏 C 却打 complete"，N=2 honesty 也以配置态 voter 数（而非 responder 数）判定。**仅当 manifest 完全不可得（老集群/无签名 roster）**才退回 responder 集合并在日志标注，避免把新语义变成对老部署的硬破坏。`RosterPhaseVoter` 常量对齐 `internal/proto`。

### B2 — ✅ 采纳并修复：cluster-scoped 滚动锁 + membership 互斥

用 **replicated `cluster_meta` marker**（key `cluster_upgrade_active`）实现 cluster-wide reservation，镜像既有 `force_single_active`：

- `internal/cluster/membership_ops.go:388-402` — `MetaKeyUpgradeActive` + `PlanSetUpgradeActive`(UPSERT) / `PlanClearUpgradeActive`(DELETE)。
- orchestrator（`cmd/tether/cluster_upgrade_drive.go`）roll start 经 leader 的 `acquire-lock` signed trigger 取锁、**干净跑完**才 `release-lock`；**HALT / Ctrl-C 有意保留持锁**（成员变更在部分升级态持续被挡），锁是幂等 UPSERT，重跑（继续升或回滚）自愈。
- broker trigger handler（`internal/broker/cluster_upgrade_trigger.go:136-150`）新增 `acquire-lock`/`release-lock`，leader-only `Propose`，与其余 op 同样受 account 签名门。
- membership op 把该 marker 当**阻塞条件**：`StartJoinOperation` / `StartRetireOperation`（`cluster_operation_controller.go`）在最前置门 `upgradeActive(RODB())` 直接拒。

**为何用 marker 而非真正的 `cluster_operations` op**：reload 原语本身带 Stage-C M4 的 `NonTerminalOperations` 自检（in-flight op 时拒 reload）——若 roll 用一条真 operation 占位，会**自己把自己每台的 reload 挡死**。marker 与 operation 表解耦，既互斥 join/retire、又不毒化 roll 自身。回归：`TestB2UpgradeLockBlocksMembership`（join+retire 双拒、释放后 join 放行、证明锁在 nonce 消费前拒）、`TestUpgradeTriggerAcquireLock*`（签名门 + 非集群 fail-closed + M1 交互）。

### M1 — ✅ 采纳并修复：mutex 消 race + retriable 码消 HALT

两面都修：
- **race**：`clusterAdminHandle` 现由 `clusterAdminMu sync.RWMutex` 守护（`broker.go:426`），写经 `setClusterAdminHandle`（`broker.go:1016`）、读经 `clusterAdminHandleFn()`。`-race` 下 `make test` 干净。
- **启动窗口 HALT**：nil handle 时**仅 reload/transfer-leader** 返回 **retriable** `cluster_not_ready`（`cluster_upgrade_trigger.go:95-99`）；ctl 侧 `sendUpgradeTrigger`（`cluster_upgrade.go:256-269`）对该码在 `upgradeConvergeTimeout` 内重试——re-exec 后的窗口成为"短暂等待"而非 HALT。`acquire-lock`/`release-lock`/`reexec-agent` 不需 handle，不被该门挡（`TestUpgradeTriggerLockOpsBypassHandleNilGate`）。

关于"顺序修复仍然需要"：我采用你列出的**替代路径（同步指针 + readiness gate）+ ctl 重试**，二者在结果上等价于 reorder（race 由 mutex 根除、窗口由 retriable 码 + 重试消解）。没有把 subscription 物理移到 handle 赋值之后，是因为 subscription 属于 ordered late-wiring / ordered-shutdown 链，硬挪会破坏该顺序契约；而 readiness-gate 达成同一安全性且不动契约。

### M2 — ✅ 采纳并修复：多 leader 快照 fail-closed（外审 regression 现绿）

`internal/clusterupgrade/plan.go` 在 `N2WriteFence` 之后统计 `leaderCount`，`> 1` 即 `Refused` 并 return：

```
ambiguous snapshot: more than one broker self-reports as the writable leader
(a leadership transition spanned the probe window) — wait for leadership to stabilize, then re-run
```

你新增的 `TestExternalReviewComputeRefusesMultipleWritableLeaders` 现在 **PASS**（`go test ./internal/clusterupgrade` 全绿）。

### M3 — ✅ 采纳并修复（可达子集）+ 诚实标注 stream-ready 为后续

`waitVersion`（`cluster_upgrade_drive.go:120-145`）barrier 增强为：目标 broker 版本达标 **且** 仍是 configured VOTER（`h.IsVoter`）**且** 未被 leader fence（`!h.LeaderContactStale`）**且** applied-lag ≤ 64。**stream-replica readiness（StreamActual==StreamTarget）当前不在 `ClusterHealthResp` 里**——我没有假装它已被覆盖：注释明确标注这是"NATS 可达子集"，stream-ready 作为 additive-report 的 documented follow-up。其余四轴（voter/leader-view/reachable/applied）已达 plan 语义。

### m1 — ✅ 采纳并修复：`ProxyHomeReported` discriminator

`internal/proto/alerts.go` 加 `ProxyHomeReported bool`（additive omitempty）；responder（`cluster_health.go:99-102`）在填 `ProxyHomeCount` 时一并置 `true`；ctl `foldProxyHomeCounts`（`cluster_status_nats.go`）保留整条 reply 的 `Reported`，`renderHomesRemote` 对未报告 broker 打 `?`、total 显示 `N (+M unknown)`——不再把 unknown 当 0。

### m2 — ✅ 采纳并修复：socket status 接入 runtime `JetStreamUnavailable`

`ClusterAdmin` 加 `jsUnavail func() bool` + `SetJSUnavailFn`（`clusteradmin.go`），`clusterwrite.go` 用 `b.jsUnavail.Load` 接线；`StatusReport`（`clusterstatus.go`）OR 聚合 `jsClusteredForceSingle || jsUnavail()`，force-single 保留原专属补救 banner、其余 JS-503 打通用 `DATA-PLANE DEGRADED: JetStream is UNAVAILABLE (sustained 503)` banner，两者**去重为单条**。SSH 到 broker host 的 socket status 现与 laptop `--remote` 看到同一 runtime 信号。

### m3 — ✅ 采纳并修复：删除备份噪声

`test/d8/integration_test.go.bak_orig` 已删除（纯手工备份，无 fixture 价值）。

### Doubt · reexec-agent SID 作用域 — ✅ 文档补齐

`docs/cluster.md §5.6.10` 新增明确条目：**运行 `cluster upgrade` 的 ctl 必须登录在能看见这些同机 agent 的 session**（agent leg 经 session-scoped `SubjCmdForwarded(<SID>, colocated_agent_nid, upgrade)` 投递）；否则 agent leg `agent_no_responders` 而 HALT、broker 已升成半升级——补救是切到正确 session（或把同机 agent 挪进该 session）后重跑。broker 守护重载走 account-signed 广播、不受 session 影响。

### Doubt · reload primitive 不 fail-closed — ✅ 改为 fail-closed（升级为修复）

不止文档：把原语本身改成**强制 sha**。`handleBrokerUpgradeReload`（`reexec.go`）空 `ExpectSHA256` 直接 `bad_request` 拒（不再 `if != ""` 跳过）；对称地 agent `handleReExecOnly`（`internal/agent/upgrade.go`）空 `SHA256` 返回 `sha256_required`。杜绝"无 sha 守护就 re-exec 盘上任意 image"，与"reload 总验 on-disk 二进制"的注释一致。回归：`TestReloadRefusesEmptySHA`、`TestReExecOnlyRefusesEmptySHA`、`TestReExecOnlyRefusesShaMismatch`。

### Doubt · `TETHER_AUTO_REBALANCE` 发现性 — ✅ 同步进 broker-ops

该 env 是 **broker 启动开关**，归属 `docs/broker-ops.md §5.5`（`tether serve` 段新增"环境变量"小节，含默认关的理由 + `Environment=` 持久化 + 交叉引用 `cluster.md §5.6.11`）。usage.md 是 ctl/agent 手册、不是该 env 的家，故落 broker-ops。

### 部署层验证（simcluster）

你建议 B1/B2/M1 修好后跑 deploy-tier drill 验 PID-preserving re-exec + N=3 no-write-fence。这几项 hermetic Go 套件结构上够不到，确应由 `test/simcluster` 的 N=3 rolling-upgrade drill 验。此为**独立部署层门、不改本轮 verdict**；weilandserver 现可经本机 `tether ctl` 够到，待你确认后我跑相关 drill（不整套 loop）。
