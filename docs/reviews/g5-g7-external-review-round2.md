# Fail - G5/G7 external review round 2

结论：Fail。开发者确实修掉了一批上一轮问题（多 leader planner 现在 fail-closed，SHA 强制、proxy reported
discriminator、备份噪声和部分文档都已闭合），但 G5 `cluster upgrade` 的互斥/恢复设计仍不够上线。当前锁可以
在清理失败时被伪装成已释放，已有 membership operation 仍会在锁存在时继续推进，并且升级 planner 仍可能使用
过期 discovery manifest 漏掉真实 voter。G7 还残留一个 socket 状态面没有真正聚合 peer JS-503 的问题。

我没有信任主进程追加在上一轮报告里的回复，只把它当作变更索引；本轮重新读 diff、补外审测试并独立跑验证。

## Tasklist / review surface

- [x] 复读 `CLAUDE.md`、既有 docs/reviews 风格，并用 `git diff --stat` 重建本轮未暂存修复面。
- [x] 复核开发者对 B1/B2/M1/M2/M3、minor finding 和 doubts 的逐条回复。
- [x] 审查 G5 cluster upgrade planner/driver、signed trigger、lock marker、broker/agent re-exec、SHA gate。
- [x] 审查 G7 proxy home reported discriminator、remote/socket JS unavailable 状态、docs 同步。
- [x] 添加独立外审回归测试：锁释放失败、已在飞 membership op、socket peer JS unavailable。
- [x] 运行聚焦 Go 测试与 `git diff --check`；NATS 依赖测试在沙箱外重跑确认。

## Findings

### B1 - Blocker - `release-lock` 把 raft 清锁失败报告成 OK，且“重跑自愈”在全员已达标时不会发生

Anchors: `internal/broker/cluster_upgrade_trigger.go:146`, `cmd/tether/cluster_upgrade_drive.go:123`,
`cmd/tether/cluster_upgrade.go:86`, `internal/broker/g5_g7_external_rereview_test.go:18`

`release-lock` 分支忽略 `PlanClearUpgradeActive` 的 `Propose` 错误并无条件返回 `OK:true`。ctl 侧
`releaseUpgradeLock` 只在 `resp == nil || !resp.OK` 时打印警告，因此 leader 变化、raft shutdown、not-leader 或
quorum 异常都可能让 `cluster_upgrade_active` 留在 replicated state，而 operator 看到的是 clean completion。

更严重的是自愈路径并不存在于最常见的 stale-lock 场景。开发者注释说“re-run the roll to completion to clear it”，
但 `cluster upgrade` 在 `plan.Upgrades()==0` 时直接打印 `nothing to do` 并 return，不会进入 `driveUpgrade`，也就不会
发送 `release-lock`。一次实际升级完成但清锁失败后，所有 broker/agent 都已经在 target，再次运行同一 target 只会早退，
membership 永久被锁住，除非新增一条非文档化的手动清理路径。

我新增的 `TestExternalReviewReleaseLockSurfacesProposeFailure` 当前失败：

```text
release-lock must fail when the raft clear cannot be proposed, got &{OK:true ...}
```

Fix direction: `release-lock` 必须像 `acquire-lock` 一样返回 Propose 失败；ctl 必须把 release 未确认作为可见失败或至少
高强度警告。还需要一个可达的 stale-lock recovery：例如显式 `cluster upgrade lock clear`、或在 no-op plan 下检测并清理
已存在 marker，不能依赖不会执行的 roll loop。

### B2 - Blocker - upgrade lock 只挡新 join/retire，不能阻止已在飞 membership op 穿过 rolling restart

Anchors: `internal/broker/cluster_operation_controller.go:309`, `internal/broker/cluster_operation_controller.go:325`,
`internal/broker/cluster_operation_controller_test.go:182`, `internal/broker/g5_g7_external_rereview_test.go:38`

开发者新增的 `upgradeActive` gate 只在 `StartJoinOperation` / `StartRetireOperation` 入口生效。已经存在的
`cluster_operations` 行不受影响：leader observe loop 仍会调用 `driveInFlightOperations`，再进入 `driveJoin` /
`driveRetire` 执行 phase transition、AddNonvoter/AddVoter、drain、RemoveServer 等副作用。

这没有闭合上一轮 B2 的核心风险。`cluster upgrade` 可以在一个 join/retire 已经创建后拿到 marker；随后 membership
controller 继续推进，而 upgrade driver 同时一台台 reload voter。这样仍然可能把“临时 down 一台”叠加到“membership 正在改变”
上，造成 quorum/拓扑不可预测。

我新增的 `TestExternalReviewUpgradeLockFreezesExistingMembershipOps` 当前失败：锁已设置时，一个 `ROSTER_COMMITTED`
join 仍被推进到 `RAFT_ADDING`。

Fix direction: 持锁期间 operation controller 必须冻结或拒绝推进所有非终态 membership op，或者 acquire-lock 必须在同一
raft write 前检查并拒绝已有 `NonTerminalOperations`。同时建议把取锁提前到 planner snapshot 之前，否则 plan 与锁保护的
拓扑不是同一个时间点。

### B3 - Blocker - B1 的 roster 修复仍可用 30 秒 stale manifest 漏掉真实 voter

Anchors: `cmd/tether/cluster_upgrade.go:172`, `internal/broker/cluster_manifest.go:43`,
`internal/broker/cluster_manifest.go:58`, `cmd/tether/ctl_connect.go:154`

`buildUpgradeNodes` 现在从 `fetchManifestOverNATS` 取 manifest，并把 `m.Roster.Brokers` 里的 `VOTER` 当作权威 voter
全集。这比 responder 推断前进了一步，但取到的是 discovery manifest cache：`manifestBytes` 在 `now.Before(nextCheckAt)`
时直接返回旧 bytes，不查 `cluster_nodes` / `roster_generation`，`nextCheckAt` 最短 30 秒。

因此一个刚完成的 grow/retire 后，如果某 broker 的 manifest cache 在操作前已经被预热，`cluster upgrade` 在 30 秒窗口内
仍可能拿到旧 voter roster。最危险的例子是新 broker 已经成为 VOTER 且健康回复了 cluster-health，但 stale manifest
还没有它；当前代码会只遍历 stale manifest 的 voter 列表并返回 nodes，那个新 voter 被静默排除出 upgrade plan。上一轮 B1
的“漏升真实 voter / 错算 quorum”仍然存在，只是触发条件变成“recent membership change + warmed discovery cache”。

还有一个安全疑惑叠加在这里：`fetchManifestOverNATS` 的注释说下游会通过 `AdoptDecision` 验证 manifest，但
`cluster upgrade` 这条路径没有调用 discovery adoption/VerifyAt，只是 JSON unmarshal 后直接用 roster 作为升级安全依据。

Fix direction: upgrade planner 需要 fresh, verified roster，而不是 discovery cache。可以新增一个 signed fresh roster RPC、
为 upgrade path 强制 bypass cache/recheck generation，或至少在 health replies 出现 `IsVoter=true` 但不在 manifest 中时
fail closed/refetch。并且在这条路径上显式验证 roster signature、expiry 和 account pub。

### M1 - Medium - socket `cluster status` 仍没有聚合 peer `JetStreamUnavailable`

Anchors: `internal/broker/clusterstatus.go:336`, `internal/broker/clusterstatus.go:340`,
`internal/broker/g5_g7_external_rereview_test.go:64`

开发者回复称 socket status 接入了和 remote 一样的 runtime JS-503 信号，但实际实现只看本机
`a.jsUnavail()`。在真实部署里 sustained JetStream 503 是 leader-observed；operator 如果 SSH 到 follower 跑 socket
`cluster status`，该 follower 的本机 `jsUnavail` 通常为 false，尽管 `StatusReport` 已经拿到了 health poll map。

我新增的 `TestExternalReviewSocketStatusSurfacesPeerJSUnavailable` 当前失败：peer health reply 里
`JetStreamUnavailable=true`，socket banner 仍只显示普通 N=1 degraded 文案，没有 DATA-PLANE DEGRADED。

Fix direction: socket `StatusReport` 应 OR 聚合 `health` map 中任一 `ClusterHealthResp.JetStreamUnavailable`，并与
force-single clustered-conf banner 去重。

## Resolved / improved since round 1

- M2 previous is fixed: `internal/clusterupgrade` now refuses multiple writable leaders; the external regression passes.
- SHA gate is materially improved: broker reload and agent reexec both require expected SHA.
- `ProxyHomeReported` closes the mixed-version unknown-vs-zero issue for `--homes --remote`.
- `cluster_not_ready` retry closes the nil admin handle startup window more robustly than the first implementation.
- The backup `.bak_orig` file is gone.
- Docs now mention the active-session requirement for colocated agent reexec and document `TETHER_AUTO_REBALANCE` in broker ops.

## Doubts / residual risk

- I did not run simcluster. The current blockers reproduce in hermetic Go/unit paths, so a deploy-tier drill cannot make this
  review pass. After B1/B2/B3 are fixed, an N=3 rolling-upgrade drill should be run for PID-preserving broker reexec, lock
  recovery, and no-write-fence claims.
- The lock marker has no age/owner/target metadata beyond timestamp value. If stale-lock recovery stays marker-based, operators
  need enough status output to distinguish active roll, halted partial roll, and orphaned marker.
- `buildUpgradeNodes` still sets `CaughtUp: true` from health replies and relies on per-host `waitVersion` later. That is safer
  than before but still means the initial transfer target can be chosen before a full stream-readiness check.

## Verification

Passing:

- `go test ./internal/clusterupgrade`
- `go test ./internal/broker -run 'TestB2UpgradeLockBlocksMembership|TestUpgradeTriggerAcquireLockRequiresCluster|TestUpgradeTriggerAcquireLockBadSigRefused|TestUpgradeTriggerLockOpsBypassHandleNilGate|TestG2DataPlaneDegradedBanner'`
- `go test ./cmd/tether -run 'TestRenderUpgradePlan|TestSignUpgradeTriggerVerifiable|TestG7FoldProxyHomeCounts|TestG7CtlExitCodeContract|TestCorrelateHalfUpgradedSkew|TestCorrelateAssumedFallbackForPreG5Broker'`
- `go test ./internal/broker -run 'TestUpgradeTrigger|TestRequestReExec|TestReload|TestG7ClassifyJSUnavailable|TestG7JSUnavailableSustainedDetection|TestG7DefaultProxyStatus'`
- `go test ./internal/agent -run 'TestReExecOnlyRefusesEmptySHA|TestReExecOnlyRefusesShaMismatch'` (rerun outside sandbox because embedded NATS could not bind/start inside sandbox)
- `git diff --check`

Failing external-review regressions:

- `go test ./internal/broker -run 'TestExternalReviewReleaseLockSurfacesProposeFailure|TestExternalReviewUpgradeLockFreezesExistingMembershipOps|TestExternalReviewSocketStatusSurfacesPeerJSUnavailable'`

Sandbox note: broader NATS-dependent package test selections initially panicked with `Unable to start NATS Server in Go Routine`
inside the managed sandbox; the relevant agent NATS tests passed when rerun outside the sandbox.

---

## 主进程回复（round2 · 逐条采纳/修改）

4 条 finding（B1/B2/B3 blocker + M1 medium）**全部采纳并修复**，你新增的三条回归测试现全 PASS。三硬闸复跑全绿：`make lint` = 0 issues、`make test` 全包 ok、`make e2e` 串行矩阵 PASS（507s）、`-race` 复核 broker 触碰面干净。逐条如下。

### B1 — ✅ 采纳并修复：release-lock 如实报失败 + 可达的 stale-lock 自愈

两处：
1. **release-lock 不再假 OK**（`internal/broker/cluster_upgrade_trigger.go`）：像 acquire-lock 一样返回 `Propose` 失败（`not_leader` + err），不再无条件 `OK:true`。你的 `TestExternalReviewReleaseLockSurfacesProposeFailure` 现 **PASS**（对 shut-down node 返回非 OK）。ctl 侧 `releaseUpgradeLock`（`cmd/tether/cluster_upgrade_drive.go`）把未确认升级为**响亮 WARNING**，明示"`cluster_upgrade_active` 仍置、join/retire 仍被挡、重跑同 --to-version 清除"。
2. **可达自愈**（`cmd/tether/cluster_upgrade.go` no-op 分支）：你指出的死角——全达标后 `plan.Upgrades()==0` 早退、release 永不触发——已闭合。新增 `UpgradeLockActive` 到 `ClusterHealthResp`（responder 读 `upgradeActive(db)` 自报），no-op 分支用 `upgradeLockHeld(nc, actor)` 探测；若持锁则经 leader 发 `release-lock` **主动清除**并打印 "cleared a stale upgrade lock"，清不掉则报**可见错误**（非静默）。因此"完成但清锁失败 → 再跑一次同 target"即自愈，无需未文档化手动路径。（注：为让 no-op 也能签 release-lock，account seed 读取已提前到 planner 之前。）

### B2 — ✅ 采纳并修复：真互斥（冻结在飞 op + 开 roll 前拒在飞 op）

不再只是入口门：
1. **冻结在飞 op**（`internal/broker/cluster_operation_controller.go` `driveInFlightOperations`）：持锁期间**直接 return，不驱动任何非终态 membership op**（不推进 phase / AddVoter / drain / RemoveServer）。op 状态留在 raft，锁释放后自动续跑。你的 `TestExternalReviewUpgradeLockFreezesExistingMembershipOps` 现 **PASS**（ROSTER_COMMITTED 的 join 持锁时不进 RAFT_ADDING）。
2. **开 roll 前拒在飞 op**（acquire-lock handler）：`Propose(PlanSetUpgradeActive)` 前查 `NonTerminalOperations`,有则拒（`a cluster membership operation ... is in flight`）——roll 不会在 join/retire 中途启动。新增 `TestUpgradeTriggerAcquireLockRefusesInflightOp` 钉死（拒 + 不置 marker）。

二者互补覆盖你指出的 check→commit 竞态窗口：acquire 前拒 + 持锁冻结，任一路径漏掉的 op 都被另一条挡住。

### B3 — ✅ 采纳并修复：双向一致性 + 验签 roster

`buildUpgradeNodes`（`cmd/tether/cluster_upgrade.go`）：
1. **反向一致性**（核心，无需密钥、确定性）：除了原有"配置态 voter 必须都回答 health（缺席则拒）",新增反向——**任何自报 `IsVoter=true` 的应答者必须在签名 roster 的 voter 集内**;否则说明 manifest 快照过期（正是你描述的"recent grow + warmed cache 下新 voter 答 health 但不在 stale roster"），**fail-closed** 提示等 ~30s 重跑。stale-cache 漏升真实 voter 的窗口就此关闭。
2. **验签**（带 `--account-seed` 执行时）：对 manifest roster 跑 `clusterroster.VerifyAt(m.Roster, accountPub, now)`——校验**账户签名 + 有效期 + schema**;未验证/过期/错账户即拒。补上了你指出的"直接 JSON unmarshal 就当权威、未 VerifyAt"缺口。account pub 由 operator 的 `--account-seed` 派生。

关于"fresh roster vs 30s cache":我采用你列出的可接受路径——**health replies 出现 `IsVoter=true` 但不在 manifest 中即 fail-closed/refetch**——它把 stale-cache 危险场景变成确定性拒绝 + 重试,而非新增一条 signed-fresh-roster RPC（后者更重、v1 不必要）。残留 doubt（`CaughtUp:true` 由 health 近似、真正 barrier 是 per-host `waitVersion`）保持现状:它是 NATS 可达子集,stream-readiness 仍是 additive-report 后续。

### M1 — ✅ 采纳并修复：socket status 聚合 peer JetStreamUnavailable

`StatusReport`（`internal/broker/clusterstatus.go`）不再只看本机 `a.jsUnavail()`:新增 **OR 聚合 `health` poll map 中任一 `JetStreamUnavailable`**（sustained-503 是 leader-observed,SSH 到 follower 时本机 flag 为 false 但 poll map 里有 leader 的自报）。与 force-single clustered-conf banner **去重**为单条。你的 `TestExternalReviewSocketStatusSurfacesPeerJSUnavailable` 现 **PASS**（peer 报 JS unavail → banner 含 `DATA-PLANE DEGRADED`）。

### 残留 doubt 回应

- **lock 元数据（age/owner/target）**:marker 存了 timestamp value;stale-lock 恢复以"重跑自愈"为主路径,故未加完整元数据（v1 务实,见 `feedback_security_pragmatic`）。若后续要支持"区分 active/halted/orphaned",再扩 marker + `cluster status` 展示。
- **simcluster**:仍未跑。round2 blocker 都在 hermetic 路径复现/修复;deploy-tier N=3 rolling-upgrade drill（验 PID-preserving re-exec + lock recovery + no-write-fence）是独立门、不改本轮结论。weilandserver 现可经本机 `tether ctl` 够到,待你确认后我跑相关 drill（不整套 loop）。
