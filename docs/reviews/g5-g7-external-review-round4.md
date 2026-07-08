# Pass - G5/G7 external review round 4

结论：Pass。round3 的 blocker 已闭合：`buildUpgradeNodes` 的 signed-roster 反向一致性检查现在不再依赖
`IsVoter`，而是要求任何 cluster-health responder 的 `NodeID` 都必须存在于 signed roster 的任一 phase 中。这样
pre-G5 broker 即使不带 additive `is_voter` 字段，也不会再被 stale roster 静默漏掉。上一轮外审失败回归和本轮新增的
“roster-known pre-G5 learner 不误拒”正向测试均通过。

我仍未信任主进程自述；本轮只按 diff、ACL 和测试结果判断。

## Tasklist / review surface

- [x] 复查 round4 未暂存 delta：`cmd/tether/cluster_upgrade.go`、`cluster_upgrade_drive.go`、新增
  `g5_roster_consistency_test.go`、round3 报告回复。
- [x] 复核 round3 B1：pre-G5 responder absent from roster 必须 fail-closed。
- [x] 复核 no-op stale-lock 提示：无 `--account-seed` 时不再只打印 `nothing to do`。
- [x] 复核 cluster-health responder spoofing 的 NATS ACL 假设。
- [x] 运行上一轮失败回归、本轮新增正向测试、G5/G7 聚焦回归和 `git diff --check`。
- [x] 通过本机 `tether exec` 到 `weilandserver` 复查 simcluster 新调用路径，并运行隔离
  `00-skeleton` deploy-tier smoke。

## Findings

No blocking findings.

### Round3 B1 - resolved

`cmd/tether/cluster_upgrade.go` 现在构建 `rosterSet` 覆盖 signed roster 中所有 broker phase，而不是只构建 voter
set；随后对 `byNode` 中每个非空 responder `NodeID` 做 presence-in-roster 检查。这个不变量是版本无关的，能覆盖
pre-G5 broker 没有 `IsVoter` 字段的混版窗口。

我新增的上一轮失败测试现在通过：

- `TestExternalReviewBuildUpgradeNodesRejectsResponderAbsentFromRosterEvenPreG5`

开发者新增的正向测试也覆盖了不过度拒绝：

- `TestBuildUpgradeNodesAllowsRosterKnownPreG5Learner`

ACL 侧我检查了 `internal/auth/permissions.go`：普通 activated member 只有 publish `cluster-health.req` 和 subscribe
`_INBOX` 权限，没有 subscribe `ctrl.by.*.cluster-health.req` 的权限；因此不能直接伪造成 broker responder 回包。真正能
回答该 broadcast 的仍是 broker 连接。

## Doubts / residual risk

- Simcluster 新路径已验证：本机 `tether exec` 可调度 `weilandserver`，并且隔离 `00-skeleton` drill 通过
  13 个断言（N=1 cutover、agent join、tier-A/B push/pull）。这只覆盖基础 deploy-tier smoke；最终发布前仍建议跑
  N=3 rolling upgrade drill，覆盖 PID-preserving broker reexec、stale-lock recovery、mixed-version roster 和 no-write-fence。
- `CLAUDE.md`、`test/simcluster/README.md` 和本地设备运维文档仍主要描述旧的 `remote.sh`/SSH 路径。若以后
  simcluster 只能从本机 `tether` CLI 通过 `weilandserver` 调用，建议单独更新这些运行手册，避免审查者按旧入口误跑。
- No-op stale-lock without `--account-seed` 现在会给出清晰 warning 但仍返回 success。考虑到没有 seed 无法签 release-lock，
  这不是本轮阻塞；若运维脚本依赖 exit code，后续可考虑让 “lock held but no seed” 返回非零。
- `acquire-lock` 的 in-flight op 预检仍不是与 marker write 同一条 raft command。当前 controller freeze 已覆盖危险副作用，
  但如果未来要求“roll 启动瞬间绝对没有非终态 op”，需要 conditional write 或 operation-level reservation。

## Verification

Passing:

- `git diff --check`
- `go test ./cmd/tether -run 'TestExternalReviewBuildUpgradeNodesRejectsResponderAbsentFromRosterEvenPreG5|TestBuildUpgradeNodesAllowsRosterKnownPreG5Learner'`
- `go test ./internal/broker -run 'TestExternalReviewReleaseLockSurfacesProposeFailure|TestExternalReviewUpgradeLockFreezesExistingMembershipOps|TestExternalReviewSocketStatusSurfacesPeerJSUnavailable|TestUpgradeTriggerAcquireLockRefusesInflightOp|TestUpgradeTriggerLockOpsBypassHandleNilGate'`
- `go test ./internal/clusterupgrade`
- `go test ./cmd/tether -run 'Test(RenderUpgradePlan|SignUpgradeTriggerVerifiable|G7FoldProxyHomeCounts|G7CtlExitCodeContract|CorrelateHalfUpgradedSkew|CorrelateAssumedFallbackForPreG5Broker|G3FetchManifestOverNATS|RefreshCtlEndpointsGateAHoldsOverLiveNATS)'`
- `go test ./internal/broker -run 'TestUpgradeTrigger|TestB2UpgradeLockBlocksMembership|TestG2DataPlaneDegradedBanner|TestG7ClassifyJSUnavailable|TestG7JSUnavailableSustainedDetection|TestG7DefaultProxyStatus'`
- `go test ./internal/agent -run 'TestReExecOnlyRefusesEmptySHA|TestReExecOnlyRefusesShaMismatch'`
- `./tether exec --timeout 25m --cwd /home/weiland/dist_experiment_control/test/simcluster weilandserver ./simcluster drill 00-skeleton`

NATS-dependent cmd/agent tests were run outside the managed sandbox because embedded nats-server cannot reliably bind/start
inside the sandbox.
