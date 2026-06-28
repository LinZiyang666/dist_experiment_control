# v0.4.4 grow-onto-migrated-broker fix — Stage-C adversarial review + main-process resolution

Stage-C: 6 Opus reviewers (one per fix) → adversarial verify → ship/block synth (workflow
`v044-grow-fix-review`, 17 agents / ~1.5M tok). **Verdict: BLOCK** — 7 confirmed blocker/major. All 7
adopted + fixed by the main process below. Then the new e2e test (must-fix #5) surfaced a DEEPER,
review-missed blocker (§B) that gates the live grow.

## A. The 7 confirmed must-fixes — all RESOLVED

| # | sev | finding | resolution |
|---|-----|---------|------------|
| 1 | blocker | **A `join prepare` best-effort WARN emits a poisoned bundle** (empty cert_fp → wireClusterEarly crash-loop before nats.Connect; empty bus_nkey → learner backfill deadlock) | `cmd/tether/cluster_join.go`: **fail-closed** — return an error if bus_nkey (from broker.nk) or cert_fp can't be derived; `--cert-fp` stays the explicit override. Stale `cert_fp OPTIONAL / D6 backfills` comments corrected (join_bundle.go, cluster_join.go). |
| 2 | major | **A empty re-admit clobbers a good bus_nkey to ''** via unconditional `bus_nkey_pub=excluded…` in ON CONFLICT DO UPDATE → re-arms the deadlock | `internal/cluster/membership_ops.go`: empty-PRESERVING `CASE WHEN excluded.x!='' THEN excluded.x ELSE cluster_nodes.x END` for both bus_nkey_pub + cert_fp. Test `TestD7UpsertEmptyReadmitPreservesIdentity`. |
| 3 | major | **G `reaperMayDelete` compared command-domain AppliedIndex vs raft CommitIndex** → permanently false in cluster mode (election LogNoop bumps CommitIndex, FSM-ignored) → the leader boot orphan reaper was INERT (the exact no-op class this epic exists to catch) | `internal/broker/clusterwrite.go` + `internal/cluster/read.go`: gate on `RaftAppliedIndex() >= CommitIndex()` (raft domain — advances on the noop). Proven by `TestRaftAppliedIndexCatchesUpToCommitOnLeader` (raft predicate converges, command-domain does NOT) + `TestReaperMayDeleteGate`. |
| 4 | major | **STEP-1 resnapshot audit-window guard over-fires on EVERY real migrated broker** (raw `LastIndex > audit_published_index`: config/noop/self-begetting checkpoint always sit above the cursor) → forced `--accept-audit-loss` for a phantom loss; the documented restart-drain-stop remedy provably never cleared it | `internal/cluster/offline.go` `UnpublishedAuditOpsInLog` (offline scan of (pub, LastIndex] counting ONLY OpReconcileBatch/OpTransferAudit, poison fail-closed); `internal/clusteroffline/offline.go` guard rewired. Clean broker now resnapshots without the flag; remedy genuinely clears it. Tests: white-box `TestUnpublishedAuditOpsInLog` + d9 `resnapshot_test` rewritten (clean broker succeeds). |
| 5 | major | **STEP-0 proof gap**: keystone test asserted snapshot EXISTS but not log COMPACTION, and NO end-to-end migrated-leader→fresh-joiner test (the exact hole that let v0.4.3 ship a no-op) | (a) compaction assertion (`RaftLastIndex==0`) added to `grow_migrated_snapshot_test` + `resnapshot_test`. (b) **new e2e `TestD9GrowFromMigratedLeader`** (seed FK-bearing rows pre-init → AddNode → assert row parity) — which then **caught §B**. |
| 6 | major | **D Render zero-routes fail-closed had no direct unit test** (only transitive via /bin/true) | `internal/natscluster/config_test.go` `TestRenderClusteredZeroRoutesFailsClosed` (asserts the 'ZERO routes' error directly). |
| 7 | major | **G had zero cluster-mode coverage** | `TestRaftAppliedIndexCatchesUpToCommitOnLeader` (cluster layer) + `TestReaperMayDeleteGate` (broker branches). |

`make test` + `d7_integration` + `d9_integration` all green.

## B. NEW BLOCKER the e2e test exposed (review-missed, gates the live grow)

**A bootstrapped joiner becomes a SILENT HOLLOW VOTER.** The broker enters cluster mode only with a
seeded DB (`assertClusterDBConsistent` requires it), and the only way to seed is `cluster init`, which
BOOTSTRAPS the joiner's own `{self}` raft (`node.New`, `!existing` branch, no JoinMode). When that node is
then `AddVoter`'d, the two independently-bootstrapped logs share the low-index `config@1 + noop@2` prefix
(same term), so the leader replicates via **LOG replay** and never ships `InstallSnapshot` — so the
leader's snapshot-only MIGRATED rows (direct-seeded by `cluster init --from-existing`, present in NO log
entry) never reach the joiner. The joiner settles as a VOTER with its OWN empty DB.

Diagnostic proof (TestD9GrowFromMigratedLeader, now `t.Skip`'d as KNOWN-OPEN): after join, leader A has
`cluster_nodes=2 / sessions=1`, joiner B has `cluster_nodes=1(self) / sessions=0 / nodes=0` at the SAME
appliedIndex=12. `testTwoBrokerJoinReplicates` stayed green throughout because it only checks the joiner
participates in NEW writes, never that it gained the leader's PRE-join data — the mask that let this ship.

STEP-0 (leader init snapshot+compaction) is **necessary but not sufficient**. The complete fix needs:
1. **JoinMode**: a joiner starts with EMPTY raft (no bootstrap) so its nextIndex decays below the leader's
   FirstIndex → `InstallSnapshot` → it loads the leader's full snapshot. Requires a `cluster join init`
   (seed self_node_id + a join-pending marker, NO `BootstrapSingleNode`) + broker detection (cluster mode
   for a join-pending DB) + `node.New` JoinMode (skip bootstrap when `!existing && JoinMode`).
2. **Restore identity preservation**: `fsm.Restore` installs a COPY of the leader's DB, whose
   `cluster_meta.self_node_id` is the LEADER's — the joiner must PRESERVE its own self_node_id (and any
   node-local rows) across the install, or it adopts the leader's identity.
3. The operational procedure (runbook) must direct a joiner to `cluster join init`, NOT `cluster init`
   (the live racknerd ran `cluster init --from-existing` = operator error compounding the design hole).

This is an architecture-level change deserving its own plan + (ideally) its own external review. It is
the TRUE remaining blocker for the live N=1→N=2 grow; the §A fixes are correct + banked but do not by
themselves make the grow produce a faithful voter.
