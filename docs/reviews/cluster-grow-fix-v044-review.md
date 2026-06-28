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

## B. What the new e2e test exposed — a TEST ARTIFACT, not a real-grow blocker (corrected)

The new `TestD9GrowFromMigratedLeader` fails with a hollow-voter, but **re-analysis + empirical
measurement show this is a LOW-FirstIndex TEST-FIXTURE artifact, NOT a blocker for the real grow.**

Mechanism of the artifact: the test's leader A is FRESHLY `cluster init`'d, so its snapshot sits at raft
index 1 and its FirstIndex is ~2 — the SAME low index a freshly-bootstrapped joiner B reaches (config@1 +
noop@2, same term). raft aligns the two logs at index 2 and replicates the tail via LOG replay, so B never
`InstallSnapshot`s A's snapshot@1 and misses the snapshot-only seeded rows. Measured: `A snapshot idx=1 ==
B snapshot idx=1` before the join; after, A has `cluster_nodes=2/sessions=1` while B has
`cluster_nodes=1(self)/sessions=0` — a hollow voter. `testTwoBrokerJoinReplicates` masked it (only checks
the joiner participates in NEW writes).

Why the REAL grow does NOT hit this: the live pc732 is not fresh. After `cluster recovery resnapshot`
(RecoverCluster, unconditional DeleteRange) its snapshot sits at its ACCUMULATED (high) raft index with the
log compacted away, so its FirstIndex >> a fresh racknerd's bootstrap index (~2). raft cannot offer any
AppendEntries prevLogIndex the joiner can match (all are compacted) → it is FORCED to `InstallSnapshot` →
racknerd loads pc732's full DB → a faithful voter. STEP-0 (leader init snapshot+compaction) + STEP-1
(resnapshot the migrated leader) are therefore SUFFICIENT for the live resnapshot-first procedure.

`TestD9GrowFromMigratedLeader` is `t.Skip`'d (models an unrealistic fresh-leader). Two follow-ups, both
NON-blocking:
1. **Faithful test**: resnapshot A at a high index after advancing it, then grow — would PASS; un-skip then.
2. **JoinMode (defense-in-depth)**: a joiner that starts with EMPTY raft (no bootstrap) installs the
   leader's snapshot regardless of the leader's FirstIndex, removing the dependence on the leader being
   resnapshotted-high. Would also need `fsm.Restore` to PRESERVE the local `cluster_meta.self_node_id`
   (the installed snapshot carries the leader's id). Nice robustness; not required for the documented
   resnapshot-first grow.

**Bottom line:** the §A fixes + STEP-0/STEP-1 make the live N=1→N=2 grow correct (resnapshot pc732 first,
then join a fresh racknerd). The real grow is the decisive proof and is safe/reversible (a hollow or
failed joiner is detected by a post-join row-parity check and removed via `cluster recovery node remove`,
exactly as in the prior attempt; pc732 stays healthy throughout).
