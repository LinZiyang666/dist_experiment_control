# cluster phase-transition fluidity — internal review ROUND 2 (v0.4.2)

> Second Stage C adversarial review (4 Opus lenses + synth), run AFTER the round-1 fixes + the shrink
> build. Verdict: **CONDITIONAL — no FAIL-class defect, NO round-1 code regression**, 0 blockers, 4
> majors. All 4 majors + the actionable minors are now fixed (this file records the dispositions).
> Main-process dispositions inline (✅ fixed / ✎ fixed-with-change / ⏭ deferred-as-follow-up).

## Round-1 fixes the round-2 reviewers VERIFIED as landed-correct

B1 set-raft-addr/set-route SELF-ONLY (enforced at the ADMIN layer, not just CLI — a hand-crafted
adminsock request with NodeID=peer is refused) + single-RaftConfiguration-read collision-reject +
write-free AddVoter-skip; B2 AddNonvoter no-wedge PRIMITIVE (TestAddNonvoterUnreachableNoWedge, real
raft leader, -race); m2 SelfConfiguredAddr removed; m4 PlanClusterNodeRoute bumps BOTH
topology+roster gen (change-gated) + url validation (m9); m5/m6 node_readdr tests + appliers
symmetry; m8 doctor 'nats_route' label; M1 shrink renders a real standalone conf (real nats-server
parser). **The reviewers' "usage.md / architecture.md not updated" items were STALE** — the synth
verified via git diff that both ARE updated.

## Round-2 MAJORS — all fixed

- **MAJOR-1 (all 4 reviewers, top consensus) — the keystone DELIVERY through driveJoin was untested.**
  The no-wedge PROPERTY was proven (AddNonvoter unit) but the live grow path (driveJoin
  RaftAdding→AddNonvoter→promote) had ZERO coverage; a revert of `:455 AddNonvoter→AddVoter` would
  re-wedge N=1 with a green suite, and test/d7 exercises the OLD AddNode path. ✅ **FIXED**:
  `internal/broker/cluster_rebind_test.go` TestDriveJoinStagesNonvoterNoWedge — drives a real join op
  through RAFT_ADDING and asserts the joiner enters the raft config as a **NONVOTER** (not a voter)
  + a concurrent write still commits (no wedge). A revert to direct-AddVoter fails both assertions.
  Runs -race.
- **MAJOR-2 — `--confirm-single` was a pure operator assertion (no machine check)**, so
  de-clustering at N≥2 would split this node from the mesh. ✎ **FIXED**: runReconcileToStandalone now
  `fetchClusterStatusReport(f.takeoverSocket)` and REFUSES when the live voter count > 1; --confirm-single
  is kept as the typed-intent confirm ON TOP. Fails-open with a loud warning only when the socket is
  unreachable (then --confirm-single is all that remains).
- **MAJOR-3 — runbook §1.0 still documented the removed `--node` peer-rebind** (contradicting B1 +
  §2.3). ✅ **FIXED**: §1.0 now says SELF-ONLY + the transfer-leader path; the stale `--node` sentence
  is gone.
- **MAJOR-4 — OpClusterAdd (direct-AddVoter wedge path) was still allowlisted + live-dispatched.**
  ✅ **FIXED**: removed OpClusterAdd from `internal/adminsock/protocol.go` clusterOps — a raw socket
  request is now rejected as an unrouted op; all grows go through OpClusterJoinApprove→driveJoin. The
  dead dispatch case is left referenced (lint-clean); the TestD7ClusterModeNotEnabled representative
  list was updated to drop the now-unrouted cluster_add and add cluster_set_raft_addr.

## Round-2 MINORS

- ✅ **BuildMergedConf mTLS-harvest** now gated on `!cfg.Standalone` (a standalone render never needs
  the routes mTLS, and harvesting it from a malformed clustered conf failed confusingly).
- ✅ **Portless route** — validateNatsRoute + PlanClusterNodeRoute now require `u.Port() != ""` (symmetry
  with raft_addr's SplitHostPort); tests cover `nats://h`.
- ⏭ **collision-branch test** (the dup-address reject at clusterdrain.go:371 has no executable
  coverage — the single-node harness has no second server) — follow-up; logic is correct by inspection.
- ⏭ **runReconcileToStandalone command unit** (the guards + auth-identity harvest) — follow-up; the
  lower layers (Standalone render, IsClusteredJetStream) are tested; this is cmd-orchestration glue.
- ⏭ **doctor advisory leader-scope** (the loopback-advertise advisory reflects the queried node, not
  necessarily the leader) — follow-up; advisory-only, consistent with the round-1 m8 disposition.
- ⏭ **protected-mode §2.3 wording** (the row says "no commit"; the exact mechanism is a leader-gate
  "no leader; retry" bounce) — follow-up doc nuance; the SEMANTIC (refused in quorum-loss) is correct.
- ⏭ **write-free no-op comment / gen-bump order** (NITs) — harmless, match existing patterns.

## Deferral verdicts (round-2 reviewers)

- **m1 OpClusterAdd** — now CLOSED (MAJOR-4). **m3 init loopback guard + offline-doctor split** —
  OK_TO_DEFER (online set-raft-addr covers grow-prep; an init-time loopback advertise is recoverable
  online, no force-single). **protected-mode clean message** — OK_TO_DEFER (ops fail SAFELY; only the
  message is suboptimal). **Mechanism E (transport advertise decouple), N=1 status banner, join
  approve --check** — OK_TO_DEFER (cosmetic / nice-to-have per the plan). All tracked as open plan items.

## State

All 4 round-2 majors + the actionable minors fixed; `go build ./...` + `go test` on every touched
package (cluster, broker, natsconf, natscluster, adminsock, cmd/tether) GREEN; the concurrency-touching
keystone tests pass under -race. No round-1 fix regressed. **Ready for the user's external review**
(the deferred follow-up items are open plan items, not v0.4.2 blockers; golangci-lint must run in an
environment where it is installed before release).
