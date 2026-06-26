//go:build c7_integration

// Package c7 holds the C7 staging quorum-loss→recover drill. C7 Stage-C disposition: the CORE of the
// "演练工具" requirement — quorum-loss → force-single → recover → restart-writable, with the
// force_single_active marker + RecoverCluster no-double-apply asserted — is ALREADY covered by the
// gated test/d7 TestD7Matrix "ForceSingleRecoverRestart" drill + internal/clusteroffline/offline_test.go
// (live-peer TCP HARD-REFUSE, empty-state refuse, flock, dump-before-wipe). No unproven SAFETY
// invariant is deferred.
//
// What remains (this gated drill, tracked NOT silently closed): the C7-specific TEMPORAL MONOTONE
// invariant — `cluster status` exit code continuously != 0 from the kill, through force-single (exit 3),
// through the N=2-stable waypoint, becoming 0 (HEALTHY_HA) ONLY at the first N>=3 + streams-AllAtTarget
// sample — asserted over the PRODUCTION computeHealth/AlertReconciler/force-single-clear wiring, PLUS
// the explicit negative check that N=2-stable is still non-green even though force_single_active has
// cleared and no severe alert is present (the manual:credrot clears-at-N=2 trap, c7-plan D9). The
// cheaper non-flaky build is the inmem-raft d7 harness stitch (startD7Cluster(3)→kill→ForceSingle→
// regrow N1→N2→N3, sampling StatusReport().ExitCode at each waypoint with stubbed StreamActual/Target),
// NOT a real clustered-JS harness (CLAUDE.md documents clustered-JS starvation flake).
package c7

import "testing"

func TestC7DrillNotGreenUntilN3AndStreamsAtTarget(t *testing.T) {
	t.Skip("C7 staging-drill temporal-monotone stitch — tracked follow-up (build on the inmem-raft d7 " +
		"harness; the offline force-single/recover safety primitives are already proven by test/d7 + " +
		"internal/clusteroffline). See docs/reviews/c7-review.md 'Staging-drill disposition'.")
}
