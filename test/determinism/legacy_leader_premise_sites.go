package determinism

// legacy_leader_premise_sites.go — the DRAINING LEDGER for TestLeadershipIsNeverAssumedFromAStaleRead.
//
// Every entry is a test function that, on 2026-09-01, read leadership bare — outside a polling
// predicate (a func literal handed to WaitForCond / waitForCond / WithLeader …) and outside a
// for-statement's own condition. Most of them are hand-written observe → act → re-observe loops that
// are correct today (d7's adminForLeader rescans on every not-leader error; the cluster helpers poll
// in a for body and return the index); the ledger does not judge, it freezes. Drain an entry by
// acting through clusterharness.WithLeader, which does the re-observe for you, and deleting the line.
//
// ON THE NUMBER 13. The first landing of this gate (same day) counted 5, because its scanner treated
// ANY for / range / select body and ANY func literal as a "re-evaluated shape" — which is exactly the
// d3 flake's shape (a read inside an attempt loop, acted on once per iteration; internal review L6-F9
// / L1-F7). The stricter scanner found 8 more. The one site plan B5 named as WithLeader's first
// adopter — test/d3's follower PIN write — was converted rather than ledgered, so it is not here.
//
// KEYED "<path>: <func>". Only ever REMOVED (legacyLeaderPremiseSitesCap); a stale entry FAILS.

const legacyLeaderPremiseSitesCap = 13

var legacyLeaderPremiseSites = map[string]bool{
	"internal/broker/reconcile_passes_test.go: legacyReconcileTickOracle":                                    true,
	"internal/broker/rotate_cert_test.go: rotate2NodeFollower":                                               true,
	"internal/cluster/prevote_test.go: waitForLeader":                                                        true,
	"internal/cluster/recover_online_test.go: TestRecoverToSelfOnlineConcurrentPropose":                      true,
	"internal/cluster/recover_online_test.go: TestRecoverToSelfOnlineFailureLeavesReadOnlyThenRestartLeader": true,
	"internal/cluster/recover_online_test.go: TestRecoverToSelfOnlineNilFactoryRefuses":                      true,
	"internal/cluster/transport_test.go: waitNodeLeader":                                                     true,
	"internal/cluster/transport_test.go: waitRealLeader":                                                     true,
	"test/d5/phasefluidity_rebind_test.go: TestPhaseFluidityMTLSRebind":                                      true,
	"test/d5/publisher_test.go: TestD5FollowerNeverPublishes":                                                true,
	"test/d7/integration_test.go: (*d7Cluster).adminForLeader":                                               true,
	"test/d7/integration_test.go: (*d7Cluster).applyMetaSetOnLeaderAndWaitApplied":                           true,
	"test/d7/integration_test.go: (*d7Cluster).nudgeLeadershipHome":                                          true,
}
