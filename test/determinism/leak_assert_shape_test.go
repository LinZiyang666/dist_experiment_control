package determinism

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// leak_assert_shape_test.go — every goroutine/fd leak assertion must exercise its subject N>=5 times (B9).
//
// THE ARITHMETIC THIS ENFORCES
// ----------------------------
// internal/testharness.AssertNoGoroutineLeak passes when the delta is <= GoroutineLeakTolerance (2), and
// that floor is correct: it is derived from the runtime's own shifting helper goroutines, and tightening
// it trades a real signal for false positives. The consequence is arithmetic rather than stylistic — a
// subject exercised ONCE can leak up to two goroutines per exercise and stay green forever, which is
// exactly what TestTunnelServerCloseWithActiveSessionNoLeak did: it opened one tunnel session while the
// leak it was named after (the yamux watcher, the per-conn bridge) is per-session shaped.
//
// WHY A GATE AND NOT A ONE-OFF FIX
// --------------------------------
// The roadmap's obligation (S1-refactor-roadmap.md:517) is "every leak assertion exercises the subject
// N>=5 times", and the first attempt at it fixed ONE call site while the shared helper's doc asserted the
// whole repo complied. The internal review enumerated twelve call sites and found six still at one
// exercise. A gate is the difference between a claim and a property; without it, site thirteen arrives
// with the same defect and the same confident comment.
//
// WHAT IT LOOKS FOR, AND WHY THAT SHAPE
// -------------------------------------
// The first design of this check matched a single expression — a comparison containing
// runtime.NumGoroutine() against `ident + INT`. That form appears in roughly six places repo-wide and in
// NONE of the four copies B9 deleted, which all wrote `last = runtime.NumGoroutine()` on one line and
// `if last-before <= 2` on another. A gate that cannot see the shape it was written to outlaw is worse
// than no gate, because its passing is read as evidence.
//
// So this scans for the CALL, not the comparison: any call to a leak-assertion helper (by name), inside a
// test function, must be preceded by a QUALIFYING loop — or be listed below with a reason.
//
// WHAT "QUALIFYING" MEANS, AND WHY THE FIRST VERSION WAS TOO LOOSE
// ----------------------------------------------------------------
// The first version accepted "the body contains any for/range at all". The mutation-honesty audit
// pointed out that this admits the most common false shape in the repo:
//
//	for _, tc := range cases {          // <- a loop, so the old gate was satisfied
//	    t.Run(tc.name, func(t *testing.T) {
//	        before := runtime.NumGoroutine()
//	        doTheThingOnce()             // <- exercised ONCE per subtest
//	        assertNoGoroutineLeak(t, tc.name, before)
//	    })
//	}
//
// Every subtest here takes its own baseline, exercises once, and asserts once. The loop iterates
// SUB-CASES, not exercises, and the ±2 tolerance swallows a per-exercise leak in each of them. So the
// predicate now has three load-bearing conjuncts:
//
//	(1) the loop must NOT contain the leak-assertion call. A loop that contains it is a loop of
//	    baseline-exercise-assert triples, which is precisely the shape above.
//	(2) the loop must run at least leakExerciseRoundsFloor (5) times, and that count must be STATICALLY
//	    RESOLVABLE from the same function or file. See loopRounds for the four shapes the repo actually
//	    uses; anything it cannot resolve is reported, because "it happens to iterate enough times today"
//	    is not a property a reader can check.
//	(3) the loop must contain that test's exact site-scoped exercise anchor. Five empty iterations, or
//	    five calls to unrelated setup followed by one real subject call, prove no leak sample size.
//
// None is substitutable: (1) rejects subcase loops, (2) proves sample size, and (3) binds that size to
// the subject rather than to arbitrary syntax.
//
// A NOTE ON CALIBRATING (2), because getting it wrong is expensive in both directions
// -----------------------------------------------------------------------------------
// The first attempt at (2) accepted only `for i := 0; i < <int literal>; i++`. Run against the tree it
// reported four offenders, and all four were CORRECT code the predicate could not read: `for range K`
// with `const K = 20`, `for round := 0; round < leakExercises; round++` with a function-local const, and
// a range over a slice built as `make([]int, leakExerciseRounds)`. A gate that forces correct code to be
// rewritten to suit its parser trains people to edit the gate, which is how a gate dies. So loopRounds
// resolves constants and make-lengths rather than demanding one blessed spelling — and it still returns
// 0, i.e. reports, for anything genuinely opaque.

// leakExerciseRoundsFloor is the N the roadmap requires. It is duplicated from
// test/concurrency's leakExerciseRounds rather than imported because this package cannot import a
// _test.go identifier from another package — and TestLeakRoundsConstantMatchesTheFloor below fails if
// the two ever drift, so the duplication cannot rot silently.
const leakExerciseRoundsFloor = 5

// leakAssertHelpers are the assertion entry points. Local shims delegating to internal/testharness keep
// their own names, so both spellings are listed.
//
// NOT listed, deliberately:
//
//	waitGoroutineBaseline    it is not an assertion. It is a CEILING used to wait for the process to
//	                         quiesce BEFORE the real baseline is taken (internal/cluster/transport_test.go
//	                         :167 and :486 pass NumGoroutine()+8). Requiring a repeated exercise around it
//	                         would demand a loop that asserts nothing. It used to be listed here AND
//	                         exempted below, which is two mechanisms disagreeing about one fact.
//	assertNoGoroutineGrowth  no such identifier exists anywhere in this repo. It was listed as though it
//	                         did, which makes the helper set look more complete than it is — the exact
//	                         failure mode this gate exists to prevent, one level up.
var leakAssertHelpers = map[string]bool{
	"assertNoGoroutineLeak": true,
	"assertNoFDLeak":        true,
	"AssertNoGoroutineLeak": true,
	"AssertNoFDLeak":        true,
	"pollGoroutinesAtMost":  true,
}

// singleExerciseExemptions are leak assertions that legitimately run their subject once, WITH the reason.
// An entry is a claim that a per-exercise leak in that subject is structurally impossible or is covered
// elsewhere — not a note that repeating it would be inconvenient.
//
// The key is "<repo-relative file>:<the test function's name>", and
// TestSingleExerciseExemptionsAreLiveAndSiteScoped below fails on any entry that does not name a real
// test function that really calls a leak helper. (It was empty for a while; the previous single entry
// named a helper rather than a test function, so it could never match anything — see leakAssertHelpers
// above for where that fact now lives instead.)
var singleExerciseExemptions = map[string]string{
	// origin: prerelease audit round 2, CC-2. This one is not a leak assertion at all: it
	// is a test OF AssertNoFDLeak, driving it through a synthetic *testing.T to prove it
	// FAILS on an FDCountFailed baseline instead of no-opping. There is no subject to
	// exercise repeatedly — the subject IS the assertion — and looping it five times
	// would assert the same thing five times.
	//
	// The gate exists because the shared fd/goroutine gate had no test of its own, which
	// is how the §3 sweep changed its most important branch unguarded.
	"internal/testharness/leakgate_test.go:TestAssertNoFDLeakFailsOnAnUnreadableBaseline": "" +
		"the subject under test IS AssertNoFDLeak itself, driven through a synthetic *testing.T; " +
		"there is no leaking resource to exercise, so a 5-round loop would repeat one assertion",
}

// leakExerciseAnchors names one load-bearing call that must occur inside the qualifying loop at every
// live assertion site. Counting arbitrary loop bodies proved only "something iterated": an empty loop,
// or five rounds of unrelated setup followed by one real exercise, false-greened. The exact
// file:function key keeps this list site-scoped; the live/stale checks below make additions, renames and
// deletions fail closed.
var leakExerciseAnchors = map[string]string{
	// origin: hot-update S7 plan §11.3 / T52; S9 composition/runner and S7 receive-time custody follow-ups.
	"internal/infraattach/composition_test.go:TestBrokerCompositionRejectsIncompleteRuntimeBeforeBusinessDial": "exerciseBrokerComposition",
	"internal/infracompose/topology_test.go:TestManagedTopologyBindsSYSAndAcceptedProcess":                     "exerciseManagedTopology",
	"test/infraharness/installedrunner/main_test.go:TestInstalledEntryWitnessRejectsUnboundBytes":              "exerciseEntryWitness",

	// origin: hot-update S10 plan §11.2 / P29; S8 FLOW/SS CENSUS and S9 NATS graph handoffs.
	"internal/agentupgradeport/flow_ss_census_test.go:TestFlowCensusKeepsTerminalReceiptsAndRetirement":     "exerciseFlowJournalCensus",
	"internal/agentupgradeport/flow_ss_census_test.go:TestSSCensusPreservesIdleMaterialAndCutIdentity":      "exerciseSSIdentityCensus",
	"internal/agentupgradeport/parent_lease_linux_test.go:TestBorrowedRuntimeLeaseKeepsOriginalReservation": "exerciseBorrowedRuntimeLease",
	"test/hotupdate/rollback_transaction_test.go:TestReadyPendingReverseKeepsCurrentFlowSSState":            "exercisePendingFlowSSReverse",
	"test/infranats/placement_readiness_test.go:TestNATSStockPlacementRequiresEveryPeer":                    "exerciseStockPlacement",
	// origin: hot-update S7 plan §11.3 / T52; S10 D-A/SINGLEACK/tagged sweeps and S9 supply/readback owners.
	"internal/broker/transaction_port_test.go:TestSingleBrokerTransactionRefusesMissingMaintenanceCapabilityBeforeQueue": "exerciseSingleBrokerTransactionMissingCapability",
	"internal/brokercontrol/maintenance/local_owner_test.go:TestSingleMaintenanceOwnerRetainsCASAndFixedProof":           "exerciseSingleMaintenanceOwner",
	"internal/caddyconfig/admin_reload_test.go:TestCaddyAdminReloadVerifiesFreshReadback":                                "exerciseAdminReloadReadback",
	"internal/testharness/release_candidate_test.go:TestReleaseCandidateBudgetRejectsRaceBinary":                         "exerciseReleaseCandidateBudget",
	"internal/testharness/release_candidate_test.go:TestReleaseCandidateCacheTracksSourceAndOverlay":                     "exerciseReleaseCandidateCache",
	"test/cli_e2e/upgrade_budget_build_test.go:TestUpgradeBudgetCandidateUsesReleaseBuild":                               "exerciseUpgradeBudgetCandidateBuild",
	"test/clusterharness/broker_product_budget_linux_test.go:TestBrokerBudgetCandidateUsesReleaseBuild":                  "exerciseBrokerBudgetBuild",
	"test/hotupdate/fullstack_pty_exec_test.go:TestFullstackSyscallObserverJoinsWithFixture":                             "exerciseFullstackSyscallObserver",
	"test/hotupdate/fullstack_task_exec_test.go:TestFullstackRuntimeSampleUsesProductCustody":                            "exerciseFullstackRuntimeSample",
	"test/hotupdate/transfer_budget_build_test.go:TestTransferBudgetCandidateUsesReleaseBuild":                           "exerciseTransferBudgetBuild",
	"test/infracaddy/tcp_window_test.go:TestCaddyTCPBackpressureReleasesOriginalStream":                                  "exerciseTCPBackpressureRelease",
	"test/infracaddy/websocket_stream_test.go:TestCaddyWebsocketStreamDeliversAvailablePrefix":                           "exerciseWebsocketPrefix",
	"test/infraharness/supply_controls_test.go:TestInfraSupplySourceCustody":                                             "exerciseSupplySourceCustody",
	"test/infranats/stock_supply_test.go:TestNATSWaitRecognizesNativeKillStatus":                                         "exerciseNativeWait",

	// origin: hot-update S7 plan §11.3 / T52; S8 roll-recovery-stall five-round resource subjects.
	"cmd/tether/cluster_roll_address_linux_test.go:TestSignedRollPortLeaseSurvivesHandoffAcrossProcesses":                   "exerciseSignedRollPortLease",
	"internal/brokerupgradeport/activation_failure_test.go:TestBrokerActivationRetainsArmFailure":                           "exerciseBrokerArmFailure",
	"internal/brokerupgradeport/activation_failure_test.go:TestBrokerArmingRefusalCannotHideRecoveryAsTransient":            "exerciseBrokerArmingRecoveryObservation",
	"internal/brokerupgradeport/activation_failure_test.go:TestBrokerUnqueuedArmRetiresReservationWithoutReturned":          "exerciseBrokerUnqueuedArm",
	"internal/upgradestore/broker_activation_recovery_test.go:TestBrokerRecoversActivationWithoutDurableSelector":           "exerciseBrokerActivationRecovery",
	"internal/upgradestore/broker_activation_recovery_test.go:TestBrokerUnqueuedActivationReleasesCurrentWithSourceProof":   "exerciseBrokerUnqueuedActivation",
	"internal/upgradestore/broker_activation_recovery_test.go:TestImportedBrokerRecoversAfterSourceJournalsUncertainty":     "exerciseImportedBrokerRecovery",
	"internal/upgradestore/preactivation_cancellation_test.go:TestBrokerPreActivationFailureReleasesCurrentWithSourceProof": "exerciseBrokerPreActivationFailure",

	"cmd/tether/cluster_roll_artifact_linux_test.go:TestSignedRollBuildReusesPackageArtifacts":   "exerciseSignedRollArtifactReuse",
	"test/cli_e2e/realstack_auth_timeout_test.go:TestCalloutFaultTimeoutStartsAfterBootstrap":    "exerciseCalloutFaultTimeout",
	"test/cli_e2e/reliable_subprocess_test.go:TestFixtureIsolationKeepsGoTelemetryWithinSandbox": "exerciseFixtureTelemetrySandbox",
	"test/p13/proxy_reconnect_e2e_test.go:TestProxyStartupKeepsProductLivenessWindow":            "exerciseProxyProductLiveness",
	// origin: hot-update S7 plan §11.3 / T52; merged legacy, readback and signed-roll lifecycle subjects.
	"internal/agentupgradeport/parent_lease_linux_test.go:TestParentLeaseQueriesAndReleaseShareOneFence":                   "exerciseParentLeaseFence",
	"internal/broker/legacy_transfer_history_test.go:TestLegacyTransferHistoryRetainsAdmissionIdentity":                    "exerciseLegacyTransferHistory",
	"internal/jsstream/object_readback_test.go:TestTransferWriterRechecksRawReceiptAfterEarlyLookup":                       "exerciseEarlyWriterReadback",
	"internal/jsstream/object_readback_test.go:TestTransferWriterRequiresOriginalMessageIDInRawReceipt":                    "exerciseWriterMessageIDReadback",
	"internal/spawnsafe/version_probe_linux_test.go:TestVersionProbeSupervisorAvoidsExitDelayAndPreservesCandidateOptions": "exerciseVersionProbeRaceOptions",
	"internal/upgradeartifact/publication_readback_linux_test.go:TestPublicationDoesNotRepeatFinalWholeImageReadback":      "exercisePublicationReadBudget",
	"internal/upgradeartifact/publication_readback_linux_test.go:TestPublicationFinalReadbackRejectsLateImageChanges":      "exercisePublicationLateChanges",
	"internal/upgradeartifact/publication_readback_linux_test.go:TestPublicationRejectsGrantExpiryDuringReceiptSync":       "exercisePublicationSyncExpiry",
	"internal/xferworker/receiver_admission_test.go:TestTransferLandedReceiverAllowsNextTierBWhileCourierWaits":            "exerciseReceiverAdmission",
	"test/hotupdate/authority_evidence_test.go:TestAuthorityParallelTerminationRequiresSuppliedEvidence":                   "exerciseAuthorityEvidenceAdmission",

	// origin: hot-update S7 plan §11.3 / T52; TRANSFER-MAKETEST five-round subjects.
	"internal/jsstream/object_retention_admission_test.go:TestTransferWriterRevalidatesRetentionBeforeRecoveryAndMetadata": "exerciseWriterRetentionBoundaries",
	"test/cli_e2e/reliable_subprocess_test.go:TestFixtureIsolationKeepsUnixSocketPathsWithinKernelLimit":                   "exerciseFixtureUnixSocket",
	"test/transferharness/cluster_test.go:TestTransferClusterReservationsExcludeOutboundPorts":                             "exerciseTransferClusterReservation",

	// origin: hot-update S8 cluster-roll F10 — five reads while the child holds the guard.
	"internal/upgradestore/prepared_test.go:TestParentBindingReadDoesNotRelockTheHeldGuard": "exerciseParentBindingGuard",
	// origin: hot-update S10 plan §0 / LINT-L2 errcheck; each owner exercises five rounds directly.
	"internal/cli/transfer_capacity_test.go:TestTransferSelectionPreservesCapacityReleaseFailure":          "exerciseTransferCapacityReleaseFailure",
	"internal/cli/transfer_command_cleanup_test.go:TestTransferCommandRetiresSourceThroughItsCurrentOwner": "exerciseTransferCommandSourceCustody",

	// Each subject is the direct call in its existing five-round lifecycle.
	// origin: S10 internal review GATE-FINAL F3 / S7 OWNER-RECOVERY
	"internal/broker/upgrade_tx_forward_test.go:TestSilentAgentDoesNotDelayAnotherAgentsTransaction":            "exerciseSilentAgentForward",
	"internal/infraattach/broker_recovery_test.go:TestBrokerRecoveryRetiresUnknownProducerTransport":            "exerciseBrokerUnknownRetirement",
	"internal/infratransfer/recovery_test.go:TestTransferRecoveryCapturesOriginalACKsBeforeSeal":                "exerciseOriginalACKs",
	"internal/infrawitness/attachment_linux_test.go:TestPhysicalWitnessBindsOriginalSocketAndIndependentHealth": "exercisePhysicalWitness",
	"test/cli_e2e/broker_transaction_cut_test.go:TestTransactionCLIPrepareStartWaitCompletesBrokerCut":          "exerciseBrokerTransactionCut",
	"internal/xferclient/push_recovery_test.go:TestTransferPushRetriesOwnerRecoveringAcrossWorkerFreeze":        "exercisePushFrozenOwnerRecovery",
	"internal/xferclient/push_recovery_test.go:TestTransferPushOwnerRecoveryPreservesDefinitiveRefusals":        "exercisePushRecoveryRefusals",
	"internal/xferclient/push_recovery_test.go:TestTransferPushOwnerRecoveryStopsAtOriginalDeadline":            "exercisePushRecoveryDeadline",

	// origin: hot-update S7 plan §11.3 / T52; S8 R256, PULL-B and S9 INSTRUMENTS-3.
	"internal/agentupgradeport/authority_census_test.go:TestAgentCensusCarriesCurrentIncarnation":                          "exerciseAgentCensusIncarnation",
	"internal/cli/transfer_status_connection_test.go:TestTransferSettledStatusUsesRetainedMemberConnection":                "exerciseSettledStatusConnection",
	"internal/upgraderoll/endpoint_test.go:TestRollEndpointRunsOnlyTheSealedSteps":                                         "exerciseBoundRoll",
	"internal/xferclient/prepared_pull_reconcile_test.go:TestTransferPreparedPullReconcilesLostDataAttachment":             "exercisePreparedPullAttachmentRecovery",
	"internal/xferclient/prepared_pull_reconcile_test.go:TestTransferPreparedPullRecoveryRequiresLiveOriginalOwner":        "exercisePreparedPullRecoveryRefusal",
	"internal/xferserve/sender_retirement_test.go:TestTransferPullRetirementRetriesExactSenderReceipt":                     "exercisePullRetirementRetry",
	"internal/xferworker/prepare_recovery_test.go:TestTransferWorkerPrepareReplayRecoversUnallocatedSender":                "exercisePrepareSenderRecovery",
	"internal/xferworker/sender_reconnect_test.go:TestTransferWorkerPullSenderRetriesTransportFailure":                     "exercisePullSenderTransportRecovery",
	"test/simcluster/drills/lib/infraobserve/probe_test.go:TestPhysicalProtocolProbeKeepsExactConnectionAndClosesFailures": "exerciseProtocolProbe",

	// origin: hot-update S7 plan §11.3 / T52; merged S8–S10 lifecycle receipts.
	// Subjects are live calls in the existing five-round loops, never exemptions.
	"cmd/tether/upgrade_transaction_cli_test.go:TestTransactionCLIQueryUnknownIsZeroWaitIs74":                               "exerciseUnknownTransactionCLI",
	"internal/agent/infra_wiring_test.go:TestAgentInfraLifecycle":                                                           "exerciseAgentInfra",
	"internal/agent/infra_wiring_test.go:TestAgentInfraMovePreservesDataOwners":                                             "exerciseAgentInfra",
	"internal/agent/upgrade_runtime_port_test.go:TestAgentTransactionCutCarriesAdjunctAndLease":                             "exerciseTransactionCut",
	"internal/agent/upgrade_transaction_boot_test.go:TestFinishBootReturnsTransactionImageToSource":                         "exerciseTransactionFinishBoot",
	"internal/agent/upgrade_transaction_boot_test.go:TestTransactionBootImportsOnlyTheAcceptedCut":                          "exerciseTransactionBootImport",
	"internal/agent/upgrade_transaction_refusal_test.go:TestAbandonedTransactionCutThawsWithinBudget":                       "exerciseAbandonedTransactionCut",
	"internal/agentboot/upgrade_cut_custody_test.go:TestBootAdoptsUpgradeLeaseBeforeActivationCleanup":                      "exerciseBootUpgradeLease",
	"internal/agentboot/upgrade_cut_custody_test.go:TestBootKeepsAdoptedLeaseUntilActivationCommits":                        "exerciseBootLeaseRelease",
	"internal/agentboot/upgrade_cut_custody_test.go:TestBootRefusedUpgradeLeaseKeepsPristineCustody":                        "exerciseBootRefusedUpgradeLease",
	"internal/broker/reconcile_authority_test.go:TestGuardedBrokerWritesRejectRecreatedSession":                             "exerciseBrokerAuthorityWrites",
	"internal/broker/reconcile_authority_test.go:TestGuardedForwardPlannerRejectsRecreatedSession":                          "exerciseForwardAuthorityPlanner",
	"internal/broker/reconcile_authority_test.go:TestGuardedSessionCascadeKeepsReplacementDomain":                           "exerciseGuardedSessionCascade",
	"internal/broker/upgrade_trigger_test.go:TestBrokerUpgradeEntriesLeakNothing":                                           "exerciseBrokerUpgradeEntries",
	"internal/brokercontrol/maintenance/state_test.go:TestBrokerMaintenanceLeakCycles":                                      "exerciseMaintenanceExpiry",
	"internal/brokercontrol/upgrade_port_test.go:TestAdminUpgradePortJoinsOwnedCancellation":                                "exerciseAdminUpgradePort",
	"internal/brokerimage/boot_upgrade_cut_test.go:TestBrokerBootAdmitsUpgradeCutOutsidePrivateClosure":                     "exerciseBrokerUpgradeCut",
	"internal/brokerimage/boot_upgrade_cut_test.go:TestBrokerPrepareAdmitsOnlyTheClosedUpgradeCut":                          "exerciseBrokerPrepareExtras",
	"internal/cli/infra_connection_test.go:TestCLIInfraLifecycle":                                                           "exerciseCLIInfra",
	"internal/cli/transfer_commit_settlement_test.go:TestTransferNativeLostCommitUsesRetainedTerminal":                      "exerciseNativeCommitSettlement",
	"internal/handoff/image_cleanup_test.go:TestPreparePartialFailureReleasesDescriptorCopies":                              "Prepare",
	"internal/infraattach/client_test.go:TestInfraAttachmentLifecycle":                                                      "exerciseAttachment",
	"internal/infraroot/installfs/files_test.go:TestInstalledFilesLifecycle":                                                "exerciseInstalledFiles",
	"internal/agent/bootstrap_supervision_test.go:TestIsolatedAgentReapsDetachedPTYSession":                                 "exerciseIsolatedActivationPTY",
	"internal/agent/session_destroyed_pty_test.go:TestPTYIncarnationRejectionRevokesSession":                                "exercisePTYRecreatedAuthority",
	"internal/agent/transfer_session_test.go:TestTransferImportedReceiverSnapshotsRebuiltSession":                           "exerciseTransferReceiverSession",
	"internal/xferserve/legacy_gc_lifetime_test.go:TestLegacyReaperDeadlineAndStopJoinBothLists":                            "exerciseLegacyReaperLifetime",
	"internal/broker/infra_wiring_test.go:TestBrokerInfraLifecycle":                                                         "exerciseBrokerInfra",
	"internal/infraattach/broker_test.go:TestBrokerAttachmentLifecycle":                                                     "exerciseBrokerAttachment",
	"internal/broker/session_cascade_forward_test.go:TestForwardedSessionCascadeDeletesOnlyCapturedCreation":                "exerciseForwardedSessionCascade",
	"internal/broker/session_rm_contract_test.go:TestSessionRmRejectsUnavailableCreationAuthority":                          "exerciseSessionRmUnavailableAuthority",
	"internal/infracli/command_test.go:TestInfraCLILifecycle":                                                               "exerciseInfraCLI",
	"cmd/tether/infra_upgrade_test.go:TestInfraCLIStatusJSONAndNativeExits":                                                 "exerciseInfraCLIProcess",
	"internal/broker/infra_control_test.go:TestBrokerInfraControlUsesAccountAuthorityAndDirectSubscription":                 "exerciseBrokerInfraControl",
	"internal/infraattach/service_test.go:TestBrokerServiceSharesRecoveryAndAdminHealthRuntime":                             "exerciseBrokerService",
	"internal/xferagent/transport_attempt_test.go:TestTransferAgentAttemptRecordsItsEndOnEveryRefusal":                      "exerciseRefusedAgentAttempts",
	"internal/xferclient/prepare_reconcile_test.go:TestTransferClientReplaysPrepareAfterForwardedReplyLoss":                 "exercisePrepareReconciliation",
	"test/hotupdate/transfer_collector_lifecycle_test.go:TestTransferCollectorAcceptsLateBatchWithoutHidingSurvivors":       "exerciseLateTransferBatch",
	"internal/agentupgrade/transaction_wiring_test.go:TestControlProbeTraversesTheBrokerRelay":                              "exerciseControlProbeRelay",
	"internal/brokerupgrade/runtime_port_test.go:TestControlProbeRelayAnswersOnlyTheRequestingAgent":                        "exerciseControlProbeRelay",
	"test/hotupdate/transaction_agent_cut_test.go:TestTransactionCLIPrepareStartWaitCompletesAgentCut":                      "exerciseTransactionAgentCut",
	"internal/jsstream/object_reader_batch_test.go:TestTransferObjectScopesLateBatchCompletionToSatisfiedPull":              "exerciseObjectLateBatchCompletion",
	"internal/jsstream/object_reader_batch_test.go:TestTransferObjectRejectsUnsatisfiedPullStatuses":                        "exerciseObjectUnsatisfiedPullStatuses",
	"cmd/tether/cluster_roll_address_linux_test.go:TestSignedRollListenersAvoidEphemeralSourcePorts":                        "exerciseSignedRollPortRange",
	"internal/infratransfer/recovery_readback_test.go:TestTransferRecoveryDistinguishesUnknownPublishFromReadback":          "exerciseUnknownACKReadback",
	"internal/brokerupgrade/agent_query_test.go:TestAgentQueryRecoversLostCutReplyWithinOriginalBudget":                     "exerciseLostAgentQuery",
	"internal/brokerupgrade/agent_query_test.go:TestAgentQueryRecoveryKeepsDeadlineAndMutationBoundary":                     "exerciseAgentQueryBoundaries",
	"test/cli_e2e/transfer_cut_test.go:TestTransferVictimCutsOverlapAndJoinBeforeCancellation":                              "exerciseTransferVictimCutJoin",
	"test/cli_e2e/transfer_cut_test.go:TestTransferFixtureSetupCannotConsumeOriginalInvocationWait":                         "exerciseTransferFixtureDeadline",
	"internal/cluster/wal_concurrency_test.go:TestWALConcurrency":                                                           "exerciseWALConcurrency",
	"internal/cluster/wal_writer_progress_test.go:TestWALConcurrencyWithSlowDurableCommit":                                  "exerciseWALConcurrency",
	"internal/cluster/wal_writer_progress_test.go:TestWALBackupKeepsWriterAvailable":                                        "exerciseWALWriterDuringBackup",
	"internal/drillobserve/custody_linux_test.go:TestCustodyRecordedStream":                                                 "exerciseCustodyRecording",
	"internal/jsstream/object_streaming_test.go:TestTransferReaderSyncsPublishedPrefixBeforeMetadata":                       "exerciseTransferStreamingPrefix",
	"internal/xferserve/directory_test.go:TestTransferDirectoryClaimWaitsForOriginalCASVisibility":                          "exerciseDirectoryClaimVisibility",
	"internal/xferserve/directory_test.go:TestTransferPermitClaimUncertaintyKeepsOriginalInvocation":                        "exercisePermitClaimUncertainty",
	"test/e2e/parallel/transfer_partition_test.go:TestTransferBoundaryPartitionsAlsoCoverDirectMatrix":                      "exerciseTransferDirectPartitions",
	"test/hotupdate/transfer_collector_lifecycle_test.go:TestTransferDrainACKKeepsItsReceiveWatermark":                      "exerciseTransferDrainWatermark",
	"test/hotupdate/transfer_collector_lifecycle_test.go:TestTransferDrainACKRetainsPoisonAndOneUse":                        "exerciseTransferDrainRejections",
	"test/hotupdate/transfer_pull_selection_test.go:TestTransferSmallPullReconcilesAcceptedPrepare":                         "exerciseAcceptedSmallPull",
	"cmd/tether/cluster_roll_address_linux_test.go:TestSignedRollFutureListenersKeepKernelCustodyUntilHandoff":              "exerciseSignedRollListenerCustody",
	"internal/tunnel/server_retirement_test.go:TestImportedAllocationRetirementRequiresItsSealedFenceAndJoinedOwners":       "exerciseImportedAllocationRetirement",
	"internal/upgradecli/clusterroll/broker_settlement_test.go:TestBrokerRollSettlesTopologyBeforeDispatchingOriginalChild": "exerciseBrokerTopologySettlement",
	"internal/upgraderoll/settlement_test.go:TestAcceptedArmingResultQueriesTheOriginalParentAndChild":                      "exerciseAcceptedArmingSettlement",
	"test/clusterharness/broker_product_barrier_linux_test.go:TestCohortTrafficRequiresTheEmittedReadinessEvent":            "exerciseCohortReadinessEvent",
	"test/clusterharness/broker_quorum_settlement_linux_test.go:TestBrokerCohortCleanupRequiresItsOriginalParentSettlement": "exerciseCohortParentSettlement",
	"test/p10/upgrade_e2e_test.go:TestUpgradeLegacyInstallRequiresTransaction":                                              "exerciseUpgradeLegacyInstall",
	"test/p10/upgrade_e2e_test.go:TestUpgradeSmokeGateRejectsNonBinary":                                                     "exerciseUpgradeSmokeGate",
	"test/p10/upgrade_e2e_test.go:TestUpgradeRejectedWhilePending":                                                          "exerciseUpgradePendingRefusal",
	"test/p10/upgrade_e2e_test.go:TestUpgradeConcurrentInstallRejected":                                                     "exerciseUpgradeConcurrentInstall",
	"test/p10/upgrade_e2e_test.go:TestUpgradeConcurrentInstallAcrossAgentsRejected":                                         "exerciseUpgradeConcurrentAgents",
	"test/p10/upgrade_e2e_test.go:TestUpgradeExpiredLegacyMarkerCannotAuthorizeRetry":                                       "exerciseUpgradeExpiredMarker",
	"test/p10/upgrade_e2e_test.go:TestUpgradeCandidateRejectsBadSHA":                                                        "exerciseUpgradeCandidateSHA",
	"test/p10/upgrade_e2e_test.go:TestUpgradeLegacyURLCannotAuthorizeDownload":                                              "exerciseUpgradeLegacyURL",
	"internal/jsstream/object_resume_bounds_test.go:TestTransferReaderBoundsLostRecoveryReplies":                            "exerciseTransferReaderLostReply",
	"internal/upgradecli/stage_port_test.go:TestStagePortLifecycleNoLeaks":                                                  "exerciseStagePort",
	"internal/upgradecut/handoff_test.go:TestAdjunctCustodyReleasesOnlyAnActiveAdoptedLease":                                "exerciseAdjunctCustody",
	"internal/upgradecut/handoff_test.go:TestAdjunctHandoffResourceIsClosed":                                                "exerciseAdjunctHandoffResource",
	"internal/upgradecut/handoff_test.go:TestAdjunctTakeLeaseFromHandoffSource":                                             "exerciseAdjunctTakeLease",
	"internal/xferagent/inline_pull_test.go:TestTransferInlinePullSurvivesCutReplayAndSettlement":                           "exerciseInlinePullCut",
	"internal/xferagent/inline_push_test.go:TestTransferInlinePushReceiverLandsRetainedPayload":                             "exerciseInlinePushReceiver",
	"internal/xferagent/inline_push_test.go:TestTransferInlinePushReceiverSurvivesImageCutAndRollback":                      "exerciseInlinePushReceiverCut",
	"internal/xferclient/commit_settlement_test.go:TestTransferLostCommitClosedGateResolvesCanonicalTerminal":               "exerciseLostCommitClosedGate",
	"internal/xfersend/inline_sender_test.go:TestTransferInlineSenderRetainsExactSealedCopy":                                "exerciseInlineSenderCopy",
	"internal/xferserve/inline_link_test.go:TestTransferInlinePrepareCarriesRetainedCopyAndSeal":                            "exerciseInlinePrepareLink",
	"test/fullstackharness/cmd/supervise/dispatch_linux_test.go:TestRunAllBindsFullstackSupervisor":                         "exerciseRunAllLauncherBinding",
	"test/fullstackharness/cmd/supervise/main_linux_test.go:TestSupervisorDiagnosticsDistinguishCommandFailure":             "exerciseSupervisorFailureDiagnostic",
	"test/fullstackharness/cmd/supervise/signal_linux_test.go:TestSupervisorEscalatesIgnoredSignal":                         "exerciseSupervisorIgnoredSignal",
	"test/fullstackharness/cmd/supervise/signal_linux_test.go:TestSupervisorForwardsSignalsAndKeepsEscapeFailure":           "exerciseSupervisorSignalForwarding",
	"test/fullstackharness/process_reaping_linux_test.go:TestFullstackSupervisorReapsAdoptedZombiesWhileTargetLives":        "exerciseLauncherAdoptedZombie",
	"test/fullstackharness/process_reaping_linux_test.go:TestFullstackSupervisorReapsInheritedHeredocWriters":               "exerciseLauncherInheritedWriter",
	"test/fullstackharness/process_reaping_linux_test.go:TestFullstackSupervisorRetainsExitDescendantCensus":                "exerciseLauncherExitCensus",
	"test/hotupdate/transfer_legacy_test.go:TestTransferClusterLeaderChangeKeepsOriginalInvocation":                         "exerciseTransferClusterLeaderChange",
	"test/hotupdate/transfer_legacy_test.go:TestTransferClusterOriginRemovalRefusesReplacement":                             "exerciseTransferClusterOriginRemoval",
	"test/hotupdate/transfer_legacy_test.go:TestTransferTopologyStartsPrivateCluster":                                       "exerciseTransferClusterTopology",
	"test/transferharness/cluster_test.go:TestTransferClusterInfrastructureKeepsPrivateStores":                              "exerciseTransferClusterInfrastructure",
	"test/transferharness/cluster_test.go:TestTransferClusterListenerPortsCannotBeClaimedByOutboundNATS":                    "exerciseTransferClusterPortIsolation",

	// origin: hot-update S7 plan §11.3 / T52; S8–S10 merged lifecycle receipts.
	// Each subject is the live direct call in its five-round exercise loop.
	"cmd/tether/session_removal_connection_test.go:TestSessionRemovalBinaryReconnectsWithRestrictedScope":                   "exerciseSessionRemovalReconnect",
	"cmd/tether/session_rm_transfer_barrier_test.go:TestSessionRemovalHonoursTheTransferSettlementBarrier":                  "exerciseSessionRemovalHonoursTheTransferSettlementBarrier",
	"cmd/tether/transfer_abandon_test.go:TestLegacyPullClosesItsPrivateTmpDescriptor":                                       "exerciseLegacyPullDescriptor",
	"cmd/tether/transfer_abandon_test.go:TestLegacyPullFailureSendsFailedFinalize":                                          "exerciseLegacyPullFailure",
	"cmd/tether/transfer_capable_path_test.go:TestTransferCommandsCompleteThroughCapableBrokerAndAgent":                     "exerciseTransferCommandsCompleteThroughCapableBrokerAndAgent",
	"cmd/tether/transfer_capable_path_test.go:TestTransferRateLimitIsSealedAndPaced":                                        "exerciseTransferRateLimitIsSealedAndPaced",
	"cmd/tether/transfer_commit_lost_test.go:TestTransferCommitLostResumesTheSameTransfer":                                  "exerciseTransferCommitLostResumesTheSameTransfer",
	"cmd/tether/transfer_legacy_acl_test.go:TestPutWatchdogHonoursAPutThatFinishedInsideTheDeniedPurge":                     "exerciseLegacyDeniedPurgeCompletion",
	"cmd/tether/transfer_legacy_acl_test.go:TestPutWatchdogReturnsThePutsOwnErrorHeldByTheDeniedPurge":                      "exerciseLegacyDeniedPurgeReadError",
	"cmd/tether/transfer_startup_test.go:TestTransferCapabilitiesNeverDowngradeDuringStartup":                               "exerciseTransferStartup",
	"internal/adminsock/infra_test.go:TestInfraAdminLifecycle":                                                              "exerciseInfraAdmin",
	"internal/adminsock/infra_test.go:TestInfraWireAdjacentArtifactQuadrants":                                               "exerciseInfraArtifactQuadrants",
	"internal/agent/runtime_observation_test.go:TestAgentRuntimeGaugeComesFromObservedProcess":                              "exerciseAgentRuntimeObservation",
	"internal/agent/transfer_cut_test.go:TestTransferImageCutCarriesLiveTransferAndRetainedReceipts":                        "exerciseTransferImageCut",
	"internal/agent/upgrade_state_test.go:TestAgentControlReadyRejectsStaleNonceAndOwner":                                   "exerciseAgentControlProbe",
	"internal/agent/upgrade_state_test.go:TestAgentLateRegisterCannotUnlinkNextUpgrade":                                     "exerciseAgentRegisterUpgradeWriter",
	"internal/agent/upgrade_state_test.go:TestAgentRegisterKeepsUpgradePendingUntilControlReady":                            "exerciseAgentRegisterUpgradeWriter",
	"internal/agent/upgrade_state_test.go:TestAgentRegisterRejectsChangedIncarnation":                                       "exerciseAgentRegisterIncarnation",
	"internal/agent/upgrade_state_test.go:TestRunArmsWatchdogAndRollsBackAtDeadline":                                        "exerciseRunUpgradeWatchdog",
	"internal/auth/infra_permissions_test.go:TestInfraACLMatchesLiveSubscriptions":                                          "exerciseInfraSubscriptions",
	"internal/broker/auth_jetstream_order_test.go:TestImageAuthBecomesUsableBeforeJetStreamWitness":                         "exerciseImageAuthBecomesUsableBeforeJetStreamWitness",
	"internal/broker/auth_jetstream_order_test.go:TestImageAuthRebindsJetStreamToCurrentTransport":                          "exerciseImageAuthCurrentJetStream",
	"internal/broker/auth_jetstream_order_test.go:TestImageAuthSurvivesJetStreamWitnessFailure":                             "exerciseImageAuthSurvivesJetStreamWitnessFailure",
	"internal/broker/auth_jetstream_order_test.go:TestImageJetStreamFailureKeepsAuthUsableWithoutFullReady":                 "exerciseImageJetStreamFailure",
	"internal/broker/cluster_control_resume_test.go:TestBrokerFinalReloadGuardRefusalThawsOriginalOwner":                    "exerciseBrokerFinalReloadGuard",
	"internal/broker/cluster_control_resume_test.go:TestClusterControlRepeatedLegacyRefusalThawsOneOwner":                   "exerciseClusterRepeatedLegacyRefusal",
	"internal/broker/cluster_control_resume_test.go:TestClusterControlResumePreservesOneUpgradeResponder":                   "exerciseClusterControlResumePreservesOneUpgradeResponder",
	"internal/broker/legacy_reload_policy_test.go:TestLegacyReloadPolicyCoversLocalAndSignedEntries":                        "exerciseLegacyReloadPolicy",
	"internal/broker/maintenance_local_admission_test.go:TestLocalReloadRefusesFleetWithoutMaintenanceCapability":           "exerciseLocalReloadRefusesFleetWithoutMaintenanceCapability",
	"internal/broker/maintenance_local_admission_test.go:TestLocalReloadRequiresSignedInstalledAgentAck":                    "exerciseLocalInstalledAgentAck",
	"internal/broker/transfer_cluster_authority_test.go:TestTransferClusterConfigHandoffRetiresOriginalWriter":              "exerciseClusterConfigHandoff",
	"internal/broker/transfer_cluster_authority_test.go:TestTransferClusterConfigHandoffWaitsForInflightUpdate":             "exerciseInflightConfigHandoff",
	"internal/broker/transfer_cluster_authority_test.go:TestTransferClusterPermitKeepsOriginalWindowAcrossLeaderAndRemoval": "exerciseOriginalClusterPermit",
	"internal/broker/transfer_cluster_authority_test.go:TestTransferClusterRevokedReceiverUsesRestrictedReconnect":          "exerciseClusterSettlementReconnect",
	"internal/broker/transfer_cluster_authority_test.go:TestTransferClusterSettlementResolvesCurrentOriginalOwner":          "exerciseCurrentSettlementOwner",
	"internal/broker/transfer_graph_cut_test.go:TestTransferGraphSurvivesBrokerCutRollback":                                 "exerciseTransferGraphRollback",
	"internal/broker/transfer_reconcile_deadline_test.go:TestTransferProductionReaperTTLListsReleaseRegistry":               "exerciseProductionReaperTTL",
	"internal/broker/transfer_route_census_test.go:TestTransferRouteStartupRetriesOccupiedPort":                             "exerciseTransferRoutePortCollision",
	"internal/broker/upgrade_legacy_policy_test.go:TestLegacyClusterReexecRequiresPolicyBeforeDispatch":                     "exerciseLegacyClusterReexec",
	"internal/broker/upgrade_legacy_policy_test.go:TestLegacyUpgradeRefusalUsesPositiveBusyEvidence":                        "exerciseLegacyUpgradeEvidence",
	"internal/broker/writer_retirement_test.go:TestTransferAuthenticatedWriterWitnessFencesFailedObjectCleanup":             "exerciseAuthenticatedWriterRetirement",
	"internal/broker/xfer_converge_admission_test.go:TestTransferBrokerCreatedBucketAdmitsCapableObject":                    "exerciseBrokerCreatedBucket",
	"internal/broker/xfer_converge_admission_test.go:TestTransferLegacyFullBucketRefusesWithoutEviction":                    "exerciseLegacyFullBucket",
	"internal/broker/xfer_converge_admission_test.go:TestTransferOldConvergeFailsClosedUntilDiscardReapplied":               "exerciseOldDiscardConverge",
	"internal/brokercontrol/infra_recovery_test.go:TestBrokerRecoveryFencesStreamAndAuditRoots":                             "exerciseBrokerRecoveryRoots",
	"internal/brokerimage/legacy_reload_policy_test.go:TestLegacyReloadBackendsCannotArmWithoutGrant":                       "exerciseLegacyReloadBackends",
	"internal/brokerimage/store_reload_boundary_test.go:TestReloadPreparedGuardRefusalResumesOriginalWriter":                "exerciseReloadPreparedGuardRefusal",
	"internal/brokerimage/store_reload_boundary_test.go:TestReloadShutdownGuardRefusalKeepsOpenStoreUsable":                 "exerciseReloadShutdownGuardRefusalKeepsOpenStoreUsable",
	"internal/cli/transfer_cleanup_test.go:TestTransferAttachmentRetainsConsumerCleanup":                                    "exerciseTransferConsumerCleanup",
	"internal/cli/transfer_legacy_cleanup_test.go:TestTombstoneUploadedObjectReturnsOnItsOwnDeadline":                       "exerciseTombstoneUploadedObjectReturnsOnItsOwnDeadline",
	"internal/cli/transfer_legacy_compatibility_test.go:TestTransferLegacyPullPrepareIncludesByteBudget":                    "exerciseTransferLegacyPullPrepareIncludesByteBudget",
	"internal/cli/transfer_legacy_compatibility_test.go:TestTransferLegacyPushWaitsForReceiverOutcome":                      "exerciseTransferLegacyPushWaitsForReceiverOutcome",
	"internal/cli/transfer_legacy_compatibility_test.go:TestTransferLegacyReplyBeyondControlWindow":                         "exerciseTransferLegacyReplyBeyondControlWindow",
	"internal/cli/transfer_legacy_outcome_test.go:TestTransferLegacyCommitAckCannotDecideOutcome":                           "exerciseLegacyCommitOutcome",
	"internal/cli/transfer_legacy_probe_test.go:TestTransferSelectionAcceptsLegacyBrokerCaps":                               "exerciseLegacyBrokerCaps",
	"internal/cli/transfer_pull_selection_test.go:TestTransferPullSelectionRefusesUnprovenSizeBeforeCreation":               "exerciseUnprovenPullSize",
	"internal/cli/transfer_pull_selection_test.go:TestTransferSelectionPinsSmallPullToInlineBeforeCreation":                 "exercisePullSelection",
	"internal/cli/transfer_selection_test.go:TestTransferSelectionCannotDowngradeCreatedInvocation":                         "exerciseTransferCreatedSelection",
	"internal/cli/writer_retirement_test.go:TestTransferWriterRetirementRequiresExactAuthenticatedACK":                      "exerciseWriterRetirementACK",
	"internal/drillobserve/native_exit_linux_test.go:TestNativeExitFixtureRejectsMissingLaunchOwner":                        "exerciseNativeMissingLaunchOwner",
	"internal/drillobserve/native_exit_linux_test.go:TestNativeExitFixtureReleasesAndVerifiesSupervisedResult":              "exerciseNativeExitFixture",
	"internal/drillobserve/native_exit_linux_test.go:TestNativeExitKeepsObserverUntilFinalSealedACK":                        "exerciseNativeExit",
	"internal/drillobserve/native_exit_linux_test.go:TestNativeExitLegacyCustodyRetainsImmediateExit":                       "exerciseLegacyNativeExit",
	"internal/drillobserve/native_exit_linux_test.go:TestNativeExitReleaseRequiresFreshWatermark":                           "exerciseNativeReleaseGuard",
	"internal/drillobserve/runtime_sample_test.go:TestRuntimeSampleCustodyRejectsPoison":                                    "exerciseRuntimeSampleCustody",
	"internal/drillobserve/transfer_generation_test.go:TestTransferImportBindsGenerationBeforeConstructorObservation":       "exerciseTransferImportGeneration",
	"internal/drillobserve/transfer_lifecycle_test.go:TestTransferExecDescriptorsRequireSealedStartupAuthority":             "exerciseTransferExecAuthority",
	"internal/drillobserve/transfer_settlement_test.go:TestTransferFinalizerObservesPendingBeforeHistoryCall":               "exerciseTransferPendingHistory",
	"internal/filetransfer/transfer_forwarded_codec_test.go:TestTransferForwardedCanonicalCarriers":                         "exerciseTransferForwardedCanonicalCarriers",
	"internal/infralaunch/authenticated_exec_test.go:TestAuthenticatedLaunchExecHandshake":                                  "exerciseAuthenticatedExec",
	"internal/infrapriv/launch_query_test.go:TestLaunchQueryUsesAuthenticatedManagementEndpoint":                            "exerciseLaunchQuery",
	"internal/infrarepair/gate_test.go:TestInfraRecoveryLifecycle":                                                          "exerciseRecovery",
	"internal/infraroot/installed_bootstrap_test.go:TestInstalledBootstrapInitializesBeforeOpeningWorker":                   "exerciseInstalledBootstrap",
	"internal/infraroot/launch_journal_test.go:TestInstalledRootJournalsAuthenticatedSamePIDExec":                           "exerciseRootJournalExec",
	"internal/infraupgrade/nats_recovery_release_test.go:TestNATSPreCutRecoveryVerifiesBeforeRelease":                       "exerciseNATSPreCutRecovery",
	"internal/jsstream/consumer_retirement_test.go:TestConsumerCleanupRebindDeletesAndRecoversCapacity":                     "exerciseConsumerCleanupRebind",
	"internal/jsstream/object_retirement_checkpoint_test.go:TestTransferRetirementCheckpointsBeforePublishAndPurge":         "exerciseRetirementCheckpoint",
	"internal/port/authority_guard_test.go:TestPortMutationRejectsRecreatedSession":                                         "exercisePortAuthority",
	"internal/proc/authority_guard_test.go:TestProcessMutationRejectsRecreatedSession":                                      "exerciseProcessMutation",
	"internal/session/authority_guard_test.go:TestSessionCleanupRejectsNewIncarnation":                                      "exerciseSessionCleanup",
	"internal/testharness/process_reaping_linux_test.go:TestProcessSupervisionReapsAdoptedZombiesWhileTargetLives":          "exerciseAdoptedZombieReaping",
	"internal/testharness/process_reaping_linux_test.go:TestProcessSupervisionRetainsUnwaitedZombieCensus":                  "exerciseUnwaitedZombieCensus",
	"internal/testharness/process_supervision_linux_test.go:TestProcessSupervisionRetainsDescendantCensus":                  "exerciseSupervisorCensus",
	"internal/transferpath/transfer_atomic_binding_test.go:TestTransferAtomicGrantRequiresDeclaredObjectVersion":            "exerciseTransferAtomicGrantRequiresDeclaredObjectVersion",
	"internal/upgradeobserve/recovery_gate_test.go:TestRecoveryGateRetainsAdmissionUntilCompletion":                         "exerciseRecoveryGate",
	"internal/upgradestore/proof_composition_test.go:TestCompletionCommitFencesBeginUntilDurableCAS":                        "exerciseCompletionCommit",
	"internal/upgradestore/transaction_boundary_leak_test.go:TestTransactionBoundaryLeakCycles":                             "exerciseTransactionBoundary",
	"internal/upgradewire/project_test.go:TestUpgradeAdminClientKeepsTransactionAndLegacyShapes":                            "exerciseUpgradeAdminClient",
	"internal/upgradewire/project_test.go:TestUpgradeAdminSocketRoutesStrictTransactions":                                   "exerciseUpgradeAdminSocket",
	"internal/upgradewire/project_test.go:TestUpgradeAdminTransactionCutKeepsReplyCustody":                                  "exerciseUpgradeAdminCut",
	"internal/xferagentimage/owner_test.go:TestTransferImageImportAdoptsThePreparedEntry":                                   "exercisePreparedImageAdoption",
	"internal/xferauth/abandon_canonical_test.go:TestTransferAbandonPersistsWithRealLedger":                                 "exerciseTransferAbandonPersistsWithRealLedger",
	"internal/xferauth/abandon_canonical_test.go:TestTransferAbandonProducesPublishableCanonical":                           "exerciseTransferAbandonProducesPublishableCanonical",
	"internal/xferauth/session_deadline_test.go:TestSessionControlLifetimeNoLeaks":                                          "exerciseSessionControlLifetime",
	"internal/xferauth/settlement_recovery_test.go:TestRecoveryExplicitAbandonmentPersistsCanonicalTerminal":                "exerciseRecoveryExplicitAbandonmentPersistsCanonicalTerminal",
	"internal/xferauth/settlement_recovery_test.go:TestRecoveryFinalizerRetiresAfterTrackerGenerationAdvance":               "exerciseRecoveryFinalizerRetiresAfterTrackerGenerationAdvance",
	"internal/xferauth/settlement_recovery_test.go:TestRecoveryReceiverFailureAfterSyncedBytesSettles":                      "exerciseRecoveryReceiverFailureAfterSyncedBytesSettles",
	"internal/xferauth/settlement_recovery_test.go:TestRecoverySessionRemovalPassesOriginalOriginToCancel":                  "exerciseRecoverySessionRemovalPassesOriginalOriginToCancel",
	"internal/xferbucket/config_discard_test.go:TestTransferBucketMigratesDiscardWithRecoveryWindow":                        "exerciseDiscardMigration",
	"internal/xferbucket/config_discard_test.go:TestTransferDiscardMigrationLeavesOperatorStreamsUntouched":                 "exerciseOperatorDiscard",
	"internal/xferbucket/config_discard_test.go:TestTransferDiscardMigrationRetainsUnknownUpdate":                           "exerciseUnknownDiscardUpdate",
	"internal/xferclient/legacy_probe_test.go:TestTransferLegacySDKCancellationJoinsOriginalPut":                            "exerciseLegacySDKCancellation",
	"internal/xferclient/legacy_watchdog_test.go:TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes":                   "exerciseLegacyProbeStrikes",
	"internal/xferclient/pull_durability_linux_test.go:TestTransferPullDurabilityRecoveryUsesOriginalAtomicAndExactHistory": "exercisePullDurabilityRecovery",
	"internal/xferclient/writer_retirement_test.go:TestTransferWriterStopRetainsWitnessUntilAuthenticatedACK":               "exerciseWriterWitnessDelivery",
	"internal/xfercontrol/pull_selection_test.go:TestTransferPullSelectionProbeNeverCreatesAuthority":                       "exercisePullSelectionProbe",
	"internal/xfercontrol/runtime_binding_test.go:TestTransferRuntimeRebindRetriesOriginalDirtyProposal":                    "exerciseRuntimeDirtyRebind",
	"internal/xferfinal/orphan_completion_test.go:TestTransferLegacyOrphanCompletionRetainsUncertainWrites":                 "exerciseTransferLegacyOrphanCompletionRetainsUncertainWrites",
	"internal/xferfinal/orphan_completion_test.go:TestTransferLegacyOrphanPersistsCompletedRetirement":                      "exerciseTransferLegacyOrphanPersistsCompletedRetirement",
	"internal/xferfinal/writer_retirement_test.go:TestTransferFinalizerTakesWriterRetirementFromTheFence":                   "exerciseFinalizerWriterRetirement",
	"internal/xfergc/collector_test.go:TestTransferCollectorJoinsRetirementCheckpointPass":                                  "exerciseRetirementCheckpointPass",
	"internal/xfergc/retirement_checkpoint_test.go:TestTransferRetirementCheckpointJournalNoLeaks":                          "exerciseRetirementCheckpoint",
	"internal/xfergc/retirement_checkpoint_test.go:TestTransferRetirementCheckpointProducerLockKeepsDeadline":               "exerciseRetirementCheckpointProducerLockDeadline",
	"internal/xferimage/tracker_abandon_test.go:TestTransferAbandonedTrackerPassesCurrentCodec":                             "exerciseAbandonedTrackerCodec",
	"internal/xferimage/tracker_abandon_test.go:TestTransferAbandonedTrackerRoundtripKeepsCurrentEvidence":                  "exerciseAbandonedTrackerRoundtrip",
	"internal/xferreceive/image_checkpoint_test.go:TestTransferReceiverImageCarriesVerifiedPrimitiveDigest":                 "exerciseReceiverPrimitiveDigest",
	"internal/xfersend/inline_budget_test.go:TestTransferSenderInlineRateRefusesBeforeReservation":                          "exerciseSenderInlineAdmission",
	"internal/xferserve/assemble_test.go:TestTransferConfigWriterSucceedsAcrossGraphReinstall":                              "exerciseConfigWriterReinstall",
	"internal/xferserve/authority_service_test.go:TestTransferAuthorityJoinIncludesLeaderVerification":                      "exerciseAuthorityLeaderJoin",
	"internal/xferserve/directory_test.go:TestTransferDirectoryClaimsBindIssuerAndSurviveReopen":                            "exerciseDurableDirectoryClaim",
	"internal/xferserve/directory_test.go:TestTransferDirectoryRejectsRetainedStreamLossAndUnsafeRetention":                 "exerciseDirectoryLoss",
	"internal/xferserve/retirement_test.go:TestTransferRetirementCollectionSharesProducerMutex":                             "exerciseRetirementProducerMutex",
	"internal/xferserve/retirement_test.go:TestTransferRetirementCollectionUsesOriginalAuthority":                           "exerciseRetirementAuthority",
	"internal/xferserve/retirement_test.go:TestTransferRetirementCourierRequiresPhysicalReceiverRole":                       "exercisePhysicalReceiverRole",
	"internal/xferserve/settlement_journal_test.go:TestTransferCapsuleJournalRejectsRetainedForgery":                        "exerciseCapsuleJournalForgery",
	"internal/xferserve/settlement_lifetime_test.go:TestTransferSettlementReconnectJoinsRemoteLookup":                       "exerciseRemoteSettlementJoin",
	"internal/xferserve/settlement_owner_test.go:TestTransferSettlementOwnerRefusesIncompleteAuthority":                     "exerciseSettlementOwnerRefusal",
	"internal/xferserve/writer_fence_test.go:TestTransferWriterRetirementPreservesOriginalPendingFence":                     "exerciseOriginalWriterFence",
	"internal/xfertrack/reaped_test.go:TestTransferWatchdogReapedIdentityOutlivesItsRow":                                    "exerciseWatchdogReapedIdentity",
	"internal/xfertrack/terminal_continuation_test.go:TestTerminalCancellationAfterSyncedBytes":                             "exerciseTerminalCancellationAfterSyncedBytes",
	"internal/xfertrack/terminal_continuation_test.go:TestTerminalCleanupAfterTrackerImageCut":                              "exerciseTerminalCleanupAfterTrackerImageCut",
	"internal/xfertrack/terminal_continuation_test.go:TestTerminalSessionRemovalClosesReceiver":                             "exerciseTerminalSessionRemovalClosesReceiver",
	"internal/xfertrack/terminal_continuation_test.go:TestTransferAbandonmentSerializesLateFinishAcrossImageCuts":           "exerciseAbandonmentImageReplay",
	"internal/xfertrack/writer_witness_test.go:TestTransferWriterWitnessUsesCurrentSerializedRevision":                      "exerciseWriterWitnessRevision",
	"internal/xferworker/consumer_cleanup_test.go:TestWorkerImageRetainsDetachedConsumerCleanup":                            "exerciseWorkerConsumerCleanup",
	"internal/xferworker/snapshot_test.go:TestTransferWorkerSnapshotUsesJoinedActorFacts":                                   "exerciseJoinedActorSnapshot",
	"internal/xferworker/worker_byte_lifecycle_test.go:TestWorkerByteActorsResumeAndStopWithoutLeaks":                       "exerciseWorkerByteActors",
	"test/architecture/transfer_drill_contract_test.go:TestTransferReaderRecoveryRescansAndRejectsEvidenceDamage":           "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferCollectorAllowsDeclaredRecoverableServerOutage":          "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferControlBudgetIncludesRetriesAndTerminalOutcome":          "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferDrainRejectsReplayedAcknowledgement":                     "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferDrainRequiresFreshNonceWatermarkAndNoPoison":             "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferKeeperConsumesProducerOFDProtocol":                       "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferLaunchBootstrapActivatesRealGoObserver":                  "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferOFDErrorBranchesCloseEveryReceivedDescriptor":            "reviewTransferPython",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferOFDGoSendReachesPythonCollector":                         "exerciseTransferOFDGoSend",
	"test/architecture/transfer_drill_evidence_test.go:TestTransferSnapshotProvidesSchedulerSample":                         "reviewTransferPython",
	"test/cli_e2e/transfer_continuation_test.go:TestTransferBinaryContinuationKeepsOriginalInvocation":                      "exerciseTransferBinaryContinuation",
	"test/cli_e2e/transfer_default_graph_test.go:TestTransferCapablePushPullThroughDefaultGraph":                            "exerciseTransferDefaultGraph",
	"test/cli_e2e/transfer_legacy_test.go:TestTransferBinaryLegacyInteroperability":                                         "exerciseTransferBinaryLegacy",
	"test/cli_e2e/transfer_legacy_test.go:TestTransferBinaryProbeSilenceNeverSelectsLegacy":                                 "exerciseTransferProbeSilence",
	"test/cli_e2e/transfer_revoke_test.go:TestTransferRevokeAndCancelVictimsAreIndependent":                                 "exerciseTransferVictims",
	"test/cli_e2e/transfer_revoke_test.go:TestTransferSessionRemovalRequiresExplicitAbandon":                                "exerciseTransferExplicitAbandon",
	"test/cli_e2e/transfer_serve_image_test.go:TestTransferServeAndAgentImagesKeepCapablePushPull":                          "exerciseTransferServeImages",
	"test/cli_e2e/transfer_status_test.go:TestTransferBinaryRateBudgetRejectsBeforeCreate":                                  "exerciseTransferBinaryRateBudget",
	"test/cli_e2e/transfer_status_test.go:TestTransferBinaryStatusIsReadOnly":                                               "exerciseTransferBinaryStatus",
	"test/cli_e2e/transfer_status_test.go:TestTransferBinaryStatusRejectsInvalidScopeAndRate":                               "exerciseTransferBinaryValidation",
	"test/fullstackharness/cmd/supervise/main_linux_test.go:TestSupervisorArtifactFailurePreventsLaunch":                    "exerciseSupervisorArtifactFailure",
	"test/fullstackharness/cmd/supervise/main_linux_test.go:TestSupervisorLogAndReceiptPreserveChildContract":               "exerciseSupervisorEvidence",
	"test/fullstackharness/cmd/supervise/main_linux_test.go:TestSupervisorReceiptIncludesDescendantCleanup":                 "exerciseSupervisorDescendant",
	"test/fullstackharness/driver_test.go:TestAssemblyExecutableRequiresCurrentSource":                                      "exerciseAssemblyExecutable",
	"test/fullstackharness/driver_test.go:TestFullstackDriverNeverRepairsProductState":                                      "exerciseProductDriver",
	"test/fullstackharness/ofd_handoff_linux_test.go:TestOFDWitnessFollowsPreparedExec":                                     "exerciseOFDPreparedExec",
	"test/fullstackharness/ofd_migration_linux_test.go:TestOFDWitnessAcceptsDescriptorMigrationAcrossCut":                   "exerciseOFDDescriptorMigration",
	"test/fullstackharness/ofd_migration_linux_test.go:TestOFDWitnessRejectsUnprovenDescriptorMigration":                    "exerciseUnprovenOFDTransition",
	"test/fullstackharness/process_status_linux_test.go:TestSupervisorFailureOutranksTargetStatus":                          "exerciseSupervisorStatusPrecedence",
	"test/fullstackharness/process_status_linux_test.go:TestSupervisorPreservesCleanupFailureWithFailedTarget":              "exerciseSupervisorFailedTarget",
	"test/fullstackharness/process_status_linux_test.go:TestSupervisorReportsAdoptedZombieAsLeak":                           "exerciseSupervisorZombie",
	"test/hotupdate/artifact_provenance_test.go:TestArtifactStageRefusesFailedCurrentBuild":                                 "exerciseArtifactFailedBuild",
	"test/hotupdate/assembly_authenticity_test.go:TestAssemblyRejectsUnboundExecutables":                                    "exerciseUnboundAssembly",
	"test/hotupdate/assembly_authenticity_test.go:TestCustodyRejectionClosesNonSocketFD":                                    "exerciseCustodyRejection",
	"test/hotupdate/assembly_authenticity_test.go:TestObserverStoppedSupervisorStillReaps":                                  "exerciseStoppedObserver",
	"test/hotupdate/assembly_authenticity_test.go:TestObserverTimeoutStillJoinsEveryChild":                                  "exerciseObserverTimeout",
	"test/hotupdate/authority_recreation_test.go:TestAuthorityCompareThenRecreateRejectsWrite":                              "exerciseAuthorityPredicate",
	"test/hotupdate/authority_recreation_test.go:TestAuthorityParallelFlowTransferTermination":                              "exerciseAuthorityParallelTermination",
	"test/hotupdate/authority_recreation_test.go:TestLateCleanupCannotDeleteNewIncarnation":                                 "exerciseLateAuthorityCleanup",
	"test/hotupdate/compatibility_readers_test.go:TestUpgradeConfigurationParsersCurrentAndPrevious":                        "exerciseUpgradeConfigurationParsers",
	"test/hotupdate/compatibility_readers_test.go:TestUpgradeGoCheckpointDecoderContinuation":                               "exerciseUpgradeGoCheckpoint",
	"test/hotupdate/compatibility_readers_test.go:TestUpgradeWireEightQuadrants":                                            "exerciseUpgradeWireQuadrants",
	"test/hotupdate/fullstack_flow_exec_test.go:TestFullstackFlowCarrierKeepsOriginalDuplex":                                "exerciseFlowCarrierLifecycle",
	"test/hotupdate/fullstack_flow_exec_test.go:TestFullstackOriginalFlowsUseEveryInfraCut":                                 "exerciseOriginalFlowCut",
	"test/hotupdate/fullstack_flow_supply_test.go:TestFullstackFlowSupplyOutlivesSchedule":                                  "exerciseFlowSupply",
	"test/hotupdate/fullstack_incarnation_test.go:TestFullstackIncarnationRejectsLateCleanup":                               "exerciseIncarnationCut",
	"test/hotupdate/fullstack_ownership_test.go:TestFullstackOwnershipIntervalsAreExclusive":                                "exerciseOwnershipCut",
	"test/hotupdate/fullstack_pty_exec_test.go:TestFullstackPTYPendingControlAcrossLayers":                                  "exercisePTYPendingControl",
	"test/hotupdate/fullstack_pty_exec_test.go:TestFullstackPTYZombieTailAndSessionCleanup":                                 "exercisePTYRetirement",
	"test/hotupdate/fullstack_resource_test.go:TestFiveGenerationResourceBaseline":                                          "exerciseGenerationResourceCycle",
	"test/hotupdate/fullstack_rollout_test.go:TestMixedWorkloadSurvivesFullStackRollout":                                    "exerciseMixedRollout",
	"test/hotupdate/fullstack_soak_test.go:TestFullstackSoakRejectsOneLeakPerCycle":                                         "exerciseFixedImageCycle",
	"test/hotupdate/fullstack_ss_exec_test.go:TestFullstackSSPrimitiveLedgerUsesActualProduct":                              "exerciseFullstackSSPrimitive",
	"test/hotupdate/fullstack_ss_exec_test.go:TestFullstackSSStateAcrossInfraRollback":                                      "exerciseSSCut",
	"test/hotupdate/fullstack_task_exec_test.go:TestFullstackTasksRetainCurrentRollbackState":                               "exerciseTaskRollback",
	"test/hotupdate/fullstack_transaction_test.go:TestFullstackRollbackRequiresFreshCurrentProof":                           "exerciseCurrentRollback",
	"test/hotupdate/fullstack_transfer_exec_test.go:TestFullstackTransferCohortsRespectOriginalBudget":                      "exerciseTransferCohort",
	"test/hotupdate/fullstack_transfer_exec_test.go:TestFullstackTransferSealsLateTerminal":                                 "exerciseTransferEndSeal",
	"test/hotupdate/legacy_admission_test.go:TestLegacyUpgradeWireRequiresQuiescenceFence":                                  "exerciseLegacyUpgradeWireFence",
	"test/hotupdate/legacy_policy_cli_test.go:TestLegacyUpgradePolicyWithOldCLI":                                            "exerciseLegacyPolicyCLI",
	"test/hotupdate/legacy_transaction_test.go:TestLegacyIssuerRouteChange":                                                 "exerciseLegacyIssuerRoute",
	"test/hotupdate/legacy_transaction_test.go:TestUpgradeWireProbeUsesOriginalCTLIdentity":                                 "exerciseUpgradeWireProbe",
	"test/hotupdate/transfer_broker_test.go:TestTransferBrokerExecKeepsTrackerBeforeReaper":                                 "exerciseTransferBrokerExec",
	"test/hotupdate/transfer_broker_test.go:TestTransferSettlementBoundariesPrecedeAcknowledgement":                         "exerciseTransferSettlementBoundaries",
	"test/hotupdate/transfer_exec_test.go:TestTransferAgentExecAcrossEveryBoundary":                                         "exerciseTransferAgentExec",
	"test/hotupdate/transfer_legacy_test.go:TestTransferBinaryHarnessNoLeaks":                                               "exerciseTransferBinaryHarness",
	"test/hotupdate/transfer_legacy_test.go:TestTransferDestinationObserverRejectsTransientPartialWrite":                    "exerciseTransferDestinationObserver",
	"test/hotupdate/transfer_object_test.go:TestTransferObjectSDKReadDoesNotWaitForRawCensus":                               "exerciseTransferObjectSDKRead",
	"test/hotupdate/transfer_observation_test.go:TestTransferBrokerImageInstallationEmitsCustody":                           "exerciseTransferBrokerImageCustody",
	"test/hotupdate/transfer_observation_test.go:TestTransferProductionOwnersEmitCustody":                                   "exerciseTransferProductionCustody",
	"test/hotupdate/transfer_observation_test.go:TestTransferRelayWaitsForProductionSubscriptions":                          "exerciseTransferRelayReadiness",
	"test/hotupdate/transfer_pull_selection_test.go:TestTransferSmallPullUsesInlineAndExactTerminal":                        "exerciseSmallPullRoute",
	"test/hotupdate/transfer_restart_test.go:TestTransferCarrierAndJSRestartReceiptsAreDistinct":                            "exerciseTransferRestart",
	"test/hotupdate/transfer_rollback_test.go:TestTransferPartialActiveRollbackKeepsCurrentOwners":                          "exerciseTransferPartialRollback",
	"test/hotupdate/transfer_rollback_test.go:TestTransferWatchdogRollbackKeepsOriginalDeadline":                            "exerciseTransferWatchdog",
	"test/infrabusiness/business_oracle_test.go:TestBusinessOracleAcceptsDistinctServersWithEqualCID":                       "exerciseDistinctServersWithEqualCID",
	"test/infrabusiness/continuity_test.go:TestInfraBusinessLifecycle":                                                      "exerciseBusinessLifecycle",
	"test/infracaddy/binary_test.go:TestInfraCaddyLifecycle":                                                                "exerciseCaddyLifecycle",
	"test/infraprivilege/authority_test.go:TestInfraPrivilegeCanaryBoundary":                                                "exercisePrivilegeLifecycle",
	"test/infraprivilege/authority_test.go:TestInfraPrivilegeLifecycle":                                                     "exercisePrivilegeLifecycle",
	"test/infraprivilege/support_test.go:TestInfraSupportingRuntimeCycle":                                                   "exerciseSupportingRuntimeCycle",
	"test/p10/installer_boundary_test.go:TestNATSConfigWriteRejectsPostValidationSymlink":                                   "exerciseNATSConfigWrite",
	"test/transferharness/supervision_linux_test.go:TestTransferHarnessStopJoinsLiveProcessTree":                            "exerciseTransferProcessTree",
	"test/transferharness/supervision_test.go:TestTransferHarnessSupervisionControls":                                       "exerciseTransferSupervisionControl",

	// origin: hot-update S8 plan §11.2 P23; S9 plan §12 P20/P28/P40; S10 plan §10.2 P08.
	// Each registered subject has five direct exercises and both shared leak assertions.
	"cmd/tether-infra-helper/main_test.go:TestInfraRootLifecycle":                                 "exerciseRoot",
	"internal/infraroot/bind_test.go:TestInfraRootLifecycle":                                      "exerciseRoot",
	"internal/infraupgrade/transaction_test.go:TestInfraImageAndSupportDomainsExcludeOverlap":     "exerciseSupportDomain",
	"internal/storewitness/witness_test.go:TestStoreWitnessContenderRecordsTransientUnlock":       "exerciseStoreWitnessContender",
	"internal/storewitness/witness_test.go:TestStoreWitnessRejectsInvalidCustody":                 "exerciseStoreWitnessCustody",
	"internal/upgradelock/domain_test.go:TestDomainRegistersInfrastructureParticipants":           "exerciseInfrastructureDomain",
	"internal/upgradeobserve/registry_test.go:TestInfrastructureRegistryKeepsComponentIdentity":   "exerciseInfrastructureRegistry",
	"internal/upgradestore/store_test.go:TestJournalRegistersInfrastructureComponents":            "exerciseInfrastructureJournal",
	"test/cli_e2e/upgrade_transaction_test.go:TestUpgradeTransactionCLIUsesFreshCurrentProof":     "exerciseUpgradeTransactionCLI",
	"test/cli_e2e/upgrade_unknown_test.go:TestUpgradeUnknownQueryAndWaitHaveDistinctExitCodes":    "exerciseUpgradeUnknownCLI",
	"test/hotupdate/fullstack_store_witness_test.go:TestFullstackStoreWitnessPrecedesReplay":      "exerciseStoreWitness",
	"test/hotupdate/transaction_driver_test.go:TestDriverResumesOriginalPlan":                     "exerciseTransactionDriverResume",
	"test/hotupdate/transaction_driver_test.go:TestLateWriterCannotClaimNextTransaction":          "exerciseTransactionLateWriter",
	"test/hotupdate/transaction_lease_test.go:TestExecPreservesDomainLeaseThroughCommit":          "exerciseTransactionLeaseExec",
	"test/hotupdate/transaction_readonly_test.go:TestReadOnlyRollbackColdBootUpgrade":             "exerciseTransactionReadonly",
	"test/hotupdate/transaction_rollback_test.go:TestRollbackExportsCurrentStateNotStaleSnapshot": "exerciseTransactionCurrentRollback",
	"test/infranats/embedded_test.go:TestInfraNATSLifecycle":                                      "exerciseNATSLifecycle",
	// origin: hot-update S7 plan §11.2 / S7-GATES merged adapter lifecycle receipt
	"internal/infratransfer/adapter_test.go:TestInfraTransferLifecycle":                                  "exerciseTransfer",
	"internal/testharness/process_supervision_linux_test.go:TestProcessSignalRequiresRecordedStartTicks": "exerciseProcessSignalIdentity",
	// origin: hot-update S6 plan §9.3 / FIX-ORPHAN; each fatal exit owns five real process trees.
	"internal/testharness/process_supervision_linux_test.go:TestProcessSupervisionReapsAfterPanic":         "exerciseProcessSupervision",
	"internal/testharness/process_supervision_linux_test.go:TestProcessSupervisionReapsAfterTimeout":       "exerciseProcessSupervision",
	"internal/testharness/process_supervision_linux_test.go:TestProcessSupervisionReapsAfterSIGKILL":       "exerciseProcessSupervision",
	"internal/testharness/process_supervision_linux_test.go:TestProcessSupervisionReapsAfterSamePIDExec":   "exerciseProcessSupervision",
	"internal/testharness/process_supervision_linux_test.go:TestProcessSupervisionPreservesCommandResults": "exerciseSupervisedCommand",
	// origin: hot-update S6 plan §9.3 / T54; live five-round subject calls.
	"cmd/tether/ctl_attempt_receipt_test.go:TestCtlAdmissionLeavesTimeForFinalReceipt":                        "exerciseCtlResultReserve",
	"internal/agent/instance_id_acceptor_test.go:TestClaimProbeRefreshesActualAgentHeartbeat":                 "exerciseClaimHeartbeat",
	"internal/broker/broker_image_authority_test.go:TestImageBusinessWakesEveryAgentWithoutInventingLiveness": "exerciseImageAgentWake",
	"internal/cluster/broker_image_test.go:TestImageSoleVoterCampaignKeepsProductionTimers":                   "exerciseImageSoleVoterCampaign",
	"internal/tunnel/transport/register_test.go:TestRegisterReaderRetainsSocketOwnership":                     "exerciseRegisterReaderSocket",

	"internal/clusterupgrade/broker_scope_test.go:TestBrokerPlanLeakCycles": "exerciseBrokerPlan",
	// origin: hot-update S6 plan §9.3 / T54; integrated owners, five-round call expressions.
	"cmd/tether/broker_preflight_store_test.go:TestBrokerPreflightClusterProbeNeverOpensRaftStore":                           "exerciseLiveStoreReaderProbe",
	"cmd/tether/ctl_attempt_receipt_test.go:TestCtlBinaryAttemptReceiptMatchesCollector":                                     "exerciseCtlBinaryReceipt",
	"cmd/tether/ctl_attempt_receipt_test.go:TestNodeLsAttemptReceiptMatchesBrokerCollector":                                  "exerciseNodeLsReceipt",
	"cmd/tether/serve_cold_store_compat_test.go:TestServeColdSingleMemoryStoreReachesRun":                                    "exerciseServeMemoryStore",
	"cmd/tether/serve_cold_store_compat_test.go:TestServeColdSingleStartToleratesNewerMigrationLedger":                       "exerciseServeNewerMigrationLedger",
	"internal/agent/maintenance_hold_test.go:TestAgentMaintenanceUsesSharedBrokermaintCodec":                                 "exerciseMaintenanceSharedCodec",
	"internal/agent/maintenance_hold_test.go:TestMaintenanceHoldSurvivesUnchangedProxyKeyNudge":                              "exerciseMaintenanceNudgeAndRevoke",
	"internal/broker/broker_image_authority_test.go:TestBrokerAuthFirstWaitsForLocalProofAfterPeerConnect":                   "exercisePeerConnectBeforeLocalProof",
	"test/hotupdate/broker_run_test.go:TestBrokerActiveImportFailureUsesCurrentCut":                                          "exerciseBrokerActiveImportFailure",
	"test/hotupdate/broker_run_test.go:TestBrokerExecStaysInsideRun":                                                         "exerciseBrokerExecInsideRun",
	"internal/broker/broker_image_run_test.go:TestBrokerImageRecordsCarryCutAndProducerClock":                                "exerciseBrokerImageRecords",
	"test/hotupdate/broker_run_test.go:TestBrokerRollbackReaderRemainsBoundToIncomingProof":                                  "exerciseBrokerRollbackReaderBinding",
	"internal/broker/broker_image_run_test.go:TestBrokerStopDuringCutLeavesFDsWithCurrentOwner":                              "exerciseBrokerStopDuringCut",
	"internal/broker/home_mutation_wiring_test.go:TestClusterHomeMutatorsCommitWithProductionWiring":                         "exerciseProductionHomeMutators",
	"internal/broker/home_mutation_wiring_test.go:TestCrashedMaintenanceRecordDoesNotPinHomeForever":                         "exerciseCrashedMaintenanceReap",
	"internal/broker/home_mutation_wiring_test.go:TestEndedMaintenanceReturnStaysPlannedAfterExpiry":                         "exerciseEndedMaintenanceReturn",
	"internal/broker/home_mutation_wiring_test.go:TestMaintainedRebalanceDoesNotChangeTheVoterPlan":                          "exerciseMaintainedRebalance",
	"internal/broker/home_mutation_wiring_test.go:TestMaintenanceDrainGuardsBothHomes":                                       "exerciseMaintainedDrain",
	"internal/broker/maintenance_protocol_test.go:TestMaintenanceCutRequiresEveryInstalledObserver":                          "exerciseInstalledMaintenanceObservers",
	"internal/broker/maintenance_protocol_test.go:TestMaintenanceProofWaitsForLocalCommandCursor":                            "exerciseMaintenanceAppliedCursor",
	"internal/broker/reexec_test.go:TestReloadAdmissionRejectsChangedMembership":                                             "exerciseReloadAdmissionMembership",
	"internal/broker/reexec_test.go:TestSoleVoterAdmissionDoesNotPollRemoteCapabilities":                                     "exerciseSoleVoterAdmissionPoll",
	"internal/brokercontrol/loopset_test.go:TestLoopSetConcurrentGoIsRaceFree":                                               "exerciseConcurrentLoops",
	"internal/brokerimage/boot_test.go:TestBrokerPureProbeProofRequiresCapabilities":                                         "exerciseBrokerPureProbeCapabilities",
	"internal/brokerimage/cluster_store_refusal_test.go:TestClusterStoreRefusedFreezeRestoresWriteAdmission":                 "exerciseClusterStoreRefusal",
	"internal/brokerimage/cut_private_owner_test.go:TestBrokerCutPrivateStateHasOneCloser":                                   "exerciseCutPrivateOwner",
	"internal/brokerimage/cut_receiver_validation_test.go:TestBrokerPreparedBundleDecodesAtReceiver":                         "exercisePreparedReceiverBundle",
	"internal/brokerimage/cut_test.go:TestBrokerFinalProjectionUsesCurrentResourceIdentities":                                "exerciseBrokerFinalProjection",
	"internal/brokerimage/cut_test.go:TestBrokerFinalizerRefusalRetainsPrivateCloser":                                        "exerciseBrokerFinalizerRefusal",
	"internal/brokerimage/cut_test.go:TestBrokerRuntimeBindingSurvivesCutDecoration":                                         "exerciseBrokerDecoratedCut",
	"internal/brokerimage/import_tunnel_source_test.go:TestBrokerTunnelPristineRepackRoundTrip":                              "exerciseBrokerTunnelRepackProcess",
	"internal/brokerimage/single_store_cold_test.go:TestColdSinglePartialOwnerRetainsCanonicalIdentity":                      "exercisePartialColdSingleOwner",
	"internal/brokerimage/single_store_cold_test.go:TestColdSingleStoreKeepsImageIneligibleDatabasesUsable":                  "exerciseColdSingleCompatibility",
	"internal/brokerimage/single_store_resume_test.go:TestSingleStoreRefusedFreezeRestoresWriteAdmission":                    "exerciseSingleStoreRefusal",
	"internal/brokerimage/store_reload_admission_test.go:TestBrokerReloadBusyTransferDefersBeforeRPCDeadline":                "exerciseBrokerBusyTransfer",
	"internal/brokerimage/store_reload_admission_test.go:TestBrokerReloadFreezeStartsWithFreshDeadline":                      "exerciseBrokerFreshCutDeadline",
	"internal/brokerimage/store_reload_recovery_test.go:TestReloadAdmissionRefusalPrecedesStoreClosing":                      "exerciseReloadAdmissionRefusal",
	"internal/brokerimage/store_reload_recovery_test.go:TestReloadCloseFailureIsNotTreatedAsOpenStore":                       "exerciseReloadCloseFailure",
	"internal/brokerimage/store_reload_recovery_test.go:TestReloadClusterShutdownFailureKeepsActualClosePhases":              "exerciseReloadClusterCloseReceipt",
	"internal/cli/cluster_roll_test.go:TestBrokerRollHealthWaitsForExpectedVoters":                                           "exerciseDelayedBrokerHealth",
	"internal/cli/cluster_roll_test.go:TestLocalReloadUsesComponentBudgetAndKeepsUnknownResult":                              "exerciseLocalReloadCompletion",
	"internal/cli/natsconn_test.go:TestConnectWithinKeepsEarlyPingRetryable":                                                 "exerciseConnectEarlyPing",
	"internal/cluster/broker_image_test.go:TestImageShutdownResamplesMultiVoterLeadership":                                   "exerciseChangedShutdownMembership",
	"internal/cluster/snapshot_floor_recovery_test.go:TestRecoverSingleNodeAcceptsOlderLocalSnapshot":                        "exerciseOfflineSnapshotRecovery",
	"internal/cluster/snapshot_floor_recovery_test.go:TestRecoverToSelfOnlineAcceptsOlderLocalSnapshot":                      "exerciseOnlineSnapshotRecovery",
	"internal/cluster/snapshot_install_transport_test.go:TestFollowerInstallsCompactedLeaderSnapshotOverProductionTransport": "exerciseProductionSnapshotCatchup",
	"internal/cluster/snapshot_install_transport_test.go:TestRuntimeSnapshotFloorReadsCommittedConfiguration":                "exerciseCommittedSnapshotConfiguration",
	"internal/execclient/admission_test.go:TestAdmissionLegacyProofRequiresExplicitOldRelease":                               "exerciseLegacyVersionProof",
	"internal/flow/image_test.go:TestFlowRestoredDirectionIsPristineOnly":                                                    "exerciseFlowRestoredDirection",
	"internal/flow/manager_test.go:TestFlowCloseJoinsRetirementCallback":                                                     "exerciseFlowCloseRetirementJoin",
	"internal/tunnel/client_lifecycle_test.go:TestClientStopWaitsForRootCleanup":                                             "exerciseClientStopReceipt",
	"internal/tunnel/server_image_test.go:TestBrokerImageRevokeRequiresJoinedSafePoint":                                      "exerciseBrokerImageRevokeSafePoint",
	"internal/tunnel/server_image_test.go:TestBrokerImportedRevokeSurvivesRejectedRegistration":                              "exerciseBrokerImportedRevoke",
	"internal/tunnel/server_import_listener_owner_test.go:TestImportedServerActivationCannotRestartClosedOwner":              "exerciseImportedServerActivationStop",
	"test/cli_e2e/broker_control_test.go:TestBrokerControlIntegrationLeakCycles":                                             "exerciseRealCalloutCut",
	"test/clusterharness/broker_image_test.go:TestBrokerStoreFixtureConstructorFailureRetainsLock":                           "exerciseStoreConstructorRefusal",
	"test/clusterharness/broker_image_test.go:TestBrokerStoreFixtureLeakCycles":                                              "exerciseStoreExecRollback",
	"test/clusterharness/broker_image_test.go:TestBrokerStoreFixtureRejectsUnobservedClaims":                                 "exerciseStoreFixtureUnobservedClaims",
	"test/clusterharness/broker_maintenance_test.go:TestBrokerN3MaintenancePreservesHomeAcrossExec":                          "exerciseN3MaintenanceExec",
	"test/clusterharness/broker_product_cohort_test.go:TestBrokerCohortReservesFutureListenerAddresses":                      "exerciseBrokerFutureAddresses",
	"test/clusterharness/broker_product_cohort_test.go:TestBrokerCohortSocketCensusUsesFullTuple":                            "exerciseBrokerSocketTuple",
	"test/clusterharness/broker_product_test.go:TestBrokerProductFixtureCloseJoinsResources":                                 "exerciseBrokerProductFixtureClose",
	"test/clusterharness/broker_product_test.go:TestBrokerObservationBuildRejectsChangedInputs":                              "exerciseBrokerObservationBuildInputs",
	"test/hotupdate/broker_lifecycle_seam_test.go:TestBrokerLifecycleKernelOracleCycles":                                     "exerciseBrokerSeamKernelOracle",
	"test/hotupdate/broker_lifecycle_seam_test.go:TestBrokerPreparedListenerRevokeCannotResurrectEndpoint":                   "exerciseBrokerPreparedRevoke",
	"test/hotupdate/broker_lifecycle_seam_test.go:TestCurrentRuntimeStopAfterExecError":                                      "exerciseBrokerExecErrorStop",
	"test/hotupdate/broker_rollback_test.go:TestBrokerActiveRollbackKeepsNewACK":                                             "exerciseActiveRollbackStop",
	"test/hotupdate/broker_rollback_test.go:TestBrokerCurrentRollbackAndNormalStop":                                          "exerciseActiveRollbackStop",
	"test/hotupdate/broker_rollback_test.go:TestBrokerLifecycleSeamLeakCycles":                                               "exerciseActiveRollbackStop",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerACKBeforeReplay":                                                     "exerciseBrokerACKBeforeReplay",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerAppliedIndexMonotonic":                                               "exerciseBrokerAppliedIndex",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerJSCurrentWitness":                                                    "exerciseBrokerJSWitness",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerJSWitnessRejectsSameNameRecreation":                                  "exerciseBrokerJSRecreation",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerLockHeldAcrossExec":                                                  "exerciseBrokerStoreLockExec",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerPreflightNeverOpensStore":                                            "exerciseBrokerStorePreflight",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerShutdownReceiptOrder":                                                "exerciseBrokerShutdownOrder",
	"test/hotupdate/broker_store_exec_test.go:TestBrokerStoreLockProbePositiveControl":                                       "exerciseBrokerStoreLockProbeControl",
	"test/hotupdate/broker_store_exec_test.go:TestRaftSnapshotRPCJoinWithinCut":                                              "exerciseBrokerSnapshotRPCJoin",
	"test/hotupdate/broker_store_exec_test.go:TestSoleVoterReloadWithinBudget":                                               "exerciseSoleVoterProductReload",
	"test/p13/broker_exec_test.go:TestBrokerExecOriginalFlowCohort":                                                          "exerciseBrokerOriginalCohort",
	"test/p13/broker_exec_test.go:TestBrokerFlowCohortLeakCycles":                                                            "exerciseBrokerOriginalCohort",

	// origin: S8 internal review SIGNED-ROLL-FIXTURES F1–F4; direct five-round exercises.
	"cmd/tether/cluster_lock_keeper_test.go:TestDriveUpgradeActuallyStartsAKeeper":                    "exerciseSignedRollKeeper",
	"cmd/tether/cluster_upgrade_agentless_test.go:TestAgentlessClusterCompletesAWholeRoll":            "exerciseSignedAgentlessRoll",
	"cmd/tether/cluster_upgrade_agentless_test.go:TestARealAgentStillGetsReExecdOnAMixedCluster":      "exerciseSignedMixedRoll",
	"cmd/tether/cluster_upgrade_agentless_test.go:TestAgentlessDriveIsGovernedByThePlanNotALiveProbe": "exerciseSignedPlanGovernedRoll",

	// origin: hot-update S6 plan §9.3 / T54 — verified live >=5-round call expressions.
	"cmd/tether/cluster_upgrade_scope_test.go:TestBrokerScopeAndTransfersDeferred":                  "exerciseBrokerScopeAndTransfers",
	"cmd/tether/cluster_upgrade_scope_test.go:TestBrokerDaemonRollLeavesAgentUntouched":             "exerciseBrokerDaemonRoll",
	"cmd/tether/ctl_budget_test.go:TestControlResultUnknownNoReplay":                                "exerciseControlResultUnknown",
	"cmd/tether/ctl_budget_test.go:TestBrokerCommandLeakCycles":                                     "exerciseCtlAdmissionCancel",
	"cmd/tether/serve_test.go:TestServeSingleStoreCleanupOnRunFailure":                              "exerciseServeSingleStoreFailure",
	"internal/adminsock/image_test.go:TestBrokerAdminLeakCycles":                                    "exerciseAdminInheritedClose",
	"internal/agent/broker_maintenance_test.go:TestBrokerAgentHoldLeakCycles":                       "exerciseAgentMaintenanceHold",
	"internal/agentboot/broker_role_test.go:TestBrokerBootLeakCycles":                               "exerciseBrokerRoleRedispatch",
	"internal/broker/broker_image_authority_test.go:TestBrokerAuthorityLeakCycles":                  "exerciseAuthorityRebind",
	"internal/broker/broker_image_maintenance_test.go:TestMaintenanceEveryHomeMutator":              "exerciseHomeMutationGuard",
	"internal/broker/broker_image_maintenance_test.go:TestMaintenancePreservesDwellAndReturn":       "exerciseHomeMaintenanceHistory",
	"internal/broker/broker_image_maintenance_test.go:TestBrokerHomeLeakCycles":                     "exerciseMaintainedHomeTick",
	"internal/broker/broker_image_maintenance_test.go:TestMaintenanceHomeGuardedPlannerIntegration": "exerciseGuardedHomePlanner",
	"internal/broker/broker_image_run_test.go:TestBrokerRunLeakCycles":                              "exerciseBrokerExecReturned",
	"internal/broker/broker_image_run_test.go:TestBrokerTransferFenceIncludesProvisionAndLedger":    "exerciseBrokerTransferFence",
	"internal/broker/broker_image_run_test.go:TestBrokerLoopJoinRequiresActualExit":                 "exerciseBrokerLoopJoin",
	"internal/broker/broker_image_run_test.go:TestBrokerCurrentOwnerSkipsColdCleanup":               "exerciseBrokerColdCleanup",
	"internal/broker/broker_image_run_test.go:TestBrokerReloadKeepsAbsoluteBudget":                  "exerciseBrokerReloadBudget",
	"internal/broker/broker_image_run_test.go:TestBrokerRestoredStoreSkipsColdGeneration":           "exerciseBrokerRestoredRefusal",
	"internal/broker/reexec_test.go:TestReloadRefusesOnLeader":                                      "exerciseReloadMultipleVoters",
	"internal/broker/reexec_test.go:TestReloadAllowsSoleVoter":                                      "exerciseReloadSoleVoter",
	"internal/broker/upgrade_holder_test.go:TestBrokerHolderLeakCycles":                             "exerciseHolderLostLease",
	"internal/brokerimage/boot_finish_test.go:TestBrokerBootFinishRefusalKeepsActiveOwner":          "exerciseBrokerBootFinishRefusal",
	"internal/brokerimage/boot_test.go:TestBrokerActualFDWhitelist":                                 "exerciseBrokerFDWhitelist",
	"internal/brokerimage/cut_test.go:TestBrokerCutLeakCycles":                                      "exerciseCutFailure",
	"internal/brokerimage/import_test.go:TestBrokerImportLeakCycles":                                "exerciseBrokerPartialTake",
	"internal/brokerimage/runtime_test.go:TestBrokerCurrentRuntimeReopenStopSerializes":             "exerciseRuntimeReopenStop",
	"internal/brokermetrics/image_test.go:TestBrokerMetricsLeakCycles":                              "exerciseMetricsInheritedHTTP",
	"internal/cli/cluster_roll_test.go:TestBrokerRollLeakCycles":                                    "exerciseRollLeaderChanged",
	"internal/cli/retry_test.go:TestBrokerCLIRetryLeakCycles":                                       "exerciseCLIRetryCancel",
	"internal/cluster/broker_image_test.go:TestBrokerPartialCloseBlocksReopen":                      "exerciseNodeCloseUncertainty",
	"internal/cluster/broker_image_test.go:TestBrokerRoleObserverDetectsABA":                        "exerciseRoleObserverABA",
	"internal/cluster/broker_image_test.go:TestBrokerNodeLeakCycles":                                "exerciseNodePartialClose",
	"internal/cluster/broker_image_test.go:TestRaftSnapshotRPCJoinWithinCut":                        "exerciseSnapshotRPCJoin",
	"internal/cluster/broker_image_test.go:TestBrokerLockHeldAcrossExec":                            "exerciseLockExec",
	"internal/cluster/broker_image_test.go:TestBrokerNodeCurrentStoreCut":                           "exerciseNodeCurrentStoreCut",
	"internal/cluster/snapshot_image_test.go:TestInstallSnapshotCurrentFloor":                       "exerciseSnapshotCurrentFloor",
	"internal/cluster/snapshot_image_test.go:TestBrokerRestartSkipsStartupSnapshot":                 "exerciseSnapshotRestart",
	"internal/cluster/snapshot_image_test.go:TestBrokerSnapshotLeakCycles":                          "exerciseSnapshotCancel",
	"internal/cluster/snapshot_image_test.go:TestBrokerSnapshotStageControls":                       "exerciseSnapshotStageControls",
	"internal/clusterio/transport_test.go:TestBrokerTransportLeakCycles":                            "exerciseStalledRPC",
	"internal/clusterio/transport_test.go:TestBrokerTransportUnknownCloseAndReaderJoin":             "exerciseTransportUnknownClose",
	"internal/clustermanifest/image_test.go:TestBrokerManifestLeakCycles":                           "exerciseManifestInheritedHTTP",
	"internal/execclient/admission_test.go:TestAdmissionProbeTimeoutDoesNotDowngrade":               "exerciseProbeBlackout",
	"internal/execclient/admission_test.go:TestRetryStableLogicalIdentity":                          "exerciseStableAdmission",
	"internal/execclient/admission_test.go:TestBrokerExecClientLeakCycles":                          "exerciseAdmissionBlackout",
	"internal/httplisten/image_test.go:TestBrokerHTTPLeakCycles":                                    "exerciseHTTPKeepaliveFreeze",
	"internal/listenerimage/keeper_test.go:TestBrokerListenerLeakCycles":                            "exerciseKeeperPauseAdopt",
	"internal/natsinbox/budget_test.go:TestBrokerInboxLeakCycles":                                   "exerciseInboxFallbackCancel",
	"internal/proxydial/budget_test.go:TestBrokerProxyLeakCycles":                                   "exerciseProxyDeadlineCancel",
	"internal/storage/image_test.go:TestBrokerStorageLeakCycles":                                    "exerciseStorageCloseFailure",
	"internal/subhttp/image_test.go:TestBrokerSubHTTPLeakCycles":                                    "exerciseSubHTTPInheritedHTTP",
	"internal/tunnel/flow_endpoint_test.go:TestEndpointResumeWaitsForAdmissionBeforeFreeze":         "exerciseEndpointResumeAdmission",
	"internal/tunnel/server_image_test.go:TestBrokerServerImageLeakCycles":                          "exerciseServerPartialImport",
	"internal/tunnel/server_image_test.go:TestBrokerServerRuntimeImageRoundTrip":                    "exerciseBrokerServerRuntimeImage",
	"internal/tunnel/server_pause_test.go:TestEndpointPauseKeepsPendingFIFO":                        "exerciseEndpointPauseFIFO",
	"internal/tunnel/server_pause_test.go:TestEndpointTimerCallbackJoin":                            "exerciseEndpointTimerJoin",
	"internal/tunnel/server_pause_test.go:TestServerCarrierJoinBeforeRawFreeze":                     "exerciseServerCarrierJoin",
	"internal/tunnel/server_pause_test.go:TestBrokerServerPauseLeakCycles":                          "exerciseServerPauseFailure",
	// origin: hot-update S5 plan §8 / DRILL-RUN — committed revoke and first heartbeat.
	"internal/agent/proxy_lifetime_test.go:TestHeartbeatPublishesReapedCorpseStateOnItsFirstTick": "exerciseInitialHeartbeat",
	"internal/broker/events_test.go:TestClusterProxyRevokeEmitsKeysetChanged":                     "exerciseCommittedProxyRevoke",
	// origin: hot-update S6 plan §12.1 P00 / T54 — exact producer receipt.
	"internal/brokerimage/contractstub/stub_test.go:TestBrokerContractStubOwners": "exerciseBrokerContractStubOwners",
	// origin: hot-update S5 plan §8 / T49 / DRILL-OBSERVER
	"internal/agent/observation_test.go:TestAgentCustodyPollingJoinsRunCancellation":                     "exerciseAgentCustodyPolling",
	"internal/execsession/shared_budget_test.go:TestRuntimeObservationCountsLiveOwnedReservations":       "exerciseRuntimeObservation",
	"internal/flow/observation_linux_test.go:TestFlowObservationKeepsOriginalBindingAndCommittedOffsets": "exerciseFlowObservation",
	"internal/tunnel/observation_test.go:TestCarrierObservationUsesNegotiatedSessionAndOriginalPrefix":   "exerciseCarrierObservation",

	// origin: hot-update S5 plan §8 / T49 — register merged cut and carrier subjects.
	"internal/agent/proxy_freeze_test.go:TestHeartbeatKeepsFrozenProxyOwner":                        "exerciseFrozenProxyHeartbeat",
	"internal/agent/proxy_timer_image_test.go:TestProxyCutMatchesSealedSSAuthorityBeforeCallbacks":  "exerciseProxySealedAuthority",
	"internal/agent/ssproxy/private_state_test.go:TestSSCutOwnsAuthorityTimerAndMaterialMembership": "exerciseSSPrivateState",
	"internal/tunnel/flow_image_replay_test.go:TestRestoredClientCanReplayOriginalExpose":           "exerciseRestoredExposeReplay",
	"internal/tunnel/flow_private_state_test.go:TestClientCutKeepsTerminalReceiptDecisions":         "exerciseClientPrivateReceipts",
	"internal/tunnel/flow_reconnect_test.go:TestCarrierRecoveryWaitsForSupervisorPublication":       "exerciseSupervisorPublication",
	"internal/tunnel/flow_reconnect_test.go:TestConcurrentCarrierRecoverySharesOneAttachment":       "exerciseConcurrentCarrierRecovery",
	"internal/tunnel/flow_reconnect_test.go:TestExpiredCarrierRecoveryRetainsRefusal":               "exerciseExpiredCarrierRefusal",
	"internal/tunnel/flow_reconnect_test.go:TestRejectedRegisterKeepsDifferentAllocation":           "exerciseRegisterRefusalIsolation",
	"internal/tunnel/flow_revoke_test.go:TestFlowRevokeClosesPeerBeforeDataStreamRetires":           "exerciseFlowPeerRevoke",
	"internal/tunnel/flow_config_test.go:TestClientFlowConfigurationAfterStartKeepsLifetime":        "exerciseStartedClientFlowConfig",
	"internal/tunnel/flow_config_test.go:TestClientFlowConfigurationCannotOvertakeOpen":             "exerciseClientFlowConfigAdmission",
	"internal/tunnel/flow_restart_test.go:TestMissingFlowOwnerStillRequiresAuthorityAndSupervisor":  "exerciseMissingFlowOwner",
	"internal/tunnel/flow_restart_test.go:TestOrdinarySupervisorRecoversAfterFlowBrokerRestart":     "exerciseFlowBrokerRestart",
	"test/hotupdate/flow_exec_test.go:TestNodeUpgradeWaitObservesRealRegister":                      "exerciseUpgradeRegisterObserver",

	// origin: hot-update S5 plan §8 / §10.2 — merged revoke, legacy output and
	// liveness subjects each call this exercise inside their five-round loop.
	"internal/agent/ctl_liveness_test.go:TestCtlLivenessReaperHangsUpSilentRun":                                    "exerciseCtlLivenessSilentRun",
	"internal/agent/ssproxy/survivor_authority_test.go:TestSSConflictingCurrentKeysetHoldsSurvivorUntilFreshGrant": "exerciseSSConflictingCurrentKeyset",
	"internal/flow/pump_revoke_test.go:TestHardRevokeHoldsFinalControlUntilRawWorkersJoin":                         "exerciseHardRevokeControlBoundary",
	"internal/flow/pump_revoke_test.go:TestTerminalReceiptPublishesOnlyReleasedReplay":                             "exerciseTerminalReplayPublication",
	"internal/pty/legacy_output_test.go:TestLegacyOutputFlushesAfterIdleWithoutClosingSource":                      "exerciseLegacyOutputAfterIdle",
	"internal/tunnel/flow_revoke_test.go:TestHardRevokeClosesPublicFlowWithoutTailReceipt":                         "exerciseHardRevokePublicFlow",
	"internal/tunnel/flow_revoke_test.go:TestHardRevokeRetainedReceiptStillSubmitsPublicClose":                     "exerciseRetainedRevokeSubmission",

	// origin: hot-update S5 plan §8 / T49 — audited integrated fix subjects.
	"internal/agent/proxy_authority_cut_test.go:TestRestoredFreshAuthorityDominatesDelayedKeysetPush":        "exerciseRestoredFreshAuthority",
	"internal/agent/proxy_readiness_test.go:TestPlannedProxyCutKeepsLastReadinessWithinDetachedGrace":        "exercisePlannedProxyReadiness",
	"internal/agent/ssproxy/image_exec_test.go:TestSSPristineDetachPreservesOriginalClosure":                 "exerciseSSPristineDetach",
	"internal/agent/ssproxy/survivor_authority_test.go:TestFreshGenerationAllowsRestoredLowerEpoch":          "exerciseSSGenerationRecovery",
	"internal/agent/ssproxy/survivor_authority_test.go:TestSSMaterialQuotaFreshGrantKeepsSurvivorForwarding": "exerciseSSQuotaSurvivor",
	"internal/agent/ssproxy/survivor_authority_test.go:TestSSOlderAuthorityCannotSuspendCurrentSurvivor":     "exerciseSSStaleAuthority",
	"internal/agent/ssproxy/upstream_restore_test.go:TestSSConsumedRefusalSocketImageKeepsItsReceipt":        "exerciseSSRefusalRestore",
	"internal/agent/ssproxy/upstream_restore_test.go:TestSSOriginalRawUpstreamTupleRestoresOnEveryRoute":     "exerciseSSUpstreamRestore",
	"internal/agent/upgrade_image_reader_test.go:TestFinalPrepareRejectsFlowAdmittedAfterReaderPreflight":    "exerciseLateFlowReaderGate",
	"internal/agentimage/capability_retained_test.go:TestFrozenBundleRequiresEveryRetainedReaderDomain":      "exerciseFrozenReaderDomains",
	"internal/agentimage/freeze_flow_ss_spawn_test.go:TestFreezeFlowSSDrainsSpawnBeforeProcessTable":         "exerciseFreezeFlowSSSpawnDrain",
	"internal/agentimage/freeze_flow_ss_spawn_test.go:TestFreezeFlowSSSpawnDrainSharesPrepareDeadline":       "exerciseFreezeFlowSSSpawnDeadline",
	"internal/agentimage/restore_domains_test.go:TestFrozenDomainsImportAndDetachCompleteDescriptors":        "exercisePristineDomainImport",
	"internal/broker/proxy_authority_deadline_test.go:TestProxyAuthoritySQLWaitUsesLookupBudget":             "exerciseProxyAuthoritySQLBudget",
	"internal/broker/tunnel_authority_scope_test.go:TestTunnelAuthorityRequiresProxyParticipation":           "exerciseTunnelProxyParticipation",
	"internal/broker/tunnel_register_authority_test.go:TestFlowRegisterPreservesTransientAuthorityReasons":   "exerciseFlowRegisterAuthorityReasons",
	"internal/flow/image_capacity_test.go:TestFlowImageRetainsTerminalReceiptsBeyondActiveLimit":             "exerciseTerminalImageCapacity",
	"internal/flow/image_detach_test.go:TestFlowPristineDetachPreservesOriginalClosure":                      "exerciseFlowPristineDetach",
	"internal/flow/pump_overlap_test.go:TestAppliedOverlapWritesOnlyUnappliedSuffix":                         "exerciseAppliedOverlapSuffix",
	"internal/tunnel/flow_abort_test.go:TestAbortCloseKeepsDataStreamUntilTailReceipt":                       "exerciseAbortStreamTail",
	"internal/tunnel/flow_image_capacity_test.go:TestClientImageRetainsTerminalReceiptsBeyondActiveLimit":    "exerciseClientTerminalCapacity",
	"internal/tunnel/flow_open_stream_test.go:TestRejectedOpenReleasesCarrierStream":                         "exerciseRejectedOpenStream",
	"internal/tunnel/flow_permit_test.go:TestEndpointPermitSupportsSuccessiveOwnerGenerations":               "exerciseSuccessiveOwnerPermits",
	"internal/tunnel/flow_settlement_test.go:TestReconnectReconcilesLostFinalFINReceipt":                     "exerciseLostFinalFINReceipt",
	"internal/tunnel/flow_settlement_test.go:TestTerminalFlowReceiptsDoNotBreakCarrierResume":                "exerciseTerminalFlowResume",
	"test/hotupdate/flow_exec_test.go:TestFlowProductCommandStopsWhenAgentExits":                             "exerciseFlowCommandAgentExit",
	"test/hotupdate/rollback_test.go:TestRollbackFixtureKeepsIndependentPrivateTargetTraffic":                "exerciseRollbackPrivateTargetTraffic",
	// origin: hot-update S5 plan §8 / §10.2 / T51 — pin each live
	// subject call after its baseline and inside its audited five-round loop.
	"internal/agent/bootstrap_flow_test.go:TestBootstrapFlowRestoreStartsNoWorkers":                                      "exerciseBootstrapFlowRestore",
	"internal/agent/flow_restore_test.go:TestAgentRestoresRawThenResumeThenRegister":                                     "exerciseAgentFlowRestore",
	"internal/agent/flow_restore_test.go:TestFlowResumeRetriesKeepTheAbsoluteCutDeadline":                                "exerciseFlowResumeRetry",
	"internal/agent/flow_restore_test.go:TestFlowResumeSchedulerKeepsActiveEndpointsAheadOfIdle":                         "exerciseFlowResumeScheduler",
	"internal/agent/proxy_image_test.go:TestProxyPairDigestIsNotSuppressed":                                              "exerciseProxyPairDigest",
	"internal/agent/proxy_restore_order_test.go:TestProxyApplyEntrypointsAreSerialized":                                  "exerciseProxyApplyEntrypoints",
	"internal/agent/proxy_timer_image_test.go:TestProxyCutMetadataRestoresBeforeCallbacks":                               "exerciseProxyCutMetadata",
	"internal/agent/proxy_timer_image_test.go:TestProxyFiredCleanupSurvivesCut":                                          "exerciseProxyFiredCleanup",
	"internal/agent/proxy_timer_image_test.go:TestProxyFlowReservationKeepsOrdinaryExposeIndependent":                    "exerciseProxyFlowReservation",
	"internal/agent/ssproxy/crypto_nonce_test.go:TestSSNonceContinuityAndNoReseal":                                       "exerciseSSNonceCut",
	"internal/agent/ssproxy/fence_test.go:TestSSFreezeAndCleanupJoinOutsideLocks":                                        "exerciseSSFreezeCleanup",
	"internal/agent/ssproxy/image_exec_test.go:TestSSFiveRoundImageOwnershipFailures":                                    "exerciseSSImageOwnership",
	"internal/agent/ssproxy/image_exec_test.go:TestSSImageClosureSalvageAndRedaction":                                    "exerciseSSImageBoundary",
	"internal/agent/ssproxy/image_test.go:TestSSSecretsNeverEnterPublicCarriers":                                         "exerciseSSPublicCarriers",
	"internal/agent/ssproxy/legacy_darwin_test.go:TestLegacySSDarwinLifecycle":                                           "exerciseLegacySSDarwin",
	"internal/agent/ssproxy/revoke_flow_test.go:TestSSSilentRevokeClosesBoundFlow":                                       "exerciseSSSilentRevoke",
	"internal/agent/ssproxy/state_codec_test.go:TestSSTrialParserLifecycle":                                              "exerciseSSTrialParser",
	"internal/agent/upgrade_domain_flow_test.go:TestFlowSSCandidateAndActualPrevDomains":                                 "exerciseFlowSSReaderDomains",
	"internal/agent/upgrade_domain_flow_test.go:TestPendingOldPrevRejectsNewFlowSSAdmission":                             "exercisePendingFlowSSAdmission",
	"internal/agent/upgrade_flow_failure_test.go:TestFlowSSExecFailureThawsBothDirections":                               "exerciseFlowSSExecFailure",
	"internal/agentboot/flow_ss_activation_test.go:TestFlowSSPartialActivationUsesCurrentCut":                            "exerciseFlowSSPartialActivation",
	"internal/agentboot/flow_ss_activation_test.go:TestImageActivationOwnerExcludesPristineCleanup":                      "exerciseActivationOwnerCleanup",
	"internal/agentboot/flow_ss_redispatch_test.go:TestFlowSSRedispatchUsesCurrentState":                                 "exerciseCurrentRedispatch",
	"internal/agentimage/bundle_test.go:TestBundleFlowSSMetadataKeepsInactiveResponsibility":                             "exerciseBundleFlowSSMetadata",
	"internal/agentimage/bundle_test.go:TestBundleFreezeWindowStartsAtAdmission":                                         "exerciseBundleAdmissionTimestamp",
	"internal/agentimage/flow_ss_activation_test.go:TestDomainCompleteActivationWindow":                                  "exerciseDomainCompleteActivationWindow",
	"internal/agentimage/flow_ss_image_test.go:TestBundleFinalReexportIncludesFlowSSRevoke":                              "exerciseBundleFinalReexportIncludesFlowSSRevoke",
	"internal/agentimage/flow_ss_image_test.go:TestBundleImportSharesNodeReplayBudget":                                   "exerciseTargetReplayBudget",
	"internal/agentimage/flow_ss_image_test.go:TestComposedSealedStateBoundCountsAllDomains":                             "exerciseComposedSealedStateBound",
	"internal/agentimage/flow_ss_image_test.go:TestComposedSnapshotResourcesRemainComplete":                              "exerciseComposedSnapshotResources",
	"internal/agentimage/flow_ss_image_test.go:TestComposedSnapshotRetainsBorrowedEndpointState":                         "exerciseRetainedEndpointState",
	"internal/agentimage/flow_ss_image_test.go:TestFinalReexportRefusalKeepsSealedCut":                                   "exerciseFinalReexportRefusal",
	"internal/agentimage/flow_ss_image_test.go:TestFinalReexportRequiresRetainedDomainOwners":                            "exerciseFinalReexportRequiresOwners",
	"internal/broker/broker_generation_survives_test.go:TestExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch": "exerciseExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch",
	"internal/broker/broker_refuses_unpersisted_test.go:TestExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration": "exerciseExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration",
	"internal/broker/capability_gate_trust_test.go:TestExternalReviewHeartbeatConvergesAfterDBEpochRewind":               "exerciseExternalReviewHeartbeatConvergesAfterDBEpochRewind",
	"internal/broker/capability_gate_trust_test.go:TestExternalReviewZeroEpochHeartbeatRetriesUnreadyProxy":              "exerciseExternalReviewZeroEpochHeartbeatRetriesUnreadyProxy",
	"internal/broker/flow_rehome_test.go:TestReaperDoesNotRehomeDuringPlannedCarrierRebuild":                             "exercisePlannedReaperRebuild",
	"internal/broker/lease_model_test.go:TestLeaseModelRandomSequencesHoldTheInvariants":                                 "exerciseLeaseModelDelayedProbe",
	"internal/broker/proxy_authority_test.go:TestProxyAuthoritySnapshotNeverMixesKeysAndEpoch":                           "exerciseProxyAuthoritySnapshot",
	"internal/broker/proxy_reconcile_test.go:TestProxyReaperReadinessControls":                                           "exerciseProxyReaperReadinessControls",
	"internal/broker/proxy_reconnect_test.go:TestRepairProxyNoStormWhenOnNotReadyAtSamePair":                             "exerciseRepairProxyNoStormWhenOnNotReadyAtSamePair",
	"internal/broker/proxy_reconnect_test.go:TestRepairProxyPushesOnGenEpochMismatch":                                    "exerciseRepairProxyPushesOnGenEpochMismatch",
	"internal/broker/tunnel_authority_test.go:TestResumeAuthorityLookupStopsAndRejectsKnownLag":                          "exerciseTunnelAuthority",
	"internal/execsession/shared_budget_test.go:TestSharedReplayBudgetCountsOwnedDomains":                                "exerciseSharedRuntimeBudget",
	"internal/execsession/shared_budget_test.go:TestSharedReplayImportFailureReturnsOwnedReservations":                   "exerciseSharedImportFailure",
	"internal/execsession/shared_budget_test.go:TestSharedReplayLiveExecKeepsNodeBudget":                                 "exerciseSharedLiveExec",
	"internal/flow/fence_test.go:TestFlowFreezeJoinsWithoutClosingBusiness":                                              "exerciseFlowFreezeJoinsWithoutClosingBusiness",
	"internal/flow/image_test.go:TestFlowImageFrozenRuntimeRoundTrip":                                                    "exerciseFlowImageRuntimeRoundTrip",
	"internal/flow/image_test.go:TestFlowImagePendingAndRetirementSurvive":                                               "exerciseFlowImage",
	"internal/flow/image_test.go:TestFlowImageSalvageKeepsEveryTakenDescriptor":                                          "exerciseFlowImageSalvage",
	"internal/flow/manager_test.go:TestFlowACKUsesAppliedNotCarrierSent":                                                 "exerciseFlowACKUsesAppliedNotCarrierSent",
	"internal/flow/manager_test.go:TestFlowAbortKeepsFinalTailReceipt":                                                   "exerciseFlowAbortKeepsFinalTailReceipt",
	"internal/flow/manager_test.go:TestFlowDedupeBeforePartialWrite":                                                     "exerciseFlowDedupeBeforePartialWrite",
	"internal/flow/manager_test.go:TestFlowFINCloseWritePreservesReverseTail":                                            "exerciseFlowFINCloseWritePreservesReverseTail",
	"internal/flow/manager_test.go:TestFlowFiveRoundLifecycle":                                                           "exerciseFlowLifecycle",
	"internal/flow/open_test.go:TestFlowOpenReconciliationKeepsOriginalDial":                                             "exerciseFlowOpenReconciliationKeepsOriginalDial",
	"internal/flow/retirement_test.go:TestRetiredOpenCannotDialAfterReceiptGC":                                           "exerciseRetiredOpenCannotDialAfterReceiptGC",
	"internal/rawsocket/image_test.go:TestRawSocketImageIdentityAndSalvage":                                              "exerciseRawSocketImage",
	"internal/rawsocket/socket_linux_test.go:TestRawSocketFiveRoundLifecycle":                                            "exerciseRawSocketLifecycle",
	"internal/sealedstate/state_linux_test.go:TestSealedStateFiveRoundLifecycle":                                         "exerciseSealedStateLifecycle",
	"internal/tunnel/flow_alpn_test.go:TestFlowALPNNMinusOneQuadrants":                                                   "exerciseFlowALPN",
	"internal/tunnel/flowcarrier/carrier_test.go:TestYamuxSharedWriterCancellationBounds":                                "exerciseSharedWriterCancellation",
	"internal/tunnel/flow_endpoint_test.go:TestEndpointSurvivesSessionLossAndResumes":                                    "exerciseEndpointResume",
	"internal/tunnel/flow_endpoint_test.go:TestTunnelFlowFiveRoundLifecycle":                                             "exerciseTunnelFlowLifecycle",
	"internal/tunnel/flow_image_test.go:TestClientImagePreparePermitAndFDClosure":                                        "exerciseClientImageDiagnostics",
	"internal/tunnel/flow_resume_test.go:TestCurrentOwnerTicketCoversLongIdleAndFirstDetach":                             "exerciseCurrentOwnerTicketCoversLongIdleAndFirstDetach",
	"internal/tunnel/flow_resume_test.go:TestPrepareActivateLostReplyHasOneOwner":                                        "exercisePrepareActivateLostReplyHasOneOwner",
	"internal/tunnel/flow_resume_test.go:TestResumeReRunsTokenLookupAndFence":                                            "exerciseResumeReRunsTokenLookupAndFence",
	"internal/tunnel/legacy_darwin_test.go:TestLegacyExposeDarwinLifecycle":                                              "exerciseLegacyExposeDarwin",
	"internal/tunnel/register_buffer_test.go:TestRegisterReaderRetainsYamuxPrefix":                                       "exerciseRegisterBuffer",
	"internal/workload/replay_budget_test.go:TestReplayACKKeepsCapacityReservation":                                      "exerciseReplayACKReservation",
	"test/hotupdate/carrier_test.go:TestFlowCarrierBindOracleControls":                                                   "exerciseFlowTLSRelay",
	"test/hotupdate/carrier_test.go:TestFlowCarrierRebuildLifecycleMatrix":                                               "exerciseFlowCarrierLifecycle",
	"test/hotupdate/flow_exec_test.go:TestAgentSamePIDExecKeepsFourTCPFlows":                                             "exerciseAgentFourTCPFlows",
	"test/hotupdate/flow_lifecycle_test.go:TestFlowContinuityOracleRejectsSocketAndTailFaults":                           "exerciseFlowContinuityOracle",
	"test/hotupdate/flow_lifecycle_test.go:TestHalfCloseTailAcrossRealExec":                                              "exerciseHalfCloseRealExec",
	"test/hotupdate/partial_activation_test.go:TestReadyPendingWatchdogTakesSecondCurrentCut":                            "exerciseReadyPendingFlowSSWatchdog",
	"test/hotupdate/raw_probe_test.go:TestRawProbeDetectsDroppedAndDuplicatedBytes":                                      "exerciseRawProbe",
	"test/hotupdate/rollback_test.go:TestActiveRollbackExportsCurrentFlowSSState":                                        "exerciseActiveFlowSSRollback",
	"test/hotupdate/ss_exec_test.go:TestSSActualSealLedgerCoversIndependentDuplexTrace":                                  "exerciseSSSealControl",
	"test/hotupdate/ss_exec_test.go:TestSSIndependentReferenceDuplexTrace":                                               "exerciseSSReferenceDuplex",
	"test/hotupdate/ss_exec_test.go:TestSSRealExecKeepsIndependentCipherTrace":                                           "exerciseSSRealExec",
	"test/hotupdate/ss_reference_test.go:TestSSReferenceRejectsNonceReuse":                                               "exerciseSSReference",
	"test/hotupdate/ss_revoke_test.go:TestSSRevokedDuringCutNeverWritesAfterGrant":                                       "exerciseSSCutRevoke",
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	"test/d7/integration_test.go:TestD7Matrix": "exerciseD7LeadershipMove",
	// origin: hot-update S4 plan §8.4 / §10 / T47 — pin the runtime and upgrade
	// exercises inside the five-round loops, rather than unrelated setup calls.
	"internal/agent/upgrade_domain_test.go:TestPTYCandidateDomainsAndSiblingPrev":           "cutForward",
	"internal/agent/upgrade_domain_test.go:TestPTYDomainVersionProbeAndPureExecReadability": "CombinedOutput",
	"internal/agent/upgrade_domain_test.go:TestPTYFinalPrepareRechecksDomains":              "cutForward",
	"internal/agent/upgrade_domain_test.go:TestPTYPendingAdmissionCannotFallBack":           "domainRequirePTYAdmission",
	"internal/agent/upgrade_image_test.go:TestPTYUpgradeExecFailureRestoresBothDirections":  "reExecInPlace",
	"internal/agent/upgrade_image_test.go:TestPreflightDefersOnEachReasonCode":              "domainExerciseLegacyStages",
	"internal/execsession/activation_test.go:TestPTYRuntimeAdmissionLockDeadline":           "exercisePTYAdmissionDeadline",
	"internal/execsession/pty_image_test.go:TestPTYRuntimeFiveRoundImportFailures":          "exercisePTYImageFailure",
	"internal/execsession/pty_revoke_test.go:TestPTYRevokeBeforeQueuedInputActivation":      "exercisePTYRevoke",
	"internal/execsession/pty_test.go:TestPTYSpawnBudgetAndFailureOwnership":                "Handle",
	"internal/execsession/pty_test.go:TestPtyInputAbortStillSettles":                        "exercisePTYInputAbort",
	// origin: hot-update S4 plan §5.1 / §8.4 — real replay snapshots across strict metadata round trips.
	"internal/ptysession/image_types_test.go:TestPTYImageRoundTripPreservesResponsibility":                         "exercisePTYImageRoundTrip",
	"internal/ptysession/actor_test.go:TestPtyImageRejectsInconsistentResponsibility":                              "tableActor",
	"internal/ptysession/actor_test.go:TestPtyImportBorrowsFrozenOwnersWithoutActivating":                          "tableActor",
	"internal/ptysession/control_test.go:TestHangupSequenceNeverSplitsAcrossSnapshot":                              "exerciseHangupSnapshot",
	"internal/ptysession/control_test.go:TestPTYKAAbsoluteDeadlinesAcrossGenerations":                              "exerciseKADeadlines",
	"internal/ptysession/control_test.go:TestPtyControlReplayDoesNotRepeatSideEffects":                             "exerciseControlReplay",
	"internal/ptysession/control_test.go:TestPtyHangupFailureReceipts":                                             "exerciseHangupReceipts",
	"internal/ptysession/fence_test.go:TestPtyDuplexDeadlineIsolation":                                             "exerciseDeadlineIsolation",
	"internal/ptysession/fence_test.go:TestPtyFreezeDeadlineIncludesControlAdmission":                              "exerciseFreezeAdmission",
	"internal/ptysession/image_exec_test.go:TestPtyActorSurvivesSamePIDExecWithBlockedSlave":                       "runActorExecParent",
	"internal/ptysession/input_test.go:TestPtyInputCreditAndOverlap":                                               "exerciseInputCredit",
	"internal/ptysession/input_test.go:TestPtyLogicalFINIsNotTerminalEOF":                                          "runActorExecParent",
	"internal/ptysession/lifecycle_test.go:TestPtyRepeatedLifecycleReleasesResources":                              "exerciseActorLifecycle",
	"internal/ptysession/output_test.go:TestPtyOutputAbortFreezesFinalEnd":                                         "exerciseOutputAbort",
	"internal/ptysession/output_test.go:TestPtyTailOutlivesLeaderExit":                                             "runActorExecParent",
	"internal/terminalio/io_test.go:TestTerminalIOCancellationAndDescriptorOwnership":                              "exerciseTerminalIO",
	"internal/procrequest/pty_exec_test.go:TestPTYIntentAndSettledKindSurviveImage":                                "CombinedOutput",
	"internal/procrequest/pty_test.go:TestPTYRegistrySharesAdmissionAndRejectsUnknownKinds":                        "newRequestFixture",
	"internal/workload/control_test.go:TestProcessControlGuardAndSessionCleanup":                                   "exerciseControlGuard",
	"internal/execclient/pty_lifecycle_test.go:TestPTYClientRepeatedCancelAndRetryReleasesResources":               "exercisePTYClientCancelAndRetry",
	"cmd/tether/run_reliable_test.go:TestRunReliableErrorExitClasses":                                              "runResizeEvents",
	"internal/pty/legacy_io_test.go:TestLegacyPTYOwnerRepeatedLifecycle":                                           "exerciseLegacyPTYLifecycle",
	"internal/pty/master_poll_test.go:TestPtyResizePreservesPollability":                                           "exercisePtyResizeDeadlines",
	"internal/pty/process_owner_test.go:TestSessionDrainingOwnerExcludesPriorIOAndWait":                            "exerciseSessionOwnerExclusion",
	"internal/pty/terminal_test.go:TestPtyEIOOnlyInStartedDomain":                                                  "exerciseTerminalEIODomains",
	"internal/pty/terminal_test.go:TestPtyMasterTransferAndWrongDescriptorRefusal":                                 "exerciseTerminalTransfer",
	"internal/agent/bootstrap_test.go:TestPTYAdmissionEstablishedBeforeSubscription":                               "bootFixture",
	"internal/agent/run_legacy_test.go:TestLegacyPTYInputDoesNotBlockOtherSessions":                                "exerciseLegacyPTYIntake",
	"internal/agent/run_reliable_test.go:TestPTYScopeAndActorStamp":                                                "exercisePTYScope",
	"internal/agent/session_destroyed_pty_test.go:TestPTYSessionRecreationBoundary":                                "exercisePTYRecreatedAuthority",
	"internal/agent/session_destroyed_pty_test.go:TestSessionDestroyedEventKeepsDaemonContext":                     "newBootAgent",
	"internal/agent/session_destroyed_pty_test.go:TestSessionDestroyedRevokesPTYAcrossCut":                         "exercisePTYRevocationCut",
	"internal/agentboot/image_test.go:TestImagePartialActivationKeepsOwnershipUntilCurrentCut":                     "newCutFixture",
	"internal/agentboot/pty_activation_test.go:TestPTYPartialActivationNeverDetachesInitialImage":                  "exercisePTYPartialActivation",
	"internal/agentimage/pty_exec_test.go:TestPTYBundleRollbackCarriesCurrentState":                                "ptyRollbackArgs",
	"internal/agentimage/pty_test.go:TestImportedThawFailureCommitsResourceResponsibility":                         "newBundleFixture",
	"internal/agentimage/pty_test.go:TestPTYBundleSalvagePreservesEveryTakenDescriptor":                            "newPTYBundleConfig",
	"internal/agentimage/pty_test.go:TestPrepareCheckedSeesFinalExportBeforeDuplicating":                           "PrepareChecked",
	"test/cli_e2e/run_darwin_test.go:TestLegacyRunDarwinTerminalLifecycle":                                         "newRealStack",
	"test/cli_e2e/run_reliable_test.go:TestPTYHarnessDuplexBytesAndDescriptorRestoration":                          "newPTYFixture",
	"test/cli_e2e/run_reliable_test.go:TestRunReliableLocalCancellationRestoresTerminal":                           "startTerminalCtl",
	"internal/workload/control_exec_test.go:TestSessionCleanupSurvivesSamePIDExec":                                 "CombinedOutput",
	"internal/workload/replay_test.go:TestReplayRepeatedRestoreReleasesResources":                                  "exerciseReplayRestore",
	"internal/workload/replay_exec_test.go:TestReplayExecRepeatedlyReleasesResources":                              "exerciseReplayExec",
	"internal/workload/output_test.go:TestOutputRepeatedLifecycleReleasesResources":                                "exerciseOutputLifecycle",
	"internal/workload/input_test.go:TestInputRepeatedLifecycleReleasesResources":                                  "exerciseInputPartialWrite",
	"internal/workload/process_test.go:TestManagerRepeatedLifecycleReleasesResources":                              "exerciseProcessLifecycle",
	"internal/workload/process_state_test.go:TestProcessStateRepeatedRestoreReleasesResources":                     "exerciseProcessRestore",
	"internal/handoff/image_exec_test.go:TestHandoffRepeatedExecReleasesResources":                                 "exerciseHandoffExec",
	"internal/jsstream/object_reader_lifecycle_test.go:TestTransferObjectConstructorCancellationReleasesResources": "GetTransferObject",
	"internal/jsstream/object_reader_test.go:TestTransferObjectCloseAndCancelReleaseResources":                     "GetTransferObject",
	"internal/tunnel/server_restart_test.go:TestTunnelClientRecoversAcrossServerRestarts":                          "startServer",
	// origin: docs/reviews/hot-update-stable-tree-plan.md §4 5a — the three reliable-exec
	// primitives the handoff left unregistered: a pollable PTY master, the request registry
	// and the exec runtime (whose loop also runs the post-start setup-failure shape).
	"internal/pty/master_poll_test.go:TestMasterPollableLifecycleReleasesResources":        "exerciseMasterReadDeadline",
	"internal/procrequest/registry_test.go:TestRegistryRepeatedLifecycleReleasesResources": "exerciseRegistryLifecycle",
	"internal/execsession/runtime_test.go:TestRuntimeRepeatedExecutionReleasesResources":   "exerciseRuntimeExecution",
	// origin: docs/reviews/hot-update-agent-image-abi-plan.md exit #5 — the image fence's three
	// round trips: the runtime cut in one process (freeze→export→import→discard→thaw), the
	// composed bundle's rollback (freeze→prepare→close→thaw), and the composed bundle across a
	// real same-PID exec measured from the parent.
	"internal/execsession/runtime_image_test.go:TestRuntimeImageRoundTripReleasesResources":              "exerciseRuntimeImageRoundTrip",
	"internal/execsession/pty_records_test.go:TestPTYControlCommitReceiptDeduplicatesRetries":            "auditedPTY",
	"internal/execsession/pty_records_test.go:TestPTYImageStateReceiptsMatchExportAndImportedOwners":     "auditedPTY",
	"internal/execsession/pty_records_test.go:TestPTYReliableLifecycleReceiptsExcludeUserData":           "auditedPTY",
	"internal/execsession/pty_records_test.go:TestPTYCarrierReceiptsKeepCurrentStateAndSuppressAttempts": "auditedPTY",
	"internal/execsession/pty_records_test.go:TestPTYSettlementReceiptFollowsAckCommitExactlyOnce":       "auditedPTY",
	// origin: hot-update S4 internal review rv-fd-4, rv-fd-2, rv-fd-3, rv-fd-V1 (fix round FX-A)
	"internal/agent/bootstrap_activation_test.go:TestRunRetainsRecoveryAfterPartialPTYActivation":                  "exerciseRunRetainsRecoveryAfterPartialPTYActivation",
	"internal/agent/bootstrap_recovery_test.go:TestRunActivationFailureKeepsCurrentStateRecoveryReachable":         "exerciseRunActivationFailureKeepsCurrentStateRecoveryReachable",
	"internal/execsession/activation_retry_test.go:TestPartialActivationRetryKeepsAlreadyActiveActors":             "exercisePartialActivationRetryKeepsAlreadyActiveActors",
	"internal/execsession/activation_progress_test.go:TestActivationRetryResumesOnlyUnfinishedActors":              "exerciseActivationRetryResumesOnlyUnfinishedActors",
	"internal/agentimage/freeze_admission_test.go:TestFreezeDeadlineIncludesProcessOwnershipPrecheck":              "exerciseFreezeDeadlineIncludesProcessOwnershipPrecheck",
	"internal/ptysession/fence_recovery_test.go:TestFreezeDeadlineIncludesTimeoutRecoveryLock":                     "exerciseFreezeDeadlineIncludesTimeoutRecoveryLock",
	"internal/execsession/activation_progress_test.go:TestFreezeDeadlineIncludesRuntimeOwnershipSnapshot":          "TaskPIDsUntil",
	"internal/execsession/activation_progress_test.go:TestFreezeRollbackBoundsRuntimeAccountingLock":               "exerciseFreezeRollbackAccountingLock",
	"internal/agentimage/freeze_admission_test.go:TestFreezeRollbackIncludesManagerActivationGuard":                "Freeze",
	"internal/agent/bootstrap_recovery_test.go:TestActivationCutBoundsOwnershipGuards":                             "exerciseCutOwnershipDeadline",
	"internal/execsession/activation_progress_test.go:TestFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor": "exerciseFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor",
	// origin: hot-update S4 drill-run4 36.A/36.C — retained terminal after activation; carrier diagnostics off the raw terminal
	"internal/execsession/pty_terminal_retention_test.go:TestPTYRetainedTerminalAnswersLateRequests":    "ptyCreate",
	"internal/execsession/pty_terminal_retention_test.go:TestPTYTerminalCompletionSerializesRevocation": "ptyRoundTrip",
	"test/cli_e2e/pty_terminal_retention_test.go:TestRunReliableRetainedTerminalAfterActivation":        "startTerminalCtlStderr",
	"test/cli_e2e/pty_carrier_output_test.go:TestRunReliableCarrierDiagnosticsStayOutsideTerminal":      "startTerminalCtlStderr",
	// origin: hot-update S4 plan §8.4; CLI-LATENCY child race diagnostic x5.
	"test/cli_e2e/realstack_subprocess_race_test.go:TestRealStackRaceSubprocessRetainsDiagnostics": "CombinedOutput",
	// origin: hot-update S4 drill-run3 36.A — in-time spawns drain inside the cut instead of deferring it
	"internal/agent/upgrade_image_test.go:TestPreflightAllowsInTimeSpawnBeforeQuiescingCut":               "exercisePreflightInTimeSpawn",
	"internal/agentimage/freeze_spawn_test.go:TestFreezeWaitsForInTimeSpawnAndImagesChild":                "beginSpawnCut",
	"internal/agentimage/freeze_spawn_test.go:TestFreezeStillDefersOnAbandonedSpawn":                      "RunStart",
	"internal/agentimage/freeze_spawn_test.go:TestFreezeDrainsNonRuntimeSlotsBeforeExport":                "exerciseFreezeNonRuntimeDrain",
	"internal/agentimage/freeze_spawn_test.go:TestFreezeSpawnDrainDeadlineRollsBackRuntime":               "exerciseFreezeSpawnDrainRollback",
	"internal/agentimage/freeze_spawn_test.go:TestFreezeCreationRacingDrainWaitsUntilThaw":                "exerciseFreezeCreationRacingDrain",
	"internal/agentimage/freeze_spawn_test.go:TestFreezeUnderContinuousCreationCompletesEveryRequestOnce": "beginSpawnCut",
	"internal/spawnsafe/spawn_admission_test.go:TestSpawnTimeoutClassifiesTouchAndResolutionAsAbandoned":  "exerciseSpawnBackgroundTimeout",
	"internal/spawnsafe/spawn_admission_test.go:TestSpawnCutGuardUsesAbsoluteDeadline":                    "exerciseSpawnCutGuardDeadline",
	"internal/spawnsafe/spawn_admission_test.go:TestSpawnCutOrdersDrainBeforeRacingCreation":              "exerciseSpawnCutOrdering",
	"internal/spawnsafe/spawn_admission_test.go:TestSpawnCutDeadlineAndAbandonReopenAdmission":            "exerciseSpawnCutDeadlineAndAbandon",
	"internal/spawnsafe/spawn_admission_test.go:TestSpawnCutGatesTouchResolutionAndProbeLaunch":           "exerciseSpawnCutBackgroundOperations",
	// origin: hot-update S4 close — bounded admission recovery refusal and thaw-failure responsibility
	"internal/agent/process_recovery_test.go:TestProcessClientsContinueAfterBoundedAdmissionRecovery":            "exerciseProcessAdmissionRecovery",
	"internal/execsession/activation_progress_test.go:TestExecThawFailureRetainsPartialActivationResponsibility": "exerciseExecThawFailure",
	// origin: hot-update S4 internal review rv-fd-3 follow-up (expired-thaw recovery hand-off, Handle admission bound)
	"internal/execsession/activation_handoff_test.go:TestExpiredThawRetainsRecoveryThroughRuntimeLock": "exerciseExpiredThawHandoff",
	"internal/execsession/activation_handoff_test.go:TestDelayedThawCannotActivateLaterCut":            "exerciseDelayedThawGeneration",
	"internal/execsession/admission_test.go:TestHandleBoundsEveryAdmissionWait":                        "exerciseHandleAdmissionBounds",
	// origin: hot-update S4 internal review rv-eof-1 = rv-fd-1, rv-eof-2 (fix round FX-B)
	"internal/ptysession/input_finality_test.go:TestPtyInputEndedWaitsForInflightWriteReceipt":    "exerciseSourceEndFIN",
	"internal/ptysession/input_finality_test.go:TestInputEndedWaitsForInFlightWriteAccounting":    "exerciseSourceEndWithoutFIN",
	"internal/ptysession/input_finality_test.go:TestPtyInputAbortWaitsForInflightWriteReceipt":    "exerciseInflightInputAbort",
	"internal/execsession/pty_input_test.go:TestPTYCommittedReplayAbortIsAStatusReply":            "exerciseCommittedReplayAbortStatus",
	"internal/execclient/pty_replay_test.go:TestPTYReplayPrefixAbortDrainsThroughAgentAndSettles": "exerciseReplayAbortThroughAgent",
	// origin: hot-update S4 internal review rv-auth RV-AUTH-1/2/3 (fix round FX-C)
	"internal/execsession/pty_terminal_authority_test.go:TestPTYTerminalAuthorityRevocationPreservesRestorability": "newPTYRuntime",
	"internal/ptysession/control_revocation_test.go:TestPTYRevocationRetainsUnfinishedCleanup":                     "tableActor",
	"internal/ptysession/control_authority_deadline_test.go:TestPTYControlRequiresUnexpiredImportedAuthority":      "startActorFixture",
	"internal/ptysession/control_authority_deadline_test.go:TestPTYDuplicateControlDoesNotRenewExpiredAuthority":   "startActorFixture",
	"internal/agent/pty_health_generation_test.go:TestPTYHealthProofCannotOutliveCurrentConnectionGeneration":      "exercisePTYHealthGeneration",
	"internal/ptysession/control_health_test.go:TestPTYHealthProofRechecksGenerationAfterControlWait":              "startActorFixture",
	"internal/agent/pty_carrier_records_test.go:TestPTYCarrierReceiptsFollowEstablishedSessions":                   "exercisePTYCarrierReceipts",
	"internal/agentimage/freeze_test.go:TestBundleFreezeRollbackReleasesResources":                                 "exerciseBundleFreezeRollback",
	"internal/agentimage/bundle_exec_test.go:TestBundleRepeatedExecReleasesResources":                              "exerciseBundleExec",
	// hot-update S3 (ctl): the `node upgrade --all` fan-out over upgrade_deferred / image_abi_unsupported
	// replies, five rounds per classification.
	"cmd/tether/exitcode_test.go:TestUpgradeDeferredCodesAreClassified": "runUpgradeAll",
	// hot-update S3 (agent boot path): each bootstrap guard repeats its subject — the boot check, the
	// constructor that restores, a whole importing agent, the thaw, and the exit funnel.
	"internal/agent/bootstrap_test.go:TestBootRollbackExecFailureKeepsImageHeld":   "bootUpgradeCheckLocked",
	"internal/agent/bootstrap_test.go:TestImportFailureNeverRegistersEmptyTable":   "New",
	"internal/agent/bootstrap_test.go:TestReconcileSnapshotIncludesImportedTasks":  "runBootAgent",
	"internal/agent/bootstrap_test.go:TestSessionDestroyedTombstoneSurvivesImport": "activateImage",
	"internal/agent/bootstrap_test.go:TestFinishBootRedispatchTable":               "finishBoot",
	// hot-update S3 (agent cut path): the cut, its rollback and its admission gate, five rounds each.
	"internal/agent/upgrade_image_test.go:TestUpgradeExecFailureThawsOriginalBusiness":       "exerciseUpgradeExecFailure",
	"internal/agent/upgrade_image_test.go:TestRunExitDuringCutWaitsForAbandon":               "reExecInPlace",
	"internal/agent/upgrade_image_test.go:TestWatchdogRollbackReexportsCurrentStateOrDefers": "watchdogRollback",
	"internal/agent/upgrade_image_test.go:TestRunFailedBeforeControlReadyTable":              "runFailedBeforeControlReady",
	"internal/agent/upgrade_image_test.go:TestUpgradeCutLeavesNoInheritableDescriptors":      "reExecInPlace",
	"internal/agent/upgrade_image_test.go:TestUpgradeGateRefusesEveryLegacyAdmission":        "cutExerciseLegacyAdmission",
	"internal/agent/upgrade_image_test.go:TestReExecOnlyFailureKeepsTheCutAlive":             "cutForward",
	"internal/agent/upgrade_image_test.go:TestUpgradeHomeProbeIsBoundedAndSingleFlight":      "probeUpgradeHome",
	"internal/agent/upgrade_image_test.go:TestUpgradeDefersForDisconnectedTunnelSessions":    "AddProxy",
	// hot-update S3 (real binaries): five deferred `node upgrade` attempts against a live PTY.
	"test/cli_e2e/node_upgrade_image_test.go:TestNodeUpgradeDefersOnBusyResource": "exerciseNodeUpgradeBusyResource",
	// hot-update S3 internal-review fixes: the courier receipt rule (CC-3), the expose admission
	// paths (CC-4), the activation remainder and the version-probe supervisor (RS-1, RS-3).
	"internal/agent/proc_delivery_test.go:TestRegisterReceiptRetainsExitWhenStartAckArrivesAfterSnapshot": "receiptLaunchRegister",
	"internal/agent/expose_cut_admission_test.go:TestImageCutDefersWhileExposeReplayHandshakeIsInFlight":  "replayPortsFromState",
	"internal/agent/expose_cut_admission_test.go:TestImageCutBlocksExposeReplay":                          "replayPortsFromState",
	"internal/agent/expose_cut_admission_test.go:TestImageCutBlocksHomePushOpeningNewExpose":              "handleHomeForwarded",
	"internal/agent/expose_cut_admission_test.go:TestHomeAdmissionCoversRetriesAndSuccessorWorkers":       "applyHomeDirectives",
	"internal/agent/expose_cut_admission_test.go:TestFlowBusyCountsAdmissionBeforeInstalledSessions":      "replayPortsFromState",
	"internal/agentboot/image_resource_lifetime_test.go:TestImageCompletionClosesUntakenPipeBeforeExit":   "Start",
	"internal/agent/upgrade_probe_lifetime_test.go:TestCandidateABIProbeLeavesNoDescendant":               "smokeVersionImageABI",
	// hot-update S3 internal-review fixes, cut/rollback state machine (RB-1..4, CC-1/2/5/6, CC-V1, TD-7b).
	"internal/agent/upgrade_rollback_test.go:TestRunAuthoritativeRefusalRedispatchesImportedTasks":        "Run",
	"internal/agent/upgrade_rollback_test.go:TestWatchdogReadyImageRetainsHandoffAfterCommitWriteFailure": "watchdogRollback",
	"internal/agent/upgrade_rollback_test.go:TestWatchdogMarkerGenerationMatchesRedispatchedBundle":       "watchdogRollback",
	"internal/agent/upgrade_rollback_test.go:TestWatchdogExecFailureRecordsThawedResources":               "watchdogRollback",
	"internal/agent/upgrade_rollback_test.go:TestWatchdogDefersBusyCutBeforePreviousImageProbe":           "watchdogRollback",
	"internal/agent/subprocess_race_test.go:TestRaceSubprocessRetainsDiagnostics":                         "CombinedOutput",
	"internal/agent/upgrade_rollback_test.go:TestRunFailureRechecksParentAfterLock":                       "runFailedBeforeControlReady",
	"internal/agent/upgrade_rollback_test.go:TestBootstrapFailureEmitsRedispatchReceipt":                  "finishBoot",
	"internal/agent/upgrade_admission_test.go:TestSuccessfulCommitClearsDeferredRollbackAdmission":        "watchdogRollback",
	"internal/agent/upgrade_admission_test.go:TestInstallRefusesWhileReExecOnlyOwnsFrozenImage":           "cutForward",
	"internal/agent/upgrade_admission_test.go:TestUpgradeAdmissionSpansInstallRecovery":                   "cutForward",
	"internal/agent/upgrade_admission_test.go:TestInstallRefusesFrozenCutBeforeDownload":                  "cutForward",
	"internal/agent/upgrade_admission_test.go:TestExpiredLiveUpgradeKeepsItsPreviousImageUntilResolution": "cutForward",
	"internal/agent/upgrade_admission_test.go:TestExpiredPendingAdmissionRequiresLiveOwnership":           "cutForward",
	"internal/agent/upgrade_image_test.go:TestUpgradeCutFailureAfterFlipRollsDiskBack":                    "cutForward",
	// origin: gotcha #72 — the anchor is the ladder itself: a loop that merely dialt sockets would
	// prove nothing about whether a WEDGED teardown leaks its closer goroutine and socket.
	"internal/agent/conn_teardown_leak_test.go:TestWedgedTeardownRepeatsWithoutLeaking":              "Do",
	"internal/broker/alert_reconcile_test.go:TestD8AlertReconcileRunCancels":                         "Run",
	"internal/broker/transfer_audit_forward_test.go:TestD8TransferAuditForwardLeakGate":              "exerciseTransferAuditForward",
	"internal/broker/process_resume_pty_test.go:TestProcessResumePTYAuthorizationMatrix":             "exercisePTYAuthorizationMatrix",
	"internal/clusteroffline/restore_test.go:TestRestoredNodeRepeatedStartStopDoesNotLeakGoroutines": "New",
	"internal/tunnel/tunnel_reconnect_test.go:TestTunnelReconnectChurnNoGoroutineLeak":               "DropTransport",
	"internal/tunnel/tunnel_reconnect_test.go:TestTunnelReconnectVsConcurrentOpenSamePort":           "Open",
	"test/concurrency/goroutine_leak_rounds_test.go:TestAdminsockRepeatedStartCloseNoGoroutineLeak":  "Start",
	"test/concurrency/goroutine_leak_rounds_test.go:TestAgentRepeatedRunNoGoroutineLeak":             "runOnce",
	"test/concurrency/goroutine_leak_rounds_test.go:TestTunnelServerRepeatedCloseNoGoroutineLeak":    "Start",
	"test/concurrency/goroutine_leak_test.go:TestAdminsockServerCloseNoGoroutineLeak":                "Start",
	"test/concurrency/goroutine_leak_test.go:TestAgentRunCancelNoGoroutineLeak":                      "Run",
	"test/concurrency/goroutine_leak_test.go:TestBrokerImmediateCancelNoGoroutineLeak":               "Run",
	"test/concurrency/goroutine_leak_test.go:TestBrokerRepeatedRunNoGoroutineLeak":                   "startBrokerNoTunnel",
	"test/concurrency/goroutine_leak_test.go:TestBrokerRunCancelNoGoroutineLeak":                     "startBrokerNoTunnel",
	"test/concurrency/goroutine_leak_test.go:TestTunnelServerCloseNoGoroutineLeak":                   "Start",
	"test/concurrency/goroutine_leak_test.go:TestTunnelServerCloseWithActiveSessionNoLeak":           "Open",
	"test/concurrency/spawnsafe_stress_test.go:TestSpawnsafePolicy_concurrentGenSwap":                "Prepare",
	"test/d4/forward_test.go:TestD4ForwardChurnNoGoroutineLeak":                                      "Forward",
	"test/d5/publisher_test.go:TestD5RunPublishesAndNoLeak":                                          "Run",

	// origin: hot-update S6 plan §8.2 / T31 / FIX-BLACKOUT
	"internal/execclient/admission_legacy_test.go:TestAdmissionNoRespondersRequiresLiveLegacyPeer": "exerciseProbeNoResponderBlackout",
	"internal/execclient/admission_legacy_test.go:TestAdmissionLegacyProofCannotOutliveBudget":     "exerciseLegacyProofDeadline",
	// origin: hot-update S6 FIX-ROLL
	"internal/cli/cluster_roll_catchup_test.go:TestBrokerRollWaitsForLockCommitCatchUp":                "exerciseBrokerRollLockCommitCatchUp",
	"test/clusterharness/readiness_test.go:TestRoutedJetStreamReadinessRequiresEveryIngress":           "exerciseRoutedJetStreamReadiness",
	"test/clusterharness/readiness_linux_test.go:TestBrokerQuorumStartupRequiresInfrastructureWitness": "exerciseBrokerQuorumInfrastructureRefusal",

	// origin: hot-update S6 plan §6.3 / T28 / T56 / TESTFIX-2 — register-bind refusal, public-port
	// reservation and proxy-drop subjects, each the sole call of its five-round loop.
	"internal/tunnel/flow_register_bind_test.go:TestFlowRegisterBindFailurePreservesRefusalAndSurvivor": "exerciseFlowRegisterBindFailure",
	"test/clusterharness/broker_product_cohort_test.go:TestBrokerProductPublicPortReservation":          "exerciseBrokerProductPublicPortReservation",
	"test/p13/proxy_reconnect_e2e_test.go:TestProxyDisableDuringTunnelDropStaysDown":                    "exerciseProxyDisableDuringTunnelDrop",

	// origin: hot-update S7 plan §11.3 / T52 — transfer owners. Every loop body below calls only its
	// named exercise; TestTransferLifecycleNoLeaks pins exerciseTransferLifecycle (drillobserve:
	// exerciseTransferCustody) as test/e2e/parallel transferLiveCycles requires.
	"cmd/tether/transfer_command_test.go:TestTransferCommandStatusRateAndExitCodes":                                       "exerciseTransferCommand",
	"cmd/tether/transfer_compliance_test.go:TestCommitLocalTempNoForceConcurrentSingleWinner":                             "exerciseLocalTransferConcurrentCommit",
	"cmd/tether/transfer_mode_test.go:TestBothPullPathsCarryTheMode":                                                      "exercisePullModePaths",
	"internal/agentimage/transfer_image_test.go:TestTransferImageLifecycleNoLeaks":                                        "exerciseTransferImageLifecycle",
	"internal/agent/transfer_lifecycle_test.go:TestTransferCapabilityInstalledByNormalRegister":                           "exerciseTransferRegister",
	"internal/agent/transfer_lifecycle_test.go:TestTransferLifecycleNoLeaks":                                              "exerciseTransferLifecycle",
	"internal/agent/transfer_lifecycle_test.go:TestTransferRegisterGoneStopsOriginalWorker":                               "exerciseTransferRegisterGone",
	"internal/agent/transfer_upgrade_test.go:TestTransferPendingPrevAdmissionRequiresDurableCommit":                       "exerciseTransferPendingAdmission",
	"internal/agent/transfer_upgrade_test.go:TestTransferReturningExecRebindsBeforeThaw":                                  "exerciseTransferReturningExec",
	"internal/agent/transfer_upgrade_test.go:TestTransferUpgradeRequiresPreparedAndCourierDomains":                        "exerciseTransferUpgradeDomains",
	"internal/authcallout/transfer_settlement_test.go:TestTransferLifecycleNoLeaks":                                       "exerciseTransferLifecycle",
	"internal/broker/cluster_forward_config_test.go:TestTransferClusterConfigUsesRetiredCoordinatorProof":                 "exerciseTransferConfigHandoff",
	"internal/broker/cluster_forward_join_test.go:TestTransferImageJoinPropagatesBorrowerDeadline":                        "exerciseTransferImageJoin",
	"internal/brokercontrol/audit_publisher_history_test.go:TestTransferAuditCanceledAttemptRetainsHistoryResponsibility": "exerciseTransferAuditCanceledAttempt",
	"internal/brokercontrol/audit_publisher_history_test.go:TestTransferAuditHistorySeparatesStartAndTerminalSlots":       "exerciseTransferAuditHistorySlots",
	"internal/brokercontrol/transfer_history_test.go:TestTransferAuditPublishAckLossAcrossLeaderReplay":                   "exerciseTransferTerminalCheckpointLoss",
	"internal/brokercontrol/transfer_history_test.go:TestTransferContinuationMissingHistoryNeverAcknowledges":             "exerciseTransferMissingHistory",
	"internal/brokercontrol/transfer_history_test.go:TestTransferLifecycleNoLeaks":                                        "exerciseTransferLifecycle",
	"internal/brokercontrol/transfer_history_test.go:TestTransferStartAuditSurvivesLeadershipMove":                        "exerciseTransferStartUnacknowledged",
	"internal/brokerimage/transfer_runtime_test.go:TestTransferLifecycleNoLeaks":                                          "exerciseTransferLifecycle",
	"internal/brokerimage/transfer_runtime_test.go:TestTransferRefusedCutResumesBorrowersAfterRebind":                     "exerciseTransferRefusedJoin",
	"internal/broker/terminal_ledger_survives_test.go:TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit":           "exerciseTerminalLedger",
	"internal/broker/transfer_route_test.go:TestTransferAuditJoinUsesOriginalDeadline":                                    "exerciseTransferAuditJoin",
	"internal/broker/transfer_route_census_test.go:TestTransferClusterCensusRequiresOriginalFollowerProof":                "exerciseTransferFollowerCensus",
	"internal/broker/transfer_route_test.go:TestTransferClusterForwardJoinRetainsBlockedOwner":                            "exerciseTransferClusterJoin",
	"internal/broker/transfer_route_test.go:TestTransferClusterForwardRejectsMalformedProofs":                             "exerciseTransferMalformedProofs",
	"internal/broker/transfer_route_test.go:TestTransferClusterForwardUsesOneDeadline":                                    "exerciseTransferForwardDeadline",
	"internal/broker/transfer_route_test.go:TestTransferConfigForwardNeverAppliesOnFollower":                              "exerciseTransferFollowerConfig",
	"internal/broker/transfer_route_test.go:TestTransferN3RoutingUsesOriginalFollowerOwner":                               "exerciseTransferBusinessRouting",
	"internal/broker/transfer_session_removal_test.go:TestTransferSessionRemovalAuditLifetime":                            "exerciseTransferSessionRemoval",
	"internal/broker/transfer_wiring_test.go:TestTransferLifecycleNoLeaks":                                                "exerciseTransferLifecycle",
	"internal/broker/transfer_wiring_test.go:TestTransferWiringColdSubscriptionsAreBroadcast":                             "exerciseTransferSubscriptions",
	"internal/broker/transfer_wiring_cleanup_test.go:TestTransferWiringConfirmedTerminalStillRetiresObject":               "exerciseTransferConfirmedCleanup",
	"internal/cli/transfer_binding_test.go:TestTransferClientFactoriesUseDedicatedReceiptIO":                              "exerciseTransferFactories",
	"internal/cli/transfer_binding_test.go:TestTransferSelectionRequiresExplicitCapZero":                                  "exerciseTransferSelection",
	"internal/cli/transfer_engine_test.go:TestTransferNativeEngineReconcilesLostCommitWithoutReupload":                    "exerciseNativeEngine",
	"internal/cli/transfer_lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                "exerciseTransferLifecycle",
	"internal/cli/transfer_settlement_test.go:TestTransferSessionRemovalPreservesExplicitAuthority":                       "exerciseTransferSessionRemoval",
	"internal/cli/transfer_settlement_test.go:TestTransferSettlementRetainsReceiverAndOriginalScope":                      "exerciseTransferSettlement",
	"internal/drillobserve/transfer_lifecycle_test.go:TestTransferLifecycleNoLeaks":                                       "exerciseTransferCustody",
	"internal/filetransfer/transfer_lifecycle_test.go:TestTransferContinuationDelegatesWithoutLegacyEffects":              "exerciseTransferContinuation",
	"internal/filetransfer/transfer_lifecycle_test.go:TestTransferContinuationRejectsUnavailableOrMismatchedOwner":        "exerciseTransferRefusals",
	"internal/filetransfer/transfer_lifecycle_test.go:TestTransferLifecycleNoLeaks":                                       "exerciseTransferLifecycle",
	"internal/jsstream/object_reader_lifecycle_test.go:TestTransferDirtyWindowSurvivesLastReaderClose":                    "exerciseTransferReaderDirtyWindow",
	"internal/jsstream/object_resume_test.go:TestTransferReaderExportsOnlyConfirmedSinkPrefix":                            "exerciseTransferReaderCheckpoint",
	"internal/jsstream/object_resume_test.go:TestTransferReaderRecoversHealthyConnectionDrops":                            "exerciseTransferReaderRecovery",
	"internal/jsstream/object_resume_test.go:TestTransferReaderRejectsChangedVersionAndInvalidCheckpoint":                 "exerciseTransferReaderValidation",
	"internal/jsstream/object_snapshot_test.go:TestTransferWriterSnapshotPreservesAdmissionAndOwnsBytes":                  "exerciseWriterSnapshot",
	"internal/jsstream/object_writer_leak_test.go:TestTransferLifecycleNoLeaks":                                           "exerciseTransferLifecycle",
	"internal/jsstream/object_writer_test.go:TestTransferPayloadGeometryAndRTTBudget":                                     "exerciseWriterRTT",
	"internal/jsstream/object_writer_throughput_test.go:TestTransferWriterThroughputAt100msWSSRTT":                        "exerciseWriterRTT",
	"internal/transferpath/transfer_atomic_leak_test.go:TestTransferLifecycleNoLeaks":                                     "exerciseTransferLifecycle",
	"internal/xferauth/adapters_test.go:TestTransferPermitPersistsOriginalAuthorityAcrossSigners":                         "exercisePermitAuthority",
	"internal/xferauth/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                    "exerciseTransferLifecycle",
	"internal/xferbridge/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                  "exerciseTransferLifecycle",
	"internal/xferbucket/binding_test.go:TestTransferLegacyConfigUsesSingleCoordinator":                                   "exerciseTransferLegacyConfig",
	"internal/xferbucket/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                  "exerciseTransferLifecycle",
	"internal/xferclient/inline_test.go:TestTransferClientInlinePreservesOriginalInvocation":                              "exerciseInlineInvocation",
	"internal/xferclient/inline_test.go:TestTransferClientPreparedEvidenceCannotBeReplaced":                               "exercisePreparedEvidence",
	"internal/xferclient/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                  "exerciseTransferLifecycle",
	"internal/xfercontrol/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                 "exerciseTransferLifecycle",
	"internal/xfercourier/lifecycle_test.go:TestTransferCourierFreezeAndStopJoinCallbacks":                                "exerciseCourierCallbackJoin",
	"internal/xfercourier/lifecycle_test.go:TestTransferCourierLifecycleNoLeak":                                           "exerciseTransferLifecycle",
	"internal/xfercourier/lifecycle_test.go:TestTransferCourierObservationsKeepProducerOrder":                             "exerciseCourierObservationOrder",
	"internal/xferfinal/finalizer_test.go:TestSingleStagedTerminalActuallyRetires":                                        "exerciseRealFinalizerRetirement",
	"internal/xferfinal/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                   "exerciseTransferLifecycle",
	"internal/xferfinal/orphan_test.go:TestTransferLegacyOrphanRetirementRequiresExactStorageAndCensus":                   "exerciseLegacyOrphanRetirement",
	"internal/xferfinal/tracker_storage_test.go:TestTransferTrackerPublishesCanonicalHistoryToRealStorage":                "exerciseTrackerStorage",
	"internal/xfergc/collector_storage_test.go:TestTransferCrossHomeGCPreservesLiveAndReapsOrphan":                        "exerciseStorageGC",
	"internal/xfergc/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                      "exerciseTransferLifecycle",
	"internal/xferhistory/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                 "exerciseTransferLifecycle",
	"internal/xferledger/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                  "exerciseTransferLifecycle",
	"internal/xferreceive/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                 "exerciseTransferLifecycle",
	"internal/xferreceive/receiver_test.go:TestTransferSyncedPrefixSurvivesShortWriteAndCut":                              "exerciseProductionReceiver",
	"internal/xfersend/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                    "exerciseTransferLifecycle",
	"internal/xfersend/sender_test.go:TestTransferSourceMutationNeverPublishesMetadata":                                   "exerciseSourceMutationStorage",
	"internal/xfertrack/grant_test.go:TestTransferReceiverCannotLandAfterBrokerFailure":                                   "exerciseReceiverDependency",
	"internal/xfertrack/grant_test.go:TestTransferReceiverEvidenceAdvancesOnRealLedger":                                   "exerciseDurableReceiverEvidence",
	"internal/xfertrack/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                   "exerciseTransferLifecycle",
	"internal/xferworker/lifecycle_test.go:TestTransferLifecycleNoLeaks":                                                  "exerciseTransferLifecycle",
	"test/security/transfer_continuation_test.go:TestTransferContinuationSettlementNATSPermissions":                       "exerciseTransferSettlementNATSPermissions",
	"test/transferharness/harness_test.go:TestTransferLifecycleNoLeaks":                                                   "exerciseTransferLifecycle",

	// origin: hot-update S8 plan §11.2 — upgrade orchestration owners (P02–P11/P18/P21/P25).
	"internal/agentupgrade/rollback_test.go:TestAgentEngineLifecycleJoinsWatchdogAndPreservesFDs":         "exerciseAgentEngine",
	"internal/brokerupgrade/engine_test.go:TestBrokerUpgradeEngineLeakCycles":                             "exerciseBrokerUpgradeEngine",
	"internal/brokerupgrade/rollback_test.go:TestBrokerRollbackEngineLeakCycles":                          "exerciseBrokerRollbackEngine",
	"internal/upgradeartifact/publish_test.go:TestPublicationReleasesFilesAndSupervisedChildren":          "exercisePublicationCleanup",
	"internal/upgradeartifact/resolve_test.go:TestReadOnlyRollbackColdBootUpgrade":                        "exerciseReadOnlyRollbackColdBootUpgrade",
	"internal/upgradeauthority/sql_test.go:TestCreationSQLReadersReleaseResources":                        "exerciseCreationSQL",
	"internal/upgradecli/commands_test.go:TestUpgradeStageLifecycleNoLeaks":                               "exerciseStageLifetime",
	"internal/upgradecli/commands_test.go:TestUpgradeWaitCancellationStopsQueries":                        "exerciseWaitCancellation",
	"internal/upgradecut/take_test.go:TestAdjunctFailedTakeCannotUnlockParent":                            "exerciseAdjunctBorrowedCleanup",
	"internal/upgradecut/take_test.go:TestAdjunctTakeKeepsLeaseThroughAuxiliaryCleanup":                   "exerciseAdjunctTakeCleanup",
	"internal/upgradecut/take_test.go:TestAdjunctTakePreservesFailedCustody":                              "exerciseAdjunctTakeRefusal",
	"internal/upgradelegacy/proxy_test.go:TestLegacyProxyLifecycleNoLeaks":                                "exerciseLegacyProxyLifecycle",
	"internal/upgradelock/domain_test.go:TestDomainIdentitySurvivesBinaryReplacement":                     "exerciseDomainReplacement",
	"internal/upgradelock/lease_test.go:TestAdoptRequiresOriginalOFDAndPreservesRefusedCustody":           "exerciseAdoptLease",
	"internal/upgradelock/lease_test.go:TestBorrowedLeaseClosePreservesParentReservation":                 "exerciseBorrowedLease",
	"internal/upgradelock/lease_test.go:TestExecPreservesDomainLeaseThroughCommit":                        "exerciseLeaseExec",
	"internal/upgradelock/lease_test.go:TestJournalGuardUsesSeparateInodeAndSerializes":                   "exerciseJournalGuard",
	"internal/upgradeobserve/registry_test.go:TestRegistryAdmissionSequencer":                             "exerciseAdmissionSequencer",
	"internal/upgradeobserve/registry_test.go:TestRegistrySealSerializesAdmission":                        "exerciseSealAdmission",
	"internal/upgraderoll/resume_test.go:TestDriverKeeperStopsOnEveryExit":                                "exerciseDriverKeeper",
	"internal/upgraderoll/resume_test.go:TestReservationKeeperJoinsInflightRenewal":                       "exerciseKeeperJoin",
	"internal/upgradestore/store_test.go:TestJournalConcurrentWritersReleaseDescriptors":                  "exerciseConcurrentJournal",
	"test/hotupdate/testsupervisor/supervisor_test.go:TestChildSupervisorJoinsCancelledAndFailedCommands": "exerciseChildSupervisorCommands",
	"test/hotupdate/testsupervisor/supervisor_test.go:TestChildSupervisorSurvivesTestTimeout":             "exerciseChildSupervisor",
	"test/hotupdate/transaction_crash_test.go:TestActionCrashRecovery":                                    "exerciseTransactionActionCrashRecovery",
	"test/hotupdate/transaction_crash_test.go:TestStartNeverRecreatesExpiredGrant":                        "exerciseTransactionGrantReplay",
	"test/hotupdate/transaction_crash_test.go:TestTransactionCrashInteractions":                           "exerciseTransactionCrashInteractions",
	"test/hotupdate/transaction_faults_test.go:TestTransactionFaultManifestComplete":                      "exerciseTransactionFaultManifest",
	"test/hotupdate/transaction_faults_test.go:TestTransactionFaultReceipts":                              "exerciseTransactionFaultReceipts",
	"test/hotupdate/transaction_mutants_test.go:TestTransactionMutantReceipts":                            "exerciseTransactionMutantReceipts",
	// origin: hot-update S8 plan §5.2 / §11.2 P20 — five supervised retrofit runs that must never start tether.
	"test/p10/install_sh_test.go:TestInstallShDoesNotForkTether": "exerciseRetrofitNeverStarts",

	// origin: hot-update S9 plan §12 / §4.5 / §9.5 — infra, ingress and planned-recovery owners.
	"internal/caddyconfig/admin_test.go:TestCaddyAdminLifecycle":                      "exerciseCaddyAdmin",
	"internal/connrecovery/teardown_test.go:TestPlannedRecoveryLifecycle":             "exercisePlannedRecovery",
	"internal/infraartifact/probe_test.go:TestInfraArtifactLifecycle":                 "exerciseArtifact",
	"internal/infraauth/owner_test.go:TestInfraAuthLifecycle":                         "exerciseAuth",
	"internal/infracontract/contractstub/stub_test.go:TestInfraContractStubLifecycle": "exerciseContractStub",
	"internal/infrahost/acme_test.go:TestACMELifecycle":                               "exerciseACME",
	"internal/infrahost/unit_startup_test.go:TestInfraUnitLifecycle":                  "exerciseUnit",
	"internal/infrahost/census_test.go:TestInfraCensusLifecycle":                      "exerciseCensus",
	"internal/infrahost/identity_test.go:TestInfraIdentityLifecycle":                  "exerciseIdentity",
	"internal/infrahost/worker_test.go:TestInfraHostLifecycle":                        "exerciseHost",
	"internal/infralaunch/launch_test.go:TestInfraLaunchLifecycle":                    "exerciseLaunch",
	"internal/inframaint/hold_test.go:TestInfraMaintenanceLifecycle":                  "exerciseMaintenance",
	"internal/infrapriv/ipc_lifecycle_test.go:TestInfraIPCLifecycle":                  "exerciseIPC",
	"internal/infrareadiness/readiness_test.go:TestInfraReadinessLifecycle":           "exerciseReadiness",
	"internal/infratransport/move_test.go:TestInfraTransportLifecycle":                "exerciseTransport",
	"internal/infraupgrade/caddy_reload_test.go:TestCaddyDriverLifecycle":             "exerciseCaddyDriver",
	"internal/infraupgrade/nats_lifecycle_test.go:TestNATSDriverLifecycle":            "exerciseNATSDriver",
	"internal/ingressmove/move_lifecycle_test.go:TestIngressMoveLifecycle":            "exerciseMove",
	"internal/ingressswitch/rules_test.go:TestIngressSwitchLifecycle":                 "exerciseIngress",
	"internal/natsconf/system_reload_test.go:TestSystemPrincipalLifecycle":            "exerciseSystemPrincipal",
	"internal/proxydial/proxydial_test.go:TestPlannedProxyCancellationLifecycle":      "exercisePlannedProxy",
	"test/infraharness/process_test.go:TestInfraHarnessLifecycle":                     "exerciseHarness",
	"test/infraharness/process_test.go:TestInfraSupervisorReapsAfterTestParentDeath":  "exerciseParentDeath",

	// origin: hot-update S10 plan §6 / §11.2 — fullstack harness owners (P04/P05/LINT-s10).
	"test/fullstackharness/cmd/supervise/main_linux_test.go:TestSupervisorReturnClosesReadyDescriptor": "exerciseSupervisorReturn",
	"test/fullstackharness/identity_linux_test.go:TestFullstackExecutableReadRejectsExecABA":           "exerciseExecutableRead",
	"test/fullstackharness/identity_ofd_linux_test.go:TestFullstackOFDWitnessRejectsReopen":            "exerciseOFDWitness",
	"test/fullstackharness/process_test.go:TestFullstackSupervisorReapsOnOwnerDeath":                   "exerciseSupervisedProcess",

	// origin: hot-update S7 plan §11.3 / T52; CUT-CAPACITY, MAKETEST and S8 legacy-route owner requests.
	"test/hotupdate/transfer_transport_test.go:TestTransferCarrierProxyJoinsEverySocket":                               "exerciseTransferCarrierProxy",
	"test/hotupdate/transfer_transport_test.go:TestTransferSameStoreRestartFencesAuthenticationAndKeeps409":            "exerciseTransferRestartAuthentication",
	"internal/cli/transfer_legacy_compatibility_test.go:TestTransferLegacyPullRejectsExistingDestinationBeforePrepare": "exerciseLegacyExistingDestination",
	"internal/xferserve/status_origin_test.go:TestTransferStatusResolvesRetainedLocalPermit":                           "exerciseRetainedLocalStatus",
	"internal/infraattach/observation_test.go:TestAttachmentObservationExportsOriginalConnection":                      "exerciseAttachmentExport",
	"internal/drillobserve/infrastructure_linux_test.go:TestInfrastructureCustodyChallengeSealsCompleteRoster":         "exerciseInfrastructureCustody",
	"internal/upgradecli/stage_latency_test.go:TestStagePublicationOverlapsImagesAndJoinsChecks":                       "exerciseStagePublicationOverlap",
	"test/clusterharness/broker_product_barrier_linux_test.go:TestBrokerPreparedBarrierReportsReloadRefusal":           "exerciseBrokerPreparedBarrierRefusal",
	"test/chaos/disk_failure_test.go:TestAgentUpgradeRequiresTransactionBeforeURLFetch":                                "exerciseAgentUpgradeUnfencedURL",
	"test/security/path_traversal_test.go:TestUpgradeTarballRelativePathEntryRejected":                                 "exerciseUpgradeTarballRejected",
	"test/security/path_traversal_test.go:TestUpgradeTarballAbsolutePathEntryRejected":                                 "exerciseUpgradeTarballRejected",
	"test/security/path_traversal_test.go:TestUpgradeTarballSymlinkEntryRejected":                                      "exerciseUpgradeTarballRejected",
	"test/security/path_traversal_test.go:TestUpgradeTarballHardlinkEntryRejected":                                     "exerciseUpgradeTarballRejected",
	"test/security/path_traversal_test.go:TestUpgradeTarballMultipleEntriesOnlyTetherAccepted":                         "exerciseUpgradeTarballAccepted",
	"test/security/path_traversal_test.go:TestUpgradeTarballOnlyTraversalEntriesNoTetherRejected":                      "exerciseUpgradeTarballRejected",
	"test/cli_e2e/node_upgrade_image_test.go:TestNodeUpgradeCarriesLiveExecAcrossExec":                                 "exerciseNodeUpgradeLiveExec",
	"test/cli_e2e/node_upgrade_image_test.go:TestNodeUpgradeGenerationChainKeepsLiveExec":                              "exerciseNodeUpgradeLiveExec",
	"test/cli_e2e/node_upgrade_image_test.go:TestNodeUpgradeRefusesLegacyCandidateWithLiveTasks":                       "exerciseNodeUpgradeLegacyCandidate",
	"test/p9/admin_request_validation_test.go:TestAdminRejectsMalformedRequest":                                        "exerciseAdminMalformedRequest",
	// origin: hot-update S7 plan §11.3 / T17; S10 plan §11.2 / P29 — verified direct five-round exercises.
	"internal/cluster/sole_voter_campaign_test.go:TestImageSoleVoterCampaignPrecedesHeartbeat":                      "exerciseImageCampaignElectionCause",
	"internal/drillobserve/runtime_sample_receive_test.go:TestRuntimeSampleCustodyRetriesInterruptedReceive":        "exerciseRuntimeInterruptedReceive",
	"internal/xferimage/worker_codec_test.go:TestTransferWorkerImageCodecMatchesDescriptors":                        "exerciseWorkerCodecStateBuilders",
	"test/clusterharness/broker_cohort_payload_linux_test.go:TestBrokerCohortPayloadKeepsItsIndependentBytePattern": "exerciseBrokerCohortPayload",
	"test/hotupdate/compatibility_evidence_test.go:TestUpgradeCompatibilityRequiresSuppliedEvidence":                "exerciseUpgradeCompatibilityEvidenceAdmission",
}

func TestEveryLeakAssertionExercisesItsSubjectRepeatedly(t *testing.T) {
	root := repoRoot(t)

	var offenders []string
	seenAnchors := map[string]bool{}
	scanned := 0
	err := walkTestFilesForLeakShape(t, root, func(rel string, fn *ast.FuncDecl) {
		assertPos, calls := leakAssertCallPos(fn.Body)
		if !calls {
			return
		}
		scanned++
		key := rel + ":" + fn.Name.Name
		// THE EXEMPTION IS CHECKED FIRST — origin: prerelease audit round 2, CC-2.
		//
		// It used to be consulted only AFTER the anchor lookup, which made it unreachable
		// for the class it was written for. An exemption is a claim that the site has no
		// subject to exercise repeatedly; a site with no subject cannot name an anchor
		// call either, so it was reported as "no site-scoped exercise anchor" and the
		// exemption never applied. TestSingleExerciseExemptionsAreLiveAndSiteScoped still
		// requires every entry to name a real test that really calls a leak helper, so
		// this loosens nothing that was previously enforced — it makes an escape hatch
		// that the gate documents actually usable.
		if _, exempt := singleExerciseExemptions[key]; exempt {
			return
		}
		anchor := leakExerciseAnchors[key]
		if anchor == "" {
			offenders = append(offenders, key+" (no site-scoped exercise anchor)")
			return
		}
		seenAnchors[key] = true
		if qualifyingLoopBefore(fn.Body, assertPos, anchor) {
			return
		}
		offenders = append(offenders, key+" (missing looped call "+anchor+")")
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// NON-VACUITY over the live tree, and here that is LEGITIMATE: this gate's success state still
	// contains leak assertions (unlike the naming freeze, whose success state empties the thing it
	// counts — see docs/testing-standards.md G2b). If the scan stops finding them, it has gone blind: a
	// renamed helper, a moved package, or a walk that no longer reaches the suites.
	if scanned < 8 {
		t.Fatalf("found only %d test(s) calling a leak-assertion helper; expected at least 8. The scan has "+
			"gone blind (helper renamed, package moved, or the walk stopped reaching the suites) — which "+
			"would make every assertion below vacuous.", scanned)
	}
	for key, anchor := range leakExerciseAnchors {
		if !seenAnchors[key] {
			t.Errorf("leakExerciseAnchors lists %s -> %s, but that test no longer has a live leak assertion; "+
				"remove or update the stale anchor in the same change", key, anchor)
		}
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d leak assertion(s) do not exercise their subject in a qualifying loop:\n  %s\n\n"+
			"The tolerance is ±2 by derivation (see internal/testharness/leakgate.go), so a subject "+
			"exercised once can leak up to two goroutines per exercise and stay green forever. At N "+
			"exercises a per-exercise leak of 1 shows up as +N.\n"+
			"A qualifying loop (a) runs >= %d times with a bound this gate can resolve from the same "+
			"function — an int literal, a local const, the leakExerciseRounds constant, or the length of "+
			"a locally-built slice — and (b) does NOT contain the leak assertion itself. A "+
			"`for _, tc := range cases { t.Run(... assert ...) }` fails (b): it loops SUB-CASES, and each "+
			"one still exercises the subject exactly once.\n"+
			"Wrap the subject in such a loop, or add an entry to singleExerciseExemptions WITH a reason.",
			len(offenders), strings.Join(offenders, "\n  "), leakExerciseRoundsFloor)
	}
}

// TestSingleExerciseExemptionsAreLiveAndSiteScoped keeps the exemption list from rotting into a
// permanent allow-everything. Every key must name a test function that EXISTS and that actually calls a
// leak helper — otherwise the entry is decoration, and its reason is being trusted for nothing.
//
// This is not hypothetical bookkeeping: the list's only historical entry keyed a HELPER
// (internal/cluster/transport_test.go:waitGoroutineBaseline) while the scan only ever iterates Test*
// functions, so it could never match. It read as a considered exemption for two batches.
// leakGuardNames are the identifiers that count as "this package has a goroutine leak guard at all"
// for TestLeakGateCoversRiskyPackages. It is leakAssertHelpers PLUS waitGoroutineBaseline: the shape
// gate above deliberately excludes waitGoroutineBaseline because it is a ceiling, not an
// exercise-N-times assertion — but as a COVERAGE question ("does internal/cluster have anything that
// fails when goroutines do not return after Shutdown?") it is exactly that thing, and it is what
// transport_test.go relies on. Two gates, two questions, two lists.
var leakGuardNames = map[string]bool{
	"assertNoGoroutineLeak": true, "assertNoFDLeak": true,
	"AssertNoGoroutineLeak": true, "AssertNoFDLeak": true,
	"pollGoroutinesAtMost": true, "waitGoroutineBaseline": true,
}

// riskyImportPaths are the imports that make a package a leak risk: a raft node, a tunnel, a PTY
// or a yamux session each own goroutines that outlive the call that created them.
var riskyImportPaths = []string{
	`"github.com/hashicorp/raft"`,
	`"github.com/hashicorp/yamux"`,
	`"github.com/LinZiyang666/tether/internal/tunnel"`,
	`"github.com/LinZiyang666/tether/internal/pty"`,
}

// origin: hot-update S5 plan §8 / §10.2 — flow deliberately does not import
// yamux. Socket, sealed-FD and image owners remain risks even when layering
// removes every import that the historical scanner used to discover them.
var flowSSLeakRiskPackages = []string{
	"internal/rawsocket", "internal/sealedstate", "internal/flow", "internal/tunnel",
	"internal/agent/ssproxy", "internal/agentimage", "internal/agentboot", "test/hotupdate",
}

// leakCoverageExceptions: risky packages allowed to have no leak guard, each with a reason. Empty on
// the day the gate landed: internal/broker gained two shared-helper assertions (converted from inline
// polls), internal/clusteroffline gained a five-round restart test, internal/cluster is covered by
// waitGoroutineBaseline, internal/agent and internal/tunnel already had shared-helper assertions.
var leakCoverageExceptions = map[string]string{}

// leakCoverageExceptionsCap pins the table both ways (internal review L1-F8). Package-keyed: an
// entry covers every future test in that package, which is why the cap moves only with a reason.
const leakCoverageExceptionsCap = 0

// TestLeakGateCoversRiskyPackages: every production package that imports a risky dependency has at
// least one test file calling a leak guard. CLAUDE.md §5 says "触碰隧道/PTY/reconcile/传输/Raft 必须带
// -race + 泄漏门"; on 2026-09-01 three of the five packages that import those things (broker, cluster
// as counted by the shape gate, clusteroffline) had no call the gate could see. The rule was prose.
// origin: docs/reviews/test-system-overhaul-plan.md B5 (distributed D3).
//
// gate-control: TestLeakCoveragePredicateSeesTheShapes
func TestLeakGateCoversRiskyPackages(t *testing.T) {
	root := repoRoot(t)
	if n := len(leakCoverageExceptions); n != leakCoverageExceptionsCap {
		t.Errorf("leakCoverageExceptions has %d entries, cap says %d — move the cap in the same change, with the reason", n, leakCoverageExceptionsCap)
	}
	risky := map[string]bool{}
	guarded := map[string]bool{}
	for _, pkg := range append(append([]string(nil), flowSSLeakRiskPackages...), brokerLeakRiskPackages...) {
		risky[pkg] = true
	}
	for _, pkg := range transferLeakRiskPackages {
		risky[pkg] = true
	}
	for _, top := range []string{"cmd", "internal", "test/hotupdate", "test/clusterharness", "test/cli_e2e", "test/p13", "test/transferharness"} {
		// Missing implementations stay uncovered, rather than turning a not-yet
		// landed test package into an exemption from its required leak guard.
		if _, err := os.Stat(filepath.Join(root, top)); os.IsNotExist(err) && (top == "test/hotupdate" || top == "test/transferharness") {
			continue
		}
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if n := d.Name(); n == "testdata" || n == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") {
				return nil
			}
			src, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			rel, _ := filepath.Rel(root, filepath.Dir(p))
			pkg := filepath.ToSlash(rel)
			if strings.HasSuffix(p, "_test.go") {
				if leakGuardCalled(string(src)) {
					guarded[pkg] = true
				}
				return nil
			}
			if importsAnyRisky(string(src)) {
				risky[pkg] = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	if len(risky) < 4 {
		t.Fatalf("only %d risky package(s) found (%v) — the import scan has gone blind", len(risky), risky)
	}
	var uncovered []string
	for pkg := range risky {
		if guarded[pkg] {
			continue
		}
		if _, ok := leakCoverageExceptions[pkg]; ok {
			continue
		}
		uncovered = append(uncovered, pkg)
	}
	sort.Strings(uncovered)
	if len(uncovered) > 0 {
		t.Errorf("%d package(s) import raft/yamux/tunnel/pty or own flow/SS resources but no test in them calls a leak guard:\n  %s\n\n"+
			"Add a leak assertion (internal/testharness.AssertNoGoroutineLeak, in a >=5-round exercise loop) "+
			"to a test that starts and stops the thing, or register the package in leakCoverageExceptions "+
			"WITH a reason.", len(uncovered), strings.Join(uncovered, "\n  "))
	}
	for pkg := range leakCoverageExceptions {
		if !risky[pkg] {
			t.Errorf("leakCoverageExceptions names %s, which imports nothing risky — delete the entry", pkg)
		}
		if guarded[pkg] {
			t.Errorf("leakCoverageExceptions names %s, which now has a leak guard — delete the entry", pkg)
		}
	}
}

// origin: hot-update S5 plan §10.1 / T49 — the independent set and parsed
// bodies make deletion of a risk row or of the measured exercise fail closed.
func TestFlowSSLeakRiskControls(t *testing.T) {
	want := strings.Fields("internal/rawsocket internal/sealedstate internal/flow internal/tunnel internal/agent/ssproxy internal/agentimage internal/agentboot test/hotupdate")
	got := append([]string(nil), flowSSLeakRiskPackages...)
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("flow/SS leak-risk registration = %v, want %v", got, want)
	}
	const body = `before := runtime.NumGoroutine()
	for round := 0; round < 5; round++ { exerciseFlow(t) }
	testharness.AssertNoGoroutineLeak(t, "flow", before)`
	for _, tc := range []struct {
		name, body, anchor string
		want               bool
	}{
		{"measured", body, "exerciseFlow", true},
		{"missing anchor", body, "missingExercise", false},
		{"four rounds", strings.Replace(body, "round < 5", "round < 4", 1), "exerciseFlow", false},
		{"exercise removed", strings.Replace(body, "exerciseFlow(t)", "", 1), "exerciseFlow", false},
		{"function value", strings.Replace(body, "exerciseFlow(t)", "_ = exerciseFlow", 1), "exerciseFlow", false},
		{"deferred assertion after five subtests", `before := runtime.NumGoroutine()
 defer testharness.AssertNoGoroutineLeak(t,"flow",before)
 for round:=0;round<5;round++ { t.Run("round",func(t *testing.T){exerciseFlow(t)}) }`, "exerciseFlow", true},
		{"deferred assertion with four subtests", `before := runtime.NumGoroutine()
 defer testharness.AssertNoGoroutineLeak(t,"flow",before)
 for round:=0;round<4;round++ { t.Run("round",func(t *testing.T){exerciseFlow(t)}) }`, "exerciseFlow", false},
		{"deferred assertion with uncalled exercise", `before := runtime.NumGoroutine()
 defer testharness.AssertNoGoroutineLeak(t,"flow",before)
 for round:=0;round<5;round++ { t.Run("round",func(t *testing.T){_ = exerciseFlow}) }`, "exerciseFlow", false},
		{"deferred per-subtest assertions", `for round:=0;round<5;round++ {
 t.Run("round",func(t *testing.T){before:=runtime.NumGoroutine();defer testharness.AssertNoGoroutineLeak(t,"flow",before);exerciseFlow(t)}) }`, "exerciseFlow", false},
		{"deferred assertion before parallel subtests", `before:=runtime.NumGoroutine()
 defer testharness.AssertNoGoroutineLeak(t,"flow",before)
 for round:=0;round<5;round++ {t.Run("round",func(t *testing.T){t.Parallel();exerciseFlow(t)})}`, "exerciseFlow", false},
		{"deferred assertion before unjoined goroutines", `before:=runtime.NumGoroutine()
 defer testharness.AssertNoGoroutineLeak(t,"flow",before)
 for round:=0;round<5;round++ {go exerciseFlow(t)}`, "exerciseFlow", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "flow_test.go", "package flow\nfunc TestFlow(t *testing.T) {"+tc.body+"}", 0)
			if err != nil {
				t.Fatal(err)
			}
			fn := f.Decls[0].(*ast.FuncDecl)
			pos, calls := leakAssertCallPos(fn.Body)
			if !calls {
				t.Fatal("control did not reach the leak assertion")
			}
			if got := qualifyingLoopBefore(fn.Body, pos, tc.anchor); got != tc.want {
				t.Fatalf("measured exercise qualifies = %v, want %v", got, tc.want)
			}
		})
	}
}

func importsAnyRisky(src string) bool {
	for _, imp := range riskyImportPaths {
		if strings.Contains(src, imp) {
			return true
		}
	}
	return false
}

// leakGuardCalled reports whether some `Test*` function in src calls a leak guard — inside the
// test's own body (closures included), never in a helper. The first version accepted a call
// anywhere in the file, so a guard parked in a helper nobody calls, or in a test-shaped function
// with a lower-case tail, counted as coverage (internal review L1-F10). Reachability inside the
// test body (an `if false`) is not judged: that is a lie of a different kind, and a reviewer's.
func leakGuardCalled(src string) bool {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, 0)
	if err != nil {
		return false
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Body == nil || !isTestFuncName(fd.Name.Name) || !strings.HasPrefix(fd.Name.Name, "Test") {
			continue
		}
		found := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				if leakGuardNames[fn.Name] {
					found = true
				}
			case *ast.SelectorExpr:
				if leakGuardNames[fn.Sel.Name] {
					found = true
				}
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func TestLeakCoveragePredicateSeesTheShapes(t *testing.T) {
	if !importsAnyRisky("package x\nimport \"github.com/hashicorp/raft\"\n") {
		t.Fatal("raft import not seen as risky")
	}
	if importsAnyRisky("package x\nimport \"github.com/hashicorp/go-hclog\"\n") {
		t.Fatal("unrelated import read as risky")
	}
	for _, src := range []string{
		"package x\nfunc TestA(t *testing.T) { testharness.AssertNoGoroutineLeak(t, \"a\", 0) }\n",
		"package x\nfunc TestB(t *testing.T) { assertNoFDLeak(t, \"b\", 0, 0) }\n",
		"package x\nfunc TestC(t *testing.T) { waitGoroutineBaseline(t, 0, 0) }\n",
	} {
		if !leakGuardCalled(src) {
			t.Fatalf("guard call not seen in %q", src)
		}
	}
	if leakGuardCalled("package x\nfunc TestD(t *testing.T) { _ = runtime.NumGoroutine() }\n") {
		t.Fatal("a bare NumGoroutine read is not a guard")
	}
	// Coverage is a property of TESTS: a guard in a helper nobody calls, or in a function whose name
	// go test would not run, is not coverage (internal review L1-F10).
	for _, src := range []string{
		"package x\nfunc leakCheck(t *testing.T) { testharness.AssertNoGoroutineLeak(t, \"a\", 0) }\nfunc TestE(t *testing.T) {}\n",
		"package x\nfunc Testify(t *testing.T) { testharness.AssertNoGoroutineLeak(t, \"a\", 0) }\n",
	} {
		if leakGuardCalled(src) {
			t.Fatalf("a guard outside a Test body counted as coverage: %q", src)
		}
	}
	// …but a guard inside a closure within the test body is the test's own call.
	if !leakGuardCalled("package x\nfunc TestF(t *testing.T) { t.Cleanup(func() { assertNoFDLeak(t, \"f\", 0, 0) }) }\n") {
		t.Fatal("a guard in a closure inside the test body must count")
	}
}

func TestSingleExerciseExemptionsAreLiveAndSiteScoped(t *testing.T) {
	root := repoRoot(t)

	live := map[string]bool{}
	err := walkTestFilesForLeakShape(t, root, func(rel string, fn *ast.FuncDecl) {
		if _, calls := leakAssertCallPos(fn.Body); calls {
			live[rel+":"+fn.Name.Name] = true
		}
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	for key, reason := range singleExerciseExemptions {
		if !strings.Contains(key, ":Test") {
			t.Errorf("exemption key %q does not name a Test function. The scan iterates Test* functions "+
				"only, so a key naming a helper can never match and the exemption is dead. If the point "+
				"is that a HELPER is not an assertion, remove it from leakAssertHelpers instead.", key)
			continue
		}
		if !live[key] {
			t.Errorf("exemption %q (%q) names no live leak assertion — the test was renamed, deleted, or "+
				"no longer calls a leak helper. Remove the entry in the same commit; a stale exemption is "+
				"how an allow-list decays into an allow-everything.", key, reason)
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("exemption %q carries no reason. An exemption without one is just a slower absence "+
				"of a gate (docs/testing-standards.md G3).", key)
		}
	}
}

// TestQualifyingLoopPredicateDiscriminates is the self-check, over SYNTHESIZED sources. It has to be
// synthesized: the tree's success state is "every call site already qualifies", so there is no real
// negative example to point at, and a non-vacuity assertion made against real input would fail exactly
// when the codebase is correct (docs/testing-standards.md G2/G2b).
//
// The four rows are the four ways this predicate can be wrong, and the SUB-CASE row is the one the
// mutation-honesty audit found the first version getting wrong.
func TestQualifyingLoopPredicateDiscriminates(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
		why  string
	}{
		{
			name: "literal-bound loop before the assertion",
			body: `before := 0
	for i := 0; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why:  "the canonical shape: N exercises, then one assertion",
		},
		{
			name: "leakExerciseRounds-bound loop before the assertion",
			body: `before := 0
	for i := 0; i < leakExerciseRounds; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why:  "the constant is the preferred spelling and must be accepted",
		},
		{
			name: "SUB-CASE loop that CONTAINS the assertion",
			body: `for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := 0
			exercise()
			assertNoGoroutineLeak(t, tc.name, before)
		})
	}`,
			want: false,
			why: "THE LOAD-BEARING ROW. This is a loop of baseline-exercise-assert triples: each subtest " +
				"exercises the subject exactly once and the ±2 tolerance swallows a per-exercise leak in " +
				"every one of them. The first version of this gate accepted it because it only asked " +
				"'is there a loop anywhere in the body'",
		},
		{
			name: "loop that runs fewer than the floor",
			body: `before := 0
	for i := 0; i < 2; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why:  "2 exercises against a ±2 tolerance still cannot separate a per-exercise leak of 1",
		},
		{
			name: "range-over-int with a local const (the tunnel suites' spelling)",
			body: `before := 0
	const K = 20
	for range K {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "correct code the first version of this predicate could not read. A gate that forces a " +
				"rewrite of correct code teaches people to edit the gate",
		},
		{
			name: "range over a slice built with make(_, leakExerciseRounds)",
			body: `before := 0
	ports := make([]int, leakExerciseRounds)
	for _, p := range ports {
		exercise(p)
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "TestTunnelServerCloseWithActiveSessionNoLeak's real shape: the round count IS the slice " +
				"length, and it is visible one line up",
		},
		{
			name: "range over a slice built with make(_, 2)",
			body: `before := 0
	ports := make([]int, 2)
	for _, p := range ports {
		exercise(p)
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "resolving the length must not mean ACCEPTING it — 2 exercises against a ±2 tolerance " +
				"still proves nothing",
		},
		{
			name: "range over an opaque slice",
			body: `before := 0
	for _, p := range portsFromSomewhereElse() {
		exercise(p)
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "'it happens to have 6 entries today' is not a property a reader can check; name the " +
				"count or use the constant",
		},
		{
			name: "loop AFTER the assertion",
			body: `before := 0
	exercise()
	assertNoGoroutineLeak(t, "x", before)
	for i := 0; i < 10; i++ {
		cleanup()
	}`,
			want: false,
			why:  "a loop that runs after the assertion exercised nothing before it",
		},
		{
			name: "unrelated loop before the leak baseline",
			body: `for i := 0; i < 5; i++ {
		unrelatedSetup()
	}
	before := 0
	exercise()
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a loop that completes before the leak baseline cannot contribute to the measured " +
				"delta, so accepting it makes the strengthened leak gate false-green",
		},
		{
			name: "loop bound is five but starts at four",
			body: `before := 0
	for i := 4; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "the condition's upper bound is not the iteration count; this loop exercises the " +
				"subject once and remains hidden by the +/-2 noise floor",
		},
		// The rest of the derivation, added alongside the two reviewer rows above. Each is a way the
		// three clauses can disagree with the bound — the class the reviewer's row belongs to.
		{
			name: "non-unit step: bound 10 but only 5 iterations",
			body: `before := 0
	for i := 0; i < 10; i += 2 {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why:  "10/2 == 5, which meets the floor; deriving the count has to work in both directions",
		},
		{
			name: "non-unit step that falls SHORT: bound 8, step 2, 4 iterations",
			body: `before := 0
	for i := 0; i < 8; i += 2 {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "reading the bound (8) would accept this; the count is 4 and a per-exercise leak of 1 " +
				"is still inside the ±2 floor at four exercises",
		},
		{
			name: "no initializer: start value unknowable",
			body: `before := 0
	i := someStartingPoint()
	for ; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why:  "`for ; i < 5; i++` could start at 4 or at -100; the bound alone proves nothing",
		},
		{
			name: "condition tests a different variable than the initializer sets",
			body: `before := 0
	for i := 0; j < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why:  "three clauses that do not agree on a variable are not a derivable count",
		},
		{
			name: "body can break out early",
			body: `before := 0
	for i := 0; i < 5; i++ {
		if done() {
			break
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "with a break the written bound is an UPPER limit, not the number of exercises that " +
				"happened — the loop may have run once",
		},
		{
			name: "a nested loop's break does not disqualify the outer loop",
			body: `before := 0
	for i := 0; i < 5; i++ {
		for {
			break
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "that break binds to the inner loop. Treating it as an early exit would reject correct " +
				"code and push people to edit the gate, which is how a gate dies",
		},
		{
			name: "a nested loop can break the labelled outer loop",
			body: `before := 0
outer:
	for i := 0; i < 5; i++ {
		for {
			exercise()
			break outer
		}
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a labelled break inside a nested loop binds to the OUTER loop. Pruning every nested " +
				"loop from the control-flow walk mistakes this one real exercise for five",
		},
		{
			name: "a return inside a switch exits the whole test",
			body: `before := 0
	for i := 0; i < 5; i++ {
		switch {
		case done():
			return
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a switch owns an unlabelled break, but it does not own return or goto. Pruning the whole " +
				"switch hides an early function exit and turns the written bound into a false count",
		},
		{
			name: "continue can bypass the exercise on every iteration",
			body: `before := 0
	for i := 0; i < 5; i++ {
		if skip() {
			continue
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "the loop can complete five iterations while exercising the subject zero times. The gate " +
				"is meant to prove repeated exercise, not merely repeated evaluation of a loop body",
		},
		{
			name: "an empty loop is not five exercises",
			body: `before := 0
	for i := 0; i < 5; i++ {
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a five-iteration loop is not evidence that the leak-producing subject ran at all. " +
				"Deleting the load-bearing call must not make a release gate greener",
		},
		{
			name: "an unrelated loop after the baseline is not five exercises",
			body: `before := 0
	for i := 0; i < 5; i++ {
		collectMetrics()
	}
	exercise()
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "position after the baseline proves only that the loop contributes to the measurement " +
				"window, not that it exercises the subject whose leak is asserted",
		},
		{
			name: "the baseline is re-taken AFTER a setup loop",
			body: `for i := 0; i < 5; i++ {
		unrelatedSetup()
	}
	before := 0
	for i := 0; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "the reviewer's anchor row with a REAL exercise loop after the baseline — anchoring must " +
				"reject the setup loop without also rejecting the genuine one that follows it",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "package p\n\nconst leakExerciseRounds = 5\n\nfunc TestSynthetic(t *testing.T) {\n\t" +
				tc.body + "\n}\n"
			f, err := parser.ParseFile(token.NewFileSet(), "synthetic_test.go", src, 0)
			if err != nil {
				t.Fatalf("parse: %v\n%s", err, src)
			}
			var fn *ast.FuncDecl
			for _, d := range f.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "TestSynthetic" {
					fn = fd
				}
			}
			if fn == nil {
				t.Fatal("synthetic test function not found")
			}
			pos, calls := leakAssertCallPos(fn.Body)
			if !calls {
				t.Fatal("the synthetic body must call a leak helper, or this row tests nothing")
			}
			if got := qualifyingLoopBefore(fn.Body, pos, "exercise"); got != tc.want {
				t.Errorf("qualifies = %v, want %v.\nwhy it matters: %s", got, tc.want, tc.why)
			}
		})
	}
}

// TestLeakRoundsConstantMatchesTheFloor pins that this package's duplicated floor still equals the
// constant the suites actually loop on. Cross-package _test.go identifiers cannot be imported, so the
// number is re-read from source; a silent drift would make the gate demand a different N from the one
// the tests use.
func TestLeakRoundsConstantMatchesTheFloor(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "internal", "testharness", "leakgate.go")
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if name.Name != "LeakExerciseRounds" || i >= len(vs.Values) {
				continue
			}
			lit, ok := vs.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				continue
			}
			found = true
			v, _ := strconv.Atoi(lit.Value)
			if v != leakExerciseRoundsFloor {
				t.Errorf("testharness.LeakExerciseRounds = %d but this gate's floor is %d. They describe "+
					"the same obligation; a gate demanding an N the suites do not use is a gate nobody "+
					"can satisfy without editing the gate.", v, leakExerciseRoundsFloor)
			}
		}
		return true
	})
	if !found {
		t.Fatalf("could not find `LeakExerciseRounds = <int>` in %s — this cross-check has gone blind, "+
			"so the two numbers can now drift silently", path)
	}
}

// TestLeakAssertShapeGateSeesTheHistoricalShape is the mutation the FIRST design of this gate could not
// see, expressed as a permanent test rather than a one-off experiment. The four helper copies B9 deleted
// all wrote the baseline and the comparison on separate lines, which a single-expression matcher misses
// entirely — so this pins that the CALL-based scan sees a call with no qualifying loop around it.
func TestLeakAssertShapeGateSeesTheHistoricalShape(t *testing.T) {
	const src = `package p

func TestHistoricalShape(t *testing.T) {
	before := runtime.NumGoroutine()
	srv := start()
	srv.Close()
	assertNoGoroutineLeak(t, "historical", before)
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "synthetic_test.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		pos, calls := leakAssertCallPos(fd.Body)
		if !calls {
			t.Fatal("the historical shape must be RECOGNISED as a leak assertion; a scan that does not " +
				"see it reports the whole repo compliant")
		}
		if qualifyingLoopBefore(fd.Body, pos, "exercise") {
			t.Error("the historical single-exercise shape must NOT qualify — it is the exact shape this " +
				"gate was written to outlaw")
		}
	}
}

// walkTestFilesForLeakShape visits every Test* function in every _test.go file under root.
func walkTestFilesForLeakShape(t *testing.T, root string, visit func(rel string, fn *ast.FuncDecl)) error {
	t.Helper()
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			visit(rel, fn)
		}
		return nil
	})
}

// leakAssertCallPos reports the position of the FIRST leak-assertion call in body, and whether there is
// one at all. The first is the right one to anchor on: a qualifying loop must precede the earliest
// assertion, since anything after it cannot have contributed to that assertion's delta.
func leakAssertCallPos(body *ast.BlockStmt) (token.Pos, bool) {
	pos := token.NoPos
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		if leakAssertHelpers[name] && (pos == token.NoPos || call.Pos() < pos) {
			pos = call.Pos()
		}
		return true
	})
	return pos, pos != token.NoPos
}

// qualifyingLoopBefore reports whether body contains a loop that
//
//	(a) starts AFTER the leak baseline was captured, and before assertPos;
//	(b) does NOT contain assertPos;
//	(c) runs at least leakExerciseRoundsFloor times, derived from init/cond/post.
//	(d) contains the exact site-scoped exercise anchor.
//
// (b) rejects the table-driven shape, where the loop iterates sub-cases and each iteration does its own
// baseline-exercise-assert. (a) and the "derived" half of (c) are external review B2-5, and both were
// real false-greens in the first version:
//
//	(a) `for i := 0; i < 5; i++ { unrelatedSetup() }` THEN `before := ...` THEN one exercise. Five
//	    iterations, all of them finished before the baseline was taken, so none can contribute to the
//	    measured delta. The old predicate only asked "is there a loop before the assertion".
//	(b) `for i := 4; i < 5; i++` runs ONCE while the condition's upper bound reads as five. The old
//	    loopRounds returned the RHS of the condition, i.e. it reported the bound and called it a count.
//
// Both are now negative controls in TestQualifyingLoopPredicateDiscriminates and must stay there: they
// are the two ways this gate can claim compliance it has not verified.
func qualifyingLoopBefore(body *ast.BlockStmt, assertPos token.Pos, exerciseAnchor string) bool {
	env := intEnv(body)
	baseline, ok := leakBaselinePos(body, assertPos)
	if !ok {
		// FAIL CLOSED. Without a locatable baseline there is no way to tell an exercise loop from a setup
		// loop, and this gate's entire job is to not report unverified compliance. A test whose baseline
		// is not a plain assignment to a variable the assertion names can say so in
		// singleExerciseExemptions, with a reason.
		return false
	}
	// A top-level defer captures its baseline arguments here but invokes the
	// assertion on return, after all synchronous t.Run exercise rounds. Only
	// that direct shape gets end-of-body timing: a defer inside a subtest/loop
	// cannot stand in for one measured five-round lifecycle.
	// origin: hot-update S7 plan §11.2 / S7-GATES deferred leak receipt
	deferredAssertion := false
	for _, statement := range body.List {
		if deferred, ok := statement.(*ast.DeferStmt); ok && deferred.Call.Pos() == assertPos {
			assertPos = body.End()
			deferredAssertion = true
			break
		}
	}
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		var pos, end token.Pos
		switch loop := n.(type) {
		case *ast.ForStmt:
			pos, end = loop.Pos(), loop.End()
		case *ast.RangeStmt:
			pos, end = loop.Pos(), loop.End()
		default:
			return true
		}
		if pos < baseline {
			return true // finished before the measurement started: it cannot be the exercise
		}
		if pos > assertPos {
			return true // a loop that runs after the assertion exercised nothing before it
		}
		if pos < assertPos && assertPos < end {
			return true // the assertion is INSIDE this loop: sub-cases, not exercises
		}
		if loopRounds(n, env) >= leakExerciseRoundsFloor && loopContainsCall(n, exerciseAnchor) && (!deferredAssertion || !deferredLoopMayOutliveTest(n)) {
			found = true
			return false
		}
		return true
	})
	return found
}

// Parent defers run before parallel subtests start. Likewise, a go statement
// does not prove its exercise has joined before the deferred measurement.
// Keep the new timing rule limited to synchronously completed loop rounds.
// origin: hot-update S7 plan §11.2 / S7-GATES deferred leak receipt
func deferredLoopMayOutliveTest(loop ast.Node) bool {
	async := false
	ast.Inspect(loop, func(node ast.Node) bool {
		if _, ok := node.(*ast.GoStmt); ok {
			async = true
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Parallel" {
				async = true
			}
		}
		return !async
	})
	return async
}

func loopContainsCall(loop ast.Node, want string) bool {
	found := false
	ast.Inspect(loop, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		if name == want {
			found = true
			return false
		}
		return true
	})
	return found
}

// leakBaselinePos returns the position at which the leak baseline was captured — the end of the LAST
// assignment, before assertPos, to any variable the assertion call mentions.
//
// The assertion's own arguments are what identify the baseline: assertNoGoroutineLeak(t, label, before),
// assertNoFDLeak(t, label, fdBefore) and pollGoroutinesAtMost(baseline+2, d) all name it, the last one
// inside an expression. So every ident in the call's arguments is a candidate, and the newest assignment
// to any of them is the measurement point.
//
// Returns ok=false when no such assignment exists, which the caller treats as "unprovable" rather than
// as "no constraint".
func leakBaselinePos(body *ast.BlockStmt, assertPos token.Pos) (token.Pos, bool) {
	var call *ast.CallExpr
	ast.Inspect(body, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || c.Pos() != assertPos {
			return true
		}
		call = c
		return false
	})
	if call == nil {
		return token.NoPos, false
	}
	names := map[string]bool{}
	for _, arg := range call.Args {
		ast.Inspect(arg, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				names[id.Name] = true
			}
			return true
		})
	}
	best := token.NoPos
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Pos() > assertPos {
			return true
		}
		for _, lhs := range as.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok || !names[id.Name] {
				continue
			}
			if as.End() > best {
				best = as.End()
			}
		}
		return true
	})
	return best, best != token.NoPos
}

// intEnv resolves the integer names visible inside one test body, so loopRounds can read the spellings
// the repo actually uses instead of demanding one. Three sources, all local and all visible in the same
// diff as any change to them:
//
//	const K = 20                                  -> K = 20
//	x := make([]int, leakExerciseRounds)          -> len(x) = 5
//	xs := []T{a, b, c, d, e}                      -> len(xs) = 5
//
// Plus leakExerciseRounds / LeakExerciseRounds, the shared constant, which is defined in another package
// and pinned to this file's floor by TestLeakRoundsConstantMatchesTheFloor.
//
// Anything else resolves to 0 and is therefore REPORTED. That is the intended bias: an unresolvable
// bound is not proof of N>=5, and the fix is to name the count rather than to widen this function.
func intEnv(body *ast.BlockStmt) map[string]int {
	env := map[string]int{
		"leakExerciseRounds": leakExerciseRoundsFloor,
		"LeakExerciseRounds": leakExerciseRoundsFloor,
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.ValueSpec: // const K = 20 / var K = 20
			for i, name := range node.Names {
				if i < len(node.Values) {
					if v := intValue(node.Values[i], env); v > 0 {
						env[name.Name] = v
					}
				}
			}
		case *ast.AssignStmt:
			for i, rhs := range node.Rhs {
				if i >= len(node.Lhs) {
					break
				}
				id, ok := node.Lhs[i].(*ast.Ident)
				if !ok || id.Name == "_" {
					continue
				}
				if v := lengthOf(rhs, env); v > 0 {
					env[id.Name] = v
				}
			}
		}
		return true
	})
	return env
}

// lengthOf resolves the element count of a slice-producing expression: `make(T, N)` and `[]T{…}`.
func lengthOf(e ast.Expr, env map[string]int) int {
	switch node := e.(type) {
	case *ast.CallExpr:
		if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "make" && len(node.Args) >= 2 {
			return intValue(node.Args[1], env)
		}
	case *ast.CompositeLit:
		if _, ok := node.Type.(*ast.ArrayType); ok {
			return len(node.Elts)
		}
	}
	return 0
}

// intValue resolves an int literal or a name already in env; 0 means "not statically visible".
func intValue(e ast.Expr, env map[string]int) int {
	switch node := e.(type) {
	case *ast.BasicLit:
		if node.Kind != token.INT {
			return 0
		}
		v, err := strconv.Atoi(node.Value)
		if err != nil {
			return 0
		}
		return v
	case *ast.Ident:
		return env[node.Name]
	}
	return 0
}

// loopRounds returns the statically DERIVED iteration count of a for or range statement, or 0 when it
// cannot be derived:
//
//	for i := S; i < N; i += K    -> ceil((N-S)/K)     (i++ is K=1; <= adds one)
//	for range N                  -> N                 (range-over-int)
//	for _, x := range xs         -> len(xs)           when xs's length is in env
//
// external review B2-5: this used to return the RHS of the condition and call it a count, so
// `for i := 4; i < 5; i++` — one iteration — reported five. The bound is not the count; the count comes
// from all three clauses, and a loop missing any of them is unprovable rather than assumed.
//
// A loop whose body can exit early is also unprovable: `break` or `return` means the written bound is an
// upper limit, not the number of exercises that happened. `continue` is fine (the iteration still
// completed) and so is a nested loop's own break, which is why the walk stops descending into nested
// loops and switches.
func loopRounds(n ast.Node, env map[string]int) int {
	switch loop := n.(type) {
	case *ast.ForStmt:
		if bodyCanExitEarly(loop.Body) {
			return 0
		}
		return forRounds(loop, env)
	case *ast.RangeStmt:
		if bodyCanExitEarly(loop.Body) {
			return 0
		}
		// `for range 20` and `for range K` — range-over-int, and the spelling the tunnel suites use.
		if v := intValue(loop.X, env); v > 0 {
			return v
		}
		// `for _, p := range publicPorts` — resolvable when the slice was built locally.
		if id, ok := loop.X.(*ast.Ident); ok {
			return env[id.Name]
		}
	}
	return 0
}

// forRounds derives the count of a three-clause for statement. Every clause must name the SAME variable
// and resolve to an integer, or the answer is 0.
func forRounds(loop *ast.ForStmt, env map[string]int) int {
	v, start, ok := loopInitVar(loop.Init, env)
	if !ok {
		return 0
	}
	bin, ok := loop.Cond.(*ast.BinaryExpr)
	if !ok || (bin.Op != token.LSS && bin.Op != token.LEQ) {
		return 0
	}
	lhs, ok := bin.X.(*ast.Ident)
	if !ok || lhs.Name != v {
		return 0 // the condition tests a different variable than the initializer sets
	}
	end := intValue(bin.Y, env)
	if end == 0 {
		return 0
	}
	if bin.Op == token.LEQ {
		end++
	}
	step, ok := loopStep(loop.Post, v)
	if !ok || step <= 0 {
		return 0
	}
	if end <= start {
		return 0
	}
	span := end - start
	rounds := span / step
	if span%step != 0 {
		rounds++
	}
	return rounds
}

// loopInitVar reads `i := <int>` (or `i = <int>`) and returns the variable name and its start value.
// A missing or non-integer initializer makes the loop unprovable — `for ; i < 5; i++` could start
// anywhere.
func loopInitVar(init ast.Stmt, env map[string]int) (name string, start int, ok bool) {
	as, isAssign := init.(*ast.AssignStmt)
	if !isAssign || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
		return "", 0, false
	}
	id, isIdent := as.Lhs[0].(*ast.Ident)
	if !isIdent {
		return "", 0, false
	}
	// A literal 0 start is the common case and intValue returns 0 for "unresolvable" too, so read the
	// literal directly rather than through intValue's sentinel.
	switch rhs := as.Rhs[0].(type) {
	case *ast.BasicLit:
		if rhs.Kind != token.INT {
			return "", 0, false
		}
		v, err := strconv.Atoi(rhs.Value)
		if err != nil {
			return "", 0, false
		}
		return id.Name, v, true
	case *ast.Ident:
		if v, known := env[rhs.Name]; known {
			return id.Name, v, true
		}
	}
	return "", 0, false
}

// loopStep reads `i++`, `i += K` and `i = i + K` for the loop variable and returns K.
func loopStep(post ast.Stmt, v string) (int, bool) {
	switch p := post.(type) {
	case *ast.IncDecStmt:
		id, ok := p.X.(*ast.Ident)
		if !ok || id.Name != v || p.Tok != token.INC {
			return 0, false
		}
		return 1, true
	case *ast.AssignStmt:
		if len(p.Lhs) != 1 || len(p.Rhs) != 1 {
			return 0, false
		}
		id, ok := p.Lhs[0].(*ast.Ident)
		if !ok || id.Name != v {
			return 0, false
		}
		if p.Tok == token.ADD_ASSIGN {
			lit, isLit := p.Rhs[0].(*ast.BasicLit)
			if !isLit || lit.Kind != token.INT {
				return 0, false
			}
			k, err := strconv.Atoi(lit.Value)
			if err != nil {
				return 0, false
			}
			return k, true
		}
	}
	return 0, false
}

// bodyCanExitEarly reports whether a loop body contains control flow that makes the written bound an
// UPPER LIMIT rather than a count of exercises.
//
// EXTERNAL REVIEW RB2-2 — THE FIRST VERSION WAS UNSOUND IN THREE WAYS
// --------------------------------------------------------------------
// It pruned every nested loop, switch, select and type switch on the assumption that any exit inside
// belongs to that construct. That assumption is false three times over, and each falsification was
// supplied as a synthesized row that the gate accepted as "at least five exercises":
//
//	break outer   inside a nested loop  -> exits THIS loop after one exercise (labels ignore nesting)
//	return        inside a switch       -> exits the whole test; a switch owns `break`, not `return`
//	continue      before the exercise   -> five iterations, zero exercises
//
// The third is the subtlest and the reviewer stated the principle exactly: "A continue is not harmless
// unless the gate proves the load-bearing exercise occurs before every continue path." This gate does
// not do reachability analysis, so it does not try to prove that — it REPORTS instead, which is the
// direction an unprovable case has to fail in.
//
// The rule now, with nesting tracked explicitly rather than by pruning:
//
//	return                      always      (a FuncLit's return belongs to the closure, so FuncLits are skipped)
//	goto                        always      (target unknowable from here)
//	any LABELLED break/continue always      (the label may name this loop; resolving it is not worth the
//	                                        machinery when the conservative answer costs one comment)
//	unlabelled break            iff no enclosing loop/switch/select inside this body owns it
//	unlabelled continue         iff no enclosing loop inside this body owns it
func bodyCanExitEarly(body *ast.BlockStmt) bool {
	var walk func(n ast.Node, loopDepth, breakOwnerDepth int) bool
	walk = func(n ast.Node, loopDepth, breakOwnerDepth int) bool {
		switch node := n.(type) {
		case nil:
			return false

		case *ast.FuncLit:
			// A return inside a closure returns from the CLOSURE. Its breaks/continues cannot bind to
			// anything out here either, because Go forbids branching out of a function literal.
			return false

		case *ast.ForStmt:
			return walk(node.Body, loopDepth+1, breakOwnerDepth+1)
		case *ast.RangeStmt:
			return walk(node.Body, loopDepth+1, breakOwnerDepth+1)

		case *ast.SwitchStmt:
			// A switch owns an unlabelled `break` but NOT `continue`, `return` or `goto`. Keep walking
			// with the break-owner depth raised and the loop depth unchanged — pruning the whole switch is
			// what hid the return.
			return walk(node.Body, loopDepth, breakOwnerDepth+1)
		case *ast.TypeSwitchStmt:
			return walk(node.Body, loopDepth, breakOwnerDepth+1)
		case *ast.SelectStmt:
			return walk(node.Body, loopDepth, breakOwnerDepth+1)

		case *ast.BranchStmt:
			switch node.Tok {
			case token.GOTO:
				return true
			case token.BREAK:
				return node.Label != nil || breakOwnerDepth == 0
			case token.CONTINUE:
				return node.Label != nil || loopDepth == 0
			}
			return false

		case *ast.ReturnStmt:
			return true
		}

		// Everything else: recurse into children at the same depths.
		found := false
		ast.Inspect(n, func(c ast.Node) bool {
			if found || c == nil || c == n {
				return !found
			}
			switch c.(type) {
			case *ast.FuncLit, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt,
				*ast.SelectStmt, *ast.BranchStmt, *ast.ReturnStmt:
				if walk(c, loopDepth, breakOwnerDepth) {
					found = true
				}
				return false // walk() handled this subtree at the right depths
			}
			return true
		})
		return found
	}
	return walk(body, 0, 0)
}

// Resources remain risks when dependency extraction removes the old import
// trigger. Pure compile stubs do not substitute for their runtime owners.
// brokermaint/controlbudget are pure leaves; their owning maintenance/cancel
// cycles live in brokerimage/cli, while every leaf remains in the race matrix.
// The measured maintenance owner moved to brokercontrol/maintenance.
// origin: S7 internal review GATES-LEAD F6 / hot-update S7 plan §11.4
// origin: hot-update S6 plan §9.3 / T54
var brokerLeakRiskPackages = strings.Fields("internal/brokercontrol/maintenance internal/brokerimage internal/listenerimage internal/clusterio internal/cluster internal/storage internal/adminsock internal/httplisten internal/subhttp internal/brokermetrics internal/clustermanifest internal/cli internal/natsinbox internal/proxydial internal/execclient internal/clusterupgrade internal/broker internal/agent internal/agentboot internal/tunnel cmd/tether test/clusterharness test/cli_e2e test/p13 test/hotupdate")

// Frozen subject names preserve independently owned failure seams within the
// same package. One passing cycle cannot stand in for partial Take plus active
// rollback just because both tests are under brokerimage or hotupdate.
// origin: hot-update S6 plan §12.1 / T54–T57
var brokerLeakSubjects = map[string]string{
	"TestBrokerCutLeakCycles":                "exerciseCutFailure",
	"TestBrokerImportLeakCycles":             "exerciseBrokerPartialTake",
	"TestBrokerStorageLeakCycles":            "exerciseStorageCloseFailure",
	"TestBrokerNodeLeakCycles":               "exerciseNodePartialClose",
	"TestBrokerTransportLeakCycles":          "exerciseStalledRPC",
	"TestBrokerSnapshotLeakCycles":           "exerciseSnapshotCancel",
	"TestBrokerServerPauseLeakCycles":        "exerciseServerPauseFailure",
	"TestBrokerServerImageLeakCycles":        "exerciseServerPartialImport",
	"TestBrokerListenerLeakCycles":           "exerciseKeeperPauseAdopt",
	"TestBrokerHTTPLeakCycles":               "exerciseHTTPKeepaliveFreeze",
	"TestBrokerMetricsLeakCycles":            "exerciseMetricsInheritedHTTP",
	"TestBrokerManifestLeakCycles":           "exerciseManifestInheritedHTTP",
	"TestBrokerSubHTTPLeakCycles":            "exerciseSubHTTPInheritedHTTP",
	"TestBrokerAdminLeakCycles":              "exerciseAdminInheritedClose",
	"TestBrokerRunLeakCycles":                "exerciseBrokerExecReturned",
	"TestBrokerAuthorityLeakCycles":          "exerciseAuthorityRebind",
	"TestBrokerHomeLeakCycles":               "exerciseMaintainedHomeTick",
	"TestBrokerMaintenanceLeakCycles":        "exerciseMaintenanceExpiry",
	"TestBrokerHolderLeakCycles":             "exerciseHolderLostLease",
	"TestBrokerAgentHoldLeakCycles":          "exerciseAgentMaintenanceHold",
	"TestBrokerCLIRetryLeakCycles":           "exerciseCLIRetryCancel",
	"TestBrokerProxyLeakCycles":              "exerciseProxyDeadlineCancel",
	"TestBrokerInboxLeakCycles":              "exerciseInboxFallbackCancel",
	"TestBrokerExecClientLeakCycles":         "exerciseAdmissionBlackout",
	"TestBrokerBootLeakCycles":               "exerciseBrokerRoleRedispatch",
	"TestBrokerCommandLeakCycles":            "exerciseCtlAdmissionCancel",
	"TestBrokerRollLeakCycles":               "exerciseRollLeaderChanged",
	"TestBrokerStoreFixtureLeakCycles":       "exerciseStoreExecRollback",
	"TestBrokerControlIntegrationLeakCycles": "exerciseRealCalloutCut",
	"TestBrokerLifecycleSeamLeakCycles":      "exerciseActiveRollbackStop",
}

// Resolve the actual shared import so an identically named local shim cannot
// masquerade as the two required assertions. Only Test-body statements count.
// origin: hot-update S6 plan §9.3 / T54
func brokerMeasuredLeakTest(file *ast.File, fn *ast.FuncDecl, anchor string) bool {
	if file == nil || fn == nil || fn.Recv != nil || fn.Body == nil || anchor == "" {
		return false
	}
	shared := ""
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err == nil && path == "github.com/LinZiyang666/tether/internal/testharness" {
			shared = "testharness"
			if imp.Name != nil {
				shared = imp.Name.Name
			}
		}
	}
	if shared == "" || shared == "." || shared == "_" {
		return false
	}
	counts := map[string]int{}
	for _, stmt := range fn.Body.List {
		expr, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expr.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || id.Name != shared {
			continue
		}
		if sel.Sel.Name != "AssertNoGoroutineLeak" && sel.Sel.Name != "AssertNoFDLeak" {
			continue
		}
		if !qualifyingLoopBefore(fn.Body, call.Pos(), anchor) {
			return false
		}
		counts[sel.Sel.Name]++
	}
	return counts["AssertNoGoroutineLeak"] == 1 && counts["AssertNoFDLeak"] == 1
}

// Missing runtime tests stay red until producers land their exact site/anchor
// receipts; no absent owner receives an exception or a synthetic cycle.
// origin: hot-update S6 plan §9.3 / T54
// gate-control: TestBrokerLeakGateAndRaceCoverageControls
func TestBrokerLeakOwnersHaveMeasuredCycles(t *testing.T) {
	root := repoRoot(t)
	guarded := map[string]bool{}
	subjects := map[string]int{}
	inventory := readTestFunctionInventory(t, filepath.Join(root, "test/determinism", testFunctionInventoryGolden))
	for _, pkg := range brokerLeakRiskPackages {
		files, err := filepath.Glob(filepath.Join(root, pkg, "*_test.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || !strings.HasPrefix(fn.Name.Name, "TestBroker") || !strings.HasSuffix(fn.Name.Name, "LeakCycles") {
					continue
				}
				key := filepath.ToSlash(rel) + ":" + fn.Name.Name
				if !brokerMeasuredLeakTest(file, fn, leakExerciseAnchors[key]) {
					t.Errorf("%s: requires exact anchor, >=5 rounds and two direct shared assertions", key)
					continue
				}
				if !inventory[filepath.ToSlash(rel)+": "+fn.Name.Name] {
					t.Errorf("%s: absent from test inventory", key)
					continue
				}
				if expected, frozen := brokerLeakSubjects[fn.Name.Name]; frozen {
					if leakExerciseAnchors[key] != expected {
						t.Errorf("%s: expected frozen subject %s", key, expected)
						continue
					}
					subjects[fn.Name.Name]++
				}
				guarded[pkg] = true
			}
		}
		if !guarded[pkg] {
			t.Errorf("%s: no registered S6 measured leak cycle (pending runtime owner)", pkg)
		}
	}
	for name := range brokerLeakSubjects {
		if subjects[name] != 1 {
			t.Errorf("%s: expected one registered frozen subject, got %d (pending runtime owner)", name, subjects[name])
		}
	}
}

// Each defect passes through the live shape predicate, including assertions
// hidden in a dead helper and a function value substituted for a call.
// origin: hot-update S6 plan §11 / T54
func TestBrokerLeakGateAndRaceCoverageControls(t *testing.T) {
	const frozen = "internal/brokercontrol/maintenance internal/brokerimage internal/listenerimage internal/clusterio internal/cluster internal/storage internal/adminsock internal/httplisten internal/subhttp internal/brokermetrics internal/clustermanifest internal/cli internal/natsinbox internal/proxydial internal/execclient internal/clusterupgrade internal/broker internal/agent internal/agentboot internal/tunnel cmd/tether test/clusterharness test/cli_e2e test/p13 test/hotupdate"
	if strings.Join(brokerLeakRiskPackages, " ") != frozen {
		t.Fatal("S6 leak owner registry changed")
	}
	const frozenSubjects = `TestBrokerCutLeakCycles exerciseCutFailure
TestBrokerImportLeakCycles exerciseBrokerPartialTake
TestBrokerStorageLeakCycles exerciseStorageCloseFailure
TestBrokerNodeLeakCycles exerciseNodePartialClose
TestBrokerTransportLeakCycles exerciseStalledRPC
TestBrokerSnapshotLeakCycles exerciseSnapshotCancel
TestBrokerServerPauseLeakCycles exerciseServerPauseFailure
TestBrokerServerImageLeakCycles exerciseServerPartialImport
TestBrokerListenerLeakCycles exerciseKeeperPauseAdopt
TestBrokerHTTPLeakCycles exerciseHTTPKeepaliveFreeze
TestBrokerMetricsLeakCycles exerciseMetricsInheritedHTTP
TestBrokerManifestLeakCycles exerciseManifestInheritedHTTP
TestBrokerSubHTTPLeakCycles exerciseSubHTTPInheritedHTTP
TestBrokerAdminLeakCycles exerciseAdminInheritedClose
TestBrokerRunLeakCycles exerciseBrokerExecReturned
TestBrokerAuthorityLeakCycles exerciseAuthorityRebind
TestBrokerHomeLeakCycles exerciseMaintainedHomeTick
TestBrokerMaintenanceLeakCycles exerciseMaintenanceExpiry
TestBrokerHolderLeakCycles exerciseHolderLostLease
TestBrokerAgentHoldLeakCycles exerciseAgentMaintenanceHold
TestBrokerCLIRetryLeakCycles exerciseCLIRetryCancel
TestBrokerProxyLeakCycles exerciseProxyDeadlineCancel
TestBrokerInboxLeakCycles exerciseInboxFallbackCancel
TestBrokerExecClientLeakCycles exerciseAdmissionBlackout
TestBrokerBootLeakCycles exerciseBrokerRoleRedispatch
TestBrokerCommandLeakCycles exerciseCtlAdmissionCancel
TestBrokerRollLeakCycles exerciseRollLeaderChanged
TestBrokerStoreFixtureLeakCycles exerciseStoreExecRollback
TestBrokerControlIntegrationLeakCycles exerciseRealCalloutCut
TestBrokerLifecycleSeamLeakCycles exerciseActiveRollbackStop`
	rows := strings.Split(frozenSubjects, "\n")
	if len(brokerLeakSubjects) != len(rows) {
		t.Fatal("frozen S6 leak subjects changed")
	}
	for _, row := range rows {
		parts := strings.Fields(row)
		if brokerLeakSubjects[parts[0]] != parts[1] {
			t.Fatalf("missing/changed frozen subject %s", row)
		}
	}
	const src = `package owner
import th "github.com/LinZiyang666/tether/internal/testharness"
func TestOwnedCycles(t *testing.T) {
 beforeG := runtime.NumGoroutine()
 beforeFD := th.FDCount()
 for round := 0; round < 5; round++ { exerciseOwned(t) }
 th.AssertNoGoroutineLeak(t, "owner", beforeG)
 th.AssertNoFDLeak(t, "owner", beforeFD, 0)
}`
	for _, tc := range []struct {
		name, source, anchor string
		want                 bool
	}{
		{"positive", src, "exerciseOwned", true},
		{"anchor omitted", src, "", false},
		{"wrong anchor", src, "unrelated", false},
		{"four rounds", strings.Replace(src, "round < 5", "round < 4", 1), "exerciseOwned", false},
		{"function value", strings.Replace(src, "exerciseOwned(t)", "_ = exerciseOwned", 1), "exerciseOwned", false},
		{"goroutine assertion omitted", strings.Replace(src, "th.AssertNoGoroutineLeak(t, \"owner\", beforeG)", "", 1), "exerciseOwned", false},
		{"FD assertion omitted", strings.Replace(src, "th.AssertNoFDLeak(t, \"owner\", beforeFD, 0)", "", 1), "exerciseOwned", false},
		{"dead helper", strings.Replace(src, " th.AssertNoGoroutineLeak", "}\nfunc deadHelper(t *testing.T) { th.AssertNoGoroutineLeak", 1), "exerciseOwned", false},
		{"local shim", strings.ReplaceAll(src, "th.AssertNo", "shim.AssertNo"), "exerciseOwned", false},
		{"wrong import", strings.Replace(src, "internal/testharness", "internal/fake", 1), "exerciseOwned", false},
		{"conditional assertion", strings.Replace(src, " th.AssertNoFDLeak(t, \"owner\", beforeFD, 0)", " if false { th.AssertNoFDLeak(t, \"owner\", beforeFD, 0) }", 1), "exerciseOwned", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "owner_test.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			var fn *ast.FuncDecl
			for _, decl := range file.Decls {
				if d, ok := decl.(*ast.FuncDecl); ok && d.Name.Name == "TestOwnedCycles" {
					fn = d
				}
			}
			if got := brokerMeasuredLeakTest(file, fn, tc.anchor); got != tc.want {
				t.Fatalf("measured cycle = %v, want %v", got, tc.want)
			}
		})
	}
}

// Explicit ownership survives dependency slimming; absent lifecycle tests stay red.
// No anchor is registered until its real Test and exercise land.
// origin: hot-update S7 plan §11.3 / T52
var transferLeakRiskPackages = []string{
	"internal/jsstream",
	"internal/transferpath",
	"internal/filetransfer",
	"internal/drillobserve",
	"internal/xferworker",
	"internal/xfersend",
	"internal/xferreceive",
	"internal/xfercourier",
	"internal/xfertrack",
	"internal/xferfinal",
	"internal/xferledger",
	"internal/xferhistory",
	"internal/xferbucket",
	"internal/xfergc",
	"internal/xfercontrol",
	"internal/xferclient",
	"internal/xferauth",
	"internal/xferbridge",
	"internal/xferagent",
	"internal/xferserve",
	"internal/agent",
	"internal/broker",
	"internal/brokerimage",
	"internal/brokercontrol",
	"internal/authcallout",
	"internal/cli",
	"test/transferharness",
}

const fullstackOwnershipInventory = "test/architecture/testdata/fullstack_ownership.tsv"

// Re-read disk on every walk; only the same path AND exact bytes can reuse an
// immutable parse. Both the test inventory and the union gate consume this
// cache, avoiding two full reparses per mutation without hiding disk changes.
// origin: hot-update S10 plan §11.1 / §12.2 / P29
func fullstackParsedInventoryFile(path string, raw []byte) (*ast.File, error) {
	key := filepath.ToSlash(path) + "\x00" + string(raw)
	fullstackInventoryASTMemo.Lock()
	file := fullstackInventoryASTMemo.files[key]
	fullstackInventoryASTMemo.Unlock()
	if file != nil {
		return file, nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	fullstackInventoryASTMemo.Lock()
	fullstackInventoryASTMemo.files[key] = file
	fullstackInventoryASTMemo.Unlock()
	return file, nil
}

// The manifest is a responsibility contract; actual siblings are an independent
// denominator. A moved or newly split leaf must not vanish because its owner
// forgot to list it, and a stale path cannot stand in for the successor file.
// origin: hot-update S10 plan §10.2 / §12.2 / P29
func fullstackOwnershipProblem(root string) error {
	rows, err := fullstackInventoryRows(root, fullstackOwnershipInventory, "")
	if err != nil {
		return err
	}
	owned := map[string]string{}
	for _, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) != 2 || fields[0] == "" || fields[1] == "" || owned[fields[0]] != "" || filepath.Clean(fields[0]) != fields[0] || strings.HasPrefix(fields[0], "../") || filepath.IsAbs(fields[0]) {
			return fmt.Errorf("invalid/duplicate ownership row %q", row)
		}
		owned[fields[0]] = fields[1]
		if info, err := os.Stat(filepath.Join(root, fields[0])); err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("dead ownership row %s", fields[0])
		}
	}
	plan, err := os.ReadFile(filepath.Join(root, "docs/reviews/hot-update-fullstack-acceptance-plan.md"))
	if err != nil {
		return err
	}
	manifestRow := regexp.MustCompile(`(?m)^\| (P[0-9]{2}) [^|]+\| (?:HARD|NORMAL) \| [0-9]+ \|[^|]*\|(.*)\|$`)
	pathQuote := regexp.MustCompile("`([^`]+)`")
	packages := map[string]bool{}
	for _, row := range manifestRow.FindAllStringSubmatch(string(plan), -1) {
		packages[row[1]] = true
		for _, path := range pathQuote.FindAllStringSubmatch(row[2], -1) {
			if _, err := os.Stat(filepath.Join(root, path[1])); err == nil && owned[path[1]] != row[1] {
				return fmt.Errorf("manifest ownership mismatch %s: got %s, want %s", path[1], owned[path[1]], row[1])
			}
		}
	}
	if len(packages) != 36 {
		return fmt.Errorf("empty/incomplete S10 manifest root: %d packages", len(packages))
	}
	for _, top := range []string{"internal", "cmd", "test"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "__pycache__" {
					return filepath.SkipDir
				}
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			base := entry.Name()
			required := strings.HasPrefix(base, "fullstack_") || strings.HasPrefix(base, "fullstack-") || strings.Contains(base, "hot-update-fullstack") || strings.Contains(base, "hot_update_fullstack")
			for _, dir := range []string{"internal/storewitness/", "test/fullstackharness/", "test/hotupdate/acceptance/", "test/hotupdate/evidencecmd/"} {
				required = required || strings.HasPrefix(rel, dir)
			}
			if required && owned[rel] == "" {
				return fmt.Errorf("unrecorded actual ownership %s", rel)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Check each assertion independently. A goroutine assertion followed by a new
// FD baseline cannot borrow the earlier exercise loop; the first-assertion-only
// historical shape check did not distinguish those two lifecycles.
// origin: hot-update S10 plan §11.2 / P29
func fullstackLeakInventory(root string, files map[string]*ast.File, update bool) error {
	anchors := fullstackInventoryMap(files["test/determinism/leak_assert_shape_test.go"], "leakExerciseAnchors")
	if len(anchors) == 0 {
		return fmt.Errorf("empty leak anchor root")
	}
	seen := map[string]bool{}
	var actual []string
	for path, file := range files {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			key := path + ":" + fn.Name.Name
			if _, calls := leakAssertCallPos(fn.Body); calls {
				seen[key] = true
			}
			if !strings.HasPrefix(path, "test/fullstackharness/") && !strings.Contains(filepath.Base(path), "fullstack_") && path != "internal/agent/runtime_observation_test.go" && !strings.HasPrefix(path, "internal/storewitness/") && path != "internal/drillobserve/runtime_sample_test.go" {
				continue
			}
			anchor, _ := strconv.Unquote(fullstackInventoryExpr(anchors[key]))
			ordinals := map[string]int{}
			var failure error
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || failure != nil {
					return true
				}
				name := fullstackInventoryExpr(call.Fun)
				name = name[strings.LastIndex(name, ".")+1:]
				if !leakAssertHelpers[name] {
					return true
				}
				ordinals[name]++
				actual = append(actual, key+"\t"+name+"#"+strconv.Itoa(ordinals[name])+"\t"+anchor)
				direct := false
				ast.Inspect(fn.Body, func(parent ast.Node) bool {
					if _, closure := parent.(*ast.FuncLit); closure {
						return false // a subtest/cleanup closure has a separate lifetime
					}
					direct = direct || parent == call
					return true
				})
				if anchor == "" || !direct || !qualifyingLoopBefore(fn.Body, call.Pos(), anchor) {
					failure = fmt.Errorf("%s: %s missing direct five-round leak anchor %s", key, name, anchor)
				}
				return true
			})
			if failure != nil {
				return failure
			}
		}
	}
	for key := range anchors {
		if !seen[key] {
			return fmt.Errorf("dead leak anchor %s", key)
		}
	}
	if update {
		if err := appendFullstackInventorySection(root, "leak-assertions", actual); err != nil {
			return err
		}
	}
	recorded, err := fullstackInventoryRows(root, fullstackObservationInventory, "leak-assertions")
	if err != nil {
		return err
	}
	return fullstackInventoryDifference("leak assertions", actual, recorded)
}

// A struct ledger alone cannot notice a counter wired into the wrong binary
// slot. Join the actual producer and consumer projections to the observation
// ledger's slot names; a symmetric rename never changes the schema silently.
// origin: hot-update S10 plan §5.3 / §12.2 / P29
func fullstackRuntimeCodecProblem(root string, files map[string]*ast.File) error {
	file := files["internal/drillobserve/receipt/runtime.go"]
	if file == nil {
		return fmt.Errorf("missing runtime codec root")
	}
	labels := map[string]string{
		"1": "schema", "c.PID": "pid", "c.StartTicks": "start_ticks", "c.Binding.Generation": "generation",
		"c.Goroutines": "goroutines", "c.FDs": "fd_count", "c.ReplayReserved": "replay_reserved",
		"c.ActiveExec": "active_exec", "c.ActivePTY": "active_pty", "c.RetainedRecords": "retained_records",
		"c.CourierPending": "courier_pending", "c.Consumers": "consumer_count", "c.Temps": "temp_count",
		"c.UpgradeLocks": "upgrade_lock_count", "c.OldOwners": "old_owner_count", "c.Sequence": "sample_sequence",
	}
	encoded, decoded := map[int]string{}, map[int]string{0: "1"}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if fn.Name.Name == "EncodeRuntimeSample" {
				if pair, ok := node.(*ast.KeyValueExpr); ok && fullstackInventoryExpr(pair.Key) == "Values" {
					if values, ok := pair.Value.(*ast.CompositeLit); ok {
						for index, value := range values.Elts {
							encoded[index] = fullstackInventoryExpr(value)
						}
					}
				}
			}
			if fn.Name.Name == "DecodeRuntimeSample" {
				if pair, ok := node.(*ast.KeyValueExpr); ok {
					if value, ok := pair.Value.(*ast.IndexExpr); ok && fullstackInventoryExpr(value.X) == "v" {
						if index, err := strconv.Atoi(fullstackInventoryExpr(value.Index)); err == nil {
							decoded[index] = "c." + fullstackInventoryExpr(pair.Key)
						}
					}
				}
				if binding, ok := node.(*ast.CompositeLit); ok && fullstackInventoryExpr(binding.Type) == "RuntimeBinding" && len(binding.Elts) == 3 {
					if value, ok := binding.Elts[2].(*ast.IndexExpr); ok && fullstackInventoryExpr(value.X) == "v" {
						if index, err := strconv.Atoi(fullstackInventoryExpr(value.Index)); err == nil {
							decoded[index] = "c.Binding.Generation"
						}
					}
				}
			}
			return true
		})
	}
	rows, err := fullstackInventoryRows(root, fullstackObservationInventory, "producer-consumer")
	if err != nil {
		return err
	}
	var recorded, actual []string
	for _, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) == 5 && fields[0] == "runtime-sample-v1" && strings.HasPrefix(fields[1], "Values[") {
			if fields[2] != "internal/drillobserve/receipt/runtime.go:EncodeRuntimeSample" || fields[3] != "internal/drillobserve/receipt/runtime.go:DecodeRuntimeSample" || fields[4] != "P06" {
				return fmt.Errorf("runtime codec producer/consumer ownership drift: %s", row)
			}
			recorded = append(recorded, fields[1])
		}
	}
	if len(encoded) != 16 || len(decoded) != 16 {
		return fmt.Errorf("runtime codec does not cover all 16 actual slots")
	}
	for index, expression := range encoded {
		label := labels[expression]
		if label == "" || decoded[index] != expression {
			return fmt.Errorf("runtime codec slot %d: producer=%s, consumer=%s", index, expression, decoded[index])
		}
		actual = append(actual, fmt.Sprintf("Values[%d].%s", index, label))
	}
	return fullstackInventoryDifference("runtime codec slots", actual, recorded)
}

func fullstackInventoryProblem(root string) error {
	files, err := fullstackInventorySources(root)
	if err != nil {
		return err
	}
	var problems []error
	current, _, err := collectTestFunctionInventory(root)
	if err != nil {
		return err
	}
	var tests []string
	for key := range current {
		tests = append(tests, key)
	}
	recorded, err := fullstackInventoryRows(root, "test/determinism/"+testFunctionInventoryGolden, "")
	if err != nil {
		return err
	}
	problems = append(problems, fullstackInventoryDifference("test", tests, recorded))
	for _, section := range []string{"actual-go-fields", "enum-members"} {
		recorded, err := fullstackInventoryRows(root, fullstackObservationInventory, section)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		var actual []string
		if section == "actual-go-fields" {
			actual, err = fullstackObservationFields(files, recorded)
		} else {
			actual, err = fullstackEnumInventory(files)
		}
		if err == nil {
			err = fullstackInventoryDifference(section, actual, recorded)
		}
		problems = append(problems, err)
	}
	problems = append(problems, fullstackOwnershipProblem(root), fullstackLeakInventory(root, files, false), fullstackErrorRootInventory(files), fullstackRuntimeCodecProblem(root, files))
	return errors.Join(problems...)
}

// This signs source registration only. Existing wire/image/private-codec gates
// and P27 decoder tests sign their own closures; live drill receipts remain with
// P19/P20/P27/P35 and are never invented by an inventory updater.
// origin: hot-update S10 plan §10.2 / §12.2 / P29
// gate-control: TestFullstackInventoryGateControls
func TestFullstackInventoriesCoverActualUnion(t *testing.T) {
	if *updateFullstackInventories {
		root := repoRoot(t)
		files, err := fullstackInventorySources(root)
		if err != nil {
			t.Fatal(err)
		}
		recorded, err := fullstackInventoryRows(root, fullstackObservationInventory, "actual-go-fields")
		if err != nil {
			t.Fatal(err)
		}
		fields, err := fullstackObservationFields(files, recorded)
		if err != nil {
			t.Fatal(err)
		}
		enums, err := fullstackEnumInventory(files)
		if err != nil {
			t.Fatal(err)
		}
		for section, rows := range map[string][]string{"actual-go-fields": fields, "enum-members": enums} {
			if err := appendFullstackInventorySection(root, section, rows); err != nil {
				t.Fatal(err)
			}
		}
		if err := fullstackLeakInventory(root, files, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := fullstackInventoryProblem(repoRoot(t)); err != nil {
		t.Fatal(err)
	}
}

func fullstackInventoryFixture(t *testing.T) string {
	t.Helper()
	real, root := repoRoot(t), t.TempDir()
	copyFile := func(path string) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(real, path))
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, top := range []string{"internal", "cmd", "test"} {
		err := filepath.WalkDir(filepath.Join(real, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "__pycache__" {
					return filepath.SkipDir
				}
				return nil
			}
			if entry.Type().IsRegular() {
				rel, err := filepath.Rel(real, path)
				if err != nil {
					return err
				}
				copyFile(rel)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := fullstackInventoryRows(real, fullstackOwnershipInventory, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		copyFile(strings.Split(row, "\t")[0])
	}
	return root
}

// Every control edits copied REAL files on disk and invokes the gate's reader,
// including its registry source. No go-test subprocess, overlay, invented leak
// exercise or deployment receipt can stand in for the production denominator.
// origin: hot-update S10 plan §10.2 / §12.2 / P29
func TestFullstackInventoryGateControls(t *testing.T) {
	root := fullstackInventoryFixture(t)
	check := func() error { return fullstackInventoryProblem(root) }
	if err := check(); err != nil {
		t.Fatalf("real-source baseline: %v", err)
	}
	const testLedger = "test/determinism/" + testFunctionInventoryGolden
	const leakSource = "test/determinism/leak_assert_shape_test.go"
	const ofdTest = "test/fullstackharness/identity_ofd_linux_test.go"
	anchorKey := ofdTest + ":TestFullstackOFDWitnessRejectsReopen"
	raw, err := os.ReadFile(filepath.Join(root, leakSource))
	if err != nil {
		t.Fatal(err)
	}
	var anchorRow string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, `"`+anchorKey+`":`) {
			anchorRow = line + "\n"
		}
	}
	if anchorRow == "" {
		t.Fatal("actual OFD exercise anchor is missing")
	}
	for _, mutation := range []struct {
		name, path, old, replacement, want string
	}{
		{"missing-test-row", testLedger, leakSource + ": TestFullstackInventoriesCoverActualUnion\n", "", "test inventory"},
		{"dead-test-row", testLedger, "", "internal/ghost/dead_test.go: TestDeadInventory\n", "dead rows"},
		{"new-test", ofdTest, "", "\nfunc TestUnrecordedInventory(t *testing.T) {}\n", "unrecorded actual union"},
		{"missing-field", fullstackObservationInventory, "test/hotupdate/acceptance/contract/types.go:Stamp.QueryNonce\t-\tstring\n", "", "actual-go-fields inventory"},
		{"changed-field-codec", "test/hotupdate/acceptance/contract/types.go", "QueryNonce", "ChangedQueryNonce", "actual-go-fields inventory"},
		{"new-field", "test/fullstackharness/process.go", "type ProcessIdentity struct {", "type ProcessIdentity struct {\n Unregistered string", "actual-go-fields inventory"},
		{"dead-field-row", fullstackObservationInventory, "# section: actual-go-fields\n", "# section: actual-go-fields\ninternal/ghost/dead.go:Ghost.Field\t-\tstring\n", "dead rows"},
		{"wrong-runtime-encoder", "internal/drillobserve/receipt/runtime.go", "c.Goroutines, c.FDs,", "c.FDs, c.Goroutines,", "runtime codec slot"},
		{"wrong-runtime-decoder", "internal/drillobserve/receipt/runtime.go", "Goroutines: v[4]", "Goroutines: v[5]", "runtime codec"},
		{"missing-runtime-slot", fullstackObservationInventory, "runtime-sample-v1\tValues[4].goroutines\tinternal/drillobserve/receipt/runtime.go:EncodeRuntimeSample\tinternal/drillobserve/receipt/runtime.go:DecodeRuntimeSample\tP06\n", "", "runtime codec slots inventory"},
		{"wrong-runtime-codec-owner", fullstackObservationInventory, "runtime-sample-v1\tValues[4].goroutines\tinternal/drillobserve/receipt/runtime.go:EncodeRuntimeSample", "runtime-sample-v1\tValues[4].goroutines\tinternal/drillobserve/receipt/runtime.go:DecodeRuntimeSample", "runtime codec producer/consumer ownership drift"},
		{"missing-enum-member", fullstackObservationInventory, "xfercontract.StatusState\tStatusLive\n", "", "enum-members inventory"},
		{"new-enum-member", "internal/xfercontract/contracts.go", "", "\nconst StatusUnexpected StatusState = 99\n", "enum-members inventory"},
		{"new-enum-family", "internal/upgradetx/contracts.go", "", "\ntype UnexpectedFamily uint8\nconst (UnexpectedZero UnexpectedFamily = iota; UnexpectedOne)\n", "unregistered actual typed family"},
		{"new-string-vocabulary", "internal/upgradetx/contracts.go", "", "\ntype UnexpectedFamily string\nconst (UnexpectedFirst UnexpectedFamily = \"first\"; UnexpectedSecond UnexpectedFamily = \"second\")\n", "unregistered actual typed family"},
		{"dead-enum-prefix", "test/determinism/enum_switch_default_test.go", `"StatusUnknown", "StatusLive", "StatusTerminal"`, `"StatusUnknown", "StatusLive", "StatusTerminal", "StatusMissing"`, "dead enum member prefix"},
		{"missing-error-root", "cmd/tether/error_code_coverage_test.go", "\t\"internal/infrarepair\",\n", "", "error-root inventory"},
		{"dead-error-root", "cmd/tether/error_code_coverage_test.go", "var fullstackErrorLeafRoots = []string{", "var fullstackErrorLeafRoots = []string{\n\"internal/ghost\",", "error-root inventory"},
		{"disconnected-error-reader", "cmd/tether/error_code_coverage_test.go", "for code := range fullstackLeafCodeUniverse(t, root) {", "for code := range map[string]bool{} {", "disconnected from the actual classifier reader"},
		{"missing-leak-anchor", leakSource, anchorRow, "", "missing direct five-round leak anchor"},
		{"dead-leak-anchor", leakSource, "\nvar leakExerciseAnchors = map[string]string{\n", "\nvar leakExerciseAnchors = map[string]string{\n\"test/ghost_test.go:TestGhost\": \"exerciseGhost\",\n", "dead leak anchor"},
		{"single-leak-round", ofdTest, "round < 5", "round < 1", "missing direct five-round leak anchor"},
		{"fd-baseline-after-exercise", ofdTest, "testharness.AssertNoFDLeak(t,", "f = testharness.FDCount()\n testharness.AssertNoFDLeak(t,", "AssertNoFDLeak missing direct five-round leak anchor"},
		{"nested-leak-assertion", ofdTest, "testharness.AssertNoFDLeak(t, \"OFD witness lifecycle\", f, 0)", "func() { testharness.AssertNoFDLeak(t, \"OFD witness lifecycle\", f, 0) }()", "AssertNoFDLeak missing direct five-round leak anchor"},
		{"removed-fd-assertion", ofdTest, "testharness.AssertNoFDLeak(t, \"OFD witness lifecycle\", f, 0)", "", "leak assertions inventory"},
		{"missing-assertion-row", fullstackObservationInventory, "test/fullstackharness/identity_ofd_linux_test.go:TestFullstackOFDWitnessRejectsReopen\tAssertNoFDLeak#1\texerciseOFDWitness\n", "", "leak assertions inventory"},
		{"missing-ownership-row", fullstackOwnershipInventory, "test/fullstackharness/process.go\tP05\n", "", "manifest ownership mismatch"},
		{"wrong-owner", fullstackOwnershipInventory, "test/fullstackharness/process.go\tP05\n", "test/fullstackharness/process.go\tP04\n", "manifest ownership mismatch"},
		{"dead-ownership", fullstackOwnershipInventory, "", "test/ghost.go\tP29\n", "dead ownership row"},
		{"duplicate-ownership", fullstackOwnershipInventory, "", "test/fullstackharness/process.go\tP05\n", "duplicate inventory row"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			path := filepath.Join(root, mutation.path)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			mutated := string(original) + mutation.replacement
			if mutation.old != "" {
				if strings.Count(string(original), mutation.old) != 1 {
					t.Fatalf("mutation anchor must occur once: %q", mutation.old)
				}
				mutated = strings.Replace(string(original), mutation.old, mutation.replacement, 1)
			}
			if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil {
				t.Fatal(err)
			}
			red := check()
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			if red == nil || !strings.Contains(red.Error(), mutation.want) {
				t.Fatalf("mutation must fail for %q, got %v", mutation.want, red)
			}
			if err := check(); err != nil {
				t.Fatalf("restore stays red: %v", err)
			}
			t.Logf("ASSERTION_RED: %v; restored GREEN", red)
		})
	}
}
