package architecture

// legacy_ctx_background_sites.go — the DRAINING LEDGER for TestCtxBackgroundSitesAreAnnotatedOrLedgered.
//
// Every entry is a production `context.Background()` call that predates the gate and carries no
// `ctx-root:` / `ctx-none:` annotation. CLAUDE.md §5 promised these would be annotated "改到那一行时
// 顺手补"; between the promise (39 sites) and this ledger (2026-09-01) the count went UP to 44 and
// exactly one site was annotated. A promise with no mechanism is a number that only grows.
//
// ON THE NUMBER 40: a grep for `context.Background()` over cmd/ + internal/ says 44. Three of those
// are mentions inside comments and one is the single annotated site; the AST scan sees 40 calls with
// no annotation. The grep number is the one that rots; this one is re-derivable from the gate.
//
// KEYED "<path>: <func>", site-scoped like every other ledger in this tree (test_naming_test.go M7).
// Rules: an entry is only ever REMOVED (legacyCtxBackgroundSitesCap enforces it); an entry whose site
// is gone or annotated FAILS the gate until deleted. Drain by annotating the line — one comment.

const legacyCtxBackgroundSitesCap = 40

var legacyCtxBackgroundSites = map[string]bool{
	"cmd/tether/cluster_upgrade_drive.go: notifyMilestone":                     true,
	"cmd/tether/main.go: main":                                                 true,
	"cmd/tether/transfer.go: sendFinalize":                                     true,
	"internal/adminsock/server.go: (*Server).handleAudit":                      true,
	"internal/adminsock/server.go: (*Server).handleEvents":                     true,
	"internal/agent/conn_teardown.go: (*connTracker).Dial":                     true,
	"internal/agent/proxy.go: (*Agent).onNATSReconnect":                        true,
	"internal/agent/run.go: (*Agent).waitForAttachOnSub":                       true,
	"internal/agent/ssproxy/server.go: (*Server).Start":                        true,
	"internal/agent/transfer.go: (*Agent).handlePullForwarded":                 true,
	"internal/agent/transfer.go: (*Agent).handlePushCommitForwarded":           true,
	"internal/agent/upgrade.go: smokeVersion":                                  true,
	"internal/broker/broker.go: (*Broker).publishAudit":                        true,
	"internal/broker/cluster_forward.go: (*Forwarder).forward":                 true,
	"internal/broker/cluster_grow_cutover.go: (*Broker).hardRestartNatsServer": true,
	"internal/broker/clusterbackup.go: (*clusterAdminBackend).handleBackup":    true,
	"internal/broker/clusterwrite.go: (*Broker).clusterJSPlaceable":            true,
	"internal/broker/clusterwrite.go: (*Broker).clusterStreamsReady":           true,
	"internal/broker/clusterwrite.go: (*Broker).wireClusterLate":               true,
	"internal/broker/incident.go: (*clusterAdminBackend).handleExportIncident": true,
	"internal/broker/sessions.go: (*Broker).handleSessionCreate":               true,
	"internal/broker/sessions.go: (*Broker).handleSessionRm":                   true,
	"internal/broker/transfer.go: (*Broker).finalizeTransfer":                  true,
	"internal/broker/transfer.go: (*Broker).handleEvTransfer":                  true,
	"internal/broker/transfer.go: (*Broker).handleFinalizeReq":                 true,
	"internal/broker/xfer_provision.go: provisionXferBucketWithLimit":          true,
	"internal/cli/completion.go: CompleteAllocatedExposeNames":                 true,
	"internal/cli/completion.go: CompleteOnlineNodes":                          true,
	"internal/cli/completion.go: CompleteOwnedSessions":                        true,
	"internal/cli/completion.go: CompleteVisibleSessions":                      true,
	"internal/cluster/node.go: (*Node).ApplyMetaSet":                           true,
	"internal/cluster/node.go: raftConfig":                                     true,
	"internal/cluster/snapshot.go: (*fsm).restoreFrom":                         true,
	"internal/cluster/snapshot.go: (*fsmSnapshot).persist":                     true,
	"internal/clusteroffline/backup.go: OfflineBackup":                         true,
	"internal/clusteroffline/doctor.go: DBPreflight":                           true,
	"internal/clusteroffline/offline.go: dumpTable":                            true,
	"internal/clusteroffline/offline.go: lookupHostForProbeAdvice":             true,
	"internal/clusteroffline/offline.go: previewRecoveredRoster":               true,
	"internal/httplisten/httplisten.go: Serve":                                 true,
}
