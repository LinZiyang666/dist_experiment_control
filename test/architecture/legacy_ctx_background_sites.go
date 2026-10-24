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

// origin: hot-update S4 plan §10 — smokeVersion now marks its root probe context.
// origin: hot-update S5 plan §10.2 / AGENT-SLIM-S5 — extracted tracker and
// transfer callbacks now carry their ctx-none annotations at the actual roots.
// origin: hot-update S6 plan §9.3 / T53 — eight annotated roots drain the ledger.
// origin: hot-update S7 plan §6.4 / T31 — the session-removal rewrite annotated
// handleSessionRm's pre-Run fallback root (ctx-none), draining its entry.
// origin: hot-update S7 plan §11.4 / BROKER-SLIM-2; the moved restart root is annotated.
// origin: hot-update S10 lint L3a F044 — the moved StreamObserve root is annotated.
// origin: hot-update S7 plan §11.4 / T50; legacy terminal handlers no longer create root contexts.
const legacyCtxBackgroundSitesCap = 20

var legacyCtxBackgroundSites = map[string]bool{
	"internal/agent/run.go: (*Agent).waitForAttachOnSub":                       true,
	"internal/agent/ssproxy/server.go: (*Server).Start":                        true,
	"internal/broker/cluster_forward.go: (*Forwarder).forward":                 true,
	"internal/broker/clusterbackup.go: (*clusterAdminBackend).handleBackup":    true,
	"internal/broker/clusterwrite.go: (*Broker).clusterJSPlaceable":            true,
	"internal/broker/clusterwrite.go: (*Broker).clusterStreamsReady":           true,
	"internal/broker/incident.go: (*clusterAdminBackend).handleExportIncident": true,
	"internal/broker/sessions.go: (*Broker).handleSessionCreate":               true,
	"internal/broker/transfer.go: (*Broker).finalizeTransfer":                  true,
	"internal/cli/completion.go: CompleteAllocatedExposeNames":                 true,
	"internal/cli/completion.go: CompleteOnlineNodes":                          true,
	"internal/cli/completion.go: CompleteOwnedSessions":                        true,
	"internal/cli/completion.go: CompleteVisibleSessions":                      true,
	"internal/cluster/node.go: (*Node).ApplyMetaSet":                           true,
	"internal/cluster/node.go: raftConfig":                                     true,
	"internal/clusteroffline/backup.go: OfflineBackup":                         true,
	"internal/clusteroffline/doctor.go: DBPreflight":                           true,
	"internal/clusteroffline/offline.go: dumpTable":                            true,
	"internal/clusteroffline/offline.go: lookupHostForProbeAdvice":             true,
	"internal/clusteroffline/offline.go: previewRecoveredRoster":               true,
}
