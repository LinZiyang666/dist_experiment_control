package clusteroffline

// bundle_scope.go (R10 #53) — the bundle's SCOPE, stated out loud at BOTH ends.
//
// A backup bundle is { state.db, manifest.json }: the committed FSM database and the identity /
// provenance manifest. It has never contained JetStream, and `RestoreFromBackup` additionally resets
// `audit_published_index` to 0 (see normalizeRestoreStaging) because the fresh single-voter raft log
// restarts at index 1 — so there is nothing left to re-derive history from either.
//
// Until R10 that combination was SILENT: `cluster backup` said "backup complete", `cluster recovery
// restore` said "restore complete", and the runbook's disaster-recovery section never mentioned
// JetStream. An operator who followed §5.2 verbatim after a total-loss event recovered the control
// plane and permanently lost every session transcript and the audit trail — and was told nothing.
//
// R10 chose option (b) of the two-way decision: the bundle STAYS state.db-only, and BOTH ends warn
// unmissably. Rationale (recorded here because the choice is load-bearing):
//
//   - JetStream lives in the nats-server process's own store, not in tether's SQLite file. Pulling it
//     into the bundle means tether becomes a JetStream backup tool: it would have to talk to a LIVE
//     nats-server (so `backup --offline`, whose entire premise is a stopped daemon, could not produce
//     a complete bundle — the offline and online bundles would silently differ in scope, which is the
//     same class of lie #53 is about), and restore would have to re-import streams into a JS meta that
//     does not exist yet at restore time (the daemon is stopped; the conf may still be clustered).
//   - nats-server already ships the correct, supported tool for this (`nats stream backup/restore`),
//     and the codebase already points operators at it for the grow/shrink JS-store resets. A second,
//     tether-flavored, less-tested copy of that machinery is a liability, not a feature.
//   - What was actually unacceptable was the SILENCE, not the scope. Warning at both ends costs
//     nothing and removes the failure mode entirely.
//
// The verbs below are deliberately imperative and identical in both directions, so an operator who
// reads only one of them still learns the whole story.

// JetStreamBackupScopeWarning is printed by `cluster backup` (online AND offline) right after a
// successful bundle write.
const JetStreamBackupScopeWarning = "" +
	"⚠ BUNDLE SCOPE: this bundle contains the FSM state DB ONLY (roster, ports, sessions, alerts,\n" +
	"  the applied cursor). JetStream is NOT in it — per-session history (history-<sid>), the events\n" +
	"  stream, the forensic incident bundle and any in-flight OBJ_xfer live in the nats-server\n" +
	"  JetStream store, which no tether backup reads. Restoring this bundle brings back the control\n" +
	"  plane with EMPTY history/audit, and the audit re-derive cursor is reset to 0 so nothing\n" +
	"  backfills it.\n" +
	"  To have a recoverable history/audit, back JetStream up SEPARATELY, next to every bundle:\n" +
	"      for s in $(nats stream ls -n); do nats stream backup \"$s\" \"<js-backup-dir>/$s\"; done\n" +
	"  See docs/cluster-runbook.md §5 (backup & disaster recovery)."

// JetStreamRestoreScopeWarning is printed by `cluster recovery restore` on completion. Same facts,
// stated as what just happened.
const JetStreamRestoreScopeWarning = "" +
	"⚠ HISTORY/AUDIT NOT RESTORED: the bundle carried the FSM state DB only, so this node came back\n" +
	"  with EMPTY JetStream — no per-session history (history-<sid>), no events stream, no forensic\n" +
	"  incident bundle. The audit re-derive cursor was reset to 0 and does NOT backfill.\n" +
	"  If you took a JetStream backup alongside the bundle, restore it AFTER nats-server is up on the\n" +
	"  final (standalone) conf:\n" +
	"      for d in <js-backup-dir>/*; do nats stream restore \"$(basename \"$d\")\" \"$d\"; done\n" +
	"  See docs/cluster-runbook.md §5 (backup & disaster recovery)."
