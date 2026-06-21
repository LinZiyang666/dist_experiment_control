package cluster

// applyCommitGate, when non-nil, is invoked inside FSM.Apply immediately BEFORE
// the SQLite commit, with the entry's raft index. It is a TEST-ONLY seam (nil in
// production, negligible nil-check cost) used by the §13.4 kill-9 crash matrix to
// land a SIGKILL provably inside the "raft committed, SQLite NOT yet committed"
// window (architecture §3.7 FP1) rather than racing on stdout. It is unexported,
// so only in-package (internal/cluster) tests can set it; never set in prod.
var applyCommitGate func(index uint64)

// applyFailHook, when non-nil, is invoked inside FSM.Apply just before the commit
// and may return an error to simulate a transient infra failure (Begin/Commit) —
// used to exercise the fail-stop retry+panic path (§3.7 D1 amendment). Test-only.
var applyFailHook func(index uint64) error

// backupStepGate, when non-nil, is invoked inside the online-backup loop after the
// FIRST Step has copied pages (so the destination temp file exists but the backup
// is incomplete). Used by the §13.4 fp3 fault point to SIGKILL the child mid-backup
// and prove the live DB is untouched. Test-only (nil in production).
var backupStepGate func()
