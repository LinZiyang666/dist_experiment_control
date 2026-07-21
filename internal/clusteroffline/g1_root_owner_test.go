package clusteroffline

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
)

// TestRootAgainstNonRootDir pins the G1 #6 decision helper. An offline op (cluster init /
// force-single / resnapshot / recover / restore) running as root against a data dir owned by a
// DIFFERENT non-root user is the hazard: it creates root-owned tether.lock / raft/ / tether.db that
// a later `sudo -u tether` op cannot open (EACCES at the flock, or cluster.RaftStoreLockedByDaemon's
// HARD error on an unreadable raft.db). The guard only flags a root euid against a non-root-owned
// dir; every consistent-ownership case is silent.
func TestRootAgainstNonRootDir(t *testing.T) {
	cases := []struct {
		name   string
		euid   int
		dirUID uint32
		want   bool
	}{
		{"root euid vs tether-owned dir → warn (the #6 hazard)", 0, 1000, true},
		{"root euid vs root-owned dir → no warn (consistent)", 0, 0, false},
		{"tether euid vs tether-owned dir → no warn (correct usage)", 1000, 1000, false},
		{"tether euid vs root-owned dir → no warn (helper flags root euid only)", 1000, 0, false},
		{"root euid vs some other non-root uid → warn", 0, 42, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RootAgainstNonRootDir(tc.euid, tc.dirUID); got != tc.want {
				t.Fatalf("RootAgainstNonRootDir(%d, %d) = %v, want %v", tc.euid, tc.dirUID, got, tc.want)
			}
		})
	}
}

// TestWarnRootDataDirOwnerBestEffort pins the #6 WARN's best-effort contract (internal-review go-code
// nit): it NEVER panics and NEVER fails the op regardless of dir state — in particular a stat error
// (a root-init into a not-yet-created DataDir, the very case the nudge can't observe) and a nil logger
// are both clean no-ops, so the guard can be called unconditionally before every offline op.
func TestWarnRootDataDirOwnerBestEffort(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	warnRootDataDirOwner(filepath.Join(t.TempDir(), "does-not-exist"), discard) // stat fails → no-op
	warnRootDataDirOwner(t.TempDir(), nil)                                      // nil logger → no-op
	warnRootDataDirOwner(t.TempDir(), discard)                                  // existing dir → no panic
}
