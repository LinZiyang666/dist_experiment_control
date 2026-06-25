package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// TestB5CertExpiryAdvisory (B5 plan §F.14): a cert whose window is within window/8 yields an
// advisory; a fresh cert (just rotated, full window) and an unset cert yield nothing. Advisory
// only — this function never touches health/ExitCode (the caller appends it to the banner).
func TestB5CertExpiryAdvisory(t *testing.T) {
	window := 24 * time.Hour // certRotationWindow default; threshold = window/8 = 3h
	near := []adminsock.ClusterNodeStatus{
		{NodeID: "brk-a", CertValidSecs: int64((2 * time.Hour).Seconds())}, // < 3h → near
	}
	if adv := certExpiryAdvisory(near, window); adv == "" || !strings.Contains(adv, "brk-a") {
		t.Errorf("a 2h-remaining cert must produce an advisory naming brk-a, got %q", adv)
	}
	fresh := []adminsock.ClusterNodeStatus{
		{NodeID: "brk-a", CertValidSecs: int64((20 * time.Hour).Seconds())}, // > 3h → silent
	}
	if adv := certExpiryAdvisory(fresh, window); adv != "" {
		t.Errorf("a 20h-remaining cert must be silent, got %q", adv)
	}
	none := []adminsock.ClusterNodeStatus{{NodeID: "brk-a", CertValidSecs: 0}}
	if adv := certExpiryAdvisory(none, window); adv != "" {
		t.Errorf("an unset cert must be silent, got %q", adv)
	}
}

// TestB5MetricsSnapshotSingleMode (B5 plan §F.2/§F.17): a single broker (clusterMode=false, cl=nil)
// produces an honest degenerate snapshot — ClusterMode false, no panic, no fabricated raft gauges —
// and /readyz reports ready ("single"). This is the off-by-default byte-equivalence guarantee.
func TestB5MetricsSnapshotSingleMode(t *testing.T) {
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger()}}
	snap := b.metricsSnapshot()
	if snap.ClusterMode {
		t.Error("single-mode snapshot must report ClusterMode=false")
	}
	if snap.IsLeader || snap.Voters != 0 || len(snap.Peers) != 0 {
		t.Errorf("single-mode snapshot must not fabricate raft/peer gauges: %+v", snap)
	}
	if snap.AlertsActive != 0 { // empty alerts table
		t.Errorf("single-mode alerts_active = %d, want 0", snap.AlertsActive)
	}
	ok, reason := b.metricsReady()
	if !ok || reason != "single" {
		t.Errorf("single-mode /readyz = (%v,%q), want (true,\"single\")", ok, reason)
	}
}
