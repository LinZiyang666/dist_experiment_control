package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/natsconf"
)

// origin: s6_s8_external_review_test.go (renamed in B6) — docs/reviews/s6-s8-external-review.md
func TestHealthContactStaleFailsClosedForQuorumLostExLeader(t *testing.T) {
	tests := []struct {
		name                          string
		fenced, stateLeader, writable bool
		leaderID, selfID              string
		want                          bool
	}{
		{"writable leader", false, true, true, "self", "self", false},
		{"leader state but failed barrier", false, true, false, "self", "self", true},
		{"fresh follower with another leader", false, false, false, "other", "self", false},
		{"leaderless candidate", false, false, false, "", "self", true},
		{"demoted ex-leader still naming self", false, false, false, "self", "self", true},
		{"fenced follower", true, false, false, "other", "self", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := healthContactStale(tt.fenced, tt.stateLeader, tt.writable, tt.leaderID, tt.selfID); got != tt.want {
				t.Fatalf("healthContactStale(%v,%v,%v,%q,%q)=%v want %v", tt.fenced, tt.stateLeader, tt.writable, tt.leaderID, tt.selfID, got, tt.want)
			}
		})
	}
}

func TestOptionalStableTunnelCertPairIsFailClosed(t *testing.T) {
	dir := t.TempDir()
	cert, present, err := loadOptionalStableTunnelCert(dir)
	if err != nil || present || cert != nil {
		t.Fatalf("empty optional cert dir = (%v,%v,%v), want (nil,false,nil)", cert, present, err)
	}
	if err := os.WriteFile(filepath.Join(dir, secretTunnelCert), []byte("not-a-certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, present, err = loadOptionalStableTunnelCert(dir)
	if err == nil || present || cert != nil {
		t.Fatalf("partial optional cert pair = (%v,%v,%v), want (nil,false,error)", cert, present, err)
	}
	if !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("partial-pair error must be actionable, got %q", err)
	}
}

func TestTopologyRestartDueUsesDistinctStableRosterSlots(t *testing.T) {
	peers := []natsconf.Broker{{ServerName: "brk3"}, {ServerName: "brk1"}, {ServerName: "brk2"}}
	confTime := time.Unix(1_700_000_000, 0)

	if topologyRestartDue("brk1", peers, confTime, confTime.Add(topoRestartBaseDelay-time.Nanosecond)) {
		t.Fatal("first peer restarted before the base delay")
	}
	if !topologyRestartDue("brk1", peers, confTime, confTime.Add(topoRestartBaseDelay)) {
		t.Fatal("first peer did not become due at its slot")
	}
	if topologyRestartDue("brk2", peers, confTime, confTime.Add(topoRestartBaseDelay)) {
		t.Fatal("second peer shared the first peer's restart slot")
	}
	if !topologyRestartDue("brk2", peers, confTime, confTime.Add(topoRestartBaseDelay+topoRestartSpacing)) {
		t.Fatal("second peer did not become due at its own slot")
	}
	if topologyRestartDue("missing", peers, confTime, confTime.Add(time.Hour)) {
		t.Fatal("a node absent from the replicated peer set must never restart")
	}
}
