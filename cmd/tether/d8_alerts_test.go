package main

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// TestRenderBannerSevereOnly: the always-on banner renders SEVERE alerts only (INFO kinds live
// in `alert ls`, no alert fatigue), and --json mode suppresses it entirely.
func TestRenderBannerSevereOnly(t *testing.T) {
	alerts := []proto.AlertView{
		{Kind: "replication_degraded", Severity: proto.SeveritySevere, Message: "replicas below target"},
		{Kind: "below_quorum", Severity: proto.SeverityInfo, Message: "tolerates 0 failures"},
	}
	var sb strings.Builder
	renderBanner(&sb, alerts, false)
	out := sb.String()
	if !strings.Contains(out, "replication_degraded") {
		t.Fatalf("severe alert not in banner: %q", out)
	}
	if strings.Contains(out, "below_quorum") {
		t.Fatalf("INFO alert leaked into the always-on banner: %q", out)
	}

	var sj strings.Builder
	renderBanner(&sj, alerts, true) // --json suppresses
	if sj.Len() != 0 {
		t.Fatalf("--json must suppress the banner, got %q", sj.String())
	}
}

// TestGateBlockMessageNoNuclearLeak (B1 item 5): the destructive-gate message must NEVER name the
// operator-only escape hatches (force-single / recover) to a regular ctl user, must point at the
// read-only diagnosis path + runbook, and must say "condition" not "alert".
func TestGateBlockMessageNoNuclearLeak(t *testing.T) {
	cases := []struct {
		name        string
		gate        proto.DestructiveGate
		wantBullets []string
		notBullets  []string
	}{
		{"quorum-lost only", proto.DestructiveGate{QuorumLost: true}, []string{"quorum_lost"}, []string{"force_single_active"}},
		{"force-single only", proto.DestructiveGate{ForceSingleActive: true}, []string{"force_single_active"}, []string{"quorum_lost"}},
		{"both", proto.DestructiveGate{QuorumLost: true, ForceSingleActive: true}, []string{"quorum_lost", "force_single_active"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := gateBlockMessage(c.gate)
			// The security-critical property: no nuclear operator verbs.
			for _, banned := range []string{"force-single", "recover"} {
				if strings.Contains(msg, banned) {
					t.Errorf("message leaks the operator-only command %q: %q", banned, msg)
				}
			}
			// The safe diagnosis path + override are present, and "condition" not "alert".
			for _, want := range []string{"tether cluster status", "cluster-runbook.md §3", "--ack-alerts", "condition"} {
				if !strings.Contains(msg, want) {
					t.Errorf("message missing %q: %q", want, msg)
				}
			}
			for _, want := range c.wantBullets {
				if !strings.Contains(msg, want) {
					t.Errorf("message missing bullet %q: %q", want, msg)
				}
			}
			for _, no := range c.notBullets {
				if strings.Contains(msg, no) {
					t.Errorf("message has unexpected bullet %q: %q", no, msg)
				}
			}
		})
	}
}

// TestGateDestructiveAckOverride: --ack-alerts short-circuits the gate (and does not even probe,
// so a nil conn is fine — proving the override is honored before any cluster contact).
func TestGateDestructiveAckOverride(t *testing.T) {
	if err := gateDestructive(nil, "actor", true); err != nil {
		t.Fatalf("--ack-alerts override must return nil without probing, got %v", err)
	}
}
