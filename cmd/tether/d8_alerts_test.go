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
