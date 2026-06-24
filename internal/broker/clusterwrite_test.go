package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusternodes"
)

func TestTunnelCertMatchesPinnedHonorsPreviousWindow(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	valid := now.Add(time.Hour)
	expired := now.Add(-time.Nanosecond)

	if !tunnelCertMatchesPinned("sha256:new", &clusternodes.HomeNode{CertFP: "sha256:new"}, now) {
		t.Fatal("current pin should match")
	}
	if !tunnelCertMatchesPinned("sha256:old", &clusternodes.HomeNode{CertFP: "sha256:new", CertFPPrev: "sha256:old", CertValid: &valid}, now) {
		t.Fatal("previous pin should match inside rotation window")
	}
	if tunnelCertMatchesPinned("sha256:old", &clusternodes.HomeNode{CertFP: "sha256:new", CertFPPrev: "sha256:old", CertValid: &expired}, now) {
		t.Fatal("expired previous pin must not match")
	}
	if tunnelCertMatchesPinned("sha256:old", &clusternodes.HomeNode{CertFP: "sha256:new", CertFPPrev: "sha256:old"}, now) {
		t.Fatal("previous pin without valid_until must not match")
	}
}
