package broker

import (
	"strings"
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

// TestTunnelCertPinMismatchErrorPointsAtFileRestore (R11 P12/DOC-23): the wireClusterEarly
// pin-mismatch error runs BEFORE the admin socket is up, so its remedy must be a reachable FILE
// restore — it must NOT point at `tether cluster rotate-tunnel-cert` (which needs that socket).
func TestTunnelCertPinMismatchErrorPointsAtFileRestore(t *testing.T) {
	self := &clusternodes.HomeNode{CertFP: "sha256:pinned", CertFPPrev: "sha256:older"}
	err := tunnelCertPinMismatchError("sha256:ondisk", self, "brk-a", "/etc/tether/secrets")
	msg := err.Error()

	// The old, dead-ending remedy must be gone.
	if strings.Contains(msg, "rotate-tunnel-cert") {
		t.Fatalf("bricked-state error must NOT point at rotate-tunnel-cert (admin socket is down); got %q", msg)
	}
	// It must name the reachable file restore: the previous cert + key files under the secrets dir.
	for _, want := range []string{secretTunnelCert, secretTunnelKey, "/etc/tether/secrets", "restore", "restart"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("recovery guidance must mention %q; got %q", want, msg)
		}
	}
}
