package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// repin_test.go (formerly r11_repin_test.go; R11 #63) — establishes, from source, the ACTUAL tunnel-cert re-pin mechanism,
// and pins the residual R6 could not resolve.
//
// FINDING (source-grounded): rotate-tunnel-cert updates the roster's cert_fp but does NOT bump the
// expose epoch. The re-pin the fleet DOES perform (R6 proved it 3/3) rides the FULL-register reply:
// homeForRegister reads the home's cert pins FRESH from the roster, so the next full register carries
// the rotated pin at the SAME epoch, and the agent applies it in place via ApplyHome
// (TestD6ReviewSameEpochPinUpdateQueuedDuringLoop pins the agent half). That register fires on any
// NATS reconnect/boot — which is why an online rotation re-pins in practice, exactly as R6 observed.
//
// RESIDUAL → R14 (pinned by the second test): the R8 ACTIVE home-delivery PUSH does NOT cover a bare
// rotation of a SILENT agent — its convergence oracle (homeAssignmentApplied) is keyed on the EPOCH,
// and a cert rotation leaves the epoch unchanged, so the pass sees "converged" and pushes nothing.
// Closing that gap needs a pin-carrying applied-ack (a wire change) so the pass can detect pin skew —
// out of scope for R11's identity/credentials facets.

// TestRotatedCertPinRidesTheRegisterReply proves the delivery vehicle carries the rotated pin.
func TestRotatedCertPinRidesTheRegisterReply(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "brk-a", clusterMode: true}
	// The HOME broker brk-b, initially pinned to OLD.
	seedClusterNode(t, b, "brk-b", "tether-brk-b", "10.0.0.2:7000", "sha256:OLD", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-b", 1)

	ha1 := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{})
	if ha1 == nil || len(ha1.Directives) != 1 {
		t.Fatalf("want one home directive, got %+v", ha1)
	}
	if got := ha1.Directives[0].CertPins.Current; got != "sha256:OLD" {
		t.Fatalf("pre-rotation pin = %q, want sha256:OLD", got)
	}
	epoch := ha1.Directives[0].Epoch

	// brk-b rotates its OWN tunnel cert (self-only); the roster row is what replicates to brk-a (the
	// home-delivery leader). Simulate that committed result directly.
	valid := time.Now().Add(time.Hour)
	if _, err := db.Exec(
		`UPDATE cluster_nodes SET cert_fp=?, cert_fp_prev=?, cert_fp_valid_until=? WHERE node_id='brk-b'`,
		"sha256:NEW", "sha256:OLD", valid); err != nil {
		t.Fatalf("rotate roster cert: %v", err)
	}

	ha2 := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{})
	if ha2 == nil || len(ha2.Directives) != 1 {
		t.Fatalf("want one home directive post-rotation, got %+v", ha2)
	}
	d := ha2.Directives[0]
	// The delivery vehicle now carries the NEW pin (with OLD as previous, in-window) at the SAME epoch.
	if d.CertPins.Current != "sha256:NEW" {
		t.Fatalf("post-rotation pin = %q, want sha256:NEW (register reply must carry the rotated pin)", d.CertPins.Current)
	}
	if d.CertPins.Previous != "sha256:OLD" {
		t.Fatalf("post-rotation previous pin = %q, want sha256:OLD (rotation window)", d.CertPins.Previous)
	}
	if d.Epoch != epoch {
		t.Fatalf("a cert rotation must NOT bump the epoch: got %d, was %d", d.Epoch, epoch)
	}
}

// TestActivePushIsBlindToBareCertRotation pins the R14 residual: the R8 home-delivery PUSH pass
// treats a rotated-pin-but-same-epoch assignment as ALREADY CONVERGED (epoch-keyed oracle), so a
// SILENT agent gets no active push and must wait for its next full register. If a future batch makes
// the pass pin-aware, this test flips and forces the residual note to be revisited.
func TestActivePushIsBlindToBareCertRotation(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "brk-a", clusterMode: true}
	seedClusterNode(t, b, "brk-b", "tether-brk-b", "10.0.0.2:7000", "sha256:OLD", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-b", 1)

	// The agent already confirmed the current epoch (1) long ago (issued-token ack path, C1/M-1).
	seedHomeApplied(b, 14000, 1)

	// Rotate the cert (epoch unchanged).
	valid := time.Now().Add(time.Hour)
	if _, err := db.Exec(
		`UPDATE cluster_nodes SET cert_fp=?, cert_fp_prev=?, cert_fp_valid_until=? WHERE node_id='brk-b'`,
		"sha256:NEW", "sha256:OLD", valid); err != nil {
		t.Fatalf("rotate roster cert: %v", err)
	}
	ha := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{})
	if ha.Directives[0].CertPins.Current != "sha256:NEW" {
		t.Fatal("sanity: register reply should carry the NEW pin")
	}
	// The active-push oracle still says "converged" (epoch match) → the pass publishes NOTHING. This
	// is the documented #63 residual: a silent agent gets no active re-pin (it re-pins on next register).
	if !b.homeAssignmentApplied(ha) {
		t.Fatal("expected the epoch-keyed oracle to report converged on a bare pin rotation (the #63 residual); " +
			"if this now fails, the active push became pin-aware — revisit the R14 residual note")
	}
}
