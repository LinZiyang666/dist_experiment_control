package tunnel

import (
	"context"
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: control-connection-recovery second internal review, pin upgrades must
// verify an existing fallback transport through every home-directive entry.
func TestApplyHomeUpgradeVerifiesExistingFallback(t *testing.T) {
	for _, matching := range []bool{false, true} {
		t.Run(map[bool]string{false: "foreign-peer", true: "matching-peer"}[matching], func(t *testing.T) {
			h := newReplayTunnelHarness(t)
			h.cli.Close(h.publicPort)
			h.srv.CloseProxy(h.publicPort)
			// Cluster init from an existing single broker backfills home_broker
			// without advancing an allocation's initial epoch.
			h.epoch = 0
			if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
				"", h.epoch, proto.CertPins{}); err != nil {
				t.Fatalf("establish fallback transport: %v", err)
			}
			before, registers := h.session(), h.registers.Load()
			pins := h.pins
			if !matching {
				_, pins = replayTestCertificate(t)
			}
			err := h.cli.ApplyHome(h.publicPort, h.addr, h.epoch, pins)
			if !matching {
				if err == nil {
					t.Fatal("same-epoch ApplyHome accepted a foreign pin without verifying the fallback TLS peer")
				}
				h.cli.mu.Lock()
				got := h.cli.sessions[h.publicPort]
				unchanged := got == before && got.certPins.Current == "" && !got.pinned && got.brokerAddr == ""
				h.cli.mu.Unlock()
				if !unchanged {
					t.Fatal("failed pin upgrade changed the existing fallback's authorization")
				}
			} else {
				if err != nil {
					t.Fatalf("matching pin could not promote the existing transport: %v", err)
				}
				h.cli.mu.Lock()
				got := h.cli.sessions[h.publicPort]
				verified := got == before && got.pinned && got.brokerAddr == h.addr && got.certPins.Current == pins.Current
				h.cli.mu.Unlock()
				if !verified {
					t.Error("successful named-home upgrade did not retain actual peer verification and named-home identity")
				}
				if err := h.cli.ApplyHome(h.publicPort, "", h.epoch, proto.CertPins{}); !errors.Is(err, ErrHomePinsRequired) {
					t.Errorf("a named-home upgrade permitted its next redial to drop certificate verification: %v", err)
				}
			}
			if h.session() != before || h.registers.Load() != registers {
				t.Fatal("same-epoch home update replaced the existing supervisor or issued another REGISTER")
			}
			if err := h.freshRoundTrip(); err != nil {
				t.Fatalf("home update damaged existing forwarding: %v", err)
			}
			if matching {
				if !h.srv.DropTransport(h.publicPort) {
					t.Fatal("no transport to interrupt after the accepted upgrade")
				}
				h.waitRoundTrip(t)
				if h.session() != before || h.registers.Load() <= registers {
					t.Fatal("accepted upgrade did not recover on the same supervisor")
				}
			}
		})
	}
}

// An already named home cannot change its address without a newer epoch.
// A rejected directive must preserve the current supervisor and forwarding.
func TestApplyHomeSameEpochRejectsDifferentAddress(t *testing.T) {
	h := newReplayTunnelHarness(t)
	other := startAdmissionServer(t)
	before, registers := h.session(), h.registers.Load()
	if err := h.cli.ApplyHome(h.publicPort, other.addr, h.epoch, h.pins); err == nil {
		t.Fatal("same-epoch directive reported success for a different home address")
	}
	h.cli.mu.Lock()
	got := h.cli.sessions[h.publicPort]
	unchanged := got == before && got.brokerAddr == h.addr && got.certPins.Current == h.pins.Current
	h.cli.mu.Unlock()
	if !unchanged || h.registers.Load() != registers {
		t.Fatal("rejected address change altered the current home or issued another REGISTER")
	}
	if err := h.freshRoundTrip(); err != nil {
		t.Fatalf("rejected address change damaged existing forwarding: %v", err)
	}
}

// The first named home after an offline single-to-cluster migration can use
// another advertised endpoint at epoch zero. Its actual peer pin is the proof;
// subsequent reconnects must follow that endpoint without replacing live flows.
func TestApplyHomeAdoptsFallbackAddress(t *testing.T) {
	h := newReplayTunnelHarness(t)
	h.cli.Close(h.publicPort)
	h.srv.CloseProxy(h.publicPort)
	h.epoch = 0
	if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		"", h.epoch, proto.CertPins{}); err != nil {
		t.Fatalf("establish fallback transport: %v", err)
	}
	before, registers := h.session(), h.registers.Load()
	oldServer, oldCancel, oldAddr := h.srv, h.serverCancel, h.addr
	// A second control endpoint presents the same authorized broker certificate.
	// It does not own the public port until the old service is stopped below.
	h.startServer(t, "127.0.0.1:0")
	if h.addr == oldAddr {
		t.Fatal("fixture did not create a distinct advertised endpoint")
	}
	if err := h.cli.ApplyHome(h.publicPort, h.addr, h.epoch, h.pins); err != nil {
		t.Fatalf("first named home at the unchanged migration epoch was rejected: %v", err)
	}
	if h.session() != before || h.registers.Load() != registers {
		t.Fatal("naming the fallback replaced its supervisor or sent another REGISTER")
	}
	if err := h.freshRoundTrip(); err != nil {
		t.Fatalf("naming the fallback interrupted existing forwarding: %v", err)
	}
	oldCancel()
	oldServer.Close()
	h.waitRoundTrip(t)
	if h.session() != before || h.registers.Load() <= registers {
		t.Fatal("the existing supervisor did not recover through the advertised endpoint")
	}
	h.cli.mu.Lock()
	got := h.cli.sessions[h.publicPort]
	adopted := got == before && got.pinned && got.brokerAddr == h.addr
	h.cli.mu.Unlock()
	if !adopted {
		t.Fatal("the verified named home was not retained after recovery")
	}
}
