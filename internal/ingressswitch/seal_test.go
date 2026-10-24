package ingressswitch

import (
	"errors"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: S9 internal review s9-p9eng F07
func TestRetirementSealAcceptsOnlyEmptyWildcardListeners(t *testing.T) {
	for _, returning := range []bool{false, true} {
		for _, peer := range []string{"0.0.0.0:0", "[::]:0"} {
			t.Run(peer+map[bool]string{false: "/primary", true: "/shadow"}[returning], func(t *testing.T) {
				s, backend, records, evidence, _ := controllerFixture(t, returning)
				backend.current = records.change.After
				listener := proofFixture(evidence.seal.Owner, !returning).Listener
				listener.Remote = peer
				evidence.seal.Census.Sockets = []c.Socket{listener}
				if _, err := s.RetirementSeal(records.change, deadline(t), nil); err != nil {
					t.Fatalf("sealed empty listener rejected: %v", err)
				}
			})
		}
	}
}

// origin: S9 internal review s9-p9eng F07
func TestRetirementSealWildcardDoesNotHideResponsibilities(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Seal)
	}{
		{"concrete-peer", func(s *Seal) { s.Census.Sockets[0].Remote = "127.0.0.1:0" }},
		{"concrete-ipv6", func(s *Seal) { s.Census.Sockets[0].Remote = "[::1]:0" }},
		{"wildcard-with-port", func(s *Seal) { s.Census.Sockets[0].Remote = "0.0.0.0:43210" }},
		{"ipv6-with-port", func(s *Seal) { s.Census.Sockets[0].Remote = "[::]:443" }},
		{"unparsed-peer", func(s *Seal) { s.Census.Sockets[0].Remote = "*:0" }},
		{"missing-address", func(s *Seal) { s.Census.Sockets[0].Remote = ":0" }},
		{"missing-port", func(s *Seal) { s.Census.Sockets[0].Remote = "0.0.0.0" }},
		{"scoped-wildcard", func(s *Seal) { s.Census.Sockets[0].Remote = "[::%lo]:0" }},
		{"wrong-owner", func(s *Seal) { s.Owner.StartTicks++ }},
		{"unobserved-owner", func(s *Seal) { s.Census.Sockets[0].Owner.StartTicks++ }},
		{"pending-owner", func(s *Seal) { s.Census.OldOwnerPending = 1 }},
		{"accepted-tcp", func(s *Seal) { s.AcceptedTCP = 1 }},
		{"pending-syn", func(s *Seal) { s.PendingSYN = 1 }},
		{"pending-conntrack", func(s *Seal) { s.PendingConntrack = 1 }},
		{"open-barrier", func(s *Seal) { s.BarrierClosed = false }},
		{"unknown-accepted", func(s *Seal) { s.AcceptedComplete = false }},
		{"unknown-listeners", func(s *Seal) { s.ListenersComplete = false }},
		{"unknown-syn", func(s *Seal) { s.SYNComplete = false }},
		{"unknown-conntrack", func(s *Seal) { s.ConntrackComplete = false }},
		{"unknown-census", func(s *Seal) { s.Census.Complete = false }},
		{"accepted-socket", func(s *Seal) {
			s.Census.Sockets = append(s.Census.Sockets, proofFixture(s.Owner, false).Accepted)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			change := changeFixture(false)
			seal := sealFixture(change)
			listener := proofFixture(seal.Owner, true).Listener
			listener.Remote = "0.0.0.0:0"
			seal.Census.Sockets = []c.Socket{listener}
			tc.edit(&seal)
			if err := VerifySeal(seal, change.After, change.Primary); !errors.Is(err, c.ErrIncomplete) {
				t.Fatalf("unsealed responsibility accepted: %v", err)
			}
		})
	}
}
