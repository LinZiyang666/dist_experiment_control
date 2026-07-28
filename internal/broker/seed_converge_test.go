package broker

import (
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// g3_seed_converge_test.go — G3 #1 pure-function tests for the seed auto-derive/converge helpers
// (DeriveSeedEndpoints / seedSetEqual / seedHostsMatchAnyBroker). No NATS/DB — adversarial table cases
// pinning the load-bearing invariants: path preservation, nats:// refusal, undialable skip, deterministic
// order (change-gate fuel), the 8-cap keeping VOTERs, and the host-match clobber guard.

func vb(host, phase string) proto.RosterBroker {
	return proto.RosterBroker{PublicHost: host, Phase: phase}
}

func TestDeriveSeedEndpoints(t *testing.T) {
	tests := []struct {
		name    string
		prev    []string
		brokers []proto.RosterBroker
		want    []string
	}{
		{
			name: "prev empty -> nil (never bootstrap first publish)",
			prev: nil, brokers: []proto.RosterBroker{vb("b1", proto.RosterPhaseVoter)}, want: nil,
		},
		{
			name: "grow converge, path preserved",
			prev: []string{"wss://b1:443/nats"},
			brokers: []proto.RosterBroker{
				vb("b1", proto.RosterPhaseVoter), vb("b2", proto.RosterPhaseVoter),
			},
			want: []string{"wss://b1:443/nats", "wss://b2:443/nats"},
		},
		{
			name:    "shrink: retired node absent from roster drops out",
			prev:    []string{"wss://b1:443/nats", "wss://b2:443/nats"},
			brokers: []proto.RosterBroker{vb("b1", proto.RosterPhaseVoter)},
			want:    []string{"wss://b1:443/nats"},
		},
		{
			name:    "domain migration: re-template onto the current host (pc732 drops out)",
			prev:    []string{"wss://pc732:443"},
			brokers: []proto.RosterBroker{vb("racknerd", proto.RosterPhaseVoter)},
			want:    []string{"wss://racknerd:443"},
		},
		{
			name: "undialable hosts skipped (loopback / 0.0.0.0 / empty)",
			prev: []string{"wss://b1:443"},
			brokers: []proto.RosterBroker{
				vb("b1", proto.RosterPhaseVoter), vb("localhost", proto.RosterPhaseVoter),
				vb("127.0.0.1", proto.RosterPhaseVoter), vb("0.0.0.0", proto.RosterPhaseVoter),
				vb("", proto.RosterPhaseVoter),
			},
			want: []string{"wss://b1:443"},
		},
		{
			name:    "nats:// template refused (no auto cleartext-PIN propagation)",
			prev:    []string{"nats://b1:4222"},
			brokers: []proto.RosterBroker{vb("b1", proto.RosterPhaseVoter)},
			want:    nil,
		},
		{
			name:    "non-tls/wss scheme refused",
			prev:    []string{"http://b1:80"},
			brokers: []proto.RosterBroker{vb("b1", proto.RosterPhaseVoter)},
			want:    nil,
		},
		{
			name:    "unparseable template -> nil",
			prev:    []string{"ht tp://:bad"},
			brokers: []proto.RosterBroker{vb("b1", proto.RosterPhaseVoter)},
			want:    nil,
		},
		{
			name: "VOTER preferred over transient/draining, each tier sorted",
			prev: []string{"tls://x:6222"},
			brokers: []proto.RosterBroker{
				vb("d1", proto.RosterPhaseDraining), vb("v2", proto.RosterPhaseVoter),
				vb("c1", proto.RosterPhaseCatchUp), vb("v1", proto.RosterPhaseVoter),
			},
			want: []string{"tls://v1:6222", "tls://v2:6222", "tls://c1:6222", "tls://d1:6222"},
		},
		{
			name:    "duplicate public_host collapses to one URL (m16)",
			prev:    []string{"wss://x:443"},
			brokers: []proto.RosterBroker{vb("dup", proto.RosterPhaseVoter), vb("dup", proto.RosterPhaseVoter)},
			want:    []string{"wss://dup:443"},
		},
		{
			name:    "VOTER_ADD_FAILED broker excluded from seeds (m11)",
			prev:    []string{"wss://x:443"},
			brokers: []proto.RosterBroker{vb("ok", proto.RosterPhaseVoter), vb("failed", proto.RosterPhaseAddFailed)},
			want:    []string{"wss://ok:443"},
		},
		{
			name:    "mixed VIP+broker set: VIP dropped on rebuild, template from broker prev[0] (m8)",
			prev:    []string{"wss://b1:443", "tls://vip.example:8443"},
			brokers: []proto.RosterBroker{vb("b1", proto.RosterPhaseVoter), vb("b2", proto.RosterPhaseVoter)},
			want:    []string{"wss://b1:443", "wss://b2:443"},
		},
		{
			name:    "garbage prev[0] falls through to next parseable tls/wss template (m18)",
			prev:    []string{"ht tp://:bad", "wss://b1:443"},
			brokers: []proto.RosterBroker{vb("b2", proto.RosterPhaseVoter)},
			want:    []string{"wss://b2:443"},
		},
		{
			name:    "nats:// prev[0] falls through to a tls/wss template, never propagates cleartext (m18/security)",
			prev:    []string{"nats://b1:4222", "wss://b1:443"},
			brokers: []proto.RosterBroker{vb("b2", proto.RosterPhaseVoter)},
			want:    []string{"wss://b2:443"},
		},
		{
			name:    "prev[0] is a VIP: derived broker endpoint inherits VIP's port — WRONG port (m9 documented hazard)",
			prev:    []string{"wss://vip.example:8443", "wss://self.example:443"},
			brokers: []proto.RosterBroker{vb("self.example", proto.RosterPhaseVoter)},
			want:    []string{"wss://self.example:8443"}, // documents the mixed-set template hazard (durable custom → InviteSeeds)
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeriveSeedEndpoints(tt.prev, tt.brokers)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("DeriveSeedEndpoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeriveSeedEndpointsCapKeepsVoters(t *testing.T) {
	// 9 VOTERs + 1 draining; cap is 8 and must keep VOTERs (the draining one must be dropped).
	var brokers []proto.RosterBroker
	for _, h := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		brokers = append(brokers, vb(h, proto.RosterPhaseVoter))
	}
	brokers = append(brokers, vb("zdrain", proto.RosterPhaseDraining))
	got := DeriveSeedEndpoints([]string{"wss://x:443"}, brokers)
	if len(got) != 8 {
		t.Fatalf("cap: got %d endpoints, want 8", len(got))
	}
	for _, u := range got {
		if u == "wss://zdrain:443" {
			t.Fatalf("cap kept the draining node over a VOTER: %v", got)
		}
	}
}

func TestDeriveSeedEndpointsDeterministic(t *testing.T) {
	// Same broker set in DIFFERENT input order must derive byte-identical output (feeds the change-gate;
	// a non-deterministic set would churn the non-change-gated seed_generation).
	prev := []string{"wss://x:443"}
	a := DeriveSeedEndpoints(prev, []proto.RosterBroker{
		vb("b3", proto.RosterPhaseVoter), vb("b1", proto.RosterPhaseVoter), vb("b2", proto.RosterPhaseVoter),
	})
	b := DeriveSeedEndpoints(prev, []proto.RosterBroker{
		vb("b1", proto.RosterPhaseVoter), vb("b2", proto.RosterPhaseVoter), vb("b3", proto.RosterPhaseVoter),
	})
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("non-deterministic: %v vs %v", a, b)
	}
}

func TestSeedSetEqual(t *testing.T) {
	tests := []struct {
		a, b []string
		want bool
	}{
		{[]string{"x", "y"}, []string{"y", "x"}, true}, // order-independent
		{[]string{"x"}, []string{"x"}, true},
		{[]string{"x", "y"}, []string{"x"}, false},
		{nil, nil, true},
		{[]string{"x"}, []string{"y"}, false},
	}
	for _, tt := range tests {
		if got := seedSetEqual(tt.a, tt.b); got != tt.want {
			t.Errorf("seedSetEqual(%v,%v)=%v want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestSeedHostsMatchAnyBroker(t *testing.T) {
	brokers := []proto.RosterBroker{vb("racknerd", proto.RosterPhaseVoter)}
	if !seedHostsMatchAnyBroker([]string{"wss://racknerd:443"}, brokers) {
		t.Error("stored set containing a broker public_host must match (broker-endpoint set → take over)")
	}
	if seedHostsMatchAnyBroker([]string{"wss://vip.lb.example:443"}, brokers) {
		t.Error("stored set matching NO broker must NOT match (operator VIP/LB set → hands-off)")
	}
	if seedHostsMatchAnyBroker(nil, brokers) {
		t.Error("empty stored set must not match")
	}
	// m7 (documented limitation): a stored set of ONLY departed-peer endpoints (the survivor's public_host
	// absent) matches no current broker → hands-off, exactly like a VIP set → the dead endpoint lingers.
	// host-match cannot distinguish "dead-peer-only" from "operator VIP". Not fatal (bootstrap/cfg floor
	// covers cold start); pinned so a future survivor-anchored improvement can flip this GAP to GREEN.
	if seedHostsMatchAnyBroker([]string{"wss://dead-peer:443"}, brokers) {
		t.Error("m7 GAP: dead-peer-only set matches no live broker (documents host-match limitation)")
	}
}

func TestDeriveSeedEndpointsExactlyEight(t *testing.T) {
	// Exactly MaxSeedEndpoints dialable brokers → all kept (no off-by-one truncation at the cap boundary).
	var brokers []proto.RosterBroker
	for _, h := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		brokers = append(brokers, vb(h, proto.RosterPhaseVoter))
	}
	if got := DeriveSeedEndpoints([]string{"wss://x:443"}, brokers); len(got) != 8 {
		t.Fatalf("exactly-8 dialable: got %d endpoints, want all 8 kept (no truncation)", len(got))
	}
}
