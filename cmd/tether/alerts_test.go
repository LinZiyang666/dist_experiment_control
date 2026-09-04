package main

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// origin: d8_alerts_test.go (renamed in B6) — docs/reviews/d8-external-review.md
//
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

// origin: prerelease audit increment 2 internal review, pairing-sweep/IMPACT-F1.
//
// A DESTRUCTIVE OPERATION MUST NOT READ "I COULD NOT ASK" AS "THE CLUSTER IS FINE".
//
// probeClusterHealth used to fold every failure — a refused SUBSCRIBE on its own reply
// inbox, a refused publish, a timeout, an N=1 deployment with no responder — into an
// empty slice, and proto.EvalDestructiveGate of an empty slice is an UNBLOCKED gate. So
// a ctl whose inbox was refused proceeded with `session rm` / `cluster retire` having
// learned nothing, and the safety gate failed open on exactly the misconfiguration it
// was written to survive.
//
// Both directions are asserted, because either alone is satisfied by a broken
// implementation: a gate that always blocks would pass the first case and make N=1
// unusable, and one that never blocks would pass the second and be the bug.
func TestADestructiveOpBlocksWhenTheHealthProbeCannotBeMade(t *testing.T) {
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()

	cases := []struct {
		name      string
		subAllow  []string
		wantBlock bool
	}{
		{
			// The failure this test exists for: the identity may publish, but may not
			// subscribe to its own reply inbox. Every probe is impossible.
			name:      "own reply inbox refused",
			subAllow:  []string{"nothing.at.all"},
			wantBlock: true,
		},
		{
			// N=1: the probe WORKS, nobody answers, and the gate correctly does not fire.
			name:      "probe works and nobody answers",
			subAllow:  []string{">"},
			wantBlock: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := natstest.DefaultTestOptions
			opts.Port = -1
			opts.Nkeys = []*natsserver.NkeyUser{{
				Nkey: pub,
				Permissions: &natsserver.Permissions{
					Publish:   &natsserver.SubjectPermission{Allow: []string{">"}},
					Subscribe: &natsserver.SubjectPermission{Allow: tc.subAllow},
				},
			}}
			ns := natstest.RunServer(&opts)
			t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
			if !ns.ReadyForConnections(2 * time.Second) {
				t.Fatal("embedded nats-server not ready")
			}

			signer := func(nonce []byte) ([]byte, error) {
				k, err := nkeys.FromSeed(seed)
				if err != nil {
					return nil, err
				}
				defer k.Wipe()
				return k.Sign(nonce)
			}
			nc, err := nats.Connect(ns.ClientURL(), nats.Nkey(pub, signer), nats.Timeout(4*time.Second))
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer nc.Close()

			err = gateDestructive(nc, pub, false)
			if tc.wantBlock && err == nil {
				t.Fatal("the destructive gate allowed the operation although its health probe could " +
					"not be made.\n\n" +
					"An empty health report must mean \"nobody answered\", never \"I was refused\": " +
					"the second one is a deployment problem, and proceeding through it is how a " +
					"`session rm` lands on a cluster that has lost quorum.")
			}
			if !tc.wantBlock && err != nil {
				t.Fatalf("the destructive gate blocked a healthy N=1 deployment: %v.\n\n"+
					"At N=1 no broker subscribes the health subject, the probe legitimately returns "+
					"nothing, and every destructive verb must still work.", err)
			}
			// --ack-alerts remains the escape hatch in BOTH cases.
			if err := gateDestructive(nc, pub, true); err != nil {
				t.Fatalf("--ack-alerts did not override the gate: %v", err)
			}
		})
	}
}
