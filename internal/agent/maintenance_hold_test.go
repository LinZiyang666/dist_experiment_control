package agent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// The leader's proxy reaper re-publishes an UNCHANGED keyset directive
// (verb proxy-keys) to every healthy+ready proxy node on every 5s observe tick
// (proxy_reconcile.go healthy+ready branch -> pushProxyDirectiveWithHome ->
// proxy.go pushProxyDirective). dispatchAgentMaintenance ends the installed
// hold on ANY proxy-keys/home/expose-rm subject before decoding, authorizing
// or comparing it, so a routine nudge (not a revoke) drops the route hold and
// re-enables silence escape inside the maintenance window (plan §7.3 / T44).
// origin: hot-update S6 internal review rv-cluster
func TestMaintenanceHoldSurvivesUnchangedProxyKeyNudge(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseMaintenanceNudgeAndRevoke(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "maintenance review x5", beforeG)
	testharness.AssertNoFDLeak(t, "maintenance review x5", beforeFD, 0)
}
func exerciseMaintenanceNudgeAndRevoke(t *testing.T) {
	t.Helper()

	f := newAgentMaintenanceFixture(t, false)
	installMaintenanceTestHold(t, f)
	if !agentMaintenanceProtects(f.a, f.nc) {
		t.Fatal("precondition: hold not installed")
	}
	nudge, err := json.Marshal(proto.ProxyDirective{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	msg := &nats.Msg{Subject: proto.SubjCmdForwarded(f.a.cfg.SID, nidOf(f.a), "proxy-keys"), Data: nudge}
	if dispatchAgentMaintenance(f.a, f.nc, msg) {
		t.Fatal("proxy-keys must still reach the ordinary dispatcher")
	}
	f.a.handleProxyKeysForwarded(f.nc, msg, 1)
	if !agentMaintenanceProtects(f.a, f.nc) {
		t.Error("an unchanged proxy-keys nudge (not a revoke) ended the installed maintenance hold")
	}
	f.a.handleProxyKeysForwarded(f.nc, &nats.Msg{Data: []byte(`{"enabled":false,"epoch":1}`)}, 2)
	if agentMaintenanceProtects(f.a, f.nc) {
		t.Error("validated proxy revoke did not end the hold")
	}
}

func brokermaintSign(t *testing.T, seed []byte) brokermaint.SignProof {
	return func(b []byte) (string, error) {
		sig, err := auth.SignWithSeed(seed, b)
		if err != nil {
			return "", err
		}
		return base64.RawStdEncoding.EncodeToString(sig), nil
	}
}

// P15 owns the shared canonical codec (plan §9.1: "maintenanceReq/Ack
// canonical ... P15 codec 实现"). The agent adapter signs and verifies with a
// different domain prefix and a different body (proof Sig included), and its
// ProofDigest hashes a different byte string. A broker that issues proofs with
// brokermaint.MakeProof, signs the request with CanonicalRequestBytes and
// aggregates ACKs with MaintenanceCutDeadline/CanonicalAckBytes can therefore
// never install a hold nor verify an ACK. The P15 request codec also cannot
// encode the agent's proof-less first-leg challenge at all.
// origin: hot-update S6 internal review rv-cluster
func TestAgentMaintenanceUsesSharedBrokermaintCodec(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseMaintenanceSharedCodec(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "maintenance review x5", beforeG)
	testharness.AssertNoFDLeak(t, "maintenance review x5", beforeFD, 0)
}
func exerciseMaintenanceSharedCodec(t *testing.T) {
	t.Helper()

	f := newAgentMaintenanceFixture(t, false)
	f.req.Record.VoterDigest = strings.Repeat("a", 64)
	f.req.Record.BaselineDigest = strings.Repeat("b", 64)
	if err := brokermaint.ValidateRecord(f.req.Record); err != nil {
		t.Fatalf("fixture record invalid for P15: %v", err)
	}
	if _, err := brokermaint.CanonicalRequestBytes(f.req.Record, brokermaint.Proof{}, f.req.NID, f.req.ConnectedBrokerID, f.req.IngressServerID, ""); err == nil {
		t.Log("P15 codec can encode the proof-less challenge leg")
	} else {
		t.Errorf("P15 request codec cannot encode the agent's proof-less challenge leg: %v", err)
	}
	challenge := maintenanceTestChallenge(t, f) // agent's own first-leg format
	proof, err := brokermaint.MakeProof(f.req.Record, challenge.Nonce, f.req.Record.IssuerBootID,
		f.req.Record.IssuedBootNS+int64(time.Second), 1, brokermaintSign(t, f.issuer))
	if err != nil {
		t.Fatal(err)
	}
	req := f.req
	req.Proof, req.RouteDigest = proof, challenge.RouteDigest
	// P15 route rule: a quorum/sole-voter request carries an EMPTY ingress
	// server ID; the agent requires it to equal the live NATS server ID.
	if _, err := brokermaint.CanonicalRequestBytes(req.Record, req.Proof, req.NID, req.ConnectedBrokerID, req.IngressServerID, req.RouteDigest); err != nil {
		t.Errorf("P15 codec rejects the agent's quorum route shape (ingress=%q): %v", req.IngressServerID, err)
	}
	req.IngressServerID = ""
	body, err := brokermaint.CanonicalRequestBytes(req.Record, req.Proof, req.NID, req.ConnectedBrokerID, req.IngressServerID, req.RouteDigest)
	if err != nil {
		t.Fatal(err)
	}
	if req.Sig, err = brokermaintSign(t, f.issuer)(body); err != nil {
		t.Fatal(err)
	}
	if _, err := acceptAgentMaintenance(f.a, f.nc, req); err != nil {
		t.Errorf("agent rejects a P15-signed proof/request: %v", err)
	}

	// Independently: an ACK the agent signs today does not verify under P15.
	g := newAgentMaintenanceFixture(t, false)
	installed := installMaintenanceTestHold(t, g)
	ack, err := acceptAgentMaintenance(g.a, g.nc, installed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, ack.IngressServerID,
		ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS); err != nil {
		t.Errorf("P15 codec rejects the agent's quorum ACK route shape (ingress=%q): %v", ack.IngressServerID, err)
	}
	ackBytes, err := brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, "",
		ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := base64.RawStdEncoding.DecodeString(ack.Sig)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifySignature(g.a.cfg.Identity.PublicKey, ackBytes, sig); err != nil {
		t.Errorf("agent ACK does not verify under brokermaint.CanonicalAckBytes: %v", err)
	}
	if want, err := brokermaint.ProofDigest(installed.Proof); err != nil || want != ack.ProofDigest {
		t.Errorf("agent ProofDigest %q != brokermaint.ProofDigest %q (%v)", ack.ProofDigest, want, err)
	}
}
