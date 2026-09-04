package authcallout

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// D3 handler-seam contract tests (R2 fail-closed + R3 PIN-write). These exercise
// the Handler's seam wiring with fake seams (no real cluster.Node), so the
// allow/deny/no-write contract is deterministic. The real Node.Propose ->
// raft.ErrNotLeader mapping is proven separately in internal/cluster.

func agentReq(t *testing.T, clientPub, sid, nid, pin string) string {
	return signedRequest(t, freshUserPub(t), clientPub, "tether-agent:"+sid+":"+nid, pin)
}

// TestD3PINSeamFollowerTransientDeny: a PIN bootstrap whose write seam reports
// ErrNotLeader is DENIED with a transient (not_leader) reason and writes NO row.
func TestD3PINSeamFollowerTransientDeny(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")
	called := false
	h.ProvisionAgentWrite = func(sid, nid, fp, pin string, now time.Time) error {
		called = true
		return ErrNotLeader // simulate this broker not being the raft leader
	}
	clientPub := freshUserPub(t)

	respJWT, err := h.Handle(agentReq(t, clientPub, "lab", "lab-1", "test-pin"))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" {
		t.Fatal("a follower PIN-bootstrap must be DENIED, not allowed")
	}
	if !strings.Contains(resp.Error, "not_leader") {
		t.Errorf("deny reason must be the transient not_leader marker; got %q", resp.Error)
	}
	if !called {
		t.Error("the ProvisionAgentWrite seam must have been consulted")
	}
	// No row written on this node.
	if _, err := agentprov.Lookup(h.DB, "lab", "lab-1"); err == nil {
		t.Fatal("a not_leader deny must NOT write an agent_provisioning row")
	}
}

// TestD3PINSeamMemberFollowerTransientDeny: same for the ctl member-join write.
func TestD3PINSeamMemberFollowerTransientDeny(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")
	h.JoinMemberWrite = func(sid, fp, pin string, now time.Time) error {
		return ErrNotLeader
	}
	clientPub := freshUserPub(t)

	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), clientPub, "tether-cli:lab", "test-pin"))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" || !strings.Contains(resp.Error, "not_leader") {
		t.Fatalf("member-join on a follower must be a transient not_leader deny; got %q", resp.Error)
	}
}

// TestD3PINSeamLeaderSucceeds: a write seam that succeeds (simulating a leader
// Propose commit) results in an ALLOW. Here the seam performs the real write so
// the binding exists.
func TestD3PINSeamLeaderSucceeds(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")
	h.ProvisionAgentWrite = func(sid, nid, fp, pin string, now time.Time) error {
		return agentprov.ProvisionWithPIN(h.DB, sid, nid, fp, pin, auth.VerifyPIN, now)
	}
	clientPub := freshUserPub(t)

	respJWT, err := h.Handle(agentReq(t, clientPub, "lab", "lab-1", "test-pin"))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error != "" {
		t.Fatalf("leader PIN-bootstrap must succeed; got %q", resp.Error)
	}
	if _, err := agentprov.Lookup(h.DB, "lab", "lab-1"); err != nil {
		t.Fatalf("leader success must have written the binding: %v", err)
	}
}

// TestD3PINSeamPreservesTypedBusinessErrors: a seam returning a typed business
// error (invalid PIN) maps to the SAME deny as today (not a generic not_leader).
func TestD3PINSeamPreservesTypedBusinessErrors(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")
	h.ProvisionAgentWrite = func(sid, nid, fp, pin string, now time.Time) error {
		return agentprov.ErrInvalidPIN
	}
	clientPub := freshUserPub(t)

	respJWT, _ := h.Handle(agentReq(t, clientPub, "lab", "lab-1", "wrong"))
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" || !strings.Contains(resp.Error, "invalid PIN") {
		t.Fatalf("typed business errors must flow through the seam unchanged; got %q", resp.Error)
	}
}

// TestD3FailClosedDeniesAlreadyProvisioned: when LeaderContactStale trips, an
// already-provisioned (no-PIN) reconnect is fenced; with the predicate nil it is
// allowed (regression — production default).
func TestD3FailClosedDeniesAlreadyProvisioned(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")
	clientPub := freshUserPub(t)

	// Bootstrap (direct mutator, nil seams) so the binding exists.
	if resp, _ := jwt.DecodeAuthorizationResponseClaims(mustHandle(t, h, agentReq(t, clientPub, "lab", "lab-1", "test-pin"))); resp.Error != "" {
		t.Fatalf("bootstrap should succeed: %q", resp.Error)
	}

	// nil predicate => allowed (today's behavior).
	if resp, _ := jwt.DecodeAuthorizationResponseClaims(mustHandle(t, h, agentReq(t, clientPub, "lab", "lab-1", ""))); resp.Error != "" {
		t.Fatalf("with no fence predicate, already-provisioned reconnect must be allowed; got %q", resp.Error)
	}

	// Fenced => denied even for a valid binding.
	h.LeaderContactStale = func(time.Time) bool { return true }
	resp, _ := jwt.DecodeAuthorizationResponseClaims(mustHandle(t, h, agentReq(t, clientPub, "lab", "lab-1", "")))
	if resp.Error == "" || !strings.Contains(resp.Error, "fenced") {
		t.Fatalf("a fenced node must DENY an already-provisioned reconnect; got %q", resp.Error)
	}
}

func mustHandle(t *testing.T, h *Handler, req string) string {
	t.Helper()
	out, err := h.Handle(req)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

var _ = nkeys.CreateUser
