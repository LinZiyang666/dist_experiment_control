package authcallout

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// parseRole is the only piece of pure logic in this package that doesn't
// require a NATS server / DB to exercise; the full handler is covered
// end-to-end by test/p3.
func TestParseRole(t *testing.T) {
	cases := []struct {
		name        string
		want        role
		wantSid     string
		wantNid     string
	}{
		{"tether-cli", roleCtlUnactivated, "", ""},
		{"tether-cli:lab", roleCtlActivated, "lab", ""},
		{"tether-cli:", roleUnknown, "", ""},
		{"tether-agent:lab:lab-1", roleAgent, "lab", "lab-1"},
		{"tether-agent:lab:", roleUnknown, "", ""},
		{"tether-agent::lab-1", roleUnknown, "", ""},
		{"tether-agent:onlyone", roleUnknown, "", ""},
		{"", roleUnknown, "", ""},
		{"tether-foo", roleUnknown, "", ""},
	}
	for _, c := range cases {
		gotRole, gotSid, gotNid := parseRole(c.name)
		if gotRole != c.want || gotSid != c.wantSid || gotNid != c.wantNid {
			t.Errorf("parseRole(%q) = (%d, %q, %q); want (%d, %q, %q)",
				c.name, gotRole, gotSid, gotNid, c.want, c.wantSid, c.wantNid)
		}
	}
}

// freshHandler builds a Handler with a fresh in-memory DB and account key.
// Returned accountPub is needed when constructing fake server requests.
func freshHandler(t *testing.T) (*Handler, string) {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	accountKp, _ := nkeys.CreateAccount()
	accountPub, _ := accountKp.PublicKey()

	return &Handler{
		DB:        db,
		AccountKp: accountKp,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		JWTTTL:    time.Hour,
	}, accountPub
}

// signedRequest builds + signs a fake AuthorizationRequestClaims as if it
// came from a NATS server. Allows feeding the handler without a real server.
func signedRequest(t *testing.T, ephemeralPub, clientNkey, name, token string) string {
	t.Helper()
	serverKp, _ := nkeys.CreateServer()
	serverPub, _ := serverKp.PublicKey()

	rc := jwt.NewAuthorizationRequestClaims(serverPub)
	rc.UserNkey = ephemeralPub
	rc.Server = jwt.ServerID{ID: serverPub, Name: serverPub}
	rc.ConnectOptions = jwt.ConnectOptions{Name: name, Nkey: clientNkey, Token: token}
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// freshUserPub returns the public key of a freshly-created user nkey.
func freshUserPub(t *testing.T) string {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	return pub
}

// seedSessionWithPin inserts an ACTIVE sessions row whose pin_hash matches
// the supplied raw PIN under auth.HashPIN, so subsequent agent CONNECTs in
// a test can present that PIN to bootstrap.
func seedSessionWithPin(t *testing.T, h *Handler, sid, pin string) {
	t.Helper()
	hash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.DB.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?, ?, ?, ?)`,
		sid, sid, "SHA256:test-owner", hash,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestHandleAllowsUnactivated(t *testing.T) {
	h, _ := freshHandler(t)
	ephemeral := freshUserPub(t)
	client := freshUserPub(t)

	respJWT, err := h.Handle(signedRequest(t, ephemeral, client, "tether-cli", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("expected allow, got error %q", resp.Error)
	}

	uc, err := jwt.DecodeUserClaims(resp.Jwt)
	if err != nil {
		t.Fatal(err)
	}
	if uc.Subject != ephemeral {
		t.Errorf("subject: got %q want ephemeral %q", uc.Subject, ephemeral)
	}
	// Pub allow must be locked to the CLIENT's nkey, not the ephemeral.
	want := "tether.v1.ctrl.by." + client + ".session.create.req"
	found := false
	for _, p := range uc.Pub.Allow {
		if p == want {
			found = true
		}
	}
	if !found {
		t.Errorf("expected pub allow %q in unactivated perms; got %v", want, uc.Pub.Allow)
	}
}

func TestHandleDeniesUnknownRole(t *testing.T) {
	h, _ := freshHandler(t)
	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t), "tether-foo", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" || !strings.Contains(resp.Error, "unknown role") {
		t.Errorf("expected unknown-role denial, got %q", resp.Error)
	}
}

func TestHandleDeniesMissingClientNkey(t *testing.T) {
	h, _ := freshHandler(t)
	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), "", "tether-cli", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" || !strings.Contains(resp.Error, "client must present") {
		t.Errorf("expected client nkey required, got %q", resp.Error)
	}
}

func TestHandleDeniesActivatedNonMember(t *testing.T) {
	h, _ := freshHandler(t)
	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t), "tether-cli:lab", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" || !strings.Contains(resp.Error, "not active") {
		t.Errorf("expected session-not-active denial, got %q", resp.Error)
	}
}

// P4 F1: agent role is no longer hard-denied. Without provisioning AND
// without a PIN it must still be rejected — the "anyone can claim any
// agent slot" attack is blocked by requiring either prior PIN-bootstrap
// or a fresh PIN.
func TestHandleAgentRoleDeniedWithoutProvisioningAndPIN(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t),
		"tether-agent:lab:lab-1", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" {
		t.Fatalf("agent without provisioning and without PIN must be denied; got allow")
	}
	if !strings.Contains(resp.Error, "not provisioned") {
		t.Errorf("expected denial to mention provisioning; got %q", resp.Error)
	}
}

// PIN-bootstrap path: first connect with a valid PIN registers the agent;
// a second connect without PIN succeeds because the (sid,nid)→fp binding
// now exists.
func TestHandleAgentRolePINBootstrapAndRebind(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	clientKp, _ := nkeys.CreateUser()
	clientPub, _ := clientKp.PublicKey()

	// Bootstrap: with PIN.
	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), clientPub,
		"tether-agent:lab:lab-1", "test-pin"))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error != "" {
		t.Fatalf("bootstrap must succeed with valid PIN; got %q", resp.Error)
	}

	// Re-connect: no PIN.
	respJWT2, err := h.Handle(signedRequest(t, freshUserPub(t), clientPub,
		"tether-agent:lab:lab-1", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp2, _ := jwt.DecodeAuthorizationResponseClaims(respJWT2)
	if resp2.Error != "" {
		t.Errorf("re-connect after PIN-bootstrap must succeed; got %q", resp2.Error)
	}
}

// A different agent identity arriving with a valid PIN to a slot already
// taken by another fp must be denied. PIN does NOT override the existing
// (sid,nid)→fp binding — operator must explicitly revoke first.
func TestHandleAgentRoleRejectsHijack(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	firstKp, _ := nkeys.CreateUser()
	firstPub, _ := firstKp.PublicKey()
	if _, err := h.Handle(signedRequest(t, freshUserPub(t), firstPub,
		"tether-agent:lab:lab-1", "test-pin")); err != nil {
		t.Fatal(err)
	}

	hijackerKp, _ := nkeys.CreateUser()
	hijackerPub, _ := hijackerKp.PublicKey()
	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), hijackerPub,
		"tether-agent:lab:lab-1", "test-pin"))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" {
		t.Fatalf("hijack of an already-bound nid must be denied; got allow")
	}
	if !strings.Contains(resp.Error, "different agent identity") {
		t.Errorf("expected denial to mention identity binding; got %q", resp.Error)
	}
}

// Wildcard sid/nid in agent role must also be denied (defense in depth —
// even if someone re-enables roleAgent, parseRole returning roleUnknown
// for "*" would route to the unknown-role denial; this test pins that
// exact pattern.)
func TestHandleWildcardAgentRoleDenied(t *testing.T) {
	h, _ := freshHandler(t)
	respJWT, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t), "tether-agent:*:*", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error == "" {
		t.Fatal("tether-agent:*:* must be denied")
	}
}

