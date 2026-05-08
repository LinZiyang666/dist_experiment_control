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
	for _, p := range uc.Permissions.Pub.Allow {
		if p == want {
			found = true
		}
	}
	if !found {
		t.Errorf("expected pub allow %q in unactivated perms; got %v", want, uc.Permissions.Pub.Allow)
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

func TestHandleAgentRoleAllowsWithoutMembership(t *testing.T) {
	h, _ := freshHandler(t)
	ephemeral := freshUserPub(t)
	client := freshUserPub(t)

	respJWT, err := h.Handle(signedRequest(t, ephemeral, client, "tether-agent:lab:lab-1", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if resp.Error != "" {
		t.Fatalf("agent should be allowed in P3, got error %q", resp.Error)
	}

	uc, _ := jwt.DecodeUserClaims(resp.Jwt)
	wantSub := "tether.v1.s.lab.cmd.node.lab-1.*.req.forwarded"
	found := false
	for _, p := range uc.Permissions.Sub.Allow {
		if p == wantSub {
			found = true
		}
	}
	if !found {
		t.Errorf("expected agent sub allow %q; got %v", wantSub, uc.Permissions.Sub.Allow)
	}
}

// PermissionsForUnactivated reference, used by callers — pin import.
var _ = auth.PermissionsForUnactivated
