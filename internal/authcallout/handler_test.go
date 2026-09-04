package authcallout

import (
	"encoding/base64"
	"io"
	"log/slog"
	"strings"
	"sync"
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
		name    string
		want    role
		wantSid string
		wantNid string
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
	signNonceInto(t, rc, clientNkey)
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// testUserKeys remembers the private half of every user nkey these tests mint, so a
// hand-built request can carry a REAL signature over the nonce.
//
// origin: prerelease audit increment 2 internal review, admission-enforcement/L9-F1.
// Before that finding the handler never checked the signature, so hand-built requests
// did not need one and none of them had one. Rather than thread a keypair through 46
// call sites, the two mint helpers register what they create and the two request
// builders look it up — so a test that wants a valid identity keeps saying exactly what
// it said before, and a request for an nkey nobody minted fails LOUDLY here instead of
// quietly becoming an unsigned request that the handler then denies for the wrong reason.
var testUserKeys sync.Map // pub string -> nkeys.KeyPair

// signNonceInto stamps a fresh nonce and this client's signature over it, the way
// nats-server does when a client presented `sig` on CONNECT (server/auth_callout.go
// fills ClientInformation.Nonce only in that case).
//
// An empty clientNkey is left alone on purpose: that is a test asserting the "must
// present a user nkey" refusal, which the handler reaches before it looks at signatures.
func signNonceInto(t *testing.T, rc *jwt.AuthorizationRequestClaims, clientNkey string) {
	t.Helper()
	if clientNkey == "" {
		return
	}
	v, ok := testUserKeys.Load(clientNkey)
	if !ok {
		t.Fatalf("no seed registered for client nkey %q.\n\n"+
			"Mint it with freshUserPub/freshClientPub so this helper can sign the nonce for it. "+
			"Without a signature the handler denies at the identity check and the test would be "+
			"asserting the wrong refusal.", clientNkey)
	}
	kp, _ := v.(nkeys.KeyPair)
	nonce := "nonce-" + clientNkey[:8]
	sig, err := kp.Sign([]byte(nonce))
	if err != nil {
		t.Fatal(err)
	}
	rc.ClientInformation.Nonce = nonce
	rc.ConnectOptions.SignedNonce = base64.RawURLEncoding.EncodeToString(sig)
}

// freshUserPub returns the public key of a freshly-created user nkey.
func freshUserPub(t *testing.T) string {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	testUserKeys.Store(pub, kp)
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
	want := "tether.v2.ctrl.by." + client + ".session.create.req"
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

	clientPub := freshUserPub(t)

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

// TestEmitEventOnPinFailure pins the P7 wiring: when the auth_callout
// rejects a wrong-PIN attempt for either ctl or agent role, the
// injected EmitEvent callback fires with kind="pin_failed". Without
// this hook the P7 events stream would silently miss bad-PIN
// observations even though the broker logs them.
func TestEmitEventOnPinFailure(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	var (
		mu      sync.Mutex
		emitted []emittedEvent
	)
	h.EmitEvent = func(kind string, fields map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		emitted = append(emitted, emittedEvent{kind: kind, fields: fields})
	}

	// ctl path: tether-cli:lab + wrong PIN
	if _, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t),
		"tether-cli:lab", "wrong-pin")); err != nil {
		t.Fatal(err)
	}
	// agent path: tether-agent:lab:lab-1 + wrong PIN (no prior provisioning)
	if _, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t),
		"tether-agent:lab:lab-1", "wrong-pin")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	gotCtl, gotAgent := false, false
	for _, e := range emitted {
		if e.kind != "pin_failed" {
			continue
		}
		if e.fields["role"] == "ctl" {
			gotCtl = true
		}
		if e.fields["role"] == "agent" {
			gotAgent = true
		}
	}
	if !gotCtl {
		t.Errorf("ctl wrong-PIN must emit pin_failed{role:ctl}; got %+v", emitted)
	}
	if !gotAgent {
		t.Errorf("agent wrong-PIN must emit pin_failed{role:agent}; got %+v", emitted)
	}
}

// TestEmitEventOnMemberJoined pins the symmetric success path:
// PIN-bootstrap that successfully writes a members row (or
// agent_provisioning row) emits kind="member_joined".
func TestEmitEventOnMemberJoined(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	var (
		mu      sync.Mutex
		emitted []emittedEvent
	)
	h.EmitEvent = func(kind string, fields map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		emitted = append(emitted, emittedEvent{kind: kind, fields: fields})
	}

	// ctl join via PIN (correct).
	if _, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t),
		"tether-cli:lab", "test-pin")); err != nil {
		t.Fatal(err)
	}
	// agent provision via PIN (correct).
	if _, err := h.Handle(signedRequest(t, freshUserPub(t), freshUserPub(t),
		"tether-agent:lab:lab-1", "test-pin")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	gotCtl, gotAgent := false, false
	for _, e := range emitted {
		if e.kind != "member_joined" {
			continue
		}
		if e.fields["role"] == "ctl" {
			gotCtl = true
		}
		if e.fields["role"] == "agent" {
			gotAgent = true
		}
	}
	if !gotCtl {
		t.Errorf("ctl PIN-join success must emit member_joined{role:ctl}; got %+v", emitted)
	}
	if !gotAgent {
		t.Errorf("agent PIN-bootstrap success must emit member_joined{role:agent}; got %+v", emitted)
	}
}

type emittedEvent struct {
	kind   string
	fields map[string]any
}

// origin: prerelease audit increment 2 internal review, admission-enforcement/L9-F1 ≡
// red-team/RED-TEAM-REFUTE-F1.
//
// THE ACTOR IN EVERY PERMISSION TEMPLATE MUST BE A PROVEN IDENTITY.
//
// `ConnectOptions.Nkey` is a string the connecting party types. nats-server does NOT
// verify it for a key it has no static `users:` entry for — it forwards the nkey, the
// nonce and the signature and leaves the check to the callout — and this package's own
// package doc asserted the opposite for three releases. Two things rest directly on the
// binding:
//
//   - auth.InboxPrefixFor(actor) is only isolation if the actor is yours. Otherwise
//     knowing a victim's PUBLIC key (sys.events publishes it in session_created{actor})
//     is enough to subscribe to that victim's private reply inbox, which carries agent
//     register replies — a raw tunnel token and every subscriber PSK.
//   - session.MayCreateSession keys on a fingerprint of the actor parsed out of the
//     PUBLISH subject, and NATS only pins that subject to ConnectOptions.Nkey.
//
// So a forged nkey is simultaneously an inbox-disclosure and an admission bypass, and
// the only place either can be stopped is here.
func TestAConnectionCannotPresentSomebodyElsesUserNkey(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	victimPub := freshUserPub(t)
	attackerKp, _ := nkeys.CreateUser()

	// Positive control FIRST, so a handler that denies everything cannot pass this test:
	// the victim itself, signing its own nonce, is allowed.
	if reason := denyReason(t, mustHandle(t, h,
		signedRequest(t, freshUserPub(t), victimPub, "tether-cli", ""))); reason != "" {
		t.Fatalf("the holder of the nkey was refused: %s", reason)
	}

	forged := forgedNkeyRequest(t, victimPub, attackerKp, "tether-cli", "")
	if reason := denyReason(t, mustHandle(t, h, forged)); reason == "" {
		t.Fatal("a connection presenting the VICTIM's public key with an ATTACKER's signature was " +
			"ALLOWED.\n\n" +
			"Every grant this handler mints is scoped by ConnectOptions.Nkey: the `ctrl.by.<actor>` " +
			"publish subjects, the private reply inbox derived by auth.InboxPrefixFor, and the " +
			"fingerprint the session-create allow-list is keyed on. If that field is not proven, " +
			"knowing a victim's public key — which sys.events hands to any session member — is " +
			"enough to read its replies and to create sessions as it.")
	}

	// And an unsigned CONNECT is a refusal, not an exemption: nats-server only fills the
	// nonce when the client sent a signature, so "no signature" is exactly the case this
	// check exists for. It must not fall through to a permissive template — an empty
	// jwt.Permissions is UNRESTRICTED in nats-server.
	unsigned := unsignedNkeyRequest(t, victimPub, "tether-cli", "")
	if reason := denyReason(t, mustHandle(t, h, unsigned)); reason == "" {
		t.Fatal("a CONNECT carrying an nkey but NO signature over the server nonce was ALLOWED")
	}
}

// forgedNkeyRequest builds the exact shape a real attacker sends: the victim's PUBLIC
// key in ConnectOptions.Nkey, and a signature over the nonce made with a key the
// attacker owns. nats-server accepts this CONNECT and forwards it verbatim.
func forgedNkeyRequest(t *testing.T, victimPub string, attackerKp nkeys.KeyPair, name, token string) string {
	t.Helper()
	serverKp, _ := nkeys.CreateServer()
	serverPub, _ := serverKp.PublicKey()

	rc := jwt.NewAuthorizationRequestClaims(serverPub)
	rc.UserNkey = freshUserPub(t)
	rc.Server = jwt.ServerID{ID: serverPub, Name: serverPub}
	rc.ConnectOptions = jwt.ConnectOptions{Name: name, Nkey: victimPub, Token: token}
	nonce := "nonce-forged"
	sig, err := attackerKp.Sign([]byte(nonce))
	if err != nil {
		t.Fatal(err)
	}
	rc.ClientInformation.Nonce = nonce
	rc.ConnectOptions.SignedNonce = base64.RawURLEncoding.EncodeToString(sig)
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// unsignedNkeyRequest carries an nkey and no signature at all.
func unsignedNkeyRequest(t *testing.T, clientNkey, name, token string) string {
	t.Helper()
	serverKp, _ := nkeys.CreateServer()
	serverPub, _ := serverKp.PublicKey()

	rc := jwt.NewAuthorizationRequestClaims(serverPub)
	rc.UserNkey = freshUserPub(t)
	rc.Server = jwt.ServerID{ID: serverPub, Name: serverPub}
	rc.ConnectOptions = jwt.ConnectOptions{Name: name, Nkey: clientNkey, Token: token}
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// A different agent identity arriving with a valid PIN to a slot already
// taken by another fp must be denied. PIN does NOT override the existing
// (sid,nid)→fp binding — operator must explicitly revoke first.
func TestHandleAgentRoleRejectsHijack(t *testing.T) {
	h, _ := freshHandler(t)
	seedSessionWithPin(t, h, "lab", "test-pin")

	firstPub := freshUserPub(t)
	if _, err := h.Handle(signedRequest(t, freshUserPub(t), firstPub,
		"tether-agent:lab:lab-1", "test-pin")); err != nil {
		t.Fatal(err)
	}

	hijackerPub := freshUserPub(t)
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

// FuzzParseRole drives the connection-name classifier with arbitrary names.
//
// The name is CLIENT-CONTROLLED (ensureAgentProvisioned's own comment says so): whatever comes back
// from parseRole is what the auth flow trusts before re-validating. The invariants are the ones the
// callers assume without checking — an agent role always carries a non-empty sid AND nid, an
// activated CLI a non-empty sid, unknown carries nothing — plus a rebuild: the classified role and
// fields reproduce the exact input, so no name maps to a role it does not literally spell.
// origin: docs/reviews/test-system-overhaul-plan.md B2 (infra I7).
func FuzzParseRole(f *testing.F) {
	for _, s := range []string{
		"tether-cli", "tether-cli:lab", "tether-cli:",
		"tether-agent:lab:gpu1", "tether-agent:lab:", "tether-agent::gpu1", "tether-agent:lab",
		"tether-agent:lab:gpu1:extra", "tether-agent:", "", "x", "tether-cli:lab:extra",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		r, sid, nid := parseRole(name)
		switch r {
		case roleUnknown:
			if sid != "" || nid != "" {
				t.Fatalf("roleUnknown for %q carried sid=%q nid=%q", name, sid, nid)
			}
		case roleCtlUnactivated:
			if name != "tether-cli" || sid != "" || nid != "" {
				t.Fatalf("roleCtlUnactivated for %q (sid=%q nid=%q)", name, sid, nid)
			}
		case roleCtlActivated:
			if sid == "" || nid != "" || "tether-cli:"+sid != name {
				t.Fatalf("roleCtlActivated for %q does not rebuild: sid=%q nid=%q", name, sid, nid)
			}
		case roleAgent:
			if sid == "" || nid == "" || "tether-agent:"+sid+":"+nid != name {
				t.Fatalf("roleAgent for %q does not rebuild: sid=%q nid=%q", name, sid, nid)
			}
		default:
			t.Fatalf("parseRole(%q) returned an unknown role %d", name, r)
		}
	})
}
