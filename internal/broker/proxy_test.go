package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// proxyTestBroker boots a no-auth broker over an in-process NATS server with a
// session owned by a fresh actor, and returns a connected ctl plus that actor.
func proxyTestBroker(t *testing.T) (*nats.Conn, string /*ownerPub*/, string /*sid*/, *Broker) {
	t.Helper()
	url := startNATS(t)
	db := openDB(t)
	ownerPub, ownerFP := testharness.FreshUserPub(t)
	if _, err := session.Create(db, "lab", "lab", ownerFP, "pinhash", time.Now()); err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{NATSURL: url, DB: db, Logger: silentLogger(), PublicHost: "broker.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = b.Run(ctx) }()
	waitNATSReady(t, url)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc, ownerPub, "lab", b
}

func req(t *testing.T, nc *nats.Conn, subj string, body any, out any) {
	t.Helper()
	b, _ := json.Marshal(body)
	reply, err := nc.Request(subj, b, 2*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subj, err)
	}
	if err := json.Unmarshal(reply.Data, out); err != nil {
		t.Fatalf("decode %s: %v", subj, err)
	}
}

func TestProxyOwnerOnlyAndLifecycle(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)

	// --- non-owner is rejected on every mutating verb (admin_denied) ---
	stranger, _ := testharness.FreshUserPub(t)
	var setR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(stranger, sid), proto.ProxySetReq{Enabled: true}, &setR)
	if setR.Code != "not_owner" {
		t.Fatalf("non-owner proxy.set: code=%q want not_owner", setR.Code)
	}
	var subR proto.ProxySubCreateResp
	req(t, nc, proto.SubjCtrlProxySubCreate(stranger, sid), proto.ProxySubCreateReq{Name: "x"}, &subR)
	if subR.Code != "not_owner" {
		t.Fatalf("non-owner sub.create: code=%q want not_owner", subR.Code)
	}

	// --- owner enables ---
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &setR)
	if !setR.OK || !setR.Enabled {
		t.Fatalf("owner enable failed: %+v", setR)
	}
	if on, _ := session.GetProxyEnabled(b.cfg.DB, sid); !on {
		t.Fatal("proxy_enabled not persisted")
	}

	// --- create two subscribers; each URL is distinct and well-formed ---
	var aURL, bURL string
	for _, name := range []string{"alice", "bob"} {
		var r proto.ProxySubCreateResp
		req(t, nc, proto.SubjCtrlProxySubCreate(owner, sid), proto.ProxySubCreateReq{Name: name}, &r)
		if !r.OK || !strings.Contains(r.SubURL, "/sub/") {
			t.Fatalf("create %s: %+v", name, r)
		}
		if name == "alice" {
			aURL = r.SubURL
		} else {
			bURL = r.SubURL
		}
	}
	if aURL == bURL {
		t.Fatal("two subscribers must get distinct URLs")
	}

	// duplicate active name → sub_name_taken
	var dup proto.ProxySubCreateResp
	req(t, nc, proto.SubjCtrlProxySubCreate(owner, sid), proto.ProxySubCreateReq{Name: "alice"}, &dup)
	if dup.Code != "sub_name_taken" {
		t.Fatalf("dup name: code=%q want sub_name_taken", dup.Code)
	}
	// invalid name → sub_name_invalid
	var bad proto.ProxySubCreateResp
	req(t, nc, proto.SubjCtrlProxySubCreate(owner, sid), proto.ProxySubCreateReq{Name: "has/slash"}, &bad)
	if bad.Code != "sub_name_invalid" {
		t.Fatalf("bad name: code=%q want sub_name_invalid", bad.Code)
	}

	// --- status is MEMBER-readable (owner is a member) and shows 2 subs ---
	var stat proto.ProxyStatusResp
	req(t, nc, proto.SubjCtrlProxyStatus(owner, sid), proto.ProxyStatusReq{}, &stat)
	if !stat.Enabled || len(stat.Subscribers) != 2 {
		t.Fatalf("status: enabled=%v subs=%d", stat.Enabled, len(stat.Subscribers))
	}
	for _, s := range stat.Subscribers {
		// Status must never leak secrets.
		if strings.Contains(s.Name, "psk") {
			t.Fatal("status leaked a secret-looking field")
		}
	}

	// --- revoke isolation: revoking alice leaves bob ACTIVE ---
	var rev proto.ProxySubRevokeResp
	req(t, nc, proto.SubjCtrlProxySubRevoke(owner, sid), proto.ProxySubRevokeReq{Name: "alice"}, &rev)
	if !rev.OK {
		t.Fatalf("revoke alice: %+v", rev)
	}
	active, _ := proxysub.ListActive(b.cfg.DB, sid)
	if len(active) != 1 || active[0].Name != "bob" {
		t.Fatalf("after revoke alice, active=%v", active)
	}
	// double revoke → already_revoked; unknown → sub_not_found
	req(t, nc, proto.SubjCtrlProxySubRevoke(owner, sid), proto.ProxySubRevokeReq{Name: "alice"}, &rev)
	if rev.Code != "already_revoked" {
		t.Fatalf("double revoke: code=%q", rev.Code)
	}
	req(t, nc, proto.SubjCtrlProxySubRevoke(owner, sid), proto.ProxySubRevokeReq{Name: "ghost"}, &rev)
	if rev.Code != "sub_not_found" {
		t.Fatalf("revoke ghost: code=%q", rev.Code)
	}

	// --- disable clears the flag ---
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: false}, &setR)
	if !setR.OK || setR.Enabled {
		t.Fatalf("disable: %+v", setR)
	}
	if on, _ := session.GetProxyEnabled(b.cfg.DB, sid); on {
		t.Fatal("proxy_enabled still set after off")
	}
}

// A subject that matches the registered wildcard but carries an invalid sid
// must be rejected as subject_malformed BEFORE any owner/DB work, and must NOT
// flip the (valid) session's proxy flag.
func TestProxySubjectMalformedRejected(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)
	// `BAD!SID` is a single NATS token (matches `s.*`) but fails ValidateSID,
	// so handleProxySet's ParseCtrlProxy returns subject_malformed up front.
	bad := "tether.v1.ctrl.by." + owner + ".s.BAD!SID.proxy.set.req"
	reply, err := nc.Request(bad, []byte(`{"enabled":true}`), 2*time.Second)
	if err != nil {
		t.Fatalf("expected a subject_malformed reply, got transport error: %v", err)
	}
	var r struct{ Code string }
	if err := json.Unmarshal(reply.Data, &r); err != nil {
		t.Fatal(err)
	}
	if r.Code != "subject_malformed" {
		t.Fatalf("code=%q want subject_malformed", r.Code)
	}
	// No DB mutation: the real session's flag is untouched.
	if on, _ := session.GetProxyEnabled(b.cfg.DB, sid); on {
		t.Fatal("malformed request mutated session state")
	}
}

// Acting on a DELETING session is refused by the C.1 §6 precheck, before owner.
func TestProxyRejectsDeletingSession(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)
	if err := session.Tombstone(b.cfg.DB, sid, time.Now()); err != nil {
		t.Fatal(err)
	}
	var setR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &setR)
	if setR.Code != "session_not_found_or_deleting" {
		t.Fatalf("DELETING session: code=%q", setR.Code)
	}
}
