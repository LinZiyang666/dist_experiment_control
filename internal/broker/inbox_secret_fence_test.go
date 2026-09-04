package broker

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
)

// inbox_secret_fence_test.go — a long-lived credential is only ever published into a
// reply space its requester alone can read.
//
// origin: prerelease audit external review B-1. The N-1 compatibility grant
// (auth.LegacyInboxAllow) hands `Sub _INBOX.>` to any connection that declines to send
// auth.InboxCapableMarker — and an attacker declines simply by not sending it, with a
// freshly minted nkey and no membership of anything. No ACL can narrow that: a
// pre-cutover client picks its own random reply subject AFTER connecting, so a
// per-connection permission issued at CONNECT time cannot name it. The space therefore
// cannot be made private, and the only remaining lever is to keep the credentials out.
//
// What flows there is not metadata: ProxyDirective carries the raw tunnel Token and every
// subscriber's Shadowsocks PSK, and ProxySubCreateResp carries a /sub bearer token. Those
// do not rotate, so a disclosure does NOT end when the eavesdropped client upgrades —
// which is why this could not be left to close itself over the N-1 window.

// legacyInboxConn dials with nats.go's DEFAULT inbox prefix, i.e. exactly what a
// pre-cutover ctl or agent does.
func legacyInboxConn(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("legacy dial: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestRegisterWithholdsProxyCredentialsFromASharedInbox(t *testing.T) {
	modern, _, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BumpProxyEpoch(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	}
	registerReq := proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "v0.5.0",
		Capabilities:   []string{proto.CapProxyV1},
	}

	// --- POSITIVE CONTROL: a private inbox still gets the directive ---
	// Without this half the test would pass just as well against a broker that had
	// stopped minting proxy directives altogether.
	registerReq.NID = "modern-1"
	var ok proto.NodeRegisterResp
	req(t, modern, proto.SubjNodeRegister(sid, "modern-1"), registerReq, &ok)
	if !ok.OK {
		t.Fatalf("register on a private inbox was refused: %+v", ok)
	}
	if ok.Proxy == nil || ok.Proxy.Token == "" {
		t.Fatalf("a register on the per-identity inbox must still receive its credentials, got %+v", ok.Proxy)
	}
	if ok.Code != "" {
		t.Fatalf("a private-inbox register must carry no degradation code, got %q", ok.Code)
	}

	// --- THE FENCE: the same register on the shared space gets no credentials ---
	legacy := legacyInboxConn(t, b.cfg.NATSURL)
	registerReq.NID = "legacy-1"
	var degraded proto.NodeRegisterResp
	req(t, legacy, proto.SubjNodeRegister(sid, "legacy-1"), registerReq, &degraded)

	if degraded.Proxy != nil {
		t.Fatalf("the tunnel token and every subscriber PSK were published into `_INBOX`, where any "+
			"connection holding a fresh nkey can read them: %+v", degraded.Proxy)
	}
	if degraded.Code != proto.CodeLegacyInboxNoSecrets {
		t.Fatalf("a withheld directive must say so (code=%q, want %q); a silent nil is "+
			"indistinguishable from `proxy off` and leaves the operator with nothing to act on",
			degraded.Code, proto.CodeLegacyInboxNoSecrets)
	}
	// THE REGISTER STILL SUCCEEDS, and that is the load-bearing half of the design.
	// Refusing would be simpler code and a worse outcome: the node would never come
	// ONLINE, so `tether node upgrade` — the one command that fixes this — could not
	// reach it, and a fleet of pre-cutover agents would have no in-band upgrade path.
	if !degraded.OK {
		t.Fatalf("a shared-inbox register must SUCCEED without its credentials, got %+v", degraded)
	}
}

func TestSubCreateWithholdsTheBearerURLFromASharedInbox(t *testing.T) {
	modern, owner, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	subj := proto.SubjCtrlProxySubCreate(owner, sid)

	// --- POSITIVE CONTROL ---
	var ok proto.ProxySubCreateResp
	req(t, modern, subj, proto.ProxySubCreateReq{Name: "alice"}, &ok)
	if !ok.OK || ok.SubURL == "" {
		t.Fatalf("a private-inbox sub create must return its URL, got %+v", ok)
	}

	// --- THE FENCE. Same actor subject (this IS the owner, on an un-upgraded machine);
	// only the reply space differs. ---
	legacy := legacyInboxConn(t, b.cfg.NATSURL)
	var degraded proto.ProxySubCreateResp
	req(t, legacy, subj, proto.ProxySubCreateReq{Name: "bob"}, &degraded)
	if degraded.SubURL != "" {
		t.Fatalf("a /sub bearer token was published into `_INBOX`: %q", degraded.SubURL)
	}
	if degraded.Code != proto.CodeLegacyInboxNoSecrets {
		t.Fatalf("sub create code=%q, want %q", degraded.Code, proto.CodeLegacyInboxNoSecrets)
	}
}

// TestIsPrivateInboxSubjectIsAWhitelist pins the direction the classifier fails in.
//
// Written as `!strings.HasPrefix(subject, "_INBOX.")` it would answer "private" for every
// subject that is neither root — a future inbox scheme, or a service subject someone
// reuses as a reply-to — and the cost of guessing wrong in THAT direction is publishing a
// tunnel token into a space with an unknown reader set.
func TestIsPrivateInboxSubjectIsAWhitelist(t *testing.T) {
	private := auth.InboxPrefixFor("UTESTACTOR") + ".abc.1"
	for _, tc := range []struct {
		subject string
		want    bool
	}{
		{private, true},
		{auth.InboxRoot + ".x.y", true},
		{"_INBOX.abcdef", false},
		{"_INBOX.abcdef.1", false},
		{"tether.v2.s.lab.something", false}, // a service subject used as reply-to
		{"", false},
		{auth.InboxRoot, false},               // the bare root is not a subtree member
		{"_TINBOXY.a.b", false},               // a neighbouring root must not match on prefix alone
		{"x." + auth.InboxRoot + ".a", false}, // the root must be at the FRONT
	} {
		if got := auth.IsPrivateInboxSubject(tc.subject); got != tc.want {
			t.Errorf("IsPrivateInboxSubject(%q) = %v, want %v", tc.subject, got, tc.want)
		}
	}
}

// TestOnlyOneFunctionEverSetsTheSubURL is the structural half of R2-B1, and it exists
// because the behavioural half was already written once and still missed a live handler.
//
// origin: prerelease audit external review R2-B1. The first fence went into
// `proxySubCreate`; cluster mode dispatches to `handleProxySubCreateCluster`, which kept
// replying with the bearer URL unconditionally. The test written with that fix constructed
// only a single-mode broker, so nothing executed the clustered branch — a behavioural test
// can only cover the paths somebody remembered to build.
//
// So this asserts the property that makes forgetting IMPOSSIBLE rather than merely unlikely:
// `ProxySubCreateResp.SubURL` is assigned in exactly ONE function, and that function applies
// the fence. A third create path can still be added — but it cannot hand out the token
// without either calling the helper or turning this red.
func TestOnlyOneFunctionEverSetsTheSubURL(t *testing.T) {
	const helper = "proxySubCreateReply"
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	seenHelper := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Name.Name == helper {
				seenHelper = true
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				kv, ok := n.(*ast.KeyValueExpr)
				if !ok {
					return true
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok || key.Name != "SubURL" {
					return true
				}
				if fn.Name.Name != helper {
					offenders = append(offenders,
						fmt.Sprintf("%s in %s (%s)", key.Name, fn.Name.Name, fset.Position(kv.Pos())))
				}
				return true
			})
		}
	}
	// NON-VACUITY: if the helper were renamed or deleted, every site would look compliant
	// by virtue of there being nothing to compare against.
	if !seenHelper {
		t.Fatalf("%s does not exist in this package; the scan below would then pass for any code at all", helper)
	}
	if len(offenders) != 0 {
		t.Fatalf("ProxySubCreateResp.SubURL is set outside %s at %v.\n\n"+
			"The bearer token may only leave the broker through the one function that checks whether "+
			"the reply space is private. A second create handler that sets it directly is exactly how "+
			"R2-B1 happened: the fence covered single mode while cluster mode handed the token to the "+
			"shared _INBOX.", helper, offenders)
	}
}
