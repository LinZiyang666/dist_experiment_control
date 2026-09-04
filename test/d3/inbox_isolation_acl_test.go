package d3_test

import (
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// inbox_isolation_acl_test.go — the ANONYMOUS half of the reply-inbox leak, asserted
// against a REAL nats-server with the real auth_callout responder.
//
// origin: prerelease audit proto-auth-acl/L1-F1 ≡ broker-proxy-http/L3-F1 (the
// disclosure) and proto-auth-acl/L1-F2 (the sys.events feed). Both were reachable
// with NO credential at all: a syntactically valid user nkey plus the CONNECT name
// "tether-cli" is the entire admission test for the unactivated template, and the
// control plane is on the public internet by design.
//
// These assert BEHAVIOUR, not template contents. A template assertion would still
// pass if the client stopped honouring its own prefix, and it cannot see what
// nats-server actually enforces.

// TestUnactivatedConnectionCannotReadOtherInboxesOrSysEvents is the guard.
//
// EACH SUBJECT GETS ITS OWN CONNECTION. The async error handler records the LAST
// violation and never clears it, so two assertions sharing one connection are not
// independent: the first refusal satisfies the second one's check no matter what the
// second subject is actually granted. The first version of this test did share a
// connection, and mutation verification caught it — restoring the sys.events grant
// left it green.
func TestUnactivatedConnectionCannotReadOtherInboxesOrSysEvents(t *testing.T) {
	brokers := []brokerID{newBrokerID(t)}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, 1, nil))
	url := servers[0].ClientURL()
	h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
	runResponder(t, url, brokers[0], h)

	forbidden := []struct {
		name, subject, why string
	}{
		// `_INBOX.>` IS NOT LISTED, and that is by design rather than an oversight. An
		// unmarked connection receives the pre-cutover grant, which IS `_INBOX.>` — that
		// is what makes an old ctl keep working, and the residual it leaves is stated in
		// requirements §6.7: traffic from binaries that predate the private inbox stays
		// readable there until they are upgraded. That is the meaning of the N-1 window,
		// not a hole in it.
		//
		// What such a connection must NOT reach is the root upgraded principals use, and
		// THAT is refused at subscribe time — origin: prerelease audit increment 2
		// internal review, ops-upgrade/L16-F1.
		//
		// An earlier design put the private inbox INSIDE `_INBOX` and bounded the
		// compatibility grant with a deny, which made this a delivery-side property that
		// could not be asserted here at all. It also did not hold: measured against
		// nats-server 2.10.22 / 2.11.0 / 2.11.9 / 2.12.0 — every version the fleet has
		// run — a holder of that grant subscribed `_INBOX.<victim-hash>.>` and received
		// every reply beneath it. Separate roots make the refusal happen at SUBSCRIBE, on
		// every server version, which is both stronger and observable.
		{
			"the root upgraded principals use", auth.InboxRoot + ".>",
			"an unmarked connection that can name the modern root reads every upgraded " +
				"principal's replies: agent register replies (tunnel token + every subscriber PSK), " +
				"the print-once /sub token, and all `tether exec` output",
		},
		{
			"one upgraded principal's own subtree", auth.InboxPrefixFor("UDUMMYVICTIMACTORFORTHISTEST") + ".>",
			"naming a victim's derived prefix is the SHAPE that escaped the previous design: it is " +
				"three literal tokens, so a token-counting bound admits it, and the `>` then reaches " +
				"everything below",
		},
		{
			"sys.events feed", "tether.v2.sys.events",
			"session_created{sid, owner_fp} / member_joined / pin_failed become a live enumeration " +
				"of every session and owner fingerprint, to a caller holding no credential at all",
		},
	}
	for _, f := range forbidden {
		t.Run(f.name, func(t *testing.T) {
			cli, getErr := connectCLIWithErrCapture(t, url) // FRESH connection per subject
			_, subErr := cli.SubscribeSync(f.subject)
			_ = cli.Flush()
			time.Sleep(250 * time.Millisecond)
			if subErr == nil && !isPermViolation(getErr()) {
				t.Fatalf("an anonymous connection subscribed to %s and the server did not refuse it: %s",
					f.subject, f.why)
			}
		})
	}
}

// TestConnectionCanReadOnlyItsOwnScopedInbox is the POSITIVE control for the guard
// above: the refusals must be scoped, not the symptom of a dead connection. The same
// anonymous identity must still be able to subscribe to — and receive a reply on —
// the inbox subtree auth_callout derived from its own nkey.
func TestConnectionCanReadOnlyItsOwnScopedInbox(t *testing.T) {
	brokers := []brokerID{newBrokerID(t)}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, 1, nil))
	url := servers[0].ClientURL()
	h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
	runResponder(t, url, brokers[0], h)

	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()

	// Exactly what a real client does — through the SAME helper, which is the point.
	//
	// This used to call nats.CustomInboxPrefix directly, and when the legacy space gained
	// its bounding deny that made this test reproduce the exact failure the helper exists
	// to prevent: prefix without marker ⇒ the callout hands out the LEGACY grant, whose
	// deny covers the deep subtree this connection just chose, so every request times out
	// with nothing on the wire. test/architecture's inbox_prefix_pairing gate now forbids
	// the bare call in production; this is the test-side half of the same lesson.
	cli, err := nats.Connect(url, append([]nats.Option{
		nats.Name("tether-cli"),
		nats.Nkey(pub, sigFromSeed(seed)),
		nats.Timeout(4 * time.Second),
	}, natsinbox.InboxConnectOptions(pub)...)...)
	if err != nil {
		t.Fatalf("CLI connect with its scoped inbox prefix: %v", err)
	}
	defer cli.Close()

	// A responder on the broker side answers on msg.Reply — the ordinary path.
	bconn := connectBroker(t, url, brokers[0])
	if _, err := bconn.Subscribe("tether.v2.ctrl.by."+pub+".session.list.req", func(m *nats.Msg) {
		_ = m.Respond([]byte("pong"))
	}); err != nil {
		t.Fatalf("responder subscribe: %v", err)
	}
	_ = bconn.Flush()

	reply, err := cli.Request("tether.v2.ctrl.by."+pub+".session.list.req", nil, 3*time.Second)
	if err != nil {
		t.Fatalf("request/reply over the scoped inbox failed: %v — the prefix the client sets and "+
			"the subtree auth_callout grants have drifted apart, which breaks every ctl command", err)
	}
	if string(reply.Data) != "pong" {
		t.Fatalf("reply = %q, want pong", reply.Data)
	}
}

// origin: prerelease audit round 2, A-F7.
//
// THE PACKAGE-LEVEL nats.NewInbox() IS NOT USABLE BY A SCOPED CONNECTION, and
// cmd/tether/alert_gate.go's comment asserts exactly that as its reason for calling
// the METHOD instead. Round 2's objection was that nothing proved it: the change
// could be reverted and every test stayed green, while the destructive-operation
// gate silently stopped gating (probeClusterHealth returns nil when its
// SubscribeSync is refused, and nil means "no gate", not "gate failed").
//
// Both halves are asserted here — the method's inbox works, the package-level one
// is refused — because a test showing only the first would also pass if the server
// were granting `_INBOX.>` to everyone, which is the disclosure L1-F1 closed.
//
// BOTH HALVES USED TO BE INVERTED, and it passed anyway — origin: prerelease audit
// increment 2 internal review, marker-channel/F1 ≡ pairing-sweep/F1 ≡ acl-oracle/F1,
// three lanes independently. The connection was built with nats.CustomInboxPrefix and NO
// marker, so the callout handed it the COMPATIBILITY grant: its own `cli.NewInbox()` was
// the subscription being refused, and the package-level `nats.NewInbox()` — plain
// `_INBOX.<nuid>` — was the one the grant allowed. The async error recorder keeps the
// last violation and never clears it, so the refusal of the FIRST subscription satisfied
// the assertion about the SECOND. Two inverted claims and a recorder that cannot tell
// them apart came out green.
//
// The connection is now built through natsinbox.InboxConnectOptions, which is both what
// production does and what makes the two claims true: an upgraded connection is granted
// its own subtree under auth.InboxRoot and nothing under `_INBOX`.
func TestThePackageLevelInboxIsRefusedWhileTheConnectionsOwnWorks(t *testing.T) {
	brokers := []brokerID{newBrokerID(t)}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, 1, nil))
	url := servers[0].ClientURL()
	h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
	runResponder(t, url, brokers[0], h)

	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()

	var permErr error
	var permMu sync.Mutex
	cli, err := nats.Connect(url, append([]nats.Option{
		nats.Name("tether-cli"),
		nats.Nkey(pub, sigFromSeed(seed)),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
			permMu.Lock()
			defer permMu.Unlock()
			permErr = e
		}),
		nats.Timeout(4 * time.Second),
		// BOTH halves, through the one helper. Setting the prefix alone is precisely the
		// half-wired shape that made this test assert the inverse of the truth for a
		// whole release — see the doc comment above — and it is the shape
		// test/architecture's pairing gate forbids in production.
	}, natsinbox.InboxConnectOptions(pub)...)...)
	if err != nil {
		t.Fatalf("CLI connect: %v", err)
	}
	defer cli.Close()

	// The METHOD honours CustomInboxPrefix — this is what probeClusterHealth calls.
	own, err := cli.SubscribeSync(cli.NewInbox())
	if err != nil {
		t.Fatalf("subscribing to the connection's OWN inbox failed: %v", err)
	}
	defer func() { _ = own.Unsubscribe() }()

	// The PACKAGE-LEVEL helper mints `_INBOX.<nuid>`, which this connection is not
	// granted. nats-server reports that asynchronously, so flush and let it land.
	if _, serr := cli.SubscribeSync(nats.NewInbox()); serr != nil {
		return // refused synchronously; that is the expected outcome too
	}
	_ = cli.Flush()
	time.Sleep(250 * time.Millisecond)
	permMu.Lock()
	got := permErr
	permMu.Unlock()
	if !isPermViolation(got) {
		t.Fatalf("a scoped connection subscribed to the package-level nats.NewInbox() subject and the "+
			"server did not refuse it (async error: %v).\n\n"+
			"Either the bare `_INBOX.>` grant is back — which is the disclosure L1-F1 closed, every "+
			"other connection's exec output and tunnel tokens readable by any member — or this test "+
			"can no longer tell.", got)
	}
}

// origin: prerelease audit round 2, A-F1 (the redesign).
//
// THE LEGACY GRANT MUST NOT REACH A MODERN INBOX — against a REAL nats-server, because
// this is the one property the whole N-1 design rests on and it is a property of
// nats-server's matching rules, not of our code.
//
// WHY IT DRIVES A REAL SERVER, restated after the design that made it necessary was
// thrown out — origin: prerelease audit increment 2 internal review, ops-upgrade/L16-F1.
//
// The first design kept the private inbox inside `_INBOX` and relied on
// `deny _INBOX.*.*.>` to bound the compatibility grant. Two nats-server behaviours made
// that fragile in ways no amount of reasoning about the permission strings could reveal:
//
//   - a narrow ALLOW does not bound depth. The server matches a SUBSCRIBE subject
//     against the allow list treating the subscription's own `*` and `>` as literal
//     tokens, so a holder of `_INBOX.*.*` may subscribe `_INBOX.<victim>.>` and be
//     ACCEPTED.
//   - the deny that was supposed to catch that bites on the DELIVERY path, and only
//     once the server has installed the connection's mperms filter — which it does
//     lazily, under a predicate that changed upstream between v2.12 and v2.14. Under
//     the older predicate the filter was never installed for that subscription and
//     every reply beneath the victim's prefix was delivered. Measured on 2.10.22,
//     2.11.0, 2.11.9 and 2.12.0 — every version this project has ever shipped.
//
// The roots are disjoint now, so the refusal happens at SUBSCRIBE on every server
// version and none of that bookkeeping is load-bearing any more. This test still drives
// the real server, because "the server refuses it" is the claim, and the previous design
// is a standing demonstration that the claim cannot be read off the permission strings.
func TestLegacyGrantCannotReachAModernInbox(t *testing.T) {
	brokers := []brokerID{newBrokerID(t)}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, 1, nil))
	url := servers[0].ClientURL()
	h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
	runResponder(t, url, brokers[0], h)

	// A MODERN client: the deep per-identity prefix plus the marker, exactly as
	// natsinbox.InboxConnectOptions hands them out.
	victimKp, _ := nkeys.CreateUser()
	victimPub, _ := victimKp.PublicKey()
	victimRaw, _ := victimKp.Seed()
	victimSeed := append([]byte(nil), victimRaw...)
	victimKp.Wipe()
	victimOpts := append([]nats.Option{
		nats.Name("tether-cli"),
		nats.Nkey(victimPub, sigFromSeed(victimSeed)),
		nats.Timeout(4 * time.Second),
	}, natsinbox.InboxConnectOptions(victimPub)...)
	victim, err := nats.Connect(url, victimOpts...)
	if err != nil {
		t.Fatalf("modern client connect: %v", err)
	}
	defer victim.Close()

	// Its own deep inbox must work — otherwise the negative below proves nothing.
	secretInbox := victim.NewInbox()
	own, err := victim.SubscribeSync(secretInbox)
	if err != nil {
		t.Fatalf("a modern client cannot subscribe to its OWN inbox: %v", err)
	}
	if err := victim.Flush(); err != nil {
		t.Fatal(err)
	}

	// A LEGACY client: no marker, so the callout hands it the pre-cutover `_INBOX.>`.
	// This is also exactly what an attacker gets by claiming to be old.
	snoop, getErr := connectCLIWithErrCapture(t, url) // no marker → compatibility grant
	// EVERY SHAPE such a client could try in order to reach the modern root, including
	// the one that escaped the previous design (a victim's literal prefix plus `>`).
	escapes := []string{
		auth.InboxRoot + ".>",
		auth.InboxRoot + ".*.>",
		auth.InboxRoot + ".*.*.>",
		auth.InboxPrefixFor(victimPub) + ".>",
		secretInbox, // the exact subject, named literally
		">",
		"*.>",
	}
	subs := map[string]*nats.Subscription{}
	for _, s := range escapes {
		sub, serr := snoop.SubscribeSync(s)
		if serr != nil {
			continue // refused synchronously — the expected outcome
		}
		subs[s] = sub
	}
	_ = snoop.Flush()

	// The broker replies into the victim's private inbox.
	bconn := connectBroker(t, url, brokers[0])
	if err := bconn.Publish(secretInbox, []byte("tunnel-token-and-every-psk")); err != nil {
		t.Fatalf("broker publish into the modern inbox: %v", err)
	}
	_ = bconn.Flush()

	if _, err := own.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("the modern client did not receive its OWN reply: %v.\n\n"+
			"Its grant and its prefix have drifted apart, which breaks every command.", err)
	}
	for name, sub := range subs {
		if msg, err := sub.NextMsg(400 * time.Millisecond); err == nil {
			t.Fatalf("a connection holding the COMPATIBILITY grant read an upgraded client's reply "+
				"via %q: %q.\n\n"+
				"That is the whole disclosure this release exists to close, and it means the two "+
				"inbox roots are no longer disjoint — so handing out the compatibility grant on the "+
				"client's own say-so is a privilege escalation again.\n\nasync err: %v",
				name, msg.Data, getErr())
		}
	}

	// POSITIVE CONTROL: the legacy space must still WORK for a legacy client, or the
	// assertions above are satisfied by a grant that permits nothing at all.
	legacyInbox := nats.NewInbox() // `_INBOX.<nuid>` — what a pre-cutover binary uses
	mine, err := snoop.SubscribeSync(legacyInbox)
	if err != nil {
		t.Fatalf("a legacy client cannot subscribe to its own `_INBOX.<nuid>`: %v.\n\n"+
			"Every pre-cutover ctl and agent then loses every reply — the N-1 break this "+
			"redesign exists to avoid.", err)
	}
	_ = snoop.Flush()
	if err := bconn.Publish(legacyInbox, []byte("legacy reply")); err != nil {
		t.Fatal(err)
	}
	_ = bconn.Flush()
	if _, err := mine.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("a legacy client did not receive its own reply: %v", err)
	}
}

// origin: prerelease audit increment 2 internal review, ops-upgrade/L16-F1 (the
// separate-root redesign) — this is the guard for the one N-1 quadrant that redesign
// costs, and for the code written to pay it back.
//
// A PRE-CUTOVER BROKER GRANTS NOTHING UNDER auth.InboxRoot. Its auth_callout mints
// `Sub _INBOX.>` and stops there, so an upgraded client that simply set its private
// prefix would have every subscription to its own inbox REFUSED and every request would
// time out. On a real fleet that is what "roll the broker back while six agents are
// already upgraded" looks like.
//
// natsinbox.Connect probes once and falls back. Both directions are asserted here
// because either alone is satisfiable by a broken implementation: a Connect that ALWAYS
// falls back would pass the first case and silently give up the private inbox
// everywhere, and one that NEVER falls back would pass the second and wedge every client
// against an older broker.
func TestAClientFallsBackToTheSharedInboxAgainstAPreCutoverBroker(t *testing.T) {
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()

	// The responder needs to answer wherever the client's reply subject lands, so it
	// holds `>` — it stands in for a broker, whose own grants are not what is under test.
	responderKp, _ := nkeys.CreateUser()
	responderPub, _ := responderKp.PublicKey()
	responderRaw, _ := responderKp.Seed()
	responderSeed := append([]byte(nil), responderRaw...)
	responderKp.Wipe()
	wideOpen := &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: []string{">"}},
		Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
	}

	cases := []struct {
		name         string
		clientGrant  *natsserver.Permissions
		wantFallback bool
	}{
		{
			name: "pre-cutover broker",
			// EXACTLY what a broker older than auth.InboxRoot mints.
			clientGrant: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: []string{">"}},
				Subscribe: &natsserver.SubjectPermission{Allow: []string{"_INBOX.>"}},
			},
			wantFallback: true,
		},
		{
			name:         "upgraded broker",
			clientGrant:  jwtToServerPerms(auth.PermissionsForUnactivated(pub, false)),
			wantFallback: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := baseClusterOpts()
			// Static nkey users rather than a callout: the point is the SHAPE of the grant
			// the client ends up with, and a static user states that shape literally
			// instead of routing it through a handler this test is not about.
			grant := tc.clientGrant
			if grant.Publish == nil || len(grant.Publish.Allow) == 0 {
				// The unactivated template deliberately has no inbox Pub; give it the
				// service subject it needs so the positive case can actually make a request.
				grant.Publish = &natsserver.SubjectPermission{Allow: []string{">"}}
			}
			o.Nkeys = []*natsserver.NkeyUser{
				{Nkey: pub, Permissions: grant},
				{Nkey: responderPub, Permissions: wideOpen},
			}
			servers := startRouted(t, []*natsserver.Options{o})
			url := servers[0].ClientURL()

			nc, fellBack, err := natsinbox.Connect(url, pub, []nats.Option{
				nats.Name("tether-cli"),
				nats.Nkey(pub, sigFromSeed(seed)),
				nats.Timeout(4 * time.Second),
			})
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer nc.Close()

			if fellBack != tc.wantFallback {
				t.Fatalf("fellBack = %v, want %v.\n\n"+
					"Against a pre-cutover broker the client MUST drop back to the shared inbox or "+
					"every one of its requests times out; against an upgraded one it must NOT, or "+
					"the private inbox this release exists to give it is quietly never used.",
					fellBack, tc.wantFallback)
			}

			// AND THE CONNECTION MUST ACTUALLY WORK, in both cases. Without this the
			// assertion above is satisfied by a Connect that returns a dead conn.
			svc := "tether.v2.ctrl.by." + pub + ".session.list.req"
			rconn, err := nats.Connect(url, nats.Nkey(responderPub, sigFromSeed(responderSeed)),
				nats.Timeout(4*time.Second))
			if err != nil {
				t.Fatalf("responder connect: %v", err)
			}
			defer rconn.Close()
			if _, err := rconn.Subscribe(svc, func(m *nats.Msg) { _ = m.Respond([]byte("pong")) }); err != nil {
				t.Fatalf("responder subscribe: %v", err)
			}
			_ = rconn.Flush()

			reply, err := nc.Request(svc, nil, 3*time.Second)
			if err != nil {
				t.Fatalf("request/reply over the %s inbox failed: %v", tc.name, err)
			}
			if string(reply.Data) != "pong" {
				t.Fatalf("reply = %q, want pong", reply.Data)
			}
		})
	}
}
