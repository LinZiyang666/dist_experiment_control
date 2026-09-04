package d3_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// connectBroker connects using the broker nkey (an AuthUser that bypasses callout
// and carries the static PermissionsForBroker, incl. the RF1 cluster.* grants).
func connectBroker(t *testing.T, url string, b brokerID) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Name("tetherd-acl"), nats.Nkey(b.pub, sigFromSeed(b.seed)), nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("broker connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// errCapture connects a fresh unactivated CLI (callout-issued PermissionsForUnactivated
// JWT) with an async error handler so permission violations are observable.
func connectCLIWithErrCapture(t *testing.T, url string) (*nats.Conn, func() error) {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()

	var mu sync.Mutex
	var lastErr error
	nc, err := nats.Connect(url,
		nats.Name("tether-cli"),
		nats.Nkey(pub, sigFromSeed(seed)),
		nats.Timeout(4*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
			mu.Lock()
			lastErr = e
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("CLI connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	cliActors.Store(nc, pub)
	getErr := func() error {
		mu.Lock()
		defer mu.Unlock()
		return lastErr
	}
	return nc, getErr
}

// cliActors remembers which nkey each test CLI connected with, so a caller can name a
// subject that connection is genuinely granted without threading the key through five
// call sites. origin: prerelease audit round 2 — the positive control below used to
// publish to `_INBOX.<anything>`, which was only "allowed" because the anonymous
// template carried a surplus Pub grant on the whole reply space.
var cliActors sync.Map

func cliActorOf(t *testing.T, nc *nats.Conn) string {
	t.Helper()
	v, ok := cliActors.Load(nc)
	if !ok {
		t.Fatal("this connection was not built by connectCLIWithErrCapture")
	}
	return v.(string)
}

// TestD3RF1ApplyACL: the broker nkey may pub/sub cluster.apply.* (positive,
// delivered), but a callout-issued user JWT may NOT pub OR sub cluster.* (RF1).
func TestD3RF1ApplyACL(t *testing.T) {
	brokers := []brokerID{newBrokerID(t)}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, 1, nil))
	url := servers[0].ClientURL()

	// The broker connection is also the auth_callout responder (so the CLI below
	// can connect).
	h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
	runResponder(t, url, brokers[0], h)

	// Positive: broker pub -> broker sub on tether.v2.cluster.apply.demo.
	bconn := connectBroker(t, url, brokers[0])
	sub, err := bconn.SubscribeSync("tether.v2.cluster.apply.demo")
	if err != nil {
		t.Fatalf("broker subscribe cluster.apply.demo: %v", err)
	}
	_ = bconn.Flush()
	if err := bconn.Publish("tether.v2.cluster.apply.demo", []byte("hi")); err != nil {
		t.Fatalf("broker publish cluster.apply.demo: %v", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("broker must be able to pub+sub cluster.apply.* (RF1 positive): %v", err)
	}

	// Negative: a callout-issued CLI JWT is denied pub AND sub on cluster.*.
	cli, getErr := connectCLIWithErrCapture(t, url)

	// Positive control (§13.8): the SAME connection can pub an ALLOWED subject —
	// proves the deny below is subject-scoped, not a dead/unauthorized connection.
	//
	// A SUBJECT THE CTL GENUINELY NEEDS, not `_INBOX.<anything>` — origin: prerelease
	// audit round 2. This used the reply space as a convenient "something allowed",
	// which quietly made it the only thing asserting that anonymous clients may publish
	// into every connection's inbox. They may not, and they never needed to: a ctl names
	// its inbox in the reply FIELD and publishes to the service subject.
	if err := cli.Publish("tether.v2.ctrl.by."+cliActorOf(t, cli)+".session.list.req", []byte("ok")); err != nil {
		t.Fatalf("allowed-subject publish failed: %v", err)
	}
	_ = cli.Flush()
	time.Sleep(150 * time.Millisecond)
	if isPermViolation(getErr()) {
		t.Fatalf("a pub on an allowed subject must NOT trigger a permission violation: %v", getErr())
	}

	if err := cli.Publish("tether.v2.cluster.apply.demo", []byte("x")); err != nil {
		// Some nats client versions surface the violation synchronously; that's a deny too.
		if !isPermViolation(err) {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}
	_ = cli.Flush()
	if !waitErr(getErr, isPermViolation, 2*time.Second) {
		t.Fatal("a callout-issued CLI JWT must be DENIED pub on cluster.apply.* (RF1 negative)")
	}

	// Reset and test subscribe.
	cli2, getErr2 := connectCLIWithErrCapture(t, url)
	if _, err := cli2.SubscribeSync("tether.v2.cluster.>"); err != nil {
		if !isPermViolation(err) {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
	}
	_ = cli2.Flush()
	if !waitErr(getErr2, isPermViolation, 2*time.Second) {
		t.Fatal("a callout-issued CLI JWT must be DENIED sub on cluster.> (RF1 negative)")
	}
}

// TestD3NoSecretLeakOnBusSubjects: the PIN-bearing auth request flows on
// $SYS.REQ.USER.AUTH, which the broker observes (positive) but a callout-issued
// CLI cannot subscribe to (negative) — so the PIN never reaches a non-broker.
func TestD3NoSecretLeakOnBusSubjects(t *testing.T) {
	const pin = "secret-pin-9f3a2b"
	brokers := []brokerID{newBrokerID(t)}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, 1, nil))
	url := servers[0].ClientURL()

	// Responder that records the decoded token (PIN) it observed on the request.
	var mu sync.Mutex
	var sawToken string
	h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
	nc, err := nats.Connect(url, nats.Name("tetherd"), nats.Nkey(brokers[0].pub, sigFromSeed(brokers[0].seed)), nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	if _, err := nc.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", func(m *nats.Msg) {
		if req, derr := jwt.DecodeAuthorizationRequestClaims(string(m.Data)); derr == nil {
			mu.Lock()
			if req.ConnectOptions.Token == pin {
				sawToken = req.ConnectOptions.Token
			}
			mu.Unlock()
		}
		resp, herr := h.Handle(string(m.Data))
		if herr == nil && m.Reply != "" {
			_ = m.Respond([]byte(resp))
		}
	}); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()

	// Positive: a CLI connects carrying the PIN; the broker observes it in flight.
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()
	cli, err := nats.Connect(url, nats.Name("tether-cli"), nats.Nkey(pub, sigFromSeed(seed)), nats.Token(pin), nats.Timeout(4*time.Second))
	if err != nil {
		t.Fatalf("CLI connect with PIN: %v", err)
	}
	defer cli.Close()
	ok := false
	for i := 0; i < 100 && !ok; i++ {
		mu.Lock()
		ok = sawToken == pin
		mu.Unlock()
		if !ok {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !ok {
		t.Fatal("positive control: the broker must observe the PIN on the auth request (proves the secret really flowed)")
	}

	// Negative: a callout-issued CLI cannot subscribe to the PIN-bearing subject.
	snoop, getErr := connectCLIWithErrCapture(t, url)
	if _, err := snoop.SubscribeSync("$SYS.REQ.USER.AUTH"); err != nil {
		if !isPermViolation(err) {
			t.Fatalf("unexpected: %v", err)
		}
	}
	_ = snoop.Flush()
	if !waitErr(getErr, isPermViolation, 2*time.Second) {
		t.Fatal("a non-broker must be DENIED subscribe to $SYS.REQ.USER.AUTH — the PIN must not leak to non-brokers")
	}
}

func isPermViolation(err error) bool {
	if err == nil {
		return false
	}
	// Match the specific nats-server signal, not a bare "permission" substring (which
	// could match an unrelated future "permission denied").
	return strings.Contains(strings.ToLower(err.Error()), "permissions violation")
}

func waitErr(get func() error, pred func(error) bool, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred(get()) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred(get())
}
