package p3_test

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// TestNATSDeniesCrossSessionEvSubscribe — architecture P3 spec scenario:
// "session A 的 token 订阅 tether.v2.s.<sidB>.ev.> → NATS permission denied".
//
// alice activates 'lab' (member). She tries to subscribe to 'prod.ev.>'.
// NATS denies; nc.Subscribe returns no error (subscriptions are
// asynchronously denied), but nc's async err callback fires with
// ErrPermissionViolation. We verify by collecting async errors.
func TestNATSDeniesCrossSessionEvSubscribe(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	alice := admittedIdentity(t, db)
	mustCreate(t, url, alice, "lab", "p")

	bob := admittedIdentity(t, db)
	mustCreate(t, url, bob, "prod", "q")

	// alice connects activated for "lab" — JWT permissions sub allow only
	// `tether.v2.s.lab.ev.>`. Trying to sub `tether.v2.s.prod.ev.>` must be
	// denied at NATS.
	errs := make(chan error, 4)
	nc, err := cli.ConnectNATSWithNkey(url, alice,
		nats.Name(cli.CtlNameForSession("lab")),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errs <- err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	if _, err := nc.Subscribe(proto.SubjectPrefix+".s.prod.ev.>", func(*nats.Msg) {}); err != nil {
		// Synchronous denial is a perfectly valid outcome. Any non-nil
		// error here means NATS rejected the subscribe — attack prevented.
		return
	}
	_ = nc.Flush()

	// Wait briefly for async permission violation.
	select {
	case asyncErr := <-errs:
		if asyncErr == nil {
			t.Fatal("expected permission violation, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected NATS permission violation for cross-session sub, none arrived")
	}
}

// TestNATSDeniesForwardedPub — architecture P3 spec scenario:
// "ctl pub s.<S>.cmd.node.<N>.run.req.forwarded → NATS permission denied
// (.forwarded 不在 ctl allow)".
//
// activated alice tries to pub on the forwarded subject; NATS permission
// templates exclude .forwarded for both unactivated and activated CLI.
func TestNATSDeniesForwardedPub(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	alice := admittedIdentity(t, db)
	mustCreate(t, url, alice, "lab", "p")

	errs := make(chan error, 4)
	nc, err := cli.ConnectNATSWithNkey(url, alice,
		nats.Name(cli.CtlNameForSession("lab")),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errs <- err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	if err := nc.Publish(proto.SubjCmdForwarded("lab", "lab-1", "run"), []byte("attack")); err != nil {
		// sync surface — also acceptable
		return
	}
	_ = nc.Flush()

	select {
	case asyncErr := <-errs:
		if asyncErr == nil {
			t.Fatal("expected permission violation, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected NATS permission violation for .forwarded pub, none arrived")
	}
}

// TestNATSDeniesForgedActorPub — architecture P3 spec scenario:
// "ctl 伪造他人 by.<actor> 段 pub → NATS permission denied (actor 段被
// tetherd 写死为本连接 nkey)".
//
// alice activated for lab tries to pub `cmd.by.<bob>.node.lab-1.run.req`.
// Her JWT only allows `cmd.by.<alice>.node.*.*.req`. NATS denies.
func TestNATSDeniesForgedActorPub(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	alice := admittedIdentity(t, db)
	mustCreate(t, url, alice, "lab", "p")

	bob := freshIdentity(t)

	errs := make(chan error, 4)
	nc, err := cli.ConnectNATSWithNkey(url, alice,
		nats.Name(cli.CtlNameForSession("lab")),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errs <- err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Forge bob's actor in the subject.
	forged := proto.SubjCmdBy("lab", bob.PublicKey, "lab-1", "run")
	if err := nc.Publish(forged, []byte("attack")); err != nil {
		return
	}
	_ = nc.Flush()

	select {
	case asyncErr := <-errs:
		if asyncErr == nil {
			t.Fatal("expected permission violation, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected NATS permission violation for forged by.<actor> pub, none arrived")
	}
}
