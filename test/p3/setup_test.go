// auth_callout test harness shared by every file in test/p3.
//
// startAuthNATS spins up an embedded nats-server configured for auth_callout:
//
//   - The broker's user nkey is in Options.Nkeys with full broker
//     permissions, AND in AuthCallout.AuthUsers so the broker bypasses
//     auth_callout itself.
//   - All other clients fall through to auth_callout, which is handled by
//     the broker once it has connected and subscribed to $SYS.REQ.USER.AUTH.
//
// startBrokerWithAuth wires those keys to broker.New(Config{AuthCallout: ...}).
package p3_test

import (
	"context"
	"database/sql"
	"github.com/LinZiyang666/tether/internal/session"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// startAuthNATS launches an embedded NATS server with auth_callout enabled.
// Returns the client URL plus the broker user nkey seed and account nkey
// seed (both needed by startBrokerWithAuth).
func startAuthNATS(t *testing.T) (url string, brokerSeed, accountSeed []byte) {
	t.Helper()

	brokerKp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	brokerPub, _ := brokerKp.PublicKey()
	rawBrokerSeed, _ := brokerKp.Seed()
	brokerSeed = append([]byte(nil), rawBrokerSeed...)
	brokerKp.Wipe()

	accountKp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, _ := accountKp.PublicKey()
	rawAcct, _ := accountKp.Seed()
	accountSeed = append([]byte(nil), rawAcct...)
	accountKp.Wipe()

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	if os.Getenv("TETHER_TEST_VERBOSE") != "" {
		opts.Debug = true
		opts.Trace = true
	}

	opts.Nkeys = []*natsserver.NkeyUser{
		{
			Nkey:        brokerPub,
			Permissions: jwtToServerPerms(auth.PermissionsForBroker()),
		},
	}
	opts.AuthCallout = &natsserver.AuthCallout{
		Issuer:    accountPub,
		Account:   "$G",
		AuthUsers: []string{brokerPub},
	}

	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL(), brokerSeed, accountSeed
}

// startBrokerWithAuth boots an in-process broker wired to the embedded
// nats-server's auth_callout.
func startBrokerWithAuth(t *testing.T, url string, db *sql.DB, brokerSeed, accountSeed []byte) func() {
	t.Helper()

	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
		AuthCallout: &broker.AuthCalloutConfig{
			BrokerNkeySeed: brokerSeed,
			AccountSeed:    accountSeed,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	waitAuthCalloutReady(t, url)

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit on cancel")
		}
	}
}

// waitAuthCalloutReady probes by attempting an unactivated CLI CONNECT with
// a throwaway nkey. CONNECT only succeeds once the broker has subscribed to
// $SYS.REQ.USER.AUTH and started replying.
func waitAuthCalloutReady(t *testing.T, url string) {
	t.Helper()

	probeKp, _ := nkeys.CreateUser()
	probePub, _ := probeKp.PublicKey()
	rawSeed, _ := probeKp.Seed()
	seed := append([]byte(nil), rawSeed...)
	probeKp.Wipe()

	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		nc, err := nats.Connect(url,
			nats.Nkey(probePub, sigCB),
			nats.Name(cli.CtlNameUnactivated),
			nats.Timeout(200*time.Millisecond),
		)
		if err == nil {
			nc.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker auth_callout not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// jwtToServerPerms converts our jwt.Permissions templates into the
// server-side Permissions struct used by static Nkeys configuration.
func jwtToServerPerms(p jwt.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish: &natsserver.SubjectPermission{
			Allow: p.Pub.Allow,
			Deny:  p.Pub.Deny,
		},
		Subscribe: &natsserver.SubjectPermission{
			Allow: p.Sub.Allow,
			Deny:  p.Sub.Deny,
		},
	}
}

// B9: delegates to internal/testharness — the body was a character-for-character copy, escape hatch
// included, which is the shape where the two silently drift apart.
func silentLog() *slog.Logger {
	return testharness.SilentLog()
}

// openDB returns a fresh in-memory SQLite for the broker.
// openDB delegates to internal/testharness (B9) — see the note in internal/session/session_test.go.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	return testharness.OpenDB(t)
}

// freshIdentity generates an isolated CLI identity inside a per-test home.
func freshIdentity(t *testing.T) *cli.Identity {
	t.Helper()
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// admittedIdentity is freshIdentity plus admission to create sessions.
//
// origin: prerelease audit round 2. `session create` used to admit anybody who could
// reach the control plane — the public internet by design — so a stranger became an
// owner, and from there an activated member and (with the PIN they had just chosen) a
// provisioned agent. It now requires the caller's fingerprint in session_creators, and a
// test about session mechanics has to name the identity the operator admitted.
//
// Per-test on purpose: admitting everything inside the harness would switch the gate off
// for the whole suite.
func admittedIdentity(t *testing.T, db *sql.DB) *cli.Identity {
	t.Helper()
	id := freshIdentity(t)
	fp, err := auth.FingerprintFromActor(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.AllowCreator(db, fp, "test", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	return id
}

// ctlConnUnactivated opens a CLI conn with name=tether-cli (unactivated).
func ctlConnUnactivated(t *testing.T, url string, id *cli.Identity) *nats.Conn {
	t.Helper()
	nc, err := cli.ConnectNATSWithNkey(url, id, nats.Name(cli.CtlNameUnactivated))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// ctlConnActivated opens a CLI conn with name=tether-cli:<sid> and an
// optional Token (PIN, only needed for first-time join).
func ctlConnActivated(t *testing.T, url string, id *cli.Identity, sid, pin string) (*nats.Conn, error) {
	t.Helper()
	opts := []nats.Option{nats.Name(cli.CtlNameForSession(sid))}
	if pin != "" {
		opts = append(opts, nats.Token(pin))
	}
	nc, err := cli.ConnectNATSWithNkey(url, id, opts...)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { nc.Close() })
	return nc, nil
}
