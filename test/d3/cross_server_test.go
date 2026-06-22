package d3_test

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// connectCLI connects a fresh unactivated CLI (forces a real auth_callout — the
// user nkey is not an AuthUser) to url. Returns the connection or the error.
func connectCLI(url string) (*nats.Conn, error) {
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...)
	kp.Wipe()
	return nats.Connect(url,
		nats.Name("tether-cli"),
		nats.Nkey(pub, sigFromSeed(seed)),
		nats.Timeout(4*time.Second),
	)
}

// TestD3CrossServerCalloutAuthorizes is the exit-criterion-1 test: a client
// connects to server A while the ONLY auth_callout responder (queue group
// tether-authcallout) is broker B on the routed server B. Success proves B
// answered the request raised on A over the routes. Two vacuity controls prove
// the success is not spurious.
func TestD3CrossServerCalloutAuthorizes(t *testing.T) {
	t.Run("positive: B on routed server answers A's request", func(t *testing.T) {
		brokers := []brokerID{newBrokerID(t), newBrokerID(t)}
		acctKp, acctPub := newAccount(t)
		servers := startRouted(t, authClusterOpts(brokers, acctPub, 2, nil))
		// Responder ONLY on server B (index 1).
		h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
		runResponder(t, servers[1].ClientURL(), brokers[1], h)

		nc, err := connectCLI(servers[0].ClientURL()) // connect to A
		if err != nil {
			t.Fatalf("cross-server callout failed — B did not answer A's request: %v", err)
		}
		nc.Close()
	})

	t.Run("vacuity: no responder => client is denied", func(t *testing.T) {
		brokers := []brokerID{newBrokerID(t), newBrokerID(t)}
		_, acctPub := newAccount(t)
		servers := startRouted(t, authClusterOpts(brokers, acctPub, 2, nil))
		// NO responder anywhere. This row asserts NON-AUTHORIZATION (the connect
		// fails because no callout answers), discriminating responder-present from
		// responder-absent; it is not an assertion of an explicit DENY reason.
		if nc, err := connectCLI(servers[0].ClientURL()); err == nil {
			nc.Close()
			t.Fatal("with no auth_callout responder the client must be denied (proves the positive case depended on B answering)")
		}
	})

	t.Run("vacuity: responder signs with a non-Issuer key => A rejects", func(t *testing.T) {
		brokers := []brokerID{newBrokerID(t), newBrokerID(t)}
		acctKp, acctPub := newAccount(t)
		servers := startRouted(t, authClusterOpts(brokers, acctPub, 2, nil))
		// Responder on B signs with a DIFFERENT account key than the configured Issuer.
		wrongKp, err := nkeys.CreateAccount()
		if err != nil {
			t.Fatal(err)
		}
		_ = acctKp
		h := &authcallout.Handler{DB: openMemDB(t), AccountKp: wrongKp, Now: time.Now}
		runResponder(t, servers[1].ClientURL(), brokers[1], h)

		if nc, err := connectCLI(servers[0].ClientURL()); err == nil {
			nc.Close()
			t.Fatal("A must reject a response not signed by the configured Issuer (proves A validates the account signature, not mere reachability)")
		}
	})
}
