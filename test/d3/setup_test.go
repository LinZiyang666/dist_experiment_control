// Package d3_test holds the §13.8 cross-server / RF1-ACL / fail-closed / PIN
// behavioral suite for the D3 NATS cluster layer. It uses REAL ≥2-node routed
// embedded nats-servers (not the single-server harness) so cross-server callout
// and the broker-only ACL are exercised end-to-end.
package d3_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/storage"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type brokerID struct {
	pub  string
	seed []byte
}

func newBrokerID(t *testing.T) brokerID {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...) // copy BEFORE Wipe (Seed may alias the internal buffer)
	kp.Wipe()
	return brokerID{pub: pub, seed: seed}
}

func newAccount(t *testing.T) (nkeys.KeyPair, string) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	return kp, pub
}

// baseClusterOpts returns embedded-server options with clustering enabled on a
// random port. Caller fills in Nkeys + AuthCallout.
func baseClusterOpts() *natsserver.Options {
	o := natstest.DefaultTestOptions
	o.Port = -1
	o.Cluster.Name = "tether-d3"
	o.Cluster.Host = "127.0.0.1"
	o.Cluster.Port = -1
	return &o
}

// authClusterOpts builds per-server options for an n-server routed cluster where
// every broker pub is an AuthUser (with static PermissionsForBroker) on every
// server and the shared account is the Issuer. authUsersOverride, if non-nil, is
// called to customize each server's AuthUsers (for vacuity controls).
func authClusterOpts(brokers []brokerID, accountPub string, n int, authUsersOverride func(server int) []string) []*natsserver.Options {
	allPubs := make([]string, len(brokers))
	staticUsers := make([]*natsserver.NkeyUser, len(brokers))
	for i, b := range brokers {
		allPubs[i] = b.pub
		staticUsers[i] = &natsserver.NkeyUser{Nkey: b.pub, Permissions: jwtToServerPerms(auth.PermissionsForBroker())}
	}
	out := make([]*natsserver.Options, n)
	for s := 0; s < n; s++ {
		o := baseClusterOpts()
		o.Nkeys = staticUsers
		au := allPubs
		if authUsersOverride != nil {
			au = authUsersOverride(s)
		}
		o.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: au}
		out[s] = o
	}
	return out
}

// startRouted starts the servers in optsList as a routed mesh (server 0 is the
// seed). Returns the servers; waits for readiness and full mesh.
func startRouted(t *testing.T, optsList []*natsserver.Options) []*natsserver.Server {
	t.Helper()
	servers := make([]*natsserver.Server, len(optsList))
	servers[0] = natstest.RunServer(optsList[0])
	seedRoute := fmt.Sprintf("nats://127.0.0.1:%d", servers[0].ClusterAddr().Port)
	for i := 1; i < len(optsList); i++ {
		optsList[i].Routes = natsserver.RoutesFromStr(seedRoute)
		servers[i] = natstest.RunServer(optsList[i])
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})
	for _, s := range servers {
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("routed server not ready")
		}
	}
	for _, s := range servers {
		waitRoutes(t, s, len(servers)-1)
	}
	return servers
}

// runResponder connects broker b to server url and answers $SYS.REQ.USER.AUTH in
// the tether-authcallout queue group with the given Handler (caller owns it so it
// can wire seams / a custom DB). signAccountKp is the account key the Handler signs
// responses with.
func runResponder(t *testing.T, url string, b brokerID, h *authcallout.Handler) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Name("tetherd"), nats.Nkey(b.pub, sigFromSeed(b.seed)), nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("responder connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	if _, err := nc.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", func(m *nats.Msg) {
		resp, herr := h.Handle(string(m.Data))
		if herr != nil {
			return
		}
		if m.Reply != "" {
			_ = m.Respond([]byte(resp))
		}
	}); err != nil {
		t.Fatalf("responder subscribe: %v", err)
	}
	_ = nc.Flush()
	return nc
}

func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func waitRoutes(t *testing.T, ns *natsserver.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ns.NumRoutes() >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server %s never formed %d route(s) (have %d)", ns.Name(), want, ns.NumRoutes())
}

func sigFromSeed(seed []byte) func([]byte) ([]byte, error) {
	s := append([]byte(nil), seed...)
	return func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(s)
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
}

func waitForCond(within time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}

func jwtToServerPerms(p jwt.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: p.Pub.Allow, Deny: p.Pub.Deny},
		Subscribe: &natsserver.SubjectPermission{Allow: p.Sub.Allow, Deny: p.Sub.Deny},
	}
}
