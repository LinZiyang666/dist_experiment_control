// infra_fixture_test.go holds the production-path broker infrastructure fixtures.
// origin: S10 internal review ARCH-REDS-2 F3
package broker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// The wire recorder (infraWire) and the admission/mutation predicates live in
// infra_wire_recorder_test.go; this file holds the production-path fixtures.
type brokerInfraFixture struct {
	opts   natsserver.Options
	server *natsserver.Server
	peers  []*natsserver.Server
	wire   *infraWire
	db     *sql.DB
	keys   map[string]nkeys.KeyPair
	pubs   map[string]string
	admin  jetstream.JetStream
	// peerProbes counts peer answers to the broker's own ordinary probes.
	peerProbes atomic.Int64
}

func newBrokerInfraFixture(t *testing.T, jetStream, peer bool) *brokerInfraFixture {
	t.Helper()
	f := &brokerInfraFixture{db: openDB(t), keys: map[string]nkeys.KeyPair{}, pubs: map[string]string{}}
	for _, role := range []string{"broker", "member", "agent", "account"} {
		create := nkeys.CreateUser
		if role == "account" {
			create = nkeys.CreateAccount
		}
		key, err := create()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(key.Wipe)
		f.keys[role] = key
		f.pubs[role], _ = key.PublicKey()
	}
	f.seed(t, f.db)
	f.opts = natsserver.Options{ServerName: "A", Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true,
		Nkeys:       []*natsserver.NkeyUser{{Nkey: f.pubs["broker"]}},
		AuthCallout: &natsserver.AuthCallout{Issuer: f.pubs["account"], Account: "$G", AuthUsers: []string{f.pubs["broker"]}}}
	if jetStream {
		f.opts.JetStream, f.opts.StoreDir = true, t.TempDir()
	}
	if peer {
		f.opts.Cluster = natsserver.ClusterOpts{Name: "infra", Host: "127.0.0.1", Port: -1}
	}
	f.start(t)
	// Freeze the ports: a restart is the same local server the broker must rejoin.
	f.opts.Port = f.server.Addr().(*net.TCPAddr).Port
	if peer {
		// Three brokers: the server hands each callout to one of three queue
		// members, so a peer answers most of the local broker's probes.
		f.opts.Cluster.Port = f.server.ClusterAddr().Port
		f.startPeer(t, "B")
		f.startPeer(t, "C")
	}
	t.Cleanup(f.stop)
	f.wire = newInfraWire(t, fmt.Sprintf("127.0.0.1:%d", f.opts.Port))
	if jetStream {
		admin := f.direct(t, f.server.ClientURL())
		f.admin, _ = jetstream.New(admin)
	}
	return f
}

// seed activates the member and provisions the agent through the ordinary
// session/provisioning writers; the callout decides them like any client.
func (f *brokerInfraFixture) seed(t *testing.T, db *sql.DB) {
	t.Helper()
	memberFP, _ := auth.FingerprintFromActor(f.pubs["member"])
	agentFP, _ := auth.FingerprintFromActor(f.pubs["agent"])
	if _, err := session.Create(db, infraSID, infraSID, memberFP, "unused", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(db, infraSID, infraNID, agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func (f *brokerInfraFixture) start(t *testing.T) {
	t.Helper()
	opts := f.opts
	s, err := natsserver.NewServer(&opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("infra fixture server not ready")
	}
	f.server = s
}

func (f *brokerInfraFixture) stop() {
	if f.server != nil {
		f.server.Shutdown()
		f.server.WaitForShutdown()
	}
}

// down/up model a stock local restart: while the server is gone its port (and
// so the recorder in front of it) refuses connections.
func (f *brokerInfraFixture) down() {
	f.wire.suspend()
	f.stop()
}

func (f *brokerInfraFixture) up(t *testing.T) {
	t.Helper()
	f.start(t)
	f.wire.resume(t)
}

func (f *brokerInfraFixture) restart(t *testing.T, b *Broker) {
	t.Helper()
	nc := b.nc.Load()
	f.down()
	// Server shutdown and client EOF run on separate goroutines. Observe the
	// old admission's retirement before the replacement can make a wait for
	// "admitted" accidentally succeed on the previous connected generation.
	// origin: S10 internal review POLISH-A F3 / hot-update S9 plan §4.4 / T48
	if !testharness.WaitFor(t, 20*time.Second, 5*time.Millisecond, func() bool {
		return !b.infra.Admitted() && !nc.IsConnected()
	}) {
		t.Fatal("old broker admission did not retire before restart")
	}
	f.up(t)
}

// startPeer is a routed survivor whose own normal queue is HEALTHY. A peer
// answer to the local server's callout is never the local normal-queue proof,
// and must not cost the local broker its admission either.
func (f *brokerInfraFixture) startPeer(t *testing.T, name string) {
	t.Helper()
	opts := f.opts
	opts.ServerName, opts.Port, opts.JetStream, opts.StoreDir = name, -1, false, ""
	opts.Cluster = natsserver.ClusterOpts{Name: "infra", Host: "127.0.0.1", Port: -1}
	opts.Routes = natsserver.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", f.opts.Cluster.Port))
	peer, err := natsserver.NewServer(&opts)
	if err != nil {
		t.Fatal(err)
	}
	peer.Start()
	f.peers = append(f.peers, peer)
	t.Cleanup(func() { peer.Shutdown(); peer.WaitForShutdown() })
	if !peer.ReadyForConnections(5*time.Second) || !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool { return f.server.NumRoutes() >= len(f.peers) }) {
		t.Fatal("peer not routed")
	}
	h := &authcallout.Handler{DB: f.db, AccountKp: f.keys["account"], Now: time.Now, JWTTTL: time.Hour, Logger: silentLogger()}
	nc := f.direct(t, peer.ClientURL())
	if _, err := nc.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", func(m *nats.Msg) {
		if response, err := h.Handle(string(m.Data)); err == nil {
			if req, err := jwt.DecodeAuthorizationRequestClaims(string(m.Data)); err == nil && req.Server.ID != peer.ID() &&
				(req.ConnectOptions.Nkey == f.pubs["member"] || req.ConnectOptions.Nkey == f.pubs["agent"]) {
				f.peerProbes.Add(1)
			}
			_ = m.Respond([]byte(response))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

// direct is a static broker client that bypasses the wire recorder.
func (f *brokerInfraFixture) direct(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Nkey(f.pubs["broker"], f.keys["broker"].Sign), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func (f *brokerInfraFixture) infra(recovery bool) *infraattach.BrokerInfra {
	member, agent := proto.SubjCtrlNodeList(f.pubs["member"], infraSID), proto.SubjNodeRegister(infraSID, infraNID)
	respond := func(m *nats.Msg) { _ = m.Respond([]byte(`{"ok":true}`)) }
	in := &infraattach.BrokerInfra{Auth: &infraattach.BrokerAuth{AccountPublicKey: f.pubs["account"],
		Business: []infraauth.Binding{{Subject: member, Handle: respond}, {Subject: agent, Handle: respond}},
		Member: infraauth.Identity{Name: "tether-cli:" + infraSID, PublicKey: f.pubs["member"], Sign: f.keys["member"].Sign,
			RPCSubject: member, RPCResponse: []byte(`{"ok":true}`)},
		Agent: infraauth.Identity{Name: "tether-agent:" + infraSID + ":" + infraNID, PublicKey: f.pubs["agent"], Sign: f.keys["agent"].Sign,
			RPCSubject: agent, RPCResponse: []byte(`{"ok":true}`)}}}
	if !recovery {
		return in
	}
	in.Recovery = &infraattach.BrokerRecovery{Domain: c.ID{7},
		Assets: func(d c.Deadline, _ c.Stop) (c.AssetSeal, error) {
			left, err := controlbudget.Remaining(d)
			if err != nil {
				return c.AssetSeal{}, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), left) // ctx-none: test port has only a Deadline.
			defer cancel()
			var names []string
			list := f.admin.StreamNames(ctx)
			for name := range list.Name() {
				names = append(names, name)
			}
			slices.Sort(names)
			return c.AssetSeal{Accounts: c.Digest{1}, Assets: sha256.Sum256([]byte(strings.Join(names, ","))), Complete: true}, list.Err()
		},
		Exclusive: func(d c.Deadline, _ c.Stop) error { _, err := controlbudget.Remaining(d); return err },
		Transfers: func(c.Deadline) (c.TransferSeal, error) {
			return c.TransferSeal{Version: 1, SourceSHA256: c.Digest{1}, SchemaSHA256: c.Digest{2}, InvocationSet: c.Digest{3},
				ReceiptSet: c.Digest{4}, ConsumerSet: c.Digest{5}, Complete: true}, nil
		}}
	return in
}

func (f *brokerInfraFixture) config(recovery bool) Config {
	brokerSeed, _ := f.keys["broker"].Seed()
	accountSeed, _ := f.keys["account"].Seed()
	return Config{NATSURL: f.wire.url(), DB: f.db, Logger: silentLogger(), Now: time.Now,
		AuthCallout: &AuthCalloutConfig{BrokerNkeySeed: brokerSeed, AccountSeed: accountSeed}, Infra: f.infra(recovery)}
}

// run drives the production Broker.Run cold path and returns its stop.
func (f *brokerInfraFixture) run(t *testing.T, recovery bool) *Broker {
	t.Helper()
	cfg := f.config(recovery)
	cfg.ReadyCh, cfg.ReconcileInterval = make(chan struct{}), 50*time.Millisecond
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: the test owns this broker's Run lifetime.
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Error("broker Run did not stop")
		}
	})
	select {
	case <-cfg.ReadyCh:
	case err := <-done:
		t.Fatalf("broker Run failed before ready: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("broker never became ready")
	}
	return b
}

func infraAdmitted(t *testing.T, b *Broker) {
	t.Helper()
	if !testharness.WaitFor(t, 20*time.Second, 5*time.Millisecond, func() bool {
		nc := b.nc.Load()
		return b.infra.Admitted() && nc != nil && nc.IsConnected()
	}) {
		t.Fatal("broker business NC was never re-admitted")
	}
}

func infraCutDeadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
