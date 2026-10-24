package infraattach

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// brokerAttachFixture is one restartable local server with the real normal
// callout handler, an activated member and a provisioned agent. Restarts reuse
// the same port and JetStream store, like a stock nats-server process restart.
type brokerAttachFixture struct {
	opts    ns.Options
	server  *ns.Server
	db      *sql.DB
	keys    map[string]nkeys.KeyPair
	pubs    map[string]string
	handler *authcallout.Handler
	stall   atomic.Pointer[chan struct{}]
	calls   atomic.Int64
	replies atomic.Int64
	owner   *Broker
	nc      *nats.Conn
}

const attachSID, attachNID = "attachb", "node"

func brokerAttachDeadline(t *testing.T, budget time.Duration) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func newBrokerAttachFixture(t *testing.T, jetStream bool) *brokerAttachFixture {
	t.Helper()
	f := &brokerAttachFixture{keys: map[string]nkeys.KeyPair{}, pubs: map[string]string{}}
	var err error
	if f.db, err = storage.Open(":memory:"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
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
	memberFP, _ := auth.FingerprintFromActor(f.pubs["member"])
	agentFP, _ := auth.FingerprintFromActor(f.pubs["agent"])
	if _, err = session.Create(f.db, attachSID, attachSID, memberFP, "unused", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = agentprov.Provision(f.db, attachSID, attachNID, agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.handler = &authcallout.Handler{DB: f.db, AccountKp: f.keys["account"], Now: time.Now, JWTTTL: time.Hour, Logger: testharness.SilentLog()}
	f.opts = ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true,
		Nkeys:       []*ns.NkeyUser{{Nkey: f.pubs["broker"]}},
		AuthCallout: &ns.AuthCallout{Issuer: f.pubs["account"], Account: "$G", AuthUsers: []string{f.pubs["broker"]}}}
	if jetStream {
		f.opts.JetStream, f.opts.StoreDir = true, t.TempDir()
	}
	f.start(t)
	// Freeze the port so a restart is the same local address the broker dials.
	f.opts.Port = f.server.Addr().(*net.TCPAddr).Port
	t.Cleanup(f.stop)
	return f
}

func (f *brokerAttachFixture) start(t *testing.T) {
	t.Helper()
	opts := f.opts
	s, err := ns.NewServer(&opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("broker attachment server not ready")
	}
	f.server = s
}

func (f *brokerAttachFixture) stop() {
	if f.server != nil {
		f.server.Shutdown()
		f.server.WaitForShutdown()
	}
}

func (f *brokerAttachFixture) url() string { return fmt.Sprintf("nats://127.0.0.1:%d", f.opts.Port) }

// handle is the real normal callout handler, optionally stalled so a test can
// inspect the server while the ordinary auth-first proof is still incomplete.
func (f *brokerAttachFixture) handle(request string) (string, error) {
	f.calls.Add(1)
	if ch := f.stall.Load(); ch != nil {
		select {
		case <-*ch:
		case <-time.After(10 * time.Second):
			return "", errors.New("stall not released")
		}
	}
	return f.handler.Handle(request)
}

// replied stands in for S6's reply witnesses (AuthReplied in production).
func (f *brokerAttachFixture) replied(string, string) { f.replies.Add(1) }

func (f *brokerAttachFixture) config(recovery *BrokerRecovery) BrokerConfig {
	member, agent := proto.SubjCtrlNodeList(f.pubs["member"], attachSID), proto.SubjNodeRegister(attachSID, attachNID)
	respond := func(m *nats.Msg) { _ = m.Respond([]byte(`{"ok":true}`)) }
	seed, _ := f.keys["broker"].Seed()
	return BrokerConfig{URL: f.url(), Logger: testharness.SilentLog(),
		Options: func() ([]nats.Option, string, error) { return BrokerOptions(testharness.SilentLog(), seed, true) },
		Infra: &BrokerInfra{Recovery: recovery, Auth: &BrokerAuth{AccountPublicKey: f.pubs["account"],
			Business: []infraauth.Binding{{Subject: member, Handle: respond}, {Subject: agent, Handle: respond}},
			Member: infraauth.Identity{Name: "tether-cli:" + attachSID, PublicKey: f.pubs["member"], Sign: f.keys["member"].Sign,
				RPCSubject: member, RPCResponse: []byte(`{"ok":true}`)},
			Agent: infraauth.Identity{Name: "tether-agent:" + attachSID + ":" + attachNID, PublicKey: f.pubs["agent"], Sign: f.keys["agent"].Sign,
				RPCSubject: agent, RPCResponse: []byte(`{"ok":true}`)}}}}
}

func (f *brokerAttachFixture) open(t *testing.T, recovery *BrokerRecovery) {
	t.Helper()
	var err error
	if f.owner, err = NewBroker(f.config(recovery)); err != nil {
		t.Fatal(err)
	}
	if f.nc, err = f.owner.Connect(0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.nc.Close()
		if err := f.owner.Close(brokerAttachDeadline(t, 5*time.Second)); err != nil {
			t.Error(err)
		}
	})
}

func (f *brokerAttachFixture) arm(t *testing.T) infraauth.Receipt {
	t.Helper()
	r, err := f.owner.Arm(f.handle, f.replied, brokerAttachDeadline(t, 10*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The only queue member here answered both probes of every generation
	// proven so far; each delivered answer reaches the reply witnesses before
	// the admission it proves (an SDK redial may already have re-proven one).
	if f.replies.Load() < 2 {
		t.Fatalf("delivered answers reached the reply witness %d times, want both probes", f.replies.Load())
	}
	return r
}

// subscriptions is the server's own record of what each named connection has
// actually installed — not what the client merely buffered.
func (f *brokerAttachFixture) subscriptions(t *testing.T) map[string][]string {
	t.Helper()
	z, err := f.server.Connz(&ns.ConnzOptions{SubscriptionsDetail: true, Limit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, conn := range z.Conns {
		out[conn.Name] = append(out[conn.Name], "")
		for _, sub := range conn.SubsDetail {
			out[conn.Name] = append(out[conn.Name], sub.Subject+"|"+sub.Queue)
		}
	}
	return out
}

func (f *brokerAttachFixture) cid(t *testing.T, name string) uint64 {
	t.Helper()
	z, err := f.server.Connz(&ns.ConnzOptions{Limit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for _, conn := range z.Conns {
		if conn.Name == name {
			return conn.Cid
		}
	}
	return 0
}

func waitAttach(t *testing.T, what string, predicate func() bool) {
	t.Helper()
	if !testharness.WaitFor(t, 15*time.Second, 5*time.Millisecond, predicate) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// requireAuthFirstWhileStalled holds the normal handler inside the ordinary
// member/agent CONNECT, then reads the SERVER state: the auth-only NC already
// owns the local normal queue, while no business connection and no business
// SUB exist yet. Releasing the stall must then admit the business NC.
func requireAuthFirstWhileStalled(t *testing.T, f *brokerAttachFixture, admit func()) {
	t.Helper()
	stall := make(chan struct{})
	f.stall.Store(&stall)
	calls := f.calls.Load()
	admit()
	waitAttach(t, "ordinary identity CONNECT to reach the normal handler", func() bool { return f.calls.Load() > calls })
	for range 20 {
		subs := f.subscriptions(t)
		if !slices.Contains(subs["tetherd-auth"], "$SYS.REQ.USER.AUTH|tether-authcallout") {
			t.Fatalf("ordinary CONNECT ran before the auth-only NC owned the local normal queue: %v", subs)
		}
		if _, business := subs["tetherd"]; business {
			t.Fatalf("business NC dialled before ordinary auth was proven: %v", subs)
		}
		for name, list := range subs {
			if slices.Contains(list, "biz.replay|") {
				t.Fatalf("business SUB replayed on %q before auth_usable: %v", name, subs)
			}
		}
		if f.owner.Admitted() {
			t.Fatal("business admission opened during the auth-first proof")
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.stall.Store(nil)
	close(stall)
}

// origin: hot-update S9 plan §4.4 / T48 / M24; rv-business F2 / rv-infra F10
func TestBrokerAttachmentGatesBusinessDialOnAuthGeneration(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	f.open(t, nil)
	// A SUB created before admission is only BUFFERED by nats.go; the server
	// sees it when the gated dial finally succeeds and the SDK replays.
	if _, err := f.nc.Subscribe("biz.replay", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	// gotcha #89: no peer address is ever dialled, even if the SDK pool learnt one.
	if _, err := f.owner.Dial("tcp", "127.0.0.1:1"); !errors.Is(err, c.ErrIncomplete) {
		t.Fatalf("broker business dial roamed off its local server: %v", err)
	}
	if _, err := f.owner.Dial("tcp", f.owner.address); !errors.Is(err, ErrBrokerAdmission) {
		t.Fatalf("business dial admitted with no normal handler: %v", err)
	}
	var cold infraauth.Receipt
	done := make(chan error, 1)
	requireAuthFirstWhileStalled(t, f, func() {
		go func() {
			var err error
			cold, err = f.owner.Arm(f.handle, f.replied, brokerAttachDeadline(t, 10*time.Second), nil)
			done <- err
		}()
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// nats.go reports connected after CONNECT/PONG and only then flushes the
	// SUB replay, so the server's own record is polled, not sampled once.
	if cold.Phase != infraauth.ControlReady {
		t.Fatalf("cold admission incomplete: %+v", cold)
	}
	waitAttach(t, "cold business SUB on the server", func() bool { return slices.Contains(f.subscriptions(t)["tetherd"], "biz.replay|") })
	firstBusiness := f.cid(t, "tetherd")
	// Ordinary reconnect: a stock local restart retires every socket. The SDK
	// redials by itself, so the gate alone must hold back the SUB replay.
	requireAuthFirstWhileStalled(t, f, func() {
		f.stop()
		waitAttach(t, "synchronous admission fence", func() bool { return !f.owner.Admitted() })
		f.start(t)
	})
	waitAttach(t, "ordinary reconnect admission", func() bool { return f.owner.Admitted() && f.nc.IsConnected() })
	_, receipt, err := f.owner.auth.Business()
	if err != nil || receipt.Generation <= cold.Generation {
		t.Fatalf("reconnect did not re-prove a fresh generation: %+v err=%v", receipt, err)
	}
	waitAttach(t, "replayed business SUB on the server", func() bool { return slices.Contains(f.subscriptions(t)["tetherd"], "biz.replay|") })
	if f.cid(t, "tetherd") == 0 || firstBusiness == 0 {
		t.Fatal("business connection identity missing")
	}
}

// origin: hot-update S9 plan §4.4 / T48; rv-business F2 (disconnect closes business admission)
func TestBrokerAttachmentFencesAdmissionOnAuthLoss(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	f.open(t, nil)
	var delivered atomic.Int64
	handler := Admit(f.owner, func(*nats.Msg) { delivered.Add(1) })
	if _, err := f.nc.Subscribe("biz.fence", handler); err != nil {
		t.Fatal(err)
	}
	first := f.arm(t)
	business := f.cid(t, "tetherd")
	// Losing ONLY the auth-only NC removes the local normal queue. Business may
	// not remain admitted without it: the watcher retires the business socket,
	// and the redial must earn a fresh generation through the same gate.
	if err := f.server.DisconnectClientByID(f.cid(t, "tetherd-auth")); err != nil {
		t.Fatal(err)
	}
	waitAttach(t, "business redial after auth loss", func() bool {
		cid := f.cid(t, "tetherd")
		return cid != 0 && cid != business && f.owner.Admitted()
	})
	_, second, err := f.owner.auth.Business()
	if err != nil || second.Generation <= first.Generation {
		t.Fatalf("auth loss reused the old generation: %+v err=%v", second, err)
	}
	// Losing ONLY the business socket fences inside its raw Read, while the auth
	// generation is still live. The admission worker is held so neither a
	// re-proof nor the generation watcher can be what closes admission.
	f.owner.mu.Lock()
	f.owner.working = true
	raw := f.owner.raw
	f.owner.mu.Unlock()
	_ = raw.Conn.Close()
	waitAttach(t, "synchronous fence on business socket loss", func() bool { return !f.owner.Admitted() })
	if _, live, err := f.owner.auth.Business(); err != nil || live.Generation != second.Generation {
		t.Fatalf("the auth generation changed under the held worker: %+v err=%v", live, err)
	}
	handler(&nats.Msg{Subject: "biz.fence"})
	f.owner.mu.Lock()
	f.owner.working = false
	f.owner.mu.Unlock()
	waitAttach(t, "business re-admission after the socket loss", func() bool { return f.owner.Admitted() && f.nc.IsConnected() })
	// While the local server is down the fence is closed: a queued business
	// callback cannot run under a lost generation.
	f.stop()
	waitAttach(t, "fence on disconnect", func() bool { return !f.owner.Admitted() })
	handler(&nats.Msg{Subject: "biz.fence"})
	if delivered.Load() != 0 {
		t.Fatal("fenced business handler ran")
	}
	f.start(t)
	f.arm(t)
	handler(&nats.Msg{Subject: "biz.fence"})
	if delivered.Load() != 1 {
		t.Fatal("re-admitted business handler did not run")
	}
}

// origin: hot-update S9 plan §4.4 / T48 (S6 Fence/Thaw re-admission)
func TestBrokerAttachmentRetireAndRearm(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	f.open(t, nil)
	// A cancelled cold Run must not wait out the admission budget.
	stall, stopped := make(chan struct{}), make(chan struct{})
	f.stall.Store(&stall)
	close(stopped)
	began := time.Now()
	if _, err := f.owner.Arm(f.handle, f.replied, brokerAttachDeadline(t, 10*time.Second), stopped); !errors.Is(err, ErrBrokerAdmission) || time.Since(began) > 2*time.Second {
		t.Fatalf("stopped Arm waited %s / %v", time.Since(began), err)
	}
	f.stall.Store(nil)
	close(stall)
	first := f.arm(t)
	business := f.cid(t, "tetherd")
	// Retire JOINS the normal queue: while an admitted callback still borrows
	// the store, the S6 Fence step must not report a clean retirement.
	held := make(chan struct{})
	f.stall.Store(&held)
	calls := f.calls.Load()
	dialed := make(chan struct{})
	go func() {
		defer close(dialed)
		member := nats.Nkey(f.pubs["member"], f.keys["member"].Sign)
		if nc, err := nats.Connect(f.url(), member, nats.Name("tether-cli:"+attachSID), nats.NoReconnect()); err == nil {
			nc.Close()
		}
	}()
	waitAttach(t, "an ordinary CONNECT to enter the normal handler", func() bool { return f.calls.Load() > calls })
	if err := f.owner.Retire(brokerAttachDeadline(t, 200*time.Millisecond)); err == nil {
		t.Fatal("Retire reported a joined normal queue while its callback still borrowed the store")
	}
	f.stall.Store(nil)
	close(held)
	<-dialed
	if err := f.owner.Retire(brokerAttachDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if f.owner.Admitted() {
		t.Fatal("retire left business admission open")
	}
	// Retire has joined and closed the auth-only socket; the server drops the
	// client (and its normal queue) when it reads that close, asynchronously.
	waitAttach(t, "retired normal queue leaves the server", func() bool { return f.cid(t, "tetherd-auth") == 0 })
	if _, err := f.owner.Dial("tcp", f.owner.address); !errors.Is(err, ErrBrokerAdmission) {
		t.Fatalf("retired gate admitted a dial: %v", err)
	}
	// A reversible fence keeps the old business socket; Arm must redial it
	// through the gate rather than readmit the stale connection.
	second := f.arm(t)
	if second.Generation <= first.Generation || f.cid(t, "tetherd") == business {
		t.Fatalf("re-arm reused the retired generation or socket: %+v", second)
	}
}

// origin: hot-update S9 plan §4.4 / P22 (N-1: an unconfigured broker is the historical connect)
func TestBrokerAttachmentLegacyModeIsUnchanged(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	seed, _ := f.keys["broker"].Seed()
	owner, err := NewBroker(BrokerConfig{URL: f.url(), Logger: testharness.SilentLog(),
		Options: func() ([]nats.Option, string, error) { return BrokerOptions(testharness.SilentLog(), seed, true) }})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := owner.Connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if owner.Configured() || !owner.Admitted() || owner.Recovery() != nil || ImageExclusive(owner) != nil {
		t.Fatal("legacy broker acquired S9 state")
	}
	if nc.Opts.CustomDialer != nil || nc.Opts.RetryOnFailedConnect || !nc.Opts.IgnoreDiscoveredServers || nc.Opts.MaxReconnect != -1 {
		t.Fatalf("legacy options changed: %+v", nc.Opts)
	}
	if nc.Opts.InboxPrefix != auth.InboxPrefixFor(f.pubs["broker"]) {
		t.Fatal("legacy broker lost its private reply inbox")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if bound, err := owner.BindJetStream(js, nil); err != nil || bound != js {
		t.Fatal("legacy JetStream client was wrapped")
	}
	var called bool
	Admit(owner, func(*nats.Msg) { called = true })(&nats.Msg{})
	if !called {
		t.Fatal("legacy business handler was fenced")
	}
	if _, err := owner.Arm(func(string) (string, error) { return "", nil }, nil, 0, nil); !errors.Is(err, c.ErrUnsupported) {
		t.Fatal("legacy broker accepted an auth-first handler")
	}
	if owner.Retire(0) != nil || owner.Close(0) != nil {
		t.Fatal("legacy close reported S9 work")
	}
}

// origin: hot-update S9 plan §4.4 / P22 (a partial S9 descriptor is never legacy mode)
func TestBrokerAttachmentRejectsPartialDescriptor(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	assets := func(c.Deadline, c.Stop) (c.AssetSeal, error) { return c.AssetSeal{}, nil }
	for name, mutate := range map[string]func(*BrokerConfig){
		"recovery without identities": func(cfg *BrokerConfig) { cfg.Infra.Auth = nil; cfg.Infra.Recovery = &BrokerRecovery{} },
		"no static credential": func(cfg *BrokerConfig) {
			cfg.Options = func() ([]nats.Option, string, error) { return BrokerOptions(testharness.SilentLog(), nil, false) }
		},
		"incomplete recovery": func(cfg *BrokerConfig) { cfg.Infra.Recovery = &BrokerRecovery{Domain: c.ID{1}, Assets: assets} },
		"no option builder":   func(cfg *BrokerConfig) { cfg.Options = nil },
		"roaming pool":        func(cfg *BrokerConfig) { cfg.URL += ",nats://127.0.0.1:1" },
		"missing agent":       func(cfg *BrokerConfig) { cfg.Infra.Auth.Agent = infraauth.Identity{} },
	} {
		cfg := f.config(nil)
		mutate(&cfg)
		if _, err := NewBroker(cfg); err == nil {
			t.Errorf("%s: partial S9 descriptor accepted", name)
		}
	}
}

func recoveryPorts(t *testing.T, f *brokerAttachFixture) (*BrokerRecovery, jetstream.JetStream) {
	t.Helper()
	seed, _ := f.keys["broker"].Seed()
	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kp.Wipe)
	admin, err := nats.Connect(f.url(), nats.Nkey(f.pubs["broker"], kp.Sign), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	js, err := jetstream.New(admin)
	if err != nil {
		t.Fatal(err)
	}
	return &BrokerRecovery{Domain: c.ID{9},
		Assets: func(d c.Deadline, _ c.Stop) (c.AssetSeal, error) {
			left, err := controlbudget.Remaining(d)
			if err != nil {
				return c.AssetSeal{}, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), left) // ctx-none: test port has only a Deadline.
			defer cancel()
			var names []string
			list := js.StreamNames(ctx)
			for name := range list.Name() {
				names = append(names, name)
			}
			slices.Sort(names)
			return c.AssetSeal{Accounts: c.Digest{1}, Assets: c.Digest(sha256.Sum256([]byte(fmt.Sprint(names)))), Complete: true}, list.Err()
		},
		Exclusive: func(d c.Deadline, _ c.Stop) error { _, err := controlbudget.Remaining(d); return err },
		Transfers: func(c.Deadline) (c.TransferSeal, error) {
			return c.TransferSeal{Version: 1, SourceSHA256: c.Digest{1}, SchemaSHA256: c.Digest{2}, InvocationSet: c.Digest{3}, ReceiptSet: c.Digest{4}, ConsumerSet: c.Digest{5}, Complete: true}, nil
		}}, js
}

// origin: hot-update S9 plan §4.3 / T40 / T49 / M20; rv-business F2 (pre-repair read gates ordinary reconnect)
func TestBrokerRecoveryBlocksBusinessOnLostPrefix(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost=%v", lost), func(t *testing.T) {
			f := newBrokerAttachFixture(t, true)
			ports, admin := recoveryPorts(t, f)
			ctx := t.Context()
			if _, err := admin.CreateStream(ctx, jetstream.StreamConfig{Name: "AUDITX", Subjects: []string{"auditx.>"}, Storage: jetstream.FileStorage}); err != nil {
				t.Fatal(err)
			}
			f.open(t, ports)
			f.arm(t)
			rawJS, err := jetstream.New(f.nc)
			if err != nil {
				t.Fatal(err)
			}
			js, err := f.owner.BindJetStream(rawJS, nil)
			if err != nil || js == rawJS {
				t.Fatalf("configured JetStream was not bound to the recovery gate: %v", err)
			}
			msg := &nats.Msg{Subject: "auditx.original", Data: []byte(`{"original":true}`)}
			if err := PublishAudit(f.owner, js, msg, 2*time.Second); err != nil {
				t.Fatal(err)
			}
			recovery := f.owner.Recovery()
			plan := c.Plan{Tx: c.ID{3}, Domain: ports.Domain, Component: c.NATS}
			cp, err := recovery.Seal(plan, brokerAttachDeadline(t, 5*time.Second))
			if err != nil || len(f.owner.ledger) == 0 {
				t.Fatalf("seal did not capture the original ACK: %v", err)
			}
			if err := ImageExclusive(f.owner); !errors.Is(err, ErrInfraPending) {
				t.Fatal("sealed recovery did not refuse an image cut")
			}
			if err := ExclusiveRole(f.owner, func(bool) error { return nil })(true); !errors.Is(err, ErrInfraPending) {
				t.Fatal("sealed recovery did not refuse the cut-role sample")
			}
			// A repair producer is fenced as soon as the seal exists.
			if err := PublishAudit(f.owner, js, &nats.Msg{Subject: "auditx.repair", Data: []byte(`{"original":true}`)}, time.Second); !errors.Is(err, infrarepair.ErrFenced) {
				t.Fatalf("sealed producer published: %v", err)
			}
			if lost {
				stream, err := admin.Stream(ctx, "AUDITX")
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.DeleteMsg(ctx, 1); err != nil {
					t.Fatal(err)
				}
			}
			f.stop()
			waitAttach(t, "admission fence", func() bool { return !f.owner.Admitted() })
			f.start(t)
			if lost {
				// The read-only prefix read happens before business replay and keeps
				// failing: business stays fenced instead of serving over a hole.
				time.Sleep(2 * time.Second)
				if f.owner.Admitted() || f.nc.IsConnected() || f.cid(t, "tetherd") != 0 {
					t.Fatal("business reconnected over a lost acknowledged message")
				}
				if err := recovery.VerifyBeforeReplay(cp, brokerAttachDeadline(t, 5*time.Second), nil); !errors.Is(err, infrarepair.ErrReceipt) {
					t.Fatalf("lost prefix verified: %v", err)
				}
				return
			}
			waitAttach(t, "reconnect after the verified prefix", func() bool { return f.owner.Admitted() && f.nc.IsConnected() })
			if err := recovery.VerifyBeforeReplay(cp, brokerAttachDeadline(t, 5*time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if err := recovery.Release(cp, brokerAttachDeadline(t, 5*time.Second)); err != nil {
				t.Fatal(err)
			}
			if ImageExclusive(f.owner) != nil {
				t.Fatal("released recovery still refuses image cuts")
			}
			if err := PublishAudit(f.owner, js, &nats.Msg{Subject: "auditx.after", Data: []byte(`{}`)}, 2*time.Second); err != nil {
				t.Fatalf("released producer still fenced: %v", err)
			}
		})
	}
}

// origin: hot-update S9 plan §4.3 / T41 (the ACK ledger is bounded and prunes legitimately retired receipts)
func TestBrokerRecoveryCapturesLiveACKWindow(t *testing.T) {
	f := newBrokerAttachFixture(t, true)
	ports, admin := recoveryPorts(t, f)
	ctx := t.Context()
	for _, name := range []string{"KEEP", "GONE", "TRIM"} {
		if _, err := admin.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
	}
	f.open(t, ports)
	f.arm(t)
	rawJS, _ := jetstream.New(f.nc)
	js, err := f.owner.BindJetStream(rawJS, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"KEEP.a", "GONE.a", "TRIM.a", "TRIM.b"} {
		if err := PublishAudit(f.owner, js, &nats.Msg{Subject: subject, Data: []byte(subject)}, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := admin.DeleteStream(ctx, "GONE"); err != nil {
		t.Fatal(err)
	}
	trim, _ := admin.Stream(ctx, "TRIM")
	if err := trim.Purge(ctx, jetstream.WithPurgeKeep(1)); err != nil {
		t.Fatal(err)
	}
	r, err := f.owner.captureACKs(brokerAttachDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, ack := range r.ACKs {
		kept = append(kept, ack.Subject)
	}
	if !slices.Equal(kept, []string{"KEEP.a", "TRIM.b"}) {
		t.Fatalf("captured receipts %v, want the live KEEP.a and retained TRIM.b only", kept)
	}
	for i := range brokerACKWindow + 10 {
		_ = f.owner.acknowledged(infrarepair.ACK{Account: "$G", Stream: "KEEP", Subject: "KEEP.x", Sequence: uint64(100 + i), Message: c.Digest{1}})
	}
	if len(f.owner.ledger) != brokerACKWindow || f.owner.ledger[0].Sequence != 110 {
		t.Fatalf("ACK ledger not bounded to the newest window: %d", len(f.owner.ledger))
	}
}

// origin: hot-update S9 plan §4.4 / P22 (every admission worker, watcher and raw owner is joined)
func TestBrokerAttachmentLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerAttachment(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker attachment", goroutines)
	testharness.AssertNoFDLeak(t, "broker attachment", fds, 0)
}

func exerciseBrokerAttachment(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	f.open(t, nil)
	f.arm(t)
	f.stop()
	waitAttach(t, "fence", func() bool { return !f.owner.Admitted() })
	f.start(t)
	waitAttach(t, "re-admission", func() bool { return f.owner.Admitted() && f.nc.IsConnected() })
	if err := f.owner.Retire(brokerAttachDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	f.arm(t)
}
