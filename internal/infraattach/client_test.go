package infraattach

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/inframaint"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/ingressmove"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func attachDeadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// P17 owns the normal queue and obtains actual ordinary member/agent CONNECT,
// PONG and private RPC receipts. The recording witness below only tests P23's
// composition boundary; it is not a kernel/canonical ingress acceptance oracle.
type attachFixture struct {
	server    *ns.Server
	db        *sql.DB
	keys      []nkeys.KeyPair
	owner     *infraauth.Owner
	config    Config
	client    *Client
	nc        *nats.Conn
	private   ed25519.PrivateKey
	mu        sync.Mutex
	view      inframaint.View
	now       atomic.Int64
	checks    atomic.Int32
	block     atomic.Bool
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
}

type attachAuthTransport struct{ *infratransport.Transport }

func (t *attachAuthTransport) Cancel() { t.Interrupt() }
func (t *attachAuthTransport) Join(d c.Deadline) error {
	t.Interrupt()
	_, err := controlbudget.Remaining(d)
	return err
}

type recordingWitness struct{}

func (recordingWitness) Verify(c.Attachment, c.RouteTarget, c.Deadline) error { return nil }
func (recordingWitness) Census(c.Plan, []c.Attachment, c.Deadline) (c.Census, error) {
	return c.Census{Complete: true, Generation: 1, Digest: c.Digest{1}}, nil
}
func attachProcess() c.Process {
	return c.Process{BootID: "fixture", PID: 1, StartTicks: 1, InvocationID: "fixture", ExeDevice: 1, ExeInode: 1, ExeSHA256: c.Digest{1}, Cgroup: "/fixture"}
}

func newAttachFixture(t *testing.T) *attachFixture {
	t.Helper()
	f := &attachFixture{entered: make(chan struct{}), release: make(chan struct{})}
	var err error
	f.db, err = storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		key, e := nkeys.CreateUser()
		if e != nil {
			t.Fatal(e)
		}
		f.keys = append(f.keys, key)
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	f.keys = append(f.keys, account)
	broker, _ := f.keys[0].PublicKey()
	member, _ := f.keys[1].PublicKey()
	agent, _ := f.keys[2].PublicKey()
	accountPub, _ := account.PublicKey()
	memberFP, _ := auth.FingerprintFromActor(member)
	agentFP, _ := auth.FingerprintFromActor(agent)
	if _, err = session.Create(f.db, "attach", "attach", memberFP, "unused", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = agentprov.Provision(f.db, "attach", "node", agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.server, err = ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true,
		Nkeys: []*ns.NkeyUser{{Nkey: broker}}, AuthCallout: &ns.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{broker}}})
	if err != nil {
		t.Fatal(err)
	}
	f.server.Start()
	if !f.server.ReadyForConnections(time.Second) {
		t.Fatal("auth server not ready")
	}
	handler := &authcallout.Handler{DB: f.db, AccountKp: account, Now: time.Now, JWTTTL: time.Hour, Logger: testharness.SilentLog()}
	memberSubject, agentSubject := proto.SubjCtrlNodeList(member, "attach"), proto.SubjNodeRegister("attach", "node")
	respond := func(m *nats.Msg) {
		if !strings.HasPrefix(m.Reply, auth.InboxRoot+".") {
			t.Error("ordinary RPC lost private inbox")
		}
		_ = m.Respond([]byte(`{"ok":true}`))
	}
	f.owner, err = infraauth.New(infraauth.Config{URL: f.server.ClientURL(), AccountPublicKey: accountPub,
		Options: func() []nats.Option { return []nats.Option{nats.Nkey(broker, f.keys[0].Sign)} },
		Transport: func(d c.Deadline, stop c.Stop) (infraauth.Transport, error) {
			transport, e := infratransport.New(1, d, stop, nil)
			return &attachAuthTransport{transport}, e
		},
		HandleAuth: handler.Handle,
		Business:   []infraauth.Binding{{Subject: memberSubject, Handle: respond}, {Subject: agentSubject, Handle: respond}},
		Member:     infraauth.Identity{Name: "tether-cli:attach", PublicKey: member, Sign: f.keys[1].Sign, RPCSubject: memberSubject, RPCResponse: []byte(`{"ok":true}`)},
		Agent:      infraauth.Identity{Name: "tether-agent:attach:node", PublicKey: agent, Sign: f.keys[2].Sign, RPCSubject: agentSubject, RPCResponse: []byte(`{"ok":true}`)}})
	if err != nil {
		t.Fatal(err)
	}
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.private = private
	registry, err := ingressmove.New(recordingWitness{}, func(_ c.Plan, p c.RouteProof, _ c.Deadline) error {
		return inframaint.VerifyProof(p, c.ID{4}, "fixture", pub)
	})
	if err != nil {
		t.Fatal(err)
	}
	f.view = inframaint.View{HomeDigest: c.Digest{2}, AuthorityRevision: 1, PolicyRevision: 1, Known: true, Healthy: true, Registered: true}
	f.now.Store(1_000_000_000)
	f.config = Config{Busy: func() bool { return false }, Registry: registry, Role: "control", Domain: c.ID{4}, KeyID: "fixture", Key: pub,
		Clock:          func() (string, int64, error) { return "fixture", f.now.Load(), nil },
		View:           func() inframaint.View { f.mu.Lock(); defer f.mu.Unlock(); return f.view },
		Authentication: func(_ string, d c.Deadline, stop c.Stop) (infraauth.Receipt, error) { return f.owner.Open(d, nil) },
		Recovery: func(r infraauth.Receipt, d c.Deadline, stop c.Stop) error {
			f.checks.Add(1)
			if !authReady(r) {
				return c.ErrIncomplete
			}
			if f.block.Load() {
				f.enterOnce.Do(func() { close(f.entered) })
				select {
				case <-f.release:
				case <-stop:
					return errors.New("recovery canceled")
				}
			}
			_, e := controlbudget.Remaining(d)
			return e
		},
		Rebind: func(nc *nats.Conn, d c.Deadline) error {
			remaining, e := controlbudget.Remaining(d)
			if e != nil {
				return e
			}
			_, e = nc.Request(memberSubject, nil, remaining)
			return e
		},
		Observe: func(nc *nats.Conn, raw net.Conn, participant c.ID, generation uint64, _ c.Deadline) (c.Attachment, error) {
			cid, e := nc.GetClientID()
			a := c.Attachment{Participant: participant, Generation: generation, CID: cid, URL: nc.ConnectedUrl(), ServerID: nc.ConnectedServerId(), HealthGeneration: generation, Ingress: attachProcess(),
				Raw: c.Socket{NetNS: 1, Device: 1, Inode: generation, Cookie: generation, Local: raw.LocalAddr().String(), Remote: raw.RemoteAddr().String(), Owner: attachProcess()}}
			f.mu.Lock()
			f.view.Attachment = a
			f.mu.Unlock()
			return a, e
		}}
	return f
}
func (f *attachFixture) connect(t *testing.T) {
	t.Helper()
	member, _ := f.keys[1].PublicKey()
	d := attachDeadline(t)
	client, options, err := Prepare([]nats.Option{nats.Nkey(member, f.keys[1].Sign), nats.Name("tether-cli:attach"), nats.MaxReconnects(-1), nats.ReconnectWait(10 * time.Millisecond), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {})}, d, f.config)
	if err != nil {
		t.Fatal(err)
	}
	f.client = client
	f.nc, _, err = natsinbox.ConnectWithin(f.server.ClientURL(), member, options, d, nil)
	if err == nil {
		err = Finish(f.nc, d)
	}
	if err != nil {
		t.Fatal(err)
	}
}
func (f *attachFixture) close(t *testing.T) {
	t.Helper()
	f.block.Store(false)
	select {
	case <-f.release:
	default:
		close(f.release)
	}
	if f.client != nil {
		f.client.Release()
		if err := f.client.Close(attachDeadline(t)); err != nil {
			t.Error(err)
		}
	}
	if f.owner != nil {
		if err := f.owner.Close(attachDeadline(t)); err != nil {
			t.Error(err)
		}
	}
	f.server.Shutdown()
	f.server.WaitForShutdown()
	_ = f.db.Close()
	for _, key := range f.keys {
		key.Wipe()
	}
}

// origin: hot-update S9 plan §4.4 / P23 / M25; rv-business F2 / rv-infra F10
func TestClientReconnectWaitsForOrdinaryAuthAndACKPrefix(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	old, err := f.client.Observe(attachDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := f.nc.SubscribeSync(f.nc.NewInbox())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	f.block.Store(true)
	if err = f.nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect bypassed recovery")
	}
	connections, err := f.server.Connz(&ns.ConnzOptions{Subscriptions: true, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, conn := range connections.Conns {
		for _, subject := range conn.Subs {
			if subject == sub.Subject {
				t.Fatal("SDK replayed business SUB before normal auth and ACK-prefix gate")
			}
		}
	}
	close(f.release)
	deadline := time.Now().Add(3 * time.Second)
	for !f.nc.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err = f.nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// SDK reconnect callbacks are asynchronous; observe the registry commit.
	for time.Now().Before(deadline) {
		f.client.mu.Lock()
		ready := f.client.registered && f.client.registeredGeneration > old.Generation
		f.client.mu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	newAttachment, err := f.client.Observe(attachDeadline(t))
	if err != nil || newAttachment.CID == old.CID || newAttachment.Generation <= old.Generation || For(f.nc) != f.client || f.checks.Load() != 2 {
		t.Fatalf("reconnect lost original invocation or generation: %v", err)
	}
	if _, err = f.config.Registry.Seal(c.Plan{Tx: c.ID{5}, Domain: c.ID{4}, Step: 1, Component: c.NATS, Before: attachProcess(), PolicyRevision: 1}, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	// The test exercised a real seal. Release its exact checkpoint, so the
	// fixture cannot hide retained registry responsibility during Close.
	cp, err := f.config.Registry.Seal(c.Plan{Tx: c.ID{5}, Domain: c.ID{4}, Step: 1, Component: c.NATS, Before: attachProcess(), PolicyRevision: 1}, attachDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.config.Registry.Release(cp, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// Direct Connection tests do not run the orchestrator's Bind commit. Refresh
// its real registry explicitly for teardown; a stale participant must not be
// silently discarded merely because the local raw owner has moved.
func refreshFixtureRegistration(t *testing.T, f *attachFixture, old uint64) {
	t.Helper()
	d := attachDeadline(t)
	if err := f.config.Registry.Remove(f.client.id, old, d); err != nil {
		t.Fatal(err)
	}
	a, err := f.config.Registry.Admit(f.client.id, "control", d)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.config.Registry.Register(a, f.client, d); err != nil {
		t.Fatal(err)
	}
}

func prepareProof(t *testing.T, f *attachFixture) c.RouteProof {
	t.Helper()
	old, err := f.client.Observe(attachDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := f.client.Challenge(c.ID{9}, c.RouteOutbound)
	if err != nil {
		t.Fatal(err)
	}
	p := c.RouteProof{Purpose: c.RoutePrepare, Direction: c.RouteOutbound, Target: c.RouteTarget{URL: old.URL, NodeID: "node", Component: c.NATS, Ingress: old.Ingress}, Version: 1, Tx: challenge.Tx, Participant: old.Participant, Old: old,
		HomeDigest: f.view.HomeDigest, AuthorityRevision: 1, PolicyRevision: 1, RemainingNS: uint64(8 * time.Second), Nonce: challenge.Nonce, KeyID: "fixture"}
	p, err = inframaint.SignProof(p, c.ID{4}, func(data []byte) ([]byte, error) { return ed25519.Sign(f.private, data), nil })
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// origin: hot-update S9 plan §4.4–§4.6 / P23 / M25
func TestClientRefusesIncompleteCapabilityAndAuthReceipts(t *testing.T) {
	if _, _, err := Prepare(nil, attachDeadline(t), Config{Authentication: func(string, c.Deadline, c.Stop) (infraauth.Receipt, error) { return infraauth.Receipt{}, nil }}); !errors.Is(err, c.ErrUnsupported) {
		t.Fatal("partial composition installed a capability")
	}
	f := newAttachFixture(t)
	defer f.close(t)
	r, err := f.owner.Open(attachDeadline(t), nil)
	if err != nil || !authReady(r) {
		t.Fatalf("ordinary receipt unavailable: %v", err)
	}
	for _, corrupt := range []func(*infraauth.Receipt){
		func(r *infraauth.Receipt) { r.AuthSUBs = nil }, func(r *infraauth.Receipt) { r.Generation++ },
		func(r *infraauth.Receipt) { r.Probes[0].CID = 0 }, func(r *infraauth.Receipt) { r.Probes[1].ServerID = "peer" },
		func(r *infraauth.Receipt) { r.AuthUsable = r.BusinessPONG }, func(r *infraauth.Receipt) { r.Probes[0].RequestDigest = c.Digest{} },
	} {
		altered := r
		corrupt(&altered)
		if authReady(altered) {
			t.Fatal("malformed ordinary auth receipt granted business admission")
		}
	}
}

// Every NC obtains a distinct pre-dial token and physical receipt. Sharing the
// control owner's registry is essential; sharing its participant/CID is invalid.
// origin: hot-update S9 plan §4.5 / P23 / M25
func TestClientRegistersControlAndIndependentData(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	member, _ := f.keys[1].PublicKey()
	config := f.config
	config.Role = "data"
	d := attachDeadline(t)
	data, opts, err := Prepare([]nats.Option{nats.Nkey(member, f.keys[1].Sign), nats.Name("tether-cli:attach")}, d, config)
	if err != nil {
		t.Fatal(err)
	}
	nc, _, err := natsinbox.ConnectWithin(f.server.ClientURL(), member, opts, d, nil)
	if err == nil {
		err = Finish(nc, d)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := data.Close(attachDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	plan := c.Plan{Tx: c.ID{5}, Domain: c.ID{4}, Step: 1, Component: c.NATS, Before: attachProcess(), PolicyRevision: 1}
	cp, err := config.Registry.Seal(plan, d)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := config.Registry.Snapshot(cp, d)
	if err != nil || len(rows) != 2 {
		t.Fatalf("complete cohort omitted ctl or independent data NC: rows=%d err=%v", len(rows), err)
	}
	if rows[0].Participant == rows[1].Participant || rows[0].CID == rows[1].CID || rows[0].Raw.Local == rows[1].Raw.Local {
		t.Fatal("data attachment inherited control connection identity")
	}
	if err := config.Registry.Release(cp, d); err != nil {
		t.Fatal(err)
	}
}

// Close must cancel the raw worker before entering nc.mu. The reconnect holds
// that mutex while its actual pre-dial gate waits on P14's cancellation channel.
// origin: hot-update S9 plan §4.5 / P23 / M25
func TestClientCloseInterruptsReconnectGateBeforeNATSMutex(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	f.block.Store(true)
	if err := f.nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect did not enter recovery")
	}
	closed := make(chan error, 1)
	go func() { closed <- f.client.Close(attachDeadline(t)) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		close(f.release)
		<-closed
		t.Fatal("Close waited on nc.mu before interrupting and joining raw admission")
	}
	if !f.nc.IsClosed() {
		t.Fatal("joined raw retirement left the original NC alive")
	}
}

// A canceled planned request cannot abandon a gate worker or leave a late
// SDK reconnect free to replay subscriptions onto its retiring generation.
// origin: hot-update S9 plan §4.5 / P23 / M25
func TestClientPlannedCancellationJoinsRawAdmission(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	p := prepareProof(t, f)
	if _, err := f.client.Install(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	f.block.Store(true)
	stop := make(chan struct{})
	d := attachDeadline(t)
	moved := make(chan error, 1)
	go func() { _, err := f.client.Attach(p.Target, d, stop); moved <- err }()
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("planned move did not enter recovery")
	}
	close(stop)
	select {
	case err := <-moved:
		if err == nil {
			t.Fatal("canceled move claimed attachment success")
		}
	case <-time.After(500 * time.Millisecond):
		close(f.release)
		<-moved
		t.Fatal("planned cancellation failed to join raw admission")
	}
	if !f.nc.IsClosed() {
		t.Fatal("canceled planned move returned before the SDK worker retired")
	}
	if _, err := f.client.Dial("tcp", f.server.Addr().String()); !errors.Is(err, context.Canceled) {
		t.Fatal("late dial resurrected canceled generation")
	}
}

// Pool edits and observer labels cannot replace this NC's actual raw facts.
// origin: hot-update S9 plan §2.3 / §4.5 / P23 / M25
func TestClientRejectsContradictoryAttachmentObservation(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	observe := f.client.config.Observe
	for _, alter := range []func(*c.Attachment){
		func(a *c.Attachment) { a.Participant = c.ID{99} }, func(a *c.Attachment) { a.Generation++ }, func(a *c.Attachment) { a.CID++ },
		func(a *c.Attachment) { a.ServerID = "peer" }, func(a *c.Attachment) { a.URL = "nats://127.0.0.1:1" },
		func(a *c.Attachment) { a.Raw.Local = "127.0.0.1:1" }, func(a *c.Attachment) { a.Raw.Remote = "127.0.0.1:1" },
	} {
		f.client.config.Observe = func(nc *nats.Conn, raw net.Conn, participant c.ID, generation uint64, d c.Deadline) (c.Attachment, error) {
			a, err := observe(nc, raw, participant, generation, d)
			alter(&a)
			return a, err
		}
		if _, err := f.client.Observe(attachDeadline(t)); !errors.Is(err, c.ErrIncomplete) {
			t.Fatal("observer label overrode actual client socket")
		}
	}
	f.client.config.Observe = observe
}

// Closing a carrier must join the normal REGISTER borrower as well as raw IO.
// origin: hot-update S9 plan §4.5 / P23 / M25
func TestClientCloseJoinsNormalRegistration(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	rebind := f.client.config.Rebind
	f.client.config.Rebind = func(nc *nats.Conn, d c.Deadline) error {
		once.Do(func() { close(entered) })
		remaining, _ := controlbudget.Remaining(d)
		select {
		case <-release:
		case <-time.After(remaining):
			return controlbudget.ErrExpired
		}
		return rebind(nc, d)
	}
	if err := f.nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("normal registration did not start")
	}
	closed := make(chan error, 1)
	d := attachDeadline(t)
	go func() { closed <- f.client.Close(d) }()
	select {
	case <-closed:
		close(release)
		t.Fatal("Close retired authority before normal REGISTER borrower joined")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("normal registration borrower failed to join")
	}
}

// origin: hot-update S9 plan §9.5 / P23
func TestInfraAttachmentLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAttachment(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra attachment x5", beforeG)
	testharness.AssertNoFDLeak(t, "infra attachment x5", beforeFD, 0)
}
func exerciseAttachment(t *testing.T) {
	t.Helper()
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	if !f.client.Installed() {
		t.Fatal("registered normal connection did not install composition")
	}
	p := prepareProof(t, f)
	if _, err := f.client.Install(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Attach(p.Target, attachDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	refreshFixtureRegistration(t, f, p.Old.Generation)
	f.client.Release()
}
