package infraauth

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type wireEvent struct {
	Connection int
	Direction  string
	Kind       string
	Subject    string
	Queue      string
	SID        string
	BootNS     int64
}

type authFixture struct {
	t              *testing.T
	server         *natsserver.Server
	db             *sql.DB
	keys           []nkeys.KeyPair
	config         Config
	owner          *Owner
	mu             sync.Mutex
	events         []wireEvent
	transports     []*testTransport
	privateReplies int
	closeOnce      sync.Once
}

func newAuthFixture(t *testing.T) *authFixture {
	t.Helper()
	f := &authFixture{t: t}
	for range 4 {
		key, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		f.keys = append(f.keys, key)
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	f.keys = append(f.keys, account)
	f.db, err = storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	brokerPublic, _ := f.keys[0].PublicKey()
	memberPublic, _ := f.keys[1].PublicKey()
	agentPublic, _ := f.keys[2].PublicKey()
	accountPublic, _ := account.PublicKey()
	memberFP, _ := auth.FingerprintFromActor(memberPublic)
	agentFP, _ := auth.FingerprintFromActor(agentPublic)
	if _, err := session.Create(f.db, "authgate", "authgate", memberFP, "unused", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(f.db, "authgate", "node", agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	opts := natstest.DefaultTestOptions
	opts.Host, opts.Port, opts.AlwaysEnableNonce = "127.0.0.1", -1, true
	opts.AuthTimeout = 0.15
	opts.Cluster = natsserver.ClusterOpts{Host: "127.0.0.1", Port: -1, Name: "authgate"}
	opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPublic}}
	opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPublic, Account: "$G", AuthUsers: []string{brokerPublic}}
	f.server = natstest.RunServer(&opts)
	handler := &authcallout.Handler{DB: f.db, AccountKp: account, Now: time.Now, JWTTTL: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	memberSubject := proto.SubjCtrlNodeList(memberPublic, "authgate")
	agentSubject := proto.SubjNodeRegister("authgate", "node")
	// These are ordinary application subjects with real NATS private-inbox
	// request/reply, not auxiliary auth responders. Full broker assembly is
	// owned by P22/P30; this leaf fixture exercises its binding descriptor.
	respond := func(msg *nats.Msg) {
		if !strings.HasPrefix(msg.Reply, auth.InboxRoot+".") {
			t.Error("ordinary RPC used a shared inbox")
			return
		}
		f.mu.Lock()
		f.privateReplies++
		f.mu.Unlock()
		if err := msg.Respond([]byte(`{"ok":true}`)); err != nil {
			t.Error(err)
		}
	}
	f.config = Config{
		URL: f.server.ClientURL(), AccountPublicKey: accountPublic,
		Options: func() []nats.Option { return []nats.Option{nats.Nkey(brokerPublic, f.keys[0].Sign)} }, Transport: f.transport,
		HandleAuth: handler.Handle,
		Authority:  []Binding{{Subject: proto.SubjClusterApply("pin-verify"), Handle: respond}},
		Business:   []Binding{{Subject: memberSubject, Queue: "tether-ctl", Handle: respond}, {Subject: agentSubject, Handle: respond}},
		Member:     Identity{Name: "tether-cli:authgate", PublicKey: memberPublic, Sign: f.keys[1].Sign, RPCSubject: memberSubject, RPCRequest: []byte(`{}`), RPCResponse: []byte(`{"ok":true}`)},
		Agent:      Identity{Name: "tether-agent:authgate:node", PublicKey: agentPublic, Sign: f.keys[2].Sign, RPCSubject: agentSubject, RPCRequest: []byte(`{}`), RPCResponse: []byte(`{"ok":true}`)},
	}
	t.Cleanup(f.close)
	return f
}

func (f *authFixture) newOwner() *Owner {
	f.t.Helper()
	owner, err := New(f.config)
	if err != nil {
		f.t.Fatal(err)
	}
	f.owner = owner
	return owner
}

func (f *authFixture) close() {
	f.closeOnce.Do(func() {
		if f.owner != nil {
			if err := f.owner.Close(testDeadline(f.t, 3*time.Second)); err != nil {
				f.t.Error(err)
			}
		}
		f.server.Shutdown()
		f.server.WaitForShutdown()
		_ = f.db.Close()
		for _, key := range f.keys {
			key.Wipe()
		}
	})
}

func testDeadline(t *testing.T, duration time.Duration) infracontract.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

func (f *authFixture) transport(deadline infracontract.Deadline, _ infracontract.Stop) (Transport, error) {
	// ctx-none: the transport factory's frozen leaf signature has no context.
	ctx, cancel := context.WithCancel(context.Background())
	f.mu.Lock()
	transport := &testTransport{fixture: f, id: len(f.transports), ctx: ctx, cancel: cancel, deadline: deadline}
	f.transports = append(f.transports, transport)
	f.mu.Unlock()
	return transport, nil
}

// This fixture has no detached dial workers: net.Dialer runs synchronously on
// the caller. It owns the real raw TCP sockets and their cancellation. P14's
// DNS/proxy/TLS/WSS worker implementation must be wired by P22 before that
// integration can be signed; this is not a replacement production transport.
type testTransport struct {
	fixture     *authFixture
	id          int
	ctx         context.Context
	cancel      context.CancelFunc
	deadline    infracontract.Deadline
	mu          sync.Mutex
	connections []net.Conn
	active      int
	canceled    bool
}

func (d *testTransport) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	if d.canceled {
		d.mu.Unlock()
		return nil, ErrFenced
	}
	d.active++
	d.mu.Unlock()
	defer func() { d.mu.Lock(); d.active--; d.mu.Unlock() }()
	remaining, err := controlbudget.Remaining(d.deadline)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: remaining}
	nc, err := dialer.DialContext(d.ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.canceled {
		_ = nc.Close()
		return nil, ErrFenced
	}
	d.connections = append(d.connections, nc)
	return &traceConnection{Conn: nc, transport: d}, nil
}

func (d *testTransport) Cancel() {
	d.cancel()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.canceled = true
	for _, nc := range d.connections {
		_ = nc.Close()
	}
}

func (d *testTransport) Join(infracontract.Deadline) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.canceled || d.active != 0 {
		return errors.New("raw dial did not retire")
	}
	return nil
}

type traceConnection struct {
	net.Conn
	transport   *testTransport
	mu          sync.Mutex
	readBuffer  string
	writeBuffer string
}

func (c *traceConnection) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	c.record("read", data[:n])
	return n, err
}

func (c *traceConnection) Write(data []byte) (int, error) {
	n, err := c.Conn.Write(data)
	c.record("write", data[:n])
	return n, err
}

func (c *traceConnection) record(direction string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	buffer := &c.readBuffer
	if direction == "write" {
		buffer = &c.writeBuffer
	}
	*buffer += string(data)
	for {
		line, tail, complete := strings.Cut(*buffer, "\r\n")
		if !complete {
			return
		}
		*buffer = tail
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		event := wireEvent{Connection: c.transport.id, Direction: direction}
		switch fields[0] {
		case "PING", "PONG", "CONNECT":
			event.Kind = fields[0]
		case "SUB":
			if len(fields) < 3 {
				continue
			}
			event.Kind, event.Subject, event.SID = fields[0], fields[1], fields[len(fields)-1]
			if len(fields) == 4 {
				event.Queue = fields[2]
			}
		case "UNSUB":
			if len(fields) < 2 {
				continue
			}
			event.Kind, event.SID = fields[0], fields[1]
		case "PUB":
			if len(fields) < 3 {
				continue
			}
			event.Kind, event.Subject = fields[0], fields[1]
		default:
			continue
		}
		event.BootNS, _ = controlbudget.Now()
		f := c.transport.fixture
		f.mu.Lock()
		f.events = append(f.events, event)
		f.mu.Unlock()
	}
}

func (f *authFixture) wire() []wireEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wireEvent(nil), f.events...)
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("handshake did not arrive")
	}
}

func releaseOnCleanup(t *testing.T, signal chan struct{}) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(signal) }) }
	t.Cleanup(release)
	return release
}

func requireNoBusiness(t *testing.T, f *authFixture, owner *Owner) {
	t.Helper()
	if nc, _, err := owner.Business(); nc != nil || !errors.Is(err, ErrFenced) {
		t.Fatal("business admission opened before auth/recovery")
	}
	for _, event := range f.wire() {
		if event.Kind == "CONNECT" && event.Connection >= 3 {
			t.Fatal("business CONNECT preceded auth/recovery")
		}
		for _, binding := range f.config.Business {
			if event.Kind == "SUB" && event.Subject == binding.Subject {
				t.Fatal("business SUB preceded auth/recovery")
			}
		}
	}
}

func requireOrder(t *testing.T, f *authFixture, receipt Receipt, offset int) {
	t.Helper()
	if receipt.Phase != ControlReady || receipt.Generation == 0 || receipt.AuthCID == 0 || receipt.BusinessCID == 0 || receipt.AuthCID == receipt.BusinessCID || receipt.ServerID != f.server.ID() {
		t.Fatalf("invalid ready identity: %+v", receipt)
	}
	// Required: 0 < AuthPONG < AuthUsable < BusinessPONG < ControlReady; any
	// link out of order fails.
	if receipt.AuthPONG <= 0 || receipt.AuthUsable <= receipt.AuthPONG || receipt.BusinessPONG <= receipt.AuthUsable || receipt.ControlReady <= receipt.BusinessPONG {
		t.Fatalf("barrier order: %+v", receipt)
	}
	for _, proof := range receipt.Probes {
		if proof.Generation != receipt.Generation || proof.ServerID != receipt.ServerID || proof.CID == 0 || proof.RequestDigest == (infracontract.Digest{}) || proof.ReplyBootNS <= receipt.AuthPONG || proof.PONGBootNS <= proof.ReplyBootNS || proof.PONGBootNS >= receipt.AuthUsable || proof.RPCBootNS <= receipt.BusinessPONG || proof.RPCBootNS >= receipt.ControlReady {
			t.Fatalf("ordinary proof order: %+v", proof)
		}
	}
	queueSUB, authoritySUB, authPONG, businessSUB, businessPONG, rpcPUB := 0, 0, 0, 0, 0, 0
	// The only non-queue SUB allowed on the auth subject is one probe witness
	// per ordinary identity: armed after the normal queue's PONG, retired
	// (UNSUB on the wire) before auth_usable, and never a responder — the auth
	// NC sends exactly the two answers its normal queue gave the two probes.
	// origin: hot-update S9 P22 resume — design finding (callout queue selection)
	witnesses, retired, answers := map[string]int64{}, 0, 0
	events := f.wire()
	for _, event := range events {
		if event.Connection == offset && event.Kind == "UNSUB" {
			if armed, ok := witnesses[event.SID]; ok && armed > 0 && event.BootNS < receipt.AuthUsable {
				witnesses[event.SID] = -1
				retired++
			}
		}
		if event.Connection == offset && event.Kind == "PUB" && strings.HasPrefix(event.Subject, "$SYS._INBOX.") {
			answers++
		}
		if event.Connection < offset {
			continue
		}
		if event.Connection == offset && event.Kind == "SUB" && event.Subject == authSubject {
			switch {
			case event.Queue == authQueue && event.BootNS < receipt.AuthPONG:
				queueSUB++
			case event.Queue == "" && event.BootNS > receipt.AuthPONG && event.BootNS < receipt.AuthUsable && witnesses[event.SID] == 0:
				witnesses[event.SID] = event.BootNS
			default:
				t.Fatal("auxiliary auth responder on wire")
			}
		}
		if event.Connection == offset && event.Kind == "PONG" && event.Direction == "read" && event.BootNS <= receipt.AuthPONG {
			authPONG++
		}
		for _, binding := range f.config.Authority {
			if event.Kind == "SUB" && event.Subject == binding.Subject {
				if event.Connection != offset || event.BootNS >= receipt.AuthPONG {
					t.Fatal("authority SUB was not on the acknowledged auth-only connection")
				}
				authoritySUB++
			}
		}
		for _, binding := range f.config.Business {
			if event.Kind == "SUB" && event.Subject == binding.Subject {
				if event.BootNS <= receipt.AuthUsable || event.Connection != offset+3 {
					t.Fatal("business SUB escaped auth-only gate")
				}
				businessSUB++
			}
		}
		if event.Connection == offset+3 && event.Kind == "PONG" && event.Direction == "read" && event.BootNS <= receipt.BusinessPONG {
			businessPONG++
		}
		if (event.Connection == offset+1 || event.Connection == offset+2) && event.Kind == "PUB" && (event.Subject == f.config.Member.RPCSubject || event.Subject == f.config.Agent.RPCSubject) {
			rpcPUB++
		}
	}
	if queueSUB != 1 || authoritySUB != len(f.config.Authority) || authPONG == 0 || businessSUB != len(f.config.Business) || businessPONG == 0 || rpcPUB != 2 {
		t.Fatalf("wire evidence empty: queue=%d authPONG=%d businessSUB=%d businessPONG=%d RPC=%d", queueSUB, authPONG, businessSUB, businessPONG, rpcPUB)
	}
	if len(witnesses) != 2 || retired != 2 || answers != 2 {
		t.Fatalf("probe witness lifecycle: armed=%d retired-before-usable=%d auth-NC answers=%d (want 2/2/2)", len(witnesses), retired, answers)
	}
}

// origin: hot-update S9 plan §4.4 / T38
func TestOrdinaryReconnectAuthenticatesBeforeBusiness(t *testing.T) {
	f := newAuthFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseAuth := releaseOnCleanup(t, release)
	handle := f.config.HandleAuth
	var block sync.Once
	f.config.HandleAuth = func(request string) (string, error) {
		block.Do(func() { close(entered); <-release })
		return handle(request)
	}
	recoveryEntered, recoveryRelease := make(chan struct{}), make(chan struct{})
	releaseRecovery := releaseOnCleanup(t, recoveryRelease)
	var recoveryOnce sync.Once
	f.config.Recovery = func(_ uint64, _ infracontract.Deadline, stop infracontract.Stop) error {
		var err error
		recoveryOnce.Do(func() {
			close(recoveryEntered)
			select {
			case <-recoveryRelease:
			case <-stop:
				err = ErrFenced
			}
		})
		return err
	}
	owner := f.newOwner()
	type result struct {
		receipt Receipt
		err     error
	}
	done := make(chan result, 1)
	go func() { receipt, err := owner.Open(testDeadline(t, 3*time.Second), nil); done <- result{receipt, err} }()
	waitSignal(t, entered)
	requireNoBusiness(t, f, owner)
	releaseAuth()
	waitSignal(t, recoveryEntered)
	requireNoBusiness(t, f, owner)
	releaseRecovery()
	first := <-done
	if first.err != nil {
		t.Fatal(first.err)
	}
	requireOrder(t, f, first.receipt, 0)
	if err := f.server.DisconnectClientByID(first.receipt.AuthCID); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	old := owner.current
	owner.mu.Unlock()
	waitSignal(t, old.done)
	if _, _, err := owner.Business(); !errors.Is(err, ErrFenced) {
		t.Fatal("disconnect retained business admission")
	}
	// Reuse the same immutable descriptor on ordinary reconnect. The fixture
	// recovery callback also runs again; only its first handshake is blocked.
	second, err := owner.Open(testDeadline(t, 3*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireOrder(t, f, second, 4)
	if second.Generation != first.receipt.Generation+1 || second.AuthCID == first.receipt.AuthCID || second.BusinessCID == first.receipt.BusinessCID {
		t.Fatal("ordinary reconnect reused a stale generation")
	}
	if nc, _, err := owner.Business(); err != nil || nc.Opts.AllowReconnect || !nc.Opts.IgnoreDiscoveredServers {
		t.Fatal("business auto-replay or roaming enabled")
	}
}

// origin: hot-update S9 plan §4.4 / TestInfraAuthLifecycle
func TestInfraAuthLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAuth(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ordinary auth lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "ordinary auth lifecycle", beforeFD, 0)
}

func exerciseAuth(t *testing.T) {
	t.Helper()
	f := newAuthFixture(t)
	owner := f.newOwner()
	stop := make(chan struct{})
	receipt, err := owner.Open(testDeadline(t, 3*time.Second), stop)
	if err != nil {
		t.Fatal(err)
	}
	requireOrder(t, f, receipt, 0)
	close(stop)
	if err := owner.Close(testDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Model the allowed scheduling order where retire sees lost before fence
	// reaches its Cancel loop. The retirement worker must independently break
	// the actual raw connection before it enters nats.Close and transport.Join.
	transport, err := f.transport(testDeadline(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	brokerPublic, _ := f.keys[0].PublicKey()
	nc, err := nats.Connect(f.config.URL, nats.Nkey(brokerPublic, f.keys[0].Sign), nats.SetCustomDialer(transport), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	race := &generation{lost: make(chan struct{}), built: make(chan struct{}), done: make(chan struct{}), fenced: true, transports: []Transport{transport}, connections: []*nats.Conn{nc}}
	close(race.lost)
	close(race.built)
	go retire(race)
	if err := waitDone(race, testDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := owner.Business(); !errors.Is(err, ErrFenced) {
		t.Fatal("closed auth admitted business")
	}
	// Include a failure window in each exercise, then prove retirement before
	// shutting down the fixture (rather than counting fixture cleanup as join).
	if err := session.Tombstone(f.db, "authgate", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Open(testDeadline(t, time.Second), nil); err == nil {
		t.Fatal("deleted session authenticated")
	}
	if err := owner.Close(testDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		connections, err := f.server.Connz(&natsserver.ConnzOptions{})
		return err == nil && len(connections.Conns) == 0
	}) {
		t.Fatal("owner retained raw clients before fixture cleanup")
	}
	f.close()
}
