// proxy_reconnect_e2e_test.go — full-stack tests for the proxy-tunnel-reconnect
// leaf increment (docs/reviews/proxy-tunnel-reconnect-plan.md). Unlike
// TestProxySubscriptionE2E (which uses a recordingAdapter), these wire the REAL
// TunnelExposeAdapter to the broker's REAL reverse-tunnel server and drive an
// actual data-plane drop via a controllable TCP relay between agent and broker.
package p13_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// severableRelay is a dumb bidirectional TCP relay (agent → relay → broker
// tunnel control port). sever() closes every active conn pair, forcing the
// agent's yamux session to drop exactly like a residential-link blip — WITHOUT
// touching NATS. The relay keeps accepting, so the agent's reconnect dial lands
// a fresh pipe to the broker.
type severableRelay struct {
	ln        net.Listener
	dst       string
	mu        sync.Mutex
	conns     []net.Conn
	closeOnce sync.Once
}

func newSeverableRelay(t *testing.T, dst string) *severableRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &severableRelay{ln: ln, dst: dst}
	t.Cleanup(r.close)
	go r.acceptLoop()
	return r
}

func (r *severableRelay) addr() string { return r.ln.Addr().String() }

func (r *severableRelay) acceptLoop() {
	for {
		c, err := r.ln.Accept()
		if err != nil {
			return
		}
		go r.pipe(c)
	}
}

func (r *severableRelay) pipe(c net.Conn) {
	d, err := net.Dial("tcp", r.dst)
	if err != nil {
		_ = c.Close()
		return
	}
	r.mu.Lock()
	r.conns = append(r.conns, c, d)
	r.mu.Unlock()
	go func() { _, _ = io.Copy(d, c); _ = d.Close() }()
	_, _ = io.Copy(c, d)
	_ = c.Close()
}

func (r *severableRelay) sever() {
	r.mu.Lock()
	cs := r.conns
	r.conns = nil
	r.mu.Unlock()
	for _, c := range cs {
		_ = c.Close()
	}
}

// close stops accepting (a durable outage — the agent's reconnect dial now gets
// connection-refused) and severs active conns. Idempotent (also the t.Cleanup).
func (r *severableRelay) close() {
	r.closeOnce.Do(func() { _ = r.ln.Close() })
	r.sever()
}

func allocatedProxyPort(t *testing.T, db *sql.DB, sid, nid string) string {
	t.Helper()
	var p int
	if err := db.QueryRow(
		`SELECT port FROM port_allocations WHERE sid=? AND nid=? AND name='__proxy__' AND state='ALLOCATED'`,
		sid, nid).Scan(&p); err != nil {
		t.Fatalf("no ALLOCATED __proxy__ port for %s/%s: %v", sid, nid, err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(p))
}

func waitTCP(t *testing.T, addr string, want bool, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		got := err == nil
		if got {
			_ = c.Close()
		}
		if got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("addr %s connectable==%v not reached within %v", addr, want, max)
}

// assertTCPStays asserts addr keeps a fixed connectability for the whole window
// (used to prove a disabled exit is NOT resurrected by reconnect).
func assertTCPStays(t *testing.T, addr string, want bool, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
		got := err == nil
		if got {
			_ = c.Close()
		}
		if got != want {
			t.Fatalf("addr %s connectable flipped to %v during the no-resurrect window", addr, got)
		}
		time.Sleep(80 * time.Millisecond)
	}
}

func nodeStatus(db *sql.DB, sid, nid string) string {
	var s string
	_ = db.QueryRow(`SELECT status FROM nodes WHERE sid=? AND nid=?`, sid, nid).Scan(&s)
	return s
}

// fullStackProxy spins up broker (with real tunnel server) + agent (with the
// real TunnelExposeAdapter) behind a severable relay, flips proxy ON, and waits
// until the agent is serving (proxy_ready=1 + public port bound). Returns the
// pieces the tests drive.
type fullStackProxy struct {
	db       *sql.DB
	nc       *nats.Conn
	ownerPub string
	relay    *severableRelay
	pubAddr  string
}

func startFullStackProxy(t *testing.T) *fullStackProxy {
	t.Helper()
	url := testharness.StartNATS(t)
	db := testharness.OpenDB(t)
	ownerPub, ownerFP := testharness.FreshUserPub(t)
	if _, err := session.Create(db, "lab", "lab", ownerFP, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	btun := freePort(t)
	b, err := broker.New(broker.Config{
		NATSURL: url, DB: db, Logger: logger,
		ReconcileInterval: 50 * time.Millisecond, StaleAfter: 300 * time.Millisecond, OfflineAfter: 2 * time.Second,
		PublicHost: "broker.test", PortBandLow: 14000, PortBandHigh: 14010,
		SubHTTPAddr: freePort(t), TunnelControlAddr: btun, TunnelPublicHost: "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	bctx, bcancel := context.WithCancel(context.Background())
	t.Cleanup(bcancel)
	go func() { _ = b.Run(bctx) }()
	testharness.WaitConnect(t, url, 3*time.Second)
	time.Sleep(100 * time.Millisecond)

	relay := newSeverableRelay(t, btun)
	logger2 := slog.New(slog.NewTextHandler(io.Discard, nil))
	adapter := agent.NewTunnelExposeAdapter(relay.addr(), "lab", "lab-1", logger2)
	actx, acancel := context.WithCancel(context.Background())
	adapter.Start(actx)
	ag, err := agent.New(agent.Config{
		NATSURL: url, SID: "lab", NID: "lab-1", Home: t.TempDir(),
		ExposeAdapter: adapter, Logger: logger2,
		HeartbeatInterval: 100 * time.Millisecond, RegisterTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	adone := make(chan struct{})
	go func() { _ = ag.Run(actx); close(adone) }()
	t.Cleanup(func() {
		acancel()
		select {
		case <-adone:
		case <-time.After(3 * time.Second):
			t.Error("agent did not exit")
		}
	})
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	var setR proto.ProxySetResp
	proxyReq(t, nc, proto.SubjCtrlProxySet(ownerPub, "lab"), proto.ProxySetReq{Enabled: true}, &setR)
	if !setR.OK {
		t.Fatalf("proxy on: %+v", setR)
	}
	if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
		return proxyReady(db, "lab", "lab-1")
	}) {
		t.Fatal("agent never ACKed proxy readiness through the real tunnel")
	}
	pubAddr := allocatedProxyPort(t, db, "lab", "lab-1")
	waitTCP(t, pubAddr, true, 5*time.Second) // broker bound the public port
	return &fullStackProxy{db: db, nc: nc, ownerPub: ownerPub, relay: relay, pubAddr: pubAddr}
}

// TestProxyFalseOnlineRecoversAfterTunnelDrop is the headline regression: a
// data-plane drop with NATS untouched must (1) flip the node unready + unbind
// the public port (no more false-online), then (2) SELF-HEAL via the agent's
// tunnel reconnect — proxy_ready→1 and the port re-binds — with the control
// plane never reconnecting (node stays ONLINE throughout = NATS-independent).
func TestProxyFalseOnlineRecoversAfterTunnelDrop(t *testing.T) {
	f := startFullStackProxy(t)

	f.relay.sever() // data-plane blip; NATS untouched

	// (1) the dead exit is dropped from /sub, not advertised: proxy_ready clears.
	// (This is the reliable Defect-B signal; the public-port unbind is corroborating
	// but transient — the agent self-heals within one backoff, so we don't race it.)
	// 15s (was 5s): under the full make e2e-parallel matrix this -race subprocess runs after every
	// other heavy matrix, so tunnel-drop detection + /sub re-advertise is slower; the assertion
	// is "eventually clears" (it clears in ~0.5s in isolation), so the extra headroom only
	// prevents a load-induced false Defect-B regression.
	if !testharness.WaitFor(t, 15*time.Second, 20*time.Millisecond, func() bool {
		return !proxyReady(f.db, "lab", "lab-1")
	}) {
		t.Fatal("proxy_ready did not clear on tunnel drop (Defect B: false-online persists)")
	}

	// (2) self-heal via tunnel reconnect (NOT NATS / NOT process restart).
	//
	// 15s, matched to step (1) above (testing-standards §T1). Step (1) was raised 5s→15s for load and
	// step (2) was left at 10s, but step (2) waits on strictly MORE work: the agent's reconnect
	// backoff, a fresh tunnel handshake, a REGISTER round-trip, and the broker's proxy_ready write —
	// where step (1) only waits for drop DETECTION. Sizing the cheaper wait for a loaded box and the
	// more expensive one for an idle box is backwards.
	//
	// HONEST ATTRIBUTION: this was prompted by ONE `make test` failure of this test that did not
	// reproduce in a subsequent clean `make test`, 12 focused iterations, 8 whole-package iterations,
	// or a full `./test/...` run — the last three deliberately under a concurrent `make e2e-parallel`.
	// So the failing step is NOT confirmed and this is not a proven fix; it removes the one real
	// §T1 asymmetry visible in the file. It cannot mask a product defect: the assertion is
	// "eventually recovers", so a self-heal that never happens still fails, just later.
	if !testharness.WaitFor(t, 15*time.Second, 50*time.Millisecond, func() bool {
		return proxyReady(f.db, "lab", "lab-1")
	}) {
		t.Fatal("proxy_ready never recovered — tunnel reconnect did not self-heal")
	}
	waitTCP(t, f.pubAddr, true, 15*time.Second)

	// Negative control: the control plane was never disturbed — the node stayed
	// ONLINE the whole time (recovery came from the data-plane reconnect alone).
	if s := nodeStatus(f.db, "lab", "lab-1"); s != "ONLINE" {
		t.Fatalf("node status=%q want ONLINE (NATS should have been untouched)", s)
	}
}

// TestProxyDisableDuringTunnelDropStaysDown is the security invariant: if the
// owner disables the proxy while the data plane is dropped, the agent's blind
// reconnect must NOT resurrect the exit — the public port stays refused and the
// node stays unready, forever (no rebind).
func TestProxyDisableDuringTunnelDropStaysDown(t *testing.T) {
	f := startFullStackProxy(t)

	// Durable data-plane outage: close the relay so the agent's reconnect dial is
	// refused (it cannot self-heal). This makes the disable-vs-reconnect outcome
	// deterministic instead of racing the agent's ~500ms self-heal.
	f.relay.close()

	// Owner disables while the tunnel is down: disableProxy frees the port row
	// (CloseSession + port.Free) and pushes a disable directive over NATS, so the
	// reconnecting agent's next REGISTER hits a terminal DENY and/or it tears down
	// on the directive. Either way the exit must stay down — a blind reconnect must
	// never resurrect a disabled exit.
	var setR proto.ProxySetResp
	proxyReq(t, f.nc, proto.SubjCtrlProxySet(f.ownerPub, "lab"), proto.ProxySetReq{Enabled: false}, &setR)
	if !setR.OK || setR.Enabled {
		t.Fatalf("proxy off: %+v", setR)
	}

	// proxy_ready must settle to 0 and the public port must NEVER come back.
	if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
		return !proxyReady(f.db, "lab", "lab-1")
	}) {
		t.Fatal("proxy_ready did not clear after disable-during-drop")
	}
	// No resurrection: the port stays refused across a sustained window that
	// comfortably exceeds the agent's reconnect backoff.
	assertTCPStays(t, f.pubAddr, false, 3*time.Second)
}
