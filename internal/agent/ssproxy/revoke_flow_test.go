package ssproxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
)

type ssTarget struct {
	listener net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    []net.Conn
	bytes    atomic.Int64
	accepts  atomic.Int64
}

func newSSTarget(t *testing.T) *ssTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := &ssTarget{listener: ln}
	target.wg.Add(1)
	go func() {
		defer target.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			target.mu.Lock()
			target.conns = append(target.conns, c)
			target.mu.Unlock()
			target.accepts.Add(1)
			target.wg.Add(1)
			go func() {
				defer target.wg.Done()
				defer func() { _ = c.Close() }()
				b := make([]byte, 32768)
				for {
					n, err := c.Read(b)
					if n > 0 {
						target.bytes.Add(int64(n))
						if _, e := c.Write(b[:n]); e != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return target
}
func (s *ssTarget) close() {
	_ = s.listener.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
func awaitSS(t *testing.T, predicate func() bool, what string) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if predicate() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal(what)
}

// The kernel arm isolates the SS revoke owner using real duplex sockets and
// a recorded flow identity; the carrier arm uses the actual P1 tunnel owners.
// The latter intentionally fails at the P1 stub until that implementation lands.
// origin: hot-update S5 plan §6.5 / T21
func TestSSSilentRevokeClosesBoundFlow(t *testing.T) {
	t.Run("kernel", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseSSSilentRevoke(t)
		}
		testharness.AssertNoGoroutineLeak(t, "SS silent revoke", beforeG)
		testharness.AssertNoFDLeak(t, "SS silent revoke", beforeFD, 0)
	})
	t.Run("carrier", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			exerciseSSCarrierRevoke(t, false)
		}
	})
	// origin: hot-update S5 plan §4.1 / §9.1 / DRILL-RUN
	t.Run("legacy", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("Linux tuple reservation")
		}
		g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseSSCarrierRevoke(t, true)
		}
		testharness.AssertNoGoroutineLeak(t, "legacy SS tuple", g0)
		testharness.AssertNoFDLeak(t, "legacy SS tuple", fd0, 0)
	})
}
func exerciseSSSilentRevoke(t *testing.T) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	s := New(nil)
	port, err := s.Start(0, []Key{{KeyID: "alice", Secret: "alice"}, {KeyID: "bob", Secret: "bob"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	addr := net.JoinHostPort("127.0.0.1", itoa(port))
	alice, bob := dialSS(t, addr, "alice", target.listener.Addr().String()), dialSS(t, addr, "bob", target.listener.Addr().String())
	defer func() { _ = alice.conn.Close(); _ = bob.conn.Close() }()
	for _, c := range []*ssClient{alice, bob} {
		c.send(t, []byte("duplex"))
		if !bytes.Equal(c.recv(t, 6), []byte("duplex")) {
			t.Fatal("duplex prerequisite failed")
		}
	}
	var victim *ssActor
	s.mu.Lock()
	for _, a := range s.imageState.actors {
		if string(a.state.bytes("authorized_key_id")) == "alice" {
			victim = a
		}
	}
	s.mu.Unlock()
	if victim == nil {
		t.Fatal("no authenticated alice actor")
	}
	// The kernel arm deliberately starts at the revoke boundary; the carrier
	// arm below verifies the preceding ExpectFlow reservation/accept binding.
	id := [16]byte{71}
	s.mu.Lock()
	victim.state.putBytes("flow_id", id[:])
	upstream := victim.upstream.(*net.TCPConn)
	s.mu.Unlock()
	duplicate, _, err := descriptorFile(upstream)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = duplicate.Close() }()
	revoked := make(chan [16]byte, 1)
	if err := s.SetFlowRevoke(func(got [16]byte, reason string) {
		if reason != "revoked" {
			t.Error("untyped revoke reason")
		}
		revoked <- got
	}); err != nil {
		t.Fatal(err)
	}
	frozen, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	initial, err := frozen.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	if len(initial.Resources) != 3 {
		t.Fatal("frozen cut lacks both live actors and listener")
	}
	if err := s.SetKeys([]Key{{KeyID: "bob", Secret: "bob"}}); err != nil {
		t.Fatal(err)
	}
	late, err := frozen.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	sawRevoked := false
	for _, r := range late.Records {
		if r.Kind == "ss-actor" {
			p := wrapPrivateRecord(r)
			if stateID(p.bytes("flow_id")) == id {
				sawRevoked = p.flag("revoked")
			}
		}
	}
	if !sawRevoked {
		t.Fatal("late frozen revoke disappeared from the final cut")
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: []Key{{KeyID: "bob", Secret: "bob"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-revoked:
		if got != id {
			t.Fatal("wrong bound FlowID revoked")
		}
	default:
		t.Fatal("revoke omitted bound FlowID callback")
	}
	// Alice is silent and stays open in the fixture. Product retirement, not a
	// fixture Read/Close, must remove both SS connections and the actor owner.
	awaitSS(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.allConns) == 2 && len(s.imageState.actors) == 1
	}, "silent victim retained SS sockets or actor")
	raw, err := duplicate.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	var writeErr error
	if err := raw.Control(func(fd uintptr) { n, writeErr = syscall.Write(int(fd), []byte("forbidden")) }); err != nil {
		t.Fatal(err)
	}
	if n > 0 || writeErr == nil {
		t.Fatal("closing one wrapper left the prepared OFD duplicate writable")
	}
	bob.send(t, []byte("survivor"))
	if !bytes.Equal(bob.recv(t, 8), []byte("survivor")) {
		t.Fatal("bob was interrupted by alice revoke")
	}
	if target.accepts.Load() != 2 {
		t.Fatal("survivor redialed target")
	}
}

func exerciseSSCarrierRevoke(t *testing.T, legacy bool) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	s := New(nil)
	ssPort, err := s.Start(0, []Key{{KeyID: "alice", Secret: "alice"}, {KeyID: "bob", Secret: "bob"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	budget := &workload.ReplayBudget{Limit: 256 << 20}
	controlAddr := net.JoinHostPort("127.0.0.1", itoa(freeTCPPort(t)))
	server := tunnel.NewServer(controlAddr, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// This configuration refusal is a real dependency failure, never accepted
	// as success. Once P1 lands the rest exercises pubConn and replay retirement.
	cfg := tunnel.FlowConfig{Budget: budget, InstanceID: "ss-revoke", OwnerGen: 1, BootID: "ss-revoke-boot", MaxEndpoints: 128, MaxPendingFDs: 32,
		TokenLookup: func(tunnel.AuthRequest) error { return nil },
		Authorize: func(r tunnel.AuthRequest) (tunnel.Authority, error) {
			return tunnel.Authority{Enabled: true, Allocation: r.Allocation, Generation: 1, ProxyEpoch: 1, Keys: []tunnel.AuthorizedKey{{ID: "alice", Secret: []byte("alice")}, {ID: "bob", Secret: []byte("bob")}}}, nil
		}}
	if !legacy {
		if err := server.SetFlowConfig(cfg); err != nil {
			t.Fatalf("real flow owner required: %v", err)
		}
	}
	// ctx-root: isolated carrier fixture, never an SS lifetime parent.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := tunnel.NewClient(controlAddr, "ss-test", "ss-node", func(int) (int, error) { return ssPort, nil }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cfg.BeforeLocalConnect = func(id flow.ID, tuple rawsocket.Tuple) error {
		if id == (flow.ID{}) {
			return ExpectLegacyFlow(s, tuple)
		}
		return s.ExpectFlow([16]byte(id), tuple)
	}
	if err := client.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFlowRevoke(func(id [16]byte, _ string) {
		if err := client.RevokeFlow(flow.ID(id), flow.ReasonRevoked); err != nil {
			t.Error(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	client.Start(ctx)
	port := freeTCPPort(t)
	defer client.Close(port)
	if err := client.Open(port, ssPort, "token"); err != nil {
		t.Fatal(err)
	}
	alice := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "alice", target.listener.Addr().String())
	defer func() { _ = alice.conn.Close() }()
	alice.send(t, []byte("duplex"))
	if !bytes.Equal(alice.recv(t, 6), []byte("duplex")) {
		t.Fatal("carrier duplex prerequisite")
	}
	if legacy {
		if client.FlowCensus().Legacy != 1 || budget.Used() != 0 {
			t.Fatal("legacy session was presented as a resumable flow")
		}
		bob := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "bob", target.listener.Addr().String())
		defer func() { _ = bob.conn.Close() }()
		bob.send(t, []byte("before"))
		if !bytes.Equal(bob.recv(t, 6), []byte("before")) {
			t.Fatal("legacy survivor prerequisite")
		}
		if err := s.SetKeys([]Key{{KeyID: "bob", Secret: "bob"}}); err != nil {
			t.Fatal(err)
		}
		_ = alice.conn.SetReadDeadline(time.Now().Add(time.Second))
		var one [1]byte
		if _, err := alice.conn.Read(one[:]); !isClosedRead(err) {
			t.Fatal("legacy victim remained open", err)
		}
		bob.send(t, []byte("after"))
		if !bytes.Equal(bob.recv(t, 5), []byte("after")) {
			t.Fatal("legacy revoke killed survivor")
		}
		if target.accepts.Load() != 2 {
			t.Fatal("legacy target was redialed")
		}
		return
	}
	before := budget.Used()
	if before == 0 {
		t.Fatal("replay reservation is empty")
	}
	if err := s.SetKeys([]Key{{KeyID: "bob", Secret: "bob"}}); err != nil {
		t.Fatal(err)
	}
	awaitSS(t, func() bool { return budget.Used() == 0 }, "silent victim retained public flow replay")
	_ = alice.conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := alice.conn.Read(b[:]); err == nil || err != io.EOF && !isClosedRead(err) {
		t.Fatalf("public victim was not closed: %v", err)
	}
}
func isClosedRead(err error) bool { return err != nil && !timeoutError(err) }
