package tunnel

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// open_deadline_test.go — the caller's ctx bounds Client.OpenHome: the wait for the REGISTER
// answer, and the install after it. Nothing here bounds the SESSION an open installs — that stays on
// the Client's Start ctx (tunnel_reconnect_test.go covers its supervisor).

// slowRegisterServer is a broker tunnel server whose REGISTER lookup holds every answer for `hold`
// (nil after that = OK). registers counts the handshakes that reached the lookup.
type slowRegisterServer struct {
	srv       *Server
	addr      string
	hold      *atomic.Int64 // nanoseconds
	registers *atomic.Int64
	onLookup  atomic.Pointer[func()] // fired right before the lookup answers OK (nil = nothing)
}

func startSlowRegisterServer(t *testing.T) *slowRegisterServer {
	t.Helper()
	var hold, registers atomic.Int64
	s := &slowRegisterServer{hold: &hold, registers: &registers}
	lookup := func(_, _ string, _ int, _ string, _ int64) error {
		registers.Add(1)
		time.Sleep(time.Duration(hold.Load()))
		if fn := s.onLookup.Load(); fn != nil {
			(*fn)()
		}
		return nil
	}
	controlPort := findFreePort(t)
	s.addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort))
	s.srv = NewServer(s.addr, "127.0.0.1", lookup, silentLog())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := s.srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func newOpenDeadlineClient(t *testing.T, addr string) *Client {
	t.Helper()
	cli := NewClient(addr, "lab", "lab-1", func(int) (int, error) { return 1, nil }, silentLog())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cli.Start(ctx)
	return cli
}

// TestOpenHomeIsCutAtTheCallersDeadlineAndNeverInstallsLate: a REGISTER the home answers after the
// caller's deadline is cut at the deadline (the error names the deadline), and the session is NOT
// installed when the answer finally arrives — the agent already told the broker the open failed and
// the broker rolled the allocation back; a session appearing afterwards would serve a port that no
// longer exists on the control plane (external review F1's "迟到成功").
// origin: simcluster-speed external review F1
func TestOpenHomeIsCutAtTheCallersDeadlineAndNeverInstallsLate(t *testing.T) {
	// Margins (re-review R1): the hold is 5× the deadline and the `took` bound 3× it, so a loaded
	// -race matrix moves the numbers without moving the verdict.
	s := startSlowRegisterServer(t)
	s.hold.Store(int64(1500 * time.Millisecond))
	cli := newOpenDeadlineClient(t, s.addr)
	port := findFreePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := cli.OpenHome(ctx, port, 1, "tok", "", 0, proto.CertPins{})
	took := time.Since(start)
	if err == nil {
		t.Fatalf("OpenHome returned nil after %s against a 300 ms deadline and a 1500 ms REGISTER hold", took)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v: the caller's deadline must be the named cause, not the closed-conn symptom", err)
	}
	if took > 900*time.Millisecond {
		t.Fatalf("OpenHome took %s: it waited out the REGISTER hold instead of the caller's deadline", took)
	}
	// The home answers OK at ~1500 ms. Give it that and more: no session may appear.
	time.Sleep(1700*time.Millisecond - took)
	if cli.HasSession(port) {
		t.Fatalf("a session for port %d was installed after the caller's deadline — the late success the review named", port)
	}
	if got := s.registers.Load(); got != 1 {
		t.Fatalf("REGISTERs=%d, want exactly the one cut handshake (no supervisor was spawned to redial)", got)
	}
}

// TestOpenHomeInstallsAndOutlivesTheCallersContext is the other half of the contract, the one that
// keeps this from becoming gotcha #80 one layer down: an open that finishes inside the deadline
// installs the session, and cancelling / expiring the CALLER'S ctx afterwards does not touch it — the
// supervisor still self-heals a transport drop (a session on a dead ctx reads its first drop as an
// intentional teardown and never redials, so "HasSession right after cancel" alone would not see it).
// MUTATION: derive sessCtx from hsCtx (or from ctx) in OpenHome and this goes red.
func TestOpenHomeInstallsAndOutlivesTheCallersContext(t *testing.T) {
	s := startSlowRegisterServer(t)
	cli := newOpenDeadlineClient(t, s.addr)
	cli.SetBackoffForTest(10*time.Millisecond, 80*time.Millisecond)
	port := findFreePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := cli.OpenHome(ctx, port, 1, "tok", "", 0, proto.CertPins{}); err != nil {
		cancel()
		t.Fatalf("OpenHome: %v", err)
	}
	cancel() // the caller is done; its deadline/cancel must be inert from here on
	time.Sleep(50 * time.Millisecond)
	if !s.srv.DropTransport(port) {
		t.Fatalf("no live session for port %d on the home right after a successful open", port)
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.registers.Load() < 2 || !cli.HasSession(port) {
		if time.Now().After(deadline) {
			t.Fatalf("after the caller's ctx ended, a transport drop was not healed (REGISTERs=%d, session=%v): the session's life was tied to the caller's ctx — a control-plane deadline became a data-plane lifetime",
				s.registers.Load(), cli.HasSession(port))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestOpenHomeRefusesAnAlreadyDoneContext: a ladder whose budget is gone before the next call starts
// must not dial at all (no REGISTER reaches the home).
func TestOpenHomeRefusesAnAlreadyDoneContext(t *testing.T) {
	s := startSlowRegisterServer(t)
	cli := newOpenDeadlineClient(t, s.addr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cli.OpenHome(ctx, findFreePort(t), 1, "tok", "", 0, proto.CertPins{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want the ctx error without a dial", err)
	}
	if got := s.registers.Load(); got != 0 {
		t.Fatalf("REGISTERs=%d: a done ctx must not reach the home", got)
	}
}

// TestOpenHomeDiscardsARegisteredTransportWhenTheDeadlinePassesBeforeInstall pins the fence under
// c.mu: the home answers OK, but the caller's ctx ends right as that answer is produced. Whichever side
// of the handshake read the cancel lands on, the outcome must be the same — no session, a ctx error,
// and the registered transport closed (the home sees the drop and frees its side). The cancel is fired
// from inside the lookup, immediately before the OK is written, twenty times, so both interleavings
// (conn closed before the OK is read; OK read, then the install fence) are exercised.
// MUTATION: delete the `ctx.Err()` fence after `c.mu.Lock()` in OpenHome and the run that reads the OK
// first installs the session — this goes red.
func TestOpenHomeDiscardsARegisteredTransportWhenTheDeadlinePassesBeforeInstall(t *testing.T) {
	s := startSlowRegisterServer(t)
	cli := newOpenDeadlineClient(t, s.addr)
	for i := 0; i < 20; i++ {
		port := findFreePort(t)
		ctx, cancel := context.WithCancel(context.Background())
		fire := func() { cancel() }
		s.onLookup.Store(&fire)
		err := cli.OpenHome(ctx, port, 1, "tok", "", 0, proto.CertPins{})
		s.onLookup.Store(nil)
		if err == nil {
			cancel()
			t.Fatalf("iteration %d: OpenHome succeeded although the caller's ctx ended before the install", i)
		}
		if !errors.Is(err, context.Canceled) {
			cancel()
			t.Fatalf("iteration %d: err=%v, want the caller's ctx as the cause", i, err)
		}
		if cli.HasSession(port) {
			cancel()
			t.Fatalf("iteration %d: a session was installed for a cancelled open", i)
		}
		cancel()
	}
}
