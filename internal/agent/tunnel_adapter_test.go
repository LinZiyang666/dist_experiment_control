package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/tunnel"
)

// tunnel_adapter_test.go — TunnelExposeAdapter's op lock and the ctx it hands the tunnel client.
// The agent's forwarded-expose ladder gives AddProxy ONE deadline for the whole open; the adapter
// must spend it on the lock wait as well as on the dial, and must not start a dial whose budget is
// already gone (external review F1: "锁等待" was outside the budget).

func silentAdapterLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startCountingTunnelServer is a broker tunnel server that admits every REGISTER and counts them.
func startCountingTunnelServer(t *testing.T) (addr string, registers *atomic.Int64) {
	t.Helper()
	var n atomic.Int64
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	srv := tunnel.NewServer(addr, "127.0.0.1", func(_, _ string, _ int, _ string, _ int64) error {
		n.Add(1)
		return nil
	}, silentAdapterLog())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return addr, &n
}

// origin: simcluster-speed external review F1 (adapter lock wait outside the budget)
func TestAddProxyGivesUpTheLockWaitAtTheCallersDeadline(t *testing.T) {
	addr, registers := startCountingTunnelServer(t)
	ad := NewTunnelExposeAdapter(addr, "lab", "lab-1", silentAdapterLog())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ad.Start(ctx)

	// Another op (a slow rehome, a RemoveProxy stuck behind a closing yamux) holds the adapter's
	// op lock for longer than this expose's whole budget. Margins per re-review R1: hold 6× the
	// budget, `took` bound 3.5× it.
	ad.acquireOpBlocking()
	released := make(chan struct{})
	go func() {
		time.Sleep(1200 * time.Millisecond)
		ad.releaseOp()
		close(released)
	}()

	openCtx, cancelOpen := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelOpen()
	start := time.Now()
	err := ad.AddProxy(openCtx, PortToken{Name: "web", Port: 14003, LocalPort: 8080, Token: "tok"})
	took := time.Since(start)
	if err == nil {
		t.Fatalf("AddProxy succeeded after %s behind a 1200 ms lock hold and a 200 ms budget", took)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want the caller's deadline", err)
	}
	if took > 700*time.Millisecond {
		t.Fatalf("AddProxy took %s: it waited for the lock instead of the budget", took)
	}
	<-released
	// The budget expired in the lock wait: no dial may have started afterwards, and the port
	// must not be mapped (a stale localFor entry would answer the broker's next stream for it).
	if got := registers.Load(); got != 0 {
		t.Fatalf("REGISTERs=%d: an open whose budget expired in the lock wait must not dial", got)
	}
	if _, err := ad.lookupLocal(14003); err == nil {
		t.Fatal("localFor still maps the port of an open that never happened")
	}
	if ad.HasSession(14003) {
		t.Fatal("a session exists for an open that was refused at the lock")
	}
}

// The lock is the same lock: a successful AddProxy still excludes RemoveProxy, and an AddProxy that
// gets the lock inside its budget opens normally.
func TestAddProxyOpensWhenTheLockIsFreeInsideTheBudget(t *testing.T) {
	addr, registers := startCountingTunnelServer(t)
	ad := NewTunnelExposeAdapter(addr, "lab", "lab-1", silentAdapterLog())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ad.Start(ctx)

	ad.acquireOpBlocking()
	go func() { time.Sleep(50 * time.Millisecond); ad.releaseOp() }()
	openCtx, cancelOpen := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelOpen()
	// The public port is bound by the tunnel SERVER on REGISTER; reserve a number and free it
	// (the same TOCTOU probe the tunnel tests use — good enough for one open).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	publicPort := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	if err := ad.AddProxy(openCtx, PortToken{Name: "web", Port: publicPort, LocalPort: 8080, Token: "tok"}); err != nil {
		t.Fatalf("AddProxy after the lock was released inside the budget: %v", err)
	}
	if registers.Load() < 1 {
		t.Fatal("no REGISTER reached the home")
	}
	if !ad.HasSession(publicPort) {
		t.Fatal("the open succeeded but no session is installed")
	}
}
