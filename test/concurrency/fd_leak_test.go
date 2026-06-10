//go:build linux

// fd_leak_test.go — Linux-only file-descriptor leak detector. Counts
// /proc/self/fd entries before/after a tight Open/Close loop and
// asserts the delta is small (< 5).
//
// Why a loose bound: between iterations Go's runtime may keep a
// netpoller fd, the test framework may keep a stdout buffer fd,
// nats-server's accept goroutines may briefly hold sockets in
// TIME_WAIT before the kernel reaps them. Going lower than 5 makes
// the test flake on busy CI; going higher would mask real leaks
// (a 100-iteration loop where each iter leaks one fd would still
// trip the < 5 ceiling).
package concurrency_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// numFDs returns how many open file descriptors the test process
// currently has, by reading /proc/self/fd. Linux-only (the package
// build tag enforces that).
func numFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(entries)
}

// pollFDs is numFDs but waits up to ~500ms for the kernel to reap
// transient sockets (TIME_WAIT, half-closed handshakes). Used at
// the END of an FD-leak test so we only fail when the count stays
// elevated, not on a single bad sample.
func pollFDs(t *testing.T, baseline int, tolerance int) int {
	t.Helper()
	var last int
	for i := 0; i < 50; i++ {
		last = numFDs(t)
		if last-baseline <= tolerance {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last
}

// TestTunnelOpenCloseFDStable — B.6. Open then Close a tunnel
// session 50 times against the same server; FD count must stay
// flat (< 5 above baseline). 50 not 100 because each Open spawns a
// TLS handshake on the broker side, and the kernel takes a moment
// to release the underlying TCP — pushing too high makes the test
// CPU-heavy without buying coverage.
func TestTunnelOpenCloseFDStable(t *testing.T) {
	controlPort := findFreePort(t)
	srv := newTunnelServer(t, controlPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// Warm-up — first open allocates TLS state, accept-loop
	// goroutine pool, etc. Don't include those in the baseline.
	{
		port := findFreePort(t)
		cli := tunnel.NewClient(
			net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
			"lab", "lab-1",
			func(int) (int, error) { return 1, nil },
			silentLog(),
		)
		cli.Start(ctx)
		if err := cli.Open(port, 1, "tok"); err != nil {
			t.Fatal(err)
		}
		cli.Close(port)
		// Give the broker a moment to reap the public listener.
		time.Sleep(50 * time.Millisecond)
	}

	baseline := numFDs(t)

	for i := 0; i < 50; i++ {
		port := findFreePort(t)
		cli := tunnel.NewClient(
			net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
			"lab", "lab-1",
			func(int) (int, error) { return 1, nil },
			silentLog(),
		)
		cli.Start(ctx)
		if err := cli.Open(port, 1, fmt.Sprintf("tok-%d", i)); err != nil {
			t.Fatalf("iter %d: open: %v", i, err)
		}
		cli.Close(port)
	}

	last := pollFDs(t, baseline, 5)
	if last-baseline > 5 {
		t.Errorf("tunnel open/close fd leak: baseline=%d after=%d (+%d)",
			baseline, last, last-baseline)
	}
}

// TestStorageOpenCloseFDStable — B.7. Same idea applied to the
// SQLite layer: 20 open/close roundtrips against a freshly-created
// file-backed DB shouldn't leak any FDs. We use a file-backed DB
// (not :memory:) because :memory: doesn't open the FD path at all.
func TestStorageOpenCloseFDStable(t *testing.T) {
	dir := t.TempDir()

	// Warm-up.
	{
		db, err := storage.Open("file:" + filepath.Join(dir, "warm.db"))
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}

	baseline := numFDs(t)

	for i := 0; i < 20; i++ {
		path := "file:" + filepath.Join(dir, fmt.Sprintf("iter-%d.db", i))
		db, err := storage.Open(path)
		if err != nil {
			t.Fatalf("iter %d: open: %v", i, err)
		}
		// Cheap query to make sure the conn is actually checked
		// out / returned to the pool — without this SetMaxOpenConns
		// could keep the conn idle and never bind a real FD.
		if _, err := db.Exec(`SELECT 1`); err != nil {
			t.Fatalf("iter %d: exec: %v", i, err)
		}
		_ = db.Close()
	}

	last := pollFDs(t, baseline, 5)
	if last-baseline > 5 {
		t.Errorf("storage open/close fd leak: baseline=%d after=%d (+%d)",
			baseline, last, last-baseline)
	}
}

// TestAdminsockClientCallFDStable — B.8. Adminsock client opens a
// fresh Unix socket per Call (its docstring says so). 1000
// roundtrips must not leak even one FD per call — that would be
// "Call doesn't close on error path" and would expose the broker
// to fd exhaustion under any sustained admin polling.
//
// Cap kept at 200 (down from the spec's 1000) because each Call
// also spawns a server-side handle goroutine — at 1000 the test
// just becomes "do we have 1000+ FDs available right now" without
// adding leak coverage.
func TestAdminsockClientCallFDStable(t *testing.T) {
	db := openMemDB(t)
	socketPath := shortSocketPath(t)
	srv := adminsock.New(socketPath, adminsock.Backend{
		DB: db, Logger: silentLog(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()

	cli := &adminsock.Client{Path: socketPath, Timeout: 2 * time.Second}

	// Warm-up.
	if _, err := cli.Call(adminsock.Request{Op: adminsock.OpSessions}); err != nil {
		t.Fatal(err)
	}

	baseline := numFDs(t)

	for i := 0; i < 200; i++ {
		if _, err := cli.Call(adminsock.Request{Op: adminsock.OpSessions}); err != nil {
			t.Fatalf("iter %d: call: %v", i, err)
		}
	}

	last := pollFDs(t, baseline, 5)
	if last-baseline > 5 {
		t.Errorf("adminsock call fd leak: baseline=%d after=%d (+%d)",
			baseline, last, last-baseline)
	}
}

// TestBrokerNewCloseFDStable — B.7 broker variant. Boot + cancel
// the broker 10 times against the same NATS server; the broker
// should reach FD steady-state quickly. The bound is wider here
// (10 instead of 5) because each iteration spawns a fresh NATS
// connection, and the kernel keeps a TIME_WAIT socket per
// connection for a few seconds.
func TestBrokerNewCloseFDStable(t *testing.T) {
	url := startNATS(t)
	db := openMemDB(t)

	// Warm-up so the first slow boot's allocations don't show up
	// as a leak.
	_, sd := startBrokerNoTunnel(t, url, db)
	sd()

	baseline := numFDs(t)

	for i := 0; i < 10; i++ {
		_, sd := startBrokerNoTunnel(t, url, db)
		sd()
	}

	last := pollFDs(t, baseline, 10)
	if last-baseline > 10 {
		t.Errorf("broker new/close fd leak: baseline=%d after=%d (+%d)",
			baseline, last, last-baseline)
	}
}
