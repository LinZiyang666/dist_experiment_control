//go:build linux

package httplisten

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §6.1 / T20
func TestHTTPFreezeJoinsStoreHandlers(t *testing.T) {
	t.Run("accepted_keepalive", exerciseHTTPKeepaliveFreeze)
	t.Run("sql_cancel", func(t *testing.T) {
		db, err := storage.Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer closeHTTPTestResource(t, db)
		exerciseHTTPQueryCancel(t, db)
	})
	t.Run("store_seal", func(t *testing.T) {
		if err := brokerimage.InstallColdSingleStoreSupport(); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		store, db, err := brokerimage.OpenSingleStore(brokerimage.SingleStoreConfig{
			DBPath: filepath.Join(dir, "state.db"), DataDir: dir, Registry: storage.DefaultDriverRegistry(),
		})
		if err != nil {
			t.Fatalf("P03 real store required: %v", err)
		}
		defer closeHTTPTestResource(t, db)
		lock, err := store.LockResource()
		if err != nil {
			t.Fatal(err)
		}
		// This test is the terminal owner of its private fixture. Release only
		// after its driver-close receipt, never during image handoff.
		defer func() {
			for _, f := range lock.Files {
				_ = f.Close()
			}
		}()
		exerciseHTTPQueryCancel(t, db)
		deadline := httpImageDeadline(t, 5*time.Second)
		if err := store.Quiesce(deadline); err != nil {
			t.Fatal(err)
		}
		seal, err := store.CloseForImage(deadline)
		if err != nil {
			t.Fatal(err)
		}
		if seal.Writer != storage.CloseClosed || seal.ReadOnly != storage.CloseClosed || seal.DriverWriters != 0 || seal.DriverReaders != 0 || seal.ActiveBackups != 0 || seal.ActiveRPCs != 0 {
			t.Fatalf("HTTP join did not precede actual store seal: %+v", seal)
		}
	})
}

// origin: hot-update S6 plan §11 / §12.1 P09
func TestBrokerHTTPLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseHTTPKeepaliveFreeze(t)
	}
	testharness.AssertNoGoroutineLeak(t, "HTTP keepalive freeze", beforeG)
	testharness.AssertNoFDLeak(t, "HTTP keepalive freeze", beforeFD, 0)
}

func exerciseHTTPKeepaliveFreeze(t *testing.T) {
	t.Helper()
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/blocked" {
			close(entered)
			<-r.Context().Done()
			close(canceled)
			<-release // user code deliberately outlives Serve/Close
			return
		}
		_, _ = io.WriteString(w, "current-generation\n")
	})
	k := captureHTTPKeeper(t)
	defer closeHTTPTestResource(t, k)
	original := k.Descriptor()
	owner, err := StartImage(k, handler, "test-http", httpImageDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(httpImageDeadline(t, 5*time.Second)) }()
	conn := dialHTTP(t, k)
	defer closeHTTPTestResource(t, conn)
	reader := bufio.NewReader(conn)
	writeHTTPRequest(t, conn, "/warm")
	assertHTTPBody(t, reader, "current-generation\n")
	writeHTTPRequest(t, conn, "/blocked")
	awaitHTTP(t, entered)
	if err := owner.Join(httpImageDeadline(t, time.Second)); !errors.Is(err, ErrImageBusy) {
		t.Fatalf("unfenced Join=%v", err)
	}
	if err := owner.Fence(httpImageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	awaitHTTP(t, canceled)
	awaitHTTPServeDone(t, owner.(*serverOwner))
	if err := owner.Join(httpImageDeadline(t, 40*time.Millisecond)); !errors.Is(err, ErrImageDeadline) {
		t.Fatalf("Serve returned while live handler survived: Join=%v", err)
	}
	if err := owner.Resume(); !errors.Is(err, ErrImageBusy) {
		t.Fatalf("Resume before handler exit=%v", err)
	}
	// The kernel must still accept a fresh socket during the paused view;
	// the response is checked after Resume, so handshake alone is no oracle.
	fresh := dialHTTP(t, k)
	defer closeHTTPTestResource(t, fresh)
	writeHTTPRequest(t, fresh, "/fresh")
	if calls.Load() != 2 {
		t.Fatalf("business admitted after fence: %d", calls.Load())
	}
	// Releasing the handler is not cleanup-only: prove Join now succeeds and
	// the original LISTEN identity actually serves the queued fresh request.
	release <- struct{}{}
	if err := owner.Join(httpImageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if k.Descriptor() != original {
		t.Fatal("listener identity changed during fence")
	}
	if _, _, err := k.Export(); err != nil {
		t.Fatalf("keeper not exportable after join: %v", err)
	}
	if err := owner.Resume(); err != nil {
		t.Fatal(err)
	}
	assertHTTPBody(t, bufio.NewReader(fresh), "current-generation\n")
	if err := owner.Stop(httpImageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Resume(); !errors.Is(err, ErrImageStopped) {
		t.Fatalf("terminal owner resumed: %v", err)
	}
	probe, err := net.DialTimeout("tcp", original.Address, 200*time.Millisecond)
	if err == nil {
		closeHTTPTestResource(t, probe)
		t.Fatal("normal Stop left LISTEN socket open")
	}
}

func exerciseHTTPQueryCancel(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background() // ctx-root: hermetic SQL cancellation fixture.
	busy, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeHTTPTestResource(t, busy)
	baseWaits := db.Stats().WaitCount
	entered, queryDone := make(chan struct{}), make(chan error, 1)
	k := captureHTTPKeeper(t)
	defer closeHTTPTestResource(t, k)
	owner, err := StartImage(k, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		var value int
		err := db.QueryRowContext(r.Context(), "SELECT 1").Scan(&value)
		queryDone <- err
	}), "sql-http", httpImageDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(httpImageDeadline(t, 5*time.Second)) }()
	conn := dialHTTP(t, k)
	defer closeHTTPTestResource(t, conn)
	writeHTTPRequest(t, conn, "/query")
	awaitHTTP(t, entered)
	waitHTTPCondition(t, func() bool { return db.Stats().WaitCount > baseWaits })
	if err := owner.Fence(httpImageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Join(httpImageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-queryDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("accepted SQL not canceled: %v", err)
		}
	default:
		t.Fatal("join preceded accepted SQL completion")
	}
	if err := busy.Close(); err != nil {
		t.Fatal(err)
	}
	if db.Stats().InUse != 0 {
		t.Fatal("HTTP query retained driver connection")
	}
}

// origin: hot-update S6 plan §6.1 / T20
func TestHTTPImageRejectsExpiredDeadlineBeforeFence(t *testing.T) {
	k := captureHTTPKeeper(t)
	defer closeHTTPTestResource(t, k)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "still-live\n") })
	if owner, err := StartImage(k, h, "expiry", 0); owner != nil || !errors.Is(err, ErrImageDeadline) {
		t.Fatalf("invalid start=%v, %v", owner, err)
	}
	owner, err := StartImage(k, h, "expiry", httpImageDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(httpImageDeadline(t, 5*time.Second)) }()
	if err := owner.Fence(1); !errors.Is(err, ErrImageDeadline) {
		t.Fatalf("expired fence=%v", err)
	}
	conn := dialHTTP(t, k)
	defer closeHTTPTestResource(t, conn)
	writeHTTPRequest(t, conn, "/")
	assertHTTPBody(t, bufio.NewReader(conn), "still-live\n")
}

// origin: hot-update S6 plan §6.1 / T20
func TestHTTPKeepaliveAdmissionSharesFence(t *testing.T) {
	var calls atomic.Int32
	o := newServerOwner(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }), "admission") // ctx-root: isolated HTTP admission state fixture.
	o.fenced = true
	// A previously accepted connection can dispatch a parsed request after
	// Fence acquires the latch. Exercise that exact handler boundary directly.
	w := &httpAdmissionWriter{header: make(http.Header)}
	r, err := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	if err != nil {
		t.Fatal(err)
	}
	o.serveHTTP(w, r)
	if calls.Load() != 0 || w.status != http.StatusServiceUnavailable || w.header.Get("Connection") != "close" {
		t.Fatalf("fenced request entered business: calls=%d status=%d", calls.Load(), w.status)
	}
}

type httpAdmissionWriter struct {
	header http.Header
	status int
}

func (w *httpAdmissionWriter) Header() http.Header         { return w.header }
func (w *httpAdmissionWriter) WriteHeader(status int)      { w.status = status }
func (w *httpAdmissionWriter) Write(p []byte) (int, error) { return len(p), nil }

func captureHTTPKeeper(t *testing.T) listenerimage.Keeper {
	t.Helper()
	ln, err := Bind("http-image-test", "127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	k, err := listenerimage.Capture(ln, "http-image-test")
	if err != nil {
		if k != nil {
			_ = k.Close()
		} else {
			_ = ln.Close()
		}
		t.Fatal(err)
	}
	return k
}
func httpImageDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(duration))
}
func dialHTTP(t *testing.T, k listenerimage.Keeper) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", k.Descriptor().Address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn
}
func writeHTTPRequest(t *testing.T, conn net.Conn, path string) {
	t.Helper()
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\n\r\n", path); err != nil {
		t.Fatal(err)
	}
}
func assertHTTPBody(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != want {
		t.Fatalf("HTTP response: status=%d body=%q err=%v", resp.StatusCode, body, err)
	}
}
func awaitHTTP(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP fixture handshake timed out")
	}
}
func waitHTTPCondition(t *testing.T, ready func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(end) {
			t.Fatal("HTTP condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func awaitHTTPServeDone(t *testing.T, o *serverOwner) {
	t.Helper()
	waitHTTPCondition(t, func() bool { o.mu.Lock(); defer o.mu.Unlock(); return o.serveDone })
}

// origin: hot-update S6 plan §6.1 / T20
func TestHTTPServeJoinsCancellationCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: ordinary HTTP shutdown fixture.
	defer cancel()
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	ln, err := Bind("ordinary-http", "127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeHTTPTestResource(t, ln)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-r.Context().Done()
			close(canceled)
			<-release
		}), "ordinary-http")
	}()
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer closeHTTPTestResource(t, conn)
	writeHTTPRequest(t, conn, "/")
	awaitHTTP(t, entered)
	cancel()
	awaitHTTP(t, canceled)
	select {
	case err := <-done:
		t.Fatalf("Serve returned before handler and cancellation callback joined: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary Serve did not join")
	}
}

// origin: hot-update S6 plan §6.1 / T20
func TestHTTPImageRefusesHijackedWork(t *testing.T) {
	k := captureHTTPKeeper(t)
	defer closeHTTPTestResource(t, k)
	hijacked := make(chan net.Conn, 1)
	owner, err := StartImage(k, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			panic(err)
		}
		hijacked <- conn
	}), "hijack-http", httpImageDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(httpImageDeadline(t, 5*time.Second)) }()
	conn := dialHTTP(t, k)
	defer closeHTTPTestResource(t, conn)
	writeHTTPRequest(t, conn, "/")
	var serverConn net.Conn
	select {
	case serverConn = <-hijacked:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP hijack was not exercised")
	}
	defer closeHTTPTestResource(t, serverConn)
	if err := owner.Fence(httpImageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Join(httpImageDeadline(t, 5*time.Second)); !errors.Is(err, ErrImageHijacked) {
		t.Fatalf("hijacked work treated as joined: %v", err)
	}
	if err := owner.Resume(); !errors.Is(err, ErrImageBusy) {
		t.Fatalf("hijacked work resumed: %v", err)
	}
}

// origin: hot-update S6 plan §11 / T20
// Terminal-stop assertions close some resources before deferred cleanup runs.
// Only an already-closed resource is benign; other release failures fail the test.
func closeHTTPTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("close test resource: %v", err)
	}
}
