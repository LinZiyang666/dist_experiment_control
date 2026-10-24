package caddyconfig

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// These default-environment tests exercise the actual single-socket HTTP
// protocol. The verifier double checks the P03 adapter invocation and every
// identity field; it is not a SO_PEERCRED or privileged root-isolation receipt.
// P31/T72 owns the real P03/kernel credential and legacy 2019 negative controls.
// origin: hot-update S9 plan §5.1 / T19
func TestCaddyAdminBindsInvocation(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := OpenAdmin(t.Context(), "/private/admin.sock", adminProcess(), 1000, nil); !errors.Is(err, c.ErrUnsupported) {
			t.Fatal("unsupported admin platform accepted")
		}
		return
	}
	opts := certificateOptions(t)
	fixture := startAdminFixture(t, opts.AdminSocket)
	defer fixture.close(t)
	cfg, err := Prepare(supportedConfig(t), opts)
	if err != nil {
		t.Fatal(err)
	}
	verify := fixture.verifier()
	admin, err := OpenAdmin(t.Context(), opts.AdminSocket, adminProcess(), uint32(os.Getuid()), verify)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAdmin(t, admin)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x", "%q"} {
		if strings.Contains(fmt.Sprintf(verb, admin), opts.AdminSocket) {
			t.Fatal("formatted admin disclosed private socket path")
		}
	}
	if got, err := admin.Load(t.Context(), cfg); err != nil || got != cfg.Digest() {
		t.Fatalf("load digest: %x, %v", got, err)
	}
	if got, err := admin.Load(t.Context(), cfg); err != nil || got != cfg.Digest() {
		t.Fatal("idempotent load failed")
	}
	if fixture.count(http.MethodPost, "/load") != 1 {
		t.Fatal("load replayed despite actual matching configuration")
	}
	if fixture.acceptCount() != 1 {
		t.Fatal("admin redialed during load/query")
	}
	if got, err := admin.Current(t.Context()); err != nil || got != cfg.Digest() {
		t.Fatal("current query lost canonical digest")
	}
	if fixture.verifyCount() != 6 {
		t.Fatal("admin omitted per-exchange identity checks")
	}
	wrong := *cfg
	wrong.adminSocket += "-other"
	for _, bad := range []*Config{nil, &wrong} {
		if _, err := admin.Load(t.Context(), bad); !errors.Is(err, ErrAdmin) {
			t.Fatal("unbound configuration accepted")
		}
	}
	// A nil context reaches these entry points as an unset context.Context
	// variable or field, not as a literal, so the refusal is driven that way.
	var unsetCtx context.Context
	if _, err := admin.Current(unsetCtx); !errors.Is(err, ErrAdmin) {
		t.Fatal("nil query context accepted")
	}
	if _, err := admin.Load(unsetCtx, cfg); !errors.Is(err, ErrAdmin) {
		t.Fatal("nil load context accepted")
	}
	if a, err := OpenAdmin(unsetCtx, opts.AdminSocket, adminProcess(), uint32(os.Getuid()), verify); a != nil || !errors.Is(err, ErrAdmin) {
		if a != nil {
			_ = a.Close()
		}
		t.Fatal("nil binding context accepted")
	}

	for _, name := range []string{"BootID", "PID", "StartTicks", "InvocationID", "ExeDevice", "ExeInode", "ExeSHA256", "Cgroup"} {
		t.Run("wrong-"+name, func(t *testing.T) {
			p := adminProcess()
			switch name {
			case "BootID":
				p.BootID += "-other"
			case "PID":
				p.PID++
			case "StartTicks":
				p.StartTicks++
			case "InvocationID":
				p.InvocationID += "-other"
			case "ExeDevice":
				p.ExeDevice++
			case "ExeInode":
				p.ExeInode++
			case "ExeSHA256":
				p.ExeSHA256[0]++
			case "Cgroup":
				p.Cgroup += "-other"
			}
			if a, err := OpenAdmin(t.Context(), opts.AdminSocket, p, uint32(os.Getuid()), verify); err == nil || a != nil {
				if a != nil {
					_ = a.Close()
				}
				t.Fatal("foreign invocation accepted")
			}
		})
	}
	for _, test := range []struct {
		name     string
		expected c.Process
		uid      uint32
		verify   PeerVerifier
	}{
		{"root-load", adminProcess(), 0, func(*net.UnixConn, c.Process, uint32) error { return nil }},
		{"missing-identity", c.Process{}, uint32(os.Getuid()), func(*net.UnixConn, c.Process, uint32) error { return nil }},
		{"missing-peer-check", adminProcess(), uint32(os.Getuid()), nil},
		{"wrong-uid", adminProcess(), uint32(os.Getuid()) + 1, verify},
		{"canary-error", adminProcess(), uint32(os.Getuid()), func(*net.UnixConn, c.Process, uint32) error { return errors.New("PRIVATE-CANARY-ACCOUNT-KEY") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, err := OpenAdmin(t.Context(), opts.AdminSocket, test.expected, test.uid, test.verify)
			if a != nil {
				_ = a.Close()
			}
			if !errors.Is(err, ErrAdmin) || strings.Contains(err.Error(), "PRIVATE-CANARY") {
				t.Fatal("unverified/root admin accepted or disclosed verifier error")
			}
		})
	}
	for _, name := range []string{"BootID", "PID", "StartTicks", "InvocationID", "ExeDevice", "ExeInode", "ExeSHA256", "Cgroup"} {
		t.Run("missing-"+name, func(t *testing.T) {
			p := adminProcess()
			switch name {
			case "BootID":
				p.BootID = ""
			case "PID":
				p.PID = 0
			case "StartTicks":
				p.StartTicks = 0
			case "InvocationID":
				p.InvocationID = ""
			case "ExeDevice":
				p.ExeDevice = 0
			case "ExeInode":
				p.ExeInode = 0
			case "ExeSHA256":
				p.ExeSHA256 = c.Digest{}
			case "Cgroup":
				p.Cgroup = ""
			}
			called := false
			a, err := OpenAdmin(t.Context(), opts.AdminSocket, p, uint32(os.Getuid()), func(*net.UnixConn, c.Process, uint32) error { called = true; return nil })
			if a != nil {
				_ = a.Close()
			}
			if !errors.Is(err, ErrAdmin) || called {
				t.Fatal("incomplete identity reached peer binding")
			}
		})
	}
	for _, mode := range []os.FileMode{0644, 0660, 0777} {
		if err := os.Chmod(opts.AdminSocket, mode); err != nil {
			t.Fatal(err)
		}
		if a, err := OpenAdmin(t.Context(), opts.AdminSocket, adminProcess(), uint32(os.Getuid()), func(*net.UnixConn, c.Process, uint32) error { return nil }); err == nil || a != nil {
			if a != nil {
				_ = a.Close()
			}
			t.Fatal("unsafe socket mode accepted")
		}
	}
	if err := os.Chmod(opts.AdminSocket, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(opts.AdminSocket), 0755); err != nil {
		t.Fatal(err)
	}
	if a, err := OpenAdmin(t.Context(), opts.AdminSocket, adminProcess(), uint32(os.Getuid()), func(*net.UnixConn, c.Process, uint32) error { return nil }); err == nil || a != nil {
		if a != nil {
			_ = a.Close()
		}
		t.Fatal("unsafe runtime directory accepted")
	}
	if err := os.Chmod(filepath.Dir(opts.AdminSocket), 0700); err != nil {
		t.Fatal(err)
	}

	t.Run("path-reused-while-old-connected", func(t *testing.T) {
		if err := os.Remove(opts.AdminSocket); err != nil {
			t.Fatal(err)
		}
		successor := startAdminFixture(t, opts.AdminSocket)
		defer successor.close(t)
		if got, err := admin.Current(t.Context()); err != nil || got != cfg.Digest() {
			t.Fatal("connected invocation followed replacement path")
		}
		fixture.close(t)
		if _, err := admin.Current(t.Context()); err == nil {
			t.Fatal("old peer exit was not detected")
		}
		if _, err := admin.Load(t.Context(), cfg); err == nil {
			t.Fatal("closed invocation replayed load")
		}
		if successor.acceptCount() != 0 || successor.count(http.MethodPost, "/stop") != 0 || successor.count(http.MethodPost, "/load") != 0 {
			t.Fatal("old binding contacted/stopped successor")
		}
	})

	t.Run("changed-identity-before-load", func(t *testing.T) {
		o := certificateOptions(t)
		f := startAdminFixture(t, o.AdminSocket)
		defer f.close(t)
		a, err := OpenAdmin(t.Context(), o.AdminSocket, adminProcess(), uint32(os.Getuid()), f.verifier())
		if err != nil {
			t.Fatal(err)
		}
		defer closeAdmin(t, a)
		f.mu.Lock()
		f.identity.InvocationID = "successor"
		f.mu.Unlock()
		cfg, err := Prepare(supportedConfig(t), o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Load(t.Context(), cfg); !errors.Is(err, ErrAdmin) {
			t.Fatal("changed invocation accepted")
		}
		if f.count(http.MethodPost, "/load") != 0 {
			t.Fatal("changed invocation received load")
		}
	})

	t.Run("lost-load-reply-query-before-replay", func(t *testing.T) {
		o := certificateOptions(t)
		f := startAdminFixture(t, o.AdminSocket)
		defer f.close(t)
		cfg, err := Prepare(supportedConfig(t), o)
		if err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.dropLoadReply = true
		f.mu.Unlock()
		a, err := OpenAdmin(t.Context(), o.AdminSocket, adminProcess(), uint32(os.Getuid()), f.verifier())
		if err != nil {
			t.Fatal(err)
		}
		defer closeAdmin(t, a)
		if _, err := a.Load(t.Context(), cfg); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatal("lost load reply did not preserve unknown outcome")
		}
		if f.count(http.MethodPost, "/load") != 1 {
			t.Fatal("no real load effect before reply loss")
		}
		if _, err := a.Load(t.Context(), cfg); err == nil {
			t.Fatal("ambiguous binding reused")
		}
		b, err := OpenAdmin(t.Context(), o.AdminSocket, adminProcess(), uint32(os.Getuid()), f.verifier())
		if err != nil {
			t.Fatal(err)
		}
		defer closeAdmin(t, b)
		if got, err := b.Load(t.Context(), cfg); err != nil || got != cfg.Digest() {
			t.Fatal("actual effect query failed")
		}
		if f.count(http.MethodPost, "/load") != 1 {
			t.Fatal("lost reply blindly replayed POST")
		}
	})
	t.Run("load-ack-without-effect", func(t *testing.T) {
		o := certificateOptions(t)
		f := startAdminFixture(t, o.AdminSocket)
		defer f.close(t)
		f.mu.Lock()
		f.ignoreLoad = true
		f.mu.Unlock()
		a, err := OpenAdmin(t.Context(), o.AdminSocket, adminProcess(), uint32(os.Getuid()), f.verifier())
		if err != nil {
			t.Fatal(err)
		}
		defer closeAdmin(t, a)
		cfg, err := Prepare(supportedConfig(t), o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Load(t.Context(), cfg); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatal("HTTP200 signed unapplied config")
		}
	})
	for _, test := range []struct {
		name   string
		body   []byte
		status int
	}{
		{"server-error-secret", []byte("PRIVATE-CANARY-ACCOUNT-KEY"), http.StatusInternalServerError},
		{"server-error-json", []byte(`{"body":"PRIVATE-CANARY-ACCOUNT-KEY"}`), http.StatusInternalServerError},
		{"invalid-query-json", []byte(`{"apps":{},"apps":{}}`), http.StatusOK},
		{"oversized-query", []byte("{\"body\":\"" + strings.Repeat("x", 4<<20) + "\"}"), http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := certificateOptions(t)
			f := startAdminFixture(t, o.AdminSocket)
			defer f.close(t)
			f.mu.Lock()
			f.queryBody = test.body
			f.queryStatus = test.status
			f.mu.Unlock()
			a, err := OpenAdmin(t.Context(), o.AdminSocket, adminProcess(), uint32(os.Getuid()), f.verifier())
			if err != nil {
				t.Fatal(err)
			}
			defer closeAdmin(t, a)
			if _, err := a.Current(t.Context()); !errors.Is(err, ErrAdmin) || strings.Contains(err.Error(), "PRIVATE-CANARY") {
				t.Fatal("bad query accepted or error disclosed private peer response")
			}
		})
	}
}

// origin: hot-update S9 plan §12 P08 / T19
func TestCaddyAdminLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("infra admin is unsupported on this platform")
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCaddyAdmin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "Caddy admin cancellation and reply loss", goroutines)
	testharness.AssertNoFDLeak(t, "Caddy admin cancellation and reply loss", fds, 0)
}

func exerciseCaddyAdmin(t *testing.T) {
	t.Helper()
	opts := certificateOptions(t)
	f := startAdminFixture(t, opts.AdminSocket)
	defer f.close(t)
	a, err := OpenAdmin(t.Context(), opts.AdminSocket, adminProcess(), uint32(os.Getuid()), f.verifier())
	if err != nil {
		t.Fatal(err)
	}
	defer closeAdmin(t, a)
	f.mu.Lock()
	f.stall = make(chan struct{})
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := a.Current(ctx); done <- err }()
	select {
	case <-f.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("admin query did not reach peer")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled admin read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled admin read did not join")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal("admin close is not idempotent")
	}
	f.mu.Lock()
	close(f.stall)
	f.stall = nil
	f.mu.Unlock()
	// Failed peer verification must close the just-dialed FD as well.
	if b, err := OpenAdmin(t.Context(), opts.AdminSocket, adminProcess(), uint32(os.Getuid()), func(*net.UnixConn, c.Process, uint32) error { return ErrAdmin }); b != nil || !errors.Is(err, ErrAdmin) {
		if b != nil {
			_ = b.Close()
		}
		t.Fatal("failed binding retained admin")
	}
	f.close(t)
}

type adminFixture struct {
	mu            sync.Mutex
	listener      *net.UnixListener
	server        *http.Server
	done          chan struct{}
	entered       chan struct{}
	data          []byte
	requests      map[string]int
	accepts       int
	verifications int
	identity      c.Process
	dropLoadReply bool
	ignoreLoad    bool
	queryBody     []byte
	queryStatus   int
	stall         chan struct{}
	closed        bool
}

func startAdminFixture(t *testing.T, socket string) *adminFixture {
	t.Helper()
	if err := os.Chmod(filepath.Dir(socket), 0700); err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	l.SetUnlinkOnClose(false)
	if err := os.Chmod(socket, 0600); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	f := &adminFixture{listener: l, done: make(chan struct{}), entered: make(chan struct{}, 1), data: []byte(`{}`), requests: make(map[string]int), identity: adminProcess()}
	f.server = &http.Server{Handler: http.HandlerFunc(f.serve), ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			f.mu.Lock()
			f.accepts++
			f.mu.Unlock()
		}
	}, ReadHeaderTimeout: time.Second}
	go func() { _ = f.server.Serve(l); close(f.done) }()
	t.Cleanup(func() { f.close(t) })
	return f
}

func (f *adminFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests[r.Method+" "+r.URL.Path]++
	stall := f.stall
	f.mu.Unlock()
	if stall != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
		<-stall
	}
	if r.URL.Path == "/load" && r.Method == http.MethodPost {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		if !f.ignoreLoad {
			f.data = append([]byte(nil), data...)
		}
		drop := f.dropLoadReply
		f.dropLoadReply = false
		f.mu.Unlock()
		if drop {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.URL.Path == "/config/" && r.Method == http.MethodGet {
		f.mu.Lock()
		data := append([]byte(nil), f.data...)
		queryBody, queryStatus := f.queryBody, f.queryStatus
		f.mu.Unlock()
		if queryBody != nil {
			w.WriteHeader(queryStatus)
			_, _ = w.Write(queryBody)
			return
		}
		var pretty strings.Builder
		// Formatting differences must not change the current-state receipt.
		var object any
		_ = json.Unmarshal(data, &object)
		b, _ := json.MarshalIndent(object, "", "  ")
		pretty.Write(b)
		_, _ = io.WriteString(w, pretty.String())
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (f *adminFixture) verifier() PeerVerifier {
	var bound *net.UnixConn
	return func(conn *net.UnixConn, expected c.Process, uid uint32) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.verifications++
		if expected != f.identity || uid != uint32(os.Getuid()) {
			return ErrAdmin
		}
		if bound != nil && bound != conn {
			return ErrAdmin
		}
		bound = conn
		return nil
	}
}

func (f *adminFixture) count(method, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[method+" "+path]
}
func (f *adminFixture) acceptCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.accepts }
func (f *adminFixture) verifyCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.verifications }

func (f *adminFixture) close(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	if f.stall != nil {
		close(f.stall)
		f.stall = nil
	}
	f.mu.Unlock()
	if err := f.server.Close(); err != nil {
		t.Error(err)
	}
	select {
	case <-f.done:
	case <-time.After(time.Second):
		t.Error("admin fixture did not join")
	}
}

// closeAdmin is the deferred close of a binding under test. Every internal
// failure path retires the socket through close(), so Close stays idempotent
// and nil; an error here means the descriptor was closed outside that
// bookkeeping, which a discarded result would hide.
func closeAdmin(t *testing.T, a *Admin) {
	t.Helper()
	if err := a.Close(); err != nil {
		t.Errorf("close admin binding: %v", err)
	}
}

func adminProcess() c.Process {
	return c.Process{BootID: "fixture-boot", PID: int32(os.Getpid()), StartTicks: 1, InvocationID: "fixture-invocation", ExeDevice: 1, ExeInode: 2, ExeSHA256: sha256.Sum256([]byte("fixture-exe")), Cgroup: "/fixture-unit"}
}
