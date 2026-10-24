package caddyconfig

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// A reload may retire its admin HTTP connection after committing the config.
// A new connection is evidence only when its peer is the original incarnation.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F1
func TestCaddyAdminReloadVerifiesFreshReadback(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("infra admin is unsupported on this platform")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdminReloadReadback(t)
	}
	testharness.AssertNoGoroutineLeak(t, "reload readback", g)
	testharness.AssertNoFDLeak(t, "reload readback", fd, 0)
}

func exerciseAdminReloadReadback(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"close-reply", "lost-reply", "different-digest", "different-pid", "different-ticks", "different-invocation", "expired-deadline"} {
		t.Run(mode, func(t *testing.T) {
			opts := certificateOptions(t)
			cfg, err := Prepare(supportedConfig(t), opts)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			f := startReloadAdminFixture(t, opts.AdminSocket, mode, cancel)
			defer f.close(t)
			// Unlike the single-FD fixture, this verifier permits another FD only
			// after checking every original identity field again.
			verify := func(_ *net.UnixConn, expected c.Process, uid uint32) error {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.verifications++
				if expected != f.identity || uid != uint32(os.Getuid()) {
					return ErrAdmin
				}
				return nil
			}
			a, err := OpenAdmin(t.Context(), opts.AdminSocket, adminProcess(), uint32(os.Getuid()), verify)
			if err != nil {
				t.Fatal(err)
			}
			defer closeAdmin(t, a)
			got, err := a.Load(ctx, cfg)
			if mode == "close-reply" || mode == "lost-reply" {
				if err != nil || got != cfg.Digest() {
					t.Fatalf("identical config on verified original peer was not success: %x: %v", got, err)
				}
				if f.acceptCount() != 2 || f.count(http.MethodGet, "/config/") != 2 || f.verifyCount() < 5 {
					t.Fatal("success lacks a freshly verified readback")
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) || got != (c.Digest{}) {
				t.Fatalf("unproven reload signed success: %x: %v", got, err)
			}
			if f.count(http.MethodPost, "/load") != 1 || f.count(http.MethodPost, "/stop") != 0 {
				t.Fatal("readback replayed a mutation or stopped an invocation")
			}
			if mode == "different-pid" || mode == "different-ticks" || mode == "different-invocation" {
				if f.count(http.MethodGet, "/config/") != 1 {
					t.Fatal("successor received a request before peer verification")
				}
			}
		})
	}
}

func startReloadAdminFixture(t *testing.T, socket, mode string, cancel context.CancelFunc) *adminFixture {
	t.Helper()
	if err := os.Chmod(filepath.Dir(socket), 0700); err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0600); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	f := &adminFixture{listener: l, done: make(chan struct{}), data: []byte(`{}`), requests: make(map[string]int), identity: adminProcess()}
	f.dropLoadReply, f.ignoreLoad = mode == "lost-reply", mode == "different-digest"
	f.server = &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Connection", "close")
			f.mu.Lock()
			switch mode {
			case "different-pid":
				f.identity.PID++
			case "different-ticks":
				f.identity.StartTicks++
			case "different-invocation":
				f.identity.InvocationID += "-successor"
			}
			f.mu.Unlock()
		}
		f.serve(w, r)
		if r.Method == http.MethodPost && mode == "expired-deadline" {
			cancel()
		}
	}), ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			f.mu.Lock()
			f.accepts++
			f.mu.Unlock()
		}
	}}
	go func() { _ = f.server.Serve(l); close(f.done) }()
	t.Cleanup(func() { f.close(t) })
	return f
}
