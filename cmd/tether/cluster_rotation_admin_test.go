package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// cluster_rotation_admin_test.go (C7) — drives `cluster retire --compromised --require-credential-
// rotation` against a STUB admin socket, pinning the C7-ROT-1 BLOCKER (no F>0 double-create → the
// NOT-SAFE alert + banner always fire) + the #5 / single-mode / typed-confirm invariants.

type stubClusterBackend struct {
	mu          sync.Mutex
	reqs        []adminsock.Request
	retireCalls int
	resp        func(req adminsock.Request, retireN int) adminsock.Response
}

func (s *stubClusterBackend) HandleCluster(req adminsock.Request) adminsock.Response {
	s.mu.Lock()
	s.reqs = append(s.reqs, req)
	if req.Op == adminsock.OpClusterRetire {
		s.retireCalls++
	}
	rn := s.retireCalls
	s.mu.Unlock()
	return s.resp(req, rn)
}

func (s *stubClusterBackend) count(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.reqs {
		if r.Op == op {
			n++
		}
	}
	return n
}

func (s *stubClusterBackend) find(op string) (adminsock.Request, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.reqs {
		if r.Op == op {
			return r, true
		}
	}
	return adminsock.Request{}, false
}

func startStubAdmin(t *testing.T, b *stubClusterBackend) string {
	t.Helper()
	// Short socket path (sun_path ~108 limit): MkdirTemp under the OS tmp with a short name.
	dir, err := os.MkdirTemp("", "c7s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "a.sock")
	srv := adminsock.New(sock, adminsock.Backend{Cluster: b})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Start(ctx) }()
	t.Cleanup(func() { cancel(); _ = srv.Close() })
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(sock); err == nil {
			return sock
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("stub admin socket never appeared")
	return ""
}

// TestC7RetireCompromisedRaisesTrackingAlert is the C7-ROT-1 BLOCKER pin: on an F>0 cluster (where the
// FIRST retire call would create the op), require mode must create the op EXACTLY ONCE and ALWAYS reach
// the alert raise + NOT-SAFE banner. The stub fails any 2nd retire with "already in flight" — the
// pre-fix two-call dance would trip that and skip the alert.
func TestC7RetireCompromisedRaisesTrackingAlert(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, retireN int) adminsock.Response {
		switch req.Op {
		case adminsock.OpClusterStatus:
			return adminsock.Response{Op: req.Op, OK: true, Cluster: &adminsock.ClusterStatusReport{Health: "HEALTHY_HA"}}
		case adminsock.OpClusterRetire:
			if retireN >= 2 {
				return adminsock.Response{Op: req.Op, Error: "an operation (... DRAIN_REQUESTED) is already in flight"}
			}
			return adminsock.Response{Op: req.Op, OK: true, OpID: "op-1"}
		case adminsock.OpClusterAlertRaise:
			return adminsock.Response{Op: req.Op, OK: true}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	}}
	sock := startStubAdmin(t, b)

	cmd := newClusterRetireCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader("brk-b\n")) // typed-confirm
	cmd.SetArgs([]string{"brk-b", "--compromised", "--require-credential-rotation"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("retire require mode failed: %v\n%s", err, out.String())
	}
	if n := b.count(adminsock.OpClusterRetire); n != 1 {
		t.Fatalf("C7-ROT-1: retire op must be created EXACTLY ONCE, got %d calls", n)
	}
	raise, ok := b.find(adminsock.OpClusterAlertRaise)
	if !ok {
		t.Fatal("C7-ROT-1: the NOT-SAFE tracking alert was never raised")
	}
	if raise.AlertKind != "manual" || raise.AlertSeverity != "severe" || raise.AlertLabel != "credrot:brk-b" {
		t.Errorf("alert raise has wrong shape: kind=%q sev=%q label=%q", raise.AlertKind, raise.AlertSeverity, raise.AlertLabel)
	}
	if !strings.Contains(out.String(), "NOT SAFE") {
		t.Errorf("the NOT-SAFE banner must print:\n%s", out.String())
	}
}

// TestC7CompromisedSingleModeFailsFast: require mode against a single-mode admin (cluster not enabled)
// fails fast WITHOUT prompting, raising no alert and creating no op.
func TestC7CompromisedSingleModeFailsFast(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, Error: "cluster mode not enabled", Code: adminsock.CodeClusterNotEnabled}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRetireCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader("brk-b\n"))
	cmd.SetArgs([]string{"brk-b", "--compromised", "--require-credential-rotation"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("require mode in single mode must fail:\n%s", out.String())
	}
	if n := b.count(adminsock.OpClusterRetire); n != 0 {
		t.Errorf("single mode must not create a retire op, got %d", n)
	}
	if n := b.count(adminsock.OpClusterAlertRaise); n != 0 {
		t.Errorf("single mode must not raise an alert, got %d", n)
	}
}

// TestC7CompromisedTypedConfirmNeverYes: require mode aborts when the typed confirm is wrong/empty;
// no op, no alert.
func TestC7CompromisedTypedConfirmNeverYes(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		if req.Op == adminsock.OpClusterStatus {
			return adminsock.Response{Op: req.Op, OK: true, Cluster: &adminsock.ClusterStatusReport{Health: "HEALTHY_HA"}}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: "op-1"}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRetireCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader("WRONG\n")) // not the node id
	cmd.SetArgs([]string{"brk-b", "--compromised", "--require-credential-rotation"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("a wrong typed-confirm must abort:\n%s", out.String())
	}
	if n := b.count(adminsock.OpClusterRetire); n != 0 {
		t.Errorf("a declined confirm must not create a retire op, got %d", n)
	}
	if n := b.count(adminsock.OpClusterAlertRaise); n != 0 {
		t.Errorf("a declined confirm must not raise an alert, got %d", n)
	}
}
