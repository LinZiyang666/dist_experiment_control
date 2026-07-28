package p9_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// origin: p9_review_risk_test.go (renamed in B6) — docs/reviews/p9-review.md
func TestReviewAdminSocketDoesNotUnlinkActiveSocket(t *testing.T) {
	socketPath := adminSocketPath(t)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	s1 := adminsock.New(socketPath, adminsock.Backend{DB: openDB(t), Logger: silentLog()})
	if err := s1.Start(ctx1); err != nil {
		t.Fatalf("start first admin socket: %v", err)
	}
	defer func() { _ = s1.Close() }()

	if resp, err := (&adminsock.Client{Path: socketPath}).Call(adminsock.Request{Op: adminsock.OpSessions}); err != nil || resp.Error != "" {
		t.Fatalf("first admin socket not reachable: resp=%+v err=%v", resp, err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	s2 := adminsock.New(socketPath, adminsock.Backend{DB: openDB(t), Logger: silentLog()})
	err := s2.Start(ctx2)
	if err == nil {
		t.Fatal("second admin socket unexpectedly started on an active socket path; it must not unlink a live broker socket")
	}
	cancel2()
}

func TestReviewAdminSocketChmodsExistingParent(t *testing.T) {
	// Pre-create the parent at 0o755 so Start's defensive re-chmod is
	// exercised. Short temp dir (not t.TempDir) — see adminSocketPath.
	base, err := os.MkdirTemp("", "tsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	dir := filepath.Join(base, "tether-run")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(dir, "admin.sock")

	ctx, cancel := context.WithCancel(context.Background())
	s := adminsock.New(socketPath, adminsock.Backend{DB: openDB(t), Logger: silentLog()})
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start admin socket: %v", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			cancel()
			_ = s.Close()
		}
	}()

	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if mode := fi.Mode().Perm(); mode != 0o700 {
		cleanup = false
		t.Fatalf("admin socket parent mode: got %o want 0700 even when parent pre-exists", mode)
	}
}
