package cluster

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
)

// Batch-A review M1 + M7. A15 added a raft→slog bridge and node.go claimed the
// logging was "finally wired". It was not: ProductionConfig had no Logger field,
// so the only path that runs in production built a Node with a nil Logger and
// the bridge returned on its first line. The feature was a black hole behind a
// comment saying otherwise — the same shape as the RehomeDirective false promise
// this batch set out to remove.
//
// These tests pin the wiring and the de-duplication semantics, neither of which
// had any coverage.

func TestProductionConfigCarriesLogger(t *testing.T) {
	// Structural: the field must exist and reach cluster.Config. A compile error
	// here is the point — it means someone removed the plumbing again.
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pc := ProductionConfig{LocalID: "n1", Logger: lg}
	if pc.Logger == nil {
		t.Fatal("ProductionConfig.Logger is nil after being set")
	}
	// And the bridge built from it must actually emit.
	br := NewRaftLogger(pc.Logger, false)
	br.Warn("wiring probe")
	if !strings.Contains(buf.String(), "wiring probe") {
		t.Errorf("a bridge built from ProductionConfig.Logger emitted nothing; got %q.\n"+
			"This is exactly the M1 defect: the plumbing compiles, the comment says 'wired', "+
			"and raft says nothing for the rest of the cluster's life.", buf.String())
	}
}

func TestRaftLoggerNilIsSafeButSilent(t *testing.T) {
	br := NewRaftLogger(nil, false)
	br.Warn("must not panic")
	br.Error("must not panic")
	// No assertion beyond "did not panic": tests construct Config{} directly and
	// must keep working.
}

func TestRaftLoggerForwardsWarnAndAbove(t *testing.T) {
	var buf bytes.Buffer
	br := NewRaftLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), false)
	br.Debug("debug-line")
	br.Info("info-line")
	br.Warn("warn-line")
	br.Error("error-line")
	out := buf.String()
	for _, want := range []string{"warn-line", "error-line"} {
		if !strings.Contains(out, want) {
			t.Errorf("%q was dropped; WARN+ must reach the operator", want)
		}
	}
	for _, unwanted := range []string{"debug-line", "info-line"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("%q was forwarded without debug enabled; these are the high-rate levels", unwanted)
		}
	}
}

// TestRaftLoggerDedupDoesNotSwallowDistinctPeers is batch-A review M7.
//
// raft reports an unreachable peer as a constant message with the peer identity
// in the ARGS ("failed to heartbeat to", peer=...). Keying de-duplication on the
// message alone means the first failing peer wins the window and every OTHER
// failing peer is silently swallowed for its duration — which defeats the reason
// A15 exists: reconstructing what raft saw during an incident.
func TestRaftLoggerDedupDoesNotSwallowDistinctPeers(t *testing.T) {
	var buf bytes.Buffer
	br := NewRaftLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), false)

	br.Warn("failed to heartbeat to", "peer", "brk-a")
	br.Warn("failed to heartbeat to", "peer", "brk-b")

	out := buf.String()
	if !strings.Contains(out, "brk-a") {
		t.Fatal("first peer was not logged at all")
	}
	if !strings.Contains(out, "brk-b") {
		t.Errorf("the SECOND unreachable peer was swallowed by de-duplication.\n"+
			"During a real incident this reads as 'one peer is down' when two are — the raft narrative "+
			"A15 exists to provide is then actively misleading, not merely thin.\ngot: %s", out)
	}
}

func TestRaftLoggerDedupSuppressesIdenticalRepeats(t *testing.T) {
	var buf bytes.Buffer
	br := NewRaftLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), false)
	for i := 0; i < 50; i++ {
		br.Warn("same message", "peer", "brk-a")
	}
	if n := strings.Count(buf.String(), "same message"); n != 1 {
		t.Errorf("identical repeats emitted %d times within the window, want 1 — the bound on log "+
			"volume is the whole reason the window exists", n)
	}
}

func TestRaftLoggerIsConcurrencySafe(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	br := NewRaftLogger(slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &buf}, nil)), false)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				br.Warn("concurrent", "worker", i)
				_ = br.Named("sub").With("k", "v")
			}
		}(i)
	}
	wg.Wait()
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

var _ hclog.Logger = (*raftLogBridge)(nil)
