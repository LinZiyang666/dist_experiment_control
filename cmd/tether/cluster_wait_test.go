package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/spf13/cobra"
)

// stubFetch swaps the fetchClusterStatusReport seam for a test and restores it.
func stubFetch(t *testing.T, fn func(string) (*adminsock.ClusterStatusReport, error)) {
	t.Helper()
	orig := fetchClusterStatusReport
	fetchClusterStatusReport = fn
	t.Cleanup(func() { fetchClusterStatusReport = orig })
}

func waitCmd(t *testing.T, ctx context.Context) *cobra.Command {
	t.Helper()
	c := &cobra.Command{}
	c.SetOut(&testBuf{})
	c.SetErr(&testBuf{})
	c.SetContext(ctx)
	return c
}

type testBuf struct{}

func (testBuf) Write(p []byte) (int, error) { return len(p), nil }

// TestB5WaitForConvergeConverges: a pred that reports done returns nil immediately.
func TestB5WaitForConvergeConverges(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "VOTER"}}}, nil
	})
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		return n != nil && n.Phase == "VOTER", ""
	}
	if err := waitForConverge(waitCmd(t, context.Background()), "/sock", "n1", pred, time.Minute, time.Second); err != nil {
		t.Fatalf("converged pred must return nil, got %v", err)
	}
}

// TestB5WaitForConvergeTimeout (plan §F.12): timeout → exit 75 (transient). A fake clock that
// advances past the deadline on the first check makes it deterministic without the 2s ticker.
func TestB5WaitForConvergeTimeout(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "CATCHING_UP"}}}, nil
	})
	base := time.Date(2026, 6, 25, 0, 0, 0, 0, time.UTC)
	calls := 0
	orig := nowFunc
	nowFunc = func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * time.Hour) // advances every call → past any tiny deadline
	}
	t.Cleanup(func() { nowFunc = orig })

	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		return false, ""
	}
	err := waitForConverge(waitCmd(t, context.Background()), "/sock", "n1", pred, time.Nanosecond, time.Second)
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitTransient {
		t.Fatalf("timeout must be exit 75 (transient), got %v", err)
	}
}

// TestB5WaitForConvergeCancel (plan §F.12): a cancelled context → exit 75, promptly (no ticker wait).
func TestB5WaitForConvergeCancel(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "CATCHING_UP"}}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		return false, ""
	}
	err := waitForConverge(waitCmd(t, ctx), "/sock", "n1", pred, 0, time.Second) // timeout 0 = no deadline
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitTransient {
		t.Fatalf("ctx-cancel must be exit 75 (transient), got %v", err)
	}
}

// TestB5WaitForConvergeFailTerminal: a failure-terminal pred (VOTER_ADD_FAILED) returns immediately
// with a non-transient error (NOT exit 75 — it must not be retried).
func TestB5WaitForConvergeFailTerminal(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "VOTER_ADD_FAILED"}}}, nil
	})
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		if n != nil && n.Phase == "VOTER_ADD_FAILED" {
			return false, "node entered VOTER_ADD_FAILED"
		}
		return false, ""
	}
	err := waitForConverge(waitCmd(t, context.Background()), "/sock", "n1", pred, time.Minute, time.Second)
	if err == nil {
		t.Fatal("failure-terminal must return an error")
	}
	var ee *ExitError
	if errors.As(err, &ee) && ee.Class == exitTransient {
		t.Fatalf("failure-terminal must NOT be exit 75 (transient): %v", err)
	}
}

// TestB5WaitForConvergeFirstMatch: a roster listing the node TWICE (roster row + orphan-voter
// append) passes the FIRST match to the predicate deterministically.
func TestB5WaitForConvergeFirstMatch(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{
			{NodeID: "n1", Phase: "VOTER"},            // first
			{NodeID: "n1", Phase: "VOTER_ADD_FAILED"}, // orphan duplicate
		}}, nil
	})
	var sawPhase string
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		if n != nil {
			sawPhase = n.Phase
		}
		return true, "" // converge so it returns after one observation
	}
	_ = waitForConverge(waitCmd(t, context.Background()), "/sock", "n1", pred, time.Minute, time.Second)
	if sawPhase != "VOTER" {
		t.Fatalf("pred must see the FIRST matching row (VOTER), saw %q", sawPhase)
	}
}

// TestB5RenderTakeoverPlanNoMutation (plan §F.18, BLOCKER-tier): --plan rendering reads the conf
// but writes NOTHING — the file bytes + mtime are unchanged and no .bak is created. `changed` is
// true when the merged conf differs from the current bytes.
func TestB5RenderTakeoverPlanNoMutation(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "nats.conf")
	original := []byte("host: 127.0.0.1\nport: 4222\n")
	if err := os.WriteFile(confPath, original, 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(confPath)
	if err != nil {
		t.Fatal(err)
	}

	peers := []natsconf.Broker{{ServerName: "brk-a", NkeyPub: "Uabc", RouteURL: "nats://10.0.0.1:6222"}}
	cmd := waitCmd(t, context.Background())
	if err := renderTakeoverPlan(cmd, confPath, "brk-a", "127.0.0.1:4222", "/var/lib/js", peers, "MERGED DIFFERENT CONTENT", "ok", true); err != nil {
		t.Fatalf("renderTakeoverPlan (json): %v", err)
	}
	if err := renderTakeoverPlan(cmd, confPath, "brk-a", "127.0.0.1:4222", "/var/lib/js", peers, "MERGED DIFFERENT CONTENT", "ok", false); err != nil {
		t.Fatalf("renderTakeoverPlan (text): %v", err)
	}

	after, err := os.Stat(confPath)
	if err != nil {
		t.Fatal(err)
	}
	now, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(now) != string(original) {
		t.Fatal("--plan must NOT modify the conf bytes")
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("--plan must NOT touch the conf mtime")
	}
	if baks, _ := filepath.Glob(confPath + ".bak*"); len(baks) != 0 {
		t.Fatalf("--plan must NOT create a .bak, found %v", baks)
	}
}
