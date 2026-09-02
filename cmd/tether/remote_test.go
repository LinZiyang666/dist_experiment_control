package main

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: g7_remote_test.go (renamed in B6)
//
// TestG7FoldProxyHomeCounts: the --homes --remote fold dedups by NodeID (a flapping broker answering
// twice must not double-count), sorts by NodeID, and carries ONLY aggregate counts (no per-SID rows →
// no cross-session topology leak). Replies with an empty NodeID are dropped.
func TestG7FoldProxyHomeCounts(t *testing.T) {
	replies := []proto.ClusterHealthResp{
		{NodeID: "brk-b", ProxyHomeCount: 5},
		{NodeID: "brk-a", ProxyHomeCount: 1},
		{NodeID: "brk-b", ProxyHomeCount: 5}, // duplicate (flap) — must collapse, not add
		{NodeID: "", ProxyHomeCount: 9},      // no id — dropped
	}
	got := foldProxyHomeCounts(replies)
	if len(got) != 2 {
		t.Fatalf("want 2 distinct brokers, got %d (%v)", len(got), got)
	}
	if got[0].NodeID != "brk-a" || got[1].NodeID != "brk-b" {
		t.Fatalf("must be sorted by NodeID, got %v", got)
	}
	if got[0].Homes != 1 || got[1].Homes != 5 {
		t.Fatalf("home counts wrong (a flapping duplicate must not double-count): %v", got)
	}
}

func TestG7FoldProxyHomeCountsEmpty(t *testing.T) {
	if got := foldProxyHomeCounts(nil); len(got) != 0 {
		t.Fatalf("no replies → empty distribution, got %v", got)
	}
}

// remote_test.go (formerly g7_remote_test.go) — G7b #16: lock the ctl --remote exit-code contract (0/2/3, never 1), especially
// exit 3 under force_single, so a monitor can catch an emergency by exit code. The exit-3 chain is
// ALREADY wired at HEAD (cluster_health.go stamps ForceSingleActive → summarizeClusterHealth →
// ctlExitCode returns 3); this is a verify-and-lock regression, not a reimplementation. The 2026-06-29
// exit-0 field observation was a stale v0.0.0-dev laptop binary (gotcha #17), not a HEAD bug.

func TestG7CtlExitCodeContract(t *testing.T) {
	cases := []struct {
		name string
		s    ctlClusterSummary
		want int
	}{
		{"no reply → 0 (single-broker laptop must not alarm)", ctlClusterSummary{AnyReply: false}, 0},
		{"force_single → 3 (emergency, monitor catches by code)", ctlClusterSummary{AnyReply: true, ForceSingleActive: true}, 3},
		{"force_single wins over writable", ctlClusterSummary{AnyReply: true, ForceSingleActive: true, WritableLeaderSeen: true}, 3},
		{"force_single wins over all-stale", ctlClusterSummary{AnyReply: true, ForceSingleActive: true, AllStale: true}, 3},
		{"writable leader → 0", ctlClusterSummary{AnyReply: true, WritableLeaderSeen: true}, 0},
		{"all-stale read-only → 2", ctlClusterSummary{AnyReply: true, AllStale: true}, 2},
		{"electing (transient) → 0", ctlClusterSummary{AnyReply: true}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ctlExitCode(c.s)
			if got != c.want {
				t.Fatalf("ctlExitCode = %d, want %d", got, c.want)
			}
			if got == 1 {
				t.Fatal("ctl --remote must NEVER return exit 1 (no DEGRADED in the ctl taxonomy)")
			}
			if got != 0 && got != 2 && got != 3 {
				t.Fatalf("ctl --remote exit must be in {0,2,3}, got %d", got)
			}
		})
	}
}
