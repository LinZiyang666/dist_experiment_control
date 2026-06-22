//go:build d5_integration

package d5_test

import (
	"context"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proc"
)

// TestD5KillDuringExpandNoAuditLoss (§13.9 exit / E-J1): with an R3 audit stream and audit
// actively flowing, a FOLLOWER (a JS replica + raft voter) is killed. The surviving
// quorum-2 cluster must (a) keep its raft leader, (b) keep the R3 stream writable, (c) keep
// publishing with a BOUNDED stall (PublishOnce still returns), and (d) lose NO audit — every
// committed record lands exactly once across the kill. This is the kill-during-reconfig
// no-loss exit assertion (the degraded stream survives a replica loss at N=3).
func TestD5KillDuringExpandNoAuditLoss(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := jsstream.HistoryStreamName("lab")

	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 3); err != nil {
		t.Fatalf("ensure history R3: %v", err)
	}
	if !waitForStreamReplicas(t, c.js[li], name, 3) {
		t.Fatal("history-lab never reached R3")
	}

	p := newPublisher(c, li, "lab")
	// Batch 1: commit + publish before the kill.
	for i := 0; i < 3; i++ {
		commitReconcile(t, c.nodes[li], "", "lab",
			[]proc.ReconProcAudit{auditProc("lab-1", pid("a", i), "reconciled_closed")}, nil)
	}
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("pre-kill publish: %v", err)
	}
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], name) == 3 }) {
		t.Fatalf("pre-kill msgs = %d, want 3", streamMsgs(t, c.js[li], name))
	}
	cpBefore, _ := c.nodes[li].AuditPublishedIndex() // M3: must advance to cover post-kill audit

	// Kill a FOLLOWER (its NATS server = a JS replica, AND its raft node = a voter). The
	// leader keeps quorum (2 of 3); the R3 stream goes degraded-but-writable. We do this while
	// re-asserting an EnsureHistoryStream(R3) raise in flight, so the kill lands during a
	// reconfig pass (kill-during-expand), not pure steady state (internal review M3).
	fi := (li + 1) % 3
	go func() { _ = jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 3) }() // reconfig in flight
	c.servers[fi].Shutdown()
	c.servers[fi].WaitForShutdown()
	_ = c.nodes[fi].Shutdown()
	c.nodes[fi] = nil

	// The leader must still be the leader (no quorum loss).
	if !waitForCond(5*time.Second, func() bool { return c.nodes[li] != nil && c.nodes[li].IsLeader() }) {
		t.Fatal("leader lost leadership after a single follower death (quorum should survive)")
	}

	// Batch 2: commit + publish AFTER the kill — bounded stall (PublishOnce returns), no loss.
	for i := 0; i < 3; i++ {
		commitReconcile(t, c.nodes[li], "", "lab",
			[]proc.ReconProcAudit{auditProc("lab-1", pid("b", i), "reconciled_closed")}, nil)
	}
	t0 := time.Now()
	if !waitForCond(15*time.Second, func() bool {
		_, err := p.PublishOnce(ctx)
		return err == nil && streamMsgs(t, c.js[li], name) == 6
	}) {
		t.Fatalf("post-kill audit loss / unbounded stall: msgs = %d, want 6", streamMsgs(t, c.js[li], name))
	}
	if stall := time.Since(t0); stall > 12*time.Second {
		t.Fatalf("post-kill stall %s exceeds the bound", stall)
	}
	// M3: the checkpoint advanced to cover the post-kill audit (the queued writes were durably
	// published, not dropped) — read via the checkpoint, not just stream depth.
	cpAfter, _ := c.nodes[li].AuditPublishedIndex()
	if cpAfter <= cpBefore {
		t.Fatalf("checkpoint did not advance across the kill: %d -> %d (post-kill audit not durably published)", cpBefore, cpAfter)
	}
}

func pid(prefix string, i int) string { return "01" + prefix + string(rune('0'+i)) }
