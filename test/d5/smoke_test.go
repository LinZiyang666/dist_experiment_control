//go:build d5_integration

package d5_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proc"
)

// staticSIDs returns a ListSIDs func over a fixed set (the leader-local session set the
// reconcile pass would otherwise read from SQLite).
func staticSIDs(sids ...string) func(ctx context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) { return sids, nil }
}

// commitReconcile commits an audit-only OpReconcileBatch (empty Body, non-empty Aux) on
// node nd, the entry the D5 publisher replays. reqID="" => raft-index-keyed dedup id;
// non-empty => reqID-keyed (the forwarded-reconcile case).
func commitReconcile(t *testing.T, nd *cluster.Node, reqID, sid string, procs []proc.ReconProcAudit, ports []proc.ReconPortAudit) {
	t.Helper()
	cmd, err := proc.PlanReconcileBatch(proc.ReconcileBatchInput{SID: sid, ProcAudit: procs, PortAudit: ports})
	if err != nil {
		t.Fatalf("plan reconcile: %v", err)
	}
	if cmd == nil {
		t.Fatal("plan reconcile produced a nil command (nothing to replay)")
	}
	if err := nd.ProposeWithReqID(reqID, func(_ *sql.DB) (*cluster.Command, error) { return cmd, nil }); err != nil {
		t.Fatalf("propose reconcile: %v", err)
	}
}

func newPublisher(c *cluster5, idx int, sids ...string) *broker.AuditPublisher {
	return broker.NewAuditPublisher(broker.AuditPublisherConfig{
		Node:     c.nodes[idx],
		JS:       c.js[idx],
		ListSIDs: staticSIDs(sids...),
	})
}

func auditProc(nid, pid, kind string) proc.ReconProcAudit {
	return proc.ReconProcAudit{NID: nid, PID: pid, Kind: kind, Ts: time.Now()}
}

// TestD5Smoke validates the 3-layer harness end-to-end: a clustered JetStream forms, an
// R=3 history stream is created, an audit-only ReconcileBatch commits through raft, the
// leader publisher replays + publishes its audit to history-<sid>, and a re-run dedups.
func TestD5Smoke(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ensureHistory(t, c, li, "lab", 3)
	if !waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3) {
		t.Fatalf("history-lab never reached R3 (have %d)", streamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab")))
	}

	commitReconcile(t, c.nodes[li], "", "lab",
		[]proc.ReconProcAudit{auditProc("lab-1", "01aaa", "reconciled_closed"), auditProc("lab-1", "01bbb", "killed_orphan")},
		[]proc.ReconPortAudit{{NID: "lab-1", Port: 8080, Name: "web", LocalPort: 3000, Kind: "reconciled", Ts: time.Now()}})

	p := newPublisher(c, li, "lab")
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("publish once: %v", err)
	}
	const want = 3 // 2 proc + 1 port
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")) == want }) {
		t.Fatalf("history-lab msgs = %d, want %d", streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")), want)
	}

	// Re-run: identical dedup ids => JetStream collapses => no growth (R-8/R-12).
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("publish once (re-run): %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")); got != want {
		t.Fatalf("re-run double-published: history-lab msgs = %d, want %d (dedup failed)", got, want)
	}
}
