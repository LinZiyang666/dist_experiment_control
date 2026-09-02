package broker

import (
	"context"
	"database/sql"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// alertFake is a controllable alertClusterReader for the reconcile-loop branch tests.
type alertFake struct {
	leader bool
	stale  bool
	voters int
}

func (f *alertFake) IsLeader() bool                    { return f.leader }
func (f *alertFake) LeaderContactStale(time.Time) bool { return f.stale }
func (f *alertFake) NumVoters() (int, error)           { return f.voters, nil }

func reconTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func activeKeys(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	keys, err := cluster.ActiveAlertKeys(db)
	if err != nil {
		t.Fatalf("active keys: %v", err)
	}
	return keys
}

// newReconciler wires a reconciler whose Propose simulates the leader Apply (plan→ExecCommand
// on the same DB) and counts proposals so a test can assert idle-zero-writes.
func newReconciler(db *sql.DB, f *alertFake, observe func(context.Context) (ReplicaReport, error), count *int) *AlertReconciler {
	return NewAlertReconciler(AlertReconcilerConfig{
		Node: f, DB: db, Observe: observe,
		Now: func() time.Time { return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC) },
		Propose: func(plan func(*sql.DB) (*cluster.Command, error)) error {
			*count++
			cmd, err := plan(db)
			if err != nil {
				return err
			}
			if cmd == nil {
				return nil
			}
			return cluster.ExecCommand(db, cmd)
		},
	})
}

func reportDegraded() ReplicaReport {
	return ReplicaReport{Observed: true, Streams: []jsstream.StreamReplicaState{{Name: "history-lab", Target: 3, Actual: 1, Ready: false}}}
}
func reportAtTarget() ReplicaReport {
	return ReplicaReport{Observed: true, Streams: []jsstream.StreamReplicaState{{Name: "history-lab", Target: 3, Actual: 3, Ready: true}}}
}
func reportUnobserved() ReplicaReport { return ReplicaReport{Observed: false} }

func TestD9AlertSignalReRaiseSameTimestampDoesNotCollide(t *testing.T) {
	db := reconTestDB(t)
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	p := AlertSignalPayload{
		Kind:     cluster.AlertKindDiskPressure,
		Node:     "node-1",
		Active:   true,
		Severity: cluster.AlertSeverityInfo,
		Message:  "disk pressure",
	}
	cmd, err := planAlertSignal(db, p, now)
	if err != nil {
		t.Fatalf("first raise plan: %v", err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatalf("first raise exec: %v", err)
	}
	p.Active = false
	cmd, err = planAlertSignal(db, p, now)
	if err != nil {
		t.Fatalf("clear plan: %v", err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatalf("clear exec: %v", err)
	}
	p.Active = true
	cmd, err = planAlertSignal(db, p, now)
	if err != nil {
		t.Fatalf("second raise plan: %v", err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatalf("second raise exec (same timestamp): %v", err)
	}
	var total, active int
	if err := db.QueryRow(`SELECT COUNT(*) FROM alerts`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM alerts WHERE state='ACTIVE'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if total != 2 || active != 1 {
		t.Fatalf("alerts total/active = %d/%d, want 2/1", total, active)
	}
}

// TestD8AlertReconcileReplicationDegraded drives the core lifecycle + the clear-condition fix:
// raise on Degraded, NO flip on a transient unobserved pass (must not false-clear), clear ONLY
// on a positive AllAtTarget observation.
func TestD8AlertReconcileReplicationDegraded(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 3}
	rep := reportDegraded()
	var n int
	r := newReconciler(db, f, func(context.Context) (ReplicaReport, error) { return rep, nil }, &n)
	key := cluster.DedupKeyGlobal(cluster.AlertKindReplicationDegraded)

	// Degraded → raise.
	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if !activeKeys(t, db)[key] {
		t.Fatal("replication_degraded not raised on Degraded report")
	}
	// Transient unobserved pass → must NOT clear (the bug a !Degraded() clear would cause).
	rep = reportUnobserved()
	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if !activeKeys(t, db)[key] {
		t.Fatal("replication_degraded FALSE-CLEARED on a transient unobserved pass")
	}
	// Positive AllAtTarget observation → clear.
	rep = reportAtTarget()
	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if activeKeys(t, db)[key] {
		t.Fatal("replication_degraded not cleared on AllAtTarget")
	}
}

// TestD8AlertReconcileIdleZeroWrites: when the desired state already matches current, a pass
// proposes NOTHING (preserves D5 idle-zero-writes — a WHERE NOT EXISTS no-op would still be a
// committed raft entry).
func TestD8AlertReconcileIdleZeroWrites(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 3}
	rep := reportDegraded()
	var n int
	r := newReconciler(db, f, func(context.Context) (ReplicaReport, error) { return rep, nil }, &n)

	if err := r.ReconcileAlertsOnce(context.Background()); err != nil { // raises once
		t.Fatalf("pass: %v", err)
	}
	n = 0
	for i := 0; i < 3; i++ { // already degraded + still degraded → no transition
		if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
			t.Fatalf("pass: %v", err)
		}
	}
	if n != 0 {
		t.Fatalf("idle reconcile proposed %d raft writes, want 0", n)
	}
}

// TestD8AlertReconcileBelowQuorum: below_quorum raises at exactly 2 voters (F==0) and clears
// once a third voter joins.
func TestD8AlertReconcileBelowQuorum(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 2}
	var n int
	r := newReconciler(db, f, nil, &n) // no Observe → replication_degraded disabled
	key := cluster.DedupKeyGlobal(cluster.AlertKindBelowQuorum)

	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if !activeKeys(t, db)[key] {
		t.Fatal("below_quorum not raised at 2 voters")
	}
	f.voters = 3
	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if activeKeys(t, db)[key] {
		t.Fatal("below_quorum not cleared at 3 voters")
	}
}

// TestD8AlertReconcileRunCancels is the FOCUSED leak gate for the reconcile loop (the live
// 3-node cluster drill can't measure this — its goroutine count tracks cluster background
// work). The Run goroutine must exit promptly on ctx cancel.
func TestD8AlertReconcileRunCancels(t *testing.T) {
	db := reconTestDB(t)
	var n int
	r := newReconciler(db, &alertFake{leader: false, voters: 3}, nil, &n)
	before := runtime.NumGoroutine()
	// Five start/cancel rounds, not one: the shared gate's ±2 tolerance would swallow a
	// per-Run leak of one goroutine at a single exercise (test/determinism/leak_assert_shape_test.go
	// header for the arithmetic). The inline poll this test used to carry was invisible to that
	// gate. origin: docs/reviews/test-system-overhaul-plan.md B5 (D3).
	for round := 0; round < 5; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		go r.Run(ctx)
		time.Sleep(60 * time.Millisecond) // let it tick at least once
		cancel()
	}
	testharness.AssertNoGoroutineLeak(t, "alert reconciler Run", before)
}

// TestD8SignalDiskAlertInert pins the build-and-prove invariant: with no alert sink attached
// (production), signalDiskAlert is a no-op (the disk monitor surfaces pressure only via the
// existing pubSysEvent) — proving the d8 guard exclusion of disk.go's seam read is non-vacuous.
func TestD8SignalDiskAlertInert(t *testing.T) {
	b := &Broker{}           // alertSink == nil
	b.signalDiskAlert(true)  // must not panic
	b.signalDiskAlert(false) // must not panic
	got := 0
	b.alertSink = func(bool) { got++ }
	b.signalDiskAlert(true)
	if got != 1 {
		t.Fatalf("after AttachAlertSink-equivalent, signalDiskAlert fired %d times, want 1", got)
	}
}

// TestD8AlertReconcileNonLeaderNoOp: a non-leader (or fence-stale) node does nothing — no
// reads, no proposes.
func TestD8AlertReconcileNonLeaderNoOp(t *testing.T) {
	db := reconTestDB(t)
	var n int
	r := newReconciler(db, &alertFake{leader: false, voters: 2}, func(context.Context) (ReplicaReport, error) { return reportDegraded(), nil }, &n)
	if err := r.ReconcileAlertsOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if n != 0 || len(activeKeys(t, db)) != 0 {
		t.Fatalf("non-leader proposed %d / raised %d alerts, want 0/0", n, len(activeKeys(t, db)))
	}
}
