package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// TestD8PlanAlertSignalTransition (review m2): the leader-side VerbAlertSignal decision is a
// pure transition gate — raise only when active && !current, clear only when !active &&
// current, and a NIL command (no raft write) otherwise. The nil-command idle path is the
// idle-zero-writes property the level-triggered disk re-assert depends on.
func TestD8PlanAlertSignalTransition(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	sig := func(active bool) AlertSignalPayload {
		return AlertSignalPayload{Kind: cluster.AlertKindDiskPressure, Node: "node-1", Active: active, Severity: cluster.AlertSeverityInfo, Message: "disk"}
	}
	apply := func(cmd *cluster.Command) {
		if cmd != nil {
			if err := cluster.ExecCommand(db, cmd); err != nil {
				t.Fatalf("exec: %v", err)
			}
		}
	}
	key := cluster.DedupKeyNode(cluster.AlertKindDiskPressure, "node-1")

	// active=true, no row → raise (non-nil command).
	cmd, err := planAlertSignal(db, sig(true), now)
	if err != nil || cmd == nil {
		t.Fatalf("first raise: cmd=%v err=%v, want non-nil/nil", cmd, err)
	}
	apply(cmd)
	if a, _ := cluster.IsAlertActive(db, key); !a {
		t.Fatal("disk_pressure not raised")
	}
	// active=true, row already ACTIVE → NIL command (idle-zero-write).
	cmd, err = planAlertSignal(db, sig(true), now)
	if err != nil || cmd != nil {
		t.Fatalf("idle re-assert: cmd=%v err=%v, want NIL (no raft write)", cmd, err)
	}
	// active=false, row ACTIVE → clear.
	cmd, err = planAlertSignal(db, sig(false), now)
	if err != nil || cmd == nil {
		t.Fatalf("clear: cmd=%v err=%v, want non-nil", cmd, err)
	}
	apply(cmd)
	if a, _ := cluster.IsAlertActive(db, key); a {
		t.Fatal("disk_pressure not cleared")
	}
	// active=false, no row → NIL command.
	cmd, err = planAlertSignal(db, sig(false), now)
	if err != nil || cmd != nil {
		t.Fatalf("clear-on-absent: cmd=%v err=%v, want NIL", cmd, err)
	}
}

// TestD8PlanAlertSignalRejectsBadEnum (review m1): an out-of-enum kind/severity returns a NIL
// command (no raft write, no FSM poison) rather than baking a row that would fail the 0009
// CHECK on every replica and brick the cluster on replay.
func TestD8PlanAlertSignalRejectsBadEnum(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	for _, p := range []AlertSignalPayload{
		{Kind: "quorum_lost", Node: "n", Active: true, Severity: cluster.AlertSeverityInfo},  // not store-backed
		{Kind: cluster.AlertKindDiskPressure, Node: "n", Active: true, Severity: "critical"}, // bad severity
		{Kind: "", Node: "n", Active: true, Severity: cluster.AlertSeverityInfo},
	} {
		cmd, err := planAlertSignal(db, p, now)
		if err != nil || cmd != nil {
			t.Fatalf("bad enum %+v: cmd=%v err=%v, want NIL/nil (rejected, no poison)", p, cmd, err)
		}
	}
}
