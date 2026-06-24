package cluster_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/session"
)

// TestProcInsertIdempotentAtApply is the audit-M8 / port-plan-F1 regression: OpProcCreate
// must be idempotent against a duplicate pid re-proposed at a NEW raft index. PlanInsert
// bakes INSERT OR IGNORE on the pid PRIMARY KEY, so the second apply is a deterministic
// RowsAffected==0 no-op on every replica — NOT a PK violation that genericExecApplier
// surfaces as a plain error, which fsm.Apply would turn into retry->panic, bricking every
// replica on log replay (VerbProcInsert forwards with reqID="" so the 0011 ledger does NOT
// dedup it; the OR IGNORE is the sole idempotency anchor). A forwarder retry of a
// committed-but-ack-lost proc insert (which the Forwarder contract explicitly invites) hits
// this exact path. Before the fix the second Propose's Apply panicked the FSM goroutine,
// crashing this test; after it, both Proposes succeed and exactly one row exists.
func TestProcInsertIdempotentAtApply(t *testing.T) {
	n, _ := newTestNode(t)
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)

	mustPropose := func(plan func(*sql.DB) (*cluster.Command, error)) {
		t.Helper()
		if err := n.Propose(plan); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	mustPropose(func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, "lab", "lab", "SHA256:owner", "ph", now)
	})
	mustPropose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "lab-1", ProtoVersion: 2}, now)
	})

	p := proc.Process{
		PID:         "01J0AGENTMINTEDULIDPID001",
		SID:         "lab",
		NID:         "lab-1",
		Argv:        []string{"sleep", "1"},
		Cwd:         "/tmp",
		StartedAt:   now,
		StartedByFP: "SHA256:caller",
		BootID:      "boot-1",
	}

	// First insert commits the row.
	mustPropose(func(db *sql.DB) (*cluster.Command, error) { return proc.PlanInsert(db, p) })
	// Second insert of the SAME pid at a NEW raft index must be a deterministic Apply
	// no-op, NOT a fail-stop panic. (A bare INSERT here would PK-violate and panic the FSM.)
	mustPropose(func(db *sql.DB) (*cluster.Command, error) { return proc.PlanInsert(db, p) })

	var count int
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT COUNT(*) FROM processes WHERE pid=?`, p.PID).Scan(&count)
	}); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if count != 1 {
		t.Fatalf("duplicate OpProcCreate must leave exactly one row, got %d", count)
	}
}
