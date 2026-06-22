package cluster

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/hashicorp/raft"
)

// logCmd builds a LogCommand entry carrying cmd at (index, term).
func logCmd(t *testing.T, index, term uint64, cmd *Command) *raft.Log {
	t.Helper()
	data, err := cmd.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: data}
}

func metaCmd(t *testing.T, key, val string) *Command {
	t.Helper()
	c, err := newClusterMetaSet(key, val)
	if err != nil {
		t.Fatalf("newClusterMetaSet: %v", err)
	}
	return c
}

// TestFSM_IdempotentReapply drives fsm.Apply directly (no raft) to prove §3.7 #2:
// an entry with index <= applied_index is a verified no-op (no op SQL runs), while
// a higher index applies. raft RE-APPLIES already-applied entries on restart, so
// the FSM must self-skip by its OWN durable applied_index.
func TestFSM_IdempotentReapply(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())

	if _, ok := f.Apply(logCmd(t, 2, 1, metaCmd(t, "t:a", "1"))).(appliedOK); !ok {
		t.Fatal("first apply @2 should be appliedOK")
	}
	if got := mustApplied(t, f); got != 2 {
		t.Fatalf("applied_index=%d want 2", got)
	}

	// Re-apply the SAME index — must be a verified no-op (idempotent skip).
	before := f.reapplyCount.Load()
	if _, ok := f.Apply(logCmd(t, 2, 1, metaCmd(t, "t:a", "MUTATED"))).(appliedNoOp); !ok {
		t.Fatal("re-apply @2 should be appliedNoOp")
	}
	if f.reapplyCount.Load() != before+1 {
		t.Fatal("re-apply must increment the reapply counter")
	}
	// The op SQL must NOT have run: the value is unchanged despite the re-apply
	// carrying "MUTATED".
	if v := mustMeta(t, f, "t:a"); v != "1" {
		t.Fatalf("idempotent re-apply must not run op SQL: t:a=%q want \"1\"", v)
	}

	// A LOWER index is also a no-op.
	if _, ok := f.Apply(logCmd(t, 1, 1, metaCmd(t, "t:b", "x"))).(appliedNoOp); !ok {
		t.Fatal("apply @1 (< applied) should be appliedNoOp")
	}
	if _, ok := mustMetaOK(t, f, "t:b"); ok {
		t.Fatal("a skipped lower-index entry must not have written t:b")
	}

	// A higher index applies.
	if _, ok := f.Apply(logCmd(t, 3, 1, metaCmd(t, "t:c", "3"))).(appliedOK); !ok {
		t.Fatal("apply @3 should be appliedOK")
	}
	if got := mustApplied(t, f); got != 3 {
		t.Fatalf("applied_index=%d want 3", got)
	}
}

// TestD4FSMReqIDDedupSemantics pins the §4.1 D4 cross-retry dedup (this replaces the
// D1 "ReqID is inert" pin — D4 activates it). It uses a NON-idempotent metaCmd
// (distinct keys t:a/t:b) so a missed dedup is OBSERVABLE (the §0bis-B non-vacuous
// framing): (a) a NEW ReqID execs + records the ledger; (b) the SAME ReqID at a NEW
// index is appliedDedup — the op SQL is SKIPPED (t:b never written) but applied_index
// ADVANCES and dedupCount increments; (c) re-delivery of the SAME index is the cheap
// index-skip (appliedNoOp, dedupCount unchanged); (d) an empty ReqID never dedups.
func TestD4FSMReqIDDedupSemantics(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())

	c1 := metaCmd(t, "t:a", "1")
	c1.ReqID = "abc123"
	c2 := metaCmd(t, "t:b", "2")
	c2.ReqID = "abc123" // identical ReqID, different index — a committed-but-ack-lost retry

	// (a) new ReqID at @2 executes.
	if _, ok := f.Apply(logCmd(t, 2, 1, c1)).(appliedOK); !ok {
		t.Fatal("apply @2 (new ReqID) should be appliedOK")
	}
	// (b) same ReqID at @3 is deduped: op SKIPPED, cursor advanced, dedupCount=1.
	if _, ok := f.Apply(logCmd(t, 3, 1, c2)).(appliedDedup); !ok {
		t.Fatal("apply @3 with the SAME ReqID must be appliedDedup (op skipped, cursor advanced)")
	}
	if got := f.dedupCount.Load(); got != 1 {
		t.Fatalf("dedupCount = %d, want 1 (the dedup branch must have fired — non-vacuity)", got)
	}
	if v, ok := mustMetaOK(t, f, "t:a"); !ok || v != "1" {
		t.Fatalf("t:a = (%q,%v) want (\"1\",true)", v, ok)
	}
	if _, ok := mustMetaOK(t, f, "t:b"); ok {
		t.Fatal("t:b MUST NOT exist — the dedup must have SKIPPED the op (non-idempotent observable)")
	}
	if got := mustApplied(t, f); got != 3 {
		t.Fatalf("applied_index must advance to 3 through the dedup, got %d (a rollback here would wedge re-delivery)", got)
	}

	// (c) raft re-delivers the SAME index @3 -> cheap index-skip, NOT a dedup.
	if _, ok := f.Apply(logCmd(t, 3, 1, c2)).(appliedNoOp); !ok {
		t.Fatal("re-delivery of the SAME index must be appliedNoOp (index-skip runs first)")
	}
	if got := f.dedupCount.Load(); got != 1 {
		t.Fatalf("dedupCount = %d after a same-index re-delivery, want 1 (index-skip must run BEFORE the dedup branch)", got)
	}

	// (d) empty ReqID never dedups: two distinct ops both apply.
	if _, ok := f.Apply(logCmd(t, 4, 1, metaCmd(t, "t:c", "3"))).(appliedOK); !ok {
		t.Fatal("apply @4 (empty ReqID) should be appliedOK")
	}
	if _, ok := f.Apply(logCmd(t, 5, 1, metaCmd(t, "t:d", "4"))).(appliedOK); !ok {
		t.Fatal("apply @5 (empty ReqID) should be appliedOK (empty ReqID never dedups)")
	}
	if v, ok := mustMetaOK(t, f, "t:d"); !ok || v != "4" {
		t.Fatalf("t:d = (%q,%v) want (\"4\",true) — empty ReqID must not dedup", v, ok)
	}
}

// TestFSM_PoisonEntry proves the §2.8 policy: a structurally-invalid committed
// entry advances applied_index past it as a logged no-op (it does NOT wedge the
// FSM by erroring forever, and is NOT counted as an idempotent reapply).
func TestFSM_PoisonEntry(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())

	poison := &raft.Log{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("not-a-valid-command")}
	res := f.Apply(poison)
	if _, isErr := res.(error); isErr {
		t.Fatalf("poison entry must NOT return an error (would wedge the FSM): %v", res)
	}
	if _, ok := res.(appliedPoison); !ok {
		t.Fatalf("poison entry must be an appliedPoison, got %T", res)
	}
	if got := mustApplied(t, f); got != 2 {
		t.Fatalf("poison entry must ADVANCE applied_index past it: got %d want 2", got)
	}
	// The FSM is not wedged: a subsequent valid entry applies.
	if _, ok := f.Apply(logCmd(t, 3, 1, metaCmd(t, "t:ok", "y"))).(appliedOK); !ok {
		t.Fatal("a valid entry after a poison entry must apply (FSM not wedged)")
	}
}

// TestDiscriminatingControl_DetectsBrokenSeparateTxn is the §13.4 non-vacuity #5/#6
// reverse control: it proves the double-apply DETECTOR (a baked-literal append,
// NOT a value+1 read-modify-write) actually distinguishes the correct same-txn
// apply (1 row) from a BROKEN separate-txn apply crashing in the gap (2 rows). If
// both produced 1 row, the kill-9 matrix would have no discriminating power.
func TestDiscriminatingControl_DetectsBrokenSeparateTxn(t *testing.T) {
	_, db := freshFSM(t, t.TempDir())
	if _, err := db.Exec(`CREATE TABLE detector(id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		t.Fatal(err)
	}
	appendOp := func(tx *sql.Tx) error { _, err := tx.Exec(`INSERT INTO detector DEFAULT VALUES`); return err }
	count := func() int {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM detector`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// CORRECT (same-txn): op + applied_index in ONE txn; a re-apply at the same
	// index is skipped, so the op runs exactly once.
	applyCorrect := func(index uint64) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		applied, err := readAppliedIndexTx(tx)
		if err != nil {
			t.Fatal(err)
		}
		if index <= applied {
			_ = tx.Rollback()
			return
		}
		if err := appendOp(tx); err != nil {
			t.Fatal(err)
		}
		if err := writeAppliedIndexTx(tx, index, 1); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	applyCorrect(2)
	applyCorrect(2) // re-apply, must be skipped
	if got := count(); got != 1 {
		t.Fatalf("correct same-txn apply must run exactly once: detector=%d want 1", got)
	}

	// Reset for the broken control.
	if _, err := db.Exec(`DELETE FROM detector`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key = ?`, metaKeyAppliedIndex); err != nil {
		t.Fatal(err)
	}

	// BROKEN (separate-txn) with a crash in the gap: op commits, but applied_index
	// is NOT advanced (simulating SIGKILL between the two commits). The re-apply
	// (applied_index still 0) runs the op AGAIN → 2 rows.
	tx1, _ := db.Begin()
	if err := appendOp(tx1); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil { // op durable
		t.Fatal(err)
	}
	// [crash here — applied_index NOT written]
	tx2, _ := db.Begin()
	applied, _ := readAppliedIndexTx(tx2)
	if 2 > applied { // re-apply re-runs the op because applied_index never advanced
		if err := appendOp(tx2); err != nil {
			t.Fatal(err)
		}
		_ = writeAppliedIndexTx(tx2, 2, 1)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 2 {
		t.Fatalf("broken separate-txn must DOUBLE-APPLY (detector=2), got %d — the detector has no discriminating power", got)
	}
}

// TestFSM_FailStopOnPersistentApplyError proves the §3.7 D1 amendment: a committed
// command that cannot be durably applied (a persistent transient infra failure)
// makes Apply FAIL-STOP (panic) rather than returning the error to raft — because
// raft would advance lastApplied past it regardless, and a co-occurring snapshot
// would then drop it (silent data loss). Fail-stop halts the node so the entry is
// re-delivered by the snapshot-free log replay on restart.
func TestFSM_FailStopOnPersistentApplyError(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	applyFailHook = func(uint64) error { return errors.New("injected persistent commit failure") }
	defer func() { applyFailHook = nil }() // declared first → runs LAST (after recover)
	defer func() {
		if recover() == nil {
			t.Fatal("Apply must FAIL-STOP (panic) on a persistent apply error, not return silently")
		}
	}()
	f.Apply(logCmd(t, 2, 1, metaCmd(t, "t:x", "1")))
	t.Fatal("unreachable: Apply should have panicked")
}

// ---- helpers ----

func mustApplied(t *testing.T, f *fsm) uint64 {
	t.Helper()
	v, err := readAppliedIndexDB(f.db)
	if err != nil {
		t.Fatalf("read applied: %v", err)
	}
	return v
}

func mustMeta(t *testing.T, f *fsm, key string) string {
	t.Helper()
	v, ok := mustMetaOK(t, f, key)
	if !ok {
		t.Fatalf("meta %q missing", key)
	}
	return v
}

func mustMetaOK(t *testing.T, f *fsm, key string) (string, bool) {
	t.Helper()
	var v string
	err := f.db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, key).Scan(&v)
	if err == nil {
		return v, true
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	t.Fatalf("meta %q: %v", key, err)
	return "", false
}
