package cluster

import (
	"errors"
	"testing"

	"github.com/hashicorp/raft"
)

// TestD4DedupLedgerSameTxnAtomicity proves the §4.1 D4 invariant that the op SQL +
// the cluster_reqid_ledger INSERT + the applied_index UPSERT live in ONE txn: an
// injected commit-time failure (applyFailHook, fired after the op + ledger insert but
// before commit) must leave NEITHER the op effect NOR the ledger row NOR the cursor.
// It calls applyCommand directly (not Apply) to observe the single-attempt rollback
// without the fail-stop retry/panic loop.
func TestD4DedupLedgerSameTxnAtomicity(t *testing.T) {
	f, db := freshFSM(t, t.TempDir())

	c := metaCmd(t, "t:atomic", "1")
	c.ReqID = "deadbeef"

	applyFailHook = func(uint64) error { return errors.New("injected commit failure") }
	defer func() { applyFailHook = nil }()

	if _, err := f.applyCommand(logCmd(t, 2, 1, c), c); err == nil {
		t.Fatal("applyCommand must surface the injected failure")
	}
	applyFailHook = nil

	if _, ok := mustMetaOK(t, f, "t:atomic"); ok {
		t.Fatal("op effect must NOT survive the rolled-back txn")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_reqid_ledger WHERE req_id='deadbeef'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("ledger row must NOT survive the rolled-back txn, got %d", n)
	}
	if got := mustApplied(t, f); got != 0 {
		t.Fatalf("applied_index must NOT advance on a rolled-back apply, got %d", got)
	}
}

// TestD4ReqIDLedgerGCBoundedAndVacuity proves (a) the in-Apply-txn deterministic GC
// bounds the ledger (an entry older than reqIDRetentionWindow indices is range-
// deleted) AND (b) the window is load-bearing: once a ReqID has aged out, a retry
// RE-EXECUTES (no dedup) — so a window set below the real retry horizon would
// double-execute (the §0bis-D vacuity bite). Uses a shrunk window so the test does
// not have to apply 1M entries.
func TestD4ReqIDLedgerGCBoundedAndVacuity(t *testing.T) {
	f, db := freshFSM(t, t.TempDir())
	orig := reqIDRetentionWindow
	reqIDRetentionWindow = 5
	defer func() { reqIDRetentionWindow = orig }()

	// @2: ReqID "aaaa" executes + records the ledger.
	c1 := metaCmd(t, "t:g1", "1")
	c1.ReqID = "aaaa"
	if _, ok := f.Apply(logCmd(t, 2, 1, c1)).(appliedOK); !ok {
		t.Fatal("@2 (new ReqID) should be appliedOK")
	}
	// @8: a later op. GC at index 8 deletes raft_index < 8-5=3, so "aaaa" (index 2) ages out.
	c2 := metaCmd(t, "t:g2", "2")
	c2.ReqID = "bbbb"
	if _, ok := f.Apply(logCmd(t, 8, 1, c2)).(appliedOK); !ok {
		t.Fatal("@8 (new ReqID) should be appliedOK")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_reqid_ledger WHERE req_id='aaaa'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("aaaa must be GC'd once it ages past the window (bounded ledger), got %d", n)
	}

	// VACUITY: "aaaa" retried at @9 now RE-EXECUTES (no ledger row to dedup against) —
	// proving a too-small window double-executes, i.e. the window is load-bearing.
	c3 := metaCmd(t, "t:g1again", "9")
	c3.ReqID = "aaaa"
	if _, ok := f.Apply(logCmd(t, 9, 1, c3)).(appliedOK); !ok {
		t.Fatal("an aged-out ReqID retry must RE-EXECUTE (appliedOK), proving the window is load-bearing")
	}
	if v, ok := mustMetaOK(t, f, "t:g1again"); !ok || v != "9" {
		t.Fatalf("the re-executed op must write its effect; t:g1again=(%q,%v)", v, ok)
	}
}

// TestD4DedupBranchSameTxnAtomicity (review m2) exercises the DEDUP-HIT branch's own
// commit point (fsm.go applyFailHook/applyCommitGate before tx.Commit): a NEW ReqID
// commits at @2, then the SAME ReqID at @3 hits the dedup branch with applyFailHook
// armed. The dedup commit must roll back cleanly — applied_index stays at 2 (NOT 3),
// no spurious advance, no wedge — and a clean re-apply at @3 then dedups to 3.
func TestD4DedupBranchSameTxnAtomicity(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	c := metaCmd(t, "t:dd", "1")
	c.ReqID = "cafe"
	if _, ok := f.Apply(logCmd(t, 2, 1, c)).(appliedOK); !ok {
		t.Fatal("@2 (new ReqID) should be appliedOK")
	}

	applyFailHook = func(uint64) error { return errors.New("injected dedup-commit failure") }
	defer func() { applyFailHook = nil }()
	if _, err := f.applyCommand(logCmd(t, 3, 1, c), c); err == nil {
		t.Fatal("the dedup-branch commit must surface the injected failure")
	}
	applyFailHook = nil
	if got := mustApplied(t, f); got != 2 {
		t.Fatalf("a rolled-back dedup commit must NOT advance applied_index past 2, got %d", got)
	}

	// Clean re-apply at @3: now the dedup branch commits and advances to 3.
	if _, ok := f.Apply(logCmd(t, 3, 1, c)).(appliedDedup); !ok {
		t.Fatal("@3 (same ReqID, clean) should be appliedDedup")
	}
	if got := mustApplied(t, f); got != 3 {
		t.Fatalf("the clean dedup must advance applied_index to 3, got %d", got)
	}
}

// TestD4DecodeRejectsBadVersionAndReqID (review m1) covers the two D4 decode-poison
// guards the plan mandated but left untested: a v1-shaped entry (the commandVersion
// 1->2 bump) and a malformed ReqID (the split-brain-ledger charset guard) must each
// be treated as POISON — advance applied_index, never wedge — and a subsequent valid
// v2 entry must still apply.
func TestD4DecodeRejectsBadVersionAndReqID(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	idx := uint64(2)
	poison := func(name string, mutate func(c *Command)) {
		c := metaCmd(t, "t:p", "1")
		mutate(c)
		data, err := c.encode()
		if err != nil {
			t.Fatalf("%s encode: %v", name, err)
		}
		res := f.Apply(&raft.Log{Index: idx, Term: 1, Type: raft.LogCommand, Data: data})
		if _, isErr := res.(error); isErr {
			t.Fatalf("%s must NOT error (would wedge): %v", name, res)
		}
		if _, ok := res.(appliedPoison); !ok {
			t.Fatalf("%s must be appliedPoison, got %T", name, res)
		}
		if got := mustApplied(t, f); got != idx {
			t.Fatalf("%s must advance applied_index to %d, got %d", name, idx, got)
		}
		idx++
	}
	poison("v1-shaped entry", func(c *Command) { c.Version = 1 })
	poison("non-hex ReqID", func(c *Command) { c.ReqID = "NOThex!" })
	poison("uppercase ReqID", func(c *Command) { c.ReqID = "ABCDEF" })
	poison("over-64 ReqID", func(c *Command) {
		long := ""
		for i := 0; i < 65; i++ {
			long += "a"
		}
		c.ReqID = long
	})

	// FSM not wedged: a valid v2 entry (empty ReqID) applies after the poison run.
	if _, ok := f.Apply(logCmd(t, idx, 1, metaCmd(t, "t:ok", "y"))).(appliedOK); !ok {
		t.Fatal("a valid v2 entry after the poison run must apply (FSM not wedged)")
	}
}
