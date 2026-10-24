//go:build linux

package transferpath

// Tier A (inline, no OBJ) and Tier B (fixed NUID) share one atomic receiver.
// The object binding decides which Grant may authorize the real primitive; the
// destination file itself is the oracle, never a recorded outcome alone.

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

func inlineAtomicGrant() xfercontract.CommitGrant {
	g := atomicGrant()
	g.Identity.ObjectVersion = ""
	return g
}

func requireRefusedGrant(t *testing.T, r *Atomic, dst string, old []byte, g xfercontract.CommitGrant, why string) {
	t.Helper()
	out := r.Commit(g, atomicDeadline(t))
	if out.Outcome != "" || out.Code != xfercontract.CodeIntentConflict {
		t.Fatalf("%s: grant admitted or decided: %+v", why, out)
	}
	got, err := os.ReadFile(dst)
	if old == nil {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s: refused grant still created the destination: %q %v", why, got, err)
		}
	} else if err != nil || !bytes.Equal(got, old) {
		t.Fatalf("%s: refused grant changed the destination: %q %v", why, got, err)
	}
	if r.state.CommitGrant != (xfercontract.CommitGrant{}) || r.state.PrimitiveOutcome.Outcome != "" {
		t.Fatalf("%s: a refused grant was retained as commit evidence: %+v", why, r.state)
	}
}

func requireLandedGrant(t *testing.T, r *Atomic, dst string, want []byte, g xfercontract.CommitGrant) {
	t.Helper()
	out := r.Commit(g, atomicDeadline(t))
	if out.Outcome != xfercontract.OutcomeLanded || !out.ParentSynced || out.Grant != g {
		t.Fatalf("declared grant did not land: %+v", out)
	}
	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("landed destination differs: %q %v", got, err)
	}
}

// TestTransferAtomicGrantRequiresDeclaredObjectVersion keeps Tier B strict
// while admitting the Tier A inline path: an empty object version is accepted
// ONLY by a receiver whose owner declared the inline binding, and only for a
// Tier A sized prefix. An undeclared receiver keeps the original Tier B rule.
// origin: hot-update S7 plan §8.1 / §4 / INTEG-D Tier A grant regression
func TestTransferAtomicGrantRequiresDeclaredObjectVersion(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferAtomicGrantRequiresDeclaredObjectVersion(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferAtomicGrantRequiresDeclaredObjectVersion", g)
	testharness.AssertNoFDLeak(t, "TestTransferAtomicGrantRequiresDeclaredObjectVersion", fd, 0)
}

func exerciseTransferAtomicGrantRequiresDeclaredObjectVersion(t *testing.T) {
	t.Helper()

	if inlineAtomicMaxBytes != proto.XferTierAMaxBytes {
		t.Fatalf("inline atomic ceiling %d diverged from tier A %d", inlineAtomicMaxBytes, proto.XferTierAMaxBytes)
	}
	t.Run("undeclared-receiver-keeps-tier-b-rule", func(t *testing.T) {
		old := []byte("old object-backed destination")
		r, dst := newTestAtomic(t, true, old)
		writeSyncedAtomic(t, r, []byte("tiny"), 0)
		// A four-byte prefix fits Tier A, so only the missing declaration can
		// refuse this grant: size is not a tier oracle.
		requireRefusedGrant(t, r, dst, old, inlineAtomicGrant(), "empty version on an undeclared receiver")
		requireLandedGrant(t, r, dst, []byte("tiny"), atomicGrant())
	})
	t.Run("tier-b-binding-is-exact", func(t *testing.T) {
		r, dst := newTestAtomic(t, false, nil)
		if err := r.BindObjectVersion("version"); err != nil {
			t.Fatal(err)
		}
		writeSyncedAtomic(t, r, []byte("object bytes"), 0)
		requireRefusedGrant(t, r, dst, nil, inlineAtomicGrant(), "tier B receiver given the inline empty version")
		other := atomicGrant()
		other.Identity.ObjectVersion = "another-nuid"
		requireRefusedGrant(t, r, dst, nil, other, "tier B receiver given a foreign NUID")
		if err := r.BindObjectVersion(""); err == nil {
			t.Fatal("a declared tier B receiver was rebound to the inline path")
		}
		if err := r.BindObjectVersion("version"); err != nil {
			t.Fatalf("idempotent rebinding refused: %v", err)
		}
		requireLandedGrant(t, r, dst, []byte("object bytes"), atomicGrant())
	})
	t.Run("tier-a-inline-binding", func(t *testing.T) {
		old := []byte("old inline destination")
		r, dst := newTestAtomic(t, true, old)
		if err := r.BindObjectVersion(""); err != nil {
			t.Fatal(err)
		}
		writeSyncedAtomic(t, r, []byte("inline bytes"), 0)
		requireRefusedGrant(t, r, dst, old, atomicGrant(), "inline receiver given an object NUID")
		requireLandedGrant(t, r, dst, []byte("inline bytes"), inlineAtomicGrant())
	})
	t.Run("tier-a-ceiling", func(t *testing.T) {
		r, dst := newTestAtomic(t, true, nil)
		if err := r.BindObjectVersion(""); err != nil {
			t.Fatal(err)
		}
		writeSyncedAtomic(t, r, make([]byte, inlineAtomicMaxBytes), 0)
		if n, err := r.WriteAt([]byte{1}, inlineAtomicMaxBytes); n != 0 || err == nil {
			t.Fatalf("inline receiver wrote past the tier A ceiling: n=%d err=%v", n, err)
		}
		requireLandedGrant(t, r, dst, make([]byte, inlineAtomicMaxBytes), inlineAtomicGrant())

		big, _ := newTestAtomic(t, true, nil)
		writeSyncedAtomic(t, big, make([]byte, inlineAtomicMaxBytes+1), 0)
		if err := big.BindObjectVersion(""); err == nil {
			t.Fatal("an object-sized prefix was declared inline")
		}
	})
	t.Run("import-requires-the-owner-declaration", func(t *testing.T) {
		old := []byte("old imported destination")
		r, dst := newTestAtomic(t, true, old)
		if err := r.BindObjectVersion(""); err != nil {
			t.Fatal(err)
		}
		writeSyncedAtomic(t, r, []byte("inline prefix"), 0)
		state, resources, err := r.Freeze(atomicDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		imported, salvage, err := ImportAtomic(state, handofftest.DupResources(t, resources))
		if err != nil || len(salvage.Resources) != 0 {
			t.Fatalf("ImportAtomic: %v salvage=%d", err, len(salvage.Resources))
		}
		current := imported.(*Atomic)
		t.Cleanup(func() { _ = current.Stop(atomicDeadline(t)) })
		if err := DiscardAtomic(r, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := ThawAtomic(current, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		// The frozen AtomicState cannot carry the declaration. Until the owner
		// repeats it, the imported receiver must not guess the inline path.
		requireRefusedGrant(t, current, dst, old, inlineAtomicGrant(), "imported receiver before its owner declaration")
		if err := current.BindObjectVersion(""); err != nil {
			t.Fatal(err)
		}
		requireLandedGrant(t, current, dst, []byte("inline prefix"), inlineAtomicGrant())
	})
	t.Run("retained-grant-fixes-the-import-binding", func(t *testing.T) {
		r, dst := newTestAtomic(t, true, nil)
		if err := r.BindObjectVersion(""); err != nil {
			t.Fatal(err)
		}
		writeSyncedAtomic(t, r, []byte("landed inline"), 0)
		// The rename lands, but the parent Sync fails: the settlement retry
		// after an image cut must still be admitted under the retained Grant.
		r.ops.sync = func(*os.File) error { return errors.New("injected parent sync failure") }
		out := r.Commit(inlineAtomicGrant(), atomicDeadline(t))
		if out.Outcome != xfercontract.OutcomeLanded || out.ParentSynced {
			t.Fatalf("expected landed with pending directory durability: %+v", out)
		}
		state, resources, err := r.Freeze(atomicDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if state.CommitGrant.Identity.ObjectVersion != "" || !state.DirSyncPending {
			t.Fatalf("inline grant or durability duty not retained: %+v", state)
		}
		oversized := state
		oversized.DurableOffset = inlineAtomicMaxBytes + 1
		oversized.PrimitiveOutcome.Bytes = oversized.DurableOffset
		if validateAtomicState(oversized) == nil {
			t.Fatal("an object-sized prefix crossed an image under an inline grant")
		}
		imported, salvage, err := ImportAtomic(state, handofftest.DupResources(t, resources))
		if err != nil || len(salvage.Resources) != 0 {
			t.Fatalf("a landed inline decision is not importable: %v salvage=%d", err, len(salvage.Resources))
		}
		current := imported.(*Atomic)
		t.Cleanup(func() { _ = current.Stop(atomicDeadline(t)) })
		if err := DiscardAtomic(r, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := ThawAtomic(current, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		requireLandedGrant(t, current, dst, []byte("landed inline"), inlineAtomicGrant())
	})
}
