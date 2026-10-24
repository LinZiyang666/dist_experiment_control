//go:build linux

package transferpath

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"golang.org/x/sys/unix"
)

// origin: hot-update S7 plan §11.3 / P05 / T10 / T11
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	var descriptions []*os.File
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t, &descriptions)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer atomic lifecycle", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "transfer atomic lifecycle", beforeFDs, 0)
	// Keep Go's file finalizers from disguising an omitted owner Close. The
	// descriptions remain reachable until both the exact FD check and each
	// consumed-handle check finish; collection is never our teardown mechanism.
	for _, file := range descriptions {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("owner did not consume an original file: %v", err)
		}
	}
	runtime.KeepAlive(descriptions)
}

func exerciseTransferLifecycle(t *testing.T, descriptions *[]*os.File) {
	t.Helper()
	// A failed exec Thaws originals; an imported ACTIVE owner then advances,
	// freezes its CURRENT prefix and imports that prefix into a rollback owner.
	r, dst := newTestAtomic(t, true, []byte("old complete"))
	sourceVP, err := ValidateForRead(dst, nil, ModeOpen)
	if err != nil {
		t.Fatal(err)
	}
	r.source, err = OpenForReadAtomic(sourceVP)
	if err != nil {
		t.Fatal(err)
	}
	r.state.Source = xfercontract.ResourceRef{ResourceID: "lifecycle-source", Name: xfercontract.SourceFileName, Kind: xfercontract.ResourceSource}
	*descriptions = append(*descriptions, r.tmp, r.parent, r.source)
	writeSyncedAtomic(t, r, []byte("first"), 0)
	state, _, err := r.Freeze(atomicDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := ThawAtomic(r, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	writeSyncedAtomic(t, r, []byte("-current"), state.DurableOffset)
	state, resources, err := r.Freeze(atomicDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	imported, _, err := ImportAtomic(state, handofftest.DupResources(t, resources))
	if err != nil {
		t.Fatal(err)
	}
	if err := DiscardAtomic(r, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	current := imported.(*Atomic)
	*descriptions = append(*descriptions, current.tmp, current.parent, current.source)
	if err := ThawAtomic(current, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	writeSyncedAtomic(t, current, []byte("-rollback"), state.DurableOffset)
	state, resources, err = current.Freeze(atomicDeadline(t))
	if err != nil || state.DurableOffset != int64(len("first-current-rollback")) {
		t.Fatalf("current rollback lost new progress: offset=%d err=%v", state.DurableOffset, err)
	}
	rollback, _, err := ImportAtomic(state, handofftest.DupResources(t, resources))
	if err != nil {
		t.Fatal(err)
	}
	*descriptions = append(*descriptions, rollback.(*Atomic).tmp, rollback.(*Atomic).parent, rollback.(*Atomic).source)
	if err := DiscardAtomic(current, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := ThawAtomic(rollback, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if out := rollback.Commit(atomicGrant(), atomicDeadline(t)); out.Outcome != xfercontract.OutcomeLanded || !out.ParentSynced {
		t.Fatalf("rollback commit: %+v", out)
	}
	// The terminal actor may still own courier/ACK responsibility, but the
	// primitive owner has no hidden data-plane FD after directory durability.
	_, resources, err = rollback.Freeze(atomicDeadline(t))
	if err != nil || len(resources) != 0 {
		t.Fatalf("terminal primitive retained descriptors: resources=%d err=%v", len(resources), err)
	}
	if err := rollback.Stop(atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != "first-current-rollback" {
		t.Fatalf("current rollback bytes: %q %v", got, err)
	}

	// A real Sync failure must leave the old checkpoint. The old owner
	// retries repair; the importer never truncates shared state to make it fit.
	failedSync, _ := newTestAtomic(t, false, nil)
	*descriptions = append(*descriptions, failedSync.tmp, failedSync.parent)
	writeSyncedAtomic(t, failedSync, []byte("durable"), 0)
	if _, err := failedSync.WriteAt([]byte("suffix"), 7); err != nil {
		t.Fatal(err)
	}
	realSync := failedSync.ops.sync
	failedSync.ops.sync = func(*os.File) error { return unix.EIO }
	if err := failedSync.Sync(); !errors.Is(err, unix.EIO) || failedSync.state.DurableOffset != 7 {
		t.Fatalf("failed Sync advanced checkpoint: %v", err)
	}
	if _, _, err := failedSync.Freeze(atomicDeadline(t)); !errors.Is(err, unix.EIO) {
		t.Fatalf("failed repair was exported: %v", err)
	}
	failedSync.ops.sync = realSync
	if err := ThawAtomic(failedSync, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := failedSync.Freeze(atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := failedSync.Stop(atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}

	exerciseAtomicBlockedIO(t, false, descriptions)
	exerciseAtomicBlockedIO(t, true, descriptions)

	// Failed abort cleanup survives a cut with no-syscall evidence. Import
	// must retain the closed gate and retry the unlink without a primitive.
	aborted, _ := newTestAtomic(t, false, nil)
	*descriptions = append(*descriptions, aborted.tmp, aborted.parent)
	writeSyncedAtomic(t, aborted, []byte("abort cleanup"), 0)
	aborted.ops.unlink = func(int, string, int) error { return unix.EACCES }
	if err := aborted.Stop(atomicDeadline(t)); err == nil {
		t.Fatal("abort cleanup failure was hidden")
	}
	state, resources, err = aborted.Freeze(atomicDeadline(t))
	if err != nil || !state.UnlinkPending || state.PrimitiveOutcome.Outcome != xfercontract.OutcomeNotEntered {
		t.Fatalf("abort cleanup lost evidence: %v", err)
	}
	cleanupOwner, _, err := ImportAtomic(state, handofftest.DupResources(t, resources))
	if err != nil {
		t.Fatal(err)
	}
	*descriptions = append(*descriptions, cleanupOwner.(*Atomic).tmp, cleanupOwner.(*Atomic).parent)
	if err := DiscardAtomic(aborted, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := cleanupOwner.Stop(atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}

	// Cancellation/revocation needs no transport to keep its local gate shut.
	for range 2 {
		canceled, target := newTestAtomic(t, false, nil)
		*descriptions = append(*descriptions, canceled.tmp, canceled.parent)
		writeSyncedAtomic(t, canceled, []byte("canceled upload"), 0)
		state, resources, err := canceled.Freeze(atomicDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		// Partial import retains a nonnil file returned alongside an error.
		_, salvage, err := ImportAtomic(state, &atomicPartialSource{source: handofftest.DupResources(t, resources), failAt: 2})
		if !errors.Is(err, unix.EIO) || len(salvage.Resources) != 2 {
			t.Fatalf("partial import ownership: %v", err)
		}
		closeAtomicSalvage(t, salvage)
		if err := canceled.Stop(atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		state, resources, err = canceled.Freeze(atomicDeadline(t))
		if err != nil || len(resources) != 0 || state.PrimitiveOutcome.Outcome != xfercontract.OutcomeNotEntered {
			t.Fatal("cancellation did not retain no-syscall evidence")
		}
		imported, _, err := ImportAtomic(state, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := ThawAtomic(imported, atomicDeadline(t)); err == nil {
			t.Fatal("import reopened a canceled gate")
		}
		if out := imported.Commit(atomicGrant(), atomicDeadline(t)); out.Outcome != xfercontract.OutcomeNotEntered {
			t.Fatalf("late Grant beat cancellation: %+v", out)
		}
		if err := imported.Stop(atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("canceled transfer landed: %v", err)
		}
	}
}

// Model a filesystem that stalls after the owner admitted IO. Handshakes
// establish the blocked window; the short deadline only bounds the join.
func exerciseAtomicBlockedIO(t *testing.T, repair bool, descriptions *[]*os.File) {
	t.Helper()
	r, dst := newTestAtomic(t, false, nil)
	*descriptions = append(*descriptions, r.tmp, r.parent)
	writeSyncedAtomic(t, r, []byte("prefix"), 0)
	entered, release, completed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	if repair {
		if _, err := r.WriteAt([]byte("suffix"), 6); err != nil {
			t.Fatal(err)
		}
		realTruncate := r.ops.truncate
		r.ops.truncate = func(f *os.File, off int64) error {
			close(entered)
			<-release
			return realTruncate(f, off)
		}
	} else {
		realWrite := r.ops.write
		r.ops.write = func(f *os.File, p []byte, off int64) (int, error) {
			close(entered)
			<-release
			return realWrite(f, p, off)
		}
		go func() {
			_, err := r.WriteAt([]byte("suffix"), 6)
			completed <- err
		}()
		<-entered
	}
	short, err := controlbudget.After(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, resources, freezeErr := r.Freeze(short)
	if repair {
		<-entered
	}
	if !errors.Is(freezeErr, controlbudget.ErrExpired) || len(resources) != 0 || r.tmp == nil || r.parent == nil {
		t.Errorf("blocked IO lost its old owner: err=%v resources=%d", freezeErr, len(resources))
	}
	if out := r.Commit(atomicGrant(), atomicDeadline(t)); out.Outcome != xfercontract.OutcomeInflight {
		t.Errorf("commit entered during blocked IO: %+v", out)
	}
	close(release)
	if !repair {
		if err := <-completed; err != nil {
			t.Fatal(err)
		}
	}
	if err := ThawAtomic(r, atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	r.ops = defaultAtomicOperations()
	if _, _, err := r.Freeze(atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(atomicDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), r.state.TmpName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stop left tmp behind: %v", err)
	}
}
