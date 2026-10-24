package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"sync"
	"testing"
)

// The predicate moved with the cap0 pull into the one-shot legacy owner (S7).
// origin: hot-update S7 plan §7 / §9 / P31
func TestPullDuplicateIDRejectionDoesNotFinalizeOriginalTransfer(t *testing.T) {
	if cli.PullPrepareFailureNeedsFinalize("transfer_id_in_flight") {
		t.Fatal("duplicate pull rejection must not finalize the original in-flight transfer")
	}
	for _, code := range []string{"path_outside_roots", "transfer_disabled", "io_error"} {
		if !cli.PullPrepareFailureNeedsFinalize(code) {
			t.Fatalf("agent-side prepare failure %q still needs broker finalization", code)
		}
	}
}

// origin: hot-update S7 plan §3 / §11.3 / P31; race the actual leaf five times.
func TestCommitLocalTempNoForceConcurrentSingleWinner(t *testing.T) {
	before := runtime.NumGoroutine()
	fdBefore := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLocalTransferConcurrentCommit(t)
	}
	testharness.AssertNoGoroutineLeak(t, "local transfer commit", before)
	testharness.AssertNoFDLeak(t, "local transfer commit", fdBefore, 0)
}

func exerciseLocalTransferConcurrentCommit(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	dst := filepath.Join(root, "dst.bin")
	tmpA := filepath.Join(root, "a.tmp")
	tmpB := filepath.Join(root, "b.tmp")
	if err := os.WriteFile(tmpA, []byte("A"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmpB, []byte("B"), 0o600); err != nil {
		t.Fatal(err)
	}

	// origin: hot-update S7 plan §3 / §11.4 / P31; both receivers pin the
	// parent before the race, so the no-force primitive must pick one winner.
	commits := []func() error{
		prepareLocalTransferCommit(t, dst, []byte("A"), "a", false),
		prepareLocalTransferCommit(t, dst, []byte("B"), "b", false),
	}
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, commit := range commits {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- commit()
		}()
	}
	wg.Wait()
	close(errs)

	ok, failed := 0, 0
	for err := range errs {
		if err == nil {
			ok++
		} else {
			failed++
		}
	}
	if ok != 1 || failed != 1 {
		t.Fatalf("success=%d failed=%d, want 1/1", ok, failed)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" && string(got) != "B" {
		t.Fatalf("dst=%q", got)
	}
}

// TestPullHelpDoesNotPromiseASizeDerivedBudget pins the user-visible boundary of C2.
//
// origin: batch-c external review C5/C16. PullPrepareReq carries no size, and both the agent and the
// broker intentionally call XferBudget with size=0 for pull, which is the fixed five-minute floor.
// The CLI must not promise the push-only size-derived behavior for a large pull.
func TestPullHelpDoesNotPromiseASizeDerivedBudget(t *testing.T) {
	flag := newPullCmd().Flags().Lookup("timeout")
	if flag == nil {
		t.Fatal("pull has no --timeout flag")
	}
	if strings.Contains(strings.ToLower(flag.Usage), "derived from the file size") {
		t.Fatalf("pull --timeout help falsely promises a size-derived tier-B budget: %q; the pull "+
			"protocol carries no size to the agent/broker budget sites and remains fixed at the floor", flag.Usage)
	}
}

// Use the production directory-pinned leaf rather than reproducing its link or
// rename in a test adapter. Prepare both files before competing for the leaf.
// origin: hot-update S7 plan §3 / P31 legacy local-write migration.
func prepareLocalTransferCommit(t *testing.T, dst string, data []byte, suffix string, force bool) func() error {
	t.Helper()
	vp, err := transferpath.ValidateForWrite(dst, nil, transferpath.ModeOpen)
	if err != nil {
		t.Fatal(err)
	}
	file, pending, err := transferpath.OpenForWriteAtomic(vp, suffix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pending.Abort)
	t.Cleanup(func() { _ = file.Close() })
	n, err := file.Write(data)
	if err != nil || n != len(data) {
		t.Fatalf("write local transfer tmp: n=%d want=%d err=%v", n, len(data), err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return func() error {
		// A refused no-force commit still owns its tmp and parent; retire both
		// before the exercise returns, not at the enclosing test's final cleanup.
		defer pending.Abort()
		return transferpath.RenameForWriteAtomic(vp, pending, force)
	}
}
