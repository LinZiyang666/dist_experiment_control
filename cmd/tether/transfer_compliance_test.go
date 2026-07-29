package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestPullDuplicateIDRejectionDoesNotFinalizeOriginalTransfer(t *testing.T) {
	if pullPrepareFailureNeedsFinalize("transfer_id_in_flight") {
		t.Fatal("duplicate pull rejection must not finalize the original in-flight transfer")
	}
	for _, code := range []string{"path_outside_roots", "transfer_disabled", "io_error"} {
		if !pullPrepareFailureNeedsFinalize(code) {
			t.Fatalf("agent-side prepare failure %q still needs broker finalization", code)
		}
	}
}

func TestCommitLocalTempNoForceConcurrentSingleWinner(t *testing.T) {
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

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, tmp := range []string{tmpA, tmpB} {
		tmp := tmp
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- commitLocalTemp(tmp, dst, false)
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
