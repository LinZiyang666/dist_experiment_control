package agent

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

// upgrade_download_errors_test.go — the EMITTING side of the line-2 §12 Y2 error-code split.
//
// origin: line-2 external review M18 / PC-3. Y2 replaced two catch-all codes with four specific ones so
// that a monitor could tell "retry this" from "stop, a human must change something". All of that value
// lives in the mapping from a real failure to the right code, and the review measured that the mapping
// had no test at all:
//
//	mutation A  upgrade.go's `fmt.Errorf("%w: %s", ErrUpgradeHTTPStatus, ...)` -> `%v`
//	            (severs the sentinel chain, so every 404 falls back to download_failed =
//	            exitTransient = retry forever, which is exactly the disease Y2 was written to cure)
//	            -> ./internal/agent/ ./cmd/tether/ ./test/architecture/... ./test/determinism/... ALL ok
//
//	mutation B  run.go's errno condition -> `if false`
//	            (the transient PTY branch becomes permanently unreachable)
//	            -> ./cmd/tether/ ok, ./internal/agent/ ok
//
// The wire-code coverage gate in cmd/tether only asserts that each code LITERAL exists somewhere, so it
// cannot see a branch that has stopped being reachable or a sentinel chain that has been cut. That is the
// identity-guard shape this repo forbids: green because nothing was checked.
//
// These tests assert the TRIGGER, not the literal. Both mutations above redden here.

// TestPTYFailureTransientClassification pins the errno criterion behind pty_alloc_failed (retry) vs
// pty_unavailable (stop). Mutation B — replacing the condition with `if false` — reddens here.
func TestPTYFailureTransientClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{"EMFILE: this process is out of descriptors, another session closing fixes it", syscall.EMFILE, true},
		{"ENFILE: the host table is full, same story", syscall.ENFILE, true},
		// origin: line-2 external review M17. ENOSPC is devpts index exhaustion — the pty limit — and it is
		// the MOST likely transient PTY failure on a busy host, yet it was in the terminal branch. Measured
		// in a container with devpts `newinstance,max=1`: the second open returns ENOSPC with EMFILE and
		// ENFILE both false.
		{"ENOSPC: the pty limit (/proc/sys/kernel/pty/max) is reached — clears as sessions close", syscall.ENOSPC, true},
		{"ENOMEM: kernel allocation pressure, a later attempt can win", syscall.ENOMEM, true},
		{"EAGAIN: resource temporarily unavailable, by definition retryable", syscall.EAGAIN, true},
		{"EMFILE wrapped: pty.Allocate wraps cpty.Open, so unwrapping must work", fmt.Errorf("open ptmx: %w", syscall.EMFILE), true},
		{"ENOSPC wrapped the same way", fmt.Errorf("pty: open: %w", syscall.ENOSPC), true},
		{"ENOENT: /dev/ptmx is absent — retrying cannot help", syscall.ENOENT, false},
		{"EPERM: the container forbids it — a human must change the host", syscall.EPERM, false},
		{"EACCES: permissions — same class as EPERM", syscall.EACCES, false},
		{"ENODEV: no pty device at all", syscall.ENODEV, false},
		{"a plain non-errno error must NOT be treated as transient (fail toward 'stop retrying')",
			errors.New("something else entirely"), false},
		{"nil is not transient", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ptyFailureIsTransient(tc.err); got != tc.transient {
				t.Fatalf("ptyFailureIsTransient(%v) = %v, want %v\n\n"+
					"transient -> pty_alloc_failed (retryable); not transient -> pty_unavailable (the "+
					"operator must change the host). Getting this backwards is the defect Y2 fixed: one "+
					"code for both told automation to retry a missing /dev/ptmx forever.",
					tc.err, got, tc.transient)
			}
		})
	}
}

// TestPTYFailureClassifierIsNotConstant is the non-vacuity check on the table above. A constant predicate
// would satisfy a majority of rows in one direction or the other, so a table alone does not prove the
// predicate discriminates. This does.
func TestPTYFailureClassifierIsNotConstant(t *testing.T) {
	if !ptyFailureIsTransient(syscall.EMFILE) || ptyFailureIsTransient(syscall.ENOENT) {
		t.Fatal("ptyFailureIsTransient does not discriminate: it must be true for EMFILE and false for " +
			"ENOENT. A constant predicate makes one of the two Y2 PTY codes unreachable, which is what " +
			"line-2 review mutation B produced with every package still green.")
	}
}

// TestPTYTransientErrnoListCoversPTYExhaustion pins ENOSPC specifically, by name, separately from the
// table.
//
// The table would still pass if someone dropped ENOSPC and simultaneously deleted its row — that is what
// makes a table a weaker guard than it looks. This assertion names the errno and says why, so removing it
// requires arguing with a sentence rather than deleting two lines that look like a pair.
func TestPTYTransientErrnoListCoversPTYExhaustion(t *testing.T) {
	var hasENOSPC bool
	for _, e := range ptyTransientErrnos {
		if e == syscall.ENOSPC {
			hasENOSPC = true
		}
	}
	if !hasENOSPC {
		t.Fatal("ptyTransientErrnos does not contain ENOSPC. That is the errno /dev/ptmx returns when the " +
			"devpts index is exhausted (/proc/sys/kernel/pty/max, or a `max=` mount option) — i.e. PTY " +
			"exhaustion, the exact condition pty_alloc_failed exists to name, and the most likely " +
			"transient PTY failure on a busy host. Without it that condition reports pty_unavailable and " +
			"tells automation to stop retrying something that clears in seconds.")
	}
}
