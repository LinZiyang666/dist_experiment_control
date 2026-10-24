//go:build linux

// output_discard_test.go — the draining pump of Output.Discard (S2 plan
// D11) when the source goes quiet: it must park in the blocking read, not
// spin on a stale deadline, and the same must hold for a discarding pump
// that was frozen, restored and thawed. output_test.go proves what Discard
// retains and drains; this file proves what it costs while nothing arrives.
// origin: hot-update S2 internal review, resources lane.
package workload

import (
	"context"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// outputCPUTime is this process's user+system CPU time.
func outputCPUTime(t *testing.T) time.Duration {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// outputPumpParkedInRead reports whether an Output pump goroutine is blocked
// inside the poller's read wait (the only place an idle discarding pump may
// be), sampled a few times so a pump that is momentarily between two reads
// does not count as spinning.
func outputPumpParkedInRead(t *testing.T) bool {
	t.Helper()
	return outputPumpsParkedInRead(t, 1) >= 1
}

// outputPumpsParkedInRead counts the Output pump goroutines parked in the
// poller's read wait, sampling until at least want are seen (or the
// attempts run out). The COUNT is what lets a test tell "the restored pump
// parks" from "some other pump was already parked" (S2 internal review
// resources-V2).
func outputPumpsParkedInRead(t *testing.T, want int) int {
	t.Helper()
	buf := make([]byte, 1<<20)
	parked := 0
	for attempt := 0; attempt < 20; attempt++ {
		parked = 0
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "(*Output).run") && strings.Contains(g, "internal/poll.runtime_pollWait") {
				parked++
			}
		}
		if parked >= want {
			return parked
		}
		time.Sleep(10 * time.Millisecond)
	}
	return parked
}

// TestOutputDiscardIdlePumpParksInsteadOfSpinning: a stream discarded while
// its pump waits on replay backpressure (log full, writer blocked) and one
// discarded while its pump is blocked in Read (pipe empty) must both end up
// parked in the poller once the writer goes quiet — Discard's own wake-up
// deadline is cleared by the pump, so the next Read blocks. The CPU bound
// is deliberately loose (a spinning pump burns the whole window).
func TestOutputDiscardIdlePumpParksInsteadOfSpinning(t *testing.T) {
	const window = 400 * time.Millisecond
	const cpuBound = 150 * time.Millisecond

	// Arm 1: pump parked on backpressure at Discard time, writer blocked on
	// a full pipe; after Discard the writer completes and then goes quiet.
	out, log, writer := outputFixture(t, 64)
	pipeSize := outputFDValue(t, writer, unix.F_GETPIPE_SZ)
	first := outputBytes(64 + pipeSize + 1024)
	written := make(chan outputWriteResult, 1)
	go func() { n, err := writer.Write(first); written <- outputWriteResult{n, err} }()
	outputAwait(t, func() bool { return log.Snapshot().End == 64 })
	out.Discard("abandoned: idle")
	outputRequireWriter(t, written, len(first))
	if !outputPumpParkedInRead(t) {
		t.Fatal("arm 1: discarding pump is not parked in the poller after the writer went quiet")
	}
	before := outputCPUTime(t)
	time.Sleep(window)
	if spent := outputCPUTime(t) - before; spent > cpuBound {
		t.Fatalf("arm 1: discarding pump burned %v of CPU in a %v idle window", spent, window)
	}
	// Retire arm 1's actor so its parked pump cannot stand in for arm 2's.
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if n := outputPumpsParkedInRead(t, 0); n != 0 {
		t.Fatalf("arm 1's pump is still parked after Close: %d", n)
	}

	// Arm 2: pump blocked in Read at Discard time (nothing buffered), then
	// frozen mid-discard, restored and thawed: the restored pump must park
	// the same way (no stale deadline survives the thaw).
	out2, log2, writer2 := outputFixture(t, 64)
	if _, err := writer2.Write(outputBytes(16)); err != nil {
		t.Fatal(err)
	}
	outputAwait(t, func() bool { return log2.Snapshot().End == 16 })
	out2.Discard("abandoned: second")
	if !outputPumpParkedInRead(t) {
		t.Fatal("arm 2: discarding pump is not parked after Discard woke a blocked read")
	}
	state, files := outputFreeze(t, out2)
	if !state.Discarding || state.ReadError != "abandoned: second" {
		t.Fatalf("frozen discarding stream: %+v", state)
	}
	restoredLog, err := RestoreReplay(&ReplayBudget{}, state.Replay, outputDuplicate(t, files["replay"]))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreOutput(state, outputDuplicate(t, files["source"]), restoredLog)
	if err != nil {
		t.Fatal(err)
	}
	out2.Thaw() // the old owner resumes draining too; both share the description
	restored.Thaw()
	// Exactly two pumps (the old owner's and the restored one) park: the
	// count proves the RESTORED pump exists and parks, not just the old one.
	if n := outputPumpsParkedInRead(t, 2); n != 2 {
		t.Fatalf("arm 2: %d discarding pumps parked after Thaw, want 2 (old owner + restored)", n)
	}
	before = outputCPUTime(t)
	time.Sleep(window)
	if spent := outputCPUTime(t) - before; spent > cpuBound {
		t.Fatalf("arm 2: restored discarding pump burned %v of CPU in a %v idle window", spent, window)
	}
	// A late write larger than the pipe buffer is drained by the pumps and
	// retained by neither: a restored pump that did not drain would leave the
	// writer blocked. Closing the writer ends both pumps without a FIN.
	late := outputBytes(pipeSize + 4096)
	lateWritten := make(chan outputWriteResult, 1)
	go func() { n, err := writer2.Write(late); lateWritten <- outputWriteResult{n, err} }()
	outputRequireWriter(t, lateWritten, len(late))
	if err := writer2.Close(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []*Output{out2, restored} {
		outputAwait(t, func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			return !p.running
		})
		if got := p.Snapshot(); got.Replay.End != 16 || got.Replay.FIN || got.ReadError != "abandoned: second" {
			t.Fatalf("drained discard stream: %+v", got)
		}
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), outputTestBudget)
	defer cancel()
	if _, _, err := out2.Freeze(ctx); err != nil {
		t.Fatalf("freeze of an ended discarding stream: %v", err)
	}
	out2.Thaw()
}
