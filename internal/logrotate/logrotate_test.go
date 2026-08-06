package logrotate

// origin: docs/reviews/h1-plan.md workstream F (2026-08-04 incident: a 5.3GB
// broker.err on a 19GB disk, written by a process with no in-band cap).

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestWriterRotatesAtCapAndKeepsChain(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.log")
	w, err := Open(p, 100, 2, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	line := strings.Repeat("a", 40) + "\n" // 41B
	for i := 0; i < 9; i++ {               // 369B through a 100B cap → rotations
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	// Hard bound: cap + one record.
	if st.Size() > 100+int64(len(line)) {
		t.Fatalf("live file %dB exceeds cap+one-record bound", st.Size())
	}
	if _, err := os.Stat(p + ".1"); err != nil {
		t.Fatalf("backup .1 missing: %v", err)
	}
	if _, err := os.Stat(p + ".2"); err != nil {
		t.Fatalf("backup .2 missing: %v", err)
	}
	if _, err := os.Stat(p + ".3"); err == nil {
		t.Fatal("backup chain exceeded K=2 — the oldest must be dropped")
	}
}

// TestWriterOversizeRecordWrittenWhole: one record larger than the cap still
// lands whole (bound = cap + one record, never a torn line).
func TestWriterOversizeRecordWrittenWhole(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.log")
	w, err := Open(p, 50, 2, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Write([]byte("seed\n")); err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("B", 200) + "\n"
	if _, err := w.Write([]byte(big)); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, p); got != big {
		t.Fatalf("oversize record torn or misplaced: %q", got)
	}
}

// TestWriterStatSeedsLegacyGiant: opening over a pre-existing file inherits
// its size, so the FIRST write past the cap rotates the legacy bulk away —
// the fleet's multi-GB nohup-era agent.log must not keep growing.
func TestWriterStatSeedsLegacyGiant(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.log")
	if err := os.WriteFile(p, []byte(strings.Repeat("L", 500)), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := Open(p, 100, 1, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Write([]byte("fresh\n")); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, p); got != "fresh\n" {
		t.Fatalf("legacy bulk not rotated away on first write: %dB", len(got))
	}
	if got := readFile(t, p+".1"); len(got) != 500 {
		t.Fatalf("legacy bulk not preserved in .1: %dB", len(got))
	}
}

// TestWriterConcurrentWritesNoTearNoLoss (-race): N goroutines × M records
// through a tiny cap; every record must appear EXACTLY once, untorn, across
// the live file + backups… minus those rotated off the end of the chain — so
// the chain is sized to keep everything.
func TestWriterConcurrentWritesNoTearNoLoss(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.log")
	// Big chain so nothing ages out: 4 writers × 50 records × 20B = 4000B; cap
	// 500B → ≤9 rotations; keep 12 backups.
	w, err := Open(p, 500, 12, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				line := []byte(strings.Repeat(string(rune('A'+g)), 18) + "\n")
				if _, err := w.Write(line); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	var all strings.Builder
	for i := 12; i >= 1; i-- {
		if b, err := os.ReadFile(p + "." + strconv.Itoa(i)); err == nil {
			all.Write(b)
		}
	}
	all.WriteString(readFile(t, p))
	counts := map[rune]int{}
	for _, ln := range strings.Split(strings.TrimSuffix(all.String(), "\n"), "\n") {
		if len(ln) != 18 {
			t.Fatalf("torn record: %q", ln)
		}
		counts[rune(ln[0])]++
	}
	for g := 0; g < 4; g++ {
		if counts[rune('A'+g)] != 50 {
			t.Fatalf("writer %c lost records: %d/50", 'A'+g, counts[rune('A'+g)])
		}
	}
}

// TestWriterDegradedSpillsAndRecovers: an unwritable path degrades (Write
// still succeeds — records spill to stderr) and heals once the path becomes
// writable, at the reopen cadence.
func TestWriterDegradedSpillsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "x.log") // parent missing → open fails
	w, err := Open(p, 1000, 2, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Write([]byte("spilled\n")); err != nil {
		t.Fatalf("degraded Write must spill, not fail: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Reopen is rate-limited; force the clock past it.
	w.mu.Lock()
	w.lastReopen = w.lastReopen.Add(-2 * reopenEvery)
	w.mu.Unlock()
	if _, err := w.Write([]byte("landed\n")); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, p); got != "landed\n" {
		t.Fatalf("post-recovery record missing: %q", got)
	}
}

// TestWriterRenameFailureTruncatesInPlace pins the package's self-declared
// anti-incident invariant: NO failure mode may leave an unbounded file. When
// the rotation rename cannot happen (here: a read-only parent directory, so
// os.Rename fails with EACCES), the Writer must TRUNCATE the live file in
// place rather than keep appending to it forever.
// origin: internal review (gate/test-quality lens) — the invariant was
// asserted in a comment and untested; deleting the fallback left the file
// growing with nothing red.
func TestWriterRenameFailureTruncatesInPlace(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory write permissions; the rename would succeed")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "x.log")
	w, err := Open(p, 100, 2, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	line := strings.Repeat("a", 40) + "\n"
	if _, err := w.Write([]byte(line)); err != nil { // seed so size > 0
		t.Fatal(err)
	}
	// Freeze the directory: rename (and create) inside it now fail.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	for i := 0; i < 20; i++ { // 20 x 41B against a 100B cap
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	// Bound: cap + one record + at most one rate-limited marker line (~90B).
	// Without the truncate fallback this file would be ~861B and climbing.
	if st.Size() > 100+int64(len(line))+128 {
		t.Fatalf("live file is %dB with rename failing — the truncate-in-place fallback did not fire; "+
			"a failed rotation must never leave an unbounded file", st.Size())
	}
	if _, err := os.Stat(p + ".1"); err == nil {
		t.Fatal("a backup appeared even though rename was impossible")
	}
}

// TestWriterDegradedReopenIsRateLimited pins the reopen cadence: raft's hclog
// is bridged synchronously onto this sink, so a per-Write open() storm on a
// sick disk would stall raft's own goroutines. Writes while degraded must
// attempt at most one open per reopenEvery.
func TestWriterDegradedReopenIsRateLimited(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	p := filepath.Join(sub, "x.log")
	w, err := Open(p, 1000, 2, 0o600) // parent missing → degraded from birth
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	// One write while the path is still broken: it ATTEMPTS an open (correct —
	// a freshly-born Writer should try immediately), fails, and stamps the
	// cadence baseline.
	if _, err := w.Write([]byte("spill\n")); err != nil {
		t.Fatal(err)
	}

	// Now make the path perfectly openable and hammer it. Every one of these
	// writes COULD succeed — the rate limit is the only thing stopping them,
	// which is exactly what raft's synchronous hclog bridge depends on when
	// the disk is sick.
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := w.Write([]byte("x\n")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(p); err == nil {
		t.Fatal("Writer reopened inside the rate-limit window — 50 writes must produce at most one open attempt per reopenEvery")
	}

	// Past the window, exactly one reopen brings it back.
	w.mu.Lock()
	w.lastReopen = w.lastReopen.Add(-2 * reopenEvery)
	w.mu.Unlock()
	if _, err := w.Write([]byte("back\n")); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, p); got != "back\n" {
		t.Fatalf("post-window record missing: %q", got)
	}
}

// TestWriterCloseLatches pins that Close is final: a later Write must spill to
// stderr, never resurrect the file. Without the latch, `f == nil` is
// indistinguishable from the degraded state and the next write past the
// reopen cadence re-creates a sink the caller believed finished.
func TestWriterCloseLatches(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.log")
	w, err := Open(p, 1000, 2, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("before\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	w.lastReopen = w.lastReopen.Add(-2 * reopenEvery) // past the cadence
	w.mu.Unlock()
	if _, err := w.Write([]byte("after\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err == nil {
		t.Fatal("Write after Close resurrected the log file — Close must latch")
	}
}
