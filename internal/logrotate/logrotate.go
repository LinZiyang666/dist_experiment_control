// Package logrotate is the in-process, size-capped log sink every long-running
// tether role writes through (h1 F). It exists because nothing else can be
// assumed to exist: the 2026-08-04 incident broker wrote 5.3GB of WARN spam
// into a systemd `append:` file on a 19GB disk with no logrotate installed,
// and the fleet's agents run under nohup redirects with the same unbounded
// shape. The cap must live in the PROCESS.
//
// Semantics:
//
//   - Size-only rotation: when a write would push the file past MaxBytes, the
//     file rotates FIRST (path → path.1 → … → path.K, oldest removed), then
//     the record is written whole to the fresh file. A single record larger
//     than the cap is still written whole — the hard bound is therefore
//     MaxBytes + one record, never a torn line.
//   - The cap is scoped to bytes written THROUGH this Writer. An external
//     appender sharing the file (a legacy nohup fd, a systemd append:) makes
//     the on-disk file undercount against the cap during the transition
//     window — accepted and documented (h1 plan F); their traffic after the
//     h1 hop is panics only.
//   - DEGRADED mode covers a FAILED filesystem, not a hung one — a write that
//     blocks forever blocks here like it would block any file sink. On write/
//     open errors the Writer spills to stderr, retries opening at most once
//     per reopenEvery (raft's hclog is bridged synchronously onto this sink —
//     a per-Write open() storm on a sick disk would stall raft's own
//     goroutines), logs the state TRANSITION, and while degraded emits a
//     once-per-minute reminder line so a permanently-broken sink can never
//     masquerade as success.
//   - If rotation's rename fails (EXDEV, permissions), the file is TRUNCATED
//     in place with a marker line: the anti-incident invariant is that NO
//     failure mode leaves an unbounded file.
//   - No multi-process locking: one Writer per file, per process.
//
// In-house instead of lumberjack: the truncate-on-rename-failure hard cap has
// no lumberjack equivalent, and this repo already prefers small owned
// primitives with adversarial tests (the leak gate precedent) over a
// dependency whose failure modes we would have to characterize anyway.
package logrotate

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	reopenEvery    = 5 * time.Second
	remindEvery    = time.Minute
	DefaultMaxMB   = 50
	DefaultBackups = 2
)

// Writer is an io.Writer with a hard size cap. Safe for concurrent Write
// (slog handlers serialize records themselves, but the agent ALSO hands this
// sink to sub-loggers — one mutex keeps every record whole).
type Writer struct {
	path     string
	maxBytes int64
	backups  int
	perm     os.FileMode

	mu             sync.Mutex
	f              *os.File
	size           int64
	degraded       bool
	closed         bool // latched by Close: never reopen (see Close's doc)
	lastReopen     time.Time
	lastRemind     time.Time
	lastRenameWarn time.Time
}

// Open creates the Writer and its file (append; stat-seeded so a pre-existing
// multi-GB legacy file rotates away on the FIRST write past the cap rather
// than growing forever from its inherited size).
func Open(path string, maxBytes int64, backups int, perm os.FileMode) (*Writer, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMB << 20
	}
	if backups <= 0 {
		backups = DefaultBackups
	}
	w := &Writer{path: path, maxBytes: maxBytes, backups: backups, perm: perm}
	if err := w.openLocked(); err != nil {
		// Degraded from birth (unwritable dir): the Writer still works —
		// records spill to stderr and reopen attempts continue on cadence.
		w.noteDegradedLocked(err)
	}
	return w, nil
}

func (w *Writer) openLocked() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, w.perm)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.f = f
	w.size = st.Size()
	if w.degraded {
		w.degraded = false
		fmt.Fprintf(os.Stderr, "tether logrotate: sink %s recovered\n", w.path)
	}
	return nil
}

func (w *Writer) noteDegradedLocked(err error) {
	now := time.Now()
	if !w.degraded {
		w.degraded = true
		w.lastRemind = now
		fmt.Fprintf(os.Stderr, "tether logrotate: sink %s DEGRADED (spilling to stderr): %v\n", w.path, err)
		return
	}
	if now.Sub(w.lastRemind) >= remindEvery {
		w.lastRemind = now
		fmt.Fprintf(os.Stderr, "tether logrotate: sink %s still degraded: %v\n", w.path, err)
	}
}

// Write implements io.Writer: rotate-then-write when p would cross the cap;
// spill to stderr while degraded.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.Stderr.Write(p)
	}
	if w.f == nil {
		now := time.Now()
		if now.Sub(w.lastReopen) >= reopenEvery {
			w.lastReopen = now
			if err := w.openLocked(); err != nil {
				w.noteDegradedLocked(err)
			}
		}
		if w.f == nil {
			// Spill: the record must land SOMEWHERE (stderr goes to
			// journald / the boot.err sink depending on the role's wiring).
			return os.Stderr.Write(p)
		}
	}
	if w.size+int64(len(p)) > w.maxBytes && w.size > 0 {
		w.rotateLocked()
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	if err != nil {
		_ = w.f.Close()
		w.f = nil
		w.noteDegradedLocked(err)
		// Spill the TAIL the file did not take (internal review): reporting
		// n==len(p) with a short write silently truncates the record, and a
		// log sink that eats half a line is worse than one that says so.
		// stderr is the same fallback the no-file branch uses.
		if n < len(p) {
			if m, serr := os.Stderr.Write(p[n:]); serr == nil {
				n += m
			}
		}
	}
	return n, nil
}

// rotateLocked shifts path → path.1 → … → path.K (oldest dropped) and opens a
// fresh file. A rename failure falls back to TRUNCATING in place with a
// marker line — a Writer must never leave an unbounded file behind.
func (w *Writer) rotateLocked() {
	_ = w.f.Close()
	w.f = nil
	for i := w.backups; i >= 2; i-- {
		_ = os.Rename(w.path+"."+strconv.Itoa(i-1), w.path+"."+strconv.Itoa(i))
	}
	if err := os.Rename(w.path, w.path+".1"); err != nil {
		if f, terr := os.OpenFile(w.path, os.O_WRONLY|os.O_TRUNC, w.perm); terr == nil {
			// The marker is RATE-LIMITED for the same reason the degraded
			// reminder is: a permanently un-renameable parent (read-only mount,
			// wrong ownership) rotates on every cap crossing, and a marker per
			// rotation is itself log volume — it re-crosses the cap and
			// re-triggers the next rotation, so the file oscillates at
			// marker+record instead of settling. Once per remindEvery keeps the
			// condition visible without becoming the thing it reports on.
			if now := time.Now(); now.Sub(w.lastRenameWarn) >= remindEvery {
				w.lastRenameWarn = now
				_, _ = fmt.Fprintf(f, "tether logrotate: rotation rename failed (%v); truncated in place\n", err)
			}
			_ = f.Close()
		}
	}
	if err := w.openLocked(); err != nil {
		w.noteDegradedLocked(err)
	}
}

// Path reports the file this Writer owns (callers derive sibling paths from
// it — e.g. the agent's boot.err panic sink next to agent.log).
func (w *Writer) Path() string { return w.path }

// Close releases the file and LATCHES the Writer closed: further Writes
// spill to stderr and never reopen the file.
//
// The latch matters (internal review): without it, `w.f == nil` is
// indistinguishable from the degraded state, so the next Write past the
// reopen cadence would re-open the very file Close just released —
// resurrecting a sink the caller believed finished, and racing whatever took
// ownership of the path afterwards.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
