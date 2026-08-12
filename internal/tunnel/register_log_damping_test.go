// register_log_damping_test.go — the #78 damper on the `read REGISTER` WARN,
// the one unauthenticated internet-triggerable log site on the tunnel control
// listener. One agent behind a half-open :7000 used to produce one WARN every
// 5s forever (the 2026-08-11 disk-fill's fuel).
// origin: docs/deploy-tier-gotchas.md #78
package tunnel

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingHandler captures slog records for counting (concurrency-safe).
type recordingHandler struct {
	mu   sync.Mutex
	recs []recordedLine
}

type recordedLine struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	rl := recordedLine{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rl.attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.recs = append(h.recs, rl)
	h.mu.Unlock()
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }
func (h *recordingHandler) count(level slog.Level, msgSub string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.recs {
		if r.level == level && strings.Contains(r.msg, msgSub) {
			n++
		}
	}
	return n
}
func (h *recordingHandler) last(level slog.Level, msgSub string) (recordedLine, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.recs) - 1; i >= 0; i-- {
		if h.recs[i].level == level && strings.Contains(h.recs[i].msg, msgSub) {
			return h.recs[i], true
		}
	}
	return recordedLine{}, false
}

// stubConn is the minimal net.Conn the damper touches (RemoteAddr only).
type stubConn struct{ net.Conn }

func (stubConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 54321}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) get() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newDampingHarness() (*Server, *recordingHandler, *fakeClock) {
	h := &recordingHandler{}
	s := NewServer("127.0.0.1:0", "127.0.0.1", nil, slog.New(h))
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	s.SetRegLogClockForTest(clk.get)
	return s, h, clk
}

func TestRegisterReadWarnDamping(t *testing.T) {
	s, h, clk := newDampingHarness()

	// A pure EOF storm (the half-open-:7000 world): 50 failures in seconds
	// must produce exactly ONE Warn, carrying the remote host (port
	// stripped) — the live incident had no way to tell WHO was dialing.
	for i := 0; i < 50; i++ {
		s.registerReadFailed(stubConn{}, io.EOF)
		clk.advance(100 * time.Millisecond)
	}
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != 1 {
		t.Fatalf("50 EOF failures must log exactly 1 Warn, got %d", got)
	}
	if last, ok := h.last(slog.LevelWarn, "read REGISTER"); !ok || last.attrs["remote"] != "203.0.113.7" {
		t.Fatalf("the Warn must carry the remote HOST, got %+v", last.attrs)
	}

	// A DIFFERENT class (timeout) is a genuinely new problem and gets its own
	// Tracker — it logs immediately (first failure of that class's run).
	s.registerReadFailed(stubConn{}, &net.OpError{Op: "read", Err: timeoutErr{}})
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != 2 {
		t.Fatalf("a new class must log its own first Warn, got %d", got)
	}

	// A successful AUTHORIZED read ends every class's run with a recovery line.
	clk.advance(1 * time.Minute)
	s.registerReadOK()
	if got := h.count(slog.LevelInfo, "recovered"); got < 1 {
		t.Fatalf("a successful read after >=Base must log recovery, got %d", got)
	}

	// The next failure after a recovery is a fresh run: it logs again.
	s.registerReadFailed(stubConn{}, io.EOF)
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != 3 {
		t.Fatalf("a fresh run after recovery must log, got %d Warns", got)
	}
}

// TestRegisterReadDampingInterleavedClasses (review M1) pins the multi-source
// fix: two coexisting failure sources of DIFFERENT classes (an EOF prober and
// a timeout half-open agent) must NOT defeat damping. With a single shared
// Tracker, every class flip logged — 100 events → ~100 WARNs, the flood
// undamped. Per-class Trackers keep it O(number of classes).
func TestRegisterReadDampingInterleavedClasses(t *testing.T) {
	s, h, clk := newDampingHarness()
	timeoutErrVal := &net.OpError{Op: "read", Err: timeoutErr{}}
	for i := 0; i < 50; i++ {
		s.registerReadFailed(stubConn{}, io.EOF)
		clk.advance(100 * time.Millisecond)
		s.registerReadFailed(stubConn{}, timeoutErrVal)
		clk.advance(100 * time.Millisecond)
	}
	// 2 classes, each paced independently. Within the ~10s the interleave
	// spans (well under the 30s Base), each class logs exactly its first —
	// so at most 2 WARNs, never ~100.
	if got := h.count(slog.LevelWarn, "read REGISTER"); got > 2 {
		t.Fatalf("interleaved classes defeated damping: %d WARNs for 100 events (want <=2)", got)
	}
	if got := h.count(slog.LevelWarn, "read REGISTER"); got < 2 {
		t.Fatalf("each of the 2 classes must still log its first WARN, got %d", got)
	}
}

// TestRegisterReadDampingReaffirmsPerCap (review Mi5) pins that a suppressed
// class re-logs once per Cap window (Due-gated) rather than being silenced for
// the whole process lifetime.
func TestRegisterReadDampingReaffirmsPerCap(t *testing.T) {
	s, h, clk := newDampingHarness()
	// Drive a class deep enough that its schedule reaches the 5min cap. Each
	// 6min gap is past the window so each reaffirms — but the LAST advance
	// leaves us with a fresh 5min window whose start we control below.
	for i := 0; i < 12; i++ {
		s.registerReadFailed(stubConn{}, io.EOF)
		clk.advance(6 * time.Minute)
	}
	// One more failure NOW anchors the window: schedule next = now + 5min (cap).
	s.registerReadFailed(stubConn{}, io.EOF)
	base := h.count(slog.LevelWarn, "read REGISTER")
	// A failure well inside that window (well under 5min) is suppressed…
	clk.advance(1 * time.Minute)
	s.registerReadFailed(stubConn{}, io.EOF)
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != base {
		t.Fatalf("a failure inside the cap window must be suppressed, got %d (base %d)", got, base)
	}
	// …and one past the cap window (>5min from the anchor) reaffirms (Due gate).
	clk.advance(5 * time.Minute)
	s.registerReadFailed(stubConn{}, io.EOF)
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != base+1 {
		t.Fatalf("a failure past the cap window must reaffirm one WARN, got %d (base %d)", got, base)
	}
}

// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F4
func TestRegisterReadDampingReaffirmationResetsSuppressedSinceLast(t *testing.T) {
	s, h, clk := newDampingHarness()
	s.registerReadFailed(stubConn{}, io.EOF) // first WARN
	clk.advance(time.Second)
	s.registerReadFailed(stubConn{}, io.EOF) // one genuinely suppressed repeat

	clk.advance(6 * time.Minute)
	s.registerReadFailed(stubConn{}, io.EOF) // reaffirmation reports that one repeat
	line, ok := h.last(slog.LevelWarn, "read REGISTER")
	if !ok || line.attrs["suppressed_since_last"] != "1" {
		t.Fatalf("first reaffirmation must report the one suppressed repeat, got %+v", line.attrs)
	}

	// No failure occurs between the two reaffirmations. The next line must
	// therefore report zero suppressed-since-last; the previously logged
	// reaffirmation is not itself a suppressed event.
	clk.advance(6 * time.Minute)
	s.registerReadFailed(stubConn{}, io.EOF)
	line, ok = h.last(slog.LevelWarn, "read REGISTER")
	if !ok || line.attrs["suppressed_since_last"] != "0" {
		t.Fatalf("reaffirmation did not reset suppressed_since_last; got %+v", line.attrs)
	}
}

func TestRegisterReadDampingAntiFlap(t *testing.T) {
	// A success/failure interleave tighter than Base must NOT turn the
	// damper into a line amplifier: the sub-Base Recover folds (no recovery
	// line) and keeps the run — so the interleave produces no extra Warns
	// either.
	s, h, clk := newDampingHarness()
	s.registerReadFailed(stubConn{}, io.EOF) // Warn #1
	for i := 0; i < 20; i++ {
		clk.advance(1 * time.Second) // < Base(30s)
		s.registerReadOK()           // folds — no recovery line
		s.registerReadFailed(stubConn{}, io.EOF)
	}
	if got := h.count(slog.LevelInfo, "recovered"); got != 0 {
		t.Fatalf("sub-Base recoveries must fold silently, got %d recovery lines", got)
	}
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != 1 {
		t.Fatalf("a tight interleave must not amplify: want 1 Warn, got %d", got)
	}
}

func TestRegisterReadDampingAccountingConserved(t *testing.T) {
	// Conservation: every failure is either logged (Warn) or counted into a
	// suppression tally that eventually surfaces (class-change Warn or
	// recovery Info). Nothing vanishes silently — swallowing lines without
	// leaving a ledger is the same defect as swallowing errors.
	s, h, clk := newDampingHarness()
	const n = 30
	for i := 0; i < n; i++ {
		s.registerReadFailed(stubConn{}, io.EOF)
		clk.advance(time.Second)
	}
	clk.advance(time.Minute)
	s.registerReadOK()
	warns := h.count(slog.LevelWarn, "read REGISTER")
	rec, ok := h.last(slog.LevelInfo, "recovered")
	if !ok {
		t.Fatal("recovery line missing")
	}
	// Single class (eof): warns(1) + suppressed(n-1) == n; nothing vanishes.
	if warns != 1 || rec.attrs["suppressed"] != "29" {
		t.Fatalf("accounting broken: warns=%d suppressed=%q want 1/%d", warns, rec.attrs["suppressed"], n-1)
	}
}

// TestRegisterReadOKOnlyAfterAuthorizedRegister (review M2) drives the REAL
// handleAgent over a net.Pipe: a parseable-but-UNAUTHORIZED REGISTER line
// (fails tokenLookup) must NOT fake a recovery Info nor re-arm the damper —
// recovery is signalled only past tokenLookup. Before the fix, registerReadOK
// ran right after the read, so any TLS-capable prober could reset the damper.
func TestRegisterReadOKOnlyAfterAuthorizedRegister(t *testing.T) {
	h := &recordingHandler{}
	rejecting := func(_, _ string, _ int, _ string, _ int64) error { return errors.New("token_mismatch") }
	s := NewServer("127.0.0.1:0", "127.0.0.1", rejecting, slog.New(h))
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	s.SetRegLogClockForTest(clk.get)

	// Arm an EOF failure run, then fail twice more so the run is well
	// established with a nonzero suppressed count (advance past Base once so a
	// recovery WOULD be loggable if registerReadOK wrongly fired).
	s.registerReadFailed(stubConn{}, io.EOF)
	clk.advance(1 * time.Minute)
	s.registerReadFailed(stubConn{}, io.EOF) // reaffirm; run continues

	// Drive handleAgent with a well-formed but bogus-token REGISTER line.
	cliConn, srvConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleAgent(context.Background(), srvConn)
		close(done)
	}()
	if _, err := cliConn.Write([]byte("REGISTER lab lab-1 14000 badtoken 0\n")); err != nil {
		t.Fatal(err)
	}
	// handleAgent writes DENY then closes; drain so it doesn't block on write.
	go func() { _, _ = io.Copy(io.Discard, cliConn) }()
	<-done
	_ = cliConn.Close()

	if got := h.count(slog.LevelInfo, "recovered"); got != 0 {
		t.Fatalf("an UNAUTHORIZED register must not fake a recovery Info, got %d", got)
	}
	// And the damper stays armed: the run continues (not Recover-reset). A
	// re-arm would Recover the tracker, so the next failure would be a fresh
	// FIRST-of-run — logging a new WARN even with NO clock advance (Due is
	// true for a fresh tracker). With the run intact and the clock NOT
	// advanced (inside the window), the next failure is suppressed to Debug.
	warnsBefore := h.count(slog.LevelWarn, "read REGISTER")
	s.registerReadFailed(stubConn{}, io.EOF) // same instant → not Due → suppressed iff run intact
	if got := h.count(slog.LevelWarn, "read REGISTER"); got != warnsBefore {
		t.Fatalf("the damper was re-armed by an unauthorized line: extra WARN (%d -> %d)", warnsBefore, got)
	}
}

// timeoutErr satisfies net.Error with Timeout()==true.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// TestHandleAgentWiresTheDamper pins the WIRING: handleAgent's read-failure
// path must go through registerReadFailed (not a raw logger.Warn) and its
// success path through registerReadOK. Source-level, same spirit as the
// reply-egress census — deleting the wiring is invisible to the unit tests
// above.
func TestHandleAgentWiresTheDamper(t *testing.T) {
	src, err := os.ReadFile("tunnel.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, "s.registerReadFailed(conn, err)") {
		t.Fatal("handleAgent no longer routes the read-REGISTER failure through the damper")
	}
	if !strings.Contains(body, "s.registerReadOK()") {
		t.Fatal("handleAgent no longer reports the successful read to the damper (recovery lines would never fire)")
	}
	if strings.Contains(body, `s.logger.Warn("tunnel server: read REGISTER", "err", err)`) {
		t.Fatal("the undamped raw Warn is back at the read-REGISTER site")
	}
}
