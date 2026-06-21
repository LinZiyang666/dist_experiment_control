package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go/jetstream"
)

// callItem is a tiny histItem builder for the drain-core tests. The
// index is carried in `verb` (printed as verb=exec-<i>) so tests can
// assert exactly WHICH entries survived a ring-buffer flush.
func callItem(i int, drained bool) histItem {
	return histItem{
		subject: proto.SubjAuditCall("lab"),
		data: []byte(fmt.Sprintf(
			`{"v":1,"kind":"call","ts":"2026-06-17T00:00:00Z","session":"lab","node":"n","actor_fp":"SHA256:x","verb":"exec-%d","ok":true}`,
			i)),
		drained: drained,
	}
}

// TestDrainHistorySnapshotSlowFirstMessageReplaysAll is THE regression
// for the remote/WSS flakiness. The old design armed a 250ms idle
// timer at function entry; if the first message landed after that
// window (consumer create + seek + RTT on a far broker), the snapshot
// returned having printed nothing. The new design has no idle window:
// when the backlog size is known (Info() succeeded), it simply waits
// on the channel for as long as the messages take.
//
// Here the first item is delayed 400ms — far longer than any old
// 250ms idle window — yet, because grace is generous (2s > the delay),
// all 3 entries are still replayed: a slow first message within the
// generous grace is never mistaken for a drained/empty stream.
func TestDrainHistorySnapshotSlowFirstMessageReplaysAll(t *testing.T) {
	ch := make(chan histItem)
	go func() {
		time.Sleep(400 * time.Millisecond)
		ch <- callItem(1, false)
		ch <- callItem(2, false)
		ch <- callItem(3, true)
		close(ch)
	}()

	var out bytes.Buffer
	start := time.Now()
	if err := drainHistorySnapshot(context.Background(), ch, &out, true /*known*/, 3, 2*time.Second /*grace > delay*/); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("returned in %v — must have waited for the slow first message, not a timer", elapsed)
	}
	if c := strings.Count(out.String(), "CALL"); c != 3 {
		t.Fatalf("slow first message truncated output: got %d CALL lines, want 3\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotStopsOnDrained: completion is the drained
// flag (NumPending==0), not the backlog count — so even if `pending`
// is stale-high, the snapshot stops the instant JetStream says this
// was the last matching message, and does NOT read items buffered
// after it.
func TestDrainHistorySnapshotStopsOnDrained(t *testing.T) {
	ch := make(chan histItem, 8)
	ch <- callItem(1, false)
	ch <- callItem(2, true)  // drained here
	ch <- callItem(3, false) // must NOT be read
	// channel intentionally left open: if drain over-reads it will
	// block and the test deadline below will catch the hang.

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drainHistorySnapshot(context.Background(), ch, &out, true /*known*/, 100 /*stale-high*/, time.Second)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain over-read past the drained message and blocked")
	}
	if c := strings.Count(out.String(), "CALL"); c != 2 {
		t.Fatalf("expected exactly 2 CALL lines up to the drained message, got %d\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotCountBoundStops: when no item ever carries
// the drained flag (e.g. a continuously-written stream whose
// NumPending never settles to 0), the seen>=pending belt-and-braces
// bound still terminates the snapshot at the backlog size read up
// front.
func TestDrainHistorySnapshotCountBoundStops(t *testing.T) {
	ch := make(chan histItem, 8)
	ch <- callItem(1, false)
	ch <- callItem(2, false)
	ch <- callItem(3, false) // beyond the pending=2 bound, must NOT be read

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drainHistorySnapshot(context.Background(), ch, &out, true /*known*/, 2 /*pending*/, time.Second)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("count bound did not terminate the snapshot")
	}
	if c := strings.Count(out.String(), "CALL"); c != 2 {
		t.Fatalf("count bound should stop at 2, got %d\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotUnknownEmptyGraceFires: when the backlog
// size is unknown (Info() failed) and nothing ever arrives, the
// first-message grace is the only escape — it must fire and return
// cleanly rather than hang. (In production the grace is generous;
// here it is tiny to keep the test fast.)
func TestDrainHistorySnapshotUnknownEmptyGraceFires(t *testing.T) {
	ch := make(chan histItem) // never sends, never closes

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drainHistorySnapshot(context.Background(), ch, &out, false /*unknown*/, 0, 50*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown-size empty stream hung — grace did not fire")
	}
	if got := out.String(); got != "" {
		t.Fatalf("unknown empty produced output: %q", got)
	}
}

// TestDrainHistorySnapshotUnknownSlowFirstNotCutByGrace: with unknown
// size but a generous grace, a slow-but-real first message (shorter
// than the grace) must NOT be mistaken for empty. This is the
// unknown-size analogue of the remote-RTT race.
func TestDrainHistorySnapshotUnknownSlowFirstNotCutByGrace(t *testing.T) {
	ch := make(chan histItem)
	go func() {
		time.Sleep(150 * time.Millisecond) // < grace below
		ch <- callItem(1, true)
		close(ch)
	}()

	var out bytes.Buffer
	if err := drainHistorySnapshot(context.Background(), ch, &out, false /*unknown*/, 0, time.Second /*grace > delay*/); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if c := strings.Count(out.String(), "CALL"); c != 1 {
		t.Fatalf("generous grace wrongly cut a real slow first message: got %d CALL lines\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotCtxCancel: ctx cancellation returns ctx.Err.
func TestDrainHistorySnapshotCtxCancel(t *testing.T) {
	ch := make(chan histItem) // blocks forever
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- drainHistorySnapshot(ctx, ch, &bytes.Buffer{}, true, 5, time.Hour)
	}()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ctx error on cancel, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return after ctx cancel")
	}
}

// TestDrainHistoryFilteredTailKeepsLastN: the ring keeps only the last
// n matching entries and flushes them in arrival order on drain.
func TestDrainHistoryFilteredTailKeepsLastN(t *testing.T) {
	const total, n = 10, 3
	ch := make(chan histItem, total)
	for i := 1; i <= total; i++ {
		ch <- callItem(i, i == total) // last one drains
	}

	var out bytes.Buffer
	if err := drainHistoryFilteredTail(context.Background(), ch, &out, n, true /*known*/, total, time.Second); err != nil {
		t.Fatalf("drain tail: %v", err)
	}
	got := out.String()
	if c := strings.Count(got, "CALL"); c != n {
		t.Fatalf("filtered tail should keep last %d, got %d\n%s", n, c, got)
	}
	// The ring must hold the LAST n (i=8,9,10) and have dropped the
	// older ones (i=1..7).
	for _, want := range []string{"verb=exec-8", "verb=exec-9", "verb=exec-10"} {
		if !strings.Contains(got, want) {
			t.Errorf("kept set missing %q\n%s", want, got)
		}
	}
	if strings.Contains(got, "verb=exec-1 ") || strings.Contains(got, "verb=exec-7") {
		t.Errorf("ring kept an entry it should have evicted\n%s", got)
	}
}

// TestDrainHistoryFilteredTailEmptyChannelFlushesNothing: a closed,
// never-fed channel (the feed goroutine hit Next()-error with no
// matching messages) flushes an empty ring and returns.
func TestDrainHistoryFilteredTailEmptyChannelFlushesNothing(t *testing.T) {
	ch := make(chan histItem)
	close(ch)
	var out bytes.Buffer
	if err := drainHistoryFilteredTail(context.Background(), ch, &out, 100, false, 0, time.Second); err != nil {
		t.Fatalf("drain tail: %v", err)
	}
	if got := out.String(); got != "" {
		t.Fatalf("empty filtered tail produced output: %q", got)
	}
}

// --- feedHistory leak-safety + drained-tagging, via a tiny fake -------
//
// The project deliberately avoids the goleak library and a real-NATS
// goroutine count (the embedded server spawns its own per-consumer
// goroutines that pollute the baseline — see test/concurrency). These
// fakes exercise the ONE goroutine this change adds, deterministically.

var errFakeIterClosed = errors.New("fake iterator closed")

// fakeMsg embeds the jetstream.Msg interface (nil) so it satisfies the
// type while only the three methods feedHistory calls are implemented.
type fakeMsg struct {
	jetstream.Msg
	subject string
	data    []byte
	pending uint64
	metaErr error
}

func (m *fakeMsg) Subject() string { return m.subject }
func (m *fakeMsg) Data() []byte    { return m.data }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{NumPending: m.pending}, nil
}

// fakeMsgCtx is a controllable jetstream.MessagesContext. Next blocks
// on either a delivered message or Stop(); Stop is idempotent.
type fakeMsgCtx struct {
	jetstream.MessagesContext
	msgs     chan jetstream.Msg
	stopped  chan struct{}
	stopOnce sync.Once
}

func newFakeMsgCtx() *fakeMsgCtx {
	return &fakeMsgCtx{msgs: make(chan jetstream.Msg, 16), stopped: make(chan struct{})}
}

func (f *fakeMsgCtx) Next(_ ...jetstream.NextOpt) (jetstream.Msg, error) {
	select {
	case m := <-f.msgs:
		return m, nil
	case <-f.stopped:
		return nil, errFakeIterClosed
	}
}

func (f *fakeMsgCtx) Stop()                 { f.stopOnce.Do(func() { close(f.stopped) }) }
func (f *fakeMsgCtx) deliver(m jetstream.Msg) { f.msgs <- m }

// TestFeedHistoryExitsOnIteratorStop: when the iterator errors (Stop),
// the reader goroutine closes ch and returns — no leak.
func TestFeedHistoryExitsOnIteratorStop(t *testing.T) {
	fc := newFakeMsgCtx()
	ch := make(chan histItem, 4)
	go feedHistory(context.Background(), fc, ch)

	fc.deliver(&fakeMsg{subject: proto.SubjAuditCall("lab"), data: []byte("{}"), pending: 1})
	fc.deliver(&fakeMsg{subject: proto.SubjAuditCall("lab"), data: []byte("{}"), pending: 0})
	<-ch
	<-ch
	fc.Stop()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected ch closed after iterator stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("feedHistory goroutine did not exit after iterator stop")
	}
}

// TestFeedHistoryExitsOnCtxCancelWhileSending: when the drain loop has
// stopped reading and the reader is blocked sending on ch, cancelling
// ctx must unblock and return it — the send-side select on ctx.Done is
// what prevents the leak.
func TestFeedHistoryExitsOnCtxCancelWhileSending(t *testing.T) {
	fc := newFakeMsgCtx()
	ch := make(chan histItem) // unbuffered, never read → send blocks
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { feedHistory(ctx, fc, ch); close(done) }()

	fc.deliver(&fakeMsg{subject: proto.SubjAuditCall("lab"), data: []byte("{}"), pending: 1})
	time.Sleep(50 * time.Millisecond) // let it reach the blocked send
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("feedHistory leaked: did not return on ctx cancel while sending")
	}
}

// TestFeedHistoryTagsDrainedFromNumPending: the drained flag is derived
// from the message's NumPending metadata (0 → drained).
func TestFeedHistoryTagsDrainedFromNumPending(t *testing.T) {
	fc := newFakeMsgCtx()
	ch := make(chan histItem, 4)
	go feedHistory(context.Background(), fc, ch)
	defer fc.Stop()

	fc.deliver(&fakeMsg{subject: proto.SubjAuditCall("lab"), data: []byte("{}"), pending: 3})
	fc.deliver(&fakeMsg{subject: proto.SubjAuditCall("lab"), data: []byte("{}"), pending: 0})

	if a := <-ch; a.drained {
		t.Error("NumPending=3 must not be tagged drained")
	}
	if b := <-ch; !b.drained {
		t.Error("NumPending=0 must be tagged drained")
	}
}

// TestFeedHistoryMetadataErrorNotDrained: if msg.Metadata() errors the
// item must be forwarded with drained=false (never silently dropped or
// mis-tagged as the last entry, which would truncate a snapshot).
func TestFeedHistoryMetadataErrorNotDrained(t *testing.T) {
	fc := newFakeMsgCtx()
	ch := make(chan histItem, 4)
	go feedHistory(context.Background(), fc, ch)
	defer fc.Stop()

	fc.deliver(&fakeMsg{subject: proto.SubjAuditCall("lab"), data: []byte(`{"verb":"exec-9"}`), metaErr: errors.New("no metadata")})
	it := <-ch
	if it.drained {
		t.Error("metadata error must not be tagged drained")
	}
	if !strings.Contains(string(it.data), "exec-9") {
		t.Errorf("item not forwarded intact: %q", it.data)
	}
}

// TestDrainHistoryFilteredTailTerminatesWithoutDrained is the must-fix
// #1 guard: on an actively-written --kind stream the per-message
// NumPending may never settle to 0, so NO item is ever tagged drained.
// With the channel left OPEN (more matching writes could come), the
// rolling idle guard must still terminate the drain and flush the last
// n. Before the fix this path's only exits were a drained item /
// channel close / ctx — so it blocked forever.
func TestDrainHistoryFilteredTailTerminatesWithoutDrained(t *testing.T) {
	ch := make(chan histItem, 8)
	for i := 1; i <= 5; i++ {
		ch <- callItem(i, false) // NONE drained
	}
	// channel deliberately left open

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		// known=false (the real --kind case), small grace so the idle
		// fallback fires quickly once the backlog goes quiet.
		done <- drainHistoryFilteredTail(context.Background(), ch, &out, 3, false, 0, 60*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain tail: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("filtered tail with no drained item never terminated (liveness gap)")
	}
	if c := strings.Count(out.String(), "CALL"); c != 3 {
		t.Fatalf("expected last 3 entries flushed, got %d\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotUnknownTerminatesWithoutDrained: the default
// --kind (no -n) path uses drainHistorySnapshot with known=false. Same
// liveness requirement — it must terminate via the rolling idle guard
// when no item is ever tagged drained, printing everything it saw.
func TestDrainHistorySnapshotUnknownTerminatesWithoutDrained(t *testing.T) {
	ch := make(chan histItem, 8)
	for i := 1; i <= 4; i++ {
		ch <- callItem(i, false)
	}
	// left open

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drainHistorySnapshot(context.Background(), ch, &out, false, 0, 60*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown-size snapshot with no drained item never terminated")
	}
	if c := strings.Count(out.String(), "CALL"); c != 4 {
		t.Fatalf("expected all 4 entries printed before idle, got %d\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotKnownCountBoundIsFastPath: for a KNOWN-size
// backlog whose items are NONE tagged drained, the count bound
// (seen>=pending) terminates as soon as `pending` items arrive — well
// before the generous idle grace — and the still-open channel does not
// hang it. Pins that completion does not depend on a drained flag when
// the size is known, and the generous grace is not on the fast path.
func TestDrainHistorySnapshotKnownCountBoundIsFastPath(t *testing.T) {
	ch := make(chan histItem, 8)
	for i := 1; i <= 4; i++ {
		ch <- callItem(i, false) // NONE drained → count bound must stop it
	}
	ch <- callItem(99, false) // beyond pending=4, must NOT be read
	// channel left open

	var out bytes.Buffer
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- drainHistorySnapshot(context.Background(), ch, &out, true /*known*/, 4, 2*time.Second /*generous*/)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("count bound did not terminate well before the 2s idle grace")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("count bound was not the fast path: took %v", elapsed)
	}
	if c := strings.Count(out.String(), "CALL"); c != 4 {
		t.Fatalf("known-size snapshot must print exactly the 4 bounded entries, got %d\n%s", c, out.String())
	}
}

// TestDrainHistorySnapshotKnownStallReturnsIncomplete: for a KNOWN-size
// backlog that proves messages exist, an idle stall before any arrive
// must NOT be reported as a clean (nil, empty) snapshot — that is the
// silent-truncation failure class this fix exists to remove. It must
// return a non-nil incomplete-replay error.
func TestDrainHistorySnapshotKnownStallReturnsIncomplete(t *testing.T) {
	ch := make(chan histItem) // never delivers
	var out bytes.Buffer
	err := drainHistorySnapshot(context.Background(), ch, &out, true /*known*/, 3 /*pending*/, 40*time.Millisecond)
	if err == nil {
		t.Fatal("known-size stall returned nil success — silent truncation")
	}
	if !strings.Contains(err.Error(), "incomplete replay") {
		t.Fatalf("want incomplete-replay error, got %v", err)
	}
	if out.String() != "" {
		t.Fatalf("no output expected on a 0-delivered stall, got %q", out.String())
	}
}

// TestDrainHistorySnapshotKnownPartialStallReturnsIncomplete: some
// entries arrive, then delivery stalls before the known bound. The
// drain prints what it got (exercising the idle branch's seen
// accounting) AND returns an incomplete error naming the shortfall —
// never a nil success.
func TestDrainHistorySnapshotKnownPartialStallReturnsIncomplete(t *testing.T) {
	ch := make(chan histItem, 8)
	ch <- callItem(1, false)
	ch <- callItem(2, false)
	// channel left open, no more → stall; pending=5

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drainHistorySnapshot(context.Background(), ch, &out, true /*known*/, 5 /*pending*/, 60*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("partial known stall returned nil success")
		}
		if !strings.Contains(err.Error(), "2 of 5") {
			t.Fatalf("error should name the shortfall '2 of 5', got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain neither completed nor erred on a known partial stall")
	}
	if c := strings.Count(out.String(), "CALL"); c != 2 {
		t.Fatalf("the 2 delivered entries must still print, got %d\n%s", c, out.String())
	}
}

// TestDrainHistoryFilteredTailKnownStallReturnsIncomplete: the filtered
// tail's known-size branch gets the same contract — flush the partial
// ring AND return incomplete, never a silent nil.
func TestDrainHistoryFilteredTailKnownStallReturnsIncomplete(t *testing.T) {
	ch := make(chan histItem, 8)
	ch <- callItem(1, false)
	ch <- callItem(2, false)
	ch <- callItem(3, false) // ring n=2 keeps the last 2
	// open → stall; pending=10

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drainHistoryFilteredTail(context.Background(), ch, &out, 2 /*n*/, true /*known*/, 10 /*pending*/, 60*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "incomplete replay") {
			t.Fatalf("want incomplete-replay error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("filtered tail did not terminate on a known partial stall")
	}
	if c := strings.Count(out.String(), "CALL"); c != 2 {
		t.Fatalf("ring (last 2) must still flush, got %d\n%s", c, out.String())
	}
}

// TestHistoryReplayBounds table-tests the pure derivation that feeds
// known/pending/start-seq into the run funcs — the arithmetic the
// original user-facing bug lived next to (newHistoryCmd), pinned without
// standing up NATS.
func TestHistoryReplayBounds(t *testing.T) {
	const lastSeq = 1000
	cases := []struct {
		name       string
		kind       string
		lastN      int
		streamMsgs uint64
		want       historyBounds
	}{
		{"full nonempty", "", 0, 50, historyBounds{known: true, pending: 50}},
		{"full empty", "", 0, 0, historyBounds{known: true, pending: 0}},
		{"-n < msgs", "", 5, 50, historyBounds{startSeq: lastSeq - 5 + 1, useStartSeq: true, known: true, pending: 5}},
		{"-n >= msgs", "", 80, 50, historyBounds{startSeq: 1, useStartSeq: true, known: true, pending: 50}},
		{"-n on empty", "", 5, 0, historyBounds{startSeq: 1, useStartSeq: true, known: true, pending: 0}},
		{"kind nonempty", "call", 0, 50, historyBounds{known: false, pending: 50}},
		{"kind empty", "call", 0, 0, historyBounds{known: true, pending: 0}},
		{"kind -n nonempty", "call", 10, 50, historyBounds{known: false, pending: 0}},
		{"kind -n empty", "call", 10, 0, historyBounds{known: true, pending: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := historyReplayBounds(tc.kind, tc.lastN, tc.streamMsgs, lastSeq)
			if got != tc.want {
				t.Errorf("historyReplayBounds(%q,%d,%d,%d) = %+v, want %+v",
					tc.kind, tc.lastN, tc.streamMsgs, lastSeq, got, tc.want)
			}
		})
	}
}
