package xfersend

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.2 / §11.3 / §12.2 P08 / T19
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeGoroutines := runtime.NumGoroutine()
	beforeFDs := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "sender lifecycle", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "sender lifecycle", beforeFDs, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	data := bytes.Repeat([]byte("l"), 2*65536)
	f := newSenderFixture(t, data)
	requireSeal(t, f)
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	first, err := f.actor.Freeze(f.deadline())
	if err != nil || first.Offset != 65536 || first.Source.Kind != x.ResourceSource || first.Source.ResourceID == "" || first.Source.Name != x.SourceFileName {
		t.Fatalf("freeze lost prefix/source closure: %+v err=%v", first.Source, err)
	}
	if first.SourceIdentity.Ino == 0 || first.SourceIdentity != f.actor.(*sender).state.SourceIdentity {
		t.Fatal("freeze lost original descriptor identity")
	}
	hash := sha256.New()
	if err := hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(first.SHA256State); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hash.Sum(nil), sha256Sum(data[:65536])) {
		t.Fatal("exported digest covers unacknowledged source bytes")
	}
	first.SHA256State[0]++
	first.Writer.Receipts[0].Offset++
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	current, err := f.actor.Freeze(f.deadline())
	if err != nil || current.Offset != int64(len(data)) || current.Writer.Receipts[0].Offset != 0 || current.ByteDeadline != first.ByteDeadline {
		t.Fatalf("current snapshot regressed or borrowed prior bytes: offset=%d err=%v", current.Offset, err)
	}
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	requireStop(t, f)
	if err := f.actor.Stop(f.deadline()); err != nil {
		t.Fatalf("second Stop repeated retirement: %v", err)
	}
	if _, err := f.file.Stat(); err == nil {
		t.Fatal("normal Stop retained the original source FD")
	}
	if err := f.actor.Step(f.deadline()); err == nil || !strings.Contains(err.Error(), x.CodeCanceled) {
		t.Fatalf("stopped sender admitted bytes: %v", err)
	}

	// A blocked writer is an already-entered callback. Freeze must time out
	// without closing its file; after it returns, the original task continues.
	f = newSenderFixture(t, data)
	requireSeal(t, f)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	f.writer.entered, f.writer.blocked = entered, unblock
	done := make(chan error, 1)
	go func() { done <- f.actor.Step(f.deadline()) }()
	awaitSenderSignal(t, entered)
	_, freezeErr := f.actor.Freeze(f.deadlineAfter(10 * time.Millisecond))
	if !errors.Is(freezeErr, controlbudget.ErrExpired) {
		close(unblock)
		joined := awaitSenderResult(t, done)
		t.Fatalf("blocked callback freeze: %v (joined step: %v)", freezeErr, joined)
	}
	if _, err := f.file.Stat(); err != nil || !f.capacity.held {
		t.Fatal("freeze timeout destroyed original ownership")
	}
	close(unblock)
	if err := awaitSenderResult(t, done); err != nil {
		t.Fatal(err)
	}
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatalf("original task did not continue after failed freeze: %v", err)
	}
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	requireStop(t, f)

	// Joining Stop has the same ownership rule, but permanently closes future
	// byte admission. An offline ordinary transport is irrelevant to retirement.
	f = newSenderFixture(t, data)
	requireSeal(t, f)
	entered, unblock = make(chan struct{}), make(chan struct{})
	f.writer.entered, f.writer.blocked = entered, unblock
	done = make(chan error, 1)
	go func() { done <- f.actor.Step(f.deadline()) }()
	awaitSenderSignal(t, entered)
	stopErr := f.actor.Stop(f.deadlineAfter(10 * time.Millisecond))
	if !errors.Is(stopErr, controlbudget.ErrExpired) {
		close(unblock)
		joined := awaitSenderResult(t, done)
		t.Fatalf("blocked callback Stop: %v (joined step: %v)", stopErr, joined)
	}
	if _, err := f.file.Stat(); err != nil || !f.capacity.held {
		t.Fatal("Stop timeout discarded unjoined source ownership")
	}
	close(unblock)
	// Stop closes FUTURE admission only: the already-entered append still
	// completes against its original owner.
	if err := awaitSenderResult(t, done); err != nil {
		t.Fatalf("entered callback did not complete after Stop timed out: %v", err)
	}
	f.transport.reply = func(x.Request) (x.Response, x.Attempt, error) {
		return x.Response{}, x.Attempt{}, errors.New("ordinary transport revoked")
	}
	requests := len(f.transport.requests)
	f.writer.stopErr = errors.New("writer retirement uncertain")
	if err := f.actor.Stop(f.deadline()); err == nil {
		t.Fatal("writer retirement error was discarded")
	}
	if _, err := f.file.Stat(); err != nil || !f.capacity.held {
		t.Fatal("uncertain writer retirement closed source ownership")
	}
	f.writer.stopErr = nil
	requireStop(t, f)
	if len(f.transport.requests) != requests {
		t.Fatal("Stop required revoked business transport")
	}

	// Stop interrupts a real rate wait instead of waiting for the byte budget.
	f = newSenderFixture(t, data)
	f.config.Intent.RateLimitBPS, f.config.Budget.RateLimitBPS, f.transport.budget.RateLimitBPS = 65536, 65536, 65536
	f.recreate(t)
	requireSeal(t, f)
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	done = make(chan error, 1)
	go func() { done <- f.actor.Step(f.deadline()) }()
	until := time.Now().Add(5 * time.Second)
	for len(f.actor.(*sender).gate) == 0 && time.Now().Before(until) {
		runtime.Gosched()
	}
	if len(f.actor.(*sender).gate) == 0 {
		t.Fatal("second Step never entered its rate wait")
	}
	if err := f.actor.Stop(f.deadlineAfter(100 * time.Millisecond)); err != nil {
		t.Fatalf("Stop failed to interrupt rate wait: %v", err)
	}
	if err := awaitSenderResult(t, done); err == nil || !strings.Contains(err.Error(), x.CodeCanceled) || f.writer.appends != 1 {
		t.Fatalf("rate wait survived cancellation: err=%v appends=%d", err, f.writer.appends)
	}
	if f.capacity.held {
		t.Fatal("canceled rate waiter retained capacity")
	}
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func awaitSenderSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("sender callback did not enter")
	}
}
func awaitSenderResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("sender callback did not join")
		return nil
	}
}

// origin: hot-update S7 plan §1.2 / §12.2 P08 / T19
func TestTransferSenderConstructorPreservesCallerOnFailure(t *testing.T) {
	for _, name := range []string{"missing_source", "missing_clock", "missing_utc", "missing_capacity", "missing_transport", "missing_writer", "missing_identity", "missing_version", "wrong_tier", "wrong_verb", "wrong_chunk", "negative_rate", "wrong_rate", "zero_remaining", "zero_budget", "future_start", "expired_start", "wrong_boot", "capacity_failure", "unreadable_source"} {
		t.Run(name, func(t *testing.T) {
			f := newSenderFixture(t, []byte("constructor source"))
			config := f.config
			capacity := &senderCapacity{}
			config.Capacity = capacity
			switch name {
			case "missing_source":
				config.Source = nil
			case "missing_clock":
				config.Clock = nil
			case "missing_utc":
				config.UTC = nil
			case "missing_capacity":
				config.Capacity = nil
			case "missing_transport":
				config.Transport = nil
			case "missing_writer":
				config.Writer = nil
			case "missing_identity":
				config.Identity.EntryNonce = ""
			case "missing_version":
				config.Identity.ObjectVersion = ""
			case "wrong_tier":
				config.Intent.Tier = "unknown"
			case "wrong_verb":
				config.Intent.Verb = "unknown"
			case "wrong_chunk":
				config.Intent.ChunkSize = 0
			case "negative_rate":
				config.Intent.RateLimitBPS = -1
			case "wrong_rate":
				config.Budget.RateLimitBPS = 1
			case "zero_remaining":
				config.Budget.RemainingNS = 0
			case "zero_budget":
				config.Budget.EffectiveBudgetNS = 0
			case "future_start":
				config.Budget.StartedAt = time.Now().Add(time.Second)
			case "expired_start":
				config.Budget.StartedAt = time.Now().Add(-301 * time.Second)
			case "wrong_boot":
				config.Clock = func() (string, int64, error) { return "", 1, nil }
			case "capacity_failure":
				capacity.fail = true
			case "unreadable_source":
				file, err := os.OpenFile(f.path, os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				// New takes ownership only on success, so the caller's own
				// close must still find this descriptor open after the refusal.
				defer func() {
					if err := file.Close(); err != nil {
						t.Errorf("constructor failure closed caller's unreadable source: %v", err)
					}
				}()
				config.Source = file
			}
			actor, err := New(config)
			if err == nil || actor != nil || capacity.held {
				t.Fatalf("constructor accepted invalid configuration or leaked reservation: actor=%v err=%v held=%v", actor, err, capacity.held)
			}
			if _, err := f.file.Stat(); err != nil {
				t.Fatal("constructor failure closed caller's source")
			}
			requireStop(t, f)
		})
	}
}

// origin: hot-update S7 plan §8.1 / T19: tier A has no object version/store.
// origin: FIX-INLINE — Tier A's only byte carrier is now the retained copy
// verified against the durable seal. An object writer is never that carrier,
// and a copy that cannot be verified must not stand in for one: before this
// fix the assertion pinned the absent carrier itself (payload_unsupported).
func TestTransferSenderInlineSealRejectsMissingByteCarrier(t *testing.T) {
	f := newInlineSenderFixture(t, []byte("inline source"))
	seal := requireSeal(t, f)
	if seal.Chunks != 0 || seal.ChunkSize != 0 || seal.SHA256 != digest([]byte("inline source")) {
		t.Fatalf("inline seal fabricated object geometry: %+v", seal)
	}
	withWriter := f.config
	withWriter.Writer = f.writer
	if actor, err := New(withWriter); err == nil || actor != nil || !strings.Contains(err.Error(), x.CodeIntentConflict) {
		t.Fatalf("an object writer was accepted as an inline carrier: actor=%T err=%v", actor, err)
	}
	original, err := f.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	// Same inode, same size and restored mtime: only the bytes changed after
	// the durable seal, so only the copy's own verification can refuse it.
	rewriteSource(t, f, 0, []byte("CHANGED"), original.ModTime())
	if err := f.actor.Step(f.deadline()); err == nil || !strings.Contains(err.Error(), x.CodeSourceChanged) {
		t.Fatalf("unverifiable inline copy produced false success: %v", err)
	}
	if done, ok := f.actor.(interface{ Finalized() bool }); !ok || done.Finalized() {
		t.Fatal("refused inline copy reported a finished sender")
	}
	if _, data, err := Inline(f.actor, f.deadline()); err == nil || data != nil {
		t.Fatalf("refused inline copy is still served: bytes=%d err=%v", len(data), err)
	}
	state, err := f.actor.Freeze(f.deadline())
	if err != nil || state.Inline != nil || state.Offset != 0 {
		t.Fatalf("refused inline copy crossed the image: bytes=%d offset=%d err=%v", len(state.Inline), state.Offset, err)
	}
	requireStop(t, f)
}

// newInlineSenderFixture is a Tier A pull: no object version, no writer, an
// unknown source size, and the frozen 30s inline budget.
func newInlineSenderFixture(t *testing.T, data []byte) *senderFixture {
	t.Helper()
	f := newSenderFixture(t, data)
	f.config.Intent = x.Intent{Verb: "pull", Tier: "a", Path: f.path}
	f.config.Identity.ObjectVersion, f.config.Writer = "", nil
	f.config.Budget.EffectiveBudgetNS, f.config.Budget.RemainingNS = int64(30*time.Second), int64(30*time.Second)
	f.transport.budget = f.config.Budget
	f.recreate(t)
	return f
}

// origin: hot-update S7 plan §1.2 / LQ2 / T19
func TestTransferSenderSourceBudgetRejectsOverflowAndBadGeometry(t *testing.T) {
	for _, tc := range []struct {
		size, rate int64
		want       time.Duration
	}{
		{0, 0, 300 * time.Second},
		{1, 0, 300 * time.Second},
		{proto.XferMaxBytes, 0, 2108 * time.Second},
		{proto.XferMaxBytes, 4 * 1024 * 1024, 2108 * time.Second},
		{66549440, 256 * 1024, 568 * time.Second},
		{proto.XferMaxBytes, 1, 0},
		{proto.XferMaxBytes + 1, 0, 0},
		{-1, 0, 0},
		{1, -1, 0},
	} {
		got, err := sourceBudget(x.Intent{Tier: "b", RateLimitBPS: tc.rate}, tc.size)
		if tc.want == 0 {
			if err == nil {
				t.Fatalf("invalid size/rate got a budget: size=%d rate=%d budget=%v", tc.size, tc.rate, got)
			}
		} else if err != nil || got != tc.want {
			t.Fatalf("source budget: size=%d rate=%d got=%v want=%v err=%v", tc.size, tc.rate, got, tc.want, err)
		}
	}
	if _, err := sourceBudget(x.Intent{Tier: "a"}, proto.XferTierAMaxBytes+1); err == nil {
		t.Fatal("oversize inline source admitted")
	}
	for _, chunk := range []uint32{0, 1024*1024 + 1} {
		if _, err := sourceGeometry(x.Intent{Tier: "b", ChunkSize: chunk}, 1); err == nil {
			t.Fatalf("invalid chunk geometry admitted: %d", chunk)
		}
	}
	for _, tc := range []struct {
		size int64
		want uint32
	}{{0, 0}, {65536, 1}, {65537, 2}} {
		count, err := sourceGeometry(x.Intent{Tier: "b", ChunkSize: 65536}, tc.size)
		if err != nil || count != tc.want {
			t.Fatalf("partial last chunk geometry: size=%d got=%d want=%d err=%v", tc.size, count, tc.want, err)
		}
	}
}

// origin: hot-update S7 plan §1.2 / §8.2 / T19
func TestTransferSenderRejectsForeignBootAndInvalidDeadlines(t *testing.T) {
	f := newSenderFixture(t, []byte("deadline source"))
	if _, err := f.actor.Seal(0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero Seal deadline admitted: %v", err)
	}
	if _, err := f.actor.Freeze(0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero Freeze deadline admitted: %v", err)
	}
	requireSeal(t, f)
	oldClock := f.actor.(*sender).config.Clock
	f.actor.(*sender).config.Clock = func() (string, int64, error) {
		_, now, err := oldClock()
		return "foreign-boot", now, err
	}
	if err := f.actor.Step(f.deadline()); err == nil || !strings.Contains(err.Error(), x.CodeBudgetExceeded) {
		t.Fatalf("foreign boot continued bytes: %v", err)
	}
	f.actor.(*sender).config.Clock = oldClock
	f.clockDelta.Store(int64(301 * time.Second))
	if err := f.actor.Step(f.deadline()); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("expired byte budget admitted bytes: %v", err)
	}
	if f.writer.appends != 0 {
		t.Fatal("invalid deadline produced object bytes")
	}
	requireStop(t, f)
}

// origin: hot-update S7 plan §2.2 / §8.2 / T19
func TestTransferSenderReconstructsOnlyOriginalWriterPrefix(t *testing.T) {
	for _, name := range []string{"confirmed", "pending", "finalized", "corrupt_receipt", "corrupt_pending", "corrupt_metadata", "retired"} {
		t.Run(name, func(t *testing.T) {
			data := bytes.Repeat([]byte("s"), 2*65536)
			f := newSenderFixture(t, data)
			requireSeal(t, f)
			before, err := f.actor.Freeze(f.deadline())
			if err != nil {
				t.Fatal(err)
			}
			if name == "pending" || name == "corrupt_pending" {
				f.writer.fault = "lost_ack"
			}
			err = f.actor.Step(f.deadline())
			if name == "pending" || name == "corrupt_pending" {
				if err == nil {
					t.Fatal("pending restart did not exercise a lost ACK")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if name == "finalized" || name == "corrupt_metadata" {
				for step := 0; step < 2; step++ {
					if err := f.actor.Step(f.deadline()); err != nil {
						t.Fatal(err)
					}
				}
			}
			if name == "corrupt_receipt" {
				f.writer.receipts[0].Offset++
			}
			if name == "corrupt_pending" {
				f.writer.pending.Bytes[0]++
			}
			// Simulate the registry replacing only the sleeping actor. The writer
			// and supplied file are still the original owners, with no path reopen.
			f.writer.fault = ""
			if name == "corrupt_metadata" || name == "retired" {
				f.writer.fault = name
			}
			f.recreate(t)
			requireSeal(t, f)
			err = f.actor.Step(f.deadline())
			if name == "corrupt_receipt" || name == "corrupt_pending" || name == "corrupt_metadata" || name == "retired" {
				code := x.CodeObjectConflict
				if name == "corrupt_metadata" {
					code = x.CodeObjectVersionMismatch
				}
				if err == nil || !strings.Contains(err.Error(), code) {
					t.Fatalf("corrupt inherited prefix accepted: %v", err)
				}
				requireStop(t, f)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for step := 0; step < 2; step++ {
				if err := f.actor.Step(f.deadline()); err != nil {
					t.Fatal(err)
				}
			}
			state, err := f.actor.Freeze(f.deadline())
			if err != nil || state.Offset != int64(len(data)) || state.Source != before.Source || state.SourceIdentity != before.SourceIdentity {
				t.Fatalf("restart lost original prefix/descriptor: offset=%d err=%v", state.Offset, err)
			}
			metas, published := f.writer.snapshot()
			if metas != 1 || f.writer.appends != 2 || !bytes.Equal(published, data) {
				t.Fatalf("restart replayed acknowledged bytes/metadata: appends=%d metas=%d bytes=%d", f.writer.appends, metas, len(published))
			}
			requireStop(t, f)
		})
	}
}
