package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestHistorySnapshotDefaultReplaysEverything pins the no-flag
// path: `tether history` (no -n, no --kind, no --follow) replays
// EVERY entry in the stream and exits on idle. The companion
// TestHistoryKindTailCountsFilteredEntries pins the filtered
// ring-buffer path; this one pins the unfiltered DeliverAllPolicy
// path that the default invocation actually takes.
func TestHistorySnapshotDefaultReplaysEverything(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab"); err != nil {
		t.Fatal(err)
	}

	// 5 calls + 5 procs + 5 ports — 15 entries total. Default
	// snapshot must surface all of them.
	for i := 0; i < 5; i++ {
		publishAuditForHistoryTest(t, js, proto.SubjAuditCall("lab"), map[string]any{
			"v": 1, "kind": "call", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "lab-1", "actor_fp": "SHA256:test",
			"verb": "exec", "ok": true,
		})
		publishAuditForHistoryTest(t, js, proto.SubjAuditProc("lab"), map[string]any{
			"v": 1, "kind": "started", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "lab-1", "pid": "p", "rc": nil, "cmd": "x",
		})
		publishAuditForHistoryTest(t, js, proto.SubjAuditPort("lab"), map[string]any{
			"v": 1, "kind": "expose", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "lab-1", "port": 14000 + i, "name": "n",
		})
	}

	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runHistorySnapshot(context.Background(), cons, &out, true /*known*/, 15 /*pending*/); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	got := out.String()
	if c := strings.Count(got, "CALL"); c != 5 {
		t.Errorf("expected 5 CALL lines; got %d\n%s", c, got)
	}
	if c := strings.Count(got, "PROC"); c != 5 {
		t.Errorf("expected 5 PROC lines; got %d\n%s", c, got)
	}
	if c := strings.Count(got, "PORT"); c != 5 {
		t.Errorf("expected 5 PORT lines; got %d\n%s", c, got)
	}
}

// TestHistoryStartSeqTailReplaysExactlyLastN drives the real -n
// unfiltered path end-to-end on embedded NATS: it derives the bounds
// via historyReplayBounds (the production helper), builds the actual
// DeliverByStartSequencePolicy ordered consumer at OptStartSeq, and
// asserts runHistorySnapshot returns EXACTLY the last N entries — and
// well under historyFirstMsgGrace, proving NumPending/count (not a
// timer) drove completion. This is the shape the original bug hit
// hardest and previously had zero coverage.
func TestHistoryStartSeqTailReplaysExactlyLastN(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab"); err != nil {
		t.Fatal(err)
	}
	const total = 30
	for i := 1; i <= total; i++ {
		publishAuditForHistoryTest(t, js, proto.SubjAuditCall("lab"), map[string]any{
			"v": 1, "kind": "call", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "n", "actor_fp": "SHA256:x",
			"verb": fmt.Sprintf("exec-%d", i), "ok": true,
		})
	}
	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	run := func(t *testing.T, lastN, wantCount int, wantFirstVerb, wantLastVerb, absentVerb string) {
		t.Helper()
		b := historyReplayBounds("", lastN, info.State.Msgs, info.State.LastSeq)
		cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
			DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
			OptStartSeq:   b.startSeq,
		})
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		start := time.Now()
		if err := runHistorySnapshot(context.Background(), cons, &out, b.known, b.pending); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("-n %d took %v — NumPending/count did not drive completion", lastN, elapsed)
		}
		got := out.String()
		if c := strings.Count(got, "CALL"); c != wantCount {
			t.Fatalf("-n %d: want %d CALL lines, got %d\n%s", lastN, wantCount, c, got)
		}
		if !strings.Contains(got, "verb="+wantFirstVerb) || !strings.Contains(got, "verb="+wantLastVerb) {
			t.Errorf("-n %d: expected window [%s..%s]\n%s", lastN, wantFirstVerb, wantLastVerb, got)
		}
		if absentVerb != "" && strings.Contains(got, "verb="+absentVerb+" ") {
			t.Errorf("-n %d: entry %s should be outside the window\n%s", lastN, absentVerb, got)
		}
	}

	// N < Msgs: exactly the last 5 (exec-26..exec-30), exec-1 excluded.
	t.Run("N<Msgs", func(t *testing.T) { run(t, 5, 5, "exec-26", "exec-30", "exec-1") })
	// N >= Msgs: start=1, all 30 (exec-1..exec-30).
	t.Run("N>=Msgs", func(t *testing.T) { run(t, 80, total, "exec-1", "exec-30", "") })
}

// TestHistorySnapshotEmptyStreamExitsCleanly: a brand-new stream
// with zero entries must NOT hang — the up-front consumer-pending
// check (NumPending == 0) makes runHistorySnapshot return nil
// immediately, without blocking on a Next() that would never fire.
func TestHistorySnapshotEmptyStreamExitsCleanly(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(context.Background(), js, "empty"); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("empty"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runHistorySnapshot(context.Background(), cons, &out, true /*known*/, 0 /*empty*/)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("empty snapshot: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot on empty stream did not exit on idle window")
	}
	if got := out.String(); got != "" {
		t.Errorf("empty snapshot produced output: %q", got)
	}
}

// TestHistoryFollowStreamsLiveEntries pins the --follow path:
// runHistoryFollow subscribes with DeliverNewPolicy, prints each
// new audit entry as it lands, and returns nil when ctx is
// canceled. Two-stage: publish before-cancel, observe output,
// cancel, observe orderly return.
func TestHistoryFollowStreamsLiveEntries(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(context.Background(), js, "tail"); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("tail"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}

	var (
		out  bytes.Buffer
		mu   sync.Mutex
		safe = struct{ buf *bytes.Buffer }{buf: &out}
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runHistoryFollow(ctx, cons, &lockingWriter{mu: &mu, w: safe.buf})
	}()

	// Give the follow consumer a moment to subscribe before we
	// start publishing — DeliverNewPolicy means anything published
	// before that subscribe lands won't be delivered.
	time.Sleep(150 * time.Millisecond)

	for i := 0; i < 3; i++ {
		publishAuditForHistoryTest(t, js, proto.SubjAuditCall("tail"), map[string]any{
			"v": 1, "kind": "call", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "tail", "node": "n1", "actor_fp": "SHA256:f",
			"verb": "exec", "ok": true,
		})
	}

	// Wait until the writer has received all 3 CALL lines OR
	// the test deadline elapses. Polling avoids a fixed sleep
	// that could either flake under CPU pressure or waste time.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		c := strings.Count(out.String(), "CALL")
		mu.Unlock()
		if c >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("follow returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runHistoryFollow did not return after ctx cancel")
	}

	mu.Lock()
	got := out.String()
	mu.Unlock()
	if c := strings.Count(got, "CALL"); c != 3 {
		t.Errorf("follow: expected 3 CALL lines, got %d\n%s", c, got)
	}
}

// lockingWriter serializes writes to the shared buffer the test
// reads concurrently. The runHistoryFollow goroutine writes; the
// poll loop above reads; without the mutex `go test -race` would
// flag this even though the actual data race is harmless.
type lockingWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (w *lockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}
