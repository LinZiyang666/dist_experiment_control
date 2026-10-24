package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// stallingTransferReaderJS loses the reply of an armed stream-info exchange:
// the call waits for its caller's context, exactly as nats.go waits for a
// request whose reply was lost across a server restart.
type stallingTransferReaderJS struct {
	jetstream.JetStream
	stall   atomic.Bool
	stalled atomic.Int32
}

func (j *stallingTransferReaderJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if j.stall.Load() {
		j.stalled.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return j.JetStream.Stream(ctx, name)
}

// A push receiver mid-read during a same-store JetStream restart lost one
// recovery reply and then held its worker's entry token for the rest of the
// byte budget: the original grant could not even be redelivered.
// origin: S7 FIX-CUT reader recovery stall / hot-update S7 plan §3 / T06 / M04 / T46
func TestTransferReaderBoundsLostRecoveryReplies(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferReaderLostReply(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer reader lost recovery reply", goroutines)
	testharness.AssertNoFDLeak(t, "transfer reader lost recovery reply", fds, 0)
}

func exerciseTransferReaderLostReply(t *testing.T) {
	t.Helper()
	// The subtest's cleanup stops its server before the leak census.
	t.Run("lost-stream-info-reply", func(t *testing.T) { exerciseTransferReaderLostReplyRound(t) })
}

func exerciseTransferReaderLostReplyRound(t *testing.T) {
	t.Helper()
	url := startJSNATS(t)
	observer := newJS(t, url)
	ctx, store, stream := transferObjectFixture(t, observer, "stall")
	body := make([]byte, 6*(32<<10)+5)
	for i := range body {
		body[i] = byte(i*7 + i/(32<<10))
	}
	spec := transferReaderSpec(ctx, t, observer, store, stream, body)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	actual, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	js := &stallingTransferReaderJS{JetStream: actual}
	reader, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Stop(transferReaderDeadline(t, 3*time.Second)) }()
	// A lost recovery lookup may leave an older consumer's independent duty
	// after this reader stops; settle it through the still-live observer before
	// the fixture server disappears and strands its global capacity reservation.
	// origin: hot-update S10 plan §0 / LINT-L2 full-package teardown
	t.Cleanup(func() { settleConsumerCleanup(t, nc, observer) })
	sink, err := os.CreateTemp(t.TempDir(), "sink")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()
	h := sha256.New()
	// The receiver passes its whole remaining byte budget as the read deadline.
	deadline := transferReaderDeadline(t, 40*time.Second)
	// One complete four-chunk batch leaves no buffered delivery behind.
	for range 4 {
		chunk, err := reader.ReadChunk(deadline)
		if err != nil {
			t.Fatal(err)
		}
		syncTransferReaderChunk(t, reader, sink, h, chunk)
	}
	r := reader.(*transferResumeReader)
	if err := observer.DeleteConsumer(ctx, spec.Stream, r.consumer); err != nil {
		t.Fatal(err)
	}
	js.stall.Store(true)
	started := time.Now()
	_, err = reader.ReadChunk(deadline)
	elapsed := time.Since(started)
	js.stall.Store(false)
	if err == nil || !strings.Contains(err.Error(), xfercontract.CodeOwnerRecovering) || js.stalled.Load() == 0 || elapsed > upgradebudget.TransferProgress {
		t.Fatalf("lost recovery reply held the reader: elapsed=%v stalled=%d err=%v", elapsed, js.stalled.Load(), err)
	}
	// The same reader then rebuilds from its synced prefix and finishes.
	for {
		chunk, err := reader.ReadChunk(deadline)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		syncTransferReaderChunk(t, reader, sink, h, chunk)
	}
	got, err := os.ReadFile(sink.Name())
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("reader lost bytes after a bounded recovery miss: bytes=%d err=%v", len(got), err)
	}
	if err := reader.Stop(transferReaderDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}
