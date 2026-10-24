package jsstream

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The read-only lookup may beat the outgoing append to storage. Force that
// ordering on a real stream: a missing speculative result cannot stand in for
// the exact post-ACK receipt, and cannot advance the next chunk or metadata.
// origin: S7 internal review TRANSFER-MAKETEST-2 F2 / hot-update S7 plan §2.2 / T09
func TestTransferWriterRechecksRawReceiptAfterEarlyLookup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseEarlyWriterReadback(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "early writer readback", g)
	testharness.AssertNoFDLeak(t, "early writer readback", fd, 0)
}

func exerciseEarlyWriterReadback(t *testing.T) {
	t.Helper()
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := bytes.Repeat([]byte("early-readback"), 5000)
	spec := writerSpec(t, js, "readback", body, 32<<10)
	wrapped := &earlyReadbackJS{JetStream: js, subject: chunkSubject(spec), read: make(chan struct{})}
	w := mustWriter(t, wrapped, spec)
	writerComplete(t, w, spec, body, 0)
	if !wrapped.missing || wrapped.lookups < int(spec.Chunks)+1 {
		t.Fatalf("did not observe the real early miss and later receipt: missing=%t lookups=%d", wrapped.missing, wrapped.lookups)
	}
	writerVerify(t, js, spec, body)
}

type earlyReadbackJS struct {
	jetstream.JetStream
	subject string
	read    chan struct{}
	once    sync.Once
	missing bool
	lookups int
}

func (j *earlyReadbackJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := j.JetStream.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return earlyReadbackStream{Stream: stream, owner: j}, nil
}

func (j *earlyReadbackJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if msg.Subject == j.subject {
		select {
		case <-j.read:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return j.JetStream.PublishMsg(ctx, msg, opts...)
}

type earlyReadbackStream struct {
	jetstream.Stream
	owner *earlyReadbackJS
}

func (s earlyReadbackStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	raw, err := s.Stream.GetMsg(ctx, seq, opts...)
	s.owner.lookups++
	s.owner.once.Do(func() {
		s.owner.missing = err == jetstream.ErrMsgNotFound
		close(s.owner.read)
	})
	return raw, err
}

// A durable ACK and correct payload cannot authorize a receipt whose MsgID
// names another publication. Readers share the six transfer headers, while
// the writer additionally proves its own fixed publication identity.
// origin: S7 internal review TRANSFER-MAKETEST-2 F2 / hot-update S7 plan §2.2 / T03
func TestTransferWriterRequiresOriginalMessageIDInRawReceipt(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseWriterMessageIDReadback(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "writer message identity", g)
	testharness.AssertNoFDLeak(t, "writer message identity", fd, 0)
}

func exerciseWriterMessageIDReadback(t *testing.T) {
	t.Helper()
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := []byte("an acknowledged payload still needs its original message identity")
	spec := writerSpec(t, js, "messageid", body, 32<<10)
	w := mustWriter(t, changedMessageIDJS{JetStream: js}, spec)
	if _, err := w.Append(0, body, writerDeadline(t)); err == nil || !strings.Contains(err.Error(), "transfer_object_conflict") {
		t.Fatalf("writer adopted a different publication's durable ACK: %v", err)
	}
	if _, err := w.Finalize(writerDeadline(t)); err == nil {
		t.Fatal("foreign publication identity exposed metadata")
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil || state.PendingAttempt == nil || len(state.Receipts) != 0 {
		t.Fatalf("failed raw identity advanced the source cursor: %+v %v", state.Receipts, err)
	}
}

type changedMessageIDJS struct{ jetstream.JetStream }

func (j changedMessageIDJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	msg.Header.Set(jetstream.MsgIDHeader, "different-publication")
	return j.JetStream.PublishMsg(ctx, msg, opts...)
}
