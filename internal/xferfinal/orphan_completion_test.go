package xferfinal

// Copy into internal/xferfinal. Real SDK object, raw metadata and purges;
// owner census uses an explicit complete/empty fixture.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type orphanReviewJS struct {
	jetstream.JetStream
	purges int
	fault  string
}
type orphanReviewStream struct {
	jetstream.Stream
	parent *orphanReviewJS
}

func (j *orphanReviewJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	s, err := j.JetStream.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return orphanReviewStream{Stream: s, parent: j}, nil
}
func (s orphanReviewStream) Purge(ctx context.Context, opts ...jetstream.StreamPurgeOpt) error {
	s.parent.purges++
	return s.Stream.Purge(ctx, opts...)
}

// Completion faults leave the real tombstone/absence on the embedded server;
// only the transport result or raw evidence at this boundary is corrupted.
func (j *orphanReviewJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	var object jetstream.ObjectInfo
	completion := json.Unmarshal(msg.Data, &object) == nil && object.Deleted && object.Chunks == 0
	if completion && j.fault == "before_send" {
		return nil, errors.New("completion publication unavailable")
	}
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	if err == nil && completion {
		if j.fault == "after_send" {
			return nil, errors.New("completion acknowledgement lost")
		}
		if j.fault == "wrong_ack" {
			ack.Stream = "another-stream"
		}
	}
	return ack, err
}
func (s orphanReviewStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	raw, err := s.Stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return raw, err
	}
	var object jetstream.ObjectInfo
	if s.parent.fault == "foreign_readback" && json.Unmarshal(raw.Data, &object) == nil && object.Deleted && object.Chunks == 0 {
		object.NUID = "replacement"
		copy := *raw
		copy.Data, _ = json.Marshal(object)
		return &copy, nil
	}
	return raw, nil
}

// origin: S7 retention/GC adversarial review
// origin: S7 internal review rv-gc F5
func exerciseTransferLegacyOrphanPersistsCompletedRetirement(t *testing.T) {
	nc, err := nats.Connect(testharness.StartJSNATS(t), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-completed", Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.Put(t.Context(), jetstream.ObjectMeta{Name: "original"}, bytes.NewBufferString("orphan bytes"))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_xfer-completed")
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := xferledger.New(x.LedgerConfig{Base: x.Base{Clock: fixtureClock}, Root: t.TempDir(), FallbackRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	backend := &orphanReviewJS{JetStream: js}
	f := newFinalFixture(t, "unused")
	cfg := f.config()
	now := time.Now().Add(4 * time.Hour)
	cfg.UTC = func() time.Time { return now }
	cfg.Ledger = ledger
	cfg.LegacyOrphans = &x.LegacyOrphanConfig{JS: backend, Owners: &orphanOwners{local: x.LedgerCensus{Ready: true, Complete: true}}}
	port, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = port.Stop(finalDeadline(t, time.Second)) }()
	for pass := 0; pass < 2; pass++ {
		if pass != 0 {
			info, err := store.GetInfo(t.Context(), "original", jetstream.GetObjectInfoShowDeleted())
			if err != nil {
				t.Fatal(err)
			}
			if info.Deleted && info.Chunks == 0 {
				break
			}
		}
		candidate := orphanCandidate(t, stream, "xfer-completed", object.NUID)
		if _, err := port.(x.LegacyOrphanFinalizer).RetireLegacyOrphan(candidate, finalDeadline(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	metadata, err := store.GetInfo(t.Context(), "original", jetstream.GetObjectInfoShowDeleted())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Chunks != 0 || backend.purges != 1 {
		t.Fatalf("completed orphan remains a pending candidate: deleted=%v chunks=%d size=%d purge calls=%d", metadata.Deleted, metadata.Chunks, metadata.Size, backend.purges)
	}
}

// A completion marker is durable only after its CAS, ACK and raw bytes agree.
// Retry retains the original NUID and a completed marker survives a new owner.
// origin: S7 internal review rv-gc F5
func exerciseTransferLegacyOrphanCompletionRetainsUncertainWrites(t *testing.T) {
	for _, fault := range []string{"before_send", "after_send", "wrong_ack", "foreign_readback"} {
		t.Run(fault, func(t *testing.T) {
			nc, err := nats.Connect(testharness.StartJSNATS(t), nats.ReconnectBufSize(-1))
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-completion", Storage: jetstream.FileStorage})
			if err != nil {
				t.Fatal(err)
			}
			object, err := store.Put(t.Context(), jetstream.ObjectMeta{Name: "original"}, bytes.NewBufferString("original bytes"))
			if err != nil {
				t.Fatal(err)
			}
			stream, err := js.Stream(t.Context(), "OBJ_xfer-completion")
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := xferledger.New(x.LedgerConfig{Base: x.Base{Clock: fixtureClock}, Root: t.TempDir(), FallbackRoot: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			backend := &orphanReviewJS{JetStream: js, fault: fault}
			fixture := newFinalFixture(t, "")
			cfg := fixture.config()
			now := time.Now().Add(4 * time.Hour)
			cfg.UTC = func() time.Time { return now }
			cfg.Ledger = ledger
			cfg.LegacyOrphans = &x.LegacyOrphanConfig{JS: backend, Owners: &orphanOwners{local: x.LedgerCensus{Ready: true, Complete: true}}}
			port, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			candidate := orphanCandidate(t, stream, "xfer-completion", object.NUID)
			receipt, err := port.(x.LegacyOrphanFinalizer).RetireLegacyOrphan(candidate, finalDeadline(t, 5*time.Second))
			if stopErr := port.Stop(finalDeadline(t, time.Second)); stopErr != nil {
				t.Fatal(stopErr)
			}
			if err == nil || receipt != (x.LegacyOrphanReceipt{}) {
				t.Fatalf("uncertain completion returned a retirement receipt: %+v %v", receipt, err)
			}
			backend.fault = ""
			now = now.Add(time.Hour)
			info, err := store.GetInfo(t.Context(), "original", jetstream.GetObjectInfoShowDeleted())
			if err != nil {
				t.Fatal(err)
			}
			if info.Chunks != 0 {
				fresh, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				candidate = orphanCandidate(t, stream, "xfer-completion", object.NUID)
				_, retryErr := fresh.(x.LegacyOrphanFinalizer).RetireLegacyOrphan(candidate, finalDeadline(t, 5*time.Second))
				if err = fresh.Stop(finalDeadline(t, time.Second)); err != nil {
					t.Fatal(err)
				}
				if retryErr != nil {
					t.Fatal(retryErr)
				}
			} else if backend.purges != 1 {
				t.Fatalf("persisted completion purged twice after restart: %d", backend.purges)
			}
			info, err = store.GetInfo(t.Context(), "original", jetstream.GetObjectInfoShowDeleted())
			if err != nil || info.Chunks != 0 || !info.Deleted {
				t.Fatalf("completion did not converge durably: %+v %v", info, err)
			}
		})
	}
}

// origin: S7 internal review rv-gc F5
func TestTransferLegacyOrphanPersistsCompletedRetirement(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferLegacyOrphanPersistsCompletedRetirement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-gc F5
func TestTransferLegacyOrphanCompletionRetainsUncertainWrites(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferLegacyOrphanCompletionRetainsUncertainWrites(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}
