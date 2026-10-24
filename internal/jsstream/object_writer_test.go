package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §2.2 / T01
func TestTransferWriterResumesSameVersionAfterServerRestart(t *testing.T) {
	t.Run("global_fence_retry", exerciseWriterFenceRetry)
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := bytes.Repeat([]byte("restart-fixed-version"), 9000)
	spec := writerSpec(t, js, "restart", body, 32<<10)
	fault := &writerFaultJS{JetStream: js, drop: chunkSubject(spec)}
	w := mustWriter(t, fault, spec)
	if _, err := w.Append(0, body[:spec.ChunkSize], writerDeadline(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost ACK: %v", err)
	}
	rawBefore := writerRaw(t, js, spec)
	if len(rawBefore) != 1 {
		t.Fatal("lost ACK did not actually persist first chunk")
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil || state.PendingAttempt == nil || len(state.Receipts) != 0 {
		t.Fatalf("freeze lost pending: %+v %v", state, err)
	}
	js.Conn().Close()
	srv.restart(t)
	nextJS := writerJS(t, srv.url())
	restored, err := ImportTransferWriter(nextJS, state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restored.Append(0, body[:spec.ChunkSize], writerDeadline(t)); err == nil {
		t.Fatal("pristine import admitted bytes")
	}
	if err = ThawTransferWriter(restored, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := restored.ResolvePending(writerDeadline(t))
	if err != nil || !found || receipt.StreamSeq != rawBefore[0].Sequence {
		t.Fatalf("original sequence lost: %+v %v %v", receipt, found, err)
	}
	if rows := writerRaw(t, nextJS, spec); len(rows) != 1 || rows[0].Subject != chunkSubject(spec) {
		t.Fatalf("resume minted a replacement physical version: %d chunks", len(rows))
	}
	writerComplete(t, restored, spec, body, 1)
	writerVerify(t, nextJS, spec, body)
	rows := writerRaw(t, nextJS, spec)
	if rows[0].Sequence != rawBefore[0].Sequence {
		t.Fatal("restart replaced original NUID/sequence")
	}
	if _, err = restored.Append(0, bytes.Repeat([]byte("!"), int(spec.ChunkSize)), writerDeadline(t)); err == nil {
		t.Fatal("changed acknowledged bytes accepted")
	}
	if err = restored.Stop(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// Stopping byte admission is not the retention boundary: a complete original
// must remain byte-exact for old SDK readers until the proven retirement fence.
// origin: S7 internal review FIX-SDKGET F1 / INTEG-O item 7
func TestTransferFinalizedObjectRemainsSDKReadableBeforeRetirement(t *testing.T) {
	for _, cut := range []bool{false, true} {
		t.Run(fmt.Sprintf("cut-%t", cut), func(t *testing.T) {
			srv := writerServer(t, nil)
			js := writerJS(t, srv.url())
			body := bytes.Repeat([]byte("original-SDK-object\x00\xff"), 5000)
			spec := writerSpec(t, js, "sdk-retained", body, 32<<10)
			fault := &writerFaultJS{JetStream: js}
			if cut {
				fault.drop = chunkSubject(spec)
			}
			w := mustWriter(t, fault, spec)
			first, err := w.Append(0, body[:spec.ChunkSize], writerDeadline(t))
			if cut {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("original accepted chunk did not lose its ACK: %v", err)
				}
				state, freezeErr := w.Freeze(writerDeadline(t))
				if freezeErr != nil || state.PendingAttempt == nil {
					t.Fatalf("cut lost the original pending chunk: %+v %v", state, freezeErr)
				}
				w, err = ImportTransferWriter(js, state)
				if err != nil {
					t.Fatal(err)
				}
				if err = ThawTransferWriter(w, writerDeadline(t)); err != nil {
					t.Fatal(err)
				}
				var found bool
				first, found, err = w.ResolvePending(writerDeadline(t))
				if err != nil || !found {
					t.Fatalf("original chunk reconciliation failed: %+v %t %v", first, found, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			writerComplete(t, w, spec, body, 1)
			writerVerify(t, js, spec, body)
			if err = w.Stop(writerDeadline(t)); err != nil {
				t.Fatal(err)
			}
			state, err := w.Freeze(writerDeadline(t))
			if err != nil || state.MetaReceipt == nil || state.Receipts[0].StreamSeq != first.StreamSeq {
				t.Fatalf("finalized original receipt changed: %+v %v", state, err)
			}
			writerVerify(t, js, spec, body)
			proof := x.RetirementProof{Identity: spec.Identity, EntryNonce: spec.Identity.EntryNonce,
				WriterRetired: true, ConsumersRetired: true}
			if _, err = RetireTransferObject(js, &state, proof, writerDeadline(t)); err != nil {
				t.Fatal(err)
			}
			store, err := js.ObjectStore(t.Context(), spec.Bucket)
			if err != nil {
				t.Fatal(err)
			}
			if reader, getErr := store.Get(t.Context(), spec.Identity.TransferID); !errors.Is(getErr, jetstream.ErrObjectNotFound) {
				if reader != nil {
					_ = reader.Close()
				}
				t.Fatalf("retirement did not end original SDK readability: %v", getErr)
			}
		})
	}
}

func exerciseWriterFenceRetry(t *testing.T) {
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := []byte("unrelated writes may fence an attempt but cannot change its identity")
	spec := writerSpec(t, js, "fence-retry", body, 32<<10)
	w := mustWriter(t, js, spec)
	if err := w.(*objectWriter).ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	other := spec
	other.Identity.TransferID = "unrelated"
	if _, err := js.PublishMsg(t.Context(), metadataMessage(other, objectMetadata(other, false), 0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, body, writerDeadline(t)); err == nil {
		t.Fatal("stale global fence unexpectedly succeeded")
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	w, err = ImportTransferWriter(js, state)
	if err != nil {
		t.Fatal(err)
	}
	if err = ThawTransferWriter(w, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	old := w.(*objectWriter).state.PendingAttempt
	r, found, err := w.ResolvePending(writerDeadline(t))
	if err != nil || !found || r.StreamSeq != 2 {
		t.Fatalf("CAS retry: %+v %v %v", r, found, err)
	}
	if old.ExpectedStreamSeq != 0 || old.AttachmentGeneration != 1 {
		t.Fatal("retry mutated the retired attempt")
	}
	if _, err = w.Finalize(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	writerVerify(t, js, spec, body)
}

// origin: hot-update S7 plan §2.2 / T02
func TestTransferLatePublishAcrossDefaultDedupWindow(t *testing.T) {
	srv := writerServer(t, nil)
	observer := writerJS(t, srv.url())
	body := bytes.Repeat([]byte("late-publish"), 3000)
	spec := writerSpec(t, observer, "dedupe", body, 64<<10)
	stream, err := observer.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if stream.CachedInfo().Config.Duplicates != 2*time.Minute {
		t.Fatalf("test must use real default 120s, got %v", stream.CachedInfo().Config.Duplicates)
	}
	relay := newWriterRelay(t, srv.url(), chunkSubject(spec))
	js := writerJS(t, relay.url)
	w := mustWriter(t, js, spec)
	short, _ := controlbudget.After(250 * time.Millisecond)
	if _, err = w.Append(0, body, short); err == nil {
		t.Fatal("held original returned an ACK")
	}
	first := relay.next(t)
	short, _ = controlbudget.After(250 * time.Millisecond)
	if _, _, err = w.ResolvePending(short); err == nil {
		t.Fatal("held retry returned an ACK")
	}
	second := relay.next(t)
	if !bytes.Equal(first.body(), second.body()) {
		t.Fatal("retry changed original headers or bytes")
	}
	if got := writerRaw(t, observer, spec); len(got) != 0 {
		t.Fatal("absent lookup was not before original acceptance")
	}
	first.release(t)
	if err = js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	accepted := time.Now()
	if got := writerRaw(t, observer, spec); len(got) != 1 {
		t.Fatal("original was not accepted with its ACK lost")
	}
	// sleep-fixture: the pinned server's actual default dedupe window must
	// expire while an ALREADY SENT competing HPUB remains in the relay.
	timer := time.NewTimer(2*time.Minute + 5*time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	second.release(t)
	if err = js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	rows := writerRaw(t, observer, spec)
	if len(rows) != 1 {
		t.Fatalf("physical duplicate chunk after %v: got %d sequences, want 1", time.Since(accepted), len(rows))
	}
	receipt, found, err := w.ResolvePending(writerDeadline(t))
	if err != nil || !found || receipt.StreamSeq != rows[0].Sequence {
		t.Fatalf("resolve original: %+v %v %v", receipt, found, err)
	}
	if _, err = w.Finalize(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	writerVerify(t, observer, spec, body)
	t.Logf("default-window wall=%v physical_chunk_sequences=%d", time.Since(accepted), len(rows))
}

// origin: hot-update S7 plan §2.2 / T03
func TestTransferMetadataRequiresAllReceipts(t *testing.T) {
	for _, point := range []string{"last_chunk_ack", "metadata_ack", "foreign_metadata", "source_digest", "raw_receipt", "incarnation"} {
		t.Run(point, func(t *testing.T) {
			srv := writerServer(t, nil)
			js := writerJS(t, srv.url())
			body := bytes.Repeat([]byte("metadata"), 5000)
			spec := writerSpec(t, js, "metadata", body, 64<<10)
			fault := &writerFaultJS{JetStream: js}
			if point == "last_chunk_ack" {
				fault.drop = chunkSubject(spec)
			}
			if point == "metadata_ack" {
				fault.drop = metadataSubject(spec)
			}
			if point == "raw_receipt" {
				fault.corrupt = true
			}
			if point == "source_digest" {
				spec.SHA256 = strings.Repeat("0", 64)
			}
			w := mustWriter(t, fault, spec)
			_, err := w.Append(0, body, writerDeadline(t))
			if point == "raw_receipt" {
				if err == nil {
					t.Fatal("ACK accepted without exact raw header/body readback")
				}
				if _, err = w.Finalize(writerDeadline(t)); err == nil {
					t.Fatal("corrupt raw receipt exposed metadata")
				}
				return
			}
			if point == "last_chunk_ack" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("last chunk ACK was not lost: %v", err)
				}
				if _, err = w.Finalize(writerDeadline(t)); err == nil {
					t.Fatal("metadata exposed with last receipt missing")
				}
				stream, e := js.Stream(t.Context(), spec.Stream)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec)); !errors.Is(e, jetstream.ErrMsgNotFound) {
					t.Fatalf("early raw metadata: %v", e)
				}
				if _, found, e := w.ResolvePending(writerDeadline(t)); e != nil || !found {
					t.Fatalf("last chunk readback: %v %v", found, e)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if point == "source_digest" {
				if _, err = w.Finalize(writerDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeSourceChanged) {
					t.Fatalf("changed digest: %v", err)
				}
				return
			}
			if point == "foreign_metadata" {
				foreign := spec
				foreign.Identity.ObjectVersion = "foreign-version"
				if _, err = js.PublishMsg(t.Context(), metadataMessage(foreign, objectMetadata(foreign, false), 0, 1)); err != nil {
					t.Fatal(err)
				}
				if _, err = w.Finalize(writerDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeObjectVersionMismatch) {
					t.Fatalf("foreign metadata adopted: %v", err)
				}
				stream, e := js.Stream(t.Context(), spec.Stream)
				if e != nil {
					t.Fatal(e)
				}
				raw, e := stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec))
				if e != nil || !bytes.Equal(raw.Data, objectMetadata(foreign, false)) {
					t.Fatal("foreign metadata overwritten")
				}
				return
			}
			if point == "incarnation" {
				if err = js.DeleteStream(t.Context(), spec.Stream); err != nil {
					t.Fatal(err)
				}
				if _, err = js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: spec.Bucket}); err != nil {
					t.Fatal(err)
				}
				if _, err = w.Finalize(writerDeadline(t)); err == nil {
					t.Fatal("recreated stream accepted")
				}
				return
			}
			_, err = w.Finalize(writerDeadline(t))
			if point == "metadata_ack" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("metadata ACK was not lost: %v", err)
				}
				state, e := w.Freeze(writerDeadline(t))
				if e != nil {
					t.Fatal(e)
				}
				if state.Phase != x.WriteMetadataPending || state.MetaReceipt == nil || state.MetaReceipt.MetadataSeq != 0 {
					t.Fatal("lost metadata state omitted")
				}
				w, e = ImportTransferWriter(js, state)
				if e != nil {
					t.Fatal(e)
				}
				if e = ThawTransferWriter(w, writerDeadline(t)); e != nil {
					t.Fatal(e)
				}
				_, err = w.Finalize(writerDeadline(t))
			}
			if err != nil {
				t.Fatal(err)
			}
			writerVerify(t, js, spec, body)
		})
	}
}

// origin: hot-update S7 plan §2.3 / T04
func TestTransferRetirementRejectsLateReconnectPublish(t *testing.T) {
	t.Run("lost_fence_ack_and_foreign_version", exerciseRetirementEvidence)
	srv := writerServer(t, nil)
	observer := writerJS(t, srv.url())
	body := []byte("first HPUB arrives only after the original chunk subject has been purged")
	spec := writerSpec(t, observer, "retirement", body, 32<<10)
	relay := newWriterRelay(t, srv.url(), chunkSubject(spec))
	js := writerJS(t, relay.url)
	w := mustWriter(t, js, spec)
	short, _ := controlbudget.After(250 * time.Millisecond)
	if _, err := w.Append(0, body, short); err == nil {
		t.Fatal("held HPUB unexpectedly acknowledged")
	}
	late := relay.next(t)
	if err := w.Stop(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	proof := x.RetirementProof{Identity: spec.Identity, EntryNonce: spec.Identity.EntryNonce, WriterRetired: true, ConsumersRetired: true}
	missing := proof
	missing.ConsumersRetired = false
	if _, err = RetireTransferObject(observer, &state, missing, writerDeadline(t)); err == nil {
		t.Fatal("purge without consumer retirement")
	}
	receipt, err := RetireTransferObject(observer, &state, proof, writerDeadline(t))
	if err != nil || !receipt.ObjectPurged || receipt.FenceSequence <= state.PendingAttempt.ExpectedStreamSeq {
		t.Fatalf("retirement: %+v %v", receipt, err)
	}
	late.release(t)
	if err = js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if rows := writerRaw(t, observer, spec); len(rows) != 0 {
		t.Fatalf("late old HPUB resurrected %d physical chunks", len(rows))
	}
	stream, err := observer.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if stream.CachedInfo().State.LastSeq < receipt.FenceSequence {
		t.Fatal("filtered purge lowered global sequence")
	}
	raw, err := stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec))
	if err != nil || !bytes.Equal(raw.Data, objectMetadata(spec, true)) {
		t.Fatalf("missing exact tombstone: %v", err)
	}
	if _, err = w.Append(0, body, writerDeadline(t)); err == nil {
		t.Fatal("stopped writer admitted new attempt")
	}
	if again, e := RetireTransferObject(observer, &state, proof, writerDeadline(t)); e != nil || again.FenceSequence != receipt.FenceSequence {
		t.Fatalf("retirement replay: %+v %v", again, e)
	}
}

// origin: hot-update S7 plan §2.1 / T09
func TestTransferPayloadGeometryAndRTTBudget(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("encoded_limits_and_reservation", func(t *testing.T) {
		srv := writerServer(t, nil)
		js := writerJS(t, srv.url())
		spec := writerSpec(t, js, "geometry", []byte("payload"), 32<<10)
		ctl := writerJS(t, writerServer(t, func(opts *natsserver.Options) { opts.MaxPayload = 1 << 20 }).url())
		agent := writerJS(t, writerServer(t, func(opts *natsserver.Options) { opts.MaxPayload = 512 << 10 }).url())
		stream, err := js.Stream(t.Context(), spec.Stream)
		if err != nil {
			t.Fatal(err)
		}
		config := stream.CachedInfo().Config
		config.MaxMsgSize = 400 << 10
		stream, err = js.UpdateStream(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		size, err := TransferChunkSize(spec, js.Conn().MaxPayload(), ctl.Conn().MaxPayload(), agent.Conn().MaxPayload(), int64(stream.CachedInfo().Config.MaxMsgSize))
		if err != nil || size >= 400<<10 || size < 32<<10 {
			t.Fatalf("actual header subtraction: %d %v", size, err)
		}
		msg := chunkMessage(spec, x.PendingAttempt{Index: ^uint32(0), Offset: 1<<63 - 1, Length: ^uint32(0), PreviousSubjectSeq: ^uint64(0), ExpectedStreamSeq: ^uint64(0)})
		if int(size)+msg.Size()-len(msg.Subject) != 400<<10 {
			t.Fatal("geometry does not use encoded complete header")
		}
		if _, err = TransferChunkSize(spec, 32<<10); err == nil {
			t.Fatal("payload without header space admitted")
		}
		if _, err = TransferChunkSize(spec, 0, -1); err == nil {
			t.Fatal("unknown endpoint limits admitted")
		}
		oversize := spec
		oversize.ChunkSize = 1 << 20
		stream, err = js.Stream(t.Context(), spec.Stream)
		if err != nil {
			t.Fatal(err)
		}
		config = stream.CachedInfo().Config
		config.MaxMsgSize = 64 << 10
		if _, err = js.UpdateStream(t.Context(), config); err != nil {
			t.Fatal(err)
		}
		if _, err = mustWriter(t, js, oversize).Append(0, []byte("payload"), writerDeadline(t)); err == nil {
			t.Fatal("fixed geometry silently shrank to fit stream limit")
		}
		if rows := writerRaw(t, js, spec); len(rows) != 0 {
			t.Fatal("geometry refusal occurred after first chunk")
		}
		spec.Size, spec.Chunks = 2<<30, 65536
		port := mustWriter(t, js, spec)
		if cap(port.(*objectWriter).state.Receipts) != 65536 {
			t.Fatal("2 GiB minimum geometry was not reserved before IO")
		}
		spec.Chunks++
		if _, err = NewTransferWriter(js, spec); err == nil {
			t.Fatal("receipt capacity overflow accepted")
		}
		badConn, e := nats.Connect(srv.url())
		if e != nil {
			t.Fatal(e)
		}
		defer badConn.Close()
		badJS, e := jetstream.New(badConn)
		if e != nil {
			t.Fatal(e)
		}
		spec.Chunks--
		if _, e = NewTransferWriter(badJS, spec); e == nil {
			t.Fatal("default reconnect buffer admitted")
		}
	})
	// Correctness is exercised under race instrumentation too. The absolute
	// product rate floor belongs to the dedicated non-race matrix unit.
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("wss_100ms_rtt_%d", round), func(t *testing.T) { exerciseWriterRTT(t, 0) })
	}
	// Direct after the rounds (formerly deferred at the top: same point, before
	// t.Cleanup) so the leak shape gate sees the five-round loop first.
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "writer RTT", goroutines)
	testharness.AssertNoFDLeak(t, "writer RTT", fds, 0)
}

func exerciseRetirementEvidence(t *testing.T) {
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := []byte("retained until exact retirement fence ACK")
	spec := writerSpec(t, js, "fence-evidence", body, 32<<10)
	w := mustWriter(t, js, spec)
	writerComplete(t, w, spec, body, 0)
	if err := w.Stop(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	proof := x.RetirementProof{Identity: spec.Identity, EntryNonce: spec.Identity.EntryNonce, WriterRetired: true, ConsumersRetired: true}
	fault := &writerFaultJS{JetStream: js, drop: metadataSubject(spec)}
	if _, err = RetireTransferObject(fault, &state, proof, writerDeadline(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fence ACK not withheld: %v", err)
	}
	if state.RetirementFence.Purged || len(writerRaw(t, js, spec)) != 1 {
		t.Fatal("purged before fence ACK was resolved")
	}
	if _, err = RetireTransferObject(js, &state, proof, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if len(writerRaw(t, js, spec)) != 0 {
		t.Fatal("lost fence ACK could not be reconciled")
	}
	// A new same-name version must survive an old finalizer's retry.
	foreign := spec
	foreign.Identity.ObjectVersion = "foreignNUID"
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	old, err := stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.PublishMsg(t.Context(), metadataMessage(foreign, objectMetadata(foreign, false), old.Sequence, stream.CachedInfo().State.LastSeq)); err != nil {
		t.Fatal(err)
	}
	if _, err = RetireTransferObject(js, &state, proof, writerDeadline(t)); err == nil {
		t.Fatal("foreign same-name metadata adopted during retirement")
	}
	raw, err := stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec))
	if err != nil || !bytes.Equal(raw.Data, objectMetadata(foreign, false)) {
		t.Fatal("retirement deleted foreign metadata")
	}
}

func writerDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

func mustWriter(t *testing.T, js jetstream.JetStream, spec x.ObjectSpec) Writer {
	t.Helper()
	w, err := NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func writerSpec(t *testing.T, js jetstream.JetStream, bucket string, body []byte, chunk uint32) x.ObjectSpec {
	t.Helper()
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	return x.ObjectSpec{Identity: x.Identity{TransferID: "payload", ObjectVersion: "fixedNUID", IntentDigest: "intent-digest", EntryNonce: "entry-nonce"},
		Bucket: bucket, Stream: "OBJ_" + bucket, StreamCreated: stream.CachedInfo().Created.Format(time.RFC3339Nano),
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(body)), ChunkSize: chunk, Chunks: uint32((len(body) + int(chunk) - 1) / int(chunk))}
}

func writerComplete(t *testing.T, w Writer, s x.ObjectSpec, body []byte, start uint32) {
	t.Helper()
	for index := start; index < s.Chunks; index++ {
		offset := int(index) * int(s.ChunkSize)
		if _, err := w.Append(index, body[offset:offset+int(chunkLength(s, index))], writerDeadline(t)); err != nil {
			t.Fatalf("append %d: %v", index, err)
		}
	}
	if _, err := w.Finalize(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func writerRaw(t *testing.T, js jetstream.JetStream, s x.ObjectSpec) []*jetstream.RawStreamMsg {
	t.Helper()
	stream, err := js.Stream(t.Context(), s.Stream)
	if err != nil {
		t.Fatal(err)
	}
	var rows []*jetstream.RawStreamMsg
	for seq := uint64(1); ; {
		raw, err := stream.GetMsg(t.Context(), seq, jetstream.WithGetMsgSubject("$O."+s.Bucket+".C.>"))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return rows
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, raw)
		seq = raw.Sequence + 1
	}
}

func writerVerify(t *testing.T, js jetstream.JetStream, s x.ObjectSpec, body []byte) {
	t.Helper()
	rows := writerRaw(t, js, s)
	if len(rows) != int(s.Chunks) {
		t.Fatalf("raw physical chunk count %d != %d", len(rows), s.Chunks)
	}
	for index, raw := range rows {
		offset := index * int(s.ChunkSize)
		data := body[offset : offset+int(chunkLength(s, uint32(index)))]
		if raw.Subject != chunkSubject(s) || raw.Header.Get("Tether-Xfer-Index") != fmt.Sprint(index) || !bytes.Equal(raw.Data, data) {
			t.Fatalf("raw index/version/bytes changed at %d", index)
		}
	}
	store, err := js.ObjectStore(t.Context(), s.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.GetInfo(t.Context(), s.Identity.TransferID)
	if err != nil || info.NUID != s.Identity.ObjectVersion || info.Chunks != s.Chunks || info.Size != uint64(s.Size) || info.Opts.ChunkSize != s.ChunkSize {
		t.Fatalf("SDK metadata mismatch: %+v %v", info, err)
	}
	reader, err := store.Get(t.Context(), s.Identity.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, body) || sha256.Sum256(got) != sha256.Sum256(body) {
		t.Fatalf("SDK Get changed bytes: %d %v", len(got), err)
	}
}

// The server executes the real publication. The wrapper withholds only its
// successful reply, so a false-positive request failure cannot create a pass.
type writerFaultJS struct {
	jetstream.JetStream
	drop    string
	corrupt bool
}

func (j *writerFaultJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if j.corrupt {
		msg.Header.Set("Tether-Xfer-SHA256", strings.Repeat("0", 64))
	}
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	if err == nil && msg.Subject == j.drop {
		j.drop = ""
		return nil, context.DeadlineExceeded
	}
	return ack, err
}
