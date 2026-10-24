//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A slow raw census must not defer the independent SDK read until after the
// original object's legitimate retirement. The writer and retirement are real;
// only the unrelated census lock is held through the available read window.
// origin: S7 internal review FIX-SDKGET F1 / INTEG-O item 7
// origin: S7 internal review FIX-SDKGET F2 — retain physical chunks before retirement.
func TestTransferObjectSDKReadDoesNotWaitForRawCensus(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("retained-object", func(t *testing.T) { exerciseTransferObjectSDKRead(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "independent transfer SDK reader", g)
	testharness.AssertNoFDLeak(t, "independent transfer SDK reader", fd, 0)
}

func exerciseTransferObjectSDKRead(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url, nats.ReconnectBufSize(-1))
	transferMust(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	transferMust(t, err)
	s := &transferStack{t: t, sid: "sdk-reader", nc: nc, js: js}
	bucket := proto.XferBucketName(s.sid)
	_, err = js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage})
	transferMust(t, err)
	_, err = js.CreateStream(t.Context(), jetstream.StreamConfig{Name: jsstream.HistoryStreamName(s.sid), Subjects: []string{proto.SubjAuditTransfer(s.sid)}})
	transferMust(t, err)
	stream, err := js.Stream(t.Context(), "OBJ_"+bucket)
	transferMust(t, err)
	body := bytes.Repeat([]byte("original SDK bytes\x00\xff"), 5000)
	id := x.Identity{TransferID: "original", ObjectVersion: "originalNUID", EntryNonce: "original-entry", IntentDigest: "original-intent"}
	spec := x.ObjectSpec{Identity: id, Bucket: bucket, Stream: "OBJ_" + bucket,
		StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano),
		Size:          int64(len(body)), SHA256: transferDigest(body), ChunkSize: 32 << 10,
		Chunks: uint32((len(body) + (32 << 10) - 1) / (32 << 10))}
	writer, err := jsstream.NewTransferWriter(js, spec)
	transferMust(t, err)
	deadline, err := controlbudget.After(10 * time.Second)
	transferMust(t, err)
	for index := uint32(0); index < spec.Chunks; index++ {
		offset := int(index) * int(spec.ChunkSize)
		_, err = writer.Append(index, body[offset:min(len(body), offset+int(spec.ChunkSize))], deadline)
		transferMust(t, err)
	}
	r := &transferRun{id: id.TransferID, cell: transferCell{tier: "b"}, body: body,
		boundary: x.Observation{ObjectVersion: id.ObjectVersion}}
	raw := startTransferRawWatch(t, s, r)
	closed := false
	defer func() {
		if !closed {
			raw.close()
		}
	}()
	// Hold the raw census while publication and ordinary retirement proceed.
	var state x.WriterState
	raw.mu.Lock()
	func() {
		defer raw.mu.Unlock()
		_, err = writer.Finalize(deadline)
		transferMust(t, err)
		store, storeErr := js.ObjectStore(t.Context(), bucket)
		transferMust(t, storeErr)
		published, infoErr := store.GetInfo(t.Context(), id.TransferID)
		transferMust(t, infoErr)
		for _, defect := range []string{"published-version", "returned-version", "published-size", "published-digest"} {
			observed := *published
			expected := *r
			switch defect {
			case "published-version":
				observed.NUID = "foreignNUID"
			case "returned-version":
				// The publication matched an earlier version, but Get must verify
				// the metadata returned by its own lookup rather than that snapshot.
				expected.boundary.ObjectVersion = "foreignNUID"
				observed.NUID = "foreignNUID"
			case "published-size":
				observed.Size++
			case "published-digest":
				observed.Digest = "foreign-digest"
			}
			if readTransferSDKObject(t.Context(), store, &transferRawWatch{s: s, r: &expected}, &observed) == nil {
				t.Fatalf("SDK Get accepted %s for the original object", defect)
			}
		}
		// sleep-fixture: the complete original remains retained for one second,
		// independent of the delayed raw census and without changing product GC.
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
		transferMust(t, writer.Stop(deadline))
		state, err = writer.Freeze(deadline)
		transferMust(t, err)
		proof := x.RetirementProof{Identity: id, EntryNonce: id.EntryNonce, WriterRetired: true, ConsumersRetired: true}
		_, retireErr := jsstream.RetireTransferObject(js, &state, proof, deadline)
		transferMust(t, retireErr)
	}()
	raw.close()
	closed = true
	// SDK EOF precedes its callback's consumer deletion. Keep the original
	// one-second retention window, then observe bounded asynchronous cleanup.
	if !testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool {
		info, err := stream.Info(t.Context())
		transferMust(t, err)
		return info.State.Consumers == 0
	}) {
		t.Fatal("independent object readers retained consumers after close")
	}
	if !raw.sdk {
		t.Fatal("original object was never independently readable through SDK Get")
	}
	transferMust(t, raw.err)
	transferMust(t, raw.sdkErr)
	for _, receipt := range state.Receipts {
		row, ok := raw.rows[spec.Stream][receipt.StreamSeq]
		if !ok || row.Sequence != receipt.StreamSeq || len(row.Data) != int(receipt.Length) {
			t.Fatalf("raw delivery lost physical receipt at index %d sequence %d", receipt.Index, receipt.StreamSeq)
		}
	}
	var chunks []transferharness.RawMessage
	for _, row := range raw.rows[spec.Stream] {
		if row.Subject == "$O."+bucket+".C."+id.ObjectVersion {
			chunks = append(chunks, row)
		}
	}
	transferMust(t, validateTransferChunks(chunks, id.ObjectVersion, body))
}
