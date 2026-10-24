package jsstream

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// A sealed pull must Sync an existing prefix while its original sender is
// still publishing. Metadata proves completion; waiting for it before opening
// the consumer prevents the receiver from ever reaching a live prefix cut.
// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §3 / T39
func TestTransferReaderSyncsPublishedPrefixBeforeMetadata(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferStreamingPrefix(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "streaming transfer prefix", g)
	testharness.AssertNoFDLeak(t, "streaming transfer prefix", fd, 0)
}

func exerciseTransferStreamingPrefix(t *testing.T) {
	t.Helper()
	if validTransferBucket(strings.Repeat("a", 257)) {
		t.Fatal("reader bucket validation diverged from the writer's token contract")
	}
	for _, completion := range []string{"exact", "foreign", "empty"} {
		t.Run(completion, func(t *testing.T) { exerciseTransferStreamingCompletion(t, completion) })
	}
}

func exerciseTransferStreamingCompletion(t *testing.T, completion string) {
	t.Helper()
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := make([]byte, 8*(32<<10))
	for i := range body {
		body[i] = byte(i*31 + i/(32<<10))
	}
	if completion == "empty" {
		body = nil
	}
	spec := writerSpec(t, js, "streaming", body, 32<<10)
	intent := sha256.Sum256([]byte("original sealed pull intent"))
	spec.Identity.IntentDigest = hex.EncodeToString(intent[:])
	w := mustWriter(t, js, spec)
	defer func() {
		if err := w.Stop(writerDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	appendChunks := func(first, end uint32) {
		t.Helper()
		for index := first; index < end; index++ {
			offset := int(index * spec.ChunkSize)
			if _, err := w.Append(index, body[offset:offset+int(spec.ChunkSize)], writerDeadline(t)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if spec.Chunks != 0 {
		appendChunks(0, 4)
	}
	r, err := NewTransferReader(js, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Stop(transferReaderDeadline(t, 3*time.Second)); err != nil {
			t.Error(err)
		}
	}()
	if completion == "empty" {
		// No final chunk exists to carry the completion gate for an empty
		// object. Its EOF must independently require final metadata too.
		if _, err := r.ReadChunk(transferReaderDeadline(t, time.Second)); err == nil || errors.Is(err, io.EOF) {
			t.Fatal("empty source supplied EOF before completion metadata")
		}
		if _, err := w.Finalize(writerDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReadChunk(transferReaderDeadline(t, time.Second)); !errors.Is(err, io.EOF) {
			t.Fatalf("empty completed object has no verified EOF: %v", err)
		}
		return
	}
	sink, err := os.CreateTemp(t.TempDir(), "original-sink")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			t.Error(err)
		}
	}()
	h := sha256.New()
	readSynced := func() x.SinkCheckpoint {
		t.Helper()
		chunk, err := r.ReadChunk(transferReaderDeadline(t, 2*time.Second))
		if err != nil {
			t.Fatalf("published prefix could not reach the original sink before metadata: %v", err)
		}
		return syncTransferReaderChunk(t, r, sink, h, chunk)
	}
	for range 4 {
		cp := readSynced()
		if cp.DurableOffset <= 0 || cp.DurableOffset >= spec.Size {
			t.Fatal("receiver did not retain a nonempty partial Sync prefix")
		}
	}
	appendChunks(4, spec.Chunks)
	for range 3 {
		readSynced()
	}
	// All chunks exist, but a final chunk cannot authorize the caller's commit
	// until the original sender publishes its exact final metadata. Keep that
	// pending chunk for retry rather than consuming it on the metadata miss.
	if chunk, err := r.ReadChunk(transferReaderDeadline(t, time.Second)); err == nil || len(chunk.Data) != 0 {
		t.Fatal("reader exposed its final chunk before exact completion metadata")
	}
	if _, err := w.Finalize(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if completion == "foreign" {
		// The prefix was fetched against a trusted source seal. A later final
		// object with the same public name still cannot replace that version.
		stream, err := js.Stream(t.Context(), spec.Stream)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec))
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(meta.Data, &object); err != nil {
			t.Fatal(err)
		}
		object["nuid"] = json.RawMessage(`"foreignVersion"`)
		data, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.PublishMsg(t.Context(), &nats.Msg{Subject: meta.Subject, Data: data}); err != nil {
			t.Fatal(err)
		}
		if chunk, err := r.ReadChunk(transferReaderDeadline(t, time.Second)); err == nil || !strings.Contains(err.Error(), x.CodeObjectVersionMismatch) || len(chunk.Data) != 0 {
			t.Fatalf("streamed prefix accepted foreign completion metadata: %v", err)
		}
		return
	}
	cp := readSynced()
	if cp.DurableOffset != spec.Size || cp.ChunkIndex != spec.Chunks {
		t.Fatal("metadata reconciliation replaced the original receiver prefix")
	}
	if _, err := r.ReadChunk(transferReaderDeadline(t, time.Second)); !errors.Is(err, io.EOF) {
		t.Fatalf("complete original object has no verified EOF: %v", err)
	}
	got, err := os.ReadFile(sink.Name())
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("streaming sink differs from the original source: %v", err)
	}
}
