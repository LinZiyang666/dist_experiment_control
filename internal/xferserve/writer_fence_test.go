package xferserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// An authenticated original witness may retain a larger expectation than the
// current INFO exposes. INFO cannot erase that responsibility or authorize an
// earlier tombstone ACK to purge the original pending publisher's subject.
// origin: S7 internal review rv-terminal / FIX-TERM request 3 / S7 plan §2.3
func TestTransferWriterRetirementPreservesOriginalPendingFence(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseOriginalWriterFence(t) })
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseOriginalWriterFence(t *testing.T) {
	t.Helper()
	_, setup := assemblyHost(t, t.TempDir())
	nc, err := nats.Connect(setup.Conn().ConnectedUrl(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-original"}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_xfer-original")
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("original"), 4096)
	sum := sha256.Sum256(data)
	id := x.Identity{SID: "original", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	spec := x.ObjectSpec{Identity: id, Bucket: "xfer-original", Stream: "OBJ_xfer-original", StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano),
		Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), ChunkSize: 32768, Chunks: 1}
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	d := assemblyDeadline(t)
	chunk, err := writer.Append(0, data, d)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Stop(d); err != nil {
		t.Fatal(err)
	}
	journal := &RetirementJournal{Root: filepath.Join(t.TempDir(), "retirement")}
	if err = journal.Pin(id, func() (x.ObjectSpec, error) { return spec, nil }); err != nil {
		t.Fatal(err)
	}
	row := x.LedgerRow{Identity: id, EntryNonce: id.EntryNonce, Intent: x.Intent{Verb: "push", Tier: "b"}, CanonicalTerminal: &x.CanonicalRecord{Kind: "failed"},
		Cleanup: x.CleanupState{Identity: id, WriterFence: x.RetirementFence{WriterRetired: true, ExpectedStreamSeq: chunk.StreamSeq + 10}}}
	objects := Objects{Retirement: journal, JS: func() jetstream.JetStream { return js }, Rows: func(x.Identity, x.Deadline) (x.LedgerRow, error) { return row, nil }}
	proof := x.RetirementProof{Identity: id, EntryNonce: id.EntryNonce, OwnerGeneration: 1, ReceiverRetired: true}
	if receipt, err := objects.Retire(id, proof, d); err == nil || receipt.ObjectPurged {
		t.Fatal("current INFO erased original pending expectation and authorized an earlier purge")
	}
	if _, err = stream.GetMsg(t.Context(), chunk.StreamSeq); err != nil {
		t.Fatal("unretired original chunk was purged:", err)
	}
	checkpoint, err := journal.Load(id)
	if err != nil || checkpoint.RetirementFence.ExpectedStreamSeq < row.Cleanup.WriterFence.ExpectedStreamSeq || checkpoint.RetirementFence.Purged {
		t.Fatalf("deferred fence lost durable original expectation: %+v %v", checkpoint.RetirementFence, err)
	}
}
