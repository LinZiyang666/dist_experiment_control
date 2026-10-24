//go:build linux && amd64 && infranats_integration

package infranats_test

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type storeBaseline struct {
	object        x.WriterState
	streamCreated time.Time
	consumer      *nats.ConsumerInfo
	audit         []*nats.RawStreamMsg
	sink          *os.File
	cp            x.SinkCheckpoint
	body          []byte
}

func seedCurrentStore(t *testing.T, n *stockNode, replicas int) *storeBaseline {
	t.Helper()
	// origin: S9 internal review INFRA-SUITE-SUPPLIES F8
	// Account INFO can succeed before the last configured meta peer joins.
	// Wait for the actual peer set before asking stock NATS to place R3.
	if replicas > 1 {
		waitStockPlacement(t, n.peers)
	}
	js := dataConnection(t, n.url)
	body := bytes.Repeat([]byte("current store adjacent version\n"), 7000)
	spec := objectSpec(t, js, body)
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if replicas > 1 {
		cfg := stream.CachedInfo().Config
		cfg.Replicas = replicas
		if _, err := js.UpdateStream(t.Context(), cfg); err != nil {
			t.Fatal(err)
		}
		// Update acknowledges the assignment before its new Raft group has
		// elected a leader. Read currentness before the first original write;
		// retrying that write would conceal a setup failure or a lost ACK.
		poll(t, 15*time.Second, "replicated object stream current before original writes", func() bool {
			info, err := stream.Info(t.Context())
			return err == nil && currentStreamGroup(info.Cluster, replicas)
		})
	}
	w, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	for idx := uint32(0); idx < spec.Chunks; idx++ {
		start := int(idx * spec.ChunkSize)
		if _, err := w.Append(idx, body[start:min(start+int(spec.ChunkSize), len(body))], budget(t, 3*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finalize(budget(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	state, err := w.Freeze(budget(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := js.Conn().JetStream(nats.MaxWait(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := legacy.AddConsumer(spec.Stream, &nats.ConsumerConfig{Durable: "original", AckPolicy: nats.AckExplicitPolicy, Replicas: replicas, FilterSubject: "$O.payload.C." + spec.Identity.ObjectVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.AddStream(&nats.StreamConfig{Name: "AUDIT", Subjects: []string{"audit.>"}, Storage: nats.FileStorage, Replicas: replicas}); err != nil {
		t.Fatal(err)
	}
	var audit []*nats.RawStreamMsg
	for _, kind := range []string{"START", "TERMINAL"} {
		msg := nats.NewMsg("audit.original")
		msg.Header.Set(nats.MsgIdHdr, "original-"+kind)
		msg.Data = []byte(kind + "/original-transfer/canonical-receipt")
		ack, err := legacy.PublishMsg(msg)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := legacy.GetMsg("AUDIT", ack.Sequence)
		if err != nil {
			t.Fatal(err)
		}
		audit = append(audit, raw)
	}
	sink, err := os.CreateTemp(n.root, "original-sink-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Error(err)
		}
	})
	return &storeBaseline{object: state, streamCreated: stream.CachedInfo().Created, consumer: consumer, audit: audit, sink: sink, body: body}
}

func verifyCurrentStore(t *testing.T, n *stockNode, b *storeBaseline, hop int) {
	t.Helper()
	js := dataConnection(t, n.url)
	// These read-only checks run before a writer, reader or replay is rebound.
	// They preserve the entire original multiset, not only LastSeq or final SHA.
	verifyRaw(t, js, b.object)
	stream, err := js.Stream(t.Context(), b.object.ObjectSpec.Stream)
	if err != nil || !stream.CachedInfo().Created.Equal(b.streamCreated) {
		t.Fatal("current store asset was recreated")
	}
	store, err := js.ObjectStore(t.Context(), b.object.ObjectSpec.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.GetInfo(t.Context(), b.object.ObjectSpec.Identity.TransferID)
	if err != nil || info.NUID != b.object.ObjectSpec.Identity.ObjectVersion {
		t.Fatalf("current object metadata changed across hop: %v", err)
	}
	legacy, err := js.Conn().JetStream()
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := legacy.ConsumerInfo(b.object.ObjectSpec.Stream, b.consumer.Name)
	if err != nil || !consumer.Created.Equal(b.consumer.Created) || consumer.AckFloor != b.consumer.AckFloor || consumer.Delivered != b.consumer.Delivered {
		t.Fatal("original durable consumer identity or cursor was reset")
	}
	for _, before := range b.audit {
		raw, err := legacy.GetMsg("AUDIT", before.Sequence)
		if err != nil || !reflect.DeepEqual(raw, before) {
			t.Fatal("current audit receipt was repaired or lost")
		}
	}
	for prior := 1; prior < hop; prior++ {
		raw, err := legacy.GetMsg("AUDIT", uint64(2+prior))
		if err != nil || string(raw.Data) != fmt.Sprint("current-hop-", prior) {
			t.Fatal("rollback restored an old store instead of current writes")
		}
	}
	if _, err := legacy.Publish("audit.current", []byte(fmt.Sprint("current-hop-", hop))); err != nil {
		t.Fatal(err)
	}
	// One original sink and its SHA state continue through every adjacent hop.
	r, err := jsstream.NewTransferReader(js, b.object.ObjectSpec, b.cp)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	}()
	h := sha256.New()
	if len(b.cp.SHA256State) > 0 {
		if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(b.cp.SHA256State); err != nil {
			t.Fatal(err)
		}
	}
	chunk, err := r.ReadChunk(budget(t, 3*time.Second))
	if err != nil || chunk.Receipt.Index != b.cp.ChunkIndex {
		t.Fatalf("original reader prefix did not continue: %v", err)
	}
	b.cp = syncChunk(t, r, b.sink, h, chunk)
	frozen, err := r.Freeze(budget(t, 3*time.Second))
	if err != nil || frozen.DeletePending || frozen.DirtyBarrier || frozen.SinkCheckpoint.DurableOffset != b.cp.DurableOffset {
		t.Fatal("old reader responsibility not retired")
	}
	// The deliberately retained durable consumer remains; ephemeral readers retire.
	requireConsumers(t, stream, 1)
}

func currentStreamGroup(group *jetstream.ClusterInfo, size int) bool {
	if group == nil || group.Leader == "" || len(group.Replicas) != size-1 {
		return false
	}
	for _, peer := range group.Replicas {
		if peer == nil || !peer.Current || peer.Offline || peer.Lag != 0 {
			return false
		}
	}
	return true
}
