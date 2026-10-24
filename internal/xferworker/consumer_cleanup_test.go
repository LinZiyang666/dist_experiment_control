package xferworker

import (
	"bytes"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1–2
func TestWorkerImageRetainsDetachedConsumerCleanup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("closed-attachment", func(t *testing.T) { exerciseWorkerConsumerCleanup(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "worker detached consumer cleanup", g)
	testharness.AssertNoFDLeak(t, "worker detached consumer cleanup", fd, 0)
}

func exerciseWorkerConsumerCleanup(t *testing.T) {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	connect := func() jetstream.JetStream {
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		return js
	}
	data, management := connect(), connect()
	store, err := management.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Put(t.Context(), jetstream.ObjectMeta{Name: "object", Opts: &jetstream.ObjectMetaOptions{ChunkSize: 32 << 10}}, bytes.NewReader(bytes.Repeat([]byte("a"), 64<<10))); err != nil {
		t.Fatal(err)
	}
	reader, err := jsstream.GetTransferObject(t.Context(), data, store, "cleanup", "object")
	if err != nil {
		t.Fatal(err)
	}
	duty := jsstream.ConsumerCleanupFor(data.Conn())
	t.Cleanup(func() {
		if duty != nil {
			if err := duty.Bind(management, deadline(t, 5*time.Second)); err != nil {
				t.Error(err)
				return
			}
			if err := duty.Wait(deadline(t, 5*time.Second)); err != nil {
				t.Error(err)
			}
		}
	})
	data.Conn().Close()
	_ = reader.Close()
	// The old binding cannot confirm deletion; capture its actual server duty.
	if duty == nil {
		t.Fatal("reader has no cleanup owner")
	}
	before, err := duty.Freeze(deadline(t, 5*time.Second))
	if err != nil || len(before.Deletes) != 1 {
		t.Fatalf("missing consumer duty: %+v %v", before, err)
	}
	foreign := connect()
	foreignReader, err := jsstream.GetTransferObject(t.Context(), foreign, store, "cleanup", "object")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = foreignReader.Close() })
	fixture := newWorkerFixture()
	o := newOwner(t, fixture)
	if err := RetainConsumerCleanup(o, duty); err != nil {
		t.Fatal(err)
	}
	unrelated := newOwner(t, fixture)
	if Retained(unrelated) {
		t.Fatal("foreign reader made an idle worker retained")
	}
	_, empty := exportWorker(t, unrelated)
	if len(empty.ConsumerCleanup) != 0 {
		t.Fatal("idle worker captured foreign reader")
	}
	_, state := exportWorker(t, o)
	field := reflect.ValueOf(state).FieldByName("ConsumerCleanup")
	if !field.IsValid() || field.Len() != 1 {
		t.Fatal("sealed worker snapshot lost detached consumer cleanup")
	}
	encoded, err := xferimage.EncodeWorker(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := xferimage.DecodeWorker(encoded)
	if err != nil || !reflect.DeepEqual(state, decoded) {
		t.Fatalf("cleanup codec lost evidence: %v", err)
	}
	imported, _, err := ImportWorker(fixture.cfg, decoded, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := imported.Thaw(Bindings{JS: management, Authority: fixture.cfg.Authority, OwnerGeneration: 2}, deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := duty.Wait(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	stream, err := management.Stream(t.Context(), before.Deletes[0].Stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Consumers != 1 {
		t.Fatalf("management did not delete actual consumer: %+v %v", info, err)
	}
	if Retained(imported) {
		t.Fatal("confirmed detached cleanup kept an idle worker retained before Stop")
	}
	if err := imported.Stop(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if jsstream.ConsumerCleanupFor(data.Conn()) != nil {
		t.Fatal("settled consumer still occupies old attachment")
	}
	if Retained(imported) {
		t.Fatal("settled detached cleanup kept an idle worker retained")
	}
	if err := foreignReader.Close(); err != nil {
		t.Fatal(err)
	}
	// A fresh image has no original heap cleanup handle to accidentally reuse.
	consumer, err := management.CreateConsumer(t.Context(), "OBJ_cleanup", jetstream.ConsumerConfig{Name: "DetachedImageConsumer", AckPolicy: jetstream.AckNonePolicy, FilterSubject: "$O.cleanup.C.*"})
	if err != nil {
		t.Fatal(err)
	}
	info, err = stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	image := x.WorkerState{Schema: x.StateSchema, Lineage: x.Lineage{InstanceID: "agent", BootID: "boot", Generation: 1}, Admission: x.AdmissionState{Closed: true, Generation: 1}, ConsumerCleanup: []x.ConsumerCleanupState{{OwnerID: "DetachedImageOwner", Deletes: []x.ConsumerDelete{{Stream: "OBJ_cleanup", StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Consumer: consumer.CachedInfo().Name}}}}}
	fresh, _, err := ImportWorker(fixture.cfg, image, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = RebindCurrentConsumerCleanup(fresh, management)
		_ = fresh.Stop(deadline(t, 5*time.Second))
	})
	_, imageState := exportWorker(t, fresh.(*owner))
	if !reflect.DeepEqual(image.ConsumerCleanup, imageState.ConsumerCleanup) {
		t.Fatal("fresh import lost detached duty before admission")
	}
	if err := fresh.Thaw(Bindings{JS: management, Authority: fixture.cfg.Authority, OwnerGeneration: 2}, deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Stop(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Terminal host Stop may itself create a detached duty. Its first failure
	// must hand that duty to management and retry the same local owner.
	replacement := connect()
	legacy, err := jsstream.GetTransferObject(t.Context(), replacement, store, "cleanup", "object")
	if err != nil {
		t.Fatal(err)
	}
	closing := jsstream.ConsumerCleanupFor(replacement.Conn())
	replacement.Conn().Close()
	_ = legacy.Close()
	if err := RetainConsumerCleanup(o, closing); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := StopWithConsumerCleanup(o, func() error { calls++; return closing.Wait(deadline(t, 5*time.Second)) }, management); err != nil || calls != 2 {
		t.Fatalf("terminal Stop did not retry its retained duty: calls=%d err=%v", calls, err)
	}
	if jsstream.ConsumerCleanupFor(replacement.Conn()) != nil {
		t.Fatal("terminal Stop retained a settled consumer")
	}

}
