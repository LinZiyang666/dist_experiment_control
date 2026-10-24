package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Broker-created storage must accept the real capable writer in both deployed
// replica shapes. Hand-configuring DiscardNew in a fixture hid this regression.
// origin: S7 internal review FIX-CONVERGE F1
func TestTransferBrokerCreatedBucketAdmitsCapableObject(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerCreatedBucket(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker-created transfer storage", g)
	testharness.AssertNoFDLeak(t, "broker-created transfer storage", fd, 0)
}

func exerciseBrokerCreatedBucket(t *testing.T) {
	t.Helper()
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("replicas_%d", replicas), func(t *testing.T) {
			ctx, js := convergeAdmissionJS(t, replicas)
			creation := &convergeCreationJS{JetStream: js}
			b := convergeBroker(t, creation)
			const reservation = 256 << 20
			bucket, limit, err := ensureXferBucketSizedWithLimit(ctx, b, "lab", replicas, reservation)
			if err != nil {
				t.Fatalf("real broker CREATE refused its own bucket: %v", err)
			}
			if creation.first == nil || creation.first.Discard != jetstream.DiscardNew {
				t.Fatalf("CREATE must protect ACKed chunks even before its supplementing UPDATE: %+v", creation.first)
			}
			info := convergeAdmissionInfo(ctx, t, js, bucket)
			if info.Config.Discard != jetstream.DiscardNew || info.Config.Replicas != replicas || info.Config.MaxBytes != reservation || info.Config.MaxAge != 0 || info.Config.Duplicates < 2228*time.Second || limit != reservation-int64(xferBucketChunkMargin) {
				t.Fatalf("broker-created config cannot protect ACKed chunks: config=%+v limit=%d", info.Config, limit)
			}
			body := bytes.Repeat([]byte("capable object"), 8192)
			w, spec := convergeAdmissionWriter(t, js, info, body)
			var first x.ChunkReceipt
			for index := uint32(0); index < spec.Chunks; index++ {
				start := int(index * spec.ChunkSize)
				end := min(start+int(spec.ChunkSize), len(body))
				r, err := w.Append(index, body[start:end], convergeAdmissionDeadline(t))
				if err != nil {
					t.Fatal(err)
				}
				if index == 0 {
					first = r
				}
			}
			if _, err := w.Finalize(convergeAdmissionDeadline(t)); err != nil {
				t.Fatal(err)
			}
			store, err := js.ObjectStore(ctx, bucket)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.GetBytes(ctx, spec.Identity.TransferID)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("capable transfer cannot be read through SDK: bytes=%d err=%v", len(got), err)
			}
			stream, err := js.Stream(ctx, info.Config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.GetMsg(ctx, first.StreamSeq); err != nil {
				t.Fatalf("first acknowledged chunk was evicted: %v", err)
			}
			convergeStopOwner(t, b)
		})
	}
}

// Observe the first successful backing-stream resolution, between CREATE and
// its supplementary UPDATE. A later migration cannot conceal unsafe creation.
type convergeCreationJS struct {
	jetstream.JetStream
	first *jetstream.StreamConfig
}

func (j *convergeCreationJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := j.JetStream.Stream(ctx, name)
	if err == nil && j.first == nil {
		cfg := stream.CachedInfo().Config
		j.first = &cfg
	}
	return stream, err
}

// An N-1 broker can project the config through ObjectStoreConfig and restore
// DiscardOld. The writer must stop before another byte, then continue the same
// version only after the authoritative coordinator has re-applied its policy.
// origin: S7 internal review FIX-CONVERGE F1
// origin: S7 internal review TRANSFER-MAKETEST-2 F2
func TestTransferOldConvergeFailsClosedUntilDiscardReapplied(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseOldDiscardConverge(t)
	}
	testharness.AssertNoGoroutineLeak(t, "old broker discard convergence", g)
	testharness.AssertNoFDLeak(t, "old broker discard convergence", fd, 0)
}

func exerciseOldDiscardConverge(t *testing.T) {
	t.Helper()
	t.Run("old_projection_then_reapply", func(t *testing.T) {
		ctx, js := convergeAdmissionJS(t, 1)
		b := convergeBroker(t, js)
		bucket, _, err := ensureXferBucketSizedWithLimit(ctx, b, "lab", 1, 256<<20)
		if err != nil {
			t.Fatal(err)
		}
		info := convergeAdmissionInfo(ctx, t, js, bucket)
		body := bytes.Repeat([]byte("x"), 64<<10)
		w, spec := convergeAdmissionWriter(t, js, info, body)
		first, err := w.Append(0, body[:32<<10], convergeAdmissionDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		// Model an N-1 full-config convergence that writes the zero/default
		// Discard policy. The current pinned SDK explicitly uses DiscardNew;
		// this old-writer counterexample must reset the backing stream itself.
		cfg := info.Config
		cfg.Discard = jetstream.DiscardOld
		if _, err := js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		flipped := convergeAdmissionInfo(ctx, t, js, bucket)
		if flipped.Config.Discard != jetstream.DiscardOld {
			t.Fatal("old broker convergence did not restore the server default")
		}
		if _, err := w.Append(1, body[32<<10:], convergeAdmissionDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeStorageHorizon) {
			t.Fatalf("writer continued after N-1 restored eviction: %v", err)
		}
		if current := convergeAdmissionInfo(ctx, t, js, bucket); current.State.LastSeq != flipped.State.LastSeq || current.State.Msgs != flipped.State.Msgs {
			t.Fatalf("fail-closed refusal published bytes: before=%+v after=%+v", flipped.State, current.State)
		}
		if _, _, err := ensureXferBucketSizedWithLimit(ctx, b, "lab", 1, 256<<20); err != nil {
			t.Fatalf("configuration owner did not re-apply its policy: %v", err)
		}
		if _, err := w.Append(1, body[32<<10:], convergeAdmissionDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Finalize(convergeAdmissionDeadline(t)); err != nil {
			t.Fatal(err)
		}
		store, err := js.ObjectStore(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.GetBytes(ctx, spec.Identity.TransferID)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("original object did not continue: bytes=%d err=%v", len(got), err)
		}
		stream, err := js.Stream(ctx, info.Config.Name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.GetMsg(ctx, first.StreamSeq); err != nil {
			t.Fatalf("reapply lost the original receipt: %v", err)
		}
		convergeStopOwner(t, b)
	})
}

// Capacity refusal must precede legacy admission, preserve the original ACKed
// chunk, and map to retryable exit 75 rather than a permanent size error.
// origin: S7 internal review FIX-CONVERGE F1
func TestTransferLegacyFullBucketRefusesWithoutEviction(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyFullBucket(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy full transfer bucket", g)
	testharness.AssertNoFDLeak(t, "legacy full transfer bucket", fd, 0)
}

func exerciseLegacyFullBucket(t *testing.T) {
	t.Helper()
	t.Run("acknowledged_inflight_chunk", func(t *testing.T) {
		ctx, js := convergeAdmissionJS(t, 1)
		const bucket = "xfer-lab"
		// An existing small operator reservation is deliberate. The owner must
		// migrate its eviction policy without changing that reservation.
		if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, MaxBytes: 128 << 10, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
		b := convergeBroker(t, js)
		if _, err := b.transferRuntime.Load().Buckets.Apply(x.ConfigPatch{Bucket: bucket}, convergeAdmissionDeadline(t)); err != nil {
			t.Fatal(err)
		}
		info := convergeAdmissionInfo(ctx, t, js, bucket)
		body := bytes.Repeat([]byte("L"), 64<<10)
		w, _ := convergeAdmissionWriter(t, js, info, body)
		first, err := w.Append(0, body[:32<<10], convergeAdmissionDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		// Fill using a distinct unfinished legacy NUID. Bound the fixture so
		// DiscardOld cannot turn its fill loop into a hang by evicting forever.
		full := false
		for i := 0; i < 16; i++ {
			_, err = js.Publish(ctx, "$O."+bucket+".C.LEGACY", bytes.Repeat([]byte("F"), 16<<10))
			if err != nil {
				var apiErr *jetstream.APIError
				if !errors.As(err, &apiErr) || !strings.Contains(apiErr.Description, "maximum bytes") {
					t.Fatalf("fixture did not reach stream capacity: %v", err)
				}
				full = true
				break
			}
		}
		if !full {
			stream, err := js.Stream(ctx, info.Config.Name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.GetMsg(ctx, first.StreamSeq); errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("legacy eviction lost acknowledged in-flight chunk at sequence %d", first.StreamSeq)
			}
			t.Fatal("full bucket kept accepting chunks by evicting its oldest receipts")
		}
		before := convergeAdmissionInfo(ctx, t, js, bucket)
		_, tooLarge, perr := b.provisionXferBucket(ctx, "lab", 32<<10)
		if tooLarge != nil || perr == nil {
			t.Fatalf("full bucket was admitted or misreported as a permanent size error: tooLarge=%+v perr=%v", tooLarge, perr)
		}
		code, detail := xferProvisionRefusal(perr)
		if code != x.CodeCapacityExceeded || detail == "" || xferclient.ExitCode(&xferclient.Error{Code: code}) != 75 {
			t.Fatalf("legacy capacity refusal must be retryable: code=%s detail=%s exit=%d", code, detail, xferclient.ExitCode(&xferclient.Error{Code: code}))
		}
		if after := convergeAdmissionInfo(ctx, t, js, bucket); after.State.Msgs != before.State.Msgs || after.State.LastSeq != before.State.LastSeq {
			t.Fatalf("refused legacy admission changed stored objects: before=%+v after=%+v", before.State, after.State)
		}
		// Exercise SDK Put too: it must fail at the server rather than evict
		// another object's in-flight chunk. This is fixture IO, not resume.
		store, err := js.ObjectStore(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutBytes(ctx, "late-legacy", body); err == nil {
			t.Fatal("legacy Put evicted the in-flight object instead of refusing capacity")
		}
		stream, err := js.Stream(ctx, info.Config.Name)
		if err != nil {
			t.Fatal(err)
		}
		message, err := stream.GetMsg(ctx, first.StreamSeq)
		if err != nil || !bytes.Equal(message.Data, body[:32<<10]) {
			t.Fatalf("legacy capacity refusal lost ACKed chunk: err=%v", err)
		}
		// SDK failure can append its own deleted metadata as cleanup. That
		// tombstone never licenses eviction of the original object's receipts.
		convergeStopOwner(t, b)
	})
}

func convergeAdmissionDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// Moving sizing out of broker must preserve the original sealed-ceiling
// fallback: a non-positive readback must never become unlimited admission.
// origin: S7 internal review FIX-CONVERGE F1
func TestTransferLegacySealedCapacityKeepsFiniteCeiling(t *testing.T) {
	seal := x.BucketSeal{Bucket: "xfer-lab", MaxBytes: 256 << 20}
	for _, actual := range []int64{-1, 0, 128 << 20, 512 << 20} {
		info := &jetstream.StreamInfo{Config: jetstream.StreamConfig{MaxBytes: actual}, State: jetstream.StreamState{Bytes: 16 << 20}}
		bucket, got, err := xferserve.LegacySealedBucketCapacity(seal, info)
		want := int64(176 << 20)
		if actual == 128<<20 {
			want = 48 << 20
		}
		if err != nil || bucket != seal.Bucket || got != want {
			t.Fatalf("readback MaxBytes=%d changed finite payload ceiling: bucket=%s got=%d want=%d err=%v", actual, bucket, got, want, err)
		}
	}
}

func convergeStopOwner(t *testing.T, b *Broker) {
	t.Helper()
	if err := b.transferRuntime.Load().Buckets.Stop(convergeAdmissionDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func convergeAdmissionInfo(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string) *jetstream.StreamInfo {
	t.Helper()
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func convergeAdmissionWriter(t *testing.T, js jetstream.JetStream, info *jetstream.StreamInfo, body []byte) (jsstream.Writer, x.ObjectSpec) {
	t.Helper()
	digest := sha256.Sum256(body)
	spec := x.ObjectSpec{Identity: x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "object", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "fixedversion"}, Bucket: "xfer-lab", Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:]), ChunkSize: 32 << 10, Chunks: uint32((len(body) + (32 << 10) - 1) / (32 << 10))}
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Stop(convergeAdmissionDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	return writer, spec
}

// Embedded file stores model the single and N3 reservation/replica shapes;
// there are no subprocesses or installed binaries in this fixture.
func convergeAdmissionJS(t *testing.T, replicas int) (context.Context, jetstream.JetStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	var listeners []net.Listener
	var routes []string
	if replicas == 3 {
		for i := 0; i < replicas; i++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			listeners = append(listeners, listener)
			routes = append(routes, "nats://"+listener.Addr().String())
		}
	}
	var servers []*server.Server
	for i := 0; i < replicas; i++ {
		opts := server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, ServerName: fmt.Sprintf("converge-%d", i), StoreDir: t.TempDir(), JetStreamMaxStore: 16 << 30}
		if replicas == 3 {
			opts.Cluster = server.ClusterOpts{Name: "converge-storage", Host: "127.0.0.1", Port: listeners[i].Addr().(*net.TCPAddr).Port}
			opts.Routes = server.RoutesFromStr(strings.Join(routes, ","))
			_ = listeners[i].Close()
		}
		servers = append(servers, natstest.RunServerCallback(&opts, func(s *server.Server) {
			t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
		}))
	}
	nc, err := nats.Connect(servers[0].ClientURL(), nats.Timeout(2*time.Second), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if replicas == 3 && !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		leaders := 0
		for _, s := range servers {
			if s.NumRoutes() < 2 || !s.JetStreamIsClustered() || !s.JetStreamIsCurrent() {
				return false
			}
			// A routed socket and one AccountInfo response can precede the
			// third peer's storage advertisement. R=3 requires all three.
			if s.JetStreamIsLeader() {
				leaders++
				if len(s.JetStreamClusterPeers()) != replicas {
					return false
				}
			}
		}
		if leaders != 1 {
			return false
		}
		probe, probeCancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer probeCancel()
		_, err := js.AccountInfo(probe)
		return err == nil
	}) {
		t.Fatal("JetStream N3 metadata group did not become ready")
	}
	return ctx, js
}
