package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// These integration counterexamples use both the real bucket coordinator and
// real writer. Keep them above the pure bucket predicates so the bucket leaf's
// tests cannot acquire a reverse import through the writer consumer.
// origin: S7 internal review FIX-ARCH F2
// origin: S7 internal review rv-jsnats F2
func TestConfigRecoverySealRejectsEvictingStream(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	bucket := "xfer-eviction"
	cfg := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
	cfg.Discard, cfg.MaxMsgs = jetstream.DiscardOld, 1
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	owner := bucketCoordinator(t, js, x.StorageLimits{}, 1, nil)
	seal, err := owner.Apply(x.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
	if err != nil {
		return // Unsafe retention must be refused before first byte.
	}
	nc, err := nats.Connect(js.Conn().ConnectedUrl(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	dataJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("x"), 64<<10)
	digest := sha256.Sum256(body)
	spec := x.ObjectSpec{Identity: x.Identity{TransferID: "payload", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "version"}, Bucket: bucket, Stream: seal.Stream, StreamCreated: seal.StreamCreated, Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:]), ChunkSize: 32 << 10, Chunks: 2}
	w, err := NewTransferWriter(dataJS, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop(bucketDeadline(t, time.Second)) })
	first, err := w.Append(0, body[:32<<10], bucketDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(1, body[32<<10:], bucketDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	stream, err := dataJS.Stream(ctx, spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	_, lost := stream.GetMsg(ctx, first.StreamSeq)
	meta, err := w.Finalize(bucketDeadline(t, time.Second))
	if err != nil {
		t.Fatalf("unsafe seal allowed acknowledged data eviction; finalization error=%v first receipt=%v", err, lost)
	}
	if errors.Is(lost, jetstream.ErrMsgNotFound) {
		t.Fatalf("recovery seal accepted DiscardOld/MaxMsgs=1; first acknowledged chunk vanished before H, yet Finalize succeeded at seq=%d", meta.MetadataSeq)
	}
}

// StreamCreated binds the prepared version's incarnation. The writer must also
// enforce the seal's retention predicate on the actual stream when first used;
// an operator change between prepare and first byte cannot reuse stale approval.
// origin: S7 internal review rv-jsnats F2
func TestConfigWriterRechecksRetentionAfterSeal(t *testing.T) {
	for _, change := range []string{"discard_old_bytes", "per_subject", "short_ttl"} {
		t.Run(change, func(t *testing.T) {
			ctx, js := bucketFixture(t, false, 0)
			bucket := "xfer-sealed-writer"
			operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			owner := bucketCoordinator(t, js, x.StorageLimits{}, 1, nil)
			seal, err := owner.Apply(x.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
			if err != nil {
				t.Fatal(err)
			}
			cfg := bucketInfo(ctx, t, js, bucket).Config
			switch change {
			case "discard_old_bytes":
				cfg.Discard = jetstream.DiscardOld
			case "per_subject":
				cfg.MaxMsgsPerSubject = 1
			case "short_ttl":
				cfg.MaxAge, cfg.Duplicates = time.Minute, time.Minute
			}
			if _, err := js.UpdateStream(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			nc, err := nats.Connect(js.Conn().ConnectedUrl(), nats.ReconnectBufSize(-1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			dataJS, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			body := bytes.Repeat([]byte("x"), 64<<10)
			digest := sha256.Sum256(body)
			spec := x.ObjectSpec{Identity: x.Identity{TransferID: "payload", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "version"}, Bucket: bucket, Stream: seal.Stream, StreamCreated: seal.StreamCreated, Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:]), ChunkSize: 32 << 10, Chunks: 2}
			w, err := NewTransferWriter(dataJS, spec)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Stop(bucketDeadline(t, time.Second)) })
			r, err := w.Append(0, body[:32<<10], bucketDeadline(t, time.Second))
			if err == nil || !strings.Contains(err.Error(), x.CodeStorageHorizon) || r != (x.ChunkReceipt{}) {
				t.Fatalf("writer used stale retention seal: receipt=%+v err=%v", r, err)
			}
			if info := bucketInfo(ctx, t, js, bucket); info.State.Msgs != 0 || info.State.LastSeq != 0 {
				t.Fatalf("unsafe retention reached first publish: %+v", info.State)
			}
		})
	}
}

// Healthy appends retain the first-byte admission seal under the exclusive
// coordinator. Recovery and metadata must still re-read actual policy, so an
// operator change cannot become a receipt or expose a completed object.
// origin: S7 internal review TRANSFER-MAKETEST F1 / S7 plan §2.1 / LQ3 / T09
func TestTransferWriterRevalidatesRetentionBeforeRecoveryAndMetadata(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseWriterRetentionBoundaries(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "writer retention boundaries", g)
	testharness.AssertNoFDLeak(t, "writer retention boundaries", fd, 0)
}

func exerciseWriterRetentionBoundaries(t *testing.T) {
	t.Helper()
	for _, boundary := range []string{"recovery", "metadata"} {
		t.Run(boundary, func(t *testing.T) {
			srv := writerServer(t, nil)
			js := writerJS(t, srv.url())
			body := bytes.Repeat([]byte("r"), 64<<10)
			spec := writerSpec(t, js, "retention-boundary", body, 32<<10)
			fault := &writerFaultJS{JetStream: js}
			w := mustWriter(t, fault, spec)
			if _, err := w.Append(0, body[:32<<10], writerDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if boundary == "recovery" {
				fault.drop = chunkSubject(spec)
			}
			_, err := w.Append(1, body[32<<10:], writerDeadline(t))
			if boundary == "recovery" && !errors.Is(err, context.DeadlineExceeded) || boundary == "metadata" && err != nil {
				t.Fatalf("boundary was not reached: %v", err)
			}
			stream, err := js.Stream(t.Context(), spec.Stream)
			if err != nil {
				t.Fatal(err)
			}
			streamConfig := stream.CachedInfo().Config
			streamConfig.Discard, streamConfig.MaxBytes = jetstream.DiscardOld, 4<<20
			if _, err = js.UpdateStream(t.Context(), streamConfig); err != nil {
				t.Fatal(err)
			}
			if boundary == "recovery" {
				_, _, err = w.ResolvePending(writerDeadline(t))
			} else {
				_, err = w.Finalize(writerDeadline(t))
			}
			if err == nil || !strings.Contains(err.Error(), x.CodeStorageHorizon) {
				t.Fatalf("unsafe retention passed the %s boundary: %v", boundary, err)
			}
			if _, err := stream.GetLastMsgForSubject(t.Context(), metadataSubject(spec)); !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("unsafe policy exposed metadata: %v", err)
			}
		})
	}
}

func bucketFixture(t *testing.T, clustered bool, duplicates time.Duration) (context.Context, jetstream.JetStream) {
	t.Helper()
	// ctx-root: test owns embedded servers and all bounded configuration operations.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	var servers []*server.Server
	count := 1
	if clustered {
		count = 3
	}
	var routePorts []net.Listener
	var routes []string
	if clustered {
		for i := 0; i < count; i++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			routePorts = append(routePorts, listener)
			routes = append(routes, "nats://"+listener.Addr().String())
		}
	}
	for i := 0; i < count; i++ {
		opts := server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true,
			ServerName: fmt.Sprintf("bucket-%d", i), StoreDir: t.TempDir(), JetStreamMaxStore: 16 << 30,
			JetStreamLimits: server.JSLimitOpts{Duplicates: duplicates}}
		if clustered {
			opts.Cluster = server.ClusterOpts{Name: "bucket-config", Host: "127.0.0.1", Port: routePorts[i].Addr().(*net.TCPAddr).Port}
			opts.Routes = server.RoutesFromStr(strings.Join(routes, ","))
			_ = routePorts[i].Close()
		}
		servers = append(servers, natstest.RunServerCallback(&opts, func(s *server.Server) {
			// Register after TempDir, before Start: shutdown must finish before
			// file-store cleanup, including a startup failure.
			t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
		}))
	}
	nc, err := nats.Connect(servers[0].ClientURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if clustered {
		if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
			for _, s := range servers {
				if s.NumRoutes() < 2 {
					return false
				}
			}
			// Startup may drop an INFO before the metadata leader is known.
			// Each probe expires independently so a later ready leader is tried.
			probeCtx, probeCancel := context.WithTimeout(ctx, 250*time.Millisecond)
			_, err := js.AccountInfo(probeCtx)
			probeCancel()
			return err == nil
		}) {
			t.Fatal("JetStream cluster did not become ready")
		}
	}
	return ctx, js
}

func bucketDeadline(t *testing.T, budget time.Duration) controlbudget.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

func bucketClock() (string, int64, error) {
	now, err := controlbudget.Now()
	return "test-boot", now, err
}

func bucketOwner(generation uint64) x.ConfigWriterProof {
	return x.ConfigWriterProof{BrokerID: fmt.Sprintf("broker-%d", generation), InstanceID: fmt.Sprintf("instance-%d", generation), BootID: "test-boot", Nonce: fmt.Sprintf("nonce-%d", generation), Generation: generation}
}

func bucketCoordinator(t *testing.T, js jetstream.JetStream, limits x.StorageLimits, generation uint64, previous *x.ConfigWriterState) xferbucket.Coordinator {
	t.Helper()
	authority, err := xferbucket.NewAuthority(bucketOwner(generation), previous)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := xferbucket.New(xferbucket.Config{Base: x.Base{Clock: bucketClock}, JS: js, Authority: authority, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func bucketInfo(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string) *jetstream.StreamInfo {
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

func operatorBucket(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string, ttl time.Duration) jetstream.StreamConfig {
	t.Helper()
	_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, MaxBytes: 512 << 20, TTL: ttl, Storage: jetstream.FileStorage, Description: "operator description", Compression: true, Metadata: map[string]string{"operator": "preserve"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := bucketInfo(ctx, t, js, bucket).Config
	cfg.MaxConsumers = 37
	cfg.MaxMsgSize = 512 << 10
	cfg.ConsumerLimits.MaxAckPending = 71
	_, err = js.UpdateStream(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return bucketInfo(ctx, t, js, bucket).Config
}
