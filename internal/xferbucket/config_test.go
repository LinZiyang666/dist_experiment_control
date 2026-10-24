package xferbucket

// Real embedded file-store JetStream serves all configuration and readback IO.
// Interceptors delay real API calls or their replies; they never fabricate success.
// Cross-broker forwarding and real Raft leadership changes belong to P23.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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

func bucketOwner(generation uint64) xfercontract.ConfigWriterProof {
	return xfercontract.ConfigWriterProof{BrokerID: fmt.Sprintf("broker-%d", generation), InstanceID: fmt.Sprintf("instance-%d", generation), BootID: "test-boot", Nonce: fmt.Sprintf("nonce-%d", generation), Generation: generation}
}

func bucketCoordinator(t *testing.T, js jetstream.JetStream, limits xfercontract.StorageLimits, generation uint64, previous *xfercontract.ConfigWriterState) Coordinator {
	t.Helper()
	authority, err := NewAuthority(bucketOwner(generation), previous)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := New(Config{Base: xfercontract.Base{Clock: bucketClock}, JS: js, Authority: authority, Limits: limits})
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

func assertBucketConfig(ctx context.Context, t *testing.T, js jetstream.JetStream, seal xfercontract.BucketSeal, expected jetstream.StreamConfig) {
	t.Helper()
	info := bucketInfo(ctx, t, js, seal.Bucket)
	want, err := configDigest(expected)
	if err != nil {
		t.Fatal(err)
	}
	got, err := configDigest(info.Config)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || seal.ConfigDigest != got || seal.MaxAge != expected.MaxAge || seal.Duplicates != expected.Duplicates || seal.MaxBytes != expected.MaxBytes || seal.Replicas != expected.Replicas {
		t.Fatalf("full config not preserved: want=%+v actual=%+v seal=%+v", expected, info.Config, seal)
	}
	if info.State.Msgs != 0 || info.State.Bytes != 0 {
		t.Fatalf("configuration emitted first-byte side effects: %+v", info.State)
	}
	if seal.StreamCreated != info.Created.UTC().Format(time.RFC3339Nano) || seal.MaxMsgSize != int64(expected.MaxMsgSize) || seal.MaxPayload != js.Conn().MaxPayload() {
		t.Fatalf("seal does not bind actual incarnation/payload limits: %+v", seal)
	}
}

type interceptedJS struct {
	jetstream.JetStream
	updates atomic.Int32
	creates atomic.Int32
	lookups atomic.Int32
	update  func(context.Context, jetstream.StreamConfig) (jetstream.Stream, error)
	lookup  func(context.Context, string) (jetstream.Stream, error)
	create  func(context.Context, jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error)
}

func (j *interceptedJS) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	j.updates.Add(1)
	if j.update != nil {
		return j.update(ctx, cfg)
	}
	return j.JetStream.UpdateStream(ctx, cfg)
}

func (j *interceptedJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	j.lookups.Add(1)
	if j.lookup != nil {
		return j.lookup(ctx, name)
	}
	return j.JetStream.Stream(ctx, name)
}

func (j *interceptedJS) CreateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	j.creates.Add(1)
	if j.create != nil {
		return j.create(ctx, cfg)
	}
	return j.JetStream.CreateObjectStore(ctx, cfg)
}

// origin: hot-update S7 plan §2.1 / LQ3 / T05 / M14
func TestTransferBucketConfigPreservesOperatorLimitsAcrossOwners(t *testing.T) {
	if objectRecoveryHorizon != 2228*time.Second {
		t.Fatalf("recovery horizon no longer covers the frozen byte budget plus slack: %s", objectRecoveryHorizon)
	}
	ctx, js := bucketFixture(t, true, 0)
	t.Run("ensure_raise_shrink_intent_and_handoff", func(t *testing.T) {
		bucket := "xfer-operator"
		expected := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		first := bucketCoordinator(t, js, xfercontract.StorageLimits{}, 1, nil)
		patch := xfercontract.ConfigPatch{Bucket: bucket, MaxBytes: 256 << 20}
		seal, err := first.Apply(patch, bucketDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		expected.Duplicates = objectRecoveryHorizon
		assertBucketConfig(ctx, t, js, seal, expected)
		// A subsequent patch must see the latest operator config rather than
		// a full snapshot retained by the preceding ensure operation.
		expected.Description = "operator changed between patches"
		expected.Metadata["operator"] = "latest value"
		if _, err := js.UpdateStream(ctx, expected); err != nil {
			t.Fatal(err)
		}
		patch.Replicas = 3
		seal, err = first.Apply(patch, bucketDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		expected.Replicas = 3
		assertBucketConfig(ctx, t, js, seal, expected)
		if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
			probeCtx, probeCancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer probeCancel()
			stream, err := js.Stream(probeCtx, "OBJ_"+bucket)
			if err != nil {
				return false
			}
			info := stream.CachedInfo()
			if info.Cluster == nil || info.Cluster.Leader == "" {
				return false
			}
			actual := 1
			for _, replica := range info.Cluster.Replicas {
				if replica != nil && replica.Current && !replica.Offline {
					actual++
				}
			}
			return actual == 3
		}) {
			t.Fatal("raised replica group did not finish catchup")
		}
		state, err := first.Freeze(bucketDeadline(t, 5*time.Second))
		if err != nil || len(state.Proofs) != 1 || !state.Proofs[0].GateClosed || !state.Proofs[0].RequestsJoined || !state.Admission.Closed {
			t.Fatalf("retirement lacks joined gate proof: state=%+v err=%v", state, err)
		}
		if _, err := first.Apply(patch, bucketDeadline(t, time.Second)); err == nil {
			t.Fatal("retired writer admitted an update")
		}
		second := bucketCoordinator(t, js, xfercontract.StorageLimits{}, 2, &state)
		patch.Replicas, patch.Duplicates = 1, objectRecoveryHorizon+time.Minute
		seal, err = second.Apply(patch, bucketDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		expected.Duplicates = patch.Duplicates
		assertBucketConfig(ctx, t, js, seal, expected)
		patch.Duplicates = objectRecoveryHorizon
		seal, err = second.Apply(patch, bucketDeadline(t, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		assertBucketConfig(ctx, t, js, seal, expected)
		if seal.Proof.BrokerID != "broker-2" || seal.Proof.Generation != 2 {
			t.Fatalf("successor seal names old owner: %+v", seal.Proof)
		}
		if err := second.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("creation_is_confirmed_after_duplicate_window_update", func(t *testing.T) {
		wrapped := &interceptedJS{JetStream: js}
		owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-created", MaxBytes: 256 << 20}, bucketDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if wrapped.creates.Load() != 1 || wrapped.updates.Load() != 1 || seal.MaxBytes <= 0 || seal.Duplicates < objectRecoveryHorizon {
			t.Fatalf("creation did not supplement/confirm full config: %+v creates=%d updates=%d", seal, wrapped.creates.Load(), wrapped.updates.Load())
		}
		if info := bucketInfo(ctx, t, js, seal.Bucket); info.State.Msgs != 0 || info.Config.Storage != jetstream.FileStorage {
			t.Fatalf("creation emitted bytes or changed storage: %+v", info)
		}
	})
	t.Run("short_TTL_and_duplicate_ceiling_have_zero_mutations", func(t *testing.T) {
		bucket := "xfer-short"
		expected := operatorBucket(ctx, t, js, bucket, 30*time.Minute)
		wrapped := &interceptedJS{JetStream: js}
		owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket, Replicas: 3}, bucketDeadline(t, 5*time.Second))
		assertHorizonRefusal(t, seal, err)
		before, _ := configDigest(expected)
		after, _ := configDigest(bucketInfo(ctx, t, js, bucket).Config)
		if before != after || wrapped.updates.Load() != 0 || wrapped.creates.Load() != 0 || bucketInfo(ctx, t, js, bucket).State.Msgs != 0 {
			t.Fatal("short TTL refusal changed config or sent first chunk")
		}
		limited := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{MaxDuplicates: 120 * time.Second}, 1, nil)
		seal, err = limited.Apply(xfercontract.ConfigPatch{Bucket: "xfer-no-create"}, bucketDeadline(t, time.Second))
		assertHorizonRefusal(t, seal, err)
		if wrapped.updates.Load() != 0 || wrapped.creates.Load() != 0 || wrapped.lookups.Load() != 1 {
			t.Fatal("known ceiling refusal performed storage IO")
		}
		if _, err := js.Stream(ctx, "OBJ_xfer-no-create"); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("ceiling refusal created stream: %v", err)
		}
	})
	t.Run("server_actual_duplicate_ceiling_is_a_horizon_refusal", func(t *testing.T) {
		limitedCtx, limitedJS := bucketFixture(t, false, 120*time.Second)
		bucket := "xfer-server-cap"
		expected := operatorBucket(limitedCtx, t, limitedJS, bucket, 48*time.Hour)
		owner := bucketCoordinator(t, limitedJS, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket}, bucketDeadline(t, 5*time.Second))
		assertHorizonRefusal(t, seal, err)
		before, _ := configDigest(expected)
		info := bucketInfo(limitedCtx, t, limitedJS, bucket)
		after, _ := configDigest(info.Config)
		if before != after || info.State.Msgs != 0 {
			t.Fatal("server ceiling refusal changed stream or sent chunk")
		}
		if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatalf("definite server refusal was treated as sent-unknown: %v", err)
		}
	})
	t.Run("finite_disk_server_account_limits_refuse_before_create", func(t *testing.T) {
		for name, limits := range map[string]xfercontract.StorageLimits{
			"disk": {DiskBytes: 64 << 20}, "server": {ServerMaxBytes: 64 << 20}, "account": {AccountMaxBytes: 64 << 20},
		} {
			wrapped := &interceptedJS{JetStream: js}
			owner := bucketCoordinator(t, wrapped, limits, 1, nil)
			seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-small-" + name}, bucketDeadline(t, time.Second))
			if err == nil || seal != (xfercontract.BucketSeal{}) || !strings.Contains(err.Error(), xfercontract.CodeCapacityExceeded) || wrapped.creates.Load() != 0 {
				t.Fatalf("small store admitted creation: seal=%+v err=%v creates=%d", seal, err, wrapped.creates.Load())
			}
		}
	})
	t.Run("uncertain_lookup_never_creates", func(t *testing.T) {
		wrapped := &interceptedJS{JetStream: js, lookup: func(context.Context, string) (jetstream.Stream, error) { return nil, context.DeadlineExceeded }}
		owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-lookup"}, bucketDeadline(t, time.Second))
		if err == nil || seal != (xfercontract.BucketSeal{}) || wrapped.creates.Load() != 0 {
			t.Fatalf("uncertain INFO created a bucket: seal=%+v err=%v", seal, err)
		}
	})
	t.Run("unlimited_existing_reservation_cannot_authorize_bytes", func(t *testing.T) {
		bucket := "xfer-unlimited"
		if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
		wrapped := &interceptedJS{JetStream: js}
		owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
		if err == nil || seal != (xfercontract.BucketSeal{}) || !strings.Contains(err.Error(), xfercontract.CodeCapacityExceeded) || wrapped.updates.Load() != 0 {
			t.Fatalf("unlimited reservation produced a seal: seal=%+v err=%v", seal, err)
		}
	})
	t.Run("operator_change_after_update_rejects_seal", func(t *testing.T) {
		bucket := "xfer-conflict"
		operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		wrapped := &interceptedJS{JetStream: js}
		wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
			stream, err := js.UpdateStream(callCtx, cfg)
			if err != nil {
				return nil, err
			}
			cfg.Description = "operator changed during transaction"
			_, err = js.UpdateStream(callCtx, cfg)
			return stream, err
		}
		owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
		if err == nil || seal != (xfercontract.BucketSeal{}) || !strings.Contains(err.Error(), xfercontract.CodeObjectConflict) {
			t.Fatalf("conflicting readback authorized first byte: seal=%+v err=%v", seal, err)
		}
		if info := bucketInfo(ctx, t, js, bucket); info.Config.Description != "operator changed during transaction" || info.State.Msgs != 0 {
			t.Fatalf("conflict clobbered operator config or published: %+v", info)
		}
	})
	t.Run("concurrent_patches_read_current_snapshot", func(t *testing.T) {
		bucket := "xfer-concurrent"
		expected := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		wrapped := &interceptedJS{JetStream: js}
		var active, maximum atomic.Int32
		wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
			n := active.Add(1)
			for old := maximum.Load(); n > old; old = maximum.Load() {
				if maximum.CompareAndSwap(old, n) {
					break
				}
			}
			defer active.Add(-1)
			return js.UpdateStream(callCtx, cfg)
		}
		owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
		var wg sync.WaitGroup
		results := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				deadline, err := controlbudget.After(5 * time.Second)
				if err == nil {
					_, err = owner.Apply(xfercontract.ConfigPatch{Bucket: bucket, Duplicates: objectRecoveryHorizon + time.Duration(i)*time.Second}, deadline)
				}
				results <- err
			}(i)
		}
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Fatal(err)
			}
		}
		expected.Duplicates = objectRecoveryHorizon + 7*time.Second
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket, Duplicates: objectRecoveryHorizon}, bucketDeadline(t, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		assertBucketConfig(ctx, t, js, seal, expected)
		if maximum.Load() != 1 {
			t.Fatalf("bucket had concurrent full-config writers: %d", maximum.Load())
		}
	})
}

func assertHorizonRefusal(t *testing.T, seal xfercontract.BucketSeal, err error) {
	t.Helper()
	if err == nil || seal != (xfercontract.BucketSeal{}) {
		t.Fatalf("unsafe storage authorized first byte: seal=%+v err=%v", seal, err)
	}
	for _, required := range []string{xfercontract.CodeStorageHorizon, "Duplicates", "MaxAge", objectRecoveryHorizon.String()} {
		if !strings.Contains(err.Error(), required) {
			t.Fatalf("horizon refusal omits required operator guidance %q: %v", required, err)
		}
	}
}

// bucketCensus is the owner's own census witness; it records every bucket it was asked about.
type bucketCensus struct {
	live  int
	err   error
	asked []string
}

func (c *bucketCensus) Responsible(bucket string, deadline controlbudget.Deadline) (int, error) {
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return 0, err
	}
	c.asked = append(c.asked, bucket)
	return c.live, c.err
}

func shrinkOwner(t *testing.T, js jetstream.JetStream, census Census) (Coordinator, *interceptedJS) {
	t.Helper()
	authority, err := NewAuthority(bucketOwner(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &interceptedJS{JetStream: js}
	owner, err := NewWithCensus(Config{Base: xfercontract.Base{Clock: bucketClock}, JS: wrapped, Authority: authority}, census)
	if err != nil {
		t.Fatal(err)
	}
	return owner, wrapped
}

// applyOnlyPort hides Shrink: an owner that cannot prove idleness.
type applyOnlyPort struct {
	Coordinator
	applies atomic.Int32
}

func (p *applyOnlyPort) Apply(patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	p.applies.Add(1)
	return p.Coordinator.Apply(patch, deadline)
}

type recordingShrinker struct {
	applyOnlyPort
	shrinks atomic.Int32
}

func (p *recordingShrinker) Shrink(patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	p.shrinks.Add(1)
	return p.Coordinator.(Shrinker).Shrink(patch, deadline)
}

func putBucketBytes(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string, size int) {
	t.Helper()
	store, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutBytes(ctx, "stored", make([]byte, size)); err != nil {
		t.Fatal(err)
	}
}

// A shrink is the only MaxBytes change the owner makes, and only under an authoritative proof taken
// under its serializer: a fresh INFO whose stored bytes fit AND its own empty live-transfer census.
// Every refusal is the typed reservation refusal and leaves the operator's reservation untouched.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func TestTransferBucketShrinkRequiresAuthoritativeIdleProof(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	t.Run("idle_census_shrinks_the_full_config", func(t *testing.T) {
		bucket := "xfer-shrink-idle"
		expected := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		census := &bucketCensus{}
		owner, _ := shrinkOwner(t, js, census)
		seal, err := owner.(Shrinker).Shrink(xfercontract.ConfigPatch{Bucket: bucket, MaxBytes: 300 << 20}, bucketDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		expected.MaxBytes, expected.Duplicates = 300<<20, objectRecoveryHorizon
		assertBucketConfig(ctx, t, js, seal, expected)
		if len(census.asked) != 1 || census.asked[0] != bucket {
			t.Fatalf("shrink did not consult the owner's census for its bucket: %v", census.asked)
		}
	})
	t.Run("stored_bytes_within_the_new_reservation_shrink", func(t *testing.T) {
		bucket := "xfer-shrink-stored"
		operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		putBucketBytes(ctx, t, js, bucket, 1<<20)
		owner, _ := shrinkOwner(t, js, &bucketCensus{})
		seal, err := owner.(Shrinker).Shrink(xfercontract.ConfigPatch{Bucket: bucket, MaxBytes: 300 << 20}, bucketDeadline(t, 5*time.Second))
		if err != nil || seal.MaxBytes != 300<<20 || bucketInfo(ctx, t, js, bucket).Config.MaxBytes != 300<<20 {
			t.Fatalf("idle bucket whose bytes fit was not shrunk: seal=%+v err=%v", seal, err)
		}
	})
	t.Run("refusals_leave_the_reservation_unchanged", func(t *testing.T) {
		unavailable := errors.New("census owner unreachable")
		for _, tc := range []struct {
			name   string
			census *bucketCensus
			stored int
			target int64
			asked  int
			cause  error
		}{
			{name: "live-responsibility", census: &bucketCensus{live: 1}, target: 300 << 20, asked: 1},
			{name: "census-unavailable", census: &bucketCensus{err: unavailable}, target: 300 << 20, asked: 1, cause: unavailable},
			{name: "no-census", target: 300 << 20},
			{name: "stored-bytes-exceed", census: &bucketCensus{}, stored: 4 << 20, target: 2 << 20},
			{name: "growth", census: &bucketCensus{}, target: 1 << 30},
		} {
			bucket := "xfer-refuse-" + tc.name
			operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			if tc.stored > 0 {
				putBucketBytes(ctx, t, js, bucket, tc.stored)
			}
			before, _ := configDigest(bucketInfo(ctx, t, js, bucket).Config)
			var census Census
			if tc.census != nil {
				census = tc.census
			}
			owner, wrapped := shrinkOwner(t, js, census)
			seal, err := owner.(Shrinker).Shrink(xfercontract.ConfigPatch{Bucket: bucket, MaxBytes: tc.target}, bucketDeadline(t, 5*time.Second))
			if err == nil || !strings.Contains(err.Error(), xfercontract.CodeObjectConflict) || seal != (xfercontract.BucketSeal{}) || wrapped.updates.Load() != 0 {
				t.Fatalf("%s: unproved shrink was applied: seal=%+v err=%v updates=%d", tc.name, seal, err, wrapped.updates.Load())
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("%s: census failure lost its cause: %v", tc.name, err)
			}
			if tc.census != nil && len(tc.census.asked) != tc.asked {
				t.Fatalf("%s: census consulted %d times, want %d", tc.name, len(tc.census.asked), tc.asked)
			}
			if after, _ := configDigest(bucketInfo(ctx, t, js, bucket).Config); after != before {
				t.Fatalf("%s: refused shrink changed the configuration", tc.name)
			}
		}
		owner, wrapped := shrinkOwner(t, js, &bucketCensus{})
		seal, err := owner.(Shrinker).Shrink(xfercontract.ConfigPatch{Bucket: "xfer-refuse-missing", MaxBytes: 300 << 20}, bucketDeadline(t, time.Second))
		if err == nil || seal != (xfercontract.BucketSeal{}) || wrapped.creates.Load() != 0 {
			t.Fatalf("shrink of a missing bucket created or sealed one: seal=%+v err=%v", seal, err)
		}
		if _, err := owner.(Shrinker).Shrink(xfercontract.ConfigPatch{Bucket: "xfer-refuse-missing"}, bucketDeadline(t, time.Second)); err == nil {
			t.Fatal("an unlimited reservation was accepted as a shrink target")
		}
	})
	t.Run("ensure_target_never_shrinks_even_when_idle", func(t *testing.T) {
		bucket := "xfer-ensure-idle"
		operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		census := &bucketCensus{}
		owner, _ := shrinkOwner(t, js, census)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket, MaxBytes: 256 << 20}, bucketDeadline(t, 5*time.Second))
		if err != nil || seal.MaxBytes != 512<<20 || len(census.asked) != 0 {
			t.Fatalf("an ensure normalized the operator reservation: seal=%+v err=%v asked=%v", seal, err, census.asked)
		}
	})
	t.Run("legacy_adapter_routes_reductions_to_the_proof", func(t *testing.T) {
		bucket := "xfer-adapter-shrink"
		operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		owner, _ := shrinkOwner(t, js, &bucketCensus{})
		recording := &recordingShrinker{applyOnlyPort: applyOnlyPort{Coordinator: owner}}
		current := bucketInfo(ctx, t, js, bucket).Config
		grown := current
		grown.MaxBytes = 1 << 30
		if _, err := Bind(js, recording).UpdateStream(ctx, grown); err == nil || recording.shrinks.Load() != 0 || recording.applies.Load() != 0 {
			t.Fatalf("legacy growth reached the owner: err=%v shrinks=%d applies=%d", err, recording.shrinks.Load(), recording.applies.Load())
		}
		applyOnly := &applyOnlyPort{Coordinator: owner}
		reduced := current
		reduced.MaxBytes = 300 << 20
		if _, err := Bind(js, applyOnly).UpdateStream(ctx, reduced); err == nil || !strings.Contains(err.Error(), xfercontract.CodeObjectConflict) || applyOnly.applies.Load() != 0 {
			t.Fatalf("an owner without Shrink received a reduction: err=%v applies=%d", err, applyOnly.applies.Load())
		}
		if _, err := Bind(js, recording).UpdateStream(ctx, reduced); err != nil || recording.shrinks.Load() != 1 {
			t.Fatalf("legacy stream reduction did not use the proof-gated shrink: err=%v shrinks=%d", err, recording.shrinks.Load())
		}
		if _, err := Bind(js, recording).UpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, MaxBytes: 280 << 20}); err != nil || recording.shrinks.Load() != 2 {
			t.Fatalf("legacy object-store reduction did not use the proof-gated shrink: err=%v shrinks=%d", err, recording.shrinks.Load())
		}
		info := bucketInfo(ctx, t, js, bucket)
		if info.Config.MaxBytes != 280<<20 || info.Config.Description != current.Description || info.Config.MaxConsumers != current.MaxConsumers {
			t.Fatalf("legacy reduction lost operator configuration: %+v", info.Config)
		}
	})
}
