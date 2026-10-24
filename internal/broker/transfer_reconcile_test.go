package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// transfer_reconcile_test.go — the orphan CHUNK sweep of the xfer bucket reaper (gotcha #85).

// origin: simcluster-speed drill 67 receipts on images #7/#8 (the `$JS.API.STREAM.PURGE` permissions
// violation every failed Put leaves on the ctl's stderr is the visible half; this is the invisible half).
//
// TestOrphanChunkGroupsArePurgedByTheBucketReap builds the three chunk-group shapes a per-session bucket
// accumulates and drives reapBucketObjects over them on a real embedded JetStream:
//   - LIVE:  a completed Put — meta + chunks — protected by the ledger set (its object is in flight);
//   - PUT:   chunks with NO meta — an abandoned Put (watchdog-cancelled, timed out, ctl died);
//   - TOMB:  a meta whose Deleted=true plus its chunks — a ctl/agent Delete whose PURGE the ACL denied.
//
// Under a non-zero floor with fresh chunks nothing is purged (an upload in flight must never be torn
// out); under the zero floor PUT and TOMB are purged, LIVE's chunks and object survive, and the object
// reaper — which lists objects — could never have seen PUT at all. Deleting the reapOrphanChunks call
// leaves PUT and TOMB in place and this test red; treating tombstoned objects as referenced keeps TOMB.
func TestOrphanChunkGroupsArePurgedByTheBucketReap(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const bucket = "xfer-lab"
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	// LIVE: a real, complete object (3 chunks at the default 128 KiB chunk size).
	live, err := store.Put(ctx, jetstream.ObjectMeta{Name: "tid-live"}, bytes.NewReader(bytes.Repeat([]byte("L"), 3*xferChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	// PUT: chunks under a NUID no meta will ever name.
	for i := 0; i < 4; i++ {
		if _, err := js.Publish(ctx, "$O."+bucket+".C.PUTNUID0000000000001", bytes.Repeat([]byte("P"), 1024)); err != nil {
			t.Fatal(err)
		}
	}
	// TOMB: chunks plus a tombstone meta (Deleted=true) naming them — the shape a denied client purge leaves.
	for i := 0; i < 2; i++ {
		if _, err := js.Publish(ctx, "$O."+bucket+".C.TOMBNUID000000000001", bytes.Repeat([]byte("T"), 1024)); err != nil {
			t.Fatal(err)
		}
	}
	tomb, _ := json.Marshal(jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "tid-gone"}, Bucket: bucket, NUID: "TOMBNUID000000000001", Deleted: true})
	tm := nats.NewMsg("$O." + bucket + ".M." + base64.URLEncoding.EncodeToString([]byte("tid-gone")))
	tm.Data = tomb
	tm.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	if _, err := js.PublishMsg(ctx, tm); err != nil {
		t.Fatal(err)
	}

	chunkGroups := func() map[string]uint64 {
		st, err := js.Stream(ctx, "OBJ_"+bucket)
		if err != nil {
			t.Fatal(err)
		}
		si, err := st.Info(ctx, jetstream.WithSubjectFilter("$O."+bucket+".C.>"))
		if err != nil {
			t.Fatal(err)
		}
		return si.State.Subjects
	}
	if groups := chunkGroups(); len(groups) != 3 {
		t.Fatalf("fixture: want 3 chunk groups (live, put, tomb), got %v", groups)
	}

	now := time.Now().UTC()
	b := &Broker{transfers: xfertrack.NewLegacyTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: func() time.Time { return now }, ClusterDataDir: t.TempDir()}
	setBrokerJS(b, js)
	t.Cleanup(func() { setBrokerJS(b, nil) })
	protected := map[string]bool{bucket + "/tid-live": true}

	// 1. A one-hour floor over chunks written seconds ago: nothing may move.
	if n := b.reapBucketObjects(ctx, "OBJ_"+bucket, time.Hour, false, protected, map[string]bool{}); n != 0 {
		t.Fatalf("fresh chunk groups were purged under a 1h floor (deleted=%d) — an upload in flight would be torn out", n)
	}
	if groups := chunkGroups(); len(groups) != 3 {
		t.Fatalf("after the floored pass: want all 3 groups intact, got %v", groups)
	}

	// 1b. The floor is SIZE-AWARE for the leader's cross-home GC: each orphan group must be judged with
	// its own byte count (chunks × xferChunkSize), so a peer-home 2 GiB upload keeps its size extra. Both
	// reapBucketObjects passes above and below run sizeAware=false, where the size argument is ignored —
	// round-2 review R4-2-F10 replaced the size with 0 and stayed green. Drive the sweep directly with a
	// recording floor that keeps everything.
	objs, err := store.List(ctx, jetstream.ListObjectsShowDeleted())
	if err != nil {
		t.Fatal(err)
	}
	sizes := map[int64]int{}
	keepAll := func(size int64) time.Duration { sizes[size]++; return time.Hour }
	if n := reapOrphanChunks(ctx, js, "OBJ_"+bucket, objs, keepAll, now, silentLogger()); n != 0 {
		t.Fatalf("a keep-everything floor still purged %d group(s)", n)
	}
	if sizes[4*xferChunkSize] != 1 || sizes[2*xferChunkSize] != 1 || len(sizes) != 2 {
		t.Fatalf("the sweep asked the floor with sizes %v, want exactly {4 chunks: 1, 2 chunks: 1} (PUT and TOMB by their own byte counts)", sizes)
	}

	// 2. Zero floor (the test broker's "reap everything now"): the two orphan groups go, LIVE stays.
	n := b.reapBucketObjects(ctx, "OBJ_"+bucket, 0, false, protected, map[string]bool{})
	if n != 2 {
		t.Fatalf("zero-floor pass reclaimed %d item(s), want exactly the 2 orphan chunk groups (PUT and TOMB)", n)
	}
	groups := chunkGroups()
	if len(groups) != 1 {
		t.Fatalf("after the zero-floor pass: want only the live object's chunk group, got %v", groups)
	}
	if _, ok := groups["$O."+bucket+".C."+live.NUID]; !ok {
		t.Fatalf("the LIVE object's chunk group was purged: %v (its meta references NUID %s)", groups, live.NUID)
	}
	if _, err := store.GetInfo(ctx, "tid-live"); err != nil {
		t.Fatalf("the live object must still be readable after the sweep: %v", err)
	}
}

// Only these tests call the chunk-only adapter; production uses the reaper owner.
// origin: S7 internal review GATES-PROD F1 / broker code ceiling
// xferChunkSize is nats.go's object-store chunk size (objDefaultChunkSize, 128 KiB); tether never sets
// ObjectMeta.Opts.ChunkSize, so a chunk group's byte size is its message count times this. It is only an
// estimate fed to the size-aware floor, never a correctness input.
const xferChunkSize = 128 << 10

// reapOrphanChunks purges the chunk groups of the OBJ_xfer bucket `name` that NO object references — the
// storage half of an abandoned tier-B Put (gotcha #85).
//
// WHY THE OBJECT REAPER CANNOT SEE THEM. nats.go's ObjectStore.Put publishes the chunks first, on ONE
// subject per object (`$O.<bucket>.C.<nuid>`), and the meta entry (`$O.<bucket>.M.<name>`) last. A Put
// that is cancelled (the #84 watchdog / bounded retry), that times out, or whose ctl dies mid-upload
// leaves the chunks with no meta; nats.go's own cleanup for that case is `STREAM.PURGE` with a subject
// filter, which the ctl's and the agent's ACLs DENY by design (file-transfer-plan Round-4 #3: bucket
// lifecycle is the broker's). That denial was harmless when a bucket was per-transfer and the broker
// deleted the whole bucket on failure. Since v0.2.2 the bucket is per-SESSION and lives for ever, the
// broker reaps per OBJECT through store.List — and a chunk group with no meta is not an object, so
// nobody has ever deleted one: every failed Put has been leaving up to its full size behind, counted
// against the bucket's MaxBytes, until the refusals turned into `insufficient storage`. A deleted object
// has the same shape: the ctl/agent Delete writes the tombstone, its PURGE is denied, the chunks stay.
//
// WHAT IS SAFE TO PURGE. A chunk group whose NUID no live object references AND whose NEWEST chunk is
// older than the caller's floor. The floor is the same one the object reaper applies (per-home grace,
// cross-home extra, the uptime term that shields a previous incarnation's live uploads) — an upload in
// flight keeps writing chunks (a 2 GiB Put at the 2 MiB/s admission floor writes one every 60 ms; one
// stalled longer than 30 s is being cancelled by the ctl's watchdog), so "newest chunk older than the
// grace" is what distinguishes abandoned from live without knowing the transfer id, which the chunk
// group does not carry. The per-bucket busy skip at the call site (any local in-flight transfer → the
// whole bucket is skipped) still applies above this function.
//
// A package-level function taking the JetStream handle, not a Broker method: the structural-budget
// ratchet pins the type's method count (see brokerUptime).
func reapOrphanChunks(ctx context.Context, js jetstream.JetStream, name string, objs []*jetstream.ObjectInfo,
	floorFor func(int64) time.Duration, now time.Time, logger interface {
		Warn(string, ...any)
		Info(string, ...any)
	}) int {
	until, _ := ctx.Deadline()
	return xferserve.ReapOrphanChunks(until, ctx.Done(), js, name, objs, floorFor, now, logger)
}
