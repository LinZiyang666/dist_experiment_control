package xfergc

// Real embedded file-store JetStream exercises object listing and raw storage.
// Owner RPCs and scheduling use explicit boundary spies, which never purge.
// The separate T29 retirement case requires the P02/P11-P14 implementations.

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type gcFixture struct {
	t      *testing.T
	server *server.Server
	nc     *nats.Conn
	js     jetstream.JetStream
	ctx    context.Context
	cancel context.CancelFunc
}

func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		s.WaitForShutdown()
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		s.Shutdown()
		s.WaitForShutdown()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	return &gcFixture{t: t, server: s, nc: nc, js: js, ctx: ctx, cancel: cancel}
}

func (f *gcFixture) close() {
	f.cancel()
	f.nc.Close()
	f.server.Shutdown()
	f.server.WaitForShutdown()
}

func (f *gcFixture) store(sid string, ttl time.Duration) jetstream.ObjectStore {
	f.t.Helper()
	store, err := f.js.CreateObjectStore(f.ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-" + sid, Storage: jetstream.FileStorage, TTL: ttl})
	if err != nil {
		f.t.Fatal(err)
	}
	return store
}

func gcDeadline(t *testing.T, budget time.Duration) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func gcBase(utc func() time.Time) x.Base {
	return x.Base{Clock: func() (string, int64, error) {
		now, err := controlbudget.Now()
		return "gc-test-boot", now, err
	}, UTC: utc}
}

type censusOwners struct {
	mu             sync.Mutex
	local          x.LedgerCensus
	remote         map[x.Origin]x.LedgerCensus
	fail           map[x.Origin]error
	calls          []x.Origin
	failAfterLocal int
}

func (o *censusOwners) Census(origin x.Origin, deadline x.Deadline) (x.LedgerCensus, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return x.LedgerCensus{}, err
	}
	o.calls = append(o.calls, origin)
	if origin == (x.Origin{}) {
		if o.failAfterLocal > 0 {
			o.failAfterLocal--
			if o.failAfterLocal == 0 {
				return x.LedgerCensus{}, errors.New("census lost after retirement fence")
			}
		}
		return o.local, o.fail[origin]
	}
	return o.remote[origin], o.fail[origin]
}

type schedulingFinalizer struct {
	mu    sync.Mutex
	calls []x.Identity
	err   error
}

func (f *schedulingFinalizer) Advance(id x.Identity, deadline x.Deadline) (x.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return x.Status{}, err
	}
	f.calls = append(f.calls, id)
	// A scheduling spy proves no retirement. Pending is a real responsibility,
	// not a fabricated successful cleanup/history receipt.
	return x.Status{Identity: id, State: x.StatusLive, Phase: x.PhaseCleanupPending}, f.err
}

func (f *schedulingFinalizer) RetireLegacyOrphan(candidate x.LegacyOrphanCandidate, deadline x.Deadline) (x.LegacyOrphanReceipt, error) {
	_, err := f.Advance(x.Identity{SID: "owners", TransferID: candidate.Name, ObjectVersion: candidate.NUID}, deadline)
	return x.LegacyOrphanReceipt{}, err // A scheduling spy supplies no retirement receipt.
}

func (f *schedulingFinalizer) Stop(x.Deadline) error {
	return errors.New("test scheduling boundary does not own collector lifetime")
}

func (f *schedulingFinalizer) identities() []x.Identity {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]x.Identity(nil), f.calls...)
}

func collectorFor(t *testing.T, f *gcFixture, owners x.OwnerResolver, finalizer x.FinalizerPort, utc func() time.Time) *collector {
	t.Helper()
	owner, err := New(Config{Base: gcBase(utc), JS: f.js, Owners: owners, Finalizer: finalizer})
	if err != nil {
		t.Fatal(err)
	}
	return owner.(*collector)
}

func testGCRow(origin x.Origin, name, version string, phase x.Phase) x.LedgerRow {
	id := x.Identity{SID: "owners", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: name, EntryNonce: "nonce-" + name, IntentDigest: "intent", ObjectVersion: version}
	start := time.Now().UTC()
	return x.LedgerRow{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, Revision: 1, Intent: x.Intent{Verb: "push", Tier: "b", Size: 7}, StartedAt: start,
		BudgetSeal: x.BudgetSeal{StartedAt: start, EffectiveBudgetNS: int64(300 * time.Second)}, Phase: phase}
}

// origin: hot-update S7 plan §5.3 / T29 / M13
func TestTransferCollectorPreservesLiveAndSchedulesOrphan(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	store := f.store("owners", 0)
	live, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "live"}, bytes.NewReader([]byte("live payload")))
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "orphan"}, bytes.NewReader([]byte("orphan payload")))
	if err != nil {
		t.Fatal(err)
	}
	local := x.Origin{BrokerID: "new-home", InstanceID: "new-process", BootID: "new-boot", Generation: 2}
	origin := x.Origin{BrokerID: "old-home", InstanceID: "old-process", BootID: "old-boot", Generation: 1}
	row := testGCRow(origin, "live", live.NUID, x.PhaseSending)
	now := row.StartedAt.Add(4 * time.Hour)
	// The stored rate seal extends the original deadline. Object age and the
	// legacy size formula would each incorrectly expire this live transfer.
	row.BudgetSeal.StartedAt = now.Add(-10 * time.Minute)
	row.BudgetSeal.EffectiveBudgetNS = int64(2108 * time.Second)
	current := row
	current.Revision++ // The original owner advanced after identical primary/outbox snapshots.
	owners := &censusOwners{local: x.LedgerCensus{Origin: local, Ready: true, Complete: true, Rows: []x.LedgerRow{row, row}},
		remote: map[x.Origin]x.LedgerCensus{origin: {Origin: origin, Ready: true, Complete: true, Rows: []x.LedgerRow{current}}}}
	finalizer := &schedulingFinalizer{}
	c := collectorFor(t, f, owners, finalizer, func() time.Time { return now })
	c.born = now.Add(-5 * time.Hour)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	view, err := c.Pass(gcDeadline(t, time.Second))
	if err != nil || !view.Complete {
		t.Fatalf("complete cross-home pass: %+v, %v", view, err)
	}
	calls := finalizer.identities()
	if len(calls) != 1 || calls[0].TransferID != "orphan" || calls[0].ObjectVersion != orphan.NUID {
		t.Fatalf("must protect old live version and nominate exact orphan: %+v", calls)
	}
	if len(owners.calls) != 2 || owners.calls[1] != origin {
		t.Fatalf("did not consult original follower: %+v", owners.calls)
	}
	// Candidate selection itself must not delete either physical version.
	for _, info := range []*jetstream.ObjectInfo{live, orphan} {
		if _, err := store.GetInfo(f.ctx, info.Name); err != nil {
			t.Fatalf("collector directly removed %s: %v", info.Name, err)
		}
	}
}

// origin: hot-update S7 plan §5.3 / T29 / M13
func TestTransferCollectorIncompleteOwnersProtectEvidence(t *testing.T) {
	for _, defect := range []string{"unreachable", "unready", "incomplete", "wrong-origin", "absent", "nonce-conflict", "duplicate", "local-unreadable", "local-row-conflict", "bad-local-nonce", "protection-conflict"} {
		t.Run(defect, func(t *testing.T) {
			f := newGCFixture(t)
			defer f.close()
			store := f.store("owners", 0)
			info, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "pending"}, bytes.NewReader([]byte("pending payload")))
			if err != nil {
				t.Fatal(err)
			}
			origin := x.Origin{BrokerID: "old-home", InstanceID: "instance", BootID: "boot", Generation: 1}
			row := testGCRow(origin, "pending", info.NUID, x.PhaseCleanupPending)
			proof := x.LedgerCensus{Origin: origin, Ready: true, Complete: true, Rows: []x.LedgerRow{row}}
			owners := &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true, Rows: []x.LedgerRow{row}}, remote: map[x.Origin]x.LedgerCensus{}, fail: map[x.Origin]error{}}
			switch defect {
			case "unreachable":
				owners.fail[origin] = errors.New("origin disconnected")
			case "unready":
				proof.Ready = false
			case "incomplete":
				proof.Complete = false
			case "wrong-origin":
				proof.Origin.Generation++
			case "absent":
				proof.Rows = nil
			case "nonce-conflict":
				proof.Rows[0].Identity.EntryNonce = "replacement"
			case "duplicate":
				proof.Rows = append(proof.Rows, row)
			case "local-unreadable":
				owners.local.Complete = false
			case "local-row-conflict":
				conflict := row
				conflict.CanonicalStart = &x.CanonicalRecord{Identity: row.Identity, Digest: "different canonical bytes"}
				owners.local.Rows = append(owners.local.Rows, conflict)
			case "bad-local-nonce":
				owners.local.Origin = origin // Exercise local validation without a redundant remote guard.
				owners.local.Rows[0].EntryNonce = ""
			case "protection-conflict":
				owners.local.Entries = []x.Protection{{Identity: row.Identity, ObjectVersion: "foreign-version"}}
			}
			owners.remote[origin] = proof
			finalizer := &schedulingFinalizer{}
			now := row.StartedAt.Add(4 * time.Hour)
			c := collectorFor(t, f, owners, finalizer, func() time.Time { return now })
			c.born = now.Add(-5 * time.Hour)
			defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
			view, err := c.Pass(gcDeadline(t, time.Second))
			if err == nil || view.Complete || view.Code != x.CodeResultUnknown || len(finalizer.identities()) != 0 {
				t.Fatalf("incomplete origin authorized cleanup: census=%+v calls=%+v err=%v", view, finalizer.identities(), err)
			}
			if _, err := store.GetInfo(f.ctx, "pending"); err != nil {
				t.Fatalf("lost pending evidence: %v", err)
			}
		})
	}
}

// origin: hot-update S7 plan §5.3 / T29 / M13
func TestTransferCollectorLiveProtectionKeepsCleanupProgress(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	origin := x.Origin{BrokerID: "local", InstanceID: "instance", BootID: "boot", Generation: 1}
	row := testGCRow(origin, "cleanup", "version", x.PhaseCleanupPending)
	owners := &censusOwners{local: x.LedgerCensus{Origin: origin, Ready: true, Complete: true, Rows: []x.LedgerRow{row},
		Entries: []x.Protection{{Identity: row.Identity, Phase: row.Phase, ObjectVersion: row.Identity.ObjectVersion}}}}
	finalizer := &schedulingFinalizer{err: errors.New("writer fence remains pending")}
	c := collectorFor(t, f, owners, finalizer, time.Now)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	for pass := 0; pass < 2; pass++ {
		if _, err := c.Pass(gcDeadline(t, time.Second)); err == nil {
			t.Fatal("pending cleanup was reported complete")
		}
	}
	calls := finalizer.identities()
	if len(calls) != 2 || calls[0] != row.Identity || calls[1] != row.Identity {
		t.Fatalf("live responsibility disabled its finalizer or changed identity: %+v", calls)
	}
}

type tracedStore struct {
	jetstream.ObjectStore
	mu      sync.Mutex
	started chan struct{}
	active  int
	calls   int
	ctxs    []context.Context
}

func (s *tracedStore) List(ctx context.Context, opts ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error) {
	s.mu.Lock()
	s.active++
	s.calls++
	s.ctxs = append(s.ctxs, ctx)
	if s.started != nil {
		close(s.started)
		s.started = nil
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}()
	return s.ObjectStore.List(ctx, opts...)
}

type tracedJS struct {
	jetstream.JetStream
	store jetstream.ObjectStore
}

func (js tracedJS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return js.store, nil
}

func assertListsJoined(t *testing.T, s *tracedStore) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != 0 || s.calls == 0 {
		t.Fatalf("list did not join: active=%d calls=%d", s.active, s.calls)
	}
	for _, ctx := range s.ctxs {
		if _, bounded := ctx.Deadline(); !bounded || ctx.Err() == nil {
			t.Fatalf("list context was unbounded or not canceled after join: bounded=%v err=%v", bounded, ctx.Err())
		}
	}
}

// origin: hot-update S7 plan lead LQ3 / §3 / T30
func TestTransferTTLListCancellationReleasesNextPass(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	store := newTTLRaceStore(t, f)
	defer store.close()
	store.seed(t)
	// Prove the pinned SDK's actual TTL race. Metadata from the normal Put
	// expires before CREATE; none is published to release the stalled watch.
	rawCtx, rawCancel := context.WithTimeout(f.ctx, 2*time.Second)
	_, rawErr := store.List(rawCtx)
	rawCancel()
	if !errors.Is(rawErr, context.DeadlineExceeded) {
		t.Fatalf("TTL-empty fixture did not reach the SDK List wedge: %v", rawErr)
	}
	owners := &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}
	finalizer := &schedulingFinalizer{}
	c := collectorFor(t, f, owners, finalizer, time.Now)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	for _, site := range []string{"candidates", "aged-observation"} {
		t.Run(site, func(t *testing.T) {
			store.seed(t)
			trace := &tracedStore{ObjectStore: store}
			d := gcDeadline(t, 2*time.Second)
			result := make(chan error, 1)
			go func() {
				var err error
				if site == "candidates" {
					_, err = c.collectBucketObjects(c.root, trace, d)
				} else {
					err = c.observeAgedObjects(c.root, trace, d)
				}
				result <- err
			}()
			var err error
			select {
			case err = <-result:
			case <-time.After(3 * time.Second):
				c.cancel()
				select {
				case <-result:
				case <-time.After(time.Second):
					t.Fatal("unbounded List also ignored cancellation")
				}
				t.Fatal("List ignored its absolute deadline")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s list lost deadline evidence: %v", site, err)
			}
			assertListsJoined(t, trace)
		})
	}
	trace := &tracedStore{ObjectStore: store, started: make(chan struct{})}
	c.cfg.JS = tracedJS{JetStream: f.js, store: trace}
	store.seed(t)
	view, err := c.Pass(gcDeadline(t, 2*time.Second))
	if err == nil || view.Complete {
		t.Fatalf("stalled pass fabricated a complete census: %+v %v", view, err)
	}
	assertListsJoined(t, trace)
	view, err = c.Pass(gcDeadline(t, 2*time.Second))
	if err != nil || !view.Complete {
		t.Fatalf("next empty-stream reconcile pass did not recover: %+v %v", view, err)
	}
	assertListsJoined(t, trace)
	if trace.calls != 3 {
		t.Fatalf("next reconcile pass did not reach List: calls=%d", trace.calls)
	}
	stream, err := f.js.Stream(f.ctx, "OBJ_xfer-ttl")
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, time.Second, 10*time.Millisecond, func() bool {
		info, err := stream.Info(f.ctx)
		return err == nil && info.State.Consumers == 0 && info.State.Msgs == 0
	}) {
		t.Fatal("canceled lists left watcher consumers or wrote fixture metadata")
	}
}

// origin: hot-update S7 plan lead LQ3 / §5.3 / T30
func TestTransferCollectorListSuccessCancelsContexts(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	store := f.store("contexts", 0)
	if _, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "object"}, bytes.NewReader([]byte("payload"))); err != nil {
		t.Fatal(err)
	}
	c := collectorFor(t, f, &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}, &schedulingFinalizer{}, time.Now)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	trace := &tracedStore{ObjectStore: store}
	if objects, err := c.collectBucketObjects(c.root, trace, gcDeadline(t, time.Second)); err != nil || len(objects) != 1 {
		t.Fatalf("successful list: count=%d err=%v", len(objects), err)
	}
	if err := c.observeAgedObjects(c.root, trace, gcDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	assertListsJoined(t, trace)
}

// origin: hot-update S7 plan §5.3 / T29
func TestTransferCollectorEligibilityKeepsOriginalBudget(t *testing.T) {
	origin := x.Origin{BrokerID: "origin"}
	row := testGCRow(origin, "transfer", "version", x.PhaseReceiving)
	now := row.StartedAt.Add(10 * time.Minute)
	c := &collector{cfg: Config{Base: gcBase(func() time.Time { return now })}}
	row.StartedAt = now // A late source update cannot renew the sealed start.
	row.BudgetSeal.StartedAt = now.Add(-6 * time.Minute)
	if !c.rowEligible(row) {
		t.Fatal("rate-sealed original start was reset")
	}
	row.BudgetSeal.EffectiveBudgetNS = int64(2108 * time.Second)
	if c.rowEligible(row) {
		t.Fatal("rate seal was replaced with legacy size budget")
	}
	row.BudgetSeal = x.BudgetSeal{}
	row.StartedAt = now.Add(-6 * time.Minute)
	if !c.rowEligible(row) {
		t.Fatal("legacy zero seal lost its original size formula")
	}
	row.Phase = x.Phase("corrupt")
	if c.rowEligible(row) {
		t.Fatal("unknown phase authorized cleanup")
	}
	c.born = now.Add(-5 * time.Hour)
	for _, object := range []*jetstream.ObjectInfo{
		{Size: 7},
		{Size: uint64(proto.XferMaxBytes) + 1, ModTime: now.Add(-4 * time.Hour)},
		{Size: 7, ModTime: now.Add(-time.Minute)},
	} {
		if c.legacyOldEnough(object) {
			t.Fatalf("untrusted age/size nominated an orphan: %+v", object)
		}
	}
}

// origin: hot-update S7 plan §5.3 / T29 / M13
func TestTransferCollectorVersionHeadersCannotBecomeLegacy(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	store := f.store("owners", 0)
	info, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "version"}, bytes.NewReader([]byte("version payload")))
	if err != nil {
		t.Fatal(err)
	}
	message := nats.NewMsg("$O.xfer-owners.C." + info.NUID)
	message.Header.Set("Tether-Xfer-Version", "1")
	message.Data = []byte("version payload")
	if _, err := f.js.PublishMsg(f.ctx, message); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutBytes(f.ctx, "zero-data", nil); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(4 * time.Hour)
	finalizer := &schedulingFinalizer{}
	c := collectorFor(t, f, &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}, finalizer, func() time.Time { return now })
	c.born = now.Add(-5 * time.Hour)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	if view, err := c.Pass(gcDeadline(t, time.Second)); err == nil || view.Complete || len(finalizer.identities()) != 0 {
		t.Fatalf("v1 marker became legacy permission: %+v calls=%+v err=%v", view, finalizer.identities(), err)
	}
}

// origin: hot-update S7 plan §2.3 / §5.3 / T29 / M13
func TestTransferCollectorUnmappedVersionPreservesChunks(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	f.store("owners", 0)
	message := nats.NewMsg("$O.xfer-owners.C.unmapped")
	message.Header.Set("Tether-Xfer-Version", "1")
	message.Data = []byte("live upload without metadata")
	if _, err := f.js.PublishMsg(f.ctx, message); err != nil {
		t.Fatal(err)
	}
	finalizer := &schedulingFinalizer{}
	c := collectorFor(t, f, &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}, finalizer, time.Now)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	if view, err := c.Pass(gcDeadline(t, time.Second)); err == nil || view.Complete || view.Code != x.CodeResultUnknown {
		t.Fatalf("unmapped version became legacy absence: %+v %v", view, err)
	}
	stream, err := f.js.Stream(f.ctx, "OBJ_xfer-owners")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetLastMsgForSubject(f.ctx, message.Subject); err != nil || len(finalizer.identities()) != 0 {
		t.Fatalf("unmapped version was retired: calls=%+v err=%v", finalizer.identities(), err)
	}
}

// origin: hot-update S7 plan §8.1 / §5.3 / T30
func TestTransferCollectorRejectsClockAndAdmissionDrift(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	owners := &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}
	cfg := Config{Base: gcBase(time.Now), JS: f.js, Owners: owners, Finalizer: &schedulingFinalizer{}}
	missing := cfg
	missing.Clock = nil
	if owner, err := New(missing); err == nil || owner != nil {
		t.Fatal("incomplete clock configuration created an owner")
	}
	owner, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := owner.(*collector)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	d := gcDeadline(t, 30*time.Second)
	bounded, err := c.boundedDeadline(d)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := c.remaining(bounded)
	if err != nil || remaining > 5*time.Second || remaining < 4*time.Second {
		t.Fatalf("pass cap is not five seconds: %s %v", remaining, err)
	}
	now, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []x.Deadline{0, -1, x.Deadline(now - 1)} {
		if _, err := c.Pass(invalid); err == nil {
			t.Fatalf("invalid deadline %d performed work", invalid)
		}
	}
	c.cfg.Clock = func() (string, int64, error) { return "different-boot", now, nil }
	if _, err := c.Pass(d); err == nil || len(owners.calls) != 0 {
		t.Fatal("changed boot admitted a pass")
	}
}

// The optional production journal callback runs inside the collector's actual
// pass admission. Stop cannot retire that owner while its journal/authority
// borrower is in flight, even if object enumeration later refuses its census.
// origin: hot-update S7 plan §5.3 / §8.2 / FIX-TERM request 4
func TestTransferCollectorJoinsRetirementCheckpointPass(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRetirementCheckpointPass(t)
	}
	testharness.AssertNoGoroutineLeak(t, "retirement checkpoint pass", g)
	testharness.AssertNoFDLeak(t, "retirement checkpoint pass", fd, 0)
}

type checkpointPassOwners struct {
	*censusOwners
	entered chan x.Deadline
	release chan struct{}
}

func (o *checkpointPassOwners) CollectRetirementCheckpoints(d x.Deadline) error {
	o.entered <- d
	<-o.release
	return errors.New("retained original responsibility")
}
func exerciseRetirementCheckpointPass(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	owners := &checkpointPassOwners{censusOwners: &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}, entered: make(chan x.Deadline, 1), release: make(chan struct{})}
	finalizer := &schedulingFinalizer{}
	c := collectorFor(t, f, owners, finalizer, time.Now)
	done := make(chan error, 1)
	d := gcDeadline(t, 10*time.Second)
	go func() { _, err := c.Pass(d); done <- err }()
	released := false
	defer func() {
		if !released {
			close(owners.release)
			<-done
		}
		_ = c.Stop(gcDeadline(t, time.Second))
	}()
	var inherited x.Deadline
	select {
	case inherited = <-owners.entered:
	case err := <-done:
		released = true
		close(owners.release)
		t.Fatalf("managed pass omitted retirement callback: %v", err)
	case <-time.After(time.Second):
		t.Fatal("retirement callback not admitted")
	}
	remaining, err := controlbudget.Remaining(inherited)
	if err != nil || inherited > d || remaining > 5*time.Second {
		t.Fatalf("retirement callback renewed pass deadline: %v %v", remaining, err)
	}
	if err := c.Stop(gcDeadline(t, 10*time.Millisecond)); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("Stop retired a live checkpoint borrower: %v", err)
	}
	close(owners.release)
	released = true
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("checkpoint uncertainty was hidden by successful census")
		}
	case <-time.After(time.Second):
		t.Fatal("checkpoint pass did not join")
	}
	if err := c.Stop(gcDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pass(gcDeadline(t, time.Second)); err == nil {
		t.Fatal("stopped collector reacquired checkpoint admission")
	}
	// Cancellation is independent evidence. Also preserve journal uncertainty
	// on a fresh, uncanceled pass whose object scan otherwise succeeds.
	fresh := collectorFor(t, f, owners, finalizer, time.Now)
	defer func() { _ = fresh.Stop(gcDeadline(t, time.Second)) }()
	if _, err := fresh.Pass(gcDeadline(t, time.Second)); err == nil {
		t.Fatal("checkpoint uncertainty was hidden on an uncanceled pass")
	}
}
