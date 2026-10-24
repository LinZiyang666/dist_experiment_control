package infratransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/jsstream"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type trace struct {
	sync.Mutex
	lines []string
}

func (l *trace) Noticef(string, ...any) {}
func (l *trace) Warnf(string, ...any)   {}
func (l *trace) Fatalf(string, ...any)  {}
func (l *trace) Errorf(string, ...any)  {}
func (l *trace) Debugf(string, ...any)  {}
func (l *trace) Tracef(format string, args ...any) {
	l.Lock()
	defer l.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}
func (l *trace) reset() { l.Lock(); defer l.Unlock(); l.lines = nil }
func (l *trace) snapshot() []string {
	l.Lock()
	defer l.Unlock()
	return append([]string(nil), l.lines...)
}

// origin: hot-update S9 plan §8 / T43 / M21
func TestTransferAdapterRetiresConsumerBeforeReturn(t *testing.T) {
	s := newServer(t)
	log := &trace{}
	s.SetLogger(log, false, true)
	old, next := connect(t, s), connect(t, s)
	body := bytes.Repeat([]byte("synced original prefix and discarded suffix\n"), 5000)
	spec := specification(t, old, body)
	w, err := jsstream.NewTransferWriter(old, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	// A legal foreign-subject gap separates raw stream sequence from logical
	// chunk index. Otherwise an ordinal-as-sequence mutant could pass vacuously.
	if _, err = old.Publish(t.Context(), "$O.original.C.foreignNUID", []byte("foreign sequence gap")); err != nil {
		t.Fatal(err)
	}
	for idx := uint32(0); idx < spec.Chunks; idx++ {
		start, end := int(idx*spec.ChunkSize), min(len(body), int((idx+1)*spec.ChunkSize))
		if _, err = w.Append(idx, body[start:end], deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = w.Finalize(deadline(t)); err != nil {
		t.Fatal(err)
	}
	port, err := jsstream.NewTransferReader(old, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := WrapReader(port, func(i.Deadline) (jetstream.JetStream, error) { return next, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	chunk, err := r.ReadChunk(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	sink, err := os.CreateTemp(t.TempDir(), "original-sink")
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, sink)
	if _, err = sink.WriteAt(chunk.Data, 0); err != nil {
		t.Fatal(err)
	}
	if err = sink.Sync(); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, _ = h.Write(chunk.Data)
	hashState, _ := h.(encoding.BinaryMarshaler).MarshalBinary()
	cp := x.SinkCheckpoint{ChunkIndex: 1, DurableOffset: int64(len(chunk.Data)), LastConsumedStreamSeq: chunk.Receipt.StreamSeq, SHA256State: hashState}
	if cp.LastConsumedStreamSeq == uint64(cp.ChunkIndex) {
		t.Fatal("raw-sequence continuation lane has no legal stream gap")
	}
	if err = r.ConfirmSynced(cp); err != nil {
		t.Fatal(err)
	}
	// Fetch one more chunk without Sync. The successor must refetch it from
	// cp.LastConsumedStreamSeq, rather than use the old delivery ordinal.
	unsynced, err := r.ReadChunk(deadline(t))
	if err != nil || unsynced.Receipt.Index != 1 {
		t.Fatalf("second fetch did not exercise an unsynced suffix: %v", err)
	}
	stream, err := old.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	consumers := stream.ListConsumers(t.Context())
	var names []string
	for info := range consumers.Info() {
		names = append(names, info.Name)
	}
	if err = consumers.Err(); err != nil || len(names) != 1 {
		t.Fatalf("original consumer absent: %v / %d", err, len(names))
	}
	cid, err := old.Conn().GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	log.reset()
	cfg := config()
	cfg.Readers = []*Reader{r}
	cfg.FenceData = func(i.ID, i.Deadline) error { return nil } // The test owns each completed sink section synchronously.
	a := mustAdapter(t, cfg)
	seal, err := a.Fence(i.ID{8}, deadline(t))
	if err != nil || !seal.Complete {
		t.Fatalf("consumer retirement did not complete: %v", err)
	}
	if r.state.ConsumerIdentity != names[0] || r.state.ConnectionIdentity == "" || !reflect.DeepEqual(r.state.SinkCheckpoint, cp) || r.state.DirtyBarrier || r.state.DeletePending {
		t.Fatal("seal lost the exact consumer or original synced checkpoint")
	}
	exposed, err := r.Freeze(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	exposed.SinkCheckpoint.SHA256State[0] ^= 1
	retained, err := r.Freeze(deadline(t))
	if err != nil || !reflect.DeepEqual(retained.SinkCheckpoint, cp) {
		t.Fatal("caller snapshot mutation rewrote the original synced prefix")
	}
	if _, err = stream.Consumer(t.Context(), names[0]); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("old named consumer was not deleted: %v", err)
	}
	// Independent server protocol trace proves the old NC processed UNSUB,
	// then PING/PONG, before its consumer delete request. State booleans alone
	// would accept a mutant which pretends a socket barrier completed.
	unsub, ping, pong, deleted := -1, -1, -1, -1
	for n, line := range log.snapshot() {
		if !strings.Contains(line, fmt.Sprintf("cid:%d", cid)) {
			continue
		}
		if strings.Contains(line, "[UNSUB ") && unsub == -1 {
			unsub = n
		}
		if strings.Contains(line, "[PING]") && unsub != -1 && ping == -1 {
			ping = n
		}
		if strings.Contains(line, "[PONG]") && ping != -1 && pong == -1 {
			pong = n
		}
		if strings.Contains(line, "$JS.API.CONSUMER.DELETE."+spec.Stream+"."+names[0]) && deleted == -1 {
			deleted = n
		}
	}
	if unsub < 0 || ping <= unsub || pong <= ping || deleted <= pong {
		t.Fatalf("UNSUB/PONG/delete order missing: %d %d %d %d", unsub, ping, pong, deleted)
	}
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = r.ReadChunk(deadline(t)); err == nil {
		t.Fatal("return binding bypassed reader admission fence")
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	recovered, err := r.ReadChunk(deadline(t))
	if err != nil || recovered.Receipt != unsynced.Receipt || !bytes.Equal(recovered.Data, unsynced.Data) {
		t.Fatalf("successor changed the unsynced original chunk: %v", err)
	}
	if _, err = sink.Stat(); err != nil {
		t.Fatal("infra migration closed the original sink")
	}
	// A different successor owns a new consumer while the exact old name stays
	// absent. Delete-before-return is independent of transfer completion.
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Consumers != 1 {
		t.Fatalf("successor consumer census: %v", err)
	}
	if _, err = stream.Consumer(t.Context(), names[0]); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatal("successor adopted the original consumer")
	}
}

type deleteFault struct {
	jetstream.JetStream
	mu       sync.Mutex
	refuse   bool
	attempts int
}

func (j *deleteFault) DeleteConsumer(ctx context.Context, stream, name string) error {
	j.mu.Lock()
	j.attempts++
	refuse := j.refuse
	j.mu.Unlock()
	if refuse {
		return nats.ErrNoResponders
	}
	return j.JetStream.DeleteConsumer(ctx, stream, name)
}

// origin: hot-update S9 plan §8 / T43
func TestTransferAdapterRetainsUnconfirmedConsumerDelete(t *testing.T) {
	s := newServer(t)
	js := connect(t, s)
	body := bytes.Repeat([]byte("consumer"), 10000)
	spec := specification(t, js, body)
	w, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	for idx := uint32(0); idx < spec.Chunks; idx++ {
		start, end := int(idx*spec.ChunkSize), min(len(body), int((idx+1)*spec.ChunkSize))
		if _, err = w.Append(idx, body[start:end], deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = w.Finalize(deadline(t)); err != nil {
		t.Fatal(err)
	}
	fault := &deleteFault{JetStream: js, refuse: true}
	port, err := jsstream.NewTransferReader(fault, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := WrapReader(port, func(i.Deadline) (jetstream.JetStream, error) { return js, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fault.mu.Lock()
		fault.refuse = false
		fault.mu.Unlock()
		if err := r.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if _, err = r.ReadChunk(deadline(t)); err != nil {
		t.Fatal(err)
	}
	cfg := config()
	cfg.Readers = []*Reader{r}
	cfg.FenceData = func(i.ID, i.Deadline) error { return nil } // No sink syscall is active in this owned reader lane.
	a := mustAdapter(t, cfg)
	seal, err := a.Fence(i.ID{9}, deadline(t))
	if err == nil || seal.Complete {
		t.Fatal("unconfirmed consumer delete signed a complete seal")
	}
	if err = a.Rebind(seal, deadline(t)); err == nil {
		t.Fatal("unconfirmed delete rebound a successor")
	}
	fault.mu.Lock()
	count := fault.attempts
	fault.refuse = false
	fault.mu.Unlock()
	if count == 0 {
		t.Fatal("fault never hit the real consumer delete path")
	}
	seal, err = a.Fence(i.ID{9}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
}

// Port probes return explicit errors for unsupported IO. They exercise adapter
// registration/retry guards, not S7 persistence or live business continuity.
type snapshotWorker struct {
	state              x.WorkerState
	freezeErr, thawErr error
	freezes, thaws     int
}

func (*snapshotWorker) Prepare(x.Prepare, x.Deadline) (x.Status, error) {
	return x.Status{}, i.ErrUnsupported
}
func (*snapshotWorker) Start(x.Identity) error { return i.ErrUnsupported }
func (*snapshotWorker) Apply(x.Control, x.Deadline) (x.Status, error) {
	return x.Status{}, i.ErrUnsupported
}
func (*snapshotWorker) Status(x.Identity) (x.Status, error) { return x.Status{}, i.ErrUnsupported }
func (p *snapshotWorker) Freeze(x.Deadline) (x.WorkerFrozen, error) {
	p.freezes++
	return &workerSnapshot{p}, p.freezeErr
}
func (p *snapshotWorker) Thaw(x.Bindings, x.Deadline) error { p.thaws++; return p.thawErr }
func (*snapshotWorker) Stop(x.Deadline) error               { return i.ErrUnsupported }

type workerSnapshot struct{ p *snapshotWorker }

func (f *workerSnapshot) Export() (x.WorkerState, []handoff.Resource, error) {
	state, err := copyState(f.p.state)
	return state, nil, err
}
func (*workerSnapshot) DetachPristine() (x.Salvage, error) { return x.Salvage{}, i.ErrUnsupported }

// origin: hot-update S9 plan §8 / T42–T43
func TestTransferAdapterRejectsIncompleteAndForeignSeals(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"source", func(c *Config) { c.SourceSHA256 = i.Digest{} }},
		{"schema", func(c *Config) { c.SchemaSHA256[0] ^= 1 }},
		{"recovery", func(c *Config) { c.BeforeReplay = nil }},
		{"typed-nil-worker", func(c *Config) {
			c.Workers = []Worker{{Owner: (*snapshotWorker)(nil), Current: func(i.Deadline) (x.Bindings, error) { return x.Bindings{}, nil }}}
		}},
		{"nil-current", func(c *Config) { c.Workers = []Worker{{Owner: &snapshotWorker{}}} }},
		{"duplicate-worker", func(c *Config) {
			p := &snapshotWorker{}
			w := Worker{Owner: p, Current: func(i.Deadline) (x.Bindings, error) { return x.Bindings{}, nil }}
			c.Workers = []Worker{w, w}
		}},
		{"capacity", func(c *Config) { c.Readers = make([]*Reader, 1025) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := config()
			tc.change(&c)
			if _, err := New(c); err == nil {
				t.Fatal("incomplete registrations were admitted")
			}
		})
	}
	cfg := config()
	recoveryCalls := 0
	cfg.BeforeReplay = func(i.ID, i.Deadline) error { recoveryCalls++; return nil }
	a := mustAdapter(t, cfg)
	if _, err := a.Fence(i.ID{}, deadline(t)); err == nil {
		t.Fatal("empty transaction accepted")
	}
	if _, err := a.Fence(i.ID{1}, 0); err == nil {
		t.Fatal("invalid operation deadline accepted")
	}
	seal, err := a.Fence(i.ID{1}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Fence(i.ID{2}, deadline(t)); err == nil {
		t.Fatal("second transaction stole the fence")
	}
	for _, field := range []string{"Version", "SourceSHA256", "SchemaSHA256", "InvocationSet", "ReceiptSet", "ConsumerSet", "Complete"} {
		t.Run(field, func(t *testing.T) {
			bad := seal
			value := reflect.ValueOf(&bad).Elem().FieldByName(field)
			switch value.Kind() {
			case reflect.Uint16:
				value.SetUint(2)
			case reflect.Bool:
				value.SetBool(false)
			case reflect.Array:
				value.Index(0).SetUint(value.Index(0).Uint() ^ 1)
			}
			if err := a.Rebind(bad, deadline(t)); err == nil {
				t.Fatal("tampered seal rebound")
			}
			if err := a.Release(bad, deadline(t)); err == nil {
				t.Fatal("tampered seal released")
			}
		})
	}
	if recoveryCalls != 0 {
		t.Fatal("foreign seal reached recovery side effects")
	}
	if err := a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §8 / T42
func TestTransferAdapterKeepsPartialReleaseAndOriginalDeadline(t *testing.T) {
	for _, b := range []x.Bindings{
		{OwnerGeneration: 2, Transport: &transport{}},
		{OwnerGeneration: 2, Authority: &authority{}},
		{OwnerGeneration: 2, Authority: &authority{}, Transport: (*transport)(nil)},
		{OwnerGeneration: 1, Authority: &authority{}, Transport: &transport{}},
	} {
		cfg := config()
		p := &snapshotWorker{state: x.WorkerState{Admission: x.AdmissionState{Closed: true}, Lineage: x.Lineage{Generation: 2}}}
		cfg.Workers = []Worker{{Owner: p, Current: func(i.Deadline) (x.Bindings, error) { return b, nil }}}
		a := mustAdapter(t, cfg)
		seal, err := a.Fence(i.ID{9}, deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err = a.Rebind(seal, deadline(t)); err == nil || p.thaws != 0 {
			t.Fatal("incomplete or older successor binding admitted business")
		}
	}
	first := &snapshotWorker{state: x.WorkerState{Admission: x.AdmissionState{Closed: true}}}
	second := &snapshotWorker{state: first.state, thawErr: nats.ErrNoResponders}
	cfg := config()
	var received []i.Deadline
	current := func(d i.Deadline) (x.Bindings, error) {
		received = append(received, d)
		return x.Bindings{Authority: &authority{}, Transport: &transport{}, OwnerGeneration: 1}, nil
	}
	cfg.Workers = []Worker{{Owner: first, Current: current}, {Owner: second, Current: current}}
	a := mustAdapter(t, cfg)
	d := deadline(t)
	seal, err := a.Fence(i.ID{1}, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Rebind(seal, d); err != nil {
		t.Fatal(err)
	}
	if err := a.Release(seal, d); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatal("partial release discarded failure")
	}
	second.thawErr = nil
	if err := a.Release(seal, d); err != nil {
		t.Fatal(err)
	}
	if first.thaws != 1 || second.thaws != 2 || len(received) != 2 || received[0] != d || received[1] != d {
		t.Fatal("retry rebound active owner or renewed operation deadline")
	}
	// The adapter's bounded serialization cannot enter side effects after an
	// expired queue wait. The test uses no background worker to make it pass.
	a.serial <- struct{}{}
	short, err := controlbudget.After(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Fence(i.ID{2}, short); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatal("blocked lifecycle ignored original deadline")
	}
	<-a.serial
	if first.freezes != 1 || second.freezes != 1 {
		t.Fatal("expired fence reached an owner")
	}
}

// Real durable ledger and tracker lifecycle, with admission held until the
// recovery callback approves. No callback probe certifies a real Bind here.
// origin: hot-update S9 plan §4.3 / §8 / T42
func TestTransferAdapterTrackerWaitsForRecoveryGate(t *testing.T) {
	b := base()
	ledger, err := xferledger.New(x.LedgerConfig{Base: b, Root: t.TempDir(), FallbackRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	m, err := xfertrack.New(x.TrackerConfig{Base: b, Origin: origin, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	created := time.Now().UTC()
	id := identity()
	permit := x.CreationPermit{Identity: id, Origin: origin, IssuedAt: created, CreateUntil: created.Add(time.Minute), PermitNonce: "originalPermit", Signature: "verified-port"}
	if _, err := m.Admit(permit, x.Intent{Tier: "b", Verb: "push", Path: "target", Size: 32 << 10, ChunkSize: 32 << 10, SHA256: hexDigest([]byte("source"))}, deadline(t)); err != nil {
		t.Fatal(err)
	}
	before, err := m.Status(x.StatusRequest{Identity: id, Origin: origin}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config()
	calls, ready := 0, false
	cfg.BeforeReplay = func(i.ID, i.Deadline) error {
		if !ready {
			return nats.ErrNoResponders
		}
		return nil
	}
	cfg.Trackers = []Tracker{{Owner: m, Current: func(i.Deadline) (x.Bindings, error) {
		calls++
		return x.Bindings{Ledger: ledger, OwnerGeneration: 2}, nil
	}}}
	a := mustAdapter(t, cfg)
	if seal, err := a.Fence(i.ID{2}, deadline(t)); !errors.Is(err, i.ErrUnsupported) || seal.Complete {
		t.Fatal("live tracker admitted without a current history/finalizer binding")
	}
	backendCalls := 0
	cfg.Trackers[0].RebindBackend = func(x.Bindings, i.Deadline) error { backendCalls++; return nil } // This lane checks the host seam; P30 proves actual history IO.
	a = mustAdapter(t, cfg)
	seal, err := a.Fence(i.ID{2}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Rebind(seal, deadline(t)); !errors.Is(err, nats.ErrNoResponders) || calls != 0 || backendCalls != 0 {
		t.Fatal("tracker binding ran before pre-repair verification")
	}
	ready = true
	if err := a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	stillFrozen, _, err := a.trackers[0].frozen.Export()
	if err != nil || stillFrozen.Generation != 1 || !stillFrozen.Admission.Closed {
		t.Fatal("Rebind opened tracker admission or discarded the original generation")
	}
	ready = false
	if err := a.Release(seal, deadline(t)); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatal("Release bypassed renewed recovery verification")
	}
	ready = true
	if err := a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	after, err := m.Status(x.StatusRequest{Identity: id, Origin: origin}, deadline(t))
	if err != nil || after.Identity != before.Identity || after.Origin != before.Origin || after.Budget.RemainingNS > before.Budget.RemainingNS || backendCalls != 1 {
		t.Fatal("tracker resumed with a replaced invocation or stale backend")
	}
	frozen, err := m.Freeze(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := frozen.Export()
	if err != nil || state.Generation != 2 || len(state.Entries) != 1 || state.Entries[0].BudgetSeal != stillFrozen.Entries[0].BudgetSeal ||
		!reflect.DeepEqual(state.WatchdogDeadlines, stillFrozen.WatchdogDeadlines) {
		t.Fatal("tracker did not bind the actual current backend")
	}
}

// origin: hot-update S9 plan §8 / T42–T43
func TestTransferAdapterRequiresClosedActorAndDataClosure(t *testing.T) {
	cases := []struct {
		name   string
		state  x.WorkerState
		resume bool
	}{
		{"open-admission", x.WorkerState{}, false},
		{"inflight-callback", x.WorkerState{Admission: x.AdmissionState{Closed: true, Inflight: 1}}, false},
		{"receiver-thaw-unbound", x.WorkerState{Admission: x.AdmissionState{Closed: true}, Receivers: []x.ReceiverState{{Identity: identity()}}}, false},
		{"writer-unregistered", x.WorkerState{Admission: x.AdmissionState{Closed: true}, Senders: []x.SenderState{{Identity: identity(), Writer: x.WriterState{ObjectSpec: x.ObjectSpec{Identity: identity()}}}}}, false},
		{"reader-unregistered", x.WorkerState{Admission: x.AdmissionState{Closed: true}, Receivers: []x.ReceiverState{{Identity: identity(), Reader: x.ReaderState{ObjectSpec: x.ObjectSpec{Identity: identity()}, Phase: x.ReaderOpen}}}}, true},
		{"dirty-consumer", x.WorkerState{Admission: x.AdmissionState{Closed: true}, Receivers: []x.ReceiverState{{Reader: x.ReaderState{DirtyBarrier: true}}}}, true},
		{"courier-delete-pending", x.WorkerState{Admission: x.AdmissionState{Closed: true}, Couriers: []x.CourierState{{Cleanup: x.CleanupState{Consumers: []x.ReaderState{{DeletePending: true}}}}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &snapshotWorker{state: tc.state}
			cfg := config()
			w := Worker{Owner: p, Current: func(i.Deadline) (x.Bindings, error) { return x.Bindings{}, i.ErrUnsupported }}
			if tc.resume {
				w.ResumeActors = func(i.Deadline) error { return nil }
			}
			cfg.Workers = []Worker{w}
			a := mustAdapter(t, cfg)
			if seal, err := a.Fence(i.ID{1}, deadline(t)); err == nil || seal.Complete {
				t.Fatal("incomplete actor closure signed a seal")
			}
		})
	}
	p := &snapshotWorker{state: x.WorkerState{Admission: x.AdmissionState{Closed: true}}, freezeErr: nats.ErrNoResponders}
	cfg := config()
	cfg.Workers = []Worker{{Owner: p, Current: func(i.Deadline) (x.Bindings, error) { return x.Bindings{}, i.ErrUnsupported }}}
	a := mustAdapter(t, cfg)
	if seal, err := a.Fence(i.ID{2}, deadline(t)); !errors.Is(err, nats.ErrNoResponders) || seal.Complete {
		t.Fatal("failed freeze became a complete inventory")
	}
	p.freezeErr = nil
	if _, err := a.Fence(i.ID{2}, deadline(t)); err != nil {
		t.Fatal(err)
	}
	cfg = config()
	p = &snapshotWorker{state: x.WorkerState{Admission: x.AdmissionState{Closed: true}}}
	cfg.Workers = []Worker{{Owner: p, Current: func(i.Deadline) (x.Bindings, error) { return x.Bindings{}, i.ErrUnsupported }}}
	cfg.FenceData = func(i.ID, i.Deadline) error { return nats.ErrNoResponders }
	a = mustAdapter(t, cfg)
	if seal, err := a.Fence(i.ID{4}, deadline(t)); !errors.Is(err, nats.ErrNoResponders) || seal.Complete || p.freezes != 0 {
		t.Fatal("producer section was not joined before owner snapshots")
	}
}

// origin: hot-update S9 plan §8 / T42 / M21
func TestTransferAdapterSealBindsEveryPrivateResponsibility(t *testing.T) {
	state := x.WorkerState{Admission: x.AdmissionState{Closed: true},
		Entries:  []x.WorkerEntry{{Prepare: x.Prepare{Identity: identity(), Budget: x.BudgetSeal{StartedAt: time.Unix(1, 0)}}}},
		Senders:  []x.SenderState{{SourceIdentity: x.FileIdentity{}, ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: 12}}},
		Couriers: []x.CourierState{{Attempt: x.Attempt{Sent: true}, Canonical: x.CanonicalRecord{Bytes: []byte("private-canonical")}}}}
	sealState := func(s x.WorkerState) i.TransferSeal {
		cfg := config()
		cfg.Workers = []Worker{{Owner: &snapshotWorker{state: s}, Current: func(i.Deadline) (x.Bindings, error) { return x.Bindings{}, i.ErrUnsupported }}}
		seal, err := mustAdapter(t, cfg).Fence(i.ID{3}, deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		return seal
	}
	original := sealState(state)
	changes := []struct {
		name   string
		mutate func(*x.WorkerState)
	}{
		{"NUID", func(s *x.WorkerState) { s.Entries[0].Prepare.Identity.ObjectVersion = "replacementNUID" }},
		{"start", func(s *x.WorkerState) { s.Entries[0].Prepare.Budget.StartedAt = time.Unix(2, 0) }},
		{"remaining", func(s *x.WorkerState) { s.Entries[0].Prepare.Budget.RemainingNS++ }},
		{"rate", func(s *x.WorkerState) { s.Entries[0].Prepare.Budget.RateLimitBPS++ }},
		{"byte-deadline", func(s *x.WorkerState) { s.Senders[0].ByteDeadline.Deadline++ }},
		{"source", func(s *x.WorkerState) { s.Senders[0].Source.ResourceID = "replacementSource" }},
		{"terminal-bytes", func(s *x.WorkerState) { s.Couriers[0].Canonical.Bytes[0] ^= 1 }},
		{"sent-unknown", func(s *x.WorkerState) { s.Couriers[0].Attempt.Sent = false }},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			copy, err := copyState(state)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&copy)
			if got := sealState(copy); got.ReceiptSet == original.ReceiptSet {
				t.Fatal("seal omitted original private responsibility")
			}
		})
	}
}
