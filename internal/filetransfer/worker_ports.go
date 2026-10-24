package filetransfer

// worker_ports.go binds the persistent transfer worker's frozen factories to
// this agent: its filesystem policy, its CURRENT session for control requests
// and a dedicated data attachment for object bytes. The frozen actor configs
// carry no path, origin or sealed source, so the directed broker carriers
// record those facts here, keyed by the retained identity, before the worker
// creates an actor. Nothing here mints identity, authority or a terminal.
// origin: hot-update S7 plan §4 / §8.1 / §12.2 P20 / INTEG-D agent wiring

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// workerPlan holds the per-entry facts the broker's directed carriers delivered.
type workerPlan struct {
	identity x.Identity
	origin   x.Origin
	intent   x.Intent
	inline   []byte
	budget   x.BudgetSeal
	source   *x.SourceSeal
}

type workerPorts struct {
	h        *Handler
	boot     string
	capacity *workerCapacity
	mu       sync.Mutex
	plans    map[x.Key]*workerPlan
	seals    map[x.Identity]x.SourceSeal
	couriers map[x.Identity]x.Courier
	data     map[x.Identity]*nats.Conn
}

func newWorkerPorts(h *Handler) *workerPorts {
	// A missing boot identity is surfaced by the clock on first use; the worker
	// constructor then refuses rather than comparing incomparable BOOTTIMEs.
	boot, _ := procrequest.CurrentBootID()
	return &workerPorts{h: h, boot: boot, capacity: &workerCapacity{held: make(map[x.ReservationID]x.Reservation)},
		plans: make(map[x.Key]*workerPlan), seals: make(map[x.Identity]x.SourceSeal),
		couriers: make(map[x.Identity]x.Courier), data: make(map[x.Identity]*nats.Conn)}
}

func (p *workerPorts) clock() (string, int64, error) {
	if p.boot == "" {
		return "", 0, errors.New("filetransfer: boot identity unavailable")
	}
	now, err := controlbudget.Now()
	return p.boot, now, err
}

// fillConfig supplies the agent-bound ports a host left unset. An explicitly
// provided port (tests, image import) always wins; nothing is overwritten.
func (p *workerPorts) fillConfig(cfg *x.WorkerConfig) {
	if cfg.Clock == nil {
		cfg.Clock = p.clock
	}
	if cfg.UTC == nil {
		cfg.UTC = time.Now
	}
	if cfg.Capacity == nil {
		cfg.Capacity = p.capacity
	}
	if cfg.Authority == nil {
		cfg.Authority = workerAuthority{h: p.h}
	}
	if cfg.Transport == nil {
		cfg.Transport = p
	}
	if cfg.MakeSender == nil {
		cfg.MakeSender = loggedFactory(p, "sender", p.makeSender)
	}
	if cfg.MakeReceiver == nil {
		cfg.MakeReceiver = loggedFactory(p, "receiver", p.makeReceiver)
	}
	if cfg.MakeCourier == nil {
		cfg.MakeCourier = loggedFactory(p, "courier", p.makeCourier)
	}
}

// Factory refusals happen inside the worker's runner, after the carrier reply
// was sent; the debug record is the only local trace of why an actor is absent.
func loggedFactory[C any, A any](p *workerPorts, actor string, make func(C) (A, error)) func(C) (A, error) {
	return func(cfg C) (A, error) {
		created, err := make(cfg)
		if err != nil && p.h.Logger != nil {
			p.h.Logger.Debug("agent: transfer actor not created", "actor", actor, "err", err)
		}
		return created, err
	}
}

// record retains a carrier's facts. The identity is the broker-authenticated
// retained identity; a different entry under the same key is never merged.
func (p *workerPorts) record(r x.Request, budget x.BudgetSeal, source *x.SourceSeal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := x.Key{SID: r.Identity.SID, TransferID: r.Identity.TransferID}
	plan := p.plans[k]
	if plan == nil {
		if r.Op != x.OpPrepare || r.Intent == nil {
			return errors.New(x.CodeOwnerRecovering)
		}
		plan = &workerPlan{identity: r.Identity, origin: r.Origin, intent: *r.Intent, inline: append([]byte(nil), r.Inline...)}
		p.plans[k] = plan
	}
	if plan.identity != r.Identity || plan.origin != r.Origin {
		return errors.New(x.CodeIntentConflict)
	}
	// Budgets only arrive from the deciding broker and keep the original start.
	if plan.budget.StartedAt.IsZero() || budget.StartedAt.Equal(plan.budget.StartedAt) {
		plan.budget = budget
	} else {
		return errors.New(x.CodeIntentConflict)
	}
	if source != nil {
		if plan.source != nil && *plan.source != *source {
			return errors.New(x.CodeSourceChanged)
		}
		seal := *source
		plan.source = &seal
	}
	return nil
}

func (p *workerPorts) plan(id x.Identity) (workerPlan, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan := p.plans[x.Key{SID: id.SID, TransferID: id.TransferID}]
	if plan == nil || plan.identity != id {
		return workerPlan{}, false
	}
	copy := *plan
	copy.inline = append([]byte(nil), plan.inline...)
	return copy, true
}

// makeReceiver builds the push receiver at the original destination. The
// sealed budget and geometry come from the broker's commit carrier; Tier A
// reads the retained inline bytes, Tier B the fixed object version.
func (p *workerPorts) makeReceiver(cfg x.ReceiveConfig) (x.Receiver, error) {
	plan, ok := p.plan(cfg.Identity)
	if !ok || !plan.budget.SourceSealed || (cfg.Identity.ObjectVersion != "" && plan.source == nil) {
		return nil, errors.New(x.CodeOwnerRecovering)
	}
	vp, err := transferpath.ValidateForWrite(plan.intent.Path, p.h.Roots, p.h.Mode)
	if err != nil {
		return nil, err
	}
	nonce, err := workerNonce()
	if err != nil {
		return nil, err
	}
	sink, err := transferpath.NewAtomicReceiver(vp, nonce, plan.intent.Force)
	if err != nil {
		return nil, err
	}
	inline := newInlineReader(plan.inline)
	var reader x.ObjectReader = inline
	if cfg.Identity.ObjectVersion != "" {
		spec, specErr := p.objectSpec(cfg.Identity, *plan.source)
		if specErr == nil {
			reader, specErr = jsstream.NewTransferReader(p.h.JetStream(), spec, inline.confirmed)
		}
		if specErr != nil {
			return nil, errors.Join(specErr, stopAtomic(sink))
		}
	}
	cfg.Budget, cfg.Reader, cfg.Atomic = plan.budget, reader, sink
	receiver, err := xferreceive.New(cfg)
	if err != nil {
		return nil, errors.Join(err, stopAtomic(sink), stopReader(reader))
	}
	return receiver, nil
}

// makeSender pins the original source FD for a pull. Its Tier B writer is
// created only after the sender sealed the source, from that exact seal.
func (p *workerPorts) makeSender(cfg x.SendConfig) (x.Sender, error) {
	plan, ok := p.plan(cfg.Identity)
	if !ok {
		return nil, errors.New(x.CodeOwnerRecovering)
	}
	vp, err := transferpath.ValidateForRead(plan.intent.Path, p.h.Roots, p.h.Mode)
	if err != nil {
		return nil, err
	}
	source, err := transferpath.OpenForReadAtomic(vp)
	if err != nil {
		return nil, err
	}
	cfg.Source = source
	if cfg.Intent.Tier == "b" {
		cfg.Writer = &deferredWriter{ports: p, identity: cfg.Identity}
	}
	sender, err := xfersend.New(cfg)
	if err != nil {
		return nil, errors.Join(err, source.Close())
	}
	return sender, nil
}

func (p *workerPorts) makeCourier(cfg x.CourierConfig) (x.Courier, error) {
	courier, err := xfercourier.New(cfg)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.couriers[cfg.Identity] = courier
	p.mu.Unlock()
	return courier, nil
}

func (p *workerPorts) objectSpec(id x.Identity, seal x.SourceSeal) (x.ObjectSpec, error) {
	js := p.h.JetStream()
	if js == nil {
		return x.ObjectSpec{}, errors.New(x.CodeOwnerRecovering)
	}
	bucket := proto.XferBucketName(id.SID)
	// ctx-none: frozen actor factories carry no context; this lookup is bounded.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return x.ObjectSpec{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return x.ObjectSpec{}, err
	}
	return x.ObjectSpec{Identity: id, Bucket: bucket, Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano),
		Size: seal.Size, SHA256: seal.SHA256, ChunkSize: seal.ChunkSize, Chunks: seal.Chunks}, nil
}

// Request is the worker's agent→broker control port on the authenticated
// agent subtree. A sender's read-only seal reconciliation replays the exact
// seal it already sent (the tracker returns the same status for it); a
// terminal receipt binds the deciding broker's canonical bytes into the
// retained courier before the courier evaluates the ACK.
// origin: hot-update S7 plan §4 / §6.3 / INTEG-D agent verbs
func (p *workerPorts) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	var attempt x.Attempt
	if plan, ok := p.plan(r.Identity); ok && r.Origin == (x.Origin{}) {
		r.Origin = plan.origin
	}
	if r.Op == x.OpStatus {
		p.mu.Lock()
		seal, ok := p.seals[r.Identity]
		p.mu.Unlock()
		if !ok {
			return x.Response{}, attempt, errors.New(x.CodeReconcileRequired)
		}
		r.Op, r.Source = x.OpSource, &seal
	}
	if r.Op == x.OpSource && r.Source != nil {
		p.mu.Lock()
		p.seals[r.Identity] = *r.Source
		p.mu.Unlock()
	}
	subject, body, err := proto.EncodeAgentTransferRequest(r)
	if err != nil {
		return x.Response{}, attempt, err
	}
	js := p.h.JetStream()
	if js == nil || js.Conn() == nil || js.Conn().IsClosed() {
		return x.Response{}, attempt, errors.New(x.CodeOwnerRecovering)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return x.Response{}, attempt, err
	}
	// ctx-none: the frozen Transport port supplies an absolute deadline, no caller ctx.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	reply, err := js.Conn().RequestWithContext(ctx, subject, body)
	if err != nil {
		attempt.Sent = !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, nats.ErrInvalidConnection)
		p.debug("agent: transfer request unanswered", r, err)
		return x.Response{}, attempt, err
	}
	attempt.Sent = true
	response, err := proto.DecodeTransferResponse(r.Op, "", reply.Data)
	if err != nil {
		p.debug("agent: transfer reply undecodable", r, err)
		return x.Response{}, attempt, errors.Join(errors.New(x.CodeReconcileRequired), err)
	}
	if response.Code != "" {
		p.debug("agent: transfer request refused", r, errors.New(response.Code))
	}
	if r.Op == x.OpTerminal && response.Receipt != nil {
		record, err := proto.DecodeTransferTerminalCanonical(r.Identity, reply.Data)
		p.mu.Lock()
		courier := p.couriers[r.Identity]
		p.mu.Unlock()
		if err == nil && record != nil && courier != nil {
			if err = xfercourier.BindCanonical(courier, *record); err != nil {
				return x.Response{}, attempt, err
			}
		}
	}
	return response, attempt, nil
}

func (p *workerPorts) debug(message string, r x.Request, err error) {
	if p.h.Logger != nil {
		p.h.Logger.Debug(message, "op", r.Op, "transfer_id", r.Identity.TransferID, "err", err)
	}
}

// OpenData dials this invocation's dedicated data attachment. Reconnect
// buffering is disabled, so a stale publish is never replayed after the
// writer's retirement fence.
func (p *workerPorts) OpenData(id x.Identity, _ x.Deadline) (jetstream.JetStream, error) {
	p.mu.Lock()
	nc := p.data[id]
	p.mu.Unlock()
	if nc == nil {
		dialed, err := p.dialData()
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		if existing := p.data[id]; existing != nil {
			dialed.Close()
			dialed = existing
		}
		p.data[id] = dialed
		nc = dialed
		p.mu.Unlock()
	}
	return jetstream.New(nc)
}

// dialData opens a worker-owned attachment for object bytes. It clones the
// CURRENT control connection's options — the agent's identity, signature
// callback, private reply inbox and dialer — minus every session callback, so
// losing this attachment can neither tear down the control session nor arm its
// watchdogs, and with reconnect buffering off. Building it here keeps the
// pinned Agent files from growing.
// origin: hot-update S7 plan §2.3 / INTEG-D agent wiring
func (p *workerPorts) dialData() (*nats.Conn, error) {
	if p.h.JetStream == nil {
		return nil, errors.New(x.CodePayloadUnsupported)
	}
	js := p.h.JetStream()
	if js == nil || js.Conn() == nil {
		return nil, errors.New(x.CodeOwnerRecovering)
	}
	opts := js.Conn().Opts
	opts.ReconnectBufSize, opts.FlusherTimeout, opts.NoCallbacksAfterClientClose = -1, 5*time.Second, true
	opts.ClosedCB, opts.DisconnectedCB, opts.DisconnectedErrCB, opts.ConnectedCB, opts.ReconnectedCB = nil, nil, nil, nil, nil
	opts.DiscoveredServersCB, opts.AsyncErrorCB, opts.ReconnectErrCB, opts.LameDuckModeHandler = nil, nil, nil, nil
	return opts.Connect()
}

func (p *workerPorts) CloseData(id x.Identity, _ x.Deadline) error {
	p.mu.Lock()
	nc := p.data[id]
	delete(p.data, id)
	p.mu.Unlock()
	if nc != nil {
		nc.Close()
	}
	return nil
}

// workerAuthority: the deciding broker already authorized every carrier and
// the agent's own subject ACL scopes it. A stopped handler admits nothing new.
type workerAuthority struct{ h *Handler }

func (a workerAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error {
	if a.h.closed.Load() {
		return errors.New(x.CodeOwnerRecovering)
	}
	return nil
}

// deferredWriter constructs the fixed-version writer from the seal the sender
// already sent to the broker. It holds no bytes before that point.
type deferredWriter struct {
	ports    *workerPorts
	identity x.Identity
	mu       sync.Mutex
	writer   jsstream.Writer
}

func (w *deferredWriter) ensure(d x.Deadline) (jsstream.Writer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		return w.writer, nil
	}
	w.ports.mu.Lock()
	seal, ok := w.ports.seals[w.identity]
	w.ports.mu.Unlock()
	if !ok {
		return nil, errors.New(x.CodeReconcileRequired)
	}
	spec, err := w.ports.objectSpec(w.identity, seal)
	if err != nil {
		return nil, err
	}
	js, err := w.ports.OpenData(w.identity, d)
	if err != nil {
		return nil, err
	}
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		return nil, err
	}
	w.writer = writer
	return writer, nil
}

func (w *deferredWriter) Append(index uint32, data []byte, d x.Deadline) (x.ChunkReceipt, error) {
	writer, err := w.ensure(d)
	if err != nil {
		return x.ChunkReceipt{}, err
	}
	return writer.Append(index, data, d)
}
func (w *deferredWriter) ResolvePending(d x.Deadline) (x.ChunkReceipt, bool, error) {
	writer, err := w.ensure(d)
	if err != nil {
		return x.ChunkReceipt{}, false, err
	}
	return writer.ResolvePending(d)
}
func (w *deferredWriter) Finalize(d x.Deadline) (x.ObjectReceipt, error) {
	writer, err := w.ensure(d)
	if err != nil {
		return x.ObjectReceipt{}, err
	}
	return writer.Finalize(d)
}
func (w *deferredWriter) Snapshot(d x.Deadline) (x.WriterState, error) {
	writer, err := w.ensure(d)
	if err != nil {
		return x.WriterState{}, err
	}
	snapshot, ok := writer.(interface {
		Snapshot(x.Deadline) (x.WriterState, error)
	})
	if !ok {
		return x.WriterState{}, errors.New(x.CodeReconcileRequired)
	}
	return snapshot.Snapshot(d)
}
func (w *deferredWriter) Freeze(d x.Deadline) (x.WriterState, error) {
	writer, err := w.ensure(d)
	if err != nil {
		return x.WriterState{}, err
	}
	return writer.Freeze(d)
}
func (w *deferredWriter) Stop(d x.Deadline) error {
	w.mu.Lock()
	writer := w.writer
	w.mu.Unlock()
	var err error
	if writer != nil {
		err = writer.Stop(d)
	}
	if err == nil {
		err = w.ports.CloseData(w.identity, d)
	}
	return err
}

// inlineReader is the Tier A ObjectReader: one chunk of the retained bytes,
// stream sequence zero, then EOF once that prefix is durably synced. Its
// snapshot reports exactly the receiver's last confirmed sink checkpoint, the
// same agreement the receiver demands of an object reader.
type inlineReader struct {
	mu        sync.Mutex
	data      []byte
	confirmed x.SinkCheckpoint
}

func newInlineReader(data []byte) *inlineReader {
	// This is a standard-library invariant, not caller-controlled wire state.
	encoder, ok := sha256.New().(encoding.BinaryMarshaler)
	if !ok {
		panic("SHA-256 checkpoint encoder is unavailable")
	}
	empty, err := encoder.MarshalBinary()
	if err != nil {
		panic("SHA-256 initial checkpoint encoding failed")
	}
	return &inlineReader{data: data, confirmed: x.SinkCheckpoint{SHA256State: empty}}
}

func (r *inlineReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.confirmed.DurableOffset == int64(len(r.data)) {
		return x.Chunk{}, io.EOF
	}
	data := append([]byte(nil), r.data...)
	return x.Chunk{Data: data, Receipt: x.ChunkReceipt{Length: uint32(len(data)), SHA256: sha256.Sum256(data)}}, nil
}
func (r *inlineReader) ConfirmSynced(cp x.SinkCheckpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cp.DurableOffset < r.confirmed.DurableOffset || cp.DurableOffset > int64(len(r.data)) {
		return errors.New(x.CodeObjectConflict)
	}
	r.confirmed = cp
	r.confirmed.SHA256State = append([]byte(nil), cp.SHA256State...)
	return nil
}
func (r *inlineReader) Freeze(x.Deadline) (x.ReaderState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := r.confirmed
	cp.SHA256State = append([]byte(nil), cp.SHA256State...)
	return x.ReaderState{SinkCheckpoint: cp}, nil
}
func (r *inlineReader) Stop(x.Deadline) error { return nil }

func stopAtomic(sink x.AtomicPort) error {
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	return sink.Stop(d)
}

func stopReader(reader x.ObjectReader) error {
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	return reader.Stop(d)
}

func workerNonce() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// workerCode recovers the stable transfer code from a leaf error, never its
// private detail; an unrecognized error is reported as result unknown.
func workerCode(err error, fallback string) string {
	var coded interface{ TransferCode() string }
	if errors.As(err, &coded) && coded.TransferCode() != "" {
		return coded.TransferCode()
	}
	var pve *transferpath.PathValidationError
	if errors.As(err, &pve) {
		return pve.Code
	}
	for _, field := range strings.FieldsFunc(err.Error(), func(r rune) bool { return r == ' ' || r == ':' }) {
		if strings.HasPrefix(field, "transfer_") {
			return field
		}
	}
	return fallback
}

// workerCapacity is the agent's single bounded account for transfer actors.
// It refuses before allocation and never evicts live responsibility.
// origin: hot-update S7 plan §8.3
type workerCapacity struct {
	mu   sync.Mutex
	next uint64
	held map[x.ReservationID]x.Reservation
	used x.Reservation
}

func (a *workerCapacity) Reserve(r x.Reservation) (x.ReservationID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.NonReplayBytes < 0 || r.ReplayBytes < 0 || r.SealedBytes < 0 || r.PublicBytes < 0 || r.HandoffBytes < 0 ||
		r.NonReplayBytes > 64<<20-a.used.NonReplayBytes || r.ReplayBytes > 256<<20-a.used.ReplayBytes ||
		r.SealedBytes > 32<<20-a.used.SealedBytes || r.PublicBytes > 16<<20-a.used.PublicBytes ||
		r.HandoffBytes > 64<<20-a.used.HandoffBytes || r.FDs > 16384-a.used.FDs || r.Resources > 16384-a.used.Resources {
		return "", errors.New(x.CodeCapacityExceeded)
	}
	a.next++
	id := x.ReservationID("agent-" + hex.EncodeToString([]byte{byte(a.next >> 24), byte(a.next >> 16), byte(a.next >> 8), byte(a.next)}))
	a.held[id] = r
	a.used.NonReplayBytes += r.NonReplayBytes
	a.used.ReplayBytes += r.ReplayBytes
	a.used.SealedBytes += r.SealedBytes
	a.used.PublicBytes += r.PublicBytes
	a.used.HandoffBytes += r.HandoffBytes
	a.used.FDs += r.FDs
	a.used.Resources += r.Resources
	return id, nil
}

func (a *workerCapacity) Release(id x.ReservationID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.held[id]
	if !ok {
		return errors.New("filetransfer: unknown transfer reservation")
	}
	delete(a.held, id)
	a.used.NonReplayBytes -= r.NonReplayBytes
	a.used.ReplayBytes -= r.ReplayBytes
	a.used.SealedBytes -= r.SealedBytes
	a.used.PublicBytes -= r.PublicBytes
	a.used.HandoffBytes -= r.HandoffBytes
	a.used.FDs -= r.FDs
	a.used.Resources -= r.Resources
	return nil
}
