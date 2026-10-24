package xferagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go/jetstream"
)

// actorDeadline bounds factory network/file work. The worker calls factories
// from Start, inside its own admitted callback, with no deadline parameter.
const actorDeadline = 30 * time.Second

// objectSpec binds the permit's fixed object version to the CURRENT stream
// incarnation; the writer/reader then refuse a recreated stream on every call.
func (r *Runtime) objectSpec(js jetstream.JetStream, id x.Identity, size int64, sha string, chunk uint32, d x.Deadline) (x.ObjectSpec, error) {
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return x.ObjectSpec{}, err
	}
	if chunk == 0 {
		return x.ObjectSpec{}, fmt.Errorf("%s: tier-b chunk geometry is missing", x.CodePayloadUnsupported)
	}
	// ctx-none: frozen actor factories carry no caller ctx; this lookup is bounded.
	ctx, cancel := context.WithTimeout(context.Background(), min(remaining, 5*time.Second))
	defer cancel()
	bucket := proto.XferBucketName(id.SID)
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return x.ObjectSpec{}, fmt.Errorf("%s: %w", x.CodeOwnerRecovering, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return x.ObjectSpec{}, fmt.Errorf("%s: %w", x.CodeOwnerRecovering, err)
	}
	chunks := uint32((size + int64(chunk) - 1) / int64(chunk))
	return x.ObjectSpec{Identity: id, Bucket: bucket, Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano),
		SHA256: sha, Size: size, ChunkSize: chunk, Chunks: chunks}, nil
}

// makeSender serves a pull: the agent reads its pinned source and writes the
// fixed object version. The frozen writer binds the complete spec (size and
// digest) at construction, but only the sender's own Seal hashes the source;
// the writer is therefore built on first use from the seal the broker just
// acknowledged, so a large source is hashed once, not twice.
// origin: hot-update S7 plan §2 / §4 / P08 / P20
func (r *Runtime) makeSender(cfg x.SendConfig) (x.Sender, error) {
	if cfg.Intent.Tier != "a" && cfg.Intent.Tier != "b" {
		return nil, fmt.Errorf("%s: unknown sender tier", x.CodePayloadUnsupported)
	}
	d, err := controlbudget.After(actorDeadline)
	if err != nil {
		return nil, err
	}
	vp, err := transferpath.ValidateForRead(cfg.Intent.Path, r.host.Roots, r.host.Mode)
	if err != nil {
		return nil, err
	}
	source, err := transferpath.OpenForReadAtomic(vp)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = source.Close()
		}
	}()
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > proto.XferMaxBytes {
		return nil, fmt.Errorf("%s: source is not a bounded regular file", x.CodeSourceChanged)
	}
	// Tier A has no object version and no data attachment: the pinned sender
	// seals the source and retains its exact copy, which the original prepare
	// and every replay of it return through the link (Runtime.inlineOf).
	// origin: hot-update S7 plan §8.1 / FIX-INLINE (FIX-TIERA report item 3)
	if cfg.Intent.Tier == "a" {
		cfg.Source, cfg.Writer = source, nil
		sender, err := xfersend.New(cfg)
		if err != nil {
			return nil, err
		}
		keep = true
		live := &liveSender{Sender: sender, source: source, rt: r, id: cfg.Identity}
		r.retainInline(live)
		return live, nil
	}
	js, err := r.OpenData(cfg.Identity, d)
	if err != nil {
		return nil, err
	}
	writer := &sealedWriter{rt: r, id: cfg.Identity, js: js}
	cfg.Source, cfg.Writer = source, writer
	sender, err := xfersend.New(cfg)
	if err != nil {
		return nil, errors.Join(err, r.CloseData(cfg.Identity, d))
	}
	keep = true
	return &liveSender{Sender: sender, writer: writer, source: source}, nil
}

// liveSender lets the worker's current-cut rollback reopen the writer that
// the sender's Freeze closed; the sender's own cursor and hash are intact.
// Without it a refused or failed cut left every pull's writer closed.
// origin: hot-update S7 plan §8.2 / P21 — S7 production activation
type liveSender struct {
	x.Sender
	writer *sealedWriter // nil for Tier A: an inline sender has no object writer
	source *os.File
	ref    x.ResourceRef
	rt     *Runtime // Tier A only: forgets the retained copy once Stop succeeds
	id     x.Identity
}

func (s *liveSender) Freeze(d x.Deadline) (x.SenderState, error) {
	state, err := s.Sender.Freeze(d)
	if err == nil {
		s.ref = state.Source
	}
	return state, err
}
func (s *liveSender) Resources() []handoff.Resource {
	return []handoff.Resource{{ID: s.ref.ResourceID, Kind: s.ref.Kind, Files: map[string]*os.File{s.ref.Name: s.source}}}
}

func (s *liveSender) Thaw(d x.Deadline) error {
	if s.writer == nil {
		return nil // an inline sender's Freeze closes no writer to reopen
	}
	return s.writer.thaw(d)
}

// Stop retires the wrapped sender first; only a stopped inline sender (source
// closed, copy released) leaves the runtime's replay index. A failed Stop
// keeps both, so a retry and a later prepare replay still find the owner.
// origin: hot-update S7 plan §8.1 / FIX-INLINE
func (s *liveSender) Stop(d x.Deadline) error {
	if err := s.Sender.Stop(d); err != nil {
		return err
	}
	if s.rt != nil {
		s.rt.forgetInline(s)
	}
	return nil
}

// retainInline indexes a Tier A sender by entry for prepare replies.
func (r *Runtime) retainInline(s *liveSender) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inline == nil {
		r.inline = map[x.Key]*liveSender{}
	}
	r.inline[x.Key{SID: s.id.SID, TransferID: s.id.TransferID}] = s
}

func (r *Runtime) forgetInline(s *liveSender) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := x.Key{SID: s.id.SID, TransferID: s.id.TransferID}
	if r.inline[key] == s {
		delete(r.inline, key)
	}
}

// inlineOf returns the original entry's retained inline copy and the durable
// seal it was verified against. The sender serializes this read with its own
// steps; d bounds the wait, so a busy sender is "not yet", never a guess.
// origin: hot-update S7 plan §8.1 / FIX-INLINE
func (r *Runtime) inlineOf(id x.Identity, d x.Deadline) (x.SourceSeal, []byte, bool) {
	r.mu.Lock()
	s := r.inline[x.Key{SID: id.SID, TransferID: id.TransferID}]
	r.mu.Unlock()
	if s == nil || s.id != id {
		return x.SourceSeal{}, nil, false
	}
	seal, data, err := xfersend.Inline(s.Sender, d)
	return seal, data, err == nil
}

// Finalized keeps the registry's settled-sender check on the wrapped actor.
func (s *liveSender) Finalized() bool {
	f, ok := s.Sender.(interface{ Finalized() bool })
	return ok && f.Finalized()
}

// sealedWriter constructs the real fixed-version writer from the durably
// acknowledged source seal on its first use. Before the seal exists there is
// no object state to write, freeze or export.
type sealedWriter struct {
	rt       *Runtime
	id       x.Identity
	js       jetstream.JetStream
	mu       sync.Mutex
	inner    x.ObjectWriter
	imported *x.WriterState
}

func (w *sealedWriter) writer(d x.Deadline) (x.ObjectWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inner != nil {
		return w.inner, nil
	}
	if w.imported != nil {
		js, err := w.rt.OpenData(w.id, d)
		if err != nil {
			return nil, err
		}
		inner, err := jsstream.ImportTransferWriter(js, *w.imported)
		if err != nil {
			return nil, err
		}
		if err := jsstream.ThawTransferWriter(inner, d); err != nil {
			return nil, err
		}
		w.inner, w.js, w.imported = inner, js, nil
		return inner, nil
	}
	seal, ok := w.rt.sealOf(w.id)
	if !ok {
		return nil, fmt.Errorf("%s: source is not durably sealed", x.CodeReconcileRequired)
	}
	if w.js == nil {
		var err error
		w.js, err = w.rt.OpenData(w.id, d)
		if err != nil {
			return nil, err
		}
	}
	spec, err := w.rt.objectSpec(w.js, w.id, seal.Size, seal.SHA256, seal.ChunkSize, d)
	if err != nil {
		return nil, err
	}
	inner, err := jsstream.NewTransferWriter(w.js, spec)
	if err != nil {
		return nil, err
	}
	w.inner = inner
	return inner, nil
}

func (w *sealedWriter) Snapshot(d x.Deadline) (x.WriterState, error) {
	w.mu.Lock()
	if w.imported != nil {
		state := *w.imported
		w.mu.Unlock()
		return state, nil
	}
	w.mu.Unlock()
	inner, err := w.writer(d)
	if err != nil {
		return x.WriterState{}, err
	}
	s, ok := inner.(interface {
		Snapshot(x.Deadline) (x.WriterState, error)
	})
	if !ok {
		return x.WriterState{}, fmt.Errorf("%s: writer snapshot is unavailable", x.CodeReconcileRequired)
	}
	return s.Snapshot(d)
}

func (w *sealedWriter) Append(index uint32, data []byte, d x.Deadline) (x.ChunkReceipt, error) {
	inner, err := w.writer(d)
	if err != nil {
		return x.ChunkReceipt{}, err
	}
	return inner.Append(index, data, d)
}

func (w *sealedWriter) ResolvePending(d x.Deadline) (x.ChunkReceipt, bool, error) {
	inner, err := w.writer(d)
	if err != nil {
		return x.ChunkReceipt{}, false, err
	}
	return inner.ResolvePending(d)
}

func (w *sealedWriter) Finalize(d x.Deadline) (x.ObjectReceipt, error) {
	inner, err := w.writer(d)
	if err != nil {
		return x.ObjectReceipt{}, err
	}
	return inner.Finalize(d)
}

func (w *sealedWriter) Freeze(d x.Deadline) (x.WriterState, error) {
	w.mu.Lock()
	if w.imported != nil {
		state := *w.imported
		w.mu.Unlock()
		return state, nil
	}
	inner := w.inner
	w.mu.Unlock()
	if inner == nil {
		return x.WriterState{}, fmt.Errorf("%s: unsealed sender has no writer state", x.CodeReconcileRequired)
	}
	return inner.Freeze(d)
}

// thaw reopens a writer whose Freeze closed it (a no-op before first use).
func (w *sealedWriter) thaw(d x.Deadline) error {
	w.mu.Lock()
	inner := w.inner
	w.mu.Unlock()
	if inner == nil {
		return nil
	}
	return jsstream.ThawTransferWriter(inner, d)
}

// Stop retires the writer (when one exists) before closing its attachment.
func (w *sealedWriter) Stop(d x.Deadline) error {
	w.mu.Lock()
	inner := w.inner
	w.mu.Unlock()
	if inner != nil {
		if err := inner.Stop(d); err != nil {
			return err
		}
	}
	return w.rt.CloseData(w.id, d)
}

// makeReceiver serves a push: the agent reads the fixed object version into a
// pinned-parent tmp and commits only with the broker's durable grant. A push
// size and digest are fixed by the permit before the first byte, so the
// receiver's budget is sealed from the retained intent itself.
// origin: hot-update S7 plan §3 / §4 / P07 / P20
func (r *Runtime) makeReceiver(cfg x.ReceiveConfig) (x.Receiver, error) {
	intent, ok := r.intent(cfg.Identity)
	if !ok || intent.Verb != "push" || (intent.Tier != "a" && intent.Tier != "b") {
		return nil, fmt.Errorf("%s: receiver intent is not retained", x.CodeOwnerRecovering)
	}
	d, err := controlbudget.After(actorDeadline)
	if err != nil {
		return nil, err
	}
	vp, err := transferpath.ValidateForWrite(intent.Path, r.host.Roots, r.host.Mode)
	if err != nil {
		return nil, err
	}
	now, err := controlbudget.Now()
	if err != nil {
		return nil, err
	}
	atomic, err := transferpath.NewAtomicReceiver(vp, "x"+strconv.FormatInt(now, 36), intent.Force)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = atomic.Stop(d)
		}
	}()
	if !cfg.Budget.SourceSealed && cfg.Budget.Size == intent.Size {
		cfg.Budget.SourceSealed = true
	}
	// A Tier A push has no object to read: its authenticated payload is the
	// bytes the worker entry retained and reserved at Prepare, served without
	// a data attachment or a second copy. Refusing here failed every small
	// capable push at its commit grant (receiver creation).
	// origin: hot-update S7 plan §8.1 / FIX-INLINE
	if intent.Tier == "a" {
		worker, _ := r.boundWorker()
		inline, ok := xferworker.PreparedInline(worker, cfg.Identity)
		if !ok || int64(len(inline)) != intent.Size {
			return nil, fmt.Errorf("%s: inline payload is not retained", x.CodeOwnerRecovering)
		}
		cfg.Reader, cfg.Atomic = newInlineReader(inline), atomic
		receiver, err := xferreceive.New(cfg)
		if err != nil {
			return nil, err
		}
		keep = true
		return &liveReceiver{Receiver: receiver, atomic: atomic}, nil
	}
	js, err := r.OpenData(cfg.Identity, d)
	if err != nil {
		return nil, err
	}
	spec, err := r.objectSpec(js, cfg.Identity, intent.Size, intent.SHA256, intent.ChunkSize, d)
	if err != nil {
		return nil, errors.Join(err, r.CloseData(cfg.Identity, d))
	}
	reader, err := jsstream.NewTransferReader(js, spec, x.SinkCheckpoint{})
	if err != nil {
		return nil, errors.Join(err, r.CloseData(cfg.Identity, d))
	}
	data := &dataReader{inner: reader, rt: r, id: cfg.Identity, js: js, spec: spec}
	cfg.Reader, cfg.Atomic = data, atomic
	receiver, err := xferreceive.New(cfg)
	if err != nil {
		return nil, errors.Join(err, reader.Stop(d), r.CloseData(cfg.Identity, d))
	}
	keep = true
	return &liveReceiver{Receiver: receiver, reader: data, atomic: atomic}, nil
}

// liveReceiver lets the worker's current-cut rollback reopen what the
// receiver's Freeze fenced, in the order xferreceive.Thaw requires: the
// atomic sink, then a reader resumed from the receiver's own synced
// checkpoint (Freeze retired the old consumer), then receiver admission.
// origin: hot-update S7 plan §3 / §8.2 / T08 — S7 production activation
type liveReceiver struct {
	x.Receiver
	reader    *dataReader // nil for Tier A: the inline reader has no consumer to retire
	atomic    transferpath.AtomicReceiver
	resources []handoff.Resource
}

func (r *liveReceiver) Freeze(d x.Deadline) (x.ReceiverState, error) {
	state, err := r.Receiver.Freeze(d)
	if err == nil {
		_, r.resources, err = r.atomic.Freeze(d)
	}
	return state, err
}
func (r *liveReceiver) Resources() []handoff.Resource { return r.resources }

func (r *liveReceiver) Thaw(d x.Deadline) error {
	if err := transferpath.ThawAtomic(r.atomic, d); err != nil {
		return err
	}
	if r.reader == nil {
		return xferreceive.Thaw(r.Receiver)
	}
	if err := r.reader.reopen(r.Progress().Checkpoint, d); err != nil {
		return err
	}
	return xferreceive.Thaw(r.Receiver)
}

// Progress keeps the registry's non-cutting step view on the wrapped actor.
func (r *liveReceiver) Progress() x.ReceiverState {
	p, ok := r.Receiver.(interface{ Progress() x.ReceiverState })
	if !ok {
		return x.ReceiverState{}
	}
	return p.Progress()
}

// inlineReader serves a Tier A push's retained payload as the suffix after
// the receiver's synced checkpoint. It owns no connection and no copy: the
// worker entry retains and accounts the bytes, and ConfirmSynced is its only
// progress, so a rollback re-serves exactly the unsynced suffix. Its Freeze
// reports the synced checkpoint the receiver requires its reader to match.
// origin: hot-update S7 plan §3 / §8.1 / FIX-INLINE
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

// importInlineReader resumes a Tier A push's reader at its imported receiver's
// durable tmp prefix. The image carries no reader for it, only that prefix and
// the entry's retained payload; the prefix must be exactly that payload's
// (xferreceive.ValidInlineState), so the reader can never serve a suffix of
// other bytes, nor re-serve the synced prefix as if nothing had landed.
// origin: hot-update S7 plan §3 / §8.1–§8.2 / S7 FIX-TIERA2
func importInlineReader(payload []byte, state x.ReceiverState) (*inlineReader, error) {
	if !xferreceive.ValidInlineState(state, payload) {
		return nil, fmt.Errorf("%s: imported inline receiver is not its retained payload's prefix", x.CodeObjectConflict)
	}
	reader := newInlineReader(payload)
	reader.confirmed = state.Checkpoint
	reader.confirmed.SHA256State = bytes.Clone(state.Checkpoint.SHA256State)
	return reader, nil
}

func (r *inlineReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	offset := r.confirmed.DurableOffset
	if offset >= int64(len(r.data)) {
		return x.Chunk{}, io.EOF
	}
	rest := bytes.Clone(r.data[offset:])
	return x.Chunk{Data: rest, Receipt: x.ChunkReceipt{Offset: offset, Length: uint32(len(rest)), SHA256: sha256.Sum256(rest)}}, nil
}

func (r *inlineReader) ConfirmSynced(cp x.SinkCheckpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cp.DurableOffset < r.confirmed.DurableOffset || cp.DurableOffset > int64(len(r.data)) {
		return fmt.Errorf("%s: inline checkpoint is outside the retained payload", x.CodeObjectConflict)
	}
	r.confirmed = cp
	r.confirmed.SHA256State = bytes.Clone(cp.SHA256State)
	return nil
}

func (r *inlineReader) Freeze(x.Deadline) (x.ReaderState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := r.confirmed
	cp.SHA256State = bytes.Clone(cp.SHA256State)
	return x.ReaderState{SinkCheckpoint: cp}, nil
}

func (*inlineReader) Stop(x.Deadline) error { return nil }

// makeCourier binds the deciding broker's canonical bytes before the courier
// can accept the matching history receipt (xfercourier.BindCanonical). The
// bytes arrive with the broker's terminal reply on this same transport.
// origin: hot-update S7 plan §4 / §5.1 / T16
func (r *Runtime) makeCourier(cfg x.CourierConfig) (x.Courier, error) {
	wrapped := &courierTransport{Transport: cfg.Transport, rt: r}
	cfg.Transport = wrapped
	courier, err := xfercourier.New(cfg)
	if err != nil {
		return nil, err
	}
	wrapped.courier = courier
	return courier, nil
}

type courierTransport struct {
	x.Transport
	rt      *Runtime
	courier x.Courier
}

func (t *courierTransport) Request(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	resp, attempt, err := t.Transport.Request(req, d)
	if err == nil && t.courier != nil {
		if record, ok := t.rt.takeCanonical(req.Identity); ok {
			if bindErr := xfercourier.BindCanonical(t.courier, record); bindErr != nil {
				return x.Response{}, attempt, bindErr
			}
		}
	}
	return resp, attempt, err
}

// The data attachment belongs to the entry's byte owner. Stopping that owner
// is the moment its dedicated connection can close; never earlier, because a
// stopped-with-error writer may still own a sent publish to fence.
// dataReader owns the entry's reader on its data attachment. A rollback that
// follows a successful reader Freeze (which retired the consumer) resumes a
// fresh reader from the synced checkpoint; the attachment stays the same.
type dataReader struct {
	rt       *Runtime
	id       x.Identity
	js       jetstream.JetStream
	spec     x.ObjectSpec
	mu       sync.Mutex
	inner    x.ObjectReader
	frozen   bool
	retained *x.ReaderState
}

func (r *dataReader) current() x.ObjectReader {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inner
}

func (r *dataReader) ReadChunk(d x.Deadline) (x.Chunk, error) {
	r.mu.Lock()
	if r.inner == nil {
		js, err := r.rt.OpenData(r.id, d)
		if err != nil {
			r.mu.Unlock()
			return x.Chunk{}, err
		}
		reader, err := jsstream.NewTransferReader(js, r.spec, r.retained.SinkCheckpoint)
		if err != nil {
			r.mu.Unlock()
			return x.Chunk{}, err
		}
		r.inner, r.js, r.retained = reader, js, nil
	}
	inner := r.inner
	r.mu.Unlock()
	return inner.ReadChunk(d)
}
func (r *dataReader) ConfirmSynced(c x.SinkCheckpoint) error { return r.current().ConfirmSynced(c) }

func (r *dataReader) Freeze(d x.Deadline) (x.ReaderState, error) {
	r.mu.Lock()
	if r.inner == nil && r.retained != nil {
		state := *r.retained
		r.frozen = true
		r.mu.Unlock()
		return state, nil
	}
	r.mu.Unlock()
	state, err := r.current().Freeze(d)
	if err == nil {
		// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 2
		// Freeze joined the fetch; Stop releases the inner reader reservation.
		// Keep the attachment so rollback reopens at this exact synced prefix.
		err = r.current().Stop(d)
	}
	if err == nil {
		r.mu.Lock()
		r.frozen = true
		r.mu.Unlock()
	}
	return state, err
}

func (r *dataReader) reopen(checkpoint x.SinkCheckpoint, d x.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.frozen {
		return nil // the reader never left its admission
	}
	if r.inner == nil && r.retained != nil {
		r.retained.SinkCheckpoint, r.frozen = checkpoint, false
		return nil
	}
	next, err := jsstream.NewTransferReader(r.js, r.spec, checkpoint)
	if err != nil {
		return err
	}
	if err = r.inner.Stop(d); err != nil {
		return errors.Join(err, next.Stop(d))
	}
	r.inner, r.frozen = next, false
	return nil
}

func (r *dataReader) Stop(d x.Deadline) error {
	if inner := r.current(); inner != nil {
		if err := inner.Stop(d); err != nil {
			return err
		}
	}
	return r.rt.CloseData(r.id, d)
}
