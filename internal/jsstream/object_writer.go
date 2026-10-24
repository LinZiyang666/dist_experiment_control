package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"hash"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Deliberately exclude ObjectStore from the capable implementation's port.
// There is no Put closure or ID generator with which to restart an upload.
// origin: hot-update S7 plan §2.2 / §7 / T01
type objectWriterJS interface {
	Stream(context.Context, string) (jetstream.Stream, error)
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type chunkReadback struct {
	raw *jetstream.RawStreamMsg
	err error
}

type objectWriter struct {
	js         objectWriterJS
	maxPayload func() int64
	gate       chan struct{}
	closed     atomic.Bool
	stopping   atomic.Bool
	frozen     bool
	state      x.WriterState
	stream     jetstream.Stream
	lastSeq    uint64
	digest     hash.Hash
	observe    func(x.Observation)
}

// NewTransferWriter binds the version minted by prepare. Admission does no IO;
// the first deadline-bearing operation checks the actual storage incarnation.
func NewTransferWriter(js jetstream.JetStream, spec x.ObjectSpec) (Writer, error) {
	w, err := newObjectWriter(js, spec)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// newObjectWriter returns the concrete owner so ImportTransferWriter can
// install retained state without asserting on the public port.
func newObjectWriter(js jetstream.JetStream, spec x.ObjectSpec) (*objectWriter, error) {
	if js == nil || js.Conn() == nil || js.Conn().Opts.ReconnectBufSize != -1 {
		return nil, fmt.Errorf("%s: dedicated connection must disable reconnect buffering", x.CodePayloadUnsupported)
	}
	if err := validateObjectSpec(spec); err != nil {
		return nil, err
	}
	w := &objectWriter{js: js, maxPayload: js.Conn().MaxPayload, gate: make(chan struct{}, 1), digest: sha256.New()}
	w.state = x.WriterState{ObjectSpec: spec, Phase: x.WriteOpen, AttachmentGeneration: 1,
		Receipts: make([]x.ChunkReceipt, 0, spec.Chunks)}
	return w, nil
}

// ImportTransferWriter is pure: retained bytes and receipts are cloned before
// any network access. Thaw is explicit so a pristine import can be re-exported.
// origin: hot-update S7 plan §8.2 / T01
func ImportTransferWriter(js jetstream.JetStream, state x.WriterState) (Writer, error) {
	w, err := newObjectWriter(js, state.ObjectSpec)
	if err != nil {
		return nil, err
	}
	if err = validateWriterState(state); err != nil {
		return nil, err
	}
	w.state = cloneWriterState(state)
	w.stopping.Store(state.RetirementFence.WriterRetired)
	w.frozen = true
	w.closed.Store(true)
	return w, nil
}

// ThawTransferWriter never replaces an attempt's attachment generation or
// headers. New attempts belong to the successor; retained attempts stay exact.
func ThawTransferWriter(port Writer, deadline controlbudget.Deadline) error {
	w, ok := port.(*objectWriter)
	if !ok {
		return errors.New("transfer writer: foreign implementation")
	}
	_, release, err := lockWriter(w, deadline)
	if err != nil {
		return err
	}
	defer release()
	if w.stopping.Load() || w.state.RetirementFence.WriterRetired {
		return fmt.Errorf("%s: writer retired", x.CodeCanceled)
	}
	// A timed-out Freeze already closed admission but may not have acquired
	// the IO gate. Rollback must reopen that retained owner after joining it.
	// origin: hot-update S7 plan §8.2 / T01
	if w.frozen || w.closed.Load() {
		w.state.AttachmentGeneration++
		w.frozen = false
		w.closed.Store(false)
	}
	return nil
}

// SetTransferWriterObserver installs an owner-scoped sink, never a process
// global hook. The sink must not call back into the writer while recording.
func SetTransferWriterObserver(port Writer, observe func(x.Observation), deadline controlbudget.Deadline) error {
	w, ok := port.(*objectWriter)
	if !ok {
		return errors.New("transfer writer: foreign implementation")
	}
	_, release, err := lockWriter(w, deadline)
	if err != nil {
		return err
	}
	defer release()
	w.observe = observe
	return nil
}

func writerContext(deadline controlbudget.Deadline) (context.Context, context.CancelFunc, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	// ctx-none: the frozen data-plane port supplies an absolute BOOTTIME deadline.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	return ctx, cancel, nil
}

// lockWriter joins the operation under its absolute deadline. Its single
// release unwinds in the same order as the former paired defers: unlock, then
// cancel. Failed acquisition also cancels the bounded context immediately.
// origin: hot-update S7 plan §11.1 / S7 internal review rv-jsnats F2
func lockWriter(w *objectWriter, deadline controlbudget.Deadline) (context.Context, func(), error) {
	ctx, cancel, err := writerContext(deadline)
	if err != nil {
		return nil, nil, err
	}
	if err = w.lock(ctx); err != nil {
		cancel()
		return nil, nil, err
	}
	return ctx, func() {
		w.unlock()
		cancel()
	}, nil
}

func (w *objectWriter) lock(ctx context.Context) error {
	select {
	case w.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			w.unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (w *objectWriter) unlock() { <-w.gate }

func (w *objectWriter) ready(ctx context.Context) error {
	if w.stopping.Load() || w.closed.Load() {
		return fmt.Errorf("%s: writer admission closed", x.CodeCanceled)
	}
	if w.stream != nil {
		// Reusing the coordinator's first-byte seal hid an N-1 full-config
		// writer restoring eviction after an ACK. Check the actual incarnation
		// and retention before every new publish, pending resolution and metadata.
		// A third synchronous RTT breaks T09's single-pending 1 MiB geometry
		// at 100 ms WSS RTT; publishPending overlaps only read-only readback.
		// Operator maintenance must still avoid live transfers (§2.1).
		// origin: S7 internal review TRANSFER-MAKETEST-2 F2 / S7 plan §2.1 / T09
		info, err := w.stream.Info(ctx)
		if err != nil {
			return err
		}
		return checkWriterStream(w.state, info)
	}
	stream, err := w.js.Stream(ctx, w.state.ObjectSpec.Stream)
	if err != nil {
		return err
	}
	info := stream.CachedInfo()
	if err = checkWriterStream(w.state, info); err != nil {
		return err
	}
	w.stream, w.lastSeq = stream, info.State.LastSeq
	// A restored digest is derived from exact original receipts, not a source
	// pathname or untrusted stream-wide sequence used as a byte cursor.
	w.digest.Reset()
	for _, r := range w.state.Receipts {
		raw, readErr := stream.GetMsg(ctx, r.StreamSeq)
		if readErr != nil {
			w.stream = nil
			return readErr
		}
		if readErr = checkChunk(w.state.ObjectSpec, r, raw); readErr != nil {
			w.stream = nil
			return readErr
		}
		_, _ = w.digest.Write(raw.Data)
	}
	return nil
}

func (w *objectWriter) Append(index uint32, data []byte, deadline controlbudget.Deadline) (x.ChunkReceipt, error) {
	ctx, release, err := lockWriter(w, deadline)
	if err != nil {
		return x.ChunkReceipt{}, err
	}
	defer release()
	if err = w.ready(ctx); err != nil {
		return x.ChunkReceipt{}, err
	}
	s := w.state.ObjectSpec
	if index >= s.Chunks || len(data) != int(chunkLength(s, index)) {
		return x.ChunkReceipt{}, fmt.Errorf("%s: append geometry changed", x.CodeSourceChanged)
	}
	r := x.ChunkReceipt{Index: index, Offset: int64(index) * int64(s.ChunkSize), Length: uint32(len(data)), SHA256: sha256.Sum256(data)}
	if int(index) < len(w.state.Receipts) {
		old := w.state.Receipts[index]
		r.StreamSeq = old.StreamSeq
		if old != r {
			return x.ChunkReceipt{}, fmt.Errorf("%s: receipt bytes changed", x.CodeSourceChanged)
		}
		return old, nil
	}
	if int(index) != len(w.state.Receipts) || w.state.MetaReceipt != nil {
		return x.ChunkReceipt{}, fmt.Errorf("%s: noncontiguous append", x.CodeObjectConflict)
	}
	if p := w.state.PendingAttempt; p != nil {
		if p.Index != index || !bytes.Equal(p.Bytes, data) {
			return x.ChunkReceipt{}, fmt.Errorf("%s: pending bytes changed", x.CodeSourceChanged)
		}
		r, _, err = w.resolve(ctx)
		return r, err
	}
	previous := uint64(0)
	if index > 0 {
		previous = w.state.Receipts[index-1].StreamSeq
	}
	w.state.PendingAttempt = &x.PendingAttempt{Index: index, Offset: r.Offset, Length: r.Length, SHA256: r.SHA256,
		Bytes: bytes.Clone(data), PreviousSubjectSeq: previous, ExpectedStreamSeq: w.lastSeq,
		AttachmentGeneration: w.state.AttachmentGeneration}
	w.state.Phase = x.WritePending
	return w.publishPending(ctx)
}

func (w *objectWriter) publishPending(ctx context.Context) (x.ChunkReceipt, error) {
	p := w.state.PendingAttempt
	msg := chunkMessage(w.state.ObjectSpec, *p)
	if err := checkWriterPayload(w.maxPayload(), w.stream.CachedInfo(), msg, w.state.ObjectSpec); err != nil {
		return x.ChunkReceipt{}, err
	}
	w.emit(x.ObserveAppend, p, 0)
	// The stream CAS fixes a successful append at ExpectedStreamSeq+1. Queue
	// its read-only lookup behind the outgoing publish, overlapping propagation
	// with the durable ACK. The short batching delay is only an optimization:
	// an early miss or a different seq always gets a fresh post-ACK read below.
	// Every lookup joins before this pending chunk can advance or be frozen.
	// origin: S7 internal review TRANSFER-MAKETEST-2 F2 / S7 plan §2.2 / T09
	read := make(chan chunkReadback, 1)
	go func() {
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		raw, err := w.stream.GetMsg(ctx, p.ExpectedStreamSeq+1)
		read <- chunkReadback{raw, err}
	}()
	ack, err := w.js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
	got := <-read
	if err != nil {
		return x.ChunkReceipt{}, err
	}
	if ack == nil || ack.Stream != w.state.ObjectSpec.Stream || ack.Sequence <= p.PreviousSubjectSeq || ack.Sequence <= p.ExpectedStreamSeq {
		return x.ChunkReceipt{}, fmt.Errorf("%s: invalid chunk ACK", x.CodeObjectConflict)
	}
	raw := got.raw
	if got.err != nil || raw == nil || raw.Sequence != ack.Sequence {
		raw, err = w.stream.GetMsg(ctx, ack.Sequence)
		if err != nil {
			return x.ChunkReceipt{}, err
		}
	}
	r := pendingReceipt(*p, ack.Sequence)
	if err = checkChunk(w.state.ObjectSpec, r, raw); err != nil {
		return x.ChunkReceipt{}, err
	}
	return w.adopt(r, raw.Data), nil
}

func (w *objectWriter) ResolvePending(deadline controlbudget.Deadline) (x.ChunkReceipt, bool, error) {
	ctx, release, err := lockWriter(w, deadline)
	if err != nil {
		return x.ChunkReceipt{}, false, err
	}
	defer release()
	if err = w.ready(ctx); err != nil {
		return x.ChunkReceipt{}, false, err
	}
	return w.resolve(ctx)
}

func (w *objectWriter) resolve(ctx context.Context) (x.ChunkReceipt, bool, error) {
	p := w.state.PendingAttempt
	if p == nil {
		return x.ChunkReceipt{}, false, nil
	}
	// A mixed-version config writer can restore DiscardOld after our first
	// ACK. Recheck the actual backing policy before another publish, so the
	// same version pauses safely until the coordinator re-applies its seal.
	// origin: hot-update S7 plan §2.1 / LQ3; S7 internal review FIX-CONVERGE F1
	info, err := w.stream.Info(ctx)
	if err != nil {
		return x.ChunkReceipt{}, false, err
	}
	if err = checkWriterStream(w.state, info); err != nil {
		return x.ChunkReceipt{}, false, err
	}
	var found *jetstream.RawStreamMsg
	for seq := p.PreviousSubjectSeq + 1; ; {
		raw, readErr := w.stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(chunkSubject(w.state.ObjectSpec)))
		if errors.Is(readErr, jetstream.ErrMsgNotFound) {
			break
		}
		if readErr != nil {
			return x.ChunkReceipt{}, false, readErr
		}
		if found != nil || raw.Sequence < seq || raw.Sequence <= p.ExpectedStreamSeq {
			return x.ChunkReceipt{}, false, fmt.Errorf("%s: duplicate or unordered pending chunk", x.CodeObjectConflict)
		}
		if err = checkChunk(w.state.ObjectSpec, pendingReceipt(*p, raw.Sequence), raw); err != nil {
			return x.ChunkReceipt{}, false, err
		}
		found, seq = raw, raw.Sequence+1
	}
	if found != nil {
		return w.adopt(pendingReceipt(*p, found.Sequence), found.Data), true, nil
	}
	// Only a strictly advanced global fence makes a new attempt safe. An
	// unchanged fence retries the exact bytes/headers, including after a cut.
	// origin: hot-update S7 plan §2.2 / T02
	if info.State.LastSeq > p.ExpectedStreamSeq {
		next := *p
		next.ExpectedStreamSeq = info.State.LastSeq
		next.AttachmentGeneration = w.state.AttachmentGeneration
		w.state.PendingAttempt = &next
	}
	r, err := w.publishPending(ctx)
	return r, err == nil, err
}

func (w *objectWriter) adopt(r x.ChunkReceipt, data []byte) x.ChunkReceipt {
	w.emit(x.ObserveResolve, w.state.PendingAttempt, r.StreamSeq)
	w.state.Receipts = append(w.state.Receipts, r)
	w.state.PendingAttempt = nil
	w.state.Phase = x.WriteOpen
	w.lastSeq = r.StreamSeq
	_, _ = w.digest.Write(data)
	return r
}

// Snapshot joins current IO without closing byte admission. Sender prefix
// reconstruction is a read; only the lifecycle owner may perform Freeze.
// origin: hot-update S7 plan §2.2 / T19 / INTEG-E storage regression
func (w *objectWriter) Snapshot(deadline controlbudget.Deadline) (x.WriterState, error) {
	_, release, err := lockWriter(w, deadline)
	if err != nil {
		return x.WriterState{}, err
	}
	defer release()
	return cloneWriterState(w.state), nil
}

func (w *objectWriter) Freeze(deadline controlbudget.Deadline) (x.WriterState, error) {
	w.closed.Store(true)
	_, release, err := lockWriter(w, deadline)
	if err != nil {
		return x.WriterState{}, err
	}
	defer release()
	w.frozen = true
	if w.stopping.Load() {
		// Stop may have timed out before taking the gate. Freeze has now
		// joined that IO and the irreversible stop latch prevents new IO.
		w.state.RetirementFence.WriterRetired = true
	}
	return cloneWriterState(w.state), nil
}

func (w *objectWriter) Stop(deadline controlbudget.Deadline) error {
	// A cut rollback cannot reopen admission after even an incomplete Stop.
	// origin: hot-update S7 plan §2.3 / §8.2 / T04
	w.stopping.Store(true)
	w.closed.Store(true)
	_, release, err := lockWriter(w, deadline)
	if err != nil {
		return err
	}
	defer release()
	// No future attempt can pass ready; taking the gate joins admitted IO.
	// It does NOT prove already sent bytes are absent from the network.
	w.state.RetirementFence.WriterRetired = true
	return nil
}

func (w *objectWriter) emit(kind x.ObservationKind, p *x.PendingAttempt, seq uint64) {
	if w.observe == nil {
		return
	}
	id := w.state.ObjectSpec.Identity
	e := x.Observation{Kind: kind, TransferID: id.TransferID, EntryNonce: id.EntryNonce,
		ObjectVersion: id.ObjectVersion, ActualACKSeq: seq, StreamIdentity: w.state.ObjectSpec.StreamCreated}
	e.BoottimeNS, _ = controlbudget.Now()
	if kind == x.ObserveFinalize {
		e.Index, e.Offset, e.SHA256 = w.state.ObjectSpec.Chunks, w.state.ObjectSpec.Size, w.state.ObjectSpec.SHA256
		e.AttachmentGeneration = w.state.AttachmentGeneration
	}
	if p != nil {
		e.Index, e.Offset, e.Length = p.Index, p.Offset, p.Length
		e.SHA256 = hex.EncodeToString(p.SHA256[:])
		e.ExpectedSubjectSeq, e.ExpectedStreamSeq = p.PreviousSubjectSeq, p.ExpectedStreamSeq
		e.AttachmentGeneration = p.AttachmentGeneration
	}
	w.observe(e)
}

// The format/receipt helpers are shared with bucket admission. Keep these
// private names as aliases so reader and writer callers use one predicate.
// origin: hot-update S7 plan §2.1–§2.3 / S7 internal review FIX-ARCH F2
var (
	validateObjectSpec  = xferbucket.ValidateObjectSpec
	checkWriterStream   = xferbucket.CheckWriterStream
	chunkSubject        = xferbucket.ChunkSubject
	chunkLength         = xferbucket.ChunkLength
	pendingReceipt      = xferbucket.PendingReceipt
	chunkMessage        = xferbucket.ChunkMessage
	checkChunk          = xferbucket.CheckChunk
	checkWriterPayload  = xferbucket.CheckWriterPayload
	metadataSubject     = xferbucket.MetadataSubject
	objectMetadata      = xferbucket.ObjectMetadata
	metadataReceipt     = xferbucket.MetadataReceipt
	metadataMessage     = xferbucket.MetadataMessage
	cloneObjectReceipt  = xferbucket.CloneObjectReceipt
	cloneWriterState    = xferbucket.CloneWriterState
	validateWriterState = xferbucket.ValidateWriterState
	objectChunkToken    = xferbucket.ObjectChunkToken
)

// TransferPayloadLimit preserves the public entry point for the shared bucket predicate.
func TransferPayloadLimit(maxPayload int64, streamLimits ...int64) (int64, error) {
	return xferbucket.TransferPayloadLimit(maxPayload, streamLimits...)
}

// TransferChunkSize preserves the public entry point for the shared bucket predicate.
func TransferChunkSize(spec x.ObjectSpec, limits ...int64) (uint32, error) {
	return xferbucket.TransferChunkSize(spec, limits...)
}

// CheckTransferRetention preserves the public entry point for the shared bucket predicate.
func CheckTransferRetention(cfg jetstream.StreamConfig, horizon time.Duration) error {
	return xferbucket.CheckTransferRetention(cfg, horizon)
}

func (w *objectWriter) Finalize(deadline controlbudget.Deadline) (x.ObjectReceipt, error) {
	ctx, release, err := lockWriter(w, deadline)
	if err != nil {
		return x.ObjectReceipt{}, err
	}
	defer release()
	if err = w.ready(ctx); err != nil {
		return x.ObjectReceipt{}, err
	}
	s := w.state.ObjectSpec
	// A pending last chunk is not a receipt, even if its publish returned a
	// timeout after the server wrote it. Only ResolvePending may adopt it.
	// origin: hot-update S7 plan §2.2 / T03
	if w.state.PendingAttempt != nil || len(w.state.Receipts) != int(s.Chunks) {
		return x.ObjectReceipt{}, fmt.Errorf("%s: metadata requires every chunk receipt", x.CodeReconcileRequired)
	}
	if hex.EncodeToString(w.digest.Sum(nil)) != s.SHA256 {
		return x.ObjectReceipt{}, fmt.Errorf("%s: complete object digest changed", x.CodeSourceChanged)
	}
	info, err := w.stream.Info(ctx)
	if err != nil {
		return x.ObjectReceipt{}, err
	}
	if err = checkWriterStream(w.state, info); err != nil {
		return x.ObjectReceipt{}, err
	}
	data := objectMetadata(s, false)
	raw, err := exactMetadata(ctx, w.stream, s, data)
	if err != nil {
		return x.ObjectReceipt{}, err
	}
	if raw == nil {
		if w.state.MetaReceipt != nil && w.state.MetaReceipt.MetadataSeq != 0 {
			return x.ObjectReceipt{}, fmt.Errorf("%s: acknowledged metadata disappeared", x.CodeObjectVersionMismatch)
		}
		// Save immutable bytes before sending. A lost ACK never permits a
		// same-name foreign version to be mistaken for this invocation.
		w.state.MetaReceipt = &x.ObjectReceipt{Spec: s, Metadata: bytes.Clone(data)}
		w.state.Phase = x.WriteMetadataPending
		msg := metadataMessage(s, data, 0, info.State.LastSeq)
		if err = checkWriterPayload(w.maxPayload(), info, msg, s); err != nil {
			return x.ObjectReceipt{}, err
		}
		ack, pubErr := w.js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
		if pubErr != nil {
			return x.ObjectReceipt{}, pubErr
		}
		if ack == nil || ack.Stream != s.Stream || ack.Sequence <= info.State.LastSeq {
			return x.ObjectReceipt{}, fmt.Errorf("%s: invalid metadata ACK", x.CodeObjectConflict)
		}
		raw, err = exactMetadata(ctx, w.stream, s, data)
		if err != nil {
			return x.ObjectReceipt{}, err
		}
		if raw == nil || raw.Sequence != ack.Sequence {
			return x.ObjectReceipt{}, fmt.Errorf("%s: metadata ACK readback mismatch", x.CodeObjectConflict)
		}
	}
	r := metadataReceipt(s, raw)
	w.state.MetaReceipt, w.state.Phase = &r, x.WriteFinalized
	w.emit(x.ObserveFinalize, nil, r.MetadataSeq)
	return cloneObjectReceipt(r), nil
}

func exactMetadata(ctx context.Context, stream jetstream.Stream, s x.ObjectSpec, data []byte) (*jetstream.RawStreamMsg, error) {
	raw, err := stream.GetLastMsgForSubject(ctx, metadataSubject(s))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if raw.Subject != metadataSubject(s) || !bytes.Equal(raw.Data, data) {
		return nil, fmt.Errorf("%s: foreign metadata at object name", x.CodeObjectVersionMismatch)
	}
	return raw, nil
}

// RetireTransferObject is the finalizer's low-level primitive. The proof must
// come from joined owners; neither timeout nor Freeze is a retirement proof.
// State is updated before returning so a lost purge ACK retains its fence.
// origin: hot-update S7 plan §2.3 / T04
func RetireTransferObject(js jetstream.JetStream, state *x.WriterState, proof x.RetirementProof, deadline controlbudget.Deadline) (x.CleanupReceipt, error) {
	return RetireTransferObjectRecorded(js, state, proof, deadline, nil)
}

// RetireTransferObjectRecorded checkpoints the original CAS before publishing
// and the exact ACK before purge. The production finalizer durably records
// these states; a failed checkpoint cannot authorize the following effect.
// origin: hot-update S7 plan §2.3 / T04 / T21
func RetireTransferObjectRecorded(js jetstream.JetStream, state *x.WriterState, proof x.RetirementProof, deadline controlbudget.Deadline, checkpoint func(x.WriterState) error) (x.CleanupReceipt, error) {
	if state == nil || js == nil || proof.Identity != state.ObjectSpec.Identity || proof.EntryNonce != proof.Identity.EntryNonce ||
		!proof.WriterRetired || !proof.ConsumersRetired || !state.RetirementFence.WriterRetired {
		return x.CleanupReceipt{}, fmt.Errorf("%s: writer and consumer retirement proof required", x.CodeReconcileRequired)
	}
	if err := validateWriterState(*state); err != nil {
		return x.CleanupReceipt{}, err
	}
	ctx, cancel, err := writerContext(deadline)
	if err != nil {
		return x.CleanupReceipt{}, err
	}
	defer cancel()
	stream, err := js.Stream(ctx, state.ObjectSpec.Stream)
	if err != nil {
		return x.CleanupReceipt{}, err
	}
	info := stream.CachedInfo()
	if err = checkWriterStream(*state, info); err != nil {
		return x.CleanupReceipt{}, err
	}
	s := state.ObjectSpec
	state.Phase = x.WriteRetiring
	state.RetirementFence.ConsumersRetired = true
	tombstone := objectMetadata(s, true)
	raw, err := stream.GetLastMsgForSubject(ctx, metadataSubject(s))
	previous := uint64(0)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return x.CleanupReceipt{}, err
	}
	if err == nil {
		previous = raw.Sequence
		if !bytes.Equal(raw.Data, tombstone) && !bytes.Equal(raw.Data, objectMetadata(s, false)) {
			return x.CleanupReceipt{}, fmt.Errorf("%s: retirement found foreign metadata", x.CodeObjectVersionMismatch)
		}
	} else {
		raw = nil
	}
	if raw == nil || !bytes.Equal(raw.Data, tombstone) {
		// INFO cannot erase a retained original writer expectation. A reset or
		// older view must defer purge until a real ACK advances past both floors.
		// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
		state.RetirementFence.ExpectedStreamSeq = max(state.RetirementFence.ExpectedStreamSeq, info.State.LastSeq)
		state.RetirementFence.PreviousMetadataSeq = previous
		if checkpoint != nil {
			if err = checkpoint(cloneWriterState(*state)); err != nil {
				return x.CleanupReceipt{}, err
			}
		}
		msg := metadataMessage(s, tombstone, previous, info.State.LastSeq)
		ack, pubErr := js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
		if pubErr != nil {
			return x.CleanupReceipt{}, pubErr
		}
		if ack == nil || ack.Stream != s.Stream || ack.Sequence <= info.State.LastSeq {
			return x.CleanupReceipt{}, fmt.Errorf("%s: invalid retirement fence ACK", x.CodeObjectConflict)
		}
		raw, err = exactMetadata(ctx, stream, s, tombstone)
		if err != nil {
			return x.CleanupReceipt{}, err
		}
		if raw == nil || raw.Sequence != ack.Sequence {
			return x.CleanupReceipt{}, fmt.Errorf("%s: retirement ACK mismatch", x.CodeObjectConflict)
		}
	}
	floor := state.RetirementFence.ExpectedStreamSeq
	if p := state.PendingAttempt; p != nil {
		floor = max(floor, p.ExpectedStreamSeq)
	}
	if len(state.Receipts) != 0 {
		floor = max(floor, state.Receipts[len(state.Receipts)-1].StreamSeq)
	}
	if raw.Sequence <= floor {
		return x.CleanupReceipt{}, fmt.Errorf("%s: retirement fence predates attempts", x.CodeObjectConflict)
	}
	digest := sha256.Sum256(raw.Data)
	state.RetirementFence.Receipt = &x.HistoryReceipt{Stream: s.Stream, Sequence: raw.Sequence, RecordDigest: hex.EncodeToString(digest[:])}
	if checkpoint != nil {
		if err = checkpoint(cloneWriterState(*state)); err != nil {
			return x.CleanupReceipt{}, err
		}
	}
	// Filtered purge preserves LastSeq on the pinned server. Purging only the
	// original NUID also protects a foreign version racing after the readback.
	// origin: hot-update S7 plan §2.3 / T04
	if err = stream.Purge(ctx, jetstream.WithPurgeSubject(chunkSubject(s))); err != nil {
		return x.CleanupReceipt{}, err
	}
	state.RetirementFence.Purged, state.Phase = true, x.WriteRetired
	if checkpoint != nil {
		if err = checkpoint(cloneWriterState(*state)); err != nil {
			return x.CleanupReceipt{}, err
		}
	}
	return x.CleanupReceipt{Identity: s.Identity, OwnerGeneration: proof.OwnerGeneration,
		WriterRetired: true, ConsumersRetired: true, FenceSequence: raw.Sequence, ObjectPurged: true,
		Digest: hex.EncodeToString(digest[:])}, nil
}
