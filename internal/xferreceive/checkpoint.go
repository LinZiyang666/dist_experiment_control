package xferreceive

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

func emptyCheckpoint() (x.SinkCheckpoint, error) {
	state, err := marshalCheckpointHash(sha256.New())
	return x.SinkCheckpoint{SHA256State: state}, err
}

// The checkpoint carries SHA-256's own marshaled state. A hash that cannot
// export it is refused like any other unrepresentable checkpoint.
func marshalCheckpointHash(h hash.Hash) ([]byte, error) {
	marshaler, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, receiveError(x.CodeObjectConflict, "checkpoint hash is not serializable")
	}
	return marshaler.MarshalBinary()
}

func checkpointHash(cp x.SinkCheckpoint) (hash.Hash, error) {
	h := sha256.New()
	if cp.DurableOffset < 0 || len(cp.SHA256State) < 8 {
		return nil, receiveError(x.CodeObjectConflict, "invalid checkpoint hash")
	}
	unmarshaler, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		return nil, receiveError(x.CodeObjectConflict, "checkpoint hash is not restorable")
	}
	if err := unmarshaler.UnmarshalBinary(cp.SHA256State); err != nil {
		return nil, err
	}
	// Go's SHA-256 marshaled state ends in its big-endian byte count. Bind it
	// to the durable offset, so a valid hash of a longer prefix cannot pass
	// import simply because UnmarshalBinary accepts its encoding.
	// origin: hot-update S7 plan §3 / T08
	if binary.BigEndian.Uint64(cp.SHA256State[len(cp.SHA256State)-8:]) != uint64(cp.DurableOffset) {
		return nil, receiveError(x.CodeObjectConflict, "checkpoint hash is ahead of the sink")
	}
	return h, nil
}

func checkpointDigest(cp x.SinkCheckpoint) (string, error) {
	h, err := checkpointHash(cp)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func extendCheckpoint(cp x.SinkCheckpoint, chunk x.Chunk, inline bool) (x.SinkCheckpoint, error) {
	h, err := checkpointHash(cp)
	if err != nil {
		return x.SinkCheckpoint{}, err
	}
	_, _ = h.Write(chunk.Data)
	state, err := marshalCheckpointHash(h)
	if err != nil {
		return x.SinkCheckpoint{}, err
	}
	next := x.SinkCheckpoint{ChunkIndex: cp.ChunkIndex, DurableOffset: cp.DurableOffset + int64(len(chunk.Data)),
		LastConsumedStreamSeq: chunk.Receipt.StreamSeq, SHA256State: state}
	if !inline {
		next.ChunkIndex++
	}
	return next, nil
}

func copyCheckpoint(cp x.SinkCheckpoint) x.SinkCheckpoint {
	cp.SHA256State = bytes.Clone(cp.SHA256State)
	return cp
}

func sameCheckpoint(a, b x.SinkCheckpoint) bool {
	return a.ChunkIndex == b.ChunkIndex && a.DurableOffset == b.DurableOffset &&
		a.LastConsumedStreamSeq == b.LastConsumedStreamSeq && bytes.Equal(a.SHA256State, b.SHA256State)
}

func (r *receiver) remaining(deadline x.Deadline) (time.Duration, error) {
	boot, now, err := r.config.Clock()
	if err != nil {
		return 0, err
	}
	if boot != r.state.ByteDeadline.BootID || now <= 0 || deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	if now >= int64(deadline) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func (r *receiver) byteDeadline(deadline x.Deadline) (x.Deadline, error) {
	if deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	if deadline > r.state.ByteDeadline.Deadline {
		deadline = r.state.ByteDeadline.Deadline
	}
	_, err := r.remaining(deadline)
	return deadline, err
}

func (r *receiver) checkWriteAdmission(deadline x.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping || r.state.GateClosed || r.state.Stopped {
		return receiveError(x.CodeCanceled, "receiver gate closed")
	}
	if r.freezing || r.frozen {
		return receiveError(x.CodeOwnerRecovering, "receiver frozen")
	}
	_, err := r.remaining(deadline)
	return err
}

// takeOperation waits only for the existing synchronous owner. It never calls
// Stop concurrently with WriteAt/Sync/Commit, and cannot close their original
// files merely because a cut's deadline elapsed.
func (r *receiver) takeOperation(deadline x.Deadline) error {
	for {
		remaining, err := r.remaining(deadline)
		if err != nil {
			return err
		}
		r.mu.Lock()
		busy := r.busy
		if busy == nil {
			r.busy = make(chan struct{})
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-busy:
			timer.Stop()
		case <-timer.C:
			return controlbudget.ErrExpired
		}
	}
}

func (r *receiver) Freeze(deadline x.Deadline) (state x.ReceiverState, err error) {
	r.mu.Lock()
	if r.freezing || r.stopping || r.state.Stopped {
		r.mu.Unlock()
		return state, receiveError(x.CodeOwnerRecovering, "receiver cannot freeze during lifecycle work")
	}
	wasFrozen := r.frozen
	r.freezing = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.freezing = false
		r.frozen = err == nil || wasFrozen
		r.mu.Unlock()
	}()
	if err = r.takeOperation(deadline); err != nil {
		return state, err
	}
	defer r.endOperation()
	r.mu.Lock()
	inflight := r.state.Outcome.Outcome == x.OutcomeInflight
	r.mu.Unlock()
	// A returned but unresolved syscall is just as unrepresentable as a
	// blocked one. Keep its original owner instead of exporting "not started".
	// origin: hot-update S7 plan §3 / §4 / T08
	if inflight {
		return state, receiveError(x.CodeCommitInflight, "receiver primitive is unresolved")
	}
	if r.confirm {
		if err = r.confirmCheckpoint(); err != nil {
			return state, err
		}
	}
	// Atomic.Freeze is the old filesystem owner's truncate-and-Sync boundary.
	// ReceiveConfig exposes no truncate primitive; mismatched evidence must
	// defer this cut, never be patched during pristine import.
	// origin: hot-update S7 plan §3 / T08
	atomic, _, err := r.config.Atomic.Freeze(deadline)
	r.written = 0 // A refused atomic cut may already have removed the suffix.
	if err != nil {
		return state, err
	}
	r.mu.Lock()
	cp := copyCheckpoint(r.state.Checkpoint)
	outcome := r.state.Outcome
	r.mu.Unlock()
	if atomic.DurableOffset != cp.DurableOffset {
		return state, receiveError(x.CodeObjectConflict, "sink and checkpoint offsets disagree")
	}
	if atomic.PrimitiveOutcome.Outcome == x.OutcomeInflight ||
		(atomic.PrimitiveOutcome.Outcome == x.OutcomeLanded && outcome.Outcome != x.OutcomeLanded) ||
		(outcome.Outcome == x.OutcomeLanded && (atomic.PrimitiveOutcome.Outcome != x.OutcomeLanded || atomic.PrimitiveOutcome.Grant != outcome.Grant)) {
		return state, receiveError(x.CodeResultUnknown, "sink and receiver primitive evidence disagree")
	}
	if outcome.Outcome == x.OutcomeLanded {
		// The WRONLY sink proves the primitive; this receiver proves its bytes
		// from the synced hash. Join those facts before the image validates the
		// landed primitive, without overwriting contradictory sink evidence.
		// origin: hot-update S7 plan §3 / §8.2 / T08 / T17; INTEG-M image cut
		if (atomic.PrimitiveOutcome.Bytes != 0 && atomic.PrimitiveOutcome.Bytes != outcome.Bytes) ||
			(atomic.PrimitiveOutcome.SHA256 != "" && atomic.PrimitiveOutcome.SHA256 != outcome.SHA256) {
			return state, receiveError(x.CodeResultUnknown, "sink and receiver source evidence disagree")
		}
		atomic.PrimitiveOutcome.Bytes, atomic.PrimitiveOutcome.SHA256 = outcome.Bytes, outcome.SHA256
	}
	reader, err := r.config.Reader.Freeze(deadline)
	if err != nil {
		return state, err
	}
	if !sameCheckpoint(reader.SinkCheckpoint, cp) || !sameReaderObject(r.state.Identity, r.state.Budget.Size, reader) {
		return state, receiveError(x.CodeObjectConflict, "reader and checkpoint prefixes disagree")
	}
	if _, err = r.remaining(deadline); err != nil {
		return state, err
	}
	// An inline reader's snapshot only had to agree with the checkpoint above.
	// It owns no object, consumer or connection, so the receiver exports no
	// reader at all: ValidInlineState is the one inline shape this export, the
	// import below and the worker image all accept. Exporting the agreeing
	// snapshot itself was refused by the image, so no cut could carry an
	// active Tier A push receiver.
	// origin: hot-update S7 plan §8.1 / §8.3 / S7 FIX-TIERA2
	if r.state.Identity.ObjectVersion == "" {
		reader = x.ReaderState{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return state, receiveError(x.CodeCanceled, "receiver stopped during freeze")
	}
	r.state.Reader, r.state.Atomic = reader, atomic
	r.pending = nil
	state = r.state
	state.Checkpoint = copyCheckpoint(cp)
	state.Reader.SinkCheckpoint = copyCheckpoint(reader.SinkCheckpoint)
	if state.Identity.ObjectVersion == "" && !inlineShape(state) {
		return x.ReceiverState{}, receiveError(x.CodeObjectConflict, "inline receiver state is not its durable tmp prefix")
	}
	return state, nil
}

// ValidInlineState is the single definition of an inline (Tier A) receiver's
// exported state, shared by this receiver's Freeze/Import and the worker image
// validator. A Tier A push has no object version, so it owns no object reader,
// consumer or connection: its reader state is the zero value. Its checkpoint
// is the durable tmp prefix of one inline chunk (no chunk index or stream
// sequence), and InlineOffset and the atomic sink's offset name that same
// prefix within the sealed size. The worker entry retains the authenticated
// payload at Prepare; that payload must be exactly the sealed size, and the
// prefix hash exactly its prefix's (at EOF, therefore, the sealed digest).
// origin: hot-update S7 plan §3 / §8.1 / §8.3 / S7 FIX-TIERA2
func ValidInlineState(state x.ReceiverState, payload []byte) bool {
	if !inlineShape(state) || int64(len(payload)) != state.Budget.Size {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(payload[:state.Checkpoint.DurableOffset])
	exact, err := marshalCheckpointHash(h)
	return err == nil && bytes.Equal(exact, state.Checkpoint.SHA256State)
}

// inlineShape is the part of ValidInlineState a receiver can prove without
// owning the payload bytes: the reader-free, single-chunk prefix shape.
func inlineShape(state x.ReceiverState) bool {
	cp := state.Checkpoint
	if _, err := checkpointHash(cp); err != nil {
		return false
	}
	return state.Identity.ObjectVersion == "" && reflect.DeepEqual(state.Reader, x.ReaderState{}) && state.Budget.SourceSealed &&
		cp.ChunkIndex == 0 && cp.LastConsumedStreamSeq == 0 && cp.DurableOffset <= state.Budget.Size &&
		state.InlineOffset == cp.DurableOffset && state.Atomic.DurableOffset == cp.DurableOffset
}

// Thaw reopens only receiver admission. The worker must first thaw/rebind the
// supplied reader and atomic owners; this function neither reopens a pathname
// nor restores an earlier checkpoint. It is an additive package operation
// because the frozen Receiver interface has no Thaw method.
func Thaw(actor Receiver) error {
	r, ok := actor.(*receiver)
	if !ok {
		return receiveError(x.CodeIntentConflict, "receiver belongs to another implementation")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.busy != nil || r.freezing || r.stopping || r.state.Stopped {
		return receiveError(x.CodeOwnerRecovering, "receiver cannot thaw during lifecycle work")
	}
	r.frozen = false
	return nil
}

// Snapshot reports the live progress and primitive evidence without a
// lifecycle cut. Freeze closes admission and freezes the sink's original
// descriptors; a registry that only needs to publish status between steps
// must not leave its receiver (and atomic sink) permanently fenced.
// origin: hot-update S7 plan §3 / §4 / INTEG-D agent receiver wiring
func (r *receiver) Snapshot() x.ReceiverState {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.state
	state.Checkpoint = copyCheckpoint(r.state.Checkpoint)
	state.Reader.SinkCheckpoint = copyCheckpoint(r.state.Reader.SinkCheckpoint)
	return state
}

// Outcome supplies the original receiver's closed-gate or primitive proof to
// its worker after Stop, including a refused join. An unresolved result stays
// unresolved; neither a timeout nor zero state becomes a failed receipt.
// origin: hot-update S7 plan §4 / T08
func Outcome(actor Receiver) (x.ReceiverOutcome, error) {
	r, ok := actor.(*receiver)
	if !ok {
		return x.ReceiverOutcome{}, receiveError(x.CodeIntentConflict, "receiver belongs to another implementation")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	outcome := r.state.Outcome
	if !r.state.GateClosed || outcome.Outcome == "" || outcome.Outcome == x.OutcomeInflight {
		return outcome, receiveError(x.CodeResultUnknown, "receiver outcome is unresolved")
	}
	return outcome, nil
}

// Progress is the registry's between-steps observation: identity, the synced
// checkpoint, the gate and the primitive outcome. Unlike Freeze it is not a
// cut: it does not freeze the atomic sink (Freeze's truncate-and-Sync
// boundary) or close the reader, so stepping continues afterwards. Using
// Freeze for this left every live receiver "already fenced" after one step.
// origin: hot-update S7 plan §4 / T08 — S7 production activation
func (r *receiver) Progress() x.ReceiverState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return x.ReceiverState{Identity: r.state.Identity, Budget: r.state.Budget, Checkpoint: copyCheckpoint(r.state.Checkpoint),
		Grant: r.state.Grant, Outcome: r.state.Outcome, GateClosed: r.state.GateClosed, Stopped: r.state.Stopped, ByteDeadline: r.state.ByteDeadline}
}

// Import constructs a sleeping actor around ports already imported by their
// owners. It is pure with respect to business files and preserves the original
// boot deadline, even when only directory durability remains outstanding.
func Import(config Config, state x.ReceiverState) (Receiver, error) {
	if config.Clock == nil || config.Reader == nil || config.Atomic == nil || !validIdentity(state.Identity) ||
		config.Identity != state.Identity || config.Budget != state.Budget || state.ByteDeadline.Deadline <= 0 ||
		!state.Budget.SourceSealed || state.Budget.Size < 0 || state.Budget.RemainingNS <= 0 ||
		state.Budget.RemainingNS > state.Budget.EffectiveBudgetNS || (state.Stopped && !state.GateClosed) {
		return nil, receiveError(x.CodeIntentConflict, "receiver import identity or budget mismatch")
	}
	boot, _, err := config.Clock()
	if err != nil {
		return nil, err
	}
	if boot == "" || boot != state.ByteDeadline.BootID {
		return nil, receiveError(x.CodeIntentConflict, "receiver import boot mismatch")
	}
	cp := state.Checkpoint
	if _, err := checkpointHash(cp); err != nil {
		return nil, err
	}
	// An inline receiver imports exactly the reader-free shape its Freeze
	// exported (ValidInlineState); an object receiver's reader must still
	// name the same synced prefix of the same object.
	// origin: hot-update S7 plan §8.1 / §8.3 / S7 FIX-TIERA2
	inline := state.Identity.ObjectVersion == ""
	if cp.DurableOffset > state.Budget.Size || state.Atomic.DurableOffset != cp.DurableOffset ||
		(inline && !inlineShape(state)) ||
		(!inline && (!sameCheckpoint(cp, state.Reader.SinkCheckpoint) || !sameReaderObject(state.Identity, state.Budget.Size, state.Reader) ||
			state.InlineOffset != 0 || (cp.DurableOffset == 0) != (cp.ChunkIndex == 0) || (cp.ChunkIndex == 0) != (cp.LastConsumedStreamSeq == 0))) {
		return nil, receiveError(x.CodeObjectConflict, "receiver import prefix mismatch")
	}
	if state.Grant != (x.CommitGrant{}) && !validGrant(state.Identity, state.Grant) {
		return nil, receiveError(x.CodeIntentConflict, "receiver import grant mismatch")
	}
	if (state.Outcome.Outcome != "" && (!state.GateClosed || state.Outcome.Grant != state.Grant)) ||
		(state.Outcome.Outcome == "" && state.Outcome != (x.ReceiverOutcome{})) ||
		(state.Outcome.Outcome == x.OutcomeInflight) ||
		(state.Outcome.Outcome == x.OutcomeLanded && !validGrant(state.Identity, state.Grant)) ||
		(state.GateClosed && state.Outcome.Outcome != x.OutcomeNotEntered && state.Outcome.Outcome != x.OutcomeLanded) {
		return nil, receiveError(x.CodeResultUnknown, "receiver import lacks a resolved primitive boundary")
	}
	// The frozen AtomicState cannot carry the sink's object binding; repeat the
	// owner declaration on the imported port (idempotent for a retained Grant).
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
	if err := bindSink(config); err != nil {
		return nil, err
	}
	state.Checkpoint = copyCheckpoint(cp)
	state.Reader.SinkCheckpoint = copyCheckpoint(state.Reader.SinkCheckpoint)
	return &receiver{config: config, state: state, frozen: true}, nil
}

func sameReaderObject(id x.Identity, size int64, reader x.ReaderState) bool {
	// Inline adapters have no NUID or object metadata. A tier-B checkpoint
	// must stay attached to its original object, including on rollback.
	return id.ObjectVersion == "" || (reader.ObjectSpec.Identity == id && reader.ObjectSpec.Size == size)
}

func (r *receiver) Stop(deadline x.Deadline) error {
	r.mu.Lock()
	if r.state.Stopped {
		r.mu.Unlock()
		return nil
	}
	// This remains closed even when joining a filesystem syscall times out.
	// A later grant or Step must not make a failed/canceled receiver land.
	// origin: hot-update S7 plan §4 / T08
	r.stopping, r.state.GateClosed = true, true
	r.mu.Unlock()
	// origin: S7 internal review rv-lifecycle F4
	// Interrupt the network reader before joining Step; the atomic files stay
	// owned until that Step (including any WriteAt/Sync/Commit) has returned.
	readerErr := r.config.Reader.Stop(deadline)
	if err := r.takeOperation(deadline); err != nil {
		return err
	}
	defer r.endOperation()
	r.mu.Lock()
	if r.state.Outcome.Outcome == "" {
		r.state.Outcome = x.ReceiverOutcome{Grant: r.state.Grant, Outcome: x.OutcomeNotEntered,
			Bytes: r.state.Checkpoint.DurableOffset, Code: x.CodeCanceled}
	}
	r.mu.Unlock()
	// Both owners retain their cleanup on error. Retrying Stop may therefore
	// finish cleanup, but can never turn cancellation back into admission.
	err := errors.Join(readerErr, r.config.Atomic.Stop(deadline))
	if err == nil {
		r.mu.Lock()
		r.state.Stopped = true
		r.pending = nil
		r.mu.Unlock()
	}
	return err
}

func (r *receiver) observeSync(synced bool, cp x.SinkCheckpoint) {
	if r.config.Observe == nil {
		return
	}
	boot, now, _ := r.config.Clock()
	r.mu.Lock()
	id, durable, original := r.state.Identity, copyCheckpoint(r.state.Checkpoint), r.state.ByteDeadline
	r.mu.Unlock()
	digest := sha256.Sum256(durable.SHA256State)
	r.config.Observe(x.Observation{Kind: x.ObserveSynced, BootID: boot, BoottimeNS: now,
		TransferID: id.TransferID, EntryNonce: id.EntryNonce, ObjectVersion: id.ObjectVersion,
		Phase: x.PhaseReceiving, FetchedOffset: cp.DurableOffset, DurableOffset: durable.DurableOffset,
		Index: durable.ChunkIndex, Offset: durable.DurableOffset, LastConsumedStreamSeq: durable.LastConsumedStreamSeq,
		HashStateDigest: hex.EncodeToString(digest[:]), FileSynced: synced, OriginalDeadline: original})
}

// Ports borrows the receiver's immutable child bindings for current-owner
// rollback. The worker must already have fenced and joined all actor calls.
// origin: S7 internal review rv-recovery F01
func Ports(actor Receiver) (x.ObjectReader, x.AtomicPort, error) {
	r, ok := actor.(*receiver)
	if !ok {
		return nil, nil, receiveError(x.CodeIntentConflict, "receiver belongs to another implementation")
	}
	return r.config.Reader, r.config.Atomic, nil
}
