package xferreceive

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A receiver owns one synchronous IO operation at a time. The operation's
// caller remains the owner of a blocked filesystem syscall; a cut can time out
// without detaching that syscall into an untracked background goroutine.
// origin: hot-update S7 plan §3 / §4 / T08
type receiver struct {
	mu       sync.Mutex
	config   Config
	state    x.ReceiverState
	busy     chan struct{}
	freezing bool
	frozen   bool
	stopping bool
	pending  *x.Chunk
	written  int
	confirm  bool
	eof      bool
}

var _ Factory = New

// New takes ownership of already admitted reader/sink ports. The registry
// reserves their buffers and FD closure before construction. No transport
// context is retained: replacement of a control attachment cannot cancel IO.
// For inline transfers the worker supplies an in-memory ObjectReader adapter;
// the frozen ReceiveConfig has no separate inline payload or digest field.
func New(config Config) (Receiver, error) {
	if config.Clock == nil || config.Reader == nil || config.Atomic == nil || !validIdentity(config.Identity) {
		return nil, receiveError(x.CodeIntentConflict, "incomplete receiver configuration")
	}
	boot, now, err := config.Clock()
	if err != nil {
		return nil, err
	}
	budget := config.Budget
	if boot == "" || now <= 0 || !budget.SourceSealed || budget.Size < 0 ||
		budget.RemainingNS <= 0 || budget.RemainingNS > budget.EffectiveBudgetNS || budget.RemainingNS > math.MaxInt64-now {
		return nil, receiveError(x.CodeBudgetExceeded, "invalid receiver budget")
	}
	checkpoint, err := emptyCheckpoint()
	if err != nil {
		return nil, err
	}
	if err := bindSink(config); err != nil {
		return nil, err
	}
	return &receiver{config: config, state: x.ReceiverState{
		Identity: config.Identity, Budget: budget, Checkpoint: checkpoint,
		ByteDeadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(now + budget.RemainingNS)},
	}}, nil
}

// bindSink: the receiver actor is the owner that knows the transfer identity.
// A sink supporting an object binding (the real atomic receiver, fresh or
// imported) is bound here, so an empty Tier A version can never authorize a
// Tier B primitive and a Tier A inline receiver is not refused as unbound.
// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
func bindSink(config Config) error {
	if binder, ok := config.Atomic.(interface{ BindObjectVersion(string) error }); ok {
		if err := binder.BindObjectVersion(config.Identity.ObjectVersion); err != nil {
			return receiveError(x.CodeObjectVersionMismatch, "receiver sink object binding")
		}
	}
	return nil
}

func validIdentity(id x.Identity) bool {
	return id.SID != "" && id.NID != "" && id.ActorFP != "" && id.AgentInstance != "" &&
		id.TransferID != "" && id.EntryNonce != "" && id.IntentDigest != ""
}

func receiveError(code, detail string) error { return fmt.Errorf("%s: %s", code, detail) }

func validGrant(id x.Identity, grant x.CommitGrant) bool {
	terminal := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	return grant.Identity == id && grant.GrantNonce != "" && grant.ReceiverInstance != "" &&
		grant.TerminalID == hex.EncodeToString(terminal[:])
}

func (r *receiver) Reserve(grant x.CommitGrant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// The first valid grant is immutable, including across lost replies. A
	// cancellation that won admission cannot be undone by its delayed reply.
	// origin: hot-update S7 plan §4 / T08
	if !validGrant(r.state.Identity, grant) {
		return receiveError(x.CodeIntentConflict, "invalid receiver grant")
	}
	// A replayed Grant is not new authority after the original gate stopped.
	// origin: hot-update S7 plan §4 / T12 / INTEG-E real receiver regression
	if r.stopping || r.state.Stopped {
		return receiveError(x.CodeCanceled, "receiver gate closed")
	}
	if r.state.Grant != (x.CommitGrant{}) {
		if r.state.Grant != grant {
			return receiveError(x.CodeTerminalConflict, "receiver grant changed")
		}
		return nil
	}
	if r.state.GateClosed || r.stopping || r.state.Stopped {
		return receiveError(x.CodeCanceled, "receiver gate closed")
	}
	if r.freezing || r.frozen {
		return receiveError(x.CodeOwnerRecovering, "receiver frozen")
	}
	r.state.Grant = grant
	return nil
}

func (r *receiver) Step(deadline x.Deadline) error {
	if err := r.beginStep(); err != nil {
		return err
	}
	defer r.endOperation()
	// Directory durability is settlement of an effect that already happened.
	// It must remain retryable after the original byte deadline expires.
	r.mu.Lock()
	outcome := r.state.Outcome
	r.mu.Unlock()
	if outcome.Outcome == x.OutcomeLanded || outcome.Outcome == x.OutcomeInflight {
		return r.commit(deadline)
	}
	deadline, err := r.byteDeadline(deadline)
	if err != nil {
		return err
	}
	if r.confirm {
		if err := r.confirmCheckpoint(); err != nil {
			return err
		}
	}
	if r.eof {
		return r.commit(deadline)
	}
	if r.pending == nil {
		chunk, readErr := r.config.Reader.ReadChunk(deadline)
		if errors.Is(readErr, io.EOF) {
			if len(chunk.Data) != 0 {
				return r.failBytes("reader returned bytes with EOF")
			}
			r.mu.Lock()
			complete := r.state.Checkpoint.DurableOffset == r.state.Budget.Size
			r.mu.Unlock()
			if !complete {
				return r.failBytes("reader ended before the sealed size")
			}
			// No data chunk exists to Sync an empty tmp. Its inode still needs
			// durability before rename, just like a nonempty destination.
			// origin: hot-update S7 plan §3 / T08
			if r.state.Budget.Size == 0 {
				if err := r.checkWriteAdmission(deadline); err != nil {
					return err
				}
				r.observeSync(false, r.state.Checkpoint)
				if err := r.config.Atomic.Sync(); err != nil {
					return err
				}
				r.observeSync(true, r.state.Checkpoint)
			}
			r.eof = true
			return r.commit(deadline)
		}
		if readErr != nil {
			return readErr
		}
		if err := r.acceptChunk(chunk); err != nil {
			return err
		}
	}
	return r.writeChunk(deadline)
}

func (r *receiver) beginStep() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping || r.state.Stopped || (r.state.GateClosed && r.state.Outcome.Outcome == x.OutcomeNotEntered) {
		return receiveError(x.CodeCanceled, "receiver gate closed")
	}
	if r.freezing || r.frozen || r.busy != nil {
		return receiveError(x.CodeOwnerRecovering, "receiver operation already fenced")
	}
	r.busy = make(chan struct{})
	return nil
}

func (r *receiver) endOperation() {
	r.mu.Lock()
	close(r.busy)
	r.busy = nil
	r.mu.Unlock()
}

func (r *receiver) acceptChunk(chunk x.Chunk) error {
	r.mu.Lock()
	cp, size := r.state.Checkpoint, r.state.Budget.Size
	inline := r.state.Identity.ObjectVersion == ""
	r.mu.Unlock()
	limit := 1 << 20
	if inline {
		limit = 8 << 20
	}
	// Global stream gaps are legal. Logical chunk gaps, rewinds, changed bytes
	// and a forged requested length must fail before even the first WriteAt.
	// origin: hot-update S7 plan §3 / T08
	if len(chunk.Data) == 0 || len(chunk.Data) > limit || chunk.Receipt.Length != uint32(len(chunk.Data)) ||
		chunk.Receipt.Offset != cp.DurableOffset || int64(len(chunk.Data)) > size-cp.DurableOffset ||
		chunk.Receipt.Index != cp.ChunkIndex || chunk.Receipt.SHA256 != sha256.Sum256(chunk.Data) ||
		(!inline && chunk.Receipt.StreamSeq <= cp.LastConsumedStreamSeq) || (inline && chunk.Receipt.StreamSeq != 0) {
		return r.failBytes("chunk does not extend the synced prefix")
	}
	r.pending, r.written = &chunk, 0
	return nil
}

func (r *receiver) writeChunk(deadline x.Deadline) error {
	chunk := r.pending
	for r.written < len(chunk.Data) {
		if err := r.checkWriteAdmission(deadline); err != nil {
			return err
		}
		n, err := r.config.Atomic.WriteAt(chunk.Data[r.written:], chunk.Receipt.Offset+int64(r.written))
		if n < 0 || n > len(chunk.Data)-r.written {
			return r.failBytes("sink returned an invalid write count")
		}
		// Even an error can have written bytes. Retaining n avoids resending
		// that suffix in this owner; it is still NOT durable checkpoint state.
		// origin: hot-update S7 plan §3 / T08
		r.written += n
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	if err := r.checkWriteAdmission(deadline); err != nil {
		return err
	}
	// Compute the candidate on a separate hash. Failed or blocked Sync must
	// leave offset, index, sequence and hash at the same previous prefix.
	checkpoint, err := extendCheckpoint(r.state.Checkpoint, *chunk, r.state.Identity.ObjectVersion == "")
	if err != nil {
		return err
	}
	r.observeSync(false, checkpoint)
	if err := r.config.Atomic.Sync(); err != nil {
		return err
	}
	r.mu.Lock()
	r.state.Checkpoint = checkpoint
	if r.state.Identity.ObjectVersion == "" {
		r.state.InlineOffset = checkpoint.DurableOffset
	}
	r.confirm = true
	r.pending, r.written = nil, 0
	r.mu.Unlock()
	r.observeSync(true, checkpoint)
	return r.confirmCheckpoint()
}

func (r *receiver) confirmCheckpoint() error {
	r.mu.Lock()
	cp := copyCheckpoint(r.state.Checkpoint)
	r.mu.Unlock()
	if err := r.config.Reader.ConfirmSynced(cp); err != nil {
		return err
	}
	r.confirm = false
	return nil
}

func (r *receiver) failBytes(detail string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.GateClosed = true
	r.state.Outcome = x.ReceiverOutcome{Grant: r.state.Grant, Outcome: x.OutcomeNotEntered,
		Bytes: r.state.Checkpoint.DurableOffset, Code: x.CodeObjectConflict, Error: detail}
	return receiveError(x.CodeObjectConflict, detail)
}

func (r *receiver) commit(deadline x.Deadline) error {
	if _, err := r.remaining(deadline); err != nil {
		return err
	}
	r.mu.Lock()
	previous := r.state.Outcome
	if previous.Outcome == x.OutcomeLanded && previous.ParentSynced && !previous.TmpCleanupPending {
		r.mu.Unlock()
		return io.EOF
	}
	if r.stopping || r.state.Stopped || previous.Outcome == x.OutcomeNotEntered {
		r.mu.Unlock()
		return receiveError(x.CodeCanceled, "receiver gate closed")
	}
	if r.freezing || r.frozen {
		r.mu.Unlock()
		return receiveError(x.CodeOwnerRecovering, "receiver frozen")
	}
	grant := r.state.Grant
	if !validGrant(r.state.Identity, grant) {
		r.mu.Unlock()
		return receiveError(x.CodeCommitInflight, "receiver awaits commit grant")
	}
	// Cancellation and primitive entry share this latch. Stop can request a
	// join after this point, but can never invent a no-syscall failure.
	// origin: hot-update S7 plan §4 / T08
	r.state.GateClosed = true
	r.state.Outcome = x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeInflight}
	cp := copyCheckpoint(r.state.Checkpoint)
	r.mu.Unlock()
	outcome := r.config.Atomic.Commit(grant, deadline)
	digest, digestErr := checkpointDigest(cp)
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome.Grant != grant || (outcome.Outcome != x.OutcomeNotEntered && outcome.Outcome != x.OutcomeInflight && outcome.Outcome != x.OutcomeLanded) ||
		(previous.Outcome == x.OutcomeLanded && outcome.Outcome != x.OutcomeLanded) || digestErr != nil ||
		(outcome.Bytes != 0 && outcome.Bytes != cp.DurableOffset) || (outcome.SHA256 != "" && outcome.SHA256 != digest) {
		// Keep the strongest prior evidence when a port cannot resolve its
		// result. In particular, landed can never turn back into failed.
		if previous.Outcome == x.OutcomeLanded {
			r.state.Outcome = previous
		}
		return receiveError(x.CodeResultUnknown, "sink returned inconsistent primitive evidence")
	}
	outcome.Bytes = cp.DurableOffset
	outcome.SHA256 = digest
	r.state.Outcome = outcome
	if outcome.Outcome == x.OutcomeLanded && outcome.ParentSynced && !outcome.TmpCleanupPending {
		return io.EOF
	}
	if outcome.Code != "" {
		return receiveError(outcome.Code, "receiver primitive requires settlement")
	}
	return receiveError(x.CodeResultUnknown, "receiver primitive requires settlement")
}
