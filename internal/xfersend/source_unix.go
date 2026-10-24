//go:build linux || darwin

package xfersend

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"golang.org/x/sys/unix"
)

// sender serializes byte work without wrapping uninterruptible file IO in a
// goroutine. A timed-out Freeze therefore leaves the original owner intact.
// The registry fences actor calls around Freeze; this interface has no Thaw.
// origin: hot-update S7 plan §4 / §8.2 / P08
type sender struct {
	config        Config
	state         x.SenderState
	gate          chan struct{}
	stop          chan struct{}
	stopOnce      sync.Once
	reservation   x.ReservationID
	hash          hash.Hash
	pending       *x.Chunk
	index         uint32
	lastSeq       uint64
	sealed        bool
	sealUnknown   bool
	finalized     bool
	writerDone    bool
	sourceDone    bool
	startedNS     int64
	writerChecked bool
}

var _ Factory = New

// New takes ownership of Source only on success. Capacity is reserved before
// hashing or allocating replay bytes; constructor failures leave its FD alone.
func New(config Config) (Sender, error) {
	if config.Source == nil || config.Clock == nil || config.UTC == nil || config.Capacity == nil || config.Transport == nil ||
		(config.Intent.Tier != "a" && config.Intent.Tier != "b") || (config.Intent.Verb != "push" && config.Intent.Verb != "pull") ||
		config.Identity.SID == "" || config.Identity.TransferID == "" || config.Identity.EntryNonce == "" || config.Identity.IntentDigest == "" ||
		(config.Intent.Tier == "b" && (config.Writer == nil || config.Identity.ObjectVersion == "")) ||
		(config.Intent.Tier == "a" && (config.Writer != nil || config.Identity.ObjectVersion != "")) {
		return nil, fmt.Errorf("%s: incomplete sender configuration", x.CodeIntentConflict)
	}
	if _, err := sourceGeometry(config.Intent, 0); err != nil {
		return nil, err
	}
	if config.Budget.StartedAt.IsZero() || config.Budget.RemainingNS <= 0 || config.Budget.EffectiveBudgetNS <= 0 ||
		config.Budget.EffectiveBudgetNS > int64(proto.XferTierBMaxBudget) || config.Budget.RateLimitBPS != config.Intent.RateLimitBPS || config.Intent.RateLimitBPS < 0 {
		return nil, fmt.Errorf("%s: invalid sender budget", x.CodeBudgetExceeded)
	}
	boot, now, err := config.Clock()
	if err != nil {
		return nil, err
	}
	elapsed := config.UTC().Sub(config.Budget.StartedAt)
	remaining := min(config.Budget.RemainingNS, config.Budget.EffectiveBudgetNS-int64(elapsed))
	if boot == "" || now <= 0 || elapsed < 0 || remaining <= 0 || now > math.MaxInt64-remaining {
		return nil, fmt.Errorf("%s: invalid original sender deadline", x.CodeBudgetExceeded)
	}
	identity, _, sourceSize, err := sourceStat(config.Source)
	if err != nil {
		return nil, err
	}
	// Admission precedes capacity, hashing and source publication. A low inline
	// rate cannot become a 30s worker after its caller accepted a longer budget.
	// origin: S7 internal review rv-wiring F8
	if _, err := sourceBudget(config.Intent, sourceSize); err != nil {
		return nil, err
	}
	id, err := config.Capacity.Reserve(x.Reservation{NonReplayBytes: int64(config.Intent.ChunkSize) + sourceReadBytes, SealedBytes: 256})
	if err != nil {
		return nil, err
	}
	ref := sha256.Sum256([]byte(config.Identity.SID + "\n" + config.Identity.TransferID + "\n" + config.Identity.EntryNonce))
	s := &sender{config: config, gate: make(chan struct{}, 1), stop: make(chan struct{}), reservation: id, hash: sha256.New(), startedNS: now - int64(elapsed)}
	s.state = x.SenderState{Identity: config.Identity, Intent: config.Intent, Budget: config.Budget,
		ByteDeadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(now + remaining)}, SourceIdentity: identity,
		Source: x.ResourceRef{ResourceID: "xfer-source-" + hex.EncodeToString(ref[:]), Name: x.SourceFileName, Kind: x.ResourceSource}}
	s.state.Budget.RemainingNS = remaining
	return s, nil
}

// Import keeps the original source OFD, hash prefix and boot deadline. It does
// no network IO and never seals a source again; the first Step verifies the
// inherited writer receipts before it can append.
// origin: hot-update S7 plan §2.2 / §8.2 / T35
func Import(config Config, state x.SenderState) (Sender, error) {
	// Tier A carries its bytes as the retained inline copy, never an object
	// writer; Tier B still requires its original fixed-version writer.
	// origin: hot-update S7 plan §8.1–§8.2 / FIX-INLINE
	if config.Source == nil || config.Clock == nil || config.UTC == nil || config.Capacity == nil || config.Transport == nil ||
		(config.Writer == nil) != (config.Intent.Tier == "a") || state.Identity != config.Identity || state.Intent != config.Intent || state.Budget != config.Budget || state.Stopped {
		return nil, sourceChanged()
	}
	if !inlineStateValid(config.Intent, state) {
		return nil, sourceChanged()
	}
	boot, _, err := config.Clock()
	if err != nil || boot != state.ByteDeadline.BootID || state.ByteDeadline.Deadline <= 0 {
		return nil, sourceChanged()
	}
	h := sha256.New()
	sealed := state.Budget.SourceSealed
	if sealed {
		if len(state.SHA256State) < 8 || binary.BigEndian.Uint64(state.SHA256State[len(state.SHA256State)-8:]) != uint64(state.Offset) {
			return nil, sourceChanged()
		}
		// Resuming a prefix requires a hash that can import its sealed state.
		// origin: hot-update S7 plan §3.1; S10 LINT-L4
		decoder, ok := h.(encoding.BinaryUnmarshaler)
		if !ok {
			return nil, sourceChanged()
		}
		if err := decoder.UnmarshalBinary(state.SHA256State); err != nil {
			return nil, sourceChanged()
		}
	} else if state.SourceSeal != (x.SourceSeal{}) || state.Offset != 0 || len(state.SHA256State) != 0 {
		return nil, sourceChanged()
	}
	s := &sender{config: config, state: state, gate: make(chan struct{}, 1), stop: make(chan struct{}), hash: h,
		sealed: sealed, finalized: state.Writer.Phase == x.WriteFinalized || state.Inline != nil}
	s.state.Inline = bytes.Clone(state.Inline)
	if sealed {
		if err := checkSource(s); err != nil {
			return nil, err
		}
	} else if identity, _, _, err := sourceStat(config.Source); err != nil || identity != state.SourceIdentity {
		return nil, sourceChanged()
	}
	// Pacing stays anchored to the original start; changing the current image
	// cannot buy a new byte budget or a fresh rate burst.
	_, now, err := config.Clock()
	if err != nil {
		return nil, err
	}
	s.startedNS = now - int64(config.UTC().Sub(state.Budget.StartedAt))
	s.reservation, err = config.Capacity.Reserve(x.Reservation{NonReplayBytes: int64(config.Intent.ChunkSize) + sourceReadBytes, SealedBytes: 256})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func remaining(s *sender, deadline x.Deadline) (time.Duration, error) {
	if deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	boot, now, err := s.config.Clock()
	if err != nil {
		return 0, err
	}
	if boot != s.state.ByteDeadline.BootID || now <= 0 {
		return 0, fmt.Errorf("%s: sender clock identity changed", x.CodeBudgetExceeded)
	}
	if now >= int64(deadline) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func byteRemaining(s *sender, deadline x.Deadline) (time.Duration, error) {
	select {
	case <-s.stop:
		return 0, fmt.Errorf("%s: sender stopped", x.CodeCanceled)
	default:
	}
	return remaining(s, min(deadline, s.state.ByteDeadline.Deadline))
}

func enter(s *sender, deadline x.Deadline) error {
	duration, err := remaining(s, deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case s.gate <- struct{}{}:
		if _, err := remaining(s, deadline); err != nil {
			<-s.gate
			return err
		}
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (s *sender) Seal(deadline x.Deadline) (x.SourceSeal, error) {
	if err := enter(s, deadline); err != nil {
		return x.SourceSeal{}, err
	}
	defer func() { <-s.gate }()
	if _, err := byteRemaining(s, deadline); err != nil {
		return x.SourceSeal{}, err
	}
	if s.state.SourceSeal.SourceFingerprint == "" {
		identity, fingerprint, size, err := sourceStat(s.config.Source)
		if err != nil || identity != s.state.SourceIdentity {
			return x.SourceSeal{}, sourceChanged()
		}
		if _, err := sourceBudget(s.config.Intent, size); err != nil {
			return x.SourceSeal{}, err
		}
		chunks, err := sourceGeometry(s.config.Intent, size)
		if err != nil {
			return x.SourceSeal{}, err
		}
		digest, err := hashSource(s, size, deadline)
		if err != nil {
			return x.SourceSeal{}, err
		}
		s.state.SourceSeal = x.SourceSeal{Size: size, SHA256: digest, ChunkSize: s.config.Intent.ChunkSize, Chunks: chunks,
			RateLimitBPS: s.config.Intent.RateLimitBPS, SourceFingerprint: fingerprint}
		if err := checkSource(s); err != nil {
			s.state.SourceSeal = x.SourceSeal{}
			return x.SourceSeal{}, err
		}
	}
	if (s.config.Intent.Verb == "push" && s.config.Intent.Size != s.state.SourceSeal.Size) ||
		(s.config.Intent.SHA256 != "" && s.config.Intent.SHA256 != s.state.SourceSeal.SHA256) {
		return x.SourceSeal{}, sourceChanged()
	}
	if err := checkSource(s); err != nil {
		return x.SourceSeal{}, err
	}
	if s.sealed {
		return s.state.SourceSeal, nil
	}
	op := x.OpSource
	if s.sealUnknown {
		op = x.OpStatus
	}
	// The CallID travels as a capable wire token ([A-Za-z0-9_-]); a ':' made the
	// shared codec refuse every source seal before it could leave the agent.
	// origin: hot-update S7 plan §6.2 / INTEG-D agent verbs
	request := x.Request{Op: op, CallID: "xfer-source-" + s.config.Identity.EntryNonce, Identity: s.config.Identity}
	if op == x.OpSource {
		seal := s.state.SourceSeal // The transport borrows a copy, never actor state.
		request.Source = &seal
	}
	boot, now, err := s.config.Clock()
	if err != nil || boot != s.state.ByteDeadline.BootID {
		return x.SourceSeal{}, fmt.Errorf("%s: sender clock identity changed", x.CodeBudgetExceeded)
	}
	controlDeadline := min(deadline, s.state.ByteDeadline.Deadline, x.Deadline(now+int64(5*time.Second)))
	response, attempt, err := s.config.Transport.Request(request, controlDeadline)
	emit(s, x.ObserveAttempt, attempt)
	s.sealUnknown = s.sealUnknown || attempt.Sent
	if err != nil {
		return x.SourceSeal{}, err
	}
	if response.Code != "" || response.Error != "" {
		code := response.Code
		if code == "" {
			code = x.CodeResultUnknown
		}
		return x.SourceSeal{}, fmt.Errorf("%s: source admission refused", code)
	}
	if response.Status == nil || response.Status.Identity != s.config.Identity || response.Status.State != x.StatusLive {
		s.sealUnknown = true
		return x.SourceSeal{}, fmt.Errorf("%s: source admission lacks original live identity", x.CodeReconcileRequired)
	}
	if op == x.OpStatus && !response.Status.Budget.SourceSealed {
		// An authoritative live unsealed entry permits replay only on the next
		// call, after this reconciliation has completed.
		s.sealUnknown = false
		return x.SourceSeal{}, fmt.Errorf("%s: source seal still pending", x.CodeReconcileRequired)
	}
	required, err := sourceBudget(s.config.Intent, s.state.SourceSeal.Size)
	if err != nil {
		return x.SourceSeal{}, err
	}
	if err := sealBudget(s, response.Status.Budget, required); err != nil {
		return x.SourceSeal{}, err
	}
	s.sealed = true
	s.sealUnknown = false
	return s.state.SourceSeal, nil
}

// Step advances one acknowledged chunk. Unknown sends retain their bytes and
// must ResolvePending before any later append or source cursor advancement.
// origin: hot-update S7 plan §2.2 / T19
func (s *sender) Step(deadline x.Deadline) error {
	if err := enter(s, deadline); err != nil {
		return err
	}
	defer func() { <-s.gate }()
	if _, err := byteRemaining(s, deadline); err != nil {
		return err
	}
	// origin: hot-update S7 plan §12.2 P08 / T19: no first byte before the durable source seal.
	if !s.sealed {
		return fmt.Errorf("%s: source must be durably sealed before sending", x.CodeIntentConflict)
	}
	if s.finalized {
		return nil
	}
	if s.config.Writer == nil {
		return copyInline(s, min(deadline, s.state.ByteDeadline.Deadline))
	}
	deadline = min(deadline, s.state.ByteDeadline.Deadline)
	if !s.writerChecked {
		// Inspecting a live writer must not perform a lifecycle cut: Freeze
		// closes admission and would strand the first append on the real owner.
		// origin: hot-update S7 plan §2.2 / T19 / INTEG-E storage regression
		snapshot, ok := s.config.Writer.(interface {
			Snapshot(x.Deadline) (x.WriterState, error)
		})
		if !ok {
			return fmt.Errorf("%s: writer snapshot is unavailable", x.CodeReconcileRequired)
		}
		state, err := snapshot.Snapshot(deadline)
		if err != nil {
			return err
		}
		if !matchesSource(s, state.ObjectSpec) {
			return fmt.Errorf("%s: writer is not the original source version", x.CodeObjectVersionMismatch)
		}
		if err := resumeSource(s, state, deadline); err != nil {
			return err
		}
		s.writerChecked = true
	}
	if s.finalized {
		return nil
	}
	if err := checkSource(s); err != nil {
		return err
	}
	if s.pending != nil {
		receipt, found, err := s.config.Writer.ResolvePending(deadline)
		if err != nil {
			return err
		}
		if found {
			return acceptChunk(s, receipt)
		}
	}
	if s.state.Offset == s.state.SourceSeal.Size {
		return finalizeSource(s, deadline)
	}
	if err := paceSource(s, deadline); err != nil {
		return err
	}
	if s.pending == nil {
		length := min(int64(s.state.SourceSeal.ChunkSize), s.state.SourceSeal.Size-s.state.Offset)
		data := make([]byte, int(length))
		n, err := s.config.Source.ReadAt(data, s.state.Offset)
		if err != nil || n != len(data) {
			return sourceChanged()
		}
		s.pending = &x.Chunk{Data: data, Receipt: x.ChunkReceipt{Index: s.index, Offset: s.state.Offset, Length: uint32(length), SHA256: sha256.Sum256(data)}}
	}
	if _, err := byteRemaining(s, deadline); err != nil {
		return err
	}
	receipt, err := s.config.Writer.Append(s.index, s.pending.Data, deadline)
	if err != nil {
		return err
	}
	return acceptChunk(s, receipt)
}

func acceptChunk(s *sender, receipt x.ChunkReceipt) error {
	want := s.pending.Receipt
	// A receipt is progress only for the exact original read, never merely the
	// requested length or the server's all-subject stream cursor.
	if receipt.Index != want.Index || receipt.Offset != want.Offset || receipt.Length != want.Length || receipt.SHA256 != want.SHA256 || receipt.StreamSeq <= s.lastSeq {
		return fmt.Errorf("%s: receipt does not match the pending source chunk", x.CodeObjectConflict)
	}
	_, _ = s.hash.Write(s.pending.Data)
	s.state.Offset += int64(receipt.Length)
	s.index++
	s.lastSeq = receipt.StreamSeq
	s.pending = nil
	return nil
}

func finalizeSource(s *sender, deadline x.Deadline) error {
	// Checking the transmitted digest catches a changed middle chunk even if an
	// attacker restored the original file before the final readback.
	// origin: hot-update S7 plan §2.2 / T19
	if s.index != s.state.SourceSeal.Chunks || hex.EncodeToString(s.hash.Sum(nil)) != s.state.SourceSeal.SHA256 {
		return sourceChanged()
	}
	digest, err := hashSource(s, s.state.SourceSeal.Size, deadline)
	if err != nil {
		return err
	}
	if err := checkSource(s); err != nil || digest != s.state.SourceSeal.SHA256 {
		return sourceChanged()
	}
	if _, err := byteRemaining(s, deadline); err != nil {
		return err
	}
	receipt, err := s.config.Writer.Finalize(deadline)
	if err != nil {
		return err
	}
	spec := receipt.Spec
	if !matchesSource(s, spec) || receipt.MetadataSeq == 0 || receipt.MetadataDigest == "" {
		return fmt.Errorf("%s: metadata receipt does not match the source seal", x.CodeObjectVersionMismatch)
	}
	s.finalized = true
	return nil
}

func matchesSource(s *sender, spec x.ObjectSpec) bool {
	seal := s.state.SourceSeal
	return spec.Identity == s.config.Identity && spec.Size == seal.Size && spec.SHA256 == seal.SHA256 &&
		spec.ChunkSize == seal.ChunkSize && spec.Chunks == seal.Chunks
}

// copyInline retains the exact sealed bytes of a Tier A source. Tier A has no
// object version, writer or data attachment: this copy, verified against the
// durable seal, is the transfer's only byte carrier and answers the original
// prepare and every replay of it. The registry grows its entry reservation
// from the same seal before this step (xferimage.WorkerInlineReservation), so
// the allocation is already accounted. The copy is local: the sealed rate
// paces the receiver's byte leg, not a memory copy. Bounded ReadAt calls keep
// the shared OFD offset and let a concurrent Freeze time out between reads.
// origin: hot-update S7 plan §8.1 / §8.3 / FIX-INLINE
func copyInline(s *sender, deadline x.Deadline) error {
	seal := s.state.SourceSeal
	if seal.Size < 0 || seal.Size > proto.XferTierAMaxBytes || seal.ChunkSize != 0 || seal.Chunks != 0 {
		return fmt.Errorf("%s: inline source exceeds its tier", x.CodePayloadUnsupported)
	}
	if err := checkSource(s); err != nil {
		return err
	}
	data := make([]byte, int(seal.Size))
	for offset := int64(0); offset < seal.Size; {
		if _, err := byteRemaining(s, deadline); err != nil {
			return err
		}
		end := min(offset+sourceReadBytes, seal.Size)
		n, err := s.config.Source.ReadAt(data[offset:end], offset)
		if n != int(end-offset) || (err != nil && err != io.EOF) {
			return sourceChanged()
		}
		offset = end
	}
	sum := sha256.New()
	_, _ = sum.Write(data)
	// A copy taken across an in-place rewrite matches neither the seal nor the
	// unchanged descriptor facts; both are checked after the last read.
	if hex.EncodeToString(sum.Sum(nil)) != seal.SHA256 {
		return sourceChanged()
	}
	if err := checkSource(s); err != nil {
		return err
	}
	s.hash, s.state.Offset, s.state.Inline, s.finalized = sum, seal.Size, data, true
	return nil
}

// inlineStateValid binds an imported inline copy to its seal: only a sealed
// Tier A sender owns one, it is the whole sealed source, and its cursor says
// so (the hash state is bound to that cursor separately). A Tier B sender
// never carries inline bytes; a Tier A sender without its copy has sent none.
// origin: hot-update S7 plan §8.2 / FIX-INLINE
func inlineStateValid(intent x.Intent, state x.SenderState) bool {
	if state.Inline == nil {
		return intent.Tier != "a" || state.Offset == 0
	}
	sum := sha256.Sum256(state.Inline)
	return intent.Tier == "a" && state.Budget.SourceSealed && int64(len(state.Inline)) == state.SourceSeal.Size &&
		state.Offset == state.SourceSeal.Size && hex.EncodeToString(sum[:]) == state.SourceSeal.SHA256
}

// Inline returns a copy of a finalized Tier A sender's retained bytes and the
// durable seal they were verified against. It serializes with Step and Freeze
// on the sender's own gate and never re-reads the source, so a prepare replay
// after any cut serves the original bytes. It refuses until the copy exists,
// after Stop, and for Tier B.
// origin: hot-update S7 plan §8.1 / FIX-INLINE
func Inline(actor Sender, deadline x.Deadline) (x.SourceSeal, []byte, error) {
	s, ok := actor.(*sender)
	if !ok {
		return x.SourceSeal{}, nil, fmt.Errorf("%s: not a transfer sender", x.CodePayloadUnsupported)
	}
	if err := enter(s, deadline); err != nil {
		return x.SourceSeal{}, nil, err
	}
	defer func() { <-s.gate }()
	if s.config.Intent.Tier != "a" || !s.sealed || s.state.Stopped || s.state.Inline == nil {
		return x.SourceSeal{}, nil, fmt.Errorf("%s: inline copy is not retained", x.CodeReconcileRequired)
	}
	return s.state.SourceSeal, bytes.Clone(s.state.Inline), nil
}

// Finalized reports that the original object metadata is published, so no
// byte step remains. The registry asks only from the step owner after Step
// returned, letting its runner end instead of spinning on successful no-ops;
// the sender, pinned source and writer stay owned until Stop.
// origin: hot-update S7 plan §4 / INTEG-D agent sender wiring
func (s *sender) Finalized() bool { return s.finalized }

func (s *sender) Freeze(deadline x.Deadline) (x.SenderState, error) {
	if err := enter(s, deadline); err != nil {
		return x.SenderState{}, err
	}
	defer func() { <-s.gate }()
	state := s.state
	// The retained inline copy crosses the cut by value: a codec caller must
	// not alias the bytes a later prepare replay still serves (nil stays nil).
	state.Inline = bytes.Clone(s.state.Inline)
	var err error
	if s.config.Writer != nil {
		state.Writer, err = s.config.Writer.Freeze(deadline)
		if err != nil {
			return x.SenderState{}, err
		}
	}
	marshaler, ok := s.hash.(encoding.BinaryMarshaler)
	if !ok {
		return x.SenderState{}, fmt.Errorf("%s: sender hash state is not serializable", x.CodeReconcileRequired)
	}
	state.SHA256State, err = marshaler.MarshalBinary()
	if err != nil {
		return x.SenderState{}, err
	}
	if _, err := remaining(s, deadline); err != nil {
		return x.SenderState{}, err
	}
	emit(s, x.ObserveFreeze, x.Attempt{})
	return state, nil
}

func (s *sender) Stop(deadline x.Deadline) error {
	// Cancel admission before joining, including a rate wait with a long byte
	// budget. Failure to join preserves the source and reservation for retry.
	s.stopOnce.Do(func() { close(s.stop) })
	if err := enter(s, deadline); err != nil {
		return err
	}
	defer func() { <-s.gate }()
	if s.state.Stopped {
		return nil
	}
	if !s.writerDone && s.config.Writer != nil {
		if err := s.config.Writer.Stop(deadline); err != nil {
			return err
		}
	}
	s.writerDone = true
	if !s.sourceDone {
		if err := s.config.Source.Close(); err != nil {
			return fmt.Errorf("xfersend: source close failed")
		}
		s.sourceDone = true
	}
	if err := s.config.Capacity.Release(s.reservation); err != nil {
		return err
	}
	// A stopped owner serves no replay; its registry entry's reservation for
	// the inline copy is released with that entry, never before this point.
	s.pending, s.state.Inline = nil, nil
	s.state.Stopped = true
	return nil
}

func emit(s *sender, kind x.ObservationKind, attempt x.Attempt) {
	if s.config.Observe == nil {
		return
	}
	boot, now, err := s.config.Clock()
	if err != nil {
		return
	}
	s.config.Observe(x.Observation{Kind: kind, BootID: boot, BoottimeNS: now, TransferID: s.config.Identity.TransferID,
		EntryNonce: s.config.Identity.EntryNonce, ObjectVersion: s.config.Identity.ObjectVersion, Leg: "sender", Phase: x.PhaseSending,
		Index: s.index, Offset: s.state.Offset, Source: s.state.Source, OriginalDeadline: s.state.ByteDeadline, Attempt: attempt})
}

// A sleeping worker can construct its sender from the original FD and imported
// writer without a second state argument in the frozen Factory. Rebuild the
// hash from exact confirmed receipts; a pending read is never confirmed progress.
// The worker validates SenderState's boot/OFD custody before invoking New.
// origin: hot-update S7 plan §2.2 / §8.2 / T19
func resumeSource(s *sender, state x.WriterState, deadline x.Deadline) error {
	switch state.Phase {
	case x.WriteOpen, x.WritePending, x.WriteMetadataPending, x.WriteFinalized:
		// These phases retain byte or metadata responsibility at this owner.
	case x.WriteRetiring, x.WriteRetired:
		return fmt.Errorf("%s: inherited writer is retiring", x.CodeObjectConflict)
	default:
		return fmt.Errorf("%s: inherited writer phase is unknown", x.CodeObjectConflict)
	}
	hash := sha256.New()
	var offset int64
	var lastSeq uint64
	for index, receipt := range state.Receipts {
		if _, err := byteRemaining(s, deadline); err != nil {
			return err
		}
		length := min(int64(s.state.SourceSeal.ChunkSize), s.state.SourceSeal.Size-offset)
		if index >= int(s.state.SourceSeal.Chunks) || receipt.Index != uint32(index) || receipt.Offset != offset ||
			receipt.Length != uint32(length) || receipt.StreamSeq <= lastSeq {
			return fmt.Errorf("%s: inherited receipt prefix is not contiguous", x.CodeObjectConflict)
		}
		data := make([]byte, int(length))
		if n, err := s.config.Source.ReadAt(data, offset); err != nil || n != len(data) || sha256.Sum256(data) != receipt.SHA256 {
			return sourceChanged()
		}
		_, _ = hash.Write(data)
		offset += length
		lastSeq = receipt.StreamSeq
	}
	var pending *x.Chunk
	if attempt := state.PendingAttempt; attempt != nil {
		length := min(int64(s.state.SourceSeal.ChunkSize), s.state.SourceSeal.Size-offset)
		if attempt.Index != uint32(len(state.Receipts)) || attempt.Offset != offset || length <= 0 ||
			attempt.Length != uint32(length) || len(attempt.Bytes) != int(length) || sha256.Sum256(attempt.Bytes) != attempt.SHA256 {
			return fmt.Errorf("%s: inherited pending chunk changed", x.CodeObjectConflict)
		}
		pending = &x.Chunk{Receipt: x.ChunkReceipt{Index: attempt.Index, Offset: offset, Length: attempt.Length, SHA256: attempt.SHA256},
			Data: append([]byte(nil), attempt.Bytes...)}
	}
	finalized := state.Phase == x.WriteFinalized
	if finalized {
		meta := state.MetaReceipt
		if pending != nil || offset != s.state.SourceSeal.Size || meta == nil || !matchesSource(s, meta.Spec) ||
			meta.MetadataSeq == 0 || meta.MetadataDigest == "" || hex.EncodeToString(hash.Sum(nil)) != s.state.SourceSeal.SHA256 {
			return fmt.Errorf("%s: inherited metadata lacks the original complete prefix", x.CodeObjectVersionMismatch)
		}
	}
	// Commit the rebuilt closure only after validating it entirely. Timeout or
	// corruption cannot leave a partial hash that a later call appends twice.
	s.hash, s.state.Offset, s.index, s.lastSeq = hash, offset, uint32(len(state.Receipts)), lastSeq
	s.pending, s.finalized = pending, finalized
	return nil
}

func sourceChanged() error {
	return fmt.Errorf("%s: pinned source no longer matches its seal", x.CodeSourceChanged)
}

func checkSource(s *sender) error {
	identity, fingerprint, size, err := sourceStat(s.config.Source)
	if err != nil || identity != s.state.SourceIdentity || fingerprint != s.state.SourceSeal.SourceFingerprint || size != s.state.SourceSeal.Size {
		return sourceChanged()
	}
	return nil
}

// ReadAt leaves the shared OFD cursor untouched. Deadline checks between bounded
// reads let a concurrent Freeze time out without creating a late IO goroutine.
func hashSource(s *sender, size int64, deadline x.Deadline) (string, error) {
	hash := sha256.New()
	buffer := make([]byte, sourceReadBytes)
	for offset := int64(0); offset < size; {
		if _, err := byteRemaining(s, deadline); err != nil {
			return "", err
		}
		length := min(int64(len(buffer)), size-offset)
		n, err := s.config.Source.ReadAt(buffer[:length], offset)
		if n != int(length) || (err != nil && err != io.EOF) {
			return "", sourceChanged()
		}
		_, _ = hash.Write(buffer[:n])
		offset += int64(n)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

const sourceReadBytes = 64 * 1024

// The current proto constants remain the sizing SSOT. The rate-aware formula
// is evaluated before asking the origin to durably seal an unknown pull size.
// origin: hot-update S7 plan §1.2 / LQ2
func sourceBudget(intent x.Intent, size int64) (time.Duration, error) {
	// Source discovery must tighten the same original budget used by ctl and
	// tracker; a sender-specific formula could admit bytes the tracker refuses.
	// Check before multiplying by nanoseconds, including a one-byte/s request.
	// origin: S7 internal review rv-wiring F8
	return proto.TransferBudget(intent.Tier, size, intent.RateLimitBPS)
}

func sourceGeometry(intent x.Intent, size int64) (uint32, error) {
	if intent.Tier == "a" {
		return 0, nil
	}
	if intent.ChunkSize == 0 || intent.ChunkSize > 1024*1024 {
		return 0, fmt.Errorf("%s: unsupported source chunk geometry", x.CodePayloadUnsupported)
	}
	count := (size + int64(intent.ChunkSize) - 1) / int64(intent.ChunkSize)
	if count > math.MaxUint32 {
		return 0, fmt.Errorf("%s: excessive source chunk count", x.CodePayloadUnsupported)
	}
	return uint32(count), nil
}

func sealBudget(s *sender, budget x.BudgetSeal, required time.Duration) error {
	if !budget.SourceSealed || budget.Size != s.state.SourceSeal.Size || budget.RateLimitBPS != s.config.Intent.RateLimitBPS ||
		!budget.StartedAt.Equal(s.config.Budget.StartedAt) || budget.EffectiveBudgetNS != int64(required) || budget.RemainingNS <= 0 {
		return fmt.Errorf("%s: source admission did not preserve the original budget", x.CodeIntentConflict)
	}
	boot, now, err := s.config.Clock()
	if err != nil || boot != s.state.ByteDeadline.BootID {
		return fmt.Errorf("%s: source clock identity changed", x.CodeBudgetExceeded)
	}
	elapsed := s.config.UTC().Sub(budget.StartedAt)
	if elapsed < 0 {
		return fmt.Errorf("%s: source start is in the future", x.CodeBudgetExceeded)
	}
	remaining := min(budget.RemainingNS, int64(required-elapsed), int64(s.state.ByteDeadline.Deadline)-now)
	if remaining <= 0 || now > math.MaxInt64-remaining {
		return fmt.Errorf("%s: original source deadline expired", x.CodeBudgetExceeded)
	}
	budget.RemainingNS = remaining
	s.state.Budget = budget
	s.state.ByteDeadline.Deadline = x.Deadline(now + remaining)
	return nil
}

// An explicit rate limits byte admission, including after a lost ACK. The
// origin's original start anchors pacing, so a freeze/retry cannot reset it.
// Zero retains the existing unrestricted byte admission behavior.
func paceSource(s *sender, deadline x.Deadline) error {
	rate := s.config.Intent.RateLimitBPS
	if rate == 0 || s.state.Offset == 0 {
		return nil
	}
	due := time.Duration(s.state.Offset/rate)*time.Second + time.Duration((s.state.Offset%rate)*int64(time.Second)/rate)
	for {
		remaining, err := byteRemaining(s, deadline)
		if err != nil {
			return err
		}
		boot, now, err := s.config.Clock()
		if err != nil || boot != s.state.ByteDeadline.BootID {
			return fmt.Errorf("%s: source pacing clock identity changed", x.CodeBudgetExceeded)
		}
		elapsed := time.Duration(now - s.startedNS)
		wait := due - elapsed
		if wait <= 0 {
			return nil
		}
		timer := time.NewTimer(min(wait, remaining))
		select {
		case <-s.stop:
			timer.Stop()
			return fmt.Errorf("%s: sender stopped", x.CodeCanceled)
		case <-timer.C:
		}
	}
}

// Source checks use the inherited descriptor exclusively. Neither a renamed
// pathname nor a matching inode obtained by reopening supplies OFD custody.
// origin: hot-update S7 plan §2.2 / §3 / T19
func sourceStat(file *os.File) (x.FileIdentity, string, int64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return x.FileIdentity{}, "", 0, sourceChanged()
	}
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
	if err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || flags&unix.O_ACCMODE == unix.O_WRONLY {
		return x.FileIdentity{}, "", 0, sourceChanged()
	}
	identity := x.FileIdentity{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino), Mode: uint32(stat.Mode), Flags: int32(flags)}
	// Access time is deliberately absent: hashing the file must not invalidate
	// its own seal. Namespace operations can change ctime without changing bytes,
	// so final full hashing, rather than ctime, detects in-place modifications.
	facts := fmt.Sprintf("xfer-source-v1\n%d\n%d\n%d\n%d\n%d\n%d\n%d", identity.Dev, identity.Ino,
		identity.Mode, identity.Flags, stat.Size, stat.Mtim.Sec, stat.Mtim.Nsec)
	digest := sha256.Sum256([]byte(facts))
	return identity, hex.EncodeToString(digest[:]), stat.Size, nil
}
