package xferclient

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// Client has one serialized owner for IO and retained mutation evidence. Cancel
// closes byte admission without acquiring that owner, so blocked control IO
// cannot admit another chunk or primitive after cancellation.
// origin: hot-update S7 plan §7 / T33
type Client struct {
	cfg                                  Config
	gate                                 chan struct{}
	byteStop                             chan struct{}
	cancelOnce                           sync.Once
	cancelReason                         atomic.Pointer[x.CancelReason]
	stopAdmission                        atomic.Bool
	identity                             x.Identity
	origin                               x.Origin
	permit                               *x.CreationPermit
	phase                                x.Phase
	probed, prepared, committed, stopped bool
	deadline                             x.Deadline
	bootID                               string
	startedNS                            int64
	budget                               x.BudgetSeal
	brokerStarted                        time.Time
	lastRequestNS                        int64
	pending                              *x.Request
	receipt                              *x.TerminalReceipt
	grant                                *x.CommitGrant
	outcome                              *x.ReceiverOutcome
	js                                   jetstream.JetStream
	dataOpen                             bool
	spec                                 *x.ObjectSpec
	writer                               x.ObjectWriter
	writerWitness                        *x.WriterState
	reader                               x.ObjectReader
	checkpoint                           x.SinkCheckpoint
	pushOffset                           int64
	pushIndex                            uint32
	pushLastSeq                          uint64
	pushPending                          []byte
	pushSeal                             *x.SourceSeal
	preparedSource                       *x.SourceSeal
	settlement                           *x.SettlementCapsule
	inline                               []byte
	sourcePublished, objectFinalized     bool
	sourceInfo                           os.FileInfo
	sendStarted                          int64
	reservation                          x.ReservationID
	eventSeq                             uint64
}

var _ Invocation = (*Client)(nil)
var _ Factory = NewClient

// NewClient accepts an already minted business ID. Successful construction
// transfers Source/Destination ownership to Stop; a rejected config retains it.
// There is deliberately no ID generator, SDK ObjectStore, or legacy closure.
// origin: hot-update S7 plan §7 / M01
func NewClient(cfg Config) (Invocation, error) {
	refuse := func(message string) (Invocation, error) {
		return nil, clientError(x.CodePayloadUnsupported, cfg.Identity, x.PhaseOpen, false, errors.New(message))
	}
	if cfg.Clock == nil || cfg.UTC == nil || cfg.Capacity == nil || cfg.Transport == nil {
		// A hidden wall clock or unaccounted buffer would disagree with the
		// broker's original budget after a cut. Require the outer owner ports.
		// origin: hot-update S7 plan §1.2 / §8.1
		return refuse("clock, UTC, capacity and transport are required")
	}
	if cfg.Identity.SID == "" || cfg.Identity.NID == "" || cfg.Identity.ActorFP == "" || cfg.Identity.TransferID == "" {
		// Construction is the last point at which a missing business identity
		// can be refused without an admitted worker or publication obligation.
		// origin: hot-update S7 plan §7 / T33
		return refuse("the original session, node, actor and transfer ID are required")
	}
	if cfg.Intent.Verb != "push" && cfg.Intent.Verb != "pull" {
		return refuse("invalid transfer verb")
	}
	if cfg.Intent.Tier != "a" && cfg.Intent.Tier != "b" {
		return refuse("invalid transfer tier")
	}
	if cfg.Intent.Path == "" || cfg.Intent.Size < 0 || cfg.Intent.Size > proto.XferMaxBytes {
		return refuse("invalid path or transfer size")
	}
	if cfg.Intent.Tier == "a" && cfg.Intent.ChunkSize != 0 {
		return refuse("inline transfers have no object geometry")
	}
	if _, err := proto.TransferBudget(cfg.Intent.Tier, cfg.Intent.Size, cfg.Intent.RateLimitBPS); err != nil {
		return nil, clientError(x.CodePayloadUnsupported, cfg.Identity, x.PhaseOpen, false, err)
	}
	if cfg.Intent.Verb == "push" {
		// Pin ownership is supplied by the CLI; reopening its pathname here
		// would let a source replacement silently become the resumed transfer.
		// origin: hot-update S7 plan §7 / T33
		if cfg.Source == nil || cfg.Intent.Tier == "b" && cfg.MakeWriter == nil || !validSHA(cfg.Intent.SHA256) {
			return refuse("push requires a pinned source, receipt writer and SHA-256")
		}
	} else if cfg.Destination == nil || cfg.Intent.Tier == "b" && cfg.MakeReader == nil {
		return refuse("pull requires an atomic destination and checkpoint reader")
	}
	if cfg.Intent.ChunkSize != 0 && (cfg.Intent.ChunkSize < 32*1024 || cfg.Intent.ChunkSize > 1024*1024) {
		return refuse("chunk size must be between 32 KiB and 1 MiB")
	}
	encoded, err := json.Marshal(cfg.Intent)
	// The frozen Intent has ordered scalar fields and no mutable maps. Hash
	// that exact request, before payload negotiation supplies source geometry.
	// A later source seal must not rewrite the pull request's unknown facts.
	// origin: hot-update S7 plan §6.1 / T33
	if err != nil {
		return refuse("intent cannot be encoded")
	}
	digest := sha256.Sum256(encoded)
	intentDigest := hex.EncodeToString(digest[:])
	if cfg.Identity.IntentDigest != "" && cfg.Identity.IntentDigest != intentDigest {
		return nil, clientError(x.CodeIntentConflict, cfg.Identity, x.PhaseOpen, false, nil)
	}
	cfg.Identity.IntentDigest = intentDigest
	c := &Client{cfg: cfg, identity: cfg.Identity, phase: x.PhaseOpen, gate: make(chan struct{}, 1), byteStop: make(chan struct{})}
	c.gate <- struct{}{}
	return c, nil
}

// Run can be reentered after an unavailable response, but never starts a second
// transaction. Every retained pending operation is reconciled before replay.
func (c *Client) Run(deadline x.Deadline) (x.TerminalReceipt, error) {
	if err := c.acquire(deadline); err != nil {
		return x.TerminalReceipt{}, err
	}
	defer c.release()
	// A lost reply to a sent mutation consumes one five-second operation
	// budget, not the invocation's: the sent request stays pending and this
	// same invocation keeps reconciling it status-first (and replaying only its
	// original CallID) until the original deadline. Only then is the result
	// reported unknown. The real NATS request waits out its whole operation
	// budget for a lost reply, which left no time to reconcile inside it.
	// origin: hot-update S7 plan §7 (commit/reserve reply lost; unresolved at the original deadline -> exit 74) / INTEG-D commit_lost
	recoverPull := false
	for {
		var receipt x.TerminalReceipt
		var err error
		if recoverPull {
			// Preparation already owns the original entry. Reconcile it before
			// reopening its data attachment; an unavailable census cannot admit
			// more IO, and a terminal can only settle the original invocation.
			// origin: S7 internal review FIX-TIERA2 F6 / hot-update S7 plan §6.1 / §7 / T46
			var status x.Status
			status, err = c.reconcile(clientContinuationDeadline(c, deadline))
			if err == nil && c.receipt != nil {
				return c.result(nil)
			}
			if err == nil && status.State != x.StatusLive {
				return c.result(c.failure(x.CodeResultUnknown, errors.New("prepared pull has no live original owner")))
			}
		}
		if err == nil {
			receipt, err = runClient(c, deadline)
		}
		if !canContinueClient(c, err, deadline) {
			return receipt, err
		}
		recoverPull = c.prepared && c.cfg.Intent.Verb == "pull" && c.pending == nil && c.outcome == nil
		if waitErr := c.wait(clientContinuationDeadline(c, deadline), 500*time.Millisecond, true); waitErr != nil && c.cancelReason.Load() == nil {
			return receipt, err
		}
	}
}

// canContinueClient admits only an unknown result that still owns a sent mutation or
// an accepted commit. Cancellation, Stop, a terminal receipt and every refusal
// return to the caller at once; no state here can mint another request.
// An accepted pull also retains its byte responsibility across local attachment
// recovery; authoritative permission/identity refusals still return immediately.
// origin: S7 internal review FIX-TIERA2 F6 / hot-update S7 plan §7 / T46
func canContinueClient(c *Client, err error, deadline x.Deadline) bool {
	code := ErrorCode(err)
	retainedMutation := code == x.CodeResultUnknown && (c.pending != nil || c.committed)
	recoveringPull := c.prepared && c.cfg.Intent.Verb == "pull" && (code == x.CodeResultUnknown || code == x.CodeOwnerRecovering)
	if err == nil || c.receipt != nil || c.stopAdmission.Load() || c.cancelReason.Load() != nil ||
		!retainedMutation && !recoveringPull {
		return false
	}
	_, remainingErr := c.remaining(clientContinuationDeadline(c, deadline))
	return remainingErr == nil
}

// Before commit the sealed byte deadline also bounds continuation; an
// accepted commit or landed outcome is settled until the invocation deadline.
func clientContinuationDeadline(c *Client, deadline x.Deadline) x.Deadline {
	if c.committed || c.outcome != nil || c.deadline == 0 {
		return deadline
	}
	return minDeadline(deadline, c.deadline)
}

func runClient(c *Client, deadline x.Deadline) (x.TerminalReceipt, error) {
	if c.receipt != nil {
		return *c.receipt, c.resultError(*c.receipt)
	}
	if c.stopAdmission.Load() {
		return x.TerminalReceipt{}, c.failure(x.CodeResultUnknown, errors.New("invocation stopped"))
	}
	// The byte deadline closes new IO and primitive admission. Settlement has
	// a separate bounded attempt: reentry must still deliver an already landed
	// outcome or read the original remote terminal after that deadline expires.
	// origin: hot-update S7 plan §1.2 / §7 / T33
	if c.outcome != nil {
		return c.result(c.settle(deadline))
	}
	if c.committed {
		return c.result(c.waitTerminal(deadline))
	}
	if c.pending != nil && c.pending.Op == x.OpCommit && c.deadline != 0 {
		if _, byteErr := c.remaining(c.deadline); byteErr != nil {
			_, err := c.reconcile(deadline)
			if err == nil && c.receipt == nil {
				err = c.failure(x.CodeCommitInflight, byteErr)
			}
			return c.result(err)
		}
	}
	if err := c.start(deadline); err != nil {
		return x.TerminalReceipt{}, err
	}
	deadline = minDeadline(deadline, c.deadline)
	if c.cancelReason.Load() != nil {
		_, err := c.cancel(deadline)
		if err == nil {
			err = c.failure(x.CodeCanceled, nil)
		}
		return c.result(err)
	}
	if err := c.prepare(deadline); err != nil || c.receipt != nil {
		return c.result(err)
	}
	var err error
	if c.cfg.Intent.Verb == "push" {
		err = c.push(deadline)
	} else {
		err = c.pull(deadline)
	}
	if c.cancelReason.Load() != nil && c.receipt == nil {
		_, cancelErr := c.cancel(deadline)
		if cancelErr != nil {
			err = cancelErr
		}
	}
	return c.result(err)
}

func (c *Client) result(err error) (x.TerminalReceipt, error) {
	if c.receipt != nil {
		return *c.receipt, c.resultError(*c.receipt)
	}
	if err == nil {
		err = c.failure(x.CodeResultUnknown, nil)
	}
	return x.TerminalReceipt{}, err
}

func (c *Client) start(deadline x.Deadline) error {
	// Reentry is continuation, including after a failed control attempt. It
	// cannot buy a new byte deadline merely by supplying a later UI timeout.
	// origin: hot-update S7 plan §1.2 / §7 / T33
	if c.deadline != 0 {
		return nil
	}
	bootID, now, err := c.cfg.Clock()
	if err != nil || bootID == "" || now <= 0 || deadline <= x.Deadline(now) {
		return c.failure(x.CodeBudgetExceeded, err)
	}
	budget, err := proto.TransferBudget(c.cfg.Intent.Tier, c.cfg.Intent.Size, c.cfg.Intent.RateLimitBPS)
	if err != nil {
		return c.failure(x.CodeBudgetExceeded, err)
	}
	if c.cfg.Intent.Verb == "pull" && c.cfg.Intent.Tier == "b" && c.cfg.Intent.Size == 0 {
		// Source discovery is provisional, not the old five-minute fallback.
		// The prepared source budget later tightens this same original clock.
		// origin: hot-update S7 plan §1.2 / lead LQ2
		budget = proto.XferTierBMaxBudget
	}
	if now > math.MaxInt64-int64(budget) {
		return c.failure(x.CodeBudgetExceeded, controlbudget.ErrInvalidDeadline)
	}
	c.bootID, c.startedNS = bootID, now
	c.deadline = minDeadline(deadline, x.Deadline(now+int64(budget)))
	c.budget = x.BudgetSeal{StartedAt: c.cfg.UTC(), Size: c.cfg.Intent.Size, RateLimitBPS: c.cfg.Intent.RateLimitBPS,
		EffectiveBudgetNS: int64(budget), RemainingNS: int64(c.deadline) - now}
	return nil
}

// Original start and budget only tighten. A later Run or a smaller source seal
// cannot reset either the local deadline or the ledger's started-at timestamp.
// origin: hot-update S7 plan §1.2 / lead LQ2
func rateBudget(size, rate int64) (time.Duration, error) {
	// A high requested cap cannot assume the link actually meets it. A low
	// cap must be budgeted as requested rather than pretending it is 2 MiB/s.
	// origin: hot-update S7 plan §1.2 / lead LQ2
	// The shared formula also owns this compatibility test entry.
	// origin: S7 internal review rv-wiring F8
	return proto.TransferBudget("b", size, rate)
}

func (c *Client) prepare(deadline x.Deadline) error {
	// Successful negotiation is retained, while an unavailable response is
	// never interpreted as cap0. Only the outer precreation choice can build
	// a Legacy invocation; there is no fallback edge from this state machine.
	// origin: hot-update S7 plan §7 / T33
	if !c.probed {
		response, err := c.read(x.Request{Op: x.OpProbe, CallID: c.callID(x.OpProbe), Identity: c.identity}, deadline)
		if err != nil {
			return err
		}
		p := response.Probe
		if p == nil || p.Version != x.Version || p.HistoryGuardVersion != x.Version || p.BucketConfigVersion != x.Version {
			return c.failure(x.CodePayloadUnsupported, errors.New("explicit transfer-v1 capability is required"))
		}
		if p.Code != "" || p.Error != "" {
			return c.failure(p.Code, errors.New(p.Error))
		}
		if p.InstanceID == "" || (c.identity.AgentInstance != "" && c.identity.AgentInstance != p.InstanceID) {
			return c.failure(x.CodeIntentConflict, errors.New("agent instance changed"))
		}
		c.identity.AgentInstance = p.InstanceID
		// Headers and stream framing need room beside the data. Never negotiate
		// an oversized chunk from a missing/zero limit response.
		limit := min(p.MaxPayload, p.MaxMsgSize) - 4096
		chunkSize := c.cfg.Intent.ChunkSize
		if chunkSize == 0 {
			chunkSize = uint32(max(int64(0), min(int64(128*1024), limit)))
		}
		if limit < 32*1024 || int64(chunkSize) > limit {
			return c.failure(x.CodePayloadUnsupported, errors.New("payload limits cannot carry the agreed chunk size"))
		}
		// Geometry is source-sealed, while the original Intent stays immutable.
		c.spec = &x.ObjectSpec{ChunkSize: chunkSize}
		c.probed = true
	}
	if c.permit == nil {
		intent := c.cfg.Intent
		response, err := c.mutate(x.Request{Op: x.OpPermit, CallID: c.callID(x.OpPermit), Identity: c.identity, Origin: c.origin, Intent: &intent}, deadline)
		if err != nil || c.receipt != nil {
			return err
		}
		p := response.Permit
		if p == nil || !sameScope(c.identity, p.Identity) || p.Identity.EntryNonce == "" || (c.cfg.Intent.Tier == "b") != (p.Identity.ObjectVersion != "") ||
			!validOrigin(p.Origin) || p.Signature == "" || p.PermitNonce == "" || !p.CreateUntil.After(p.IssuedAt) {
			return c.failure(x.CodeIntentConflict, errors.New("invalid creation permit binding"))
		}
		if c.identity.EntryNonce != "" && c.identity.EntryNonce != p.Identity.EntryNonce ||
			c.identity.ObjectVersion != "" && c.identity.ObjectVersion != p.Identity.ObjectVersion {
			return c.failure(x.CodeObjectVersionMismatch, nil)
		}
		c.identity, c.origin = p.Identity, p.Origin
		// Copy the permit rather than retain a transport-owned response pointer.
		// Its signature, nonce, origin and NUID must survive every later retry.
		// origin: hot-update S7 plan §6.1 / §7
		permit := *p
		c.permit = &permit
	}
	if err := bindClientDestination(c); err != nil {
		return err
	}
	if !c.prepared {
		// Expiry only fences first creation. A pending prepare must reconcile
		// the original entry before deciding whether its old permit can replay.
		// origin: hot-update S7 plan §6.1 / T33
		if c.pending == nil && !c.cfg.UTC().Before(c.permit.CreateUntil) {
			return c.failure(x.CodePermitExpired, nil)
		}
		if c.cfg.Intent.Tier == "a" && c.cfg.Intent.Verb == "push" {
			if err := c.prepareInline(deadline); err != nil {
				return err
			}
		}
		intent := c.cfg.Intent
		response, err := c.mutate(x.Request{Op: x.OpPrepare, CallID: c.callID(x.OpPrepare), Identity: c.identity, Origin: c.origin,
			Intent: &intent, Permit: c.permit, Inline: c.inline}, deadline)
		if err != nil || c.receipt != nil {
			return err
		}
		if response.Status == nil || response.Status.State != x.StatusLive {
			return c.failure(x.CodeResultUnknown, errors.New("prepare has no live receipt"))
		}
		c.prepared = true
	}
	return c.checkBytes(deadline)
}

// bindDestination declares the permit's object incarnation to a sink that
// supports it, before any byte reaches it: the fixed Tier B NUID, or "" for the
// Tier A inline path. Repeating it on reentry is idempotent; a different value
// is a conflict, never a reason to rebind the receiver to a new object.
// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
func bindClientDestination(c *Client) error {
	if c.cfg.Intent.Verb != "pull" || c.cfg.Destination == nil || c.permit == nil {
		return nil
	}
	binder, ok := c.cfg.Destination.(interface{ BindObjectVersion(string) error })
	if !ok {
		return nil
	}
	if err := binder.BindObjectVersion(c.identity.ObjectVersion); err != nil {
		return c.failure(x.CodeObjectVersionMismatch, err)
	}
	return nil
}

func (c *Client) failure(code string, cause error) error {
	if code == "" {
		code = x.CodeResultUnknown
	}
	return clientError(code, c.identity, c.phase, c.permit != nil || c.pending != nil || c.prepared || c.outcome != nil ||
		validOrigin(c.origin) && c.identity.EntryNonce != "", cause)
}

func (c *Client) callID(op x.Operation) string { return c.identity.TransferID + "-" + string(op) }

func minDeadline(a, b x.Deadline) x.Deadline { return min(a, b) }

func (c *Client) remaining(deadline x.Deadline) (time.Duration, error) {
	// BOOTTIME from another boot is not comparable. Failing this check must
	// close admission rather than interpret the clock change as extra budget.
	// origin: hot-update S7 plan §1.2 / T33
	bootID, now, err := c.cfg.Clock()
	if err != nil {
		return 0, err
	}
	if deadline <= 0 || now <= 0 || bootID == "" || c.bootID != "" && bootID != c.bootID {
		return 0, controlbudget.ErrInvalidDeadline
	}
	if now >= int64(deadline) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func (c *Client) acquire(deadline x.Deadline) error {
	// Identity is immutable outside the serialized owner. The config copy is
	// used here because the owner may currently be accepting a signed permit.
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return clientError(x.CodeResultUnknown, c.cfg.Identity, x.PhaseOpen, true, err)
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-c.gate:
		return nil
	case <-timer.C:
		return clientError(x.CodeResultUnknown, c.cfg.Identity, x.PhaseOpen, true, controlbudget.ErrExpired)
	}
}

func (c *Client) release() { c.gate <- struct{}{} }

func (c *Client) checkBytes(deadline x.Deadline) error {
	if c.stopAdmission.Load() || c.cancelReason.Load() != nil {
		return c.failure(x.CodeCanceled, nil)
	}
	if _, err := c.remaining(minDeadline(deadline, c.deadline)); err != nil {
		return c.failure(x.CodeBudgetExceeded, err)
	}
	return nil
}

func (c *Client) attemptDeadline(deadline x.Deadline) (x.Deadline, error) {
	// Bound one logical control operation, including its reconciliation and
	// backoff. A retry must consume this deadline, not create another 5 s.
	// origin: hot-update S7 plan §1.2 / §7
	remaining, err := c.remaining(deadline)
	if err != nil {
		return 0, c.failure(x.CodeResultUnknown, err)
	}
	return deadline - x.Deadline(max(time.Duration(0), remaining-5*time.Second)), nil
}

// wait never hides an unbounded sleep behind a context-free port. Cancellation
// interrupts byte pacing; settlement retries still have their own finite limit.
func (c *Client) wait(deadline x.Deadline, duration time.Duration, bytes bool) error {
	remaining, err := c.remaining(deadline)
	if err != nil {
		return c.failure(x.CodeResultUnknown, err)
	}
	timer := time.NewTimer(min(remaining, duration))
	defer timer.Stop()
	if bytes {
		select {
		case <-c.byteStop:
			return c.failure(x.CodeCanceled, nil)
		case <-timer.C:
		}
	} else {
		<-timer.C
	}
	if _, err := c.remaining(deadline); err != nil {
		return c.failure(x.CodeResultUnknown, err)
	}
	return nil
}

func validSHA(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == hex.EncodeToString(decoded)
}

func (c *Client) Cancel(reason x.CancelReason, deadline x.Deadline) (x.Status, error) {
	switch reason {
	case x.CancelUser, x.CancelDeadline, x.CancelRevoked, x.CancelStopped:
	default:
		return x.Status{}, clientError(x.CodePayloadUnsupported, c.cfg.Identity, x.PhaseOpen, false, errors.New("invalid cancel reason"))
	}
	c.cancelReason.CompareAndSwap(nil, &reason)
	c.cancelOnce.Do(func() { close(c.byteStop) })
	if err := c.acquire(deadline); err != nil {
		return x.Status{}, err
	}
	defer c.release()
	return c.cancel(deadline)
}

func (c *Client) cancel(deadline x.Deadline) (x.Status, error) {
	if c.receipt != nil {
		return x.Status{State: x.StatusTerminal, Phase: c.phase, Identity: c.identity, Origin: c.origin, Terminal: c.receipt}, nil
	}
	if c.permit == nil && c.pending == nil {
		return x.Status{}, c.failure(x.CodeCanceled, errors.New("invocation has not been admitted"))
	}
	// Resolve the old request before switching operation: cancellation must not
	// erase a sent-unknown commit, or contradict an already landed primitive.
	if c.pending != nil {
		if _, err := c.reconcile(deadline); err != nil {
			return x.Status{}, err
		}
		if c.receipt != nil {
			return x.Status{State: x.StatusTerminal, Phase: c.phase, Identity: c.identity, Origin: c.origin, Terminal: c.receipt}, nil
		}
		c.pending = nil
	}
	if c.outcome != nil {
		if err := c.settle(deadline); err != nil {
			return x.Status{}, err
		}
		return x.Status{State: x.StatusTerminal, Phase: c.phase, Identity: c.identity, Origin: c.origin, Terminal: c.receipt}, nil
	}
	reason := c.cancelReason.Load()
	request := x.CancelRequest{CallID: c.callID(x.OpCancel), Origin: c.origin, Reason: *reason}
	response, err := c.mutate(x.Request{Op: x.OpCancel, CallID: request.CallID, Identity: c.identity, Origin: c.origin, Cancel: &request}, deadline)
	if err != nil {
		return x.Status{}, err
	}
	if response.Status != nil {
		return *response.Status, nil
	}
	if c.receipt != nil {
		return x.Status{State: x.StatusTerminal, Phase: c.phase, Identity: c.identity, Origin: c.origin, Terminal: c.receipt}, nil
	}
	return x.Status{}, c.failure(x.CodeResultUnknown, nil)
}

// Stop joins the serialized owner before releasing ports. A failed cleanup is
// retained for a later Stop, and never deletes broker settlement responsibility.
// origin: hot-update S7 plan §7 / §11.3
func (c *Client) Stop(deadline x.Deadline) error {
	c.stopAdmission.Store(true)
	c.cancelOnce.Do(func() { close(c.byteStop) })
	if err := c.acquire(deadline); err != nil {
		return err
	}
	defer c.release()
	if c.stopped {
		return nil
	}
	var errs []error
	if c.writer != nil {
		if err := stopClientWriter(c, deadline); err != nil {
			errs = append(errs, err)
		}
	}
	if c.reader != nil {
		if err := c.reader.Stop(deadline); err != nil {
			errs = append(errs, err)
		} else {
			c.reader = nil
		}
	}
	if c.cfg.Destination != nil {
		if err := c.cfg.Destination.Stop(deadline); err != nil {
			errs = append(errs, err)
		} else {
			c.cfg.Destination = nil
		}
	}
	if c.cfg.Source != nil {
		if err := c.cfg.Source.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		} else {
			c.cfg.Source = nil
		}
	}
	if c.dataOpen && c.writer == nil && c.reader == nil {
		// Close only after both leaf owners retire. A writer Stop error can
		// still own a sent publish and needs its original attachment to fence it.
		// origin: hot-update S7 plan §2.3 / §11.3
		if err := c.cfg.Transport.CloseData(c.identity, deadline); err != nil {
			errs = append(errs, err)
		} else {
			c.dataOpen = false
			c.js = nil
		}
	}
	if len(errs) == 0 && c.reservation != "" {
		// Failed cleanup retains its reservation identity. Releasing the bytes
		// before the last port stops would hide the real simultaneous peak.
		// origin: hot-update S7 plan §8.3 / §11.3
		if err := c.cfg.Capacity.Release(c.reservation); err != nil {
			errs = append(errs, err)
		} else {
			c.reservation = ""
		}
	}
	c.stopped = len(errs) == 0
	return errors.Join(errs...)
}

func (c *Client) openData(deadline x.Deadline) error {
	if c.dataOpen {
		return nil
	}
	limit, err := c.attemptDeadline(deadline)
	if err != nil {
		return err
	}
	js, err := c.cfg.Transport.OpenData(c.identity, limit)
	if js != nil {
		c.dataOpen, c.js = true, js
	}
	if err != nil {
		return c.failure(x.CodeOwnerRecovering, err)
	}
	c.dataOpen, c.js = true, js
	if js == nil {
		return c.failure(x.CodeResultUnknown, fmt.Errorf("data attachment returned no JetStream interface"))
	}
	return nil
}
