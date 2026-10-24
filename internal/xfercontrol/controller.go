package xfercontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// ErrNotOwner is a routing result, never a wire response. Broadcast adapters
// must remain silent when the local authority/tracker returns this sentinel;
// replying unknown here could beat the original owner's authoritative reply.
// origin: hot-update S7 plan §6.1 / T28
var ErrNotOwner = errors.New("xfercontrol: request belongs to another origin")

// Error preserves a stable transfer code without publishing a dependency's
// diagnostic text (which may contain paths or settlement credentials).
type Error struct {
	Code  string
	cause error
}

func (e *Error) Error() string        { return errorMessage(e.Code) }
func (e *Error) Unwrap() error        { return e.cause }
func (e *Error) TransferCode() string { return e.Code }

func refusal(code string) error { return &Error{Code: code} }

func errorMessage(code string) string {
	if code == x.CodeStorageHorizon {
		return code + ": required Duplicates >= 2228s and MaxAge = 0 or >= 2228s"
	}
	return code
}

func errorResponse(err error) (x.Response, error) {
	if errors.Is(err, ErrNotOwner) {
		return x.Response{}, err
	}
	code := x.CodeResultUnknown
	var coded interface{ TransferCode() string }
	if errors.As(err, &coded) {
		code = coded.TransferCode()
	}
	return x.Response{Code: code, Error: errorMessage(code)}, err
}

type controller struct {
	cfg     Config
	bootID  string
	mu      sync.Mutex
	stopped bool
	active  int
	drained chan struct{}
}

var _ Factory = New
var _ Controller = (*controller)(nil)

// New borrows its ports: stopping this control ingress joins its handlers but
// never stops the tracker, courier, bucket coordinator or shared transport.
// Their retained settlement responsibilities outlive a subscription generation.
// Authority.Check must distinguish create (ONLINE/current home), continuation
// (ONLINE or STALE/original identity) and read/settlement authority. Tracker.Status
// owns complete census and persisted-origin validation; a nonlocal owner returns
// ErrNotOwner. PermitSigner.Verify authenticates the permit, NOT its expiry.
// origin: hot-update S7 plan §6 / §8.2 / P16
func New(cfg Config) (Controller, error) {
	if cfg.Clock == nil || cfg.UTC == nil || cfg.Tracker == nil || cfg.Authority == nil ||
		cfg.Permits == nil || cfg.Buckets == nil || cfg.Transport == nil {
		return nil, fmt.Errorf("xfercontrol: incomplete control configuration")
	}
	boot, now, err := cfg.Clock()
	if err != nil || boot == "" || now <= 0 {
		return nil, fmt.Errorf("xfercontrol: unavailable admission clock")
	}
	drained := make(chan struct{})
	close(drained)
	return &controller{cfg: cfg, bootID: boot, drained: drained}, nil
}

// boundedDeadline takes the minimum once; every subsequent port receives this
// same absolute deadline, including status, capability, bucket and forwarding.
// No timeout wrapper can leave an unjoined goroutine performing late writes.
func (c *controller) boundedDeadline(d x.Deadline) (x.Deadline, error) {
	boot, now, err := c.cfg.Clock()
	if err != nil || boot != c.bootID || now <= 0 || now > math.MaxInt64-int64(5*time.Second) {
		return 0, refusal(x.CodeOwnerRecovering)
	}
	if d <= 0 || int64(d) <= now {
		return 0, &Error{Code: x.CodeBudgetExceeded, cause: controlbudget.ErrExpired}
	}
	return min(d, x.Deadline(now+int64(5*time.Second))), nil
}

func (c *controller) enter() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return refusal(x.CodeOwnerRecovering)
	}
	if c.active == 0 {
		c.drained = make(chan struct{})
	}
	c.active++
	return nil
}

func (c *controller) leave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	if c.active == 0 {
		close(c.drained)
	}
}

// Handle is a synchronous operation. A sent-unknown forward is returned to the
// caller for reconciliation; the controller never starts a hidden retry loop.
func (c *controller) Handle(req x.Request, who x.Principal, d x.Deadline) (x.Response, error) {
	d, err := c.boundedDeadline(d)
	if err != nil {
		return errorResponse(err)
	}
	if err = validateRequest(req, who); err != nil {
		return errorResponse(err)
	}
	if err = c.enter(); err != nil {
		return errorResponse(err)
	}
	defer c.leave()
	resp, err := c.handle(req, who, d)
	if err != nil {
		return errorResponse(err)
	}
	return resp, nil
}

func (c *controller) handle(req x.Request, who x.Principal, d x.Deadline) (x.Response, error) {
	switch req.Op {
	case x.OpProbe:
		if err := c.cfg.Authority.Check(req.Identity, x.AccessStatus, d); err != nil {
			return x.Response{}, err
		}
		return c.probe(req, d)
	case x.OpPermit:
		return c.permit(req, d)
	case x.OpPrepare:
		return c.prepare(req, d)
	case x.OpStatus, x.OpSource, x.OpReserve, x.OpCommit, x.OpTerminal, x.OpCancel, x.OpFinalize:
		return c.continueTransfer(req, who, d)
	case x.OpCensus, x.OpBucketConfig:
		// P00 Request/Response cannot carry a LedgerCensus or ConfigPatch.
		// These broker-only descriptors are consumed by directed adapters,
		// never acknowledged by pretending an empty public response is a census.
		return x.Response{}, refusal(x.CodePayloadUnsupported)
	default:
		return x.Response{}, refusal(x.CodePayloadUnsupported)
	}
}

// Stop closes admission before waiting, under the same lock as handler Add.
// Timeout retains the closed gate and all in-flight ownership; a later Stop
// can prove the drain, but cannot reopen this retired subscription generation.
// origin: hot-update S7 plan §8.2 / P16
func (c *controller) Stop(d x.Deadline) error {
	c.mu.Lock()
	c.stopped = true
	done := c.drained
	c.mu.Unlock()
	select {
	case <-done:
		return nil
	default:
	}
	d, err := c.boundedDeadline(d)
	if err != nil {
		return err
	}
	_, now, err := c.cfg.Clock()
	if err != nil {
		return refusal(x.CodeOwnerRecovering)
	}
	timer := time.NewTimer(time.Duration(int64(d) - now))
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return &Error{Code: x.CodeOwnerRecovering, cause: controlbudget.ErrExpired}
	}
}

// IntentDigest covers every field in the frozen declaration, including rate,
// force and geometry. The source seal may refine a pull without rewriting this
// original digest. Signers and worker adapters must use the same encoding.
// origin: hot-update S7 plan §6.1 / T26
func IntentDigest(intent x.Intent) string {
	// The public wire adds the version domain separator. A private digest here
	// made every real signed permit disagree with the controller's admission.
	// origin: hot-update S7 plan §6.1 / T26
	digest, _ := proto.TransferIntentDigest(proto.Intent(intent))
	return digest
}

// probe's transport adapter supplies authenticated capability evidence for the
// agent and every currently eligible publisher/config owner. It must recheck
// the membership set for each admission; the controller keeps no stale cache.
// A timeout is not an authenticated cap0 reply, and never selects legacy.
// origin: hot-update S7 plan §6.3 / §7 / P16
func (c *controller) probe(req x.Request, d x.Deadline) (x.Response, error) {
	resp, _, err := c.cfg.Transport.Request(req, d)
	if err != nil {
		return x.Response{}, err
	}
	if resp.Code != "" {
		return x.Response{}, refusal(resp.Code)
	}
	if resp.Error != "" || resp.Probe == nil || resp.Status != nil || resp.Permit != nil || resp.Grant != nil || resp.Receipt != nil {
		return x.Response{}, refusal(x.CodeResultUnknown)
	}
	p := resp.Probe
	if p.Code != "" {
		return x.Response{}, refusal(p.Code)
	}
	if p.Error != "" || !validToken(p.InstanceID) || (req.Identity.AgentInstance != "" && p.InstanceID != req.Identity.AgentInstance) {
		return x.Response{}, refusal(x.CodeResultUnknown)
	}
	// Only the requested read-only source inspection may introduce size
	// evidence. A missing answer cannot silently turn a small pull into OBJ;
	// the all-empty capability answer remains the explicit precreation cap0.
	// origin: hot-update S7 plan §6.1 / §7 / S7 internal review INTEG-O F4
	if req.Intent == nil && resp.Source != nil {
		return x.Response{}, refusal(x.CodeResultUnknown)
	}
	if req.Intent != nil && p.Version == x.Version && (resp.Source == nil || resp.Source.Size < 0 || resp.Source.Size > proto.XferMaxBytes || *resp.Source != (x.SourceSeal{Size: resp.Source.Size})) {
		return x.Response{}, refusal(x.CodePayloadUnsupported)
	}
	return resp, nil
}

func (c *controller) requireCapability(req x.Request, d x.Deadline) error {
	probe := x.Request{Op: x.OpProbe, CallID: req.CallID, Identity: req.Identity, Origin: req.Origin}
	resp, err := c.probe(probe, d)
	if err != nil {
		return err
	}
	p := resp.Probe
	if p.Version != x.Version || p.HistoryGuardVersion != x.Version || p.BucketConfigVersion != x.Version {
		return refusal(x.CodePayloadUnsupported)
	}
	// A default OBJ stream has no extra message ceiling. Only missing connection
	// evidence is unknown; probe and first-byte admission share that distinction.
	// origin: S7 internal review rv-jsnats F4
	if _, err := jsstream.TransferPayloadLimit(p.MaxPayload, p.MaxMsgSize); err != nil {
		return refusal(x.CodePayloadUnsupported)
	}
	if req.Intent.Tier == "b" {
		geometry, err := jsstream.TransferChunkSize(x.ObjectSpec{Identity: req.Identity}, p.MaxPayload, p.MaxMsgSize)
		if err != nil || req.Intent.ChunkSize > geometry {
			return refusal(x.CodePayloadUnsupported)
		}
	}
	return nil
}

func (c *controller) permit(req x.Request, d x.Deadline) (x.Response, error) {
	if err := c.cfg.Authority.Check(req.Identity, x.AccessCreate, d); err != nil {
		return x.Response{}, err
	}
	if err := c.requireCapability(req, d); err != nil {
		return x.Response{}, err
	}
	// Issue owns durable business-ID idempotence, including a lost first reply.
	// A per-controller map would disappear on hot import and let a new home
	// mint a second authority; no such cache is used here.
	var p x.CreationPermit
	var err error
	// The frozen Issue port predates explicit session/actor scope. A production
	// signer consumes the authenticated identity without a shared mutable binding.
	// origin: hot-update S7 plan §6.1 / T26
	if scoped, ok := c.cfg.Permits.(interface {
		IssueFor(x.Identity, x.Intent, x.Deadline) (x.CreationPermit, error)
	}); ok {
		p, err = scoped.IssueFor(req.Identity, *req.Intent, d)
	} else {
		p, err = c.cfg.Permits.Issue(*req.Intent, req.Identity.TransferID, req.Identity.AgentInstance, d)
	}
	if err != nil {
		return x.Response{}, err
	}
	if !identityMatches(req.Identity, p.Identity) || !validIdentity(p.Identity, true) ||
		!validOrigin(p.Origin) || p.Identity.IntentDigest != IntentDigest(*req.Intent) ||
		!validToken(p.PermitNonce) || p.Signature == "" || !permitGeometry(p, *req.Intent) {
		return x.Response{}, refusal(x.CodeIntentConflict)
	}
	if err := c.cfg.Permits.Verify(p); err != nil {
		return x.Response{}, err
	}
	if err := c.creationWindow(p); err != nil {
		return x.Response{}, err
	}
	return x.Response{Permit: &p}, nil
}

func (c *controller) creationWindow(p x.CreationPermit) error {
	now := c.cfg.UTC()
	// The five-minute ceiling is a creation fence, not a renewable lease.
	// Verify authenticates old permits too so retained work can outlive it.
	// origin: hot-update S7 plan §6.1 / T26 / M07
	if p.IssuedAt.IsZero() || now.Before(p.IssuedAt) || !now.Before(p.CreateUntil) ||
		!p.CreateUntil.After(p.IssuedAt) || p.CreateUntil.Sub(p.IssuedAt) > 5*time.Minute {
		return refusal(x.CodePermitExpired)
	}
	return nil
}

func permitGeometry(p x.CreationPermit, intent x.Intent) bool {
	return (intent.Tier == "a" && p.Identity.ObjectVersion == "") ||
		(intent.Tier == "b" && validToken(p.Identity.ObjectVersion))
}

func (c *controller) prepare(req x.Request, d x.Deadline) (x.Response, error) {
	p := *req.Permit
	if p.Identity != req.Identity || p.Origin != req.Origin ||
		p.Identity.IntentDigest != IntentDigest(*req.Intent) || !permitGeometry(p, *req.Intent) {
		return x.Response{}, refusal(x.CodeIntentConflict)
	}
	if err := c.cfg.Permits.Verify(p); err != nil {
		return x.Response{}, err
	}
	if err := c.cfg.Authority.Check(req.Identity, x.AccessStatus, d); err != nil {
		return x.Response{}, err
	}
	// Status authenticates original-origin ownership before any new-home check
	// or bucket mutation. Absence/error are deliberately different branches.
	// origin: hot-update S7 plan §6.1 / T26 / M07
	status, err := c.readStatus(req, d)
	if err != nil {
		return x.Response{}, err
	}
	if status.State != x.StatusUnknown {
		if status.State == x.StatusTerminal {
			return x.Response{Status: &status}, nil
		}
		if err := c.cfg.Authority.Check(req.Identity, x.AccessContinue, d); err != nil {
			return x.Response{}, err
		}
		if status.Phase != x.PhaseOpen {
			return x.Response{Status: &status}, nil
		}
		// Open retains an admitted but unacknowledged preparation. Re-forward
		// only that same permit/CallID to the original agent instance.
	} else {
		if err := c.creationWindow(p); err != nil {
			return x.Response{}, err
		}
		if err := c.cfg.Authority.Check(req.Identity, x.AccessCreate, d); err != nil {
			return x.Response{}, err
		}
		if err := c.requireCapability(req, d); err != nil {
			return x.Response{}, err
		}
	}
	// Tier A carries its authenticated inline bytes on the prepare request
	// (Request.Inline, INTEG-D carrier amendment): a push carries exactly the
	// intended bytes and a pull none. Admission never creates a zero-byte or
	// substituted inline worker; the bytes reach the agent only by forwarding.
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A inline path
	if req.Intent.Tier == "a" {
		digest := sha256.Sum256(req.Inline)
		if (req.Intent.Verb == "push" && (int64(len(req.Inline)) != req.Intent.Size || hex.EncodeToString(digest[:]) != req.Intent.SHA256)) ||
			(req.Intent.Verb == "pull" && len(req.Inline) != 0) {
			return x.Response{}, refusal(x.CodeIntentConflict)
		}
	} else if len(req.Inline) != 0 {
		return x.Response{}, refusal(x.CodeIntentConflict)
	}
	if status.State == x.StatusUnknown {
		status, err = c.cfg.Tracker.Admit(p, *req.Intent, d)
		if err != nil {
			return x.Response{}, err
		}
		if err = validateStatus(status, req); err != nil || status.State != x.StatusLive {
			return x.Response{}, refusal(x.CodeResultUnknown)
		}
		if status.Phase != x.PhaseOpen {
			return x.Response{Status: &status}, nil
		}
	}
	// Tier A has no OBJ: neither bucket configuration nor its horizon applies.
	if req.Intent.Tier == "a" {
		return c.forward(req, d)
	}
	// Configuration policy/disk limits belong to the coordinator. A zero size
	// patch asks it to retain its sizing policy, not to set MaxBytes to zero.
	// No first byte is forwarded until its full readback proves the horizon.
	horizon := proto.XferTierBMaxBudget + 120*time.Second
	seal, err := c.cfg.Buckets.Apply(x.ConfigPatch{
		Bucket: proto.XferBucketName(req.Identity.SID), Duplicates: horizon, RecoveryHorizon: horizon,
	}, d)
	if err != nil {
		return x.Response{}, err
	}
	if seal.Bucket != proto.XferBucketName(req.Identity.SID) || seal.Stream != "OBJ_"+seal.Bucket ||
		seal.StreamCreated == "" || seal.ConfigDigest == "" || seal.MaxBytes <= 0 ||
		seal.Duplicates < horizon || (seal.MaxAge != 0 && seal.MaxAge < horizon) {
		return x.Response{}, refusal(x.CodeStorageHorizon)
	}
	// The coordinator owns stored-message overhead reservations. Even the raw
	// payload must fit its confirmed ceiling; a positive but smaller stream
	// limit is not first-byte permission. Writers validate actual header size;
	// admission also reserves the encoded worst-case headers before forwarding.
	// origin: hot-update S7 plan §2.1 / P16
	if seal.MaxBytes < req.Intent.Size {
		return x.Response{}, refusal(x.CodeCapacityExceeded)
	}
	// origin: hot-update S7 plan §2.1 / T09; S7 internal review rv-jsnats F4
	geometry, err := jsstream.TransferChunkSize(x.ObjectSpec{Identity: req.Identity, Bucket: seal.Bucket}, seal.MaxPayload, seal.MaxMsgSize)
	if err != nil || req.Intent.ChunkSize > geometry {
		return x.Response{}, refusal(x.CodePayloadUnsupported)
	}
	return c.forward(req, d)
}

func identityMatches(want, got x.Identity) bool {
	if want.EntryNonce != "" {
		return want == got
	}
	return want.SID == got.SID && want.NID == got.NID && want.ActorFP == got.ActorFP && want.TransferID == got.TransferID &&
		(want.AgentInstance == "" || want.AgentInstance == got.AgentInstance) &&
		(want.EntryNonce == "" || want.EntryNonce == got.EntryNonce) &&
		(want.IntentDigest == "" || want.IntentDigest == got.IntentDigest) &&
		(want.ObjectVersion == "" || want.ObjectVersion == got.ObjectVersion)
}

func terminalID(id x.Identity) string {
	digest := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	return hex.EncodeToString(digest[:])
}

func validReceipt(r x.TerminalReceipt, id x.Identity) bool {
	return r.TerminalID == terminalID(id) && (r.Kind == "complete" || r.Kind == "failed") &&
		r.CanonicalDigest != "" && r.ReqID != "" && r.HistoryStream != "" && r.HistorySeq > 0 &&
		r.Bytes >= 0 && (r.Kind != "complete" || !r.OutcomeUnknown)
}

// validateStatus cannot manufacture census evidence. The tracker/resolver port
// must return unknown only for a ready complete authority set; an error, empty
// value, mismatched boot or remote silence must never become that value here.
// origin: hot-update S7 plan §6.1 / T28
func validateStatus(s x.Status, req x.Request) error {
	if s.Code != "" {
		return refusal(s.Code)
	}
	if s.Error != "" {
		return refusal(x.CodeResultUnknown)
	}
	if !identityMatches(req.Identity, s.Identity) || !validOrigin(s.Origin) ||
		(req.Origin != (x.Origin{}) && req.Origin != s.Origin) {
		return refusal(x.CodeResultUnknown)
	}
	switch s.State {
	case x.StatusUnknown:
		if s.Phase != "" || s.Terminal != nil || s.CleanupPending || s.DurableOffset != 0 ||
			s.ChunkIndex != 0 || s.LastConsumedStreamSeq != 0 {
			return refusal(x.CodeResultUnknown)
		}
	case x.StatusLive:
		if !validIdentity(s.Identity, true) || !livePhase(s.Phase) || s.Terminal != nil || s.DurableOffset < 0 {
			return refusal(x.CodeResultUnknown)
		}
	case x.StatusTerminal:
		if !validIdentity(s.Identity, true) || s.Terminal == nil || !validReceipt(*s.Terminal, s.Identity) ||
			(s.Phase != x.PhaseHistoryConfirmed && s.Phase != x.PhaseCleanupPending && s.Phase != x.PhaseSettled) {
			return refusal(x.CodeTerminalConflict)
		}
	default:
		return refusal(x.CodeResultUnknown)
	}
	return nil
}

func livePhase(p x.Phase) bool {
	switch p {
	case x.PhaseOpen, x.PhasePrepared, x.PhaseSending, x.PhaseReceiving, x.PhaseCommitReserved,
		x.PhaseDecided, x.PhaseTerminalPending, x.PhaseCleanupPending:
		return true
	case x.PhaseHistoryConfirmed, x.PhaseSettled:
		// Exact history makes the entry terminal for status, not live.
		return false
	default:
		return false
	}
}

func (c *controller) readStatus(req x.Request, d x.Deadline) (x.Status, error) {
	// ControlConfig has no all-origin resolver/census port. A local empty
	// tracker cannot prove global absence for the standalone status command.
	// P23 must resolve a verified original origin before entering this leaf.
	// origin: hot-update S7 plan §6.1 / T28
	if req.Origin == (x.Origin{}) {
		return x.Status{}, refusal(x.CodeResultUnknown)
	}
	s, err := c.cfg.Tracker.Status(x.StatusRequest{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin}, d)
	if err != nil {
		return x.Status{}, err
	}
	// A closed receiver gate with an unresolved primitive is retained live
	// responsibility. It must still accept the original receiver's late terminal;
	// only source/grant admission treats result-unknown as a refusal.
	// origin: hot-update S7 plan §4 / T12 / T39
	if s.State == x.StatusLive && s.Code == x.CodeResultUnknown && s.Error == "" &&
		(req.Op == x.OpStatus || req.Op == x.OpTerminal || req.Op == x.OpFinalize || req.Op == x.OpCancel) {
		proof := s
		proof.Code = ""
		return s, validateStatus(proof, req)
	}
	return s, validateStatus(s, req)
}

func (c *controller) continueTransfer(req x.Request, who x.Principal, d x.Deadline) (x.Response, error) {
	access := x.AccessContinue
	if req.Op == x.OpStatus {
		access = x.AccessStatus
	} else if req.Op == x.OpTerminal || req.Op == x.OpFinalize || req.Op == x.OpCancel || who.Role == x.RoleSettlement {
		access = x.AccessSettle
	}
	if err := c.cfg.Authority.Check(req.Identity, access, d); err != nil {
		return x.Response{}, err
	}
	s, err := c.readStatus(req, d)
	if err != nil {
		return x.Response{}, err
	}
	if req.Op == x.OpStatus {
		return x.Response{Status: &s}, nil
	}
	if s.State == x.StatusUnknown {
		return x.Response{}, refusal(x.CodeUnknown)
	}
	// Resolve the original owner even for mutations whose tracker method lacks
	// an Origin argument. Never send SourceSeal/Finish to a current-home entry
	// just because its business ID happens to match.
	switch req.Op {
	case x.OpSource:
		s, err = c.cfg.Tracker.SourceSeal(req.Identity, *req.Source, d)
	case x.OpReserve, x.OpCommit:
		grant, grantErr := c.cfg.Tracker.Reserve(req.Identity, *req.Reserve, d)
		if grantErr != nil {
			return x.Response{}, grantErr
		}
		if grant.Identity != req.Identity || grant.ReceiverInstance != req.Reserve.ReceiverInstance ||
			!validToken(grant.GrantNonce) || grant.TerminalID != terminalID(req.Identity) {
			return x.Response{}, refusal(x.CodeIntentConflict)
		}
		// Commit is the legacy carrier for the same durable reservation. Its
		// adapter must deliver this grant to the original receiver; it cannot
		// execute an ungranted legacy primitive as a fallback.
		return x.Response{Grant: &grant}, nil
	case x.OpTerminal, x.OpFinalize:
		receipt, finishErr := c.cfg.Tracker.Finish(req.Identity, *req.Outcome, d)
		if finishErr != nil {
			return x.Response{}, finishErr
		}
		if !validReceipt(receipt, req.Identity) || !receiptMatchesOutcome(receipt, *req.Outcome) {
			return x.Response{}, refusal(x.CodeTerminalConflict)
		}
		return x.Response{Receipt: &receipt}, nil
	case x.OpCancel:
		s, err = c.cfg.Tracker.Cancel(req.Identity, *req.Cancel, d)
	case x.OpProbe, x.OpPermit, x.OpPrepare, x.OpStatus, x.OpCensus, x.OpBucketConfig:
		// Read-only and admission verbs are dispatched before continuation.
		return x.Response{}, refusal(x.CodePayloadUnsupported)
	default:
		return x.Response{}, refusal(x.CodePayloadUnsupported)
	}
	if err != nil {
		return x.Response{}, err
	}
	if err := validateStatus(s, req); err != nil {
		return x.Response{}, err
	}
	return x.Response{Status: &s}, nil
}

// A well-shaped receipt for the opposite outcome is still not an ACK. The
// controller does not repair canonical decisions; it retains the conflict so
// the original receiver keeps its settlement responsibility.
// origin: hot-update S7 plan §4 / §6.2 / P16
func receiptMatchesOutcome(r x.TerminalReceipt, o x.ReceiverOutcome) bool {
	if r.OutcomeUnknown {
		return false // Only the explicit session-removal barrier may abandon.
	}
	if r.Kind == "complete" {
		return o.Outcome == x.OutcomeLanded && o.ParentSynced && r.Bytes == o.Bytes && r.SHA256 == o.SHA256
	}
	return o.Outcome == x.OutcomeNotEntered
}

func (c *controller) forward(req x.Request, d x.Deadline) (x.Response, error) {
	resp, attempt, err := c.cfg.Transport.Request(req, d)
	if err != nil {
		if attempt.Sent {
			return x.Response{}, &Error{Code: x.CodeReconcileRequired, cause: err}
		}
		return x.Response{}, err
	}
	if resp.Code != "" {
		return x.Response{}, refusal(resp.Code)
	}
	if resp.Error != "" || resp.Status == nil || resp.Permit != nil || resp.Grant != nil || resp.Receipt != nil || resp.Probe != nil {
		return x.Response{}, refusal(x.CodeResultUnknown)
	}
	if err := validateStatus(*resp.Status, req); err != nil {
		return x.Response{}, err
	}
	if resp.Status.State == x.StatusUnknown {
		return x.Response{}, refusal(x.CodeResultUnknown)
	}
	return resp, nil
}
