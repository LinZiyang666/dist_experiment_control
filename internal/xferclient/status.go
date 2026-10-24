package xferclient

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// QueryStatus is read-only even when Origin is empty and the transport must
// resolve it. An unavailable or malformed reply never proves state=unknown.
// origin: hot-update S7 plan §6.1 / §7 / T33
func QueryStatus(transport x.Transport, request x.StatusRequest, deadline x.Deadline) (x.Status, error) {
	if transport == nil || request.CallID == "" || request.Identity.SID == "" || request.Identity.TransferID == "" {
		return x.Status{}, clientError(x.CodePayloadUnsupported, request.Identity, x.PhaseOpen, false, errors.New("invalid status query"))
	}
	limit, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return x.Status{}, clientError(x.CodeResultUnknown, request.Identity, x.PhaseOpen, true, err)
	}
	deadline = minDeadline(deadline, limit)
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return x.Status{}, clientError(x.CodeResultUnknown, request.Identity, x.PhaseOpen, true, err)
	}
	response, _, err := transport.Request(x.Request{Op: x.OpStatus, CallID: request.CallID, Identity: request.Identity, Origin: request.Origin}, deadline)
	if err != nil {
		return x.Status{}, clientError(x.CodeResultUnknown, request.Identity, x.PhaseOpen, true, err)
	}
	if response.Code != "" || response.Error != "" {
		code := response.Code
		if code == "" {
			code = x.CodeResultUnknown
		}
		return x.Status{}, clientError(code, request.Identity, x.PhaseOpen, true, errors.New(response.Error))
	}
	if response.Status == nil {
		return x.Status{}, clientError(x.CodeResultUnknown, request.Identity, x.PhaseOpen, true, errors.New("missing authoritative status"))
	}
	status := *response.Status
	if err := validateStatus(status, request.Identity, request.Origin); err != nil {
		return x.Status{}, err
	}
	return status, nil
}

func sameScope(want, got x.Identity) bool {
	return want.SID == got.SID && want.NID == got.NID && want.ActorFP == got.ActorFP &&
		want.TransferID == got.TransferID && want.IntentDigest == got.IntentDigest &&
		(want.AgentInstance == "" || want.AgentInstance == got.AgentInstance)
}

func validOrigin(origin x.Origin) bool {
	return origin.BrokerID != "" && origin.InstanceID != "" && origin.BootID != ""
}

func validPhase(phase x.Phase) bool {
	switch phase {
	case x.PhaseOpen, x.PhasePrepared, x.PhaseSending, x.PhaseReceiving, x.PhaseCommitReserved,
		x.PhaseDecided, x.PhaseTerminalPending, x.PhaseHistoryConfirmed, x.PhaseCleanupPending, x.PhaseSettled:
		return true
	default:
		return false
	}
}

// ValidateStatus checks discovered scope and physical terminal proof for native
// transports as well as the invocation. Read-only CLI queries must not bypass
// the same original-owner checks merely because they perform no byte work.
// origin: hot-update S7 plan §6.1 / T33 / INTEG-D
func ValidateStatus(status x.Status, identity x.Identity, origin x.Origin) error {
	return validateStatus(status, identity, origin)
}

func validateStatus(status x.Status, identity x.Identity, origin x.Origin) error {
	refuse := func(code, message string) error {
		return clientError(code, identity, status.Phase, true, errors.New(message))
	}
	// A standalone query may know only SID/transfer ID/node. Every supplied
	// binding must match; discovered bindings must still name an original owner.
	if status.Identity.SID != identity.SID || status.Identity.TransferID != identity.TransferID ||
		identity.NID != "" && status.Identity.NID != identity.NID ||
		identity.ActorFP != "" && status.Identity.ActorFP != identity.ActorFP ||
		identity.AgentInstance != "" && status.Identity.AgentInstance != identity.AgentInstance ||
		identity.IntentDigest != "" && status.Identity.IntentDigest != identity.IntentDigest ||
		identity.EntryNonce != "" && status.Identity.EntryNonce != identity.EntryNonce ||
		identity.ObjectVersion != "" && status.Identity.ObjectVersion != identity.ObjectVersion {
		return refuse(x.CodeIntentConflict, "status identity does not match the original invocation")
	}
	if !validOrigin(status.Origin) || validOrigin(origin) && status.Origin != origin {
		return refuse(x.CodeResultUnknown, "status has no matching original origin")
	}
	if status.Code != "" && status.Code != x.CodeUnknown || status.Error != "" {
		code := status.Code
		if code == "" {
			code = x.CodeResultUnknown
		}
		return refuse(code, status.Error)
	}
	switch status.State {
	// States are affirmative evidence from the original owner. In particular,
	// neither an omitted enum nor the first nonowner reply proves absence.
	// origin: hot-update S7 plan §6.1 / T33
	case x.StatusUnknown:
		if status.Terminal != nil {
			return refuse(x.CodeTerminalConflict, "absence carries a terminal")
		}
	case x.StatusLive:
		if !validPhase(status.Phase) || status.Terminal != nil || status.Identity.EntryNonce == "" {
			return refuse(x.CodeResultUnknown, "invalid live responsibility")
		}
	case x.StatusTerminal:
		if !validPhase(status.Phase) || status.Terminal == nil {
			return refuse(x.CodeResultUnknown, "terminal status has no exact receipt")
		}
		if err := validateReceipt(*status.Terminal); err != nil {
			return refuse(x.CodeTerminalConflict, err.Error())
		}
		if status.Identity.EntryNonce == "" || status.Terminal.TerminalID != terminalSlot(status.Identity) {
			return refuse(x.CodeTerminalConflict, "terminal status has a different entry slot")
		}
	default:
		return refuse(x.CodeResultUnknown, "invalid status state")
	}
	return nil
}

func validateReceipt(receipt x.TerminalReceipt) error {
	// A nonempty terminal kind is insufficient: exact canonical identity and
	// a physical history position are the publication owner's acknowledgement.
	// origin: hot-update S7 plan §5.2 / §7 / T33
	if receipt.TerminalID == "" || !validSHA(receipt.CanonicalDigest) || receipt.ReqID == "" || receipt.HistoryStream == "" || receipt.HistorySeq == 0 {
		return errors.New("terminal lacks exact physical history proof")
	}
	if receipt.Kind != "complete" && receipt.Kind != "failed" || receipt.Bytes < 0 {
		return errors.New("invalid terminal kind or byte count")
	}
	if receipt.Kind == "complete" && (receipt.OutcomeUnknown || receipt.Code != "" || !validSHA(receipt.SHA256)) {
		return errors.New("complete cannot describe an unknown or failed outcome")
	}
	return nil
}

func (c *Client) read(request x.Request, deadline x.Deadline) (x.Response, error) {
	// Read-only probes/status can retry without a mutation receipt, but their
	// waits still share the single operation deadline rather than the byte TTL.
	// origin: hot-update S7 plan §1.2 / §7
	operationDeadline, err := c.attemptDeadline(deadline)
	if err != nil {
		return x.Response{}, err
	}
	deadline = operationDeadline
	for {
		limit, err := c.attemptDeadline(deadline)
		if err != nil {
			return x.Response{}, err
		}
		_, c.lastRequestNS, err = c.cfg.Clock()
		if err != nil {
			return x.Response{}, c.failure(x.CodeResultUnknown, err)
		}
		response, attempt, err := c.cfg.Transport.Request(request, limit)
		c.observeAttempt(attempt)
		if err == nil {
			if response.Code != "" || response.Error != "" {
				return x.Response{}, c.failure(response.Code, errors.New(response.Error))
			}
			return response, nil
		}
		if err := c.wait(deadline, 500*time.Millisecond, false); err != nil {
			return x.Response{}, err
		}
	}
}

// mutate alone can replay a control mutation. Keeping the sent request before
// any status IO prevents a timeout, cancellation or second Run from losing it.
// origin: hot-update S7 plan §7 / T33
func (c *Client) mutate(request x.Request, deadline x.Deadline) (x.Response, error) {
	operationDeadline, err := c.attemptDeadline(deadline)
	if err != nil {
		return x.Response{}, err
	}
	deadline = operationDeadline
	for {
		if c.pending != nil {
			if c.pending.Op != request.Op || c.pending.CallID != request.CallID {
				return x.Response{}, c.failure(x.CodeReconcileRequired, errors.New("a different mutation remains unresolved"))
			}
			if _, err := c.reconcile(deadline); err != nil {
				return x.Response{}, err
			}
			if c.receipt != nil {
				c.pending = nil
				return x.Response{}, nil
			}
			request = *c.pending
		}
		// Reconciliation remains read-only after cancellation. It must not
		// turn into another prepare, source publication or commit admission.
		// Settlement of a retained outcome still uses the original finalize.
		// origin: hot-update S7 plan §7 / T33
		if byteMutation(request.Op) && (c.cancelReason.Load() != nil || c.stopAdmission.Load()) {
			return x.Response{}, c.failure(x.CodeCanceled, nil)
		}
		limit, err := c.attemptDeadline(deadline)
		if err != nil {
			return x.Response{}, err
		}
		_, c.lastRequestNS, err = c.cfg.Clock()
		if err != nil {
			return x.Response{}, c.failure(x.CodeResultUnknown, err)
		}
		// A replay re-sends a mutation whose earlier attempt is still sent-unknown.
		replay := c.pending != nil
		response, attempt, err := c.cfg.Transport.Request(request, limit)
		c.observeAttempt(attempt)
		if err != nil {
			if attempt.Sent {
				saved := request
				c.pending = &saved
				continue
			}
			if waitErr := c.wait(deadline, 500*time.Millisecond, false); waitErr != nil {
				return x.Response{}, waitErr
			}
			continue
		}
		if response.Code != "" || response.Error != "" {
			// The broker's typed reply confirms only ctl delivery. Its controller
			// answers transfer_reconcile_required when its own forward to the
			// agent stayed sent-unknown (a parked or reconnecting agent), and
			// result_unknown when it cannot tell what took effect: the same duty
			// as a lost reply here. reconcile_required is the engine's internal
			// control value (plan §7), never a final user-facing result. Keep the
			// mutation and reconcile its original owner before replaying it; Run
			// continues within the original deadline and then exits 74.
			// origin: S7 internal review FIX-TIERA2 F1 / hot-update S7 plan §7 / T46
			// An accepted prepare already owns the transfer. Its FIRST commit
			// can reach the receiver's grant gate during Freeze, after the broker
			// reserved the Grant. That deferral cannot end the original invocation:
			// retain this request and reconcile before replaying its same CallID.
			// Before preparation, a first unready-owner refusal still exits 75.
			// origin: S7 internal review OWNER-RECOVERING-RETRY F1 / hot-update S7 plan §4 / §7
			if attempt.Sent && (sentResultUnknown(response.Code, replay) || c.prepared && response.Code == x.CodeOwnerRecovering) {
				saved := request
				c.pending = &saved
				return x.Response{}, c.failure(x.CodeResultUnknown, errors.New(response.Error))
			}
			// A refusal to a replay does not retroactively prove that its earlier
			// sent attempt had no effect. Retain that pending request.
			// A receiver closes its gate after landing too. Until the original
			// owner supplies an exact terminal, a repeated commit's closed-gate
			// refusal cannot decide cancellation. Keep the same sent commit so
			// Run continues status-first through publication and retirement.
			// origin: S7 internal review FIX-CUT F1 / hot-update S7 plan §4 / §7
			if response.Code == x.CodeCanceled && c.pending != nil && c.pending.Op == x.OpCommit {
				return x.Response{}, c.failure(x.CodeResultUnknown, errors.New(response.Error))
			}
			return x.Response{}, c.failure(response.Code, errors.New(response.Error))
		}
		if response.Permit != nil && (!sameScope(c.identity, response.Permit.Identity) || !validOrigin(response.Permit.Origin) ||
			response.Permit.Identity.EntryNonce == "" || (c.cfg.Intent.Tier == "b") != (response.Permit.Identity.ObjectVersion != "") || response.Permit.PermitNonce == "" || response.Permit.Signature == "") {
			saved := request
			c.pending = &saved
			return x.Response{}, c.failure(x.CodeIntentConflict, errors.New("permit reply lost the original binding"))
		}
		if response.Grant != nil && (request.Reserve == nil || response.Grant.Identity != c.identity || response.Grant.GrantNonce == "" ||
			response.Grant.TerminalID != terminalSlot(c.identity) || response.Grant.ReceiverInstance != request.Reserve.ReceiverInstance) {
			// An ACK for another receiver/slot does not discharge this request.
			// Preserve it so cancellation and Stop retain the original evidence.
			// origin: hot-update S7 plan §4 / §7 / T33
			saved := request
			c.pending = &saved
			return x.Response{}, c.failure(x.CodeIntentConflict, errors.New("grant reply lost the original receiver binding"))
		}
		if response.Status != nil {
			if err := c.acceptStatus(*response.Status); err != nil {
				saved := request
				c.pending = &saved
				return x.Response{}, err
			}
		}
		if response.Receipt != nil {
			if err := c.acceptReceipt(*response.Receipt); err != nil {
				saved := request
				c.pending = &saved
				return x.Response{}, err
			}
		}
		if !responseEvidence(request.Op, response) {
			saved := request
			c.pending = &saved
			if waitErr := c.wait(deadline, 500*time.Millisecond, false); waitErr != nil {
				return x.Response{}, waitErr
			}
			continue
		}
		if request.Op == x.OpPrepare {
			if err := c.acceptPrepared(response); err != nil {
				saved := request
				c.pending = &saved
				return x.Response{}, err
			}
		}
		c.pending = nil
		return response, nil
	}
}

func responseEvidence(op x.Operation, response x.Response) bool {
	switch op {
	case x.OpPermit:
		return response.Permit != nil
	case x.OpPrepare, x.OpSource, x.OpCancel:
		return response.Status != nil
	case x.OpReserve:
		return response.Grant != nil
	case x.OpCommit:
		return response.Status != nil || response.Receipt != nil
	case x.OpTerminal, x.OpFinalize:
		return response.Receipt != nil || response.Status != nil && response.Status.State == x.StatusTerminal
	case x.OpProbe, x.OpStatus, x.OpCensus, x.OpBucketConfig:
		return false
	default:
		return false
	}
}

func (c *Client) reconcile(deadline x.Deadline) (x.Status, error) {
	response, err := c.read(x.Request{Op: x.OpStatus, CallID: c.callID(x.OpStatus), Identity: c.identity, Origin: c.origin}, deadline)
	if err != nil {
		return x.Status{}, reconcileError(c, err)
	}
	if response.Status == nil {
		return x.Status{}, c.failure(x.CodeResultUnknown, errors.New("status unavailable"))
	}
	status := *response.Status
	if err := c.acceptStatus(status); err != nil {
		return x.Status{}, reconcileError(c, err)
	}
	if c.receipt != nil || c.pending == nil {
		return status, nil
	}
	switch status.State {
	case x.StatusUnknown:
		// Only a still-valid, original permit permits retrying the original
		// prepare after affirmative original-owner absence. Unknown never
		// authorizes a new permit or another commit/finalize.
		// origin: hot-update S7 plan §6.1 / §7 / T33
		if c.pending.Op != x.OpPrepare || c.permit == nil || !c.cfg.UTC().Before(c.permit.CreateUntil) || status.Origin != c.permit.Origin {
			return x.Status{}, c.failure(x.CodeResultUnknown, errors.New("original responsibility cannot be proved"))
		}
	case x.StatusLive:
		if c.pending.Op == x.OpPermit {
			// A lost permit reply lacks Origin in P00's ProbeResult. Only a
			// live original-owner response can fix its routing; an absence
			// response must not send a new permit to today's home.
			c.origin = status.Origin
			c.identity = status.Identity
			c.pending.Origin = status.Origin
		}
	case x.StatusTerminal:
		return status, nil
	default:
		return x.Status{}, c.failure(x.CodeResultUnknown, nil)
	}
	return status, nil
}

// An unready owner may be retried by a read-only query. The active transfer
// command still has no terminal proof, so it must preserve exit 74 after send.
// Permission refusals retain their independent exit 77 classification.
// origin: hot-update S7 plan §6.1 / §7 / T33
func reconcileError(c *Client, err error) error {
	if unreadyOwner(ErrorCode(err)) {
		return c.failure(x.CodeResultUnknown, err)
	}
	return err
}

func unreadyOwner(code string) bool {
	switch code {
	case x.CodeOwnerRecovering, x.CodeSessionDeleting, x.CodeUnknown:
		return true
	default:
		return false
	}
}

// sentResultUnknown names the typed replies after which a sent mutation's
// effect is still unknown. An unready owner's refusal of a FIRST attempt is a
// definitive, retryable no (exit 75): nothing earlier was sent. Refusing a
// replay proves nothing about the earlier sent attempt, so the same unready-
// owner codes keep that original pending, exactly as reconcileError does for
// a status read. Ownership, intent and permission refusals stay final.
// origin: S7 internal review FIX-TIERA2 F1 / hot-update S7 plan §7 / T46
func sentResultUnknown(code string, replay bool) bool {
	switch code {
	case x.CodeReconcileRequired, x.CodeResultUnknown:
		return true
	default:
		return replay && unreadyOwner(code)
	}
}

func byteMutation(op x.Operation) bool {
	switch op {
	case x.OpPermit, x.OpPrepare, x.OpSource, x.OpReserve, x.OpCommit:
		return true
	case x.OpProbe, x.OpStatus, x.OpTerminal, x.OpCancel, x.OpFinalize, x.OpCensus, x.OpBucketConfig:
		return false
	default:
		return true
	}
}

func (c *Client) acceptStatus(status x.Status) error {
	if err := validateStatus(status, c.identity, c.origin); err != nil {
		return err
	}
	// Read-only status also serves inline transfers with no object version.
	// This capable byte engine knows its tier and requires its fixed B NUID.
	if status.State != x.StatusUnknown && c.cfg.Intent.Tier == "b" && status.Identity.ObjectVersion == "" {
		return c.failure(x.CodeObjectVersionMismatch, errors.New("tier B status lost its original object version"))
	}
	if status.State == x.StatusTerminal {
		if status.Identity.EntryNonce == "" || c.cfg.Intent.Tier == "b" && status.Identity.ObjectVersion == "" {
			return c.failure(x.CodeResultUnknown, errors.New("terminal status lacks the original entry binding"))
		}
		c.identity, c.origin = status.Identity, status.Origin
		return c.acceptReceipt(*status.Terminal)
	}
	if status.State == x.StatusUnknown {
		return nil
	}
	c.phase = status.Phase
	budget := status.Budget
	if budget.StartedAt.IsZero() || budget.Size < 0 || budget.RateLimitBPS != c.cfg.Intent.RateLimitBPS ||
		budget.EffectiveBudgetNS <= 0 || budget.RemainingNS <= 0 || budget.RemainingNS > budget.EffectiveBudgetNS {
		return c.failure(x.CodeResultUnknown, errors.New("live status has no bounded original budget"))
	}
	if !c.brokerStarted.IsZero() && !budget.StartedAt.Equal(c.brokerStarted) {
		return c.failure(x.CodeIntentConflict, errors.New("original start changed"))
	}
	allowed, err := proto.TransferBudget(c.cfg.Intent.Tier, budget.Size, budget.RateLimitBPS)
	if err != nil {
		return c.failure(x.CodeBudgetExceeded, err)
	}
	if !budget.SourceSealed && c.cfg.Intent.Verb == "pull" && c.cfg.Intent.Tier == "b" {
		allowed = proto.XferTierBMaxBudget
	}
	if budget.EffectiveBudgetNS > int64(allowed) || budget.SourceSealed && c.cfg.Intent.Verb == "push" && budget.Size != c.cfg.Intent.Size {
		return c.failure(x.CodeIntentConflict, errors.New("budget does not match the sealed source"))
	}
	_, now, err := c.cfg.Clock()
	if err != nil {
		return c.failure(x.CodeResultUnknown, err)
	}
	remaining := budget.RemainingNS - max(int64(0), now-c.lastRequestNS)
	// Cross-host remaining time loses this entire request's elapsed time. It
	// never substitutes the peer's BOOTTIME or enlarges our original deadline.
	// origin: hot-update S7 plan §1.2 / T33
	if remaining <= 0 {
		return c.failure(x.CodeBudgetExceeded, nil)
	}
	c.deadline = minDeadline(c.deadline, x.Deadline(now)+x.Deadline(remaining))
	c.brokerStarted, c.budget = budget.StartedAt, budget
	return nil
}

func (c *Client) acceptReceipt(receipt x.TerminalReceipt) error {
	if err := validateReceipt(receipt); err != nil {
		return c.failure(x.CodeTerminalConflict, err)
	}
	if c.receipt != nil && *c.receipt != receipt {
		return c.failure(x.CodeTerminalConflict, errors.New("terminal receipt changed"))
	}
	if c.identity.EntryNonce == "" || receipt.TerminalID != terminalSlot(c.identity) {
		return c.failure(x.CodeTerminalConflict, errors.New("terminal does not match the original entry slot"))
	}
	if c.grant != nil && receipt.TerminalID != c.grant.TerminalID {
		return c.failure(x.CodeTerminalConflict, errors.New("terminal does not match the reserved slot"))
	}
	// Operator abandonment explicitly preserves outcome uncertainty. Every
	// other failed receipt contradicts a locally witnessed landed primitive.
	// origin: hot-update S7 plan §4 / §7 / lead LQ1 / T33
	if c.outcome != nil && c.outcome.Outcome == x.OutcomeLanded && receipt.Kind == "failed" && !receipt.OutcomeUnknown {
		return c.failure(x.CodeTerminalConflict, errors.New("failed receipt contradicts the landed primitive"))
	}
	if receipt.Kind == "complete" {
		if c.cfg.Intent.Verb == "pull" && (c.outcome == nil || c.outcome.Outcome != x.OutcomeLanded || !c.outcome.ParentSynced) {
			return c.failure(x.CodeTerminalConflict, errors.New("complete has no local durable landed outcome"))
		}
		size, digest := c.cfg.Intent.Size, c.cfg.Intent.SHA256
		if c.spec != nil && c.spec.SHA256 != "" {
			size, digest = c.spec.Size, c.spec.SHA256
		}
		if receipt.Bytes != size || receipt.SHA256 != digest {
			return c.failure(x.CodeTerminalConflict, errors.New("terminal source digest or size differs"))
		}
		if c.outcome != nil && c.outcome.Code != "" {
			return c.failure(x.CodeDurabilityUnknown, errors.New("local landed outcome remains unresolved"))
		}
	}
	copy := receipt
	c.receipt, c.phase = &copy, x.PhaseHistoryConfirmed
	return nil
}

func terminalSlot(identity x.Identity) string {
	// The slot is independent of call IDs and content ReqID. Deriving it from
	// the entry nonce also excludes a replacement entry with the same user ID.
	// origin: hot-update S7 plan §4 / §7 / T33
	digest := sha256.Sum256([]byte("xfer-terminal-v1\n" + identity.SID + "\n" + identity.TransferID + "\n" + identity.EntryNonce))
	return hex.EncodeToString(digest[:])
}

func (c *Client) resultError(receipt x.TerminalReceipt) error {
	if receipt.OutcomeUnknown {
		return clientError(x.CodeResultUnknown, c.identity, c.phase, true, nil)
	}
	if receipt.Kind == "complete" {
		return nil
	}
	code := receipt.Code
	if code == "" {
		code = x.CodeResultUnknown
	}
	return clientError(code, c.identity, c.phase, false, errors.New("confirmed failed transfer"))
}

func (c *Client) waitTerminal(deadline x.Deadline) error {
	for c.receipt == nil {
		if c.cancelReason.Load() != nil {
			return c.failure(x.CodeCanceled, nil)
		}
		status, err := c.reconcile(deadline)
		if err != nil {
			return err
		}
		if status.State == x.StatusUnknown {
			return c.failure(x.CodeResultUnknown, nil)
		}
		if c.receipt == nil {
			if err := c.wait(deadline, 500*time.Millisecond, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) observeAttempt(attempt x.Attempt) {
	if c.cfg.Observe == nil {
		return
	}
	c.eventSeq++
	c.cfg.Observe(x.Observation{Kind: x.ObserveAttempt, TransferID: c.identity.TransferID, EntryNonce: c.identity.EntryNonce,
		ObjectVersion: c.identity.ObjectVersion, Phase: c.phase, EventSeq: c.eventSeq, Attempt: attempt})
}
