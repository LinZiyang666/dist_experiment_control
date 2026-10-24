package xfercourier

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

var (
	ErrConflict   = errors.New(xfercontract.CodeTerminalConflict)
	ErrReconcile  = errors.New(xfercontract.CodeReconcileRequired)
	ErrRecovering = errors.New(xfercontract.CodeOwnerRecovering)
	ErrStopped    = errors.New("xfercourier: stopped")
)

// The registry schedules Step; the courier creates no independent timer or
// goroutine. Its lifetime therefore survives replacement of a request/session
// context, and every transport callback belongs to the registry's cut barrier.
// origin: hot-update S7 plan §4 / §8.2 / T16
type courier struct {
	mu                   sync.Mutex
	observationMu        sync.Mutex
	config               Config
	state                xfercontract.CourierState
	bootID               string
	active               uint32
	idle                 chan struct{}
	stepping             bool
	freezing             bool
	stopping             bool
	reservation          xfercontract.ReservationID
	canonicalReservation xfercontract.ReservationID
	eventSeq             uint64
	// P00 has no conflict-evidence field. Refuse export rather than lose this
	// second receipt across a cut; both the original decision and evidence stay
	// with this owner until the contract can represent the complete closure.
	conflict *xfercontract.TerminalReceipt
}

var _ Factory = New
var _ Courier = (*courier)(nil)

// New retains a terminal outcome and a capsule-bound settlement transport.
// The transport must already authenticate that capsule's receiver NKey; the
// frozen Transport port cannot carry a capsule or manufacture its NKey proof.
// Canonical history bytes belong to the deciding broker, not the receiver.
func New(config Config) (Courier, error) {
	c, err := newCourier(config)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// newCourier returns the concrete owner so Import installs retained state
// without asserting on the public Courier port.
func newCourier(config Config) (*courier, error) {
	if config.Clock == nil || config.UTC == nil || config.Capacity == nil || config.Transport == nil {
		return nil, errors.New("xfercourier: missing clock, accounting or settlement transport")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	boot, now, err := config.Clock()
	if err != nil || boot == "" || now <= 0 {
		return nil, errors.New("xfercourier: settlement clock unavailable")
	}
	// Reserve before copying private responsibility. The fixed metadata ceiling
	// covers concurrent snapshots and request copies, including capsule strings.
	// origin: hot-update S7 plan §8.3 / T16
	id, err := config.Capacity.Reserve(xfercontract.Reservation{NonReplayBytes: 64 << 10, SealedBytes: 64 << 10})
	if err != nil {
		return nil, fmt.Errorf("%s: courier reservation refused", xfercontract.CodeCapacityExceeded)
	}
	if id == "" {
		return nil, errors.New("xfercourier: empty reservation identity")
	}
	idle := make(chan struct{})
	close(idle)
	return &courier{config: config, bootID: boot, idle: idle, reservation: id,
		state: xfercontract.CourierState{Identity: config.Identity, Outcome: config.Outcome,
			Capsule: config.Capsule, Cleanup: xfercontract.CleanupState{Identity: config.Identity,
				TmpUnlinkPending:     config.Outcome.TmpCleanupPending,
				DirectorySyncPending: config.Outcome.Outcome == xfercontract.OutcomeLanded && !config.Outcome.ParentSynced}}}, nil
}

// BindCanonical supplies the original broker decision without changing the
// frozen Config/interface. Until it arrives, even a plausible receipt cannot
// retire delivery: the receiver cannot guess the broker's timestamp or ReqID.
// P00/P16 must provide these bytes through the continuation response seam.
// origin: hot-update S7 plan §4 / §5.1 / T16
func BindCanonical(owner Courier, record xfercontract.CanonicalRecord) error {
	c, ok := owner.(*courier)
	if !ok || c == nil {
		return errors.New("xfercourier: foreign owner")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.freezing || c.stopping {
		return ErrRecovering
	}
	if err := validateCanonical(c.state, record); err != nil {
		return err
	}
	if len(c.state.Canonical.Bytes) != 0 {
		old := c.state.Canonical
		if old.Identity != record.Identity || old.Kind != record.Kind || old.ReqID != record.ReqID ||
			old.Digest != record.Digest || !bytes.Equal(old.Bytes, record.Bytes) {
			return ErrConflict
		}
		return nil
	}
	if c.state.Stopped {
		return ErrStopped
	}
	// The record and two simultaneous sealed copies share the process capacity
	// account. Never install bytes after a failed reservation.
	id, err := c.config.Capacity.Reserve(xfercontract.Reservation{
		NonReplayBytes: int64(len(record.Bytes)) * 3, SealedBytes: int64(len(record.Bytes)) * 3})
	if err != nil || id == "" {
		return fmt.Errorf("%s: canonical reservation refused", xfercontract.CodeCapacityExceeded)
	}
	record.Bytes = bytes.Clone(record.Bytes)
	c.state.Canonical = record
	c.canonicalReservation = id
	return nil
}

// Import preserves the original boot-relative retry, ACK and cleanup closure.
// The registry must install this sleeping owner before scheduling any Step.
// origin: hot-update S7 plan §8.1 / §8.2 / T16
func Import(config Config, state xfercontract.CourierState) (Courier, error) {
	if state.Identity != config.Identity || state.Outcome != config.Outcome || state.Capsule != config.Capsule {
		return nil, ErrConflict
	}
	if state.Cleanup.Identity != state.Identity || state.Reply != "" || len(state.Cleanup.Consumers) != 0 ||
		state.Cleanup.WriterFence != (xfercontract.RetirementFence{}) {
		return nil, errors.New("xfercourier: unsupported cleanup or reply responsibility")
	}
	if state.Attempt.Ordinal == ^uint32(0) || state.Attempt.BackoffNS < 0 || state.NextAttempt.Deadline < 0 {
		return nil, errors.New("xfercourier: invalid retry state")
	}
	c, err := newCourier(config)
	if err != nil {
		return nil, err
	}
	refuse := func(err error) (Courier, error) {
		// No callback has been admitted; these reservations have one closer.
		return nil, errors.Join(err, c.release())
	}
	if state.NextAttempt.Deadline != 0 && state.NextAttempt.BootID != c.bootID {
		return refuse(errors.New("xfercourier: retry boot changed"))
	}
	if canonicalPresent(state.Canonical) {
		if err := BindCanonical(c, state.Canonical); err != nil {
			return refuse(err)
		}
	}
	if state.Receipt != nil {
		if !state.Stopped {
			return refuse(errors.New("xfercourier: acknowledged retry still active"))
		}
		if err := c.Acknowledge(*state.Receipt); err != nil {
			return refuse(err)
		}
	}
	c.state = cloneState(state)
	c.observe(xfercontract.ObserveImport, state.Attempt)
	return c, nil
}

// Acknowledge closes future send admission in the same critical section as
// receipt validation. An already sent attempt is joined by Freeze/Stop and
// cannot replace the accepted receipt when its callback finally returns.
// origin: hot-update S7 plan §4 / §5.2 / T16
func (c *courier) Acknowledge(receipt xfercontract.TerminalReceipt) error {
	c.mu.Lock()
	if c.freezing || c.stopping {
		c.mu.Unlock()
		return ErrRecovering
	}
	if err := validateReceipt(c.state, receipt); err != nil {
		// Preserve authentic-looking contradictory history evidence without
		// allowing an invalid identity or partial ACK to poison another entry.
		if errors.Is(err, ErrConflict) && boundedReceipt(receipt) && receipt.TerminalID == terminalID(c.state.Identity) &&
			receipt.HistoryStream == historyStream(c.state.Identity.SID) && receipt.HistorySeq != 0 {
			copyReceipt := receipt
			c.conflict = &copyReceipt
		}
		c.mu.Unlock()
		return err
	}
	if c.conflict != nil {
		c.mu.Unlock()
		return ErrConflict
	}
	if c.state.Receipt != nil {
		if *c.state.Receipt != receipt {
			copyReceipt := receipt
			c.conflict = &copyReceipt
			c.mu.Unlock()
			return ErrConflict
		}
		c.mu.Unlock()
		return nil
	}
	if c.state.Stopped {
		c.mu.Unlock()
		return ErrStopped
	}
	copyReceipt := receipt
	c.state.Receipt = &copyReceipt
	c.state.Stopped = true
	c.enterLocked()
	c.mu.Unlock()
	defer c.leave()
	c.observe(xfercontract.ObserveAcknowledge, xfercontract.Attempt{})
	return nil
}

func validateConfig(config Config) error {
	// Validation never authenticates a capsule: issuer signature and NKey
	// possession are enforced by the bound transport. These checks stop an
	// accidental cross-entry binding before accounting or network effects.
	// origin: hot-update S7 plan §6.4 / §8.3 / T16
	id := config.Identity
	for _, token := range []string{id.SID, id.NID, id.AgentInstance, id.TransferID, id.EntryNonce} {
		if !validToken(token) {
			return errors.New("xfercourier: invalid transfer identity")
		}
	}
	// A real actor is the canonical "SHA256:" fingerprint (raw standard
	// base64 with '/' and '+'), body data rather than a subject token.
	// origin: hot-update S7 plan §6.2 / INTEG-D agent wiring
	if !validToken(id.ActorFP) && !validFingerprint(id.ActorFP) {
		return errors.New("xfercourier: invalid transfer identity")
	}
	if !validDigest(id.IntentDigest) || (id.ObjectVersion != "" && !validToken(id.ObjectVersion)) {
		return errors.New("xfercourier: invalid intent or object identity")
	}
	capsule := config.Capsule
	if capsule.Identity != id || !validToken(capsule.Origin.BrokerID) || !validToken(capsule.Origin.InstanceID) ||
		capsule.Origin.BootID == "" || capsule.Origin.Generation == 0 {
		return errors.New("xfercourier: settlement identity mismatch")
	}
	// An origin-only binding (no issued credential) still routes settlement to
	// the retained original owner over the receiver's ordinary authenticated
	// transport; it simply cannot settle after revocation. A PARTIAL credential
	// is malformed and refused, never treated as either form.
	// origin: hot-update S7 plan §4 / §6.4 / INTEG-D courier canonical carrier
	credential := []string{capsule.ReceiverNkey, capsule.Issuer, capsule.Nonce, capsule.Signature}
	if capsule.ReceiverNkey == "" && capsule.Issuer == "" && capsule.Nonce == "" && capsule.Signature == "" {
		credential = nil
	}
	for _, value := range append(credential, capsule.Origin.BootID) {
		if value == "" || len(value) > 4096 || strings.ContainsAny(value, "\r\n") {
			return errors.New("xfercourier: incomplete settlement binding")
		}
	}
	o := config.Outcome
	if o.Bytes < 0 || len(o.Error) > 4096 || len(o.Code) > 256 || (o.SHA256 != "" && !validDigest(o.SHA256)) {
		return errors.New("xfercourier: invalid receiver outcome")
	}
	if o.Outcome != xfercontract.OutcomeNotEntered && o.Outcome != xfercontract.OutcomeInflight && o.Outcome != xfercontract.OutcomeLanded {
		return errors.New("xfercourier: unresolved receiver outcome")
	}
	if o.Grant != (xfercontract.CommitGrant{}) {
		if o.Grant.Identity != id || !validToken(o.Grant.GrantNonce) ||
			!validToken(o.Grant.ReceiverInstance) || o.Grant.TerminalID != terminalID(id) {
			return ErrConflict
		}
	} else if o.Outcome != xfercontract.OutcomeNotEntered {
		return errors.New("xfercourier: primitive outcome without grant")
	}
	if (o.Primitive != "" && o.Primitive != xfercontract.PrimitiveRename && o.Primitive != xfercontract.PrimitiveLink) ||
		(o.Outcome != xfercontract.OutcomeNotEntered && o.Primitive == "") {
		return errors.New("xfercourier: invalid commit primitive")
	}
	if o.Outcome == xfercontract.OutcomeNotEntered && (o.ParentSynced || o.Code == "") {
		return errors.New("xfercourier: missing closed-gate failure")
	}
	return nil
}

func validateCanonical(state xfercontract.CourierState, record xfercontract.CanonicalRecord) error {
	if record.Identity != state.Identity || record.Kind != terminalKind(state.Outcome) ||
		record.Kind == "" || !validDigest(record.ReqID) || len(record.Bytes) == 0 || len(record.Bytes) > 64<<10 {
		return ErrConflict
	}
	sum := sha256.Sum256(record.Bytes)
	if record.Digest != hex.EncodeToString(sum[:]) {
		return ErrConflict
	}
	// Hashing bytes alone cannot bind a foreign row whose metadata was relabelled
	// with this Identity. Check the audit payload against the receiver facts too.
	// origin: hot-update S7 plan §4 / §5.1 / T16
	var row schema.AuditTransfer
	if err := json.Unmarshal(record.Bytes, &row); err != nil || row.V != schema.AuditSchemaVersion ||
		row.Session != state.Identity.SID || row.Node != state.Identity.NID || row.ActorFp != state.Identity.ActorFP ||
		row.TransferID != state.Identity.TransferID || row.Kind != record.Kind || row.Bytes != state.Outcome.Bytes ||
		row.SHA256 != state.Outcome.SHA256 || row.Code != state.Outcome.Code {
		return ErrConflict
	}
	return nil
}

func validateReceipt(state xfercontract.CourierState, receipt xfercontract.TerminalReceipt) error {
	// A boolean terminal, raft ACK, missing physical proof or broker-invented
	// completion cannot discharge the receiver's exact original decision.
	// origin: hot-update S7 plan §4 / §5.2 / T16
	if receipt.TerminalID != terminalID(state.Identity) || receipt.Kind != terminalKind(state.Outcome) ||
		receipt.Kind == "" || receipt.Bytes != state.Outcome.Bytes || receipt.SHA256 != state.Outcome.SHA256 ||
		receipt.Code != state.Outcome.Code || receipt.OutcomeUnknown {
		return ErrConflict
	}
	if !canonicalPresent(state.Canonical) {
		return ErrReconcile
	}
	if receipt.CanonicalDigest != state.Canonical.Digest || receipt.ReqID != state.Canonical.ReqID {
		return ErrConflict
	}
	if receipt.HistoryStream != historyStream(state.Identity.SID) || receipt.HistorySeq == 0 {
		return ErrReconcile
	}
	return nil
}

// Rejected evidence still consumes memory if retained. Bound it independently
// of semantic validation so a malformed reply cannot exhaust the reservation
// while preserving the two legitimate sides of a terminal conflict.
// origin: hot-update S7 plan §4 / §8.3 / T16
func boundedReceipt(receipt xfercontract.TerminalReceipt) bool {
	return len(receipt.TerminalID) <= 64 && len(receipt.Kind) <= 8 && len(receipt.CanonicalDigest) <= 64 &&
		len(receipt.ReqID) <= 64 && len(receipt.HistoryStream) <= 512 && len(receipt.SHA256) <= 64 && len(receipt.Code) <= 256
}

func canonicalPresent(record xfercontract.CanonicalRecord) bool {
	return record.Identity != (xfercontract.Identity{}) || record.Kind != "" || record.ReqID != "" || record.Digest != "" || len(record.Bytes) != 0
}

func terminalKind(outcome xfercontract.ReceiverOutcome) string {
	switch outcome.Outcome {
	case xfercontract.OutcomeNotEntered:
		return "failed"
	case xfercontract.OutcomeLanded:
		if outcome.ParentSynced && outcome.Code == "" {
			return "complete"
		}
		return ""
	case xfercontract.OutcomeInflight:
		return ""
	default:
		return ""
	}
}

func terminalID(id xfercontract.Identity) string {
	sum := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	return hex.EncodeToString(sum[:])
}

func historyStream(sid string) string { return "history-" + sid }

func validToken(token string) bool {
	if token == "" || len(token) > 256 {
		return false
	}
	for _, r := range token {
		if r != '-' && r != '_' && (r < '0' || r > '9') && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') {
			return false
		}
	}
	return true
}

func validFingerprint(value string) bool {
	body, ok := strings.CutPrefix(value, "SHA256:")
	if !ok || len(body) != 43 {
		return false
	}
	for _, r := range body {
		if r != '+' && r != '/' && (r < '0' || r > '9') && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') {
			return false
		}
	}
	return true
}

func validDigest(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	for _, r := range digest {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// ValidateEvidence applies the courier's exact canonical/receipt checks to
// sender-only settlement, which has no courier actor to retain those bytes.
func ValidateEvidence(o xfercontract.CanonicalOutcome) error {
	if o.Receipt == nil {
		return ErrReconcile
	}
	// A broker's explicit abandonment is a decision to retire responsibility,
	// not the absent receiver's physical outcome. Keep this separate from
	// Acknowledge: ordinary couriers must still reject unknown-outcome ACKs.
	// origin: S7 internal review TRANSFER-MAKETEST F7 / S7 plan lead LQ1 / §6.4
	if o.Receipt.OutcomeUnknown || o.Outcome.Code == xfercontract.ReasonSettlementAbandoned {
		return validateAdministrativeEvidence(o)
	}
	s := xfercontract.CourierState{Identity: o.Identity, Outcome: o.Outcome, Canonical: o.Canonical}
	if err := validateCanonical(s, o.Canonical); err != nil {
		return err
	}
	return validateReceipt(s, *o.Receipt)
}

func validateAdministrativeEvidence(o xfercontract.CanonicalOutcome) error {
	r := o.Receipt
	if !r.OutcomeUnknown || r.Kind != "failed" || r.Code != xfercontract.ReasonSettlementAbandoned || o.Outcome.Code != r.Code ||
		r.TerminalID != terminalID(o.Identity) || r.Bytes < 0 || r.Bytes != o.Outcome.Bytes || r.SHA256 != o.Outcome.SHA256 ||
		len(o.Canonical.Bytes) == 0 || len(o.Canonical.Bytes) > 64<<10 {
		return ErrConflict
	}
	var audit schema.AuditTransfer
	if json.Unmarshal(o.Canonical.Bytes, &audit) != nil || !audit.OutcomeUnknown || audit.Kind != "failed" ||
		audit.Code != r.Code || audit.Bytes != r.Bytes || audit.SHA256 != r.SHA256 {
		return ErrConflict
	}
	// Regeneration binds the full original identity and canonical content ReqID,
	// so relabelled envelopes and self-consistent forged digests cannot retire
	// another entry. The history proof is still required independently.
	canonical, err := schema.CanonicalTransferAudit(audit)
	if err != nil || canonical.Identity != o.Identity || canonical.Kind != o.Canonical.Kind || canonical.ReqID != o.Canonical.ReqID ||
		canonical.Digest != o.Canonical.Digest || !bytes.Equal(canonical.Bytes, o.Canonical.Bytes) ||
		r.CanonicalDigest != canonical.Digest || r.ReqID != canonical.ReqID {
		return ErrConflict
	}
	if r.HistoryStream != historyStream(o.Identity.SID) || r.HistorySeq == 0 {
		return ErrReconcile
	}
	return nil
}
