package infrahost

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// EffectPhase is private to the root journal. Unknown is sticky until a new,
// authorized recovery supplies independent evidence; it is never retry advice.
type EffectPhase uint8

const (
	EffectIntent     EffectPhase = 1
	EffectApplied    EffectPhase = 2
	EffectUnknown    EffectPhase = 3
	EffectNotApplied EffectPhase = 4
)

type Slots struct {
	Active   infracontract.Artifact
	Previous infracontract.Artifact
	Pending  infracontract.Artifact
}

// The journal contains identities and root-approved record digests, never key
// bytes or caller paths. Slots retain the rollback artifact; a launcher permit
// is bound to one candidate/config/owner and the original admission deadline.
// Warm and storage state survive worker death independently of worker lifetime.
// origin: hot-update S9 plan §2.2 / §6.2 / T09
type HostState struct {
	Tx              infracontract.ID
	Step            uint32
	Sequence        uint64
	PolicyRevision  uint64
	KeyID           string
	Slots           [5]Slots
	StartupFence    bool
	Permit          infracontract.Digest
	LaunchCandidate infracontract.Artifact
	LaunchConfig    infracontract.Digest
	LaunchDeadline  infracontract.Deadline
	LaunchClaimed   bool
	LaunchOwner     infracontract.Process
	Warm            infracontract.Process
	Redirect        infracontract.Digest
	CurrentStorage  infracontract.Digest
	SupportKind     infracontract.ArtifactKind
	SupportBefore   infracontract.Process
	SupportPending  bool
	Retirement      infracontract.Digest
}
type JournalEvent struct {
	Revision      uint64
	Previous      infracontract.Digest
	Digest        infracontract.Digest
	Request       infracontract.HostRequest
	RequestDigest infracontract.Digest
	Holder        infracontract.Peer
	Deadline      infracontract.Deadline
	Phase         EffectPhase
	State         HostState
	Receipt       infracontract.HostReceipt
}

// Events retain the original request and exact receipt so a lost reply remains
// a query even after a process restart. This is a separate private namespace;
// neither public wire versioning nor a route proof can authenticate this file.
type JournalImage struct {
	Version    uint16
	Purpose    string
	HostBootID string
	Domain     infracontract.ID
	Events     []JournalEvent
}

const journalPurpose, journalLimit = "tether-infra-journal-v1", 4 << 20

// Journal owns only a pinned directory FD. Each CAS opens the permanent lock
// inode independently, so distinct workers (and distinct processes) serialize.
// Close must follow all workers' Close; no goroutine is hidden in this owner.
type Journal struct {
	directory *os.File
	boot      string
	domain    infracontract.ID
	authority Authority
	clock     hostClock
}

// OpenJournal never bootstraps. An absent file is an explicit disabled host,
// not permission to create a new domain over an existing installation.
func OpenJournal(path, boot string, domain infracontract.ID, authority Authority) (*Journal, error) {
	if boot == "" || domain == (infracontract.ID{}) || nilInterface(authority) {
		return nil, ErrPermission
	}
	dir, err := openPrivateDirectory(path)
	if err != nil {
		return nil, err
	}
	j := &Journal{dir, boot, domain, authority, systemHostClock{}}
	if _, err := j.load(); err != nil {
		_ = dir.Close()
		return nil, err
	}
	return j, nil
}

// InitializeJournal is a root-bootstrap API, never a socket verb. The caller
// creates the private directory; this function exclusively creates the initial
// journal while retaining a permanent lock inode. It never starts business.
func InitializeJournal(path, boot string, domain infracontract.ID, initial HostState, d infracontract.Deadline) error {
	if boot == "" || domain == (infracontract.ID{}) || initial.Tx != (infracontract.ID{}) || initial.Sequence != 0 {
		return ErrJournal
	}
	dir, err := openPrivateDirectory(path)
	if err != nil {
		return err
	}
	// Every durable effect below is followed by its own Sync; closing the pinned
	// directory descriptor afterwards has nothing left to report.
	defer func() { _ = dir.Close() }()
	lock, err := openPrivateFile(dir, "journal.lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return ErrJournal
	}
	if err := lock.Sync(); err != nil {
		_ = lock.Close()
		return ErrJournal
	}
	_ = lock.Close()
	if err := dir.Sync(); err != nil {
		return ErrJournal
	}
	unlock, err := lockJournal(dir, d, nil)
	if err != nil {
		return err
	}
	defer unlock()
	if file, err := openPrivateFile(dir, "journal.json", os.O_RDONLY, 0600); err == nil {
		_ = file.Close()
		return ErrCAS
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrJournal
	}
	j := &Journal{directory: dir, boot: boot, domain: domain, clock: systemHostClock{}}
	im := JournalImage{Version: 1, Purpose: journalPurpose, HostBootID: boot, Domain: domain}
	return j.append(&im, JournalEvent{State: initial})
}
func (j *Journal) Close() error { return j.directory.Close() }

// Read returns owned values, not aliases into mutable worker state.
func (j *Journal) Read(d infracontract.Deadline) (HostState, infracontract.Digest, error) {
	if _, err := j.clock.Remaining(d); err != nil {
		return HostState{}, infracontract.Digest{}, err
	}
	im, err := j.load()
	if err != nil {
		return HostState{}, infracontract.Digest{}, err
	}
	last := im.Events[len(im.Events)-1]
	return last.State, last.Digest, nil
}
func (j *Journal) load() (JournalImage, error) {
	f, err := openPrivateFile(j.directory, "journal.json", os.O_RDONLY, 0600)
	if err != nil {
		return JournalImage{}, ErrJournal
	}
	defer func() { _ = f.Close() }() // Read-only; the bytes are validated below, not by close.
	data, err := io.ReadAll(io.LimitReader(f, journalLimit+1))
	if err != nil || len(data) > journalLimit {
		return JournalImage{}, ErrJournal
	}
	var im JournalImage
	if json.Unmarshal(data, &im) != nil || im.Version != 1 || im.Purpose != journalPurpose || im.HostBootID != j.boot || im.Domain != j.domain || len(im.Events) == 0 {
		return im, ErrJournal
	}
	// Exact re-encoding rejects duplicate keys, missing fields, unknown fields
	// and a partial final write. Guessing a default selector is unsafe here.
	canonical, err := encodeJournal(im)
	if err != nil || !bytes.Equal(data, canonical) {
		return im, ErrJournal
	}
	// A result cannot be transplanted to another request, host or domain. The
	// receipt's own digest is excluded from its event hash to avoid a cycle,
	// then checked against that hash when the committed event is read back.
	var previous infracontract.Digest
	for i, e := range im.Events {
		if e.Revision != uint64(i+1) || e.Previous != previous || e.Digest != eventDigest(e, im.HostBootID, im.Domain) {
			return im, ErrJournal
		}
		if i > 0 {
			requestBytes, err := infracontract.EncodeRequest(e.Request)
			if err != nil || sha256.Sum256(requestBytes) != e.RequestDigest || e.Request.HostBootID != j.boot || e.Request.Domain != j.domain || e.Deadline <= 0 || e.Phase < EffectIntent || e.Phase > EffectNotApplied {
				return im, ErrJournal
			}
			if e.Phase != EffectIntent {
				if _, err := infracontract.EncodeReceipt(e.Receipt); err != nil || e.Receipt.Journal != e.Digest || e.Receipt.RequestDigest != e.RequestDigest || e.Receipt.Tx != e.Request.Tx || e.Receipt.Sequence != e.Request.Sequence || e.Receipt.Op != e.Request.Op || e.Receipt.Revision != e.Revision {
					return im, ErrJournal
				}
			}
		}
		previous = e.Digest
	}
	return im, nil
}

// The request version is not the journal version. Every digest includes this
// purpose, host and domain, including the genesis record. Receipts bind to their
// event through Journal and to the full canonical request through RequestDigest.
func eventDigest(e JournalEvent, boot string, domain infracontract.ID) infracontract.Digest {
	e.Digest, e.Receipt.Journal = infracontract.Digest{}, infracontract.Digest{}
	data, _ := json.Marshal(struct {
		Purpose string
		Boot    string
		Domain  infracontract.ID
		Event   JournalEvent
	}{journalPurpose, boot, domain, e})
	return sha256.Sum256(data)
}
func (j *Journal) append(im *JournalImage, e JournalEvent) error {
	if len(im.Events) >= 2048 {
		return ErrJournal
	} // No eviction of replay or unresolved ownership.
	e.Revision = uint64(len(im.Events) + 1)
	if len(im.Events) != 0 {
		e.Previous = im.Events[len(im.Events)-1].Digest
	}
	if e.Phase != 0 && e.Phase != EffectIntent {
		e.Receipt.Tx, e.Receipt.Sequence, e.Receipt.Op = e.Request.Tx, e.Request.Sequence, e.Request.Op
		e.Receipt.Revision, e.Receipt.RequestDigest = e.Revision, e.RequestDigest
	}
	e.Digest = eventDigest(e, im.HostBootID, im.Domain)
	if e.Phase != 0 && e.Phase != EffectIntent {
		e.Receipt.Journal = e.Digest
		if _, err := infracontract.EncodeReceipt(e.Receipt); err != nil {
			return ErrJournal
		}
	}
	im.Events = append(im.Events, e)
	data, err := encodeJournal(*im)
	if err != nil {
		return err
	}
	// origin: hot-update S9 plan §2.2 / T09
	// File sync precedes rename and directory sync. Only then may a caller
	// dispatch the effect or reply with a committed receipt.
	return replacePrivateFile(j.directory, "journal.json", data)
}

// begin returns either a committed replay or a durable intent. Authorization
// is repeated even for replay, so revocation never becomes a cached privilege.
// It returns before any backend wait and reserves the domain through the event,
// rather than keeping an advisory lock alive across a systemd/launcher cycle.
// origin: hot-update S9 plan §2.2 / §6.1 / T09
func (j *Journal) begin(r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (JournalEvent, *infracontract.HostReceipt, error) {
	unlock, err := lockJournalAt(j.directory, d, stop, j.clock)
	if err != nil {
		return JournalEvent{}, nil, err
	}
	defer unlock()
	im, err := j.load()
	if err != nil {
		return JournalEvent{}, nil, err
	}
	if r.HostBootID != j.boot || r.Domain != j.domain {
		return JournalEvent{}, nil, ErrPermission
	}
	holder, err := j.authority.Authorize(r, d)
	if err != nil {
		return JournalEvent{}, nil, ErrPermission
	}
	if holder.PID <= 0 || holder.StartTicks == 0 || holder.BootID != j.boot {
		return JournalEvent{}, nil, ErrPermission
	}
	data, err := infracontract.EncodeRequest(r)
	if err != nil {
		return JournalEvent{}, nil, err
	}
	digest := sha256.Sum256(data)
	last := im.Events[len(im.Events)-1]
	var transactionSequence uint64
	// Same sequence with different bytes is not another attempt. Search before
	// current-head checks: a genuine old receipt remains queryable after later
	// effects, but never causes those effects to be repeated or undone.
	for i := len(im.Events) - 1; i > 0; i-- {
		e := im.Events[i]
		if e.Request.Tx == r.Tx {
			transactionSequence = max(transactionSequence, e.Request.Sequence)
		}
		if e.Request.Tx == r.Tx && e.Request.Sequence == r.Sequence {
			if e.RequestDigest != digest {
				return JournalEvent{}, nil, ErrCAS
			}
			if e.Phase == EffectApplied || e.Phase == EffectNotApplied {
				receipt := e.Receipt
				return e, &receipt, receiptError(e)
			}
			// Pending/unknown replay never acquires a new deadline or redispatches.
			return e, nil, ErrHalted
		}
		if e.Request.Nonce == r.Nonce {
			return JournalEvent{}, nil, ErrCAS
		}
	}
	if r.Op == infracontract.Inspect {
		// Even read-only requests receive a durable, replayable observation.
		if r.ExpectedJournal != (infracontract.Digest{}) && r.ExpectedJournal != last.Digest {
			return JournalEvent{}, nil, ErrCAS
		}
	} else if r.ExpectedJournal != last.Digest {
		return JournalEvent{}, nil, ErrCAS
	}
	state := last.State
	// A busy business domain may accept support bytes, but that staging request
	// must neither take the holder's domain nor advance its sequence/step.
	// Active/previous selectors remain unchanged until a later authorized cut.
	staging := r.Op == infracontract.Stage && r.Component == infracontract.HostSupport && state.Tx != (infracontract.ID{}) && state.Tx != r.Tx
	if state.Tx != (infracontract.ID{}) && state.Tx != r.Tx && !staging {
		return JournalEvent{}, nil, ErrBusy
	}
	if !staging && (r.Sequence <= state.Sequence || r.Step < state.Step) || staging && r.Sequence <= transactionSequence {
		return JournalEvent{}, nil, ErrCAS
	}
	if state.PolicyRevision > r.PolicyRevision {
		return JournalEvent{}, nil, ErrPermission
	}
	// An ambiguous effect owns the domain until independent recovery. The only
	// live-effect exception is the exact same start's launcher handshake; that
	// child must not acquire a second business domain to satisfy its parent.
	pending := pendingEvents(im)
	for _, e := range pending {
		if !permitsNestedLaunch(e, r) && r.Op != infracontract.RecoverHost {
			return JournalEvent{}, nil, ErrBusy
		}
	}
	if r.Op == infracontract.RecoverHost && !state.SupportPending {
		if len(pending) == 0 {
			if state.StartupFence || state.Warm != (infracontract.Process{}) || state.Permit != (infracontract.Digest{}) || r.ReturnProof == (infracontract.Digest{}) || r.Payload != last.RequestDigest {
				return JournalEvent{}, nil, ErrHalted
			}
		} else if len(pending) != 1 || pending[0].RequestDigest != r.Payload || pending[0].Request.Tx != r.Tx || pending[0].Request.Expected != r.Expected {
			return JournalEvent{}, nil, ErrHalted
		}
	}
	if state.SupportPending && state.SupportKind == infracontract.HelperBinary && r.Op != infracontract.RecoverHost && r.Op != infracontract.Inspect && !staging {
		return JournalEvent{}, nil, ErrBusy
	}
	if err := validateTransitionAt(j.clock, state, r); err != nil {
		return JournalEvent{}, nil, err
	}
	if err := checkStopAt(j.clock, d, stop); err != nil {
		return JournalEvent{}, nil, err
	}
	if !staging {
		state.Tx, state.Step, state.Sequence, state.PolicyRevision, state.KeyID = r.Tx, r.Step, r.Sequence, r.PolicyRevision, r.KeyID
	}
	// The durable start intent is the permit. A launcher must be able to claim
	// it before systemd returns the originating start job's reply.
	if r.Op == infracontract.StartWarm || r.Op == infracontract.ReleaseStart {
		state.Permit = r.Payload
		state.LaunchCandidate, state.LaunchConfig, state.LaunchDeadline = r.Candidate, r.Config, d
		state.LaunchClaimed, state.LaunchOwner = false, infracontract.Process{}
	}
	e := JournalEvent{Request: r, RequestDigest: digest, Holder: holder, Deadline: d, Phase: EffectIntent, State: state}
	if err := j.append(&im, e); err != nil {
		return e, nil, err
	}
	return im.Events[len(im.Events)-1], nil, nil
}
func permitsNestedLaunch(e JournalEvent, r infracontract.HostRequest) bool {
	return e.Phase == EffectIntent && (e.Request.Op == infracontract.StartWarm || e.Request.Op == infracontract.ReleaseStart) && (r.Op == infracontract.LaunchClaim || r.Op == infracontract.LaunchAck) && e.Request.Tx == r.Tx && e.Request.Step == r.Step && e.Request.Candidate == r.Candidate && e.Request.Config == r.Config && e.Request.Payload == r.Payload
}

// Physical backends revalidate their own proofs. These shared checks additionally
// prevent even a correctly signed backend request from crossing root selectors,
// replacing the rollback generation or spending a launch permit twice.
// origin: hot-update S9 plan §6.2 / T09 / T10 / T11
// Exec changes the executable while retaining the invocation's lifetime identity.
// The backend independently verifies the new executable and inherited lock; the
// journal must correlate that observation with the original Claim, not its image.
// origin: S9 internal review rv-infra F04
func validateTransition(s HostState, r infracontract.HostRequest) error {
	return validateTransitionAt(systemHostClock{}, s, r)
}

func validateTransitionAt(clock hostClock, s HostState, r infracontract.HostRequest) error {
	switch r.Op {
	case infracontract.Stage:
		slot := s.Slots[r.Candidate.Kind]
		if slot.Pending != (infracontract.Artifact{}) && slot.Pending != r.Candidate {
			return ErrBusy
		}
		if slot.Previous != (infracontract.Artifact{}) && slot.Active != r.Candidate && slot.Previous != r.Candidate {
			return ErrBusy
		}
	case infracontract.Promote, infracontract.UpdateSupport:
		slot := s.Slots[r.Candidate.Kind]
		if slot.Pending != r.Candidate || slot.Active == r.Candidate || slot.Active.BinarySHA256 == (infracontract.Digest{}) {
			return ErrCAS
		}
		expectedSHA := slot.Active.BinarySHA256
		if r.Op == infracontract.UpdateSupport {
			if s.SupportPending {
				return ErrBusy
			}
			// A launcher has already exec'd stock code. Its file is updated by
			// the helper worker, whose identity is the helper's active image.
			expectedSHA = s.Slots[infracontract.HelperBinary].Active.BinarySHA256
		}
		if expectedSHA != r.Expected.ExeSHA256 {
			return ErrCAS
		}
		if r.Op == infracontract.Promote && !s.StartupFence {
			return ErrPermission
		}
	case infracontract.ReleaseStart:
		if !s.StartupFence || s.Slots[r.Candidate.Kind].Active != r.Candidate {
			return ErrPermission
		}
		if r.Component == infracontract.Caddy && s.CurrentStorage == (infracontract.Digest{}) {
			return ErrPermission
		}
	case infracontract.LaunchClaim, infracontract.LaunchAck:
		if s.Permit == (infracontract.Digest{}) || s.Permit != r.Payload || s.LaunchCandidate != r.Candidate || r.Config != (infracontract.Digest{}) && s.LaunchConfig != r.Config || s.Step != r.Step {
			return ErrPermission
		}
		if _, err := clock.Remaining(s.LaunchDeadline); err != nil {
			return ErrPermission
		}
		if r.Op == infracontract.LaunchClaim && s.LaunchClaimed || r.Op == infracontract.LaunchAck && (!s.LaunchClaimed || (!sameLifetime(s.LaunchOwner, r.Expected) || s.LaunchOwner.InvocationID != r.Expected.InvocationID || s.LaunchOwner.Cgroup != r.Expected.Cgroup) || r.Expected.ExeSHA256 != s.LaunchCandidate.BinarySHA256 || (r.Expected.ExeDevice == s.LaunchOwner.ExeDevice && r.Expected.ExeInode == s.LaunchOwner.ExeInode)) {
			return ErrPermission
		}
	case infracontract.RetireWarm:
		if s.Warm == (infracontract.Process{}) || r.ReturnProof == (infracontract.Digest{}) {
			return ErrPermission
		}
	case infracontract.StartWarm:
		if s.Permit != (infracontract.Digest{}) {
			return ErrBusy
		}
	case infracontract.Inspect, infracontract.FenceStart, infracontract.SignalLDM, infracontract.SignalStop, infracontract.SignalGraceful, infracontract.SwitchIngress, infracontract.FinalizeStorage, infracontract.RecoverHost, infracontract.RotatePolicy:
	case infracontract.VerifyIngress: // origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
		return ErrPermission
	default:
		return ErrPermission
	}
	return nil
}

// Work backwards because a recovered effect has both its original intent and
// a later resolution. An old unknown record must not resurrect ownership after
// that resolution, and an unrelated successful operation must not erase it.
func pendingEvents(im JournalImage) []JournalEvent {
	var pending []JournalEvent
	seen := make(map[infracontract.Digest]bool)
	for i := len(im.Events) - 1; i > 0; i-- {
		e := im.Events[i]
		if seen[e.RequestDigest] {
			continue
		}
		seen[e.RequestDigest] = true
		if e.Phase == EffectIntent || e.Phase == EffectUnknown {
			pending = append(pending, e)
		}
	}
	return pending
}
func (j *Journal) unresolved(digest infracontract.Digest, d infracontract.Deadline) (JournalEvent, error) {
	if _, err := j.clock.Remaining(d); err != nil {
		return JournalEvent{}, err
	}
	im, err := j.load()
	if err != nil {
		return JournalEvent{}, err
	}
	for _, e := range pendingEvents(im) {
		if e.RequestDigest == digest {
			return e, nil
		}
	}
	return JournalEvent{}, ErrHalted
}
func (j *Journal) recoveryTarget(digest infracontract.Digest, d infracontract.Deadline) (JournalEvent, error) {
	if _, err := j.clock.Remaining(d); err != nil {
		return JournalEvent{}, err
	}
	im, err := j.load()
	if err != nil {
		return JournalEvent{}, err
	}
	for i := len(im.Events) - 1; i > 0; i-- {
		if im.Events[i].RequestDigest == digest {
			return im.Events[i], nil
		}
	}
	return JournalEvent{}, ErrHalted
}

// Merge into current state, not the pre-dispatch snapshot: a launcher may have
// committed Claim/Ack while its parent waited. Once recovery has resolved this
// intent, a late original completion has no authority to change that result.
// origin: hot-update S9 plan §2.2 / §6.1 / T09
func (j *Journal) finish(intent JournalEvent, result infracontract.HostReceipt, effectErr error, d infracontract.Deadline) (infracontract.HostReceipt, error) {
	unlock, err := lockJournalAt(j.directory, d, nil, j.clock)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	defer unlock()
	im, err := j.load()
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	current := im.Events[len(im.Events)-1].State
	found := false
	for _, e := range pendingEvents(im) {
		found = found || e.RequestDigest == intent.RequestDigest && e.Phase == EffectIntent
	}
	if !found {
		return infracontract.HostReceipt{}, ErrCAS
	}
	intent.State, intent.Receipt = current, result
	intent.Phase = EffectApplied
	if effectErr == nil && !validResultCode(result.Code) {
		effectErr = ErrHalted
		intent.Receipt.Code = ""
	}
	if effectErr != nil {
		intent.Phase, intent.Receipt.Applied, intent.Receipt.Code = EffectUnknown, false, "infra_result_unknown"
		var refusal *Refusal
		if errors.As(effectErr, &refusal) {
			intent.Phase, intent.Receipt.Code = EffectNotApplied, refusalCode(refusal.Cause)
		}
		// Backend error strings are never persisted: they can contain credentials.
	} else if intent.Request.Op == infracontract.RecoverHost && !current.SupportPending {
		resolved := false
		for _, e := range pendingEvents(im) {
			if e.RequestDigest != intent.Request.Payload {
				continue
			}
			if result.RequestDigest != e.RequestDigest {
				return infracontract.HostReceipt{}, ErrHalted
			}
			e.State, e.Receipt = current, result
			e.Phase = EffectNotApplied
			if result.Applied {
				e.Phase = EffectApplied
				applyState(&e.State, e.Request, result)
			}
			if err := j.append(&im, e); err != nil {
				return infracontract.HostReceipt{}, err
			}
			intent.State, resolved = e.State, true
			break
		}
		if !resolved {
			// A completed domain is released only with an independent retirement
			// census. The request's proof is revalidated by HostFacts.Resolve.
			if result.RequestDigest != intent.Request.Payload || !result.Applied || !completeCensus(result.Census) || result.Census.OldOwnerPending != 0 || intent.Request.ReturnProof == (infracontract.Digest{}) {
				return infracontract.HostReceipt{}, ErrHalted
			}
			intent.State.Tx, intent.State.Step, intent.State.Sequence = infracontract.ID{}, 0, 0
			intent.State.Retirement = intent.Request.ReturnProof
		}
	} else {
		if !result.Applied && intent.Request.Op != infracontract.Inspect {
			return infracontract.HostReceipt{}, ErrHalted
		}
		applyState(&intent.State, intent.Request, result)
	}
	if err := j.append(&im, intent); err != nil {
		return infracontract.HostReceipt{}, err
	}
	receipt := im.Events[len(im.Events)-1].Receipt
	return receipt, receiptError(im.Events[len(im.Events)-1])
}
func refusalCode(err error) string {
	for _, known := range []error{ErrBusy, ErrPermission, ErrArtifact, ErrStopped, infracontract.ErrIncomplete, infracontract.ErrUnsupported} {
		if errors.Is(err, known) {
			return known.Error()
		}
	}
	return "infra_internal"
}

func receiptError(e JournalEvent) error {
	if e.Phase == EffectUnknown {
		return ErrHalted
	}
	if e.Phase != EffectNotApplied {
		return nil
	}
	for _, known := range []error{ErrBusy, ErrPermission, ErrArtifact, ErrStopped, infracontract.ErrIncomplete, infracontract.ErrUnsupported} {
		if e.Receipt.Code == known.Error() {
			return known
		}
	}
	return ErrPermission
}

// Selector publication is part of the result commit, not an earlier filesystem
// symlink mutation. Consumers read this exact journal head, while an unresolved
// physical backend effect keeps its intent and requires read-only reconciliation.
// origin: hot-update S9 plan §6.2 / T09 / T10
func applyState(s *HostState, r infracontract.HostRequest, result infracontract.HostReceipt) {
	switch r.Op {
	case infracontract.Stage:
		s.Slots[r.Candidate.Kind].Pending = r.Candidate
	case infracontract.FenceStart:
		s.StartupFence = true
	case infracontract.Promote, infracontract.UpdateSupport:
		slot := &s.Slots[r.Candidate.Kind]
		slot.Previous, slot.Active, slot.Pending = slot.Active, r.Candidate, infracontract.Artifact{}
		if r.Op == infracontract.UpdateSupport {
			s.SupportBefore, s.SupportKind, s.SupportPending = r.Expected, r.Candidate.Kind, true
		}
	case infracontract.StartWarm:
		s.Warm = result.Current
	case infracontract.SwitchIngress:
		s.Redirect = r.Payload
	case infracontract.FinalizeStorage:
		s.CurrentStorage = r.Payload
	case infracontract.ReleaseStart:
		s.StartupFence = false
	case infracontract.RetireWarm:
		s.Warm, s.Retirement = infracontract.Process{}, r.ReturnProof
	case infracontract.RecoverHost:
		if s.SupportPending {
			s.SupportPending, s.Retirement = false, r.ReturnProof
		}
	case infracontract.LaunchClaim:
		s.LaunchClaimed, s.LaunchOwner = true, r.Expected
	case infracontract.LaunchAck:
		s.Permit, s.LaunchCandidate, s.LaunchConfig, s.LaunchDeadline, s.LaunchClaimed, s.LaunchOwner = infracontract.Digest{}, infracontract.Artifact{}, infracontract.Digest{}, 0, false, infracontract.Process{}
	case infracontract.Inspect, infracontract.SignalLDM, infracontract.SignalStop, infracontract.SignalGraceful, infracontract.RotatePolicy, infracontract.VerifyIngress:
	}
}

// JournalDescriptors is used by the actual encoder, not a disconnected table.
// P35 freezes these rows and the reachable infracontract records in its ledger.
func JournalDescriptors() []infracontract.RecordDescriptor {
	shapes := []struct{ name, fields string }{
		{"JournalImage", "Version:uint16 Purpose:string HostBootID:string Domain:infracontract.ID Events:[]infrahost.JournalEvent"},
		{"JournalEvent", "Revision:uint64 Previous:infracontract.Digest Digest:infracontract.Digest Request:infracontract.HostRequest RequestDigest:infracontract.Digest Holder:infracontract.Peer Deadline:controlbudget.Deadline Phase:infrahost.EffectPhase State:infrahost.HostState Receipt:infracontract.HostReceipt"},
		{"HostState", "Tx:infracontract.ID Step:uint32 Sequence:uint64 PolicyRevision:uint64 KeyID:string Slots:[5]infrahost.Slots StartupFence:bool Permit:infracontract.Digest LaunchCandidate:infracontract.Artifact LaunchConfig:infracontract.Digest LaunchDeadline:controlbudget.Deadline LaunchClaimed:bool LaunchOwner:infracontract.Process Warm:infracontract.Process Redirect:infracontract.Digest CurrentStorage:infracontract.Digest SupportKind:infracontract.ArtifactKind SupportBefore:infracontract.Process SupportPending:bool Retirement:infracontract.Digest"},
		{"Slots", "Active:infracontract.Artifact Previous:infracontract.Artifact Pending:infracontract.Artifact"},
	}
	var rows []infracontract.RecordDescriptor
	for _, shape := range shapes {
		row := infracontract.RecordDescriptor{Name: shape.name, Version: 1, MaxBytes: journalLimit}
		for _, field := range strings.Fields(shape.fields) {
			name, typ, _ := strings.Cut(field, ":")
			row.Fields = append(row.Fields, infracontract.FieldDescriptor{Name: name, Type: typ})
		}
		rows = append(rows, row)
	}
	return rows
}

func encodeJournal(im JournalImage) ([]byte, error) {
	types := []reflect.Type{reflect.TypeFor[JournalImage](), reflect.TypeFor[JournalEvent](), reflect.TypeFor[HostState](), reflect.TypeFor[Slots]()}
	for i, row := range JournalDescriptors() {
		typ := types[i]
		if typ.NumField() != len(row.Fields) {
			return nil, ErrJournal
		}
		for n, field := range row.Fields {
			if typ.Field(n).Name != field.Name || typ.Field(n).Type.String() != field.Type || typ.Field(n).Tag != "" {
				return nil, ErrJournal
			}
		}
	}
	data, err := json.Marshal(im)
	if err != nil || len(data) > journalLimit {
		return nil, ErrJournal
	}
	return data, nil
}

// OperationRecord resolves the latest exact operation from the durable log.
// Callers must check its phase; an intent is never proof that an effect applied.
// origin: S9 internal review rv-infra F03 / F04
func (j *Journal) OperationRecord(r infracontract.HostRequest, d infracontract.Deadline) (JournalEvent, error) {
	if _, err := j.clock.Remaining(d); err != nil {
		return JournalEvent{}, err
	}
	im, err := j.load()
	if err != nil {
		return JournalEvent{}, err
	}
	for i := len(im.Events) - 1; i > 0; i-- {
		e := im.Events[i]
		if e.Request.Op == r.Op && e.Request.Tx == r.Tx && e.Request.Step == r.Step && e.Request.Component == r.Component {
			return e, nil
		}
	}
	return JournalEvent{}, ErrPermission
}
