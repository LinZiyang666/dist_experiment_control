// Package infraupgrade coordinates infrastructure owners without acquiring their
// process, socket, storage or business lifetimes.
package infraupgrade

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/netip"
	"reflect"
	"runtime"
	"sync/atomic"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

const caddyHandoverBudget = upgradebudget.ComponentHandover

// CaddyJournal is an additional non-root adapter seam, not a change to the
// frozen Transaction interface. Status alone cannot reconstruct an installed
// cohort, an outstanding signed request, or a return's original nonce. The S8
// adapter must persist this record under the acquired domain before returning.
// Missing support refuses admission; it must never be replaced by a memory map
// in production. Store must CAS against the domain holder and policy revision.
// origin: hot-update S9 plan §2.2 / §7.2 / T22
type CaddyJournal interface {
	LoadCaddy(c.Plan, c.Deadline) (CaddyTransition, error)
	StoreCaddy(c.Plan, CaddyTransition, c.Deadline) error
}

// CaddyRequests belongs to the authenticated Host client, never the root worker.
// It registers the immutable plan/config evidence referenced by Payload and
// supplies the current key, nonce and signature. All other operands are fixed
// by the driver and checked again after signing. The root independently checks
// every evidence reference; a digest is not a caller's permission to stop.
type CaddyRequests interface {
	AuthorizeCaddy(c.Plan, c.HostRequest, c.Deadline) (c.HostRequest, error)
}

// CaddyIngress verifies the current independent ingress fence and the full
// responsibility set against the supplied physical census. Its owner must read
// the active rules, both address families, accepted TCP, SYN and conntrack; an
// applied SwitchIngress receipt alone is not a current seal. The proof covers
// the exact displaced/serving incarnations and must remain closed through stop.
// origin: S9 internal review rv-infra F07
type CaddyIngress interface {
	VerifyCaddyIngress(c.Plan, c.Process, c.Process, c.Census, c.Deadline, c.Stop) error
}

// CaddyTransition is private controller recovery state, not public wire or an
// S5/S6 image. Cursor names the next barrier; Pending retains the exact signed
// request across lost replies. No secret bytes or filesystem paths belong here.
// Version and the declaration order belong in the S9 private-state inventory.
type CaddyTransition struct {
	Version          uint16
	Plan             c.Plan
	Cursor           uint16
	Deadline         c.Deadline
	HandoverStarted  bool
	TimedOut         bool
	Changed          bool
	Sequence         uint64
	HostRevision     uint64
	CensusGeneration uint64
	CensusDigest     c.Digest
	Journal          c.Digest
	Warm             c.Process
	Main             c.Process
	Checkpoint       c.Checkpoint
	Prepares         []c.RouteProof
	Binds            []c.RouteProof
	Move             c.MoveResult
	Bound            c.Census
	BeforeCensus     c.Census
	WarmCensus       c.Census
	ReturnProof      c.Digest
	Pending          *c.HostRequest
	Receipt          *c.HostReceipt
}

type caddyDriver struct {
	deps     c.Dependencies
	journal  CaddyJournal
	requests CaddyRequests
	busy     atomic.Bool
	now      func() (int64, error)
	wait     func(c.Stop) error
}

var _ c.DriverFactory = NewCaddy

// NewCaddy only binds capabilities. No worker, timer, process or file is owned
// by the driver; the root and transport owners join their own bounded actions.
func NewCaddy(deps c.Dependencies) (c.Driver, error) {
	for _, dependency := range []any{deps.Host, deps.Artifacts, deps.Ready, deps.Cohort, deps.Routes, deps.Tx, deps.Observe} {
		if caddyNil(dependency) {
			return nil, c.ErrContractStub
		}
	}
	journal, ok := deps.Tx.(CaddyJournal)
	requests, signed := deps.Host.(CaddyRequests)
	_, sealed := deps.Host.(CaddyIngress)
	if !ok || !signed || !sealed {
		return nil, c.ErrContractStub
	}
	return &caddyDriver{deps: deps, journal: journal, requests: requests, now: controlbudget.Now, wait: caddyPoll}, nil
}

func caddyNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	return (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil()
}

func (d *caddyDriver) Advance(p c.Plan, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	return d.run(p, deadline, stop, false)
}

func (d *caddyDriver) Recover(p c.Plan, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	return d.run(p, deadline, stop, true)
}

func (d *caddyDriver) run(p c.Plan, deadline c.Deadline, stop c.Stop, recover bool) (status c.Status, resultErr error) {
	// Backend diagnostics can contain private tuples or key paths. Only the
	// closed code vocabulary may escape through the public Driver boundary.
	defer func() { resultErr = caddySafeError(resultErr) }()
	if !d.busy.CompareAndSwap(false, true) {
		return c.Status{}, errors.New("infra_busy")
	}
	defer d.busy.Store(false)
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || p.Component != c.Caddy || c.ValidatePlan(p) != nil || p.Action == c.Recover {
		return c.Status{}, c.ErrUnsupported
	}
	if err := d.budget(deadline, stop); err != nil {
		return c.Status{}, err
	}
	var s c.Status
	var x CaddyTransition
	var err error
	if recover {
		s, err = d.deps.Tx.Load(p.Tx, deadline)
		if err == nil {
			x, err = d.journal.LoadCaddy(p, deadline)
		}
		if err != nil {
			return s, err
		}
		if !caddyTransitionValid(x, p) || !caddyStatus(s, p) {
			return s, c.ErrIncomplete
		}
		if s.Phase == c.Completed {
			if x.Cursor < caddyComplete || s.Proof != x.Journal || s.Current != x.Main {
				return s, c.ErrIncomplete
			}
			return s, nil
		}
		now, clockErr := d.now()
		if clockErr != nil {
			return s, clockErr
		}
		x.TimedOut = x.TimedOut || (x.HandoverStarted && now >= int64(x.Deadline))
		// Recovery has a new bounded action budget, but retains the original
		// timeout and exact pending request. It cannot renew a route proof or
		// convert a failed original 120-second handover into a timely one.
		census, inspectErr := d.inspect(&x, deadline)
		if inspectErr != nil {
			return s, inspectErr
		}
		if x.Cursor > caddyStartWarm && x.Cursor < caddyRetireWarm && !caddyPresent(census, x.Warm) {
			return s, c.ErrIncomplete
		}
		if x.Cursor > caddyReleaseStart && !caddyPresent(census, x.Main) {
			return s, c.ErrIncomplete
		}
	} else {
		s, err = d.deps.Tx.Acquire(p, deadline)
		if err != nil {
			return s, err
		}
		if !caddyStatus(s, p) || s.Phase != c.Prepared {
			return s, errors.New("infra_recovery_required")
		}
		x = CaddyTransition{Version: 1, Plan: p, Deadline: deadline, Main: p.Before}
		if err = d.save(x, deadline); err != nil {
			return s, err
		}
	}
	if !recover {
		deadline = min(deadline, x.Deadline)
	}
	for x.Cursor < caddyFinished {
		if err = d.budget(deadline, stop); err != nil {
			return d.fail(&x, s, deadline, err)
		}
		if err = d.barrier(&x, &s, deadline, stop); err != nil {
			return d.fail(&x, s, deadline, err)
		}
		if x.Cursor == caddyValidate && !x.HandoverStarted {
			// Artifact download/validation is outside ComponentHandover. Start
			// its clock exactly once after both immutable images are verified.
			now, clockErr := d.now()
			if clockErr != nil {
				return d.fail(&x, s, deadline, clockErr)
			}
			x.Deadline = min(deadline, c.Deadline(now+int64(caddyHandoverBudget)))
			x.HandoverStarted = true
			deadline = x.Deadline
		}
		// Observe an overrun before accepting even the final barrier. There is
		// deliberately no timeout cleanup that kills a still-serving shadow.
		if err = d.budget(deadline, stop); err != nil {
			return d.fail(&x, s, deadline, err)
		}
		x.Cursor++
		x.Pending, x.Receipt = nil, nil
		if err = d.save(x, deadline); err != nil {
			return d.fail(&x, s, deadline, err)
		}
	}
	return s, nil
}

func caddySafeError(err error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range []error{c.ErrUnsupported, c.ErrIncomplete, c.ErrContractStub} {
		if errors.Is(err, sentinel) {
			return sentinel
		}
	}
	for _, code := range []string{"infra_artifact_invalid", "infra_config_invalid", "infra_incompatible", "infra_permission_denied", "infra_policy_stale", "infra_not_enabled", "infra_busy", "infra_not_ready", "infra_budget_exceeded", "infra_result_unknown", "infra_recovery_required", "infra_internal"} {
		if err.Error() == code {
			return errors.New(code)
		}
	}
	return errors.New("infra_internal")
}

func caddyStatus(s c.Status, p c.Plan) bool {
	return s.Tx == p.Tx && s.Step == p.Step && s.Before == p.Before && s.Revision != 0 && s.Phase >= c.Prepared && s.Phase <= c.Halted
}

// The durable adapter must use a strict versioned codec. These semantic checks
// additionally prevent a truncated/re-keyed record from authorizing a later
// barrier, even when its individual DTOs can be decoded successfully.
// origin: hot-update S9 plan §2.2 / §7.2 / T22
func caddyTransitionValid(x CaddyTransition, p c.Plan) bool {
	if x.Version != 1 || x.Plan != p || x.Cursor > caddyFinished || x.Deadline <= 0 || (x.Cursor > caddyValidate && !x.HandoverStarted) {
		return false
	}
	if x.Cursor > caddyInspect && (x.Journal == (c.Digest{}) || x.HostRevision == 0 || x.Sequence == 0) {
		return false
	}
	if x.Cursor > caddyStartWarm && (!caddyProcess(x.Warm) || x.Warm == p.Before || x.Warm.ExeSHA256 != p.Candidate.BinarySHA256) {
		return false
	}
	if x.Cursor > caddyReleaseStart && (!caddyProcess(x.Main) || x.Main.ExeSHA256 != p.Candidate.BinarySHA256 || x.Main == x.Warm || (p.Action == c.Reload && x.Main != p.Before)) {
		return false
	}
	if x.Pending == nil {
		return x.Receipt == nil
	}
	r := *x.Pending
	encoded, err := c.EncodeRequest(r)
	if err != nil || len(r.Signature) != 64 || r.Tx != p.Tx || r.Step != p.Step || r.Domain != p.Domain || r.Component != c.Caddy || r.PolicyRevision != p.PolicyRevision || r.HostBootID != p.Before.BootID || (r.Candidate != (c.Artifact{}) && r.Candidate != p.Candidate) || (r.Config != (c.Digest{}) && r.Config != p.ConfigSHA256) || (r.Payload != (c.Digest{}) && r.Payload != caddyPlanDigest(p)) {
		return false
	}
	if x.Receipt == nil {
		return r.Sequence == x.Sequence+1 && r.ExpectedJournal == x.Journal
	}
	receipt := x.Receipt
	return receipt.Tx == p.Tx && receipt.Op == r.Op && receipt.Sequence == r.Sequence && r.Sequence == x.Sequence && receipt.Revision == x.HostRevision && receipt.Journal == x.Journal && receipt.RequestDigest == sha256.Sum256(encoded) && receipt.Applied && receipt.Code == ""
}

func (d *caddyDriver) budget(deadline c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return errors.New("infra_budget_exceeded")
	default:
	}
	now, err := d.now()
	if err != nil || deadline <= 0 || now >= int64(deadline) {
		return errors.New("infra_budget_exceeded")
	}
	return nil
}

func (d *caddyDriver) save(x CaddyTransition, deadline c.Deadline) error {
	return d.journal.StoreCaddy(x.Plan, x, deadline)
}

func (d *caddyDriver) phase(x *CaddyTransition, s *c.Status, phase c.Phase, proof c.Digest, deadline c.Deadline) error {
	if proof == (c.Digest{}) {
		return c.ErrIncomplete
	}
	next, err := d.deps.Tx.CompareAndSwap(*s, phase, proof, deadline)
	if err != nil {
		return err
	}
	if !caddyStatus(next, x.Plan) || next.Phase != phase || next.Revision <= s.Revision || next.Proof != proof {
		return errors.New("infra_result_unknown")
	}
	*s = next
	return nil
}

func (d *caddyDriver) record(x *CaddyTransition, kind string, evidence c.Digest) error {
	now, err := d.now()
	if err != nil {
		return err
	}
	o := c.Observation{Version: 1, Tx: x.Plan.Tx, Step: x.Plan.Step, Kind: kind,
		BootID: x.Plan.Before.BootID, BootNS: now, Subject: x.Plan.Domain,
		Generation: max(1, x.HostRevision), Evidence: evidence}
	if err := c.ValidateObservation(o); err != nil {
		return err
	}
	return d.deps.Observe.Record(o)
}

func (d *caddyDriver) fail(x *CaddyTransition, s c.Status, deadline c.Deadline, cause error) (c.Status, error) {
	code := "infra_internal"
	for _, known := range []string{"infra_unsupported", "infra_artifact_invalid", "infra_config_invalid", "infra_incompatible", "infra_permission_denied", "infra_policy_stale", "infra_not_enabled", "infra_busy", "infra_not_ready", "infra_inventory_incomplete", "infra_budget_exceeded", "infra_result_unknown", "infra_recovery_required"} {
		if cause.Error() == known {
			code = known
			break
		}
	}
	x.TimedOut = x.TimedOut || code == "infra_budget_exceeded"
	phase := c.Deferred
	if x.Changed || x.Pending != nil {
		phase = c.Halted
	}
	// No compensation is safe after redirect, a lost reply or an admin load.
	// In particular, neither an unconditional unflip nor RetireWarm belongs here.
	s.Code, s.Detail = code, "Caddy barrier requires recovery"
	proof := x.Journal
	if proof == (c.Digest{}) {
		proof = caddyPlanDigest(x.Plan)
	}
	// An expired adapter may refuse persistence. Return a truthful local status
	// and the persistence error; never give the local projection a new revision.
	phaseErr := d.phase(x, &s, phase, proof, deadline)
	s.Phase = phase
	saveErr := d.save(*x, deadline)
	observeErr := d.record(x, "halted", proof)
	if phaseErr != nil || saveErr != nil || observeErr != nil {
		return s, errors.New("infra_result_unknown")
	}
	return s, errors.New(code)
}

func caddyPlanDigest(p c.Plan) c.Digest {
	data, _ := json.Marshal(p) // Fixed, validated DTO; no fallible field encoders.
	return sha256.Sum256(append([]byte("tether-caddy-plan-v1\n"), data...))
}

func (d *caddyDriver) inspect(x *CaddyTransition, deadline c.Deadline) (c.Census, error) {
	census, err := d.deps.Host.Inspect(x.Plan.Domain, deadline)
	if err != nil {
		return c.Census{}, err
	}
	if !caddyCensus(census) || census.Generation < x.CensusGeneration || (census.Generation == x.CensusGeneration && census.Digest != x.CensusDigest) {
		return c.Census{}, c.ErrIncomplete
	}
	x.CensusGeneration, x.CensusDigest = census.Generation, census.Digest
	return census, nil
}

// A process-free or opaque census cannot prove a working ingress. Each socket
// owner must appear exactly, including invocation and executable identity.
func caddyCensus(census c.Census) bool {
	if !census.Complete || census.Generation == 0 || census.Digest == (c.Digest{}) || len(census.Processes) == 0 {
		return false
	}
	seen := make(map[c.Process]bool, len(census.Processes))
	for _, p := range census.Processes {
		if !caddyProcess(p) || seen[p] {
			return false
		}
		seen[p] = true
	}
	for _, socket := range census.Sockets {
		if !seen[socket.Owner] || socket.NetNS == 0 || socket.Device == 0 || socket.Inode == 0 || socket.Cookie == 0 {
			return false
		}
	}
	return true
}

func caddyProcess(p c.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks != 0 && p.InvocationID != "" && p.ExeDevice != 0 && p.ExeInode != 0 && p.ExeSHA256 != (c.Digest{}) && p.Cgroup != ""
}

func caddyPresent(census c.Census, expected c.Process) bool {
	for _, p := range census.Processes {
		if p == expected {
			return true
		}
	}
	return false
}

// This is deliberately stricter than WSS=0: slow HTTP, TLS handshakes, idle
// keepalive, late Upgrade and pending accepts remain the census producer's
// obligations until its ingress seal proves that they cannot acquire work.
// origin: hot-update S9 plan §5.2 / T22 / M12
func caddyDrained(census c.Census, expected c.Process) bool {
	if !caddyCensus(census) || census.OldOwnerPending != 0 {
		return false
	}
	for _, socket := range census.Sockets {
		if socket.Owner == expected {
			// Complete + zero pending is the producer's empty-queue proof.
			// Only a listener's explicit wildcard peer can remain until SIGTERM;
			// accepted, malformed and unknown socket responsibilities still block.
			// The caller separately verifies the current independent ingress seal.
			// origin: S9 internal review rv-infra F07
			peer, err := netip.ParseAddrPort(socket.Remote)
			local, localErr := netip.ParseAddrPort(socket.Local)
			if err != nil || localErr != nil || local.Port() == 0 || !peer.Addr().IsUnspecified() || peer.Port() != 0 {
				return false
			}
		}
	}
	return true
}
