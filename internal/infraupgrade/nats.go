// Package infraupgrade coordinates infrastructure owners through neutral contracts.
// The NATS driver owns ordering, not the implementation of a readiness or host
// receipt. In particular, a complete readiness result must already prove actual
// B/S/J placement, ordinary auth, supported persistence and current assets. This
// package cannot infer those facts from a broker count or from a version string.
//
// Root still verifies every expected incarnation and effect under its journal
// CAS. Keeping that boundary below the coordinator prevents a delayed controller
// from using a new systemd MainPID as the target of an old stop request.
package infraupgrade

import (
	"errors"
	"reflect"
	"runtime"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

var _ c.DriverFactory = NewNATS

// NewNATS constructs the non-root coordinator. Host must be the authenticated
// client adapter: it supplies its key/signature to the structured intents below,
// and its durable journal owns effect deduplication. No backend runs here.
// origin: hot-update S9 plan §2.1 / §4.2 / T29
func NewNATS(deps c.Dependencies) (c.Driver, error) {
	for _, dep := range []any{deps.Host, deps.Artifacts, deps.Ready, deps.Cohort,
		deps.Routes, deps.Transfers, deps.Recovery, deps.Tx, deps.Observe} {
		if natsMissing(dep) {
			return nil, c.ErrContractStub
		}
	}
	journal, _ := deps.Tx.(NATSJournal)
	return &natsDriver{deps: deps, journal: journal}, nil
}

func natsMissing(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// The mutex never waits behind network work. A concurrent caller gets busy;
// ownership remains with the original attempt until every short seal is released.
// A halted run retains checkpoints, including a lost-reply request's exact bytes.
type natsDriver struct {
	mu      sync.Mutex
	deps    c.Dependencies
	run     *natsRun
	journal NATSJournal
}

type natsRun struct {
	// These values belong to one immutable Tx/Step. A rollback gets a new plan
	// whose Before is the current owner; changing this plan would let old route
	// signatures or root requests authorize a different transition.
	plan           c.Plan
	status         c.Status
	deadline       c.Deadline
	stop           c.Stop
	actionDeadline c.Deadline
	timedOut       bool
	phasePending   *TransactionChange
	ready          c.Readiness
	census         c.Census
	cohort         c.Checkpoint
	recovery       c.Checkpoint
	transfers      c.TransferSeal
	// Outbound and return are separate signed purposes with separate original
	// deadlines. They must not share a retry counter that renews a route hold.
	outbound natsMigration
	inbound  natsMigration
	journal  c.Digest
	sequence uint64
	pending  *c.HostRequest
	// Receipt-backed progress is separate from public phase publication. Root
	// may have applied an effect even when S8 or the observation sink is down.
	// The flags prevent such a publication failure from sending another signal.
	cut           bool
	fenced        bool
	promoted      bool
	ldmApplied    bool
	stopApplied   bool
	startReleased bool
	oldExited     bool
	ackVerified   bool
	rebound       bool
	verified      bool
	returned      bool
	// Partial acquisition also creates cleanup responsibility. A missing handle
	// is retained as unknown, never represented by a successful zero-value release.
	cohortHeld    bool
	recoveryHeld  bool
	transfersHeld bool
}

// A migration keeps its original deadline, prepared nonces and actual move
// result across a lost Bind reply. Recovery must not dial a second generation.
type natsMigration struct {
	deadline     c.Deadline
	moveDeadline c.Deadline
	proofs       []c.RouteProof
	installed    int
	moved        *c.MoveResult
	bound        []c.RouteProof
	done         bool
}

// Advance owns one node, never a cluster loop. P20 may advance the next node only
// after this node returns Completed; a retained failure blocks even another Tx.
// origin: hot-update S9 plan §4.2 / §7.2 / T31
func (d *natsDriver) Advance(p c.Plan, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	if d.journal == nil {
		return natsRefusal(p, c.ErrContractStub), c.ErrContractStub
	}
	if err := natsPlan(p); err != nil {
		return natsRefusal(p, err), err
	}
	if !d.mu.TryLock() {
		err := natsError("infra_busy", nil)
		return natsRefusal(p, err), err
	}
	defer d.mu.Unlock()
	if d.run != nil {
		return d.run.status, natsError("infra_recovery_required", nil)
	}
	bounded, err := natsDeadline(deadline, upgradebudget.ComponentHandover)
	if err != nil {
		return natsRefusal(p, err), err
	}
	r := &natsRun{plan: p, deadline: bounded, stop: stop}
	if err := natsCheck(r); err != nil {
		return natsRefusal(p, err), err
	}
	r.status, err = d.deps.Tx.Acquire(p, bounded)
	if err != nil {
		return natsRefusal(p, err), natsError(natsCode(err), err)
	}
	// Acquire is the domain authority, including conflicts with image cuts and
	// another driver instance. A retained/terminal transaction is not a fresh
	// Prepared state; it must be queried through Recover instead of rolled again.
	// origin: hot-update S9 plan §7.2 / T31
	if r.status.Tx != p.Tx || r.status.Step != p.Step || r.status.Before != p.Before ||
		r.status.Phase != c.Prepared || r.status.Revision == 0 {
		return r.status, natsError("infra_recovery_required", nil)
	}
	d.run = r
	if err := d.save(r); err != nil {
		return d.fail(r, err)
	}
	if err := d.prepare(r); err != nil {
		return d.fail(r, err)
	}
	if err := d.cutover(r); err != nil {
		return d.fail(r, err)
	}
	if err := d.finish(r); err != nil {
		return d.fail(r, err)
	}
	d.run = nil
	return r.status, nil
}

func (d *natsDriver) prepare(r *natsRun) error {
	// Inspect precedes staging so an already replaced process cannot be used as
	// this step's before owner. Disk slots are not evidence of the running image.
	receipt, err := d.host(r, c.Inspect, c.Digest{})
	if err != nil {
		return err
	}
	if receipt.Current != r.plan.Before || !natsHasProcess(r.census, r.plan.Before) {
		return c.ErrIncomplete
	}
	if err := d.deps.Artifacts.Verify(r.plan.Candidate, natsActionDeadline(r), r.stop); err != nil {
		return err
	}
	if err := d.deps.Artifacts.Verify(r.plan.Previous, natsActionDeadline(r), r.stop); err != nil {
		return err
	}
	// Both directions read current state in the same store. A rollback is another
	// cut with actual Before/Previous, never restoration of a pre-upgrade backup.
	if err := d.deps.Artifacts.Compatible(r.plan.Previous, r.plan.Candidate, r.plan.ConfigSHA256, r.plan.Store, natsActionDeadline(r)); err != nil {
		return err
	}
	if err := d.deps.Artifacts.Compatible(r.plan.Candidate, r.plan.Previous, r.plan.ConfigSHA256, r.plan.Store, natsActionDeadline(r)); err != nil {
		return err
	}
	if err := d.record(r, "artifact_verified", r.plan.Candidate.BinarySHA256); err != nil {
		return err
	}
	if _, err := d.host(r, c.Stage, r.plan.Candidate.CatalogSHA256); err != nil {
		return err
	}
	if err := d.phase(r, c.Staged); err != nil {
		return err
	}
	ready, err := d.readReady(r, false)
	if err != nil {
		return err
	}
	r.ready = ready
	if err := d.phase(r, c.Ready); err != nil {
		return err
	}

	// Partial seals remain owned even when an adapter returns an error. Release
	// is by the original Tx/checkpoint; losing the handle can strand admission.
	// Transfers and recovery must name the same original invocation/receipt set.
	// A later independently sampled checkpoint could otherwise forget a sent,
	// unacknowledged attempt while still producing a valid-looking digest.
	// origin: hot-update S9 plan §4.3 / T29 / T30
	r.transfersHeld = true
	if err := d.save(r); err != nil {
		return err
	}
	r.transfers, err = d.deps.Transfers.Fence(r.plan.Tx, natsActionDeadline(r))
	r.transfersHeld = true
	if err != nil {
		return err
	}
	if !natsTransferComplete(r.transfers) {
		return c.ErrIncomplete
	}
	r.recoveryHeld = true
	if err := d.save(r); err != nil {
		return err
	}
	r.recovery, err = d.deps.Recovery.Seal(r.plan, natsActionDeadline(r))
	r.recoveryHeld = true
	if err != nil {
		return err
	}
	if !natsCheckpointComplete(r.recovery) || r.recovery.Transfers != r.transfers {
		return c.ErrIncomplete
	}
	r.cohortHeld = true
	if err := d.save(r); err != nil {
		return err
	}
	r.cohort, err = d.deps.Cohort.Seal(r.plan, natsActionDeadline(r))
	r.cohortHeld = true
	if err != nil {
		return err
	}
	if !natsCheckpointComplete(r.cohort) || r.cohort.Assets != r.recovery.Assets ||
		r.cohort.Transfers != r.transfers {
		return c.ErrIncomplete
	}
	if err := d.record(r, "asset_sealed", r.cohort.Proof); err != nil {
		return err
	}
	if err := d.phase(r, c.Moving); err != nil {
		return err
	}
	if err := d.install(r, c.RouteOutbound); err != nil {
		return err
	}
	if !natsStandalone(r.ready) {
		if err := d.move(r, c.RouteOutbound); err != nil {
			return err
		}
	}
	// This second read is under the real asset/cohort seals. A late data NC,
	// deleted/recreated consumer, or lost quorum invalidates the prepared cohort.
	// origin: hot-update S9 plan §4.2 / T29
	latest, err := d.readReady(r, false)
	if err != nil {
		return err
	}
	if latest.Topology != r.ready.Topology || latest.Seal != r.cohort.Assets ||
		natsStandalone(latest) != natsStandalone(r.ready) {
		return c.ErrIncomplete
	}
	return nil
}

func (d *natsDriver) install(r *natsRun, direction c.RouteDirection) error {
	// Prepare is cached before Install. A missing installation ACK may be queried
	// with the original nonce, but it is never permission to create a new hold.
	// Each participant receives its own intent, including independently owned NCs.
	// origin: hot-update S9 plan §4.5 / §4.6 / T29
	migration := natsMigrationFor(r, direction)
	if migration.deadline == 0 {
		deadline, err := natsDeadline(natsActionDeadline(r), upgradebudget.BrokerMaintenanceTTL)
		if err != nil {
			return err
		}
		migration.deadline = deadline
	}
	if migration.proofs == nil {
		if err := d.save(r); err != nil {
			return err
		}
		proofs, err := d.deps.Routes.Prepare(r.plan, r.cohort, direction, migration.deadline)
		if err != nil {
			return err
		}
		if len(proofs) == 0 {
			return c.ErrIncomplete
		}
		migration.proofs = proofs
	}
	if err := d.save(r); err != nil {
		return err
	}
	proofs := migration.proofs
	seen := make(map[c.ID]bool, len(proofs))
	for index, proof := range proofs {
		if !natsRouteValid(r, proof, direction, c.RoutePrepare) || seen[proof.Participant] || proof.New != (c.Attachment{}) {
			return c.ErrIncomplete
		}
		seen[proof.Participant] = true
		if index >= migration.installed {
			if _, err := controlbudget.Remaining(migration.deadline); err != nil {
				return natsError("infra_budget_exceeded", err)
			}
			if err := d.deps.Cohort.Install(r.cohort, proof, migration.deadline); err != nil {
				return err
			}
			migration.installed++
			if err := d.save(r); err != nil {
				return err
			}
		}
	}
	return d.record(r, "successor_installed", r.cohort.Proof)
}

func (d *natsDriver) move(r *natsRun, direction c.RouteDirection) error {
	// Cohort owns raw cancellation and same-generation query semantics if Move
	// itself loses a reply. Once its actual result is known, only Bind is retried:
	// another Move would obscure which attachment received the original ACK.
	// origin: hot-update S9 plan §4.5 / §7.2 / T31
	migration := natsMigrationFor(r, direction)
	if migration.done {
		return nil
	}
	if migration.moveDeadline == 0 {
		deadline, err := natsDeadline(migration.deadline, upgradebudget.RequestLatency)
		if err != nil {
			return err
		}
		migration.moveDeadline = deadline
	}
	deadline := migration.moveDeadline
	deadline = min(deadline, natsActionDeadline(r))
	if err := d.save(r); err != nil {
		return err
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return natsError("infra_budget_exceeded", err)
	}
	var err error
	if migration.moved == nil {
		var moved c.MoveResult
		if direction == c.RouteOutbound {
			moved, err = d.deps.Cohort.Move(r.cohort, deadline, r.stop)
		} else {
			moved, err = d.deps.Cohort.Return(r.cohort, deadline, r.stop)
		}
		if err != nil {
			return err
		}
		migration.moved = &moved
		if err := d.save(r); err != nil {
			return err
		}
	}
	moved := *migration.moved
	if !natsCensusComplete(moved.Census) || len(moved.Attachments) == 0 || len(moved.Attachments) > len(migration.proofs) {
		return c.ErrIncomplete
	}
	if (direction == c.RouteReturn || natsStandalone(r.ready)) && len(moved.Attachments) != len(migration.proofs) {
		return c.ErrIncomplete
	}
	if migration.bound == nil {
		proofs, err := d.deps.Routes.Bind(r.plan, moved, direction, deadline)
		if err != nil {
			return err
		}
		migration.bound = proofs
	}
	if err := d.save(r); err != nil {
		return err
	}
	proofs := migration.bound
	if len(proofs) != len(moved.Attachments) {
		return c.ErrIncomplete
	}
	seen := make(map[c.ID]bool, len(proofs))
	for _, proof := range proofs {
		if !natsRouteValid(r, proof, direction, c.RouteBind) || seen[proof.Participant] ||
			!natsBoundAttachment(proof, migration.proofs, moved.Attachments) {
			return c.ErrIncomplete
		}
		seen[proof.Participant] = true
	}
	census, err := d.deps.Cohort.Bind(r.cohort, proofs, deadline)
	if err != nil {
		return err
	}
	// Complete comes from the cohort owner, which knows roles. In N3 it seals
	// every movable client while the broker's local-only NC remains installed
	// but attached locally until restart. Return must retire all attachments.
	// origin: hot-update S9 plan §4.2 / §4.5 / T29
	allowLocalPending := direction == c.RouteOutbound && !natsStandalone(r.ready)
	if !natsCensusComplete(census) || (!allowLocalPending && census.OldOwnerPending != 0) || census.Generation != moved.Census.Generation {
		return c.ErrIncomplete
	}
	r.census = census
	migration.done = true
	return d.record(r, "attachment_changed", census.Digest)
}

func natsMigrationFor(r *natsRun, direction c.RouteDirection) *natsMigration {
	if direction == c.RouteOutbound {
		return &r.outbound
	}
	return &r.inbound
}

// These are consumer-side consistency checks, not signature verification.
// Routes produces signed evidence; Cohort.Install/Bind authenticate it against
// actual participants and their full sealed set. A nonempty byte slice alone
// never establishes that a caller possesses a signing key or a live connection.
// origin: hot-update S9 plan §4.6 / T29
func natsRouteValid(r *natsRun, proof c.RouteProof, direction c.RouteDirection, purpose c.RoutePurpose) bool {
	return proof.Version == 1 && proof.Tx == r.plan.Tx && proof.Participant != (c.ID{}) &&
		proof.Purpose == purpose && proof.Direction == direction && proof.Nonce != (c.ID{}) &&
		proof.PolicyRevision == r.plan.PolicyRevision && proof.AuthorityRevision != 0 &&
		proof.HomeDigest != (c.Digest{}) && proof.KeyID != "" && len(proof.Signature) != 0 &&
		proof.RemainingNS > 0 && proof.RemainingNS <= uint64(upgradebudget.BrokerMaintenanceTTL) &&
		proof.Old.Participant == proof.Participant && proof.Old.Generation != 0 &&
		proof.Target.Component == c.NATS && proof.Target.URL != "" && proof.Target.NodeID != ""
}

// Match a Bind to both its immutable Prepare and the actual MoveResult. The
// target may use the same NATS server behind a different ingress, so neither CID
// nor URL alone is sufficient. A forwarded ACK cannot stand in for another NC.
// The cohort owner performs the independent accepted-socket and home checks.
// origin: hot-update S9 plan §4.5 / §4.6 / T29
func natsBoundAttachment(bound c.RouteProof, prepared []c.RouteProof, attachments []c.Attachment) bool {
	for _, intent := range prepared {
		if intent.Participant != bound.Participant {
			continue
		}
		if bound.Nonce != intent.Nonce || bound.Old != intent.Old || bound.Target != intent.Target ||
			bound.HomeDigest != intent.HomeDigest || bound.AuthorityRevision != intent.AuthorityRevision ||
			bound.RemainingNS > intent.RemainingNS || bound.New.Participant != bound.Participant ||
			bound.New.Generation <= bound.Old.Generation || bound.New.HealthGeneration == 0 ||
			bound.New.CID == 0 || bound.New.ServerID == "" || bound.New.Raw == bound.Old.Raw ||
			bound.New.URL != bound.Target.URL || !natsProcessComplete(bound.New.Ingress) ||
			bound.New.Raw.Cookie == 0 || bound.New.Raw.Inode == 0 {
			return false
		}
		for _, actual := range attachments {
			if actual == bound.New {
				return true
			}
		}
	}
	return false
}

func natsPlan(p c.Plan) error {
	// The driver is portable Go; capability comes from the real host adapter,
	// never from a successful cross-build on an unsupported operating system.
	// origin: hot-update S9 plan lead LQ1 / T30
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return c.ErrUnsupported
	}
	if p.Component != c.NATS || (p.Action != c.Upgrade && p.Action != c.Rollback) {
		return c.ErrUnsupported
	}
	if p.Tx == (c.ID{}) || p.Domain == (c.ID{}) || p.Step == 0 || p.PolicyRevision == 0 ||
		p.ConfigSHA256 == (c.Digest{}) || !natsProcessComplete(p.Before) ||
		!natsArtifactComplete(p.Candidate) || !natsArtifactComplete(p.Previous) ||
		p.Previous.BinarySHA256 != p.Before.ExeSHA256 || p.Store.Device == 0 || p.Store.Inode == 0 ||
		p.Store.MountID == 0 || p.Store.LockDevice == 0 || p.Store.LockInode == 0 {
		return natsError("infra_config_invalid", nil)
	}
	return nil
}

func natsArtifactComplete(a c.Artifact) bool {
	return a.Kind == c.NATSBinary && a.Version != "" && a.GOOS == "linux" && a.GOARCH == "amd64" &&
		a.ArchiveSHA256 != (c.Digest{}) && a.BinarySHA256 != (c.Digest{}) && a.CatalogSHA256 != (c.Digest{})
}

// Missing identity is unknown, including a missing invocation on a reused PID.
// Artifact verification still belongs to the root/readiness owners: these field
// checks do not turn a caller-provided SHA into a measured /proc/exe receipt.
// origin: hot-update S9 plan §2.3 / T31
func natsProcessComplete(p c.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" &&
		p.ExeDevice > 0 && p.ExeInode > 0 && p.ExeSHA256 != (c.Digest{}) && p.Cgroup != ""
}

// A proven empty process set is useful only while the startup fence is closed.
// It still needs an authoritative generation/digest and Complete flag. Initial
// admission and final completion separately require the expected live process;
// an empty final census cannot prove that the successor is serving.
// origin: hot-update S9 plan §2.3 / T31
func natsCensusComplete(census c.Census) bool {
	if !census.Complete || census.Generation == 0 || census.Digest == (c.Digest{}) {
		return false
	}
	for _, process := range census.Processes {
		if !natsProcessComplete(process) {
			return false
		}
	}
	for _, socket := range census.Sockets {
		if !natsProcessComplete(socket.Owner) || socket.Cookie == 0 || socket.Inode == 0 {
			return false
		}
	}
	return true
}

func natsHasProcess(census c.Census, want c.Process) bool {
	for _, process := range census.Processes {
		if process == want {
			return true
		}
	}
	return false
}

func natsAssetComplete(seal c.AssetSeal) bool {
	return seal.Complete && seal.Revision > 0 && seal.Accounts != (c.Digest{}) &&
		seal.Assets != (c.Digest{}) && seal.Participants != (c.Digest{})
}

// A seal for zero active transfers still carries the digest of the observed
// empty sets. Default zero fields mean no producer supplied evidence, not that
// there were no transfer obligations. The source/schema bind the S7 adapter.
// origin: hot-update S9 plan §2.1 / §4.3 / T29
func natsTransferComplete(seal c.TransferSeal) bool {
	return seal.Complete && seal.Version == 1 && seal.SourceSHA256 != (c.Digest{}) &&
		seal.SchemaSHA256 != (c.Digest{}) && seal.InvocationSet != (c.Digest{}) &&
		seal.ReceiptSet != (c.Digest{}) && seal.ConsumerSet != (c.Digest{})
}

func natsCheckpointComplete(checkpoint c.Checkpoint) bool {
	return checkpoint.Cohort != (c.ID{}) && checkpoint.Proof != (c.Digest{}) &&
		natsAssetComplete(checkpoint.Assets) && natsTransferComplete(checkpoint.Transfers)
}

func natsCheck(r *natsRun) error {
	// Stop is admission cancellation only. The driver never closes a business
	// resource or gives an owner this channel as its lifetime root.
	select {
	case <-r.stop:
		return natsError("infra_budget_exceeded", nil)
	default:
	}
	if _, err := controlbudget.Remaining(natsActionDeadline(r)); err != nil {
		return natsError("infra_budget_exceeded", err)
	}
	return nil
}

// Errors retain their cause for errors.Is but never copy adapter error strings
// into public status, logs, or the returned message (which may contain secrets).
type natsFailure struct {
	code  string
	cause error
}

func (e *natsFailure) Error() string           { return e.code }
func (e *natsFailure) Unwrap() error           { return e.cause }
func natsError(code string, cause error) error { return &natsFailure{code: code, cause: cause} }

func natsCode(err error) string {
	var failure *natsFailure
	if errors.As(err, &failure) {
		return failure.code
	}
	if errors.Is(err, c.ErrUnsupported) {
		return "infra_unsupported"
	}
	if errors.Is(err, c.ErrIncomplete) {
		return "infra_inventory_incomplete"
	}
	if errors.Is(err, c.ErrContractStub) {
		return "infra_not_enabled"
	}
	// The neutral contract has no shared typed error beyond its three sentinels.
	// Preserve only an exact closed code; never parse or publish arbitrary detail.
	if err != nil {
		switch err.Error() {
		case "infra_unsupported", "infra_artifact_invalid", "infra_config_invalid", "infra_incompatible",
			"infra_permission_denied", "infra_policy_stale", "infra_not_enabled", "infra_busy", "infra_not_ready",
			"infra_inventory_incomplete", "infra_budget_exceeded", "infra_result_unknown", "infra_recovery_required", "infra_internal":
			return err.Error()
		}
	}
	return "infra_internal"
}

func natsRefusal(p c.Plan, err error) c.Status {
	return c.Status{Tx: p.Tx, Step: p.Step, Before: p.Before, Phase: c.Deferred, Code: natsCode(err)}
}
