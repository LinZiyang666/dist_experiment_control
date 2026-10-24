package infraupgrade

import (
	"crypto/rand"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// Recover never resets the before incarnation, refreshes a route lease, or
// blindly re-signals a new MainPID. A lost effect reply is queried using exactly
// the original request/sequence; the root journal decides whether it applied.
// origin: hot-update S9 plan §7.2 / T31
func (d *natsDriver) Recover(p c.Plan, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	if d.journal == nil {
		return natsRefusal(p, c.ErrContractStub), c.ErrContractStub
	}
	if err := natsPlan(p); err != nil {
		return natsRefusal(p, err), err
	}
	if !d.mu.TryLock() {
		return natsRefusal(p, natsError("infra_busy", nil)), natsError("infra_busy", nil)
	}
	defer d.mu.Unlock()
	status, err := d.deps.Tx.Load(p.Tx, deadline)
	if err != nil {
		return natsRefusal(p, err), natsError(natsCode(err), err)
	}
	if status.Tx != p.Tx || status.Step != p.Step || status.Before != p.Before || status.Revision == 0 {
		return status, natsError("infra_recovery_required", nil)
	}
	r, err := restoreNATSRecovery(d, p, status, deadline)
	if err != nil {
		return status, err
	}
	if status.Phase == c.Completed {
		return recoverNATSTerminal(d, r, status, deadline, stop)
	}
	// Recovery owns a bounded action, not a renewed normal handover or route
	// hold. Keep the original timeout permanently in the transaction sidecar.
	if _, err := controlbudget.Remaining(r.deadline); err != nil {
		r.timedOut = true
	}
	r.actionDeadline, err = natsDeadline(deadline, upgradebudget.ComponentHandover)
	if err != nil {
		return status, err
	}
	r.stop = stop
	if err := natsCheck(r); err != nil {
		return d.fail(r, err)
	}
	if err := d.save(r); err != nil {
		return d.fail(r, err)
	}
	if r.phasePending != nil {
		if err := d.phase(r, r.phasePending.Phase); err != nil {
			return d.fail(r, err)
		}
	}
	// Current physical evidence is required even when the last receipt says a
	// successor started. Never infer current ownership from yesterday's status.
	census, err := d.inspect(r)
	if err != nil {
		return d.fail(r, err)
	}
	if r.oldExited && natsOwnsOld(census, p.Before) {
		return d.fail(r, c.ErrIncomplete)
	}
	if r.status.Phase == c.Cutting {
		r.cut = true
	}
	if r.pending != nil {
		// Query the journal even when an independent census already says the
		// old process exited: an unacknowledged request is still a responsibility.
		if _, err := d.host(r, r.pending.Op, r.pending.Payload); err != nil {
			return d.fail(r, err)
		}
	}
	if !r.cut {
		return recoverNATSBeforeCut(d, r)
	}
	if !r.startReleased {
		if err := d.cutover(r); err != nil {
			return d.fail(r, err)
		}
	}
	if err := d.finish(r); err != nil {
		return d.fail(r, err)
	}
	if err := d.save(r); err != nil {
		return d.fail(r, err)
	}
	d.run = nil
	return r.status, nil
}

// restoreNATSRecovery reconciles a saved phase intent with its exact durable
// revision before any recovery action may use the sidecar's held resources.
// origin: hot-update S9 plan §7.2 / T31; S10 LINT-L4 responsibility split
func restoreNATSRecovery(d *natsDriver, p c.Plan, status c.Status, deadline c.Deadline) (*natsRun, error) {
	r := d.run
	if r == nil {
		// P00 has no durable checkpoint/request loader: Status contains only a
		// digest, and HostReceipt has no phase/checkpoint. A restarted controller
		// cannot reconstruct them safely by calling Seal again. Keep the durable
		// status and require the transaction/root owner to restore that evidence.
		x, err := d.journal.LoadNATS(p, deadline)
		if err != nil || !natsTransitionValid(x, p) {
			return nil, natsError("infra_recovery_required", err)
		}
		r = natsRestore(x)
		d.run = r
	}
	if r.plan != p {
		return nil, natsError("infra_recovery_required", nil)
	}
	// A phase publication may have committed before its reply or sidecar write.
	// Only the exact saved intent can explain a newer durable revision.
	// origin: S9 internal review rv-infra F08
	if intent := r.phasePending; intent != nil && status.Revision > r.status.Revision &&
		status.Phase == intent.Phase && status.Proof == intent.Proof {
		r.status, r.phasePending = status, nil
		if status.Phase == c.Cutting {
			r.cut = true
		}
	}
	if status.Revision != r.status.Revision || status.Phase != r.status.Phase || status.Proof != r.status.Proof {
		return nil, natsError("infra_recovery_required", nil)
	}
	return r, nil
}

// recoverNATSTerminal is a fresh physical query, with no renewed handover lease
// and no retroactive timeout of the original completed transaction.
// origin: hot-update S9 plan §7.2 / T31; S10 LINT-L4 responsibility split
func recoverNATSTerminal(d *natsDriver, r *natsRun, status c.Status, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	var err error
	// Terminal queries must not retroactively mark a timely normal handover
	// as timed out just because this later recovery command arrives late.
	r.actionDeadline, err = natsDeadline(deadline, upgradebudget.ComponentHandover)
	if err != nil {
		return status, err
	}
	r.stop = stop
	census, err := d.inspect(r)
	if err != nil || !r.returned || r.pending != nil || !r.oldExited || r.cohortHeld || r.recoveryHeld || r.transfersHeld ||
		natsOwnsOld(census, r.plan.Before) || census.OldOwnerPending != 0 || !natsHasProcess(census, r.status.Current) {
		return status, natsError("infra_recovery_required", err)
	}
	d.run = nil
	return r.status, nil
}

// recoverNATSBeforeCut releases only the original partial seals. It cannot mint
// another checkpoint or drop a transfer fence before its recovery seal settles.
// origin: hot-update S9 plan §7.2 / T31; S10 LINT-L4 responsibility split
func recoverNATSBeforeCut(d *natsDriver, r *natsRun) (c.Status, error) {
	var err error
	// A pre-cut recovery only releases the original partial seals. It does
	// not manufacture a second checkpoint and silently restart an attempt.
	// Completing an interrupted Seal is explicit recovery of the same plan,
	// under its still-held transfer fence. A nonzero checkpoint is immutable.
	// The whole cleanup shares one business budget, including a partial join.
	// origin: S9 internal review s9-p18 F1
	r.actionDeadline, err = natsDeadline(natsActionDeadline(r), upgradebudget.RequestLatency)
	if err != nil {
		return d.fail(r, err)
	}
	if r.recoveryHeld && r.recovery == (c.Checkpoint{}) {
		if !r.transfersHeld || !natsTransferComplete(r.transfers) {
			return d.fail(r, c.ErrIncomplete)
		}
		r.recovery, err = d.deps.Recovery.Seal(r.plan, natsActionDeadline(r))
		if err != nil {
			return d.fail(r, err)
		}
	}
	if err := d.release(r); err != nil {
		return d.fail(r, err)
	}
	if err := d.phase(r, c.Deferred); err != nil {
		return d.fail(r, err)
	}
	d.run = nil
	return r.status, natsError("infra_not_ready", nil)
}

func natsDeadline(parent c.Deadline, limit time.Duration) (c.Deadline, error) {
	// Clip once and retain the result in the run/migration. Recomputing this
	// helper after a failed attempt would otherwise grant the full budget again.
	// Every child operation receives local absolute time, never a peer's clock.
	if _, err := controlbudget.Remaining(parent); err != nil {
		return 0, natsError("infra_budget_exceeded", err)
	}
	local, err := controlbudget.After(limit)
	if err != nil {
		return 0, natsError("infra_budget_exceeded", err)
	}
	if parent < local {
		return parent, nil
	}
	return local, nil
}

func (d *natsDriver) host(r *natsRun, op c.HostOp, payload c.Digest) (c.HostReceipt, error) {
	// This is a structured local intent, not an unauthenticated wire request.
	// The injected non-root client must resolve immutable payload references,
	// add the active key, sign the full canonical request, and verify the receipt
	// digest before returning. Binding a raw privileged backend here is invalid.
	// The root backend must re-read census/checkpoint references under its CAS;
	// a controller's nonzero digest alone cannot authorize an effect.
	// origin: hot-update S9 plan §2.1 / §2.2 / T29 / T31
	if err := natsCheck(r); err != nil {
		return c.HostReceipt{}, err
	}
	if r.pending == nil {
		remaining, err := controlbudget.Remaining(natsActionDeadline(r))
		if err != nil {
			return c.HostReceipt{}, natsError("infra_budget_exceeded", err)
		}
		r.sequence++
		request := c.HostRequest{Version: c.RequestVersion, Op: op, Component: c.NATS,
			Tx: r.plan.Tx, Step: r.plan.Step, Domain: r.plan.Domain, Sequence: r.sequence,
			PolicyRevision: r.plan.PolicyRevision, HostBootID: r.plan.Before.BootID,
			Expected: r.plan.Before, ExpectedJournal: r.journal, RemainingNS: uint64(remaining)}
		if _, err := rand.Read(request.Nonce[:]); err != nil {
			return c.HostReceipt{}, natsError("infra_internal", err)
		}
		// Follow P00's required/forbidden operands. Credentials are not held by
		// this business coordinator; only the bound client adapter may sign.
		switch op {
		case c.Inspect:
		case c.Stage, c.Promote, c.ReleaseStart:
			request.Candidate, request.Store, request.Config = r.plan.Candidate, r.plan.Store, r.plan.ConfigSHA256
			request.Payload = payload
		case c.FenceStart, c.SignalLDM, c.SignalStop:
			request.BeforeCensus, request.Payload = r.census.Digest, payload
		case c.SignalGraceful, c.StartWarm, c.SwitchIngress, c.FinalizeStorage, c.RetireWarm,
			c.RecoverHost, c.RotatePolicy, c.UpdateSupport, c.LaunchClaim, c.LaunchAck, c.VerifyIngress:
			// origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
			return c.HostReceipt{}, c.ErrUnsupported
		default:
			return c.HostReceipt{}, c.ErrUnsupported
		}
		r.pending = &request
	}
	request := *r.pending
	if request.Op != op {
		return c.HostReceipt{}, natsError("infra_result_unknown", nil)
	}
	// Persist the exact nonce, sequence and operands before sending. Recovery
	// queries this request, even if root committed before the controller died.
	// origin: S9 internal review rv-infra F08
	if err := d.save(r); err != nil {
		return c.HostReceipt{}, err
	}
	receipt, err := d.deps.Host.Do(request, natsActionDeadline(r), r.stop)
	if err != nil {
		if errors.Is(err, c.ErrUnsupported) || errors.Is(err, c.ErrContractStub) {
			// These contract refusals promise no effect; an unavailable backend
			// must not look like an ambiguous signal or advertise hot capability.
			r.pending = nil
			return c.HostReceipt{}, err
		}
		// Never regenerate sequence, nonce, or RemainingNS after a lost reply.
		return c.HostReceipt{}, natsError("infra_result_unknown", err)
	}
	// Receipt identity is necessary even after transport authentication. A late
	// reply for a previous sequence, or an unapplied mutation, cannot advance the
	// local cursor. Retain the pending request so recovery can query its result.
	if receipt.Tx != request.Tx || receipt.Sequence != request.Sequence || receipt.Op != request.Op ||
		receipt.Revision == 0 || receipt.RequestDigest == (c.Digest{}) || receipt.Journal == (c.Digest{}) ||
		receipt.Code != "" || (op != c.Inspect && !receipt.Applied) || !natsCensusComplete(receipt.Census) {
		return c.HostReceipt{}, natsError("infra_result_unknown", nil)
	}
	r.pending = nil
	r.journal, r.census = receipt.Journal, receipt.Census
	// Mark actual effects before recording telemetry. A failed observer must
	// not turn a known-applied signal into a new request on the next recovery.
	switch op {
	case c.FenceStart:
		r.fenced = true
	case c.Promote:
		r.promoted = true
	case c.SignalLDM:
		r.ldmApplied = true
	case c.SignalStop:
		r.stopApplied = true
	case c.ReleaseStart:
		r.startReleased = true
	case c.Inspect, c.Stage, c.SignalGraceful, c.StartWarm, c.SwitchIngress, c.FinalizeStorage,
		c.RetireWarm, c.RecoverHost, c.RotatePolicy, c.UpdateSupport, c.LaunchClaim, c.LaunchAck:
	case c.VerifyIngress: // origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
		return receipt, c.ErrUnsupported
	default:
		return receipt, c.ErrUnsupported
	}
	if err := d.save(r); err != nil {
		return receipt, err
	}
	if op == c.SignalLDM || op == c.SignalStop {
		if err := d.record(r, "signal_applied", receipt.RequestDigest); err != nil {
			return receipt, err
		}
	}
	return receipt, nil
}

func (d *natsDriver) inspect(r *natsRun) (c.Census, error) {
	// Inspect queries the independent physical producer, not Status.Census.
	// This read has no signal or selector side effect and must be safe to repeat.
	if err := natsCheck(r); err != nil {
		return c.Census{}, err
	}
	census, err := d.deps.Host.Inspect(r.plan.Domain, natsActionDeadline(r)) // origin: S9 internal review rv-infra F05
	if err != nil {
		return c.Census{}, err
	}
	if !natsCensusComplete(census) {
		return c.Census{}, c.ErrIncomplete
	}
	r.census = census
	return census, nil
}

func (d *natsDriver) phase(r *natsRun, phase c.Phase) error {
	// A root effect and S8 publication are different commits. Only publish the
	// journal actually returned by root, then verify the acknowledged revision.
	// If that publication is ambiguous, the driver retains responsibility; it
	// does not infer success from the requested phase or a local increment.
	// origin: hot-update S9 plan §2.3 / §7.2 / T31
	if err := natsCheck(r); err != nil {
		return err
	}
	before := r.status
	if phase != c.Halted && phase != c.Deferred {
		before.Code, before.Detail = "", ""
	}
	if r.timedOut {
		before.Code, before.Detail = "infra_budget_exceeded", "Original NATS handover exceeded its deadline; recovery retired the remaining responsibilities"
	}
	r.phasePending = &TransactionChange{Phase: phase, Proof: r.journal}
	if err := d.save(r); err != nil {
		return err
	}
	next, err := d.deps.Tx.CompareAndSwap(before, phase, r.journal, natsActionDeadline(r))
	if err != nil {
		return err
	}
	if next.Tx != before.Tx || next.Step != before.Step || next.Before != before.Before ||
		next.Phase != phase || next.Revision <= before.Revision || next.Proof != r.journal {
		return natsError("infra_result_unknown", nil)
	}
	r.status = next
	r.status.Census = r.census
	if r.timedOut {
		r.status.Code, r.status.Detail = before.Code, before.Detail
	}
	r.phasePending = nil
	return d.save(r)
}

func (d *natsDriver) record(r *natsRun, kind string, evidence c.Digest) error {
	// Observations carry producer evidence, not a digest of our own Status. Full
	// process/socket details stay behind the injected owners' private evidence
	// boundary. A failed sink is an actual failed barrier, never best-effort success.
	if evidence == (c.Digest{}) {
		return c.ErrIncomplete
	}
	if err := d.save(r); err != nil {
		return err
	}
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	return d.deps.Observe.Record(c.Observation{Version: 1, Tx: r.plan.Tx, Step: r.plan.Step,
		Kind: kind, BootID: r.plan.Before.BootID, BootNS: now, Subject: r.plan.Domain,
		Generation: r.census.Generation, Evidence: evidence})
}

// Each release may have partially succeeded. Clear a held bit only after its
// owner acknowledges; recovery reuses that exact seal rather than minting IDs.
func (d *natsDriver) release(r *natsRun) error {
	// Release attempts are independent: a cohort failure does not justify leaking
	// an acquired transfer seal too. Unknown identities are the exception; calling
	// a release with a zero handle could operate on an unrelated/current owner.
	// origin: hot-update S9 plan §7.2 / T29 / TestNATSDriverLifecycle
	var failures []error
	if err := d.save(r); err != nil {
		return err
	}
	if r.cohortHeld {
		if r.cohort.Cohort == (c.ID{}) {
			failures = append(failures, c.ErrIncomplete)
		} else if err := d.deps.Cohort.Release(r.cohort, natsActionDeadline(r)); err != nil {
			failures = append(failures, err)
		} else {
			r.cohortHeld = false
		}
	}
	if r.recoveryHeld {
		// Even an unchanged old server can have lost an acknowledged write.
		// Verify the retained receipt before any producer can repair that loss;
		// incomplete seals stay fenced until an explicit same-plan Recover.
		// origin: hot-update S9 plan §4.3 / T40; S9 internal review s9-p18 F1
		deadline, err := natsDeadline(natsActionDeadline(r), upgradebudget.RequestLatency)
		if err != nil {
			failures = append(failures, err)
		} else if !natsCheckpointComplete(r.recovery) {
			failures = append(failures, c.ErrIncomplete)
		} else if err := d.deps.Recovery.VerifyBeforeReplay(r.recovery, deadline, r.stop); err != nil {
			failures = append(failures, err)
		} else if err := d.deps.Recovery.Release(r.recovery, deadline); err != nil {
			failures = append(failures, err)
		} else {
			r.recoveryHeld = false
		}
	}
	// Transfer receipts are inputs to a partial recovery seal. Releasing them
	// while recovery is still held would make same-plan completion impossible.
	// origin: S9 internal review s9-p18 F1
	if r.transfersHeld && !r.recoveryHeld {
		if r.transfers.InvocationSet == (c.Digest{}) {
			failures = append(failures, c.ErrIncomplete)
		} else if err := d.deps.Transfers.Release(r.transfers, natsActionDeadline(r)); err != nil {
			failures = append(failures, err)
		} else {
			r.transfersHeld = false
		}
	}
	failures = append(failures, d.save(r))
	return errors.Join(failures...)
}

func (d *natsDriver) fail(r *natsRun, cause error) (c.Status, error) {
	// A failed phase publication keeps its intent until explicit recovery can
	// compare it with durable status. Replacing it with Halted could hide a CAS
	// that actually committed. Failed/expired persistence never grants an effect.
	// origin: S9 internal review rv-infra F08
	if r.phasePending != nil {
		_ = d.save(r)
		projection := r.status
		projection.Phase, projection.Code = c.Halted, "infra_recovery_required"
		return projection, natsError(projection.Code, cause)
	}
	phase, code := c.Halted, "infra_recovery_required"
	if !r.cut && r.pending == nil {
		if err := d.release(r); err == nil {
			phase, code = c.Deferred, natsCode(cause)
		} else {
			cause = errors.Join(cause, err)
		}
	}
	// On expiry or failed CAS, keep the last durable revision and retain this
	// owner. The caller still receives a non-success status and a recovery error.
	if err := d.phase(r, phase); err != nil {
		cause = errors.Join(cause, err)
		phase, code = c.Halted, "infra_recovery_required"
	} else if phase == c.Deferred {
		d.run = nil
	}
	projection := r.status
	projection.Phase, projection.Code = phase, code
	projection.Detail = "NATS transition did not complete; inspect the transaction before continuing"
	if err := d.record(r, "halted", r.journal); err != nil {
		cause = errors.Join(cause, err)
	}
	return projection, natsError(code, cause)
}
