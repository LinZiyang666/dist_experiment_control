package infraupgrade

import (
	"crypto/sha256"
	"errors"
	"reflect"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// CaddyReloader is implemented by the non-root readiness/admin adapter. Validate
// must reject legacy root admin, unsupported configuration and bad certificates
// before redirect. Reload uses the already authenticated invocation socket and
// queries the loaded digest after a lost reply; it must not redial a successor.
// Neither the root HostOp vocabulary nor ReadyProbe.Read authorizes /load.
// origin: hot-update S9 plan §5.1 / §5.4 / T22
type CaddyReloader interface {
	ValidateCaddyReload(c.Plan, c.Deadline, c.Stop) error
	ReloadCaddy(c.Plan, c.Process, c.Deadline, c.Stop) (c.Digest, error)
}

// Cursor values are persisted, append-only private state. The phase vocabulary
// remains P00's; a no-op reload barrier still carries the validated config proof.
const (
	caddyValidate = iota
	caddyInspect
	caddyStage
	caddyFence
	caddyStartWarm
	caddyWarmReady
	caddySealOutbound
	caddyPrepareOutbound
	caddyRedirectWarm
	caddyMoveOutbound
	caddyDrainOld
	caddyPromote
	caddyCut
	caddyOldExit
	caddyFinalizeStorage
	caddyReleaseStart
	caddyMainReady
	caddySealReturn
	caddyPrepareReturn
	caddyRedirectMain
	caddyMoveReturn
	caddyDrainWarm
	caddyRetireWarm
	caddyWarmExit
	caddyReleaseCohort
	caddyComplete
	caddyFinished
)

// barrier runs the step the persisted cursor names, and is the ONE switch that
// names every cursor member: a new cursor must be routed here deliberately. The
// cursor-specific steps are grouped by the invocation they act on: the outbound
// handover to warm, the cutover of the original invocation, and the return to
// main. Seal, prepare and move are the same barrier in both directions; cohort
// release and completion close the transition.
func (d *caddyDriver) barrier(x *CaddyTransition, s *c.Status, deadline c.Deadline, stop c.Stop) error {
	switch x.Cursor {
	case caddyValidate:
		return d.validate(x, deadline, stop)
	case caddyInspect, caddyStage, caddyFence, caddyStartWarm, caddyWarmReady, caddyRedirectWarm:
		return d.outboundBarrier(x, s, deadline, stop)
	case caddySealOutbound, caddySealReturn:
		return d.seal(x, deadline)
	case caddyPrepareOutbound, caddyPrepareReturn:
		return d.prepare(x, deadline)
	case caddyMoveOutbound, caddyMoveReturn:
		return d.move(x, deadline, stop)
	case caddyDrainOld, caddyPromote, caddyCut, caddyOldExit, caddyFinalizeStorage, caddyReleaseStart:
		return d.cutoverBarrier(x, s, deadline, stop)
	case caddyMainReady, caddyRedirectMain, caddyDrainWarm, caddyRetireWarm, caddyWarmExit:
		return d.returnBarrier(x, s, deadline, stop)
	case caddyReleaseCohort:
		return d.deps.Cohort.Release(x.Checkpoint, deadline)
	case caddyComplete:
		return d.complete(x, s, deadline, stop)
	case caddyFinished:
		return c.ErrIncomplete // Terminal: there is no step left to run.
	default:
		// The cursor is persisted private state; a value outside the vocabulary
		// is never a step to run.
		return c.ErrIncomplete
	}
}

// outboundBarrier owns the steps that stand up the warm invocation and point
// ingress at it while the original invocation is still the owner. barrier
// routes only those cursors here; any other is not a step of this group.
func (d *caddyDriver) outboundBarrier(x *CaddyTransition, s *c.Status, deadline c.Deadline, stop c.Stop) error {
	p := x.Plan
	switch x.Cursor {
	case caddyInspect:
		if _, err := d.host(x, c.Inspect, p.Before, deadline, stop); err != nil {
			return err
		}
		census, err := d.inspect(x, deadline)
		if err != nil {
			return err
		}
		if !caddyPresent(census, p.Before) {
			return c.ErrIncomplete
		}
		x.BeforeCensus = census
		return nil
	case caddyStage:
		if _, err := d.host(x, c.Stage, p.Before, deadline, stop); err != nil {
			return err
		}
		return d.phase(x, s, c.Staged, x.Journal, deadline)
	case caddyFence:
		if p.Action == c.Reload {
			return nil // Config validation is the explicit no-binary-change proof.
		}
		_, err := d.host(x, c.FenceStart, p.Before, deadline, stop)
		return err
	case caddyStartWarm:
		receipt, err := d.host(x, c.StartWarm, p.Before, deadline, stop)
		if err != nil {
			return err
		}
		x.Warm, err = d.started(x, receipt.Current, true, deadline, stop)
		return err
	case caddyWarmReady:
		census, err := d.ready(x, x.Warm, deadline, stop)
		if err != nil {
			return err
		}
		x.WarmCensus = census
		return d.phase(x, s, c.Ready, census.Digest, deadline)
	case caddyRedirectWarm:
		if _, err := d.host(x, c.SwitchIngress, p.Before, deadline, stop); err != nil {
			return err
		}
		return d.phase(x, s, c.Moving, x.Journal, deadline)
	}
	return c.ErrIncomplete
}

// cutoverBarrier owns the steps that drain, cut and retire the original
// invocation and, for an upgrade, release the startup of the new main. barrier
// routes only those cursors here; any other is not a step of this group.
func (d *caddyDriver) cutoverBarrier(x *CaddyTransition, s *c.Status, deadline c.Deadline, stop c.Stop) error {
	p := x.Plan
	switch x.Cursor {
	case caddyDrainOld:
		return d.drain(x, p.Before, deadline, stop, false)
	case caddyPromote:
		if p.Action != c.Reload {
			if _, err := d.host(x, c.Promote, p.Before, deadline, stop); err != nil {
				return err
			}
		}
		return d.phase(x, s, c.Cutting, x.Journal, deadline)
	case caddyCut:
		// Recheck immediately before the destructive call. The root must also
		// resolve this exact census under CAS, sealing late accepts/conntrack.
		if err := d.drain(x, p.Before, deadline, stop, false); err != nil {
			return err
		}
		if p.Action == c.Reload {
			return d.reload(x, deadline, stop)
		}
		if _, err := d.host(x, c.SignalGraceful, p.Before, deadline, stop); err != nil {
			return err
		}
		return d.record(x, "signal_applied", x.Journal)
	case caddyOldExit:
		census, err := d.inspect(x, deadline)
		if err != nil {
			return err
		}
		if !caddyDrained(census, p.Before) || (p.Action != c.Reload && caddyPresent(census, p.Before)) {
			return c.ErrIncomplete
		}
		x.BeforeCensus = census
		if p.Action != c.Reload {
			if err := d.record(x, "old_exit", census.Digest); err != nil {
				return err
			}
		}
		return d.phase(x, s, c.Verifying, census.Digest, deadline)
	case caddyFinalizeStorage:
		if p.Action == c.Reload {
			return nil // The existing writer and current tree remain owned.
		}
		if _, err := d.host(x, c.FinalizeStorage, p.Before, deadline, stop); err != nil {
			return err
		}
		return d.record(x, "storage_finalized", x.Journal)
	case caddyReleaseStart:
		if p.Action == c.Reload {
			return nil
		}
		receipt, err := d.host(x, c.ReleaseStart, p.Before, deadline, stop)
		if err != nil {
			return err
		}
		x.Main, err = d.started(x, receipt.Current, false, deadline, stop)
		if err != nil {
			return err
		}
		return d.record(x, "startup_released", x.Journal)
	}
	return c.ErrIncomplete
}

// returnBarrier owns the steps that move ingress back to main and retire the
// warm invocation once it has drained. barrier routes only those cursors here;
// any other is not a step of this group.
func (d *caddyDriver) returnBarrier(x *CaddyTransition, s *c.Status, deadline c.Deadline, stop c.Stop) error {
	switch x.Cursor {
	case caddyMainReady:
		census, err := d.ready(x, x.Main, deadline, stop)
		if err != nil {
			return err
		}
		x.BeforeCensus = census
		return d.phase(x, s, c.Returning, census.Digest, deadline)
	case caddyRedirectMain:
		// Main readiness is a bypass proof for this incarnation, not a
		// successful canonical connection which could still hit the shadow.
		if _, err := d.ready(x, x.Main, deadline, stop); err != nil {
			return err
		}
		_, err := d.host(x, c.SwitchIngress, x.Main, deadline, stop)
		return err
	case caddyDrainWarm:
		if err := d.drain(x, x.Warm, deadline, stop, true); err != nil {
			return err
		}
		return d.phase(x, s, c.Retiring, x.ReturnProof, deadline)
	case caddyRetireWarm:
		if err := d.drain(x, x.Warm, deadline, stop, true); err != nil {
			return err
		}
		_, err := d.host(x, c.RetireWarm, x.Warm, deadline, stop)
		return err
	case caddyWarmExit:
		census, err := d.inspect(x, deadline)
		if err != nil {
			return err
		}
		if caddyPresent(census, x.Warm) || !caddyDrained(census, x.Warm) || !caddyPresent(census, x.Main) {
			return c.ErrIncomplete
		}
		x.WarmCensus = census
		return d.record(x, "owner_retired", census.Digest)
	}
	return c.ErrIncomplete
}

func (d *caddyDriver) validate(x *CaddyTransition, deadline c.Deadline, stop c.Stop) error {
	p := x.Plan
	if p.Before.ExeSHA256 != p.Previous.BinarySHA256 || p.Candidate.GOOS != "linux" || p.Candidate.GOARCH != "amd64" {
		return errors.New("infra_artifact_invalid")
	}
	for _, artifact := range []c.Artifact{p.Candidate, p.Previous} {
		resolved, err := d.deps.Artifacts.Resolve(c.CaddyBinary, artifact.Version, artifact.GOOS, artifact.GOARCH)
		if err != nil {
			return err
		}
		if resolved != artifact {
			return errors.New("infra_artifact_invalid")
		}
		if err := d.deps.Artifacts.Verify(artifact, deadline, stop); err != nil {
			return err
		}
	}
	if err := d.deps.Artifacts.Compatible(p.Previous, p.Candidate, p.ConfigSHA256, p.Store, deadline); err != nil {
		return err
	}
	if err := d.deps.Artifacts.Compatible(p.Candidate, p.Previous, p.ConfigSHA256, p.Store, deadline); err != nil {
		return err
	}
	if p.Action == c.Reload {
		reloader, ok := d.deps.Ready.(CaddyReloader)
		if !ok || p.Candidate != p.Previous {
			return c.ErrUnsupported
		}
		if err := reloader.ValidateCaddyReload(p, deadline, stop); err != nil {
			return err
		}
	}
	return d.record(x, "artifact_verified", p.Candidate.BinarySHA256)
}

func (d *caddyDriver) reload(x *CaddyTransition, deadline c.Deadline, stop c.Stop) error {
	reloader, ok := d.deps.Ready.(CaddyReloader)
	if !ok {
		return c.ErrUnsupported
	}
	// The persistent cursor records intent before /load. The adapter's digest
	// query makes recovery idempotent without changing the expected invocation.
	x.Changed = true
	if err := d.save(*x, deadline); err != nil {
		return err
	}
	if err := d.budget(deadline, stop); err != nil {
		return err
	}
	digest, err := reloader.ReloadCaddy(x.Plan, x.Plan.Before, deadline, stop)
	if err != nil {
		return err
	}
	if digest != x.Plan.ConfigSHA256 {
		return errors.New("infra_config_invalid")
	}
	return nil
}

func (d *caddyDriver) host(x *CaddyTransition, op c.HostOp, expected c.Process, deadline c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if err := d.budget(deadline, stop); err != nil {
		return c.HostReceipt{}, err
	}
	if x.Pending == nil {
		now, err := d.now()
		if err != nil {
			return c.HostReceipt{}, err
		}
		r := c.HostRequest{Version: 1, Op: op, Component: c.Caddy, Tx: x.Plan.Tx, Step: x.Plan.Step,
			Domain: x.Plan.Domain, Sequence: x.Sequence + 1, PolicyRevision: x.Plan.PolicyRevision,
			HostBootID: x.Plan.Before.BootID, Expected: expected, ExpectedJournal: x.Journal,
			RemainingNS: uint64(int64(deadline) - now)}
		// Operand admission follows the frozen closed table, without placing
		// unrelated secrets or caller-controlled paths in the request.
		values := map[string]any{"Candidate": x.Plan.Candidate, "Store": x.Plan.Store,
			"Config": x.Plan.ConfigSHA256, "BeforeCensus": x.BeforeCensus.Digest,
			"WarmCensus": x.WarmCensus.Digest, "ReturnProof": x.ReturnProof, "Payload": caddyPlanDigest(x.Plan)}
		rule := c.HostOpDescriptors()[int(op)-1]
		for _, field := range append(rule.Required, rule.Optional...) {
			if value, ok := values[field]; ok {
				reflect.ValueOf(&r).Elem().FieldByName(field).Set(reflect.ValueOf(value))
			}
		}
		signed, err := d.requests.AuthorizeCaddy(x.Plan, r, deadline)
		if err != nil {
			return c.HostReceipt{}, err
		}
		operands := signed
		operands.KeyID, operands.Nonce, operands.Signature = "", c.ID{}, nil
		if !reflect.DeepEqual(operands, r) || len(signed.Signature) != 64 {
			return c.HostReceipt{}, errors.New("infra_permission_denied")
		}
		if _, err := c.EncodeRequest(signed); err != nil {
			return c.HostReceipt{}, err
		}
		x.Pending = &signed
		// Intent is durable before dispatch, including a possibly lost reply.
		if err := d.save(*x, deadline); err != nil {
			return c.HostReceipt{}, err
		}
	}
	r := *x.Pending
	if r.Op != op || r.Expected != expected {
		return c.HostReceipt{}, errors.New("infra_recovery_required")
	}
	if x.Receipt != nil {
		return *x.Receipt, nil
	}
	if op != c.Inspect && op != c.Stage {
		x.Changed = true
	}
	receipt, err := d.deps.Host.Do(r, deadline, stop)
	if err != nil {
		return c.HostReceipt{}, err
	}
	encoded, err := c.EncodeRequest(r)
	if err != nil {
		return c.HostReceipt{}, err
	}
	if receipt.Tx != r.Tx || receipt.Sequence != r.Sequence || receipt.Op != op || !receipt.Applied || receipt.Code != "" || receipt.RequestDigest != sha256.Sum256(encoded) || receipt.Journal == (c.Digest{}) || receipt.Revision <= x.HostRevision {
		return c.HostReceipt{}, errors.New("infra_result_unknown")
	}
	x.Sequence, x.HostRevision, x.Journal = receipt.Sequence, receipt.Revision, receipt.Journal
	x.Receipt = &receipt
	if err := d.save(*x, deadline); err != nil {
		return c.HostReceipt{}, err
	}
	return receipt, nil
}
