package infraupgrade

import (
	"errors"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// A nonblocking systemd job may legitimately reply before MainPID exists.
// Wait on independent domain census without resubmitting the start operation.
// The root's domain census must include the managed units and descendants;
// main stays in the original unit cgroup, warm belongs to its transient unit.
// No root journal lock is held across this wait or the launcher handshake.
// origin: hot-update S9 plan §6.1 / §5.3 / T22
func (d *caddyDriver) started(x *CaddyTransition, expected c.Process, warm bool, deadline c.Deadline, stop c.Stop) (c.Process, error) {
	if expected != (c.Process{}) && !caddyProcess(expected) {
		return c.Process{}, c.ErrIncomplete
	}
	for {
		if err := d.budget(deadline, stop); err != nil {
			return c.Process{}, err
		}
		census, err := d.inspect(x, deadline)
		if err != nil {
			return c.Process{}, err
		}
		var found c.Process
		for _, process := range census.Processes {
			if process == x.Plan.Before || (!warm && process == x.Warm) || process.ExeSHA256 != x.Plan.Candidate.BinarySHA256 || process.BootID != x.Plan.Before.BootID {
				continue
			}
			if (process.Cgroup != x.Plan.Before.Cgroup) != warm {
				continue
			}
			if found != (c.Process{}) || (expected != (c.Process{}) && process != expected) {
				return c.Process{}, c.ErrIncomplete
			}
			found = process
		}
		if found != (c.Process{}) {
			return found, nil
		}
		// Poll cadence is not a service lifetime or a fresh admission budget.
		// Stop releases only this wait; it never kills the serving process.
		if err := d.wait(stop); err != nil {
			return c.Process{}, err
		}
	}
}

func caddyPoll(stop c.Stop) error {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-stop:
		return errors.New("infra_budget_exceeded")
	case <-timer.C:
		return nil
	}
}

func caddyDirection(x *CaddyTransition) (c.RouteDirection, c.Process, c.Process) {
	if x.Cursor >= caddySealReturn {
		return c.RouteReturn, x.Warm, x.Main
	}
	return c.RouteOutbound, x.Plan.Before, x.Warm
}

func (d *caddyDriver) ready(x *CaddyTransition, expected c.Process, deadline c.Deadline, stop c.Stop) (c.Census, error) {
	p := x.Plan
	p.Before = expected
	ready, err := d.deps.Ready.Read(p, deadline, stop)
	if err != nil {
		return c.Census{}, err
	}
	// P08/P02 must produce the accepted-socket bypass proof for this exact
	// process; a healthy shadow sharing SNI and upstream ServerID is insufficient.
	if !ready.Complete || ready.Running != expected || ready.Auth == (c.Digest{}) || ready.WSS == (c.Digest{}) {
		return c.Census{}, errors.New("infra_not_ready")
	}
	census, err := d.inspect(x, deadline)
	if err != nil {
		return c.Census{}, err
	}
	if !caddyPresent(census, expected) {
		return c.Census{}, c.ErrIncomplete
	}
	if err := d.record(x, "auth_usable", ready.Auth); err != nil {
		return c.Census{}, err
	}
	if err := d.record(x, "business_ready", ready.WSS); err != nil {
		return c.Census{}, err
	}
	return census, nil
}

func (d *caddyDriver) seal(x *CaddyTransition, deadline c.Deadline) error {
	// Return is a fresh stable step, never a reuse of outbound ACKs. Releasing
	// the outbound seal does not release business resources or the S8 domain.
	if x.Cursor == caddySealReturn && x.Checkpoint.Cohort != (c.ID{}) {
		if err := d.deps.Cohort.Release(x.Checkpoint, deadline); err != nil {
			return err
		}
	}
	p := x.Plan
	_, old, _ := caddyDirection(x)
	p.Before = old
	checkpoint, err := d.deps.Cohort.Seal(p, deadline)
	if err != nil {
		return err
	}
	if checkpoint.Cohort == (c.ID{}) || checkpoint.Proof == (c.Digest{}) || !checkpoint.Assets.Complete || (checkpoint.Transfers.Version != 0 && !checkpoint.Transfers.Complete) {
		return c.ErrIncomplete
	}
	x.Checkpoint, x.Prepares, x.Binds = checkpoint, nil, nil
	x.Move, x.Bound = c.MoveResult{}, c.Census{}
	return d.record(x, "asset_sealed", checkpoint.Proof)
}

func (d *caddyDriver) prepare(x *CaddyTransition, deadline c.Deadline) error {
	direction, old, target := caddyDirection(x)
	if x.Prepares == nil {
		proofs, err := d.deps.Routes.Prepare(x.Plan, x.Checkpoint, direction, deadline)
		if err != nil {
			return err
		}
		x.Prepares = proofs
	}
	if len(x.Prepares) == 0 {
		return c.ErrIncomplete
	}
	seen := make(map[c.ID]bool, len(x.Prepares))
	for _, proof := range x.Prepares {
		if _, err := c.EncodeRouteProof(proof); err != nil {
			return err
		}
		if proof.Purpose != c.RoutePrepare || proof.Direction != direction || proof.Tx != x.Plan.Tx || proof.PolicyRevision != x.Plan.PolicyRevision || proof.Old.Ingress != old || proof.Target.Ingress != target || proof.Target.Component != c.Caddy || len(proof.Signature) != 64 || seen[proof.Participant] {
			return c.ErrIncomplete
		}
		seen[proof.Participant] = true
	}
	// Persist original nonces before installing the first participant. A partial
	// installation failure must not renew everyone else's eight-second hold.
	if err := d.save(*x, deadline); err != nil {
		return err
	}
	for _, proof := range x.Prepares {
		if err := d.deps.Cohort.Install(x.Checkpoint, proof, deadline); err != nil {
			return err
		}
	}
	return d.record(x, "successor_installed", x.Checkpoint.Proof)
}

func (d *caddyDriver) move(x *CaddyTransition, deadline c.Deadline, stop c.Stop) error {
	direction, _, target := caddyDirection(x)
	now, err := d.now()
	if err != nil {
		return err
	}
	moveDeadline := min(deadline, c.Deadline(now+int64(upgradebudget.RequestLatency)))
	if x.Move.Census.Digest == (c.Digest{}) {
		var moved c.MoveResult
		if direction == c.RouteOutbound {
			moved, err = d.deps.Cohort.Move(x.Checkpoint, moveDeadline, stop)
		} else {
			moved, err = d.deps.Cohort.Return(x.Checkpoint, moveDeadline, stop)
		}
		if err != nil {
			return err
		}
		// An error may accompany partial evidence. It cannot become a successful
		// cached move merely because recovery finds a nonzero census digest.
		x.Move = moved
		if err := d.save(*x, deadline); err != nil {
			return err
		}
	}
	if !caddyCensus(x.Move.Census) || !caddyPresent(x.Move.Census, target) || len(x.Move.Attachments) != len(x.Prepares) || len(x.Prepares) == 0 {
		return c.ErrIncomplete
	}
	attachments := make(map[c.ID]c.Attachment, len(x.Move.Attachments))
	for _, attachment := range x.Move.Attachments {
		if _, exists := attachments[attachment.Participant]; exists || attachment.Ingress != target {
			return c.ErrIncomplete
		}
		attachments[attachment.Participant] = attachment
	}
	if x.Binds == nil {
		proofs, err := d.deps.Routes.Bind(x.Plan, x.Move, direction, moveDeadline)
		if err != nil {
			return err
		}
		x.Binds = proofs
	}
	if len(x.Binds) != len(x.Prepares) {
		return c.ErrIncomplete
	}
	prepares := make(map[c.ID]c.RouteProof, len(x.Prepares))
	for _, proof := range x.Prepares {
		prepares[proof.Participant] = proof
	}
	for _, proof := range x.Binds {
		before, ok := prepares[proof.Participant]
		if !ok {
			return c.ErrIncomplete
		}
		delete(prepares, proof.Participant)
		if _, err := c.EncodeRouteProof(proof); err != nil {
			return err
		}
		// Signatures are verified by Cohort.Bind; here the driver checks that
		// its signer did not change the intended participant/nonce/physical move.
		if proof.Purpose != c.RouteBind || proof.Direction != direction || proof.Tx != x.Plan.Tx || proof.Old != before.Old || proof.New != attachments[proof.Participant] || proof.Target != before.Target || proof.Nonce != before.Nonce || proof.HomeDigest != before.HomeDigest || proof.AuthorityRevision != before.AuthorityRevision || proof.PolicyRevision != before.PolicyRevision || proof.KeyID != before.KeyID || proof.RemainingNS > before.RemainingNS || len(proof.Signature) != 64 {
			return c.ErrIncomplete
		}
	}
	if err := d.save(*x, deadline); err != nil {
		return err
	}
	bound, err := d.deps.Cohort.Bind(x.Checkpoint, x.Binds, moveDeadline)
	if err != nil {
		return err
	}
	if err := d.budget(moveDeadline, stop); err != nil {
		return err
	}
	if !caddyCensus(bound) || !caddyPresent(bound, target) {
		return c.ErrIncomplete
	}
	x.Bound = bound
	if direction == c.RouteReturn {
		x.ReturnProof = bound.Digest
		return d.record(x, "return_verified", bound.Digest)
	}
	return d.record(x, "attachment_changed", bound.Digest)
}

func (d *caddyDriver) drain(x *CaddyTransition, expected c.Process, deadline c.Deadline, stop c.Stop, warm bool) error {
	var census c.Census
	for {
		if err := d.budget(deadline, stop); err != nil {
			return err
		}
		var err error
		census, err = d.inspect(x, deadline)
		if err != nil {
			return err
		}
		if caddyDrained(census, expected) {
			// A snapshot cannot close admission. In particular an empty LISTEN
			// remains capable of accepting work until the independent fence holds.
			// origin: S9 internal review rv-infra F07
			target := x.Warm
			if warm {
				target = x.Main
			}
			seal, ok := d.deps.Host.(CaddyIngress)
			if !ok {
				return c.ErrContractStub
			}
			if err := seal.VerifyCaddyIngress(x.Plan, expected, target, census, deadline, stop); err != nil {
				return err
			}
			break
		}
		// Finite HTTP/TLS work may finish naturally after the product NCs move.
		// Wait within the original budget; neither truncate it nor sign zero
		// from two transient observations. The root seals accepts under CAS.
		if err := d.wait(stop); err != nil {
			return err
		}
	}
	if warm {
		if x.ReturnProof == (c.Digest{}) || !caddyPresent(census, x.Main) {
			return c.ErrIncomplete
		}
		x.WarmCensus = census
	} else {
		if !caddyPresent(census, x.Warm) {
			return c.ErrIncomplete
		}
		x.BeforeCensus = census
	}
	return nil
}

func (d *caddyDriver) complete(x *CaddyTransition, s *c.Status, deadline c.Deadline, stop c.Stop) error {
	if x.ReturnProof == (c.Digest{}) {
		return c.ErrIncomplete
	}
	census, err := d.ready(x, x.Main, deadline, stop)
	if err != nil {
		return err
	}
	// The final set contains the main only: no warm unit, old invocation,
	// support worker or unknown owner can hide behind a positive live count.
	// A rollback is allowed to reuse an older SHA; compare full incarnations.
	// origin: hot-update S9 plan §2.3 / §5.3 / T22 / M12
	if len(census.Processes) != 1 || !caddyPresent(census, x.Main) || !caddyDrained(census, x.Warm) || (x.Plan.Action != c.Reload && caddyPresent(census, x.Plan.Before)) {
		return c.ErrIncomplete
	}
	// Query the root journal after retirement; S8 commits exactly that proof.
	// This read is authenticated and receipt-bound, never copied from Status.
	if _, err := d.host(x, c.Inspect, x.Main, deadline, stop); err != nil {
		return err
	}
	if err := d.budget(deadline, stop); err != nil {
		return err
	}
	s.Current, s.Census, s.Code, s.Detail = x.Main, census, "", ""
	if err := d.phase(x, s, c.Completed, x.Journal, deadline); err != nil {
		return err
	}
	return d.record(x, "completed", x.Journal)
}
