package upgraderoll

import (
	"context"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Control is the broker's authenticated holder/collector port. It operates on
// the original parent token and absolute deadline. Implementations must retain
// release tombstones, use expected-holder CAS, and obtain fresh fleet/home
// observations; a CLI or a local journal cannot fabricate those remote facts.
// No new Raft operation or speculative S7 interface is introduced here.
// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §5.1 L20
type Control interface {
	Reserve(context.Context, upgradetx.Plan) error
	Check(context.Context, upgradetx.Plan, string) (FleetProof, error)
	Renew(context.Context, upgradetx.Handle, int64) error
	Release(context.Context, upgradetx.Plan, upgradetx.Step, string) (upgradetx.Receipt, error)
	Artifact(context.Context, upgradetx.Grant) (upgradeartifact.Receipt, error)
	Converge(context.Context, upgradetx.Step, string) (upgradetx.Receipt, error)
	ObserveParent(context.Context, upgradetx.Plan, string) (upgradetx.Observation, error)
	RenewalInterval() time.Duration
}

type endpoint struct {
	upgradeport.Endpoint
	journal PlanStore
	control Control
	parent  upgradetx.Handle
}
type reservation struct {
	endpoint *endpoint
	plan     upgradetx.Plan
}

// BindEndpoint joins the sole writer and authenticated component route to the
// holder port. Every reservation effect reloads the original sealed snapshot;
// Stop is local and cannot release the daemon's pending execution authority.
func BindEndpoint(journal PlanStore, components upgradeport.Endpoint, control Control, parent upgradetx.Handle) (Endpoint, error) {
	if journal == nil || components == nil || control == nil || !validHandle(parent) {
		return nil, refusal(upgradetx.CodeNotWired, "roll control ports are unavailable", true)
	}
	return &endpoint{Endpoint: components, journal: journal, control: control, parent: parent}, nil
}

func (e *endpoint) saved(plan upgradetx.Plan) error {
	if e.journal == nil || e.control == nil || e.Endpoint == nil {
		return refusal(upgradetx.CodeNotWired, "roll control ports are unavailable", true)
	}
	if !sameAttempt(plan.Parent, e.parent) {
		return refusal(upgradetx.CodeUnsettled, "reservation belongs to another parent", true)
	}
	saved, err := e.journal.LoadPlan(plan.Parent)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(saved, plan) {
		return refusal(upgradetx.CodeUnsettled, "roll differs from the sealed parent plan", true)
	}
	return nil
}

func (e *endpoint) Reserve(ctx context.Context, plan upgradetx.Plan) (Reservation, error) {
	if err := e.saved(plan); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, refusal(upgradetx.CodeRejected, "driver context is required", true)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.control.RenewalInterval() <= 0 {
		return nil, refusal(upgradetx.CodeUnsettled, "reservation renewal budget is missing", true)
	}
	if err := e.control.Reserve(ctx, clonePlan(plan)); err != nil {
		return nil, acceptedError(err)
	}
	return &reservation{endpoint: e, plan: clonePlan(plan)}, nil
}

func (e *endpoint) step(grant upgradetx.Grant, kind upgradetx.StepKind) (upgradetx.Step, error) {
	if e.journal == nil || e.control == nil {
		return upgradetx.Step{}, refusal(upgradetx.CodeNotWired, "roll control ports are unavailable", true)
	}
	if grant.ParentTxID != e.parent.TxID {
		return upgradetx.Step{}, refusal(upgradetx.CodeUnsettled, "step belongs to another parent", true)
	}
	plan, err := e.journal.LoadPlan(e.parent)
	if err != nil {
		return upgradetx.Step{}, err
	}
	if err := e.saved(plan); err != nil {
		return upgradetx.Step{}, err
	}
	for _, step := range plan.Steps {
		if step.Grant == grant && (kind == "" && isCut(step) || step.Kind == kind) {
			return step, nil
		}
	}
	return upgradetx.Step{}, refusal(upgradetx.CodeUnsettled, "step grant is outside the sealed parent plan", true)
}

func (e *endpoint) Artifact(ctx context.Context, grant upgradetx.Grant) (upgradeartifact.Receipt, error) {
	if _, err := e.step(grant, ""); err != nil {
		return upgradeartifact.Receipt{}, err
	}
	receipt, err := e.control.Artifact(ctx, grant)
	if err == nil && receipt.Grant != grant {
		return upgradeartifact.Receipt{}, refusal(upgradetx.CodeUnsettled, "artifact belongs to another grant", true)
	}
	return receipt, err
}

func (e *endpoint) Converge(ctx context.Context, step upgradetx.Step, nonce string) (upgradetx.Receipt, error) {
	saved, err := e.step(step.Grant, upgradetx.StepHomeConverge)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	if !reflect.DeepEqual(saved, step) {
		return upgradetx.Receipt{}, refusal(upgradetx.CodeUnsettled, "home step differs from the sealed plan", true)
	}
	return e.control.Converge(ctx, saved, nonce)
}

func (e *endpoint) ObserveParent(ctx context.Context, plan upgradetx.Plan, nonce string) (upgradetx.Observation, error) {
	if err := e.saved(plan); err != nil {
		return upgradetx.Observation{}, err
	}
	return e.control.ObserveParent(ctx, clonePlan(plan), nonce)
}

func (r *reservation) Check(ctx context.Context, plan upgradetx.Plan, nonce string) (FleetProof, error) {
	if !reflect.DeepEqual(plan, r.plan) {
		return FleetProof{}, refusal(upgradetx.CodeUnsettled, "reservation plan changed", true)
	}
	if err := r.endpoint.saved(r.plan); err != nil {
		return FleetProof{}, err
	}
	return r.endpoint.control.Check(ctx, clonePlan(r.plan), nonce)
}
func (r *reservation) Renew(ctx context.Context, parent upgradetx.Handle) error {
	if parent != r.plan.Parent {
		return refusal(upgradetx.CodeUnsettled, "reservation holder changed", true)
	}
	if err := r.endpoint.saved(r.plan); err != nil {
		return err
	}
	return r.endpoint.control.Renew(ctx, parent, r.plan.Deadline)
}
func (r *reservation) Release(ctx context.Context, step upgradetx.Step, nonce string) (upgradetx.Receipt, error) {
	if err := r.endpoint.saved(r.plan); err != nil {
		return upgradetx.Receipt{}, err
	}
	for _, saved := range r.plan.Steps {
		if saved.Kind == upgradetx.StepRelease && reflect.DeepEqual(saved, step) {
			return r.endpoint.control.Release(ctx, clonePlan(r.plan), step, nonce)
		}
	}
	return upgradetx.Receipt{}, refusal(upgradetx.CodeUnsettled, "release step differs from the sealed plan", true)
}
func (r *reservation) RenewalInterval() time.Duration { return r.endpoint.control.RenewalInterval() }
func (r *reservation) Stop() error                    { return nil } // No local worker or retained descriptor; daemon keeps the holder.
