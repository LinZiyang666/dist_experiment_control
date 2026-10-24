package upgraderoll

import (
	"reflect"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ValidatePlan is the daemon journal's roll-specific structural predicate.
// The neutral transaction predicate has no host-release steps; keeping this
// closure with the driver gives persistence and execution one plan grammar.
// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §3.1
func ValidatePlan(plan upgradetx.Plan) error { return validatePlan(plan) }

// ValidateParentCompletion binds a final aggregate observation to the saved
// plan and every durable step result. The endpoint independently proves the
// holder release; an empty action list can never stand in for those effects.
// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §5.1
func ValidateParentCompletion(plan upgradetx.Plan, record upgradetx.Record, observation upgradetx.Observation) error {
	if err := validatePlan(plan); err != nil {
		return err
	}
	a := observation.Current
	if !sameAttempt(record.Forward.Handle, plan.Parent) || a.Handle != record.Forward.Handle ||
		!reflect.DeepEqual(record.Forward.Scope, plan.Scope) || !reflect.DeepEqual(a.Scope, plan.Scope) {
		return refusal(upgradetx.CodeUnsettled, "parent completion identity differs from the sealed plan", true)
	}
	for _, step := range plan.Steps {
		if !stepDurable(record, plan, step) {
			return refusal(upgradetx.CodeUnsettled, "parent step has no durable result", true)
		}
	}
	if err := completed(a, observation, record.Grant, observation.Nonce); err != nil {
		return err
	}
	// Aggregate control is freshly probed at this observation, unlike the
	// immutable component receipt consumed by upgradetx.Completed. Record
	// validation still enforces the independent three-receipt census grammar.
	candidate := record
	candidate.Forward = a
	return upgradetx.ValidateRecord(candidate)
}
