package upgradestore

import (
	"encoding/json"
	"math"
	"reflect"

	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

var _ upgraderoll.PlanStore = (*Store)(nil)

// SealPlan publishes the original ordered plan in the same checksum and RMW
// transaction as its accepted parent. A lost reply can only reattach this plan;
// a later roster or a replacement child grant cannot overwrite it.
// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §3.1
func (s *Store) SealPlan(plan upgradetx.Plan) (out upgradetx.Plan, err error) {
	plan = copyPlan(plan)
	if err = upgraderoll.ValidatePlan(plan); err != nil {
		return out, err
	}
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		if err := planParent(e, plan); err != nil {
			return err
		}
		for _, saved := range e.Plans {
			if saved.Parent.TxID != plan.Parent.TxID {
				continue
			}
			if !reflect.DeepEqual(saved, plan) {
				return failure(upgradetx.CodeUnsettled, "sealed parent plan is immutable", true)
			}
			out = copyPlan(saved)
			return nil
		}
		if e.Current == nil || !sameAttempt(e.Current.Forward.Handle, plan.Parent) || settled(*e.Current) || len(e.Current.Actions) != 0 {
			return failure(upgradetx.CodeUnsettled, "parent plan must be sealed before its first effect", true)
		}
		if len(e.Plans) >= maxItems {
			return failure(upgradetx.CodeDeferred, "parent plan capacity exhausted", true)
		}
		e.Plans = append(e.Plans, plan)
		if err := bump(e); err != nil {
			return err
		}
		if err := s.write(d, true); err != nil {
			return err
		}
		out = copyPlan(plan)
		return nil
	})
	if err != nil {
		out = upgradetx.Plan{}
	}
	return
}

// LoadPlan never reconstructs an absent snapshot from Current or live nodes.
// The captured cut and bounded revision must still identify the original parent.
func (s *Store) LoadPlan(parent upgradetx.Handle) (out upgradetx.Plan, err error) {
	err = s.locked(func(_ *diskFile, e *diskEntry) error {
		for _, plan := range e.Plans {
			if sameAttempt(plan.Parent, parent) && parent.Revision >= plan.Parent.Revision {
				if err := planParent(e, plan); err != nil {
					return err
				}
				for _, record := range records(e) {
					if record.Grant.Handle.TxID == parent.TxID && parent.Revision <= record.Revision {
						out = copyPlan(plan)
						return nil
					}
				}
			}
		}
		return failure(upgradetx.CodeGrantUnknown, "parent plan is not recorded", false)
	})
	return
}

// CompletePlan commits bookkeeping only after the full aggregate proof. It
// does not run component transitions or synthesize receipts from step counts.
func (s *Store) CompletePlan(expected upgradetx.Handle, observation upgradetx.Observation) (out upgradetx.View, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		if err := currentCAS(e, expected); err != nil {
			return err
		}
		var saved *upgradetx.Plan
		for i := range e.Plans {
			if sameAttempt(e.Plans[i].Parent, expected) {
				saved = &e.Plans[i]
				break
			}
		}
		if saved == nil {
			return failure(upgradetx.CodeGrantUnknown, "parent plan is not recorded", true)
		}
		record := *e.Current
		if record.Reverse.Handle.TxID != "" || settled(record) || record.Revision == math.MaxUint64 {
			return failure(upgradetx.CodeUnsettled, "parent execution is immutable", true)
		}
		if err := upgraderoll.ValidateParentCompletion(*saved, record, observation); err != nil {
			return err
		}
		record.Forward = observation.Current
		record.Revision++
		record.Forward.Handle.Revision = record.Revision
		record.EffectiveSHA = record.Forward.TargetSHA
		e.Current = &record
		if err := bump(e); err != nil {
			return err
		}
		if err := s.write(d, true); err != nil {
			return err
		}
		out = upgradetx.View{Historical: record, Metadata: *metadata(e, record.Grant.Handle.TxID), Observation: observation}
		return nil
	})
	if err != nil {
		out = upgradetx.View{}
	}
	return
}

func planParent(e *diskEntry, plan upgradetx.Plan) error {
	for _, record := range records(e) {
		if sameAttempt(record.Forward.Handle, plan.Parent) && record.Revision >= plan.Parent.Revision && record.Key == e.Key &&
			reflect.DeepEqual(record.Forward.Scope, plan.Scope) && record.Grant.ExpiresAt == plan.Deadline && record.Grant.Mode == upgradetx.ModeHot {
			return nil
		}
	}
	return failure(upgradetx.CodeUnsettled, "sealed plan has no matching accepted parent", true)
}

func validatePlans(e *diskEntry) error {
	if len(e.Plans) > maxItems {
		return corrupt()
	}
	seen := map[string]bool{}
	for _, plan := range e.Plans {
		if seen[plan.Parent.TxID] || upgraderoll.ValidatePlan(plan) != nil || planParent(e, plan) != nil {
			return corrupt()
		}
		seen[plan.Parent.TxID] = true
	}
	return nil
}

func copyPlan(plan upgradetx.Plan) upgradetx.Plan {
	data, _ := json.Marshal(plan)
	var copy upgradetx.Plan
	_ = json.Unmarshal(data, &copy)
	return copy
}
