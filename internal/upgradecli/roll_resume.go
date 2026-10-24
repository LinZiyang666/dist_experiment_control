package upgradecli

import (
	"context"

	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ResumeParent is `cluster upgrade --resume <parent-tx-id>`. It drives only
// the ORIGINAL sealed plan, loaded by the parent handle this ctl recorded when
// the plan was sealed, through upgraderoll.Run. It never re-plans from the live
// roster, never skips a participant because its release already matches, never
// prepares a new child and never mints child IDs: the sealed target-issued
// grants are the only execution authority (§3.1 / IL14 / M09).
// Without the parent journal and roll endpoint adapters the resume is an
// accepted, unsettled parent (74): the operator queries each original child.
// origin: hot-update S8 plan §3.1 / §4.2 / §11.2 P11/P19
func ResumeParent(ctx context.Context, home, parentTxID string, journal upgraderoll.PlanStore, endpoint upgraderoll.Endpoint) (upgradetx.View, error) {
	grant, err := RecallGrant(home, parentTxID)
	if err != nil {
		return upgradetx.View{}, err
	}
	if journal == nil || endpoint == nil {
		return upgradetx.View{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true,
			Detail: "parent journal and roll endpoint adapters are not wired (S8 P02/P15); query each original child with its own --tx-id"}
	}
	plan, err := journal.LoadPlan(grant.Handle)
	if err != nil {
		return upgradetx.View{}, err
	}
	if !upgradetx.SameCut(plan.Parent, grant.Handle) {
		return upgradetx.View{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true, Detail: "sealed plan does not belong to the recorded parent"}
	}
	return upgraderoll.Run(ctx, journal, endpoint, plan)
}
