package upgraderoll

import (
	"reflect"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ObservationOnly distinguishes an inert equal-byte authorization from an
// executable child without expanding the frozen StepKind vocabulary. Its
// prepared record remains prepared: observing a skip creates no image cut.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2 / IL14
func ObservationOnly(g upgradetx.Grant) bool { return g.SourceSHA != "" && g.SourceSHA == g.TargetSHA }

// ObserveUnchanged closes the original scope with a fresh source observation.
// Historical completion cannot prove a skip, and a skip cannot certify a
// previous generation's retirement. All live resources must still belong to
// the original source owners in the one generation described before sealing.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2
func ObserveUnchanged(v upgradetx.View, g upgradetx.Grant, nonce string) error {
	a, o := v.Historical.Forward, v.Observation
	if !ObservationOnly(g) || v.Code != "" || v.Historical.Grant != g || a.Handle != g.Handle ||
		a.Phase != upgradetx.PhasePrepared || a.CutStage != upgradetx.CutNone || a.Outcome != upgradetx.OutcomePending ||
		a.SourceSHA != g.SourceSHA || a.TargetSHA != g.TargetSHA || a.Direction != upgradetx.DirectionForward || a.SourceGeneration == 0 || a.SourceGeneration+1 != a.Handle.Generation || !reflect.DeepEqual(a, o.Current) ||
		!o.OwnerKnown || o.Nonce != nonce || o.At <= 0 || o.At != o.Census.Stamp.At ||
		o.RunningSHA != g.TargetSHA || o.EffectiveSHA != g.TargetSHA || o.Census.Stamp.Producer != g.Issuer ||
		!upgradetx.SameResources(o.Census.Resources, a.Scope.Expected) || ScopeDigest(a.Scope) != g.ScopeDigest || !validCensus(o.Census, a.Handle, a.Scope, g.ScopeDigest, nonce) {
		return refusal(upgradetx.CodeUnsettled, "observation-only participant is not freshly proven at target", true)
	}
	for _, row := range o.Census.Resources {
		if !upgradetx.ResourceReleased(row) && !upgradetx.ResourceNotApplicable(row) && row.Generation != a.SourceGeneration {
			return refusal(upgradetx.CodeUnsettled, "observation-only participant has another live generation", true)
		}
	}
	return nil
}
