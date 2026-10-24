package upgradestore_test

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type recoveryProbeRuntime struct {
	*composedRuntime
	failRetire bool
	defect     string
}

func (r *recoveryProbeRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	if r.failRetire {
		r.failRetire = false
		// The agent's policy preserves uncertainty rather than automatically
		// reversing on a durability/authority-class refusal. Both roles now
		// exercise the accepted recovery state through the real Transition.
		return upgradetx.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "temporary retirement proof unavailable", Accepted: true}
	}
	return r.composedRuntime.Retire(h)
}

func (r *recoveryProbeRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	o, err := r.composedRuntime.Observe(s)
	switch r.defect {
	case "owner":
		o.OwnerKnown = false
	case "custody":
		o.Census.Resources[0].Owner.PID++
	case "identity":
		o.Census.Resources[0].Origin = "replacement"
	case "revision":
		o.Current.Handle.Revision++
	case "running":
		o.RunningSHA = o.Current.SourceSHA
	case "control":
		o.Current.Control.Known = false
	}
	return o, err
}

// origin: S8 internal review rv3-leaves F9
func TestAcceptedCutRecoveryRequiresFreshOwnerAndCustody(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		for _, defect := range []string{"none", "owner", "custody", "identity", "revision", "running", "control"} {
			t.Run(string(role)+"/"+defect, func(t *testing.T) {
				r := &recoveryProbeRuntime{composedRuntime: newComposedRuntime(t, role), failRetire: true}
				_, err := boundaryEndpoint(r.store, r.composedRuntime, r).Start(t.Context(), r.grant, r.receipt)
				if err == nil || r.effects != 1 {
					t.Fatal("retirement refusal did not cross the activated boundary")
				}
				r.reopen()
				a := r.current()
				if a.Phase != upgradetx.PhaseRecoveryRequired {
					t.Fatalf("refusal did not persist recovery: %s", a.Phase)
				}
				r.defect = defect
				e := boundaryEndpoint(r.store, r.composedRuntime, r)
				var v upgradetx.View
				if role == upgradetx.ComponentAgent {
					v, err = agentupgrade.Boot(t.Context(), e, a.Handle)
				} else {
					v, err = brokerupgrade.Resume(t.Context(), e, a.Handle)
				}
				if defect == "none" {
					if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
						t.Fatalf("fresh same-cut recovery failed: %v", err)
					}
				} else if err == nil || composedAttempt(v.Historical).Phase == upgradetx.PhaseCompleted {
					t.Fatalf("unproven %s recovery completed: %v", defect, err)
				}
				if r.effects != 1 {
					t.Fatal("recovery redispatched activation")
				}
			})
		}
	}
}
