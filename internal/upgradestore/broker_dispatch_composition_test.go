package upgradestore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// armingRuntime is the broker's S6 store cut seen from the S8 engine: Activate
// returns once the final image is armed (the source's Start replies, the exec
// follows and never returns), and the exec hands custody to the target. The
// target's observations report the journal's own durable selector, as the
// production runtime does — never the running bytes as if they were selected.
type armingRuntime struct {
	*composedRuntime
	armed int
}

func (r *armingRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	if _, err := r.composedRuntime.Activate(c); err != nil {
		return upgradeport.Activation{}, err
	}
	r.armed++
	return upgradeport.Activation{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: brokerupgrade.ReasonActivationDispatched, Accepted: true}
}

func (r *armingRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	o, err := r.composedRuntime.Observe(s)
	if v, loadErr := r.store.Load(r.options.Key, ""); loadErr == nil {
		o.EffectiveSHA = v.Historical.EffectiveSHA
	}
	return o, err
}

// A broker Start whose S6 store cut is armed returns accepted-unknown and
// leaves the activation intent exactly as journaled (partial_active, no
// recovery claim), because the exec follows the reply. The imported target —
// a fresh engine over the same journal, as after exec — then settles the
// activation from a fresh running-image proof, which is what moves the durable
// selector to the target, and completes with all three receipts. A target
// that demanded the selector before settling the activation could never
// complete a real cut (S8 BROKER-EXPORTER R-11 real-cut finding).
// origin: hot-update S8 plan §3.1 / §4.1 / §4.2; S8 BROKER-EXPORTER R-11
func TestArmedBrokerActivationCompletesOnTheImportedTarget(t *testing.T) {
	r := &armingRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker)}
	// ctx-root: hermetic source Start; its exec is the armed reply.
	view, err := brokerupgrade.New(r.store, r, r.registry).Start(context.Background(), r.grant, r.receipt)
	var typed *upgradetx.Error
	if !errors.As(err, &typed) || typed.Detail != brokerupgrade.ReasonActivationDispatched || !typed.Accepted || r.armed != 1 {
		t.Fatalf("armed Start reply: %v armed=%d", err, r.armed)
	}
	a := composedAttempt(view.Historical)
	if a.Phase != upgradetx.PhaseActivating || a.CutStage != upgradetx.CutPartialActive || a.Outcome != upgradetx.OutcomePending || view.Historical.EffectiveSHA != r.grant.SourceSHA {
		t.Fatalf("armed Start rewrote its activation intent: phase=%s stage=%s outcome=%s effective=%s", a.Phase, a.CutStage, a.Outcome, view.Historical.EffectiveSHA)
	}
	expected := a.Handle
	expected.Revision = view.Historical.Revision
	// ctx-root: the imported target's completion entry.
	view, err = brokerupgrade.Resume(context.Background(), brokerupgrade.New(r.store, r, r.registry), expected)
	a = composedAttempt(view.Historical)
	if err != nil || a.Phase != upgradetx.PhaseCompleted || a.Outcome != upgradetx.OutcomeSuccess || view.Historical.EffectiveSHA != r.grant.TargetSHA {
		t.Fatalf("imported target did not complete: phase=%s reason=%q effective=%s err=%v", a.Phase, view.Historical.Reason, view.Historical.EffectiveSHA, err)
	}
	for _, action := range view.Historical.Actions {
		if action.State != upgradetx.ActionDurable {
			t.Fatalf("completed with a non-durable %s action", action.Kind)
		}
	}
	if r.effects != 1 {
		t.Fatalf("target completion activated again: effects=%d", r.effects)
	}
}
