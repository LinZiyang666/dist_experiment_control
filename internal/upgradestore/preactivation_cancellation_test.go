package upgradestore_test

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type preactivationRuntime struct {
	*composedRuntime
	defect                 string
	cancel                 context.CancelFunc
	boundary               upgradetx.Attempt
	activationCalls, thaws int
}

func (r *preactivationRuntime) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) {
	r.boundary = r.current()
	cut, err := r.composedRuntime.ExportCurrent(h)
	switch r.defect {
	case "export error":
		return upgradeport.Cut{}, errors.New("current export refused before activation")
	case "invalid export":
		cut.Attempt.CutID = "another cut"
	case "stale export":
		cut.Sequence = 0
	}
	return cut, err
}

func (r *preactivationRuntime) Preflight(ctx context.Context, g upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	proof, err := r.composedRuntime.Preflight(ctx, g, artifact)
	if r.preflights == 3 && r.defect == "cancelled caller" {
		r.cancel()
	}
	return proof, err
}

func (r *preactivationRuntime) Activate(cut upgradeport.Cut) (upgradeport.Activation, error) {
	r.activationCalls++
	return r.composedRuntime.Activate(cut)
}

func (r *preactivationRuntime) Thaw(h upgradetx.Handle) error {
	r.thaws++
	return r.composedRuntime.Thaw(h)
}

// origin: S8 internal review roll-recovery-stall F7
func TestBrokerPreActivationFailureReleasesCurrentWithSourceProof(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerPreActivationFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker preactivation cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "broker preactivation cancellation", beforeFD, 0)
}

func exerciseBrokerPreActivationFailure(t *testing.T) {
	t.Helper()
	for _, defect := range []string{"export error", "invalid export", "stale export", "reader withdrawn", "cancelled caller"} {
		t.Run(defect, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := &preactivationRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker), defect: defect, cancel: cancel}
			if defect == "reader withdrawn" {
				r.failPreflightAt = 3
			}
			e := brokerupgrade.New(r.store, r, r.registry)
			view, err := e.Start(ctx, r.grant, r.receipt)
			if err == nil || r.activationCalls != 0 || r.effects != 0 || r.boundary.CutStage != upgradetx.CutPartialActive {
				t.Fatalf("fault missed the durable preactivation boundary: calls=%d effects=%d stage=%s err=%v", r.activationCalls, r.effects, r.boundary.CutStage, err)
			}
			if !upgradetx.CancellationSettled(view.Historical) || !upgradetx.ColdSelectionSafe(view.Historical) || r.thaws != 1 || view.Historical.EffectiveSHA != r.grant.SourceSHA {
				t.Fatalf("engine stranded an activation it never called: phase=%s stage=%s thaws=%d", composedAttempt(view.Historical).Phase, composedAttempt(view.Historical).CutStage, r.thaws)
			}
			thaws := 0
			for _, action := range view.Historical.Actions {
				if action.Kind == upgradetx.ActionFreeze && action.State == upgradetx.ActionDurable {
					thaws++
				}
				if (action.Kind == upgradetx.ActionActivate || action.Kind == upgradetx.ActionDispatch) && action.State != upgradetx.ActionIntent {
					t.Fatal("unqueued cancellation invented activation results")
				}
			}
			if thaws != 2 {
				t.Fatal("source thaw lacks its own durable action")
			}
			r.reopen()
			durable, err := r.store.Load(r.options.Key, r.grant.Handle.TxID)
			if err != nil || !upgradetx.CancellationSettled(durable.Historical) {
				t.Fatalf("preactivation cancellation did not survive reopen: %v", err)
			}
			retry, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
			if err != nil || retry.Historical.Revision != durable.Historical.Revision || r.activationCalls != 0 || r.thaws != 1 {
				t.Fatalf("accepted retry repeated a refused preactivation: %v", err)
			}
		})
	}
}
