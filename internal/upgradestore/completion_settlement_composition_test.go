package upgradestore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// settlingRuntime models one real cut across a same-PID exec. The source
// engine's Activate hands custody to the target (the composed runtime's
// effect) and then never returns while the target engine finishes the cut —
// the exec has replaced the source. One deterministic event follows a chosen
// running sample and precedes the registry's gated snapshot: the carried
// resource settles (running → terminal, retained). The registry observers read
// the same live resources, so the gated snapshot sees the settlement and the
// running sample does not.
//
//   - "settled": after the completion's final running sample (the Observe
//     following the durable Released receipt), before Commit's snapshot.
//   - "recovered": the first Retire fails (an ordinary unproven release), so
//     the cut is recovery_required; the next completion's recovery sample is
//     followed by the settlement, before Reconcile's snapshot.
type settlingRuntime struct {
	*composedRuntime
	mode         string
	settled      int
	retireFailed bool
	entered      chan upgradeport.Cut
	released     chan struct{}
}

func (r *settlingRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	if _, err := r.composedRuntime.Activate(c); err != nil {
		return upgradeport.Activation{}, err
	}
	r.entered <- c
	<-r.released
	return upgradeport.Activation{}, errors.New("the source process was replaced by its exec")
}

func (r *settlingRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	o, err := r.composedRuntime.Observe(s)
	a := r.current()
	if r.settled == 0 && (r.mode == "settled" && a.Released.Kind != "" || r.mode == "recovered" && a.Phase == upgradetx.PhaseRecoveryRequired) {
		r.settled++
		r.resources[0].State, r.resources[0].Custody = "terminal", "retained"
	}
	return o, err
}

func (r *settlingRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	if r.mode == "recovered" && !r.retireFailed {
		r.retireFailed = true
		return upgradetx.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "old generation release is unproven", Accepted: true}
	}
	return r.composedRuntime.Retire(h)
}

func (r *settlingRuntime) engine() upgradeport.Endpoint {
	if r.options.Key.Component == upgradetx.ComponentAgent {
		return agentupgrade.New(r.store, r, r.registry)
	}
	return brokerupgrade.New(r.store, r, r.registry)
}

// complete runs the imported target's own completion entry (agent Boot,
// broker Resume) for the given handle.
func (r *settlingRuntime) complete(h upgradetx.Handle) (upgradetx.View, error) {
	// ctx-root: the imported target's completion entry.
	ctx := context.Background()
	if r.options.Key.Component == upgradetx.ComponentAgent {
		return agentupgrade.Boot(ctx, r.engine(), h)
	}
	return brokerupgrade.Resume(ctx, r.engine(), h)
}

// A carried transfer that settles between the imported target's running sample
// and the admission gate's snapshot is ordinary progress of a cut that is
// already complete. The target's own completion entry (agent Boot, broker
// Resume) — and its recovery path — must commit it with the real journal and
// admission registry, instead of leaving recovery_required with all three
// receipts true ("boot completion evidence is unavailable"). The steady arm and
// the injection count prove the event lands exactly once, between the two
// samples; the replaced source's late return must not rewrite the record.
// origin: S8 BROKER-EXPORTER F1 (S8 FIX-DESCRIBE finding F1: 5/220 boundary
// cells recovery_required with all three receipts true); plan §4.2
func TestImportedCutCompletesWhenATransferSettlesBetweenSamples(t *testing.T) {
	for _, component := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		for _, mode := range []string{"steady", "settled", "recovered"} {
			t.Run(string(component)+"/"+mode, func(t *testing.T) {
				r := &settlingRuntime{composedRuntime: newComposedRuntime(t, component), mode: mode,
					entered: make(chan upgradeport.Cut, 1), released: make(chan struct{})}
				done := make(chan error, 1)
				go func() {
					// ctx-root: hermetic source Start; its exec never returns.
					_, err := r.engine().Start(context.Background(), r.grant, r.receipt)
					done <- err
				}()
				var cut upgradeport.Cut
				select {
				case cut = <-r.entered:
				case err := <-done:
					t.Fatalf("source Start ended before activation: %v", err)
				case <-time.After(30 * time.Second):
					t.Fatal("source Start never reached activation")
				}
				view, err := r.complete(cut.Attempt)
				if mode == "recovered" {
					a := composedAttempt(view.Historical)
					if err == nil || a.Phase != upgradetx.PhaseRecoveryRequired || r.settled != 0 {
						t.Fatalf("failed release did not leave recovery: phase=%s settled=%d err=%v", a.Phase, r.settled, err)
					}
					h := a.Handle
					h.Revision = view.Historical.Revision
					view, err = r.complete(h)
				}
				a := composedAttempt(view.Historical)
				if err != nil || a.Direction != upgradetx.DirectionForward || a.Phase != upgradetx.PhaseCompleted || a.Outcome != upgradetx.OutcomeSuccess {
					t.Errorf("imported completion: phase=%s outcome=%s reason=%q receipts=%s/%s/%s err=%v", a.Phase, a.Outcome, view.Historical.Reason,
						a.Control.Kind, a.Resources.Kind, a.Released.Kind, err)
				}
				if want := map[string]int{"steady": 0, "settled": 1, "recovered": 1}[mode]; r.settled != want {
					t.Errorf("settlement injected %d times, want %d", r.settled, want)
				}
				close(r.released)
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					t.Fatal("replaced source Start did not return")
				}
				after, loadErr := r.store.Load(r.options.Key, r.grant.Handle.TxID)
				if loadErr != nil || err == nil && after.Historical.Revision != view.Historical.Revision {
					t.Fatalf("late source return rewrote the imported completion: %v", loadErr)
				}
			})
		}
	}
}
