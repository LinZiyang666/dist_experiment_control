package upgradestore_test

import (
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// The runtime port models a selector unflip and its independent readback. All
// cancellation decisions, journal writes, and subsequent Start use real leaves.
type cancellationRuntime struct {
	*composedRuntime
	defect  string
	thawed  bool
	freezes int
}

func (r *cancellationRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	r.freezes++
	return r.composedRuntime.Freeze(h)
}
func (r *cancellationRuntime) Thaw(h upgradetx.Handle) error {
	r.thawed = true
	if r.defect == "thaw" {
		return errors.New("unflip failed")
	}
	return r.composedRuntime.Thaw(h)
}
func (r *cancellationRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	o, err := r.composedRuntime.Observe(s)
	if r.thawed {
		switch r.defect {
		case "selector":
			o.EffectiveSHA = r.grant.TargetSHA
		case "owner":
			o.Census.Stamp.Producer.PID++
		case "custody":
			o.Census.Resources[0].Owner.PID++
		case "admitting":
			o.Census.Admitting++
		}
	}
	return o, err
}
func (r *cancellationRuntime) endpoint() upgradeport.Endpoint {
	if r.options.Key.Component == upgradetx.ComponentAgent {
		return agentupgrade.New(r.store, r, r.registry)
	}
	return brokerupgrade.New(r.store, r, r.registry)
}
func cancellationFrozen(t *testing.T, r *cancellationRuntime) upgradetx.Record {
	t.Helper()
	record, err := r.store.Start(r.grant, upgradetx.Fence{Owner: r.owner, RunningSHA: r.running})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []upgradetx.Event{{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady}, {Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine}} {
		record, err = r.store.Apply(composedAttempt(record).Handle, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	action := upgradetx.Action{ID: txnHex("7", 32), Handle: composedAttempt(record).Handle, Kind: upgradetx.ActionFreeze, IntentDigest: txnHex("8", 64), State: upgradetx.ActionIntent, Nonce: txnHex("9", 32)}
	record, err = r.store.Apply(composedAttempt(record).Handle, upgradetx.Event{Action: action})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Freeze(composedAttempt(record).Handle); err != nil {
		t.Fatal(err)
	}
	for _, state := range []upgradetx.ActionState{upgradetx.ActionObserved, upgradetx.ActionDurable} {
		action.State, action.ResultDigest = state, txnHex("a", 64)
		record, err = r.store.Apply(composedAttempt(record).Handle, upgradetx.Event{Action: action})
		if err != nil {
			t.Fatal(err)
		}
	}
	return record
}

// origin: S8 internal review rv-fault F4
func TestCancellationThawProofSurvivesReopenAndSecondStart(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		for _, defect := range []string{"none", "thaw", "selector", "owner", "custody", "admitting"} {
			t.Run(string(role)+"/"+defect, func(t *testing.T) {
				r := &cancellationRuntime{composedRuntime: newComposedRuntime(t, role), defect: defect}
				record := cancellationFrozen(t, r)
				original := composedAttempt(record)
				v, err := r.endpoint().Cancel(t.Context(), original.Handle)
				if !r.thawed || r.freezes != 1 {
					t.Fatal("exercise did not cross Freeze/Thaw")
				}
				if defect != "none" {
					if err == nil || composedAttempt(v.Historical).Phase == upgradetx.PhaseCancelled {
						t.Fatalf("unproven thaw released current: %v", err)
					}
					r.reopen()
					if _, err = r.store.CloseWindow(r.current().Handle); err == nil {
						t.Fatal("unproven thaw closed window")
					}
					return
				}
				if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCancelled {
					t.Fatalf("safe thaw was not cancelled: %v", err)
				}
				r.reopen()
				if _, err = r.store.CloseWindow(r.current().Handle); err != nil {
					t.Fatalf("safe thaw retained current: %v", err)
				}
				r.reopen()
				cold, err := r.store.Load(r.options.Key, "")
				if err != nil {
					t.Fatal(err)
				}
				path, err := upgradeartifact.Resolve(r.options.Key, cold)
				if err != nil || path != r.receipt.SourcePath {
					t.Fatalf("cancelled cold boot lost source selector: %q %v", path, err)
				}
				r.prepare([]byte("A"), []byte("C"))
				v, err = r.endpoint().Start(t.Context(), r.grant, r.receipt)
				if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
					t.Fatalf("cancelled owner could not upgrade again: %v", err)
				}
				if composedAttempt(v.Historical).SourceGeneration != original.SourceGeneration {
					t.Fatal("cancellation invented a source generation")
				}
			})
		}
	}
}

// A failed activation-intent write occurs after successful Freeze and dispatch
// intent. The real journal retains the exact boundary; no effect is fabricated.
type activationWriteStore struct {
	*upgradestore.Store
	fail bool
}

func (s *activationWriteStore) Apply(h upgradetx.Handle, e upgradetx.Event) (upgradetx.Record, error) {
	if s.fail && e.Action.Kind == upgradetx.ActionActivate && e.Action.State == upgradetx.ActionIntent {
		return upgradetx.Record{}, errors.New("activation intent write failed")
	}
	return s.Store.Apply(h, e)
}

// origin: S8 internal review rv-fault F4
func TestEnginePristineRecoveryCanThawAndUpgradeAgain(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := &cancellationRuntime{composedRuntime: newComposedRuntime(t, role), defect: "none"}
			writer := &activationWriteStore{Store: r.store, fail: true}
			var endpoint upgradeport.Endpoint
			if role == upgradetx.ComponentAgent {
				endpoint = agentupgrade.New(writer, r, r.registry)
			} else {
				endpoint = brokerupgrade.New(writer, r, r.registry)
			}
			_, err := endpoint.Start(t.Context(), r.grant, r.receipt)
			if err == nil || r.freezes != 1 || r.effects != 0 {
				t.Fatal("intent write fault did not preserve pristine runtime")
			}
			r.reopen()
			v, err := r.endpoint().Cancel(t.Context(), r.current().Handle)
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCancelled {
				t.Fatalf("known source cannot leave pristine recovery: %v", err)
			}
			if !r.thawed {
				t.Fatal("cancellation did not thaw original source")
			}
			r.reopen()
			r.prepare([]byte("A"), []byte("C"))
			v, err = r.endpoint().Start(t.Context(), r.grant, r.receipt)
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("thawed source cannot upgrade again: %v", err)
			}
		})
	}
}
