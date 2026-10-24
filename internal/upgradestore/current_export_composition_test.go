package upgradestore_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Explicit runtime input at the integration port. The descriptor oracle runs
// before Activate's first replay effect; it does not claim a production broker
// database import, S7 transfer migration, or a same-PID exec.
type currentExportState struct {
	Exec      map[string]bool
	Transfer  map[string]uint64
	Authority map[string]bool
}
type currentExportRuntime struct {
	*composedRuntime
	state        currentExportState
	reverse      bool
	lateAtFreeze bool
	checked      int
}

func (r *currentExportRuntime) admit(domain upgradetx.Domain, id string) {
	r.t.Helper()
	row := upgradetx.Resource{Domain: domain, ID: id, Origin: "new-request", Owner: r.owner, Generation: 2, State: "active", Custody: "target"}
	seq, err := r.registry.Begin(row)
	if err != nil {
		r.t.Fatal(err)
	}
	r.sequence = seq
	r.resources = append(r.resources, row)
	if err = r.registry.Finish(seq, row); err != nil {
		r.t.Fatal(err)
	}
}
func (r *currentExportRuntime) export(h upgradetx.Handle) (upgradeport.Cut, error) {
	cut, err := r.cut(h)
	if err != nil {
		return cut, err
	}
	cut.Scope.Expected = append([]upgradetx.Resource{}, r.resources...)
	cut.Scope, err = upgradetx.CanonicalScope(cut.Scope)
	if err != nil {
		return cut, err
	}
	cut.Descriptor, err = json.Marshal(r.state)
	return cut, err
}
func (r *currentExportRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	if r.reverse && r.lateAtFreeze {
		// This request began after Prepare and completes before admission freezes.
		// Its identity cannot retroactively enter the immutable grant commitment.
		r.admit(upgradetx.DomainExec, "during-prepare")
		r.state.Exec["during-prepare"] = true
	}
	cut, err := r.export(h)
	if r.reverse {
		// Existing work may finish an ACK while the export barrier drains it.
		r.state.Transfer["new-transfer"] = 37
		delete(r.state.Exec, "delete-me")
		delete(r.state.Authority, "revoke-me")
	}
	return cut, err
}
func (r *currentExportRuntime) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) {
	return r.export(h)
}
func (r *currentExportRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	if r.reverse {
		var got currentExportState
		if err := json.Unmarshal(c.Descriptor, &got); err != nil {
			r.t.Fatal(err)
		}
		if !got.Exec["new-exec"] || got.Transfer["new-transfer"] != 37 || got.Exec["delete-me"] || got.Authority["revoke-me"] || !got.Authority["keep-me"] {
			r.t.Fatalf("before first Replay: stale new work, ACK, delete or revoke: %+v", got)
		}
		if r.lateAtFreeze && !got.Exec["during-prepare"] {
			r.t.Fatal("before first Replay: lost request admitted after Prepare")
		}
		if !upgradetx.SameResources(c.Scope.Expected, r.resources) {
			r.t.Fatal("before first Replay: exported custody lost current identities")
		}
		r.checked++
	}
	return r.composedRuntime.Activate(c)
}

// origin: S8 internal review rv-fault F5
func TestBrokerCurrentExportPreservesNewWorkBeforeFirstReplay(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "after-completion"
		if late {
			name = "after-prepare"
		}
		t.Run(name, func(t *testing.T) {
			r := &currentExportRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker), lateAtFreeze: late, state: currentExportState{Exec: map[string]bool{"job": true, "delete-me": true}, Transfer: map[string]uint64{}, Authority: map[string]bool{"revoke-me": true, "keep-me": true}}}
			endpoint := brokerupgrade.New(r.store, r, r.registry)
			v, err := endpoint.Start(t.Context(), r.grant, r.receipt)
			if err != nil {
				t.Fatal(err)
			}
			original := v.Historical
			r.admit(upgradetx.DomainExec, "new-exec")
			r.admit(upgradetx.DomainTransferTracker, "new-transfer")
			r.state.Exec["new-exec"], r.state.Transfer["new-transfer"] = true, 19
			r.scope.Expected = append([]upgradetx.Resource{}, r.resources...)
			r.scope, err = upgradetx.CanonicalScope(r.scope)
			if err != nil {
				t.Fatal(err)
			}
			r.reopen()
			r.reverse = true
			a := r.current()
			endpoint = brokerupgrade.New(r.store, r, r.registry)
			fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: original.RecordedPreviousSHA, Incarnation: txnHex("f", 64)}
			v, err = endpoint.Rollback(t.Context(), fence, upgradetx.ReverseRequest{ClientNonce: txnHex("f", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: r.scope})
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.checked != 1 {
				t.Fatalf("current reverse failed before complete replay: checked=%d err=%v", r.checked, err)
			}
			expected := 3
			if late {
				expected++
			}
			if len(composedAttempt(v.Historical).Released.Census.Resources) != expected {
				t.Fatal("durable completion lost current work")
			}
			r.reopen()
			got, err := r.store.Load(r.options.Key, "")
			if err != nil || !reflect.DeepEqual(got.Historical, v.Historical) {
				t.Fatalf("current reverse lost durable evidence: %v", err)
			}
			old, err := r.store.Load(r.options.Key, original.Grant.Handle.TxID)
			if err != nil || !reflect.DeepEqual(old.Historical, original) {
				t.Fatal("reverse rewrote original completion")
			}
		})
	}
}

type forwardAdmissionRuntime struct{ *composedRuntime }

func (r *forwardAdmissionRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	row := upgradetx.Resource{Domain: upgradetx.DomainTransferWorker, ID: "after-prepare", Origin: "late-create", Owner: r.owner, Generation: r.current().SourceGeneration, State: "active", Custody: "source"}
	seq, err := r.registry.Begin(row)
	if err != nil {
		return upgradeport.Cut{}, err
	}
	r.sequence = seq
	r.resources = append(r.resources, row)
	if err = r.registry.Finish(seq, row); err != nil {
		return upgradeport.Cut{}, err
	}
	c, err := r.composedRuntime.Freeze(h)
	if err != nil {
		return c, err
	}
	c.Scope.Expected = append([]upgradetx.Resource{}, r.resources...)
	c.Scope, err = upgradetx.CanonicalScope(c.Scope)
	return c, err
}

// origin: S8 internal review rv-fault F5
func TestForwardCutIncludesAdmissionAfterPrepare(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := &forwardAdmissionRuntime{newComposedRuntime(t, role)}
			var endpoint upgradeport.Endpoint
			if role == upgradetx.ComponentAgent {
				endpoint = agentupgrade.New(r.store, r, r.registry)
			} else {
				endpoint = brokerupgrade.New(r.store, r, r.registry)
			}
			v, err := endpoint.Start(t.Context(), r.grant, r.receipt)
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("late creation prevented forward cut: %v", err)
			}
			if len(composedAttempt(v.Historical).Scope.Expected) != 1 || len(composedAttempt(v.Historical).Released.Census.Resources) != 2 {
				t.Fatal("grant commitment changed or late custody disappeared")
			}
			r.reopen()
			got, err := r.store.Load(r.options.Key, "")
			if err != nil || !reflect.DeepEqual(got.Historical, v.Historical) {
				t.Fatal("late creation did not survive reopen")
			}
		})
	}
}
