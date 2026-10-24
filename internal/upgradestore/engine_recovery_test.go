package upgradestore_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Uses the existing composed runtime for local facts, and real journal,
// transition, publication, admission registry and engines. No process is spawned.
type cutWriteStore struct {
	*upgradestore.Store
	writes, stop int
	last         upgradetx.Event
	reopen       func(upgradetx.Record)
}
type journalCut struct{}

func (s *cutWriteStore) Apply(h upgradetx.Handle, event upgradetx.Event) (upgradetx.Record, error) {
	r, err := s.Store.Apply(h, event)
	if err == nil {
		s.writes++
		s.last = event
		if s.reopen != nil {
			s.reopen(r)
		}
		if s.writes == s.stop {
			panic(journalCut{})
		}
	}
	return r, err
}

func runUntilJournalCut(t *testing.T, f func()) (cut bool) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(journalCut); !ok {
				panic(p)
			}
			cut = true
		}
	}()
	f()
	return false
}

// origin: S8 internal review rv3-leaves F10
func TestJournalWriteCrashCanFinishActivatedCut(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		for stop := 1; stop <= 40; stop++ {
			t.Run(fmt.Sprintf("%s/write-%02d", role, stop), func(t *testing.T) {
				r := newComposedRuntime(t, role)
				store := &cutWriteStore{Store: r.store, stop: stop, reopen: func(written upgradetx.Record) {
					r.reopen()
					loaded, loadErr := r.store.Load(r.options.Key, "")
					if loadErr != nil || !reflect.DeepEqual(loaded.Historical, written) {
						t.Fatalf("successful journal write changed after independent reopen: revision=%d err=%v", written.Revision, loadErr)
					}
				}}
				var endpoint upgradeport.Endpoint
				if role == upgradetx.ComponentAgent {
					endpoint = agentupgrade.New(store, r, r.registry)
				} else {
					endpoint = brokerupgrade.New(store, r, r.registry)
				}
				cut := runUntilJournalCut(t, func() { _, _ = endpoint.Start(t.Context(), r.grant, r.receipt) })
				if !cut {
					t.Skip("attempt completed before this write ordinal")
				}
				if store.writes != stop {
					t.Fatal("crash boundary was not hit exactly")
				}
				loaded, loadErr := r.store.Load(r.options.Key, "")
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				pending := composedAttempt(loaded.Historical)
				if (store.last.Action.Kind == upgradetx.ActionSelect || store.last.Action.Kind == upgradetx.ActionActivate) &&
					(store.last.Action.State == upgradetx.ActionObserved || store.last.Action.State == upgradetx.ActionDurable) && loaded.Historical.EffectiveSHA != r.running {
					t.Fatal("durable selector readback remained tied to the missing Control receipt")
				}
				if r.effects == 0 && loaded.Historical.EffectiveSHA != r.running {
					t.Fatal("selector intent invented an effect before activation")
				}
				uncertain := pending.Phase != upgradetx.PhaseCompleted && r.effects != 0
				for _, action := range loaded.Historical.Actions {
					if pending.Phase != upgradetx.PhaseCompleted && (action.Kind == upgradetx.ActionSelect || action.Kind == upgradetx.ActionDispatch || action.Kind == upgradetx.ActionActivate) {
						uncertain = true
					}
				}
				path, coldErr := upgradeartifact.ResolveCanonical(r.location.CanonicalPath, r.options.Key, &loaded)
				if uncertain && coldErr == nil {
					t.Fatalf("ordinary cold boot authorized unresolved %s/%s: %s", store.last.Action.Kind, store.last.Action.State, path)
				}
				if r.effects == 0 {
					return
				}
				r.reopen()
				a := r.current()
				var v upgradetx.View
				var err error
				if role == upgradetx.ComponentAgent {
					v, err = agentupgrade.Boot(t.Context(), r.endpoint(), a.Handle)
				} else {
					v, err = brokerupgrade.Resume(t.Context(), r.endpoint(), a.Handle)
				}
				if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 1 {
					t.Fatalf("durable cut after %s/%s phase=%s stage=%s; resume phase=%s effects=%d err=%v", store.last.Action.Kind, store.last.Action.State, a.Phase, a.CutStage, composedAttempt(v.Historical).Phase, r.effects, err)
				}
			})
		}
	}
}

type afterControlStore struct {
	*upgradestore.Store
	r        *composedRuntime
	admitted bool
}

func (s *afterControlStore) Apply(h upgradetx.Handle, e upgradetx.Event) (upgradetx.Record, error) {
	r, err := s.Store.Apply(h, e)
	if err == nil && e.Receipt.Kind == upgradetx.ReceiptControlReady && !s.admitted {
		s.admitted = true
		row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "new-after-control", Origin: "real-request", Owner: s.r.owner, Generation: composedAttempt(r).Handle.Generation, State: "active", Custody: "target"}
		seq, beginErr := s.r.registry.Begin(row)
		if beginErr != nil {
			s.r.t.Fatal(beginErr)
		}
		s.r.sequence = seq
		s.r.resources = append(s.r.resources, row)
		if finishErr := s.r.registry.Finish(seq, row); finishErr != nil {
			s.r.t.Fatal(finishErr)
		}
	}
	return r, err
}

// origin: S8 internal review rv3-leaves F4
func TestAdmissionAfterControlStillCompletesForward(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			store := &afterControlStore{Store: r.store, r: r}
			var endpoint upgradeport.Endpoint
			if role == upgradetx.ComponentAgent {
				endpoint = agentupgrade.New(store, r, r.registry)
			} else {
				endpoint = brokerupgrade.New(store, r, r.registry)
			}
			v, err := endpoint.Start(t.Context(), r.grant, r.receipt)
			if !store.admitted {
				t.Fatal("missing real admission")
			}
			if err != nil || v.Historical.Forward.Phase != upgradetx.PhaseCompleted || r.effects != 1 {
				got, loadErr := r.store.Load(r.options.Key, "")
				t.Fatalf("ordinary admitted work prevented forward completion: current=%s forward=%s reverse=%s effects=%d err=%v load=%v", composedAttempt(got.Historical).Phase, got.Historical.Forward.Phase, got.Historical.Reverse.Phase, r.effects, err, loadErr)
			}
		})
	}
}

type failRetireOnceRuntime struct {
	*composedRuntime
	fail bool
}

func (r *failRetireOnceRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	if r.fail {
		r.fail = false
		return upgradetx.Receipt{}, errors.New("temporary observer unavailable")
	}
	return r.composedRuntime.Retire(h)
}

// origin: S8 internal review rv3-leaves F9
func TestBrokerResumeAfterTemporaryRetirementFailure(t *testing.T) {
	r := &failRetireOnceRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker), fail: true}
	e := brokerupgrade.New(r.store, r, r.registry)
	_, err := e.Start(t.Context(), r.grant, r.receipt)
	if err == nil || r.effects != 1 {
		t.Fatal("missing post-activation failure")
	}
	r.reopen()
	h := r.current().Handle
	e = brokerupgrade.New(r.store, r, r.registry)
	v, err := brokerupgrade.Resume(t.Context(), e, h)
	if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
		row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "after-repair", Origin: "new-request", Owner: r.owner, Generation: h.Generation, State: "active", Custody: "target"}
		_, admissionErr := r.registry.Begin(row)
		t.Fatalf("healthy current owner cannot resume: phase=%s err=%v; new admission=%v", composedAttempt(v.Historical).Phase, err, admissionErr)
	}
	if r.effects != 1 {
		t.Fatal("resume repeated activation")
	}
	row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "after-repair", Origin: "new-request", Owner: r.owner, Generation: h.Generation, State: "active", Custody: "target"}
	seq, err := r.registry.Begin(row)
	if err != nil {
		t.Fatalf("completed recovery retained admission fence: %v", err)
	}
	if err = r.registry.Finish(seq, row); err != nil {
		t.Fatal(err)
	}
}

// origin: S8 internal review rv3-leaves F10
func TestColdResolveCannotSelectOldCodeAfterActivation(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			store := &cutWriteStore{Store: r.store, stop: 12}
			// Both engines have activated by this durable result write, and
			// neither has persisted the control receipt. Keep a live source of
			// runtime facts; only the controller journal is reopened.
			var e upgradeport.Endpoint
			if role == upgradetx.ComponentAgent {
				e = agentupgrade.New(store, r, r.registry)
			} else {
				e = brokerupgrade.New(store, r, r.registry)
			}
			if !runUntilJournalCut(t, func() { _, _ = e.Start(t.Context(), r.grant, r.receipt) }) || r.effects != 1 {
				t.Fatal("cut missed activated runtime")
			}
			r.reopen()
			v, err := r.store.Load(r.options.Key, "")
			if err != nil {
				t.Fatal(err)
			}
			path, err := upgradeartifact.Resolve(r.options.Key, v)
			if err == nil && path == r.receipt.SourcePath {
				t.Fatalf("partial ACTIVE journal selected old source without current export or reverse: phase=%s stage=%s running_target=%t", composedAttempt(v.Historical).Phase, composedAttempt(v.Historical).CutStage, r.running == r.grant.TargetSHA)
			}
			if path, err = upgradeartifact.ResolveCanonical(r.location.CanonicalPath, r.options.Key, &v); err == nil {
				t.Fatalf("canonical cold adapter authorized interrupted activation: %s", path)
			}
		})
	}
}
