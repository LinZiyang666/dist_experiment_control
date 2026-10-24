package upgradestore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func actionFixture(r upgradetx.Record, kind upgradetx.ActionKind) upgradetx.Action {
	return upgradetx.Action{ID: strings.Repeat("d", 32), Handle: attempt(r).Handle, Kind: kind, IntentDigest: strings.Repeat("e", 64), State: upgradetx.ActionIntent, Nonce: strings.Repeat("9", 32)}
}

func apply(t *testing.T, s *Store, r upgradetx.Record, action upgradetx.Action) upgradetx.Record {
	t.Helper()
	r, err := s.Apply(attempt(r).Handle, upgradetx.Event{Action: action})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// origin: hot-update S8 plan §3.2 / §9.1
func TestActionCrashRecovery(t *testing.T) {
	kinds := []upgradetx.ActionKind{upgradetx.ActionPublish, upgradetx.ActionSelect, upgradetx.ActionFreeze, upgradetx.ActionDispatch, upgradetx.ActionActivate, upgradetx.ActionControl, upgradetx.ActionMigrate, upgradetx.ActionRetire, upgradetx.ActionReverse, upgradetx.ActionRelease}
	for _, kind := range kinds {
		for _, window := range []string{"before_intent", "before_effect", "before_receipt", "after_receipt"} {
			t.Run(string(kind)+"/"+window, func(t *testing.T) {
				s, req := newFixture(t)
				r := start(t, s, prepare(t, s, req))
				action := actionFixture(r, kind)
				if window != "before_intent" {
					r = apply(t, s, r, action)
				}
				// The external fact is a separate durable object. Reopening the
				// journal loses all local execution memory; recovery may query
				// this object but is never given a way to repeat its effect.
				effectPath := filepath.Join(filepath.Dir(s.options.Path), "external-effect")
				payload := []byte(action.ID + "/" + string(kind) + "/" + action.Nonce)
				if window == "before_receipt" || window == "after_receipt" {
					f, err := os.OpenFile(effectPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.Write(payload); err != nil {
						_ = f.Close()
						t.Fatal(err)
					}
					if err := f.Sync(); err != nil {
						_ = f.Close()
						t.Fatal(err)
					}
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if window == "after_receipt" {
					action.State, action.ResultDigest = upgradetx.ActionObserved, digestBytes(payload)
					r = apply(t, s, r, action)
					action.State = upgradetx.ActionDurable
					r = apply(t, s, r, action)
				}
				s = reopen(t, s)
				view, err := s.Load(s.options.Key, r.Grant.Handle.TxID)
				if err != nil || view.Observation.OwnerKnown || !reflect.DeepEqual(view.Historical, r) {
					t.Fatal("reopen changed history or fabricated current proof")
				}
				observed := 0
				got, err := s.RecoverActions(attempt(r).Handle, func(a upgradetx.Action) (string, error) {
					observed++
					if a.ID != action.ID || a.Nonce != action.Nonce || a.Kind != kind {
						t.Fatal("observer received a different action")
					}
					b, err := os.ReadFile(effectPath)
					if err != nil {
						return "", err
					}
					return digestBytes(b), nil
				})
				switch window {
				case "before_intent":
					if err != nil || observed != 0 || len(got.Actions) != 0 {
						t.Fatal("missing intent became an action")
					}
				case "before_effect":
					requireCode(t, err, upgradetx.CodeUnsettled)
					if _, err := os.Stat(effectPath); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("recovery executed missing effect")
					}
				case "before_receipt", "after_receipt":
					if err != nil || observed != 1 || len(got.Actions) != 1 || got.Actions[0].State != upgradetx.ActionDurable || got.Actions[0].ResultDigest != digestBytes(payload) {
						t.Fatalf("independent recovery failed: %v", err)
					}
					bytes, err := os.ReadFile(effectPath)
					if err != nil || string(bytes) != string(payload) {
						t.Fatal("recovery changed effect")
					}
					// A durable field is historical evidence. Delete the external
					// fact and even this state must become unknown on recheck.
					if err := os.Remove(effectPath); err != nil {
						t.Fatal(err)
					}
					_, err = reopen(t, s).RecoverActions(attempt(got).Handle, func(upgradetx.Action) (string, error) { return "", os.ErrNotExist })
					requireCode(t, err, upgradetx.CodeUnsettled)
				}
			})
		}
	}
}

// origin: hot-update S8 plan §3.2 / intent/result monotonicity
func TestActionEvidenceRequiresIntentAndStableIdentity(t *testing.T) {
	s, req := newFixture(t)
	r := start(t, s, prepare(t, s, req))
	a := actionFixture(r, upgradetx.ActionDispatch)
	a.State, a.ResultDigest = upgradetx.ActionDurable, strings.Repeat("f", 64)
	_, err := s.Apply(attempt(r).Handle, upgradetx.Event{Action: a})
	requireCode(t, err, upgradetx.CodeRejected)
	a.State, a.ResultDigest = upgradetx.ActionIntent, ""
	r = apply(t, s, r, a)
	for _, change := range []func(*upgradetx.Action){
		func(a *upgradetx.Action) { a.Nonce = "foreign" },
		func(a *upgradetx.Action) { a.IntentDigest = strings.Repeat("f", 64) },
		func(a *upgradetx.Action) { a.Kind = upgradetx.ActionActivate },
		func(a *upgradetx.Action) { a.Handle.CutID = strings.Repeat("f", 32) },
		func(a *upgradetx.Action) { a.State = upgradetx.ActionDurable },
	} {
		candidate := a
		candidate.Handle, candidate.State, candidate.ResultDigest = attempt(r).Handle, upgradetx.ActionObserved, strings.Repeat("f", 64)
		change(&candidate)
		_, err := s.Apply(attempt(r).Handle, upgradetx.Event{Action: candidate})
		requireCode(t, err, upgradetx.CodeRejected)
	}
	a.State, a.ResultDigest = upgradetx.ActionObserved, strings.Repeat("f", 64)
	r = apply(t, s, r, a)
	a.Handle, a.State, a.ResultDigest = attempt(r).Handle, upgradetx.ActionDurable, strings.Repeat("a", 64)
	_, err = s.Apply(attempt(r).Handle, upgradetx.Event{Action: a})
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §3.2 / lost legacy dispatch cannot be resent
func TestLegacyDispatchRecoveryStaysUnknown(t *testing.T) {
	s, req := newFixture(t)
	req.Mode = upgradetx.ModeLegacyInterrupting
	r := start(t, s, prepare(t, s, req))
	r = apply(t, s, r, actionFixture(r, upgradetx.ActionDispatch))
	calls := 0
	_, err := reopen(t, s).RecoverActions(attempt(r).Handle, func(upgradetx.Action) (string, error) { calls++; return strings.Repeat("e", 64), nil })
	requireCode(t, err, upgradetx.CodeUnsettled)
	if calls != 0 {
		t.Fatal("legacy intent was inferred from unrelated external observation")
	}
}

// seedCompleted is a disk-codec fixture, not a replacement state machine. It
// supplies a known terminal input to storage/window tests; P01 transition tests
// separately exercise reaching that input through production event semantics.
func seedCompleted(t *testing.T, s *Store, r upgradetx.Record) upgradetx.Record {
	t.Helper()
	a := r.Forward
	a.Phase, a.CutStage, a.Outcome = upgradetx.PhaseCompleted, upgradetx.CutReleased, upgradetx.OutcomeSuccess
	for _, kind := range []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased} {
		stamp := upgradetx.Stamp{Attempt: a.Handle, Producer: s.options.Owner, Nonce: strings.Repeat("8", 32), Sequence: 7, At: s.now().UnixNano(), ScopeDigest: r.Grant.ScopeDigest}
		resources := append([]upgradetx.Resource(nil), a.Scope.Expected...)
		for i := range resources {
			resources[i].Custody, resources[i].State = "released", "terminal"
		}
		receipt := upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, EvidenceDigest: strings.Repeat("f", 64), Census: upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: resources, ExpectedDigest: expectedDigest(a.Scope)}}
		switch kind {
		case upgradetx.ReceiptControlReady:
			a.Control = receipt
		case upgradetx.ReceiptResourcesMigrated:
			a.Resources = receipt
		case upgradetx.ReceiptOldGenerationReleased:
			a.Released = receipt
		}
	}
	r.Forward, r.EffectiveSHA = a, a.TargetSHA
	if err := s.locked(func(d *diskFile, e *diskEntry) error { e.Current = &r; return s.write(d, true) }); err != nil {
		t.Fatal(err)
	}
	return r
}

// origin: hot-update S8 plan §3.1 / §3.3 / M11
func TestNextStartClosesWindowAndFencesLateWriters(t *testing.T) {
	s, req := newFixture(t)
	r := start(t, s, prepare(t, s, req))
	_, err := s.CloseWindow(attempt(r).Handle)
	requireCode(t, err, upgradetx.CodeUnsettled)
	r = seedCompleted(t, s, r)
	history := r
	req.ClientNonce, req.SourceSHA, req.TargetSHA = "next", req.TargetSHA, strings.Repeat("c", 64)
	g := prepare(t, s, req)
	fence := upgradetx.Fence{Current: attempt(r).Handle, Owner: s.options.Owner, RunningSHA: g.SourceSHA, PreviousSHA: r.RecordedPreviousSHA}
	syncs := 0
	s.beforeSync = func(kind string) error {
		if kind == "directory" {
			syncs++
		}
		return nil
	}
	next, err := s.Start(g, fence)
	if err != nil || syncs != 2 {
		t.Fatalf("next Start did not durably close before acceptance: syncs=%d err=%v", syncs, err)
	}
	view, err := s.Load(s.options.Key, history.Grant.Handle.TxID)
	if err != nil || !reflect.DeepEqual(view.Historical, history) || view.Metadata.Window != upgradetx.WindowClosed {
		t.Fatal("window closure rewrote immutable history")
	}
	_, err = s.Apply(attempt(history).Handle, upgradetx.Event{Action: actionFixture(history, upgradetx.ActionSelect)})
	requireCode(t, err, upgradetx.CodeRejected)
	_, err = s.CloseWindow(attempt(history).Handle)
	requireCode(t, err, upgradetx.CodeRejected)
	_, err = s.Archive(attempt(history).Handle, upgradetx.DeathProof{})
	requireCode(t, err, upgradetx.CodeRejected)
	view, err = reopen(t, s).Load(s.options.Key, next.Grant.Handle.TxID)
	if err != nil || !reflect.DeepEqual(view.Historical, next) {
		t.Fatal("late writer changed successor")
	}
}

// origin: hot-update S8 plan §8.1 / M12 historical reverse fence
func TestReverseGrantBindsCurrentAndRecordedPrevious(t *testing.T) {
	s, req := newFixture(t)
	r := seedCompleted(t, s, start(t, s, prepare(t, s, req)))
	fence := upgradetx.Fence{Current: attempt(r).Handle, Owner: s.options.Owner, RunningSHA: r.Forward.TargetSHA, PreviousSHA: r.RecordedPreviousSHA, Incarnation: "current-incarnation"}
	reverse := upgradetx.ReverseRequest{ClientNonce: "reverse", ActorFP: req.ActorFP, HistoricalTxID: r.Grant.Handle.TxID, TargetSHA: r.RecordedPreviousSHA, Scope: req.Scope}
	for _, change := range []func(*upgradetx.Fence, *upgradetx.ReverseRequest){
		func(f *upgradetx.Fence, _ *upgradetx.ReverseRequest) { f.Current.TxID = strings.Repeat("1", 32) },
		func(f *upgradetx.Fence, _ *upgradetx.ReverseRequest) { f.Current.CutID = strings.Repeat("1", 32) },
		func(f *upgradetx.Fence, _ *upgradetx.ReverseRequest) { f.Current.Generation++ },
		func(f *upgradetx.Fence, _ *upgradetx.ReverseRequest) { f.RunningSHA = f.PreviousSHA },
		func(_ *upgradetx.Fence, r *upgradetx.ReverseRequest) { r.TargetSHA = strings.Repeat("c", 64) },
		func(_ *upgradetx.Fence, r *upgradetx.ReverseRequest) { r.HistoricalTxID = strings.Repeat("1", 32) },
	} {
		badFence, badReq := fence, reverse
		change(&badFence, &badReq)
		_, err := s.PrepareReverse(badFence, badReq)
		requireCode(t, err, upgradetx.CodeRejected)
	}
	g, err := s.PrepareReverse(fence, reverse)
	if err != nil {
		t.Fatal(err)
	}
	if g.Handle.TxID == r.Grant.Handle.TxID || g.Handle.CutID == r.Forward.Handle.CutID || g.Handle.Generation != r.Forward.Handle.Generation+1 || g.SourceSHA != fence.RunningSHA || g.TargetSHA != r.RecordedPreviousSHA {
		t.Fatal("reverse reused a forward identity or target")
	}
	got, err := s.Start(g, fence)
	if err != nil {
		t.Fatal(err)
	}
	if got.Reverse.Direction != upgradetx.DirectionReverse || got.Reverse.Control.Known || got.Reverse.Resources.Known || got.Reverse.Released.Known {
		t.Fatal("reverse borrowed forward completion")
	}
	view, err := s.Load(s.options.Key, r.Grant.Handle.TxID)
	if err != nil || !reflect.DeepEqual(view.Historical, r) {
		t.Fatal("manual reverse rewrote completed execution")
	}
}

func deathFixture(s *Store, r upgradetx.Record) upgradetx.DeathProof {
	observer := s.options.Owner
	observer.InstanceID, observer.PID = "independent-observer", observer.PID+1
	stamp := upgradetx.Stamp{Attempt: attempt(r).Handle, Producer: observer, Nonce: strings.Repeat("7", 32), At: s.now().UnixNano(), Sequence: 9, ScopeDigest: r.Grant.ScopeDigest}
	resources := append([]upgradetx.Resource(nil), attempt(r).Scope.Expected...)
	for i := range resources {
		resources[i].State, resources[i].Custody = "terminal", "released"
	}
	return upgradetx.DeathProof{Owner: r.Grant.Issuer, Stamp: stamp, Reaped: true, NoOwnedFDs: true, Custody: upgradetx.Census{Stamp: stamp, Known: true, Complete: true, ExpectedDigest: expectedDigest(attempt(r).Scope), Resources: resources}}
}

// origin: hot-update S8 plan §8.1 / PID absence is not custody clearance
func TestArchiveRequiresIndependentDeathAndExactCustody(t *testing.T) {
	s, req := newFixture(t)
	r := start(t, s, prepare(t, s, req))
	for _, change := range []func(*upgradetx.DeathProof){
		func(d *upgradetx.DeathProof) { d.Reaped = false },
		func(d *upgradetx.DeathProof) { d.NoOwnedFDs = false },
		func(d *upgradetx.DeathProof) { d.Owner.StartTicks++ },
		func(d *upgradetx.DeathProof) { d.Custody.Known = false },
		func(d *upgradetx.DeathProof) { d.Custody.Complete = false },
		func(d *upgradetx.DeathProof) { d.Custody.Resources = nil },
		func(d *upgradetx.DeathProof) { d.Custody.Resources[0].ID = "different-resource" },
		func(d *upgradetx.DeathProof) { d.Custody.Admitting = 1 },
		func(d *upgradetx.DeathProof) { d.Custody.OldOwners = []upgradetx.Owner{r.Grant.Issuer} },
		func(d *upgradetx.DeathProof) { d.Stamp.Producer = d.Owner; d.Custody.Stamp = d.Stamp },
		func(d *upgradetx.DeathProof) {
			d.Stamp.At -= int64(upgradebudget.RequestLatency) + 1
			d.Custody.Stamp = d.Stamp
		},
	} {
		d := deathFixture(s, r)
		change(&d)
		_, err := s.Archive(attempt(r).Handle, d)
		requireCode(t, err, upgradetx.CodeUnsettled)
	}
	view, err := s.Archive(attempt(r).Handle, deathFixture(s, r))
	if err != nil {
		t.Fatal(err)
	}
	if attempt(view.Historical).Phase != upgradetx.PhaseAbandoned || attempt(view.Historical).Outcome != upgradetx.OutcomeUnknown {
		t.Fatal("owner death invented a rollback")
	}
	req.ClientNonce = "successor-after-death"
	if next := prepare(t, reopen(t, s), req); next.Handle.Generation <= r.Forward.Handle.Generation {
		t.Fatal("archive reset generation identity")
	}
	_, err = s.Apply(attempt(r).Handle, upgradetx.Event{Action: actionFixture(r, upgradetx.ActionRelease)})
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §4.1 / P01 dependency, not a fake state machine
func TestJournalPersistsValidatedPhaseTransitions(t *testing.T) {
	s, req := newFixture(t)
	// origin: S10 internal review NO-SKIP-AS-PASS F3
	// Bind the landed P01 transition port: a missing implementation must fail
	// instead of letting an inert P00 witness certify journal persistence.
	s.options.Transition = upgradetx.Transition
	r := start(t, s, prepare(t, s, req))
	for _, phase := range []upgradetx.Phase{upgradetx.PhaseStaged, upgradetx.PhaseReady, upgradetx.PhaseFreezing} {
		cut := upgradetx.CutNone
		if phase == upgradetx.PhaseFreezing {
			cut = upgradetx.CutPristine
		}
		next, err := s.Apply(attempt(r).Handle, upgradetx.Event{Phase: phase, CutStage: cut, Outcome: upgradetx.OutcomePending})
		if err != nil {
			t.Fatal(err)
		}
		view, err := reopen(t, s).Load(s.options.Key, next.Grant.Handle.TxID)
		if err != nil || !reflect.DeepEqual(next, view.Historical) || attempt(next).Phase != phase || next.Revision <= r.Revision {
			t.Fatal("validated transition did not persist with a new CAS revision")
		}
		_, err = s.Apply(attempt(r).Handle, upgradetx.Event{Phase: phase, CutStage: cut, Outcome: upgradetx.OutcomePending})
		requireCode(t, err, upgradetx.CodeRejected)
		r = next
	}
}

// origin: hot-update S8 plan §3.1 / §4.2 / missing independent receipts
func TestJournalRejectsIncompleteTerminalProof(t *testing.T) {
	for _, change := range []func(*upgradetx.Record){
		func(r *upgradetx.Record) { r.Forward.Control.Known = false },
		func(r *upgradetx.Record) { r.Forward.Resources.Complete = false },
		func(r *upgradetx.Record) { r.Forward.Released.Known = false },
		func(r *upgradetx.Record) { r.Forward.Released.Census.Resources = nil },
		func(r *upgradetx.Record) { r.Forward.Released.Census.Resources[0].ID = "unrelated" },
		func(r *upgradetx.Record) { r.Forward.Released.Census.Resources[0].Custody = "source" },
		func(r *upgradetx.Record) { r.Forward.Resources.Census.Resources[0].Custody = "source" },
		func(r *upgradetx.Record) {
			r.Forward.Resources.Census.Resources[0].Custody = "target"
			r.Forward.Resources.Census.Resources[0].Generation = r.Forward.Handle.Generation + 1
		},
		func(r *upgradetx.Record) { r.Forward.Released.Census.OldOwners = []upgradetx.Owner{r.Grant.Issuer} },
		func(r *upgradetx.Record) { r.Forward.Control.Stamp.Attempt.CutID = strings.Repeat("f", 32) },
		func(r *upgradetx.Record) { r.Forward.Control.RunningSHA = r.Grant.SourceSHA },
	} {
		s, req := newFixture(t)
		r := seedCompleted(t, s, start(t, s, prepare(t, s, req)))
		change(&r)
		data, err := os.ReadFile(s.options.Path)
		if err != nil {
			t.Fatal(err)
		}
		var d diskFile
		if err := strictJSON(data, &d); err != nil {
			t.Fatal(err)
		}
		d.Entries[0].Current = &r
		if err := os.WriteFile(s.options.Path, marshalFixture(t, d), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = Open(s.options)
		requireCode(t, err, upgradetx.CodeStateCorrupt)
	}
	s, req := newFixture(t)
	r := seedCompleted(t, s, start(t, s, prepare(t, s, req)))
	// origin: S8 internal review rv3-leaves F5
	// This positive fixture witnesses a live target throughout the cut. Keeping
	// Control's seeded released terminal would instead resurrect execution.
	for _, receipt := range []*upgradetx.Receipt{&r.Forward.Control, &r.Forward.Resources, &r.Forward.Released} {
		for i := range receipt.Census.Resources {
			receipt.Census.Resources[i].Custody, receipt.Census.Resources[i].State, receipt.Census.Resources[i].Generation = "target", "running", r.Forward.Handle.Generation
		}
	}
	if err := s.locked(func(d *diskFile, e *diskEntry) error { e.Current = &r; return s.write(d, true) }); err != nil {
		t.Fatalf("valid target custody rejected: %v", err)
	}
}

// origin: hot-update S8 plan §4.1 / §8.1 / pending reverse is the same transaction
func TestPendingReverseKeepsOriginalTransactionAndActionHistory(t *testing.T) {
	s, req := newFixture(t)
	g := prepare(t, s, req)
	r := start(t, s, g)
	r = apply(t, s, r, actionFixture(r, upgradetx.ActionActivate))
	r.Forward.Phase, r.Forward.CutStage = upgradetx.PhaseActivating, upgradetx.CutPartialActive
	r.EffectiveSHA = g.TargetSHA
	if err := s.locked(func(d *diskFile, e *diskEntry) error { e.Current = &r; return s.write(d, true) }); err != nil {
		t.Fatal(err)
	}
	fence := upgradetx.Fence{Current: attempt(r).Handle, Owner: s.options.Owner, RunningSHA: g.TargetSHA, PreviousSHA: g.SourceSHA, Incarnation: "current-incarnation"}
	scope := req.Scope
	scope.Expected = append(append([]upgradetx.Resource(nil), scope.Expected...), scope.Expected[0])
	scope.Expected[1].ID = "admitted-after-forward"
	reverse, err := s.PrepareReverse(fence, upgradetx.ReverseRequest{ClientNonce: "reverse", ActorFP: req.ActorFP, HistoricalTxID: g.Handle.TxID, TargetSHA: g.SourceSHA, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if reverse.Handle.TxID != g.Handle.TxID || reverse.Handle.CutID == g.Handle.CutID || reverse.Handle.Generation != g.Handle.Generation+1 {
		t.Fatal("pending reverse lost its transaction or reused its attempt")
	}
	current, err := s.Start(reverse, fence)
	if err != nil {
		t.Fatal(err)
	}
	r.Forward.Phase = upgradetx.PhaseRollbackPending
	if !reflect.DeepEqual(current.Forward, r.Forward) || !reflect.DeepEqual(current.Actions, r.Actions) || current.Grant != g || len(current.Reverse.Scope.Expected) != 2 {
		t.Fatal("reverse lost current resources or forward history")
	}
	for _, retry := range []upgradetx.Grant{g, reverse} {
		queried, err := reopen(t, s).Start(retry, upgradetx.Fence{})
		if err != nil || !reflect.DeepEqual(queried, current) {
			t.Fatal("accepted retry restarted an attempt")
		}
	}
	_, err = s.Apply(attempt(r).Handle, upgradetx.Event{Action: actionFixture(r, upgradetx.ActionSelect)})
	requireCode(t, err, upgradetx.CodeRejected)
	recovered, err := s.RecoverActions(attempt(current).Handle, func(upgradetx.Action) (string, error) {
		t.Fatal("reverse used forward action evidence")
		return "", nil
	})
	if err != nil || !reflect.DeepEqual(recovered, current) {
		t.Fatal("reverse query changed forward evidence")
	}
}
