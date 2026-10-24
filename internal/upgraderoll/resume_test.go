package upgraderoll

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §3.1 / §3.2 / §9.1 / M09
func TestDriverResumesOriginalPlan(t *testing.T) {
	for _, lostAt := range []uint32{1, 2, 5} {
		t.Run(testID(int(lostAt)), func(t *testing.T) {
			m, p := newRollModel(t)
			m.startHook = func(step upgradetx.Step) error {
				if step.Ordinal == lostAt {
					return errors.New("reply lost after acceptance")
				}
				return nil
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if m.starts[m.plan.Steps[lostAt-1].Grant.Handle.TxID] != 1 || !m.holder || slices.Contains(m.log, "release:8") {
				t.Fatal("lost reply was retried or parent released")
			}
			original, err := m.LoadPlan(m.plan.Parent)
			if err != nil {
				t.Fatal(err)
			}
			before := clonePlan(original)
			m.startHook = nil
			view, err := Run(t.Context(), m, p, original)
			if err != nil {
				t.Fatal(err)
			}
			if view.Historical.Forward.Phase != upgradetx.PhaseCompleted || m.holder || m.stopCount != 2 || !reflect.DeepEqual(before, *m.sealed) {
				t.Fatalf("resume did not complete the original parent: phase=%s held=%v stops=%d", view.Historical.Forward.Phase, m.holder, m.stopCount)
			}
			for _, step := range before.Steps {
				if isCut(step) && m.starts[step.Grant.Handle.TxID] != 1 {
					t.Fatalf("child %d was recreated: %v", step.Ordinal, m.starts)
				}
			}
			if len(view.Historical.Actions) != len(before.Steps) {
				t.Fatalf("lost ordered steps: actions=%d steps=%d", len(view.Historical.Actions), len(before.Steps))
			}
			for _, action := range view.Historical.Actions {
				if action.State != upgradetx.ActionDurable {
					t.Fatal("parent completed before durable step results")
				}
			}
			if slices.Index(m.log, "start:2") <= slices.Index(m.log, "start:1") || slices.Index(m.log, "release:4") <= slices.Index(m.log, "converge:3") || slices.Index(m.log, "release:8") <= slices.Index(m.log, "release:7") {
				t.Fatal("component/host/cluster hierarchy changed")
			}
			// A second completed Run is a fresh-proof no-op, without additional
			// starts, journal writes or a new parent/child handle.
			writes := len(m.record.Actions)
			revision := m.record.Revision
			if _, err := Run(t.Context(), m, p, original); err != nil {
				t.Fatal(err)
			}
			if len(m.record.Actions) != writes || m.record.Revision != revision {
				t.Fatal("completed parent execution was rewritten")
			}
			for _, step := range before.Steps {
				if isCut(step) && m.starts[step.Grant.Handle.TxID] != 1 {
					t.Fatal("completed resume restarted a child")
				}
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / §8.3 M09
func TestDriverRejectsChangedResumePlan(t *testing.T) {
	for _, name := range []string{"random child", "different target", "changed order", "changed manifest"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			m.startHook = func(upgradetx.Step) error { return errors.New("driver stopped after first dispatch") }
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			original := clonePlan(*m.sealed)
			changed := clonePlan(original)
			switch name {
			case "random child":
				changed.Steps[1].Grant.Handle.TxID = testID(999)
			case "different target":
				changed.Steps[1].Grant.TargetSHA = testSHA(999)
			case "changed order":
				changed.Steps[0], changed.Steps[1] = changed.Steps[1], changed.Steps[0]
			case "changed manifest":
				changed.TargetManifestDigest = testSHA(999)
			}
			m.startHook = nil
			effects := len(m.starts)
			reserveCalls := m.reserveCalls
			if _, err := Run(t.Context(), m, p, changed); err == nil {
				t.Fatal("resume silently replanned")
			}
			if !reflect.DeepEqual(original, *m.sealed) || len(m.starts) != effects || m.reserveCalls != reserveCalls || !m.holder {
				t.Fatal("changed resume replaced the original authority")
			}
		})
	}
}

// origin: hot-update S8 plan §3.3 / §9.1 / M11
func TestLateWriterCannotClaimNextTransaction(t *testing.T) {
	m, p := newRollModel(t)
	original := clonePlan(m.plan)
	next := m.record
	next.Forward.Handle.TxID = testID(900)
	next.Forward.Handle.CutID = testID(901)
	next.Forward.Handle.Generation++
	next.Revision = 1
	next.Forward.Handle.Revision = 1
	next.Grant.Handle = next.Forward.Handle
	replaced := false
	m.applyHook = func(_ upgradetx.Handle, event upgradetx.Event) error {
		if !replaced && event.Action.State == upgradetx.ActionObserved {
			replaced = true
			m.record = next
		}
		return nil
	}
	_, err := runModel(t, m, p)
	requireUnsettled(t, err)
	if !replaced || !reflect.DeepEqual(m.record, next) || slices.Contains(m.log, "release:4") {
		t.Fatal("old driver claimed or released the next transaction")
	}
	for _, state := range []upgradetx.ActionState{upgradetx.ActionIntent, upgradetx.ActionObserved, upgradetx.ActionDurable} {
		if err := writeStep(m, original, original.Steps[0], state, testSHA(10)); err == nil {
			t.Fatal("late writer was accepted")
		}
		if !reflect.DeepEqual(m.record, next) {
			t.Fatal("late writer modified new Current")
		}
	}
}

// origin: hot-update S8 plan §3.1 / §11.2 P11
func TestDriverQueriesExpiredChildrenWithoutStarting(t *testing.T) {
	m, p := newRollModel(t)
	m.plan.Deadline = time.Now().Add(-time.Second).UnixNano()
	for i := range m.plan.Steps {
		m.plan.Steps[i].Grant.ExpiresAt = m.plan.Deadline
	}
	m.record.Grant.ExpiresAt = m.plan.Deadline
	_, err := runModel(t, m, p)
	requireUnsettled(t, err)
	if len(m.starts) != 0 || slices.Contains(m.log, "reserve") {
		t.Fatal("expired parent acquired new execution authority")
	}
	for _, step := range m.plan.Steps {
		if isCut(step) && !slices.Contains(m.log, "query:"+stringOrdinal(step.Ordinal)) {
			t.Fatalf("expired parent omitted child %d", step.Ordinal)
		}
	}
}

func stringOrdinal(ordinal uint32) string { return strconv.FormatUint(uint64(ordinal), 10) }

// origin: hot-update S8 plan §3.2 release / §5.1
func TestDriverResumesLostClusterReleaseReply(t *testing.T) {
	m, p := newRollModel(t)
	m.releaseError = true
	_, err := runModel(t, m, p)
	requireUnsettled(t, err)
	if m.holder || slices.Contains(m.log, "complete-parent") {
		t.Fatal("release loss did not preserve pending parent")
	}
	m.releaseError = false
	view, err := runModel(t, m, p)
	if err != nil {
		t.Fatal(err)
	}
	if view.Historical.Forward.Phase != upgradetx.PhaseCompleted || m.holder {
		t.Fatal("original release tombstone was not resumed")
	}
	for _, step := range m.plan.Steps {
		if isCut(step) && m.starts[step.Grant.Handle.TxID] != 1 {
			t.Fatal("release recovery restarted child execution")
		}
	}
}

// origin: hot-update S8 plan §3.2 / §11.2 P11
func TestChildDispatchRequiresDurableIntentReadback(t *testing.T) {
	m, p := newRollModel(t)
	m.applyNoWrite = true
	_, err := runModel(t, m, p)
	requireUnsettled(t, err)
	if len(m.starts) != 0 || m.startCalls != 0 || len(m.record.Actions) != 0 {
		t.Fatal("intent without durable readback dispatched a child")
	}
}

// origin: hot-update S8 plan §3.1 / §4.2 / §8.3 M06,M09
func TestParentPlanClosesEveryOriginalChildScope(t *testing.T) {
	for _, name := range []string{"missing helper", "missing domain", "unknown participant"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			switch name {
			case "missing helper":
				step := &m.plan.Steps[4]
				attempt := m.children[step.Grant.Handle.TxID]
				attempt.Scope.Expected = attempt.Scope.Expected[:1]
				step.Grant.ScopeDigest = ScopeDigest(attempt.Scope)
				m.children[step.Grant.Handle.TxID] = attempt
			case "missing domain":
				for i := range m.plan.Steps {
					step := &m.plan.Steps[i]
					if !isCut(*step) {
						continue
					}
					attempt := m.children[step.Grant.Handle.TxID]
					attempt.Scope.Domains = attempt.Scope.Domains[1:]
					step.Grant.ScopeDigest = ScopeDigest(attempt.Scope)
					m.children[step.Grant.Handle.TxID] = attempt
				}
			case "unknown participant":
				delete(m.children, m.plan.Steps[4].Grant.Handle.TxID)
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if len(m.starts) != 0 || len(m.record.Actions) != 0 || !m.holder {
				t.Fatal("incomplete original scopes dispatched an effect")
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / §4.2 / §11.2 P11
func TestChildDispatchRequiresFreshSourceProof(t *testing.T) {
	for _, name := range []string{"unknown source", "wrong source sha", "reverse source", "different current attempt", "missing source census"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			m.queryHook = func(step upgradetx.Step, v *upgradetx.View) error {
				if step.Ordinal != 1 || v.Historical.Forward.Phase != upgradetx.PhasePrepared {
					return nil
				}
				switch name {
				case "unknown source":
					v.Observation.OwnerKnown = false
				case "wrong source sha":
					v.Observation.RunningSHA = testSHA(999)
				case "reverse source":
					v.Observation.Current.Direction = upgradetx.DirectionReverse
					v.Historical.Forward.Direction = upgradetx.DirectionReverse
				case "different current attempt":
					v.Observation.Current.Handle.CutID = testID(999)
				case "missing source census":
					v.Observation.Census.Known = false
				}
				return nil
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if len(m.starts) != 0 || len(m.record.Actions) != 0 {
				t.Fatal("unproved source permitted a child effect")
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / §3.2 / §11.2 P11
func TestDriverDoesNotResendUnacceptedStart(t *testing.T) {
	m, p := newRollModel(t)
	m.plan.Deadline = time.Now().Add(upgradebudget.RequestLatency).UnixNano()
	m.record.Grant.ExpiresAt = m.plan.Deadline
	for i := range m.plan.Steps {
		m.plan.Steps[i].Grant.ExpiresAt = m.plan.Deadline
	}
	m.startHook = func(step upgradetx.Step) error {
		attempt := m.children[step.Grant.Handle.TxID]
		attempt.Phase = upgradetx.PhasePrepared
		attempt.CutStage = upgradetx.CutNone
		attempt.Outcome = upgradetx.OutcomePending
		m.children[step.Grant.Handle.TxID] = attempt
		return nil
	}
	_, err := runModel(t, m, p)
	requireUnsettled(t, err)
	if m.startCalls != 1 || !m.holder || slices.Contains(m.log, "start:2") {
		t.Fatal("unaccepted Start was automatically resent")
	}
}

// origin: hot-update S8 plan §6.3 / §11.2 P11 keeper five rounds
func TestReservationKeeperJoinsInflightRenewal(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseKeeperJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "inflight parent renewal", beforeG)
	testharness.AssertNoFDLeak(t, "inflight parent renewal", beforeFD, 0)
}

func exerciseKeeperJoin(t *testing.T) {
	t.Helper()
	m, _ := newRollModel(t)
	m.interval = time.Millisecond
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	m.renewHook = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		close(exited)
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	driveCtx, fail := context.WithCancelCause(ctx)
	defer fail(nil)
	stop := keepReservation(driveCtx, fail, &rollReservation{model: m}, m.plan.Parent)
	select {
	case <-entered:
	case <-ctx.Done():
		stop()
		t.Fatal("renewal was not exercised")
	}
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	<-canceled
	// The renewal is deliberately held after acknowledging cancellation. This
	// timer observes a forbidden early return, not a scheduler sleep used to
	// establish the lease state; all state transitions use channel handshakes.
	timer := time.NewTimer(upgradebudget.ReplyDrain)
	early := false
	select {
	case <-stopped:
		early = true
	case <-timer.C:
	}
	timer.Stop()
	close(release)
	<-exited
	<-stopped
	if early {
		t.Fatal("keeper Stop returned before the inflight renewal exited")
	}
}

// origin: hot-update S8 plan §4.2 / §8.3 M05,M06,M07,M15
func TestDriverRequiresFreshIndependentCompletion(t *testing.T) {
	mutants := map[string]func(*upgradetx.View){
		"historical nonce":             func(v *upgradetx.View) { v.Observation.Nonce = testID(999) },
		"running sha only on disk":     func(v *upgradetx.View) { v.Observation.RunningSHA = v.Historical.Grant.SourceSHA },
		"missing control":              func(v *upgradetx.View) { v.Observation.Current.Control.Known = false },
		"missing resources":            func(v *upgradetx.View) { v.Observation.Current.Resources.Complete = false },
		"missing released":             func(v *upgradetx.View) { v.Observation.Current.Released.Known = false },
		"wrong cut":                    func(v *upgradetx.View) { v.Observation.Current.Released.Stamp.Attempt.CutID = testID(999) },
		"wrong producer":               func(v *upgradetx.View) { v.Observation.Current.Released.Stamp.Producer = testOwner(99) },
		"future receipt revision":      func(v *upgradetx.View) { v.Observation.Current.Released.Stamp.Attempt.Revision++ },
		"reverse receipt":              func(v *upgradetx.View) { v.Observation.Current.Direction = upgradetx.DirectionReverse },
		"new admission":                func(v *upgradetx.View) { v.Observation.Census.Admitting = 1 },
		"missing original":             func(v *upgradetx.View) { v.Observation.Census.Resources = nil },
		"unknown empty":                func(v *upgradetx.View) { v.Observation.Census.Known = false; v.Observation.Census.Resources = nil },
		"stale sequence":               func(v *upgradetx.View) { v.Observation.Census.Stamp.Sequence++ },
		"control probe from old query": func(v *upgradetx.View) { v.Observation.Current.Control.Stamp.Nonce = testID(999) },
	}
	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			m.queryHook = func(step upgradetx.Step, v *upgradetx.View) error {
				if step.Ordinal == 1 && v.Historical.Forward.Phase == upgradetx.PhaseCompleted {
					mutate(v)
				}
				return nil
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if !m.holder || slices.Contains(m.log, "start:2") || slices.Contains(m.log, "release:4") {
				t.Fatal("incomplete proof advanced the parent")
			}
		})
	}
}

// origin: hot-update S8 plan §7.1 / §11.2 P11
func TestAcceptedParentFailureNeverSuggestsRestart(t *testing.T) {
	m, p := newRollModel(t)
	m.startHook = func(upgradetx.Step) error {
		return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "child refused after dispatch"}
	}
	_, err := runModel(t, m, p)
	var transactionErr *upgradetx.Error
	if !errors.As(err, &transactionErr) || !transactionErr.Accepted || transactionErr.Code != upgradetx.CodeUnsettled {
		t.Fatalf("accepted parent returned retryable refusal: %v", err)
	}
	if !m.holder || m.startCalls != 1 {
		t.Fatal("accepted failure discarded the original execution authority")
	}
}

// origin: hot-update S8 plan §3.2 / §5.2 / §8.3 M16
func TestChildDispatchRequiresOriginalArtifact(t *testing.T) {
	for _, name := range []string{"other grant same sha", "wrong manifest", "wrong source", "missing stage path"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			m.artifactHook = func(r *upgradeartifact.Receipt) {
				switch name {
				case "other grant same sha":
					r.Grant.Handle.TxID = testID(900)
				case "wrong manifest":
					r.ManifestDigest = testSHA(900)
				case "wrong source":
					r.SourceSHA = testSHA(900)
				case "missing stage path":
					r.SourcePath = ""
				}
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if len(m.starts) != 0 || len(m.record.Actions) != 0 || !m.holder {
				t.Fatal("unbound artifact authorized an effect")
			}
		})
	}
}

// origin: hot-update S8 plan §5.1 / §11.2 P11
func TestParentCompletionRequiresIndependentProof(t *testing.T) {
	for _, name := range []string{"missing parent release", "unknown parent", "wrong parent scope"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			m.parentHook = func(o *upgradetx.Observation) {
				switch name {
				case "missing parent release":
					o.Current.Released.Known = false
				case "unknown parent":
					o.OwnerKnown = false
				case "wrong parent scope":
					o.Current.Scope.Expected = nil
				}
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if slices.Contains(m.log, "complete-parent") {
				t.Fatal("parent was signed from child-only proof")
			}
		})
	}
}

type blockedRollPort struct {
	*rollPort
	entered chan struct{}
	once    sync.Once
}

func (p *blockedRollPort) Query(ctx context.Context, _ upgradetx.Key, _, _ string) (upgradetx.View, error) {
	p.once.Do(func() { close(p.entered) })
	<-ctx.Done()
	return upgradetx.View{}, ctx.Err()
}

// origin: hot-update S8 plan §6.3 / §11.2 P11 keeper five rounds
func TestDriverKeeperStopsOnEveryExit(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseDriverKeeper(t)
	}
	testharness.AssertNoGoroutineLeak(t, "parent reservation keeper", beforeG)
	testharness.AssertNoFDLeak(t, "parent reservation keeper", beforeFD, 0)
}

func exerciseDriverKeeper(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"renewal fails", "caller cancels", "missing renewal budget", "successful driver"} {
		m, p := newRollModel(t)
		if mode == "successful driver" {
			if _, err := runModel(t, m, p); err != nil {
				t.Fatal(err)
			}
			if m.stopCount != 1 {
				t.Fatal("successful keeper was not joined")
			}
			continue
		}
		m.interval = time.Millisecond
		if mode == "missing renewal budget" {
			m.interval = 0
		}
		blocked := &blockedRollPort{rollPort: p, entered: make(chan struct{})}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		if mode == "renewal fails" {
			m.renewHook = func(ctx context.Context) error {
				select {
				case <-blocked.entered:
					return errors.New("expected holder changed")
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		var cancelDone chan struct{}
		if mode == "caller cancels" {
			cancelDone = make(chan struct{})
			go func() {
				defer close(cancelDone)
				select {
				case <-blocked.entered:
					cancel()
				case <-ctx.Done():
				}
			}()
		}
		_, err := Run(ctx, m, blocked, m.plan)
		cancel()
		if cancelDone != nil {
			<-cancelDone
		}
		requireUnsettled(t, err)
		if mode == "renewal fails" && !slices.Contains(m.log, "renew") {
			t.Fatal("keeper never attempted renewal")
		}
		if m.stopCount != 1 || !m.holder || slices.Contains(m.log, "release:8") {
			t.Fatal("keeper failed to stop or released pending parent")
		}
	}
}
