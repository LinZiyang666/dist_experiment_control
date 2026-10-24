package agentupgrade

import (
	"context"
	"errors"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func reverseInputs(f *engineFixture, view upgradetx.View) (upgradetx.Fence, upgradetx.ReverseRequest) {
	a := current(view.Historical)
	handle := a.Handle
	handle.Revision = view.Historical.Revision
	fence := upgradetx.Fence{Current: handle, Owner: f.owner, RunningSHA: f.running, PreviousSHA: view.Historical.RecordedPreviousSHA, Incarnation: strings.Repeat("9", 64)}
	req := upgradetx.ReverseRequest{ClientNonce: newID(), ActorFP: f.grant.ActorFP, HistoricalTxID: handle.TxID, TargetSHA: fence.PreviousSHA, Scope: f.scope, ExpiresAt: time.Now().Add(120 * time.Second).UnixNano()}
	return fence, req
}

func importedFixture(t *testing.T, stage upgradetx.CutStage) *engineFixture {
	t.Helper()
	f := fixture(t)
	r, err := f.store.Start(f.grant, upgradetx.Fence{})
	if err != nil {
		t.Fatal(err)
	}
	r.Forward.Phase, r.Forward.CutStage = upgradetx.PhaseActivating, stage
	f.store.records[f.grant.Handle.TxID] = r
	f.running, f.effective = f.grant.TargetSHA, f.grant.TargetSHA
	return f
}

// origin: hot-update S8 plan §8.1 R1/R2 / P09
func TestAgentRollbackExportsCurrentCut(t *testing.T) {
	f := fixture(t)
	before := startFixture(t, f)
	original := before.Historical
	// This original work appeared after forward completion. The reverse
	// sealed scope must include it; a pristine input cannot carry its ACKs.
	f.scope.Expected = append(append([]upgradetx.Resource(nil), f.scope.Expected...), upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "new-child-after-forward", Origin: "new", Owner: f.owner, Generation: 2, State: "active", Custody: "current"})
	f.sequence++
	fence, request := reverseInputs(f, before)
	f.activation = func(cut upgradeport.Cut) error {
		if string(cut.Descriptor) != "current-latest-ACK-and-new-child" || len(cut.Scope.Expected) != 2 {
			t.Fatal("reverse used the pristine forward snapshot")
		}
		return nil
	}
	view, err := f.endpoint.Rollback(testContext(t), fence, request)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	a := current(view.Historical)
	if a.Direction != upgradetx.DirectionReverse || a.Phase != upgradetx.PhaseCompleted || a.TargetSHA != original.RecordedPreviousSHA || a.SourceSHA != original.Forward.TargetSHA || a.Handle.CutID == original.Forward.Handle.CutID || a.Handle.Generation != original.Forward.Handle.Generation+1 {
		t.Fatalf("reverse identity was reused: %+v", a)
	}
	if !reflect.DeepEqual(original, f.store.records[original.Grant.Handle.TxID]) {
		t.Fatal("reverse rewrote historical forward completion")
	}
	if strings.Count(strings.Join(f.calls, ","), "freeze") != 1 || strings.Count(strings.Join(f.calls, ","), "export-current") != 1 {
		t.Fatalf("rollback exports = %v", f.calls)
	}
}

// origin: hot-update S8 plan §8.1 / M07 / P09
func TestAgentReverseRejectsForwardReceipts(t *testing.T) {
	f := fixture(t)
	before := startFixture(t, f)
	fence, request := reverseInputs(f, before)
	f.sealed = func(r *upgradetx.Receipt) {
		r.Stamp.Attempt = before.Historical.Forward.Handle
		r.Census.Stamp = r.Stamp
	}
	_, err := f.endpoint.Rollback(testContext(t), fence, request)
	requireError(t, err, upgradetx.CodeUnsettled, true)
	if current(f.store.records[f.store.currentID]).Phase == upgradetx.PhaseCompleted {
		t.Fatal("forward receipt completed reverse")
	}
	if !reflect.DeepEqual(before.Historical, f.store.records[f.grant.Handle.TxID]) {
		t.Fatal("failed reverse rewrote forward history")
	}
}

// origin: hot-update S8 plan §8.1 / M01 / P09
func TestAgentReverseRequiresRetirementBeforeThirdGeneration(t *testing.T) {
	f := fixture(t)
	before := startFixture(t, f)
	fence, request := reverseInputs(f, before)
	f.retirement = func(r *upgradetx.Receipt) { r.Census.OldOwners = []upgradetx.Owner{f.owner} }
	_, err := f.endpoint.Rollback(testContext(t), fence, request)
	requireError(t, err, upgradetx.CodeUnsettled, true)
	if strings.Count(strings.Join(f.calls, ","), "activate") != 1 || f.running != f.grant.TargetSHA {
		t.Fatal("third generation activated while a retained owner was live")
	}
}

// origin: hot-update S8 plan §3.3 / §8.1 / M11 / M12
func TestLateWriterCannotClaimNextTransaction(t *testing.T) {
	f := fixture(t)
	first := startFixture(t, f)
	fence, request := reverseInputs(f, first)
	secondGrant := f.grant
	secondGrant.Handle.TxID, secondGrant.Handle.CutID, secondGrant.Handle.Generation = newID(), newID(), 3
	secondGrant.SourceSHA, secondGrant.TargetSHA = f.grant.TargetSHA, strings.Repeat("d", 64)
	f.store.grants[secondGrant.Handle.TxID] = secondGrant
	secondRecord, err := f.store.Start(secondGrant, upgradetx.Fence{})
	if err != nil {
		t.Fatal(err)
	}
	// B is accepted and serving while its migration is pending. A's old
	// watchdog must not read Current afresh and cancel or reverse B.
	secondRecord.Forward.Phase, secondRecord.Forward.CutStage = upgradetx.PhaseMigrating, upgradetx.CutActive
	f.store.records[secondGrant.Handle.TxID] = secondRecord
	f.running, f.effective = secondGrant.TargetSHA, secondGrant.TargetSHA
	events, calls := len(f.store.events), len(f.calls)
	_, err = Watchdog(testContext(t), f.endpoint, fence, time.Now().Add(-time.Second).UnixNano(), request)
	if len(f.store.events) != events || len(f.calls) != calls || f.store.reverses != 0 || f.store.currentID != secondGrant.Handle.TxID || !reflect.DeepEqual(secondRecord, f.store.records[secondGrant.Handle.TxID]) {
		t.Fatal("late watchdog claimed the next transaction")
	}
	requireError(t, err, upgradetx.CodeUnsettled, true)
	_, err = f.endpoint.Rollback(testContext(t), fence, request)
	if len(f.store.events) != events || len(f.calls) != calls || f.store.reverses != 0 || !reflect.DeepEqual(secondRecord, f.store.records[secondGrant.Handle.TxID]) {
		t.Fatal("late writer claimed the next transaction")
	}
	requireError(t, err, upgradetx.CodeRejected, true)
}

// origin: hot-update S8 plan §3.3 / §8.1 / P09
func TestAgentWatchdogRecoversOnlyCapturedPendingCut(t *testing.T) {
	f := importedFixture(t, upgradetx.CutPartialActive)
	view, err := f.store.Load(f.grant.Handle.Key, f.grant.Handle.TxID)
	if err != nil {
		t.Fatal(err)
	}
	fence, request := reverseInputs(f, view)
	view, err = Watchdog(testContext(t), f.endpoint, fence, time.Now().Add(-time.Second).UnixNano(), request)
	if err != nil || f.store.reverses != 1 || current(view.Historical).Direction != upgradetx.DirectionReverse || current(view.Historical).Phase != upgradetx.PhaseCompleted {
		t.Fatalf("watchdog recovery: %+v, %v", view.Historical.Forward, err)
	}
}

// origin: hot-update S8 plan §3.2 / §4.2 / P09
func TestAgentBootResumesEvidenceWithoutRedispatch(t *testing.T) {
	for _, stage := range []upgradetx.CutStage{upgradetx.CutPartialActive, upgradetx.CutActive} {
		t.Run(string(stage), func(t *testing.T) {
			f := importedFixture(t, stage)
			// Activation intent advances the journal after the cut is sealed.
			record := f.store.records[f.grant.Handle.TxID]
			record.Revision, record.Forward.Handle.Revision = 3, 3
			f.store.records[f.grant.Handle.TxID] = record
			view, err := Boot(testContext(t), f.endpoint, f.grant.Handle)
			if err != nil || current(view.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("boot resume: %+v, %v", view.Historical.Forward, err)
			}
			if f.store.starts != 1 || strings.Join(f.calls, ",") != "retire" {
				t.Fatalf("boot replayed an activation: %v", f.calls)
			}
		})
	}
	for _, problem := range []string{"source_image", "pristine", "unknown_owner", "stale_handle", "future_revision"} {
		t.Run(problem, func(t *testing.T) {
			f := importedFixture(t, upgradetx.CutPartialActive)
			expected := f.grant.Handle
			switch problem {
			case "source_image":
				f.running = f.grant.SourceSHA
			case "pristine":
				r := f.store.records[expected.TxID]
				r.Forward.CutStage = upgradetx.CutPristine
				f.store.records[expected.TxID] = r
			case "unknown_owner":
				f.observation = func(o *upgradetx.Observation) { o.OwnerKnown = false }
			case "stale_handle":
				expected.CutID = newID()
			case "future_revision":
				expected.Revision++
			}
			_, err := Boot(testContext(t), f.endpoint, expected)
			if err == nil {
				t.Fatal("boot invented current activation proof")
			}
			if current(f.store.records[f.grant.Handle.TxID]).Phase == upgradetx.PhaseCompleted || len(f.calls) != 0 {
				t.Fatal("invalid boot dispatched or completed")
			}
		})
	}
}

// origin: hot-update S8 plan §8.1 R0/R1 / P09
func TestAgentFailureUsesPristineThawOrCurrentReverse(t *testing.T) {
	t.Run("pristine", func(t *testing.T) {
		f := fixture(t)
		f.freezeError = errors.New("freeze failed")
		view, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
		requireError(t, err, upgradetx.CodeUnsettled, true)
		if current(view.Historical).Phase != upgradetx.PhaseCancelled || f.store.reverses != 0 || !strings.Contains(strings.Join(f.calls, ","), "thaw") || f.running != f.grant.SourceSHA {
			t.Fatal("pristine failure did not preserve and thaw source")
		}
	})
	t.Run("partial_active", func(t *testing.T) {
		f := fixture(t)
		failed := false
		f.activation = func(cut upgradeport.Cut) error {
			if !failed {
				failed = true
				return errors.New("ACTIVE reply lost")
			}
			if string(cut.Descriptor) != "current-latest-ACK-and-new-child" {
				t.Fatal("partial ACTIVE restored pristine input")
			}
			return nil
		}
		view, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
		requireError(t, err, upgradetx.CodeRejected, true)
		if current(view.Historical).Direction != upgradetx.DirectionReverse || current(view.Historical).Phase != upgradetx.PhaseCompleted || f.running != f.grant.SourceSHA {
			t.Fatal("known partial ACTIVE did not complete a current reverse")
		}
	})
	t.Run("unknown_current", func(t *testing.T) {
		f := fixture(t)
		f.activation = func(cut upgradeport.Cut) error {
			f.observation = func(o *upgradetx.Observation) { o.OwnerKnown = false }
			return errors.New("current owner lost")
		}
		view, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
		requireError(t, err, upgradetx.CodeUnsettled, true)
		if current(view.Historical).Phase != upgradetx.PhaseRecoveryRequired || f.store.reverses != 0 || strings.Contains(strings.Join(f.calls, ","), "thaw") {
			t.Fatal("unknown owner authorized automatic recovery")
		}
	})
}

// origin: hot-update S8 plan §8.1 / P09
func TestAgentReverseCancelPreservesServingForward(t *testing.T) {
	f := fixture(t)
	before := startFixture(t, f)
	fence, request := reverseInputs(f, before)
	g, err := f.store.PrepareReverse(fence, request)
	if err != nil {
		t.Fatal(err)
	}
	record, err := f.store.Start(g, fence)
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.endpoint.Cancel(testContext(t), current(record).Handle)
	if err != nil {
		t.Fatal(err)
	}
	if current(view.Historical).Phase != upgradetx.PhaseCancelled || current(view.Historical).Outcome != upgradetx.OutcomeCancelled || f.running != f.grant.TargetSHA {
		t.Fatal("cancelled reverse claimed rolled_back or changed the serving image")
	}
	if !reflect.DeepEqual(before.Historical, f.store.records[f.grant.Handle.TxID]) {
		t.Fatal("reverse cancellation rewrote original completion")
	}
}

// origin: hot-update S8 plan §8.1 / §7.1 / P09
func TestAgentArchiveRequiresIndependentDeathAndCustody(t *testing.T) {
	cases := []struct {
		name   string
		inject func(*upgradetx.DeathProof)
	}{
		{"not_reaped", func(d *upgradetx.DeathProof) { d.Reaped = false }},
		{"fd_retained", func(d *upgradetx.DeathProof) { d.NoOwnedFDs = false }},
		{"unknown_custody", func(d *upgradetx.DeathProof) { d.Custody.Known = false }},
		{"retained_owner", func(d *upgradetx.DeathProof) { d.Custody.OldOwners = []upgradetx.Owner{d.Owner} }},
		{"wrong_cut", func(d *upgradetx.DeathProof) { d.Stamp.Attempt.CutID = newID(); d.Custody.Stamp = d.Stamp }},
		{"missing_resource", func(d *upgradetx.DeathProof) { d.Custody.Resources = nil }},
		{"dead_owner_active", func(d *upgradetx.DeathProof) {
			d.Custody.Resources[0].State, d.Custody.Resources[0].Custody = "active", "current"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := importedFixture(t, upgradetx.CutActive)
			f.death = tc.inject
			_, err := f.endpoint.Archive(testContext(t), f.grant.Handle)
			requireError(t, err, upgradetx.CodeUnsettled, true)
			if f.store.archives != 0 {
				t.Fatal("archive inferred death from incomplete evidence")
			}
		})
	}
	f := importedFixture(t, upgradetx.CutActive)
	view, err := f.endpoint.Archive(testContext(t), f.grant.Handle)
	if err != nil || f.store.archives != 1 || current(view.Historical).Phase != upgradetx.PhaseAbandoned {
		t.Fatalf("verified archive: %+v, %v", view, err)
	}
}

type runtimeWithoutDeath struct{ upgradeport.RuntimePort }

// origin: hot-update S8 plan §3.1 / §4.1 / §8.1 / P09
func TestAgentTerminalHistoryAndUnknownDeathRemainImmutable(t *testing.T) {
	f := fixture(t)
	original := startFixture(t, f).Historical
	f.observation = func(o *upgradetx.Observation) {
		o.OwnerKnown = false
		o.Census.Known = false
		o.Census.Complete = false
	}
	view, err := Boot(testContext(t), f.endpoint, current(original).Handle)
	if err != nil || view.Code != upgradetx.CodeUnsettled || !reflect.DeepEqual(view.Historical, original) {
		t.Fatal("cold boot rewrote a terminal execution")
	}
	_, err = f.endpoint.Archive(testContext(t), current(original).Handle)
	requireError(t, err, upgradetx.CodeRejected, true)
	if f.store.archives != 0 {
		t.Fatal("completed history was abandoned")
	}
	f = importedFixture(t, upgradetx.CutActive)
	f.endpoint = New(f.store, runtimeWithoutDeath{f.runtime}, f.registry)
	_, err = f.endpoint.Archive(testContext(t), f.grant.Handle)
	requireError(t, err, upgradetx.CodeUnsettled, true)
	if f.store.archives != 0 {
		t.Fatal("ordinary observation was substituted for death proof")
	}
}

// origin: hot-update S8 plan §11.2 P09 / L21
func TestAgentEngineLifecycleJoinsWatchdogAndPreservesFDs(t *testing.T) {
	// Initialize the runtime poller before measuring descriptors; its epoll
	// and eventfd are process-wide and do not belong to an engine exercise.
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = read.Close()
	_ = write.Close()
	goroutines, descriptors := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentEngine(t)
	}
	testharness.AssertNoGoroutineLeak(t, "agent transaction engine", goroutines)
	testharness.AssertNoFDLeak(t, "agent transaction engine", descriptors, 0)
}

func exerciseAgentEngine(t *testing.T) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := write.Close(); err != nil {
			t.Error(err)
		}
	}()
	f := fixture(t)
	f.file = write
	// Competing delivery of the same accepted Start must join the original
	// transaction rather than reenter its effect pipeline.
	type result struct {
		view upgradetx.View
		err  error
	}
	results := make(chan result, 6)
	ctx := testContext(t)
	for worker := 0; worker < 6; worker++ {
		go func() {
			view, startErr := f.endpoint.Start(ctx, f.grant, f.artifact)
			results <- result{view, startErr}
		}()
	}
	var view upgradetx.View
	for worker := 0; worker < 6; worker++ {
		select {
		case started := <-results:
			if started.err != nil {
				t.Fatal(started.err)
			}
			view = started.view
		case <-ctx.Done():
			t.Fatal("concurrent Start callers did not join")
		}
	}
	if f.store.starts != 1 || strings.Count(strings.Join(f.calls, ","), "activate") != 1 {
		t.Fatal("concurrent retries repeated accepted effects")
	}
	fence, request := reverseInputs(f, view)
	if _, err = f.endpoint.Rollback(testContext(t), fence, request); err != nil {
		t.Fatal(err)
	}
	if n, writeErr := write.Write([]byte("original-OFD")); writeErr != nil || n != 12 {
		t.Fatalf("engine closed runtime-owned FD: n=%d, %v", n, writeErr)
	}
	ctx, cancel := context.WithCancel(testContext(t))
	done := make(chan error, 1)
	go func() {
		_, waitErr := Watchdog(ctx, f.endpoint, fence, time.Now().Add(time.Minute).UnixNano(), request)
		done <- waitErr
	}()
	cancel()
	select {
	case waitErr := <-done:
		if !errors.Is(waitErr, context.Canceled) {
			t.Fatalf("watchdog cancellation: %v", waitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("watchdog did not join after cancellation")
	}
}
