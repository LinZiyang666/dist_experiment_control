//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type transactionDriverDispatch struct {
	Slot               uint32
	Handle             upgradetx.Handle
	ActionID           string
	IntentNS, EffectNS int64
	Effects            int
}

type transactionDriverEvidence struct {
	Seal                                                                                               transactionOriginalSeal
	Round                                                                                              int
	RunID, Role                                                                                        string
	Original, Reopened, Resumed                                                                        upgradetx.Plan
	PlanDurableNS, DriverDeathNS, RestartNS, AllChildrenCompletedNS, HostReleasedNS, ClusterReleasedNS int64
	DriverBefore, DriverAfter                                                                          upgradetx.Owner
	CLI                                                                                                transactionP23CLI
	ReservationBefore, ReservationAfter                                                                string
	Participants                                                                                       []upgradetx.Key
	Dispatches                                                                                         []transactionDriverDispatch
	Queried                                                                                            []upgradetx.Handle
	Children                                                                                           []upgradetx.View
	ExpiredStartCode                                                                                   string
	ExpiredStartAccepted                                                                               bool
	ExpiredStartEffects                                                                                int
}

// The driver is the only process allowed to die in this exercise. Child IDs,
// host reservation and the managed snapshot were sealed before its first effect.
// Release ordering is checked independently of the child's local completion.
// origin: hot-update S8 plan §3.1 / §5.1 / M09/M10
func judgeTransactionDriver(e transactionDriverEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	p := e.Original
	if !reflect.DeepEqual(p, e.Reopened) || !reflect.DeepEqual(p, e.Resumed) || len(p.Scope.Participants) < 2 || !reflect.DeepEqual(p.Scope.Participants, e.Participants) {
		return fmt.Errorf("M09: driver restart replanned or omitted a participant")
	}
	if err := judgeTransactionP23CLI(e.CLI, "upgrade", p.Parent.TxID, 0); err != nil {
		// cluster upgrade uses --resume, unlike component operations' --tx-id.
		if !slices.Contains(e.CLI.Argv, "cluster") || !slices.Contains(e.CLI.Argv, "upgrade") || !slices.Contains(e.CLI.Argv, "--resume") || !slices.Contains(e.CLI.Argv, p.Parent.TxID) || !transactionOwnerKnown(e.CLI.Owner) || !transactionHex(e.CLI.BinarySHA, 32) || e.CLI.RC != 0 || e.CLI.BeginNS < e.RestartNS || e.CLI.EndNS <= e.CLI.BeginNS {
			return fmt.Errorf("missing actual resumed cluster CLI receipt")
		}
	}
	if e.PlanDurableNS <= 0 || e.DriverDeathNS <= e.PlanDurableNS || e.RestartNS <= e.DriverDeathNS || !transactionOwnerKnown(e.DriverBefore) || !transactionOwnerKnown(e.DriverAfter) || e.DriverBefore == e.DriverAfter {
		return fmt.Errorf("driver was not independently killed and restarted")
	}
	if e.CLI.View.Historical.Forward.Phase != upgradetx.PhaseCompleted || !transactionSameCut(e.CLI.View.Historical.Forward.Handle, p.Parent) {
		return fmt.Errorf("resumed CLI did not complete the original parent")
	}
	if e.ReservationBefore == "" || e.ReservationBefore != e.ReservationAfter || e.HostReleasedNS <= e.AllChildrenCompletedNS || e.ClusterReleasedNS <= e.HostReleasedNS {
		return fmt.Errorf("driver death released or deadlocked the component/host/cluster hierarchy")
	}
	cut := map[uint32]upgradetx.Step{}
	covered := map[upgradetx.Key]bool{}
	for _, step := range p.Steps {
		if step.Kind != upgradetx.StepAgentCut && step.Kind != upgradetx.StepBrokerCut {
			continue
		}
		if cut[step.Ordinal].Grant.Handle.TxID != "" || !transactionHex(step.Grant.Handle.TxID, 16) || step.Grant.Handle.Key != step.Participant {
			return fmt.Errorf("invalid fixed child grant")
		}
		cut[step.Ordinal], covered[step.Participant] = step, true
	}
	for _, participant := range p.Scope.Participants {
		if !covered[participant] {
			return fmt.Errorf("M09: sealed plan omits a managed participant")
		}
	}
	if len(cut) != len(e.Dispatches) || len(cut) != len(e.Children) {
		return fmt.Errorf("M09: original children were omitted or dispatched twice")
	}
	seen := map[uint32]bool{}
	lostReply := false
	for _, dispatch := range e.Dispatches {
		step, ok := cut[dispatch.Slot]
		if !ok || seen[dispatch.Slot] || !transactionSameCut(dispatch.Handle, step.Grant.Handle) || dispatch.ActionID == "" || dispatch.IntentNS < e.PlanDurableNS || dispatch.EffectNS <= dispatch.IntentNS || dispatch.Effects != 1 {
			return fmt.Errorf("M09: child dispatch changed identity or repeated its effect")
		}
		seen[dispatch.Slot] = true
		if dispatch.EffectNS < e.DriverDeathNS {
			lostReply = true
			found := false
			for _, queried := range e.Queried {
				found = found || transactionSameCut(queried, dispatch.Handle)
			}
			if !found {
				return fmt.Errorf("accepted child was redispatched instead of queried after driver death")
			}
		}
	}
	if !lostReply {
		return fmt.Errorf("driver died without a dispatched accepted child")
	}
	seenChildren := map[string]bool{}
	for _, child := range e.Children {
		a := child.Observation.Current
		found := false
		for _, step := range cut {
			found = found || transactionSameCut(a.Handle, step.Grant.Handle)
		}
		if !found || seenChildren[a.Handle.TxID] || child.Observation.At >= e.HostReleasedNS {
			return fmt.Errorf("host released before fresh original child completion")
		}
		seenChildren[a.Handle.TxID] = true
		if err := upgradetx.Completed(a, child.Observation); err != nil {
			return err
		}
	}
	if e.ExpiredStartAccepted || e.ExpiredStartEffects != 0 || e.ExpiredStartCode != upgradetx.CodeGrantUnknown && e.ExpiredStartCode != upgradetx.CodeGrantExpired {
		return fmt.Errorf("M10: obsolete Start after reopen/GC reconstructed execution")
	}
	return nil
}

// origin: hot-update S8 plan §9.1 / P23 / M09
func TestDriverResumesOriginalPlan(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionDriverResume(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "driver resume receipts x5", g)
	testharness.AssertNoFDLeak(t, "driver resume receipts x5", fd, 0)
}

func exerciseTransactionDriverResume(t *testing.T, round int) {
	t.Helper()
	for _, role := range []string{"single", "sole-voter", "N3"} {
		name := fmt.Sprintf("p23/round-%d/driver/%s", round, role)
		var e transactionDriverEvidence
		if err := readTransactionEvidence(name, &e); err != nil {
			t.Fatalf("NOT_RUN pending P15/P19: %s: %v", name, err)
		}
		if e.Round != round || e.Seal.Round != round || e.Role != role || e.RunID != name || e.Seal.RunID != name {
			t.Fatal("driver receipt was reused across roles or rounds")
		}
		if err := judgeTransactionDriver(e); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

type transactionLateWriterEvidence struct {
	Seal                       transactionOriginalSeal
	Round                      int
	Role, Entry, RunID         string
	Captured                   upgradetx.Handle
	CompletedA                 upgradetx.Record
	BeforeB, AfterB, ReopenedB upgradetx.Record
	StartedBNS, CallbackNS     int64
	Accepted                   bool
	Effects                    int
	HistoricalRollbackCode     string
}

var transactionLateWriterEntries = []string{"boot", "install", "report", "register", "watchdog", "rollback", "unlink", "reexec", "close_window", "archive", "gc"}

// origin: hot-update S8 plan §3.3 / M11/M12
func judgeTransactionLateWriter(e transactionLateWriterEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	if !slices.Contains(transactionLateWriterEntries, e.Entry) || e.CompletedA.Forward.Phase != upgradetx.PhaseCompleted || !transactionSameCut(e.Captured, e.CompletedA.Forward.Handle) || e.Captured.Revision != e.CompletedA.Revision || e.BeforeB.Grant.Handle.TxID == e.Captured.TxID || e.BeforeB.Grant.Handle.TxID == "" || e.StartedBNS <= 0 || e.CallbackNS <= e.StartedBNS {
		return fmt.Errorf("late writer did not capture completed A before B became Current")
	}
	if e.Accepted || e.Effects != 0 || !reflect.DeepEqual(e.BeforeB, e.AfterB) || !reflect.DeepEqual(e.BeforeB, e.ReopenedB) {
		return fmt.Errorf("M11: old writer/watchdog/unlink claimed the next transaction")
	}
	if e.Entry == "rollback" && e.HistoricalRollbackCode != upgradetx.CodeWindowClosed && e.HistoricalRollbackCode != upgradetx.CodeRejected {
		return fmt.Errorf("M12: historical A rollback was accepted against B")
	}
	return nil
}

// Each callback is exercised through its real integration entry, including
// void report/unlink paths. Durable reopen catches deletion hidden by a cache.
// origin: hot-update S8 plan §3.3 / §9.1 / P23
func TestLateWriterCannotClaimNextTransaction(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionLateWriter(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "late writer receipts x5", g)
	testharness.AssertNoFDLeak(t, "late writer receipts x5", fd, 0)
}

func exerciseTransactionLateWriter(t *testing.T, round int) {
	t.Helper()
	for _, role := range []string{"agent", "broker"} {
		for _, entry := range transactionLateWriterEntries {
			name := fmt.Sprintf("p23/round-%d/late/%s-%s", round, role, entry)
			var e transactionLateWriterEvidence
			if err := readTransactionEvidence(name, &e); err != nil {
				t.Fatalf("NOT_RUN pending P14/P15/P17: %s: %v", name, err)
			}
			if e.Round != round || e.Seal.Round != round || e.Role != role || e.Entry != entry || e.RunID != name || e.Seal.RunID != name {
				t.Fatal("late writer receipt was reused")
			}
			if err := judgeTransactionLateWriter(e); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
	}
}

func transactionDriverOracleControl() transactionDriverEvidence {
	seal, _ := transactionOracleControl()
	first := transactionRollbackOracleControl("R2").After
	first.Observation.At = 120
	second := transactionRollbackOracleControl("R2").After
	second.Observation.Current.Handle.TxID = strings.Repeat("9", 32)
	second.Observation.Current.Handle.Key.ConfiguredNID = "second-agent"
	second.Observation.Census.Stamp.Attempt = second.Observation.Current.Handle
	for _, receipt := range []*upgradetx.Receipt{&second.Observation.Current.Control, &second.Observation.Current.Resources, &second.Observation.Current.Released} {
		receipt.Stamp.Attempt = second.Observation.Current.Handle
		receipt.Census.Stamp = receipt.Stamp
	}
	second.Observation.Current.Scope.Participants = []upgradetx.Key{second.Observation.Current.Handle.Key}
	// The second child has a distinct participant, so its scope and expected
	// commitments must be recomputed before it can be a completed control.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-A F1; hot-update S8 plan §4.2
	second = transactionResealOracleView(second)
	p := upgradetx.Plan{Parent: seal.Attempt.Handle, Scope: seal.Original}
	p.Scope.Participants = []upgradetx.Key{first.Observation.Current.Handle.Key, second.Observation.Current.Handle.Key}
	for i, v := range []upgradetx.View{first, second} {
		p.Steps = append(p.Steps, upgradetx.Step{Ordinal: uint32(i + 1), Participant: v.Observation.Current.Handle.Key, Grant: upgradetx.Grant{Handle: v.Observation.Current.Handle}, Kind: upgradetx.StepAgentCut})
	}
	e := transactionDriverEvidence{Seal: seal, Original: p, Reopened: p, Resumed: p, Participants: p.Scope.Participants, PlanDurableNS: 10, DriverDeathNS: 30, RestartNS: 40, AllChildrenCompletedNS: 120, HostReleasedNS: 130, ClusterReleasedNS: 140, DriverBefore: transactionTestOwner(10), DriverAfter: transactionTestOwner(11), ReservationBefore: "original-token", ReservationAfter: "original-token", Children: []upgradetx.View{first, second}, ExpiredStartCode: upgradetx.CodeGrantUnknown}
	e.CLI = transactionP23CLI{Argv: []string{"cluster", "upgrade", "--resume", p.Parent.TxID}, Owner: e.DriverAfter, BinarySHA: seal.BinarySHA, ExecutedPath: seal.ExecutedPath, BeginNS: 40, EndNS: 150}
	e.CLI.View.Historical.Forward = upgradetx.Attempt{Handle: p.Parent, Phase: upgradetx.PhaseCompleted}
	for i, step := range p.Steps {
		e.Dispatches = append(e.Dispatches, transactionDriverDispatch{Slot: step.Ordinal, Handle: step.Grant.Handle, ActionID: fmt.Sprintf("action-%d", i), IntentNS: 11 + int64(i), EffectNS: 20 + int64(i), Effects: 1})
		e.Queried = append(e.Queried, step.Grant.Handle)
	}
	return e
}

// origin: hot-update S8 plan §3.1 / M09/M10
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F1
func TestTransactionDriverRejectsReplanning(t *testing.T) {
	if err := judgeTransactionDriver(transactionDriverOracleControl()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*transactionDriverEvidence){
		"M09-random-child": func(e *transactionDriverEvidence) {
			e.Resumed.Steps = slices.Clone(e.Resumed.Steps)
			e.Resumed.Steps[0].Grant.Handle.TxID = strings.Repeat("a", 32)
		},
		"M09-missing-participant": func(e *transactionDriverEvidence) { e.Participants = e.Participants[:1] },
		"M09-double-dispatch":     func(e *transactionDriverEvidence) { e.Dispatches[0].Effects++ },
		"early-host-release":      func(e *transactionDriverEvidence) { e.HostReleasedNS = e.AllChildrenCompletedNS },
		"M10-GC-rebegin":          func(e *transactionDriverEvidence) { e.ExpiredStartAccepted = true; e.ExpiredStartEffects = 1 },
		"parent-incomplete":       func(e *transactionDriverEvidence) { e.CLI.View.Historical.Forward.Phase = upgradetx.PhaseMigrating },
	} {
		t.Run(name, func(t *testing.T) {
			e := transactionDriverOracleControl()
			mutate(&e)
			if judgeTransactionDriver(e) == nil {
				t.Fatal("driver defect accepted")
			}
		})
	}
}

// origin: hot-update S8 plan §3.3 / M11/M12
func TestTransactionLateWriterRejectsClaim(t *testing.T) {
	seal, p := transactionOracleControl()
	a := upgradetx.Record{Revision: p.Attempt.Handle.Revision, Forward: p.Attempt}
	b := a
	b.Grant.Handle.TxID = strings.Repeat("9", 32)
	for _, entry := range transactionLateWriterEntries {
		e := transactionLateWriterEvidence{Seal: seal, Entry: entry, Captured: a.Forward.Handle, CompletedA: a, BeforeB: b, AfterB: b, ReopenedB: b, StartedBNS: 100, CallbackNS: 110, HistoricalRollbackCode: upgradetx.CodeWindowClosed}
		if err := judgeTransactionLateWriter(e); err != nil {
			t.Fatal(err)
		}
		e.AfterB.Revision++
		if judgeTransactionLateWriter(e) == nil {
			t.Fatal("late callback rewrote B")
		}
		e.AfterB = e.BeforeB
		e.ReopenedB = upgradetx.Record{}
		if judgeTransactionLateWriter(e) == nil {
			t.Fatal("late unlink destroyed the only durable journal")
		}
		if entry == "rollback" {
			e.ReopenedB = e.BeforeB
			e.HistoricalRollbackCode = ""
			if judgeTransactionLateWriter(e) == nil {
				t.Fatal("historical rollback adopted B")
			}
		}
	}
}
