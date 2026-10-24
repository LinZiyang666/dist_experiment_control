//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

var transactionCrashActions = []upgradetx.ActionKind{upgradetx.ActionPublish, upgradetx.ActionSelect, upgradetx.ActionFreeze, upgradetx.ActionDispatch, upgradetx.ActionActivate, upgradetx.ActionControl, upgradetx.ActionMigrate, upgradetx.ActionRetire, upgradetx.ActionReverse, upgradetx.ActionRelease}
var transactionDeathWindows = []string{"before_intent", "intent_before_effect", "effect_before_receipt", "after_receipt"}

type transactionCrashEvidence struct {
	Seal                        transactionOriginalSeal
	SchemaVersion, Round        int
	RunID, Role, Window         string
	Kind                        upgradetx.ActionKind
	Handle                      upgradetx.Handle
	Before, Reopened            upgradetx.Record
	Intent, Result              upgradetx.Action
	IntentDurable               bool
	BeforeEffects, AfterEffects int
	EffectID, RecoveredEffectID string
	CustodyKnown                bool
	Reaped                      bool
	Survivors                   []upgradetx.Owner
	NativeRC                    int
}

// The four death windows have fixed effect cardinalities. Reopening a journal
// is not permission to start an unaccepted action, and an effect without a
// receipt must be queried by its original identity before settlement.
// origin: hot-update S8 plan §3.2 / §8.3 / P25
func judgeTransactionCrash(e transactionCrashEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	if e.SchemaVersion != 1 || e.Handle.TxID == "" || e.Kind == "" || !e.Reaped || len(e.Survivors) != 0 {
		return fmt.Errorf("crash evidence lacks original identity or supervisor reap")
	}
	if e.Before.Key != e.Reopened.Key || e.Before.Grant.Handle != e.Reopened.Grant.Handle || e.Before.Forward.Handle != e.Reopened.Forward.Handle || e.Before.RecordedPreviousSHA != e.Reopened.RecordedPreviousSHA {
		return fmt.Errorf("reopen changed accepted transaction/prev instead of recovering it")
	}
	if e.Window == "before_intent" {
		if e.IntentDurable || e.BeforeEffects != 0 || e.AfterEffects != 0 || e.Intent.ID != "" || e.Result.ID != "" {
			return fmt.Errorf("before_intent: implicit action execution after reopen")
		}
		return nil
	}
	if !e.IntentDurable || e.Intent.ID == "" || e.Intent.Handle != e.Handle || e.Intent.Kind != e.Kind || e.Intent.State != upgradetx.ActionIntent || e.Intent.IntentDigest == "" {
		return fmt.Errorf("crash lost durable intent or adopted another attempt")
	}
	if !e.CustodyKnown {
		if e.AfterEffects != e.BeforeEffects || e.Result.ID != "" || e.NativeRC != 74 {
			return fmt.Errorf("unknown custody caused redispatch or false completion")
		}
		return nil
	}
	if e.NativeRC != 0 || e.AfterEffects != 1 || e.Result.ID != e.Intent.ID || e.Result.Handle != e.Handle || e.Result.Kind != e.Kind || e.Result.State != upgradetx.ActionDurable || e.Result.ResultDigest == "" {
		return fmt.Errorf("action was duplicated or lacks its own durable receipt")
	}
	switch e.Window {
	case "intent_before_effect":
		if e.BeforeEffects != 0 || e.RecoveredEffectID == "" {
			return fmt.Errorf("intent_before_effect: recovery did not perform exactly one authorized effect")
		}
	case "effect_before_receipt", "after_receipt":
		if e.BeforeEffects != 1 || e.EffectID == "" || e.RecoveredEffectID != e.EffectID {
			return fmt.Errorf("effect identity changed instead of querying original custody")
		}
	default:
		return fmt.Errorf("unknown action death window")
	}
	return nil
}

func transactionCrashControl(kind upgradetx.ActionKind, window string) transactionCrashEvidence {
	seal, _ := transactionOracleControl()
	h := seal.Attempt.Handle
	record := upgradetx.Record{Version: 2, Key: h.Key, Grant: upgradetx.Grant{Handle: h}, Forward: seal.Attempt, RecordedPreviousSHA: seal.Attempt.SourceSHA}
	e := transactionCrashEvidence{Seal: seal, SchemaVersion: 1, Kind: kind, Window: window, Handle: h, Before: record, Reopened: record, CustodyKnown: true, Reaped: true}
	if window == "before_intent" {
		return e
	}
	e.IntentDurable = true
	e.Intent = upgradetx.Action{ID: strings.Repeat("6", 32), Handle: h, Kind: kind, State: upgradetx.ActionIntent, IntentDigest: strings.Repeat("7", 64)}
	e.Result = e.Intent
	e.Result.State, e.Result.ResultDigest = upgradetx.ActionDurable, strings.Repeat("8", 64)
	e.AfterEffects, e.RecoveredEffectID = 1, "original-effect"
	if window != "intent_before_effect" {
		e.BeforeEffects, e.EffectID = 1, "original-effect"
	}
	return e
}

// Controls test all forty action/window combinations and the independent
// unknown-custody arm. No product IO is simulated by returning success.
// origin: hot-update S8 plan §3.2 / P25
func TestTransactionCrashRecoveryOracleControls(t *testing.T) {
	if len(transactionCrashActions) != 10 || len(transactionDeathWindows) != 4 {
		t.Fatal("incomplete crash Cartesian set")
	}
	var actions []string
	for _, kind := range transactionCrashActions {
		actions = append(actions, string(kind))
	}
	if !reflect.DeepEqual(actions, []string{"publish", "select", "freeze", "dispatch", "activate", "control", "migrate", "retire", "reverse", "release"}) || !reflect.DeepEqual(transactionDeathWindows, []string{"before_intent", "intent_before_effect", "effect_before_receipt", "after_receipt"}) {
		t.Fatal("crash action/death-window closure changed")
	}
	for _, kind := range transactionCrashActions {
		for _, window := range transactionDeathWindows {
			t.Run(string(kind)+"/"+window, func(t *testing.T) {
				good := transactionCrashControl(kind, window)
				if err := judgeTransactionCrash(good); err != nil {
					t.Fatal(err)
				}
				bad := good
				bad.AfterEffects++
				if judgeTransactionCrash(bad) == nil {
					t.Fatal("implicit/duplicate effect accepted")
				}
				bad = good
				bad.Reopened.Forward.Handle.TxID = strings.Repeat("9", 32)
				if judgeTransactionCrash(bad) == nil {
					t.Fatal("reopen changed accepted identity")
				}
				if window == "before_intent" {
					return
				}
				unknown := good
				unknown.CustodyKnown, unknown.AfterEffects, unknown.Result, unknown.NativeRC = false, unknown.BeforeEffects, upgradetx.Action{}, 74
				if err := judgeTransactionCrash(unknown); err != nil {
					t.Fatal(err)
				}
				unknown.AfterEffects++
				if judgeTransactionCrash(unknown) == nil {
					t.Fatal("unknown custody redispatched an effect")
				}
				bad = good
				bad.IntentDurable = false
				if judgeTransactionCrash(bad) == nil {
					t.Fatal("effect without durable intent accepted")
				}
				bad = good
				bad.Result.Handle.CutID = strings.Repeat("9", 32)
				if judgeTransactionCrash(bad) == nil {
					t.Fatal("foreign cut settled action")
				}
				if window != "intent_before_effect" {
					bad = good
					bad.RecoveredEffectID = "replacement-effect"
					if judgeTransactionCrash(bad) == nil {
						t.Fatal("recovery replaced the original effect")
					}
				}
			})
		}
	}
}

// Same test name as the plan's P02/P25 contract. These are real death/reopen
// receipts for each action, not a pure CAS test or an in-memory fake Store.
// origin: hot-update S8 plan §3.2 / §9.1 / P25
func TestActionCrashRecovery(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionActionCrashRecovery(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "action crash receipts x5", g)
	testharness.AssertNoFDLeak(t, "action crash receipts x5", fd, 0)
}

func exerciseTransactionActionCrashRecovery(t *testing.T, round int) {
	t.Helper()
	for _, role := range []string{"agent", "single", "sole-voter", "N3"} {
		for _, kind := range transactionCrashActions {
			for _, window := range transactionDeathWindows {
				for _, known := range []bool{true, false} {
					var e transactionCrashEvidence
					name := fmt.Sprintf("round-%d/crash/%s-%s-%s-known-%t", round, role, kind, window, known)
					if err := readTransactionEvidence(name, &e); err != nil {
						t.Fatal(err)
					}
					if e.RunID != fmt.Sprintf("%s/round-%d", name, round) || e.Round != round || e.Role != role || e.Kind != kind || e.Window != window || e.CustodyKnown != known {
						t.Fatalf("wrong crash cell: %s", name)
					}
					if err := judgeTransactionCrash(e); err != nil {
						t.Fatalf("%s: %v", name, err)
					}
				}
			}
		}
	}
}

type transactionGrantReplayEvidence struct {
	Seal                 transactionOriginalSeal
	SchemaVersion, Round int
	RunID, Cause, Code   string
	Accepted             bool
	NativeRC, Effects    int
	Before, Reopened     upgradetx.Record
}

func judgeTransactionGrantReplay(e transactionGrantReplayEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	want := upgradetx.CodeGrantUnknown
	if e.Cause == "expired" {
		want = upgradetx.CodeGrantExpired
	}
	if e.SchemaVersion != 1 || e.Code != want || e.Accepted || e.Effects != 0 || e.NativeRC == 0 || !reflect.DeepEqual(e.Before, e.Reopened) {
		return fmt.Errorf("M10: reopen/GC/boot issuer authorized an obsolete Start")
	}
	return nil
}

// origin: hot-update S8 plan §3.1 / §9.1 / M10
func TestTransactionGrantReplayOracleControls(t *testing.T) {
	for _, cause := range []string{"unknown", "expired", "gc", "boot_issuer"} {
		seal, _ := transactionOracleControl()
		e := transactionGrantReplayEvidence{Seal: seal, SchemaVersion: 1, Cause: cause, Code: upgradetx.CodeGrantUnknown, NativeRC: 64}
		if cause == "expired" {
			e.Code = upgradetx.CodeGrantExpired
		}
		if err := judgeTransactionGrantReplay(e); err != nil {
			t.Fatal(err)
		}
		for _, change := range []func(*transactionGrantReplayEvidence){func(e *transactionGrantReplayEvidence) { e.Accepted = true }, func(e *transactionGrantReplayEvidence) { e.Effects = 1 }, func(e *transactionGrantReplayEvidence) { e.Reopened.Revision++ }, func(e *transactionGrantReplayEvidence) { e.Code = upgradetx.CodeNotWired }} {
			bad := e
			change(&bad)
			if judgeTransactionGrantReplay(bad) == nil {
				t.Fatal("obsolete grant replay accepted")
			}
		}
	}
}

// origin: hot-update S8 plan §3.1 / §9.1 / M10
func TestStartNeverRecreatesExpiredGrant(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionGrantReplay(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "expired grant receipts x5", g)
	testharness.AssertNoFDLeak(t, "expired grant receipts x5", fd, 0)
}

func exerciseTransactionGrantReplay(t *testing.T, round int) {
	t.Helper()
	for _, cause := range []string{"unknown", "expired", "gc", "boot_issuer"} {
		var e transactionGrantReplayEvidence
		if err := readTransactionEvidence(fmt.Sprintf("round-%d/grant-%s", round, cause), &e); err != nil {
			t.Fatal(err)
		}
		if e.RunID != fmt.Sprintf("grant-%s/round-%d", cause, round) || e.Round != round || e.Cause != cause {
			t.Fatal("wrong grant replay identity")
		}
		if err := judgeTransactionGrantReplay(e); err != nil {
			t.Fatal(err)
		}
	}
}

var transactionInteractions = []string{"late_revoke_current_rollback", "nats_cut_leader_move", "begin_archive_gc", "driver_death_host_reservation"}

type transactionInteractionEvidence struct {
	Seal                                             transactionOriginalSeal
	SchemaVersion, Round                             int
	RunID, Interaction, Point                        string
	CurrentKnown                                     bool
	Original, Current                                upgradetx.Handle
	OriginalScope, CurrentScope                      upgradetx.Scope
	QueryOnly, Revoked, Resurrected, CurrentRetained bool
	PermissionRC                                     int
	Begin, Effect, Sealed                            uint64
	OriginalPlan, ResumedPlan                        upgradetx.Plan
	HostBefore, HostAfter                            string
	ReleasedEarly                                    bool
	Phase, Outcome                                   string
	NativeRC                                         int
}

// The third dimension is R0/R1/R2 for each fault pair, independently split by
// current-owner knowledge. Unknown has priority over attempted reverse/GC.
// origin: hot-update S8 plan §8.2 / P25
func judgeTransactionInteraction(e transactionInteractionEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	if e.SchemaVersion != 1 || e.Original.TxID == "" || !reflect.DeepEqual(e.OriginalScope, e.CurrentScope) {
		return fmt.Errorf("interaction changed the original sealed set")
	}
	if !e.CurrentKnown {
		if !e.CurrentRetained || e.Phase != "recovery_required" || e.Outcome != "unknown" || e.NativeRC != 74 {
			return fmt.Errorf("unknown owner falsely completed/released the interaction")
		}
		return nil
	}
	switch e.Interaction {
	case "late_revoke_current_rollback":
		if !e.Revoked || e.Resurrected || e.PermissionRC != 77 {
			return fmt.Errorf("late revoke lost priority over current rollback")
		}
		if e.Point != "R0" && (e.Current.CutID == e.Original.CutID || e.Current.Generation <= e.Original.Generation) {
			return fmt.Errorf("partial ACTIVE/new work used a stale rollback snapshot")
		}
	case "nats_cut_leader_move":
		if !e.QueryOnly || e.Current.TxID != e.Original.TxID {
			return fmt.Errorf("NATS/leader change redispatched a different transaction")
		}
	case "begin_archive_gc":
		if e.Begin == 0 || e.Effect <= e.Begin || e.Sealed < e.Effect || !e.CurrentRetained {
			return fmt.Errorf("Begin/GC race lost a live admission or current record")
		}
	case "driver_death_host_reservation":
		if len(e.OriginalPlan.Steps) == 0 || !reflect.DeepEqual(e.OriginalPlan, e.ResumedPlan) || e.HostBefore == "" || e.HostBefore != e.HostAfter || e.ReleasedEarly {
			return fmt.Errorf("driver death replanned children or released the host reservation")
		}
	default:
		return fmt.Errorf("unknown interaction")
	}
	return nil
}

// origin: hot-update S8 plan §8.2 / P25
func TestTransactionCrashInteractionOracleControls(t *testing.T) {
	if !reflect.DeepEqual(transactionInteractions, []string{"late_revoke_current_rollback", "nats_cut_leader_move", "begin_archive_gc", "driver_death_host_reservation"}) {
		t.Fatal("interaction closure changed")
	}
	seal, p := transactionOracleControl()
	for _, interaction := range transactionInteractions {
		for _, point := range []string{"R0", "R1", "R2"} {
			t.Run(interaction+"/"+point, func(t *testing.T) {
				good := transactionInteractionEvidence{Seal: seal, SchemaVersion: 1, Interaction: interaction, Point: point, CurrentKnown: true, Original: seal.Attempt.Handle, Current: seal.Attempt.Handle, OriginalScope: seal.Original, CurrentScope: seal.Original, QueryOnly: true, Revoked: true, PermissionRC: 77, CurrentRetained: true, Begin: 1, Effect: 2, Sealed: 3, OriginalPlan: p.Plan, ResumedPlan: p.Plan, HostBefore: "original-host-token", HostAfter: "original-host-token"}
				if interaction == "late_revoke_current_rollback" && point != "R0" {
					good.Current.CutID = strings.Repeat("9", 32)
					good.Current.Generation++
				}
				if err := judgeTransactionInteraction(good); err != nil {
					t.Fatal(err)
				}
				bad := good
				switch interaction {
				case "late_revoke_current_rollback":
					bad.Resurrected = true
				case "nats_cut_leader_move":
					bad.Current.TxID = strings.Repeat("9", 32)
				case "begin_archive_gc":
					bad.Begin = 0
				case "driver_death_host_reservation":
					bad.ReleasedEarly = true
				}
				if judgeTransactionInteraction(bad) == nil {
					t.Fatal("fault interaction defect accepted")
				}
				unknown := good
				unknown.CurrentKnown, unknown.Phase, unknown.Outcome, unknown.NativeRC = false, "recovery_required", "unknown", 74
				if err := judgeTransactionInteraction(unknown); err != nil {
					t.Fatal(err)
				}
				unknown.CurrentRetained = false
				if judgeTransactionInteraction(unknown) == nil {
					t.Fatal("unknown current was archived/GCed")
				}
			})
		}
	}
}

// origin: hot-update S8 plan §8.2 / §11.2 P25
func TestTransactionCrashInteractions(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionCrashInteractions(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "crash interaction receipts x5", g)
	testharness.AssertNoFDLeak(t, "crash interaction receipts x5", fd, 0)
}

func exerciseTransactionCrashInteractions(t *testing.T, round int) {
	t.Helper()
	for _, interaction := range transactionInteractions {
		for _, point := range []string{"R0", "R1", "R2"} {
			for _, known := range []bool{true, false} {
				var e transactionInteractionEvidence
				name := fmt.Sprintf("round-%d/interactions/%s-%s-known-%t", round, interaction, point, known)
				if err := readTransactionEvidence(name, &e); err != nil {
					t.Fatal(err)
				}
				if e.RunID != fmt.Sprintf("%s/round-%d", name, round) || e.Round != round || e.Interaction != interaction || e.Point != point || e.CurrentKnown != known {
					t.Fatalf("wrong interaction cell: %s", name)
				}
				if err := judgeTransactionInteraction(e); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			}
		}
	}
}
