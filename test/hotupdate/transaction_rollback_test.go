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

// These are observer inputs, not product DTO extensions. P26's supervised
// fixture supplies the actual CLI wait status, syscall/OFD and pre-Replay rows.
// Missing evidence remains red; the oracle controls below never sign a product
// run. Reuse P25's original seal so the independent original set has one owner.
// origin: hot-update S8 plan §8.1 / §9.0 / §11.2 P23
type transactionP23CLI struct {
	Argv                    []string
	ExecutedPath, BinarySHA string
	Owner                   upgradetx.Owner
	BeginNS, EndNS          int64
	RC                      int
	View                    upgradetx.View
}

type transactionP23FD struct {
	ID, OFD string
	Owner   upgradetx.Owner
}

type transactionP23Write struct {
	Domain, Key, Digest string
	Sequence            uint64
	Absent              bool
}

type transactionRollbackEvidence struct {
	Seal                                            transactionOriginalSeal
	Round                                           int
	Role, Point, RunID                              string
	Before                                          upgradetx.Observation
	After                                           upgradetx.View
	CLI                                             transactionP23CLI
	RunningOwnerBefore, RunningOwnerAfter           upgradetx.Owner
	ProcRunningSHA                                  string
	ForwardHistoricalBefore, ForwardHistoricalAfter upgradetx.Record
	ParentHistoricalBefore, ParentHistoricalAfter   upgradetx.Record
	Taken, Remaining, Salvaged                      []transactionP23FD
	OldRetiredSequence, ReverseActivateSequence     uint64
	Admissions                                      []transactionAdmission
	Writes, BeforeReplay                            []transactionP23Write
	ACKReadNS, FirstReplayNS                        int64
}

func transactionSameCut(a, b upgradetx.Handle) bool {
	return a.Key == b.Key && a.TxID == b.TxID && a.CutID == b.CutID && a.Generation == b.Generation
}

func judgeTransactionP23CLI(c transactionP23CLI, verb, txID string, rc int) error {
	if !transactionHex(c.BinarySHA, 32) || !strings.HasPrefix(c.ExecutedPath, "/") || !transactionOwnerKnown(c.Owner) || c.BeginNS <= 0 || c.EndNS < c.BeginNS || c.RC != rc {
		return fmt.Errorf("missing supervised real CLI identity or native exit status")
	}
	if !slices.Contains(c.Argv, verb) {
		return fmt.Errorf("wrong product CLI verb")
	}
	for i := 0; i+1 < len(c.Argv); i++ {
		if c.Argv[i] == "--tx-id" && c.Argv[i+1] == txID {
			return nil
		}
	}
	return fmt.Errorf("CLI did not address the original transaction")
}

// A current export must preserve exact object identities, not their old owners.
// Ownership can migrate; loss of an original or late admission cannot.
func transactionP23Contains(resources []upgradetx.Resource, expected []upgradetx.Resource) bool {
	for _, resource := range expected {
		if !transactionHasResource(resources, resource) {
			return false
		}
	}
	return true
}

// Latest ACKs are derived from every acknowledged write, so repeated keys and
// delete/revoke absence cannot be hidden behind a digest of an old prefix.
// origin: hot-update S8 plan §8.1 / M13
func judgeTransactionLatestWrites(writes, observed []transactionP23Write, readNS, replayNS int64) error {
	if readNS <= 0 || replayNS <= readNS || len(writes) < 8 {
		return fmt.Errorf("M13: missing pre-Replay ACK observation")
	}
	latest := map[string]transactionP23Write{}
	domains := map[string]bool{}
	overwrite, deleted, revoked := false, false, false
	var last uint64
	for _, write := range writes {
		if write.Sequence <= last || write.Key == "" || write.Domain == "" || !write.Absent && !transactionHex(write.Digest, 32) || write.Absent && write.Digest != "" {
			return fmt.Errorf("invalid acknowledged write ledger")
		}
		last = write.Sequence
		key := write.Domain + "/" + write.Key
		_, repeated := latest[key]
		overwrite = overwrite || repeated && !write.Absent
		deleted = deleted || repeated && write.Absent && write.Domain != "acl"
		revoked = revoked || repeated && write.Absent && write.Domain == "acl"
		latest[key], domains[write.Domain] = write, true
	}
	for _, domain := range []string{"session", "node", "proxy", "transfer", "acl"} {
		if !domains[domain] {
			return fmt.Errorf("M13: durable oracle omitted %s", domain)
		}
	}
	if !overwrite || !deleted || !revoked || len(observed) != len(latest) {
		return fmt.Errorf("M13: latest overwrite/absence not exercised")
	}
	seen := map[string]bool{}
	for _, actual := range observed {
		key := actual.Domain + "/" + actual.Key
		if seen[key] || latest[key] != actual {
			return fmt.Errorf("M13: old DB lost the latest ACK before Replay")
		}
		seen[key] = true
	}
	return nil
}

// origin: hot-update S8 plan §8.1 / M02/M07/M08/M13
func judgeTransactionRollback(e transactionRollbackEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	if !reflect.DeepEqual(e.ForwardHistoricalBefore, e.ForwardHistoricalAfter) || !reflect.DeepEqual(e.ParentHistoricalBefore, e.ParentHistoricalAfter) {
		return fmt.Errorf("reverse rewrote immutable forward/parent history")
	}
	verb := "rollback"
	if e.Role == "agent" {
		verb = "upgrade-rollback"
	}
	if err := judgeTransactionP23CLI(e.CLI, verb, e.Before.Current.Handle.TxID, 0); err != nil {
		return err
	}
	if !reflect.DeepEqual(e.CLI.View, e.After) {
		return fmt.Errorf("CLI receipt differs from the sampled product View")
	}
	if !transactionOwnerKnown(e.RunningOwnerBefore) || e.RunningOwnerBefore.BootID != e.RunningOwnerAfter.BootID || e.RunningOwnerBefore.PID != e.RunningOwnerAfter.PID || e.RunningOwnerBefore.StartTicks != e.RunningOwnerAfter.StartTicks {
		return fmt.Errorf("rollback replaced the runtime process")
	}
	if e.Point == "R0" {
		if e.Before.Current.CutStage != upgradetx.CutPristine || !e.Before.OwnerKnown || !e.After.Observation.OwnerKnown || !e.After.Observation.Census.Known || !e.After.Observation.Census.Complete || e.After.Observation.Census.Admitting != 0 || e.After.Observation.Current.Phase != upgradetx.PhaseCancelled || e.After.Observation.Current.Outcome != upgradetx.OutcomeCancelled || e.After.Observation.RunningSHA != e.Before.RunningSHA || !transactionSameCut(e.Before.Current.Handle, e.After.Observation.Current.Handle) || !transactionP23Contains(e.After.Observation.Census.Resources, e.Seal.Original.Expected) {
			return fmt.Errorf("R0 failed to thaw the original known owner without a reverse effect")
		}
		return nil
	}
	if e.Point != "R1" && e.Point != "R2" {
		return fmt.Errorf("unknown rollback point")
	}
	a := e.After.Observation.Current
	if a.Direction != upgradetx.DirectionReverse || a.Handle.CutID == e.Before.Current.Handle.CutID || a.Handle.Generation != e.Before.Current.Handle.Generation+1 || a.SourceGeneration != e.Before.Current.Handle.Generation || a.SourceSHA != e.Before.RunningSHA || a.TargetSHA != e.ForwardHistoricalBefore.RecordedPreviousSHA {
		return fmt.Errorf("M07: reverse reused the forward attempt or stale SHA pair")
	}
	if e.ProcRunningSHA != a.TargetSHA || e.After.Observation.RunningSHA != e.ProcRunningSHA {
		return fmt.Errorf("M02: selected disk bytes did not replace the running image")
	}
	if err := upgradetx.Completed(a, e.After.Observation); err != nil {
		return err
	}
	if !transactionP23Contains(a.Scope.Expected, e.Seal.Original.Expected) || !transactionP23Contains(a.Resources.Census.Resources, e.Seal.Original.Expected) {
		return fmt.Errorf("current export lost an original resource")
	}
	if e.Point == "R1" {
		if e.Before.Current.CutStage != upgradetx.CutPartialActive || len(e.Taken) == 0 || len(e.Remaining) == 0 || e.OldRetiredSequence == 0 || e.ReverseActivateSequence <= e.OldRetiredSequence {
			return fmt.Errorf("R1 did not exercise partial Take and retirement before G+2")
		}
		union := append(append([]transactionP23FD(nil), e.Taken...), e.Remaining...)
		if len(union) != len(e.Salvaged) {
			return fmt.Errorf("R1 lost partial Take salvage")
		}
		seen := map[string]bool{}
		for _, fd := range union {
			if fd.ID == "" || fd.OFD == "" || seen[fd.ID] || !transactionOwnerKnown(fd.Owner) || !slices.Contains(e.Salvaged, fd) {
				return fmt.Errorf("R1 reopened or lost an original OFD")
			}
			seen[fd.ID] = true
		}
	} else {
		if e.Before.Current.CutStage != upgradetx.CutActive || len(e.Admissions) == 0 {
			return fmt.Errorf("R2 omitted new candidate work")
		}
		for _, admission := range e.Admissions {
			if admission.Begin == 0 || admission.Effect <= admission.Begin || admission.Finish <= admission.Effect || admission.Resource.Generation != e.Before.Current.Handle.Generation || !transactionHasResource(a.Scope.Expected, admission.Resource) || !transactionHasResource(a.Released.Census.Resources, admission.Resource) {
				return fmt.Errorf("R2 replayed bootstrap state and lost new work")
			}
		}
	}
	return judgeTransactionLatestWrites(e.Writes, e.BeforeReplay, e.ACKReadNS, e.FirstReplayNS)
}

// Literal product receipts require every role and every point in each of five
// independently supervised rounds. A R0 preflight refusal cannot fill R1.
// origin: hot-update S8 plan §9.1 / §11.2 P23
func TestRollbackExportsCurrentStateNotStaleSnapshot(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionCurrentRollback(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "current rollback receipts x5", g)
	testharness.AssertNoFDLeak(t, "current rollback receipts x5", fd, 0)
}

func exerciseTransactionCurrentRollback(t *testing.T, round int) {
	t.Helper()
	for _, role := range []string{"agent", "single", "sole-voter", "N3"} {
		for _, point := range []string{"R0", "R1", "R2"} {
			name := fmt.Sprintf("p23/round-%d/rollback/%s-%s", round, role, point)
			var e transactionRollbackEvidence
			if err := readTransactionEvidence(name, &e); err != nil {
				t.Fatalf("NOT_RUN pending P13-P17/P19: %s: %v", name, err)
			}
			if e.Round != round || e.Seal.Round != round || e.Role != role || e.Point != point || e.RunID != name || e.Seal.RunID != name {
				t.Fatal("rollback receipt was reused across roles/rounds/points")
			}
			if err := judgeTransactionRollback(e); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
	}
}

// Control data exercises this reader's rejection paths only. It is explicitly
// separate from the required product receipts, and contains no spawned process.
func transactionRollbackOracleControl(point string) transactionRollbackEvidence {
	seal, p := transactionOracleControl()
	before := p.Observation
	before.Current.CutStage = upgradetx.CutActive
	before.Current.Phase, before.Current.Outcome = upgradetx.PhaseMigrating, upgradetx.OutcomePending
	if point == "R1" {
		before.Current.CutStage = upgradetx.CutPartialActive
	}
	a := p.Attempt
	a.Handle.CutID, a.Handle.Generation = strings.Repeat("7", 32), 3
	a.Direction, a.SourceGeneration, a.SourceSHA, a.TargetSHA = upgradetx.DirectionReverse, 2, a.TargetSHA, a.SourceSHA
	a.Scope.Expected = append(append([]upgradetx.Resource(nil), seal.Original.Expected...), p.Admissions[0].Resource)
	// Canonical identity ordering is part of the frozen scope contract.
	slices.SortFunc(a.Scope.Expected, func(a, b upgradetx.Resource) int {
		return strings.Compare(transactionIdentity(a), transactionIdentity(b))
	})
	owner := transactionTestOwner(3)
	owner.BootID, owner.PID, owner.StartTicks = p.Observation.Census.Stamp.Producer.BootID, p.Observation.Census.Stamp.Producer.PID, p.Observation.Census.Stamp.Producer.StartTicks
	for _, r := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
		r.Stamp.Attempt, r.Stamp.Producer, r.RunningSHA = a.Handle, owner, a.TargetSHA
		r.Census.Stamp = r.Stamp
		r.Census.Resources = append([]upgradetx.Resource(nil), r.Census.Resources...)
		for i := range r.Census.Resources {
			r.Census.Resources[i].Owner, r.Census.Resources[i].Generation = owner, 3
		}
	}
	o := p.Observation
	o.Current, o.RunningSHA, o.EffectiveSHA = a, a.TargetSHA, a.TargetSHA
	o.Nonce, o.At = strings.Repeat("8", 32), 120
	o.Census = a.Released.Census
	o.Census.Stamp.Nonce, o.Census.Stamp.At = o.Nonce, o.At
	view := upgradetx.View{Historical: upgradetx.Record{RecordedPreviousSHA: a.TargetSHA}, Observation: o}
	view = transactionResealOracleView(view)
	e := transactionRollbackEvidence{Seal: seal, Point: point, Role: "agent", Before: before, After: view, ProcRunningSHA: a.TargetSHA, RunningOwnerBefore: owner, RunningOwnerAfter: owner, OldRetiredSequence: 1, ReverseActivateSequence: 2, Admissions: p.Admissions, ACKReadNS: 100, FirstReplayNS: 120}
	e.ForwardHistoricalBefore.RecordedPreviousSHA = a.TargetSHA
	e.ForwardHistoricalAfter = e.ForwardHistoricalBefore
	e.CLI = transactionP23CLI{Argv: []string{"node", "upgrade-rollback", "test-agent", "--tx-id", before.Current.Handle.TxID}, ExecutedPath: seal.ExecutedPath, BinarySHA: seal.BinarySHA, Owner: owner, BeginNS: 90, EndNS: 130, View: view}
	e.Taken = []transactionP23FD{{"taken", "pipe:original", owner}}
	e.Remaining = []transactionP23FD{{"remaining", "lease:original", owner}}
	e.Salvaged = append(append([]transactionP23FD(nil), e.Taken...), e.Remaining...)
	for i, domain := range []string{"session", "node", "proxy", "transfer", "acl", "session", "proxy", "acl"} {
		e.Writes = append(e.Writes, transactionP23Write{Domain: domain, Key: "object", Digest: strings.Repeat("a", 64), Sequence: uint64(i + 1)})
	}
	e.Writes[5].Digest = strings.Repeat("b", 64)
	e.Writes[6].Digest, e.Writes[6].Absent = "", true
	e.Writes[7].Digest, e.Writes[7].Absent = "", true
	latest := map[string]transactionP23Write{}
	for _, w := range e.Writes {
		latest[w.Domain+"/"+w.Key] = w
	}
	for _, w := range latest {
		e.BeforeReplay = append(e.BeforeReplay, w)
	}
	if point == "R0" {
		e.Before.Current.CutStage = upgradetx.CutPristine
		e.After.Observation = e.Before
		e.After.Observation.Current.Phase, e.After.Observation.Current.Outcome = upgradetx.PhaseCancelled, upgradetx.OutcomeCancelled
		e.CLI.View = e.After
	}
	return e
}

// Only oracle control data is resealed here. Changing a control's scope or
// participant changes its canonical commitments; the independent JSON digest
// in the original-set seal cannot substitute for either product commitment.
// Live receipts must continue to arrive unchanged through readTransactionEvidence.
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F1; hot-update S8 plan §4.2
func transactionResealOracleView(view upgradetx.View) upgradetx.View {
	a := &view.Observation.Current
	a.Scope.Expected = slices.Clone(a.Scope.Expected)
	for i := range a.Scope.Expected {
		switch a.Scope.Expected[i].Custody {
		case "original":
			a.Scope.Expected[i].Custody = "source"
		case "migrated":
			a.Scope.Expected[i].Custody = "target"
		}
	}
	scopeDigest, err := upgradetx.ScopeDigest(a.Scope)
	if err != nil {
		panic(err)
	}
	expectedDigest, err := upgradetx.ExpectedDigest(a.Scope)
	if err != nil {
		panic(err)
	}
	for _, r := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
		r.Stamp.Attempt, r.Stamp.ScopeDigest = a.Handle, scopeDigest
		r.Census.Stamp, r.Census.ExpectedDigest = r.Stamp, expectedDigest
		r.Census.Resources = slices.Clone(r.Census.Resources)
		for i := range r.Census.Resources {
			if r.Census.Resources[i].Custody == "migrated" {
				r.Census.Resources[i].Custody = "target"
			}
		}
	}
	view.Observation.Census = a.Released.Census
	view.Observation.Census.Stamp.Nonce = view.Observation.Nonce
	view.Observation.Census.Stamp.At = view.Observation.At
	return view
}

// origin: hot-update S8 plan §8.1 / M02/M07/M13
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F1
func TestTransactionRollbackRejectsStaleEvidence(t *testing.T) {
	for _, point := range []string{"R0", "R1", "R2"} {
		if err := judgeTransactionRollback(transactionRollbackOracleControl(point)); err != nil {
			t.Fatalf("%s control: %v", point, err)
		}
	}
	for name, mutate := range map[string]func(*transactionRollbackEvidence){
		"M02-disk-only": func(e *transactionRollbackEvidence) { e.ProcRunningSHA = e.Before.RunningSHA },
		"M07-forward-receipt": func(e *transactionRollbackEvidence) {
			e.After.Observation.Current.Control = e.Before.Current.Control
			e.CLI.View = e.After
		},
		"control-missing": func(e *transactionRollbackEvidence) {
			e.After.Observation.Current.Control = upgradetx.Receipt{}
			e.CLI.View = e.After
		},
		"resources-missing": func(e *transactionRollbackEvidence) {
			e.After.Observation.Current.Resources = upgradetx.Receipt{}
			e.CLI.View = e.After
		},
		"released-missing": func(e *transactionRollbackEvidence) {
			e.After.Observation.Current.Released = upgradetx.Receipt{}
			e.CLI.View = e.After
		},
		"scope-commitment-stale": func(e *transactionRollbackEvidence) {
			e.After.Observation.Current.Scope.ID = strings.Repeat("f", 32)
			e.CLI.View = e.After
		},
		"expected-commitment-stale": func(e *transactionRollbackEvidence) {
			e.After.Observation.Current.Released.Census.ExpectedDigest = strings.Repeat("f", 64)
			e.CLI.View = e.After
		},
		"historical-query-nonce": func(e *transactionRollbackEvidence) {
			e.After.Observation.Nonce = e.After.Observation.Current.Released.Stamp.Nonce
			e.After.Observation.Census.Stamp.Nonce = e.After.Observation.Nonce
			e.CLI.View = e.After
		},
		"unknown-census":           func(e *transactionRollbackEvidence) { e.After.Observation.Census.Known = false; e.CLI.View = e.After },
		"R1-lost-remaining-OFD":    func(e *transactionRollbackEvidence) { e.Salvaged = e.Salvaged[:1] },
		"R1-third-generation":      func(e *transactionRollbackEvidence) { e.OldRetiredSequence = e.ReverseActivateSequence },
		"M13-replay-before-oracle": func(e *transactionRollbackEvidence) { e.FirstReplayNS = e.ACKReadNS },
		"M13-lost-delete":          func(e *transactionRollbackEvidence) { e.BeforeReplay = e.BeforeReplay[:len(e.BeforeReplay)-1] },
		"M13-stale-overwrite": func(e *transactionRollbackEvidence) {
			for i := range e.BeforeReplay {
				if e.BeforeReplay[i].Domain == "session" {
					e.BeforeReplay[i] = e.Writes[0]
				}
			}
		},
		"M13-resurrected-revoke": func(e *transactionRollbackEvidence) {
			for i := range e.BeforeReplay {
				if e.BeforeReplay[i].Domain == "acl" {
					e.BeforeReplay[i] = e.Writes[4]
				}
			}
		},
		"immutable-parent": func(e *transactionRollbackEvidence) { e.ParentHistoricalAfter.Revision++ },
	} {
		t.Run(name, func(t *testing.T) {
			e := transactionRollbackOracleControl("R1")
			mutate(&e)
			if judgeTransactionRollback(e) == nil {
				t.Fatal("rollback evidence defect accepted")
			}
		})
	}
	e := transactionRollbackOracleControl("R2")
	e.After.Observation.Current.Scope.Expected = append([]upgradetx.Resource(nil), e.Seal.Original.Expected...)
	e.CLI.View = e.After
	if judgeTransactionRollback(e) == nil {
		t.Fatal("R2 lost a newly admitted task")
	}
	e = transactionRollbackOracleControl("R0")
	e.After.Observation.OwnerKnown = false
	e.CLI.View = e.After
	if judgeTransactionRollback(e) == nil {
		t.Fatal("R0 cancellation signed unknown current custody")
	}
}
