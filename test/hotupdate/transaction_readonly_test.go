//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type transactionReadonlyEvidence struct {
	Seal                                                       transactionOriginalSeal
	Round                                                      int
	Case, RunID                                                string
	Key                                                        upgradetx.Key
	CanonicalPath, CanonicalBeforeSHA, CanonicalAfterSHA       string
	CanonicalUID                                               uint32
	CanonicalMode, SourceMode, TargetMode                      uint32
	JournalPath, WriterIdentity                                string
	Stage                                                      transactionP23CLI
	Artifact                                                   upgradeartifact.Receipt
	SelectedGrant                                              upgradetx.Grant
	StageBefore, StageAfter                                    upgradetx.Record
	Forward, Reverse, Cold, Second                             upgradetx.View
	ForwardProcSHA, ReverseProcSHA, ColdProcSHA, SecondProcSHA string
	ColdCLI                                                    transactionP23CLI
	ColdStartedFrom                                            string
	ForwardOwner, ColdOwner                                    upgradetx.Owner
	SiblingBefore, SiblingAfter                                upgradetx.View
	AlienGrant                                                 upgradetx.Grant
	AlienAccepted                                              bool
	AlienCode                                                  string
	SelectorsBeforeAlien, SelectorsAfterAlien                  []string
}

var transactionReadonlyCases = []string{"root-agent", "user-agent", "root-broker", "user-broker", "multi-uid"}

// Canonical is an immutable dispatcher; every component owns its selector and
// journal. Stage publishes exactly the authorized source/candidate pair without
// becoming another journal writer or starting a service.
// origin: hot-update S8 plan §5.2 / P23 / M02/M16
func judgeTransactionReadonly(e transactionReadonlyEvidence) error {
	if err := judgeTransactionProvenance(e.Seal); err != nil {
		return err
	}
	if !slices.Contains(transactionReadonlyCases, e.Case) || e.Key != e.SelectedGrant.Handle.Key || e.JournalPath == "" || !filepath.IsAbs(e.JournalPath) || e.WriterIdentity == "" {
		return fmt.Errorf("readonly component lacks its unique registered journal")
	}
	if !filepath.IsAbs(e.CanonicalPath) || e.CanonicalPath != e.Artifact.CanonicalPath || e.CanonicalMode&0222 != 0 || e.CanonicalMode&0111 == 0 || e.CanonicalBeforeSHA != e.CanonicalAfterSHA || !transactionHex(e.CanonicalBeforeSHA, 32) {
		return fmt.Errorf("readonly canonical was replaced or made writable by the fixture")
	}
	if e.Artifact.Grant != e.SelectedGrant || e.Artifact.SourceSHA != e.SelectedGrant.SourceSHA || e.Artifact.TargetSHA != e.SelectedGrant.TargetSHA || e.Artifact.SourceSHA == e.Artifact.TargetSHA || !transactionHex(e.Artifact.ManifestDigest, 32) || filepath.Base(e.Artifact.SourcePath) != e.Artifact.SourceSHA || filepath.Base(e.Artifact.TargetPath) != e.Artifact.TargetSHA || e.SourceMode&0222 != 0 || e.TargetMode&0222 != 0 || e.SourceMode&0111 == 0 || e.TargetMode&0111 == 0 {
		return fmt.Errorf("M16: root slots were adopted by another grant or lack immutable SHA provenance")
	}
	if e.Stage.RC != 0 || !transactionHex(e.Stage.BinarySHA, 32) || !transactionOwnerKnown(e.Stage.Owner) || e.Stage.BeginNS <= 0 || e.Stage.EndNS <= e.Stage.BeginNS || !slices.Contains(e.Stage.Argv, "upgrade") || !slices.Contains(e.Stage.Argv, "stage") || !slices.Contains(e.Stage.Argv, "--grant") || !slices.Contains(e.Stage.Argv, "--source") || !slices.Contains(e.Stage.Argv, "--candidate") || !slices.Contains(e.Stage.Argv, "--receipt-out") || !reflect.DeepEqual(e.StageBefore, e.StageAfter) {
		return fmt.Errorf("product stage started execution or became a second journal writer")
	}
	for _, pair := range []struct {
		view      upgradetx.View
		proc, sha string
	}{{e.Forward, e.ForwardProcSHA, e.Artifact.TargetSHA}, {e.Reverse, e.ReverseProcSHA, e.Artifact.SourceSHA}, {e.Second, e.SecondProcSHA, e.Artifact.TargetSHA}} {
		if pair.view.Observation.RunningSHA != pair.sha || pair.view.Observation.EffectiveSHA != pair.sha || pair.proc != pair.sha {
			return fmt.Errorf("M02: readonly selector did not select the running image")
		}
		if err := upgradetx.Completed(pair.view.Observation.Current, pair.view.Observation); err != nil {
			return err
		}
	}
	if e.Reverse.Observation.Current.Direction != upgradetx.DirectionReverse || e.Reverse.Observation.Current.TargetSHA != e.Forward.Historical.RecordedPreviousSHA || e.Reverse.Observation.Current.Handle.CutID == e.Forward.Observation.Current.Handle.CutID || e.Reverse.Observation.Current.Handle.Generation != e.Forward.Observation.Current.Handle.Generation+1 {
		return fmt.Errorf("readonly rollback reused forward proof")
	}
	if e.ColdStartedFrom != e.CanonicalPath || e.ColdCLI.ExecutedPath != e.CanonicalPath || e.ColdCLI.RC != 0 || !transactionHex(e.ColdCLI.BinarySHA, 32) || !transactionOwnerKnown(e.ColdOwner) || !transactionOwnerKnown(e.ForwardOwner) || e.ColdOwner == e.ForwardOwner || e.ColdCLI.BeginNS <= 0 || e.ColdCLI.EndNS <= e.ColdCLI.BeginNS || e.ColdProcSHA != e.Artifact.SourceSHA || e.Cold.Observation.RunningSHA != e.Artifact.SourceSHA || e.Cold.Observation.EffectiveSHA != e.Artifact.SourceSHA {
		return fmt.Errorf("ordinary cold boot ignored the component's rolled-back selector")
	}
	if e.Key.Component == upgradetx.ComponentAgent {
		if !slices.Contains(e.ColdCLI.Argv, "agent") || !slices.Contains(e.ColdCLI.Argv, "--session") || !slices.Contains(e.ColdCLI.Argv, "--nid") || slices.Contains(e.ColdCLI.Argv, "--config") {
			return fmt.Errorf("cold agent did not use its ordinary registered argv")
		}
	} else if !slices.Contains(e.ColdCLI.Argv, "serve") || !slices.Contains(e.ColdCLI.Argv, "--config") {
		return fmt.Errorf("cold broker did not use its ordinary registered argv")
	}
	if e.Second.Observation.Current.SourceSHA != e.Artifact.SourceSHA || e.Second.Observation.Current.Handle.TxID == e.Forward.Observation.Current.Handle.TxID || e.Second.Observation.Current.Handle.Generation <= e.Reverse.Observation.Current.Handle.Generation {
		return fmt.Errorf("second upgrade started from canonical/newest bytes or reused an old grant")
	}
	if e.AlienGrant.Handle.TxID == e.SelectedGrant.Handle.TxID || e.AlienGrant.SourceSHA != e.SelectedGrant.SourceSHA || e.AlienGrant.TargetSHA != e.SelectedGrant.TargetSHA || e.AlienAccepted || e.AlienCode != upgradetx.CodeRejected && e.AlienCode != upgradetx.CodeGrantUnknown || !reflect.DeepEqual(e.SelectorsBeforeAlien, e.SelectorsAfterAlien) {
		return fmt.Errorf("M16: same SHA pair authorized cross-grant publication/adoption")
	}
	if e.Case == "multi-uid" {
		if e.CanonicalUID != 0 || e.SiblingBefore.Historical.Key.UID == e.Key.UID || e.SiblingBefore.Historical.Key.DomainID != e.Key.DomainID || !reflect.DeepEqual(e.SiblingBefore, e.SiblingAfter) {
			return fmt.Errorf("multi-UID sibling selector or journal was overwritten")
		}
	} else if strings.HasPrefix(e.Case, "root-") && e.Key.UID != 0 || strings.HasPrefix(e.Case, "user-") && e.Key.UID == 0 {
		return fmt.Errorf("readonly role was relabeled instead of exercising its real UID")
	}
	return nil
}

// The five lanes include actual root/non-root processes and independent UID
// selectors. Evidence is generated by product stage and ordinary cold argv,
// never by fixture copies of .prev, journal edits, chmod or ExecStart changes.
// origin: hot-update S8 plan §5.2 / §9.1 / §11.2 P23
func TestReadOnlyRollbackColdBootUpgrade(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionReadonly(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "readonly stage rollback cold boot x5", g)
	testharness.AssertNoFDLeak(t, "readonly stage rollback cold boot x5", fd, 0)
}

func exerciseTransactionReadonly(t *testing.T, round int) {
	t.Helper()
	for _, lane := range transactionReadonlyCases {
		name := fmt.Sprintf("p23/round-%d/readonly/%s", round, lane)
		var e transactionReadonlyEvidence
		if err := readTransactionEvidence(name, &e); err != nil {
			t.Fatalf("NOT_RUN pending P13-P17/P19: %s: %v", name, err)
		}
		if e.Round != round || e.Seal.Round != round || e.Case != lane || e.RunID != name || e.Seal.RunID != name {
			t.Fatal("readonly evidence was reused across UIDs/rounds")
		}
		if err := judgeTransactionReadonly(e); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func transactionReadonlyOracleControl() transactionReadonlyEvidence {
	reverse := transactionRollbackOracleControl("R2")
	forward := transactionRollbackOracleControl("R2").After
	// Keep a separately completed forward to test the readonly execution chain.
	forward.Observation.Current.Direction = upgradetx.DirectionForward
	forward.Observation.Current.SourceSHA, forward.Observation.Current.TargetSHA = reverse.After.Observation.Current.TargetSHA, reverse.After.Observation.Current.SourceSHA
	forward.Observation.RunningSHA, forward.Observation.EffectiveSHA = forward.Observation.Current.TargetSHA, forward.Observation.Current.TargetSHA
	forward.Historical.RecordedPreviousSHA = forward.Observation.Current.SourceSHA
	forward.Observation.Current.Handle.CutID, forward.Observation.Current.Handle.Generation, forward.Observation.Current.SourceGeneration = strings.Repeat("2", 32), 2, 1
	for _, r := range []*upgradetx.Receipt{&forward.Observation.Current.Control, &forward.Observation.Current.Resources, &forward.Observation.Current.Released} {
		r.RunningSHA, r.Stamp.Attempt = forward.Observation.Current.TargetSHA, forward.Observation.Current.Handle
		r.Census.Stamp = r.Stamp
		r.Census.Resources = slices.Clone(r.Census.Resources)
		for i := range r.Census.Resources {
			r.Census.Resources[i].Generation = 2
		}
	}
	forward.Observation.Census = forward.Observation.Current.Released.Census
	forward.Observation.Census.Stamp.Nonce, forward.Observation.Census.Stamp.At = forward.Observation.Nonce, forward.Observation.At
	second := transactionRollbackOracleControl("R2").After
	second.Observation.Current.Direction = upgradetx.DirectionForward
	second.Observation.Current.SourceSHA, second.Observation.Current.TargetSHA = forward.Observation.Current.SourceSHA, forward.Observation.Current.TargetSHA
	second.Observation.RunningSHA, second.Observation.EffectiveSHA = second.Observation.Current.TargetSHA, second.Observation.Current.TargetSHA
	second.Observation.Current.Handle.TxID = strings.Repeat("a", 32)
	second.Observation.Current.Handle.Generation, second.Observation.Current.SourceGeneration = 4, 3
	for _, r := range []*upgradetx.Receipt{&second.Observation.Current.Control, &second.Observation.Current.Resources, &second.Observation.Current.Released} {
		r.RunningSHA, r.Stamp.Attempt = second.Observation.Current.TargetSHA, second.Observation.Current.Handle
		r.Census.Stamp = r.Stamp
		r.Census.Resources = slices.Clone(r.Census.Resources)
		for i := range r.Census.Resources {
			r.Census.Resources[i].Generation = 4
		}
	}
	second.Observation.Census = second.Observation.Current.Released.Census
	second.Observation.Census.Stamp.Nonce, second.Observation.Census.Stamp.At = second.Observation.Nonce, second.Observation.At
	key := forward.Observation.Current.Handle.Key
	grant := upgradetx.Grant{Handle: forward.Observation.Current.Handle, SourceSHA: forward.Observation.Current.SourceSHA, TargetSHA: forward.Observation.Current.TargetSHA}
	canonical := "/test-only/canonical"
	artifact := upgradeartifact.Receipt{Grant: grant, CanonicalPath: canonical, SourcePath: "/test-only/slots/" + grant.SourceSHA, TargetPath: "/test-only/slots/" + grant.TargetSHA, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: strings.Repeat("c", 64)}
	owner, coldOwner := transactionTestOwner(2), transactionTestOwner(3)
	e := transactionReadonlyEvidence{Seal: reverse.Seal, Case: "user-agent", Key: key, CanonicalPath: canonical, CanonicalBeforeSHA: strings.Repeat("d", 64), CanonicalAfterSHA: strings.Repeat("d", 64), CanonicalMode: 0555, SourceMode: 0555, TargetMode: 0555, JournalPath: "/test-only/journal", WriterIdentity: "original-daemon", Artifact: artifact, SelectedGrant: grant, Forward: forward, Reverse: reverse.After, Cold: reverse.After, Second: second, ForwardProcSHA: grant.TargetSHA, ReverseProcSHA: grant.SourceSHA, ColdProcSHA: grant.SourceSHA, SecondProcSHA: grant.TargetSHA, ColdStartedFrom: canonical, ForwardOwner: owner, ColdOwner: coldOwner, AlienCode: upgradetx.CodeRejected, SelectorsBeforeAlien: []string{grant.SourceSHA}, SelectorsAfterAlien: []string{grant.SourceSHA}}
	e.AlienGrant = grant
	e.AlienGrant.Handle.TxID = strings.Repeat("f", 32)
	e.Stage = transactionP23CLI{Argv: []string{"upgrade", "stage", "--grant", "grant.json", "--source", "source", "--candidate", "candidate", "--receipt-out", "receipt.json"}, Owner: owner, BinarySHA: reverse.Seal.BinarySHA, BeginNS: 10, EndNS: 20}
	e.ColdCLI = transactionP23CLI{ExecutedPath: canonical, Argv: []string{"agent", "--session", key.SID, "--nid", key.ConfiguredNID}, Owner: coldOwner, BinarySHA: e.CanonicalBeforeSHA, BeginNS: 30, EndNS: 40}
	return e
}

// origin: hot-update S8 plan §5.2 / M02/M16
func TestTransactionReadonlyRejectsCrossGrantAndColdDrift(t *testing.T) {
	if err := judgeTransactionReadonly(transactionReadonlyOracleControl()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*transactionReadonlyEvidence){
		"M16-cross-grant":             func(e *transactionReadonlyEvidence) { e.Artifact.Grant.Handle.TxID = e.AlienGrant.Handle.TxID },
		"M16-alien-adopt":             func(e *transactionReadonlyEvidence) { e.AlienAccepted = true },
		"cold-newest-bytes":           func(e *transactionReadonlyEvidence) { e.ColdProcSHA = e.Artifact.TargetSHA },
		"stage-second-writer":         func(e *transactionReadonlyEvidence) { e.StageAfter.Revision++ },
		"fixture-chmod":               func(e *transactionReadonlyEvidence) { e.CanonicalMode = 0755 },
		"second-upgrade-wrong-source": func(e *transactionReadonlyEvidence) { e.Second.Observation.Current.SourceSHA = e.Artifact.TargetSHA },
	} {
		t.Run(name, func(t *testing.T) {
			e := transactionReadonlyOracleControl()
			mutate(&e)
			if judgeTransactionReadonly(e) == nil {
				t.Fatal("readonly product-chain defect accepted")
			}
		})
	}
}
