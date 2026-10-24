package upgradestore

import (
	"fmt"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv-txn F06
func TestSelectorReadbackSurvivesEveryJournalSyncWindow(t *testing.T) {
	for _, stage := range []string{"file", "directory"} {
		t.Run(stage, func(t *testing.T) {
			s, req := newFixture(t)
			s.options.Transition = upgradetx.Transition
			r := start(t, s, prepare(t, s, req))
			for _, e := range []upgradetx.Event{{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady}, {Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine}, {Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive}} {
				var err error
				r, err = s.Apply(attempt(r).Handle, e)
				if err != nil {
					t.Fatal(err)
				}
			}
			a := attempt(r)
			stamp := upgradetx.Stamp{Attempt: a.Handle, Producer: s.options.Owner, Nonce: strings.Repeat("8", 32), Sequence: 1, At: s.now().UnixNano(), ScopeDigest: r.Grant.ScopeDigest}
			resources := append([]upgradetx.Resource{}, a.Scope.Expected...)
			for i := range resources {
				resources[i].Generation, resources[i].Custody = a.Handle.Generation, "target"
			}
			receipt := upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, EvidenceDigest: strings.Repeat("e", 64), Census: upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: resources, ExpectedDigest: expectedDigest(a.Scope)}}
			event := upgradetx.Event{Receipt: receipt, Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive}
			hits := 0
			s.beforeSync = func(kind string) error {
				if kind == stage {
					hits++
					return syscall.ENOSPC
				}
				return nil
			}
			out, err := s.Apply(a.Handle, event)
			requireCode(t, err, upgradetx.CodeUnsettled)
			if out.Version != 0 || hits != 1 {
				t.Fatal("failed selector sync published success")
			}
			s = reopen(t, s)
			v, err := s.Load(s.options.Key, "")
			if err != nil {
				t.Fatal(err)
			}
			if stage == "file" {
				if !reflect.DeepEqual(v.Historical, r) {
					t.Fatal("pre-rename crash lost source selection")
				}
				out, err = s.Apply(a.Handle, event)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				out = v.Historical
				if _, err = s.Apply(a.Handle, event); err == nil {
					t.Fatal("post-rename stale callback acquired current")
				}
			}
			if out.EffectiveSHA != a.TargetSHA || !reflect.DeepEqual(attempt(out).Control, receipt) {
				t.Fatal("readback and selector were not atomic")
			}
		})
	}
}

// origin: S8 internal review rv-txn F10
func TestIdleGenerationSurvivesHistoryCollectionAndFirstResource(t *testing.T) {
	s, req := newFixture(t)
	s.options.SourceGeneration = 7
	s.options.Transition = upgradetx.Transition
	req.Scope.Expected = nil
	r := start(t, s, prepare(t, s, req))
	if r.Forward.SourceGeneration != 7 || r.Forward.Handle.Generation != 8 {
		t.Fatal("idle source ignored independent runtime generation")
	}
	err := s.locked(func(d *diskFile, e *diskEntry) error {
		// Exercise the actual count/age collector with terminal history, then
		// reopen without a current execution or any business resource rows.
		for i := 0; i < retainedHistory+1; i++ {
			a := r
			a.Grant.Handle.TxID = fmt.Sprintf("%032x", i+1)
			a.Forward.Handle = a.Grant.Handle
			a.Forward.Phase, a.Forward.Outcome = upgradetx.PhaseRejected, upgradetx.OutcomeRejected
			e.Archives = append(e.Archives, a)
			e.Metadata = append(e.Metadata, upgradetx.Metadata{TxID: a.Grant.Handle.TxID, Window: upgradetx.WindowClosed, ArchivedAt: s.now().Add(-historyAge - time.Hour).UnixNano()})
		}
		e.Current, e.AcceptedGrants = nil, nil
		e.Metadata = e.Metadata[1:]
		collect(e, s.now().UnixNano())
		if len(e.Archives) != retainedHistory {
			t.Fatal("history collection did not execute")
		}
		return s.write(d, true)
	})
	if err != nil {
		t.Fatal(err)
	}
	s = reopen(t, s)
	req.ClientNonce = "after-history-collection"
	r = start(t, s, prepare(t, s, req))
	if r.Forward.SourceGeneration != 8 || r.Forward.Handle.Generation != 9 {
		t.Fatal("history collection reset the idle generation")
	}
	resource := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "first-business-resource", Origin: "after-idle", Owner: s.options.Owner, Generation: r.Forward.SourceGeneration, State: "active", Custody: "source"}
	if upgradetx.ValidateResource(resource) != nil {
		t.Fatal("first resource inherited an unusable source generation")
	}
	if _, err = s.Apply(r.Forward.Handle, upgradetx.Event{Phase: upgradetx.PhaseStaged}); err != nil {
		t.Fatal(err)
	}
}
