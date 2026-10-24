package brokerupgradeport

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// A carried adjunct is admitted only as the accepted, still unsettled
// activation of this broker's Key whose target bytes are running; anything
// else refuses before the import takes a descriptor.
// origin: hot-update S8 plan §3.3 / §5.1 / §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11
func TestInheritedCutMustBeThisBrokersAcceptedActivation(t *testing.T) {
	f := newCutFixture(t)
	state := importedTarget(t, f)
	if err := VerifyInherited(f.journal, f.key, state, f.grant.TargetSHA); err != nil {
		t.Fatalf("the accepted activation was refused: %v", err)
	}
	type defect = func(*upgradecut.State, *upgradetx.Key, *string, *upgradetx.Attempt)
	for name, mutate := range map[string]defect{
		"another broker key": func(_ *upgradecut.State, k *upgradetx.Key, _ *string, _ *upgradetx.Attempt) {
			k.ConfigPath = "/etc/other.yaml"
		},
		"source bytes running": func(_ *upgradecut.State, _ *upgradetx.Key, r *string, _ *upgradetx.Attempt) { *r = f.grant.SourceSHA },
		"another cut": func(s *upgradecut.State, _ *upgradetx.Key, _ *string, _ *upgradetx.Attempt) {
			s.Attempt.CutID = strings.Repeat("8", 32)
		},
		"a newer handle": func(s *upgradecut.State, _ *upgradetx.Key, _ *string, _ *upgradetx.Attempt) { s.Attempt.Revision++ },
		"another source image": func(s *upgradecut.State, _ *upgradetx.Key, _ *string, _ *upgradetx.Attempt) {
			s.SourceSHA = strings.Repeat("e", 64)
		},
		"no activation intent": func(_ *upgradecut.State, _ *upgradetx.Key, _ *string, a *upgradetx.Attempt) {
			a.CutStage = upgradetx.CutPristine
		},
		"already completed": func(_ *upgradecut.State, _ *upgradetx.Key, _ *string, a *upgradetx.Attempt) {
			a.Phase = upgradetx.PhaseCompleted
		},
		"cancelled before exec": func(_ *upgradecut.State, _ *upgradetx.Key, _ *string, a *upgradetx.Attempt) {
			a.Phase = upgradetx.PhaseCancelled
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newCutFixture(t)
			state := importedTarget(t, f)
			key, running := f.key, f.grant.TargetSHA
			mutate(&state, &key, &running, &f.journal.view.Historical.Forward)
			if err := VerifyInherited(f.journal, key, state, running); err == nil {
				t.Fatal("a carried adjunct that is not this broker's accepted activation was admitted")
			}
		})
	}
}

// The importer compares the carried authority map with the store it opened:
// a session recreated (or removed, or added) across the cut does not match,
// a store it cannot read is an unknown incarnation, and a runtime that
// imported no cut has nothing to compare.
// origin: hot-update S8 plan §6.1 / §6.2 / §8.1 / §11.2 P17; S8 BROKER-EXPORTER R-11
func TestInheritedAuthorityMapMustMatchTheOpenedStore(t *testing.T) {
	hash, _ := testPINHash()
	for name, tc := range map[string]struct {
		mutate func(*testing.T, *cutFixture)
		match  bool
	}{
		"unchanged": {func(*testing.T, *cutFixture) {}, true},
		"recreated": {func(t *testing.T, f *cutFixture) {
			if _, err := f.db.Exec(`DELETE FROM sessions WHERE sid = 'lab'`); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Create(f.db, "lab", "lab", "owner-fp", hash, time.Now().UTC().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
		}, false},
		"added": {func(t *testing.T, f *cutFixture) {
			if _, err := session.Create(f.db, "zeta", "zeta", "owner-fp", hash, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		}, false},
		"store unreadable": {func(t *testing.T, f *cutFixture) { _ = f.db.Close() }, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := newCutFixture(t)
			importedTarget(t, f)
			tc.mutate(t, f)
			if err := MatchInherited(f.runtime); (err == nil) != tc.match {
				t.Fatalf("match=%t: %v", tc.match, err)
			}
		})
	}
	f := newCutFixture(t)
	if err := MatchInherited(f.runtime); err != nil {
		t.Fatalf("a runtime that imported no cut refused: %v", err)
	}
	if err := MatchInherited(nil); err != nil {
		t.Fatalf("another runtime refused: %v", err)
	}
}

// origin: S8 internal review cluster-roll F8
func TestInheritedBrokerLeaseNamesTheOriginalParent(t *testing.T) {
	f := newCutFixture(t)
	state := importedTarget(t, f)
	f.journal.view.Historical.Grant.ParentTxID = strings.Repeat("a", 32)
	state.ParentToken = f.journal.view.Historical.Grant.ParentTxID
	if err := VerifyInherited(f.journal, f.key, state, f.grant.TargetSHA); err != nil {
		t.Fatal("original parent refused:", err)
	}
	state.ParentToken = strings.Repeat("b", 32)
	if VerifyInherited(f.journal, f.key, state, f.grant.TargetSHA) == nil {
		t.Fatal("another parent's lease was admitted")
	}
}
