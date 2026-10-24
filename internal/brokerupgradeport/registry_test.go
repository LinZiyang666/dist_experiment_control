package brokerupgradeport

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// importedTarget turns a cut fixture into the process that imported its cut:
// target bytes running, the activation intent journaled, and the adjunct's
// export scope naming one transfer created after the grant (a late identity
// the source admitted before it had a registry).
func importedTarget(t *testing.T, f *cutFixture) upgradecut.State {
	t.Helper()
	f.running = f.grant.TargetSHA
	attempt := &f.journal.view.Historical.Forward
	attempt.Phase, attempt.CutStage = upgradetx.PhaseActivating, upgradetx.CutPartialActive
	late := TrackerEntry{SID: "lab", TransferID: "push-2", EntryNonce: "nonce-2"}
	exported := f.facts
	exported.Trackers = append(append([]TrackerEntry(nil), f.facts.Trackers...), late)
	exported.Authorities, _ = SessionAuthorities(f.db)
	rows, err := censusRows(exported, f.owner, attempt.SourceGeneration, "source")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := exportScope(attempt.Scope, rows)
	if err != nil {
		t.Fatal(err)
	}
	f.facts.Trackers = exported.Trackers
	state := upgradecut.State{Attempt: attempt.Handle, SourceSHA: f.grant.SourceSHA, TargetSHA: f.grant.TargetSHA, Owner: f.owner,
		Authorities: exported.Authorities, Scope: scope}
	f.runtime.host.Inherited = &state
	return state
}

// The importer's registry is built from the accepted attempt's original scope;
// every identity the sealed export added (created after the grant) is
// registered as the admission it was, before any completion sample, and the
// runtime's samples then name that registry's sequence. Without that, the
// first snapshot refuses the late transfer as an identity nobody admitted.
// A cut that is not the journal's accepted attempt gets no registry.
// origin: hot-update S8 plan §2.2 / §4.2 / P03 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestImportedRegistryAdmitsTheExportedLateIdentities(t *testing.T) {
	f := newCutFixture(t)
	state := importedTarget(t, f)
	registry, err := CutRegistry(f.runtime, f.journal, state)
	if err != nil {
		t.Fatal(err)
	}
	sequence, ok := upgradeobserve.Sequence(registry)
	if !ok || sequence != 2 {
		t.Fatalf("late identity was not admitted exactly once: sequence=%d", sequence)
	}
	stamp := upgradetx.Stamp{Attempt: f.grant.Handle, Producer: f.owner, Nonce: strings.Repeat("4", 32), ScopeDigest: f.grant.ScopeDigest}
	o, err := f.runtime.observe(stamp, false)
	if err != nil || o.Census.Stamp.Sequence != sequence {
		t.Fatalf("target sample does not name the registry sequence: %d %v", o.Census.Stamp.Sequence, err)
	}
	census, err := registry.Snapshot(o.Census.Stamp)
	if err != nil || !census.Known || census.Admitting != 0 {
		t.Fatalf("completion snapshot refused the exported late identity: %+v %v", census, err)
	}
	found := false
	for _, row := range census.Resources {
		found = found || row.ID == "tracker:lab/push-2" && row.Origin == "entry:nonce-2"
	}
	if !found {
		t.Fatalf("snapshot lost the late transfer: %+v", census.Resources)
	}
	other := state
	other.Attempt.CutID = strings.Repeat("8", 32)
	if _, err := CutRegistry(f.runtime, f.journal, other); err == nil {
		t.Fatal("a cut the journal never accepted got a completion registry")
	}
}
