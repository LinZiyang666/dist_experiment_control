package agent

import (
	"errors"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// ownerJournal records the owner a deferred opener was handed.
type ownerJournal struct {
	contractstub.Store
	owner upgradetx.Owner
}

// origin: hot-update S8 plan §3.1 / §5.1 / §5.2; S8 FIX-DESCRIBE (P19 request
// R3, composed with P19's OpenDeferredJournal). cmd names the registered
// journal; only agent.New knows the lineage whose ProcessOwner issues grants.
// The journal opens exactly once, under that owner, into a copy of the
// configured ports. A cold start whose journal cannot open keeps business
// first and runs without transaction capability; an inherited image refuses,
// because its carried cut can only be verified against that journal.
func TestRegisteredJournalOpensUnderTheLineageOwner(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("journal owner kernel identity requires Linux")
	}
	want, err := agentupgrade.ProcessOwner("lineage-instance")
	if err != nil {
		// origin: S10 internal review NO-SKIP-AS-PASS F3
		t.Fatalf("SETUP-RED: NOT_RUN pending S8 P14: kernel process identity unavailable: %v", err)
	}
	agentWith := func(ports *UpgradeDependencies, instance string) *Agent {
		return &Agent{cfg: Config{Upgrade: ports, Logger: testharness.SilentLog()}, instanceID: instance}
	}
	if opened, imported, err := openAndImportUpgrade(agentWith(nil, "lineage-instance"), nil); opened != nil || imported != nil || err != nil {
		t.Fatal("an unregistered agent gained transaction ports")
	}
	existing := &UpgradeDependencies{Store: contractstub.Store{}, Open: func(upgradetx.Owner) (upgradetx.Store, error) {
		t.Fatal("a configured journal was reopened")
		return nil, nil
	}}
	if opened, _, err := openAndImportUpgrade(agentWith(existing, "lineage-instance"), nil); opened != existing || err != nil {
		t.Fatal("a configured journal was replaced")
	}
	opens := 0
	ports := &UpgradeDependencies{Key: upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "lab"}, Open: func(owner upgradetx.Owner) (upgradetx.Store, error) {
		opens++
		return ownerJournal{owner: owner}, nil
	}}
	opened, imported, err := openAndImportUpgrade(agentWith(ports, "lineage-instance"), nil)
	if err != nil || opens != 1 || opened == ports || opened.Open != nil || ports.Store != nil || opened.Key != ports.Key || imported != opened {
		t.Fatalf("journal not opened once into a copy: opens=%d err=%v", opens, err)
	}
	if store, ok := opened.Store.(ownerJournal); !ok || store.owner != want {
		t.Fatalf("journal owner is not this lineage's process owner: %+v want %+v", opened.Store, want)
	}
	failure := &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "corrupt"}
	for name, tc := range map[string]struct {
		ports    *UpgradeDependencies
		instance string
		cause    error
	}{
		"missing_lineage": {ports, "", nil},
		"journal_failure": {&UpgradeDependencies{Open: func(upgradetx.Owner) (upgradetx.Store, error) { return nil, failure }}, "lineage-instance", failure},
		"nil_journal":     {&UpgradeDependencies{Open: func(upgradetx.Owner) (upgradetx.Store, error) { return nil, nil }}, "lineage-instance", nil},
	} {
		before := opens
		if opened, imported, err := openAndImportUpgrade(agentWith(tc.ports, tc.instance), nil); opened != nil || imported != nil || err != nil {
			t.Fatalf("%s: a cold start did not run without transaction capability: %v", name, err)
		}
		_, _, err := openAndImportUpgrade(agentWith(tc.ports, tc.instance), &agentboot.Image{})
		if err == nil || tc.cause != nil && !errors.Is(err, tc.cause) {
			t.Fatalf("%s: an inherited image ran without the journal that accepted its cut: %v", name, err)
		}
		if name == "missing_lineage" && opens != before {
			t.Fatal("a missing lineage opened the journal under another identity")
		}
	}
}
