package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// TestEffectiveDialURLsIncludesVerifiedSeeds (Stage-C M1) — a verified SeedBundle endpoint must be a
// LIVE dial tier (between the roster and the seed floor), so a runtime endpoint rotation reaches the
// dialer (the SeedBundle is not write-only dead state).
func TestEffectiveDialURLsIncludesVerifiedSeeds(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.rosterMu.Lock()
	a.pinAccount = pub
	a.rosterMu.Unlock()
	a.adoptManifest(buildTestRoster(t, seed, pub, 1, time.Hour), buildTestSeeds(t, seed, pub, 1, time.Hour, "wss://seedB.example.com:443"))
	if got := a.effectiveDialURLs(); !strings.Contains(got, "seedB.example.com") {
		t.Fatalf("a verified seed endpoint must be a live dial tier (M1): %q", got)
	}
}

func buildTestSeeds(t *testing.T, seed []byte, pub string, gen uint64, expiresIn time.Duration, eps ...string) *proto.SeedBundle {
	t.Helper()
	if len(eps) == 0 {
		eps = []string{"wss://b1.example.com:443"}
	}
	s, err := clusterroster.BuildSeeds(seed, pub, gen, eps, "https://c/c.json",
		rosterTestBase.Format(time.RFC3339Nano), rosterTestBase.Add(expiresIn).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAdoptDecisionRosterAndSeeds(t *testing.T) {
	seed, pub := mustAccount(t)
	next, ok := AdoptDecision(RosterState{Pin: pub},
		buildTestRoster(t, seed, pub, 10, time.Hour), buildTestSeeds(t, seed, pub, 20, time.Hour),
		"nats://seed:4222", rosterTestBase)
	if !ok || next.RosterGen != 10 || next.SeedGen != 20 {
		t.Fatalf("both children must adopt: ok=%v rg=%d sg=%d", ok, next.RosterGen, next.SeedGen)
	}
}

func TestAdoptDecisionRejectsForgedSeed(t *testing.T) {
	seed, pub := mustAccount(t)
	otherSeed, _ := mustAccount(t) // a DIFFERENT account signs the seed bundle (claims pub → sig fail)
	next, ok := AdoptDecision(RosterState{Pin: pub},
		buildTestRoster(t, seed, pub, 5, time.Hour), buildTestSeeds(t, otherSeed, pub, 6, time.Hour),
		"nats://seed:4222", rosterTestBase)
	if !ok {
		t.Fatal("the valid roster should still adopt")
	}
	if next.SeedGen != 0 {
		t.Fatalf("a forged seed bundle must be rejected, seed_gen=%d", next.SeedGen)
	}
}

func TestAdoptDecisionSeedGenRollback(t *testing.T) {
	seed, pub := mustAccount(t)
	next, ok := AdoptDecision(RosterState{Pin: pub, SeedGen: 10}, nil, buildTestSeeds(t, seed, pub, 9, time.Hour),
		"nats://seed:4222", rosterTestBase)
	if ok || next.SeedGen != 10 {
		t.Fatalf("a seed-generation rollback must be rejected: ok=%v sg=%d", ok, next.SeedGen)
	}
}

func TestAdoptDecisionSeedsNeedPin(t *testing.T) {
	seed, pub := mustAccount(t)
	// pin=="" and no roster → a seed bundle cannot be verified (nothing to pin against).
	if _, ok := AdoptDecision(RosterState{}, nil, buildTestSeeds(t, seed, pub, 1, time.Hour), "nats://seed:4222", rosterTestBase); ok {
		t.Fatal("seeds must not adopt without a pin (no TOFU from seeds alone)")
	}
}

func TestRosterCacheSeparateFromStateJSON(t *testing.T) {
	home := t.TempDir()
	if err := WriteRosterCache(home, "lab", &RosterCache{PinAccountPub: "P", Generation: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(home, "agent", "lab", "state.json")); !os.IsNotExist(err) {
		t.Fatal("WriteRosterCache must NOT touch state.json (no lost-update of expose tokens)")
	}
	rc, err := ReadRosterCache(home, "lab")
	if err != nil || rc == nil || rc.PinAccountPub != "P" || rc.Generation != 3 {
		t.Fatalf("roster_cache.json round-trip failed: %+v %v", rc, err)
	}
}

func TestStateJSONRosterMigration(t *testing.T) {
	home := t.TempDir()
	ss := newStateStore(home, "lab") // legacy C1: cache lived inside state.json.Roster
	if err := ss.SetRosterCache(&RosterCache{PinAccountPub: "LEGACY", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	rc, err := ReadRosterCache(home, "lab") // no roster_cache.json yet → migrate
	if err != nil || rc == nil || rc.PinAccountPub != "LEGACY" || rc.Generation != 7 {
		t.Fatalf("migration from state.json.Roster failed: %+v %v", rc, err)
	}
}
