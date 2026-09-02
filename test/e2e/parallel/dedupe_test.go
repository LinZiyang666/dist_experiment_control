package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func mkUnit(matrix, pkg, tags string, race bool, timeo string) unit {
	return unit{matrix: matrix, pkg: pkg, tags: tags, race: race, timeo: timeo,
		baseArgs: []string{"test", "-count=1", "-timeout", timeo},
		name:     strings.TrimSuffix(strings.TrimPrefix(matrix, "Test"), "Matrix") + ":" + strings.TrimSuffix(strings.TrimPrefix(pkg, "./"), "/...")}
}

func sameHash(string, string, bool) (string, error) { return "deadbeef", nil }

// folds returns only the notes that record a fold (the kept-apart notes carry a reason instead).
func folds(notes []foldNote) []foldNote {
	var out []foldNote
	for _, n := range notes {
		if n.reason == "" {
			out = append(out, n)
		}
	}
	return out
}

func TestDedupeFoldsIdenticalClosuresToTheFirstNameWithTheLongestTimeout(t *testing.T) {
	units := []unit{
		mkUnit("TestD5Matrix", "./internal/cluster/...", "d5_integration", true, "300s"),
		mkUnit("TestD1Matrix", "./internal/cluster/...", "", true, "240s"),
		mkUnit("TestD3Matrix", "./internal/cluster/...", "", true, "300s"),
		mkUnit("TestD1Matrix", "./test/cluster/...", "", true, "240s"),
	}
	kept, notes, err := dedupeUnits(units, sameHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 2 {
		t.Fatalf("kept %d units, want 2: %+v", len(kept), kept)
	}
	if kept[0].name != "D1:internal/cluster" || kept[0].timeo != "300s" || !strings.Contains(strings.Join(kept[0].baseArgs, " "), "-timeout 300s") {
		t.Fatalf("kept unit drifted: %+v", kept[0])
	}
	f := folds(notes)
	if len(f) != 1 || f[0].kept != "D1:internal/cluster" || len(f[0].dropped) != 2 {
		t.Fatalf("notes drifted: %+v", notes)
	}
	if strings.Join(f[0].droppedMatrices, ",") != "TestD3Matrix,TestD5Matrix" {
		t.Fatalf("a fold must record which matrices it folded away (coverage self-check input): %v", f[0].droppedMatrices)
	}
}

func TestDedupeKeepsUnitsWhoseClosuresDiffer(t *testing.T) {
	units := []unit{
		mkUnit("TestD4Matrix", "./internal/broker/...", "", true, "300s"),
		mkUnit("TestPhaseFluidityMatrix", "./internal/broker/...", "phasefluidity_integration", true, "120s"),
	}
	byTags := func(pkg, tags string, race bool) (string, error) { return "h-" + tags, nil }
	kept, notes, err := dedupeUnits(units, byTags)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 2 || len(folds(notes)) != 0 {
		t.Fatalf("differing closures must not fold: kept=%d notes=%+v", len(kept), notes)
	}
}

func TestDedupeNeverFoldsAcrossRaceRunFilterExtraOrWholeUnits(t *testing.T) {
	a := mkUnit("TestAMatrix", "./p/...", "", true, "60s")
	b := mkUnit("TestBMatrix", "./p/...", "", false, "60s") // race differs
	c := mkUnit("TestCMatrix", "./p/...", "", true, "60s")
	c.runFilter, c.hasRun = "^TestX$", true // selection differs
	d := mkUnit("TestDMatrix", "./p/...", "", true, "60s")
	d.extra = "-short" // semantics differ
	w := unit{matrix: "TestWMatrix", name: "TestWMatrix", whole: true}
	w2 := unit{matrix: "TestWMatrix", name: "TestWMatrix", whole: true}
	kept, notes, err := dedupeUnits([]unit{a, b, c, d, w, w2}, sameHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 6 || len(notes) != 0 {
		t.Fatalf("nothing here is foldable: kept=%d notes=%+v", len(kept), notes)
	}
}

// origin: test-system-overhaul internal review L4-F4
func TestDedupeAndInventoryKeySeePassThroughFlags(t *testing.T) {
	full := parseGoTestArgs("TestAMatrix", []string{"go", "test", "-race", "-count=1", "./p/..."})
	short := parseGoTestArgs("TestBMatrix", []string{"go", "test", "-race", "-short", "-count=1", "./p/..."})
	counted := parseGoTestArgs("TestCMatrix", []string{"go", "test", "-race", "-count=3", "./p/..."})
	verbose := parseGoTestArgs("TestDMatrix", []string{"go", "test", "-race", "-v", "-count=1", "./p/..."})
	if len(full) != 1 || len(short) != 1 || len(counted) != 1 || len(verbose) != 1 {
		t.Fatalf("parse: %d %d %d %d", len(full), len(short), len(counted), len(verbose))
	}
	if full[0].extra != "" || short[0].extra != "-short" || counted[0].extra != "-count=3" || verbose[0].extra != "-v" {
		t.Fatalf("extra drifted: full=%q short=%q counted=%q verbose=%q", full[0].extra, short[0].extra, counted[0].extra, verbose[0].extra)
	}
	// The inventory key: a matrix switching to -short, or to -v (testing.Verbose() is branched on
	// in the tree — external review suggestion 2), must be visible to the golden.
	fullKey, shortKey := unitKey(full[0]), unitKey(short[0])
	fullKey = strings.Replace(fullKey, "TestAMatrix", "X", 1)
	shortKey = strings.Replace(shortKey, "TestBMatrix", "X", 1)
	if fullKey == shortKey {
		t.Error("-short does not change the inventory key: a matrix switching to -short is invisible to the golden")
	}
	verboseKey := strings.Replace(unitKey(verbose[0]), "TestDMatrix", "X", 1)
	if fullKey == verboseKey {
		t.Errorf("-v does not change the inventory key (%q): testing.Verbose() changes what a test does", verboseKey)
	}
	// The dedupe group: a -short unit must never fold with a full one — same binary, different run.
	kept, notes, err := dedupeUnits([]unit{full[0], short[0]}, sameHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 2 || len(folds(notes)) != 0 {
		t.Errorf("a -short unit was folded with a full one (kept=%d notes=%+v): same binary, different selection", len(kept), notes)
	}
}

func TestDedupeFailsOpenOnHashErrorAndSaysWhy(t *testing.T) {
	units := []unit{
		mkUnit("TestD1Matrix", "./internal/cluster/...", "", true, "240s"),
		mkUnit("TestD5Matrix", "./internal/cluster/...", "d5_integration", true, "300s"),
	}
	broken := func(pkg, tags string, race bool) (string, error) {
		if tags != "" {
			return "", errors.New("go list exploded")
		}
		return "h", nil
	}
	kept, notes, err := dedupeUnits(units, broken)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 2 || len(folds(notes)) != 0 {
		t.Fatalf("a hash error must keep both units: kept=%d notes=%+v", len(kept), notes)
	}
	if len(notes) != 1 || notes[0].kept != "D1:internal/cluster" || !strings.Contains(notes[0].reason, "go list exploded") {
		t.Fatalf("a kept-apart group must be reported with its reason: %+v", notes)
	}
}

// origin: test-system-overhaul internal review L4-F7
func TestDedupeReportsGroupsItKeptApartAndWhy(t *testing.T) {
	units := []unit{
		mkUnit("TestD4Matrix", "./internal/broker/...", "", true, "300s"),
		mkUnit("TestD5Matrix", "./internal/broker/...", "d5_integration", true, "300s"),
		mkUnit("TestD1Matrix", "./internal/cluster/...", "", true, "300s"),
		mkUnit("TestD5Matrix", "./internal/cluster/...", "d5_integration", true, "300s"),
	}
	h := func(pkg, tags string, race bool) (string, error) {
		if strings.Contains(pkg, "cluster") && tags != "" {
			return "", errors.New("go list exploded")
		}
		return "h-" + tags, nil
	}
	_, notes, err := dedupeUnits(units, h)
	if err != nil {
		t.Fatal(err)
	}
	var apart []string
	for _, n := range notes {
		if n.dropped == nil {
			apart = append(apart, n.kept+": "+n.reason)
		}
	}
	slices.Sort(apart)
	if len(apart) != 2 || !strings.Contains(apart[0], "go list exploded") || !strings.Contains(apart[1], "closure differs") {
		t.Fatalf("kept-apart groups must be reported with a reason, got %v", apart)
	}
}

// origin: test-system-overhaul internal review L4-F8
func TestFoldNotesCarryTheDroppedMatricesForTheCoverageCheck(t *testing.T) {
	units := []unit{
		mkUnit("TestD1Matrix", "./internal/cluster/...", "", true, "300s"),
		mkUnit("TestOnlyClusterMatrix", "./internal/cluster/...", "", true, "300s"),
	}
	kept, notes, err := dedupeUnits(units, sameHash)
	if err != nil {
		t.Fatal(err)
	}
	covered := coveredMatrices(kept, notes)
	if !covered["TestOnlyClusterMatrix"] || !covered["TestD1Matrix"] {
		t.Fatalf("a matrix folded away entirely must still count as represented, or a correct fold blocks the gate: %v", covered)
	}
	if kept, _, _ := dedupeUnits(units, sameHash); coveredMatrices(kept, nil)["TestOnlyClusterMatrix"] {
		t.Fatal("control: without the notes the folded matrix is NOT covered — the notes are load-bearing")
	}
}

// origin: test-system-overhaul internal review L4-F6
func TestClosureHashArgsCarryTheRaceBuildTag(t *testing.T) {
	args := goListArgs("./internal/cluster/...", "d5_integration", true)
	if !slices.Contains(args, "-race") || !slices.Contains(args, "-tags") || !slices.Contains(args, "d5_integration") {
		t.Fatalf("a -race group must hash the -race closure under its tags (shard.go's listPackageTests already does): %v", args)
	}
	if plain := goListArgs("./internal/cluster/...", "", false); slices.Contains(plain, "-race") || slices.Contains(plain, "-tags") {
		t.Fatalf("a no-race, no-tags group must hash the plain closure: %v", plain)
	}
}

// origin: test-system-overhaul internal review L4-F5
func TestWholeUnitEnvCarriesShuffleThroughGOFLAGS(t *testing.T) {
	got := wholeUnitEnv([]string{"PATH=/x", "GOFLAGS=-mod=mod"}, "on")
	var goflags string
	for _, kv := range got {
		if strings.HasPrefix(kv, "GOFLAGS=") {
			goflags = kv
		}
	}
	if goflags != "GOFLAGS=-mod=mod -shuffle=on" {
		t.Fatalf("GOFLAGS = %q; whole-matrix children must inherit -shuffle without losing existing GOFLAGS", goflags)
	}
	if fresh := wholeUnitEnv([]string{"PATH=/x"}, "17"); len(fresh) != 2 || fresh[1] != "GOFLAGS=-shuffle=17" {
		t.Fatalf("no existing GOFLAGS: got %v", fresh)
	}
	if n := len(wholeUnitEnv([]string{"PATH=/x"}, "")); n != 1 {
		t.Fatalf("no -shuffle must add nothing, got %d entries", n)
	}
}

// TestClosureHashDistinguishesTagSets runs the REAL hasher against the tree: the two facts the fold
// rests on. internal/cluster builds identically with and without d5_integration (the redundancy
// the D matrices carry); internal/broker does NOT build identically under phasefluidity_integration
// (the tag adds a file), which is exactly the pair a static "known identical" table would have got
// wrong.
func TestClosureHashDistinguishesTagSets(t *testing.T) {
	plain, err := goListClosureHash("./internal/cluster/...", "", true)
	if err != nil {
		t.Fatal(err)
	}
	d5, err := goListClosureHash("./internal/cluster/...", "d5_integration", true)
	if err != nil {
		t.Fatal(err)
	}
	if plain != d5 {
		t.Fatalf("internal/cluster closure differs under d5_integration (%s vs %s) — the D-matrix redundancy is real "+
			"after all, or a tag-gated file appeared in a shared package (TestIntegrationTagsAreLocalToTheirSuiteDir)", plain, d5)
	}
	broker, err := goListClosureHash("./internal/broker/...", "", true)
	if err != nil {
		t.Fatal(err)
	}
	fluid, err := goListClosureHash("./internal/broker/...", "phasefluidity_integration", true)
	if err != nil {
		t.Fatal(err)
	}
	if broker == fluid {
		t.Fatalf("internal/broker closure is the SAME under phasefluidity_integration (%s) — the hasher cannot see "+
			"tag-gated files, and the fold would silently drop the lifecycle drill", broker)
	}
}
