package upgradeobserve

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv-fault F5
func TestReverseRegistryKeepsAdmissionOutsidePreparedScope(t *testing.T) {
	f := fixture()
	registry := newRegistry(t, f)
	late := f.resources[0]
	late.ID, late.Origin = "after-prepare", "late-admission"
	seq, err := registry.Begin(late)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Finish(seq, late); err != nil {
		t.Fatal(err)
	}
	f.resources = append(f.resources, late)
	next := f.attempt
	next.Handle.CutID, next.Handle.Generation, next.SourceGeneration = strings.Repeat("a", 32), 3, 2
	next.SourceSHA, next.TargetSHA = next.TargetSHA, next.SourceSHA
	next.Direction = upgradetx.DirectionReverse
	if err = Advance(registry, next, f.producer); err != nil {
		t.Fatalf("late admission blocked reverse: %v", err)
	}
	f.attempt = next
	for i := range f.resources {
		f.resources[i].Generation = 3
	}
	stamp := stampFor(t, f, seq)
	if _, err = registry.Snapshot(stamp); err != nil {
		t.Fatal(err)
	}
	f.resources = f.resources[:1]
	if _, err = registry.Snapshot(stamp); err == nil {
		t.Fatal("reverse forgot previously admitted identity")
	}
}
