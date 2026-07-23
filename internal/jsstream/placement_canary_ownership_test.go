package jsstream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestPlacementCanaryLeavesAnAuthoredLookalikeAlone closes the residual the round-4 re-review named but
// did not require closed: an EMPTY operator stream whose config matches the canary shape exactly. The
// emptiness check already prevents data loss, but shape alone still let the probe delete somebody else's
// stream object. The canary now stamps an ownership MARKER into stream metadata, and a stream that
// carries someone else's authorship is never ours regardless of how well its shape matches.
func TestPlacementCanaryLeavesAnAuthoredLookalikeAlone(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Exactly the canary's shape, EMPTY, but authored by someone else.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: PlacementCanaryStreamName, Subjects: []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy, MaxMsgs: 1, MaxAge: time.Minute,
		Storage: jetstream.MemoryStorage, Replicas: 1,
		Metadata: map[string]string{"owner": "ops-team"},
	}); err != nil {
		t.Fatalf("fixture stream: %v", err)
	}

	if err := ProbeMetaCanPlace(ctx, js, 1); err == nil {
		t.Fatal("the probe reported placement PROVEN while the canary name was held by a stream it does " +
			"not own — a false positive that also implies it did not verify anything")
	}
	if _, err := js.Stream(ctx, PlacementCanaryStreamName); err != nil {
		t.Fatalf("the probe deleted an empty stream authored by someone else: %v", err)
	}
}

// TestPlacementCanaryRefusesAMarkerlessLookalike is the round-5 correction of an earlier test of mine
// that asserted the OPPOSITE. I had allowed a marker-less, identically-shaped, empty stream to be deleted
// as "probably our own residue", justified by servers that might not echo stream metadata. The pinned
// server demonstrably does echo it, so that fallback protected nothing and licensed a destructive guess
// at an operator's stream (and, with a durable consumer on it, their consumer too). Fail closed instead:
// an unproven claim on the name wedges the probe until a human looks, which is recoverable — deleting is
// not.
func TestPlacementCanaryRefusesAMarkerlessLookalike(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Identical shape, empty, no consumers, and NO ownership marker.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: PlacementCanaryStreamName, Subjects: []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy, MaxMsgs: 1, MaxAge: time.Minute,
		Storage: jetstream.MemoryStorage, Replicas: 1,
	}); err != nil {
		t.Fatalf("fixture stream: %v", err)
	}

	err = ProbeMetaCanPlace(ctx, js, 1)
	if err == nil {
		t.Fatal("the probe claimed placement using a stream whose ownership it cannot prove")
	}
	if !strings.Contains(err.Error(), "remove or rename it") {
		t.Fatalf("the refusal does not tell an operator how to clear it: %v", err)
	}
	if _, serr := js.Stream(ctx, PlacementCanaryStreamName); serr != nil {
		t.Fatalf("the probe deleted a stream it could not prove it owned: %v", serr)
	}
}

// TestPlacementCanaryReclaimsItsOwnMarkedResidue is the other half: residue this probe actually left
// behind (a crash between create and cleanup) always carries the marker, so it MUST still be reclaimable
// — otherwise every later probe would fail on "name already in use" and wedge the join gate for good.
func TestPlacementCanaryReclaimsItsOwnMarkedResidue(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Residue exactly as this probe writes it, marker included.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: PlacementCanaryStreamName, Subjects: []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy, MaxMsgs: 1, MaxAge: time.Minute,
		Storage: jetstream.MemoryStorage, Replicas: 1,
		Metadata: map[string]string{placementCanaryOwnerKey: placementCanaryOwnerVal},
	}); err != nil {
		t.Fatalf("fixture residue: %v", err)
	}
	if err := ProbeMetaCanPlace(ctx, js, 1); err != nil {
		t.Fatalf("the probe could not reclaim its own abandoned canary, so the join gate would stay wedged "+
			"on a name it owns: %v", err)
	}
}
