package jsstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type failFirstPlacementStreamLookup struct {
	jetstream.JetStream
	failed bool
}

func (j *failFirstPlacementStreamLookup) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if !j.failed {
		j.failed = true
		return nil, errors.New("injected placement-canary lookup failure")
	}
	return j.JetStream.Stream(ctx, name)
}

// TestExternalReviewOfflineAssignmentDoesNotProveCurrentPlacement models a 3->2
// retire followed by a 2->3 regrow. tether does not peer-remove the retired JS
// member, so StreamInfo continues to list that dead assignment. Counting it makes
// the G69 gate declare a new R3 asset placeable before the new joiner has joined the
// JS meta group—the precise post-grow refusal the gate claims to measure.
func TestExternalReviewOfflineAssignmentDoesNotProveCurrentPlacement(t *testing.T) {
	info := &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{
		Replicas: []*jetstream.PeerInfo{
			{Name: "retired-peer", Offline: true, Current: false},
		},
	}}
	if got := AssignedReplicas(info); got != 1 {
		t.Fatalf("offline retired peer counted as current placement evidence: got %d assigned, want 1 live assignment", got)
	}
}

// TestExternalRereviewPlacementCanaryDoesNotDeletePreexistingStream protects
// against treating either a conventional fixed name or a matching config as an
// ownership proof. An operator-controlled stream can have the exact canary
// config and contain data; the probe never publishes, so a non-empty stream is
// conclusive evidence that it is not a disposable abandoned probe artifact.
func TestExternalRereviewPlacementCanaryDoesNotDeletePreexistingStream(t *testing.T) {
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

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: PlacementCanaryStreamName, Subjects: []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy, MaxMsgs: 1, MaxAge: time.Minute,
		Storage: jetstream.MemoryStorage, Replicas: 1,
	}); err != nil {
		t.Fatalf("fixture stream: %v", err)
	}
	if _, err := js.Publish(ctx, placementCanarySubject, []byte("must survive")); err != nil {
		t.Fatalf("fixture publish: %v", err)
	}

	_ = ProbeMetaCanPlace(ctx, js, 1)

	stream, err := js.Stream(ctx, PlacementCanaryStreamName)
	if err != nil {
		t.Fatalf("placement probe deleted a pre-existing stream it did not own: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 || len(info.Config.Subjects) != 1 ||
		info.Config.Subjects[0] != placementCanarySubject {
		t.Fatalf("placement probe replaced or mutated operator stream: config=%+v state=%+v",
			info.Config, info.State)
	}
}

// TestExternalRereviewPlacementCanaryDoesNotDeleteEmptyStreamWithConsumer
// covers operator state that State.Msgs cannot see. An empty stream may own
// durable consumers and is not disposable. A missing metadata marker does not
// prove it is legacy probe residue; deleting it also deletes those consumers.
func TestExternalRereviewPlacementCanaryDoesNotDeleteEmptyStreamWithConsumer(t *testing.T) {
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

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: PlacementCanaryStreamName, Subjects: []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy, MaxMsgs: 1, MaxAge: time.Minute,
		Storage: jetstream.MemoryStorage, Replicas: 1,
	})
	if err != nil {
		t.Fatalf("fixture stream: %v", err)
	}
	if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "operator-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		FilterSubject: placementCanarySubject,
	}); err != nil {
		t.Fatalf("fixture consumer: %v", err)
	}

	if err := ProbeMetaCanPlace(ctx, js, 1); err == nil {
		t.Fatal("the probe reported success by reclaiming an unmarked stream with operator-owned consumers")
	}
	stream, err = js.Stream(ctx, PlacementCanaryStreamName)
	if err != nil {
		t.Fatalf("placement probe deleted the empty operator stream and its durable consumer: %v", err)
	}
	if _, err := stream.Consumer(ctx, "operator-consumer"); err != nil {
		t.Fatalf("placement probe deleted the operator's durable consumer: %v", err)
	}
}

// TestExternalRereviewPlacementCanaryLookupErrorCannotBypassStateProtection
// covers the error side of the initial lookup. An unknown lookup result is not
// "stream absent". If the probe continues, CreateStream can idempotently return
// an existing marked stream; its messages/consumers must not be deleted merely
// because the second Info call succeeded.
func TestExternalRereviewPlacementCanaryLookupErrorCannotBypassStateProtection(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	realJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg := jetstream.StreamConfig{
		Name: PlacementCanaryStreamName, Subjects: []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy, MaxMsgs: 1, MaxAge: time.Minute,
		Storage: jetstream.MemoryStorage, Replicas: 1,
		Metadata: map[string]string{placementCanaryOwnerKey: placementCanaryOwnerVal},
	}
	if _, err := realJS.CreateStream(ctx, cfg); err != nil {
		t.Fatalf("fixture stream: %v", err)
	}
	if _, err := realJS.Publish(ctx, placementCanarySubject, []byte("must survive lookup uncertainty")); err != nil {
		t.Fatalf("fixture publish: %v", err)
	}

	wrapped := &failFirstPlacementStreamLookup{JetStream: realJS}
	probeErr := ProbeMetaCanPlace(ctx, wrapped, 1)
	stream, err := realJS.Stream(ctx, PlacementCanaryStreamName)
	if err != nil {
		t.Fatalf("the probe deleted a populated canary after its initial lookup failed (probe error %v): %v",
			probeErr, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("the probe mutated the populated stream after lookup uncertainty: state=%+v", info.State)
	}
	if probeErr == nil {
		t.Fatal("the probe treated an unknown initial lookup as absence and reported placement from an old stream")
	}
}
