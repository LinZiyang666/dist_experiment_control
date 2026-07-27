package broker

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestExternalReviewAlertForwardOutcomeIsCounted exercises the production alert sink rather
// than the source-text tripwire in forward_metrics_test.go. A broker with no apply responder
// gets the normal retriable not_leader outcome; that attempt still has to be observable because
// the sink deliberately discards its error and otherwise fails silently forever.
func TestExternalReviewAlertForwardOutcomeIsCounted(t *testing.T) {
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	b := &Broker{selfID: "external-review-node"}
	b.AttachAlertSink(NewForwarderWithLimit(nc, 25*time.Millisecond, 0))
	b.alertSink(true)

	got := b.forwards.snapshot()
	if got[VerbAlertSignal+"/not_leader"] != 1 {
		t.Fatalf("silent alert forward outcome was not counted: got %v", got)
	}
}
