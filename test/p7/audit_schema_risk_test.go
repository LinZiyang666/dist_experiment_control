package p7_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestAuditProcExitMatchesPublishedSchema(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	runExec(t, nc, "lab", pub, "lab-1", []string{"sh", "-c", "exit 7"})

	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{proto.SubjAuditProc("lab")},
	})
	if err != nil {
		t.Fatal(err)
	}
	it, err := cons.Messages()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := it.Next()
		if err != nil {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(msg.Data(), &raw); err != nil {
			t.Fatal(err)
		}
		if raw["kind"] != "exit" {
			continue
		}
		var got schema.AuditProc
		if err := json.Unmarshal(msg.Data(), &got); err != nil {
			t.Fatal(err)
		}
		if got.RC == nil || *got.RC != 7 {
			t.Fatalf("audit.proc exit must encode rc=7 per schema; decoded %+v from %s", got, string(msg.Data()))
		}
		return
	}
	t.Fatal("audit.proc exit not observed")
}
