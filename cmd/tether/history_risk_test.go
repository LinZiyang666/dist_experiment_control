package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHistoryKindTailCountsFilteredEntries(t *testing.T) {
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
	if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		publishAuditForHistoryTest(t, js, proto.SubjAuditCall("lab"), map[string]any{
			"v": 1, "kind": "call", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "lab-1", "actor_fp": "SHA256:test",
			"verb": "exec", "ok": true, "i": i,
		})
		publishAuditForHistoryTest(t, js, proto.SubjAuditProc("lab"), map[string]any{
			"v": 1, "kind": "start", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "lab-1", "pid": fmt.Sprintf("p-%02d", i),
		})
		publishAuditForHistoryTest(t, js, proto.SubjAuditProc("lab"), map[string]any{
			"v": 1, "kind": "exit", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "node": "lab-1", "pid": fmt.Sprintf("p-%02d", i),
			"exit_code": 0,
		})
	}

	// Pin the history --kind ... -n N contract: 50 exec runs must
	// produce 50 audit.call entries that all show up here. The
	// runHistoryFilteredTail helper in history.go uses
	// DeliverAllPolicy + a ring buffer over the filtered stream so
	// the count is correct regardless of how many non-matching
	// messages interleave in the underlying stream.
	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{proto.SubjAuditCall("lab")},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	// Mirrors the real --kind path: matching count is unknown up front
	// (known=false), so the drain relies on per-message NumPending.
	if err := runHistoryFilteredTail(context.Background(), cons, &out, 100, false /*known*/, 0); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(out.String(), "CALL"); got != 50 {
		t.Fatalf("history --kind call -n 100 should return all 50 call entries; got %d\n%s", got, out.String())
	}
}

func publishAuditForHistoryTest(t *testing.T, js jetstream.JetStream, subject string, v map[string]any) {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, body); err != nil {
		t.Fatal(err)
	}
}
