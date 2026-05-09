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
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startHistoryJSNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded JS nats-server not ready")
	}
	waitHistoryJSReady(t, ns)
	return ns.ClientURL()
}

func waitHistoryJSReady(t *testing.T, ns *natsserver.Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ns.JetStreamEnabled() && ns.JetStreamIsCurrent() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("JS not ready after 2s")
}

func TestHistoryKindTailCountsFilteredEntries(t *testing.T) {
	url := startHistoryJSNATS(t)
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

	// Pin the contract from P7 review F1: history --kind call -n 100
	// must return ALL 50 audit.call entries from the 50 exec runs,
	// not the ~33 the LastSeq-N+1 + FilterSubjects combination
	// over-truncated to. The fix is the runHistoryFilteredTail helper
	// in history.go (DeliverAllPolicy + ring buffer over the
	// filtered stream); this test pins that helper's behavior.
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
	if err := runHistoryFilteredTail(context.Background(), cons, &out, 100, 200*time.Millisecond); err != nil {
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
