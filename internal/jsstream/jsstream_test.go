package jsstream

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestTransferTerminalCommittedCreatesNoConsumer protects the recovery query's server-side
// resource boundary. Recovery calls it once per staged row; an ordered consumer left behind
// on every call survives until its inactive threshold and can exhaust a stream's consumer
// limit during a large replay. The query therefore reads raw messages and must create
// NOTHING — this assertion is what stops it from quietly going back to a consumer.
//
// origin: prerelease audit external review R3-M2. Formerly named
// ...CleansUpItsScanConsumer, which described a cleanup the function no longer performs
// because it no longer creates the thing that needed cleaning up.
func TestTransferTerminalCommittedCreatesNoConsumer(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	const sid = "consumer-cleanup"
	if err := EnsureHistoryStream(ctx, js, sid, ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: time.Now().UTC(),
		Session: sid, Node: "n1", TransferID: "tid-cleanup", Tier: "a",
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, proto.SubjAuditTransfer(sid), payload); err != nil {
		t.Fatal(err)
	}
	committed, err := TransferTerminalCommitted(ctx, js, sid, rec.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("published terminal was not found")
	}
	stream, err := js.Stream(ctx, HistoryStreamName(sid))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Consumers != 0 {
		t.Fatalf("terminal lookup left %d ordered consumer(s) behind; want 0 after the scan returns", info.State.Consumers)
	}
}

func TestTransferTerminalCommittedFailsClosedOnUnreadableAudit(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	const sid = "unreadable-audit"
	if err := EnsureHistoryStream(ctx, js, sid, ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, proto.SubjAuditTransfer(sid), []byte("{")); err != nil {
		t.Fatal(err)
	}
	committed, err := TransferTerminalCommitted(ctx, js, sid, "tid-unknown")
	if err == nil || committed {
		t.Fatalf("unreadable transfer audit returned committed=%v err=%v; want unknown error", committed, err)
	}
	stream, err := js.Stream(ctx, HistoryStreamName(sid))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Consumers != 0 {
		t.Fatalf("failed scan left %d consumer(s), want 0", info.State.Consumers)
	}
}

// TestTransferTerminalCommittedReadsOnlyTransferAudit is the adversarial case, and it pins
// the COST property rather than only the answer.
//
// The per-session history stream carries `audit.>` — call, proc and port records share it
// with transfer records. A scan that walks raw sequences reads every one of those bodies,
// one round trip each, and its only early exit is the transfer's own start row. That row is
// absent exactly when audit publishing is failing, which is the same condition that keeps a
// ledger row staged — so the degraded state re-walked the entire stream on every reap pass,
// inline on the reconcile goroutine. Answer-only tests cannot see that; counting the
// server-side reads can.
//
// origin: prerelease audit external review R3-M1/R3-M2 follow-up.
func TestTransferTerminalCommittedReadsOnlyTransferAudit(t *testing.T) {
	url := startJSNATS(t)
	js := newJS(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const sid = "audit-noise"
	if err := EnsureHistoryStream(ctx, js, sid, ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	pub := func(subject string, body []byte) uint64 {
		t.Helper()
		ack, err := js.Publish(ctx, subject, body)
		if err != nil {
			t.Fatal(err)
		}
		return ack.Sequence
	}
	xfer := func(tid, kind string) uint64 {
		t.Helper()
		body, err := json.Marshal(schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Ts: time.Now().UTC(),
			Session: sid, Node: "n1", TransferID: tid, Tier: "a",
		})
		if err != nil {
			t.Fatal(err)
		}
		return pub(proto.SubjAuditTransfer(sid), body)
	}

	// The noise a real session generates between two transfers. 60 records is a rounding
	// error against a 1 GiB stream, and already enough to separate O(transfers) from
	// O(everything).
	var holes []uint64
	for i := 0; i < 20; i++ {
		holes = append(holes, pub(proto.SubjAuditCall(sid), []byte(`{"v":1,"kind":"call"}`)))
		pub(proto.SubjAuditProc(sid), []byte(`{"v":1,"kind":"proc"}`))
		pub(proto.SubjAuditPort(sid), []byte(`{"v":1,"kind":"port"}`))
	}
	// A DIFFERENT transfer that did finish — its terminal must not be mistaken for ours.
	xfer("tid-other", "start")
	xfer("tid-other", "complete")
	// Ours: started, never finished. This is the answer that makes the caller publish.
	xfer("tid-ours", "start")

	// Deleted sequences are normal in a limits stream; the walk must step over them
	// without reporting a read failure (which would retain the row forever).
	stream, err := js.Stream(ctx, HistoryStreamName(sid))
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range holes[:5] {
		if derr := stream.DeleteMsg(ctx, seq); derr != nil {
			t.Fatal(derr)
		}
	}

	gets := countStreamMsgGets(t, url)
	committed, err := TransferTerminalCommitted(ctx, js, sid, "tid-ours")
	if err != nil {
		t.Fatal(err)
	}
	if committed {
		t.Fatal("a transfer with a start and no terminal was reported as already committed; " +
			"the caller would drop the staged terminal and leave the transfer with none")
	}
	// Ours is one of four transfer records; the scan needs those plus the not-found that
	// ends it. Anything near the 60 noise records means the subject filter is gone and the
	// reap pass is back to reading the whole stream.
	if n := gets(); n > 8 {
		t.Fatalf("the scan issued %d STREAM.MSG.GET requests for a stream holding 4 transfer "+
			"records among 60 audit records: it is reading the whole stream, not the subject", n)
	}

	// And the same scan must find the terminal once it exists.
	xfer("tid-ours", "failed")
	switch committed, err = TransferTerminalCommitted(ctx, js, sid, "tid-ours"); {
	case err != nil:
		t.Fatal(err)
	case !committed:
		t.Fatal("the committed terminal was not found behind the session's other audit traffic")
	}
}

// TestTransferTerminalCommittedTreatsEitherTerminalAsCommitted pins the kind-agnostic rule.
// The invariant is EXACTLY ONE terminal per transfer, so a committed `complete` makes a
// staged `failed` a contradiction to DROP — not a different record to add. Matching only the
// staged row's own kind is precisely how a contradictory pair gets written, and a
// contradictory pair is worse than a dangling start: no consumer can tell which is true.
//
// origin: prerelease audit external review R3-M1.
func TestTransferTerminalCommittedTreatsEitherTerminalAsCommitted(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, tc := range []struct{ name, committedKind string }{
		{"complete already committed, failed staged", "complete"},
		{"failed already committed, complete staged", "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sid := "opposite-" + tc.committedKind
			if err := EnsureHistoryStream(ctx, js, sid, ReplicasSingle); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(schema.AuditTransfer{
				V: schema.AuditSchemaVersion, Kind: tc.committedKind, Verb: "push",
				Ts: time.Now().UTC(), Session: sid, Node: "n1", TransferID: "tid-x", Tier: "a",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := js.Publish(ctx, proto.SubjAuditTransfer(sid), body); err != nil {
				t.Fatal(err)
			}
			committed, err := TransferTerminalCommitted(ctx, js, sid, "tid-x")
			if err != nil {
				t.Fatal(err)
			}
			if !committed {
				t.Fatalf("a committed %q terminal did not count as committed; the caller would "+
					"publish the opposite terminal and leave two contradictory ones", tc.committedKind)
			}
		})
	}
}

// countStreamMsgGets returns a function reporting how many raw-message reads the server has
// been asked for since it was installed. It is a second connection on the same account, so
// it observes the request subject without perturbing the client under test.
func countStreamMsgGets(t *testing.T, url string) func() int {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	sub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.GET.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return func() int {
		t.Helper()
		if ferr := nc.Flush(); ferr != nil {
			t.Fatal(ferr)
		}
		n := 0
		for {
			if _, nerr := sub.NextMsg(200 * time.Millisecond); nerr != nil {
				return n
			}
			n++
		}
	}
}

// startJSNATS is a thin alias over testharness.StartJSNATS so existing
// call sites in this file (and tests inside this package historically)
// don't have to change. The shared implementation handles the
// JetStream-ready wait that prevents flakes under load.
func startJSNATS(t *testing.T) string { return testharness.StartJSNATS(t) }

func newJS(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func TestEnsureEventsStreamIsIdempotent(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx := context.Background()

	if err := EnsureEventsStream(ctx, js, ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	// Second call must succeed (already-exists path).
	if err := EnsureEventsStream(ctx, js, ReplicasSingle); err != nil {
		t.Fatalf("re-create: %v", err)
	}
}

func TestEnsureHistoryStreamCreatesPerSession(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx := context.Background()

	if err := EnsureHistoryStream(ctx, js, "lab", ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	if err := EnsureHistoryStream(ctx, js, "lab", ReplicasSingle); err != nil {
		t.Fatalf("re-create same sid: %v", err)
	}
	if err := EnsureHistoryStream(ctx, js, "prod", ReplicasSingle); err != nil {
		t.Fatal(err)
	}

	sids, err := ListHistorySIDs(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"lab": true, "prod": true}
	for _, s := range sids {
		delete(want, s)
	}
	if len(want) != 0 {
		t.Errorf("missing history streams: %v", want)
	}
}

func TestDeleteHistoryStreamIdempotent(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx := context.Background()

	if err := EnsureHistoryStream(ctx, js, "lab", ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	if err := DeleteHistoryStream(ctx, js, "lab"); err != nil {
		t.Fatal(err)
	}
	// Re-deleting a missing stream must not error — H.3 phase ②
	// must be safe to retry after a crash.
	if err := DeleteHistoryStream(ctx, js, "lab"); err != nil {
		t.Errorf("re-delete: %v", err)
	}
}

func TestSIDFromHistoryStream(t *testing.T) {
	cases := []struct {
		stream  string
		wantSID string
		wantOK  bool
	}{
		{"history-lab", "lab", true},
		{"history-lab-1", "lab-1", true},
		{"events", "", false},
		{"random", "", false},
	}
	for _, c := range cases {
		gotSID, gotOK := SIDFromHistoryStream(c.stream)
		if gotSID != c.wantSID || gotOK != c.wantOK {
			t.Errorf("SIDFromHistoryStream(%q) = (%q, %v); want (%q, %v)",
				c.stream, gotSID, gotOK, c.wantSID, c.wantOK)
		}
	}
}
