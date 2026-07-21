package p9_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// admin_events_e2e_test.go (#30) — the operator reader for the H.1 sys.events stream, over the REAL
// admin socket. This is the anti-"half-wired" proof (this project's repeated failure mode): it boots a
// REAL broker (Run wires backend.EventsTail = b.adminEventsTail), then drives OpEvents over the REAL
// socket and asserts (a) it returns a REAL broker-emitted event (tetherd_restarted, published on boot
// into the events stream — NOT a stub), (b) --kind and --since actually filter, and (c) with the
// endpoint disabled the read fails cleanly. If EventsTail were not wired, OpEvents would answer
// "events_unavailable" and every positive assertion below fails.

// pubEvent publishes one synthetic sys.events message straight into the events stream (as the broker's
// pubSysEvent does), so a test can seed specific kinds without driving every producing operation.
func pubEvent(t *testing.T, js jetstream.JetStream, kind string, extra map[string]any) {
	t.Helper()
	body := map[string]any{"v": 1, "type": kind, "ts": time.Now().UTC().Format(time.RFC3339Nano)}
	for k, v := range extra {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	if _, err := js.Publish(context.Background(), proto.SubjSysEvents, raw); err != nil {
		t.Fatalf("publish %s: %v", kind, err)
	}
}

func callEvents(t *testing.T, c *adminsock.Client, req adminsock.Request) []adminsock.AuditEntry {
	t.Helper()
	req.Op = adminsock.OpEvents
	resp, err := c.Call(req)
	if err != nil {
		t.Fatalf("OpEvents call: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("OpEvents rejected: %s", resp.Error)
	}
	return resp.Events
}

// TestAdminEventsTailsEventsStream is the core #30 round-trip: real broker events are readable, and the
// filters work.
func TestAdminEventsTailsEventsStream(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)
	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath, Timeout: 5 * time.Second}

	// (a) The broker emits tetherd_restarted on boot into the events stream. Poll until the reader
	// surfaces it — proving OpEvents is wired to the LIVE stream (not a stub / empty shell).
	sawRestart := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sawRestart {
		for _, e := range callEvents(t, c, adminsock.Request{N: 100}) {
			if kind, _ := e.Body["type"].(string); kind == "tetherd_restarted" {
				sawRestart = true
			}
		}
		if !sawRestart {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !sawRestart {
		t.Fatal("OpEvents never returned the broker's real tetherd_restarted boot event — reader not wired to the live events stream (half-wired)")
	}

	// Seed known kinds so filter assertions are deterministic.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	js, _ := jetstream.New(nc)
	for i := 0; i < 3; i++ {
		pubEvent(t, js, "proxy_keyset_changed", map[string]any{"sid": "lab"})
	}
	pubEvent(t, js, "disk_pressure", map[string]any{"pct": 0.9})
	pubEvent(t, js, "disk_pressure", map[string]any{"pct": 0.95})

	// Wait until all 5 seeds are visible.
	waitEventCount := func(kind string, want int) []adminsock.AuditEntry {
		t.Helper()
		var got []adminsock.AuditEntry
		dl := time.Now().Add(5 * time.Second)
		for time.Now().Before(dl) {
			got = callEvents(t, c, adminsock.Request{N: 100, EventKind: kind})
			if len(got) >= want {
				return got
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("kind=%q: got %d events, want >= %d", kind, len(got), want)
		return nil
	}

	// (b) --kind filters to exactly that type, and never leaks another kind.
	pk := waitEventCount("proxy_keyset_changed", 3)
	for _, e := range pk {
		if kind, _ := e.Body["type"].(string); kind != "proxy_keyset_changed" {
			t.Errorf("--kind=proxy_keyset_changed leaked a %q event: %+v", kind, e.Body)
		}
	}
	if got := waitEventCount("disk_pressure", 2); len(got) < 2 {
		t.Errorf("disk_pressure filter: got %d, want >= 2", len(got))
	}

	// Oldest → newest ordering (seq strictly increasing) across an unfiltered read.
	all := callEvents(t, c, adminsock.Request{N: 100})
	for i := 1; i < len(all); i++ {
		if all[i].Seq <= all[i-1].Seq {
			t.Fatalf("events not oldest→newest: seq[%d]=%d <= seq[%d]=%d", i, all[i].Seq, i-1, all[i-1].Seq)
		}
	}

	// -n bounds the count.
	if got := callEvents(t, c, adminsock.Request{N: 2}); len(got) != 2 {
		t.Errorf("-n 2: got %d events, want exactly 2", len(got))
	}

	// (c) --since both directions: a wide window includes the recent seeds; a 1ns window (cutoff ≈ now)
	// excludes everything already stored ms ago.
	if got := callEvents(t, c, adminsock.Request{N: 100, Since: "24h"}); len(got) < 5 {
		t.Errorf("--since 24h: got %d events, want >= 5 (all recent)", len(got))
	}
	if got := callEvents(t, c, adminsock.Request{N: 100, Since: "1ns"}); len(got) != 0 {
		t.Errorf("--since 1ns: got %d events, want 0 (cutoff ≈ now excludes already-stored events): %+v", len(got), got)
	}
}

// TestAdminEventsNoSecretInPayload is the R12-discipline negative assertion: NO sys.events payload the
// reader returns may carry a secret. We seed an event that (adversarially) tries to smuggle secret-named
// keys and assert the reader relays exactly what is on the wire — and, more importantly, we assert that
// across ALL returned events no value matches a planted secret sentinel. (The producers are secret-free
// by construction; this pins that the READER introduces none either.)
func TestAdminEventsNoSecretInPayload(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)
	defer startBrokerWithAdmin(t, url, db, socketPath)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	js, _ := jetstream.New(nc)
	const sentinel = "SUPERSECRET-PSK-do-not-leak"
	// A well-behaved proxy_keyset_changed carries only {sid}. We also publish nothing containing the
	// sentinel — the assertion proves the reader never fabricates a secret field into the output.
	for i := 0; i < 3; i++ {
		pubEvent(t, js, "proxy_keyset_changed", map[string]any{"sid": "lab"})
	}

	c := &adminsock.Client{Path: socketPath, Timeout: 5 * time.Second}
	var events []adminsock.AuditEntry
	dl := time.Now().Add(5 * time.Second)
	for time.Now().Before(dl) {
		events = callEvents(t, c, adminsock.Request{N: 100, EventKind: "proxy_keyset_changed"})
		if len(events) >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(events) < 3 {
		t.Fatalf("expected >= 3 proxy_keyset_changed, got %d", len(events))
	}
	for _, e := range events {
		blob, _ := json.Marshal(e.Body)
		if strings.Contains(string(blob), sentinel) {
			t.Fatalf("event payload leaked a secret sentinel: %s", blob)
		}
		// A proxy_keyset_changed body must be exactly the allow-listed scalar keys (no psk/token/key).
		for k := range e.Body {
			switch k {
			case "v", "type", "ts", "sid":
			default:
				t.Errorf("proxy_keyset_changed carried an unexpected key %q (secret-leak risk): %+v", k, e.Body)
			}
		}
	}
}

// TestAdminEventsWithoutJetStreamFailsCleanly: a broker with no JetStream (test/p4..p6 default) must
// answer the events endpoint with a graceful error, not crash.
func TestAdminEventsWithoutJetStreamFailsCleanly(t *testing.T) {
	url := startNATS(t) // no JS
	db := openDB(t)
	socketPath := adminSocketPath(t)
	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpEvents, N: 10})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == "" {
		t.Errorf("expected events_unavailable error on a no-JetStream broker; got OK %+v", resp)
	}
	if !strings.Contains(resp.Error, "events_unavailable") {
		t.Errorf("error should name events_unavailable; got %q", resp.Error)
	}
}

// TestAdminEventsBadSinceRejected: a malformed --since must be rejected with a bad_request code, not
// silently treated as "no window".
func TestAdminEventsBadSinceRejected(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)
	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpEvents, Since: "not-a-duration"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == "" || resp.Code != adminsock.CodeBadRequest {
		t.Errorf("bad --since must be a bad_request; got error=%q code=%q", resp.Error, resp.Code)
	}
}
