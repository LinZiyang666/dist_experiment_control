package broker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// events_test.go (formerly r30_events_test.go; #30) — task 2: cluster-mode proxy sub create/revoke must emit the operator-facing
// sys.events{type:proxy_keyset_changed}. The single-mode path emits it (via pushCurrentKeyset); the
// cluster path bumped the keyset epoch through Raft but never surfaced the event (drill 73's "cluster-
// mode revoke's ABSENT proxy_keyset_changed"). These tests drive the REAL cluster handlers on a live
// single-node raft leader and assert, via the audit tap (which fires at the top of publishAudit, the
// same conduit pubSysEvent uses), that the event is emitted on the SAME subject the operator reader
// (`admin events`) tails — and that it never carries a secret.
//
// Mutation verification (run out-of-band, both confirmed RED): commenting out the
// b.pubSysEvent("proxy_keyset_changed", …) line in either handler makes the corresponding
// "sawKeysetChanged" assertion fail — proving the assertion actually pins the emit, not a coincidence.

// eventTap captures every sys.events payload published while it is installed.
type eventTap struct {
	mu   sync.Mutex
	subj []string
	body [][]byte
}

func installEventTap(t *testing.T) *eventTap {
	t.Helper()
	tap := &eventTap{}
	auditTapForTest = func(subject string, payload []byte) {
		tap.mu.Lock()
		defer tap.mu.Unlock()
		tap.subj = append(tap.subj, subject)
		tap.body = append(tap.body, append([]byte(nil), payload...))
	}
	t.Cleanup(func() { auditTapForTest = nil })
	return tap
}

// sysEvents returns the decoded bodies of every message tapped on the sys.events subject.
func (tap *eventTap) sysEvents(t *testing.T) []map[string]any {
	t.Helper()
	tap.mu.Lock()
	defer tap.mu.Unlock()
	var out []map[string]any
	for i, s := range tap.subj {
		if s != proto.SubjSysEvents {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(tap.body[i], &m); err != nil {
			t.Fatalf("sys.event %d is not JSON: %v (%s)", i, err, tap.body[i])
		}
		out = append(out, m)
	}
	return out
}

func (tap *eventTap) countKind(t *testing.T, kind string) int {
	n := 0
	for _, m := range tap.sysEvents(t) {
		if k, _ := m["type"].(string); k == kind {
			n++
		}
	}
	return n
}

// newClusterProxyBroker builds a cluster-mode broker on a live single-node raft leader, with cfg.DB =
// the node's RODB (so the handlers' read-backs see committed state), a session, and proxy enabled.
func newClusterProxyBroker(t *testing.T, sid, ownerFP string) (*Broker, *cluster.Node) {
	t.Helper()
	n, _ := d7SingleNode(t, "brk-a")
	now := time.Now().UTC()
	b := &Broker{
		cfg:    Config{DB: n.RODB(), Logger: silentLogger(), Now: func() time.Time { return time.Now().UTC() }},
		selfID: "brk-a",
	}
	b.clusterMode = true
	b.cl = &clusterRuntime{node: n}

	if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, sid, sid, ownerFP, "pin-hash", now)
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanProxySetEnabled(sid, true, session.ProxyHAFreeze, now)
	}); err != nil {
		t.Fatalf("enable proxy: %v", err)
	}
	return b, n
}

// TestClusterProxyRevokeEmitsKeysetChanged is the task-2 core: a real cluster-mode revoke emits
// proxy_keyset_changed on the sys.events subject, and the event never carries the (known) PSK sentinel.
func TestClusterProxyRevokeEmitsKeysetChanged(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseCommittedProxyRevoke(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "committed proxy revoke x5", beforeG)
	testharness.AssertNoFDLeak(t, "committed proxy revoke x5", beforeFD, 0)
}

// A real committed revoke must deliver fresh keys without a reaper tick or
// agent heartbeat. The silent original recipient cannot drive its own repair.
// origin: hot-update S5 plan §6.4 / §6.5 / T28 / DRILL-RUN 75.A
func exerciseCommittedProxyRevoke(t *testing.T) {
	t.Helper()
	const sid, fp = "lab", "owner-fp"
	const pskSentinel = "PSK-SENTINEL-must-never-appear"
	const tokenHashSentinel = "TOKENHASH-SENTINEL"
	f := newFlowReaperFixture(t, 14200, 14210)
	b, n := f.b, f.b.cl.node

	// Seed one ACTIVE subscriber "alice" with a KNOWN psk so the no-secret assertion has a sentinel.
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return proxysub.PlanCreate(sid, "sub-alice", "alice", tokenHashSentinel, pskSentinel, fp, time.Now().UTC())
	}); err != nil {
		t.Fatalf("seed subscriber: %v", err)
	}

	// A second subscriber proves the push is the authoritative keyset, not
	// a blanket disable or an empty synthetic revocation directive.
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return proxysub.PlanCreate(sid, "sub-bob", "bob", "bob-token", "bob-key", fp, time.Now().UTC())
	}); err != nil {
		t.Fatal(err)
	}
	nc := b.nc.Load()
	sub, err := nc.SubscribeSync(proto.SubjCmdForwarded(sid, "node", "proxy-keys"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	tap := f.tap
	b.handleProxySubRevokeCluster(sid, fp, "actor", "alice", &nats.Msg{}) // Empty reply; observe the real forwarded directive.

	message, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("committed revoke waited for periodic repair: %v", err)
	}
	var directive proto.ProxyDirective
	if err := json.Unmarshal(message.Data, &directive); err != nil {
		t.Fatal(err)
	}
	epoch, err := session.GetProxyEpoch(b.cfg.DB, sid)
	if err != nil || !directive.Enabled || directive.Epoch != epoch || len(directive.Keys) != 1 || directive.Keys[0].SubID != "sub-bob" {
		t.Fatal("immediate revoke push did not retain exactly the committed survivor key")
	}
	if got := tap.countKind(t, "proxy_keyset_changed"); got != 1 {
		t.Fatalf("cluster revoke must emit exactly one proxy_keyset_changed, got %d (all sys.events: %+v)", got, tap.sysEvents(t))
	}
	// The event body must be exactly the allow-listed scalar keys — and no captured sys.event may
	// contain the PSK or its token hash.
	for _, m := range tap.sysEvents(t) {
		if k, _ := m["type"].(string); k != "proxy_keyset_changed" {
			continue
		}
		if m["sid"] != sid {
			t.Errorf("proxy_keyset_changed sid = %v, want %q", m["sid"], sid)
		}
		for key := range m {
			switch key {
			case "v", "type", "ts", "sid":
			default:
				t.Errorf("proxy_keyset_changed carried unexpected key %q (secret-leak risk): %+v", key, m)
			}
		}
	}
	for i, raw := range tap.body {
		if strings.Contains(string(raw), pskSentinel) || strings.Contains(string(raw), tokenHashSentinel) {
			t.Fatalf("sys.event %d (%s) leaked a secret: %s", i, tap.subj[i], raw)
		}
	}
}

// TestClusterProxyCreateEmitsKeysetChanged: a real cluster-mode create (which MINTS a token+PSK
// internally) emits proxy_keyset_changed, and the event is structurally secret-free (only {v,type,
// ts,sid}). The minted secret can only travel the (dropped) reply's SubURL, never the event.
func TestClusterProxyCreateEmitsKeysetChanged(t *testing.T) {
	const sid, fp = "lab", "owner-fp"
	b, _ := newClusterProxyBroker(t, sid, fp)

	tap := installEventTap(t)
	b.handleProxySubCreateCluster(sid, fp, "actor", "alice", &nats.Msg{})

	if got := tap.countKind(t, "proxy_keyset_changed"); got != 1 {
		t.Fatalf("cluster create must emit exactly one proxy_keyset_changed, got %d (all sys.events: %+v)", got, tap.sysEvents(t))
	}
	for _, m := range tap.sysEvents(t) {
		if k, _ := m["type"].(string); k != "proxy_keyset_changed" {
			continue
		}
		for key := range m {
			switch key {
			case "v", "type", "ts", "sid":
			default:
				t.Errorf("proxy_keyset_changed carried unexpected key %q: %+v", key, m)
			}
		}
	}
}

// TestClusterProxyRevokeNoOpStaysSilent pins the epoch-delta precision: revoking a name that is not
// ACTIVE bumps nothing, so NO proxy_keyset_changed is emitted (matches single mode's ErrAlreadyRevoked
// path). This also guards against a naive "emit unconditionally after Propose" — that would fire here.
func TestClusterProxyRevokeNoOpStaysSilent(t *testing.T) {
	const sid, fp = "lab", "owner-fp"
	b, _ := newClusterProxyBroker(t, sid, fp)

	tap := installEventTap(t)
	// "ghost" was never created ⇒ the change-gated revoke is a no-op ⇒ epoch unchanged ⇒ no event.
	b.handleProxySubRevokeCluster(sid, fp, "actor", "ghost", &nats.Msg{})

	if got := tap.countKind(t, "proxy_keyset_changed"); got != 0 {
		t.Fatalf("a no-op revoke must NOT emit proxy_keyset_changed, got %d (%+v)", got, tap.sysEvents(t))
	}
}
