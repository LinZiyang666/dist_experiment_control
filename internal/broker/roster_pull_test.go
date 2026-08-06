package broker

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// g3_roster_pull_test.go — G3 #17 改法二 broker responder: a ctl on the live conn pulls the signed
// manifest from the connected broker (SubjCtrlClusterRoster). It serves the pre-signed manifestBytes()
// cache (no per-request sign) and stays silent in single mode (ErrNoResponders → ctl falls back).

func g3NatsConn(t *testing.T) *nats.Conn {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestG3ClusterRosterPullServesSignedManifest(t *testing.T) {
	nc := g3NatsConn(t)
	b, accountPub, _ := newManifestTestBroker(t)

	sub, err := SubscribeClusterRosterPull(nc, b.manifestBytes, nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	msg, err := nc.Request(proto.SubjCtrlClusterRoster("Uactor"), nil, 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var m proto.ClusterManifest
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if m.Roster == nil || m.Seeds == nil {
		t.Fatal("roster-pull reply must carry both roster and seeds")
	}
	// The reply is what the ctl adopt path verifies against the OOB pin — it must verify.
	if err := clusterroster.VerifyAt(m.Roster, accountPub, b.cfg.Now()); err != nil {
		t.Errorf("roster must verify against the account pin: %v", err)
	}
	if err := clusterroster.VerifySeedsAt(m.Seeds, accountPub, b.cfg.Now()); err != nil {
		t.Errorf("seeds must verify against the account pin: %v", err)
	}
}

// m12: this fixture is CLUSTER-mode-but-unsigned (selfID set here to "" so manifestBytes returns
// (nil,false)), and the responder IS subscribed → a ctl request gets a TIMEOUT, not ErrNoResponders. A
// TRUE single-mode broker never wires this responder (cluster-only mount) and would fast-fail with
// ErrNoResponders. Either way the ctl falls back; the distinction is latency, pinned here honestly.
func TestG3ClusterRosterPullUnsignedIsSilentTimeout(t *testing.T) {
	nc := g3NatsConn(t)
	b, _, _ := newManifestTestBroker(t)
	b.selfID = "" // manifestBytes returns (nil,false) → responder stays silent

	sub, err := SubscribeClusterRosterPull(nc, b.manifestBytes, nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	_, rerr := nc.Request(proto.SubjCtrlClusterRoster("Uactor"), nil, 300*time.Millisecond)
	if rerr == nil {
		t.Fatal("an unsigned broker must NOT reply a body")
	}
	if !errors.Is(rerr, nats.ErrTimeout) {
		t.Errorf("a SUBSCRIBED-but-silent responder yields a timeout (not the ErrNoResponders a true single-mode broker would give), got %v", rerr)
	}
}

// M6: the responder must DELEGATE to the passed manifestFn (the ≥30s-cached manifestBytes in prod) — it
// must never re-sign per request itself. A regression that swapped in a per-request buildSignedRoster /
// buildSeedBundle (an unauthenticated-adjacent ed25519 signing amplifier) would stop calling manifestFn,
// so this counting fake pins "each request → exactly one manifestFn call, no responder-side signing".
func TestG3RosterPullDelegatesToManifestFn(t *testing.T) {
	nc := g3NatsConn(t)
	var calls int32
	fakeFn := func() ([]byte, bool) {
		atomic.AddInt32(&calls, 1)
		return []byte(`{"schema_version":1}`), true
	}
	sub, err := SubscribeClusterRosterPull(nc, fakeFn, nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	const n = 20
	for i := 0; i < n; i++ {
		if _, rerr := nc.Request(proto.SubjCtrlClusterRoster("Ux"), nil, time.Second); rerr != nil {
			t.Fatalf("request %d: %v", i, rerr)
		}
	}
	if got := atomic.LoadInt32(&calls); got != n {
		t.Errorf("responder must call manifestFn exactly once per request (never re-sign per request), got %d for %d requests", got, n)
	}
}
