// Package p13_test is the P13 proxy-subscription end-to-end phase test
// (joins the architecture e2e matrix). It exercises the FULL control plane
// in-process: an owner flips the master switch, a real agent applies the
// directive (starts its embedded SS server + ACKs readiness), a subscriber
// is minted, the broker-served /sub URL renders that live node, and a revoke
// makes the token 404. The SS byte round-trip itself is an in-process unit
// test (internal/agent/ssproxy) per the plan's test matrix.
package p13_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type recordingAdapter struct {
	mu      sync.Mutex
	added   []agent.PortToken
	removed []string
}

func (r *recordingAdapter) AddProxy(p agent.PortToken) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, p)
	return nil
}
func (r *recordingAdapter) RemoveProxy(name string, _ int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, name)
	return nil
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func proxyReq(t *testing.T, nc *nats.Conn, subj string, body, out any) {
	t.Helper()
	b, _ := json.Marshal(body)
	reply, err := nc.Request(subj, b, 3*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subj, err)
	}
	if err := json.Unmarshal(reply.Data, out); err != nil {
		t.Fatalf("decode %s: %v", subj, err)
	}
}

func proxyReady(db *sql.DB, sid, nid string) bool {
	var v int
	_ = db.QueryRow(`SELECT proxy_ready FROM nodes WHERE sid=? AND nid=?`, sid, nid).Scan(&v)
	return v == 1
}

func TestProxySubscriptionE2E(t *testing.T) {
	url := testharness.StartNATS(t)
	db := testharness.OpenDB(t)
	ownerPub, ownerFP := testharness.FreshUserPub(t)
	if _, err := session.Create(db, "lab", "lab", ownerFP, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	subAddr := freePort(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            logger,
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
		PublicHost:        "broker.test",
		PortBandLow:       14000,
		PortBandHigh:      14010,
		SubHTTPAddr:       subAddr,
	})
	if err != nil {
		t.Fatal(err)
	}
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	go func() { _ = b.Run(bctx) }()
	testharness.WaitConnect(t, url, 3*time.Second)
	time.Sleep(100 * time.Millisecond) // let subscriptions + http listener settle

	// Real agent with a recording adapter (records the proxy tunnel open).
	adapter := &recordingAdapter{}
	ag, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Home:              t.TempDir(),
		ExposeAdapter:     adapter,
		Logger:            logger,
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	actx, acancel := context.WithCancel(context.Background())
	adone := make(chan struct{})
	go func() { _ = ag.Run(actx); close(adone) }()
	// Stop the agent and WAIT for Run to fully exit before the test's TempDir
	// (the agent Home) is removed — otherwise a late state.json write races the
	// cleanup walk.
	defer func() {
		acancel()
		select {
		case <-adone:
		case <-time.After(3 * time.Second):
			t.Error("agent did not exit")
		}
	}()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// --- owner turns the proxy ON ---
	var setR proto.ProxySetResp
	proxyReq(t, nc, proto.SubjCtrlProxySet(ownerPub, "lab"), proto.ProxySetReq{Enabled: true}, &setR)
	if !setR.OK || setR.AffectedNodes != 1 {
		t.Fatalf("proxy on: %+v", setR)
	}

	// Agent must apply the directive: open the tunnel + ACK readiness.
	if !testharness.WaitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
		return proxyReady(db, "lab", "lab-1")
	}) {
		t.Fatal("agent never ACKed proxy readiness (proxy_ready=1)")
	}
	adapter.mu.Lock()
	gotTunnel := len(adapter.added) == 1 && adapter.added[0].Name == "__proxy__"
	adapter.mu.Unlock()
	if !gotTunnel {
		t.Fatal("agent did not open the proxy tunnel")
	}

	// --- mint a subscription ---
	var createR proto.ProxySubCreateResp
	proxyReq(t, nc, proto.SubjCtrlProxySubCreate(ownerPub, "lab"), proto.ProxySubCreateReq{Name: "alice"}, &createR)
	if !createR.OK || !strings.Contains(createR.SubURL, "/sub/") {
		t.Fatalf("sub create: %+v", createR)
	}
	token := createR.SubURL[strings.LastIndex(createR.SubURL, "/sub/")+len("/sub/"):]

	// --- the broker-served /sub URL renders the live node ---
	subURL := fmt.Sprintf("http://%s/sub/%s", subAddr, token)
	body, code := httpGet(t, subURL)
	if code != 200 {
		t.Fatalf("/sub status=%d", code)
	}
	if !strings.Contains(body, "lab-1") || !strings.Contains(body, "broker.test") ||
		!strings.Contains(body, "chacha20-ietf-poly1305") {
		t.Fatalf("/sub render missing node/host/cipher:\n%s", body)
	}

	// --- revoke makes the token 404 (no more egress for that subscriber) ---
	var revR proto.ProxySubRevokeResp
	proxyReq(t, nc, proto.SubjCtrlProxySubRevoke(ownerPub, "lab"), proto.ProxySubRevokeReq{Name: "alice"}, &revR)
	if !revR.OK {
		t.Fatalf("revoke: %+v", revR)
	}
	if _, code := httpGet(t, subURL); code != 404 {
		t.Fatalf("revoked token /sub status=%d want 404", code)
	}

	// A still-valid subscriber (bob) renders the live node while ON.
	var bobR proto.ProxySubCreateResp
	proxyReq(t, nc, proto.SubjCtrlProxySubCreate(ownerPub, "lab"), proto.ProxySubCreateReq{Name: "bob"}, &bobR)
	bobURL := fmt.Sprintf("http://%s/sub/%s", subAddr,
		bobR.SubURL[strings.LastIndex(bobR.SubURL, "/sub/")+len("/sub/"):])
	if b, _ := httpGet(t, bobURL); !strings.Contains(b, "lab-1") {
		t.Fatalf("bob's /sub should list lab-1 while proxy is on:\n%s", b)
	}

	// --- proxy off: assert the AGENT actually tore the proxy down (adapter
	// RemoveProxy), not just the broker-cleared DB flag (F9). ---
	proxyReq(t, nc, proto.SubjCtrlProxySet(ownerPub, "lab"), proto.ProxySetReq{Enabled: false}, &setR)
	if !testharness.WaitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
		adapter.mu.Lock()
		removed := len(adapter.removed) >= 1 && adapter.removed[len(adapter.removed)-1] == "__proxy__"
		adapter.mu.Unlock()
		return removed
	}) {
		t.Fatal("agent never removed the proxy tunnel after proxy off")
	}
	// And bob's (valid) subscription now renders no exit node.
	if b, _ := httpGet(t, bobURL); strings.Contains(b, "lab-1") {
		t.Fatalf("after proxy off, a valid subscription still lists an exit node:\n%s", b)
	}
}

func httpGet(t *testing.T, url string) (string, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode
}

// F9: an agent that joins AFTER the switch is already ON must pick up the
// proxy directive via its register reply and become a live exit node.
func TestProxyJoinAfterOn(t *testing.T) {
	url := testharness.StartNATS(t)
	db := testharness.OpenDB(t)
	ownerPub, ownerFP := testharness.FreshUserPub(t)
	if _, err := session.Create(db, "lab", "lab", ownerFP, "pin", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Enable + a subscriber BEFORE any agent exists.
	if _, err := session.SetProxyEnabled(db, "lab", true); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BumpProxyEpoch(db, "lab"); err != nil {
		t.Fatal(err)
	}
	carol, err := proxysub.Create(db, "lab", "carol", ownerFP, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_ = ownerPub

	subAddr := freePort(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := broker.New(broker.Config{
		NATSURL: url, DB: db, Logger: logger,
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond, OfflineAfter: 900 * time.Millisecond,
		PublicHost: "broker.test", PortBandLow: 14000, PortBandHigh: 14010, SubHTTPAddr: subAddr,
	})
	if err != nil {
		t.Fatal(err)
	}
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	go func() { _ = b.Run(bctx) }()
	testharness.WaitConnect(t, url, 3*time.Second)
	time.Sleep(100 * time.Millisecond)

	adapter := &recordingAdapter{}
	ag, err := agent.New(agent.Config{
		NATSURL: url, SID: "lab", NID: "lab-1", Home: t.TempDir(),
		ExposeAdapter: adapter, Logger: logger,
		HeartbeatInterval: 100 * time.Millisecond, RegisterTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	actx, acancel := context.WithCancel(context.Background())
	adone := make(chan struct{})
	go func() { _ = ag.Run(actx); close(adone) }()
	defer func() {
		acancel()
		select {
		case <-adone:
		case <-time.After(3 * time.Second):
			t.Error("agent did not exit")
		}
	}()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	// The agent joined after ON → register reply carried the directive.
	if !testharness.WaitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
		return proxyReady(db, "lab", "lab-1")
	}) {
		t.Fatal("late-joining agent never became proxy-ready")
	}
	subURL := fmt.Sprintf("http://%s/sub/%s", subAddr,
		carol.Token) // carol.Token is the raw token
	body, code := httpGet(t, subURL)
	if code != 200 || !strings.Contains(body, "lab-1") {
		t.Fatalf("late-joining agent not rendered: code=%d\n%s", code, body)
	}
}
