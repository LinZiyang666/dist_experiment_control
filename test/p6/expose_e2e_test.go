// Package p6_test exercises the P6 control plane (port allocation +
// expose/expose-rm + ev.port lifecycle + agent state.json) end-to-end
// in the same anonymous-NATS harness used by test/p4 / test/p5.
//
// What's covered HERE: the SQLite + protocol + agent-side state.json
// layer that the architecture pins as the source of truth for ports.
//
// What's NOT covered here: the actual TCP forwarding via embedded frp.
// That's exercised manually per the P6 architecture spec
// (`python -m http.server` + `curl http://broker:14022`) and lives in
// internal/frpmgr — its own coverage. Splitting these two concerns is
// what lets the in-memory test suite stay deterministic and fast: the
// frp data plane needs real OS sockets and a real frpc subprocess.
package p6_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// ----- harness --------------------------------------------------------------
//
// Generic primitives live in internal/testharness; the helpers below
// are the phase-specific bits.

func startNATS(t *testing.T) string                  { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                    { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                        { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string)     { return testharness.FreshUserPub(t) }

func seed(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, fp, "test-pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

func startBroker(t *testing.T, url string, db *sql.DB, opts ...func(*broker.Config)) func() {
	t.Helper()
	cfg := broker.Config{
		NATSURL:                 url,
		DB:                      db,
		Logger:                  silentLog(),
		ReconcileInterval:       50 * time.Millisecond,
		StaleAfter:              300 * time.Millisecond,
		OfflineAfter:            900 * time.Millisecond,
		PublicHost:              "test.local",
		PortBandLow:             14000,
		PortBandHigh:            14002,
		PortRevokeAfterDur:      300 * time.Millisecond,
		ExposeForwardTimeoutDur: 1 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

// recordingAdapter implements agent.ExposeAdapter and records every
// AddProxy / RemoveProxy call for assertions. AddProxyErr lets a test
// force a frpc-failure rollback path.
type recordingAdapter struct {
	mu          sync.Mutex
	added       []agent.PortToken
	removed     []string
	AddProxyErr error
}

func (r *recordingAdapter) AddProxy(p agent.PortToken) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.AddProxyErr != nil {
		return r.AddProxyErr
	}
	r.added = append(r.added, p)
	return nil
}
func (r *recordingAdapter) RemoveProxy(name string, _ int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, name)
	return nil
}
func (r *recordingAdapter) snapshot() ([]agent.PortToken, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := append([]agent.PortToken(nil), r.added...)
	rm := append([]string(nil), r.removed...)
	return a, rm
}

func startAgent(t *testing.T, url, sid, nid, home string, adapter agent.ExposeAdapter) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Home:              home,
		ExposeAdapter:     adapter,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

// runExpose mimics what `tether expose` does (without the CLI layer):
// publish ExposeReq, return ExposeResp.
func runExpose(t *testing.T, nc *nats.Conn, sid, actor, nid, name string, local int) proto.ExposeResp {
	t.Helper()
	body, _ := json.Marshal(proto.ExposeReq{Name: name, LocalPort: local})
	subj := proto.SubjCmdBy(sid, actor, nid, "expose")
	respMsg, err := nc.Request(subj, body, 3*time.Second)
	if err != nil {
		t.Fatalf("expose request: %v", err)
	}
	var resp proto.ExposeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("expose response parse: %v", err)
	}
	return resp
}

func runExposeRm(t *testing.T, nc *nats.Conn, sid, actor, nid, name string) proto.ExposeRmResp {
	t.Helper()
	body, _ := json.Marshal(proto.ExposeRmReq{Name: name})
	subj := proto.SubjCmdBy(sid, actor, nid, "expose-rm")
	respMsg, err := nc.Request(subj, body, 3*time.Second)
	if err != nil {
		t.Fatalf("expose-rm request: %v", err)
	}
	var resp proto.ExposeRmResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("expose-rm response parse: %v", err)
	}
	return resp
}

// ----- tests ----------------------------------------------------------------

func TestExposeAllocatesAndPersistsToken(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	home := t.TempDir()
	adapter := &recordingAdapter{}
	defer startAgent(t, url, "lab", "lab-1", home, adapter)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 8888)
	if resp.Code != "" {
		t.Fatalf("expose rejected: %s %s", resp.Code, resp.Error)
	}
	if resp.Port < 14000 || resp.Port > 14002 {
		t.Errorf("port outside test band: got %d", resp.Port)
	}
	if resp.PublicHost != "test.local" {
		t.Errorf("public host: got %q want test.local", resp.PublicHost)
	}

	// Architecture F.4 storage rule: the raw token belongs to
	// (broker→agent) only. Verify the agent adapter received it and
	// the SQLite row stores SHA256(token), never the raw value; the
	// ctl-facing struct (proto.ExposeResp) has no field that could
	// carry it.
	added, _ := adapter.snapshot()
	if len(added) != 1 {
		t.Fatalf("adapter AddProxy calls: got %d want 1", len(added))
	}
	if added[0].Token == "" {
		t.Error("agent adapter must receive the raw token")
	}
	if added[0].Port != resp.Port {
		t.Errorf("adapter / ctl port mismatch: %d vs %d", added[0].Port, resp.Port)
	}

	a, err := port.LookupByName(db, "lab", "jupyter")
	if err != nil {
		t.Fatalf("lookup by name: %v", err)
	}
	if a.Port != resp.Port || a.LocalPort != 8888 || a.CreatedByFP != fp {
		t.Errorf("DB row mismatch: %+v", a)
	}
	if a.TokenHash != port.HashToken(added[0].Token) {
		t.Errorf("token hash didn't match the agent-side raw token")
	}
}

func TestExposeRmFreesPortAndPortIsReusable(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	first := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 8888)
	if first.Code != "" {
		t.Fatalf("first expose: %s", first.Code)
	}
	rm := runExposeRm(t, nc, "lab", pub, "lab-1", "jupyter")
	if !rm.OK {
		t.Fatalf("expose rm: %s %s", rm.Code, rm.Error)
	}
	if rm.Port != first.Port {
		t.Errorf("rm port: got %d want %d", rm.Port, first.Port)
	}

	second := runExpose(t, nc, "lab", pub, "lab-1", "other", 9999)
	if second.Code != "" {
		t.Fatalf("re-expose after rm: %s", second.Code)
	}
	if second.Port != first.Port {
		t.Errorf("port reuse after rm: got %d want %d", second.Port, first.Port)
	}
}

func TestExposeAdapterFailureRollsBackPortRow(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	adapter := &recordingAdapter{AddProxyErr: errFake("fake_frpc_failure")}
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), adapter)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 8888)
	if resp.Code == "" {
		t.Fatal("adapter failure must surface as a non-empty Code")
	}
	if !strings.Contains(resp.Code, "agent_rejected:frpc_failed") {
		t.Errorf("code: got %q want substring agent_rejected:frpc_failed", resp.Code)
	}
	// Port row must be FREED (or absent ALLOCATED) after rollback.
	if _, err := port.LookupByName(db, "lab", "jupyter"); err == nil {
		t.Error("ALLOCATED row should have been rolled back; lookup unexpectedly succeeded")
	}
}

func TestExposeRejectsDuplicateName(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	if r := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 8888); r.Code != "" {
		t.Fatalf("first expose: %s", r.Code)
	}
	r := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 9999)
	if r.Code != "name_taken" {
		t.Errorf("expected name_taken, got %s", r.Code)
	}
}

func TestExposeRejectsDeletingSession(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	r := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 8888)
	if r.Code != "session_not_found_or_deleting" {
		t.Errorf("expected session_not_found_or_deleting, got %s", r.Code)
	}
}

func TestExposeRejectsMissingNode(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	r := runExpose(t, nc, "lab", pub, "ghost", "jupyter", 8888)
	if r.Code != "node_not_found" {
		t.Errorf("expected node_not_found, got %s", r.Code)
	}
}

// Reconciler test: an OFFLINE node's ports get REVOKED after the
// configured threshold. We use the broker's PortRevokeAfterDur=300ms
// (set in the test harness) so the test runs in <2s.
func TestReconcilerRevokesOfflineNodePorts(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	stopAgent := startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})

	nc, _ := nats.Connect(url)
	defer nc.Close()

	if r := runExpose(t, nc, "lab", pub, "lab-1", "jupyter", 8888); r.Code != "" {
		t.Fatalf("expose: %s", r.Code)
	}

	// Subscribe to ev.port.<port>.revoked BEFORE killing the agent.
	a, err := port.LookupByName(db, "lab", "jupyter")
	if err != nil {
		t.Fatal(err)
	}
	gotRevoke := make(chan struct{}, 1)
	subRevoked, _ := nc.Subscribe(proto.SubjEvPort("lab", a.Port, "revoked"), func(m *nats.Msg) {
		select {
		case gotRevoke <- struct{}{}:
		default:
		}
	})
	defer func() { _ = subRevoked.Unsubscribe() }()

	// Kill agent → node OFFLINE after ~900ms (broker config) → port
	// reconciler revokes after PortRevokeAfterDur=300ms → expect event
	// within ~2s total.
	stopAgent()

	select {
	case <-gotRevoke:
	case <-time.After(3 * time.Second):
		t.Fatal("port revoke event never arrived")
	}

	// Port row must now be in REVOKED state, and the port number must
	// be reusable for another expose.
	if _, err := port.LookupByName(db, "lab", "jupyter"); err == nil {
		t.Error("ALLOCATED row should be gone after revoke")
	}
}

// errFake is a tiny error helper local to this file (avoids importing errors).
type errFake string

func (e errFake) Error() string { return string(e) }
