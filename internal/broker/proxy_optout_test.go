// proxy_optout_test.go — the broker half of #78's proxy.participate opt-out:
// the register-time fold into nodes.proxy_capable, the inline single-mode
// free of an existing allocation, the status visibility hint, and the reaper
// teardown selection's capability leg.
// origin: docs/deploy-tier-gotchas.md #78
package broker

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
)

// optOutRegister drives the REAL register path over NATS (the full
// handleRegister: fold + inline free + hint), returning the reply.
func optOutRegister(t *testing.T, h *proxyOptOutHarness, nid string, optOut bool) proto.NodeRegisterResp {
	t.Helper()
	var resp proto.NodeRegisterResp
	req(t, h.nc, proto.SubjNodeRegister(h.sid, nid), proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "v0.5.0",
		NID:            nid,
		Capabilities:   []string{proto.CapProxyV1},
		ProxyOptOut:    optOut,
	}, &resp)
	if !resp.OK {
		t.Fatalf("register(%s, optOut=%v) refused: %+v", nid, optOut, resp)
	}
	return resp
}

type proxyOptOutHarness struct {
	nc  *nats.Conn
	sid string
	b   *Broker
}

func newProxyOptOutHarness(t *testing.T) *proxyOptOutHarness {
	t.Helper()
	nc, _, sid, b := proxyTestBroker(t)
	// The session proxy switch ON — the world where directives get minted.
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BumpProxyEpoch(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	}
	return &proxyOptOutHarness{nc: nc, sid: sid, b: b}
}

func TestProxyOptOutRegisterFoldsAndFrees(t *testing.T) {
	h := newProxyOptOutHarness(t)

	// Phase 1 — participate: the register reply mints a full directive.
	resp := optOutRegister(t, h, "lab-1", false)
	if resp.Proxy == nil || resp.Proxy.PublicPort == 0 || resp.Proxy.Token == "" {
		t.Fatalf("participating register must mint a proxy directive, got %+v", resp.Proxy)
	}
	if !h.b.nodeProxyCapable(h.sid, "lab-1") {
		t.Fatal("participating node must persist proxy_capable=1")
	}

	// Phase 2 — the operator flips participate: false and the agent
	// re-registers. Fold: proxy_capable=0. Free: the ALLOCATED row from
	// phase 1 must be gone (single mode has no reaper to catch it later).
	resp = optOutRegister(t, h, "lab-1", true)
	if resp.Proxy != nil {
		t.Fatalf("opted-out register must carry NO proxy directive, got %+v", resp.Proxy)
	}
	if h.b.nodeProxyCapable(h.sid, "lab-1") {
		t.Fatal("opt-out must fold into nodes.proxy_capable=0")
	}
	// Freed = the ALLOCATED row is gone (Lookup reports nil or not-found).
	if existing, err := port.LookupProxyByNode(h.b.cfg.DB, h.sid, "lab-1"); existing != nil {
		t.Fatalf("opt-out register must free the existing __proxy__ allocation, still have %+v (err=%v)", existing, err)
	}
	// The convergence loop must skip the node entirely (repairProxy reads
	// proxy_capable) — a re-push here would re-flood an old-broker world.
	for _, nid := range h.b.onlineNIDs(h.sid) {
		if nid == "lab-1" {
			t.Fatal("opted-out node must not appear in onlineNIDs (the repair/allocate pool)")
		}
	}

	// Phase 3 — visibility: proxy status distinguishes "won't" from "can't".
	nodes, err := h.b.proxyStatusNodes(h.sid)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range nodes {
		if n.NID == "lab-1" {
			found = true
			if !n.OptedOut {
				t.Fatal("proxy status must mark the node opted-out")
			}
		}
	}
	if !found {
		t.Fatal("opted-out node must still be LISTED in proxy status (won't ≠ invisible)")
	}

	// Phase 4 — recovery: flipping back to participate re-enters the pool
	// and re-mints. A broken recovery path would make opt-out a silent
	// one-way switch.
	resp = optOutRegister(t, h, "lab-1", false)
	if resp.Proxy == nil || resp.Proxy.PublicPort == 0 {
		t.Fatalf("re-participating register must re-mint, got %+v", resp.Proxy)
	}
	if !h.b.nodeProxyCapable(h.sid, "lab-1") {
		t.Fatal("re-participating node must persist proxy_capable=1 again")
	}
	nodes, err = h.b.proxyStatusNodes(h.sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if n.NID == "lab-1" && n.OptedOut {
			t.Fatal("re-participating node must lose the opted-out mark")
		}
	}
}

// TestFreeOptOutProxyRowSingleSerializesWithProxyOp (review Mi4) proves the
// opt-out free serializes against the proxyOpMu the enable path holds — so a
// row `proxy on` mints while register is committing the opt-out is always
// collected, never leaked. Deterministic: hold the lock (as enableProxy
// would), show the free BLOCKS, then release and show it collects the row.
func TestFreeOptOutProxyRowSingleSerializesWithProxyOp(t *testing.T) {
	_, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.5.0", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// A row the enable path minted (the leak candidate).
	if _, err := port.AllocateProxy(b.cfg.DB, sid, "lab-1", b.cfg.PortAllocCfg()); err != nil {
		t.Fatal(err)
	}

	b.proxyOpMu.Lock() // stand in for enableProxy holding the op lock
	freed := make(chan struct{})
	go func() {
		freeOptOutProxyRowSingle(b, sid, "lab-1") // must block on proxyOpMu
		close(freed)
	}()

	// While the lock is held the free cannot have run: the row survives.
	select {
	case <-freed:
		b.proxyOpMu.Unlock()
		t.Fatal("free ran without taking proxyOpMu — the Mi4 race is open")
	case <-time.After(100 * time.Millisecond):
	}
	if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1"); existing == nil {
		b.proxyOpMu.Unlock()
		t.Fatal("row was freed while the op lock was held")
	}

	b.proxyOpMu.Unlock()
	select {
	case <-freed:
	case <-time.After(2 * time.Second):
		t.Fatal("free did not proceed after the op lock was released")
	}
	if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1"); existing != nil {
		t.Fatalf("the minted row was not collected after the lock released: %+v", existing)
	}
}

// TestRepairProxyCollectsLeftoverRowFromNotCapableNode (review F7) pins the
// single-mode convergence retry: if the register-time inline free failed
// transiently, a leftover ALLOCATED __proxy__ row on a not-capable (opted-out)
// node must be collected by the NEXT heartbeat-driven repairProxy — single
// mode has no reaper, so without this the row would survive to the next
// reconnect.
func TestRepairProxyCollectsLeftoverRowFromNotCapableNode(t *testing.T) {
	_, _, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	// Node registered NOT proxy-capable (opted out) but a stale ALLOCATED row
	// survives — exactly the state a transient register-time Free leaves.
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.5.0", ProxyCapable: false,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := port.AllocateProxy(b.cfg.DB, sid, "lab-1", b.cfg.PortAllocCfg()); err != nil {
		t.Fatal(err)
	}
	if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1"); existing == nil {
		t.Fatal("precondition: the leftover row must exist")
	}

	// A heartbeat drives repairProxy; the not-capable arm must collect the row.
	b.repairProxy(sid, "lab-1", 0, 0)

	if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1"); existing != nil {
		t.Fatalf("repairProxy did not collect the leftover row from a not-capable node: %+v", existing)
	}
}

func TestProxyOptOutOldAgentZeroValueParticipates(t *testing.T) {
	// N-1: an old agent's register has no proxy_opt_out field — the zero
	// value must mean full participation, byte-identical to pre-#78.
	h := newProxyOptOutHarness(t)
	resp := optOutRegister(t, h, "lab-old", false)
	if resp.Proxy == nil || resp.Proxy.PublicPort == 0 {
		t.Fatalf("zero-value opt-out must participate fully, got %+v", resp.Proxy)
	}
	if !h.b.nodeProxyCapable(h.sid, "lab-old") {
		t.Fatal("zero-value opt-out must keep proxy_capable=1")
	}
}

func TestStaleProxyTeardownRowsSelectsOptedOutNodes(t *testing.T) {
	// The reaper teardown selection's #78 leg: an ALLOCATED __proxy__ row
	// whose NODE is proxy_capable=0 must be selected for teardown even while
	// its session stays ACTIVE+proxy-enabled (the cluster residual: the
	// allocation predates the opt-out; every other gate now skips the node,
	// so nothing else would ever free the row).
	_, _, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.5.0", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	alloc, err := port.AllocateProxy(b.cfg.DB, sid, "lab-1", b.cfg.PortAllocCfg())
	if err != nil {
		t.Fatal(err)
	}

	// Capable node + live session ⇒ NOT selected.
	stale, err := staleProxyTeardownRows(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range stale {
		if r.nid == "lab-1" {
			t.Fatalf("a capable node's live allocation must not be torn down: %+v", r)
		}
	}

	// The node re-registers opted-out (capable=0) ⇒ its row IS selected.
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.5.0", ProxyCapable: false,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, err = staleProxyTeardownRows(b)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range stale {
		if r.sid == sid && r.nid == "lab-1" && r.port == alloc.Port {
			found = true
		}
	}
	if !found {
		t.Fatalf("an opted-out node's ALLOCATED row must be selected for teardown; got %+v", stale)
	}
}
