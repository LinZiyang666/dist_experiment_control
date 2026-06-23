package broker

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/nats-io/nats.go"
)

// Round-4 F3: when the allocation-store read fails during `proxy off`, OFF must
// NOT report success (a silent partial OFF would mislead the operator). The
// switch is still flipped off (authoritative for new REGISTERs); we just
// surface that cleanup couldn't complete. Injected by dropping port_allocations
// AFTER the session row exists, so the atomic switch/epoch commit still succeeds
// but port.ListBySession fails.
func TestProxyOffReportsErrorWhenAllocStoreFails(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)

	var setR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &setR)
	if !setR.OK {
		t.Fatalf("enable: %+v", setR)
	}

	if _, err := b.cfg.DB.Exec(`DROP TABLE port_allocations`); err != nil {
		t.Fatal(err)
	}

	var offR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: false}, &offR)
	if offR.OK {
		t.Fatalf("proxy off reported success despite alloc-store failure: %+v", offR)
	}
	// The switch/epoch commit succeeded BEFORE the alloc-store read failed, so
	// the switch is authoritatively OFF (the failure is only port recycling).
	if on, _ := session.GetProxyEnabled(b.cfg.DB, sid); on {
		t.Fatal("switch must be OFF even when port cleanup failed")
	}
}

// `proxy off` is a kill switch. Even when the durable epoch bump fails, the
// command must fail closed: deny new proxy REGISTERs and synchronously close
// already-installed public listeners, while reporting the store error.
func TestProxyOffKillsDataPlaneWhenEpochBumpFails(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)

	var onR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &onR)
	if !onR.OK {
		t.Fatalf("enable: %+v", onR)
	}

	controlPort := externalReviewFreePort(t)
	publicPort := externalReviewFreePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := tunnel.NewServer(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1",
		func(_, _ string, _ int, _ string, _ int64) error { return nil },
		silentLogger(),
	)
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	b.tunnelSrv = srv
	cli := tunnel.NewClient(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		sid,
		"lab-1",
		func(int) (int, error) { return 1, nil },
		silentLogger(),
	)
	cli.Start(ctx)
	if err := cli.Open(publicPort, 1, "token"); err != nil {
		t.Fatal(err)
	}
	if !externalReviewWaitListening(publicPort, true) {
		t.Fatal("precondition: proxy public listener did not start")
	}

	if _, err := b.cfg.DB.Exec(`
CREATE TRIGGER external_review_fail_proxy_epoch
BEFORE UPDATE OF proxy_epoch ON sessions
BEGIN
	SELECT RAISE(ABORT, 'epoch bump failure');
END;`); err != nil {
		t.Fatal(err)
	}

	var offR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: false}, &offR)
	if offR.OK {
		t.Fatalf("proxy off succeeded despite a failed epoch bump: %+v", offR)
	}
	if !externalReviewWaitListening(publicPort, false) {
		t.Fatal("failed OFF left the installed public listener reachable")
	}
	if on, err := session.GetProxyEnabled(b.cfg.DB, sid); err != nil || on {
		t.Fatalf("failed OFF did not persist the fail-closed switch: on=%v err=%v", on, err)
	}
}

func externalReviewFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func externalReviewWaitListening(port int, wantUp bool) bool {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		up := err == nil
		if conn != nil {
			_ = conn.Close()
		}
		if up == wantUp {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// Self-review (round-5): the OFF repair path must ALSO escalate the generation
// past a restored-behind agent — otherwise a disable directive stamped at the
// low broker generation is dropped by an agent at a higher applied generation,
// and it keeps serving past OFF (a kill-switch hole the ON path alone fixed).
func TestProxyOffRepairEscalatesPastRestoredAgentGeneration(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Proxy is OFF in the restored DB, but an agent still serves at a higher
	// generation issued before the restore.
	if _, err := session.BumpProxyEpoch(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	}
	b.proxyGen = 100
	const agentGen int64 = 200

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ProxyGeneration: agentGen, ProxyEpoch: 5})
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()

	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.Enabled {
			t.Fatalf("OFF repair pushed an enable directive: %+v", d)
		}
		if d.Generation <= agentGen {
			t.Fatalf("OFF disable generation=%d not escalated past agent generation=%d (agent would drop it)", d.Generation, agentGen)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OFF repair produced no disable directive for a still-serving agent")
	}
}

// Round-4 F3: a stale ALLOCATED __proxy__ row for a node that is NOT proxy_ready
// (e.g. a prior OFF whose port.Free failed while the agent cleared its
// footprint) must be ROTATED on the next `proxy on` — the node must receive a
// full token-bearing directive, not a tokenless keyset it can't act on.
func TestProxyOnRotatesStaleNotReadyAllocation(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Seed a stale ALLOCATED proxy row, node left NOT ready (default 0).
	stale, err := port.AllocateProxy(b.cfg.DB, sid, "lab-1", b.cfg.PortAllocCfg())
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan *nats.Msg, 4)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var setR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &setR)
	if !setR.OK {
		t.Fatalf("enable: %+v", setR)
	}

	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.Token == "" || d.PublicPort == 0 {
			t.Fatalf("not-ready node got a tokenless directive; rotate failed: %+v", d)
		}
		// A fresh token must be minted (the stale row was freed + re-allocated).
		// The port NUMBER may be recycled — what matters is the new token hash.
		if a, err := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1"); err != nil || a.TokenHash == stale.TokenHash {
			t.Fatalf("rotate did not mint a fresh token (hash unchanged %q)", stale.TokenHash)
		}
	case <-time.After(time.Second):
		t.Fatal("no directive pushed on enable")
	}
}

// Self-review: concurrent escalations from multiple restored-behind agents must
// converge through the transactional max — the durable generation ends strictly
// above every agent's, and stays monotonic (no lost update under -race).
func TestProxyGenerationEscalationConvergesUnderConcurrency(t *testing.T) {
	_, _, _, b := proxyTestBroker(t)
	b.proxyGen = 100

	agentGens := []int64{150, 300, 220, 500, 410}
	var wg sync.WaitGroup
	for _, g := range agentGens {
		wg.Add(1)
		go func(ag int64) {
			defer wg.Done()
			b.escalateProxyGen(ag)
		}(g)
	}
	wg.Wait()

	final := b.proxyGenLoad()
	for _, g := range agentGens {
		if final <= g {
			t.Fatalf("final generation %d not above agent generation %d", final, g)
		}
	}
	// Durable row must match the in-memory value (no lost persist).
	var persisted int64
	if err := b.cfg.DB.QueryRow(`SELECT generation FROM proxy_meta WHERE id=1`).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != final {
		t.Fatalf("persisted generation %d != in-memory %d", persisted, final)
	}
}
