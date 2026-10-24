//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S5 internal review rv-authority-6
func TestPlannedProxyCutKeepsLastReadinessWithinDetachedGrace(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exercisePlannedProxyReadiness(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "planned proxy readiness", g0)
	testharness.AssertNoFDLeak(t, "planned proxy readiness", fd0, 0)
}
func exercisePlannedProxyReadiness(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: real planned carrier exercise
	defer cancel()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	address := reserveFlowTestAddress(t)
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := tunnel.FlowConfig{Budget: &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}, InstanceID: a.instanceID, OwnerGen: 1, BootID: boot, MaxEndpoints: 128, MaxPendingFDs: 32, TokenLookup: func(tunnel.AuthRequest) error { return nil }, Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
		// Initial RESUME can expose the listener before AddProxy returns. Its
		// local-connect hook must already identify the same started SS owner.
		// origin: hot-update S5 plan §6.5 / T38 — integration startup seam.
		a.proxy.mu.Lock()
		published := a.proxy.srv != nil && a.proxy.localPort != 0
		a.proxy.mu.Unlock()
		if !published {
			t.Error("SS listener metadata was not published before RESUME authority")
			return tunnel.Authority{}, errors.New("fixture SS listener metadata unavailable")
		}
		return tunnel.Authority{Enabled: true, Allocation: req.Allocation, Generation: 7, ProxyEpoch: 1, Keys: []tunnel.AuthorizedKey{{ID: "bob", Secret: []byte("private-bob")}}}, nil
	}}
	server := tunnel.NewServer(address, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, testharness.SilentLog())
	if err := server.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	adapter := NewTunnelExposeAdapter(address, a.cfg.SID, nidOf(a), testharness.SilentLog())
	cfg.Budget = &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	if err := adapter.client.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	a.cfg.ExposeAdapter = adapter
	adapter.Start(ctx)
	defer stopExposeAdapter(a)
	down := make(chan struct{}, 4)
	adapter.SetSessionStateHook(func(port int, up bool) {
		a.onTunnelSessionState(port, up)
		if !up {
			down <- struct{}{}
		}
	})
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	a.ncBox.Store(nc)
	sub, err := nc.SubscribeSync(proto.SubjEvNodeProxyReady(a.cfg.SID, nidOf(a), "unready"))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(sub.Unsubscribe)
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	public := reserveFlowTestAddress(t)
	_, portText, _ := net.SplitHostPort(public)
	port, _ := strconv.Atoi(portText)
	defer adapter.client.Close(port)
	a.applyProxyDirective(nc, &proto.ProxyDirective{Enabled: true, Generation: 7, Epoch: 1, PublicPort: port, Token: "private-token", Keys: []proto.ProxyKey{{SubID: "bob", Secret: "private-bob"}}})
	if !a.proxyBound() {
		t.Fatal("real ready SS listener and TLS endpoint required")
	}
	// Production closes admission before PREPARE/Freeze. The retained endpoint
	// and original port/token remain owned throughout this planned down edge.
	a.gate.close()
	// An invalid next lineage fails real PREPARE before detaching. Its unused
	// reservation must not later report a healthy attached proxy as unready.
	if _, err := freezeForUpgrade(a, time.Now().Add(tunnel.ResumeAttempt)); err == nil {
		t.Fatal("invalid next flow owner unexpectedly froze")
	}
	if a.proxy.planned.deadlineBootNS != 0 || a.proxy.planned.timer != nil || !a.proxyBound() {
		t.Fatal("failed PREPARE retained a readiness expiry for an attached carrier")
	}
	beginPlannedProxyReadiness(a)
	defer a.gate.open()
	if err := adapter.client.PrepareHandoff(2, [16]byte{61}, time.Now().Add(tunnel.ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	frozen, err := adapter.client.FreezeFlows(time.Now().Add(tunnel.ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
	select {
	case <-down:
	case <-time.After(time.Second):
		t.Fatal("actual supervisor down edge was not observed")
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if pending, _, err := sub.Pending(); err != nil || pending != 0 {
		t.Fatal("planned carrier cut emitted unready and could rotate historical reaper dwell")
	}
	var heartbeat proto.HeartbeatPayload
	if err := json.Unmarshal(heartbeatPayload(a, time.Now()), &heartbeat); err != nil {
		t.Fatal(err)
	}
	if !heartbeat.ProxyBound || adapter.client.FlowCensus().Retained != 1 {
		t.Fatal("planned cut cleared heartbeat readiness or lost retained endpoint")
	}

	saved, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	if saved.plannedDeadlineBootNS == 0 {
		t.Fatal("planned cut lacks absolute readiness expiry")
	}
	a.onTunnelSessionState(port, false)
	again, err := snapshotProxyCut(a)
	if err != nil || again.plannedDeadlineBootNS != saved.plannedDeadlineBootNS {
		t.Fatal("repeated planned drop renewed readiness grace")
	}
	encoded, err := encodeProxyCut(saved)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProxyCut(encoded)
	if err != nil || decoded.plannedDeadlineBootNS != saved.plannedDeadlineBootNS || !decoded.tunnelUp {
		t.Fatal("sealed cut lost historical readiness or its original deadline")
	}
	legacyV2, err := encodeLegacyProxyCut(saved)
	if err != nil {
		t.Fatal(err)
	}
	legacy := append([]byte(nil), legacyV2[:len(legacyV2)-8]...)
	legacy[5] = 1
	if old, err := decodeProxyCut(legacy); err != nil || old.plannedDeadlineBootNS != 0 {
		t.Fatal("prior private proxy envelope became unreadable")
	}
	receiver := newProxyTestAgent(t)
	defer stopProxyOnRunExit(receiver)
	// Model an imported cut near the end of its original grace. Activation
	// must expire that deadline, rather than starting another eight seconds.
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	decoded.plannedDeadlineBootNS = now + int64(30*time.Millisecond)
	if err := restoreProxyCut(receiver, decoded); err != nil {
		t.Fatal(err)
	}
	if !receiver.proxyTunnelUp.Load() || receiver.proxy.planned.timer != nil {
		t.Fatal("constructor lost valid historical readiness or started a timer")
	}
	receiver.ncBox.Store(nc)
	if err := activateProxyTimer(receiver); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatal("imported readiness did not expire at its original deadline")
	}
	if receiver.proxyTunnelUp.Load() {
		t.Fatal("expired planned cut remained ready")
	}
	receiver.gate.close()
	receiver.onTunnelSessionState(port, false)
	if receiver.proxyTunnelUp.Load() {
		t.Fatal("late planned callback renewed an expired cut")
	}
	// A genuine authoritative disable terminates the retained listener and
	// reports unready immediately, despite the original cut's remaining grace.
	a.applyProxyDirective(nc, &proto.ProxyDirective{Enabled: false, Generation: 7, Epoch: 2})
	if a.proxyBound() {
		t.Fatal("planned grace concealed authoritative revocation")
	}
}
