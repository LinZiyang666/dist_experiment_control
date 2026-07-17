package agent

import (
	"context"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// roster_runtime_test.go (C1 §8) — end-to-end agent-over-NATS tests of the live paths: the periodic
// roster refresh (RosterRefreshOnly → adopts a newer roster, no full re-register) and the
// stuck-reconnect session rebuild (the L3 machinery reconnects + re-registers without deadlock).

func signedRoster(t *testing.T, seed []byte, pub string, gen uint64) *proto.ClusterRoster {
	t.Helper()
	r, err := clusterroster.Build(seed, pub, gen,
		[]proto.RosterBroker{{NodeID: "b1", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter}},
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestRosterRefreshConvergesNoRaft: the boot register delivers gen 10; the periodic refresh (which
// MUST carry RosterRefreshOnly) delivers gen 20 and the agent converges to it well under the 5min
// budget. Asserts at least one refresh request set RosterRefreshOnly=true (the no-raft path).
func TestRosterRefreshConvergesNoRaft(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	var refreshOnlySeen atomic.Int32
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(msg *nats.Msg) {
		var req proto.NodeRegisterReq
		_ = json.Unmarshal(msg.Data, &req)
		gen := uint64(10)
		if req.RosterRefreshOnly {
			refreshOnlySeen.Add(1)
			gen = 20
		}
		resp := proto.NodeRegisterResp{OK: true, Roster: signedRoster(t, seed, pub, gen)}
		b, _ := json.Marshal(resp)
		_ = msg.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{
		NATSURL:               url,
		SID:                   "lab",
		NID:                   "lab-1",
		AccountPub:            pub, // OOB pin → deterministic accept
		HeartbeatInterval:     30 * time.Millisecond,
		RegisterTimeout:       2 * time.Second,
		RosterRefreshInterval: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for a.cachedRosterGen() < 20 {
		if time.Now().After(deadline) {
			t.Fatalf("agent did not converge to refreshed gen 20: have %d, refreshOnly=%d",
				a.cachedRosterGen(), refreshOnlySeen.Load())
		}
		time.Sleep(15 * time.Millisecond)
	}
	if refreshOnlySeen.Load() == 0 {
		t.Fatal("refresh must use the RosterRefreshOnly (no-raft) path, none seen")
	}
	cancel()
	<-done
}

// TestTopologyEventTriggersSignedRosterRefresh covers the consecutive-retire
// island found by simcluster-41. A healthy NATS connection does not reconnect on
// its own when its broker leaves the mesh, so a topology edge must wake the
// roster-only pull immediately; the response remains account-signed and
// generation-checked by the normal adoption path.
func TestTopologyEventTriggersSignedRosterRefresh(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	var refreshOnlySeen atomic.Int32
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(msg *nats.Msg) {
		var req proto.NodeRegisterReq
		_ = json.Unmarshal(msg.Data, &req)
		gen := uint64(10)
		if req.RosterRefreshOnly {
			refreshOnlySeen.Add(1)
			gen = 20
		}
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: signedRoster(t, seed, pub, gen)})
		_ = msg.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{
		NATSURL: url, SID: "lab", NID: "lab-1", AccountPub: pub,
		HeartbeatInterval: 30 * time.Millisecond, RegisterTimeout: 2 * time.Second,
		RosterRefreshInterval: time.Hour, // prove the event, not the periodic timer, caused the pull.
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, 3*time.Second, func() bool { return a.cachedRosterGen() == 10 })

	event, _ := json.Marshal(map[string]any{"type": "nats_topology_applied", "desired_gen": 2})
	if err := stub.Publish(proto.SubjSysEvents, event); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()
	waitFor(t, 3*time.Second, func() bool {
		return refreshOnlySeen.Load() > 0 && a.cachedRosterGen() == 20
	})
	cancel()
	<-done
}

// TestSessionRebuildReconnects: firing the stuck-reconnect watchdog (fireRedial) must rebuild the
// session — the Run loop tears the old conn down and re-connects + re-registers on the freshest
// dial pool — without deadlocking. Validates the L3 session-loop machinery.
func TestSessionRebuildReconnects(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	var registerSeen, heartbeatSeen atomic.Int32
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(msg *nats.Msg) {
		registerSeen.Add(1)
		b, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = msg.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", func(*nats.Msg) {
		heartbeatSeen.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		HeartbeatInterval: 25 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
		// Disable the periodic refresh so the only re-register is the rebuild itself.
		RosterRefreshInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Wait until the session is fully established (heartbeats flowing → sessCancel published).
	waitFor(t, 3*time.Second, func() bool { return registerSeen.Load() >= 1 && heartbeatSeen.Load() >= 1 })
	before := registerSeen.Load()

	// Fire the watchdog: simulate a stuck reconnect.
	a.fireRedial()

	// The rebuild must reconnect and re-register on the same (seed) broker.
	waitFor(t, 4*time.Second, func() bool { return registerSeen.Load() > before })

	// The agent is still running (not exited) — cancel cleanly.
	select {
	case err := <-done:
		t.Fatalf("agent exited unexpectedly during rebuild: %v", err)
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not exit on cancel after rebuild")
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within deadline")
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// TestRebuildNoDoubleForwardedDispatch (Stage-C TEST-3): after a session rebuild, a single forwarded
// exec must be dispatched EXACTLY once — proving there are not two live cmd.*.forwarded
// subscriptions (the centerpiece L3 no-double-dispatch invariant). An empty-argv exec yields one
// deterministic "argv required" reply per dispatch.
func TestRebuildNoDoubleForwardedDispatch(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	var registerSeen, heartbeatSeen atomic.Int32
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(m *nats.Msg) {
		registerSeen.Add(1)
		b, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", func(*nats.Msg) { heartbeatSeen.Add(1) }); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{NATSURL: url, SID: "lab", NID: "lab-1", HeartbeatInterval: 25 * time.Millisecond, RegisterTimeout: 2 * time.Second, RosterRefreshInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, 3*time.Second, func() bool { return registerSeen.Load() >= 1 && heartbeatSeen.Load() >= 1 })

	before := registerSeen.Load()
	a.fireRedial()
	waitFor(t, 4*time.Second, func() bool { return registerSeen.Load() > before && heartbeatSeen.Load() >= before+1 })

	// Count agent dispatches of one forwarded exec (empty argv → exactly one error chunk per dispatch).
	var replies atomic.Int32
	inbox := nats.NewInbox()
	if _, err := stub.Subscribe(inbox, func(*nats.Msg) { replies.Add(1) }); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()
	payload, _ := json.Marshal(proto.ExecReq{}) // empty argv → "argv required"
	if err := stub.PublishMsg(&nats.Msg{Subject: proto.SubjCmdForwarded("lab", "lab-1", "exec"), Reply: inbox, Data: payload}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()
	time.Sleep(500 * time.Millisecond)
	if got := replies.Load(); got != 1 {
		t.Fatalf("a forwarded exec must be dispatched EXACTLY once after a rebuild (no double subscription), got %d", got)
	}
	cancel()
	<-done
}

// TestRefreshConvergesUnderLeaderFailover (Stage-C TEST-12): the FIRST refresh tick gets a transient
// CodeLeaderUnavailable; the agent must still converge to the newer roster via the SHORT fail-backoff
// (not a full interval later), well under the 5min budget.
func TestRefreshConvergesUnderLeaderFailover(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	seed, _ := auth.GenerateUserSeed()
	pub, _ := auth.PublicKeyFromSeed(seed)
	var refreshAttempts atomic.Int32
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(m *nats.Msg) {
		var req proto.NodeRegisterReq
		_ = json.Unmarshal(m.Data, &req)
		if req.RosterRefreshOnly {
			// Fail the first refresh with a transient leader failover, then serve gen 20.
			if refreshAttempts.Add(1) == 1 {
				b, _ := json.Marshal(proto.NodeRegisterResp{OK: false, Code: proto.CodeLeaderUnavailable, Error: "failover"})
				_ = m.Respond(b)
				return
			}
			b, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: signedRoster(t, seed, pub, 20)})
			_ = m.Respond(b)
			return
		}
		b, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: signedRoster(t, seed, pub, 10)})
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{NATSURL: url, SID: "lab", NID: "lab-1", AccountPub: pub, HeartbeatInterval: 30 * time.Millisecond, RegisterTimeout: 2 * time.Second, RosterRefreshInterval: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	a.rosterRefreshFailBackoff = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, 3*time.Second, func() bool { return a.cachedRosterGen() >= 20 })
	if refreshAttempts.Load() < 2 {
		t.Fatalf("expected a failed refresh then a retry; attempts=%d", refreshAttempts.Load())
	}
	cancel()
	<-done
}

// TestRebuildNoGoroutineLeak (Stage-C TEST-5, light): several rebuild cycles + clean cancel must not
// grow the goroutine count (the refresh loop + watchdog must all unwind).
func TestRebuildNoGoroutineLeak(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	var registerSeen atomic.Int32
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(m *nats.Msg) {
		registerSeen.Add(1)
		b, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{NATSURL: url, SID: "lab", NID: "lab-1", HeartbeatInterval: 25 * time.Millisecond, RegisterTimeout: 2 * time.Second, RosterRefreshInterval: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, 3*time.Second, func() bool { return registerSeen.Load() >= 1 })

	base := runtime.NumGoroutine()
	for i := 0; i < 4; i++ {
		before := registerSeen.Load()
		a.fireRedial()
		waitFor(t, 4*time.Second, func() bool { return registerSeen.Load() > before })
	}
	cancel()
	<-done
	// Allow lingering goroutines (drain/AfterFunc) to unwind, poll with tolerance.
	deadline := time.Now().Add(2 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= base+4 { // tolerance for test-runtime scheduler noise
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak after rebuild cycles: base=%d now=%d", base, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
