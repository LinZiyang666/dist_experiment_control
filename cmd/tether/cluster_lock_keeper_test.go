package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_lock_keeper_test.go (R7b) — the HOLDER half of the lease.
//
// internal/broker proves the reaper side: a lock whose lease lapsed is released, and a lock whose lease is
// being renewed is not. Those proofs are worth nothing unless something actually renews. Without this
// keeper the lease turns every membership lock into a 15-minute fuse under a LIVE operation, which is
// worse than the leak it replaces — so these tests exist to make "nobody is renewing" a detectable
// regression rather than a silent one.

// syncBuffer is a mutex-guarded bytes.Buffer. The keeper writes its progress/warning lines from its own
// goroutine while these tests read them, so an unguarded bytes.Buffer is a race in the TEST, not in the
// code under test — and a -race failure there would mask real findings.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// --------------------------------------------------------------------------
// the keeper loop
// --------------------------------------------------------------------------

// TestKeeperRenewsUntilStopped is the property the whole mechanism rests on: while the orchestrator lives,
// renewals keep landing. If this ever stops being true, every long-running grow/roll loses its lock
// mid-flight and two membership changes can interleave.
func TestKeeperRenewsUntilStopped(t *testing.T) {
	var renewals int32
	var out syncBuffer
	k := startLockKeeper(context.Background(), "grow", time.Millisecond,
		func(context.Context) (bool, error) { atomic.AddInt32(&renewals, 1); return true, nil }, &out)

	waitFor(t, time.Second, func() bool { return atomic.LoadInt32(&renewals) >= 5 },
		"the keeper never renewed — a leased lock with no renewer expires under a LIVE operation")
	k.Stop()

	settled := atomic.LoadInt32(&renewals)
	time.Sleep(20 * time.Millisecond)
	if after := atomic.LoadInt32(&renewals); after != settled {
		t.Fatalf("the keeper renewed %d more times AFTER Stop() — a keeper that outlives its orchestrator keeps a "+
			"dead operation's lock alive forever, which is the permanent fence R7b removes", after-settled)
	}
	if k.Renewals() < 5 {
		t.Fatalf("Renewals() = %d, want >= 5", k.Renewals())
	}
}

// TestKeeperSurvivesTransientFailures: a renewal failure is NOT evidence the lock is gone. An election, a
// dropped reply, a momentarily-unreachable leader — the TTL is sized (lock_lease.go) to survive two
// consecutive misses, so the keeper must keep trying rather than give up or declare the lock lost.
func TestKeeperSurvivesTransientFailures(t *testing.T) {
	var calls, oks int32
	var out syncBuffer
	k := startLockKeeper(context.Background(), "upgrade", time.Millisecond, func(context.Context) (bool, error) {
		if n := atomic.AddInt32(&calls, 1); n <= 3 {
			return true, errors.New("no writable leader visible (election in progress?)")
		}
		atomic.AddInt32(&oks, 1)
		return true, nil
	}, &out)
	defer k.Stop()

	waitFor(t, time.Second, func() bool { return atomic.LoadInt32(&oks) >= 3 },
		"the keeper gave up after transient failures — a single election would then expire a live lock")
	if k.Lost() {
		t.Fatal("a transient renewal failure was mistaken for losing the lock; the keeper must only latch on the broker's explicit terminal reply")
	}
	if !strings.Contains(out.String(), "renewal failed") {
		t.Fatalf("the operator must SEE renewals failing before the lease runs out; got %q", out.String())
	}
}

// TestKeeperLatchesAndStopsOnTerminal: when the broker says the lock is not ours, retrying is pointless (a
// renewal is structurally incapable of re-acquiring). The keeper must stop AND latch, because the
// orchestrator's exit code depends on it.
func TestKeeperLatchesAndStopsOnTerminal(t *testing.T) {
	var calls int32
	var out syncBuffer
	k := startLockKeeper(context.Background(), "grow", time.Millisecond,
		func(context.Context) (bool, error) { atomic.AddInt32(&calls, 1); return false, nil }, &out)
	defer k.Stop()

	waitFor(t, time.Second, func() bool { return k.Lost() },
		"the keeper did not latch the terminal 'lock not held' reply")

	settled := atomic.LoadInt32(&calls)
	time.Sleep(20 * time.Millisecond)
	if after := atomic.LoadInt32(&calls); after != settled {
		t.Fatalf("the keeper kept renewing (%d more) after being told the lock is not held — it would renew somebody "+
			"ELSE's lock, or spin against a released one, for the rest of the operation", after-settled)
	}
	if !strings.Contains(out.String(), "lock LOST") {
		t.Fatalf("losing the membership mutex mid-operation must be LOUD; got %q", out.String())
	}
	// And it must convert into a non-zero exit class.
	err := k.LostErr("cluster add completed but lost its lock")
	if err == nil {
		t.Fatal("LostErr returned nil")
	}
	if rc := classifyExit(err); rc == exitOK {
		t.Fatal("an operation that ran without the membership mutex must NOT exit 0 — automation cannot distinguish it from a serialized run")
	}
}

// TestKeeperStopIsIdempotentAndJoins: both orchestrators call Stop() explicitly on the success path AND
// via defer, so a second Stop must not panic on a closed channel. Stop must also JOIN — the repo's leak
// gate counts goroutines, and a keeper still winding down when its command returns is a leak.
func TestKeeperStopIsIdempotentAndJoins(t *testing.T) {
	var out syncBuffer
	running := make(chan struct{}, 1)
	k := startLockKeeper(context.Background(), "grow", time.Millisecond, func(context.Context) (bool, error) {
		select {
		case running <- struct{}{}:
		default:
		}
		return true, nil
	}, &out)
	<-running
	k.Stop()
	k.Stop() // must not panic
	k.Stop()
}

// TestKeeperExitsOnContextCancel: a ctl killed with ^C tears its context down; the keeper must exit with
// it rather than outliving the process's own cancellation.
func TestKeeperExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out syncBuffer
	k := startLockKeeper(ctx, "upgrade", time.Millisecond, func(context.Context) (bool, error) { return true, nil }, &out)
	cancel()

	done := make(chan struct{})
	go func() { k.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the keeper did not exit after its context was cancelled")
	}
}

// TestKeeperLeavesNoGoroutines is the repo's NumGoroutine poll-with-tolerance leak gate, applied to the
// one long-lived object R7b adds. Deliberately NOT goleak (see test/concurrency/helpers_test.go).
func TestKeeperLeavesNoGoroutines(t *testing.T) {
	settle := func() int {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	before := settle()

	var out syncBuffer
	for i := 0; i < 25; i++ {
		k := startLockKeeper(context.Background(), "grow", time.Millisecond,
			func(context.Context) (bool, error) { return true, nil }, &out)
		time.Sleep(2 * time.Millisecond)
		k.Stop()
	}
	// A terminal-reply keeper returns on its own; Stop() must still join cleanly.
	for i := 0; i < 25; i++ {
		k := startLockKeeper(context.Background(), "upgrade", time.Millisecond,
			func(context.Context) (bool, error) { return false, nil }, &out)
		k.Stop()
	}

	after := settle()
	if after-before > 2 {
		t.Fatalf("goroutine leak: %d before, %d after 50 keeper lifecycles (tolerance 2) — the keeper is a "+
			"long-lived object added by R7b and must not survive its Stop()", before, after)
	}
}

// waitFor polls pred until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, pred func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// --------------------------------------------------------------------------
// the renewal wire calls
// --------------------------------------------------------------------------

// TestRenewLeaseRereolvesTheLeaderEveryCall pins the property that makes the keeper work during a rolling
// upgrade at all: `cluster upgrade` MOVES leadership off every host it rolls, so a leader cached at
// acquire time is wrong for most of the roll. If the leader were cached, every renewal after the first
// transfer would fail and the lease would lapse mid-roll.
func TestRenewLeaseReresolvesTheLeaderEveryCall(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-r7b-leadermove"

	// The health responder reports a DIFFERENT leader on each probe — the roll moving leadership.
	var probe int32
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		leader := fmt.Sprintf("brk-%d", atomic.AddInt32(&probe, 1))
		body, _ := json.Marshal(proto.ClusterHealthResp{NodeID: leader, WritableLeaderConfirmed: true, LeaderID: leader})
		_ = m.Respond(body)
	})

	// The responder runs on a NATS callback goroutine; the assertions run here. Guard the slice or -race
	// reports the test itself, not the code.
	var mu sync.Mutex
	var targets []string
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		if req.Op != "renew-lock" {
			return
		}
		mu.Lock()
		targets = append(targets, req.TargetNode)
		mu.Unlock()
		body, _ := json.Marshal(proto.ClusterUpgradeResp{OK: true})
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		held, rerr := renewUpgradeLease(context.Background(), nc, actor, seed)
		if rerr != nil || !held {
			t.Fatalf("renewal %d: held=%v err=%v", i+1, held, rerr)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(targets) != 3 {
		t.Fatalf("expected 3 renewals, got %d", len(targets))
	}
	if targets[0] == targets[1] && targets[1] == targets[2] {
		t.Fatalf("every renewal went to the SAME leader (%v) — the leader is being cached, so a rolling upgrade "+
			"would fail every renewal after its first leadership transfer and the lease would lapse mid-roll", targets)
	}
}

// TestRenewLeaseClassifiesRepliesCorrectly: only the broker's explicit lock_not_held is terminal.
// Misclassifying a transient as terminal aborts a healthy operation; misclassifying terminal as transient
// means the keeper renews a lock somebody else now holds.
func TestRenewLeaseClassifiesRepliesCorrectly(t *testing.T) {
	cases := []struct {
		name      string
		resp      proto.ClusterGrowResp
		wantHeld  bool
		wantError bool
	}{
		{"ok", proto.ClusterGrowResp{OK: true}, true, false},
		{"lock not held is TERMINAL", proto.ClusterGrowResp{Code: clusterLockNotHeldCode, Error: "holder=brk-other"}, false, false},
		{"not_leader is TRANSIENT", proto.ClusterGrowResp{Code: "not_leader", Error: "leadership moved"}, true, true},
		{"store_error is TRANSIENT", proto.ClusterGrowResp{Code: "store_error", Error: "raft down"}, true, true},
		// NOTE: "cluster_not_ready" is deliberately absent. sendGrowTrigger swallows that code in its own
		// converge-poll loop (it means "broker still starting — keep waiting"), so it never reaches the
		// classification switch under test, and including it here would just make the case sit out the
		// full growConvergeTimeout.
	}
	// One embedded NATS server for the whole table: each subtest gets its own actor, so the subjects do
	// not collide. Starting a server per row cost more wall-clock than every assertion in this file.
	url := startCLIExternalReviewNATS(t)
	seed, _ := cliExternalAccount(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			actor := "Uactor-r7b-" + strings.ReplaceAll(tc.name, " ", "-")
			serveLeaderHealth(t, nc, actor, "brk-leader")
			mustSub(t, nc, proto.SubjCtrlClusterGrow(actor), func(m *nats.Msg) {
				if m.Reply == "" {
					return
				}
				body, _ := json.Marshal(tc.resp)
				_ = m.Respond(body)
			})
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}

			held, rerr := renewGrowLease(context.Background(), nc, actor, seed, "brk-joiner")
			if held != tc.wantHeld {
				t.Fatalf("held=%v, want %v — a terminal reply misread as transient makes the keeper renew a lock it "+
					"does not own; a transient misread as terminal aborts a healthy operation", held, tc.wantHeld)
			}
			if (rerr != nil) != tc.wantError {
				t.Fatalf("err=%v, wantError=%v", rerr, tc.wantError)
			}
		})
	}
}

// TestRenewLeaseTreatsAnUnresolvableLeaderAsTransient: no writable leader means an election is running,
// not that the lock changed hands. Declaring the lock lost here would abort every operation that spans an
// election.
func TestRenewLeaseTreatsAnUnresolvableLeaderAsTransient(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)

	held, rerr := renewGrowLease(context.Background(), nc, "Uactor-r7b-noleader", seed, "brk-joiner")
	if !held {
		t.Fatal("an unresolvable leader was treated as losing the lock — every operation spanning an election would abort")
	}
	if rerr == nil {
		t.Fatal("the failure must be surfaced so the operator sees renewals failing before the lease runs out")
	}
}

// TestProductionRenewCadenceIsTheLeaseInterval guards the wiring: the orchestrators must renew at
// cluster.LockLeaseRenewInterval, not at some interval that happens to be shorter than the TTL today.
func TestProductionRenewCadenceIsTheLeaseInterval(t *testing.T) {
	if growLeaseRenewInterval != cluster.LockLeaseRenewInterval {
		t.Fatalf("grow keeper renews every %s, but the lease is sized for %s", growLeaseRenewInterval, cluster.LockLeaseRenewInterval)
	}
	if upgradeLeaseRenewInterval != cluster.LockLeaseRenewInterval {
		t.Fatalf("upgrade keeper renews every %s, but the lease is sized for %s", upgradeLeaseRenewInterval, cluster.LockLeaseRenewInterval)
	}
	if growLeaseRenewInterval >= cluster.LockLeaseTTL {
		t.Fatal("the renew cadence is not shorter than the TTL — the lock expires before it is ever renewed")
	}
}

// --------------------------------------------------------------------------
// THE WIRING (the half-wired hazard this batch was written to avoid)
// --------------------------------------------------------------------------

// TestDriveUpgradeActuallyStartsAKeeper closes the failure mode that makes this whole file necessary:
// every piece of the lease can exist, compile, and pass its own unit tests while NOTHING CALLS THE KEEPER.
// That state is not merely incomplete — it is a REGRESSION, because the acquire stamps a 15-minute lease
// that nobody renews, so a live roll loses its lock on a fixed timer.
//
// The test drives a real `cluster upgrade` far enough to take the lock, stalls it inside the roll (a slow
// leadership probe), and asserts renewals land on the wire. It then asserts the keeper STOPS when the roll
// HALTs — the abandonment path where the lease is supposed to start running down.
func TestDriveUpgradeActuallyStartsAKeeper(t *testing.T) {
	prev := upgradeLeaseRenewInterval
	upgradeLeaseRenewInterval = time.Millisecond
	t.Cleanup(func() { upgradeLeaseRenewInterval = prev })

	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-r7b-wiring"

	// A deliberately SLOW health responder: it holds the roll inside waitLeader long enough for the
	// compressed renew cadence to fire many times.
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		time.Sleep(2 * time.Millisecond)
		body, _ := json.Marshal(proto.ClusterHealthResp{
			NodeID: "brk-leader", WritableLeaderConfirmed: true, LeaderID: "brk-leader",
		})
		_ = m.Respond(body)
	})

	var mu sync.Mutex
	var renews int
	var sawAcquire bool
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		resp := proto.ClusterUpgradeResp{OK: true}
		mu.Lock()
		switch req.Op {
		case "acquire-lock":
			sawAcquire = true
		case "renew-lock":
			renews++
		case "transfer-leader":
			// Refuse, so the roll HALTs — the abandonment path.
			resp = proto.ClusterUpgradeResp{Code: "store_error", Error: "injected refusal"}
		}
		mu.Unlock()
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// One step that transfers leadership first: acquire-lock → (keeper starts) → waitLeader spins on the
	// slow health probe → transfer-leader refused → HALT.
	plan := clusterupgrade.Plan{Steps: []clusterupgrade.Step{
		{Kind: clusterupgrade.StepUpgrade, NodeID: "brk-a", TransferTo: "brk-b"},
	}}

	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(context.Background())

	err = driveUpgrade(cmd, nc, actor, "sid", seed, plan, "v9.9.9", "", "")
	if err == nil {
		t.Fatal("the injected transfer-leader refusal must HALT the roll")
	}

	mu.Lock()
	acquired, n := sawAcquire, renews
	mu.Unlock()
	if !acquired {
		t.Fatal("fixture precondition: the roll never took the lock")
	}
	if n == 0 {
		t.Fatal("driveUpgrade took the roll lock and NEVER RENEWED IT. The acquire stamps a lease that expires in " +
			"LockLeaseTTL, so with no keeper wired the reaper will release the lock out from under a LIVE roll — " +
			"strictly worse than the leak R7b set out to fix. The keeper exists but nothing calls it.")
	}

	// The HALT path must have stopped the keeper (driveUpgrade defers Stop on every return). If it had
	// not, renewals would keep arriving after the function returned — an abandoned roll whose lock never
	// decays, i.e. the permanent fence, restored.
	mu.Lock()
	settled := renews
	mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	after := renews
	mu.Unlock()
	if after != settled {
		t.Fatalf("%d renewals arrived AFTER driveUpgrade returned — the keeper outlived the HALT, so the abandoned "+
			"roll lock would be renewed forever and never decay", after-settled)
	}
}
