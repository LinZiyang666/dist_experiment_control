package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// home_delivery_test.go (formerly r8_home_delivery_test.go; R8a P1) — the batch's one-vote-veto invariant.
//
//	Delivery of a home change must NOT be conditional on the peer producing any event.
//
// Every test here keeps the fake agent COMPLETELY SILENT in the sense that matters:
// it never registers, never re-registers, never publishes a heartbeat, never sends a
// command, and never reconnects. It only listens. If the data plane still follows a
// drain, the invariant holds; if the only thing that ever moved a home was a
// reconnect, every one of these goes red.

// homeTestAgent is a listen-only fake agent. It subscribes to its own forwarded-
// command subject and acks pushed home directives — nothing else. It deliberately
// exposes NO way to register or heartbeat, so no test in this file can accidentally
// satisfy the invariant through the old reconnect path.
type homeTestAgent struct {
	mu       sync.Mutex
	received []proto.HomeAssignment
	ackAll   bool // when false the agent applies nothing and acks nothing (a stalled rehome)
	nc       *nats.Conn
	sub      *nats.Subscription
}

func newHomeTestAgent(t *testing.T, url, sid, nid string, ackAll bool) *homeTestAgent {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("agent connect: %v", err)
	}
	a := &homeTestAgent{ackAll: ackAll, nc: nc}
	sub, err := nc.Subscribe(proto.SubjCmdForwarded(sid, nid, homeDeliveryVerb), func(msg *nats.Msg) {
		var ha proto.HomeAssignment
		if json.Unmarshal(msg.Data, &ha) != nil {
			return
		}
		a.mu.Lock()
		a.received = append(a.received, ha)
		ack := a.ackAll
		a.mu.Unlock()
		if !ack || msg.Reply == "" {
			return
		}
		// Mirror the REAL agent faithfully: it acks PER PORT — one single-directive message per
		// applied home, ALL published to this push's single reply subject (ackHomeApplied,
		// home_push.go:88, called once per port). A batched single-message echo would hide the
		// single-use-token / per-port-ack defect (self-review blocker), so fan out here too.
		for _, d := range ha.Directives {
			single, _ := json.Marshal(&proto.HomeAssignment{Directives: []proto.HomeDirective{d}})
			_ = a.nc.Publish(msg.Reply, single)
		}
	})
	if err != nil {
		t.Fatalf("agent subscribe: %v", err)
	}
	a.sub = sub
	t.Cleanup(func() { _ = sub.Unsubscribe(); nc.Close() })
	return a
}

func (a *homeTestAgent) setAck(v bool) {
	a.mu.Lock()
	a.ackAll = v
	a.mu.Unlock()
}

func (a *homeTestAgent) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.received)
}

// lastHomeFor returns the (broker addr, epoch) of the newest directive for port.
func (a *homeTestAgent) lastHomeFor(port int) (string, int64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := len(a.received) - 1; i >= 0; i-- {
		for _, d := range a.received[i].Directives {
			if d.PublicPort == port {
				return d.BrokerAddr, d.Epoch, true
			}
		}
	}
	return "", 0, false
}

// peerSilenceMonitor counts every message an agent could produce on the three
// agent-pub lifecycle subjects (register / unregister / heartbeat).
//
// It exists because the invariant is about CAUSATION, not just outcome. Without it,
// a test could pass because the fake agent happened to re-register for some unrelated
// reason — i.e. through the very reconnect-gated path this batch replaces — and the
// bug would look fixed whether or not it was. Asserting the count is UNCHANGED across
// the drain turns "the data plane followed" into "the data plane followed WITHOUT the
// peer emitting anything", which is the actual claim.
type peerSilenceMonitor struct {
	mu    sync.Mutex
	n     int
	which []string
}

func newPeerSilenceMonitor(t *testing.T, url, sid, nid string) *peerSilenceMonitor {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("silence monitor connect: %v", err)
	}
	m := &peerSilenceMonitor{}
	for _, subj := range []string{
		proto.SubjNodeRegister(sid, nid),
		proto.SubjNodeUnregister(sid, nid),
		proto.SubjNodeHeartbeat(sid, nid),
	} {
		s := subj
		sub, err := nc.Subscribe(s, func(*nats.Msg) {
			m.mu.Lock()
			m.n++
			m.which = append(m.which, s)
			m.mu.Unlock()
		})
		if err != nil {
			t.Fatalf("silence monitor subscribe %s: %v", s, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("silence monitor flush: %v", err)
	}
	t.Cleanup(nc.Close)
	return m
}

// count returns how many lifecycle messages the peer has emitted so far.
func (m *peerSilenceMonitor) count() (int, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n, append([]string(nil), m.which...)
}

// homeDeliveryBroker builds a CLUSTER-mode broker wired to a live NATS conn, with
// the ack inbox subscribed — i.e. the production delivery chain minus raft.
func homeDeliveryBroker(t *testing.T, url, selfID string) (*Broker, *nats.Conn) {
	t.Helper()
	db := openDB(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("broker connect: %v", err)
	}
	t.Cleanup(nc.Close)
	b := &Broker{clusterMode: true, selfID: selfID}
	b.cfg = Config{DB: db, Logger: silentLogger(), Now: time.Now, HomeDeliverInterval: 5 * time.Second}
	b.nc.Store(nc)
	sub, err := b.subscribeHomeAcks(nc)
	if err != nil {
		t.Fatalf("subscribe home acks: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return b, nc
}

// seedHomeApplied simulates a genuine agent applied-ack under the token model
// (external review C1/M-1): record the directive as OUTSTANDING exactly as a push
// would, then deliver the ack on that token's reply subject. A raw handleHomeAck with
// no token is now (correctly) ignored, so tests that assert convergence must go
// through the issued-token path a real agent travels.
func seedHomeApplied(b *Broker, port int, epoch int64) {
	token := b.recordHomeOutstanding([]proto.HomeDirective{{PublicPort: port, Epoch: epoch}}, b.now())
	body, _ := json.Marshal(&proto.HomeAssignment{Directives: []proto.HomeDirective{{PublicPort: port, Epoch: epoch}}})
	b.handleHomeAck(&nats.Msg{Subject: b.homeDelivery().inbox + "." + token, Data: body})
}

// waitFor polls pred until true or the deadline; returns whether it became true.
func waitFor(d time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return pred()
}

// rehomeRow simulates what DrainNode's raft write does to the replicated row: it
// re-points home_broker and bumps the epoch. Nothing else — no notification, no
// disconnect, no lame-duck. That is EXACTLY the production drain's footprint, and
// the point of the invariant is that this alone must reach the data plane.
func rehomeRow(t *testing.T, b *Broker, port int, newHome string, newEpoch int64) {
	t.Helper()
	if _, err := b.cfg.DB.Exec(
		`UPDATE port_allocations SET home_broker=?, epoch=? WHERE port=?`, newHome, newEpoch, port); err != nil {
		t.Fatalf("rehome row: %v", err)
	}
}

// TestHomeDeliveryPrunesAttemptsForDepartedTargets (external review N-9) proves the pass prunes the
// attempts backoff map for (sid,nid) pairs that no longer own any homed expose, WITHOUT touching a
// still-live target's earned backoff. The leak this closes: a non-converged node whose expose is
// released is never enumerated again, so homeDeliveryReset (the only other delete) never fires and its
// attempts entry lives forever — a slow map leak the NumGoroutine/fd gate cannot see.
func TestHomeDeliveryPrunesAttemptsForDepartedTargets(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedClusterNode(t, b, "brk-b", "brk-b", "brk-b:7000", "fpB", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-b", 2)

	// One pass at t0: the live, un-converged target earns a real attempts entry (pushed, no ack).
	t0 := time.Now()
	if err := b.reconcileHomeDelivery(context.Background(), t0); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	hd := b.homeDelivery()
	hd.mu.Lock()
	live, hasLive := hd.attempts["lab|lab-1"]
	var liveBackoff time.Duration
	if hasLive {
		liveBackoff = live.backoff
	}
	// Inject a ghost: a (sid,nid) that earned a backoff and then had its expose released — the leak shape.
	hd.attempts["gone|gone"] = &homeDeliveryAttempt{want: "stale", nextAt: t0.Add(time.Hour), backoff: time.Minute}
	hd.mu.Unlock()
	if !hasLive {
		t.Fatal("the live un-converged target must have earned an attempts entry after one pass")
	}

	// Second pass at the SAME t0: the live target is NOT due (nextAt is in the future), so it must be
	// preserved with its backoff intact; the ghost — no longer a live key — must be pruned.
	if err := b.reconcileHomeDelivery(context.Background(), t0); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	hd.mu.Lock()
	_, ghostStill := hd.attempts["gone|gone"]
	stillLive, liveStill := hd.attempts["lab|lab-1"]
	hd.mu.Unlock()
	if ghostStill {
		t.Fatal("a departed (sid,nid) attempts entry must be pruned by the pass (N-9)")
	}
	if !liveStill {
		t.Fatal("a still-live target's attempts entry must NOT be pruned")
	}
	if stillLive.backoff != liveBackoff {
		t.Fatalf("prune must not disturb a live target's earned backoff: was %v now %v", liveBackoff, stillLive.backoff)
	}

	// Now RELEASE the live target's expose (a real drain / expose-rm) so it leaves the target set, then
	// run a pass: its entry — un-converged and no longer enumerated — must be pruned too.
	if _, err := b.cfg.DB.Exec(`DELETE FROM port_allocations WHERE port=?`, 14000); err != nil {
		t.Fatalf("release expose: %v", err)
	}
	if err := b.reconcileHomeDelivery(context.Background(), t0.Add(time.Hour)); err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	hd.mu.Lock()
	_, orphanStill := hd.attempts["lab|lab-1"]
	hd.mu.Unlock()
	if orphanStill {
		t.Fatal("an un-converged target whose expose was released must have its attempts entry pruned (N-9 leak)")
	}
}

// TestHomeDeliveryFollowsADrainWithASilentAgent is THE exit assertion of R8a.
//
// The agent connects once and then does NOTHING: no register, no heartbeat, no
// reconnect, no command. A "drain" re-points its expose's home through the DB.
// Within a bounded number of reconcile ticks the agent must be told about the new
// home. Before R8 this was structurally impossible: homeForRegister was reachable
// only from handleRegister, so with no register there was no delivery, ever.
func TestHomeDeliveryFollowsADrainWithASilentAgent(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	agent := newHomeTestAgent(t, url, "lab", "lab-1", true)
	silence := newPeerSilenceMonitor(t, url, "lab", "lab-1")

	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedClusterNode(t, b, "brk-b", "brk-b", "brk-b:7000", "fpB", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 1)

	// Steady state: the pass delivers the CURRENT home once and the agent acks it.
	if err := b.reconcileHomeDelivery(context.Background(), time.Now()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return b.homeAppliedEpoch(14000) >= 1 }) {
		t.Fatal("the agent never received/acked the initial home directive")
	}

	// Baseline the peer's event count. It is already 0 (this fake cannot register),
	// but reading it here is what makes the post-drain comparison a comparison.
	beforeN, _ := silence.count()
	if beforeN != 0 {
		t.Fatalf("the fake agent emitted %d lifecycle message(s) before the drain; it must be listen-only", beforeN)
	}

	// --- the drain: a pure control-plane write, and NOTHING else ---
	rehomeRow(t, b, 14000, "brk-b", 2)

	// The agent stays silent. Only the reconcile pass runs.
	now := time.Now()
	for i := 0; i < 10; i++ {
		if err := b.reconcileHomeDelivery(context.Background(), now.Add(time.Duration(i)*b.cfg.HomeDeliverInterval)); err != nil {
			t.Fatalf("pass: %v", err)
		}
		if waitFor(300*time.Millisecond, func() bool { return b.homeAppliedEpoch(14000) >= 2 }) {
			break
		}
	}
	addr, epoch, ok := agent.lastHomeFor(14000)
	if !ok {
		t.Fatal("INVARIANT VIOLATED: a silent agent was never told its expose moved")
	}
	if addr != "brk-b:7000" || epoch != 2 {
		t.Fatalf("INVARIANT VIOLATED: agent's newest home = (%s, epoch %d), want (brk-b:7000, epoch 2)", addr, epoch)
	}
	if got := b.homeAppliedEpoch(14000); got < 2 {
		t.Fatalf("applied epoch = %d, want >= 2 (the ack must close the loop)", got)
	}

	// THE CAUSATION HALF. The data plane followed — now prove it did NOT follow
	// because the peer produced an event. If this count moved, the delivery could
	// have ridden the old reconnect-gated path and the test above proves nothing.
	afterN, which := silence.count()
	if afterN != beforeN {
		t.Fatalf("the peer emitted %d lifecycle message(s) across the drain (%v); "+
			"the invariant requires delivery WITHOUT any peer event, so this result is not attributable to the pass",
			afterN-beforeN, which)
	}
}

// TestHomeDeliveryConvergesEveryPortOfAMultiExposeAgent is the self-review regression for the
// per-port-ack blocker (C1). A node with >=2 homed exposes gets ONE push carrying ALL directives
// (home_convergence.go: "a node with 20 exposes gets one push"), and the real agent acks PER PORT — N
// separate messages on the ONE reply subject. The broker must record convergence for EVERY port, not
// just whichever ack consumed the token first. With the single-use-per-push token this RED-ed: only one
// port's applied epoch advanced, and drain/retire/upgrade reported a false non-convergence forever.
func TestHomeDeliveryConvergesEveryPortOfAMultiExposeAgent(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	_ = newHomeTestAgent(t, url, "lab", "lab-1", true)
	seedClusterNode(t, b, "brk-b", "brk-b", "brk-b:7000", "fpB", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th0", "brk-b", 3)
	seedHomedExpose(t, b, "lab", "lab-1", "api", 14001, "th1", "brk-b", 3)

	// One pass: both directives ride one token; the agent acks each port separately.
	if err := b.reconcileHomeDelivery(context.Background(), time.Now()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		return b.homeAppliedEpoch(14000) >= 3 && b.homeAppliedEpoch(14001) >= 3
	}) {
		t.Fatalf("not every port of a multi-expose agent converged: 14000=%d 14001=%d (want both >=3) — a "+
			"single-use token dropped the second per-port ack", b.homeAppliedEpoch(14000), b.homeAppliedEpoch(14001))
	}
}

// TestHomeAckPerDirectiveTokenRejectsSiblingForgery is the SR-8 (external re-review HIGH) regression: the
// ack rides the broker _INBOX, and EVERY agent may Sub _INBOX.> (auth/permissions.go), so a token is
// DISCLOSED on the shared bus the moment its port is acked. If one token covered a whole multi-port push,
// a token disclosed by the FIRST port's ack would authorize a forged ack for a still-un-acked SIBLING port
// — cross-session false convergence that bypasses the drain/retire/upgrade rc gate. This proves (a) a
// multi-port push mints one token PER DIRECTIVE, and (b) a token disclosed by one port's ack does NOT
// authorize a sibling.
func TestHomeAckPerDirectiveTokenRejectsSiblingForgery(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	seedClusterNode(t, b, "brk-b", "brk-b", "brk-b:7000", "fpB", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th0", "brk-b", 5)
	seedHomedExpose(t, b, "lab", "lab-1", "api", 14001, "th1", "brk-b", 5)

	ha := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{})
	if ha == nil || len(ha.Directives) != 2 {
		t.Fatalf("fixture: want a 2-directive assignment, got %+v", ha)
	}
	b.pushHomeAssignment(b.nc.Load(), "lab", "lab-1", ha)

	// (a) per-DIRECTIVE tokens: exactly two outstanding tokens, EACH covering a single port.
	hd := b.homeDelivery()
	hd.mu.Lock()
	tokenFor := map[int]string{}
	for tok, o := range hd.outstanding {
		if len(o.dirs) != 1 {
			hd.mu.Unlock()
			t.Fatalf("a multi-port push minted a token covering %d ports — a disclosed token would authorize a sibling (SR-8)", len(o.dirs))
		}
		for p := range o.dirs {
			tokenFor[p] = tok
		}
	}
	inbox := hd.inbox
	hd.mu.Unlock()
	if tokenFor[14000] == "" || tokenFor[14001] == "" || tokenFor[14000] == tokenFor[14001] {
		t.Fatalf("expected a distinct token per port; got %v", tokenFor)
	}

	// (b) the victim acks P1 (consuming token_P1 and, in production, disclosing it on the shared bus); an
	// attacker who read token_P1 off the bus then forges a sibling ack for the un-acked P2 using it.
	ackP1, _ := json.Marshal(&proto.HomeAssignment{Directives: []proto.HomeDirective{{PublicPort: 14000, Epoch: 5}}})
	b.handleHomeAck(&nats.Msg{Subject: inbox + "." + tokenFor[14000], Data: ackP1})
	if b.homeAppliedEpoch(14000) != 5 {
		t.Fatalf("the legitimate P1 ack must converge (got %d)", b.homeAppliedEpoch(14000))
	}
	forge, _ := json.Marshal(&proto.HomeAssignment{Directives: []proto.HomeDirective{{PublicPort: 14001, Epoch: 5}}})
	b.handleHomeAck(&nats.Msg{Subject: inbox + "." + tokenFor[14000], Data: forge})
	if got := b.homeAppliedEpoch(14001); got != 0 {
		t.Fatalf("a token disclosed by port 14000's ack authorized an unacked sibling port 14001: epoch=%d "+
			"— cross-session ack forgery (SR-8) bypassing the drain/retire/upgrade rc gate", got)
	}
}

// TestHomeDeliveryConvergedIsSilent is the R7a registry contract: a converged pass
// must have ZERO side effects across consecutive ticks. Without it the pass would
// re-publish every 5s forever to every agent in the fleet.
func TestHomeDeliveryConvergedIsSilent(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	newHomeTestAgent(t, url, "lab", "lab-1", true)

	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 1)

	now := time.Now()
	if err := b.reconcileHomeDelivery(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.homeAppliedEpoch(14000) >= 1 }) {
		t.Fatal("initial delivery never acked")
	}
	pushesAfterConverge := b.homeDeliveryStats()

	// Ten more ticks over a converged fleet must publish NOTHING.
	for i := 1; i <= 10; i++ {
		if err := b.reconcileHomeDelivery(context.Background(), now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	pushes := b.homeDeliveryStats()
	if pushes != pushesAfterConverge {
		t.Fatalf("converged pass published %d extra assignment(s); a converged pass must be silent", pushes-pushesAfterConverge)
	}
}

// TestHomeDeliveryRetriesUntilApplied: an agent that receives but does NOT apply
// (its rehome is failing) must keep being re-delivered to. This is the difference
// between an applied-ack and a received-ack — with a received-ack the broker would
// go quiet here and report a convergence that does not exist.
func TestHomeDeliveryRetriesUntilApplied(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	agent := newHomeTestAgent(t, url, "lab", "lab-1", false) // receives, never acks

	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 1)

	now := time.Now()
	// Backoff doubles from the pass interval, so space the ticks generously.
	for i := 0; i < 4; i++ {
		if err := b.reconcileHomeDelivery(context.Background(), now.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	if n := agent.count(); n < 3 {
		t.Fatalf("an unapplied home was re-delivered only %d time(s); it must keep retrying", n)
	}
	if got := b.homeAppliedEpoch(14000); got != 0 {
		t.Fatalf("applied epoch = %d, want 0 — a RECEIVED push must never be mistaken for an APPLIED home", got)
	}

	// Now let it apply: convergence must close.
	agent.setAck(true)
	if err := b.reconcileHomeDelivery(context.Background(), now.Add(10*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.homeAppliedEpoch(14000) >= 1 }) {
		t.Fatal("delivery never converged after the agent started applying")
	}
}

// TestHomeDeliveryBackoffResetsOnANewDrain: a node that earned a long backoff by
// failing an OLD assignment must not make the operator wait it out when a NEW drain
// mints a new epoch. This is what keeps "bounded time" bounded by the pass interval
// rather than by the 5-minute backoff cap.
func TestHomeDeliveryBackoffResetsOnANewDrain(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	agent := newHomeTestAgent(t, url, "lab", "lab-1", false)

	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedClusterNode(t, b, "brk-b", "brk-b", "brk-b:7000", "fpB", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 1)

	now := time.Now()
	// Burn the backoff up on the OLD assignment.
	for i := 0; i < 6; i++ {
		if err := b.reconcileHomeDelivery(context.Background(), now.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	before := agent.count()
	// The probe ticks must land INSIDE the backoff window earned by the last burn
	// tick (which was at now+5h and set nextAt = +160s). Spacing them an hour apart
	// like the burn loop would put them past the 5-minute cap, and the "swallowed"
	// assertion below would be vacuous — it would pass whether or not a backoff exists.
	lastBurn := now.Add(5 * time.Hour)
	// A tick a second later must be swallowed by the backoff...
	if err := b.reconcileHomeDelivery(context.Background(), lastBurn.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if agent.count() != before {
		t.Fatal("the backoff is not being applied at all")
	}
	// ...but a NEW drain changes the EXPECTED state and must publish immediately.
	rehomeRow(t, b, 14000, "brk-b", 2)
	if err := b.reconcileHomeDelivery(context.Background(), lastBurn.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return agent.count() > before }) {
		t.Fatal("a NEW drain inherited the old backoff — the operator would wait out an unrelated penalty")
	}
	if addr, epoch, ok := agent.lastHomeFor(14000); !ok || addr != "brk-b:7000" || epoch != 2 {
		t.Fatalf("newest directive = (%s, %d), want (brk-b:7000, 2)", addr, epoch)
	}
}

// TestHomeDeliveryIsInertInSingleMode: single mode emits no home directives at all
// (selfID==""), so the pass must publish nothing — the register reply stays
// byte-identical to pre-D6, which is the standing D6/D9 inertness contract.
func TestHomeDeliveryIsInertInSingleMode(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	b.clusterMode = false
	b.selfID = ""
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 1)

	if err := b.reconcileHomeDelivery(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if pushes := b.homeDeliveryStats(); pushes != 0 {
		t.Fatalf("single mode published %d home assignment(s); it must be inert", pushes)
	}
}

// TestHomeDeliveryPrunesAppliedOnExposeRemoval keeps the leader-local cache bounded
// across expose churn (a long-lived leader must not accumulate one entry per port
// ever allocated).
func TestHomeDeliveryPrunesAppliedOnExposeRemoval(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	newHomeTestAgent(t, url, "lab", "lab-1", true)
	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 1)

	if err := b.reconcileHomeDelivery(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.homeAppliedEpoch(14000) >= 1 }) {
		t.Fatal("never acked")
	}
	if _, err := b.cfg.DB.Exec(`DELETE FROM port_allocations WHERE port=14000`); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcileHomeDelivery(context.Background(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := b.homeAppliedEpoch(14000); got != 0 {
		t.Fatalf("applied entry for a freed port survived pruning (epoch %d)", got)
	}
}

// TestHomeAckIsMonotone: a duplicated / re-ordered ack must never walk the applied
// epoch backwards and re-open a converged port (which would make the pass thrash).
func TestHomeAckIsMonotone(t *testing.T) {
	b := &Broker{clusterMode: true, selfID: "brk-a"}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	seedHomeApplied(b, 14000, 5)
	seedHomeApplied(b, 14000, 2) // stale/duplicate
	if got := b.homeAppliedEpoch(14000); got != 5 {
		t.Fatalf("applied epoch = %d after a stale ack, want 5 (monotone)", got)
	}
}

// TestHomeDeliveryUsesTheRegisterBuilder pins the "no second policy" property: the
// pushed assignment must be byte-identical to what the register reply would carry
// for the same agent. Two builders would inevitably drift, and a drift here means
// the push and the reconnect disagree about where an expose lives.
func TestHomeDeliveryUsesTheRegisterBuilder(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	agent := newHomeTestAgent(t, url, "lab", "lab-1", true)
	seedClusterNode(t, b, "brk-b", "brk-b", "brk-b:7000", "fpB", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-b", 3)

	want := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{})
	if want == nil {
		t.Fatal("fixture produced no register-side assignment")
	}
	if err := b.reconcileHomeDelivery(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return agent.count() > 0 }) {
		t.Fatal("nothing delivered")
	}
	agent.mu.Lock()
	got := agent.received[0]
	agent.mu.Unlock()
	gotJSON, _ := json.Marshal(&got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("pushed assignment differs from the register-reply assignment:\n push=%s\n reg =%s", gotJSON, wantJSON)
	}
}

// --- rc semantics --------------------------------------------------------------

// TestDrainRefusesRcZeroWhenDataPlaneStale is the drain verb's rc assertion, and the
// behavioural mutation guard for the ClusterAdmin wiring: with the convergence probe
// reporting a stale applied epoch, awaitHomeConvergence MUST return
// ErrDataPlaneNotConverged (which clusterCodeFor maps to a nonzero-classified code).
func TestDrainRefusesRcZeroWhenDataPlaneStale(t *testing.T) {
	migrated := []rehomedExpose{
		{sid: "lab", nid: "lab-1", name: "svc", port: 14000, epoch: 7, toBroker: "brk-b"},
		{sid: "lab", nid: "lab-1", name: "api", port: 14001, epoch: 7, toBroker: "brk-b"},
	}
	kicks := 0
	a := &ClusterAdmin{
		logger:        silentLogger(),
		now:           time.Now,
		homeAppliedFn: func(port int) int64 { return 6 }, // one epoch behind: NOT converged
		homeDeliverFn: func(string, string) { kicks++ },
	}
	err := a.awaitHomeConvergence("drain", "brk-a", migrated, time.Now().Add(-time.Second))
	var dp *ErrDataPlaneNotConverged
	if err == nil {
		t.Fatal("drain returned success while the data plane was still on the OLD home — this is the release blocker")
	}
	if !asErr(err, &dp) {
		t.Fatalf("want *ErrDataPlaneNotConverged, got %T: %v", err, err)
	}
	if len(dp.Pending) != 2 {
		t.Fatalf("pending = %d, want both exposes named", len(dp.Pending))
	}
	if kicks != 1 {
		t.Fatalf("delivery kicks = %d, want exactly 1 (one push per owning agent, not per expose)", kicks)
	}
	// The message must name the stranded ports — a bare timeout would leave the
	// operator with nothing to act on.
	if !strings.Contains(dp.Error(), "port 14000") || !strings.Contains(dp.Error(), "port 14001") {
		t.Fatalf("error does not enumerate the stranded exposes: %s", dp.Error())
	}
	// And it must classify as the retry-later wire code, not as an internal error.
	if code := clusterCodeFor(err); code != codeDataplaneNotConverged {
		t.Fatalf("clusterCodeFor = %q, want %q", code, codeDataplaneNotConverged)
	}
}

// TestDrainReturnsOKOnceTheDataPlaneConfirms is the other half: the gate must not be
// a permanent refusal. A converged data plane returns nil promptly.
func TestDrainReturnsOKOnceTheDataPlaneConfirms(t *testing.T) {
	migrated := []rehomedExpose{{sid: "lab", nid: "lab-1", name: "svc", port: 14000, epoch: 7, toBroker: "brk-b"}}
	a := &ClusterAdmin{
		logger:        silentLogger(),
		now:           time.Now,
		homeAppliedFn: func(int) int64 { return 7 },
		homeDeliverFn: func(string, string) {},
	}
	if err := a.awaitHomeConvergence("drain", "brk-a", migrated, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("converged data plane must return success, got %v", err)
	}
}

// TestRetireHoldsWhileTheDataPlaneIsStale is the THIRD verb's rc assertion.
//
// `cluster retire` is the RESUMABLE form of drain, so it does not block like drain
// does — it HOLDS in REHOME_EXPOSES: the op keeps its state, records a data-plane
// last_error, and the controller re-drives while the delivery pass keeps pushing.
// The rc consequence is that the op never reaches a terminal SUCCEEDED state on a
// stale data plane; opMaxAttempts eventually routes it to BLOCKED, which is nonzero
// for `cluster retire --wait`.
//
// What matters for rc semantics, and what this pins: the op must NOT advance out of
// REHOME_EXPOSES, and the recorded error must name the stranded exposes.
func TestRetireHoldsWhileTheDataPlaneIsStale(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, silentLogger())
	admin.homeAppliedFn = func(int) int64 { return 6 } // one epoch behind
	admin.homeDeliverFn = func(string, string) {}

	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	in := cluster.OpStartInput{
		OpID: "op-retire-stale", Kind: cluster.OpKindRetire, TargetNode: "brk-a",
		InitState: cluster.OpStateRehomeExposes, Confirmed: true,
		Timeline: `[{"s":"REHOME_EXPOSES"}]`,
	}
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, now)
	}); err != nil {
		t.Fatalf("seed retire op: %v", err)
	}
	op, err := cluster.OperationByID(n.RODB(), "op-retire-stale")
	if err != nil {
		t.Fatal(err)
	}

	migrated := []rehomedExpose{
		{sid: "lab", nid: "lab-1", name: "svc", port: 14000, epoch: 7, toBroker: "brk-b"},
	}
	pending := admin.pendingHomeConvergence(migrated)
	if len(pending) != 1 {
		t.Fatalf("a stale applied epoch must be reported pending; got %d", len(pending))
	}
	// This is the exact call driveRetire's REHOME_EXPOSES arm makes.
	admin.recordOpError(op, &ErrDataPlaneNotConverged{Verb: "retire", NodeID: "brk-a", Pending: pending})

	after, err := cluster.OperationByID(n.RODB(), "op-retire-stale")
	if err != nil {
		t.Fatal(err)
	}
	if after.OpState != cluster.OpStateRehomeExposes {
		t.Fatalf("retire advanced to %s on a stale data plane; it must HOLD in REHOME_EXPOSES "+
			"so it can never reach the irreversible RemoveServer while tunnels still point at the target", after.OpState)
	}
	if !strings.Contains(after.LastError, "port 14000") {
		t.Fatalf("the held op's last_error must name the stranded expose, got %q", after.LastError)
	}
	if !strings.Contains(after.LastError, "cluster retire") {
		t.Fatalf("the held op's last_error must identify the verb, got %q", after.LastError)
	}
	// And a converged data plane must clear the hold.
	admin.homeAppliedFn = func(int) int64 { return 7 }
	if p := admin.pendingHomeConvergence(migrated); len(p) != 0 {
		t.Fatalf("a converged data plane must release the retire hold; still pending %v", p)
	}
}

// TestRetireRehomeHoldIsBoundedToBlocked is the self-review high regression: the REHOME_EXPOSES hold must
// be BOUNDED. Before the fix it held via recordOpError forever (no opAttempts, no deadline), so a migrated
// agent that never reconnected fenced the whole membership plane indefinitely — and the code comment
// falsely claimed opMaxAttempts would route it to BLOCKED. This drives boundRehomeConvergence across a fake
// clock and proves: first hold STAMPS a replicated deadline, before it the op keeps holding, and PAST it the
// op routes to BLOCKED (a nonzero terminal `cluster retire --wait` sees, escapable via `cluster ops`).
func TestRetireRehomeHoldIsBoundedToBlocked(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, silentLogger())
	clk := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	admin.now = func() time.Time { return clk }

	in := cluster.OpStartInput{
		OpID: "op-retire-bound", Kind: cluster.OpKindRetire, TargetNode: "brk-a",
		InitState: cluster.OpStateRehomeExposes, Confirmed: true,
		Timeline: `[{"s":"REHOME_EXPOSES"}]`,
	}
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, clk) }); err != nil {
		t.Fatalf("seed op: %v", err)
	}
	reread := func() *cluster.Operation {
		op, err := cluster.OperationByID(n.RODB(), "op-retire-bound")
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	holdErr := &ErrDataPlaneNotConverged{Verb: "retire", NodeID: "brk-a"}

	// (1) First hold: no deadline yet → STAMP one (not blocked); op stays REHOME_EXPOSES WITH a deadline.
	if blocked, stamped := admin.boundRehomeConvergence(reread(), holdErr); blocked || !stamped {
		t.Fatalf("first hold must stamp a deadline (blocked=%v stamped=%v)", blocked, stamped)
	}
	if op := reread(); op.OpState != cluster.OpStateRehomeExposes || op.CatchupDeadline == 0 {
		t.Fatalf("after stamp: state=%s deadline=%d (want REHOME_EXPOSES with a nonzero deadline)", op.OpState, op.CatchupDeadline)
	}

	// (2) Before the deadline: neither blocked nor re-stamped → keep holding in REHOME_EXPOSES.
	if blocked, stamped := admin.boundRehomeConvergence(reread(), holdErr); blocked || stamped {
		t.Fatalf("before the deadline the op must keep holding (blocked=%v stamped=%v)", blocked, stamped)
	}
	if reread().OpState != cluster.OpStateRehomeExposes {
		t.Fatal("op left REHOME_EXPOSES before its convergence deadline")
	}

	// (3) Past the deadline: route to BLOCKED — the nonzero-terminal rc bound the verb promises.
	clk = clk.Add(opRehomeConvergeTimeout + time.Minute)
	if blocked, _ := admin.boundRehomeConvergence(reread(), holdErr); !blocked {
		t.Fatal("a retire whose data plane never converged must route to BLOCKED once the deadline passes")
	}
	if got := reread().OpState; got != cluster.OpStateBlocked {
		t.Fatalf("op state = %s, want BLOCKED (a permanently-unconverged retire must not fence membership forever)", got)
	}

	// (4) F2: `cluster ops confirm` of the BLOCKED retire must reset the convergence window — re-enter
	// DRAIN_REQUESTED with catchup_deadline=0 — so boundRehomeConvergence re-stamps a FRESH deadline instead
	// of re-BLOCKing on the old expired one (which would make confirm a no-op).
	if err := admin.ConfirmOp("op-retire-bound"); err != nil {
		t.Fatalf("confirm blocked retire: %v", err)
	}
	if op := reread(); op.OpState != cluster.OpStateDrainRequested {
		t.Fatalf("a confirmed retire must re-enter DRAIN_REQUESTED, got %s", op.OpState)
	} else if op.CatchupDeadline != 0 {
		t.Fatalf("confirm must RESET catchup_deadline to 0 for a fresh window; got %d — the retry would immediately re-BLOCK (F2)", op.CatchupDeadline)
	}
}

// TestRetireConsultsTheConvergenceGate is the anti-half-wiring pin for the verb
// above: the gate is only meaningful if driveRetire actually calls it BEFORE leaving
// REHOME_EXPOSES. Seeding that state through the real FSM needs raft-replicated
// port_allocations rows, for which no plan command exists — so the call site is
// pinned in source, the same idiom TestWireClusterLateWiresHomeConvergence uses.
func TestRetireConsultsTheConvergenceGate(t *testing.T) {
	src := readRepoFile(t, "cluster_operation_controller.go")
	for _, want := range []string{
		// CRIT-1: the gate must read the DURABLE oracle (re-derived from current state each
		// tick), NOT migrateExposes' self-destructing return, or the SECOND controller tick
		// sees an empty list and walks on to the irreversible RemoveServer.
		"a.pendingRetireConvergence(op.TargetNode,",
		`&ErrDataPlaneNotConverged{Verb: "retire"`,
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("driveRetire no longer contains %q — retire would walk on to the irreversible "+
				"RemoveServer with the data plane still pointing at the node being removed", want)
		}
	}
	// And it must NOT gate on the self-destructing migrateExposes return (the CRIT-1 bug shape).
	if strings.Contains(src, "a.pendingHomeConvergence(migrated)") {
		t.Fatal("driveRetire still gates on pendingHomeConvergence(migrated) — that list is empty on the " +
			"second tick (migrateExposes self-destructs), so the gate leaks straight to RemoveServer (CRIT-1)")
	}
	// F3/round-2: the retire gate must scope by the FIXED op.CreatedAt origin, NOT a sliding wall-clock
	// window — a sliding window drifts past the row's fixed last_rehome_at across a confirm-retry and
	// silently completes RemoveServer (fail-open).
	if !strings.Contains(src, "parseClusterTime(op.CreatedAt)") {
		t.Fatal("driveRetire no longer scopes the convergence gate by op.CreatedAt — a fixed origin is required " +
			"so a still-stranded row never ages out across a `cluster ops confirm` retry (round-2 fail-open)")
	}
	if strings.Contains(src, "pendingRetireConvergence(op.TargetNode, a.now().Add(") {
		t.Fatal("driveRetire scopes the convergence gate by a SLIDING wall-clock window — it drifts past the " +
			"row's fixed last_rehome_at on a confirm-retry and fails OPEN (round-2 self-review)")
	}
}

// TestDrainConsultsTheDurableConvergenceGate is the SYMMETRIC pin for the DRAIN half of CRIT-1 (self-review
// medium): the retire pin above covers only cluster_operation_controller.go, so a revert of JUST DrainNode's
// call site back to the self-destructing migrated list would reintroduce the drain re-run false-rc=0 with the
// whole suite green (the unit DrainNode tests bail before the gate; both paths short-circuit to nil). Pin the
// drain call site from source too.
func TestDrainConsultsTheDurableConvergenceGate(t *testing.T) {
	src := readRepoFile(t, "clusterdrain.go")
	if !strings.Contains(src, "a.pendingRetireConvergence(nodeID,") {
		t.Fatal("DrainNode no longer gates on the durable pendingRetireConvergence(nodeID) — on a drain re-run " +
			"migrateExposes returns empty and `cluster drain` would print a false rc=0 with the tunnel stranded (CRIT-1 drain half)")
	}
	if strings.Contains(src, `awaitHomeConvergence("drain", nodeID, migrated`) {
		t.Fatal("DrainNode still feeds the self-destructing `migrated` list to awaitHomeConvergence — on re-run " +
			"that list is empty and awaitHomeConvergence returns nil immediately (false rc=0, CRIT-1 drain half)")
	}
}

// TestPendingRetireConvergenceIsDurable is the CRIT-1 hermetic regression. The gate
// oracle must re-derive the un-confirmed set from CURRENT replicated state on EVERY
// call, so a retire re-driven on a later controller tick (or a drain the operator
// re-runs) — by which point migrateExposes has already re-pointed the rows and returns
// an empty list — still sees the stranded exposes instead of waving the op on to the
// irreversible RemoveServer. It mutation-checks the two failure shapes of the old
// migrateExposes-return gate: (a) a SECOND call must not self-destruct to empty, and
// (b) a converged agent must release the hold (not a permanent refusal).
func TestPendingRetireConvergenceIsDurable(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, silentLogger())
	db := n.DB()
	if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('lab','lab','SHA256:o','h')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES ('lab','lab-1',?,'ONLINE')`, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ins := func(port int, home string, epoch int64, lastRehome string) {
		if _, err := db.Exec(
			`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, last_rehome_at)
			 VALUES (?,?,?,?,?,?, 'ALLOCATED','SHA256:o',?,?,?,?)`,
			port, "lab", "lab-1", "svc", 9000+port, "th", time.Now().UTC(), home, epoch, lastRehome); err != nil {
			t.Fatalf("seed port %d: %v", port, err)
		}
	}
	ins(14000, "brk-b", 7, "") // MOVED off brk-a, agent has NOT confirmed → the stranded expose
	ins(14001, "brk-a", 7, "") // still homed ON brk-a (migrate will move it) → excluded from the gate

	applied := map[int]int64{14000: 6} // one epoch behind: the tunnel still points at brk-a
	admin.homeAppliedFn = func(p int) int64 { return applied[p] }

	pending, err := admin.pendingRetireConvergence("brk-a", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].port != 14000 {
		t.Fatalf("gate must report the moved-but-unconfirmed expose (and exclude the still-on-brk-a row); got %+v", pending)
	}
	// The CRIT-1 property: a SECOND call re-reads current state and STILL holds. The old
	// gate (pendingHomeConvergence over migrateExposes' return) would be empty here — the
	// rows already moved off brk-a — and the op would barrel on to RemoveServer.
	pending2, err := admin.pendingRetireConvergence("brk-a", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending2) != 1 {
		t.Fatalf("gate self-destructed on the second tick (CRIT-1): got %d pending, want 1", len(pending2))
	}
	// Mutation: once the agent confirms the new epoch, the gate releases.
	applied[14000] = 7
	pending3, err := admin.pendingRetireConvergence("brk-a", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending3) != 0 {
		t.Fatalf("a converged data plane must release the gate; still pending %+v", pending3)
	}
}

// TestPendingRetireConvergenceRecencyWindow is the F3 (external re-review) + round-2-self-review regression:
// the retire gate scopes by a FIXED origin (the op's created_at), so (1) an UNRELATED, OLD stranded expose
// (rehomed before this op started) is NOT reported — one dead tenant must not freeze EVERY retire; (2) THIS
// op's row IS reported; and (3) — the round-2 fail-open — because the origin is FIXED, the row never ages
// out no matter how far wall-clock advances (a `cluster ops confirm` retry that runs long past the first
// migrate), whereas the earlier SLIDING window (now−Δ) would drift past the row's fixed last_rehome_at and
// wrongly drop a still-stranded row, silently completing RemoveServer.
func TestPendingRetireConvergenceRecencyWindow(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, silentLogger())
	db := n.DB()
	if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('lab','lab','SHA256:o','h')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES ('lab','lab-1',?,'ONLINE')`, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	opStart := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC) // the retire op's created_at = the FIXED origin
	ins := func(port int, epoch int64, lastRehome time.Time) {
		if _, err := db.Exec(
			`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, last_rehome_at)
			 VALUES (?,?,?,?,?,?, 'ALLOCATED','SHA256:o',?,'brk-b',?,?)`,
			port, "lab", "lab-1", "svc", 9000+port, "th", opStart.UTC(), epoch, lastRehome.String()); err != nil {
			t.Fatalf("seed port %d: %v", port, err)
		}
	}
	ins(14000, 7, opStart.Add(30*time.Second)) // THIS op migrated it (AFTER op start) — unconfirmed
	ins(14001, 7, opStart.Add(-1*time.Hour))   // an UNRELATED prior op's stranded expose (before op start)
	admin.homeAppliedFn = func(int) int64 { return 6 }

	// (1)+(2) FIXED origin = op.CreatedAt: only this op's row is reported, the old stranded one dropped.
	scoped, err := admin.pendingRetireConvergence("brk-a", opStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped) != 1 || scoped[0].port != 14000 {
		t.Fatalf("fixed origin must report ONLY this op's row, not the pre-op stranded one; got %+v (F3)", scoped)
	}
	// (3) NO AGING: the SAME fixed origin still reports the row however far wall-clock has advanced (the
	// function compares last_rehome_at>=origin, no `now`). The earlier SLIDING window would fail-open here:
	// at a far-future now (a long confirm retry) it drifts PAST the row's fixed last_rehome_at and drops it.
	farNow := opStart.Add(60 * time.Minute)
	stillScoped, err := admin.pendingRetireConvergence("brk-a", opStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(stillScoped) != 1 || stillScoped[0].port != 14000 {
		t.Fatalf("the fixed origin must NEVER age out this op's still-stranded row; got %+v", stillScoped)
	}
	sliding := farNow.Add(-2 * opRehomeConvergeTimeout) // = opStart+40m > the row's last_rehome_at (opStart+30s)
	if aged, _ := admin.pendingRetireConvergence("brk-a", sliding); len(aged) != 0 {
		t.Fatalf("sanity: the abandoned sliding window should have aged the row out (proving why the caller uses "+
			"the FIXED op.CreatedAt); got %+v", aged)
	}
	// Drain-style zero window: fleet-wide, both unconfirmed exposes reported (fail-closed).
	fleet, err := admin.pendingRetireConvergence("brk-a", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(fleet) != 2 {
		t.Fatalf("the zero window must stay fleet-wide (fail-closed); got %d, want 2", len(fleet))
	}
}

// TestUpgradeHomesConvergedVerdict is the upgrade verb's rc oracle: a row whose agent
// has not confirmed the current epoch must be reported, and a fully-confirmed fleet
// must report clean. `cluster upgrade` turns a non-empty list into a nonzero exit.
func TestUpgradeHomesConvergedVerdict(t *testing.T) {
	url := startNATS(t)
	b, _ := homeDeliveryBroker(t, url, "brk-a")
	seedClusterNode(t, b, "brk-a", "brk-a", "brk-a:7000", "fpA", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "brk-a", 4)

	pending, err := b.homesUnconverged()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || !strings.Contains(pending[0], "port=14000") {
		t.Fatalf("an unconfirmed home must be reported; got %v", pending)
	}
	seedHomeApplied(b, 14000, 4)
	if pending, err = b.homesUnconverged(); err != nil || len(pending) != 0 {
		t.Fatalf("a confirmed fleet must report clean; got %v (err %v)", pending, err)
	}
}

// asErr is errors.As under a name that keeps the assertions above readable.
func asErr(err error, target **ErrDataPlaneNotConverged) bool {
	return errors.As(err, target)
}

// --- anti-half-wiring ----------------------------------------------------------

// TestHomeDeliveryIsRegisteredInTheProductionPassSet is one half of the anti-
// half-wiring guard: this project has twice shipped code that compiled, tested and
// linted clean while being functionally DEAD (R5's `_b_migrate_forensics`, R7b's
// never-started lease keeper). A delivery channel nobody schedules is exactly that
// failure mode, so the registration itself is asserted, not assumed.
func TestHomeDeliveryIsRegisteredInTheProductionPassSet(t *testing.T) {
	b := &Broker{}
	b.cfg = Config{
		Logger: silentLogger(), ReconcileInterval: time.Second, ProcGCInterval: time.Minute,
		XferReapInterval: time.Minute, GrowLockReapInterval: time.Minute,
		UpgradeLockReapInterval: time.Minute, HomeDeliverInterval: 5 * time.Second,
		DrainMarkerReapInterval: time.Minute,
	}
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	b.registerCoreReconcilePasses()
	for _, s := range b.reconcilers.status() {
		if s.Name == "home-delivery" {
			if s.Authority != authorityLeader.String() {
				t.Fatalf("home-delivery must be leader-gated (N brokers pushing the same assignment is pure "+
					"amplification); authority = %q", s.Authority)
			}
			return
		}
	}
	t.Fatal("the home-delivery pass is NOT registered — the active delivery channel would be dead code")
}

// TestWireClusterLateWiresHomeConvergence is the other half: the probes DrainNode
// needs are injected at wire time, and a nil probe silently degrades the drain back
// to "rc=0 on an unconverged data plane". There is no way to run wireClusterLate
// hermetically here (it needs raft), so this pins the two assignments in the wiring
// source. Crude, but it is the ONLY thing standing between this batch and the exact
// half-wiring the R7b review documented twice.
func TestWireClusterLateWiresHomeConvergence(t *testing.T) {
	src := readRepoFile(t, "clusterwrite.go")
	for _, want := range []string{
		"admin.homeAppliedFn = b.homeAppliedEpoch",
		"admin.homeDeliverFn = b.deliverHomeNow",
		"b.subscribeHomeAcks(nc)",
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("wireClusterLate no longer contains %q — the drain's data-plane gate is unwired and every rc-semantics test above becomes vacuous", want)
		}
	}
}

// TestHomeVerbIsWireStable pins the forwarded-command verb against the agent's
// dispatch switch. The two sides are independent string literals (the subject
// builder takes a free-form verb), so a rename on one side would turn the whole
// channel into "unknown forwarded verb" warnings with no build/test/lint failure.
func TestHomeVerbIsWireStable(t *testing.T) {
	if homeDeliveryVerb != "home" {
		t.Fatalf("homeDeliveryVerb = %q; the agent's dispatchForwarded case is `home`", homeDeliveryVerb)
	}
	if got := proto.SubjCmdForwarded("lab", "lab-1", homeDeliveryVerb); !strings.HasSuffix(got, ".home.req.forwarded") {
		t.Fatalf("delivery subject = %q, want a .home.req.forwarded leaf", got)
	}
}

// readRepoFile reads a source file from this package's directory. `go test` runs
// with the package directory as the working directory, so a bare name resolves.
func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}
