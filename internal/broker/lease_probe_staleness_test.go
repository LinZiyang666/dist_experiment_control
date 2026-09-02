package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Staleness of the cached probe observation.

func subscribeClaimProbeAs(t *testing.T, url, sid, nid, instanceID string) *nats.Conn {
	t.Helper()
	c, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("agent conn: %v", err)
	}
	t.Cleanup(c.Close)
	if _, err := c.Subscribe(proto.SubjCmdForwarded(sid, nid, "*"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		body, _ := json.Marshal(proto.ClaimProbeResp{InstanceID: instanceID})
		_ = c.Publish(m.Reply, body)
	}); err != nil {
		t.Fatalf("agent subscribe: %v", err)
	}
	if err := c.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return c
}

// A GRANT INVALIDATES THE OBSERVATION THAT LICENSED IT, AND NOTHING SAYS SO.
//
// The "nobody is subscribed" observation is objective and correctly cached —
// but it is true only until the register it just authorised installs its own
// subscription, which happens milliseconds later. probeTTL keeps it for 10s;
// leaseGrantWindow (= leaseSubscribeSettle, 5s since review round 2 widened it
// from 1s) covers only the first part of that, so for the remainder of probeTTL
// every clone that presents the same name reads "free" out of the cache and is
// handed the bare name while the incumbent it would collide with is live and
// answering. That remainder — [leaseGrantWindow, probeTTL) — is B5 in
// docs/reviews/cloned-credential-instances-review-round2.md and is NOT what
// this test exercises (see the 1.1s comment below).
func TestGrantInvalidatesTheFreeObservationThatAuthorisedIt(t *testing.T) {
	b, _, url := leaseBrokerWithBus(t)

	// The predecessor died; its row is still warm and nobody is subscribed.
	// This is the ordinary restart shape.
	seedBeat(t, b, "gpu1", 0)

	// A2 (the restart) registers: the probe finds nobody, A2 keeps the bare
	// name, and {nobody subscribed} lands in probeCache.
	if lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA}); lease != nil || code != "" || err != nil {
		t.Fatalf("A2 restart: lease=%v code=%q err=%v", lease, code, err)
	}
	// A2 now does what every agent does next: installs its forwarded
	// subscription and starts answering claim-probe.
	subscribeClaimProbeAs(t, url, "lab", "gpu1", testInstanceA)

	// A clone starts 1.1s later — INSIDE leaseGrantWindow (5s), with the cached
	// {nobody subscribed} observation still fresh. This line used to say "past
	// leaseGrantWindow": it was written when the window was 1s and was not
	// updated when round 2 widened the window, so for six weeks the comment
	// described a branch the test had stopped taking. What refuses the bare
	// name at 1.1s is the grant window, not the probe; this test pins the
	// IN-window branch. origin: docs/reviews/test-system-overhaul-plan.md B0.
	later := time.Now().UTC().Add(1100 * time.Millisecond)
	lease, code, err := adjudicateLease(b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceB}, later)
	if code == leaseReasonProbePending {
		t.Fatalf("unexpected transient")
	}
	if err != nil || code != "" {
		t.Fatalf("B: code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("clone B was granted the bare name \"gpu1\" while A2 is live and answering " +
			"claim-probe: probeCache still holds the \"nobody is subscribed\" observation from " +
			"A2's own grant. Both processes now serve one forwarded subject — every exec runs twice.")
	}
}

// A BACKGROUND PROBE'S ANSWER IS WRITTEN UNCONDITIONALLY, so an observation
// whose evidence predates a NEWER, definitive one still overwrites it and then
// rules the cache for a further probeTTL. Here the stale write erases the fact
// that the incumbent answered, and the silence rule then reads a live agent as
// dead.
//
// WHAT THIS TEST PINS TODAY, MEASURED 2026-09-01 (test-system overhaul, internal review L6-F1 /
// L5-F11). The sentence above describes the defect this test was written against. The product's
// answer to it was TWO changes: (1) the background probe RE-CHECKS silence before recording it
// (broker.go, "RE-CHECK SILENCE BEFORE RECORDING IT"); (2) it refuses to clobber a cache entry
// younger than its own start ("DO NOT CLOBBER A NEWER OBSERVATION"). This test proves (1): the
// recheck sees A2 and the cache says "held by A2". It does NOT prove (2): with the younger-
// observation guard disabled (mutated to never fire), this test, the lease model's six scenarios
// and its 40×12 random walk all stay green — because under single-flight probeInFlight the only
// writer of the cache is the background probe itself, an inline definitive answer is deliberately
// NOT cached, and a second background probe cannot start until the first has stored. There is no
// path today on which a younger entry exists when the guard runs; it is defensive, and its G1
// mutation (plan §4 B3 ②) is recorded as NOT caught. If that ever changes — a second cache writer
// — this paragraph is the first thing to delete, and the mutation is the test to write.
func TestBackgroundProbeMustNotOverwriteANewerObservation(t *testing.T) {
	b, _, url := leaseBrokerWithBus(t)
	seedBeat(t, b, "gpu1", 0)

	// A silent subscriber: a dying process whose subscription the server has
	// not reaped. It never answers, so the background probe will time out.
	silent, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("silent conn: %v", err)
	}
	defer silent.Close()
	if _, err := silent.Subscribe(proto.SubjCmdForwarded("lab", "gpu1", "*"), func(*nats.Msg) {}); err != nil {
		t.Fatalf("silent subscribe: %v", err)
	}
	if err := silent.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	start := time.Now()
	// A register hits the ambiguous shape and launches the 3s background probe.
	if _, ready := leaseProbe(b, "lab", "gpu1", time.Now().UTC()); ready {
		t.Fatalf("expected the ambiguous shape to defer")
	}

	// The real incumbent finishes starting up and installs its subscription.
	subscribeClaimProbeAs(t, url, "lab", "gpu1", testInstanceA)

	// It registers. The inline probe is ANSWERED — a definitive observation,
	// strictly newer than the one the background probe is still waiting on.
	if lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA}); lease != nil || code != "" || err != nil {
		t.Fatalf("A: lease=%v code=%q err=%v", lease, code, err)
	}
	grantedAt := time.Now()

	// The background probe times out and clobbers the cache with UNKNOWN.
	time.Sleep(backgroundProbeBudget - time.Since(start) + 300*time.Millisecond)
	raw, ok := b.probeCache.Load(leaseKey("lab", "gpu1"))
	if !ok {
		t.Fatal("cache entry vanished")
	}
	v0, _ := raw.(probeVerdict)
	t.Logf("cache after the background write: answered=%v responder=%q definitive=%v age=%v",
		v0.answer.answered, v0.answer.responder, v0.answer.definitive, time.Since(v0.at))

	// A clone arrives past leaseSubscribeSettle, with the incumbent beating and
	// answering. A FRESH probe would say "held by A"; the cache says nothing.
	later := grantedAt.UTC().Add(6 * time.Second)
	if _, err := b.cfg.DB.Exec(`UPDATE nodes SET last_heartbeat_at = ? WHERE sid='lab' AND nid='gpu1'`,
		later.Add(-time.Second)); err != nil {
		t.Fatalf("beat: %v", err)
	}
	lease, code, err := adjudicateLease(b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceC}, later)
	if code == leaseReasonProbePending || err != nil || code != "" {
		t.Fatalf("C: code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("clone C was granted the bare name: a background probe whose evidence predates " +
			"the incumbent's subscription overwrote the newer observation that the incumbent HAD " +
			"answered, and the silence rule then read a live, answering agent as dead.")
	}
}

// THE OTHER DIRECTION OF THE SAME STALENESS: a "held by A" observation outlives
// A. Nothing tells the broker a process died, so the cache keeps asserting an
// incumbent that is gone and the restarting agent is renamed — the STALE-row
// outcome §3d of the review calls the worst one.
func TestHeldObservationMustNotOutliveTheHolder(t *testing.T) {
	b, _, url := leaseBrokerWithBus(t)
	seedBeat(t, b, "gpu1", 0)

	// The incumbent A, subscribed and answering.
	a := subscribeClaimProbeAs(t, url, "lab", "gpu1", testInstanceA)

	// A clone probes and is correctly suffixed. {responder: A} is now cached.
	if lease, _, _ := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceC}); lease == nil {
		t.Fatal("the clone should have been suffixed")
	}

	// A is hard-killed: no farewell, and its subscription goes with the socket.
	a.Close()
	time.Sleep(150 * time.Millisecond)

	// A2 restarts under the same bare name a second later. A fresh probe would
	// answer ErrNoResponders — nobody is there — and A2 would keep gpu1.
	later := time.Now().UTC().Add(1500 * time.Millisecond)
	lease, code, err := adjudicateLease(b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceB}, later)
	if code == leaseReasonProbePending || err != nil || code != "" {
		t.Fatalf("A2: code=%q err=%v", code, err)
	}
	if lease != nil {
		t.Fatalf("the restarting agent was renamed to %q on the strength of a cached observation "+
			"of a process that is already dead; the bare name the operator addresses now goes STALE",
			lease.AssignedNID)
	}
}
