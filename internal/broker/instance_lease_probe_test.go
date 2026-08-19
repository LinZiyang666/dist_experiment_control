package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

func leaseBrokerWithBus(t *testing.T) (*Broker, *nats.Conn, string) {
	t.Helper()
	url := testharness.StartNATS(t)
	b := leaseBroker(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("broker conn: %v", err)
	}
	t.Cleanup(nc.Close)
	b.nc.Store(nc)
	return b, nc, url
}

// DEFECT 1 — the grant→subscribe window is unprotected.
//
// handleRegister replies to instance A and A only installs its forwarded
// subscription AFTER it has processed that reply. registerNode has already
// stamped last_heartbeat_at, so a second clone registering in that window
// enters the ambiguity branch, the probe finds NO interest (A has not
// subscribed yet), and the adjudicator GRANTS THE SAME NAME AGAIN.
func TestAdjudicateLeaseGrantsTheSameNameTwiceInsideTheSubscribeWindow(t *testing.T) {
	b, _, _ := leaseBrokerWithBus(t)

	// Instance A registers: no row yet, so it is granted outright.
	if lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA}); lease != nil || code != "" || err != nil {
		t.Fatalf("A: lease=%v code=%q err=%v", lease, code, err)
	}
	// registerNode's upsert stamps last_heartbeat_at = now. A has NOT yet
	// subscribed — session() subscribes only after the reply comes back.
	seedBeat(t, b, "gpu1", 0)

	// Instance B (the clone) registers milliseconds later.
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" {
		t.Fatalf("B: code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("BOTH instances were granted \"gpu1\": the interest probe cannot see an incumbent " +
			"that has not reached the subscribe step yet, and the in-memory holder — which DOES " +
			"know a different instance was granted microseconds ago — is only consulted for an " +
			"EQUAL id. Both clones now subscribe to one forwarded subject: the fan-out is back.")
	}
}

// DEFECT 2 — an agent's own replayed subscription contests its own re-register.
//
// nats.go resends every subscription BEFORE it invokes ReconnectHandler, and
// ReconnectHandler → onNATSReconnect re-registers on that same connection. So
// at re-register time the agent's own `cmd.node.<nid>.*.req.forwarded` interest
// is live. With the holder map cold (broker restart / leader election — the
// exact states adjudicateLease's own comment says it must survive) the probe
// answers "in use" and the LONE agent is handed a suffix.
func TestAdjudicateLeaseDoesNotContestAnAgentAgainstItsOwnSubscription(t *testing.T) {
	b, _, url := leaseBrokerWithBus(t)

	// The agent: connected, subscribed on the forwarded subject, beating — and
	// ANSWERING claim-probe with its own instance id, which is what a current
	// agent does (internal/agent, replyClaimProbe). That answer is the whole
	// fix for this defect: the probe can now tell "someone else holds this
	// name" from "the thing answering is the very process re-registering".
	agentConn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("agent conn: %v", err)
	}
	defer agentConn.Close()
	if _, err := agentConn.Subscribe(proto.SubjCmdForwarded("lab", "gpu1", "*"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		body, _ := json.Marshal(proto.ClaimProbeResp{InstanceID: testInstanceA})
		_ = agentConn.Publish(m.Reply, body)
	}); err != nil {
		t.Fatalf("agent subscribe: %v", err)
	}
	if err := agentConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	seedBeat(t, b, "gpu1", 0)

	// Cold holder map (post-restart / post-election), same instance id: this
	// is onNATSReconnect's re-register.
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA})
	if err != nil || code != "" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if lease != nil {
		t.Fatalf("a lone agent re-registering after a reconnect was suffixed to %q: the probe "+
			"found the agent's OWN replayed subscription. onNATSReconnect then IGNORES resp.Lease, "+
			"so it keeps the basename while the broker believes it handed out a new one, and the "+
			"contested reply's EMPTY AcceptedProcesses makes procCourier.onRegisterSuccess drop "+
			"every pending proc.exit event.",
			lease.AssignedNID)
	}
}

// origin: internal review — the reviewer found (correctly, at the time) that
// NOTHING answered claim-probe, so the "a live incumbent answers in ~1ms"
// rationale was false and every contested probe burned the full budget. That is
// now fixed: the agent has a claim-probe arm (internal/agent, replyClaimProbe).
//
// This test keeps the reviewer's scenario as the N-1 case it still is — a
// subscriber that does NOT implement the verb, i.e. any pre-feature agent. The
// contract for that case is unchanged and deliberate: pay the budget, then take
// the conservative branch. Suffixing a live device is recoverable on its next
// restart; handing two live processes one name is not.
func TestSilentSubscriberCostsTheFullProbeBudgetAndIsTreatedAsHeld(t *testing.T) {
	b, _, url := leaseBrokerWithBus(t)
	agentConn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("agent conn: %v", err)
	}
	defer agentConn.Close()
	// Exactly dispatchForwarded's default arm: log, return, never reply.
	if _, err := agentConn.Subscribe(proto.SubjCmdForwarded("lab", "gpu1", "*"), func(*nats.Msg) {}); err != nil {
		t.Fatalf("agent subscribe: %v", err)
	}
	if err := agentConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	start := time.Now()
	held, known := probeNameHeldByOther(b, "lab", "gpu1", "aaaaaaaaaaaaaaaaaaaaaaaaaa")
	if !held || known {
		t.Fatalf("a subscriber that does not answer must read as held=true, known=false — an "+
			"UNKNOWN answer, so the caller can weigh it against the heartbeat instead of renaming "+
			"a device that is merely far away; got held=%v known=%v", held, known)
	}
	if d := time.Since(start); d < claimProbeBudget {
		t.Fatalf("probe returned in %v, so something answered — this case is supposed to be the "+
			"pre-feature agent that never replies", d)
	} else {
		t.Logf("probe took %v (full %v budget), as expected for a subscriber with no claim-probe arm",
			d, claimProbeBudget)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2
//
// COST OF THE AMBIGUOUS WINDOW — the invariant, after the fix.
//
// nats.go delivers ONE subscription's messages serially on a single goroutine,
// so every millisecond handleRegister spends is a millisecond no OTHER node's
// register is handled. The reviewer's original finding stands as the reason
// this test exists: against a subscriber with no claim-probe arm (any
// pre-feature agent) a blocking probe cost the FULL budget, and a fleet
// re-registering after a broker restart was serialized at that rate.
//
// The fix is not a smaller number — it is moving the probe OFF the handler. So
// what this pins now is the property, not the budget: ONE adjudication call
// must return promptly no matter how silent the subscriber is. It returns the
// transient leaseReasonProbePending; the agent retries and lands on the cached
// verdict.
//
// MUTATION: make leaseProbe call probeNameHeldByOtherWithin inline instead of
// spawning it, and this goes red.
func TestContestedProbeCostsTheFullBudgetAgainstASilentSubscriber(t *testing.T) {
	url := testharness.StartNATS(t)
	anc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("agent connect: %v", err)
	}
	t.Cleanup(anc.Close)
	// A real agent's forwarded subscription: interest exists, but no verb
	// handler replies to claim-probe.
	sub, err := anc.Subscribe(proto.SubjCmdForwarded("lab", "gpu1", "*"), func(*nats.Msg) {})
	if err != nil {
		t.Fatalf("agent subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := anc.Flush(); err != nil {
		t.Fatal(err)
	}

	b := leaseBroker(t)
	bnc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("broker connect: %v", err)
	}
	t.Cleanup(bnc.Close)
	b.nc.Store(bnc)
	seedBeat(t, b, "gpu1", 0)

	// ONE call — the handler's call. Not the retry loop: the point is precisely
	// that the handler returns without waiting for the probe.
	start := time.Now()
	lease, code, _ := adjudicateLease(b, "lab", "gpu1",
		&proto.NodeRegisterReq{InstanceID: testInstanceB}, time.Now().UTC())
	d := time.Since(start)
	if d >= claimProbeBudget {
		t.Errorf("one contested register blocked the register handler for %v; registers are "+
			"delivered serially on one goroutine, so a fleet re-registering after a broker "+
			"restart is serialized at this rate. The probe must run OFF the handler.", d)
	}
	if code != leaseReasonProbePending || lease != nil {
		t.Fatalf("a register that needs a probe must be answered with the transient %q and no "+
			"assignment, so the agent retries onto the cached verdict; got lease=%v code=%q",
			leaseReasonProbePending, lease, code)
	}
	// And the deferred verdict must still arrive: a silent subscriber reads as
	// held/unknown, which with a fresh beat keeps the conservative branch.
	if l, c, err := adjudicated(t, b, "lab", "gpu1",
		&proto.NodeRegisterReq{InstanceID: testInstanceB}); err != nil || c != "" || l == nil {
		t.Fatalf("the retry must reach a real decision; got lease=%v code=%q err=%v", l, c, err)
	}
}
