package p2_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §3.1
//
// THE CANONICAL CLONE ARRIVAL IS SIMULTANEOUS, NOT SEQUENCED.
//
// TestTwoInstancesOfOneImageBecomeTwoDevices waits for the first instance's
// `nodes` row before it starts the second, which gives instance A time to
// process its register reply and install its forwarded subscription. Every
// real arrival — one Deployment scaled to 2, a `kubectl rollout restart`, a
// node drain returning both pods, both clones reconnecting after a broker
// outage — starts them together.
//
// In that window the interest probe is blind: A has been GRANTED the name but
// has not subscribed yet, so the probe gets ErrNoResponders and B is granted
// the same name. The leader-local holder map does know a different instance was
// granted microseconds ago; it is only ever consulted for an EQUAL id.
//
// TIMING NOTE, so a green run is never mistaken for a fix: this is a window,
// not a constant. It is RED 5/5 at normal speed and GREEN under -race, because
// -race slows B's whole connect+register more than it slows A's two remaining
// steps after the reply. The deterministic form of the same defect is
// TestAdjudicateLeaseGrantsTheSameNameTwiceInsideTheSubscribeWindow in
// internal/broker, which drives the adjudicator directly. Fix the window, not
// this test's sleep.
func TestSimultaneouslyLaunchedClonesBecomeTwoDevices(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	defer cloneStartBroker(t, url, db)()

	// No waitForNodeCount between them: this is the arrival shape the fix is for.
	stopA := cloneStartAgent(t, url, "lab", "jupyter")
	defer stopA()
	stopB := cloneStartAgent(t, url, "lab", "jupyter")
	defer stopB()

	got := waitForNodeCount(t, db, "lab", 2, 6*time.Second)
	if len(got) != 2 || got[0] != "jupyter" || got[1] != "jupyter-02" {
		t.Fatalf("two clones launched together produced %v; both processes are subscribed to one "+
			"forwarded subject, so every command executes twice — the pre-fix production state", got)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.3
//
// A REFUSAL THE AGENT DOES NOT HONOUR IS WORSE THAN NO REFUSAL.
//
// adjudicateLease now answers "contested, but no name could be issued" (an
// over-long basename, or an exhausted suffix space) with a lease whose
// AssignedNID is EMPTY, and handleRegister replies with it and returns having
// touched nothing — deliberately, because it has already PROVEN a different
// live process holds the name.
//
// The agent's two register sites then disagree. onNATSReconnect returns on any
// non-nil lease. session() only returns for an ACCEPTABLE one, and an empty
// AssignedNID is not acceptable — so it logs a warning and carries on into
// onRegisterSuccess (which drops every pending proc exit, because a contested
// reply carries no AcceptedProcesses), into the forwarded subscription on the
// name the broker just refused it, and into the heartbeat loop that keeps the
// incumbent's row looking fresh.
//
// This test observes the subscription itself: a claim probe on the refused
// name is answered only by a process that reached the subscribe step.
func TestAnUnissuableLeaseRefusalIsHonouredOnTheSessionRegisterPath(t *testing.T) {
	for _, tc := range []struct {
		name     string
		leaseRaw string
		wantSub  bool
	}{
		// The control: a lease the agent CAN adopt. It rebuilds under the
		// assigned name and never subscribes on the basename.
		{"assignable lease is adopted", `{"ok":true,"lease":{"assigned_nid":"gpu1-02","basename":"gpu1"}}`, false},
		// The refusal: contested, no name issuable. The broker wrote nothing.
		{"unissuable lease is a refusal", `{"ok":true,"lease":{"basename":"gpu1"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := testharness.StartNATS(t)
			obs, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close()

			var registers atomic.Int64
			sub, err := obs.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
				var body struct {
					RosterRefreshOnly bool `json:"roster_refresh_only"`
				}
				_ = json.Unmarshal(m.Data, &body)
				if body.RosterRefreshOnly {
					_ = m.Respond([]byte(`{"ok":true}`))
					return
				}
				registers.Add(1)
				_ = m.Respond([]byte(tc.leaseRaw))
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			// The assigned name registers normally, so the control agent can settle.
			sub2, err := obs.Subscribe(proto.SubjNodeRegister("lab", "gpu1-02"), func(m *nats.Msg) {
				_ = m.Respond([]byte(`{"ok":true}`))
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub2.Unsubscribe() }()
			_ = obs.Flush()

			a, err := agent.New(agent.Config{
				NATSURL:           url,
				SID:               "lab",
				NID:               "gpu1",
				Logger:            testharness.SilentLog(),
				HeartbeatInterval: 100 * time.Millisecond,
				RegisterTimeout:   2 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("agent did not exit")
				}
			}()

			// Wait until the agent has processed at least one register.
			deadline := time.Now().Add(3 * time.Second)
			for registers.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			time.Sleep(400 * time.Millisecond) // let the subscribe step, if any, land

			// A claim probe on the BASENAME's forwarded subject is answered only
			// by a process that installed a subscription there.
			_, perr := obs.Request(proto.SubjCmdForwarded("lab", "gpu1", proto.ClaimProbeVerb),
				nil, 500*time.Millisecond)
			subscribed := perr == nil
			if subscribed != tc.wantSub {
				t.Fatalf("subscribed on the basename's forwarded subject = %v, want %v "+
					"(probe err=%v). A register the broker REFUSED must never reach the subscribe "+
					"step: the whole protection is that a refused instance never installs a "+
					"second subscription on the incumbent's forwarded subject.",
					subscribed, tc.wantSub, perr)
			}
		})
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2
//
// THE PROBE IS A BLOCKING REQUEST INSIDE THE FLEET'S ONE REGISTER SUBSCRIPTION.
//
// The broker subscribes ONE async handler to
// `…ctrl.s.*.node.*.register.req` — every session, every node — and nats.go
// delivers a subscription's messages serially on that subscription's single
// goroutine. The claim probe runs inside that handler, so the time it takes is
// time during which NO other node in the deployment can register.
//
// The design argues the cost away with "a live incumbent answers in ~1ms
// locally, so the full budget is only spent in a partition". Two live cases
// spend it anyway: any agent that predates the claim-probe verb (its
// dispatchForwarded default arm logs and returns without replying), and any
// agent whose answer is slower than the fixed 200ms budget.
//
// This measures an UNRELATED node's register latency behind one such probe.
func TestOneContestedProbeDoesNotStallEveryOtherNodesRegister(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	defer cloneStartBroker(t, url, db)()

	// A live-but-silent incumbent: interest exists on the forwarded subject
	// (so the probe is not answered with ErrNoResponders) and nothing replies.
	// That is exactly a pre-feature agent.
	anc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer anc.Close()
	sub, err := anc.Subscribe(proto.SubjCmdForwarded("lab", "silent", "*"), func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if _, err := db.Exec(
		`INSERT INTO nodes(sid,nid,status,last_heartbeat_at) VALUES('lab','silent','ONLINE',?)`,
		time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_ = anc.Flush()

	body := func(nid, iid string) []byte {
		b, _ := json.Marshal(proto.NodeRegisterReq{
			ProtoVersion: proto.ProtoVersion, NID: nid, InstanceID: iid,
		})
		return b
	}
	cnc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer cnc.Close()

	// Fire the contested register, then an unrelated node's register right
	// behind it, and time the SECOND one.
	go func() {
		_, _ = cnc.Request(proto.SubjNodeRegister("lab", "silent"),
			body("silent", "bbbbbbbbbbbbbbbbbbbbbbbbbb"), 3*time.Second)
	}()
	time.Sleep(20 * time.Millisecond) // let the contested one reach the handler
	start := time.Now()
	if _, err := cnc.Request(proto.SubjNodeRegister("lab", "bystander"),
		body("bystander", "cccccccccccccccccccccccccc"), 5*time.Second); err != nil {
		t.Fatalf("bystander register: %v", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("an unrelated node's register waited %v behind ONE contested probe. Registers "+
			"for the whole deployment are delivered on a single subscription goroutine, so a herd "+
			"of clones (or a fleet re-registering after a broker restart) serialises at this rate "+
			"against the agent's own RegisterTimeout.", d)
	}
}
