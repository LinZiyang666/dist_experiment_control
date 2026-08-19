package agent

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2
//
// The adoption site in Agent.session takes lease.AssignedNID on trust. Every
// other place this process accepts a routing name from outside re-validates it
// (agent.New's TETHER_ROUTING_NID restore calls proto.ValidateNID before
// adopting), and the plan requires a lease name to satisfy ValidateNID because
// ParseCmdBy / ParseSidNidFromCtrl / ParseEvProc all re-apply it. An
// unvalidated name is adopted into the CONNECT name, so the next connect is a
// hard auth denial — which connectNATS treats as FATAL for an initial connect.
func TestAdoptedLeaseNameMustBeAValidNID(t *testing.T) {
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "gpu1",
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: time.Second,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	const bogus = "NOT A VALID NID"
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{AssignedNID: bogus, Basename: "gpu1"},
		})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rebuild, serr := a.session(ctx)
	if serr != nil {
		t.Fatalf("session: %v", serr)
	}
	if got := nidOf(a); got == bogus {
		t.Fatalf("the agent adopted %q as its routing name: it now presents that string as the "+
			"nid in its CONNECT name and in every subject it builds. proto.ValidateNID(%q) = %v; "+
			"an auth_callout broker denies it and connectNATS treats an initial auth failure as fatal "+
			"(rebuild=%v)", got, bogus, proto.ValidateNID(bogus), rebuild)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.1
//
// Adoption is unbounded: session() returns rebuild=true for ANY assigned name
// that differs from the current one, and Run's loop reconnects immediately with
// no backoff and no cap on how many times a name may be reassigned. A broker
// (or anything that can win the race to a register reply inbox) that keeps
// handing out fresh names turns the agent into a full-rate connect/register
// storm against the bus.
func TestRepeatedLeaseAssignmentsDoNotSpinTheSessionLoop(t *testing.T) {
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "gpu1",
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: time.Second,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var registers atomic.Int64
	// Answer EVERY node register in the session with a different name.
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "*"), func(m *nats.Msg) {
		n := registers.Add(1)
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{AssignedNID: proto.LeaseNameFor("gpu1", int(2+n%90)), Basename: "gpu1"},
		})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(time.Second)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not exit")
	}
	if n := registers.Load(); n > 20 {
		t.Fatalf("the agent re-connected and re-registered %d times in one second under a broker "+
			"that keeps reassigning names: adoption has no cap and the rebuild loop has no backoff", n)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.1
//
// Lease adoption returns rebuild=true, so Run WILL start a successor session —
// but it never sets a.rebuildRequested, which is the only input session()'s
// teardown defer uses to pick the escalation intent. The ladder therefore
// escalates as a SHUTDOWN (os.Exit) rather than a REBUILD (self-exec in place),
// which on the unsupervised setsid deployment leaves the node down for good.
// Every other rebuild path (roster.go's two) sets the flag before calling
// fin.Do(teardownRebuild).
func TestLeaseAdoptionTearsDownWithTheRebuildIntent(t *testing.T) {
	url := testharness.StartNATS(t)
	oldClose, oldGrace := closeBudget, poisonGrace
	closeBudget, poisonGrace = 100*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { closeBudget, poisonGrace = oldClose, oldGrace })

	release := make(chan struct{})
	var escalated atomic.Value
	a, err := New(Config{
		NATSURL:            url,
		SID:                "lab",
		NID:                "gpu1",
		Logger:             testharness.SilentLog(),
		HeartbeatInterval:  time.Second,
		RegisterTimeout:    2 * time.Second,
		TeardownCloseFn:    func(*nats.Conn) { <-release },
		TeardownEscalateFn: func(intent string) { escalated.Store(intent) },
	})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{AssignedNID: "gpu1-02", Basename: "gpu1"},
		})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rebuild, serr := a.session(ctx)
	close(release)
	if serr != nil || !rebuild {
		t.Fatalf("expected a lease-driven rebuild; got rebuild=%v err=%v", rebuild, serr)
	}
	if got, _ := escalated.Load().(string); got != "rebuild" {
		t.Fatalf("wedged teardown of a lease adoption escalated as %q, want \"rebuild\": "+
			"the session is being replaced, so escalation must self-exec and keep the node alive, "+
			"not os.Exit(91)", got)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// realExec (upgrade_state.go) now builds its environment with execEnv, which
// STRIPS the two lineage variables and then re-adds them from the process-global
// execLineage. That global is only populated by agent.New — but one realExec
// caller runs BEFORE any Agent exists: main() calls BootUpgradeCheck before
// Cobra parsing, and its bootRollback arm execs through realExec. On that path
// execEnv deletes the inherited lineage and re-adds nothing, so the rolled-back
// image starts as a stranger: a leased instance comes back contesting for its
// basename. The pre-change code (os.Environ) preserved it.
func TestExecEnvPreservesAnInheritedLineageBeforeAnyAgentExists(t *testing.T) {
	const iid = "abcdefghijklmnopqrstuvwxyz"
	t.Setenv(instanceIDEnv, iid)
	t.Setenv(routingNIDEnv, "gpu1-02")

	// The boot path runs before agent.New, so the process-global lineage is
	// empty there. Reproduce that state exactly.
	savedID := execLineage.instanceID.Load()
	savedNID := execLineage.routingNID.Load()
	execLineage.instanceID.Store(nil)
	execLineage.routingNID.Store(nil)
	t.Cleanup(func() {
		execLineage.instanceID.Store(savedID)
		execLineage.routingNID.Store(savedNID)
	})

	var sawID, sawNID bool
	for _, kv := range execEnv() {
		switch kv {
		case instanceIDEnv + "=" + iid:
			sawID = true
		case routingNIDEnv + "=gpu1-02":
			sawNID = true
		}
	}
	if !sawID || !sawNID {
		t.Fatalf("execEnv dropped the inherited lineage on the pre-Agent boot path "+
			"(instance_id kept=%v routing_nid kept=%v); a boot-time rollback therefore renames "+
			"a leased instance, which is exactly what carrying the lineage across exec exists to prevent",
			sawID, sawNID)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// D2 requires the instance lineage to be STRIPPED from managed child
// environments, so that a user command which itself starts an agent begins a new
// lineage instead of continuing this one (two processes the broker would then
// read as one instance, i.e. the fan-out restored through the environment).
// execBaseEnv does strip — but buildExecCmd only ever assigns cmd.Env when the
// request carries an explicit env map, and the stripped base it computes is
// handed to the spawn policy, whose result is used only on the hung-mount
// outage branch. On the ordinary path cmd.Env stays nil and os/exec inherits
// the agent's environment verbatim.
func TestExecChildrenNeverInheritTheInstanceLineage(t *testing.T) {
	const iid = "abcdefghijklmnopqrstuvwxyz"
	t.Setenv(instanceIDEnv, iid)
	a, err := New(Config{
		NATSURL: "nats://127.0.0.1:1",
		SID:     "lab",
		NID:     "gpu1",
		Logger:  testharness.SilentLog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd, _, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"/bin/true"}})
	if err != nil {
		t.Fatal(err)
	}
	// THE DEFENCE IS THE AGENT'S OWN ENVIRONMENT, not the child's.
	//
	// The reviewer's diagnosis was right — several spawn paths deliberately
	// leave cmd.Env nil (remote-fs-resilience M2 pins the healthy-hangable path
	// as byte-identical), so stripping at each call site could never be
	// complete, and the next path added would silently miss it. So the agent
	// CONSUMES the lineage variables at startup (mintInstanceID) and re-injects
	// them only at the two syscall.Exec sites. A child cannot inherit what the
	// parent no longer has, which makes nil-env paths safe by construction.
	if got := os.Getenv(instanceIDEnv); got != "" {
		t.Fatalf("after New() the agent still carries %s=%q in its own environment; every "+
			"nil-env spawn path would hand it to the child", instanceIDEnv, got)
	}
	// And whatever env the child does get must not contain it either.
	childEnv := cmd.Env
	if childEnv == nil {
		childEnv = os.Environ()
	}
	for _, kv := range childEnv {
		if kv == instanceIDEnv+"="+iid {
			t.Fatalf("exec child inherited %s", kv)
		}
	}
}
