package main

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// pty_intake_addressing_test.go — where a keystroke goes.
//
// origin: prerelease audit round 2, I-F6.
//
// `.in` and `.resize` were published on a SESSION-scoped subject and every agent in the
// session subscribed to the matching wildcard. So every agent received a copy of every
// other node's raw keystroke stream and dropped it on the pid lookup. That stream is not
// metadata — it is whatever the operator types into an interactive program, which on
// this fleet includes passwords typed at a jump host.
//
// The fix is additive, and being additive is the whole difficulty: a new ctl has to
// drive an OLD agent (legacy subject only) without making a NEW agent act on the same
// keystroke twice. proto.PtyNodeHeader is what separates those two cases, and these
// tests pin both halves of it.

func ptyBus(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(testharness.StartNATS(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// A NEW agent: it takes the node-scoped copy and drops any legacy copy that carries the
// header. Mirrors startPtyIntake's two subscriptions.
func newAgentIntake(t *testing.T, nc *nats.Conn, sid, nid string, got chan<- string) {
	t.Helper()
	if _, err := nc.Subscribe(proto.SubjectPrefix+".s."+sid+".node."+nid+".pty.*.in",
		func(m *nats.Msg) { got <- "node:" + string(m.Data) }); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(proto.SubjectPrefix+".s."+sid+".pty.*.in", func(m *nats.Msg) {
		if m.Header.Get(proto.PtyNodeHeader) != "" {
			return
		}
		got <- "legacy:" + string(m.Data)
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

// origin: prerelease audit round 2, I-F6.
//
// EXACTLY ONCE. An upgraded ctl publishes to both subjects, so without the header an
// upgraded agent would write every keystroke to the pty twice — and a keystroke stream
// cannot be de-duplicated after the fact, so "twice" means the operator's shell sees
// "llss" when they typed "ls".
func TestAnUpgradedCtlDeliversEachKeystrokeOnceToAnUpgradedAgent(t *testing.T) {
	nc := ptyBus(t)
	got := make(chan string, 8)
	newAgentIntake(t, nc, "lab", "gpu1", got)

	publishPtyBoth(nc, proto.SubjPtyInNode("lab", "gpu1", "p1"), proto.SubjPtyIn("lab", "p1"), []byte("ls\n"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var delivered []string
	deadline := time.After(2 * time.Second)
	for {
		select {
		case d := <-got:
			delivered = append(delivered, d)
			if len(delivered) > 1 {
				t.Fatalf("one keystroke was delivered %d times: %v.\n\n"+
					"The ctl publishes to both the node-scoped and the legacy subject so it can "+
					"still drive an agent that predates the node-scoped one. An upgraded agent "+
					"subscribes to both, so the legacy copy MUST carry %s and be dropped.",
					len(delivered), delivered, proto.PtyNodeHeader)
			}
		case <-time.After(300 * time.Millisecond):
			if len(delivered) != 1 {
				t.Fatalf("keystroke deliveries = %v, want exactly one (the node-scoped copy)", delivered)
			}
			if delivered[0] != "node:ls\n" {
				t.Fatalf("the keystroke arrived on the LEGACY path (%q); the node-scoped "+
					"subscription is what removes the fan-out and it must be the one that wins",
					delivered[0])
			}
			return
		case <-deadline:
			t.Fatal("no keystroke arrived at all")
		}
	}
}

// origin: prerelease audit round 2, I-F6.
//
// THE N-1 DIRECTION. An OLD ctl publishes only the legacy subject and sets no header, so
// an upgraded agent must still act on it — otherwise upgrading the agent (which
// requirements §6.7 says happens BEFORE the ctl) breaks every interactive run.
func TestAnOldCtlStillReachesAnUpgradedAgent(t *testing.T) {
	nc := ptyBus(t)
	got := make(chan string, 8)
	newAgentIntake(t, nc, "lab", "gpu1", got)

	// Exactly what a pre-I-F6 ctl does: one publish, no header.
	if err := nc.Publish(proto.SubjPtyIn("lab", "p1"), []byte("ls\n")); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if d != "legacy:ls\n" {
			t.Fatalf("delivery = %q, want the legacy path", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an OLD ctl's keystroke never reached the agent.\n\n" +
			"The session-scoped subscription is retained precisely for this case; dropping it " +
			"before every ctl in the fleet is upgraded breaks interactive runs during the " +
			"broker→agent→ctl upgrade order requirements §6.7 mandates.")
	}
}

// origin: prerelease audit round 2, I-F6.
//
// THE OTHER N-1 DIRECTION, and the one the first version of this file could not see: an
// upgraded ctl driving an agent that has NOT been upgraded. That agent subscribes only
// to the session-scoped subject and knows nothing about the header, so the ctl must keep
// publishing the legacy copy. Dropping it would look correct in every test that models
// an upgraded agent, and would silently break every interactive run against a node that
// is one release behind — including during a rollback, which requirements §6.7 calls a
// first-class case.
func TestAnUpgradedCtlStillReachesAnOldAgent(t *testing.T) {
	nc := ptyBus(t)
	got := make(chan string, 4)
	// An OLD agent: the session-scoped subscription only, and no header check because
	// the header did not exist when it was built.
	if _, err := nc.Subscribe(proto.SubjectPrefix+".s.lab.pty.*.in",
		func(m *nats.Msg) { got <- string(m.Data) }); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	publishPtyBoth(nc, proto.SubjPtyInNode("lab", "gpu1", "p1"), proto.SubjPtyIn("lab", "p1"), []byte("ls\n"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if d != "ls\n" {
			t.Fatalf("delivery = %q", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an upgraded ctl's keystroke never reached an agent that only subscribes to the " +
			"session-scoped subject.\n\n" +
			"Publishing node-scoped ONLY closes the fan-out and breaks every run against a node " +
			"that has not been upgraded yet — or has been rolled back.")
	}
}

// origin: prerelease audit round 2, I-F6.
//
// THE POINT OF THE WHOLE CHANGE: a keystroke addressed to gpu1 must not be delivered to
// gpu2's connection at all. This asserts the SUBJECT's addressing; the ACL that makes it
// unforgeable — an agent may only subscribe under its own nid — is asserted against a
// real nats-server in test/d3.
func TestANodeScopedKeystrokeIsNotDeliveredToAnotherNode(t *testing.T) {
	nc := ptyBus(t)
	mine := make(chan string, 4)
	theirs := make(chan string, 4)
	if _, err := nc.Subscribe(proto.SubjectPrefix+".s.lab.node.gpu1.pty.*.in",
		func(m *nats.Msg) { mine <- string(m.Data) }); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(proto.SubjectPrefix+".s.lab.node.gpu2.pty.*.in",
		func(m *nats.Msg) { theirs <- string(m.Data) }); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := nc.Publish(proto.SubjPtyInNode("lab", "gpu1", "p1"), []byte("hunter2\n")); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mine:
	case <-time.After(2 * time.Second):
		t.Fatal("the addressed node did not receive its own keystroke")
	}
	select {
	case d := <-theirs:
		t.Fatalf("gpu2 received gpu1's keystroke (%q).\n\n"+
			"That is the fan-out I-F6 reports: every agent in the session saw every other "+
			"node's raw input, which is whatever the operator typed — including a password at "+
			"a jump host.", d)
	case <-time.After(300 * time.Millisecond):
	}
}
