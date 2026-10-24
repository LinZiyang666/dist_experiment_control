package xferagent

import (
	"bytes"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A sender's source seal attempt is its own durable evidence. During a same-
// store JetStream restart the agent's control session is still reconnecting,
// so the attempt ends before any publish; one refused at publish ends there.
// Both ended at a real instant: an attempt without a finish time reads as
// still in flight, and the production observation encoder refuses it.
// origin: S7 internal review FIX-TIERA2 F1 / hot-update S7 plan §10.4 / T41 / T46
func TestTransferAgentAttemptRecordsItsEndOnEveryRefusal(t *testing.T) {
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, MaxPayload: 1024})
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer func() { server.Shutdown(); server.WaitForShutdown() }()
	if !server.ReadyForConnections(3 * time.Second) {
		t.Fatal("embedded NATS did not start")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRefusedAgentAttempts(t, server.ClientURL())
	}
	testharness.AssertNoGoroutineLeak(t, "refused agent attempts", g)
	testharness.AssertNoFDLeak(t, "refused agent attempts", fd, 0)
}

func exerciseRefusedAgentAttempts(t *testing.T, url string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	disconnected := inlineRuntime(t)
	connected, err := New(Host{SID: "session", NID: func() string { return "node" }, Instance: func() string { return "agent-instance" },
		JetStream: func() jetstream.JetStream { return js }})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		rt   *Runtime
		body []byte
	}{
		// The control session is not connected: the attempt ends before SUB.
		{"session-reconnecting", disconnected, []byte("{}")},
		// The server's payload ceiling refuses this PUB before it leaves.
		{"publish-refused", connected, bytes.Repeat([]byte("x"), 4096)},
	} {
		deadline, err := controlbudget.After(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_, attempt, err := tc.rt.request("ev.session.node.transfer.source.req", tc.body, "xfer-source-entry", deadline)
		if err == nil || attempt.Sent {
			t.Fatalf("%s: refused attempt was reported sent: err=%v attempt=%+v", tc.name, err, attempt)
		}
		if attempt.StartedMonoNS <= 0 || attempt.FinishedMonoNS < attempt.StartedMonoNS {
			t.Fatalf("%s: refused attempt carries no end: %+v", tc.name, attempt)
		}
		// The real encoder, stamped exactly as the process observer stamps it.
		boot, now, err := disconnected.base().Clock()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := drillobserve.EncodeTransfer(x.Observation{Kind: x.ObserveAttempt, PID: os.Getpid(), ProcessStart: 1, BootID: boot, Generation: 1,
			EventSeq: 1, BoottimeNS: now, TransferID: "transfer", EntryNonce: "entry", Leg: "sender", Phase: x.PhaseSending,
			OriginalDeadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(now) + x.Deadline(time.Second)}, Attempt: attempt}); err != nil {
			t.Fatalf("%s: the observer cannot record the refused seal attempt: %v (%+v)", tc.name, err, attempt)
		}
	}
}
