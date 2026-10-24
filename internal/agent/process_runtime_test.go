//go:build linux

// process_runtime_test.go — the reliable process runtime's lifetime is the
// AGENT's, not one NATS session's. A session rebuild (the roster watchdog's
// fireRedial: the old conn is torn down, a new one connects and re-registers)
// must leave a running reliable task untouched: same PID, bytes continue at
// the next offset, the exact exit code settles — and the replies to the
// resumed client go out on the NEW connection (liveConn), never the dead
// spawn-time one. This is the hermetic middle layer between the execclient
// unit tests (no agent) and the real-binary e2e (nats.go reconnects only,
// never a session rebuild: every e2e gap is shorter than redialAfter).
// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2
// (TestReliableTaskSurvivesSessionRebuild); S1 internal review E5(a).
package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// rebuildProbe drives the process-resume protocol from a plain NATS client
// straight at the agent's forwarded subject (no broker in this test: the
// actor is whatever the body says, exactly what a broker would have stamped).
type rebuildProbe struct {
	t       *testing.T
	nc      *nats.Conn
	subject string
	inbox   string
	sub     *nats.Subscription
	callID  uint64
}

func (p *rebuildProbe) call(req proto.ProcessResumeReq) (proto.ProcessResumeReply, bool) {
	p.t.Helper()
	p.callID++
	req.Version, req.CallID = proto.ProcessResumeVersion, p.callID
	body, err := json.Marshal(req)
	if err != nil {
		p.t.Fatal(err)
	}
	if err := p.nc.PublishRequest(p.subject, p.inbox, body); err != nil {
		p.t.Fatal(err)
	}
	for {
		msg, err := p.sub.NextMsg(400 * time.Millisecond)
		if err != nil {
			return proto.ProcessResumeReply{}, false // no responder / lost: the caller resends
		}
		var reply proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil {
			p.t.Fatal(err)
		}
		if reply.CallID != req.CallID {
			continue // a late reply to an earlier attempt
		}
		if reply.Kind == "error" {
			p.t.Fatalf("op %s refused: %s (%s)", req.Op, reply.Code, reply.Error)
		}
		return reply, true
	}
}

func TestReliableTaskSurvivesSessionRebuild(t *testing.T) {
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
	// The courier's proc events are answered so started/exit settle.
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.proc.*", func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond([]byte(`{"ok":true}`))
		}
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{NATSURL: url, SID: "lab", NID: "lab-1", HeartbeatInterval: 25 * time.Millisecond,
		RegisterTimeout: 2 * time.Second, RosterRefreshInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	runtimeBefore := a.processRuntime
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor(t, 3*time.Second, func() bool { return registerSeen.Load() >= 1 && heartbeatSeen.Load() >= 1 })

	probe := &rebuildProbe{t: t, nc: stub, subject: proto.SubjCmdForwarded("lab", "lab-1", proto.ProcessResumeVerb),
		inbox: "_TINBOX.rebuild-test.reply"}
	probe.sub, err = stub.SubscribeSync(probe.inbox)
	if err != nil {
		t.Fatal(err)
	}
	actor, lineage := "SHA256:"+strings.Repeat("A", 43), "rebuildlineage" // a strict base64 fingerprint body
	cap, ok := probe.call(proto.ProcessResumeReq{Op: "probe", ActorFP: actor, ClientLineage: lineage})
	if !ok || cap.Lease == nil {
		t.Fatalf("probe: %+v", cap)
	}
	handle := &proto.ProcessHandle{Lease: *cap.Lease, RequestID: "rebuildrequest"}
	// ~3 s of output at 40 lines/s, then exit 23.
	script := "i=0; while [ $i -lt 120 ]; do echo line$i; i=$((i+1)); sleep 0.025; done; exit 23"
	created, ok := probe.call(proto.ProcessResumeReq{Op: "create", ActorFP: actor, Handle: handle, AttachEpoch: 1,
		Exec: &proto.ExecReq{Argv: []string{"/bin/sh", "-c", script}}})
	if !ok || created.PID == "" {
		t.Fatalf("create: %+v", created)
	}
	pid := created.PID

	// Pump acks until the terminal, rebuilding the session once we hold at
	// least 20 lines. Replies that go missing during the rebuild (the
	// forwarded subscription moves to the new conn) are simply resent.
	var out strings.Builder
	var stdoutACK, stderrACK uint64
	var stdoutFIN, stderrFIN bool
	rebuilt := false
	var terminal proto.ProcessResumeReply
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		reply, ok := probe.call(proto.ProcessResumeReq{Op: "ack", ActorFP: actor, Handle: handle, AttachEpoch: 1,
			StdoutACK: stdoutACK, StderrACK: stderrACK, StdoutFIN: stdoutFIN, StderrFIN: stderrFIN})
		if !ok {
			continue
		}
		if reply.PID != pid {
			t.Fatalf("the task changed identity across the rebuild: %s → %s", pid, reply.PID)
		}
		if reply.Stdout != nil {
			if reply.Stdout.Offset != stdoutACK {
				t.Fatalf("stdout batch at offset %d, expected the next byte %d: bytes lost or replayed", reply.Stdout.Offset, stdoutACK)
			}
			out.Write(reply.Stdout.Data)
			stdoutACK += uint64(len(reply.Stdout.Data))
			if reply.Stdout.FIN {
				stdoutFIN = true
			}
		}
		if reply.Stderr != nil {
			stderrACK += uint64(len(reply.Stderr.Data))
			if reply.Stderr.FIN {
				stderrFIN = true
			}
		}
		if !rebuilt && strings.Count(out.String(), "\n") >= 20 {
			before := registerSeen.Load()
			a.fireRedial() // the roster watchdog: tear the session down, rebuild it
			waitFor(t, 5*time.Second, func() bool { return registerSeen.Load() > before })
			rebuilt = true
		}
		if reply.TerminalID != "" && stdoutFIN && stderrFIN {
			terminal = reply
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !rebuilt {
		t.Fatal("the session was never rebuilt while the task ran")
	}
	if terminal.TerminalID == "" {
		t.Fatalf("no terminal within the budget: %d bytes so far", stdoutACK)
	}
	if terminal.ExitCode != 23 || terminal.Signal != 0 || terminal.Error != "" {
		t.Fatalf("terminal: %+v", terminal)
	}
	for i := 0; i < 120; i++ {
		if !strings.Contains(out.String(), "line"+itoa(i)+"\n") {
			t.Fatalf("line%d missing from the delivered stdout (%d bytes)", i, out.Len())
		}
	}
	settled, ok := probe.call(proto.ProcessResumeReq{Op: "terminal-ack", ActorFP: actor, Handle: handle, AttachEpoch: 1,
		StdoutACK: stdoutACK, StderrACK: stderrACK, StdoutFIN: true, StderrFIN: true, TerminalID: terminal.TerminalID})
	if !ok || settled.Phase != "settled" {
		t.Fatalf("terminal-ack: %+v", settled)
	}
	if a.processRuntime != runtimeBefore {
		t.Fatal("the process runtime was replaced by the session rebuild: its tasks would have died with the old session")
	}
	select {
	case err := <-done:
		t.Fatalf("agent exited during the test: %v", err)
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not exit on cancel")
	}
}
