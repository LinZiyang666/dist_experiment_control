package p5_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func TestRunRecordsProcessLifecycleForPs(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	body, _ := json.Marshal(proto.RunReq{
		Argv: []string{"sh", "-c", "trap 'exit 130' INT; sleep 30"},
		Cols: 80,
		Rows: 24,
	})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy("lab", pub, "lab-1", "run"), inbox, body); err != nil {
		t.Fatal(err)
	}

	// Timeouts: 2s held under local loopback NATS but fired
	// intermittently on shared GitHub Actions runners (the post-
	// attach sess.Start fork/exec + reply round-trip can take
	// > 2s when the CI host is loaded). 10s budget keeps the
	// happy path fast (test exits as soon as the message arrives)
	// and avoids the spurious nats:timeout failure.
	msg, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ready proto.RunChunk
	if err := json.Unmarshal(msg.Data, &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Kind != "ready" {
		t.Fatalf("expected ready, got %+v", ready)
	}

	att, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
	if err := nc.Publish(proto.SubjPtyAttach("lab", ready.PID), att); err != nil {
		t.Fatal(err)
	}

	msg, err = sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var started proto.RunChunk
	if err := json.Unmarshal(msg.Data, &started); err != nil {
		t.Fatal(err)
	}
	if started.Kind != "started" {
		t.Fatalf("expected started, got %+v", started)
	}

	defer func() {
		killBody, _ := json.Marshal(proto.KillReq{PID: ready.PID, Signal: 2})
		_, _ = nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "kill"), killBody, 2*time.Second)
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
			msg, err := sub.NextMsg(100 * time.Millisecond)
			if err != nil {
				continue
			}
			var c proto.RunChunk
			if json.Unmarshal(msg.Data, &c) == nil && c.Kind == "exit" {
				return
			}
		}
	}()

	var p *proc.Process
	waitUntil(t, "run process to be recorded RUNNING", 2*time.Second, func() bool {
		got, err := proc.Get(db, ready.PID)
		if err != nil {
			return false
		}
		p = got
		return p.Status == proc.StateRunning
	})
	if p.StartedByFP != fp {
		t.Fatalf("run process started_by_fp mismatch: got %q want %q", p.StartedByFP, fp)
	}

	killBody, _ := json.Marshal(proto.KillReq{PID: ready.PID, Signal: 2})
	resp, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "kill"), killBody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var killResp proto.KillResp
	if err := json.Unmarshal(resp.Data, &killResp); err != nil {
		t.Fatal(err)
	}
	if !killResp.OK {
		t.Fatalf("kill rejected: %+v", killResp)
	}

	waitUntil(t, "run process to emit exit chunk after kill", 3*time.Second, func() bool {
		msg, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			return false
		}
		var c proto.RunChunk
		return json.Unmarshal(msg.Data, &c) == nil && c.Kind == "exit"
	})

	waitUntil(t, "run process to be recorded EXITED", 2*time.Second, func() bool {
		got, err := proc.Get(db, ready.PID)
		if err != nil {
			return false
		}
		p = got
		return p.Status == proc.StateExited
	})
}

func waitUntil(t *testing.T, label string, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: condition not met within %s", label, timeout)
}
