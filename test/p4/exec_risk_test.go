package p4_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func TestExecMissingNodeReturnsBrokerError(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	body, _ := json.Marshal(proto.ExecReq{Argv: []string{"echo", "x"}})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy("lab", pub, "missing-node", "exec"), inbox, body); err != nil {
		t.Fatal(err)
	}

	msg, err := sub.NextMsg(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("exec against a missing node timed out instead of returning a broker error chunk: %v", err)
	}
	var c proto.ExecChunk
	if err := json.Unmarshal(msg.Data, &c); err != nil {
		t.Fatal(err)
	}
	if c.Kind != "error" {
		t.Fatalf("missing node should return an error chunk, got %+v", c)
	}
}

func TestPsRejectsDeletingSession(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBroker(t, url, db)()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	body, _ := json.Marshal(proto.PsReq{})
	msg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.PsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code == "" {
		t.Fatalf("ps on a DELETING session should be rejected, got success with %+v", resp.Processes)
	}
}

func TestExecRecordsStartedByFingerprint(t *testing.T) {
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

	res := runExec(t, nc, "lab", pub, "lab-1", []string{"echo", "started-by"})
	if res.Err != "" || res.ExitCode != 0 {
		t.Fatalf("exec failed: %+v", res)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		p, err := proc.Get(db, res.PID)
		if err == nil {
			if p.StartedByFP != fp {
				t.Fatalf("process started_by_fp mismatch: got %q want %q", p.StartedByFP, fp)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process row was not recorded: pid=%s", res.PID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
