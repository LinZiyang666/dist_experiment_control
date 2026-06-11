package p4_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// TestSafeReqRoundTripsBrokerBothVerbs pins the wire safety of the
// remote-fs-resilience Safe field: it must survive the broker's
// Unmarshal → stamp-ActorFP → Marshal forward (architecture C.1 §4) on BOTH
// exec and run, while ActorFP is still overwritten with the broker-parsed fp.
// We register an ONLINE node directly (no real agent) so we can subscribe to
// the .forwarded subject ourselves and inspect the forwarded body.
func TestSafeReqRoundTripsBrokerBothVerbs(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	if err := node.Register(db, node.RegisterInput{
		SID: "lab", NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "test", OS: "linux", Arch: "amd64", BootID: "b",
	}, time.Now().UTC()); err != nil {
		t.Fatalf("node.Register: %v", err)
	}
	defer startBroker(t, url, db)()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	t.Run("exec", func(t *testing.T) {
		ch := make(chan *nats.Msg, 1)
		sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded("lab", "lab-1", "exec"), ch)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()

		// Safe:true and a bogus ActorFP (which the broker must overwrite).
		body, _ := json.Marshal(proto.ExecReq{Argv: []string{"echo"}, Safe: true, ActorFP: "SHA256:bogus"})
		inbox := nc.NewInbox()
		if err := nc.PublishRequest(proto.SubjCmdBy("lab", pub, "lab-1", "exec"), inbox, body); err != nil {
			t.Fatal(err)
		}

		select {
		case m := <-ch:
			var fwd proto.ExecReq
			if err := json.Unmarshal(m.Data, &fwd); err != nil {
				t.Fatal(err)
			}
			if !fwd.Safe {
				t.Errorf("Safe was lost across the broker forward: %+v", fwd)
			}
			if fwd.ActorFP != fp {
				t.Errorf("ActorFP not broker-stamped: got %q want %q", fwd.ActorFP, fp)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("exec.req.forwarded never arrived")
		}
	})

	t.Run("run", func(t *testing.T) {
		ch := make(chan *nats.Msg, 1)
		sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded("lab", "lab-1", "run"), ch)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()

		body, _ := json.Marshal(proto.RunReq{Argv: []string{"bash"}, Safe: true, ActorFP: "SHA256:bogus"})
		inbox := nc.NewInbox()
		if err := nc.PublishRequest(proto.SubjCmdBy("lab", pub, "lab-1", "run"), inbox, body); err != nil {
			t.Fatal(err)
		}

		select {
		case m := <-ch:
			var fwd proto.RunReq
			if err := json.Unmarshal(m.Data, &fwd); err != nil {
				t.Fatal(err)
			}
			if !fwd.Safe {
				t.Errorf("Safe was lost across the broker forward: %+v", fwd)
			}
			if fwd.ActorFP != fp {
				t.Errorf("ActorFP not broker-stamped: got %q want %q", fwd.ActorFP, fp)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("run.req.forwarded never arrived")
		}
	})
}
