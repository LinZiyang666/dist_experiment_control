package broker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func TestD9ExposeRollbackUsesFencedAllocationFree(t *testing.T) {
	url := startNATS(t)
	brokerNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer brokerNC.Close()
	agentNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentNC.Close()
	ctlNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer ctlNC.Close()

	actor := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{cfg: Config{
		DB:                      openDB(t),
		Logger:                  silentLogger(),
		Now:                     time.Now,
		PublicHost:              "broker.example.test",
		PortBandLow:             14000,
		PortBandHigh:            14000,
		ExposeForwardTimeoutDur: time.Second,
	}}
	seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")

	agentErr := make(chan error, 1)
	agentSub, err := agentNC.Subscribe(proto.SubjCmdForwarded("lab", "lab-1", "expose"), func(msg *nats.Msg) {
		var req proto.ExposeForwardedReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			agentErr <- err
			_ = msg.Respond(mustJSON(proto.ExposeForwardedResp{OK: false, Code: "bad_json", Error: err.Error()}))
			return
		}
		if req.Port != 14000 || req.Name != "first" {
			agentErr <- fmt.Errorf("unexpected forward request: %+v", req)
			_ = msg.Respond(mustJSON(proto.ExposeForwardedResp{OK: false, Code: "bad_request"}))
			return
		}

		if err := port.Free(b.cfg.DB, req.Port, time.Now().UTC()); err != nil {
			agentErr <- fmt.Errorf("free original allocation: %w", err)
			_ = msg.Respond(mustJSON(proto.ExposeForwardedResp{OK: false, Code: "setup_failed", Error: err.Error()}))
			return
		}
		second, err := port.Allocate(b.cfg.DB, "lab", "lab-1", "second", 9001, req.Port, fp, false, b.cfg.PortAllocCfg())
		if err != nil {
			agentErr <- fmt.Errorf("reuse allocation: %w", err)
			_ = msg.Respond(mustJSON(proto.ExposeForwardedResp{OK: false, Code: "setup_failed", Error: err.Error()}))
			return
		}
		if second.Port != req.Port {
			agentErr <- fmt.Errorf("expected port reuse %d, got %d", req.Port, second.Port)
			_ = msg.Respond(mustJSON(proto.ExposeForwardedResp{OK: false, Code: "setup_failed"}))
			return
		}

		agentErr <- nil
		_ = msg.Respond(mustJSON(proto.ExposeForwardedResp{OK: false, Code: "agent_denied", Error: "test reject"}))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = agentSub.Unsubscribe() }()

	exposeSubj := proto.SubjCmdBy("lab", actor, "lab-1", "expose")
	brokerSub, err := brokerNC.Subscribe(exposeSubj, func(msg *nats.Msg) { b.handleExposeReq(brokerNC, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = brokerSub.Unsubscribe() }()
	if err := agentNC.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := brokerNC.Flush(); err != nil {
		t.Fatal(err)
	}

	body := mustJSON(proto.ExposeReq{Name: "first", LocalPort: 9000, RemotePort: 14000})
	reply, err := ctlNC.Request(exposeSubj, body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.ExposeResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != "agent_rejected:agent_denied" {
		t.Fatalf("expose response code = %q, want agent_rejected:agent_denied (err=%q)", resp.Code, resp.Error)
	}
	select {
	case err := <-agentErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("agent handler did not run")
	}

	got, err := port.LookupByName(b.cfg.DB, "lab", "second")
	if err != nil {
		t.Fatalf("rollback freed the reused allocation: %v", err)
	}
	if got.Port != 14000 || got.LocalPort != 9001 {
		t.Fatalf("reused allocation changed: %+v", got)
	}
	if _, err := port.LookupByName(b.cfg.DB, "lab", "first"); err == nil {
		t.Fatal("original allocation stayed active after simulated agent-side free")
	}
}
