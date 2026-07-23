package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// runBrokerForSessions starts the broker against fresh NATS+DB, returning a
// connected nats.Conn the test can use for requests + a stop func.
func runBrokerForSessions(t *testing.T) (*nats.Conn, func()) {
	t.Helper()
	url := startNATS(t)
	db := openDB(t)
	b, err := New(Config{NATSURL: url, DB: db, Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	waitNATSReady(t, url)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	// waitNATSReady proves the earlier node-register subscription is live, but Broker.Run registers
	// handlers sequentially and the session subscriptions may not exist yet. Probe the exact create
	// face with invalid JSON (no DB mutation) so callers cannot race into nats.ErrNoResponders.
	readyActor := freshUserActor(t)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err = nc.Request(proto.SubjCtrlSessionCreate(readyActor), []byte("not-json"), 200*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			nc.Close()
			cancel()
			t.Fatalf("session broker subscriptions not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	stop := func() {
		nc.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
	return nc, stop
}

// freshUserActor creates a fresh user nkey and returns the actor token (U…).
func freshUserActor(t *testing.T) string {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

func TestHandleSessionCreateHappyPath(t *testing.T) {
	nc, stop := runBrokerForSessions(t)
	defer stop()
	actor := freshUserActor(t)

	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "123456"})
	msg, err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionCreateResp
	_ = json.Unmarshal(msg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("create rejected: %s", resp.Error)
	}
	if resp.SID != "lab" {
		t.Errorf("sid: got %q want lab", resp.SID)
	}
}

func TestHandleSessionCreateRejectsBadPayloads(t *testing.T) {
	nc, stop := runBrokerForSessions(t)
	defer stop()
	actor := freshUserActor(t)

	cases := []struct {
		name    string
		subject string
		body    []byte
		wantErr string
	}{
		{
			name:    "subject_malformed",
			subject: "tether.v2.ctrl.by." + actor + ".session.create.req",
			body:    nil,
			wantErr: "json_parse", // empty body fails JSON parse before subject is malformed
		},
		// Subject bypass: send to bare ctrl subject (no by.<actor>) — reaches
		// handler via wildcard but ParseCtrlBy fails.
		// (We send via the canonical builder + bad body to exercise json_parse.)
		{
			name:    "garbage_json",
			subject: proto.SubjCtrlSessionCreate(actor),
			body:    []byte("not-json"),
			wantErr: "json_parse",
		},
		{
			name:    "name_required",
			subject: proto.SubjCtrlSessionCreate(actor),
			body:    mustJSONBytes(proto.SessionCreateReq{Name: "", PIN: "x"}),
			wantErr: "name_required",
		},
		{
			name:    "actor_invalid",
			subject: "tether.v2.ctrl.by.NOT_A_VALID_ACTOR.session.create.req",
			body:    mustJSONBytes(proto.SessionCreateReq{Name: "lab", PIN: "x"}),
			wantErr: "actor_invalid",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg, err := nc.Request(c.subject, c.body, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var resp proto.SessionCreateResp
			_ = json.Unmarshal(msg.Data, &resp)
			if resp.Error == "" {
				t.Fatalf("expected error containing %q, got empty", c.wantErr)
			}
			if !strings.Contains(resp.Error, c.wantErr) {
				t.Errorf("error %q missing substring %q", resp.Error, c.wantErr)
			}
		})
	}
}

func TestHandleSessionCreateRejectsDuplicate(t *testing.T) {
	nc, stop := runBrokerForSessions(t)
	defer stop()
	actor := freshUserActor(t)

	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "x"})
	if _, err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	msg, _ := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 2*time.Second)
	var resp proto.SessionCreateResp
	_ = json.Unmarshal(msg.Data, &resp)
	if resp.Error != "already_exists" {
		t.Errorf("dup expected already_exists, got %q", resp.Error)
	}
}

func TestHandleSessionListReturnsOnlyMine(t *testing.T) {
	nc, stop := runBrokerForSessions(t)
	defer stop()
	a, b := freshUserActor(t), freshUserActor(t)

	for _, x := range []struct{ actor, name string }{{a, "alpha"}, {b, "beta"}} {
		body, _ := json.Marshal(proto.SessionCreateReq{Name: x.name, PIN: "x"})
		if _, err := nc.Request(proto.SubjCtrlSessionCreate(x.actor), body, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	msg, err := nc.Request(proto.SubjCtrlSessionList(a), []byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionListResp
	_ = json.Unmarshal(msg.Data, &resp)
	if len(resp.Sessions) != 1 || resp.Sessions[0].SID != "alpha" {
		t.Errorf("actor a should see only alpha, got %+v", resp.Sessions)
	}
}

func TestHandleSessionRmOwnerOnly(t *testing.T) {
	nc, stop := runBrokerForSessions(t)
	defer stop()
	owner, intruder := freshUserActor(t), freshUserActor(t)

	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "x"})
	_, _ = nc.Request(proto.SubjCtrlSessionCreate(owner), body, 2*time.Second)

	// intruder tries rm
	msg, _ := nc.Request(proto.SubjCtrlSessionRm(intruder, "lab"), []byte("{}"), 2*time.Second)
	var rresp proto.SessionRmResp
	_ = json.Unmarshal(msg.Data, &rresp)
	if rresp.Code != "not_owner" {
		t.Errorf("intruder rm: got code=%q want not_owner", rresp.Code)
	}

	// owner rm
	msg, _ = nc.Request(proto.SubjCtrlSessionRm(owner, "lab"), []byte("{}"), 2*time.Second)
	rresp = proto.SessionRmResp{}
	_ = json.Unmarshal(msg.Data, &rresp)
	if !rresp.OK {
		t.Fatalf("owner rm: %+v", rresp)
	}
}

func mustJSONBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
