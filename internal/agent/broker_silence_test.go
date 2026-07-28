package agent

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// r14_broker_silence_test.go — #48 (docs/reviews/r6-findings.md): after a broker is retired the
// agent stays connected to its still-running nats-server (the disconnect never comes) while the
// retired broker answers nothing (handleRegister early-returns on isClusterFollower). Neither the
// "current broker is leaving" roster edge (needs a reply) nor the disconnect-armed redial watchdog
// can fire, so the islanded agent starves. The fix: sustained roster-refresh silence on a healthy
// connection, with another dialable voter known, rebuilds the session onto that voter.

func TestHasOtherDialableVoter(t *testing.T) {
	voter := func(host string) proto.RosterBroker {
		return proto.RosterBroker{PublicHost: host, Phase: proto.RosterPhaseVoter}
	}
	mk := func(brokers ...proto.RosterBroker) *Agent {
		a := &Agent{}
		a.cachedRoster = &proto.ClusterRoster{Brokers: brokers}
		return a
	}

	if !mk(voter("a.example.com"), voter("b.example.com")).hasOtherDialableVoter("a.example.com") {
		t.Error("two distinct dialable voters: must find the other one as an exit")
	}
	if mk(voter("a.example.com")).hasOtherDialableVoter("a.example.com") {
		t.Error("a lone voter (itself) must NOT count as an exit")
	}
	if mk(voter("a.example.com"), proto.RosterBroker{PublicHost: "b.example.com", Phase: proto.RosterPhaseDraining}).
		hasOtherDialableVoter("a.example.com") {
		t.Error("a non-voter must not count as a dialable-voter exit")
	}
	if mk(voter("a.example.com"), voter("127.0.0.1")).hasOtherDialableVoter("a.example.com") {
		t.Error("an undialable (loopback) voter must not count")
	}
	if (&Agent{}).hasOtherDialableVoter("a.example.com") {
		t.Error("no cached roster → no exit")
	}
}

func TestExcludeHostFromDial(t *testing.T) {
	in := "nats://a.example.com:4222,nats://b.example.com:4222"
	if got := excludeHostFromDial(in, "a.example.com"); got != "nats://b.example.com:4222" {
		t.Errorf("exclude a.example.com: got %q", got)
	}
	if got := excludeHostFromDial(in, "z.example.com"); got != in {
		t.Errorf("no-match must be unchanged: got %q", got)
	}
	if got := excludeHostFromDial(in, ""); got != in {
		t.Errorf("empty host must be unchanged: got %q", got)
	}
	// Excluding the ONLY host must not strand the agent onto an empty dial string.
	solo := "nats://a.example.com:4222"
	if got := excludeHostFromDial(solo, "a.example.com"); got != solo {
		t.Errorf("must not strand to empty: got %q", got)
	}
}

func TestTakeAvoidHostIsOneShot(t *testing.T) {
	a := &Agent{}
	a.setAvoidHost("a.example.com")
	if got := a.takeAvoidHost(); got != "a.example.com" {
		t.Fatalf("first take: got %q, want a.example.com", got)
	}
	if got := a.takeAvoidHost(); got != "" {
		t.Fatalf("second take must be empty (one-shot): got %q", got)
	}
}

// TestBrokerSilenceEscapesToVoter is the end-to-end #48 proof over real NATS: the broker establishes
// the session once (delivers a roster naming a survivor voter) then goes SILENT — the retired-broker
// island. With no disconnect and no roster reply, the agent must still detect the silence and rebuild
// onto the known voter, avoiding the silent broker's host on the next dial.
func TestBrokerSilenceEscapesToVoter(t *testing.T) {
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	// A survivor voter on a distinct DIALABLE host so hasOtherDialableVoter sees an exit.
	roster, err := clusterroster.Build(seed, pub, 10,
		[]proto.RosterBroker{{NodeID: "survivor", PublicHost: "survivor.example.com", Phase: proto.RosterPhaseVoter}},
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	var established atomic.Bool
	if _, err := stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(msg *nats.Msg) {
		var req proto.NodeRegisterReq
		_ = json.Unmarshal(msg.Data, &req)
		// Answer the FIRST full register once (establish the session + deliver the roster). After
		// that, answer NOTHING — model the retired broker that early-returns on isClusterFollower.
		if req.RosterRefreshOnly || established.Load() {
			return
		}
		established.Store(true)
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: roster})
		_ = msg.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	escaped := make(chan string, 1)
	hook := func(_ *Agent, avoided string) {
		select {
		case escaped <- avoided:
		default:
		}
	}
	afterSilenceEscapeHook.Store(&hook)
	defer afterSilenceEscapeHook.Store(nil)

	a, err := New(Config{
		NATSURL: url, SID: "lab", NID: "lab-1", AccountPub: pub,
		HeartbeatInterval:     30 * time.Millisecond,
		RegisterTimeout:       200 * time.Millisecond,
		RosterRefreshInterval: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	a.rosterRefreshFailBackoff = 30 * time.Millisecond // test seam: immutable once Run begins
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	select {
	case host := <-escaped:
		if host == "" {
			t.Fatal("escape fired but avoided an empty host")
		}
		if host != "127.0.0.1" {
			t.Fatalf("escape avoided %q; want the silent broker's host 127.0.0.1", host)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("agent did not escape the silent broker within 6s (it starved on the island)")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not exit after cancel")
	}
}
