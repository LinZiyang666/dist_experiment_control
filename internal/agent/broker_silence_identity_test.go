package agent

import (
	"context"
	"encoding/json"
	neturl "net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: control-connection-recovery plan, silence decisions use node identity.
func TestBrokerSilenceUsesConnectedNodeIdentity(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		connected  string
		rosterNode string
		nilReply   bool
		expired    bool
		wantEscape bool
	}{
		{name: "single-broker-alias", connected: "solo", rosterNode: "solo"},
		{name: "legacy-unknown-single-alias", rosterNode: "solo"},
		{name: "removed-current-sole-survivor", connected: "retired", rosterNode: "survivor", wantEscape: true},
		{name: "nil-reply-disables-old-cache", connected: "retired", rosterNode: "survivor", nilReply: true},
		{name: "expired-roster-disables-escape", connected: "retired", rosterNode: "survivor", expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			a, err := New(Config{NATSURL: url, SID: "lab", NID: "identity-agent", AccountPub: pub,
				Now: func() time.Time { return now }, Logger: testharness.SilentLog()})
			if err != nil {
				t.Fatal(err)
			}
			a.loadRosterCacheAtBoot()
			brokers := []proto.RosterBroker{{NodeID: tc.rosterNode, PublicHost: "203.0.113.55",
				NatsRoute: "nats://203.0.113.55:6222", Phase: proto.RosterPhaseVoter}}
			knownThenRemoved := tc.connected != "" && tc.connected != tc.rosterNode
			if knownThenRemoved {
				brokers = append(brokers, proto.RosterBroker{NodeID: tc.connected, PublicHost: "203.0.113.44",
					NatsRoute: "nats://203.0.113.44:6222", Phase: proto.RosterPhaseVoter})
			}
			roster, err := clusterroster.Build(seed, pub, 1, brokers,
				now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			if !a.adoptRoster(roster) {
				t.Fatal("valid signed fixture roster was not accepted")
			}
			a.ncBox.Store(nc)
			connectedURL := nc.ConnectedUrl()
			a.connURLBox.Store(&connectedURL)
			rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL,
				proto.NodeRegisterResp{OK: true, Roster: roster, ConnectedBrokerID: tc.connected})
			if knownThenRemoved {
				roster, err = clusterroster.Build(seed, pub, 2, brokers[:1],
					now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
				if err != nil {
					t.Fatal(err)
				}
				if !a.adoptRoster(roster) {
					t.Fatal("the signed roster removing the current node was not accepted")
				}
				// The removed node no longer has a DB row for server-name lookup.
				// A reply without a fresh mapping must preserve the already known
				// identity while this same connection remains on the removed node.
				rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL,
					proto.NodeRegisterResp{OK: true, Roster: roster})
			}
			if tc.nilReply {
				rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL,
					proto.NodeRegisterResp{OK: true})
			}
			if tc.expired {
				now = now.Add(2 * time.Hour)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.setSessionCancel(cancel)
			escaped := a.rebuildOnBrokerSilence(nc)
			if escaped != tc.wantEscape {
				t.Fatalf("silence escape=%v, want %v", escaped, tc.wantEscape)
			}
			if (ctx.Err() != nil) != tc.wantEscape || a.rebuilding.Load() != tc.wantEscape {
				t.Fatal("silence policy and the actual session cancellation disagree")
			}
			if !tc.wantEscape && a.takeAvoidHost() != "" {
				t.Fatal("a refused escape still changed the next connection's dial exclusions")
			}
		})
	}
}

func TestConnectedBrokerIdentityRejectsStaleRouteReplies(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "identity-agent", Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	a.ncBox.Store(nc)
	serverName, connectedURL := nc.ConnectedServerName(), nc.ConnectedUrl()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	roster, err := clusterroster.Build(seed, pub, 1,
		[]proto.RosterBroker{{NodeID: "current", PublicHost: "203.0.113.55", Phase: proto.RosterPhaseVoter}},
		time.Now().UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if !a.adoptRoster(roster) {
		t.Fatal("valid signed fixture roster was not accepted")
	}
	response := proto.NodeRegisterResp{OK: true, ConnectedBrokerID: "current", Roster: roster}
	rememberConnectedBroker(a, nc, serverName, connectedURL, response)
	for _, stale := range []struct{ serverName, url string }{
		{"previous-server", connectedURL},
		{serverName, "nats://203.0.113.66:4222"},
	} {
		response.ConnectedBrokerID = "stale"
		rememberConnectedBroker(a, nc, stale.serverName, stale.url, response)
		a.rosterMu.Lock()
		current := a.rosterRoute
		got := ""
		if current != nil {
			got = current.brokerID
		}
		a.rosterMu.Unlock()
		if got != "current" {
			t.Fatalf("late reply from a different route replaced the current identity: %q", got)
		}
	}
	successor, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	a.ncBox.Store(successor)
	response.ConnectedBrokerID = "successor"
	rememberConnectedBroker(a, successor, successor.ConnectedServerName(), successor.ConnectedUrl(), response)
	response.ConnectedBrokerID = "old-connection"
	rememberConnectedBroker(a, nc, serverName, connectedURL, response)
	a.rosterMu.Lock()
	current := a.rosterRoute
	got := ""
	if current != nil {
		got = current.brokerID
	}
	a.rosterMu.Unlock()
	if got != "successor" {
		t.Fatalf("a retired NATS connection replaced the successor's identity: %q", got)
	}
	// Identity fallback may use only a previously accepted roster. An unsigned
	// response carrying a convincing address match must not invent a broker ID.
	parsedURL, err := neturl.Parse(successor.ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}
	a.rosterMu.Lock()
	a.rosterRoute = nil
	a.rosterMu.Unlock()
	unsigned := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "unsigned-imposter", PublicHost: parsedURL.Hostname(), Phase: proto.RosterPhaseVoter},
	}}
	rememberConnectedBroker(a, successor, successor.ConnectedServerName(), successor.ConnectedUrl(),
		proto.NodeRegisterResp{OK: true, Roster: unsigned})
	a.rosterMu.Lock()
	current = a.rosterRoute
	got = ""
	if current != nil {
		got = current.brokerID
	}
	a.rosterMu.Unlock()
	if got != "" {
		t.Fatalf("an unverified response roster supplied a connected identity: %q", got)
	}
}

func TestBrokerSilenceExcludesEveryKnownCurrentAlias(t *testing.T) {
	now := time.Now().UTC()
	roster := &proto.ClusterRoster{ExpiresAt: now.Add(time.Hour).Format(time.RFC3339Nano), Brokers: []proto.RosterBroker{
		{NodeID: "current", PublicHost: "current.public", NatsRoute: "nats://203.0.113.44:6222", Phase: proto.RosterPhaseVoter},
		{NodeID: "peer", PublicHost: "peer.public", NatsRoute: "nats://203.0.113.55:6222", Phase: proto.RosterPhaseVoter},
	}}
	avoid, other := rosterVoterDestination(roster, "current.alias", "current", []string{"current.previous"}, now)
	if !other {
		t.Fatal("the distinct peer voter was not selected as an escape destination")
	}
	pool := "nats://current.alias:4222,nats://current.public:4222,nats://203.0.113.44:4222,nats://current.previous:4222,nats://peer.public:4222"
	if got := excludeHostFromDial(pool, avoid); got != "nats://peer.public:4222" {
		t.Fatalf("current broker aliases survived the first-dial exclusion: %s", got)
	}
	// Two public aliases of the same NodeID never provide a second voter.
	roster.Brokers[1].NodeID = "current"
	if _, other := rosterVoterDestination(roster, "current.alias", "current", nil, now); other {
		t.Fatal("multiple aliases for one NodeID were treated as different voters")
	}
}

func TestSingleBrokerAliasSurvivesRepeatedRosterSilence(t *testing.T) {
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
	build := func(generation uint64) *proto.ClusterRoster {
		roster, err := clusterroster.Build(seed, pub, generation,
			[]proto.RosterBroker{{NodeID: "solo", PublicHost: "203.0.113.55",
				NatsRoute: "nats://203.0.113.55:6222", Phase: proto.RosterPhaseVoter}},
			time.Now().UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
		if err != nil {
			t.Fatal(err)
		}
		return roster
	}
	initial, recovered := build(1), build(2)
	var registrations, refreshes, wrongServerNames atomic.Int32
	var answering atomic.Bool
	serverName := stub.ConnectedServerName()
	if _, err := stub.Subscribe(proto.SubjNodeRegister("lab", "identity-agent"), func(msg *nats.Msg) {
		var request proto.NodeRegisterReq
		if json.Unmarshal(msg.Data, &request) != nil {
			return
		}
		if request.ServerID != serverName {
			wrongServerNames.Add(1)
		}
		roster := initial
		if request.RosterRefreshOnly {
			refreshes.Add(1)
			if !answering.Load() {
				return
			}
			roster = recovered
		} else {
			registrations.Add(1)
		}
		payload, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: roster, ConnectedBrokerID: "solo"})
		_ = msg.Respond(payload)
	}); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "identity-agent", AccountPub: pub,
		HeartbeatInterval: time.Second, RegisterTimeout: 80 * time.Millisecond,
		RosterRefreshInterval: 40 * time.Millisecond, Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	a.rosterRefreshFailBackoff = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("agent did not stop after the silence test")
		}
	})
	if !testharness.WaitFor(t, 8*time.Second, 10*time.Millisecond, func() bool {
		return refreshes.Load() >= maxSilentRosterRefreshes+2
	}) {
		t.Fatalf("did not exercise repeated silence; refreshes=%d registrations=%d", refreshes.Load(), registrations.Load())
	}
	if registrations.Load() != 1 || a.rebuilding.Load() {
		t.Fatalf("a single broker alias caused a session rebuild: registrations=%d", registrations.Load())
	}
	answering.Store(true)
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
		return a.cachedRosterGen() == 2
	}) {
		t.Fatal("the original session could not refresh after the broker answered again")
	}
	if registrations.Load() != 1 {
		t.Fatal("recovery performed an unnecessary full registration")
	}
	if wrongServerNames.Load() != 0 {
		t.Fatal("full register or roster refresh omitted the current NATS server name")
	}
}
