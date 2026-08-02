package agent

import (
	"context"
	"encoding/json"
	neturl "net/url"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// roster_proactive_rehome_test.go — the WIRING from a roster refresh to the proactive re-home.
//
// origin: post-release technical-debt cleanup; drill 41's two recorded gaps
//
// WHY THIS EXISTS, AND WHY THE EXISTING TEST WAS NOT ENOUGH
// ---------------------------------------------------------
// TestRosterReconnectOnlyLeavesRetiredCurrentBroker pins the POLICY — the pure predicate
// rosterRequiresReconnect over hand-built rosters. Nothing pinned the WIRING: that refreshRosterOnce
// actually feeds it the previous roster, the freshly-signed one and the live connection's URL, and acts
// on a true answer.
//
// That gap had a cost. simcluster drill 41 recorded TWO not_covered gaps claiming the agent "does not
// physically leave the retired-but-still-meshed broker in-window", blaming a suspected host/IP match
// failure in this exact path, and flagged its own reasoning as "unconfirmed from a torn-down run". The
// claim was then repeated as established fact through three later rounds of review. A live capture
// showed the opposite: the path fires, the connected URL is a hostname, and the agent re-registers on a
// remaining voter 62 MILLISECONDS after the decision.
//
// So the mechanism was fine and the coverage was not: a working fast path looked broken, and a REAL
// regression in it would have been invisible at every tier — the unit tests only saw the predicate, and
// the drill had already written the failure off as expected. This test closes the tier that was missing.
func TestRosterRefreshTriggersProactiveRehomeOffALeavingBroker(t *testing.T) {
	url := startNATS(t)

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	// HOW THE FIXTURE NAMES THE CONNECTED BROKER, AND WHY IT MATTERS
	// ---------------------------------------------------------------
	// The policy has two arms: the connected broker is marked LEAVING, or it was REMOVED from the signed
	// roster. Both need the connected host to be identifiable in the roster, and a hermetic test connects
	// to an embedded server on 127.0.0.1.
	//
	// origin: p-b2 internal review M5 + M6. The first version of this fixture put that loopback address in
	// the entry's PublicHost, and BOTH of the things it then said were wrong. rosterRequiresReconnect
	// filters entries by IsUndialableHost(b.PublicHost) (roster.go:420) before it looks at anything else,
	// so a loopback PublicHost was dropped from the current-roster scan on every row: `currentPresent`
	// could never become true, and the removal arm fired whatever the served roster contained. The
	// positive row was OVER-DETERMINED — a byte-identical roster passed it too. External review F3 later
	// fixed that production asymmetry by identifying the current broker before filtering OTHER,
	// undialable destinations. The comment that stood here also concluded from the same mistake that a
	// hermetic test "cannot reach the LEAVING arm", which is FALSE: the match
	// (rosterBrokerMatchesHost) accepts PublicHost OR the NatsRoute hostname.
	//
	// So the entry carries a DIALABLE PublicHost and the loopback in its NatsRoute. It survives the
	// filter, it still matches the connection, both arms are reachable, and the unchanged-roster row can
	// finally be a real negative control rather than a row that would have failed.
	connectedHost := connectedHostOf(t, url)
	self := proto.RosterBroker{NodeID: "self", PublicHost: "self.example",
		NatsRoute: "nats://" + connectedHost + ":6222", Phase: proto.RosterPhaseVoter}
	selfRetiring := self
	selfRetiring.Phase = proto.RosterPhaseRetiring
	peer := proto.RosterBroker{NodeID: "peer", PublicHost: "peer.example",
		NatsRoute: "nats://peer.example:6222", Phase: proto.RosterPhaseVoter}

	knownBefore := []proto.RosterBroker{self, peer}
	removed := []proto.RosterBroker{peer}
	leaving := []proto.RosterBroker{selfRetiring, peer}

	cases := []struct {
		name        string
		prev        []proto.RosterBroker
		served      []proto.RosterBroker
		gen         uint64
		wantRebuild bool
		why         string
	}{
		{
			name: "connected broker REMOVED from the signed roster",
			prev: knownBefore, served: removed, gen: 6, wantRebuild: true,
			why: "the wiring the drill-41 gaps claimed did not work: a refresh that learns its own broker " +
				"is gone must hand off to requestRosterReconnect, not merely update the next dial pool",
		},
		{
			name: "connected broker marked RETIRING in the signed roster",
			prev: knownBefore, served: leaving, gen: 6, wantRebuild: true,
			why: "this is the arm a real `cluster shrink` takes, and until M6 it had no wiring coverage at " +
				"any tier — a short-circuit added before the leaving branch would have left the removal " +
				"arm, make test and make e2e-parallel all green",
		},
		{
			name: "roster unchanged, connected broker still a voter",
			prev: knownBefore, served: knownBefore, gen: 6, wantRebuild: false,
			why: "the negative control that makes the two positive rows mean anything: if this fires, the " +
				"agent rebuilds its session on every roster tick forever, and both rows above would pass " +
				"for that reason instead of for theirs",
		},
		{
			name: "refresh about brokers this agent is not on",
			prev: removed, served: removed, gen: 6, wantRebuild: false,
			why: "the previous-roster fence exists so an unrecognised connection alias does not look like " +
				"a removal on EVERY refresh; with the fence empty no arm may fire, or a healthy agent " +
				"rebuilds its session forever",
		},
		{
			// origin: p-b2 internal review m1. `accepted &&` on roster.go:398 had no assertion anywhere:
			// `_ = accepted` left the whole package green.
			name: "REJECTED roster (generation rollback) that would otherwise say re-home",
			prev: knownBefore, served: removed, gen: 2, wantRebuild: false,
			why: "a lagging follower answering with a pre-removal generation is rejected by the monotonic " +
				"generation rule, and a rejected roster must not reach the policy at all. Without the " +
				"`accepted &&` fence it does: the agent tears down its session, re-homes, gets the same " +
				"stale answer on the next refresh, and repeats — once per tick, forever",
		},
	}

	const seedGen = 5

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newRosterRehomeAgent(t, accountPub)
			// origin: internal review CT-2 (S5) — publish a session cancel so this fixture
			// represents a LIVE session. rebuildOntoVoter now rolls the rebuild flags back when
			// there is neither a finalizer nor a cancel (nothing to tear down: latching them
			// would disarm the successor session). Without this line the fixture would exercise
			// that rollback instead of the wiring this test is about.
			a.setSessionCancel(func() {})
			seedRoster := mustBuildRoster(t, seed, accountPub, seedGen, tc.prev)
			if !a.adoptRoster(seedRoster) {
				t.Fatal("fixture: the agent must adopt the initial signed roster")
			}

			served := mustBuildRoster(t, seed, accountPub, tc.gen, tc.served)
			nc, stop := serveRosterRefresh(t, url, a, served)
			defer stop()

			if !a.refreshRosterOnce(context.Background(), nc) {
				t.Fatal("refreshRosterOnce reported failure against a responding broker")
			}

			got := a.rebuildRequested.Load()
			if got != tc.wantRebuild {
				t.Errorf("rebuildRequested = %v, want %v.\nwhy it matters: %s", got, tc.wantRebuild, tc.why)
			}
		})
	}
}

// TestProactiveRehomeIsBlindToAConnectedHostTheRosterDoesNotName is the hole the drill SUSPECTED and
// that this path really does have — just not the one that bit it.
//
// rosterRequiresReconnect identifies "the broker I am on" by STRING-matching the connected hostname
// against each roster entry's PublicHost or NatsRoute host. Any connected host the roster does not spell
// the same way is invisible: `currentPresent` stays false and `knownBefore` is false, so BOTH arms go
// quiet and the agent sits on a broker the signed roster says is leaving.
//
// origin: p-b2 internal review m10. The first version of this test called that "IP blindness" and used
// only an IP address, which named the least reachable instance of the defect and told the next reader
// that only the IP form needed fixing. The defect is string identity, and the rows below say so: an IP
// (nats.go reconnected onto a DISCOVERED server, whose advertised URL is commonly an IP) and an ALIAS
// (a vanity or CNAME name in cfg.NATSURL, or a broker_url that differs from the roster's public_host)
// fail identically. In the shipped topology the alias form is the more likely of the two: agents dial a
// configured URL, and PublicHost comes from a separate config field on the broker.
//
// These state the current behaviour exactly rather than asserting a fix, because closing it needs an
// identity the roster does not carry today (the broker's nats server_name — a signed-wire addition).
// Recording it as an executable statement is what stops it from being re-discovered as a mystery: the
// drill spent three review rounds blaming this for a failure it did not cause.
func TestProactiveRehomeIsBlindToAConnectedHostTheRosterDoesNotName(t *testing.T) {
	prev := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "self", PublicHost: "self.example", NatsRoute: "nats://self.example:6222", Phase: proto.RosterPhaseVoter},
		{NodeID: "peer", PublicHost: "peer.example", NatsRoute: "nats://peer.example:6222", Phase: proto.RosterPhaseVoter},
	}}
	cur := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "self", PublicHost: "self.example", NatsRoute: "nats://self.example:6222", Phase: proto.RosterPhaseRetiring},
		{NodeID: "peer", PublicHost: "peer.example", NatsRoute: "nats://peer.example:6222", Phase: proto.RosterPhaseVoter},
	}}

	if !rosterRequiresReconnect(prev, cur, "nats://self.example:4222") {
		t.Fatal("premise: a connected URL SPELLED THE WAY THE ROSTER SPELLS IT must re-home off a retiring " +
			"broker (this is what a live drill capture observed doing exactly that)")
	}
	for _, tc := range []struct{ name, connected string }{
		{"IP form (nats.go reconnected onto a discovered server)", "nats://10.0.0.7:4222"},
		{"alias form (vanity/CNAME name that is not the roster's public_host)", "nats://broker.vanity.example:4222"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if rosterRequiresReconnect(prev, cur, tc.connected) {
				t.Fatal("this test records a KNOWN limitation and it has apparently been fixed — good. " +
					"Replace it with an assertion that a connected host the roster does not name DOES " +
					"re-home, and delete this note.")
			}
		})
	}
	t.Log("recorded limitation: the connected broker is identified by string identity, so any connected " +
		"host the signed roster does not spell identically — an IP, a vanity name, a CNAME — leaves the " +
		"proactive re-home quiet. Closing it needs the broker's nats server_name in the signed roster, " +
		"i.e. a wire addition and its own increment.")
}

// TestUndialableCurrentBrokerIdentityPrecedesDestinationFiltering pins the distinction between
// identifying the current broker and deciding which other brokers are usable destinations.
//
// origin: batch B2 debt external review F3. The current-roster scan used to discard loopback entries
// before checking whether one named the live connection, while the previous-roster fence did not. A
// byte-identical roster therefore looked like a removal and rebuilt on every refresh. The filter belongs
// only on OTHER brokers: loopback is valid identity for a cfg.NATSURL floor, but not a failover target.
func TestUndialableCurrentBrokerIdentityPrecedesDestinationFiltering(t *testing.T) {
	prev := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "self", PublicHost: "127.0.0.1", NatsRoute: "nats://127.0.0.1:6222", Phase: proto.RosterPhaseVoter},
		{NodeID: "peer", PublicHost: "peer.example", NatsRoute: "nats://peer.example:6222", Phase: proto.RosterPhaseVoter},
	}}
	unchanged := &proto.ClusterRoster{Brokers: append([]proto.RosterBroker(nil), prev.Brokers...)}
	if rosterRequiresReconnect(prev, unchanged, "nats://127.0.0.1:4222") {
		t.Fatal("a byte-identical loopback roster looked like a removal; identity must be checked before " +
			"undialable hosts are filtered as failover destinations")
	}

	retiring := &proto.ClusterRoster{Brokers: append([]proto.RosterBroker(nil), prev.Brokers...)}
	retiring.Brokers[0].Phase = proto.RosterPhaseRetiring
	if !rosterRequiresReconnect(prev, retiring, "nats://127.0.0.1:4222") {
		t.Fatal("checking loopback identity must not make it a failover destination or hide its RETIRING " +
			"phase; a dialable peer exists, so the current broker should re-home")
	}
}

// newRosterRehomeAgent builds the minimum Agent the roster path needs: an account pin so the signed
// roster verifies, and the config the refresh round trip reads.
//
// cfg.RegisterTimeout is the load-bearing field — refreshRosterOnce bounds its request with it, and a
// zero value makes the round trip expire before the fixture broker can answer. cfg.NATSURL is a floor
// for the dial pool and is deliberately a name the roster does NOT carry, so it cannot accidentally
// satisfy the host match under test (see the M5/M6 note above).
//
// origin: p-b2 internal review n7 — this used to also seed a.rosterRefreshNow, described as something
// "the rebuild plumbing touches". Nothing on this path reads it; it was a dead assignment that made the
// fixture look like it was modelling more state than it was.
func newRosterRehomeAgent(t *testing.T, accountPub string) *Agent {
	t.Helper()
	a := &Agent{}
	a.cfg.SID, a.cfg.NID = "lab", "agt1"
	a.cfg.Logger = d6Logger()
	a.cfg.NATSURL = "nats://self.example:4222"
	a.cfg.RegisterTimeout = 3 * time.Second
	a.pinAccount = accountPub
	return a
}

// connectedHostOf returns the hostname the agent's connection will report, so the fixture's roster can
// name the SAME host the policy will read out of nc.ConnectedUrl(). Hard-coding "127.0.0.1" would work
// today and break the moment the harness binds elsewhere.
func connectedHostOf(t *testing.T, natsURL string) string {
	t.Helper()
	u, err := neturl.Parse(natsURL)
	if err != nil || u.Hostname() == "" {
		t.Fatalf("cannot read the host out of the test NATS URL %q: %v", natsURL, err)
	}
	return u.Hostname()
}

func mustBuildRoster(t *testing.T, seed []byte, accountPub string, gen uint64, brokers []proto.RosterBroker) *proto.ClusterRoster {
	t.Helper()
	now := time.Now().UTC()
	r, err := clusterroster.Build(seed, accountPub, gen, brokers,
		now.Add(-time.Minute).Format(time.RFC3339), now.Add(time.Hour).Format(time.RFC3339))
	if err != nil {
		t.Fatalf("build signed roster: %v", err)
	}
	return r
}

// serveRosterRefresh stands up a fake broker that answers the agent's roster-only register with the
// supplied signed roster, and returns a connection whose ConnectedUrl() the policy will read.
func serveRosterRefresh(t *testing.T, url string, a *Agent, roster *proto.ClusterRoster) (*nats.Conn, func()) {
	t.Helper()
	srv, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := srv.Subscribe(proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID), func(msg *nats.Msg) {
		payload, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: roster})
		_ = msg.Respond(payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Flush(); err != nil {
		t.Fatal(err)
	}

	// The AGENT side of the connection. Its ConnectedUrl() is the URL it dialed, which is what the
	// policy compares against the roster — the same shape a live agent has.
	cli, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	return cli, func() {
		_ = sub.Unsubscribe()
		srv.Close()
		cli.Close()
	}
}
