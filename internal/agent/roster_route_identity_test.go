package agent

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// Real cache loading and roster refresh over embedded NATS; the register reply
// is a signed N-1 fixture. No broker membership or WAN behavior is simulated.
// origin: control-connection-recovery second internal review, cache trust boundary.
func TestRosterRouteIdentityRejectsInvalidBootCache(t *testing.T) {
	seed, pub := mustAccount(t)
	for _, kind := range []string{"expired", "expires-after-boot", "bad-signature", "wrong-account", "future-schema"} {
		t.Run(kind, func(t *testing.T) {
			serverURL := startNATS(t)
			nc, err := nats.Connect(serverURL)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			parsed, err := url.Parse(serverURL)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			old, err := clusterroster.Build(seed, pub, 1, []proto.RosterBroker{{
				NodeID: "untrusted-old-node", PublicHost: "old.invalid", NatsRoute: "nats://" + parsed.Host,
				Phase: proto.RosterPhaseVoter,
			}}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "expired":
				old, err = clusterroster.Build(seed, pub, 1, old.Brokers,
					now.Add(-2*time.Hour).Format(time.RFC3339Nano), now.Add(-time.Hour).Format(time.RFC3339Nano))
			case "bad-signature":
				old.Sig = "00"
			case "wrong-account":
				otherSeed, otherPub := mustAccount(t)
				old, err = clusterroster.Build(otherSeed, otherPub, 1, old.Brokers,
					now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			case "future-schema":
				old.SchemaVersion = proto.ClusterRosterSchemaVersion + 1
				var sig []byte
				sig, err = auth.SignWithSeed(seed, clusterroster.CanonicalRosterBytes(old))
				old.Sig = hex.EncodeToString(sig)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := clusterroster.VerifyAt(old, pub, now); err == nil && kind != "expires-after-boot" {
				t.Fatal("invalid cache fixture unexpectedly verified")
			}
			store := newStateStore(t.TempDir(), "lab")
			if err := store.SetRosterCache(&RosterCache{PinAccountPub: pub, Generation: 1, Roster: old}); err != nil {
				t.Fatal(err)
			}
			a, err := New(Config{NATSURL: serverURL, SID: "lab", NID: "cache-agent", AccountPub: pub,
				Now: func() time.Time { return now }, Logger: testharness.SilentLog()})
			if err != nil {
				t.Fatal(err)
			}
			a.stateStore = store
			a.loadRosterCacheAtBoot()
			if a.effectiveDialURLs() != serverURL && kind != "expires-after-boot" {
				t.Fatal("invalid cache already contaminated the initial dial pool")
			}
			if kind == "expires-after-boot" {
				now = now.Add(2 * time.Hour)
			}
			a.ncBox.Store(nc)
			connectedURL := nc.ConnectedUrl()
			a.connURLBox.Store(&connectedURL)
			fresh, err := clusterroster.Build(seed, pub, 2, []proto.RosterBroker{{
				NodeID: "actual-single-broker", PublicHost: "current.invalid", NatsRoute: "nats://current.invalid:6222",
				Phase: proto.RosterPhaseVoter,
			}}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: fresh})
			if err != nil {
				t.Fatal(err)
			}
			sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "cache-agent"), func(msg *nats.Msg) { _ = msg.Respond(body) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			if !a.refreshRosterOnce(context.Background(), nc, rosterWakeTimer) {
				t.Fatal("valid signed N-1 roster refresh failed")
			}
			if a.cachedRosterGen() != 2 {
				t.Fatal("valid fresh roster was not adopted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.setSessionCancel(cancel)
			if a.rebuildOnBrokerSilence(nc) || ctx.Err() != nil {
				t.Fatalf("unverified boot cache manufactured a previous ingress identity and tore down the sole broker: kind=%s", kind)
			}
		})
	}
}

// A verified N-1 host mapping is usable while fresh. Once the actual live route
// was identified, roster expiry cannot erase it: the same route may be retired
// while a newly signed roster names only the survivor.
func TestRosterRouteIdentityPreservesVerifiedIngress(t *testing.T) {
	seed, pub := mustAccount(t)
	for _, expiresAfterProof := range []bool{false, true} {
		name := "fresh-cache"
		if expiresAfterProof {
			name = "known-route-before-expiry"
		}
		t.Run(name, func(t *testing.T) {
			serverURL := startNATS(t)
			nc, err := nats.Connect(serverURL)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			now := time.Now().UTC()
			old, err := clusterroster.Build(seed, pub, 1, []proto.RosterBroker{{
				NodeID: "retired", PublicHost: "retired.invalid", NatsRoute: serverURL, Phase: proto.RosterPhaseVoter,
			}}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			store := newStateStore(t.TempDir(), "lab")
			if err := store.SetRosterCache(&RosterCache{PinAccountPub: pub, Generation: 1, Roster: old}); err != nil {
				t.Fatal(err)
			}
			a, err := New(Config{NATSURL: serverURL, SID: "lab", NID: "cache-agent", AccountPub: pub,
				Now: func() time.Time { return now }, Logger: testharness.SilentLog()})
			if err != nil {
				t.Fatal(err)
			}
			a.stateStore = store
			a.loadRosterCacheAtBoot()
			a.ncBox.Store(nc)
			connectedURL := nc.ConnectedUrl()
			a.connURLBox.Store(&connectedURL)
			if expiresAfterProof {
				rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL,
					proto.NodeRegisterResp{OK: true, Roster: old})
				now = now.Add(2 * time.Hour)
			}
			fresh, err := clusterroster.Build(seed, pub, 2, []proto.RosterBroker{{
				NodeID: "survivor", PublicHost: "survivor.invalid", NatsRoute: "nats://survivor.invalid:6222", Phase: proto.RosterPhaseVoter,
			}}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL,
				proto.NodeRegisterResp{OK: true, Roster: fresh})
			if !a.adoptRoster(fresh) {
				t.Fatal("fresh signed survivor roster was not adopted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.setSessionCancel(cancel)
			if !a.rebuildOnBrokerSilence(nc) || ctx.Err() == nil {
				t.Fatal("validly identified retired ingress could not escape to the sole survivor")
			}
		})
	}
}

// Hostnames are not broker identifiers: several members can advertise the same
// route hostname with different ports. An N-1 response cannot resolve that
// ambiguity, so retiring one member must not manufacture proof about the other.
func TestRosterRouteIdentityRejectsAmbiguousHost(t *testing.T) {
	for _, explicitID := range []string{"", "b-current"} {
		name := "legacy-ambiguous"
		if explicitID != "" {
			name = "explicit-identity"
		}
		t.Run(name, func(t *testing.T) {
			serverURL := startNATS(t)
			nc, err := nats.Connect(serverURL)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			seed, pub := mustAccount(t)
			now := time.Now().UTC()
			a, err := New(Config{NATSURL: serverURL, SID: "lab", NID: "cache-agent", AccountPub: pub,
				Now: func() time.Time { return now }, Logger: testharness.SilentLog()})
			if err != nil {
				t.Fatal(err)
			}
			a.loadRosterCacheAtBoot()
			parsed, err := url.Parse(serverURL)
			if err != nil {
				t.Fatal(err)
			}
			old, err := clusterroster.Build(seed, pub, 1, []proto.RosterBroker{
				{NodeID: "a-retired", PublicHost: "retired.invalid", NatsRoute: "nats://" + parsed.Hostname() + ":6222", Phase: proto.RosterPhaseVoter},
				{NodeID: "b-current", PublicHost: "current.invalid", NatsRoute: "nats://" + parsed.Hostname() + ":7222", Phase: proto.RosterPhaseVoter},
			}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			if !a.adoptRoster(old) {
				t.Fatal("valid initial roster was not accepted")
			}
			a.ncBox.Store(nc)
			connectedURL := nc.ConnectedUrl()
			a.connURLBox.Store(&connectedURL)
			rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL, proto.NodeRegisterResp{OK: true, Roster: old, ConnectedBrokerID: explicitID})
			fresh, err := clusterroster.Build(seed, pub, 2, []proto.RosterBroker{
				{NodeID: "b-current", PublicHost: "current.invalid", NatsRoute: "nats://new-route.invalid:7222", Phase: proto.RosterPhaseVoter},
			}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			rememberConnectedBroker(a, nc, nc.ConnectedServerName(), connectedURL, proto.NodeRegisterResp{OK: true, Roster: fresh})
			if !a.adoptRoster(fresh) {
				t.Fatal("valid current roster was not accepted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.setSessionCancel(cancel)
			if a.rebuildOnBrokerSilence(nc) || ctx.Err() != nil {
				t.Fatal("ambiguous shared hostname manufactured the retired node's identity and tore down the actual sole broker")
			}
		})
	}
}
