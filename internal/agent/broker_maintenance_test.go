package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type agentMaintenanceFixture struct {
	a      *Agent
	nc     *nats.Conn
	issuer []byte
	now    atomic.Int64
	req    proto.BrokerMaintenanceReq
}

func newAgentMaintenanceFixture(t *testing.T, single bool) *agentMaintenanceFixture {
	t.Helper()
	f := &agentMaintenanceFixture{}
	f.now.Store(int64(10 * time.Second))
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	agentPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: "maintenance-ingress",
		Nkeys: []*natsserver.NkeyUser{{Nkey: agentPub}}})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("fixture NATS server not ready")
	}
	url := server.ClientURL()
	nc, err := nats.Connect(url, nats.Nkey(agentPub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(seed, nonce) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	f.nc = nc
	f.issuer, err = auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(f.issuer)
	if err != nil {
		t.Fatal(err)
	}
	f.a, err = New(Config{NATSURL: url, SID: "lab", NID: "hold-agent", AccountPub: pub,
		Identity: &cli.Identity{Seed: seed, PublicKey: agentPub}, Logger: testharness.SilentLog(),
		RegisterTimeout: 25 * time.Millisecond, RosterRefreshInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	f.a.maintenanceNow = func() (int64, error) { return f.now.Load(), nil }
	f.a.loadRosterCacheAtBoot()
	f.a.ncBox.Store(nc)
	connectedURL := nc.ConnectedUrl()
	f.a.connURLBox.Store(&connectedURL)
	response := proto.NodeRegisterResp{OK: true, ConnectedBrokerID: "current", BrokerImageMaintenance: brokermaint.Capability}
	if single {
		response.ConnectedBrokerID = ""
	} else {
		roster, err := clusterroster.Build(f.issuer, pub, 1, []proto.RosterBroker{
			{NodeID: "current", PublicHost: "127.0.0.1", Phase: proto.RosterPhaseVoter},
			{NodeID: "peer", PublicHost: "203.0.113.55", Phase: proto.RosterPhaseVoter},
		}, time.Now().UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
		if err != nil {
			t.Fatal(err)
		}
		if !f.a.adoptRoster(roster) {
			t.Fatal("fixture signed roster rejected")
		}
		response.Roster = roster
	}
	rememberConnectedBroker(f.a, nc, nc.ConnectedServerName(), connectedURL, response)
	f.req = proto.BrokerMaintenanceReq{NID: nidOf(f.a), ConnectedBrokerID: response.ConnectedBrokerID, IngressServerID: "",
		Record: brokermaint.Record{CutID: "cut-1", Holder: "holder-1", Home: response.ConnectedBrokerID, InstanceID: "broker-instance", IssuerBootID: "issuer-boot",
			Mode: proto.BrokerImageModeQuorum, Generation: 1, Phase: brokermaint.MaintPrepared,
			IssuedBootNS: int64(1000 * time.Hour), ExpiresBootNS: int64(1000*time.Hour) + agentMaintenanceTTL,
			Term: 1, ConfigIndex: 1, VoterDigest: strings.Repeat("a", 64), BaselineDigest: strings.Repeat("b", 64), RequiredCapability: brokermaint.Capability}}
	if single {
		f.req.IngressServerID = nc.ConnectedServerId()
		f.req.Record.Term, f.req.Record.ConfigIndex, f.req.Record.VoterDigest = 0, 0, ""
		f.req.Record.Mode = proto.BrokerImageModeSingle
		f.req.Record.IngressServerID = nc.ConnectedServerId()
	}
	return f
}

// The protocol has one shared codec, with independent byte goldens in
// brokermaint. These fixtures exercise real cross-package signed messages.
func maintenanceTestBytes(t *testing.T, value any) []byte {
	t.Helper()
	var prefix string
	var body []byte
	var err error
	switch v := value.(type) {
	case brokermaint.Proof:
		v.Sig = ""
		body, err = json.Marshal(v)
		body = bytes.TrimSuffix(body, []byte(`,"sig":""}`))
		body = append(body, '}')
		prefix = "broker-maint-proof-v1\n"
	case proto.BrokerMaintenanceReq:
		v.Sig, v.Proof.Sig = "", ""
		body, err = json.Marshal(v)
		body = bytes.Replace(body, []byte(`,"sig":""},"nid"`), []byte(`},"nid"`), 1)
		body = bytes.TrimSuffix(body, []byte(`,"sig":""}`))
		body = append(body, '}')
		prefix = "broker-maint-request-v1\n"
	case proto.BrokerMaintenanceAck:
		v.Sig = ""
		body, err = json.Marshal(v)
		body = bytes.TrimSuffix(body, []byte(`,"sig":""}`))
		body = append(body, '}')
		prefix = "broker-maint-ack-v1\n"
	default:
		t.Fatal("unknown fixture signed value")
	}
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(prefix), body...)
}

func maintenanceProofTestDigest(t *testing.T, proof brokermaint.Proof) string {
	t.Helper()
	digest, err := brokermaint.ProofDigest(proof)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func signMaintenanceTestRequest(t *testing.T, f *agentMaintenanceFixture, req proto.BrokerMaintenanceReq) proto.BrokerMaintenanceReq {
	t.Helper()
	req.Sig = ""
	sig, err := auth.SignWithSeed(f.issuer, maintenanceTestBytes(t, req))
	if err != nil {
		t.Fatal(err)
	}
	req.Sig = base64.RawStdEncoding.EncodeToString(sig)
	return req
}

func maintenanceTestChallenge(t *testing.T, f *agentMaintenanceFixture) proto.BrokerMaintenanceAck {
	t.Helper()
	protectedBefore := agentMaintenanceProtects(f.a, f.nc)
	ack, err := acceptAgentMaintenance(f.a, f.nc, signMaintenanceTestRequest(t, f, f.req))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Accepted || ack.RemainingNS != 0 || ack.Nonce == "" || ack.RouteDigest == "" || agentMaintenanceProtects(f.a, f.nc) != protectedBefore {
		t.Fatal("challenge claimed an installed hold")
	}
	verifyMaintenanceTestAck(t, f, ack)
	return ack
}

func maintenanceTestProof(t *testing.T, f *agentMaintenanceFixture, challenge proto.BrokerMaintenanceAck, req proto.BrokerMaintenanceReq) proto.BrokerMaintenanceReq {
	t.Helper()
	req.RouteDigest = challenge.RouteDigest
	req.Proof = brokermaint.Proof{CutID: req.Record.CutID, Nonce: challenge.Nonce, IssuerBootID: req.Record.IssuerBootID,
		ResponseBootNS: req.Record.IssuedBootNS + int64(time.Second), ExpiresBootNS: req.Record.ExpiresBootNS,
		RecordDigest: func() string {
			d, err := brokermaint.RecordDigest(req.Record)
			if err != nil {
				t.Fatal(err)
			}
			return d
		}(), AppliedIndex: 1}
	sig, err := auth.SignWithSeed(f.issuer, maintenanceTestBytes(t, req.Proof))
	if err != nil {
		t.Fatal(err)
	}
	req.Proof.Sig = base64.RawStdEncoding.EncodeToString(sig)
	return signMaintenanceTestRequest(t, f, req)
}

func verifyMaintenanceTestAck(t *testing.T, f *agentMaintenanceFixture, ack proto.BrokerMaintenanceAck) {
	t.Helper()
	sig, err := base64.RawStdEncoding.DecodeString(ack.Sig)
	if err != nil {
		t.Fatal(err)
	}
	ack.Sig = ""
	if err := auth.VerifySignature(f.a.cfg.Identity.PublicKey, maintenanceTestBytes(t, ack), sig); err != nil {
		t.Fatal(err)
	}
}

func installMaintenanceTestHold(t *testing.T, f *agentMaintenanceFixture) proto.BrokerMaintenanceReq {
	t.Helper()
	challenge := maintenanceTestChallenge(t, f)
	req := maintenanceTestProof(t, f, challenge, f.req)
	ack, err := acceptAgentMaintenance(f.a, f.nc, req)
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Accepted || ack.RemainingNS != int64(7*time.Second) || !agentMaintenanceProtects(f.a, f.nc) {
		t.Fatal("accepted ACK does not describe the installed fixed-expiry hold")
	}
	if ack.CutID != req.Record.CutID || ack.Nonce != req.Proof.Nonce || ack.InstanceID != req.Record.InstanceID || ack.ProofDigest != maintenanceProofTestDigest(t, req.Proof) {
		t.Fatal("ACK identity does not bind the installed proof")
	}
	verifyMaintenanceTestAck(t, f, ack)
	return req
}

type maintenanceMiss struct {
	source string
	count  int
}
type maintenanceWakeLog struct {
	slog.Handler
	misses chan maintenanceMiss
}

func (h maintenanceWakeLog) Enabled(context.Context, slog.Level) bool { return true }
func (h maintenanceWakeLog) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "agent: roster refresh missed" {
		var event maintenanceMiss
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "source" {
				event.source = a.Value.String()
			}
			if a.Key == "consecutive" {
				event.count = int(a.Value.Int64())
			}
			return true
		})
		h.misses <- event
	}
	return nil
}

// origin: hot-update S6 plan §7.3 / T44.
func TestAgentSilenceTwoWakeMaintenance(t *testing.T) {
	t.Run("silent-two-immediate-topology-wake", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		installMaintenanceTestHold(t, f)
		ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated roster loop lifetime.
		f.a.setSessionCancel(cancel)
		misses := make(chan maintenanceMiss, 8)
		f.a.cfg.Logger = slog.New(maintenanceWakeLog{Handler: testharness.SilentLog().Handler(), misses: misses})
		done := make(chan struct{})
		go func() { defer close(done); f.a.rosterRefreshLoop(ctx, f.nc) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("roster loop did not join")
			}
		})
		wake := func(want int) {
			t.Helper()
			handleSysEvent(f.a, &nats.Msg{Data: []byte(`{"type":"nats_topology_changed"}`)}, cancel)
			select {
			case miss := <-misses:
				if miss.source != rosterWakeTopology || miss.count != want {
					t.Fatalf("wake=%+v, want topology failure %d", miss, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("topology wake did not run roster refresh")
			}
		}
		wake(1)
		wake(2)
		wake(3)
		if rosterSilenceRequiresRebuild(f.a, f.nc, 3) || ctx.Err() != nil || f.a.rebuilding.Load() || f.a.takeAvoidHost() != "" {
			t.Fatal("third immediate miss escaped while maintenance held the current route")
		}
		f.now.Store(int64(17 * time.Second))
		wake(4)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("fixed expiry did not restore normal silence escape")
		}
		if ctx.Err() == nil || !f.a.rebuilding.Load() || f.a.takeAvoidHost() == "" {
			t.Fatal("expiry failed to perform the ordinary rebuild")
		}
	})
	t.Run("proof-cannot-restart-local-expiry", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		req := installMaintenanceTestHold(t, f)
		f.now.Store(int64(15 * time.Second))
		challenge := maintenanceTestChallenge(t, f)
		replay := maintenanceTestProof(t, f, challenge, f.req)
		// A later response cannot recover lifetime already consumed by the first proof.
		replay.Proof.ResponseBootNS = replay.Record.IssuedBootNS
		replay.Proof.Sig = ""
		sig, err := auth.SignWithSeed(f.issuer, maintenanceTestBytes(t, replay.Proof))
		if err != nil {
			t.Fatal(err)
		}
		replay.Proof.Sig = base64.RawStdEncoding.EncodeToString(sig)
		replay = signMaintenanceTestRequest(t, f, replay)
		ack, err := acceptAgentMaintenance(f.a, f.nc, replay)
		if err != nil {
			t.Fatal(err)
		}
		if ack.RemainingNS != int64(2*time.Second) || req.Proof.Nonce != ack.Nonce {
			t.Fatal("same-cut reproof restarted the lifetime or nonce")
		}
		f.now.Store(int64(17 * time.Second))
		if _, err := acceptAgentMaintenance(f.a, f.nc, replay); err == nil || agentMaintenanceProtects(f.a, f.nc) {
			t.Fatal("expired proof renewed maintenance")
		}
	})
	t.Run("delayed-first-proof-consumes-queue-time", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		challenge := maintenanceTestChallenge(t, f)
		f.now.Store(int64(18 * time.Second))
		if _, err := acceptAgentMaintenance(f.a, f.nc, maintenanceTestProof(t, f, challenge, f.req)); err == nil || agentMaintenanceProtects(f.a, f.nc) {
			t.Fatal("late first proof received a fresh lifetime")
		}
	})
}

// origin: hot-update S6 plan §7.1 / §7.3 / T44.
func TestBrokerAgentMaintenanceIdentityAndRevoke(t *testing.T) {
	for _, mutate := range []struct {
		name string
		fn   func(*proto.BrokerMaintenanceReq)
	}{
		{"wrong-nid", func(r *proto.BrokerMaintenanceReq) { r.NID = "other" }},
		{"wrong-ingress", func(r *proto.BrokerMaintenanceReq) { r.IngressServerID = "other" }},
		{"wrong-home", func(r *proto.BrokerMaintenanceReq) { r.Record.Home = "peer" }},
		{"wrong-instance", func(r *proto.BrokerMaintenanceReq) { r.Record.InstanceID = "successor" }},
		{"wrong-boot", func(r *proto.BrokerMaintenanceReq) { r.Proof.IssuerBootID = "other" }},
		{"wrong-nonce", func(r *proto.BrokerMaintenanceReq) { r.Proof.Nonce = "other" }},
		{"wrong-route", func(r *proto.BrokerMaintenanceReq) { r.RouteDigest = "other" }},
		{"wrong-digest", func(r *proto.BrokerMaintenanceReq) { r.Proof.RecordDigest = "other" }},
		{"wrong-proof-cut", func(r *proto.BrokerMaintenanceReq) { r.Proof.CutID = "other" }},
		{"wrong-proof-expiry", func(r *proto.BrokerMaintenanceReq) { r.Proof.ExpiresBootNS++ }},
		{"proof-before-issue", func(r *proto.BrokerMaintenanceReq) { r.Proof.ResponseBootNS = r.Record.IssuedBootNS - 1 }},
		{"proof-after-expiry", func(r *proto.BrokerMaintenanceReq) { r.Proof.ResponseBootNS = r.Record.ExpiresBootNS }},
		{"missing-applied-cursor", func(r *proto.BrokerMaintenanceReq) { r.Proof.AppliedIndex = 0 }},
		{"extended-expiry", func(r *proto.BrokerMaintenanceReq) { r.Record.ExpiresBootNS++ }},
		{"bad-signature", func(r *proto.BrokerMaintenanceReq) { r.Proof.Sig = "invalid" }},
		{"unknown-capability", func(r *proto.BrokerMaintenanceReq) { r.Record.RequiredCapability = "future" }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			f := newAgentMaintenanceFixture(t, false)
			challenge := maintenanceTestChallenge(t, f)
			req := maintenanceTestProof(t, f, challenge, f.req)
			mutate.fn(&req)
			if mutate.name == "wrong-instance" {
				req.Proof.RecordDigest = agentMaintenanceDigest(req.Record)
			}
			if mutate.name != "bad-signature" {
				req.Proof.Sig = ""
				sig, err := auth.SignWithSeed(f.issuer, maintenanceTestBytes(t, req.Proof))
				if err != nil {
					t.Fatal(err)
				}
				req.Proof.Sig = base64.RawStdEncoding.EncodeToString(sig)
			}
			req = signMaintenanceTestRequest(t, f, req)
			if _, err := acceptAgentMaintenance(f.a, f.nc, req); err == nil || agentMaintenanceProtects(f.a, f.nc) {
				t.Fatal("mismatched signed proof installed a hold")
			}
		})
	}
	for _, verb := range []string{"home", "proxy-keys", "expose-rm", "session_destroyed", "agent_evicted", "route-change", "reconnect", "capability-missing", "ended"} {
		t.Run(verb, func(t *testing.T) {
			f := newAgentMaintenanceFixture(t, false)
			req := installMaintenanceTestHold(t, f)
			switch verb {
			case "session_destroyed", "agent_evicted":
				body, _ := json.Marshal(map[string]string{"type": verb, "sid": "lab", "nid": "hold-agent"})
				handleSysEvent(f.a, &nats.Msg{Data: body}, func() {})
			case "route-change":
				f.a.ncBox.Store(nil)
			case "reconnect":
				f.a.ptyHealthGeneration.Add(1)
			case "capability-missing":
				rememberConnectedBroker(f.a, f.nc, f.nc.ConnectedServerName(), f.nc.ConnectedUrl(), proto.NodeRegisterResp{OK: true, ConnectedBrokerID: "current", Roster: f.a.cachedRoster})
			case "ended":
				req.Record.Phase = brokermaint.MaintEnded
				req = maintenanceTestProof(t, f, proto.BrokerMaintenanceAck{Nonce: req.Proof.Nonce, RouteDigest: req.RouteDigest}, req)
				if _, err := acceptAgentMaintenance(f.a, f.nc, req); err == nil {
					t.Fatal("ended record accepted as a hold")
				}
			default:
				switch verb {
				case "proxy-keys":
					f.a.applyProxyDirective(f.nc, &proto.ProxyDirective{Enabled: false})
				case "expose-rm", "home":
					f.a.stateStore = newStateStore(t.TempDir(), "lab")
					if err := f.a.stateStore.AddPort(PortToken{Name: "svc", Port: 14001, Epoch: 1, HomeBrokerAddr: "old"}); err != nil {
						t.Fatal(err)
					}
					if verb == "expose-rm" {
						data, _ := json.Marshal(proto.ExposeRmForwardedReq{Name: "svc", Port: 14001})
						f.a.handleExposeRmForwarded(f.nc, &nats.Msg{Data: data})
					} else {
						invalidateMaintenanceForHome(f.a, proto.HomeDirective{Name: "svc", PublicPort: 14001, Epoch: 2, BrokerAddr: "new"})
					}
				}
			}
			if agentMaintenanceProtects(f.a, f.nc) {
				t.Fatal("maintenance took precedence over authority invalidation")
			}
			if _, err := acceptAgentMaintenance(f.a, f.nc, req); err == nil {
				t.Fatal("old proof restored invalidated authority")
			}
		})
	}
	t.Run("same-route-refresh", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		installMaintenanceTestHold(t, f)
		rememberConnectedBroker(f.a, f.nc, f.nc.ConnectedServerName(), f.nc.ConnectedUrl(), proto.NodeRegisterResp{OK: true, ConnectedBrokerID: "current", Roster: f.a.cachedRoster, BrokerImageMaintenance: brokermaint.Capability})
		if !agentMaintenanceProtects(f.a, f.nc) {
			t.Fatal("same route refresh discarded a valid hold")
		}
	})
	t.Run("single-real-server-id", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, true)
		installMaintenanceTestHold(t, f)
		if f.a.rosterRoute.actualBrokerID != "" || f.a.cachedRoster != nil {
			t.Fatal("single hold invented cluster identity")
		}
	})
}

// origin: hot-update S6 plan §9.1 / T44.
func TestBrokerAgentMaintenanceCapabilityAndDispatch(t *testing.T) {
	f := newAgentMaintenanceFixture(t, true)
	// Registration capture verifies the actual production request; the responder
	// represents only the existing wire contract, not a maintenance broker.
	requests := make(chan proto.NodeRegisterReq, 2)
	sub, err := f.nc.Subscribe(proto.SubjNodeRegister("lab", "hold-agent"), func(msg *nats.Msg) {
		var req proto.NodeRegisterReq
		if json.Unmarshal(msg.Data, &req) != nil {
			return
		}
		requests <- req
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, BrokerImageMaintenance: brokermaint.Capability})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: hermetic control subscription lifetime.
	defer cancel()
	f.a.setRunCtx(ctx)
	if _, err := f.a.register(ctx, f.nc); err != nil {
		t.Fatal(err)
	}
	if req := <-requests; req.BrokerImageMaintenance != brokermaint.Capability {
		t.Fatal("full register omitted capability")
	}
	if !f.a.refreshRosterOnce(ctx, f.nc, rosterWakeTopology) {
		t.Fatal("fixture roster refresh failed")
	}
	if req := <-requests; req.BrokerImageMaintenance != brokermaint.Capability || !req.RosterRefreshOnly {
		t.Fatal("refresh omitted capability")
	}
	subFwd, err := f.nc.Subscribe(proto.SubjCmdForwarded("lab", "hold-agent", proto.BrokerMaintenanceVerb), func(msg *nats.Msg) {
		if !dispatchAgentMaintenance(f.a, f.nc, msg) {
			t.Error("maintenance was not dispatched")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subFwd.Unsubscribe() })
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	send := func(req proto.BrokerMaintenanceReq) proto.BrokerMaintenanceAck {
		t.Helper()
		body, _ := json.Marshal(req)
		msg, err := f.nc.Request(proto.SubjCmdForwarded("lab", "hold-agent", proto.BrokerMaintenanceVerb), body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var ack proto.BrokerMaintenanceAck
		if err := json.Unmarshal(msg.Data, &ack); err != nil {
			t.Fatal(err)
		}
		verifyMaintenanceTestAck(t, f, ack)
		return ack
	}
	challenge := send(signMaintenanceTestRequest(t, f, f.req))
	if challenge.Accepted || agentMaintenanceProtects(f.a, f.nc) {
		t.Fatal("forwarded challenge acknowledged installation")
	}
	ack := send(maintenanceTestProof(t, f, challenge, f.req))
	if !ack.Accepted || !agentMaintenanceProtects(f.a, f.nc) {
		t.Fatal("forwarded ACK preceded hold installation")
	}
}

// origin: hot-update S6 plan §11 / §12.1 P17.
func TestBrokerAgentHoldLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("hold", func(t *testing.T) { exerciseAgentMaintenanceHold(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "agent maintenance hold x5", beforeG)
	testharness.AssertNoFDLeak(t, "agent maintenance hold x5", beforeFD, 0)
}

func exerciseAgentMaintenanceHold(t *testing.T) {
	t.Helper()
	f := newAgentMaintenanceFixture(t, false)
	installMaintenanceTestHold(t, f)
	f.now.Store(int64(17 * time.Second))
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: one exercise's session cancellation receipt.
	defer cancel()
	f.a.setSessionCancel(cancel)
	if !rosterSilenceRequiresRebuild(f.a, f.nc, 3) || ctx.Err() == nil {
		t.Fatal("expired hold did not release the ordinary session")
	}
}

// origin: hot-update S6 plan §7.1 / §7.3 / T44.
func TestBrokerAgentMaintenanceRejectsUnsupportedAndLateAck(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*agentMaintenanceFixture)
	}{
		{"missing-identity", func(f *agentMaintenanceFixture) { f.a.cfg.Identity = nil }},
		{"unsupported-clock", func(f *agentMaintenanceFixture) { f.now.Store(0) }},
		{"clock-overflow", func(f *agentMaintenanceFixture) { f.now.Store(math.MaxInt64) }},
		{"unknown-mode", func(f *agentMaintenanceFixture) { f.req.Record.Mode = "future" }},
		{"unknown-phase", func(f *agentMaintenanceFixture) { f.req.Record.Phase = 0 }},
		{"missing-generation", func(f *agentMaintenanceFixture) { f.req.Record.Generation = 0 }},
		{"missing-instance", func(f *agentMaintenanceFixture) { f.req.Record.InstanceID = "" }},
		{"missing-boot", func(f *agentMaintenanceFixture) { f.req.Record.IssuerBootID = "" }},
		{"missing-holder", func(f *agentMaintenanceFixture) { f.req.Record.Holder = "" }},
		{"missing-baseline", func(f *agentMaintenanceFixture) { f.req.Record.BaselineDigest = "" }},
		{"invalid-identifier", func(f *agentMaintenanceFixture) { f.req.Record.Holder = "bad\nholder" }},
		{"ttl-over-eight-seconds", func(f *agentMaintenanceFixture) { f.req.Record.ExpiresBootNS++ }},
		{"ttl-reversed", func(f *agentMaintenanceFixture) { f.req.Record.ExpiresBootNS = f.req.Record.IssuedBootNS - 1 }},
		{"missing-term", func(f *agentMaintenanceFixture) { f.req.Record.Term = 0 }},
		{"missing-config", func(f *agentMaintenanceFixture) { f.req.Record.ConfigIndex = 0 }},
		{"missing-voters", func(f *agentMaintenanceFixture) { f.req.Record.VoterDigest = "" }},
		{"unsupported-observer", func(f *agentMaintenanceFixture) {
			f.a.rosterMu.Lock()
			f.a.rosterRoute.maintenanceCapability = "future"
			f.a.rosterMu.Unlock()
		}},
		{"fallback-is-not-actual-identity", func(f *agentMaintenanceFixture) {
			f.a.rosterMu.Lock()
			f.a.rosterRoute.actualBrokerID = ""
			f.a.rosterMu.Unlock()
			f.req.ConnectedBrokerID = ""
		}},
		{"missing-route", func(f *agentMaintenanceFixture) { f.a.rosterMu.Lock(); f.a.rosterRoute = nil; f.a.rosterMu.Unlock() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgentMaintenanceFixture(t, false)
			tc.mutate(f)
			if _, err := acceptAgentMaintenance(f.a, f.nc, signMaintenanceTestRequest(t, f, f.req)); err == nil || agentMaintenanceProtects(f.a, f.nc) {
				t.Fatal("unsupported observer/record obtained a hold")
			}
		})
	}
	t.Run("single-cannot-use-hostname", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, true)
		f.req.Record.IngressServerID = "single.alias"
		if _, err := acceptAgentMaintenance(f.a, f.nc, signMaintenanceTestRequest(t, f, f.req)); err == nil {
			t.Fatal("single trusted server_name instead of actual server ID")
		}
	})
	t.Run("unsigned-request", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		if _, err := acceptAgentMaintenance(f.a, f.nc, f.req); err == nil {
			t.Fatal("unsigned command accepted")
		}
	})
	t.Run("signed-fields-tampered", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		req := signMaintenanceTestRequest(t, f, f.req)
		req.Record.Holder = "tampered"
		if _, err := acceptAgentMaintenance(f.a, f.nc, req); err == nil {
			t.Fatal("unsigned record change accepted")
		}
	})
	t.Run("proof-without-local-challenge", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		req := maintenanceTestProof(t, f, proto.BrokerMaintenanceAck{Nonce: "foreign", RouteDigest: "foreign"}, f.req)
		if _, err := acceptAgentMaintenance(f.a, f.nc, req); err == nil {
			t.Fatal("proof installed without a local t0")
		}
	})
	t.Run("expires-during-install", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		challenge := maintenanceTestChallenge(t, f)
		var calls atomic.Int32
		f.a.maintenanceNow = func() (int64, error) {
			if calls.Add(1) == 1 {
				return int64(16 * time.Second), nil
			}
			return int64(17 * time.Second), nil
		}
		if _, err := acceptAgentMaintenance(f.a, f.nc, maintenanceTestProof(t, f, challenge, f.req)); err == nil || f.a.brokerMaintenance.installed {
			t.Fatal("ACK claimed lifetime exhausted during installation")
		}
	})
	t.Run("active-phase-cannot-regress", func(t *testing.T) {
		f := newAgentMaintenanceFixture(t, false)
		req := installMaintenanceTestHold(t, f)
		req.Record.Phase = brokermaint.MaintActive
		active := maintenanceTestProof(t, f, proto.BrokerMaintenanceAck{Nonce: req.Proof.Nonce, RouteDigest: req.RouteDigest}, req)
		if _, err := acceptAgentMaintenance(f.a, f.nc, active); err != nil {
			t.Fatal(err)
		}
		prepared := maintenanceTestProof(t, f, proto.BrokerMaintenanceAck{Nonce: req.Proof.Nonce, RouteDigest: req.RouteDigest}, f.req)
		if _, err := acceptAgentMaintenance(f.a, f.nc, prepared); err == nil {
			t.Fatal("active hold regressed to prepared")
		}
	})
}

// origin: hot-update S6 plan §7.3 / §9.1 / T44.
func TestBrokerAgentMaintenanceSessionWiring(t *testing.T) {
	f := newAgentMaintenanceFixture(t, true)
	sub, err := f.nc.Subscribe(proto.SubjNodeRegister("lab", "hold-agent"), func(msg *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, BrokerImageMaintenance: brokermaint.Capability})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: actual agent Run lifetime.
	done := make(chan error, 1)
	go func() { done <- f.a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("actual agent Run did not join")
		}
	})
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		f.a.rosterMu.Lock()
		defer f.a.rosterMu.Unlock()
		route := f.a.rosterRoute
		return route != nil && route.nc != f.nc && route.maintenanceCapability == brokermaint.Capability
	}) {
		t.Fatal("actual agent did not register its maintenance route")
	}
	send := func(req proto.BrokerMaintenanceReq) proto.BrokerMaintenanceAck {
		t.Helper()
		body, _ := json.Marshal(req)
		msg, err := f.nc.Request(proto.SubjCmdForwarded("lab", "hold-agent", proto.BrokerMaintenanceVerb), body, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var ack proto.BrokerMaintenanceAck
		if err := json.Unmarshal(msg.Data, &ack); err != nil {
			t.Fatal(err)
		}
		verifyMaintenanceTestAck(t, f, ack)
		return ack
	}
	challenge := send(signMaintenanceTestRequest(t, f, f.req))
	if challenge.Accepted {
		t.Fatal("actual subscription acknowledged only receiving the challenge")
	}
	ack := send(maintenanceTestProof(t, f, challenge, f.req))
	if !ack.Accepted || !agentMaintenanceProtects(f.a, f.a.ncBox.Load()) {
		t.Fatal("actual session subscription did not install maintenance")
	}
}
