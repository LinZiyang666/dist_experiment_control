// These tests use embedded pinned NATS and the real auth-callout Handler.
// Host census, configuration-fence and physical Caddy/WSS evidence are explicit
// models; adjacent stock binaries, multi-host kernel observations and production
// broker wiring remain P02/P14/P17/P18/P22/P28 integration obligations.
package infrareadiness

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func readinessDeadline(t *testing.T) infracontract.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func modelProcess(id int32) infracontract.Process {
	return infracontract.Process{BootID: fmt.Sprint("boot-", id), PID: id, StartTicks: uint64(id),
		InvocationID: fmt.Sprint("invocation-", id), ExeDevice: 1, ExeInode: uint64(id),
		ExeSHA256: infracontract.Digest{byte(id)}, Cgroup: fmt.Sprint("/unit-", id)}
}

func topologyModel() (Topology, Snapshot) {
	topology := Topology{Revision: 1, Complete: true,
		Brokers: []Broker{{NodeID: "broker1", Host: "host1", Voter: true}}}
	for i := 1; i <= 3; i++ {
		topology.Servers = append(topology.Servers, Server{ID: fmt.Sprint("server", i), Name: fmt.Sprint("nats", i),
			Host: fmt.Sprint("host", i), Running: modelProcess(int32(i)), Version: "2.14.6", Online: true, JetStream: true})
	}
	members := []Member{{ServerID: "server1", Current: true, Online: true},
		{ServerID: "server2", Current: true, Online: true}, {ServerID: "server3", Current: true, Online: true}}
	stream := Group{ID: "stream-raft", Account: "$G", Stream: "history", CreatedNS: 1,
		ConfigSHA256: infracontract.Digest{1}, Replicas: 3, Leader: "server1", Members: members}
	consumer := stream
	consumer.ID, consumer.Consumer = "consumer-raft", "receipt"
	return topology, canonicalSnapshot(Snapshot{Complete: true,
		Accounts: []Account{{ID: "$G", Streams: 1, Consumers: 1}}, Groups: []Group{stream, consumer},
		Meta: Group{ID: "cluster/meta", Replicas: 3, Leader: "server2", Members: members}})
}

// origin: hot-update S9 plan §3.2 / T24 / M13
func TestInfraTopologyUsesActualPlacement(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Topology, *Snapshot)
		ok   bool
	}{
		{"one_broker_three_actual_servers", func(*Topology, *Snapshot) {}, true},
		{"three_brokers_two_actual_servers", func(top *Topology, _ *Snapshot) {
			top.Brokers = append(top.Brokers, Broker{"broker2", "host2", true}, Broker{"broker3", "host3", true})
			top.Servers = top.Servers[:2]
		}, false},
		{"target_r1", func(_ *Topology, s *Snapshot) {
			s.Groups[0].Replicas = 1
			s.Groups[0].Members = s.Groups[0].Members[:1]
		}, false},
		{"survivor_r1", func(_ *Topology, s *Snapshot) {
			s.Groups[0].Replicas, s.Groups[0].Leader = 1, "server2"
			s.Groups[0].Members = []Member{{ServerID: "server2", Current: true, Online: true}}
		}, true},
		{"target_r2", func(_ *Topology, s *Snapshot) {
			s.Groups[0].Replicas = 2
			s.Groups[0].Members = s.Groups[0].Members[:2]
		}, false},
		{"surviving_r2", func(_ *Topology, s *Snapshot) {
			s.Groups[0].Replicas, s.Groups[0].Leader = 2, "server2"
			s.Groups[0].Members = append([]Member(nil), s.Groups[0].Members[1:]...)
		}, true},
		{"same_host_fake_replicas", func(top *Topology, _ *Snapshot) { top.Servers[2].Host = "host2" }, false},
		{"target_alias_host", func(top *Topology, _ *Snapshot) { top.Servers[2].Host = "host1" }, false},
		{"consumer_lag", func(_ *Topology, s *Snapshot) { s.Groups[0].Members[2].Lag = 1 }, false},
		{"consumer_not_current", func(_ *Topology, s *Snapshot) { s.Groups[0].Members[2].Current = false }, false},
		{"consumer_offline", func(_ *Topology, s *Snapshot) { s.Groups[0].Members[2].Online = false }, false},
		{"missing_consumer", func(_ *Topology, s *Snapshot) { s.Groups = s.Groups[1:] }, false},
		{"unknown_consumer_leader", func(_ *Topology, s *Snapshot) { s.Groups[0].Leader = "unknown" }, false},
		{"unknown_consumer_peer", func(_ *Topology, s *Snapshot) { s.Groups[0].Members[2].ServerID = "unknown" }, false},
		{"consumer_no_leader", func(_ *Topology, s *Snapshot) { s.Groups[0].Leader = "" }, false},
		{"missing_group_identity", func(_ *Topology, s *Snapshot) { s.Groups[0].ID = "" }, false},
		{"missing_creation_identity", func(_ *Topology, s *Snapshot) { s.Groups[0].CreatedNS = 0 }, false},
		{"missing_config_identity", func(_ *Topology, s *Snapshot) { s.Groups[0].ConfigSHA256 = infracontract.Digest{} }, false},
		{"missing_group_member", func(_ *Topology, s *Snapshot) { s.Groups[0].Members = s.Groups[0].Members[:2] }, false},
		{"duplicate_asset_identity", func(_ *Topology, s *Snapshot) { s.Groups[1].ID = s.Groups[0].ID }, false},
		{"unknown_account", func(_ *Topology, s *Snapshot) { s.Groups[0].Account = "foreign" }, false},
		{"orphan_consumer", func(_ *Topology, s *Snapshot) { s.Groups[0].Stream = "unknown" }, false},
		{"meta_on_target", func(_ *Topology, s *Snapshot) { s.Meta.Leader = "server1" }, false},
		{"unknown_meta_leader", func(_ *Topology, s *Snapshot) { s.Meta.Leader = "unknown" }, false},
		{"wrong_configured_majority", func(_ *Topology, s *Snapshot) { s.Meta.Replicas = 5 }, false},
		{"unknown_host", func(top *Topology, _ *Snapshot) { top.Servers[2].Host = "" }, false},
		{"unknown_process", func(top *Topology, _ *Snapshot) { top.Servers[2].Running.StartTicks = 0 }, false},
		{"incomplete_topology", func(top *Topology, _ *Snapshot) { top.Complete = false }, false},
		{"incomplete_inventory", func(_ *Topology, s *Snapshot) { s.Complete = false }, false},
		{"sole_voter_standalone", func(top *Topology, s *Snapshot) {
			top.Standalone, top.Servers = true, top.Servers[:1]
			s.Meta = Group{}
			for i := range s.Groups {
				s.Groups[i].Replicas = 1
				s.Groups[i].Members = s.Groups[i].Members[:1]
			}
		}, true},
		{"default_single_without_raft_voters", func(top *Topology, s *Snapshot) {
			top.Brokers, top.Standalone, top.Servers = nil, true, top.Servers[:1]
			s.Meta = Group{}
			for i := range s.Groups {
				s.Groups[i].Replicas = 1
				s.Groups[i].Members = s.Groups[i].Members[:1]
			}
		}, true},
		{"standalone_has_hidden_peers", func(top *Topology, _ *Snapshot) { top.Standalone = true }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			top, snapshot := topologyModel()
			tc.edit(&top, &snapshot)
			target, err := validateTopology(top, top.Servers[0].Running)
			if tc.name == "three_brokers_two_actual_servers" && err == nil {
				t.Fatal("broker count certified a two-server NATS topology")
			}
			if err == nil {
				err = validateSnapshot(top, snapshot, target.Host)
			}
			if (err == nil) != tc.ok {
				t.Fatalf("readiness accepted=%t, want=%t, err=%v", err == nil, tc.ok, err)
			}
		})
	}
	t.Run("same_pid_wrong_incarnation", func(t *testing.T) {
		top, _ := topologyModel()
		before := top.Servers[0].Running
		before.StartTicks++
		if _, err := validateTopology(top, before); err == nil {
			t.Fatal("PID alone certified the process incarnation")
		}
	})
	t.Run("consumer_effective_replicas", func(t *testing.T) {
		top, _ := topologyModel()
		parent := &nats.StreamInfo{Config: nats.StreamConfig{Name: "history", Replicas: 3, Retention: nats.LimitsPolicy}, Created: time.Now()}
		consumer := &nats.ConsumerInfo{Name: "ephemeral", Stream: "history", Created: time.Now(),
			Cluster: &nats.ClusterInfo{Leader: "nats2"}}
		g, err := consumerGroup(top, "$G", parent, consumer)
		if err != nil || g.Replicas != 1 || validateGroup(top, g, "host1", false) != nil {
			t.Fatal("off-target default ephemeral R1 was misclassified")
		}
		consumer.Cluster.Leader = "nats1"
		g, err = consumerGroup(top, "$G", parent, consumer)
		if err != nil || validateGroup(top, g, "host1", false) == nil {
			t.Fatal("target ephemeral R1 was accepted")
		}
		consumer.Config.Durable = "receipt"
		consumer.Cluster = &nats.ClusterInfo{Leader: "nats2", RaftGroup: "durable-raft", Replicas: []*nats.PeerInfo{
			{Name: "nats1", Current: true}, {Name: "nats3", Current: true}}}
		g, err = consumerGroup(top, "$G", parent, consumer)
		if err != nil || g.Replicas != 3 || validateGroup(top, g, "host1", false) != nil {
			t.Fatal("durable default did not retain its actual R3 group")
		}
		consumer.Config.Replicas = 5
		g, err = consumerGroup(top, "$G", parent, consumer)
		if err != nil || g.Replicas != 3 {
			t.Fatal("consumer count above parent did not use the server's effective count")
		}
	})
	t.Run("pinned_embedded_actual_groups", exerciseClusterInventory)
}

type readinessModel struct {
	topology  Topology
	snapshot  Snapshot
	seal      infracontract.AssetSeal
	topReads  int
	invReads  int
	sealReads int
	change    func(*readinessModel, string)
}

func newReadinessModel(t *testing.T) *readinessModel {
	t.Helper()
	top, snapshot := topologyModel()
	accounts, assets, err := SealDigests(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return &readinessModel{topology: top, snapshot: snapshot,
		seal: infracontract.AssetSeal{Revision: 1, Accounts: accounts, Assets: assets, Participants: infracontract.Digest{1}, Complete: true}}
}

func (m *readinessModel) ReadTopology(infracontract.Plan, infracontract.Deadline, infracontract.Stop) (Topology, error) {
	m.topReads++
	if m.change != nil {
		m.change(m, "topology")
	}
	return m.topology, nil
}

func (m *readinessModel) ReadInventory(Topology, infracontract.Deadline, infracontract.Stop) (Snapshot, error) {
	m.invReads++
	if m.change != nil {
		m.change(m, "inventory")
	}
	return canonicalSnapshot(m.snapshot), nil
}

func (m *readinessModel) ReadSeal(infracontract.Plan, infracontract.Deadline, infracontract.Stop) (infracontract.AssetSeal, error) {
	m.sealReads++
	if m.change != nil {
		m.change(m, "seal")
	}
	return m.seal, nil
}

func modelWSS(serverID string) WSSProof {
	return WSSProof{SNI: "infra.example", Pin: infracontract.Digest{9}, HTTP101: true,
		Attachment: infracontract.Attachment{ServerID: serverID, CID: 10, Generation: 1, HealthGeneration: 1,
			URL: "wss://infra.example", Ingress: modelProcess(80), Raw: infracontract.Socket{NetNS: 1,
				Device: 1, Inode: 1, Cookie: 1, Local: "local", Remote: "remote", Owner: modelProcess(81)}}}
}

func (m *readinessModel) Check(s Server, identity Identity, _ infracontract.Deadline, _ infracontract.Stop) (AuthEvidence, error) {
	return AuthEvidence{Identity: identity, ServerID: s.ID, CID: 5, PrivateInbox: true, RPC: infracontract.Digest{3},
		Receipt: AuthReceipt{Identity: identity, PrincipalSHA256: infracontract.Digest{6}, ServerID: s.ID, CID: 5, Generation: 1, RequestSHA256: infracontract.Digest{4}, Queue: "tether-authcallout"}, WSS: modelWSS(s.ID)}, nil
}

type editedAuth struct {
	source AuthSource
	edit   func(*AuthEvidence)
}

func (a editedAuth) Check(s Server, id Identity, d infracontract.Deadline, stop infracontract.Stop) (AuthEvidence, error) {
	e, err := a.source.Check(s, id, d, stop)
	a.edit(&e)
	return e, err
}

// origin: hot-update S9 plan §3.3 / T26
func TestInfraReadinessUsesOrdinaryAuth(t *testing.T) {
	cases := []struct {
		name string
		edit func(*AuthEvidence)
	}{
		{"peer_queue_masks_local_missing", func(e *AuthEvidence) { e.Receipt.ServerID = "peer" }},
		{"temporary_nonqueue_responder", func(e *AuthEvidence) { e.Receipt.Queue = "" }},
		{"wrong_client_id", func(e *AuthEvidence) { e.Receipt.CID++ }},
		{"wrong_authenticated_role", func(e *AuthEvidence) { e.Receipt.Identity = 0 }},
		{"agent_receipt_for_member", func(e *AuthEvidence) { e.Receipt.Identity = ProvisionedAgent }},
		{"no_authenticated_principal", func(e *AuthEvidence) { e.Receipt.PrincipalSHA256 = infracontract.Digest{} }},
		{"stale_queue_generation", func(e *AuthEvidence) { e.Receipt.Generation = 0 }},
		{"no_auth_request_observation", func(e *AuthEvidence) { e.Receipt.RequestSHA256 = infracontract.Digest{} }},
		{"shared_inbox", func(e *AuthEvidence) { e.PrivateInbox = false }},
		{"no_allowed_rpc", func(e *AuthEvidence) { e.RPC = infracontract.Digest{} }},
		{"wrong_identity", func(e *AuthEvidence) { e.Identity = 0 }},
		{"wrong_info_server", func(e *AuthEvidence) { e.ServerID = "unknown" }},
		{"wss_no_upgrade", func(e *AuthEvidence) { e.WSS.HTTP101 = false }},
		{"wss_no_pin", func(e *AuthEvidence) { e.WSS.Pin = infracontract.Digest{} }},
		{"wss_no_sni", func(e *AuthEvidence) { e.WSS.SNI = "" }},
		{"wss_wrong_upstream", func(e *AuthEvidence) { e.WSS.Attachment.ServerID = "peer" }},
		{"wss_unknown_ingress", func(e *AuthEvidence) { e.WSS.Attachment.Ingress.StartTicks = 0 }},
		{"wss_url_only", func(e *AuthEvidence) { e.WSS.Attachment.Raw = infracontract.Socket{} }},
		{"wss_unknown_socket_cookie", func(e *AuthEvidence) { e.WSS.Attachment.Raw.Cookie = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newReadinessModel(t)
			probe, err := New(m, m, m, editedAuth{source: m, edit: tc.edit})
			if err != nil {
				t.Fatal(err)
			}
			r, err := probe.Read(infracontract.Plan{Component: infracontract.NATS, Before: m.topology.Servers[0].Running}, readinessDeadline(t), nil)
			if !errors.Is(err, infracontract.ErrIncomplete) || r != (infracontract.Readiness{}) {
				t.Fatalf("invalid auth evidence authorized readiness: %v", err)
			}
		})
	}
	t.Run("real_activated_member_and_provisioned_agent", exerciseOrdinaryAuth)
}

type authFixture struct {
	server    *natsserver.Server
	broker    *nats.Conn
	queue     *nats.Subscription
	spec      Server
	ordinary  *OrdinaryAuth
	receiptMu sync.Mutex
	receipts  map[uint64]AuthReceipt
	rpcCount  int
}

func ordinaryAuthFixture(t *testing.T) *authFixture {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(accountKey.Wipe)
	accountPub, _ := accountKey.PublicKey()
	brokerKey, _ := nkeys.CreateUser()
	t.Cleanup(brokerKey.Wipe)
	brokerPub, _ := brokerKey.PublicKey()
	keys := make(map[Identity]nkeys.KeyPair)
	pubs := make(map[Identity]string)
	for _, id := range []Identity{ActivatedMember, ProvisionedAgent} {
		kp, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(kp.Wipe)
		keys[id], pubs[id] = kp, ""
		pubs[id], _ = kp.PublicKey()
	}
	memberFP, _ := auth.FingerprintFromActor(pubs[ActivatedMember])
	agentFP, _ := auth.FingerprintFromActor(pubs[ProvisionedAgent])
	if _, err := session.Create(db, "ready", "readiness", memberFP, "unused-no-pin-reconnect", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(db, "ready", "node1", agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: "ordinary-auth", NoLog: true, NoSigs: true,
		AlwaysEnableNonce: true, AuthTimeout: 0.2, Nkeys: []*natsserver.NkeyUser{{Nkey: brokerPub}},
		AuthCallout: &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(time.Second) {
		t.Fatal("ordinary auth server not ready")
	}
	brokerNC, err := nats.Connect(ns.ClientURL(), nats.Nkey(brokerPub, brokerKey.Sign), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(brokerNC.Close)
	f := &authFixture{server: ns, broker: brokerNC, receipts: make(map[uint64]AuthReceipt),
		spec: Server{ID: ns.ID(), Name: ns.Name(), Host: "host1", Version: natsserver.VERSION, Running: modelProcess(1), Online: true, JetStream: true}}
	handler := &authcallout.Handler{DB: db, AccountKp: accountKey, Now: time.Now, JWTTTL: time.Hour,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// This is the fixture's long-lived ordinary queue, using the real production
	// decision Handler. No second auth subscription is installed by readiness.
	f.queue, err = brokerNC.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", func(msg *nats.Msg) {
		request, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data))
		if err != nil {
			t.Error("invalid real auth request")
			return
		}
		response, err := handler.Handle(string(msg.Data))
		if err != nil {
			t.Error("real auth handler failed")
			return
		}
		f.receiptMu.Lock()
		id := ActivatedMember
		if request.ConnectOptions.Name == "tether-agent:ready:node1" {
			id = ProvisionedAgent
		}
		f.receipts[request.ClientInformation.ID] = AuthReceipt{Identity: id, PrincipalSHA256: sha256.Sum256([]byte(request.ConnectOptions.Nkey)), ServerID: brokerNC.ConnectedServerId(),
			CID: request.ClientInformation.ID, Generation: 1, RequestSHA256: sha256.Sum256(msg.Data), Queue: msg.Sub.Queue}
		f.receiptMu.Unlock()
		if err := msg.Respond([]byte(response)); err != nil {
			t.Error("auth response failed")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	subjects := map[Identity]string{ActivatedMember: proto.SubjCtrlBy(pubs[ActivatedMember], "session.list.req"),
		ProvisionedAgent: proto.SubjNodeRegister("ready", "node1")}
	for _, subject := range subjects {
		_, err := brokerNC.Subscribe(subject, func(msg *nats.Msg) {
			f.receiptMu.Lock()
			f.rpcCount++
			f.receiptMu.Unlock()
			if err := msg.Respond([]byte(`{"ok":true}`)); err != nil {
				t.Error("ordinary RPC response failed")
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := brokerNC.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	f.ordinary = &OrdinaryAuth{
		Dial: func(_ Server, id Identity, _ infracontract.Deadline, _ infracontract.Stop) (*nats.Conn, error) {
			name := "tether-cli:ready"
			if id == ProvisionedAgent {
				name = "tether-agent:ready:node1"
			}
			opts := append(natsinbox.InboxConnectOptions(pubs[id]), nats.Nkey(pubs[id], keys[id].Sign),
				nats.Name(name), nats.NoReconnect(), nats.Timeout(time.Second))
			return nats.Connect(ns.ClientURL(), opts...)
		},
		Receipt: func(nc *nats.Conn, _ Identity, _ infracontract.Deadline, _ infracontract.Stop) (AuthReceipt, error) {
			cid, err := nc.GetClientID()
			f.receiptMu.Lock()
			defer f.receiptMu.Unlock()
			return f.receipts[cid], err
		},
		RPC: func(nc *nats.Conn, id Identity, d infracontract.Deadline, stop infracontract.Stop) (infracontract.Digest, error) {
			data, err := (NATSRequester{Conn: nc}).Request(subjects[id], []byte(`{}`), d, stop)
			if err != nil {
				return infracontract.Digest{}, err
			}
			var reply struct {
				OK bool `json:"ok"`
			}
			if json.Unmarshal(data, &reply) != nil || !reply.OK {
				return infracontract.Digest{}, infracontract.ErrIncomplete
			}
			return sha256.Sum256(data), nil
		},
		// Physical Caddy evidence is a model in this component test. The real
		// pinned TLS/101/cid/accepted-socket producer is tested by P14/P28/P29.
		WSS: func(s Server, _ Identity, _ infracontract.Deadline, _ infracontract.Stop) (WSSProof, error) {
			return modelWSS(s.ID), nil
		},
	}
	return f
}

func exerciseOrdinaryAuth(t *testing.T) {
	t.Helper()
	f := ordinaryAuthFixture(t)
	for _, id := range []Identity{ActivatedMember, ProvisionedAgent} {
		e, err := f.ordinary.Check(f.spec, id, readinessDeadline(t), nil)
		if err != nil || !validReceipt(e.Receipt, f.spec.ID, e.CID) || !e.PrivateInbox || e.RPC == (infracontract.Digest{}) {
			t.Fatalf("ordinary identity %d failed: %v", id, err)
		}
	}
	f.receiptMu.Lock()
	count := f.rpcCount
	f.receiptMu.Unlock()
	if count != 2 {
		t.Fatalf("actual allowed RPC count=%d, want=2", count)
	}
	originalReceipt := f.ordinary.Receipt
	f.ordinary.Receipt = func(nc *nats.Conn, id Identity, d infracontract.Deadline, stop infracontract.Stop) (AuthReceipt, error) {
		r, err := originalReceipt(nc, id, d, stop)
		r.ServerID = "peer-responder"
		return r, err
	}
	if _, err := f.ordinary.Check(f.spec, ActivatedMember, readinessDeadline(t), nil); err == nil {
		t.Fatal("peer queue receipt masked missing local queue")
	}
	f.ordinary.Receipt = originalReceipt
	if err := f.queue.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := f.broker.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// No temporary readiness responder may make CONNECT succeed after the only
	// normal queue has been removed. Keep the same provisioned credentials.
	if _, err := f.ordinary.Check(f.spec, ActivatedMember, readinessDeadline(t), nil); err == nil {
		t.Fatal("readiness survived removal of the normal auth queue")
	}
}

// origin: hot-update S9 plan §12 P11 / L:TestInfraReadinessLifecycle
func TestInfraReadinessLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint("round", round), func(t *testing.T) { exerciseReadiness(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "infra readiness", beforeG)
	testharness.AssertNoFDLeak(t, "infra readiness", beforeFD, 2)
}

func exerciseReadiness(t *testing.T) {
	t.Helper()
	f := inventoryFixture(t, 2)
	snapshot, err := f.reader.ReadInventory(f.topology, readinessDeadline(t), nil)
	if err != nil || !snapshot.Complete || len(snapshot.Groups) < 2 {
		t.Fatalf("real nonempty snapshot failed: %v", err)
	}
	stop := make(chan struct{})
	close(stop)
	if _, err := f.reader.ReadInventory(f.topology, readinessDeadline(t), stop); err == nil {
		t.Fatal("closed stop admitted inventory")
	}
	// Exercise active cancellation with a real request that has a subscriber but
	// deliberately no reply; completion must join the cancellation watcher.
	arrived := make(chan struct{})
	_, err = f.business.Subscribe("readiness.blocked", func(*nats.Msg) { close(arrived) })
	if err != nil {
		t.Fatal(err)
	}
	if err := f.business.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	stop = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := (NATSRequester{Conn: f.business}).Request("readiness.blocked", nil, readinessDeadline(t), stop)
		done <- err
	}()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("request never reached real subscriber")
	}
	close(stop)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled request succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("request did not join after cancellation")
	}
	exerciseOrdinaryAuth(t)
}
