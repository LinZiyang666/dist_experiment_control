// Inventory tests read real SYS/JS APIs from the pinned embedded server. Faults
// corrupt or drop actual replies after forwarding; configuration-seal/host facts
// are models, and stock adjacent-version/multi-host receipts belong to P28.
package infrareadiness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type inventoryTestFixture struct {
	server   *natsserver.Server
	system   *nats.Conn
	business *nats.Conn
	access   []AccountAccess
	reader   *JetStream
	topology Topology
	js       nats.JetStreamContext
}

func inventoryFixture(t *testing.T, accounts int) *inventoryTestFixture {
	t.Helper()
	sysAccount := natsserver.NewAccount("$SYS")
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: "inventory",
		NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir(), SystemAccount: "$SYS", Accounts: []*natsserver.Account{sysAccount},
		Users: []*natsserver.User{{Username: "sys", Password: "sys-test", Account: sysAccount,
			Permissions: &natsserver.Permissions{Publish: &natsserver.SubjectPermission{Allow: []string{
				"$SYS.REQ.SERVER.*.JSZ", "$SYS.REQ.SERVER.*.VARZ"}}, Subscribe: &natsserver.SubjectPermission{Allow: []string{"_INBOX.>"}}}}}}
	for i := 0; i < accounts; i++ {
		a := natsserver.NewAccount(fmt.Sprintf("account%03d", i))
		opts.Accounts = append(opts.Accounts, a)
		opts.Users = append(opts.Users, &natsserver.User{Username: a.Name, Password: "business-test", Account: a})
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(time.Second) {
		t.Fatal("inventory server not ready")
	}
	f := &inventoryTestFixture{server: ns, topology: Topology{Revision: 1, Complete: true, Standalone: true,
		Brokers: []Broker{{NodeID: "sole-voter", Host: "host1", Voter: true}}, Servers: []Server{{ID: ns.ID(), Name: ns.Name(),
			Host: "host1", Running: modelProcess(1), Version: natsserver.VERSION, JetStream: true, Online: true}}}}
	f.system, err = nats.Connect(ns.ClientURL(), nats.UserInfo("sys", "sys-test"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.system.Close)
	for i := 0; i < accounts; i++ {
		name := fmt.Sprintf("account%03d", i)
		acc, err := ns.LookupAccount(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := acc.EnableJetStream(nil, nil); err != nil {
			t.Fatal(err)
		}
		nc, err := nats.Connect(ns.ClientURL(), nats.UserInfo(name, "business-test"), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		f.access = append(f.access, AccountAccess{ID: name, Query: NATSRequester{Conn: nc}})
		if i == 0 {
			f.business = nc
		}
	}
	f.js, err = f.business.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		name := fmt.Sprint("history", i)
		if _, err := f.js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Replicas: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.js.Publish(name, []byte("original-acknowledged-payload")); err != nil {
			t.Fatal(err)
		}
		if _, err := f.js.AddConsumer(name, &nats.ConsumerConfig{Durable: "receipt", AckPolicy: nats.AckExplicitPolicy}); err != nil {
			t.Fatal(err)
		}
	}
	f.reader, err = NewJetStream(NATSRequester{Conn: f.system}, f.access)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type replyFault struct {
	real     Requester
	edit     func(string, []byte, []byte) ([]byte, error)
	requests int
}

func (r *replyFault) Request(subject string, payload []byte, d infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
	// All observed queries must remain strictly read-only, including negatives.
	if strings.Contains(subject, ".CREATE") || strings.Contains(subject, ".UPDATE") || strings.Contains(subject, ".DELETE") || strings.Contains(subject, ".PURGE") {
		return nil, errors.New("inventory attempted an asset mutation")
	}
	r.requests++
	data, err := r.real.Request(subject, payload, d, stop)
	if err != nil {
		return nil, err
	}
	return r.edit(subject, payload, data)
}

func intPointer(i int) *int { return &i }

// origin: hot-update S9 plan §2.2 / §3.2 / T25 / M13
func TestInfraInventoryRequiresAllPages(t *testing.T) {
	f := inventoryFixture(t, 65) // Force the real SYS account enumeration over two pages.
	d := readinessDeadline(t)
	snapshot, err := f.reader.ReadInventory(f.topology, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Complete || len(snapshot.Accounts) != 65 || len(snapshot.Groups) != 6 ||
		validateSnapshot(f.topology, snapshot, "host1") != nil {
		t.Fatal("nonempty all-account inventory did not close")
	}
	// A snapshot signs account/name/creation/config/group identity, not names only.
	accountsDigest, assetDigest, err := SealDigests(snapshot)
	if err != nil || accountsDigest == (infracontract.Digest{}) || assetDigest == (infracontract.Digest{}) {
		t.Fatal("snapshot has no identity digest")
	}
	for _, g := range snapshot.Groups {
		if g.Account != "account000" || g.ID == "" || g.CreatedNS == 0 || g.ConfigSHA256 == (infracontract.Digest{}) {
			t.Fatal("asset lost its identity")
		}
	}
	// Keep the complete fixture-only inventory as a receipt in go test -json;
	// these records contain config digests and synthetic account names, no keys,
	// payloads or private socket tuples. Production readiness exposes digests only.
	// origin: hot-update S9 plan §12 P11 / R:full asset snapshot
	logAssetSnapshot(t, "single", snapshot)
	cases := []struct {
		name string
		edit func(string, []byte, []byte) ([]byte, error)
	}{
		{"missing_stream_page", func(subject string, payload, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.LIST" {
				return data, nil
			}
			var request struct {
				Offset int `json:"offset"`
			}
			_ = json.Unmarshal(payload, &request)
			var page streamPage
			_ = json.Unmarshal(data, &page)
			if request.Offset == 0 && len(page.Streams) > 2 {
				page.Streams = page.Streams[:2]
				page.Limit = intPointer(2)
			}
			if request.Offset == 2 {
				page.Streams = nil
			}
			return json.Marshal(page)
		}},
		{"repeated_stream_offset", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.LIST" {
				return data, nil
			}
			var page streamPage
			_ = json.Unmarshal(data, &page)
			if *page.Total != 0 {
				page.Offset = intPointer(1)
			}
			return json.Marshal(page)
		}},
		{"changed_stream_total", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.LIST" {
				return data, nil
			}
			var page streamPage
			_ = json.Unmarshal(data, &page)
			page.Total = intPointer(*page.Total + 1)
			return json.Marshal(page)
		}},
		{"duplicate_stream", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.LIST" {
				return data, nil
			}
			var page streamPage
			_ = json.Unmarshal(data, &page)
			if len(page.Streams) > 1 {
				page.Streams[1] = page.Streams[0]
			}
			return json.Marshal(page)
		}},
		{"missing_consumer", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if !strings.HasPrefix(subject, "$JS.API.CONSUMER.LIST.") {
				return data, nil
			}
			var page consumerPage
			_ = json.Unmarshal(data, &page)
			page.Consumers = nil
			return json.Marshal(page)
		}},
		{"wrong_consumer_stream", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if !strings.HasPrefix(subject, "$JS.API.CONSUMER.LIST.") {
				return data, nil
			}
			var page consumerPage
			_ = json.Unmarshal(data, &page)
			page.Consumers[0].Stream = "another"
			return json.Marshal(page)
		}},
		{"missing_upstream_asset", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.LIST" {
				return data, nil
			}
			var page streamPage
			_ = json.Unmarshal(data, &page)
			page.Missing = []string{"foreign"}
			return json.Marshal(page)
		}},
		{"offline_upstream_asset", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.LIST" {
				return data, nil
			}
			var page streamPage
			_ = json.Unmarshal(data, &page)
			page.Offline = []byte(`{"history":"offline"}`)
			return json.Marshal(page)
		}},
		{"missing_account_page", func(subject string, payload, data []byte) ([]byte, error) {
			if !strings.HasSuffix(subject, ".JSZ") {
				return data, nil
			}
			var request struct {
				Offset int `json:"offset"`
			}
			_ = json.Unmarshal(payload, &request)
			if request.Offset == 0 {
				return data, nil
			}
			var response systemResponse
			_ = json.Unmarshal(data, &response)
			response.Data.Accounts = nil
			return json.Marshal(response)
		}},
		{"duplicate_account_page", func(subject string, payload, data []byte) ([]byte, error) {
			if !strings.HasSuffix(subject, ".JSZ") {
				return data, nil
			}
			var request struct {
				Offset int `json:"offset"`
			}
			_ = json.Unmarshal(payload, &request)
			if request.Offset == 0 {
				return data, nil
			}
			var response systemResponse
			_ = json.Unmarshal(data, &response)
			response.Data.Accounts[0].ID = "account000"
			return json.Marshal(response)
		}},
		{"unknown_server_response", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if !strings.HasSuffix(subject, ".JSZ") {
				return data, nil
			}
			var response systemResponse
			_ = json.Unmarshal(data, &response)
			response.Server.ID = "unknown"
			return json.Marshal(response)
		}},
		{"wrong_running_version", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if !strings.HasSuffix(subject, ".VARZ") {
				return data, nil
			}
			var response systemResponse
			_ = json.Unmarshal(data, &response)
			response.Data.Version = "disk-version"
			return json.Marshal(response)
		}},
		{"user_config_metadata_changed", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject != "$JS.API.STREAM.INFO.history0" {
				return data, nil
			}
			var info nats.StreamInfo
			_ = json.Unmarshal(data, &info)
			if info.Config.Metadata == nil {
				info.Config.Metadata = make(map[string]string)
			}
			info.Config.Metadata["operator_note"] = "changed"
			return json.Marshal(info)
		}},
		{"oversized_page", func(subject string, _ []byte, data []byte) ([]byte, error) {
			if strings.HasSuffix(subject, ".VARZ") {
				return append(data, bytes.Repeat([]byte(" "), infracontract.MaxCensusPageBytes-len(data)+1)...), nil
			}
			return data, nil
		}},
		{"malformed_reply", func(string, []byte, []byte) ([]byte, error) { return []byte("{} trailing"), nil }},
		{"lost_reply", func(string, []byte, []byte) ([]byte, error) { return nil, errors.New("reply lost after real effect") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sys := &replyFault{real: NATSRequester{Conn: f.system}, edit: tc.edit}
			access := append([]AccountAccess(nil), f.access...)
			business := &replyFault{real: access[0].Query, edit: tc.edit}
			access[0].Query = business
			reader, err := NewJetStream(sys, access)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := reader.ReadInventory(f.topology, readinessDeadline(t), nil)
			if !errors.Is(err, infracontract.ErrIncomplete) || snapshot.Complete {
				t.Fatalf("incomplete actual inventory accepted: %v", err)
			}
			if sys.requests+business.requests == 0 {
				t.Fatal("fault did not observe a real query")
			}
		})
	}
	t.Run("foreign_account_without_read_permission", func(t *testing.T) {
		reader, err := NewJetStream(NATSRequester{Conn: f.system}, f.access[:1])
		if err != nil {
			t.Fatal(err)
		}
		if snapshot, err := reader.ReadInventory(f.topology, readinessDeadline(t), nil); !errors.Is(err, infracontract.ErrIncomplete) || snapshot.Complete {
			t.Fatal("unreadable account was silently omitted")
		}
	})
	t.Run("snapshot_byte_limit", func(t *testing.T) {
		b := &readBudget{bytes: infracontract.MaxCensusBytes - 1}
		var out paged
		if err := b.query(NATSRequester{Conn: f.business}, "$JS.API.STREAM.LIST", nil, &out, readinessDeadline(t), nil); !errors.Is(err, infracontract.ErrIncomplete) {
			t.Fatal("snapshot byte bound ignored")
		}
	})
	t.Run("same_name_delete_recreate", func(t *testing.T) {
		recreated := false
		query := &replyFault{real: f.access[0].Query, edit: func(subject string, _ []byte, data []byte) ([]byte, error) {
			if subject == "$JS.API.CONSUMER.LIST.history0" && !recreated {
				recreated = true
				old, err := f.js.StreamInfo("history0")
				if err != nil {
					return nil, err
				}
				if err := f.js.DeleteStream("history0"); err != nil {
					return nil, err
				}
				if _, err := f.js.AddStream(&old.Config); err != nil {
					return nil, err
				}
				if _, err := f.js.AddConsumer("history0", &nats.ConsumerConfig{Durable: "receipt", AckPolicy: nats.AckExplicitPolicy}); err != nil {
					return nil, err
				}
			}
			return data, nil
		}}
		access := append([]AccountAccess(nil), f.access...)
		access[0].Query = query
		reader, err := NewJetStream(NATSRequester{Conn: f.system}, access)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot, err := reader.ReadInventory(f.topology, readinessDeadline(t), nil); err == nil || snapshot.Complete {
			t.Fatal("recreated asset kept old readiness")
		}
		if !recreated {
			t.Fatal("real delete/create fault was not exercised")
		}
	})
	t.Run("fence_and_observation_revalidation", exerciseSealRevalidation)
}

func exerciseSealRevalidation(t *testing.T) {
	t.Helper()
	cases := []struct {
		name string
		edit func(*readinessModel, string)
		ok   bool
	}{
		{"stable_joined_fence", func(*readinessModel, string) {}, true},
		{"unsealed_writer", func(m *readinessModel, _ string) { m.seal.Complete = false }, false},
		{"wrong_accounts_digest", func(m *readinessModel, _ string) { m.seal.Accounts = infracontract.Digest{7} }, false},
		{"wrong_assets_digest", func(m *readinessModel, _ string) { m.seal.Assets = infracontract.Digest{7} }, false},
		{"sent_unknown_not_joined", func(m *readinessModel, _ string) { m.seal.Participants = infracontract.Digest{} }, false},
		{"fence_revision_changed", func(m *readinessModel, phase string) {
			if phase == "seal" && m.sealReads == 2 {
				m.seal.Revision++
			}
		}, false},
		{"same_name_recreated_after_auth", func(m *readinessModel, phase string) {
			if phase == "inventory" && m.invReads == 2 {
				m.snapshot.Groups[0].CreatedNS++
			}
		}, false},
		{"config_changed_after_auth", func(m *readinessModel, phase string) {
			if phase == "inventory" && m.invReads == 2 {
				m.snapshot.Groups[0].ConfigSHA256[0]++
			}
		}, false},
		{"late_participant_join", func(m *readinessModel, phase string) {
			if phase == "seal" && m.sealReads == 2 {
				m.seal.Participants[0]++
			}
		}, false},
		{"physical_server_restarted", func(m *readinessModel, phase string) {
			if phase == "topology" && m.topReads == 2 {
				m.topology.Servers[0].Running.StartTicks++
			}
		}, false},
		{"late_topology_revision", func(m *readinessModel, phase string) {
			if phase == "topology" && m.topReads == 2 {
				m.topology.Revision++
			}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newReadinessModel(t)
			m.change = tc.edit
			p, err := New(m, m, m, m)
			if err != nil {
				t.Fatal(err)
			}
			r, err := p.Read(infracontract.Plan{Component: infracontract.NATS, Before: m.topology.Servers[0].Running}, readinessDeadline(t), nil)
			if (err == nil && r.Complete) != tc.ok {
				t.Fatalf("seal readiness=%t want=%t: %v", r.Complete, tc.ok, err)
			}
			if !tc.ok && r != (infracontract.Readiness{}) {
				t.Fatal("partial readiness escaped a refused fence")
			}
		})
	}
	m := newReadinessModel(t)
	p, err := New(m, m, m, m)
	if err != nil {
		t.Fatal(err)
	}
	plan := infracontract.Plan{Component: infracontract.NATS, Before: m.topology.Servers[0].Running}
	for _, d := range []infracontract.Deadline{0, 1} {
		if _, err := p.Read(plan, d, nil); err == nil {
			t.Fatal("invalid/expired deadline admitted readiness")
		}
	}
	stop := make(chan struct{})
	close(stop)
	if _, err := p.Read(plan, readinessDeadline(t), stop); err == nil {
		t.Fatal("stopped read was admitted")
	}
	plan.Component = infracontract.Caddy
	if _, err := p.Read(plan, readinessDeadline(t), nil); !errors.Is(err, infracontract.ErrUnsupported) {
		t.Fatal("JS readiness accepted a different component")
	}
	if _, err := New(nil, m, m, m); err == nil {
		t.Fatal("nil topology factory accepted")
	}
	if _, err := NewJetStream(nil, nil); err == nil {
		t.Fatal("nil inventory factory accepted")
	}
	// The public boundary has one total deadline, rather than a refreshed budget
	// per page. An expired real NC query never begins a new admission window.
	d, err := controlbudget.After(time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (NATSRequester{}).Request("$JS.API.INFO", nil, d, nil); err == nil {
		t.Fatal("missing requester admitted inventory")
	}
}

// Real pinned JS meta/stream/consumer groups run in three embedded servers.
// Host identities are modeled: this is not a multi-host or stock-binary receipt.
// origin: hot-update S9 plan §3.2 / T24
func exerciseClusterInventory(t *testing.T) {
	t.Helper()
	top := Topology{Complete: true, Revision: 1, Brokers: []Broker{{NodeID: "broker1", Host: "host1", Voter: true}}}
	var servers []*natsserver.Server
	// The seed also needs an explicitly configured route to enter clustered JS.
	// A self route supplies that configuration without adding a fourth peer.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	seedAddress, ok := reservation.Addr().(*net.TCPAddr)
	if !ok {
		_ = reservation.Close()
		t.Fatal("TCP route reservation has an unexpected address")
	}
	seedPort := seedAddress.Port
	seedRoute, err := url.Parse("nats-route://" + reservation.Addr().String())
	if err != nil {
		_ = reservation.Close()
		t.Fatal(err)
	}
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	routes := []*url.URL{seedRoute}
	for i := 1; i <= 3; i++ {
		sys, biz := natsserver.NewAccount("$SYS"), natsserver.NewAccount("business")
		opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
			ServerName: fmt.Sprint("nats", i), JetStream: true, StoreDir: t.TempDir(), Routes: routes,
			Cluster:       natsserver.ClusterOpts{Name: "readiness", Host: "127.0.0.1", Port: -1},
			SystemAccount: "$SYS", Accounts: []*natsserver.Account{sys, biz}, Users: []*natsserver.User{
				{Username: "sys", Password: "sys-test", Account: sys, Permissions: &natsserver.Permissions{
					Publish:   &natsserver.SubjectPermission{Allow: []string{"$SYS.REQ.SERVER.*.VARZ", "$SYS.REQ.SERVER.*.JSZ"}},
					Subscribe: &natsserver.SubjectPermission{Allow: []string{"_INBOX.>"}}}},
				{Username: "business", Password: "business-test", Account: biz}}}
		if i == 1 {
			opts.Cluster.Port = seedPort
		}
		ns, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		ns.Start()
		t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
		if !ns.ReadyForConnections(time.Second) {
			t.Fatal("clustered inventory server not ready")
		}
		if i == 1 {
			route, err := url.Parse("nats-route://" + ns.ClusterAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			routes = []*url.URL{route}
		}
		account, err := ns.LookupAccount("business")
		if err != nil {
			t.Fatal(err)
		}
		if err := account.EnableJetStream(nil, nil); err != nil {
			t.Fatal(err)
		}
		servers = append(servers, ns)
		top.Servers = append(top.Servers, Server{ID: ns.ID(), Name: ns.Name(), Version: natsserver.VERSION,
			Host: fmt.Sprint("host", i), Running: modelProcess(int32(i)), JetStream: true, Online: true})
	}
	sys, err := nats.Connect(servers[0].ClientURL(), nats.UserInfo("sys", "sys-test"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sys.Close)
	business, err := nats.Connect(servers[0].ClientURL(), nats.UserInfo("business", "business-test"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(business.Close)
	reader, err := NewJetStream(NATSRequester{Conn: sys}, []AccountAccess{{ID: "business", Query: NATSRequester{Conn: business}}})
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		_, _, err := reader.readSystem(top, &readBudget{}, readinessDeadline(t), nil)
		return err == nil
	}) {
		t.Fatal("real JS meta topology did not converge")
	}
	js, err := business.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "replicated", Subjects: []string{"replicated"}, Replicas: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish("replicated", []byte("original-replicated-ack")); err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddConsumer("replicated", &nats.ConsumerConfig{Durable: "receipt", AckPolicy: nats.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	var target Server
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		var err error
		snapshot, err = reader.ReadInventory(top, readinessDeadline(t), nil)
		if err != nil || len(snapshot.Groups) != 2 {
			return false
		}
		for _, s := range top.Servers {
			if s.ID != snapshot.Meta.Leader {
				target = s
				break
			}
		}
		return validateSnapshot(top, snapshot, target.Host) == nil
	}) {
		t.Fatal("real R3 asset/consumer placement did not become current")
	}
	for _, g := range snapshot.Groups {
		if g.Replicas != 3 || len(g.Members) != 3 || g.ID == "" {
			t.Fatal("actual replicated group identity was omitted")
		}
	}
	logAssetSnapshot(t, "cluster", snapshot)
	if _, err := validateTopology(top, target.Running); err != nil {
		t.Fatal(err)
	}
	// Remove the normal meta leader from one follower's report while preserving
	// a known, off-reporter server identity. Only independent agreement can reject
	// this; the actual leader's own peer list remains intact and current.
	fault := &replyFault{real: NATSRequester{Conn: sys}, edit: func(subject string, _ []byte, data []byte) ([]byte, error) {
		if !strings.HasSuffix(subject, ".JSZ") {
			return data, nil
		}
		var response systemResponse
		if err := json.Unmarshal(data, &response); err != nil {
			return nil, err
		}
		if response.Server.ID == target.ID {
			for _, s := range top.Servers {
				if s.ID != snapshot.Meta.Leader && s.ID != target.ID {
					response.Data.Meta.Leader = s.Name
					break
				}
			}
		}
		return json.Marshal(response)
	}}
	broken, err := NewJetStream(fault, []AccountAccess{{ID: "business", Query: NATSRequester{Conn: business}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broken.ReadInventory(top, readinessDeadline(t), nil); !errors.Is(err, infracontract.ErrIncomplete) {
		t.Fatal("follower disagreement was hidden by the leader's healthy group")
	}
}

func logAssetSnapshot(t *testing.T, topology string, snapshot Snapshot) {
	t.Helper()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("infra_asset_snapshot %s %s", topology, payload)
}
