//go:build d9_integration

package d9_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The production cluster-GROW risk these two tests cover (no other test does — d5 bootstraps
// clustered from the start; d9's other tests share one in-process NATS):
//
// The live N=1 broker (pc732) runs STANDALONE JetStream — its nats.conf has a `jetstream{}`
// block but NO `cluster{}` block, because `cluster init --from-existing` removed the cluster
// block so JS would start (clustered JS refuses to start without configured routes, and a
// single node can never reach the JS meta-group's quorum-of-2, so N=1 MUST run JS standalone).
// Growing to N=2 restarts that node CLUSTERED.
//
// FINDING (testGrowInPlaceOrphansStreams): NATS does NOT migrate a standalone-JS server's
// streams into a clustered meta. The clustered meta-group forms FINE (it is NOT a wedge), but
// the pre-existing standalone streams are ORPHANED — invisible to the clustered meta (404) with
// their on-disk files left as garbage. The broker would then re-create empty streams over that
// garbage. So the grow MUST reset the converting node's JetStream store first.
//
// FIX (testGrowResetThenStaggeredWorks): wipe the converting node's JS store, then bring the
// cluster up in the PRODUCTION rolling order (new node first, former-N1 LAST) — the meta forms
// and a stream raises to R=2. NOTE the data cost: the reset drops ALL of that node's JetStream
// audit/history (history-<sid>, events, in-flight OBJ_xfer); see cluster-runbook.md §1 step 3a.

// testGrowInPlaceOrphansStreams is the CONSTRAINT (negative control): restart a standalone-JS
// node CLUSTERED in place (NO JS-store reset). The meta forms (proven by AccountInfo serving on
// a FRESH context — not the shared/expiring context that made an earlier version of this test a
// false "wedge"), but the pre-existing stream is ORPHANED (404). If a future NATS bump makes the
// in-place transition preserve the stream, this flips and the grow can be simplified.
func testGrowInPlaceOrphansStreams(t *testing.T) {
	storeA := t.TempDir()

	// standalone A + SEED stream + data
	sA := growStartNats(t, growStandaloneOpts("orph-A", storeA))
	ncA := growConnect(t, sA.ClientURL())
	jsA, err := jetstream.New(ncA)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	growSeedStream(t, jsA, "SEED", "seed.x", 3)
	ncA.Close()
	sA.Shutdown()
	sA.WaitForShutdown()

	// in-place clustered restart (DIRTY store, no reset) + fresh B, brought up meshed.
	servers := growMesh(t, []string{"orph-A", "orph-B"}, []string{storeA, t.TempDir()})
	nc := growConnect(t, servers[0].ClientURL())
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New cluster: %v", err)
	}

	// the meta SERVES (fresh-context AccountInfo) — this is NOT a wedge.
	if !growWait(30*time.Second, func() bool { return growAccountInfoOK(js) }) {
		t.Fatal("clustered JS meta did not serve after in-place restart")
	}
	// ...but the pre-existing SEED stream is ORPHANED (not in the clustered meta).
	if growStreamPresent(js, "SEED") {
		t.Skip("SEED survived the in-place clustered restart — NATS now migrates standalone JS state; the grow may drop the JS reset")
	}
	t.Log("CONFIRMED: in-place standalone->clustered restart ORPHANS the pre-existing stream (meta forms; stream 404) — the JS-store reset is required")
}

// testGrowResetThenStaggeredWorks is the FIX in the PRODUCTION rolling order: wipe the converting
// node's JS store, bring the cluster up new-node-first / former-N1-LAST, and prove the meta forms
// + a stream replicates to R=2 with its data. (The seeded SEED stream is intentionally gone — the
// documented reset cost.)
func testGrowResetThenStaggeredWorks(t *testing.T) {
	storeA := t.TempDir()

	sA := growStartNats(t, growStandaloneOpts("grow-A", storeA))
	ncA := growConnect(t, sA.ClientURL())
	jsA, err := jetstream.New(ncA)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	growSeedStream(t, jsA, "SEED", "seed.x", 3)
	ncA.Close()
	sA.Shutdown()
	sA.WaitForShutdown()

	// FIX (1): reset the converting node's JetStream store.
	if err := os.RemoveAll(filepath.Join(storeA, "jetstream")); err != nil {
		t.Fatalf("reset JS store: %v", err)
	}

	// FIX (2): PRODUCTION rolling order — new node (B) FIRST, former-N1 (A) LAST.
	servers := growStaggered(t, []string{"grow-B", "grow-A"}, []string{t.TempDir(), storeA})
	var aURL string
	for _, s := range servers {
		if s.Name() == "grow-A" {
			aURL = s.ClientURL()
		}
	}
	nc := growConnect(t, aURL)
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New cluster: %v", err)
	}

	if !growWait(30*time.Second, func() bool { return growAccountInfoOK(js) }) {
		t.Fatal("FINDING: clustered JS meta did not serve after reset + staggered (production-order) grow")
	}
	if growStreamPresent(js, "SEED") {
		t.Fatal("SEED unexpectedly present after the JS-store reset")
	}

	// the broker re-creates streams clustered; prove a fresh stream replicates to R=2 with data.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "POSTGROW", Subjects: []string{"postgrow.x"}, Replicas: 1, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create post-grow stream: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := js.Publish(ctx, "postgrow.x", []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("post-grow publish: %v", err)
		}
	}
	if !growWait(40*time.Second, func() bool {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, e := js.Stream(c, "POSTGROW")
		if e != nil {
			return false
		}
		cfg := s.CachedInfo().Config
		cfg.Replicas = 2
		_, e = js.UpdateStream(c, cfg)
		return e == nil
	}) {
		t.Fatal("FINDING: could not expand stream to R=2 after grow")
	}
	if !growWait(30*time.Second, func() bool { // 30s margin for the replica add + catch-up under -race load
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, e := js.Stream(c, "POSTGROW")
		if e != nil {
			return false
		}
		ci, e := s.Info(c)
		return e == nil && ci.Config.Replicas == 2 && ci.State.Msgs == 5
	}) {
		t.Fatal("FINDING: stream did not settle at R=2 with its 5 messages")
	}
	t.Log("PASS: reset + staggered (production-order) clustered grow reached working R=2 with no data loss on the new stream")
}

// ---- bring-up helpers ----

// growMesh brings up len(names) clustered-JS servers ~simultaneously and waits for the JS meta.
func growMesh(t *testing.T, names, stores []string) []*natsserver.Server {
	t.Helper()
	n := len(names)
	clusterPorts, routes := growClusterPortsAndRoutes(t, n)
	servers := make([]*natsserver.Server, n)
	for i := 0; i < n; i++ {
		servers[i] = growStartClusteredNoWait(t, names[i], stores[i], clusterPorts[i], routes)
	}
	growAwaitReadyAndMeta(t, servers)
	return servers
}

// growStaggered brings the servers up ONE AT A TIME, in the given order (the production rolling
// restart: new node first, former-N1/leader last), then waits for the JS meta.
func growStaggered(t *testing.T, names, stores []string) []*natsserver.Server {
	t.Helper()
	n := len(names)
	clusterPorts, routes := growClusterPortsAndRoutes(t, n)
	servers := make([]*natsserver.Server, n)
	for i := 0; i < n; i++ {
		servers[i] = growStartClusteredNoWait(t, names[i], stores[i], clusterPorts[i], routes)
		if !servers[i].ReadyForConnections(30 * time.Second) {
			t.Fatalf("server %s not ready", names[i])
		}
		time.Sleep(2 * time.Second) // one-at-a-time gap (a real rolling restart is sequential)
	}
	growAwaitReadyAndMeta(t, servers)
	return servers
}

func growClusterPortsAndRoutes(t *testing.T, n int) ([]int, []*url.URL) {
	t.Helper()
	ports := make([]int, n)
	strs := make([]string, n)
	for i := range ports {
		ports[i] = growFreePort(t)
		strs[i] = fmt.Sprintf("nats://127.0.0.1:%d", ports[i])
	}
	return ports, natsserver.RoutesFromStr(strings.Join(strs, ","))
}

func growStartClusteredNoWait(t *testing.T, name, store string, clusterPort int, routes []*url.URL) *natsserver.Server {
	t.Helper()
	s, err := natsserver.NewServer(growClusteredOpts(name, store, clusterPort, routes))
	if err != nil {
		t.Fatalf("NewServer(%s): %v", name, err)
	}
	go s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s
}

// growAwaitReadyAndMeta waits for all servers ready, routes meshed, and a single JS meta leader
// with the complete peer set (the d5 readiness recipe — server-level, no client context).
func growAwaitReadyAndMeta(t *testing.T, servers []*natsserver.Server) {
	t.Helper()
	n := len(servers)
	for _, s := range servers {
		if !s.ReadyForConnections(30 * time.Second) {
			t.Fatalf("server %s not ready", s.Name())
		}
	}
	for _, s := range servers {
		if !growWait(25*time.Second, func() bool { return s.NumRoutes() >= n-1 }) {
			t.Fatalf("server %s did not mesh (NumRoutes<%d)", s.Name(), n-1)
		}
	}
	if !growWait(40*time.Second, func() bool {
		leaders, complete := 0, false
		for _, s := range servers {
			if !s.JetStreamIsClustered() {
				return false
			}
			if s.JetStreamIsLeader() {
				leaders++
				if len(s.JetStreamClusterPeers()) >= n {
					complete = true
				}
			}
		}
		return leaders == 1 && complete
	}) {
		t.Fatalf("FINDING: %d-node clustered JS meta-group did not form (no single leader with complete peers)", n)
	}
}

// ---- per-call FRESH-context client helpers (a shared/expiring context caused an earlier false
// "wedge"; every JS call here gets its own short context) ----

func growAccountInfoOK(js jetstream.JetStream) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := js.AccountInfo(ctx)
	return err == nil
}

func growStreamPresent(js jetstream.JetStream, name string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := js.Stream(ctx, name)
	return err == nil
}

func growSeedStream(t *testing.T, js jetstream.JetStream, name, subj string, msgs int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: name, Subjects: []string{subj}, Replicas: 1, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("seed stream %s: %v", name, err)
	}
	for i := 0; i < msgs; i++ {
		if _, err := js.Publish(ctx, subj, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("seed publish %s: %v", name, err)
		}
	}
}

// ---- option builders + low-level helpers ----

func growStandaloneOpts(name, store string) *natsserver.Options {
	return &natsserver.Options{
		Host: "127.0.0.1", Port: -1, ServerName: name,
		JetStream: true, JetStreamMaxMemory: 1 << 30, JetStreamMaxStore: 16 << 30,
		StoreDir: store, NoLog: true, NoSigs: true,
	}
}

func growClusteredOpts(name, store string, clusterPort int, routes []*url.URL) *natsserver.Options {
	o := growStandaloneOpts(name, store)
	o.Cluster = natsserver.ClusterOpts{Name: "tether-grow", Host: "127.0.0.1", Port: clusterPort}
	o.Routes = routes
	return o
}

func growFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return p
}

func growStartNats(t *testing.T, o *natsserver.Options) *natsserver.Server {
	t.Helper()
	s, err := natsserver.NewServer(o)
	if err != nil {
		t.Fatalf("NewServer(%s): %v", o.ServerName, err)
	}
	go s.Start()
	if !s.ReadyForConnections(20 * time.Second) {
		t.Fatalf("server %s not ready", o.ServerName)
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s
}

func growConnect(t *testing.T, u string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(u, nats.Timeout(5*time.Second), nats.RetryOnFailedConnect(true), nats.MaxReconnects(20))
	if err != nil {
		t.Fatalf("connect %s: %v", u, err)
	}
	return nc
}

func growWait(within time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return pred()
}
