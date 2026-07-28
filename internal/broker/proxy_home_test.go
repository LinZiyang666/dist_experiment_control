package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// g7_proxy_home_test.go — G7a #2: the DEFAULT `proxy status` view (non---cluster) must label each exit
// with its OWN home broker's public_host (where /sub points subscribers), not the answering broker's
// host, and must gate the vended port+host on the home's health so it agrees with the /sub render.
// Pre-G7a, proxyStatusNodes unconditionally used publicHostFor() (the answering broker) and never even
// SELECTed home_broker — the field bug that mislabeled pc732-homed exits as linziyang.top:1400x.

// seedClusterNodeHost inserts one cluster_nodes row with an EXPLICIT public_host (the shared
// seedClusterNode hard-codes "pub.example" for everyone, which cannot prove per-home rendering).
func seedClusterNodeHost(t *testing.T, b *Broker, nodeID, publicHost, certFP, phase string) {
	t.Helper()
	if _, err := b.cfg.DB.Exec(
		`INSERT OR REPLACE INTO cluster_nodes
		   (node_id, name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr, public_host, cert_fp, phase, added_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		nodeID, nodeID, "Nident", nodeID, "127.0.0.1:7400", "nats://127.0.0.1:6222",
		"127.0.0.1:7443", publicHost, certFP, phase, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed cluster_node %s: %v", nodeID, err)
	}
}

// origin: g7_proxy_home_test.go (renamed in B6)
//
// TestG7DefaultProxyStatusRendersHomeHost is the core #2 assertion: a proxy homed on a REMOTE healthy
// voter is rendered with that voter's public_host, not the answering broker's, in the DEFAULT view.
func TestG7DefaultProxyStatusRendersHomeHost(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "a.example"}, selfID: "brk-a"}
	b.clusterMode = true
	seedClusterNodeHost(t, b, "brk-a", "a.example", "sha256:a", "VOTER")
	seedClusterNodeHost(t, b, "brk-b", "b.example", "sha256:b", "VOTER")
	// proxy for lab/lab-1 is homed on the REMOTE voter brk-b; brk-a is answering.
	seedHomedExpose(t, b, "lab", "lab-1", "__proxy__", 14001, "th1", "brk-b", 1)

	nodes, err := b.proxyStatusNodes("lab")
	if err != nil {
		t.Fatalf("proxyStatusNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("want 1 node, got %d", len(nodes))
	}
	e := nodes[0]
	if e.PublicHost != "b.example" {
		t.Fatalf("default view must render the HOME broker's host: PublicHost=%q, want b.example (NOT the answering a.example)", e.PublicHost)
	}
	if e.PublicPort != 14001 {
		t.Fatalf("healthy home must vend the port: PublicPort=%d, want 14001", e.PublicPort)
	}
	if e.HomeBroker != "brk-b" {
		t.Fatalf("HomeBroker=%q, want brk-b", e.HomeBroker)
	}
}

// TestG7DefaultProxyStatusSelfHome: a proxy homed on the answering (self) voter renders self's host.
func TestG7DefaultProxyStatusSelfHome(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "a.example"}, selfID: "brk-a"}
	b.clusterMode = true
	seedClusterNodeHost(t, b, "brk-a", "a.example", "sha256:a", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "__proxy__", 14002, "th2", "brk-a", 1)

	nodes, err := b.proxyStatusNodes("lab")
	if err != nil {
		t.Fatalf("proxyStatusNodes: %v", err)
	}
	if nodes[0].PublicHost != "a.example" || nodes[0].PublicPort != 14002 {
		t.Fatalf("self-homed exit: host=%q port=%d, want a.example:14002", nodes[0].PublicHost, nodes[0].PublicPort)
	}
}

// TestG7DefaultProxyStatusUnhealthyHomeVendsNothing: an exit whose home broker is NOT deliverable
// (public_host empty here) must show neither port nor host — matching /sub (which excludes it) and the
// --cluster view — so the default status never advertises an unreachable exit at a wrong host.
func TestG7DefaultProxyStatusUnhealthyHomeVendsNothing(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "a.example"}, selfID: "brk-a"}
	b.clusterMode = true
	seedClusterNodeHost(t, b, "brk-a", "a.example", "sha256:a", "VOTER")
	seedClusterNodeHost(t, b, "brk-b", "", "sha256:b", "VOTER") // no public_host → not deliverable
	seedHomedExpose(t, b, "lab", "lab-1", "__proxy__", 14003, "th3", "brk-b", 1)

	nodes, err := b.proxyStatusNodes("lab")
	if err != nil {
		t.Fatalf("proxyStatusNodes: %v", err)
	}
	e := nodes[0]
	if e.PublicPort != 0 || e.PublicHost != "" {
		t.Fatalf("unhealthy home must vend nothing: port=%d host=%q, want 0/\"\"", e.PublicPort, e.PublicHost)
	}
	if e.HomeBroker != "brk-b" {
		t.Fatalf("HomeBroker should still be reported for diagnosis: %q, want brk-b", e.HomeBroker)
	}
}

// TestG7DefaultProxyStatusReadyGatedOnHomeHealth (C6 audit): the default view's Ready must AGREE with the
// home-gated port/host. An exit whose home broker is unhealthy must be Ready=false EVEN WITH proxy_ready=1
// (the raw column the pre-C6 code trusted unconditionally), so no self-contradictory Ready=true+empty-endpoint
// row is rendered; a healthy home with proxy_ready=1 stays Ready=true.
func TestG7DefaultProxyStatusReadyGatedOnHomeHealth(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "a.example"}, selfID: "brk-a"}
	b.clusterMode = true
	seedClusterNodeHost(t, b, "brk-a", "a.example", "sha256:a", "VOTER") // healthy, deliverable home (self)
	seedClusterNodeHost(t, b, "brk-b", "", "sha256:b", "VOTER")          // unhealthy home (no public_host)
	seedHomedExpose(t, b, "lab", "healthy-1", "__proxy__", 14001, "t1", "brk-a", 1)
	seedHomedExpose(t, b, "lab", "unhealthy-1", "__proxy__", 14002, "t2", "brk-b", 1)
	// both agents report proxy_ready=1 — the raw liveness the pre-C6 code surfaced as Ready without the gate.
	if _, err := db.Exec(`UPDATE nodes SET proxy_ready=1 WHERE sid='lab'`); err != nil {
		t.Fatalf("set proxy_ready: %v", err)
	}

	nodes, err := b.proxyStatusNodes("lab")
	if err != nil {
		t.Fatalf("proxyStatusNodes: %v", err)
	}
	byNID := map[string]proto.ProxyNodeEntry{}
	for _, e := range nodes {
		byNID[e.NID] = e
	}
	if h, ok := byNID["healthy-1"]; !ok || !h.Ready || h.PublicPort == 0 {
		t.Fatalf("C6: a healthy home with proxy_ready=1 must stay Ready with a vended port: %+v", h)
	}
	if u, ok := byNID["unhealthy-1"]; !ok || u.Ready || u.PublicPort != 0 {
		t.Fatalf("C6: an unhealthy home must be Ready=false and vend no port EVEN with proxy_ready=1: %+v", u)
	}
}

// TestG7DefaultProxyStatusSingleModeByteIdentical: with clusterMode off (single broker) there is no home
// concept — home_broker is ” and the entry stays byte-identical to pre-G7a (publicHostFor()).
func TestG7DefaultProxyStatusSingleModeByteIdentical(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "solo.example"}, selfID: ""}
	b.clusterMode = false
	// single-mode expose has no home_broker (migration-0010 default '').
	seedHomedExpose(t, b, "lab", "lab-1", "__proxy__", 14004, "th4", "", 0)

	nodes, err := b.proxyStatusNodes("lab")
	if err != nil {
		t.Fatalf("proxyStatusNodes: %v", err)
	}
	e := nodes[0]
	if e.PublicHost != "solo.example" || e.PublicPort != 14004 {
		t.Fatalf("single mode must stay byte-identical: host=%q port=%d, want solo.example:14004", e.PublicHost, e.PublicPort)
	}
	if e.HomeBroker != "" {
		t.Fatalf("single mode must not surface a HomeBroker: %q", e.HomeBroker)
	}
}

// TestG7DefaultVsClusterViewHostAgree (render-equivalence, scoped to the /sub-rendered exit): for a
// healthy-home exit the DEFAULT view and the --cluster view must project the SAME public host, so
// `proxy status` and `proxy status --cluster` never disagree about where an exit egresses.
func TestG7DefaultVsClusterViewHostAgree(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), PublicHost: "a.example"}, selfID: "brk-a"}
	b.clusterMode = true
	seedClusterNodeHost(t, b, "brk-a", "a.example", "sha256:a", "VOTER")
	seedClusterNodeHost(t, b, "brk-b", "b.example", "sha256:b", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "__proxy__", 14005, "th5", "brk-b", 1)
	// the --cluster view only lists proxy_capable agents (the ones /sub renders); mark ours so both
	// views return the same node and the host projection can be compared.
	if _, err := db.Exec(`UPDATE nodes SET proxy_capable=1 WHERE sid=? AND nid=?`, "lab", "lab-1"); err != nil {
		t.Fatalf("mark proxy_capable: %v", err)
	}

	def, err := b.proxyStatusNodes("lab")
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	clu, err := b.proxyStatusNodesCluster("lab")
	if err != nil {
		t.Fatalf("cluster: %v", err)
	}
	if def[0].PublicHost != clu[0].PublicHost {
		t.Fatalf("default and --cluster host disagree for the same exit: default=%q cluster=%q", def[0].PublicHost, clu[0].PublicHost)
	}
	if def[0].PublicHost != "b.example" {
		t.Fatalf("both views should render the home host b.example, got %q", def[0].PublicHost)
	}
}
