package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/proto"
)

func b4QueryRebuild(t *testing.T, db *sql.DB, port int) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`SELECT rebuild_on_failure FROM port_allocations WHERE port=?`, port).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func b4PortRowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func b4InsertRoster(t *testing.T, db *sql.DB, nodeID, phase, certFP string) {
	t.Helper()
	// nats_server_id is set (the §6.5 SSOT is always populated in production); LookupByNodeID
	// scans it into a plain string, so a NULL would error the lookup and mask the eligibility
	// predicate under test.
	if _, err := db.Exec(`INSERT INTO cluster_nodes
		(node_id, name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr, public_host, cert_fp, phase, added_at)
		VALUES(?,?,'pub',?,'127.0.0.1:7400','127.0.0.1:6222','127.0.0.1:7000','localhost',?,?, '2026-01-01T00:00:00Z')`,
		nodeID, nodeID, "srv-"+nodeID, certFP, phase); err != nil {
		t.Fatalf("seed roster %s: %v", nodeID, err)
	}
}

// origin: b4_expose_test.go (renamed in B6) — docs/reviews/b4-plan.md, docs/reviews/b4-review.md
//
// TestB4OldShapedExposeReqRebuildsOn (plan §F.10): a literal pre-B4 expose body decodes with
// RebuildOff=false and, driven through the single-mode allocate path, persists
// rebuild_on_failure=1 — proving a dropped flag fails toward rebuild-ON (the safe direction),
// end-to-end (decode → persist), not just at the marshal layer.
func TestB4OldShapedExposeReqRebuildsOn(t *testing.T) {
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now, PortBandLow: 14000, PortBandHigh: 14010}}
	actor := freshUserActor(t)
	seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}

	var req proto.ExposeReq
	if err := json.Unmarshal([]byte(`{"name":"j","local_port":8888}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.RebuildOff || req.OnBroker != "" {
		t.Fatalf("pre-B4 body must decode to RebuildOff=false/OnBroker='': %+v", req)
	}
	alloc, err := b.allocatePort("lab", "lab-1", req.Name, req.LocalPort, req.RemotePort, req.RebuildOff, req.OnBroker, fp)
	if err != nil {
		t.Fatal(err)
	}
	if got := b4QueryRebuild(t, b.cfg.DB, alloc.Port); got != 1 {
		t.Fatalf("old-shaped expose persisted rebuild_on_failure=%d, want 1 (rebuild ON)", got)
	}
}

// TestB4OnBrokerRejectionsWriteNoRow (plan §F.11): every --on-broker rejection cause returns the
// typed sentinel AND writes NO port_allocations row (validation aborts before any raft Propose):
//   - single mode + --on-broker                 → errOnBrokerSingleMode
//   - cluster mode, unknown node                 → errOnBrokerUnknown
//   - cluster mode, non-VOTER phase node         → errOnBrokerUnknown
//   - cluster mode, VOTER with empty cert pin    → errOnBrokerUnknown
//
// The positive "VOTER → pin written" path needs a real raft node and is covered in test/d6.
func TestB4OnBrokerRejectionsWriteNoRow(t *testing.T) {
	actor := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}

	// Single mode: --on-broker is meaningless.
	bs := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now, PortBandLow: 14000, PortBandHigh: 14010}}
	seedD8TransferMemberNode(t, bs, actor, "lab", "lab-1")
	if _, err := bs.allocatePort("lab", "lab-1", "j", 8888, 0, false, "brk-x", fp); !errors.Is(err, errOnBrokerSingleMode) {
		t.Fatalf("single-mode --on-broker: want errOnBrokerSingleMode, got %v", err)
	}
	if n := b4PortRowCount(t, bs.cfg.DB); n != 0 {
		t.Fatalf("single-mode rejection must write no row, got %d", n)
	}

	// Cluster mode: ineligible targets.
	bc := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now, PortBandLow: 14000, PortBandHigh: 14010}, clusterMode: true}
	seedD8TransferMemberNode(t, bc, actor, "lab", "lab-1")
	b4InsertRoster(t, bc.cfg.DB, "brk-catchup", phaseCatchingUp, "sha256:cc") // non-VOTER
	b4InsertRoster(t, bc.cfg.DB, "brk-nocert", phaseVoter, "")                // VOTER, no cert pin
	// D1 (Stage-C): a VOTER (Eligible) + cert-pinned node that carries a broker_draining marker
	// must STILL be rejected — the drain marker precedes the phase->DRAINING flip.
	b4InsertRoster(t, bc.cfg.DB, "brk-draining", phaseVoter, "sha256:dd")
	if _, err := bc.cfg.DB.Exec(`INSERT INTO cluster_meta(key, value) VALUES('draining:brk-draining','')`); err != nil {
		t.Fatal(err)
	}
	for _, onb := range []string{"brk-missing", "brk-catchup", "brk-nocert", "brk-draining"} {
		if _, err := bc.allocatePort("lab", "lab-1", "j", 8888, 0, false, onb, fp); !errors.Is(err, errOnBrokerUnknown) {
			t.Fatalf("--on-broker %s: want errOnBrokerUnknown, got %v", onb, err)
		}
	}
	if n := b4PortRowCount(t, bc.cfg.DB); n != 0 {
		t.Fatalf("cluster-mode rejection must write no row, got %d", n)
	}
}

// TestB4OnBrokerPredicateSense (Stage-C m2-b): a cheap lock on the SENSE of the --on-broker
// eligibility predicate without a real raft node (the positive allocate path needs Propose →
// test/d6). A healthy VOTER + cert-pinned + non-draining node satisfies the exact predicate
// allocatePort uses; the reject fixtures do not. A future inversion (e.g. `CertFP == ""`) would
// flip this and fail here, where every rejection-only test would still pass.
func TestB4OnBrokerPredicateSense(t *testing.T) {
	db := openDB(t)
	b4InsertRoster(t, db, "brk-good", phaseVoter, "sha256:ok")
	b4InsertRoster(t, db, "brk-catchup", phaseCatchingUp, "sha256:cc")
	b4InsertRoster(t, db, "brk-nocert", phaseVoter, "")

	eligible := func(nodeID string) bool {
		home, err := clusternodes.LookupByNodeID(db, nodeID)
		if err != nil {
			return false
		}
		draining, _ := cluster.DrainingNodes(db)
		return home.Eligible() && home.CertFP != "" && !slices.Contains(draining, nodeID)
	}
	if !eligible("brk-good") {
		t.Error("a healthy VOTER+cert node must satisfy the --on-broker predicate (sense inverted?)")
	}
	for _, bad := range []string{"brk-catchup", "brk-nocert", "brk-missing"} {
		if eligible(bad) {
			t.Errorf("%s must NOT satisfy the --on-broker predicate", bad)
		}
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key, value) VALUES('draining:brk-good','')`); err != nil {
		t.Fatal(err)
	}
	if eligible("brk-good") {
		t.Error("a draining VOTER must NOT satisfy the --on-broker predicate")
	}
}
