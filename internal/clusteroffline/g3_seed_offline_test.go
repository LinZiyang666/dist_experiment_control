package clusteroffline

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// g3_seed_offline_test.go — G3 #1 offline drop-only seed convergence. pruneRosterPeers now also drops the
// departed peers' client endpoints from the published seeds (in the SAME txn) + MAX-floor bumps
// seed_generation, so the offline force-single path leaves the SAME converged client view as the online
// path. Invariants pinned: drop-departed, VIP preservation (a non-peer host is kept), empty-set floor
// (never wipe), and monotone seed_generation.

func insertNodeHost(t *testing.T, dbPath, nodeID, host string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) `+
			`VALUES(?,?,'Upub','tether-x','127.0.0.1:1','nats://x','x:7000',?,'sha256:ab','VOTER','2026-06-23 00:00:00 +0000 UTC')`,
		nodeID, nodeID, host); err != nil {
		t.Fatalf("insert %s: %v", nodeID, err)
	}
}

func setMeta(t *testing.T, dbPath, key, value string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value); err != nil {
		t.Fatalf("set meta %s: %v", key, err)
	}
}

func metaText(t *testing.T, dbPath, key string) string {
	t.Helper()
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&s); err != nil {
		return ""
	}
	return s
}

func TestPruneRosterPeersDropsDepartedSeeds(t *testing.T) {
	_, dbPath := mustDataDir(t)
	insertNodeHost(t, dbPath, "self", "self.example")
	insertNodeHost(t, dbPath, "dead-1", "dead1.example")
	// seeds carry both broker endpoints + a VIP the operator floored in.
	setMeta(t, dbPath, cluster.MetaKeySeedEndpoints,
		strings.Join([]string{"wss://self.example:443", "wss://dead1.example:443", "wss://vip.lb:443"}, "\n"))
	setMeta(t, dbPath, cluster.MetaKeySeedGeneration, "100")
	now := time.Date(2026, 7, 6, 3, 0, 0, 0, time.UTC)

	if err := pruneRosterPeers(dbPath, []Peer{{NodeID: "dead-1"}}, now, nil); err != nil {
		t.Fatalf("pruneRosterPeers: %v", err)
	}
	// m17 (atomicity): ONE pruneRosterPeers call removes the roster row AND drops its seed endpoint — both
	// land as a single observable state (they share one txn; a refactor that split them would break this).
	if rowCount(t, dbPath, "dead-1") != 0 {
		t.Error("m17: departed peer's roster row must be deleted in the same call")
	}
	got := metaText(t, dbPath, cluster.MetaKeySeedEndpoints)
	if strings.Contains(got, "dead1.example") {
		t.Errorf("departed peer's endpoint not dropped from seeds: %q", got)
	}
	if !strings.Contains(got, "self.example") || !strings.Contains(got, "vip.lb") {
		t.Errorf("survivor + VIP endpoints must be kept: %q", got)
	}
	if metaUint(t, dbPath, cluster.MetaKeySeedGeneration) <= 100 {
		t.Errorf("seed_generation must advance monotonically after a seed drop (was 100, got %d)",
			metaUint(t, dbPath, cluster.MetaKeySeedGeneration))
	}
}

func TestPruneRosterPeersSeedEmptyFloor(t *testing.T) {
	// If dropping the departed host would EMPTY the seed set, keep the stored set (never wipe / strand a
	// cold-start client) and do NOT bump seed_generation.
	_, dbPath := mustDataDir(t)
	insertNodeHost(t, dbPath, "self", "self.example")
	insertNodeHost(t, dbPath, "dead-1", "dead1.example")
	setMeta(t, dbPath, cluster.MetaKeySeedEndpoints, "wss://dead1.example:443") // ONLY the dead host
	setMeta(t, dbPath, cluster.MetaKeySeedGeneration, "100")
	now := time.Date(2026, 7, 6, 3, 0, 0, 0, time.UTC)

	if err := pruneRosterPeers(dbPath, []Peer{{NodeID: "dead-1"}}, now, nil); err != nil {
		t.Fatalf("pruneRosterPeers: %v", err)
	}
	if got := metaText(t, dbPath, cluster.MetaKeySeedEndpoints); got != "wss://dead1.example:443" {
		t.Errorf("empty-set floor violated: seeds wiped/changed to %q (must keep stored)", got)
	}
	if g := metaUint(t, dbPath, cluster.MetaKeySeedGeneration); g != 100 {
		t.Errorf("seed_generation must NOT bump when the drop is suppressed by the empty-set floor: 100 -> %d", g)
	}
}

func TestPruneRosterPeersAllDeadWarns(t *testing.T) {
	// A8: when the drop would EMPTY the seed set (every published seed points at a departed broker), the
	// empty-set floor keeps the stale set — but it must now WARN loudly (parity with the online sibling
	// deriveAndConvergeSeedsFromRoster), since the survivor otherwise silently advertises only dead endpoints.
	_, dbPath := mustDataDir(t)
	insertNodeHost(t, dbPath, "self", "self.example")
	insertNodeHost(t, dbPath, "dead-1", "dead1.example")
	setMeta(t, dbPath, cluster.MetaKeySeedEndpoints, "wss://dead1.example:443") // ONLY the dead host
	now := time.Date(2026, 7, 6, 3, 0, 0, 0, time.UTC)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := pruneRosterPeers(dbPath, []Peer{{NodeID: "dead-1"}}, now, logger); err != nil {
		t.Fatalf("pruneRosterPeers: %v", err)
	}
	if !strings.Contains(buf.String(), "only dead endpoints") {
		t.Errorf("A8: an all-dead seed drop must WARN loudly; log was %q", buf.String())
	}
	// The empty-set floor still holds: the stale set is kept (never wiped).
	if got := metaText(t, dbPath, cluster.MetaKeySeedEndpoints); got != "wss://dead1.example:443" {
		t.Errorf("empty-set floor: the stale set must be kept, got %q", got)
	}
}

func TestPruneRosterPeersNoSeedsNoOp(t *testing.T) {
	// No seeds published → the seed convergence is a clean no-op (roster/topology still bump).
	_, dbPath := mustDataDir(t)
	insertNodeHost(t, dbPath, "self", "self.example")
	insertNodeHost(t, dbPath, "dead-1", "dead1.example")
	now := time.Date(2026, 7, 6, 3, 0, 0, 0, time.UTC)
	if err := pruneRosterPeers(dbPath, []Peer{{NodeID: "dead-1"}}, now, nil); err != nil {
		t.Fatalf("pruneRosterPeers: %v", err)
	}
	if metaText(t, dbPath, cluster.MetaKeySeedEndpoints) != "" {
		t.Error("no seeds were published; convergence must not create any")
	}
}
