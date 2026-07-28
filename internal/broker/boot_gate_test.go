package broker

import (
	"database/sql"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/storage"
)

// r10_boot_gate_test.go — the internal half of R10 P2/P4.
//
// P2's chain ends at a predicate that lives in this package and is unexported. cmd/tether proves
// "restore --config makes DetectClusterMode say CLUSTER"; this file proves the other half — that a
// cluster-seeded DB with clusterMode=false really is the boot FATAL that made runbook §5.2
// impossible to execute. Neither test asserts its own oracle.

// seededClusterDB builds the DB shape `cluster init` / `cluster recovery restore` leave behind:
// cluster_meta.applied_index + a cluster_nodes self row. That is exactly what clusterDBSeeded looks
// for, so it is the input that makes the missing-seam case fatal.
func seededClusterDB(t *testing.T) *sql.DB {
	t.Helper()
	p := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.OpenWAL("file:" + p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,join_nonce,join_sig)
		 VALUES('brk-a','broker-a','Ident-a','brk-a','10.0.0.1:7400','nats://10.0.0.1:6222','10.0.0.1:7000','a.example','sha256:x','VOTER','2026-07-18 00:00:00 +0000 UTC','N','S')`); err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"self_node_id", "brk-a"}, {"applied_index", "9001"}} {
		if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?)`, kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestSeededDBWithoutTheSeamIsTheBootFatal pins BOTH arms of the predicate the P2 seam controls.
// Only the pair is meaningful: an implementation that always errored, or never errored, would pass
// one arm each.
func TestSeededDBWithoutTheSeamIsTheBootFatal(t *testing.T) {
	db := seededClusterDB(t)

	// No seam in broker.yaml ⇒ ClusterDataDir == "" ⇒ clusterMode == false ⇒ FATAL. This is
	// verbatim what a restored host did before R10 P2, on every single boot.
	err := assertClusterDBConsistent(db, false)
	if err == nil {
		t.Fatal("a cluster-seeded DB in single mode MUST be fatal — otherwise the P2 seam is pointless")
	}
	if !strings.Contains(err.Error(), "refusing to silently downgrade") {
		t.Errorf("unexpected fatal text (the drill/runbook match on it): %v", err)
	}
	if !strings.Contains(err.Error(), "broker.cluster.data_dir") {
		t.Errorf("the fatal must name the missing seam field: %v", err)
	}

	// Seam present ⇒ clusterMode == true ⇒ the daemon starts. This is the post-R10 state.
	if err := assertClusterDBConsistent(db, true); err != nil {
		t.Fatalf("with the seam applied the same DB must start cleanly, got: %v", err)
	}
}

// TestDetectClusterModeKeysOnTheSeamDataDir pins the link cmd/tether's test depends on: the ONLY
// thing that turns cluster mode on is a non-empty broker.cluster.data_dir carrying raft state. A
// partial seam (raft_addr but no data_dir) boots SINGLE — which is why the runbook listing only
// three seam fields was itself a defect (D2).
func TestDetectClusterModeKeysOnTheSeamDataDir(t *testing.T) {
	if on, err := DetectClusterMode(""); err != nil || on {
		t.Fatalf("empty data_dir must mean single mode, got on=%v err=%v", on, err)
	}
	dir := t.TempDir()
	if on, err := DetectClusterMode(dir); err == nil || on {
		t.Fatalf("data_dir set but no raft state must be a loud error, got on=%v err=%v", on, err)
	}
	// With REAL raft state present it is cluster mode. Bootstrap it the way `cluster recovery
	// restore` does (a fabricated raft.db would only prove the file-exists half of the probe).
	if err := cluster.BootstrapSingleNode(dir, "brk-a", "127.0.0.1:7400", slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if on, err := DetectClusterMode(dir); err != nil || !on {
		t.Fatalf("data_dir + raft state must mean cluster mode, got on=%v err=%v", on, err)
	}
}

// TestDeClusterRemedyIsOneSSOT (R10 P4) — the remedy sentence is emitted from three places: the N=1
// boot FATAL below, the DATA-PLANE-DEGRADED status banner, and `cluster recovery restore`'s
// completion text. Until R10 those were three hand-copied literals. The finding was that the product
// "knew exactly what to say and only said it too late"; three copies is how the late one gets fixed
// and the early one rots. Pin that the boot FATAL is built from the shared constants.
func TestDeClusterRemedyIsOneSSOT(t *testing.T) {
	src, err := os.ReadFile("broker.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, "natsconf.DeClusterRemedyCmd") {
		t.Error("the N=1 boot FATAL must build its remedy from natsconf.DeClusterRemedyCmd, not a private copy")
	}
	if strings.Contains(body, "reconcile nats --to-standalone --confirm-single --server-name") {
		t.Error("broker.go re-introduced a hand-copied remedy literal — use the natsconf SSOT")
	}
	st, err := os.ReadFile("clusterstatus.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(st), "reconcile nats --to-standalone --confirm-single --server-name") {
		t.Error("clusterstatus.go re-introduced a hand-copied remedy literal — use the natsconf SSOT")
	}
	// And the constant itself still names the verb + both arguments an operator must supply.
	for _, tok := range []string{"--to-standalone", "--confirm-single", "--server-name", "--broker-nkey"} {
		if !strings.Contains(natsconf.DeClusterRemedyCmd, tok) {
			t.Errorf("DeClusterRemedyCmd lost %q — it is no longer runnable", tok)
		}
	}
}
