package clusternodes

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func insertNode(t *testing.T, db *sql.DB, nodeID, natsServer, tunnelAddr, certFP string, prev *string, valid *time.Time, phase string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		   (node_id, name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr, public_host, cert_fp, cert_fp_prev, cert_fp_valid_until, phase, added_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		nodeID, nodeID, "Nident", natsServer, "127.0.0.1:7400", "nats://127.0.0.1:6222",
		tunnelAddr, "pub.example", certFP, prev, valid, phase, time.Now().UTC(),
	); err != nil {
		t.Fatalf("insert node: %v", err)
	}
}

// TestD6LookupByNatsServer (R-1/R-3): resolves a row by the agent-reported
// server_name; empty server / no match → ErrNotFound; NULL prev/valid_until →
// "" / nil; eligibility is exposed raw (caller decides).
func TestD6LookupByNatsServer(t *testing.T) {
	db := openDB(t)
	insertNode(t, db, "node-1", "tether-1", "10.0.0.1:7000", "sha256:aaa", nil, nil, "VOTER")

	got, err := LookupByNatsServer(db, "tether-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.NodeID != "node-1" || got.TunnelAddr != "10.0.0.1:7000" || got.CertFP != "sha256:aaa" {
		t.Fatalf("row mismatch: %+v", got)
	}
	if got.CertFPPrev != "" || got.CertValid != nil {
		t.Fatalf("NULL prev/valid must map to zero: prev=%q valid=%v", got.CertFPPrev, got.CertValid)
	}
	if !got.Eligible() {
		t.Fatal("VOTER must be Eligible")
	}

	if _, err := LookupByNatsServer(db, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty server: want ErrNotFound, got %v", err)
	}
	if _, err := LookupByNatsServer(db, "tether-nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no match: want ErrNotFound, got %v", err)
	}
}

// TestD6LookupByNodeID (R-9 rehome-delivery): resolves by home_broker node_id;
// the rotation window columns round-trip (prev + valid_until non-NULL).
func TestD6LookupByNodeID(t *testing.T) {
	db := openDB(t)
	valid := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	prev := "sha256:old"
	insertNode(t, db, "node-2", "tether-2", "10.0.0.2:7000", "sha256:new", &prev, &valid, "VOTER")

	got, err := LookupByNodeID(db, "node-2")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.CertFPPrev != "sha256:old" {
		t.Fatalf("prev fp = %q, want sha256:old", got.CertFPPrev)
	}
	if got.CertValid == nil || !got.CertValid.Equal(valid) {
		t.Fatalf("valid_until = %v, want %v", got.CertValid, valid)
	}

	if _, err := LookupByNodeID(db, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty node: want ErrNotFound, got %v", err)
	}
}

// TestD6PhaseEligibility: only VOTER is home-eligible.
func TestD6PhaseEligibility(t *testing.T) {
	for _, tc := range []struct {
		phase string
		want  bool
	}{
		{"VOTER", true},
		{"DRAINING", false},
		{"CATCHING_UP", false},
		{"RETIRING", false},
		{"JOIN_VERIFIED_PENDING_VOTER", false},
	} {
		n := &HomeNode{Phase: tc.phase}
		if n.Eligible() != tc.want {
			t.Errorf("phase %q Eligible = %v, want %v", tc.phase, n.Eligible(), tc.want)
		}
	}
}
