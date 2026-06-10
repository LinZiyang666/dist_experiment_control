package port

import (
	"errors"
	"testing"
)

func TestAllocateProxyOnePerNodeReuseOrReplace(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, last_heartbeat_at, status) VALUES (?, ?, ?, ?)`,
		"lab", "lab-2", nil, "ONLINE",
	); err != nil {
		t.Fatalf("seed lab-2: %v", err)
	}

	a1, err := AllocateProxy(db, "lab", "lab-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a1.Name != ProxyPortName || a1.Token == "" || a1.CreatedByFP != "" {
		t.Fatalf("unexpected proxy alloc: %+v", a1)
	}

	// A second node in the same session gets its own proxy port — the
	// shared reserved name must NOT collide (per-(sid,nid) uniqueness).
	a2, err := AllocateProxy(db, "lab", "lab-2", nil)
	if err != nil {
		t.Fatalf("second node proxy alloc must not collide on name: %v", err)
	}
	if a2.Port == a1.Port {
		t.Fatal("two nodes must get distinct proxy ports")
	}

	// Re-allocating for lab-1 replaces (frees old, mints fresh token); still
	// exactly one ALLOCATED __proxy__ row for lab-1.
	a1b, err := AllocateProxy(db, "lab", "lab-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a1b.Token == a1.Token {
		t.Fatal("replace must mint a fresh token")
	}
	var active int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE sid='lab' AND nid='lab-1' AND name=? AND state='ALLOCATED'`,
		ProxyPortName,
	).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("want exactly one ALLOCATED proxy row for lab-1, got %d", active)
	}

	// The replaced allocation is reachable by its fresh token hash.
	got, err := LookupProxyByNode(db, "lab", "lab-1")
	if err != nil || got.Port != a1b.Port {
		t.Fatalf("lookup after replace: %v port=%v", err, got)
	}
}

func TestLookupProxyByNodeNotFound(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	if _, err := LookupProxyByNode(db, "lab", "lab-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
