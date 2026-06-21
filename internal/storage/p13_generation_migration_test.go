package storage

import (
	"testing"
)

// Round-5 F2: proxy_meta is delivered by a FORWARD migration (0007), not by
// amending the already-applied 0006. A DB that recorded 0006 but not 0007 must
// still receive proxy_meta when migrations re-run — otherwise the broker can't
// obtain a durable fencing generation and (correctly) refuses to start.
func TestProxyGenerationForwardMigrationAppliesOnExisting0006DB(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Simulate a DB that applied through 0006 but never saw 0007: forget the
	// 0007 record and drop the table it created.
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version = '0007_proxy_generation.sql'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE proxy_meta`); err != nil {
		t.Fatal(err)
	}

	// Re-running migrations (what every broker start does) must re-apply 0007.
	if err := ApplyMigrations(db); err != nil {
		t.Fatalf("re-running migrations on a 0006-only DB failed: %v", err)
	}

	var gen int64
	if err := db.QueryRow(`SELECT generation FROM proxy_meta WHERE id = 1`).Scan(&gen); err != nil {
		t.Fatalf("proxy_meta not delivered by forward migration: %v", err)
	}
	if gen != 0 {
		t.Fatalf("fresh proxy_meta generation = %d, want 0", gen)
	}
}
