package proxysub

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:owner", "x",
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return db
}

func TestCreateReturnsRawTokenOnceHashStored(t *testing.T) {
	db := newDB(t)
	now := time.Now()
	s, err := Create(db, "lab", "alice", "SHA256:owner", now)
	if err != nil {
		t.Fatal(err)
	}
	if s.Token == "" {
		t.Fatal("Create must return a raw token")
	}
	if s.TokenHash != HashToken(s.Token) {
		t.Fatal("TokenHash must be SHA256(Token)")
	}
	if s.PSK == "" || s.Cipher == "" {
		t.Fatal("Create must populate psk + cipher")
	}
	// DB stores only the hash, never the raw token.
	var raw int
	if err := db.QueryRow(`SELECT COUNT(*) FROM proxy_subscribers WHERE token_hash=?`, s.Token).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 0 {
		t.Fatal("raw token must not be findable as a stored value")
	}
}

func TestActiveNameUniquenessAndReuseAfterRevoke(t *testing.T) {
	db := newDB(t)
	now := time.Now()
	if _, err := Create(db, "lab", "alice", "SHA256:owner", now); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(db, "lab", "alice", "SHA256:owner", now); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("duplicate active name: want ErrNameTaken, got %v", err)
	}
	if err := Revoke(db, "lab", "alice", now); err != nil {
		t.Fatal(err)
	}
	// After revoke the name is free again.
	if _, err := Create(db, "lab", "alice", "SHA256:owner", now); err != nil {
		t.Fatalf("recreate after revoke: %v", err)
	}
}

func TestRevokeIsolationKeepsOtherPSKs(t *testing.T) {
	db := newDB(t)
	now := time.Now()
	a, _ := Create(db, "lab", "alice", "SHA256:owner", now)
	b, _ := Create(db, "lab", "bob", "SHA256:owner", now)
	c, _ := Create(db, "lab", "carol", "SHA256:owner", now)

	if err := Revoke(db, "lab", "bob", now); err != nil {
		t.Fatal(err)
	}
	active, err := ListActive(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("want 2 active after one revoke, got %d", len(active))
	}
	psks := map[string]string{}
	for _, s := range active {
		psks[s.Name] = s.PSK
	}
	if psks["alice"] != a.PSK || psks["carol"] != c.PSK {
		t.Fatal("revoking bob must not change alice/carol PSKs")
	}
	if _, ok := psks["bob"]; ok {
		t.Fatal("bob must be absent from active set")
	}
	_ = b
}

func TestRevokeErrors(t *testing.T) {
	db := newDB(t)
	now := time.Now()
	if err := Revoke(db, "lab", "ghost", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoke unknown: want ErrNotFound, got %v", err)
	}
	if _, err := Create(db, "lab", "alice", "SHA256:owner", now); err != nil {
		t.Fatal(err)
	}
	if err := Revoke(db, "lab", "alice", now); err != nil {
		t.Fatal(err)
	}
	if err := Revoke(db, "lab", "alice", now); !errors.Is(err, ErrAlreadyRevoked) {
		t.Fatalf("double revoke: want ErrAlreadyRevoked, got %v", err)
	}
}

func TestLookupByTokenHashActiveOnly(t *testing.T) {
	db := newDB(t)
	now := time.Now()
	s, _ := Create(db, "lab", "alice", "SHA256:owner", now)
	h := HashToken(s.Token)

	got, err := LookupByTokenHash(db, "lab", h)
	if err != nil || got.Name != "alice" {
		t.Fatalf("active lookup failed: %v", err)
	}
	sid, err := LookupSIDByTokenHash(db, h)
	if err != nil || sid != "lab" {
		t.Fatalf("sid lookup: sid=%q err=%v", sid, err)
	}

	if err := Revoke(db, "lab", "alice", now); err != nil {
		t.Fatal(err)
	}
	if _, err := LookupByTokenHash(db, "lab", h); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked token must not resolve: %v", err)
	}
	if _, err := LookupSIDByTokenHash(db, h); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked token sid must not resolve: %v", err)
	}
}
