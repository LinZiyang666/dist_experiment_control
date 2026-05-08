package p1_test

import (
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/storage"
)

func TestAccountSignerRejectsUserSeed(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("GenerateUserSeed: %v", err)
	}

	signer, err := auth.LoadAccountSigner(seed)
	if err == nil {
		pub, _ := signer.AccountPublicKey()
		t.Fatalf("LoadAccountSigner accepted a user seed as an account signer; derived public key %q", pub)
	}
}

func TestStorageForeignKeysHoldAcrossPooledConnections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(2)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin held connection: %v", err)
	}
	defer tx.Rollback()

	_, err = db.Exec(
		`INSERT INTO members(sid, pubkey_fp, role, via) VALUES (?,?,?,?)`,
		"missing-session", "SHA256:member", "member", "pin",
	)
	if err == nil {
		t.Fatal("foreign-key violation was accepted on a pooled connection")
	}
}
