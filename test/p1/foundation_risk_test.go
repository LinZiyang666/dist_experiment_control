package p1_test

import (
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func freshAccountSeed(t *testing.T) []byte {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		kp.Wipe()
		t.Fatalf("Seed: %v", err)
	}
	out := append([]byte(nil), seed...)
	kp.Wipe()
	return out
}

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

func TestIssueUserJWTRejectsEmptySubjectWithoutPanic(t *testing.T) {
	signer, err := auth.LoadAccountSigner(freshAccountSeed(t))
	if err != nil {
		t.Fatalf("LoadAccountSigner: %v", err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("IssueUserJWT panicked for empty user public key: %v", r)
		}
	}()

	if _, err := signer.IssueUserJWT("", jwt.Permissions{}, 0); err == nil {
		t.Fatal("IssueUserJWT must reject an empty user public key")
	}
}

func TestStorageForeignKeysHoldAcrossPooledConnections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(2)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin held connection: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = db.Exec(
		`INSERT INTO members(sid, pubkey_fp, role, via) VALUES (?,?,?,?)`,
		"missing-session", "SHA256:member", "member", "pin",
	)
	if err == nil {
		t.Fatal("foreign-key violation was accepted on a pooled connection")
	}
}
