package broker

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// cluster_manifest_test.go (C2) — the security-critical manifest properties: inert in single mode,
// both children account-signed + verifiable against the pin, a swapped account_pub is rejected, and
// the served body NEVER contains a secret.

func newManifestTestBroker(t *testing.T) (b *Broker, accountPub string, seed []byte) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, _ = auth.PublicKeyFromSeed(seed)
	db, err := storage.OpenWAL("file:" + filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('self','self','p','self','10.0.0.1:7400','nats://10.0.0.1:6222','10.0.0.1:7443','b1.example.com','sha256:s','VOTER','2026-06-24 00:00:00 +0000 UTC')`); err != nil {
		t.Fatal(err)
	}
	// Published seeds.
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('seed_endpoints','wss://b1.example.com:443'),('seed_bootstrap','https://c.example.com/.well-known/tether/cluster.json'),('seed_generation','5')`); err != nil {
		t.Fatal(err)
	}
	b = &Broker{selfID: "self"}
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
	b.cfg.DB = db
	b.cfg.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	return b, accountPub, seed
}

func TestManifestInertSingleBroker(t *testing.T) {
	b := &Broker{} // selfID == ""
	if body, ok := b.manifestBytes(); ok || body != nil {
		t.Fatalf("a single broker must serve no manifest, got ok=%v", ok)
	}
}

func TestManifestBothChildrenSignedAndVerifiable(t *testing.T) {
	b, accountPub, _ := newManifestTestBroker(t)
	body, ok := b.manifestBytes()
	if !ok {
		t.Fatal("clustered broker must serve a manifest")
	}
	var m proto.ClusterManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0).Add(time.Minute).UTC()
	if m.Roster == nil || clusterroster.VerifyAt(m.Roster, accountPub, now) != nil {
		t.Fatal("manifest roster must verify against the account pin")
	}
	if m.Seeds == nil || clusterroster.VerifySeedsAt(m.Seeds, accountPub, now) != nil {
		t.Fatal("manifest seeds must verify against the account pin")
	}
	// MITM account-swap: replacing account_pub with an attacker key must fail verification vs the pin.
	m.Roster.AccountPub = "ATTACKER"
	if clusterroster.VerifyAt(m.Roster, accountPub, now) == nil {
		t.Fatal("a swapped account_pub must be rejected against the OOB pin")
	}
}

// TestManifestNoSecrets — the served body must never contain seed/nkey/CA/session-token material.
func TestManifestNoSecrets(t *testing.T) {
	b, accountPub, seed := newManifestTestBroker(t)
	body, ok := b.manifestBytes()
	if !ok {
		t.Fatal("expected a manifest")
	}
	s := string(body)
	// Stage-C m4: grep the ACTUAL signing seed bytes (the strongest guard) + the nkey seed prefixes
	// for both USER (SU) and ACCOUNT (SA) keys + other secret markers.
	if strings.Contains(s, string(seed)) {
		t.Fatal("manifest must NEVER contain the raw account signing seed")
	}
	for _, forbidden := range []string{"SUA", "SUB", "SAA", "SAB", "PRIVATE", "BEGIN ", "account.nk", "session_pin", "\"pin\""} {
		if strings.Contains(s, forbidden) {
			t.Fatalf("manifest must not contain %q: %s", forbidden, s)
		}
	}
	if !strings.Contains(s, accountPub) {
		t.Fatal("manifest should carry the (public) account_pub")
	}
}

// TestManifestServesFromCache — a second call returns the cached bytes without re-signing (the GET hot
// path); identical bytes within the recheck window.
func TestManifestServesFromCache(t *testing.T) {
	b, _, _ := newManifestTestBroker(t)
	b1, _ := b.manifestBytes()
	b2, _ := b.manifestBytes()
	if string(b1) != string(b2) {
		t.Fatal("a second GET within the cache window must serve byte-identical cached bytes")
	}
}
