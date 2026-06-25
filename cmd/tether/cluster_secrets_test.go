package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/nkeys"
)

// TestReadClusterPublicIdentitiesHappyNoLeak (B3 item 1): valid seeds derive the PUBLIC nkeys,
// and the PRIVATE seed bytes NEVER appear in any output field (the security-critical property).
func TestReadClusterPublicIdentitiesHappyNoLeak(t *testing.T) {
	dir := t.TempDir()
	acctKP, _ := nkeys.CreateAccount()
	acctSeed, _ := acctKP.Seed()
	brkKP, _ := nkeys.CreateUser()
	brkSeed, _ := brkKP.Seed()
	if err := os.WriteFile(filepath.Join(dir, "account.nk"), acctSeed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broker.nk"), brkSeed, 0o600); err != nil {
		t.Fatal(err)
	}

	// No conf file → cross-check is skipped → both values substitute.
	ids := readClusterPublicIdentities(dir, filepath.Join(dir, "nonexistent.conf"))
	acctPub, _ := acctKP.PublicKey()
	brkPub, _ := brkKP.PublicKey()
	if ids.AccountIssuer != acctPub {
		t.Errorf("AccountIssuer = %q, want %q", ids.AccountIssuer, acctPub)
	}
	if ids.BrokerNkey != brkPub {
		t.Errorf("BrokerNkey = %q, want %q", ids.BrokerNkey, brkPub)
	}
	all := ids.AccountIssuer + "|" + ids.BrokerNkey + "|" + ids.Note
	if strings.Contains(all, string(acctSeed)) || strings.Contains(all, string(brkSeed)) {
		t.Fatal("PRIVATE seed leaked into the substituted output")
	}
}

// confWith renders a minimal nats.conf (Preflight-acceptable) with the given auth_callout issuer
// + a single broker user nkey, so the cross-check branch can be exercised.
func confWith(issuer, brokerNkey string) string {
	return `host: "127.0.0.1"
port: 4222
jetstream { store_dir: "/var/lib/tether/js" }
authorization {
  users: [{ nkey: "` + brokerNkey + `" }]
  auth_callout {
    issuer: "` + issuer + `"
    auth_users: [ "` + brokerNkey + `" ]
  }
}
`
}

// TestReadClusterPublicIdentitiesCrossCheck (B3 review M3): the conf cross-check — the single
// "prints a wrong command" path. Match → both substitute (Note==""); a skew refuses to substitute
// that value + sets Note (never a confidently-wrong substitution).
func TestReadClusterPublicIdentitiesCrossCheck(t *testing.T) {
	dir := t.TempDir()
	acctKP, _ := nkeys.CreateAccount()
	acctSeed, _ := acctKP.Seed()
	brkKP, _ := nkeys.CreateUser()
	brkSeed, _ := brkKP.Seed()
	_ = os.WriteFile(filepath.Join(dir, "account.nk"), acctSeed, 0o600)
	_ = os.WriteFile(filepath.Join(dir, "broker.nk"), brkSeed, 0o600)
	acctPub, _ := acctKP.PublicKey()
	brkPub, _ := brkKP.PublicKey()
	otherAcct, _ := nkeys.CreateAccount()
	otherAcctPub, _ := otherAcct.PublicKey()
	otherUser, _ := nkeys.CreateUser()
	otherUserPub, _ := otherUser.PublicKey()
	writeConf := func(body string) string {
		p := filepath.Join(dir, "nats.conf")
		_ = os.WriteFile(p, []byte(body), 0o600)
		return p
	}

	// (a) match → both substitute, no Note.
	ids := readClusterPublicIdentities(dir, writeConf(confWith(acctPub, brkPub)))
	if ids.AccountIssuer != acctPub || ids.BrokerNkey != brkPub || ids.Note != "" {
		t.Errorf("match: got %+v, want both substituted + no Note", ids)
	}
	// (b) issuer mismatch → AccountIssuer suppressed + Note.
	ids = readClusterPublicIdentities(dir, writeConf(confWith(otherAcctPub, brkPub)))
	if ids.AccountIssuer != "" || ids.BrokerNkey != brkPub || !strings.Contains(ids.Note, "issuer") {
		t.Errorf("issuer-mismatch: got %+v, want AccountIssuer suppressed + issuer Note", ids)
	}
	// (c) broker mismatch → BrokerNkey suppressed + Note.
	ids = readClusterPublicIdentities(dir, writeConf(confWith(acctPub, otherUserPub)))
	if ids.BrokerNkey != "" || ids.AccountIssuer != acctPub || !strings.Contains(ids.Note, "broker") {
		t.Errorf("broker-mismatch: got %+v, want BrokerNkey suppressed + broker Note", ids)
	}
}

// TestReadClusterPublicIdentitiesFailClosed (B3 item 1): garbled / missing secrets leave the value
// "" (a <placeholder> for the caller) + a NOTE — never a confidently-wrong substitution.
func TestReadClusterPublicIdentitiesFailClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "account.nk"), []byte("garbage-not-a-seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	// broker.nk intentionally missing.
	ids := readClusterPublicIdentities(dir, filepath.Join(dir, "nonexistent.conf"))
	if ids.AccountIssuer != "" || ids.BrokerNkey != "" {
		t.Errorf("garbled/missing must not substitute, got %+v", ids)
	}
	if ids.Note == "" {
		t.Error("a NOTE must explain the unresolved values")
	}
	if orPlaceholder(ids.AccountIssuer, "<account-public-nkey>") != "<account-public-nkey>" {
		t.Error("orPlaceholder must yield the placeholder for an empty value")
	}
}
