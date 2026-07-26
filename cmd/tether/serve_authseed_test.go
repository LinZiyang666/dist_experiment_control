package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/nats-io/nkeys"
)

// Batch-A A6. test/p1/foundation_risk_test.go has always asserted that
// auth.LoadAccountSigner rejects a user seed — and it does. What no test
// covered is whether the PRODUCTION path ever calls it. It did not: the broker
// read account.nk with readPrivateSeedFile and handed the bytes straight to
// nkeys.FromSeed, which happily parses a user seed.
//
// So a reviewer looking for "is the wrong-kind seed guarded?" found a green
// risk test and stopped. The guard existed; nothing invoked it.
//
// These tests assert the wiring itself, in the package that owns it.

func writeSeeds(t *testing.T, brokerSeed, accountSeed []byte) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broker.nk"), brokerSeed, 0o600); err != nil {
		t.Fatalf("write broker.nk: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "account.nk"), accountSeed, 0o600); err != nil {
		t.Fatalf("write account.nk: %v", err)
	}
	return dir
}

func TestLoadAuthCalloutSeedsRejectsWrongKindAccountSeed(t *testing.T) {
	userSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("GenerateUserSeed: %v", err)
	}
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("GenerateUserSeed: %v", err)
	}

	// account.nk holds a USER seed (SU…). It parses; only the derived key kind
	// gives it away.
	dir := writeSeeds(t, brokerSeed, userSeed)

	cfg, err := loadAuthCalloutSeeds(dir)
	if err == nil {
		t.Fatalf("loadAuthCalloutSeeds accepted a user seed as account.nk (cfg=%v).\n"+
			"The broker would start clean and then sign every user JWT with a wrong-kind issuer, "+
			"so nats-server rejects EVERY client at auth_callout: 'the service is up but nobody can "+
			"connect', with nothing in the log naming account.nk.", cfg != nil)
	}
	// The message has to name the file and the expected kind — this failure is
	// discovered by an operator at 3am, not by a Go developer.
	for _, want := range []string{"account.nk", "ACCOUNT seed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q; an operator cannot act on it", err, want)
		}
	}
}

func TestLoadAuthCalloutSeedsAcceptsAccountSeed(t *testing.T) {
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	accountSeed, err := kp.Seed()
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	defer kp.Wipe()
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("GenerateUserSeed: %v", err)
	}
	dir := writeSeeds(t, brokerSeed, accountSeed)

	cfg, err := loadAuthCalloutSeeds(dir)
	if err != nil {
		t.Fatalf("a correct account seed was rejected: %v — A6 must not make a healthy deployment "+
			"fail to start", err)
	}
	// The seed must reach the broker unchanged: A6 is a validation step, not a
	// transformation. The signing path (uc.Audience, h.Now()) stays byte-identical.
	if string(cfg.AccountSeed) != string(accountSeed) {
		t.Error("AccountSeed was altered in transit; A6 must only validate, never rewrite the seed")
	}
	if string(cfg.BrokerNkeySeed) != string(brokerSeed) {
		t.Error("BrokerNkeySeed was altered in transit")
	}
}
