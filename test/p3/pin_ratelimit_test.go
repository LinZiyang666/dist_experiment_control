package p3_test

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
)

// pin_ratelimit_test.go — P7/#25 END-TO-END through a REAL embedded nats-server +
// REAL broker auth_callout + REAL Argon2 PIN verify (memory discipline: an
// auth_callout change must be proven locally in real auth_callout mode, not just
// at the handler unit level).
//
// All clients share the loopback peer address, so this exercises the "one source
// keeps failing" path: after the per-IP budget is spent by wrong-PIN CONNECTs,
// even a CORRECT-PIN CONNECT from that source is refused at the NATS layer.
//
// MUTATION: with the #25 guard removed the final CORRECT-PIN CONNECT succeeds
// (the reported bug); this test then fails at "correct PIN must now be REFUSED".
func TestPINRateLimitEndToEndRealCallout(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	stop := startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)
	defer stop()

	const sid, pin = "lab", "correct-pin"
	hash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?, ?, ?, ?)`,
		sid, sid, "SHA256:test-owner", hash,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	// Positive control: while the budget is full, a CORRECT PIN joins (proves the
	// harness + PIN are good, so a later refusal is the rate limit — not a broken setup).
	if nc, err := ctlConnActivated(t, url, freshIdentity(t), sid, pin); err != nil {
		t.Fatalf("control: correct PIN must join while budget is full: %v", err)
	} else {
		nc.Close()
	}

	// Brute force: 12 wrong-PIN CONNECTs (>burst, robust to token refill during the
	// real Argon2 verifies) from throwaway identities — each REFUSED, budget spent.
	for i := 0; i < 12; i++ {
		if nc, err := ctlConnActivated(t, url, freshIdentity(t), sid, "wrong-pin"); err == nil {
			nc.Close()
			t.Fatalf("wrong-PIN attempt %d was accepted", i+1)
		}
	}

	// The key assertion: a fresh client with the CORRECT PIN from this now-blocked
	// source is REFUSED at the NATS layer (auth_callout deny → Authorization Violation).
	nc, err := ctlConnActivated(t, url, freshIdentity(t), sid, pin)
	if err == nil {
		nc.Close()
		t.Fatal("after 12 wrong PINs the correct PIN must now be REFUSED (rate limited), but CONNECT succeeded")
	}
	if !strings.Contains(err.Error(), "Authorization Violation") {
		t.Fatalf("expected an auth_callout refusal, got %v", err)
	}
}
