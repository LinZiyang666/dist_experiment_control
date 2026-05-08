package p3_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
)

// TestForgedActorCannotTombstoneVictimSession is the round-1 risk test.
// With auth_callout enabled, NATS pins each connection's `by.<actor>` to its
// real nkey, so a publish to ctrl.by.<owner>.session.lab.rm.req from the
// attacker's connection is denied at the NATS layer (the message never
// reaches tetherd). We accept either:
//
//  1. nats.Request returns an error (NATS perm violation / timeout — the
//     publish was denied or no responder ever heard it),
//  2. Request gets a reply with OK=false (broker app-layer reject).
//
// Either way, the session must NOT be tombstoned. The original test asserted
// shape (1) only; we widened it because the architecturally-correct fix
// (auth_callout) prevents the attack at NATS rather than at the broker.
func TestForgedActorCannotTombstoneVictimSession(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := freshIdentity(t)
	mustCreate(t, url, owner, "lab", "owner-pin")

	// Attacker is a member of *some other session* (or none at all). We
	// connect them as activated for "lab" — they're NOT a member, so the
	// CONNECT itself should be rejected by auth_callout. That is the
	// strongest guarantee, but even if we relax to unactivated, the JWT
	// permissions don't cover ctrl.by.<owner>.session.lab.rm.req.
	attacker := freshIdentity(t)

	// First scenario: attacker tries to activate "lab" without PIN — denied.
	if _, err := ctlConnActivated(t, url, attacker, "lab", ""); err == nil {
		t.Fatal("attacker CONNECT to lab without PIN must be denied at auth_callout")
	}

	// Second scenario: attacker connects unactivated and tries to forge
	// `by.<owner>.session.lab.rm.req`. NATS denies the pub; Request times out
	// or returns perm violation. Session must remain ACTIVE either way.
	attackerNC := ctlConnUnactivated(t, url, attacker)
	msg, err := attackerNC.Request(
		proto.SubjCtrlSessionRm(owner.PublicKey, "lab"),
		[]byte("{}"),
		1*time.Second,
	)
	if err == nil {
		var resp proto.SessionRmResp
		_ = json.Unmarshal(msg.Data, &resp)
		if resp.OK {
			t.Fatalf("forged actor was accepted: attacker %s tombstoned owner session by publishing as %s",
				attacker.PublicKey, owner.PublicKey)
		}
	}

	// Verify "lab" is still ACTIVE — owner can still activate it.
	if _, err := ctlConnActivated(t, url, owner, "lab", ""); err != nil {
		t.Fatalf("owner can no longer activate lab; the attack succeeded silently: %v", err)
	}
}

// Force the import to be used in this file.
var _ = cli.CtlNameUnactivated
