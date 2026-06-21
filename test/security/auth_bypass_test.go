package security_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// === C.16 unprovisioned-nkey CONNECT denied (regression) ===================

// Note: a full CONNECT-deny test against auth_callout requires the
// p3 harness (startAuthNATS), and TestAuthCalloutRejectsUnprovisionedAgentRole
// already covers it. We add a unit-level guard here that the
// agentprov.Lookup returns ErrNotProvisioned for an unknown
// (sid, nid) — that's the precondition the auth_callout handler
// branches on.
func TestAgentProvLookupRejectsUnprovisioned(t *testing.T) {
	db := openDB(t)
	_, err := agentprov.Lookup(db, "lab", "lab-1")
	if err == nil {
		t.Errorf("Lookup of unprovisioned (sid, nid) returned nil error")
	}
	if err.Error() != "agentprov: (sid,nid) not provisioned" {
		t.Errorf("expected ErrNotProvisioned; got %v", err)
	}
}

// === C.17 member impersonating owner — broker app-layer reject ============

// TestMemberCannotImpersonateOwnerOnUpgradeSubject: in the
// no-auth_callout broker (where NATS does NOT pin the subject's
// `by.<actor>` segment), the broker MUST still reject an upgrade
// from a non-owner. That's the application-layer defense even when
// the NATS-layer pinning fails (e.g. operator misconfigured
// auth_callout off).
//
// This re-exercises the C11 actor-token validation: the broker
// derives fp from the subject, then runs IsOwner against SQLite.
func TestMemberCannotImpersonateOwnerOnUpgradeSubject(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seedSession(t, db, "lab", ownerFP)

	intruderPub, intruderFP := freshUserPub(t)
	if err := session.AddMember(db, "lab", intruderFP, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	seedNodeOnline(t, db, "lab", "lab-1")

	defer startBroker(t, url, db, withUpgradeAllow([]string{"https://allowed.example.com/"}))()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.UpgradeReq{
		URL:          "https://allowed.example.com/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion,
	})
	respMsg, err := nc.Request(proto.SubjCmdBy("lab", intruderPub, "lab-1", "upgrade"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.UpgradeResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	if resp.OK {
		t.Errorf("non-owner member triggered upgrade: %+v", resp)
	}
	if resp.Code != "not_owner" {
		t.Errorf("expected not_owner; got %+v", resp)
	}
}

// === C.18 actor token shape rejected ======================================

// TestUpgradeWithMalformedActorTokenRejected: a subject whose
// `by.<actor>` slot doesn't pass ValidateActorToken (e.g. missing
// the leading 'U', wrong length) must NOT reach the
// session/owner/upgrade pipeline.
//
// Defense: ParseCmdBy validates actor format BEFORE the broker
// dispatch table fires.
func TestUpgradeWithMalformedActorTokenRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seedSession(t, db, "lab", ownerFP)
	seedNodeOnline(t, db, "lab", "lab-1")
	defer startBroker(t, url, db, withUpgradeAllow([]string{"https://allowed.example.com/"}))()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.UpgradeReq{
		URL:          "https://allowed.example.com/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion,
	})

	// We can't construct a malformed-actor subject through
	// proto.SubjCmdBy (it just sprintfs), but we can hand-craft
	// the subject string directly.
	for _, badActor := range []string{
		"NOTANACTOR",
		strings.Repeat("U", 56),       // right shape, wrong charset (U+U... has no valid base32 CRC)
		"U" + strings.Repeat("Z", 55), // wrong charset alphabet
		"u" + strings.Repeat("A", 55), // lowercase 'u'
		"",
	} {
		subj := "tether.v2.s.lab.cmd.by." + badActor + ".node.lab-1.upgrade.req"
		respMsg, err := nc.Request(subj, body, 500*time.Millisecond)
		if err != nil {
			// Timeout is fine — broker's wildcard subscription matches
			// `cmd.by.*.node.*.upgrade.req`, but the dispatch handler
			// rejects via ParseCmdBy. If it never replies that's also
			// acceptable (no subscriber matched).
			continue
		}
		var resp proto.UpgradeResp
		_ = json.Unmarshal(respMsg.Data, &resp)
		if resp.OK {
			t.Errorf("malformed actor %q reached upgrade success path: %+v", badActor, resp)
		}
	}
}

// === C.19 agent A cannot register as agent B ===============================

// TestAgentProvBindingFirstWriteWins: the agent_provisioning binding
// is first-write-wins. Once agent A's fp is bound to (sid, nid), a
// second Provision call with agent B's fp must fail with
// ErrAlreadyProvisioned. Auth_callout uses this to deny "agent A
// pretending to be agent B".
func TestAgentProvBindingFirstWriteWins(t *testing.T) {
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seedSession(t, db, "lab", ownerFP)

	_, fpA := freshUserPub(t)
	_, fpB := freshUserPub(t)

	if err := agentprov.Provision(db, "lab", "lab-1", fpA, time.Now().UTC()); err != nil {
		t.Fatalf("first provision (A): %v", err)
	}
	err := agentprov.Provision(db, "lab", "lab-1", fpB, time.Now().UTC())
	if err == nil || err != agentprov.ErrAlreadyProvisioned {
		t.Errorf("expected ErrAlreadyProvisioned for second provision; got %v", err)
	}
	// And A can re-bind idempotently.
	if err := agentprov.Provision(db, "lab", "lab-1", fpA, time.Now().UTC()); err != nil {
		t.Errorf("idempotent re-provision of A failed: %v", err)
	}
}

// === C.20 Evicted agent cannot re-bind without re-provisioning ============

// TestAgentProvEvictedBindingCanRebind: after admin evict
// (DELETE FROM agent_provisioning), the slot is free for a NEW
// fp to claim — that's the documented behavior. Any agent
// re-connecting with the OLD fp must re-PIN.
func TestAgentProvEvictedAllowsNewBinding(t *testing.T) {
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seedSession(t, db, "lab", ownerFP)

	_, fpA := freshUserPub(t)
	if err := agentprov.Provision(db, "lab", "lab-1", fpA, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Simulate admin evict.
	if _, err := db.Exec(`DELETE FROM agent_provisioning WHERE sid=? AND nid=?`, "lab", "lab-1"); err != nil {
		t.Fatal(err)
	}
	// New agent B can now claim the slot.
	_, fpB := freshUserPub(t)
	if err := agentprov.Provision(db, "lab", "lab-1", fpB, time.Now().UTC()); err != nil {
		t.Errorf("post-evict re-provision (B) failed: %v", err)
	}
	// And lookup returns B, not A.
	got, err := agentprov.Lookup(db, "lab", "lab-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != fpB {
		t.Errorf("post-evict lookup returned %q, want %q", got, fpB)
	}
}

// === C.21 PIN brute force defense documentation ==========================

// TestPINBruteForceNoAccountLockoutCurrentBehavior: the auth_callout
// handler does NOT impose any rate-limit / lockout per (sid, fp) on
// failed PIN attempts. After 100 wrong PINs in a row, the 101st with
// the CORRECT pin still succeeds. This is the documented v1
// behavior — auth-rate-limiting was deferred (P-future).
//
// FINDING (medium): no PIN-attempt rate limit; an attacker with
// knowledge of (sid, nid) can brute-force a 4-digit PIN at ~1
// CONNECT/sec for the next ~3 hours.
//
// We don't actually run 100 argon2 verifies (would take ~20s); we
// run 5 wrong + 1 right and assert the right PIN still works.
func TestPINBruteForceNoLockout5Tries(t *testing.T) {
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seedSessionWithPIN(t, db, "lab", ownerFP, "right")

	for i := 0; i < 5; i++ {
		err := session.JoinWithPIN(db, "lab", "fp-"+string(rune('a'+i)), "wrong", auth.VerifyPIN, time.Now().UTC())
		if err == nil {
			t.Fatalf("attempt %d: wrong PIN was accepted", i)
		}
	}
	// Now a fresh actor with the right PIN still succeeds.
	if err := session.JoinWithPIN(db, "lab", "fp-correct", "right", auth.VerifyPIN, time.Now().UTC()); err != nil {
		t.Errorf("right PIN rejected after 5 wrong tries: %v", err)
	}
	t.Logf("FINDING: no PIN rate limit; 5 wrong + 1 right succeeds (101st right would also succeed)")
}
