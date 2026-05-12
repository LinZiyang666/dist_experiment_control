package auth

import (
	"strings"
	"testing"
)

// File-transfer-plan §Auth — bucket lifecycle authority is broker-only.
// Activated members and agents MUST NOT be granted STREAM.CREATE / DELETE
// / PURGE / UPDATE on OBJ_xfer-* streams. This is the negative half of
// Round-3 #1 + #3 + Round-4 #3 (PURGE explicitly excluded so a failed
// ObjectStore.Put cannot client-side-purge a bucket the broker is the
// authoritative deleter for).
func TestNoStreamLifecycleOnObjXfer(t *testing.T) {
	cases := []struct {
		name string
		pubs []string
	}{
		{"activated_member", PermissionsForActivatedMember(sampleActor, "lab").Pub.Allow},
		{"agent", PermissionsForAgent("lab", "lab-1").Pub.Allow},
	}
	bannedVerbs := []string{"STREAM.CREATE", "STREAM.DELETE", "STREAM.PURGE", "STREAM.UPDATE"}
	for _, tc := range cases {
		for _, allow := range tc.pubs {
			if !strings.Contains(allow, ".OBJ_xfer-") {
				continue
			}
			for _, verb := range bannedVerbs {
				if strings.Contains(allow, verb) {
					t.Errorf("%s: pub allow %q contains banned %q (broker-only)",
						tc.name, allow, verb)
				}
			}
		}
	}
}

// Sid scoping invariant: every OBJ_xfer / $O.xfer entry on the
// activated-member template must contain the bound sid; an unscoped
// `OBJ_xfer-*` (matching cross-session) would let session-A peek into
// session-B's bucket.
func TestObjXferEntriesScopedToSession(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	for _, allow := range perms.Pub.Allow {
		if !strings.Contains(allow, "xfer-") && !strings.Contains(allow, "OBJ_xfer-") {
			continue
		}
		// Must contain the literal "lab-" between "xfer-" and the next dot/end.
		if !strings.Contains(allow, "xfer-lab-") {
			t.Errorf("activated_member pub %q is not sid-scoped (expected xfer-lab-)", allow)
		}
	}
	for _, allow := range perms.Sub.Allow {
		if !strings.Contains(allow, "xfer-") {
			continue
		}
		if !strings.Contains(allow, "xfer-lab-") {
			t.Errorf("activated_member sub %q is not sid-scoped", allow)
		}
	}
}

// Caps probe must be sid+actor scoped (file-transfer-plan §Wire).
func TestCapsProbePermissionShape(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	want := "tether.v1.ctrl.by." + sampleActor + ".s.lab.caps.req"
	for _, allow := range perms.Pub.Allow {
		if allow == want {
			return
		}
	}
	t.Errorf("missing caps.req allow: %q\nhave: %v", want, perms.Pub.Allow)
}

// Pull receiver finalize must be sid+actor scoped, transfer-id wildcarded
// (broker enforces transfer-id ↔ actor ownership application-side).
// Reviewer Round-4 #1 fix.
func TestFinalizePermissionShape(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	want := "tether.v1.ctrl.by." + sampleActor + ".s.lab.transfer.*.finalize.req"
	for _, allow := range perms.Pub.Allow {
		if allow == want {
			return
		}
	}
	t.Errorf("missing finalize.req allow: %q\nhave: %v", want, perms.Pub.Allow)
}

// Cross-session NATS-layer denial: an activated-member JWT for session
// "lab" must NOT contain any allow whose sid segment is anything other
// than "lab" (Round-4 #1 negative — the sid is bound at JWT mint time;
// a session-B member should never have a finalize allow under
// `s.<otherSid>.transfer.*.finalize.req`).
func TestActivatedMemberSidIsLockedAtMint(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	for _, allow := range allEntries(perms) {
		if strings.Contains(allow, ".s.") &&
			!strings.Contains(allow, ".s.lab.") &&
			!strings.HasSuffix(allow, ".s.lab") {
			// Permit `_INBOX.>` and the actor-only ctrl.by.*.session.* family.
			if strings.HasPrefix(allow, "_INBOX") {
				continue
			}
			if strings.HasPrefix(allow, "tether.v1.ctrl.by."+sampleActor+".session.") {
				continue
			}
			t.Errorf("activated_member allow %q references a different sid (template was minted for sid=lab)", allow)
		}
	}
}
