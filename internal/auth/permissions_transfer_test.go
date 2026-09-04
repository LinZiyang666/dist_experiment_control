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
		{"activated_member", PermissionsForActivatedMember(sampleActor, "lab", false).Pub.Allow},
		{"agent", PermissionsForAgent("lab", "lab-1", sampleActor, true).Pub.Allow},
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
// activated-member template must contain the bound sid as a complete
// token; an unscoped `OBJ_xfer-*` (matching cross-session) would let
// session-A peek into session-B's bucket. v0.2.2 design: per-session
// bucket xfer-<sid>, so the bound sid appears as the last segment of
// the bucket name (e.g. "OBJ_xfer-lab" or "$O.xfer-lab.M.>").
func TestObjXferEntriesScopedToSession(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
	check := func(label string, allows []string) {
		for _, allow := range allows {
			if !strings.Contains(allow, "xfer-") && !strings.Contains(allow, "OBJ_xfer-") {
				continue
			}
			// Must reference the bound sid via "xfer-lab" — either at
			// end-of-string ("OBJ_xfer-lab"), or before a dot
			// ("OBJ_xfer-lab.>", "$O.xfer-lab.M.>"). Anything else
			// would be either unscoped or referencing the wrong sid.
			if !strings.Contains(allow, "xfer-lab.") &&
				!strings.HasSuffix(allow, "xfer-lab") {
				t.Errorf("%s %q is not sid-scoped (expected xfer-lab as a token)",
					label, allow)
			}
		}
	}
	check("activated_member pub", perms.Pub.Allow)
	check("activated_member sub", perms.Sub.Allow)
}

// Caps probe must be sid+actor scoped (file-transfer-plan §Wire).
func TestCapsProbePermissionShape(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
	want := "tether.v2.ctrl.by." + sampleActor + ".s.lab.caps.req"
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
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
	want := "tether.v2.ctrl.by." + sampleActor + ".s.lab.transfer.*.finalize.req"
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
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
	for _, allow := range allEntries(perms) {
		if strings.Contains(allow, ".s.") &&
			!strings.Contains(allow, ".s.lab.") &&
			!strings.HasSuffix(allow, ".s.lab") {
			// Permit `_INBOX.>` and the actor-only ctrl.by.*.session.* family.
			if strings.HasPrefix(allow, "_INBOX") {
				continue
			}
			if strings.HasPrefix(allow, "tether.v2.ctrl.by."+sampleActor+".session.") {
				continue
			}
			t.Errorf("activated_member allow %q references a different sid (template was minted for sid=lab)", allow)
		}
	}
}
