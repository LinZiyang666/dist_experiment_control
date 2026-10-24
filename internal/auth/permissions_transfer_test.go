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

// origin: hot-update S7 plan §6.3–§6.4 / T31
func TestTransferContinuationPermissionsStayInScope(t *testing.T) {
	member := PermissionsForActivatedMember(sampleActor, "lab", false)
	allowed := func(patterns []string, subject string) bool {
		for _, p := range patterns {
			if tokenMatch(p, subject) {
				return true
			}
		}
		return false
	}
	for _, op := range []string{"probe", "permit", "status", "source", "reserve", "terminal", "cancel"} {
		own := "tether.v2.ctrl.by." + sampleActor + ".s.lab.node.node.transfer.id." + op + ".req"
		if !allowed(member.Pub.Allow, own) {
			t.Errorf("missing own %s", op)
		}
		for _, foreign := range []string{strings.Replace(own, ".s.lab.", ".s.other.", 1), strings.Replace(own, sampleActor, "other", 1)} {
			if allowed(member.Pub.Allow, foreign) {
				t.Errorf("foreign grant: %s", foreign)
			}
		}
	}
	for _, op := range []string{"census", "bucket-config", "unknown"} {
		if allowed(member.Pub.Allow, "tether.v2.ctrl.by."+sampleActor+".s.lab.node.node.transfer.id."+op+".req") {
			t.Errorf("management grant: %s", op)
		}
	}
	p := PermissionsForTransferSettlement(sampleActor, "lab", "node", "id")
	for _, op := range []string{"status", "terminal", "cancel"} {
		own := "tether.v2.ctrl.by." + sampleActor + ".s.lab.node.node.transfer.id." + op + ".req"
		if !allowed(p.Pub.Allow, own) {
			t.Errorf("missing settlement %s", op)
		}
		for _, foreign := range []string{strings.Replace(own, ".s.lab.", ".s.other.", 1), strings.Replace(own, sampleActor, "other", 1), strings.Replace(own, ".transfer.id.", ".transfer.other.", 1), strings.Replace(own, ".node.node.", ".node.other.", 1)} {
			if allowed(p.Pub.Allow, foreign) {
				t.Errorf("settlement scope escaped: %s", foreign)
			}
		}
	}
	for _, subject := range []string{"$JS.API.STREAM.INFO.OBJ_xfer-lab", "$JS.API.STREAM.UPDATE.OBJ_xfer-lab", "$O.xfer-lab.C.chunk", "tether.v2.s.lab.cmd.by." + sampleActor + ".node.node.push.req", "tether.v2.ctrl.by." + sampleActor + ".s.lab.node.node.transfer.id.permit.req", "tether.v2.s.lab.audit.transfer", "_INBOX.any", "tether.v2.ctrl.s.lab.node.node.register.req"} {
		if allowed(p.Pub.Allow, subject) || allowed(p.Sub.Allow, subject) {
			t.Errorf("settlement acquired business permission %s", subject)
		}
	}
	if !allowed(p.Sub.Allow, InboxPrefixFor(sampleActor)+".reply") || allowed(p.Sub.Allow, InboxPrefixFor("other")+".reply") {
		t.Fatal("settlement private reply scope drifted")
	}
	if !allowed(PermissionsForBroker().Pub.Allow, "$JS.API.STREAM.UPDATE.OBJ_xfer-lab") {
		t.Fatal("broker missing bucket config authority")
	}
	removal := PermissionsForSessionRemoval(sampleActor, "lab")
	for _, subject := range []string{"tether.v2.ctrl.by." + sampleActor + ".session.lab.rm.req", "tether.v2.ctrl.by." + sampleActor + ".cluster-health.req"} {
		if !allowed(removal.Pub.Allow, subject) {
			t.Errorf("removal owner cannot retry: %s", subject)
		}
	}
	for _, subject := range []string{"tether.v2.ctrl.by." + sampleActor + ".session.other.rm.req", "tether.v2.ctrl.by.other.session.lab.rm.req", "tether.v2.ctrl.by." + sampleActor + ".session.lab.join.req", "tether.v2.ctrl.by." + sampleActor + ".s.lab.node.node.transfer.id.permit.req", "$O.xfer-lab.C.chunk", "tether.v2.s.lab.audit.transfer"} {
		if allowed(removal.Pub.Allow, subject) || allowed(removal.Sub.Allow, subject) {
			t.Errorf("removal owner acquired unrelated authority: %s", subject)
		}
	}
	if !allowed(removal.Sub.Allow, InboxPrefixFor(sampleActor)+".reply") || allowed(removal.Sub.Allow, InboxPrefixFor("other")+".reply") || allowed(removal.Sub.Allow, "_INBOX.any") {
		t.Fatal("removal owner private reply scope drifted")
	}
}

// origin: hot-update S7 plan §6.3 / T31
func TestTransferAgentPermissionsKeepClosedOperations(t *testing.T) {
	p := PermissionsForAgent("lab", "node", sampleActor, false)
	allows := func(subject string) bool {
		for _, pattern := range p.Pub.Allow {
			if tokenMatch(pattern, subject) {
				return true
			}
		}
		return false
	}
	for _, op := range []string{"source", "reserve", "terminal", "cancel"} {
		subject := "tether.v2.s.lab.ev.node.node.transfer.id." + op + ".req"
		if !allows(subject) {
			t.Errorf("own %s denied", op)
		}
		for _, other := range []string{strings.Replace(subject, ".s.lab.", ".s.other.", 1), strings.Replace(subject, ".node.node.", ".node.other.", 1)} {
			if allows(other) {
				t.Errorf("foreign request allowed: %s", other)
			}
		}
	}
	for _, op := range []string{"probe", "permit", "status", "census", "bucket-config", "unknown"} {
		if allows("tether.v2.s.lab.ev.node.node.transfer.id." + op + ".req") {
			t.Errorf("agent acquired %s authority", op)
		}
	}
	for _, leaf := range []string{"state", "proc.pid.started", "proc.pid.exit", "proxy.ready", "proxy.unready", "transfer.id.complete", "transfer.id.failed"} {
		if !allows("tether.v2.s.lab.ev.node.node." + leaf) {
			t.Errorf("legacy event denied: %s", leaf)
		}
	}
}

// origin: hot-update S7 plan §11.4 / T31
func TestTransferPermissionScannerIncludesEveryGrant(t *testing.T) {
	grants := grantSubjects(t, "permissions.go")
	for _, op := range []string{"probe", "permit", "status", "source", "reserve", "terminal", "cancel"} {
		want := "PREFIX.ctrl.by.*.s.*.node.*.transfer.*." + op + ".req"
		found := false
		for _, grant := range grants {
			if grant == want {
				found = true
			}
		}
		if !found {
			t.Errorf("ACL scanner lost the transfer leaf grant: %s", want)
		}
	}
}
