package auth

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/jwt/v2"
)

const sampleActor = "UABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV"

// templates lists every role's permission template plus a flag whether broker-
// scope wildcards (`s.*.>` / `ctrl.by.*.>`) are tolerated. Only the broker
// template is allowed those — they're how tetherd routes across sessions.
type templateCase struct {
	name       string
	perms      jwt.Permissions
	brokerWide bool
}

func allTemplates() []templateCase {
	return []templateCase{
		{"unactivated", PermissionsForUnactivated(sampleActor), false},
		{"activated_member", PermissionsForActivatedMember(sampleActor, "lab"), false},
		{"agent", PermissionsForAgent("lab", "lab-1"), false},
		{"broker", PermissionsForBroker(), true},
	}
}

// Top-level wildcard `tether.v2.>` is forbidden in EVERY template, including
// the broker template. We never want any single subscription/publish entry
// to cover the entire protocol surface.
func TestNoTopLevelWildcard(t *testing.T) {
	for _, tc := range allTemplates() {
		for _, allow := range allEntries(tc.perms) {
			if allow == subjectPrefix+".>" {
				t.Errorf("%s: top-level wildcard %q forbidden", tc.name, allow)
			}
		}
	}
}

// "Cross-subtree" wildcard = `tether.v2.s.<sid>.>` — covers cmd, ev, audit,
// pty, and ctrl.* in one entry. Allowed only on broker template (as
// `s.*.ev.>` / `s.*.audit.>` etc., which still pin a leaf subtree).
func TestNoCrossSubtreeWildcard(t *testing.T) {
	for _, tc := range allTemplates() {
		for _, allow := range allEntries(tc.perms) {
			if !strings.HasPrefix(allow, subjectPrefix+".s.") || !strings.HasSuffix(allow, ".>") {
				continue
			}
			body := strings.TrimSuffix(strings.TrimPrefix(allow, subjectPrefix+".s."), ".>")
			parts := strings.Split(body, ".")
			// Format must be at least <sid>.<subtree>.> — i.e. >= 2 parts.
			// e.g. "lab.ev" → ok, "lab" → cross-subtree.
			if len(parts) < 2 {
				t.Errorf("%s: cross-subtree wildcard %q (no subtree pinned)", tc.name, allow)
			}
		}
	}
}

// ctl templates (unactivated, activated) MUST NOT pub `.req.forwarded` —
// that subject is tetherd-pub / agent-sub. Architecture C.4 invariant.
func TestCtlCannotPublishForwarded(t *testing.T) {
	ctlTemplates := []templateCase{
		{"unactivated", PermissionsForUnactivated(sampleActor), false},
		{"activated_member", PermissionsForActivatedMember(sampleActor, "lab"), false},
	}
	for _, tc := range ctlTemplates {
		for _, allow := range tc.perms.Pub.Allow {
			if strings.HasSuffix(allow, ".req.forwarded") ||
				strings.Contains(allow, ".req.forwarded.") {
				t.Errorf("ctl %s: pub allow %q targets .forwarded subject", tc.name, allow)
			}
			// A `cmd.node.*.*.req` pattern (without the `cmd.by.<actor>.`
			// prefix) would also match forwarded shape. Reject.
			if strings.Contains(allow, ".cmd.node.") && strings.Contains(allow, ".req") &&
				!strings.Contains(allow, ".cmd.by.") {
				t.Errorf("ctl %s: pub allow %q has forwarded shape", tc.name, allow)
			}
		}
	}
}

// ctl pub allows must lock the actor segment to the connection's own actor —
// `by.*.<...>` would let one ctl forge another ctl's identity in subjects.
func TestCtlPubLocksActorSegment(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	for _, allow := range perms.Pub.Allow {
		if !strings.Contains(allow, ".by.") {
			continue // entries without a by.<actor> segment (e.g. _INBOX.>)
		}
		if !strings.Contains(allow, ".by."+sampleActor+".") {
			t.Errorf("activated pub allow %q has by.* / by.<other> shape (must pin actor=%q)",
				allow, sampleActor)
		}
	}
}

// TestD8bMemberAlertACLCarveOut: the D8b carve-out — a member CAN pub the actor-scoped
// cluster-health + alert RPCs (banner + client-synth gating are member-reachable), but the
// §13.8 negative invariant holds: a member's pub allow never reaches the broker-only
// tether.v2.cluster.* namespace (note ".cluster-health" is a DISTINCT token from ".cluster.").
func TestD8bMemberAlertACLCarveOut(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	want := []string{
		subjectPrefix + ".ctrl.by." + sampleActor + ".cluster-health.req",
		subjectPrefix + ".ctrl.by." + sampleActor + ".alert.ls.req",
		subjectPrefix + ".ctrl.by." + sampleActor + ".alert.ack.req",
	}
	for _, w := range want {
		if !contains(perms.Pub.Allow, w) {
			t.Errorf("member must be allowed to pub %q (D8b carve-out)", w)
		}
	}
	for _, allow := range perms.Pub.Allow {
		if strings.Contains(allow, ".cluster.") { // broker-only (cluster.apply.* / cluster.>)
			t.Errorf("member pub allow %q reaches the broker-only cluster.* namespace", allow)
		}
	}
	// Scope is ACTIVATED-member (review M2): an UNACTIVATED CLI does NOT get the carve-out —
	// every destructive op the gate guards itself needs a session, so this scope is sufficient
	// and tighter. Pin it so the "session-independent subject" naming never silently widens.
	un := PermissionsForUnactivated(sampleActor)
	for _, w := range want {
		if contains(un.Pub.Allow, w) {
			t.Errorf("unactivated CLI must NOT be granted %q (D8b carve-out is activated-member scope)", w)
		}
	}
}

// agents are NEVER allowed to publish `audit.*` — audit is tetherd-single-
// writer (architecture C.1 §4 / B.2 note).
func TestAgentCannotPublishAudit(t *testing.T) {
	perms := PermissionsForAgent("lab", "lab-1")
	for _, allow := range perms.Pub.Allow {
		if strings.Contains(allow, ".audit.") {
			t.Errorf("agent pub allow %q includes audit subject", allow)
		}
	}
}

// agents must only sub their own node's forwarded subject (B.2 agent template).
func TestAgentSubScopedToOwnNode(t *testing.T) {
	perms := PermissionsForAgent("lab", "lab-1")
	for _, allow := range perms.Sub.Allow {
		if !strings.Contains(allow, ".cmd.node.") {
			continue
		}
		if !strings.Contains(allow, ".cmd.node.lab-1.") {
			t.Errorf("agent sub allow %q must pin node=lab-1", allow)
		}
	}
}

// TestSubjectPrefixInSyncWithProto is the REAL cross-check that auth's
// import-cycle copy of the subject prefix stays synced with the proto SSOT.
//
// The non-test permissions.go deliberately duplicates `subjectPrefix` to avoid
// importing proto (cycle through proto's ed25519/jwt identifier validation),
// but this _test.go is free to import proto (proto does NOT depend on
// internal/auth — verified). Asserting against the literal "tether.v2" would be
// a tautology that a future bump (e.g. proto→"tether.v3" while forgetting to
// update permissions.go) would still pass — silently pointing every JWT ACL at
// the wrong subject tree. Assert against the live SSOT instead.
func TestSubjectPrefixInSyncWithProto(t *testing.T) {
	if subjectPrefix != proto.SubjectPrefix {
		t.Fatalf("auth subjectPrefix diverged from proto SSOT: auth=%q proto=%q "+
			"(update internal/auth/permissions.go to match)", subjectPrefix, proto.SubjectPrefix)
	}
	// Literal anchor so the current wire prefix is also greppable.
	if proto.SubjectPrefix != "tether.v2" {
		t.Fatalf("expected proto.SubjectPrefix=tether.v2 at D0, got %q", proto.SubjectPrefix)
	}
}

// TestD3RF1ClusterACLOnlyBroker (distributed-broker §6.2 RF1): the broker-only
// cluster.* ACL must be present in the broker template (pub AND sub) and ABSENT
// from every user template. The guard matches the version-prefixed literal
// (tether.v2.cluster…), NOT a bare "cluster." substring, so it cannot false-match
// an unrelated subject.
func TestD3RF1ClusterACLOnlyBroker(t *testing.T) {
	clusterPrefix := subjectPrefix + ".cluster"
	wantBroker := []string{subjectPrefix + ".cluster.apply.>", subjectPrefix + ".cluster.>"}

	for _, tc := range allTemplates() {
		has := func(set []string, want string) bool {
			for _, s := range set {
				if s == want {
					return true
				}
			}
			return false
		}
		if tc.brokerWide {
			for _, w := range wantBroker {
				if !has(tc.perms.Pub.Allow, w) {
					t.Errorf("broker template missing PUB %q (RF1 forwarder publishes here)", w)
				}
				if !has(tc.perms.Sub.Allow, w) {
					t.Errorf("broker template missing SUB %q (RF1 leader subscribes here)", w)
				}
			}
			continue
		}
		for _, entry := range allEntries(tc.perms) {
			if strings.HasPrefix(entry, clusterPrefix) {
				t.Errorf("%s: user template must NOT carry any cluster.* grant, found %q", tc.name, entry)
			}
			// User templates must also never reach the system account ($SYS.*) —
			// that is where the PIN-bearing auth requests flow (broker-only).
			if strings.HasPrefix(entry, "$SYS.") {
				t.Errorf("%s: user template must NOT carry any $SYS.* grant, found %q", tc.name, entry)
			}
		}
	}

	// SSOT cross-check: the literals in PermissionsForBroker match proto's SSOT.
	if proto.SubjClusterApplyWildcard != subjectPrefix+".cluster.apply.>" {
		t.Errorf("proto.SubjClusterApplyWildcard=%q out of sync with auth ACL literal", proto.SubjClusterApplyWildcard)
	}
	if proto.SubjClusterWildcard != subjectPrefix+".cluster.>" {
		t.Errorf("proto.SubjClusterWildcard=%q out of sync with auth ACL literal", proto.SubjClusterWildcard)
	}
}

func allEntries(p jwt.Permissions) []string {
	out := make([]string, 0, len(p.Pub.Allow)+len(p.Sub.Allow))
	out = append(out, p.Pub.Allow...)
	out = append(out, p.Sub.Allow...)
	return out
}
