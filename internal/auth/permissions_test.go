package auth

import (
	"strings"
	"testing"

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

// Top-level wildcard `tether.v1.>` is forbidden in EVERY template, including
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

// "Cross-subtree" wildcard = `tether.v1.s.<sid>.>` — covers cmd, ev, audit,
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

// SubjectPrefix duplicated in this package vs proto must match.
func TestSubjectPrefixInSyncWithProto(t *testing.T) {
	// Encoded into subjectPrefix const. proto.SubjectPrefix is "tether.v1".
	// If they ever diverge, every other test here breaks first; this is just
	// an explicit pin for grep-ability.
	if subjectPrefix != "tether.v1" {
		t.Fatalf("subjectPrefix in internal/auth diverged: %q", subjectPrefix)
	}
}

func allEntries(p jwt.Permissions) []string {
	out := make([]string, 0, len(p.Pub.Allow)+len(p.Sub.Allow))
	out = append(out, p.Pub.Allow...)
	out = append(out, p.Sub.Allow...)
	return out
}
