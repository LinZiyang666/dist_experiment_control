package auth

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/jwt/v2"
)

// origin: external review 2026-09-03, fingerprint canonicalization finding.
//
// A syntactically decodable base64 string is not necessarily one this package can
// produce.  RawStdEncoding is permissive about the unused low bits in the last
// character unless Strict is requested, so accepting a non-canonical spelling lets
// an administrator create an allow-list row that can never equal a generated
// fingerprint even though the CLI reports it as admitted.
func TestValidFingerprintRejectsNonCanonicalBase64(t *testing.T) {
	canonicalPayload := base64.RawStdEncoding.EncodeToString(make([]byte, 32))
	if canonicalPayload[len(canonicalPayload)-1] != 'A' {
		t.Fatalf("test fixture is no longer canonical zero-digest base64: %q", canonicalPayload)
	}
	nonCanonical := canonicalPayload[:len(canonicalPayload)-1] + "B"
	if _, err := base64.RawStdEncoding.DecodeString(nonCanonical); err != nil {
		t.Fatalf("negative control must remain decodable by the permissive decoder: %v", err)
	}
	if ValidFingerprint("SHA256:" + nonCanonical) {
		t.Fatal("ValidFingerprint accepted a non-canonical digest spelling; session-allow can " +
			"therefore report success for a row no generated fingerprint can ever match")
	}
}

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
		{"unactivated", PermissionsForUnactivated(sampleActor, false), false},
		{"activated_member", PermissionsForActivatedMember(sampleActor, "lab", false), false},
		{"agent", PermissionsForAgent("lab", "lab-1", sampleActor, true), false},
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
		{"unactivated", PermissionsForUnactivated(sampleActor, false), false},
		{"activated_member", PermissionsForActivatedMember(sampleActor, "lab", false), false},
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
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
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
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
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
	un := PermissionsForUnactivated(sampleActor, false)
	for _, w := range want {
		if contains(un.Pub.Allow, w) {
			t.Errorf("unactivated CLI must NOT be granted %q (D8b carve-out is activated-member scope)", w)
		}
	}
}

// TestG3RosterPullGrantedBothTemplates (G3 #17): roster-pull is granted in BOTH templates — UNLIKE the
// activated-only cluster-health carve-out — because refreshCtlEndpoints fires on every expandable connect,
// including unactivated ones (session list / login). The §13.8 invariant still holds: ".cluster-roster."
// is a DISTINCT token from ".cluster.", so a member grant of it never reaches the broker-only namespace.
func TestG3RosterPullGrantedBothTemplates(t *testing.T) {
	want := subjectPrefix + ".ctrl.by." + sampleActor + ".cluster-roster.req"
	if !contains(PermissionsForUnactivated(sampleActor, false).Pub.Allow, want) {
		t.Errorf("unactivated CLI must be granted %q (refresh fires on unactivated connects)", want)
	}
	if !contains(PermissionsForActivatedMember(sampleActor, "lab", false).Pub.Allow, want) {
		t.Errorf("activated member must be granted %q", want)
	}
	for _, allow := range append(
		append([]string{}, PermissionsForUnactivated(sampleActor, false).Pub.Allow...),
		PermissionsForActivatedMember(sampleActor, "lab", false).Pub.Allow...,
	) {
		if strings.Contains(allow, ".cluster.") {
			t.Errorf("pub allow %q reaches the broker-only cluster.* namespace (§13.8)", allow)
		}
	}
}

// agents are NEVER allowed to publish `audit.*` — audit is tetherd-single-
// writer (architecture C.1 §4 / B.2 note).
func TestAgentCannotPublishAudit(t *testing.T) {
	perms := PermissionsForAgent("lab", "lab-1", sampleActor, true)
	for _, allow := range perms.Pub.Allow {
		if strings.Contains(allow, ".audit.") {
			t.Errorf("agent pub allow %q includes audit subject", allow)
		}
	}
}

// agents must only sub their own node's forwarded subject (B.2 agent template).
func TestAgentSubScopedToOwnNode(t *testing.T) {
	perms := PermissionsForAgent("lab", "lab-1", sampleActor, true)
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

// origin: prerelease audit round 2, A-F1 (also reported as C1 and guard-vacuity G1).
//
// THE MEMBER TEMPLATE IS ONE UNAUTHENTICATED REQUEST AWAY FROM A STRANGER.
//
// The first fix removed the bare `_INBOX.>` grant from PermissionsForUnactivated only,
// on the reasoning that the other two templates "require session membership — so what
// survives is cross-session reading BY AN AUTHORIZED PRINCIPAL, not by a stranger".
// That reasoning was wrong and a verifier reproduced the counter-example against a real
// nats-server: the unactivated template grants `Pub …session.create.req`,
// handleSessionCreate has no admission control, creating a session makes you its owner,
// and the next CONNECT mints the MEMBER template. Membership costs a stranger one
// request, so the wildcard was reachable by anyone on the internet — and with it every
// other connection's replies: each agent's register reply (tunnel token + every
// subscriber PSK), the print-once /sub bearer token, and all `tether exec` output.
//
// The assertion is on the SUBSCRIBE list specifically. Pub still carries `_INBOX.>` and
// must: a client has to be able to set a reply subject.
func TestNoTemplateGrantsTheBareInboxWildcardToASubscriber(t *testing.T) {
	cases := []struct {
		name  string
		perms jwt.Permissions
	}{
		{"unactivated", PermissionsForUnactivated(sampleActor, false)},
		{"activated member", PermissionsForActivatedMember(sampleActor, "lab", false)},
		// An agent at or past the cutover. The legacy grant below is the ONE
		// deliberate exception and has its own test.
		{"upgraded agent", PermissionsForAgent("lab", "lab-1", sampleActor, false)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, s := range tc.perms.Sub.Allow {
				if s == "_INBOX.>" {
					t.Fatalf("the %s template SUBSCRIBES to the whole reply space.\n\n"+
						"That is a strict superset of every per-identity `_INBOX.<tok>` subtree, so "+
						"deriving a private prefix buys nothing while any principal also holds this. "+
						"Reaching this template costs a stranger one unauthenticated "+
						"session.create.req, and what it reads is tunnel tokens, subscriber PSKs, the "+
						"print-once /sub token and every exec's output.", tc.name)
				}
			}
			// And the replacement must actually be there, or the template simply
			// receives nothing and the test above is satisfied by a broken client.
			want := inboxSubjectFor(sampleActor)
			var found bool
			for _, s := range tc.perms.Sub.Allow {
				if s == want {
					found = true
				}
			}
			if !found {
				t.Errorf("the %s template has no derived inbox subtree (%q) — it can receive no "+
					"replies at all", tc.name, want)
			}
		})
	}
}

// origin: prerelease audit round 2, A-F1; rewritten by increment 2 internal review,
// ops-upgrade/L16-F1.
//
// THE TWO INBOX SPACES MUST BE DISJOINT. This is the invariant the whole N-1 design
// rests on: because a legacy grant and a per-identity subtree cannot overlap, the
// callout can hand out the legacy grant on nothing but the client's own say-so. An
// attacker who claims to be old reaches only traffic that pre-cutover binaries put in
// the shared space themselves; one who claims to be modern reaches only a subtree
// derived from their own nkey.
//
// WHAT THIS TEST USED TO ASSERT, AND WHY THAT WAS NOT ENOUGH. The first version checked
// TOKEN COUNTS: the modern prefix had to be three tokens, the legacy allow at most
// three, the deny at least four. Every one of those held, and the property was still
// false on every server the fleet runs — because "the legacy allow cannot NAME a
// four-token subject" is not the same claim as "a holder of the legacy allow cannot
// RECEIVE one", and the deny that was supposed to bridge them is installed lazily by
// nats-server under a predicate that changed upstream between v2.12 and v2.14. A test
// that counts tokens cannot see that; a test that asks whether two subject PATTERNS can
// name a common subject can, and that is what this one does.
//
// It is now an overlap test over the patterns themselves, so it goes red for any change
// that puts the two spaces back under one root — including the exact shape this release
// shipped with before the review.
func TestTheLegacyAndModernInboxSpacesAreDisjoint(t *testing.T) {
	// A concrete subject from the modern space: what nats.go's response multiplexer
	// actually delivers on, `<prefix>.<nuid>.<seq>`.
	modern := InboxPrefixFor(sampleActor) + ".NMaR91TY9m5XZsHA5foU60.1"
	for _, legacy := range LegacyInboxAllow {
		if grantCanReach(legacy, modern) {
			t.Fatalf("a holder of the legacy grant %q can construct a subscription that receives %q.\n\n"+
				"Then \"claim to be old\" is a privilege escalation again: the callout hands the legacy "+
				"grant to anyone who asks for it, on the strength of these two spaces being disjoint.\n\n"+
				"Do NOT fix this by adding a deny. A deny is bookkeeping nats-server performs lazily, "+
				"and the predicate that triggers it changed between v2.12 and v2.14 — under the older "+
				"one, `deny _INBOX.*.*.>` was never installed for a subscription to `_INBOX.<lit>.>` and "+
				"every four-token reply was delivered. Measured. Keep the ROOTS disjoint instead.",
				legacy, modern)
		}
	}
	// And the roots really are different roots, stated separately so the failure names
	// the cause rather than the symptom.
	if strings.HasPrefix(InboxRoot+".", "_INBOX.") || strings.HasPrefix("_INBOX.", InboxRoot+".") {
		t.Fatalf("InboxRoot %q is not disjoint from the legacy `_INBOX` root at the first token", InboxRoot)
	}
}

// grantCanReach reports whether a holder of the subscribe-allow pattern `allow` can
// construct SOME subscription that receives messages published to `subject`.
//
// THAT IS THE QUESTION, and it is not the same as "do these two patterns overlap" — the
// first draft of this helper asked the overlap question and its own control test caught
// it. `allow _INBOX.*.*` and the subject `_INBOX.a.b.<nuid>` do NOT overlap as patterns:
// the allow names exactly three tokens, the subject has four. And yet a holder of that
// allow reads that subject on every nats-server the fleet has run, because it does not
// have to subscribe to a pattern the allow "contains" — it subscribes to
// `_INBOX.a.>`, and the server admits THAT because canSubscribe matches a
// subscription's own `*`/`>` as ORDINARY LITERAL TOKENS against the allow list. Three
// literal tokens, admitted by `_INBOX.*.*`, and then `>` does what `>` does.
//
// So the rule modelled here is: walking the allow left to right, the first position
// where it stops pinning a literal is a position where the subscriber may write `>`
// instead — and from there it reaches everything below.
func grantCanReach(allow, subject string) bool {
	at := strings.Split(allow, ".")
	st := strings.Split(subject, ".")
	for i := range at {
		if at[i] == ">" {
			// The allow itself is unbounded from here down.
			return i <= len(st)
		}
		if i >= len(st) {
			// The allow pins more tokens than the subject has; any admitted
			// subscription names a token the subject does not.
			return false
		}
		if at[i] == "*" {
			// The subscriber may put a literal `>` in this slot. That subscription is
			// admitted, and it receives every subject that shares the first i tokens —
			// this one included, as long as there is something at or below position i.
			return true
		}
		if at[i] != st[i] {
			return false
		}
	}
	// Every allow token was a literal and matched: it admits exactly this depth.
	return len(at) == len(st)
}

// origin: prerelease audit increment 2 internal review, ops-upgrade/L16-F1.
//
// EVERY RESPONDER MUST BE ABLE TO PUBLISH INTO BOTH INBOX ROOTS.
//
// msg.Respond publishes to whatever inbox the REQUESTER chose, and the requester's
// vintage is independent of the responder's. During the N-1 window both are live at
// once: a pre-cutover ctl waits on `_INBOX.<nuid>.<seq>` while an upgraded broker waits
// on `_TINBOX.<a>.<b>.<nuid>`, and one agent may have to answer both within a second of
// each other. A responder missing either grant drops those replies.
//
// AND IT DROPS THEM SILENTLY. nats-server refuses the PUBLISH and the request simply
// times out at the far end; the responder logs nothing, because as far as it is
// concerned it answered. That asymmetry — loud on subscribe, silent on publish — is why
// the Pub side gets a mechanical guard while the Sub side is mostly self-announcing.
//
// The list is also a FILE for a clustered broker: natsconf.Render writes
// PermissionsForBroker into nats.conf as the static nkey user's permissions block. A
// missing entry there is a deployment that must be re-rendered, not a rebuild.
func TestEveryResponderCanPublishToBothInboxRoots(t *testing.T) {
	responders := map[string]jwt.Permissions{
		// legacy=true and false both, because the Pub side must NOT be conditioned on the
		// responder's own marker — that would break N-1 in both directions at once.
		"agent (pre-cutover)": PermissionsForAgent("lab", "lab-1", sampleActor, true),
		"agent (upgraded)":    PermissionsForAgent("lab", "lab-1", sampleActor, false),
		"broker":              PermissionsForBroker(),
	}
	for name, perms := range responders {
		for _, root := range []string{"_INBOX.>", InboxRoot + ".>"} {
			found := false
			for _, a := range perms.Pub.Allow {
				if a == root {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("the %s template cannot publish to %q.\n\n"+
					"It answers requests, and a requester of the other vintage waits for its reply "+
					"there. The server refuses the publish and the far end times out with nothing to "+
					"explain it — this template's own logs show a successful Respond.\n\n"+
					"Pub allow was: %v", name, root, perms.Pub.Allow)
			}
		}
	}
}

// The control for the test above: the shape this release ALMOST shipped must be
// reported as reachable. Without this, grantCanReach returning a constant false would
// leave the disjointness test green forever — and the first draft of that helper, which
// asked a subtly different question, was caught by exactly these rows.
//
// The DELIVERED/not-delivered annotations are measured, one embedded nats-server per
// version, with internal/auth's own allow list:
//
//	2.10.22 / 2.11.0 / 2.11.9 / 2.12.0   sub `_INBOX.aaaaaaaa.>` → DELIVERED
//	2.14.0                                sub `_INBOX.aaaaaaaa.>` → not delivered (deny installed)
func TestGrantReachSeesTheShapeThisReleaseAlmostShipped(t *testing.T) {
	// gate-control: TestTheLegacyAndModernInboxSpacesAreDisjoint
	reachable := []struct{ allow, subject string }{
		// The pre-review design: the legacy allow against a per-identity subtree that
		// lived inside `_INBOX`. The escape subscription is `_INBOX.aaaaaaaa.>`.
		{"_INBOX.*.*", "_INBOX.aaaaaaaa.bbbbbbbb.NUID"},
		{"_INBOX.*.*", "_INBOX.aaaaaaaa.bbbbbbbb.NUID.1"},
		// The plainer forms of the same mistake.
		{"_INBOX.>", "_INBOX.aaaaaaaa.bbbbbbbb.NUID"},
		{"_INBOX.*", "_INBOX.aaaaaaaa.bbbbbbbb.NUID"},
		{"_INBOX.*", "_INBOX.aaaaaaaa"},
		// A grant on the modern root would of course reach the modern space; this row
		// exists so the helper cannot pass by keying on the string "_INBOX".
		{InboxRoot + ".*.*", InboxRoot + ".aaaaaaaa.bbbbbbbb.NUID"},
	}
	for _, c := range reachable {
		if !grantCanReach(c.allow, c.subject) {
			t.Errorf("grantCanReach(%q, %q) = false, want true — the disjointness test it "+
				"backs would pass on a design that leaks", c.allow, c.subject)
		}
	}
	unreachable := []struct{ allow, subject string }{
		// The shipped design.
		{"_INBOX.>", InboxRoot + ".aaaaaaaa.bbbbbbbb.NUID"},
		{"_INBOX.*.*", InboxRoot + ".aaaaaaaa.bbbbbbbb.NUID"},
		// A literal allow admits exactly its own depth and nothing under it.
		{"_INBOX.a.b", "_INBOX.a.b.c"},
		{"_INBOX.a.b", "_INBOX.a"},
		{"_INBOX.a.b", "_INBOX.c.d"},
	}
	for _, c := range unreachable {
		if grantCanReach(c.allow, c.subject) {
			t.Errorf("grantCanReach(%q, %q) = true, want false", c.allow, c.subject)
		}
	}
}

// origin: prerelease audit round 2, A-F1.
//
// A CONNECTION GETS ONE SPACE OR THE OTHER, NEVER BOTH — and never neither.
//
// Both halves matter. Both would mean the deny swallows the client's own deep subtree
// (the deny cannot carve an exception for "this connection's hash", since every
// connection is minted from the same template), so a modern client's every request would
// time out with nothing on the wire to explain it. Neither would mean no inbox at all.
func TestEachTemplateGetsExactlyOneInboxSpace(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for name, perms := range map[string]jwt.Permissions{
			"unactivated": PermissionsForUnactivated(sampleActor, legacy),
			"member":      PermissionsForActivatedMember(sampleActor, "lab", legacy),
			"agent":       PermissionsForAgent("lab", "lab-1", sampleActor, legacy),
		} {
			deep, shallow := false, false
			for _, a := range perms.Sub.Allow {
				switch {
				case a == inboxSubjectFor(sampleActor):
					deep = true
				case strings.HasPrefix(a, "_INBOX"):
					shallow = true
				}
			}
			if deep && shallow {
				t.Errorf("%s (legacy=%v) holds BOTH inbox spaces.\n\n"+
					"Holding both means this identity can read the shared pre-cutover space as well "+
					"as its own — i.e. exactly the disclosure the private inbox exists to close, "+
					"granted by the template that was supposed to close it.", name, legacy)
			}
			if !deep && !shallow {
				t.Errorf("%s (legacy=%v) holds NEITHER inbox space and cannot receive a reply "+
					"at all", name, legacy)
			}
			if legacy != shallow {
				t.Errorf("%s: legacy=%v but shared-grant=%v — the flag is not selecting the space",
					name, legacy, shallow)
			}
			// NO DENY ON EITHER BRANCH — origin: prerelease audit increment 2 internal
			// review, ops-upgrade/L16-F1. This used to require a deny alongside the
			// legacy allow, because the two spaces then shared the `_INBOX` root and the
			// deny was the only thing bounding them. It bounded nothing: nats-server
			// installs the message-level deny filter lazily, and under the predicate
			// every shipped server used it was never installed for the one subscription
			// shape that mattered. The roots are disjoint now, so a deny here would be
			// dead weight that reads as a safety net — which is worse than none.
			if len(perms.Sub.Deny) > 0 {
				t.Errorf("%s (legacy=%v) carries a Sub deny %v.\n\n"+
					"Disjoint roots need no deny, and a deny here would re-create the dependency on "+
					"server-version-specific filter bookkeeping that this design exists to remove.",
					name, legacy, perms.Sub.Deny)
			}
		}
	}
}

// origin: prerelease audit round 2, I-F6.
//
// THE AGENT'S KEYSTROKE INTAKE IS PINNED TO ITS OWN NODE.
//
// `.in` and `.resize` were granted as a SESSION wildcard, so nats-server delivered every
// node's raw keystroke stream to every agent in the session; the agent dropped it on the
// pid lookup, which means the disclosure happened on the wire and the product could not
// see it. `.in` is not metadata — it is whatever the operator typed, which on this fleet
// includes a password entered at a jump host.
//
// TestAgentSubScopedToOwnNode above pins the same property for the forwarded command
// subject and did not extend to pty, which is how this survived: the pattern was already
// established one line away.
func TestAgentPtyIntakeIsPinnedToItsOwnNode(t *testing.T) {
	perms := PermissionsForAgent("lab", "lab-1", sampleActor, true)

	nodeScoped := map[string]bool{}
	for _, allow := range perms.Sub.Allow {
		if !strings.Contains(allow, ".node.") || !strings.Contains(allow, ".pty.") {
			continue
		}
		if !strings.Contains(allow, ".node.lab-1.") {
			t.Errorf("agent pty sub allow %q must pin node=lab-1.\n\n"+
				"A wildcard here is the fan-out: nats-server delivers every other node's "+
				"keystrokes to this agent, and no product-level pid check can un-send them.", allow)
		}
		switch {
		case strings.HasSuffix(allow, ".pty.*.in"):
			nodeScoped["in"] = true
		case strings.HasSuffix(allow, ".pty.*.resize"):
			nodeScoped["resize"] = true
		}
	}
	for _, want := range []string{"in", "resize"} {
		if !nodeScoped[want] {
			t.Errorf("the agent has NO node-scoped `.%s` grant, so its intake can only be the "+
				"session-wide one.\n\nSub.Allow = %v", want, perms.Sub.Allow)
		}
	}
}

// origin: prerelease audit round 2, I-F6.
//
// The publisher's side. A member may drive any node in its OWN session, so the nid stays
// a wildcard here — the narrowing that matters is the agent's Sub above. What must hold
// is that the grant exists at all: without it an upgraded ctl's node-scoped publish is
// refused by the server and the operator types into a black hole.
func TestAMemberMayPublishNodeScopedPtyInput(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab", false)
	want := map[string]bool{
		subjectPrefix + ".s.lab.node.*.pty.*.in":     false,
		subjectPrefix + ".s.lab.node.*.pty.*.resize": false,
	}
	for _, allow := range perms.Pub.Allow {
		if _, ok := want[allow]; ok {
			want[allow] = true
		}
	}
	for subj, granted := range want {
		if !granted {
			t.Errorf("a member cannot publish %q.\n\n"+
				"The agent now subscribes there; without the matching Pub grant the server "+
				"refuses the publish and every interactive keystroke is silently dropped.", subj)
		}
	}
}

// origin: prerelease audit round 2, the ctl half of the L1-F1 surface.
//
// ONLY A RESPONDER MAY PUBLISH INTO THE REPLY SPACE.
//
// A ctl never publishes INTO an inbox: nc.PublishRequest sends to the SERVICE subject
// and merely names its inbox in the reply field, which needs no permission on the inbox.
// So `Pub "_INBOX.>"` on the two client templates was pure surplus — and the unactivated
// one is handed to ANY connection presenting a syntactically valid nkey, i.e. it let the
// internet publish into every other connection's reply space.
//
// The agent and the broker KEEP it, and must: msg.Respond publishes to the requester's
// inbox, and exec/run stream MANY messages to one reply subject, so nats' response
// permissions (which bound the count) are not a drop-in for them.
//
// The old positive control in test/d3 published to `_INBOX.d3probe` purely because it
// was "something allowed" — which is how a surplus grant survives: the only thing
// exercising it was a test using it as scenery.
func TestOnlyResponderTemplatesMayPublishIntoTheReplySpace(t *testing.T) {
	clients := map[string]jwt.Permissions{
		"unactivated":      PermissionsForUnactivated(sampleActor, false),
		"activated member": PermissionsForActivatedMember(sampleActor, "lab", false),
	}
	for name, perms := range clients {
		for _, allow := range perms.Pub.Allow {
			if strings.HasPrefix(allow, "_INBOX") {
				t.Errorf("the %s template may publish %q.\n\n"+
					"A ctl has no reason to publish into a reply inbox, and this template is "+
					"reachable with no credential at all — so the grant hands the internet a "+
					"write into every connection's reply space. Removing it costs nothing: "+
					"PublishRequest needs permission on the SERVICE subject, not the inbox.",
					name, allow)
			}
		}
	}

	// The responders must still have it, or every request/reply on the deployment stops.
	responders := map[string]jwt.Permissions{
		"agent":  PermissionsForAgent("lab", "lab-1", sampleActor, true),
		"broker": PermissionsForBroker(),
	}
	for name, perms := range responders {
		found := false
		for _, allow := range perms.Pub.Allow {
			if allow == "_INBOX.>" {
				found = true
			}
		}
		if !found {
			t.Errorf("the %s template can no longer publish into the reply space.\n\n"+
				"msg.Respond publishes to the REQUESTER's inbox; without this grant every "+
				"reply on the deployment is refused by the server and every ctl command "+
				"times out.", name)
		}
	}
}
