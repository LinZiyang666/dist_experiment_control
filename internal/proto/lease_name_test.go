package proto

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2
//
// The lease-name grammar decides whether a node is treated as EPHEMERAL —
// excluded from fleet upgrades, ineligible for proxy egress. Mis-classifying an
// operator's own device as ephemeral is the expensive direction, so the parser
// is strict and this table pins exactly where the boundary sits.
func TestSplitLeaseNameClassifiesOnlyGenuineLeaseNames(t *testing.T) {
	cases := []struct {
		nid      string
		wantBase string
		wantN    int
		wantLeas bool
		why      string
	}{
		{"gpu1", "gpu1", 0, false, "a plain basename is never a lease"},
		{"gpu1-02", "gpu1", 2, true, "the canonical shape"},
		{"gpu1-99", "gpu1", 99, true, "the ceiling is still a lease"},
		{"a-b-c-07", "a-b-c", 7, true, "a basename may contain dashes"},

		// The boundary cases that protect operator-owned names.
		{"gpu1-2", "gpu1-2", 0, false, "unpadded is NOT a lease: node.List orders nid as TEXT, so accepting both spellings would make ordering depend on which one a broker happened to mint"},
		{"gpu1-002", "gpu1-002", 0, false, "over-wide is not the grammar we mint"},
		{"gpu1-01", "gpu1-01", 0, false, "the basename holder is conceptually 01 and never carries a suffix, so a row spelled this way came from an operator"},
		{"gpu1-00", "gpu1-00", 0, false, "same reasoning as -01"},
		{"gpu1-west", "gpu1-west", 0, false, "a dashed word must not read as a suffix"},
		{"gpu1-x2", "gpu1-x2", 0, false, "non-digits are not a suffix"},
		{"gpu1-2x", "gpu1-2x", 0, false, "trailing non-digit is not a suffix"},
		{"-02", "-02", 0, false, "an empty basename is not a lease name"},
		{"02", "02", 0, false, "too short to be a lease name"},
		{"", "", 0, false, "empty input"},
	}
	for _, c := range cases {
		t.Run(c.nid, func(t *testing.T) {
			base, n, leased := SplitLeaseName(c.nid)
			if base != c.wantBase || n != c.wantN || leased != c.wantLeas {
				t.Fatalf("SplitLeaseName(%q) = (%q, %d, %v), want (%q, %d, %v) — %s",
					c.nid, base, n, leased, c.wantBase, c.wantN, c.wantLeas, c.why)
			}
		})
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2
//
// Every name this package can MINT must survive the validator that four subject
// parsers re-apply to it. If a minted name failed ValidateNID, the instance
// would authenticate and then be unable to route — the worst possible split.
func TestEveryMintableLeaseNameIsAValidNID(t *testing.T) {
	base := strings.Repeat("b", MaxLeaseBasenameLen)
	for n := FirstLeaseSuffix; n <= MaxLeaseSuffix; n++ {
		name := LeaseNameFor(base, n)
		if err := ValidateNID(name); err != nil {
			t.Fatalf("minted lease name %q (len %d) fails ValidateNID: %v", name, len(name), err)
		}
		if gotBase, gotN, leased := SplitLeaseName(name); !leased || gotBase != base || gotN != n {
			t.Fatalf("round-trip failed for %q: got (%q, %d, %v)", name, gotBase, gotN, leased)
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2 (M4)
//
// Zero-padding is load-bearing, not cosmetic: eight production sites order by
// nid as TEXT and the load-bearing one is the /sub proxy list rendered to every
// subscriber. This asserts the property that padding exists to provide —
// lexicographic order equals numeric order — and it is exactly what an
// unpadded implementation breaks.
//
// MUTATION: render the suffix with %d instead of %0*d and this fails at 10.
func TestLeaseNamesSortLexicographicallyInNumericOrder(t *testing.T) {
	var names []string
	for n := FirstLeaseSuffix; n <= MaxLeaseSuffix; n++ {
		names = append(names, LeaseNameFor("node", n))
	}
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for i := range names {
		if names[i] != sorted[i] {
			t.Fatalf("lexicographic order diverges from numeric order at %d: minted %q, sorted %q\n"+
				"this is what unpadded suffixes break, and the visible consequence is the /sub "+
				"document order changing as a fleet grows past nine instances",
				i, names[i], sorted[i])
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2
//
// The suffix budget must be reserved, never taken by truncation: two distinct
// over-long basenames would truncate to the same prefix and silently merge two
// unrelated device families into one lease namespace.
func TestLeaseBudgetLeavesRoomForTheWidestSuffix(t *testing.T) {
	widest := LeaseNameFor(strings.Repeat("b", MaxLeaseBasenameLen), MaxLeaseSuffix)
	if len(widest) != 32 {
		t.Fatalf("the widest mintable lease name is %d chars (%q); the budget must exactly fill "+
			"ValidateNID's 32, otherwise it is either wasteful or unsafe", len(widest), widest)
	}
	if err := ValidateNID(widest); err != nil {
		t.Fatalf("widest lease name rejected: %v", err)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// The instance id is CLIENT-SUPPLIED, so it is validated fail-closed before it
// can reach a log line, a decision or a reply. It must also NOT share
// idCharset, which ValidateSID uses — coupling those two contracts would mean a
// change made for instance ids silently widened the session-id space.
func TestValidateInstanceIDIsStrictAndIndependentOfTheSIDCharset(t *testing.T) {
	good := strings.Repeat("a", 26)
	if err := ValidateInstanceID(good); err != nil {
		t.Fatalf("canonical instance id rejected: %v", err)
	}
	bad := []struct{ in, why string }{
		{"", "empty is never valid; absence is handled by the caller, not here"},
		{strings.Repeat("a", 25), "too short"},
		{strings.Repeat("a", 27), "too long"},
		{strings.Repeat("A", 26), "uppercase would not be subject-safe in the lowercase world"},
		{strings.Repeat("a", 25) + "-", "a dash is legal in a nid but not here"},
		{strings.Repeat("a", 25) + ".", "a dot would split a NATS subject token"},
		{strings.Repeat("a", 25) + "*", "a NATS wildcard must never be accepted"},
		{strings.Repeat("a", 25) + ">", "a NATS full wildcard must never be accepted"},
		{strings.Repeat("a", 25) + "\n", "a newline would forge a log line"},
		{strings.Repeat("a", 25) + "\x00", "NUL"},
	}
	for _, c := range bad {
		if err := ValidateInstanceID(c.in); err == nil {
			t.Fatalf("ValidateInstanceID(%q) accepted; must reject — %s", c.in, c.why)
		}
	}
	// A valid nid that is not a valid instance id, and vice versa: the two
	// contracts must be independently evolvable.
	if err := ValidateInstanceID("gpu1"); err == nil {
		t.Fatal("a short nid must not pass as an instance id")
	}
	if err := ValidateNID(good); err != nil {
		t.Fatalf("sanity: a 26-char lowercase id should also be a legal nid, got %v", err)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §1 I1
//
// I1 (single-instance invariance) in the subject layer: for an agent holding
// its basename — which is every agent on every single-agent device — every
// nid-bearing subject builder must produce exactly what it produced before this
// increment existed. The lease only ever changes the VALUE of the node token,
// never the grammar, so this is a pure-function check with no I/O.
//
// MUTATION: make any builder append or reorder a token, or make a basename
// holder route under a suffix, and this fails.
func TestBasenameHolderSubjectsAreUnchangedByTheLeaseGrammar(t *testing.T) {
	const sid, nid = "lab", "gpu1"
	if _, _, leased := SplitLeaseName(nid); leased {
		t.Fatalf("precondition: %q must not classify as a lease name", nid)
	}
	want := map[string]string{
		"register":     fmt.Sprintf("tether.%s.ctrl.s.%s.node.%s.register.req", SubjectVersionToken, sid, nid),
		"unregister":   fmt.Sprintf("tether.%s.ctrl.s.%s.node.%s.unregister.req", SubjectVersionToken, sid, nid),
		"heartbeat":    fmt.Sprintf("tether.%s.ctrl.s.%s.node.%s.heartbeat", SubjectVersionToken, sid, nid),
		"cmdForwarded": fmt.Sprintf("tether.%s.s.%s.cmd.node.%s.exec.req.forwarded", SubjectVersionToken, sid, nid),
	}
	got := map[string]string{
		"register":     SubjNodeRegister(sid, nid),
		"unregister":   SubjNodeUnregister(sid, nid),
		"heartbeat":    SubjNodeHeartbeat(sid, nid),
		"cmdForwarded": SubjCmdForwarded(sid, nid, "exec"),
	}
	for k, w := range want {
		if got[k] != w {
			t.Fatalf("%s subject drifted:\n got  %q\n want %q", k, got[k], w)
		}
	}
	// And the token count must stay put: dispatchForwarded hard-asserts
	// len(parts)==10 with the verb at index 7, so an extra segment anywhere
	// would be silently dropped as malformed rather than failing loudly.
	if n := len(strings.Split(got["cmdForwarded"], ".")); n != 10 {
		t.Fatalf("forwarded subject has %d tokens, want exactly 10 — the agent's dispatcher hard-asserts this", n)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §1 I2
//
// I2 (suffix equivalence) in the subject layer: a leased instance's subjects
// must be the SAME FUNCTION of its name that a real device's are. If this ever
// needed a translation layer, that layer would be where I2 breaks.
func TestLeasedInstanceSubjectsAreTheSameFunctionOfItsName(t *testing.T) {
	const sid = "lab"
	leased := LeaseNameFor("gpu1", 2)
	if err := ValidateNID(leased); err != nil {
		t.Fatalf("lease name is not addressable: %v", err)
	}
	if got, want := SubjCmdForwarded(sid, leased, "exec"),
		fmt.Sprintf("tether.%s.s.%s.cmd.node.%s.exec.req.forwarded", SubjectVersionToken, sid, leased); got != want {
		t.Fatalf("leased forwarded subject:\n got  %q\n want %q", got, want)
	}
	// Same token count as a basename holder's: an un-upgraded ctl parses it
	// with the same rules and therefore needs no change to address it.
	if a, b := len(strings.Split(SubjCmdForwarded(sid, "gpu1", "exec"), ".")),
		len(strings.Split(SubjCmdForwarded(sid, leased, "exec"), ".")); a != b {
		t.Fatalf("leased subject token count %d != basename token count %d", b, a)
	}
}
