package proto

// Boundary tests for identifier validators and subject parsers.
//
// The existing identifiers_test.go covers smoke-level happy/sad cases.
// This file goes further: it systematically table-drives wildcard
// characters (NATS '.' '*' '>'), control bytes, unicode, length cliffs,
// and the round-trip property between SubjCmdBy / ParseCmdBy.
//
// Whenever a subject parser silently accepts a string the validators
// reject (or vice versa) we have a potential subject-injection
// vector — calling those out is the primary motivation for these
// tests.

import (
	"strings"
	"testing"
)

// validActor is a syntactically-valid actor token (matches the
// `^U[A-Z2-7]{55}$` regex that ValidateActorToken requires). Used as
// filler in subject round-trip tests.
const validActor = "U" + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" // 1+55

// ---------- ValidateSID boundary table -----------------------------

func TestValidateSIDBoundaries(t *testing.T) {
	cases := []struct {
		name string
		s    string
		ok   bool
	}{
		{"empty", "", false},
		{"single_letter", "a", true},
		{"single_digit_first", "1", false},          // must start with [a-z]
		{"single_dash", "-", false},                 // must start with [a-z]
		{"contains_dot", "lab.1", false},            // NATS subject separator
		{"contains_star", "lab*", false},            // NATS partial wildcard
		{"contains_gt", "lab>", false},              // NATS terminal wildcard
		{"contains_space", "lab 1", false},
		{"contains_tab", "lab\t1", false},
		{"contains_nul", "lab\x001", false},
		{"contains_uppercase", "Lab", false},
		{"contains_underscore", "lab_1", false},
		{"unicode_lab01", "实验室01", false},          // outside [a-z0-9-]
		{"len_1_letter", "a", true},
		{"len_26", "a" + strings.Repeat("b", 25), true},
		{"len_32", "a" + strings.Repeat("b", 31), true},
		{"len_33", "a" + strings.Repeat("b", 32), false},
		{"len_256", "a" + strings.Repeat("b", 255), false},
		{"len_1024", "a" + strings.Repeat("b", 1023), false},
		{"all_letters", "abcdefgh", true},
		{"alpha_then_digits", "a1234", true},
		{"trailing_dash", "lab-", false},
		{"middle_dash", "lab-1", true},
		{"reserved_default", "default", false},
		{"reserved_prefix_system", "system-x", false},
		{"reserved_prefix_ctl", "ctl-foo", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := ValidateSID(c.s)
			gotOK := err == nil
			if gotOK != c.ok {
				t.Fatalf("ValidateSID(%q): ok=%v want %v (err=%v)", c.s, gotOK, c.ok, err)
			}
		})
	}
}

// ---------- ValidateNID boundary table -----------------------------

func TestValidateNIDBoundaries(t *testing.T) {
	cases := []struct {
		name string
		s    string
		ok   bool
	}{
		{"empty", "", false},
		{"single_letter", "a", true},
		{"single_digit", "1", true},                // nid allows leading digit
		{"single_dash", "-", true},                 // nid allows leading dash
		{"contains_dot", "node.with.dots", false},
		{"contains_star", "node*", false},
		{"contains_gt", "node>", false},
		{"contains_space", "node 1", false},
		{"contains_tab", "node\t1", false},
		{"contains_nul", "node\x001", false},
		{"uppercase", "Node-1", false},
		{"underscore", "node_1", false},
		{"unicode", "节点1", false},
		{"trailing_dash_allowed", "node-", true}, // documented in identifiers.go
		{"good_lab1", "lab-1", true},
		{"good_gpu01", "gpu-01", true},
		{"len_32", strings.Repeat("a", 32), true},
		{"len_33", strings.Repeat("a", 33), false},
		{"len_256", strings.Repeat("a", 256), false},
		{"len_1024", strings.Repeat("a", 1024), false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := ValidateNID(c.s)
			gotOK := err == nil
			if gotOK != c.ok {
				t.Fatalf("ValidateNID(%q): ok=%v want %v (err=%v)", c.s, gotOK, c.ok, err)
			}
		})
	}
}

// ---------- ValidateActorToken boundary table ----------------------

func TestValidateActorTokenBoundaries(t *testing.T) {
	cases := []struct {
		name string
		s    string
		ok   bool
	}{
		{"empty", "", false},
		{"too_short", "U" + strings.Repeat("A", 54), false},
		{"too_long", "U" + strings.Repeat("A", 56), false},
		{"good_uppercase_A", validActor, true},
		{"lower_prefix", "u" + strings.Repeat("A", 55), false},
		{"contains_equals", "U" + strings.Repeat("A", 54) + "=", false},
		{"all_digits_after_U", "U" + strings.Repeat("1", 55), false}, // 1 not in [A-Z2-7]
		{"contains_lowercase", "U" + strings.Repeat("a", 55), false},
		{"good_with_2_to_7", "U" + strings.Repeat("234567", 9) + "Z", true},
		{"contains_dot", "U" + strings.Repeat("A", 27) + "." + strings.Repeat("A", 27), false},
		{"contains_star", "U" + strings.Repeat("A", 27) + "*" + strings.Repeat("A", 27), false},
		{"contains_gt", "U" + strings.Repeat("A", 27) + ">" + strings.Repeat("A", 27), false},
		{"contains_nul", "U" + strings.Repeat("A", 27) + "\x00" + strings.Repeat("A", 27), false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := ValidateActorToken(c.s)
			gotOK := err == nil
			if gotOK != c.ok {
				t.Fatalf("ValidateActorToken(%q): ok=%v want %v (err=%v)", c.s, gotOK, c.ok, err)
			}
		})
	}
}

// ---------- ParseCmdBy round-trip property -------------------------

// Subject pair generators for round-trip tests. We use a curated
// catalogue rather than randomised generation because the input space
// is dense and the failure modes are predictable; if the property holds
// for these representative shapes, it holds for the whole accepted
// charset (idCharset is a regexp without any non-ASCII branches).
var (
	roundtripSIDs = []string{"a", "lab", "lab-1", "abcd1234", "a" + strings.Repeat("b", 31)}
	roundtripNIDs = []string{"a", "1gpu", "lab-1", "node-007", "default", strings.Repeat("a", 32)}
	roundtripVerbs = []string{"run", "exec", "kill", "expose", "expose-rm", "upgrade"}
)

func TestParseCmdByRoundtrip(t *testing.T) {
	for _, sid := range roundtripSIDs {
		for _, nid := range roundtripNIDs {
			for _, verb := range roundtripVerbs {
				subj := SubjCmdBy(sid, validActor, nid, verb)
				gotSid, gotActor, gotNid, gotVerb, ok := ParseCmdBy(subj)
				if !ok {
					t.Errorf("ParseCmdBy(%q) = ok=false; want true (sid=%q nid=%q verb=%q)",
						subj, sid, nid, verb)
					continue
				}
				if gotSid != sid || gotActor != validActor || gotNid != nid || gotVerb != verb {
					t.Errorf("roundtrip drift: subj=%q decoded=(sid=%q actor=%q nid=%q verb=%q) want=(sid=%q actor=%q nid=%q verb=%q)",
						subj, gotSid, gotActor, gotNid, gotVerb, sid, validActor, nid, verb)
				}
			}
		}
	}
}

// ---------- ParseCmdBy malformed-subject rejection -----------------

func TestParseCmdByRejectsMalformed(t *testing.T) {
	good := SubjCmdBy("lab", validActor, "lab-1", "run")
	cases := []struct {
		name string
		s    string
	}{
		{"empty", ""},
		{"just_prefix", SubjectPrefix},
		{"too_few_tokens", "tether.v1.s.lab.cmd.by." + validActor + ".node.lab-1"},
		{"too_many_tokens", good + ".extra"},
		{"wrong_version", strings.Replace(good, ".v1.", ".v2.", 1)},
		{"wrong_root", strings.Replace(good, "tether.", "tetherx.", 1)},
		{"missing_cmd_segment", strings.Replace(good, ".cmd.", ".xmd.", 1)},
		{"missing_by_segment", strings.Replace(good, ".by.", ".xy.", 1)},
		{"missing_node_segment", strings.Replace(good, ".node.", ".xnode.", 1)},
		{"missing_req_suffix", strings.TrimSuffix(good, ".req")},
		{"all_dots", strings.Repeat(".", 10)},
		{"all_blank", "          "},
		{"sid_invalid_uppercase",
			"tether.v1.s.LAB.cmd.by." + validActor + ".node.lab-1.run.req"},
		{"actor_invalid_short",
			"tether.v1.s.lab.cmd.by.UAAA.node.lab-1.run.req"},
		{"nid_with_dot_breaks_token_count",
			"tether.v1.s.lab.cmd.by." + validActor + ".node.lab.1.run.req"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, _, _, _, ok := ParseCmdBy(c.s); ok {
				t.Fatalf("ParseCmdBy(%q) accepted a malformed subject", c.s)
			}
		})
	}
}

// ---------- ParseEvProc round-trip + rejection ---------------------

func TestParseEvProcRoundtrip(t *testing.T) {
	cases := []struct {
		sid, nid, pid, kind string
	}{
		{"lab", "lab-1", "01hzxk", "started"},
		{"lab", "lab-1", "01hzxk", "exit"},
		{"lab", "lab-1", "01hzxk", "future_kind"}, // parser is verb-agnostic
	}
	for _, c := range cases {
		subj := SubjEvProc(c.sid, c.nid, c.pid, c.kind)
		s, n, p, k, ok := ParseEvProc(subj)
		if !ok {
			t.Errorf("ParseEvProc(%q) ok=false", subj)
			continue
		}
		if s != c.sid || n != c.nid || p != c.pid || k != c.kind {
			t.Errorf("ParseEvProc drift: subj=%q got=(%q,%q,%q,%q)", subj, s, n, p, k)
		}
	}
}

func TestParseEvProcRejectsMalformed(t *testing.T) {
	good := SubjEvProc("lab", "lab-1", "01h", "exit")
	cases := []string{
		"",
		SubjectPrefix,
		good + ".extra",
		strings.TrimSuffix(good, ".exit"),
		strings.Replace(good, ".v1.", ".v2.", 1),
		strings.Replace(good, ".ev.", ".cmd.", 1),
		strings.Replace(good, ".node.", ".xnode.", 1),
		strings.Replace(good, ".proc.", ".xproc.", 1),
		"tether.v1.s.LAB.ev.node.lab-1.proc.01h.exit",          // sid invalid
		"tether.v1.s.lab.ev.node.LAB.proc.01h.exit",            // nid invalid
		strings.Repeat(".", 9),
	}
	for i, s := range cases {
		if _, _, _, _, ok := ParseEvProc(s); ok {
			t.Errorf("case %d: ParseEvProc(%q) accepted malformed subject", i, s)
		}
	}
}

// ---------- ParseSidNidFromCtrl round-trip + rejection -------------

func TestParseSidNidFromCtrlRoundtrip(t *testing.T) {
	for _, sid := range []string{"a", "lab", "lab-1"} {
		for _, nid := range []string{"a", "lab-1", "node-007", "1gpu"} {
			subj := SubjNodeRegister(sid, nid)
			s, n, ok := ParseSidNidFromCtrl(subj)
			if !ok || s != sid || n != nid {
				t.Errorf("ParseSidNidFromCtrl(%q) = (%q,%q,%v); want (%q,%q,true)",
					subj, s, n, ok, sid, nid)
			}
		}
	}
}

func TestParseSidNidFromCtrlRejectsMalformed(t *testing.T) {
	good := SubjNodeRegister("lab", "lab-1")
	cases := []string{
		"",
		SubjectPrefix,
		strings.Replace(good, ".v1.", ".v2.", 1),
		strings.Replace(good, ".ctrl.", ".cmd.", 1),
		strings.Replace(good, ".s.lab.", ".x.lab.", 1),
		strings.Replace(good, ".node.", ".xnode.", 1),
		"tether.v1.ctrl.s.LAB.node.lab-1.register.req", // sid invalid
		"tether.v1.ctrl.s.lab.node.LAB.register.req",   // nid invalid
		"tether.v1.ctrl.s.lab.node.lab-1",              // too short
	}
	for i, s := range cases {
		if _, _, ok := ParseSidNidFromCtrl(s); ok {
			t.Errorf("case %d: ParseSidNidFromCtrl(%q) accepted malformed", i, s)
		}
	}
}

// TestSubjectInjectionViaSubjCmdBy documents a pre-existing wire-layer
// behaviour: SubjCmdBy is fmt.Sprintf-only, it does NOT validate sid /
// nid / actor / verb before splicing them into the subject string.
//
// As a result, a caller passing sid="lab.>" or nid="*" will produce a
// well-formed-looking but adversarial subject. ParseCmdBy DOES catch
// these on the receive side (because '.' breaks the token count and
// '*' fails ValidateSID/NID), so the bug is contained: malicious input
// reaches NATS as a subscription error, not as a privilege escalation.
//
// We pin both directions:
//
//   1. SubjCmdBy embeds the wildcard verbatim (current behaviour),
//   2. ParseCmdBy refuses to round-trip the resulting subject.
//
// If a future refactor adds validation to SubjCmdBy, point (2) still
// holds and point (1) becomes a panic / error — which is fine; this
// test will start failing and the dev will update it.
func TestSubjectInjectionViaSubjCmdBy(t *testing.T) {
	type inj struct{ sid, actor, nid, verb string }
	cases := []inj{
		{"lab.>", validActor, "lab-1", "run"},
		{"lab", validActor, "*", "run"},
		{"lab", validActor, "lab-1", "run.>"},
		{"lab", "*", "lab-1", "run"},
	}
	for _, c := range cases {
		subj := SubjCmdBy(c.sid, c.actor, c.nid, c.verb)
		// 1. Builder embedded the input verbatim.
		if !strings.Contains(subj, c.sid) || !strings.Contains(subj, c.nid) ||
			!strings.Contains(subj, c.verb) || !strings.Contains(subj, c.actor) {
			t.Fatalf("SubjCmdBy did not embed inputs verbatim: subj=%q inputs=%+v", subj, c)
		}
		// 2. ParseCmdBy refuses to accept the malformed subject.
		if _, _, _, _, ok := ParseCmdBy(subj); ok {
			t.Fatalf("ParseCmdBy accepted an injected subject: %q (inputs=%+v)", subj, c)
		}
	}
}
