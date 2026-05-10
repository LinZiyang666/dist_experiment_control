package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadAgentYAMLRejectsTraversalSID — security regression. Before
// this fix, loadAgentYAML did filepath.Join(home, "agent", sid,
// "agent.yaml") and returned whatever it found. With sid =
// "../../../etc/passwd", filepath.Clean produces "/etc/passwd/agent.yaml"
// — i.e. fully outside <home>. On a multi-user box where one
// account writes a malicious agent.yaml elsewhere, that account
// could trick another user's `tether agent --session <evil>` into
// reading it. The fix: ValidateSID before path concatenation.
//
// Test driving: feed each known-malicious sid; require an error
// AND require the load NOT to actually open anything outside home.
func TestLoadAgentYAMLRejectsTraversalSID(t *testing.T) {
	home := t.TempDir()

	cases := []struct {
		name string
		sid  string
	}{
		{"dotdot_root", "../../../etc/passwd"},
		{"dotdot_relative", "../escape"},
		{"absolute_path", "/etc/passwd"},
		{"slash_separator", "lab/extra"},
		{"backslash", `lab\extra`},
		{"nul_byte", "lab\x00.yaml"},
		{"empty", ""},
		{"reserved_admin", "admin"},
		{"reserved_glob", "*"},
		{"too_long", strings.Repeat("a", 100)},
		{"uppercase", "LAB"},
		{"leading_dash", "-lab"},
		{"trailing_dash", "lab-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ay, err := loadAgentYAML(home, tc.sid)
			if err == nil {
				t.Errorf("malicious sid %q: expected error, got nil + %+v",
					tc.sid, ay)
			}
			if ay != (agentYAML{}) {
				t.Errorf("malicious sid %q: returned non-zero config %+v",
					tc.sid, ay)
			}
		})
	}
}

// TestLoadAgentYAMLAcceptsValidSID is the symmetric positive: known-
// good sids still work. Without this, a too-strict regex regression
// would lock everyone out without obvious failure mode in the tests
// above (which only check rejection).
func TestLoadAgentYAMLAcceptsValidSID(t *testing.T) {
	home := t.TempDir()
	for _, sid := range []string{
		"lab", "lab-1", "g3-prod", "session-2026-04-01",
		"a", "abc123", "z9",
	} {
		t.Run(sid, func(t *testing.T) {
			// File doesn't exist → should still return zero
			// struct + nil error (the documented "no install"
			// path).
			ay, err := loadAgentYAML(home, sid)
			if err != nil {
				t.Fatalf("valid sid %q rejected: %v", sid, err)
			}
			if ay != (agentYAML{}) {
				t.Errorf("valid sid %q with no file: expected zero, got %+v",
					sid, ay)
			}
		})
	}
}

// TestLoadAgentYAMLDoesNotEscapeHomeWithMaliciousSID is the actual
// containment test. Plant an agent.yaml file at a path that a
// pre-fix loadAgentYAML would have read; require the post-fix
// version not to read it.
func TestLoadAgentYAMLDoesNotEscapeHomeWithMaliciousSID(t *testing.T) {
	parent := t.TempDir()
	home := filepath.Join(parent, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}

	// Plant the bait at parent/agent/escape/agent.yaml — exactly
	// where filepath.Join(home, "agent", "../escape", "agent.yaml")
	// resolves under filepath.Clean.
	bait := filepath.Join(parent, "agent", "escape")
	if err := os.MkdirAll(bait, 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte("broker_url: wss://attacker.example/\nsession: leaked\n")
	if err := os.WriteFile(filepath.Join(bait, "agent.yaml"), body, 0o600); err != nil {
		t.Fatal(err)
	}

	ay, err := loadAgentYAML(home, "../escape")
	if err == nil && ay.BrokerURL == "wss://attacker.example/" {
		t.Fatal("path traversal succeeded: loadAgentYAML read planted bait — " +
			"the ValidateSID check is missing or bypassed")
	}
	// Belt + suspenders: even if the validator changes shape later,
	// any non-empty result here is wrong.
	if ay.BrokerURL != "" {
		t.Errorf("loadAgentYAML returned non-empty BrokerURL %q for "+
			"malicious sid; expected zero or error", ay.BrokerURL)
	}
}
