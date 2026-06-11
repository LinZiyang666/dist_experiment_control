package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestLoadAgentYAMLMissingFile: missing agent.yaml is the unhappy
// path operators see during dev (no install.sh ran). It MUST come
// back as an empty struct + nil error so the caller can fall
// through to flag defaults.
func TestLoadAgentYAMLMissingFile(t *testing.T) {
	home := t.TempDir()
	got, err := loadAgentYAML(home, "lab")
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if !reflect.DeepEqual(got, agentYAML{}) {
		t.Errorf("missing file should yield zero struct; got %+v", got)
	}
}

// TestLoadAgentYAMLFullFile pins the install.sh-generated yaml
// shape: every documented field round-trips into the matching
// struct field. This is the test that would have caught a
// "loadAgentYAML reads but doesn't unmarshal broker_url" regression
// — the reviewer's TestReviewAgentInstallStartPathUsesConfiguredBroker
// only greps source for the strings, so it passes even when the
// behavior is broken.
func TestLoadAgentYAMLFullFile(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte("broker_url: wss://broker.example.com:443\n" +
		"session: lab\n" +
		"nid: lab-1\n" +
		"tunnel_addr: broker.example.com:7000\n")
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadAgentYAML(home, "lab")
	if err != nil {
		t.Fatal(err)
	}
	want := agentYAML{
		BrokerURL:  "wss://broker.example.com:443",
		Session:    "lab",
		NID:        "lab-1",
		TunnelAddr: "broker.example.com:7000",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("yaml round-trip:\n  got  %+v\n  want %+v", got, want)
	}
}

// TestLoadAgentYAMLPartial: install.sh might be older than the
// running binary, so a yaml without tunnel_addr should still
// load — missing fields stay zero-valued.
func TestLoadAgentYAMLPartial(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte("broker_url: wss://broker.example.com:443\nsession: lab\nnid: lab-1\n")
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadAgentYAML(home, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if got.BrokerURL != "wss://broker.example.com:443" {
		t.Errorf("broker_url not loaded: %+v", got)
	}
	if got.TunnelAddr != "" {
		t.Errorf("missing tunnel_addr should stay empty; got %q", got.TunnelAddr)
	}
}

func TestExternalReviewAgentYAMLExposesPrivateDestinationOptIn(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte("session: lab\nnid: lab-1\nproxy:\n  allow_private_destinations: true\n")
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadAgentYAML(home, "lab")
	if err != nil {
		t.Fatal(err)
	}

	proxy := reflect.ValueOf(got).FieldByName("Proxy")
	if !proxy.IsValid() {
		t.Fatal("documented proxy.allow_private_destinations setting is not represented in agentYAML")
	}
	allow := proxy.FieldByName("AllowPrivateDestinations")
	if !allow.IsValid() || allow.Kind() != reflect.Bool || !allow.Bool() {
		t.Fatal("documented proxy.allow_private_destinations setting was not loaded as true")
	}
}

// TestLoadAgentYAMLMalformed: a typo in the operator's
// hand-edited yaml should surface as an error rather than silent
// fall-through to flag defaults — otherwise the operator wonders
// why their changes have no effect.
func TestLoadAgentYAMLMalformed(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"),
		[]byte("broker_url: \"unterminated\nstring\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadAgentYAML(home, "lab")
	if err == nil {
		t.Errorf("malformed yaml should error")
	}
}

func TestLoadAgentYAMLRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "misspelled allow_roots cannot fall open",
			body: "session: lab\nnid: lab-1\nfile_transfer:\n  allow_root:\n    - /srv/data\n",
		},
		{
			name: "misspelled top-level file_transfer cannot fall open",
			body: "session: lab\nnid: lab-1\nfile_transfers:\n  allow_roots: []\n",
		},
		{
			name: "unknown proxy option rejected",
			body: "session: lab\nnid: lab-1\nproxy:\n  allow_private_destination: true\n",
		},
		{
			name: "second document cannot hide narrowing",
			body: "session: lab\nnid: lab-1\n---\nfile_transfer:\n  allow_roots: []\n",
		},
		{
			// F11: an empty MIDDLE document must not let a later non-empty
			// document slip through the multi-doc check (was a fail-open).
			name: "empty middle document cannot hide a later narrowing",
			body: "session: lab\nnid: lab-1\n---\n# empty\n---\nfile_transfer:\n  allow_roots: []\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			dir := filepath.Join(home, "agent", "lab")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadAgentYAML(home, "lab"); err == nil {
				t.Fatal("unknown field was silently ignored")
			}
		})
	}
}

// Regression: strict decoding (KnownFields + multi-doc reject) must still
// TOLERATE a benign empty / comment-only / whitespace-only agent.yaml (zero
// struct → caller falls through to CLI flags) and a lone trailing `---`.
// An empty placeholder config failing agent startup was a strict-parsing
// over-rejection; the real-second-document rejection
// (TestLoadAgentYAMLRejectsUnknownFields) stays intact.
func TestLoadAgentYAML_ToleratesEmptyAndTrailingSeparator(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty file", ""},
		{"comment only", "# nothing configured here\n"},
		{"whitespace only", "\n\n"},
		{"trailing separator", "session: lab\nnid: a100\n---\n"},
		{"multiple empty trailing docs", "session: lab\nnid: a100\n---\n---\n# trailing\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			dir := filepath.Join(home, "agent", "lab")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadAgentYAML(home, "lab"); err != nil {
				t.Errorf("benign config rejected by strict decoder: %v", err)
			}
		})
	}
}

// TestPickFlagOrYamlOnAgentCmd pins the cross-cmd reuse of the
// helper: pickFlagOrYaml was originally written for the serve
// command but the agent-yaml wiring depends on it doing the right
// thing through an agent cobra command instance too.
func TestPickFlagOrYamlOnAgentCmd(t *testing.T) {
	cmd := newAgentCmd()
	// Default state: no flag changed → yaml wins when non-empty.
	if got := pickFlagOrYaml(cmd, "nats-url", "nats://127.0.0.1:4222", "wss://yaml.example/"); got != "wss://yaml.example/" {
		t.Errorf("default+yaml: got %q want yaml value", got)
	}
	// Default state: yaml empty → cobra default sticks.
	if got := pickFlagOrYaml(cmd, "nats-url", "nats://127.0.0.1:4222", ""); got != "nats://127.0.0.1:4222" {
		t.Errorf("default+empty yaml: got %q want cobra default", got)
	}
	// Explicit flag changes precedence.
	if err := cmd.Flags().Set("nats-url", "nats://flag.example/"); err != nil {
		t.Fatal(err)
	}
	if got := pickFlagOrYaml(cmd, "nats-url", "nats://flag.example/", "wss://yaml.example/"); got != "nats://flag.example/" {
		t.Errorf("explicit flag: got %q want flag value", got)
	}
	// Sanity: tunnel-addr shares the helper too.
	cmd2 := newAgentCmd()
	if got := pickFlagOrYaml(cmd2, "tunnel-addr", "127.0.0.1:7000", "broker.example:7000"); got != "broker.example:7000" {
		t.Errorf("tunnel-addr default+yaml: got %q want yaml", got)
	}
}

// The transfer-unrestrict tri-state (open / narrow / disabled) hinges on
// yaml.v3 distinguishing "allow_roots key absent" (slice stays nil → OPEN)
// from "allow_roots: []" (non-nil len-0 slice → DISABLED). cmd/tether
// derives Config.RootsConfigured from exactly `AllowRoots != nil`, so this
// table pins the decoder quirk the whole design depends on — a yaml lib
// bump that collapsed nil→[] would silently disable transfer on every
// fresh install (the inverse failure), and this test catches it.
func TestLoadAgentYAML_AllowRootsNilVsEmpty(t *testing.T) {
	const base = "broker_url: wss://b:443\nsession: lab\nnid: a100\n"
	tests := []struct {
		name           string
		ftBlock        string
		wantNil        bool // AllowRoots == nil → OPEN discriminator
		wantLen        int
		wantConfigured bool // == (AllowRoots != nil) → what cmd/tether stamps
	}{
		{"no file_transfer block", "", true, 0, false},
		{"empty file_transfer block", "file_transfer: {}\n", true, 0, false},
		{"allow_roots null", "file_transfer:\n  allow_roots:\n", true, 0, false},
		{"allow_roots tilde", "file_transfer:\n  allow_roots: ~\n", true, 0, false},
		{"allow_roots explicit empty", "file_transfer:\n  allow_roots: []\n", false, 0, true},
		{"allow_roots one entry", "file_transfer:\n  allow_roots:\n    - /tmp\n", false, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			dir := filepath.Join(home, "agent", "lab")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), []byte(base+tc.ftBlock), 0o600); err != nil {
				t.Fatal(err)
			}
			ay, err := loadAgentYAML(home, "lab")
			if err != nil {
				t.Fatalf("loadAgentYAML: %v", err)
			}
			if gotNil := ay.FileTransfer.AllowRoots == nil; gotNil != tc.wantNil {
				t.Errorf("AllowRoots==nil = %v, want %v", gotNil, tc.wantNil)
			}
			if got := len(ay.FileTransfer.AllowRoots); got != tc.wantLen {
				t.Errorf("len = %d, want %d", got, tc.wantLen)
			}
			// The exact expression cmd/tether feeds into Config.RootsConfigured.
			if gotConfigured := ay.FileTransfer.AllowRoots != nil; gotConfigured != tc.wantConfigured {
				t.Errorf("RootsConfigured = %v, want %v", gotConfigured, tc.wantConfigured)
			}
		})
	}
}
