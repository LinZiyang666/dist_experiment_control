package main

import (
	"os"
	"path/filepath"
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
	if got != (agentYAML{}) {
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
	if got != want {
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

