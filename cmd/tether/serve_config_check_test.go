// serve_config_check_test.go — `tether serve --config-check` (review M6): the
// upgrade-safe config validator. It must validate the fully-resolved config
// and exit BEFORE any side effect — no DB open, no migration — so an operator
// can validate a strict broker.yaml with the NEW binary during an upgrade
// without that binary migrating the live tether.db (which plain `serve` does,
// poisoning a decide-not-to-flip rollback — broker-ops §8.4).
// origin: docs/deploy-tier-gotchas.md #75
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runServeConfigCheck(t *testing.T, configPath string) (string, error) {
	t.Helper()
	cmd := newServeCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--config", configPath, "--config-check"})
	err := cmd.Execute()
	return out.String(), err
}

func TestServeConfigCheckAcceptsValidAndRejectsStrict(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.yaml")
	if err := os.WriteFile(good, []byte("broker:\n  domain: example.com\n  storage:\n    db: /var/lib/tether/tether.db\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := runServeConfigCheck(t, good)
	if err != nil {
		t.Fatalf("valid config must pass --config-check: %v\n%s", err, out)
	}
	if !strings.Contains(out, "config OK") || !strings.Contains(out, "mode=single") {
		t.Fatalf("--config-check must report OK + mode; got: %q", out)
	}

	// A mis-nested / typo'd key must FAIL loudly (the #75 strict decoder),
	// so an operator catches it with the new binary before flipping.
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("broker:\n  domain: example.com\nobservability:\n  log_file: /x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runServeConfigCheck(t, bad); err == nil {
		t.Fatal("--config-check must reject a mis-nested broker.yaml")
	}
}

func TestServeConfigCheckHasNoDBSideEffect(t *testing.T) {
	// The whole point (M6): validating with the new binary must NOT touch the
	// DB. Point --config-check's db at a path that does NOT exist and assert
	// it is STILL absent after — a real `serve` would storage.Open + migrate
	// it into existence.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  storage:\n    db: "+dbPath+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runServeConfigCheck(t, cfg); err != nil {
		t.Fatalf("config-check should pass: %v", err)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("--config-check created/opened the DB at %s (must have zero side effect): stat err=%v", dbPath, err)
	}
}

// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F1
func TestServeConfigCheckRejectsPostLoadValidationErrors(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	// newLoggerTo rejects this during a real serve startup. A config preflight
	// that returns before that validation gives the operator a false "config
	// OK", then the actual upgrade restart fails on the same file.
	if err := os.WriteFile(cfg, []byte("broker:\n  observability:\n    log_level: definitely-not-a-level\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := runServeConfigCheck(t, cfg); err == nil {
		t.Fatalf("--config-check accepted a log level that real serve startup rejects; output=%q", out)
	}
}

// TestServeConfigCheckRejectsWebhookAndListenErrors extends F1's differential
// coverage: config-check and a real serve must reach the same verdict on the
// alert webhook URL and on listen-address format — both validated by the
// shared broker.ValidateConfig.
// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F1
func TestServeConfigCheckRejectsWebhookAndListenErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "forbidden webhook scheme",
			body: "broker:\n  observability:\n    alert_webhook_url: ftp://invalid.example/hook\n",
		},
		{
			name: "webhook with userinfo (secret-in-url)",
			body: "broker:\n  observability:\n    alert_webhook_url: https://user:pw@example.com/hook\n",
		},
		{
			name: "malformed NATS URL",
			body: "broker:\n  nats:\n    url: \"://not-a-url\"\n",
		},
		{
			name: "mixed websocket and TCP NATS pool",
			body: "broker:\n  nats:\n    url: \"nats://127.0.0.1:4222,ws://127.0.0.1:8080\"\n",
		},
		{
			name: "NATS URL with invalid port",
			body: "broker:\n  nats:\n    url: \"nats://127.0.0.1:notaport\"\n",
		},
		{
			name: "malformed sub listen port",
			body: "broker:\n  sub:\n    listen: 127.0.0.1:notaport\n",
		},
		{
			name: "subscription listener must be loopback",
			body: "broker:\n  sub:\n    listen: 0.0.0.0:8090\n",
		},
		{
			name: "metrics listen out of range port",
			body: "broker:\n  observability:\n    metrics_listen: 0.0.0.0:99999\n",
		},
		{
			name: "metrics listen malformed DNS host",
			body: "broker:\n  observability:\n    metrics_listen: bad$host:9090\n",
		},
		{
			name: "metrics listen zone on non-IP host",
			body: "broker:\n  observability:\n    metrics_listen: \"[example.com%eth0]:9090\"\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := filepath.Join(dir, "broker.yaml")
			if err := os.WriteFile(cfg, []byte(tc.body), 0o644); err != nil {
				t.Fatal(err)
			}
			if out, err := runServeConfigCheck(t, cfg); err == nil {
				t.Fatalf("--config-check accepted a config a real serve rejects; output=%q", out)
			}
		})
	}
}

// TestServeConfigCheckAcceptsValidWebhookAndListen is the paired positive case:
// a well-formed webhook + loopback listen must PASS (no false rejection).
func TestServeConfigCheckAcceptsValidWebhookAndListen(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	body := "broker:\n" +
		"  observability:\n" +
		"    alert_webhook_url: https://ops.example.com/hook\n" +
		"    metrics_listen: 127.0.0.1:9090\n" +
		"  sub:\n" +
		"    listen: 127.0.0.1:8090\n"
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := runServeConfigCheck(t, cfg); err != nil {
		t.Fatalf("valid config with webhook + listen must pass; err=%v out=%q", err, out)
	}
}

// net.Listen and the real loopback-only sub listener accept port 0 to request
// an ephemeral port. A preflight-only 1..65535 restriction would reject a
// configuration that the listener itself can start, which is the opposite
// direction of the same F1 parity contract.
// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F1
func TestServeConfigCheckAcceptsEphemeralLoopbackListen(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  sub:\n    listen: 127.0.0.1:0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := runServeConfigCheck(t, cfg); err != nil {
		t.Fatalf("config-check rejected a loopback address the real listener accepts; err=%v out=%q", err, out)
	}
}
