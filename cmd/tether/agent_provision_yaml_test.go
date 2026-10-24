package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// agent_provision_yaml_test.go — real shell/Python generation and production
// loadAgentYAML decoding; Docker, systemd and ONLINE polling are stubbed.
// No agent, Caddy, NATS or network is started; this does not prove readiness.
// origin: H0 Caddy cross-review H0-CADDY-X1.
func TestLoadAgentYAMLFromSimProvisioning(t *testing.T) {
	helper, err := filepath.Abs(filepath.Join("..", "..", "test", "simcluster", "drills", "lib", "agentyaml.sh"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(helper)
	if err != nil {
		t.Fatal(err)
	}
	generate := func(t *testing.T, source, url, address string) string {
		t.Helper()
		home := t.TempDir()
		dir := filepath.Join(home, "agent", "lab")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		capture := filepath.Join(dir, "agent.yaml")
		const script = `set -u
warn() { printf '%s\n' "$*" >&2; }
dexec() { return 0; }
d() { [ "$1" = cp ] || return 3; cp "$2" "$CAPTURE"; }
ctr_name() { printf 'fixture-%s' "$1"; }
sctl() { return 0; }
poll_until() { return 0; }
. "$HELPER"
agent_provision_yaml agt1 lab "$1" open 45 "$2"
`
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", script, "fixture", url, address)
		cmd.Env = append(os.Environ(), "HELPER="+source, "CAPTURE="+capture)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("generate agent.yaml: %v\n%s", err, out)
		}
		if data, err := os.ReadFile(capture); err != nil || len(data) == 0 {
			t.Fatalf("generation produced no document: %v", err)
		}
		return home
	}
	for _, tc := range []struct{ url, address string }{
		{"nats://brk1:4222", ""}, // legacy derivation remains usable
		{"wss://brk1:443/nats", "brk1:7000"},
		{"wss://192.0.2.1:443/nats", "192.0.2.1:7000"},
		{"wss://[::1]:443/nats", "[::1]:7000"},
		{"wss://[2001:db8::1]:443/nats", "[2001:db8::1]:7000"},
		{"wss://brk1:443/nats", "[fe80::1%eth0]:7000"},
	} {
		t.Run(tc.url+"/"+tc.address, func(t *testing.T) {
			home := generate(t, helper, tc.url, tc.address)
			got, err := loadAgentYAML(home, "lab")
			if err != nil {
				t.Fatalf("actual generated document rejected by production yaml.v3 loader: %v", err)
			}
			wantAddress := tc.address
			if wantAddress == "" {
				wantAddress = "brk1:7000"
			}
			want := agentYAML{BrokerURL: tc.url, Session: "lab", NID: "agt1", TunnelAddr: wantAddress}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded agent.yaml = %+v, want %+v", got, want)
			}
		})
	}
	t.Run("bare IPv6 negative control", func(t *testing.T) {
		const quoted = `print(key + ": " + json.dumps(value))`
		if strings.Count(string(body), quoted) != 1 {
			t.Fatal("negative control must remove exactly one scalar-quoting operation")
		}
		mutant := filepath.Join(t.TempDir(), "agentyaml.sh")
		if err := os.WriteFile(mutant, []byte(strings.Replace(string(body), quoted, `print(key + ": " + value)`, 1)), 0o600); err != nil {
			t.Fatal(err)
		}
		home := generate(t, mutant, "wss://[::1]:443/nats", "[::1]:7000")
		generated, err := os.ReadFile(filepath.Join(home, "agent", "lab", "agent.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(generated), "tunnel_addr: [::1]:7000\n") {
			t.Fatal("negative control did not emit the original malformed IPv6 scalar")
		}
		if _, err := loadAgentYAML(home, "lab"); err == nil {
			t.Fatal("production yaml.v3 loader accepted the unquoted IPv6 mutant")
		}
	})
}
