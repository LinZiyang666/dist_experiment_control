package natsconf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleClusterConfig() Config {
	self := Broker{ServerName: "tether-a", NkeyPub: "UBROKERA", RouteURL: "nats://10.0.0.1:6222"}
	// a REAL peer so the clustered render carries a route (a lone-self clustered conf is unbootable
	// and now correctly refused by Render — audit D).
	peer := Broker{ServerName: "tether-b", NkeyPub: "UBROKERB", RouteURL: "nats://10.0.0.2:6222"}
	return Config{
		Local:         self,
		Peers:         []Broker{self, peer},
		AccountIssuer: "UACCOUNT",
		JSStoreDir:    "/var/lib/tether/jetstream",
		ClientListen:  "127.0.0.1:4222",
		ClusterListen: "0.0.0.0:6222",
		CAFile:        "/etc/tether/secrets/cluster-ca.pem",
		CertFile:      "/etc/tether/secrets/route-cert.pem",
		KeyFile:       "/etc/tether/secrets/route-key.pem",
	}
}

func TestBuildMergedConfPreservesAndRoundTrips(t *testing.T) {
	own, err := Preflight(writeConf(t, installSHConf+"\nmax_payload: 16777216\n"))
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	merged, err := BuildMergedConf(own, sampleClusterConfig())
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	// Cluster directives present.
	for _, want := range []string{"server_name:", "cluster {", "authorization {", "auth_callout"} {
		if !strings.Contains(merged, want) {
			t.Errorf("merged conf missing cluster directive %q", want)
		}
	}
	// install.sh websocket{8222 no_tls} preserved.
	if !strings.Contains(merged, "websocket {") || !strings.Contains(merged, "port: 8222") || !strings.Contains(merged, "no_tls: true") {
		t.Errorf("merged conf dropped the install.sh websocket block:\n%s", merged)
	}
	// tuning passthrough preserved.
	if !strings.Contains(merged, "max_payload: 16777216") {
		t.Errorf("merged conf dropped max_payload (documented tuning)")
	}
	// Round-trip: the merged conf itself must pass preflight (no unknown directives
	// introduced — every directive is in a recognized bucket).
	if _, err := Preflight(writeConf(t, merged)); err != nil {
		t.Fatalf("merged conf must round-trip through preflight, got: %v", err)
	}
	// And re-parse: websocket port survives as 8222 no_tls (not a raw-text splice artifact).
	own2, err := Preflight(writeConf(t, merged))
	if err != nil {
		t.Fatal(err)
	}
	if ws, ok := own2.Parsed["websocket"].(map[string]any); !ok || intVal(ws["port"]) != 8222 {
		t.Fatalf("re-parsed websocket port != 8222: %v", own2.Parsed["websocket"])
	}
}

func TestApplyAtomicSwapAndOneTimeBackup(t *testing.T) {
	conf := writeConf(t, installSHConf)
	if err := Apply(conf, "server_name: \"tether-a\"\n"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got, _ := os.ReadFile(conf)
	if !strings.Contains(string(got), "tether-a") {
		t.Fatal("conf not replaced by Apply")
	}
	baks, _ := filepath.Glob(conf + ".bak.*")
	if len(baks) != 1 {
		t.Fatalf("want exactly one pristine .bak, got %d", len(baks))
	}
	// The .bak is the PRISTINE pre-takeover conf.
	bakBody, _ := os.ReadFile(baks[0])
	if !strings.Contains(string(bakBody), "store_dir") || strings.Contains(string(bakBody), "tether-a") {
		t.Fatal(".bak is not the pristine pre-takeover conf")
	}
	// A second Apply must NOT overwrite the pristine .bak (skip-if-exists).
	if err := Apply(conf, "server_name: \"tether-a2\"\n"); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if baks2, _ := filepath.Glob(conf + ".bak.*"); len(baks2) != 1 {
		t.Fatalf("second apply must not add a .bak, got %d", len(baks2))
	}
}

func TestApplyBackupPreservesPrivatePermissions(t *testing.T) {
	conf := writeConf(t, "authorization { users = [{user: \"u\", password: \"secret\"}] }\n")
	if err := os.Chmod(conf, 0o600); err != nil {
		t.Fatalf("chmod source: %v", err)
	}
	if err := Apply(conf, "server_name: \"tether-a\"\n"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	baks, _ := filepath.Glob(conf + ".bak.*")
	if len(baks) != 1 {
		t.Fatalf("want exactly one backup, got %d", len(baks))
	}
	for _, path := range []string{conf, baks[0]} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got&0o077 != 0 {
			t.Fatalf("%s mode = %o, group/world bits must be clear", path, got)
		}
	}
}
