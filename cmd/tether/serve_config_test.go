package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clustermanifest"
	"github.com/spf13/cobra"
)

// TestPickPublicHostPrecedence pins the architecture A.3 public_host
// fallback chain: explicit --public-host > yaml broker.public_host >
// yaml broker.domain > cobra default.
func TestPickPublicHostPrecedence(t *testing.T) {
	const cobraDefault = "localhost"

	cases := []struct {
		name           string
		flagChanged    bool
		flagVal        string
		yamlPublicHost string
		yamlDomain     string
		want           string
	}{
		{"explicit flag wins", true, "flag.example.com", "yaml-public.example.com", "domain.example.com", "flag.example.com"},
		{"yaml public_host wins over domain", false, cobraDefault, "yaml-public.example.com", "domain.example.com", "yaml-public.example.com"},
		{"yaml domain fallback when public_host empty", false, cobraDefault, "", "domain.example.com", "domain.example.com"},
		{"cobra default when neither set", false, cobraDefault, "", "", cobraDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newServeCmd()
			if tc.flagChanged {
				if err := cmd.Flags().Set("public-host", tc.flagVal); err != nil {
					t.Fatal(err)
				}
			}
			got := pickPublicHost(cmd, tc.flagVal, tc.yamlPublicHost, tc.yamlDomain)
			if got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestEffectiveAuthSeedsDirDefaultsToClusterSecretsInClusterMode(t *testing.T) {
	if got := effectiveAuthSeedsDir("/explicit/seeds", true, "/cluster/secrets"); got != "/explicit/seeds" {
		t.Fatalf("explicit seeds dir = %q, want explicit", got)
	}
	if got := effectiveAuthSeedsDir("", true, "/cluster/secrets"); got != "/cluster/secrets" {
		t.Fatalf("cluster default seeds dir = %q, want cluster secrets", got)
	}
	if got := effectiveAuthSeedsDir("", false, "/cluster/secrets"); got != "" {
		t.Fatalf("single mode default seeds dir = %q, want empty", got)
	}
}

func TestLoadAuthCalloutSeedsRejectsTooOpenOrSymlinkSeeds(t *testing.T) {
	dir := t.TempDir()
	writeSeeds := func(mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "broker.nk"), []byte("broker"), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "account.nk"), []byte("account"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeSeeds(0o644)
	if _, err := loadAuthCalloutSeeds(dir); err == nil || !strings.Contains(err.Error(), "permissions") {
		t.Fatalf("too-open broker.nk must be rejected, got %v", err)
	}

	if err := os.Remove(filepath.Join(dir, "broker.nk")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "account.nk"), filepath.Join(dir, "broker.nk")); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAuthCalloutSeeds(dir); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink broker.nk must be rejected, got %v", err)
	}
}

// TestParsePortBand pins the frp.port_range parser. Empty → (0,0)
// (broker falls back to its 14000-14999 default); valid range
// passes through; bad inputs are rejected.
func TestParsePortBand(t *testing.T) {
	cases := []struct {
		in       string
		wantLow  int
		wantHigh int
		wantErr  bool
	}{
		{"", 0, 0, false},
		{"14000-14999", 14000, 14999, false},
		{" 14000 - 14999 ", 14000, 14999, false},
		{"14000", 0, 0, true},
		{"14000-", 0, 0, true},
		{"-14999", 0, 0, true},
		{"abc-14999", 0, 0, true},
		{"14999-14000", 0, 0, true},
		{"0-14999", 0, 0, true},
		{"14000-99999", 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			low, high, err := parsePortBand(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q; got %d-%d", tc.in, low, high)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if low != tc.wantLow || high != tc.wantHigh {
				t.Errorf("got %d-%d want %d-%d", low, high, tc.wantLow, tc.wantHigh)
			}
		})
	}
}

// TestPickFlagOrYamlPrecedence pins the standard "flag > yaml >
// default" precedence used by the rest of the serve flags. Cobra
// supplies the default into flagVal; the helper just chooses
// between flagVal (when --foo was explicitly passed) and yamlVal.
func TestPickFlagOrYamlPrecedence(t *testing.T) {
	cmd := &cobra.Command{Use: "x"}
	cmd.Flags().String("opt", "cobra-default", "")

	// Not changed → yaml wins when non-empty.
	if got := pickFlagOrYaml(cmd, "opt", "cobra-default", "yaml-val"); got != "yaml-val" {
		t.Errorf("yaml fallback: got %q want yaml-val", got)
	}
	// Not changed + yaml empty → cobra default.
	if got := pickFlagOrYaml(cmd, "opt", "cobra-default", ""); got != "cobra-default" {
		t.Errorf("default fallback: got %q want cobra-default", got)
	}
	// Changed → flag wins regardless of yaml.
	if err := cmd.Flags().Set("opt", "flag-val"); err != nil {
		t.Fatal(err)
	}
	if got := pickFlagOrYaml(cmd, "opt", "flag-val", "yaml-val"); got != "flag-val" {
		t.Errorf("explicit flag: got %q want flag-val", got)
	}
}

// TestDiskCheckIntervalFlagDefault pins the #39 knob's serve surface: the flag exists, is a
// duration, and DEFAULTS to 0 — which serve.go/startDiskMonitor read as "use the built-in 5m
// default". A regression that changed the default here (e.g. to 5m) would break the flag>yaml>default
// precedence (the yaml would never be consulted because Changed()==false but the value wouldn't be 0).
func TestDiskCheckIntervalFlagDefault(t *testing.T) {
	f := newServeCmd().Flags().Lookup("disk-check-interval")
	if f == nil {
		t.Fatal("serve: --disk-check-interval flag missing")
	}
	if f.Value.Type() != "duration" {
		t.Errorf("--disk-check-interval type = %q, want duration", f.Value.Type())
	}
	if f.DefValue != "0s" {
		t.Errorf("--disk-check-interval default = %q, want 0s (0 ⇒ built-in 5m default)", f.DefValue)
	}
}

// TestResolveManifestListen pins the #27 cluster-mode default for the C2 well-known
// discovery manifest listener: an operator who leaves --cluster-manifest-listen unset
// in cluster mode (exactly what the `cluster init` broker.yaml seam leaves — it never
// writes a manifest_listen key) gets the default loopback bind so discovery is
// serve-ready. Single mode, an explicit addr, and an explicit empty opt-out are
// all left untouched.
func TestResolveManifestListen(t *testing.T) {
	cases := []struct {
		name        string
		cur         string
		clusterMode bool
		flagChanged bool
		want        string
	}{
		{"cluster + unset -> default (the #27 fix)", "", true, false, defaultManifestListen},
		{"single mode + unset -> stays empty (disabled)", "", false, false, ""},
		{"cluster + explicit addr wins", "127.0.0.1:9999", true, true, "127.0.0.1:9999"},
		{"cluster + explicit empty flag -> opt out stays empty", "", true, true, ""},
		{"cluster + yaml addr (not changed) preserved", "127.0.0.1:7000", true, false, "127.0.0.1:7000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveManifestListen(tc.cur, tc.clusterMode, tc.flagChanged); got != tc.want {
				t.Errorf("resolveManifestListen(%q,%v,%v) = %q, want %q", tc.cur, tc.clusterMode, tc.flagChanged, got, tc.want)
			}
		})
	}
}

// TestDefaultManifestListenBindsAndIsReachable proves the #27 default addr is a valid
// loopback that actually BINDS and ACCEPTS connections (curl-reachable, not connection
// refused) — the whole point of defaulting it. It serves a 503 (no manifest yet), which
// is a served HTTP response, i.e. the listener is bound (a refused connection would be a
// transport error instead).
func TestDefaultManifestListenBindsAndIsReachable(t *testing.T) {
	ln, err := clustermanifest.Bind(defaultManifestListen)
	if err != nil {
		t.Skipf("default manifest addr %s not bindable in this environment (port busy?): %v", defaultManifestListen, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- clustermanifest.ServeListener(ctx, ln, func() ([]byte, bool) { return nil, false }) }()

	url := "http://" + defaultManifestListen + clustermanifest.ManifestPath
	var resp *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("default manifest listener not reachable at %s: %v", url, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("bound manifest listener: got status %d, want 503 (no manifest yet)", resp.StatusCode)
	}
	cancel()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("manifest server did not exit on ctx cancel")
	}
}
