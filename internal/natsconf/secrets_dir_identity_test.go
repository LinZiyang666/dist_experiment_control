package natsconf

import (
	"path/filepath"
	"testing"
)

// secrets_dir_identity_test.go — Config.ApplySecretsDirIdentity and synthesizeClusterListen.
//
// origin: adversarial review of B5 (lane B5), reviewer-authored.
//
// WHY: B5's stated result is that "the filenames are here and nowhere else" and that the two
// first-grow renders now share ONE derivation instead of eight hand-copied lines. Nothing tested
// that derivation. Verified by mutation on a scratch copy of the tree: rewriting
// "cluster-ca.pem" -> "MUTANT-ca.pem" and "route-cert.pem" -> "MUTANT-cert.pem" inside
// ApplySecretsDirIdentity left internal/natsconf, internal/broker and cmd/tether all green,
// including the four new goldens — because the grow-cutover golden hand-writes the four values the
// helper produces (golden_merged_test.go sets CAFile/CertFile/KeyFile/ClusterListen literally)
// rather than calling the helper. A shared derivation nothing calls in a test is a shared
// derivation nothing pins.
//
// These filenames are a DEPLOY-SURFACE contract: `cluster init` and the provisioning runbook write
// exactly these names into the secrets dir, and internal/broker/cutover.go's secretClusterCA /
// secretRouteCert / secretRouteKey constants read the same three. Changing one spelling without the
// others produces a broker that renders a conf pointing at cert files that do not exist — which
// `nats-server -t` catches only if the paths are checked at validation time.
func TestApplySecretsDirIdentitySetsTheDocumentedSecretsFilenames(t *testing.T) {
	const secrets = "/etc/tether/secrets"
	cfg := Config{}
	cfg.ApplySecretsDirIdentity(secrets, "nats://10.0.0.7:6222")

	for _, tc := range []struct {
		field, got, want string
	}{
		{"CAFile", cfg.CAFile, filepath.Join(secrets, "cluster-ca.pem")},
		{"CertFile", cfg.CertFile, filepath.Join(secrets, "route-cert.pem")},
		{"KeyFile", cfg.KeyFile, filepath.Join(secrets, "route-key.pem")},
		{"ClusterListen", cfg.ClusterListen, "0.0.0.0:6222"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q — these names are written by `cluster init` and read back by "+
				"internal/broker/cutover.go's secret* constants; the three spellings must agree or the "+
				"rendered conf points at cert files that do not exist", tc.field, tc.got, tc.want)
		}
	}
}

// TestApplySecretsDirIdentityLeavesClusterNameAlone pins the one thing the helper's doc comment
// says it deliberately does NOT do. Setting ClusterName here would be silently wrong rather than
// loudly wrong: Render defaults an empty name to "tether", which is also BuildMergedConf's harvest
// default, so a fallback-rendered node keeps matching an already-clustered peer's harvested name.
// A helper that started forcing a name would split the mesh in two named clusters with no error.
func TestApplySecretsDirIdentityLeavesClusterNameAlone(t *testing.T) {
	cfg := Config{ClusterName: "operator-chosen"}
	cfg.ApplySecretsDirIdentity("/etc/tether/secrets", "nats://10.0.0.7:6222")
	if cfg.ClusterName != "operator-chosen" {
		t.Errorf("ApplySecretsDirIdentity overwrote ClusterName (%q) — it must leave it alone so an "+
			"empty one falls through to Render's \"tether\" default, matching the harvest default",
			cfg.ClusterName)
	}
	empty := Config{}
	empty.ApplySecretsDirIdentity("/etc/tether/secrets", "nats://10.0.0.7:6222")
	if empty.ClusterName != "" {
		t.Errorf("ApplySecretsDirIdentity invented a ClusterName (%q); it must stay empty so Render's "+
			"default and BuildMergedConf's harvest default remain the single source", empty.ClusterName)
	}
}

// TestSynthesizeClusterListenDerivesThePortFromTheRouteURL covers the derivation that used to be
// EXPORTED purely so internal/broker could reproduce it byte-for-byte. It is now unexported and
// called from both paths, which only helps if the derivation itself is right: the grow cutover
// applies whatever this returns and then hard-restarts nats-server on it.
func TestSynthesizeClusterListenDerivesThePortFromTheRouteURL(t *testing.T) {
	cases := []struct {
		name, routeURL, want string
	}{
		{"normal route url", "nats://10.0.0.7:6222", "0.0.0.0:6222"},
		{"non-default port", "nats://10.0.0.7:7422", "0.0.0.0:7422"},
		{"surrounding whitespace", "  nats://10.0.0.7:6222\n", "0.0.0.0:6222"},
		{"no scheme", "10.0.0.7:6222", "0.0.0.0:6222"},
		{"ipv6 literal", "nats://[fd00::1]:6222", "0.0.0.0:6222"},
		// Fail-SOFT cases: the doc says it falls back to the nats default route port. That is a
		// deliberate choice and it is worth pinning, because the alternative (returning "") would make
		// Render emit a cluster{} block with no listen and let nats-server pick its own.
		{"no port", "nats://10.0.0.7", "0.0.0.0:6222"},
		{"empty", "", "0.0.0.0:6222"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := synthesizeClusterListen(tc.routeURL); got != tc.want {
				t.Errorf("synthesizeClusterListen(%q) = %q, want %q", tc.routeURL, got, tc.want)
			}
		})
	}
}

// TestSecretsDirFallbackRendersThroughTheSharedHelper closes the specific hole the mutation found:
// it drives BuildMergedConf over a live STANDALONE conf using the helper rather than hand-written
// paths, so a change to the filenames or the listen derivation shows up in an assertion about the
// bytes a broker would actually load.
func TestSecretsDirFallbackRendersThroughTheSharedHelper(t *testing.T) {
	own, err := Preflight(writeConf(t, reconcileStandaloneConf))
	if err != nil {
		t.Fatalf("fixture must preflight clean: %v", err)
	}
	cfg := Config{
		Local: Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		Peers: []Broker{
			{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
			{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
		},
		AccountIssuer: "ABROKERACCOUNTPUB",
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
	}
	cfg.ApplySecretsDirIdentity("/etc/tether/secrets", "nats://10.0.0.1:6222")

	merged, err := BuildMergedConf(own, cfg)
	if err != nil {
		t.Fatalf("the first-grow secrets-dir fallback must render (this is the path `cluster add` "+
			"applies then hard-restarts on): %v", err)
	}
	for _, want := range []string{
		`ca_file: "/etc/tether/secrets/cluster-ca.pem"`,
		`cert_file: "/etc/tether/secrets/route-cert.pem"`,
		`key_file: "/etc/tether/secrets/route-key.pem"`,
		`listen: "0.0.0.0:6222"`,
	} {
		if !containsLine(merged, want) {
			t.Errorf("rendered conf is missing %s\n%s", want, merged)
		}
	}
}

func containsLine(s, want string) bool {
	for _, l := range splitLinesTrimmed(s) {
		if l == want {
			return true
		}
	}
	return false
}

func splitLinesTrimmed(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\n' {
			line := s[start:i]
			for len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
				line = line[1:]
			}
			for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
				line = line[:len(line)-1]
			}
			out = append(out, line)
			start = i + 1
		}
	}
	return out
}
