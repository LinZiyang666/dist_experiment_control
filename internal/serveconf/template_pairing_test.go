// template_pairing_test.go — #75's second half: pin install.sh's broker.yaml
// heredoc TEMPLATE to this package's strict schema. The strict decoder (Load)
// is only safe because every key the official template writes is either
// consumed by Go or covered by an inert stub (broker.tls.*). That pairing had
// no gate before: install.sh and this schema could drift independently, and
// with KnownFields(true) a drifted template would refuse to boot every fresh
// official install. This test extracts the REAL heredoc (not a hand-copied
// fixture, which would inevitably be written to match the schema and go green
// while the real template burns) and strict-parses it.
// origin: docs/deploy-tier-gotchas.md #75
package serveconf

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// extractBrokerYAMLTemplate pulls the broker.yaml heredoc body out of
// scripts/install.sh and substitutes the shell variables it interpolates with
// dummy values, yielding the yaml a real install would write.
func extractBrokerYAMLTemplate(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "scripts", "install.sh")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read install.sh: %v", err)
	}
	lines := strings.Split(string(body), "\n")
	start := -1
	for i, ln := range lines {
		if strings.Contains(ln, `cat > "$ETC_DIR/broker.yaml" <<EOF`) {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatal("install.sh no longer contains the broker.yaml heredoc marker; update this extractor with it")
	}
	end := -1
	for i := start; i < len(lines); i++ {
		if strings.TrimRight(lines[i], " \t\r") == "EOF" {
			end = i
			break
		}
	}
	if end < 0 {
		t.Fatal("broker.yaml heredoc has no terminating EOF line")
	}
	tmpl := strings.Join(lines[start:end], "\n")
	// The heredoc is UNQUOTED on purpose (install.sh expands $DOMAIN etc.).
	// Substitute every $VAR with a dummy path-ish token, exactly like a real
	// run would produce concrete values.
	return regexp.MustCompile(`\$[A-Z_]+`).ReplaceAllString(tmpl, "/dummy")
}

// uncommentClusterBlock rewrites the template's `# cluster:` block (and its
// `#   key: value` children) into live yaml — exactly the documented HA
// enablement action ("Uncomment when joining/forming a cluster"). Prose
// comments outside that block are left untouched.
func uncommentClusterBlock(t *testing.T, tmpl string) string {
	t.Helper()
	lines := strings.Split(tmpl, "\n")
	inBlock := false
	found := false
	for i, ln := range lines {
		switch {
		// Only the BARE `  # cluster:` line (nothing after the colon) is the
		// block header; the template also has a PROSE line beginning
		// `  # cluster: HA mode (…)` which must stay a comment.
		case strings.TrimRight(ln, " \t") == "  # cluster:":
			lines[i] = strings.Replace(ln, "# ", "", 1)
			inBlock, found = true, true
		case inBlock && strings.HasPrefix(ln, "  #   "):
			lines[i] = strings.Replace(ln, "#   ", "  ", 1)
		case inBlock:
			inBlock = false
		}
	}
	if !found {
		t.Fatal("template no longer carries the commented `# cluster:` block; update this transformer with the new HA-enable shape")
	}
	return strings.Join(lines, "\n")
}

// TestInstallShTemplateClusterBlockParsesStrict (review Mi12): the COMMENTED
// cluster block is the documented HA enablement path — an operator uncomments
// it in place. Under the strict decoder, a key added to that block without a
// schema field would brick exactly the operator who follows the docs: the
// mirror image of the fleet-brick TestInstallShTemplateParsesStrict guards.
func TestInstallShTemplateClusterBlockParsesStrict(t *testing.T) {
	yaml := uncommentClusterBlock(t, extractBrokerYAMLTemplate(t))
	dir := t.TempDir()
	p := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("the template's uncommented cluster block no longer parses under the strict decoder — "+
			"the documented HA-enable action would refuse to boot: %v", err)
	}
	if cfg.Broker.Cluster.DataDir == "" || cfg.Broker.Cluster.SecretsDir == "" {
		t.Fatalf("uncommented cluster block did not land in the schema: %+v", cfg.Broker.Cluster)
	}
}

func TestInstallShTemplateParsesStrict(t *testing.T) {
	yaml := extractBrokerYAMLTemplate(t)
	dir := t.TempDir()
	p := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("the official install.sh broker.yaml template no longer parses under the strict decoder — "+
			"a fresh official install would refuse to boot. Either the template grew a key the schema lacks "+
			"(add a field or an inert stub) or the schema dropped one: %v", err)
	}
	// The deployed default log sink must stay wired: h1 F's capped file sink
	// is only the DEPLOYED default because the template carries it.
	if cfg.Broker.Obs.LogFile == "" {
		t.Fatal("template lost observability.log_file — the deployed broker would fall back to journal with no in-process cap")
	}
	// The inert TLS stub must actually absorb the template's ops-only block.
	if cfg.Broker.TLS.ACME.Email == "" {
		t.Fatal("template's broker.tls.acme.email did not land in the inert stub — the stub and template drifted")
	}
}
