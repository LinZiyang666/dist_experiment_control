package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/serveconf"
)

// cluster_seam_test.go (G4 #5) — `cluster init` must APPLY the broker.yaml cluster seam (it previously only
// PRINTED it, so an operator/automation had to hand-append it). The seam must decode back through serveconf.

func TestApplyClusterSeam_AppendsAndDecodes(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  domain: example.com\n  public_host: brk-a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", defaultNatsConfPath)
	if err != nil || !applied {
		t.Fatalf("seam must be applied to a config without one: applied=%v err=%v", applied, err)
	}
	// The seam must parse back through the real broker.yaml decoder into the cluster section.
	c, err := serveconf.Load(cfg)
	if err != nil {
		t.Fatalf("seam-applied broker.yaml must still decode: %v", err)
	}
	if c.Broker.Cluster.RaftAddr != "brk-a:7400" || c.Broker.Cluster.DataDir != "/var/lib/tether" || c.Broker.Cluster.SecretsDir != "/etc/tether/secrets" {
		t.Fatalf("cluster seam did not decode into broker.cluster: %+v", c.Broker.Cluster)
	}
	if c.Broker.Domain != "example.com" {
		t.Fatalf("the pre-existing broker fields must be preserved: %+v", c.Broker)
	}
}

func TestApplyClusterSeam_Idempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	// R3-B1: idempotency requires a COMPLETE matching seam (a raft_addr-only seam is a PARTIAL seam that boots
	// single mode — it must hard-error, not skip; that is TestApplyClusterSeam_RejectsMismatchedExistingSeam).
	if err := os.WriteFile(cfg, []byte("broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk-a:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", defaultNatsConfPath)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("a config that already carries a complete matching cluster seam must NOT be double-appended")
	}
	raw, _ := os.ReadFile(cfg)
	if strings.Count(string(raw), "raft_addr:") != 1 {
		t.Fatalf("idempotent apply must leave exactly one raft_addr seam:\n%s", raw)
	}
}

// TestApplyClusterSeamThreadsNatsConfPath (external review N-4c) proves --nats-conf actually reaches the
// written seam (it was hardcoded to defaultNatsConfPath before), and that the no-thrash upgrade story
// holds: a pre-fix default-path seam stays idempotent under the default param, and a genuine mismatch
// hard-errors naming nats_conf_path rather than silently overwriting.
func TestApplyClusterSeamThreadsNatsConfPath(t *testing.T) {
	base := "broker:\n  domain: example.com\n"

	// (a) a custom --nats-conf lands in the seam verbatim.
	t.Run("custom path lands in seam", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "broker.yaml")
		if err := os.WriteFile(cfg, []byte(base), 0o644); err != nil {
			t.Fatal(err)
		}
		applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", "/custom/nats.conf")
		if err != nil || !applied {
			t.Fatalf("custom-path seam must apply: applied=%v err=%v", applied, err)
		}
		c, err := serveconf.Load(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if c.Broker.Cluster.NatsConfPath != "/custom/nats.conf" {
			t.Fatalf("seam must record the custom nats_conf_path, got %q", c.Broker.Cluster.NatsConfPath)
		}
		// (b) re-running with the same custom path is idempotent (no thrash).
		if applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", "/custom/nats.conf"); err != nil || applied {
			t.Fatalf("re-run with the same custom path must be idempotent: applied=%v err=%v", applied, err)
		}
	})

	// (c) a pre-fix seam (default path) + default param is idempotent — the upgrade path cannot thrash.
	t.Run("pre-fix default seam stays idempotent", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "broker.yaml")
		seam := "broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk-a:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: " + defaultNatsConfPath + "\n"
		if err := os.WriteFile(cfg, []byte(seam), 0o644); err != nil {
			t.Fatal(err)
		}
		if applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", defaultNatsConfPath); err != nil || applied {
			t.Fatalf("a pre-fix default-path seam under the default param must be idempotent: applied=%v err=%v", applied, err)
		}
	})

	// (d) a pre-fix default seam + a custom --nats-conf is a genuine mismatch → HARD error (never silent).
	t.Run("mismatch hard-errors naming nats_conf_path", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "broker.yaml")
		seam := "broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk-a:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: " + defaultNatsConfPath + "\n"
		if err := os.WriteFile(cfg, []byte(seam), 0o644); err != nil {
			t.Fatal(err)
		}
		applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", "/custom/nats.conf")
		if err == nil || applied {
			t.Fatalf("a stale nats_conf_path must hard-error (never a silent overwrite): applied=%v err=%v", applied, err)
		}
		if !strings.Contains(err.Error(), "nats_conf_path") {
			t.Fatalf("the mismatch error must name nats_conf_path (have/want), got: %v", err)
		}
	})

	// (e) an empty natsConfPath is a caller bug → fail loud (no silent default substitution).
	t.Run("empty path is rejected", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "broker.yaml")
		if err := os.WriteFile(cfg, []byte(base), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", ""); err == nil {
			t.Fatal("an empty natsConfPath must be rejected, not silently defaulted")
		}
	})
}

func TestApplyClusterSeam_MissingFileIsNoError(t *testing.T) {
	applied, err := applyClusterSeam(filepath.Join(t.TempDir(), "nope.yaml"), "/d", "h:7400", "/s", defaultNatsConfPath)
	if err != nil {
		t.Fatalf("a missing config is not an error (the operator may run without one): %v", err)
	}
	if applied {
		t.Fatal("a missing config cannot have had a seam applied")
	}
}
