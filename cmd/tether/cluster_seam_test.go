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
	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets")
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
	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets")
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

func TestApplyClusterSeam_MissingFileIsNoError(t *testing.T) {
	applied, err := applyClusterSeam(filepath.Join(t.TempDir(), "nope.yaml"), "/d", "h:7400", "/s")
	if err != nil {
		t.Fatalf("a missing config is not an error (the operator may run without one): %v", err)
	}
	if applied {
		t.Fatal("a missing config cannot have had a seam applied")
	}
}
