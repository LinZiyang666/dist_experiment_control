package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/serveconf"
)

func TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	body := []byte("broker:\n  domain: example.com\nstorage:\n  scratch: /tmp\n")
	if err := os.WriteFile(cfg, body, 0o644); err != nil {
		t.Fatal(err)
	}

	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets")
	if err != nil || !applied {
		t.Fatalf("expected seam append attempt: applied=%v err=%v", applied, err)
	}
	parsed, err := serveconf.Load(cfg)
	if err != nil {
		t.Fatalf("config should still parse: %v", err)
	}
	if parsed.Broker.Cluster.RaftAddr == "" {
		t.Fatalf("external review: seam was not attached to broker.cluster when broker: was not the final top-level key")
	}
}
