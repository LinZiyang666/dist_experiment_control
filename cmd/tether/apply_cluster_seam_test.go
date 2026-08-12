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
	// #75 update: the original fixture used a second TOP-LEVEL `storage:`
	// block (the EOF-append failure mode this test pinned — the seam would
	// nest under it). The strict decoder now refuses any second top-level
	// key up front (TestApplyClusterSeam_UnparseableConfigRefuses), so that
	// world is unreachable; what remains worth pinning is placement
	// independence WITHIN the legal shape — `broker:` carrying keys after
	// the insertion point plus trailing comments, where an EOF append would
	// still land in the wrong position.
	body := []byte("broker:\n  domain: example.com\n  storage:\n    db: /tmp/t.db\n# trailing operator comment\n")
	if err := os.WriteFile(cfg, body, 0o644); err != nil {
		t.Fatal(err)
	}

	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets", defaultNatsConfPath)
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
