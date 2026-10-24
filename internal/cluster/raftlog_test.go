package cluster

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// Batch-A review M1 + M7. A15 added a raft→slog bridge and node.go claimed the
// logging was "finally wired". It was not: ProductionConfig had no Logger field,
// so the only path that runs in production built a Node with a nil Logger and
// the bridge returned on its first line. The feature was a black hole behind a
// comment saying otherwise — the same shape as the RehomeDirective false promise
// this batch set out to remove.
//
// These tests pin the wiring and the de-duplication semantics, neither of which
// had any coverage.

func TestProductionConfigCarriesLogger(t *testing.T) {
	// Structural: the field must exist and reach cluster.Config. A compile error
	// here is the point — it means someone removed the plumbing again.
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pc := ProductionConfig{LocalID: "n1", Logger: lg}
	if pc.Logger == nil {
		t.Fatal("ProductionConfig.Logger is nil after being set")
	}
	// And the bridge built from it must actually emit.
	br := NewRaftLogger(pc.Logger, false)
	br.Warn("wiring probe")
	if !strings.Contains(buf.String(), "wiring probe") {
		t.Errorf("a bridge built from ProductionConfig.Logger emitted nothing; got %q.\n"+
			"This is exactly the M1 defect: the plumbing compiles, the comment says 'wired', "+
			"and raft says nothing for the rest of the cluster's life.", buf.String())
	}
}
