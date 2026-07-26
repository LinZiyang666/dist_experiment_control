package cluster

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestExternalReviewRaftLoggerKeepsDistinctTypedPeers models the actual
// Hashicorp Raft calls in replication.go. Peer identities are passed as
// raft.ServerAddress / raft.ServerID, not as the built-in string type.
func TestExternalReviewRaftLoggerKeepsDistinctTypedPeers(t *testing.T) {
	var buf bytes.Buffer
	br := NewRaftLogger(slog.New(slog.NewTextHandler(&buf, nil)), false)

	br.Error("failed to heartbeat to", "peer", raft.ServerAddress("10.0.0.2:7400"))
	br.Error("failed to heartbeat to", "peer", raft.ServerAddress("10.0.0.3:7400"))

	out := buf.String()
	for _, peer := range []string{"10.0.0.2:7400", "10.0.0.3:7400"} {
		if !strings.Contains(out, peer) {
			t.Errorf("distinct typed peer %q was suppressed by de-duplication; got:\n%s", peer, out)
		}
	}
}

// TestExternalReviewRaftLoggerStillExcludesNumericStringers guards the other
// half of the de-duplication contract: changing term/index/duration values must
// not turn de-duplication off. time.Duration is numeric but implements
// fmt.Stringer, so a broad Stringer fallback accidentally puts it in the key.
func TestExternalReviewRaftLoggerStillExcludesNumericStringers(t *testing.T) {
	if got, want := dedupKey("heartbeat timeout", []any{time.Second}), "heartbeat timeout"; got != want {
		t.Fatalf("numeric duration entered de-dup key: got %q, want %q", got, want)
	}
}
