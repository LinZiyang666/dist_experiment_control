// Package cluster_test holds D1's black-box tests driven through the public
// cluster.Node API (architecture §3, plan docs/reviews/d1-plan.md §5). The
// in-package (kill-9 commit-gate, reapply-count) tests live in internal/cluster.
package cluster_test

import (
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// quietLogger discards FSM logging in tests.
func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// newTestNode constructs a single-node cluster.Node on an inmem transport with a
// fresh temp data dir, waits for N=1 leadership, and registers cleanup.
func newTestNode(t *testing.T) (*cluster.Node, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	_, trans := raft.NewInmemTransport("")
	n, err := cluster.New(cluster.Config{
		LocalID:      "n1",
		DataDir:      dir,
		DBPath:       dbPath,
		Transport:    trans,
		Logger:       quietLogger(),
		ApplyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}
	return n, dbPath
}

// readMeta reads a cluster_meta value through a node's bounded-stale read.
func readMeta(t *testing.T, n *cluster.Node, key string) (string, bool) {
	t.Helper()
	var (
		val   string
		found bool
	)
	err := n.BoundedStaleRead(func(db *sql.DB) error {
		err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, key).Scan(&val)
		switch err {
		case nil:
			found = true
			return nil
		case sql.ErrNoRows:
			return nil
		default:
			return err
		}
	})
	if err != nil {
		t.Fatalf("readMeta %q: %v", key, err)
	}
	return val, found
}
