//go:build linux

package brokerimage

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/hashicorp/raft"
)

// A cut refused before any store close (here: the tunnel leaf reports a busy
// legacy session after Store.Quiesce) must roll back the WHOLE original graph,
// including the store's admission fence. Otherwise the broker reports
// control_ready while every Node write is refused forever.
// origin: hot-update S6 internal review rv-writer
func TestClusterStoreRefusedFreezeRestoresWriteAdmission(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseClusterStoreRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "cluster refused cut admission", beforeG)
	testharness.AssertNoFDLeak(t, "cluster refused cut admission", beforeFD, 0)
}

func exerciseClusterStoreRefusal(t *testing.T) {
	t.Helper()
	_, transport := raft.NewInmemTransport("refused-cut")
	node, store := clusterStoreRefusalFixture(t, transport)
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := node.ApplyMetaSet("t:before", "ok"); err != nil {
		t.Fatal(err)
	}
	f := newCutFixture(t)
	f.cfg.Mode, f.cfg.Store = ModeSoleVoter, store
	f.control.authHook = func() error { return node.ApplyMetaSet("t:auth-resumed", "ok") }
	f.cfg.FreezeServer = func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) {
		return nil, errors.New("legacy_session_busy")
	}
	cut, err := Freeze(f.cfg)
	if cut != nil || err == nil || err.Error() != "legacy_session_busy" {
		t.Fatalf("fixture must refuse at the tunnel leaf after Store.Quiesce: cut=%v err=%v events=%v", cut, err, f.events.snapshot())
	}
	if err := node.ApplyMetaSet("t:after-refused-cut", "ok"); err != nil {
		t.Fatalf("refused cut left the store write-fenced after reporting rollback: %v (events %v)", err, f.events.snapshot())
	}
}

func clusterStoreRefusalFixture(t *testing.T, transport raft.Transport) (*cluster.Node, StoreOwner) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	node, err := cluster.New(cluster.Config{LocalID: "refused-cut", DataDir: dir, DBPath: dbPath,
		Transport: transport, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ApplyTimeout: 5 * time.Second,
		ImageRegistry: storage.DefaultDriverRegistry(),
		ImageStamp: func(db *sql.DB) (storage.StoreStamp, error) {
			identity, err := storage.CanonicalStoreIdentity(dbPath, dir)
			if err != nil {
				return storage.StoreStamp{}, err
			}
			return storage.ReadStoreStamp(db, identity)
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	lock, err := cluster.AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture keeps its lock until the real Node has joined and closed SQL.
	t.Cleanup(func() { _ = node.Shutdown(); _ = lock.Release() })
	store, err := NewClusterStore(node, lock, cluster.ProductionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return node, store
}
