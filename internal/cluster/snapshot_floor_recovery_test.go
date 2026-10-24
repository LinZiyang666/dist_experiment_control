package cluster

import (
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/LinZiyang666/tether/internal/testharness"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// The runtime InstallSnapshot floor must not refuse raft.RecoverCluster's own
// local-snapshot restore: RecoverCluster restores the NEWEST local snapshot and
// then replays the local log tail on top. A live DB that has advanced past the
// last snapshot is the normal state of every node that has ever snapshotted.
// origin: hot-update S6 internal review rv-writer
func TestRecoverToSelfOnlineAcceptsOlderLocalSnapshot(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseOnlineSnapshotRecovery(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "online snapshot recovery", gb)
	testharness.AssertNoFDLeak(t, "online snapshot recovery", fb, 0)
}

func exerciseOnlineSnapshotRecovery(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	id := raft.ServerID("brk-floor-online")
	n := mustNode(t, dir, id)
	n.transportFactory = inmemFactory(id)
	if err := n.ApplyMetaSet("t:before-snapshot", "a"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := n.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := n.ApplyMetaSet("t:after-snapshot", "b"); err != nil {
		t.Fatalf("post-snapshot write: %v", err)
	}
	if err := n.RecoverToSelfOnline(string(id)); err != nil {
		t.Fatalf("online force-single refused by snapshot floor: %v", err)
	}
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("not leader after recover: %v", err)
	}
	var v string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:after-snapshot'`).Scan(&v); err != nil || v != "b" {
		t.Fatalf("post-snapshot ACK lost: %q %v", v, err)
	}
}

// origin: hot-update S6 internal review rv-writer
func TestRecoverSingleNodeAcceptsOlderLocalSnapshot(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseOfflineSnapshotRecovery(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "offline snapshot recovery", gb)
	testharness.AssertNoFDLeak(t, "offline snapshot recovery", fb, 0)
}

func exerciseOfflineSnapshotRecovery(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	id := raft.ServerID("brk-floor-offline")
	n, err := openNodeRaw(dir, id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("leader: %v", err)
	}
	if err := n.ApplyMetaSet("t:before-snapshot", "a"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := n.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := n.ApplyMetaSet("t:after-snapshot", "b"); err != nil {
		t.Fatalf("post-snapshot write: %v", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := RecoverSingleNode(dir, filepath.Join(dir, "state.db"), string(id), string(id), quietLogger()); err != nil {
		t.Fatalf("offline force-single refused by snapshot floor: %v", err)
	}
}
