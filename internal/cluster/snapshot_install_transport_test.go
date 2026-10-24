package cluster

import (
	"bytes"
	"io"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/LinZiyang666/tether/internal/testharness"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// A follower behind the leader's compacted log can only catch up through
// InstallSnapshot. The production constructor wraps every mTLS transport in
// imageNetworkTransport; that wrapper must still deliver a legitimate leader
// snapshot to Raft (or validate and stage it), never refuse every snapshot.
// Here leader A is grow-ready (log compacted by GrowReadySnapshot) and B joins
// as a nonvoter, exactly the `cluster grow` shape documented in offline.go.
// origin: hot-update S6 internal review rv-writer
func TestFollowerInstallsCompactedLeaderSnapshotOverProductionTransport(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseProductionSnapshotCatchup(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "production snapshot catch-up", gb)
	testharness.AssertNoFDLeak(t, "production snapshot catch-up", fb, 0)
}

func exerciseProductionSnapshotCatchup(t *testing.T) {
	t.Helper()
	ca := newTestCA(t)
	dirA, dirB := t.TempDir(), t.TempDir()

	trA, err := NewMTLSTransport(MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "snap-a")})
	if err != nil {
		t.Fatal(err)
	}
	addrA := trA.LocalAddr()
	a, err := New(fastMultinodeCfg("snap-a", dirA, trA, []raft.Server{{Suffrage: raft.Voter, ID: "snap-a", Address: addrA}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Shutdown() })
	if err := a.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := a.ApplyMetaSet("t:snapshot-only", "seeded"); err != nil {
		t.Fatal(err)
	}
	if err := a.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := GrowReadySnapshot(dirA, filepath.Join(dirA, "state.db"), "snap-a", string(addrA), quietLogger()); err != nil {
		t.Fatalf("grow-ready snapshot: %v", err)
	}

	trA2, err := NewMTLSTransport(MTLSTransportConfig{BindAddr: string(addrA), CACert: ca.pool, Leaf: ca.leaf(t, "snap-a")})
	if err != nil {
		t.Fatal(err)
	}
	a2, err := New(fastMultinodeCfg("snap-a", dirA, trA2, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a2.Shutdown() })
	if err := a2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	trB, err := NewMTLSTransport(MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "snap-b")})
	if err != nil {
		t.Fatal(err)
	}
	// B knows A as the sole voter, so it never campaigns; it only follows.
	b, err := New(fastMultinodeCfg("snap-b", dirB, trB, []raft.Server{{Suffrage: raft.Voter, ID: "snap-a", Address: addrA}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Shutdown() })
	if err := a2.AddNonvoter("snap-b", string(trB.LocalAddr())); err != nil {
		t.Fatal(err)
	}
	ok := waitForCond(15*time.Second, func() bool {
		var v string
		return b.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:snapshot-only'`).Scan(&v) == nil && v == "seeded"
	})
	if !ok {
		t.Fatalf("follower never installed the compacted leader snapshot (B applied=%d, A last=%d)",
			b.raft.Load().AppliedIndex(), a2.raft.Load().LastIndex())
	}
}

// origin: hot-update S6 plan §3.4 / T15 / T16; internal review rv-writer-2
func TestProductionSnapshotHooksOwnEveryLifecyclePhase(t *testing.T) {
	if imageSnapshotInitialize == nil || imageSnapshotStage == nil || imageSnapshotFence == nil || imageSnapshotResume == nil || imageSnapshotBackup == nil {
		t.Fatal("production snapshot lifecycle has an unassigned adapter")
	}
}

// Configuration-only commits must advance the network floor without changing
// the command-only FSM contract. Bootstrap configuration and later membership
// entries live in Bolt even when no newer SQL command or snapshot exists.
// origin: hot-update S6 plan §3.4 / T15; internal review rv-writer-2/11
func TestRuntimeSnapshotFloorReadsCommittedConfiguration(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseCommittedSnapshotConfiguration(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "committed snapshot configuration", gb)
	testharness.AssertNoFDLeak(t, "committed snapshot configuration", fb, 0)
}

func exerciseCommittedSnapshotConfiguration(t *testing.T) {
	t.Helper()
	ca := newTestCA(t)
	dir := t.TempDir()
	transport, err := NewMTLSTransport(MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "config-floor")})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(fastMultinodeCfg("config-floor", dir, transport, nil))
	if n != nil {
		t.Cleanup(func() { _ = n.Shutdown() })
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyMetaSet("t:config-floor", "ack"); err != nil {
		t.Fatal(err)
	}
	old := raft.EncodeConfiguration(n.raft.Load().GetConfiguration().Configuration())
	cursor, err := n.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.AddNonvoter("config-other", "127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	if after, err := n.AppliedIndex(); err != nil || after != cursor {
		t.Fatalf("membership changed SQL command cursor: %d -> %d / %v", cursor, after, err)
	}
	index, _, err := committedSnapshotConfiguration(n)
	if err != nil || index <= cursor {
		t.Fatalf("membership not durably committed above SQL cursor: %d / %v", index, err)
	}
	payload := snapshotBytes(t, n.fsm)
	stamp, err := snapshotStampDB(n.db)
	if err != nil {
		t.Fatal(err)
	}
	req := raft.InstallSnapshotRequest{SnapshotVersion: raft.SnapshotVersionMax,
		Term: stamp.term, LastLogIndex: n.raft.Load().LastIndex() + 1, LastLogTerm: stamp.term,
		ConfigurationIndex: 1, Configuration: old, Size: int64(len(payload))}
	before := clusterMetaHash(t, n.db)
	if stage, err := stageNodeSnapshotRPC(n, &req, ticketForSnapshotRequest(req), io.NopCloser(bytes.NewReader(payload))); err == nil {
		_ = stage.Close()
		t.Fatal("runtime snapshot rolled back a committed configuration-only entry")
	}
	req.ConfigurationIndex = index
	req.Configuration = raft.EncodeConfiguration(n.raft.Load().GetConfiguration().Configuration())
	stage, err := stageNodeSnapshotRPC(n, &req, ticketForSnapshotRequest(req), io.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		t.Fatalf("matching committed configuration refused: %v", err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	// Keep the transport request and its captured ticket inseparable. The
	// payload itself is valid, so a missing adapter check would stage it even
	// though Raft would consume different request metadata.
	ticket := ticketForSnapshotRequest(req)
	changed := req
	changed.Size++
	if stage, err := stageNodeSnapshotRPC(n, &changed, ticket, io.NopCloser(bytes.NewReader(payload))); err == nil {
		_ = stage.Close()
		t.Fatal("production adapter accepted a changed snapshot request")
	}
	if stage, err := stageNodeSnapshotRPC(n, &req, ticket, bytes.NewReader(payload)); err == nil {
		_ = stage.Close()
		t.Fatal("production adapter accepted a payload without a close owner")
	}
	different := n.raft.Load().GetConfiguration().Configuration()
	different.Servers = append([]raft.Server(nil), different.Servers...)
	different.Servers[0].Address = "127.0.0.1:2"
	req.Configuration = raft.EncodeConfiguration(different)
	if stage, err := stageNodeSnapshotRPC(n, &req, ticketForSnapshotRequest(req), io.NopCloser(bytes.NewReader(payload))); err == nil {
		_ = stage.Close()
		t.Fatal("runtime snapshot changed a same-index committed configuration")
	}
	if after := clusterMetaHash(t, n.db); before != after {
		t.Fatal("configuration staging changed acknowledged SQL state")
	}
}
