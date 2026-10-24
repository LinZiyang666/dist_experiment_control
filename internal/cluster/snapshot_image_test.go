package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/raft"
)

// origin: hot-update S6 plan §3.4 / T15
// Force the NEW durable ACK between staging and Restore's final mutation point.
// The reader intentionally loses its wrapper identity, just as raft's counting
// reader and FileSnapshotStore do; metadata survives by the staged byte digest.
func TestInstallSnapshotCurrentFloor(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSnapshotCurrentFloor(t)
		exerciseRaftSnapshotInstall(t)
	}
	testharness.AssertNoGoroutineLeak(t, "snapshot current floor", gb)
	testharness.AssertNoFDLeak(t, "snapshot current floor", fb, 0)
}

func exerciseSnapshotCurrentFloor(t *testing.T) {
	t.Helper()
	f, db := freshFSM(t, t.TempDir())
	n := &Node{fsm: f, db: db, ro: f.ro}
	defer func() { _ = f.ro.Close(); _ = db.Close() }()
	f.appliers[OpClusterMetaSet] = snapshotLockApplier{owner: f, delegate: f.appliers[OpClusterMetaSet], t: t}
	applySnapshotMeta(t, f, 10, 2, "ack:old", "old-bytes")
	stale := snapshotBytes(t, f)
	ticket := snapshotTestTicket(stale, 12, 2) // config/barrier gap is legal
	stage, err := StageSnapshotRPC(n, ticket, io.NopCloser(bytes.NewReader(stale)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stage.Close() }()
	staged, err := io.ReadAll(stage)
	if err != nil {
		t.Fatal(err)
	}
	read := &snapshotEOFBarrier{Reader: bytes.NewReader(staged), ready: make(chan struct{}), proceed: make(chan struct{})}
	result := make(chan error, 1)
	defer func() { _ = read.Close() }()
	go func() { result <- f.Restore(read) }()
	select {
	case <-read.ready:
	case <-time.After(5 * time.Second):
		_ = read.Close()
		<-result
		t.Fatal("restore did not reach the staged EOF barrier")
	}
	applySnapshotMeta(t, f, 20, 3, "ack:new", "new-nonce-and-bytes")
	before := clusterMetaHash(t, db)
	inodeBefore, err := os.Stat(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = read.Close()
	if err := <-result; err == nil || !strings.Contains(err.Error(), "current command floor") {
		t.Fatalf("stale snapshot after NEW ACK must return an install error, got %v", err)
	}
	if got := clusterMetaHash(t, db); got != before {
		t.Fatal("stale snapshot changed acknowledged rows/cursor/term")
	}
	inodeAfter, err := os.Stat(f.dbPath)
	if err != nil || !os.SameFile(inodeBefore, inodeAfter) {
		t.Fatalf("stale restore changed the live inode: %v", err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}

	// Unbound bytes and conflicting all-entry metadata must fail closed; these
	// assertions kill nil/no-op and unbound-reader mutations independently.
	if err := f.Restore(io.NopCloser(bytes.NewReader(snapshotBytes(t, f)))); err == nil {
		t.Fatal("runtime FSM accepted an unbound snapshot")
	}
	bad := snapshotTestTicket(snapshotBytes(t, f), 19, 3)
	if owner, err := StageSnapshotRPC(n, bad, io.NopCloser(bytes.NewReader(snapshotBytes(t, f)))); err == nil {
		_ = owner.Close()
		t.Fatal("SQL cursor beyond RPC snapshot index was accepted")
	}
	bad = snapshotTestTicket(snapshotBytes(t, f), 22, 2)
	if owner, err := StageSnapshotRPC(n, bad, io.NopCloser(bytes.NewReader(snapshotBytes(t, f)))); err == nil {
		_ = owner.Close()
		t.Fatal("payload term beyond RPC snapshot term was accepted")
	}
	bad = snapshotTestTicket(snapshotBytes(t, f), 22, 3)
	bad.ConfigIndex = bad.Index + 1
	if owner, err := StageSnapshotRPC(n, bad, io.NopCloser(bytes.NewReader(snapshotBytes(t, f)))); err == nil {
		_ = owner.Close()
		t.Fatal("future configuration index was accepted")
	}
	if got := clusterMetaHash(t, db); got != before {
		t.Fatal("rejected metadata polluted current SQL state")
	}

	// Valid HIGHER snapshot: prefix retained, SQL floor 30, Raft index 34. A
	// guard equating SQL and all-entry cursors would incorrectly refuse this.
	source, srcDB := freshFSM(t, t.TempDir())
	defer func() { _ = source.ro.Close(); _ = srcDB.Close() }()
	applySnapshotMeta(t, source, 10, 2, "ack:old", "old-bytes")
	applySnapshotMeta(t, source, 20, 3, "ack:new", "new-nonce-and-bytes")
	applySnapshotMeta(t, source, 30, 4, "ack:catchup", "higher-bytes")
	fresh := snapshotBytes(t, source)
	owner, err := StageSnapshotRPC(n, snapshotTestTicket(fresh, 34, 4), io.NopCloser(bytes.NewReader(fresh)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	payload, err := io.ReadAll(owner)
	if err != nil {
		t.Fatal(err)
	}
	wrongPayload := append(append([]byte(nil), payload...), 0)
	if err := f.Restore(io.NopCloser(bytes.NewReader(wrongPayload))); err == nil {
		t.Fatal("a different staged payload consumed the snapshot RPC ticket")
	}
	if got := clusterMetaHash(t, db); got != before {
		t.Fatal("unbound higher payload changed current state")
	}
	observed := false
	f.snapshotInstallObserve = func() {
		observed = true
		if f.mutationMu.TryLock() {
			f.mutationMu.Unlock()
			t.Error("restore mutation point is outside Apply serialization")
		}
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader(payload))); err != nil {
		t.Fatalf("valid higher snapshot must catch up: %v", err)
	}
	if !observed {
		t.Fatal("successful catch-up never reached the restore mutation point")
	}
	if got, want := clusterMetaHash(t, db), clusterMetaHash(t, srcDB); got != want {
		t.Fatalf("catch-up restored wrong contents: %s != %s", got, want)
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader(payload))); err == nil {
		t.Fatal("snapshot ticket was reused")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	configuration := raft.Configuration{Servers: []raft.Server{{ID: "new-config", Address: "new-config", Suffrage: raft.Voter}}}
	recordSnapshotConfiguration(f, 35, configuration)
	if cursor, err := readAppliedIndexDB(db); err != nil || cursor != 30 {
		t.Fatalf("configuration polluted SQL command cursor: %d, %v", cursor, err)
	}
	configDigest := sha256.Sum256(raft.EncodeConfiguration(configuration))
	badConfig := snapshotTestTicket(payload, 40, 4)
	badConfig.ConfigIndex, badConfig.ConfigDigest = 34, hex.EncodeToString(configDigest[:])
	if owner, err := StageSnapshotRPC(n, badConfig, io.NopCloser(bytes.NewReader(payload))); err == nil {
		_ = owner.Close()
		t.Fatal("snapshot rolled back a configuration-only entry")
	}
	badConfig.ConfigIndex = 35
	badConfig.ConfigDigest = strings.Repeat("0", 64)
	if owner, err := StageSnapshotRPC(n, badConfig, io.NopCloser(bytes.NewReader(payload))); err == nil {
		_ = owner.Close()
		t.Fatal("same-index configuration with a different digest was accepted")
	}
}

// origin: hot-update S6 plan §3.4 / T17
// The stale snapshot is REAL, and newer acknowledged rows are in the durable DB.
// Enabling startup Restore loses the exact replay/no-op evidence asserted here.
func TestBrokerRestartSkipsStartupSnapshot(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSnapshotRestart(t)
	}
	testharness.AssertNoGoroutineLeak(t, "snapshot startup skip", gb)
	testharness.AssertNoFDLeak(t, "snapshot startup skip", fb, 0)
}

func exerciseSnapshotRestart(t *testing.T) {
	t.Helper()
	r := newRestartable(t, t.TempDir(), "snapshot-restart")
	n := r.open()
	mustApply(t, n, "t:ack:before-snapshot", "old")
	if err := n.Snapshot(); err != nil {
		t.Fatal(err)
	}
	meta, rc := openSnapshot(t, r.dir)
	_ = rc.Close()
	for i := 0; i < 3; i++ {
		mustApply(t, n, fmt.Sprintf("t:ack:after-snapshot:%d", i), fmt.Sprintf("nonce:%d", i))
	}
	want := clusterMetaHash(t, n.db)
	floor, err := n.AppliedIndex()
	if err != nil || floor <= meta.Index {
		t.Fatalf("fixture has no newer acknowledged command: %d <= %d, %v", floor, meta.Index, err)
	}
	n = r.restart()
	if !raftConfig(Config{}).NoSnapshotRestoreOnStart {
		t.Fatal("startup snapshot restore is enabled")
	}
	if got := clusterMetaHash(t, n.db); got != want {
		t.Fatal("restart lost durable ACK prefix")
	}
	if got := n.fsm.reapplyCount.Load(); got != 3 {
		t.Fatalf("restart replay performed %d verified no-ops, want exactly 3", got)
	}
	if got, err := n.AppliedIndex(); err != nil || got != floor {
		t.Fatalf("restart changed the SQL command floor: %d != %d, %v", got, floor, err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	r.current = nil
}

// origin: hot-update S6 plan §12.1 P05 / T16 cancellation seam
func TestBrokerSnapshotLeakCycles(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSnapshotCancel(t)
	}
	testharness.AssertNoGoroutineLeak(t, "snapshot cancellation", gb)
	testharness.AssertNoFDLeak(t, "snapshot cancellation", fb, 0)
}

func exerciseSnapshotCancel(t *testing.T) {
	t.Helper()
	f, db := freshFSM(t, t.TempDir())
	n := &Node{fsm: f, db: db, ro: f.ro}
	defer func() { _ = f.ro.Close(); _ = db.Close() }()
	before := clusterMetaHash(t, db)
	r, w := io.Pipe()
	defer func() { _ = w.Close() }()
	result := make(chan error, 1)
	go func() { result <- f.Restore(r) }()
	// Pipe.Write returns only after the real streaming reader consumed bytes.
	if _, err := w.Write([]byte("SQLite format 3\x00")); err != nil {
		t.Fatal(err)
	}
	joined := FenceSnapshotWork(n)
	if err := <-result; err == nil {
		t.Fatal("cancelled/stalled snapshot reported success")
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled restore reader did not join")
	}
	if got := clusterMetaHash(t, db); got != before {
		t.Fatal("cancelled stage changed live state")
	}
	if files, err := filepath.Glob(filepath.Join(f.tmpDir, "restore-*.db")); err != nil || len(files) != 0 {
		t.Fatalf("cancelled stage leaked scratch files: %v, %v", files, err)
	}
	if err := ResumeSnapshotWork(n); err != nil {
		t.Fatal(err)
	}

	// Cancel between actual backup steps; ALWAYS Finish the modernc handle.
	backupStepGate = func() { _ = FenceSnapshotWork(n) }
	err := BackupSnapshotDB(t.Context(), n, filepath.Join(f.tmpDir, "cancelled-backup.db"))
	backupStepGate = nil
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("backup ignored its cancellation token: %v", err)
	}
	if db.Stats().InUse != 0 || f.ro.Stats().InUse != 0 {
		t.Fatal("cancelled backup retained a SQL reader")
	}
	if err := ResumeSnapshotWork(n); err != nil {
		t.Fatal(err)
	}
	store, err := raft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, transport := raft.NewInmemTransport("persist-test")
	defer func() { _ = transport.Close() }()
	configuration := raft.Configuration{Servers: []raft.Server{{ID: "persist-test", Address: "persist-test", Suffrage: raft.Voter}}}
	sink, err := store.Create(raft.SnapshotVersionMax, 3, 1, configuration, 1, transport)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	backupStepGate = func() { _ = FenceSnapshotWork(n) }
	err = snapshot.Persist(sink)
	backupStepGate = nil
	snapshot.Release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Persist ignored its cancellation token: %v", err)
	}
	list, err := store.List()
	if err != nil || len(list) != 0 {
		t.Fatalf("cancelled Persist finalized a snapshot: %v, %v", list, err)
	}
	if err := ResumeSnapshotWork(n); err != nil {
		t.Fatal(err)
	}
	sink, err = store.Create(raft.SnapshotVersionMax, 3, 1, configuration, 1, transport)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snapshot.Release()
	_, persisted, err := store.Open(sink.ID())
	if err != nil {
		t.Fatal(err)
	}
	persistedBytes, readErr := io.ReadAll(persisted)
	closeErr := persisted.Close()
	if readErr != nil || closeErr != nil || len(persistedBytes) == 0 {
		t.Fatalf("successful Persist did not stream bytes: %v, %v", readErr, closeErr)
	}
	good := snapshotBytes(t, f)
	owner, err := StageSnapshotRPC(n, snapshotTestTicket(good, 3, 1), io.NopCloser(bytes.NewReader(good)))
	if err != nil {
		t.Fatal(err)
	}
	joined = FenceSnapshotWork(n)
	select {
	case <-joined:
		t.Fatal("staged RPC was declared joined before its owner closed")
	default:
	}
	if err := ResumeSnapshotWork(n); err == nil {
		t.Fatal("resume accepted a retained RPC owner")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	stagedOwner := owner.(*snapshotRPCStage)
	if _, err := stagedOwner.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("staging owner did not close its FD: %v", err)
	}
	if _, err := os.Stat(stagedOwner.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging owner did not remove its scratch file: %v", err)
	}
	<-joined
	if err := ResumeSnapshotWork(n); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §3.4 / bounded stage and schema controls
func TestBrokerSnapshotStageControls(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSnapshotStageControls(t)
	}
	testharness.AssertNoGoroutineLeak(t, "snapshot staging controls", gb)
	testharness.AssertNoFDLeak(t, "snapshot staging controls", fb, 0)
}

func exerciseSnapshotStageControls(t *testing.T) {
	t.Helper()
	if _, err := copySnapshot(t.Context(), io.Discard, strings.NewReader("12345"), 4); err == nil {
		t.Fatal("bounded staging accepted a fifth byte")
	}
	if n, err := copySnapshot(t.Context(), io.Discard, strings.NewReader("1234"), 4); err != nil || n != 4 {
		t.Fatalf("exact-limit staging refused: %d, %v", n, err)
	}
	f, db := freshFSM(t, t.TempDir())
	defer func() { _ = f.ro.Close(); _ = db.Close() }()
	n := &Node{fsm: f, db: db, ro: f.ro}
	before := clusterMetaHash(t, db)
	src, srcDB := freshFSM(t, t.TempDir())
	defer func() { _ = src.ro.Close(); _ = srcDB.Close() }()
	if _, err := srcDB.Exec(`DROP TABLE cluster_reqid_ledger`); err != nil {
		t.Fatal(err)
	}
	bad := snapshotBytes(t, src)
	if owner, err := StageSnapshotRPC(n, snapshotTestTicket(bad, 3, 1), io.NopCloser(bytes.NewReader(bad))); err == nil {
		_ = owner.Close()
		t.Fatal("unchanged migration ledger with a dropped table was accepted")
	}
	if got := clusterMetaHash(t, db); got != before {
		t.Fatal("bad schema changed live state")
	}
	good := snapshotBytes(t, f)
	configuration := raft.Configuration{Servers: []raft.Server{{ID: "peer", Address: "peer", Suffrage: raft.Voter}}}
	request := raft.InstallSnapshotRequest{SnapshotVersion: raft.SnapshotVersionMax, Term: 1, LastLogIndex: 3, LastLogTerm: 1, Configuration: raft.EncodeConfiguration(configuration), ConfigurationIndex: 1, Size: int64(len(good))}
	if err := ValidateSnapshotRPC(ticketForSnapshotRequest(request), &request); err != nil {
		t.Fatalf("matching actual RPC metadata refused: %v", err)
	}
	for _, configurationBytes := range [][]byte{
		[]byte("malformed-msgpack"),
		append(append([]byte(nil), request.Configuration...), 0),
		raft.EncodeConfiguration(raft.Configuration{}),
		raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{ID: "peer", Address: "peer", Suffrage: raft.Nonvoter}}}),
		raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{ID: "peer", Address: "peer", Suffrage: raft.ServerSuffrage(99)}}}),
		raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{ID: "peer", Address: "peer", Suffrage: raft.Voter}, {ID: "peer", Address: "other", Suffrage: raft.Voter}}}),
	} {
		badRequest := request
		badRequest.Configuration = configurationBytes
		if err := ValidateSnapshotRPC(ticketForSnapshotRequest(badRequest), &badRequest); err == nil {
			t.Fatal("invalid RPC configuration accepted")
		}
	}
	for _, delta := range []int64{-1, 1} {
		ticket := snapshotTestTicket(good, 3, 1)
		ticket.PayloadBytes += delta
		if owner, err := StageSnapshotRPC(n, ticket, io.NopCloser(bytes.NewReader(good))); err == nil {
			_ = owner.Close()
			t.Fatal("mismatched payload length accepted")
		}
	}
}

// origin: hot-update S6 plan §12.1 P05 / FuzzBrokerSnapshotStamp
func FuzzBrokerSnapshotStamp(f *testing.F) {
	for _, seed := range [][2]string{{"", ""}, {"0", "0"}, {"10", "2"}, {"18446744073709551615", "1"}, {"01", "2"}, {"1", ""}, {"-1", "2"}, {"1", "18446744073709551616"}} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, index, term string) {
		i, trm, err := parseSnapshotCursor(index, term)
		if err != nil {
			return
		}
		if index == "" && term == "" {
			if i != 0 || trm != 0 {
				t.Fatal("empty cursor is nonzero")
			}
			return
		}
		if formatUint(i) != index || formatUint(trm) != term || (i == 0) != (trm == 0) {
			t.Fatal("cursor parser accepted a noncanonical/impossible pair")
		}
	})
}

func applySnapshotMeta(t *testing.T, f *fsm, index, term uint64, key, value string) {
	t.Helper()
	data, err := json.Marshal(NewCommand(OpClusterMetaSet, Statement{SQL: `INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, Args: []any{key, value}}))
	if err != nil {
		t.Fatal(err)
	}
	if result := f.Apply(&raft.Log{Type: raft.LogCommand, Index: index, Term: term, Data: data}); result != (appliedOK{index}) {
		t.Fatalf("durable ACK was not applied: %#v", result)
	}
}

func snapshotBytes(t *testing.T, f *fsm) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := backupTo(t.Context(), f.ro, path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func snapshotTestTicket(payload []byte, index, term uint64) clusterio.SnapshotTicket {
	digest := sha256.Sum256([]byte("actual-configuration"))
	return clusterio.SnapshotTicket{RPCID: 1, Index: index, Term: term, ConfigIndex: 1, ConfigDigest: hex.EncodeToString(digest[:]), PayloadBytes: int64(len(payload))}
}

type snapshotEOFBarrier struct {
	*bytes.Reader
	ready, proceed chan struct{}
	once           sync.Once
}

func (r *snapshotEOFBarrier) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		close(r.ready)
		<-r.proceed
	}
	return n, err
}

func (r *snapshotEOFBarrier) Close() error { r.once.Do(func() { close(r.proceed) }); return nil }

// exerciseRaftSnapshotInstall uses actual Raft InstallSnapshot futures and their
// real metadata changes. It supplies P04's transport boundary in this unit harness;
// P26 owns the independent production mTLS + ACK interleave receipt.
func exerciseRaftSnapshotInstall(t *testing.T) {
	t.Helper()
	f, db := freshFSM(t, t.TempDir())
	defer func() { _ = f.ro.Close(); _ = db.Close() }()
	n := &Node{fsm: f, db: db, ro: f.ro}
	RequireSnapshotTickets(n)
	_, receiving := raft.NewInmemTransport("snapshot-target")
	_, sending := raft.NewInmemTransport("snapshot-source")
	defer func() { _ = receiving.Close(); _ = sending.Close() }()
	sending.Connect("snapshot-target", receiving)
	transport := &snapshotTransportHarness{Transport: receiving, requests: make(chan raft.RPC), stop: make(chan struct{}), done: make(chan struct{}), owner: n}
	store := raft.NewInmemStore()
	snaps, err := raft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	// No bootstrap/election: the first valid snapshot supplies real membership.
	// A 2-voter configuration then keeps this node a follower during the test.
	// Use the production multi-node floor: default N=1 elections (200 ms)
	// otherwise race disk staging while this isolated follower has no heartbeat.
	config := raftConfig(Config{LocalID: "snapshot-target", HeartbeatTimeout: MultinodeHeartbeatTimeout, ElectionTimeout: MultinodeElectionTimeout, LeaderLeaseTimeout: MultinodeLeaderLeaseTimeout})
	r, err := raft.NewRaft(config, f, store, store, snaps, transport)
	if err != nil {
		t.Fatal(err)
	}
	n.raft.Store(r)
	go transport.run()
	defer func() {
		if err := r.Shutdown().Error(); err != nil {
			t.Error(err)
		}
		close(transport.stop)
		<-transport.done
	}()
	configuration := raft.Configuration{Servers: []raft.Server{
		{Suffrage: raft.Voter, ID: "snapshot-target", Address: "snapshot-target"},
		{Suffrage: raft.Voter, ID: "snapshot-source", Address: "snapshot-source"},
	}}
	src, srcDB := freshFSM(t, t.TempDir())
	defer func() { _ = src.ro.Close(); _ = srcDB.Close() }()
	applySnapshotMeta(t, src, 5, 1, "ack:prefix", "original")
	payload := snapshotBytes(t, src)
	req := raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax}, SnapshotVersion: raft.SnapshotVersionMax, Term: 1, LastLogIndex: 10, LastLogTerm: 1, Configuration: raft.EncodeConfiguration(configuration), ConfigurationIndex: 1, Size: int64(len(payload))}
	resp := raft.InstallSnapshotResponse{}
	if err := sending.InstallSnapshot("snapshot-target", "snapshot-target", &req, &resp, bytes.NewReader(payload)); err != nil || !resp.Success {
		t.Fatalf("valid initial InstallSnapshot failed: success=%t, err=%v", resp.Success, err)
	}
	applySnapshotMeta(t, f, 20, 1, "ack:new", "new durable bytes")
	before := clusterMetaHash(t, db)
	cf := r.GetConfiguration()
	if err := cf.Error(); err != nil {
		t.Fatal(err)
	}
	beforeIndex, beforeTerm, beforeConfigIndex := r.AppliedIndex(), r.Stats()["term"], latestSnapshotConfigIndex(t, snaps)
	beforeConfig := cf.Configuration()
	// A corrupted ticket/config MUST be caught before Raft can touch the term.
	changed := req
	changed.Term = 100
	changed.Configuration = raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: "intruder", Address: "intruder"}}})
	ticket := ticketForSnapshotRequest(req)
	if err := ValidateSnapshotRPC(ticket, &changed); err == nil {
		t.Fatal("configuration-polluting request matched the original RPC ticket")
	}
	// Actually submit a stale snapshot carrying a different membership. The
	// pre-Raft dynamic floor must reject it with a non-nil install error.
	stale := changed
	stale.Term = 1
	resp = raft.InstallSnapshotResponse{}
	if err := sending.InstallSnapshot("snapshot-target", "snapshot-target", &stale, &resp, bytes.NewReader(payload)); err == nil || resp.Success {
		t.Fatalf("stale InstallSnapshot must fail: success=%t, err=%v", resp.Success, err)
	}
	cf = r.GetConfiguration()
	if err := cf.Error(); err != nil {
		t.Fatal(err)
	}
	if r.AppliedIndex() != beforeIndex || r.Stats()["term"] != beforeTerm || latestSnapshotConfigIndex(t, snaps) != beforeConfigIndex ||
		!bytes.Equal(raft.EncodeConfiguration(cf.Configuration()), raft.EncodeConfiguration(beforeConfig)) || clusterMetaHash(t, db) != before {
		t.Fatal("rejected InstallSnapshot polluted rows/cursor/Raft index/term/configuration")
	}
	// Successful higher install proves the production restore path ran and that
	// membership/index advancement is observable when, and only when, accepted.
	applySnapshotMeta(t, src, 20, 1, "ack:new", "new durable bytes")
	applySnapshotMeta(t, src, 30, 1, "ack:catchup", "fresh")
	payload = snapshotBytes(t, src)
	term, err := parseUint(r.Stats()["term"])
	if err != nil {
		t.Fatal(err)
	}
	req.Term = term
	req.LastLogIndex, req.ConfigurationIndex, req.Size = 34, 25, int64(len(payload))
	resp = raft.InstallSnapshotResponse{}
	if err := sending.InstallSnapshot("snapshot-target", "snapshot-target", &req, &resp, bytes.NewReader(payload)); err != nil || !resp.Success {
		t.Fatalf("valid higher InstallSnapshot failed: success=%t, err=%v", resp.Success, err)
	}
	cf = r.GetConfiguration()
	if err := cf.Error(); err != nil {
		t.Fatal(err)
	}
	if r.AppliedIndex() != 34 || latestSnapshotConfigIndex(t, snaps) != 25 || clusterMetaHash(t, db) != clusterMetaHash(t, srcDB) {
		t.Fatalf("higher InstallSnapshot failed to advance SQL and separate Raft metadata: applied=%d config=%d SQL=%s source=%s", r.AppliedIndex(), latestSnapshotConfigIndex(t, snaps), clusterMetaHash(t, db), clusterMetaHash(t, srcDB))
	}
}

type snapshotTransportHarness struct {
	raft.Transport
	requests   chan raft.RPC
	stop, done chan struct{}
	owner      *Node
}

func (s *snapshotTransportHarness) Consumer() <-chan raft.RPC { return s.requests }

func (s *snapshotTransportHarness) run() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case rpc := <-s.Transport.Consumer():
			req, ok := rpc.Command.(*raft.InstallSnapshotRequest)
			if !ok {
				select {
				case s.requests <- rpc:
				case <-s.stop:
					return
				}
				continue
			}
			ticket := ticketForSnapshotRequest(*req)
			err := ValidateSnapshotRPC(ticket, req)
			var stage io.ReadCloser
			if err == nil {
				stage, err = StageSnapshotRPC(s.owner, ticket, io.NopCloser(rpc.Reader))
			}
			if err != nil {
				rpc.Respond(&raft.InstallSnapshotResponse{}, err)
				continue
			}
			forwarded := rpc
			forwarded.Reader = stage
			responses := make(chan raft.RPCResponse, 1)
			forwarded.RespChan = responses
			select {
			case s.requests <- forwarded:
			case <-s.stop:
				_ = stage.Close()
				return
			}
			select {
			case response := <-responses:
				_ = stage.Close()
				rpc.Respond(response.Response, response.Error)
			case <-s.stop:
				_ = stage.Close()
				return
			}
		}
	}
}

func ticketForSnapshotRequest(req raft.InstallSnapshotRequest) clusterio.SnapshotTicket {
	digest := sha256.Sum256(req.Configuration)
	return clusterio.SnapshotTicket{RPCID: 1, Index: req.LastLogIndex, Term: req.LastLogTerm, ConfigIndex: req.ConfigurationIndex, ConfigDigest: hex.EncodeToString(digest[:]), PayloadBytes: req.Size}
}

func latestSnapshotConfigIndex(t *testing.T, store raft.SnapshotStore) uint64 {
	t.Helper()
	// raft v1.7.3 GetConfiguration().Index() is always zero (it constructs a
	// future with latest but without latestIndex). Read the independent persisted
	// snapshot metadata rather than treating that API's zero as a real index.
	list, err := store.List()
	if err != nil || len(list) == 0 {
		t.Fatalf("missing persisted Raft snapshot metadata: %v", err)
	}
	return list[0].ConfigurationIndex
}

// The test observes the SAME mutex from inside a real committed SQL applier.
// Pool size alone cannot serialize the final floor check with a later Apply.
type snapshotLockApplier struct {
	owner    *fsm
	delegate Applier
	t        *testing.T
}

func (a snapshotLockApplier) ApplyTx(tx *sql.Tx, cmd *Command) error {
	if a.owner.mutationMu.TryLock() {
		a.owner.mutationMu.Unlock()
		a.t.Error("Apply transaction is outside Restore serialization")
	}
	return a.delegate.ApplyTx(tx, cmd)
}
