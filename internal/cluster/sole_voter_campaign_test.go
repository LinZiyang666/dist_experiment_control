package cluster

import (
	"bytes"
	"database/sql"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The elapsed-time gate also includes SQLite validation and OS scheduling. Keep
// it intact, and independently distinguish an actual heartbeat-driven election
// from time spent before Raft starts. The cold election proves the log oracle
// sees the production heartbeat; the hot election must reach leader without it.
// origin: S6 internal review SOLE-VOTER-CAMPAIGN F1
func TestImageSoleVoterCampaignPrecedesHeartbeat(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseImageCampaignElectionCause(t)
	}
	testharness.AssertNoGoroutineLeak(t, "image campaign election cause x5", beforeG)
	testharness.AssertNoFDLeak(t, "image campaign election cause x5", beforeFD, 0)
}

func exerciseImageCampaignElectionCause(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	lock, err := AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "lock.Release", lock.Release)
	ca := newTestCA(t)
	transportConfig := MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "campaign-cause")}
	transport, err := NewMTLSTransport(transportConfig)
	if err != nil {
		t.Fatal(err)
	}
	transportConfig.BindAddr = string(transport.LocalAddr())
	coldLog := &campaignElectionLog{}
	cfg := fastMultinodeCfg("campaign-cause", dir, transport, nil)
	cfg.Logger = slog.New(slog.NewTextHandler(coldLog, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cold, err := New(cfg)
	if cold != nil {
		defer checkImageCleanup(t, "cold.Shutdown", cold.Shutdown)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := cold.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := cold.ApplyMetaSet("t:campaign-cause", "original-ack"); err != nil {
		t.Fatal(err)
	}
	// Shutdown joins Raft before inspecting its log, so a missing heartbeat
	// cannot be explained by the logger still being in flight.
	cfg, stamp := closeCurrentStoreSeed(t, cold)
	if !coldLog.contains("heartbeat timeout reached, starting election") || !coldLog.contains("entering leader state") {
		t.Fatalf("cold election did not establish the heartbeat oracle: %s", coldLog.text())
	}
	opened := time.Duration(0)
	opener := cfg.ImageOpenCurrent
	cfg.ImageOpenCurrent = func(config storage.OpenImageConfig) (*sql.DB, error) {
		started := time.Now()
		db, err := opener(config)
		opened = time.Since(started)
		return db, err
	}
	hotLog := &campaignElectionLog{}
	cfg.Logger = slog.New(slog.NewTextHandler(hotLog, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg.Transport, err = NewMTLSTransport(transportConfig)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current, err := NewForImage(cfg, stamp)
	if current != nil {
		defer checkImageCleanup(t, "current.Shutdown", current.Shutdown)
	}
	if err != nil {
		t.Fatal(err)
	}
	reopened := time.Since(started)
	if err := current.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	// A real acknowledged write proves election, the leader barrier and replay
	// all completed on the current store, rather than merely observing Candidate.
	if err := current.ApplyMetaSet("t:campaign-current", "current-ack"); err != nil {
		t.Fatal(err)
	}
	written := time.Since(started)
	if err := current.Shutdown(); err != nil {
		t.Fatal(err)
	}
	t.Logf("hot campaign: current-open=%s constructor=%s first-write=%s", opened, reopened, written)
	if hotLog.contains("heartbeat timeout reached, starting election") {
		t.Fatalf("hot singleton used its heartbeat timer to elect: %s", hotLog.text())
	}
	if !hotLog.contains("entering candidate state") || !hotLog.contains("entering leader state") {
		t.Fatalf("hot election trace is incomplete: %s", hotLog.text())
	}
	if written > 5*time.Second {
		t.Fatalf("hot singleton first write exceeded its cut budget: %s", written)
	}
	// Inspect the running Raft configuration as well as the retained inputs:
	// rebuilding raftConfig(cfg) alone misses a shortcut applied only to rc.
	live := current.raft.Load().ReloadableConfig()
	if live.HeartbeatTimeout != MultinodeHeartbeatTimeout || live.ElectionTimeout != MultinodeElectionTimeout {
		t.Fatalf("hot campaign changed live Raft timers: %+v", live)
	}
	rc := raftConfig(current.cfg)
	if rc.HeartbeatTimeout != MultinodeHeartbeatTimeout || rc.ElectionTimeout != MultinodeElectionTimeout || rc.LeaderLeaseTimeout != MultinodeLeaderLeaseTimeout {
		t.Fatal("hot campaign shortened production timers")
	}
}

// Raft logs from its own goroutines. Inspecting an ordinary bytes.Buffer while
// those goroutines write would make the diagnostic itself a race.
type campaignElectionLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *campaignElectionLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *campaignElectionLog) contains(message string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Contains(l.buf.Bytes(), []byte(message))
}

func (l *campaignElectionLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
