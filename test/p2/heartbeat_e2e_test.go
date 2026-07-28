// Package p2_test exercises the full P2 control loop end-to-end:
// embedded NATS server + tether serve (in-process broker) + tether agent
// (in-process), driving the ONLINE → STALE → OFFLINE state machine on
// accelerated timings (sub-second thresholds) so the test wall time stays
// under a few seconds.
package p2_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// TestHeartbeatLifecycle drives the canonical P2 control loop:
// agent registers → broker writes ONLINE → heartbeats arrive → agent
// canceled → broker reconciles to STALE → reconciles to OFFLINE → agent
// re-registers → broker brings back to ONLINE.
//
// Timings are scaled: 50ms heartbeat / 200ms STALE / 600ms OFFLINE so the
// state machine exercises every branch in well under a second per phase.
func TestHeartbeatLifecycle(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Wide STALE window (200ms → 3s) so the matrix-load CPU
	// pressure can't make the test fly past STALE and observe
	// only OFFLINE. Audit shard 05: original 200/600 was 400ms
	// wide; under `make e2e-parallel` parallel loadgen one tick of
	// scheduling jitter could miss it. The total test deadline
	// is still bounded by the per-phase 2s waitForState calls.
	bCfg := broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silent,
		ReconcileInterval: 30 * time.Millisecond,
		StaleAfter:        200 * time.Millisecond,
		OfflineAfter:      3 * time.Second,
	}
	b, err := broker.New(bCfg)
	if err != nil {
		t.Fatal(err)
	}

	bCtx, bCancel := context.WithCancel(context.Background())
	defer bCancel()
	bDone := make(chan error, 1)
	go func() { bDone <- b.Run(bCtx) }()

	// --- Phase 1: ONLINE ----------------------------------------------------
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Logger:            silent,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	aCtx, aCancel := context.WithCancel(context.Background())
	aDone := make(chan error, 1)
	go func() { aDone <- a.Run(aCtx) }()

	waitForState(t, db, "lab", "lab-1", "ONLINE", 2*time.Second)

	// --- Phase 2: kill agent → STALE -----------------------------------------
	aCancel()
	select {
	case <-aDone:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not exit on cancel")
	}
	waitForState(t, db, "lab", "lab-1", "STALE", 2*time.Second)

	// --- Phase 3: still gone → OFFLINE ---------------------------------------
	// Now that OfflineAfter is 3s (was 600ms), Phase 3 needs a
	// matching deadline.
	waitForState(t, db, "lab", "lab-1", "OFFLINE", 5*time.Second)

	// --- Phase 4: agent re-registers → ONLINE again --------------------------
	a2, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Logger:            silent,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	a2Ctx, a2Cancel := context.WithCancel(context.Background())
	defer a2Cancel()
	a2Done := make(chan error, 1)
	go func() { a2Done <- a2.Run(a2Ctx) }()

	waitForState(t, db, "lab", "lab-1", "ONLINE", 2*time.Second)

	// --- Cleanup -------------------------------------------------------------
	a2Cancel()
	<-a2Done
	bCancel()
	select {
	case <-bDone:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not exit on cancel")
	}
}

// startNATS launches an embedded NATS server on an ephemeral port.
// B9: delegates to the single implementation in internal/testharness — the body was
// character-for-character identical.
func startNATS(t *testing.T) string {
	t.Helper()
	return testharness.StartNATS(t)
}

// openDB returns a fresh on-disk SQLite seeded with the "lab" session row
// so the agent's register.req has a valid FK target. The on-disk path
// (vs :memory:) exercises the production code path; broker unit tests in
// internal/broker use :memory: instead.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open("file:" + filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:p2-test-owner", "p2-test-hash",
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return db
}

// waitForState polls node.List until the named (sid, nid) reaches `want`,
// failing the test if `timeout` elapses first.
func waitForState(t *testing.T, db *sql.DB, sid, nid, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		snaps, err := node.List(db)
		if err != nil {
			t.Fatalf("node.List: %v", err)
		}
		for _, s := range snaps {
			if s.SID == sid && s.NID == nid {
				last = s.Status
				if last == want {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("waitForState(%s/%s, %q) timed out after %s; last=%q", sid, nid, want, timeout, last)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
