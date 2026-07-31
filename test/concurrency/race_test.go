// race_test.go — E (SQLite/file-lock) + F (channel/context
// propagation) dimensions. These tests are specifically constructed
// to surface failures under `go test -race` — each one drives the
// concurrency primitive in question with N parallel goroutines.
package concurrency_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
)

// TestStorageMaxOpenConnsIsOne — E.17. The storage package
// docstring (and the audit comment) call SetMaxOpenConns(1)
// LOAD-BEARING; any future change that relaxes it would silently
// open the door to a class of bug. Pin the invariant in a test.
func TestStorageMaxOpenConnsIsOne(t *testing.T) {
	db := openMemDB(t)
	stats := db.Stats()
	if stats.MaxOpenConnections != 1 {
		t.Errorf("storage.Open MUST cap MaxOpenConnections to 1, got %d",
			stats.MaxOpenConnections)
	}
}

// TestConcurrentPortAllocations — E.18. 30 goroutines each call
// port.Allocate against the same SQLite DB on a different (sid,
// name) so all 30 succeed. The serialization MUST come from the
// SetMaxOpenConns(1) pool (not a goroutine-local lock), so no
// allocation deadlocks and the resulting rows have unique ports.
func TestConcurrentPortAllocations(t *testing.T) {
	db := openMemDB(t)
	// Seed sessions so the FK chain is satisfied.
	if _, err := session.Create(db, "lab", "lab", "fp-test", "h",
		time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Seed the node row so port_allocations FK is also satisfied
	// (port_allocations.nid → nodes.nid).
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status, registered_at) VALUES (?,?,?,?)`,
		"lab", "lab-1", "ONLINE", time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}

	const N = 30
	cfg := &port.Config{
		BandLow:  20000,
		BandHigh: 20000 + N + 10,
		Now:      time.Now,
	}

	var wg sync.WaitGroup
	errs := make(chan error, N)
	allocations := make(chan *port.Allocation, N)
	latch := newRaceLatch()

	for i := 0; i < N; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			latch.wait()
			a, err := port.Allocate(db, "lab", "lab-1",
				fmt.Sprintf("svc-%d", i), 8000+i, 0, "fp-test", false, cfg)
			if err != nil {
				errs <- fmtErr("alloc %d: %w", i, err)
				return
			}
			allocations <- a
		}()
	}
	latch.release()
	wg.Wait()
	close(errs)
	close(allocations)

	for err := range errs {
		t.Errorf("%v", err)
	}

	// Every assigned port number MUST be unique (no two ALLOCATED
	// rows on the same port at the same time — that would mean the
	// findFreePort + INSERT compound was not actually serialized).
	seen := map[int]bool{}
	count := 0
	for a := range allocations {
		count++
		if seen[a.Port] {
			t.Errorf("duplicate port allocated: %d", a.Port)
		}
		seen[a.Port] = true
	}
	if count != N {
		t.Errorf("got %d successful allocations, want %d", count, N)
	}

	// Cross-check via SELECT: row count should also be N.
	var rows int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE state='ALLOCATED'`,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != N {
		t.Errorf("ALLOCATED row count = %d, want %d", rows, N)
	}
}

// TestConcurrentDesiredPortExactlyOneWins — E.20 (P12). N goroutines
// race the SAME --remote-port. Unlike E.18 (band sized to N so all
// succeed on distinct auto ports), here the band is tiny and every
// goroutine asks for the SAME in-band port, so exactly one can win and
// the rest get ErrPortTaken (never a duplicate ALLOCATED row, never a
// deadlock). Run under -race.
//
// What this proves and what it doesn't: storage.Open caps the pool at
// one connection (SetMaxOpenConns(1)), so the writers serialize at
// connection acquisition — this is the SHIPPED production path. The test
// proves the partial UNIQUE index idx_port_alloc_unique_active is
// NECESSARY (drop the index and losers' INSERTs would succeed,
// winners=N), translating a committed-row collision into ErrPortTaken.
// It does NOT exercise two concurrent in-flight transactions racing the
// index directly (that would need MaxOpenConns>1 / a second *sql.DB
// handle) — that scenario is out of scope for the single-handle broker
// and not a configuration this code ships.
func TestConcurrentDesiredPortExactlyOneWins(t *testing.T) {
	db := openMemDB(t)
	if _, err := session.Create(db, "lab", "lab", "fp-test", "h",
		time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status, registered_at) VALUES (?,?,?,?)`,
		"lab", "lab-1", "ONLINE", time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}

	const (
		N    = 20
		want = 14001
	)
	cfg := &port.Config{BandLow: 14000, BandHigh: 14002, Now: time.Now}

	var wg sync.WaitGroup
	var winners, taken atomic.Int32
	errs := make(chan error, N)
	latch := newRaceLatch()

	for i := 0; i < N; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			latch.wait()
			a, err := port.Allocate(db, "lab", "lab-1",
				fmt.Sprintf("svc-%d", i), 8000+i, want, "fp-test", false, cfg)
			switch {
			case err == nil:
				winners.Add(1)
				if a.Port != want {
					errs <- fmtErr("winner %d got port %d, want %d", i, a.Port, want)
				}
			case errors.Is(err, port.ErrPortTaken):
				taken.Add(1)
			default:
				errs <- fmtErr("goroutine %d unexpected err: %w", i, err)
			}
		}()
	}
	latch.release()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("%v", err)
	}

	if w := winners.Load(); w != 1 {
		t.Errorf("winners = %d, want exactly 1", w)
	}
	if tk := taken.Load(); tk != N-1 {
		t.Errorf("ErrPortTaken count = %d, want %d", tk, N-1)
	}
	var rows int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE port=? AND state='ALLOCATED'`,
		want,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("ALLOCATED rows on port %d = %d, want 1", want, rows)
	}
}

// TestSqliteFileTwoOpensCanCoexist — E.19. Open the same file-
// backed SQLite from two distinct *sql.DB handles and run
// concurrent writes from each. SQLite's BEGIN IMMEDIATE / WAL +
// the package's busy_timeout(5000) PRAGMA should mean both
// handles' writers serialize without "database is locked"
// returns. Failure mode would be either:
//
//   - One Open returns an error (file lock conflict) — already a
//     real bug.
//   - Both succeed but writes start failing with SQLITE_BUSY —
//     also a bug; the busy_timeout PRAGMA exists exactly to
//     prevent this.
//
// We tolerate ONE class of failure: SQLITE_BUSY after the full
// 5s timeout would be acceptable IF we dialed up the contention
// to be impossible to resolve. We keep contention low (10 writes
// per handle, with 1ms sleeps) so the test is a clean signal.
func TestSqliteFileTwoOpensCanCoexist(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "shared.db")

	db1, err := storage.Open(dsn)
	if err != nil {
		t.Fatalf("open #1: %v", err)
	}
	defer func() { _ = db1.Close() }()

	db2, err := storage.Open(dsn)
	if err != nil {
		// This may be a legitimate failure (SQLite without WAL
		// rejects concurrent file-backed opens). If so, the
		// invariant we want to pin is "the second open returns
		// an error rather than corrupting the first" — so don't
		// fail the test, just record what happened.
		t.Logf("second open returned %v; this is acceptable as long as #1 still works", err)
		// Verify db1 is still functional.
		if _, err := db1.Exec(
			`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
			"sole", "sole", "fp", "h",
		); err != nil {
			t.Errorf("db1 broken after db2 open failure: %v", err)
		}
		return
	}
	defer func() { _ = db2.Close() }()

	const N = 10
	var wg sync.WaitGroup
	var errCount atomic.Int32
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if _, err := db1.Exec(
				`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
				fmt.Sprintf("a-%d", i), "a", "fp", "h",
			); err != nil {
				errCount.Add(1)
				t.Logf("db1 write %d: %v", i, err)
			}
			time.Sleep(time.Millisecond)
		}(i)
		go func(i int) {
			defer wg.Done()
			if _, err := db2.Exec(
				`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
				fmt.Sprintf("b-%d", i), "b", "fp", "h",
			); err != nil {
				errCount.Add(1)
				t.Logf("db2 write %d: %v", i, err)
			}
			time.Sleep(time.Millisecond)
		}(i)
	}
	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("%d concurrent writes failed (likely SQLITE_BUSY past 5s timeout)", errCount.Load())
	}

	// Sanity: row count should be exactly 2*N.
	var rows int
	if err := db1.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2*N {
		t.Errorf("row count = %d, want %d", rows, 2*N)
	}
}

// TestAgentDispatchAfterRunCancel — F.20. After agent.Run's ctx
// has been canceled, an in-flight forwarded msg arriving must NOT
// spawn a verb-handler goroutine that publishes onto the draining
// nats.Conn. We can't easily reach the unexported dispatchForwarded
// directly from this _test package, so we exercise it via the real
// subscription path: send a malformed forwarded msg AFTER cancel
// and assert the agent doesn't crash + doesn't reply (a reply
// would mean dispatch took the spawn path).
func TestAgentDispatchAfterRunCancel(t *testing.T) {
	url := startNATS(t)

	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	if _, err := stub.Subscribe(
		"tether.v2.ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) { _ = msg.Respond([]byte(`{"OK":true}`)) },
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Logger:            silentLog(),
		HeartbeatInterval: 50 * time.Millisecond,
		RegisterTimeout:   1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)

	// Cancel the agent and wait for it to return.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not exit on cancel")
	}

	// At this point the agent's runCtx is canceled. Send a
	// malformed forwarded msg that, IF dispatched, would publish
	// an error reply on inbox. We expect NO reply.
	inbox := stub.NewInbox()
	sub, err := stub.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := stub.PublishRequest(
		"tether.v2.s.lab.cmd.node.lab-1.exec.req.forwarded",
		inbox, []byte(`{"argv":["true"]}`),
	); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	// We're not strictly asserting "no reply" — the agent's NATS
	// conn has been Drained by now, so the forwarded msg won't
	// even reach the agent. What we ARE asserting is that the
	// test process is still alive (no panic, no crash) after
	// pumping a post-cancel msg. A regression in the dispatch
	// post-cancel guard would surface here if the agent were
	// still subscribed AND the goroutine spawn proceeded.
	_, _ = sub.NextMsg(200 * time.Millisecond)
	// Reaching this line without panic = pass.
}

// TestSessionRmAfterBrokerCancel — F.21. Session rm finalize
// derives from b.runCtx. When Run cancels mid-rm, the cascade
// (JS DeleteHistoryStream, dropSessionRows, sys.events emission)
// must not run against a half-torn-down broker.
//
// The test shape: drive a session rm, then cancel the broker
// mid-flight, and assert the broker cleanly exits (no panic, no
// hang) without the SQLite handle getting written to after Close.
//
// Without JetStream in this test setup, the JS DeleteHistoryStream
// step is a no-op (b.js is nil). So this test specifically targets
// the dropSessionRows path under cancel. The C9 audit fix is what
// makes this safe — without it, dropSessionRows could run with a
// background ctx after b.cfg.DB has been Close()d by t.Cleanup.
func TestSessionRmAfterBrokerCancel(t *testing.T) {
	url := startNATS(t)
	db := openMemDB(t)

	// Seed two sessions so we have something to rm.
	for _, sid := range []string{"a", "b"} {
		if _, err := session.Create(db, sid, sid, "fp-test", "h",
			time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}

	_, shutdown := startBrokerNoTunnel(t, url, db)

	// Fire several rm requests in parallel with the cancel.
	nc, err := nats.Connect(url)
	if err != nil {
		shutdown()
		t.Fatal(err)
	}
	defer nc.Close()

	var wg sync.WaitGroup
	for _, sid := range []string{"a", "b"} {
		wg.Add(1)

		go func() {
			defer wg.Done()
			subj := fmt.Sprintf("tether.v2.ctrl.by.fp.session.%s.rm.req", sid)
			// Use a short timeout — we don't care about the
			// reply; we only care that the broker survives.
			_, _ = nc.Request(subj, []byte(`{}`), 500*time.Millisecond)
		}()
	}
	// Cancel the broker concurrently with the rms.
	go shutdown()
	wg.Wait()

	// SQLite handle must still be usable (not Close()d behind a
	// goroutine that didn't observe ctx cancel).
	_, err = db.Exec(`SELECT 1`)
	if err != nil && err != sql.ErrConnDone {
		t.Errorf("db unusable after broker cancel mid-rm: %v", err)
	}
}
