package broker

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"errors"
	"sort"
	"strings"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// reconcile_passes_test.go (R7a) — the per-pass contract.
//
// Two obligations are discharged here.
//
// 1. EFFECT EQUIVALENCE. The cadence half of the rewrite is proven in
//    reconcile_registry_test.go under a fake clock; this file proves the other
//    half — that a registry sweep leaves the database in the byte-identical
//    state the pre-R7 inline loop body would have. The pre-R7 bodies are
//    reproduced verbatim below as ORACLES and run against a parallel fixture.
//
// 2. THE THREE IDEMPOTENCE TESTS, for every registered pass:
//      (a) once converged, N consecutive ticks produce ZERO further side effects;
//      (b) after an external change, the next tick re-converges;
//      (c) two brokers driving the same state produce exactly ONE write.
//
//    (a) is the one that matters most. Periodizing an action that was previously
//    one-shot is only safe if the converged state is a fixed point; if it is
//    not, R7 does not fix a bug, it schedules one every 30 seconds.

const (
	passStale   = 30 * time.Second
	passOffline = 90 * time.Second
)

var passEpoch = time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)

// --------------------------------------------------------------------------
// fixtures
// --------------------------------------------------------------------------

func passTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedLivenessFixture installs one session and three nodes whose last heartbeat
// ages straddle both thresholds: fresh (ONLINE), stale (STALE), ancient
// (OFFLINE). All three rows start mislabeled ONLINE so a reconcile pass has real
// work to do on its first tick and none on the second.
func seedLivenessFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('lab','lab','o','p','ACTIVE',?)`, passEpoch); err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		nid string
		age time.Duration
	}{
		{"fresh", 1 * time.Second},
		{"stale", 45 * time.Second},
		{"gone", 10 * time.Minute},
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO nodes(nid,sid,status,registered_at,last_heartbeat_at) VALUES(?,'lab','ONLINE',?,?)`,
			r.nid, passEpoch, passEpoch.Add(-r.age)); err != nil {
			t.Fatal(err)
		}
	}
}

func nodeStatuses(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT nid, status FROM nodes ORDER BY nid`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var nid, st string
		if err := rows.Scan(&nid, &st); err != nil {
			t.Fatal(err)
		}
		out[nid] = st
	}
	return out
}

// passBroker builds a NATS-free, single-mode broker driven by a fake clock.
// publishOnConn/publishAudit degrade to a logged warning without a connection,
// which is exactly the pre-R7 behavior on a disconnected broker, so the DB
// effects under test are unaffected.
func passBroker(t *testing.T, db *sql.DB, clk *fakeClock) *Broker {
	t.Helper()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{
		DB:                   db,
		Logger:               silentLogger(),
		Now:                  clk.Now,
		ReconcileInterval:    time.Second,
		StaleAfter:           passStale,
		OfflineAfter:         passOffline,
		ProcRetention:        time.Hour,
		ProcGCInterval:       5 * time.Minute,
		XferReapInterval:     5 * time.Minute,
		GrowLockReapInterval: 30 * time.Second,
		// R7b: registering a pass with a zero interval is a wiring PANIC by design, so a fixture that
		// forgets a new pass's interval fails loudly rather than silently dropping the pass.
		UpgradeLockReapInterval: 30 * time.Second,
		HomeDeliverInterval:     5 * time.Second,  // R8a P1 home-delivery pass
		DrainMarkerReapInterval: 30 * time.Second, // batch C drain-marker orphan reaper
	}
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	b.registerCoreReconcilePasses()
	b.reconcilers.start(clk.Now())
	return b
}

// passBrokerFollower is passBroker with the registry's leadership gate wired
// FALSE. It models "this broker is a cluster follower" at the only layer that
// matters for the leader-only contract — the registry gate — without faking a
// clusterMode broker that has no raft runtime (which would simply nil-panic in
// livenessDB, proving nothing).
func passBrokerFollower(t *testing.T, db *sql.DB, clk *fakeClock) *Broker {
	t.Helper()
	b := passBroker(t, db, clk)
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, func() bool { return false })
	b.registerCoreReconcilePasses()
	b.reconcilers.start(clk.Now())
	return b
}

// runPass drives exactly one named pass, bypassing the schedule. Scheduling is
// proven separately; these tests are about what a pass DOES.
func runPass(t *testing.T, b *Broker, name string, now time.Time) error {
	t.Helper()
	for _, p := range b.reconcilers.passes {
		if p.name == name {
			return p.fn(context.Background(), now)
		}
	}
	t.Fatalf("no registered pass named %q", name)
	return nil
}

// --------------------------------------------------------------------------
// 1. EFFECT EQUIVALENCE with the pre-R7 inline loop bodies
// --------------------------------------------------------------------------

// legacyReconcileTickOracle is the pre-R7 `case <-ticker.C:` body, verbatim.
// It is the ORACLE, not a helper: it must never be refactored to share code
// with the registry passes, because then it would stop being independent
// evidence and start being a tautology.
func legacyReconcileTickOracle(b *Broker, now time.Time) {
	n, err := node.ReconcileStates(b.livenessDB(), now, b.cfg.StaleAfter, b.cfg.OfflineAfter)
	if err != nil {
		b.cfg.Logger.Warn("broker: reconcile failed", "err", err)
	} else if n > 0 {
		b.cfg.Logger.Info("broker: state transitions", "count", n)
	}
	if !b.clusterMode || b.cl.node.IsLeader() {
		if revoked := b.reconcilePorts(now); revoked > 0 {
			b.cfg.Logger.Info("broker: port revocations", "count", revoked)
		}
	}
	if closed := b.reconcileTunnelSessions(); closed > 0 {
		b.cfg.Logger.Info("broker: stale tunnel proxies closed", "count", closed)
	}
}

// legacyGCTickOracle is the pre-R7 `case <-gcTicker.C:` body, verbatim.
func legacyGCTickOracle(b *Broker, now time.Time) {
	if b.clusterMode {
		return
	}
	cutoff := now.Add(-b.cfg.ProcRetention)
	n, err := proc.GCExited(b.livenessDB(), cutoff)
	if err != nil {
		b.cfg.Logger.Warn("broker: proc gc", "err", err)
	} else if n > 0 {
		b.cfg.Logger.Info("broker: proc gc", "deleted", n, "cutoff", cutoff)
	}
}

// TestReconcilePassEffectsMatchLegacy runs the pre-R7 oracle and the R7 registry
// against two independently-seeded but identical fixtures for 20 ticks, and
// requires the resulting database state to be identical at every step.
//
// Twenty ticks rather than one, because a single tick would not distinguish "the
// same effect" from "the same FIRST effect": several of these passes are
// convergent, so their second tick is a no-op regardless of whether the first
// one was faithful.
func TestReconcilePassEffectsMatchLegacy(t *testing.T) {
	clkLegacy := newFakeClock(passEpoch)
	clkRegistry := newFakeClock(passEpoch)

	dbLegacy := passTestDB(t)
	dbRegistry := passTestDB(t)
	seedLivenessFixture(t, dbLegacy)
	seedLivenessFixture(t, dbRegistry)
	seedPortFixture(t, dbLegacy)
	seedPortFixture(t, dbRegistry)
	seedProcFixture(t, dbLegacy)
	seedProcFixture(t, dbRegistry)

	bLegacy := passBroker(t, dbLegacy, clkLegacy)
	bRegistry := passBroker(t, dbRegistry, clkRegistry)

	for tick := 1; tick <= 20; tick++ {
		nowL := clkLegacy.advance(time.Second)
		legacyReconcileTickOracle(bLegacy, nowL)
		legacyGCTickOracle(bLegacy, nowL)

		nowR := clkRegistry.advance(time.Second)
		for _, name := range []string{"node-states", "ports", "tunnel-sessions", "proc-gc"} {
			if err := runPass(t, bRegistry, name, nowR); err != nil {
				t.Fatalf("tick %d: pass %s returned %v; the pre-R7 bodies never returned an error, so returning one here changes cadence via backoff", tick, name, err)
			}
		}

		if got, want := nodeStatuses(t, dbRegistry), nodeStatuses(t, dbLegacy); !sameStringMap(got, want) {
			t.Fatalf("tick %d: node states diverged\n registry=%v\n  legacy=%v", tick, got, want)
		}
		if got, want := portStates(t, dbRegistry), portStates(t, dbLegacy); !sameStringMap(got, want) {
			t.Fatalf("tick %d: port allocation states diverged\n registry=%v\n  legacy=%v", tick, got, want)
		}
		if got, want := procPIDs(t, dbRegistry), procPIDs(t, dbLegacy); !sameStringMap(got, want) {
			t.Fatalf("tick %d: processes diverged\n registry=%v\n  legacy=%v", tick, got, want)
		}
	}

	// Sanity: the fixture must have actually exercised the passes. An
	// equivalence proof over two no-ops proves nothing.
	if st := nodeStatuses(t, dbRegistry); st["gone"] != "OFFLINE" || st["stale"] != "STALE" || st["fresh"] != "ONLINE" {
		t.Fatalf("fixture never converged — the equivalence run was vacuous: %v", st)
	}
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// --------------------------------------------------------------------------
// 2a. pass: node-states
// --------------------------------------------------------------------------

func TestPassNodeStatesIdempotence(t *testing.T) {
	// (a) converged ⇒ zero side effects on 3 consecutive ticks.
	t.Run("converged is a fixed point", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedLivenessFixture(t, db)
		b := passBroker(t, db, clk)

		if err := runPass(t, b, "node-states", clk.Now()); err != nil {
			t.Fatal(err)
		}
		converged := nodeStatuses(t, db)

		for i := 0; i < 3; i++ {
			// Time does NOT advance: the converged state must be stable under
			// re-evaluation, independent of any clock movement.
			if err := runPass(t, b, "node-states", clk.Now()); err != nil {
				t.Fatal(err)
			}
			if got := nodeStatuses(t, db); !sameStringMap(got, converged) {
				t.Fatalf("tick %d mutated a converged fixture: %v -> %v", i+1, converged, got)
			}
		}
	})

	// (b) external drift ⇒ re-converges.
	t.Run("re-converges after external drift", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedLivenessFixture(t, db)
		b := passBroker(t, db, clk)
		if err := runPass(t, b, "node-states", clk.Now()); err != nil {
			t.Fatal(err)
		}
		if nodeStatuses(t, db)["gone"] != "OFFLINE" {
			t.Fatal("precondition: 'gone' must have converged to OFFLINE")
		}

		// Something outside the loop flips it back — a stale write, a restored
		// backup, an operator. The pass must not need an event to notice.
		if _, err := db.Exec(`UPDATE nodes SET status='ONLINE' WHERE nid='gone'`); err != nil {
			t.Fatal(err)
		}
		if err := runPass(t, b, "node-states", clk.Now()); err != nil {
			t.Fatal(err)
		}
		if got := nodeStatuses(t, db)["gone"]; got != "OFFLINE" {
			t.Fatalf("pass did not re-converge after external drift: 'gone' is %q, want OFFLINE", got)
		}
	})

	// (c) two brokers ⇒ no duplicate writes.
	//
	// node-states is per-broker-local by design (each broker reconciles the
	// agents homed to it, reading its own livenessDB), so the meaningful
	// statement is that concurrent evaluation converges to the same fixed point
	// without corrupting rows — which is what the shared-DB race below asserts.
	t.Run("concurrent brokers converge without corruption", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedLivenessFixture(t, db)
		b1 := passBroker(t, db, clk)
		b2 := passBroker(t, db, clk)

		var wg sync.WaitGroup
		for _, b := range []*Broker{b1, b2} {
			wg.Add(1)
			go func(b *Broker) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					_ = runPass(t, b, "node-states", clk.Now())
				}
			}(b)
		}
		wg.Wait()

		st := nodeStatuses(t, db)
		if st["fresh"] != "ONLINE" || st["stale"] != "STALE" || st["gone"] != "OFFLINE" {
			t.Fatalf("concurrent reconcilers left a non-converged state: %v", st)
		}
	})
}

// --------------------------------------------------------------------------
// 2b. pass: ports (the LEADER-ONLY shape)
// --------------------------------------------------------------------------

func seedPortFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	// The node must exist and be long-OFFLINE for the revoke scan to see it.
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('ports','ports','o','p','ACTIVE',?)`, passEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at,last_heartbeat_at) VALUES('dead','ports','OFFLINE',?,?)`,
		passEpoch, passEpoch.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at,last_heartbeat_at) VALUES('live','ports','ONLINE',?,?)`,
		passEpoch, passEpoch); err != nil {
		t.Fatal(err)
	}
	ins := `INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at) VALUES(?,'ports',?,?,?,?,'ALLOCATED','fp',?)`
	if _, err := db.Exec(ins, 20001, "dead", "svc-dead", 8080, "hash-dead", passEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ins, 20002, "live", "svc-live", 8081, "hash-live", passEpoch); err != nil {
		t.Fatal(err)
	}
}

func portStates(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT port, state FROM port_allocations ORDER BY port`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var p int
		var st string
		if err := rows.Scan(&p, &st); err != nil {
			t.Fatal(err)
		}
		out[fmt.Sprint(p)] = st
	}
	return out
}

func TestPassPortsIdempotence(t *testing.T) {
	t.Run("converged is a fixed point", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedPortFixture(t, db)
		b := passBroker(t, db, clk)

		if err := runPass(t, b, "ports", clk.Now()); err != nil {
			t.Fatal(err)
		}
		converged := portStates(t, db)
		if converged["20001"] != "REVOKED" || converged["20002"] != "ALLOCATED" {
			t.Fatalf("precondition: the OFFLINE node's port must be revoked and the live one left alone: %v", converged)
		}
		for i := 0; i < 3; i++ {
			if err := runPass(t, b, "ports", clk.Now()); err != nil {
				t.Fatal(err)
			}
			if got := portStates(t, db); !sameStringMap(got, converged) {
				t.Fatalf("tick %d mutated a converged fixture: %v -> %v", i+1, converged, got)
			}
		}
	})

	t.Run("re-converges after a new allocation goes stale", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedPortFixture(t, db)
		b := passBroker(t, db, clk)
		if err := runPass(t, b, "ports", clk.Now()); err != nil {
			t.Fatal(err)
		}

		// The live node dies while the broker was not looking. No event fires;
		// only the periodic pass can notice.
		if _, err := db.Exec(`UPDATE nodes SET status='OFFLINE', last_heartbeat_at=? WHERE nid='live'`, passEpoch.Add(-24*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := runPass(t, b, "ports", clk.Now()); err != nil {
			t.Fatal(err)
		}
		if got := portStates(t, db)["20002"]; got != "REVOKED" {
			t.Fatalf("pass did not re-converge: port 20002 is %q, want REVOKED", got)
		}
	})

	// (c) The leader-only gate is what makes "two brokers, one write" TRUE for
	// this pass, and it is enforced by the registry rather than by the pass. A
	// follower must never even invoke it.
	t.Run("a follower never runs the pass", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedPortFixture(t, db)

		follower := passBrokerFollower(t, db, clk)

		for i := 0; i < 5; i++ {
			follower.reconcilers.runDue(context.Background(), clk.advance(time.Second))
		}
		if got := portStates(t, db)["20001"]; got != "ALLOCATED" {
			t.Fatalf("a follower revoked a port: 20001 is %q — the leader-only gate is not being enforced", got)
		}
		for _, s := range follower.reconcilers.status() {
			if s.Name == "ports" && s.Runs != 0 {
				t.Fatalf("leader-only pass 'ports' ran %d times on a follower", s.Runs)
			}
		}
	})
}

// --------------------------------------------------------------------------
// 2c. pass: tunnel-sessions
// --------------------------------------------------------------------------

func TestPassTunnelSessionsIdempotence(t *testing.T) {
	clk := newFakeClock(passEpoch)
	db := passTestDB(t)
	b := passBroker(t, db, clk)

	// (a) With no tunnel server installed (the overwhelmingly common shape:
	// TunnelControlAddr unset) the pass must be a total no-op, forever.
	for i := 0; i < 3; i++ {
		if err := runPass(t, b, "tunnel-sessions", clk.Now()); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}
	if n := b.reconcileTunnelSessions(); n != 0 {
		t.Fatalf("reconcileTunnelSessions closed %d proxies with no tunnel server installed", n)
	}

	// (b)/(c) The pass only ever closes fds owned by THIS process, so a second
	// broker cannot double-close another's listener; and its "drift" input is
	// the local session table, re-read every tick. Both are structural, and are
	// asserted at the registry level: the pass is per-broker (never leader-
	// gated) so every broker keeps reaping its own fds even as leadership moves.
	for _, s := range b.reconcilers.status() {
		if s.Name == "tunnel-sessions" && s.Authority == authorityLeader.String() {
			t.Fatal("tunnel-sessions must NOT be leader-only: it reaps this process's own listener fds, and a follower that stopped reaping would leak them until it happened to win an election")
		}
	}
}

// --------------------------------------------------------------------------
// 2d. pass: proc-gc (the DIFFERENT-CADENCE shape)
// --------------------------------------------------------------------------

func seedProcFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('gc','gc','o','p','ACTIVE',?)`, passEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at,last_heartbeat_at) VALUES('n1','gc','ONLINE',?,?)`, passEpoch, passEpoch); err != nil {
		t.Fatal(err)
	}
	mk := func(pid string, endedAgo time.Duration) {
		p := proc.Process{PID: pid, SID: "gc", NID: "n1", Argv: []string{"x"}, StartedAt: passEpoch.Add(-endedAgo - time.Minute)}
		if err := proc.Insert(db, p); err != nil {
			t.Fatal(err)
		}
		if err := proc.MarkExited(db, pid, 0, passEpoch.Add(-endedAgo)); err != nil {
			t.Fatal(err)
		}
	}
	mk("old-1", 3*time.Hour) // past the 1h retention ⇒ collectable
	mk("old-2", 2*time.Hour) // past retention ⇒ collectable
	mk("recent", time.Minute)
}

func procPIDs(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT pid, status FROM processes ORDER BY pid`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var pid, st string
		if err := rows.Scan(&pid, &st); err != nil {
			t.Fatal(err)
		}
		out[pid] = st
	}
	return out
}

func TestPassProcGCIdempotence(t *testing.T) {
	t.Run("converged is a fixed point", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedProcFixture(t, db)
		b := passBroker(t, db, clk)

		if err := runPass(t, b, "proc-gc", clk.Now()); err != nil {
			t.Fatal(err)
		}
		converged := procPIDs(t, db)
		if _, still := converged["old-1"]; still {
			t.Fatalf("precondition: retention-expired rows must be collected: %v", converged)
		}
		if _, kept := converged["recent"]; !kept {
			t.Fatalf("proc-gc collected a row inside the retention window: %v", converged)
		}
		for i := 0; i < 3; i++ {
			if err := runPass(t, b, "proc-gc", clk.Now()); err != nil {
				t.Fatal(err)
			}
			if got := procPIDs(t, db); !sameStringMap(got, converged) {
				t.Fatalf("tick %d mutated a converged fixture: %v -> %v", i+1, converged, got)
			}
		}
	})

	t.Run("re-converges as rows age past retention", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedProcFixture(t, db)
		b := passBroker(t, db, clk)
		if err := runPass(t, b, "proc-gc", clk.Now()); err != nil {
			t.Fatal(err)
		}
		if _, kept := procPIDs(t, db)["recent"]; !kept {
			t.Fatal("precondition: 'recent' must survive the first sweep")
		}
		// Two hours pass. The row is now retention-expired; no event announces
		// that — only the periodic pass can act on the passage of time.
		if err := runPass(t, b, "proc-gc", clk.advance(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, kept := procPIDs(t, db)["recent"]; kept {
			t.Fatal("proc-gc did not re-converge once 'recent' aged past retention")
		}
	})

	// (c) In cluster mode `processes` is replicated, so deleting rows outside
	// raft would fork leader/follower SQLite contents. The pass must remain a
	// hard no-op there — this is the "two brokers must not both write" property
	// for proc-gc, and it is enforced by a MODE gate, not a leadership gate
	// (a leader deleting replicated rows outside raft is just as wrong).
	t.Run("cluster mode is a hard no-op", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		seedProcFixture(t, db)
		b := passBroker(t, db, clk)
		before := procPIDs(t, db)
		b.clusterMode = true

		for i := 0; i < 3; i++ {
			if err := runPass(t, b, "proc-gc", clk.advance(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		if got := procPIDs(t, db); !sameStringMap(got, before) {
			t.Fatalf("proc-gc deleted replicated rows outside raft in cluster mode: %v -> %v", before, got)
		}
	})
}

// --------------------------------------------------------------------------
// 2e. pass: grow-lock (#31) — the third-shape sample
// --------------------------------------------------------------------------

func setGrowMarker(t *testing.T, db *sql.DB, joiner string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('cluster_grow_active',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, joiner); err != nil {
		t.Fatal(err)
	}
}

func insertOp(t *testing.T, db *sql.DB, opID, target, state string, terminal bool, updated time.Time) {
	t.Helper()
	term := 0
	if terminal {
		term = 1
	}
	if _, err := db.Exec(
		`INSERT INTO cluster_operations(op_id,kind,target_node,op_state,terminal,created_at,updated_at) VALUES(?,'join',?,?,?,?,?)`,
		opID, target, state, term, updated, updated); err != nil {
		t.Fatal(err)
	}
}

func noVoters(string) (bool, error) { return false, nil }

// TestGrowLockDecision is the #31 truth table. Every row is a state the cluster
// can actually be in; the two that clear the marker are exactly the two the
// product already treats as "grow complete", and the two that do NOT are the
// ones where clearing would rip the serialization mutex out from under a live
// grow.
func TestGrowLockDecision(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(t *testing.T, db *sql.DB)
		isVoter   func(string) (bool, error)
		wantClear bool
		why       string
	}{
		{
			name:      "no marker",
			setup:     func(*testing.T, *sql.DB) {},
			isVoter:   noVoters,
			wantClear: false,
			why:       "nothing is held — the converged state must be a total no-op, or the pass writes every tick forever",
		},
		{
			name: "marker + live op",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
				insertOp(t, db, "op-1", "brk-b", "CATCHING_UP", false, passEpoch)
			},
			isVoter:   noVoters,
			wantClear: false,
			why:       "a grow IS in flight; clearing here would let a concurrent retire/upgrade slip past the mutex mid-grow",
		},
		{
			name: "marker + terminal op",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
				insertOp(t, db, "op-1", "brk-b", "SERVING", true, passEpoch)
			},
			isVoter:   noVoters,
			wantClear: true,
			why:       "#31 proper: the grow finished and the CLI's best-effort release was lost — this is the leak",
		},
		{
			name: "marker + failed terminal op",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
				insertOp(t, db, "op-1", "brk-b", "FAILED", true, passEpoch)
			},
			isVoter:   noVoters,
			wantClear: true,
			why:       "an ABORTED/FAILED grow is just as over as a successful one; leaving membership fenced after a failure is the worse outcome",
		},
		{
			name: "marker + no op at all + not a voter",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
			},
			isVoter:   noVoters,
			wantClear: false,
			why:       "the acquire-lock window: `cluster add` holds the marker before its op exists. Without a lease there is nothing to expire, so R7a's fail-closed behavior must survive R7b verbatim (TestGrowLockLeaseExpiry covers the leased case)",
		},
		{
			name: "marker + no op + already a voter",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
			},
			isVoter:   func(id string) (bool, error) { return id == "brk-b", nil },
			wantClear: true,
			why:       "the CLI's own P0 shortcut — a VOTER with no live join op is a completed grow whose op row was pruned",
		},
		{
			name: "marker + terminal op for a DIFFERENT node",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
				insertOp(t, db, "op-1", "brk-c", "SERVING", true, passEpoch)
			},
			isVoter:   noVoters,
			wantClear: false,
			why:       "another node's finished grow says nothing about this marker's holder",
		},
		{
			name: "marker + newer live op supersedes an older terminal one",
			setup: func(t *testing.T, db *sql.DB) {
				setGrowMarker(t, db, "brk-b")
				insertOp(t, db, "op-old", "brk-b", "FAILED", true, passEpoch.Add(-time.Hour))
				insertOp(t, db, "op-new", "brk-b", "CATCHING_UP", false, passEpoch)
			},
			isVoter:   noVoters,
			wantClear: false,
			why:       "a RETRY after a failed grow: an old terminal row must never authorize clearing the retry's live lock",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := passTestDB(t)
			tc.setup(t, db)
			joiner, clear, err := growLockDecision(db, tc.isVoter, passEpoch)
			if err != nil {
				t.Fatalf("decision returned %v", err)
			}
			if clear != tc.wantClear {
				t.Fatalf("clear=%v, want %v (joiner=%q)\nwhy: %s", clear, tc.wantClear, joiner, tc.why)
			}
		})
	}
}

func TestPassGrowLockIdempotence(t *testing.T) {
	// (a) converged ⇒ zero side effects. The pass's "write" is a raft Propose;
	// with no marker set the decision must never reach it, on any tick.
	t.Run("converged is a fixed point", func(t *testing.T) {
		db := passTestDB(t)
		for i := 0; i < 3; i++ {
			joiner, clear, err := growLockDecision(db, noVoters, passEpoch)
			if err != nil {
				t.Fatal(err)
			}
			if clear || joiner != "" {
				t.Fatalf("tick %d proposed a write against a converged cluster (joiner=%q clear=%v)", i+1, joiner, clear)
			}
		}
	})

	// Also a fixed point once a grow is legitimately in flight: 3 ticks, no write.
	t.Run("in-flight grow is a fixed point", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarker(t, db, "brk-b")
		insertOp(t, db, "op-1", "brk-b", "CATCHING_UP", false, passEpoch)
		for i := 0; i < 3; i++ {
			if _, clear, err := growLockDecision(db, noVoters, passEpoch); err != nil || clear {
				t.Fatalf("tick %d would have cleared a LIVE grow's lock (err=%v)", i+1, err)
			}
		}
	})

	// (b) drift ⇒ re-converges. The op reaches a terminal state while the CLI's
	// release is lost; the very next tick must decide to clear.
	t.Run("re-converges when a release is lost", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarker(t, db, "brk-b")
		insertOp(t, db, "op-1", "brk-b", "CATCHING_UP", false, passEpoch)
		if _, clear, _ := growLockDecision(db, noVoters, passEpoch); clear {
			t.Fatal("precondition: a live grow must not be cleared")
		}
		if _, err := db.Exec(`UPDATE cluster_operations SET op_state='SERVING', terminal=1 WHERE op_id='op-1'`); err != nil {
			t.Fatal(err)
		}
		joiner, clear, err := growLockDecision(db, noVoters, passEpoch)
		if err != nil || !clear || joiner != "brk-b" {
			t.Fatalf("pass did not notice the finished grow: joiner=%q clear=%v err=%v", joiner, clear, err)
		}

		// And once the clear lands, it is a fixed point again — the pass must
		// not re-propose a DELETE against an already-empty key every 30s.
		if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key='cluster_grow_active'`); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, clear, _ := growLockDecision(db, noVoters, passEpoch); clear {
				t.Fatalf("tick %d re-proposed a clear against an already-released lock", i+1)
			}
		}
	})

	// (c) two brokers ⇒ one write. Structural: the pass is leader-only, and the
	// clear goes through raft, so a follower neither evaluates nor proposes.
	t.Run("a follower neither evaluates nor proposes", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		follower := passBrokerFollower(t, db, clk)

		for i := 0; i < 10; i++ {
			follower.reconcilers.runDue(context.Background(), clk.advance(30*time.Second))
		}
		for _, s := range follower.reconcilers.status() {
			if s.Name != "grow-lock" {
				continue
			}
			if s.Runs != 0 {
				t.Fatalf("grow-lock ran %d times on a follower — two brokers would race the same replicated clear", s.Runs)
			}
			if s.Skips == 0 {
				t.Fatal("grow-lock never even came due on a follower — it must be scheduled and gated, not unscheduled")
			}
		}
	})

	// The pass must be inert on a single-mode broker: there is no grow lock
	// without a cluster, and a nil raft node must never be dereferenced.
	t.Run("single mode is inert", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		b := passBroker(t, db, clk)
		for i := 0; i < 3; i++ {
			if err := runPass(t, b, "grow-lock", clk.Now()); err != nil {
				t.Fatalf("single-mode grow-lock returned %v", err)
			}
		}
	})
}

// TestGrowLockClearIsValueBound pins that the clear the pass proposes is the
// SAME joiner-bound plan the CLI's release-lock trigger uses. An unconditional
// DELETE would let a stale reaper wipe a DIFFERENT joiner's live marker — the
// exact failure external review M1 hardened the CLI path against.
func TestGrowLockClearIsValueBound(t *testing.T) {
	cmd, err := cluster.PlanClearGrowActive("brk-b")
	if err != nil {
		t.Fatal(err)
	}
	// R7b: the clear is now TWO statements in one FSM transaction — the value-bound marker DELETE, then
	// the lease DELETE guarded on the marker having actually gone (see leaseClearStmt).
	if len(cmd.Body) != 2 {
		t.Fatalf("expected the marker clear + its guarded lease clear, got %d statements", len(cmd.Body))
	}
	stmt := cmd.Body[0].SQL
	if !strings.Contains(stmt, "brk-b") {
		t.Fatalf("the reaper's clear must be bound to the joiner value (an unconditional DELETE could wipe a DIFFERENT grow's live marker); rendered: %s", stmt)
	}

	// And a real fixture proves the binding actually holds: a marker owned by
	// brk-c must survive a clear aimed at brk-b.
	db := passTestDB(t)
	setGrowMarker(t, db, "brk-c")
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	if got := growActiveJoiner(db); got != "brk-c" {
		t.Fatalf("a clear for brk-b wiped brk-c's marker (now %q) — the serialization mutex is not value-bound", got)
	}
}

// --------------------------------------------------------------------------
// 2f. pass: xfer-orphan-reap (#58/P10)
// --------------------------------------------------------------------------

// TestPassXferOrphanReapIdempotence covers the shapes reachable without a live
// JetStream; the JetStream-backed behavior is exercised in
// TestXferOrphanReapPeriodicSafety.
func TestPassXferOrphanReapIdempotence(t *testing.T) {
	// (a) Without JetStream the pass must be a total no-op on every tick — this
	// is the default single-mode dev/test shape, and a pass that errored here
	// would put itself into permanent backoff for no reason.
	t.Run("no JetStream is a fixed point", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		b := passBroker(t, db, clk)
		for i := 0; i < 3; i++ {
			if err := runPass(t, b, "xfer-orphan-reap", clk.Now()); err != nil {
				t.Fatalf("tick %d: %v", i+1, err)
			}
		}
	})

	// (c) #58/P10 FIX: the reaper is PER-BROKER, not leader-only. A session homed to a
	// NON-LEADER broker must be reapable by that broker — the leader fails
	// homeOwnsXferBucket for it, so a leader-only reaper never collects it and its tier-B
	// garbage is immortal (the exact #58 leak). So a follower's registry MUST invoke the
	// pass. Safety no longer comes from leader-exclusivity but from two gates the pass
	// applies internally: reaperCaughtUp (a not-caught-up broker reaps nothing) and
	// homeOwnsXferBucket (a broker only ever touches buckets whose session is entirely
	// homed to itself — home is a partition, so no two brokers touch the same bucket; the
	// data-loss protection is proven in TestXferOrphanReapHomePartition).
	t.Run("the reaper is per-broker, not leader-only (#58/P10)", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		follower := passBrokerFollower(t, db, clk)

		for i := 0; i < 4; i++ {
			follower.reconcilers.runDue(context.Background(), clk.advance(5*time.Minute))
		}
		ran := false
		for _, s := range follower.reconcilers.status() {
			if s.Name == "xfer-orphan-reap" {
				if s.Authority == authorityLeader.String() {
					t.Fatal("#58/P10 regression: xfer-orphan-reap is leader-gated again — a session homed to a follower would never be reaped, so its tier-B objects leak forever")
				}
				ran = s.Runs > 0
			}
		}
		if !ran {
			t.Fatal("#58/P10 regression: the reaper never ran on a follower — a non-leader home would leak tier-B objects forever")
		}
		// Safety layer 1 (catch-up): a cluster-mode broker with no wired raft node is NOT
		// caught up and must delete nothing — its empty/stale view would misclassify live
		// cluster-wide objects as orphan.
		if (&Broker{cl: &clusterRuntime{}}).reaperCaughtUp() {
			t.Fatal("reaperCaughtUp must be false on a cluster broker with no raft node (not caught up)")
		}
	})

	// The #58 regression itself, stated as a property: the reaper is REGISTERED,
	// so it can run again after boot. Before R7 its only call site was inside
	// Run's JetStream probe, behind a gate that is false at that moment on every
	// cluster-mode broker — one skipped call and never another.
	t.Run("the reaper is periodic, not boot-only", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		b := passBroker(t, db, clk)
		found := false
		for _, s := range b.reconcilers.status() {
			if s.Name == "xfer-orphan-reap" {
				found = true
				if s.Interval <= 0 {
					t.Fatal("orphan reaper registered without a cadence")
				}
			}
		}
		if !found {
			t.Fatal("#58/P10 regression: the orphan xfer reaper is not registered as a periodic pass, so it can only ever run at boot")
		}
	})
}

// TestXferOrphanReapLiveTransferGuard is the safety argument for periodizing a
// DELETE, asserted rather than merely reasoned.
//
// The reaper skips any bucket named in transfers.activeOBJStreams(). The tracker
// entry — with its bucket already populated — is inserted BEFORE the prepare is
// forwarded to the agent, i.e. before any object can exist. So a live transfer's
// bucket is excluded for the whole of its life. This test pins that exclusion,
// because if it ever regresses, R7 converts a harmless boot-time sweep into a
// process that deletes in-flight tier-B payloads every five minutes.
func TestXferOrphanReapLiveTransferGuard(t *testing.T) {
	tr := newTransferTracker()
	if code := tr.put(&transferEntry{transferID: "t1", sid: "lab", bucket: "xfer-lab"}); code != "" {
		t.Fatalf("put: %s", code)
	}
	active := tr.activeOBJStreams()
	if _, ok := active["OBJ_xfer-lab"]; !ok {
		t.Fatalf("a live transfer's bucket must appear in the reaper's exclusion set, got %v", active)
	}

	// And once it finishes, the bucket stops being protected — otherwise the
	// reaper could never collect anything and #58 would be "fixed" into a no-op.
	tr.remove("t1")
	if _, ok := tr.activeOBJStreams()["OBJ_xfer-lab"]; ok {
		t.Fatal("a completed transfer must leave the exclusion set, or the reaper can never collect")
	}
}

// TestXferOrphanReapPeriodicSafety is the R-a risk test, run against a REAL
// JetStream: it proves that turning a boot-only DELETE into a five-minute loop
// collects garbage without ever touching live work.
//
// This is the single most dangerous change in R7a. A boot-only reaper that
// wrongly deletes costs one restart's worth of data; the same reaper on a timer
// costs it 288 times a day. So the test asserts both directions on the same
// bucket, across consecutive ticks:
//
//	orphan object      ⇒ collected on the first tick, and the state is then a
//	                     fixed point (ticks 2 and 3 change nothing);
//	in-flight object   ⇒ survives every tick for as long as the tracker holds it.
func TestXferOrphanReapPeriodicSafety(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	clk := newFakeClock(passEpoch)
	db := passTestDB(t)
	b := passBroker(t, db, clk)
	b.js = js
	b.nc.Store(nc)

	ctx := context.Background()
	mkBucket := func(sid string) jetstream.ObjectStore {
		t.Helper()
		os, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-" + sid})
		if err != nil {
			t.Fatalf("create bucket %s: %v", sid, err)
		}
		return os
	}
	put := func(os jetstream.ObjectStore, name string) {
		t.Helper()
		if _, err := os.PutBytes(ctx, name, []byte("payload")); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	names := func(os jetstream.ObjectStore) []string {
		t.Helper()
		objs, err := os.List(ctx)
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil
		}
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		var out []string
		for _, o := range objs {
			if !o.Deleted {
				out = append(out, o.Name)
			}
		}
		sort.Strings(out)
		return out
	}

	orphanStore := mkBucket("orphan-sess")
	put(orphanStore, "leftover-1")
	put(orphanStore, "leftover-2")

	liveStore := mkBucket("live-sess")
	put(liveStore, "in-flight-1")
	// Register the transfer EXACTLY as the push/pull prepare handlers do: the
	// entry carries its bucket and is in the tracker for the whole transfer.
	if code := b.transfers.put(&transferEntry{transferID: "t-live", sid: "live-sess", bucket: "xfer-live-sess"}); code != "" {
		t.Fatalf("tracker put: %s", code)
	}

	// --- tick 1: collect the orphans, spare the live transfer ---
	if err := runPass(t, b, "xfer-orphan-reap", clk.advance(5*time.Minute)); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if got := names(orphanStore); len(got) != 0 {
		t.Fatalf("tick 1 left orphan objects behind: %v (this is the #58/P10 leak)", got)
	}
	if got := names(liveStore); len(got) != 1 || got[0] != "in-flight-1" {
		t.Fatalf("tick 1 DELETED AN IN-FLIGHT TRANSFER'S OBJECT: %v — periodizing the reaper is unsafe", got)
	}

	// --- ticks 2 and 3: converged state is a fixed point ---
	for tick := 2; tick <= 3; tick++ {
		if err := runPass(t, b, "xfer-orphan-reap", clk.advance(5*time.Minute)); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		if got := names(orphanStore); len(got) != 0 {
			t.Fatalf("tick %d: converged bucket changed: %v", tick, got)
		}
		if got := names(liveStore); len(got) != 1 {
			t.Fatalf("tick %d deleted the in-flight object: %v", tick, got)
		}
	}

	// --- drift: the transfer finishes and its object becomes collectable ---
	b.transfers.remove("t-live")
	if err := runPass(t, b, "xfer-orphan-reap", clk.advance(5*time.Minute)); err != nil {
		t.Fatalf("post-completion tick: %v", err)
	}
	if got := names(liveStore); len(got) != 0 {
		t.Fatalf("the reaper never collects once the transfer completes: %v — #58 would be 'fixed' into a permanent no-op", got)
	}
}

// putXferOrphan creates the OBJ_xfer-<bucket> object store and writes one orphan object into it (no
// tracker entry), so the reap pass sees a bucket with accumulating garbage.
func putXferOrphan(t *testing.T, ctx context.Context, js jetstream.JetStream, bucket, name string) {
	t.Helper()
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("create object store %s: %v", bucket, err)
	}
	if _, err := store.PutBytes(ctx, name, []byte("orphan")); err != nil {
		t.Fatalf("put %s/%s: %v", bucket, name, err)
	}
}

// TestXferUnreapableBucketCounter (external review N-6) proves the reap pass counts ONLY the buckets no
// broker can ever reap that actually hold aged garbage: a split-home session and a zero-node session
// count; a single-home-elsewhere session (a peer reaps it — the noise guard) and a split-home but EMPTY
// bucket (no disk risk — the aged guard) do NOT. It is a gauge (Store, not Add) and changes no reaping.
// TestXferCrossHomeReapAgeDerivation pins the #58 Lane C cross-home GC age floor to 3× the tier-B timeout
// so the safety margin (a transfer still live on another home terminates within one tier-B timeout) cannot
// silently drift when a future tier edits the timeout.
func TestXferCrossHomeReapAgeDerivation(t *testing.T) {
	if xferCrossHomeReapAge != 3*transferTimeoutTierB {
		t.Fatalf("xferCrossHomeReapAge must stay 3×transferTimeoutTierB (got %s, want 3×%s) — the cross-home GC "+
			"floor's cross-node/clock-skew margin depends on this relation", xferCrossHomeReapAge, transferTimeoutTierB)
	}
	// minor-7: the cross-home floor must ALWAYS exceed the per-home grace — a future tier edit that inverts
	// this would make cross-home reap MORE aggressive than home reap (tearing out a peer-home's live object).
	if xferCrossHomeReapAge <= xferReapMinObjectAge {
		t.Fatalf("xferCrossHomeReapAge (%s) must exceed the per-home grace xferReapMinObjectAge (%s)",
			xferCrossHomeReapAge, xferReapMinObjectAge)
	}
}

// TestXferCrossHomeGCSkipsBusyBucket pins M6: the leader must NEVER cross-home GC a split-home bucket for
// which it holds a LIVE tracker entry (its own in-flight transfer), even under a 1ns floor.
func TestXferCrossHomeGCSkipsBusyBucket(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := &Broker{transfers: newTransferTracker(), selfID: "node-A"}
	b.cfg = Config{DB: db, Logger: silentLogger(), Now: time.Now, XferCrossHomeReapAge: time.Nanosecond}
	b.js = js
	b.nc.Store(nc)
	b.xferReapMinAge = 0

	// split-home session (node-A + node-B), leader holds a LIVE tracker entry for its bucket.
	seedHomedNode(t, db, "split", "sp1", "srv-A", "node-A")
	seedHomedNode(t, db, "split", "sp2", "srv-B", "node-B")
	putXferOrphan(t, ctx, js, "xfer-split", "obj")
	if code := b.transfers.put(&transferEntry{transferID: "obj", sid: "split", nid: "sp1", verb: "pull", tier: "b", bucket: "xfer-split", startedAt: time.Now()}); code != "" {
		t.Fatalf("put live: %s", code)
	}

	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects: %v", err)
	}
	store, err := js.ObjectStore(ctx, "xfer-split")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	if _, err := store.GetInfo(ctx, "obj"); err != nil {
		t.Fatalf("the leader must NOT cross-home GC a bucket it holds a LIVE tracker entry for (M6): %v", err)
	}
}

// TestXferCrossHomeGCReapsSplitHome is the #58 Lane C fix pin: the caught-up LEADER reaps the AGED orphan
// objects of a split-home / zero-node bucket no single home can reap, and the unreapable gauge falls to 0.
// A single-home-ELSEWHERE bucket is left for its own home to reap (not orphaned-everywhere).
func TestXferCrossHomeGCReapsSplitHome(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := &Broker{transfers: newTransferTracker(), selfID: "node-A"}
	b.cfg = Config{DB: db, Logger: silentLogger(), Now: time.Now, XferCrossHomeReapAge: time.Nanosecond}
	b.js = js
	b.nc.Store(nc)
	b.xferReapMinAge = 0 // any non-deleted object is aged garbage for the gauge

	// split-home (node-A + node-B) orphan → the LEADER cross-home GCs it.
	seedHomedNode(t, db, "split", "sp1", "srv-A", "node-A")
	seedHomedNode(t, db, "split", "sp2", "srv-B", "node-B")
	putXferOrphan(t, ctx, js, "xfer-split", "obj")
	// zero-node session orphan → cross-home GC'd.
	putXferOrphan(t, ctx, js, "xfer-zero", "obj")
	// single-home ELSEWHERE (node-B) orphan → NOT orphaned-everywhere → left for node-B, NOT GC'd here.
	seedHomedNode(t, db, "elsewhere", "e1", "srv-B", "node-B")
	putXferOrphan(t, ctx, js, "xfer-elsewhere", "obj")

	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects: %v", err)
	}

	xferObjCount := func(bucket string) int {
		store, err := js.ObjectStore(ctx, bucket)
		if err != nil {
			return -1
		}
		objs, err := store.List(ctx)
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return 0
		}
		if err != nil {
			t.Fatalf("list %s: %v", bucket, err)
		}
		n := 0
		for _, o := range objs {
			if !o.Deleted {
				n++
			}
		}
		return n
	}
	if got := xferObjCount("xfer-split"); got != 0 {
		t.Fatalf("split-home bucket must be cross-home GC'd by the leader, still has %d objects", got)
	}
	if got := xferObjCount("xfer-zero"); got != 0 {
		t.Fatalf("zero-node bucket must be cross-home GC'd by the leader, still has %d objects", got)
	}
	if got := xferObjCount("xfer-elsewhere"); got != 1 {
		t.Fatalf("a single-home-elsewhere bucket must be LEFT for its own home (node-B), got %d objects (must not cross-home GC)", got)
	}
	if got := b.xferUnreapableBuckets.Load(); got != 0 {
		t.Fatalf("after the leader cross-home GC the unreapable gauge must fall to 0 (the #58-fixed signal), got %d", got)
	}
}

func TestXferUnreapableBucketCounter(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := &Broker{transfers: newTransferTracker(), selfID: "node-A"}
	b.cfg = Config{DB: db, Logger: silentLogger(), Now: time.Now}
	b.js = js
	b.nc.Store(nc)
	b.xferReapMinAge = 0 // any non-deleted object is "aged garbage" for this test

	// split-home (node-A + node-B) with an orphan → COUNTS.
	seedHomedNode(t, db, "split", "sp1", "srv-A", "node-A")
	seedHomedNode(t, db, "split", "sp2", "srv-B", "node-B")
	putXferOrphan(t, ctx, js, "xfer-split", "obj")
	// single-home elsewhere with an orphan → NOT counted (node-B reaps it — noise guard).
	seedHomedNode(t, db, "elsewhere", "e1", "srv-B", "node-B")
	putXferOrphan(t, ctx, js, "xfer-elsewhere", "obj")
	// zero-node session bucket with an orphan → COUNTS.
	putXferOrphan(t, ctx, js, "xfer-zero", "obj")
	// split-home but EMPTY bucket → NOT counted (no aged garbage — aged guard).
	seedHomedNode(t, db, "splitempty", "se1", "srv-A", "node-A")
	seedHomedNode(t, db, "splitempty", "se2", "srv-B", "node-B")
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-splitempty"}); err != nil {
		t.Fatalf("create empty store: %v", err)
	}

	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects: %v", err)
	}
	if got := b.xferUnreapableBuckets.Load(); got != 2 {
		t.Fatalf("unreapable buckets = %d, want 2 (split + zero; elsewhere and split-empty must NOT count)", got)
	}

	// A SECOND pass must re-publish the SAME gauge, not accumulate — it is a gauge (Store), not a
	// counter (Add). Mutation Store→Add would give 4 here and this reds.
	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects (2nd pass): %v", err)
	}
	if got := b.xferUnreapableBuckets.Load(); got != 2 {
		t.Fatalf("2nd pass unreapable buckets = %d, want still 2 (gauge Store, not counter Add)", got)
	}

	// HEAL the split-home session: drop its node-B binding so it becomes single-home to self (node-A).
	// homeOwnsXferBucket now owns it → it reaps (no longer orphaned-everywhere), so the gauge must FALL to
	// 1 (only the zero-node bucket remains). A gauge that never dropped (or Add) would fail here.
	if _, err := db.Exec(`DELETE FROM nodes WHERE sid='split' AND nid='sp2'`); err != nil {
		t.Fatalf("heal split session: %v", err)
	}
	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects (post-heal): %v", err)
	}
	if got := b.xferUnreapableBuckets.Load(); got != 1 {
		t.Fatalf("post-heal unreapable buckets = %d, want 1 (split healed to single-home → reapable; only zero-node remains)", got)
	}
}

// TestXferUnreapableBucketSkipsFreshObjects (external review N-6) proves the aged-object guard: a
// split-home bucket whose only object is FRESH (within the grace) is NOT counted — the gauge tracks
// accumulating garbage, not transient in-flight objects.
func TestXferUnreapableBucketSkipsFreshObjects(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := &Broker{transfers: newTransferTracker(), selfID: "node-A"}
	b.cfg = Config{DB: db, Logger: silentLogger(), Now: time.Now}
	b.js = js
	b.nc.Store(nc)
	b.xferReapMinAge = time.Hour // shield fresh objects (real wall-clock ModTime)

	seedHomedNode(t, db, "split", "sp1", "srv-A", "node-A")
	seedHomedNode(t, db, "split", "sp2", "srv-B", "node-B")
	putXferOrphan(t, ctx, js, "xfer-split", "just-uploaded") // written now → inside the 1h grace

	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects: %v", err)
	}
	if got := b.xferUnreapableBuckets.Load(); got != 0 {
		t.Fatalf("a fresh-only split-home bucket must NOT count as unreapable garbage, got %d", got)
	}
}

// TestAdminEventsTailTruncatesAtScanCap (external review N-5) proves adminEventsTail reports TRUNCATED
// when the eventsMaxScan cap is hit before a natural stop: a --kind that matches NONE of the newest
// eventsMaxScan messages exhausts the scan budget and returns a partial (empty) tail with truncated=true
// — never a silent "(no events)". The control (a --kind that matches immediately, satisfying n) is a
// COMPLETE stop → truncated=false. Mutation: drop `truncated=true` on the scan-cap break → this reds.
func TestAdminEventsTailTruncatesAtScanCap(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()
	if err := jsstream.EnsureEventsStream(ctx, js, 1); err != nil {
		t.Fatalf("ensure events stream: %v", err)
	}

	b := &Broker{}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js
	b.nc.Store(nc)

	total := eventsMaxScan + 10
	body := []byte(`{"type":"noise"}`)
	for i := 0; i < total; i++ {
		if err := nc.Publish(proto.SubjSysEvents, body); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	stream, err := js.Stream(ctx, jsstream.EventsStreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		info, ierr := stream.Info(ctx)
		if ierr != nil {
			t.Fatalf("info: %v", ierr)
		}
		if info.State.Msgs >= uint64(total) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("events stream never captured %d msgs (got %d)", total, info.State.Msgs)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A --kind matching NONE forces the walk to the eventsMaxScan cap → truncated.
	entries, truncated, err := b.adminEventsTail(ctx, 1, 0, "rare")
	if err != nil {
		t.Fatalf("adminEventsTail(rare): %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 matches for kind=rare, got %d", len(entries))
	}
	if !truncated {
		t.Fatal("hitting the eventsMaxScan cap with no match must report truncated=true (N-5)")
	}

	// Control: a --kind matching the newest message satisfies n immediately → COMPLETE, not truncated.
	entries, truncated, err = b.adminEventsTail(ctx, 1, 0, "noise")
	if err != nil {
		t.Fatalf("adminEventsTail(noise): %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 match for kind=noise n=1, got %d", len(entries))
	}
	if truncated {
		t.Fatal("collecting the requested n matches is a COMPLETE stop — truncated must be false")
	}
}

// TestXferReapGraceIsWiredInProduction (external review M-3, wiring pin) proves broker.New actually
// wires the fresh-object shield to a POSITIVE grace. TestXferReapShieldsFreshObjects (below) hand-sets
// the value on a zero-value Broker, so it can NOT catch a regression that drops the wiring in New — a
// zero grace disables the shield and an in-flight object becomes reapable mid-upload. The >0 assert is
// load-bearing and MUST come first: it goes red under BOTH mutations — dropping `xferReapMinAge:
// xferReapMinObjectAge` from New (zero value) AND zeroing the const xferReapMinObjectAge. An
// equality-only assert would stay GREEN under the zeroed const (both sides 0). New does not dial NATS
// (it validates + sets defaults + one single-mode proxy-gen DB write), so a garbage URL suffices.
func TestXferReapGraceIsWiredInProduction(t *testing.T) {
	db := openDBWithSession(t, "lab")
	b, err := New(Config{NATSURL: "nats://127.0.0.1:1", DB: db, Logger: silentLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.xferReapMinAge <= 0 {
		t.Fatalf("broker.New must wire a POSITIVE xferReapMinAge (the M-3 fresh-object shield); got %v", b.xferReapMinAge)
	}
	if b.xferReapMinAge != xferReapMinObjectAge {
		t.Fatalf("broker.New wired xferReapMinAge=%v, want the M-3 const xferReapMinObjectAge=%v", b.xferReapMinAge, xferReapMinObjectAge)
	}
}

// TestXferReapShieldsFreshObjects (external review M-3) proves the ModTime grace: with a positive
// xferReapMinAge a freshly-written object is NOT reaped even though the tracker has NO in-flight entry
// for it (the snapshot-staleness / mid-transfer-rehome race that could otherwise tear out an object that
// is mid-upload), and IS reaped once the grace is disabled. Production sets the grace in broker.New; a
// zero-value broker leaves it 0, which is why the reap-logic tests above keep their prompt-reap semantics.
func TestXferReapShieldsFreshObjects(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()

	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{DB: passTestDB(t), Logger: silentLogger(), Now: time.Now}
	b.js = js
	b.nc.Store(nc)
	b.xferReapMinAge = time.Hour // production shields fresh in-flight objects

	os, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-fresh-sess"})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := os.PutBytes(ctx, "just-uploaded", []byte("payload")); err != nil {
		t.Fatalf("put: %v", err)
	}
	liveCount := func() int {
		t.Helper()
		objs, lerr := os.List(ctx)
		if errors.Is(lerr, jetstream.ErrNoObjectsFound) {
			return 0
		}
		if lerr != nil {
			t.Fatalf("list: %v", lerr)
		}
		n := 0
		for _, o := range objs {
			if !o.Deleted {
				n++
			}
		}
		return n
	}

	// No tracker entry ⇒ the bucket LOOKS orphan — but the object is FRESH, so the grace must spare it.
	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reap (shielded): %v", err)
	}
	if n := liveCount(); n != 1 {
		t.Fatalf("a fresh object was reaped despite the M-3 grace: %d live objects", n)
	}

	// Disable the grace (a zero-value/legacy broker, or a genuinely aged object) ⇒ the orphan IS reaped.
	b.xferReapMinAge = 0
	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reap (grace off): %v", err)
	}
	if n := liveCount(); n != 0 {
		t.Fatalf("with the grace disabled the orphan must be reaped; %d live objects remain", n)
	}
}

// TestXferReapAfterRestartPreservesLedgerBackedLiveObject exercises the production deletion path,
// not just the timeout constants.
//
// origin: batch-c external review C12. A broker restart rebuilds transfers EMPTY but preserves the
// xfer-inflight ledger. That ledger is the only durable proof that an object is still covered by the
// size-derived watchdog. The home reaper must consult it (or provide an equivalent durable guard)
// before deleting an object older than the short orphan grace.
func TestXferReapAfterRestartPreservesLedgerBackedLiveObject(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx := context.Background()
	db := passTestDB(t)
	now := time.Now().UTC()
	b := &Broker{transfers: newTransferTracker(), selfID: "node-A"}
	b.cfg = Config{
		DB:             db,
		Logger:         silentLogger(),
		Now:            func() time.Time { return now.Add(xferReapMinObjectAge + time.Minute) },
		ClusterDataDir: t.TempDir(),
	}
	b.js = js
	b.nc.Store(nc)
	b.xferReapMinAge = xferReapMinObjectAge
	seedHomedNode(t, db, "restart-live", "agent-1", "srv-A", "node-A")

	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-restart-live"})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	payload := make([]byte, 64*1024*1024)
	if _, err := store.PutBytes(ctx, "tid-restart-live", payload); err != nil {
		t.Fatalf("put live object: %v", err)
	}

	// Persist exactly the record the prepare path writes, then model the restarted process by leaving
	// its new in-memory tracker empty. At +3m this transfer is still within its >=5m budget.
	b.writeXferInflight(&transferEntry{
		transferID: "tid-restart-live",
		sid:        "restart-live",
		nid:        "agent-1",
		verb:       "push",
		tier:       "b",
		bucket:     "xfer-restart-live",
		path:       "/dst",
		size:       int64(len(payload)),
		startedAt:  now,
	})
	if b.transfers.get("tid-restart-live") != nil {
		t.Fatal("restart fixture is invalid: the new process tracker must be empty")
	}

	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatalf("reconcileXferObjects: %v", err)
	}
	if _, err := store.GetInfo(ctx, "tid-restart-live"); err != nil {
		t.Fatalf("home reaper deleted a ledger-backed live transfer only %s after restart: %v; its "+
			"size-derived watchdog budget is %s", xferReapMinObjectAge+time.Minute, err,
			proto.XferBudget("b", int64(len(payload)), proto.XferPushLegs))
	}
}

// TestXferOrphanReapHomePartition (#58/P10) is the NEW safety argument that REPLACES
// leader-exclusivity: a broker reaps ONLY buckets whose session is entirely homed to itself.
// This is what makes "every caught-up broker runs the reaper" safe — home is a partition, so
// no two brokers ever touch the same bucket. Two homed sessions with orphan objects in both;
// the node-A broker collects its own (sess-A) and leaves node-B's bucket (sess-B) untouched.
//
// It is also the data-loss guard: reverting the homeOwnsXferBucket gate would let this broker
// wipe node-B's objects (a follower's live in-flight transfer that node-A's empty tracker
// cannot see). The complementary #58 claim — that a NON-LEADER home may reap at all (the
// reaperMayDelete→reaperCaughtUp change) — needs a real non-leader raft node and is verified
// at the deploy tier (drill 96); here selfID is set with b.cl nil, so reaperCaughtUp is true.
func TestXferOrphanReapHomePartition(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	clk := newFakeClock(passEpoch)
	db := passTestDB(t)
	b := passBroker(t, db, clk)
	b.selfID = "node-A" // non-empty ⇒ homeOwnsXferBucket takes its REAL query path
	b.js = js
	b.nc.Store(nc)

	// sess-A is homed HERE (node-A); sess-B is homed to node-B (a different broker).
	seedHomedNode(t, db, "sess-A", "a-1", "srv-A", "node-A")
	seedHomedNode(t, db, "sess-B", "b-1", "srv-B", "node-B")

	ctx := context.Background()
	mkOrphan := func(sid string) jetstream.ObjectStore {
		t.Helper()
		os, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-" + sid})
		if err != nil {
			t.Fatalf("create bucket %s: %v", sid, err)
		}
		if _, err := os.PutBytes(ctx, "orphan-"+sid, []byte("payload")); err != nil {
			t.Fatalf("put %s: %v", sid, err)
		}
		return os
	}
	live := func(os jetstream.ObjectStore) int {
		t.Helper()
		objs, err := os.List(ctx)
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return 0
		}
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		n := 0
		for _, o := range objs {
			if !o.Deleted {
				n++
			}
		}
		return n
	}

	osA := mkOrphan("sess-A")
	osB := mkOrphan("sess-B")

	if err := runPass(t, b, "xfer-orphan-reap", clk.advance(5*time.Minute)); err != nil {
		t.Fatalf("reap tick: %v", err)
	}

	if n := live(osA); n != 0 {
		t.Fatalf("node-A did not reap its OWN home's orphan (sess-A): %d left — the #58/P10 leak", n)
	}
	if n := live(osB); n != 1 {
		t.Fatalf("node-A reaped sess-B, homed to node-B (%d objects survived, want 1) — home partition violated: a broker wiped another home's bucket (DATA LOSS)", n)
	}
}
