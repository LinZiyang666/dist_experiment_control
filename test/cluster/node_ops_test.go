package cluster_test

import (
	"database/sql"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/session"
)

// TestMultiFSM_SameStreamConverges is the §13.2 same-stream proof: the SAME baked
// Command applied to two independent fresh DBs yields byte-identical content
// INCLUDING token_hash (which the direct-vs-FSM differential excludes, since there
// each arm mints its own token). This proves the leader-baked op replays
// deterministically across replicas — the core multi-FSM convergence guarantee.
func TestMultiFSM_SameStreamConverges(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 7, time.UTC)
	cfg := fixedClock(now)

	a := freshDB(t)
	b := freshDB(t)
	seedSessionNode(t, a, "lab", "lab-1", now)
	seedSessionNode(t, b, "lab", "lab-1", now)

	// Plan ONCE (on a), then apply the SAME command to both replicas.
	_, cmd, err := port.PlanAllocate(a, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(a, cmd); err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(b, cmd); err != nil {
		t.Fatal(err)
	}
	// INCLUDE token_hash this time — same command must converge it too.
	if logicalHash(t, a, "port_allocations") != logicalHash(t, b, "port_allocations") {
		t.Fatal("same-stream replicas diverged (incl token_hash) — replay nondeterminism")
	}
}

// TestNode_ProposeRealRaftPath drives ops through the REAL raft path
// (Node.Propose -> raft.Apply -> fsm.Apply, under applyMu), not just ExecCommand.
// It exercises a secret-returning op (PlanAllocate) and asserts the raw token is
// captured by the closure (out of band) while the committed row carries only the
// hash — the §5 secret-return contract end to end.
func TestNode_ProposeRealRaftPath(t *testing.T) {
	n, _ := newTestNode(t)
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)

	mustPropose := func(plan func(*sql.DB) (*cluster.Command, error)) {
		t.Helper()
		if err := n.Propose(plan); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	mustPropose(func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, "lab", "lab", "SHA256:owner", "ph", now)
	})
	mustPropose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "lab-1", ProtoVersion: 2}, now)
	})

	var alloc *port.Allocation
	mustPropose(func(db *sql.DB) (*cluster.Command, error) {
		a, cmd, err := port.PlanAllocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", cfg)
		alloc = a
		return cmd, err
	})
	if alloc == nil || alloc.Token == "" {
		t.Fatal("secret-returning Plan did not surface a raw token through Propose")
	}

	// read back through the node's bounded-stale read: the row exists, hashed.
	var gotPort int
	var gotHash string
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT port, token_hash FROM port_allocations WHERE name='jupyter' AND state='ALLOCATED'`).Scan(&gotPort, &gotHash)
	}); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if gotPort != alloc.Port {
		t.Fatalf("committed port %d != planned %d", gotPort, alloc.Port)
	}
	if gotHash != alloc.TokenHash || gotHash == alloc.Token {
		t.Fatalf("committed row must carry the HASH, not the raw token (hash=%q raw=%q)", gotHash, alloc.Token)
	}

	// a duplicate name must be rejected by the leader Plan (ErrNameTaken) BEFORE
	// proposing — proving Plan-side business errors short-circuit Propose.
	err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
		_, cmd, err := port.PlanAllocate(db, "lab", "lab-1", "jupyter", 9999, 0, "SHA256:caller", cfg)
		return cmd, err
	})
	if err != port.ErrNameTaken {
		t.Fatalf("duplicate allocate must return ErrNameTaken, got %v", err)
	}
}

// TestConcurrentPropose is the Stage C T5 fix: the load-bearing applyMu /
// no-open-handle / no-deadlock concurrency claim (d2-plan §6/§8) was untested.
// K goroutines race Node.Propose. (a) Same (sid,name): exactly one wins, the rest
// get ErrNameTaken — proving applyMu serializes the Plan-read+Apply so the
// (sid,name) invariant holds and no two Plans bake the same key. (b) Same desired
// port: exactly one wins, the rest ErrPortTaken (NOT a fail-stop panic — the B1
// fix). Runs under -race with a wall-clock deadline (deadlock => fail) and a
// goroutine-count tolerance gate.
func TestConcurrentPropose(t *testing.T) {
	n, _ := newTestNode(t)
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)

	// parents
	if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, "lab", "lab", "o", "p", now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "lab-1", ProtoVersion: 2}, now)
	}); err != nil {
		t.Fatal(err)
	}

	base := runtime.NumGoroutine()

	race := func(label string, plan func() func(*sql.DB) (*cluster.Command, error), okErr func(error) bool) int {
		const K = 12
		var wg sync.WaitGroup
		errs := make([]error, K)
		done := make(chan struct{})
		for i := 0; i < K; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = n.Propose(plan())
			}(i)
		}
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatalf("%s: concurrent Propose DEADLOCKED (applyMu vs single-writer pool?)", label)
		}
		wins := 0
		for _, e := range errs {
			switch {
			case e == nil:
				wins++
			case okErr(e):
			default:
				t.Fatalf("%s: unexpected error (a fail-stop panic or wrong type?): %v", label, e)
			}
		}
		return wins
	}

	// (a) same (sid,name): exactly one winner.
	if w := race("same-name",
		func() func(*sql.DB) (*cluster.Command, error) {
			return func(db *sql.DB) (*cluster.Command, error) {
				_, cmd, err := port.PlanAllocate(db, "lab", "lab-1", "dup", 1000, 0, "fp", cfg)
				return cmd, err
			}
		},
		func(e error) bool { return e == port.ErrNameTaken }); w != 1 {
		t.Fatalf("same-name race: %d winners, want exactly 1", w)
	}

	// (b) same desired port: exactly one winner, rest ErrPortTaken (B1, no panic).
	var dport int
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT port FROM port_allocations WHERE name='dup'`).Scan(&dport)
	}); err != nil {
		t.Fatal(err)
	}
	dport++ // a fresh in-band port all racers will desire
	if w := race("same-desired-port",
		func() func(*sql.DB) (*cluster.Command, error) {
			return func(db *sql.DB) (*cluster.Command, error) {
				// distinct names so the collision is on the PORT, not the name.
				_, cmd, err := port.PlanAllocate(db, "lab", "lab-1", randName(), 1000, dport, "fp", cfg)
				return cmd, err
			}
		},
		func(e error) bool { return e == port.ErrPortTaken }); w != 1 {
		t.Fatalf("same-desired-port race: %d winners, want exactly 1 (ErrPortTaken for losers, never panic)", w)
	}

	// goroutine tolerance: no leak from the Propose churn.
	for i := 0; i < 50; i++ {
		if runtime.NumGoroutine() <= base+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak after concurrent Propose: base=%d now=%d", base, runtime.NumGoroutine())
}

// randName returns a per-call unique name without math/rand (the determinism lint
// forbids it nearby) — a monotonic counter is enough for test uniqueness.
var nameCtr int
var nameMu sync.Mutex

func randName() string {
	nameMu.Lock()
	defer nameMu.Unlock()
	nameCtr++
	return "n" + itoa(nameCtr)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
