package broker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go/jetstream"
)

// reconcile_authority_test.go — the two properties B7 added to the registry: an authority resolved
// ONCE PER SWEEP, and duration/overrun observability measured on the injected clock.

// TestAuthorityIsResolvedOncePerSweep is the reason passAuthority is a value and not a per-pass
// closure.
//
// The roadmap asked for `leaderOnly bool -> authority func() bool`. With closures, N gated passes in
// one sweep would evaluate leadership N times, and leadership lost between the first and the last would
// let the first half of a tick act as leader and the second half not. One of the duties gated this way
// drives membership phase transitions, where a split view inside one tick is the no-silent-fork hazard
// §8.1 exists to prevent — so this is not a tidiness property.
//
// The old bool had the property by accident (runDue read one registry-level isLeader). This test makes
// it explicit, so a future change to closures fails here instead of in production.
func TestAuthorityIsResolvedOncePerSweep(t *testing.T) {
	var evaluations atomic.Int64
	r := newReconcileRegistry(silentLogger(), func() bool {
		evaluations.Add(1)
		return true
	})
	// FIVE gated passes, all due on the same sweep. If the answer were resolved per pass this would be
	// 5 evaluations; the contract is 1.
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		r.register(name, time.Second, authorityLeader, func(context.Context, time.Time) error { return nil })
	}
	// Plus an ungated one, to prove authorityAny costs no evaluation at all.
	r.register("ungated", time.Second, authorityAny, func(context.Context, time.Time) error { return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	ran := r.runDue(context.Background(), clk.advance(time.Second))

	if len(ran) != 6 {
		t.Fatalf("expected all 6 passes to run, got %d: %v", len(ran), ran)
	}
	if got := evaluations.Load(); got != 1 {
		t.Errorf("the leadership source was consulted %d times in ONE sweep, want exactly 1.\n"+
			"Every gated pass in a sweep must see the SAME answer: leadership lost mid-sweep would "+
			"otherwise let the first passes act as leader and the rest not, and one of them proposes "+
			"membership phase transitions.", got)
	}

	// A second sweep re-resolves it — the answer is cached for the sweep, not for the process.
	r.runDue(context.Background(), clk.advance(time.Second))
	if got := evaluations.Load(); got != 2 {
		t.Errorf("the second sweep must re-resolve authority (got %d evaluations total, want 2); caching "+
			"across sweeps would keep running leader-gated passes after leadership moved", got)
	}
}

// TestAuthorityAnyRunsWithoutALeadershipSource pins the single-mode default. The entire current fleet
// runs single-mode brokers, where there is no raft leader to ask: a nil leadership source means
// "permitted". A refactor that made a nil source mean "denied" would silently stop every leader-gated
// pass on every broker in production while every clustered test stayed green.
func TestAuthorityAnyRunsWithoutALeadershipSource(t *testing.T) {
	var leaderRuns, anyRuns int
	r := newReconcileRegistry(silentLogger(), nil) // nil == single mode
	r.register("gated", time.Second, authorityLeader, func(context.Context, time.Time) error { leaderRuns++; return nil })
	r.register("ungated", time.Second, authorityAny, func(context.Context, time.Time) error { anyRuns++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	r.runDue(context.Background(), clk.advance(time.Second))

	if anyRuns != 1 {
		t.Errorf("an authorityAny pass must always run: got %d runs", anyRuns)
	}
	if leaderRuns != 1 {
		t.Errorf("with NO leadership source (single mode) a leader-gated pass must still run: got %d runs.\n"+
			"Treating nil as \"not the leader\" disables ports, grow-lock, upgrade-lock and home-delivery "+
			"on every single-mode broker — which today is the whole fleet.", leaderRuns)
	}
}

// TestPassDurationAndOverrunAreRecorded pins the observability that replaced the timeout B7 refused to
// ship. It is measured on the INJECTED clock, so it is deterministic and adds no real sleep.
func TestPassDurationAndOverrunAreRecorded(t *testing.T) {
	clk := newFakeClock(regEpoch)
	r := newReconcileRegistry(silentLogger(), nil)
	r.now = clk.Now // the duration clock; production wires b.cfg.Now

	// "slow" advances the clock by 3× its own interval on its FIRST run only — an overrun that then
	// recovers. "quick" does not move the clock at all.
	//
	// The "first run only" part is load-bearing and was a real defect in this test: a body that
	// advances the clock EVERY time makes the second sweep measure 3s again, so `p.maxDur = dur`
	// (high-water property deleted) lands on the same 3s and the assertion at the bottom cannot tell
	// the difference. Mutation-verified in both directions — see the comment above that assertion.
	slowRuns := 0
	r.register("slow", time.Second, authorityAny, func(context.Context, time.Time) error {
		slowRuns++
		if slowRuns == 1 {
			clk.advance(3 * time.Second)
		}
		return nil
	})
	r.register("quick", time.Second, authorityAny, func(context.Context, time.Time) error { return nil })

	r.start(clk.Now())
	r.runDue(context.Background(), clk.advance(time.Second))

	byName := map[string]reconcilePassStatus{}
	for _, s := range r.status() {
		byName[s.Name] = s
	}
	slow, quick := byName["slow"], byName["quick"]

	if slow.LastDur != 3*time.Second {
		t.Errorf("slow LastDur = %v, want 3s (measured on the injected clock)", slow.LastDur)
	}
	if slow.MaxDur != 3*time.Second {
		t.Errorf("slow MaxDur = %v, want 3s", slow.MaxDur)
	}
	if slow.Overruns != 1 {
		t.Errorf("slow Overruns = %d, want 1 — it took 3s against a 1s interval, so it is no longer "+
			"keeping up with itself AND it held the sweep lock for every other pass", slow.Overruns)
	}
	if quick.Overruns != 0 {
		t.Errorf("quick Overruns = %d, want 0", quick.Overruns)
	}

	// MaxDur is a high-water mark: a later fast run must not erase the evidence of the slow one. This
	// is what makes the CLI's SLOWEST column useful after the fact.
	//
	// The second sweep RECOVERS (slowRuns == 2 does not move the clock), so LastDur drops to 0 while
	// MaxDur must stay at 3s. Asserting BOTH is what gives this arm teeth: under the mutation
	// `if dur > p.maxDur { p.maxDur = dur }` → `p.maxDur = dur` the two collapse onto each other and
	// MaxDur reads 0. Before the recovery was introduced this whole arm was VACUOUS — the pass took 3s
	// on every sweep, so max-vs-last was unobservable and the mutation ran green.
	r.runDue(context.Background(), clk.advance(time.Second))
	if slowRuns != 2 {
		t.Fatalf("the second sweep did not re-run slow (ran %d times) — without a SECOND completion "+
			"there is no later run to erase the high-water mark with, and this arm proves nothing", slowRuns)
	}
	for _, s := range r.status() {
		if s.Name != "slow" {
			continue
		}
		if s.LastDur != 0 {
			t.Errorf("slow LastDur = %v after the recovered run, want 0 — if the recovery did not "+
				"actually take zero clock time, MaxDur and LastDur are equal and the assertion below "+
				"cannot distinguish a high-water mark from a plain overwrite", s.LastDur)
		}
		if s.MaxDur != 3*time.Second {
			t.Errorf("MaxDur must be a high-water mark, got %v after a subsequent fast run — an operator "+
				"looking at a recovered pass would see no trace of the wedge, and the CLI's SLOWEST "+
				"column silently degrades into LAST", s.MaxDur)
		}
	}
}

// TestPassDurationWithoutAClockIsInertNotWrong: a registry with no duration clock (every test that does
// not care) must report zero durations and NEVER let that influence scheduling. The fake-clock
// equivalence proofs depend on this.
func TestPassDurationWithoutAClockIsInertNotWrong(t *testing.T) {
	var runs int
	r := newReconcileRegistry(silentLogger(), nil) // r.now left nil on purpose
	r.register("p", time.Second, authorityAny, func(context.Context, time.Time) error { runs++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	for i := 0; i < 5; i++ {
		r.runDue(context.Background(), clk.advance(time.Second))
	}
	if runs != 5 {
		t.Fatalf("scheduling must be unaffected by the absence of a duration clock: runs=%d, want 5", runs)
	}
	for _, s := range r.status() {
		if s.LastDur != 0 || s.MaxDur != 0 || s.Overruns != 0 {
			t.Errorf("with no duration clock the measurements must be zero, not garbage: %+v", s)
		}
	}
}

// A saved plan and each late writer must preserve a same-key replacement even
// when SID, owner, timestamp, node, PID, boot, allocation token and epoch match.
// Only the creation salt differs, so older fences cannot sign this test.
// origin: hot-update S8 plan §6.2 / §11.2 P16 / M17
func TestGuardedBrokerWritesRejectRecreatedSession(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerAuthorityWrites(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker guarded writes x5", g)
	testharness.AssertNoFDLeak(t, "broker guarded writes x5", fd, 0)
}

func brokerAuthorityPIN(salt byte) string {
	return "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{salt}, 16)) + "$" + base64.RawStdEncoding.EncodeToString(make([]byte, 32))
}

func seedBrokerAuthority(t *testing.T, db *sql.DB, salt byte) upgradeauthority.Guard {
	t.Helper()
	now := time.Date(2026, 10, 13, 1, 2, 3, 456789000, time.UTC)
	if _, err := session.Create(db, "lab", "lab", "owner'with?text", brokerAuthorityPIN(salt), now); err != nil {
		t.Fatal(err)
	}
	g, err := upgradeauthority.Capture(db, upgradeauthority.Guard{SID: "lab", InstanceID: strings.Repeat("a", 26), ConfiguredNID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"node", "previous"} {
		if err := node.Register(db, node.RegisterInput{SID: "lab", NID: nid, BootID: "replacement", ProtoVersion: proto.ProtoVersion}, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := proc.Insert(db, proc.Process{PID: "shared-pid", SID: "lab", NID: "previous", Argv: []string{"/bin/true"}, StartedAt: now, BootID: "same-boot", StartTimeTicks: 99}); err != nil {
		t.Fatal(err)
	}
	if _, err := port.Allocate(db, "lab", "node", "same-port", 8080, 14000, "owner'with?text", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE port_allocations SET token_hash='same-token' WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	return g
}

func applyBrokerAuthorityPlan(t *testing.T, db *sql.DB, cmd *cluster.Command) {
	t.Helper()
	if cmd == nil {
		t.Fatal("positive control did not render a command")
	}
	for _, statement := range cmd.Body {
		if _, err := db.Exec(statement.SQL); err != nil {
			t.Fatal(err)
		}
	}
}

func brokerAuthorityRows(t *testing.T, db *sql.DB) string {
	t.Helper()
	var result [][][]string
	for _, query := range []string{`SELECT nid,sid,boot_id,status,CAST(last_heartbeat_at AS TEXT),proxy_ready FROM nodes ORDER BY nid`, `SELECT pid,sid,nid,status,COALESCE(exit_code,''),COALESCE(CAST(ended_at AS TEXT),'') FROM processes ORDER BY pid`, `SELECT port,sid,nid,name,token_hash,state,epoch FROM port_allocations ORDER BY port`} {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		var table [][]string
		for rows.Next() {
			fields := make([]sql.NullString, len(columns))
			args := make([]any, len(columns))
			for i := range fields {
				args[i] = &fields[i]
			}
			if err := rows.Scan(args...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			var values []string
			for _, field := range fields {
				values = append(values, fmt.Sprintf("%t:%s", field.Valid, field.String))
			}
			table = append(table, values)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		result = append(result, table)
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func requireBrokerAuthorityRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), upgradetx.CodeSessionIncarnationMismatch) {
		t.Fatalf("old creation was not refused: %v", err)
	}
}

func exerciseBrokerAuthorityWrites(t *testing.T) {
	t.Helper()
	db := testharness.OpenDB(t)
	old := seedBrokerAuthority(t, db, 1)
	b := &Broker{cfg: Config{DB: db, Now: time.Now, Logger: silentLogger()}}
	req := proto.NodeRegisterReq{Incarnation: old.Incarnation, InstanceID: old.InstanceID, PreviousNID: "previous", LocalProcesses: []proto.LocalProcess{{PID: "shared-pid", State: "running", StartTimeTicks: 99}}, BootID: "same-boot"}
	in := node.RegisterInput{SID: "lab", NID: "node", BootID: "old", Authority: &old}
	saved, err := planRegisterWithRefiles(db, in, req.PreviousNID, req.LocalProcesses, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// The ordinary first register learns a real digest; image proof cannot be
	// overwritten by the result of a later SID lookup.
	learned, err := captureRegisterAuthority(b, "lab", "node", &proto.NodeRegisterReq{InstanceID: old.InstanceID})
	if err != nil || learned.Incarnation != old.Incarnation {
		t.Fatal("first register did not learn the original creation")
	}
	if err := node.Register(db, in, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	current := seedBrokerAuthority(t, db, 2)
	if current.Incarnation == old.Incarnation {
		t.Fatal("salt did not distinguish the replacement")
	}
	before := brokerAuthorityRows(t, db)
	applyBrokerAuthorityPlan(t, db, saved)
	if got := brokerAuthorityRows(t, db); got != before {
		t.Fatal("compare-then-recreate plan changed the replacement")
	}
	requireBrokerAuthorityRefusal(t, node.Register(db, in, time.Now()))
	_, err = node.PlanRegister(db, in, time.Now())
	requireBrokerAuthorityRefusal(t, err)
	_, err = captureRegisterAuthority(b, "lab", "node", &req)
	requireBrokerAuthorityRefusal(t, err)
	exit := ProcMarkExitedPayload(upgradeauthority.ExitRequest("lab", "node", req, "shared-pid", 23, time.Now()))
	requireBrokerAuthorityRefusal(t, markProcExitAuthority(b, exit))
	_, err = planProcExitAuthority(db, exit)
	requireBrokerAuthorityRefusal(t, err)
	_, err = refileReconcileAuthority(b, "lab", "node", req, "shared-pid")
	requireBrokerAuthorityRefusal(t, err)
	requireBrokerAuthorityRefusal(t, markNodeOfflineOnReleaseAuthority(b, old, "node"))
	requireBrokerAuthorityRefusal(t, heartbeatNodeAuthority(b, in, time.Now()))
	if accepted, closed, keep, revoke, drop := b.reconcileOnRegister("lab", "node", req); len(accepted)+len(closed)+len(keep)+len(revoke)+len(drop) != 0 {
		t.Fatal("stale G.1 emitted replacement-domain directives")
	}
	a, err := port.LookupByName(db, "lab", "same-port")
	if err != nil {
		t.Fatal(err)
	}
	a.Authority = old
	id := allocIdentity(*a)
	encoded, err := json.Marshal(id)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PortFreeAllocationPayload
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.allocation().Authority != old {
		t.Fatal("forward projection dropped captured authority")
	}
	requireBrokerAuthorityRefusal(t, b.freePortAllocation(*a))
	requireBrokerAuthorityRefusal(t, b.revokePortAllocation(decoded.allocation(), time.Now()))
	requireBrokerAuthorityRefusal(t, tombstoneSessionAuthority(b, old))
	if got := brokerAuthorityRows(t, db); got != before {
		t.Fatal("late writer changed replacement rows")
	}
	foreign := exit
	foreign.Sid = "different"
	foreign.Authority = &current
	requireBrokerAuthorityRefusal(t, markProcExitAuthority(b, foreign))
	_, err = planProcExitAuthority(db, foreign)
	requireBrokerAuthorityRefusal(t, err)
	wrongName := current
	wrongName.ConfiguredNID = "different"
	in.Authority = &wrongName
	requireBrokerAuthorityRefusal(t, node.Register(db, in, time.Now()))
	// Positive controls prevent a missing writer from masquerading as refusal.
	in.Authority = &current
	if err := node.Register(db, in, time.Now()); err != nil {
		t.Fatal(err)
	}
	exit.Authority = &current
	if err := markProcExitAuthority(b, exit); err != nil {
		t.Fatal(err)
	}
	p, err := proc.GetInSession(db, "lab", "shared-pid")
	if err != nil || p.Status != proc.StateExited || p.ExitCode == nil || *p.ExitCode != 23 {
		t.Fatal("current guarded exit did not commit")
	}
	_, nc := newXferTestJS(t)
	subject := proto.SubjEvProc("lab", "node", "shared-pid", "exit")
	sub, err := nc.Subscribe(subject, b.handleProcEvent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"exit_code": 23, "ended_at": time.Now(), "incarnation": old.Incarnation, "instance_id": old.InstanceID})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := nc.Request(subject, data, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ack proto.ProcEventAck
	if err := json.Unmarshal(msg.Data, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.OK || ack.Code != upgradetx.CodeSessionIncarnationMismatch {
		t.Fatal("old exit borrowed the replacement's terminal ACK")
	}

	a.Authority = current
	if err := b.freePortAllocation(*a); err != nil {
		t.Fatal(err)
	}
	var allocationState string
	err = db.QueryRow(`SELECT state FROM port_allocations WHERE sid='lab' AND name='same-port'`).Scan(&allocationState)
	if err != nil || allocationState != string(port.StateFreed) {
		t.Fatal("current guarded free did not commit")
	}
}

// Forward decoding drives the real leader planner, not a fake proposal callback.
// origin: hot-update S8 plan §6.2 / §11.2 P16
func TestGuardedForwardPlannerRejectsRecreatedSession(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseForwardAuthorityPlanner(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "forward authority planner x5", g)
	testharness.AssertNoFDLeak(t, "forward authority planner x5", fd, 0)
}

func exerciseForwardAuthorityPlanner(t *testing.T) {
	t.Helper()
	leader, _ := d7SingleNode(t, "guarded-forward")
	now := time.Now().UTC()
	var original, current upgradeauthority.Guard
	create := func(salt byte) {
		t.Helper()
		if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return session.PlanCreate(db, "lab", "lab", "owner", brokerAuthorityPIN(salt), now)
		}); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
			g, err := upgradeauthority.Capture(db, upgradeauthority.Guard{SID: "lab", InstanceID: strings.Repeat("a", 26), ConfiguredNID: "node"})
			if err != nil {
				return nil, err
			}
			current = g
			return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "node", BootID: "replacement", Authority: &g}, now)
		}); err != nil {
			t.Fatal(err)
		}
		if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return proc.PlanInsert(db, proc.Process{PID: "shared-pid", SID: "lab", NID: "node", Argv: []string{"/bin/true"}, StartedAt: now})
		}); err != nil {
			t.Fatal(err)
		}
	}
	dispatch := func(verb string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return dispatchForward(leader, func() time.Time { return now }, forwardDeps{}, forwardEnvelope{Verb: verb, Payload: data})
	}
	create(1)
	original = current
	if err := dispatch(VerbSessionTombstone, SessionMutatePayload{SID: "lab", Authority: &original}); err != nil {
		t.Fatal(err)
	}
	if err := dispatch(VerbSessionDrop, SessionMutatePayload{SID: "lab", Authority: &original}); err != nil {
		t.Fatal(err)
	}
	create(2)
	before := brokerAuthorityRows(t, leader.RODB())
	for _, call := range []struct {
		verb  string
		value any
	}{
		{VerbNodeRegister, node.RegisterInput{SID: "lab", NID: "node", BootID: "old", Authority: &original}},
		{VerbProcMarkExited, ProcMarkExitedPayload{Pid: "shared-pid", Sid: "lab", ExitCode: 23, EndedAt: now, Authority: &original}},
		{VerbReconcile, ReconcilePayload{SID: "lab", NID: "node", Req: proto.NodeRegisterReq{Incarnation: original.Incarnation, InstanceID: original.InstanceID}}},
		{VerbSessionTombstone, SessionMutatePayload{SID: "lab", Authority: &original}},
		{VerbSessionDrop, SessionMutatePayload{SID: "lab", Authority: &original}},
	} {
		requireBrokerAuthorityRefusal(t, dispatch(call.verb, call.value))
	}
	if brokerAuthorityRows(t, leader.RODB()) != before {
		t.Fatal("forwarded late writer changed replacement rows")
	}
	if err := dispatch(VerbProcMarkExited, ProcMarkExitedPayload{Pid: "shared-pid", Sid: "lab", ExitCode: 23, EndedAt: now, Authority: &current}); err != nil {
		t.Fatal(err)
	}
	p, err := proc.GetInSession(leader.RODB(), "lab", "shared-pid")
	if err != nil || p.Status != proc.StateExited {
		t.Fatal("forwarded current writer did not commit")
	}
	req := proto.NodeRegisterReq{BootID: "same", Incarnation: original.Incarnation, InstanceID: original.InstanceID}
	first := ReconcileReqID("lab", "node", req)
	req.Incarnation = current.Incarnation
	if first == ReconcileReqID("lab", "node", req) {
		t.Fatal("recreated authority reused old reconcile deduplication identity")
	}
	if err := dispatch(VerbSessionTombstone, SessionMutatePayload{SID: "lab", Authority: &current}); err != nil {
		t.Fatal(err)
	}
	deleting := brokerAuthorityRows(t, leader.RODB())
	requireBrokerAuthorityRefusal(t, dispatch(VerbSessionDrop, SessionMutatePayload{SID: "lab"}))
	requireBrokerAuthorityRefusal(t, dispatch(VerbSessionDrop, SessionMutatePayload{SID: "lab", Authority: &original}))
	if brokerAuthorityRows(t, leader.RODB()) != deleting {
		t.Fatal("late forwarded cleanup erased a DELETING replacement")
	}
	if err := dispatch(VerbSessionDrop, SessionMutatePayload{SID: "lab", Authority: &current}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Get(leader.RODB(), "lab"); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("current forwarded cleanup did not commit")
	}

}

// Pause the real external delete after the write reservation. A second pool's
// immediate DELETE must be busy, and a later A callback cannot touch B's stores.
// origin: hot-update S8 plan §6.2 / §8.3 M17 / P16
func TestGuardedSessionCascadeKeepsReplacementDomain(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseGuardedSessionCascade(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "guarded external cascade x5", g)
	testharness.AssertNoFDLeak(t, "guarded external cascade x5", fd, 0)
}

type authorityCascadeBarrier struct {
	jetstream.JetStream
	entered, release chan struct{}
}

func (j *authorityCascadeBarrier) DeleteStream(ctx context.Context, name string) error {
	if name == "history-lab" {
		close(j.entered)
		select {
		case <-j.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return j.JetStream.DeleteStream(ctx, name)
}

func exerciseGuardedSessionCascade(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authority.db")
	db, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	testharness.CloseDBOnCleanup(t, db)
	other, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	testharness.CloseDBOnCleanup(t, other)
	if _, err := other.Exec(`PRAGMA busy_timeout=0`); err != nil {
		t.Fatal(err)
	}
	old := seedBrokerAuthority(t, db, 1)
	js, _ := newXferTestJS(t)
	if err := jsstream.EnsureHistoryStream(t.Context(), js, "lab", 1); err != nil {
		t.Fatal(err)
	}
	obj, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-lab"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obj.PutBytes(t.Context(), "old-object", []byte("old")); err != nil {
		t.Fatal(err)
	}
	gate := &authorityCascadeBarrier{JetStream: js, entered: make(chan struct{}), release: make(chan struct{})}
	b := &Broker{cfg: Config{DB: db, Now: time.Now, Logger: silentLogger()}}
	setBrokerJS(b, gate)
	if err := tombstoneSessionAuthority(b, old); err != nil {
		t.Fatal(err)
	}
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	joined := false
	go func() { done <- cascadeSessionAuthority(b, d, t.Context().Done(), old) }()
	defer func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cascade did not join")
			}
		}
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real external delete was not reached")
	}
	if _, err := other.Exec(`DELETE FROM sessions WHERE sid='lab'`); err == nil || (!strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy")) {
		t.Fatalf("compare-then-recreate crossed external cleanup reservation: %v", err)
	}
	close(gate.release)
	err = <-done
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Get(db, "lab"); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("current cleanup did not delete the old session")
	}
	current := seedBrokerAuthority(t, db, 2)
	if err := tombstoneSessionAuthority(b, current); err != nil {
		t.Fatal(err)
	}
	setBrokerJS(b, js)
	if err := jsstream.EnsureHistoryStream(t.Context(), js, "lab", 1); err != nil {
		t.Fatal(err)
	}
	obj, err = js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-lab"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obj.PutBytes(t.Context(), "new-object", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	before := brokerAuthorityRows(t, db)
	requireBrokerAuthorityRefusal(t, cascadeSessionAuthority(b, d, t.Context().Done(), upgradeauthority.Guard{SID: "lab"}))
	requireBrokerAuthorityRefusal(t, cascadeSessionRm(t.Context(), b, "lab"))
	requireBrokerAuthorityRefusal(t, cascadeSessionAuthority(b, d, t.Context().Done(), old))
	if brokerAuthorityRows(t, db) != before {
		t.Fatal("late final cleanup erased replacement rows")
	}
	if _, err := js.Stream(t.Context(), "history-lab"); err != nil {
		t.Fatal("late final cleanup erased replacement history")
	}
	data, err := obj.GetBytes(t.Context(), "new-object")
	if err != nil || string(data) != "replacement" {
		t.Fatal("late final cleanup erased replacement transfer domain")
	}
}

// Saved SQL proves Apply-time fencing; this companion gate proves the first
// register comparison itself remains inside the actual SQLite transaction.
// origin: hot-update S8 plan §6.2 / §11.2 P16
func TestNodeRegisterComparesWithinWriteTransaction(t *testing.T) {
	source, err := os.ReadFile("../node/node.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "../node/node.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var begin, compare, write token.Pos
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Register" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "planRegister" {
				if len(call.Args) == 0 {
					t.Fatal("register comparison lost its reader")
				}
				reader, ok := call.Args[0].(*ast.Ident)
				if !ok || reader.Name != "tx" {
					t.Fatal("register compare moved outside the write transaction")
				}
				compare = call.Pos()
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				if receiver, ok := selector.X.(*ast.Ident); ok {
					if receiver.Name == "db" && selector.Sel.Name == "Begin" {
						begin = call.Pos()
					}
					if receiver.Name == "tx" && selector.Sel.Name == "Exec" && write == 0 {
						write = call.Pos()
					}
				}
			}
			return true
		})
	}
	if begin == 0 || compare == 0 || write == 0 || begin >= compare || compare >= write {
		t.Fatal("transaction/compare/write boundary is missing or reordered")
	}
}
