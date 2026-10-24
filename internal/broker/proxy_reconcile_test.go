package broker

// proxy_reconcile_test.go — unit coverage for the h1 E2 M3 rotation damper
// (the decision core, driven with a fake clock; the raft alert propose path
// no-ops on a bare Broker and is exercised by the cluster harnesses).
// origin: docs/reviews/h1-plan.md workstream E2 (2026-08-04 incident: the
// undamped M3 loop rotated every 20s forever — ~4k leaked FREED rows/day).

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

func damperBroker(clk *fakeClock) *Broker {
	b := &Broker{}
	b.cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	b.cfg.Now = clk.Now
	return b
}

// flowReaperFixture keeps the real leader/FSM and event publisher in the loop.
// Only observation time and the agent's readiness input are controlled here;
// allocation rotation and rehome must execute their production SQL planners.
// origin: hot-update S5 plan §4.5 / T14
type flowReaperFixture struct {
	b     *Broker
	clock *fakeClock
	alloc *port.Allocation
	tap   *eventTap
}

// Election waits can overlap, but these fixtures publish through the existing
// process-wide audit hook. Keep that hook exclusive until its cleanup removes
// it; the real Raft/SQL/event assertions still run with one observation owner.
// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
var flowReaperObservationMu sync.Mutex

func newFlowReaperFixture(t *testing.T, low, high int) *flowReaperFixture {
	t.Helper()
	b, leader := newClusterProxyBroker(t, "lab", "owner-fp")
	clock := newFakeClock(passEpoch)
	b.cfg.Now = clock.Now
	b.cfg.PortBandLow, b.cfg.PortBandHigh = low, high
	// Roster fixtures are populated before any concurrent reconciliation. All
	// writes under test below still go through the real leader's Propose/Apply.
	seed := &Broker{cfg: Config{DB: leader.DB()}}
	seedClusterNode(t, seed, "brk-a", "nats-a", "127.0.0.1:7443", "sha256:a", "VOTER")
	seedClusterNode(t, seed, "brk-b", "nats-b", "127.0.0.1:7444", "sha256:b", "VOTER")
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{
			SID: "lab", NID: "node", ProtoVersion: proto.ProtoVersion, ProxyCapable: true, NatsServer: "nats-a",
		}, clock.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := node.Heartbeat(b.livenessDB(), "lab", "node", clock.Now()); err != nil {
		t.Fatal(err)
	}
	setFlowReaperReady(t, b, true)
	var alloc *port.Allocation
	if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
		var cmd *cluster.Command
		var err error
		alloc, cmd, err = port.PlanAllocateProxy(db, "lab", "node", "brk-a", b.cfg.PortAllocCfg())
		return cmd, err
	}); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	b.nc.Store(nc)
	flowReaperObservationMu.Lock()
	t.Cleanup(flowReaperObservationMu.Unlock)
	tap := installEventTap(t)
	b.driveProxyReconcile()
	// Healthy-all-ready is deliberately silent. The computed count snapshot
	// witnesses that the gates did not skip the tick; negative controls below
	// separately require nonempty rotation/rehome event publications.
	if counts, ok := b.proxyEvtCounts.Load("lab"); !ok || counts != (proxyEventCounts{Enabled: true, CapableNodes: 1, ReadyNodes: 1}) {
		t.Fatalf("initial reaper tick did not observe one healthy ready node: %v", counts)
	}
	return &flowReaperFixture{b: b, clock: clock, alloc: alloc, tap: tap}
}

func setFlowReaperReady(t *testing.T, b *Broker, ready bool) {
	t.Helper()
	if err := node.SetProxyReady(b.livenessDB(), "lab", "node", ready); err != nil {
		t.Fatal(err)
	}
}

// One read transaction captures all allocation history, not just the currently
// active row: a free/reallocate cycle can end at the same port and epoch.
type flowReaperRow struct {
	ID        int64
	Port      int
	TokenHash string
	Home      string
	Epoch     int64
	State     string
}

func flowReaperSnapshot(t *testing.T, b *Broker) []flowReaperRow {
	t.Helper()
	tx, err := b.cfg.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(`SELECT row_id,port,token_hash,home_broker,epoch,state
		FROM port_allocations WHERE sid='lab' AND nid='node' AND name='__proxy__' ORDER BY row_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []flowReaperRow
	for rows.Next() {
		var row flowReaperRow
		if err := rows.Scan(&row.ID, &row.Port, &row.TokenHash, &row.Home, &row.Epoch, &row.State); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("allocation snapshot is empty")
	}
	return out
}

func assertFlowReaperUnchanged(t *testing.T, f *flowReaperFixture, before []flowReaperRow, events int) {
	t.Helper()
	if !reflect.DeepEqual(before, flowReaperSnapshot(t, f.b)) {
		t.Fatal("planned rebuild changed allocation history, port, token, home, epoch or state")
	}
	if got := len(f.tap.sysEvents(t)); got != events {
		t.Fatalf("planned rebuild emitted rotation/rehome/readiness events: before=%d after=%d", events, got)
	}
}

// origin: hot-update S5 plan §4.5 / T14
// These are the broker-side controls for the planned-cut readiness contract.
// They deliberately do not claim to test the agent's latch implementation.
func TestProxyReaperReadinessControls(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	before, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			exerciseProxyReaperReadinessControls(t)
			t.Run("initial-home-apply", func(t *testing.T) { exerciseInitialProxyHomeApply(t) })
		})
	}
	testharness.AssertNoGoroutineLeak(t, "proxy reaper readiness controls", before)
	// The lazily initialized process-wide network poller retains two FDs.
	testharness.AssertNoFDLeak(t, "proxy reaper readiness controls", beforeFD, 2)
}

func exerciseProxyReaperReadinessControls(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"recovered-ready", "lost-ready", "extended-loss", "unhealthy-home"} {
		t.Run(scenario, func(t *testing.T) {
			// Independent Raft owners overlap production election waits; the
			// enclosing round joins every cleanup before leak measurement.
			// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
			t.Parallel()
			f := newFlowReaperFixture(t, 22000, 22010)
			before := flowReaperSnapshot(t, f.b)
			events := len(f.tap.sysEvents(t))
			// Build two real bad observations. The damper must not yet have
			// rotated; recovering readiness must clear this historical dwell.
			setFlowReaperReady(t, f.b, false)
			for tick := 0; tick < proxyRehomeDwell-1; tick++ {
				f.b.driveProxyReconcile()
				f.clock.advance(observeTickInterval)
			}
			if !reflect.DeepEqual(before, flowReaperSnapshot(t, f.b)) {
				t.Fatal("allocation changed before the dwell threshold")
			}
			if got, _ := f.b.proxyDwell.Load(f.alloc.Port); got != proxyRehomeDwell-1 {
				t.Fatalf("historical bad dwell=%v, want %d", got, proxyRehomeDwell-1)
			}
			setFlowReaperReady(t, f.b, true)
			if scenario == "extended-loss" {
				// Unlike the old-dwell loss control, this starts a fresh run of
				// bad observations: t=0, interval, 2*interval must all be seen.
				f.b.driveProxyReconcile()
			}
			if scenario == "lost-ready" {
				setFlowReaperReady(t, f.b, false)
			}
			if scenario == "unhealthy-home" {
				if err := f.b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
					return cluster.PlanClusterNodePhase("brk-a", "DRAINING", []string{"VOTER"}, "", f.clock.Now())
				}); err != nil {
					t.Fatal(err)
				}
			}
			// A cut immediately before the tick sees the old dwell=2. No
			// healthy tick is smuggled between recovery and that first cut tick.
			for tick := 0; tick < proxyRehomeDwell+1; tick++ {
				if scenario == "extended-loss" {
					setFlowReaperReady(t, f.b, false)
				}
				f.b.driveProxyReconcile()
				if scenario == "extended-loss" && tick < proxyRehomeDwell-1 && !reflect.DeepEqual(before, flowReaperSnapshot(t, f.b)) {
					t.Fatal("fresh readiness loss rotated before the shortest bad-observation span")
				}
				f.clock.advance(observeTickInterval)
				if scenario == "recovered-ready" {
					// The first recovered tick legitimately re-announces ready.
					if tick == 0 {
						events = len(f.tap.sysEvents(t))
					}
					assertFlowReaperUnchanged(t, f, before, events)
				}
			}
			after := flowReaperSnapshot(t, f.b)
			switch scenario {
			case "recovered-ready":
				if _, ok := f.b.proxyDwell.Load(f.alloc.Port); ok {
					t.Fatal("a healthy ready tick retained historical bad dwell")
				}
			case "lost-ready", "extended-loss":
				if len(after) != 2 || after[0].State != "FREED" || after[1].State != "ALLOCATED" ||
					after[0].TokenHash == after[1].TokenHash || !f.b.proxyHomeHealthy(after[1].Home) || after[1].Epoch != 0 {
					t.Fatal("readiness loss did not rotate the allocation through real SQL")
				}
				if f.tap.countKind(t, evRehomeStalled) != 1 || f.tap.countKind(t, evHomeReassignSucceeded) != 0 {
					t.Fatal("rotation must emit stalled, never successful rehome")
				}
			case "unhealthy-home":
				if len(after) != 1 || after[0].Port != before[0].Port || after[0].TokenHash != before[0].TokenHash ||
					after[0].Home != "brk-b" || after[0].Epoch != before[0].Epoch+1 {
					t.Fatal("unhealthy home did not rehome the original allocation despite ready latch")
				}
				if f.tap.countKind(t, evHomeReassignStarted) != 1 || f.tap.countKind(t, evHomeReassignSucceeded) != 1 {
					t.Fatal("unhealthy home must emit exactly one started/succeeded event pair")
				}
			}
		})
	}
}

// A freshly minted token must not outrun a reachable remote home's committed
// allocation. The same bounded wait as ordinary exposes keeps terminal unknown
// token replies terminal; it does not turn them into client retries.
// origin: hot-update S5 plan §6.4 / §9.1 / DRILL-RUN 75.C
func exerciseInitialProxyHomeApply(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"applied", "disabled-during-wait", "home-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			// Independent Raft owners overlap production election waits; the
			// enclosing round joins every cleanup before leak measurement.
			// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
			t.Parallel()
			f := newFlowReaperFixture(t, 22100, 22110)
			b, leader, nc := f.b, f.b.cl.node, f.b.nc.Load()
			if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "node",
					ProtoVersion: proto.ProtoVersion, ProxyCapable: true, NatsServer: "nats-b"}, f.clock.Now())
			}); err != nil {
				t.Fatal(err)
			}
			before, err := leader.AppliedIndex()
			if err != nil {
				t.Fatal(err)
			}
			var allow atomic.Bool
			polled := make(chan struct{}, 1)
			cursor, err := nc.Subscribe(proto.SubjClusterCursor, func(m *nats.Msg) {
				select {
				case polled <- struct{}{}:
				default:
				}
				// A high cursor from the wrong peer cannot release the push.
				wrong, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-a", AppliedIndex: before + 1000})
				_ = m.Respond(wrong)
				if scenario == "home-unavailable" {
					return
				}
				applied := before
				if allow.Load() {
					applied, _ = leader.AppliedIndex()
				}
				body, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-b", AppliedIndex: applied})
				_ = m.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cursor.Unsubscribe() })
			push, err := nc.SubscribeSync(proto.SubjCmdForwarded("lab", "node", "proxy-keys"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = push.Unsubscribe() })
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			done := make(chan struct{})
			go func() { b.allocateAndPushProxy(nc, "lab", "node", nil, 0); close(done) }()
			// Joining on every failure keeps a red mutation from leaking its
			// real proposer/publisher into the following exercise round.
			defer func() { allow.Store(true); <-done }()
			if msg, err := push.NextMsg(100 * time.Millisecond); err == nil || msg != nil {
				t.Fatal("initial proxy token was pushed before the home applied its allocation")
			}
			select {
			case <-polled:
			case <-time.After(time.Second):
				t.Fatal("initial proxy allocation never checked the remote home cursor")
			}
			if scenario == "disabled-during-wait" {
				if err := leader.Propose(func(*sql.DB) (*cluster.Command, error) {
					return session.PlanProxySetEnabled("lab", false, session.ProxyHAFreeze, f.clock.Now())
				}); err != nil {
					t.Fatal(err)
				}
			}
			allow.Store(true)
			msg, err := push.NextMsg(exposeHomeApplyBudget + 2*exposeHomeApplyStep)
			if err != nil {
				t.Fatal(err)
			}
			var d proto.ProxyDirective
			if err := json.Unmarshal(msg.Data, &d); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			if elapsed > exposeHomeApplyBudget+2*exposeHomeApplyStep {
				t.Fatal("initial proxy allocation exceeded the existing home apply budget")
			}
			if scenario == "disabled-during-wait" {
				if d.Enabled || d.Token != "" || len(d.Keys) != 0 {
					t.Fatal("home wait reused authority revoked during the wait")
				}
			} else if !d.Enabled || d.Token == "" || d.Home == nil || d.Home.NodeID != "brk-b" {
				t.Fatal("initial proxy push lost its single-use token or committed home")
			}
			if scenario == "home-unavailable" && elapsed < exposeHomeApplyBudget {
				t.Fatal("another peer's cursor or a missing home bypassed the bounded apply wait")
			}
			rows := flowReaperSnapshot(t, b)
			if len(rows) != 2 || rows[0].State != "FREED" || rows[1].State != "ALLOCATED" || rows[1].Home != "brk-b" {
				t.Fatal("home apply wait reminted or discarded the committed allocation")
			}
		})
	}
}

// TestProxyRotateBackoffSchedule pins the damped rotation ladder: with the
// node persistently unready, successive rotations are allowed only at
// 20s → 40s → 80s → … → the 10min cap. Mutation check: removing the
// proxyRotateDue gate (always-rotate) collapses every wait to zero and this
// test goes red.
func TestProxyRotateBackoffSchedule(t *testing.T) {
	clk := newFakeClock(passEpoch)
	b := damperBroker(clk)

	rotate := func() { proxyRotateFailed(b, "lab", "wsl") }
	if !proxyRotateDue(b, "lab", "wsl") {
		t.Fatal("first rotation must be immediately due (no tracker yet)")
	}
	rotate()

	wantWaits := []time.Duration{
		20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second, 320 * time.Second, 600 * time.Second, 600 * time.Second,
	}
	for i, wait := range wantWaits {
		if proxyRotateDue(b, "lab", "wsl") {
			t.Fatalf("step %d: rotation due immediately after a rotation — the damper is off", i)
		}
		clk.advance(wait - time.Millisecond)
		if proxyRotateDue(b, "lab", "wsl") {
			t.Fatalf("step %d: rotation due %v early", i, time.Millisecond)
		}
		clk.advance(time.Millisecond)
		if !proxyRotateDue(b, "lab", "wsl") {
			t.Fatalf("step %d: rotation not due at its scheduled %v", i, wait)
		}
		rotate()
	}
}

// TestProxyRotateClearHysteresis pins the 60s sustained-ready contract
// (plan critique-4 — the incident link "intermittently healed"):
//   - 11 consecutive ready ticks do NOT drop the tracker;
//   - an unready tick after a short blip DECAYS the schedule one step
//     instead of resetting it;
//   - 12 consecutive ready ticks drop the tracker (a future failure starts
//     back at the 20s base).
func TestProxyRotateClearHysteresis(t *testing.T) {
	clk := newFakeClock(passEpoch)
	b := damperBroker(clk)

	// Build a run of 3 rotations → next wait would be 160s.
	for i := 0; i < 3; i++ {
		proxyRotateFailed(b, "lab", "wsl")
		clk.advance(15 * time.Minute)
	}

	// 11 ready ticks: tracker must survive.
	for i := 0; i < proxyBindClearTicks-1; i++ {
		proxyRotateReady(b, "lab", "wsl")
	}
	if _, ok := b.proxyRotate.Load(proxyRotateKey("lab", "wsl")); !ok {
		t.Fatal("tracker dropped before the clear hysteresis elapsed")
	}

	// Blip ends: unready again → the tracker DECAYS one step (fails 3 → 2).
	// Fail schedules base·2^fails using the pre-increment count, so the next
	// rotation lands at base·2² = 80s — stepped down one notch from the 160s
	// the undecayed run would have scheduled, and NOT reset to the 20s base.
	proxyRotateUnready(b, "lab", "wsl")
	proxyRotateFailed(b, "lab", "wsl")
	clk.advance(80*time.Second - time.Millisecond)
	if proxyRotateDue(b, "lab", "wsl") {
		t.Fatal("decay must step down one notch (80s), not reset to the 20s base")
	}
	clk.advance(time.Millisecond)
	if !proxyRotateDue(b, "lab", "wsl") {
		t.Fatal("decayed schedule must be due at 80s")
	}

	// Now a full sustained-ready run: 12 ticks → tracker gone; the next
	// failure starts a fresh run at the base cadence.
	clk.advance(time.Hour)
	for i := 0; i < proxyBindClearTicks; i++ {
		proxyRotateReady(b, "lab", "wsl")
	}
	if _, ok := b.proxyRotate.Load(proxyRotateKey("lab", "wsl")); ok {
		t.Fatal("tracker must drop after 12 consecutive ready ticks")
	}
	proxyRotateFailed(b, "lab", "wsl")
	clk.advance(20 * time.Second)
	if !proxyRotateDue(b, "lab", "wsl") {
		t.Fatal("post-recovery failure run must restart at the 20s base")
	}
}

// The alert sweep and the rotation tracker must share the same clear
// hysteresis. Looking only at proxy_ready would clear the persisted alert on
// the first healthy tick even though proxyRotateReady deliberately requires
// twelve consecutive ticks.
func TestProxyBindStalledSetHonorsClearHysteresis(t *testing.T) {
	clk := newFakeClock(passEpoch)
	b := damperBroker(clk)
	key := proxyRotateKey("lab", "wsl")
	proxyRotateFailed(b, "lab", "wsl")

	if !proxyBindStalledThisTick(b, key, false) {
		t.Fatal("unready node must be stalled")
	}
	for i := 0; i < proxyBindClearTicks-1; i++ {
		proxyRotateReady(b, "lab", "wsl")
		if !proxyBindStalledThisTick(b, key, true) {
			t.Fatalf("ready tick %d cleared stalled state before hysteresis elapsed", i+1)
		}
	}
	proxyRotateReady(b, "lab", "wsl")
	if proxyBindStalledThisTick(b, key, true) {
		t.Fatal("full sustained-ready run must clear stalled state")
	}

	// Leader restart shape: no local tracker, but committed unready state is
	// independently sufficient to retain the alert.
	b.proxyRotate.Delete(key)
	if !proxyBindStalledThisTick(b, key, false) {
		t.Fatal("unready node without a tracker must remain stalled after restart")
	}
}

// ---- proxy_bind_stalled alert convergence ----------------------------------
// origin: internal review (raft + concurrency lenses) — the first version of
// sweepProxyBindAlerts only cleared alerts whose SESSION was gone and left the
// healed-node clear to an in-memory tracker that dies with the process, so
// after ANY broker restart an alert for a healed node could never clear.

// alertSweepBroker builds a Broker with a real DB (so ActiveAlerts works) and
// a stub propose that records what the sweep would commit. It deliberately
// does NOT stand up raft: the decision under test is WHICH alerts the sweep
// resolves, and cluster.PlanAlertClear's SQL is covered by the cluster tests.
func alertSweepBroker(t *testing.T, clk *fakeClock) (*Broker, *sql.DB) {
	t.Helper()
	db := passTestDB(t)
	b := damperBroker(clk)
	b.cfg.DB = db
	return b, db
}

func seedActiveBindAlert(t *testing.T, db *sql.DB, sid, nid string) string {
	t.Helper()
	key := cluster.DedupKeyNode(cluster.AlertKindProxyBindStalled, sid+"/"+nid)
	if _, err := db.Exec(
		`INSERT INTO alerts(id, kind, severity, dedup_key, state, message, raised_at)
		 VALUES (?,?,?,?, 'ACTIVE', 'stalled', ?)`,
		"id-"+key, cluster.AlertKindProxyBindStalled, cluster.AlertSeverityInfo, key, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	return key
}

func activeBindAlertKeys(t *testing.T, db *sql.DB) []string {
	t.Helper()
	alerts, err := cluster.ActiveAlerts(db)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, a := range alerts {
		if a.Kind == cluster.AlertKindProxyBindStalled {
			out = append(out, a.DedupKey)
		}
	}
	return out
}

// TestSweepProxyBindAlertsDecisions drives the five cases the
// review found unhandled or wrongly handled. It asserts on which keys the
// sweep DECIDES to clear (b.cl is nil so no raft write happens — the
// decision, not the commit, is what regressed).
func TestSweepProxyBindAlertsDecisions(t *testing.T) {
	cases := []struct {
		name        string
		sid, nid    string
		activeSID   bool
		stalledNow  bool
		nodeStatus  string // "" = no node row at all (evicted)
		proxyReady  bool   // the AUTHORITATIVE health bit (h1 external review F2)
		wantCleared bool
	}{
		{
			// The restart case: alert ACTIVE, session active, node ONLINE and
			// GENUINELY healed — no tracker exists because the process
			// restarted. Before the internal-review fix this stayed ACTIVE
			// forever.
			//
			// F2 correction: "no tracker" is NOT recovery evidence. This case
			// must carry the real readiness bit, or it would assert exactly the
			// false-clear the external review found.
			name: "healed node after restart clears", sid: "lab", nid: "wsl",
			activeSID: true, stalledNow: false, nodeStatus: "ONLINE", proxyReady: true, wantCleared: true,
		},
		{
			// F2's own case: restarted leader (empty stalledNow), node ONLINE
			// but proxy_ready=0 — the data plane is provably still down.
			name: "still-unready online node after restart keeps its alert", sid: "lab", nid: "wsl",
			activeSID: true, stalledNow: false, nodeStatus: "ONLINE", proxyReady: false, wantCleared: false,
		},
		{
			// Still failing on this very tick — must NOT clear.
			name: "currently stalled stays", sid: "lab", nid: "wsl",
			activeSID: true, stalledNow: true, nodeStatus: "ONLINE", proxyReady: false, wantCleared: false,
		},
		{
			// An unreachable agent IS a stalled proxy: its absence from
			// stalledNow is an artifact of onlineNIDs, not evidence of health.
			name: "offline node keeps its alert", sid: "lab", nid: "wsl",
			activeSID: true, stalledNow: false, nodeStatus: "OFFLINE", proxyReady: false, wantCleared: false,
		},
		{
			// Evicted node: the row is gone, so the subject is gone.
			name: "evicted node clears", sid: "lab", nid: "wsl",
			activeSID: true, stalledNow: false, nodeStatus: "", proxyReady: false, wantCleared: true,
		},
		{
			name: "session gone clears", sid: "lab", nid: "wsl",
			activeSID: false, stalledNow: false, nodeStatus: "ONLINE", proxyReady: false, wantCleared: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clk := newFakeClock(passEpoch)
			b, db := alertSweepBroker(t, clk)
			if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES(?,?,'o','p','ACTIVE',?)`,
				c.sid, c.sid, passEpoch); err != nil {
				t.Fatal(err)
			}
			if c.nodeStatus != "" {
				ready := 0
				if c.proxyReady {
					ready = 1
				}
				if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,proxy_capable,proxy_ready,registered_at,last_heartbeat_at) VALUES(?,?,?,1,?,?,?)`,
					c.nid, c.sid, c.nodeStatus, ready, passEpoch, passEpoch); err != nil {
					t.Fatal(err)
				}
			}
			key := seedActiveBindAlert(t, db, c.sid, c.nid)
			// A tracker exists iff the node is judged stalled right now.
			if c.stalledNow {
				proxyRotateFailed(b, c.sid, c.nid)
			}

			activeSIDs := map[string]bool{}
			if c.activeSID {
				activeSIDs[c.sid] = true
			}
			stalled := map[string]bool{}
			if c.stalledNow {
				stalled[proxyRotateKey(c.sid, c.nid)] = true
			}
			var cleared []string
			sweepProxyBindAlerts(b, activeSIDs, stalled, func(k string) { cleared = append(cleared, k) })

			got := len(cleared) == 1 && cleared[0] == key
			if got != c.wantCleared {
				t.Fatalf("sweep cleared=%v (keys %v), want cleared=%v for alert %s; still-active=%v",
					got, cleared, c.wantCleared, key, activeBindAlertKeys(t, db))
			}
			if c.wantCleared {
				if _, ok := b.proxyReadyTicks.Load(proxyRotateKey(c.sid, c.nid)); ok {
					t.Fatal("cleared alert left a proxyReadyTicks entry — sync.Map leak")
				}
			}
		})
	}
}

// origin: h1 external review F2
// An ACTIVE alert surviving a broker restart has no leader-local tracker. If
// the ONLINE node is still unready, that missing tracker is not evidence of
// recovery: the first post-restart sweep must retain the alert until the
// current data-plane observation proves readiness.
func TestSweepProxyBindAlertsKeepsStillUnreadyOnlineNodeAfterRestart(t *testing.T) {
	clk := newFakeClock(passEpoch)
	b, db := alertSweepBroker(t, clk)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at,proxy_enabled)
		 VALUES('lab','lab','o','p','ACTIVE',?,1)`, passEpoch,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(nid,sid,status,proxy_capable,proxy_ready,registered_at,last_heartbeat_at)
		 VALUES('wsl','lab','ONLINE',1,0,?,?)`, passEpoch, passEpoch,
	); err != nil {
		t.Fatal(err)
	}
	key := seedActiveBindAlert(t, db, "lab", "wsl")

	// Restart state: the replicated alert survived, while both leader-local
	// maps are empty. Exercise the sweep's independent authoritative recheck:
	// even a stale/empty caller set cannot erase an alert for an unready node.
	var cleared []string
	sweepProxyBindAlerts(b, map[string]bool{"lab": true}, map[string]bool{},
		func(k string) { cleared = append(cleared, k) })

	if len(cleared) != 0 {
		t.Fatalf("still-unready ONLINE node lost ACTIVE alert after restart: cleared=%v, alert=%s", cleared, key)
	}
}

// TestProxyBindAlertRaiseIsLevelTriggered pins the fix for the edge-trigger:
// the raise must be attempted on EVERY rotation past the threshold, not only
// on the exact Nth one. A single failed Propose (leadership blip) at rotation
// 3 would otherwise silence the alert for the entire remaining stall, and
// proposeProxyBindAlert's error path only logs.
func TestProxyBindAlertRaiseIsLevelTriggered(t *testing.T) {
	clk := newFakeClock(passEpoch)
	b := damperBroker(clk)
	raises := 0
	// proposeProxyBindAlert returns early without a cluster node, so count the
	// CALL SITE condition directly against the tracker the production code
	// consults.
	for i := 0; i < 6; i++ {
		proxyRotateFailed(b, "lab", "wsl")
		tr, ok := loadProxyTracker(b, proxyRotateKey("lab", "wsl"))
		if !ok {
			t.Fatal("tracker vanished")
		}
		if tr.Fails() >= proxyBindAlertAfter {
			raises++
		}
		clk.advance(15 * time.Minute) // past any backoff hold
	}
	if raises != 4 {
		t.Fatalf("raise attempted on %d of rotations 3..6, want 4 — an edge-triggered "+
			"`== proxyBindAlertAfter` lets one failed Propose silence the whole stall", raises)
	}
}
