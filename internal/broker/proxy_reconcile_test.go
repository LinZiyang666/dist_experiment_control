package broker

// proxy_reconcile_test.go — unit coverage for the h1 E2 M3 rotation damper
// (the decision core, driven with a fake clock; the raft alert propose path
// no-ops on a bare Broker and is exercised by the cluster harnesses).
// origin: docs/reviews/h1-plan.md workstream E2 (2026-08-04 incident: the
// undamped M3 loop rotated every 20s forever — ~4k leaked FREED rows/day).

import (
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

func damperBroker(clk *fakeClock) *Broker {
	b := &Broker{}
	b.cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	b.cfg.Now = clk.Now
	return b
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
