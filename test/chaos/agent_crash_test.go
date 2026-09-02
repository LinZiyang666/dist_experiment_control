// Agent-crash chaos tests. Cover dimension C from the chaos
// checklist: agent OFFLINE detection, agent re-register convergence,
// LOST process derivation, and expose-token persistence across an
// agent restart.
package chaos_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// TestAgentCrashGetsMarkedOFFLINE pins C.11: with OfflineAfter set
// to a short value, killing the agent must result in nodes.status =
// 'OFFLINE' within OfflineAfter + one ReconcileInterval.
func TestAgentCrashGetsMarkedOFFLINE(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 15*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)
	ah := startAgentExplicit(t, url, "lab", "n1")

	testharness.WaitNodeOnline(t, db, "lab", "n1", 3*time.Second)

	// "Crash" the agent (clean cancel + drain — the closest we get
	// without sending an actual SIGKILL to ourselves).
	ah.Stop()
	proveInjected(t, "agent stopped (heartbeat frozen)", agentHeartbeatFrozen(t, db, "lab", "n1"))

	// Broker config in startBrokerExplicit: OfflineAfter=900ms,
	// ReconcileInterval=50ms. So we should see OFFLINE within ~1s of
	// the last heartbeat.
	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		var st string
		err := db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		return err == nil && st == "OFFLINE"
	}) {
		var st string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		t.Errorf("agent crash: node did not reach OFFLINE within 5s; status=%q", st)
	}
}

// TestAgentRestartReconcilesONLINE pins C.12: after a crash → restart,
// the new agent's register must drive the row back to ONLINE without
// operator action (G.1 reconcile path).
func TestAgentRestartReconcilesONLINE(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 20*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)
	ah1 := startAgentExplicit(t, url, "lab", "n1")
	testharness.WaitNodeOnline(t, db, "lab", "n1", 3*time.Second)
	ah1.Stop()
	proveInjected(t, "agent stopped (heartbeat frozen)", agentHeartbeatFrozen(t, db, "lab", "n1"))

	// Wait for OFFLINE first so we know the recovery isn't just a
	// "never noticed it died" no-op.
	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		var st string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		return st == "OFFLINE"
	}) {
		t.Fatal("never reached OFFLINE before agent restart")
	}

	_ = startAgentExplicit(t, url, "lab", "n1")

	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		var st string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		return st == "ONLINE"
	}) {
		var st string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		t.Errorf("after agent restart: status=%q want ONLINE", st)
	}
}

// TestAgentCrashWithRunningProcDerivesLOSTThenReconciles pins C.13:
// a RUNNING row whose owning agent is OFFLINE shows as LOST in
// `tether ps` (read-time derivation per architecture P8 / proc.go).
// On agent restart with empty LocalProcesses snapshot the row gets
// reconciled to EXITED rc=-1 (missed-exit fallback).
func TestAgentCrashWithRunningProcDerivesLOSTThenReconciles(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 20*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)
	ah1 := startAgentExplicit(t, url, "lab", "n1")
	testharness.WaitNodeOnline(t, db, "lab", "n1", 3*time.Second)

	// Pre-seed a RUNNING row directly (mimics "exec started, then
	// agent died before recording exit").
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hzcrash000000000000000', 'lab', 'n1', '["sleep","9999"]', ?, 'RUNNING', ?)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	// Crash agent.
	ah1.Stop()
	proveInjected(t, "agent stopped (heartbeat frozen)", agentHeartbeatFrozen(t, db, "lab", "n1"))
	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		var st string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		return st == "OFFLINE"
	}) {
		t.Fatal("agent never reached OFFLINE")
	}

	// Confirm ps now reports LOST for this proc.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	psBody, _ := json.Marshal(proto.PsReq{})
	respMsg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), psBody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.PsResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	gotLost := false
	for _, p := range resp.Processes {
		if p.PID == "01hzcrash000000000000000" && p.Status == "LOST" {
			gotLost = true
			break
		}
	}
	if !gotLost {
		t.Errorf("after agent crash, ps did not derive LOST for the orphaned RUNNING row: %+v",
			resp.Processes)
	}

	// Restart agent. With empty LocalProcesses snapshot, broker reconciles
	// → EXITED rc=-1 per architecture G.1.
	_ = startAgentExplicit(t, url, "lab", "n1")

	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		var st string
		var rc *int
		_ = db.QueryRow(
			`SELECT status, exit_code FROM processes WHERE pid='01hzcrash000000000000000'`,
		).Scan(&st, &rc)
		return st == "EXITED" && rc != nil && *rc == -1
	}) {
		var st string
		var rc *int
		_ = db.QueryRow(
			`SELECT status, exit_code FROM processes WHERE pid='01hzcrash000000000000000'`,
		).Scan(&st, &rc)
		t.Errorf("after agent restart: status=%q rc=%v want EXITED rc=-1", st, rc)
	}
}

// TestAgentCrashKeepsExposeTokenInBrokerDB pins C.14: when the agent
// has an active expose and crashes, the broker's port_allocations
// row stays ALLOCATED so the token is still valid when the agent
// reconnects (architecture F.6 — agent restart re-uses the same
// port via state.json + token re-presentation).
//
// We can't actually run the tunnel (would require frps + frpc); we
// pin the state-side guarantee: ALLOCATED row survives the crash AND
// the next register accepts the agent's claim under keep_ports.
func TestAgentCrashKeepsExposeTokenInBrokerDB(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 20*time.Second)
	defer cancel()

	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)

	// Bring the agent up so the node row is real.
	home := t.TempDir()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "n1",
		Home:              home,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, cancelAgent := context.WithCancel(ctx)
	agentDone := make(chan error, 1)
	go func() { agentDone <- a.Run(agentCtx) }()
	t.Cleanup(func() {
		cancelAgent()
		select {
		case <-agentDone:
		case <-time.After(3 * time.Second):
		}
	})
	testharness.WaitNodeOnline(t, db, "lab", "n1", 3*time.Second)

	// Pre-seed an ALLOCATED port_allocations row + state.json (=
	// "agent had a live expose").
	rawToken := "chaos-expose-token-aaaa"
	tokenHash := port.HashToken(rawToken)
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (14111, 'lab', 'n1', 'live-svc', 9100, ?, 'ALLOCATED', ?, ?)`,
		tokenHash, fp, now,
	); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stateJSON := []byte(`{"port_tokens":[{"name":"live-svc","port":14111,"local_port":9100,"token":"` + rawToken + `"}]}`)
	if err := os.WriteFile(filepath.Join(stateDir, "state.json"), stateJSON, 0o600); err != nil {
		t.Fatal(err)
	}

	// Crash agent.
	cancelAgent()
	select {
	case <-agentDone:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not exit on crash")
	}

	// port_allocations row MUST still be ALLOCATED (= broker did not
	// drop it just because the agent vanished).
	var st string
	if err := db.QueryRow(
		`SELECT state FROM port_allocations WHERE port=14111`,
	).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "ALLOCATED" {
		t.Errorf("port row after agent crash: state=%q want ALLOCATED", st)
	}

	// Restart agent. Its register snapshot includes the LocalPort with
	// the matching token_hash; broker MUST list it in keep_ports
	// (NOT revoke_ports).
	a2, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "n1",
		Home:              home,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent2Ctx, cancelAgent2 := context.WithCancel(ctx)
	agent2Done := make(chan error, 1)
	go func() { agent2Done <- a2.Run(agent2Ctx) }()
	t.Cleanup(func() {
		cancelAgent2()
		select {
		case <-agent2Done:
		case <-time.After(3 * time.Second):
		}
	})

	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		var s string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&s)
		return s == "ONLINE"
	}) {
		t.Fatal("agent did not re-register after restart")
	}

	// state.json should still mention live-svc — broker said keep, agent
	// must not have pruned it.
	body, err := os.ReadFile(filepath.Join(stateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytesContain(body, "live-svc") {
		t.Errorf("state.json after agent restart no longer mentions live-svc: %s", body)
	}

	// Port row state must still be ALLOCATED (= no spurious revoke).
	var st2 string
	if err := db.QueryRow(
		`SELECT state FROM port_allocations WHERE port=14111`,
	).Scan(&st2); err != nil {
		t.Fatal(err)
	}
	if st2 != "ALLOCATED" {
		t.Errorf("port row after agent restart: state=%q want ALLOCATED", st2)
	}
}

func bytesContain(haystack []byte, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	n := []byte(needle)
	for i := 0; i+len(n) <= len(haystack); i++ {
		if equalBytes(haystack[i:i+len(n)], n) {
			return true
		}
	}
	return false
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
