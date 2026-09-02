// Package p8_test exercises G.1 (agent reconnect) and G.2 (broker
// boot) reconciliation end-to-end. The chaos invariants from
// architecture P8 boil down to:
//
//   - kill agent → restart agent → every RUNNING row owned by that
//     node becomes EXITED(rc=-1) via reconciled_closed audit;
//   - revoked port reaches the agent on next register and the agent
//     drops both the tunnel session and its state.json entry;
//   - boot-time node-state recompute moves a long-stale node from
//     persisted ONLINE to OFFLINE without waiting for the ticker;
//   - LOST is derived (not stored) — `tether ps` reports a RUNNING
//     row as LOST while the owning node is OFFLINE.
package p8_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ----- harness --------------------------------------------------------------

func startJSNATS(t *testing.T) string            { return testharness.StartJSNATS(t) }
func startNATS(t *testing.T) string              { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                    { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

func seedSession(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	stackharness.SeedSession(t, db, sid, fp)
}

func startBroker(t *testing.T, url string, db *sql.DB) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

func startAgent(t *testing.T, url, sid, nid string) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

// startAgentManual is startAgent without the auto-cleanup deferral — the
// returned stop closure can be called mid-test (e.g. simulating agent
// crash) and the boolean tracks whether a subsequent call should
// no-op so the test's defer doesn't double-cancel.
func startAgentManual(t *testing.T, url, sid, nid string) (stop func(), wait func()) {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
	wait = func() {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit on wait")
		}
	}
	return stop, wait
}

// startBrokerManual is startBroker with non-deferred stop, mirror of
// startAgentManual. Used by the chaos tests that restart the broker
// mid-run.
func startBrokerManual(t *testing.T, url string, db *sql.DB) (stop func()) {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

// ----- tests ----------------------------------------------------------------

// TestG1MissedExitOnAgentRestart pins the architecture P8 chaos
// invariant: 10 long-running rows pre-seeded as RUNNING (= broker
// thinks they're alive), then a fresh agent registers with an empty
// LocalProcesses snapshot. Broker MUST mark all 10 as EXITED(rc=-1)
// and emit reconciled_closed audit per row.
func TestG1MissedExitOnAgentRestart(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// Pre-create the history stream so audit.proc{kind:reconciled_closed}
	// has somewhere to land. seedSession only inserts the row.
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}

	// Pre-seed 10 RUNNING processes for (lab, lab-1). The node row
	// doesn't exist yet — INSERT it manually since proc.Insert
	// requires the FK target.
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	const N = 10
	for i := 0; i < N; i++ {
		pid := []byte("01hz000000000000000000000z")
		pid[len(pid)-1] = byte('a' + i)
		if _, err := db.Exec(
			`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
			 VALUES (?, 'lab', 'lab-1', ?, ?, 'RUNNING', ?)`,
			string(pid), `["sleep","9999"]`, now, fp,
		); err != nil {
			t.Fatal(err)
		}
	}

	// Subscribe to audit.proc BEFORE starting broker so we catch
	// reconciled_closed publishes that happen during register.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	reconciledN := make(chan int, 1)
	gotReconciled := 0
	subAudit, _ := nc.Subscribe(proto.SubjAuditProc("lab"), func(m *nats.Msg) {
		if strings.Contains(string(m.Data), `"kind":"reconciled_closed"`) {
			gotReconciled++
			if gotReconciled == N {
				select {
				case reconciledN <- gotReconciled:
				default:
				}
			}
		}
	})
	defer func() { _ = subAudit.Unsubscribe() }()

	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	// Wait for all N reconciled_closed audits.
	select {
	case <-reconciledN:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected %d reconciled_closed audits, got %d", N, gotReconciled)
	}

	// Verify SQLite state: every RUNNING row should now be EXITED rc=-1.
	rows, err := db.Query(`SELECT pid, status, exit_code FROM processes WHERE sid='lab' AND nid='lab-1'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	exited := 0
	for rows.Next() {
		var pid, status string
		var rc *int
		if err := rows.Scan(&pid, &status, &rc); err != nil {
			t.Fatal(err)
		}
		if status != "EXITED" {
			t.Errorf("pid=%s: status=%q want EXITED", pid, status)
			continue
		}
		if rc == nil || *rc != -1 {
			t.Errorf("pid=%s: rc=%v want -1", pid, rc)
			continue
		}
		exited++
	}
	if exited != N {
		t.Errorf("exited rows: got %d want %d", exited, N)
	}
}

// TestG1RevokedPortReachesAgentOnReconnect simulates a port the broker
// REVOKED while the agent was offline. The agent's state.json still
// has the entry; the agent re-presents (port, token_hash) on
// register, and the reply MUST list that port in revoke_ports.
func TestG1RevokedPortReachesAgent(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// Insert node row + a REVOKED port_allocations row directly.
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	rawToken := "test-revoked-token-deadbeef"
	tokenHash := port.HashToken(rawToken)
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at)
		 VALUES (14077, 'lab', 'lab-1', 'jupyter', 8888, ?, 'REVOKED', ?, ?, ?)`,
		tokenHash, fp, now.Add(-30*time.Minute), now,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	// Issue a register req carrying the agent's claimed local port.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		LocalPorts: []proto.LocalPort{
			{Port: 14077, Name: "jupyter", LocalPort: 8888, TokenHash: tokenHash},
		},
	})
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.RevokePorts) != 1 || resp.RevokePorts[0] != 14077 {
		t.Errorf("expected revoke_ports=[14077]; got %v", resp.RevokePorts)
	}
	if len(resp.KeepPorts) != 0 {
		t.Errorf("expected empty keep_ports; got %v", resp.KeepPorts)
	}
}

// TestG1OrphanPortReachesAgent: agent claims a port the broker has no
// row for (token_hash mismatch / row vanished). Broker MUST list it
// in revoke_ports so the agent tears down the stray tunnel.
func TestG1OrphanPortReachesAgent(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	_ = fp

	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		LocalPorts: []proto.LocalPort{
			{Port: 14077, Name: "ghost", LocalPort: 8888, TokenHash: "00deadbeef"},
		},
	})
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.RevokePorts) != 1 || resp.RevokePorts[0] != 14077 {
		t.Errorf("expected revoke_ports=[14077] for orphan; got %v", resp.RevokePorts)
	}
}

// TestG2BootRecomputesNodeStatus: a node row persisted with status=
// ONLINE but a stale last_heartbeat_at must be moved to OFFLINE
// during broker boot reconcile, BEFORE the first ticker fires.
func TestG2BootRecomputesNodeStatus(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	// Persisted as ONLINE; last heartbeat 2 minutes ago.
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	// Without waiting for the ticker, status should already be OFFLINE.
	var status string
	if err := db.QueryRow(`SELECT status FROM nodes WHERE sid='lab' AND nid='lab-1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "OFFLINE" {
		t.Errorf("boot reconcile: status=%q want OFFLINE", status)
	}
}

// TestPsDerivesLOSTWhenNodeOffline: a RUNNING process row whose
// owning node is OFFLINE shows as LOST in `tether ps`. SQLite stays
// RUNNING (LOST is read-time derivation per architecture / proc.go
// pkgdoc).
func TestPsDerivesLOSTWhenNodeOffline(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hz_test_lost', 'lab', 'lab-1', '["sleep","9999"]', ?, 'RUNNING', ?)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	body, _ := json.Marshal(proto.PsReq{})
	respMsg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.PsResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.Processes) != 1 {
		t.Fatalf("expected 1 proc; got %d", len(resp.Processes))
	}
	if resp.Processes[0].Status != "LOST" {
		t.Errorf("ps status: got %q want LOST", resp.Processes[0].Status)
	}

	// Sanity: SQLite row is still RUNNING (LOST is a view, not state).
	var sqliteStatus string
	if err := db.QueryRow(
		`SELECT status FROM processes WHERE pid='01hz_test_lost'`,
	).Scan(&sqliteStatus); err != nil {
		t.Fatal(err)
	}
	if sqliteStatus != "RUNNING" {
		t.Errorf("SQLite row mutated: got %q want RUNNING", sqliteStatus)
	}
}

// TestChaosKillAgentRestartConverges is the full architecture P8
// invariant: real broker, real agent, exec drives RUNNING rows, then
// the agent is killed mid-flight and a fresh agent registers. After
// re-register every row that the broker thought was RUNNING under
// (lab, lab-1) MUST become EXITED(rc=-1) with reconciled_closed audit.
//
// We can't actually use `sleep 9999` from runExec — runExec waits for
// exit. Instead we directly seed processes table with RUNNING rows
// AFTER the live agent has registered (so the node row is real and
// the FK target exists), then kill the agent and confirm the second
// register's reconciliation reply lists them all.
func TestChaosKillAgentRestartConverges(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}

	defer startBroker(t, url, db)()

	// First agent: the node row gets created via real register.
	stop1, _ := startAgentManual(t, url, "lab", "lab-1")
	// stop1 is called explicitly mid-test (chaos crash); guard
	// against an early Fatal between here and that call (audit
	// shard 05 F20). stoppedExplicitly tracks whether the test
	// already ran the explicit call so Cleanup doesn't double-stop.
	stoppedExplicitly := false
	t.Cleanup(func() {
		if !stoppedExplicitly {
			stop1()
		}
	})

	// Verify node row is ONLINE.
	var status string
	if err := db.QueryRow(
		`SELECT status FROM nodes WHERE sid='lab' AND nid='lab-1'`,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "ONLINE" {
		t.Fatalf("after register: status=%q want ONLINE", status)
	}

	// Seed RUNNING process rows under this real node (FK satisfied).
	const N = 10
	now := time.Now().UTC()
	for i := 0; i < N; i++ {
		pid := []byte("01hzchaos00000000000000000")
		pid[len(pid)-1] = byte('a' + i)
		if _, err := db.Exec(
			`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
			 VALUES (?, 'lab', 'lab-1', ?, ?, 'RUNNING', ?)`,
			string(pid), `["sleep","9999"]`, now, fp,
		); err != nil {
			t.Fatal(err)
		}
	}

	// Subscribe to audit.proc BEFORE killing/restart so we see the
	// reconciled_closed batch.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	gotReconciled := 0
	doneCh := make(chan struct{}, 1)
	subAudit, _ := nc.Subscribe(proto.SubjAuditProc("lab"), func(m *nats.Msg) {
		if strings.Contains(string(m.Data), `"kind":"reconciled_closed"`) {
			gotReconciled++
			if gotReconciled == N {
				select {
				case doneCh <- struct{}{}:
				default:
				}
			}
		}
	})
	defer func() { _ = subAudit.Unsubscribe() }()

	// Kill the live agent (= chaos crash).
	stoppedExplicitly = true
	stop1()

	// Restart agent (fresh process — empty LocalProcesses snapshot).
	defer startAgent(t, url, "lab", "lab-1")()

	select {
	case <-doneCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("expected %d reconciled_closed audits after agent restart; got %d",
			N, gotReconciled)
	}

	// Every seeded row must be EXITED rc=-1.
	rows, err := db.Query(
		`SELECT pid, status, exit_code FROM processes WHERE sid='lab' AND nid='lab-1' ORDER BY pid`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	exited := 0
	for rows.Next() {
		var pid, st string
		var rc *int
		if err := rows.Scan(&pid, &st, &rc); err != nil {
			t.Fatal(err)
		}
		if st != "EXITED" || rc == nil || *rc != -1 {
			t.Errorf("pid=%s: status=%q rc=%v want (EXITED, -1)", pid, st, rc)
			continue
		}
		exited++
	}
	if exited != N {
		t.Errorf("converged rows: got %d want %d", exited, N)
	}

	// History stream must contain those audits — the live subscription
	// above just proved they were published, but we want to also pin
	// "they actually landed in history-<sid>" so a future regression
	// that flips publishAudit back to nc.Publish gets caught.
	js, _ := jetstream.New(nc)
	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{proto.SubjAuditProc("lab")},
	})
	if err != nil {
		t.Fatal(err)
	}
	streamReconciled := 0
	streamDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(streamDeadline) {
		batch, err := cons.Fetch(N+10, jetstream.FetchMaxWait(300*time.Millisecond))
		if err != nil {
			break
		}
		empty := true
		for msg := range batch.Messages() {
			empty = false
			if strings.Contains(string(msg.Data()), `"kind":"reconciled_closed"`) {
				streamReconciled++
			}
		}
		if empty {
			break
		}
		if streamReconciled >= N {
			break
		}
	}
	if streamReconciled < N {
		t.Errorf("history-lab reconciled_closed count: got %d want >= %d",
			streamReconciled, N)
	}
}

// TestG1AgentAppliesRevokePorts: end-to-end pin that the agent's
// applyReconciliation actually drops a state.json entry the broker
// flagged as REVOKED. We pre-seed state.json + a REVOKED row in the
// broker DB, boot a real agent against a real broker, and verify
// state.json no longer has the entry after register.
func TestG1AgentAppliesRevokePorts(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// Pre-seed a REVOKED port_allocations row + a state.json with the
	// matching token. The agent's home is a per-test tmpdir.
	rawToken := "revoked-token-aaaa"
	tokenHash := port.HashToken(rawToken)
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at)
		 VALUES (14088, 'lab', 'lab-1', 'rev-svc', 9000, ?, 'REVOKED', ?, ?, ?)`,
		tokenHash, fp, now.Add(-30*time.Minute), now,
	); err != nil {
		t.Fatal(err)
	}

	// Bootstrap state.json under a t.TempDir Home.
	home := t.TempDir()
	stateDir := home + "/agent/lab"
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stateJSON := []byte(`{"port_tokens":[{"name":"rev-svc","port":14088,"local_port":9000,"token":"` + rawToken + `"}]}`)
	if err := os.WriteFile(stateDir+"/state.json", stateJSON, 0o600); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Home:              home,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	// Wait for register + applyReconciliation to settle.
	deadline := time.Now().Add(2 * time.Second)
	gone := false
	for time.Now().Before(deadline) {
		body, _ := os.ReadFile(stateDir + "/state.json")
		if !strings.Contains(string(body), "rev-svc") {
			gone = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !gone {
		body, _ := os.ReadFile(stateDir + "/state.json")
		t.Errorf("state.json still references rev-svc after revoke: %s", body)
	}
}

// TestChaosBrokerRestartPreservesNodeTimer pins the architecture P8
// note: "ALLOCATED→REVOKED 的 15min OFFLINE 计时在 broker 重启后保留
// 剩余时间". A node that was ALREADY OFFLINE for >15min before the
// broker died must trigger port revocation immediately on the new
// broker's boot reconcile, without waiting another 15min.
func TestChaosBrokerRestartPreservesNodeTimer(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// Persist a node that's been OFFLINE for 30min.
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-30*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	rawToken := "preserve-timer-token"
	tokenHash := port.HashToken(rawToken)
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (14099, 'lab', 'lab-1', 'preserve', 9100, ?, 'ALLOCATED', ?, ?)`,
		tokenHash, fp, now.Add(-30*time.Minute),
	); err != nil {
		t.Fatal(err)
	}

	// Boot a fresh broker (= post-crash restart).
	defer startBroker(t, url, db)()

	// Without waiting for any ticker, the boot reconcilePorts should
	// have already moved 14099 to REVOKED.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		var st string
		if err := db.QueryRow(
			`SELECT state FROM port_allocations WHERE port=14099`,
		).Scan(&st); err == nil && st == "REVOKED" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var st string
	_ = db.QueryRow(`SELECT state FROM port_allocations WHERE port=14099`).Scan(&st)
	t.Errorf("port 14099 state after broker boot: got %q want REVOKED", st)
}

// TestChaosBrokerRestartDoesNotForgetExitedRows: cosmetic but
// load-bearing — restarting the broker must not lose the EXITED rows
// or revert them to RUNNING. v1 has no in-memory state to lose, but
// pinning this means future caching layers can't regress.
func TestChaosBrokerRestartDoesNotForgetExitedRows(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	rc := 0
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, ended_at, status, exit_code, started_by_fp)
		 VALUES ('01hz_already_done', 'lab', 'lab-1', '["true"]', ?, ?, 'EXITED', ?, ?)`,
		now.Add(-time.Minute), now, rc, fp,
	); err != nil {
		t.Fatal(err)
	}

	stop := startBrokerManual(t, url, db)
	stop() // cycle once
	defer startBroker(t, url, db)()

	var status string
	var rcOut *int
	if err := db.QueryRow(
		`SELECT status, exit_code FROM processes WHERE pid='01hz_already_done'`,
	).Scan(&status, &rcOut); err != nil {
		t.Fatal(err)
	}
	if status != "EXITED" || rcOut == nil || *rcOut != 0 {
		t.Errorf("EXITED row regressed across restart: status=%q rc=%v", status, rcOut)
	}
}

// TestG1AcceptedProcessesEcho: the broker's accepted_processes array
// must include any pid the agent claims to be running AND the broker
// also has as RUNNING. Verifies the no-op path doesn't accidentally
// transition matched rows.
func TestG1AcceptedProcessesEcho(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hz_keepme', 'lab', 'lab-1', '["sleep","9999"]', ?, 'RUNNING', ?)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01hz_keepme", State: "running"},
		},
	})
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.AcceptedProcesses) != 1 || resp.AcceptedProcesses[0] != "01hz_keepme" {
		t.Errorf("expected accepted=[01hz_keepme]; got %v", resp.AcceptedProcesses)
	}
	if len(resp.ReconciledProcesses) != 0 {
		t.Errorf("expected empty reconciled; got %v", resp.ReconciledProcesses)
	}

	// Row stays RUNNING.
	var st string
	if err := db.QueryRow(
		`SELECT status FROM processes WHERE pid='01hz_keepme'`,
	).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "RUNNING" {
		t.Errorf("RUNNING row mutated by accept: got %q", st)
	}
}

// TestG1AgentReportedExitFlowsToSQLite: agent says "I observed pid
// X exited with rc=42"; broker MUST mark the row EXITED with rc=42
// (not the missed-exit fallback rc=-1).
func TestG1AgentReportedExit(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hz_died_42', 'lab', 'lab-1', '["sh","-c","exit 42"]', ?, 'RUNNING', ?)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	rc := 42
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01hz_died_42", State: "exited", RC: &rc},
		},
	})
	nc, _ := nats.Connect(url)
	defer nc.Close()
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.ReconciledProcesses) != 1 ||
		resp.ReconciledProcesses[0].PID != "01hz_died_42" ||
		resp.ReconciledProcesses[0].RC != 42 {
		t.Errorf("expected reconciled=[(01hz_died_42, EXITED, 42)]; got %+v",
			resp.ReconciledProcesses)
	}

	var st string
	var rcOut *int
	if err := db.QueryRow(
		`SELECT status, exit_code FROM processes WHERE pid='01hz_died_42'`,
	).Scan(&st, &rcOut); err != nil {
		t.Fatal(err)
	}
	if st != "EXITED" || rcOut == nil || *rcOut != 42 {
		t.Errorf("expected EXITED rc=42; got status=%q rc=%v", st, rcOut)
	}
}

// TestG1MultipleNodesIsolated: two distinct nodes' RUNNING rows do
// NOT cross-contaminate each other on register. Node lab-1 registers
// with empty snapshot; node lab-2's RUNNING rows must STAY RUNNING.
func TestG1MultipleNodesIsolated(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	for _, nid := range []string{"lab-1", "lab-2"} {
		if _, err := db.Exec(
			`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
			 VALUES (?, 'lab', ?, 'OFFLINE', ?)`, nid, now.Add(-2*time.Minute), now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hz_node1', 'lab', 'lab-1', '["a"]', ?, 'RUNNING', ?),
		        ('01hz_node2', 'lab', 'lab-2', '["b"]', ?, 'RUNNING', ?)`,
		now, fp, now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
	})
	nc, _ := nats.Connect(url)
	defer nc.Close()
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}

	// lab-1's row must be EXITED.
	var st1 string
	if err := db.QueryRow(
		`SELECT status FROM processes WHERE pid='01hz_node1'`,
	).Scan(&st1); err != nil {
		t.Fatal(err)
	}
	if st1 != "EXITED" {
		t.Errorf("lab-1 row: got %q want EXITED", st1)
	}

	// lab-2's row MUST remain RUNNING.
	var st2 string
	if err := db.QueryRow(
		`SELECT status FROM processes WHERE pid='01hz_node2'`,
	).Scan(&st2); err != nil {
		t.Fatal(err)
	}
	if st2 != "RUNNING" {
		t.Errorf("lab-2 row cross-contaminated: got %q want RUNNING", st2)
	}
}

// TestG1AlreadyExitedRowsSkipped: a row already in EXITED state must
// NOT trigger a second reconciled_closed audit when the agent
// re-registers (idempotency).
func TestG1AlreadyExitedRowsSkipped(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	zero := 0
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, ended_at, status, exit_code, started_by_fp)
		 VALUES ('01hz_already_exited', 'lab', 'lab-1', '["true"]', ?, ?, 'EXITED', ?, ?)`,
		now.Add(-time.Minute), now, zero, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	reconciledCount := 0
	subAudit, _ := nc.Subscribe(proto.SubjAuditProc("lab"), func(m *nats.Msg) {
		if strings.Contains(string(m.Data), `"kind":"reconciled_closed"`) {
			reconciledCount++
		}
	})
	defer func() { _ = subAudit.Unsubscribe() }()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
	})
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}

	// Negative window: prove the broker did NOT publish a
	// reconciled_closed audit for an already-EXITED row. Wait long
	// enough that any erroneous publish would've gone through (200ms
	// covers register handler + audit publish even on a slow box),
	// then assert. The 200ms is documented as a *lower bound*, not a
	// guess: a true positive can't take longer than this register
	// round-trip.
	time.Sleep(200 * time.Millisecond)
	if reconciledCount != 0 {
		t.Errorf("expected 0 reconciled audits for already-EXITED row; got %d",
			reconciledCount)
	}
}

// TestG1KeepPortsForLiveAlloc: agent re-presents an ALLOCATED port —
// reply MUST list it under keep_ports (not revoke).
func TestG1KeepPortsForLiveAlloc(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-30*time.Second), now); err != nil {
		t.Fatal(err)
	}
	rawToken := "alive-token-cccc"
	tokenHash := port.HashToken(rawToken)
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (14066, 'lab', 'lab-1', 'live', 9200, ?, 'ALLOCATED', ?, ?)`,
		tokenHash, fp, now.Add(-time.Minute),
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		LocalPorts: []proto.LocalPort{
			{Port: 14066, Name: "live", LocalPort: 9200, TokenHash: tokenHash},
		},
	})
	nc, _ := nats.Connect(url)
	defer nc.Close()
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.KeepPorts) != 1 || resp.KeepPorts[0] != 14066 {
		t.Errorf("expected keep_ports=[14066]; got %v", resp.KeepPorts)
	}
	if len(resp.RevokePorts) != 0 {
		t.Errorf("expected empty revoke_ports; got %v", resp.RevokePorts)
	}
}

// TestG1PIDReuseTriggersReconcileAndOrphan covers the architecture
// G.1 PID-reuse path: SQLite has a RUNNING row with (boot_id_A,
// start_time_ticks_A); agent reports SAME ULID as running but with
// a DIFFERENT start_time_ticks. Broker MUST mark the original row
// EXITED(rc=-1, reconciled_closed) AND list the pid in
// drop_processes so the agent kills the new (unrelated) OS process
// that's currently squatting that pid.
func TestG1PIDReuseTriggersReconcileAndOrphan(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	// Seed RUNNING row with explicit triple data.
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp, boot_id, start_time_ticks)
		 VALUES ('01hz_reuse', 'lab', 'lab-1', '["sleep","9999"]', ?, 'RUNNING', ?, 'boot-A', 1000)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	// Agent reports SAME ULID + SAME boot_id but different start_time_ticks
	// → triple mismatch → PID reuse.
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		BootID:         "boot-A",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01hz_reuse", State: "running", StartTimeTicks: 9999},
		},
	})
	nc, _ := nats.Connect(url)
	defer nc.Close()
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}

	// Original row must be EXITED rc=-1 (reconciled_closed in audit).
	if len(resp.ReconciledProcesses) != 1 ||
		resp.ReconciledProcesses[0].PID != "01hz_reuse" ||
		resp.ReconciledProcesses[0].RC != -1 {
		t.Errorf("expected reconciled=[(01hz_reuse, EXITED, -1)] for PID reuse; got %+v",
			resp.ReconciledProcesses)
	}
	// New PID must be in drop_processes (agent kills the squatter).
	foundDrop := false
	for _, p := range resp.DropProcesses {
		if p == "01hz_reuse" {
			foundDrop = true
			break
		}
	}
	if !foundDrop {
		t.Errorf("expected 01hz_reuse in drop_processes after PID reuse; got %v",
			resp.DropProcesses)
	}
	if len(resp.AcceptedProcesses) != 0 {
		t.Errorf("PID-reuse path must NOT accept; got accepted=%v", resp.AcceptedProcesses)
	}

	var st string
	var rcOut *int
	if err := db.QueryRow(
		`SELECT status, exit_code FROM processes WHERE pid='01hz_reuse'`,
	).Scan(&st, &rcOut); err != nil {
		t.Fatal(err)
	}
	if st != "EXITED" || rcOut == nil || *rcOut != -1 {
		t.Errorf("PID-reuse: expected EXITED rc=-1; got status=%q rc=%v", st, rcOut)
	}
}

// TestG1TripleMatchKeepsRowRunning: same setup as the reuse test but
// with matching triple data → row stays RUNNING (acceptance path).
func TestG1TripleMatchKeepsRowRunning(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp, boot_id, start_time_ticks)
		 VALUES ('01hz_match', 'lab', 'lab-1', '["sleep","9999"]', ?, 'RUNNING', ?, 'boot-A', 5555)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		BootID:         "boot-A",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01hz_match", State: "running", StartTimeTicks: 5555},
		},
	})
	nc, _ := nats.Connect(url)
	defer nc.Close()
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.AcceptedProcesses) != 1 || resp.AcceptedProcesses[0] != "01hz_match" {
		t.Errorf("triple-match: expected accepted=[01hz_match]; got %v", resp.AcceptedProcesses)
	}
	if len(resp.ReconciledProcesses) != 0 {
		t.Errorf("triple-match: expected empty reconciled; got %v", resp.ReconciledProcesses)
	}

	var st string
	if err := db.QueryRow(
		`SELECT status FROM processes WHERE pid='01hz_match'`,
	).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != "RUNNING" {
		t.Errorf("triple-match row mutated: got %q", st)
	}
}

// TestG1MissingTripleFallsBackToAccept: when EITHER side has no
// triple data (exec children case; legacy rows; non-Linux agent),
// reconcile MUST keep the existing accept behavior. No defensive
// kills purely because /proc isn't available.
func TestG1MissingTripleFallsBackToAccept(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	// Row with NULL boot_id / start_time_ticks (legacy / exec child).
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hz_legacy', 'lab', 'lab-1', '["sleep","9999"]', ?, 'RUNNING', ?)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		BootID:         "boot-A",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01hz_legacy", State: "running", StartTimeTicks: 1234},
		},
	})
	nc, _ := nats.Connect(url)
	defer nc.Close()
	respMsg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	if !resp.OK {
		t.Fatalf("register rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.AcceptedProcesses) != 1 || resp.AcceptedProcesses[0] != "01hz_legacy" {
		t.Errorf("missing-triple: expected accepted=[01hz_legacy]; got %v",
			resp.AcceptedProcesses)
	}
	if len(resp.DropProcesses) != 0 {
		t.Errorf("missing-triple: expected empty drop_processes; got %v",
			resp.DropProcesses)
	}
}

// TestG1StartTimeTicksPersistedFromProcStarted pins the data path:
// agent's ProcStartedEvent now carries (boot_id, start_time_ticks),
// broker writes them to processes.boot_id / .start_time_ticks. The
// next register reconcile reads them back via proc.ListBySession and
// can compare. Without this wiring, the triple check above is dead
// code in production.
func TestG1StartTimeTicksPersistedFromProcStarted(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`, now, now); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	// Publish a synthetic proc.started event with the triple set.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	ev, _ := json.Marshal(proto.ProcStartedEvent{
		PID:            "01hz_persist_check",
		Argv:           []string{"sleep", "9999"},
		StartedAt:      now,
		StartedByFP:    fp,
		BootID:         "synthetic-boot",
		StartTimeTicks: 7777,
	})
	if err := nc.Publish(
		proto.SubjEvProc("lab", "lab-1", "01hz_persist_check", "started"), ev,
	); err != nil {
		t.Fatal(err)
	}
	// Wait for broker to handle the event.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		_ = db.QueryRow(
			`SELECT COUNT(*) FROM processes WHERE pid='01hz_persist_check'`,
		).Scan(&n)
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var bootID string
	var startTimeTicks int64
	if err := db.QueryRow(
		`SELECT boot_id, start_time_ticks FROM processes WHERE pid='01hz_persist_check'`,
	).Scan(&bootID, &startTimeTicks); err != nil {
		t.Fatal(err)
	}
	if bootID != "synthetic-boot" {
		t.Errorf("processes.boot_id: got %q want synthetic-boot", bootID)
	}
	if startTimeTicks != 7777 {
		t.Errorf("processes.start_time_ticks: got %d want 7777", startTimeTicks)
	}
}

// TestPsRecoversFromLOSTOnReconnect: a process row showing as LOST
// (RUNNING + node OFFLINE) flips back to RUNNING in `tether ps`
// once the agent re-registers AND reports the pid as still running.
func TestPsRecoversFromLOSTOnReconnect(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`, now.Add(-2*time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES ('01hz_recover', 'lab', 'lab-1', '["sleep","9999"]', ?, 'RUNNING', ?)`,
		now, fp,
	); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	// Step 1: confirm ps shows LOST.
	body, _ := json.Marshal(proto.PsReq{})
	respMsg, _ := nc.Request(proto.SubjCtrlPs(pub, "lab"), body, 2*time.Second)
	var resp proto.PsResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	if len(resp.Processes) != 1 || resp.Processes[0].Status != "LOST" {
		t.Fatalf("baseline: expected one LOST proc; got %+v", resp.Processes)
	}

	// Step 2: agent register with the pid claimed running.
	regBody, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01hz_recover", State: "running"},
		},
	})
	regResp, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), regBody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var regOut proto.NodeRegisterResp
	_ = json.Unmarshal(regResp.Data, &regOut)
	if !regOut.OK {
		t.Fatalf("register rejected: %s", regOut.Error)
	}

	// Step 3: ps now sees RUNNING (node went ONLINE on register).
	respMsg2, _ := nc.Request(proto.SubjCtrlPs(pub, "lab"), body, 2*time.Second)
	var resp2 proto.PsResp
	_ = json.Unmarshal(respMsg2.Data, &resp2)
	if len(resp2.Processes) != 1 || resp2.Processes[0].Status != "RUNNING" {
		t.Errorf("after register: expected RUNNING; got %+v", resp2.Processes)
	}
}
