// Package p9_test exercises the broker-local admin Unix socket
// (architecture I.2b / P9). Same in-process broker + agent pattern
// as test/p4..p8 — what's new is the AdminSocketPath wire-up and
// the admin client talking to it.
package p9_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// osListenUnix / osDialUnix are tiny indirections so the import
// group stays compact; they're literal one-liners over net.Listen /
// net.Dial.
func osListenUnix(path string) (net.Listener, error) { return net.Listen("unix", path) }
func osDialUnix(path string) (net.Conn, error)       { return net.Dial("unix", path) }

// ----- harness --------------------------------------------------------------

func startNATS(t *testing.T) string              { return testharness.StartNATS(t) }
func startJSNATS(t *testing.T) string            { return testharness.StartJSNATS(t) }
func openDB(t *testing.T) *sql.DB                { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                    { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

func seedSession(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, fp, "test-pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

// adminSocketPath returns a path inside a fresh, broker-created
// subdir of a SHORT temp dir so adminsock.Server.Start's mkdir(0o700)
// applies (the temp dir itself comes back 0o755, which would hide a
// regression where the broker fails to set 0o700 on its own freshly-
// created parent). os.MkdirTemp("", ...) rather than t.TempDir():
// the latter embeds the full test name, which on macOS pushes the
// path past the kernel's sun_path limit (104 bytes on Darwin, 108
// on Linux) and bind fails with EINVAL.
func adminSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "tether-run", "admin.sock")
}

// startBrokerWithAdmin boots a broker with the admin socket wired
// to socketPath. JS is enabled because the audit endpoint depends
// on it; tests that don't exercise audit can ignore that.
func startBrokerWithAdmin(t *testing.T, url string, db *sql.DB, socketPath string) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
		AdminSocketPath:   socketPath,
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
	// Poll a working admin call rather than just the file: a stale
	// socket file from a prior crash would satisfy os.Stat without
	// actually being bound. We use OpSessions because it's
	// always-available and cheap.
	c := &adminsock.Client{Path: socketPath, Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := c.Call(adminsock.Request{Op: adminsock.OpSessions}); err == nil && resp.Error == "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
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

// ----- tests ----------------------------------------------------------------

// TestAdminSocketPermissions: the socket file MUST be 0600 and its
// parent dir MUST be 0700. Architecture P9 / I.2b acceptance: only
// the broker's effective uid can connect because the OS rejects
// open() from any other uid before user code runs.
func TestAdminSocketPermissions(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	fi, err := os.Stat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if mode := fi.Mode().Perm(); mode != 0o600 {
		t.Errorf("socket file mode: got %o want 0600", mode)
	}
	parent, err := os.Stat(filepath.Dir(socketPath))
	if err != nil {
		t.Fatal(err)
	}
	if mode := parent.Mode().Perm(); mode != 0o700 {
		t.Errorf("socket parent dir mode: got %o want 0700", mode)
	}
}

// TestAdminRuntimeEndToEnd (R13) is the anti-"half-wired" proof for the runtime introspection verb:
// it boots a REAL broker (Run wires backend.RuntimeSnapshot = b.runtimeSnapshot and the real R7
// registry), then drives OpRuntime over the REAL admin socket and asserts (a) live process numbers,
// (b) the core reconcilers are present — proving the registry is actually reachable, and (c) a
// reconciler's last-tick ADVANCES across two calls — proving the value flows from the running
// registry through the socket, not from a stub or a one-shot snapshot.
func TestAdminRuntimeEndToEnd(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)
	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath, Timeout: time.Second}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("runtime: %s", resp.Error)
	}
	rep := resp.Runtime
	if rep == nil {
		t.Fatal("nil runtime report — the broker Run did not wire RuntimeSnapshot (half-wired)")
	}
	// Live values. This test shares the process with the in-proc broker, so runtime.NumGoroutine()
	// is directly comparable (± scheduling jitter).
	if rep.Goroutines < 2 {
		t.Errorf("goroutines=%d, want a live count", rep.Goroutines)
	}
	if live := runtime.NumGoroutine(); absI(rep.Goroutines-live) > 50 {
		t.Errorf("goroutines=%d far from in-process NumGoroutine=%d", rep.Goroutines, live)
	}
	if rep.Threads < 1 {
		t.Errorf("threads=%d, want >=1", rep.Threads)
	}
	if rep.OpenFDs < 1 {
		t.Errorf("open_fds=%d, want >=1 on linux", rep.OpenFDs)
	}
	if rep.RSSBytes < 1 {
		t.Errorf("rss=%d, want >0", rep.RSSBytes)
	}
	if rep.UptimeSeconds <= 0 {
		t.Errorf("uptime=%v, want >0", rep.UptimeSeconds)
	}
	// The REAL registry is reachable: the standing core passes appear.
	firstTick := map[string]time.Time{}
	for _, r := range rep.Reconcilers {
		firstTick[r.Name] = r.LastTick
	}
	for _, want := range []string{"node-states", "ports", "tunnel-sessions", "home-delivery"} {
		if _, ok := firstTick[want]; !ok {
			t.Errorf("reconciler %q missing (%+v) — the R7 registry is not wired to OpRuntime", want, rep.Reconcilers)
		}
	}
	// last-tick is LIVE: node-states fires every ReconcileInterval (50ms in this harness). Poll until
	// its LastTick advances past the first observation.
	deadline := time.Now().Add(3 * time.Second)
	advanced := false
	for time.Now().Before(deadline) && !advanced {
		r2, e := c.Call(adminsock.Request{Op: adminsock.OpRuntime})
		if e == nil && r2.Runtime != nil {
			for _, rr := range r2.Runtime.Reconcilers {
				if rr.Name == "node-states" && rr.LastTick.After(firstTick["node-states"]) {
					advanced = true
				}
			}
		}
		if !advanced {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !advanced {
		t.Errorf("node-states LastTick never advanced past %v — last-tick is not live through the socket", firstTick["node-states"])
	}
}

func absI(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// TestAdminSocketStaleSocketReclaim: a stale socket file from a
// previous broker crash MUST not block re-bind. The broker should
// unlink it and listen fresh.
func TestAdminSocketStaleSocketReclaim(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)

	// Manually pre-create the parent dir at 0700 + a stale Unix
	// socket file at socketPath. SetUnlinkOnClose(false) makes Go
	// leave the socket dirent behind on Close, exactly matching
	// the post-crash state (the kernel doesn't auto-reclaim
	// AF_UNIX bind paths on listener termination).
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		t.Fatal(err)
	}
	stub, err := osListenUnix(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if uln, ok := stub.(*net.UnixListener); ok {
		uln.SetUnlinkOnClose(false)
	}
	_ = stub.Close()
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("stale socket setup: %v", err)
	}

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpSessions})
	if err != nil {
		t.Fatalf("admin sessions after stale-reclaim: %v", err)
	}
	if resp.Error != "" {
		t.Errorf("admin sessions: %s", resp.Error)
	}
}

// TestAdminSessionsListsSeededRow round-trips one seeded session
// through the admin endpoint.
func TestAdminSessionsListsSeededRow(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpSessions})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("admin sessions: %s", resp.Error)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].SID != "lab" {
		t.Errorf("expected one session 'lab'; got %+v", resp.Sessions)
	}
	if resp.Sessions[0].OwnerFP != fp {
		t.Errorf("owner_fp: got %q want %q", resp.Sessions[0].OwnerFP, fp)
	}
	if resp.Sessions[0].State != "ACTIVE" {
		t.Errorf("state: got %q want ACTIVE", resp.Sessions[0].State)
	}
}

// TestAdminNodesShowsRegisteredAgent: a real agent registers, and
// `admin nodes` sees it.
func TestAdminNodesShowsRegisteredAgent(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()
	defer startAgent(t, url, "lab", "lab-1")()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpNodes})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("admin nodes: %s", resp.Error)
	}
	if len(resp.Nodes) != 1 || resp.Nodes[0].NID != "lab-1" {
		t.Fatalf("expected one node 'lab-1'; got %+v", resp.Nodes)
	}
	if resp.Nodes[0].Status != "ONLINE" {
		t.Errorf("status: got %q want ONLINE", resp.Nodes[0].Status)
	}
}

// TestAdminAuditTailsHistoryStream: drive a few exec calls, then
// admin audit tails them out of history-<sid>.
func TestAdminAuditTailsHistoryStream(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	socketPath := adminSocketPath(t)

	// Pre-create history stream so audit pubs land somewhere.
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	// Publish a synthetic audit.call so we don't have to spin up an
	// agent to drive a real exec.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	js, _ := jetstream.New(nc)
	for i := 0; i < 5; i++ {
		body, _ := json.Marshal(map[string]any{
			"v": 1, "kind": "call", "ts": time.Now().UTC().Format(time.RFC3339Nano),
			"session": "lab", "verb": "exec", "ok": true, "i": i,
		})
		if _, err := js.Publish(context.Background(), proto.SubjAuditCall("lab"), body); err != nil {
			t.Fatal(err)
		}
	}

	c := &adminsock.Client{Path: socketPath, Timeout: 5 * time.Second}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpAudit, SID: "lab", N: 10})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("admin audit: %s", resp.Error)
	}
	if len(resp.Audit) < 5 {
		t.Errorf("expected ≥ 5 audit entries; got %d", len(resp.Audit))
	}
	calls := 0
	for _, e := range resp.Audit {
		if strings.HasSuffix(e.Subject, ".audit.call") {
			calls++
		}
	}
	if calls < 5 {
		t.Errorf("expected ≥ 5 audit.call entries; got %d in %+v", calls, resp.Audit)
	}
}

// TestAdminAuditWithoutJetStreamFailsCleanly: when broker has no JS
// (the test/p4 / p5 / p6 default), audit endpoint MUST return a
// graceful error rather than crashing.
func TestAdminAuditWithoutJetStreamFailsCleanly(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpAudit, SID: "lab", N: 10})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == "" {
		t.Errorf("expected audit_unavailable error; got OK %+v", resp)
	}
}

// TestAdminEvictRemovesAgentRows: evict deletes both
// agent_provisioning and nodes rows.
func TestAdminEvictRemovesAgentRows(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	socketPath := adminSocketPath(t)

	// Seed an agent_provisioning row so evict has both rows to remove.
	agentPub, agentFP := freshUserPub(t)
	if err := agentprov.Provision(db, "lab", "lab-1", agentFP, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_ = agentPub
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`, now, now); err != nil {
		t.Fatal(err)
	}

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpEvict, SID: "lab", NID: "lab-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("admin evict: %s", resp.Error)
	}
	if resp.Evict == nil || !resp.Evict.AgentProvDeleted || !resp.Evict.NodeRowDeleted {
		t.Errorf("evict result: got %+v", resp.Evict)
	}

	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM agent_provisioning WHERE sid='lab' AND nid='lab-1'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("agent_provisioning row still present: %d", n)
	}
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM nodes WHERE sid='lab' AND nid='lab-1'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("nodes row still present: %d", n)
	}
}

// TestAdminEvictTriggersAgentShutdown: evict broadcasts sys.events
// {type:agent_evicted}; a live agent subscribed to that event MUST
// shut down within the architecture P9 1s budget.
func TestAdminEvictTriggersAgentShutdown(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	// Wait until the agent's register has actually committed a
	// nodes row; the subsequent evict assertion needs that row to
	// exist or it just no-ops with no broadcast (audit shard 05
	// F11: 300ms dead-reckon + 1s evict budget stacks budgets).
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpEvict, SID: "lab", NID: "lab-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("admin evict: %s", resp.Error)
	}
	if !resp.Evict.BroadcastedEvicted {
		t.Errorf("expected broadcast=true; got %+v", resp.Evict)
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Errorf("agent did not shut down within 1s of admin evict")
	}
}

// TestAdminEvictReapsManagedExecChild is the #26 regression: an evicted agent must REAP its managed OS
// children, not merely self-exit. On a bare setsid-nohup deploy (no systemd cgroup) a leaked child lingers
// in the HOST process table after the daemon exits. This drives a REAL managed exec (a sleep that records
// its own pid), evicts, and asserts the child pid is GONE from the host process table — the exit-assertion
// distinction of NOT just trusting the admin call's rc.
func TestAdminEvictReapsManagedExecChild(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	// A managed exec whose child records its own pid then `exec sleep` (the recorded pid IS the surviving
	// process). Publish straight onto the agent's forwarded-command subscription (what the broker forwards).
	dir, err := os.MkdirTemp("", "evictchild")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	pidFile := filepath.Join(dir, "child.pid")

	nc, _ := nats.Connect(url)
	defer nc.Close()
	body, _ := json.Marshal(proto.ExecReq{Argv: []string{"sh", "-c", "echo $$ > " + pidFile + "; exec sleep 120"}})
	if err := nc.PublishMsg(&nats.Msg{
		Subject: proto.SubjCmdForwarded("lab", "lab-1", "exec"),
		Reply:   nats.NewInbox(),
		Data:    body,
	}); err != nil {
		t.Fatal(err)
	}

	childPID := 0
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b, rerr := os.ReadFile(pidFile); rerr == nil {
			if p, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil && p > 0 {
				childPID = p
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("managed exec child never recorded its pid (did the forwarded exec dispatch?)")
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("baseline: managed child pid %d must be alive BEFORE evict, got %v", childPID, err)
	}

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpEvict, SID: "lab", NID: "lab-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" || resp.Evict == nil || !resp.Evict.BroadcastedEvicted {
		t.Fatalf("evict did not broadcast: %+v err=%v", resp.Evict, err)
	}

	// #26: the managed child must DISAPPEAR from the host process table (ESRCH), not just have the daemon
	// exit. SIGKILL delivery + the parent's reap are async, so poll.
	gone := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(childPID, 0); errors.Is(err, syscall.ESRCH) {
			gone = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !gone {
		// Best-effort cleanup so a failing run doesn't leak the sleep.
		_ = syscall.Kill(-childPID, syscall.SIGKILL)
		t.Fatalf("#26: managed exec child pid %d STILL present in the host process table after evict (leaked)", childPID)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("agent did not shut down after evict")
	}
}

// TestAdminUnknownOp pins the dispatch error path.
func TestAdminUnknownOp(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	c := &adminsock.Client{Path: socketPath}
	resp, err := c.Call(adminsock.Request{Op: "no-such-op"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Error, "unknown op") {
		t.Errorf("expected unknown-op error; got %+v", resp)
	}
}

// TestAdminRejectsMalformedRequest covers the json_parse error path.
func TestAdminRejectsMalformedRequest(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)

	defer startBrokerWithAdmin(t, url, db, socketPath)()

	conn, err := osDialUnix(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte("not-json\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	var resp adminsock.Response
	if err := json.Unmarshal(buf[:n-1], &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Error, "json_parse") {
		t.Errorf("expected json_parse error; got %+v", resp)
	}
}
