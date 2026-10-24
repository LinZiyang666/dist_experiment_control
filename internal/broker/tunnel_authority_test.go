package broker

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
)

// origin: hot-update S5 plan §4.3 / T29
func TestResumeAuthorityLookupStopsAndRejectsKnownLag(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseTunnelAuthority(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "bounded tunnel authority", beforeG)
	testharness.AssertNoFDLeak(t, "bounded tunnel authority", beforeFD, 0)
}

func exerciseTunnelAuthority(t *testing.T) {
	t.Helper()
	t.Run("server-configuration", func(t *testing.T) { exerciseTunnelAuthorityConfiguration(t) })
	for _, mode := range []string{"stop", "deadline", "budget", "already-stopped"} {
		t.Run("pool-"+mode, func(t *testing.T) { exerciseTunnelAuthorityPool(t, mode) })
	}
	t.Run("sqlite-lock", func(t *testing.T) { exerciseTunnelAuthorityLock(t) })
	t.Run("snapshot", func(t *testing.T) { exerciseTunnelAuthoritySnapshot(t) })
	t.Run("known-lag", func(t *testing.T) { exerciseTunnelAuthorityCluster(t) })
	t.Run("legacy-expiry", func(t *testing.T) { exerciseLegacyAuthorityExpiry(t) })
}

// origin: hot-update S5 plan §4.1 / §4.4 / T29
// Installing production callbacks must provide the issuer identity required
// by v1; otherwise Broker.Run fails before it can start the tunnel listener.
func exerciseTunnelAuthorityConfiguration(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return // Darwin installs no resumable owner, per the frozen S4 L6 rule.
	}
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger()}}
	server := tunnel.NewServer("127.0.0.1:0", "127.0.0.1", b.tunnelTokenLookup, silentLogger())
	defer server.Close()
	if err := configureTunnelAuthority(b, server); err != nil {
		t.Fatalf("production v1 authority configuration failed: %v", err)
	}
	if err := server.Start(t.Context()); err != nil {
		t.Fatalf("configured v1 listener failed to start: %v", err)
	}
}

func authorityFixture(t *testing.T, db *sql.DB) (*Broker, tunnel.AuthRequest) {
	t.Helper()
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "home"}
	seedHomedExpose(t, b, "lab", "lab-1", port.ProxyPortName, 14000, "authority-token-hash", "home", 4)
	for _, stmt := range []string{
		`UPDATE nodes SET proxy_capable=1 WHERE sid='lab' AND nid='lab-1'`,
		`UPDATE sessions SET proxy_enabled=1,proxy_epoch=7 WHERE sid='lab'`,
		`UPDATE proxy_meta SET generation=11 WHERE id=1`,
		`INSERT INTO cluster_meta(key,value) VALUES('applied_index','19')`,
		`INSERT INTO proxy_subscribers(sid,sub_id,name,token_hash,psk,cipher,created_by_fp) VALUES
		('lab','alice','alice','alice-token','first-password','aes-256-gcm','owner'),
		('lab','bob','bob','bob-token','second-password','aes-256-gcm','owner')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return b, tunnel.AuthRequest{Allocation: tunnel.Allocation{SID: "lab", NID: "lab-1", Port: 14000, TokenHash: "authority-token-hash", Epoch: 4}}
}

func authoritySQLWaiters() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "database/sql.(*DB).conn(")
}

func exerciseTunnelAuthorityPool(t *testing.T, mode string) {
	t.Helper()
	db := openDB(t)
	b, req := authorityFixture(t, db)
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	stop := make(chan struct{})
	req.Stop = stop
	waiters := authoritySQLWaiters()
	priorWaits := db.Stats().WaitCount
	switch mode {
	case "stop":
		req.Deadline = time.Now().Add(10 * time.Second)
	case "deadline":
		req.Deadline = time.Now().Add(50 * time.Millisecond)
	case "budget":
		req.Deadline = time.Now().Add(10 * time.Second)
	case "already-stopped":
		close(stop)
	}
	start := time.Now()
	finished := make(chan error, 1)
	go func() {
		grant, err := tunnelAuthorityLookup(b, req)
		if len(grant.Keys) != 0 || grant.Enabled {
			err = fmt.Errorf("blocked lookup returned authority")
		}
		finished <- err
	}()
	if mode != "already-stopped" {
		if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return db.Stats().WaitCount > priorWaits }) {
			t.Fatal("lookup never entered the blocked SQL pool")
		}
	}
	if mode == "stop" {
		close(stop)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, errTunnelAuthorityRetry) {
			t.Fatalf("blocked lookup error = %v, want try_again", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lookup did not stop and join within its operation budget")
	}
	limit := 500 * time.Millisecond
	if mode == "budget" {
		limit = tunnel.LookupBudget + 300*time.Millisecond
	}
	if elapsed := time.Since(start); elapsed > limit {
		t.Fatalf("%s cancellation took %s, limit %s", mode, elapsed, limit)
	}
	// Keep the fixture connection held while observing the returned operation:
	// releasing it first would let an abandoned uncancellable worker hide.
	if !testharness.WaitFor(t, 100*time.Millisecond, time.Millisecond, func() bool { return authoritySQLWaiters() == waiters }) {
		t.Fatal("authorization returned with an orphaned SQL worker still waiting")
	}
	if got := db.Stats().InUse; got != 1 {
		t.Fatalf("lookup retained a pooled connection: in-use=%d, want fixture's one", got)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	req.Stop, req.Deadline = nil, time.Time{}
	if _, err := tunnelAuthorityLookup(b, req); err != nil {
		t.Fatalf("lookup did not recover after pool release: %v", err)
	}
}

func exerciseTunnelAuthorityLock(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authority.db")
	seedBrokerFixtureDBFile(t, path)
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	b, req := authorityFixture(t, db)
	writer, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = writer.Exec("ROLLBACK") }()
	start := time.Now()
	req.Deadline = start.Add(100 * time.Millisecond)
	grant, err := tunnelAuthorityLookup(b, req)
	if !errors.Is(err, errTunnelAuthorityRetry) || grant.Enabled || len(grant.Keys) != 0 {
		t.Fatalf("locked SQL must refuse without a grant, error=%v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("SQLite busy handler exceeded lookup budget: %s", elapsed)
	}
	if _, err := writer.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	var busy int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&busy); err != nil || busy != 5000 {
		t.Fatalf("borrowed connection busy timeout not restored: busy=%d err=%v", busy, err)
	}
	req.Deadline = time.Time{}
	if _, err := tunnelAuthorityLookup(b, req); err != nil {
		t.Fatalf("lookup failed after SQL lock release: %v", err)
	}
}

func exerciseTunnelAuthoritySnapshot(t *testing.T) {
	t.Helper()
	db := openDB(t)
	b, req := authorityFixture(t, db)
	grant, err := tunnelAuthorityLookup(b, req)
	if err != nil || !grant.Enabled || grant.Allocation != req.Allocation || grant.Generation != 11 ||
		grant.ProxyEpoch != 7 || grant.AppliedRevision != 19 || len(grant.Keys) != 2 {
		t.Fatalf("complete authority snapshot missing fields: error=%v", err)
	}
	if grant.Keys[0].ID != "alice" || string(grant.Keys[0].Secret) != "first-password" ||
		grant.Keys[1].ID != "bob" || string(grant.Keys[1].Secret) != "second-password" {
		t.Fatal("authority omitted or changed active key material")
	}
	if err := b.tunnelTokenLookup("lab", "lab-1", 14000, "authority-token-hash", 4); err != nil {
		t.Fatalf("legacy validation differs from bounded validation: %v", err)
	}
	// Legacy's unhomed epoch rule cannot authorize an invented v1 tuple.
	if _, err := db.Exec(`UPDATE port_allocations SET home_broker=''`); err != nil {
		t.Fatal(err)
	}
	wrongEpoch := req
	wrongEpoch.Allocation.Epoch++
	if _, err := tunnelAuthorityLookup(b, wrongEpoch); !errors.Is(err, errTunnelAuthorityRevoked) {
		t.Fatalf("v1 authorized an unhomed allocation with the wrong epoch: %v", err)
	}
	if err := b.tunnelTokenLookup("lab", "lab-1", 14000, "authority-token-hash", wrongEpoch.Allocation.Epoch); err != nil {
		t.Fatalf("legacy unhomed epoch compatibility changed: %v", err)
	}

	// Hold one real WAL read snapshot while another connection changes both
	// keys and epoch. Then prove a fresh operation observes the whole new pair.
	path := filepath.Join(t.TempDir(), "snapshot.db")
	seedBrokerFixtureDBFile(t, path)
	wal, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal.Close() }()
	wb, wreq := authorityFixture(t, wal)
	ro, err := storage.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	wb.cfg.DB = ro
	tx, err := ro.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var pinned string
	if err := tx.QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&pinned); err != nil {
		t.Fatal(err)
	}
	update, err := wal.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = update.Rollback() }()
	for _, stmt := range []string{
		`UPDATE sessions SET proxy_epoch=8 WHERE sid='lab'`,
		`UPDATE proxy_subscribers SET state='REVOKED' WHERE sub_id='alice'`,
		`UPDATE proxy_subscribers SET psk='rotated-password' WHERE sub_id='bob'`,
		`UPDATE cluster_meta SET value='20' WHERE key='applied_index'`,
	} {
		if _, err := update.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := update.Commit(); err != nil {
		t.Fatal(err)
	}
	old, err := readTunnelAuthorityTx(t.Context(), wb, tx, wreq.Allocation)
	if err != nil || old.ProxyEpoch != 7 || old.AppliedRevision != 19 || len(old.Keys) != 2 || string(old.Keys[1].Secret) != "second-password" {
		t.Fatalf("read transaction mixed keyset and epoch: error=%v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	fresh, err := tunnelAuthorityLookup(wb, wreq)
	if err != nil || fresh.ProxyEpoch != 8 || fresh.AppliedRevision != 20 || len(fresh.Keys) != 1 || string(fresh.Keys[0].Secret) != "rotated-password" {
		t.Fatalf("fresh lookup reused an old keyset: error=%v", err)
	}
	for _, stmt := range []string{
		`UPDATE sessions SET proxy_enabled=0 WHERE sid='lab'`,
		`UPDATE sessions SET proxy_enabled=1,proxy_epoch=-1 WHERE sid='lab'`,
		`UPDATE sessions SET proxy_epoch=7; UPDATE proxy_meta SET generation=-1`,
		`UPDATE proxy_meta SET generation=11; UPDATE nodes SET status='OFFLINE'`,
		`UPDATE nodes SET status='ONLINE'; UPDATE sessions SET state='DELETING'`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
		if g, err := tunnelAuthorityLookup(b, req); !errors.Is(err, errTunnelAuthorityRevoked) || g.Enabled || len(g.Keys) != 0 {
			t.Fatalf("invalid or revoked snapshot returned authority: error=%v", err)
		}
	}
	if _, err := db.Exec(`UPDATE sessions SET state='ACTIVE'; DROP TABLE proxy_subscribers`); err != nil {
		t.Fatal(err)
	}
	if _, err := tunnelAuthorityLookup(b, req); !errors.Is(err, errTunnelAuthorityRetry) {
		t.Fatalf("SQL keyset failure must refuse as transient: %v", err)
	}
}

// The lag case uses real three-node Raft replication and a positive follower
// row. Holding its writer pool keeps a committed revocation out of SQLite while
// the other two members commit it; releasing the pool lets the same row revoke.
// origin: hot-update S5 plan §4.3 / lead Q1 / T29
func exerciseTunnelAuthorityCluster(t *testing.T) {
	t.Helper()
	transports := make([]*raft.InmemTransport, 3)
	peers := make([]raft.Server, 3)
	nodes := make([]*cluster.Node, 3)
	probes := make([]clusterharness.LeaderProbe, 3)
	for i := range transports {
		addr, transport := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("authority-%d", i)))
		transports[i] = transport
		peers[i] = raft.Server{ID: raft.ServerID(addr), Address: addr, Suffrage: raft.Voter}
	}
	for i, transport := range transports {
		for j, other := range transports {
			if i != j {
				transport.Connect(peers[j].Address, other)
			}
		}
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Shutdown()
			}
		}
	}()
	for i := range nodes {
		dir := t.TempDir()
		// origin: hot-update S6 plan §9.3 / TESTFIX; retain real storage policies
		// without replaying schema history for every independent authority cut.
		seedBrokerFixtureDBFile(t, filepath.Join(dir, "state.db"))
		n, err := cluster.New(cluster.Config{
			LocalID: peers[i].ID, DataDir: dir, DBPath: filepath.Join(dir, "state.db"),
			Transport: transports[i], BootstrapPeers: peers, Logger: silentLogger(),
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i], probes[i] = n, n
	}
	var follower int
	_, err := clusterharness.WithLeader(t, probes, 10*time.Second, func(leader int) error {
		follower = (leader + 1) % len(nodes)
		return nodes[leader].Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.NewCommand(cluster.OpReconcileBatch,
				cluster.Stmt(`INSERT OR IGNORE INTO sessions(sid,name,owner_pubkey_fp,pin_hash,proxy_enabled,proxy_epoch) VALUES('lab','lab','owner','hash',1,7)`),
				cluster.Stmt(`INSERT OR IGNORE INTO nodes(sid,nid,status,proxy_capable) VALUES('lab','lab-1','ONLINE',1)`),
				cluster.Stmt(`INSERT OR IGNORE INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,home_broker,epoch,created_at) VALUES(14000,'lab','lab-1','__proxy__',9000,'known-positive','ALLOCATED','owner','',0,CURRENT_TIMESTAMP)`),
				cluster.Stmt(`INSERT OR IGNORE INTO proxy_subscribers(sid,sub_id,name,token_hash,psk,cipher,created_by_fp) VALUES('lab','alice','alice','alice-token','password','aes-256-gcm','owner')`)), nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{cfg: Config{DB: nodes[follower].RODB(), Logger: silentLogger()}, clusterMode: true, cl: &clusterRuntime{node: nodes[follower]}}
	req := tunnel.AuthRequest{Allocation: tunnel.Allocation{SID: "lab", NID: "lab-1", Port: 14000, TokenHash: "known-positive"}}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		grant, err := tunnelAuthorityLookup(b, req)
		return err == nil && grant.Enabled && len(grant.Keys) == 1
	}) {
		t.Fatal("positive authority did not replicate to the follower")
	}
	held, err := nodes[follower].DB().Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN 39.F.
	// The previous agent cannot retry a transient REGISTER denial. Keep its
	// one request alive while a real unrelated committed write finishes apply,
	// then authorize from a fresh transaction within the original lookup budget.
	_, err = clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error {
		return nodes[leader].Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.NewCommand(cluster.OpReconcileBatch, cluster.Stmt(`UPDATE sessions SET name='lag-control' WHERE sid='lab'`)), nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		lag, err := nodes[follower].CommandApplyLagging()
		return err == nil && lag
	}) {
		t.Fatal("legacy REGISTER fixture has no committed apply lag")
	}
	legacyResult := make(chan error, 1)
	go func() { legacyResult <- b.tunnelTokenLookup("lab", "lab-1", 14000, "known-positive", 0) }()
	select {
	case err := <-legacyResult:
		t.Fatalf("legacy REGISTER abandoned or authorized known lag before apply: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-legacyResult:
		if err != nil {
			t.Fatalf("legacy REGISTER failed after benign apply caught up: %v", err)
		}
	case <-time.After(tunnel.LookupBudget + 300*time.Millisecond):
		t.Fatal("legacy REGISTER renewed its original authorization deadline")
	}
	held, err = nodes[follower].DB().Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error {
		if leader == follower {
			return errors.New("test follower became leader before lag injection")
		}
		return nodes[leader].Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.NewCommand(cluster.OpReconcileBatch, cluster.Stmt(`UPDATE sessions SET proxy_enabled=0,proxy_epoch=proxy_epoch+1 WHERE sid='lab' AND proxy_enabled=1`)), nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		lag, err := nodes[follower].CommandApplyLagging()
		return err == nil && lag
	}) {
		t.Fatal("fixture did not create real committed command lag")
	}
	var enabled, present int
	if err := b.cfg.DB.QueryRow(`SELECT proxy_enabled,(SELECT COUNT(*) FROM port_allocations WHERE token_hash='known-positive' AND state='ALLOCATED') FROM sessions WHERE sid='lab'`).Scan(&enabled, &present); err != nil || enabled != 1 || present != 1 {
		t.Fatalf("known-lag fixture lacks a positive authorization row: enabled=%d present=%d err=%v", enabled, present, err)
	}
	grant, err := tunnelAuthorityLookup(b, req)
	if !errors.Is(err, errTunnelAuthorityLag) || grant.Enabled || len(grant.Keys) != 0 {
		t.Fatalf("positive token on known-lagging replica must refuse: error=%v", err)
	}
	legacyStarted := time.Now()
	persistentLegacyResult := make(chan error, 1)
	persistentLegacyDone := make(chan struct{})
	go func() {
		defer close(persistentLegacyDone)
		persistentLegacyResult <- b.tunnelTokenLookup("lab", "lab-1", 14000, "known-positive", 0)
	}()
	defer func() { _ = held.Close(); <-persistentLegacyDone }()
	select {
	case err := <-persistentLegacyResult:
		if !errors.Is(err, errTunnelAuthorityLag) {
			t.Fatalf("legacy callback bypassed known-lag rejection: %v", err)
		}
	case <-time.After(tunnel.LookupBudget + 300*time.Millisecond):
		t.Fatal("legacy callback renewed its original authorization deadline")
	}
	if elapsed := time.Since(legacyStarted); elapsed > tunnel.LookupBudget+300*time.Millisecond {
		t.Fatalf("legacy callback exceeded its unchanged lookup budget: %s", elapsed)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		_, err := tunnelAuthorityLookup(b, req)
		return errors.Is(err, errTunnelAuthorityRevoked)
	}) {
		t.Fatal("applied revoke was not terminal after follower caught up")
	}
	if err := b.tunnelTokenLookup("lab", "lab-1", 14000, "known-positive", 0); !errors.Is(err, errTunnelAuthorityRevoked) {
		t.Fatalf("legacy REGISTER retried or hid a committed revoke: %v", err)
	}
}

// A final native SQL read can start before the absolute deadline and expire
// after prior known lag. That cancellation must preserve the last lag refusal
// without turning Stop, revocation or an unrelated store error into lag.
// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN run262.
func exerciseLegacyAuthorityExpiry(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"known-lag-deadline", "no-prior-lag-deadline", "stop-after-lag", "store-fault-after-lag", "revoke-after-lag", "already-expired"} {
		t.Run(mode, func(t *testing.T) {
			db := openDB(t)
			b, req := authorityFixture(t, db)
			b.clusterMode = true // The production missing-cluster gate refuses as lag.
			stop := make(chan struct{})
			req.Stop = stop
			req.Deadline = time.Now().Add(250 * time.Millisecond)
			if mode == "already-expired" {
				req.Deadline = time.Now().Add(-time.Millisecond)
			}
			calls := 0
			firstLag := false
			expiredRead := false
			start := time.Now()
			err := lookupLegacyTunnelTokenAttempts(req, func(next tunnel.AuthRequest) (tunnel.Authority, error) {
				calls++
				if calls == 1 && mode != "no-prior-lag-deadline" {
					grant, err := tunnelAuthoritySnapshotAttempt(b, next)
					if !errors.Is(err, errTunnelAuthorityLag) || grant.Enabled || len(grant.Keys) != 0 {
						t.Fatalf("first native read lacks real lag: %v", err)
					}
					firstLag = true
					if mode == "stop-after-lag" {
						close(stop)
					}
					return grant, err
				}
				if mode == "known-lag-deadline" || mode == "no-prior-lag-deadline" {
					held, err := db.Conn(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = held.Close() }()
					waits := db.Stats().WaitCount
					grant, err := tunnelAuthoritySnapshotAttempt(b, next)
					expiredRead = db.Stats().WaitCount > waits && !time.Now().Before(next.Deadline)
					if grant.Enabled || len(grant.Keys) != 0 {
						t.Fatal("blocked read returned authority")
					}
					return grant, err
				}
				b.clusterMode = false
				if mode == "store-fault-after-lag" {
					if _, err := db.Exec("DROP TABLE proxy_subscribers"); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "revoke-after-lag" {
					if _, err := db.Exec("UPDATE sessions SET proxy_enabled=0"); err != nil {
						t.Fatal(err)
					}
				}
				return tunnelAuthoritySnapshotAttempt(b, next)
			})
			want := errTunnelAuthorityRetry
			if mode == "known-lag-deadline" {
				want = errTunnelAuthorityLag
			}
			if mode == "revoke-after-lag" {
				want = errTunnelAuthorityRevoked
			}
			if !errors.Is(err, want) {
				t.Fatalf("%s refusal=%v, want %v", mode, err, want)
			}
			if mode == "known-lag-deadline" && (!firstLag || !expiredRead || calls != 2) {
				t.Fatal("deadline did not interrupt the second real SQL read after known lag")
			}
			if mode == "no-prior-lag-deadline" && (firstLag || !expiredRead || calls != 1) {
				t.Fatal("first expired read was incorrectly made known lag")
			}
			if mode == "already-expired" && calls != 0 {
				t.Fatal("expired request started an authority read")
			}
			if elapsed := time.Since(start); elapsed > 550*time.Millisecond {
				t.Fatalf("caller deadline renewed: %s", elapsed)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("expiry retained a SQL connection")
			}
		})
	}
}
