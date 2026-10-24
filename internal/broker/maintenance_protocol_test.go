package broker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The proof floor can advance after PREPARED. A follower must observe the real
// later command before ACK, while retaining the issuer's original cut lifetime.
// origin: hot-update S6 plan §7.1 / T39 / N3 maintenance integration
func TestMaintenanceProofWaitsForLocalCommandCursor(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	// Independent fixtures overlap election waits; this parent joins every
	// round and its cleanup before the process-wide leak assertions.
	// origin: hot-update S6 plan §9.3 / TESTFIX
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				exerciseMaintenanceAppliedCursor(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "maintenance applied cursor x5", g)
	testharness.AssertNoFDLeak(t, "maintenance applied cursor x5", fd, 0)
}

func exerciseMaintenanceAppliedCursor(t *testing.T) {
	t.Helper()
	b, n := newClusterProxyBroker(t, "cursor", "owner")
	before, err := n.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- n.Propose(func(*sql.DB) (*cluster.Command, error) {
			close(entered)
			<-release
			return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES('maintenance-cursor-witness','applied')`)), nil
		})
	}()
	<-entered
	queryDone := make(chan error, 1)
	go func() { queryDone <- awaitBrokerMaintenanceCursor(b, before+1) }()
	released, joined := false, false
	defer func() {
		if !released {
			close(release)
		}
		if !joined {
			<-writeDone
			<-queryDone
		}
	}()
	select {
	case err := <-queryDone:
		close(release)
		released = true
		<-writeDone
		joined = true
		t.Fatalf("proof did not wait for the actual SQL command: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	released = true
	writeErr, queryErr := <-writeDone, <-queryDone
	joined = true
	if writeErr != nil || queryErr != nil {
		t.Fatalf("cursor write/wait: %v / %v", writeErr, queryErr)
	}
	var value string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='maintenance-cursor-witness'`).Scan(&value); err != nil || value != "applied" {
		t.Fatalf("cursor receipt had no real command: %v %q", err, value)
	}
}

// Real broker and agent subscription handlers exchange the shared signed codec.
// The incapable route must refuse before PREPARED, while the capable route must
// really install its hold before ACTIVE and admission can be acknowledged.
// origin: hot-update S6 internal review rv-cluster-3
func TestMaintenanceCutRequiresEveryInstalledObserver(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	// Independent fixtures overlap election waits; this parent joins every
	// round and its cleanup before the process-wide leak assertions.
	// origin: hot-update S6 plan §9.3 / TESTFIX
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				exerciseInstalledMaintenanceObservers(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "maintenance protocol x5", beforeG)
	testharness.AssertNoFDLeak(t, "maintenance protocol x5", beforeFD, 0)
}

func exerciseInstalledMaintenanceObservers(t *testing.T) {
	t.Helper()
	b, n := newClusterProxyBroker(t, "lab", "owner")
	issuer, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := auth.PublicKeyFromSeed(seed)
	issuerPub, _ := auth.PublicKeyFromSeed(issuer)
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: "maintenance-live", Nkeys: []*natsserver.NkeyUser{{Nkey: pub}, {Nkey: issuerPub}}})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(server.ClientURL(), nats.Nkey(issuerPub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(issuer, nonce) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	b.nc.Store(nc)
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: issuer}
	seedClusterNode(t, &Broker{cfg: Config{DB: n.DB()}}, b.selfID, server.Name(), "127.0.0.1:7443", "sha256:fixture", "VOTER")
	fp, _ := auth.FingerprintFromActor(pub)
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpAgentProvision, cluster.Stmt(`INSERT INTO agent_provisioning(sid,nid,agent_fp) VALUES('lab','capable',`+cluster.MustLitText(fp)+`)`)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := InitializeBrokerImageStatus(b, 3, ""); err != nil {
		t.Fatal(err)
	}
	setBrokerImagePhase(b, proto.BrokerImagePhaseServing)
	trigger, err := b.SubscribeClusterUpgradeTrigger(nc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.Unsubscribe() })
	register, err := nc.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", b.handleRegister)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = register.Unsubscribe() })
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(agent.Config{NATSURL: server.ClientURL(), SID: "lab", NID: "capable", Home: t.TempDir(), ProxyOptOut: true, AccountPub: issuerPub, Identity: &cli.Identity{Seed: seed, PublicKey: pub}, Logger: silentLogger(), RegisterTimeout: time.Second, RosterRefreshInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("agent did not join")
		}
	})
	// A claim-probe reply witnesses the production forwarded subscription, not
	// merely CONNECT or a row written before the register response is applied.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := nc.Request(proto.SubjCmdForwarded("lab", "capable", proto.ClaimProbeVerb), nil, 50*time.Millisecond)
		if err == nil {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("agent command subscription missing: %v", err)
		}
	}
	if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return cluster.PlanSetUpgradeActiveExpected(db, "holder-live", b.cfg.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "incapable", ProtoVersion: proto.ProtoVersion, NatsServer: server.Name()}, b.cfg.Now())
	}); err != nil {
		t.Fatal(err)
	}
	req := &proto.ClusterUpgradeReq{Op: "maintenance", TargetNode: b.selfID, TransferTo: b.selfID, Holder: "holder-live", CutID: "cut-live"}
	if response := establishBrokerMaintenance(b, req); response.OK || !strings.Contains(response.Error, "maintenance_observer_unsupported") {
		t.Fatalf("incapable route admitted: %+v", response)
	}
	if _, err := cluster.ReadBrokerMaintenance(n.RODB(), b.selfID); err != sql.ErrNoRows {
		t.Fatalf("refusal changed maintenance authority: %v", err)
	}
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return node.PlanEvict("lab", "incapable") }); err != nil {
		t.Fatal(err)
	}
	response := establishBrokerMaintenance(b, req)
	if !response.OK || response.Maintenance == nil || response.Maintenance.Phase != brokermaint.MaintActive {
		t.Fatalf("capable real observers refused: %+v", response)
	}
	if !proxyHomeMaintained(b, b.selfID) {
		t.Fatal("installed voter proof did not protect home")
	}
	now, _ := procrequest.BootNow()
	request := adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, Holder: req.Holder, CutID: req.CutID}
	if err := captureBrokerReloadAdmission(b, request, controlbudget.Deadline(now+int64(5*time.Second))); err != nil {
		t.Fatalf("aggregate did not admit target: %v", err)
	}
	if err := validateBrokerReloadRole(b, false); err != nil {
		t.Fatalf("admission role refused: %v", err)
	}
	history, err := ExportHomeMaintenanceHistory(b)
	if err != nil || history.Cache == nil || len(history.Cache.Observations) != 1 {
		t.Fatalf("real proof cache was not exported: %v", err)
	}
	successor := &Broker{cfg: Config{DB: n.RODB(), AuthCallout: b.cfg.AuthCallout, Logger: silentLogger()}, cl: &clusterRuntime{node: n}, selfID: b.selfID, clusterMode: true}
	if err := RestoreHomeMaintenanceHistory(successor, history); err != nil {
		t.Fatalf("real proof cache refused restore: %v", err)
	}
	if !proxyHomeMaintained(successor, b.selfID) {
		t.Fatal("restored cache lost the original protected deadline")
	}
	restored, err := ExportHomeMaintenanceHistory(successor)
	if err != nil || restored.Cache.Observations[0].Lease.ExpiresLocalBootNS != history.Cache.Observations[0].Lease.ExpiresLocalBootNS {
		t.Fatal("restore rebased the original proof deadline")
	}
	// Sealed cut lineage cannot authorize beyond the original local proof or
	// separate an aggregate deadline from the exact accepted record.
	// origin: hot-update S6 plan §2.2 / §7.1 / T39
	for _, mutate := range []func(*HomeMaintenanceHistory){
		func(h *HomeMaintenanceHistory) { h.CutDeadline = h.Cache.Observations[0].Lease.ExpiresLocalBootNS + 1 },
		func(h *HomeMaintenanceHistory) { h.CutRecord.CutID = "" },
		func(h *HomeMaintenanceHistory) { h.CutRecord.Home = "another-home" },
	} {
		bad := history
		mutate(&bad)
		fresh := &Broker{cfg: Config{DB: n.RODB(), AuthCallout: b.cfg.AuthCallout, Logger: silentLogger()}, cl: &clusterRuntime{node: n}, selfID: b.selfID, clusterMode: true}
		if err := RestoreHomeMaintenanceHistory(fresh, bad); !errors.Is(err, brokermaint.ErrIdentity) {
			t.Fatalf("invalid accepted cut history restored: %v", err)
		}
	}
	b.cl.homeMaintenance.mu.Lock()
	originalDeadline := b.cl.homeMaintenance.cutDeadline
	b.cl.homeMaintenance.cutDeadline = now
	b.cl.homeMaintenance.mu.Unlock()
	if err := validateBrokerReloadRole(b, false); err == nil {
		t.Fatal("expired aggregate admitted the cut")
	}
	b.cl.homeMaintenance.mu.Lock()
	b.cl.homeMaintenance.cutDeadline = originalDeadline
	b.cl.homeMaintenance.mu.Unlock()
	if err := n.AddNonvoter("new-member", "new-member"); err != nil {
		t.Fatal(err)
	}
	if err := validateBrokerReloadRole(b, false); err == nil {
		t.Fatal("membership changed after admission but cut remained allowed")
	}
}
