package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/raft"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// Reload arming is coalesced into Run. The historical serve cancel trigger is
// ignored so no ordinary-exit defer runs before the image exec safe point.
// origin: hot-update S6 plan §4 / T01

func TestRequestReExecArmsAndTriggers(t *testing.T) {
	b := &Broker{}
	if b.ReExecRequested() {
		t.Fatal("a fresh broker must not have re-exec armed")
	}
	triggered := 0
	b.SetReloadTrigger(func() { triggered++ })
	b.RequestReExec()
	if !b.ReExecRequested() {
		t.Fatal("RequestReExec must arm the trampoline flag serve.go checks after Run")
	}
	if triggered != 0 {
		t.Fatalf("reload cancelled Run %d times", triggered)
	}
	b.RequestReExec()
	select {
	case <-brokerReloadRequests(b):
	default:
		t.Fatal("reload did not wake Run")
	}
	select {
	case <-brokerReloadRequests(b):
		t.Fatal("reload queued a duplicate cut")
	default:
	}
}

func TestRequestReExecNilTriggerSafe(t *testing.T) {
	b := &Broker{}
	b.RequestReExec() // no trigger set (e.g. a test / pre-Run) → must not panic
	if !b.ReExecRequested() {
		t.Fatal("re-exec must still be armed even with no trigger wired")
	}
}

// Staged SHA, running SHA, fresh topology, operation census, then arm.

func TestReloadAlreadyAtVersionSkips(t *testing.T) {
	b := &clusterAdminBackend{}
	sha, err := onDiskBinarySHA256()
	if err != nil {
		t.Fatal(err)
	}
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: proto.ReleaseVersion, ExpectSHA256: sha})
	// origin: S8 internal review rv3-leaves F2
	if !brokerimage.ReloadAlreadyRunning(proto.ReleaseVersion, sha, sha) {
		t.Fatal("identical running bytes lost the pure idempotency check")
	}
	if resp.OK || resp.AlreadyAtVersion || resp.Reloading || resp.Code != "try_again" {
		t.Fatalf("legacy no-op lacks fresh transaction/control proof: %+v", resp)
	}
}

func TestReloadRefusesOnLeader(t *testing.T) {
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
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseReloadMultipleVoters(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "broker reload topology", g)
	testharness.AssertNoFDLeak(t, "broker reload topology", fd, 0)
}

func exerciseReloadMultipleVoters(t *testing.T) {
	t.Helper()
	b, _ := brokerReloadLeaderFixture(t)

	sha, err := onDiskBinarySHA256()
	if err != nil {
		t.Fatal(err)
	}
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha})
	// origin: S8 internal review rv3-leaves F2
	if !errors.Is(brokerimage.CheckReloadRole(b.admin.Node), brokerimage.ErrReloadLeader) {
		t.Fatal("multiple-voter leader lost its independent role refusal")
	}
	if resp.OK || resp.Reloading || resp.Code != "try_again" {
		t.Fatalf("multiple-voter legacy reload bypassed policy: %+v", resp)
	}
}

func TestReloadShaMismatchRefuses(t *testing.T) {
	b := &clusterAdminBackend{requestReExec: func() {}} // admin nil → non-leader path; reaches the sha gate
	resp := b.handleBrokerUpgradeReload(adminsock.Request{
		Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999",
		ExpectSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if resp.Code != adminsock.CodeShaMismatch {
		t.Fatalf("a stale/unstaged on-disk binary must refuse the reload; got %+v", resp)
	}
}

func TestReloadArmsReExecOnSuccess(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	sha, err := onDiskBinarySHA256() // the on-disk digest MUST match (the sha guard is mandatory)
	if err != nil {
		t.Fatalf("hash self: %v", err)
	}
	fired := make(chan struct{})
	b := &clusterAdminBackend{requestReExec: func() { close(fired) }}
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha})
	// origin: S8 internal review rv3-leaves F2
	if resp.OK || resp.Reloading || resp.Code != "try_again" {
		t.Fatalf("a staged SHA alone authorized legacy reload: %+v", resp)
	}
	select {
	case <-fired:
		t.Fatal("legacy reload armed an execution without a grant")
	case <-time.After(2 * time.Second):
	}
}

func TestReloadUnwiredReplies(t *testing.T) {
	sha, err := onDiskBinarySHA256()
	if err != nil {
		t.Fatalf("hash self: %v", err)
	}
	b := &clusterAdminBackend{} // requestReExec nil (single mode / not wired)
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha})
	// origin: S8 internal review rv3-leaves F2
	if resp.OK || resp.Reloading || resp.Code != "try_again" {
		t.Fatalf("an unwired legacy reload bypassed policy: %+v", resp)
	}
}

// TestReloadRefusesEmptySHA pins the External-review fail-closed fix: an unguarded reload (no expect_sha256)
// must be REFUSED, never re-exec whatever happens to be on disk. Idempotency/leader gates fire first, so
// use a non-idempotent ToVersion and a non-leader (admin nil) backend to reach the sha gate.
func TestReloadRefusesEmptySHA(t *testing.T) {
	armed := false
	b := &clusterAdminBackend{requestReExec: func() { armed = true }}
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999"})
	if resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("an unguarded reload (empty expect_sha256) must be refused with bad_request; got %+v", resp)
	}
	if armed {
		t.Fatal("an unguarded reload must NOT arm the re-exec")
	}
}

// origin: hot-update S6 plan §5 / T37
func TestReloadAllowsSoleVoter(t *testing.T) {
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
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseReloadSoleVoter(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "broker reload topology", g)
	testharness.AssertNoFDLeak(t, "broker reload topology", fd, 0)
}

func exerciseReloadSoleVoter(t *testing.T) {
	t.Helper()
	backend, n := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	if voters, err := n.NumVoters(); err != nil || voters != 1 {
		t.Fatalf("N1 fixture: %d, %v", voters, err)
	}
	armed := false
	backend.requestReExec = func() { armed = true }
	sha, err := onDiskBinarySHA256()
	if err != nil {
		t.Fatal(err)
	}
	resp := backend.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha})
	// origin: S8 internal review rv3-leaves F2
	if err := brokerimage.CheckReloadRole(n); err != nil {
		t.Fatalf("sole voter lost its independent role eligibility: %v", err)
	}
	if resp.OK || resp.Reloading || armed || resp.Code != "try_again" {
		t.Fatalf("sole voter legacy reload bypassed policy: %+v armed=%v", resp, armed)
	}
}

// A sole voter admitted before a join cannot reuse a later role sample to cut
// the changed cluster. Refusal must leave its original SQLite writer usable.
// origin: hot-update S6 plan §5 / T38 / internal review rv-cluster-7
func TestReloadAdmissionRejectsChangedMembership(t *testing.T) {
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
				exerciseReloadAdmissionMembership(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "reload admission membership x5", g)
	testharness.AssertNoFDLeak(t, "reload admission membership x5", fd, 0)
}

func exerciseReloadAdmissionMembership(t *testing.T) {
	t.Helper()
	_, n := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	b := &Broker{cfg: Config{DB: n.RODB(), Now: time.Now}, selfID: "brk-a", clusterMode: true, cl: &clusterRuntime{node: n}}
	now, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	if err := captureBrokerReloadAdmission(b, adminsock.Request{}, controlbudget.Deadline(now+int64(5*time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := validateBrokerReloadRole(b, false); err != nil {
		t.Fatalf("unchanged sole voter refused: %v", err)
	}
	if err := n.AddNonvoter("joining", "joining:7400"); err != nil {
		t.Fatal(err)
	}
	if err := validateBrokerReloadRole(b, false); err == nil {
		t.Fatal("membership changed after admission but reload remained allowed")
	}
	if err := n.DB().Ping(); err != nil {
		t.Fatalf("refused cut closed original writer: %v", err)
	}
}

// origin: hot-update S6 plan §5 / T01
func TestReloadSameReleaseRequiresRunningSHA(t *testing.T) {
	if brokerimage.ReloadAlreadyRunning(proto.ReleaseVersion, "staged-new", "running-old") {
		t.Fatal("same release skipped a different artifact")
	}
	if !brokerimage.ReloadAlreadyRunning(proto.ReleaseVersion, "ABC", "abc") {
		t.Fatal("identical running SHA did not skip")
	}
	if brokerimage.ReloadAlreadyRunning(proto.ReleaseVersion, "staged", "") {
		t.Fatal("unknown running image skipped reload")
	}
}

// origin: hot-update S6 plan §3.2 / T01
func TestBrokerFinalSealRefusesUnknownOwners(t *testing.T) {
	seal := storage.StoreSeal{Stamp: storage.StoreStamp{Journal: "wal"}, Writer: storage.CloseClosed, ReadOnly: storage.CloseClosed, Bolt: storage.CloseClosed, Transport: storage.CloseClosed, Raft: storage.CloseClosed}
	if !brokerSealClosed(seal) {
		t.Fatal("known closed seal refused")
	}
	mutants := []func(*storage.StoreSeal){
		func(s *storage.StoreSeal) { s.Writer = storage.CloseUnknown },
		func(s *storage.StoreSeal) { s.ReadOnly = storage.CloseStopped },
		func(s *storage.StoreSeal) { s.Bolt = storage.CloseOpen },
		func(s *storage.StoreSeal) { s.Transport = storage.CloseStopped },
		func(s *storage.StoreSeal) { s.Raft = storage.CloseOpen },
		func(s *storage.StoreSeal) { s.DriverWriters = 1 },
		func(s *storage.StoreSeal) { s.DriverReaders = 1 },
		func(s *storage.StoreSeal) { s.ActiveRPCs = 1 },
		func(s *storage.StoreSeal) { s.ActiveBackups = 1 },
		func(s *storage.StoreSeal) { s.CheckpointBusy = 1 },
		func(s *storage.StoreSeal) { s.CheckpointLog = 1 },
		func(s *storage.StoreSeal) { s.CheckpointDone = 1 },
		func(s *storage.StoreSeal) { s.Stamp.Journal = "" },
	}
	for i, mutate := range mutants {
		bad := seal
		mutate(&bad)
		if brokerSealClosed(bad) {
			t.Fatalf("unsafe seal %d accepted", i)
		}
	}
}

// Connected real voters keep the production lease valid while the race test
// binary is hashed. A disconnected second voter tests quorum loss, not the
// intended fresh multiple-voter leader refusal.
func brokerReloadLeaderFixture(t *testing.T) (*clusterAdminBackend, *cluster.Node) {
	t.Helper()
	var peers []raft.Server
	var transports []*raft.InmemTransport
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("reload-%d", i)
		addr, transport := raft.NewInmemTransport(raft.ServerAddress(id + ":7400"))
		transports = append(transports, transport)
		peers = append(peers, raft.Server{ID: raft.ServerID(id), Address: addr, Suffrage: raft.Voter})
	}
	for i, transport := range transports {
		for j, peer := range transports {
			if i != j {
				transport.Connect(peers[j].Address, peer)
			}
		}
	}
	nodes := make([]*cluster.Node, 0, len(peers))
	for i, peer := range peers {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.db")
		seedBrokerFixtureDBFile(t, path)
		node, err := cluster.New(cluster.Config{LocalID: peer.ID, DataDir: dir, DBPath: path, Transport: transports[i], BootstrapPeers: peers,
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout, LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Shutdown() })
		nodes = append(nodes, node)
	}
	// WaitForLeader means "this node is leader", not "the cluster elected".
	// Observe every connected voter so a legitimate election of node 1/2 is not
	// turned into a fixture timeout. Production election timers remain unchanged.
	var selected *cluster.Node
	// origin: hot-update S6 plan §5 / T38; observe leadership only in polling predicates.
	if !testharness.WaitFor(t, 30*time.Second, 20*time.Millisecond, func() bool {
		for _, node := range nodes {
			if node.IsLeader() {
				selected = node
				return true
			}
		}
		return false
	}) {
		t.Fatal("no multiple-voter leader")
	}
	return &clusterAdminBackend{admin: NewClusterAdmin(selected, nil)}, selected
}

// origin: hot-update S6 plan §0 exit 4 / T01
func TestSoleVoterAdmissionDoesNotPollRemoteCapabilities(t *testing.T) {
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
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseSoleVoterAdmissionPoll(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "sole voter capability admission", g)
	testharness.AssertNoFDLeak(t, "sole voter capability admission", fd, 0)
}
func exerciseSoleVoterAdmissionPoll(t *testing.T) {
	t.Helper()
	backend, _ := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	polls := 0
	backend.admin.HealthPoll = func() map[string]proto.ClusterHealthResp { polls++; return nil }
	if err := assertAllVotersSupportSessionCreatorOps(backend.admin, "session-allow"); err != nil || polls != 0 {
		t.Fatalf("sole voter waited for remote capabilities: polls=%d err=%v", polls, err)
	}
}
