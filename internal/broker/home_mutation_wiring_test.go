package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
)

// Production never calls ConfigureHomeMaintenance (grep: only tests do), so a
// cluster broker built the way Run builds it keeps nil P14 planners. Every
// guarded home writer then fails closed with "not implemented (S6 P14)": the
// initial expose allocation, reaper rotation and every rehome/rebalance.
// origin: hot-update S6 internal review rv-cluster
func TestClusterHomeMutatorsCommitWithProductionWiring(t *testing.T) {
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
				exerciseProductionHomeMutators(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "home mutation review x5", beforeG)
	testharness.AssertNoFDLeak(t, "home mutation review x5", beforeFD, 0)
}
func exerciseProductionHomeMutators(t *testing.T) {
	t.Helper()

	f := newFlowReaperFixture(t, 14620, 14630)
	if err := f.b.moveProxyHomeTo(f.b.nc.Load(), f.alloc, "brk-b", nil, 0); err != nil {
		t.Errorf("production-wired rehome/rebalance failed: %v", err)
	}
	if _, err := f.b.allocatePort("lab", "node", "svc", 8080, 0, false, "", "owner-fp"); err != nil {
		t.Errorf("production-wired cluster expose allocation failed: %v", err)
	}
	// Root-cause control: the same move commits once the real P14 planners are installed.
	plans := HomeMutationPlanners{Reassign: port.PlanReassignHomeGuarded, Allocate: port.PlanAllocateGuarded,
		Rotate: port.PlanRotateAllocationGuarded, Free: port.PlanFreeAllocationGuarded}
	if err := ConfigureHomeMaintenance(f.b, plans, func() map[string]brokermaint.LocalLease { return nil },
		func() (int64, error) { return int64(1000 * time.Second), nil }); err != nil {
		t.Fatal(err)
	}
	current, err := port.LookupProxyByNode(f.b.cfg.DB, f.alloc.SID, f.alloc.NID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.b.moveProxyHomeTo(f.b.nc.Load(), current, "brk-b", nil, 0); err != nil {
		t.Fatalf("control: real planners still fail: %v", err)
	}
}

// Drain and retire must use the same proof cache as manual/automatic rehome.
// Both source and destination are guarded, and deferral emits no move event.
// origin: hot-update S6 plan §7.2 / T41 / internal review rv-cluster-1
func TestMaintenanceDrainGuardsBothHomes(t *testing.T) {
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
				exerciseMaintainedDrain(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "maintained drain x5", g)
	testharness.AssertNoFDLeak(t, "maintained drain x5", fd, 0)
}

func exerciseMaintainedDrain(t *testing.T) {
	t.Helper()
	f := newFlowReaperFixture(t, 14720, 14730)
	plans := HomeMutationPlanners{Reassign: port.PlanReassignHomeGuarded, Allocate: port.PlanAllocateGuarded, Rotate: port.PlanRotateAllocationGuarded, Free: port.PlanFreeAllocationGuarded}
	proofs := map[string]brokermaint.LocalLease{}
	if err := ConfigureHomeMaintenance(f.b, plans, func() map[string]brokermaint.LocalLease { return proofs }, func() (int64, error) { return int64(time.Second), nil }); err != nil {
		t.Fatal(err)
	}
	a := NewClusterAdmin(nil, silentLogger())
	a.Node = f.b.cl.node
	a.HomeReassign = func(db *sql.DB, expected *port.Allocation, target string) (int64, *cluster.Command, error) {
		return planHomeReassign(f.b, db, expected, target)
	}
	events := 0
	a.EmitEvent = func(string, map[string]any) { events++ }
	for _, home := range []string{"brk-a", "brk-b"} {
		lease := maintainedHomeLease(home)
		proofs[home] = lease
		raw, _ := json.Marshal(lease.Record)
		if err := f.b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+cluster.MustLitText(homeMaintenancePrefix+home)+`,`+cluster.MustLitText(string(raw))+`)`)), nil
		}); err != nil {
			t.Fatal(err)
		}
		var deferred *HomeMaintenanceDeferredError
		if err := a.MigrateExposes("brk-a"); !errors.As(err, &deferred) || deferred.Home != home {
			t.Fatalf("drain bypassed %s proof: %v", home, err)
		}
		current, err := port.LookupByTokenHash(f.b.cfg.DB, f.alloc.TokenHash)
		if err != nil || !sameHomeAllocation(current, f.alloc) || events != 0 {
			t.Fatalf("deferred drain changed allocation or published an attempt: %v %+v events=%d", err, current, events)
		}
		if err := f.b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.NewCommand(cluster.OpClusterMetaClear, cluster.Stmt(`DELETE FROM cluster_meta WHERE key=`+cluster.MustLitText(homeMaintenancePrefix+home))), nil
		}); err != nil {
			t.Fatal(err)
		}
		delete(proofs, home)
	}
	realPlanner := a.HomeReassign
	a.HomeReassign = func(db *sql.DB, expected *port.Allocation, target string) (int64, *cluster.Command, error) {
		epoch, cmd, err := realPlanner(db, expected, target)
		if err == nil && cmd != nil {
			// A successful Raft Apply can still affect no allocation rows.
			// Preserve the real full-identity command, then force that CAS miss.
			cmd.Body[0].SQL += " AND 0"
		}
		return epoch, cmd, err
	}
	if err := a.MigrateExposes("brk-a"); err != nil || events != 0 {
		t.Fatalf("zero-row drain published a move: err=%v events=%d", err, events)
	}
	current, err := port.LookupByTokenHash(f.b.cfg.DB, f.alloc.TokenHash)
	if err != nil || !sameHomeAllocation(current, f.alloc) {
		t.Fatal("zero-row control unexpectedly moved the allocation")
	}
	a.HomeReassign = realPlanner
	if err := a.MigrateExposes("brk-a"); err != nil {
		t.Fatalf("ordinary drain failed after protection ended: %v", err)
	}
	current, err = port.LookupByTokenHash(f.b.cfg.DB, f.alloc.TokenHash)
	if err != nil || current.HomeBroker != "brk-b" || current.Epoch <= f.alloc.Epoch || events != 3 {
		t.Fatalf("ordinary drain lacked committed readback/events: %v %+v events=%d", err, current, events)
	}
	// The standalone fallback must borrow the current node, including when a
	// fixture binds it after constructing the admin. Capturing the constructor's
	// nil node would silently lose the guarded planners on this second move.
	// origin: hot-update S6 plan §7.2 / T41 / BROKER-SLIM
	prior := *current
	a.HomeReassign = nil
	if err := a.MigrateExposes("brk-b"); err != nil {
		t.Fatal(err)
	}
	current, err = port.LookupByTokenHash(f.b.cfg.DB, f.alloc.TokenHash)
	if err != nil || current.HomeBroker != "brk-a" || current.Epoch <= prior.Epoch || events != 6 {
		t.Fatalf("cold fallback lost current node/readback/events: %v %+v events=%d", err, current, events)
	}

}

// The observe loop ticks every 5s and decides broker_down from a single 2s
// poll, so a cut's down edge is seen at Ti and its return at ~Ti+5s. The
// fixed proof expiry is <=8s after issue and the cut starts with only >=5s
// left, so the return of a SUCCESSFUL (ENDED) cut is normally observed after
// that expiry. suppressPlannedHomeReturn requires now < original expiry even
// for an ENDED record, so the planned return is reclassified as an ordinary
// broker return and arms auto-rebalance (plan §7.2: "ended后plannedreturn
// 沿baseline识别，不触发故障migration/rotation").
// origin: hot-update S6 internal review rv-cluster
func TestEndedMaintenanceReturnStaysPlannedAfterExpiry(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseEndedMaintenanceReturn(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "planned return x5", beforeG)
	testharness.AssertNoFDLeak(t, "planned return x5", beforeFD, 0)
}
func exerciseEndedMaintenanceReturn(t *testing.T) {
	t.Helper()

	b, _, proofs, now := unitMaintainedBroker(t)
	lease := maintainedHomeLease("from")
	proofs["from"] = lease
	storeHomeMaintenance(t, b.cfg.DB, lease)
	if !suppressPlannedHomeReturn(b, "from", true, false) {
		t.Fatal("precondition: protected down edge not recorded")
	}
	ended := lease
	ended.Record.Phase = brokermaint.MaintEnded
	storeHomeMaintenance(t, b.cfg.DB, ended)
	*now = lease.ExpiresLocalBootNS + int64(time.Second) // next 5s observe tick
	if !suppressPlannedHomeReturn(b, "from", false, true) {
		t.Error("return of an ENDED (successful) cut observed one tick after the fixed expiry became an ordinary return")
	}
}

func staleMaintenanceRecord(home, holder, cut string, phase brokermaint.Phase) brokermaint.Record {
	return brokermaint.Record{CutID: cut, Holder: holder, Home: home, InstanceID: "instance-a",
		IssuerBootID: "issuer-boot-a", Mode: "quorum", Generation: 1, Phase: phase,
		IssuedBootNS: int64(time.Second), ExpiresBootNS: int64(time.Second) + int64(brokermaint.MaxLifetime),
		Term: 1, ConfigIndex: 1, VoterDigest: strings.Repeat("a", 64), BaselineDigest: strings.Repeat("b", 64),
		RequiredCapability: brokermaint.Capability}
}

// A cut that crashes (power loss, kill -9) after its record reached ACTIVE
// leaves the row in cluster_meta forever: nothing reaps it, and the crashed
// holder's lock is reaped by the lease reconciler, after which no holder can
// end or replace it. Plan §7.1 requires a crash to stop protecting the home
// within the original <=8s. With the real P14 planners wired, a leader that has
// no cached proof for that record (every restarted or newly elected leader)
// refuses every guarded move off that home with ErrMaintenanceUnknown, while
// proxyHomeMaintained reports "not maintained": the reaper retries forever
// and the dead home's proxies are never failed over.
// origin: hot-update S6 internal review rv-cluster
func TestCrashedMaintenanceRecordDoesNotPinHomeForever(t *testing.T) {
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
				exerciseCrashedMaintenanceReap(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "home mutation review x5", beforeG)
	testharness.AssertNoFDLeak(t, "home mutation review x5", beforeFD, 0)
}
func exerciseCrashedMaintenanceReap(t *testing.T) {
	t.Helper()

	f := newFlowReaperFixture(t, 14640, 14650)
	plans := HomeMutationPlanners{Reassign: port.PlanReassignHomeGuarded, Allocate: port.PlanAllocateGuarded,
		Rotate: port.PlanRotateAllocationGuarded, Free: port.PlanFreeAllocationGuarded}
	// Long after the record's fixed 8s expiry, with an empty (restarted) cache.
	now := int64(1000 * time.Second)
	if err := ConfigureHomeMaintenance(f.b, plans, func() map[string]brokermaint.LocalLease { return nil },
		func() (int64, error) { return now, nil }); err != nil {
		t.Fatal(err)
	}
	stale := staleMaintenanceRecord("brk-a", "crashed-holder", "crashed-cut", brokermaint.MaintActive)
	raw, err := json.Marshal(stale)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+
			cluster.MustLitText(homeMaintenancePrefix+"brk-a")+`,`+cluster.MustLitText(string(raw))+`)`)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if proxyHomeMaintained(f.b, "brk-a") {
		t.Fatal("precondition: an expired, uncached record must not be reported as maintained")
	}
	err = f.b.moveProxyHomeTo(f.b.nc.Load(), f.alloc, "brk-b", nil, 0)
	if err != nil {
		t.Errorf("expired crashed maintenance record still blocks failover off the dead home: %v (ErrMaintenanceUnknown=%v)",
			err, errors.Is(err, port.ErrMaintenanceUnknown))
	}
	// Nor can any later holder clear or supersede it once the crashed holder's lock is gone.
	marker, _ := cluster.UpgradeHolderMarker("next-holder")
	lock := cluster.UpgradeLockState{Marker: marker, Lease: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}
	next := staleMaintenanceRecord("brk-a", "next-holder", "next-cut", brokermaint.MaintPrepared)
	next.InstanceID, next.IssuerBootID = "instance-b", "issuer-boot-b"
	if _, err := cluster.PlanBrokerMaintenance(f.b.cl.node.RODB(), next, lock); err != nil {
		t.Errorf("a stale ACTIVE record permanently refuses every later maintenance for the home: %v", err)
	}
	ended := stale
	ended.Phase = brokermaint.MaintEnded
	if _, err := cluster.PlanBrokerMaintenance(f.b.cl.node.RODB(), ended, lock); err == nil {
		t.Log("note: a non-owning holder may end the stale record")
	} else {
		t.Logf("no live holder can end the stale record: %v", err)
	}
	if err := reapBrokerMaintenance(f.b); err != nil {
		t.Fatalf("leader did not reap the unowned cut: %v", err)
	}
	actual, err := cluster.ReadBrokerMaintenance(f.b.cl.node.RODB(), stale.Home)
	if err != nil || actual.Phase != brokermaint.MaintEnded {
		t.Fatalf("crashed authority survived reaping: %v %+v", err, actual)
	}
	if err := f.b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return cluster.PlanSetUpgradeActiveExpected(db, "next-holder", f.b.cfg.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		lock, err := cluster.ReadUpgradeLock(db)
		if err != nil {
			return nil, err
		}
		return cluster.PlanBrokerMaintenance(db, next, lock)
	}); err != nil {
		t.Fatalf("fresh cut did not commit: %v", err)
	}
	actual, err = cluster.ReadBrokerMaintenance(f.b.cl.node.RODB(), next.Home)
	if err != nil || actual != next {
		t.Fatalf("fresh preparation readback: %v %+v", err, actual)
	}
}

// Maintenance must defer the actual full-voter plan before any move, rather
// than changing its target set and moving a different proxy elsewhere.
// origin: hot-update S6 internal review rv-cluster-10
func TestMaintainedRebalanceDoesNotChangeTheVoterPlan(t *testing.T) {
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
				exerciseMaintainedRebalance(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "maintained rebalance x5", beforeG)
	testharness.AssertNoFDLeak(t, "maintained rebalance x5", beforeFD, 0)
}
func exerciseMaintainedRebalance(t *testing.T) {
	t.Helper()
	f := newFlowReaperFixture(t, 14700, 14710)
	for _, nid := range []string{"extra-a", "extra-b"} {
		if err := f.b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: nid, ProtoVersion: proto.ProtoVersion, ProxyCapable: true, NatsServer: "nats-a"}, f.b.cfg.Now())
		}); err != nil {
			t.Fatal(err)
		}
		if err := node.SetProxyReady(f.b.livenessDB(), "lab", nid, true); err != nil {
			t.Fatal(err)
		}
		if err := f.b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			_, cmd, err := port.PlanAllocateProxy(db, "lab", nid, "brk-a", f.b.cfg.PortAllocCfg())
			return cmd, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	f.b.lastObserveMu.Lock()
	f.b.lastObserve = []peerObserve{{NodeID: "brk-b", Reachable: true}}
	f.b.lastObserveMu.Unlock()
	lease := maintainedHomeLease("brk-b")
	plans := HomeMutationPlanners{Reassign: port.PlanReassignHomeGuarded, Allocate: port.PlanAllocateGuarded, Rotate: port.PlanRotateAllocationGuarded, Free: port.PlanFreeAllocationGuarded}
	if err := ConfigureHomeMaintenance(f.b, plans, func() map[string]brokermaint.LocalLease { return map[string]brokermaint.LocalLease{"brk-b": lease} }, func() (int64, error) { return int64(time.Second), nil }); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(lease.Record)
	if err := f.b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+cluster.MustLitText(homeMaintenancePrefix+"brk-b")+`,`+cluster.MustLitText(string(raw))+`)`)), nil
	}); err != nil {
		t.Fatal(err)
	}
	report, err := f.b.rebalanceProxyHomes(false)
	var deferred *HomeMaintenanceDeferredError
	if !errors.As(err, &deferred) || deferred.Home != "brk-b" || report == nil || report.Voters != 2 || report.Planned != 1 || len(report.Moves) != 0 {
		t.Fatalf("maintenance changed/partially executed the plan: %+v %v", report, err)
	}
	f.b.lastObserveMu.Lock()
	f.b.lastObserve = []peerObserve{{NodeID: "brk-b", Reachable: false}}
	f.b.lastObserveMu.Unlock()
	report, err = f.b.rebalanceProxyHomes(false)
	deferred = nil
	if !errors.As(err, &deferred) || report == nil || report.Voters != 2 || report.Planned != 1 || len(report.Moves) != 0 {
		t.Fatalf("planned downtime changed the voter plan: %+v %v", report, err)
	}
	if f.tap.countKind(t, evHomeReassignStarted) != 0 {
		t.Fatal("deferred plan published a move")
	}
}
