package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The unit boundary takes an already validated P15 LocalLease. Signature and
// cross-clock challenge validation remain P15's responsibility; integration
// below uses the concrete guarded planners and a valid fixed-expiry lease.
func maintainedHomeLease(home string) brokermaint.LocalLease {
	record := brokermaint.Record{CutID: "cut-" + home, Holder: "holder", Home: home,
		InstanceID: "instance", IssuerBootID: "issuer-boot", Generation: 1,
		Phase: brokermaint.MaintActive, IssuedBootNS: 900_000_000, ExpiresBootNS: 8_900_000_000,
		Mode: "quorum", Term: 1, ConfigIndex: 1, VoterDigest: strings.Repeat("a", 64), BaselineDigest: strings.Repeat("b", 64),
		RequiredCapability: brokermaint.Capability}
	digest, _ := brokermaint.RecordDigest(record)
	return brokermaint.LocalLease{Record: record, ExpiresLocalBootNS: 8_000_000_000,
		Proof: brokermaint.Proof{CutID: record.CutID, IssuerBootID: record.IssuerBootID, Nonce: "nonce", RecordDigest: digest, Sig: "verified-by-P15", ResponseBootNS: 900_000_000, ExpiresBootNS: record.ExpiresBootNS, AppliedIndex: 1}}
}
func storeHomeMaintenance(t *testing.T, db *sql.DB, lease brokermaint.LocalLease) {
	t.Helper()
	raw, err := json.Marshal(lease.Record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		homeMaintenancePrefix+lease.Record.Home, string(raw)); err != nil {
		t.Fatal(err)
	}
}
func unitMaintainedBroker(t *testing.T) (*Broker, *port.Allocation, map[string]brokermaint.LocalLease, *int64) {
	t.Helper()
	db := openDBWithSession(t, "maint-session")
	b := &Broker{cfg: Config{DB: db, Now: func() time.Time { return time.Unix(100, 0) }, Logger: silentLogger()}, cl: &clusterRuntime{}}
	if err := node.Register(db, node.RegisterInput{SID: "maint-session", NID: "maint-node", ProtoVersion: proto.ProtoVersion}, b.cfg.Now()); err != nil {
		t.Fatal(err)
	}
	alloc, cmd, err := port.PlanAllocateProxy(db, "maint-session", "maint-node", "from", &port.Config{BandLow: 14000, BandHigh: 14010})
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range cmd.Body {
		if _, err := db.Exec(statement.SQL, statement.Args...); err != nil {
			t.Fatal(err)
		}
	}
	proofs := map[string]brokermaint.LocalLease{}
	now := int64(1_000_000_000)
	b.cl.homeMaintenance.proofs = func() map[string]brokermaint.LocalLease { return proofs }
	b.cl.homeMaintenance.now = func() (int64, error) { return now, nil }
	return b, alloc, proofs, &now
}

// origin: hot-update S6 plan §7.2 / T41
func TestMaintenanceEveryHomeMutator(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseHomeMutationGuard(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "home writer guard x5", beforeG)
	testharness.AssertNoFDLeak(t, "home writer guard x5", beforeFD, 0)
}
func exerciseHomeMutationGuard(t *testing.T) {
	t.Helper()
	b, a, proofs, now := unitMaintainedBroker(t)
	for _, which := range []string{"from", "to"} {
		lease := maintainedHomeLease(which)
		proofs[which] = lease
		storeHomeMaintenance(t, b.cfg.DB, lease)
		// Every public writer shares this guard in its Propose callback. Reassign
		// covers manual/auto/rehome; allocation covers absent/rotation and from/to.
		var deferred *HomeMaintenanceDeferredError
		_, _, err := planHomeReassign(b, b.cfg.DB, a, "to")
		if !errors.As(err, &deferred) || deferred.Home != which {
			t.Fatalf("%s reassign guard missing: %v", which, err)
		}
		expected := a
		if which == "to" {
			expected = nil
		}
		_, _, err = planHomeAllocate(b, b.cfg.DB, expected, port.AllocationIntent{SID: a.SID, NID: a.NID, Name: port.ProxyPortName, HomeBroker: "to"})
		deferred = nil
		if !errors.As(err, &deferred) || deferred.Home != which {
			t.Fatalf("%s allocation/rotation guard missing: %v", which, err)
		}
		if _, err := b.cfg.DB.Exec(`DELETE FROM cluster_meta WHERE key=?`, homeMaintenancePrefix+which); err != nil {
			t.Fatal(err)
		}
		delete(proofs, which)
	}
	g, _, err := guardHomeMutation(b, b.cfg.DB, a, "from", "to", false)
	if err != nil || len(g.ExpectedMaintenance) != 2 || g.ExpectedMaintenance["from"] != "" || g.ExpectedMaintenance["to"] != "" {
		t.Fatalf("unrelated home blocked or absence not captured: %+v %v", g, err)
	}
	for _, mutate := range []func(*port.Allocation){
		func(a *port.Allocation) { a.Port++ }, func(a *port.Allocation) { a.SID += "stale" }, func(a *port.Allocation) { a.NID += "stale" },
		func(a *port.Allocation) { a.Name += "stale" }, func(a *port.Allocation) { a.TokenHash += "stale" },
		func(a *port.Allocation) { a.HomeBroker = "to" }, func(a *port.Allocation) { a.Epoch++ }, func(a *port.Allocation) { a.State = port.StateFreed },
	} {
		stale := *a
		mutate(&stale)
		if _, _, err := guardHomeMutation(b, b.cfg.DB, &stale, "from", "to", false); err == nil {
			t.Fatal("stale allocation accepted")
		}
	}
	lease := maintainedHomeLease("from")
	proofs["from"] = lease
	storeHomeMaintenance(t, b.cfg.DB, lease)
	// Unknown proofs cannot manufacture maintenance; changing any fixed
	// identity field also prevents an old signed cache from protecting a new cut.
	for _, change := range []func(*brokermaint.LocalLease){
		func(l *brokermaint.LocalLease) { l.Record.Holder = "replacement" },
		func(l *brokermaint.LocalLease) { l.Record.InstanceID = "replacement" },
		func(l *brokermaint.LocalLease) { l.Record.IssuerBootID = "replacement" },
		func(l *brokermaint.LocalLease) { l.Record.BaselineDigest = "replacement" },
		func(l *brokermaint.LocalLease) { l.Proof.CutID = "replacement" },
		func(l *brokermaint.LocalLease) { l.Proof.IssuerBootID = "replacement" },
		func(l *brokermaint.LocalLease) { l.ExpiresLocalBootNS = 0 },
	} {
		invalid := lease
		change(&invalid)
		proofs["from"] = invalid
		if proxyHomeMaintained(b, "from") {
			t.Fatal("mismatched cached proof protected a home")
		}
	}
	delete(proofs, "from")
	if proxyHomeMaintained(b, "from") {
		t.Fatal("missing proof manufactured maintenance")
	}
	proofs["from"] = lease
	*now = lease.ExpiresLocalBootNS
	if _, _, err := guardHomeMutation(b, b.cfg.DB, a, "from", "to", false); err != nil {
		t.Fatalf("expiry prevented normal recovery: %v", err)
	}
	*now--
	changed := lease
	changed.Record.Holder = "replacement"
	storeHomeMaintenance(t, b.cfg.DB, changed)
	if _, _, err := guardHomeMutation(b, b.cfg.DB, a, "from", "to", false); err != nil {
		t.Fatalf("stale proof protected replacement holder: %v", err)
	}
	if _, err := b.cfg.DB.Exec(`UPDATE cluster_meta SET value='invalid' WHERE key=?`, homeMaintenancePrefix+"from"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := guardHomeMutation(b, b.cfg.DB, a, "from", "to", false); err == nil {
		t.Fatal("malformed maintenance accepted")
	}
}

// origin: hot-update S6 plan §7.2 / T43
func TestMaintenancePreservesDwellAndReturn(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseHomeMaintenanceHistory(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "home maintenance history x5", beforeG)
	testharness.AssertNoFDLeak(t, "home maintenance history x5", beforeFD, 0)
}
func exerciseHomeMaintenanceHistory(t *testing.T) {
	t.Helper()
	b, a, proofs, now := unitMaintainedBroker(t)
	lease := maintainedHomeLease("from")
	proofs["from"] = lease
	storeHomeMaintenance(t, b.cfg.DB, lease)
	b.proxyDwell.Store(a.Port, 2)
	b.autoRebalanceArm.pending = map[string]int{"other": 2, "from": 4}
	b.autoRebalanceArm.cooldown = 2
	if !suppressPlannedHomeReturn(b, "from", true, false) {
		t.Fatal("planned down edge was not recorded")
	}
	history, err := ExportHomeMaintenanceHistory(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Dwells) != 1 || history.Dwells[0].Dwell != 2 || len(history.Returns) != 1 {
		t.Fatalf("history omitted: %+v", history)
	}
	successor := &Broker{cfg: b.cfg, cl: &clusterRuntime{}}
	successor.cl.homeMaintenance.proofs = b.cl.homeMaintenance.proofs
	successor.cl.homeMaintenance.now = b.cl.homeMaintenance.now
	if err := RestoreHomeMaintenanceHistory(successor, history); err != nil {
		t.Fatal(err)
	}
	got, _ := successor.proxyDwell.Load(a.Port)
	if got != 2 || !reflect.DeepEqual(successor.autoRebalanceArm.pending, b.autoRebalanceArm.pending) {
		t.Fatal("history changed on import")
	}
	if err := RestoreHomeMaintenanceHistory(successor, history); err == nil {
		t.Fatal("duplicate history installation accepted")
	}
	ended := lease
	ended.Record.Phase = brokermaint.MaintEnded
	storeHomeMaintenance(t, b.cfg.DB, ended)
	if proxyHomeMaintained(successor, "from") {
		t.Fatal("ended maintenance still fences writers")
	}
	if !suppressPlannedHomeReturn(successor, "from", false, true) {
		t.Fatal("ended planned return became a crash return")
	}
	if successor.autoRebalanceArm.pending["other"] != 2 {
		t.Fatal("planned return cleared unrelated pending work")
	}
	storeHomeMaintenance(t, b.cfg.DB, lease)
	if !suppressPlannedHomeReturn(b, "from", true, false) {
		t.Fatal("second down not recorded")
	}
	*now = lease.ExpiresLocalBootNS
	if suppressPlannedHomeReturn(b, "from", false, true) || proxyHomeMaintained(b, "from") {
		t.Fatal("expired maintenance hid a real crash")
	}
	// Matching the original record cannot restart a failed proof's fixed deadline.
	*now++
	if suppressPlannedHomeReturn(b, "from", true, false) {
		t.Fatal("late record read renewed protection")
	}
	arm := &autoRebalanceArm{pending: map[string]int{"from": 4, "other": 1}}
	if !tickHomeReturns(arm, nil, func(string) bool { return true }, func(home string) bool { return home == "from" }, true) {
		t.Fatal("unrelated pending return did not fire")
	}
	if len(arm.pending) != 1 || arm.pending["from"] != 4 {
		t.Fatalf("maintained pending work was globally cleared: %v", arm.pending)
	}
	arm.cooldown = 0
	if tickHomeReturns(arm, nil, func(string) bool { return false }, func(string) bool { return false }, true) || len(arm.pending) != 0 {
		t.Fatal("expiry did not restore normal pending cancellation")
	}
	// The decoder path must refuse corruption before installing ANY dwell.
	for _, mutate := range []func(*HomeMaintenanceHistory){
		func(h *HomeMaintenanceHistory) { h.Dwells[0].SID = "other" },
		func(h *HomeMaintenanceHistory) { h.Returns[0].ExpiresLocalBootNS++ },
		func(h *HomeMaintenanceHistory) { h.AutoPending["other"] = -1 },
	} {
		raw, _ := json.Marshal(history)
		var bad HomeMaintenanceHistory
		if err := json.Unmarshal(raw, &bad); err != nil {
			t.Fatal(err)
		}
		mutate(&bad)
		fresh := &Broker{cfg: b.cfg, cl: &clusterRuntime{}}
		fresh.cl.homeMaintenance.proofs = b.cl.homeMaintenance.proofs
		fresh.cl.homeMaintenance.now = b.cl.homeMaintenance.now
		if err := RestoreHomeMaintenanceHistory(fresh, bad); err == nil {
			t.Fatal("invalid history installed")
		}
		if _, ok := fresh.proxyDwell.Load(a.Port); ok {
			t.Fatal("partial history installed before rejection")
		}
	}
}

// origin: hot-update S6 plan §7.2 / T41 / T43
func TestBrokerHomeLeakCycles(t *testing.T) {
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
				exerciseMaintainedHomeTick(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "maintained home tick x5", beforeG)
	testharness.AssertNoFDLeak(t, "maintained home tick x5", beforeFD, 0)
}
func exerciseMaintainedHomeTick(t *testing.T) {
	t.Helper()
	f := newFlowReaperFixture(t, 14400, 14410)
	b := f.b
	lease := maintainedHomeLease("brk-a")
	now := int64(1_000_000_000)
	b.cl.homeMaintenance.proofs = func() map[string]brokermaint.LocalLease { return map[string]brokermaint.LocalLease{"brk-a": lease} }
	b.cl.homeMaintenance.now = func() (int64, error) { return now, nil }
	raw, _ := json.Marshal(lease.Record)
	if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+cluster.MustLitText(homeMaintenancePrefix+"brk-a")+`,`+cluster.MustLitText(string(raw))+`)`)), nil
	}); err != nil {
		t.Fatal(err)
	}
	baseline := flowReaperSnapshot(t, b)

	var receipts []drillobserve.Event
	previousHook := drillobserve.Hook
	drillobserve.Hook = func(event drillobserve.Event) {
		if event.Kind == "reaper" && event.Domain == "proxy" {
			receipts = append(receipts, event)
		}
	}
	t.Cleanup(func() { drillobserve.Hook = previousHook })
	b.proxyDwell.Store(f.alloc.Port, 2)
	for _, ready := range []bool{true, false} {
		setFlowReaperReady(t, b, ready)
		for tick := 0; tick < 3; tick++ {
			b.driveProxyReconcile()
		}
		if dwell, _ := b.proxyDwell.Load(f.alloc.Port); dwell != 2 {
			t.Fatalf("maintenance cleared/increased historical dwell: %v", dwell)
		}
		if got := flowReaperSnapshot(t, b); !reflect.DeepEqual(got, baseline) {
			t.Fatal("maintained healthy/unready home rotated")
		}
	}
	if err := b.moveProxyHomeTo(b.nc.Load(), f.alloc, "brk-b", nil, 0); err == nil {
		t.Fatal("manual rebalance escaped maintenance")
	}
	for _, kind := range []string{evHomeReassignStarted, evHomeReassignSucceeded, evHomeReassignFailed, evRehomeStalled} {
		if f.tap.countKind(t, kind) != 0 {
			t.Fatalf("maintenance emitted mutation event before commit: %v", f.tap.sysEvents(t))
		}
	}
	b.lastObserveMu.Lock()
	b.lastObserve = []peerObserve{{NodeID: "brk-a", Reachable: false}}
	b.lastObserveMu.Unlock()
	b.selfID = "observer"
	b.driveProxyReconcile()
	if dwell, _ := b.proxyDwell.Load(f.alloc.Port); dwell != 2 {
		t.Fatalf("maintained unhealthy branch changed dwell: %v", dwell)
	}
	if len(receipts) != 7 {
		t.Fatalf("missing maintained reaper observations: %d", len(receipts))
	}
	for i, event := range receipts {
		if event.Values[5] != 1 || event.Values[6] != 2 || event.Values[0] != uint64(f.alloc.Port) || string(event.Data) != "brk-a" {
			t.Fatal("maintained reaper receipt lost baseline identity")
		}
		if (i < 3 && event.Values[4] != 1) || (i >= 3 && event.Values[4] != 0) || (i == 6 && event.Values[3] != 0) {
			t.Fatal("maintenance fabricated healthy/ready observation")
		}
	}
	now = lease.ExpiresLocalBootNS
	b.driveProxyReconcile()
	// The concrete planner now commits the third bad observation. One tick
	// after expiry must suffice: resetting the historical dwell would defer it.
	// origin: hot-update S6 plan §7.2 / T43
	current, err := port.LookupByTokenHash(b.read().SQL(), f.alloc.TokenHash)
	if err != nil || current.HomeBroker != "brk-b" || current.Epoch != f.alloc.Epoch+1 {
		t.Fatalf("expiry did not resume historical failure dwell: allocation=%+v err=%v", current, err)
	}
	if dwell, ok := b.proxyDwell.Load(f.alloc.Port); ok {
		t.Fatalf("committed rehome retained old dwell: %v", dwell)
	}
	if f.tap.countKind(t, evHomeReassignStarted) != 1 || f.tap.countKind(t, evHomeReassignSucceeded) != 1 {
		t.Fatal("expiry did not publish exactly one committed rehome")
	}
}

// Concrete planners must commit and read back full identity, while expiry resumes
// ordinary allocation rotation without resetting historical failure dwell.
// origin: hot-update S6 plan §7.2 / T41
func TestMaintenanceHomeGuardedPlannerIntegration(t *testing.T) {
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
				exerciseGuardedHomePlanner(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "guarded home planner x5", beforeG)
	testharness.AssertNoFDLeak(t, "guarded home planner x5", beforeFD, 0)
}
func exerciseGuardedHomePlanner(t *testing.T) {
	t.Helper()
	f := newFlowReaperFixture(t, 14500, 14510)
	plans := HomeMutationPlanners{Reassign: port.PlanReassignHomeGuarded, Allocate: port.PlanAllocateGuarded,
		Rotate: port.PlanRotateAllocationGuarded, Free: port.PlanFreeAllocationGuarded}
	if err := ConfigureHomeMaintenance(f.b, plans, func() map[string]brokermaint.LocalLease { return nil }, func() (int64, error) { return 1_000_000_000, nil }); err != nil {
		t.Fatal(err)
	}
	if err := f.b.moveProxyHomeTo(f.b.nc.Load(), f.alloc, "brk-b", nil, 0); err != nil {
		t.Fatalf("real guarded home planner required: %v", err)
	}
	if f.tap.countKind(t, evHomeReassignStarted) != 1 || f.tap.countKind(t, evHomeReassignSucceeded) != 1 {
		t.Fatal("committed move missing lifecycle events")
	}
	updated, err := port.LookupProxyByNode(f.b.cfg.DB, f.alloc.SID, f.alloc.NID)
	if err != nil || updated.HomeBroker != "brk-b" || updated.Epoch != f.alloc.Epoch+1 || updated.TokenHash != f.alloc.TokenHash {
		t.Fatal("full committed move identity changed")
	}
	stale := *f.alloc
	if err := f.b.moveProxyHomeTo(f.b.nc.Load(), &stale, "brk-a", nil, 0); err == nil {
		t.Fatal("stale move succeeded")
	}
	if f.tap.countKind(t, evHomeReassignStarted) != 1 {
		t.Fatal("stale proposal emitted started")
	}
	// A real healthy-but-unready expiry must rotate and retire the old token.
	lease := maintainedHomeLease("brk-b")
	now := int64(1_000_000_000)
	f.b.cl.homeMaintenance.proofs = func() map[string]brokermaint.LocalLease { return map[string]brokermaint.LocalLease{"brk-b": lease} }
	f.b.cl.homeMaintenance.now = func() (int64, error) { return now, nil }
	raw, _ := json.Marshal(lease.Record)
	if err := f.b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpClusterMetaSet,
			cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+cluster.MustLitText(homeMaintenancePrefix+"brk-b")+`,`+cluster.MustLitText(string(raw))+`)`)), nil
	}); err != nil {
		t.Fatal(err)
	}
	f.b.proxyDwell.Store(updated.Port, 2)
	setFlowReaperReady(t, f.b, false)
	f.b.driveProxyReconcile()
	now = lease.ExpiresLocalBootNS
	f.b.driveProxyReconcile()
	rotated, err := port.LookupProxyByNode(f.b.cfg.DB, updated.SID, updated.NID)
	if err != nil || rotated.TokenHash == updated.TokenHash {
		t.Fatal("expiry did not perform real guarded allocation rotation")
	}
}

// origin: hot-update S6 plan §7.2 / T41
func TestHomeMaintenanceWiringRejectsIncompleteSources(t *testing.T) {
	b, _, _, _ := unitMaintainedBroker(t)
	if err := ConfigureHomeMaintenance(b, HomeMutationPlanners{}, nil, nil); err == nil {
		t.Fatal("missing planner/proof source accepted")
	}
	b.cl.homeMaintenance.now = func() (int64, error) { return 0, nil }
	if _, _, err := guardHomeMutation(b, b.cfg.DB, nil, "", "other", true); err == nil {
		t.Fatal("zero clock admitted a mutation")
	}
	if !proxyHomeMaintained(b, "other") {
		t.Fatal("unknown clock authorized reaper mutation")
	}
	b.cl.homeMaintenance.now = func() (int64, error) { return 1_000_000_000, nil }
	_, _, err := planHomeAllocate(b, b.cfg.DB, nil, port.AllocationIntent{HomeBroker: "other"})
	if err == nil || !strings.Contains(err.Error(), "broker: guarded home allocation unavailable") {
		t.Fatalf("missing implementation faked success: %v", err)
	}
}
