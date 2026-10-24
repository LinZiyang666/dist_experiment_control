package cluster

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
)

// origin: hot-update S6 plan §7.1 / T40
func TestUpgradeHolderCAS(t *testing.T) {
	f, db := freshFSM(t, t.TempDir())
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	apply := func(cmd *Command, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if cmd == nil {
			t.Fatal("missing replicated command")
		}
		if err := ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
	}
	read := func() UpgradeLockState {
		t.Helper()
		s, err := ReadUpgradeLock(db)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	// Identical leader wall timestamps must not grant the same identity to two rolls.
	a, err := PlanSetUpgradeActiveExpected(db, "A", now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PlanSetUpgradeActiveExpected(db, "B", now)
	if err != nil {
		t.Fatal(err)
	}
	apply(a, nil)
	ownedA := read()
	apply(b, nil)
	if read() != ownedA || ownedA.Lease == "" {
		t.Fatal("same-timestamp acquire stole the holder or omitted its lease")
	}
	renew, err := PlanRenewUpgradeLeaseExpected(db, "A", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	release, err := PlanClearUpgradeActiveExpected(db, "A", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	expire, err := PlanExpireUpgradeLeaseExpected(db, "A", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	// A loses its lease and B obtains it before A's three delayed commands apply.
	apply(PlanSetUpgradeActiveExpected(db, "B", now.Add(LockLeaseTTL+time.Second)))
	ownedB := read()
	for _, cmd := range []*Command{renew, release, expire} {
		apply(cmd, nil)
		if read() != ownedB {
			t.Fatal("late A command changed B's marker/lease")
		}
	}
	for _, plan := range []func() (*Command, error){
		func() (*Command, error) {
			return PlanRenewUpgradeLeaseExpected(db, "A", now.Add(LockLeaseTTL+2*time.Second))
		},
		func() (*Command, error) {
			return PlanExpireUpgradeLeaseExpected(db, "A", now.Add(LockLeaseTTL+2*time.Second))
		},
		func() (*Command, error) {
			return PlanClearUpgradeActiveExpected(db, "A", now.Add(LockLeaseTTL+2*time.Second))
		},
	} {
		if _, err := plan(); !errors.Is(err, ErrUpgradeHolder) {
			t.Fatalf("late A admission = %v", err)
		}
	}
	// Both old live verbs and old takeover after expiry must leave S6 ownership intact.
	apply(PlanRenewUpgradeLease(now.Add(LockLeaseTTL + 2*time.Second)))
	apply(PlanExpireUpgradeLease(now.Add(LockLeaseTTL + 2*time.Second)))
	apply(PlanClearUpgradeActive())
	apply(PlanSetUpgradeActive(now.Add(3 * LockLeaseTTL)))
	if read() != ownedB {
		t.Fatal("holder-less operation touched an S6 lock")
	}
	// Losing expiry alone revokes renewal/release authority, even before takeover.
	if _, err := PlanRenewUpgradeLeaseExpected(db, "B", now.Add(3*LockLeaseTTL)); !errors.Is(err, ErrUpgradeHolder) {
		t.Fatalf("expired owner renewed: %v", err)
	}
	if _, err := PlanClearUpgradeActiveExpected(db, "B", now.Add(3*LockLeaseTTL)); !errors.Is(err, ErrUpgradeHolder) {
		t.Fatalf("expired owner released: %v", err)
	}
	// A stale expiry hint cannot clear a renewal committed after that hint.
	reap, err := PlanReapUpgradeLease(db, now.Add(3*LockLeaseTTL))
	if err != nil {
		t.Fatal(err)
	}
	apply(PlanRenewUpgradeLeaseExpected(db, "B", now.Add(LockLeaseTTL+2*time.Second)))
	renewedB := read()
	apply(reap, nil)
	if read() != renewedB {
		t.Fatal("reaper cleared a newly renewed pair")
	}
	cmd, err := PlanReapUpgradeLease(db, now.Add(LockLeaseTTL+3*time.Second))
	if err != nil || cmd != nil {
		t.Fatalf("fresh reaper did not recheck expiry: %v %v", cmd, err)
	}
	apply(PlanClearUpgradeActiveExpected(db, "B", now.Add(LockLeaseTTL+3*time.Second)))
	if read() != (UpgradeLockState{}) {
		t.Fatal("release left half of marker/lease pair")
	}
	for _, bad := range []string{"", "bad\x00value", string([]byte{0xff})} {
		if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?)`, MetaKeyUpgradeActive, bad); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadUpgradeLock(db); err == nil {
			t.Fatal("corrupt lock value accepted")
		}
		if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key=?`, MetaKeyUpgradeActive); err != nil {
			t.Fatal(err)
		}
	}
	// Exercise actual FSM apply, including its applied-index receipt.
	acquired, err := PlanSetUpgradeActiveExpected(db, "C", now.Add(4*LockLeaseTTL))
	if err != nil {
		t.Fatal(err)
	}
	if result := d7Apply(t, f, 1, acquired); func() bool { _, ok := result.(appliedOK); return !ok }() {
		t.Fatalf("FSM apply: %v", result)
	}
	marker, _ := UpgradeHolderMarker("C")
	if read().Marker != marker {
		t.Fatal("FSM did not apply holder")
	}
}

// origin: hot-update S6 plan §7.1 / T40
func TestBrokerMaintenanceRecordCAS(t *testing.T) {
	_, db := freshFSM(t, t.TempDir())
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	apply := func(cmd *Command, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if err := ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
	}
	apply(PlanSetUpgradeActiveExpected(db, "holder", now))
	lock, err := ReadUpgradeLock(db)
	if err != nil {
		t.Fatal(err)
	}
	record := brokermaint.Record{CutID: "cut", Holder: "holder", Home: "home", InstanceID: "instance", IssuerBootID: "boot", Mode: "quorum", Generation: 1, Phase: brokermaint.MaintPrepared, IssuedBootNS: 1, ExpiresBootNS: 1 + int64(8*time.Second), Term: 1, ConfigIndex: 1, VoterDigest: "voters", BaselineDigest: "baseline", RequiredCapability: brokermaint.Capability}
	for _, mutate := range []func(*brokermaint.Record){
		func(r *brokermaint.Record) { r.ExpiresBootNS++ }, func(r *brokermaint.Record) { r.IssuedBootNS = 0 }, func(r *brokermaint.Record) { r.ExpiresBootNS = r.IssuedBootNS }, func(r *brokermaint.Record) { r.Mode = "unknown" }, func(r *brokermaint.Record) { r.CutID = "" }, func(r *brokermaint.Record) { r.InstanceID = "" }, func(r *brokermaint.Record) { r.IssuerBootID = "" }, func(r *brokermaint.Record) { r.Generation = 0 }, func(r *brokermaint.Record) { r.Term = 0 }, func(r *brokermaint.Record) { r.ConfigIndex = 0 }, func(r *brokermaint.Record) { r.VoterDigest = "" }, func(r *brokermaint.Record) { r.BaselineDigest = "" }, func(r *brokermaint.Record) { r.RequiredCapability = "" }, func(r *brokermaint.Record) { r.InstanceID = "bad\nidentifier" },
	} {
		changed := record
		changed.Home = "first-invalid"
		mutate(&changed)
		if _, err := PlanBrokerMaintenance(db, changed, lock); err == nil {
			t.Fatal("invalid initial maintenance accepted")
		}
	}
	apply(PlanBrokerMaintenance(db, record, lock))
	actual, err := ReadBrokerMaintenance(db, "home")
	if err != nil || actual != record {
		t.Fatalf("record/readback mismatch: %v", err)
	}
	for _, mutate := range []func(*brokermaint.Record){
		func(r *brokermaint.Record) { r.ExpiresBootNS++ }, func(r *brokermaint.Record) { r.CutID = "other" }, func(r *brokermaint.Record) { r.Holder = "other" }, func(r *brokermaint.Record) { r.InstanceID = "other" }, func(r *brokermaint.Record) { r.Home = "" }, func(r *brokermaint.Record) { r.IssuerBootID = "other" }, func(r *brokermaint.Record) { r.Generation++ }, func(r *brokermaint.Record) { r.Term++ }, func(r *brokermaint.Record) { r.ConfigIndex++ }, func(r *brokermaint.Record) { r.VoterDigest = "other" }, func(r *brokermaint.Record) { r.BaselineDigest = "other" }, func(r *brokermaint.Record) { r.RequiredCapability = "unknown" }, func(r *brokermaint.Record) { r.Phase = 0 },
	} {
		changed := record
		mutate(&changed)
		if _, err := PlanBrokerMaintenance(db, changed, lock); err == nil {
			t.Fatal("maintenance drift accepted")
		}
	}
	active := record
	active.Phase = brokermaint.MaintActive
	stale, err := PlanBrokerMaintenance(db, active, lock)
	if err != nil {
		t.Fatal(err)
	}
	ended := record
	ended.Phase = brokermaint.MaintEnded
	apply(PlanBrokerMaintenance(db, ended, lock))
	apply(stale, nil)
	actual, err = ReadBrokerMaintenance(db, "home")
	if err != nil || actual != ended {
		t.Fatal("stale active resurrected ended maintenance")
	}
	if _, err := PlanBrokerMaintenance(db, record, lock); err == nil {
		t.Fatal("phase regression accepted")
	}
	// An ended baseline stays durable until a different cut is explicitly prepared.
	next := record
	next.CutID = "next"
	apply(PlanBrokerMaintenance(db, next, lock))
	actual, err = ReadBrokerMaintenance(db, "home")
	if err != nil || actual != next {
		t.Fatalf("next cut: %v", err)
	}
	foreign := record
	foreign.Home = "foreign"
	foreign.Phase = brokermaint.MaintActive
	if _, err := PlanBrokerMaintenance(db, foreign, lock); err == nil {
		t.Fatal("first phase active accepted")
	}
	if _, err := newClusterMetaSet(BrokerMaintenanceKeyPrefix+"foreign", "invalid"); err == nil {
		t.Fatal("generic meta writer escaped test prefix")
	}
	if _, err := ReadBrokerMaintenance(db, "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing record = %v", err)
	}
}
