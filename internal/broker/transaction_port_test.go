//go:build linux

package broker

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S6 internal review SINGLEACK F1 / lead decision D-C / plan §7.1
func TestSingleBrokerTransactionRefusesMissingMaintenanceCapabilityBeforeQueue(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSingleBrokerTransactionMissingCapability(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "single transaction maintenance refusal", g)
	testharness.AssertNoFDLeak(t, "single transaction maintenance refusal", fd, 0)
}

func exerciseSingleBrokerTransactionMissingCapability(t *testing.T) {
	t.Helper()
	db := openDBWithSession(t, "legacy-session")
	b, err := New(Config{DB: db, NATSURL: "nats://127.0.0.1:1", Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "legacy-session", NID: "legacy-agent", ProtoVersion: proto.ProtoVersion}, b.cfg.Now()); err != nil {
		t.Fatal(err)
	}
	d, err := controlbudget.After(100 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := brokerTransactionPort(b).Arm(d); !errors.Is(err, brokerimage.ErrMaintenanceObserverUnsupported) {
		t.Fatalf("single-mode planned cut bypassed capability admission: %v", err)
	}
	if b.reloadSignal.Requested() {
		t.Fatal("capability-less single-mode cut queued work before refusal")
	}
	if b.imageControl != nil && b.imageControl.localMaintenance.Record.CutID != "" {
		t.Fatal("refused cut installed maintenance ownership")
	}
}

// origin: S6 internal review SINGLEACK F2 / lead decision D-C / plan §7.1
func TestSingleBrokerCutRequiresRetainedMaintenanceOwner(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		if err := validateBrokerReloadRole(&Broker{}, stopped); !errors.Is(err, brokerimage.ErrMaintenanceObserverUnsupported) {
			t.Fatalf("single cut without retained owner passed stopped=%t: %v", stopped, err)
		}
	}
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []brokermaint.Phase{brokermaint.MaintPrepared, brokermaint.MaintEnded} {
		lease := brokermaint.LocalLease{Record: brokermaint.Record{CutID: "local-cut", Phase: phase}, ExpiresLocalBootNS: int64(d)}
		b := &Broker{imageControl: &brokerImageControl{localMaintenance: lease}}
		if err := validateBrokerReloadRole(b, true); !errors.Is(err, brokermaint.ErrIdentity) {
			t.Fatalf("single cut accepted a non-active retained owner (%v): %v", phase, err)
		}
	}
	lease := brokermaint.LocalLease{Record: brokermaint.Record{CutID: "local-cut", Phase: brokermaint.MaintActive}, ExpiresLocalBootNS: 1}
	if err := validateBrokerReloadRole(&Broker{imageControl: &brokerImageControl{localMaintenance: lease}}, true); !errors.Is(err, brokermaint.ErrExpired) {
		t.Fatalf("single cut extended its retained expiry: %v", err)
	}
}

// The S8 route arms the same S6 store cut the old reload did, so a
// cluster-mode arm must first capture the role/maintenance admission the cut
// validates at its freeze and final guards; an arm without it is refused by
// the S6 cut itself ("image role admission missing"). Nothing runs the cut
// here: the arm's own wait for the Run loop simply expires.
// origin: hot-update S8 plan §4.2 / §6.3 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11 (lead acceptance: sole-voter product cut)
func TestBrokerTransactionArmCapturesClusterAdmission(t *testing.T) {
	b, _, _ := newJoinedControlFixture(t)
	if err := validateBrokerReloadRole(b, false); err == nil {
		t.Fatal("a cluster cut was admitted before any arm captured its role")
	}
	d, err := controlbudget.After(300 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := brokerTransactionPort(b).Arm(d); err == nil {
		t.Fatal("an arm without a Run loop reported an armed cut")
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	admitted := m.admitted
	m.mu.Unlock()
	if !admitted {
		t.Fatal("the S8 arm did not capture the cluster reload admission")
	}
	if err := validateBrokerReloadRole(b, false); err != nil {
		t.Fatalf("the captured admission does not admit the unchanged sole voter: %v", err)
	}
}
