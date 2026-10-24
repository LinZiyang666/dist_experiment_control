//go:build linux

package brokerimage

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// A create handler owns transfer admission before its durable tracker row exists.
// A short join refusal is busy evidence; a census read error remains a store error.
// origin: hot-update S6 internal review rv-ctl-11 / plan §5 / T45
func TestBrokerReloadBusyTransferDefersBeforeRPCDeadline(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerBusyTransfer(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "reload busy transfer x5", g)
	testharness.AssertNoFDLeak(t, "reload busy transfer x5", fds, 0)
}

func exerciseBrokerBusyTransfer(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	r := &StoreRun[int]{Slot: slot, Capture: func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil }, Freeze: Freeze,
		Preflight: func(int, controlbudget.Deadline) error { return nil }, PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil },
		InstallCurrent: func(int) error { return nil }, Recover: func(int, Cut, storage.StoreSeal, controlbudget.Deadline) error { return nil },
		DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/unused"}
	var gate sync.Mutex
	var signal StoreReloadSignal
	var admission StoreAdmission
	if !admission.Begin() {
		t.Fatal("create admission unexpectedly closed")
	}
	defer admission.End()
	reload := &LocalStoreReload[int]{Image: r, Gate: &gate, Signal: &signal, Resume: func(int) { admission.Resume() }, Status: func(int) *proto.BrokerImageStatus { return nil },
		Fence: func(_ int, d controlbudget.Deadline) (uint32, error) { return 0, admission.Fence(d) }}
	start := time.Now()
	response := reload.HandleCluster(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload})
	if response.Code != adminsock.CodeUpgradeDeferred || response.Error != "transfers_in_flight" || response.Reloading || response.OK || signal.Requested() {
		t.Fatalf("busy create was not deferred: %+v", response)
	}
	if time.Since(start) >= time.Second {
		t.Fatal("busy create consumed the CLI's RPC budget")
	}
	if !admission.Begin() {
		t.Fatal("busy refusal did not resume creation")
	}
	admission.End()
	reload.Fence = func(int, controlbudget.Deadline) (uint32, error) { return 0, errors.New("census read failure") }
	if response := reload.HandleCluster(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}); response.Code != adminsock.CodeStoreError {
		t.Fatalf("read error misclassified as busy: %+v", response)
	}
}

// Preflight can finish at the end of its own budget without spending the cut's
// five seconds. Inspect the real callback boundary before any store is closed.
// origin: hot-update S6 internal review rv-ctl-11 / plan §3.2
func TestBrokerReloadFreezeStartsWithFreshDeadline(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerFreshCutDeadline(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "reload separate deadlines x5", g)
	testharness.AssertNoFDLeak(t, "reload separate deadlines x5", fds, 0)
}

func exerciseBrokerFreshCutDeadline(t *testing.T) {
	t.Helper()
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("capture stop before freeze")
	stale := controlbudget.Deadline(1)
	var preflight controlbudget.Deadline
	r := &StoreRun[int]{Slot: slot, Capture: func(_ int, d controlbudget.Deadline) (Config, error) {
		left, err := controlbudget.Remaining(d)
		if err != nil || left < 4*time.Second || d <= preflight {
			t.Errorf("freeze reused preflight/admission deadline: %v %s", err, left)
		}
		return Config{}, refusal
	}, Freeze: Freeze, Preflight: func(_ int, d controlbudget.Deadline) error {
		preflight = d
		if _, err := controlbudget.Remaining(d); err != nil {
			t.Errorf("preflight reused expired admission: %v", err)
		}
		return nil
	}, PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil }, InstallCurrent: func(int) error { return nil },
		Recover: func(int, Cut, storage.StoreSeal, controlbudget.Deadline) error { return nil }, DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/unused"}
	err = ReloadCurrentStore(r, 0, StoreShutdown{}, StoreRunCallbacks[int]{Deadline: func() (controlbudget.Deadline, error) { return stale, nil }, Phase: func(int, string) {}, Resume: func(int) {}, Bind: func(int) {}, Armed: func(int, error) {}, Exec: func(*handoff.Prepared, Bundle, string, []string) error { return fmt.Errorf("unexpected exec") }})
	if !errors.Is(err, refusal) {
		t.Fatalf("capture refusal lost: %v", err)
	}
}
