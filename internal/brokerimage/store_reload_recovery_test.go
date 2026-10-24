//go:build linux

package brokerimage

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/hashicorp/raft"
)

// Role or membership drift discovered after Freeze is still a reversible
// refusal. It must precede the irreversible close marker and leave the same
// writer/control graph usable, without opening a replacement pool.
// origin: hot-update S6 plan §5 / T38 / internal review rv-cluster-7
func TestReloadAdmissionRefusalPrecedesStoreClosing(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseReloadAdmissionRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "reload admission refusal x5", g)
	testharness.AssertNoFDLeak(t, "reload admission refusal x5", fd, 0)
}

func exerciseReloadAdmissionRefusal(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("membership changed after admission")
	shutdowns, executions := 0, 0
	resumed := false
	phase := ""
	run := &StoreRun[int]{Slot: slot, Capture: func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil }, Freeze: Freeze,
		Preflight: func(int, controlbudget.Deadline) error { return nil }, PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil },
		InstallCurrent: func(int) error { return nil }, Recover: func(int, Cut, storage.StoreSeal, controlbudget.Deadline) error {
			t.Error("refused admission entered closed-store recovery")
			return ErrStoreClose
		},
		DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/not-executed"}
	err = ReloadCurrentStore(run, 0, StoreShutdown{Close: func() error { shutdowns++; return nil }}, StoreRunCallbacks[int]{
		Deadline: func() (controlbudget.Deadline, error) { return f.cfg.Deadline, nil }, Phase: func(_ int, value string) { phase = value }, Resume: func(int) { resumed = true }, Bind: func(int) {}, Armed: func(int, error) {},
		BeforeShutdown: func(int) error { return failure }, Exec: func(*handoff.Prepared, Bundle, string, []string) error { executions++; return nil },
	})
	if !errors.Is(err, failure) || shutdowns != 0 || executions != 0 || !resumed || phase != proto.BrokerImagePhaseControlReady || f.store.openN != 0 {
		t.Fatalf("reversible refusal crossed store boundary: err=%v shutdown=%d exec=%d resumed=%v phase=%s opens=%d", err, shutdowns, executions, resumed, phase, f.store.openN)
	}
	for _, event := range f.events.snapshot() {
		if event == "writer.close" || event == "store.close" {
			t.Fatalf("refused admission closed current writer: %v", f.events.snapshot())
		}
	}
}

// After the store close was ATTEMPTED (CloseForImage ran and failed at the
// writer close), the cut must not resume business on that store as if it were
// pristine: the plan's close-unknown row requires a fenced single owner and no
// Thaw of the original graph.
// origin: hot-update S6 internal review rv-writer
func TestReloadCloseFailureIsNotTreatedAsOpenStore(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("writer-close-%d", round), func(t *testing.T) { exerciseReloadCloseFailure(t, false) })
		t.Run(fmt.Sprintf("node-shutdown-%d", round), func(t *testing.T) { exerciseReloadCloseFailure(t, true) })
	}
	testharness.AssertNoGoroutineLeak(t, "reload unknown store close", beforeG)
	testharness.AssertNoFDLeak(t, "reload unknown store close", beforeFD, 0)
}

func exerciseReloadCloseFailure(t *testing.T, shutdownFailure bool) {
	t.Helper()
	f := newCutFixture(t)
	f.events.fail = "writer.close" // CloseForImage reaches writer close and fails
	var thawErr error
	var phase string
	resumed := false
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil },
		func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	run := &StoreRun[int]{
		Slot:                slot,
		Capture:             func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil },
		Freeze:              Freeze,
		Preflight:           func(int, controlbudget.Deadline) error { return nil },
		PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil },
		InstallCurrent:      func(int) error { return nil },
		Recover: func(_ int, cut Cut, seal storage.StoreSeal, deadline controlbudget.Deadline) error {
			if shutdownFailure {
				for _, phase := range []storage.ClosePhase{seal.Writer, seal.ReadOnly, seal.Bolt, seal.Transport, seal.Raft} {
					if phase != storage.CloseUnknown {
						t.Errorf("shutdown failure misreported phase %d", phase)
					}
				}
			} else if seal.Writer != storage.CloseUnknown {
				t.Error("failed writer close has no unknown receipt")
			}
			if StoreSealClosed(seal) {
				t.Error("failed close certified a closed store")
			}
			if err := cut.Discard(); err == nil {
				t.Error("unknown close discarded its only owner")
			}
			thawErr = cut.Thaw(deadline) // the only rollback primitive the cut offers
			return thawErr
		},
		DrainArmed:    func(controlbudget.Deadline) error { return nil },
		WithExecLatch: func(fn func() error) error { return fn() },
		CandidatePath: "/nonexistent",
	}
	err = ReloadCurrentStore(run, 0, StoreShutdown{Close: func() error {
		if shutdownFailure {
			return errors.New("injected shutdown failure")
		}
		return nil
	}}, StoreRunCallbacks[int]{
		Deadline: func() (controlbudget.Deadline, error) { return f.cfg.Deadline, nil },
		Phase:    func(_ int, value string) { phase = value }, Resume: func(int) { resumed = true }, Bind: func(int) {}, Armed: func(int, error) {},
		Exec: func(*handoff.Prepared, Bundle, string, []string) error { return errors.New("unreachable") },
	})
	if err == nil {
		t.Fatal("close failure must not report success")
	}
	if !errors.Is(thawErr, ErrStoreClose) {
		t.Fatalf("cut resumed business on a store whose close was attempted and failed; events=%v reopen=%d",
			f.events.snapshot(), f.store.openN)
	}
	if resumed || phase != proto.BrokerImagePhaseFailed || f.store.openN != 0 {
		t.Fatalf("unknown writer resumed admission or reopened: resumed=%v phase=%s opens=%d", resumed, phase, f.store.openN)
	}
	for _, event := range f.events.snapshot() {
		if strings.HasSuffix(event, ".auth") || strings.HasSuffix(event, ".business") || strings.HasSuffix(event, ".transfers.resume") {
			t.Fatalf("unknown writer resumed original graph: %v", f.events.snapshot())
		}
	}
}

// The failed transport really closes its sockets, but its error still makes
// that one phase unknown. Recovery must receive the Node's actual per-object
// receipt instead of zero phases or a fabricated fully closed store.
// origin: hot-update S6 plan §3.2 / T10; internal review rv-writer-6
func TestReloadClusterShutdownFailureKeepsActualClosePhases(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseReloadClusterCloseReceipt(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "reload cluster partial close receipt", beforeG)
	testharness.AssertNoFDLeak(t, "reload cluster partial close receipt", beforeFD, 0)
}

type failedCloseTransport struct {
	*raft.InmemTransport
	failure error
}

func (tr *failedCloseTransport) Close() error {
	return errors.Join(tr.InmemTransport.Close(), tr.failure)
}

func exerciseReloadClusterCloseReceipt(t *testing.T) {
	t.Helper()
	_, transport := raft.NewInmemTransport("refused-cut")
	failure := errors.New("injected transport close uncertainty")
	node, store := clusterStoreRefusalFixture(t, &failedCloseTransport{InmemTransport: transport, failure: failure})
	f := newCutFixture(t)
	f.cfg.Store, f.cfg.Mode = store, ModeSoleVoter
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recovered := false
	run := &StoreRun[int]{
		Slot: slot, Capture: func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil },
		Freeze: Freeze, Preflight: func(int, controlbudget.Deadline) error { return nil },
		PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil },
		InstallCurrent:      func(int) error { return nil },
		Recover: func(_ int, cut Cut, seal storage.StoreSeal, deadline controlbudget.Deadline) error {
			recovered = true
			actual, err := cluster.ClosedImageStore(node)
			if err == nil || actual.Transport != storage.CloseUnknown || actual.Writer != storage.CloseClosed {
				t.Fatal("fixture did not close the writer with only the transport receipt unknown")
			}
			if !reflect.DeepEqual(actual, seal) {
				t.Error("recovery lost the actual cluster close phases")
			}
			return cut.Thaw(deadline)
		},
		DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/nonexistent",
	}
	err = ReloadCurrentStore(run, 0, StoreShutdown{Close: func() error { return node.Shutdown() }}, StoreRunCallbacks[int]{
		Deadline: func() (controlbudget.Deadline, error) { return f.cfg.Deadline, nil }, Phase: func(int, string) {},
		Resume: func(int) { t.Error("partial close resumed admission") }, Bind: func(int) {}, Armed: func(int, error) {},
		Exec: func(*handoff.Prepared, Bundle, string, []string) error {
			t.Error("partial close reached exec")
			return nil
		},
	})
	if !recovered || !errors.Is(err, failure) || !errors.Is(err, ErrStoreClose) {
		t.Fatalf("partial cluster close was not fenced: recovered=%v err=%v", recovered, err)
	}
}
