//go:build linux

package brokerimage

import (
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/hashicorp/raft"
	"runtime"
	"testing"
	"time"
)

// origin: S6 internal review rv-final F1
func TestReloadShutdownGuardRefusalKeepsOpenStoreUsable(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseReloadShutdownGuardRefusalKeepsOpenStoreUsable(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestReloadShutdownGuardRefusalKeepsOpenStoreUsable x5", g)
	testharness.AssertNoFDLeak(t, "TestReloadShutdownGuardRefusalKeepsOpenStoreUsable x5", fd, 0)
}

func exerciseReloadShutdownGuardRefusalKeepsOpenStoreUsable(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("role changed in guardedShutdown before Node.Shutdown")
	resumed, closedRecovery := false, false
	phase := ""
	run := &StoreRun[int]{Slot: slot,
		Capture: func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil }, Freeze: Freeze,
		Preflight:           func(int, controlbudget.Deadline) error { return nil },
		PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil },
		InstallCurrent:      func(int) error { return nil },
		Recover: func(_ int, c Cut, s storage.StoreSeal, d controlbudget.Deadline) error {
			closedRecovery = true
			return c.Thaw(d)
		},
		DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(f func() error) error { return f() }, CandidatePath: "/never-executed"}
	err = ReloadCurrentStore(run, 0, StoreShutdown{Prepare: func(controlbudget.Deadline) error { return failure }}, StoreRunCallbacks[int]{
		Deadline: func() (controlbudget.Deadline, error) { return f.cfg.Deadline, nil },
		Phase:    func(_ int, p string) { phase = p }, Resume: func(int) { resumed = true }, Bind: func(int) {}, Armed: func(int, error) {},
		BeforeShutdown: func(int) error { return nil }, Exec: func(*handoff.Prepared, Bundle, string, []string) error { t.Fatal("unexpected exec"); return nil },
	})
	if !errors.Is(err, failure) {
		t.Fatalf("lost refusal: %v", err)
	}
	if f.store.closed || f.store.openN != 0 {
		t.Fatal("fixture unexpectedly touched a store constructor/close")
	}
	if !resumed || phase != proto.BrokerImagePhaseControlReady {
		t.Fatalf("pre-close refusal stranded open store: closedRecovery=%v resumed=%v phase=%s err=%v events=%v", closedRecovery, resumed, phase, err, f.events.snapshot())
	}
}

// origin: S6 internal review rv-final F1
// origin: S10 internal review MAKETEST-BROKER-IMAGE F1
func TestReloadPreparedGuardRefusalResumesOriginalWriter(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseReloadPreparedGuardRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "prepared guard refusal x5", g)
	testharness.AssertNoFDLeak(t, "prepared guard refusal x5", fd, 0)
}

func exerciseReloadPreparedGuardRefusal(t *testing.T) {
	t.Helper()
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			_, transport := raft.NewInmemTransport("refused-cut")
			n, store := clusterStoreRefusalFixture(t, transport)
			if err := n.WaitForLeader(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			// Leadership is visible before the first configuration/no-op commits.
			// Establish an applied writer baseline before Freeze fences admission,
			// so Prepare reaches the intended post-prepare refusal in either arm.
			// origin: S10 internal review MAKETEST-BROKER-IMAGE F1
			if err := n.ApplyMetaSet("t:before-refusal", "same-writer"); err != nil {
				t.Fatal(err)
			}
			index, err := n.AppliedIndex()
			if err != nil || index == 0 {
				t.Fatalf("refusal fixture has no committed writer baseline: index=%d err=%v", index, err)
			}
			writer := n.DB()
			f := newCutFixture(t)
			f.cfg.Mode, f.cfg.Store = ModeSoleVoter, store
			f.control.authHook = func() error { return n.ApplyMetaSet("t:auth-resumed", "same-writer") }
			slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("role or maintenance changed before actual close")
			resumed, closed := false, false
			r := &StoreRun[int]{Slot: slot, Capture: func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil }, Freeze: Freeze, Preflight: func(int, controlbudget.Deadline) error { return nil }, PrepareNodeShutdown: cluster.PrepareImageShutdown, InstallCurrent: func(int) error { return nil }, Recover: func(int, Cut, storage.StoreSeal, controlbudget.Deadline) error {
				t.Error("entered closed recovery")
				return ErrStoreClose
			}, DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/never-executed"}
			err = ReloadCurrentStore(r, 0, StoreShutdown{Prepare: func(d controlbudget.Deadline) error {
				if !prepared {
					return failure
				}
				return r.PrepareNodeShutdown(n, d)
			}, AfterPrepare: func() error { return failure }, Close: func() error { closed = true; return n.Shutdown() }}, StoreRunCallbacks[int]{Deadline: func() (controlbudget.Deadline, error) { return f.cfg.Deadline, nil }, Phase: func(int, string) {}, Resume: func(int) { resumed = true }, Bind: func(int) {}, Armed: func(int, error) {}, BeforeShutdown: func(int) error { return nil }, Exec: func(*handoff.Prepared, Bundle, string, []string) error { t.Fatal("unexpected exec"); return nil }})
			if !errors.Is(err, failure) || !resumed || closed || n.DB() != writer {
				t.Fatalf("original owner was not resumed: err=%v resumed=%v closed=%v", err, resumed, closed)
			}
			if err := writer.Ping(); err != nil {
				t.Fatal(err)
			}
			var baseline string
			if err := writer.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:before-refusal'`).Scan(&baseline); err != nil || baseline != "same-writer" {
				t.Fatalf("refused cut lost the committed writer baseline: value=%q err=%v", baseline, err)
			}
			if err := n.ApplyMetaSet("t:after-refusal", "same-writer"); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"old.auth", "public.listener.resume", "old.business"} {
				found := false
				for _, got := range f.events.snapshot() {
					found = found || got == want
				}
				if !found {
					t.Fatalf("missing restored owner %s: %v", want, f.events.snapshot())
				}
			}
		})
	}
}
