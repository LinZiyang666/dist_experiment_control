//go:build linux

package ptysession

import (
	"errors"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 internal review rv-auth RV-AUTH-2
func TestPTYControlRequiresUnexpiredImportedAuthority(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, op := range []string{"keepalive", "resize", "signal", "hangup"} {
			t.Run(op, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				f := startActorFixture(t, "signal.signal(signal.SIGINT,lambda *_: mark('interrupt'))\nmark('ready')\nwait('exit')\n")
				waitMark(t, f.dir, "ready")
				st, _, err := f.a.Freeze(time.Now().Add(actorTestBudget))
				requireDependency(t, err)
				var clock atomic.Int64
				clock.Store(int64(time.Hour))
				cfg := f.a.cfg
				cfg.Clock = func() (int64, error) { return clock.Load(), nil }
				st.AuthorityDeadlineBootNS = clock.Load() + int64(time.Second)
				st.KAIntervalMS = 1000
				st.KADeadlineBootNS = clock.Load() + int64(180*time.Second)
				originalKA := st.KADeadlineBootNS
				imported, err := Import(cfg, st)
				requireDependency(t, err)
				// The old frozen actor has no borrowers; transfer its exact owners to
				// the imported actor without opening another descriptor wrapper.
				f.a = imported
				requireDependency(t, f.a.PreActivate())
				requireDependency(t, f.a.Activate())
				clock.Store(st.AuthorityDeadlineBootNS + 1)
				r := ControlRequest{Seq: 1, Op: op}
				if op == "resize" {
					r.Cols, r.Rows = 100, 40
				}
				if op == "signal" {
					r.Signal = int(syscall.SIGINT)
				}
				receipt, err := f.a.ApplyControl(r)
				f.a.mu.Lock()
				revoked, ka := f.a.state.Revoked, f.a.state.KADeadlineBootNS
				f.a.mu.Unlock()
				if !errors.Is(err, ErrRevoked) || receipt.Seq != 0 || ka != originalKA || revoked == "" {
					t.Errorf("expired authority accepted %s: receipt=%+v err=%v revoked=%q KA=%d original=%d", op, receipt, err, revoked, ka, originalKA)
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "authority regression", g0)
	testharness.AssertNoFDLeak(t, "authority regression", fd0, 0)
}

// origin: hot-update S4 internal review rv-auth RV-AUTH-2
func TestPTYDuplicateControlDoesNotRenewExpiredAuthority(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("duplicate-and-zero-authority", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := startActorFixture(t, "mark('ready')\nwait('exit')\n")
			waitMark(t, f.dir, "ready")
			var clock atomic.Int64
			clock.Store(int64(time.Hour))
			f.a.cfg.Clock = func() (int64, error) { return clock.Load(), nil }
			f.a.mu.Lock()
			f.a.state.KAIntervalMS = 1000
			f.a.mu.Unlock()
			q := ControlRequest{Seq: 1, Op: "keepalive"}
			first, err := f.a.ApplyControl(q)
			if err != nil || first.Seq != 1 || first.Error != "" {
				t.Fatalf("zero authority rejected KA: %+v %v", first, err)
			}
			f.a.mu.Lock()
			grant := f.a.state.KADeadlineBootNS
			f.a.state.AuthorityDeadlineBootNS = clock.Load() + 1
			f.a.mu.Unlock()
			clock.Add(int64(time.Second))
			if got, err := f.a.ApplyControl(q); err != nil || got != first {
				t.Fatalf("duplicate receipt changed after expiry: %+v %v", got, err)
			}
			f.a.mu.Lock()
			ka, revoked := f.a.state.KADeadlineBootNS, f.a.state.Revoked
			f.a.mu.Unlock()
			if ka != grant || revoked != "" {
				t.Fatalf("duplicate renewed KA or changed authority: KA=%d want=%d revoked=%q", ka, grant, revoked)
			}
			q.Seq++
			if got, err := f.a.ApplyControl(q); !errors.Is(err, ErrRevoked) || got != first {
				t.Fatalf("new KA revived expired authority: %+v %v", got, err)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "duplicate authority", g0)
	testharness.AssertNoFDLeak(t, "duplicate authority", fd0, 0)
}
