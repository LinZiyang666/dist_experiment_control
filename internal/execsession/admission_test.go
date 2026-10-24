//go:build linux

package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S4 internal review rv-fd rv-fd-3 activation recovery stall
func TestHandleBoundsEveryAdmissionWait(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseHandleAdmissionBounds(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "bounded Handle admission", beforeG)
	testharness.AssertNoFDLeak(t, "bounded Handle admission", beforeFD, 0)
}

func exerciseHandleAdmissionBounds(t *testing.T) {
	type pending struct {
		name   string
		f      *runtimeFixture
		done   chan error
		unlock bool
	}
	var requests []pending
	for _, name := range []string{"quiescing", "frozen", "freezing", "activating", "partialActive", "lock"} {
		f := newRuntimeFixture(t, 256)
		req, _ := clockedCreate(t, f.r, f.scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
		failureAwaitRunning(t, f.r, f.scope, req)
		f.r.mu.Lock()
		switch name {
		case "quiescing":
			f.r.quiescing = true
			req.Op, req.Exec = "create", &proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}}
		case "frozen":
			f.r.frozen = true
		case "freezing":
			f.r.freezing = true
		case "activating":
			f.r.activating = true
		case "partialActive":
			f.r.partialActive = true
		}
		if name != "lock" {
			f.r.mu.Unlock()
		}
		done := make(chan error, 1)
		go func() { _, err := f.r.Handle(f.scope, req, "failure-inbox"); done <- err }()
		requests = append(requests, pending{name: name, f: f, done: done, unlock: name == "lock"})
	}
	// All six missing-owner shapes run concurrently against real children.
	// A stuck recovery must neither admit a request nor strand its goroutine.
	deadline := time.NewTimer(upgradebudget.RequestLatency + time.Second)
	defer deadline.Stop()
	for _, p := range requests {
		var err error
		late := false
		select {
		case err = <-p.done:
		case <-deadline.C:
			late = true
		}
		if p.unlock {
			p.f.r.mu.Unlock()
		}
		// These predicates deliberately have no owner. Restore only this
		// injected fixture state before normal child teardown and join.
		p.f.r.mu.Lock()
		p.f.r.quiescing, p.f.r.frozen, p.f.r.freezing = false, false, false
		p.f.r.activating, p.f.r.partialActive = false, false
		p.f.r.cond.Broadcast()
		p.f.r.mu.Unlock()
		p.f.close(t)
		if late {
			err = <-p.done
			// Keep joining the remaining old-code waiters without another delay.
			deadline.Reset(time.Nanosecond)
		}
		if late || !errors.Is(err, ErrRollbackPending) || !errors.Is(err, procrequest.ErrBusy) {
			t.Errorf("%s Handle wait exceeded recovery budget or lost its defined refusal: late=%t err=%v", p.name, late, err)
		}
	}
}
