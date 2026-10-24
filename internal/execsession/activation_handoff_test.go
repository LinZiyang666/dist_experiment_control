//go:build linux

package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 internal review rv-fd rv-fd-3 activation recovery stall
func TestExpiredThawRetainsRecoveryThroughRuntimeLock(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseExpiredThawHandoff(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "expired thaw handoff", beforeG)
	testharness.AssertNoFDLeak(t, "expired thaw handoff", beforeFD, 0)
}

func exerciseExpiredThawHandoff(t *testing.T) {
	f := newRuntimeFixture(t, 256)
	req, _ := clockedCreate(t, f.r, f.scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, f.r, f.scope, req)
	imageFreeze(t, f.r)
	done := make(chan error, 1)
	go func() {
		reply, err := f.r.Handle(f.scope, req, "failure-inbox")
		if err == nil && reply.Phase != "running" {
			err = errors.New("recovery lost the running task")
		}
		done <- err
	}()
	// Register the waiter before the recovery commit. This also makes a
	// missing admission broadcast deterministic, rather than relying on a
	// request that might first enter after the recovery has already finished.
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.activation.changed != nil
	}) {
		f.close(t)
		<-done
		t.Fatal("Handle never registered its admission wait")
	}
	// Manager.FreezeUntil can spend the entire cut deadline. An observer or
	// protocol entrant then wins this short lock exactly at rollback admission.
	// The pending receipt must own recovery even before that lock is available.
	f.r.mu.Lock()
	started := time.Now()
	err := ThawUntil(f.r, started.Add(-time.Millisecond), false)
	queued := f.r.activation.handoff.Load()
	retryErr := ThawUntil(f.r, started.Add(-time.Millisecond), false)
	retained := queued != nil && f.r.activation.handoff.Load() == queued
	elapsed := time.Since(started)
	f.r.mu.Unlock()
	if !errors.Is(err, ErrRollbackPending) || !errors.Is(retryErr, ErrRollbackPending) || !retained || elapsed > 300*time.Millisecond {
		f.close(t)
		<-done
		t.Fatalf("expired rollback did not retain one bounded owner: retained=%t elapsed=%s err=%v retry=%v", retained, elapsed, err, retryErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("retained rollback did not restore admission: %v", err)
		}
	case <-time.After(time.Second):
		f.close(t) // release and join the old-code waiter before failing
		<-done
		t.Fatal("ErrRollbackPending had no owner: Handle remained frozen after the lock was released")
	}
	if f.r.Frozen() {
		t.Fatal("successful recovery left a frozen runtime")
	}
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return f.r.activation.handoff.Load() == nil }) {
		t.Fatal("completed recovery retained its queued owner")
	}
	imageFreeze(t, f.r)
	if err := ThawUntil(f.r, time.Now().Add(time.Second), false); err != nil {
		t.Fatalf("bounded successful recovery lost its confirmation: %v", err)
	}
	exerciseExpiredEmptyThaw(t)
}

func exerciseExpiredEmptyThaw(t *testing.T) {
	f := newRuntimeFixture(t, 256)
	imageFreeze(t, f.r)
	f.r.mu.Lock()
	err := ThawUntil(f.r, time.Now().Add(-time.Millisecond), false)
	f.r.mu.Unlock()
	if !errors.Is(err, ErrRollbackPending) {
		t.Fatalf("empty rollback did not retain its lock handoff: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := f.r.Handle(f.scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1,
			Op: "probe", ActorFP: f.scope.ActorFP, ClientLineage: f.scope.ClientLineage}, "failure-inbox")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("empty cut did not reopen admission: %v", err)
		}
	case <-time.After(time.Second):
		f.close(t)
		<-done
		t.Fatal("empty cut retained partialActive after every owner had completed")
	}
}

// origin: hot-update S4 internal review rv-fd rv-fd-3 activation recovery stall
func TestDelayedThawCannotActivateLaterCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseDelayedThawGeneration(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "generation-scoped thaw handoff", beforeG)
	testharness.AssertNoFDLeak(t, "generation-scoped thaw handoff", beforeFD, 0)
}

func exerciseDelayedThawGeneration(t *testing.T) {
	f := newRuntimeFixture(t, 256)
	req, _ := clockedCreate(t, f.r, f.scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, f.r, f.scope, req)
	imageFreeze(t, f.r)
	// Suspend a handoff between reading the generation and publishing its
	// owner. A separate legal thaw and recut can win that scheduling window.
	queued := &thawHandoff{generation: f.r.activation.generation.Load(), done: make(chan struct{})}
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	imageFreeze(t, f.r)
	if queued.generation == f.r.activation.generation.Load() {
		t.Fatal("a new cut reused the previous recovery generation")
	}
	if !f.r.activation.handoff.CompareAndSwap(nil, queued) {
		t.Fatal("fixture already had a thaw owner")
	}
	go completeThawHandoff(f.r, queued, time.Now().Add(time.Second), false)
	if err := awaitThawHandoff(queued, time.Now().Add(time.Second)); !errors.Is(err, ErrRollbackPending) {
		t.Fatalf("stale rollback was not refused: %v", err)
	}
	if !f.r.Frozen() {
		t.Fatal("a delayed previous-generation rollback activated the new cut")
	}
	if err := f.r.Thaw(); err != nil {
		t.Fatalf("stale owner prevented the current cut's real thaw: %v", err)
	}
	queued = &thawHandoff{generation: f.r.activation.generation.Load(), done: make(chan struct{})}
	imageFreeze(t, f.r)
	if !f.r.activation.handoff.CompareAndSwap(nil, queued) {
		t.Fatal("fixture already had a replacement owner")
	}
	// The current rollback arrives while the delayed previous job still
	// occupies the slot. It must retain its own owner, rather than treating
	// that stale acknowledgement as a promise to restore the current cut.
	err := ThawUntil(f.r, time.Now().Add(time.Second), false)
	go completeThawHandoff(f.r, queued, time.Now().Add(time.Second), false)
	<-queued.done // join the displaced owner before fixture teardown
	if err != nil || f.r.Frozen() || f.r.activation.handoff.Load() != nil {
		t.Fatalf("current rollback joined an obsolete owner instead of replacing it: err=%v frozen=%t", err, f.r.Frozen())
	}
}
