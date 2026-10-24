//go:build linux

package clusterharness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S8 internal review P13-COHORT F2; hot-update S6 plan §8.1 / T48
func TestCohortTrafficRequiresTheEmittedReadinessEvent(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseCohortReadinessEvent(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "cohort emitted readiness x5", g)
	testharness.AssertNoFDLeak(t, "cohort emitted readiness x5", fds, 0)
}

func exerciseCohortReadinessEvent(t *testing.T) {
	t.Helper()
	f := &brokerProductFixture{test: t, root: t.TempDir()}
	c := newProductCohort(f)
	if err := os.MkdirAll(c.agentHome, 0700); err != nil {
		t.Fatal(err)
	}
	c.opening = &cohortOpening{c: c}
	_, _ = f.logs.Write([]byte("{\"phase\":\"serving\"}\n"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := awaitCohortReady(ctx, c); !errors.Is(err, context.Canceled) {
		t.Fatal("serving status released the application before its readiness event")
	}
	join := c.releaseOnReady()
	defer func() {
		if err := join(); err != nil {
			t.Error(err)
		}
	}()
	_, _ = f.logs.Write([]byte("{\"event\":\"control_ready\",\"cut_id\":\"original-cut\",\"boot_ns\":1}\n"))
	if err := awaitCohortReady(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(c.agentHome, "cohort-opening-release"))
	if err != nil || string(data) != "ready" {
		t.Fatal("the actual readiness watcher did not release the original OPEN")
	}
}

// A refused reload cannot produce a closed-store marker. Preserve the actual
// refusal so a missing upstream port is not misreported as a barrier timeout.
// origin: S10 internal review MAKETEST-BROKER-IMAGE F2
func TestBrokerPreparedBarrierReportsReloadRefusal(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				exerciseBrokerPreparedBarrierRefusal(t)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "prepared barrier refusal x5", g)
	testharness.AssertNoFDLeak(t, "prepared barrier refusal x5", fd, 0)
}

func exerciseBrokerPreparedBarrierRefusal(t *testing.T) {
	t.Helper()
	refused := errors.New("reload refused before store close")
	f := &brokerProductFixture{root: t.TempDir(), done: make(chan struct{}), preparedResponse: make(chan error, 1)}
	f.preparedResponse <- refused
	if err := f.awaitMarker("closed"); !errors.Is(err, refused) || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("prepared barrier lost the reload refusal: %v", err)
	}
	// The successful arming reply still belongs to the later cut checkpoint.
	// Looking for an observation must not steal that reply from its consumer.
	// An unbuffered exchange makes consumption and restoration observable:
	// only the later reply consumer can publish the next barrier's marker.
	f.preparedResponse = make(chan error)
	response, stop, done := f.preparedResponse, make(chan struct{}), make(chan struct{})
	written := make(chan error, 1)
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		select {
		case response <- nil:
		case <-stop:
			return
		}
		select {
		case err := <-response:
			written <- errors.Join(err, os.WriteFile(filepath.Join(f.root, "lifecycle-prepared.json"), []byte("{}"), 0600))
		case <-stop:
		}
	}()
	if err := f.awaitMarker("lifecycle-prepared"); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatalf("prepared barrier lost the arming reply: %v", err)
	}
}
