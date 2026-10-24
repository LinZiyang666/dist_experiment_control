package agent

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
func TestAgentCustodyPollingJoinsRunCancellation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentCustodyPolling(t)
	}
	testharness.AssertNoGoroutineLeak(t, "agent custody polling", g)
	testharness.AssertNoFDLeak(t, "agent custody polling", fd, 0)
}
func exerciseAgentCustodyPolling(t *testing.T) {
	t.Helper()
	prior := drillobserve.Hook
	events := make(chan drillobserve.Event, 16)
	drillobserve.Hook = func(e drillobserve.Event) { events <- e }
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: test owns the complete polling lifetime.
	done := make(chan struct{})
	rt := new(execsession.Runtime)
	// origin: hot-update S10 plan §5.3 / P07: follow the moved producer.
	go func() {
		drillobserve.RunRuntimeSamples(ctx.Done(), drillobserve.RuntimeBinding{Domain: "agent", InstanceID: "custody-polling"}, func() { execsession.ObserveRuntime(rt) })
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("polling teardown outlived cancellation")
		}
		drillobserve.Hook = prior
	}()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	// Kernel-bound runtime samples are unsupported off Linux. Returning without
	// emitting fake identity is the required lifetime behavior there.
	if runtime.GOOS != "linux" {
		select {
		case <-done:
			if len(events) != 0 {
				t.Fatal("unsupported sampler fabricated runtime observations")
			}
		case <-timer.C:
			t.Fatal("unsupported sampler did not return")
		}
		return
	}
	domains := map[string]bool{}
	for len(domains) < 2 {
		select {
		case e := <-events:
			if e.Kind == "domain-live" {
				domains[e.Domain] = true
			}
		case <-timer.C:
			t.Fatal("live runtime domains never observed")
		}
	}
	cancel()
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("polling outlived Run cancellation")
	}
}
