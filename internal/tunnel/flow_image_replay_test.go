//go:build linux

package tunnel

import (
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review HOTUPDATE-T44/T45
func TestRestoredClientCanReplayOriginalExpose(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRestoredExposeReplay(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "restored expose replay x5", beforeG)
	testharness.AssertNoFDLeak(t, "restored expose replay x5", beforeFD, 0)
}

func exerciseRestoredExposeReplay(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	public, target, _ := settlementConnection(t, f, 1)
	frozen, err := f.client.FreezeFlows(time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := frozen.Discard(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	im, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	cfg := f.client.flowState.cfg
	cfg.Budget = &workload.ReplayBudget{}
	restored, salvage, err := RestoreClient(cfg, im, handofftest.DupResources(t, snapshot.Resources))
	if err != nil {
		closeClientImageResources(salvage)
		t.Fatal(err)
	}
	defer func() { restored.Close(f.port); _ = restored.flowState.manager.Close() }()
	if restored.logger == nil || restored.backoffBase <= 0 || restored.backoffMax < restored.backoffBase {
		t.Fatal("restored client lacks operational logger/backoff defaults")
	}
	if err := restored.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	if err := restored.ResumeCarriers(time.Now().Add(DetachedGrace)); err != nil {
		t.Fatal(err)
	}
	restored.Start(t.Context())
	if err := restored.OpenHome(t.Context(), f.port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{}); err != nil {
		t.Fatal(err)
	}
	if _, err := public.Write([]byte("after-replay")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("after-replay"))
	if _, err := io.ReadFull(target, got); err != nil || string(got) != "after-replay" {
		t.Fatalf("original expose after replay: %q %v", got, err)
	}
}
