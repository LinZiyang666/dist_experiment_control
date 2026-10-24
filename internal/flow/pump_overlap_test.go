//go:build linux

package flow

import (
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review rv-flow-4
func TestAppliedOverlapWritesOnlyUnappliedSuffix(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAppliedOverlapSuffix(t)
	}
	testharness.AssertNoGoroutineLeak(t, "applied overlap suffix", beforeG)
	testharness.AssertNoFDLeak(t, "applied overlap suffix", beforeFD, 0)
}

func exerciseAppliedOverlapSuffix(t *testing.T) {
	t.Helper()
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	receive(t, m, req, incoming(req, KindData, 0, []byte("012345")))
	waitFlow(t, m, req.ID, func(s State) bool { return s.RxApplied == 6 })
	got := make([]byte, 6)
	if _, err := io.ReadFull(peer, got); err != nil || string(got) != "012345" {
		t.Fatalf("initial bytes: %q %v", got, err)
	}
	receive(t, m, req, incoming(req, KindData, 4, []byte("45XYZ")))
	got = make([]byte, 3)
	if _, err := io.ReadFull(peer, got); err != nil || string(got) != "XYZ" {
		t.Fatalf("applied overlap corrupted new suffix: %q %v", got, err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.RxApplied == 9 && s.RxReceived == 9 })
	_ = peer.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if n, err := peer.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("overlap delivered excess bytes")
	}
}
