//go:build linux

package workload_test

import (
	"bytes"
	"errors"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §3.4 / TestReplayACKKeepsCapacityReservation
func TestReplayACKKeepsCapacityReservation(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseReplayACKReservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "replay ACK reservation", goroutines)
	testharness.AssertNoFDLeak(t, "replay ACK reservation", fds, 0)
}

func exerciseReplayACKReservation(t *testing.T) {
	t.Helper()
	const capacity = workload.DefaultReplayCapacity
	budget := &workload.ReplayBudget{Limit: capacity}
	log, err := workload.NewReplay(budget, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if budget.Used() != capacity || workload.ReplayReservedBytes(log) != capacity || workload.ReplayReservedBytes(nil) != 0 {
		t.Fatal("empty log did not reserve its full capacity")
	}
	payload := bytes.Repeat([]byte{0xa7}, int(capacity))
	if n, err := log.Append(payload); n != len(payload) || err != nil {
		t.Fatalf("fill ring: n=%d err=%v", n, err)
	}
	for _, ack := range []uint64{uint64(capacity / 2), uint64(capacity / 2), 1, uint64(capacity)} {
		if err := log.Ack(ack); err != nil {
			t.Fatal(err)
		}
		if got := budget.Used(); got != capacity || workload.ReplayReservedBytes(log) != capacity {
			t.Fatalf("ACK released capacity reservation: got=%d want=%d", got, capacity)
		}
		if extra, err := workload.NewReplay(budget, 1); extra != nil || !errors.Is(err, workload.ErrBackpressure) {
			if extra != nil {
				_ = extra.Close()
			}
			t.Fatalf("ACK admitted another owner: %v", err)
		}
	}
	if log.Snapshot().Base != uint64(capacity) || log.Snapshot().End != uint64(capacity) {
		t.Fatal("ACK failed to free all retained bytes")
	}
	if n, err := log.Append(payload); n != len(payload) || err != nil {
		t.Fatalf("ACK did not allow full refill: n=%d err=%v", n, err)
	}
	state, _, err := log.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if state.End != 2*uint64(capacity) || budget.Used() != capacity {
		t.Fatal("freeze lost refilled bytes or reservation")
	}
	file, detached, err := log.Detach()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if budget.Used() != 0 || workload.ReplayReservedBytes(log) != 0 {
		t.Fatal("detach retained a reservation")
	}
	restored, err := workload.RestoreReplay(budget, detached, file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	if budget.Used() != capacity || workload.ReplayReservedBytes(restored) != capacity {
		t.Fatal("restore did not charge the same owner")
	}
	if err := log.Close(); err != nil || budget.Used() != capacity {
		t.Fatal("closing detached wrapper released another owner's reservation")
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil || budget.Used() != 0 || workload.ReplayReservedBytes(restored) != 0 {
		t.Fatal("close did not release exactly once")
	}
}
