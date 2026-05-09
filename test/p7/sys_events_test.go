package p7_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)


// collectEvents subscribes to sys.events on the given conn and
// returns a snapshot func plus an unsubscribe. Tests use it to
// assert that the right kinds fired without polling.
func collectEvents(t *testing.T, nc *nats.Conn) (func() []map[string]any, func()) {
	t.Helper()
	var (
		mu     sync.Mutex
		events []map[string]any
	)
	sub, err := nc.Subscribe(proto.SubjSysEvents, func(m *nats.Msg) {
		var ev map[string]any
		if err := json.Unmarshal(m.Data, &ev); err == nil {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		out := make([]map[string]any, len(events))
		copy(out, events)
		return out
	}
	stop := func() { _ = sub.Unsubscribe() }
	return snap, stop
}

// TestTetherdRestartedEmittedOnBoot pins that broker startup pubs
// the tetherd_restarted event so ops monitoring can correlate
// restarts with downstream behavior.
func TestTetherdRestartedEmittedOnBoot(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)

	// Subscribe BEFORE the broker boots so we don't miss the early
	// tetherd_restarted event.
	nc, _ := nats.Connect(url)
	defer nc.Close()
	snap, stop := collectEvents(t, nc)
	defer stop()

	defer startBroker(t, url, db)()

	// Wait for the event to land in the events stream too.
	time.Sleep(200 * time.Millisecond)
	js, _ := jetstream.New(nc)
	stream, err := js.Stream(context.Background(), jsstream.EventsStreamName)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := stream.Info(context.Background())
	if info == nil || info.State.Msgs == 0 {
		t.Fatal("events stream has 0 msgs after broker boot")
	}

	got := snap()
	found := false
	for _, ev := range got {
		if ev["type"] == "tetherd_restarted" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected tetherd_restarted in sys.events; got %+v", got)
	}
}

// TestAgentRegisteredEmittedOnHandshake pins that successful node
// register emits agent_registered.
func TestAgentRegisteredEmittedOnHandshake(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	nc, _ := nats.Connect(url)
	defer nc.Close()
	snap, stop := collectEvents(t, nc)
	defer stop()

	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	// Wait for registration round-trip.
	time.Sleep(500 * time.Millisecond)

	got := snap()
	found := false
	for _, ev := range got {
		if ev["type"] == "agent_registered" && ev["sid"] == "lab" && ev["nid"] == "lab-1" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected agent_registered{sid=lab,nid=lab-1}; got %+v", got)
	}
}

// TestDiskPressureFiresAboveThreshold uses a mocked DiskUsageFn to
// drive the broker over the configured threshold. Verifies the
// disk_pressure sys.event is emitted exactly once per spike (no
// per-tick spam) and re-arms after dipping below.
func TestDiskPressureFiresAboveThreshold(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)

	var (
		fakeMu       sync.Mutex
		fakeUsedFrac = 0.10 // start clean
	)
	usageFn := func(_ string) (used, total uint64, err error) {
		fakeMu.Lock()
		defer fakeMu.Unlock()
		const totalBytes = 1_000_000
		return uint64(float64(totalBytes) * fakeUsedFrac), totalBytes, nil
	}

	b, err := broker.New(broker.Config{
		NATSURL:               url,
		DB:                    db,
		Logger:                silentLog(),
		ReconcileInterval:     50 * time.Millisecond,
		StaleAfter:            300 * time.Millisecond,
		OfflineAfter:          900 * time.Millisecond,
		StoreDir:              t.TempDir(),
		DiskCheckInterval:     30 * time.Millisecond,
		DiskPressureThreshold: 0.80,
		DiskUsageFn:           usageFn,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// Let broker subscribe + JS init + monitor start.
	time.Sleep(300 * time.Millisecond)

	nc, _ := nats.Connect(url)
	defer nc.Close()
	snap, stop := collectEvents(t, nc)
	defer stop()

	countPressure := func() int {
		got := snap()
		n := 0
		for _, ev := range got {
			if ev["type"] == "disk_pressure" {
				n++
			}
		}
		return n
	}

	// Phase 1: nothing yet.
	time.Sleep(150 * time.Millisecond)
	if n := countPressure(); n != 0 {
		t.Fatalf("disk_pressure fired prematurely: count=%d", n)
	}

	// Phase 2: cross threshold.
	fakeMu.Lock()
	fakeUsedFrac = 0.85
	fakeMu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countPressure() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if countPressure() < 1 {
		t.Fatal("disk_pressure never fired after threshold cross")
	}

	// Phase 3: stay above threshold for a few ticks; count must NOT
	// keep climbing (edge-trigger guarantee).
	time.Sleep(250 * time.Millisecond)
	if n := countPressure(); n > 1 {
		t.Errorf("disk_pressure spammed while still above threshold: count=%d (want 1)", n)
	}

	// Phase 4: dip below + spike again → expect a second emission.
	fakeMu.Lock()
	fakeUsedFrac = 0.10
	fakeMu.Unlock()
	time.Sleep(100 * time.Millisecond)
	fakeMu.Lock()
	fakeUsedFrac = 0.95
	fakeMu.Unlock()

	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if countPressure() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := countPressure(); n < 2 {
		t.Errorf("disk_pressure did not re-arm after dipping below threshold: count=%d", n)
	}

	cancel()
	<-done
}

