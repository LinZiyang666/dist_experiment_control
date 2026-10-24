package tunnel

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §9.3 / T56
func TestClientStopWaitsForRootCleanup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseClientStopReceipt(t)
	}
	testharness.AssertNoGoroutineLeak(t, "client root stop receipt", g)
	testharness.AssertNoFDLeak(t, "client root stop receipt", fd, 0)
}
func exerciseClientStopReceipt(t *testing.T) {
	t.Helper()
	canceled, cleaned := make(chan struct{}), make(chan struct{})
	c := &Client{cancel: func() { close(canceled) }, stopped: cleaned}
	stopped := make(chan error, 1)
	go func() { stopped <- StopClient(c, time.Second) }()
	<-canceled
	select {
	case err := <-stopped:
		t.Fatal("stop returned before retained owner cleanup", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(cleaned)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	blocked := &Client{stopped: make(chan struct{})}
	if err := StopClient(blocked, time.Millisecond); err == nil {
		t.Fatal("missing root receipt accepted as cleanup")
	}
}
