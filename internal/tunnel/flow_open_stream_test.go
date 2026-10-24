//go:build linux

package tunnel

import (
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review rv-flow-5
func TestRejectedOpenReleasesCarrierStream(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRejectedOpenStream(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "rejected OPEN stream", beforeG)
	testharness.AssertNoFDLeak(t, "rejected OPEN stream", beforeFD, 0)
}

func exerciseRejectedOpenStream(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	_ = f.target.Close()
	public, err := net.DialTCP("tcp4", nil, f.endpoint.listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(public.Close)
	_ = public.SetReadDeadline(time.Now().Add(ResumeAttempt))
	if n, err := public.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("refused target did not close public flow: %d %v", n, err)
	}
	f.client.flowState.mu.Lock()
	client := f.peer.attachment
	f.client.flowState.mu.Unlock()
	f.server.mu.Lock()
	broker := f.endpoint.attachment
	f.server.mu.Unlock()
	if client == nil || broker == nil {
		t.Fatal("refused target tore down carrier")
	}
	waitEndpoint(t, func() bool { return client.Mux.NumStreams() == 1 && broker.Mux.NumStreams() == 1 })
	waitEndpoint(t, func() bool { return flow.Retirement(f.client.flowState.manager, f.peer.id).RetiredThrough == 1 })
	client.Mu.Lock()
	binds, streams := len(client.Bound), len(client.Streams)
	client.Mu.Unlock()
	if binds != 0 || streams != 0 || f.client.flowState.cfg.Budget.Used() != 0 || f.server.flowState.cfg.Budget.Used() != 0 {
		t.Fatal("rejected OPEN retained its reservation")
	}
	if r := flow.Retirement(f.client.flowState.manager, f.peer.id); r.RetiredThrough != 1 {
		t.Fatalf("failed connector retained live retirement responsibility: %+v", r)
	}
	select {
	case <-client.Stop:
		t.Fatal("rejection closed healthy carrier")
	default:
	}
}
