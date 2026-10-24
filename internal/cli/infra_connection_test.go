package cli

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func cliInfraServer(t *testing.T) *ns.Server {
	t.Helper()
	s, err := ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("client fixture not ready")
	}
	return s
}

type invocationObserver struct {
	mu      sync.Mutex
	ordinal uint32
	ids     []string
}

func (o *invocationObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ordinal++
	now, err := controlbudget.Now()
	return controlbudget.Attempt{LogicalID: id, Ordinal: o.ordinal, Phase: phase, StartedMonoNS: now}, err
}
func (o *invocationObserver) Record(a controlbudget.Attempt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ids = append(o.ids, a.LogicalID)
	return nil
}
func (*invocationObserver) Close() error { return nil }

// The server commits the first request but withholds its reply. The original
// invocation retains its handle, NC and observer while the raw CID changes.
// Planned signed attachment/canonical ingress acceptance is independently
// exercised by P30; this test proves the ctl construction and retry seam.
// origin: hot-update S9 plan §4.5 / T51 / M25
func TestCLIInfraMoveRetainsOneInvocation(t *testing.T) {
	s := cliInfraServer(t)
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	responder, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()
	var effects atomic.Int32
	var deliveries atomic.Int32
	committed := make(chan struct{})
	_, err = responder.Subscribe("invocation.req", func(m *nats.Msg) {
		if string(m.Data) != "original-call-id" {
			t.Error("invocation ID changed")
		}
		if deliveries.Add(1) == 1 {
			effects.Add(1)
			close(committed)
			return
		}
		_ = m.Respond([]byte("original-effect"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = responder.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	observer := &invocationObserver{}
	nc, err := ConnectWithin(s.ClientURL(), id, retryDeadline(t, 250*time.Millisecond), BindControlObserver(observer, "original-invocation"), nats.ReconnectWait(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	owner := infraattach.For(nc)
	if owner == nil || owner.Installed() {
		t.Fatal("ctl raw owner missing or unconfigured fixture advertised infra capability")
	}
	oldCID, err := nc.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	_, err = RequestControlWithin(firstCtx, nc, "invocation.req", []byte("original-call-id"), "original-invocation", observer)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing reply became success: %v", err)
	}
	select {
	case <-committed:
	default:
		t.Fatal("lost reply lacked a real effect")
	}
	// Reconnect after the original admission budget has expired.
	// origin: hot-update S9 plan §4.5 / T51
	time.Sleep(200 * time.Millisecond)
	if err = nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cid, _ := nc.GetClientID()
		if nc.IsConnected() && cid != oldCID {
			break
		}
		time.Sleep(time.Millisecond)
	}
	secondCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	reply, err := RequestControlWithin(secondCtx, nc, "invocation.req", []byte("original-call-id"), "original-invocation", observer)
	cancel()
	if err != nil || string(reply.Data) != "original-effect" {
		t.Fatalf("retained invocation could not recover its original effect: %v", err)
	}
	newCID, err := nc.GetClientID()
	if err != nil || newCID == oldCID || infraattach.For(nc) != owner || effects.Load() != 1 || deliveries.Load() != 2 {
		t.Fatal("reconnect replaced invocation, reused attachment or repeated effect")
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.ids) < 4 {
		t.Fatal("connect/private inbox/request attempt receipts missing")
	}
	for _, got := range observer.ids {
		if got != "original-invocation" {
			t.Fatalf("attempt changed logical invocation to %q", got)
		}
	}
}

// origin: hot-update S9 plan §9.5 / P23
func TestCLIInfraLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCLIInfra(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ctl infra attachment x5", beforeG)
	testharness.AssertNoFDLeak(t, "ctl infra attachment x5", beforeFD, 0)
}
func exerciseCLIInfra(t *testing.T) {
	t.Helper()
	s := cliInfraServer(t)
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// The legacy public constructor and ConnectWithin share the same owner.
	nc, err := ConnectNATSWithNkey(s.ClientURL(), id, nats.ReconnectWait(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	owner := infraattach.For(nc)
	if owner == nil {
		t.Fatal("unbounded ctl constructor bypassed raw ownership")
	}
	oldCID, _ := nc.GetClientID()
	if err = nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cid, _ := nc.GetClientID()
		if nc.IsConnected() && cid != oldCID {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(retryDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if !nc.IsClosed() {
		t.Fatal("raw owner Close did not retire the original NC")
	}
}
