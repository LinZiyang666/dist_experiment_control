package cli

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: S7 internal review rv-wiring F6 / rv-n1compat F2
func TestTransferSelectionCannotDowngradeCreatedInvocation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferCreatedSelection(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseTransferCreatedSelection(t *testing.T) {
	t.Helper()
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	var calls atomic.Int32
	sub, err := nc.Subscribe(proto.SubjCtrlCaps(id.PublicKey, "lab"), func(m *nats.Msg) {
		calls.Add(1)
		_ = m.Respond([]byte(`{"ok":true,"jetstream_ready":true,"max_payload":1048576,"broker_proto":2}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID, cfg.Identity.EntryNonce = "node", "original", "admitted-entry"
	deadline, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := SelectTransferClient(&cfg, deadline)
	if legacy || err == nil || calls.Load() != 0 {
		t.Fatalf("created invocation renegotiated cap0: legacy=%v err=%v caps=%d", legacy, err, calls.Load())
	}
}
