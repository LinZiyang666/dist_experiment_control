package cli

import (
	"encoding/json"
	"github.com/LinZiyang666/tether/internal/testharness"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Copy into internal/cli. The existing fixture owns and joins its embedded NATS.
// The peer exposes the actual pre-S7 caps subject and no new probe subscription.
// origin: S7 internal review rv-wiring F6
func TestTransferSelectionAcceptsLegacyBrokerCaps(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyBrokerCaps(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}
func exerciseLegacyBrokerCaps(t *testing.T) {
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	body, err := json.Marshal(proto.CapsResp{OK: true, JetStreamReady: true, MaxPayload: 1 << 20, BrokerProto: proto.ProtoVersion})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.Subscribe(proto.SubjCtrlCaps(id.PublicKey, "lab"), func(m *nats.Msg) { _ = m.Respond(body) })
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
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := SelectTransferClient(&cfg, d)
	if err != nil || !legacy {
		t.Fatalf("N-1 caps responder is healthy but precreation negotiation cannot select cap0: legacy=%v err=%v", legacy, err)
	}
}
