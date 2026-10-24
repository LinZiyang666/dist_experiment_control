package broker

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// One wildcard subscription serves every agent's upgrade-tx requests, and NATS
// delivers its messages one at a time. An agent that never answers (crashed,
// partitioned, or replaced by its own exec) must cost only its own requester
// the forward timeout: another agent's transaction on the same broker is
// answered promptly, each reply reaches exactly its own requester, and the
// silent agent's requester gets the deferral at its ORIGINAL deadline, not a
// longer one. Five rounds over the real embedded NATS bus also prove the
// relay goroutines end with their requests.
// origin: S8 BROKER-EXPORTER R-9 (FIX-DESCRIBE request R-9); hot-update S8 plan §7.2 / §11.2 P15
func TestSilentAgentDoesNotDelayAnotherAgentsTransaction(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSilentAgentForward(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "silent agent forward x5", g)
	testharness.AssertNoFDLeak(t, "silent agent forward x5", fd, 0)
}

func exerciseSilentAgentForward(t *testing.T) {
	t.Helper()
	bus := newS8TxBus(t)
	timeout := bus.b.cfg.UpgradeForwardTimeout()
	if err := node.Register(bus.b.cfg.DB, node.RegisterInput{SID: "lab", NID: "silent", ProtoVersion: proto.ProtoVersion}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"node", "silent"} {
		if err := brokerupgrade.RememberAgentTransaction(bus.b.read().SQL(), func(plan func(*sql.DB) (*cluster.Command, error)) error {
			return maintenanceWrite(bus.b, plan)
		}, "lab", nid, []string{proto.CapUpgradeTXV1}); err != nil {
			t.Fatal(err)
		}
	}
	silentNC, err := nats.Connect(bus.url)
	if err != nil {
		t.Fatal(err)
	}
	defer silentNC.Close()
	delivered := make(chan struct{}, 1)
	if _, err := silentNC.Subscribe(proto.SubjCmdForwarded("lab", "silent", brokerupgrade.SignedTransactionOp), func(*nats.Msg) {
		delivered <- struct{}{} // received, never answered
	}); err != nil {
		t.Fatal(err)
	}
	if err := silentNC.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	silentKey := bus.key
	silentKey.ConfiguredNID = "silent"
	type answer struct {
		response upgradewire.Response
		elapsed  time.Duration
		err      error
	}
	silent := make(chan answer, 1)
	start := time.Now()
	go func() {
		body, _ := upgradewire.EncodeRequest(s8PrepareRequest(silentKey, bus.fp))
		msg, err := bus.ctl.Request(proto.SubjCmdBy("lab", bus.actor, "silent", brokerupgrade.SignedTransactionOp), body, timeout+5*time.Second)
		a := answer{elapsed: time.Since(start), err: err}
		if err == nil {
			a.response, a.err = upgradewire.DecodeResponse(msg.Data)
		}
		silent <- a
	}()
	select {
	case <-delivered:
	case <-time.After(timeout):
		t.Fatal("the silent agent never received its forward")
	}
	// The broker is now waiting on the silent agent. Inline forwarding held
	// this subscription until that wait ended (timeout after `start`).
	asked := time.Now()
	got := bus.send(t, bus.ctl, bus.actor, bus.fp)
	if waited := time.Since(asked); waited > timeout/2 {
		t.Fatalf("an answering agent waited %v behind a silent agent (forward timeout %v)", waited, timeout)
	}
	if got.Code != "" || got.View.Historical.Key != bus.key || bus.forwarded.Load() != 1 {
		t.Fatalf("answering agent's reply was not its own: %+v forwarded=%d", got, bus.forwarded.Load())
	}
	a := <-silent
	if a.err != nil || a.response.Code != upgradetx.CodeDeferred || !strings.Contains(a.response.Error, "reply is unavailable") {
		t.Fatalf("silent agent's requester: %+v %v", a.response, a.err)
	}
	if a.elapsed < timeout-100*time.Millisecond || a.elapsed > timeout+time.Second {
		t.Fatalf("silent agent's requester answered after %v; its original deadline was %v", a.elapsed, timeout)
	}
}
