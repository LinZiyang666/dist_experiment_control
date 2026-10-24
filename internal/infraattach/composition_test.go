package infraattach

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infracompose"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The real ordinary-auth fixture and broker recovery owner run here. The graph
// deliberately lacks installed runtime sources; neither an advertised capability
// callback nor an older explicit Dependencies graph may hide that setup error.
// origin: hot-update S9 plan §4.3 / §8 / P22 / NATS-COMPOSITION
func TestBrokerCompositionRejectsIncompleteRuntimeBeforeBusinessDial(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerComposition(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker incomplete composition", beforeG)
	testharness.AssertNoFDLeak(t, "broker incomplete composition", beforeFD, 0)
}

func exerciseBrokerComposition(t *testing.T) {
	t.Helper()
	f := newBrokerAttachFixture(t, true)
	service, domain := serviceConfiguration(t)
	// The old complete-shaped dependency table could construct a Service. A
	// requested production composition must be validated instead of falling back.
	service.Composition = &infracompose.Config{}
	service.Capability = func() *proto.InfraCapability {
		t.Error("incomplete graph evaluated capability")
		return &proto.InfraCapability{Version: 1, NATS: true, Caddy: true}
	}
	config := f.config(&BrokerRecovery{Domain: domain,
		Assets:    func(c.Deadline, c.Stop) (c.AssetSeal, error) { return c.AssetSeal{}, c.ErrIncomplete },
		Exclusive: func(c.Deadline, c.Stop) error { return c.ErrIncomplete },
		Transfers: func(c.Deadline) (c.TransferSeal, error) { return c.TransferSeal{}, c.ErrIncomplete }})
	config.Infra.Service, config.Infra.Pending = service, func() bool { return false }
	owner, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(brokerAttachDeadline(t, 5*time.Second)); err != nil {
			t.Error(err)
		}
	}()
	nc, err := owner.Connect(brokerAttachDeadline(t, 5*time.Second))
	if nc != nil {
		nc.Close()
	}
	if err == nil || owner.service != nil || InfraBindings(owner) != nil || owner.Admitted() {
		t.Fatal("incomplete composition reached business admission")
	}
	if owner.gate == nil {
		t.Fatal("composition did not use the broker's actual recovery construction root")
	}
	recovery, ok := owner.Recovery().(infracompose.Recovery)
	if !ok || recovery.(brokerRecovery).gate != owner.gate {
		t.Fatal("short asset fence detached from original broker recovery gate")
	}
}
