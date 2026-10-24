package broker

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/nats-io/nats.go"
)

type activationController struct {
	x.Controller
	calls int
}

func (c *activationController) Handle(x.Request, x.Principal, x.Deadline) (x.Response, error) {
	c.calls++
	return x.Response{}, nil
}

// A registered instance alone cannot authorize the production ingress. The
// explicit graph adapter remains available to hosts that install it themselves.
// origin: hot-update S7 plan §6.3 / T39; INTEG-G / INTEG-M semantic merge
func TestTransferProductionIngressRequiresInstanceProof(t *testing.T) {
	b, graph, probe := newWiringGraph(t)
	id := probe.status.Identity
	id.ActorFP, id.IntentDigest = strings.Repeat("a", 64), strings.Repeat("b", 64)
	probe.status.Identity, probe.census.Rows[0].Identity = id, id
	controller := &activationController{}
	graph.Controller = controller
	graph.Link = &xferserve.Link{Conn: func() *nats.Conn { return nil }}
	b.transferRuntime.Store(graph)
	b.transferAgents.Record(id.SID, id.NID, id.AgentInstance, x.Version)
	request := x.Request{Op: x.OpCancel, CallID: "cancel-original", Identity: id, Origin: graph.Origin,
		Cancel: &x.CancelRequest{CallID: "cancel-original", Origin: graph.Origin, Reason: x.CancelUser}}
	subject, body, err := proto.EncodeAgentTransferRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	msg := &nats.Msg{Subject: subject, Reply: "reply", Data: body}
	registration := xfercontrol.Registration{Operation: x.OpCancel, Role: x.RoleAgent}
	handleBrokerTransferRequest(b, registration, msg)
	if controller.calls != 0 {
		t.Fatal("unsigned production request reached the controller")
	}
	graph.Link = nil
	handleBrokerTransferRequest(b, registration, msg)
	if controller.calls != 1 {
		t.Fatal("explicit graph lost its registered-instance adapter")
	}
}

// Automatic production assembly has the same pre-readiness capability as an
// explicit factory. A cold graph must never divert a capable client into cap0.
// origin: hot-update S7 plan §9 / T39; INTEG-M default factory activation
func TestTransferDefaultFactoryAdvertisesBeforeGraphInstallation(t *testing.T) {
	b := &Broker{}
	if got := brokerTransferVersion(b); got != "" {
		t.Fatalf("no-JetStream broker advertised %q", got)
	}
	setBrokerJS(b, wiringJS{})
	if got := brokerTransferVersion(b); got != x.Version {
		t.Fatalf("starting production graph advertised %q", got)
	}
}
