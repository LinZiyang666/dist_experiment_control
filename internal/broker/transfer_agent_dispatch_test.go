package broker

import (
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/nats-io/nats.go"
)

// Copy into internal/broker. No socket or actor thread is started: this invokes
// the exact handler created by the production subscription builder.
type reviewDispatchController struct {
	x.Controller
	calls int
}

func (c *reviewDispatchController) Handle(x.Request, x.Principal, x.Deadline) (x.Response, error) {
	c.calls++
	return x.Response{}, nil
}

// origin: S7 internal review rv-wiring F2
func TestTransferAgentRegistrationReachesController(t *testing.T) {
	b := &Broker{}
	controller := &reviewDispatchController{}
	b.transferRuntime.Store(&BrokerTransfers{Controller: controller})
	b.transferAgents.Record("lab", "node", "agent", x.Version)
	id := proto.Identity{SID: "lab", NID: "node", TransferID: "transfer", AgentInstance: "agent", ActorFP: "actor", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	subject, err := proto.SubjEvTransferRequest(id.SID, id.NID, id.TransferID, proto.TransferOpReserve)
	if err != nil {
		t.Fatal(err)
	}
	origin := proto.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	body, err := json.Marshal(proto.TransferReserveReq{CallID: "reserve", Identity: id, Origin: origin, ReceiverInstance: id.AgentInstance})
	if err != nil {
		t.Fatal(err)
	}
	pattern, err := proto.SubjEvTransferRequestWildcard(proto.TransferOpReserve)
	if err != nil {
		t.Fatal(err)
	}
	registered := false
	for _, subscription := range brokerTransferWireSubscriptions(b) {
		if subscription.Subject == pattern {
			registered = true
			subscription.Handler(&nats.Msg{Subject: subject, Reply: "_INBOX.receiver", Data: body})
		}
	}
	if !registered {
		t.Fatal("production descriptor did not register the agent reserve operation")
	}
	if controller.calls != 1 {
		t.Fatalf("registered agent operation silently dropped before authority/controller: calls=%d; descriptor=%+v", controller.calls, xfercontrol.Registrations()[8])
	}
}
