package broker

import (
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// An applied ack is convergence evidence only when it is correlated with a
// directive this broker issued to the owning agent.  Every agent can publish to
// _INBOX.>, so accepting an unsolicited port/epoch lets one agent forge another
// session's data-plane convergence.
func TestCodexReviewHomeAckMustMatchAnIssuedDirective(t *testing.T) {
	b := &Broker{}
	ha := proto.HomeAssignment{Directives: []proto.HomeDirective{{
		PublicPort: 14000,
		Epoch:      999,
		BrokerAddr: "attacker.invalid:7000",
	}}}
	body, err := json.Marshal(&ha)
	if err != nil {
		t.Fatal(err)
	}

	b.handleHomeAck(&nats.Msg{Data: body})
	if got := b.homeAppliedEpoch(14000); got != 0 {
		t.Fatalf("unsolicited home ack was accepted as convergence evidence: epoch=%d", got)
	}
}
