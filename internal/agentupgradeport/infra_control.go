// Agent control subscriptions are scoped to the ordinary live NC session.
// origin: S10 internal review MAKETEST-GATES F4
package agentupgradeport

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraattach"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// The S9 bridge has a separate purpose and fixed clock mapping. The S6 exact
// route hold is never relaxed to fit a successor's new physical attachment.
// origin: hot-update S9 plan §4.6 / T50 / M25
func InfraProtectsConnection(nc *nats.Conn) bool {
	client := infraattach.For(nc)
	return client != nil && client.Protects()
}

// Normal subscriptions own the capability, not the releasing farewell. A
// missing local challenge refuses Prepare; the wire cannot reset its clock.
// origin: hot-update S9 plan §4.6 / T50
func BindInfraControl(nc *nats.Conn, sid string, nid func() string, cleanup func(func())) error {
	client := infraattach.For(nc)
	if client == nil || !client.Installed() {
		return nil
	}
	sub, err := nc.Subscribe(proto.SubjInfraMove(sid, nid()), func(msg *nats.Msg) {
		req, err := proto.DecodeInfraMoveRequest(msg.Data, sid, nid())
		var attachment c.Attachment
		d, clockErr := controlbudget.After(5 * time.Second)
		err = errors.Join(err, clockErr)
		if err == nil {
			if req.Proof.Purpose == c.RoutePrepare {
				attachment, err = client.Install(req.Proof, d)
			} else {
				attachment, err = client.Bind(req.Proof, d)
			}
		}
		if err != nil {
			body, _ := json.Marshal(proto.InfraUpgradeResp{Code: proto.InfraCode(err)})
			_ = msg.Respond(body)
			return
		}
		body, _ := json.Marshal(attachment)
		_ = msg.Respond(body)
	})
	if err == nil {
		cleanup(func() { _ = sub.Unsubscribe() })
		err = nc.FlushTimeout(5 * time.Second)
	}
	return err
}
