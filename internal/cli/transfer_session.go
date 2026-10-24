package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"time"
)

// RequestSessionRemoval carries only explicit operator abandonment authority.
// The broker owns the settlement barrier and exact failed terminals; a client
// timeout cannot authorize a second delete or discard pending transfer details.
// origin: hot-update S7 plan lead LQ1 / T34 / INTEG-D
func RequestSessionRemoval(parent context.Context, nc *nats.Conn, actor, sid string, abandon bool, refusal func(string, string) error) (proto.SessionRmResp, error) {
	body, err := json.Marshal(proto.SessionRmReq{AbandonUnsettledTransfers: abandon})
	if err != nil {
		return proto.SessionRmResp{}, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	msg, err := nc.RequestWithContext(ctx, proto.SubjCtrlSessionRm(actor, sid), body)
	if err != nil {
		return proto.SessionRmResp{}, fmt.Errorf("session rm: request: %w (broker unreachable on NATS)", err)
	}
	var response proto.SessionRmResp
	err = json.Unmarshal(msg.Data, &response)
	if err == nil && !response.OK {
		err = refusal(response.Code, response.Error)
	}
	return response, err
}
