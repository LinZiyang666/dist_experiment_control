// origin: hot-update S8 plan §12.1; S8 BROKERUPGRADE-SLIM responsibility extraction
package agentroute

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

// NATSRequest adapts the broker's own private-inbox connection to Request.
func NATSRequest(nc *nats.Conn) func(string, []byte, time.Duration) ([]byte, error) {
	return func(subject string, data []byte, timeout time.Duration) ([]byte, error) {
		reply, err := nc.Request(subject, data, timeout)
		if err != nil {
			return nil, err
		}
		return reply.Data, nil
	}
}

var controlProbeNonceRE = regexp.MustCompile(`^[0-9a-f]{32}$`)

// WithControlProbe wraps the broker's heartbeat handler. A heartbeat that
// carries a control-probe nonce and a reply inbox is also an S8 target's
// external control probe (§4.2): an agent may not publish on its own
// forwarded subject, so the broker sends a claim probe with that nonce through
// the agent's real forwarded subscriber and relays the agent's answer
// verbatim. The liveness write always runs first and unchanged; the subject
// ACL binds the heartbeat to the agent's own sid/nid, so the relay can only
// ever probe the requester itself. No new subject or broker grant is needed.
// origin: hot-update S8 plan §4.2 / M05; S8 FIX-DESCRIBE
func WithControlProbe(nc *nats.Conn, heartbeat func(*nats.Msg)) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		heartbeat(msg)
		RelayControlProbe(nc, msg)
	}
}

// RelayControlProbe answers one nonce-bound heartbeat request. The relay runs
// off the subscription goroutine, so a silent agent can delay neither this
// broker's nor any other agent's heartbeats, and it is bounded by the control
// request budget. A failed probe sends nothing: the agent's own request
// deadline is its only, and truthful, outcome.
func RelayControlProbe(nc *nats.Conn, msg *nats.Msg) {
	if nc == nil || msg == nil || msg.Reply == "" {
		return
	}
	sid, nid, ok := proto.ParseSidNidFromCtrl(msg.Subject)
	var heartbeat proto.HeartbeatPayload
	if !ok || json.Unmarshal(msg.Data, &heartbeat) != nil || !controlProbeNonceRE.MatchString(heartbeat.ControlProbeNonce) {
		return
	}
	body, _ := json.Marshal(map[string]string{"query_nonce": heartbeat.ControlProbeNonce})
	go func() {
		reply, err := nc.Request(proto.SubjCmdForwarded(sid, nid, proto.ClaimProbeVerb), body, upgradebudget.RequestLatency)
		if err == nil {
			_ = msg.Respond(reply.Data)
		}
	}()
}
