// Agent-side expose / expose-rm handlers. Two responsibilities:
//
//   - Persist the (Name, Port, LocalPort, Token) row to
//     ~/.tether/agent/<sid>/state.json so the tunnel adapter can
//     re-establish the proxy after agent restart (architecture
//     F.6 / K.1).
//
//   - Reconfigure the local tunnel client. The wiring is pluggable
//     through the ExposeAdapter interface — TunnelExposeAdapter is
//     the production implementation (in tunnel_adapter.go).
//     With Adapter == nil the handler still persists state and acks
//     OK; useful for the in-process control-plane tests where TCP
//     forwarding is out of scope.
package agent

import (
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// ExposeAdapter is the seam between agent.handleExpose* and the
// in-process tunnel client. TunnelExposeAdapter is the production
// implementation; control-plane tests pass a nil adapter and rely
// on state.json persistence + audit.
type ExposeAdapter interface {
	// AddProxy is called once per successful expose; the agent has
	// already persisted state.json by the time this fires. Return
	// non-nil to refuse the expose (broker will roll the row back).
	AddProxy(p PortToken) error

	// RemoveProxy is the symmetric drop; broker calls expose-rm.
	// Both name and publicPort are passed because adapters key their
	// internal state by either (TunnelExposeAdapter is port-keyed,
	// the test recordingAdapter is name-keyed). Failures are logged
	// but not propagated — the SQLite row is the source of truth.
	RemoveProxy(name string, publicPort int) error
}

// handleExposeForwarded persists the supplied (Name, Port, LocalPort,
// Token) into state.json and (optionally) tells the frp adapter to
// add the proxy. Replies ExposeForwardedResp{OK} on success or
// ExposeForwardedResp{Code, Error} on any failure — broker rolls
// back the SQLite ALLOCATED row when OK=false.
func (a *Agent) handleExposeForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: expose.req.forwarded without Reply inbox")
		return
	}
	var req proto.ExposeForwardedReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyExposeForwarded(msg, proto.ExposeForwardedResp{
			Code: "json_parse", Error: err.Error(),
		})
		return
	}

	tok := PortToken{
		Name:      req.Name,
		Port:      req.Port,
		LocalPort: req.LocalPort,
		Token:     req.Token,
	}
	if a.stateStore != nil {
		if err := a.stateStore.AddPort(tok); err != nil {
			a.cfg.Logger.Warn("agent: state.json AddPort", "err", err)
			a.replyExposeForwarded(msg, proto.ExposeForwardedResp{
				Code: "state_write_failed", Error: err.Error(),
			})
			return
		}
	}
	if a.cfg.ExposeAdapter != nil {
		if err := a.cfg.ExposeAdapter.AddProxy(tok); err != nil {
			a.cfg.Logger.Warn("agent: ExposeAdapter.AddProxy",
				"err", err, "name", req.Name, "port", req.Port)
			// Roll back the state.json entry so the next reconcile
			// doesn't try to re-establish a proxy we couldn't add.
			if a.stateStore != nil {
				_ = a.stateStore.RemovePort(req.Name)
			}
			a.replyExposeForwarded(msg, proto.ExposeForwardedResp{
				Code: "frpc_failed", Error: err.Error(),
			})
			return
		}
	}
	a.cfg.Logger.Info("agent: expose added",
		"name", req.Name, "port", req.Port, "local", req.LocalPort)
	a.replyExposeForwarded(msg, proto.ExposeForwardedResp{OK: true})
}

// handleExposeRmForwarded prunes the local state.json entry and
// drops the frp proxy. Best-effort: errors are logged, broker doesn't
// wait for ACK (this handler is invoked on a Publish-not-Request).
func (a *Agent) handleExposeRmForwarded(nc *nats.Conn, msg *nats.Msg) {
	var req proto.ExposeRmForwardedReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.cfg.Logger.Warn("agent: expose-rm.forwarded parse", "err", err)
		return
	}
	if a.stateStore != nil {
		if err := a.stateStore.RemovePort(req.Name); err != nil {
			a.cfg.Logger.Warn("agent: state.json RemovePort", "err", err, "name", req.Name)
		}
	}
	if a.cfg.ExposeAdapter != nil {
		if err := a.cfg.ExposeAdapter.RemoveProxy(req.Name, req.Port); err != nil {
			a.cfg.Logger.Warn("agent: ExposeAdapter.RemoveProxy",
				"err", err, "name", req.Name)
		}
	}
	a.cfg.Logger.Info("agent: expose removed", "name", req.Name, "port", req.Port)
}

func (a *Agent) replyExposeForwarded(msg *nats.Msg, resp proto.ExposeForwardedResp) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(&resp)
	_ = msg.Respond(body)
}
