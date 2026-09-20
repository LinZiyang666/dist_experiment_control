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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/nats-io/nats.go"
)

// The fresh-expose open retry (gotcha #86). The broker forwards an expose the instant the leader
// committed the allocation; the expose's HOME may be a follower that has not applied it yet, and its
// REGISTER answer for the not-yet-visible row is the transient home_catching_up (the same answer the
// epoch ladder gives a not-yet-applied reassign). The budget must stay inside the broker's
// ExposeForwardTimeout (5 s): the broker is holding the ctl's request while this runs, and a reply
// past that window is discarded as agent_no_responders with the allocation rolled back underneath us.
//
// The budget is ONE deadline over the whole ladder, carried as a ctx into every AddProxy (lock wait,
// dial, REGISTER, install) — not a check between calls. External review F1 showed the between-calls
// shape: a 2.6 s transient deny, a 250 ms step, then a 2.6 s success = 5.45 s, each call inside the
// broker's 5 s, the sum past it — the broker had already answered agent_no_responders and rolled the
// allocation back when the agent installed the session and reported OK. Now the second call is cut at
// the deadline, its half-open transport discarded, and the reply says home_catching_up.
const (
	exposeOpenRetryBudget = 3 * time.Second
	exposeOpenRetryStep   = 250 * time.Millisecond
)

// denyIsTransientForExpose reports whether an AddProxy error is a REGISTER deny the home will stop
// giving on its own — home_catching_up (the home has not applied the allocation / reassign yet) or
// try_again (a broker-side store fault). Every other error — a terminal deny, a dial failure, missing
// pins — is final for this attempt.
func denyIsTransientForExpose(err error) bool {
	var de *tunnel.DenyError
	if !errors.As(err, &de) {
		return false
	}
	return de.Reason == proto.ReasonHomeCatchingUp || de.Reason == "try_again"
}

// addProxyWithTransientRetry calls adapter.AddProxy under ctx and, while the error is a transient deny
// and ctx's deadline still holds a step and a further call, sleeps one step and tries again. ctx is the
// whole ladder's deadline: every call is bounded by it, so a call that outlives the budget is cut by the
// adapter and never installs. The last error is returned when the budget is spent (the caller reports a
// transient as home_catching_up), any non-transient error at once. A call cut by the deadline AFTER an
// earlier transient deny returns that deny (wrapped with the cut) — the state of the world the ctl
// should hear is "the home was still catching up", not the closed-conn wording the cut produced.
// Injected sleep keeps the ladder table-testable in milliseconds.
func addProxyWithTransientRetry(ctx context.Context, adapter ExposeAdapter, tok PortToken, step time.Duration, sleep func(time.Duration)) error {
	var lastTransient error
	for {
		err := adapter.AddProxy(ctx, tok)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			if lastTransient != nil {
				// Both wrapped: errors.As finds the DenyError (the code the ctl hears), errors.Is finds
				// the deadline (the reason the ladder stopped) — a log reader gets both.
				return fmt.Errorf("%w (retry cut by the open budget: %w)", lastTransient, err)
			}
			return err
		}
		if !denyIsTransientForExpose(err) {
			return err
		}
		lastTransient = err
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= step {
			return err
		}
		sleep(step)
	}
}

// ExposeAdapter is the seam between agent.handleExpose* and the
// in-process tunnel client. TunnelExposeAdapter is the production
// implementation; control-plane tests pass a nil adapter and rely
// on state.json persistence + audit.
type ExposeAdapter interface {
	// AddProxy is called once per successful expose; the agent has
	// already persisted state.json by the time this fires. Return
	// non-nil to refuse the expose (broker will roll the row back).
	// ctx bounds the OPEN — lock wait, dial, REGISTER, install — and
	// nothing after it; an implementation must not install a session
	// once ctx is done (external review F1: a late success behind a
	// failure the caller already reported leaves the two sides
	// disagreeing about the port).
	AddProxy(ctx context.Context, p PortToken) error

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
	// D6 §7.2/§6.5 (C1 fix): if the broker assigned a home, persist its addr +
	// epoch (so a restart re-targets it) and carry the cert pins TRANSIENTLY into
	// AddProxy (pins are never persisted; re-delivered on register). nil Home ⇒
	// the N=1 single --tunnel-addr path (empty fields, byte-identical state.json).
	if req.Home != nil {
		tok.HomeBrokerAddr = req.Home.BrokerAddr
		tok.Epoch = req.Home.Epoch
		tok.CertPins = req.Home.CertPins
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
		openCtx, cancelOpen := exposeOpenContext(a.loadRunCtx())
		err := addProxyWithTransientRetry(openCtx, a.cfg.ExposeAdapter, tok, exposeOpenRetryStep, time.Sleep)
		cancelOpen()
		if err != nil {
			a.cfg.Logger.Warn("agent: ExposeAdapter.AddProxy",
				"err", err, "name", req.Name, "port", req.Port)
			// Roll back the state.json entry so the next reconcile
			// doesn't try to re-establish a proxy we couldn't add.
			if a.stateStore != nil {
				_ = a.stateStore.RemovePort(req.Name)
			}
			// gotcha #86: a home that was still catching up for the whole budget is reported as the
			// transient it is (the ctl maps home_catching_up to "wait a few seconds and retry", exit
			// 75), not as frpc_failed ("the agent couldn't start the local proxy", exit 64 — which
			// sends the operator to read agent logs for a race the cluster will resolve by itself).
			// Two literal reply sites, not one variable: cmd/tether's error-code coverage gate reads
			// every Code: statically, and that gate is how a code without an exit class gets caught.
			if denyIsTransientForExpose(err) {
				a.replyExposeForwarded(msg, proto.ExposeForwardedResp{
					Code: proto.ReasonHomeCatchingUp, Error: err.Error(),
				})
				return
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
	// R8a P1: the port is gone, so its home-ack destination is too. Without this the
	// ack map would keep one entry per port ever pushed to, for the life of the agent.
	a.forgetHomeAck(req.Port)
	a.cfg.Logger.Info("agent: expose removed", "name", req.Name, "port", req.Port)
}

// exposeOpenContext is the forwarded expose's open deadline: exposeOpenRetryBudget from now, as a
// child of the run ctx so an agent shutdown mid-open cuts the dial too. Before Run has published a
// run ctx (the in-process handler tests) the deadline stands alone. A package function, not a method:
// the Agent type sits at its method budget and this is a pure derivation from one input.
func exposeOpenContext(runCtx context.Context) (context.Context, context.CancelFunc) {
	if runCtx == nil {
		runCtx = context.Background() // ctx-none: nats.go MsgHandler has no ctx and Run has not published one.
	}
	return context.WithTimeout(runCtx, exposeOpenRetryBudget)
}

func (a *Agent) replyExposeForwarded(msg *nats.Msg, resp proto.ExposeForwardedResp) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(&resp)
	_ = msg.Respond(body)
}
