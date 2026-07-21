package agent

import (
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// home_push.go (R8a P1) — the agent end of the ACTIVE home-directive channel.
//
// WHAT WAS BROKEN
// ---------------
// This agent applied home directives from exactly ONE source: the register reply
// (applyReconciliation -> applyHomeDirectives). It registers ONCE per NATS
// connection and its heartbeat is fire-and-forget, so the only thing that could
// ever deliver a rehome was a nats.ReconnectHandler firing. A `cluster drain` on the
// broker side stops nothing and disconnects nobody, so a healthy, quiet agent had NO
// path to learn its expose had been moved. The tunnel stayed pinned to the drained
// broker indefinitely.
//
// THE FIX IS NOT "ALSO PUSH"
// --------------------------
// A push alone would be an EDGE, and an edge dropped on the floor (agent briefly
// unreachable, publish lost) is silently gone forever — the same class of bug. The
// broker therefore re-delivers on a reconcile pass until this agent CONFIRMS the new
// home, and this file is the confirming end.
//
// The ack is deliberately an APPLIED ack, not a received ack: it is published from
// the SUCCESS path of applyOneHome (ApplyHome returned nil, a tunnel session exists,
// the new home was persisted). Acking on receipt would let the broker call a failing
// rehome "converged", and the broker's rc semantics for drain/retire are built on
// this ack — a received-ack would make `cluster drain` return rc=0 on a data plane
// that never moved, which is the exact bug being fixed.

// handleHomeForwarded applies a pushed HomeAssignment. The payload is the SAME
// proto.HomeAssignment the register reply carries, and it is fed to the SAME
// applyHomeDirectives — so a pushed rehome and a reconnect rehome are literally the
// same code path (epoch-monotone, per-port single retry loop, non-tearing at equal
// epoch). No second policy exists to drift.
//
// msg.Reply is the broker's ack inbox; it is recorded per port so applyOneHome can
// ack from its success path (which runs later, on its own goroutine).
func (a *Agent) handleHomeForwarded(_ *nats.Conn, msg *nats.Msg) {
	var ha proto.HomeAssignment
	if err := json.Unmarshal(msg.Data, &ha); err != nil {
		a.cfg.Logger.Warn("agent: home push parse", "err", err)
		return
	}
	if len(ha.Directives) == 0 {
		return
	}
	ctx := a.loadRunCtx()
	if ctx == nil || ctx.Err() != nil {
		return // mid-shutdown: dropping is correct, the broker re-delivers
	}
	if msg.Reply != "" {
		a.rehomeMu.Lock()
		for _, d := range ha.Directives {
			a.rehomeAckTo[d.PublicPort] = msg.Reply
		}
		a.rehomeMu.Unlock()
	}
	a.cfg.Logger.Info("agent: home directives pushed", "count", len(ha.Directives))
	a.applyHomeDirectives(ctx, &ha)
}

// ackHomeApplied publishes the APPLIED ack for one port. Called ONLY from the
// success tail of applyOneHome.
//
// A missing ack subject (this port's home came from a register reply, not a push) is
// a silent no-op: the register path never asked for confirmation, and inventing a
// destination would be worse than not acking.
func (a *Agent) ackHomeApplied(d proto.HomeDirective) {
	a.rehomeMu.Lock()
	reply := a.rehomeAckTo[d.PublicPort]
	a.rehomeMu.Unlock()
	if reply == "" {
		return
	}
	nc := a.ncBox.Load()
	if nc == nil {
		return
	}
	// Echo back the directive we actually installed. Re-using HomeAssignment as the
	// ack shape means the channel introduces no new wire type in either direction —
	// and (port, epoch) is a sufficient key because public ports are cluster-unique.
	body, err := json.Marshal(&proto.HomeAssignment{Directives: []proto.HomeDirective{d}})
	if err != nil {
		return
	}
	if err := nc.Publish(reply, body); err != nil {
		a.cfg.Logger.Warn("agent: home applied-ack publish", "err", err, "port", d.PublicPort)
		return
	}
	a.cfg.Logger.Info("agent: home applied-ack", "port", d.PublicPort, "epoch", d.Epoch, "home", d.BrokerAddr)
}

// forgetHomeAck drops a port's ack destination (expose removed). Keeps the map
// bounded by the number of live exposes.
func (a *Agent) forgetHomeAck(port int) {
	a.rehomeMu.Lock()
	delete(a.rehomeAckTo, port)
	a.rehomeMu.Unlock()
}
