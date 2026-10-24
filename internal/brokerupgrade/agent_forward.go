// agent_forward.go keeps one agent's upgrade-tx forward from delaying any
// other. The broker serves every agent's upgrade-tx requests from ONE wildcard
// subscription, and NATS delivers a subscription's messages serially. When the
// forward waited for the agent's reply on that goroutine, a single agent that
// never answered (crashed, partitioned, or replaced by its own exec) held the
// subscription for the whole forward timeout (30 s), and every other agent's
// upgrade traffic queued behind it — FIX-DESCRIBE measured wait=31 s on about
// a quarter of real cuts before the agent learned to refuse early.
// origin: S8 BROKER-EXPORTER R-9 (FIX-DESCRIBE request R-9); hot-update S8 plan §7.2 / §11.2 P15
package brokerupgrade

import (
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// AgentForwardLimit bounds one broker's forwards in flight. Each holds one
// goroutine and one private-inbox request until its own deadline; the bound
// keeps a fleet of silent agents from growing that without limit. A request
// beyond it is refused before anything is sent (a plain 75 deferral).
const AgentForwardLimit = 64

// AgentForwarder relays admitted agent transactions off the subscription
// goroutine. The zero value is ready to use.
type AgentForwarder struct{ inflight atomic.Int64 }

// Forward answers one upgrade-tx request exactly once: respond (only when the
// request named a reply inbox) and then audit (for the outcomes the shared
// policy audits). The caller binds respond to the message it received, so the
// answer always goes to this request's own reply inbox. Ownership/node
// denials and every pre-send refusal (inbox, decoding, actor, capability —
// all store reads) are answered on the caller's goroutine, before any later
// message is read. Only the relay to the agent runs on its own goroutine,
// with the request's ORIGINAL deadline — receipt plus r.Timeout — so moving
// it off the subscription goroutine can never extend what the requester was
// promised. A full forwarder refuses instead of queueing: a queued request
// would spend its deadline waiting.
func (f *AgentForwarder) Forward(admitted bool, denialCode, denialDetail string, r AgentRoute, respond func(body []byte), audit func(code string)) {
	if r.Received.IsZero() {
		r.Received = time.Now()
	}
	deadline := r.Received.Add(r.Timeout)
	reply := func(body []byte, code string) {
		if r.Reply != "" {
			respond(body)
		}
		if audit != nil && AuditAgentTransaction(admitted, denialCode) {
			audit(code)
		}
	}
	if !admitted {
		reply(encodeAgentResponse(upgradewire.Response{Code: denialCode, Error: denialDetail}))
		return
	}
	forward, err := admitAgentTransaction(r)
	if err != nil {
		reply(encodeAgentResponse(upgradewire.ErrorResponse(err)))
		return
	}
	if f.inflight.Add(1) > AgentForwardLimit {
		f.inflight.Add(-1)
		reply(encodeAgentResponse(upgradewire.ErrorResponse(refusal(upgradetx.CodeDeferred, "agent transaction forwarding is at capacity"))))
		return
	}
	go func() {
		body, code, err := forward.relay(r, time.Until(deadline))
		// The slot covers the wait on the agent only; it is free before the
		// reply is written, so an answered request never counts against the next.
		f.inflight.Add(-1)
		if err != nil {
			body, code = encodeAgentResponse(upgradewire.ErrorResponse(err))
		}
		reply(body, code)
	}()
}
