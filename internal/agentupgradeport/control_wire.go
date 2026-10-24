// control_wire.go holds what the agent says on the control wire about its
// transaction capability: the register capability list, the nonce-bound
// heartbeat its external control probe sends, and the early acceptance reply
// of a Start whose activation is about to exec. They are pure leaves of the
// engine (agentupgrade), kept here so that package stays under its
// structural threshold.
// origin: hot-update S8 plan §4.2 / §6.1 / §7.1 / §7.2; S8 FIX-DESCRIBE
package agentupgradeport

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// RegisterCapabilities is the register's capability list. proxy-v1 is always
// implemented. upgrade-tx-v1 is advertised exactly while this process holds a
// bound transaction endpoint: the broker forwards upgrade-tx (describe,
// prepare, start, ...) only to an agent whose LATEST register advertised it,
// and an unregistered agent keeps receiving the shared legacy refusal.
// It says nothing about image readers: those are the version declarations,
// which stay unadvertised until every exporter exists.
// origin: hot-update S8 plan §6.1 / §7.2; S8 FIX-DESCRIBE (P15 request)
func RegisterCapabilities(endpoint upgradeport.Endpoint) []string {
	if endpoint == nil {
		return []string{proto.CapProxyV1}
	}
	return []string{proto.CapProxyV1, proto.CapUpgradeTXV1}
}

// HeartbeatProbe sends the agent's own heartbeat with the probe nonce added
// as a request on its heartbeat subject; the broker relays it as a claim
// probe through the agent's real forwarded subscriber and returns that reply
// (brokerupgrade.WithControlProbe). An undecodable heartbeat is refused rather
// than replaced by a partial one that would reset the broker's view of this
// agent's proxy readiness. An old broker never answers: ctx bounds the wait.
// origin: hot-update S8 plan §4.2 / M05; S8 FIX-DESCRIBE
func HeartbeatProbe(ctx context.Context, nc *nats.Conn, sid, nid string, heartbeat []byte, nonce string) (*nats.Msg, error) {
	var payload proto.HeartbeatPayload
	if err := json.Unmarshal(heartbeat, &payload); err != nil {
		return nil, err
	}
	payload.ControlProbeNonce = nonce
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return nc.RequestWithContext(ctx, proto.SubjNodeHeartbeat(sid, nid), body)
}

// HeartbeatPayload encodes the shared ticker and pre-freeze control body.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
// heartbeatPayload is the one constructor of the heartbeat body: the ticker
// above and the heartbeat a cut sends ahead of its freeze window (so the
// broker's 5 s staleness clock starts from the cut, not from the previous
// tick — hot-update S3 plan §2.1 step 8) publish the same bytes.
// (Moved verbatim from internal/agentupgrade/control.go by S8 FIX-DESCRIBE,
// beside the probe that extends the same body, so agentupgrade stays under
// its structural threshold.)
func HeartbeatPayload(at time.Time, generation, epoch int64, bound bool) []byte {
	payload, _ := json.Marshal(proto.HeartbeatPayload{
		Ts: at.UTC(), ProxyGeneration: generation, ProxyEpoch: epoch, ProxyBound: bound,
	})
	return payload
}

type activationNoticeKey struct{}

// ReplyOnce gives one forwarded request a single reply and an early one. A
// successful exec never returns, so a Start's own reply is lost to it: the
// broker then answered only after its whole forward timeout (30 s) that the
// result is unknown, and every hot cut kept its operator — and any producer
// parked across the cut — waiting that long for the same 74. The returned
// context carries the early accepted-unknown reply (ActivateNotifying); the
// returned reply is the request's final answer. Whichever runs first is the
// only one sent, and each is flushed before the process image can be replaced.
// origin: hot-update S8 plan §3.1 / §7.1; S8 FIX-DESCRIBE (real-cut finding)
func ReplyOnce(ctx context.Context, nc *nats.Conn, subject string) (context.Context, func([]byte)) {
	var once sync.Once
	reply := func(body []byte) {
		once.Do(func() {
			_ = nc.Publish(subject, body)
			_ = nc.FlushTimeout(upgradebudget.RequestLatency)
		})
	}
	if ctx == nil {
		return nil, reply
	}
	early := func(record upgradetx.Record) {
		response := upgradewire.ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true,
			Detail: "start accepted and activating; wait for or query the original transaction"})
		response.View.Historical = record
		body, err := upgradewire.EncodeResponse(response)
		if err == nil {
			reply(body)
		}
	}
	return context.WithValue(ctx, activationNoticeKey{}, early), reply
}

// ActivatingRefusal is the immediate answer to any request that reaches this
// process between a Start's early reply and its exec: a deferral that accepted
// nothing (retry; upgradecli's wait retries it within its deadline). Without
// it the request waited behind the activating Start, was lost to the exec, and
// the broker — which forwards upgrade-tx inline — held it and every message
// queued behind it for its whole 30 s forward timeout; a live transfer's
// control traffic then missed its admission deadline across the cut. It
// returns nil while the process is not activating, or for a runtime that
// cannot say (only an Activating runtime replaces its process image).
// origin: hot-update S8 plan §3.1 / §7.1; S8 FIX-DESCRIBE (real-cut finding)
func ActivatingRefusal(runtime upgradeport.RuntimePort) []byte {
	port, ok := runtime.(interface{ Activating() bool })
	if !ok || !port.Activating() {
		return nil
	}
	body, err := upgradewire.EncodeResponse(upgradewire.ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeDeferred,
		Detail: "target_activating: the target is replacing its image; retry"}))
	if err != nil {
		return nil
	}
	return body
}

// Activating reports whether this adapter is between a Start's early reply
// and the return of its exec (ActivatingRefusal).
func (p *runtimePort) Activating() bool { return p != nil && p.activating.Load() }

// ActivateNotifying is the engine's Activate call carrying the request's
// early reply. The engine calls it only after the activation intent is
// durable, so the reply can truthfully say the Start was accepted and must be
// queried, never retried. This adapter sends it at the last moment, after the
// final re-export and reader checks passed and right before exec: a refusal
// there still returns its exact reason, and a query issued after the reply
// finds the old subscription already gone (a fast "no responder", retried)
// instead of a doomed process that swallows it for the broker's whole
// forward timeout. Another runtime is notified just before its Activate.
func ActivateNotifying(ctx context.Context, runtime upgradeport.RuntimePort, cut upgradeport.Cut) (upgradeport.Activation, error) {
	notice, _ := ctx.Value(activationNoticeKey{}).(func(upgradetx.Record))
	var notify func()
	if notice != nil {
		record, _ := ctx.Value(activationRecordKey{}).(upgradetx.Record)
		if !upgradetx.SameCut(current(record).Handle, cut.Attempt) {
			record = upgradetx.Record{}
		}
		notify = func() { notice(record) }
	}
	if port, ok := runtime.(*runtimePort); ok {
		return port.activate(cut, notify)
	}
	if notify != nil {
		notify()
	}
	return runtime.Activate(cut)
}

// origin: S10 internal review MAKETEST-GATES F4
type ControlProof struct {
	InstanceID string `json:"instance_id"`
	Nonce      string `json:"query_nonce,omitempty"`
	Generation uint64 `json:"session_generation,omitempty"`
}

// replyClaimProbe answers a lease claim probe with this process's instance id.
//
// The id is what lets the broker distinguish "someone else holds this name"
// from "the thing answering is the very process that is re-registering" — the
// case that would otherwise rename a device on every network blip, because
// nats.go replays subscriptions before it invokes the reconnect handler.
//
// A package-level function, not a method: the structural-budget ratchet pins
// internal/agent.Agent's method count exactly.
// origin: S10 internal review MAKETEST-GATES F4
func ReplyClaimProbe(nc *nats.Conn, msg *nats.Msg, instanceID, sid string, nid func() string, sessionGen *atomic.Uint64, heartbeat func() []byte) {
	if msg.Reply == "" {
		return
	}
	var query ControlProof
	if len(msg.Data) != 0 && json.Unmarshal(msg.Data, &query) != nil {
		return
	}
	generation := uint64(0)
	if query.Nonce != "" {
		generation = sessionGen.Load()
	}
	payload, err := json.Marshal(ControlProof{InstanceID: instanceID, Nonce: query.Nonce, Generation: generation})
	if err != nil {
		return
	}
	// A broker image can miss a periodic heartbeat while its subscriptions are
	// fenced. A real claim probe wakes liveness from this live agent instead of
	// leaving new control admission to wait another full heartbeat interval.
	// origin: hot-update S6 plan §8.1 / T50 / EXITS-2-3
	_ = nc.Publish(proto.SubjNodeHeartbeat(sid, nid()), heartbeat())
	_ = nc.Publish(msg.Reply, payload)
}
