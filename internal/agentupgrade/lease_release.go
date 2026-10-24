package agentupgrade

import (
	"time"

	"github.com/LinZiyang666/tether/internal/proto"

	"github.com/nats-io/nats.go"
)

// releaseLeaseName tells the broker this process is stopping cleanly and is
// giving its node name back.
//
// WHY THIS EXISTS. Adjudication has to separate two situations that are
// indistinguishable from the broker's side: two clones launched together (the
// second must be suffixed) and one agent restarting quickly (it must keep its
// name). Both present a warm heartbeat, a recently granted holder, and — for
// the few hundred milliseconds before the server reaps the dead process's
// subscription — live interest that nobody answers. The departing process is
// the only party that knows which situation this is, so it says so.
//
// STRICTLY BEST-EFFORT. A crash, a kill -9, a severed link or a wedged teardown
// sends nothing, and adjudication still converges through the grant window, the
// interest probe and the heartbeat clock. Nothing may be built that DEPENDS on
// this arriving. That is also why it is a bare Publish with a short Flush rather
// than a request/reply: a shutdown path must not be able to hang on the network,
// and the caller is already inside a bounded teardown budget.
func ReleaseLease(nc *nats.Conn, sid, nid, configuredNID, instanceID string, marshal func(proto.NodeRegisterReq) ([]byte, error), flush time.Duration) {
	if nc == nil || instanceID == "" || nc.IsClosed() {
		return
	}
	// ProtoVersion and NID are NOT optional decoration: handleRegister rejects a
	// body whose NID does not match the subject, and rejects a proto epoch that
	// is not exactly this one, both BEFORE any lease logic runs. A farewell that
	// omits them is dropped as nid_mismatch and the name is never released.
	//
	// RosterRefreshOnly is what makes this INERT on a broker that predates the
	// feature, and it is load-bearing.
	//
	// "Additive and omitempty" only means an old broker IGNORES the new key —
	// not that it ignores the message. Without this flag a pre-feature broker
	// decodes the farewell as an ordinary register carrying an EMPTY snapshot,
	// and runs it: node.Register's ON CONFLICT wipes release_version/boot_id,
	// clears proxy_capable, and stamps status=ONLINE with a FRESH heartbeat for
	// a process that is exiting — then reconcileOnRegister sees no local
	// processes and closes every RUNNING row this node has, after which the
	// surviving children come back as pids with no rows, i.e. orphans, and get
	// SIGKILLed. A courtesy message would have destroyed the operator's work.
	//
	// That is not a hypothetical rollback story: this feature exists because the
	// agent is BAKED INTO AN IMAGE, so "brokers first, then agents" is the one
	// ordering these users cannot guarantee.
	//
	// A pre-feature broker short-circuits RosterRefreshOnly as a pure read
	// before touching anything. A current broker checks ReleasingName ahead of
	// that short-circuit, so the flag costs the farewell nothing here.
	body, err := marshal(proto.NodeRegisterReq{
		ProtoVersion:      proto.ProtoVersion,
		NID:               nid,
		InstanceID:        instanceID,
		ReleasingName:     true,
		RosterRefreshOnly: true,
		// origin: prerelease audit broker-core/BC-F1. This field was never set here,
		// which made the broker's release-the-row branch dead code in production for
		// the whole v0.5.1 line. It is the SECONDARY half of the fix — the broker now
		// decides from agent_provisioning and only consults this when that table
		// cannot be read — because a field only new agents send cannot repair a fleet
		// of old ones. Additive and omitempty, so an N-1 broker ignores it.
		LeasedNID: nid != configuredNID,
	})
	if err != nil {
		return
	}
	// nidOf, not cfg.NID: an agent that adopted a lease must release the name it
	// actually holds, not the one it was configured with.
	if err := nc.Publish(proto.SubjNodeRegister(sid, nid), body); err != nil {
		return
	}
	// Bounded: give the farewell a moment to leave the socket, then stop caring.
	_ = nc.FlushTimeout(flush)
}
