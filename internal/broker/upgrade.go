// Broker side of architecture J.4 (b) — `tether node upgrade`.
//
// The verb deliberately costs more security than e.g. `exec`: it
// rewrites the agent's own binary, so an attacker who lands here
// owns the box. The gates mirror the architecture J.4 §
// "tetherd 侧硬拒条件" + § "安全约束":
//
//   - actor must be the session OWNER (v1: session creator).
//   - URL must start with one of UpgradeURLAllowlist (mandatory,
//     no implicit default — operator must opt in by configuring it).
//   - sha256 must be 64 lowercase hex chars (sanity; agent does the
//     real verification post-download).
//   - request proto_version must equal the broker's ProtoVersion
//     (cross-proto upgrades go through J.3 reinstall, not this
//     verb).
//
// Both agent-upgrade entries share one owner admission here; the old-wire
// gates, the S8 transaction hop and their common legacy policy live in
// internal/brokerupgrade (agent_entry.go).
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P15
package broker

import (
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func (b *Broker) handleUpgradeReq(_ *nats.Conn, msg *nats.Msg) {
	// upgrade has NO cluster-role short circuit, and `.upgrade.req` is absent from
	// isBroadcastClusterSubject (clusterwrite.go:59-80 lists exec/run/kill/expose/expose-rm and
	// the transfer family, not upgrade), so it is QUEUE-GROUPED: exactly one broker handles each
	// request.
	//
	// Do NOT "tidy up" the missing short circuit. Adding a leader-only gate to a queue-grouped
	// subject is the precise shape of Mega-audit MAJ-2, whose write-up sits at broker.go:1001-1006
	// — that one was about the `.proxy.sub.*` wildcard, not about upgrade, but the mechanism
	// transfers exactly: the queue group hands the request to an arbitrary member, a leader-only
	// handler on a follower returns silently, and ctl times out ~(N-1)/N of the time. upgrade is
	// the fleet's only remote-update verb, so that failure mode would land on the one command an
	// operator reaches for when a node is already misbehaving.
	ing, den, ok := b.admit(msg.Subject, upgradeSpec)
	if !ok {
		b.replyUpgradeErr(msg, den.code, den.detail)
		// upgrade is the only converted verb that audits its OWNERSHIP refusal as well as its
		// node refusals.
		switch den.code {
		case "not_owner", "node_not_found", "node_offline":
			b.pubAuditCall(ing.sid, ing.fp, ing.actor, "upgrade", ing.nid, false,
				auditRefusal(den), msg.Reply, nil)
		}
		return
	}
	// The original forwarding contract is historical. S8 dispatch belongs to
	// upgradelegacy.Proxy after explicit authorization and durable intent;
	// keeping the bare forward here would leave an unjournaled execution path.
	// The former contract forwarded to the agent and synchronously waited for
	// its ACK so the operator's `tether node upgrade` reported the verify+rename
	// outcome inline; upgrade-safety then relayed the smoke-normalized release
	// tag that ctl --wait polls node.list for. Neither can happen any more.
	refusal := brokerupgrade.CheckLegacyAgentRequest(msg.Data, b.cfg.UpgradeURLAllowlist, b.read().SQL(), ing.sid, ing.nid)
	b.replyUpgradeErr(msg, refusal.Code, refusal.Detail)
	if refusal.Audit != "" {
		b.pubAuditCall(ing.sid, ing.fp, ing.actor, "upgrade", ing.nid, false, refusal.Audit, msg.Reply, nil)
	}
}

// upgradeTxSpec admits the S8 transaction hop exactly like the old verb: owner
// only and an ONLINE target. It has no cluster short circuit for the same
// reason (queue-grouped), so it reads the two halves of the gate directly.
var upgradeTxSpec = verbSpec{verb: brokerupgrade.SignedTransactionOp, role: admitOwner}

// handleUpgradeTxReq forwards one authenticated S8 agent transaction. Policy,
// strict decoding and the private-inbox rule live in the leaf; nothing reaches
// the agent unless its latest register advertised transaction capability.
// The relay to the agent runs off this subscription goroutine (one wildcard
// subscription serves every agent), so the reply closure stays bound to msg.
// origin: hot-update S8 plan §2.2 / §7.1 / §7.2 / §11.2 P15; S8 BROKER-EXPORTER R-9
func handleUpgradeTxReq(b *Broker, nc *nats.Conn, msg *nats.Msg) {
	ing, den, ok := b.admitSubject(msg.Subject, upgradeTxSpec)
	if ok {
		den, ok = b.admitACL(&ing, upgradeTxSpec)
	}
	b.agentForward.Forward(ok, den.code, den.detail, brokerupgrade.WithAgentRollAuthority(brokerupgrade.AgentRoute{DB: b.read().SQL(),
		SID: ing.sid, NID: ing.nid, Actor: ing.actor, ActorFP: ing.fp, Reply: msg.Reply, Body: msg.Data,
		Timeout: b.cfg.UpgradeForwardTimeout(), Request: brokerupgrade.NATSRequest(nc)}, func() *AuthCalloutConfig { return b.cfg.AuthCallout }), func(body []byte) { b.respondBytes(msg, body) }, func(code string) {
		b.pubAuditCall(ing.sid, ing.fp, ing.actor, upgradeTxSpec.verb, ing.nid, code == "", code, msg.Reply, nil)
	})
}

// Compatibility adapter keeps existing callers bound to the extracted leaf
// (legacyUpgradeRefusal lives beside its caller, cluster_upgrade_trigger.go).
// origin: hot-update S8 plan §12.1 / §11.2 P15
func cloneFamilyUpgradeConflict(b *Broker, sid, target string) bool {
	return brokerupgrade.CloneFamilyConflict(b.read().SQL(), sid, target)
}

func (b *Broker) replyUpgradeErr(msg *nats.Msg, code, detail string) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(proto.UpgradeResp{Code: code, Error: detail})
	b.respondBytes(msg, body)
}
