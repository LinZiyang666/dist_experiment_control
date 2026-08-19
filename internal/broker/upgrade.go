// Broker side of architecture J.4 (b) — `tether node upgrade`.
//
// The verb deliberately costs more security than e.g. `exec`: it
// rewrites the agent's own binary, so an attacker who lands here
// owns the box. The gates below mirror the architecture J.4 §
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
package broker

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

var sha256HexRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (b *Broker) handleUpgradeReq(nc *nats.Conn, msg *nats.Msg) {
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
	sid, actor, nid, fp := ing.sid, ing.actor, ing.nid, ing.fp

	var req proto.UpgradeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyUpgradeErr(msg, "json_parse", err.Error())
		return
	}

	// origin: upgrade-safety N-1 window — exact equality (architecture §21.1
	// site #2): a cross-epoch upgrade is the one permitted compatibility
	// break and goes through reinstall, never this verb. See §21.4.
	if req.ProtoVersion != proto.ProtoVersion {
		b.replyUpgradeErr(msg, "proto_bump_requires_reinstall",
			fmt.Sprintf("broker proto=%d, request proto=%d", proto.ProtoVersion, req.ProtoVersion))
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "proto_bump_requires_reinstall", msg.Reply, nil)
		return
	}
	if !sha256HexRE.MatchString(strings.ToLower(req.SHA256)) {
		b.replyUpgradeErr(msg, "sha256_invalid", "expected 64 lowercase hex chars")
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "sha256_invalid", msg.Reply, nil)
		return
	}
	if !urlAllowed(req.URL, b.cfg.UpgradeURLAllowlist) {
		b.replyUpgradeErr(msg, "url_not_allowed", req.URL)
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "url_not_allowed", msg.Reply, nil)
		return
	}
	if cloneFamilyUpgradeConflict(b, sid, nid) {
		b.replyUpgradeErr(msg, "clone_family_upgrade_unsupported",
			"remote upgrade is disabled for a credential family that has issued lease names; rebuild the shared image instead")
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "clone_family_upgrade_unsupported", msg.Reply, nil)
		return
	}

	// Forward to agent. We synchronously wait for the agent's ACK so
	// the operator's `tether node upgrade` reports the verify+rename
	// outcome inline (download success / sha mismatch / fs write
	// failure are all surfaced in the same reply).
	fwd := proto.UpgradeForwardedReq{
		URL:     req.URL,
		SHA256:  strings.ToLower(req.SHA256),
		ActorFP: fp,
	}
	body, _ := json.Marshal(&fwd)
	subj := proto.SubjCmdForwarded(sid, nid, "upgrade")
	respMsg, err := nc.Request(subj, body, b.cfg.UpgradeForwardTimeout())
	if err != nil {
		b.replyUpgradeErr(msg, "agent_no_responders", err.Error())
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "agent_no_responders", msg.Reply, nil)
		return
	}
	var agentResp proto.UpgradeForwardedResp
	if err := json.Unmarshal(respMsg.Data, &agentResp); err != nil {
		b.replyUpgradeErr(msg, "agent_malformed_resp", err.Error())
		return
	}
	if !agentResp.OK {
		b.replyUpgradeErr(msg, "agent_rejected:"+agentResp.Code, agentResp.Error)
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "agent_rejected:"+agentResp.Code, msg.Reply, nil)
		return
	}

	// upgrade-safety: relay the smoke-normalized release tag — ctl --wait
	// polls node.list until ReleaseVersion equals exactly this string.
	out := proto.UpgradeResp{OK: true, NewVersion: agentResp.NewVersion}
	body, _ = json.Marshal(&out)
	if msg.Reply != "" {
		b.respondBytes(msg, body)
	}
	b.cfg.Logger.Info("broker: upgrade dispatched",
		"sid", sid, "nid", nid, "url", req.URL, "actor_fp", fp,
		"new_version", agentResp.NewVersion)
	b.pubAuditCall(sid, fp, actor, "upgrade", nid, true, "", msg.Reply,
		map[string]any{"url": req.URL, "sha256": strings.ToLower(req.SHA256), "new_version": agentResp.NewVersion})
}

// cloneFamilyUpgradeConflict rejects both a leased row and its configured
// basename. In the supported shared-home deployment they may execute one
// shared binary and marker; replacing it for either process makes a nominally
// single-node operation mutate the whole clone family, and a sibling restart
// can consume the target's pending boot proof. A historical lease row keeps
// the family conservative: the broker cannot prove a stopped sibling will not
// restart during the upgrade window.
func cloneFamilyUpgradeConflict(b *Broker, sid, target string) bool {
	provisioned, bindingsKnown := node.ProvisionedNIDs(b.read().SQL(), sid)
	all, err := node.List(b.read().SQL())
	if err != nil {
		// An upgrade replaces an executable. If the authority cannot be read,
		// availability is the safe sacrifice: do not guess that this target has
		// an isolated upgrade domain.
		return true
	}
	for _, n := range all {
		if n.SID != sid {
			continue
		}
		base, _, shaped := proto.SplitLeaseName(n.NID)
		if !shaped || bindingsKnown && provisioned[n.NID] {
			continue
		}
		// With no auth-callout bindings, shape is not proof that n is leased.
		// It is nevertheless proof that an isolated binary cannot be proven,
		// so upgrade takes the conservative (possibly availability-reducing)
		// answer. The ordinary node-list classifier remains non-destructive and
		// therefore keeps its opposite fallback.
		if target == n.NID || target == base {
			return true
		}
	}
	return false
}

func (b *Broker) replyUpgradeErr(msg *nats.Msg, code, detail string) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(proto.UpgradeResp{Code: code, Error: detail})
	b.respondBytes(msg, body)
}

// urlAllowed returns true iff url has any prefix in allow. Empty
// allow → false (we don't trust an unconfigured allowlist; J.4 §
// 安全约束 makes the operator opt in explicitly).
func urlAllowed(url string, allow []string) bool {
	if len(allow) == 0 {
		return false
	}
	for _, p := range allow {
		if p == "" {
			continue
		}
		if strings.HasPrefix(url, p) {
			return true
		}
	}
	return false
}
