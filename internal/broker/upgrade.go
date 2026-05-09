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
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

var sha256HexRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (b *Broker) handleUpgradeReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "upgrade" {
		b.replyUpgradeErr(msg, "subject_malformed", "")
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyUpgradeErr(msg, "actor_invalid", err.Error())
		return
	}

	// C.1 §6 ingress gate (same as exec/run/expose/register).
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyUpgradeErr(msg, "store_error", err.Error())
		return
	}
	if !active {
		b.replyUpgradeErr(msg, "session_not_found_or_deleting", "")
		return
	}

	// J.4 § owner-only.
	owner, err := session.IsOwner(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyUpgradeErr(msg, "store_error", err.Error())
		return
	}
	if !owner {
		b.replyUpgradeErr(msg, "not_owner", "")
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "not_owner", msg.Reply, nil)
		return
	}

	// Pre-forward node ONLINE check (mirrors handleExecReq).
	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		b.replyUpgradeErr(msg, "node_not_found", "")
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "node_not_found", msg.Reply, nil)
		return
	case err != nil:
		b.replyUpgradeErr(msg, "store_error", err.Error())
		return
	case status != node.StateOnline:
		b.replyUpgradeErr(msg, "node_offline", string(status))
		b.pubAuditCall(sid, fp, actor, "upgrade", nid, false, "node_offline:"+string(status), msg.Reply, nil)
		return
	}

	var req proto.UpgradeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyUpgradeErr(msg, "json_parse", err.Error())
		return
	}

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

	out := proto.UpgradeResp{OK: true}
	body, _ = json.Marshal(&out)
	if msg.Reply != "" {
		_ = msg.Respond(body)
	}
	b.cfg.Logger.Info("broker: upgrade dispatched",
		"sid", sid, "nid", nid, "url", req.URL, "actor_fp", fp,
		"new_version", agentResp.NewVersion)
	b.pubAuditCall(sid, fp, actor, "upgrade", nid, true, "", msg.Reply,
		map[string]any{"url": req.URL, "sha256": strings.ToLower(req.SHA256), "new_version": agentResp.NewVersion})
}

func (b *Broker) replyUpgradeErr(msg *nats.Msg, code, detail string) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(proto.UpgradeResp{Code: code, Error: detail})
	_ = msg.Respond(body)
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

