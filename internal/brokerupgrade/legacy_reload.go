package brokerupgrade

import (
	"database/sql"
	"errors"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// LegacyReloadResponse is the common local/signed/old-backend reload policy.
// Existing rows are positive busy witnesses, never a complete quiescence proof.
// The old DTO contains no target-issued grant, reader or authority evidence;
// neither an account signature nor an empty database supplies those facts.
// A capable hot reload uses the transaction endpoint rather than this envelope.
// origin: S8 internal review rv3-leaves F2
func LegacyReloadResponse(req adminsock.Request, db *sql.DB) adminsock.Response {
	p := upgradecompat.LegacyPolicy{Component: upgradetx.ComponentBroker}
	if db != nil {
		for _, query := range []string{
			`SELECT EXISTS(SELECT 1 FROM processes WHERE status='RUNNING')`,
			`SELECT EXISTS(SELECT 1 FROM port_allocations WHERE state='ALLOCATED')`,
		} {
			var busy bool
			if err := db.QueryRow(query).Scan(&busy); err == nil && busy {
				p.Busy = true
			}
		}
	}
	err := upgradecompat.CheckLegacy(p)
	var txErr *upgradetx.Error
	code, detail := "try_again", "legacy_quiescence_unproved"
	if errors.As(err, &txErr) {
		detail = txErr.Detail
		if txErr.Code != upgradetx.CodeDeferred {
			code = adminsock.CodeBadRequest
		}
	}
	return adminsock.Response{Op: req.Op, Code: code, Error: detail}
}

// RouteLegacyReload leaves ordinary cluster administration on its existing
// backend while refusing every unauthorised old reload before arm or Freeze.
func RouteLegacyReload(req adminsock.Request, db *sql.DB, cluster adminsock.ClusterAdminBackend) adminsock.Response {
	if req.Op == adminsock.OpBrokerUpgradeReload {
		return LegacyReloadResponse(req, db)
	}
	if cluster != nil {
		return cluster.HandleCluster(req)
	}
	return adminsock.Response{Op: req.Op, Code: adminsock.CodeClusterNotEnabled, Error: "cluster mode is not enabled"}
}

// LegacyReexecResponse keeps the signed old verb behind the common policy.
func LegacyReexecResponse(db *sql.DB, req proto.ClusterUpgradeReq, selfID, configuredNID string, connected bool) *proto.ClusterUpgradeResp {
	// Stage-C B1: re-exec THIS broker's co-located agent into the staged binary (the broker daemon
	// already reloaded; a whole-host upgrade needs the agent leg too). Forward ReExecOnly to the agent
	// on its session-scoped upgrade subject; the agent verifies the on-disk sha + re-execs.
	// A12: when colocated_agent_nid is UNSET, fall back to b.selfID — the node_id==nid convention the
	// detect/plan sides already assume (serveconf documents the field as optional). b.selfID is this
	// broker's OWN validated node_id (== req.TargetNode, checked above), NOT an advertised/spoofable
	// field, so the g5 "never fan out by an advertised nid" invariant holds. Without this, a host that
	// left the field unset (agent registered nid==node_id) HALTed the whole roll at bad_request even
	// though detect/plan resolved its agent fine — the documented one-command roll broke mid-flight.
	agentNID := configuredNID
	if agentNID == "" {
		agentNID = selfID
	}
	if !connected {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "broker not connected"}
	}
	// origin: hot-update S8 plan §7.2 / S8 internal review rv-n1 F1
	// A signed cluster child cannot waive the old-wire admission fence.
	code, detail := LegacyAgentRefusal(db, req.SID, agentNID)
	if code == "try_again" {
		return &proto.ClusterUpgradeResp{Code: "try_again", Error: detail}
	}
	return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: detail}
	// The former forwarding diagnostic remains a contract for the explicit
	// migration adapter; this old request cannot dispatch an unjournaled exec.
	// P3: name the agentless case explicitly. A dedicated broker host (no
	// `serve --colocated-agent-nid`, no agent registered under node_id==nid) has no agent leg,
	// and a current ctl marks its step BROKER-ONLY and never sends this op. Reaching here means
	// either a genuinely unreachable agent, or a ctl too old to make that distinction — for
	// which "upgrade the ctl, or re-run the roll from a current one" is the actual fix, not
	// "go find the agent".

}
