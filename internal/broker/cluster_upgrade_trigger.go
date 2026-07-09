package broker

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// cluster_upgrade_trigger.go (G5 #13 W2b) — the account-signed remote reload/transfer-leader responder.
// Security anchor = the account signature (the operator's root authority, same trust as the signed
// roster/seeds): a broker acts ONLY if the request targets its own node_id AND the signature verifies
// against its pinned account_pub. Dispatch routes through the local admin backend, so it reuses EVERY
// gate the socket path has (reload leader-refuse/sha, transfer leader-gate) — no duplicated logic.

// upgradeTriggerSkew bounds replay: a signed request whose issued_at is farther than this from the
// broker's clock is refused (the same request can't be replayed hours later).
const upgradeTriggerSkew = 5 * time.Minute

// upgradeTriggerCodeUnauthorized is the ClusterUpgradeResp code for a request that does not carry a valid
// account signature (the socket-only adminsock vocabulary has no auth code — the socket is root-gated).
const upgradeTriggerCodeUnauthorized = "unauthorized"

// upgradeTriggerCodeNotReady (External-review M1) is a RETRIABLE code returned when the admin backend is
// not wired yet (a broker that just re-exec'd has its health responder up before clusterAdminHandle is
// set). The orchestrator retries it, so a startup window is a brief wait rather than a roll HALT.
const upgradeTriggerCodeNotReady = "cluster_not_ready"

// setClusterAdminHandle stores the admin-dispatch func under the lock (the trigger responder reads it from
// another goroutine). External-review M1.
func (b *Broker) setClusterAdminHandle(fn func(adminsock.Request) adminsock.Response) {
	b.clusterAdminMu.Lock()
	b.clusterAdminHandle = fn
	b.clusterAdminMu.Unlock()
}

func (b *Broker) clusterAdminHandleFn() func(adminsock.Request) adminsock.Response {
	b.clusterAdminMu.RLock()
	defer b.clusterAdminMu.RUnlock()
	return b.clusterAdminHandle
}

// SubscribeClusterUpgradeTrigger wires the broadcast responder. Every broker subscribes, but each request
// names exactly one TargetNode, so only that broker replies — a wrong target stays SILENT so the intended
// broker's reply is the one the orchestrator sees.
func (b *Broker) SubscribeClusterUpgradeTrigger(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterUpgradeWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		resp := b.handleUpgradeTrigger(msg.Data, b.cfg.Now())
		if resp == nil {
			return // not for this broker → stay silent
		}
		body, _ := json.Marshal(resp)
		_ = msg.Respond(body)
	})
}

// handleUpgradeTrigger verifies + dispatches one signed request. Returns nil (stay silent) when the
// request targets a DIFFERENT broker; a *ClusterUpgradeResp otherwise (including every refusal).
func (b *Broker) handleUpgradeTrigger(data []byte, now time.Time) *proto.ClusterUpgradeResp {
	var req proto.ClusterUpgradeReq
	if err := json.Unmarshal(data, &req); err != nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "malformed request"}
	}
	// Act on THIS broker only; a wrong/empty target is silent so the intended broker's reply wins.
	if b.selfID == "" || req.TargetNode != b.selfID {
		return nil
	}
	// The REAL gate: verify the account signature against this broker's pinned account_pub. Fail-closed.
	if b.cfg.AuthCallout == nil || len(b.cfg.AuthCallout.AccountSeed) == 0 {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "no account key to verify the upgrade trigger"}
	}
	accountPub, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.AccountSeed)
	if err != nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeStoreError, Error: "account key error"}
	}
	sig, decErr := hex.DecodeString(req.Sig)
	if decErr != nil || auth.VerifySignature(accountPub, proto.CanonicalUpgradeReqBytes(&req), sig) != nil {
		return &proto.ClusterUpgradeResp{Code: upgradeTriggerCodeUnauthorized, Error: "signature does not verify against the cluster account key"}
	}
	// Bound replay: issued_at must be within the skew window of the broker's clock.
	t, perr := time.Parse(time.RFC3339, req.IssuedAt)
	if perr != nil || absDuration(now.Sub(t)) > upgradeTriggerSkew {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "stale or unparseable issued_at (replay guard)"}
	}
	handle := b.clusterAdminHandleFn()
	if handle == nil && (req.Op == "reload" || req.Op == "transfer-leader") {
		// External-review M1: not a terminal failure — the admin backend wires a moment after the health
		// responder comes up (esp. just after a re-exec). Retriable so the orchestrator waits, not HALTs.
		return &proto.ClusterUpgradeResp{Code: upgradeTriggerCodeNotReady, Error: "cluster admin backend not wired yet — retry"}
	}
	// Route through the local admin backend so EVERY existing gate applies (no duplicated logic).
	switch req.Op {
	case "reload":
		ar := handle(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: req.ToVersion, ExpectSHA256: req.ExpectSHA256})
		return &proto.ClusterUpgradeResp{OK: ar.OK, Code: ar.Code, Error: ar.Error, Reloading: ar.Reloading, AlreadyAtVersion: ar.AlreadyAtVersion}
	case "transfer-leader":
		ar := handle(adminsock.Request{Op: adminsock.OpClusterTransfer, NodeID: req.TransferTo})
		out := &proto.ClusterUpgradeResp{OK: ar.OK, Code: ar.Code, Error: ar.Error}
		if ar.OK {
			out.NewLeader = req.TransferTo
		}
		return out
	case "reexec-agent":
		// Stage-C B1: re-exec THIS broker's co-located agent into the staged binary (the broker daemon
		// already reloaded; a whole-host upgrade needs the agent leg too). Forward ReExecOnly to the agent
		// on its session-scoped upgrade subject; the agent verifies the on-disk sha + re-execs.
		// A12: when colocated_agent_nid is UNSET, fall back to b.selfID — the node_id==nid convention the
		// detect/plan sides already assume (serveconf documents the field as optional). b.selfID is this
		// broker's OWN validated node_id (== req.TargetNode, checked above), NOT an advertised/spoofable
		// field, so the g5 "never fan out by an advertised nid" invariant holds. Without this, a host that
		// left the field unset (agent registered nid==node_id) HALTed the whole roll at bad_request even
		// though detect/plan resolved its agent fine — the documented one-command roll broke mid-flight.
		agentNID := b.cfg.ColocatedAgentNID
		if agentNID == "" {
			agentNID = b.selfID
		}
		nc := b.nc.Load()
		if nc == nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "broker not connected"}
		}
		fwd, _ := json.Marshal(&proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: req.ExpectSHA256})
		reply, err := nc.Request(proto.SubjCmdForwarded(req.SID, agentNID, "upgrade"), fwd, b.cfg.UpgradeForwardTimeout())
		if err != nil {
			return &proto.ClusterUpgradeResp{Code: "agent_no_responders", Error: err.Error()}
		}
		var ar proto.UpgradeForwardedResp
		if json.Unmarshal(reply.Data, &ar) != nil {
			return &proto.ClusterUpgradeResp{Code: "agent_malformed_resp", Error: "agent reply undecodable"}
		}
		if !ar.OK {
			return &proto.ClusterUpgradeResp{Code: "agent_rejected:" + ar.Code, Error: ar.Error}
		}
		return &proto.ClusterUpgradeResp{OK: true}
	case "acquire-lock":
		// External-review B2: hold a cluster-scoped roll lock so a concurrent join/retire cannot cross the
		// rolling restart. Leader-only Propose (the orchestrator targets the leader); a non-leader errors.
		if b.cl == nil || b.cl.node == nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
		}
		// G4 §B: symmetric fence — refuse to start a roll while a `cluster add` grow holds its marker (grow
		// blocks upgrade, upgrade blocks grow). The grow's join op may be briefly terminal between phases
		// while the marker stays held (cutover/seed/rebalance), so check the marker, not just NonTerminalOperations.
		if j := growActiveJoiner(b.cl.node.RODB()); j != "" {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest,
				Error: "a `cluster add` grow of " + j + " is in progress — let it finish before starting a rolling upgrade"}
		}
		// External-review round2 B2: refuse to START a roll while a membership op is already in flight — the
		// roll must own a topology-stable window. (Belt-and-suspenders with the driveInFlightOperations freeze:
		// this stops the roll at acquire; the freeze stops any op that slips through the check→commit gap.)
		if ops, oerr := cluster.NonTerminalOperations(b.cl.node.RODB()); oerr == nil && len(ops) > 0 {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest,
				Error: "a cluster membership operation (join/retire) is in flight — let it finish before starting a rolling upgrade"}
		}
		if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanSetUpgradeActive(b.cfg.Now()) }); err != nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeNotLeader, Error: "acquire upgrade lock (run against the leader): " + err.Error()}
		}
		return &proto.ClusterUpgradeResp{OK: true}
	case "release-lock":
		// External-review round2 B1: the clear MUST surface a Propose failure — a false OK leaves the
		// replicated cluster_upgrade_active marker set while the ctl believes membership resumed.
		if b.cl == nil || b.cl.node == nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
		}
		if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClearUpgradeActive() }); err != nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeNotLeader, Error: "release upgrade lock (run against the leader): " + err.Error()}
		}
		return &proto.ClusterUpgradeResp{OK: true}
	default:
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "unknown op: " + req.Op}
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
