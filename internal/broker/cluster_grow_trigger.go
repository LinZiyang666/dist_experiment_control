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

// cluster_grow_trigger.go (G4 §B) — the account-signed remote responder the `cluster add` grow orchestrator
// drives over NATS. It mirrors cluster_upgrade_trigger.go: a broker acts ONLY if the request targets its own
// node_id AND the signature verifies against its pinned account_pub (the operator's root authority). Dispatch
// routes the lock + membership steps through the SAME local admin backend as the socket path (so every gate
// applies), and the standalone→clustered cutover through the broker-local cluster_grow_cutover component.

const growTriggerSkew = 5 * time.Minute // replay bound (mirrors upgradeTriggerSkew)

// SubscribeClusterGrowTrigger wires the broadcast grow responder. Every broker subscribes; each request
// names exactly one TargetNode, so a wrong target stays SILENT and only the intended broker replies.
func (b *Broker) SubscribeClusterGrowTrigger(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterGrowWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		resp := b.handleGrowTrigger(msg.Data, b.cfg.Now())
		if resp == nil {
			return // not for this broker → stay silent
		}
		body, _ := json.Marshal(resp)
		_ = msg.Respond(body)
	})
}

// handleGrowTrigger verifies + dispatches one signed grow request. Returns nil (stay silent) when the request
// targets a DIFFERENT broker; a *ClusterGrowResp otherwise (including every refusal).
func (b *Broker) handleGrowTrigger(data []byte, now time.Time) *proto.ClusterGrowResp {
	var req proto.ClusterGrowReq
	if err := json.Unmarshal(data, &req); err != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "malformed request"}
	}
	if b.selfID == "" || req.TargetNode != b.selfID {
		return nil // wrong/empty target → silent, so the intended broker's reply wins
	}
	// The REAL gate: verify the account signature against this broker's pinned account_pub. Fail-closed.
	if b.cfg.AuthCallout == nil || len(b.cfg.AuthCallout.AccountSeed) == 0 {
		return &proto.ClusterGrowResp{Code: adminsock.CodeClusterNotEnabled, Error: "no account key to verify the grow trigger"}
	}
	accountPub, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.AccountSeed)
	if err != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeStoreError, Error: "account key error"}
	}
	sig, decErr := hex.DecodeString(req.Sig)
	if decErr != nil || auth.VerifySignature(accountPub, proto.CanonicalGrowReqBytes(&req), sig) != nil {
		return &proto.ClusterGrowResp{Code: growTriggerCodeUnauthorized, Error: "signature does not verify against the cluster account key"}
	}
	t, perr := time.Parse(time.RFC3339, req.IssuedAt)
	if perr != nil || absDuration(now.Sub(t)) > growTriggerSkew {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "stale or unparseable issued_at (replay guard)"}
	}

	switch req.Op {
	case "acquire-lock":
		if b.cl == nil || b.cl.node == nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
		}
		// Symmetric fence: an upgrade roll blocks a grow, and a DIFFERENT grow blocks this one.
		if upgradeActive(b.cl.node.RODB()) {
			return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "a `cluster upgrade` roll is in progress — retry after it completes"}
		}
		if j := growActiveJoiner(b.cl.node.RODB()); j != "" && j != req.JoinerNode {
			return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "a `cluster add` grow of " + j + " is already in progress — grows are serialized"}
		}
		// Refuse to START a grow while a membership op for a DIFFERENT node is in flight (a concurrent
		// retire/join). The grow's OWN join op does not exist yet at acquire time; a same-joiner op (a resume)
		// is allowed through.
		if ops, oerr := cluster.NonTerminalOperations(b.cl.node.RODB()); oerr == nil {
			for _, op := range ops {
				if op.TargetNode != req.JoinerNode {
					return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest,
						Error: "a membership operation for " + op.TargetNode + " is in flight — let it finish before growing"}
				}
			}
		}
		if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanSetGrowActive(req.JoinerNode, b.cfg.Now()) }); err != nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeNotLeader, Error: "acquire grow lock (run against the leader): " + err.Error()}
		}
		// H1: the acquire is a CONDITIONAL replicated mutex (PlanSetGrowActive no-ops if an upgrade marker or a
		// DIFFERENT joiner's grow marker is held). Read the marker back after apply — if it is not owned by THIS
		// joiner, an upgrade roll or a racing grow won between our preflight and our commit, so we must not report
		// a lock we do not hold.
		if growActiveJoiner(b.cl.node.RODB()) != req.JoinerNode {
			return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest,
				Error: "a concurrent `cluster upgrade` roll or another `cluster add` grow acquired the membership lock first — retry after it completes"}
		}
		return &proto.ClusterGrowResp{OK: true}

	case "renew-lock":
		// R7b: the grow lock is now LEASED. `cluster add` keeps a renewer alive for the whole grow; when the
		// orchestrator dies (a HALT at P2/P3 — the acquire-lock window R7a could not judge, because the join
		// operation row does not exist yet) the renewals stop and the reconcile pass expires the lock.
		//
		// The renewal is JOINER-BOUND and can only REFRESH, never CREATE: PlanRenewGrowLease's statements are
		// gated on cluster_grow_active still being held by THIS joiner. So a keeper goroutine that outlives its
		// release by one tick cannot re-fence membership, and a keeper for joiner A cannot extend joiner B's lock.
		if b.cl == nil || b.cl.node == nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
		}
		if req.JoinerNode == "" {
			return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "renew-lock requires a joiner"}
		}
		if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanRenewGrowLease(req.JoinerNode, b.cfg.Now())
		}); err != nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeNotLeader, Error: "renew grow lock (run against the leader): " + err.Error()}
		}
		// Read back the COMMITTED marker: a renewal that landed on a lock this joiner no longer owns is a
		// no-op at the SQL level, and reporting OK for it would let a stale keeper believe it still holds a
		// lock it does not. Tell it the truth so it stops.
		if j := growActiveJoiner(b.cl.node.RODB()); j != req.JoinerNode {
			return &proto.ClusterGrowResp{Code: growTriggerCodeLockNotHeld,
				Error: "the grow lock is no longer held by " + req.JoinerNode + " (holder=" + j + ")"}
		}
		return &proto.ClusterGrowResp{OK: true}

	case "release-lock":
		if b.cl == nil || b.cl.node == nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
		}
		// Surface a Propose failure (no false OK) — a stale marker would keep membership blocked. M1: the clear is
		// bound to req.JoinerNode, so releasing a completed grow of one joiner never wipes a different in-flight
		// grow's marker (the DELETE ... WHERE value=<joiner> is a no-op when the marker belongs to another joiner).
		if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClearGrowActive(req.JoinerNode) }); err != nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeNotLeader, Error: "release grow lock (run against the leader): " + err.Error()}
		}
		return &proto.ClusterGrowResp{OK: true}

	case "approve-join":
		handle := b.clusterAdminHandleFn()
		if handle == nil {
			return &proto.ClusterGrowResp{Code: growTriggerCodeNotReady, Error: "cluster admin backend not wired yet — retry"}
		}
		ar := handle(adminsock.Request{Op: adminsock.OpClusterJoinApprove, JoinBundle: req.JoinBundle})
		return &proto.ClusterGrowResp{OK: ar.OK, Code: ar.Code, Error: ar.Error, OpID: ar.OpID}

	case "join-status":
		handle := b.clusterAdminHandleFn()
		if handle == nil {
			return &proto.ClusterGrowResp{Code: growTriggerCodeNotReady, Error: "cluster admin backend not wired yet — retry"}
		}
		// OpID may carry either a real op_id OR (for the orchestrator's resume-discovery, findJoinOp) a
		// TARGET node id — match either, since OpClusterOps filters by both and the caller does not yet know
		// the op_id on a resume. Prefer a non-terminal op so a resume attaches to the live one.
		ar := handle(adminsock.Request{Op: adminsock.OpClusterOps, OpsNode: req.OpID})
		var match *adminsock.ClusterOpEntry
		for i := range ar.Ops {
			e := &ar.Ops[i]
			if e.OpID == req.OpID || e.TargetEnd == req.OpID {
				if match == nil || (!e.Terminal && match.Terminal) {
					match = e
				}
			}
		}
		if match != nil {
			return &proto.ClusterGrowResp{OK: true, OpID: match.OpID, OpState: match.OpState, Terminal: match.Terminal, LastError: match.LastError}
		}
		return &proto.ClusterGrowResp{Code: adminsock.CodeNodeUnknown, Error: "no operation for " + req.OpID}

	case "confirm-op":
		handle := b.clusterAdminHandleFn()
		if handle == nil {
			return &proto.ClusterGrowResp{Code: growTriggerCodeNotReady, Error: "cluster admin backend not wired yet — retry"}
		}
		ar := handle(adminsock.Request{Op: adminsock.OpClusterOpConfirm, OpID: req.OpID})
		return &proto.ClusterGrowResp{OK: ar.OK, Code: ar.Code, Error: ar.Error, OpID: req.OpID}

	case "mesh-peers":
		// Return the route-mesh peer triples ("server_name,route,bus_nkey") so a fresh joiner (which has no
		// replicated cluster_nodes yet) can render its own clustered nats.conf before booting. Any broker with
		// the committed roster can answer; the orchestrator targets the leader.
		if b.cl == nil || b.cl.node == nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
		}
		peers, qerr := meshPeerTriples(b.cl.node.RODB())
		if qerr != nil {
			return &proto.ClusterGrowResp{Code: adminsock.CodeStoreError, Error: "read mesh peers: " + qerr.Error()}
		}
		return &proto.ClusterGrowResp{OK: true, MeshPeers: peers}

	case "mesh-cutover":
		return b.performGrowCutover(&req)

	case "rebalance-proxy":
		handle := b.clusterAdminHandleFn()
		if handle == nil {
			return &proto.ClusterGrowResp{Code: growTriggerCodeNotReady, Error: "cluster admin backend not wired yet — retry"}
		}
		ar := handle(adminsock.Request{Op: adminsock.OpClusterRebalanceProxy})
		return &proto.ClusterGrowResp{OK: ar.OK, Code: ar.Code, Error: ar.Error}

	default:
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "unknown op: " + req.Op}
	}
}

// meshPeerTriples returns one "server_name,route,bus_nkey" entry per route-mesh broker in the committed
// roster (VOTER / CATCHING_UP / VOTER_ADD_FAILED — every phase that belongs in the nats route mesh), skipping
// any row missing a server_name / route / bus_nkey_pub (an identity not yet replicated — a joiner rendering
// against a partial set would produce a broken conf, so the orchestrator retries on an empty/short result).
// The phase strings are the DB-persisted schema literals used consistently across the cluster_nodes queries.
func meshPeerTriples(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT nats_server_id, nats_route, bus_nkey_pub FROM cluster_nodes ` +
		`WHERE phase IN ('VOTER','CATCHING_UP','VOTER_ADD_FAILED') AND nats_server_id != '' AND nats_route != '' ` +
		`AND bus_nkey_pub IS NOT NULL AND bus_nkey_pub != '' ORDER BY node_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var peers []string
	for rows.Next() {
		var server, route, nkey string
		if rows.Scan(&server, &route, &nkey) == nil {
			peers = append(peers, server+","+route+","+nkey)
		}
	}
	return peers, rows.Err()
}

// growTriggerCodeUnauthorized / growTriggerCodeNotReady mirror the upgrade-trigger codes: a bad signature is
// unauthorized (fail-closed), and a not-yet-wired admin backend is RETRIABLE (the orchestrator waits, not HALTs).
const (
	growTriggerCodeUnauthorized = "unauthorized"
	growTriggerCodeNotReady     = "cluster_not_ready"
	// growTriggerCodeLockNotHeld (R7b) tells a lease renewer that the lock it believes it holds is gone
	// (released, expired, or taken over). It is TERMINAL for the renewer — retrying cannot re-acquire,
	// because a renewal is structurally incapable of creating a lock.
	growTriggerCodeLockNotHeld = "lock_not_held"
)
