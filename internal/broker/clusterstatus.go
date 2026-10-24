package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// clusterstatus.go — D7 §17 status/doctor self-report + the adminsock adapter
// (build-and-prove; part of the guard-excluded ClusterAdmin mechanism). This is ONE
// broker's self-report; the ctl/NATS view aggregates reports from every reachable
// broker (it NEVER dials :7400), and the offline mode-B view (cluster status
// --offline) is the only one that direct-pings peers. Health → exit code is the
// healthExitCode SSOT.

// --- adminsock adapter ---

// clusterAdminBackend adapts ClusterAdmin to adminsock.ClusterAdminBackend. The
// injected probes (caughtUp / streamsReady) are the build-and-prove seam for the
// transports that learn a follower's applied_index and stream state — the harness
// wires them; production wiring is D9.
type clusterAdminBackend struct {
	admin        *ClusterAdmin
	caughtUp     func(nodeID string, barrier uint64) (bool, error)
	streamsReady func(nodeID string) (bool, error)
	drainNotice  time.Duration
	// B6 OPS#12 export-incident sources (may be nil in tests that don't exercise it):
	auditTail  func(ctx context.Context, sid string, n int) ([]adminsock.AuditEntry, error)
	activeSIDs func() ([]string, error)
	// C2: the cluster account public key (for minting invites on `cluster seeds publish`). nil/"" when
	// no account seed (single mode / unwired) — the CLI then prints the invite without a pin.
	accountPub func() string
	// C-rebalance: the leader-local proxy-home spread pass (`cluster rebalance proxy`). nil in single
	// mode / tests that don't wire it ⇒ the op replies cluster_not_enabled.
	rebalanceProxy func(dryRun bool) (*adminsock.ProxyRebalanceReport, error)
	// requestReExec (G5 #13) arms the broker-daemon self-re-exec (Broker.RequestReExec). nil in single
	// mode / tests that don't wire it ⇒ the reload op still replies but no restart is armed.
	requestReExec func()
	// fsArm tracks the online force-single sustained-quorum-loss dwell + single-shot arm token. nil ⇒
	// the online force-single replies cluster_not_enabled (offline floor still works).
	fsArm *forceSingleArm
}

// NewClusterAdminBackend builds the adminsock adapter. caughtUp/streamsReady may be
// nil (status/drain/remove/transfer still work; add's catch-up gate uses a
// leader-applied proxy when caughtUp is nil). auditTail/activeSIDs back export-incident
// (may be nil → export-incident returns history_unavailable).
func NewClusterAdminBackend(admin *ClusterAdmin, caughtUp func(string, uint64) (bool, error), streamsReady func(string) (bool, error),
	auditTail func(context.Context, string, int) ([]adminsock.AuditEntry, error), activeSIDs func() ([]string, error), accountPub func() string) adminsock.ClusterAdminBackend {
	return &clusterAdminBackend{admin: admin, caughtUp: caughtUp, streamsReady: streamsReady, drainNotice: 30 * time.Second,
		auditTail: auditTail, activeSIDs: activeSIDs, accountPub: accountPub}
}

func (b *clusterAdminBackend) HandleCluster(req adminsock.Request) adminsock.Response {
	// Status is leader-agnostic: any broker self-reports.
	if req.Op == adminsock.OpClusterStatus {
		rep, err := b.admin.StatusReport("ctl-nats")
		if err != nil {
			// StatusReport only fails on a DB/roster read → a store error (B2 item 4).
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
		}
		return adminsock.Response{Op: req.Op, OK: true, Cluster: rep}
	}
	// C6 建议5: `--homes` is a leader-agnostic RODB aggregate (any broker serves it) — before the gate.
	if req.Op == adminsock.OpClusterHomes {
		if b.admin.HomesReport == nil {
			return adminsock.Response{Op: req.Op, Error: "cluster mode not enabled", Code: adminsock.CodeClusterNotEnabled}
		}
		rep, err := b.admin.HomesReport()
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
		}
		return adminsock.Response{Op: req.Op, OK: true, Homes: rep}
	}
	// B6 OPS#3: an online backup is a read-only paged copy off the RO handle — ANY node serves
	// it (leader or follower), so it is handled BEFORE the leader-only gate.
	if req.Op == adminsock.OpClusterBackup {
		return b.handleBackup(req)
	}
	// B7 DOC#2: the ops view is a read-only derive off the replicated roster — any node serves it.
	if req.Op == adminsock.OpClusterOps {
		return b.handleClusterOps(req)
	}
	// C2: seeds SHOW is a read-only RODB derive — any node serves it (for `agent join`/doctor).
	if req.Op == adminsock.OpClusterSeedsShow {
		return b.handleSeedsShow(req)
	}
	// Audit MAJOR: export-incident is a read-only RODB assembler needed EXACTLY during a leaderless
	// QUORUM_LOST incident (the status card recommends it in that state) — handle it BEFORE the
	// leader gate so it is available when there is no leader, like status/backup/ops.
	if req.Op == adminsock.OpExportIncident {
		return b.handleExportIncident(req)
	}
	// Online force-single (the quorum-loss escape hatch): dispatch BEFORE the leader gate — a quorum-lost
	// survivor is never leader, so there is no leader to gate on or forward to. Local-socket-only by
	// construction (root-only Unix socket; no NATS path dispatches admin ops).
	if req.Op == adminsock.OpClusterForceSingleArm {
		return b.handleForceSingleArm(req)
	}
	if req.Op == adminsock.OpClusterForceSingleCommit {
		return b.handleForceSingleCommit(req)
	}
	// G5 #13: the broker-daemon reload REFUSES if this broker is the leader (dispatch before the leader
	// gate so it reaches the handler on the very node being restarted; the handler self-refuses on leader).
	if req.Op == adminsock.OpBrokerUpgradeReload {
		return b.handleBrokerUpgradeReload(req)
	}
	// R11 P11/#56: rotate-tunnel-cert is a SELF-ONLY verb — a broker can only hot-swap its OWN live
	// tunnel cert, and only while it is leader. It must BYPASS the generic mutating-verb leader-redirect
	// below: that redirect ("re-run on the leader host") is wrong guidance here — sending the operator to
	// brk1 to rotate brk2 still fails, because the TARGET must BE the leader, not merely be reached from
	// it. Dispatch it before the gate so RotateTunnelCert can give the correct "transfer leadership to
	// the target first" guidance directly (it distinguishes wrong-host from right-host-but-not-leader).
	if req.Op == adminsock.OpClusterRotateCrt {
		if err := b.admin.RotateTunnelCert(req.NodeID, req.CertFP, certRotationWindow); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	}
	// Mutating verbs are leader-local (§8.1, NO forwarding): fail fast naming the leader.
	if !b.admin.Node.IsLeader() {
		_, leaderID := b.admin.Node.LeaderWithID()
		host := b.admin.LeaderHost(leaderID)
		msg := "not leader; re-run on the leader host"
		if leaderID == "" {
			msg = "no leader (election in progress); retry"
		}
		return adminsock.Response{Op: req.Op, NotLeader: true, LeaderHost: host, Error: msg, Code: adminsock.CodeNotLeader}
	}

	switch req.Op {
	case adminsock.OpClusterDrain:
		return b.handleDrain(req)
	case adminsock.OpClusterRemove:
		if err := b.admin.RemoveNode(req.NodeID, req.Force); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterTransfer:
		if err := b.admin.TransferLeaderTo(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterAdd:
		return b.handleAdd(req)
	case adminsock.OpClusterJoinApprove:
		opID, err := b.admin.StartJoinOperation(req.JoinBundle)
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: opID}
	case adminsock.OpClusterRetire:
		opID, err := b.admin.StartRetireOperation(req.NodeID, req.Confirmed)
		var qc *ErrQuorumConfirmRequired
		if errors.As(err, &qc) {
			return adminsock.Response{Op: req.Op, QuorumProj: &adminsock.QuorumProjection{
				Voters: qc.Proj.Voters, Quorum: qc.Proj.Quorum, FaultTolerance: qc.Proj.FaultTolerance,
			}, Error: qc.Error(), Code: adminsock.CodeQuorumConfirmRequired}
		}
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: opID}
	case adminsock.OpClusterOpConfirm:
		if err := b.admin.ConfirmOp(req.OpID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: req.OpID}
	case adminsock.OpClusterOpAbort:
		if err := b.admin.AbortOp(req.OpID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: req.OpID}
	case adminsock.OpClusterSetRaftAddr:
		if err := b.admin.SetRaftAddr(req.NodeID, req.Host, req.AllowLoopback); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterSetRoute:
		if err := b.admin.SetNatsRoute(req.NodeID, req.NatsRoute, req.AllowLoopback); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterAlertRaise:
		return b.handleAlertRaise(req)
	case adminsock.OpClusterAlertClear:
		return b.handleAlertClear(req)
	case adminsock.OpClusterSeedsPublish:
		return b.handleSeedsPublish(req)
	case adminsock.OpClusterRebalanceProxy:
		if b.rebalanceProxy == nil {
			return adminsock.Response{Op: req.Op, Error: "proxy rebalance not available (cluster mode not enabled)", Code: adminsock.CodeClusterNotEnabled}
		}
		rep, err := b.rebalanceProxy(req.DryRun)
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: rep}
	default:
		return adminsock.Response{Op: req.Op, Error: "unknown cluster op: " + req.Op, Code: adminsock.CodeBadRequest}
	}
}

func (b *clusterAdminBackend) handleDrain(req adminsock.Request) adminsock.Response {
	if req.Abort {
		if err := b.admin.AbortDrain(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	}
	// Batch-A A13: refuse the synchronous retire path outright.
	//
	// cmd/tether/cluster.go hardcodes Retire:false, so no released CLI reaches
	// this; but the socket still accepted retire:true, and that branch performs
	// RemoveServer + a roster row delete — an IRREVERSIBLE raft membership
	// change — WITHOUT the protections the recoverable path has: no opStillLive
	// TOCTOU recheck, no replicable deadline (it carries the caller's wall
	// clock, so a broker restart loses it), no BLOCKED escape hatch, and no
	// resume. It is one hand-written socket request away from being reachable.
	//
	// `cluster retire` (OpClusterRetire) drives the same outcome through the
	// operation controller, which has all four. There is no reason to keep a
	// second, unprotected way to do the most destructive thing this API can do.
	//
	// For an older CLI that did send retire:true this is an improvement: it used
	// to silently take the unprotected path.
	//
	// Cross-version behaviour change. Review F-18: this used to point at "the
	// release note", which does not exist — the increment is not released yet,
	// and naming a document that is not there is the same false-reference habit
	// A4 set out to remove. The note is drafted under A13 in
	// docs/reviews/batch-a-progress.md and must ship with the release carrying
	// this commit.
	if req.Retire {
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeBadRequest,
			Error: "cluster drain --retire is no longer served on this socket: it performed an irreversible " +
				"raft membership change with no crash-resumable deadline and no BLOCKED escape hatch. " +
				"Use `tether cluster retire <node>` (the recoverable operation) instead."}
	}
	deadline := b.admin.Now().Add(b.drainNotice)
	if req.Now {
		deadline = b.admin.Now()
	}
	err := b.admin.DrainNode(req.NodeID, false, req.Confirmed, deadline, nil)
	var qc *ErrQuorumConfirmRequired
	if errors.As(err, &qc) {
		return adminsock.Response{Op: req.Op, QuorumProj: &adminsock.QuorumProjection{
			Voters: qc.Proj.Voters, Quorum: qc.Proj.Quorum, FaultTolerance: qc.Proj.FaultTolerance,
		}, Error: qc.Error(), Code: adminsock.CodeQuorumConfirmRequired}
	}
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
	}
	return adminsock.Response{Op: req.Op, OK: true}
}

func (b *clusterAdminBackend) handleAdd(req adminsock.Request) adminsock.Response {
	// Step 1: no token yet — issue a fresh single-use nonce for the operator to sign
	// on the joiner (`cluster sign-join <nonce>`), then re-run with --join-token.
	if req.JoinToken == "" {
		nonce, err := b.admin.IssueJoinNonce()
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, Nonce: nonce,
			Error: "sign-join required: run `tether cluster sign-join " + nonce + "` on the joining node, then re-run `cluster add` with --join-token <nonce>:<sig>"}
	}
	// Step 2: token = "<nonce>:<sigHex>". Consume the nonce (single-use) then admit.
	nonce, sigHex, ok := splitJoinToken(req.JoinToken)
	if !ok {
		return adminsock.Response{Op: req.Op, Error: "malformed --join-token (want <nonce>:<sigHex>)", Code: adminsock.CodeBadRequest}
	}
	// Audit SEC-MAJOR-1: validate the node_id charset fail-closed BEFORE it is persisted into the
	// roster / rendered into operator command lines (a shell-metachar/newline/path-separator id has
	// no legitimate use and would be echoed verbatim by status/apply/guided output).
	if err := proto.ValidateClusterNodeID(req.NodeID); err != nil {
		return adminsock.Response{Op: req.Op, Error: "invalid node_id: " + err.Error(), Code: adminsock.CodeBadRequest}
	}
	// B6 A3 version-skew gate — BEFORE claimJoinNonce so a rejected joiner does not burn the
	// single-use nonce.
	if resp, reject := b.versionSkewResponse(req); reject {
		return resp
	}
	// audit membership F2: ATOMICALLY claim the nonce (check + mark in-flight under one lock) so
	// two concurrent `cluster add` step-2 calls with the same token cannot both proceed. Released
	// below on AddNode failure so a retry can re-claim with the SAME token (review M9 property).
	if !b.admin.ClaimJoinNonce(nonce) {
		return adminsock.Response{Op: req.Op, Error: "unknown, already-used, or in-flight nonce; re-run `cluster add` (no token) for a fresh one", Code: adminsock.CodeNonceUsed}
	}
	// D9 round-1 BLOCKER: thread the joiner's full expose-home identity (else an added voter
	// has empty nats_server_id/tunnel_addr/cert_fp → resolveHomeForAgent can never home an
	// agent there → D6 failover onto grown nodes is impossible). nats_server_id == node_id
	// (the §6.5 SSOT). public_host defaults to the raft host if the operator omitted it.
	publicHost := req.PublicHost
	if publicHost == "" {
		publicHost = req.Host
	}
	in := cluster.ClusterNodeUpsertInput{
		NodeID: req.NodeID, Name: req.NodeID, NodeIdentPub: req.NodePub,
		NatsServerID: req.NodeID, RaftAddr: req.Host, NatsRoute: req.NatsRoute,
		TunnelAddr: req.TunnelAddr, PublicHost: publicHost,
		CertFP: req.CertFP, JoinNonce: nonce, JoinSigHex: sigHex, Now: b.admin.Now(),
	}
	caughtUp := func(barrier uint64) (bool, error) {
		if b.caughtUp == nil {
			// No catch-up transport wired (review m2): REFUSE rather than silently pass
			// on the leader's own cursor — the production transport that learns a
			// follower's applied_index is D9; the harness injects a real probe.
			return false, fmt.Errorf("catch-up transport not wired (D9); harness must inject caughtUp")
		}
		return b.caughtUp(in.NodeID, barrier)
	}
	if err := b.admin.AddNode(in, req.Host, caughtUp, 0); err != nil {
		b.admin.ReleaseJoinNonce(nonce) // release on failure so the operator can retry the same token
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
	}
	// success: the nonce stays claimed (single-use) — no separate consume needed.
	return adminsock.Response{Op: req.Op, OK: true}
}

// versionSkewResponse adapts the shared fail-closed exact-version decision to the legacy adminsock
// response shape. Only exact proto+release equality is admitted; missing or mismatched declarations
// carry the same terminal version_skew code. The fields are an unsigned operator-error preflight,
// while join PoP remains the authentication boundary.
func (b *clusterAdminBackend) versionSkewResponse(req adminsock.Request) (adminsock.Response, bool) {
	if err := versionSkewRefusal(req.JoinerProto, req.JoinerRelease, req.NodeID, b.admin.Logger); err != nil {
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeVersionSkew, Error: err.Error()}, true
	}
	return adminsock.Response{}, false
}

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
func clusterCodeFor(err error) string { return brokercontrol.ClusterCodeFor(err) }

const healthHealthyHA = brokercontrol.HealthHealthyHA
const healthDegraded = brokercontrol.HealthDegraded
const healthQuorumLost = brokercontrol.HealthQuorumLost
const healthForceSingle = brokercontrol.HealthForceSingle
const statusSchemaVersion = brokercontrol.StatusSchemaVersion
const certRotationWindow = brokercontrol.CertRotationWindow

func healthExitCode(health string) int             { return brokercontrol.HealthExitCode(health) }
func healthLabel(health string, voters int) string { return brokercontrol.HealthLabel(health, voters) }

const StreamActualUnobserved = brokercontrol.StreamActualUnobserved

func streamsAtTarget(nodes []adminsock.ClusterNodeStatus) bool {
	return brokercontrol.StreamsAtTarget(nodes)
}
func clusterVerdict(voters int, streamsOK bool, health string) string {
	return brokercontrol.ClusterVerdict(voters, streamsOK, health)
}

func computeHealth(forceSingle bool, leaderID string, voters int, topoDesired uint64, nodes []adminsock.ClusterNodeStatus) (health, banner, nextStep string) {
	return brokercontrol.ComputeHealth(forceSingle, leaderID, voters, topoDesired, nodes)
}
func accountKeySkewAdvisory(nodes []adminsock.ClusterNodeStatus) string {
	return brokercontrol.AccountKeySkewAdvisory(nodes)
}
func certExpiryAdvisory(nodes []adminsock.ClusterNodeStatus, window time.Duration) string {
	return brokercontrol.CertExpiryAdvisory(nodes, window)
}

type ErrJoinVersionSkew = brokercontrol.ErrJoinVersionSkew

func versionSkewRefusal(joinerProto int, joinerRelease, nodeID string, log *slog.Logger) error {
	return brokercontrol.VersionSkewRefusal(joinerProto, joinerRelease, nodeID, log)
}

func splitJoinToken(tok string) (nonce, sigHex string, ok bool) {
	return brokercontrol.SplitJoinToken(tok)
}
