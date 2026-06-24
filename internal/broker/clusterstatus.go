package broker

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
)

// clusterstatus.go — D7 §17 status/doctor self-report + the adminsock adapter
// (build-and-prove; part of the guard-excluded ClusterAdmin mechanism). This is ONE
// broker's self-report; the ctl/NATS view aggregates reports from every reachable
// broker (it NEVER dials :7400), and the offline mode-B view (cluster status
// --offline) is the only one that direct-pings peers. Health → exit code is the
// healthExitCode SSOT.

// Health strings + exit codes (§17 contract).
const (
	healthHealthyHA   = "HEALTHY_HA"
	healthDegraded    = "DEGRADED"
	healthQuorumLost  = "QUORUM_LOST"
	healthForceSingle = "FORCE_SINGLE"

	// statusSchemaVersion is the --json schema discriminator (§17 / external review
	// F4); bump on any breaking shape change so monitors can negotiate.
	statusSchemaVersion = 1

	// certRotationWindow is how long the previous tunnel cert pin stays valid after a
	// rotate-tunnel-cert (the cutover window agents reconnect within).
	certRotationWindow = 24 * time.Hour
)

// healthExitCode is the single source of truth mapping health → process exit code
// (0=HEALTHY-HA, 1=DEGRADED-writable, 2=read-only/quorum-lost, 3=force-single).
func healthExitCode(health string) int {
	switch health {
	case healthHealthyHA:
		return 0
	case healthDegraded:
		return 1
	case healthQuorumLost:
		return 2
	case healthForceSingle:
		return 3
	default:
		return 1
	}
}

type rosterRow struct {
	nodeID, name, phase string
}

// StatusReport builds this broker's self-report (view = "ctl-nats" or "offline").
// It joins the roster (cluster_nodes) against the live raft configuration for the
// role + the no-silent-fork INCONSISTENT cross-render, derives the leader, applied
// lag (self only; a leader cannot read a follower's cursor — that is the D9
// transport), the force-single marker, and the stream actual/target placeholder.
func (a *ClusterAdmin) StatusReport(view string) (*adminsock.ClusterStatusReport, error) {
	// Materialize roster + force-single marker FIRST (close rows) — no nested query
	// under the single-conn pool (the D6 deadlock lesson).
	var roster []rosterRow
	var forceSingle bool
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT node_id, name, phase FROM cluster_nodes ORDER BY node_id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r rosterRow
			if err := rows.Scan(&r.nodeID, &r.name, &r.phase); err != nil {
				_ = rows.Close()
				return err
			}
			roster = append(roster, r)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		var v string
		err = db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, cluster.MetaKeyForceSingle).Scan(&v)
		if err == nil {
			forceSingle = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cluster status: read roster: %w", err)
	}

	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return nil, fmt.Errorf("cluster status: raft config: %w", err)
	}
	role := map[string]string{}
	leaderID := ""
	voters := 0
	for _, s := range cfg {
		r := "voter"
		if !s.Voter {
			r = "learner"
		}
		if s.Leader {
			r = "leader"
			leaderID = s.NodeID
		}
		if s.Voter {
			voters++
		}
		role[s.NodeID] = r
	}

	// Self applied lag (leader-meaningful): CommitIndex - AppliedIndex.
	selfApplied, _ := a.node.AppliedIndex()
	commit := a.node.CommitIndex()
	selfLag := uint64(0)
	if commit > selfApplied {
		selfLag = commit - selfApplied
	}
	streamTarget := jsstream.ReplicasFor(voters)
	// External-review F1: report the REAL stream actual, not a synthesized actual==target. An
	// unwired probe (N=1) keeps the optimistic default; a wired-but-incomplete observation
	// reports 0 (unknown → a visible deficit in `actual/target`, never a false green).
	streamActual := streamTarget
	if a.streamObserve != nil {
		if rep, err := a.streamObserve(); err == nil && rep.Observed {
			streamActual = minStreamActual(rep, streamTarget)
		} else {
			streamActual = 0
		}
	}

	rep := &adminsock.ClusterStatusReport{SchemaVersion: statusSchemaVersion, View: view, LeaderID: leaderID}
	// §17 row 3: scatter-gather the broker-only cursor probe ONCE (if wired) so each peer's
	// reachability + applied-lag is REAL, not a blanket self-report. self is always reachable
	// (it is answering this request). nil poll ⇒ honest "unverified" for peers.
	var health map[string]proto.ClusterHealthResp
	if a.healthPoll != nil {
		health = a.healthPoll()
	}
	reachOf := func(nodeID string) (reachable bool, source string, lag uint64) {
		if nodeID == a.node.SelfID() {
			return true, "self", selfLag
		}
		if a.healthPoll == nil {
			return false, "unverified", 0 // single-broker view: we genuinely don't know
		}
		if r, ok := health[nodeID]; ok {
			l := uint64(0)
			if commit > r.AppliedIndex {
				l = commit - r.AppliedIndex
			}
			return true, "nats-health", l
		}
		return false, "nats-health", 0 // polled but no answer ⇒ unreachable
	}
	rosterIDs := map[string]bool{}
	for _, r := range roster {
		rosterIDs[r.nodeID] = true
		ro, inCfg := role[r.nodeID]
		// INCONSISTENT cross-render: roster says VOTER but raft config disagrees (or
		// the row is absent from the config), or vice-versa.
		inconsistent := (r.phase == phaseVoter && (!inCfg || ro == "learner")) ||
			(!inCfg && r.phase != phaseRetiring && r.phase != phaseAddFailed)
		reachable, source, lag := reachOf(r.nodeID)
		rep.Nodes = append(rep.Nodes, adminsock.ClusterNodeStatus{
			NodeID: r.nodeID, Name: r.name, Phase: r.phase, Role: ro,
			AppliedLag: lag, AccountNkMatch: true,
			StreamActual: streamActual, StreamTarget: streamTarget,
			Reachable: reachable, ReachSource: source,
			Inconsistent: inconsistent,
		})
	}
	// A raft voter with NO roster row is the loud INCONSISTENT case (§8.1).
	for _, s := range cfg {
		if s.Voter && !rosterIDs[s.NodeID] {
			reachable, source, _ := reachOf(s.NodeID)
			rep.Nodes = append(rep.Nodes, adminsock.ClusterNodeStatus{
				NodeID: s.NodeID, Role: role[s.NodeID], Reachable: reachable,
				ReachSource: source, Inconsistent: true,
			})
		}
	}

	rep.Health, rep.Banner, rep.NextStep = computeHealth(forceSingle, leaderID, voters, rep.Nodes)
	rep.ExitCode = healthExitCode(rep.Health)
	return rep, nil
}

// minStreamActual is the conservative cluster-wide stream actual: the smallest observed
// replica count across all streams (so a single under-replicated stream shows the deficit).
// No streams ⇒ the optimistic target (nothing to be under-replicated).
func minStreamActual(rep ReplicaReport, target int) int {
	if len(rep.Streams) == 0 {
		return target
	}
	m := rep.Streams[0].Actual
	for _, s := range rep.Streams[1:] {
		if s.Actual < m {
			m = s.Actual
		}
	}
	return m
}

// computeHealth derives the health verdict from the self-report. QUORUM_LOST is
// emitted ONLY from a positive no-leader observation (§B-9: never from absence of
// reports — that false-quorum-lost chain induces a wrong force-single).
func computeHealth(forceSingle bool, leaderID string, voters int, nodes []adminsock.ClusterNodeStatus) (health, banner, nextStep string) {
	if forceSingle {
		return healthForceSingle,
			"running in force-single (single node, no integrity) — recover as soon as peers are restored",
			"on each returning node: cluster recover --self-id <node-id> --dump-divergent <file>, then re-add it from the leader"
	}
	if leaderID == "" {
		// Review M2: this is ONE broker's local view (the multi-broker NATS
		// aggregation is D9). An empty LeaderWithID() on a partitioned MINORITY broker
		// is "I can't see a leader", which must NOT be presented as authoritative
		// cluster consensus or as an immediate force-single trigger (the false-quorum-
		// lost -> wrong-force-single chain §17/B-9 forbids). Exit 2 (read-only) is
		// honest for THIS broker; the banner makes the single-broker scope explicit and
		// conditions force-single on cross-checking the others.
		return healthQuorumLost,
			"THIS broker sees no leader — it is READ-ONLY (a single-broker view, NOT cluster consensus). Check the OTHER brokers before acting; force-single ONLY after confirming a majority is PERMANENTLY dead.",
			"on each survivor: cluster status --offline --db /var/lib/tether/tether.db ; then only after confirming peers dead: cluster force-single --self-id <this-node-id> --self-addr <this-host:7400> --confirm-peers-dead <ids...>"
	}
	degraded := false
	for _, n := range nodes {
		if n.Inconsistent || n.Phase == phaseCatchingUp || n.Phase == phaseAddFailed || n.Phase == phaseDraining || n.Phase == phaseRetiring {
			degraded = true
		}
		// External-review F5: a VOTER that a REAL health poll found UNREACHABLE, or that
		// trails the leader's commit beyond the §17 lag threshold, is DEGRADED — never call a
		// cluster HEALTHY_HA when a voter is observably down or behind (the §17 exit-code
		// contract). "nats-health" means it was actually polled (vs "unverified"/"self").
		if n.Phase == phaseVoter {
			if n.ReachSource == "nats-health" && !n.Reachable {
				degraded = true
			}
			if n.AppliedLag > observeLagThreshold {
				degraded = true
			}
		}
		// External-review F1: a JS stream below its target replica count (observed actual <
		// target) is DEGRADED — never report HEALTHY_HA while replication is short of target
		// (a node loss could then lose data). StreamTarget==0 means "not observed" → skip.
		if n.StreamTarget > 0 && n.StreamActual < n.StreamTarget {
			degraded = true
		}
	}
	proj := ProjectQuorum(voters, false)
	if proj.FaultTolerance == 0 {
		return healthDegraded,
			fmt.Sprintf("only tolerates %d failures (%d voters, quorum=%d) — add a node for HA", proj.FaultTolerance, proj.Voters, proj.Quorum),
			"cluster add <node-id> <host:7400> <node-pub>, then re-run with --join-token plus --tunnel-addr/--cert-fp/--nats-route"
	}
	if degraded {
		return healthDegraded, "a node is mid-join / draining or roster/raft INCONSISTENT — see the table", "cluster status"
	}
	return healthHealthyHA, "", ""
}

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
}

// NewClusterAdminBackend builds the adminsock adapter. caughtUp/streamsReady may be
// nil (status/drain/remove/transfer still work; add's catch-up gate uses a
// leader-applied proxy when caughtUp is nil).
func NewClusterAdminBackend(admin *ClusterAdmin, caughtUp func(string, uint64) (bool, error), streamsReady func(string) (bool, error)) adminsock.ClusterAdminBackend {
	return &clusterAdminBackend{admin: admin, caughtUp: caughtUp, streamsReady: streamsReady, drainNotice: 30 * time.Second}
}

func (b *clusterAdminBackend) HandleCluster(req adminsock.Request) adminsock.Response {
	// Status is leader-agnostic: any broker self-reports.
	if req.Op == adminsock.OpClusterStatus {
		rep, err := b.admin.StatusReport("ctl-nats")
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true, Cluster: rep}
	}
	// Mutating verbs are leader-local (§8.1, NO forwarding): fail fast naming the leader.
	if !b.admin.node.IsLeader() {
		_, leaderID := b.admin.node.LeaderWithID()
		host := b.admin.leaderHost(leaderID)
		msg := "not leader; re-run on the leader host"
		if leaderID == "" {
			msg = "no leader (election in progress); retry"
		}
		return adminsock.Response{Op: req.Op, NotLeader: true, LeaderHost: host, Error: msg}
	}

	switch req.Op {
	case adminsock.OpClusterDrain:
		return b.handleDrain(req)
	case adminsock.OpClusterRemove:
		if err := b.admin.RemoveNode(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterTransfer:
		if err := b.admin.TransferLeaderTo(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterAdd:
		return b.handleAdd(req)
	case adminsock.OpClusterRotateCrt:
		if err := b.admin.RotateTunnelCert(req.NodeID, req.CertFP, certRotationWindow); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	default:
		return adminsock.Response{Op: req.Op, Error: "unknown cluster op: " + req.Op}
	}
}

// TransferLeaderTo hands raft leadership to the SPECIFIC named voter (external
// review F1: the `cluster transfer-leader <node-id>` verb must target the named
// node, not let raft pick any peer). Resolves the node's raft address from the live
// configuration, verifies it is a voter and not self, then uses the TARGETED
// node.LeadershipTransferToServer.
func (a *ClusterAdmin) TransferLeaderTo(nodeID string) error {
	if nodeID == a.node.SelfID() {
		return fmt.Errorf("transfer-leader: %s is this node (already the leader)", nodeID)
	}
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return fmt.Errorf("transfer-leader %s: raft configuration: %w", nodeID, err)
	}
	for _, s := range cfg {
		if s.NodeID == nodeID {
			if !s.Voter {
				return fmt.Errorf("transfer-leader: %s is not a voter (cannot be leader)", nodeID)
			}
			return a.node.LeadershipTransferToServer(nodeID, s.Addr)
		}
	}
	return fmt.Errorf("transfer-leader: %s is not in the raft configuration", nodeID)
}

func (b *clusterAdminBackend) handleDrain(req adminsock.Request) adminsock.Response {
	if req.Abort {
		if err := b.admin.AbortDrain(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	}
	deadline := b.admin.now().Add(b.drainNotice)
	if req.Now {
		deadline = b.admin.now()
	}
	var ready func() (bool, error)
	if req.Retire && b.streamsReady != nil {
		ready = func() (bool, error) { return b.streamsReady(req.NodeID) }
	}
	err := b.admin.DrainNode(req.NodeID, req.Retire, req.Confirmed, deadline, ready)
	var qc *ErrQuorumConfirmRequired
	if errors.As(err, &qc) {
		return adminsock.Response{Op: req.Op, QuorumProj: &adminsock.QuorumProjection{
			Voters: qc.Proj.Voters, Quorum: qc.Proj.Quorum, FaultTolerance: qc.Proj.FaultTolerance,
		}, Error: qc.Error()}
	}
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error()}
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
		return adminsock.Response{Op: req.Op, Error: "malformed --join-token (want <nonce>:<sigHex>)"}
	}
	// Peek (don't consume yet, review M9): consume only AFTER AddNode succeeds so a
	// failed/stalled add can be retried with the SAME token (no fresh nonce + re-sign).
	if !b.admin.nonceKnown(nonce) {
		return adminsock.Response{Op: req.Op, Error: "unknown or already-used nonce; re-run `cluster add` (no token) for a fresh one"}
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
		CertFP: req.CertFP, JoinNonce: nonce, JoinSigHex: sigHex, Now: b.admin.now(),
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
		return adminsock.Response{Op: req.Op, Error: err.Error()}
	}
	b.admin.consumeJoinNonce(nonce) // consumed only on success
	return adminsock.Response{Op: req.Op, OK: true}
}

// splitJoinToken parses "<nonce>:<sigHex>" (the `cluster sign-join` output).
func splitJoinToken(tok string) (nonce, sigHex string, ok bool) {
	for i := 0; i < len(tok); i++ {
		if tok[i] == ':' {
			return tok[:i], tok[i+1:], i > 0 && i+1 < len(tok)
		}
	}
	return "", "", false
}

// leaderHost resolves a leader node_id to its roster public_host (for the non-leader
// fail-fast hint). Empty when unknown.
func (a *ClusterAdmin) leaderHost(leaderID string) string {
	if leaderID == "" {
		return ""
	}
	var host string
	_ = a.node.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT public_host FROM cluster_nodes WHERE node_id=?`, leaderID).Scan(&host)
	})
	return host
}
