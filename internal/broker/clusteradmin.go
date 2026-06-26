package broker

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// clusteradmin.go — D7 §8.1 two-phase membership orchestrator + the leader-startup
// reconciliation pass (the no-silent-fork guarantee; wired into production by runObserveLoop's
// leader-edge, audit M1). Post-D9 cutover production CONSTRUCTS this in CLUSTER mode
// (wireClusterLate builds the ClusterAdmin; NewClusterAdminBackend wires it into the admin
// socket); SINGLE mode does not. (The per-phase TestD7ProductionWiresNoCluster guard was a
// build-and-prove scaffold removed at the D9 cutover — see test/d9.)
//
// raft never leaks here: ClusterAdmin drives the raft config change through
// cluster.Node's string-typed wrappers (AddVoter/RemoveServer/...), so internal/
// broker imports internal/cluster but NOT hashicorp/raft (L-2).

// Phase constants (the 6-value cluster_nodes CHECK enum, §4.2).
const (
	phasePending    = "JOIN_VERIFIED_PENDING_VOTER"
	phaseCatchingUp = "CATCHING_UP"
	phaseVoter      = "VOTER"
	phaseAddFailed  = "VOTER_ADD_FAILED"
	phaseDraining   = "DRAINING"
	phaseRetiring   = "RETIRING"
)

const (
	defaultCatchUpPoll    = 50 * time.Millisecond
	defaultCatchUpMaxWait = 30 * time.Second
)

// ClusterAdmin orchestrates membership changes on the leader. It wraps a
// cluster.Node and is constructed only by the build-and-prove harness (and, at D9,
// by the cut-over broker). All admin changes are leader-local (§8.1): a non-leader
// fails fast naming the leader host (NonLeaderHint).
type ClusterAdmin struct {
	node      *cluster.Node
	logger    *slog.Logger
	catchPoll time.Duration
	now       func() time.Time

	// healthPoll, when set (wireClusterLate injects the broker-only cursor scatter-gather),
	// returns each reachable broker's self-reported AppliedIndex keyed by node_id. StatusReport
	// uses it to fill REAL reachability + applied-lag (§17 row 3) instead of stamping every peer
	// Reachable:true. nil ⇒ the honest "unverified" fallback (single-broker view).
	healthPoll func() map[string]proto.ClusterHealthResp

	// homesReport (C6 建议5), when set (wireClusterLate injects b.buildHomesReport), aggregates every
	// ALLOCATED expose + __proxy__ allocation's home/epoch/ready_reason for `cluster status --homes`.
	// nil ⇒ single mode (cluster_not_enabled). Leader-agnostic RODB read.
	homesReport func() (*adminsock.ClusterHomesReport, error)

	// streamObserve, when set (wireClusterLate injects the audit publisher's read-only replica
	// observation), reports the live JS stream replica state. StatusReport uses it to render
	// the REAL stream actual instead of synthesizing actual==target (external-review F1). nil
	// or an incomplete observation ⇒ actual is reported 0 (unknown / fail-closed, not green).
	streamObserve func() (ReplicaReport, error)

	// topoSelf, when set (wireClusterLate), returns THIS broker's C3 topology reconcile self-report.
	// StatusReport stamps the self row from it authoritatively (self does not poll itself for topo).
	topoSelf func() *topoSelfReport

	// caughtUpFn / streamsReadyFn (C4), when set (wireClusterLate), are the broker's catch-up + stream
	// readiness probes the operation controller drives (same funcs AddNode/DrainNode take as params).
	caughtUpFn     func(nodeID string, barrier uint64) (bool, error)
	streamsReadyFn func(nodeID string) (bool, error)

	// opAttempts (C4-M8) is the leader-local bounded-retry counter for an operation's failing
	// side-effect (keyed by op_id) → BLOCKED after opMaxAttempts. Reset on a leadership change (the new
	// leader re-counts from 0).
	opAttemptsMu sync.Mutex
	opAttempts   map[string]int

	// prepareTunnelCertRotate, when set by the production broker, verifies that the
	// target fingerprint is present on disk and returns a commit callback that hot-swaps
	// the live tunnel server cert after the DB pin update commits.
	prepareTunnelCertRotate func(newFP string) (func(), error)

	// issuedNonces is the leader-local single-use join-nonce store (OQ-5). `cluster
	// add` step 1 issues a fresh nonce; step 2 (with the signed token) consumes it.
	// This is a CONSISTENCY property, not a security boundary — §18.2.4 accepts a
	// compromised leader proposes anything; no replicated nonce ledger.
	nonceMu      sync.Mutex
	issuedNonces map[string]bool

	// B5 OPS#9 capacity-probe config, injected by SetCapacityProbes after construction (avoids a
	// NewClusterAdmin signature change rippling to every call site). All zero by default ⇒ no disk
	// statfs, ports_total 0 → the self-row capacity fields stay absent (honest "not configured").
	storeDir     string
	portBandLow  int
	portBandHigh int

	// B7 DOC#5 + C6 BD6: emitEvent (wireClusterLate injects b.pubSysEvent) is the generic leader-side
	// observability emitter the drain uses for `expose_rehomed` (back-compat) + the C6 home_reassign_*
	// / rehome_stalled lifecycle events. nil ⇒ no event (single broker / unwired); secret-free payloads.
	emitEvent func(kind string, fields map[string]any)
}

// NewClusterAdmin builds the orchestrator. now is injectable for tests (default
// time.Now); catchPoll defaults to 50ms.
func NewClusterAdmin(node *cluster.Node, logger *slog.Logger) *ClusterAdmin {
	if logger == nil {
		logger = slog.Default()
	}
	return &ClusterAdmin{node: node, logger: logger, catchPoll: defaultCatchUpPoll, now: time.Now, issuedNonces: map[string]bool{}}
}

// SetCapacityProbes injects the B5 OPS#9 self-row capacity config (store dir for the disk
// statfs + the port band for ports_used/total). Called once after construction by the production
// broker; left unset in tests that don't exercise capacity (fields stay absent — honest).
func (a *ClusterAdmin) SetCapacityProbes(storeDir string, bandLow, bandHigh int) {
	a.storeDir = storeDir
	a.portBandLow = bandLow
	a.portBandHigh = bandHigh
}

// IssueJoinNonce mints a fresh single-use nonce and records it leader-locally.
func (a *ClusterAdmin) IssueJoinNonce() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("cluster: issue nonce: %w", err)
	}
	nonce := hex.EncodeToString(buf[:])
	a.nonceMu.Lock()
	a.issuedNonces[nonce] = true
	a.nonceMu.Unlock()
	return nonce, nil
}

// claimJoinNonce ATOMICALLY claims an issued nonce for an in-flight AddNode (audit membership
// F2): it removes the nonce from the issued set and returns true ONLY if it was issued and not
// already claimed/consumed. This replaces the non-atomic peek-then-consume — adminsock dispatches
// each connection in its own goroutine (server.go `go s.handle`), so two concurrent `cluster add`
// step-2 calls with the SAME nonce could both pass a peek and both run the full two-phase add.
// The caller releaseJoinNonce()s on AddNode FAILURE so a retry can re-claim with the same token
// (preserving the review-M9 "no fresh nonce + re-sign on retry" property), and leaves it claimed
// on SUCCESS (single-use).
func (a *ClusterAdmin) claimJoinNonce(nonce string) bool {
	a.nonceMu.Lock()
	defer a.nonceMu.Unlock()
	if !a.issuedNonces[nonce] {
		return false
	}
	delete(a.issuedNonces, nonce) // claimed (removed from issued) = single-use + in-flight
	return true
}

// releaseJoinNonce returns a CLAIMED nonce to the issued set so a failed AddNode can be retried
// with the same token (audit membership F2).
func (a *ClusterAdmin) releaseJoinNonce(nonce string) {
	a.nonceMu.Lock()
	a.issuedNonces[nonce] = true
	a.nonceMu.Unlock()
}

// AddNode runs the §8.1 two-phase admission:
//
//	PHASE 1  Propose(OpClusterNodeUpsert) — every follower re-verifies the join PoP
//	         in Apply; roster lands at JOIN_VERIFIED_PENDING_VOTER.
//	(barrier) capture the leader's command-domain AppliedIndex under VerifyLeaderRead.
//	PHASE 2  AddVoter(node_id, raft_addr) — ONLY after phase 1 committed.
//	         err  -> phase VOTER_ADD_FAILED (consistent: roster knows, raft has no voter)
//	         ok   -> phase CATCHING_UP
//	(gate)   poll caughtUp(barrier) until the new voter's local applied_index reaches
//	         the barrier; max-wait -> catch_up_stalled (stays CATCHING_UP, hint set).
//	         ok   -> phase VOTER.
//
// caughtUp(barrier) reports whether the JOINING node's local applied_index has
// reached the barrier. The harness wires it to the joiner's AppliedIndex(); the
// production transport for a leader to learn a follower's applied_index is D9.
func (a *ClusterAdmin) AddNode(in cluster.ClusterNodeUpsertInput, raftAddr string, caughtUp func(barrier uint64) (bool, error), maxWait time.Duration) error {
	if maxWait <= 0 {
		maxWait = defaultCatchUpMaxWait
	}
	// C4: refuse a raw add when an operation already owns this node's membership (no two-writer hole).
	if err := a.assertNoActiveOp(in.NodeID); err != nil {
		return err
	}
	// PHASE 1 — roster admission (PoP re-verified by every follower in Apply).
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(in)
	}); err != nil {
		return fmt.Errorf("cluster add %s: phase-1 roster admission: %w", in.NodeID, err)
	}
	// Capture the command-domain catch-up barrier under a leader barrier (§8.2 / B-5).
	var barrier uint64
	if err := a.node.VerifyLeaderRead(func(*sql.DB) error {
		b, e := a.node.AppliedIndex()
		barrier = b
		return e
	}); err != nil {
		return fmt.Errorf("cluster add %s: capture catch-up barrier: %w", in.NodeID, err)
	}
	// PHASE 2 — raft config change.
	if err := a.node.AddVoter(in.NodeID, raftAddr); err != nil {
		// AddVoter failed: record VOTER_ADD_FAILED so status renders the half-state
		// (roster knows, raft has no voter — no silent fork). Best-effort; if THIS
		// Propose also fails (leadership lost), the new leader's reconciliation pass
		// re-derives the state from raft config + roster.
		if perr := a.setPhase(in.NodeID, phaseAddFailed, []string{phasePending}, err.Error()); perr != nil {
			a.logger.Error("cluster add: failed to record VOTER_ADD_FAILED", "node_id", in.NodeID, "err", perr)
		}
		return fmt.Errorf("cluster add %s: phase-2 AddVoter: %w", in.NodeID, err)
	}
	if err := a.setPhase(in.NodeID, phaseCatchingUp, []string{phasePending}, ""); err != nil {
		return fmt.Errorf("cluster add %s: phase->CATCHING_UP: %w", in.NodeID, err)
	}
	// CATCH-UP GATE (§8.2): poll the command-domain predicate; max-wait derives
	// catch_up_stalled (the node STAYS CATCHING_UP — a 7th phase would fail the
	// 0008 CHECK; the stall is carried in voter_add_error).
	deadline := a.now().Add(maxWait)
	for {
		ok, err := caughtUp(barrier)
		if err != nil {
			return fmt.Errorf("cluster add %s: catch-up probe: %w", in.NodeID, err)
		}
		if ok {
			break
		}
		if !a.now().Before(deadline) {
			if perr := a.setPhase(in.NodeID, phaseCatchingUp, []string{phaseCatchingUp}, "catch_up_stalled"); perr != nil {
				a.logger.Error("cluster add: failed to record catch_up_stalled", "node_id", in.NodeID, "err", perr)
			}
			return fmt.Errorf("cluster add %s: catch_up_stalled after %s (barrier=%d)", in.NodeID, maxWait, barrier)
		}
		time.Sleep(a.catchPoll)
	}
	if err := a.setPhase(in.NodeID, phaseVoter, []string{phaseCatchingUp}, ""); err != nil {
		return fmt.Errorf("cluster add %s: phase->VOTER: %w", in.NodeID, err)
	}
	// HA restored (review M3): once there is more than one voter, clear any
	// force_single_active marker left by a prior force-single, so the regrown cluster
	// stops reporting FORCE_SINGLE (exit 3). Best-effort; reconciliation re-clears.
	if nv, verr := a.node.NumVoters(); verr == nil && nv > 1 {
		if perr := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClearForceSingle()
		}); perr != nil {
			a.logger.Warn("cluster add: failed to clear force_single_active marker", "err", perr)
		}
	}
	return nil
}

// ReconcileMembershipOnLeadership is the §8.1 leader-startup reconciliation pass —
// the REAL no-silent-fork guarantee (status render is a display, not a safety
// property). On becoming leader, idempotently reconcile each roster row's phase
// against the live raft configuration, driven PURELY by the committed phase column +
// live config (never in-memory leader state), so a leadership change loses no
// progress and a naive cleanup never RemoveServers a mid-promote node:
//
//	{PENDING ∧ raft-voter}          AddVoter committed, old leader died before the
//	                                phase bump -> forward-complete to CATCHING_UP.
//	{VOTER_ADD_FAILED ∧ raft-voter} AddVoter timed out (≠ failed) and actually
//	                                committed -> forward-correct to CATCHING_UP.
//	{no roster row ∧ raft-voter}    loud INCONSISTENT; refuse auto-action (never
//	                                RemoveServer), point the operator at `cluster doctor`.
func (a *ClusterAdmin) ReconcileMembershipOnLeadership() error {
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return fmt.Errorf("cluster reconcile: raft configuration: %w", err)
	}
	// Materialize the roster FIRST (close the rows), THEN Propose — the FSM and the
	// reader share the one SetMaxOpenConns(1) pool, so a Propose nested inside an
	// open *sql.Rows would deadlock (the D6 lesson).
	roster := map[string]string{}
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, qerr := db.Query(`SELECT node_id, phase FROM cluster_nodes`)
		if qerr != nil {
			return qerr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id, phase string
			if serr := rows.Scan(&id, &phase); serr != nil {
				return serr
			}
			roster[id] = phase
		}
		return rows.Err()
	}); err != nil {
		return fmt.Errorf("cluster reconcile: read roster: %w", err)
	}
	voterInCfg := map[string]bool{}
	for _, s := range cfg {
		if !s.Voter {
			continue
		}
		voterInCfg[s.NodeID] = true
		phase, inRoster := roster[s.NodeID]
		switch {
		case !inRoster:
			a.logger.Error("cluster: INCONSISTENT — raft voter with no roster row; refusing auto-action, run `cluster doctor`",
				"node_id", s.NodeID, "addr", s.Addr)
		case phase == phasePending:
			if perr := a.setPhase(s.NodeID, phaseCatchingUp, []string{phasePending}, ""); perr != nil {
				a.logger.Error("cluster reconcile: PENDING->CATCHING_UP", "node_id", s.NodeID, "err", perr)
			}
		case phase == phaseAddFailed:
			if perr := a.setPhase(s.NodeID, phaseCatchingUp, []string{phaseAddFailed}, ""); perr != nil {
				a.logger.Error("cluster reconcile: VOTER_ADD_FAILED->CATCHING_UP", "node_id", s.NodeID, "err", perr)
			}
		}
	}
	// The OTHER fork direction (review B1): a roster row in a LIVE phase that is NOT a
	// raft voter — e.g. a bare `remove` of a healthy VOTER that dropped the raft voter
	// but left the roster stuck. We CANNOT safely auto-heal (we lack the join proof to
	// re-add it), so log loud INCONSISTENT so `doctor` + the operator see it (the
	// RemoveNode phase-gate now prevents creating this state; this is the backstop).
	for id, phase := range roster {
		if voterInCfg[id] {
			continue
		}
		if phase == phaseVoter || phase == phaseCatchingUp || phase == phasePending {
			a.logger.Error("cluster: INCONSISTENT — roster row in a live phase but NOT a raft voter; run `cluster doctor` (a bare remove of a live node, or a lost AddVoter)",
				"node_id", id, "phase", phase)
		}
	}
	return nil
}

// setPhase proposes an OpClusterNodePhase transition (the baked WHERE phase IN
// (<preds>) guard makes a disallowed/stale transition a deterministic no-op).
func (a *ClusterAdmin) setPhase(nodeID, to string, preds []string, voterAddErr string) error {
	return a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodePhase(nodeID, to, preds, voterAddErr, a.now())
	})
}

// PublishSeeds (C2) records the cluster's public client-dialable endpoints + bootstrap URL via raft
// (leader-only) and returns the new seed_generation so the CLI can echo it. Leader-side URL
// validation lives in cluster.PlanClusterSeedsPublish (rejects bad scheme / empty host / oversize).
func (a *ClusterAdmin) PublishSeeds(endpoints []string, bootstrap string) (gen uint64, err error) {
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterSeedsPublish(endpoints, bootstrap, a.now())
	}); err != nil {
		return 0, err
	}
	_, _, gen, err = cluster.Seeds(a.node.RODB())
	return gen, err
}

// ReadSeeds (C2) returns the published seed config (leader-agnostic, RODB read).
func (a *ClusterAdmin) ReadSeeds() (endpoints []string, bootstrap string, gen uint64, err error) {
	return cluster.Seeds(a.node.RODB())
}
