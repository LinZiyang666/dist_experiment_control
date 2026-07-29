package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
)

// force_single_finalize.go (batch C / C1) — the RETRY path for an online force-single's roster prune.
//
// WHAT THIS IS AND, MORE IMPORTANTLY, WHAT IT IS NOT
// --------------------------------------------------
// handleForceSingleCommit is UNCHANGED on the happy path. It still does the raft rewrite, waits for
// leadership, writes the force_single_active marker and the recovery epoch, and then attempts the
// roster prune and the seed convergence SYNCHRONOUSLY. When that works — which is the overwhelmingly
// common case — no operation row is created at all and the operator sees exactly what they saw
// before.
//
// Three separate arguments say the synchronous shape must stay:
//
//  1. The marker IS the visibility of the emergency. proto.DestructiveGate.Blocked() is
//     `QuorumLost || ForceSingleActive`, and the instant RecoverToSelfOnline returns, this node is a
//     writable leader — so QuorumLost goes false. If the marker were written asynchronously, there
//     would be a window in which the destructive gate is FULLY OPEN on a zero-redundancy node whose
//     JetStream is still 503: `session rm`, `expose`, `push` would all be admitted.
//
//  2. The prune is a PREREQUISITE of the documented next step. A ghost roster row is absent from the
//     raft config, so `cluster status` gives it Role "", and `reconcile nats --to-standalone` refuses
//     on any unrecognized role ("cannot prove N=1, refusing"). The runbook says, verbatim, "The
//     abandoned peers are already pruned (below), so the N=1 voter tally passes and --to-standalone
//     is unlocked". Making the prune asynchronous would break the operator's very next command with
//     an error that names a raft role rather than the actual cause.
//
//  3. Seed convergence already self-heals: driveLeaderMaintenance runs
//     deriveAndConvergeSeedsFromRoster on EVERY leader tick, and ReconcileMembershipOnLeadership runs
//     it again on every leadership edge.
//
// So this op exists for exactly one thing: the case where the synchronous prune FAILED. Before batch
// C that was a permanent dead end, and the code said so about itself — "a re-run to retry a failed
// prune is REFUSED by the dwell gate (CodeQuorumNotLost), so a LOUD fail here would be an unreachable
// dead-end". The dwell gate refuses because the node now HAS leader contact: it is its own leader.
// The result was a ghost roster row that nothing would ever remove, which is the direct origin of the
// permanent ghost VOTER on the live fleet.
//
// EVERY EXIT IS TERMINAL
// ----------------------
// There is deliberately no BLOCKED state. Today's failure mode already leaves the operator a
// deterministic finalizer (`cluster recovery node remove <ghost>`); a non-terminal op would take that
// away, because assertNoActiveOp fences the target's membership plane while an op is in flight. An
// operator standing over a freshly force-singled survivor has no peer to confirm against and no
// reason to be looking at `cluster ops`. Replacing "ghosts plus a loud log line" with "ghosts plus a
// fenced membership plane" would be a net regression, so the budget below always lands on a terminal
// state.

// opForceSingleFinalizeBudget bounds the retry. Derived from the drive cadence rather than picked:
// the controller advances an op once per observe tick, so this is "keep trying for a couple of
// dozen ticks". It is carried in the op row's catchup_deadline column — a REPLICATED deadline, not a
// process-local counter.
//
// The counter alternative was rejected on evidence. a.opAttempts is a leader-local map that resets on
// every leadership change and every restart — and a disaster recovery is exactly when a broker is
// restarting repeatedly. Worse, it only increments when a step returns an ERROR, while the failure
// mode that matters most here is "the propose returned nil but the observation never becomes true"
// (a RowsAffected==0 no-op, a poison-skip, a bounded-stale read behind the commit). That class would
// never increment the counter at all, so the op would never reach the budget and never go terminal.
// jsPlacementAdvance already established the replicated-deadline shape in this file's neighbour.
const opForceSingleFinalizeBudget = 12 * observeTickInterval

// forceSingleFinalizeParams is the finalize op's replicated re-drive input, baked ONCE at creation.
//
// Abandoned in particular must NOT be re-derived on each drive. The obvious implementation — read the
// roster, keep whatever is not in the raft config — shrinks as the prune partially succeeds, so the
// rows that failed on the first attempt would drop out of the set and never be retried. The whole
// point of the op is to retry precisely those.
type forceSingleFinalizeParams struct {
	Abandoned []string `json:"abandoned"`
}

// confirmOpKindGuard refuses a confirm for an op kind that has nothing to confirm. It is a separate
// function so it can be called — and TESTED — strictly BEFORE ConfirmOp's clearOpAttempts.
//
// That ordering is the whole content of this guard. clearOpAttempts sits above ConfirmOp's kind
// switch and runs unconditionally, so putting the refusal after it would return an error to the
// caller while the SIDE EFFECT still landed: a retry loop calling `cluster ops confirm` would reset
// the finalize op's retry budget on every call and keep it alive indefinitely. A test asserting only
// "it returns an error" cannot see that, because the un-guarded code returned one too.
func (a *ClusterAdmin) confirmOpKindGuard(op *cluster.Operation) error {
	if op.Kind == cluster.OpKindForceSingleFinalize {
		return fmt.Errorf("operation %q is a force-single finalize; it is not awaiting a confirm "+
			"(it drives itself to a terminal state — `cluster ops show %s` reports where it got to)",
			op.OpID, op.OpID)
	}
	return nil
}

func (a *ClusterAdmin) finalizeParams(op *cluster.Operation) (forceSingleFinalizeParams, error) {
	var p forceSingleFinalizeParams
	if strings.TrimSpace(op.Params) == "" {
		return p, fmt.Errorf("force-single finalize op %s has no params", op.OpID)
	}
	if err := json.Unmarshal([]byte(op.Params), &p); err != nil {
		return p, fmt.Errorf("force-single finalize op %s: decode params: %w", op.OpID, err)
	}
	if len(p.Abandoned) == 0 {
		return p, fmt.Errorf("force-single finalize op %s: empty abandoned set", op.OpID)
	}
	return p, nil
}

// startForceSingleFinalize creates the retry op. Called ONLY from handleForceSingleCommit, and only
// after the synchronous prune has already failed.
//
// It returns the op id, or "" with the reason it could not be created. A creation failure is NOT
// fatal to the recovery: the caller falls back to exactly the pre-batch-C behaviour (a loud warning
// naming `cluster recovery node remove`). That fallback matters because PlanClusterOpStart's
// single-active guard is a silent RowsAffected==0 no-op when the target already has a non-terminal
// op — reachable when a half-finished `cluster retire <self>` was in flight when quorum was lost —
// and turning a survivable recovery into a hard failure at that moment would be the exact kind of
// "one more failure mode during an emergency" this change is supposed to avoid.
func (a *ClusterAdmin) startForceSingleFinalize(selfID string, abandoned []string) (string, error) {
	if len(abandoned) == 0 {
		return "", fmt.Errorf("no abandoned peers to prune")
	}
	params, err := json.Marshal(forceSingleFinalizeParams{Abandoned: abandoned})
	if err != nil {
		return "", fmt.Errorf("encode params: %w", err)
	}
	// The discriminator must be FRESH per attempt, exactly as the retire producer uses a timestamp and
	// the join producer uses a nonce. mintOpID is a pure hash, PlanClusterOpStart refuses an op_id that
	// already exists, and cluster_operations is never garbage-collected — so a stable discriminator
	// (self + the ghost set) would let this op be created EXACTLY ONCE in the life of the cluster.
	// After a first attempt ended in FS_GHOST_LEFT, every later leadership edge would recompute the
	// same id, no-op the INSERT, fail the read-back and log a warning: the crash-window cover would be
	// permanently dead even after the operator fixed whatever made the prune fail.
	opID := mintOpID(cluster.OpKindForceSingleFinalize, selfID, a.now().Format(time.RFC3339Nano))
	in := cluster.OpStartInput{
		OpID:       opID,
		Kind:       cluster.OpKindForceSingleFinalize,
		TargetNode: selfID, // self, NEVER a ghost — see the comment on target choice below
		InitState:  cluster.OpStateFSPrunePending,
		// The operator already typed this node's id at a TTY to reach the commit, so there is nothing
		// further to confirm; marking it confirmed keeps `cluster ops` from inviting one.
		Confirmed:       true,
		CatchupDeadline: a.now().Add(opForceSingleFinalizeBudget).UnixNano(),
		Params:          string(params),
		Timeline:        a.initTimeline(cluster.OpStateFSPrunePending),
	}
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, a.now())
	}); err != nil {
		return "", err
	}
	// READ-BACK. PlanClusterOpStart's single-active guard makes a second start a deterministic
	// RowsAffected==0 no-op and Propose still returns nil, so without this the caller would hand the
	// operator an op id that does not exist and `cluster ops show` would answer "not found".
	live, err := cluster.ActiveOperationForTarget(a.node.RODB(), selfID)
	if err != nil {
		return "", fmt.Errorf("read back the finalize op: %w", err)
	}
	if live == nil || live.OpID != opID {
		return "", fmt.Errorf("the finalize op was not created (an operation for %q is already in flight)", selfID)
	}
	return opID, nil
}

// TARGET CHOICE: the op targets SELF, never one of the ghosts.
//
// A ghost target would be actively harmful: assertNoActiveOp is per-target, and
// `cluster recovery node remove <ghost>` goes through it. Targeting the ghost would make the op fence
// the one operator command that can clean up after it — a finalizer that disables its own fallback.
// Targeting self does fence `drain`/`retire` of this node while the op is live, which is both
// harmless (nobody retires the sole survivor of a disaster mid-recovery) and short: every exit is
// terminal and bounded by opForceSingleFinalizeBudget.

// driveForceSingleFinalize advances the finalize op by ONE step per tick, advance-after-observe.
func (a *ClusterAdmin) driveForceSingleFinalize(op *cluster.Operation) {
	params, err := a.finalizeParams(op)
	if err != nil {
		// A malformed op row cannot be retried into shape. Terminal, loudly.
		a.logger.Error("cluster: force-single finalize op is unusable", "op", op.OpID, "err", err)
		_ = a.transition(op, cluster.OpStateFSGhostLeft, true, err.Error(), nil)
		return
	}

	remaining, rerr := a.rosterRowsPresent(params.Abandoned)
	if rerr != nil {
		a.recordOpError(op, fmt.Errorf("read roster: %w", rerr))
		a.finalizeBudgetCheck(op, params, "could not read the roster to confirm the prune")
		return
	}
	if len(remaining) == 0 {
		// OBSERVED gone — not merely "the propose returned nil". Converge the published seeds now that
		// the roster really is {self}; this ordering is INV-4 and it is load-bearing: deriving seeds
		// before the prune commits re-derives the dead endpoints straight back into them. Seed
		// convergence is also idempotent and self-healing (every leader tick runs it), so a failure
		// here degrades to "the backstop gets it" rather than holding the op open.
		if serr := a.deriveAndConvergeSeedsFromRoster(); serr != nil {
			a.logger.Warn("cluster: force-single finalize pruned the ghosts but seed convergence failed; "+
				"the periodic leader backstop will retry it", "op", op.OpID, "err", serr)
		}
		_ = a.transition(op, cluster.OpStateFSFinalized, true, "", nil)
		// Every promised fact has now landed, so the on-disk intent has nothing left to protect.
		if cerr := clusteroffline.ClearOnlineIntent(a.dataDir); cerr != nil {
			a.logger.Warn("cluster: force-single finalize completed but the on-disk intent could not be "+
				"cleared; a later leadership tick will re-verify and clear it", "err", cerr)
		}
		a.logger.Info("cluster: force-single finalize complete — abandoned peers pruned from the roster",
			"op", op.OpID, "abandoned", params.Abandoned)
		return
	}

	// Re-confirm the ghosts are STILL absent from the committed raft config before deleting their
	// roster rows. params is a snapshot taken during the recovery; if any of those ids has since been
	// re-admitted (an operator rebuilt the cluster while this op sat in the queue), deleting its row
	// would tear a live member out of the roster. Only ever prune the ids the op was created with,
	// and only while they remain out of the config.
	// origin: batch-c internal review C1-N3 (reproduced by the verifier against a live fixture). The
	// re-confirmation must be the FULL ghost signature — phase VOTER *and* absent from the raft config
	// — not just the second half.
	//
	// "Absent from the config" alone is satisfied by a node that has just been RE-ADMITTED, because
	// that is the order the admission protocol writes in: the roster row lands first (PHASE 1
	// PlanClusterNodeUpsert / the ROSTER_COMMITTED rung) and AddVoter/AddNonvoter follows a tick or
	// more later. A finalize op sitting in the same drive loop would delete the fresh row out from
	// under it, and the join would then complete into "raft voter with no roster row" — the state
	// ReconcileMembershipOnLeadership logs as INCONSISTENT and explicitly refuses to auto-heal.
	// assertNoActiveOp cannot help here: it is per-target, and this op targets self, not the joiner.
	//
	// forceSingleGhostRows applies both halves, so reusing it keeps the creation-time and prune-time
	// predicates identical by construction rather than by two copies agreeing.
	ghostsNow, cerr := a.forceSingleGhostRows()
	if cerr != nil {
		a.recordOpError(op, fmt.Errorf("read raft config: %w", cerr))
		a.finalizeBudgetCheck(op, params, "could not read the raft configuration")
		return
	}
	stillGhost := make(map[string]bool, len(ghostsNow))
	for _, id := range ghostsNow {
		stillGhost[id] = true
	}
	var prune []string
	for _, id := range remaining {
		if stillGhost[id] {
			prune = append(prune, id)
		}
	}
	if len(prune) == 0 {
		err := fmt.Errorf("every remaining abandoned peer (%s) has stopped looking like a ghost — it is "+
			"back in the raft configuration, or its roster row has been re-admitted and is mid-join; "+
			"refusing to prune a live member", strings.Join(remaining, ","))
		a.logger.Warn("cluster: force-single finalize stopping", "op", op.OpID, "err", err)
		_ = a.transition(op, cluster.OpStateFSGhostLeft, true, err.Error(), nil)
		return
	}

	if perr := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodePrune(prune, a.now())
	}); perr != nil {
		a.recordOpError(op, fmt.Errorf("prune abandoned peers: %w", perr))
	}
	// Do NOT advance here even on success: the next tick re-reads the roster and advances only on the
	// OBSERVED absence of the rows. A propose that returns nil is not evidence the rows are gone.
	a.finalizeBudgetCheck(op, params, "the prune did not take effect")
}

// finalizeBudgetCheck lands the op on FS_GHOST_LEFT once its replicated deadline passes. A zero
// deadline counts as expired, matching the convention jsPlacementAdvance established for legacy or
// hand-seeded rows.
func (a *ClusterAdmin) finalizeBudgetCheck(op *cluster.Operation, params forceSingleFinalizeParams, why string) {
	if op.CatchupDeadline != 0 && a.now().UnixNano() <= op.CatchupDeadline {
		return
	}
	remaining, err := a.rosterRowsPresent(params.Abandoned)
	if err == nil && len(remaining) == 0 {
		// The prune DID land — most likely on this very tick, since the propose happens immediately
		// above this check. Treating an empty remaining-set as "gave up" was a real defect: it ended a
		// SUCCESSFUL finalize in FS_GHOST_LEFT and printed a ghost list naming rows that no longer
		// exist, sending the operator to `cluster recovery node remove` for nodes that are already gone
		// and telling them --to-standalone is blocked when it is not.
		if serr := a.deriveAndConvergeSeedsFromRoster(); serr != nil {
			a.logger.Warn("cluster: force-single finalize pruned the ghosts on its last tick but seed "+
				"convergence failed; the periodic leader backstop will retry it", "op", op.OpID, "err", serr)
		}
		_ = a.transition(op, cluster.OpStateFSFinalized, true, "", nil)
		if cerr := clusteroffline.ClearOnlineIntent(a.dataDir); cerr != nil {
			a.logger.Warn("cluster: force-single finalize completed on its final tick but the on-disk "+
				"intent could not be cleared", "err", cerr)
		}
		a.logger.Info("cluster: force-single finalize complete on its final tick — abandoned peers pruned",
			"op", op.OpID, "abandoned", params.Abandoned)
		return
	}
	if err != nil {
		// The roster is unreadable, so we cannot name which rows survive. Report the whole set rather
		// than an empty one: over-naming sends the operator to check rows that may already be gone,
		// which is recoverable; under-naming leaves a ghost nobody was told about, which is not.
		remaining = params.Abandoned
	}
	msg := fmt.Sprintf("force-single finalize gave up after %s: %s. Ghost roster rows remain: %s. "+
		// origin: batch-c internal review sweep-F1 — --manual is mandatory (a bare `remove` refuses and
		// redirects to `cluster retire`, which must never be aimed at a ghost).
		"Run `tether cluster recovery node remove <id> --manual` for each. Until they are gone, "+
		"`tether cluster reconcile nats --to-standalone` REFUSES (a ghost has no raft role, so N=1 "+
		"cannot be proven) and this survivor's JetStream stays 503.",
		opForceSingleFinalizeBudget, why, strings.Join(remaining, ", "))
	a.logger.Error("cluster: force-single finalize left ghosts", "op", op.OpID, "remaining", remaining)
	_ = a.transition(op, cluster.OpStateFSGhostLeft, true, msg, nil)
}

// opForceSingleFinalizeBackoff is how long to wait after a finalize gave up before starting another
// from the leadership edge. Two budgets: long enough that a crash-restart loop cannot mint an op per
// restart, short enough that an operator who fixes the cause does not wait long for the retry.
const opForceSingleFinalizeBackoff = 10 * opForceSingleFinalizeBudget

// recentlyGaveUpOnFinalize reports whether a finalize op for this node ended in FS_GHOST_LEFT inside
// the backoff window. Read errors report FALSE (proceed): failing to start the retry is the worse
// outcome of the two, since the ghosts are already confirmed present by the caller.
func (a *ClusterAdmin) recentlyGaveUpOnFinalize(selfID string) bool {
	ops, err := cluster.RecentOperations(a.node.RODB(), 200)
	if err != nil {
		return false
	}
	cutoff := a.now().Add(-opForceSingleFinalizeBackoff)
	for _, op := range ops {
		if op.Kind != cluster.OpKindForceSingleFinalize || op.TargetNode != selfID {
			continue
		}
		if op.OpState != cluster.OpStateFSGhostLeft {
			continue
		}
		// UpdatedAt is the replicated TEXT timestamp. cluster.LitTime bakes it with time.Time.String(),
		// NOT RFC3339 — parsing it as RFC3339 would fail on every row and silently disable this backoff
		// entirely. RFC3339 is accepted as a fallback so a hand-seeded or future-format row still reads.
		// An unparseable value counts as NOT recent, the same direction as a read error: failing to
		// start the retry is the worse outcome, since the caller has already confirmed ghosts exist.
		at, ok := parseOpTimestamp(op.UpdatedAt)
		if ok && at.After(cutoff) {
			return true
		}
	}
	return false
}

// opTimestampLayouts are the formats a cluster_operations timestamp column can hold. The FIRST is the
// one production writes: cluster.LitTime bakes with time.Time.String(). Getting this wrong is silent —
// every parse fails, and whatever the caller does with "unparseable" becomes its only behaviour.
var opTimestampLayouts = []string{"2006-01-02 15:04:05.999999999 -0700 MST", time.RFC3339Nano, time.RFC3339}

func parseOpTimestamp(s string) (time.Time, bool) {
	for _, layout := range opTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// rosterRowsPresent returns which of ids still have a cluster_nodes row. This is the OBSERVATION the
// prune step advances on.
func (a *ClusterAdmin) rosterRowsPresent(ids []string) ([]string, error) {
	var present []string
	err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		present = present[:0]
		for _, id := range ids {
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id = ?`, id).Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				present = append(present, id)
			}
		}
		return nil
	})
	return present, err
}

// raftConfigIDs is the set of node ids in the COMMITTED raft configuration.
func (a *ClusterAdmin) raftConfigIDs() (map[string]bool, error) {
	servers, err := a.node.RaftConfiguration()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(servers))
	for _, s := range servers {
		out[s.NodeID] = true
	}
	return out, nil
}

// forceSingleGhostRows returns roster rows that carry a LIVE VOTER phase yet are absent from the
// committed raft configuration — the exact signature a force-single leaves behind when its prune did
// not land.
//
// The phase test is VOTER and nothing wider on purpose. A node mid-join legitimately has a roster row
// before it appears in the config (that is the order the join ladder writes them in), and it sits in
// JOIN_VERIFIED_PENDING_VOTER / CATCHING_UP while it does. Accepting any "live" phase here would let
// a boot-time reconcile mistake a joining node for a ghost and delete its roster row.
func (a *ClusterAdmin) forceSingleGhostRows() ([]string, error) {
	inCfg, err := a.raftConfigIDs()
	if err != nil {
		return nil, err
	}
	var ghosts []string
	err = a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, qerr := db.Query(`SELECT node_id FROM cluster_nodes WHERE phase = ?`, phaseVoter)
		if qerr != nil {
			return qerr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if serr := rows.Scan(&id); serr != nil {
				return serr
			}
			if !inCfg[id] {
				ghosts = append(ghosts, id)
			}
		}
		return rows.Err()
	})
	return ghosts, err
}

// resumeForceSingleFinalizeOnLeadership is the CRASH-WINDOW cover, and it is why C1 is not simply
// "create the op inside the commit handler".
//
// RecoverToSelfOnline writes the {self} configuration to disk before it returns. A process killed
// after that but before the prune leaves a survivor that is a perfectly healthy single-voter cluster
// carrying ghost roster rows — and NOTHING creates an op for it, because the commit handler is long
// gone. Deriving the op from the SUBSTRATE on the leadership edge covers that window; deriving it
// only from the commit path would ship a mechanism whose coverage is narrower than it looks, which is
// worse than shipping none.
//
// The pre-rewrite intent path additionally proves the committed configuration is exact {self}
// before it mutates anything. The marker-only UPGRADE FALLBACK needs the following four conditions;
// dropping any one of them makes that substrate inference dangerous:
//
//	force_single_active is set  — proof a force-single actually happened here. Without it, any
//	                              roster/raft divergence would trigger a prune.
//	exactly one voter           — a recovered survivor is N=1. Without it, a legitimately larger
//	                              cluster with a transient roster lag qualifies.
//	phase == VOTER, not in cfg  — the ghost signature. Narrower than "any live phase" so a node
//	                              mid-join is never mistaken for one.
//	no in-flight op on self     — idempotence; also stops a retry storm on every election.
//
// Missing any of them means we do exactly what this code path did before: log INCONSISTENT and leave
// it to the operator.
func (a *ClusterAdmin) resumeForceSingleFinalizeOnLeadership() {
	if a == nil || a.node == nil {
		return
	}
	// The normal commit handler owns a freshly written intent until its synchronous prune finishes.
	// Do not race it into creating the failure-branch retry op. A restarted process has no holder and
	// takes this lock normally.
	if !a.forceSingleIntentMu.TryLock() {
		return
	}
	defer a.forceSingleIntentMu.Unlock()
	selfID := a.node.SelfID()
	if selfID == "" {
		return
	}

	// PATH 1 — the DURABLE INTENT (EXTERNAL review B1). This runs FIRST and does not consult the
	// marker, because the whole point is to cover the window in which the marker is the thing that
	// failed to be written. The intent was fsync'd BEFORE the irreversible rewrite, so its presence
	// proves recovery was authorized, while forceSingleRaftRewriteLanded below separately proves the
	// mutation actually happened.
	if a.dataDir == "" {
		// A cluster-mode broker always has one (it is where raft lives), so an empty value here means a
		// WIRING regression, not a configuration choice — and it would silently disable the only cover
		// for the post-rewrite window. Say so rather than skipping quietly.
		a.logger.Error("cluster: no cluster data dir wired — the online force-single recovery intent " +
			"cannot be read, so an interrupted recovery would NOT forward-complete")
	}
	if in, ierr := clusteroffline.ReadOnlineIntent(a.dataDir); ierr != nil {
		a.logger.Error("cluster: cannot read the online force-single intent — if a recovery was "+
			"interrupted, its marker/epoch/prune may be incomplete and `cluster status` may not show "+
			"FORCE_SINGLE. Inspect it by hand before running destructive commands.", "err", ierr)
	} else if in != nil {
		if in.SelfID != selfID {
			a.logger.Error("cluster: online force-single intent belongs to a different broker; "+
				"refusing to apply or delete it", "intent_self", in.SelfID, "this_broker", selfID)
			return
		}
		if verr := clusteroffline.ValidateOnlineIntent(*in); verr != nil {
			a.logger.Error("cluster: online force-single intent is invalid; refusing to apply or "+
				"delete it", "err", verr)
			return
		}
		rewritten, rerr := a.forceSingleRaftRewriteLanded()
		if rerr != nil {
			a.logger.Error("cluster: cannot determine whether the online force-single raft rewrite "+
				"landed; retaining the intent and refusing to mutate replicated state", "err", rerr)
			return
		}
		if !rewritten {
			// The process died after fsyncing the pre-mutation intent but before RecoverCluster changed
			// disk. Presence is not proof of mutation. The old committed configuration is positive
			// evidence that nothing needs forward-completion.
			if err := clusteroffline.ClearOnlineIntent(a.dataDir); err != nil {
				a.logger.Warn("cluster: pre-rewrite force-single intent could not be cleared", "err", err)
			} else {
				a.logger.Warn("cluster: discarded a pre-rewrite force-single intent after confirming " +
					"the committed raft configuration was not changed to {self}")
			}
			return
		}
		if err := a.applyForceSingleIntent(*in); err != nil {
			// Leave the intent in place: the next LEADER TICK retries it (driveInterruptedForceSingle —
			// this function alone runs on the leadership EDGE, and the node this branch runs on is a
			// single-voter raft that never holds another election). Loud, because until this lands the
			// destructive gate cannot see the emergency.
			a.logger.Error("cluster: an interrupted online force-single is not yet fully recorded "+
				"(force_single_active / recovery epoch); retrying on the next leader tick. Do NOT "+
				"run destructive commands until `cluster status` reports FORCE_SINGLE.", "err", err)
			return
		}
		a.logger.Warn("cluster: completed the replicated state of an interrupted online force-single "+
			"from its on-disk intent", "abandoned", in.Abandoned)
		// The marker and epoch are confirmed landed. The ghosts are the remaining work; once they are
		// gone too, the intent has nothing left to protect and is cleared.
		if a.startFinalizeForGhosts(selfID, in.Abandoned) {
			return // work remains; keep the intent whether or not an op could be started this tick
		}
		if err := clusteroffline.ClearOnlineIntent(a.dataDir); err != nil {
			a.logger.Warn("cluster: could not clear the completed force-single intent", "err", err)
		}
		return
	}

	// PATH 2 — SUBSTRATE-DERIVED, for a recovery that predates the intent (an upgrade across this
	// change) or whose intent file was lost. Narrower by necessity: with no intent to prove a recovery
	// happened, the marker is the only evidence available, so all four conditions must hold.
	if !forceSingleActive(a.node.RODB()) {
		return
	}
	voters, err := a.node.NumVoters()
	if err != nil || voters != 1 {
		return
	}
	if live, lerr := cluster.ActiveOperationForTarget(a.node.RODB(), selfID); lerr != nil || live != nil {
		return
	}
	ghosts, gerr := a.forceSingleGhostRows()
	if gerr != nil || len(ghosts) == 0 {
		return
	}
	a.startFinalizeForGhosts(selfID, ghosts)
}

// startFinalizeForGhosts starts the prune-retry op when ghost rows from `abandoned` are still
// present. It returns true whenever work may remain, including read/start errors and the terminal
// backoff window. The intent caller may clear its durable evidence only on a confirmed false.
func (a *ClusterAdmin) startFinalizeForGhosts(selfID string, abandoned []string) bool {
	if len(abandoned) == 0 {
		return false
	}
	present, perr := a.rosterRowsPresent(abandoned)
	if perr != nil {
		a.logger.Warn("cluster: could not determine whether force-single ghost rows remain; retaining "+
			"the recovery intent", "err", perr)
		return true
	}
	if len(present) == 0 {
		return false
	}
	if live, lerr := cluster.ActiveOperationForTarget(a.node.RODB(), selfID); lerr != nil {
		a.logger.Warn("cluster: could not read the active force-single finalize operation; retaining "+
			"the recovery intent", "err", lerr)
		return true
	} else if live != nil {
		return true
	}
	if a.recentlyGaveUpOnFinalize(selfID) {
		a.logger.Warn("cluster: force-single ghost roster rows are still present, but a finalize "+
			"operation gave up recently — not starting another yet. Clean them up with "+
			"`tether cluster recovery node remove <id> --manual`, or wait for the backoff.",
			"ghosts", present)
		return true
	}
	opID, serr := a.startForceSingleFinalize(selfID, present)
	if serr != nil {
		a.logger.Warn("cluster: found force-single ghost roster rows but could not start a finalize op; "+
			"clean them up with `tether cluster recovery node remove <id> --manual`",
			"ghosts", present, "err", serr)
		return true
	}
	a.logger.Warn("cluster: force-single ghost roster rows remain — started a finalize operation to prune them",
		"op", opID, "ghosts", present)
	return true
}

// driveInterruptedForceSingle is the PER-TICK half of the crash-window cover, and it is what makes
// "this broker will finish it automatically" a true statement rather than a hopeful one.
//
// origin: batch-c external re-review audit (main process). resumeForceSingleFinalizeOnLeadership runs
// on the leadership-ACQUIRED EDGE only — observability.go dispatches it under `isLeader && !wasLeader`.
// Every branch that leaves the intent on disk told the operator it would be retried "on the next
// leadership tick", but the node those branches run on is a freshly recovered SINGLE-VOTER raft, and a
// single-voter raft never holds another election. On the one shape that matters there was no next
// edge, so the promise silently resolved to "restart the broker" — during an emergency, on the survivor
// of a disaster, with the destructive gate open because the marker had not landed.
//
// The re-review's TryLock is what makes this safe to run every tick: the commit handler OWNS a freshly
// written intent until its synchronous prune finishes, so the tick can never race the happy path into
// creating a retry op for ghosts the handler is about to prune itself.
//
// It is gated on the replicated FACTS still being missing, not merely on an intent existing. An intent
// legitimately outlives its marker and epoch while a finalize op prunes the ghosts; re-entering the
// full resume every 5s in that state would re-log the ghost/backoff warnings forever. Once there is
// nothing to finish this costs one ENOENT open per leader tick.
func (a *ClusterAdmin) driveInterruptedForceSingle() {
	if a == nil || a.node == nil || a.dataDir == "" {
		return
	}
	// Silent pre-gate. Reporting an unreadable or foreign intent LOUDLY belongs to the leadership edge,
	// which fires once per election; a per-tick copy of those Error lines would be pure repetition.
	in, err := clusteroffline.ReadOnlineIntent(a.dataDir)
	if err != nil || in == nil || in.SelfID != a.node.SelfID() {
		return
	}
	if a.forceSingleFactsLanded(*in) {
		return
	}
	a.resumeForceSingleFinalizeOnLeadership()
}

// forceSingleFactsLanded reports whether both replicated facts an intent promises — the
// force_single_active marker and THAT intent's epoch — are already visible. The epoch is compared by
// VALUE for the same reason applyForceSingleIntent writes it that way: a node force-singled a second
// time carries the previous incident's token, and treating "non-empty" as landed would leave the
// per-tick retry believing a stale epoch finished the job.
func (a *ClusterAdmin) forceSingleFactsLanded(in clusteroffline.OnlineIntent) bool {
	if !forceSingleActive(a.node.RODB()) {
		return false
	}
	cur, err := a.forceSingleEpoch()
	return err == nil && cur == in.Epoch
}

// ─── ONLINE force-single intent completion (EXTERNAL review B1) ──────────────────────────────────

// applyForceSingleIntent lands the two replicated facts the recovery promised: the
// force_single_active marker and the recovery epoch. It is IDEMPOTENT and re-derivable, because it
// takes every value from the on-disk intent rather than minting anything.
//
// Both writes are verified by READING THEM BACK, not by the propose returning nil. A propose can
// return nil on a poison-skip or a RowsAffected==0 no-op, and "the marker is set" is exactly the fact
// the destructive gate and the status verdict depend on — believing it without checking is how the
// original window stayed invisible.
func (a *ClusterAdmin) applyForceSingleIntent(in clusteroffline.OnlineIntent) error {
	if err := clusteroffline.ValidateOnlineIntent(in); err != nil {
		return err
	}
	if in.SelfID != a.node.SelfID() {
		return fmt.Errorf("online force-single intent belongs to %q, not this broker %q",
			in.SelfID, a.node.SelfID())
	}
	markedAt, ok := parseOpTimestamp(in.MarkedAt)
	if !ok {
		return fmt.Errorf("online force-single intent has invalid marked_at %q", in.MarkedAt)
	}
	if !forceSingleActive(a.node.RODB()) {
		if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanSetForceSingle(markedAt)
		}); err != nil {
			return fmt.Errorf("persist force_single_active: %w", err)
		}
		if !forceSingleActive(a.node.RODB()) {
			return fmt.Errorf("force_single_active did not become visible after a successful propose")
		}
	}
	cur, err := a.forceSingleEpoch()
	if err != nil {
		return fmt.Errorf("read the recovery epoch: %w", err)
	}
	if cur == in.Epoch {
		return nil
	}
	// The epoch is compared by VALUE, not by presence: a cluster that grew back and was force-singled a
	// SECOND time carries a stale epoch from the first incident, and treating "non-empty" as done would
	// leave the split-brain detector holding a token from the wrong incident forever.
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanForceSingleEpoch(in.Epoch)
	}); err != nil {
		return fmt.Errorf("persist the recovery epoch: %w", err)
	}
	if got, gerr := a.forceSingleEpoch(); gerr != nil || got != in.Epoch {
		return fmt.Errorf("the recovery epoch did not become visible after a successful propose (got %q, want %q, err %v)",
			got, in.Epoch, gerr)
	}
	return nil
}

// forceSingleRaftRewriteLanded distinguishes the two crash edges around the pre-mutation intent.
// RecoverCluster replaces the committed configuration atomically: exact one-voter {self} means the
// rewrite landed; any other readable configuration means the process stopped before that mutation.
func (a *ClusterAdmin) forceSingleRaftRewriteLanded() (bool, error) {
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return false, err
	}
	return len(cfg) == 1 && cfg[0].NodeID == a.node.SelfID() && cfg[0].Voter, nil
}

// forceSingleEpoch reads the persisted recovery epoch ("" when absent).
func (a *ClusterAdmin) forceSingleEpoch() (string, error) {
	var v string
	err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		row := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, cluster.MetaKeyForceSingleEpoch)
		if serr := row.Scan(&v); serr != nil {
			if errors.Is(serr, sql.ErrNoRows) {
				v = ""
				return nil
			}
			return serr
		}
		return nil
	})
	return v, err
}
