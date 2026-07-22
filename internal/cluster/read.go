package cluster

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// TFence is the unified self-observation fail-closed window (§3.2 / §6.2 / §8.4):
// a node that has not had timely successful leader contact within TFence stops
// authorizing already-provisioned auth_callout reads. Pinned at 10s = k_fence(10)
// × the multinode ElectionTimeout (1000ms) — see TestFenceExceedsWorstCaseElection.
// It is ONE monotonic-clock predicate with NO raft.VerifyLeader round-trip, so a
// quorum-lost node keeps serving local reads instead of bricking (§6.2).
const TFence = 10 * time.Second

// LeaderContactStale reports whether THIS node should fail closed — stop
// authorizing already-provisioned auth_callout reads — because it has lost timely
// contact with the leader (§3.2 / §6.2). It is the single shared predicate and
// NEVER calls raft.VerifyLeader (a quorum-lost node must keep serving local reads,
// not block). now is injected for tests; production passes time.Now() (monotonic,
// NTP-step immune).
//
// Asymmetry (raft v1.7.3 — see leaderContactStaleAt):
//   - Leader => fresh. raft's leader-lease loop demotes a leader to Follower the
//     instant it cannot reach a quorum within LeaderLeaseTimeout
//     (checkLeaderLease), so State()==Leader IS proof of recent quorum contact.
//     Stateless: no timestamp, no LeaderCh goroutine (LeaderCh fires only on
//     leadership transitions, so a clock derived from it would false-fence a
//     healthy long-lived leader).
//   - Follower/Candidate => stale iff now - LastContact() > TFence. LastContact()
//     is refreshed on a successful AppendEntries (and on a granted vote /
//     InstallSnapshot); its zero value (never heard a leader) is far past TFence
//     => stale (fail-closed).
//
// Bounded residual fail-open: a partitioned ex-leader resets its follower clock at
// step-down (raft setLastContact), so the worst-case authorize-while-isolated
// window is LeaderLeaseTimeout + TFence — bounded and accepted (§8.4(b)).
func (n *Node) LeaderContactStale(now time.Time) bool {
	return leaderContactStaleAt(n.raft.Load().State(), n.raft.Load().LastContact(), now)
}

// leaderContactStaleAt is the pure decision behind LeaderContactStale, split out so
// the boundary + vacuity controls are deterministic without a live raft.
func leaderContactStaleAt(state raft.RaftState, lastContact, now time.Time) bool {
	if state == raft.Leader {
		return false // stateless: leader-lease guarantees recent quorum contact
	}
	return now.Sub(lastContact) > TFence
}

// AppliedIndex returns the FSM's durable apply cursor (architecture §3.7: the DB
// is the authority). This is a bounded-stale read off the local DB.
func (n *Node) AppliedIndex() (uint64, error) {
	return readAppliedIndexDB(n.db)
}

// VerifyLeaderRead runs fn ONLY after raft confirms this node is STILL the leader
// at read time (architecture §3.2): a VerifyLeader/ReadIndex barrier for
// correctness-sensitive reads (force-single peer health, revocation). At N=1 it
// trivially succeeds; the seam exists so D2+ consumers (auth_callout, catch-up
// barrier, force-single) attach to the correct path. D1 wires NO real consumer.
func (n *Node) VerifyLeaderRead(fn func(*sql.DB) error) error {
	if err := n.raft.Load().VerifyLeader().Error(); err != nil {
		return fmt.Errorf("cluster: verify leader: %w", err)
	}
	return fn(n.db)
}

// BoundedStaleRead runs fn against the local DB WITHOUT a leader barrier
// (architecture §3.2): explicitly bounded-stale, for ps/history/status that may
// reflect a not-yet-stepped-down leader. D1 names the seam; no real consumer yet.
func (n *Node) BoundedStaleRead(fn func(*sql.DB) error) error {
	return fn(n.db)
}

// TrailingLogs mirrors raft.DefaultConfig().TrailingLogs (10240) — the entries raft
// retains behind a snapshot. raftConfig does NOT override it, so it is the authoritative
// bound for the §6.3 R-7 audit-publish lag invariant: the D5 publisher must keep
// CommitIndex - audit_published_index < TrailingLogs, else a snapshot can truncate
// unpublished audit (a bounded loud accepted-loss, §16/§17). TestD5TrailingLogsMatchesRaft
// pins it to the raft default so it cannot drift.
const TrailingLogs uint64 = 10240

// ---- D5 audit-publisher read primitives (§6.3 / d5-plan §1). All raft-free of
// NATS: the publisher loop that consumes them lives in internal/broker (L-2). ----

// CommitIndex is the publish CEILING for the D5 audit publisher (§6.3 D5 / R-4): the
// highest raft index known committed. raft v1.7.3 exports it (api.go:1234). A
// committed entry is durable on a majority and present in THIS node's local log, so
// CommittedCommandAt(idx) for idx<=CommitIndex succeeds unless snapshot-truncated
// (R-6). Preferred over AppliedIndex as the ceiling: a just-elected leader's applied
// cursor lags CommitIndex during FSM catch-up, which would needlessly delay the
// post-election sweep — and the audit derives purely from the committed log bytes, not
// a locally-applied SQL row, so AppliedIndex is not needed for durability.
func (n *Node) CommitIndex() uint64 { return n.raft.Load().CommitIndex() }

// LogFirstIndex / LogLastIndex expose the retained raft-log range (the BoltStore log
// is truncated below FirstIndex after a snapshot). The publisher clamps its sweep
// FLOOR to max(checkpoint, LogFirstIndex) so a snapshot-truncated unpublished entry
// is a bounded loud accepted-loss, never a panic or a silent skip (§6.3 D5 / R-6).
func (n *Node) LogFirstIndex() (uint64, error) { return n.store.FirstIndex() }
func (n *Node) LogLastIndex() (uint64, error)  { return n.store.LastIndex() }

// RaftAppliedIndex is raft's OWN applied cursor — it advances on EVERY committed entry, including the
// leader-election LogNoop and config entries, which the SQLite command cursor (AppliedIndex) does NOT.
// A caught-up steady-state leader has RaftAppliedIndex == CommitIndex. Comparing the SQLite AppliedIndex
// against CommitIndex would be CROSS-DOMAIN and structurally never true (the trailing noop bumps
// CommitIndex but never the SQLite cursor), which is why reaperMayDelete uses this raft-domain index
// instead (v0.4.4 review G-reaper-gate). Stays in internal/cluster so internal/broker keeps L-2 raft-free.
func (n *Node) RaftAppliedIndex() uint64 { return n.raft.Load().AppliedIndex() }

// CaughtUp reports whether this node's raft-domain view is current enough to make a
// destructive decision (orphan/lock reap) from local state. It is the SSOT the broker's
// reaperMayDelete/reaperCaughtUp both call, so the tested predicate and the shipped one
// can never drift (v0.4.4 review G-reaper-gate + external re-review lock-reap-F2).
//
// Two conjuncts:
//   - commit > 0: this node has synced with a leader at least once. Without it,
//     applied >= commit is VACUOUSLY true in two real windows — a never-elected node
//     (both cursors 0) AND a snapshot-carrying RESTART (restoreSnapshot sets applied to
//     the snapshot index while the volatile CommitIndex is still 0 until the first
//     post-restart commit is learned, so applied=S >= commit=0 passes on the common
//     production restart path). The bound suppresses only that pre-first-commit instant.
//   - RaftAppliedIndex() >= commit: raft's own cursor has reached the commit ceiling.
//
// HONEST LIMIT #1 (dispatch vs apply): RaftAppliedIndex advances when a committed batch
// is ENQUEUED to the FSM (fsmMutateCh), NOT when fsm.Apply returns — so a committed entry
// that is dispatched-but-not-yet-applied to SQLite still reads caught-up. This closes the
// LARGE-replay case (a just-elected leader replaying a long tail backpressures the dispatch
// channel, so applied < commit) but NOT a single committed-but-unapplied entry in the µs–ms
// apply window. That narrower residual is RECORDED, not closed: the membership-lock reaps
// document it (reconcile_upgrade_lock.go — a raft Barrier was rejected because Barrier().Error()
// has no apply deadline and would hang the reconcile goroutine on a wedged FSM); the xfer/orphan
// reaps tolerate it (home partition + ModTime grace + JS-delete-fails-without-quorum).
//
// HONEST LIMIT #2 (islanded follower): an islanded node's CommitIndex FROZE at a positive
// value and its applied catches that frozen ceiling, so CaughtUp() still returns true on a
// stale view. That case is fenced by the WRITE path, not this predicate: a Propose from a
// quorum-less node cannot commit, and a JS delete fails once the colocated JS meta loses
// quorum. See TestCaughtUpIslandedFollowerFrozenCommitResidual (an executable honesty pin).
//
// Read commit first, then applied: applied advancing between the two reads only yields a
// correct true; the reverse can only yield a fail-closed false. Raft-free of NATS; stays
// in internal/cluster so internal/broker keeps L-2 raft-free.
func (n *Node) CaughtUp() bool {
	commit := n.CommitIndex()
	return commit > 0 && n.RaftAppliedIndex() >= commit
}

// CommittedCommandAt decodes the *Command at a committed raft index, reading the local
// raft log via the SAME unexported decodeCommand the FSM uses (identical poison /
// version verdicts). Pure read — no apply, no DB mutation. Typed sentinels let the
// publisher classify each entry (§6.3 D5 / R-6/R-9):
//
//	ErrLogTruncated   idx < FirstIndex (or not found) — snapshotted away; loud-loss branch
//	ErrLogNonCommand  a raft configuration / noop entry — carries no op, skip
//	ErrLogPoison      decode failed — the FSM advanced applied_index past it as a no-op, skip
func (n *Node) CommittedCommandAt(idx uint64) (*Command, error) {
	first, err := n.store.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("cluster: log first index: %w", err)
	}
	if first == 0 || idx < first {
		return nil, ErrLogTruncated
	}
	var l raft.Log
	if err := n.store.GetLog(idx, &l); err != nil {
		if errors.Is(err, raft.ErrLogNotFound) {
			return nil, ErrLogTruncated
		}
		return nil, fmt.Errorf("cluster: get log %d: %w", idx, err)
	}
	if l.Type != raft.LogCommand {
		return nil, ErrLogNonCommand
	}
	cmd, derr := decodeCommand(l.Data)
	if derr != nil {
		return nil, ErrLogPoison
	}
	return cmd, nil
}

// AuditPublishedIndex reads the REPLICATED audit-publish cursor (§6.3 D5). Missing row
// reads as 0 (mirrors AppliedIndex). Bounded-stale read off the local DB; the only
// writer is the leader-only publisher via AdvanceAuditPublished, so a leader reads its
// own committed advances and a new leader reads the replicated floor.
func (n *Node) AuditPublishedIndex() (uint64, error) {
	return readMetaUintDB(n.db, metaKeyAuditPublishedIndex)
}

// AdvanceAuditPublished proposes a monotonic advance of the audit-publish cursor to
// `to` through raft (OpAuditCheckpointSet via Propose — leader-gated; the FSM-baked
// WHERE guard makes a regress a deterministic no-op on every replica, R-3). BATCHED
// caller contract (R-5): it SKIPS the raft write entirely when `to` does not advance
// the current cursor, so an idle publisher loop issues ZERO raft writes. A
// leadership loss racing the read surfaces as raft.ErrNotLeader from Propose
// (cluster.IsNotLeader) — the caller stops publishing this tick.
func (n *Node) AdvanceAuditPublished(to uint64) error {
	cur, err := n.AuditPublishedIndex()
	if err != nil {
		return err
	}
	if to <= cur {
		return nil // R-5: no advance => no raft write
	}
	return n.Propose(func(*sql.DB) (*Command, error) {
		return planAuditCheckpointSet(to)
	})
}

// NumVoters counts Suffrage==Voter servers in the committed raft Configuration (§6.4
// D5 / R-16: target = jsstream.ReplicasFor(NumVoters)). Leader-meaningful — a
// follower's configuration may be stale, but the reconfig pass is leader-only. Raft
// passthrough; no NATS.
func (n *Node) NumVoters() (int, error) {
	fut := n.raft.Load().GetConfiguration()
	if err := fut.Error(); err != nil {
		return 0, fmt.Errorf("cluster: get configuration: %w", err)
	}
	count := 0
	for _, s := range fut.Configuration().Servers {
		if s.Suffrage == raft.Voter {
			count++
		}
	}
	return count, nil
}
