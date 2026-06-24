// D5 (§6.3 / §6.4) — the leader-only, post-commit audit publisher + JS stream replica
// reconfigurer. ONE merged long-lived goroutine (R-13/R-21) constructed ONLY by the
// test/d5 harness; production (build-and-prove, cutover=D9) never builds a cluster.Node
// nor starts this loop, and the live publishAudit/reconcileOnRegister stay byte-unchanged
// (guard test TestD5ProductionWiresNoClusterNode). It reads committed entries from the
// REPLICATED raft log via the raft-free cluster.Node primitives (L-2: this file imports
// nats+cluster, internal/cluster imports neither nats nor this package), so any NEW
// leader re-derives committed-but-unpublished audit without the dead leader's memory.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/nats-io/nats.go/jetstream"
)

// publishTimeout bounds a single js.Publish; it is a CHILD of the loop ctx (R-14) so a
// leadership loss / loop cancel aborts an in-flight PUBLISH immediately and releases its
// reply-inbox fd (else the fd leak gate fires). 5s = the d4 ApplyTimeout, generous under
// -race + a clustered JetStream. NOTE (internal review m6): a tick's checkpoint
// AdvanceAuditPublished -> raft.Apply is NOT ctx-aware (raft.Apply ignores ctx, uses its
// own applyTimeout), so loop EXIT on cancel is bounded by that Apply, not publishTimeout —
// the IsLeader() gate short-circuits before the Apply once quorum is lost, so this is a
// bounded tail, not a hang.
const publishTimeout = 5 * time.Second

// ClusterReader is the narrow raft-free view of *cluster.Node the publisher consumes
// (§6.3/§6.4 D5). *cluster.Node satisfies it; tests may substitute a fake. Keeping it an
// interface decouples the broker loop from the raft node and makes the unit tests cheap.
type ClusterReader interface {
	IsLeader() bool
	LeaderContactStale(now time.Time) bool
	CommitIndex() uint64
	LogFirstIndex() (uint64, error)
	CommittedCommandAt(idx uint64) (*cluster.Command, error)
	AuditPublishedIndex() (uint64, error)
	AdvanceAuditPublished(to uint64) error
	NumVoters() (int, error)
}

// AuditPublisherConfig wires the publisher. Node + JS are required; the rest default.
type AuditPublisherConfig struct {
	Node   ClusterReader
	JS     jetstream.JetStream
	Now    func() time.Time
	Logger *slog.Logger
	Batch  int           // checkpoint batch size (R-5); default 256
	MaxPar int           // reconcile fan-out cap (R-21); default 8
	Poll   time.Duration // steady-state tick; default 100ms
	MaxLag uint64        // §6.3 R-7 lag bound; default cluster.TrailingLogs (tests may shrink)

	// ListSIDs returns the active session ids (leader-local committed DB read) whose
	// history-<sid> / OBJ_xfer-<sid> streams the reconcile pass reconfigures.
	ListSIDs func(ctx context.Context) ([]string, error)
	// XferState ensures+raises the OBJ_xfer-<sid> object store toward target and reports
	// its replica state (broker.XferReplicaState). nil disables OBJ_xfer reconcile.
	XferState func(ctx context.Context, sid string, target int) (jsstream.StreamReplicaState, error)
	// OnPublish is a test tap fired AFTER a record's js.Publish ACKs (non-vacuity hooks).
	OnPublish func(subject, msgID string)
	// SessionExists reports whether a session row still exists in the committed DB (audit M5).
	// publishAudit consults it to disambiguate a "no stream" publish error: a GONE session means
	// its history-<sid> stream was permanently deleted by session-rm (bounded loss → advance),
	// while a still-existing session means the stream is merely not-yet-ensured (transient → R-22
	// retry). nil ⇒ conservative "exists" (always retry; preserves pre-M5 behavior in tests).
	SessionExists func(sid string) (bool, error)
}

// AuditPublisher is the merged leader-only loop (Mechanism A publish + Mechanism B
// reconfig). Exported so test/d5 (external package) can drive it; the guard test forbids
// the production-wiring files from referencing it.
type AuditPublisher struct {
	cfg          AuditPublisherConfig
	lossCnt      atomic.Uint64 // truncation-loss counter (R-6 non-vacuity, E-A6)
	lagCnt       atomic.Uint64 // §6.3 R-7: times the publish lag breached MaxLag (truncation-risk)
	delStreamCnt atomic.Uint64 // audit M5: records dropped because the destination history-<sid> stream was deleted (racing session-rm)
}

// NewAuditPublisher validates + defaults the config.
func NewAuditPublisher(cfg AuditPublisherConfig) *AuditPublisher {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 256
	}
	if cfg.MaxPar <= 0 {
		cfg.MaxPar = 8
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 100 * time.Millisecond
	}
	if cfg.MaxLag == 0 {
		cfg.MaxLag = cluster.TrailingLogs // §6.3 R-7: the raft snapshot-retention bound
	}
	return &AuditPublisher{cfg: cfg}
}

// Run is the ONE long-lived goroutine (R-13), tied to ctx (R-14). Each tick: gate on
// leader+fence, then publish drain, then reconcile pass (R-21 happens-before). It polls
// IsLeader() rather than subscribing to LeaderCh (house style, read.go:30) so the
// goroutine count is constant across N leadership flaps — trivially passing the leak gate.
func (p *AuditPublisher) Run(ctx context.Context) {
	t := time.NewTicker(p.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *AuditPublisher) tick(ctx context.Context) {
	// R-13/R-15: a follower / a fenced (quorum-lost) ex-leader within its lease does
	// nothing. The CommitIndex ceiling is the load-bearing safety; the fence is hardening.
	if !p.cfg.Node.IsLeader() || p.cfg.Node.LeaderContactStale(p.cfg.Now()) {
		return
	}
	if _, err := p.PublishOnce(ctx); err != nil {
		p.cfg.Logger.Warn("d5: audit publish", "err", err)
	}
	if _, err := p.ReconcileOnce(ctx); err != nil && !errors.Is(err, jsstream.ErrMetaGroupNotReady) {
		p.cfg.Logger.Warn("d5: replica reconcile", "err", err)
	}
}

// TruncationLossCount reports how many committed log indices the publisher had to skip
// because a snapshot truncated them before they were published (§6.3 R-6 accepted-loss).
// Tests assert it fired (E-A6) or stayed 0 (steady state).
func (p *AuditPublisher) TruncationLossCount() uint64 { return p.lossCnt.Load() }

// PublishOnce drains the re-derivable audit in (checkpoint, CommitIndex] (R-4 ceiling,
// R-6 floor) and advances the REPLICATED checkpoint in batches (R-5). It is the steady
// state AND the post-election sweep (the sweep is just a large gap). Returns the highest
// fully-handled index. Idempotent + re-runnable: a re-run re-publishes with identical
// dedup ids that JetStream collapses (R-8/R-10/R-12).
func (p *AuditPublisher) PublishOnce(ctx context.Context) (uint64, error) {
	cur, err := p.cfg.Node.AuditPublishedIndex()
	if err != nil {
		return 0, err
	}
	hi := p.cfg.Node.CommitIndex()
	if hi <= cur {
		return cur, nil
	}

	// §6.3 R-7 lag bound: the publisher must stay within MaxLag (= cluster.TrailingLogs) of
	// the commit index, else a snapshot may truncate unpublished audit before this drain
	// reaches it (a bounded loud accepted-loss, handled below). A breach is an observability
	// signal — count + loud log; it does not change the drain.
	if hi-cur >= p.cfg.MaxLag {
		p.lagCnt.Add(1)
		p.cfg.Logger.Warn("d5: audit-publish lag exceeds TrailingLogs bound (truncation-loss risk, §6.3/§17)",
			"commit", hi, "published", cur, "lag", hi-cur, "bound", p.cfg.MaxLag)
	}

	advanced := cur // highest index fully published-or-skipped
	lastCkpt := cur // highest index already checkpointed via raft
	flush := func(to uint64) error {
		if to <= lastCkpt {
			return nil
		}
		if err := p.cfg.Node.AdvanceAuditPublished(to); err != nil {
			return err
		}
		lastCkpt = to
		return nil
	}

	for idx := cur + 1; idx <= hi; idx++ {
		// Re-clamp the floor each iteration: a snapshot may fire mid-loop (R-6).
		first, ferr := p.cfg.Node.LogFirstIndex()
		if ferr != nil {
			_ = flush(advanced)
			return advanced, ferr
		}
		if first > idx {
			// [idx, lossTo] truncated before we published them: bounded loud accepted-loss
			// (R-6). Advance PAST the gap so the loop never wedges. CLAMP the gap to the
			// captured commit ceiling hi (internal review m1): a snapshot firing mid-loop can
			// push first beyond hi, and advancing/loss-counting past the ceiling would violate
			// R-4 and corrupt the §16/§17 loss metric.
			lossTo := first - 1
			if lossTo > hi {
				lossTo = hi
			}
			p.recordTruncationLoss(idx, lossTo)
			advanced = lossTo
			if lossTo >= hi {
				break // nothing left at or below the ceiling
			}
			idx = lossTo // loop's idx++ steps to lossTo+1
			continue
		}

		cmd, cerr := p.cfg.Node.CommittedCommandAt(idx)
		switch {
		case errors.Is(cerr, cluster.ErrLogTruncated):
			p.recordTruncationLoss(idx, idx)
			advanced = idx
			continue
		case errors.Is(cerr, cluster.ErrLogNonCommand), errors.Is(cerr, cluster.ErrLogPoison):
			advanced = idx // R-9: no replayable audit; skip + advance cursor
			continue
		case cerr != nil:
			_ = flush(advanced)
			return advanced, cerr
		}

		if cmd.Op == cluster.OpAuditCheckpointSet {
			// EXTERNAL-REVIEW F1 (R-5/R-9 self-begetting fix): the cursor's OWN entries must
			// NEVER advance the cursor past themselves. A checkpoint advance to N is itself a
			// committed OpAuditCheckpointSet at N+1; if the next tick advanced the cursor past
			// N+1 it would flush a new checkpoint at N+2, and an idle leader would write a fresh
			// raft entry every tick FOREVER (growing the log, accelerating snapshots, breaking
			// the idle-zero-writes guarantee). So SKIP without advancing — a later real (source)
			// entry pulls the cursor forward over this one implicitly, leaving at most one
			// trailing checkpoint that costs a cheap re-read but no raft write.
			continue
		}
		if cmd.Op == cluster.OpReconcileBatch { // R-9: ONLY reconcile is re-derivable
			if perr := p.publishReconcile(ctx, idx, cmd); perr != nil {
				// R-22: do NOT advance past an un-ACKed publish (queue, retry next tick).
				_ = flush(advanced)
				return advanced, perr
			}
		}
		if cmd.Op == cluster.OpTransferAudit { // D8a §9: re-derivable transfer audit
			if perr := p.publishTransferAudit(ctx, idx, cmd); perr != nil {
				_ = flush(advanced) // R-22: queue + retry next tick, do not advance past
				return advanced, perr
			}
		}
		advanced = idx
		if advanced-lastCkpt >= uint64(p.cfg.Batch) { // R-5: batched checkpoint
			if err := flush(advanced); err != nil {
				return advanced, err
			}
		}
	}
	if err := flush(advanced); err != nil {
		return advanced, err
	}
	return advanced, nil
}

// LagExceededCount reports how many times the publish lag breached the §6.3 R-7
// TrailingLogs bound (a truncation-loss risk). Tests assert it fires (non-vacuity).
func (p *AuditPublisher) LagExceededCount() uint64 { return p.lagCnt.Load() }

// reconcileSID returns the single session id all records of one ReconcileBatch share
// (aux.SID), or "" when the entry replayed no records.
func reconcileSID(procs []schema.AuditProc, ports []schema.AuditPort) string {
	if len(procs) > 0 {
		return procs[0].Session
	}
	if len(ports) > 0 {
		return ports[0].Session
	}
	return ""
}

func (p *AuditPublisher) recordTruncationLoss(from, to uint64) {
	if to < from {
		return
	}
	p.lossCnt.Add(to - from + 1)
	p.cfg.Logger.Warn("d5: audit lost to snapshot truncation (accepted bounded loss, §6.3/§17)",
		"from", from, "to", to)
}

// publishReconcile replays the committed ReconcileBatch's audit (PURE, from the entry's
// Aux only) and publishes each record with its dedup id. seq = position in the
// total-ordered slices ReplayReconcileAudit returns, so two leaders replaying the SAME
// committed bytes stamp IDENTICAL ids (R-8). A record's publish error aborts the entry
// (the caller does not advance the cursor past it — R-22).
func (p *AuditPublisher) publishReconcile(ctx context.Context, idx uint64, cmd *cluster.Command) error {
	procs, ports, err := proc.ReplayReconcileAudit(cmd)
	if err != nil {
		return err
	}
	// Defense-in-depth (internal review m5): the sid is baked into the committed entry from
	// the agent register via the D4 forward path, which does not re-ValidateSID. A malformed
	// sid would inject into the JS subject / dedup id. All records of one ReconcileBatch share
	// one aux.SID, so validate once; on a bad sid loud-skip the WHOLE entry (advance past it
	// without publishing to a poisoned subject) rather than error/wedge.
	if sid := reconcileSID(procs, ports); sid != "" {
		if verr := proto.ValidateSID(sid); verr != nil {
			p.cfg.Logger.Warn("d5: skipping reconcile audit with invalid sid (subject-injection guard, m5)",
				"sid", sid, "raft_index", idx, "err", verr)
			return nil
		}
	}
	for seq, rec := range procs {
		payload, merr := json.Marshal(rec)
		if merr != nil {
			return merr
		}
		if perr := p.publishAudit(ctx, proto.SubjAuditProc(rec.Session), payload, auditMsgID(idx, cmd.ReqID, "proc", seq), idx, rec.Session); perr != nil {
			return perr
		}
	}
	for seq, rec := range ports {
		payload, merr := json.Marshal(rec)
		if merr != nil {
			return merr
		}
		if perr := p.publishAudit(ctx, proto.SubjAuditPort(rec.Session), payload, auditMsgID(idx, cmd.ReqID, "port", seq), idx, rec.Session); perr != nil {
			return perr
		}
	}
	return nil
}

// publishTransferAudit replays the committed OpTransferAudit's single record (PURE, from
// cmd.Aux only — no live request, no DB) and publishes it to history-<sid>. dedup id
// q<reqID>:xfer:0 — the derived reqID (hex(sha256(transfer_id:kind))) is cross-retry
// stable, so a post-election sweep or follower→leader forwarder retry re-derives the SAME
// id and JetStream collapses the duplicate (the 0011 ledger collapses it at the FSM layer
// too — two independent anchors, neither window-bound). An empty Aux publishes nothing
// (advance past). A publish error aborts the entry (caller does not advance, R-22).
func (p *AuditPublisher) publishTransferAudit(ctx context.Context, idx uint64, cmd *cluster.Command) error {
	rec, ok, err := xferaudit.ReplayTransferAudit(cmd)
	if err != nil {
		return err
	}
	if !ok {
		return nil // empty Aux: nothing to publish
	}
	// Subject-injection guard (cf. publishReconcile m5): rec.Session is baked into the JS
	// subject; loud-skip a malformed sid rather than publish to a poisoned subject.
	if verr := proto.ValidateSID(rec.Session); verr != nil {
		p.cfg.Logger.Warn("d8: skipping transfer audit with invalid sid (subject-injection guard)",
			"sid", rec.Session, "raft_index", idx, "err", verr)
		return nil
	}
	payload, merr := json.Marshal(rec)
	if merr != nil {
		return merr
	}
	return p.publishAudit(ctx, proto.SubjAuditTransfer(rec.Session), payload, auditMsgID(idx, cmd.ReqID, "xfer", 0), idx, rec.Session)
}

func (p *AuditPublisher) publish(ctx context.Context, subject string, payload []byte, msgID string) error {
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout) // R-14: child of loop ctx
	defer cancel()
	if _, err := p.cfg.JS.Publish(pubCtx, subject, payload, jetstream.WithMsgID(msgID)); err != nil {
		return fmt.Errorf("d5: publish %s id=%s: %w", subject, msgID, err)
	}
	if p.cfg.OnPublish != nil {
		p.cfg.OnPublish(subject, msgID)
	}
	return nil
}

// publishAudit publishes one audit record, classifying a "no stream matches subject" failure
// for a session whose row is GONE as an ACCEPTED BOUNDED LOSS (audit M5 / transfer F2) instead
// of an indefinite retry. A committed audit entry (transfer OR reconcile) can sit BELOW the
// publisher cursor when a racing `session rm` deletes its history-<sid> stream — the
// transfer-audit forward is async + lifecycle-decoupled, and a lagging publisher trails the
// commit. Publishing to the deleted stream returns jetstream.ErrNoStreamResponse; under R-22
// ("never advance past an un-ACKed publish") that index would be retried FOREVER, wedging ALL
// audit publication cluster-wide.
//
// CRITICAL: a no-stream error is AMBIGUOUS — it is also returned when a still-active session's
// history stream is merely NOT-YET-ENSURED (a boot race / a brand-new session), which is
// genuinely TRANSIENT and MUST stay R-22 (the reconcile loop ensures the stream, then the
// queued audit publishes). So bounded-loss applies ONLY when streamDeletedPermanently reports
// the session GONE; otherwise (and with no SessionExists checker wired) the error propagates so
// the loop retries — preserving the queue-not-drop contract. Every other publish error
// (transient JS unavailability / timeout) likewise propagates.
func (p *AuditPublisher) publishAudit(ctx context.Context, subject string, payload []byte, msgID string, idx uint64, sid string) error {
	err := p.publish(ctx, subject, payload, msgID)
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrNoStreamResponse) && p.streamDeletedPermanently(sid) {
		p.delStreamCnt.Add(1)
		p.cfg.Logger.Warn("d5: audit dropped — session + its history stream are gone (accepted bounded loss, §6.3/§17; racing session-rm)",
			"subject", subject, "msg_id", msgID, "raft_index", idx, "sid", sid)
		return nil
	}
	return err
}

// streamDeletedPermanently reports whether a no-stream publish error for sid is a PERMANENT
// loss (the session was rm'd, so history-<sid> is gone for good) vs a TRANSIENT one (the stream
// is merely not-yet-ensured for a still-existing session). Only a wired SessionExists that
// reports the session GONE classifies as permanent; with no checker wired (tests / not wired)
// the conservative answer is "not permanent" → retry, preserving R-22 queue-not-drop.
func (p *AuditPublisher) streamDeletedPermanently(sid string) bool {
	if p.cfg.SessionExists == nil {
		return false
	}
	exists, err := p.cfg.SessionExists(sid)
	return err == nil && !exists
}

// DeletedStreamLossCount reports how many audit records the publisher dropped because their
// session (and thus its history-<sid> stream) had been removed (audit M5). Tests read it to
// assert the bounded-loss branch fired (non-vacuity).
func (p *AuditPublisher) DeletedStreamLossCount() uint64 { return p.delStreamCnt.Load() }

// auditMsgID is the JetStream dedup key (R-8/R-10). For a reqID-bearing reconcile entry
// the id keys on the CROSS-RETRY-STABLE ReqID so a D4 ack-lost retry (which commits at a
// NEW raft index via appliedDedup yet still carries Aux) re-derives the SAME ids and
// JetStream collapses them — keying on the raft index would double-publish. kind ∈
// {proc,port} partitions the two record families so they never collide on (idx,seq).
func auditMsgID(raftIndex uint64, reqID, kind string, seq int) string {
	if reqID != "" {
		return fmt.Sprintf("q%s:%s:%d", reqID, kind, seq)
	}
	return fmt.Sprintf("r%d:%s:%d", raftIndex, kind, seq)
}

// ---- Mechanism B: replica reconfig + AllAtTarget predicate ----

// ReplicaReport is the canonical §6.4 replica-health pass (R-19/R-20): the FULL stream
// set the leader observed this tick. D5 ships only the predicate (D7 consumes AllAtTarget
// as its retire gate; D8b writes the replication_degraded alert row).
type ReplicaReport struct {
	Streams  []jsstream.StreamReplicaState
	Observed bool // false => the pass could not complete a canonical observation
}

// AllAtTarget reports whether EVERY observed stream has reached its target replica count
// (R-20). Fail-closed: an unobserved or empty set => false (a fresh cluster / an errored
// pass must never falsely permit a D7 retire).
func (r ReplicaReport) AllAtTarget() bool {
	if !r.Observed || len(r.Streams) == 0 {
		return false
	}
	for _, s := range r.Streams {
		if !s.Ready {
			return false
		}
	}
	return true
}

// Degraded reports whether the cluster observed at least one under-replicated stream
// (R-19): the signal D8b will later raise as replication_degraded.
func (r ReplicaReport) Degraded() bool {
	return r.Observed && len(r.Streams) > 0 && !r.AllAtTarget()
}

// ReconcileOnce performs ONE canonical replica-reconfig pass (R-17..R-21): target =
// ReplicasFor(NumVoters); raise the events stream first (bootstrap), then every
// history-<sid> and existing OBJ_xfer-<sid>, with a bounded fan-out (MaxPar) joined
// before return. It RAISES (UpdateStream/UpdateObjectStore) and COLLECTS each stream's
// actual/target into a fail-closed canonical ReplicaReport. A hard JS error fails the
// whole pass (Observed=false) so AllAtTarget cannot false-green.
func (p *AuditPublisher) ReconcileOnce(ctx context.Context) (ReplicaReport, error) {
	nv, err := p.cfg.Node.NumVoters()
	if err != nil {
		return ReplicaReport{}, err
	}
	target := jsstream.ReplicasFor(nv)

	// events first (R-17 bootstrap): swallow a not-ready meta-group (the collected state
	// reflects the degraded actual<target); a hard error fails the pass.
	if eerr := jsstream.EnsureEventsStream(ctx, p.cfg.JS, target); eerr != nil && !errors.Is(eerr, jsstream.ErrMetaGroupNotReady) {
		return ReplicaReport{}, eerr
	}
	evState, eerr := jsstream.CollectStreamState(ctx, p.cfg.JS, jsstream.EventsStreamName, target)
	if eerr != nil {
		return ReplicaReport{}, eerr
	}
	states := []jsstream.StreamReplicaState{evState}

	sids, lerr := p.cfg.ListSIDs(ctx)
	if lerr != nil {
		return ReplicaReport{}, lerr
	}

	collected, ferr := p.reconcileSessions(ctx, sids, target)
	if ferr != nil {
		return ReplicaReport{}, ferr // R-20: a hard error => Observed stays false
	}
	states = append(states, collected...)
	return ReplicaReport{Streams: states, Observed: true}, nil
}

// ObserveReplicas is the READ-ONLY replica-health pass for the D7 retire gate (§8.3 / §9
// D8). Unlike ReconcileOnce it RAISES NOTHING — a drain-readiness check must never mutate
// JetStream topology (a raising probe could mask a stuck reconfig) — and it enumerates
// OBJ_xfer-* buckets from the LIVE JetStream stream list, NOT the DB ListSIDs, so a bucket
// that outlived its purged session row is still counted (else retire false-greens on a
// 1-replica orphan object → data loss, §9 D8). Collects events + every active history-<sid>
// + every OBJ_xfer-* and reports AllAtTarget/Degraded fail-closed (a hard JS error =>
// Observed stays false). XferState==nil disables the OBJ_xfer enumeration (events+history
// only), matching ReconcileOnce.
func (p *AuditPublisher) ObserveReplicas(ctx context.Context) (ReplicaReport, error) {
	nv, err := p.cfg.Node.NumVoters()
	if err != nil {
		return ReplicaReport{}, err
	}
	target := jsstream.ReplicasFor(nv)

	ev, eerr := jsstream.CollectStreamState(ctx, p.cfg.JS, jsstream.EventsStreamName, target)
	if eerr != nil {
		return ReplicaReport{}, eerr
	}
	states := []jsstream.StreamReplicaState{ev}

	sids, lerr := p.cfg.ListSIDs(ctx)
	if lerr != nil {
		return ReplicaReport{}, lerr
	}
	for _, sid := range sids {
		hs, herr := jsstream.CollectStreamState(ctx, p.cfg.JS, jsstream.HistoryStreamName(sid), target)
		if herr != nil {
			return ReplicaReport{}, herr
		}
		states = append(states, hs)
	}

	if p.cfg.XferState != nil { // same gate ReconcileOnce uses to enable OBJ_xfer
		xstreams, xerr := jsstream.ListXferStreams(ctx, p.cfg.JS)
		if xerr != nil {
			return ReplicaReport{}, xerr
		}
		for _, name := range xstreams {
			xs, cerr := jsstream.CollectStreamState(ctx, p.cfg.JS, name, target)
			if cerr != nil {
				return ReplicaReport{}, cerr
			}
			states = append(states, xs)
		}
	}
	return ReplicaReport{Streams: states, Observed: true}, nil
}

// reconcileSessions raises + collects history-<sid> and OBJ_xfer-<sid> for every active
// session with a bounded fan-out (R-21): MaxPar workers, all JOINED before return so a
// worker never outlives the loop. The first hard error wins; a not-ready meta-group is
// swallowed (state reflects degraded); an absent OBJ_xfer bucket is skipped.
func (p *AuditPublisher) reconcileSessions(ctx context.Context, sids []string, target int) ([]jsstream.StreamReplicaState, error) {
	var (
		mu       sync.Mutex
		out      []jsstream.StreamReplicaState
		firstErr error
		sem      = make(chan struct{}, p.cfg.MaxPar)
		wg       sync.WaitGroup
	)
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	add := func(s jsstream.StreamReplicaState) {
		mu.Lock()
		out = append(out, s)
		mu.Unlock()
	}
	for _, sid := range sids {
		sid := sid
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			// history-<sid>: always exists (created at session create); raise + collect.
			if herr := jsstream.EnsureHistoryStream(ctx, p.cfg.JS, sid, target); herr != nil && !errors.Is(herr, jsstream.ErrMetaGroupNotReady) {
				setErr(herr)
				return
			}
			hs, herr := jsstream.CollectStreamState(ctx, p.cfg.JS, jsstream.HistoryStreamName(sid), target)
			if herr != nil {
				setErr(herr)
				return
			}
			add(hs)
			// OBJ_xfer-<sid>: present only if the session ever transferred; skip if absent.
			if p.cfg.XferState == nil {
				return
			}
			xs, xerr := p.cfg.XferState(ctx, sid, target)
			switch {
			case xerr == nil:
				add(xs)
			case errors.Is(xerr, jetstream.ErrBucketNotFound), errors.Is(xerr, jetstream.ErrStreamNotFound):
				// no object store for this session => nothing to replicate; skip.
			default:
				setErr(xerr)
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}
