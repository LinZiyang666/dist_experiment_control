// ---- Mechanism B: replica reconfig + AllAtTarget predicate ----
// D5 (§6.3 / §6.4) — the leader-only, post-commit audit publisher + JS stream replica
// reconfigurer. ONE merged long-lived goroutine (R-13/R-21) constructed ONLY by the
// test/d5 harness; production (build-and-prove, cutover=D9) never builds a cluster.Node
// nor starts this loop, and the live publishAudit/reconcileOnRegister stay byte-unchanged
// (guard test TestD5ProductionWiresNoClusterNode). It reads committed entries from the
// REPLICATED raft log via the raft-free cluster.Node primitives (L-2: this file imports
// nats+cluster, internal/cluster imports neither nats nor this package), so any NEW
// leader re-derives committed-but-unpublished audit without the dead leader's memory.
package brokercontrol

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

	"github.com/LinZiyang666/tether/internal/backoff"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
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
const PublishTimeout = 5 * time.Second

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
	// Continuation also fires after exact readback; it is not a physical append counter.
	OnPublish func(subject, msgID string)
	// SessionExists reports whether a session row still exists in the committed DB (audit M5).
	// publishAudit consults it to disambiguate a "no stream" publish error: a GONE session means
	// its history-<sid> stream was permanently deleted by session-rm (bounded loss → advance),
	// while a still-existing session means the stream is merely not-yet-ensured (transient → R-22
	// retry). nil ⇒ conservative "exists" (always retry; preserves pre-M5 behavior in tests).
	SessionExists func(sid string) (bool, error)
	// Beat (B7), when non-nil, is called once per completed loop iteration so loopSet.Snapshot can
	// report per-iteration liveness. The loop owns its own ticker, so this is the only way the set can
	// tell a live loop from one that returned on its first line. nil ⇒ no liveness reported (tests).
	Beat func()
}

// AuditPublisher is the merged leader-only loop (Mechanism A publish + Mechanism B
// reconfig). Exported so test/d5 (external package) can drive it; the guard test forbids
// the production-wiring files from referencing it.
type AuditPublisher struct {
	Cfg          AuditPublisherConfig
	LossCnt      atomic.Uint64 // truncation-loss counter (R-6 non-vacuity, E-A6)
	LagCnt       atomic.Uint64 // §6.3 R-7: times the publish lag breached MaxLag (truncation-risk)
	DelStreamCnt atomic.Uint64 // audit M5: records dropped because the destination history-<sid> stream was deleted (racing session-rm)

	// h1 E3 dampers (single-goroutine: only Run's tick touches them — see
	// internal/backoff pkgdoc). reconBO paces ReconcileOnce ATTEMPTS
	// (1s→…→5min on persistent failure) — this loop retried a failing
	// JetStream stream-create every 100ms for five weeks and wrote 5.3GB of
	// identical WARN lines (the 2026-08-04 incident). pubBO is LOG-discipline
	// ONLY for PublishOnce errors: its cadence is untouched, because slowing
	// the publish drain would erode the §6.3 R-7 lag bound
	// (CommitIndex - published < TrailingLogs) that keeps snapshots from
	// truncating unpublished audit.
	ReconBO *backoff.Tracker
	PubBO   *backoff.Tracker
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
	return &AuditPublisher{
		Cfg:     cfg,
		ReconBO: backoff.New(backoff.Policy{Base: time.Second, Cap: 5 * time.Minute}),
		PubBO:   backoff.New(backoff.Policy{Base: time.Second, Cap: 5 * time.Minute}),
	}
}

// Run is the ONE long-lived goroutine (R-13), tied to ctx (R-14). Each tick: gate on
// leader+fence, then publish drain, then reconcile pass (R-21 happens-before). It polls
// IsLeader() rather than subscribing to LeaderCh (house style, read.go:30) so the
// goroutine count is constant across N leadership flaps — trivially passing the leak gate.
func (p *AuditPublisher) Run(ctx context.Context) {
	t := time.NewTicker(p.Cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.Tick(ctx)
			if p.Cfg.Beat != nil {
				p.Cfg.Beat()
			}
		}
	}
}

func (p *AuditPublisher) Tick(ctx context.Context) {
	// R-13/R-15: a follower / a fenced (quorum-lost) ex-leader within its lease does
	// nothing. The CommitIndex ceiling is the load-bearing safety; the fence is hardening.
	if !p.Cfg.Node.IsLeader() || p.Cfg.Node.LeaderContactStale(p.Cfg.Now()) {
		return
	}
	// h1 E3 log discipline. PublishOnce runs EVERY tick (the R-7 lag bound
	// depends on its cadence); only its logging is damped: first failure and
	// error-class changes log, repeats are counted, recovery logs the count.
	now := p.Cfg.Now()
	if _, err := p.PublishOnce(ctx); err != nil {
		if p.PubBO.Fail(now, ClassifyJSErrClass(err)) {
			p.Cfg.Logger.Warn("d5: audit publish", "err", err)
		}
	} else if suppressed, ok := p.PubBO.Recover(now); ok {
		p.Cfg.Logger.Info("d5: audit publish recovered", "suppressed_failures", suppressed)
	}
	// ReconcileOnce is ATTEMPT-damped: a persistently failing reconcile
	// (e.g. `create events: insufficient storage` — the incident) retries at
	// 1s→…→5min instead of every 100ms tick. ErrMetaGroupNotReady keeps its
	// historical no-Warn (a booting meta-group is normal) but participates in
	// the backoff so it stops hot-looping too. Accepted trade (plan E3): a
	// post-outage replica raise may start up to one cap-interval (5min) late.
	if !p.ReconBO.Due(now) {
		return
	}
	if _, err := p.ReconcileOnce(ctx); err != nil {
		logNow := p.ReconBO.Fail(now, ClassifyJSErrClass(err))
		if logNow && !errors.Is(err, jsstream.ErrMetaGroupNotReady) {
			p.Cfg.Logger.Warn("d5: replica reconcile (entering backoff; repeats suppressed)", "err", err)
		}
	} else if suppressed, ok := p.ReconBO.Recover(now); ok {
		p.Cfg.Logger.Info("d5: replica reconcile recovered", "suppressed_failures", suppressed)
	}
}

// TruncationLossCount reports how many committed log indices the publisher had to skip
// because a snapshot truncated them before they were published (§6.3 R-6 accepted-loss).
// Tests assert it fired (E-A6) or stayed 0 (steady state).
func (p *AuditPublisher) TruncationLossCount() uint64 { return p.LossCnt.Load() }

// PublishOnce drains the re-derivable audit in (checkpoint, CommitIndex] (R-4 ceiling,
// R-6 floor) and advances the REPLICATED checkpoint in batches (R-5). It is the steady
// state AND the post-election sweep (the sweep is just a large gap). Returns the highest
// fully-handled index. Idempotent + re-runnable: a re-run re-publishes with identical
// dedup ids that JetStream collapses (R-8/R-10/R-12).
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func (p *AuditPublisher) PublishOnce(ctx context.Context) (uint64, error) {
	return infrarepair.RunLegacyValue(ctx, p.Cfg.JS, infrarepair.AuditReplay, func(ctx context.Context) (uint64, error) {

		cur, err := p.Cfg.Node.AuditPublishedIndex()
		if err != nil {
			return 0, err
		}
		hi := p.Cfg.Node.CommitIndex()
		if hi <= cur {
			return cur, nil
		}

		// §6.3 R-7 lag bound: the publisher must stay within MaxLag (= cluster.TrailingLogs) of
		// the commit index, else a snapshot may truncate unpublished audit before this drain
		// reaches it (a bounded loud accepted-loss, handled below). A breach is an observability
		// signal — count + loud log; it does not change the drain.
		if hi-cur >= p.Cfg.MaxLag {
			p.LagCnt.Add(1)
			p.Cfg.Logger.Warn("d5: audit-publish lag exceeds TrailingLogs bound (truncation-loss risk, §6.3/§17)",
				"commit", hi, "published", cur, "lag", hi-cur, "bound", p.Cfg.MaxLag)
		}

		advanced := cur // highest index fully published-or-skipped
		lastCkpt := cur // highest index already checkpointed via raft
		flush := func(to uint64) error {
			if to <= lastCkpt {
				return nil
			}
			if err := p.Cfg.Node.AdvanceAuditPublished(to); err != nil {
				return err
			}
			lastCkpt = to
			return nil
		}

		for idx := cur + 1; idx <= hi; idx++ {
			// Re-clamp the floor each iteration: a snapshot may fire mid-loop (R-6).
			first, ferr := p.Cfg.Node.LogFirstIndex()
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
				p.RecordTruncationLoss(idx, lossTo)
				advanced = lossTo
				if lossTo >= hi {
					break // nothing left at or below the ceiling
				}
				idx = lossTo // loop's idx++ steps to lossTo+1
				continue
			}

			cmd, cerr := p.Cfg.Node.CommittedCommandAt(idx)
			switch {
			case errors.Is(cerr, cluster.ErrLogTruncated):
				p.RecordTruncationLoss(idx, idx)
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
				if perr := p.PublishReconcile(ctx, idx, cmd); perr != nil {
					// R-22: do NOT advance past an un-ACKed publish (queue, retry next tick).
					_ = flush(advanced)
					return advanced, perr
				}
			}
			if cmd.Op == cluster.OpTransferAudit { // D8a §9: re-derivable transfer audit
				if perr := p.PublishTransferAudit(ctx, idx, cmd); perr != nil {
					_ = flush(advanced) // R-22: queue + retry next tick, do not advance past
					return advanced, perr
				}
			}
			advanced = idx
			if advanced-lastCkpt >= uint64(p.Cfg.Batch) { // R-5: batched checkpoint
				if err := flush(advanced); err != nil {
					return advanced, err
				}
			}
		}
		if err := flush(advanced); err != nil {
			return advanced, err
		}
		return advanced, nil
	})
}

// LagExceededCount reports how many times the publish lag breached the §6.3 R-7
// TrailingLogs bound (a truncation-loss risk). Tests assert it fires (non-vacuity).
func (p *AuditPublisher) LagExceededCount() uint64 { return p.LagCnt.Load() }

// reconcileSID returns the single session id all records of one ReconcileBatch share
// (aux.SID), or "" when the entry replayed no records.
func ReconcileSID(procs []schema.AuditProc, ports []schema.AuditPort) string {
	if len(procs) > 0 {
		return procs[0].Session
	}
	if len(ports) > 0 {
		return ports[0].Session
	}
	return ""
}

func (p *AuditPublisher) RecordTruncationLoss(from, to uint64) {
	if to < from {
		return
	}
	p.LossCnt.Add(to - from + 1)
	p.Cfg.Logger.Warn("d5: audit lost to snapshot truncation (accepted bounded loss, §6.3/§17)",
		"from", from, "to", to)
}

// publishReconcile replays the committed ReconcileBatch's audit (PURE, from the entry's
// Aux only) and publishes each record with its dedup id. seq = position in the
// total-ordered slices ReplayReconcileAudit returns, so two leaders replaying the SAME
// committed bytes stamp IDENTICAL ids (R-8). A record's publish error aborts the entry
// (the caller does not advance the cursor past it — R-22).
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func (p *AuditPublisher) PublishReconcile(ctx context.Context, idx uint64, cmd *cluster.Command) error {
	return infrarepair.RunLegacy(ctx, p.Cfg.JS, infrarepair.AuditReplay, func(ctx context.Context) error {
		procs, ports, err := proc.ReplayReconcileAudit(cmd)
		if err != nil {
			return err
		}
		// Defense-in-depth (internal review m5): the sid is baked into the committed entry from
		// the agent register via the D4 forward path, which does not re-ValidateSID. A malformed
		// sid would inject into the JS subject / dedup id. All records of one ReconcileBatch share
		// one aux.SID, so validate once; on a bad sid loud-skip the WHOLE entry (advance past it
		// without publishing to a poisoned subject) rather than error/wedge.
		if sid := ReconcileSID(procs, ports); sid != "" {
			if verr := proto.ValidateSID(sid); verr != nil {
				p.Cfg.Logger.Warn("d5: skipping reconcile audit with invalid sid (subject-injection guard, m5)",
					"sid", sid, "raft_index", idx, "err", verr)
				return nil
			}
		}
		for seq, rec := range procs {
			payload, merr := json.Marshal(rec)
			if merr != nil {
				return merr
			}
			if perr := p.PublishAudit(ctx, proto.SubjAuditProc(rec.Session), payload, AuditMsgID(idx, cmd.ReqID, "proc", seq), idx, rec.Session); perr != nil {
				return perr
			}
		}
		for seq, rec := range ports {
			payload, merr := json.Marshal(rec)
			if merr != nil {
				return merr
			}
			if perr := p.PublishAudit(ctx, proto.SubjAuditPort(rec.Session), payload, AuditMsgID(idx, cmd.ReqID, "port", seq), idx, rec.Session); perr != nil {
				return perr
			}
		}
		return nil
	})
}

// PublishTransferAudit replays the committed record without regenerating Ts or
// ReqID. Legacy rows retain their MsgID/bounded-loss behavior. Continuation rows
// require exact history proof even after the dedupe window or publisher checkpoint
// is lost. Truncated Aux carries no proof: the origin keeps its outbox and re-submits
// the same canonical row; skipping the log gap never acknowledges that obligation.
// origin: hot-update S7 plan §5.2 / T23 / T24
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func (p *AuditPublisher) PublishTransferAudit(ctx context.Context, idx uint64, cmd *cluster.Command) error {
	return infrarepair.RunLegacy(ctx, p.Cfg.JS, infrarepair.AuditReplay, func(ctx context.Context) error {
		rec, ok, err := xferaudit.ReplayTransferAudit(cmd)
		if err != nil {
			return err
		}
		if !ok {
			return nil // empty Aux: nothing to publish
		}
		if schema.TransferContinuation(rec) {
			if _, err := ConfirmTransferAuditHistory(p, ctx, cmd); err != nil {
				return err // no exact receipt: retain both cursor and origin outbox
			}
			if p.Cfg.OnPublish != nil {
				p.Cfg.OnPublish(proto.SubjAuditTransfer(rec.Session), AuditMsgID(idx, cmd.ReqID, "xfer", 0))
			}
			return nil
		}
		// Subject-injection guard (cf. publishReconcile m5): rec.Session is baked into the JS
		// subject; loud-skip a malformed sid rather than publish to a poisoned subject.
		if verr := proto.ValidateSID(rec.Session); verr != nil {
			p.Cfg.Logger.Warn("d8: skipping transfer audit with invalid sid (subject-injection guard)",
				"sid", rec.Session, "raft_index", idx, "err", verr)
			return nil
		}
		payload, merr := json.Marshal(rec)
		if merr != nil {
			return merr
		}
		return p.PublishAudit(ctx, proto.SubjAuditTransfer(rec.Session), payload, AuditMsgID(idx, cmd.ReqID, "xfer", 0), idx, rec.Session)
	})
}

// ConfirmTransferAuditHistory returns the history receipt separately from Raft
// commit/checkpoint progress. Cluster routing returns this proof to the original
// outbox owner; no deleted-session bounded-loss branch can manufacture a receipt.
// The caller must supply an already committed command, as PublishOnce does.
// origin: hot-update S7 plan §5.2 / T23 / T24
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
//
//nolint:revive // Frozen S7 history port keeps the publisher before the per-call context.
func ConfirmTransferAuditHistory(p *AuditPublisher, ctx context.Context, cmd *cluster.Command) (xfercontract.HistoryReceipt, error) {
	return infrarepair.RunLegacyValue(ctx, p.Cfg.JS, infrarepair.AuditReplay, func(ctx context.Context) (xfercontract.HistoryReceipt, error) {

		rec, ok, err := xferaudit.ReplayTransferAudit(cmd)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		if !ok || !schema.TransferContinuation(rec) {
			return xfercontract.HistoryReceipt{}, fmt.Errorf("xferaudit: continuation record unavailable")
		}
		canonical, err := xferaudit.CanonicalTransferAudit(rec)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		budget := PublishTimeout
		if err := ctx.Err(); err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		if end, bounded := ctx.Deadline(); bounded {
			budget = min(budget, time.Until(end))
		}
		deadline, err := controlbudget.After(budget)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		history, err := xferhistory.New(xferhistory.Config{
			Base: xfercontract.Base{UTC: time.Now, Clock: func() (string, int64, error) {
				boot, err := procrequest.CurrentBootID()
				if err != nil {
					return "", 0, err
				}
				now, err := controlbudget.Now()
				return boot, now, err
			}},
			JS: p.Cfg.JS, SID: rec.Session,
		})
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		receipt, err := history.Confirm(canonical, deadline)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		// The ctx-free history port completes under its own bounded deadline. A
		// canceled leader attempt must retain responsibility even if JS accepted it;
		// the next owner reads back the same physical row before acknowledging.
		// origin: hot-update S7 plan §5.2 / T24
		if err := ctx.Err(); err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		// A successful adapter call alone is not evidence for this exact record.
		// origin: hot-update S7 plan §5.2 / T24
		if err := validateTransferHistoryReceipt(canonical, receipt); err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		return receipt, nil
	})
}

func validateTransferHistoryReceipt(record xfercontract.CanonicalRecord, receipt xfercontract.HistoryReceipt) error {
	if receipt.Stream != jsstream.HistoryStreamName(record.Identity.SID) || receipt.Sequence == 0 || receipt.RecordDigest != record.Digest {
		return fmt.Errorf("xferaudit: exact history receipt unavailable")
	}
	return nil
}

// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func (p *AuditPublisher) Publish(ctx context.Context, subject string, payload []byte, msgID string) error {
	pubCtx, cancel := context.WithTimeout(ctx, PublishTimeout) // R-14: child of loop ctx
	defer cancel()
	if err := infrarepair.PublishLegacy(pubCtx, p.Cfg.JS, subject, payload, msgID); err != nil {
		return err
	}
	if p.Cfg.OnPublish != nil {
		p.Cfg.OnPublish(subject, msgID)
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
func (p *AuditPublisher) PublishAudit(ctx context.Context, subject string, payload []byte, msgID string, idx uint64, sid string) error {
	err := p.Publish(ctx, subject, payload, msgID)
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrNoStreamResponse) && p.StreamDeletedPermanently(sid) {
		p.DelStreamCnt.Add(1)
		p.Cfg.Logger.Warn("d5: audit dropped — session + its history stream are gone (accepted bounded loss, §6.3/§17; racing session-rm)",
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
func (p *AuditPublisher) StreamDeletedPermanently(sid string) bool {
	if p.Cfg.SessionExists == nil {
		return false
	}
	exists, err := p.Cfg.SessionExists(sid)
	return err == nil && !exists
}

// DeletedStreamLossCount reports how many audit records the publisher dropped because their
// session (and thus its history-<sid> stream) had been removed (audit M5). Tests read it to
// assert the bounded-loss branch fired (non-vacuity).
func (p *AuditPublisher) DeletedStreamLossCount() uint64 { return p.DelStreamCnt.Load() }

// auditMsgID is the JetStream dedup key (R-8/R-10). For a reqID-bearing reconcile entry
// the id keys on the CROSS-RETRY-STABLE ReqID so a D4 ack-lost retry (which commits at a
// NEW raft index via appliedDedup yet still carries Aux) re-derives the SAME ids and
// JetStream collapses them — keying on the raft index would double-publish. kind ∈
// {proc,port} partitions the two record families so they never collide on (idx,seq).
func AuditMsgID(raftIndex uint64, reqID, kind string, seq int) string {
	if reqID != "" {
		return fmt.Sprintf("q%s:%s:%d", reqID, kind, seq)
	}
	return fmt.Sprintf("r%d:%s:%d", raftIndex, kind, seq)
}

// ReplicaReport is the canonical §6.4 replica-health pass (R-19/R-20): the FULL stream
// set the leader observed this tick. D5 ships only the predicate (D7 consumes AllAtTarget
// as its retire gate; D8b writes the replication_degraded alert row).
type ReplicaReport struct {
	Streams []jsstream.StreamReplicaState
	// StreamCount is the complete work-set size discovered before per-stream collection begins.
	// It may be non-zero when Observed is false: that lets the next bounded observation size itself
	// correctly after this one timed out partway through collection, without treating partial replica
	// state as a measurement.
	StreamCount int
	Observed    bool // false => the pass could not complete a canonical observation
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

// replicaReportAllAtLeast is AllAtTarget asked against a DIFFERENT target than the one
// the observation was taken with. Same fail-closed contract: an unobserved or empty set
// is never "ready".
//
// origin: prerelease audit broker-cluster-ops/#15. Retire's readiness gate asked
// "is every stream at the target implied by the CURRENT voter count", and that
// question cannot be answered yes when the reason you are retiring is that a voter is
// already down: three voters means a target of 3, a dead voter means an actual of 2,
// and the gate refuses forever. Retiring a dead voter — the case the runbook documents
// and the one operators actually hit — was structurally impossible.
//
// The right question is the one the runbook already promises: will every stream STILL
// be at target once this node is gone. That is ReplicasFor(voters-1), and it is
// strictly weaker only in the direction that matters. 3->2 retiring a live node:
// target 2, actual 3, passes. 3->2 retiring a dead one: target 2, actual 2, passes
// (it used to deadlock). Three voters with TWO dead: target 2, actual 1, still refused
// — no conservatism was traded away.
//
// A package-level function, not a method on ReplicaReport: the structural-budget
// ratchet pins type method counts.
func ReplicaReportAllAtLeast(r ReplicaReport, target int) bool {
	if !r.Observed || len(r.Streams) == 0 {
		return false
	}
	if target < 1 {
		target = 1
	}
	for _, s := range r.Streams {
		if s.Actual < target {
			return false
		}
	}
	return true
}

// replicaReportSurvivesRemoval reports whether EVERY observed stream would still hold at
// least `target` caught-up replicas once `server` is gone.
//
// origin: prerelease audit round 2, G-1. replicaReportAllAtLeast answers the same shape of
// question with a tally, which cannot express "excluding that one". Same fail-closed
// contract: an unobserved or empty set is never ready, and a stream whose placement the
// server did not report contributes no survivors.
//
// A package-level function, not a method on ReplicaReport: the structural-budget ratchet
// pins type method counts.
func ReplicaReportSurvivesRemoval(r ReplicaReport, server string, target int) bool {
	if !r.Observed || len(r.Streams) == 0 {
		return false
	}
	if target < 1 {
		target = 1
	}
	for _, s := range r.Streams {
		if jsstream.SurvivorsExcluding(s, server) < target {
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
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func (p *AuditPublisher) ReconcileOnce(ctx context.Context) (ReplicaReport, error) {
	return infrarepair.RunLegacyValue(ctx, p.Cfg.JS, infrarepair.ReplicaReconcile, func(ctx context.Context) (ReplicaReport, error) {

		nv, err := p.Cfg.Node.NumVoters()
		if err != nil {
			return ReplicaReport{}, err
		}
		target := jsstream.ReplicasFor(nv)

		// events first (R-17 bootstrap): swallow a not-ready meta-group (the collected state
		// reflects the degraded actual<target); a hard error fails the pass.
		if eerr := jsstream.EnsureEventsStream(ctx, p.Cfg.JS, target); eerr != nil && !errors.Is(eerr, jsstream.ErrMetaGroupNotReady) {
			return ReplicaReport{}, eerr
		}
		evState, eerr := jsstream.CollectStreamState(ctx, p.Cfg.JS, jsstream.EventsStreamName, target)
		if eerr != nil {
			return ReplicaReport{}, eerr
		}
		states := []jsstream.StreamReplicaState{evState}

		sids, lerr := p.Cfg.ListSIDs(ctx)
		if lerr != nil {
			return ReplicaReport{}, lerr
		}

		collected, ferr := p.ReconcileSessions(ctx, sids, target)
		if ferr != nil {
			return ReplicaReport{}, ferr // R-20: a hard error => Observed stays false
		}
		states = append(states, collected...)
		return ReplicaReport{Streams: states, Observed: true}, nil
	})
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
	nv, err := p.Cfg.Node.NumVoters()
	if err != nil {
		return ReplicaReport{}, err
	}
	target := jsstream.ReplicasFor(nv)

	sids, lerr := p.Cfg.ListSIDs(ctx)
	if lerr != nil {
		return ReplicaReport{}, lerr
	}

	var xstreams []string
	if p.Cfg.XferState != nil { // same gate ReconcileOnce uses to enable OBJ_xfer
		var xerr error
		xstreams, xerr = jsstream.ListXferStreams(ctx, p.Cfg.JS)
		if xerr != nil {
			return ReplicaReport{}, xerr
		}
	}
	// Discover the complete work set BEFORE serial collection. If collection hits its deadline, callers
	// can retain this count for the next budget without mistaking partial state for an observation.
	streamCount := 1 + len(sids) + len(xstreams)
	failed := func(err error) (ReplicaReport, error) {
		return ReplicaReport{StreamCount: streamCount}, err
	}

	ev, eerr := jsstream.CollectStreamState(ctx, p.Cfg.JS, jsstream.EventsStreamName, target)
	if eerr != nil {
		return failed(eerr)
	}
	states := []jsstream.StreamReplicaState{ev}

	for _, sid := range sids {
		hs, herr := jsstream.CollectStreamState(ctx, p.Cfg.JS, jsstream.HistoryStreamName(sid), target)
		if herr != nil {
			return failed(herr)
		}
		states = append(states, hs)
	}

	for _, name := range xstreams {
		xs, cerr := jsstream.CollectStreamState(ctx, p.Cfg.JS, name, target)
		if cerr != nil {
			return failed(cerr)
		}
		states = append(states, xs)
	}
	return ReplicaReport{Streams: states, StreamCount: streamCount, Observed: true}, nil
}

// reconcileSessions raises + collects history-<sid> and OBJ_xfer-<sid> for every active
// session with a bounded fan-out (R-21): MaxPar workers, all JOINED before return so a
// worker never outlives the loop. The first hard error wins; a not-ready meta-group is
// swallowed (state reflects degraded); an absent OBJ_xfer bucket is skipped.
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func (p *AuditPublisher) ReconcileSessions(ctx context.Context, sids []string, target int) ([]jsstream.StreamReplicaState, error) {
	return infrarepair.RunLegacyValue(ctx, p.Cfg.JS, infrarepair.ReplicaReconcile, func(ctx context.Context) ([]jsstream.StreamReplicaState, error) {

		var (
			mu  sync.Mutex
			out []jsstream.StreamReplicaState
		)
		add := func(s jsstream.StreamReplicaState) {
			mu.Lock()
			out = append(out, s)
			mu.Unlock()
		}
		firstErr := infrarepair.ParallelSessions(ctx, sids, p.Cfg.MaxPar, func(ctx context.Context, sid string) error {
			// history-<sid>: always exists (created at session create); raise + collect.
			if herr := jsstream.EnsureHistoryStream(ctx, p.Cfg.JS, sid, target); herr != nil && !errors.Is(herr, jsstream.ErrMetaGroupNotReady) {
				return herr
			}
			hs, herr := jsstream.CollectStreamState(ctx, p.Cfg.JS, jsstream.HistoryStreamName(sid), target)
			if herr != nil {
				return herr
			}
			add(hs)
			// OBJ_xfer-<sid>: present only if the session ever transferred; skip if absent.
			if p.Cfg.XferState == nil {
				return nil
			}
			xs, xerr := p.Cfg.XferState(ctx, sid, target)
			switch {
			case xerr == nil:
				add(xs)
			case errors.Is(xerr, jetstream.ErrBucketNotFound), errors.Is(xerr, jetstream.ErrStreamNotFound):
				// no object store for this session => nothing to replicate; skip.
			default:
				return xerr
			}
			return nil
		})
		if firstErr != nil {
			return nil, firstErr
		}
		return out, nil
	})
}
