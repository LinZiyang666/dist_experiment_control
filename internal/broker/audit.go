// Cross-cutting JetStream + audit + sys.events helpers for the broker.
// The actual call/proc/port audit shapes live in exec.go / expose.go;
// this file owns:
//
//   - sys.events emission (architecture H.1 events stream)
//   - boot reconciliation of session ↔ history-<sid> streams
//     (H.3 startup rules: orphan stream cleanup, missing-stream
//     rebuild, DELETING resume)
//   - the 3-phase session rm orchestration (H.3) shared across
//     interactive `session rm` and the boot DELETING resumer.
package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
)

// pubSysEvent emits one tether.v2.sys.events message. Always best-
// effort: failure to publish doesn't fail the underlying operation
// (ops events are observability, not authoritative state). Routes
// through publishAudit so it lands in the `events` JetStream stream
// when JS is available; falls back to core publish otherwise.
//
// kind values per architecture H.1: session_created /
// session_destroyed / member_joined / pin_failed / rotated_pin /
// kicked / tetherd_restarted / agent_registered /
// agent_unregistered / disk_pressure.
func (b *Broker) pubSysEvent(kind string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["v"] = 1
	fields["type"] = kind
	fields["ts"] = b.cfg.Now().UTC().Format(time.RFC3339Nano)
	body, err := json.Marshal(fields)
	if err != nil {
		b.cfg.Logger.Warn("broker: marshal sys.events", "err", err, "kind", kind)
		return
	}
	if err := b.publishAudit(proto.SubjSysEvents, body); err != nil {
		b.cfg.Logger.Warn("broker: pub sys.events", "err", err, "kind", kind)
	}
}

// finalizeSessionRm runs phases ②③④ of architecture H.3 for a
// session that's already in state=DELETING (phase ① done by the
// caller, either handleSessionRm or the boot resumer). Idempotent:
// every step is safe to retry, so a crash mid-finalize re-runs
// cleanly when the broker comes back up.
//
// Phase ② DELETE the history-<sid> stream (no-op if already gone).
// Phase ③ DELETE all dependent SQLite rows in one tx; sessions row
//
//	included so future ① on the same sid can re-create.
//
// Phase ④ pub sys.events session_destroyed.
//
// We deliberately keep ② before ③: if ③ runs first and the stream
// delete then fails, we have an orphan stream that the next boot
// would re-pair with a re-created session row of the same sid (bad
// audit-history pollution). Doing ② first means the worst case is
// "DB rows linger after ② succeeded" — those get reaped by re-runs
// of finalizeSessionRm, and any new req gets rejected by C.1 §6 in
// the meantime because state is still DELETING.
func (b *Broker) finalizeSessionRm(ctx context.Context, sid string) error {
	if b.js != nil {
		if err := jsstream.DeleteHistoryStream(ctx, b.js, sid); err != nil {
			return fmt.Errorf("phase 2: %w", err)
		}
		// Phase ②.5 (audit M4 / transfer F1): delete the tier-B transfer object store too.
		// Without this the OBJ_xfer-<sid> bucket outlives the purged session row; at N>=3 a
		// lingering bucket below target replicas permanently fails the retire gate. Idempotent
		// (DeleteXferBucket tolerates "already gone"), so a crash-mid-rm re-run is clean.
		if err := jsstream.DeleteXferBucket(ctx, b.js, sid); err != nil {
			return fmt.Errorf("phase 2.5 (xfer bucket): %w", err)
		}
	}
	if err := b.dropSession(sid); err != nil {
		return fmt.Errorf("phase 3: %w", err)
	}
	// Prune the tunnel's per-session kill-generation bookkeeping now that the
	// session is permanently gone (self-review: bounds killGenSession growth).
	if srv := b.tunnelSrv.Load(); srv != nil {
		srv.ForgetSession(sid)
	}
	b.pubSysEvent("session_destroyed", map[string]any{"sid": sid})
	b.cfg.Logger.Info("broker: session removed", "sid", sid)
	return nil
}

// dropSessionRows is the H.3 phase ③ SQL atom. ON DELETE CASCADE on
// the FK chain (sessions → members/nodes/agent_provisioning;
// nodes → processes/port_allocations) means a single DELETE FROM
// sessions covers everything; we keep the explicit per-table
// DELETEs anyway so a future schema change that drops a cascade FK
// doesn't silently leak rows.
func dropSessionRows(db *sql.DB, sid string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var state string
	switch err := tx.QueryRow(`SELECT state FROM sessions WHERE sid = ?`, sid).Scan(&state); {
	case err == nil:
		if state != string(session.StateDeleting) {
			return session.ErrDeleting
		}
	case errors.Is(err, sql.ErrNoRows):
		return nil
	default:
		return fmt.Errorf("dropSessionRows lookup: %w", err)
	}

	for _, q := range []string{
		`DELETE FROM port_allocations  WHERE sid = ?`,
		`DELETE FROM processes         WHERE sid = ?`,
		`DELETE FROM proxy_subscribers WHERE sid = ?`,
		`DELETE FROM agent_provisioning WHERE sid = ?`,
		`DELETE FROM nodes             WHERE sid = ?`,
		`DELETE FROM members           WHERE sid = ?`,
		`DELETE FROM sessions          WHERE sid = ?`,
	} {
		if _, err := tx.Exec(q, sid); err != nil {
			return fmt.Errorf("dropSessionRows %q: %w", q, err)
		}
	}
	return tx.Commit()
}

// reconcileHistoryStreamsOnBoot runs every architecture H.3 startup
// rule against the current SQLite ↔ JetStream state:
//
//   - For every state=DELETING session: re-run finalizeSessionRm.
//   - For every state=ACTIVE session WITHOUT a history-<sid> stream:
//     EnsureHistoryStream (operator may have manually deleted it,
//     or the broker crashed mid-create).
//   - For every history-<sid> stream WITHOUT a SQLite session:
//     DeleteHistoryStream (orphan).
//
// Called once from Run after JS is confirmed available. b.js MUST
// be non-nil at call time. Errors are logged but not fatal — broker
// startup succeeds even if reconcile partially fails; subsequent
// bool ticks can complete the work.
func (b *Broker) reconcileHistoryStreamsOnBoot(ctx context.Context) error {
	deleting, err := listSessionsByState(b.cfg.DB, "DELETING")
	if err != nil {
		return fmt.Errorf("list DELETING: %w", err)
	}
	for _, sid := range deleting {
		if err := b.finalizeSessionRm(ctx, sid); err != nil {
			b.cfg.Logger.Warn("broker: resume DELETING failed", "sid", sid, "err", err)
		}
	}

	active, err := listSessionsByState(b.cfg.DB, "ACTIVE")
	if err != nil {
		return fmt.Errorf("list ACTIVE: %w", err)
	}
	activeSet := map[string]bool{}
	for _, sid := range active {
		activeSet[sid] = true
		ensureCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := jsstream.EnsureHistoryStream(ensureCtx, b.js, sid, jsstream.ReplicasSingle); err != nil {
			b.cfg.Logger.Warn("broker: ensure history stream on boot",
				"sid", sid, "err", err)
		}
		cancel()
	}

	// audit G (CRITICAL data-loss): the orphan DELETE arms below list the SHARED JS meta
	// (ListHistorySIDs / ListXferStreams) but classify "orphan" against the LOCAL activeSet. On a fresh
	// joiner — or ANY not-yet-caught-up follower — the local SQLite view is stale/empty, so EVERY
	// cluster-wide stream looks orphan and would be WIPED (the first real grow would delete all cluster
	// history + every in-flight tier-B bucket). Only a CAUGHT-UP LEADER has an activeSet that reflects
	// the true cluster state. In single mode (b.cl == nil) the local DB IS the authority — reap as before.
	if !b.reaperMayDelete() {
		b.cfg.Logger.Info("broker: skipping boot orphan stream/bucket reap (not a caught-up leader — refuses to wipe cluster-wide history from a stale local view)")
		return nil
	}

	listCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	streamSIDs, err := jsstream.ListHistorySIDs(listCtx, b.js)
	cancel()
	if err != nil {
		return fmt.Errorf("list history streams: %w", err)
	}
	for _, sid := range streamSIDs {
		if activeSet[sid] {
			continue
		}
		dropCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := jsstream.DeleteHistoryStream(dropCtx, b.js, sid); err != nil {
			b.cfg.Logger.Warn("broker: orphan stream delete",
				"sid", sid, "err", err)
		} else {
			b.cfg.Logger.Info("broker: orphan history stream deleted", "sid", sid)
		}
		cancel()
	}

	// audit M4 / transfer F1: reap orphan tier-B buckets the same way. A bucket whose session
	// row is gone (no longer ACTIVE, and any DELETING session was finalized above) has no live
	// transfer, so deleting the WHOLE bucket is safe — and necessary: a lingering OBJ_xfer-<sid>
	// below target replicas blocks the D7 retire gate (AllAtTarget) forever. DeleteXferBucket
	// tolerates "already gone", so concurrent brokers racing the delete in cluster mode is
	// harmless (mirrors the history orphan reaper, which is likewise un-gated on home ownership
	// because a truly-orphan stream cannot be live anywhere).
	xferCtx, cancelX := context.WithTimeout(ctx, 3*time.Second)
	xferStreams, xerr := jsstream.ListXferStreams(xferCtx, b.js)
	cancelX()
	if xerr != nil {
		return fmt.Errorf("list xfer streams: %w", xerr)
	}
	for _, name := range xferStreams {
		sid, ok := jsstream.SIDFromXferStream(name)
		if !ok || activeSet[sid] {
			continue
		}
		dropCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := jsstream.DeleteXferBucket(dropCtx, b.js, sid); err != nil {
			b.cfg.Logger.Warn("broker: orphan xfer bucket delete", "sid", sid, "err", err)
		} else {
			b.cfg.Logger.Info("broker: orphan xfer bucket deleted", "sid", sid)
		}
		cancel()
	}
	return nil
}

// listSessionsByState is a tiny SQLite helper used only by the boot
// reconciler. Lives in this file rather than internal/session so the
// session pkg doesn't grow a state-string filter API just for one
// caller.
func listSessionsByState(db *sql.DB, state string) ([]string, error) {
	rows, err := db.Query(`SELECT sid FROM sessions WHERE state = ? ORDER BY sid`, state)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		out = append(out, sid)
	}
	return out, rows.Err()
}
