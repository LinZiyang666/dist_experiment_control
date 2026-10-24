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
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/xferauth"
)

// pubSysEvent emits one tether.v2.sys.events message. Always best-
// effort: failure to publish doesn't fail the underlying operation
// (ops events are observability, not authoritative state). Routes
// through publishAudit so it lands in the `events` JetStream stream
// when JS is available; core publish is used when there is no JS.
//
// "Falls back to core publish otherwise" would be the wrong reading and
// this comment used to invite it (internal review n9): the fallback is
// chosen by whether JS EXISTS, not by whether the publish SUCCEEDED. A
// JS publish that times out returns the error and drops the event —
// there is no core retry. That is deliberate under best-effort, but it
// has one consequence worth naming: callers that record an idempotency
// key BEFORE checking the result (topology_reconcile's eventKey) will
// never retry that event for that (gen|action|reason).
//
// kind values ACTUALLY emitted (architecture H.1): session_created /
// session_destroyed / member_joined / pin_failed / tetherd_restarted /
// agent_registered / agent_evicted / agent_roster_stale / disk_pressure,
// plus the operational proxy_* / nats_topology_* / rehome / grow-cutover
// kinds. DOC-12: architecture H.1 previously listed `rotated_pin`, `kicked`
// and `agent_unregistered`, which have NO producer in v1 — `session
// kick`/`rotate-pin` are not implemented, and node removal emits
// `agent_evicted` (I.2b operator eviction), not `kicked`. H.1 was corrected
// to match this; do not re-add those three unless the producing verbs land.
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

// The cold resumer alone verifies its locally enumerated tombstone.
// origin: hot-update S7 plan §6.4 / lead integration decision 3
func (b *Broker) finalizeSessionRm(ctx context.Context, sid string) error {
	deadline, err := transferPassDeadline(ctx)
	if err != nil {
		return err
	}
	return resumeSessionAuthority(b, deadline, ctx.Done(), sid)
}

// origin: hot-update S7 plan §6.4 / INTEG-G ordered cascade binding
func cascadeSessionRm(ctx context.Context, b *Broker, sid string) error {
	deadline, err := transferPassDeadline(ctx)
	if err != nil {
		return err
	}
	return cascadeSessionDeadline(b, deadline, ctx.Done(), sid)
}

func cascadeSessionDeadline(_ *Broker, _ controlbudget.Deadline, _ <-chan struct{}, _ string) error {
	// A SID-only delayed callback cannot learn whichever creation exists now.
	// Guarded handler and cold-resume callbacks carry their original proof.
	// origin: hot-update S8 plan §6.2 / P16
	return upgradeauthority.Match("", "")
}

// dropSessionRows retains the broker adapter used by the single-mode writer.
// origin: hot-update S7 plan §6.4 / T31
func dropSessionRows(db *sql.DB, sid string) error { return xferauth.DropSessionRows(db, sid) }

// Reconciliation retains the broker's current read and deletion authority.
// origin: hot-update S7 plan §6.4 / T31 / INTEG-G responsibility move
func (b *Broker) reconcileHistoryStreamsOnBoot(ctx context.Context) error {
	deadline, err := transferPassDeadline(ctx)
	if err != nil {
		return err
	}
	return xferauth.ReconcileSessionHistory(deadline, ctx.Done(), xferauth.SessionHistoryConfig{DB: b.cfg.DB, ReadDB: func() *sql.DB { return b.read().SQL() }, JS: brokerJS(b),
		Logger: b.cfg.Logger, Finalize: func(d controlbudget.Deadline, stop <-chan struct{}, sid string) error {
			return resumeSessionAuthority(b, d, stop, sid)
		}, MayDelete: b.reaperMayDelete})
}

// listSessionsByState keeps the existing boot-reconcile adapter.
// origin: hot-update S7 plan §6.4 / T31
func listSessionsByState(db *sql.DB, state string) ([]string, error) {
	return xferauth.ListSessionSIDs(db, state)
}

// External teardown is fenced before any history/OBJ/tunnel destruction. In
// single mode a write reservation prevents recreation during those effects;
// in cluster mode they run under the existing leader applyMu planner. Final SQL
// retains the same guard in its owner, and never learns a replacement identity.
// origin: hot-update S8 plan §6.2 / §8.3 M17 / P16
func cascadeSessionAuthority(b *Broker, d controlbudget.Deadline, stop <-chan struct{}, guard upgradeauthority.Guard) error {

	if err := upgradeauthority.Match(guard.Incarnation, guard.Incarnation); err != nil {
		return err
	}

	if b.clusterMode {
		if b.cl == nil || b.cl.node == nil {
			return singleWriteRefusal()
		}
		// A follower retains the committed tombstone's guard and routes teardown
		// to the leader, whose planner holds authority across external effects.
		// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2
		return publishSessionDestroyedAuthority(b, guard, proposeValue(b, VerbSessionDrop, SessionMutatePayload{SID: guard.SID, Authority: &guard}, func(db *sql.DB) (*cluster.Command, error) { return planSessionCascadeAuthority(b, db, d, stop, guard) }))
	}
	return publishSessionDestroyedAuthority(b, guard, cascadeSessionSingleAuthority(b, d, stop, guard))
}
