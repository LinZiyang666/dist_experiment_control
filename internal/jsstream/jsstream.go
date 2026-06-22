// Package jsstream owns the two JetStream streams tetherd uses for
// persistent state: the global `events` stream and the per-session
// `history-<sid>` streams. Architecture H.1 / H.3.
//
// Stream topology (must match H.1 verbatim):
//
//	events
//	  subjects   = ["tether.v2.sys.events"]
//	  retention  = limits, max_age=30d, max_bytes=1GiB, discard=old
//	  storage    = file
//	  subscribers: owner ctl + ops tools
//
//	history-<sid>                                         per session
//	  subjects   = ["tether.v2.s.<sid>.audit.>"]
//	  retention  = limits, max_age=-1, max_bytes=-1, discard=new
//	  storage    = file
//	  subscribers: session members via ephemeral consumers
//
// Helpers here are idempotent: EnsureXxx is safe to call on every
// boot and on every session create. Delete is a hard remove and is
// the canonical step ② of H.3 (session rm 三阶段). ListHistorySIDs
// returns just the `<sid>` part for cross-checking against SQLite
// (boot-time orphan stream cleanup).
package jsstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream-name conventions (architecture H.1).
const (
	EventsStreamName  = "events"
	HistoryStreamPref = "history-"
)

// HistoryStreamName builds "history-<sid>". sid validation is the
// caller's job (proto.ValidateSID) — this helper assumes the caller
// already trusts the input.
func HistoryStreamName(sid string) string {
	return HistoryStreamPref + sid
}

// SIDFromHistoryStream reverses HistoryStreamName. Returns ("", false)
// if the stream isn't a history-* stream. Used by orphan-cleanup to
// derive the sid from the stream name and check it against SQLite.
func SIDFromHistoryStream(stream string) (string, bool) {
	if !strings.HasPrefix(stream, HistoryStreamPref) {
		return "", false
	}
	return strings.TrimPrefix(stream, HistoryStreamPref), true
}

// EnsureEventsStream creates the events stream if it doesn't exist.
// Idempotent: a CreateStream returning "stream name already in use"
// is treated as success. Architecture H.1 spec values are inlined
// rather than imported from anywhere else; if H.1 changes, this is
// the one place to update.
func EnsureEventsStream(ctx context.Context, js jetstream.JetStream, targetReplicas int) error {
	cfg := jetstream.StreamConfig{
		Name:      EventsStreamName,
		Subjects:  []string{proto.SubjSysEvents},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    30 * 24 * time.Hour,
		MaxBytes:  1 << 30, // 1 GiB
		Discard:   jetstream.DiscardOld,
		Storage:   jetstream.FileStorage,
		Replicas:  targetReplicas, // D5 §6.4: replicasFor(nVoters); live callers pass ReplicasSingle
	}
	// NOTE: sys.events is best-effort leader-local (NOT re-derivable; R-11), so the
	// events stream carries NO Duplicates window — only the audit-bearing
	// history-<sid> streams need it (the publisher only stamps msg-ids there).
	return ensureStream(ctx, js, cfg)
}

// EnsureHistoryStream creates the per-session history stream. Called
// by handleSessionCreate AND on boot when reconciling existing
// sessions (architecture H.3 startup rule: SQLite has session row +
// no history-<sid> stream → rebuild empty stream).
//
// MaxBytes is set to a per-session ceiling so an accidental
// publish loop or an unusually chatty session can't take down the
// whole broker by exhausting the JetStream store dir. With
// Discard=DiscardNew the stream refuses new audit at the brink
// instead of evicting old (preserving audit history). Audit
// shard 03 F3: previously MaxBytes=-1 made DiscardNew unreachable
// code; the 80%-disk advisory monitor (H.4) still warns long
// before this cap matters in practice.
const historyMaxBytesPerSession = 1 << 30 // 1 GiB

func EnsureHistoryStream(ctx context.Context, js jetstream.JetStream, sid string, targetReplicas int) error {
	cfg := jetstream.StreamConfig{
		Name:       HistoryStreamName(sid),
		Subjects:   []string{historyFilterSubject(sid)},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     0, // 0 / -1 both mean "no expiry" in nats; use 0 to be explicit
		MaxBytes:   historyMaxBytesPerSession,
		Discard:    jetstream.DiscardNew,
		Storage:    jetstream.FileStorage,
		Replicas:   targetReplicas,   // D5 §6.4: replicasFor(nVoters); live callers pass ReplicasSingle
		Duplicates: AuditDedupWindow, // D5 §6.3: dedup window for the re-derivable audit publisher's msg-ids (inert in build-and-prove production — live publishAudit sets no msg-id; MP-2)
	}
	return ensureStream(ctx, js, cfg)
}

// DeleteHistoryStream is step ② of H.3 (session rm 三阶段). Returns
// nil if the stream is already gone (so callers can re-run after a
// crash-mid-rm without erroring). Other JetStream errors propagate.
func DeleteHistoryStream(ctx context.Context, js jetstream.JetStream, sid string) error {
	err := js.DeleteStream(ctx, HistoryStreamName(sid))
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	return fmt.Errorf("jsstream: delete %s: %w", HistoryStreamName(sid), err)
}

// ListHistorySIDs returns every <sid> that has a corresponding
// history-<sid> stream on the server. Used by broker startup to find
// orphan streams (those not in the SQLite sessions table → delete).
func ListHistorySIDs(ctx context.Context, js jetstream.JetStream) ([]string, error) {
	var out []string
	infos := js.ListStreams(ctx)
	for info := range infos.Info() {
		sid, ok := SIDFromHistoryStream(info.Config.Name)
		if !ok {
			continue
		}
		out = append(out, sid)
	}
	if err := infos.Err(); err != nil {
		return nil, fmt.Errorf("jsstream: list streams: %w", err)
	}
	return out, nil
}

// historyFilterSubject is the subject pattern history-<sid> filters
// on. Architecture H.1: "tether.v2.s.<sid>.audit.>" — captures
// audit.call / audit.proc / audit.port. The wildcard at the end is
// what lets us add new audit subkinds (e.g. audit.kick) later
// without re-creating the stream.
func historyFilterSubject(sid string) string {
	return fmt.Sprintf("%s.s.%s.audit.>", proto.SubjectPrefix, sid)
}

// ensureStream is the create-OR-RAISE helper (D5 §6.4 / R-18). On first call it
// creates the stream at cfg.Replicas. When the stream already exists it RECONCILES the
// replica factor toward cfg.Replicas — RAISE-ONLY: it never shrinks (shrink is the D7
// retire path, gated on AllAtTarget). Retention/limits are NOT auto-mutated (operators
// pin those via `nats stream edit`); only Replicas is reconciled, because the HA
// replica factor is owned by the cluster (replicasFor(nVoters)), not the operator.
func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	_, err := js.CreateStream(ctx, cfg)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("jsstream: create %s: %w", cfg.Name, err)
	}
	return reconcileReplicas(ctx, js, cfg)
}

// reconcileReplicas raises an existing stream's replica factor toward cfg.Replicas
// (D5 §6.4 / R-17/R-18). It compares the stream's CONFIGURED replicas (the current
// target) to cfg.Replicas: at-or-above => no-op (raise-only, never shrink). Below =>
// UpdateStream toward the target, with the meta-group readiness GATE being the
// UpdateStream rejection itself (classified to ErrMetaGroupNotReady) — an R1 stream's
// StreamInfo.Cluster cannot reveal the meta-group size, so a Cluster-based pre-check
// would falsely block the expansion (R-17).
func reconcileReplicas(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	s, err := js.Stream(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("jsstream: lookup %s: %w", cfg.Name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return fmt.Errorf("jsstream: info %s: %w", cfg.Name, err)
	}
	current := info.Config.Replicas
	if current < 1 {
		current = 1 // JS normalizes 0 -> 1 at create; defensive
	}
	if current >= cfg.Replicas {
		return nil // raise-only (R-18): already at/above target
	}
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		if IsMetaGroupNotReady(err) {
			return ErrMetaGroupNotReady // R-17: retriable; meta-group not ready
		}
		return fmt.Errorf("jsstream: update %s replicas->%d: %w", cfg.Name, cfg.Replicas, err)
	}
	return nil
}

// CollectStreamState reads a stream's live replica health for the §6.4 AllAtTarget
// predicate (R-19/R-20). It does NOT mutate (the raise is ensureStream's job); it
// reports Target=target, Actual=caught-up replicas (ActualReplicas), Ready=Actual>=Target.
// A missing stream is an error (fail-closed: the canonical pass must not silently skip
// a stream that should exist — R-20).
func CollectStreamState(ctx context.Context, js jetstream.JetStream, name string, target int) (StreamReplicaState, error) {
	s, err := js.Stream(ctx, name)
	if err != nil {
		return StreamReplicaState{}, fmt.Errorf("jsstream: lookup %s: %w", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return StreamReplicaState{}, fmt.Errorf("jsstream: info %s: %w", name, err)
	}
	actual := ActualReplicas(info)
	return StreamReplicaState{Name: name, Target: target, Actual: actual, Ready: actual >= target}, nil
}
