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
	"log/slog"
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

// XferBackingStreamPref is the prefix of the JetStream stream backing a per-session
// transfer object-store bucket: nats.go names an object store "OBJ_<bucket>" and the
// broker's bucket is "xfer-<sid>", so the backing stream is "OBJ_xfer-<sid>". Kept here
// so the read-only replica observer can enumerate xfer buckets from the LIVE stream list
// rather than a DB session list — a bucket can outlive its purged session row (§9 D8).
const XferBackingStreamPref = "OBJ_xfer-"

// ListXferStreams returns the names of all OBJ_xfer-* streams currently in JetStream
// (the object stores backing transfers). The retire-gate observer counts these directly
// off JetStream so an orphan bucket (session row gone, bucket not yet reaped) at a
// single replica is still seen — never false-greening a retire onto un-redundant data.
func ListXferStreams(ctx context.Context, js jetstream.JetStream) ([]string, error) {
	var out []string
	infos := js.ListStreams(ctx)
	for info := range infos.Info() {
		if strings.HasPrefix(info.Config.Name, XferBackingStreamPref) {
			out = append(out, info.Config.Name)
		}
	}
	if err := infos.Err(); err != nil {
		return nil, fmt.Errorf("jsstream: list streams: %w", err)
	}
	return out, nil
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

// DeleteXferBucket deletes the per-session tier-B transfer object store (bucket
// "xfer-<sid>", backed by the OBJ_xfer-<sid> stream) — the session-rm cascade's tier-B
// analogue of DeleteHistoryStream (audit M4 / transfer F1). Without it the bucket outlives
// its purged session row forever; at N>=3 a lingering bucket below its target replica count
// then PERMANENTLY fails the retire gate (AllAtTarget) and latches replication_degraded.
// Returns nil if the bucket is already gone so a crash-mid-rm re-run is idempotent.
func DeleteXferBucket(ctx context.Context, js jetstream.JetStream, sid string) error {
	err := js.DeleteObjectStore(ctx, proto.XferBucketName(sid))
	if err == nil {
		return nil
	}
	// DeleteObjectStore joins ErrBucketNotFound with the underlying ErrStreamNotFound; accept
	// either as "already gone".
	if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	return fmt.Errorf("jsstream: delete xfer bucket %s: %w", proto.XferBucketName(sid), err)
}

// SIDFromXferStream reverses the OBJ_xfer-<sid> backing-stream name. Returns ("", false)
// if stream isn't an xfer backing stream. Used by boot orphan-bucket reaping to derive the
// sid from the stream name and check it against the SQLite sessions table.
func SIDFromXferStream(stream string) (string, bool) {
	if !strings.HasPrefix(stream, XferBackingStreamPref) {
		return "", false
	}
	return strings.TrimPrefix(stream, XferBackingStreamPref), true
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
	// audit jsstream F2: raise ONLY the replica factor — start from the LIVE config and change
	// just Replicas, so an operator's `nats stream edit` limits (MaxBytes/retention/…) survive.
	// UpdateStream(cfg) with the full tether-default cfg would silently clobber those edits,
	// contradicting this function's "only Replicas is reconciled" contract (the HA replica factor
	// is cluster-owned; retention/limits are operator-owned).
	target := info.Config
	target.Replicas = cfg.Replicas
	if _, err := js.UpdateStream(ctx, target); err != nil {
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
	return StreamReplicaState{
		Name: name, Target: target, Actual: actual, Ready: actual >= target,
		// G69 additive: placement evidence, distinct from catch-up. Ready is byte-unchanged.
		Assigned: AssignedReplicas(info), Configured: info.Config.Replicas,
	}, nil
}

// PlacementCanaryStreamName is the deterministic name of the throwaway stream used to MEASURE — rather
// than infer — whether the JetStream meta can place a NEW asset at a given replica factor right now.
// The leading underscore keeps it out of every real namespace (events / history-<sid> / OBJ_xfer-<sid>).
const PlacementCanaryStreamName = "_tether_placement_canary"

// ProbeMetaCanPlace creates an EMPTY stream at targetReplicas and immediately deletes it, returning nil
// only if the meta actually accepted the placement.
//
// EXTERNAL REVIEW F3. The join gate previously INFERRED placeability from the peers already assigned to
// the long-lived `events` stream. That is a proxy, and the review showed it can be satisfied by a
// corpse: tether never issues a JS peer-remove, so a member retired in a 3->2 shrink stays listed and a
// later 2->3 regrow was "proven" on the first tick by a peer that no longer exists. Filtering Offline
// peers closed that specific counter-example, but the contract the CLI actually promises — `cluster add`
// returns 0 only when a new Replicas:N asset is creatable — deserves a DIRECT measurement.
//
// The canary is empty and immediately removed, so it never introduces the byte-copy wait that gating on
// catch-up would: there is no history to replicate. A leftover canary from a crash is harmless and is
// reclaimed by the next probe, which deletes before it creates.
// isOwnPlacementCanary reports whether an existing stream is unmistakably a canary this probe left
// behind, rather than an operator's stream that merely shares the name (round-4 ownership finding).
// placementCanaryOwnerKey/Val is an OWNERSHIP MARKER stamped into the canary's stream metadata
// (round-4 self-audit, answering the re-review's "a configuration fingerprint is not an ownership
// token"). A shape match is reproducible by anyone; a marker is at least authored.
const (
	placementCanaryOwnerKey = "tether_placement_canary"
	placementCanaryOwnerVal = "ephemeral-probe"
)

func isOwnPlacementCanary(cfg jetstream.StreamConfig) bool {
	// ROUND-5 R5-F3: the marker is REQUIRED, with no shape-only fallback. The previous version accepted a
	// marker-less lookalike whenever it carried no application metadata, justified by servers that might
	// not echo stream metadata — but the pinned server demonstrably DOES echo it (proven by this package's
	// own tests), so that fallback bought nothing and cost a destructive guess. A canary this probe left
	// behind always carries the marker; anything else is somebody's stream, and refusing to touch it can
	// at worst wedge the probe until an operator removes it. Wedging is recoverable, deleting is not.
	return cfg.Name == PlacementCanaryStreamName &&
		len(cfg.Subjects) == 1 && cfg.Subjects[0] == placementCanarySubject &&
		cfg.Storage == jetstream.MemoryStorage &&
		cfg.MaxMsgs == 1 &&
		cfg.Metadata[placementCanaryOwnerKey] == placementCanaryOwnerVal
}

// placementCanarySubject is deliberately namespaced under $TETHER so a collision with a real subject is
// not plausible; it is part of the ownership fingerprint above.
const placementCanarySubject = "$TETHER.placement.canary"

// The optional logger is variadic so callers that do not have one (tests, and any caller that only cares
// about the verdict) stay source-compatible; it is used solely to make a failed post-probe cleanup
// observable, which is never fatal to the verdict.
func ProbeMetaCanPlace(ctx context.Context, js jetstream.JetStream, targetReplicas int, logger ...*slog.Logger) error {
	if js == nil {
		return fmt.Errorf("jsstream: placement canary: no JetStream client")
	}
	existing, gerr := js.Stream(ctx, PlacementCanaryStreamName)
	if gerr == nil {
		info, ierr := existing.Info(ctx)
		if ierr != nil {
			return fmt.Errorf("jsstream: placement canary: %q already exists and could not be inspected: %w",
				PlacementCanaryStreamName, ierr)
		}
		// ROUND-5 R5-F3: emptiness is about MESSAGES, and a stream with no messages can still carry durable
		// consumers — deleting the stream would delete them too. Ownership is the marker; the message and
		// consumer counts are a second, independent refusal to destroy anything that is in use.
		if !isOwnPlacementCanary(info.Config) || info.State.Msgs > 0 || info.State.Consumers > 0 {
			return fmt.Errorf("jsstream: placement canary: the stream name %q is held by a stream this probe "+
				"does not own (%d msg(s), %d consumer(s)) — it will not be touched; remove or rename it to "+
				"re-enable the placement probe",
				PlacementCanaryStreamName, info.State.Msgs, info.State.Consumers)
		}
		// ROUND-5 R5-F4: a failed reclaim must NOT fall through to the create. CreateStream is idempotent
		// for an identical config, so it would hand back this stale object and the probe would report a
		// placement it never performed.
		if derr := js.DeleteStream(ctx, PlacementCanaryStreamName); derr != nil {
			return fmt.Errorf("jsstream: placement canary: could not reclaim this probe's own abandoned "+
				"canary %q, so a fresh placement cannot be distinguished from the stale one: %w",
				PlacementCanaryStreamName, derr)
		}
	} else if !errors.Is(gerr, jetstream.ErrStreamNotFound) {
		// R6-F3: UNKNOWN is not ABSENT. Continuing after a lookup timeout/transport/permission error lets
		// CreateStream's identical-config idempotency hand back an old marked stream, bypassing the state
		// checks above and turning cleanup into deletion of an object we never proved was disposable.
		return fmt.Errorf("jsstream: placement canary: could not determine whether %q already exists; "+
			"refusing to create or delete anything while ownership/state is unknown: %w",
			PlacementCanaryStreamName, gerr)
	}
	cfg := jetstream.StreamConfig{
		Name:      PlacementCanaryStreamName,
		Subjects:  []string{placementCanarySubject},
		Retention: jetstream.LimitsPolicy,
		MaxMsgs:   1,
		MaxAge:    time.Minute,
		Storage:   jetstream.MemoryStorage, // nothing is ever published to it; never touch the disk budget
		Replicas:  targetReplicas,
		Metadata:  map[string]string{placementCanaryOwnerKey: placementCanaryOwnerVal},
	}
	created, err := js.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("jsstream: the JetStream meta could not place a new R=%d asset: %w", targetReplicas, err)
	}
	// R5-F4: verify what came back is the object we asked for, at the factor we asked for. The gate's
	// contract is "a new R=N asset was placeable JUST NOW", so a lookup that merely found something must
	// never be reported as a placement.
	info, ierr := created.Info(ctx)
	if ierr != nil {
		return fmt.Errorf("jsstream: placement canary: created but could not be verified: %w", ierr)
	}
	if !isOwnPlacementCanary(info.Config) || info.Config.Replicas != targetReplicas {
		return fmt.Errorf("jsstream: placement canary: the stream that came back is not the R=%d canary this "+
			"probe asked for (replicas=%d) — treating placement as UNPROVEN",
			targetReplicas, info.Config.Replicas)
	}
	if info.State.Msgs > 0 || info.State.Consumers > 0 {
		// CreateStream may have returned an existing identical config after a race, and external use may
		// begin between create and verification. Message/consumer state is an independent destructive
		// guard at BOTH deletion points, not a property implied by our marker.
		return fmt.Errorf("jsstream: placement canary: the R=%d stream returned by create is already in use "+
			"(%d msg(s), %d consumer(s)); refusing destructive cleanup and treating placement as UNPROVEN",
			targetReplicas, info.State.Msgs, info.State.Consumers)
	}
	if derr := js.DeleteStream(ctx, PlacementCanaryStreamName); derr != nil && len(logger) > 0 && logger[0] != nil {
		// Not fatal: the placement WAS proven. But it must be observable, or a canary that keeps failing to
		// clean up looks identical to one that never existed.
		logger[0].Warn("jsstream: could not delete the placement canary after a successful probe — the next "+
			"probe reclaims it, but a persistent failure here needs attention",
			"err", derr, "stream", PlacementCanaryStreamName)
	}
	return nil
}
