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
func EnsureEventsStream(ctx context.Context, js jetstream.JetStream) error {
	cfg := jetstream.StreamConfig{
		Name:      EventsStreamName,
		Subjects:  []string{proto.SubjSysEvents},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    30 * 24 * time.Hour,
		MaxBytes:  1 << 30, // 1 GiB
		Discard:   jetstream.DiscardOld,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	}
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

func EnsureHistoryStream(ctx context.Context, js jetstream.JetStream, sid string) error {
	cfg := jetstream.StreamConfig{
		Name:      HistoryStreamName(sid),
		Subjects:  []string{historyFilterSubject(sid)},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    0, // 0 / -1 both mean "no expiry" in nats; use 0 to be explicit
		MaxBytes:  historyMaxBytesPerSession,
		Discard:   jetstream.DiscardNew,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
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

// ensureStream is the create-or-update helper. We intentionally do
// NOT call UpdateStream when the stream already exists — operators
// who need to widen retention should use `nats stream edit`; an
// auto-mutate could surprise an operator who's pinned a different
// limit. CreateStream returning "name already in use" is the
// signal we use to know it exists.
func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	_, err := js.CreateStream(ctx, cfg)
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return nil
	}
	return fmt.Errorf("jsstream: create %s: %w", cfg.Name, err)
}
