package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// handleSessionCreate handles ctrl.by.<actor>.session.create.req.
//
// Body: {Name, PIN}. Broker hashes PIN, calls session.Create, registers the
// caller as the owner. Reply: SessionCreateResp with sid + created_at.
func (b *Broker) handleSessionCreate(msg *nats.Msg) {
	actor, _, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.SessionCreateResp{Error: "subject_malformed"})
		return
	}
	if err := proto.ValidateActorToken(actor); err != nil {
		b.replyJSON(msg, proto.SessionCreateResp{Error: "actor_invalid: " + err.Error()})
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.SessionCreateResp{Error: "actor_decode: " + err.Error()})
		return
	}

	var req proto.SessionCreateReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyJSON(msg, proto.SessionCreateResp{Error: "json_parse: " + err.Error()})
		return
	}
	if req.Name == "" {
		b.replyJSON(msg, proto.SessionCreateResp{Error: "name_required"})
		return
	}
	pinHash, err := auth.HashPIN(req.PIN)
	if err != nil {
		b.replyJSON(msg, proto.SessionCreateResp{Error: "pin_invalid: " + err.Error()})
		return
	}

	// D9 §3 (audit #9): in cluster mode this routes through raft (Propose on the leader /
	// forward on a follower) + a read-back; single mode is the byte-identical direct mutator.
	s, err := b.createSession(req.Name, fp, pinHash)
	switch {
	case errors.Is(err, session.ErrAlreadyExists):
		b.replyJSON(msg, proto.SessionCreateResp{Error: "already_exists"})
		return
	case err != nil:
		b.replyJSON(msg, proto.SessionCreateResp{Error: err.Error()})
		return
	}

	// P7 / H.1: a brand-new session needs its own history stream
	// before any audit pub can land. Best-effort: if EnsureHistory
	// fails the session itself is still created (SQLite committed)
	// — the boot reconciler will retry on next broker start.
	if b.js != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := jsstream.EnsureHistoryStream(ctx, b.js, s.SID, jsstream.ReplicasSingle); err != nil {
			b.cfg.Logger.Warn("broker: ensure history stream on create",
				"sid", s.SID, "err", err)
		}
		cancel()
	}

	b.cfg.Logger.Info("broker: session created",
		"sid", s.SID, "owner_fp", fp, "actor", actor)
	b.pubSysEvent("session_created", map[string]any{
		"sid": s.SID, "owner_fp": fp, "actor": actor,
	})
	b.replyJSON(msg, proto.SessionCreateResp{
		SID: s.SID, OwnerFP: s.OwnerPubkeyFP, CreatedAt: s.CreatedAt,
	})
}

func (b *Broker) handleSessionList(msg *nats.Msg) {
	actor, _, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.SessionListResp{Error: "subject_malformed"})
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.SessionListResp{Error: err.Error()})
		return
	}
	sessions, err := session.ListVisible(b.cfg.DB, fp)
	if err != nil {
		b.replyJSON(msg, proto.SessionListResp{Error: "store_error: " + err.Error()})
		return
	}
	out := make([]proto.SessionEntry, 0, len(sessions))
	for _, s := range sessions {
		// Audit shard 01 F11: previous code swallowed IsOwner err
		// silently → if any per-row lookup fails, the response shows
		// is_owner=false everywhere (operator thinks they own
		// nothing). Surface the error and abort the list response
		// so the user retries instead of acting on bad data.
		isOwner, err := session.IsOwner(b.cfg.DB, s.SID, fp)
		if err != nil {
			b.replyJSON(msg, proto.SessionListResp{Error: "store_error: " + err.Error()})
			return
		}
		out = append(out, proto.SessionEntry{
			SID: s.SID, Name: s.Name, OwnerFP: s.OwnerPubkeyFP,
			State: string(s.State), CreatedAt: s.CreatedAt,
			IsOwner: isOwner,
		})
	}
	b.replyJSON(msg, proto.SessionListResp{Sessions: out})
}

// handleSessionRm handles ctrl.by.<actor>.session.<sid>.rm.req.
// Owner-only. Drives all three H.3 stages inline:
// ① tombstone (ACTIVE → DELETING) → ② DELETE history-<sid> stream
// → ③ cascade-delete dependent SQLite rows + sys.events broadcast.
// finalizeSessionRm (broker/audit.go) does ②-③ and is idempotent
// so a crash mid-finalize re-runs cleanly on next broker boot.
func (b *Broker) handleSessionRm(msg *nats.Msg) {
	actor, leaf, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.SessionRmResp{Code: "subject_malformed"})
		return
	}
	parts := strings.Split(leaf, ".")
	// "session.<sid>.rm.req"
	if len(parts) != 4 || parts[0] != "session" || parts[2] != "rm" || parts[3] != "req" {
		b.replyJSON(msg, proto.SessionRmResp{Code: "subject_malformed", Error: leaf})
		return
	}
	sid := parts[1]
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.SessionRmResp{Code: "actor_invalid", Error: err.Error()})
		return
	}
	owner, err := session.IsOwner(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyJSON(msg, proto.SessionRmResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !owner {
		b.replyJSON(msg, proto.SessionRmResp{Code: "not_owner"})
		return
	}
	// Phase ① — tombstone (architecture H.3). After this commits, C.1
	// §6 starts rejecting new req on this sid (handleExecReq /
	// handleRunReq / handleExposeReq all consult IsActive). Phases
	// ②③④ run synchronously below; on failure session stays in
	// DELETING and the boot reconciler resumes from where we left.
	// D9: route the tombstone through raft in cluster mode (the leader bakes deleting_at;
	// a follower forwards). errors.Is still classifies ErrNotFound/ErrDeleting across the
	// wire (ForwardBusinessError.Is maps the typed kinds); single mode is byte-identical.
	if err := b.tombstoneSession(sid); err != nil {
		switch {
		case errors.Is(err, session.ErrNotFound):
			b.replyJSON(msg, proto.SessionRmResp{Code: "not_found"})
		case errors.Is(err, session.ErrDeleting):
			b.replyJSON(msg, proto.SessionRmResp{Code: "already_deleting"})
		default:
			b.replyJSON(msg, proto.SessionRmResp{Code: "store_error", Error: err.Error()})
		}
		return
	}
	b.cfg.Logger.Info("broker: session tombstoned",
		"sid", sid, "by_fp", fp, "actor", actor)

	// Phases ②③④. Failures here are NON-fatal to the rm — the
	// session is in DELETING, ctl gets OK, and the boot reconciler
	// will retry on next broker start. We still log loudly because
	// it usually means the JS server is down.
	//
	// Audit shard 01 F7: derive from b.runCtx (broker shutdown
	// context) instead of context.Background so a graceful
	// shutdown mid-rm cancels the JS+SQLite cascade instead of
	// running it against a half-torn-down broker. Falls back to
	// Background when runCtx is nil (broker.New + finalize tests
	// that bypass Run).
	parent := b.runCtx
	if parent == nil {
		parent = context.Background()
	}
	finalCtx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := b.finalizeSessionRm(finalCtx, sid); err != nil {
		b.cfg.Logger.Warn("broker: session rm finalize failed; will resume on next boot",
			"sid", sid, "err", err)
	}
	b.replyJSON(msg, proto.SessionRmResp{OK: true})
}

// replyJSON marshals v and replies on msg.Reply if set. JSON marshal errors
// for these tetherd-controlled types are programming bugs, not runtime
// errors — drop with a warn log if it ever happens.
func (b *Broker) replyJSON(msg *nats.Msg, v any) {
	if msg.Reply == "" {
		return
	}
	payload, err := json.Marshal(v)
	if err != nil {
		b.cfg.Logger.Warn("broker: marshal reply", "err", err, "subject", msg.Subject)
		return
	}
	b.respondBytes(msg, payload)
}

// respondBytes is the ONE egress point for broker control-plane replies
// (h1 A2). Every reply — via replyJSON, via the reply*Err helpers, or a raw
// site — must leave through here so a failed Respond can never again be
// silent: the pre-h1 shape was `_ = msg.Respond(payload)`, which swallowed
// ErrMaxPayload for five days while every `tether ps` timed out against a
// broker that was answering (2026-08-04 incident; docs/reviews/h1-plan.md A2).
// internal/broker/reply_egress_test.go pins the census of .Respond( call
// sites so a new bare site goes red.
func (b *Broker) respondBytes(msg *nats.Msg, payload []byte) {
	respondLogged(b.cfg.Logger, b.nc.Load(), msg, payload)
}

// respondLogged is respondBytes' package-level body, split out because a
// handful of reply sites live in FREE subscriber functions
// (SubscribeClusterHealth, SubscribeAlertAck, SubscribeClusterApply, …) that
// have no *Broker receiver — they take a logger parameter and call this
// directly. nc is only consulted for the advertised MaxPayload in the
// fallback text (nil → -1); logger is nil-tolerant so test wiring can pass
// nil — the SEND semantics never depend on either being present.
//
// Failure handling:
//   - ErrMaxPayload → ERROR log with subject + byte count, then a
//     bounded-small typed fallback {code: reply_too_large} on the same
//     reply inbox. The fallback is built from proto.ReplyTooLarge (~200B,
//     pinned ≤512B by test) and sent with a
//     direct Respond — structurally non-recursive; if IT fails too, log and
//     return.
//   - conn closed / draining → WARN, not ERROR: broker teardown races every
//     in-flight handler and must not read as an error burst in the last log
//     lines before exit.
//   - anything else → ERROR log. There is no retry here: control replies are
//     request-scoped, the requester's own timeout is the retry boundary.
func respondLogged(logger *slog.Logger, nc *nats.Conn, msg *nats.Msg, payload []byte) {
	if msg.Reply == "" {
		return
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	err := msg.Respond(payload)
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, nats.ErrMaxPayload):
		maxPayload := int64(-1) // -1 = conn handle absent, advertised limit unknown
		if nc != nil {
			maxPayload = nc.MaxPayload()
		}
		logger.Error("broker: reply exceeds max_payload — sending reply_too_large fallback",
			"subject", msg.Subject, "reply_bytes", len(payload), "max_payload", maxPayload)
		fallback, merr := json.Marshal(proto.ReplyTooLarge{
			Code: proto.CodeReplyTooLarge,
			Error: fmt.Sprintf("%d-byte reply exceeds server max_payload %d; this is a tether bug (every reply section is bounded) — report it",
				len(payload), maxPayload),
		})
		if merr != nil {
			logger.Error("broker: marshal reply_too_large fallback", "err", merr, "subject", msg.Subject)
			return
		}
		if rerr := msg.Respond(fallback); rerr != nil {
			logger.Error("broker: reply_too_large fallback send failed",
				"err", rerr, "subject", msg.Subject)
		}
	case errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionDraining):
		logger.Warn("broker: reply dropped on closing conn",
			"err", err, "subject", msg.Subject, "reply_bytes", len(payload))
	default:
		logger.Error("broker: reply send failed",
			"err", err, "subject", msg.Subject, "reply_bytes", len(payload))
	}
}
