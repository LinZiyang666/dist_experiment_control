package broker

import (
	"context"
	"encoding/json"
	"errors"
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

	s, err := session.Create(b.cfg.DB, req.Name, req.Name, fp, pinHash, b.cfg.Now())
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
		if err := jsstream.EnsureHistoryStream(ctx, b.js, s.SID); err != nil {
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
	if err := session.Tombstone(b.cfg.DB, sid, b.cfg.Now()); err != nil {
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
	_ = msg.Respond(payload)
}
