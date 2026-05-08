package broker

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
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

	b.cfg.Logger.Info("broker: session created",
		"sid", s.SID, "owner_fp", fp, "actor", actor)
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
		isOwner, _ := session.IsOwner(b.cfg.DB, s.SID, fp)
		out = append(out, proto.SessionEntry{
			SID: s.SID, Name: s.Name, OwnerFP: s.OwnerPubkeyFP,
			State: string(s.State), CreatedAt: s.CreatedAt,
			IsOwner: isOwner,
		})
	}
	b.replyJSON(msg, proto.SessionListResp{Sessions: out})
}

// handleSessionRm handles ctrl.by.<actor>.session.<sid>.rm.req.
// Owner-only; tombstones (ACTIVE → DELETING). Stage 2/3 of H.3 land in P7.
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
	b.replyJSON(msg, proto.SessionRmResp{OK: true})
}

// handleSessionJoin handles ctrl.by.<actor>.session.<sid>.join.req.
//
// P3 transitional path for first-time PIN join. P3.5+ replaces this with the
// standard NATS auth_callout flow over $SYS.REQ.USER.AUTH (architecture E.2).
func (b *Broker) handleSessionJoin(msg *nats.Msg) {
	actor, leaf, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.SessionJoinResp{Code: "subject_malformed"})
		return
	}
	parts := strings.Split(leaf, ".")
	if len(parts) != 4 || parts[0] != "session" || parts[2] != "join" || parts[3] != "req" {
		b.replyJSON(msg, proto.SessionJoinResp{Code: "subject_malformed", Error: leaf})
		return
	}
	sid := parts[1]
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.SessionJoinResp{Code: "actor_invalid", Error: err.Error()})
		return
	}
	var req proto.SessionJoinReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyJSON(msg, proto.SessionJoinResp{Code: "json_parse", Error: err.Error()})
		return
	}
	switch err := session.JoinWithPIN(b.cfg.DB, sid, fp, req.PIN, auth.VerifyPIN, b.cfg.Now()); {
	case err == nil:
		isOwner, _ := session.IsOwner(b.cfg.DB, sid, fp)
		b.cfg.Logger.Info("broker: session join", "sid", sid, "fp", fp, "actor", actor)
		b.replyJSON(msg, proto.SessionJoinResp{OK: true, IsOwner: isOwner})
	case errors.Is(err, session.ErrNotFound):
		b.replyJSON(msg, proto.SessionJoinResp{Code: "not_found"})
	case errors.Is(err, session.ErrDeleting):
		b.replyJSON(msg, proto.SessionJoinResp{Code: "deleting"})
	case errors.Is(err, session.ErrInvalidPIN):
		b.replyJSON(msg, proto.SessionJoinResp{Code: "invalid_pin"})
	default:
		b.replyJSON(msg, proto.SessionJoinResp{Code: "store_error", Error: err.Error()})
	}
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
