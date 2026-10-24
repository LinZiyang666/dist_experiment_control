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
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
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

	// ADMISSION, AND IT COMES BEFORE EVERYTHING ELSE.
	//
	// origin: prerelease audit round 2. This handler used to have none at all: a
	// syntactically valid nkey was the entire test, so anybody reachable on the public
	// control plane could name a session, become its owner, and from there mint the
	// activated-member template — and, with the PIN they had just chosen, the AGENT
	// template too. Every argument in this audit of the form "an activated member is an
	// authorized principal" was therefore an argument about the internet.
	//
	// BEFORE HashPIN on purpose. argon2id costs the broker 0.15-0.3s of the single
	// serialized callout goroutine and 64 MiB, and this call site was never charged
	// against the process-wide ceiling that internal/authcallout added for exactly that
	// reason — so an unadmitted caller could burn broker CPU by the request. Refusing
	// first means the work is only ever done for somebody allowed to ask for it.
	//
	// A READ ERROR REFUSES. A broker that cannot read its own policy table must not be
	// mistaken for a broker whose table is empty.
	allowed, aerr := session.MayCreateSession(b.read().SQL(), fp)
	if aerr != nil {
		b.cfg.Logger.Error("broker: cannot read the session-creator allow-list", "err", aerr, "fp", fp)
		b.replyJSON(msg, proto.SessionCreateResp{Code: "store_error", Error: "store_error"})
		return
	}
	if !allowed {
		// Debug, not Warn — origin: prerelease audit increment 2 internal review,
		// admission-enforcement/L9-F8 ≡ red-team/REFUTE-F2. This refusal is reachable by
		// anyone on the internet with a freshly minted nkey and is not rate-limited, so a
		// Warn per attempt is an anonymous log amplifier: the attacker chooses the volume,
		// and the lines that matter get buried under it. The refusal itself is the
		// control; the log line is not.
		b.cfg.Logger.Debug("broker: refused session create from an unadmitted identity",
			"fp", fp, "actor", actor)
		b.replyJSON(msg, proto.SessionCreateResp{
			Code: "not_allowed",
			Error: "this identity may not create sessions on this broker.\n" +
				"  your fingerprint: " + fp + "\n" +
				"  an operator admits it ON THE BROKER HOST with:\n" +
				"      sudo tether admin session-allow " + fp,
		})
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
	if brokerJS(b) != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := jsstream.EnsureHistoryStream(ctx, brokerJS(b), s.SID, jsstream.ReplicasSingle); err != nil {
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
			State: string(s.State), CreatedAt: s.CreatedAt, IsOwner: isOwner,
		})
	}
	b.replyJSON(msg, proto.SessionListResp{Sessions: out})
}

// handleSessionRm handles ctrl.by.<actor>.session.<sid>.rm.req.
// Owner-only. Drives all three H.3 stages inline:
// ① tombstone (ACTIVE → DELETING) → ② DELETE history-<sid> stream
// → ③ cascade-delete dependent SQLite rows + sys.events broadcast.
// removeTombstonedSession (broker/xfer_inflight.go) settles and then
// cascadeSessionRm (broker/audit.go) does ②-③; both are idempotent
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
	// Capture before the role read so delete/recreate cannot turn an old
	// owner's admission into authority over a replacement with another owner.
	// origin: hot-update S8 plan §6.2 / P16
	guard, captureErr := upgradeauthority.Capture(b.read().SQL(), upgradeauthority.Guard{SID: sid})
	owner, err := session.IsOwner(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyJSON(msg, proto.SessionRmResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !owner {
		b.replyJSON(msg, proto.SessionRmResp{Code: "not_owner"})
		return
	}
	// The ordinary body is empty; only the explicit LQ1 abandonment rides it.
	// A malformed body never tombstones: that flag must not be guessed.
	// origin: hot-update S7 plan §6.4 / lead LQ1
	var req proto.SessionRmReq
	if len(msg.Data) != 0 && json.Unmarshal(msg.Data, &req) != nil {
		b.replyJSON(msg, proto.SessionRmResp{Code: "json_parse", Error: "invalid session removal request"})
		return
	}
	// Capture while the original owner's removal is admitted, before settlement.
	// origin: hot-update S8 plan §6.2 / P16
	if captureErr != nil {
		b.replyJSON(msg, proto.SessionRmResp{Code: "store_error", Error: captureErr.Error()})
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
	// That result is also the AUTHORITATIVE state the rest of the removal acts on: a
	// follower's local replica may not have applied the tombstone the leader committed.
	// origin: S7 lead integration decision 3
	err = tombstoneSessionAuthority(b, guard)
	retry := errors.Is(err, session.ErrDeleting)
	switch {
	case errors.Is(err, session.ErrNotFound):
		b.replyJSON(msg, proto.SessionRmResp{Code: "not_found"})
		return
	case err != nil && !retry:
		b.replyJSON(msg, proto.SessionRmResp{Code: "store_error", Error: err.Error()})
		return
	case !retry:
		b.cfg.Logger.Info("broker: session tombstoned",
			"sid", sid, "by_fp", fp, "actor", actor)
	}
	b.replyJSON(msg, removeTombstonedSessionAuthority(b, guard, retry, req.AbandonUnsettledTransfers))
}

// replyJSON marshals v and replies on msg.Reply if set. JSON marshal errors
// for these tetherd-controlled types are programming bugs, not runtime
// errors — drop with a warn log if it ever happens.
func (b *Broker) replyJSON(msg *nats.Msg, v any) {
	if msg.Reply == "" {
		return
	}
	replyJSONValue(b, msg, v)
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
