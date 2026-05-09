// Package broker is the in-process tether daemon that backs `tether serve`.
// It owns three responsibilities:
//
//  1. Node lifecycle: subscribes to register/heartbeat and drives the
//     state machine (ONLINE → STALE → OFFLINE) via internal/node.
//  2. Session management: subscribes to ctrl.by.<actor>.session.* and
//     applies CRUD via internal/session.
//  3. Authorization: when Config.AuthCallout is non-nil, runs a NATS
//     auth_callout service (issues per-connection user JWTs based on
//     CLI role + PIN). Without it, the broker is a pure P2 anonymous
//     hub — agent registers and heartbeat still work.
//
// `cmd.*.req.forwarded` (architecture C.4) command-routing lands in P4.
package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Config wires the broker to its dependencies. Everything has a sane default
// so tests can override individual pieces (timing, clock, log sink) without
// rebuilding the whole struct.
type Config struct {
	// NATSURL is the broker's connection string (e.g. "nats://127.0.0.1:4222").
	NATSURL string

	// DB is an already-opened SQLite handle (storage.Open).
	DB *sql.DB

	// Logger receives info-level lifecycle events. Defaults to slog.Default.
	Logger *slog.Logger

	// Now is injected for deterministic time in tests. Defaults to time.Now.
	Now func() time.Time

	// ReconcileInterval is how often the state-machine ticker fires.
	// Defaults to 1 second.
	ReconcileInterval time.Duration

	// StaleAfter / OfflineAfter override the node state-machine thresholds
	// (architecture D.2). Defaults: 5s / 60s.
	StaleAfter   time.Duration
	OfflineAfter time.Duration

	// AuthCallout, if non-nil, makes the broker connect with the configured
	// nkey credentials AND subscribe to $SYS.REQ.USER.AUTH to issue per-
	// connection user JWTs. Required for the architecture B.2 NATS-level
	// permissions invariant. When nil (P2 default), the broker connects
	// anonymously and no auth_callout is installed.
	AuthCallout *AuthCalloutConfig
}

// Broker holds the running state of one `tether serve` instance.
type Broker struct {
	cfg Config

	// nc is the active NATS connection. Set by Run; nil before Run /
	// after shutdown. Used by helpers (audit pubs) that need to publish
	// outside the request handler scope.
	nc *nats.Conn
}

// publishOnConn pubs through the broker's persistent NATS connection.
// Used by audit / event broadcast helpers that don't sit on a msg.Reply.
func (b *Broker) publishOnConn(subject string, payload []byte) error {
	if b.nc == nil {
		return errBrokerNotConnected
	}
	return b.nc.Publish(subject, payload)
}

var errBrokerNotConnected = errBrokerSentinel("broker: not connected")

type errBrokerSentinel string

func (e errBrokerSentinel) Error() string { return string(e) }

// New validates the config and returns a Broker not yet connected. Run
// performs the actual NATS connect and blocks until ctx is canceled.
func New(cfg Config) (*Broker, error) {
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("broker: NATSURL required")
	}
	if cfg.DB == nil {
		return nil, fmt.Errorf("broker: DB required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = time.Second
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = node.DefaultStaleAfter
	}
	if cfg.OfflineAfter == 0 {
		cfg.OfflineAfter = node.DefaultOfflineAfter
	}
	return &Broker{cfg: cfg}, nil
}

// Run connects to NATS, installs subscriptions, runs the reconcile ticker,
// and blocks until ctx is canceled. The NATS connection is drained on exit.
//
// If cfg.AuthCallout is non-nil, the broker presents its pre-configured
// nkey credentials on CONNECT (so it bypasses auth_callout itself) AND
// subscribes to $SYS.REQ.USER.AUTH to issue per-connection user JWTs.
func (b *Broker) Run(ctx context.Context) error {
	connOpts, err := b.brokerConnectOptions()
	if err != nil {
		return err
	}
	nc, err := nats.Connect(b.cfg.NATSURL, connOpts...)
	if err != nil {
		return fmt.Errorf("broker: NATS connect: %w", err)
	}
	b.nc = nc
	defer func() {
		b.nc = nil
		_ = nc.Drain()
	}()

	if subAuth, err := b.installAuthCallout(nc); err != nil {
		return err
	} else if subAuth != nil {
		defer func() { _ = subAuth.Unsubscribe() }()
	}

	subRegister, err := nc.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		b.handleRegister,
	)
	if err != nil {
		return fmt.Errorf("broker: subscribe register: %w", err)
	}
	defer func() { _ = subRegister.Unsubscribe() }()

	subHB, err := nc.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat",
		b.handleHeartbeat,
	)
	if err != nil {
		return fmt.Errorf("broker: subscribe heartbeat: %w", err)
	}
	defer func() { _ = subHB.Unsubscribe() }()

	// P3 session management subjects.
	for _, ss := range []struct {
		subj    string
		handler nats.MsgHandler
	}{
		{proto.SubjectPrefix + ".ctrl.by.*.session.create.req", b.handleSessionCreate},
		{proto.SubjectPrefix + ".ctrl.by.*.session.list.req", b.handleSessionList},
		{proto.SubjectPrefix + ".ctrl.by.*.session.*.rm.req", b.handleSessionRm},
		// P4 control plane.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.exec.req",
			func(msg *nats.Msg) { b.handleExecReq(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.ps.req", b.handlePsReq},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.started", b.handleProcEvent},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.exit", b.handleProcEvent},
		// P5 PTY control plane.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.run.req",
			func(msg *nats.Msg) { b.handleRunReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.kill.req",
			func(msg *nats.Msg) { b.handleKillReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.pty.*.failed", b.handlePtyFailed},
	} {
		sub, err := nc.Subscribe(ss.subj, ss.handler)
		if err != nil {
			return fmt.Errorf("broker: subscribe %s: %w", ss.subj, err)
		}
		defer func(s *nats.Subscription) { _ = s.Unsubscribe() }(sub)
	}

	b.cfg.Logger.Info("broker: ready",
		"nats", b.cfg.NATSURL,
		"reconcile", b.cfg.ReconcileInterval,
		"stale_after", b.cfg.StaleAfter,
		"offline_after", b.cfg.OfflineAfter,
	)

	ticker := time.NewTicker(b.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.cfg.Logger.Info("broker: shutting down")
			return ctx.Err()
		case <-ticker.C:
			n, err := node.ReconcileStates(b.cfg.DB, b.cfg.Now(),
				b.cfg.StaleAfter, b.cfg.OfflineAfter)
			if err != nil {
				b.cfg.Logger.Warn("broker: reconcile failed", "err", err)
				continue
			}
			if n > 0 {
				b.cfg.Logger.Info("broker: state transitions", "count", n)
			}
		}
	}
}

func (b *Broker) handleRegister(msg *nats.Msg) {
	sid, nid, ok := proto.ParseSidNidFromCtrl(msg.Subject)
	if !ok {
		b.replyErr(msg, "subject_malformed", "cannot parse sid/nid from "+msg.Subject)
		return
	}

	var req proto.NodeRegisterReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyErr(msg, "json_parse", err.Error())
		return
	}

	if req.NID != nid {
		b.replyErr(msg, "nid_mismatch",
			fmt.Sprintf("subject nid=%q, body nid=%q", nid, req.NID))
		return
	}

	if req.ProtoVersion != proto.ProtoVersion {
		b.replyErr(msg, "proto_mismatch",
			fmt.Sprintf("server proto=%d, client proto=%d", proto.ProtoVersion, req.ProtoVersion))
		return
	}

	in := node.RegisterInput{
		SID: sid, NID: nid,
		ProtoVersion:   req.ProtoVersion,
		ReleaseVersion: req.ReleaseVersion,
		OS:             req.OS,
		Arch:           req.Arch,
		BootID:         req.BootID,
	}
	if err := node.Register(b.cfg.DB, in, b.cfg.Now()); err != nil {
		if errors.Is(err, node.ErrSessionMissing) {
			b.replyErr(msg, "session_not_found",
				fmt.Sprintf("session %q does not exist; have an owner run `tether session create %s` first", sid, sid))
			return
		}
		b.replyErr(msg, "store_error", err.Error())
		return
	}

	b.cfg.Logger.Info("broker: node registered",
		"sid", sid, "nid", nid, "release", req.ReleaseVersion)

	payload, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
	if msg.Reply != "" {
		_ = msg.Respond(payload)
	}
}

func (b *Broker) handleHeartbeat(msg *nats.Msg) {
	sid, nid, ok := proto.ParseSidNidFromCtrl(msg.Subject)
	if !ok {
		return
	}
	if err := node.Heartbeat(b.cfg.DB, sid, nid, b.cfg.Now()); err != nil {
		// Heartbeat from an unregistered node — drop quietly. A real network
		// will see this on broker restart before re-register has happened.
		b.cfg.Logger.Debug("broker: heartbeat for unknown node",
			"sid", sid, "nid", nid, "err", err)
	}
}

func (b *Broker) replyErr(msg *nats.Msg, code, message string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.NodeRegisterResp{
		OK: false, Code: code, Error: message,
	})
	_ = msg.Respond(payload)
}
