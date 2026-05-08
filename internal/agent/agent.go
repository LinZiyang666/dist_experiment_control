// Package agent is the in-process daemon that backs `tether agent`: connects
// to NATS, sends one `register.req`, then publishes heartbeat at a fixed
// interval until the context is canceled.
//
// P2 scope: no auth nkey yet (P3), no managed-process RPCs (P4+). The
// register payload carries proto/release version + nid + os/arch + boot_id.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Config wires the agent to its dependencies.
type Config struct {
	NATSURL string
	SID     string
	NID     string

	Logger *slog.Logger

	// HeartbeatInterval defaults to 5 seconds (architecture / requirements §6.5).
	HeartbeatInterval time.Duration

	// RegisterTimeout bounds the initial request/reply round-trip.
	// Defaults to 10 seconds.
	RegisterTimeout time.Duration
}

type Agent struct {
	cfg Config
}

// New validates the config and returns an Agent not yet connected. Run
// performs the actual NATS connect and blocks until ctx is canceled.
func New(cfg Config) (*Agent, error) {
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("agent: NATSURL required")
	}
	if err := proto.ValidateSID(cfg.SID); err != nil {
		return nil, fmt.Errorf("agent: %w", err)
	}
	if err := proto.ValidateNID(cfg.NID); err != nil {
		return nil, fmt.Errorf("agent: %w", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.RegisterTimeout == 0 {
		cfg.RegisterTimeout = 10 * time.Second
	}
	return &Agent{cfg: cfg}, nil
}

// Run connects to NATS, performs one register round-trip, then publishes
// heartbeat on a ticker until ctx is canceled. The NATS connection is
// drained on exit.
func (a *Agent) Run(ctx context.Context) error {
	nc, err := nats.Connect(a.cfg.NATSURL,
		nats.Name(fmt.Sprintf("tether-agent/%s/%s", a.cfg.SID, a.cfg.NID)),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return fmt.Errorf("agent: NATS connect: %w", err)
	}
	defer func() { _ = nc.Drain() }()

	if err := a.register(ctx, nc); err != nil {
		return err
	}
	a.cfg.Logger.Info("agent: registered",
		"sid", a.cfg.SID, "nid", a.cfg.NID,
		"hb_interval", a.cfg.HeartbeatInterval,
	)

	return a.heartbeatLoop(ctx, nc)
}

func (a *Agent) register(ctx context.Context, nc *nats.Conn) error {
	req := proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            a.cfg.NID,
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		BootID:         readBootID(),
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("agent: marshal register: %w", err)
	}

	// Use ctx-bounded request so cancel during boot exits cleanly.
	reqCtx, cancel := context.WithTimeout(ctx, a.cfg.RegisterTimeout)
	defer cancel()

	msg, err := nc.RequestWithContext(reqCtx,
		proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID), payload)
	if err != nil {
		return fmt.Errorf("agent: register request: %w", err)
	}

	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return fmt.Errorf("agent: register reply parse: %w", err)
	}
	if !resp.OK {
		return fmt.Errorf("agent: register rejected (code=%s): %s", resp.Code, resp.Error)
	}
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context, nc *nats.Conn) error {
	subject := proto.SubjNodeHeartbeat(a.cfg.SID, a.cfg.NID)
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.cfg.Logger.Info("agent: shutting down")
			return ctx.Err()
		case t := <-ticker.C:
			payload, _ := json.Marshal(proto.HeartbeatPayload{Ts: t.UTC()})
			if err := nc.Publish(subject, payload); err != nil {
				a.cfg.Logger.Warn("agent: heartbeat publish", "err", err)
			}
		}
	}
}

// readBootID returns the Linux per-boot UUID, or "" on non-Linux / error.
// Used in P8's reconciliation (architecture G.1) for PID-reuse detection;
// recorded already in P2 so the column is never NULL once populated.
func readBootID() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
