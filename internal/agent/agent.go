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

	// RegisterTimeout bounds each individual register request/reply round-trip.
	// Defaults to 10 seconds. The agent retries on transient failures (no
	// responders / NATS reconnect / per-attempt timeout) until the parent
	// context is canceled — this timeout governs ONE attempt, not the whole
	// boot.
	RegisterTimeout time.Duration

	// RegisterRetryInitial is the first inter-attempt backoff after a failed
	// register attempt. Defaults to 100ms. Each subsequent failure doubles
	// the backoff up to RegisterRetryMax.
	RegisterRetryInitial time.Duration

	// RegisterRetryMax caps the inter-attempt backoff. Defaults to 2s.
	RegisterRetryMax time.Duration
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
	if cfg.RegisterRetryInitial == 0 {
		cfg.RegisterRetryInitial = 100 * time.Millisecond
	}
	if cfg.RegisterRetryMax == 0 {
		cfg.RegisterRetryMax = 2 * time.Second
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

// register loops, retrying on transient failures (no responders / per-attempt
// timeout / NATS reconnect), until either the broker accepts the registration
// or it returns an explicit OK=false rejection.
//
// Why retry: in deployment, nats-server, tetherd, and the agent are three
// separate processes started independently. NATS is often reachable before
// tetherd has installed its register subscription, in which case nc.Request
// returns ErrNoResponders. Treating that as fatal makes the agent flap on
// every broker restart; retry makes startup ordering moot.
//
// Permanent rejections (proto_mismatch, nid_mismatch, store_error) come back
// as a real reply with OK=false — those are configuration / deployment bugs
// no amount of retry will fix, so they bubble up immediately.
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
	subject := proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID)

	backoff := a.cfg.RegisterRetryInitial
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		reqCtx, cancel := context.WithTimeout(ctx, a.cfg.RegisterTimeout)
		msg, err := nc.RequestWithContext(reqCtx, subject, payload)
		cancel()

		switch {
		case err == nil:
			var resp proto.NodeRegisterResp
			if jerr := json.Unmarshal(msg.Data, &resp); jerr != nil {
				// Garbled reply — treat as transient (broker bug or
				// concurrent partial deploy). Retry.
				a.cfg.Logger.Warn("agent: register reply parse failed; retrying",
					"attempt", attempt, "err", jerr)
			} else if resp.OK {
				if attempt > 1 {
					a.cfg.Logger.Info("agent: register succeeded after retry",
						"attempts", attempt)
				}
				return nil
			} else {
				// Authoritative reject from broker. Don't retry; the operator
				// must fix config (proto, nid uniqueness, etc.).
				return fmt.Errorf("agent: register rejected (code=%s): %s",
					resp.Code, resp.Error)
			}

		default:
			// Parent ctx canceled mid-request — exit cleanly.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.cfg.Logger.Warn("agent: register attempt failed; retrying",
				"attempt", attempt, "err", err, "next_backoff", backoff)
		}

		// Sleep with backoff, but wake immediately on context cancel.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < a.cfg.RegisterRetryMax {
			backoff *= 2
			if backoff > a.cfg.RegisterRetryMax {
				backoff = a.cfg.RegisterRetryMax
			}
		}
	}
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
