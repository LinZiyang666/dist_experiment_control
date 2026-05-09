// Package agent is the in-process daemon that backs `tether agent`:
// connects to NATS, sends one `register.req`, then publishes heartbeat at
// a fixed interval until the context is canceled.
//
// Connection-level resilience (architecture C.3) is implemented at two
// layers, both ctx-aware with exponential backoff:
//   1. connectNATS retries the initial CONNECT,
//   2. register retries the request/reply.
//
// Authentication (P4 review F1):
//   - Default path: caller passes Config.Identity (loaded via
//     cli.EnsureAgentIdentity), agent CONNECTs with nats.Nkey + Name
//     "tether-agent:<sid>:<nid>". On the very first CONNECT the operator
//     also passes Config.PIN; broker auth_callout verifies the PIN and
//     binds (sid, nid)→agent_fp in `agent_provisioning`. Subsequent
//     CONNECTs need no PIN — the binding is remembered.
//   - Dev escape hatch: with Config.Identity == nil the agent CONNECTs
//     anonymously (no nkey, no name discriminator). This only works
//     against a broker without auth_callout (P2-style or
//     TETHER_DEV_NO_AUTH-style demo). cmd/tether/agent.go honours
//     TETHER_DEV_NO_AUTH=1 by leaving Identity nil.
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
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Config wires the agent to its dependencies.
type Config struct {
	NATSURL string
	SID     string
	NID     string

	// Identity, when non-nil, makes the agent CONNECT with nats.Nkey +
	// Name "tether-agent:<sid>:<nid>" so the broker auth_callout can
	// bind the nkey to (sid, nid). When nil the agent CONNECTs
	// anonymously — only safe against a broker without auth_callout
	// (TETHER_DEV_NO_AUTH style demo).
	Identity *cli.Identity

	// PIN is presented as `nats.Token(pin)` on every CONNECT in this
	// process lifetime. Required exactly once per (sid, nid) — the
	// first CONNECT binds the agent fp into agent_provisioning. After
	// that the broker accepts re-connects without PIN, but presenting
	// it again is harmless (auth_callout sees existing fp first).
	PIN string

	// Home is the agent's per-machine config root (defaults to
	// cli.DefaultHome()). state.json lives at
	// <Home>/agent/<sid>/state.json (architecture K.1). When empty
	// state persistence is disabled — fine for in-process tests.
	Home string

	// ExposeAdapter, if non-nil, is invoked from the expose /
	// expose-rm forwarded handlers to add or drop frp proxies.
	// Production agents inject the real frp-backed adapter (P6-6);
	// in-process control-plane tests leave it nil so they exercise
	// only the SQLite + state.json path without standing up frp.
	ExposeAdapter ExposeAdapter

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

	// procs tracks live `tether run` PTY sessions by pid. Used by the
	// kill verb to look up the right *pty.Session to signal. The map
	// is populated when fork+exec succeeds (after attach handshake)
	// and pruned right before the agent publishes RunChunk{Kind:exit}.
	procs   map[string]*pty.Session
	procsMu sync.Mutex

	// stateStore persists per-(sid, machine) data — currently the
	// expose port_tokens table — to ~/.tether/agent/<sid>/state.json.
	// Nil when Config.Home is empty (in-process tests).
	stateStore *stateStore
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
	a := &Agent{cfg: cfg, procs: map[string]*pty.Session{}}
	if cfg.Home != "" {
		a.stateStore = newStateStore(cfg.Home, cfg.SID)
	}
	return a, nil
}

// Run is the agent's main loop:
//  1. connect NATS (retried until ctx cancels — see connectNATS),
//  2. register with the broker (retried per `register`),
//  3. subscribe to `cmd.node.<nid>.*.req.forwarded` (P4 exec; P5 will
//     add run/PTY; P6 expose; etc.),
//  4. heartbeat ticker until ctx cancels.
//
// The NATS connection is drained on exit.
func (a *Agent) Run(ctx context.Context) error {
	nc, err := a.connectNATS(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = nc.Drain() }()

	if err := a.register(ctx, nc); err != nil {
		return err
	}
	a.cfg.Logger.Info("agent: registered",
		"sid", a.cfg.SID, "nid", a.cfg.NID,
		"hb_interval", a.cfg.HeartbeatInterval,
	)

	subFwd, err := nc.Subscribe(
		fmt.Sprintf("tether.v1.s.%s.cmd.node.%s.*.req.forwarded", a.cfg.SID, a.cfg.NID),
		func(msg *nats.Msg) { a.dispatchForwarded(nc, msg) },
	)
	if err != nil {
		return fmt.Errorf("agent: subscribe forwarded: %w", err)
	}
	defer func() { _ = subFwd.Unsubscribe() }()

	// Re-establish frp proxies for any port_tokens persisted from a
	// previous run (architecture F.6: agent restart → frpc rebuilds
	// proxies from state.json + present token; broker reconciles via
	// the token_hash already in port_allocations). No-op when state
	// store or adapter is absent.
	a.replayPortsFromState()

	return a.heartbeatLoop(ctx, nc)
}

func (a *Agent) replayPortsFromState() {
	if a.stateStore == nil || a.cfg.ExposeAdapter == nil {
		return
	}
	sf, err := a.stateStore.load()
	if err != nil {
		a.cfg.Logger.Warn("agent: state.json load on boot", "err", err)
		return
	}
	for _, p := range sf.PortTokens {
		if err := a.cfg.ExposeAdapter.AddProxy(p); err != nil {
			a.cfg.Logger.Warn("agent: replay proxy",
				"err", err, "name", p.Name, "port", p.Port)
			continue
		}
		a.cfg.Logger.Info("agent: replayed expose",
			"name", p.Name, "port", p.Port, "local", p.LocalPort)
	}
}

// connectNATS retries nats.Connect on transient failures (server not up
// yet, DNS not yet resolvable, port closed) until ctx is canceled.
//
// Architecture C.3 explicitly requires unbounded connection-level retry.
// nats.MaxReconnects(-1) only covers reconnect after the FIRST successful
// connect; the initial CONNECT itself can still fail-fast with ErrNoServers
// when the server is reachable later but not yet now (the common deployment
// case where nats-server / tetherd / agent are independent processes).
//
// Backoff reuses the same RegisterRetry knobs as register-retry — a single
// pair of dials governs all transient-NATS-interaction backoff in v1.
func (a *Agent) connectNATS(ctx context.Context) (*nats.Conn, error) {
	connOpts, err := a.buildConnOptions()
	if err != nil {
		return nil, err
	}
	backoff := a.cfg.RegisterRetryInitial
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		nc, err := nats.Connect(a.cfg.NATSURL, connOpts...)
		if err == nil {
			if attempt > 1 {
				a.cfg.Logger.Info("agent: NATS connect succeeded after retry",
					"attempts", attempt)
			}
			return nc, nil
		}
		// Auth failures are NOT transient: a wrong PIN, an unprovisioned
		// nid without --pin, or an nkey conflict will all fail every
		// retry forever. Surface the message to the operator so they
		// know what to fix instead of silently flapping.
		if isAuthFailure(err) {
			return nil, fmt.Errorf("agent: NATS auth rejected (%w); supply --pin on first run or verify session/nid", err)
		}
		a.cfg.Logger.Warn("agent: NATS connect failed; retrying",
			"attempt", attempt, "err", err, "next_backoff", backoff)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
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

		if err == nil {
			var resp proto.NodeRegisterResp
			switch {
			case json.Unmarshal(msg.Data, &resp) != nil:
				// Garbled reply — treat as transient (broker bug or
				// concurrent partial deploy). Retry.
				a.cfg.Logger.Warn("agent: register reply parse failed; retrying",
					"attempt", attempt)
			case resp.OK:
				if attempt > 1 {
					a.cfg.Logger.Info("agent: register succeeded after retry",
						"attempts", attempt)
				}
				return nil
			default:
				// Authoritative reject from broker. Don't retry; the operator
				// must fix config (proto, nid uniqueness, etc.).
				return fmt.Errorf("agent: register rejected (code=%s): %s",
					resp.Code, resp.Error)
			}
		} else {
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

// buildConnOptions assembles the nats.Options for this agent. With
// Identity set, signs CONNECT challenges via the loaded nkey and presents
// the auth_callout-aware Name. Without Identity, falls back to anonymous
// CONNECT (P2-style / TETHER_DEV_NO_AUTH demos only).
func (a *Agent) buildConnOptions() ([]nats.Option, error) {
	opts := []nats.Option{nats.MaxReconnects(-1)}

	if a.cfg.Identity == nil {
		// Anonymous fallback: name with `/` separators is intentional
		// — it does NOT match parseRole's `tether-agent:<sid>:<nid>`
		// format, so a misconfigured prod broker (auth_callout ON,
		// Identity nil) fails CONNECT immediately rather than landing
		// on an unintended role.
		opts = append(opts, nats.Name(fmt.Sprintf("tether-agent/%s/%s", a.cfg.SID, a.cfg.NID)))
		return opts, nil
	}

	id := a.cfg.Identity
	seed := append([]byte(nil), id.Seed...)
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("agent: nkey from seed: %w", err)
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
	opts = append(opts,
		nats.Name(cli.AgentName(a.cfg.SID, a.cfg.NID)),
		nats.Nkey(id.PublicKey, sigCB),
	)
	if a.cfg.PIN != "" {
		opts = append(opts, nats.Token(a.cfg.PIN))
	}
	return opts, nil
}

// isAuthFailure detects nats-server auth-rejection messages. These come
// across as plain `error` strings (not typed) — match on the substrings
// nats-server emits for the relevant cases (auth_callout deny, expired
// JWT, account mismatch). We deliberately keep the substring set small;
// anything else is treated as transient and retried.
func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"Authorization Violation",
		"authorization violation",
		"nats: Authorization",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
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
