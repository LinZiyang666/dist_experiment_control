// Package agent is the in-process daemon that backs `tether agent`:
// connects to NATS, sends one `register.req`, then publishes heartbeat at
// a fixed interval until the context is canceled.
//
// Connection-level resilience (architecture C.3) is implemented at two
// layers, both ctx-aware with exponential backoff:
//   1. connectNATS retries the initial CONNECT,
//   2. register retries the request/reply.
//
// Authentication:
//   - Default path: caller passes Config.Identity (loaded via
//     cli.EnsureAgentIdentity), agent CONNECTs with nats.Nkey + Name
//     "tether-agent:<sid>:<nid>". On the very first CONNECT the operator
//     also passes Config.PIN; broker auth_callout verifies the PIN and
//     binds (sid, nid)→agent_fp in `agent_provisioning`. Subsequent
//     CONNECTs need no PIN — the binding is remembered.
//   - Dev escape hatch: with Config.Identity == nil the agent CONNECTs
//     anonymously (no nkey, no name discriminator). This only works
//     against a broker without auth_callout. cmd/tether/agent.go
//     honours TETHER_DEV_NO_AUTH=1 by leaving Identity nil.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/port"
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
	// expose-rm forwarded handlers to add or drop tunnel proxies.
	// Production agents inject TunnelExposeAdapter (yamux-over-TCP
	// to broker, see tunnel_adapter.go); in-process control-plane
	// tests leave it nil so they exercise only the SQLite +
	// state.json path without standing up the tunnel.
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

	// UpgradeURLAllowlist is the agent-side defense-in-depth set
	// of URL prefixes accepted by `tether node upgrade`. Empty →
	// agent uses defaultAgentURLAllowlist (github.com/<org>/
	// tether/releases/). Architecture J.4 § 安全约束 mandates
	// the agent re-checks even though the broker already gated;
	// belt and suspenders against attacker reaching the
	// forwarded subject directly.
	UpgradeURLAllowlist []string

	// UpgradeNoExit, when true, suppresses the os.Exit(0) call at
	// the end of a successful upgrade. Used only by the in-process
	// test harness so a successful upgrade doesn't kill the test
	// runner. Production agents always run with this false.
	UpgradeNoExit bool

	// UpgradeExecutablePath overrides the install target for
	// installNewBinary. Empty (production default) → use
	// os.Executable() so the agent overwrites its own running
	// binary. Tests set this to a sandbox file under t.TempDir()
	// so the upgrade flow doesn't trample the go-test binary
	// itself (a successful overwrite mid-test is silent until the
	// next subprocess fork tries to exec the corrupted binary).
	UpgradeExecutablePath string
}

// procRec is one entry in Agent.procs. Tracks the PTY session plus
// the architecture G.1 PID-reuse triple captured at fork time so the
// next register snapshot can echo (started_at, start_time_ticks) back
// to the broker for verification. OSPID is kept around for the future
// "verify the OS pid is still that triple" path.
type procRec struct {
	sess           *pty.Session
	osPID          int
	startTimeTicks int64
	startedAt      time.Time
}

type Agent struct {
	cfg Config

	// procs tracks live `tether run` PTY sessions by pid. Used by the
	// kill verb to look up the right session to signal AND by the
	// register snapshot to report (PID, started_at, start_time_ticks)
	// per architecture G.1. Populated when fork+exec succeeds (after
	// attach handshake) and pruned right before the agent publishes
	// RunChunk{Kind:exit}.
	procs   map[string]*procRec
	procsMu sync.Mutex

	// stateStore persists per-(sid, machine) data — currently the
	// expose port_tokens table — to ~/.tether/agent/<sid>/state.json.
	// Nil when Config.Home is empty (in-process tests).
	stateStore *stateStore

	// runCtx is set by Run to the agent's "while running" context
	// (cancels on parent ctx OR sys.events agent_evicted). Read by
	// background work that should respect agent shutdown:
	// dispatchForwarded uses it to drop forwarded msgs that arrive
	// after shutdown started; handleUpgradeForwarded uses it to
	// abort an in-flight HTTP download. nil before Run is called.
	runCtx context.Context
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
	a := &Agent{cfg: cfg, procs: map[string]*procRec{}}
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

	resp, err := a.register(ctx, nc)
	if err != nil {
		return err
	}
	a.cfg.Logger.Info("agent: registered",
		"sid", a.cfg.SID, "nid", a.cfg.NID,
		"hb_interval", a.cfg.HeartbeatInterval,
		"reconciled", len(resp.ReconciledProcesses),
		"drop_procs", len(resp.DropProcesses),
		"revoke_ports", len(resp.RevokePorts),
	)

	subFwd, err := nc.Subscribe(
		fmt.Sprintf("tether.v1.s.%s.cmd.node.%s.*.req.forwarded", a.cfg.SID, a.cfg.NID),
		func(msg *nats.Msg) { a.dispatchForwarded(nc, msg) },
	)
	if err != nil {
		return fmt.Errorf("agent: subscribe forwarded: %w", err)
	}
	defer func() { _ = subFwd.Unsubscribe() }()

	// P9 — listen for sys.events{type:agent_evicted, sid, nid}
	// addressed to us so `tether admin evict` takes effect within
	// the architecture P9 1s budget instead of waiting for the
	// next CONNECT to be denied. The handler triggers a graceful
	// shutdown via runCtx cancel; the surrounding ctx still wins
	// any race with parent-canceled paths.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	a.runCtx = runCtx
	subEvict, err := nc.Subscribe(proto.SubjSysEvents, func(msg *nats.Msg) {
		var ev struct {
			Type string `json:"type"`
			SID  string `json:"sid"`
			NID  string `json:"nid"`
		}
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			return
		}
		if ev.Type != "agent_evicted" {
			return
		}
		if ev.SID != a.cfg.SID || ev.NID != a.cfg.NID {
			return
		}
		a.cfg.Logger.Info("agent: evicted by admin", "sid", ev.SID, "nid", ev.NID)
		cancelRun()
	})
	if err != nil {
		return fmt.Errorf("agent: subscribe sys.events: %w", err)
	}
	defer func() { _ = subEvict.Unsubscribe() }()

	// G.1 reply application MUST run before replay so any RevokePorts
	// have already pruned state.json by the time replayPortsFromState
	// reads it. Otherwise we'd re-establish a proxy that the broker
	// just told us to drop.
	a.applyReconciliation(runCtx, resp)

	// Re-establish reverse-TCP proxies for any port_tokens still in
	// state.json (architecture F.6: agent restart → tunnel client
	// rebuilds proxies from state.json + presents token; broker
	// reconciles via the token_hash already in port_allocations).
	// No-op when state store or adapter is absent.
	a.replayPortsFromState()

	return a.heartbeatLoop(runCtx, nc)
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
//
// Returns the parsed NodeRegisterResp on success so the caller can apply
// G.1 reconciliation directives.
func (a *Agent) register(ctx context.Context, nc *nats.Conn) (proto.NodeRegisterResp, error) {
	procs, ports := a.buildLocalSnapshot()
	req := proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            a.cfg.NID,
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		BootID:         readBootID(),
		LocalProcesses: procs,
		LocalPorts:     ports,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return proto.NodeRegisterResp{}, fmt.Errorf("agent: marshal register: %w", err)
	}
	subject := proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID)

	backoff := a.cfg.RegisterRetryInitial
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return proto.NodeRegisterResp{}, err
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
				return resp, nil
			default:
				// Authoritative reject from broker. Don't retry; the operator
				// must fix config (proto, nid uniqueness, etc.).
				return proto.NodeRegisterResp{}, fmt.Errorf("agent: register rejected (code=%s): %s",
					resp.Code, resp.Error)
			}
		} else {
			// Parent ctx canceled mid-request — exit cleanly.
			if ctx.Err() != nil {
				return proto.NodeRegisterResp{}, ctx.Err()
			}
			a.cfg.Logger.Warn("agent: register attempt failed; retrying",
				"attempt", attempt, "err", err, "next_backoff", backoff)
		}

		// Sleep with backoff, but wake immediately on context cancel.
		select {
		case <-ctx.Done():
			return proto.NodeRegisterResp{}, ctx.Err()
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

// buildLocalSnapshot collects the agent's view of "what is live right
// now" for G.1 reconciliation. Procs come from a.procs (the live PTY
// session map; non-PTY exec children are sync and not registered, so
// after a restart they are missing — broker correctly treats those as
// missed-exit). StartedAt + StartTimeTicks make up two thirds of the
// G.1 (boot_id, pid, start_time_ticks) triple; broker compares
// against processes.start_time_ticks for PID-reuse defense. Ports
// come from state.json (raw token → SHA256 hex matches port.HashToken
// so the broker can join on token_hash).
func (a *Agent) buildLocalSnapshot() ([]proto.LocalProcess, []proto.LocalPort) {
	a.procsMu.Lock()
	procs := make([]proto.LocalProcess, 0, len(a.procs))
	for pid, rec := range a.procs {
		procs = append(procs, proto.LocalProcess{
			PID:            pid,
			State:          "running",
			StartedAt:      rec.startedAt,
			StartTimeTicks: rec.startTimeTicks,
		})
	}
	a.procsMu.Unlock()

	var ports []proto.LocalPort
	if a.stateStore != nil {
		sf, err := a.stateStore.load()
		if err != nil {
			a.cfg.Logger.Warn("agent: load state.json for register snapshot", "err", err)
		} else {
			ports = make([]proto.LocalPort, 0, len(sf.PortTokens))
			for _, p := range sf.PortTokens {
				ports = append(ports, proto.LocalPort{
					Port:      p.Port,
					Name:      p.Name,
					LocalPort: p.LocalPort,
					TokenHash: port.HashToken(p.Token),
				})
			}
		}
	}
	return procs, ports
}

// applyReconciliation acts on the directive arrays in the broker's
// register reply. Per architecture G.1:
//   - RevokePorts: tear down the reverse-TCP proxy (if adapter wired)
//     and prune the corresponding state.json row.
//   - DropProcesses: SIGTERM + 5s + SIGKILL escalation. Only PTY
//     sessions are reachable from a.procs; exec children are
//     sync-managed and have already exited (or are reachable only by
//     OS pid, which v1 doesn't track).
func (a *Agent) applyReconciliation(ctx context.Context, resp proto.NodeRegisterResp) {
	if len(resp.RevokePorts) > 0 && a.stateStore != nil {
		sf, err := a.stateStore.load()
		if err != nil {
			a.cfg.Logger.Warn("agent: load state.json for revoke", "err", err)
		} else {
			byPort := map[int]string{}
			for _, p := range sf.PortTokens {
				byPort[p.Port] = p.Name
			}
			for _, port := range resp.RevokePorts {
				name, ok := byPort[port]
				if !ok {
					continue
				}
				if a.cfg.ExposeAdapter != nil {
					if err := a.cfg.ExposeAdapter.RemoveProxy(name, port); err != nil {
						a.cfg.Logger.Warn("agent: revoke remove proxy",
							"err", err, "port", port, "name", name)
					}
				}
				if err := a.stateStore.RemovePort(name); err != nil {
					a.cfg.Logger.Warn("agent: revoke prune state.json",
						"err", err, "name", name)
				}
				a.cfg.Logger.Info("agent: revoked", "port", port, "name", name)
			}
		}
	}

	for _, pid := range resp.DropProcesses {
		a.killOrphanProcess(ctx, pid)
	}
}

// killOrphanProcess sends SIGTERM, waits 5s, then escalates to SIGKILL
// if the session is still registered. Only reachable for PTY sessions
// (those are the only ones tracked in a.procs); v1 has no path to kill
// non-PTY exec children by their broker-assigned ULID after restart.
// In practice DropProcesses on a fresh-start agent is empty (a.procs
// is empty so we never claimed those pids), so this kill path is
// exercised only when a.procs has entries the broker doesn't know
// about — which can't happen in a single-broker deployment but is
// defensible as a "agent connected to the wrong broker by accident".
func (a *Agent) killOrphanProcess(ctx context.Context, pid string) {
	rec, ok := a.lookupProc(pid)
	if !ok {
		return
	}
	a.cfg.Logger.Info("agent: kill orphan", "pid", pid)
	if err := rec.sess.Signal(syscall.SIGTERM); err != nil {
		a.cfg.Logger.Warn("agent: kill orphan SIGTERM", "err", err, "pid", pid)
	}
	// Audit shard 01 F4: the SIGKILL escalation goroutine used to
	// have no ctx, so it survived agent shutdown — meaning it could
	// SIGKILL a freshly-spawned PID-collision in a follow-up test
	// run. Use a select so ctx-cancel exits the goroutine cleanly
	// before the 5s window elapses.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			if r, still := a.lookupProc(pid); still {
				_ = r.sess.Signal(syscall.SIGKILL)
			}
		}
	}()
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

// readStartTimeTicks returns /proc/<pid>/stat field 22 (the kernel-
// stamped boot tick when this process started), the third leg of the
// architecture G.1 PID-reuse triple. Returns 0 on any failure
// (non-Linux, /proc not mounted, pid disappeared between fork and
// read, etc.) — the broker treats 0 as "agent could not capture",
// which falls back to the no-triple-check accept path. Parsing the
// stat line correctly requires honoring the comm (bytes 2..end-of-")")
// because comm can contain spaces or close-parens; we slice from the
// LAST ')' so a process named "weird ) name" parses cleanly.
func readStartTimeTicks(osPID int) (int64, error) {
	if osPID <= 0 {
		return 0, fmt.Errorf("agent: invalid os pid %d", osPID)
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", osPID))
	if err != nil {
		return 0, err
	}
	line := string(b)
	rp := strings.LastIndexByte(line, ')')
	if rp < 0 || rp+1 >= len(line) {
		return 0, fmt.Errorf("agent: malformed /proc/%d/stat", osPID)
	}
	rest := strings.Fields(line[rp+1:])
	// rest[0] is field 3 (state); field 22 is rest[19].
	if len(rest) < 20 {
		return 0, fmt.Errorf("agent: /proc/%d/stat too short (%d fields after comm)",
			osPID, len(rest))
	}
	ticks, err := strconv.ParseInt(rest[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("agent: parse start_time_ticks: %w", err)
	}
	return ticks, nil
}
