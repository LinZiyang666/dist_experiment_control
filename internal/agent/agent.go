// Package agent is the in-process daemon that backs `tether agent`:
// connects to NATS, sends one `register.req`, then publishes heartbeat at
// a fixed interval until the context is canceled.
//
// Connection-level resilience (architecture C.3) is implemented at two
// layers, both ctx-aware with exponential backoff:
//  1. connectNATS retries the initial CONNECT,
//  2. register retries the request/reply.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	// ProxyFailClosedGrace (P13) is how long the agent may stay partitioned
	// from NATS while still serving the embedded proxy before it proactively
	// tears the SS server down (fail-closed). Defaults to 15min, aligned with
	// the broker's OFFLINE→port-REVOKE threshold. Tests override down.
	ProxyFailClosedGrace time.Duration

	// AllowRoots narrows `tether push` / `tether pull` to an allow-list of
	// absolute roots. Together with RootsConfigured it selects the agent's
	// transferMode (see resolveTransferMode in transfer.go):
	//
	//   - RootsConfigured==false (yaml key absent/null) → OPEN: whole-FS
	//     reach, equal to run/exec. The default, including fresh installs.
	//   - non-empty AllowRoots → NARROW: push/pull confined to these roots
	//     (path_outside_roots on miss).
	//   - RootsConfigured==true with empty AllowRoots (explicit
	//     `allow_roots: []`) → DISABLED: every push/pull → transfer_disabled.
	//
	// allow_roots is an optional convenience narrowing, NOT a security
	// boundary: a session member already has unrestricted run/exec on every
	// node (requirements.md §9.3), so it can reach any path regardless.
	// Mechanism hardening (EvalSymlinks-of-parent, leaf-symlink reject,
	// O_NOFOLLOW, dev+inode TOCTOU) runs in ALL modes — it defends a
	// hostile NON-member process on the host, not the member. See the
	// path-validation half of internal/agent/transfer.go.
	AllowRoots []string

	// RootsConfigured is true iff the operator wrote a
	// file_transfer.allow_roots key in agent.yaml (yaml.v3 leaves AllowRoots
	// nil when the key is absent/null). It is the discriminator between OPEN
	// (key absent) and DISABLED (explicit empty list); see
	// resolveTransferMode. cmd/tether sets it from
	// `ay.FileTransfer.AllowRoots != nil`.
	RootsConfigured bool

	// ProxyAllowPrivateDestinations (P13 round-6 F12) opts the embedded SS proxy
	// OUT of the default internet-egress-only destination policy, permitting a
	// subscription to reach loopback / private / link-local / metadata addresses
	// on this agent. Default false (deny). Set only for deployments that
	// intentionally expose private-network access.
	ProxyAllowPrivateDestinations bool

	// RemoteFS* configure hung-network-filesystem-safe spawn for exec/run
	// (docs/reviews/remote-fs-resilience-plan.md). When a network mount backing
	// $PATH/argv[0]/cwd is wedged in D-state, exec/run would otherwise hang at
	// LookPath/execve; the policy pre-resolves argv[0] against a sanitized PATH
	// and fails fast instead.
	RemoteFSMode         string        // "auto" (default) | "off"; empty ⇒ "auto". Validated in New.
	RemoteFSSafeDir      string        // optional local safe-dir override; empty ⇒ os.TempDir().
	RemoteFSProbeTimeout time.Duration // 0 ⇒ spawnsafe.DefaultProbeTimeout.
	RemoteFSSpawnTimeout time.Duration // execve start-window deadline; 0 ⇒ defaultRemoteFSSpawnTimeout.
	RemoteFSWedgeCeiling int           // max concurrent abandoned spawns; 0 ⇒ spawnsafe.DefaultWedgeCeiling.

	// Seams for hermetic tests (nil ⇒ real /proc/self/mountinfo + statfs).
	// Mirror ExposeAdapter: in-process tests inject deterministic mount
	// tables + probes so the remote-fs path runs with no real NFS.
	RemoteFSMountSource spawnsafe.MountSource
	RemoteFSProbe       spawnsafe.ProbeFn
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

	// js is the JetStream context the agent uses for file-transfer
	// Tier-B ObjectStore Put/Get. nil when the underlying nats-server
	// has no JetStream — in that case push/pull tier-B handlers reply
	// `jetstream_unavailable` and tier-A continues to work.
	js jetstream.JetStream

	// pushCommitCache remembers tier-B prep state between
	// push.req.forwarded (where we validate + reply OK) and
	// push-commit.req.forwarded (where we actually Get bytes). Keyed
	// by transfer_id; rebuilt fresh on agent restart so an in-flight
	// transfer across a restart fails cleanly with transfer_unknown
	// (the broker's watchdog catches it and writes audit failed).
	pushCommitCache map[string]pushCommitEntry
	pushCacheMu     sync.Mutex

	// canonAllowRoots is the EvalSymlinks-resolved version of
	// cfg.AllowRoots, computed once at New() and re-used on every
	// push/pull. Audit shard P11 F3 — previously each handler
	// re-canonicalized, costing O(allow_roots) syscalls per request.
	canonAllowRoots []string

	// transferMode is the resolved push/pull path policy (open / narrow /
	// disabled), computed once at New() from cfg.RootsConfigured + the RAW
	// cfg.AllowRoots length. See resolveTransferMode — it is deliberately
	// NOT derived from len(canonAllowRoots), so an all-dropped narrow
	// config fails closed instead of falling open.
	transferMode transferMode

	// proxy is the P13 embedded SS proxy runtime (lazily created on the
	// first directive). nil-safe; methods serialize through its own mutex.
	proxy *proxyRuntime

	// spawnPolicy is the hung-network-filesystem-safe spawn engine for
	// exec/run (docs/reviews/remote-fs-resilience-plan.md). Always non-nil
	// after New; inert when the machine has no hangable mounts or mode=off.
	spawnPolicy *spawnsafe.Policy

	// homeHangable records, at New time, whether cfg.Home is backed by a
	// hangable network mount. When true the agent guards its own state.json
	// reads (buildLocalSnapshot / applyReconciliation) behind the bounded
	// probe so a wedged Home cannot D-hang the re-register path (Component I).
	homeHangable bool

	// homeReadInFlight (guarded by homeReadMu) is set while a bounded, lock-free
	// Home state.json read is outstanding (possibly D-hung on a wedged mount). It
	// single-flights the bounded read so abandoned readers are bounded to ONE
	// regardless of reconnect count (review B1).
	homeReadMu       sync.Mutex
	homeReadInFlight bool

	// flcTimer is the P13 fail-closed watchdog (armed on NATS disconnect,
	// cancelled on reconnect); flcMu guards it.
	flcMu    sync.Mutex
	flcTimer *time.Timer

	// proxyHandlerWG tracks in-flight proxy-keys forwarded handlers so Run can
	// drain them on shutdown (they write state.json; a late write must not race
	// the agent Home cleanup). F9 / round-2 F4.
	proxyHandlerWG sync.WaitGroup
	// proxyDrainMu guards proxyDraining: once set (Run shutdown), dispatch will
	// not Add a new proxy handler, so proxyHandlerWG.Wait is a sound barrier.
	proxyDrainMu   sync.Mutex
	proxyDraining  bool
	proxyDispatchN int64 // monotonic proxy-keys arrival sequence (assigned in dispatch)

	// proxyApplyMu serializes live keyset-push application and orders it by the
	// arrival sequence; lastAppliedPushSeq is the highest applied (round-2 F2).
	proxyApplyMu       sync.Mutex
	lastAppliedPushSeq int64

	// ncBox holds the current *nats.Conn so the lock-free tunnel session-state
	// hook (fired on a supervisor goroutine, off p.mu/c.mu) can publish proxy
	// ready/unready without threading nc through the tunnel layer. Stored in Run
	// after connectNATS and re-stored in onNATSReconnect (the pointer is stable
	// across nats.go auto-reconnect, but we re-store defensively).
	ncBox atomic.Pointer[nats.Conn]

	// proxyPublicPort is the lock-free mirror of the embedded proxy's public
	// port (0 when not serving). The tunnel hook reads it to FILTER: only the
	// __proxy__ port's up/down transitions may move proxy_ready — a flap on a
	// regular `expose` port must not. Stored before AddProxy in proxyStartLocked,
	// cleared on teardown / fail-cleanup.
	proxyPublicPort atomic.Int64
}

// sessionStateHookSetter is the OPTIONAL capability a production ExposeAdapter
// (TunnelExposeAdapter) implements to receive data-plane up/down transitions.
// The in-process test adapter does not implement it, so the type assertion in
// New simply skips wiring there.
type sessionStateHookSetter interface {
	SetSessionStateHook(fn func(publicPort int, up bool))
}

// onTunnelSessionState is the tunnel session-state hook. It publishes proxy
// ready/unready ONLY for the embedded proxy's public port, so /sub and
// `proxy status` track real data-plane liveness (Defect B fix). Lock-free: it
// takes neither p.mu nor c.mu (AddProxy → tunnel.Open runs under p.mu, so a
// hook needing p.mu would deadlock; lock order is p.mu → c.mu).
func (a *Agent) onTunnelSessionState(publicPort int, up bool) {
	if int64(publicPort) != a.proxyPublicPort.Load() {
		return // not the proxy port — a regular expose flap, ignore
	}
	if nc := a.ncBox.Load(); nc != nil {
		a.pubProxyReady(nc, up)
	}
}

// New validates the config and returns an Agent not yet connected. Run
// performs the actual NATS connect and blocks until ctx is canceled.
func New(cfg Config) (*Agent, error) {
	if cfg.RemoteFSSpawnTimeout < 0 {
		return nil, fmt.Errorf("agent: remote_fs.spawn_timeout: must not be negative")
	}
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
	a := &Agent{
		cfg:             cfg,
		procs:           map[string]*procRec{},
		canonAllowRoots: CanonAllowRoots(cfg.AllowRoots),
		transferMode:    resolveTransferMode(cfg.RootsConfigured, cfg.AllowRoots),
		proxy:           &proxyRuntime{}, // F2: lifetime-owned, created eagerly (no init race)
	}
	if a.transferMode == modeOpen {
		// Posture-change signal: with no allow_roots configured, push/pull
		// now reaches the whole filesystem (matching run/exec). Logged once
		// at startup so an operator upgrading from the old empty==disabled
		// default sees the change at the moment it takes effect.
		cfg.Logger.Warn("file transfer: whole-filesystem reach (no file_transfer.allow_roots configured); " +
			"set file_transfer.allow_roots to narrow, or allow_roots: [] to disable push/pull")
	}
	if cfg.Home != "" {
		a.stateStore = newStateStore(cfg.Home, cfg.SID)
	}

	// Build the hung-network-filesystem-safe spawn policy. Validate the mode
	// up front so a typo'd remote_fs.mode fails loud (matching agent.yaml's
	// KnownFields strictness) instead of silently defaulting.
	mode, err := spawnsafe.ParseMode(cfg.RemoteFSMode)
	if err != nil {
		return nil, fmt.Errorf("agent: %w", err)
	}
	sp, err := spawnsafe.New(spawnsafe.Config{
		Mode:         mode,
		SafeDir:      cfg.RemoteFSSafeDir,
		ProbeTimeout: cfg.RemoteFSProbeTimeout,
		WedgeCeiling: cfg.RemoteFSWedgeCeiling,
		MountSource:  cfg.RemoteFSMountSource,
		Probe:        cfg.RemoteFSProbe,
	})
	if err != nil {
		return nil, fmt.Errorf("agent: %w", err)
	}
	a.spawnPolicy = sp
	// Component I: if the agent's own Home is on a hangable mount, its
	// state.json reads can D-hang the re-register path on every reconnect.
	// Record it (so buildLocalSnapshot can guard those reads) and warn loud.
	if cfg.Home != "" && a.spawnPolicy.IsHangablePath(cfg.Home) {
		a.homeHangable = true
		cfg.Logger.Warn("agent: Home is on a network filesystem; a server outage can stall "+
			"re-register and reconcile. Prefer a local-disk Home (e.g. /srv/local/<user>).",
			"home", cfg.Home)
	}
	// Wire the tunnel data-plane liveness hook (Defect B). The production
	// TunnelExposeAdapter implements sessionStateHookSetter; the in-process
	// test adapter does not, so this is a no-op there.
	if setter, ok := cfg.ExposeAdapter.(sessionStateHookSetter); ok {
		setter.SetSessionStateHook(a.onTunnelSessionState)
	}
	return a, nil
}

// defaultRemoteFSSpawnTimeout bounds the execve start window for a safe-mode
// spawn (NOT the child's runtime). A child that wedges in D-state during
// execve is abandoned after this; a clean-started long-running command is
// never affected. Tunable via agent.yaml remote_fs.spawn_timeout.
const defaultRemoteFSSpawnTimeout = 30 * time.Second

// spawnTimeout returns the configured execve start-window deadline.
func (a *Agent) spawnTimeout() time.Duration {
	if a.cfg.RemoteFSSpawnTimeout > 0 {
		return a.cfg.RemoteFSSpawnTimeout
	}
	return defaultRemoteFSSpawnTimeout
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
	a.ncBox.Store(nc) // publish nc for the lock-free tunnel session-state hook
	defer func() { _ = nc.Drain() }()
	// F9 / round-2 F4: drain in-flight proxy-keys handlers before the connection
	// drains. Set proxyDraining FIRST (under the lock) so dispatch cannot Add a
	// new handler after this point, THEN Wait — a sound barrier (no Add races
	// the Wait). Runs just before nc.Drain (registered right after it).
	defer func() {
		a.proxyDrainMu.Lock()
		a.proxyDraining = true
		a.proxyDrainMu.Unlock()
		a.proxyHandlerWG.Wait()
	}()

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

	// JetStream client for file-transfer Tier-B. `jetstream.New` is
	// pure client-side scaffolding (never errors), so we can stash
	// the handle eagerly. We DON'T probe with AccountInfo here —
	// the agent's perm template intentionally excludes
	// `$JS.API.INFO`, so a probe would log a spurious perm
	// violation on every agent start. Tier-B handlers surface a
	// real JS-absent error via the Put/Get path itself.
	a.js, _ = jetstream.New(nc)

	subFwd, err := nc.Subscribe(
		fmt.Sprintf("tether.v1.s.%s.cmd.node.%s.*.req.forwarded", a.cfg.SID, a.cfg.NID),
		func(msg *nats.Msg) {
			// round-2 F4: count the callback from the moment it STARTS, before
			// dispatchForwarded — so a callback preempted before it spawns a
			// proxy handler is still covered by the shutdown drain barrier
			// (proxyHandlerWG.Wait). The proxy handler takes its own Add inside
			// dispatch; this wrapper bridges the gap. Cheap for non-proxy verbs
			// (dispatchForwarded returns immediately after spawning).
			a.proxyHandlerWG.Add(1)
			defer a.proxyHandlerWG.Done()
			a.dispatchForwarded(nc, msg)
		},
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
	defer a.cancelFailClosed() // stop any armed fail-closed timer on shutdown
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

	// P13: converge the embedded SS proxy to the broker's directive (nil
	// when the session's proxy switch is off → ensures it's torn down).
	a.applyProxyDirective(runCtx, nc, resp.Proxy)

	return a.heartbeatLoop(runCtx, nc)
}

func (a *Agent) replayPortsFromState() {
	if a.stateStore == nil || a.cfg.ExposeAdapter == nil {
		return
	}
	// Bounded read (review B2): this was the lone unguarded state.json read on
	// the boot path; on a wedged hangable Home it would D-hang the whole Run()
	// goroutine before heartbeatLoop starts, sending the node OFFLINE. Degrade
	// (skip replay) instead — proxies re-establish on the next healthy reconnect
	// via the same persisted token path.
	sf, ok := a.loadStateBounded("boot replay")
	if !ok {
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
			return nil, fmt.Errorf("agent: NATS auth_callout rejected (%w)\n"+
				"  the broker accepted the connection but its auth_callout said no. Likely:\n"+
				"    - first run AND PIN is wrong       -> double-check --pin matches session pin\n"+
				"    - first run AND no --pin           -> pass --pin <pin> for the first connect\n"+
				"    - re-run AND nid already bound to a different nkey on broker\n"+
				"      -> on broker host: `sudo tether admin evict %s %s` to clear binding (a `keys/agent.nk` from a different node also triggers this)\n"+
				"    - session does not exist or is being deleted (DELETING state)\n"+
				"      -> on broker host: `sudo tether admin sessions` to check\n"+
				"    - your agent was evicted (see sys.events; check broker.err)\n"+
				"  for the exact deny reason, ask the broker operator to grep /var/log/tether/broker.err for this nkey",
				err, a.cfg.SID, a.cfg.NID)
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
		Capabilities:   []string{proto.CapProxyV1}, // P13: this build implements proxy-v1
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

// loadStateBounded reads state.json. When the agent Home is on a hangable
// network mount (Component I) the read is bounded behind a deadline so a wedged
// fs cannot D-hang the re-register / reconcile path on every NATS reconnect; on
// timeout it returns a degraded (nil, false) result and the next healthy read
// repopulates. When Home is local it reads directly with no added overhead.
// Returns (sf, ok) — ok=false means "no usable state this cycle".
func (a *Agent) loadStateBounded(why string) (*StateFile, bool) {
	if a.stateStore == nil {
		return nil, false
	}
	if !a.homeHangable {
		sf, err := a.stateStore.load()
		if err != nil {
			a.cfg.Logger.Warn("agent: load state.json", "for", why, "err", err)
			return nil, false
		}
		return sf, true
	}
	// Hangable Home: the read goes through loadNoLock (lock-free os.ReadFile,
	// torn-free because writes are atomic-rename), so an abandoned (D-hung) read
	// holds NO stateStore mutex (review B1). It is also SINGLE-FLIGHT: while one
	// read is wedged, later reconnects degrade immediately rather than each
	// spawning another D-goroutine, so abandoned readers are bounded to ONE
	// regardless of reconnect count (review B1: no O(reconnects) pile-up).
	return a.boundedHomeRead(a.stateStore.loadNoLock, why)
}

// boundedHomeRead is the single-flight, deadline-bounded, lock-free read used
// when Home is hangable. Split out (and parameterized on loadFn) so a hermetic
// test can drive it with a blockable fake — review B1 noted the original test
// stubbed the read with a closure that never exercised the mutex/abandon path.
func (a *Agent) boundedHomeRead(loadFn func() (*StateFile, error), why string) (*StateFile, bool) {
	a.homeReadMu.Lock()
	if a.homeReadInFlight {
		a.homeReadMu.Unlock()
		a.cfg.Logger.Warn("agent: state.json read already stalled on a network Home; "+
			"degraded (a prior read is still wedged)", "for", why)
		return nil, false
	}
	a.homeReadInFlight = true
	a.homeReadMu.Unlock()

	type result struct {
		sf  *StateFile
		err error
	}
	ch := make(chan result, 1) // buffered: the abandoned reader exits on fs recovery
	go func() {
		sf, err := loadFn()
		// Clear the in-flight flag BEFORE publishing the result, so a caller that
		// receives the result and immediately issues the next read sees the flag
		// already cleared (and single-flights correctly) rather than spuriously
		// degrading (external review F6, reproduced under -count=1000).
		a.homeReadMu.Lock()
		a.homeReadInFlight = false
		a.homeReadMu.Unlock()
		ch <- result{sf, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			a.cfg.Logger.Warn("agent: load state.json", "for", why, "err", r.err)
			return nil, false
		}
		return r.sf, true
	case <-time.After(a.spawnPolicy.ProbeTimeout()):
		// Abandon. homeReadInFlight stays set until the read returns (recovery),
		// so concurrent/subsequent reconnects degrade instead of piling up.
		a.cfg.Logger.Warn("agent: state.json read stalled on a network Home; "+
			"proceeding with a degraded snapshot (ports reconcile on the next healthy read)", "for", why)
		return nil, false
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
	if sf, ok := a.loadStateBounded("register snapshot"); ok {
		ports = make([]proto.LocalPort, 0, len(sf.PortTokens)+1)
		for _, p := range sf.PortTokens {
			ports = append(ports, proto.LocalPort{
				Port:      p.Port,
				Name:      p.Name,
				LocalPort: p.LocalPort,
				TokenHash: port.HashToken(p.Token),
			})
		}
		// P13: report the embedded-proxy port too, so the broker's
		// register reconcile keeps it (token_hash match) instead of
		// re-minting a token + tunnel on every reconnect.
		if sf.Proxy != nil && sf.Proxy.PublicPort != 0 {
			ports = append(ports, proto.LocalPort{
				Port:      sf.Proxy.PublicPort,
				Name:      proxyTokenName,
				LocalPort: sf.Proxy.LocalPort,
				TokenHash: port.HashToken(sf.Proxy.Token),
			})
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
	if len(resp.RevokePorts) > 0 {
		if sf, ok := a.loadStateBounded("revoke"); ok {
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
			pgen, pepoch := a.proxyGenEpoch()
			payload, _ := json.Marshal(proto.HeartbeatPayload{Ts: t.UTC(), ProxyGeneration: pgen, ProxyEpoch: pepoch})
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
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		// P13 convergence (architecture p13-plan §5 / Critique 4): the agent
		// registers ONCE and heartbeat is fire-and-forget. On a NATS reconnect
		// (the agent process stayed up while its connection bounced) trigger a
		// single re-register so the broker's register reply re-delivers the
		// authoritative proxy directive (and re-runs G.1 reconcile).
		nats.ReconnectHandler(func(nc *nats.Conn) {
			go a.onNATSReconnect(nc)
		}),
		// B1 fail-closed: arm the SS-teardown countdown on disconnect.
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
			a.armFailClosed()
		}),
	}

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
