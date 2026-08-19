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
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"net"
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
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/tunnel"
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
	// The upgrade marker and prev slot derive from its directory.
	UpgradeExecutablePath string

	// UpgradeNowFn overrides the clock the upgrade state machine reads
	// (marker deadline stamping, watchdog arming, boot decisions). nil →
	// time.Now. Tests use it to walk the register deadline without sleeping.
	UpgradeNowFn func() time.Time

	// UpgradeExecFn overrides the process-replacement call used by the
	// watchdog/boot rollback paths. nil → syscall.Exec (production). Tests
	// inject an observing fake to assert "exec'd with the restored path"
	// without replacing the go-test binary.
	UpgradeExecFn func(exePath string) error

	// TeardownEscalateFn replaces the S5 escalation of the bounded-teardown
	// ladder (gotcha #72): nil in production, where a wedged rebuild teardown
	// self-execs in place and a wedged shutdown exits non-zero. Tests inject an
	// observer so the escalation can be asserted without replacing or killing
	// the test binary. The argument is "rebuild" or "shutdown".
	TeardownEscalateFn func(intent string)

	// TeardownCloseFn replaces the actual nats.Conn close the bounded-teardown
	// ladder performs (gotcha #72): nil in production (nc.Close). Tests use it
	// to park the close until the tracked conn is poisoned, reproducing the
	// field wedge deterministically without a half-dead network.
	TeardownCloseFn func(nc *nats.Conn)

	// TeardownDialFn replaces the raw dialer the connTracker wraps (gotcha #72
	// test seam): nil in production, where the tracker wraps the proxy-aware
	// dialer. A test hands back a conn whose Write blocks forever, reproducing
	// the field hang deterministically through real nats.go frames.
	TeardownDialFn func(ctx context.Context, network, address string) (net.Conn, error)

	// UpgradeRunningImagePath overrides where the upgrade state machine reads
	// THIS PROCESS'S OWN executing image from when it needs proof that "we
	// are the staged binary" (external review F1). "" → /proc/self/exe on
	// Linux — which reads the bytes of the image this process is actually
	// running, even after the on-disk path was renamed over — falling back
	// to os.Executable elsewhere. The distinction is the whole point: on a
	// shared-binary host a sibling agent that never re-exec'd hashes the
	// REPLACED path to NewSHA while its own image is still the old binary;
	// hashing the running image instead makes the commit proof
	// process-local and unborrowable. Tests point this at a fixture file.
	UpgradeRunningImagePath string

	// UpgradeBootProofID injects the process-local boot proof in tests. In
	// production it is empty and New consumes the proof recorded by the
	// pre-Cobra BootUpgradeCheck for this process.
	UpgradeBootProofID string

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

	// ProxyOptOut (#78) is agent.yaml `proxy.participate: false`: this node
	// refuses to serve as a session-proxy egress. Reported at register
	// (NodeRegisterReq.ProxyOptOut) so a #78-aware broker stops allocating /
	// pushing entirely; ALSO enforced locally in applyProxyDirective as the
	// N-1 belt — an older broker keeps pushing directives, and without the
	// local gate this node would keep dialing a tunnel it can never serve
	// (the exact 5s WARN flood #78 documents). Zero value = participate.
	ProxyOptOut bool

	// ProxyDialRetryBase / ProxyDialRetryCap (#78) tune the first-dial
	// backoff for the proxy tunnel (proxyStartLocked). Zero ⇒ the defaults
	// (5s base — one heartbeat — capped at 5min).
	//
	// review 疑惑1: these are a TEST SEAM ONLY — there is no agent.yaml key or
	// CLI flag that sets them, so a deployment cannot tune them. They are NOT
	// an operator knob; if that ever changes, wire a yaml/flag AND state the
	// rollback compatibility (an older binary would ignore the key).
	ProxyDialRetryBase time.Duration
	ProxyDialRetryCap  time.Duration

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

	// RosterRefreshInterval (C1 §D-4) is the cadence at which an online agent pulls a
	// fresh signed roster (RosterRefreshOnly register → roster-only RODB read, no raft).
	// 0 ⇒ default 3min, full-jittered. Tests override down. A value < 0 disables refresh
	// (boot/reconnect adoption still happens).
	RosterRefreshInterval time.Duration

	// AccountPub (C1 §D-5), when non-empty, is an OUT-OF-BAND account public-key pin that
	// is AUTHORITATIVE: it disables roster TOFU and is enforced against every roster's
	// account_pub. Empty (the default) ⇒ first-roster TOFU (first-write-wins, persisted).
	AccountPub string

	// BootstrapURL (C2), when non-empty, is the well-known HTTPS manifest URL the agent fetches at
	// cold-start (async, best-effort) to learn the signed roster + seed endpoints when its cached set
	// is cold/expired. Empty ⇒ no HTTP bootstrap (steady-state NATS refresh + seed floor only).
	BootstrapURL string

	// Now is the agent clock seam (roster expiry / stale checks). nil ⇒ time.Now.
	Now func() time.Time
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

	// h1 D ctl-liveness fields, all under procsMu.
	//
	// kaGrace is the reap window derived from RunReq.KAIntervalMS
	// (max(6×interval, 3min)); 0 = the ctl never advertised keepalives =
	// NEVER reap (every pre-h1 ctl). lastKA is the agent-side receipt stamp
	// of the most recent keepalive (seeded at spawn). probeConfirmed is the
	// two-strike state: a candidate survives its FIRST expired-and-probed
	// tick and is reaped only if a SECOND probe-backed tick still finds
	// silence — closing the race where a healed link's retransmitted
	// keepalives arrive a beat late. reaped latches so Hangup fires once.
	// waitDone is set the instant sess.Wait returns: between Wait and
	// unregisterProc the child's pgid can be RECYCLED by the kernel, and a
	// reaper firing SIGHUP at a recycled pgid would shoot an innocent
	// process (plan critique-2).
	kaGrace        time.Duration
	lastKA         time.Time
	probeConfirmed bool
	reaped         bool
	waitDone       bool
}

type Agent struct {
	cfg Config

	// instanceID identifies THIS RUN of the agent process (see instance.go).
	// Immutable for the process; carried across syscall.Exec through the
	// environment so an upgrade does not read as a new instance.
	instanceID string

	// routingNID is the name this agent currently registers, subscribes and
	// publishes under. It starts as cfg.NID (the agent.yaml basename) and
	// changes only when the broker assigns a lease because another live
	// instance already holds the basename.
	//
	// THE NAME IS IMMUTABLE FOR THE LIFETIME OF A *nats.Conn. nats.go is
	// configured with MaxReconnects(-1), so it replays subscriptions on their
	// ORIGINAL subjects and re-sends the stored CONNECT name on every
	// reconnect — an in-place rename would therefore revert silently on the
	// first network blip, leaving the agent publishing under one name and
	// subscribed under another. A name change is expressible ONLY as a full
	// session rebuild, which is why adoption returns rebuild=true rather than
	// mutating anything live. It is an atomic.Pointer rather than a plain
	// field because it is read from several goroutines between sessions.
	routingNID atomic.Pointer[string]

	// leaseAdoptions bounds how many times this process will accept a new
	// routing name. Each adoption rebuilds the session, so an unbounded count
	// lets a flapping broker spin the loop at hundreds of rebuilds per second.
	leaseAdoptions atomic.Int32

	// leaseRefusals counts consecutive verdicts this process could not use — an
	// empty assignment (the suffix space is full) or a name outside its own
	// family. Each one retires the session, so without a bound the agent
	// re-competes as fast as it can connect. See leaseRefusalBackoff.
	leaseRefusals atomic.Int32

	// leaseRefusalUntil is the instant before which this process must not dial
	// again after an unusable lease verdict, as UnixNano. Set by the refusal
	// path AFTER it has already retired the session, and spent by the Run loop:
	// retirement is immediate, re-competition is what backs off. See
	// leaseRefusalBackoff.
	leaseRefusalUntil atomic.Int64

	// leaseRefusalTerminal stops the session loop after the refusal budget is
	// exhausted. The process remains alive for diagnosis, but does not keep
	// reconnecting after announcing that a restart or more suffix capacity is
	// required.
	leaseRefusalTerminal atomic.Bool

	// previousNID carries the name this agent registered under just before
	// adopting a lease, for exactly ONE register. The broker needs it to
	// recognise the agent's own running processes, whose rows are filed under
	// the old name — without it they arrive as orphans and it orders them
	// killed. Consumed (swapped to nil) when the register payload is built.
	previousNID atomic.Pointer[string]

	// procs tracks live `tether run` PTY sessions by pid. Used by the
	// kill verb to look up the right session to signal AND by the
	// register snapshot to report (PID, started_at, start_time_ticks)
	// per architecture G.1. Populated when fork+exec succeeds (after
	// attach handshake) and pruned right before the agent publishes
	// RunChunk{Kind:exit}.
	procs   map[string]*procRec
	procsMu sync.Mutex

	// execChildren tracks live `tether exec` OS children by pid so an admin EVICT
	// can reap them (#26). Unlike PTY `run` children (a.procs), a synchronous exec
	// child is otherwise held only by the handler goroutine blocked in cmd.Wait —
	// on a bare setsid-nohup deploy (no systemd cgroup) the daemon's self-exit
	// then orphans the child into the host process table. Registered right after
	// fork+exec, pruned when the synchronous handler returns.
	execChildren   map[string]*os.Process
	execChildrenMu sync.Mutex

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
	//
	// Stage-C MAJOR-1: the C1 session loop REWRITES runCtx every session, concurrently with
	// NATS-callback goroutines (dispatchForwarded / onNATSReconnect / armRedialWatchdog /
	// armFailClosed) that read it — so access is now mutex-guarded (setRunCtx/loadRunCtx). Pre-C1 it
	// was written once before any callback, so a bare field was race-free; the rebuild loop is not.
	runCtxMu sync.Mutex
	runCtx   context.Context

	// upgradeMu serializes every post-boot upgrade-marker transition
	// (commit-on-register vs the register-deadline watchdog rollback) so the
	// two can never both act on the same pending marker; upgradeWatchdogStop
	// cancels the armed watchdog once a commit lands. See upgrade_state.go.
	upgradeMu           sync.Mutex
	upgradeWatchdogStop context.CancelFunc
	upgradeBootProofID  string

	// sessFin publishes the CURRENT session's bounded-teardown finalizer to the
	// callback goroutines that must tear a session down without owning it
	// (fireRedial / rebuildOntoVoter — gotcha #72). Same mutex discipline as
	// sessCancel: rewritten every session, read from other goroutines.
	sessFinMu sync.Mutex
	sessFin   *sessionFinalizer
	// sessTracker holds the connTracker built for the connection currently being dialed, until
	// session() hands it to that session's finalizer (takeSessionTracker).
	sessTrackerMu sync.Mutex
	sessTracker   *connTracker
	// upgradeInstallMu (internal review S2) makes the WHOLE install pipeline
	// (entry gate → download → smoke → prev slot → marker → flip) mutually
	// exclusive with itself: every forwarded upgrade message runs in its own
	// nats.go handler goroutine, and two interleaved installs would clobber
	// the prev slot out from under the other's marker. TryLock only — a
	// loser replies upgrade_in_progress instead of queueing (~35s hold). A
	// separate mutex from upgradeMu on purpose: holding upgradeMu for the
	// whole download would starve the watchdog and the commit path.
	upgradeInstallMu sync.Mutex

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

	// courier is the h1 C proc-event delivery loop (started/exit as ACKed
	// requests on the CURRENT conn + register-snapshot replay). Lifetime-owned,
	// created eagerly in New like proxy; every method hangs on procCourier
	// (the Agent type-methods ledger is exact-count).
	courier *procCourier

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

	// connURLBox is the lock-free snapshot of the current connection's URL, taken while the link is
	// healthy (external review F3). Teardown/logging paths read THIS, never nc.ConnectedUrl(),
	// because every *nats.Conn observer takes the mutex a wedged doReconnect is holding.
	connURLBox atomic.Pointer[string]

	// proxyPublicPort is the lock-free mirror of the embedded proxy's public
	// port (0 when not serving). The tunnel hook reads it to FILTER: only the
	// __proxy__ port's up/down transitions may move proxy_ready — a flap on a
	// regular `expose` port must not. Stored before AddProxy in proxyStartLocked,
	// cleared on teardown / fail-cleanup.
	proxyPublicPort atomic.Int64

	// proxyTunnelUp (C5 M2) mirrors the proxy's reverse-tunnel liveness (set on the proxy port's
	// onTunnelSessionState edge, true after AddProxy opens it, false on teardown/fail-cleanup/drop). The
	// heartbeat's ProxyBound reflects this AND p.srv!=nil — so a pure tunnel drop (SS server still up,
	// home alive) reports unready and the cluster broker does NOT resurrect proxy_ready over a dead
	// tunnel. Without it ProxyBound==(p.srv!=nil) would vend a dead-tunnel exit in /sub.
	proxyTunnelUp atomic.Bool

	// reconnectInFlight (audit xx-concurrency F4) single-flights onNATSReconnect: the NATS
	// ReconnectHandler fires once per reconnect, and a flapping link could otherwise fan out an
	// UNBOUNDED set of concurrent re-register goroutines (each re-applying directives). The CAS
	// gate ensures at most one runs at a time; a reconnect arriving while one is in flight is
	// dropped (the in-flight pass re-registers against the stable nc, which is already
	// reconnected — agent.go ncBox pointer is stable across reconnects).
	reconnectInFlight atomic.Bool

	// rehome (D6 §7.4) dedup: exactly ONE applyOneHome retry loop runs per public
	// port regardless of how many reconnects fire (review A4 B1 — a flapping NATS
	// link must not spawn an unbounded fan of concurrent same-port dials). rehomeWant
	// holds the FRESHEST directive per port (a newer epoch supersedes a retrying
	// loop in place); rehomeRunning marks ports with a live loop. Guarded by rehomeMu.
	rehomeMu      sync.Mutex
	rehomeWant    map[int]proto.HomeDirective
	rehomeRunning map[int]bool
	// rehomeSeq is bumped every time rehomeWant[port] is (re)recorded — including a
	// SAME-epoch pure-pin update (external review RF2). The per-port loop tracks the
	// seq it applied and re-applies whenever it changed, so a same-epoch cert
	// rotation queued behind a running attempt is not dropped by an epoch>-only check.
	rehomeSeq map[int]uint64
	// deferredReplay holds ports whose boot replay deferred on missing cert pins
	// (a clustered PortToken with HomeBrokerAddr but no persisted pins, external
	// review F2). A directive for such a port must OPEN it from state.json (not
	// ApplyHome a non-existent session); cleared once opened. Guarded by rehomeMu.
	deferredReplay map[int]bool

	// rehomeAckTo (R8a P1) maps public port → the broker _INBOX subject an APPLIED
	// home-ack must go to. Populated by handleHomeForwarded from the push's Reply;
	// read by applyOneHome's success tail. Guarded by rehomeMu (same lock as the rest
	// of the rehome state, so a push and a running loop cannot interleave).
	rehomeAckTo map[int]string

	// --- C1 agent roster auto-discovery (consume signed roster) ---

	// rosterMu guards the in-memory roster mirror (pinAccount/rosterGen/rosterURLs),
	// loaded once at boot from state.json and updated by adoptRoster. connectNATS reads
	// rosterURLs to build the dial pool; the register req reads rosterGen; adoptRoster
	// enforces pinAccount. (Kept in memory so the hot dial/register paths take no
	// per-call state.json read — Component I.)
	rosterMu     sync.Mutex
	pinAccount   string               // durable TOFU account-pub pin ("" until first roster / OOB)
	rosterGen    uint64               // monotone roster generation high-water mark
	rosterURLs   []string             // learned client-dial URLs (VOTER-first), seed is unioned at dial time
	cachedRoster *proto.ClusterRoster // C2: last accepted roster (persisted for boot re-verification)
	seedGen      uint64               // C2: monotone seed_generation high-water mark
	seedURLs     []string             // C2: learned client-dialable seed endpoints (cold-start floor)
	cachedSeeds  *proto.SeedBundle    // C2: last accepted seed bundle (persisted for boot re-verification)
	// rosterRefreshNow is a coalescing edge trigger from signed-topology sys.events to the
	// roster-only refresh loop. It closes the healthy-connection island window during consecutive
	// retires; the periodic timer remains the loss-tolerant fallback.
	rosterRefreshNow         chan struct{}
	rosterRefreshFailBackoff time.Duration // test seam; immutable after New/Run begins
	// rosterCacheStore (C2) persists the discovery cache to its OWN roster_cache.json (split out of the
	// daemon-owned state.json). nil when Home is unset (in-process tests) → falls back to stateStore.
	rosterCacheStore *rosterCacheStore

	// rebuilding (C1 §D-1 L3) is set while a stuck-reconnect session rebuild is in
	// flight, making onNATSReconnect a no-op so nats.go's own late reconnect on the
	// dying conn cannot re-subscribe on it (no double-dispatch). Cleared by Run after
	// the dying session fully tears down.
	rebuilding atomic.Bool
	// rebuildRequested is set by the redial watchdog so the session loop returns
	// rebuild=true (a fresh dial pool) rather than treating the close as a shutdown.
	rebuildRequested atomic.Bool

	// redialMu/redialTimer (C1 §D-1 L3) is the stuck-reconnect watchdog: armed on NATS
	// disconnect, Stop'd on reconnect/connect, single-arm (one timer, re-armed not
	// stacked, mirroring flcTimer). If it fires (disconnected > redialAfter, i.e. the
	// boot pool is dead) it triggers a session rebuild that re-dials the freshest roster.
	redialMu    sync.Mutex
	redialTimer *time.Timer

	// sessCancelMu/sessCancel holds the CURRENT session's cancel func so the watchdog
	// (a separate goroutine) can unblock heartbeatLoop to force the rebuild.
	sessCancelMu sync.Mutex
	sessCancel   context.CancelFunc

	// avoidHostMu/avoidHost (#48) is a ONE-SHOT dial-pool exclusion set by the broker-silence
	// escape: when the current broker goes silent (a retired-broker NATS island) the roster
	// refresh loop rebuilds the session and stamps the silent broker's host here so connectNATS
	// steers the FIRST reconnect onto a DIFFERENT voter instead of re-sticking to the island
	// (nats.DontRandomize honors dial order, and the silent broker's nats-server is still
	// accepting connections). connectNATS consumes it once; if the survivor is momentarily
	// unreachable it falls back to the full pool (no permanent lockout).
	avoidHostMu sync.Mutex
	avoidHost   string
}

// sessionStateHookSetter is the OPTIONAL capability a production ExposeAdapter
// (TunnelExposeAdapter) implements to receive data-plane up/down transitions.
// The in-process test adapter does not implement it, so the type assertion in
// New simply skips wiring there.
type sessionStateHookSetter interface {
	SetSessionStateHook(fn func(publicPort int, up bool))
}

// homeApplier is the OPTIONAL capability (D6 §7.4) a production ExposeAdapter
// (TunnelExposeAdapter) implements to epoch-ordered-rehome an open expose to a
// new home broker. The in-process test adapter does not implement it, so
// applyHomeDirectives simply skips rehome there (the home directives are a no-op
// without a real tunnel).
type homeApplier interface {
	ApplyHome(publicPort int, brokerAddr string, epoch int64, certPins proto.CertPins) error
}

type homeSessionChecker interface {
	HasSession(publicPort int) bool
}

// afterRehomeWantSettledHook is a test seam fired when a per-port rehome worker settles. It is an
// atomic.Pointer (not a bare func var) because the worker reads it from a goroutine while the test
// sets/clears it — a bare var is a data race under -race (pre-existing, surfaced by the C1 -race
// gate). nil pointer ⇒ no hook.
var afterRehomeWantSettledHook atomic.Pointer[func(*Agent, int)]

// afterSilenceEscapeHook (#48) is a test seam fired right after the broker-silence escape closes
// the current session and rebuilds onto a voter. It carries the host the escape decided to avoid.
// Same race-safe atomic.Pointer pattern as afterRehomeWantSettledHook. nil ⇒ no hook.
var afterSilenceEscapeHook atomic.Pointer[func(*Agent, string)]

// fireAfterSilenceEscape invokes the test hook if installed (race-free load).
func fireAfterSilenceEscape(a *Agent, avoidedHost string) {
	if h := afterSilenceEscapeHook.Load(); h != nil {
		(*h)(a, avoidedHost)
	}
}

// fireAfterRehomeWantSettled invokes the test hook if installed (race-free load).
func fireAfterRehomeWantSettled(a *Agent, port int) {
	if h := afterRehomeWantSettledHook.Load(); h != nil {
		(*h)(a, port)
	}
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
	a.proxyTunnelUp.Store(up) // C5 M2: the heartbeat ProxyBound tracks this tunnel-liveness edge
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
	bootProofID := cfg.UpgradeBootProofID
	if bootProofID == "" {
		bootProofID = currentUpgradeBootProof()
	}
	// Mint (or adopt, across a re-exec) this process run's instance id. A
	// failure here is not fatal: an empty id simply means the broker treats
	// this agent exactly as a pre-feature one, which is today's behaviour. An
	// agent that refused to start because a random source hiccuped would be a
	// strictly worse outcome than one that runs without lease arbitration.
	instanceID, err := mintInstanceID()
	if err != nil {
		cfg.Logger.Warn("agent: could not mint an instance id; lease arbitration disabled for this run", "err", err)
		instanceID = ""
	}
	a := &Agent{
		cfg:                      cfg,
		instanceID:               instanceID,
		upgradeBootProofID:       bootProofID,
		procs:                    map[string]*procRec{},
		execChildren:             map[string]*os.Process{},
		canonAllowRoots:          CanonAllowRoots(cfg.AllowRoots),
		transferMode:             resolveTransferMode(cfg.RootsConfigured, cfg.AllowRoots),
		proxy:                    &proxyRuntime{}, // F2: lifetime-owned, created eagerly (no init race)
		rehomeWant:               map[int]proto.HomeDirective{},
		rehomeRunning:            map[int]bool{},
		rehomeSeq:                map[int]uint64{},
		deferredReplay:           map[int]bool{},
		rehomeAckTo:              map[int]string{},
		rosterRefreshNow:         make(chan struct{}, 1),
		rosterRefreshFailBackoff: defaultRosterRefreshFailBackoff,
	}
	a.courier = newProcCourier(a) // h1 C: lifetime-owned, eager like proxy
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
		a.rosterCacheStore = newRosterCacheStore(cfg.Home, cfg.SID) // C2: discovery cache in its own file
		// #78: an opted-out node must not carry a persisted proxy footprint —
		// the keyset-only bootstrap path (applyProxyDirective's srv==nil arm)
		// would otherwise re-dial a tunnel this node refuses to serve the
		// moment an older broker re-pushes keys. Clearing at construction
		// makes the opt-out effective even against pre-#78 brokers.
		if cfg.ProxyOptOut {
			if err := a.stateStore.SetProxy(nil); err != nil {
				cfg.Logger.Warn("agent: clear proxy footprint for opt-out", "err", err)
			}
		}
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
	// Restore a lease name adopted before a re-exec. `node upgrade` replaces
	// the process image in place, so an instance that was already running under
	// an assigned name must come back under that same name rather than
	// re-contesting for its basename — otherwise every upgrade of a leased
	// instance would shuffle names.
	// The restored name must be a lease OF THIS AGENT'S OWN BASENAME. The
	// variable exists for exactly one purpose — carry a name this process
	// already adopted across its own re-exec — so the only legal values are the
	// basename and `<basename>-NN`. Accepting any nid-shaped string would let a
	// stray environment variable move an agent onto an unrelated device's name.
	restored := strings.TrimSpace(os.Getenv(routingNIDEnv))
	// Consumed for the same reason as the instance id: a managed child must not
	// inherit it. See mintInstanceID.
	_ = os.Unsetenv(routingNIDEnv)
	if restored != "" && acceptableLeaseName(a, restored) {
		// RESUME, not adopt: this lineage already held the name before the
		// re-exec, so there is no rename here and nothing to carry across one.
		// See resumeRoutingNID for what adopting would have claimed and whose
		// processes it would have closed.
		resumeRoutingNID(a, restored)
	}
	setExecLineage(a.instanceID, nidOf(a))
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

// Run is the agent's main loop. It runs a SESSION (connect → register → subscribe →
// reconcile → replay → proxy → heartbeat) and re-runs it whenever the C1 stuck-reconnect
// watchdog requests a rebuild onto the freshest roster (live broker failover). A real
// parent-ctx cancel / admin evict ends the loop. The NATS connection is drained on each
// session exit.
func (a *Agent) Run(ctx context.Context) error {
	a.loadRosterCacheAtBoot() // seed the dial pool from state.json before the first connect
	// h1 C: the proc-event courier outlives session rebuilds (it delivers on
	// whatever conn a.ncBox currently holds), so it binds to the PARENT ctx —
	// exactly one goroutine per agent lifetime (leak-gate covered).
	go a.courier.run(ctx)
	// upgrade-safety plan §3.1: while an upgrade marker is pending, this
	// process must either register (commit) or roll back before the deadline
	// — armed here, INSIDE the new binary, so the guarantee holds even on the
	// unsupervised setsid-nohup path where no supervisor exists to help.
	a.armUpgradeWatchdog(ctx)
	// C2 §2.4 tier 2: at cold-start, asynchronously fetch the well-known manifest and adopt it (never
	// blocks Run; best-effort; a no-op without a pin). Helps when the configured NATS endpoints are
	// all dead but the HTTPS manifest is reachable; steady-state stays the NATS refresh loop.
	if a.cfg.BootstrapURL != "" {
		go a.bootstrapFetchOnce(ctx)
	}
	// Stage-C MINOR-2: ensure no armed AfterFunc (redial watchdog / fail-closed) outlives the agent
	// — a disconnect within redialAfter of an operator stop would otherwise leave a timer that later
	// fires fireRedial on a returned Run (and could perturb a NumGoroutine/fd leak-gate poll).
	defer a.stopRedialWatchdog()
	defer a.cancelFailClosed()
	for {
		rebuild, err := a.session(ctx)
		// The dying session's defers have run (conn drained, subs gone); allow the next
		// session's onNATSReconnect to operate again, and reset the rebuild request.
		//
		// origin: gotcha #72 — this clear MUST stay behind session()'s return, and session()'s
		// teardown defer now BLOCKS on the bounded finalizer (which joins its closer goroutine).
		// That ordering is load-bearing twice over: clearing `rebuilding` early would let a late
		// nats.go reconnect on the DYING conn pass the ReconnectHandler's guard and re-subscribe
		// (double dispatch), and would let a late close target the SUCCESSOR's connection. The
		// finalizer's poison+escalate ladder is what keeps that block bounded instead of
		// reintroducing the hang one frame up.
		a.rebuilding.Store(false)
		a.rebuildRequested.Store(false)
		if err != nil {
			return err
		}
		if !rebuild || ctx.Err() != nil {
			return ctx.Err()
		}
		// Spend any backoff owed for an unusable lease verdict BEFORE dialling
		// again. The session that met the verdict is already gone by now — the
		// refusal path retires immediately — so this delays only the
		// re-competition, which is the part that could otherwise spin at connect
		// speed across every clone in an image. (external review F3/F17)
		if !awaitLeaseRefusalBackoff(ctx, a) {
			return ctx.Err()
		}
		a.cfg.Logger.Info("agent: rebuilding NATS session on the freshest roster")
	}
}

// session runs one NATS connection's lifetime:
//  1. connect NATS (retried until ctx cancels — see connectNATS),
//  2. register with the broker (retried per `register`),
//  3. subscribe to `cmd.node.<nid>.*.req.forwarded` (P4 exec; P5 run/PTY; P6 expose; etc.),
//  4. heartbeat ticker until ctx cancels.
//
// It returns rebuild=true (err==nil) when the C1 watchdog Closed the conn to fail over onto
// a fresh dial pool; otherwise the terminal error (or nil on a clean parent-ctx shutdown).
// The NATS connection is drained on exit.
func (a *Agent) session(ctx context.Context) (rebuild bool, err error) {
	// Per-session reset: a prior session's shutdown barrier left proxyDraining=true; clear it
	// so this session admits proxy handlers again (the loop is sequential — no overlap).
	a.proxyDrainMu.Lock()
	a.proxyDraining = false
	a.proxyDrainMu.Unlock()

	// Stage-C MAJOR-1: derive + PUBLISH this session's runCtx BEFORE connectNATS. a.runCtx is
	// cross-session state read by armRedialWatchdog / armFailClosed / dispatchForwarded from other
	// goroutines; if it still pointed at the PREVIOUS (cancelled) session's ctx during this session's
	// connect+register setup window, the stuck-reconnect watchdog would skip arming on a disconnect
	// (its "don't arm during shutdown" guard is `a.runCtx.Err()!=nil`) — defeating failover in a
	// double-fault. cancelRun is deferred at its original position below (preserving teardown LIFO).
	runCtx, cancelRun := context.WithCancel(ctx)
	a.setRunCtx(runCtx)
	// Clear only a PREVIOUS session's watchdog before this connection can publish disconnect
	// callbacks. Doing this after connectNATS returns races an immediate DisconnectErrHandler: the
	// new session can arm its only watchdog and then have session() silently cancel it.
	a.stopRedialWatchdog()

	nc, err := a.connectNATS(ctx)
	if err != nil {
		cancelRun()
		return false, err
	}
	// origin: gotcha #72 — the session's own teardown goes through the SAME bounded finalizer the
	// rebuild callbacks use (single-flight per session). A bare `nc.Drain()` here is exactly the
	// unbounded call the incident hung on: on a RECONNECTING conn nats.go turns Drain into Close and
	// takes the same nc.mu that doReconnect holds across a no-deadline dial/handshake. Registered at
	// the position the old Drain held so teardown LIFO is unchanged. `cancelRun` is threaded in so
	// the finalizer can cancel FIRST; the later `defer cancelRun()` stays as the clean-path cancel
	// (it is idempotent).
	fin := &sessionFinalizer{
		nc: nc, tracker: a.takeSessionTracker(), cancel: cancelRun, parent: ctx, agent: a,
	}
	a.setSessionFinalizer(fin)
	defer func() {
		intent := teardownShutdown
		if ctx.Err() == nil && a.rebuildRequested.Load() {
			intent = teardownRebuild
		}
		fin.Do(intent)
		a.setSessionFinalizer(nil)
	}()
	// A parent cancellation must be able to enter the finalizer even if this session goroutine is
	// parked in a nats.Conn observer before reaching heartbeatLoop. stopParentTeardown prevents the
	// callback from outliving a session that returned for another reason; sync.Once handles races.
	stopParentTeardown := context.AfterFunc(ctx, func() { fin.Do(teardownShutdown) })
	defer stopParentTeardown()

	a.ncBox.Store(nc) // publish nc for the lock-free tunnel session-state hook
	// Snapshot the URL for teardown diagnostics only AFTER publishing the bounded finalizer. This
	// observer takes nc.mu.RLock; an immediate reconnect can already hold nc.mu across an unbounded
	// dial/handshake by the time connectNATS returns. The parent AfterFunc and redial watchdog above
	// now have a published finalizer with which to break/escalate that wedge.
	connURL := nc.ConnectedUrl()
	a.connURLBox.Store(&connURL)
	// F9 / round-2 F4: drain in-flight proxy-keys handlers before the connection drains. Set
	// proxyDraining FIRST (under the lock) so dispatch cannot Add a new handler after this point,
	// THEN Wait — a sound barrier (no Add races the Wait).
	//
	// origin: internal review CT-1 (S4) — moved from a plain defer into the finalizer's bounded
	// cleanups for the same reason as the Unsubscribe above: an in-flight proxy handler blocked on
	// a wedged connection would make this Wait unbounded, and as a defer it ran BEFORE the ladder.
	// Ordering inside the closer is preserved (drain barrier first, then subscription teardown,
	// then the close) because cleanups run in registration order and this one is registered first.
	fin.addBoundedCleanup(func() {
		a.proxyDrainMu.Lock()
		a.proxyDraining = true
		a.proxyDrainMu.Unlock()
		a.proxyHandlerWG.Wait()
	})

	// origin: external review F2 (BLOCKER) — runCtx, NOT the parent ctx. A disconnect callback can
	// request a rebuild BEFORE the first register completes; the finalizer then cancels runCtx and
	// closes/poisons the conn, but a register loop watching only the parent ctx keeps Request-ing on
	// a dead connection forever. session() never returns, so Run can neither clear `rebuilding` nor
	// start a successor: the node is permanently OFFLINE without the close ever wedging.
	// h1 C4: flush pending proc.started events BEFORE the register snapshot is
	// built — a started that never landed makes G.1's orphan pass KILL the
	// live process it describes. Bounded to one request timeout; best-effort.
	a.courier.drainStarted(runCtx, nc)
	resp, err := a.register(runCtx, nc)
	if err != nil {
		cancelRun()
		return false, err
	}
	// A lease verdict terminates this register; the reasoning lives with the
	// code, on applyLeaseVerdict. Position matters here: returning BEFORE the
	// subscribe below is what stops a refused instance ever installing a
	// subscription on the contested name.
	if applyLeaseVerdict(a, resp.Lease) {
		cancelRun()
		return true, nil
	}
	// h1 C4: the register reply settles the courier's replay channel (pending
	// exits ride the snapshot; clearance per onRegisterSuccess's rules).
	a.courier.onRegisterSuccess(resp)
	// Connected + registered = healthy: cancel any fail-closed countdown a prior session's
	// partition (preserved across a rebuild — see the conditional defer below) left armed.
	// Parity with onNATSReconnect, which cancels it on a nats.go reconnect.
	a.cancelFailClosed()
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
		// Wildcard over verbs; derive the subject from the SSOT builder so the
		// version prefix is never hardcoded (tether.v2.* after the D0 flip).
		proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), "*"),
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
		cancelRun()
		return false, fmt.Errorf("agent: subscribe forwarded: %w", err)
	}
	// origin: internal review CT-1 (S4) — Unsubscribe takes nc.mu, the SAME mutex a wedged
	// doReconnect holds. Registered as a plain defer it would run BEFORE the bounded finalizer
	// (defer LIFO) and hang the teardown right back up, ladder and all — which is precisely the
	// hostage situation #72 exists to end. Hand it to the finalizer instead: it runs inside the
	// budgeted closer goroutine, so poisoning bounds it like the close itself.
	fin.addBoundedCleanup(func() { _ = subFwd.Unsubscribe() })

	if err := a.startCtlLiveness(runCtx, nc, fin); err != nil {
		cancelRun()
		return false, err
	}

	// P9 — listen for sys.events{type:agent_evicted, sid, nid}
	// addressed to us so `tether admin evict` takes effect within
	// the architecture P9 1s budget instead of waiting for the
	// next CONNECT to be denied. The handler triggers a graceful
	// shutdown via runCtx cancel; the surrounding ctx still wins
	// any race with parent-canceled paths. (runCtx/cancelRun are created at the top of
	// session() so a.runCtx is published before connect — Stage-C MAJOR-1; the defer here
	// preserves the original teardown LIFO order.)
	defer cancelRun()
	// C1 §D-1 L3: publish this session's cancel so the stuck-reconnect watchdog (a separate
	// goroutine) can unblock heartbeatLoop to force a rebuild.
	a.setSessionCancel(cancelRun)
	defer a.clearSessionCancel()
	// Stop the fail-closed timer on a CLEAN shutdown only. On a rebuild (rebuildRequested set
	// by the watchdog) PRESERVE the countdown across the gap so a rebuild that lands back in a
	// partition still tears the proxy down — the next session cancels it once healthy.
	defer func() {
		if !a.rebuildRequested.Load() {
			a.cancelFailClosed()
		}
	}()
	subEvict, err := nc.Subscribe(proto.SubjSysEvents, func(msg *nats.Msg) {
		var ev struct {
			Type string `json:"type"`
			SID  string `json:"sid"`
			NID  string `json:"nid"`
		}
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			return
		}
		if strings.HasPrefix(ev.Type, "nats_topology_") {
			// Events are only a wake-up hint: refreshRosterOnce still verifies the account-signed
			// roster and its monotone generation. Coalesce a burst from multiple brokers.
			select {
			case a.rosterRefreshNow <- struct{}{}:
			default:
			}
			return
		}
		if ev.Type != "agent_evicted" {
			return
		}
		if ev.SID != a.cfg.SID || ev.NID != a.cfg.NID {
			return
		}
		a.cfg.Logger.Info("agent: evicted by admin", "sid", ev.SID, "nid", ev.NID)
		// #26: reap this node's managed OS children BEFORE the daemon self-exits. On a
		// bare setsid-nohup host there is no systemd cgroup to reap them, so an evicted
		// agent's exec/run children would otherwise linger in the host process table.
		a.reapManagedChildren()
		cancelRun()
	})
	if err != nil {
		return false, fmt.Errorf("agent: subscribe sys.events: %w", err)
	}
	// origin: external review F1 (BLOCKER). This was a DIRECT defer while its sibling subFwd went
	// through addBoundedCleanup — and defer LIFO put it AHEAD of the finalizer, so on a wedged
	// connection Unsubscribe (which takes nc.mu) blocked forever and fin.Do never even started:
	// cancel-first, the 10s budget, poisoning and the escalation were all unreachable. Both NATS
	// subscriptions must go through the bounded path; TestSessionTeardownBoundsEveryNATSUnsubscribe
	// fails if any Unsubscribe is left as a plain defer.
	fin.addBoundedCleanup(func() { _ = subEvict.Unsubscribe() })

	// G.1 reply application MUST run before replay so any RevokePorts
	// have already pruned state.json by the time replayPortsFromState
	// reads it. Otherwise we'd re-establish a proxy that the broker
	// just told us to drop.
	a.applyReconciliation(runCtx, resp)

	// Re-establish reverse-TCP proxies from state.json (architecture F.6), with
	// the cloned-credential lease gate; the reasoning lives on the function.
	replayPortsUnlessLeased(a)

	// P13: converge the embedded SS proxy to the broker's directive (nil
	// when the session's proxy switch is off → ensures it's torn down).
	// #78: a fresh session invalidates any dial-failure run first (mirrors
	// onNATSReconnect — the register reply re-delivers the same identity,
	// which the backoff gate would otherwise still suppress).
	resetProxyDialBackoff(a)
	a.applyProxyDirective(runCtx, nc, resp.Proxy)

	// C1 §D-4: pull a fresh signed roster on a jittered cadence (roster-only register → no raft
	// write on the broker) so an online agent converges on a new broker set ≤5min. Bound to this
	// session via runCtx; exits when the session ends / rebuilds. Stage-C MINOR-3: start it ONLY
	// when we are actually in a cluster (this register delivered a roster, or we have adopted one
	// before) so a non-cluster agent sends NO new periodic register traffic (byte-equivalent).
	if resp.Roster != nil || a.cachedRosterGen() > 0 {
		go a.rosterRefreshLoop(runCtx, nc)
	}

	hbErr := a.heartbeatLoop(runCtx, nc)
	// If the watchdog requested a rebuild, return rebuild=true (a fresh dial pool) rather than
	// treating the Close as a shutdown. A real parent-ctx cancel / evict returns rebuild=false.
	if a.rebuildRequested.Load() {
		return true, nil
	}
	return false, hbErr
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
			// D6 §7.7/R-22: a clustered expose (HomeBrokerAddr set) has NO cert
			// pins on boot (pins are never persisted) → ErrHomePinsRequired. That
			// is EXPECTED: defer — the first register reply re-delivers the home
			// directive WITH pins and rehomes it. Not a real replay failure.
			if errors.Is(err, tunnel.ErrHomePinsRequired) {
				// F2: remember this port so a later register/expose directive (which
				// carries the pins) OPENS it from state.json rather than no-op'ing an
				// ApplyHome on a session that was never created.
				a.rehomeMu.Lock()
				a.deferredReplay[p.Port] = true
				a.rehomeMu.Unlock()
				a.cfg.Logger.Info("agent: deferring clustered expose replay until pins arrive",
					"name", p.Name, "port", p.Port)
				continue
			}
			a.cfg.Logger.Warn("agent: replay proxy",
				"err", err, "name", p.Name, "port", p.Port)
			continue
		}
		a.cfg.Logger.Info("agent: replayed expose",
			"name", p.Name, "port", p.Port, "local", p.LocalPort)
	}
}

// agentProxyDialTimeout bounds a single proxy dial (proxy hop + CONNECT/SOCKS
// handshake) when the agent is configured to reach the broker through a proxy.
const agentProxyDialTimeout = 10 * time.Second

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
	// buildOpts is a CLOSURE rather than a value computed once, because the
	// CONNECT name is baked into these options (nats.Name(cli.AgentName(sid,
	// nidOf(a)))) and the routing name can change mid-loop: an auth denial under
	// an assigned lease name drops the lease and retries as the basename. With
	// the options frozen above the loop, that retry would re-present the very
	// name auth_callout just refused — the degrade would be inert and a broker
	// rollback would still kill every leased agent.
	buildOpts := func() ([]nats.Option, error) {
		o := a.buildConnOptions()
		// Proxy-aware dial. Injected here (the single connect call site) rather
		// than in buildConnOptions, which has two returns and no merge point —
		// so both the anonymous and nkey branches are covered. No-op when no
		// proxy env set.
		popts, perr := proxydial.Options(proxydial.OSEnv, agentProxyDialTimeout)
		if perr != nil {
			return nil, perr
		}
		o = append(o, popts...)
		// origin: gotcha #72 — wrap whatever dialer the options settled on (proxy-aware or default) in
		// a connTracker, so the raw net.Conns behind this *nats.Conn stay reachable for poisoning when
		// a teardown blows its budget. Appended LAST: nats.SetCustomDialer overwrites Options.CustomDialer,
		// so this must be the final word, and the tracker forwards to the proxy dialer it replaced.
		o = append(o, a.trackerDialOption(ctx, o))
		return o, nil
	}
	connOpts, perr := buildOpts()
	if perr != nil {
		return nil, perr
	}
	// #48: a ONE-SHOT host to skip on the FIRST dial (the just-silent island broker). Consumed
	// here so a later rebuild for an unrelated reason is unaffected; dropped after attempt 1 so a
	// momentarily-unreachable survivor falls back to the full pool rather than locking out forever.
	avoid := a.takeAvoidHost()
	backoff := a.cfg.RegisterRetryInitial
	// firstDeniedNID remembers the identity auth refused BEFORE any lease drop,
	// so the terminal hint can name it. Without it the hint names the basename
	// the retry fell back to, and an operator following its `admin evict`
	// advice would delete the provisioning row a healthy incumbent depends on.
	var firstDeniedNID string
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// C1 §D-1 L1: dial the learned roster URLs (VOTER-first) UNION the configured seed
		// floor, so a (re)connect/rebuild prefers live voters and can fail over to a
		// newly-added broker. effectiveDialURLs == cfg.NATSURL until a roster is adopted,
		// so a non-cluster agent's dial string is byte-identical.
		dial := a.effectiveDialURLs()
		if attempt == 1 && avoid != "" {
			dial = excludeHostFromDial(dial, avoid)
		}
		nc, err := nats.Connect(dial, connOpts...)
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
			// A DENIAL UNDER AN ASSIGNED LEASE NAME IS RECOVERABLE — drop the
			// lease and retry as the basename instead of dying.
			//
			// A lease name has no agent_provisioning row by design; only a
			// broker carrying the suffix fallback admits it. So a rollback, or
			// one un-upgraded member of the cluster-wide auth_callout queue
			// group, denies it. Without this the agent would re-present the
			// same rejected name forever, and because an auth denial is FATAL
			// here, Run would return and the process would exit — turning a
			// broker rollback into a fleet-wide agent kill instead of a
			// degrade back to today's behaviour.
			if nidOf(a) != a.cfg.NID {
				denied := nidOf(a)
				if firstDeniedNID == "" {
					firstDeniedNID = denied
				}
				dropLease(a)
				// REBUILD THE OPTIONS. The CONNECT name is baked into them, so
				// without this the retry re-presents the name that was just
				// refused and the degrade is inert.
				if rebuilt, rerr := buildOpts(); rerr == nil {
					connOpts = rebuilt
				}
				a.cfg.Logger.Warn("agent: auth denied under the assigned lease name; retrying as the configured basename",
					"sid", a.cfg.SID, "denied_nid", denied, "basename", a.cfg.NID, "err", err)
				continue
			}
			// Name the identity that was ACTUALLY refused. After a lease drop
			// that is not necessarily cfg.NID, and the hint below tells the
			// operator to `admin evict` it — evicting the basename would delete
			// the provisioning row the healthy incumbent depends on.
			deniedNID := firstDeniedNID
			if deniedNID == "" {
				deniedNID = nidOf(a)
			}
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
				err, a.cfg.SID, deniedNID)
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
		NID:            nidOf(a),
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		BootID:         readBootID(),
		LocalProcesses: procs,
		LocalPorts:     ports,
		Capabilities:   []string{proto.CapProxyV1}, // P13: this build implements proxy-v1
		// InstanceID identifies THIS process run so the broker can tell an
		// incumbent reconnecting from a second process presenting the same
		// baked credential. A non-empty value IS the capability advertisement —
		// deliberately NOT a second Capabilities token, which would change the
		// register body of every single-agent device for no benefit.
		InstanceID: a.instanceID,
		// LeasedNID tells the broker we are running under an assigned lease
		// name rather than our configured basename; it folds into the existing
		// nodes.proxy_capable expression so an ephemeral instance never holds a
		// public egress port.
		LeasedNID: nidOf(a) != a.cfg.NID,
		// Consumed with Swap: it is only meaningful on the FIRST register after
		// an adoption. Carrying it further would let a much later register claim
		// processes under a name this agent no longer has any relationship to.
		PreviousNID: previousNIDOnce(a),
		// The root of this agent's credential family, stated rather than left to
		// be parsed out of NID. Without it the broker folds a device the
		// operator named `gpu-02` into the `gpu` family and offers its clone
		// `gpu-03` — a name in a different family that this agent's binding does
		// not cover. (external review F2)
		ConfiguredNID: a.cfg.NID,
		// D6 §6.5: self-report the DETERMINISTIC nats server_name we are connected
		// to (NOT the volatile NUID) so the leader can bridge it to a home broker.
		// "" on a single-node bus → inert. ConnectedServerName reflects the actual
		// connected server, updated across reconnects (drives §7.4 rehome).
		ServerID: nc.ConnectedServerName(),
		// C1: report our cached signed-roster generation so the leader can flag a
		// sustained-behind agent (agent_roster_stale). 0 ⇒ pre-C1 / no cache. Advisory.
		RosterGen: a.cachedRosterGen(),
		// #78: the proxy.participate opt-out. Capabilities stays untouched —
		// nodeHasProxyCap falls back to a release-version check when the
		// token is absent, so omitting CapProxyV1 could NOT express opt-out.
		ProxyOptOut: a.cfg.ProxyOptOut,
	}
	// upgrade-safety plan §3.1: the first register after a `node upgrade`
	// carries the outcome (this one IS the health check-in when pending);
	// empty strings on every ordinary register keep the wire byte-identical.
	req.UpgradeState, req.UpgradeDetail = a.upgradeRegisterReport()
	payload, err := json.Marshal(req)
	if err != nil {
		return proto.NodeRegisterResp{}, fmt.Errorf("agent: marshal register: %w", err)
	}
	subject := proto.SubjNodeRegister(a.cfg.SID, nidOf(a))

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
				// Register success is the upgrade commit point (plan §3.1):
				// promote a pending marker to committed / clear the terminal
				// record this very register delivered. Mutex-racing the
				// deadline watchdog — the loser of that race no-ops.
				//
				// A CONTESTED REPLY IS NOT A REGISTER. external review F4: it
				// carries OK, but by construction it did NOT register — no
				// registerNode, no reconcile, no AcceptedProcesses — it is a
				// verdict telling this agent to come back under another name.
				// Committing on it disarms the rollback watchdog before the
				// agent has proved it can run: if the assigned name then fails
				// to authenticate or rebuild, the node is permanently offline on
				// a binary that was never health-checked, and `node upgrade`
				// reported success. The real register under the assigned name is
				// moments away and commits then.
				if resp.Lease == nil {
					a.commitUpgradeAfterRegister(req.UpgradeState)
				}
				return resp, nil
			case resp.Code == proto.CodeLeaderUnavailable:
				// audit M2 / write-forward F3: a leadership race (raft failover / election) is
				// TRANSIENT, not a config error — retry with backoff instead of exiting the agent
				// process. break exits the switch and falls through to the backoff sleep below.
				a.cfg.Logger.Warn("agent: register hit a transient leader failover; retrying",
					"attempt", attempt, "next_backoff", backoff)
			case resp.Code == proto.CodeReplyTooLarge:
				// h1 A2 (plan critique-4 MAJOR): an oversize register REPLY is a
				// broker-side bug, not this agent's config. Pre-h1 the oversize
				// reply was silently dropped and register self-healed via the
				// timeout/retry path; letting it fall into the authoritative-reject
				// default below would instead EXIT the agent — converting a
				// self-healing condition into fleet-node death. Keep the
				// self-healing shape: retry with backoff.
				a.cfg.Logger.Warn("agent: register reply exceeded broker max_payload (broker bug); retrying",
					"attempt", attempt, "next_backoff", backoff, "detail", resp.Error)
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
	// h1 C4: pending (undelivered) exits ride the snapshot as State:"exited"
	// rows — the register round-trip is the courier's replay channel, so one
	// reconnect settles every exit a broker outage dropped, with the REAL rc
	// instead of G.1's -1. No pid appears twice: unregisterProc precedes the
	// exit enqueue (see enqueueExit's documented gap).
	procs = append(procs, a.courier.pendingExitSnapshot()...)

	var ports []proto.LocalPort
	// AN INSTANCE RUNNING UNDER A LEASE OWNS NOTHING IN state.json.
	//
	// That file describes whoever holds the BASENAME. Its port tokens are valid
	// bearer credentials for THAT node's allocations, so reporting them here as
	// our own would make the broker compare them against a nid that never owned
	// them, conclude they are stale, and answer with RevokePorts — which the
	// agent applies by pruning state.json. On the reference deployment
	// ~/.tether is a SHARED NFS mount (one inode, both instances), so that
	// prune would delete the basename holder's LIVE port rows.
	//
	// Reporting an empty set is also the honest answer: a leased instance has
	// no allocations until it exposes something under its own name.
	if nidOf(a) != a.cfg.NID {
		return procs, nil
	}
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
				// R8a P1: same reason as the expose-rm path — a revoked port must not
				// leave its home-ack destination behind in the map.
				a.forgetHomeAck(port)
				a.cfg.Logger.Info("agent: revoked", "port", port, "name", name)
			}
		}
	}

	for _, pid := range resp.DropProcesses {
		a.killOrphanProcess(ctx, pid)
	}

	// D6 §7.4: apply per-expose home directives (epoch-ordered rehome). nil in
	// N=1 (no clustered broker emits Home), so this is inert there.
	a.applyHomeDirectives(ctx, resp.Home)

	// C1: adopt the account-signed broker roster (verify → relearn dial URLs → advance the
	// monotone generation → persist). nil in single mode → no-op (byte-equivalent). This runs on
	// EVERY boot/reconnect register; the periodic refresh ticker uses adoptRoster directly.
	a.adoptRoster(resp.Roster)
}

// applyHomeDirectives drives the §7.4 agent-self-driven rehome from a register
// reply's HomeAssignment. Directives for DIFFERENT ports are applied
// CONCURRENTLY (R-14); but per port there is EXACTLY ONE retry loop at a time
// (review A4 B1): a reconnect storm records the freshest directive per port
// (rehomeWant) and spawns a new applyOneHome ONLY when no loop is running for
// that port, so the live-goroutine count is bounded by the number of distinct
// ports, not by the reconnect rate. A newer epoch supersedes a retrying loop in
// place (the loop re-reads rehomeWant each iteration).
func (a *Agent) applyHomeDirectives(ctx context.Context, ha *proto.HomeAssignment) {
	if ha == nil || len(ha.Directives) == 0 {
		return
	}
	applier, ok := a.cfg.ExposeAdapter.(homeApplier)
	if !ok {
		return // adapter has no tunnel (in-process test) — nothing to rehome
	}
	for _, d := range ha.Directives {
		a.rehomeMu.Lock()
		// Record the freshest directive for this port (a stale lower-epoch
		// directive never overwrites a newer want). A >= compare keeps a SAME-epoch
		// pure-pin rotation as the latest want, and bumping rehomeSeq makes the
		// running loop re-apply it (RF2).
		if cur, ok := a.rehomeWant[d.PublicPort]; !ok || d.Epoch >= cur.Epoch {
			a.rehomeWant[d.PublicPort] = d
			a.rehomeSeq[d.PublicPort]++
		}
		if a.rehomeRunning[d.PublicPort] {
			a.rehomeMu.Unlock()
			continue // an existing loop will pick up the freshest want
		}
		a.rehomeRunning[d.PublicPort] = true
		a.rehomeMu.Unlock()
		go a.applyOneHome(ctx, applier, d.PublicPort)
	}
}

// applyOneHome runs the SINGLE retry loop for one public port (R-15). Each
// iteration applies the FRESHEST directive recorded in rehomeWant (so a newer
// epoch arriving mid-retry supersedes in place). A rehome's first dial returns
// home_catching_up BEFORE any supervisor exists (the supervisor's own retry
// never sees it), so this loop retries with full-jitter backoff until the home
// catches up, ctx is canceled, a terminal error fires, or a max wall-time
// elapses (catch_up_stalled — give up until the next reconnect; never collapse
// to terminal). On success it persists the home addr+epoch (monotone). On exit
// it clears the running flag under lock, re-checking that no newer want arrived.
func (a *Agent) applyOneHome(ctx context.Context, applier homeApplier, port int) {
	const (
		base    = 500 * time.Millisecond
		maxWait = 30 * time.Second
		deadln  = 2 * time.Minute // bound: don't retry forever (§7.2 有界重试 + 告警)
	)
	var lastSeq uint64
	defer func() {
		var restart bool
		restartCtx := ctx
		a.rehomeMu.Lock()
		if a.rehomeSeq[port] == lastSeq {
			delete(a.rehomeRunning, port)
			delete(a.rehomeWant, port)
		} else if ctx.Err() == nil {
			// A newer directive arrived after this loop's final wantChanged check while
			// applyHomeDirectives still saw rehomeRunning=true. Keep the want and hand it
			// to a fresh single worker instead of deleting it in cleanup.
			a.rehomeRunning[port] = true
			restart = true
		} else if succ := a.loadRunCtx(); succ != nil && succ.Err() == nil {
			// Mega-audit MAJ-4: this loop's per-SESSION ctx was canceled (its session ended, e.g. a
			// C1 rebuild) but a NEWER want arrived while applyHomeDirectives still saw
			// rehomeRunning=true (so the new session did NOT spawn its own loop). The SUCCESSOR session
			// is live — hand the want to a loop on ITS ctx instead of orphaning it until the next full
			// register (the reverse tunnel would otherwise stay pinned to the dead/draining home).
			a.rehomeRunning[port] = true
			restart = true
			restartCtx = succ
		} else {
			// Agent itself is shutting down (no live successor session) — drop cleanly.
			delete(a.rehomeRunning, port)
		}
		a.rehomeMu.Unlock()
		if restart {
			go a.applyOneHome(restartCtx, applier, port)
		}
	}()
	sleep := base
	start := time.Now()
	for {
		if ctx.Err() != nil {
			return
		}
		a.rehomeMu.Lock()
		d := a.rehomeWant[port]
		seq := a.rehomeSeq[port]
		lastSeq = seq
		deferred := a.deferredReplay[port]
		a.rehomeMu.Unlock()

		var err error
		if deferred {
			// F2: the boot replay deferred this port on missing pins; now that a
			// directive carries pins, OPEN it from state.json (LocalPort + raw token).
			// AddProxy → OpenHome installs the session; a silent ApplyHome no-op
			// would leave a restarted clustered expose down forever.
			outcome, oerr := a.openHomeFromState(d)
			switch outcome {
			case openStateUnavailable:
				// RF1: state.json is temporarily unreadable/unparseable; NOTHING was
				// opened. KEEP the deferred marker and give up until the next reconnect
				// re-spawns this loop with a (hopefully) healthy read — never treat a
				// failed open-from-state as a successful rehome.
				return
			case openPortAbsent:
				// The persisted entry is gone (expose-rm) — nothing to open.
				a.clearDeferred(port)
				return
			case openedOK:
				// Fall through to `err = oerr`, AddProxy's result.
			}
			err = oerr // openedOK: AddProxy's result
		} else {
			// An OPEN expose: ApplyHome rehomes (epoch>) or pure-pin-updates (epoch==)
			// without tearing a live transport on a same-epoch reconnect.
			if checker, ok := applier.(homeSessionChecker); ok && !checker.HasSession(d.PublicPort) {
				outcome, oerr := a.openHomeFromState(d)
				switch outcome {
				case openStateUnavailable:
					return
				case openPortAbsent:
					return
				case openedOK:
					// Fall through to `err = oerr`, AddProxy's result.
				}
				err = oerr
			} else {
				err = applier.ApplyHome(d.PublicPort, d.BrokerAddr, d.Epoch, d.CertPins)
			}
		}
		if err == nil {
			if deferred {
				a.clearDeferred(port) // opened from state — no longer deferred
			}
			if a.stateStore != nil {
				// Monotone persist (review L-4): UpdatePortHome never downgrades the
				// stored epoch, so a no-op/stale directive can't rewrite state.json back.
				if perr := a.stateStore.UpdatePortHome(d.Name, d.BrokerAddr, d.Epoch); perr != nil {
					a.cfg.Logger.Warn("agent: persist rehome", "err", perr, "name", d.Name, "port", port)
				}
			}
			a.cfg.Logger.Info("agent: rehomed expose", "name", d.Name, "port", port, "epoch", d.Epoch)
			// R8a P1: confirm to the broker that the DATA PLANE moved. This is the only
			// place an ack is emitted, and it sits AFTER ApplyHome/openHomeFromState
			// returned nil and the new home was persisted — so "acked" means "applied",
			// never "received". The broker's home-delivery pass stops re-delivering on
			// this signal, and drain/retire's rc semantics are decided by it.
			a.ackHomeApplied(d)
			if a.wantChanged(port, seq) {
				sleep = base
				continue // a newer directive (higher epoch OR same-epoch pin update, RF2) arrived
			}
			fireAfterRehomeWantSettled(a, port)
			return
		}
		// Transient home_catching_up (the home has not yet applied the reassign):
		// back off and retry. Any other error is terminal for THIS directive — but a
		// NEWER directive that arrived while we were applying must still be tried
		// (external review F3: the unconditional drop wedged the expose).
		var de *tunnel.DenyError
		terminal := !errors.As(err, &de) || de.Reason != proto.ReasonHomeCatchingUp
		stalled := time.Since(start) > deadln
		if terminal || stalled {
			if terminal {
				a.cfg.Logger.Warn("agent: rehome failed (terminal)", "err", err, "name", d.Name, "port", port)
			} else {
				a.cfg.Logger.Warn("agent: rehome catch_up_stalled — giving up until next reconnect",
					"name", d.Name, "port", port, "epoch", d.Epoch)
			}
			if a.wantChanged(port, seq) {
				sleep, start = base, time.Now() // a newer directive supersedes; apply it
				continue
			}
			fireAfterRehomeWantSettled(a, port)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitterDur(sleep)):
		}
		if sleep *= 2; sleep > maxWait {
			sleep = maxWait
		}
	}
}

// wantChanged reports whether rehomeWant[port] was (re)recorded since the loop
// read sequence `seq` — a higher epoch OR a same-epoch pure-pin rotation (RF2).
// Used so the per-port loop re-applies the freshest directive instead of dropping
// it on exit (external review F3/RF2).
func (a *Agent) wantChanged(port int, seq uint64) bool {
	a.rehomeMu.Lock()
	defer a.rehomeMu.Unlock()
	return a.rehomeSeq[port] != seq
}

func (a *Agent) clearDeferred(port int) {
	a.rehomeMu.Lock()
	delete(a.deferredReplay, port)
	a.rehomeMu.Unlock()
}

// openOutcome distinguishes the three results of opening an expose from state so
// the caller never treats a failed/empty open-from-state as a successful rehome
// (external review RF1).
type openOutcome int

const (
	openedOK             openOutcome = iota // AddProxy was attempted; the returned error carries its result
	openStateUnavailable                    // state.json unreadable/unparseable — keep deferred, give up until next reconnect
	openPortAbsent                          // the persisted entry is gone — nothing to open, clear deferred
)

// openHomeFromState opens (or replaces) an expose's tunnel from its persisted
// PortToken (LocalPort + raw token) against the directive's home addr/epoch/pins
// (external review F2). Used for the deferred-boot-replay path where no session
// exists yet.
func (a *Agent) openHomeFromState(d proto.HomeDirective) (openOutcome, error) {
	if a.stateStore == nil || a.cfg.ExposeAdapter == nil {
		return openPortAbsent, nil // no state/adapter — nothing to open
	}
	sf, ok := a.loadStateBounded("rehome-open")
	if !ok {
		return openStateUnavailable, nil // RF1: degraded read — DO NOT AddProxy, keep deferred
	}
	for _, p := range sf.PortTokens {
		if p.Port == d.PublicPort {
			p.HomeBrokerAddr = d.BrokerAddr
			p.Epoch = d.Epoch
			p.CertPins = d.CertPins
			return openedOK, a.cfg.ExposeAdapter.AddProxy(p)
		}
	}
	return openPortAbsent, nil // removed from state.json — nothing to open
}

// jitterDur returns a full-jitter sleep in (0, d] to de-synchronize a fleet
// retrying after a shared home failover.
func jitterDur(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(mrand.Int64N(int64(d)) + 1)
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
	subject := proto.SubjNodeHeartbeat(a.cfg.SID, nidOf(a))
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.cfg.Logger.Info("agent: shutting down")
			return ctx.Err()
		case t := <-ticker.C:
			pgen, pepoch := a.proxyGenEpoch()
			payload, _ := json.Marshal(proto.HeartbeatPayload{
				Ts: t.UTC(), ProxyGeneration: pgen, ProxyEpoch: pepoch, ProxyBound: a.proxyBound(),
			})
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
// It returns no error: every path here appends to a slice, and the (always-nil) error it used to
// return made three call sites write handling for a failure that could not happen.
func (a *Agent) buildConnOptions() []nats.Option {
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		// A reconnect can race a rolling NATS/broker restart: nats-server may accept CONNECT while
		// the auth_callout responder is not subscribed yet and return an authorization violation.
		// nats.go aborts reconnect after seeing the same auth error twice unless this option is set,
		// leaving a healthy agent process permanently disconnected. Initial authentication remains
		// fail-fast in connectNATS above; this only keeps an already-authenticated daemon retrying.
		nats.IgnoreAuthErrorAbort(),
		// C1 §D-1: keep the dial pool in our priority order (VOTER-first / draining-last from
		// DialURLs, seed floor last) instead of nats.go's default shuffle, so a reconnect
		// prefers a live voter. Fleet load is spread by the intra-VOTER shuffle in DialURLs.
		nats.DontRandomize(),
		// P13 convergence (architecture p13-plan §5 / Critique 4): the agent
		// registers ONCE and heartbeat is fire-and-forget. On a NATS reconnect
		// (the agent process stayed up while its connection bounced) trigger a
		// single re-register so the broker's register reply re-delivers the
		// authoritative proxy directive (and re-runs G.1 reconcile).
		nats.ReconnectHandler(func(nc *nats.Conn) {
			// C1 §D-1 L3: while a stuck-reconnect rebuild is in flight the dying conn is being
			// torn down by the session loop — a late nats.go reconnect on it must NOT re-subscribe
			// (that would leave two live forwarded subscriptions → double dispatch).
			if a.rebuilding.Load() {
				return
			}
			a.stopRedialWatchdog() // a successful reconnect means we are NOT stuck
			// audit xx-concurrency F4: single-flight — drop a reconnect that arrives while a
			// prior onNATSReconnect is still running so a flapping link cannot fan out unbounded
			// concurrent re-register goroutines. The in-flight pass already re-registers against
			// the (stable) reconnected conn.
			if a.reconnectInFlight.CompareAndSwap(false, true) {
				go func() {
					defer a.reconnectInFlight.Store(false)
					a.onNATSReconnect(nc)
				}()
			}
		}),
		// B1 fail-closed: arm the SS-teardown countdown on disconnect. C1 §D-1 L3: also arm the
		// stuck-reconnect watchdog so a dead boot pool triggers a rebuild on the freshest roster.
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
			a.armFailClosed()
			a.armRedialWatchdog()
		}),
	}

	if a.cfg.Identity == nil {
		// Anonymous fallback: name with `/` separators is intentional
		// — it does NOT match parseRole's `tether-agent:<sid>:<nid>`
		// format, so a misconfigured prod broker (auth_callout ON,
		// Identity nil) fails CONNECT immediately rather than landing
		// on an unintended role.
		opts = append(opts, nats.Name(fmt.Sprintf("tether-agent/%s/%s", a.cfg.SID, nidOf(a))))
		return opts
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
		// The CONNECT name carries the ROUTING name: auth_callout mints
		// PermissionsForAgent from exactly this string, so an instance running
		// under a lease must present the lease name or its JWT would grant the
		// basename's subjects instead of its own. The three-field grammar is
		// unchanged — a fourth segment would fold into parseRole's SplitN and
		// be rejected by ValidateNID on any older broker, turning a rolling
		// upgrade into a hard auth denial.
		nats.Name(cli.AgentName(a.cfg.SID, nidOf(a))),
		nats.Nkey(id.PublicKey, sigCB),
	)
	if a.cfg.PIN != "" {
		opts = append(opts, nats.Token(a.cfg.PIN))
	}
	return opts
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
