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
// `cmd.*.req.forwarded` (architecture C.4) command-routing is wired in
// internal/broker/exec.go (handleExecReq → SubjCmdForwarded).
package broker

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokermetrics"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clustermanifest"
	"github.com/LinZiyang666/tether/internal/httplisten"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/subhttp"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	// LeaseGrace is how recently a name must have heartbeat for a DIFFERENT
	// instance's register to be treated as CONTESTED. It is deliberately its
	// own knob and must never be conflated with StaleAfter or OfflineAfter:
	// those answer "is this node usable", this one answers "might the previous
	// holder still be alive". OfflineAfter (60s) would suffix every ordinary
	// `systemctl restart` (RestartSec=5); StaleAfter (5s) is shorter than one
	// heartbeat interval.
	//
	// The arithmetic, which matters: a beat lands at T, the process dies at
	// T+ε with ε ∈ [0, HeartbeatInterval], systemd waits RestartSec, and the
	// successor registers at T+ε+RestartSec+boot. With the default 6s a
	// hard-killed lone agent whose ε+boot < 1s lands INSIDE the window — so
	// the window alone would rename it. That is exactly why entering the
	// window is not the verdict: it only triggers the interest probe, which
	// returns ErrNoResponders instantly for a dead predecessor.
	// Default: DefaultLeaseGrace.
	LeaseGrace time.Duration

	// MaxInstancesPerBasename caps how many concurrent instances one basename
	// may carry. Default DefaultMaxInstancesPerBasename; the hard ceiling is
	// node.MaxLeaseSuffix, above which a suffix no longer fits the zero-padded
	// width.
	MaxInstancesPerBasename int

	// AuthCallout, if non-nil, makes the broker connect with the configured
	// nkey credentials AND subscribe to $SYS.REQ.USER.AUTH to issue per-
	// connection user JWTs. Required for the architecture B.2 NATS-level
	// permissions invariant. When nil (P2 default), the broker connects
	// anonymously and no auth_callout is installed.
	AuthCallout *AuthCalloutConfig

	// PublicHost is the operator-facing DNS name printed in expose
	// URLs (e.g. "tether.example.com"). Empty defaults to "localhost"
	// — fine for tests, useless in prod.
	PublicHost string

	// ColocatedAgentNID (G5 #19) is the nid of the tether agent co-located on this broker host, self-
	// declared into ClusterHealthResp so `node ls --brokers` correlates the broker daemon's version with
	// its agent's RELEASE. Empty ⇒ the CLI falls back to the node_id==nid convention, labelled "(assumed)".
	ColocatedAgentNID string

	// PortBandLow / PortBandHigh override the public port range
	// (architecture A.3 / F.3). Defaults 14000-14999.
	PortBandLow  int
	PortBandHigh int

	// SubHTTPAddr is the loopback listen address for the P13 read-only
	// subscription HTTP endpoint (e.g. "127.0.0.1:8090"). Empty disables
	// the HTTP listener entirely — every pre-P13 deployment is unchanged.
	SubHTTPAddr string

	// MetricsAddr (B5 OPS#1) is the listen address for the Prometheus /metrics +
	// /healthz + /readyz HTTP endpoint (e.g. "127.0.0.1:9090"). Empty disables it
	// entirely — when unset ZERO new wiring runs (off-by-default byte-equivalence).
	// Unlike SubHTTPAddr this is NOT loopback-forced (operators scrape over a private
	// interface); it vends only public topology, never secrets.
	MetricsAddr string

	// ManifestAddr (C2) is the LOOPBACK listen address for the well-known cluster discovery manifest
	// (/.well-known/tether/cluster.json), Caddy-fronted. Empty disables it entirely; even when set it
	// binds NOTHING unless the broker runs clustered (selfID != "") → non-cluster byte-equivalence.
	// The endpoint is unauthenticated but serves only an account-SIGNED, discovery-only manifest.
	ManifestAddr string

	// AlertWebhookURL (B6 OPS#2), when set, makes the leader POST every committed alert
	// raise/clear transition to this http/https endpoint (cluster mode only). Empty disables it
	// entirely — when unset the reconciler's Webhook seam is nil and ZERO new wiring runs. The
	// body carries only public topology (never secrets); the URL must not contain userinfo.
	AlertWebhookURL string

	// SubURLBase is the public origin printed in subscription URLs
	// (e.g. "https://tether.example.com"). The full URL is
	// SubURLBase + "/sub/<token>". Empty falls back to PublicHost.
	SubURLBase string

	// PortRevokeAfter is how long a port stays ALLOCATED after its
	// owning node has been OFFLINE before the reconciler revokes it
	// (architecture D.4 / F.6). Default 15min; tests override down.
	PortRevokeAfterDur time.Duration

	// ExposeForwardTimeoutDur is the broker-side timeout for the
	// expose.req → agent ACK request/reply. Default 5s; tests
	// override down.
	ExposeForwardTimeoutDur time.Duration

	// TunnelControlAddr is where the broker's reverse-TCP tunnel
	// server listens for agent control connections. Default
	// ":7000" (matches architecture A.3 frp control_listen). Empty
	// disables the tunnel server entirely (control plane still
	// works; expose just won't actually forward TCP traffic).
	TunnelControlAddr string

	// TunnelPublicHost is the bind address for the public per-port
	// listeners the tunnel server opens on demand. Default
	// "0.0.0.0" (publicly reachable). Tests use "127.0.0.1".
	TunnelPublicHost string

	// StoreDir is the local filesystem path the disk-pressure
	// monitor (architecture H.4) statfs's. In production it should
	// point at the JetStream store dir (where audit history lives).
	// Empty = monitor disabled — fine for tests / dev setups that
	// don't run JetStream.
	StoreDir string

	// DiskCheckInterval is how often the monitor samples disk usage.
	// Defaults to 5min per H.4. Tests override to 50ms.
	DiskCheckInterval time.Duration

	// DiskPressureThreshold is the used/total fraction at which the
	// monitor pubs sys.events{type:disk_pressure}. Defaults to 0.80
	// (80% per H.4). 0 → use default; values outside (0, 1] disable
	// the monitor.
	DiskPressureThreshold float64

	// DiskUsageFn is injected for deterministic tests; when nil the
	// monitor calls the package-level diskUsage helper backed by
	// syscall.Statfs.
	DiskUsageFn func(path string) (used, total uint64, err error)

	// AdminSocketPath is the absolute Unix socket path for the
	// `tether admin *` local-only admin channel (architecture
	// I.2b / P9). Empty disables the admin endpoint entirely;
	// production sets it to /var/run/tether/admin.sock and the
	// adminsock package creates it 0600 under a 0700 parent dir.
	AdminSocketPath string

	// UpgradeURLAllowlist is the set of URL prefixes
	// `tether node upgrade` will accept (architecture J.4 §
	// "url 白名单"). Default empty = `tether node upgrade` is
	// REJECTED entirely (the allowlist is mandatory; we don't
	// silently default to "github.com/<org>/" because the org
	// name is operator-specific). Operators set it via
	// broker.yaml or --upgrade-url-allow CLI flag.
	UpgradeURLAllowlist []string

	// UpgradeForwardTimeoutDur bounds the broker→agent ACK
	// request/reply for upgrade.req. Default 30s — agent has to
	// download a tarball before replying, so the budget is
	// generous. Tests override down.
	UpgradeForwardTimeoutDur time.Duration

	// ReadyCh, if non-nil, is closed by Run AFTER every NATS
	// subscription has been installed (register / heartbeat / all
	// session+exec+ps+expose+upgrade handlers). Tests use this to
	// avoid a race where they fire requests before broker.Run's
	// goroutine has reached the Subscribe loop on a slow CI runner
	// (causes spurious "no responders" errors in test/p10).
	// Production callers leave this nil.
	ReadyCh chan struct{}

	// ProcRetention is how long an EXITED row is kept in the
	// `processes` table after ended_at before the periodic GC
	// sweeps it. Set in broker.yaml as
	// `broker.storage.proc_retention`. Defaults to 1h. Long-term
	// audit lives in JetStream `history-<sid>` (byte-bounded at
	// 1 GiB per session, no time expiry — see jsstream.go).
	ProcRetention time.Duration

	// ProcGCInterval is how often the broker sweeps EXITED rows
	// past ProcRetention. Defaults to 5 min. The yaml decoder in
	// internal/serveconf rejects sub-minute values as a
	// misconfiguration safety net; raw broker.Config constructed
	// inside _test.go files bypasses that decoder and can set
	// short intervals (broker.New imposes no minimum).
	ProcGCInterval time.Duration

	// XferReapInterval is how often the R7 registry re-runs the orphan
	// xfer-object reaper. Defaults to 5 min.
	//
	// Before R7 this reaper ran EXACTLY ONCE, in Run's JetStream-probe block —
	// and its gate is structurally false at that point on every cluster-mode
	// broker (raft catch-up is not yet established), so on a cluster the one run
	// it got was always skipped and there was never a second (#58/P10). R15
	// closed the second half of the same bug: the pass is now a per-broker,
	// catch-up-gated, HOME-authoritative reap (reaperCaughtUp + homeOwnsXferBucket)
	// rather than leader-only — a session homed to a follower was otherwise
	// unreapable by ANY node, so the pass ran forever and reaped nothing on it.
	XferReapInterval time.Duration

	// XferCrossHomeReapAge (R16 #58 Lane C) is the age floor for the leader-driven cross-home GC of a
	// split-home / zero-node OBJ_xfer bucket no single home broker can reap. Defaults to xferCrossHomeReapAge
	// (3× the tier-B timeout). Exposed so a deploy-tier drill can compress it and OBSERVE the GC without a
	// multi-minute wait; production has no operator tuning story (the default is derived, never hand-set).
	XferCrossHomeReapAge time.Duration

	// GrowLockReapInterval is how often the R7 registry checks for a
	// cluster_grow_active marker whose grow has already finished (#31 — the
	// CLI's lock release is best-effort, so a dropped release used to block
	// every subsequent grow/retire/upgrade forever). Defaults to 30s.
	GrowLockReapInterval time.Duration

	// UpgradeLockReapInterval (R7b) is how often the registry checks whether the
	// cluster_upgrade_active roll lock's LEASE has expired — i.e. whether the
	// `cluster upgrade` orchestrator that took it has stopped renewing. Defaults
	// to 30s (the grow lock's cadence). The reap cadence is deliberately three
	// orders of magnitude finer than the lease TTL: it controls only how promptly
	// an already-dead lock is noticed, never how long a live one survives.
	UpgradeLockReapInterval time.Duration

	// DrainMarkerReapInterval (batch C) is how often the registry looks for an ORPHANED
	// broker_draining marker — one whose node has no cluster_nodes row at all, which happens when the
	// retire ladder's marker-clear Propose fails (its error is discarded) and the roster row is then
	// deleted. The result is a permanent broker_draining alert naming a node that no longer exists.
	// Defaults to 30s, the same family as the two lock reapers above; deliberately its OWN field
	// rather than a reuse of GrowLockReapInterval, because one field with two meanings is the exact
	// defect batch B spent a phase removing.
	DrainMarkerReapInterval time.Duration

	// HomeDeliverInterval (R8a) is how often the registry re-delivers home
	// directives to agents whose CONFIRMED-applied home epoch is behind the
	// allocation's. Defaults to 5s.
	//
	// This is the cadence the batch's one-vote-veto invariant is measured against:
	// with the agent completely silent (no reconnect, no restart, no command), a
	// drain's rehome must reach the data plane within a BOUNDED time, and this
	// interval IS that bound (times the number of retries a failing rehome needs).
	// It is deliberately short: the register-reply path it backstops used to have
	// an unbounded delivery latency ("the next reconnect", i.e. possibly never).
	HomeDeliverInterval time.Duration

	// --- D9 cluster cutover surface (all zero ⇒ single mode, byte-equivalent) ---

	// ClusterDataDir is the raft sub-tree parent (raft/raft.db + raft/snapshots),
	// from serveconf broker.cluster.data_dir. Empty ⇒ no cluster intent. The
	// AUTHORITATIVE cluster-mode trigger is the on-disk raft/ probe
	// (clusterModeEnabled in cutover.go), cross-checked against this intent; a
	// non-empty value is only an intent, never the sole trigger (a flag that
	// drifts from disk reality would let node bootstrap overwrite a live DB).
	ClusterDataDir string

	// ClusterRaftAddr is the raft transport bind (host:port), private-net only.
	ClusterRaftAddr string

	// ClusterSecretsDir is the §15 secrets directory (cluster-ca, route leaf,
	// tunnel-cert, broker.nk, node-ident, account.nk). Required in cluster mode.
	ClusterSecretsDir string
	// StableTunnelCertDir optionally supplies tunnel-cert.pem/tunnel-key.pem in SINGLE mode. When
	// auth_callout provisioning already includes that pair, using it prevents every restart (and a
	// cluster migration rollback) from replacing the ephemeral tunnel identity agents have pinned.
	// Both absent preserves legacy ephemeral behavior; a partial pair is a fatal misconfiguration.
	StableTunnelCertDir string

	// NatsServerBin / NatsConfPath (C3) locate this host's nats-server binary + the live nats.conf the
	// topology reconciler renders/swaps/reloads. Empty NatsServerBin ⇒ "nats-server" (PATH). Empty
	// NatsConfPath disables the reconciler (no live conf to manage) — inert, status reports nothing.
	NatsServerBin string
	NatsConfPath  string

	// DBPath is the SQLite file path (storage.DB / --db). In cluster mode
	// cluster.Node owns the sole WAL handle on it (storage.OpenWAL) and serve.go
	// does NOT pre-open DB via storage.Open; in single mode DB carries the
	// already-open handle and DBPath is informational.
	DBPath string
}

// PortAllocCfg returns the internal/port.Config derived from this
// broker.Config.
func (c *Config) PortAllocCfg() *port.Config {
	cfg := &port.Config{Now: c.Now}
	if c.PortBandLow > 0 {
		cfg.BandLow = c.PortBandLow
	}
	if c.PortBandHigh > 0 {
		cfg.BandHigh = c.PortBandHigh
	}
	return cfg
}

// PortRevokeAfter returns the configured ALLOCATED→REVOKED threshold,
// defaulting to 15min if unset.
func (c *Config) PortRevokeAfter() time.Duration {
	if c.PortRevokeAfterDur > 0 {
		return c.PortRevokeAfterDur
	}
	return 15 * time.Minute
}

// ExposeForwardTimeout returns the configured broker→agent ACK
// timeout, defaulting to 5s.
func (c *Config) ExposeForwardTimeout() time.Duration {
	if c.ExposeForwardTimeoutDur > 0 {
		return c.ExposeForwardTimeoutDur
	}
	return 5 * time.Second
}

// UpgradeForwardTimeout returns the upgrade.req broker→agent ACK
// budget, defaulting to 30s (agent has to download + sha-verify
// before responding).
func (c *Config) UpgradeForwardTimeout() time.Duration {
	if c.UpgradeForwardTimeoutDur > 0 {
		return c.UpgradeForwardTimeoutDur
	}
	return 30 * time.Second
}

// Broker holds the running state of one `tether serve` instance.
type Broker struct {
	cfg Config

	// nc is the active NATS connection. Set by Run; nil before Run /
	// after shutdown. Stored as atomic.Pointer so the Run shutdown
	// path (clear on context cancel) can't race with subscription
	// callbacks still flushing through publishAudit / publishOnConn
	// on a different goroutine.
	nc atomic.Pointer[nats.Conn]

	// tunnelSrv is the reverse-TCP tunnel server. Non-nil (stored) only when
	// Config.TunnelControlAddr is set. Authorizes agent REGISTERs by
	// looking up port_allocations.token_hash. An atomic.Pointer (mirroring nc) so the
	// reconcile loop's read never races a concurrent install (production sets it once inside
	// Run before the loop; a few tests inject their own server post-Run for the data-plane drills).
	tunnelSrv atomic.Pointer[tunnel.Server]

	// js is the JetStream context this broker uses for audit publish
	// + history/events stream management. Non-nil only when the
	// underlying nats-server has JetStream enabled. When nil the
	// audit pubs fall back to core publish (P4-P6 behavior) — that
	// keeps existing tests / dev setups (no -js) working without
	// changes; a JetStream-enabled deployment activates the P7
	// upgrade automatically.
	js jetstream.JetStream

	// admin is the local-only Unix-socket admin server. Non-nil
	// only when Config.AdminSocketPath is set. Started after JS so
	// the audit endpoint can read history-<sid>.
	admin *adminsock.Server

	// runCtx is the context passed to Run; available to handlers
	// that need to derive sub-contexts so a graceful shutdown
	// propagates (audit shard 01 F7: finalizeSessionRm previously
	// built a context.Background-derived one). Nil before Run.
	runCtx context.Context

	// rosterStaleMu/rosterStaleWarned (C1 §6) track the per-(sid,nid) last-warned roster
	// generation so maybeEmitRosterStale emits at most one agent_roster_stale event per
	// agent per generation. Leader-only (handleRegister early-returns on a follower);
	// the map is created under the mutex on first use (zero-value safe, no New() change),
	// bounded (approx-evicted). Concurrent agent registers share the mutex.
	rosterStaleMu     sync.Mutex
	rosterStaleWarned map[string]uint64

	// manifestCache/manifestMu (C2) hold the time-bucketed, pre-signed cluster manifest served at the
	// well-known HTTP endpoint. The GET hot path is a pure atomic load; manifestMu single-flights the
	// re-sign (rate-limited, generation/age-gated). nil until the first GET in cluster mode.
	manifestCache atomic.Pointer[manifestSnapshot]
	manifestMu    sync.Mutex

	// transfers tracks in-flight file transfers (push/pull) so the
	// broker can write audit + reap OBJ_xfer-* buckets when the
	// receiver-finalization signal arrives or the watchdog fires.
	// file-transfer-plan §Object bucket lifecycle.
	transfers *transferTracker

	// xferReapMinAge is the grace below which a freshly-modified OBJ_xfer object is never reaped
	// (external review M-3). Defaulted to xferReapMinObjectAge in New; left 0 on a zero-value test
	// Broker so the reap-logic unit tests keep their prompt-reap semantics, and set explicitly by the
	// M-3 mutation test that proves a fresh object is shielded.
	xferReapMinAge time.Duration

	// reconcilers (R7) is the periodic-reconciliation registry that drives
	// every convergence pass from Run's single ticker. Wired in Run just
	// before the loop; nil before Run. See reconcile_registry.go for the
	// one-vote-veto invariant every registered pass must satisfy.
	reconcilers *reconcileRegistry

	// bootAt (R13) is the instant Run started (from the injectable clock), the base for the
	// OpRuntime uptime. Written ONCE at the top of Run, before the admin socket's accept goroutine
	// exists, so the admin-goroutine read in runtimeSnapshot is race-free by the goroutine-start
	// happens-before edge. Zero before Run ⇒ uptime reported as 0.
	bootAt time.Time

	// homeDeliveryState (R8a) is the ACTIVE home-directive delivery bookkeeping:
	// which public port the agent has CONFIRMED applied, and the per-node
	// re-delivery backoff. Leader-local convergence cache (not replicated),
	// created lazily so the many zero-value Broker literals in this package's
	// tests keep working. See home_delivery.go.
	homeDeliveryState *homeDeliveryState
	homeDeliveryOnce  sync.Once

	// proxyGen is this broker incarnation's P13 ordering generation, stamped onto
	// every ProxyDirective. It is persisted (proxy_meta) and can be ESCALATED at
	// runtime when a connected agent reports a higher applied generation after a
	// DB restore (round-5 F3). proxyGenMu guards the runtime read/escalate; the
	// one-time set in New happens before Run so it needs no lock.
	proxyGenMu sync.Mutex
	proxyGen   int64

	// proxyOpMu serializes all owner-visible P13 mutations across the switch and
	// subscriber subjects. Those subjects are separate NATS subscriptions, so
	// their callbacks may otherwise interleave: a sub mutation can observe ON,
	// then publish Enabled:true after a concurrent OFF has completed.
	proxyOpMu sync.Mutex

	// proxyRotate / proxyReadyTicks (h1 E2) are the leader-local M3 rotation
	// dampers: proxyRotate maps "sid/nid" → *backoff.Tracker (rotation
	// schedule 20s→40s→…→10min + the proxy_bind_stalled alert trigger at the
	// 3rd consecutive rotation), proxyReadyTicks maps "sid/nid" → consecutive
	// ready-tick count (the 60s clear-side hysteresis; short ready blips only
	// DECAY the tracker one step). Both are observe-loop-only — the reaper is
	// a single goroutine, so the Trackers inside are deliberately not
	// goroutine-safe (see internal/backoff pkgdoc). Entries are dropped on
	// recover + teardown (no leak).
	proxyRotate     sync.Map
	proxyReadyTicks sync.Map

	// proxyDwell (C5) is the leader-local consecutive-home-down tick counter (port → count) for the
	// proxy rehome hysteresis. Reset on a leadership change is fine (the new leader re-counts).
	proxyDwell sync.Map

	// autoRebalanceArm (G7a #18) is the leader-local debounce state for auto-rebalance-on-broker-return.
	// Confined to the single observe-loop goroutine; reset on a leadership-lost edge. Zero value usable.
	autoRebalanceArm autoRebalanceArm

	// rehomeEvt / proxyEvtCounts (C6) are the leader-local change caches that make the rehome + proxy
	// observability events idle-zero-writes: the reaper re-enters every ~5s, so a rehome event fires
	// only when its port→mark changes, and the proxy-count events fire only on a real prev→cur
	// transition (proxyEvtCounts holds the last proxyEventCounts per sid, fed to decideProxyEvents — the
	// single source of truth, C6-EVT-4). Reset on leadership change re-announces the current condition
	// once, which is the desired "new leader states the world" behavior.
	rehomeEvt      sync.Map
	proxyEvtCounts sync.Map

	// proxyOptedOut (#78) is the "won't vs can't" visibility hint for proxy
	// status: sid/nid → true when the node's last register carried
	// proxy_opt_out. In-memory single-broker hint ONLY — not replicated, and
	// empty after a broker restart until the agent's next re-register
	// (documented [GAP]). Enforcement lives in nodes.proxy_capable, which is
	// persisted and needs none of this.
	proxyOptedOut sync.Map

	// leaseHolder is a leader-local PROBE-AVOIDANCE cache for the
	// cloned-credential lease: "sid/nid" → the instance id that most recently
	// registered under that name.
	//
	// It is deliberately NOT the source of truth, and the contest test does NOT
	// require it to be populated. An in-memory map is empty after a broker
	// restart or a leader election, so a rule shaped
	// `contested := holder != "" && holder != req.InstanceID` would read as
	// UNCONTESTED for both live clones and hand them the same name in
	// sequence — restoring the fan-out with no clone arrival and no death.
	// Contest is therefore driven by nodes.last_heartbeat_at, which is in
	// SQLite and survives both events; this map only lets an incumbent's own
	// reconnect skip the interest probe.
	leaseHolder sync.Map

	// probeCache / probeInFlight back the OFF-HANDLER interest probe. See
	// probeVerdict: running it inline would serialize every other node's
	// register behind it and force its budget to be too small for a real WAN.
	probeCache    sync.Map
	probeInFlight sync.Map

	// selfID + tunnelCert are the D6 home/cert seam. Post-D9 cutover, in CLUSTER mode
	// wireClusterEarly calls AttachClusterSeam to set this broker's cluster identity + stable
	// cert, so every D6 home-assignment path (homeForRegister / homeForExpose / selfNodeID / the
	// tunnelTokenLookup home ladder / the stable cert) activates. In SINGLE mode both stay the
	// zero value, the home ladder is inert, and responses stay byte-identical to pre-D6. (The
	// per-phase TestD6ProductionWiresNoClusterNode guard was a build-and-prove scaffold removed
	// at the D9 cutover — see test/d9.)
	//
	// selfID is this broker's cluster_nodes.node_id (== cluster.Node.SelfID()). It is the "self"
	// the tunnelTokenLookup home_broker==self filter compares against; "" ⇒ the home ladder is
	// inert (single-node).
	selfID     string
	tunnelCert *tls.Certificate

	// transferAuditSink + transferAuditWG are the D8a (§9) transfer-audit forward seam.
	// In CLUSTER mode (post-D9 cutover) wireClusterLate calls AttachTransferAuditSink
	// (transfer_audit_forward.go) so start/complete/failed route through leader Apply
	// (OpTransferAudit, re-derivable); in SINGLE mode the sink stays nil and
	// emitTransferAudit falls through to the byte-identical best-effort pubAuditTransfer.
	// transferAuditWG tracks the async forward goroutines so the ordered shutdown (and the
	// leak gate) can drain them (WaitTransferAudit).
	// onCommitted (external review F1) is invoked ONLY after the record is durably COMMITTED. It is
	// how a terminal path drops the #57 in-flight ledger without ever deleting the evidence before the
	// audit it is evidence for exists. nil = the caller has nothing to release.
	transferAuditSink func(rec schema.AuditTransfer, onCommitted func())
	// transferAuditForwardSync (R16 #57) is the SYNCHRONOUS, error-returning forward core (same fwd.Forward
	// the async sink wraps). The #57 finalize-on-recovery pass uses it so it can DELETE the durable ledger
	// ONLY on a confirmed commit — a fire-and-forget emit would destroy the evidence in the leaderless
	// post-crash window (exactly #57's flagship case). Nil in single mode (finalizer never runs there).
	transferAuditForwardSync func(payload []byte) error
	transferAuditWG          sync.WaitGroup
	// transferAuditDraining (D9 round-1 MAJOR): once the ordered shutdown sets it, new audit
	// emits forward SYNCHRONOUSLY in the NATS handler instead of spawning a tracked goroutine
	// — so a transfer event arriving in the shutdown window is drained within nc.Drain's
	// callback wait, not lost by a goroutine spawned AFTER WaitTransferAudit already returned.
	// transferAuditMu (audit M7 / xx-concurrency F1) makes the {check draining + WaitGroup.Add}
	// pair in the sink INDIVISIBLE w.r.t. the shutdown's {set draining}, so an Add(1) can never
	// land after WaitTransferAudit() observed a zero counter (the classic "Add concurrent with
	// Wait at zero" misuse that would leak the goroutine + lose the audit). A bare atomic.Bool
	// cannot make the check+Add atomic; the mutex must.
	transferAuditDraining atomic.Bool

	// jsUnavail (G7b #20③) is the leader-observed SUSTAINED JetStream-503 signal: the AlertReconciler
	// sets it after the replica observation fails with code 10008 for >= the sustained window, and clears
	// it on the first positive observation or when this node is no longer leader. The cluster-health
	// responder reads it into ClusterHealthResp.JetStreamUnavailable (cross-goroutine → atomic).
	jsUnavail atomic.Bool

	// xferUnreapableBuckets (external review N-6) is the LAST xfer-reap pass's count of OBJ_xfer
	// buckets that hold aged orphan objects yet are home-owned by NO broker (a split-home or zero-node
	// session), so nothing will EVER reap them — the racknerd small-disk-fill class. A GAUGE, not a
	// counter: topology healing (rehome/evict) legitimately drops it back to 0. Cross-goroutine (the
	// reap pass writes, the /metrics + status responders read) → atomic. It is only refreshed when the
	// reap pass actually SCANS: on a gated pass (js==nil, or not reaperCaughtUp) the value is left STALE
	// rather than fabricated — `admin runtime` last_tick is the wedge detector, matching the package's
	// omit-don't-fabricate stance. Pure observability: the reaper's delete behavior is unchanged, and this
	// must NEVER be cited to soften #58 / drill 96 (only the per-transfer-owner refinement retires the gap).
	xferUnreapableBuckets atomic.Int64

	// reExecRequested (G5 #13) is set by RequestReExec so serve.go, AFTER Run's ordered shutdown returns,
	// re-execs the (freshly-staged) on-disk binary in place (PID-preserving; immune to the #23 clean-exit
	// trap). reloadTrigger is the run-context cancel serve.go injects so RequestReExec unwinds Run through
	// its EXISTING teardown rather than a bespoke shutdown path.
	reExecRequested atomic.Bool
	reloadTrigger   context.CancelFunc

	// clusterAdminHandle (G5 #13 W2b) routes an account-signature-verified remote upgrade trigger through
	// the SAME local admin backend the root Unix socket uses, so the reload/transfer gates are not
	// duplicated. It is set AFTER wireClusterLate (once the admin backend exists) but the trigger responder
	// subscribes earlier, so access is mutex-guarded (External-review M1: the plain-field read raced the
	// write) and the responder returns a RETRIABLE cluster_not_ready until it is set (not a terminal HALT).
	clusterAdminMu     sync.RWMutex
	clusterAdminHandle func(adminsock.Request) adminsock.Response
	transferAuditMu    sync.Mutex

	// xferReplicasFn is the D8a (§9) tier-B replica seam. In CLUSTER mode (post-D9) wireClusterLate
	// sets it (AttachXferReplicas) to ReplicasFor(NumVoters) so a completed tier-B object survives
	// a home-broker kill at N>=3. In SINGLE mode it stays nil and xferTargetReplicas() returns
	// jsstream.ReplicasSingle so a freshly-created OBJ_xfer bucket is byte-identically R=1.
	xferReplicasFn func() int

	// alertSink is the D8b (§10.2) disk-pressure forward seam. In CLUSTER mode (post-D9)
	// wireClusterLate sets it (AttachAlertSink) to forward the local disk state to the leader's
	// replicated alert store (VerbAlertSignal). In SINGLE mode it stays nil and the disk monitor
	// surfaces pressure only via the existing pubSysEvent (byte-identical).
	alertSink func(active bool)

	// clusterMode is the D9 cutover switch, decided once in New by clusterModeEnabled
	// (cutover.go): false ⇒ the byte-equivalent single-broker path (every pre-D9
	// deployment; advanceProxyGeneration runs, seams stay nil, no cluster.Node); true
	// ⇒ Run constructs cluster.Node and attaches every seam (the proxy path is off and
	// New does ZERO DB write, §16.4/§483). The build-and-prove guards are replaced by
	// TestD9ClusterMode{Off,On}* keyed on this switch.
	clusterMode bool

	// cl is the cluster-mode runtime (cluster.Node + forwarder + lifecycle), nil in
	// single mode. It is a broker-package type (cutover.go) so broker.go itself does
	// not import internal/cluster; Run builds it when clusterMode is true. All
	// authoritative writes route through cl in cluster mode (proposeOrForward); reads
	// use b.cfg.DB which Run re-points to cl.node.RODB().
	cl *clusterRuntime

	// forwards tallies authoritative raft write attempts by (verb, outcome) for
	// tether_broker_raft_forward_total (batch B, B2). Its ZERO VALUE is usable, so the ~126
	// package-internal &Broker{} literals need no change.
	forwards forwardCounters

	// lastObserveMu/lastObserve cache the leader's most recent per-peer observe (B5 OPS#1):
	// the /metrics scrape reads THIS (up to observeTickInterval≈5s stale) instead of re-running
	// the 2s-blocking pollClusterHealth scatter-gather on every request. nil until the first
	// leader observe tick; a follower leaves it as-is (its scrape shows is_leader 0 + stale peers,
	// which is honest).
	lastObserveMu sync.Mutex
	lastObserve   []peerObserve

	// B6 OPS#4: cache the most recent JS stream replica posture (min observed actual + target)
	// so the /metrics scrape reads it instead of re-running the blocking ObserveReplicas. The
	// alert reconciler's Observe tick refreshes it. 0/0 until the first observe (gauge omitted).
	lastReplicaMu     sync.Mutex
	lastReplicaActual int
	lastReplicaTarget int
	// lastReplicaStreams is how many streams the previous ObserveReplicas actually enumerated. It
	// sizes the NEXT observation's deadline (clusterReplicaObserveBudget), which is the only reason it
	// is kept — it is not a metric.
	//
	// External review RB2 doubt 3: the budget scaled with `COUNT(*) FROM sessions WHERE state='ACTIVE'`,
	// which correctly predicts the per-session history streams (ListSIDs is exactly that query) but
	// counts the `OBJ_xfer-*` streams as ZERO. Those are enumerated by ListXferStreams and can outlive
	// the session that created them, so a transfer-heavy or orphan-heavy broker was given a budget for a
	// fraction of the round trips it was about to make. The previous observation's own stream count
	// captures those streams; observeStreamCountForBudget maxes it with the live session floor so a
	// stale snapshot cannot regress below the old estimate.
	lastReplicaStreams int
}

// cacheReplicaSnapshot records TWO things from one observation: the worst-case (min actual) stream
// replica posture, which the /metrics gauge reads, and the number of streams enumerated, which is NOT a
// metric — it sizes the next observation's deadline (see lastReplicaStreams).
//
// A report that was not observed never changes the gauge. If it discovered a complete StreamCount before
// failing per-stream collection, that count DOES update the budget cache: otherwise a deadline caused by
// an OBJ_xfer burst preserves the stale short deadline forever. An unobserved report without a count
// leaves both untouched.
func (b *Broker) cacheReplicaSnapshot(rep ReplicaReport) {
	if !rep.Observed || len(rep.Streams) == 0 {
		if rep.StreamCount > 0 {
			b.lastReplicaMu.Lock()
			b.lastReplicaStreams = rep.StreamCount
			b.lastReplicaMu.Unlock()
		}
		return
	}
	minActual, target := rep.Streams[0].Actual, 0
	for _, s := range rep.Streams {
		if s.Actual < minActual {
			minActual = s.Actual
		}
		if s.Target > target {
			target = s.Target
		}
	}
	b.lastReplicaMu.Lock()
	b.lastReplicaActual, b.lastReplicaTarget = minActual, target
	if rep.StreamCount > 0 {
		b.lastReplicaStreams = rep.StreamCount
	} else {
		b.lastReplicaStreams = len(rep.Streams)
	}
	b.lastReplicaMu.Unlock()
}

// observedStreamCount is the previous observation's stream count, or 0 before the first one. It is the
// scaling input for clusterReplicaObserveBudget — see the field comment for why the session count alone
// under-counted.
func (b *Broker) observedStreamCount() int {
	b.lastReplicaMu.Lock()
	defer b.lastReplicaMu.Unlock()
	return b.lastReplicaStreams
}

// peerObserve is one cached per-peer health datum for the metrics endpoint.
type peerObserve struct {
	NodeID     string
	AppliedLag uint64
	Reachable  bool
}

// publishOnConn pubs through the broker's persistent NATS connection.
// Used by event broadcast helpers (ev.port etc) that don't sit on a
// msg.Reply and don't need persistence.
func (b *Broker) publishOnConn(subject string, payload []byte) error {
	nc := b.nc.Load()
	if nc == nil {
		return errBrokerNotConnected
	}
	return nc.Publish(subject, payload)
}

// publishAudit pubs an audit message. With JetStream available
// (b.js != nil) the message goes through js.Publish so it lands in
// the history-<sid> stream with at-least-once delivery + ACK.
// Without JetStream falls back to core publish, matching P4-P6
// behavior verbatim — that path is best-effort and lossy on
// reconnect, but it keeps non-JS dev/test setups working.
//
// Architecture H.5 / H.1 — audit subjects are the only thing
// history-<sid> filters on, so this is the only function that
// needs to know about JS for normal pub.
func (b *Broker) publishAudit(subject string, payload []byte) error {
	if auditTapForTest != nil {
		auditTapForTest(subject, payload)
	}
	if b.js != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := b.js.Publish(ctx, subject, payload)
		return err
	}
	nc := b.nc.Load()
	if nc == nil {
		return errBrokerNotConnected
	}
	return nc.Publish(subject, payload)
}

var errBrokerNotConnected = errBrokerSentinel("broker: not connected")

type errBrokerSentinel string

func (e errBrokerSentinel) Error() string { return string(e) }

// New validates the config and returns a Broker not yet connected. Run
// performs the actual NATS connect and blocks until ctx is canceled.
func New(cfg Config) (*Broker, error) {
	// g75-g78 external review F1: the pure config validators run FIRST, shared
	// with `serve --config-check` (validate.go), so the preflight and a real
	// start reach the same verdict on webhook URL / listen addresses.
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	// D9 (steps 1-3): decide single vs cluster mode FIRST (raft-probe based, no DB
	// needed), so the DB requirement can differ. Single mode needs an already-open DB
	// (cfg.DB, storage.Open by serve.go — byte-identical to pre-D9). Cluster mode lets
	// cluster.Node own the sole WAL DB: cfg.DB is nil here and Run sets b.cfg.DB =
	// node.RODB() after constructing the node. FATAL on any inconsistent combination.
	clusterMode, err := clusterModeEnabled(cfg)
	if err != nil {
		return nil, err
	}
	if clusterMode {
		if cfg.DBPath == "" {
			return nil, fmt.Errorf("broker: cluster mode requires DBPath (cluster.Node owns the DB)")
		}
		// D9 C-1 belt-and-suspenders: the cluster.Node is the SOLE writable WAL pool. A
		// non-nil cfg.DB means a caller (e.g. a regressed serve.go) opened a SECOND pool on
		// the same file — refuse, so the dual-handle corruption hazard can never reappear.
		if cfg.DB != nil {
			return nil, fmt.Errorf("broker: cluster mode must be constructed with a nil cfg.DB " +
				"(the FSM owns the single WAL pool; serve.go must skip storage.Open in cluster mode)")
		}
	} else if cfg.DB == nil {
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
	if cfg.ProcRetention == 0 {
		cfg.ProcRetention = time.Hour
	}
	if cfg.ProcGCInterval == 0 {
		cfg.ProcGCInterval = 5 * time.Minute
	}
	if cfg.XferReapInterval == 0 {
		cfg.XferReapInterval = 5 * time.Minute
	}
	if cfg.XferCrossHomeReapAge == 0 {
		cfg.XferCrossHomeReapAge = xferCrossHomeReapAge
	}
	if cfg.GrowLockReapInterval == 0 {
		cfg.GrowLockReapInterval = 30 * time.Second
	}
	if cfg.UpgradeLockReapInterval == 0 {
		cfg.UpgradeLockReapInterval = 30 * time.Second
	}
	if cfg.DrainMarkerReapInterval == 0 {
		cfg.DrainMarkerReapInterval = 30 * time.Second
	}
	if cfg.HomeDeliverInterval == 0 {
		cfg.HomeDeliverInterval = 5 * time.Second
	}
	b := &Broker{
		cfg:            cfg,
		transfers:      newTransferTracker(),
		clusterMode:    clusterMode,
		xferReapMinAge: xferReapMinObjectAge, // M-3: production shields fresh in-flight objects
	}

	// proxyGen (round-3/4/5) is this broker incarnation's ordering generation: a
	// PERSISTED monotonic counter (max(stored+1, now_nanos)) so it advances across
	// restarts even if the wall clock rolls back (F1). round-5 F2: an unreadable
	// counter is FATAL — we must NOT fall open to a bare wall clock.
	//
	// D9: this is the ONLY startup-time DB write, and it runs ONLY in single mode. The
	// P13 proxy path is OUT of v1 HA (§16.4/§483), so a cluster-mode broker neither
	// serves proxy subscribe nor advances proxy generation — New is side-effect-free
	// in cluster mode (cluster.Node owns the sole writer). The single-mode DB-marker
	// consistency check + proxy-gen are here; the cluster-mode consistency check runs
	// in Run AFTER cluster.Node opens the DB (cfg.DB is nil until then).
	if !clusterMode {
		if err := assertClusterDBConsistent(cfg.DB, false); err != nil {
			return nil, err
		}
		gen, err := advanceProxyGeneration(cfg.DB, cfg.Now().UnixNano(), 0)
		if err != nil {
			return nil, fmt.Errorf("broker: durable proxy generation unavailable (fencing would be unsafe): %w", err)
		}
		b.proxyGen = gen
	}
	return b, nil
}

// n1ClusteredJetStreamFatal is the cold-start diagnostic for a LONE voter whose nats.conf still
// carries a `cluster{}` block: a single node can never reach the clustered JetStream meta quorum, so
// JS never comes up and the broker crash-loops with no self-recovery.
//
// R10 P4: the remedy literal is the shared natsconf SSOT — the same sentence the DATA-PLANE-DEGRADED
// status banner and `cluster recovery restore`'s completion text emit. Those were three hand-copied
// copies, which is how the late one gets fixed and the early one rots.
//
// Extracted from Broker.Run so the guidance can be asserted at RUNTIME (the path itself lives after
// NATS/JetStream wiring and is not hermetically reachable). It used to be pinned by scraping this
// file's source text, which only worked while the remedy was a literal — a source pin cannot see
// through a format verb. Asserting the composed string is both stronger and refactor-proof.
func n1ClusteredJetStreamFatal() error {
	return fmt.Errorf("broker: cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 "+
		"node — a single node cannot form the clustered JetStream meta quorum. The nats.conf almost "+
		"certainly still has a `cluster{}` block: de-cluster it to standalone JS with `%s` %s; %s. "+
		"(Or, if mid-grow, finish the grow / remove the half-added peer.) N=1 MUST run standalone JetStream",
		natsconf.DeClusterRemedyCmd, natsconf.DeClusterRemedyArgHint, natsconf.DeClusterRemedyOfflineNote)
}

// Run connects to NATS, installs subscriptions, runs the reconcile ticker,
// and blocks until ctx is canceled. The NATS connection is drained on exit.
//
// If cfg.AuthCallout is non-nil, the broker presents its pre-configured
// nkey credentials on CONNECT (so it bypasses auth_callout itself) AND
// subscribes to $SYS.REQ.USER.AUTH to issue per-connection user JWTs.
func (b *Broker) Run(ctx context.Context) error {
	b.runCtx = ctx
	// R13: stamp the process boot instant for the OpRuntime uptime BEFORE anything spawns a
	// goroutine that could read it (the admin socket's accept loop, below). Written once here, read
	// in runtimeSnapshot from the admin goroutine — race-free via the goroutine-start ordering. Guard
	// nil Now: a few tests drive Run on a bare Broker literal that refuses early (e.g. the data-dir
	// lock interlock) and never reaches broker.New's Now default; they never serve the admin socket,
	// so bootAt staying zero (uptime 0) is correct there.
	if b.cfg.Now != nil {
		b.bootAt = b.cfg.Now()
	}
	// Round-5 B3: hold ${ClusterDataDir}/tether.lock for the WHOLE process lifetime. The offline recovery
	// tools already take this exact lock, but until now nothing else did — so their only protection against
	// a daemon reviving mid-surgery was a one-shot bolt probe taken minutes earlier, and a daemon that came
	// back inside that window could acknowledge writes into a store the rebuild was about to delete. Taking
	// it here makes the interlock continuous and bidirectional: a daemon refuses to start while a recovery
	// is in flight, and a recovery refuses to start while a daemon is up. The flock dies with the process,
	// so a crashed daemon never wedges recovery.
	if b.cfg.ClusterDataDir != "" {
		release, err := cluster.AcquireDataDirLock(b.cfg.ClusterDataDir)
		if err != nil {
			// Round-6: do NOT print "stop the previous broker" for a PERMISSION problem. A root-run recovery
			// leaves a root-owned tether.lock, and this daemon runs as tether: the operator's broker is
			// already stopped and the real fix is chown, not stopping anything. AcquireDataDirLock
			// distinguishes the two; carry that distinction to the operator verbatim.
			if errors.Is(err, cluster.ErrDataDirLockUnusable) {
				return fmt.Errorf("broker: refusing to start — %w", err)
			}
			return fmt.Errorf("broker: refusing to start — %w\n"+
				"  If an offline recovery (force-single / resnapshot / rejoin) is in progress, let it finish;\n"+
				"  if a previous broker is still running, stop it first (systemctl stop tether-broker)", err)
		}
		defer release()
	}
	if !b.clusterMode && b.cfg.StableTunnelCertDir != "" {
		cert, present, err := loadOptionalStableTunnelCert(b.cfg.StableTunnelCertDir)
		if err != nil {
			return err
		}
		if present {
			b.tunnelCert = cert
		}
	}

	// D9 cutover: in cluster mode cluster.Node owns the sole WAL DB. Construct it FIRST
	// (it opens the merged WAL + raft), re-point reads at its read-only handle, and
	// cross-check the DB-side migration marker now that the DB is open. All
	// authoritative writes route through b.cl (proposeOrForward); liveness writes go to
	// node.DB(). The full seam attach + leader-gated loops are wired below after the
	// NATS connect + JS probe. (Step 5 replaces this defer with an ordered shutdown.)
	if b.clusterMode {
		cl, err := b.buildClusterRuntime()
		if err != nil {
			return fmt.Errorf("broker: build cluster runtime: %w", err)
		}
		b.cl = cl
		b.cfg.DB = cl.node.RODB()
		if err := assertClusterDBConsistent(b.cfg.DB, true); err != nil {
			_ = cl.node.Shutdown()
			return err
		}
		defer func() { _ = b.cl.node.Shutdown() }()
		// Early seam: stable tunnel cert + selfID, BEFORE the tunnel server is created
		// (so it advertises the stable cert_fp that agents pin against).
		if err := b.wireClusterEarly(); err != nil {
			return err
		}
	}

	connOpts, err := b.brokerConnectOptions()
	if err != nil {
		return err
	}
	nc, err := nats.Connect(b.cfg.NATSURL, connOpts...)
	if err != nil {
		return fmt.Errorf("broker: NATS connect: %w", err)
	}
	b.nc.Store(nc)
	defer func() {
		// Drain first so all in-flight subscription callbacks
		// finish reading b.nc on this connection; only THEN clear
		// the pointer. The atomic.Pointer + drain-before-clear
		// ordering closes the publishAudit-vs-Run-shutdown race
		// that race-tested test/p4 used to expose intermittently.
		_ = nc.Drain()
		b.nc.Store(nil)
	}()

	// D9 round-1 BLOCKER: build the follower→leader forwarder BEFORE installAuthCallout so
	// the authcallout PIN provision/join writes can route through raft (the handler is built
	// next + needs the seam). wireClusterLate reuses this same forwarder (does not rebuild).
	if b.clusterMode {
		b.cl.forwarder = b.newForwarder(nc, b.cfg.ExposeForwardTimeout())
	}

	if subAuth, err := b.installAuthCallout(nc); err != nil {
		return err
	} else if subAuth != nil {
		defer func() { _ = subAuth.Unsubscribe() }()
	}

	if b.cfg.TunnelControlAddr != "" {
		host := b.cfg.TunnelPublicHost
		if host == "" {
			host = "0.0.0.0"
		}
		// D6 seam: newTunnelServer (home.go) returns the ephemeral-cert NewServer
		// in production (b.tunnelCert == nil) and the stable-cert NewServerWithCert
		// only when the harness attached a cert. Production is byte-equivalent to
		// the prior tunnel.NewServer call.
		srv := b.newTunnelServer(b.cfg.TunnelControlAddr, host, b.tunnelTokenLookup, b.cfg.Logger)
		if err := srv.Start(ctx); err != nil {
			return err
		}
		b.tunnelSrv.Store(srv)
	}

	// P13: read-only subscription HTTP surface (loopback; Caddy fronts it).
	// Disabled (no listener) when SubHTTPAddr is empty — pre-P13 deployments
	// are unchanged.
	if b.cfg.SubHTTPAddr != "" {
		// round-6 F10: bind SYNCHRONOUSLY and propagate failure from Run — a bad
		// config / occupied port must fail broker startup, not leave a
		// healthy-looking broker with no subscription endpoint.
		subLn, err := subhttp.Bind(b.cfg.SubHTTPAddr)
		if err != nil {
			return fmt.Errorf("broker: subscription http: %w", err)
		}
		b.cfg.Logger.Info("broker: subscription http listening", "addr", subLn.Addr().String())
		go func() {
			if err := subhttp.ServeListener(ctx, subLn, subhttp.Config{
				DB:          b.cfg.DB,
				PublicHost:  b.publicHostFor(),
				Logger:      b.cfg.Logger,
				ClusterMode: b.clusterMode, // C5: resolve each node's authoritative home host
				Vendable:    b.proxyVendable,
			}); err != nil {
				b.cfg.Logger.Error("broker: subscription http server", "err", err)
			}
		}()
	}

	// B5 OPS#1: Prometheus /metrics + /healthz + /readyz. Off by default (MetricsAddr empty) ⇒
	// no listener, no goroutine, byte-equivalent startup. Bind SYNCHRONOUSLY and propagate the
	// error (an occupied port fails startup, never a healthy broker with a dead metrics port).
	// The snapshot/ready closures are request-time (cheap raft-accessor reads + the cached
	// observe), never the 2s-blocking StatusReport.
	if b.cfg.MetricsAddr != "" {
		metLn, err := brokermetrics.Bind(b.cfg.MetricsAddr)
		if err != nil {
			return fmt.Errorf("broker: metrics http: %w", err)
		}
		b.cfg.Logger.Info("broker: metrics http listening", "addr", metLn.Addr().String())
		go func() {
			if err := brokermetrics.ServeListener(ctx, metLn, b.metricsSnapshot, b.metricsReady); err != nil {
				b.cfg.Logger.Error("broker: metrics http server", "err", err)
			}
		}()
	}

	// C2: the well-known cluster discovery manifest (loopback, Caddy-fronted). Gated on cluster mode
	// AND the opt-in flag → a non-cluster broker (or one without --cluster-manifest-listen) binds
	// NOTHING, starts NO goroutine → byte-equivalent startup. Bind SYNCHRONOUSLY (loopback-forced;
	// an occupied/non-loopback addr fails startup, never a healthy broker with a dead manifest port).
	if b.selfID != "" && b.cfg.ManifestAddr != "" {
		manLn, err := clustermanifest.Bind(b.cfg.ManifestAddr)
		if err != nil {
			return fmt.Errorf("broker: cluster manifest http: %w", err)
		}
		b.cfg.Logger.Info("broker: cluster manifest http listening", "addr", manLn.Addr().String())
		go func() {
			if err := clustermanifest.ServeListener(ctx, manLn, b.manifestBytes); err != nil {
				b.cfg.Logger.Error("broker: cluster manifest http server", "err", err)
			}
		}()
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
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.node.list.req", b.handleNodeListReq},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.started", b.handleProcEvent},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.exit", b.handleProcEvent},
		// P5 PTY control plane.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.run.req",
			func(msg *nats.Msg) { b.handleRunReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.kill.req",
			func(msg *nats.Msg) { b.handleKillReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.pty.*.failed", b.handlePtyFailed},
		// P6 data-plane control (expose / expose-rm).
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.expose.req",
			func(msg *nats.Msg) { b.handleExposeReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.expose-rm.req",
			func(msg *nats.Msg) { b.handleExposeRmReq(nc, msg) }},
		// P10 J.4 — `tether node upgrade <nid>`.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.upgrade.req",
			func(msg *nats.Msg) { b.handleUpgradeReq(nc, msg) }},
		// P11 file transfer (file-transfer-plan v0.2.0).
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.push.req",
			func(msg *nats.Msg) { b.handlePushReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.pull.req",
			func(msg *nats.Msg) { b.handlePullReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.push-commit.req",
			func(msg *nats.Msg) { b.handlePushCommitReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.ev.node.*.transfer.*.complete", b.handleEvTransfer},
		{proto.SubjectPrefix + ".s.*.ev.node.*.transfer.*.failed", b.handleEvTransfer},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.transfer.*.finalize.req", b.handleFinalizeReq},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.caps.req", b.handleCapsReq},
		// P13 proxy subscription control plane.
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.set.req",
			func(msg *nats.Msg) { b.handleProxySet(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.status.req", b.handleProxyStatus},
		// Mega-audit MAJ-2: register the CONCRETE create/revoke/list leaves, NOT the wildcard
		// `.proxy.sub.*.req`. The wildcard never matched isBroadcastClusterSubject's per-leaf
		// HasSuffix check, so it was queue-grouped and the leader-only create/revoke landed on a
		// silent follower ~(N-1)/N of the time → ctl timeout. create/revoke → broadcast+leader-only
		// (the handler's follower gate collapses the fan-out); list → queue-grouped RODB read.
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.create.req",
			func(msg *nats.Msg) { b.handleProxySub(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.revoke.req",
			func(msg *nats.Msg) { b.handleProxySub(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.list.req",
			func(msg *nats.Msg) { b.handleProxySub(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proxy.*", b.handleProxyReadyEvent},
	} {
		// D9 round-2 BLOCKER: in a ≥2-node cluster the leader-forwarded ctl-command + event
		// subjects must be handled by EXACTLY ONE broker per message (a QUEUE group), not
		// broadcast — otherwise every broker handles each command, the followers'
		// proposeOrForward all forward to the leader, and a follower's reply can race the
		// leader's (e.g. session.create returns a spurious "already exists"). One broker
		// handles + forwards/proposes; reads reply once. EXCEPTION (round-3 BLOCKER): the
		// FILE-TRANSFER subjects stay BROADCAST — their D8 routing needs the HOME broker (the
		// in-memory tracker holder) to see every message; the home gate + tracker-presence
		// already collapse the fan-out to one answering broker. Single mode is always plain
		// Subscribe (a 1-member queue group is behaviorally identical, but stay byte-exact).
		var (
			sub *nats.Subscription
			err error
		)
		if b.clusterMode && !isBroadcastClusterSubject(ss.subj) {
			sub, err = nc.QueueSubscribe(ss.subj, ctlQueueGroup, ss.handler)
		} else {
			sub, err = nc.Subscribe(ss.subj, ss.handler)
		}
		if err != nil {
			return fmt.Errorf("broker: subscribe %s: %w", ss.subj, err)
		}
		defer func(s *nats.Subscription) { _ = s.Unsubscribe() }(sub)
	}

	// Flush so the SUB protocol frames have actually reached the
	// NATS server before we signal ready. Without this, a fast test
	// can race ahead and fire a Request() to a subject the server
	// "knows about" via our nc but hasn't yet processed the SUB for.
	// 200ms is plenty for the loopback case + slow CI runners.
	_ = nc.FlushTimeout(200 * time.Millisecond)

	// Signal ready. Tests use this to gate request firing on a slow
	// runner; production callers leave Config.ReadyCh nil.
	if b.cfg.ReadyCh != nil {
		close(b.cfg.ReadyCh)
	}

	// P7 — try JetStream AFTER all subscriptions are installed so the
	// probe round-trip doesn't head-of-line block test code that's
	// already firing requests at us. AccountInfo is the cheapest call
	// that distinguishes "JS enabled" from "JS not present" (just
	// "jetstream.New(nc)" itself never errors — it's purely client-
	// side scaffolding). When the probe fails we keep b.js == nil and
	// audit publishes fall back to core publish (P4-P6 behavior).
	if js, err := jetstream.New(nc); err == nil {
		probeCtx, cancelProbe := context.WithTimeout(ctx, 1*time.Second)
		_, infoErr := js.AccountInfo(probeCtx)
		cancelProbe()
		if infoErr == nil {
			b.js = js
			b.cfg.Logger.Info("broker: JetStream enabled")
			ensureCtx, ensureCancel := context.WithTimeout(ctx, 5*time.Second)
			if err := jsstream.EnsureEventsStream(ensureCtx, js, jsstream.ReplicasSingle); err != nil {
				ensureCancel()
				return fmt.Errorf("broker: ensure events stream: %w", err)
			}
			ensureCancel()
			if err := b.reconcileHistoryStreamsOnBoot(ctx); err != nil {
				b.cfg.Logger.Warn("broker: history-stream boot reconcile", "err", err)
			}
			// file-transfer-plan §Object bucket lifecycle G.2 —
			// reap leftover OBJ_xfer-* streams from a previous crash.
			if n, err := b.reconcileXferObjects(ctx); err != nil {
				b.cfg.Logger.Warn("broker: OBJ_xfer boot reconcile", "err", err)
			} else if n > 0 {
				b.cfg.Logger.Info("broker: orphan xfer buckets reaped", "count", n)
			}
		} else {
			b.cfg.Logger.Debug("broker: JetStream not available; audit falls back to core publish",
				"err", infoErr)
		}
	}

	// D9 cutover late wiring: attach the D8 sinks, subscribe the cluster responders,
	// and start the leader-gated audit-publisher + alert-reconciler loops. After the JS
	// probe (the loops need b.js) and the NATS connect (nc).
	if b.clusterMode {
		if b.js == nil {
			// ACTIONABLE error (v0.4.4): the bare "enable JetStream" message cost a multi-step incident
			// diagnosis. The usual cause is a lone N=1 broker whose nats.conf still carries a `cluster{}`
			// block — a single node cannot form the clustered JetStream meta quorum, so JS never comes up
			// and the broker crash-loops with no self-recovery. Name the real remedy (de-cluster to
			// standalone) vs. the N>=2 mesh-not-formed case.
			voters, verr := b.cl.node.NumVoters()
			if verr == nil && voters <= 1 {
				return n1ClusteredJetStreamFatal()
			}
			// G2 #10: voters>=2 but no JetStream at boot is the force-single-EJECTED trap — this node's
			// on-disk raft config still lists {this, peer(s)} (voters>=2), but a survivor may have
			// force-singled to {survivor} WITHOUT it, so clustered JS can never reach quorum-of-2 and the
			// broker crash-loops (exit 70). Ejection is NOT locally provable (no quorum; peers unreachable
			// by design), so emit a RANKED DIFFERENTIAL (transient-mesh vs ejected) from the local raft
			// config only — never a hard assertion — and point at the crash-loop stop + rejoin runbook.
			if verr == nil && voters >= 2 {
				if cfg, cerr := b.cl.node.RaftConfiguration(); cerr == nil {
					var peers []string
					for _, s := range cfg {
						if s.NodeID != b.selfID {
							peers = append(peers, s.NodeID)
						}
					}
					if len(peers) > 0 {
						return fmt.Errorf("broker: cluster mode requires JetStream, but it is UNAVAILABLE at boot with "+
							"voters=%d — this node's on-disk raft config still lists peer(s) %v, yet clustered JetStream cannot "+
							"reach quorum. TWO likely causes: (1) TRANSIENT — the NATS routes mesh is not yet formed / peers are "+
							"down (verify routes in /etc/tether/nats.d/nats.conf and that peers are up; JS self-heals once quorum "+
							"returns); or (2) EJECTED — a survivor ran force-single WITHOUT this node, orphaning it (a restart will "+
							"NOT self-heal). STOP the crash-loop now: systemctl stop tether-broker. Confirm which on a survivor via "+
							"`tether cluster status --remote`; if this node was force-singled out, WIPE its stale raft state and "+
							"rejoin: `tether cluster recovery rejoin prepare --self-id %s --dump-divergent <file>`, then `tether "+
							"cluster join approve` from the leader (see docs/cluster-runbook.md)", voters, peers, b.selfID)
					}
				}
			}
			return fmt.Errorf("broker: cluster mode requires JetStream, but it is UNAVAILABLE (voters=%d) — the "+
				"NATS routes mesh is likely not formed (clustered JetStream meta needs quorum). Verify the routes "+
				"in /etc/tether/nats.d/nats.conf and that peer brokers are reachable; see `tether cluster status` / "+
				"`tether cluster doctor`", voters)
		}
		if err := b.wireClusterLate(ctx, nc); err != nil {
			return err
		}
		// D9 step 5: explicit ordered shutdown. Registered AFTER the nc.Drain defer (line
		// ~531) so it runs BEFORE it (LIFO) on EVERY return path: WaitTransferAudit while
		// nc is live → cancel+join the leader loops → unsubscribe responders. The Run-top
		// node.Shutdown defer (registered first) then runs LAST, after nc.Drain.
		defer b.clusterShutdownOrdered()
	}

	// P7 / H.1 — emit tetherd_restarted as the broker becomes
	// fully ready. Done after subscriptions + JS are wired so the
	// emission itself can land in the events stream.
	b.pubSysEvent("tetherd_restarted", map[string]any{
		"nats":      b.cfg.NATSURL,
		"jetstream": b.js != nil,
	})

	// P7 / H.4 — disk-pressure monitor. No-op when StoreDir empty.
	b.startDiskMonitor(ctx)

	// R7: build the reconciliation registry and register the standing passes HERE — before the
	// admin socket's accept goroutine exists — so runtimeSnapshot's cross-goroutine read of
	// b.reconcilers is race-free (the goroutine-start happens-before edge publishes the fully
	// populated registry). start()/granularity()/the driving ticker stay below, next to the loop.
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	// B7: wire the duration clock. Without it every pass reports a zero duration — inert, but a
	// reader with no writer, which this repo has shipped once before (loopset.go records it) and which
	// is worse than no field at all because the status output looks answered.
	b.reconcilers.now = b.cfg.Now
	b.registerCoreReconcilePasses()

	// P9 / I.2b — local admin socket. No-op when path empty (the
	// in-process tests that don't exercise admin leave it unset).
	if b.cfg.AdminSocketPath != "" {
		backend := adminsock.Backend{
			DB:              b.cfg.DB,
			Now:             b.cfg.Now,
			Logger:          b.cfg.Logger,
			AuditTail:       b.adminAuditTail,
			EventsTail:      b.adminEventsTail, // #30: operator reader for the H.1 sys.events stream
			PubAgentEvicted: b.pubAgentEvicted,
			// R13: process self-introspection (goroutines/threads/fds/rss/uptime + reconciler
			// last-tick) for `tether admin runtime`. Works in single AND cluster mode.
			RuntimeSnapshot: b.runtimeSnapshot,
		}
		// D9: in cluster mode the `tether cluster *` admin verbs are served by the
		// ClusterAdmin orchestrator (D7); nil in single mode ⇒ "cluster mode not enabled".
		if b.clusterMode {
			// b.cl.admin was constructed in wireClusterLate (independent of the socket). The REAL
			// per-follower catch-up cursor (`cluster add` catch-up gate) + stream-readiness probe
			// (`cluster drain --retire` AllAtTarget gate) ARE wired in production here via
			// b.clusterCaughtUp / b.clusterStreamsReady (External-review F1) — not the harness, and
			// NOT nil (the old D9-step-10b "nil for now" is no longer true; audit DOCS-MAJOR-2).
			backend.Cluster = NewClusterAdminBackend(b.cl.admin, b.clusterCaughtUp, b.clusterStreamsReady,
				b.adminAuditTail, func() ([]string, error) { return listSessionsByState(b.cfg.DB, "ACTIVE") },
				b.accountPubOrEmpty)
			// C-rebalance: wire the leader-local proxy-home spread pass (`cluster rebalance proxy`). The
			// concrete backend type is package-local, so set it via assertion (no NewClusterAdminBackend
			// signature ripple). nil in single mode ⇒ the op replies cluster_not_enabled.
			if cab, ok := backend.Cluster.(*clusterAdminBackend); ok {
				cab.rebalanceProxy = b.rebalanceProxyHomes
				cab.fsArm = b.cl.fsArm                     // online force-single dwell+token (shared with the observe tick that feeds it)
				cab.requestReExec = b.RequestReExec        // G5 #13: broker-daemon reload arms the self-re-exec
				b.setClusterAdminHandle(cab.HandleCluster) // G5 #13 W2b: remote signed trigger routes through the same gates
			}
			// D9 round-2 BLOCKER: route `admin evict` through raft (else the direct tx hits
			// the RODB handle and fails). Single mode leaves EvictWrite nil (direct tx).
			backend.EvictWrite = b.evictNode
			// batch B / B3: make a missing EvictWrite fail CLOSED instead of silently taking the
			// single-mode direct tx. Set LAST, alongside the seam — internal/broker's
			// TestAdminBackendClusterModeIsWiredBesideTheSeam asserts they stay together.
			backend.ClusterMode = true
		}
		b.admin = adminsock.New(b.cfg.AdminSocketPath, backend)
		if err := b.admin.Start(ctx); err != nil {
			return fmt.Errorf("broker: admin socket start: %w", err)
		}
		defer func() { _ = b.admin.Close() }()
		b.cfg.Logger.Info("broker: admin socket ready", "path", b.cfg.AdminSocketPath)
	}

	// R16 #57 (minor-2): eagerly finalize any transfer stranded by a PRIOR crash, so a broker that crash-loops
	// faster than XferReapInterval still closes dangling start rows (the periodic pass is anchored at
	// start+interval). M1 makes it safe when leaderless — the ledger is deleted only on a CONFIRMED commit.
	//
	// PLACEMENT IS LOAD-BEARING: this walks a ledger and, per stranded transfer, forwards to the leader with a
	// bounded retry (~500ms each when leaderless). It MUST run AFTER the admin socket is serving. Running it
	// earlier delays the socket, and the socket IS the joiner-readiness signal `tether cluster add` polls
	// (joinerBrokerUpLocal → OpClusterStatus): a joiner restarted mid-grow that is merely still finalizing
	// would be misread as "not up in cluster mode" and the grow would HALT at the start-joiner boundary.
	// Observed on the deploy tier (drill 42 returning-node re-grow) when this ran before the socket.
	if b.clusterMode {
		if n, ferr := b.finalizeStrandedXfers(ctx); ferr != nil {
			b.cfg.Logger.Warn("broker: eager stranded-transfer finalize on boot", "err", ferr)
		} else if n > 0 {
			b.cfg.Logger.Info("broker: eagerly finalized stranded in-flight transfers on boot (#57)", "count", n)
		}
	}

	// G.2 step ① — recompute node states from persisted
	// last_heartbeat_at once at boot. The ticker below would do this
	// on its own within ReconcileInterval, but doing it eagerly means
	// the very first ps / exec request after Run() returns ready
	// observes the correct ONLINE / STALE / OFFLINE labels (and the
	// 15-min OFFLINE port revoker fires immediately if applicable
	// instead of leaking up to one ReconcileInterval).
	bootNow := b.cfg.Now()
	if n, err := node.ReconcileStates(b.livenessDB(), bootNow,
		b.cfg.StaleAfter, b.cfg.OfflineAfter); err != nil {
		b.cfg.Logger.Warn("broker: boot node reconcile", "err", err)
	} else if n > 0 {
		b.cfg.Logger.Info("broker: boot node reconcile transitions", "count", n)
	}
	if revoked := b.reconcilePorts(bootNow); revoked > 0 {
		b.cfg.Logger.Info("broker: boot port revocations", "count", revoked)
	}
	if closed := b.reconcileTunnelSessions(); closed > 0 {
		b.cfg.Logger.Info("broker: boot stale tunnel proxies closed", "count", closed)
	}

	b.cfg.Logger.Info("broker: ready",
		"nats", b.cfg.NATSURL,
		"reconcile", b.cfg.ReconcileInterval,
		"stale_after", b.cfg.StaleAfter,
		"offline_after", b.cfg.OfflineAfter,
	)

	// R7: every periodic duty now lives in the reconciliation registry rather
	// than in hand-rolled ticker arms. The four bodies that used to be inlined
	// here are registered VERBATIM (reconcile_passes.go) and keep their exact
	// cadence, leadership gate and effects — see the fake-clock equivalence
	// proof in reconcile_registry_test.go. The two tickers collapse into one
	// driving ticker at the shortest registered interval; per-pass anchored
	// deadlines reproduce what the dedicated tickers did.
	//
	// Replacing two long-lived tickers with one (and adding zero goroutines) is
	// deliberate: the registry is driven by this loop's clock and owns no
	// timers, so it stays invisible to the repo's NumGoroutine/fd leak gate.
	//
	// The registry itself and its passes were created above (before the admin socket) so the
	// OpRuntime last-tick read is race-free; here we only anchor the schedule at boot.
	b.reconcilers.start(b.cfg.Now())

	// granularity() is 0 only if nothing registered, which registerCoreReconcilePasses
	// makes impossible — but time.NewTicker(0) panics, and a wiring mistake must not
	// take the broker down with a runtime panic. Fall back to the reconcile interval.
	granularity := b.reconcilers.granularity()
	if granularity <= 0 {
		b.cfg.Logger.Warn("broker: no reconcile passes registered; falling back to the reconcile interval")
		granularity = b.cfg.ReconcileInterval
	}
	ticker := time.NewTicker(granularity)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.cfg.Logger.Info("broker: shutting down")
			return ctx.Err()
		case <-ticker.C:
			b.reconcilers.runDue(ctx, b.cfg.Now())
		}
	}
}

// Cloned-credential lease defaults. See Config.LeaseGrace for the arithmetic.
const (
	DefaultLeaseGrace              = 6 * time.Second
	DefaultMaxInstancesPerBasename = 64
	// claimProbeBudget bounds the interest probe. It is only ever paid when a
	// recent heartbeat and a different instance id coincide: a live incumbent
	// answers in ~1ms locally, and a dead one returns ErrNoResponders
	// INSTANTLY (no-responders is a server-side signal, not a timeout). The
	// full budget is therefore only spent in a partition, where the answer is
	// "suffix" anyway.
	// claimProbeBudget bounds the interest probe.
	//
	// A CURRENT agent answers from its claim-probe arm in about a millisecond
	// locally, and a dead one returns ErrNoResponders instantly (no-responders
	// is a server-side signal, not a timeout). The full budget is therefore only
	// ever paid by a subscriber that does NOT implement the verb — a pre-feature
	// agent — or by a partition.
	//
	// IT IS DELIBERATELY SMALL because handleRegister is an async subscription
	// handler and nats.go delivers one subscription serially on ONE goroutine:
	// every millisecond spent here is a millisecond no OTHER node's register is
	// being handled. Making it generous would convert one ambiguous name into
	// fleet-wide register latency. Residual, recorded in the review: even at
	// this size the stall is real, and removing it needs a per-basename lock
	// with an asynchronous handler rather than a smaller number.
	claimProbeBudget = 50 * time.Millisecond

	// Lease failure reasons. These are LOG values, not wire codes: a lease that
	// cannot be issued degrades to today's behaviour rather than refusing the
	// register, so no ctl path ever observes them. They are named constants so
	// an operator can grep for them and so the two sites cannot drift apart.
	leaseReasonUnavailable   = "nid_lease_unavailable"
	leaseReasonBadInstanceID = "instance_id_invalid"
	leaseReasonStoreError    = "store_error"
	// leaseReasonProbePending is TRANSIENT: the interest probe is running off
	// the handler and the answer is not cached yet. The register is answered
	// with a retriable code rather than being decided on a guess.
	leaseReasonProbePending = "lease_probe_pending"
)

// leaseKey is the leaseHolder map key.
func leaseKey(sid, nid string) string { return sid + "/" + nid }

// probeVerdict caches one interest probe's answer.
//
// THE PROBE MUST NOT RUN INLINE. handleRegister is an async subscription
// handler and nats.go delivers one subscription SERIALLY on a single goroutine,
// so a blocking probe stalls every OTHER node's register behind it — a fleet
// re-registering after a broker restart would be serialized at the probe's
// budget. And an inline probe must be short to limit that damage, which makes
// its budget a constant rather than a function of the fleet's round-trip time:
// on this deployment the broker is a public VPS with agents on other
// continents, so a healthy lone agent that merely answers slowly would be
// renamed on the very path the identity answer was added to protect.
//
// So the probe runs in the background with a budget generous enough for a real
// WAN, and the register that needed it is answered with a TRANSIENT code the
// agent already retries. The retry lands on the cached verdict and is decided
// without blocking anyone.
// It caches the OBSERVATION, never a verdict: see probeAnswer for why an
// asker-relative answer cannot be shared between askers.
type probeVerdict struct {
	answer probeAnswer
	at     time.Time
}

// probeTTL is how long a cached verdict is trusted. It must be short enough
// that a name freed in the meantime is not held hostage, and long enough that
// the agent's own retry arrives INSIDE it — otherwise the retry misses the
// cache, launches a second probe, and the pair livelocks at one probe per
// retry.
//
// The arithmetic that fixes the value: the agent's register backoff caps at
// RegisterRetryMax (2s), and against a subscriber that never answers the probe
// itself burns backgroundProbeBudget (3s) before it writes anything. A verdict
// must therefore stay valid for at least 3s+2s after it lands. 10s carries
// that with room for a loaded broker.
const probeTTL = 10 * time.Second

// backgroundProbeBudget is the budget of the OFF-HANDLER probe. It can afford
// to be generous precisely because nothing waits on it — the value covers an
// intercontinental round trip with room to spare.
const backgroundProbeBudget = 3 * time.Second

// leaseSubscribeSettle is how long after a grant the broker still refuses to
// read silence as death.
//
// A granted agent has to get the reply, then install its forwarded
// subscription, before it can answer a claim probe at all. Until that has
// certainly happened, silence proves nothing — and treating it as proof puts a
// clone on the incumbent's name. Comfortably larger than the grant window and
// than any local register→subscribe turnaround, while staying far below the
// heartbeat grace, so a genuinely dead predecessor is still reclaimed within
// one ordinary restart.
const leaseSubscribeSettle = 5 * time.Second

// inlineProbeGrace is how long the register handler is willing to wait for a
// DEFINITE probe answer before deferring to the background.
//
// It exists to keep the common path exactly as fast as it was before the probe
// was made asynchronous. The two definite answers both arrive far inside it:
// ErrNoResponders is synthesised by the SERVER the moment it finds no interest
// (no round trip to any agent at all), and a live local agent's claim-probe
// reply is a single hop. What does NOT fit is the ambiguous case — interest
// exists but nothing answers — and that is the one case worth deferring.
//
// Keep it small. Every millisecond here is head-of-line latency for every other
// node's register, because nats.go delivers one subscription serially.
const inlineProbeGrace = 15 * time.Millisecond

// errProbePending is the transient outcome when a verdict is not cached yet.
var errProbePending = errors.New("lease: interest probe in flight; retry")

// leaseProbe returns a cached probe verdict, starting a background probe when
// there is none. ok=false means "ask again shortly" — never a decision.
func leaseProbe(b *Broker, sid, nid string, now time.Time) (v probeVerdict, ok bool) {
	key := leaseKey(sid, nid)
	if raw, hit := b.probeCache.Load(key); hit {
		if cached, _ := raw.(probeVerdict); now.Sub(cached.at) < probeTTL {
			// CONSUME IT. The entry exists for exactly one purpose: to let the
			// agent that was told "retry" land on the answer its own probe
			// produced. Once it has decided a register it has done its job.
			//
			// Leaving it readable for the rest of probeTTL lets an observation
			// outlive the process it describes. The incumbent dies, its
			// replacement registers inside the window, and a cached "somebody
			// else holds this" — evidence about a process that no longer exists
			// — renames the restarting agent. The name the operator addresses
			// then goes STALE while a perfectly healthy device sits under a
			// suffix nobody asked for.
			//
			// Re-probing instead costs the inline grace, and only in the
			// ambiguous branch.
			b.probeCache.Delete(key)
			return cached, true
		}
	}
	// INLINE GRACE FIRST — this is what keeps I1 intact.
	//
	// Going fully async cost more than it bought. The ordinary single-agent
	// RESTART lands here: the process is new (new instance id) while its own
	// row is still warm, so it takes the ambiguity branch. Under a synchronous
	// probe that case was already fast — the dead predecessor holds no
	// subscription, so the SERVER answers ErrNoResponders immediately, in about
	// a millisecond. Deferring it to a background goroutine replaced that
	// millisecond with a transient code plus a full agent backoff, which is a
	// visible slowdown on the most common path there is. (test/p4's
	// TestAgentReconnectsWithoutPINAfterBootstrap caught exactly this.)
	//
	// So: spend a few milliseconds inline. Any DEFINITE answer — nobody
	// subscribed, or somebody actually replied — is taken right here and the
	// handler never defers. Only the genuinely ambiguous shape, "interest
	// exists but nothing answered in the grace", goes to the background, and
	// that is precisely the shape that would otherwise have burned the full
	// budget on the handler goroutine.
	if a := probeObserve(b, sid, nid, inlineProbeGrace); a.definitive {
		// NOT cached. This observation is being consumed by the caller right
		// now, so storing it can only serve SOMEBODY ELSE later — and a probe
		// result is evidence about one moment, not a standing fact. Caching it
		// here is what let a "held by A" answer outlive A: A gets hard-killed,
		// its replacement registers, and a stored observation of a process that
		// no longer exists renames the restarting agent while the operator's
		// bare name goes STALE.
		//
		// The cache exists for exactly one job — carrying a background probe's
		// result to the retry that was told to come back — and that is the only
		// place that writes it.
		return probeVerdict{answer: a, at: now}, true
	}

	// Single-flight: only the first caller launches the background probe.
	if _, busy := b.probeInFlight.LoadOrStore(key, true); busy {
		return probeVerdict{}, false
	}
	startedAt := now
	go func() {
		defer b.probeInFlight.Delete(key)
		a := probeObserve(b, sid, nid, effectiveBackgroundProbeBudget())
		// RE-CHECK SILENCE BEFORE RECORDING IT.
		//
		// This probe may have spent three seconds, and "nobody answered" is a
		// statement about the moment it was asked. The incumbent very plausibly
		// finished subscribing DURING those three seconds — that is the whole
		// reason the ambiguous branch existed — so a silence observed at the
		// start is not silence now. Writing it down anyway lets the silence rule
		// declare a live, answering agent dead and hand its name to a clone.
		//
		// One more grace-length ask settles it. Only for silence: a definite
		// answer is already evidence about a responder that spoke.
		if !a.answered {
			if recheck := probeObserve(b, sid, nid, inlineProbeGrace); recheck.answered {
				a = recheck
			}
		}
		// DO NOT CLOBBER A NEWER OBSERVATION.
		//
		// This probe may have spent the full background budget, and the world
		// moves inside it: the incumbent finishes subscribing and answers a
		// later, INLINE probe, which caches a fresh "it is alive". Storing this
		// result unconditionally would then overwrite that with evidence
		// gathered BEFORE the incumbent could speak, and the silence rule would
		// read a live, answering agent as dead and hand its name to a clone.
		//
		// Timestamp order is the whole test: keep whichever observation is
		// younger. Losing this write costs nothing — the next register probes
		// again.
		//
		// DEFENSIVE, NOT REACHABLE TODAY (measured 2026-09-01, test-system overhaul internal
		// review L6-F1). Under single-flight probeInFlight the only writer of this cache is this
		// goroutine: an inline definitive answer is deliberately not cached (above), and a second
		// background probe cannot start before this one has stored and released the key. So no
		// path exists on which `prev` is younger than `startedAt`, and disabling this guard reddens
		// nothing — not lease_probe_staleness_test.go (whose header says which of the two defences
		// it really pins), not the lease model. It stays because it is cheap and because the day a
		// second cache writer appears it becomes the only thing between that writer and the silence
		// rule; that day, write the test that reddens without it.
		if prev, ok := b.probeCache.Load(key); ok {
			if pv, _ := prev.(probeVerdict); pv.at.After(startedAt) {
				return
			}
		}
		// b.now(), not time.Now: leaseProbe judges this verdict's freshness with the `now` its
		// CALLER passes (`now.Sub(cached.at) < probeTTL`), and that `now` is the injected clock.
		// Stamping the cache with the wall clock was the one place the lease path mixed two
		// clocks — harmless in production (both are the wall clock) and fatal to any test that
		// drives the state machine with an injected one: a verdict stamped "now" by the wall
		// looks arbitrarily old or arbitrarily fresh to a fake clock. origin:
		// docs/reviews/test-system-overhaul-plan.md B3 (distributed D1). (The first version
		// added a package-level brokerNow with the same body as the existing (*Broker).now —
		// two readers of one invariant; internal review L3-F3.)
		b.probeCache.Store(key, probeVerdict{answer: a, at: b.now().UTC()})
	}()
	return probeVerdict{}, false
}

// LeaseGrantWindow is the read-only accessor for leaseGrantWindow, exported so cross-package tests
// can calibrate against the constant instead of a literal that goes stale when the constant moves
// (test/p2 slept 1200ms "deliberately LONGER than leaseGrantWindow" for six weeks after the window
// became 5s; docs/testing-standards.md T7). Not configuration: there is no setter and never will be
// — the window's value is argued in the comment above leaseSubscribeSettle.
func LeaseGrantWindow() time.Duration { return leaseGrantWindow }

// leaseGrant is what leaseHolder stores: WHO was granted a name and WHEN.
//
// The timestamp is load-bearing, not diagnostic. Within leaseGrantWindow of a
// grant this map — not the interest probe — is the authority on whether a name
// is taken, because a just-granted agent has registered but not yet subscribed
// (its order is connect → register → subscribe, and the broker replies last).
// A probe in that window answers ErrNoResponders and would hand a second clone
// the same name.
type leaseGrant struct {
	instanceID string
	grantedAt  time.Time

	// released marks a holder that said goodbye (ReleasingName).
	//
	// The entry is KEPT rather than deleted, because deleting it throws away the
	// one fact the successor's adjudication needs: that the last holder was a
	// lease-aware process. Without that, silence from the departed agent is
	// "uninformative" instead of "it is gone", and the successor gets suffixed —
	// so DELIVERING a farewell renamed the device while LOSING it did not,
	// exactly backwards from a best-effort optimisation.
	//
	// Kept and flagged, the entry means "this name was held, by someone who has
	// now left". That is strictly more information than an empty map.
	released bool
}

// leaseGrantWindow is how long a fresh grant is treated as authoritative
// without a probe.
//
// IT MODELS EXACTLY ONE GAP: between the broker replying to a grant and that
// agent installing its forwarded subscription. The agent does both in adjacent
// steps of one function, so the real gap is milliseconds; two seconds is a
// generous ceiling for a loaded host.
//
// SIZING IT LARGER IS A REAL DEFECT, not extra safety. The window suppresses
// the probe, and the probe is the ONLY thing that can tell a dead predecessor
// from a live one. A 30s window (the first value here) made a fast agent
// restart — crash loop, `systemctl restart`, an upgrade re-exec — look like a
// second clone arriving, because the grant to its predecessor was still
// "recent". It renamed a durable device, which is precisely the outcome the
// whole design is built to avoid. Caught by
// TestRestartedInstanceReclaimsItsNameRatherThanBeingSuffixed.
//
// SIZING IT SMALLER IS ALSO A REAL DEFECT, and one only the deploy-tier drill
// could show. At one second it stopped covering the gap it models. In
// simcluster drill 83 the incumbent re-registered at 06:55:16 and the clone was
// adjudicated at 06:55:18: two seconds later, so past the window, but still
// inside the incumbent's register→subscribe turnaround on a loaded container
// host. The probe then asked a name nobody had subscribed to yet, the server
// answered ErrNoResponders — correctly, there WAS no interest — and the clone
// was handed the bare name. Both instances subscribed to one forwarded subject
// and B2b/B2c caught one command producing two start rows and two exit rows:
// the exact fan-out this increment exists to remove, reintroduced by a timing
// constant. Hermetic tests could not see it; every one of them registers and
// subscribes inside one fast process.
//
// So it is now the SAME constant as leaseSubscribeSettle, because it is the
// same physical quantity seen from both sides: how long after a grant the
// broker still refuses to draw conclusions from the probe. Before the settle
// point, silence means "not subscribed yet" and the grant is authoritative;
// after it, silence means "dead" and the probe is authoritative.
//
// A restart quicker than this is handled by the departing agent's farewell
// (ReleasingName), not by shrinking the window — that is what makes the fast
// path fast WITHOUT reopening the fan-out.
const leaseGrantWindow = leaseSubscribeSettle

// adjudicateLease decides whether this register is CONTESTED — i.e. whether a
// different live process already holds the name being presented.
//
// Returns a non-nil lease when the caller must reply with it and TOUCH NOTHING
// ELSE. That ordering is the whole fix: a contested register that fell through
// to registerNode would (a) zero the incumbent's proxy_ready through
// node.Register's ON CONFLICT clause, knocking a healthy exit out of /sub, and
// (b) walk the incumbent's RUNNING process rows in reconcileOnRegister, find
// them absent from this agent's LocalProcesses, and close them all with
// ExitMark{-1} — after which the incumbent's own next register sees them as
// orphans and SIGKILLs the operator's work. Both fall out of the ordering
// alone; reconcile.go is not touched.
//
// It is a package-level function taking *Broker rather than a method because
// the structural-budget ratchet pins internal/broker.Broker's method count
// exactly, and growing a type's surface is the thing that gate exists to make
// deliberate.
func adjudicateLease(b *Broker, sid, nid string, req *proto.NodeRegisterReq, now time.Time) (*proto.NodeLease, string, error) {
	// A pre-feature agent advertises nothing, and MUST get exactly today's
	// behaviour — including the clone fan-out, which is the pre-upgrade
	// baseline rather than a regression. A non-empty InstanceID IS the
	// capability advertisement (deliberately not a Capabilities token, which
	// would perturb every lone agent's register body).
	if req.InstanceID == "" {
		return nil, "", nil
	}
	// THE CO-LOCATED AGENT IS EXEMPT, and this is a safety interlock rather
	// than a courtesy (external review F5; plan §2 already adjudicated it).
	//
	// A cluster broker upgrade drives its own host's agent by publishing to a
	// STATIC nid it was configured with (cluster_upgrade_trigger.go). That name
	// is not negotiable: if this agent were ever handed a suffix, the upgrade
	// leg would be addressed to a name nobody is subscribed to, and the host
	// would stop half-upgraded — broker on the new binary, agent on the old,
	// with no path forward that does not involve physical access.
	//
	// One machine runs one co-located agent by construction, so exempting it
	// costs nothing: there is no clone for it to be confused with.
	if b.cfg.ColocatedAgentNID != "" && nid == b.cfg.ColocatedAgentNID {
		return nil, "", nil
	}
	if err := proto.ValidateInstanceID(req.InstanceID); err != nil {
		return nil, leaseReasonBadInstanceID, err
	}

	now = now.UTC()
	// priorHolderSpoke records that the LAST process granted this name was
	// itself a lease-aware agent. That single bit is what lets an ordinary
	// restart keep its name — see the silence rule at the end of this function.
	priorHolderSpoke := false
	priorReleased := false
	var priorGrantedAt time.Time
	if v, ok := b.leaseHolder.Load(leaseKey(sid, nid)); ok {
		if g, _ := v.(leaseGrant); g.instanceID != "" {
			priorHolderSpoke = true
			priorGrantedAt = g.grantedAt
			priorReleased = g.released
			// Fast path: the recorded holder reconnecting. Idempotent by
			// construction — without matching on the instance id, every NATS
			// reconnect would burn a fresh suffix (gpu1 → gpu1-02 → …) while
			// the same live process still held the previous ones.
			if g.instanceID == req.InstanceID {
				return nil, "", nil
			}
			// THE SIMULTANEOUS-LAUNCH CASE, and the reason this branch does not
			// consult the probe.
			//
			// Both registers land on the same leader and nats.go delivers them
			// SERIALLY on one goroutine, so when a different instance was
			// granted this name moments ago, this map is authoritative — more
			// authoritative than any probe can be. The probe cannot see the
			// incumbent yet: the agent's order is connect → register →
			// subscribe, and the broker replies at the very END of
			// handleRegister, so the first clone has not subscribed when the
			// second is adjudicated. Asking NATS would answer ErrNoResponders
			// and hand BOTH of them the basename — which is the canonical
			// deployment shape (`kubectl scale`, `rollout restart`, or both
			// pods returning after a broker outage), not an exotic race.
			// …unless that holder has SAID IT IS LEAVING. The window is a proxy
			// for "the incumbent may be alive but not yet visible to a probe";
			// an explicit farewell answers that question directly and outranks
			// the guess. Skipping the window here is what lets a restart inside
			// it keep its name — the entire point of the farewell.
			if !g.released && now.Sub(g.grantedAt) < leaseGrantWindow {
				return assignLeaseName(b, sid, configuredBasename(b, sid, nid, req), now)
			}
		}
	}

	// Contest is driven by SQLite, not by the in-memory map: the map is empty
	// after a broker restart or a leader election, and a rule that required a
	// recorded holder would pass BOTH live clones through as uncontested.
	age, exists, err := node.HeartbeatAge(b.read().SQL(), sid, nid, now)
	if err != nil {
		return nil, leaseReasonStoreError, err
	}
	grace := b.cfg.LeaseGrace
	if grace <= 0 {
		grace = DefaultLeaseGrace
	}
	if !exists {
		// No row at all: nobody has ever held this name in this session. No
		// probe — this is the first register of every ordinary agent and must
		// stay fast.
		b.leaseHolder.Store(leaseKey(sid, nid), leaseGrant{instanceID: req.InstanceID, grantedAt: now})
		return nil, "", nil
	}
	if age > grace {
		// The previous holder has been silent past the grace window. The clock
		// says it is gone — but a clock is not evidence. Gotcha #72 is exactly
		// an agent whose heartbeat stopped while its socket stayed ESTABLISHED
		// and its subscriptions stayed live (10m58s, live-confirmed on this
		// fleet), and granting its name away would put two processes on one
		// forwarded subject with the incumbent still serving traffic.
		//
		// So ask. A dead predecessor answers ErrNoResponders instantly, so the
		// ordinary restart path pays nothing measurable; only the ambiguous
		// case pays the budget.
		v, ready := leaseProbe(b, sid, nid, now)
		if !ready {
			return nil, leaseReasonProbePending, errProbePending
		}
		// An UNKNOWN answer here (unparseable, or nobody answered inside a
		// WAN-sized budget) falls back to the clock, and the clock already said
		// the holder is long gone. Taking over is the right call: the
		// alternative renames a device for being far away.
		// Rendered for THIS asker — the cache holds the observation, not a
		// verdict, precisely so a stale answer computed for a different instance
		// cannot be replayed here.
		if held, known := v.answer.heldByOther(req.InstanceID); !held || !known {
			b.leaseHolder.Store(leaseKey(sid, nid), leaseGrant{instanceID: req.InstanceID, grantedAt: now})
			return nil, "", nil
		}
		return assignLeaseName(b, sid, configuredBasename(b, sid, nid, req), now)
	}

	// Ambiguous window: a recent beat AND a different instance id. Ask whether
	// the incumbent is actually a live subscriber rather than guessing from a
	// clock — a hard-killed agent that restarts in under LeaseGrace is
	// indistinguishable from a clone by timestamp alone.
	v, ready := leaseProbe(b, sid, nid, now)
	if !ready {
		return nil, leaseReasonProbePending, errProbePending
	}
	held, known := v.answer.heldByOther(req.InstanceID)
	// The ambiguous branch is rare (it needs a warm beat AND a different
	// instance id), and it is where every hard call is made. Log the evidence,
	// not just the outcome: which of "someone else holds it", "nobody is there"
	// and "nobody answered" was observed is exactly what an operator asking
	// "why did my device get renamed?" needs, and it cannot be reconstructed
	// afterwards.
	b.cfg.Logger.Info("broker: adjudicating a contested node name",
		"sid", sid, "nid", nid, "asker", req.InstanceID,
		"answered", v.answer.answered, "responder", v.answer.responder,
		"held_by_other", held, "answer_known", known,
		"prior_holder_known", priorHolderSpoke, "beat_age", age.String())
	// Inside the grace window an UNKNOWN answer keeps the conservative reading:
	// the clock says a holder was alive moments ago, so a name that nobody
	// clearly disowned stays taken. This is the one place where suffixing a
	// possibly-live device is the safer error — it self-corrects on the next
	// restart (the suffix is never persisted), while two processes on one name
	// do not.
	if !held && known {
		b.leaseHolder.Store(leaseKey(sid, nid), leaseGrant{instanceID: req.InstanceID, grantedAt: now})
		return nil, "", nil
	}

	// THE SILENCE RULE — what makes an ordinary restart keep its name.
	//
	// Getting here means interest exists on the name but nothing answered the
	// claim probe. Read literally that is ambiguous, and the conservative
	// reading above would suffix. But it is only ambiguous when we know nothing
	// about who held the name. When priorHolderSpoke is set, this broker
	// GRANTED the name to a lease-aware process, and a lease-aware process that
	// is still running always answers claim-probe (internal/agent,
	// replyClaimProbe, served off the forwarded subscription). So silence from
	// a holder we know can speak is evidence of DEATH, not of distance.
	//
	// The interest that remains is the dead process's subscription, which the
	// server has not finished reaping — every ordinary restart looks exactly
	// like this, and test/p4's TestAgentReconnectsWithoutPINAfterBootstrap is
	// that shape: stop the agent, start it again under the same nid, and the
	// old subscription is still in the interest table when the new process
	// registers. Suffixing there would rename a device for restarting, and the
	// bare name would go STALE while the user kept addressing it.
	//
	// THE SETTLE WINDOW IS LOAD-BEARING — without it this rule reintroduces the
	// very defect the increment exists to remove.
	//
	// "A live lease-aware holder always answers" only becomes true once that
	// holder has SUBSCRIBED, and the agent's order is connect → register →
	// subscribe. A clone arriving while the incumbent is still between its
	// register and its subscribe meets silence from a perfectly healthy
	// process. Reading that silence as death hands the clone the bare name, and
	// both then serve one forwarded subject: every exec runs twice.
	//
	// That is not hypothetical — it is what the simcluster deploy-tier drill
	// (83-cloned-image-instances) reported the moment this rule was added
	// without the window: B1 (two distinct rows) failed and B2b/B2c caught ONE
	// command producing TWO start rows and TWO exit rows. The grant window
	// covers the first second; past it, only elapsed time can establish that a
	// living holder would have finished subscribing and answered.
	//
	// The rule deliberately does NOT fire when the holder map is cold (broker
	// restart / leader election): then we have no evidence about the incumbent
	// and the conservative branch is right. It also does not fire for a
	// pre-feature holder, which never recorded an instance id and so never sets
	// this bit — silence from one of those really is uninformative.
	// A RELEASED holder needs no settle time: it did not merely go quiet, it
	// announced its departure. Waiting out the window there would suffix a
	// device for restarting promptly, which is the case the farewell exists to
	// make fast.
	if priorHolderSpoke && !known && (priorReleased || now.Sub(priorGrantedAt) > leaseSubscribeSettle) {
		b.leaseHolder.Store(leaseKey(sid, nid), leaseGrant{instanceID: req.InstanceID, grantedAt: now})
		return nil, "", nil
	}

	return assignLeaseName(b, sid, configuredBasename(b, sid, nid, req), now)
}

// replyLeaseVerdict adjudicates the cloned-credential lease and, when the
// register is CONTESTED, answers it and reports true so the caller returns
// having touched nothing else.
//
// POSITION IS LOAD-BEARING IN BOTH DIRECTIONS at the call site: after the
// RosterRefreshOnly short-circuit (a periodic refresh carries no InstanceID and
// would otherwise be adjudicated ~20 times an hour per agent for nothing), and
// STRICTLY BEFORE registerNode — in single mode, which is what the live fleet
// runs, registerNode IS the upsert, so a verdict reached after it has already
// overwritten the incumbent's row. That ordering, not any change to
// reconcile.go, is what stops a clone from zeroing the incumbent's proxy_ready
// and from closing every process row it is running.
//
// A package-level function taking *Broker: the structural-budget ratchet pins
// this type's method count exactly, and extracting it also keeps handleRegister
// inside the maintainability-index gate.
func replyLeaseVerdict(b *Broker, msg *nats.Msg, sid, nid string, req *proto.NodeRegisterReq) bool {
	// A FAREWELL, handled before anything else: the agent is stopping cleanly and
	// is handing the name back. Forget who held it so the next process to present
	// this credential is adjudicated as a first arrival and keeps the bare name.
	//
	// Deliberately does NOT touch the nodes row: liveness stays the heartbeat's
	// job, and rewriting status here would race the ordinary OFFLINE sweep.
	// Deliberately not authenticated beyond the register subject's own ACL
	// either — the worst a forged farewell can do is discard a cache entry that
	// the interest probe and the heartbeat clock immediately reconstruct.
	if req.ReleasingName && req.InstanceID != "" {
		if v, ok := b.leaseHolder.Load(leaseKey(sid, nid)); ok {
			// Only the CURRENT holder may release: a straggler clone saying
			// goodbye must not hand away the name its sibling now holds.
			if g, _ := v.(leaseGrant); g.instanceID == req.InstanceID {
				// FLAG, do not delete — see leaseGrant.released.
				g.released = true
				b.leaseHolder.Store(leaseKey(sid, nid), g)
			}
		}
		b.probeCache.Delete(leaseKey(sid, nid))
		// TAKE THE ROW OFFLINE TOO, or the name is not actually released.
		//
		// external review F11: clearing only the in-memory grant left the nodes
		// row ONLINE for the full OfflineAfter window (60s), and the allocator
		// reads that row — so the name this agent just handed back still counted
		// as occupied and its own restart was issued the NEXT suffix. A device
		// that restarts every few minutes walks -02, -03, -04 … until the suffix
		// space is exhausted and it is refused outright, while operators' saved
		// commands, exposes and scripts keep pointing at names that are now
		// ghosts showing ONLINE.
		//
		// Only for a name this agent actually held, and only for a LEASE (a
		// configured device's row is not ours to take offline on a farewell —
		// its liveness is the heartbeat's business). The write is scoped to the
		// row and idempotent.
		if _, _, isLease := proto.SplitLeaseName(nid); isLease && req.LeasedNID {
			if err := markNodeOfflineOnRelease(b, sid, nid); err != nil {
				b.cfg.Logger.Warn("broker: could not take a released lease row offline",
					"err", err, "sid", sid, "nid", nid)
			}
		}
		// Stop here. Falling through would run the ordinary register and stamp a
		// FRESH heartbeat for a process that is on its way out — making the row
		// look more alive at the moment of death than it did while running.
		b.replyJSON(msg, proto.NodeRegisterResp{OK: true})
		return true
	}

	lease, code, lerr := adjudicateLease(b, sid, nid, req, b.now().UTC())
	// Say WHY, every time a name is adjudicated to anything other than "keep it".
	//
	// A device that silently turns into gpu1-02 is the single most confusing
	// thing this feature can do to an operator, and the deciding evidence
	// (heartbeat age, who answered the probe, which branch fired) exists only
	// here. Without this line the only way to find out is to attach a debugger
	// to a production broker.
	if lease != nil || code != "" {
		b.cfg.Logger.Info("broker: node name adjudicated",
			"sid", sid, "requested_nid", nid, "instance", req.InstanceID,
			"assigned", leaseAssignedName(lease), "reason", code)
	}
	if lease == nil {
		if code == leaseReasonProbePending {
			// TRANSIENT, and deliberately NOT a degrade: degrading would run the
			// full register under a name we have not yet established is free,
			// which is the double-execution this increment exists to prevent.
			// CodeLeaderUnavailable is the code the agent's register loop
			// already retries — reusing it means no new client behaviour and no
			// new exit-class mapping.
			b.replyErr(msg, proto.CodeLeaderUnavailable,
				"adjudicating this name against the current holder; retrying shortly")
			return true
		}
		if code != "" {
			// DEGRADE, NEVER REFUSE. A refused register makes an agent flap or
			// exit (connectNATS treats an initial auth failure as fatal), so
			// when the lease machinery cannot answer — a store error, a
			// malformed instance id — the broker falls through to exactly
			// today's behaviour and grants the presented name. The operator
			// sees the reason here rather than a fleet that cannot start.
			b.cfg.Logger.Warn("broker: lease unavailable, granting presented nid",
				"sid", sid, "nid", nid, "code", code, "err", lerr)
		}
		return false
	}
	// CONTESTED — reply with the verdict and TOUCH NOTHING ELSE.
	//
	// An empty AssignedNID means "contested, but no name could be issued"
	// (over-long basename, exhausted suffix space). That is still a refusal the
	// challenger must honour: we have PROVEN a different live process holds this
	// name, so falling through would overwrite its row, clear its proxy_ready
	// and close its running processes.
	payload, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Lease: lease})
	if msg.Reply != "" {
		b.respondBytes(msg, payload)
	}
	if lease.AssignedNID == "" {
		b.cfg.Logger.Warn("broker: register contested but no lease name could be issued; "+
			"the challenger keeps its own name and does NOT take over the incumbent's row",
			"sid", sid, "presented_nid", nid, "basename", lease.Basename,
			"reason", code, "err", lerr)
	} else {
		b.cfg.Logger.Info("broker: register contested, lease assigned",
			"sid", sid, "presented_nid", nid, "assigned_nid", lease.AssignedNID)
	}
	return true
}

// assignLeaseName issues the lowest free lease name for nid's basename, or the
// refusal shape when none can be issued.
func assignLeaseName(b *Broker, sid, basename string, now time.Time) (*proto.NodeLease, string, error) {
	// basename is the agent's CONFIGURED name, taken verbatim — never parsed out
	// of the presented one. See NodeRegisterReq.ConfiguredNID: a name shaped
	// `<base>-NN` is just as likely to be a real device as a lease, so deriving
	// the family from the string folds `gpu-02` into `gpu` and offers its clone
	// `gpu-03`, a name in a different credential family that gpu-02's binding
	// does not cover. (external review F2)
	maxInst := b.cfg.MaxInstancesPerBasename
	if maxInst <= 0 {
		maxInst = DefaultMaxInstancesPerBasename
	}
	// SKIP NAMES ALREADY OFFERED. A contested register writes NOTHING (that
	// ordering is what protects the incumbent's row), so the DB cannot know a
	// name was just handed out. Without this, N challengers arriving together —
	// a Deployment scaled to N — are every one of them offered `-02`, and each
	// loser discovers the collision only by rebuilding its whole session and
	// registering again: O(N²) connect+auth rounds with no backoff.
	offered := func(name string) bool {
		v, ok := b.leaseHolder.Load(leaseKey(sid, name))
		if !ok {
			return false
		}
		g, _ := v.(leaseGrant)
		return now.Sub(g.grantedAt) < leaseGrantWindow
	}
	assigned, err := node.LowestFreeSuffixExcept(b.read().SQL(), sid, basename, maxInst, offered)
	if err != nil {
		// WE HAVE ALREADY PROVEN A DIFFERENT LIVE PROCESS HOLDS THIS NAME. So
		// "degrade to today's behaviour" is NOT benign here: today's behaviour
		// is registerNode + reconcileOnRegister under the incumbent's name,
		// which zeroes its proxy_ready and closes every process row it is
		// running. Degrading is the safe answer when we do not KNOW there is an
		// incumbent; it is the destructive one when we do.
		//
		// So this returns a lease with an EMPTY AssignedNID: a refusal the agent
		// can act on (it keeps running under the name it presented and logs
		// loudly) that still short-circuits handleRegister before it touches
		// anything. Refusing is also why the basename is never truncated — two
		// distinct over-long basenames would collapse to one prefix and silently
		// merge two unrelated device families into one lease namespace.
		return &proto.NodeLease{Basename: basename}, leaseReasonUnavailable, err
	}
	// Record the OFFER so the next challenger in this burst is given a different
	// name. It is not a grant — the instance may never come back under it — but
	// within the window it is enough to stop N simultaneous arrivals colliding
	// on one suffix. The marker instance id is deliberately empty: nothing may
	// mistake an offer for a holder.
	b.leaseHolder.Store(leaseKey(sid, assigned), leaseGrant{grantedAt: now})
	return &proto.NodeLease{AssignedNID: assigned, Basename: basename}, "", nil
}

// probeNameInUse reports whether anyone is still subscribed under (sid, nid).
//
// ErrNoResponders means "no subscriber exists" — not "nobody replied" — so a
// dead predecessor is detected instantly and without a timeout. Anything else
// (a responder, or a timeout in a partition) is treated as in-use, which is the
// fail-safe direction: suffixing a live device is recoverable on its next
// restart, handing two live processes the same name is not.
func probeNameHeldByOther(b *Broker, sid, nid, selfInstanceID string) (held, known bool) {
	return probeNameHeldByOtherWithin(b, sid, nid, selfInstanceID, claimProbeBudget)
}

// probeNameHeldByOtherWithin is probeNameHeldByOther with an explicit budget, so
// the background path can afford a WAN-sized one while the direct path stays
// bounded for tests that call it straight.
// probeAnswer is what the wire actually told us, stated WITHOUT reference to
// who was asking.
//
// That distinction is the whole point of this type. "Is the name held by
// someone OTHER than me?" is a different question for every asker, so its
// answer cannot be cached under a key that only names (sid, nid) — replaying
// one instance's answer to another inverts it. The incumbent's own "not a
// stranger" would license a clone to take the bare name (two processes, one
// forwarded subject, every exec twice), and a clone's "held by a stranger"
// would rename the incumbent on its next reconnect and leave the row the
// operator addresses STALE.
//
// So the cache stores the OBSERVATION — did anyone answer, and who said they
// were — and each caller derives its own verdict from it.
// backgroundProbeBudgetOverride is a TEST SEAM and nothing else: when non-zero it replaces
// backgroundProbeBudget for every background probe launched after it is set. Production never
// touches it. It exists so the lease model's random walk can afford a SILENT subscriber (the one
// event that forces the background probe, the verdict cache and the silence rule — the three
// places review round 2's B5 and gotcha #72 live in): at the real 3s budget that event costs 3s a
// step and was excluded from the walk, which left those three paths reachable only by two
// hand-written scenarios. docs/reviews/test-system-overhaul-plan.md B3 named this seam as the
// fallback; internal review L2-F1 measured that without it the walk ran the background probe zero
// times in 480 steps.
//
// Atomic, not a plain var: a background-probe goroutine leaked by an earlier test can still be
// reading the budget when the next test's Cleanup writes it back, and the race detector would be
// right to say so.
var backgroundProbeBudgetOverride atomic.Int64

func effectiveBackgroundProbeBudget() time.Duration {
	if d := backgroundProbeBudgetOverride.Load(); d > 0 {
		return time.Duration(d)
	}
	return backgroundProbeBudget
}

type probeAnswer struct {
	answered   bool   // somebody replied inside the budget
	responder  string // the instance id they claimed; "" if they named none
	definitive bool   // the observation settles the question by itself
}

// heldByOther renders one asker's verdict from an objective observation.
func (a probeAnswer) heldByOther(selfInstanceID string) (held, known bool) {
	if !a.answered {
		// Nobody there at all is a definite "free"; anything else (a timeout, a
		// malformed body) is an honest UNKNOWN the caller weighs against the
		// heartbeat clock.
		return !a.definitive, a.definitive
	}
	return a.responder == "" || a.responder != selfInstanceID, a.responder != ""
}

func probeNameHeldByOtherWithin(b *Broker, sid, nid, selfInstanceID string, budget time.Duration) (held, known bool) {
	return probeObserve(b, sid, nid, budget).heldByOther(selfInstanceID)
}

// probeObserve asks the name who holds it and reports only what came back.
func probeObserve(b *Broker, sid, nid string, budget time.Duration) probeAnswer {
	nc := b.nc.Load()
	if nc == nil {
		return probeAnswer{}
	}
	msg, err := nc.Request(proto.SubjCmdForwarded(sid, nid, proto.ClaimProbeVerb), nil, budget)
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		// Nobody is subscribed under this name at all.
		return probeAnswer{definitive: true}
	case err != nil:
		// A timeout is NOT evidence either way, and saying "held" here would be
		// wrong for a real deployment: the budget is a constant, not a function
		// of the fleet's round-trip time, so a cross-continent agent that
		// answers correctly in 80ms would be renamed on every reconnect. Report
		// UNKNOWN and let the caller weigh it against the heartbeat.
		return probeAnswer{}
	}
	var resp proto.ClaimProbeResp
	if json.Unmarshal(msg.Data, &resp) != nil {
		return probeAnswer{}
	}
	// The decisive case, and the reason the probe carries an identity at all:
	// nats.go replays subscriptions BEFORE it invokes its reconnect handler, and
	// the agent re-registers from that handler on the same connection. So the
	// responder is very often the REGISTRANT ITSELF, and a pure interest check
	// would rename a device every time its network blipped.
	return probeAnswer{answered: true, responder: resp.InstanceID, definitive: true}
}

func (b *Broker) handleRegister(msg *nats.Msg) {
	sid, nid, ok := proto.ParseSidNidFromCtrl(msg.Subject)
	if !ok {
		b.replyErr(msg, "subject_malformed", "cannot parse sid/nid from "+msg.Subject)
		return
	}
	if b.isClusterFollower() {
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

	// origin: upgrade-safety N-1 window — EXACT equality, deliberately not an
	// [N-1, N] window (architecture §21.1 site #1): the epoch is frozen inside
	// a release window, and an N-1-epoch peer publishes on a subject tree this
	// broker does not subscribe. Bumping ProtoVersion? Read §21.4 first.
	if req.ProtoVersion != proto.ProtoVersion {
		b.replyErr(msg, "proto_mismatch",
			fmt.Sprintf("server proto=%d, client proto=%d", proto.ProtoVersion, req.ProtoVersion))
		return
	}

	// C.1 §6 — every session-scoped ingress must reject DELETING /
	// missing sessions before mutating any state. register sits on
	// `ctrl.s.<sid>.node.<nid>.register.req` so the gate applies here
	// just like it does for exec/run/ps/expose. Without this, a
	// tombstoned session could get a fresh nodes row, a forced ONLINE
	// transition, an `agent_registered` sys.event, and reconcile side
	// effects while H.3 cleanup is supposed to be the only writer.
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyErr(msg, "store_error", err.Error())
		return
	}
	if !active {
		b.replyErr(msg, "session_not_found_or_deleting",
			fmt.Sprintf("session %q is missing or being deleted", sid))
		return
	}

	// C1 §D-4: a lightweight periodic roster refresh wants ONLY a fresh signed roster, not a full
	// re-register. Short-circuit BEFORE registerNode / agent_registered / reconcileOnRegister — a
	// pure RODB read that signs the replicated roster — so a fleet's ≤3-min refresh ticker costs
	// ZERO raft writes / events / reconcile (D5 idle-zero-writes preserved). The session-active gate
	// above still applies; the stale event still fires (the agent reported its cached generation).
	// A FAREWELL comes in wearing RosterRefreshOnly (that is what makes it inert
	// on a pre-feature broker — see releaseLeaseName), so it must be recognised
	// BEFORE the refresh short-circuit or a current broker would answer it with
	// a roster and never release the name.
	//
	// Ordinary refreshes carry ReleasingName=false and are unaffected.
	if req.ReleasingName {
		if replyLeaseVerdict(b, msg, sid, nid, &req) {
			return
		}
	}

	if req.RosterRefreshOnly {
		resp := proto.NodeRegisterResp{OK: true}
		resp.Roster = b.rosterForRegister(req)
		b.maybeEmitRosterStale(sid, nid, req.RosterGen, resp.Roster)
		payload, _ := json.Marshal(resp)
		if msg.Reply != "" {
			b.respondBytes(msg, payload)
		}
		return
	}

	// CLONED-CREDENTIAL LEASE. Position is load-bearing in BOTH directions:
	//
	//   AFTER the RosterRefreshOnly short-circuit above — a periodic refresh
	//   carries no InstanceID and would otherwise be adjudicated as a legacy
	//   claim roughly 20 times an hour per agent, for nothing.
	//
	//   STRICTLY BEFORE registerNode below — in single mode (which is what the
	//   live fleet runs) registerNode IS the upsert, so a contested register
	//   adjudicated after it has already overwritten the incumbent's row. That
	//   ordering, and not any change to reconcile.go, is what stops a clone
	//   from zeroing the incumbent's proxy_ready and from closing every process
	//   row the incumbent is actually running.
	if replyLeaseVerdict(b, msg, sid, nid, &req) {
		return
	}

	in := node.RegisterInput{
		SID: sid, NID: nid,
		ProtoVersion:   req.ProtoVersion,
		ReleaseVersion: req.ReleaseVersion,
		OS:             req.OS,
		Arch:           req.Arch,
		BootID:         req.BootID,
		// #78: the agent.yaml proxy.participate opt-out FOLDS into the
		// existing capability column — deliberately NOT a new RegisterInput
		// field (that payload is raft-replicated; a new field would silently
		// drop in a mixed-version cluster). With proxy_capable=0 every
		// downstream gate — repairProxy, the reaper's allocate loop,
		// onlineProxyCapableNIDs, the /sub render — skips this node through
		// the code path old non-P13 agents already exercise.
		// Q1 (cloned-credential lease): a LEASED instance is ephemeral by
		// premise, so it must not hold a public egress port whose
		// disappearance breaks every subscriber. Folded into the SAME
		// expression #78 used for the opt-out, deliberately: proxy_capable is
		// the one column every downstream gate consults — onlineNIDs, the
		// reaper's allocate/rotate/teardown, the /sub render, proxy status —
		// so this inherits #78's already-shipped N-1 story with zero new
		// branches. It also makes unreachable the 1000-port band
		// multiplication and the per-(sid,nid) proxy_bind_stalled dedup key in
		// a table with no DELETE path.
		ProxyCapable: nodeHasProxyCap(req.Capabilities, req.ReleaseVersion) && !req.ProxyOptOut && !req.LeasedNID,
		// D6 §6.5 (external review F1): persist the agent's reported nats
		// server_name into nodes.nats_server so the home bridge can resolve it at
		// expose time. "" in production (single-node agent) → inert.
		NatsServer: req.ServerID,
	}
	// D9 §3 (audit #1): cluster mode routes the identity write through raft (Propose /
	// forward) + a local liveness write; single mode is the byte-identical direct mutator.
	// PreviousNID is a row-migration hint, not authority. Restrict it to the
	// credential family proved by the presented/configured-name relationship
	// before either the raft transaction or the live reconciler consumes it.
	req.PreviousNID = trustedPreviousNID(b, sid, nid, &req)
	if err := b.registerNode(in, req.PreviousNID, req.LocalProcesses); err != nil {
		if errors.Is(err, node.ErrSessionMissing) {
			b.replyErr(msg, "session_not_found",
				fmt.Sprintf("session %q does not exist; have an owner run `tether session create %s` first", sid, sid))
			return
		}
		if errors.Is(err, node.ErrSessionNotActive) {
			b.replyErr(msg, "session_not_found_or_deleting",
				fmt.Sprintf("session %q is missing or being deleted", sid))
			return
		}
		// audit M2 / write-forward F3: a leadership race (raft failover) is TRANSIENT — reply
		// the retriable code so the agent retries its register instead of treating it as a
		// permanent rejection and EXITING the process. Must precede the store_error fallback.
		if isLeaderUnavailable(err) {
			b.replyErr(msg, proto.CodeLeaderUnavailable,
				"no raft leader available to commit the registration right now; retrying")
			return
		}
		b.replyErr(msg, "store_error", err.Error())
		return
	}

	// #78 (review N1): update the "won't vs can't" visibility hint and free
	// the existing allocation ONLY after the identity write COMMITTED — a
	// register that failed (store_error / leader_unavailable) returned above,
	// so the hint never disagrees with the persisted proxy_capable and status
	// cannot render a self-contradictory row. In-memory hint only (empties on
	// a broker restart until re-register — documented [GAP]); enforcement is
	// nodes.proxy_capable. The single-mode free has no reaper to catch it
	// later; cluster rows are the reaper teardown pass's job (raft-routed).
	if req.ProxyOptOut {
		b.proxyOptedOut.Store(sid+"/"+nid, true)
		freeOptOutProxyRowSingle(b, sid, nid)
	} else {
		b.proxyOptedOut.Delete(sid + "/" + nid)
	}
	// A LEASED instance frees any __proxy__ row standing in its name for the
	// same reason an opted-out one does: single mode has no reaper pass, and
	// every allocate/repair gate now skips it, so the row and its public port
	// would otherwise outlive the ineligibility forever.
	//
	// Deliberately NOT folded into the branch above: the opt-out also drives the
	// "won't vs can't" operator hint, and a leased instance is a CAN'T (the
	// broker decided) rather than a WON'T (the operator configured). Rendering
	// it through the opt-out hint would tell an operator to go looking for an
	// agent.yaml key that does not exist inside a baked image.
	if req.LeasedNID {
		freeOptOutProxyRowSingle(b, sid, nid)
	}

	b.cfg.Logger.Info("broker: node registered",
		"sid", sid, "nid", nid, "release", req.ReleaseVersion)
	// upgrade-safety plan §4: an agent's first register after a `node upgrade`
	// carries the outcome — without this line a rollback is indistinguishable
	// from a plain reconnect on the broker side. Log-only by design: nodes rows
	// travel through the raft-frozen RegisterInput payload, where a new field
	// would silently drop in a mixed-version cluster (wire_freeze_test.go).
	// "agent-reported" wording is deliberate (internal review S23): an
	// in-flight register racing the agent's own rollback can carry a state
	// the marker no longer holds; a later register delivers the correction.
	if req.UpgradeState != "" {
		b.cfg.Logger.Info("broker: agent-reported upgrade outcome",
			"sid", sid, "nid", nid, "upgrade_state", req.UpgradeState,
			"detail", req.UpgradeDetail, "release", req.ReleaseVersion)
	}
	b.pubSysEvent("agent_registered", map[string]any{
		"sid": sid, "nid": nid, "release": req.ReleaseVersion,
		"os": req.OS, "arch": req.Arch,
	})

	// G.1 — converge SQLite to the agent's reality. Side effects
	// (proc.MarkExited, audit.proc / audit.port emissions) happen
	// inside reconcileOnRegister; the directive arrays are returned
	// to the agent so it can drop orphan processes / proxies.
	resp := proto.NodeRegisterResp{OK: true}
	resp.AcceptedProcesses,
		resp.ReconciledProcesses,
		resp.KeepPorts,
		resp.RevokePorts,
		resp.DropProcesses = b.reconcileOnRegister(sid, nid, req)

	// P13: if the session's proxy switch is ON, attach the authoritative
	// per-node proxy directive (join/reconnect path). Nil otherwise, so a
	// proxy-off reply stays byte-identical to pre-P13.
	resp.Proxy = b.proxyDirectiveForRegister(sid, nid, req)

	// D6 §7.4: attach per-expose home directives so a reconnecting agent rehomes
	// any expose whose home was re-pointed by the leader. Self-gating — nil in
	// production (selfID=="") so the reply stays byte-identical (the Proxy precedent).
	resp.Home = b.homeForRegister(sid, nid, req)

	// B7 DOC#3: attach the account-signed broker roster for agent discovery. Self-gating — nil in
	// single mode (selfID=="") so the reply stays byte-identical (the Home/Proxy precedent).
	resp.Roster = b.rosterForRegister(req)

	// C1 §6: flag a sustained-behind agent (leader-only here; nil roster ⇒ no-op, byte-equiv).
	b.maybeEmitRosterStale(sid, nid, req.RosterGen, resp.Roster)

	payload, _ := json.Marshal(resp)
	if msg.Reply != "" {
		b.respondBytes(msg, payload)
	}
}

func (b *Broker) handleHeartbeat(msg *nats.Msg) {
	sid, nid, ok := proto.ParseSidNidFromCtrl(msg.Subject)
	if !ok {
		return
	}
	// D9: last_heartbeat_at/status is a LIVENESS column (§3.5) — written to the local
	// liveness handle, NOT through raft (high-frequency, per-broker-local, rebuilt on
	// failover). In single mode livenessDB() == b.cfg.DB (byte-identical).
	if err := node.Heartbeat(b.livenessDB(), sid, nid, b.cfg.Now()); err != nil {
		// Heartbeat from an unregistered node — drop quietly. A real network
		// will see this on broker restart before re-register has happened.
		b.cfg.Logger.Debug("broker: heartbeat for unknown node",
			"sid", sid, "nid", nid, "err", err)
		return
	}
	// P13 (round-3 F3 / round-4 F2): drive proxy convergence off EVERY heartbeat,
	// comparing the FULL (generation, epoch) pair the agent reports — let
	// repairProxy decide from switch/readiness/generation/epoch state. Calling it
	// only when ProxyEpoch>0 would skip a transiently-failed agent (epoch 0), and
	// comparing epoch alone would miss a same-epoch generation mismatch.
	var hp proto.HeartbeatPayload
	if len(msg.Data) > 0 {
		_ = json.Unmarshal(msg.Data, &hp)
	}
	if b.clusterMode {
		// C5: cluster mode does not run the single-mode repairProxy (it would write port_allocations
		// via the RODB handle). Instead each broker writes its OWN liveness proxy_ready from the
		// BROADCAST heartbeat's ProxyBound — the cross-broker convergence signal that makes /sub correct
		// on any broker (no replicated proxy_ready). Gate ready on the switch being ON so a stale bound
		// can't mark a node serving past `proxy off`.
		ready := hp.ProxyBound
		if ready {
			if on, _ := session.GetProxyEnabled(b.cfg.DB, sid); !on {
				ready = false
			}
		}
		if err := node.SetProxyReady(b.livenessDB(), sid, nid, ready); err != nil {
			b.cfg.Logger.Debug("broker: cluster proxy_ready from heartbeat", "sid", sid, "nid", nid, "err", err)
		}
		return
	}
	b.repairProxy(sid, nid, hp.ProxyGeneration, hp.ProxyEpoch)
}

func (b *Broker) replyErr(msg *nats.Msg, code, message string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.NodeRegisterResp{
		OK: false, Code: code, Error: message,
	})
	b.respondBytes(msg, payload)
}

// ValidateConfig runs every pure config validator broker applies at startup.
// Returns the first violation, or nil. It is side-effect-FREE (g75-g78 external
// review F1): `serve --config-check` and a real `tether serve` must reach the
// SAME verdict on a config. Before this, config-check returned right after the
// strict YAML decode and so reported "config OK" for configs a real startup
// rejects (a bad webhook URL, a non-loopback /sub bind). broker.New calls this
// first, and cmd/tether's config-check calls it plus the two validators that
// live outside broker (log level via newLoggerTo, auth seeds via
// loadAuthCalloutSeeds), so the two paths validate the same surface.
//
// It opens NO DB, binds NO listener, reads NO file: pure format/range checks on
// already-resolved values. Runtime availability (a port actually being free) is
// deliberately NOT its job — "config OK" is about validity, not whether the
// host is ready to bind.
func ValidateConfig(cfg Config) error {
	if cfg.NATSURL == "" {
		return fmt.Errorf("broker: NATSURL required")
	}
	// Pure syntax of the NATS URL — a scheme-less garbage string like
	// "://not-a-url" is rejected here so config-check agrees with a real
	// nats.Connect, which cannot dial it (F1-A). "existing but momentarily
	// unreachable" stays a runtime availability concern, NOT a syntax miss.
	if err := validateNATSURL(cfg.NATSURL); err != nil {
		return err
	}
	if cfg.AlertWebhookURL != "" {
		if _, err := parseWebhookURL(cfg.AlertWebhookURL); err != nil {
			return err
		}
	}
	// Listen addresses that a bad host:port would only surface at bind time —
	// caught here so config-check and startup agree. Empty = disabled/default,
	// not an error. The /sub and cluster-manifest surfaces are unauthenticated
	// + Caddy-fronted, so their real Bind REQUIRES loopback (httplisten,
	// requireLoopback=true) — config-check must apply the SAME gate (F1
	// parity), via the shared httplisten.CheckLoopback so the two can't drift.
	// /metrics and the tunnel control addr are format-only (a private-interface
	// bind is legitimate there).
	for _, a := range []struct{ name, addr string }{
		{"broker.observability.metrics_listen", cfg.MetricsAddr},
		{"broker.frp.control_listen", cfg.TunnelControlAddr},
	} {
		if err := validateListenAddr(a.name, a.addr); err != nil {
			return err
		}
	}
	for _, a := range []struct{ name, addr string }{
		{"broker.sub.listen", cfg.SubHTTPAddr},
		{"broker.cluster.manifest_listen", cfg.ManifestAddr},
	} {
		if a.addr == "" {
			continue // disabled
		}
		if err := validateListenAddr(a.name, a.addr); err != nil {
			return err
		}
		if err := httplisten.CheckLoopback(a.name, a.addr); err != nil {
			return err
		}
	}
	return nil
}

// validateListenAddr checks a host:port listen address FORMAT (empty ⇒ ok,
// meaning disabled or default). A ":8090" (host-less) form is valid — net.Listen
// accepts it — so only the port must parse and be in range. Port 0 is VALID:
// net.Listen treats it as "assign an ephemeral port", so config-check must
// accept it too (F1 parity — a preflight must not reject what a real start
// accepts).
func validateListenAddr(name, addr string) error {
	if addr == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q: %w", name, addr, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("%s %q: port must be 0-65535", name, addr)
	}
	// A host, when present, must be a bare IP or a syntactically valid DNS
	// hostname — reject what net.Listen would reject at bind (F1-B). A bare
	// ":9090" (host-less wildcard) is fine.
	if !validListenHost(host) {
		return fmt.Errorf("%s %q: malformed host", name, addr)
	}
	return nil
}

// validateNATSURL checks the NATS server URL(s) for the same pure-syntax
// validity nats.go requires, WITHOUT dialing (F1-A). The client accepts a
// comma-separated list and gives a scheme-less "host:port" entry an implicit
// "nats://"; this mirrors that so config-check and Connect reach one verdict.
// Reachability (host resolvable, port open) is deliberately NOT checked — that
// is a runtime availability concern, not a config error.
func validateNATSURL(raw string) error {
	// nats.go rejects a pool that mixes ws/wss with nats/tls before it dials
	// anything (ErrMixingWebsocketSchemes). Track the same pool-level class;
	// validating each URL independently is insufficient.
	poolClass := -1 // 0 = nats/tls, 1 = ws/wss
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			return fmt.Errorf("broker.nats.url %q: empty server entry", raw)
		}
		toParse := s
		if !strings.Contains(toParse, "://") {
			toParse = "nats://" + toParse // nats.go's implicit scheme
		}
		u, err := url.Parse(toParse)
		if err != nil {
			return fmt.Errorf("broker.nats.url %q: %w", s, err)
		}
		var class int
		switch u.Scheme {
		case "nats", "tls":
			class = 0
		case "ws", "wss":
			class = 1
		default:
			return fmt.Errorf("broker.nats.url %q: scheme must be nats/tls/ws/wss", s)
		}
		if u.Host == "" {
			return fmt.Errorf("broker.nats.url %q: missing host", s)
		}
		if poolClass >= 0 && class != poolClass {
			return fmt.Errorf("broker.nats.url %q: %w", raw, nats.ErrMixingWebsocketSchemes)
		}
		poolClass = class
	}
	return nil
}

// validListenHost reports whether host is a bind-legal listen host: empty
// (wildcard), a bare IP (v4/v6, optional IPv6 zone), or a syntactically valid
// DNS hostname. It rejects shell/URL metacharacters ("bad$host") that
// net.Listen would fail on, so config-check and a real Bind agree (F1-B).
func validListenHost(host string) bool {
	if host == "" {
		return true // wildcard bind
	}
	// A zone is legal only on an IPv6 literal (fe80::1%eth0). Do not strip an
	// arbitrary suffix before DNS validation: example.com%eth0 is not an IPv6
	// address and a real bind cannot interpret it as the hostname example.com.
	if i := strings.IndexByte(host, '%'); i >= 0 {
		ip, zone := host[:i], host[i+1:]
		return zone != "" && strings.Contains(ip, ":") && net.ParseIP(ip) != nil
	}
	if net.ParseIP(host) != nil {
		return true
	}
	// A single trailing dot is the standard absolute-FQDN spelling and is
	// accepted by resolvers. Remove it for label validation, but reject ".".
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	if len(host) > 253 {
		return false
	}
	// DNS hostname: dot-separated labels of [A-Za-z0-9_-], each 1..63 chars,
	// not starting/ending with '-'. Underscore is tolerated (real deployments
	// use it in service names); '$', space, '/' and the like are not.
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			default:
				return false
			}
		}
	}
	return true
}

// leaseAssignedName renders a verdict for the adjudication log: the name that
// was issued, or "" when the register was left to keep the name it asked for.
func leaseAssignedName(l *proto.NodeLease) string {
	if l == nil {
		return ""
	}
	return l.AssignedNID
}

// configuredBasename returns the root of this agent's credential family.
//
// It is what the agent SAYS it was configured with, falling back to the
// presented name when it says nothing — which is both the pre-feature case and
// the ordinary unleased case, and in each the presented name IS the root.
//
// Deliberately never SplitLeaseName: that would fold a real device named
// `gpu-02` into the `gpu` family (external review F2).
func configuredBasename(b *Broker, sid, nid string, req *proto.NodeRegisterReq) string {
	if req == nil || req.ConfiguredNID == "" || req.ConfiguredNID == nid {
		return nid
	}
	// IT IS CLIENT-SUPPLIED, SO IT IS CHECKED AGAINST THE NAME THIS REGISTER
	// ARRIVED UNDER.
	//
	// The only legitimate case where the two differ is an agent that already
	// holds a lease: it registers as `<base>-NN` while its configured root is
	// `<base>`. Anything else is an agent claiming membership of a credential
	// family it did not authenticate into — and the external re-review showed
	// what that buys: an agent holding gpu1's credential could declare
	// ConfiguredNID "victim" and have the broker allocate victim-02,
	// victim-03 … reserving another family's suffix space in leaseHolder,
	// without ever being able to authenticate as one of those names.
	//
	// auth_callout already binds this connection to `nid`; this makes the
	// register body agree with it instead of overriding it. On a mismatch the
	// presented name is used, which is the pre-feature reading and cannot
	// reach outside the family the agent actually authenticated as.
	if base, _, leased := proto.SplitLeaseName(nid); leased && base == req.ConfiguredNID {
		// Name shape alone is not evidence that this is a lease. A real device
		// may be provisioned as `gpu-02`; letting that device claim configured
		// root `gpu` crosses into a different credential family. When bindings
		// are available, accept the relationship only when the presented name
		// has no binding of its own and the claimed root does.
		provisioned, known := node.ProvisionedNIDs(b.read().SQL(), sid)
		if known && (provisioned[nid] || !provisioned[req.ConfiguredNID]) {
			return nid
		}
		return req.ConfiguredNID
	}
	return nid
}

func trustedPreviousNID(b *Broker, sid, nid string, req *proto.NodeRegisterReq) string {
	if req == nil || req.PreviousNID == "" {
		return ""
	}
	base := configuredBasename(b, sid, nid, req)
	// A carry is meaningful only when the current presented name is an
	// assigned member of the proved family. In particular, an independently
	// provisioned `gpu-02` resolves its base to itself and cannot name `gpu` as
	// a process-row source.
	presentedBase, _, presentedLeased := proto.SplitLeaseName(nid)
	if !presentedLeased || presentedBase != base {
		return ""
	}
	if req.PreviousNID == base {
		return req.PreviousNID
	}
	previousBase, _, previousLeased := proto.SplitLeaseName(req.PreviousNID)
	if previousLeased && previousBase == base {
		return req.PreviousNID
	}
	return ""
}
