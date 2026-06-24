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
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
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

	// PortBandLow / PortBandHigh override the public port range
	// (architecture A.3 / F.3). Defaults 14000-14999.
	PortBandLow  int
	PortBandHigh int

	// SubHTTPAddr is the loopback listen address for the P13 read-only
	// subscription HTTP endpoint (e.g. "127.0.0.1:8090"). Empty disables
	// the HTTP listener entirely — every pre-P13 deployment is unchanged.
	SubHTTPAddr string

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

	// tunnelSrv is the reverse-TCP tunnel server. Non-nil only when
	// Config.TunnelControlAddr is set. Authorizes agent REGISTERs by
	// looking up port_allocations.token_hash.
	tunnelSrv *tunnel.Server

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

	// transfers tracks in-flight file transfers (push/pull) so the
	// broker can write audit + reap OBJ_xfer-* buckets when the
	// receiver-finalization signal arrives or the watchdog fires.
	// file-transfer-plan §Object bucket lifecycle.
	transfers *transferTracker

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

	// selfID + tunnelCert are the D6 BUILD-AND-PROVE seam (cutover=D9). Both are
	// the zero value in production: serve.go never attaches a cluster identity nor
	// loads a stable cert, so every D6 home-assignment path (homeForRegister /
	// homeForExpose / selfNodeID / the tunnelTokenLookup home ladder / the stable
	// cert) is inert and the responses stay byte-identical. The test harness sets
	// them via AttachClusterSeam (internal/broker/home.go). The guard test
	// TestD6ProductionWiresNoClusterNode bans AttachClusterSeam + the stable-cert
	// constructors from production files so these can never become non-zero there.
	//
	// selfID is this broker's cluster_nodes.node_id (== cluster.Node.SelfID() at
	// D9 cutover). It is the "self" the tunnelTokenLookup home_broker==self filter
	// compares against; "" ⇒ the whole home ladder is inert (single-node). The D6
	// CORE (home assignment from the replicated cluster_nodes/port_allocations view
	// + the ladder + agent rehome + cert pinning) needs only this string + the
	// local DB; the FSM PRODUCER op OpPortReassignHome is proven by its own unit
	// test (build-and-prove, like every D2 op), not wired into the live DB here.
	selfID     string
	tunnelCert *tls.Certificate

	// transferAuditSink + transferAuditWG are the D8a (§9) BUILD-AND-PROVE seam
	// (cutover=D9). transferAuditSink is nil in production: serve.go never attaches it,
	// so emitTransferAudit falls through to the byte-identical best-effort
	// pubAuditTransfer (the read of the nil seam is the only production-visible change).
	// The harness wires it via AttachTransferAuditSink (transfer_audit_forward.go, a
	// guard-excluded mechanism file) to route start/complete/failed through leader Apply
	// (OpTransferAudit, re-derivable). transferAuditWG tracks the async forward goroutines
	// so the leak gate can drain them (WaitTransferAudit). The guard bans the write tokens
	// (transferAuditSink: / b.transferAuditSink =) from scanned production files.
	transferAuditSink func(schema.AuditTransfer)
	transferAuditWG   sync.WaitGroup
	// transferAuditDraining (D9 round-1 MAJOR): once the ordered shutdown sets it, new audit
	// emits forward SYNCHRONOUSLY in the NATS handler instead of spawning a tracked goroutine
	// — so a transfer event arriving in the shutdown window is drained within nc.Drain's
	// callback wait, not lost by a goroutine spawned AFTER WaitTransferAudit already returned.
	transferAuditDraining atomic.Bool

	// xferReplicasFn is the D8a (§9) tier-B replica seam (cutover=D9). nil in production:
	// xferTargetReplicas() returns jsstream.ReplicasSingle so a freshly-created OBJ_xfer
	// bucket is byte-identically R=1. The harness sets it (AttachXferReplicas) to
	// ReplicasFor(NumVoters) so a completed tier-B object survives a home-broker kill at
	// N>=3. The guard bans the write token (b.xferReplicasFn = / xferReplicasFn:) from
	// scanned production files.
	xferReplicasFn func() int

	// alertSink is the D8b (§10.2) disk-pressure forward seam (cutover=D9). nil in
	// production: the disk monitor surfaces pressure only via the existing pubSysEvent
	// (byte-identical). The harness sets it (AttachAlertSink) to forward the local disk
	// state to the leader's replicated alert store (VerbAlertSignal). The guard bans the
	// write token (b.alertSink = / alertSink:) from scanned production files; the disk
	// monitor's `if b.alertSink != nil` READ is allowed.
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
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("broker: NATSURL required")
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
	b := &Broker{
		cfg:         cfg,
		transfers:   newTransferTracker(),
		clusterMode: clusterMode,
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

// Run connects to NATS, installs subscriptions, runs the reconcile ticker,
// and blocks until ctx is canceled. The NATS connection is drained on exit.
//
// If cfg.AuthCallout is non-nil, the broker presents its pre-configured
// nkey credentials on CONNECT (so it bypasses auth_callout itself) AND
// subscribes to $SYS.REQ.USER.AUTH to issue per-connection user JWTs.
func (b *Broker) Run(ctx context.Context) error {
	b.runCtx = ctx

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
		b.cl.forwarder = NewForwarder(nc, b.cfg.ExposeForwardTimeout())
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
		b.tunnelSrv = b.newTunnelServer(b.cfg.TunnelControlAddr, host, b.tunnelTokenLookup, b.cfg.Logger)
		if err := b.tunnelSrv.Start(ctx); err != nil {
			return err
		}
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
				DB:         b.cfg.DB,
				PublicHost: b.publicHostFor(),
				Logger:     b.cfg.Logger,
			}); err != nil {
				b.cfg.Logger.Error("broker: subscription http server", "err", err)
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
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.*.req",
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
			if n, err := b.reconcileXferObjectsOnBoot(ctx); err != nil {
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
			return fmt.Errorf("broker: cluster mode requires JetStream; enable JetStream before starting HA broker")
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

	// P9 / I.2b — local admin socket. No-op when path empty (the
	// in-process tests that don't exercise admin leave it unset).
	if b.cfg.AdminSocketPath != "" {
		backend := adminsock.Backend{
			DB:              b.cfg.DB,
			Now:             b.cfg.Now,
			Logger:          b.cfg.Logger,
			AuditTail:       b.adminAuditTail,
			PubAgentEvicted: b.pubAgentEvicted,
		}
		// D9: in cluster mode the `tether cluster *` admin verbs are served by the
		// ClusterAdmin orchestrator (D7); nil in single mode ⇒ "cluster mode not enabled".
		if b.clusterMode {
			// caughtUp/streamsReady nil for now: status/drain/remove/transfer work and
			// add's catch-up gate uses the leader-applied proxy. The real per-follower
			// cursor + stream-ready probes are wired by Step 10b (§17 observability).
			// b.cl.admin was constructed in wireClusterLate (independent of the socket).
			// External-review F1: wire the REAL catch-up + stream-readiness transports so
			// the operator's `tether cluster add` (catch-up gate) + `cluster drain --retire`
			// (AllAtTarget gate) work through the production adminsock — not just the harness.
			backend.Cluster = NewClusterAdminBackend(b.cl.admin, b.clusterCaughtUp, b.clusterStreamsReady)
			// D9 round-2 BLOCKER: route `admin evict` through raft (else the direct tx hits
			// the RODB handle and fails). Single mode leaves EvictWrite nil (direct tx).
			backend.EvictWrite = b.evictNode
		}
		b.admin = adminsock.New(b.cfg.AdminSocketPath, backend)
		if err := b.admin.Start(ctx); err != nil {
			return fmt.Errorf("broker: admin socket start: %w", err)
		}
		defer func() { _ = b.admin.Close() }()
		b.cfg.Logger.Info("broker: admin socket ready", "path", b.cfg.AdminSocketPath)
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

	ticker := time.NewTicker(b.cfg.ReconcileInterval)
	defer ticker.Stop()

	gcTicker := time.NewTicker(b.cfg.ProcGCInterval)
	defer gcTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.cfg.Logger.Info("broker: shutting down")
			return ctx.Err()
		case <-ticker.C:
			now := b.cfg.Now()
			n, err := node.ReconcileStates(b.livenessDB(), now,
				b.cfg.StaleAfter, b.cfg.OfflineAfter)
			if err != nil {
				b.cfg.Logger.Warn("broker: reconcile failed", "err", err)
			} else if n > 0 {
				b.cfg.Logger.Info("broker: state transitions", "count", n)
			}
			// D9 round-1 MAJOR: the OFFLINE-node port-revocation scan is a leader-local
			// DECISION (like proc GC) — in cluster mode run it only on the leader, so N
			// followers don't each re-scan + forward the same (idempotent but wasteful)
			// PlanRevoke every tick. ReconcileStates above is per-broker-local liveness
			// (livenessDB), so it stays on every broker. Single mode unchanged.
			if !b.clusterMode || b.cl.node.IsLeader() {
				if revoked := b.reconcilePorts(now); revoked > 0 {
					b.cfg.Logger.Info("broker: port revocations", "count", revoked)
				}
			}
			if closed := b.reconcileTunnelSessions(); closed > 0 {
				b.cfg.Logger.Info("broker: stale tunnel proxies closed", "count", closed)
			}
		case <-gcTicker.C:
			// In cluster mode processes is replicated state, so deleting rows outside raft
			// can fork leader/follower SQLite contents. Keep retention GC single-node only
			// until it has a replicated command.
			if b.clusterMode {
				continue
			}
			cutoff := b.cfg.Now().Add(-b.cfg.ProcRetention)
			n, err := proc.GCExited(b.livenessDB(), cutoff)
			if err != nil {
				b.cfg.Logger.Warn("broker: proc gc", "err", err)
			} else if n > 0 {
				b.cfg.Logger.Info("broker: proc gc",
					"deleted", n, "cutoff", cutoff)
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

	in := node.RegisterInput{
		SID: sid, NID: nid,
		ProtoVersion:   req.ProtoVersion,
		ReleaseVersion: req.ReleaseVersion,
		OS:             req.OS,
		Arch:           req.Arch,
		BootID:         req.BootID,
		ProxyCapable:   nodeHasProxyCap(req.Capabilities, req.ReleaseVersion),
		// D6 §6.5 (external review F1): persist the agent's reported nats
		// server_name into nodes.nats_server so the home bridge can resolve it at
		// expose time. "" in production (single-node agent) → inert.
		NatsServer: req.ServerID,
	}
	// D9 §3 (audit #1): cluster mode routes the identity write through raft (Propose /
	// forward) + a local liveness write; single mode is the byte-identical direct mutator.
	if err := b.registerNode(in); err != nil {
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
		b.replyErr(msg, "store_error", err.Error())
		return
	}

	b.cfg.Logger.Info("broker: node registered",
		"sid", sid, "nid", nid, "release", req.ReleaseVersion)
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

	payload, _ := json.Marshal(resp)
	if msg.Reply != "" {
		_ = msg.Respond(payload)
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
	b.repairProxy(sid, nid, hp.ProxyGeneration, hp.ProxyEpoch)
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
