package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/natscluster"
	"github.com/LinZiyang666/tether/internal/natsreconcile"
	"github.com/nats-io/nkeys"
)

// topology_reconcile.go (C3) — the per-broker NATS topology reconcile loop: a 5th cluster loop, NOT
// leader-gated (every broker converges its OWN nats.conf). It watches the replicated topology
// generation, renders+validates+swaps+reloads this host's conf, and probes the live server for a REAL
// observed generation. It NEVER ssh/root-orchestrates: it signals only its same-uid co-located NATS
// process. A non-reloadable route/auth delta gets a deterministic, roster-staggered hard restart via
// the existing nats-server signal resolver + systemd Restart=always lifecycle.

const (
	topoReconcileInterval = 5 * time.Second
	topoMonitorListen     = "127.0.0.1:8223" // loopback monitor the reconciler renders + probes
	topoReloadTimeout     = 10 * time.Second
	topoProbeTimeout      = 3 * time.Second
	// Route/auth topology deltas are not reloadable by every supported nats-server release.  A
	// same-uid hard restart is therefore the bounded fallback, but it must be staggered so all NATS
	// voters never bounce together.  The rank is stable over the same replicated peer set.
	topoRestartBaseDelay = 12 * time.Second
	topoRestartSpacing   = 12 * time.Second
)

// topoProbeClient (C3-M4) is built ONCE and reused across probes: a fresh http.Client+Transport per
// 5s tick would strand a persistConn goroutine+fd each time (tripping the repo NumGoroutine/fd leak
// gate). Proxy:nil bypasses a WSL-style HTTP_PROXY on the loopback monitor; DisableKeepAlives closes
// the conn after each probe so no idle pool accrues.
var topoProbeClient = &http.Client{
	Timeout:   topoProbeTimeout,
	Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
}

// topoSelfReport returns this broker's latest topology reconcile self-report for the health responder
// (nil if no cluster runtime or no pass yet — the responder still sets TopoReported=true).
func (b *Broker) topoSelfReport() *topoSelfReport {
	if b.cl == nil {
		return nil
	}
	return b.cl.topoSelf.Load()
}

// runTopologyReconcileLoop converges this broker's nats.conf. Started in wireClusterLate (ctx-bounded,
// joined by clusterShutdownOrdered, leak-gated). Inert when no live conf path is configured. The loop
// is single-goroutine so reloadNatsServer calls are serial (no single-flight mutex needed).
func (b *Broker) runTopologyReconcileLoop(ctx context.Context) {
	if b.cfg.NatsConfPath == "" {
		return // no live conf to manage → reconciler inert (status reports nothing)
	}
	ticker := time.NewTicker(topoReconcileInterval)
	defer ticker.Stop()
	var lastApplied, lastObserved uint64
	var lastReloadMtime int64  // mega-audit MAJ-6: the conf mtime we last SIGHUP-reloaded (storm guard)
	var lastRestartMtime int64 // one hard-restart request per exact conf within this broker lifetime
	var lastEventKey string    // C3-m7: publish a sys.event only when the (action,reason) actually changes
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		lastApplied, lastObserved, lastReloadMtime, lastRestartMtime, lastEventKey = b.reconcileTopologyOnce(ctx, lastApplied, lastObserved, lastReloadMtime, lastRestartMtime, lastEventKey)
	}
}

func (b *Broker) reconcileTopologyOnce(ctx context.Context, lastApplied, lastObserved uint64, lastReloadMtime, lastRestartMtime int64, lastEventKey string) (uint64, uint64, int64, int64, string) {
	desired, err := cluster.TopologyGeneration(b.cfg.DB)
	if err != nil {
		return lastApplied, lastObserved, lastReloadMtime, lastRestartMtime, lastEventKey
	}
	// Self-backfill: write our own bus nkey pub when the replicated row is empty OR wrong (C3-m3
	// heals a mismatch, not only an empty value). Bounded — PlanClusterBusNkeySet's `WHERE
	// bus_nkey_pub != <lit>` makes a re-propose a no-op once correct, so no per-tick raft write.
	if b.selfBusNkeyNeedsBackfill() {
		b.backfillSelfBusNkey()
	}
	in, ok := b.buildTopologyInputs(desired)
	if !ok {
		return lastApplied, lastObserved, lastReloadMtime, lastRestartMtime, lastEventKey
	}
	// Mega-audit MAJ-6: gate the SIGHUP-reload on the conf mtime advancing since our last reload. The
	// reconciler's fast-path re-issues reload() every tick whenever the probe can't CONFIRM the load
	// (e.g. a conf with no http monitor — the probe is connection-refused forever), which without this
	// guard is ~17k SIGHUP/day/broker. A real swap bumps the conf mtime → reload fires exactly once.
	reloadedMtime := lastReloadMtime
	reload := func() error {
		mt := topoConfMtime(in.ConfPath)
		if mt != 0 && mt == reloadedMtime {
			return nil // already reloaded this exact conf; re-signaling won't change an unconfirmable probe
		}
		if rerr := b.reloadNatsServer(ctx); rerr != nil {
			return rerr
		}
		reloadedMtime = mt
		return nil
	}
	out := natsreconcile.ReconcileOnce(in, lastApplied, lastObserved, reload, b.probeNatsConfigLoadTime)
	eventKey := fmt.Sprintf("%d|%s|%s", desired, out.Action, out.Reason)
	// NATS accepts the SIGHUP command even when a route/auth delta cannot be applied live.  In that
	// case /varz remains reachable but config_load_time stays older than the swapped file.  Waiting
	// forever leaves join/retire operations stuck at NATS_ROLLED_OUT.  Once the node's deterministic
	// roster-rank delay has elapsed, request the same-uid SIGKILL lifecycle already used by the first-
	// grow cutover; systemd Restart=always revives NATS on the new conf.  We deliberately require a
	// successful stale /varz sample: monitor loss is not evidence that a restart is required.
	if out.Action == natsreconcile.ActionSwappedReloadPending && out.AppliedGen == desired {
		mt := topoConfMtime(in.ConfPath)
		// Rank by NATS server_name, not raft node_id.  They are deliberately distinct fields in the
		// roster and only happen to be equal in the sim fixture; comparing b.selfID against ServerName
		// makes the restart permanently ineligible in a valid production deployment.
		confTime := time.Unix(0, mt)
		due := mt != 0 && topologyRestartDue(in.SelfServerName, in.Peers, confTime, b.cfg.Now())
		loadTime, probeErr := b.probeNatsConfigLoadTime()
		staleLoad := probeErr == nil && !loadTime.IsZero() && loadTime.Before(confTime)
		if lastEventKey != eventKey {
			b.cfg.Logger.Warn("topology reconcile: live config remains stale after reload",
				"server_name", in.SelfServerName, "conf_time", confTime, "load_time", loadTime,
				"restart_due", due, "probe_err", probeErr, "desired_gen", desired)
		}
		if mt != 0 && mt != lastRestartMtime && due && staleLoad {
			if rerr := b.hardRestartNatsServer(); rerr != nil {
				b.cfg.Logger.Warn("topology reconcile: staggered nats-server restart failed", "err", rerr, "desired_gen", desired)
			} else if rerr := b.waitNatsLoaded(ctx, confTime); rerr != nil {
				// A successful signal-resolver exit proves only that a signal request was accepted, not
				// that systemd revived NATS on this conf. Do not burn the per-mtime guard until the live
				// monitor proves the restart; a later tick may retry a lost/no-op signal.
				b.cfg.Logger.Warn("topology reconcile: nats-server restart was not observed; will retry",
					"err", rerr, "desired_gen", desired, "conf_time", confTime)
			} else {
				lastRestartMtime = mt
				b.cfg.Logger.Warn("topology reconcile: confirmed staggered nats-server restart for a non-reloadable topology delta", "desired_gen", desired)
			}
		}
	}
	b.cl.topoSelf.Store(&topoSelfReport{Applied: out.AppliedGen, Observed: out.ObservedGen, Reason: out.Reason})
	// C3-m7: change-gate the sys.event so a stuck broker does not spam JS every 5s (a publish per tick
	// would also stall if JS is the unhealthy thing). Skip the steady-state noop/unresolvable.
	// Include the desired generation: the same action/reason on a NEW topology delta is a distinct
	// operator event and must not be suppressed by the previous generation's change gate.
	if out.Action != natsreconcile.ActionNoop && out.Action != natsreconcile.ActionUnresolvable && eventKey != lastEventKey {
		b.pubSysEvent("nats_topology_"+out.Action, map[string]any{
			"applied": out.AppliedGen, "observed": out.ObservedGen, "reason": out.Reason,
		})
	}
	return out.AppliedGen, out.ObservedGen, reloadedMtime, lastRestartMtime, eventKey
}

// waitNatsLoaded bounds the deliberate hard-restart lifecycle and accepts it only when the new
// nats-server reports a config load at/after the swapped file. A command exit alone is not proof.
func (b *Broker) waitNatsLoaded(ctx context.Context, confTime time.Time) error {
	deadline := time.NewTimer(30 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer deadline.Stop()
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("config_load_time did not reach %s within 30s", confTime.UTC().Format(time.RFC3339Nano))
		case <-tick.C:
			loadTime, err := b.probeNatsConfigLoadTime()
			if err == nil && !loadTime.Before(confTime) {
				return nil
			}
		}
	}
}

// topologyRestartDue assigns every peer a unique, deterministic restart slot from the replicated
// server-name set.  It is pure so the no-simultaneous-bounce safety property is unit-testable.
func topologyRestartDue(self string, peers []natscluster.Broker, confTime, now time.Time) bool {
	names := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.ServerName != "" {
			names = append(names, p.ServerName)
		}
	}
	sort.Strings(names)
	for rank, name := range names {
		if name == self {
			return !confTime.IsZero() && !now.Before(confTime.Add(topoRestartBaseDelay+time.Duration(rank)*topoRestartSpacing))
		}
	}
	return false
}

// topoConfMtime returns the conf file's mtime in unix-nanos (0 if absent) — the MAJ-6 reload-storm guard
// key. A swap rewrites the file (mtime advances) so a real change still reloads exactly once.
func topoConfMtime(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.ModTime().UnixNano()
}

func (b *Broker) buildTopologyInputs(desired uint64) (natsreconcile.Inputs, bool) {
	peers, err := clusternodes.ListPeersForTopology(b.cfg.DB)
	if err != nil {
		return natsreconcile.Inputs{}, false
	}
	peers = b.filterGhostPeers(peers)
	brokers := make([]natscluster.Broker, 0, len(peers))
	selfServer := ""
	for _, p := range peers {
		brokers = append(brokers, natscluster.Broker{ServerName: p.NatsServer, NkeyPub: p.BusNkeyPub, RouteURL: p.NatsRoute})
		if p.NodeID == b.selfID {
			selfServer = p.NatsServer
		}
	}
	bin := b.cfg.NatsServerBin
	if bin == "" {
		bin = "nats-server"
	}
	return natsreconcile.Inputs{
		SelfServerName: selfServer,
		Peers:          brokers,
		AccountIssuer:  b.accountPubOrEmpty(),
		ConfPath:       b.cfg.NatsConfPath,
		NatsServerBin:  bin,
		DesiredGen:     desired,
		// G4 #3: let the FIRST standalone→clustered grow render routes mTLS from the secrets dir when the
		// live standalone conf has no cluster{} block to harvest. The reconciler still WITHHOLDS that swap
		// (ActionAwaitingClusteredCutover) so only `cluster add` performs the coordinated restart.
		SecretsDir: b.cfg.ClusterSecretsDir,
	}, true
}

// filterGhostPeers drops mesh-phase roster rows that are ABSENT from the committed raft config — a
// force-single ghost (e.g. the live racknerd pc732: phase=VOTER yet moved out of raft by a prior
// force-single). Without this the ghost keeps len(Peers)>1, so the reconciler renders a CLUSTERED conf
// and clobbers a hand-de-clustered survivor conf on upgrade (the G2 #12 double-break). The route mesh
// must mirror raft membership; a not-in-config peer never belongs in it. TWO DISTINCT fallbacks: (1)
// cluster NOT WIRED (nil cl/node) returns peers UNCHANGED — there is no raft, hence no ghost risk, and the
// guard is a no-op (defense-in-depth: force-single prune already removes fresh abandoned rows at the
// source); (2) a raft-config READ ERROR on a wired node returns SELF-ONLY (fail-SAFE, NOT fail-open) — a
// fail-open pass would re-widen len(Peers)>1 and let the reconciler re-cluster a hand-de-clustered survivor
// (the exact G2 #12 double-break). Self is always kept.
func (b *Broker) filterGhostPeers(peers []clusternodes.TopoPeer) []clusternodes.TopoPeer {
	if b.cl == nil || b.cl.node == nil {
		return peers
	}
	servers, err := b.cl.node.RaftConfiguration()
	if err != nil {
		// Internal review: on a config-read error, keep ONLY self rather than fail-open to ALL peers — a
		// fail-open pass would re-widen len(Peers)>1 and let the reconciler render a CLUSTERED conf over a
		// hand-de-clustered survivor (the exact double-break this guard prevents). With peers=[self] the
		// reconciler renders standalone (live conf standalone) or fail-closes (clustered-zero-routes), never
		// a wrong clustered mesh; a genuinely multi-node cluster just skips one tick until the read recovers.
		b.cfg.Logger.Warn("topology reconcile: cannot read raft config for the G2 #12 ghost filter; keeping only self to avoid re-clustering a de-clustered survivor", "err", err)
		return filterSelfOnly(peers, b.selfID)
	}
	inCfg := make(map[string]bool, len(servers))
	for _, s := range servers {
		inCfg[s.NodeID] = true
	}
	out := make([]clusternodes.TopoPeer, 0, len(peers))
	for _, p := range peers {
		if p.NodeID == b.selfID || inCfg[p.NodeID] {
			out = append(out, p)
			continue
		}
		b.cfg.Logger.Warn("topology reconcile: excluding force-single ghost from the route mesh (roster row present but absent from committed raft config)",
			"node_id", p.NodeID, "phase", p.Phase)
	}
	return out
}

// filterSelfOnly returns just the self peer (or nil) — the fail-safe fallback when the raft config is
// unreadable, so the reconciler never renders a clustered conf over a de-clustered survivor.
func filterSelfOnly(peers []clusternodes.TopoPeer, selfID string) []clusternodes.TopoPeer {
	for _, p := range peers {
		if p.NodeID == selfID {
			return []clusternodes.TopoPeer{p}
		}
	}
	return nil
}

// reloadNatsServer sends a SIGHUP-reload to the local nats-server via the official `--signal reload`
// resolver (pgrep, no pidfile). Bounded by min(loop ctx, topoReloadTimeout) so a shutdown cancels it
// promptly (C3-m9). The loop is single-goroutine so calls are serial.
func (b *Broker) reloadNatsServer(ctx context.Context) error {
	bin := b.cfg.NatsServerBin
	if bin == "" {
		bin = "nats-server"
	}
	rctx, cancel := context.WithTimeout(ctx, topoReloadTimeout)
	defer cancel()
	return exec.CommandContext(rctx, bin, "--signal", "reload").Run()
}

// probeNatsConfigLoadTime reads the live server's config_load_time off the loopback /varz monitor,
// using the shared topoProbeClient (built once — see M4) which bypasses HTTP_PROXY for the loopback.
func (b *Broker) probeNatsConfigLoadTime() (time.Time, error) {
	resp, err := topoProbeClient.Get("http://" + topoMonitorListen + "/varz")
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var v struct {
		ConfigLoadTime time.Time `json:"config_load_time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return time.Time{}, err
	}
	return v.ConfigLoadTime, nil
}

// selfBusNkeyNeedsBackfill reports whether this broker's replicated bus_nkey_pub is missing OR wrong
// (C3-m3: a mismatch — e.g. a rotated broker.nk or a bad write — must self-heal, not only an empty
// value). Returns false on a read error or when we cannot derive our own pub (no seam to compare).
func (b *Broker) selfBusNkeyNeedsBackfill() bool {
	if b.selfID == "" || b.cfg.AuthCallout == nil || len(b.cfg.AuthCallout.BrokerNkeySeed) == 0 {
		return false
	}
	kp, err := nkeys.FromSeed(b.cfg.AuthCallout.BrokerNkeySeed)
	if err != nil {
		return false
	}
	want, err := kp.PublicKey()
	if err != nil {
		return false
	}
	var got string
	if err := b.cfg.DB.QueryRow(`SELECT bus_nkey_pub FROM cluster_nodes WHERE node_id = ?`, b.selfID).Scan(&got); err != nil {
		return false
	}
	return got != want
}

// backfillSelfBusNkey writes this broker's OWN bus nkey pub (derived from its broker.nk) via the D9
// proposeOrForward path. Idempotent (the Plan's WHERE guard); best-effort (a transient no-leader just
// retries next tick).
func (b *Broker) backfillSelfBusNkey() {
	if b.cfg.AuthCallout == nil || len(b.cfg.AuthCallout.BrokerNkeySeed) == 0 || b.selfID == "" {
		return
	}
	kp, err := nkeys.FromSeed(b.cfg.AuthCallout.BrokerNkeySeed)
	if err != nil {
		return
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return
	}
	payload, _ := json.Marshal(BusNkeySetPayload{NodeID: b.selfID, BusNkeyPub: pub})
	_ = b.proposeOrForward(VerbBusNkeySet, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterBusNkeySet(b.selfID, pub, b.cfg.Now())
	})
}
