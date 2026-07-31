package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/proto"
)

// cluster_grow_cutover.go (G4 §B / #3/#4/#10/#23) — the former-N1 standalone→clustered cutover the reconciler
// WITHHELD (ActionAwaitingClusteredCutover). Invoked ONLY by the account-signed grow-trigger `mesh-cutover`
// op on the former sole voter. It renders the clustered conf (secrets-dir mTLS), moves the standalone JS store
// aside (never deletes; data-loss is operator-gated), applies the conf, and HARD-RESTARTS nats-server via a
// same-uid SIGKILL (`nats-server --signal stop`) which systemd `Restart=always` revives clustered — the one
// lifecycle restart tether owns, since it never orchestrates systemctl (cluster.go:782-784) and the reconciler
// is SIGHUP-only (§11(h)). The SIGKILL drops this broker's own NATS connection, so the trigger reply is often
// lost; that is fine — the whole op is STAGED-idempotent, so the orchestrator simply retries and converges.

const cutoverGraceTimeout = 45 * time.Second // how long to wait for the revived nats to report clustered

// A1: the cutover liveness probe is retried a few times before it concludes "not clustered" — a single
// transient /varz error (a 3s timeout, DisableKeepAlives, a reload/GC hiccup) must never make a healthy
// clustered broker fall through to an unconditional SIGKILL of its own data plane.
const (
	cutoverProbeRetries    = 3
	cutoverProbeRetryDelay = 500 * time.Millisecond
)

// growCutoverRevivalFailed is the ClusterGrowResp code when the SIGKILL'd nats-server did not come back
// clustered — a systemd StartLimit / clean-exit stranding the data plane. The operator hint is loud.
const growCutoverRevivalFailed = "cutover_revival_failed"

// performGrowCutover runs the standalone→clustered cutover on THIS (former-N1) broker. It is STAGED-idempotent:
//   - live nats already clustered  → AlreadyDone (a re-run after success).
//   - conf on disk already clustered but nats not live-clustered → the apply landed; only re-SIGKILL + verify
//     (recovers a prior revival failure).
//   - conf still standalone → the full cutover (render → move store → apply → SIGKILL → verify).
func (b *Broker) performGrowCutover(req *proto.ClusterGrowReq) *proto.ClusterGrowResp {
	if b.cl == nil || b.cl.node == nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
	}
	confPath := b.cfg.NatsConfPath
	if confPath == "" {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "no nats.conf path configured — cannot cut over"}
	}

	own, err := natsconf.Preflight(confPath)
	if err != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "preflight live conf: " + err.Error()}
	}
	// The operation is specifically a cutover INTO a clustered JetStream mesh. This precondition must
	// dominate every topology stage: a cluster{} block can select Stage A/B, but it cannot prove that
	// JetStream is enabled. In particular, an epoch sentinel plus `jetstream: false + cluster{}`
	// previously bypassed the late guard and reached AlreadyDone or the SIGKILL/restart-only path.
	if !own.HasJetStream() {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest,
			Error: "live conf has JetStream disabled or absent — refusing an ambiguous cutover into a clustered " +
				"JetStream mesh (enable jetstream with an explicit store_dir first)"}
	}
	// A1: TOLERANT probe — a transient monitor error must not fall through to a spurious SIGKILL of a healthy
	// clustered broker (mesh-cutover is re-invoked on any grow resume).
	liveClustered, _ := b.probeNatsClusteredTolerant()
	confClustered := own.IsClusteredTopology() // TOPOLOGY (RB2-1): has this node's conf already grown?

	// R16 A5-min: a survivor that is CLUSTERED but whose clustered state PREDATES this grow (online
	// force-single / restore left it clustered — a stale single-node/dead-peer meta) must NEVER be silently
	// classified AlreadyDone/restart-only: NATS cannot absorb a joiner into a stale meta, so the 1->2 meta
	// wedges (drills 42/51 root; racknerd incident). The durable evidence that a grow cutover RESET this
	// store is a grow-bak.<epoch> backup (M3). A NORMAL grow enters this cutover STANDALONE and only reaches
	// "clustered" AFTER moving the store aside, so a clustered survivor whose store was NEVER grow-reset is
	// recovered residue. Refuse LOUDLY with the de-cluster remedy rather than wedge the grow. (The full
	// auto-reset + /jsz meta-health verdict is a deferred follow-up; loud-refuse is the safe minimum — no
	// drill depends on in-cutover auto-reset here, and the refusal routes to a SAFE de-cluster+reset+re-grow.)
	if (liveClustered || confClustered) && !b.growCutoverThisEpochEvidence(own.JSStoreDir(), req.GrowEpoch) {
		b.cfg.Logger.Error("grow cutover: survivor is CLUSTERED but no grow ever reset its JS store — recovered clustered residue; a stale single-node meta cannot absorb the joiner, so refusing rather than wedging the 1->2 meta",
			"remedy", "tether cluster reconcile nats --to-standalone --confirm-single --reset-js, then re-run cluster add")
		b.pubSysEvent("grow_cutover_clustered_residue", map[string]any{
			"remedy": "reconcile nats --to-standalone --confirm-single --reset-js",
		})
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest,
			Error: "refusing cutover: this broker is CLUSTERED but the grow never reset its JetStream store — it is recovered clustered residue (a stale single-node/dead-peer JS meta that cannot absorb the joiner, so the 1->2 meta would wedge). De-cluster + reset it to standalone first: `tether cluster reconcile nats --to-standalone --confirm-single --reset-js`, then re-run `cluster add`."}
	}

	// Stage A: live already clustered (with grow-reset evidence) → done.
	if liveClustered {
		return &proto.ClusterGrowResp{OK: true, AlreadyDone: true}
	}

	// Stage B: conf already clustered on disk but nats not live-clustered → a prior apply landed and the
	// revival failed/was interrupted. Re-SIGKILL + verify only (do NOT re-move the store — it is already reset).
	if confClustered {
		return b.restartAndVerifyClustered()
	}

	// R3 gate: the DATA-plane cutover follows the COMMITTED control plane — self must be in a committed
	// >=2-server raft config. This is the AddNonvoter-committed state the join op reaches before catch-up; it
	// guarantees the grow is real (not an uncommitted or self-only config that would re-cluster a lone survivor).
	servers, cerr := b.cl.node.RaftConfiguration()
	if cerr != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeStoreError, Error: "read raft config: " + cerr.Error()}
	}
	selfInCfg := false
	for _, s := range servers {
		if s.NodeID == b.selfID {
			selfInCfg = true
		}
	}
	if len(servers) < 2 || !selfInCfg {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest,
			Error: fmt.Sprintf("refusing cutover: self must be in a committed >=2-server raft config (have %d servers, self_in_config=%v) — the join must commit AddNonvoter first", len(servers), selfInCfg)}
	}

	// Render the clustered conf the reconciler withheld (identical render: same peers + secrets-dir mTLS).
	merged, rerr := b.renderClusteredCutoverConf(own)
	if rerr != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "render clustered conf: " + rerr.Error()}
	}
	bin := b.cfg.NatsServerBin
	if bin == "" {
		bin = "nats-server"
	}
	if derr := natsconf.DryRun(bin, merged); derr != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "clustered conf failed `nats-server -t`: " + derr.Error()}
	}

	// #4: move the standalone JS store aside BEFORE the restart (never delete; non-empty needs an ack). The
	// move happens while nats still runs the OLD standalone conf; on revival nats reads the clustered conf +
	// the fresh empty store, so the incompatible single-node meta cannot orphan the clustered meta.
	// M1: both --reset-former-js AND --preserve-js-data acknowledge the reset (the store is moved aside, never
	// deleted, in BOTH cases). Auto backup→restore is NOT implemented in v1 (documented in plan §12); the
	// moved-aside dir is the operator's restore source. So PreserveData unblocks the non-empty gate too.
	backup, merr := b.moveAsideJetStreamStore(own.JSStoreDir(), req.GrowEpoch, req.ResetAck || req.PreserveData)
	if merr != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: merr.Error()}
	}
	// M2: durably mark THIS epoch's cutover as run — even if the store was absent (no backup created) — so a
	// later resume (A5-min) is not falsely refused as recovered residue.
	b.markGrowCutoverEpoch(req.GrowEpoch)

	// Apply the clustered conf (atomic swap, .bak kept). nats has NOT reloaded it yet — the SIGKILL+revive does.
	if aerr := natsconf.Apply(confPath, merged); aerr != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeStoreError, Error: "apply clustered conf: " + aerr.Error()}
	}

	resp := b.restartAndVerifyClustered()
	resp.BackupPath = backup
	return resp
}

// cutoverAction is the Stage-B restart decision derived from a tolerant liveness probe (A1).
type cutoverAction int

const (
	cutoverAlreadyClustered cutoverAction = iota // healthy clustered — return OK, never bounce
	cutoverSIGKILLToRevive                       // up but standalone — SIGKILL so systemd revives it clustered
	cutoverAwaitRevival                          // down/unreachable — skip SIGKILL, just wait for systemd Restart=always
)

// cutoverRestartDecision maps a (clustered, reachable) probe verdict to the Stage-B action. Pure, so the
// three-way choice is table-testable without an http monitor. A down nats needs NO SIGKILL: `--signal stop`
// on an absent process just errors, and systemd Restart=always is already reviving it against the (now
// clustered) on-disk conf — so we only poll for its clustered return.
func cutoverRestartDecision(clustered, reachable bool) cutoverAction {
	switch {
	case clustered:
		return cutoverAlreadyClustered
	case reachable:
		return cutoverSIGKILLToRevive
	default:
		return cutoverAwaitRevival
	}
}

// probeNatsClusteredTolerant probes the loopback monitor up to cutoverProbeRetries times before concluding,
// so a single transient /varz error does not misread a healthy clustered nats as down (A1). A clean reply
// short-circuits immediately; a persistent error over the retry window yields (false, false = unreachable).
func (b *Broker) probeNatsClusteredTolerant() (clustered, reachable bool) {
	for i := 0; i < cutoverProbeRetries; i++ {
		if name, err := b.probeNatsClusterName(); err == nil {
			return name != "", true
		}
		if i < cutoverProbeRetries-1 {
			time.Sleep(cutoverProbeRetryDelay)
		}
	}
	return false, false
}

// restartAndVerifyClustered brings the local nats-server up CLUSTERED and polls the loopback /varz until it
// reports a cluster name, or BLOCKs loudly on a revival failure. A1: a THREE-WAY tolerant liveness check
// runs BEFORE any SIGKILL — a healthy clustered broker returns immediately (no bounce), an up-standalone
// broker is SIGKILL'd (the real revive), and a down broker is only waited on (systemd Restart=always revives
// it; SIGKILLing an absent process would just error). The store move already happened in performGrowCutover.
func (b *Broker) restartAndVerifyClustered() *proto.ClusterGrowResp {
	switch cutoverRestartDecision(b.probeNatsClusteredTolerant()) {
	case cutoverAlreadyClustered:
		return &proto.ClusterGrowResp{OK: true, AlreadyDone: true} // already clustered — do NOT bounce a healthy data plane
	case cutoverSIGKILLToRevive:
		if err := b.hardRestartNatsServer(); err != nil {
			return &proto.ClusterGrowResp{Code: growCutoverRevivalFailed,
				Error: "SIGKILL nats-server (`--signal stop`) failed: " + err.Error()}
		}
	case cutoverAwaitRevival:
		// nats is down/unreachable → no SIGKILL; systemd Restart=always is already reviving it clustered. Poll.
		// Note (A1 Stage-C): a LIVE up-standalone nats whose loopback monitor is persistently unprobeable is
		// classified here (unreachable) and its needed SIGKILL is deferred to the STAGED-idempotent driver
		// retry rather than fired blind — a rare, loud (revival_failed), self-healing edge we accept over
		// re-introducing the ambiguity the tolerant three-way probe exists to eliminate.
	}
	deadline := b.cfg.Now().Add(cutoverGraceTimeout)
	for b.cfg.Now().Before(deadline) {
		if name, err := b.probeNatsClusterName(); err == nil && name != "" {
			return &proto.ClusterGrowResp{OK: true}
		}
		time.Sleep(1 * time.Second)
	}
	// Revival failed: nats did not come back clustered. Never silently strand the data plane — surface it.
	b.cfg.Logger.Error("grow cutover: nats-server did not revive clustered after SIGKILL — the data plane is DOWN on this broker",
		"remedy", "on this host run `systemctl reset-failed nats-server && systemctl start nats-server` (the clustered conf + a fresh JS store are already in place); the JS store backup is preserved")
	b.pubSysEvent("grow_cutover_revival_failed", map[string]any{
		"remedy": "systemctl reset-failed nats-server && systemctl start nats-server",
	})
	return &proto.ClusterGrowResp{Code: growCutoverRevivalFailed,
		Error: "nats-server did not revive clustered within " + cutoverGraceTimeout.String() +
			" — run `systemctl reset-failed nats-server && systemctl start nats-server` on this host (conf + fresh store are in place; JS backup preserved)"}
}

// renderClusteredCutoverConf renders the clustered nats.conf via the SAME path the reconciler uses (peers
// from the topology inputs + the secrets-dir mTLS fallback + the synthesized route listen).
//
// THE CONTRACT, STATED CORRECTLY (B5, BUG-3)
// ------------------------------------------
// The applied conf is byte-identical to the one the reconciler DryRun-validated then withheld, EXCEPT
// for the HTTP monitor, which this path FORCES and the reconciler HARVESTS. That exception is not an
// oversight — it is the m2 decision below, and restartAndVerifyClustered depends on it.
//
// The previous version of this comment claimed unqualified byte-identity, which was simply false: the
// forcing is twenty lines further down, deliberate, and documented. It happened to be harmless because
// the two renders agree whenever the live conf already carries `http: 127.0.0.1:8223` — and disagree
// exactly when it does not, which is the case m2 exists for. A comment that overstates an invariant is
// worse than no comment: the next person to add a Config field reads "byte-identical", assumes the two
// paths cannot diverge, and does not go looking for the second assembly point.
//
// What makes the rest of the identity hold is no longer a coincidence, a convention, or a test: the two
// paths call the SAME function. natsconf.RenderDesired is the single assembly, and the only thing this
// file supplies beyond the shared topology inputs is its INTENT plus the two declared departures below.
// A new Config field can no longer be present in one render and absent from the other, because there is
// only one render — which is what the two deleted fields (natsconf.Config.JSDomain and Account, read by
// the reconciler's old literal, set by nobody) would otherwise have been the vector for.
//
// Pinned by TestCutoverRenderForcesTheMonitorOntoAConfThatHasNone and its siblings in
// cutover_render_test.go — the characterization tests for THIS function. (An earlier version of this
// comment named TestCutoverRenderMatchesReconcilerRenderExceptTheMonitor, which was false: that test
// builds both Configs from a fixture inside itself and never reaches this code, so deleting the forced
// MonitorListen below left it green. The mutation-honesty audit caught the misattribution.)
func (b *Broker) renderClusteredCutoverConf(own *natsconf.Ownership) (string, error) {
	in, ok := b.buildTopologyInputs(1)
	if !ok {
		return "", fmt.Errorf("could not build topology inputs")
	}
	if len(in.Peers) < 2 {
		return "", fmt.Errorf("fewer than 2 mesh peers (%d) — the grow has not converged the roster yet", len(in.Peers))
	}
	if in.SecretsDir == "" {
		return "", fmt.Errorf("no secrets dir configured — cannot render routes mTLS for the first-grow cutover")
	}
	var self natsconf.Broker
	for _, p := range in.Peers {
		if p.ServerName == in.SelfServerName {
			self = p
		}
	}
	if self.ServerName == "" {
		return "", fmt.Errorf("self not present in the mesh peers")
	}
	// B5: ONE assembly, shared with the reconciler. It used to be a hand-built natsconf.Config literal
	// here and another one there, with the route-listen derivation EXPORTED purely so this file could
	// reproduce it byte-for-byte — the export's own doc said "Exported so the grow cutover renders the
	// identical listen". That is what "these two renders agree" being a CONVENTION looks like; a call is
	// the difference.
	//
	// IntentForceClustered, not inference: this runs ON the standalone->clustered transition, so the live
	// conf is by definition still standalone and anything that reads the mode off it renders exactly the
	// wrong thing.
	//
	// MonitorListen is FORCED to topoMonitorListen (127.0.0.1:8223) — restartAndVerifyClustered probes
	// exactly that address, so harvesting a possibly-absent http block would false-report a healthy
	// revival as cutover_revival_failed (a 45s connection-refused). This is legal here and illegal in the
	// reconciler for the same reason: nats-server cannot hot-add an http port on SIGHUP, so only a
	// restart-bearing path may ESTABLISH one. Every other restart-bearing takeover forces 8223 too.
	//
	// SecretsDir supplies the routes-mTLS identity + route listen, needed exactly because a standalone
	// conf has no cluster{} block to harvest them from.
	return natsconf.RenderDesired(in, own, natsconf.RenderOverride{
		Intent:        natsconf.IntentForceClustered,
		MonitorListen: topoMonitorListen,
		SecretsDir:    in.SecretsDir,
	})
}

// moveAsideJetStreamStore renames the standalone JS store to <store>.grow-bak.<epoch> (NEVER deletes) so the
// broker boots clustered with a fresh empty store, and recreates the empty store dir. Gated: a NON-EMPTY
// (data-bearing) store refuses without ResetAck (loud, operator-gated — plan §3 Q3). Idempotent per epoch via
// a backup-dir-first check + sentinel, so a retry after the move does not double-move a freshly-clustered
// store. Returns the backup path. R16 A0: the delicate move-aside logic (M3 backup-dir-first, m4 fail-closed
// ReadDir, non-empty→refuse, EACCES→loud chown hint) now lives ONCE in natsconf.MoveAsideJSStore, shared with
// the grow joiner reset (A1), offline force-single (A3), and reconcile-nats --to-standalone (A4).
func (b *Broker) moveAsideJetStreamStore(storeDir, epoch string, ack bool) (string, error) {
	if storeDir == "" {
		return "", nil // no explicit store dir — nothing to move (render already refused an implicit one)
	}
	if epoch == "" {
		epoch = "unknown"
	}
	backup := storeDir + ".grow-bak." + epoch
	sentinel := filepath.Join(b.cfg.ClusterDataDir, ".grow-cutover-"+epoch+".done")
	return natsconf.MoveAsideJSStore(storeDir, backup, sentinel, ack)
}

// growCutoverThisEpochEvidence reports whether THIS grow epoch's cutover already handled the survivor — the
// durable proof (A5-min) that a clustered survivor is a freshly-reset grow store rather than recovered
// clustered residue that predates the grow (M2). It is EPOCH-SPECIFIC: keying on ANY historical grow-bak.*
// (backups are never pruned, and MoveAsideJSStore renames even an empty store, so every ex-joiner/ex-former-N1
// carries one forever) would silently DISARM the guard and absorb real racknerd-class residue as AlreadyDone.
// Two evidence forms: (a) the per-epoch sentinel .grow-cutover-<epoch>.done, written even on the absent-store
// path so a legit grow onto an empty-store survivor is not falsely refused on resume; (b) the <store>.grow-bak.<epoch>
// backup. No epoch, or a read miss, is NO evidence → refuse loudly (never silently absorb; plan §8.2).
func (b *Broker) growCutoverThisEpochEvidence(storeDir, epoch string) bool {
	if epoch == "" {
		return false // no epoch to key on → cannot prove THIS grow handled it → treat as residue (refuse loudly)
	}
	if b.cfg.ClusterDataDir != "" {
		if _, err := os.Stat(filepath.Join(b.cfg.ClusterDataDir, ".grow-cutover-"+epoch+".done")); err == nil {
			return true
		}
	}
	if storeDir != "" {
		if _, err := os.Stat(storeDir + ".grow-bak." + epoch); err == nil {
			return true
		}
	}
	return false
}

// markGrowCutoverEpoch writes the per-epoch cutover sentinel so a resume is not falsely refused as residue,
// even when the store was absent (MoveAsideJSStore writes the sentinel only when it actually moves something).
func (b *Broker) markGrowCutoverEpoch(epoch string) {
	if epoch == "" || b.cfg.ClusterDataDir == "" {
		return
	}
	sentinel := filepath.Join(b.cfg.ClusterDataDir, ".grow-cutover-"+epoch+".done")
	if _, err := os.Stat(sentinel); err == nil {
		return
	}
	_ = os.WriteFile(sentinel, []byte("grow-cutover epoch="+epoch+"\n"), 0o600)
}

// hardRestartNatsServer SIGKILLs the local nats-server via nats-server's own signal resolver
// (`nats-server --signal stop` maps to SIGKILL) — the same discovery `--signal reload` uses, no pgrep/pidfile.
// systemd `Restart=always` (install.sh) revives it, reading the freshly-applied clustered conf + fresh store.
func (b *Broker) hardRestartNatsServer() error {
	bin := b.cfg.NatsServerBin
	if bin == "" {
		bin = "nats-server"
	}
	// A6: bound the signal-send with a timeout (mirrors reloadNatsServer's C3-m9 guard) so a hung signal
	// resolver cannot block the grow-trigger subscription goroutine forever. No ctx flows through the async
	// NATS callback chain, so derive from Background; the enclosing 45s verify poll stays synchronous by design.
	ctx, cancel := context.WithTimeout(context.Background(), topoReloadTimeout)
	defer cancel()
	return exec.CommandContext(ctx, bin, "--signal", "stop").Run()
}

// probeNatsClusterName reads the loopback /varz and returns the live server's cluster name ("" = standalone /
// not clustered / not up). A non-empty name proves nats revived in CLUSTER mode.
func (b *Broker) probeNatsClusterName() (string, error) {
	//nolint:noctx // performGrowCutover/restartAndVerifyClustered are RPC handlers two hops up and
	// neither takes a ctx; this is a 3s loopback GET against a client with its own timeout.
	resp, err := topoProbeClient.Get("http://" + topoMonitorListen + "/varz")
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var varz struct {
		Cluster struct {
			Name string `json:"name"`
		} `json:"cluster"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&varz); err != nil {
		return "", err
	}
	return varz.Cluster.Name, nil
}
