package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/natscluster"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/natsreconcile"
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

	// Stage A: live already clustered → done.
	if name, err := b.probeNatsClusterName(); err == nil && name != "" {
		return &proto.ClusterGrowResp{OK: true, AlreadyDone: true}
	}

	own, err := natsconf.Preflight(confPath)
	if err != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest, Error: "preflight live conf: " + err.Error()}
	}

	// Stage B: conf already clustered on disk but nats not live-clustered → a prior apply landed and the
	// revival failed/was interrupted. Re-SIGKILL + verify only (do NOT re-move the store — it is already reset).
	if own.IsClusteredJetStream() {
		return b.restartAndVerifyClustered()
	}

	if !own.IsStandaloneJetStream() {
		return &proto.ClusterGrowResp{Code: adminsock.CodeBadRequest,
			Error: "live conf is neither standalone nor clustered JetStream — refusing an ambiguous cutover"}
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

	// Apply the clustered conf (atomic swap, .bak kept). nats has NOT reloaded it yet — the SIGKILL+revive does.
	if aerr := natsconf.Apply(confPath, merged); aerr != nil {
		return &proto.ClusterGrowResp{Code: adminsock.CodeStoreError, Error: "apply clustered conf: " + aerr.Error()}
	}

	resp := b.restartAndVerifyClustered()
	resp.BackupPath = backup
	return resp
}

// restartAndVerifyClustered SIGKILLs the local nats-server (revived clustered by systemd Restart=always) and
// polls the loopback /varz until it reports a cluster name, or BLOCKs loudly on a revival failure.
func (b *Broker) restartAndVerifyClustered() *proto.ClusterGrowResp {
	if err := b.hardRestartNatsServer(); err != nil {
		return &proto.ClusterGrowResp{Code: growCutoverRevivalFailed,
			Error: "SIGKILL nats-server (`--signal stop`) failed: " + err.Error()}
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
// from the topology inputs + the secrets-dir mTLS fallback + the synthesized route listen), so the applied
// conf is byte-identical to the one the reconciler DryRun-validated then withheld.
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
	var self natscluster.Broker
	for _, p := range in.Peers {
		if p.ServerName == in.SelfServerName {
			self = p
		}
	}
	if self.ServerName == "" {
		return "", fmt.Errorf("self not present in the mesh peers")
	}
	cfg := natscluster.Config{
		Standalone:    false,
		Local:         self,
		Peers:         in.Peers,
		AccountIssuer: in.AccountIssuer,
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
		// m2: FORCE the loopback monitor to topoMonitorListen (127.0.0.1:8223) — restartAndVerifyClustered
		// probes exactly that address, so harvesting a possibly-absent http block would false-report a healthy
		// revival as cutover_revival_failed (a 45s connection-refused). Every other restart-bearing takeover
		// forces 8223 too (cluster_natsconf.go). The revived clustered nats must serve the monitor the probe hits.
		MonitorListen: topoMonitorListen,
		CAFile:        filepath.Join(in.SecretsDir, "cluster-ca.pem"),
		CertFile:      filepath.Join(in.SecretsDir, "route-cert.pem"),
		KeyFile:       filepath.Join(in.SecretsDir, "route-key.pem"),
		ClusterListen: natsreconcile.SynthesizeClusterListen(self.RouteURL),
	}
	return natsconf.BuildMergedConf(own, cfg)
}

// moveAsideJetStreamStore renames the standalone JS store to <store>.grow-bak.<epoch> (NEVER deletes) so the
// broker boots clustered with a fresh empty store, and recreates the empty store dir. Gated: a NON-EMPTY
// (data-bearing) store refuses without ResetAck (loud, operator-gated — plan §3 Q3). Idempotent per epoch via
// a sentinel, so a retry after the move does not double-move a freshly-clustered store. Returns the backup path.
func (b *Broker) moveAsideJetStreamStore(storeDir, epoch string, ack bool) (string, error) {
	if storeDir == "" {
		return "", nil // no explicit store dir — nothing to move (render already refused an implicit one)
	}
	if epoch == "" {
		epoch = "unknown"
	}
	backup := storeDir + ".grow-bak." + epoch
	sentinel := filepath.Join(b.cfg.ClusterDataDir, ".grow-cutover-"+epoch+".done")
	// M3: the DURABLE evidence of the move is the backup dir itself — check it FIRST. The sentinel is written
	// last (after the irreversible rename+recreate) and can be lost to a crash/kill/OOM in that window; keying
	// idempotency on it alone wedged a resume with ENOTEMPTY (rename of the recreated-empty store over the
	// data-bearing backup). If the backup already exists, this epoch's move already happened → no-op.
	if _, berr := os.Stat(backup); berr == nil {
		return "", nil // already moved for this grow epoch (durable backup present) — idempotent no-op
	}
	if _, serr := os.Stat(sentinel); serr == nil {
		return "", nil // sentinel present (belt-and-suspenders) — idempotent no-op
	}
	if fi, serr := os.Stat(storeDir); serr != nil || !fi.IsDir() {
		return "", nil // no store dir on disk yet — nothing to reset
	}
	// m4: fail CLOSED on a ReadDir error — a store we cannot enumerate must be treated as POTENTIALLY
	// data-bearing (require the operator ack), never silently as empty (a gate whose job is to fail loud).
	entries, rerr := os.ReadDir(storeDir)
	if (rerr != nil || len(entries) > 0) && !ack {
		reason := "is NON-EMPTY (data-bearing)"
		if rerr != nil {
			reason = "could not be enumerated (" + rerr.Error() + ") — treating it as data-bearing"
		}
		return "", fmt.Errorf("former-N1 JetStream store %q %s — refusing to reset it without acknowledgement; "+
			"re-run `cluster add` with --reset-former-js OR --preserve-js-data (both move the store aside to %s; it is NEVER deleted, and you can restore it by hand)", storeDir, reason, backup)
	}
	if err := os.Rename(storeDir, backup); err != nil {
		return "", fmt.Errorf("move JS store aside: %w", err)
	}
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		return backup, fmt.Errorf("recreate empty JS store dir: %w", err)
	}
	if werr := os.WriteFile(sentinel, []byte(b.cfg.Now().UTC().Format(time.RFC3339Nano)+" backup="+backup+"\n"), 0o600); werr != nil {
		b.cfg.Logger.Warn("grow cutover: could not write move-aside sentinel (harmless — the backup dir is the durable idempotency guard)", "err", werr)
	}
	return backup, nil
}

// hardRestartNatsServer SIGKILLs the local nats-server via nats-server's own signal resolver
// (`nats-server --signal stop` maps to SIGKILL) — the same discovery `--signal reload` uses, no pgrep/pidfile.
// systemd `Restart=always` (install.sh) revives it, reading the freshly-applied clustered conf + fresh store.
func (b *Broker) hardRestartNatsServer() error {
	bin := b.cfg.NatsServerBin
	if bin == "" {
		bin = "nats-server"
	}
	return exec.Command(bin, "--signal", "stop").Run()
}

// probeNatsClusterName reads the loopback /varz and returns the live server's cluster name ("" = standalone /
// not clustered / not up). A non-empty name proves nats revived in CLUSTER mode.
func (b *Broker) probeNatsClusterName() (string, error) {
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
