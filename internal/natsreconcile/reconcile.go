// Package natsreconcile is the pure step-machine that converges ONE broker's nats.conf to the
// replicated desired topology (C3). It imports only natsconf + natscluster (NO nats, NO raft, NO
// broker → L-2 clean). The reload signal and the observed-generation probe are INJECTED seams
// (build-and-prove style), so the engine itself is deterministic + unit-testable with fakes; the
// broker wires the real `nats-server --signal reload` + `/varz config_load_time` probe.
package natsreconcile

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/natscluster"
	"github.com/LinZiyang666/tether/internal/natsconf"
)

// Action classifies one reconcile pass outcome (drives the status banner + sys.event).
const (
	ActionNoop                 = "noop"                   // already converged + observed live
	ActionReloaded             = "reloaded"               // swapped + reload confirmed by the probe
	ActionSwappedReloadPending = "swapped_reload_pending" // conf swapped+validated, reload NOT confirmed (degraded, not bricked)
	ActionRejected             = "rejected"               // nats-server -t rejected the render (kept old conf + .bak)
	ActionUnresolvable         = "unresolvable"           // missing peer bus identity / self not in peers (fail-closed)
	ActionUnknownDirective     = "unknown_directive"      // an unknown/include directive in the live conf (fail-closed)
	// ActionAwaitingClusteredCutover (G4 #3/#10/#4): the standalone→clustered FIRST-GROW delta is rendered +
	// DryRun-validated (proving the secrets-dir mTLS fallback works) but the swap is WITHHELD. The autonomous
	// reconciler is SIGHUP-only (§11(h)); applying a clustered conf under a running-standalone nats-server and
	// SIGHUPing it would form a clustered-alone JS meta (#10) or orphan the standalone store (#4). The
	// orchestrated `cluster add` cutover owns the apply + the full restart. Not an error — a deliberate hold.
	ActionAwaitingClusteredCutover = "awaiting_clustered_cutover"
)

// Inputs is the per-pass reconcile input. AccountIssuer comes from the broker's own account seam,
// NEVER re-parsed from the conf. Peers are the mesh-eligible brokers (incl. self), each needing a
// non-empty NkeyPub + RouteURL.
type Inputs struct {
	SelfServerName string
	Peers          []natscluster.Broker
	AccountIssuer  string
	Account        string
	JSDomain       string
	ConfPath       string
	NatsServerBin  string
	DesiredGen     uint64
	// SecretsDir (G4 #3), when set, lets the FIRST standalone→clustered grow render succeed: the live
	// standalone conf has no cluster{} block to harvest routes mTLS from (BuildMergedConf hard-fails), so
	// the reconciler derives CA/cert/key from the secrets dir + synthesizes the route listen instead. Empty
	// preserves the pre-G4 harvest-only behavior (a first grow stays ActionRejected until secrets-dir is wired).
	SecretsDir string
}

// Outcome reports the result. AppliedGen is recomputed from the on-disk conf (the conf bytes ARE the
// applied generation); ObservedGen comes from the probe (the LIVE server) — both REAL, never synthesized.
type Outcome struct {
	AppliedGen  uint64
	ObservedGen uint64
	Action      string
	Reason      string // operator next-step text (status banner)
	Err         error
}

// ReconcileOnce runs one convergence step. reload sends the SIGHUP-reload to the local nats-server;
// probe returns the live server's config_load_time. Both may be nil-returning in tests.
func ReconcileOnce(in Inputs, lastApplied, lastObserved uint64, reload func() error, probe func() (time.Time, error)) Outcome {
	// 1. Nothing desired yet → idle.
	if in.DesiredGen == 0 {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionNoop, Reason: "no desired topology generation yet"}
	}

	// 2. Unresolvable: a mesh peer with no bus nkey, or self not present, or empty peer set → keep
	//    serving the old conf (NEVER render a conf that drops a voter from auth_users). Fail-closed.
	if len(in.Peers) == 0 {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionUnresolvable, Reason: "no mesh peers known yet (converging)"}
	}
	selfPresent := false
	for _, p := range in.Peers {
		if p.ServerName == "" || p.NkeyPub == "" || p.RouteURL == "" {
			// C3-m8: an empty server_name/nkey/route ⇒ that peer's identity hasn't replicated yet — stay
			// unresolvable (Render also fail-closes on an empty Local.ServerName, but guard loud here).
			return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionUnresolvable,
				Reason: fmt.Sprintf("awaiting server-name/bus-identity/route for broker %q (converging)", p.ServerName)}
		}
		if p.ServerName == in.SelfServerName {
			selfPresent = true
		}
	}
	if in.SelfServerName == "" || !selfPresent {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionUnresolvable, Reason: "self not yet in the replicated mesh (converging)"}
	}

	// 3. Preflight the LIVE conf — fail-closed on an unknown/include directive (never overwrite a
	//    hand-tuned conf; keep the .bak intact).
	own, err := natsconf.Preflight(in.ConfPath)
	if err != nil {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionUnknownDirective,
			Reason: "nats.conf has an unrecognized directive — fix it, or `cluster reconcile nats --manual`: " + err.Error(), Err: err}
	}

	// 4. Render this broker's desired conf (a PURE function of topology — no generation marker stamped in).
	var self natscluster.Broker
	for _, p := range in.Peers {
		if p.ServerName == in.SelfServerName {
			self = p
		}
	}
	// review F4 + R3: at N=1 the reconciler PRESERVES the current NATS mode — it never crosses the
	// destructive cluster↔standalone boundary on its own. The automatic loop has no operator-intent
	// input, so "one peer" alone must NOT silently strip cluster{} from a still-clustered conf (that
	// would bypass `--to-standalone --confirm-single`, the backup warning, the full restart, and the
	// clustered→standalone JS reset). So render standalone ONLY when the live conf is ALREADY standalone
	// JetStream (the post-`--to-standalone` shape) — keeping it converged across restart/gen-bump/regrow
	// (F4); a still-clustered N=1 stays clustered, pending an explicit operator de-cluster (R3).
	standalone := len(in.Peers) == 1 && in.Peers[0].ServerName == in.SelfServerName && own.IsStandaloneJetStream()
	cfg := natscluster.Config{
		Standalone:    standalone,
		Local:         self,
		Peers:         in.Peers,
		AccountIssuer: in.AccountIssuer,
		Account:       in.Account,
		JSDomain:      in.JSDomain,
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
		// CA/Cert/Key/ClusterListen/ClusterName + the http monitor are harvested from the LIVE conf by
		// BuildMergedConf (C3-B1/B3): the reconciler PRESERVES them rather than forcing new values, so
		// it never drops routes mTLS and never hot-adds an http monitor nats-server can't reload.
	}
	// G4 #3: the FIRST standalone→clustered grow renders clustered (peers≥2) over a live conf that is still
	// standalone JetStream — which has NO cluster{} block for BuildMergedConf to harvest routes mTLS from, so
	// it would hard-fail ("no cluster{} block to harvest"). When SecretsDir is wired, supply the routes-mTLS
	// identity from the secrets dir + synthesize the route listen, so BuildMergedConf skips the harvest and
	// the render succeeds. This delta is ALSO the destructive cutover the reconciler must WITHHOLD (below).
	clusteredOverStandalone := !standalone && own.IsStandaloneJetStream()
	if clusteredOverStandalone && in.SecretsDir != "" {
		cfg.CAFile = filepath.Join(in.SecretsDir, "cluster-ca.pem")
		cfg.CertFile = filepath.Join(in.SecretsDir, "route-cert.pem")
		cfg.KeyFile = filepath.Join(in.SecretsDir, "route-key.pem")
		cfg.ClusterListen = SynthesizeClusterListen(self.RouteURL)
		// ClusterName left empty → natscluster.Render defaults it to "tether" (the harvest default), so a
		// fallback-rendered cluster name matches an already-clustered peer's harvested name.
	}
	merged, err := natsconf.BuildMergedConf(own, cfg)
	if err != nil {
		// C3-M3: a render/merge failure is a PERMANENT (mis)configuration, NOT a transient "awaiting
		// data" — classify it ActionRejected so it emits a sys.event + shows STUCK (never the
		// converging marker), so an operator is paged instead of waiting for self-healing that won't come.
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionRejected,
			Reason: "render (nats.conf could not be assembled — fix the conf/secrets, or `cluster reconcile nats --manual`): " + err.Error(), Err: err}
	}

	current, _ := os.ReadFile(in.ConfPath)
	confMtime := fileMtime(in.ConfPath)

	// 5. Already-converged fast path: on-disk conf == desired ⇒ applied=desired. Confirm OBSERVED via
	//    the probe; if the live server hasn't loaded it (crash between swap and reload), issue reload.
	if string(current) == merged {
		applied := in.DesiredGen
		if observedConfirmed(probe, confMtime) {
			return Outcome{AppliedGen: applied, ObservedGen: in.DesiredGen, Action: ActionNoop, Reason: ""}
		}
		if reload != nil {
			_ = reload()
		}
		if observedConfirmed(probe, confMtime) {
			return Outcome{AppliedGen: applied, ObservedGen: in.DesiredGen, Action: ActionReloaded, Reason: ""}
		}
		return Outcome{AppliedGen: applied, ObservedGen: lastObserved, Action: ActionSwappedReloadPending,
			Reason: "conf is current but the live server has not loaded it — a restart will pick it up"}
	}

	// 6. DryRun gate — `nats-server -t`. A bad/forged generation can NOT brick this broker (no swap).
	if err := natsconf.DryRun(in.NatsServerBin, merged); err != nil {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionRejected,
			Reason: "rendered conf failed `nats-server -t` (not swapping; .bak intact): " + err.Error(), Err: err}
	}

	// 6b. G4 #3/#10/#4 WITHHOLD: the standalone→clustered first-grow delta is now rendered + validated, but
	//     the autonomous reconciler must NOT swap it. It is SIGHUP-only (§11(h)); swapping a clustered conf
	//     under a running-standalone nats + SIGHUP would form a clustered-alone JS meta (#10) or orphan the
	//     standalone store (#4). Hold here — the orchestrated `cluster add` cutover owns the apply + the full
	//     restart (with the JS-store reset). Applied/Observed stay put: topology is honestly NOT converged.
	if clusteredOverStandalone {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionAwaitingClusteredCutover,
			Reason: "standalone→clustered cutover rendered + validated but WITHHELD — a reconciler SIGHUP cannot safely cross it; run `tether cluster add <this-broker>` to perform the coordinated restart"}
	}

	// 7. Atomic swap (.bak + tmp + rename + fsync).
	if err := natsconf.Apply(in.ConfPath, merged); err != nil {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionRejected, Reason: "apply: " + err.Error(), Err: err}
	}
	applied := in.DesiredGen
	swapTime := fileMtime(in.ConfPath)

	// 8. Reload + REAL probe → observed (never "signal returned nil").
	if reload != nil {
		if rerr := reload(); rerr != nil {
			return Outcome{AppliedGen: applied, ObservedGen: lastObserved, Action: ActionSwappedReloadPending,
				Reason: "conf swapped+validated; reload signal failed (a restart will pick it up): " + rerr.Error(), Err: rerr}
		}
	}
	if observedConfirmed(probe, swapTime) {
		return Outcome{AppliedGen: applied, ObservedGen: in.DesiredGen, Action: ActionReloaded, Reason: ""}
	}
	// C3-m6: the reload may be applying ASYNCHRONOUSLY (nats-server reloads in the background), so the
	// immediate probe can miss it; the NEXT tick's merged==current fast-path re-probes and confirms. If
	// it never confirms, the delta was non-reloadable and needs a restart. Word it without over-claiming.
	return Outcome{AppliedGen: applied, ObservedGen: lastObserved, Action: ActionSwappedReloadPending,
		Reason: "conf swapped+validated; awaiting the live server's reload confirmation (re-checked each tick; a non-reloadable delta needs a restart)"}
}

// observedConfirmed reports whether the live server's config_load_time is at/after the conf swap time
// (the server has loaded our generation). A nil probe or a probe error ⇒ NOT confirmed (loud, never
// synthesized green).
func observedConfirmed(probe func() (time.Time, error), notBefore time.Time) bool {
	if probe == nil {
		return false
	}
	loadTime, err := probe()
	if err != nil {
		return false
	}
	return !loadTime.Before(notBefore)
}

// SynthesizeClusterListen derives the local route listen ("0.0.0.0:<port>") from this broker's route URL
// ("nats://host:6222") for the G4 #3 secrets-dir fallback, when there is no live cluster{} block to harvest
// the listen from. Falls back to the nats default route port 6222 if the URL carries no parseable port.
// Exported so the grow cutover (which APPLIES the swap the reconciler WITHHELD) renders the identical listen.
func SynthesizeClusterListen(routeURL string) string {
	h := strings.TrimPrefix(strings.TrimSpace(routeURL), "nats://")
	if _, port, err := net.SplitHostPort(h); err == nil && port != "" {
		return "0.0.0.0:" + port
	}
	return "0.0.0.0:6222"
}

func fileMtime(path string) time.Time {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}
