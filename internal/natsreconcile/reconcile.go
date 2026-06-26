// Package natsreconcile is the pure step-machine that converges ONE broker's nats.conf to the
// replicated desired topology (C3). It imports only natsconf + natscluster (NO nats, NO raft, NO
// broker → L-2 clean). The reload signal and the observed-generation probe are INJECTED seams
// (build-and-prove style), so the engine itself is deterministic + unit-testable with fakes; the
// broker wires the real `nats-server --signal reload` + `/varz config_load_time` probe.
package natsreconcile

import (
	"fmt"
	"os"
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
	cfg := natscluster.Config{
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

func fileMtime(path string) time.Time {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}
