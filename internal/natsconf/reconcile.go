// reconcile.go — the pure step-machine that converges ONE broker's nats.conf to the replicated desired
// topology (C3). Was package natsreconcile until B5.
//
// The reload signal and the observed-generation probe are INJECTED seams (build-and-prove style), so the
// engine is deterministic and unit-testable with fakes; the broker wires the real
// `nats-server --signal reload` + `/varz config_load_time` probe.
//
// WHY THIS IS IN THE SAME PACKAGE AS THE RENDERER AND THE PARSER (B5)
// -------------------------------------------------------------------
// It was three packages — natsconf (parse + apply), natscluster (render), natsreconcile (converge) —
// and the boundaries were in the wrong place. natsreconcile imported BOTH of the others; the
// route-listen derivation had to be EXPORTED from it purely so the grow cutover in internal/broker
// could reproduce the identical string, with a doc comment that said exactly that; and three secrets
// FILENAMES were hand-copied between the two paths that build a first-grow conf. A function forced to be
// exported for cross-package synchronisation is direct evidence of a misplaced boundary, and there were
// four such synchronisation points, about to become five.
//
// This is NOT the "merge small packages so the count looks tidy" that the roadmap's §7.1 forbids — that
// section's criterion is the IMPORT SURFACE, and it names the leaf packages (clustermanifest,
// clusterupgrade, xferaudit, testharness) that have `internal import = 0` and must stay. These three
// were the opposite case: mutually importing, with a shared invariant maintained by convention across
// the boundary. The L-2 property that mattered — NO nats, NO raft, NO broker — is preserved and pinned
// by test/determinism's raft-confinement gate; what disappeared is only the three-way split.
package natsconf

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	Peers          []Broker
	AccountIssuer  string
	// Account and JSDomain WERE here and are deliberately GONE (B5, BUG-2). Both had a reader
	// (they were copied into Config below) and NO production writer: buildTopologyInputs,
	// the only production assembler of this struct, sets seven fields and never those two. Account was
	// harmless (empty defaults to "$G"), but JSDomain fed a Render path that emits a directive
	// Preflight REFUSES — so the dead plumbing was a loaded gun, not just clutter. Same
	// reader-with-no-writer shape loopset.go records having shipped once before.
	ConfPath      string
	NatsServerBin string
	DesiredGen    uint64
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
	own, err := Preflight(in.ConfPath)
	if err != nil {
		return Outcome{AppliedGen: lastApplied, ObservedGen: lastObserved, Action: ActionUnknownDirective,
			Reason: "nats.conf has an unrecognized directive — fix it, or `cluster reconcile nats --manual`: " + err.Error(), Err: err}
	}

	// 4. Render this broker's desired conf (a PURE function of topology — no generation marker stamped in).
	//    RenderDesired resolves self from the peer set itself, so there is no second copy of that lookup.
	// The reconciler's intent is PRESERVE, and that word carries the whole review-F4/R3 argument: the
	// autonomous loop has no operator-intent input, so a single-peer roster must NOT silently strip
	// cluster{} from a still-clustered conf (that would bypass `--to-standalone --confirm-single`, the
	// backup warning, the full restart, and the clustered→standalone JS reset). RenderIntent.standalone
	// is where the four decisions now live side by side.
	//
	// G4 #3: SecretsDir lets the FIRST standalone→clustered grow render at all — the live conf is still
	// standalone JetStream, so it has NO cluster{} block for BuildMergedConf to harvest routes mTLS from
	// and the render would hard-fail. RenderDesired applies it only on a clustered render, which is
	// exactly that case. This delta is ALSO the destructive cutover the reconciler must WITHHOLD (below).
	merged, err := RenderDesired(in, own, RenderOverride{
		Intent:     IntentPreserve,
		SecretsDir: in.SecretsDir,
		// MonitorListen deliberately empty: BuildMergedConf then HARVESTS the live conf's http block.
		// The reconciler is SIGHUP-only and nats-server rejects an http-port add on reload, so this path
		// may never force a monitor — only a restart-bearing takeover can establish one.
	})
	// clusteredOverStandalone is the WITHHOLD condition below: a clustered render over a live conf that
	// is still standalone JetStream is the destructive first-grow cutover, and a SIGHUP cannot safely
	// cross it. Derived from the same intent resolution the render used, so the two can never disagree.
	// TOPOLOGY (RB2-1): the withhold condition is "we would render CLUSTERED over a conf that has no
	// cluster{} block yet" — the destructive first-grow cutover a SIGHUP cannot cross. Whether that
	// node's JetStream is on decides how much DATA is at stake, not whether the transition is crossing.
	clusteredOverStandalone := !IntentPreserve.standalone(in, own) && own.IsStandaloneTopology()
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
	if err := DryRun(in.NatsServerBin, merged); err != nil {
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
	if err := Apply(in.ConfPath, merged); err != nil {
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

// ApplySecretsDirIdentity fills the routes-mTLS identity + route listen from a secrets directory.
//
// WHY THIS EXISTS (B5)
// --------------------
// Two paths need it and they used to hand-copy it: the reconciler's first-grow fallback (the live conf is
// still standalone JetStream, so there is no cluster{} block for BuildMergedConf to harvest mTLS from) and
// the grow cutover (same situation, one orchestration layer up). Between them they repeated three secrets
// FILENAMES and one listen derivation — and the derivation had to be EXPORTED purely so the other package
// could reproduce it byte-for-byte. Its old doc said so outright: "Exported so the grow cutover renders
// the identical listen". A function forced to be exported for cross-package synchronisation is direct
// evidence that the package boundary was drawn in the wrong place, and merging the three nats packages is
// what let it become private again.
//
// The filenames are here and nowhere else. Rotating to a different cert layout is now one edit rather
// than a search for every place that spelled "route-cert.pem".
//
// ClusterName is deliberately left alone: Render defaults an empty one to "tether", which is also the
// harvest default, so a fallback-rendered cluster name matches an already-clustered peer's harvested name.
func (c *Config) ApplySecretsDirIdentity(secretsDir, routeURL string) {
	c.CAFile = filepath.Join(secretsDir, "cluster-ca.pem")
	c.CertFile = filepath.Join(secretsDir, "route-cert.pem")
	c.KeyFile = filepath.Join(secretsDir, "route-key.pem")
	c.ClusterListen = synthesizeClusterListen(routeURL)
}

// synthesizeClusterListen derives the local route listen ("0.0.0.0:<port>") from this broker's route URL
// ("nats://host:6222") for the G4 #3 secrets-dir fallback, when there is no live cluster{} block to harvest
// the listen from. Falls back to the nats default route port 6222 if the URL carries no parseable port.
// Exported so the grow cutover (which APPLIES the swap the reconciler WITHHELD) renders the identical listen.
func synthesizeClusterListen(routeURL string) string {
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
