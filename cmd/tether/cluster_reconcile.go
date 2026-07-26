package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
)

// cluster_reconcile.go (C3) — `cluster reconcile nats`. The per-broker topology reconciler converges
// each broker's nats.conf AUTOMATICALLY (no human re-running takeover on every box); this command
// (a) `--all --wait` watches that convergence and exits non-zero naming any laggard, and (b)
// `--manual` runs the one-time MANUAL takeover (the first single→cluster cutover the reconciler
// cannot bootstrap) — the demoted `takeover-natsconf`.

func newClusterReconcileCmd(socketPath *string) *cobra.Command {
	parent := &cobra.Command{
		Use:   "reconcile",
		Short: "Reconcile cluster-managed config (NATS topology) across brokers",
		Long: "After a membership change the per-broker topology reconciler converges every broker's\n" +
			"nats.conf automatically (no manual re-run). `reconcile nats --all --wait` blocks until that\n" +
			"convergence completes; `reconcile nats --manual` runs the one-time manual takeover.",
		Example: "  tether cluster reconcile nats --all --wait   # block until every broker's nats.conf converges\n" +
			"  tether cluster reconcile nats --manual ...   # one-time manual takeover (first single→cluster cutover)",
	}
	parent.AddCommand(newClusterReconcileNatsCmd(socketPath))
	return parent
}

func newClusterReconcileNatsCmd(socketPath *string) *cobra.Command {
	var f natsconfTakeoverFlags
	var manual, all, wait, toStandalone, confirmSingle, resetJS bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "nats",
		Short: "Converge each broker's nats.conf to the desired topology (automatic) or run a one-time manual takeover (--manual)",
		Long: "Without --manual this is the AUTOMATIC path: each broker's reconciler renders + validates +\n" +
			"swaps + reloads its own nats.conf when the desired topology generation advances. `--all --wait`\n" +
			"blocks until EVERY voter's live NATS topology reaches the desired generation, exiting non-zero\n" +
			"and naming any laggard on timeout.\n\n" +
			"With --manual it runs the one-time MANUAL takeover (the first single→cluster cutover the\n" +
			"reconciler cannot bootstrap), validating with `nats-server -t` before swapping and keeping a .bak.",
		Example: "  # watch the automatic convergence after adding/removing a broker:\n" +
			"  tether cluster reconcile nats --all --wait\n\n" +
			"  # one-time MANUAL takeover (first single→cluster cutover), full --peer mesh:\n" +
			"  tether cluster reconcile nats --manual --server-name brk-a --route-url nats://10.0.0.1:6222 \\\n" +
			"      --peer brk-a,nats://10.0.0.1:6222,U… --peer brk-b,nats://10.0.0.2:6222,U…",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// --confirm-single / --reset-js are meaningful ONLY with --to-standalone; silently ignoring them
			// on the auto/manual paths would leave an operator believing a destructive intent took effect.
			if !toStandalone && (confirmSingle || resetJS) {
				return usageErr("reconcile nats: --confirm-single / --reset-js are only valid with --to-standalone")
			}
			if toStandalone {
				if all || wait || manual {
					return usageErr("reconcile nats: --to-standalone cannot be combined with --all/--wait/--manual")
				}
				return runReconcileToStandalone(cmd, &f, confirmSingle, resetJS)
			}
			if manual {
				if all || wait {
					return usageErr("reconcile nats: --manual cannot be combined with --all/--wait")
				}
				return runNatsconfTakeover(cmd, &f)
			}
			return runReconcileNatsAuto(cmd, f.takeoverSocket, f.secretsDir, f.confPath, wait, timeout)
		},
	}
	bindNatsconfTakeoverFlags(cmd, &f)
	cmd.Flags().BoolVar(&manual, "manual", false, "run a one-time MANUAL nats.conf takeover (first single→cluster cutover) instead of the automatic per-broker reconcile")
	cmd.Flags().BoolVar(&all, "all", false, "ensure ALL brokers converge their nats.conf (the reconciler runs automatically; pair with --wait to block)")
	cmd.Flags().BoolVar(&wait, "wait", false, "block until every voter's live NATS topology reaches the desired generation (or --timeout), naming any laggard")
	cmd.Flags().DurationVar(&timeout, "timeout", 60*time.Second, "with --wait: give up after this long, exit non-zero naming the laggard(s)")
	cmd.Flags().BoolVar(&toStandalone, "to-standalone", false, "SHRINK: re-render this lone survivor's nats.conf WITHOUT the cluster{} block (standalone JetStream). The FINAL N=1 de-cluster step; requires --confirm-single + a full nats-server restart")
	cmd.Flags().BoolVar(&confirmSingle, "confirm-single", false, "with --to-standalone: assert this is the SINGLE remaining voter (`cluster retire` to N=1 first — de-clustering with live peers tears the route mesh)")
	cmd.Flags().BoolVar(&resetJS, "reset-js", false, "with --to-standalone: acknowledge resetting a NON-EMPTY clustered JetStream store to standalone (history/audit loss; MOVED aside to <store>.standalone-bak.<ts>, NEVER deleted) — NATS cannot migrate a clustered JS store in place")
	return cmd
}

// runReconcileNatsAuto blocks (when wait) until every voter's observed topology generation reaches the
// desired one. It NEVER bumps a generation (a raft write + a spurious DEGRADED flap) — the reconcilers
// converge on their own; this just polls the authoritative status.
//
// R11 P6/#54 facet 1: topology-generation convergence is NOT the whole story. A rotated account.nk
// (or broker.nk) whose new identity has not been re-rendered into nats.conf is an auth_callout skew
// this poll is structurally blind to — the topology generation can be perfectly converged while the
// on-disk issuer disagrees with the rendered one. The rotation runbook (§2.1) names THIS command as
// its re-render verb, so a "converged" print here was a false all-clear on the exact command an
// operator runs to VERIFY a credential rotation. It now fails-closed on that skew (BEFORE and AFTER
// the topology poll) instead of printing converged.
func runReconcileNatsAuto(cmd *cobra.Command, socket, secretsDir, confPath string, wait bool, timeout time.Duration) error {
	// Fail-closed on a local auth_callout identity skew regardless of --wait: even the non-wait
	// message below ("converges automatically") is a form of all-clear an operator must not get on a
	// rotated-but-not-re-rendered issuer.
	if err := clusterAuthIssuerSkewError(secretsDir, confPath); err != nil {
		return err
	}
	// N-3: a KNOWN skew fails closed above; when the issuer could not be CROSS-CHECKED at all (conf
	// unreadable / no auth_callout issuer rendered yet), never print a bare all-clear — WARN that the
	// convergence report does not confirm the rendered issuer matches account.nk. Computed ONCE: the
	// conf's readability is static across the --wait loop, so this avoids the repeated nkey-derive +
	// Preflight of re-reading it per all-clear branch (external review N-3 nit).
	unverifiedReason := clusterIssuerUnverifiedReason(secretsDir, confPath)
	warnIssuerUnverified := func() {
		if unverifiedReason != "" {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: auth_callout issuer verification SKIPPED (%s) — convergence below "+
				"does not confirm the rendered issuer matches this host's account.nk; pass --conf <path> or run `tether cluster doctor`.\n", unverifiedReason)
		}
	}
	if !wait {
		warnIssuerUnverified()
		_, _ = fmt.Fprintln(cmd.OutOrStdout(),
			"each broker's topology reconciler converges automatically (≤5s after a membership change); pass --wait to block until converged.")
		return nil
	}
	var lastLaggards []string
	// Batch-A A3: this used to be a bare `time.Sleep` loop that never read
	// cmd.Context() (so Ctrl-C did nothing) and whose timeout returned a plain
	// error — exit 70, i.e. "a tether bug, retry it" per docs/usage.md §9.13.
	// A wait that ran out is EX_TEMPFAIL like every other one.
	perr := pollUntilStep(cmd.Context(), 2*time.Second, time.Now().Add(timeout), func() (bool, error) {
		rep, err := fetchClusterStatusReport(socket)
		if err != nil {
			return false, err
		}
		// C3-m11: nothing is being managed yet → there is no convergence to wait for.
		if rep.TopoDesired == 0 {
			// L-2 (external review): this is the THIRD all-clear form under --wait; like the other two it
			// must not print a bare all-clear when the issuer could not be cross-checked.
			warnIssuerUnverified()
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "no topology generation is being managed yet (nothing to converge).")
			return true, nil
		}
		laggards := topoLaggards(rep)
		if len(laggards) == 0 {
			// Topology generation converged — but re-check the issuer skew before declaring
			// all-clear (the seed could have been rotated during the wait). A skew here is a
			// non-zero exit, not a "converged" print.
			if err := clusterAuthIssuerSkewError(secretsDir, confPath); err != nil {
				return false, err
			}
			// N-3: keep the leading "all voters converged" substring (drills grep it), but append the
			// SKIPPED qualifier + a stderr warning when the issuer could not be cross-checked, so the
			// converged print is never read as "issuer verified".
			if unverifiedReason != "" {
				warnIssuerUnverified()
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "all voters converged to topology generation %d (issuer verification SKIPPED — see warning above).\n", rep.TopoDesired)
				return true, nil
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "all voters converged to topology generation %d.\n", rep.TopoDesired)
			return true, nil
		}
		lastLaggards = laggards
		return false, nil
	})
	if errors.Is(perr, errPollDeadline) {
		return &ExitError{Class: exitTransient, Err: fmt.Errorf(
			"reconcile nats: timed out after %s; not converged: %s", timeout, strings.Join(lastLaggards, ", "))}
	}
	return perr
}

// topoLaggards returns the voters not yet converged to the desired generation — INCLUDING unreachable
// voters and voters that do not report topology (C3-M2: a voter that is DEGRADED in `cluster status`
// must NOT be reported as converged here, or `--wait` would exit 0 on a false all-clear). Each is
// rendered with its state for a no-false-green wait.
func topoLaggards(rep *adminsock.ClusterStatusReport) []string {
	if rep == nil || rep.TopoDesired == 0 {
		return nil
	}
	var out []string
	for _, n := range rep.Nodes {
		if n.Phase != "VOTER" {
			continue
		}
		switch {
		case !n.Reachable:
			out = append(out, fmt.Sprintf("%s(UNREACHABLE)", n.NodeID))
		case !n.TopoReported:
			out = append(out, fmt.Sprintf("%s(not-reporting-topology)", n.NodeID))
		case n.TopoObserved < rep.TopoDesired || n.TopoReconcileReason != "":
			out = append(out, fmt.Sprintf("%s(%d/%d→%d %s)", n.NodeID, n.TopoApplied, n.TopoObserved, rep.TopoDesired, n.TopoReconcileReason))
		}
	}
	return out
}
