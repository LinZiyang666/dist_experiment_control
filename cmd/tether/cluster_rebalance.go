package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// cluster_rebalance.go (C-rebalance) — `tether cluster rebalance proxy`: even out the per-node __proxy__
// homes across the eligible voters. The reaper only rehomes on a home DOWN; this operator pass migrates
// HEALTHY proxies off an over-loaded voter onto an under-loaded one (e.g. after `cluster add` brings a
// fresh empty voter, or a drained broker rejoins). Leader-local; `--dry-run` previews the moves.
func newClusterRebalanceCmd(socketPath *string) *cobra.Command {
	root := &cobra.Command{
		Use:   "rebalance",
		Short: "Even out cluster load (proxy homes across voters)",
		Long: `tether cluster rebalance — spread load evenly across the cluster's voters.

Today it covers proxy homes: ` + "`cluster rebalance proxy`" + ` reassigns the per-node
__proxy__ homes so each eligible voter carries roughly the same number, then re-pushes
the directive so each agent re-establishes on its new home. The auto-rehome reaper only
moves a home when it goes DOWN — run this after adding a broker (or after one rejoins)
to fill the new capacity. Runs on the leader; ` + "`--dry-run`" + ` previews without moving.`,
		Example: "  tether cluster rebalance proxy --dry-run   # what would move?\n" +
			"  tether cluster rebalance proxy             # spread __proxy__ homes evenly",
	}
	root.AddCommand(newClusterRebalanceProxyCmd(socketPath))
	return root
}

func newClusterRebalanceProxyCmd(socketPath *string) *cobra.Command {
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "Spread __proxy__ homes evenly across eligible voters",
		Long: `Reassign the per-node __proxy__ homes so each eligible (deliverable + reachable)
voter carries roughly the same count (max−min ≤ 1). Each move briefly drops that proxy's
public listener while the agent re-establishes on the new home — self-healing, but mildly
disruptive, so preview with --dry-run first if you care about the exact set.`,
		Example: "  tether cluster rebalance proxy --dry-run   # what would move?\n" +
			"  tether cluster rebalance proxy             # do it",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterRebalanceProxy, DryRun: dryRun})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("rebalance proxy", resp)
			}
			return printProxyRebalance(cmd, resp.ProxyRebalance)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the moves without executing them")
	return cmd
}

// printProxyRebalance renders the rebalance report (the single place the dry-run vs executed wording
// lives). It is honest about a partial pass: the header counts the PLANNED moves, each line shows
// ok/FAILED/(dry-run), and a pass that stopped early (a leadership loss broke it) is called out so the
// operator re-runs. Any failure OR un-attempted move ⇒ exitTransient (re-running is idempotent).
func printProxyRebalance(cmd *cobra.Command, rep *adminsock.ProxyRebalanceReport) error {
	out := cmd.OutOrStdout()
	if rep == nil {
		_, _ = fmt.Fprintln(out, "rebalance proxy: nothing to do.")
		return nil
	}
	if rep.Voters < 2 {
		_, _ = fmt.Fprintf(out, "rebalance proxy: only %d eligible voter(s) — need ≥2 to spread proxy homes; nothing to do.\n", rep.Voters)
		return nil
	}
	if rep.Planned == 0 {
		_, _ = fmt.Fprintf(out, "proxy homes already balanced: %d ready proxy home(s) across %d eligible voter(s) — nothing to move.\n",
			rep.Proxies, rep.Voters)
		return nil
	}
	verb := "moving"
	if rep.DryRun {
		verb = "would move"
	}
	_, _ = fmt.Fprintf(out, "rebalance proxy (%s %d of %d ready proxy home(s) across %d eligible voter(s)):\n",
		verb, rep.Planned, rep.Proxies, rep.Voters)
	var failed int
	for _, m := range rep.Moves {
		status := "ok"
		switch {
		case m.Error != "":
			status = "FAILED: " + m.Error
			failed++
		case rep.DryRun:
			status = "(dry-run)"
		}
		_, _ = fmt.Fprintf(out, "  %s/%s :%d  %s -> %s  [%s]\n", m.SID, m.NID, m.Port, m.From, m.To, status)
	}
	unattempted := rep.Planned - len(rep.Moves)
	if unattempted > 0 {
		_, _ = fmt.Fprintf(out, "  ... stopped early: %d planned move(s) NOT attempted (lost leadership?) — re-run to finish.\n", unattempted)
	}
	if failed > 0 || unattempted > 0 {
		// A failure/stop is almost always a lost-leadership / no-quorum transition mid-pass; surface it as
		// a transient so a converge script retries once HA is restored (re-running is idempotent).
		return &ExitError{Class: exitTransient, Err: fmt.Errorf("rebalance incomplete (%d failed, %d not attempted) — retry once the cluster is HEALTHY-HA", failed, unattempted)}
	}
	return nil
}
