package main

import (
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/clusterspec"
	"github.com/spf13/cobra"
)

// cluster_apply.go — B7 AUTO#9: `cluster apply -f roster.yaml`. It reads the desired topology,
// diffs it against the LIVE roster (via the read-only OpClusterStatus), and PRINTS a quorum-safe,
// topologically-ordered convergence plan as the exact existing verbs. It is PLAN-ONLY — it never
// mutates (execution is a deferred managed-convergence increment; auto-add is code-forced out of
// scope because admission needs the joiner's PoP signature a YAML row cannot produce).

func newClusterApplyCmd(socketPath *string) *cobra.Command {
	var file string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "apply -f roster.yaml",
		Short: "Diff a desired roster.yaml against the live cluster and print the convergence plan (plan-only; never mutates)",
		Example: "  tether cluster apply -f roster.yaml          # what would it take to converge to this roster?\n" +
			"  tether cluster apply -f roster.yaml --json   # the ordered step list as JSON",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if file == "" {
				return usageErr("cluster apply requires -f <roster.yaml>")
			}
			data, err := os.ReadFile(file)
			if err != nil {
				return fmt.Errorf("cluster apply: read %s: %w", file, err)
			}
			spec, err := clusterspec.Parse(data)
			if err != nil {
				return usageErr("%v", err)
			}
			rep, err := fetchClusterStatusReport(*socketPath)
			if err != nil {
				return err
			}
			// External-review F6: a convergence plan is only as safe as the roster it diffs against.
			// Refuse to plan off a non-authoritative or incomplete status (a follower/minority/stale
			// view would yield drain/add/remove steps based on the wrong voter set). The operator must
			// re-run on the leader. This is fail-closed: the printed plan is the core copy-paste surface.
			if !rep.IsLeaderView {
				return usageErr("cluster apply needs the authoritative LEADER view of the roster; this status is a non-leader view (view_host=%s, leader=%s). Re-run `cluster apply` against the leader's admin socket.", rep.ViewHost, rep.LeaderID)
			}
			if rep.Partial || len(rep.Errors) > 0 {
				return unavailErr("cluster apply refuses a PARTIAL/errored status (the roster may be incomplete): %v. Resolve the status errors, then re-run.", rep.Errors)
			}
			live := make([]clusterspec.LiveNode, 0, len(rep.Nodes))
			for _, n := range rep.Nodes {
				live = append(live, clusterspec.LiveNode{NodeID: n.NodeID, Phase: n.Phase, Role: n.Role})
			}
			steps := clusterspec.Diff(spec, live, rep.LeaderID)

			if asJSON {
				return emitJSON(cmd.OutOrStdout(), struct {
					Schema        string             `json:"schema"` // Audit UX-MAJOR-1: (schema, schema_version) discriminator
					SchemaVersion int                `json:"schema_version"`
					Steps         []clusterspec.Step `json:"steps"`
				}{"cluster_apply_plan", 1, steps})
			}
			out := cmd.OutOrStdout()
			if len(steps) == 0 {
				_, _ = fmt.Fprintln(out, "cluster apply: the live cluster already matches roster.yaml — nothing to do.")
				return nil
			}
			_, _ = fmt.Fprintf(out, "cluster apply (PLAN ONLY — run these in order; nothing is executed):\n\n")
			for i, s := range steps {
				_, _ = fmt.Fprintf(out, "  %d. %s\n", i+1, s.Verb)
				if s.Reason != "" {
					_, _ = fmt.Fprintf(out, "     (%s)\n", s.Reason)
				}
			}
			_, _ = fmt.Fprintln(out, "\nAfter each `cluster join approve`, run `cluster reconcile nats --all --wait`. See docs/cluster-runbook.md §1.")
			return nil
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "desired roster.yaml")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the ordered step list as JSON")
	return cmd
}
