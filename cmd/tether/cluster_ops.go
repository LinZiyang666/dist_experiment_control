package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
)

// cluster_ops.go — B7 DOC#2: `cluster ops ls` / `cluster ops show <node>`. A read-only view of
// membership operations derived from the replicated roster (the broker computes it off cluster_nodes;
// the CLI just renders). --json emits the stable schema (B2 taxonomy).

func newClusterOpsCmd(socketPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ops",
		Short: "Inspect membership operations (add / drain / retire) and their state",
		Example: "  tether cluster ops ls              # all in-flight / recent membership operations\n" +
			"  tether cluster ops show brk-b      # one node's op state + timeline + resume hint",
	}
	cmd.AddCommand(newClusterOpsLsCmd(socketPath), newClusterOpsShowCmd(socketPath))
	return cmd
}

func fetchClusterOps(socketPath, node string) ([]adminsock.ClusterOpEntry, error) {
	resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterOps, OpsNode: node})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, clusterAdminError("ops", resp)
	}
	return resp.Ops, nil
}

func newClusterOpsLsCmd(socketPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List membership operations across the cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ops, err := fetchClusterOps(*socketPath, "")
			if err != nil {
				return err
			}
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), struct {
					Schema        string                     `json:"schema"` // Audit UX-MAJOR-1: discriminator
					SchemaVersion int                        `json:"schema_version"`
					Ops           []adminsock.ClusterOpEntry `json:"ops"`
				}{"cluster_ops", 1, normOps(ops)})
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "NODE\tOP\tSTATE\tPHASE\tUPDATED\tLAST_ERROR")
			for _, o := range ops {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", o.NodeID, o.Kind, o.State, o.Phase, o.UpdatedAt, o.LastError)
			}
			_ = tw.Flush()
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable JSON schema")
	return cmd
}

func newClusterOpsShowCmd(socketPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "show <node-id>",
		Short: "Show one node's membership operation (state, timeline, resume hint)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ops, err := fetchClusterOps(*socketPath, args[0])
			if err != nil {
				return err
			}
			if len(ops) == 0 {
				return usageErr("no membership operation for node %q (not in the roster)", args[0])
			}
			o := ops[0]
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), struct {
					Schema        string                   `json:"schema"` // Audit UX-MAJOR-1: discriminator
					SchemaVersion int                      `json:"schema_version"`
					Op            adminsock.ClusterOpEntry `json:"op"`
				}{"cluster_op", 1, o})
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "node:       %s\n", o.NodeID)
			_, _ = fmt.Fprintf(out, "operation:  %s\n", o.Kind)
			_, _ = fmt.Fprintf(out, "state:      %s (phase %s)\n", o.State, o.Phase)
			_, _ = fmt.Fprintf(out, "started:    %s\n", o.StartedAt)
			_, _ = fmt.Fprintf(out, "updated:    %s\n", o.UpdatedAt)
			if o.LastError != "" {
				_, _ = fmt.Fprintf(out, "last_error: %s\n", o.LastError)
			}
			if o.Resume != "" {
				_, _ = fmt.Fprintf(out, "resume:     %s\n", o.Resume)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable JSON schema")
	return cmd
}

// normOps returns a non-nil slice so `ops: []` (not null) is emitted for an empty cluster.
func normOps(ops []adminsock.ClusterOpEntry) []adminsock.ClusterOpEntry {
	if ops == nil {
		return []adminsock.ClusterOpEntry{}
	}
	return ops
}
