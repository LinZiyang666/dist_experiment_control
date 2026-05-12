package main

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
)

// defaultAdminSocket matches architecture A.3 / I.2b. Operators can
// override via --socket; the default works for the install.sh-shaped
// production deployment.
const defaultAdminSocket = "/var/run/tether/admin.sock"

func newAdminCmd() *cobra.Command {
	var socketPath string

	root := &cobra.Command{
		Use:   "admin",
		Short: "Administrative subcommands (broker-local Unix socket)",
		Long: `Admin commands talk to the broker's local /var/run/tether/admin.sock
(architecture I.2b). They MUST be run on the broker host as a user
with read+write access to the socket file (mode 0600 — typically the
'tether' system user or root).`,
	}
	root.PersistentFlags().StringVar(&socketPath, "socket", defaultAdminSocket,
		"path to the broker admin Unix socket")

	root.AddCommand(newAdminSessionsCmd(&socketPath))
	root.AddCommand(newAdminNodesCmd(&socketPath))
	root.AddCommand(newAdminAuditCmd(&socketPath))
	root.AddCommand(newAdminEvictCmd(&socketPath))
	return root
}

func newAdminSessionsCmd(socketPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "sessions",
		Short: "List all sessions in the broker SQLite",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpSessions})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "SID\tNAME\tSTATE\tOWNER\tCREATED")
			for _, s := range resp.Sessions {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					s.SID, s.Name, s.State, shortFP(s.OwnerFP), s.CreatedAt.Format(time.RFC3339))
			}
			if len(resp.Sessions) == 0 {
				_, _ = fmt.Fprintln(tw, "(no sessions)")
			}
			return tw.Flush()
		},
	}
}

func newAdminNodesCmd(socketPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List all nodes (sid, nid, status, heartbeat age, version)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpNodes})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "SESSION\tNODE\tSTATE\tHEARTBEAT\tPROTO\tRELEASE")
			now := time.Now()
			for _, n := range resp.Nodes {
				age := "-"
				if !n.LastHeartbeatAt.IsZero() {
					age = humaneAge(now.Sub(n.LastHeartbeatAt))
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
					n.SID, n.NID, n.Status, age, n.ProtoVersion, n.ReleaseVersion)
			}
			if len(resp.Nodes) == 0 {
				_, _ = fmt.Fprintln(tw, "(no nodes registered)")
			}
			return tw.Flush()
		},
	}
}

func newAdminAuditCmd(socketPath *string) *cobra.Command {
	var n int
	cmd := &cobra.Command{
		Use:   "audit <sid>",
		Short: "Tail the per-session audit history (last N entries)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op:  adminsock.OpAudit,
				SID: args[0],
				N:   n,
			})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			for _, e := range resp.Audit {
				body, _ := json.Marshal(e.Body)
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"#%d  %s  %s  %s\n",
					e.Seq, e.Ts.Format(time.RFC3339), e.Subject, string(body))
			}
			if len(resp.Audit) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(no audit entries)")
			}
			return nil
		},
	}
	cmd.Flags().IntVarP(&n, "n", "n", 50, "number of entries to tail (most recent)")
	return cmd
}

func newAdminEvictCmd(socketPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "evict <sid> <nid>",
		Short: "Remove an agent's provisioning + node row, broadcast eviction",
		Long: `Architecture P9: deletes agent_provisioning row + nodes row, then
broadcasts sys.events{type:agent_evicted}. A live agent subscribed
to sys.events shuts down within ~1s; an offline agent will be
denied at next CONNECT (no longer provisioned).`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op:  adminsock.OpEvict,
				SID: args[0],
				NID: args[1],
			})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			r := resp.Evict
			if r == nil {
				return fmt.Errorf("broker: evict reply missing")
			}
			// When neither row was present, evict is a no-op. Surface that
			// distinctly so operators don't mistake "(false false false)"
			// for a partial success.
			if !r.NodeRowDeleted && !r.AgentProvDeleted {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"nothing to evict: nid=%s not bound to sid=%s (no node row, no provisioning row)\n",
					r.NID, r.SID)
				return fmt.Errorf("not_found")
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"evicted sid=%s nid=%s (node=%v provisioning=%v broadcast=%v)\n",
				r.SID, r.NID, r.NodeRowDeleted, r.AgentProvDeleted, r.BroadcastedEvicted)
			return nil
		},
	}
}

func callAdmin(socketPath string, req adminsock.Request) (*adminsock.Response, error) {
	c := &adminsock.Client{Path: socketPath}
	return c.Call(req)
}

// shortFP truncates a SHA256 fp prefix for column display.
func shortFP(fp string) string {
	if len(fp) <= 16 {
		return fp
	}
	return fp[:16] + "…"
}

func humaneAge(d time.Duration) string {
	switch {
	case d < time.Second:
		return "<1s"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}
