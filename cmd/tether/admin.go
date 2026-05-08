package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/spf13/cobra"
)

func newAdminCmd() *cobra.Command {
	var dbPath string

	root := &cobra.Command{
		Use:   "admin",
		Short: "Administrative subcommands (P2: SQLite-direct; full admin socket lands in P9)",
	}
	root.PersistentFlags().StringVar(&dbPath, "db", "./tether.db",
		"SQLite database file (must match the path given to 'tether serve --db')")

	nodes := &cobra.Command{
		Use:   "nodes",
		Short: "List all known nodes (sid, nid, state, heartbeat age)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := storage.Open(dbPath)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			snaps, err := node.List(db)
			if err != nil {
				return err
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "SESSION\tNODE\tSTATE\tHEARTBEAT\tPROTO\tRELEASE")
			now := time.Now()
			for _, s := range snaps {
				age := "-"
				if !s.LastHeartbeatAt.IsZero() {
					age = humaneAge(now.Sub(s.LastHeartbeatAt))
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
					s.SID, s.NID, s.Status, age, s.ProtoVersion, s.ReleaseVersion)
			}
			if len(snaps) == 0 {
				_, _ = fmt.Fprintln(tw, "(no nodes registered)")
			}
			return tw.Flush()
		},
	}
	root.AddCommand(nodes)
	return root
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
