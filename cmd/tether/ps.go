package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newPsCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		showAll bool
	)
	cmd := &cobra.Command{
		Use:   "ps",
		Short: "List managed processes in the active session",
		Long: `tether ps — list processes AND exposed ports in the active session
(TETHER_SESSION env or current_session file). RUNNING-only by default;
pass -a to also show EXITED processes. Architecture F.8 — unified
view.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlPs(id.PublicKey, sid), []byte("{}"))
			if err != nil {
				return fmt.Errorf("ps: %w", err)
			}
			var resp proto.PsResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return err
			}
			if resp.Code != "" || resp.Error != "" {
				return errors.New("ps rejected: " + resp.Code + " " + resp.Error)
			}

			now := time.Now()
			out := cmd.OutOrStdout()

			_, _ = fmt.Fprintln(out, "PROCESSES")
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "  PID\tNODE\tSTATE\tEXIT\tSTARTED\tCMD")
			anyProc := false
			for _, p := range resp.Processes {
				if p.Status != "RUNNING" && !showAll {
					continue
				}
				anyProc = true
				exit := "-"
				if p.Status == "EXITED" {
					exit = fmt.Sprintf("%d", p.ExitCode)
				}
				_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\n",
					p.PID, p.NID, p.Status, exit,
					humanizeAgo(now, p.StartedAt),
					argvToCmd(p.Argv))
			}
			if !anyProc {
				_, _ = fmt.Fprintln(tw, "  (none)")
			}
			if err := tw.Flush(); err != nil {
				return err
			}

			_, _ = fmt.Fprintln(out, "\nPORTS")
			tw2 := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw2, "  NAME\tNODE\tLOCAL\tPUBLIC\tSTATE\tCREATED")
			anyPort := false
			for _, p := range resp.Ports {
				if p.State != "ALLOCATED" && !showAll {
					continue
				}
				anyPort = true
				_, _ = fmt.Fprintf(tw2, "  %s\t%s\t:%d\t:%d\t%s\t%s\n",
					p.Name, p.NID, p.LocalPort, p.Port, p.State,
					humanizeAgo(now, p.CreatedAt))
			}
			if !anyPort {
				_, _ = fmt.Fprintln(tw2, "  (none)")
			}
			return tw2.Flush()
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVarP(&showAll, "all", "a", false, "include EXITED processes (default: only RUNNING)")
	return cmd
}

func humanizeAgo(now, then time.Time) string {
	d := now.Sub(then)
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

func argvToCmd(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	out := argv[0]
	for _, a := range argv[1:] {
		out += " " + a
	}
	return out
}
