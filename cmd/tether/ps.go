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
		Long: `tether ps — list processes in the active session (TETHER_SESSION env
or current_session file). RUNNING by default; pass -a to also show
EXITED rows.

P4 scope: processes only. `+"`expose`-allocated ports show up under "+
			"`tether ps` together with processes once P6 lands.",
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

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "PID\tNODE\tSTATE\tEXIT\tSTARTED\tCMD")
			now := time.Now()
			for _, p := range resp.Processes {
				if p.Status != "RUNNING" && !showAll {
					continue
				}
				exit := "-"
				if p.Status == "EXITED" {
					exit = fmt.Sprintf("%d", p.ExitCode)
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
					p.PID, p.NID, p.Status, exit,
					humanizeAgo(now, p.StartedAt),
					argvToCmd(p.Argv))
			}
			return tw.Flush()
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
