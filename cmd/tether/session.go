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

func newSessionCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		pin     string
	)
	root := &cobra.Command{
		Use:   "session",
		Short: "Manage sessions (create / ls / rm). 'tether login' joins / activates.",
	}
	root.PersistentFlags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	root.PersistentFlags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir (~/.tether)")

	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create new session (auto-activates locally; caller becomes owner)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if pin == "" {
				return fmt.Errorf("--pin is required for session create")
			}
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameUnactivated))
			if err != nil {
				return err
			}
			defer nc.Close()

			body, _ := json.Marshal(proto.SessionCreateReq{Name: args[0], PIN: pin})
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlSessionCreate(id.PublicKey), body)
			if err != nil {
				return fmt.Errorf("session create: %w (broker unreachable on NATS)", err)
			}
			var resp proto.SessionCreateResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return err
			}
			if resp.Error != "" {
				// SessionCreateResp doesn't carry a separate Code; the
				// architecture-stable identifier is buried in Error
				// itself. Pass it through brokerErrorMessage anyway —
				// known prefixes like "session_already_exists" still
				// match the hint table.
				return brokerErrorMessage("session create", resp.Error, resp.Error)
			}
			if err := cli.WriteCurrentSession(home, resp.SID); err != nil {
				return fmt.Errorf("write current_session: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"session %q created (owner=%s)\nactivated locally — also run:\n    export TETHER_SESSION=%s\n",
				resp.SID, resp.OwnerFP, resp.SID)
			return nil
		},
	}
	create.Flags().StringVar(&pin, "pin", "", "PIN for new session (required)")

	list := &cobra.Command{
		Use:   "ls",
		Short: "List sessions visible to me",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			// Listing uses the unactivated template — its pub allow already
			// includes session.list.req. No active session required.
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameUnactivated))
			if err != nil {
				return err
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlSessionList(id.PublicKey), []byte("{}"))
			if err != nil {
				return fmt.Errorf("session list: %w (broker unreachable on NATS)", err)
			}
			var resp proto.SessionListResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return err
			}
			if resp.Code != "" || resp.Error != "" {
				return brokerErrorMessage("session list", resp.Code, resp.Error)
			}

			active := cli.ReadCurrentSession(home)
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ACTIVE\tSID\tNAME\tSTATE\tROLE\tCREATED")
			for _, s := range resp.Sessions {
				marker := " "
				if s.SID == active {
					marker = "*"
				}
				role := "member"
				if s.IsOwner {
					role = "owner"
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
					marker, s.SID, s.Name, s.State, role,
					s.CreatedAt.Format("2006-01-02 15:04"))
			}
			if len(resp.Sessions) == 0 {
				_, _ = fmt.Fprintln(tw, "(no sessions; create one with `tether session create <name> --pin <pin>`)")
			}
			return tw.Flush()
		},
	}

	rm := &cobra.Command{
		Use:   "rm <sid>",
		Short: "Tombstone session (owner-only; ACTIVE → DELETING; full delete in P7)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			// rm needs the activated-member template (it grants pub allow
			// for session.<sid>.rm.req). Require active session = arg sid.
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(args[0])))
			if err != nil {
				return connectError("session rm", natsURL, err)
			}
			defer nc.Close()

			body, _ := json.Marshal(proto.SessionRmReq{})
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlSessionRm(id.PublicKey, args[0]), body)
			if err != nil {
				return fmt.Errorf("session rm: request: %w (broker unreachable on NATS)", err)
			}
			var resp proto.SessionRmResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return err
			}
			if !resp.OK {
				if resp.Code != "" {
					return brokerErrorMessage("session rm", resp.Code, resp.Error)
				}
				return errors.New(resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "session %q tombstoned (state=DELETING)\n", args[0])
			return nil
		},
	}

	root.AddCommand(create, list, rm)
	return root
}
