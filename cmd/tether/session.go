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
				return usageErr("--pin is required for session create")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameUnactivated))
			if err != nil {
				return connectError("session create", natsURL, err)
			}
			defer nc.Close()

			body, _ := json.Marshal(proto.SessionCreateReq{Name: args[0], PIN: pin})
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlSessionCreate(id.PublicKey), body)
			if err != nil {
				return unavailErr("session create: %w (broker unreachable on NATS)", err)
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
			// Persist the broker URL the operator just used so subsequent
			// ctl commands inherit it without --nats-url. Only persist
			// if the operator actually supplied one (don't pin the
			// stale cobra default of nats://127.0.0.1:4222 on a public
			// deployment).
			if cmd.Flags().Changed("nats-url") {
				if err := cli.WriteDefaultBrokerURL(home, natsURL); err != nil {
					return fmt.Errorf("write broker_url: %w", err)
				}
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"session %q created (owner=%s, broker=%s)\nactivated locally — also run:\n    export TETHER_SESSION=%s\n",
				resp.SID, resp.OwnerFP, natsURL, resp.SID)
			return nil
		},
	}
	create.Flags().StringVar(&pin, "pin", "", "PIN for new session (required)")

	var listJSON bool
	list := &cobra.Command{
		Use:   "ls",
		Short: "List sessions visible to me",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			// Listing uses the unactivated template — its pub allow already
			// includes session.list.req. No active session required.
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameUnactivated))
			if err != nil {
				return connectError("session list", natsURL, err)
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlSessionList(id.PublicKey), []byte("{}"))
			if err != nil {
				return unavailErr("session list: %w (broker unreachable on NATS)", err)
			}
			var resp proto.SessionListResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return err
			}
			if resp.Code != "" || resp.Error != "" {
				return brokerErrorMessage("session list", resp.Code, resp.Error)
			}

			active := cli.ReadCurrentSession(home)
			if listJSON {
				return emitJSON(cmd.OutOrStdout(), sessionLsJSON{Schema: "session_ls", SchemaVersion: 1, Sessions: normSlice(resp.Sessions), ActiveSID: active})
			}
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

	var rmAckAlerts bool
	rm := &cobra.Command{
		Use:   "rm <sid>",
		Short: "Tombstone session (owner-only; ACTIVE → DELETING; full delete in P7)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
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

			// D8b §10.4: client-synth severe gate. Inert at N=1 (no responder → no gate);
			// a real cluster with quorum_lost / force_single_active blocks unless --ack-alerts.
			if gerr := gateDestructive(nc, id.PublicKey, rmAckAlerts); gerr != nil {
				return gerr
			}

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
	rm.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		// session is a subcommand of a parent with persistent --nats-url / --home.
		// Closure vars natsURL / home are bound to those persistent flags.
		changed := c.Flags().Changed("nats-url") || (c.Parent() != nil && c.Parent().PersistentFlags().Changed("nats-url"))
		cctx := cli.NewCompletionContext(home, natsURL, changed)
		t := cli.NewCompletionTransport(home, natsURL, changed)
		defer t.Close()
		return cli.CompleteOwnedSessions(t, cctx, toComplete)
	}

	rm.Flags().BoolVar(&rmAckAlerts, "ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
	list.Flags().BoolVar(&listJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	root.AddCommand(create, list, rm)
	return root
}
