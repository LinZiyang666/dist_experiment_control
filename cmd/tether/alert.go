package main

import (
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// newAlertCmd is the D8b (§10.1) user-facing alert interface: `tether alert ls` lists active
// alerts with their cluster-level ack, `tether alert ack <dedup_key>` records the single
// cluster-level ack (which suppresses only the inline ack-prompt, NOT the banner — severe
// alerts re-appear each new session). Both ride the member-reachable ctrl.by.<actor>.* RPCs.
func newAlertCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "alert",
		Short: "List / acknowledge cluster alerts",
	}
	root.AddCommand(newAlertLsCmd())
	root.AddCommand(newAlertAckCmd())
	return root
}

func newAlertLsCmd() *cobra.Command {
	var natsURL, home string
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List active cluster alerts (with cluster-level ack)",
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("alert ls", natsURL, err)
			}
			defer nc.Close()
			// Explicit operator command → STRICT (F4): a no-responder / timeout / malformed
			// reply is a real error, NOT a false "no active alerts".
			alerts, err := fetchAlertsStrict(nc, id.PublicKey)
			if err != nil {
				return err
			}
			printAlertLs(os.Stdout, alerts)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	return cmd
}

func newAlertAckCmd() *cobra.Command {
	var natsURL, home string
	cmd := &cobra.Command{
		Use:   "ack <dedup_key>",
		Short: "Acknowledge an alert (cluster-level; suppresses the inline prompt, not the banner)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("alert ack", natsURL, err)
			}
			defer nc.Close()
			reply, err := ackAlert(nc, id.PublicKey, args[0])
			if err != nil {
				return err // ackAlert already prefixes/describes (avoid the double "alert ack:" prefix)
			}
			fmt.Printf("%s\n", reply)
			fmt.Fprintln(os.Stderr, "note: a severe alert will re-appear in the banner each new session (ack suppresses only the inline prompt).")
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	return cmd
}
