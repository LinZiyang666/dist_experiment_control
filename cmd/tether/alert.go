package main

import (
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/adminsock"
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
	root.AddCommand(newAlertRaiseCmd())
	root.AddCommand(newAlertClearCmd())
	return root
}

// newAlertRaiseCmd is the B4 operator verb `tether alert raise`. Unlike ls/ack (member NATS
// RPCs), it talks to the broker's LOCAL admin socket (operator trust tier) and writes the alert
// through raft on the leader. Manual kind only.
func newAlertRaiseCmd() *cobra.Command {
	var socketPath, kind, severity, message, label string
	cmd := &cobra.Command{
		Use:   "raise",
		Short: "Raise a manual cluster alert (operator-only; via the broker admin socket)",
		Long: `tether alert raise — raise a manual, Raft-replicated cluster alert from the broker
host. Operator-only: it runs on the broker via the admin socket (not a session), so it needs
no NATS identity. The alert appears cluster-wide in ` + "`tether alert ls`" + ` and (if severe) in
the always-on ps/node banner until an operator clears it. Cluster mode only.`,
		Example: "  tether alert raise --severity severe --message \"maintenance 02:00-03:00 UTC\"\n" +
			"  tether alert raise --severity info --message \"brk-b rebooting\" --label brk-b",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if kind != "manual" {
				return usageErr("--kind must be \"manual\" (system alert kinds are raised automatically; see `tether alert ls`)")
			}
			if severity != "info" && severity != "severe" {
				return usageErr("--severity must be \"info\" or \"severe\"")
			}
			if message == "" {
				return usageErr("--message is required")
			}
			resp, err := callAdmin(socketPath, adminsock.Request{
				Op: adminsock.OpClusterAlertRaise, AlertKind: kind,
				AlertSeverity: severity, AlertMessage: message, AlertLabel: label,
			})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return alertAdminError("alert raise", resp)
			}
			key := kind
			if resp.Alert != nil && resp.Alert.DedupKey != "" {
				key = resp.Alert.DedupKey
			}
			if resp.Alert != nil && resp.Alert.AlreadyActive {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"note: a manual alert is already active for key %q (not overwritten); clear it first (`tether alert clear %s`) or use --label\n", key, key)
				return nil
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"raised: manual (%s) %q — clear with: tether alert clear %s\n", severity, message, key)
			return nil
		},
	}
	cmd.Flags().StringVar(&socketPath, "socket", defaultAdminSocket, "broker admin Unix socket")
	cmd.Flags().StringVar(&kind, "kind", "manual", "alert kind (only \"manual\" is operator-raisable)")
	cmd.Flags().StringVar(&severity, "severity", "",
		"info | severe  (severe = shown in the always-on ps/node banner; it does NOT block writes — only quorum_lost / force_single_active hard-gate destructive ops)")
	cmd.Flags().StringVar(&message, "message", "", "human-readable alert text (required)")
	cmd.Flags().StringVar(&label, "label", "", "optional disambiguator → dedup key manual:<label> (default key: \"manual\")")
	return cmd
}

// newAlertClearCmd is the B4 operator verb `tether alert clear <dedup_key>`. Idempotent.
func newAlertClearCmd() *cobra.Command {
	var socketPath string
	cmd := &cobra.Command{
		Use:     "clear <dedup_key>",
		Short:   "Clear a manual cluster alert (operator-only; via the broker admin socket)",
		Example: "  tether alert clear manual\n  tether alert clear manual:brk-b",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterAlertClear, AlertDedupKey: args[0]})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return alertAdminError("alert clear", resp)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "cleared: %s (idempotent — OK whether or not it was active)\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&socketPath, "socket", defaultAdminSocket, "broker admin Unix socket")
	return cmd
}

// alertAdminError formats an operator-alert admin reply error. A single broker has no raft, so
// cluster_not_enabled gets a clearer message than the bare "cluster mode not enabled".
func alertAdminError(verb string, resp *adminsock.Response) error {
	if resp.Code == adminsock.CodeClusterNotEnabled {
		return unavailErr("%s requires a clustered broker (alerts are Raft-replicated); this broker is single-node", verb)
	}
	return clusterAdminError(verb, resp)
}

func newAlertLsCmd() *cobra.Command {
	var natsURL, home string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List active cluster alerts (with cluster-level ack)",
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "alert ls", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()
			// Explicit operator command → STRICT (F4): a no-responder / timeout / malformed
			// reply is a real error, NOT a false "no active alerts".
			alerts, err := fetchAlertsStrict(nc, id.PublicKey)
			if err != nil {
				return err
			}
			if asJSON {
				// Each AlertView carries dedup_key, so `alert ls --json | jq -r '.alerts[].dedup_key'`
				// round-trips into `alert ack`.
				return emitJSON(cmd.OutOrStdout(), alertLsJSON{Schema: "alert_ls", SchemaVersion: 1, Alerts: normSlice(alerts)})
			}
			printAlertLs(os.Stdout, alerts)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

func newAlertAckCmd() *cobra.Command {
	var natsURL, home string
	cmd := &cobra.Command{
		Use:   "ack <dedup_key>",
		Short: "Acknowledge an alert (cluster-level; suppresses the inline prompt, not the banner)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// B3 item 4: quorum_lost / force_single_active are LIVE client-synthesized cluster-
			// health conditions (proto.EvalDestructiveGate), NOT store-backed alerts with a
			// dedup_key. Short-circuit BEFORE any session read / NATS dial so it works on a
			// partitioned box at the moment quorum is lost.
			switch args[0] {
			case "quorum_lost", "force_single_active":
				return usageErr("%q is a LIVE cluster-health condition synthesized from the brokers right now, NOT a store-backed alert with a dedup_key.\n"+
					"  • You cannot ack it; `alert ack` is for store-backed team-coordination alerts only (see `alert ls`).\n"+
					"  • --ack-alerts does NOT ack or fix this either — it FORCES one command through while quorum is still lost (dangerous).\n"+
					"  • The only real fix is restoring quorum / recovering the cluster (docs/cluster-runbook.md §3).", args[0])
			}
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "alert ack", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
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
