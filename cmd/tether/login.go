package main

import (
	"fmt"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newLoginCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		sid     string
		pin     string
	)
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Authenticate (and optionally activate a session)",
		Long: `tether login — load/generate the user nkey identity and optionally
activate a session.

  tether login                       # nkey only, no session activation
  tether login -s <sid>              # activate <sid> (caller must already be a member)
  tether login -s <sid> --pin <pin>  # first-time PIN join + activate

Activation persists to ~/.tether/current_session and is also displayed as
'export TETHER_SESSION=<sid>' so the user can copy/paste into their shell.

Membership / PIN are verified server-side via NATS auth_callout — login -s
performs a real NATS CONNECT and only writes current_session on success.
`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()

			if sid == "" {
				// Pure auth, no activation.
				nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameUnactivated))
				if err != nil {
					return fmt.Errorf("login: %w", err)
				}
				nc.Close()
				fmt.Fprintf(out,
					"authenticated — pubkey=%s\n              fp=%s\n",
					id.PublicKey, id.Fingerprint)
				return nil
			}

			// Activate sid. CONNECT triggers auth_callout, which:
			//   - if pin != "" : verify PIN, add to members, allow connect
			//   - if pin == "" : check membership, allow only if already a member
			//   - else (deleting / unknown sid / wrong pin) : reject CONNECT
			//
			// nats.Connect surfaces the rejection as an error. We do NOT
			// write current_session unless CONNECT succeeded.
			opts := []nats.Option{nats.Name(cli.CtlNameForSession(sid))}
			if pin != "" {
				opts = append(opts, nats.Token(pin))
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, opts...)
			if err != nil {
				return fmt.Errorf("login: activation refused for session %q: %w", sid, err)
			}
			nc.Close()

			if err := cli.WriteCurrentSession(home, sid); err != nil {
				return fmt.Errorf("write current_session: %w", err)
			}
			fmt.Fprintf(out,
				"activated session %q\n  also run: export TETHER_SESSION=%s\n",
				sid, sid)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVarP(&sid, "session", "s", "", "session to activate")
	cmd.Flags().StringVar(&pin, "pin", "", "PIN for first-time join (only with -s)")
	return cmd
}

func newLogoutCmd() *cobra.Command {
	var home string
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Clear active session (does not unregister from broker)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cli.WriteCurrentSession(home, ""); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(),
				"current session cleared (TETHER_SESSION env, if set, still wins)")
			return nil
		},
	}
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	return cmd
}

func newCtxCmd() *cobra.Command {
	var home string
	cmd := &cobra.Command{
		Use:   "ctx",
		Short: "Print currently active session (empty if none)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if s := cli.ReadCurrentSession(home); s != "" {
				fmt.Fprintln(cmd.OutOrStdout(), s)
			}
		},
	}
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	return cmd
}
