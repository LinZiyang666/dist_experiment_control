package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
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
`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()

			if sid == "" {
				fmt.Fprintf(out,
					"authenticated — pubkey=%s\n              fp=%s\n",
					id.PublicKey, id.Fingerprint)
				return nil
			}

			if pin != "" {
				nc, err := cli.ConnectNATSWithNkey(natsURL, id)
				if err != nil {
					return err
				}
				defer nc.Close()

				body, _ := json.Marshal(proto.SessionJoinReq{PIN: pin})
				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
				defer cancel()
				msg, err := nc.RequestWithContext(ctx,
					proto.SubjCtrlSessionJoin(id.PublicKey, sid), body)
				if err != nil {
					return fmt.Errorf("login: %w", err)
				}
				var resp proto.SessionJoinResp
				if err := json.Unmarshal(msg.Data, &resp); err != nil {
					return err
				}
				if !resp.OK {
					if resp.Code != "" {
						return fmt.Errorf("join rejected (%s): %s", resp.Code, resp.Error)
					}
					return errors.New(resp.Error)
				}
				role := "member"
				if resp.IsOwner {
					role = "owner"
				}
				fmt.Fprintf(out, "joined session %q as %s\n", sid, role)
			}

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
