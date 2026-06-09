package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// newExposeCmd registers `tether expose` and `tether expose rm`.
//
// `tether expose <node> --local 8888 --name jupyter` allocates a public
// port from the broker's [14000-14999] band, plumbs the token to the
// agent, and prints the resulting URL ("http://<broker>:<port>").
// Architecture F.3 / F.4 walks through the wire flow.
//
// `tether expose rm --name jupyter` reverses it.
func newExposeCmd() *cobra.Command {
	var (
		natsURL    string
		home       string
		local      int
		name       string
		remotePort int
	)
	cmd := &cobra.Command{
		Use:   "expose <node>",
		Short: "Expose a local port on <node> through the broker (P6)",
		Long: `tether expose — open a TCP tunnel from a public broker port to a
local port on the named agent node. After the call returns, anyone with
network access to the broker can reach <local-port> on the agent at
"http://<broker>:<allocated-port>".

The broker assigns a port from its configured band (default 14000-14999)
and generates a one-time token. Tokens are persisted on the agent at
~/.tether/agent/<sid>/state.json so frpc auto-reconnects on agent
restart without a re-expose.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			nid := args[0]
			if name == "" {
				return fmt.Errorf("--name is required (logical proxy name; used by `expose rm`)")
			}
			if local <= 0 || local > 65535 {
				return fmt.Errorf("--local must be 1..65535, got %d", local)
			}
			// Soft floor/ceiling only: 0 = auto (omit the flag). The
			// band (e.g. 14000-14999) is broker config and can differ
			// per broker, so it is NOT checked here — an in-range but
			// out-of-band value round-trips to the broker and comes
			// back as port_out_of_band. This guard only catches values
			// that can't be a port at all.
			if remotePort < 0 || remotePort > 65535 {
				return fmt.Errorf("--remote-port must be 0 (auto) or 1..65535, got %d", remotePort)
			}

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("expose", natsURL, err)
			}
			defer nc.Close()

			body, err := json.Marshal(proto.ExposeReq{Name: name, LocalPort: local, RemotePort: remotePort})
			if err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()

			subj := proto.SubjCmdBy(sid, id.PublicKey, nid, "expose")
			respMsg, err := nc.RequestWithContext(ctx, subj, body)
			if err != nil {
				return fmt.Errorf("expose: request: %w (broker or agent unreachable on NATS)", err)
			}
			var resp proto.ExposeResp
			if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
				return fmt.Errorf("expose: malformed reply: %w (broker version skew?)", err)
			}
			if resp.Code != "" {
				return brokerErrorMessage("expose", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"exposed: http://%s:%d → %s:%d  (name=%s)\n",
				resp.PublicHost, resp.Port, nid, local, resp.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().IntVar(&local, "local", 0, "local port on the agent to expose (required)")
	cmd.Flags().StringVar(&name, "name", "", "logical proxy name (required; used by `expose rm`)")
	cmd.Flags().IntVar(&remotePort, "remote-port", 0, "request a specific public port from the broker's band (default: auto lowest-free)")
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		cctx := cli.NewCompletionContext(home, natsURL, c.Flags().Changed("nats-url"))
		t := cli.NewCompletionTransport(home, natsURL, c.Flags().Changed("nats-url"))
		defer t.Close()
		return cli.CompleteOnlineNodes(t, cctx, toComplete)
	}

	cmd.AddCommand(newExposeRmCmd())
	return cmd
}

func newExposeRmCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		name    string
	)
	cmd := &cobra.Command{
		Use:   "rm <node>",
		Short: "Free a previously-exposed port",
		Long: `tether expose rm <node> --name jupyter — reverse a prior expose. The
public port is immediately returned to the pool and the agent's frpc
proxy is dropped. Idempotent: removing a non-existent name returns an
error you can ignore in scripts.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			nid := args[0]
			if name == "" {
				return fmt.Errorf("--name is required")
			}

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("expose rm", natsURL, err)
			}
			defer nc.Close()

			body, err := json.Marshal(proto.ExposeRmReq{Name: name})
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			subj := proto.SubjCmdBy(sid, id.PublicKey, nid, "expose-rm")
			respMsg, err := nc.RequestWithContext(ctx, subj, body)
			if err != nil {
				return fmt.Errorf("expose rm: request: %w (broker or agent unreachable on NATS)", err)
			}
			var resp proto.ExposeRmResp
			if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
				return fmt.Errorf("expose rm: malformed reply: %w", err)
			}
			if !resp.OK {
				return brokerErrorMessage("expose rm", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"freed: %s on %s (port %d back in pool)\n", name, nid, resp.Port)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&name, "name", "", "logical proxy name to free (required)")
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		cctx := cli.NewCompletionContext(home, natsURL, c.Flags().Changed("nats-url"))
		t := cli.NewCompletionTransport(home, natsURL, c.Flags().Changed("nats-url"))
		defer t.Close()
		return cli.CompleteOnlineNodes(t, cctx, toComplete)
	}
	_ = cmd.RegisterFlagCompletionFunc("name", func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var nid string
		if len(args) > 0 {
			nid = args[0]
		}
		cctx := cli.NewCompletionContext(home, natsURL, c.Flags().Changed("nats-url"))
		t := cli.NewCompletionTransport(home, natsURL, c.Flags().Changed("nats-url"))
		defer t.Close()
		return cli.CompleteAllocatedExposeNames(t, cctx, nid, toComplete)
	})
	return cmd
}
