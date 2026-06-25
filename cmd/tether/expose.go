package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
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
		asJSON     bool
		noRebuild  bool
		onBroker   string
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
				return usageErr("--name is required (logical proxy name; used by `expose rm`)")
			}
			if local <= 0 || local > 65535 {
				return usageErr("--local must be 1..65535, got %d", local)
			}
			// Soft floor/ceiling only: 0 = auto (omit the flag). The
			// band (e.g. 14000-14999) is broker config and can differ
			// per broker, so it is NOT checked here — an in-range but
			// out-of-band value round-trips to the broker and comes
			// back as port_out_of_band. This guard only catches values
			// that can't be a port at all.
			if remotePort < 0 || remotePort > 65535 {
				return usageErr("--remote-port must be 0 (auto) or 1..65535, got %d", remotePort)
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

			ackAlerts, _ := cmd.Flags().GetBool("ack-alerts")
			if gerr := gateDestructive(nc, id.PublicKey, ackAlerts); gerr != nil { // D8b §10.4
				return gerr
			}

			body, err := json.Marshal(proto.ExposeReq{Name: name, LocalPort: local, RemotePort: remotePort, RebuildOff: noRebuild, OnBroker: onBroker})
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
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), exposeJSON{Schema: "expose", SchemaVersion: 1, PublicHost: resp.PublicHost, Port: resp.Port, Name: resp.Name, Node: nid, HomeBroker: resp.HomeBroker, Epoch: resp.Epoch, RebuildOff: noRebuild})
			}
			line := fmt.Sprintf("exposed: http://%s:%d → %s:%d  (name=%s)", resp.PublicHost, resp.Port, nid, local, resp.Name)
			if resp.HomeBroker != "" {
				line += "  home=" + resp.HomeBroker
				if resp.Epoch > 0 {
					line += " (moved)"
				}
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
			// B4: --no-rebuild on a single broker is a no-op (no other broker to move to). The
			// broker leaves HomeBroker empty in that case; tell the operator honestly rather than
			// let them believe they pinned something.
			if noRebuild && resp.HomeBroker == "" {
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "note: --no-rebuild has no effect on a single broker (no other broker to move to).")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().IntVar(&local, "local", 0, "local port on the agent to expose (required)")
	cmd.Flags().StringVar(&name, "name", "", "logical proxy name (required; used by `expose rm`)")
	cmd.Flags().IntVar(&remotePort, "remote-port", 0, "request a specific public port from the broker's band (default: auto lowest-free)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	cmd.Flags().BoolVar(&noRebuild, "no-rebuild", false,
		"pin this expose to its home broker — do NOT auto-move it to a survivor if the home dies "+
			"(default: it auto-rehomes; with --no-rebuild it goes DOWN with its home and `cluster drain` refuses to migrate it). "+
			"Cluster only; silently ignored by a single/old broker.")
	cmd.Flags().StringVar(&onBroker, "on-broker", "",
		"pin the expose's home to a named cluster node (cluster mode only; default: the agent's connected broker)")
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
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
	cmd.AddCommand(newExposeExplainCmd())
	return cmd
}

// newExposeExplainCmd implements `tether expose explain <name>` (B4): a read-only, member-safe
// view of ONE expose's home/epoch/rebuild policy. It rides the EXISTING `ps` RPC (no new subject,
// no new proto type, no ACL change) and filters the full unfiltered port set by name, so it can
// explain a REVOKED / rehomed / FREED expose, not just an ALLOCATED one. It prints only fields
// that are actually recorded today; the deferred observability fields are named in a footer note
// rather than shown as empty placeholders.
func newExposeExplainCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		asJSON  bool
	)
	cmd := &cobra.Command{
		Use:   "explain <name>",
		Short: "Show where an expose is homed + its rebuild policy",
		Long: `tether expose explain <name> — explain one exposed port: its node, state, public
port, home broker (cluster), reassign epoch ("moved" if it has failed over), and rebuild
policy. Reads the same data as ` + "`tether ps`" + `; member-safe (no operator socket).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wantName := args[0]
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("expose explain", natsURL, err)
			}
			defer nc.Close()

			body, err := json.Marshal(proto.PsReq{IncludeExited: true})
			if err != nil {
				return fmt.Errorf("expose explain: marshal req: %w", err)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), psRequestTimeout())
			defer cancel()
			msg, err := nc.RequestWithContext(ctx, proto.SubjCtrlPs(id.PublicKey, sid), body)
			if err != nil {
				return fmt.Errorf("expose explain: request: %w (broker unreachable on NATS)", err)
			}
			var resp proto.PsResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return fmt.Errorf("expose explain: malformed reply: %w", err)
			}
			if resp.Code != "" {
				return brokerErrorMessage("expose explain", resp.Code, resp.Error)
			}
			p, ok := pickExposeByName(resp.Ports, wantName)
			if !ok {
				// J1 (Stage-C): a missing name is a successful round-trip to a healthy broker — the
				// resource just doesn't exist. That is exit 64 (usage / operator-action), NOT 69
				// (broker unreachable), matching node_not_found / session_not_found convention.
				return usageErr("expose explain: no expose named %q in this session (see `tether ps -a`)", wantName)
			}
			moved := p.Epoch > 0
			rebuild := !p.RebuildOff
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), exposeExplainJSON{
					Schema: "expose_explain", SchemaVersion: 1, Name: p.Name, Node: p.NID,
					LocalPort: p.LocalPort, PublicPort: p.Port, State: p.State, Rebuild: rebuild,
					HomeBroker: p.HomeBroker, Epoch: p.Epoch, Moved: moved, CreatedByFP: p.CreatedByFP,
				})
			}
			renderExposeExplain(cmd.OutOrStdout(), p, rebuild, moved)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

// pickExposeByName finds the expose to explain among all states. It prefers the live ALLOCATED
// row (the one that matters now); if none is ALLOCATED it returns the most recently created
// matching row (so explaining a just-freed name still shows its last state).
func pickExposeByName(ports []proto.PsPortEntry, name string) (proto.PsPortEntry, bool) {
	var best proto.PsPortEntry
	found := false
	for _, p := range ports {
		if p.Name != name {
			continue
		}
		switch {
		case p.State == "ALLOCATED":
			return p, true // the live row wins outright
		case !found || p.CreatedAt.After(best.CreatedAt):
			best, found = p, true
		}
	}
	return best, found
}

func renderExposeExplain(w io.Writer, p proto.PsPortEntry, rebuild, moved bool) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "expose:\t%s\n", p.Name)
	_, _ = fmt.Fprintf(tw, "  node:\t%s\n", p.NID)
	_, _ = fmt.Fprintf(tw, "  state:\t%s\n", p.State)
	_, _ = fmt.Fprintf(tw, "  local port:\t%d\n", p.LocalPort)
	_, _ = fmt.Fprintf(tw, "  public port:\t%d  (combine with your broker host for the URL)\n", p.Port)
	home := "(single broker / un-homed)"
	if p.HomeBroker != "" {
		home = p.HomeBroker
		if moved {
			home += fmt.Sprintf(" (moved, epoch %d)", p.Epoch)
		}
	}
	_, _ = fmt.Fprintf(tw, "  home broker:\t%s\n", home)
	if rebuild {
		_, _ = fmt.Fprintf(tw, "  rebuild:\ton — auto-rehomes to a survivor broker if the home dies\n")
	} else {
		_, _ = fmt.Fprintf(tw, "  rebuild:\toff — --no-rebuild: pinned to its home; if the home dies this expose goes DOWN and `cluster drain` will refuse to migrate it\n")
	}
	if p.CreatedByFP != "" {
		_, _ = fmt.Fprintf(tw, "  created by:\t%s\n", p.CreatedByFP)
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintln(w, "# rehome events / last_error / reconnects / ready_reason: not yet recorded (planned, B5)")
}

func newExposeRmCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		name    string
		asJSON  bool
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
				return usageErr("--name is required")
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

			ackAlerts, _ := cmd.Flags().GetBool("ack-alerts")
			if gerr := gateDestructive(nc, id.PublicKey, ackAlerts); gerr != nil { // D8b §10.4
				return gerr
			}

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
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), exposeRmJSON{Schema: "expose_rm", SchemaVersion: 1, Name: name, Node: nid, Port: resp.Port})
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"freed: %s on %s (port %d back in pool)\n", name, nid, resp.Port)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&name, "name", "", "logical proxy name to free (required)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
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
