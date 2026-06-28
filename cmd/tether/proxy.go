package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
)

// newProxyCmd builds the `tether proxy` command tree (P13): the owner-only
// per-session master switch (on/off), member-readable status, and the
// per-subscriber subscription token lifecycle (sub create/ls/revoke).
func newProxyCmd() *cobra.Command {
	var (
		natsURL string
		home    string
	)
	root := &cobra.Command{
		Use:   "proxy",
		Short: "Manage the session proxy subscription (owner-only switch + subscriptions)",
		Long: `tether proxy — turn the session's online agents into a self-hosted
Clash subscription. With the switch ON, every online agent runs an embedded
shadowsocks server reachable through the broker; subscribers import the
generated URL into Clash and egress through your agents.`,
	}
	root.PersistentFlags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	root.PersistentFlags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir (~/.tether)")

	root.AddCommand(
		newProxyOnCmd(&natsURL, &home),
		newProxyOffCmd(&natsURL, &home),
		newProxyStatusCmd(&natsURL, &home),
		newProxySubCmd(&natsURL, &home),
	)
	return root
}

// proxyRequest is the shared connect→request→decode path for the proxy ctrl
// subjects (all answered directly by the broker, none forwarded to an agent).
func proxyRequest(cmd *cobra.Command, natsURL, home, subjectVerb string, body []byte, out any) error {
	sid := cli.ReadCurrentSession(home)
	if sid == "" {
		return fmt.Errorf("no active session — run `tether login -s <sid>` first")
	}
	url := cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return err
	}
	nc, err := connectCtl(cmd, "proxy", home, url, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return err
	}
	defer nc.Close()

	var subj string
	switch subjectVerb {
	case "set":
		subj = proto.SubjCtrlProxySet(id.PublicKey, sid)
	case "status":
		subj = proto.SubjCtrlProxyStatus(id.PublicKey, sid)
	case "sub.create":
		subj = proto.SubjCtrlProxySubCreate(id.PublicKey, sid)
	case "sub.list":
		subj = proto.SubjCtrlProxySubList(id.PublicKey, sid)
	case "sub.revoke":
		subj = proto.SubjCtrlProxySubRevoke(id.PublicKey, sid)
	default:
		return fmt.Errorf("proxy: unknown verb %q", subjectVerb)
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		// No responders on a proxy subject means the broker is reachable but
		// has no proxy handlers — i.e. it predates P13 (M5 skew).
		if errors.Is(err, nats.ErrNoResponders) {
			return fmt.Errorf("proxy %s: the broker has no proxy support — it predates P13; upgrade tetherd (architecture J.3)", subjectVerb)
		}
		return fmt.Errorf("proxy %s: request: %w (broker unreachable on NATS)", subjectVerb, err)
	}
	return json.Unmarshal(resp.Data, out)
}

func newProxyOnCmd(natsURL, home *string) *cobra.Command {
	var yes bool
	var haPolicy string
	cmd := &cobra.Command{
		Use:   "on",
		Short: "Turn the proxy subscription ON for this session (owner-only)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// C5: validate the HA policy ctl-side so a typo fails fast (the broker + the 0016 CHECK also
			// reject it). Empty ⇒ keep the current/default (freeze-on-quorum-loss).
			if haPolicy != "" && haPolicy != "freeze-on-quorum-loss" && haPolicy != "disable-on-quorum-loss" {
				return fmt.Errorf("invalid --ha-policy %q (want freeze-on-quorum-loss or disable-on-quorum-loss)", haPolicy)
			}
			if !yes {
				if !confirmProxyOn(cmd) {
					return fmt.Errorf("aborted (pass --yes to skip the prompt)")
				}
			}
			body, _ := json.Marshal(proto.ProxySetReq{Enabled: true, HAPolicy: haPolicy})
			var resp proto.ProxySetResp
			if err := proxyRequest(cmd, *natsURL, *home, "set", body, &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("proxy on", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"proxy ON — pushed to %d online node(s). Create a subscription with `tether proxy sub create --name <who>`.\n",
				resp.AffectedNodes)
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the open-exit-node confirmation prompt")
	cmd.Flags().StringVar(&haPolicy, "ha-policy", "", "cluster HA policy when quorum is lost: freeze-on-quorum-loss (default, /sub keeps serving) | disable-on-quorum-loss (/sub 404s)")
	return cmd
}

func newProxyOffCmd(natsURL, home *string) *cobra.Command {
	return &cobra.Command{
		Use:   "off",
		Short: "Turn the proxy subscription OFF (stops every agent's SS server)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			body, _ := json.Marshal(proto.ProxySetReq{Enabled: false})
			var resp proto.ProxySetResp
			if err := proxyRequest(cmd, *natsURL, *home, "set", body, &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("proxy off", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "proxy OFF — agents stopped serving; subscription URLs now return no nodes.")
			return nil
		},
	}
}

func newProxyStatusCmd(natsURL, home *string) *cobra.Command {
	var asJSON bool
	var clusterView bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show proxy switch state, live nodes, and subscribers (member-readable)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			reqBody, _ := json.Marshal(proto.ProxyStatusReq{Cluster: clusterView})
			var resp proto.ProxyStatusResp
			if err := proxyRequest(cmd, *natsURL, *home, "status", reqBody, &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("proxy status", resp.Code, resp.Error)
			}
			if asJSON {
				b, _ := json.MarshalIndent(resp, "", "  ")
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
				return nil
			}
			w := cmd.OutOrStdout()
			state := "OFF"
			if resp.Enabled {
				state = "ON"
			}
			_, _ = fmt.Fprintf(w, "PROXY: %s\n", state)
			// C5 cluster view: the degraded state + HA policy + writability, then per-agent ready REASON.
			if resp.ClusterState != "" {
				_, _ = fmt.Fprintf(w, "CLUSTER: %s   ha-policy=%s   writable=%v\n", resp.ClusterState, resp.HAPolicy, resp.Writable)
				_, _ = fmt.Fprintf(w, "\nNODES\n  %-20s %-8s %-14s %-22s %s\n", "NID", "STATUS", "REASON", "HOME", "EXIT")
				for _, n := range resp.Nodes {
					exit := "-"
					if n.PublicPort != 0 {
						exit = fmt.Sprintf("%s:%d", n.PublicHost, n.PublicPort)
					}
					home := n.HomeBroker
					if home == "" {
						home = "-"
					}
					_, _ = fmt.Fprintf(w, "  %-20s %-8s %-14s %-22s %s\n", n.NID, n.Status, n.ReadyReason, home, exit)
				}
			} else {
				_, _ = fmt.Fprintf(w, "\nNODES\n  %-20s %-8s %-6s %s\n", "NID", "STATUS", "READY", "EXIT")
				for _, n := range resp.Nodes {
					exit := "-"
					if n.PublicPort != 0 {
						exit = fmt.Sprintf("%s:%d", n.PublicHost, n.PublicPort)
					}
					_, _ = fmt.Fprintf(w, "  %-20s %-8s %-6v %s\n", n.NID, n.Status, n.Ready, exit)
				}
			}
			_, _ = fmt.Fprintf(w, "\nSUBSCRIBERS\n  %-20s %-8s %s\n", "NAME", "STATE", "CREATED")
			for _, s := range resp.Subscribers {
				_, _ = fmt.Fprintf(w, "  %-20s %-8s %s\n", s.Name, s.State, s.CreatedAt.Format("2006-01-02 15:04"))
			}
			if resp.SubURLPrefix != "" {
				_, _ = fmt.Fprintf(w, "\nsubscription URL prefix: %s<token>\n", resp.SubURLPrefix)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit raw JSON")
	cmd.Flags().BoolVar(&clusterView, "cluster", false, "show the cluster-aware view: per-agent ready reason + home + the degraded state")
	return cmd
}

func newProxySubCmd(natsURL, home *string) *cobra.Command {
	root := &cobra.Command{
		Use:   "sub",
		Short: "Manage subscription tokens (create / ls / revoke; owner-only)",
	}
	root.AddCommand(newProxySubCreateCmd(natsURL, home), newProxySubListCmd(natsURL, home), newProxySubRevokeCmd(natsURL, home))
	return root
}

func newProxySubCreateCmd(natsURL, home *string) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "create --name <who>",
		Short: "Mint a subscription URL for one subscriber (printed once)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			body, _ := json.Marshal(proto.ProxySubCreateReq{Name: name})
			var resp proto.ProxySubCreateResp
			if err := proxyRequest(cmd, *natsURL, *home, "sub.create", body, &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("proxy sub create", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"subscription created for %q:\n\n  %s\n\nSave it now — it is shown only once. Revoke with `tether proxy sub revoke %s`.\n",
				resp.Name, resp.SubURL, resp.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "subscriber label, e.g. alice (1..64 printable ASCII, no '/')")
	return cmd
}

func newProxySubListCmd(natsURL, home *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List subscribers (no tokens are ever shown)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var resp proto.ProxySubListResp
			if err := proxyRequest(cmd, *natsURL, *home, "sub.list", []byte("{}"), &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("proxy sub ls", resp.Code, resp.Error)
			}
			w := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(w, "%-20s %-8s %-18s %s\n", "NAME", "STATE", "CREATED", "REVOKED")
			for _, s := range resp.Subs {
				rev := "-"
				if s.RevokedAt != nil {
					rev = s.RevokedAt.Format("2006-01-02 15:04")
				}
				_, _ = fmt.Fprintf(w, "%-20s %-8s %-18s %s\n", s.Name, s.State, s.CreatedAt.Format("2006-01-02 15:04"), rev)
			}
			return nil
		},
	}
	return cmd
}

func newProxySubRevokeCmd(natsURL, home *string) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "revoke <name>",
		Short: "Revoke one subscriber (others keep working; in-flight conns are cut)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("subscriber name required (positional or --name)")
			}
			body, _ := json.Marshal(proto.ProxySubRevokeReq{Name: name})
			var resp proto.ProxySubRevokeResp
			if err := proxyRequest(cmd, *natsURL, *home, "sub.revoke", body, &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("proxy sub revoke", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "revoked subscription %q.\n", resp.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "subscriber label to revoke")
	return cmd
}

// confirmProxyOn prints the open-exit-node liability warning and reads a y/N
// answer from the terminal. Non-interactive stdin returns false (the caller
// then tells the user to pass --yes).
func confirmProxyOn(cmd *cobra.Command) bool {
	w := cmd.ErrOrStderr()
	_, _ = fmt.Fprintln(w, "WARNING: turning the proxy ON makes EVERY online agent an open internet")
	_, _ = fmt.Fprintln(w, "exit node for ANYONE holding a subscription link (including non-members).")
	_, _ = fmt.Fprintln(w, "Their traffic egresses from your agents' IPs and you are responsible for it.")
	_, _ = fmt.Fprintln(w, "Egress is internet-only by default: agents refuse subscription traffic to their")
	_, _ = fmt.Fprintln(w, "loopback, private/LAN, link-local, and cloud-metadata addresses. Enable private")
	_, _ = fmt.Fprintln(w, "access only via agent.yaml (proxy.allow_private_destinations) and understand it")
	_, _ = fmt.Fprintln(w, "grants subscription holders lateral reach into those networks.")
	_, _ = fmt.Fprintln(w, "Kill it any time with `tether proxy off`.")
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return false
	}
	_, _ = fmt.Fprint(w, "Proceed? [y/N]: ")
	var ans string
	_, _ = fmt.Fscanln(os.Stdin, &ans)
	return ans == "y" || ans == "Y" || ans == "yes"
}
