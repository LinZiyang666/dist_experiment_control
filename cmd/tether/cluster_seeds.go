package main

import (
	"fmt"
	"strings"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/spf13/cobra"
)

// cluster_seeds.go (C2) — `tether cluster seeds publish|show`. Publish records the cluster's public
// client-dialable endpoints + the well-known manifest URL via raft (leader-only) and prints a
// ready-to-paste agent invite; show is a leader-agnostic read for scripting/doctor.

func newClusterSeedsCmd(socketPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "seeds",
		Short:   "Publish / show the cluster's public client-dialable seed endpoints (for `agent join`)",
		Example: "  tether cluster seeds publish --bootstrap https://c.example.com/.well-known/tether/cluster.json --endpoint wss://b1:443 --sid lab\n  tether cluster seeds show",
	}
	cmd.AddCommand(newClusterSeedsPublishCmd(socketPath), newClusterSeedsShowCmd(socketPath))
	return cmd
}

func newClusterSeedsPublishCmd(socketPath *string) *cobra.Command {
	var bootstrap, sid string
	var endpoints []string
	cmd := &cobra.Command{
		Use:   "publish --bootstrap <https-url> --endpoint <nats-url>...",
		Short: "Publish the bootstrap manifest URL + client-dialable endpoints (leader-only); prints a ready-to-paste invite",
		Example: "  tether cluster seeds publish \\\n" +
			"    --bootstrap https://cluster.example.com/.well-known/tether/cluster.json \\\n" +
			"    --endpoint wss://b1.example.com:443 --endpoint wss://b2.example.com:443 --sid lab",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(endpoints) == 0 {
				return fmt.Errorf("at least one --endpoint is required")
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterSeedsPublish, Bootstrap: bootstrap, Endpoints: endpoints})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("seeds publish", resp)
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "seeds published (seed_generation=%d)\n", resp.SeedGeneration)
			_, _ = fmt.Fprintln(out, "\nFront the loopback manifest listener with Caddy (add to your Caddyfile):")
			_, _ = fmt.Fprintln(out, "  handle /.well-known/tether/* { reverse_proxy 127.0.0.1:<--cluster-manifest-listen port> }")
			switch {
			case resp.SeedAccountPub == "":
				_, _ = fmt.Fprintln(out, "\n(no account key available → cannot mint an invite; run on a broker with the cluster account seed)")
			case bootstrap == "" || sid == "":
				_, _ = fmt.Fprintln(out, "\n(pass --bootstrap and --sid to also print a ready-to-paste agent invite)")
			default:
				inv, ierr := clusterroster.MintInvite(clusterroster.Invite{Pin: resp.SeedAccountPub, BootstrapURL: bootstrap, SID: sid, Seed: endpoints[0]})
				if ierr != nil {
					_, _ = fmt.Fprintf(out, "\n(could not mint invite: %v)\n", ierr)
				} else {
					_, _ = fmt.Fprintf(out, "\nAgent invite (hand OOB to a new agent → `tether agent join '<invite>'`):\n  %s\n", inv)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&bootstrap, "bootstrap", "", "the public well-known HTTPS manifest URL")
	cmd.Flags().StringArrayVar(&endpoints, "endpoint", nil, "a client-dialable NATS endpoint (nats://|tls://|wss://); repeatable")
	cmd.Flags().StringVar(&sid, "sid", "", "session id to embed in the printed invite (optional)")
	return cmd
}

func newClusterSeedsShowCmd(socketPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Show the published seed endpoints + bootstrap URL + seed_generation (leader-agnostic)",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterSeedsShow})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("seeds show", resp)
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "seed_generation: %d\n", resp.SeedGeneration)
			_, _ = fmt.Fprintf(out, "bootstrap_url:   %s\n", resp.SeedBootstrap)
			_, _ = fmt.Fprintf(out, "account_pub:     %s\n", resp.SeedAccountPub)
			_, _ = fmt.Fprintf(out, "endpoints:       %s\n", strings.Join(resp.SeedEndpoints, ", "))
			return nil
		},
	}
}
