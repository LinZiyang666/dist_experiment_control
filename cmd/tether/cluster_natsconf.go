package main

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natscluster"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/spf13/cobra"
)

// validateRouteURL fail-closes a malformed NATS route URL (audit natsconf F4): the previous
// non-empty check let a typo'd / scheme-less URL through to the rendered conf, where only the
// `nats-server -t` dry-run might catch it (and --skip-dry-run bypasses even that). Require an
// explicit nats://host:port.
func validateRouteURL(label, s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("takeover-natsconf: %s route URL %q is not a valid URL: %w", label, s, err)
	}
	if u.Scheme != "nats" {
		return fmt.Errorf("takeover-natsconf: %s route URL %q must use the nats:// scheme (got %q)", label, s, u.Scheme)
	}
	if u.Hostname() == "" || u.Port() == "" {
		return fmt.Errorf("takeover-natsconf: %s route URL %q must be nats://host:port", label, s)
	}
	return nil
}

// cluster_natsconf.go — the D9 §11 operator commands that wire the internal/natsconf leaf
// (takeover) + the internal/clusteroffline secrets preflight (doctor) to the CLI.

func newClusterTakeoverNatsconfCmd() *cobra.Command {
	var confPath, secretsDir, serverName, accountIssuer, brokerNkey, routeURL, clusterListen, natsServerBin string
	var skipDryRun bool
	var peerSpecs []string
	cmd := &cobra.Command{
		Use:   "takeover-natsconf",
		Short: "Rewrite nats.conf with the cluster directives + auth_callout (fail-closed; preserves install.sh bits)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			own, err := natsconf.Preflight(confPath)
			if err != nil {
				return err // refuses fail-closed on an unknown/include directive
			}
			// Identity SSOT: prefer the existing conf's §3.4 auth_callout; flags fill/override.
			// Broker nkey auto-read is allowed only when the auth block has exactly one nkey
			// user; generated multi-broker configs require --broker-nkey to avoid selecting a
			// peer's nkey by list position.
			ci, cn := own.AuthIdentity()
			if accountIssuer == "" {
				accountIssuer = ci
			}
			if brokerNkey == "" {
				brokerNkey = cn
			}
			if accountIssuer == "" || brokerNkey == "" {
				return fmt.Errorf("takeover-natsconf: need --account-issuer + --broker-nkey " +
					"(account issuer may be read from existing auth_callout; broker nkey is auto-read only when " +
					"the existing authorization block has exactly one nkey user)")
			}
			if serverName == "" || routeURL == "" {
				return fmt.Errorf("takeover-natsconf: --server-name and --route-url are required")
			}
			if err := validateRouteURL("--route-url", routeURL); err != nil {
				return err
			}
			// round-1 MAJOR: refuse if the client listen could not be harvested from the
			// existing conf — an empty listen makes nats-server bind the default 0.0.0.0:4222
			// (a surprise public-bind change), so fail loud instead of silently re-binding.
			clientListen := own.ClientListen()
			if clientListen == "" {
				return fmt.Errorf("takeover-natsconf: could not determine the client listen address from %q "+
					"(no host/port/listen) — refusing to emit a conf that would default-bind 0.0.0.0:4222", confPath)
			}
			self := natscluster.Broker{ServerName: serverName, NkeyPub: brokerNkey, RouteURL: routeURL}
			// External-review F2: render the FULL peer mesh, not just self. Each --peer is a
			// "server_name,route_url,bus_nkey" triple for ANOTHER broker; growth re-runs takeover
			// on EVERY node with the complete set so routes + auth_users + per-broker ACLs form a
			// real multi-node NATS mesh (Raft membership alone does not). N=1 takeover passes no
			// --peer and renders just self.
			peers := []natscluster.Broker{self}
			for _, spec := range peerSpecs {
				p, perr := parsePeerSpec(spec)
				if perr != nil {
					return perr
				}
				peers = append(peers, p)
			}
			cfg := natscluster.Config{
				Local:         self,
				Peers:         peers,
				AccountIssuer: accountIssuer,
				JSStoreDir:    own.JSStoreDir(),
				ClientListen:  clientListen,
				ClusterListen: clusterListen,
				CAFile:        filepath.Join(secretsDir, "cluster-ca.pem"),
				CertFile:      filepath.Join(secretsDir, "route-cert.pem"),
				KeyFile:       filepath.Join(secretsDir, "route-key.pem"),
			}
			// audit natsconf F2: if the existing conf ENABLES JetStream but no store_dir survived
			// (empty), REFUSE rather than render a conf that silently DISABLES JetStream (the next
			// restart would lose all streams). Fail-closed; install.sh always writes store_dir, so
			// the common path is unaffected.
			if _, hasJS := own.Parsed["jetstream"]; hasJS && own.JSStoreDir() == "" {
				return fmt.Errorf("natsconf takeover: existing conf enables jetstream but has no resolvable store_dir; " +
					"refusing to render a conf that would silently DISABLE JetStream — set jetstream.store_dir explicitly")
			}
			merged, err := natsconf.BuildMergedConf(own, cfg)
			if err != nil {
				return err
			}
			// round-1 BLOCKER: validate with `nats-server -t` BEFORE swapping — never commit
			// an invalid conf that bricks the broker on its next restart (takeover is the LAST
			// live-box step). --skip-dry-run is an explicit escape hatch (e.g. a box without
			// nats-server on PATH), logged loudly.
			if skipDryRun {
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(),
					"WARNING: --skip-dry-run set — swapping nats.conf WITHOUT `nats-server -t` validation")
			} else if err := natsconf.DryRun(natsServerBin, merged); err != nil {
				return err
			}
			if err := natsconf.Apply(confPath, merged); err != nil {
				return err
			}
			_, _ = fmt.Fprint(cmd.OutOrStdout(), natsconf.OwnershipTable(own))
			_, _ = fmt.Fprintln(cmd.OutOrStdout(),
				"nats.conf taken over (pristine .bak kept). NEXT: `systemctl restart nats-server`, "+
					"then start tether-broker in cluster mode.")
			return nil
		},
	}
	cmd.Flags().StringVar(&confPath, "conf", "/etc/tether/nats.conf", "nats-server.conf to take over")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir (CA + route leaf)")
	cmd.Flags().StringVar(&serverName, "server-name", "", "this broker's deterministic server_name (== cluster node_id)")
	cmd.Flags().StringVar(&accountIssuer, "account-issuer", "", "shared account nkey pub; empty => read from the existing conf")
	cmd.Flags().StringVar(&brokerNkey, "broker-nkey", "", "this broker's bus nkey pub; empty => read only when the existing conf has exactly one nkey user")
	cmd.Flags().StringVar(&routeURL, "route-url", "", "this broker's NATS route URL, e.g. nats://10.0.0.1:6222")
	cmd.Flags().StringVar(&clusterListen, "cluster-listen", "0.0.0.0:6222", "route listen address")
	cmd.Flags().StringVar(&natsServerBin, "nats-server", "nats-server", "nats-server binary for the -t dry-run validation")
	cmd.Flags().BoolVar(&skipDryRun, "skip-dry-run", false, "skip the `nats-server -t` validation (NOT recommended)")
	cmd.Flags().StringArrayVar(&peerSpecs, "peer", nil, "another broker as server_name,route_url,bus_nkey (repeat for each peer; the full mesh)")
	return cmd
}

// parsePeerSpec parses a --peer "server_name,route_url,bus_nkey" triple into a Broker.
func parsePeerSpec(spec string) (natscluster.Broker, error) {
	parts := strings.Split(spec, ",")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return natscluster.Broker{}, fmt.Errorf("takeover-natsconf: --peer %q must be server_name,route_url,bus_nkey", spec)
	}
	if err := validateRouteURL("--peer", parts[1]); err != nil {
		return natscluster.Broker{}, err
	}
	return natscluster.Broker{ServerName: parts[0], RouteURL: parts[1], NkeyPub: parts[2]}, nil
}

func newClusterDoctorCmd() *cobra.Command {
	var secretsDir string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Preflight the §15 secrets (missing/unreadable/world-readable key => FATAL; FDE => advisory)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			adv, fatal := clusteroffline.SecretsPreflight(secretsDir)
			for _, a := range adv {
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "ADVISORY: "+a)
			}
			if fatal != nil {
				return fatal
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "secrets preflight: OK")
			return nil
		},
	}
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir")
	return cmd
}
