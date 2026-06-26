package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

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

// natsconfTakeoverFlags holds the manual nats.conf takeover flag set, shared by the (deprecated,
// hidden) `cluster takeover-natsconf` alias and `cluster reconcile nats --manual` (C3 demotion).
type natsconfTakeoverFlags struct {
	confPath, secretsDir, serverName, accountIssuer, brokerNkey, routeURL, clusterListen, natsServerBin string
	skipDryRun, plan, asJSON, allowPartialMesh                                                          bool
	peerSpecs                                                                                           []string
	takeoverSocket                                                                                      string
}

func bindNatsconfTakeoverFlags(cmd *cobra.Command, f *natsconfTakeoverFlags) {
	cmd.Flags().StringVar(&f.confPath, "conf", "/etc/tether/nats.conf", "nats-server.conf to take over")
	cmd.Flags().StringVar(&f.secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir (CA + route leaf)")
	cmd.Flags().StringVar(&f.serverName, "server-name", "", "this broker's deterministic server_name (== cluster node_id)")
	cmd.Flags().StringVar(&f.accountIssuer, "account-issuer", "", "shared account nkey pub; empty => read from the existing conf")
	cmd.Flags().StringVar(&f.brokerNkey, "broker-nkey", "", "this broker's bus nkey pub; empty => read only when the existing conf has exactly one nkey user")
	cmd.Flags().StringVar(&f.routeURL, "route-url", "", "this broker's NATS route URL, e.g. nats://10.0.0.1:6222")
	cmd.Flags().StringVar(&f.clusterListen, "cluster-listen", "0.0.0.0:6222", "route listen address")
	cmd.Flags().StringVar(&f.natsServerBin, "nats-server", "nats-server", "nats-server binary for the -t dry-run validation")
	cmd.Flags().BoolVar(&f.skipDryRun, "skip-dry-run", false, "skip the `nats-server -t` validation (NOT recommended)")
	cmd.Flags().StringArrayVar(&f.peerSpecs, "peer", nil, "another broker as server_name,route_url,bus_nkey (repeat for each peer; the full mesh)")
	cmd.Flags().BoolVar(&f.plan, "plan", false, "dry-run: print the would-be change (ownership diff + mesh + dry-run + restart hint) and exit WITHOUT writing anything (B5 OPS#10)")
	cmd.Flags().BoolVar(&f.asJSON, "json", false, "with --plan: emit the stable machine JSON schema")
	cmd.Flags().StringVar(&f.takeoverSocket, "socket", defaultAdminSocket, "admin socket to cross-check the peer mesh against the live roster (F8)")
	cmd.Flags().BoolVar(&f.allowPartialMesh, "allow-partial-mesh", false, "skip the live-roster voter-coverage check (render a deliberately partial mesh)")
}

// runNatsconfTakeover is the shared manual-takeover engine (extracted so both the deprecated alias
// and `reconcile nats --manual` call it verbatim).
func runNatsconfTakeover(cmd *cobra.Command, f *natsconfTakeoverFlags) error {
	confPath, secretsDir, serverName := f.confPath, f.secretsDir, f.serverName
	accountIssuer, brokerNkey, routeURL := f.accountIssuer, f.brokerNkey, f.routeURL
	clusterListen, natsServerBin := f.clusterListen, f.natsServerBin
	skipDryRun, plan, asJSON, allowPartialMesh := f.skipDryRun, f.plan, f.asJSON, f.allowPartialMesh
	peerSpecs, takeoverSocket := f.peerSpecs, f.takeoverSocket
	{
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
		// External-review F8: a `nats-server -t` dry-run cannot tell that a voter is MISSING from
		// the peer set — it would happily validate a syntactically-correct but mesh-INCOMPLETE conf
		// (missing routes / auth users / per-broker ACLs). When the admin socket is reachable,
		// cross-check the rendered peers against the live LEADER roster's voters and REFUSE on any
		// omission (unless --allow-partial-mesh). When unreachable, we cannot verify — warn.
		if !allowPartialMesh {
			if missing, checked := missingVotersInMesh(takeoverSocket, peers); checked && len(missing) > 0 {
				return usageErr("takeover-natsconf: the --peer mesh is missing voter(s) present in the live roster: %v.\n"+
					"  A conf that omits a voter passes `nats-server -t` but loses that broker's routes/auth/ACLs.\n"+
					"  Add a --peer triple for each, or pass --allow-partial-mesh if this omission is deliberate.", missing)
			} else if !checked {
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(),
					"takeover-natsconf: could not reach the admin socket to verify the peer mesh against the live roster — "+
						"ensure --peer lists EVERY current voter (a missing voter yields a valid-but-incomplete conf).")
			}
		}
		cfg := natscluster.Config{
			Local:         self,
			Peers:         peers,
			AccountIssuer: accountIssuer,
			JSStoreDir:    own.JSStoreDir(),
			ClientListen:  clientListen,
			ClusterListen: clusterListen,
			// C3-B3: emit the loopback HTTP monitor so a manual cutover (the ONE restart-bearing step)
			// establishes `http:`; the per-broker reconciler then PRESERVES it (it can never hot-add a
			// monitor via SIGHUP). Keep this addr in sync with the broker's topoMonitorListen.
			MonitorListen: "127.0.0.1:8223",
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
		// B5 OPS#10 --plan: a pure dry-run. It captures (does NOT fail on) the dry-run result
		// and renders the would-be change, then returns BEFORE Apply — it mutates NOTHING (no
		// write, no .bak, no rename).
		if plan {
			dryRunResult := "ok"
			if skipDryRun {
				dryRunResult = "skipped"
			} else if e := natsconf.DryRun(natsServerBin, merged); e != nil {
				dryRunResult = e.Error()
			}
			return renderTakeoverPlan(cmd, confPath, serverName, clientListen, own.JSStoreDir(), peers, merged, dryRunResult, asJSON)
		}
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
	}
}

// newClusterTakeoverNatsconfCmd is the DEPRECATED hidden alias for `cluster reconcile nats --manual`
// (C3 demotion — the per-broker reconciler now converges automatically; manual takeover is the
// one-time single→cluster cutover escape hatch).
func newClusterTakeoverNatsconfCmd() *cobra.Command {
	var f natsconfTakeoverFlags
	cmd := &cobra.Command{
		Use:    "takeover-natsconf",
		Short:  "DEPRECATED: use `cluster reconcile nats --manual` (one-time manual nats.conf takeover)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "deprecated: use `tether cluster reconcile nats --manual`")
			return runNatsconfTakeover(cmd, &f)
		},
	}
	bindNatsconfTakeoverFlags(cmd, &f)
	return cmd
}

// missingVotersInMesh cross-checks the rendered peer set against the live LEADER roster's voters
// (External-review F8). It returns the voter node_ids absent from the peer ServerName set, and
// whether the check could actually run (the socket answered an authoritative leader view). A
// non-leader / unreachable / errored status returns checked=false (cannot verify).
func missingVotersInMesh(socket string, peers []natscluster.Broker) (missing []string, checked bool) {
	rep, err := fetchClusterStatusReport(socket)
	if err != nil || rep == nil || !rep.IsLeaderView || rep.Partial || len(rep.Errors) > 0 {
		return nil, false
	}
	have := map[string]bool{}
	for _, p := range peers {
		have[p.ServerName] = true
	}
	for _, n := range rep.Nodes {
		if n.Role == "voter" || n.Role == "leader" {
			if !have[n.NodeID] {
				missing = append(missing, n.NodeID)
			}
		}
	}
	return missing, true
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
	var secretsDir, dbPath, confPath, raftAddr, natsRoute, socketPath string
	var asJSON, offline bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose the cluster — ONLINE health check if the daemon is running, else the READ-ONLY pre-`init` preflight",
		Example: "  tether cluster doctor               # auto: online health if the daemon answers, else pre-init preflight\n" +
			"  tether cluster doctor --offline     # force the pre-init preflight (secrets/cert/ports/nats.conf/db)",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// B7 DOC#6(b): auto-detect. If the daemon's admin socket answers OpClusterStatus, run the
			// ONLINE health diagnostic; else fall back to the OFFLINE pre-init preflight. --offline
			// forces the preflight (the historical behavior, for a pre-`init` host).
			var prefix []clusteroffline.DoctorCheck
			if !offline {
				rep, err := fetchClusterStatusReport(socketPath)
				if err == nil && rep != nil {
					return renderDoctor(cmd, clusterDoctorOnline(rep), asJSON)
				}
				// Stage-C M5 + External-review F7: do NOT silently fall through to a green offline
				// preflight. If the socket FILE EXISTS but the call failed (refused/EOF/mid-restart),
				// the daemon is supposed to be up but isn't answering — the exact incident the operator
				// runs `doctor` for. Warn loudly AND inject a FATAL online_admin_socket check so the
				// offline checks still run for diagnostics but the OVERALL exit is non-zero (a monitor
				// must NOT read "no online check ran" as "cluster doctor OK").
				if _, statErr := os.Stat(socketPath); statErr == nil {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
						"cluster doctor: the broker admin socket %s did not answer (%v) — the daemon may be DOWN.\n"+
							"  Showing the OFFLINE pre-init preflight (secrets/cert/nats.conf/db) — NOT a live health check.\n", socketPath, err)
					prefix = append(prefix, clusteroffline.DoctorCheck{
						Name:   "online_admin_socket",
						Status: clusteroffline.DoctorFatal,
						Detail: fmt.Sprintf("admin socket %s exists but did not answer (%v) — daemon DOWN or mid-restart; this is NOT a healthy online cluster", socketPath, err),
					})
				}
			}
			checks := clusteroffline.Doctor(clusteroffline.DoctorOptions{
				SecretsDir: secretsDir, DBPath: dbPath, ConfPath: confPath, RaftAddr: raftAddr, NatsRoute: natsRoute,
			})
			return renderDoctor(cmd, append(prefix, checks...), asJSON)
		},
	}
	cmd.Flags().BoolVar(&offline, "offline", false, "force the offline pre-init preflight (skip the online health check)")
	cmd.Flags().StringVar(&socketPath, "socket", defaultAdminSocket, "broker admin socket (for the online check)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path (migration source)")
	cmd.Flags().StringVar(&confPath, "conf", defaultNatsConfPath, "nats.conf to preflight")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "this node's raft addr (host:7400) — checks port bindability")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "this node's NATS route (nats://host:6222) — checks port bindability")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human table)")
	return cmd
}

// renderDoctor prints the check table (or --json) + summary; returns a usage error (64) on any
// FATAL so a script / the operator knows it is not safe to proceed. Shared by `cluster doctor`
// and `cluster init --check`.
func renderDoctor(cmd *cobra.Command, checks []clusteroffline.DoctorCheck, asJSON bool) error {
	pass, advisory, fatal := clusteroffline.DoctorSummary(checks)
	out := cmd.OutOrStdout()
	if asJSON {
		type doctorJSON struct {
			Schema        string                       `json:"schema"`
			SchemaVersion int                          `json:"schema_version"`
			Checks        []clusteroffline.DoctorCheck `json:"checks"`
			Summary       struct {
				Pass     int `json:"pass"`
				Advisory int `json:"advisory"`
				Fatal    int `json:"fatal"`
			} `json:"summary"`
		}
		dj := doctorJSON{Schema: "cluster_doctor", SchemaVersion: 1, Checks: checks}
		dj.Summary.Pass, dj.Summary.Advisory, dj.Summary.Fatal = pass, advisory, fatal
		if err := emitJSON(out, dj); err != nil {
			return err
		}
	} else {
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "CHECK\tSTATUS\tDETAIL")
		for _, c := range checks {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", c.Name, c.Status, c.Detail)
		}
		_ = tw.Flush()
		_, _ = fmt.Fprintf(out, "\ndoctor: %d pass, %d advisory, %d fatal\n", pass, advisory, fatal)
	}
	if fatal > 0 {
		return usageErr("cluster doctor: %d FATAL check(s) — fix them before `cluster init`", fatal)
	}
	return nil
}

// renderTakeoverPlan renders the B5 OPS#10 `takeover-natsconf --plan` dry-run: what the takeover
// WOULD change, without writing anything. `changed` compares the rendered merged conf against the
// current file bytes (the honest "would this rewrite the file?" signal). routes/auth_users come
// from the resolved peer mesh; ownership_regenerated lists the sections takeover always rewrites.
func renderTakeoverPlan(cmd *cobra.Command, confPath, serverName, clientListen, jsStoreDir string, peers []natscluster.Broker, merged, dryRun string, asJSON bool) error {
	current, _ := os.ReadFile(confPath) // a missing/unreadable conf ⇒ all-new (changed=true)
	changed := string(current) != merged
	var routes, authUsers []string
	for _, p := range peers {
		if p.RouteURL != "" {
			routes = append(routes, p.RouteURL)
		}
		if p.NkeyPub != "" {
			authUsers = append(authUsers, p.ServerName+":"+p.NkeyPub)
		}
	}
	restartHint := "re-run on EVERY broker IN ORDER (leader LAST, so you don't trigger a re-election mid-rollout), then `systemctl restart nats-server` after each."
	regenerated := []string{"authorization", "cluster", "server_name"}
	if asJSON {
		return emitJSON(cmd.OutOrStdout(), takeoverPlanJSON{
			Schema: "takeover_natsconf_plan", SchemaVersion: 1, Changed: changed,
			ServerName: serverName, ClientListen: clientListen, JSStoreDir: jsStoreDir,
			Routes: normSlice(routes), AuthUsers: normSlice(authUsers),
			OwnershipRegenerated: regenerated, DryRun: dryRun, RestartOrderHint: restartHint,
		})
	}
	out := cmd.OutOrStdout()
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "plan (no write)\t%s\n", confPath)
	_, _ = fmt.Fprintf(tw, "  changed\t%v\n", changed)
	_, _ = fmt.Fprintf(tw, "  server_name\t%s\n", serverName)
	_, _ = fmt.Fprintf(tw, "  client_listen\t%s\n", clientListen)
	_, _ = fmt.Fprintf(tw, "  js_store_dir\t%s\n", jsStoreDir)
	_, _ = fmt.Fprintf(tw, "  routes\t%s\n", strings.Join(routes, ", "))
	_, _ = fmt.Fprintf(tw, "  auth_users\t%s\n", strings.Join(authUsers, ", "))
	_, _ = fmt.Fprintf(tw, "  regenerated\t%s\n", strings.Join(regenerated, ", "))
	_, _ = fmt.Fprintf(tw, "  dry_run\t%s\n", dryRun)
	_ = tw.Flush()
	_, _ = fmt.Fprintf(out, "# NOTHING WAS WRITTEN. To apply: re-run without --plan. Then: %s\n", restartHint)
	return nil
}
