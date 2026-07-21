package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/spf13/cobra"
)

// cluster_backup.go — the B6 OPS#3 `cluster backup` / `cluster restore` CLI. backup is dual:
// online (default; the broker writes the bundle off its RO handle — any node) or --offline (a
// stopped daemon, the non-cluster byte-equivalence path). restore is OFFLINE-only, irreversible,
// and identity-affecting, so it is a Tier-2 typed-confirm (never --yes, never a machine escape).

const defaultClusterSecretsDir = "/etc/tether/secrets"

func newClusterBackupCmd(socketPath *string) *cobra.Command {
	var offline bool
	var out, dbPath, dataDir, secretsDir string
	var allowFollower bool
	cmd := &cobra.Command{
		Use:   "backup --out <dir>",
		Short: "Write a { state.db, manifest.json } backup bundle (online via the broker leader, or --offline on a stopped daemon)",
		Example: "  # online (daemon running; runs on the LEADER for the freshest committed state):\n" +
			"  tether cluster backup --out /var/backups/tether-$(date +%F)\n" +
			"  # offline (daemon STOPPED):\n" +
			"  tether cluster backup --offline --out /var/backups/tether --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				return usageErr("cluster backup requires --out <dir> (the bundle directory to create)")
			}
			if offline {
				res, err := clusteroffline.OfflineBackup(clusteroffline.BackupOptions{
					DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, OutDir: out,
				})
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "offline backup complete: %s (%d bytes, mode=%s, self=%s, applied_index=%d)\n",
					res.BundleDir, res.StateDBBytes, res.Mode, res.SelfID, res.AppliedIndex)
				// R10 #53: state the bundle's SCOPE at the point the operator forms the belief
				// "I have a backup". Silently omitting JetStream is what made a total-loss restore
				// come back with no history/audit and no warning at either end.
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), clusteroffline.JetStreamBackupScopeWarning)
				return nil
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterBackup, BackupPath: out, AllowFollower: allowFollower})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("backup", resp)
			}
			if resp.Backup != nil {
				src := resp.Backup.SourceRole
				if resp.Backup.SourceRole == "follower" {
					src = "FOLLOWER (possibly stale — leader: " + resp.Backup.LeaderID + ")"
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "online backup complete: %s (%d bytes, applied_index=%d, self=%s, source=%s)\n",
					resp.Backup.Path, resp.Backup.Bytes, resp.Backup.AppliedIndex, resp.Backup.SelfID, src)
				// R10 #53: the ONLINE bundle has exactly the same scope as the offline one — warn
				// identically, so the operator's belief never depends on which path they used.
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), clusteroffline.JetStreamBackupScopeWarning)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&offline, "offline", false, "back up a STOPPED daemon's on-disk DB directly (no broker)")
	cmd.Flags().BoolVar(&allowFollower, "allow-stale-follower", false, "permit an online backup off a NON-leader follower (a possibly-stale local view; default refuses, re-run on the leader)")
	cmd.Flags().StringVar(&out, "out", "", "bundle directory to create (must not exist)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path (--offline)")
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (--offline)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", defaultClusterSecretsDir, "secrets dir for the advisory account_fp (--offline)")
	return cmd
}

func newClusterRestoreCmd() *cobra.Command {
	var dataDir, dbPath, secretsDir, confirmNodeID, raftAddr, configPath, natsConfPath string
	cmd := &cobra.Command{
		Use:   "restore <bundle> --confirm-node-id <id>",
		Short: "Restore a backup bundle as a fresh single-voter cluster (OFFLINE, daemon STOPPED, IRREVERSIBLE)",
		Example: "  systemctl stop tether-broker\n" +
			"  tether cluster recovery restore /var/backups/tether-2026-06-24 --confirm-node-id brk-a --secrets-dir /etc/tether/secrets",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bundle := args[0]
			if confirmNodeID == "" {
				return usageErr("restore requires --confirm-node-id <id> (the node this bundle is for)")
			}
			if err := rejectedUnattendedYes(cmd, "restore", confirmNodeID); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"restore will OVERWRITE this host's DB with the bundle and re-bootstrap a single-voter cluster.\n"+
					"The current DB is preserved under a unique <db>.pre-restore[.N].bak (printed on completion);\n"+
					"the cluster re-grows with `cluster join approve`.\n")
			// Tier-2 typed-confirm: irreversible + identity-affecting ⇒ NO --yes, NEVER a machine escape.
			if !confirmTypedNodeID(cmd, confirmNodeID,
				"CONSEQUENCE: the live DB is replaced; the restored node is a single voter with NO HA until you re-grow.",
				false, "") {
				// External-review F11: a confirmation abort is operator input → exit 64 (usage), not 70.
				return usageErr("aborted (type the node_id to confirm; --yes is never accepted)")
			}
			res, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
				BundleDir: bundle, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
				ConfirmNodeID: confirmNodeID, RaftAddrOverride: raftAddr,
			})
			if err != nil {
				return fmt.Errorf("restore: %w", err)
			}
			preserved := res.PreRestoreBackup
			if preserved == "" {
				preserved = "(none — no prior DB on this host)"
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out,
				"restore complete: node %s is now a single-voter cluster (pruned %d stale peers; bundle applied_index %d reset to 0).\n"+
					"prior DB preserved at: %s\n",
				res.SelfID, res.PrunedPeers, res.BundleAppliedIdx, preserved)

			// R10 P2: APPLY the broker.yaml cluster seam. Without it the restored host has a
			// cluster-seeded DB but broker.cluster.data_dir unset, and the daemon FATALs at boot
			// ("refusing to silently downgrade a cluster DB to single mode") — the disaster-recovery
			// runbook was structurally impossible to execute, because restore had no --config at all
			// and `install.sh` ships broker.yaml with the whole `cluster:` block commented out.
			// Same helper (and same fail-closed decode-back verification) the `cluster init` path uses.
			seamApplied, seamErr := applyRestoreClusterSeam(cmd, configPath, dataDir, res.RaftAddr, secretsDir)

			// R10 P4 (#64) + #53: the next steps the product already knew and only ever said AFTER the
			// crash / never at all. M1: gate step 3 on whether the seam was ACTUALLY applied — a
			// deliberate --config "" opt-out is seamErr==nil but seamApplied==false, and printing
			// "✓ seam is in — start the daemon" there would authorize a boot the same command warned
			// will fail. seamApplied is the tri-state that keeps the two facts from contradicting.
			printRestoreNextSteps(cmd, res, secretsDir, natsConfPath, configPath, seamApplied)
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(), clusteroffline.JetStreamRestoreScopeWarning)

			if seamErr != nil {
				// Fail CLOSED: the restore itself succeeded (and is irreversible, so the report above
				// stands), but a host whose broker.yaml has no cluster seam CANNOT start. Exiting 0 here
				// is exactly the silent half-done state R10 exists to remove.
				return seamErr
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (raft/ is wiped + re-bootstrapped)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path (the install destination)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", defaultClusterSecretsDir, "the LIVE secrets dir (the un-forgeable provenance anchor)")
	cmd.Flags().StringVar(&confirmNodeID, "confirm-node-id", "", "the node_id this bundle is for (must match the manifest)")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "override the bundle's raft addr (host:7400) for a FRESH-HOST restore where this box's IP changed (default: the bundle's address)")
	cmd.Flags().StringVar(&configPath, "config", defaultBrokerConfigPath, "broker.yaml to apply the broker.cluster seam into (P2; a restored host without it FATALs at boot — pass \"\" only to write it by hand)")
	cmd.Flags().StringVar(&natsConfPath, "nats-conf", defaultNatsConfPath, "nats.conf inspected to choose the correct NEXT step (clustered => de-cluster; fresh/standalone => render this lone voter's conf)")
	registerYesRejector(cmd)
	return cmd
}

// applyRestoreClusterSeam writes the broker.cluster seam into broker.yaml after a restore (R10 P2).
//
// The seam is what makes `serve` boot in CLUSTER mode: `assertClusterDBConsistent` FATALs when the DB
// is cluster-seeded (which a restored DB always is) but broker.cluster.data_dir is unset. `cluster
// init` has applied it since G4 #5; restore never could, because it had no --config flag — so the
// whole of runbook §5.2 (full-cluster disaster recovery) dead-ended on a boot FATAL.
//
// Empty configPath is an explicit operator opt-out (write it by hand). A configPath that does not
// EXIST is an error, not a silent skip: on a stock install `install.sh` always writes broker.yaml, so
// a missing one means the flag is wrong, and returning "applied=false, nil" there would reproduce the
// exact silent-no-op this fix removes.
//
// M1 (external review): returns an explicit (applied, err) tri-state so the caller can
// tell the three outcomes apart — SEAM IN (applied=true), a deliberate manual opt-out
// (applied=false, err=nil), and a hard failure (err!=nil). The old single-error return
// conflated the opt-out with success, so the NEXT-steps renderer printed "✓ seam is in
// — start the daemon" for a restore that had explicitly NOT installed it.
func applyRestoreClusterSeam(cmd *cobra.Command, configPath, dataDir, raftAddr, secretsDir string) (bool, error) {
	if configPath == "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
			"note: --config \"\" — the broker.cluster seam was NOT applied. Set broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path,nats_server_bin} in broker.yaml by hand before starting tether-broker, or it will refuse to start.\n")
		return false, nil
	}
	if _, err := os.Stat(configPath); err != nil {
		return false, &ExitError{Class: exitUsage, Err: fmt.Errorf(
			"restore: cannot apply the broker.cluster seam — --config %s is not readable (%v).\n"+
				"  The restored DB is cluster-seeded, so tether-broker will REFUSE to start until broker.cluster is set.\n"+
				"  Point --config at this host's broker.yaml and re-run (restore is idempotent), or set the seam by hand", configPath, err)}
	}
	applied, err := applyClusterSeam(configPath, dataDir, raftAddr, secretsDir)
	if err != nil {
		return false, &ExitError{Class: exitInternal, Err: fmt.Errorf(
			"restore: the DB was restored but the broker.cluster seam could NOT be applied to %s: %w.\n"+
				"  tether-broker will REFUSE to start until broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path,nats_server_bin} is set", configPath, err)}
	}
	if applied {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "broker.cluster seam applied to %s (data_dir=%s raft_addr=%s secrets_dir=%s nats_conf_path=%s).\n",
			configPath, dataDir, raftAddr, secretsDir, defaultNatsConfPath)
	} else {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "broker.cluster seam already present and correct in %s (raft_addr=%s).\n", configPath, raftAddr)
	}
	// Either way the seam is now IN (freshly applied or already correct).
	return true, nil
}

// printRestoreNextSteps emits the ordered, COPY-PASTE-READY next steps (R10 P4 / #64).
//
// Before R10 the completion text was one line — "NEXT: start tether-broker, then `cluster join
// approve`" — and following it verbatim crash-looped the broker, because restore prunes the roster to
// a single voter but leaves nats.conf untouched: a lone node can never reach the clustered JetStream
// meta quorum, so `broker.Run` FATALs. The product printed the missing step at that moment (the same
// de-cluster remedy shared via natsconf) — it knew what to say and only said it too late.
//
// Two situations, each with a step the operator can run as printed:
//
//	A. nats.conf is CLUSTERED  → it must be de-clustered BEFORE the daemon is started.
//	B. fresh / standalone conf → it must be rendered for this lone voter BEFORE the daemon is started.
//
// Both land on the same offline verb (`reconcile nats --manual` with NO --peer renders
// Standalone: len(peers)==1). That is not a shortcut: the ONLINE de-cluster verb proves N=1 from a
// live LEADER status view, and in situation A the daemon cannot be started at all, so it is
// structurally unreachable here. The online verb is still printed, as the thing to use once the node
// IS up.
func printRestoreNextSteps(cmd *cobra.Command, res *clusteroffline.RestoreResult, secretsDir, natsConfPath, configPath string, seamOK bool) {
	out := cmd.OutOrStdout()
	ids := readClusterPublicIdentities(secretsDir, natsConfPath)
	acctTok := orPlaceholder(ids.AccountIssuer, "<account-public-nkey>")
	brkTok := orPlaceholder(ids.BrokerNkey, "<broker-public-nkey>")
	routeURL := res.NatsRoute
	if routeURL != "" && !strings.Contains(routeURL, "://") {
		routeURL = "nats://" + routeURL
	}
	if routeURL == "" {
		routeURL = "<nats://this-host:6222>"
	}

	clustered := false
	confReadable := false
	if own, perr := natsconf.Preflight(natsConfPath); perr == nil {
		confReadable = true
		clustered = own.IsClusteredJetStream()
	}

	_, _ = fmt.Fprintln(out, "NEXT (run in order):")
	switch {
	case clustered:
		_, _ = fmt.Fprintf(out, "  1. %s is CLUSTERED, but this node is now a LONE VOTER — a single node can never reach the\n"+
			"     clustered JetStream meta quorum, so tether-broker will REFUSE to start (crash-loop). De-cluster it FIRST:\n", natsConfPath)
	case confReadable:
		_, _ = fmt.Fprintf(out, "  1. %s is standalone — render THIS node's lone-voter cluster conf (auth_callout + cluster ACL,\n"+
			"     standalone JetStream) before starting the daemon:\n", natsConfPath)
	default:
		_, _ = fmt.Fprintf(out, "  1. %s is missing/unreadable (a fresh DR box) — render THIS node's lone-voter conf before starting the daemon:\n", natsConfPath)
	}
	_, _ = fmt.Fprintf(out, "     tether cluster reconcile nats --manual --conf %s --secrets-dir %s --server-name %s --route-url %s --account-issuer %s --broker-nkey %s\n",
		natsConfPath, secretsDir, res.SelfID, routeURL, acctTok, brkTok)
	_, _ = fmt.Fprintln(out, "     # NO --peer ⇒ a STANDALONE (no cluster{}) conf, which is what N=1 requires. Works with the daemon STOPPED.")
	if ids.Note != "" {
		_, _ = fmt.Fprintf(out, "     %s\n", ids.Note)
	}
	if clustered {
		// The shared SSOT remedy — the same sentence the boot FATAL and the status banner emit.
		_, _ = fmt.Fprintf(out, "     # once the node is UP again the equivalent ONLINE verb is `%s` %s;\n     # %s\n",
			natsconf.DeClusterRemedyCmd, natsconf.DeClusterRemedyArgHint, natsconf.DeClusterRemedyOfflineNote)
	}
	_, _ = fmt.Fprintln(out, "  2. systemctl restart nats-server                       # load the conf from step 1")
	if seamOK {
		_, _ = fmt.Fprintf(out, "  3. ✓ broker.cluster seam is in %s — start the daemon: systemctl start tether-broker\n", configPath)
	} else {
		_, _ = fmt.Fprintf(out, "  3. FIX the broker.cluster seam in %s (see the error below), THEN: systemctl start tether-broker\n", configPath)
	}
	_, _ = fmt.Fprintln(out, "  4. tether cluster status                               # exit 1 DEGRADED (N=1, no redundancy) is expected here")
	_, _ = fmt.Fprintln(out, "  5. tether cluster join prepare / join approve          # re-grow to N>=3")
}

func newClusterExportIncidentCmd(socketPath *string) *cobra.Command {
	var out, since string
	var sids []string
	var forceOut bool
	cmd := &cobra.Command{
		Use:   "export-incident",
		Short: "Export a read-only forensic bundle (alert + membership timeline + per-session audit) with best-effort secret-key redaction (review before sharing)",
		Example: "  tether cluster recovery incident export --since 24h --out incident.json\n" +
			"  tether cluster recovery incident export --sid abc --sid def      # scope to specific sessions",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpExportIncident, Since: since, SIDs: sids})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("export-incident", resp)
			}
			blob, err := json.MarshalIndent(resp.Incident, "", "  ")
			if err != nil {
				return err
			}
			if out != "" {
				if err := writeIncidentFile(out, blob, forceOut); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "wrote incident bundle to %s\n", out)
				return nil
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(blob))
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "write the bundle JSON here (0600, O_EXCL — refuses to clobber/follow a symlink); empty = stdout")
	cmd.Flags().BoolVar(&forceOut, "force", false, "overwrite an existing --out file (still refuses to follow a symlink)")
	cmd.Flags().StringVar(&since, "since", "", "only alerts raised within this window (Go duration, e.g. 24h)")
	cmd.Flags().StringSliceVar(&sids, "sid", nil, "session id(s) to include audit for (default: ACTIVE sessions)")
	return cmd
}

// writeIncidentFile writes the incident bundle fail-closed (External-review F9): O_EXCL refuses to
// clobber an existing file (incident export runs as root/service — a planted symlink in a writable
// dir must not redirect the write onto a sensitive file), O_NOFOLLOW refuses to follow a symlink at
// the final path, and the file + its directory are fsync'd. --force allows an intentional overwrite
// but STILL refuses to follow a symlink.
func writeIncidentFile(path string, blob []byte, force bool) error {
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY | syscall.O_NOFOLLOW
	if force {
		// Overwrite intentionally, but never via a symlink: O_NOFOLLOW still applies; drop O_EXCL and
		// truncate. A symlink at `path` makes the open fail (ELOOP), which is what we want.
		flags = os.O_CREATE | os.O_TRUNC | os.O_WRONLY | syscall.O_NOFOLLOW
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return usageErr("refusing to overwrite existing %s (pass --force to overwrite; a symlink is never followed)", path)
		}
		return fmt.Errorf("export-incident: open --out %q (O_EXCL|O_NOFOLLOW): %w", path, err)
	}
	if _, err := f.Write(blob); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if d, derr := os.Open(filepath.Dir(path)); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
