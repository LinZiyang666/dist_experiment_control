package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
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
	var dataDir, dbPath, secretsDir, confirmNodeID, raftAddr string
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
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"restore complete: node %s is now a single-voter cluster (pruned %d stale peers; bundle applied_index %d reset to 0).\n"+
					"prior DB preserved at: %s\n"+
					"NEXT: start tether-broker, then `cluster join approve` to re-grow to N>=3.\n",
				res.SelfID, res.PrunedPeers, res.BundleAppliedIdx, preserved)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (raft/ is wiped + re-bootstrapped)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path (the install destination)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", defaultClusterSecretsDir, "the LIVE secrets dir (the un-forgeable provenance anchor)")
	cmd.Flags().StringVar(&confirmNodeID, "confirm-node-id", "", "the node_id this bundle is for (must match the manifest)")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "override the bundle's raft addr (host:7400) for a FRESH-HOST restore where this box's IP changed (default: the bundle's address)")
	registerYesRejector(cmd)
	return cmd
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
