package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// cluster_offline.go — D7 §8.4 OFFLINE + local cluster verbs. force-single / recover
// run with the daemon STOPPED and touch disk directly via internal/clusteroffline
// (which calls cluster.RecoverSingleNode; raft never enters cmd/tether). They NEVER
// honor --yes — a typed node_id is mandatory (§8.1).

const (
	defaultDataDir = "/var/lib/tether"
	defaultDBPath  = "/var/lib/tether/tether.db"
	// defaultSeed MUST equal <secrets_dir>/node-ident.nk: the broker's cluster-mode preflight
	// (SecretsPreflight) requires node-ident.nk INSIDE the secrets dir, so keygen/node-pub/join-prepare
	// must default there too — else the documented `keygen` flow mints the seed at /etc/tether/node-ident.nk
	// and the broker FATALs at start on a missing secrets/node-ident.nk (audit finding F).
	defaultSeed         = defaultClusterSecretsDir + "/node-ident.nk"
	// #22 (G1): the reconciler-managed nats.conf lives in the tether-owned /etc/tether/nats.d/ subdir
	// so the User=tether in-broker reconciler can atomically rewrite it; /etc/tether itself stays
	// root-owned (the root-run caddy reads /etc/tether/Caddyfile — a tether-owned /etc/tether would be
	// a tether->root privesc). SSOT for the default across init / reconcile / retire / preflight / serve.
	defaultNatsConfPath = "/etc/tether/nats.d/nats.conf"
)

func newClusterForceSingleCmd() *cobra.Command {
	var dataDir, dbPath, selfID, selfAddr string
	var confirmDead []string
	var guided bool
	var online, dryRun bool
	cmd := &cobra.Command{
		Use:   "force-single",
		Short: "DANGER (split-brain risk): force this node to a lone single-voter cluster — quorum-loss escape hatch only; daemon STOPPED; type node_id to confirm",
		Long: `force-single is the quorum-loss escape hatch — and the single most dangerous
cluster command. It rewrites this node into a lone single-voter cluster: if ANY listed
peer is actually alive (merely partitioned, not dead) it SPLITS THE BRAIN into two
divergent timelines, and there is NO HA / NO integrity until you recover.

STOP the daemon first (systemctl mask tether-broker && systemctl stop tether-broker). It
HARD-REFUSES if any peer is still reachable on :7400, if there is no existing raft state, or
if a daemon still holds the store. It NEVER accepts --yes; you must type this node's id to
confirm (and the split-brain consequence is shown at the prompt).`,
		Example: "  # quorum-loss drill (the majority is PERMANENTLY dead):\n" +
			"  systemctl mask tether-broker && systemctl stop tether-broker\n" +
			"  tether cluster recovery force-single --self-id brk-a --self-addr 10.0.0.1:7400 --confirm-peers-dead brk-b,brk-c\n" +
			"  systemctl unmask tether-broker && systemctl start tether-broker   # then recover each returning node",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// ONLINE: the preferred path — recover via the running broker's admin socket WITHOUT stopping
			// it (no second outage). --dry-run is a zero-mutation drill runnable on a HEALTHY cluster.
			if online {
				socket, _ := cmd.Flags().GetString("socket")
				return runForceSingleOnline(cmd, socket, selfID, confirmDead, dryRun)
			}
			// B7 DOC#7: --guided diagnoses (read-only) and PRINTS the exact command — it auto-derives
			// the --confirm-peers-dead list + TCP-probes each peer, and BLOCKS if any is still alive.
			// It executes nothing (the unchanged hard gates stay the only mutating path).
			if guided {
				return forceSingleGuided(cmd, dbPath, selfID, selfAddr)
			}
			if err := rejectedUnattendedYes(cmd, "force-single", selfID); err != nil {
				return err
			}
			if selfID == "" || selfAddr == "" {
				return usageErr("force-single requires --self-id and --self-addr")
			}
			if len(confirmDead) == 0 {
				return usageErr("force-single requires --confirm-peers-dead listing EVERY other node_id (a missed peer would split-brain)")
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"force-single will rewrite %q to a single-voter cluster (NO HA / NO integrity until recover).\n"+
					"Confirm the peers %v are TRULY dead (powered off / unreachable to agents), not merely partitioned.\n",
				selfID, confirmDead)
			if !confirmTypedNodeID(cmd, selfID,
				"CONSEQUENCE: no HA + no integrity until you recover; if ANY listed peer is alive (merely partitioned) this SPLITS THE BRAIN into two divergent timelines.",
				false, "") { // never-escapable: an env var is not "attended" for a brain-split-capable op
				return fmt.Errorf("aborted (type this node's id to confirm; --yes is never accepted)")
			}
			abandoned, err := clusteroffline.ForceSingle(clusteroffline.ForceSingleOptions{
				DataDir: dataDir, DBPath: dbPath, SelfID: selfID, SelfRaftAddr: selfAddr, ConfirmedDead: confirmDead,
			})
			if err != nil {
				return fmt.Errorf("force-single: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"force-single complete: %q is now a single-voter cluster (%d nodes abandoned).\n"+
					"systemctl unmask tether-broker && systemctl start tether-broker, then recover the others.\n", selfID, len(abandoned))
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (holds raft/ + tether.db)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&selfID, "self-id", "", "this node's cluster node_id")
	cmd.Flags().StringVar(&selfAddr, "self-addr", "", "this node's raft address (host:7400)")
	cmd.Flags().StringSliceVar(&confirmDead, "confirm-peers-dead", nil, "node_ids of EVERY other roster node (comma-separated)")
	cmd.Flags().BoolVar(&guided, "guided", false, "diagnose + print the exact force-single command (auto-derives --confirm-peers-dead, probes peers); executes nothing (B7 DOC#7)")
	cmd.Flags().BoolVar(&online, "online", false, "recover via the RUNNING broker's admin socket — no daemon stop, no second outage (the preferred path)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "with --online: a zero-mutation drill (evaluate the gates + report) runnable on a HEALTHY cluster")
	registerYesRejector(cmd)
	return cmd
}

// runForceSingleOnline drives the online force-single arm->confirm->commit flow over the broker's admin
// socket. The broker (not this CLI) runs the dwell + peer-liveness gates; the unchanged TTY-typed node_id
// confirm sits between arm and commit. If the socket is unreachable (the broker is truly down) it prints
// the OFFLINE floor command. --dry-run stops after arm (zero mutation).
func runForceSingleOnline(cmd *cobra.Command, socket, selfID string, confirmDead []string, dryRun bool) error {
	if selfID == "" {
		return usageErr("force-single --online requires --self-id (the node_id you will type to confirm)")
	}
	// F2: the operator-confirmed --self-id is SENT to the broker (NodeID) so it can reject a mistyped
	// id (else a wrong --self-id on the right socket would prompt for the wrong node yet recover the
	// socket owner). Both arm and commit carry it.
	arm, err := callAdmin(socket, adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, NodeID: selfID, ConfirmPeersDead: confirmDead, DryRun: dryRun,
	})
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
			"online force-single: broker admin socket unreachable (%v).\n"+
				"The broker may be DOWN — use the OFFLINE floor (daemon stopped, disk surgery):\n"+
				"  systemctl mask tether-broker && systemctl stop tether-broker\n"+
				"  tether cluster recovery force-single --self-id %s --self-addr <host:7400> --confirm-peers-dead %s\n",
			err, selfID, strings.Join(confirmDead, ","))
		return err
	}
	if arm.ForceSingle != nil {
		printForceSingleReport(cmd, arm.ForceSingle)
	}
	if !arm.OK {
		return clusterAdminError("force-single", arm) // a gate refusal (peer alive / quorum not lost / ...)
	}
	if dryRun {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "dry-run: gates evaluated, NO changes made.")
		return nil
	}
	// Confirm against the id the BROKER reports it owns (not the locally-typed --self-id), so the
	// prompt names the node that will actually be force-singled. The broker already rejected a
	// mismatched --self-id above, so confirmTarget == selfID here; this is belt-and-braces.
	confirmTarget := selfID
	if arm.ForceSingle != nil && arm.ForceSingle.BrokerSelfID != "" {
		confirmTarget = arm.ForceSingle.BrokerSelfID
	}
	// Unchanged hands-on confirm: TTY-only, --yes rejected, never env-escapable (brain-split-capable op).
	if !confirmTypedNodeID(cmd, confirmTarget,
		"CONSEQUENCE: no HA + no integrity until you recover; if ANY listed peer is alive (merely partitioned) this SPLITS THE BRAIN into two divergent timelines.",
		false, "") {
		return fmt.Errorf("aborted (type this node's id to confirm; --yes is never accepted)")
	}
	commit, err := callAdmin(socket, adminsock.Request{
		Op: adminsock.OpClusterForceSingleCommit, NodeID: confirmTarget, ArmToken: arm.ForceSingle.ArmToken, ConfirmPeersDead: confirmDead,
	})
	if err != nil {
		return err
	}
	if !commit.OK {
		return clusterAdminError("force-single", commit)
	}
	abandoned := 0
	if commit.ForceSingle != nil {
		abandoned = len(commit.ForceSingle.Abandoned)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"online force-single complete: %q is now a single-voter cluster (%d node(s) abandoned), writable WITHOUT a broker restart.\n",
		selfID, abandoned)
	return nil
}

func printForceSingleReport(cmd *cobra.Command, r *adminsock.ForceSingleReport) {
	out := cmd.ErrOrStderr()
	if r.BrokerSelfID != "" {
		_, _ = fmt.Fprintf(out, "  broker (this socket): %s\n", r.BrokerSelfID)
	}
	for _, p := range r.Peers {
		mark := "confirmed-dead"
		if !p.Confirmed {
			mark = "NOT in --confirm-peers-dead"
		}
		if p.Alive {
			mark += fmt.Sprintf(" — ALIVE on %s (HARD-REFUSE: would split-brain)", p.OnPort)
		}
		_, _ = fmt.Fprintf(out, "  peer %s: %s\n", p.NodeID, mark)
	}
	if r.DwellRemaining != "" {
		_, _ = fmt.Fprintf(out, "  quorum-loss dwell remaining: %s\n", r.DwellRemaining)
	}
	if r.WouldProceed {
		_, _ = fmt.Fprintln(out, "  verdict: all gates pass — force-single WOULD proceed")
	} else if r.Reason != "" {
		_, _ = fmt.Fprintf(out, "  verdict: would NOT proceed — %s\n", r.Reason)
	}
}

// newClusterResnapshotCmd is the STEP-1 grow-onto-migrated-broker remediation: make an ALREADY-init'd
// single-voter migrated broker grow-ready (write a raft snapshot + compact the log so a fresh joiner
// installs the snapshot instead of replaying the log and FK-fail-stopping). Brokers init'd by
// `cluster init --from-existing` BEFORE the fix (e.g. the live pc732) have no snapshot + a short log
// and cannot grow until resnapshot'd. OFFLINE (daemon STOPPED). Single-voter only; audit-window guarded.
func newClusterResnapshotCmd() *cobra.Command {
	var dataDir, dbPath, selfID, selfAddr, confirmNodeID string
	var acceptAuditLoss bool
	cmd := &cobra.Command{
		Use:   "resnapshot",
		Short: "OFFLINE: make an already-init'd single-voter migrated broker grow-ready (raft snapshot + log compaction); daemon STOPPED",
		Long: `resnapshot writes a full FSM snapshot of THIS single-voter broker + compacts its raft log so a
future joiner catches up via InstallSnapshot (the full DB) instead of replaying the log from index 1
onto an un-seeded DB and FK-fail-stopping. It is the one-time remediation for a broker migrated by
` + "`cluster init --from-existing`" + ` BEFORE the grow-onto-migrated-broker fix (no snapshot + short log).

STOP the daemon first (systemctl stop tether-broker). SINGLE-VOTER only — it refuses if the roster has
any non-self node (it rewrites the raft config to {self} and would drop peers). It REFUSES if the log
carries UNPUBLISHED audit (audit_published_index < raft last_index): restart the daemon briefly so the
D5 publisher drains, stop it, and re-run — or pass --accept-audit-loss to accept a bounded loud loss.
The SQLite DB is preserved (the snapshot is a copy); only the raft log is compacted.`,
		Example: "  systemctl stop tether-broker\n" +
			"  TETHER_CONFIRM_NODE_ID=pc732 tether cluster recovery resnapshot --self-id pc732 --raft-addr 155.98.36.32:7400 --confirm-node-id pc732\n" +
			"  systemctl start tether-broker",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectedUnattendedYes(cmd, "resnapshot", selfID); err != nil {
				return err
			}
			if selfID == "" || selfAddr == "" {
				return usageErr("resnapshot requires --self-id and --raft-addr (this node's current raft advertise addr)")
			}
			if !confirmTypedNodeID(cmd, selfID,
				"CONSEQUENCE: rewrites the raft log (snapshot + compaction). The SQLite DB is preserved; only the raft log is compacted.",
				true, confirmNodeID) {
				return fmt.Errorf("resnapshot: aborted (type this node's id to confirm, or pass --confirm-node-id + $%s for unattended use)", machineConfirmEnv)
			}
			if err := clusteroffline.Resnapshot(clusteroffline.ResnapshotOptions{
				DataDir: dataDir, DBPath: dbPath, SelfID: selfID, SelfRaftAddr: selfAddr, AcceptAuditLoss: acceptAuditLoss,
			}); err != nil {
				return fmt.Errorf("resnapshot: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "resnapshot complete: %q is now grow-ready (start the daemon + `cluster join approve` the joiner).\n", selfID)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (holds raft/ + tether.db)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&selfID, "self-id", "", "this node's cluster node_id — REQUIRED")
	cmd.Flags().StringVar(&selfAddr, "raft-addr", "", "this node's CURRENT raft advertise addr (host:7400) — REQUIRED (preserved in the new config)")
	cmd.Flags().BoolVar(&acceptAuditLoss, "accept-audit-loss", false, "proceed even if unpublished audit would be truncated (bounded loud loss)")
	cmd.Flags().StringVar(&confirmNodeID, "confirm-node-id", "", "unattended confirm: must equal --self-id AND match $"+machineConfirmEnv+" (no TTY)")
	registerYesRejector(cmd)
	return cmd
}

func newClusterRecoverCmd() *cobra.Command {
	var dataDir, dbPath, dumpPath, selfID, emitManifest, secretsDir string
	var guided bool
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Wipe a returning node after a forensic divergence dump, so it can rejoin clean (daemon STOPPED)",
		Example: "  systemctl mask tether-broker && systemctl stop tether-broker\n" +
			"  tether cluster recovery rejoin prepare --self-id brk-b --dump-divergent /root/divergent-brk-b.json\n" +
			"  # then `cluster init --from-existing` on this node, and `cluster join approve` on the leader to rejoin",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// B7 DOC#7: --guided prints the sequenced rejoin-prepare→init→join-approve commands (read-only); executes nothing.
			if guided {
				return recoverGuided(cmd, selfID, dumpPath, emitManifest, secretsDir)
			}
			if err := rejectedUnattendedYes(cmd, "recover", selfID); err != nil {
				return err
			}
			if dumpPath == "" {
				return usageErr("recover requires --dump-divergent <file> (the forensic dump path)")
			}
			if selfID == "" {
				return usageErr("recover requires --self-id (the node_id being wiped, to confirm you target the right node)")
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"recover will DUMP node %q's divergent rows to %s then WIPE raft/ + tether.db.\n"+
					"The dump is forensic-only and NOT auto-mergeable.\n", selfID, dumpPath)
			// Confirm on the node_id (B3): typing a fixed word gives zero protection
			// against running recover against the wrong --data-dir/--db.
			if !confirmTypedNodeID(cmd, selfID, "", false, "") { // never-escapable
				return fmt.Errorf("aborted (type the node_id to confirm; --yes is never accepted)")
			}
			n, err := clusteroffline.Recover(clusteroffline.RecoverOptions{
				DataDir: dataDir, DBPath: dbPath, DumpPath: dumpPath,
				SelfID: selfID, ManifestPath: emitManifest, SecretsDir: secretsDir,
			})
			if err != nil {
				return fmt.Errorf("recover: %w", err)
			}
			reinit := "`cluster init --from-existing`"
			if emitManifest != "" {
				reinit = fmt.Sprintf("`cluster init --from-manifest %s`", emitManifest)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"recover complete: %d rows dumped to %s, node %q wiped. Re-run %s on this node before starting tether-broker, then run `cluster join approve` on the leader to rejoin.\n",
				n, dumpPath, selfID, reinit)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&dumpPath, "dump-divergent", "", "forensic dump output path (0600, must not pre-exist)")
	cmd.Flags().StringVar(&selfID, "self-id", "", "the node_id being wiped (typed to confirm)")
	cmd.Flags().StringVar(&emitManifest, "emit-manifest", "", "also capture this node's identity to a manifest (0600) for `cluster init --from-manifest`")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "", "secrets dir for the advisory account_fp in the emitted manifest (optional)")
	cmd.Flags().BoolVar(&guided, "guided", false, "print the sequenced rejoin-prepare→init→join-approve commands with real values; executes nothing (B7 DOC#7)")
	registerYesRejector(cmd)
	return cmd
}

func newClusterNodePubCmd() *cobra.Command {
	var seedPath string
	cmd := &cobra.Command{
		Use:     "node-pub",
		Short:   "Print this node's identity public key (debug; `cluster join prepare` derives it)",
		Example: "  tether cluster node-pub   # -> U…  (debug; join prepare derives this automatically)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			seed, err := os.ReadFile(seedPath)
			if err != nil {
				return fmt.Errorf("read node-ident seed %s: %w", seedPath, err)
			}
			pub, err := auth.PublicKeyFromSeed([]byte(strings.TrimSpace(string(seed))))
			if err != nil {
				return fmt.Errorf("derive pubkey: %w", err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), pub)
			return nil
		},
	}
	cmd.Flags().StringVar(&seedPath, "seed", defaultSeed, "node-identity seed file")
	return cmd
}

func newClusterKeygenCmd() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:     "keygen",
		Short:   "Generate a new node-identity key (prints the pubkey; writes the seed 0600)",
		Example: "  tether cluster keygen --out /etc/tether/node-ident.nk   # writes the seed, prints the pubkey",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			seed, err := auth.GenerateUserSeed()
			if err != nil {
				return err
			}
			pub, err := auth.PublicKeyFromSeed(seed)
			if err != nil {
				return err
			}
			if out != "" {
				if err := os.WriteFile(out, seed, 0o600); err != nil {
					return fmt.Errorf("write seed %s: %w", out, err)
				}
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), pub)
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "write the seed to this path (0600); empty = print pubkey only")
	return cmd
}

// machineConfirmEnv is the node-id-valued env var that, TOGETHER WITH a matching
// --confirm-node-id flag, lets a machine-confirm-ESCAPABLE op (only non-F==0 `cluster remove`)
// proceed unattended. It is deliberately NOT a boolean "yes-to-everything": it must EXACTLY
// equal the node_id being confirmed, so a stray exported env cannot auto-confirm an arbitrary op.
const machineConfirmEnv = "TETHER_CONFIRM_NODE_ID"

// confirmTypedNodeID requires the operator to TYPE want exactly (no --yes accepted). consequence,
// when non-empty, is printed immediately before the prompt (so the danger is at the moment of
// typing). It reads cmd.InOrStdin() so a unit test can inject input via cmd.SetIn(...); in
// production (stdin) it still HARD-REFUSES a non-interactive terminal — there is no unattended
// path for a quorum-affecting op (B3 item 4).
//
// allowMachineEscape is a PER-CALL-SITE capability (default false), NEVER a property of the
// shared funnel: the never-escapable ops (force-single / recover / F==0-drain / init / restore)
// pass false, so even a correct flag+env STILL falls through to the TTY refuse — an env var in a
// systemd unit or CI is not "attended" for an irreversible / quorum-destructive op. Only non-F==0
// `cluster remove` passes true. When escape IS allowed, it fires ONLY if BOTH the --confirm-node-id
// flag (flagNodeID) AND the env exactly equal want; either alone refuses (a stray env or a stray
// history flag must not be enough). --yes is never the escape (rejectedUnattendedYes is unchanged).
func confirmTypedNodeID(cmd *cobra.Command, want, consequence string, allowMachineEscape bool, flagNodeID string) bool {
	if allowMachineEscape {
		envID := os.Getenv(machineConfirmEnv)
		if flagNodeID != "" && envID != "" && flagNodeID == want && envID == want {
			return true
		}
		// A partial or mismatched escape attempt is NOT an error here — it just falls through to
		// the interactive path (which HARD-REFUSES a non-interactive terminal below).
	}
	in := cmd.InOrStdin()
	if in == os.Stdin && !term.IsTerminal(int(os.Stdin.Fd())) {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "refusing: this confirmation requires an interactive terminal (no --yes for destructive cluster ops)")
		return false
	}
	if consequence != "" {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), consequence)
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "type %q to confirm: ", want)
	var got string
	_, _ = fmt.Fscanln(in, &got)
	return got == want
}
