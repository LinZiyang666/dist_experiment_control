package main

import (
	"github.com/spf13/cobra"
)

// cluster_recovery.go (C6 建议7 → C8 inversion) — the `cluster recovery` command namespace is now the
// PRIMARY home of the recovery/escape verbs (C8 flipped C6's backwards alias-vs-primary). The top-level
// `force-single`/`recover`/`restore`/`export-incident`/`remove` are kept ONE release as HIDDEN
// deprecated aliases (deprecatedClusterAlias, cluster.go) that warn to stderr + delegate to these same
// constructors, then are deleted. Each child here is a FRESH constructor instance of the unchanged
// command (a *cobra.Command cannot have two parents), so every safety gate (--confirm-peers-dead /
// --dump-divergent / typed-confirm / never-`--yes` / O_EXCL / required --manual) is byte-preserved.
//
// diagnose / force-single / rejoin prepare / restore are OFFLINE (daemon stopped; restore replays a
// backup bundle on disk). incident export / node remove are last-resort ONLINE ops (admin socket).
func newClusterRecoveryCmd(socketPath *string) *cobra.Command {
	root := &cobra.Command{
		Use:   "recovery",
		Short: "Recovery + raw escape hatches (diagnose / force-single / rejoin / restore / incident / node)",
		Long: "Recovery namespace (canonical home of the escape hatches):\n" +
			"  recovery diagnose            read-only peer-liveness probe (OFFLINE)\n" +
			"  recovery force-single        promote this node to a single-voter cluster (OFFLINE)\n" +
			"  recovery rejoin prepare      dump divergent state + emit a rejoin manifest (OFFLINE)\n" +
			"  recovery restore <bundle>    restore from a backup bundle (last-resort)\n" +
			"  recovery incident export     assemble a redacted incident bundle (ONLINE)\n" +
			"  recovery node remove <id>    RAW raft-config removal --manual (ONLINE; prefer `cluster retire`)\n",
		Example: "  tether cluster recovery diagnose --self-id brk-a --db /var/lib/tether/tether.db   # read-only: are peers dead?\n" +
			"  tether cluster recovery force-single --self-id brk-a --confirm-peers-dead brk-b,brk-c\n" +
			"  tether cluster recovery rejoin prepare --self-id brk-a --emit-manifest rejoin.json",
	}

	// recovery diagnose --offline --self-id X — the read-only peer-probe behind force-single --guided.
	var diagDB, diagSelfID string
	var diagOffline bool
	diagnose := &cobra.Command{
		Use:     "diagnose",
		Short:   "Read-only: probe whether peers are dead before a force-single (nothing is executed)",
		Args:    cobra.NoArgs,
		Example: "  tether cluster recovery diagnose --self-id brk-a --db /var/lib/tether/tether.db",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return forceSingleGuided(cmd, diagDB, diagSelfID, "")
		},
	}
	diagnose.Flags().StringVar(&diagSelfID, "self-id", "", "this node's node_id (the one that would stay alive) — REQUIRED")
	diagnose.Flags().StringVar(&diagDB, "db", defaultDBPath, "tether.db path (the on-disk roster to read)")
	// --offline is an accepted-but-inert doc-parity marker: diagnose is ALWAYS offline + read-only.
	diagnose.Flags().BoolVar(&diagOffline, "offline", false, "(always offline + read-only; accepted for doc parity)")
	_ = diagOffline

	// recovery force-single — the existing force-single command (gates byte-identical; only the parent differs).
	forceSingle := newClusterForceSingleCmd()

	// recovery rejoin prepare — `cluster recover` re-parented under a `rejoin` group as `prepare`.
	rejoin := &cobra.Command{
		Use:   "rejoin",
		Short: "Re-join a wiped node to a surviving cluster (prepare = dump divergent state + emit manifest)",
	}
	prepare := newClusterRecoverCmd()
	prepare.Use = "prepare" // == the former `cluster recover`; same --dump-divergent/--emit-manifest/typed-confirm
	prepare.Short = "Dump this node's divergent state + emit a rejoin manifest"
	prepare.Example = "  tether cluster recovery rejoin prepare --self-id brk-a --dump-divergent /root/div.json --emit-manifest rejoin.json"
	rejoin.AddCommand(prepare)

	// recovery restore <bundle> — the former top-level `cluster restore`.
	restore := newClusterRestoreCmd()

	// recovery incident export — the former top-level `cluster export-incident`.
	incident := &cobra.Command{
		Use:   "incident",
		Short: "Incident tooling (export = a redacted, replicated-source bundle for support)",
	}
	export := newClusterExportIncidentCmd(socketPath)
	export.Use = "export"
	export.Example = "  tether cluster recovery incident export --since 24h --out incident.json"
	incident.AddCommand(export)

	// recovery node remove <id> --manual — the former top-level `cluster remove` (now fail-closed escape).
	node := &cobra.Command{
		Use:   "node",
		Short: "Raw node operations (remove = last-resort raft-config removal; prefer `cluster retire`)",
	}
	nodeRemove := newClusterRemoveCmd(socketPath)
	nodeRemove.Use = "remove <node-id>"
	node.AddCommand(nodeRemove)

	root.AddCommand(diagnose, forceSingle, rejoin, restore, incident, node)
	return root
}
