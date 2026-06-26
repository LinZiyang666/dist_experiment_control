package main

import (
	"fmt"
	"strings"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/spf13/cobra"
)

// cluster_offline_wizard.go — B7 DOC#7: the recovery/force-single GUIDED front-ends. They are
// PRINTERS, not runners — they diagnose (read-only) and emit the exact pasteable command, then
// exit. They add NO disk-mutating path; the unchanged ForceSingle/Recover hard gates +
// typed-confirm stay the only mutating path (never `allowMachineEscape`).

func forceSingleGuided(cmd *cobra.Command, dbPath, selfID, selfAddr string) error {
	if selfID == "" {
		return usageErr("force-single --guided requires --self-id (the node staying alive)")
	}
	out := cmd.OutOrStdout()
	diags, err := clusteroffline.DiagnosePeers(dbPath, selfID)
	if err != nil {
		return fmt.Errorf("force-single --guided: diagnose: %w", err)
	}
	_, _ = fmt.Fprintf(out, "force-single diagnosis for %q (read-only; nothing executed):\n\n", selfID)
	if len(diags) == 0 {
		_, _ = fmt.Fprintln(out, "  no peers in the on-disk roster — this is already a single-node cluster.")
		return nil
	}
	for _, d := range diags {
		state := "DEAD (no port answered)"
		if d.Alive {
			state = "ALIVE — answered on " + d.AliveOn
		}
		_, _ = fmt.Fprintf(out, "  peer %-14s %s\n", d.NodeID, state)
	}
	if clusteroffline.AnyAlive(diags) {
		_, _ = fmt.Fprintf(out, "\n  CANNOT force-single: at least one peer is still ALIVE. force-single there would SPLIT THE BRAIN.\n"+
			"  Power off / isolate the alive peer(s) and confirm they are PERMANENTLY dead, then re-run.\n")
		return usageErr("force-single blocked: a peer is still reachable")
	}
	ids := strings.Join(clusteroffline.DeadPeerIDs(diags), ",")
	addr := selfAddr
	if addr == "" {
		addr = "<this-node-host:7400>"
	}
	_, _ = fmt.Fprintf(out, "\n  All peers are dead. Run this on THIS node (daemon STOPPED) — you will be asked to type %q to confirm:\n\n"+
		"  tether cluster recovery force-single --self-id %s --self-addr %s --confirm-peers-dead %s\n", selfID, selfID, addr, ids)
	return nil
}

func recoverGuided(cmd *cobra.Command, selfID, dumpPath, manifestPath, secretsDir string) error {
	if selfID == "" {
		return usageErr("recover --guided requires --self-id (the node being wiped)")
	}
	out := cmd.OutOrStdout()
	if dumpPath == "" {
		dumpPath = "/root/divergent-" + selfID + ".json"
	}
	if manifestPath == "" {
		manifestPath = "/root/" + selfID + "-ident.json"
	}
	sec := secretsDir
	if sec == "" {
		sec = "/etc/tether/secrets"
	}
	_, _ = fmt.Fprintf(out, "recover guide for %q (read-only; nothing executed). Run on THIS node (daemon STOPPED):\n\n", selfID)
	_, _ = fmt.Fprintf(out, "  1. Dump forensics + capture this node's identity, then wipe:\n"+
		"     tether cluster recovery rejoin prepare --self-id %s --dump-divergent %s --emit-manifest %s --secrets-dir %s\n\n", selfID, dumpPath, manifestPath, sec)
	_, _ = fmt.Fprintf(out, "  2. Re-init from the captured identity (cert_fp re-derived live):\n"+
		"     tether cluster init --from-manifest %s --secrets-dir %s\n\n", manifestPath, sec)
	_, _ = fmt.Fprintf(out, "  3. On THIS node emit a join bundle, then approve it on the LEADER:\n"+
		"     tether cluster join prepare --node-id %s --raft-addr <this-host:7400> --nats-route nats://<this-host:6222> --tunnel-addr <this-host:7000>\n"+
		"     # then on the LEADER: tether cluster join approve <bundle> --wait\n", selfID)
	return nil
}
