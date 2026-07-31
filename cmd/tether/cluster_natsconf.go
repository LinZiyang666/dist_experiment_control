package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
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
	cmd.Flags().StringVar(&f.confPath, "conf", defaultNatsConfPath, "nats-server.conf to take over")
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
// warnStandaloneJSGrow tells the operator that THIS node ran standalone JetStream and the
// clustered restart will orphan its streams unless the JS store is reset — with the accurate
// data impact (the wipe drops ALL audit/history; the re-derive cursor survives it). The runbook
// §1 step 3a is the authoritative procedure; this fires on the manual takeover / `--plan` paths.
// standaloneJSMigrationHazard reports whether THIS node is in the one state warnStandaloneJSGrow is
// about: no cluster{} block AND JetStream actually enabled, so a clustered restart would orphan real
// streams unless the store is reset first.
//
// BOTH conjuncts are load-bearing, and the second one was missing (external review RB2-1). The gate
// used to be the compound `IsStandaloneJetStream()`, which was true on mere key presence — so a broker
// with `jetstream: false` or `jetstream: disabled` was told:
//
//	sudo systemctl stop nats-server && sudo rm -rf <jetstream store_dir from nats.conf>
//
// There is no active standalone JS meta to migrate on such a node, and a dormant store directory may
// hold the only recoverable copy of its history. The command printed a destructive instruction, with a
// placeholder path because JSStoreDir() was empty, on a premise that was false.
func standaloneJSMigrationHazard(own *natsconf.Ownership) bool {
	return own.IsStandaloneTopology() && own.HasJetStream()
}

func warnStandaloneJSGrow(w io.Writer, storeDir string) {
	if storeDir == "" {
		storeDir = "<jetstream store_dir from nats.conf>"
	}
	_, _ = fmt.Fprintf(w,
		"\n⚠ STANDALONE-JS → CLUSTERED-JS on THIS node (the former N=1 broker): NATS does NOT migrate\n"+
			"  standalone JS into the clustered meta — restarting in place ORPHANS every pre-existing\n"+
			"  stream (the meta forms, but the streams go invisible to it). RESET the JS store first:\n"+
			"      sudo systemctl stop nats-server && sudo rm -rf %s && sudo systemctl start nats-server\n"+
			"  ⚠ DATA IMPACT: drops ALL JetStream audit/history (history-<sid> incl. the forensic incident\n"+
			"  bundle, the events stream, in-flight OBJ_xfer — drain those first). There is no separate\n"+
			"  audit stream and the re-derive cursor survives the wipe, so it does NOT backfill. To\n"+
			"  PRESERVE it: `nats stream backup` before / `restore` after. Fresh new voters need NO\n"+
			"  reset. See cluster-runbook.md §1 step 3a.\n",
		storeDir)
}

// R16 A4: warnClusteredJSShrink (the old `rm -rf` advisory for the clustered→standalone shrink) is RETIRED
// — `reconcile nats --to-standalone --reset-js` now MOVES the store aside (never deletes) as a product step.

// runReconcileToStandalone is the v0.4.2 SHRINK de-cluster step: re-render the lone survivor's
// nats.conf WITHOUT the cluster{} block (standalone JetStream), validate it, swap it, and surface
// the clustered→standalone JS-store-reset warning. SEMANTIC: this is the FINAL N=1 step — running it
// while peers remain would tear the route mesh, so the operator must `cluster retire` down to a
// single voter first and assert it via --confirm-single. A FULL nats-server restart is required
// afterwards (dropping cluster{} is not SIGHUP-reloadable).
func runReconcileToStandalone(cmd *cobra.Command, f *natsconfTakeoverFlags, confirmSingle, resetJS bool) error {
	own, err := natsconf.Preflight(f.confPath)
	if err != nil {
		return err
	}
	if own.IsStandaloneTopology() { // TOPOLOGY (RB2-1): "is there a cluster{} block to remove"
		// M4: an already-standalone CONF can still sit over a clustered JS STORE (a pre-R16 / hand-de-clustered
		// host — racknerd-legacy — or a crash after the conf swap but before the store move). With --reset-js,
		// forward-complete JUST the gated store move (the conf is already standalone → skip render/apply; there
		// is no route mesh to tear, so the F2 N=1 gate below is moot for a store-only reset). Without --reset-js,
		// the old refusal, but now naming the remedy so R16's own guidance never dead-ends.
		if resetJS {
			storeDir := own.JSStoreDir()
			if storeDir == "" {
				return fmt.Errorf("reconcile nats --to-standalone --reset-js: %q is standalone but declares no JetStream store_dir — nothing to reset", f.confPath)
			}
			moved, mErr := natsconf.MoveAsideJSStore(storeDir, storeDir+".standalone-bak."+nowStampUTC(), "", true /*acked*/)
			if mErr != nil {
				return fmt.Errorf("reconcile nats --to-standalone --reset-js: %w", mErr)
			}
			if moved != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"conf already standalone; the clustered JS store was reset to standalone (moved aside to %s — NEVER deleted). "+
						"A FULL `systemctl restart nats-server` is REQUIRED so nats boots on the fresh store.\n", moved)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "conf already standalone and the JS store is already empty — nothing to reset.\n")
			}
			return nil
		}
		return fmt.Errorf("reconcile nats --to-standalone: %q is already standalone (no cluster{} block) — nothing to de-cluster "+
			"(pass --reset-js if the clustered JS store still needs resetting to standalone)", f.confPath)
	}
	if !confirmSingle {
		return fmt.Errorf("reconcile nats --to-standalone: de-clustering is the FINAL N=1 step — `cluster retire` down to a SINGLE voter first, then re-run with --confirm-single (de-clustering with live peers would tear the route mesh)")
	}
	// F3 fail-closed: the source enables clustered JetStream, but natsconf.Render OMITS jetstream{}
	// entirely when store_dir is empty — so a source `jetstream {}` with no explicit store_dir would
	// SILENTLY disable JetStream on the next restart while this command claims standalone JS. Mirror
	// the grow-takeover guard: refuse unless an explicit store_dir is present (and reuse it below).
	storeDir := own.JSStoreDir()
	if storeDir == "" {
		return fmt.Errorf("reconcile nats --to-standalone: source JetStream has no explicit store_dir — refusing (a standalone render would silently disable JetStream); set store_dir in the jetstream{} block first")
	}
	// F2 fail-CLOSED machine gate: --confirm-single is the typed intent, but typed intent is NOT proof
	// of cluster size. De-clustering is destructive (it tears this node out of the route mesh), so we
	// REFUSE unless a live status report proves EXACTLY ONE voter. Socket error / no report / unknown
	// role / voters!=1 all refuse (the broker daemon is UP during this command — only nats-server
	// restarts — so the socket MUST answer). This applies even to --plan: an operator must never be
	// shown "standalone is safe" without the live proof.
	rep, e := fetchClusterStatusReport(f.takeoverSocket)
	if e != nil || rep == nil {
		return fmt.Errorf("reconcile nats --to-standalone: cannot confirm N=1 (admin socket unreachable: %v) — de-clustering is destructive and fail-closed; retire to a single voter and ensure the leader is reachable", e)
	}
	// R2: "one visible voter" is NOT by itself destructive authorization — a follower may hold a stale
	// raft config, and a partial/errored report says its node list is incomplete. Only a COMPLETE
	// LEADER view proves removing cluster{} won't tear a still-live route mesh. Require leader view +
	// non-partial + no errors, and every node an EXPLICITLY recognized role (an unknown/empty role
	// could hide a voter), then exactly one voter.
	if !rep.IsLeaderView {
		return fmt.Errorf("reconcile nats --to-standalone: status is not a leader view — run on the leader; cannot prove N=1, refusing")
	}
	if rep.Partial || len(rep.Errors) > 0 {
		return fmt.Errorf("reconcile nats --to-standalone: status is partial/errored (partial=%v, %d errors) — cannot prove N=1, refusing", rep.Partial, len(rep.Errors))
	}
	voters := 0
	for _, nd := range rep.Nodes {
		switch nd.Role {
		case "leader", "voter":
			voters++
		case "nonvoter":
			// a known non-voting role — does not count toward the voter total
		default:
			return fmt.Errorf("reconcile nats --to-standalone: node %q has an unrecognized raft role %q — cannot prove N=1, refusing", nd.NodeID, nd.Role)
		}
	}
	if voters != 1 {
		return fmt.Errorf("reconcile nats --to-standalone: the cluster has %d voters (need EXACTLY 1) — `cluster retire` down to N=1 first", voters)
	}
	serverName := f.serverName
	accountIssuer, brokerNkey := f.accountIssuer, f.brokerNkey
	ci, cn := own.AuthIdentity()
	if accountIssuer == "" {
		accountIssuer = ci
	}
	if brokerNkey == "" {
		brokerNkey = cn
	}
	if serverName == "" {
		return fmt.Errorf("reconcile nats --to-standalone: --server-name required (the lone broker's server_name)")
	}
	if accountIssuer == "" || brokerNkey == "" {
		return fmt.Errorf("reconcile nats --to-standalone: need --account-issuer + --broker-nkey (auto-read from the existing auth_callout when it carries exactly one nkey user)")
	}
	clientListen := own.ClientListen()
	if clientListen == "" {
		return fmt.Errorf("reconcile nats --to-standalone: could not determine the client listen address from %q", f.confPath)
	}
	// B5: one shared assembly. IntentForceStandalone because --to-standalone IS the operator's intent —
	// the N=1 proof above is what earns it, and inference from the (still clustered) live conf would
	// render the opposite. The monitor is left to RenderDesired's harvest rather than hand-copied here:
	// this path does not restart nats-server, so it can only ever PRESERVE an existing loopback monitor.
	self := natsconf.Broker{ServerName: serverName, NkeyPub: brokerNkey}
	merged, err := natsconf.RenderDesired(
		natsconf.Inputs{
			SelfServerName: serverName,
			// a lone survivor advertises only itself (standalone JS, no routes)
			Peers:         []natsconf.Broker{self},
			AccountIssuer: accountIssuer,
		},
		own,
		natsconf.RenderOverride{Intent: natsconf.IntentForceStandalone},
	)
	if err != nil {
		return err
	}
	if f.plan {
		dry := "ok"
		if f.skipDryRun {
			dry = "skipped"
		} else if e := natsconf.DryRun(f.natsServerBin, merged); e != nil {
			dry = e.Error()
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "--to-standalone plan: would render a STANDALONE (no cluster{}) conf for %s; nats-server -t: %s\n", serverName, dry)
		// R16 A4 preview: the clustered JS store must be reset to standalone (NATS does not migrate it in
		// place). Name --reset-js, never the old rm -rf advisory.
		if resetJS {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "  --reset-js: would MOVE the clustered JS store %q aside to <store>.standalone-bak.<ts> (NEVER deleted) so the standalone nats boots on a fresh store.\n", storeDir)
		} else {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "  NOTE: the clustered JS store %q must be reset to standalone — re-run WITHOUT --plan and WITH --reset-js to MOVE it aside (never deleted). `nats stream backup` first to preserve it live.\n", storeDir)
		}
		return nil
	}
	if f.skipDryRun {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "WARNING: --skip-dry-run set — swapping nats.conf WITHOUT `nats-server -t` validation")
	} else if err := natsconf.DryRun(f.natsServerBin, merged); err != nil {
		return err
	}
	// R16 A4: reset the clustered JS store to standalone (move-aside, never delete) BEFORE swapping the conf
	// — a refusal (non-empty store without --reset-js) then leaves the conf UNTOUCHED (a clean, re-runnable
	// refusal), never a standalone conf stranded over a clustered store the "already standalone" guard would
	// refuse to fix on re-run. Retires the old `rm -rf` advisory. Not journalled (--to-standalone is a single
	// online op); a crash between this move and the Apply below re-runs cleanly (conf still clustered).
	jsBackup := storeDir + ".standalone-bak." + nowStampUTC()
	moved, mErr := natsconf.MoveAsideJSStore(storeDir, jsBackup, "", resetJS)
	if mErr != nil {
		// origin: line-2 closure verification m9 — see the sibling branch in cluster_offline.go.
		if !errors.Is(mErr, natsconf.ErrJSStoreNeedsAck) {
			return fmt.Errorf("reconcile nats --to-standalone: the clustered JS store could not be inspected or "+
				"moved, and it must be reset to standalone before the conf is swapped — %w\n"+
				"  --reset-js will NOT help here: the flag only acknowledges a data-bearing store, and this is a "+
				"failure to read or write it. Fix the condition named above, then re-run", mErr)
		}
		return fmt.Errorf("reconcile nats --to-standalone: the clustered JS store must be reset to standalone before the "+
			"conf is swapped — %w\n"+
			"  re-run with --reset-js (moves %q aside, NEVER deleted; `nats stream backup` first to preserve it live).\n"+
			"  ⚠ DATA IMPACT: drops ALL JetStream audit/history (history-<sid>, events, in-flight OBJ_xfer) from the live store", mErr, jsBackup)
	}
	if err := natsconf.Apply(f.confPath, merged); err != nil {
		return err
	}
	// F3 post-apply proof: re-parse the swapped conf and assert it is standalone JetStream with the
	// SAME store_dir — never silently JS-less, never a moved store. Apply preserves the pristine .bak.
	if post, perr := natsconf.Preflight(f.confPath); perr != nil {
		return fmt.Errorf("reconcile nats --to-standalone: applied conf failed to re-parse: %w", perr)
	} else if post.IsClusteredTopology() {
		// TOPOLOGY (RB2-1) — same reasoning as the offline post-check: what --to-standalone owes is
		// "the cluster{} block is gone". store_dir equality below is the separate JetStream-side proof.
		return fmt.Errorf("reconcile nats --to-standalone: applied conf still carries a cluster{} block — aborting (the .bak is preserved for rollback)")
	} else if post.JSStoreDir() != storeDir {
		return fmt.Errorf("reconcile nats --to-standalone: applied conf store_dir %q != source %q (refusing to move the JS store)", post.JSStoreDir(), storeDir)
	}
	jsNote := ""
	if moved != "" {
		jsNote = fmt.Sprintf(" The clustered JS store was reset to standalone (moved aside to %s — NEVER deleted; restore by hand or `nats stream restore`).", moved)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"nats.conf de-clustered to STANDALONE (pristine .bak kept). A FULL `systemctl restart nats-server` "+
			"is REQUIRED — dropping the cluster{} block is NOT SIGHUP-reloadable.%s\n", jsNote)
	return nil
}

// nowStampUTC is a compact UTC timestamp for move-aside backup dir names (jetstream.*-bak.<ts>).
// nowStampUTC mints the per-attempt idempotency key embedded in a move-aside backup path.
//
// EXTERNAL REVIEW F5: it used to be second-precision, and MoveAsideJSStore treats an existing backup
// path as proof that THIS exact move already happened. Two attempts inside the same second therefore
// shared a key — and that is a reachable sequence, not a theoretical one: the first move succeeds, the
// Apply that follows fails, the still-running nats-server writes into the recreated store, and a retry
// within the same second matches the old backup and SKIPS the now-live data. Nanoseconds plus a random
// suffix make the key unique per attempt, so a retry always mints a distinct identity.
func nowStampUTC() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Randomness is a collision belt-and-braces on top of nanoseconds; a failure must not break the
		// operation, and nanosecond precision alone already separates two sequential attempts.
		return time.Now().UTC().Format("20060102-150405.000000000")
	}
	return time.Now().UTC().Format("20060102-150405.000000000") + "-" + hex.EncodeToString(b[:])
}

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
		self := natsconf.Broker{ServerName: serverName, NkeyPub: brokerNkey, RouteURL: routeURL}
		// External-review F2: render the FULL peer mesh, not just self. Each --peer is a
		// "server_name,route_url,bus_nkey" triple for ANOTHER broker; growth re-runs takeover
		// on EVERY node with the complete set so routes + auth_users + per-broker ACLs form a
		// real multi-node NATS mesh (Raft membership alone does not). N=1 takeover passes no
		// --peer and renders just self.
		peers := []natsconf.Broker{self}
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
		// audit natsconf F2: if the existing conf ENABLES JetStream but no store_dir survived
		// (empty), REFUSE rather than render a conf that silently DISABLES JetStream (the next
		// restart would lose all streams). Fail-closed; install.sh always writes store_dir, so
		// the common path is unaffected.
		//
		// EXTERNAL REVIEW B2-1: this used to read `_, hasJS := own.Parsed["jetstream"]` — raw KEY PRESENCE,
		// which is not enablement. nats-server treats `jetstream: false|disabled|off|no` as an explicit
		// DISABLE, so a conf whose operator had deliberately switched JetStream off hit this refusal and was
		// told to "set jetstream.store_dir explicitly" — i.e. to turn back on the subsystem they turned off.
		// The manual takeover is the documented recovery/cutover path, so that made it permanently
		// unavailable for a supported configuration shape. Ownership.HasJetStream is the ONE value-aware
		// enablement predicate; every enablement decision in the codebase now goes through it (the other one
		// is BuildMergedConf's BUG-1 guard, which already did). The fail-closed refusal is unchanged for
		// `jetstream: true|enabled` and for a bare `jetstream {}` with no resolvable store_dir — those DO
		// enable it, and those are what the guard exists for.
		if own.HasJetStream() && own.JSStoreDir() == "" {
			return fmt.Errorf("natsconf takeover: existing conf enables jetstream but has no resolvable store_dir; " +
				"refusing to render a conf that would silently DISABLE JetStream — set jetstream.store_dir explicitly")
		}
		// B5: one shared assembly. This is the site that MOTIVATED IntentStandaloneIfLone — it is a
		// fourth decision, not a variant of the other three.
		//
		// audit D: a lone node (no --peer, just self) MUST render Standalone — a clustered conf with
		// empty routes makes nats-server FATAL ("JetStream cluster requires configured routes") at boot
		// while `nats-server -t` passes it. The grow re-renders clustered once the peer mesh is supplied.
		// IntentForceStandalone would de-cluster a lone-peer takeover over an ALREADY-clustered conf;
		// IntentPreserve would render clustered-with-zero-routes and hit Render's refusal. Hence the
		// named intent, whose three cases are tabled in natsconf/render_intent_test.go.
		//
		// C3-B3: FORCE the loopback HTTP monitor so this manual cutover (the ONE restart-bearing step)
		// ESTABLISHES `http:`; the per-broker reconciler then PRESERVES it (it can never hot-add a
		// monitor via SIGHUP). Keep this addr in sync with the broker's topoMonitorListen.
		//
		// SecretsDir supplies the same three route-mTLS filenames this file used to spell out by hand;
		// ClusterListen is the operator's --cluster-listen and wins over the derived one.
		merged, err := natsconf.RenderDesired(
			natsconf.Inputs{SelfServerName: serverName, Peers: peers, AccountIssuer: accountIssuer},
			own,
			natsconf.RenderOverride{
				Intent:        natsconf.IntentStandaloneIfLone,
				MonitorListen: "127.0.0.1:8223",
				SecretsDir:    secretsDir,
				ClusterListen: clusterListen,
			},
		)
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
			if standaloneJSMigrationHazard(own) {
				warnStandaloneJSGrow(cmd.ErrOrStderr(), own.JSStoreDir())
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
		if standaloneJSMigrationHazard(own) {
			warnStandaloneJSGrow(cmd.ErrOrStderr(), own.JSStoreDir())
		}
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
func missingVotersInMesh(socket string, peers []natsconf.Broker) (missing []string, checked bool) {
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
func parsePeerSpec(spec string) (natsconf.Broker, error) {
	parts := strings.Split(spec, ",")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return natsconf.Broker{}, fmt.Errorf("takeover-natsconf: --peer %q must be server_name,route_url,bus_nkey", spec)
	}
	if err := validateRouteURL("--peer", parts[1]); err != nil {
		return natsconf.Broker{}, err
	}
	return natsconf.Broker{ServerName: parts[0], RouteURL: parts[1], NkeyPub: parts[2]}, nil
}

// onlineIssuerSkewChecks runs the auth_callout issuer/broker-nkey skew cross-check on the ONLINE
// doctor path (batch B / B4 — TODO(n3-online-doctor)).
//
// WHICH CONF IT READS, in order:
//
// First an EXPLICIT --conf, because the operator overrode it and explicit always wins. Otherwise the
// running broker's reported path, which is the file the daemon actually renders and reads.
//
// If neither is available it does NOT guess. The CLI's --conf DEFAULT is right only on a stock host;
// on a custom-conf host it names a different (or absent) file, and a FATAL "issuer skew" derived from
// the wrong file is worse than no check at all. It reports that the check did not run instead — the
// N-3 asymmetry: a KNOWN mismatch fails closed, an ABSENT check is loud but non-fatal.
func onlineIssuerSkewChecks(cmd *cobra.Command, secretsDir, flagConf, reportedConf string) []clusteroffline.DoctorCheck {
	conf := reportedConf
	if f := cmd.Flags().Lookup("conf"); f != nil && f.Changed {
		conf = flagConf
	}
	if conf == "" {
		return []clusteroffline.DoctorCheck{{
			Name:   "auth-issuer-unverified",
			Status: clusteroffline.DoctorAdvisory,
			Detail: "auth_callout issuer cross-check SKIPPED: the running broker did not report its " +
				"nats.conf path (a broker older than this CLI, or one started without one) — pass " +
				"--conf <path> to run it. This is NOT proof the rendered issuer matches the on-disk seeds",
		}}
	}
	return clusterAuthIssuerSkewChecks(secretsDir, conf)
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
					// batch B / B4 closes TODO(n3-online-doctor). The ONLINE branch used to skip
					// clusterAuthIssuerSkewChecks entirely, so a rotated-but-not-re-rendered issuer was
					// invisible on a LIVE cluster — the mode a rotated cluster actually runs in, and the
					// one an operator reaches for after a rotation. The blocker the TODO recorded was
					// that the CLI only had its own --conf DEFAULT, which on a custom-conf host names the
					// wrong file: running the check anyway meant a permanent ADVISORY on every online
					// doctor. The TODO named the fix ("reads the conf path from the running broker's
					// config"); ClusterStatusReport.NatsConfPath is that, and onlineIssuerSkewChecks
					// below prefers it over the CLI default.
					online := clusterDoctorOnline(rep)
					online = append(online, onlineIssuerSkewChecks(cmd, secretsDir, confPath, rep.NatsConfPath)...)
					return renderDoctor(cmd, online, asJSON)
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
			// R11 P6/#54 facet 2: the syntax/perms checks above never noticed that a rotated
			// account.nk/broker.nk no longer matches the issuer rendered into nats.conf. Cross-check
			// the on-disk seeds against the live conf so an auth_callout skew is FATAL, not invisible.
			checks = append(checks, clusterAuthIssuerSkewChecks(secretsDir, confPath)...)
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
		if asJSON {
			// R11 deploy-tier fix (drill 52 B3/55c): in --json mode the JSON on stdout already carries
			// summary.fatal, so exit non-zero QUIETLY — do NOT let the main sink append an "error: ..."
			// line to stderr. A caller that merges the streams (`cluster doctor --json 2>&1 | jq`) would
			// otherwise get the JSON followed by prose and fail to parse it (jq: parse error), so a
			// correctly-detected FATAL would be invisible to the machine consumer.
			return &ExitError{Class: exitUsage, Quiet: true,
				Err: fmt.Errorf("cluster doctor: %d FATAL check(s)", fatal)}
		}
		return usageErr("cluster doctor: %d FATAL check(s) — fix them before `cluster init`", fatal)
	}
	return nil
}

// renderTakeoverPlan renders the B5 OPS#10 `takeover-natsconf --plan` dry-run: what the takeover
// WOULD change, without writing anything. `changed` compares the rendered merged conf against the
// current file bytes (the honest "would this rewrite the file?" signal). routes/auth_users come
// from the resolved peer mesh; ownership_regenerated lists the sections takeover always rewrites.
func renderTakeoverPlan(cmd *cobra.Command, confPath, serverName, clientListen, jsStoreDir string, peers []natsconf.Broker, merged, dryRun string, asJSON bool) error {
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
