package natsconf

// remedy.go (R10 P4) — the SINGLE SOURCE OF TRUTH for the "your nats.conf is still clustered but
// this node is a LONE VOTER" operator remedy.
//
// The same remedy is surfaced from three places that used to carry three hand-copied literals:
//
//  1. internal/broker/broker.go — the N=1 boot FATAL ("cluster mode requires JetStream …"),
//     i.e. the crash the operator is already staring at;
//  2. internal/broker/clusterstatus.go — the DATA-PLANE-DEGRADED status banner;
//  3. cmd/tether `cluster recovery restore` — the completion text, i.e. the ONE moment where
//     saying it BEFORE the crash actually prevents the crash (R10 P4 / #64).
//
// Keeping it a shared constant is not cosmetic: R10's finding was that the product "knew exactly
// what to say and only said it too late". Three divergent copies is how the late one gets updated
// and the early one rots.
const (
	// DeClusterRemedyCmd is the online verb (the daemon must be UP: --to-standalone proves N=1
	// from a live LEADER status view before it will de-cluster).
	DeClusterRemedyCmd = "tether cluster reconcile nats --to-standalone --confirm-single --server-name <self-server-name> --broker-nkey <self-bus-nkey>"

	// DeClusterRemedyArgHint explains where the two arguments come from (they are NOT in broker.yaml
	// — that mistake cost a live incident).
	DeClusterRemedyArgHint = "(server-name = the conf's server_name; broker-nkey = the bus nkey from the broker.nk seed in secrets_dir or cluster_nodes.bus_nkey_pub, NOT broker.yaml)"

	// DeClusterRemedyOfflineNote names the OFFLINE equivalent. It is load-bearing for disaster
	// recovery: after `cluster recovery restore` the daemon is STOPPED and cannot be started (a
	// clustered conf on a lone voter makes the broker FATAL at boot), so DeClusterRemedyCmd — which
	// requires a live leader view to prove N=1 — is structurally unreachable at that moment. The
	// manual takeover renders `Standalone: len(peers)==1`, so passing NO --peer emits exactly the
	// standalone-JetStream conf a lone voter needs, without touching the admin socket.
	DeClusterRemedyOfflineNote = "with the daemon STOPPED the online verb cannot prove N=1 — use the offline render instead: `tether cluster reconcile nats --manual` with NO --peer emits a STANDALONE (no cluster{}) conf"

	// DeClusterRemedyResetJSNote closes an R16 divergence this file's own doc comment predicted.
	// R16 (A4) added an ACKNOWLEDGEMENT GATE: --to-standalone now REFUSES when the JetStream store is
	// data-bearing, because swapping a clustered conf under a live clustered store strands it, and
	// resetting it drops audit/history. The grow-cutover remedy was updated to say --reset-js; THIS
	// one was not — so the DATA-PLANE-DEGRADED banner, which fires exactly when JetStream has been
	// serving and is therefore exactly when the store IS data-bearing, told the operator to run a
	// command that would refuse. (Caught on the deploy tier by drill 92 during G67, because 92 was
	// not re-run inside R16.)
	//
	// The refusal is a guard rail, not a dead end — it names the flag, the data impact, and the
	// `nats stream backup` escape — so the command above is left UNCHANGED (advising an unconditional
	// data-dropping flag in a banner would be worse). This note just removes the surprise.
	DeClusterRemedyResetJSNote = "if this node's JetStream store still holds data the command above will REFUSE and tell you to add `--reset-js`, which MOVES the store aside (never deletes) and drops live audit/history — run `nats stream backup` first if you need it"
)
