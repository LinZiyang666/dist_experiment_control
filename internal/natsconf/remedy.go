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
)
