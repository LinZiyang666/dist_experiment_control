package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_status_nats.go (B1 item 1) — the ctl-reachable `tether cluster status --remote` path.
//
// A logged-in ctl on a laptop has no broker admin socket, so it cannot run the operator
// `cluster status` (which talks to the broker-local Unix socket on a broker host). This path
// instead broadcasts the EXISTING member-reachable cluster-health probe
// (tether.v2.ctrl.by.<actor>.cluster-health.req — already granted to members by
// PermissionsForActivatedMember, so NO new subject and NO new ACL) and folds the lightweight
// per-broker replies into a USER-LEVEL summary. It deliberately returns a coarse, reachability-
// based picture: NOT the 8-column roster table, NOT the exact voter count (the replies don't carry
// it), and NOT the operator escape hatches — those stay socket-gated on a broker host. At N=1 (no
// responder) the probe returns zero replies and this prints "no cluster detected" with exit 0,
// byte-equivalent to the single-broker world.

// ctlClusterSummary is a ctl-side render artifact folded from the broadcast cluster-health
// replies. It is NEVER on the wire (the wire type is proto.ClusterHealthResp). The "view" field
// distinguishes the shape from the socket/offline ClusterStatusReport. External-review Q3: it now
// ALSO carries the (schema, schema_version) discriminator the rest of the B2 machine-JSON contract
// uses, so a monitor expecting that pair finds it here too (the earlier B1 F6 view-only decision is
// superseded — the additive fields are harmless and the consistency is worth it).
type ctlClusterSummary struct {
	Schema             string `json:"schema"`               // "ctl_cluster_summary"
	SchemaVersion      int    `json:"schema_version"`       // 1
	View               string `json:"view"`                 // always "ctl-remote" (distinguishes from the socket ClusterStatusReport)
	BrokersSeen        int    `json:"brokers_seen"`         // distinct non-empty NodeIDs that answered
	WritableLeaderSeen bool   `json:"writable_leader_seen"` // some reply passed the VerifyLeaderRead barrier
	LeaderID           string `json:"leader_id,omitempty"`  // best-effort leader id; deliberately captured ONLY from a writable reply (a stale follower-named leader is not trusted)
	ForceSingleActive  bool   `json:"force_single_active"`  // some reply reports the persisted force-single marker
	AllStale           bool   `json:"all_stale"`            // every reply reports stale leader contact (mirrors EvalDestructiveGate)
	AnyReply           bool   `json:"any_reply"`            // at least one broker answered
	// JetStreamUnavailable (G7b #20③) is true when SOME broker self-reports a sustained JS-503 — the
	// data plane (push/pull, history, audit) is wedged even though the control plane answered. Surfaced
	// in the verdict/banner; does NOT change the 0/2/3 exit taxonomy (it is a hint, not a new exit class).
	JetStreamUnavailable bool `json:"jetstream_unavailable,omitempty"`
}

// summarizeClusterHealth folds the broadcast replies into the ctl summary. It dedups by distinct
// non-empty NodeID so a single flapping broker that answers several times cannot fake a quorum-
// sized BrokersSeen. AllStale mirrors EvalDestructiveGate's allStale (true only when EVERY reply
// reports stale leader contact) — and is forced false on zero replies (vacuous-truth guard).
func summarizeClusterHealth(replies []proto.ClusterHealthResp) ctlClusterSummary {
	s := ctlClusterSummary{Schema: "ctl_cluster_summary", SchemaVersion: 1, View: "ctl-remote", AllStale: true}
	seen := map[string]bool{}
	for _, r := range replies {
		s.AnyReply = true
		if r.NodeID != "" {
			seen[r.NodeID] = true
		}
		if r.WritableLeaderConfirmed {
			s.WritableLeaderSeen = true
			if r.LeaderID != "" {
				s.LeaderID = r.LeaderID
			}
		}
		if !r.LeaderContactStale {
			s.AllStale = false
		}
		if r.ForceSingleActive {
			s.ForceSingleActive = true
		}
		if r.JetStreamUnavailable {
			s.JetStreamUnavailable = true // G7b #20③: some broker's JetStream is wedged at 503
		}
	}
	if !s.AnyReply {
		s.AllStale = false // no replies must not read as "all stale"
	}
	s.BrokersSeen = len(seen)
	return s
}

// brokersDisplayCount is the count shown to the user: distinct non-empty NodeIDs, but at least 1
// when SOME broker answered (the responder always stamps NodeID, so this only guards a malformed
// reply with an empty id).
func (s ctlClusterSummary) brokersDisplayCount() int {
	if s.AnyReply && s.BrokersSeen == 0 {
		return 1
	}
	return s.BrokersSeen
}

// ctlVerdictLine renders the plain-language, REACHABILITY-based verdict for the ctl view. It is
// deliberately NOT phrased as an authoritative voter count ("N brokers answered" — the leader
// holds the real count + HA verdict). Force-single leads (it is the emergency the user most needs
// to know), then writable-leader, then read-only, then the transient electing case.
func ctlVerdictLine(s ctlClusterSummary) string {
	n := s.brokersDisplayCount()
	switch {
	case !s.AnyReply:
		return "no cluster detected (single broker, or no broker reachable)."
	case s.ForceSingleActive:
		// Accurate regardless of how many brokers answered: the marker is replicated, so a
		// straggler that hasn't cleared it after recovery would otherwise make this falsely say
		// "running on one broker". State it as "at least one broker reports …" (B1 review F5) and
		// avoid printing a bare operator command token (F9 — the read-only redirect is in the
		// view: footer; this line describes state, never nudges `cluster force-single`).
		return "at least one broker reports emergency mode (force_single_active) — the cluster may be running with no redundancy; check the leader on a broker host."
	case s.WritableLeaderSeen && n >= 3:
		return fmt.Sprintf("%d brokers answered with a writable leader — looks healthy. (Exact voter count + HA verdict are authoritative on the leader.)", n)
	case s.WritableLeaderSeen:
		return fmt.Sprintf("%d broker(s) answered with a writable leader; fewer than 3 answered — re-run on the leader for the exact voter count + HA verdict.", n)
	case s.AllStale:
		return "no writable leader visible — the cluster appears READ-ONLY right now."
	default:
		return "the cluster is electing a leader (transient) — re-run shortly."
	}
}

// ctlExitCode is the ctl-path exit code. It is intentionally THINNER than the operator socket
// taxonomy (no exit 1 / DEGRADED — ctl has no roster to judge replica/role health). Precedence:
// zero replies (a single-broker laptop user must not get a nonzero alarm) → 0; force_single_active
// (emergency, more informative than "writable") → 3; writable leader → 0; all-stale read-only → 2;
// electing (transient) → 0. A richer ctl taxonomy is deferred to B2.
func ctlExitCode(s ctlClusterSummary) int {
	switch {
	case !s.AnyReply:
		return 0
	case s.ForceSingleActive:
		return 3
	case s.WritableLeaderSeen:
		return 0
	case s.AllStale:
		return 2
	default:
		return 0 // electing (transient)
	}
}

// renderCtlStatus writes the ctl summary (no os.Exit — the caller exits on ctlExitCode). jsonMode
// emits the ctlClusterSummary as JSON.
func renderCtlStatus(w io.Writer, s ctlClusterSummary, jsonMode bool) {
	if jsonMode {
		b, _ := json.MarshalIndent(s, "", "  ")
		_, _ = fmt.Fprintln(w, string(b))
		return
	}
	if !s.AnyReply {
		_, _ = fmt.Fprintln(w, "cluster: no broker answered over NATS")
		_, _ = fmt.Fprintf(w, "verdict: %s\n", ctlVerdictLine(s))
		_, _ = fmt.Fprintln(w, "note:    single-broker is fully supported — see usage §1. If you DO run an HA")
		_, _ = fmt.Fprintln(w, "         cluster, check that you are logged in and the brokers are reachable.")
		return
	}
	n := s.brokersDisplayCount()
	_, _ = fmt.Fprintf(w, "cluster: %d broker(s) answered over NATS\n", n)
	_, _ = fmt.Fprintf(w, "verdict: %s\n", ctlVerdictLine(s))
	if s.JetStreamUnavailable {
		// G7b #20③: the control plane answered but a broker's JetStream is wedged at 503 — the data
		// plane (file transfer / history / audit) is DOWN. Loud + actionable so a multi-day silent rot
		// (the racknerd incident) is impossible to miss from a laptop.
		_, _ = fmt.Fprintln(w, "ALERT:   DATA-PLANE DEGRADED — a broker reports JetStream UNAVAILABLE (sustained 503).")
		_, _ = fmt.Fprintln(w, "         File transfers / history / audit are failing. Common cause: a survivor's nats.conf")
		_, _ = fmt.Fprintln(w, "         left clustered after force-single. Run `tether cluster status` on the broker host for")
		_, _ = fmt.Fprintln(w, "         the remedy (`reconcile nats --to-standalone`).")
	}
	leader := s.LeaderID
	if leader == "" {
		leader = "unknown"
	}
	_, _ = fmt.Fprintf(w, "view:    remote summary aggregated from %d broker(s); the leader (%s) holds the\n", n, leader)
	_, _ = fmt.Fprintln(w, "         authoritative verdict — run `tether cluster status` on a broker host for the")
	_, _ = fmt.Fprintln(w, "         full per-node table and exact voter count.")
}

// clusterStatusRemote is the thin shell: dial NATS with the ctl session identity (mirrors
// `tether alert ls`), broadcast the cluster-health probe, fold + render the summary, then exit on
// the ctl exit code. It is the only function here that touches NATS / os.Exit; the pure cores
// above carry the testable logic.
func clusterStatusRemote(cmd *cobra.Command, home, natsURL string, asJSON bool) error {
	sid := cli.ReadCurrentSession(home)
	if sid == "" {
		return fmt.Errorf("no active session — run `tether login -s <sid>` first (cluster status --remote needs a ctl identity)")
	}
	natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return err
	}
	nc, err := connectCtl(cmd, "cluster status", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return err
	}
	defer nc.Close()
	s := summarizeClusterHealth(probeClusterHealth(nc, id.PublicKey))
	renderCtlStatus(cmd.OutOrStdout(), s, asJSON)
	os.Exit(ctlExitCode(s))
	return nil
}

// brokerHomeCount is one broker's aggregate __proxy__ home count (G7b #16). NEVER on the wire — folded
// ctl-side from ClusterHealthResp.ProxyHomeCount; carries no per-SID identity.
type brokerHomeCount struct {
	NodeID   string `json:"node_id"`
	Homes    int    `json:"homes"`
	Reported bool   `json:"reported"` // External-review m1: false ⇒ a pre-G7 broker that did not report — unknown, not 0
}

// foldProxyHomeCounts dedups the health replies by NodeID (a flapping broker answering twice cannot
// double-count) and returns the per-broker __proxy__ home counts, sorted by NodeID for a stable render.
// A broker that did not report the count (pre-G7) is kept with Reported=false so the render shows "?"
// instead of silently counting it as 0 (which would understate the distribution in a mixed-version window).
func foldProxyHomeCounts(replies []proto.ClusterHealthResp) []brokerHomeCount {
	seen := map[string]proto.ClusterHealthResp{}
	for _, r := range replies {
		if r.NodeID != "" {
			if _, dup := seen[r.NodeID]; !dup {
				seen[r.NodeID] = r
			}
		}
	}
	out := make([]brokerHomeCount, 0, len(seen))
	for id, r := range seen {
		out = append(out, brokerHomeCount{NodeID: id, Homes: r.ProxyHomeCount, Reported: r.ProxyHomeReported})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// renderHomesRemote writes the per-broker proxy-home distribution. Descriptive read — NO exit-code
// contract (mirrors the socket `--homes` view, which also has none).
func renderHomesRemote(w io.Writer, homes []brokerHomeCount, jsonMode bool) {
	if jsonMode {
		b, _ := json.MarshalIndent(struct {
			Schema        string            `json:"schema"`
			SchemaVersion int               `json:"schema_version"`
			View          string            `json:"view"`
			Brokers       []brokerHomeCount `json:"brokers"`
		}{"ctl_homes_summary", 1, "ctl-remote", homes}, "", "  ")
		_, _ = fmt.Fprintln(w, string(b))
		return
	}
	if len(homes) == 0 {
		_, _ = fmt.Fprintln(w, "homes: no broker answered over NATS (single-broker, or not logged in / brokers unreachable)")
		return
	}
	_, _ = fmt.Fprintf(w, "proxy home distribution (over NATS, %d broker(s)):\n", len(homes))
	_, _ = fmt.Fprintf(w, "  %-24s %s\n", "BROKER", "PROXY_HOMES")
	total, unknown := 0, 0
	for _, h := range homes {
		if h.Reported {
			_, _ = fmt.Fprintf(w, "  %-24s %d\n", h.NodeID, h.Homes)
			total += h.Homes
		} else {
			// External-review m1: a pre-G7 broker did not report — show unknown, do NOT sum as 0.
			_, _ = fmt.Fprintf(w, "  %-24s ?  (broker predates the count report)\n", h.NodeID)
			unknown++
		}
	}
	if unknown > 0 {
		_, _ = fmt.Fprintf(w, "  %-24s %d (+%d unknown)\n", "(total)", total, unknown)
	} else {
		_, _ = fmt.Fprintf(w, "  %-24s %d\n", "(total)", total)
	}
	_, _ = fmt.Fprintln(w, "view:  aggregate per-broker counts from the connected brokers; run `cluster status --homes` on a broker host for the full per-expose table.")
}

// clusterStatusHomesRemote (G7b #16) renders the per-broker __proxy__ home distribution over NATS — the
// `--homes --remote` variant, so the spread is checkable from a laptop without SSHing to a broker. It
// reuses the member-granted cluster-health broadcast (no new subject/ACL) and reports only aggregate
// per-broker COUNTS (no per-SID identity). Descriptive: no exit-code contract, no os.Exit.
func clusterStatusHomesRemote(cmd *cobra.Command, home, natsURL string, asJSON bool) error {
	sid := cli.ReadCurrentSession(home)
	if sid == "" {
		return fmt.Errorf("no active session — run `tether login -s <sid>` first (cluster status --homes --remote needs a ctl identity)")
	}
	natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return err
	}
	nc, err := connectCtl(cmd, "cluster status", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return err
	}
	defer nc.Close()
	renderHomesRemote(cmd.OutOrStdout(), foldProxyHomeCounts(probeClusterHealth(nc, id.PublicKey)), asJSON)
	return nil
}
