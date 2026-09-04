// Package brokermetrics is the B5 OPS#1 Prometheus /metrics + /healthz + /readyz endpoint.
//
// It is a leaf: imports only the standard library (net/http, io, context, net, fmt, sort). The
// broker passes in a Snapshot (cheap, in-process raft-accessor reads — NEVER the 2s-blocking
// StatusReport scatter-gather) via a request-time closure, plus a readiness predicate. Off by
// default: when the broker's MetricsAddr is empty NONE of this runs (byte-equivalence).
//
// Security: the Snapshot carries ONLY public topology (leader flag, voter count, raft indices,
// per-peer lag, alert COUNT) — no nkeys/seeds/tokens/cert private material. The renderer
// whitelists fields; it never reflects a struct. The listener is NOT loopback-forced (operators
// scrape over a private interface), documented as low-sensitivity reconnaissance.
package brokermetrics

import (
	"bytes"
	"context"
	"fmt"
	"github.com/LinZiyang666/tether/internal/httplisten"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
)

// PeerSnap is one voter's observed health (up to ~5s stale — sourced from the leader's cached
// observe tick, never a live scrape-time poll).
type PeerSnap struct {
	NodeID     string
	AppliedLag uint64
	Reachable  bool
}

// Snapshot is the broker's cheap, in-process metrics view. ClusterMode=false ⇒ a single broker:
// the cluster/peer gauges are OMITTED entirely (not faked to 0 — a flat applied_index 0 reads as
// a stuck raft).
type Snapshot struct {
	ClusterMode  bool
	IsLeader     bool
	Voters       int
	QuorumMargin int // ProjectQuorum fault-tolerance (further failures tolerated)
	AppliedIndex uint64
	CommitIndex  uint64
	ForceSingle  bool
	AlertsActive int
	Peers        []PeerSnap
	// B6 OPS#4: aggregate JS stream replica posture (the minimum observed actual + its target).
	// StreamsTarget==0 means "not observed this scrape" → both gauges are omitted.
	StreamsActual int
	StreamsTarget int
	// XferUnreapableBuckets (external review N-6) is the reap pass's count of OBJ_xfer buckets that hold
	// aged orphan objects yet are home-owned by no broker (split-home / zero-node session) → immortal
	// garbage (the racknerd small-disk-fill class). Cluster-mode gauge; always emitted (emit 0) so a
	// scraper can alert on it rising. Observability only — it never means the reaper deletes them.
	XferUnreapableBuckets int

	// Batch-A A15: the audit pipeline's three loss counters. They existed on
	// AuditPublisher and were read by nobody — so when `tether history` showed a
	// hole, there was no way to tell "nothing happened then" from "the audit was
	// truncated and we dropped it". A gap you cannot distinguish from silence is
	// not an observability nicety; it decides whether an incident is
	// investigable.
	AuditTruncationLoss    int64
	AuditLagExceeded       int64
	AuditDeletedStreamLoss int64

	// ForwardOutcomes (batch B, B2) counts every authoritative raft write attempt by
	// (verb, outcome). Keys are "<verb>/<outcome>" with outcome in {ok, not_leader, error};
	// nil or empty in single mode, where nothing is forwarded.
	//
	// WHY THIS IS THE ONE OPERATOR-VISIBLE THING B2 ADDS
	//
	// Two forward paths discard their error entirely — internal/broker/alert_forward.go's
	// disk-alert signal and topology_reconcile.go's bus-nkey self-report both call
	// `_ = fwd.Forward(...)` because a level-triggered re-assert self-heals on the next tick.
	// That is the right retry policy and the wrong observability: a broker whose forwards fail
	// EVERY tick looks identical to one with nothing to say. Counting the outcome makes a
	// persistently-failing forward visible without changing the retry behaviour.
	//
	// not_leader is deliberately its own outcome rather than folded into error: it is the
	// routine leadership-race signal (cluster.ErrForwardNotLeader) that callers retry, so a
	// rising not_leader rate means election churn while a rising error rate means a real
	// rejection. Collapsing them would hide exactly the distinction an operator needs.
	ForwardOutcomes map[string]int64
}

// Render writes the Prometheus text exposition for s. Every gauge that applies is ALWAYS present
// (emit 0, never omit a series a scraper expects); cluster/peer gauges are omitted only in single
// mode. Label values are escaped per the exposition format.
func Render(w io.Writer, s Snapshot) {
	g := func(name, help string, v int64) {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
	}
	// c renders a monotonically-increasing counter. Batch-A review m5: the three
	// audit-loss series below were introduced with the `_total` suffix — the
	// Prometheus counter convention — but rendered through g as gauges. A scraper
	// reading TYPE gauge will not apply rate()/increase() correctly to them.
	// Purely additive series, so fixing the type now costs nothing.
	c := func(name, help string, v int64) {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	g("tether_broker_cluster_mode", "1 if this broker runs in clustered HA mode, else 0.", b2i(s.ClusterMode))
	g("tether_broker_alerts_active", "Number of ACTIVE cluster alerts.", int64(s.AlertsActive))
	if !s.ClusterMode {
		return // single broker: no raft/peer gauges — never fabricate HA.
	}
	g("tether_broker_is_leader", "1 if this broker is the current raft leader, else 0.", b2i(s.IsLeader))
	g("tether_broker_voters", "Number of raft voters in the cluster.", int64(s.Voters))
	g("tether_broker_quorum_margin", "Further voter failures the cluster tolerates while keeping quorum.", int64(s.QuorumMargin))
	g("tether_broker_applied_index", "This broker's command-domain applied index.", int64(s.AppliedIndex))
	g("tether_broker_commit_index", "This broker's raft commit index.", int64(s.CommitIndex))
	g("tether_broker_force_single", "1 if the force_single_active escape-hatch marker is set, else 0.", b2i(s.ForceSingle))
	c("tether_broker_audit_truncation_loss_total", "Audit records dropped because the history stream was truncated ahead of the publisher (A15).", s.AuditTruncationLoss)
	c("tether_broker_audit_lag_exceeded_total", "Times the audit publisher exceeded its lag budget (A15).", s.AuditLagExceeded)
	c("tether_broker_audit_deleted_stream_loss_total", "Audit records dropped because their target stream had been deleted (A15).", s.AuditDeletedStreamLoss)
	g("tether_broker_xfer_unreapable_buckets", "Number of xfer buckets holding aged orphan objects that no broker's home-gated reaper will ever delete (split-home / zero-node session; N-6).", int64(s.XferUnreapableBuckets))
	// batch B / B2: forward outcomes by (verb, outcome). Rendered as ONE labelled counter series
	// rather than a gauge per key, because the key set is data (17 verbs x 3 outcomes) and a
	// scraper needs rate() over it. Keys are sorted so the exposition is byte-stable across
	// scrapes — an unsorted map would make the output non-deterministic and defeat any golden test.
	if len(s.ForwardOutcomes) > 0 {
		keys := make([]string, 0, len(s.ForwardOutcomes))
		for k := range s.ForwardOutcomes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n",
			"tether_broker_raft_forward_total",
			"Authoritative raft write attempts by verb and outcome (ok|not_leader|error).",
			"tether_broker_raft_forward_total")
		for _, k := range keys {
			verb, outcome, found := strings.Cut(k, "/")
			if !found {
				continue
			}
			// \"%s\" + escapeLabel, matching the peer series below. %q would double-escape,
			// since escapeLabel already handles backslash / quote / newline.
			_, _ = fmt.Fprintf(w, "tether_broker_raft_forward_total{verb=\"%s\",outcome=\"%s\"} %d\n",
				escapeLabel(verb), escapeLabel(outcome), s.ForwardOutcomes[k])
		}
	}
	// B6 OPS#4: JS stream replica posture (actual<target ⇒ replication degraded). Omitted when
	// not observed this scrape (StreamsTarget==0) — a faked 0 would read as a degraded cluster.
	if s.StreamsTarget > 0 {
		g("tether_broker_stream_replicas_actual", "Minimum observed JS stream replica count (actual).", int64(s.StreamsActual))
		g("tether_broker_stream_replicas_target", "Target JS stream replica count.", int64(s.StreamsTarget))
	}

	// Per-peer gauges (labelled), deterministic order. Up to ~5s stale (cached observe).
	peers := append([]PeerSnap(nil), s.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i].NodeID < peers[j].NodeID })
	_, _ = fmt.Fprintf(w, "# HELP tether_broker_peer_applied_lag Per-peer command-domain applied-index lag behind the leader (up to ~5s stale).\n# TYPE tether_broker_peer_applied_lag gauge\n")
	for _, p := range peers {
		_, _ = fmt.Fprintf(w, "tether_broker_peer_applied_lag{node=\"%s\"} %d\n", escapeLabel(p.NodeID), p.AppliedLag)
	}
	_, _ = fmt.Fprintf(w, "# HELP tether_broker_peer_reachable 1 if the peer answered the last health poll, else 0 (up to ~5s stale).\n# TYPE tether_broker_peer_reachable gauge\n")
	for _, p := range peers {
		_, _ = fmt.Fprintf(w, "tether_broker_peer_reachable{node=\"%s\"} %d\n", escapeLabel(p.NodeID), b2i(p.Reachable))
	}
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// escapeLabel escapes a Prometheus label value (backslash, double-quote, newline).
func escapeLabel(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}

// Bind binds the metrics listener synchronously (plain net.Listen — NOT loopback-forced, unlike
// subhttp.Bind: the endpoint vends no secret, and operators scrape over a private interface). The
// caller propagates a bind error from broker startup so an occupied port fails the broker rather
// than leaving a healthy broker with a dead metrics port.
func Bind(addr string) (net.Listener, error) {
	// requireLoopback=false ON PURPOSE: /metrics is meant to be scraped from a
	// private interface. It is the one surface of the three that may leave
	// loopback — TestHTTPSurfaceLoopbackPolicy pins that asymmetry.
	return httplisten.Bind("brokermetrics", addr, false)
}

// ServeListener serves /metrics, /healthz, /readyz on ln until ctx is canceled. snap is a
// request-time closure (cheap, lazy — never reads at wiring time); ready returns (ok, reason).
// /metrics recovers from a snapshot panic (so a transient snapshot bug never crashes the broker
// process); /healthz is a constant 200 once the listener is up (it never snapshots, so it stays up
// regardless).
func ServeListener(ctx context.Context, ln net.Listener, snap func() Snapshot, ready func() (bool, string)) error {
	return httplisten.Serve(ctx, ln, Handler(snap, ready), "brokermetrics")
}

// Handler builds the /metrics + /healthz + /readyz mux (extracted so it is httptest-able).
func Handler(snap func() Snapshot, ready func() (bool, string)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var panicked any
		// A snapshot panic must not crash the broker process — but it must not be reported
		// as a healthy scrape either.
		//
		// origin: prerelease audit, §3 MINOR sweep. The bare recover swallowed the panic
		// AFTER the 200 header had been written, so Prometheus received 200 with a body
		// that was empty or cut off mid-metric. Every alert built on those series then
		// evaluates against "no data" instead of firing, which is the worst possible
		// failure for a monitoring surface: it fails OPEN and looks fine.
		//
		// Render into a buffer first. A panic then happens BEFORE any header is written,
		// so the handler can answer 500 — a scrape failure Prometheus reports as `up 0`.
		var buf bytes.Buffer
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = r
				}
			}()
			Render(&buf, snap())
		}()
		if panicked != nil {
			http.Error(w, "metrics snapshot failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(buf.Bytes())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ok, reason := ready()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready: "+reason+"\n")
			return
		}
		_, _ = io.WriteString(w, "ready: "+reason+"\n")
	})
	return mux
}
