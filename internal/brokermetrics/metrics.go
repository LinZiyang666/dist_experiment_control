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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
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
}

// Render writes the Prometheus text exposition for s. Every gauge that applies is ALWAYS present
// (emit 0, never omit a series a scraper expects); cluster/peer gauges are omitted only in single
// mode. Label values are escaped per the exposition format.
func Render(w io.Writer, s Snapshot) {
	g := func(name, help string, v int64) {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
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
	g("tether_broker_xfer_unreapable_buckets", "Number of xfer buckets holding aged orphan objects that no broker's home-gated reaper will ever delete (split-home / zero-node session; N-6).", int64(s.XferUnreapableBuckets))
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
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("brokermetrics: listen %q: %w", addr, err)
	}
	return ln, nil
}

// ServeListener serves /metrics, /healthz, /readyz on ln until ctx is canceled. snap is a
// request-time closure (cheap, lazy — never reads at wiring time); ready returns (ok, reason).
// /metrics recovers from a snapshot panic (so a transient snapshot bug never crashes the broker
// process); /healthz is a constant 200 once the listener is up (it never snapshots, so it stays up
// regardless).
func ServeListener(ctx context.Context, ln net.Listener, snap func() Snapshot, ready func() (bool, string)) error {
	srv := &http.Server{Handler: Handler(snap, ready), ReadHeaderTimeout: 5 * time.Second}
	context.AfterFunc(ctx, func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	})
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("brokermetrics: serve: %w", err)
	}
	return nil
}

// Handler builds the /metrics + /healthz + /readyz mux (extracted so it is httptest-able).
func Handler(snap func() Snapshot, ready func() (bool, string)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer func() { _ = recover() }() // a snapshot panic must not crash the broker process
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		Render(w, snap())
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
