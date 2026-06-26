// Package subhttp is the P13 read-only subscription HTTP surface — tether's
// first (and intentionally minimal) outward HTTP endpoint. It serves a Clash
// config for GET /sub/<token>, listing every ONLINE, proxy-ready agent as a
// Shadowsocks node keyed by the subscriber's PSK.
//
// SECURITY POSTURE (architecture p13-plan §6): bound to loopback only (Caddy
// fronts it on :443), GET-only, READ-ONLY (SELECTs only — no NATS handle, no
// writes). A single generic 404 covers unknown / revoked / DELETING-session
// tokens so there is no existence oracle. The PSK is vended here (the consumer
// needs it); the bearer token is matched by hash.
package subhttp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
)

// maxTokenLen bounds the path segment we hash (defense against absurd inputs).
const maxTokenLen = 256

// requireLoopback rejects any listen address whose host is not loopback.
func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("subhttp: invalid listen address %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("subhttp: listen address %q must be loopback (127.0.0.1, ::1, or localhost) — Caddy fronts /sub/* on :443; a non-loopback bind would expose token/PSK vending over plaintext HTTP", addr)
	}
	return nil
}

// Config wires the handler to its read-only dependencies.
type Config struct {
	DB         *sql.DB
	PublicHost string // host printed in node entries (broker's public DNS name; the per-node FALLBACK)
	Logger     *slog.Logger

	// ClusterMode (C5) makes LiveProxyNodes resolve each __proxy__ node's AUTHORITATIVE home broker
	// public host (the home broker terminates that node's tunnel), instead of this broker's PublicHost.
	// A node whose home is not a VOTER is excluded (un-vendable until it rehomes). false ⇒ P13 single
	// broker, byte-identical.
	ClusterMode bool
	// Vendable (C5) is the injected /sub read gate. nil ⇒ always vend (single mode). In cluster mode the
	// broker provides a predicate that returns false ONLY in DISABLED_NO_QUORUM (disable-on-quorum-loss
	// + no committable quorum) — FROZEN_READONLY still vends the last committed state.
	Vendable func(sid string) bool
}

// Handler returns the http.Handler serving GET /sub/<token>.
func Handler(cfg Config) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/sub/", func(w http.ResponseWriter, r *http.Request) {
		serveSub(w, r, cfg)
	})
	return mux
}

func serveSub(w http.ResponseWriter, r *http.Request, cfg Config) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token := strings.TrimPrefix(r.URL.Path, "/sub/")
	// Reject nested paths and absurd lengths before any DB work.
	if token == "" || len(token) > maxTokenLen || strings.Contains(token, "/") {
		notFound(w)
		return
	}

	tokenHash := proxysub.HashToken(token)
	sid, err := proxysub.LookupSIDByTokenHash(cfg.DB, tokenHash)
	if err != nil {
		// Unknown OR revoked → identical 404 (no existence oracle).
		notFound(w)
		return
	}
	// Session must be ACTIVE (a DELETING session yields the same 404).
	if active, err := session.IsActive(cfg.DB, sid); err != nil || !active {
		notFound(w)
		return
	}
	// C5 vend gate: DISABLED_NO_QUORUM ⇒ the same 404 (no existence oracle). FROZEN_READONLY still
	// vends the last committed state (the injected predicate returns true there).
	if cfg.Vendable != nil && !cfg.Vendable(sid) {
		notFound(w)
		return
	}
	sub, err := proxysub.LookupByTokenHash(cfg.DB, sid, tokenHash)
	if err != nil {
		notFound(w)
		return
	}

	nodes, err := liveProxyNodes(cfg.DB, sid, cfg.ClusterMode)
	if err != nil {
		cfg.Logger.Warn("subhttp: live nodes query", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	body, err := renderClash(sid, cfg.PublicHost, sub.Cipher, sub.PSK, nodes)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Profile-Update-Interval", "1")
	_, _ = w.Write(body)
}

func notFound(w http.ResponseWriter) {
	http.Error(w, "not found", http.StatusNotFound)
}

// ProxyNode is one rendered exit node. Host (C5) is the per-node home broker public host in cluster
// mode (empty in single mode → the renderer falls back to cfg.PublicHost).
type ProxyNode struct {
	NID  string
	Port int
	Host string
}

// LiveProxyNodes is the single-mode (P13) exit-node query, kept exported for back-compat.
func LiveProxyNodes(db *sql.DB, sid string) ([]ProxyNode, error) {
	return liveProxyNodes(db, sid, false)
}

// liveProxyNodes returns the ONLINE, proxy-ready exit nodes for sid, ordered by nid. The ONLINE +
// proxy_ready gates exclude offline / pre-P13 / not-yet-bound agents, so the list auto-updates as
// agents come and go. round-6 F9: the render independently honours the AUTHORITATIVE master switch +
// capability (state=ACTIVE, proxy_enabled=1, proxy_capable=1) so an OFF-with-cleanup-failure leaves no
// stale node. C5: in cluster mode each node's exit Host is its AUTHORITATIVE home broker's public host
// (the broker that terminates that node's tunnel), and a node whose home is NOT a VOTER is EXCLUDED
// (un-vendable until it rehomes) — so /sub from any broker returns working exits.
func liveProxyNodes(db *sql.DB, sid string, clusterMode bool) ([]ProxyNode, error) {
	if !clusterMode {
		rows, err := db.Query(`
			SELECT pa.nid, pa.port
			FROM port_allocations pa
			JOIN nodes n ON n.sid = pa.sid AND n.nid = pa.nid
			JOIN sessions s ON s.sid = pa.sid
			WHERE pa.sid = ?
			  AND pa.name = '__proxy__'
			  AND pa.state = 'ALLOCATED'
			  AND n.status = 'ONLINE'
			  AND n.proxy_ready = 1
			  AND n.proxy_capable = 1
			  AND s.state = 'ACTIVE'
			  AND s.proxy_enabled = 1
			ORDER BY pa.nid`, sid)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		var out []ProxyNode
		for rows.Next() {
			var n ProxyNode
			if err := rows.Scan(&n.NID, &n.Port); err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return out, rows.Err()
	}
	// Cluster mode: JOIN the home broker (must be a VOTER) and project its public_host per node.
	rows, err := db.Query(`
		SELECT pa.nid, pa.port, cn.public_host
		FROM port_allocations pa
		JOIN nodes n ON n.sid = pa.sid AND n.nid = pa.nid
		JOIN sessions s ON s.sid = pa.sid
		JOIN cluster_nodes cn ON cn.node_id = pa.home_broker
		WHERE pa.sid = ?
		  AND pa.name = '__proxy__'
		  AND pa.state = 'ALLOCATED'
		  AND n.status = 'ONLINE'
		  AND n.proxy_ready = 1
		  AND n.proxy_capable = 1
		  AND s.state = 'ACTIVE'
		  AND s.proxy_enabled = 1
		  AND cn.phase = 'VOTER'
		  AND cn.public_host != ''
		ORDER BY pa.nid`, sid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ProxyNode
	for rows.Next() {
		var n ProxyNode
		if err := rows.Scan(&n.NID, &n.Port, &n.Host); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// clashConfig is the minimal Clash document: one ss proxy per live node, a
// single select group over them, and MATCH→group (forward all, no rules).
type clashConfig struct {
	Proxies     []clashProxy `yaml:"proxies"`
	ProxyGroups []clashGroup `yaml:"proxy-groups"`
	Rules       []string     `yaml:"rules"`
}

type clashProxy struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Server   string `yaml:"server"`
	Port     int    `yaml:"port"`
	Cipher   string `yaml:"cipher"`
	Password string `yaml:"password"`
	UDP      bool   `yaml:"udp"`
}

type clashGroup struct {
	Name    string   `yaml:"name"`
	Type    string   `yaml:"type"`
	Proxies []string `yaml:"proxies"`
}

func renderClash(sid, publicHost, cipher, psk string, nodes []ProxyNode) ([]byte, error) {
	group := "tether-" + sid
	cfg := clashConfig{Rules: []string{"MATCH," + group}}
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		server := publicHost // C5: per-node home host in cluster mode; falls back to this broker's host
		if n.Host != "" {
			server = n.Host
		}
		cfg.Proxies = append(cfg.Proxies, clashProxy{
			Name: n.NID, Type: "ss", Server: server, Port: n.Port,
			Cipher: cipher, Password: psk, UDP: false,
		})
		names = append(names, n.NID)
	}
	// A Clash select group must list at least one proxy; when there are no
	// live nodes, fall back to DIRECT so the document stays valid.
	if len(names) == 0 {
		names = []string{"DIRECT"}
	}
	cfg.ProxyGroups = []clashGroup{{Name: group, Type: "select", Proxies: names}}
	return yaml.Marshal(cfg)
}

// Bind validates the address (loopback-only, F6) and binds the listener
// SYNCHRONOUSLY, returning any configuration/bind error to the caller (round-6
// F10). The broker calls this before declaring startup successful, then hands
// the listener to ServeListener in a goroutine — so a bad config or occupied
// port fails the broker start instead of leaving a healthy-looking broker with
// no subscription endpoint.
func Bind(addr string) (net.Listener, error) {
	if err := requireLoopback(addr); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("subhttp: listen %q: %w", addr, err)
	}
	return ln, nil
}

// ServeListener serves /sub on an already-bound listener until ctx is canceled.
func ServeListener(ctx context.Context, ln net.Listener, cfg Config) error {
	srv := &http.Server{
		Handler:           Handler(cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}
	context.AfterFunc(ctx, func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	})
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("subhttp: serve: %w", err)
	}
	return nil
}

// Serve binds and serves in one call (validation is synchronous; an invalid
// address returns before any goroutine). Returns immediately (nil) when addr
// is empty (feature disabled). Retained for callers/tests that want the
// combined form.
func Serve(ctx context.Context, addr string, cfg Config) error {
	if addr == "" {
		return nil
	}
	ln, err := Bind(addr)
	if err != nil {
		return err
	}
	return ServeListener(ctx, ln, cfg)
}
