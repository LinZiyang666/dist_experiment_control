// render.go — the nats-server.conf RENDERER (was package natscluster until B5).
//
// distributed-broker §1.1/§6.1/§6.2. NATS runs as its own process (topology C); tetherd orchestrates it.
// Rendering provisions no certs, writes no disk, and starts nothing — Apply/DryRun in this same package
// own the disk side, and keeping the two apart is why a render can be unit-tested against the real
// nats-server parser without touching a live conf.
//
// The rendered conf encodes the three D3 trust-surface requirements:
//   - routes mTLS over the cluster CA, flat full mesh, one JS domain (§6.1);
//   - auth_callout with the shared account as Issuer and EVERY broker pub in
//     auth_users, so broker B can answer a request raised on server A (§6.1);
//   - RF1 broker-only ACL: a static nkey `users` entry per broker pub whose
//     permissions = auth.PermissionsForBroker() (incl. cluster.apply.>/cluster.>),
//     so a broker connecting to ANY server gets its subject authority — auth_users
//     alone is not enough (§6.2 R3F3).
package natsconf

import (
	"fmt"
	"sort"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
)

// Broker is one cluster member's identity for conf rendering.
type Broker struct {
	// ServerName is the deterministic nats-server name (§6.5). D3 renders it; it
	// is persisted to cluster_nodes.nats_server_id only in D7 (first writer).
	ServerName string
	// NkeyPub is the broker bus nkey public key — an auth_callout AuthUser AND the
	// holder of a static PermissionsForBroker() entry.
	NkeyPub string
	// RouteURL is this broker's nats cluster route URL, e.g. "nats://10.0.0.2:6222".
	RouteURL string
}

// Config is the input to Render for ONE server's nats-server.conf.
type Config struct {
	Local         Broker   // the server this conf is for (server_name + listens)
	Peers         []Broker // ALL brokers incl. Local (every pub → every auth_users + static user)
	AccountIssuer string   // shared account nkey pub == auth_callout Issuer
	// JSDomain WAS here and is deliberately GONE (B5, BUG-2).
	//
	// Render emitted `jetstream { domain: … }` from it, and Preflight REFUSES that subkey
	// (jetstreamSafeSubkeys is {"store_dir"}; the refusal's own comment names "a hand-set JS domain
	// that breaks cross-cluster stream addressing"). So this renderer could emit a directive its own
	// preflight rejects — and the plumbing to reach it was live: Inputs.JSDomain fed
	// straight into this field, while NOTHING in the tree ever set that input. The day someone wired
	// it, the autonomous reconciler would have swapped a conf its OWN NEXT TICK could not parse,
	// wedging at ActionUnknownDirective *after* the swap — fail-closed one step too late.
	//
	// Deleting it rather than wiring it is the correct direction because there is no consumer:
	// internal/jsstream never sets a JS domain, and distributed-broker-architecture.md §1.1/§6.1's
	// "one JS domain per cluster" is already satisfied by the DEFAULT unnamed domain (which is
	// literally one domain shared by every server). See TestRenderEmitsOnlyPreflightSafeSubkeys.
	JSStoreDir    string // JetStream store dir
	ClientListen  string // client listen, e.g. "127.0.0.1:4222"
	ClusterName   string // cluster name (empty => "tether")
	ClusterListen string // route listen, e.g. "0.0.0.0:6222"
	// MonitorListen (C3), when set (e.g. "127.0.0.1:8223"), emits the nats-server HTTP monitor so the
	// topology reconciler can probe /varz config_load_time for a REAL observed_generation. Loopback-
	// only (no new external exposure). Empty => no monitor (single-mode bootstrap conf unchanged).
	MonitorListen string
	CAFile        string // cluster CA (routes mTLS trust anchor)
	CertFile      string // this server's route leaf
	KeyFile       string // this server's route key
	// Standalone (v0.4.2 shrink), when true, renders a STANDALONE nats.conf — JetStream WITHOUT the
	// cluster{} block (a lone N=1 broker can never reach the clustered JS meta quorum-of-2, so the
	// last voter shrinking to N=1 must drop routes and run JS standalone — the reverse of the grow
	// render). The routes mTLS files are not required in this mode; the JetStream + authorization
	// blocks are unchanged.
	Standalone bool
}

// Render returns the nats-server.conf text for cfg.Local. It is deterministic
// (peers sorted by ServerName) so golden tests are stable.
func Render(cfg Config) (string, error) {
	if cfg.Local.ServerName == "" || cfg.Local.NkeyPub == "" {
		return "", fmt.Errorf("natscluster: Local needs ServerName and NkeyPub")
	}
	if cfg.AccountIssuer == "" {
		return "", fmt.Errorf("natscluster: AccountIssuer (shared account pub) required")
	}
	if len(cfg.Peers) == 0 {
		return "", fmt.Errorf("natscluster: at least one peer (incl. self) required")
	}
	if !cfg.Standalone && (cfg.CAFile == "" || cfg.CertFile == "" || cfg.KeyFile == "") {
		return "", fmt.Errorf("natscluster: routes mTLS requires CAFile, CertFile, KeyFile")
	}
	// external review B2-7: this used to read a Config.Account field that NO production or test code
	// ever set, so the `if account == "" ` branch was the only reachable one and the field was pure dead
	// plumbing — the same shape as Config.JSDomain, which B5 deleted for exactly this reason. B5's own
	// comments claimed BOTH had been removed; only one had. The account name is a constant here: tether
	// renders auth_callout against the default global account, install.sh writes `account: "$G"`, and
	// BuildMergedConf does not harvest an account name from the live conf (only the ISSUER, via
	// Ownership.AuthIdentity). If a non-default account is ever needed it belongs in RenderOverride next
	// to the other deliberate departures, where a caller has to ask for it.
	const account = "$G"
	clusterName := cfg.ClusterName
	if clusterName == "" {
		clusterName = "tether"
	}

	peers := append([]Broker(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i].ServerName < peers[j].ServerName })

	var b strings.Builder
	fmt.Fprintf(&b, "# generated by tether (internal/natscluster) — do not hand-edit\n")
	fmt.Fprintf(&b, "server_name: %q\n", cfg.Local.ServerName)
	if cfg.ClientListen != "" {
		fmt.Fprintf(&b, "listen: %q\n", cfg.ClientListen)
	}
	if cfg.MonitorListen != "" {
		fmt.Fprintf(&b, "http: %q\n", cfg.MonitorListen) // C3: loopback monitor for the observed-gen probe
	}
	b.WriteString("\n")

	// JetStream. `store_dir` is the ONLY subkey emitted, because it is the only one
	// Preflight accepts (jetstreamSafeSubkeys) — this renderer and that parser have to agree
	// or a conf this code writes is a conf this code cannot read back. Emitted only when a store dir is
	// configured; a JS-less render is valid for tests that exercise only the routes/callout surface,
	// and BuildMergedConf refuses that combination for a conf that HAS a jetstream block (BUG-1).
	//
	// The cluster shares ONE JetStream domain, which is what the default unnamed domain already is.
	// See the JSDomain gravestone on Config for why naming it is forbidden rather than merely unused.
	if cfg.JSStoreDir != "" {
		b.WriteString("jetstream {\n")
		fmt.Fprintf(&b, "  store_dir: %q\n", cfg.JSStoreDir)
		b.WriteString("}\n\n")
	}

	// Cluster — flat full-mesh routes over cluster-CA mTLS (verify = mutual). SKIPPED in standalone
	// mode (v0.4.2 shrink to N=1): a lone node has no routes and runs JetStream standalone, so the
	// rendered conf must NOT carry a cluster{} block (the reverse of the grow transition).
	clusterRoutes := 0
	if !cfg.Standalone {
		b.WriteString("cluster {\n")
		fmt.Fprintf(&b, "  name: %q\n", clusterName)
		if cfg.ClusterListen != "" {
			fmt.Fprintf(&b, "  listen: %q\n", cfg.ClusterListen)
		}
		b.WriteString("  routes: [\n")
		for _, p := range peers {
			if p.RouteURL == "" || p.ServerName == cfg.Local.ServerName {
				continue // skip self; nats dedups but keep the conf clean
			}
			fmt.Fprintf(&b, "    %q\n", p.RouteURL)
			clusterRoutes++
		}
		b.WriteString("  ]\n")
		b.WriteString("  tls {\n")
		fmt.Fprintf(&b, "    ca_file: %q\n", cfg.CAFile)
		fmt.Fprintf(&b, "    cert_file: %q\n", cfg.CertFile)
		fmt.Fprintf(&b, "    key_file: %q\n", cfg.KeyFile)
		b.WriteString("    verify: true\n")
		b.WriteString("  }\n")
		b.WriteString("}\n\n")
	}
	// FAIL-CLOSED (audit D): a clustered render with ZERO routes (all peers self/empty) produces a
	// cluster{} block that nats-server FATAL-refuses ("JetStream cluster requires configured routes or
	// solicited leafnode") — yet `nats-server -t` PASSES it, so the unbootable conf slips dry-run. A
	// lone node MUST render Standalone (no cluster{}); refuse rather than brick the broker at boot.
	if !cfg.Standalone && clusterRoutes == 0 {
		return "", fmt.Errorf("natscluster: refusing a clustered render with ZERO routes (peers=%d, all "+
			"self/empty) — nats-server would FATAL at boot; a lone node must render Standalone", len(peers))
	}

	// audit natscluster F1: dedup peers by NkeyPub. A duplicate nkey in auth_users or the static
	// users block makes nats-server FATAL at config load — a repeated roster entry, or the local
	// node also appearing in `peers`, must not brick the broker (especially on --skip-dry-run).
	seenNkey := map[string]bool{}
	var uniqueNkeys []string
	for _, p := range peers {
		if p.NkeyPub == "" || seenNkey[p.NkeyPub] {
			continue
		}
		seenNkey[p.NkeyPub] = true
		uniqueNkeys = append(uniqueNkeys, p.NkeyPub)
	}

	// Authorization: auth_callout (every broker pub in auth_users) + a static nkey
	// user per broker pub carrying PermissionsForBroker (RF1, §6.2 R3F3).
	perms := auth.PermissionsForBroker()
	b.WriteString("authorization {\n")
	b.WriteString("  auth_callout {\n")
	fmt.Fprintf(&b, "    issuer: %q\n", cfg.AccountIssuer)
	fmt.Fprintf(&b, "    account: %q\n", account)
	b.WriteString("    auth_users: [\n")
	for _, nkey := range uniqueNkeys {
		fmt.Fprintf(&b, "      %q\n", nkey)
	}
	b.WriteString("    ]\n")
	b.WriteString("  }\n")
	b.WriteString("  users: [\n")
	for _, nkey := range uniqueNkeys {
		b.WriteString("    {\n")
		fmt.Fprintf(&b, "      nkey: %q\n", nkey)
		b.WriteString("      permissions {\n")
		writeAllowBlock(&b, "publish", perms.Pub.Allow)
		writeAllowBlock(&b, "subscribe", perms.Sub.Allow)
		b.WriteString("      }\n")
		b.WriteString("    }\n")
	}
	b.WriteString("  ]\n")
	b.WriteString("}\n")

	return b.String(), nil
}

// writeAllowBlock renders a `publish { allow: [...] }` / `subscribe { allow: [...] }`
// block with every subject quoted (subjects contain `$`, `.`, `>` and must be
// quoted in nats conf).
func writeAllowBlock(b *strings.Builder, verb string, allow []string) {
	fmt.Fprintf(b, "        %s {\n", verb)
	b.WriteString("          allow: [\n")
	for _, s := range allow {
		fmt.Fprintf(b, "            %q\n", s)
	}
	b.WriteString("          ]\n")
	b.WriteString("        }\n")
}
