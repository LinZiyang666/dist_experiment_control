// Package serveconf parses the broker.yaml shape from architecture
// A.3 § "broker.yaml 骨架". v1 implements only the fields tetherd
// actually consumes — Caddy / ACME / wss_listen are ops concerns
// (install.sh writes Caddyfile + systemd units that read the same
// yaml) and don't need to round-trip through Go state.
package serveconf

import (
	"fmt"
	"os"
	"time"

	yaml "gopkg.in/yaml.v3"
)

// Config is the in-Go projection of broker.yaml. Field names mirror
// the architecture's yaml exactly; an unset field stays at its zero
// value so callers (cmd/tether/serve.go) can decide whether to use a
// CLI-flag default instead.
type Config struct {
	Broker BrokerSection `yaml:"broker"`
}

type BrokerSection struct {
	Domain     string         `yaml:"domain"`
	PublicHost string         `yaml:"public_host"`
	NATS       NATSSection    `yaml:"nats"`
	Frp        FrpSection     `yaml:"frp"`
	Admin      AdminSection   `yaml:"admin"`
	Sub        SubSection     `yaml:"sub"`
	Storage    StorageSection `yaml:"storage"`
	Upgrade    UpgradeSection `yaml:"upgrade"`
	Cluster    ClusterSection `yaml:"cluster"`
	Obs        ObsSection     `yaml:"observability"`
}

// ObsSection (B5/B6) mirrors broker.observability: the declarative form of the
// --log-level/--log-json/--metrics-listen flags + the B6 alert webhook URL. Every key
// absent ⇒ today's defaults (info / text / no metrics / no webhook) — byte-equivalent.
type ObsSection struct {
	LogLevel        string `yaml:"log_level"`         // debug | info | warn | error
	LogJSON         bool   `yaml:"log_json"`          // structured JSON logs
	MetricsListen   string `yaml:"metrics_listen"`    // Prometheus /metrics addr; empty disables
	AlertWebhookURL string `yaml:"alert_webhook_url"` // B6 OPS#2 alert webhook (http/https); empty disables
}

// ClusterSection mirrors broker.cluster — the D9 cutover surface. It carries
// only PATHS; the cluster IDENTITY (account issuer, broker nkey, server_name /
// node_id, cert_fp) is DERIVED from SecretsDir + the existing nats.conf so it
// cannot drift (d9-plan §3). All-empty ⇒ single mode: serve.go constructs no
// cluster.Node and the broker stays byte-equivalent to a pre-D9 single broker
// (the D2 N=1 functional-equivalence floor). A non-empty DataDir is only an
// INTENT; the authoritative cluster-mode trigger is the on-disk raft/ probe
// (cluster.RaftStateExists), cross-checked against this intent in serve.go.
type ClusterSection struct {
	// DataDir holds the raft sub-tree (raft/raft.db + raft/snapshots). Empty ⇒
	// single mode. `cluster init --from-existing` creates raft/ under it.
	DataDir string `yaml:"data_dir"`
	// RaftAddr is the raft transport bind (host:port, e.g. "0.0.0.0:7400"),
	// private-net only (never public; preflight checks this).
	RaftAddr string `yaml:"raft_addr"`
	// SecretsDir is the §15 secrets directory (cluster-ca.pem/.key, this node's
	// route leaf cert/key, tunnel-cert.pem/.key, broker.nk, node-ident.nk,
	// account.nk). Required in cluster mode; preflight refuses if unreadable.
	SecretsDir string `yaml:"secrets_dir"`
	// ManifestListen (C2) is the LOOPBACK addr for the well-known cluster discovery manifest
	// (/.well-known/tether/cluster.json), Caddy-fronted. Empty disables it; bound only in cluster mode.
	ManifestListen string `yaml:"manifest_listen"`
	// NatsConfPath / NatsServerBin (C3) locate the live nats.conf the per-broker topology reconciler
	// renders/swaps/reloads + the nats-server binary for the `-t` dry-run and `--signal reload`. Empty
	// NatsConfPath disables the reconciler (it reports no topology, so the cluster is not held out of
	// HEALTHY-HA on a node that does not manage its conf). Default path /etc/tether/nats.conf.
	NatsConfPath  string `yaml:"nats_conf_path"`
	NatsServerBin string `yaml:"nats_server_bin"`
}

// UpgradeSection mirrors broker.upgrade — the architecture J.4
// safety policy. URLAllow is the exact set of URL prefixes
// `tether node upgrade` may target; mandatory (empty → broker
// rejects all upgrades).
type UpgradeSection struct {
	URLAllow []string `yaml:"url_allow"`
}

// NATSSection mirrors broker.nats. URL is the address tetherd
// connects to; WSInternal / WssListen are informational pointers
// for the install.sh-generated Caddyfile.
type NATSSection struct {
	URL        string `yaml:"url"`
	WssListen  string `yaml:"wss_listen"`
	WSInternal string `yaml:"ws_internal"`
}

type FrpSection struct {
	BindAddr      string `yaml:"bind_addr"`
	ControlListen string `yaml:"control_listen"`
	PortRange     string `yaml:"port_range"`
}

type AdminSection struct {
	Socket string `yaml:"socket"`
}

// SubSection configures the P13 read-only subscription HTTP endpoint.
type SubSection struct {
	Listen string `yaml:"listen"` // loopback addr, e.g. "127.0.0.1:8090"; empty disables
}

type StorageSection struct {
	DB      string `yaml:"db"`
	JSStore string `yaml:"js_store"`

	// ProcRetention is the SQLite `processes` table retention
	// window for EXITED rows (see docs/reviews/ps-retention-plan.md).
	// Accepts Go time.Duration syntax ("1h", "30m", "24h").
	// Empty → broker default (1h).
	ProcRetention string `yaml:"proc_retention"`

	// ProcGCInterval is how often the broker sweeps EXITED rows
	// past ProcRetention. Accepts Go time.Duration syntax.
	// Empty → broker default (5m). Values below 1 minute are
	// rejected at Load() — see validateProcGC.
	ProcGCInterval string `yaml:"proc_gc_interval"`
}

// Load reads, parses, and returns one Config. Empty path returns a
// zero-valued Config without error so the caller can treat
// "no --config" the same as "config with everything defaulted".
//
// Load runs every duration validator so a misconfigured `proc_retention`
// or `proc_gc_interval` aborts startup BEFORE any side effect
// (storage.Open creating tether.db, NATS connect, etc). Tests that
// need sub-minute GC tickers construct broker.Config directly and
// bypass this decoder; broker.New imposes no minimum.
func Load(path string) (*Config, error) {
	if path == "" {
		return &Config{}, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("serveconf: read %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("serveconf: parse %q: %w", path, err)
	}
	if _, err := cfg.ProcRetentionDuration(); err != nil {
		return nil, err
	}
	if _, err := cfg.ProcGCIntervalDuration(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ProcRetentionDuration parses broker.storage.proc_retention. Returns
// (0, nil) when unset (caller falls back to broker.New's default).
//
// Non-positive values are rejected: GC computes `cutoff = now -
// retention`; a negative retention puts the cutoff in the FUTURE and
// the next sweep would delete every EXITED row immediately, not just
// aged ones. Zero is reserved for "unset → broker default".
func (c *Config) ProcRetentionDuration() (time.Duration, error) {
	if c.Broker.Storage.ProcRetention == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.Broker.Storage.ProcRetention)
	if err != nil {
		return 0, fmt.Errorf("serveconf: broker.storage.proc_retention: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("serveconf: broker.storage.proc_retention %q "+
			"must be positive (negative or zero would push the GC cutoff "+
			"into the future and erase every EXITED row immediately)", d)
	}
	return d, nil
}

// ProcGCIntervalDuration parses broker.storage.proc_gc_interval.
// Returns (0, nil) when unset (caller falls back to broker.New's
// default). Values under 1 minute are rejected — a sub-minute sweep
// burns SQLite write capacity for negligible retention benefit.
func (c *Config) ProcGCIntervalDuration() (time.Duration, error) {
	if c.Broker.Storage.ProcGCInterval == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.Broker.Storage.ProcGCInterval)
	if err != nil {
		return 0, fmt.Errorf("serveconf: broker.storage.proc_gc_interval: %w", err)
	}
	if d < time.Minute {
		return 0, fmt.Errorf("serveconf: broker.storage.proc_gc_interval %q "+
			"< 1m (refusing sub-minute GC)", d)
	}
	return d, nil
}
