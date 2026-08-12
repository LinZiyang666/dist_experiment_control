// Package serveconf parses the broker.yaml shape from architecture
// A.3 § "broker.yaml 骨架". v1 implements only the fields tetherd
// actually consumes — Caddy / ACME / wss_listen are ops concerns
// (install.sh writes Caddyfile + systemd units that read the same
// yaml) and don't need to round-trip through Go state.
package serveconf

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	TLS        TLSSection     `yaml:"tls"`
	NATS       NATSSection    `yaml:"nats"`
	Frp        FrpSection     `yaml:"frp"`
	Admin      AdminSection   `yaml:"admin"`
	Sub        SubSection     `yaml:"sub"`
	Storage    StorageSection `yaml:"storage"`
	Upgrade    UpgradeSection `yaml:"upgrade"`
	Cluster    ClusterSection `yaml:"cluster"`
	Obs        ObsSection     `yaml:"observability"`
}

// TLSSection is an INERT stub (#75): install.sh's broker.yaml template has
// carried `broker.tls.acme.email` since P10 for Caddy/ops consumption — Go
// never reads it. It exists ONLY so the strict KnownFields decoder in Load
// does not refuse every officially-installed broker.yaml. Parsed, never
// consumed; TestInstallShTemplateParsesStrict pins template↔schema pairing.
type TLSSection struct {
	ACME ACMESection `yaml:"acme"`
}

type ACMESection struct {
	Email string `yaml:"email"`
}

// ObsSection (B5/B6) mirrors broker.observability: the declarative form of the
// --log-level/--log-json/--metrics-listen flags + the B6 alert webhook URL. Every key
// absent ⇒ today's defaults (info / text / no metrics / no webhook) — byte-equivalent.
type ObsSection struct {
	LogLevel        string `yaml:"log_level"`         // debug | info | warn | error
	LogJSON         bool   `yaml:"log_json"`          // structured JSON logs
	MetricsListen   string `yaml:"metrics_listen"`    // Prometheus /metrics addr; empty disables
	AlertWebhookURL string `yaml:"alert_webhook_url"` // B6 OPS#2 alert webhook (http/https); empty disables

	// DiskCheckInterval (#39 / R13) is how often the H.4 disk-pressure monitor samples
	// StoreDir usage. Accepts Go time.Duration syntax ("5m", "30s", "1h"). Empty → broker
	// default (5m). Values below 1s are rejected at Load() — see DiskCheckIntervalDuration.
	DiskCheckInterval string `yaml:"disk_check_interval"`

	// LogFile / LogMaxSizeMB / LogMaxBackups (h1 F) point the broker's slog
	// output at an in-process size-capped rotating file instead of stderr.
	// Empty LogFile ⇒ stderr (the binary default, so dev + the embedded-NATS
	// test harness are byte-unchanged); install.sh writes the real path into
	// the deployed broker.yaml. Sizes default to 50MB × 2 backups.
	// The cap lives in the PROCESS because the incident host had no
	// logrotate and systemd's append: sink is unbounded by construction.
	LogFile       string `yaml:"log_file"`
	LogMaxSizeMB  int    `yaml:"log_max_size_mb"`
	LogMaxBackups int    `yaml:"log_max_backups"`
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
	// HEALTHY-HA on a node that does not manage its conf). Default path /etc/tether/nats.d/nats.conf
	// (#22: a tether-owned subdir the User=tether reconciler can atomically rewrite; $ETC_DIR stays root-owned).
	NatsConfPath  string `yaml:"nats_conf_path"`
	NatsServerBin string `yaml:"nats_server_bin"`
	// ColocatedAgentNID (G5 #19) is the nid of the tether agent co-located on this broker host. It is
	// self-reported in ClusterHealthResp so `node ls --brokers` correlates the broker daemon's version
	// with its agent's RELEASE. Empty ⇒ the CLI assumes node_id==nid (labelled "(assumed)").
	ColocatedAgentNID string `yaml:"colocated_agent_nid"`
	// XferReapInterval (#58/P10) is how often the home-authoritative orphan tier-B xfer-object reaper
	// runs (reconcile_passes.go xfer-orphan-reap). Empty ⇒ broker default (5m), which is fine for
	// production (orphans are not urgent). Exposed so a deploy-tier drill / a bucket-cap-sensitive
	// operator can shorten it and OBSERVE the reap without a five-minute wait. Go duration syntax; a
	// sub-second value is rejected at Load (pure JS-API churn).
	XferReapInterval string `yaml:"xfer_reap_interval"`
	// XferCrossHomeReapAge (R16 #58 Lane C) is the age floor for the LEADER-driven cross-home GC of a
	// split-home / zero-node OBJ_xfer bucket no single home can reap. Empty ⇒ broker default (3× the tier-B
	// timeout = 15m), which is right for production (a longer floor protects a transfer still live on another
	// home across clock skew). Exposed ONLY so a deploy-tier drill can compress it and OBSERVE the GC without
	// a multi-minute wait; there is no production tuning story. Go duration syntax; sub-second is rejected.
	XferCrossHomeReapAge string `yaml:"xfer_cross_home_reap_age"`
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
	// #75: STRICT decode (KnownFields), mirroring loadAgentYAML. The
	// non-strict yaml.Unmarshal this replaces silently dropped unknown or
	// mis-nested keys — the live incident shape was an `observability:`
	// block that never took effect, with zero warning, while the broker
	// wrote unbounded logs elsewhere. yaml.v3 errors carry line + key, so
	// a typo surfaces in `systemctl status` instead of vanishing. Ops-only
	// template keys Go never consumes (broker.tls.*) are covered by inert
	// stub fields above, NOT by loosening this decoder.
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(body))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		// An empty / comment-only / whitespace-only file has no document;
		// tolerate it (zero config), matching the historical behavior and
		// the agent-side loader.
		if errors.Is(err, io.EOF) {
			return &Config{}, nil
		}
		return nil, fmt.Errorf("serveconf: parse %q: %w", path, err)
	}
	// Reject any SECOND-or-later NON-EMPTY document: it could hide a
	// section the first does not show. Benign empty trailing documents
	// (a lone `---`) decode to nil and are tolerated; the scan MUST loop
	// to io.EOF so an empty middle document cannot shield a later
	// non-empty one (mirrors loadAgentYAML's F11 fail-open fix).
	for {
		var extra any
		derr := dec.Decode(&extra)
		if errors.Is(derr, io.EOF) {
			break
		}
		if derr != nil {
			return nil, fmt.Errorf("serveconf: parse %q: %w", path, derr)
		}
		if extra != nil {
			return nil, fmt.Errorf("serveconf: parse %q: multiple YAML documents are not supported", path)
		}
	}
	if _, err := cfg.ProcRetentionDuration(); err != nil {
		return nil, err
	}
	if _, err := cfg.ProcGCIntervalDuration(); err != nil {
		return nil, err
	}
	if _, err := cfg.DiskCheckIntervalDuration(); err != nil {
		return nil, err
	}
	if _, err := cfg.XferReapIntervalDuration(); err != nil {
		return nil, err
	}
	if _, err := cfg.XferCrossHomeReapAgeDuration(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// maxReapInterval is the upper bound (external review N-5) shared by the periodic-reaper / monitor
// interval knobs. Their floor duty is "garbage / disk pressure must not be immortal"; a value like
// 10000h passes the sub-second floor check yet SILENTLY DISABLES the reaper (the racknerd small-disk
// fill was exactly an immortal-garbage incident). One run per day bounds worst-case garbage lifetime
// to ~1 day; any legitimate slow-reap motive (JS-API churn on a big cluster) is satisfied far below
// this. The clamp catches "silently disabled", not taste — it is deliberately generous.
const maxReapInterval = 24 * time.Hour

// XferReapIntervalDuration parses broker.cluster.xfer_reap_interval (#58/P10). Returns (0, nil) when
// unset (caller falls back to broker.New's 5m default). Values under 1 second are rejected — a
// sub-second reap is pure ListStreams/object-list churn against the JS API for no operator benefit,
// and a non-positive value would make the registry's anchored deadline math meaningless. Values over
// maxReapInterval (24h) are ALSO rejected (N-5): an effectively-disabled reaper lets orphan tier-B
// objects fill the disk; there is no "off" setting.
func (c *Config) XferReapIntervalDuration() (time.Duration, error) {
	if c.Broker.Cluster.XferReapInterval == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.Broker.Cluster.XferReapInterval)
	if err != nil {
		return 0, fmt.Errorf("serveconf: broker.cluster.xfer_reap_interval: %w", err)
	}
	if d < time.Second {
		return 0, fmt.Errorf("serveconf: broker.cluster.xfer_reap_interval %q "+
			"< 1s (refusing a sub-second orphan-object reap)", d)
	}
	if d > maxReapInterval {
		return 0, fmt.Errorf("serveconf: broker.cluster.xfer_reap_interval %q "+
			"> 24h (an effectively-disabled reaper lets orphan tier-B objects fill the disk — the reap must "+
			"run at least daily; there is no 'off' setting, unset means the built-in 5m default)", d)
	}
	return d, nil
}

// MinXferCrossHomeReapAge is the SAFE FLOOR for the cross-home GC age (external review F2): 3x the
// tier-B transfer timeout, i.e. the same value broker.New derives when the knob is unset. It is
// duplicated here rather than imported because internal/serveconf must not depend on internal/broker;
// a test pins the two against each other.
const MinXferCrossHomeReapAge = 15 * time.Minute

// XferCrossHomeReapAgeDuration parses broker.cluster.xfer_cross_home_reap_age (R16 #58 Lane C). Returns
// (0, nil) when unset (caller falls back to broker.New's derived 3×tier-B default). Sub-second is rejected
// (a near-zero floor would let the leader tear out a transfer still live on another home); the upper clamp
// (maxReapInterval, 24h) forbids an effectively-disabled floor that would immortalize split-home garbage.
func (c *Config) XferCrossHomeReapAgeDuration() (time.Duration, error) {
	if c.Broker.Cluster.XferCrossHomeReapAge == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.Broker.Cluster.XferCrossHomeReapAge)
	if err != nil {
		return 0, fmt.Errorf("serveconf: broker.cluster.xfer_cross_home_reap_age: %w", err)
	}
	// EXTERNAL REVIEW F2: this used to accept anything >= 1s, which let a production YAML set the
	// cross-home GC floor BELOW the tier-B watchdog. In a split-home session no broker owns the whole
	// bucket and the leader only excludes its OWN local tracker — it cannot see a transfer still live on
	// another home — so a 5s floor lets the leader delete another home's in-use object minutes before
	// that transfer's own 5-minute watchdog would end it. The knob may now only RAISE the floor above
	// the derived safe default; it can never lower it. (Drill-side time compression must construct
	// broker.Config directly rather than go through the production schema.)
	if d < MinXferCrossHomeReapAge {
		return 0, fmt.Errorf("serveconf: broker.cluster.xfer_cross_home_reap_age %q is below the safe "+
			"floor %s (3x the tier-B transfer timeout). A shorter floor lets the leader reap an object "+
			"that is still live on ANOTHER home, which no broker-local tracker can see. This knob may "+
			"only RAISE the floor; unset means the built-in derived default", d, MinXferCrossHomeReapAge)
	}
	if d > maxReapInterval {
		return 0, fmt.Errorf("serveconf: broker.cluster.xfer_cross_home_reap_age %q "+
			"> 24h (an effectively-disabled floor would immortalize split-home garbage; unset means the built-in 3×tier-B default)", d)
	}
	return d, nil
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
	if d > maxReapInterval {
		return 0, fmt.Errorf("serveconf: broker.storage.proc_gc_interval %q "+
			"> 24h (an effectively-disabled GC lets EXITED proc rows grow unbounded — it must run at least daily)", d)
	}
	return d, nil
}

// DiskCheckIntervalDuration parses broker.observability.disk_check_interval (#39 / R13). Returns
// (0, nil) when unset (caller falls back to the broker's built-in 5m default). Values under 1
// second are rejected — a sub-second disk poll is pure statfs churn for no operator benefit, and a
// negative/zero value would make time.NewTicker panic in the monitor. This is the yaml half of the
// operator knob; --disk-check-interval is the flag half, and takes precedence in serve.go.
func (c *Config) DiskCheckIntervalDuration() (time.Duration, error) {
	if c.Broker.Obs.DiskCheckInterval == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.Broker.Obs.DiskCheckInterval)
	if err != nil {
		return 0, fmt.Errorf("serveconf: broker.observability.disk_check_interval: %w", err)
	}
	if d < time.Second {
		return 0, fmt.Errorf("serveconf: broker.observability.disk_check_interval %q "+
			"< 1s (refusing a sub-second disk poll)", d)
	}
	if d > maxReapInterval {
		return 0, fmt.Errorf("serveconf: broker.observability.disk_check_interval %q "+
			"> 24h (an effectively-disabled disk monitor cannot raise disk_pressure before the disk fills — "+
			"it must run at least daily)", d)
	}
	return d, nil
}
