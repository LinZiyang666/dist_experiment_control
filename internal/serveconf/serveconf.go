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
