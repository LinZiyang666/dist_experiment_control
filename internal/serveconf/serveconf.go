// Package serveconf parses the broker.yaml shape from architecture
// A.3 § "broker.yaml 骨架". v1 implements only the fields tetherd
// actually consumes — Caddy / ACME / wss_listen are ops concerns
// (install.sh writes Caddyfile + systemd units that read the same
// yaml) and don't need to round-trip through Go state.
package serveconf

import (
	"fmt"
	"os"

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
	Storage    StorageSection `yaml:"storage"`
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

type StorageSection struct {
	DB      string `yaml:"db"`
	JSStore string `yaml:"js_store"`
}

// Load reads, parses, and returns one Config. Empty path returns a
// zero-valued Config without error so the caller can treat
// "no --config" the same as "config with everything defaulted".
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
	return &cfg, nil
}
