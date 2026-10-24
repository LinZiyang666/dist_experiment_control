// Pure broker configuration validity. `serve --config-check` and a real start
// must reach one verdict, and the S8 preflight's candidate-config gate (§6.1
// "role/config", repeated at select/freeze/final export) asks the candidate
// binary that same question before a cut or the explicit first-migration
// template (§7.2) replaces the running broker. Keeping the validators in this
// leaf lets both call sites share them without growing the Broker owner.
// origin: hot-update S8 plan §6.1 / §7.2 / §12.1 (P15 broker.go net-zero move)
package brokerupgrade

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
)

// ValidateListenAddr checks a host:port listen address FORMAT (empty ⇒ ok,
// meaning disabled or default). A ":8090" (host-less) form is valid — net.Listen
// accepts it — so only the port must parse and be in range. Port 0 is VALID:
// net.Listen treats it as "assign an ephemeral port", so config-check must
// accept it too (F1 parity — a preflight must not reject what a real start
// accepts).
func ValidateListenAddr(name, addr string) error {
	if addr == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q: %w", name, addr, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("%s %q: port must be 0-65535", name, addr)
	}
	// A host, when present, must be a bare IP or a syntactically valid DNS
	// hostname — reject what net.Listen would reject at bind (F1-B). A bare
	// ":9090" (host-less wildcard) is fine.
	if !ValidListenHost(host) {
		return fmt.Errorf("%s %q: malformed host", name, addr)
	}
	return nil
}

// ValidateNATSURL checks the NATS server URL(s) for the same pure-syntax
// validity nats.go requires, WITHOUT dialing (F1-A). The client accepts a
// comma-separated list and gives a scheme-less "host:port" entry an implicit
// "nats://"; this mirrors that so config-check and Connect reach one verdict.
// Reachability (host resolvable, port open) is deliberately NOT checked — that
// is a runtime availability concern, not a config error.
func ValidateNATSURL(raw string) error {
	// nats.go rejects a pool that mixes ws/wss with nats/tls before it dials
	// anything (ErrMixingWebsocketSchemes). Track the same pool-level class;
	// validating each URL independently is insufficient.
	poolClass := -1 // 0 = nats/tls, 1 = ws/wss
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			return fmt.Errorf("broker.nats.url %q: empty server entry", raw)
		}
		toParse := s
		if !strings.Contains(toParse, "://") {
			toParse = "nats://" + toParse // nats.go's implicit scheme
		}
		u, err := url.Parse(toParse)
		if err != nil {
			return fmt.Errorf("broker.nats.url %q: %w", s, err)
		}
		var class int
		switch u.Scheme {
		case "nats", "tls":
			class = 0
		case "ws", "wss":
			class = 1
		default:
			return fmt.Errorf("broker.nats.url %q: scheme must be nats/tls/ws/wss", s)
		}
		if u.Host == "" {
			return fmt.Errorf("broker.nats.url %q: missing host", s)
		}
		if poolClass >= 0 && class != poolClass {
			return fmt.Errorf("broker.nats.url %q: %w", raw, nats.ErrMixingWebsocketSchemes)
		}
		poolClass = class
	}
	return nil
}

// ValidListenHost reports whether host is a bind-legal listen host: empty
// (wildcard), a bare IP (v4/v6, optional IPv6 zone), or a syntactically valid
// DNS hostname. It rejects shell/URL metacharacters ("bad$host") that
// net.Listen would fail on, so config-check and a real Bind agree (F1-B).
func ValidListenHost(host string) bool {
	if host == "" {
		return true // wildcard bind
	}
	// A zone is legal only on an IPv6 literal (fe80::1%eth0). Do not strip an
	// arbitrary suffix before DNS validation: example.com%eth0 is not an IPv6
	// address and a real bind cannot interpret it as the hostname example.com.
	if i := strings.IndexByte(host, '%'); i >= 0 {
		ip, zone := host[:i], host[i+1:]
		return zone != "" && strings.Contains(ip, ":") && net.ParseIP(ip) != nil
	}
	if net.ParseIP(host) != nil {
		return true
	}
	// A single trailing dot is the standard absolute-FQDN spelling and is
	// accepted by resolvers. Remove it for label validation, but reject ".".
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	if len(host) > 253 {
		return false
	}
	// DNS hostname: dot-separated labels of [A-Za-z0-9_-], each 1..63 chars,
	// not starting/ending with '-'. Underscore is tolerated (real deployments
	// use it in service names); '$', space, '/' and the like are not.
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			default:
				return false
			}
		}
	}
	return true
}
