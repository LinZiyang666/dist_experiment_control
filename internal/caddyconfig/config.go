// Package caddyconfig prepares the supported stock Caddy configuration and
// binds its admin protocol to one permissioned invocation. It never runs Caddy,
// changes ingress, retires a process, or writes certificate/storage files.
package caddyconfig

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

const maxConfigBytes = 4 << 20

var (
	ErrConfig      = errors.New("infra_config_invalid")
	ErrCertificate = fmt.Errorf("%w: certificate rejected", ErrConfig)
)

// Options comes from the local policy/config owner, never directly from an
// untrusted /load request. Certificate and Key are immutable snapshot paths;
// P09 owns their read-only provenance, lifetime and final storage selector.
type Options struct {
	Host        string
	AdminSocket string
	Shadow      bool
	Certificate string
	Key         string
}

func (Options) String() string               { return "caddy-options" }
func (o Options) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, o.String()) }

// Config keeps private paths out of formatted status/error output. Bytes is a
// private admin payload; only Digest belongs in a journal or public receipt.
type Config struct {
	data        []byte
	digest      c.Digest
	adminSocket string
}

func (cfg *Config) Bytes() []byte              { return append([]byte(nil), cfg.data...) }
func (cfg *Config) Digest() c.Digest           { return cfg.digest }
func (cfg *Config) String() string             { return fmt.Sprintf("caddy-config:%x", cfg.digest) }
func (cfg *Config) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, cfg.String()) }

// Prepare accepts stock-adapted JSON, rather than trying to implement a second
// Caddyfile parser. P01/P29 must still run both catalog binaries' adapt/validate
// in their isolated service-UID directories; this leaf claims no stock load.
// origin: hot-update S9 plan §5.1 / T18
func Prepare(raw []byte, opts Options) (*Config, error) {
	if !validHost(opts.Host) || !privatePath(opts.AdminSocket) || len(opts.AdminSocket) > 107 || !privatePath(opts.Certificate) || !privatePath(opts.Key) {
		return nil, ErrConfig
	}
	if err := validateCertificate(opts); err != nil {
		return nil, err
	}
	root, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	// An unknown app or root setting can own listeners, run custom code or
	// change storage. Copying it into a warm process would create responsibilities
	// outside the kernel census/support proof, even if stock syntax accepts it.
	if !fields(root, "admin", "apps", "storage") {
		return nil, c.ErrUnsupported
	}
	if storage, ok := root["storage"]; ok {
		s, ok := storage.(map[string]any)
		if !ok || !fields(s, "module", "root") || s["module"] != "file_system" || !privatePath(stringValue(s["root"])) {
			return nil, c.ErrUnsupported
		}
	}
	if old, ok := root["admin"]; ok {
		a, ok := old.(map[string]any)
		if !ok || !fields(a, "listen", "disabled", "config", "enforce_origin", "origins") {
			return nil, c.ErrUnsupported
		}
		if settings, ok := a["config"]; ok {
			m, ok := settings.(map[string]any)
			if !ok || !fields(m, "persist") {
				return nil, c.ErrUnsupported
			}
		}
	}
	apps, ok := root["apps"].(map[string]any)
	if !ok || !fields(apps, "http", "tls") {
		return nil, c.ErrUnsupported
	}
	httpApp, ok := apps["http"].(map[string]any)
	if !ok || !fields(httpApp, "servers", "http_port", "https_port", "grace_period", "shutdown_delay") {
		return nil, c.ErrUnsupported
	}
	servers, ok := httpApp["servers"].(map[string]any)
	// The managed surface has one canonical TLS server. A second listener is
	// not covered by the fixed 443 -> 8443 ingress cut and must defer upstream.
	if !ok || len(servers) != 1 {
		return nil, c.ErrUnsupported
	}
	var server map[string]any
	for _, value := range servers {
		server, ok = value.(map[string]any)
	}
	if !ok || !fields(server, "listen", "routes", "tls_connection_policies", "automatic_https", "protocols", "strict_sni_host", "read_timeout", "read_header_timeout", "write_timeout", "idle_timeout", "max_header_bytes") {
		return nil, c.ErrUnsupported
	}
	listeners, ok := server["listen"].([]any)
	if !ok || len(listeners) != 1 || listeners[0] != ":443" {
		return nil, c.ErrUnsupported
	}
	if err := validateRoutes(server["routes"], opts.Host, 0); err != nil {
		return nil, err
	}
	if err := validateTLS(apps["tls"], server["tls_connection_policies"], opts.Host); err != nil {
		return nil, err
	}
	// Unknown live protocols are a P10 census decision. New invocations only
	// open TCP listeners, so the shadow cannot silently grow a QUIC obligation.
	if p, ok := server["protocols"]; ok {
		values, ok := p.([]any)
		if !ok || len(values) == 0 {
			return nil, c.ErrUnsupported
		}
		for _, value := range values {
			if value != "h1" && value != "h2" {
				return nil, c.ErrUnsupported
			}
		}
	}
	server["protocols"] = []any{"h1", "h2"}
	httpApp["grace_period"] = upgradebudget.RequestLatency.String()
	httpApp["shutdown_delay"] = "0s"
	// Unix permissions and peer credentials enforce the boundary; Origin is
	// not a substitute. Disable Caddy's autosave of private snapshot paths.
	root["admin"] = map[string]any{"listen": "unix/" + opts.AdminSocket + "|0600", "config": map[string]any{"persist": false}}
	if opts.Shadow {
		server["listen"] = []any{":8443"}
		server["automatic_https"] = map[string]any{"disable": true}
		server["tls_connection_policies"] = []any{map[string]any{}}
		// A files loader preserves the served certificate/key and its pins; it
		// must not keep automate/issuers or point at the live ACME writer tree.
		apps["tls"] = map[string]any{"certificates": map[string]any{"load_files": []any{map[string]any{"certificate": opts.Certificate, "key": opts.Key}}}}
		// Even static TLS starts stock Caddy's storage cleaner. An omitted
		// storage module would use the service UID's live default directory.
		// Isolate housekeeping in this invocation's runtime directory instead.
		root["storage"] = map[string]any{"module": "file_system", "root": filepath.Join(filepath.Dir(opts.AdminSocket), "storage")}
	}
	data, err := json.Marshal(root)
	if err != nil || len(data) > maxConfigBytes {
		return nil, ErrConfig
	}
	return &Config{data: data, digest: sha256.Sum256(data), adminSocket: opts.AdminSocket}, nil
}

func validateCertificate(opts Options) error {
	// origin: hot-update S9 plan §5.1 / T18
	// No parser/filesystem error is propagated: PEM bytes and private paths
	// are not public error material, including when an attacker names a path.
	certPEM, err := readPrivateFile(opts.Certificate)
	if err != nil {
		return ErrCertificate
	}
	keyPEM, err := readPrivateFile(opts.Key)
	if err != nil {
		return ErrCertificate
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil || len(pair.Certificate) == 0 {
		return ErrCertificate
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	now := time.Now()
	if err != nil || now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) || leaf.VerifyHostname(opts.Host) != nil {
		return ErrCertificate
	}
	return nil
}

func readPrivateFile(name string) ([]byte, error) {
	// Refuse links and special files before opening: an accidental FIFO must
	// not hang preflight. P09 supplies immutable, permissioned parent directories;
	// SameFile also rejects a different regular inode observed after the open.
	before, err := os.Lstat(name)
	if err != nil || !before.Mode().IsRegular() || before.Size() > 1<<20 {
		return nil, ErrCertificate
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, ErrCertificate
	}
	// Read-only: the SameFile/size checks and the bounded read decide the
	// result before close, so a close error has nothing left to report.
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(before, info) || info.Size() > 1<<20 {
		return nil, ErrCertificate
	}
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, ErrCertificate
	}
	return data, nil
}

func privatePath(value string) bool {
	// Caddy interprets braces as replacements and | as Unix permission syntax.
	// Accepting either in a supposedly literal private path changes its meaning.
	return filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsAny(value, "\x00\r\n{}|")
}

func validHost(host string) bool {
	if host == "" || strings.ContainsAny(host, "\x00\r\n /:*{}") {
		return false
	}
	for _, ch := range host {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '.', ch == '-':
		default:
			return false
		}
	}
	return true
}

func stringValue(value any) string { s, _ := value.(string); return s }

// boolValue is true only for a decoded JSON boolean true; a missing key or a
// value of any other type (e.g. the string "true") counts as false.
func boolValue(value any) bool { b, _ := value.(bool); return b }

func fields(m map[string]any, allowed ...string) bool {
	for key := range m {
		found := false
		for _, name := range allowed {
			if key == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Caddy modules are objects with open-ended schemas, but that is not permission
// to accept duplicate keys, trailing JSON, replacements of invalid UTF-8, or
// an unbounded recursive payload before validating the closed support surface.
func decodeObject(raw []byte) (map[string]any, error) {
	if len(raw) == 0 || len(raw) > maxConfigBytes || !utf8.Valid(raw) {
		return nil, ErrConfig
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := decodeValue(d, 0)
	if err != nil {
		return nil, ErrConfig
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrConfig
	}
	root, ok := value.(map[string]any)
	if !ok {
		return nil, ErrConfig
	}
	return root, nil
}

func decodeValue(d *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, ErrConfig
	}
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, ErrConfig
			}
			if _, exists := object[name]; exists {
				return nil, ErrConfig
			}
			value, err := decodeValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, ErrConfig
		}
		return object, nil
	case json.Delim('['):
		// Empty arrays are part of handler configuration too. Collapsing [] to
		// null can change header operations while leaving route names intact.
		array := make([]any, 0)
		for d.More() {
			value, err := decodeValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return nil, ErrConfig
		}
		return array, nil
	default:
		if _, delimiter := token.(json.Delim); delimiter {
			return nil, ErrConfig
		}
		return token, nil
	}
}

func validUpstream(value string) bool {
	host, port, err := net.SplitHostPort(value)
	return err == nil && host != "" && port != "" && !strings.ContainsAny(value, "{}\r\n\x00/")
}
