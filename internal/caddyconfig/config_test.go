package caddyconfig

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// origin: hot-update S9 plan §5.1 / T18
func TestCaddyConfigPreservesSpecificRoutes(t *testing.T) {
	opts := certificateOptions(t)
	raw := supportedConfig(t)
	before := mustObject(t, raw)
	for _, shadow := range []bool{false, true} {
		opts.Shadow = shadow
		cfg, err := Prepare(raw, opts)
		if err != nil {
			t.Fatal(err)
		}
		root := mustObject(t, cfg.Bytes())
		server := configServer(root)
		if !reflect.DeepEqual(configServer(before)["routes"], server["routes"]) {
			t.Fatal("route order or handler configuration changed")
		}
		for _, test := range []struct{ path, upstream string }{
			{"/sub/token", "127.0.0.1:8090"},
			{"/.well-known/tether/manifest", "127.0.0.1:7480"},
			{"/manifest.json", "127.0.0.1:7480"},
			{"/health", "static:ready"},
			{"/nats", "127.0.0.1:8222"},
		} {
			if actual := routedUpstream(server["routes"], test.path); actual != test.upstream {
				t.Fatalf("%s selected %q, want %q", test.path, actual, test.upstream)
			}
		}
		httpApp := root["apps"].(map[string]any)["http"].(map[string]any)
		if httpApp["grace_period"] != "5s" || httpApp["shutdown_delay"] != "0s" {
			t.Fatal("retirement grace must be finite with zero shutdown delay")
		}
		if !reflect.DeepEqual(server["protocols"], []any{"h1", "h2"}) {
			t.Fatal("new invocation opened unsupported protocol")
		}
		admin := root["admin"].(map[string]any)
		// persist must be PRESENT and false: Caddy's default for a missing key
		// is to autosave, so absence or a non-bool value is as unsafe as true.
		adminConfig, _ := admin["config"].(map[string]any)
		persist, persistIsBool := adminConfig["persist"].(bool)
		if admin["listen"] != "unix/"+opts.AdminSocket+"|0600" || !persistIsBool || persist {
			t.Fatal("admin endpoint or autosave is unsafe")
		}
		if cfg.Digest() != sha256.Sum256(cfg.Bytes()) {
			t.Fatal("config receipt is not its actual payload digest")
		}
		copy := cfg.Bytes()
		copy[0] = 'x'
		if cfg.Bytes()[0] != '{' {
			t.Fatal("caller changed immutable config")
		}
		if shadow {
			if !reflect.DeepEqual(server["listen"], []any{":8443"}) || !boolValue(server["automatic_https"].(map[string]any)["disable"]) {
				t.Fatal("shadow can bind canonical ingress or perform ACME")
			}
			storage, ok := root["storage"].(map[string]any)
			if !ok || storage["module"] != "file_system" || storage["root"] != filepath.Join(filepath.Dir(opts.AdminSocket), "storage") {
				t.Fatal("shadow housekeeping retained live/default storage")
			}
			tlsApp := root["apps"].(map[string]any)["tls"].(map[string]any)
			if _, ok := tlsApp["automation"]; ok {
				t.Fatal("shadow retained issuer")
			}
			loaders := tlsApp["certificates"].(map[string]any)
			if len(loaders) != 1 {
				t.Fatal("shadow retained automated loader")
			}
			file := loaders["load_files"].([]any)[0].(map[string]any)
			if file["certificate"] != opts.Certificate || file["key"] != opts.Key {
				t.Fatal("shadow changed certificate/pins")
			}
		} else {
			if !reflect.DeepEqual(root["storage"], before["storage"]) || !reflect.DeepEqual(root["apps"].(map[string]any)["tls"], before["apps"].(map[string]any)["tls"]) {
				t.Fatal("main changed filesystem ACME configuration")
			}
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x", "%q"} {
			if strings.Contains(fmt.Sprintf(verb, cfg), opts.Key) {
				t.Fatal("formatted config disclosed private path")
			}
			if strings.Contains(fmt.Sprintf(verb, opts), opts.Key) {
				t.Fatal("formatted options disclosed private path")
			}
		}
	}
	if !bytes.Equal(raw, supportedConfig(t)) {
		t.Fatal("preparation changed its input")
	}
	t.Run("nested-stock-handles", func(t *testing.T) {
		root := mustObject(t, raw)
		for _, value := range innerRoutes(root) {
			route := value.(map[string]any)
			route["handle"] = []any{map[string]any{"handler": "subroute", "routes": []any{map[string]any{"handle": route["handle"]}}}}
		}
		payload, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := Prepare(payload, opts)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(configServer(root)["routes"], configServer(mustObject(t, cfg.Bytes()))["routes"]) {
			t.Fatal("stock handle wrappers changed")
		}
		if got := routedUpstream(configServer(mustObject(t, cfg.Bytes()))["routes"], "/sub/token"); got != "127.0.0.1:8090" {
			t.Fatal("nested subscription route was stolen")
		}
		nested := innerRoutes(root)
		nested[0], nested[len(nested)-1] = nested[len(nested)-1], nested[0]
		payload, err = json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		if cfg, err := Prepare(payload, opts); err == nil || cfg != nil {
			t.Fatal("nested catch-all stole specific routes")
		}
	})

	// Each counterexample is a supported-looking configuration with an unsafe
	// change. Reject it before any stock process, listener or live storage access.
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"catch-all-first", func(r map[string]any) { nested := innerRoutes(r); nested[0], nested[4] = nested[4], nested[0] }},
		// origin: hot-update S9 plan §5.1 / T18 — the route-level "terminal": true
		// flag ALONE must make a catch-all steal later routes: a rewrite handler
		// never finishes the chain, so only the flag can mark this route caught.
		{"terminal-flag-catch-all-first", func(r map[string]any) {
			innerRoutes(r)[0] = map[string]any{"group": "group0", "handle": []any{map[string]any{"handler": "rewrite", "strip_path_prefix": "/legacy"}}, "terminal": true}
		}},
		{"or-catch-all", func(r map[string]any) {
			innerRoutes(r)[0].(map[string]any)["match"].([]any)[0].(map[string]any)["path"] = []any{"/sub/*", "/*"}
		}},
		{"custom-module", func(r map[string]any) {
			innerRoutes(r)[0].(map[string]any)["handle"].([]any)[0].(map[string]any)["handler"] = "third_party"
		}},
		{"external-storage", func(r map[string]any) { r["storage"] = map[string]any{"module": "s3", "root": "/private/acme-current"} }},
		{"custom-app", func(r map[string]any) { r["apps"].(map[string]any)["exec"] = map[string]any{} }},
		{"admin-loader", func(r map[string]any) {
			r["admin"] = map[string]any{"config": map[string]any{"load": map[string]any{"module": "http"}}}
		}},
		{"other-listener", func(r map[string]any) { configServer(r)["listen"] = []any{":443", ":444"} }},
		{"second-server", func(r map[string]any) {
			r["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["srv1"] = map[string]any{}
		}},
		{"quic", func(r map[string]any) { configServer(r)["protocols"] = []any{"h1", "h3"} }},
		{"client-auth", func(r map[string]any) {
			configServer(r)["tls_connection_policies"] = []any{map[string]any{"client_authentication": map[string]any{}}}
		}},
		{"foreign-host", func(r map[string]any) {
			configServer(r)["routes"].([]any)[0].(map[string]any)["match"] = []any{map[string]any{"host": []any{"other.example"}}}
		}},
		{"dynamic-upstream", func(r map[string]any) {
			innerRoutes(r)[0].(map[string]any)["handle"].([]any)[0].(map[string]any)["upstreams"] = []any{map[string]any{"dial": "{env.PRIVATE}:8090"}}
		}},
		{"custom-transport", func(r map[string]any) {
			innerRoutes(r)[0].(map[string]any)["handle"].([]any)[0].(map[string]any)["transport"] = map[string]any{"protocol": "fastcgi"}
		}},
		{"custom-matcher", func(r map[string]any) {
			innerRoutes(r)[0].(map[string]any)["match"].([]any)[0].(map[string]any)["expression"] = "true"
		}},
		{"custom-issuer", func(r map[string]any) {
			r["apps"].(map[string]any)["tls"].(map[string]any)["automation"] = map[string]any{"policies": []any{map[string]any{"issuers": []any{map[string]any{"module": "internal"}}}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := mustObject(t, raw)
			test.mutate(root)
			payload, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if cfg, err := Prepare(payload, opts); err == nil || cfg != nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
	for name, payload := range map[string][]byte{
		"duplicate":    bytes.Replace(raw, []byte(`"apps":`), []byte(`"apps":{},"apps":`), 1),
		"trailing":     append(append([]byte(nil), raw...), []byte(` {}`)...),
		"invalid-utf8": append(append([]byte(nil), raw...), 0xff),
		"oversize":     append(append([]byte(nil), raw...), bytes.Repeat([]byte(" "), (4<<20)+1)...),
		"deep":         []byte(strings.Repeat("[", 66) + strings.Repeat("]", 66)),
	} {
		t.Run(name, func(t *testing.T) {
			if cfg, err := Prepare(payload, opts); !errors.Is(err, ErrConfig) || cfg != nil {
				t.Fatal("invalid JSON accepted")
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*Options)
	}{
		{"missing-certificate", func(o *Options) { o.Certificate += "-missing" }},
		{"wrong-host", func(o *Options) { o.Host = "wrong.example" }},
		{"relative-key", func(o *Options) { o.Key = "private.pem" }},
		{"relative-admin", func(o *Options) { o.AdminSocket = "admin.sock" }},
		{"placeholder", func(o *Options) { o.Key += "{env.PRIVATE}" }},
		{"socket-mode-injection", func(o *Options) { o.AdminSocket += "|0777" }},
		{"long-admin-socket", func(o *Options) { o.AdminSocket = "/" + strings.Repeat("s", 108) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := opts
			test.change(&changed)
			if cfg, err := Prepare(raw, changed); err == nil || cfg != nil {
				t.Fatal("invalid preparation options accepted")
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*x509.Certificate)
	}{
		{"expired", func(cert *x509.Certificate) {
			cert.NotBefore = time.Now().Add(-2 * time.Hour)
			cert.NotAfter = time.Now().Add(-time.Hour)
		}},
		{"not-yet-valid", func(cert *x509.Certificate) {
			cert.NotBefore = time.Now().Add(time.Hour)
			cert.NotAfter = time.Now().Add(2 * time.Hour)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := makeCertificate(t, test.change)
			if cfg, err := Prepare(raw, bad); !errors.Is(err, ErrCertificate) || cfg != nil {
				t.Fatal("invalid certificate lifetime accepted")
			}
		})
	}
	t.Run("bad-key", func(t *testing.T) {
		bad := certificateOptions(t)
		other := certificateOptions(t)
		bad.Key = other.Key
		if cfg, err := Prepare(raw, bad); !errors.Is(err, ErrCertificate) || cfg != nil {
			t.Fatal("mismatched key accepted")
		}
	})
	t.Run("certificate-host-mismatch", func(t *testing.T) {
		bad := opts
		bad.Host = "other.example"
		payload := bytes.ReplaceAll(raw, []byte("broker.example"), []byte(bad.Host))
		if cfg, err := Prepare(payload, bad); !errors.Is(err, ErrCertificate) || cfg != nil {
			t.Fatal("certificate not valid for configured SNI accepted")
		}
	})
	t.Run("certificate-loader-bad-key", func(t *testing.T) {
		bad := certificateOptions(t)
		root := mustObject(t, raw)
		root["apps"].(map[string]any)["tls"].(map[string]any)["certificates"] = map[string]any{"load_files": []any{map[string]any{"certificate": opts.Certificate, "key": bad.Key}}}
		payload, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		if cfg, err := Prepare(payload, opts); !errors.Is(err, ErrCertificate) || cfg != nil {
			t.Fatal("bad key in original loader accepted")
		}
	})
	t.Run("certificate-symlink", func(t *testing.T) {
		bad := opts
		bad.Certificate = filepath.Join(t.TempDir(), "link.pem")
		if err := os.Symlink(opts.Certificate, bad.Certificate); err != nil {
			t.Fatal(err)
		}
		if cfg, err := Prepare(raw, bad); !errors.Is(err, ErrCertificate) || cfg != nil {
			t.Fatal("snapshot symlink accepted")
		}
	})
	t.Run("certificate-too-large", func(t *testing.T) {
		bad := certificateOptions(t)
		data, err := os.ReadFile(bad.Certificate)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, bytes.Repeat([]byte("\n"), (1<<20)+1)...)
		if err := os.WriteFile(bad.Certificate, data, 0600); err != nil {
			t.Fatal(err)
		}
		if cfg, err := Prepare(raw, bad); !errors.Is(err, ErrCertificate) || cfg != nil {
			t.Fatal("oversized certificate accepted")
		}
	})
	t.Run("secret-errors", func(t *testing.T) {
		bad := certificateOptions(t)
		canary := "PRIVATE-CANARY-ACCOUNT-KEY"
		if err := os.WriteFile(bad.Key, []byte(canary), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Prepare(raw, bad)
		if !errors.Is(err, ErrCertificate) {
			t.Fatal("malformed PEM accepted")
		}
		for _, value := range []string{canary, hex.EncodeToString([]byte(canary)), base64.StdEncoding.EncodeToString([]byte(canary)), bad.Key} {
			if strings.Contains(err.Error(), value) {
				t.Fatal("certificate error disclosed private material")
			}
		}
	})
}

func certificateOptions(t *testing.T) Options { t.Helper(); return makeCertificate(t, nil) }

func makeCertificate(t *testing.T, change func(*x509.Certificate)) Options {
	t.Helper()
	dir := t.TempDir()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "broker.example"}, DNSNames: []string{"broker.example"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if change != nil {
		change(cert)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Host: "broker.example", AdminSocket: filepath.Join(dir, "admin.sock"), Certificate: filepath.Join(dir, "certificate.pem"), Key: filepath.Join(dir, "private.pem")}
	if err := os.WriteFile(opts.Certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opts.Key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	return opts
}

func supportedConfig(t *testing.T) []byte {
	t.Helper()
	return []byte(`{"apps":{"http":{"servers":{"srv0":{"listen":[":443"],"routes":[{"match":[{"host":["broker.example"]}],"handle":[{"handler":"subroute","routes":[{"group":"group0","match":[{"path":["/sub/*"]}],"handle":[{"handler":"reverse_proxy","headers":{"request":{"set":{"X-Tether":["specific"],"X-Empty":[]}}},"upstreams":[{"dial":"127.0.0.1:8090"}]}]},{"group":"group0","match":[{"path":["/.well-known/tether/*"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:7480"}]}]},{"group":"group0","match":[{"path":["/manifest.json"]}],"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:7480"}]}]},{"group":"group0","match":[{"path":["/health"]}],"handle":[{"handler":"static_response","body":"ready","status_code":200}]},{"group":"group0","handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"127.0.0.1:8222"}]}]}]}],"terminal":true}]}}},"tls":{"automation":{"policies":[{"issuers":[{"module":"acme","email":"ops@example.com"},{"module":"zerossl","email":"ops@example.com"}]}]}}},"storage":{"module":"file_system","root":"/private/acme-current"}}`)
}

func mustObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	return root
}

func configServer(root map[string]any) map[string]any {
	return root["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["srv0"].(map[string]any)
}
func innerRoutes(root map[string]any) []any {
	return configServer(root)["routes"].([]any)[0].(map[string]any)["handle"].([]any)[0].(map[string]any)["routes"].([]any)
}

// Independent route oracle consumes emitted JSON, using the first matching
// terminal handler. It never calls the production validator to choose a route.
func routedUpstream(value any, path string) string {
	for _, value := range value.([]any) {
		route := value.(map[string]any)
		match := route["match"] == nil
		if sets, ok := route["match"].([]any); ok {
			for _, set := range sets {
				matcher := set.(map[string]any)
				if paths, ok := matcher["path"].([]any); ok {
					for _, pattern := range paths {
						p := pattern.(string)
						if p == path || strings.HasSuffix(p, "*") && strings.HasPrefix(path, strings.TrimSuffix(p, "*")) {
							match = true
						}
					}
				} else {
					match = true
				}
			}
		}
		if !match {
			continue
		}
		for _, value := range route["handle"].([]any) {
			h := value.(map[string]any)
			switch h["handler"] {
			case "subroute":
				if result := routedUpstream(h["routes"], path); result != "" {
					return result
				}
			case "reverse_proxy":
				return h["upstreams"].([]any)[0].(map[string]any)["dial"].(string)
			case "static_response":
				return "static:" + h["body"].(string)
			}
		}
	}
	return ""
}
