// Package infracaddy_test exercises real configuration preparation by default.
// Tagged tests add supervised stock Caddy processes and embedded NATS with WSS.
// Host units, nft ingress and root retrofit require the separate drill 86 cells.
package infracaddy_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/caddyconfig"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

const fixtureHost = "infra.test"

// origin: hot-update S9 plan §5.1 / T64 / M10
func TestCaddyPreflightPreservesRoutesAndStorageIsolation(t *testing.T) {
	cert, key, _ := issueFixtureCertificate(t, t.TempDir(), 1)
	for _, shadow := range []bool{false, true} {
		runtimeDir := t.TempDir()
		raw := stockConfig(cert, key, filepath.Join(runtimeDir, "live-storage"), "127.0.0.1:8222", "revision-A")
		cfg, err := caddyconfig.Prepare(raw, caddyconfig.Options{Host: fixtureHost,
			AdminSocket: filepath.Join(runtimeDir, "admin.sock"), Shadow: shadow, Certificate: cert, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		before, after := configObject(t, raw), configObject(t, cfg.Bytes())
		originalHTTP := before["apps"].(map[string]any)["http"].(map[string]any)
		httpApp := after["apps"].(map[string]any)["http"].(map[string]any)
		original := originalHTTP["servers"].(map[string]any)["main"].(map[string]any)
		server := httpApp["servers"].(map[string]any)["main"].(map[string]any)
		if !reflect.DeepEqual(original["routes"], server["routes"]) {
			t.Fatal("specific routes or their order changed during preparation")
		}
		if httpApp["grace_period"] != upgradebudget.RequestLatency.String() || httpApp["shutdown_delay"] != "0s" ||
			!reflect.DeepEqual(server["protocols"], []any{"h1", "h2"}) {
			t.Fatal("TCP protocol and finite shutdown policy missing")
		}
		admin := after["admin"].(map[string]any)
		persist, hasPersist := admin["config"].(map[string]any)["persist"].(bool)
		if admin["listen"] != "unix/"+filepath.Join(runtimeDir, "admin.sock")+"|0600" ||
			!hasPersist || persist {
			t.Fatal("admin is not a private invocation socket")
		}
		if cfg.Digest() != c.Digest(sha256.Sum256(cfg.Bytes())) {
			t.Fatal("configuration receipt does not bind the loaded bytes")
		}
		if shadow {
			disabled, hasDisable := server["automatic_https"].(map[string]any)["disable"].(bool)
			if !reflect.DeepEqual(server["listen"], []any{":8443"}) ||
				!hasDisable || !disabled {
				t.Fatal("warm entry may bind canonical ingress or become an ACME writer")
			}
			tlsApp := after["apps"].(map[string]any)["tls"].(map[string]any)
			if _, exists := tlsApp["automation"]; exists {
				t.Fatal("warm entry retains certificate issuers")
			}
			if after["storage"].(map[string]any)["root"] != filepath.Join(runtimeDir, "storage") {
				t.Fatal("warm housekeeping can modify the live storage tree")
			}
		} else if !reflect.DeepEqual(before["storage"], after["storage"]) {
			t.Fatal("main lost its current filesystem storage selector")
		}
		// Bytes is an ownership boundary: a reader cannot change the config later
		// submitted to the same invocation's admin connection.
		copy := cfg.Bytes()
		copy[0] ^= 1
		if bytes.Equal(copy, cfg.Bytes()) {
			t.Fatal("configuration exposes mutable owned bytes")
		}
	}
}

// origin: hot-update S9 plan §5.1 / T64 / M10
func TestCaddyPreflightRejectsUnsafeLiveResponsibilities(t *testing.T) {
	dir := t.TempDir()
	cert, key, _ := issueFixtureCertificate(t, dir, 1)
	raw := stockConfig(cert, key, filepath.Join(dir, "live-storage"), "127.0.0.1:8222", "revision-A")
	opts := caddyconfig.Options{Host: fixtureHost, AdminSocket: filepath.Join(dir, "admin.sock"),
		Shadow: true, Certificate: cert, Key: key}
	cases := []struct {
		name string
		edit func(map[string]any)
	}{
		{"catch_all_before_subscription", func(root map[string]any) {
			s := configServer(root)
			routes := s["routes"].([]any)
			s["routes"] = append([]any{routes[len(routes)-1]}, routes[:len(routes)-1]...)
		}},
		{"active_quic", func(root map[string]any) { configServer(root)["protocols"] = []string{"h1", "h2", "h3"} }},
		{"external_storage", func(root map[string]any) { root["storage"] = map[string]any{"module": "s3"} }},
		{"custom_module", func(root map[string]any) {
			configServer(root)["routes"] = []any{map[string]any{"handle": []any{map[string]any{"handler": "unowned_module"}}}}
		}},
		{"unobserved_listener", func(root map[string]any) { configServer(root)["listen"] = []string{":443", ":9443"} }},
		{"unowned_application", func(root map[string]any) { root["apps"].(map[string]any)["unowned"] = map[string]any{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			object := configObject(t, raw)
			tc.edit(object)
			changed, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			if cfg, err := caddyconfig.Prepare(changed, opts); err == nil || cfg != nil {
				t.Fatal("unsafe responsibility admitted before effect")
			}
		})
	}
	t.Run("bad_certificate", func(t *testing.T) {
		bad := opts
		bad.Host = "other.test"
		if cfg, err := caddyconfig.Prepare(raw, bad); !errors.Is(err, caddyconfig.ErrCertificate) || cfg != nil {
			t.Fatal("wrong-host certificate admitted before effect")
		}
	})
	// A failed preflight must not create a storage tree, listener or admin endpoint.
	for _, path := range []string{filepath.Join(dir, "live-storage"), opts.AdminSocket, filepath.Join(dir, "storage")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("preflight changed a live responsibility")
		}
	}
}

func configObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	return root
}

func configServer(root map[string]any) map[string]any {
	return root["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["main"].(map[string]any)
}

func stockConfig(cert, key, storage, upstream, revision string) []byte {
	root := map[string]any{
		"storage": map[string]any{"module": "file_system", "root": storage},
		"apps": map[string]any{
			"tls": map[string]any{"certificates": map[string]any{"load_files": []any{map[string]any{"certificate": cert, "key": key}}}},
			"http": map[string]any{"servers": map[string]any{"main": map[string]any{
				"listen": []string{":443"}, "protocols": []string{"h1", "h2"},
				"automatic_https": map[string]any{"disable": true}, "tls_connection_policies": []any{map[string]any{}},
				"routes": []any{
					map[string]any{"match": []any{map[string]any{"path": []string{"/sub/*"}}},
						"handle": []any{map[string]any{"handler": "static_response", "body": "subscription-" + revision}}},
					map[string]any{"match": []any{map[string]any{"path": []string{"/.well-known/*", "/manifest"}}},
						"handle": []any{map[string]any{"handler": "static_response", "body": "manifest-" + revision}}},
					map[string]any{"handle": []any{map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]any{"dial": upstream}}}}},
				},
			}}},
		},
	}
	data, err := json.Marshal(root)
	if err != nil {
		panic(err) // The fixture is a closed set of JSON-compatible values.
	}
	return data
}

func issueFixtureCertificate(t *testing.T, dir string, serial int64) (certPath, keyPath string, cert *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: fixtureHost},
		DNSNames: []string{fixtureHost}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath = filepath.Join(dir, "served.crt"), filepath.Join(dir, "served.key")
	writeFixtureFile(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
	writeFixtureFile(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600)
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath, cert
}

func writeFixtureFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}
