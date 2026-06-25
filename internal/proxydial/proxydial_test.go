package proxydial

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// fakeEnv builds an Env from a map.
func fakeEnv(m map[string]string) Env {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

// applyToProbe applies the options to a fresh nats.Options to inspect them
// (nats.Option is an opaque func; this is the only way to assert their effect).
func applyToProbe(t *testing.T, opts []nats.Option) nats.Options {
	t.Helper()
	var o nats.Options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	return o
}

// TestOptions_ZeroRegression: no proxy env -> nil options -> a probe shows no
// CustomDialer and no SkipHostLookup (byte-identical to the no-proxy build).
func TestOptions_ZeroRegression(t *testing.T) {
	for _, m := range []map[string]string{
		{},
		{"NO_PROXY": "example.com"}, // NO_PROXY alone is not a proxy
		{"no_proxy": "example.com"}, // lowercase NO_PROXY alone, likewise
		{"HTTPS_PROXY": ""},         // empty value = unset
		{"all_proxy": ""},           // empty lowercase value = unset
		{"FTP_PROXY": "http://x:1"}, // unrelated var
	} {
		opts, err := Options(fakeEnv(m), time.Second)
		if err != nil {
			t.Fatalf("env %v: unexpected error: %v", m, err)
		}
		if opts != nil {
			t.Fatalf("env %v: expected nil options (zero regression), got %d", m, len(opts))
		}
		o := applyToProbe(t, opts)
		if o.CustomDialer != nil || o.SkipHostLookup {
			t.Fatalf("env %v: zero-regression violated: CustomDialer=%v SkipHostLookup=%v", m, o.CustomDialer != nil, o.SkipHostLookup)
		}
	}
}

// TestOptions_AttachesDialerAndSkipHostLookup: a usable proxy env -> both
// SetCustomDialer and SkipHostLookup are present (SkipHostLookup is mandatory,
// else nats.go pre-resolves to a fake-ip).
func TestOptions_AttachesDialerAndSkipHostLookup(t *testing.T) {
	opts, err := Options(fakeEnv(map[string]string{"HTTPS_PROXY": "http://127.0.0.1:7897"}), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	o := applyToProbe(t, opts)
	if o.CustomDialer == nil {
		t.Error("expected a CustomDialer")
	}
	if !o.SkipHostLookup {
		t.Error("SkipHostLookup MUST be set with the proxy dialer (fake-ip命门)")
	}
}

// TestOptions_Precedence: uppercase beats lowercase; HTTPS_PROXY beats ALL_PROXY
// beats HTTP_PROXY.
func TestOptions_Precedence(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string // expected dialer.proxy.Host
	}{
		{"https_over_all", map[string]string{"HTTPS_PROXY": "http://h:1", "ALL_PROXY": "http://a:2"}, "h:1"},
		{"all_over_http", map[string]string{"ALL_PROXY": "http://a:2", "HTTP_PROXY": "http://p:3"}, "a:2"},
		{"upper_over_lower", map[string]string{"HTTPS_PROXY": "http://U:1", "https_proxy": "http://l:2"}, "U:1"},
		{"lower_when_no_upper", map[string]string{"all_proxy": "socks5://l:9"}, "l:9"},
		// Precedence is PER-KEY: HTTPS before ALL before HTTP, each upper-then-
		// lower. So a lowercase https_proxy beats an uppercase ALL_PROXY.
		{"lower_https_over_upper_all", map[string]string{"https_proxy": "http://lo:1", "ALL_PROXY": "http://up:2"}, "lo:1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, err := Options(fakeEnv(c.env), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			o := applyToProbe(t, opts)
			d, ok := o.CustomDialer.(*dialer)
			if !ok {
				t.Fatalf("expected *dialer, got %T", o.CustomDialer)
			}
			if d.proxy.Host != c.want {
				t.Errorf("selected proxy host = %q, want %q", d.proxy.Host, c.want)
			}
		})
	}
}

// TestOptions_FailClosed: a set-but-malformed/unsupported proxy URL errors
// (never silently falls back to a direct dial that would just hang again).
func TestOptions_FailClosed(t *testing.T) {
	for _, bad := range []string{
		"://nohost",
		"ftp://host:21",
		"http://", // no host
		"not a url with spaces",
	} {
		_, err := Options(fakeEnv(map[string]string{"ALL_PROXY": bad}), time.Second)
		if err == nil {
			t.Errorf("proxy URL %q must fail closed, got nil error", bad)
		}
	}
}

// TestOptions_FailClosedNoCredentialLeak: a set-but-invalid proxy URL fails
// closed WITHOUT echoing any credential (or userinfo/host) from the proxy env —
// the error flows to CLI/agent error paths and the agent's systemd journal.
func TestOptions_FailClosedNoCredentialLeak(t *testing.T) {
	cases := []struct {
		raw     string
		secrets []string // none of these may appear in the error
	}{
		{"socks6://alice:s3cretpass@127.0.0.1:7897", []string{"s3cretpass", "alice", "127.0.0.1"}},
		{"ftp://bob:hunter2@proxy.internal:21", []string{"hunter2", "bob", "proxy.internal"}},
		{"http://carol:pw@[::1", []string{"pw", "carol"}}, // unbalanced bracket -> url.Parse fails
	}
	for _, c := range cases {
		_, err := Options(fakeEnv(map[string]string{"ALL_PROXY": c.raw}), time.Second)
		if err == nil {
			t.Errorf("%q must fail closed", c.raw)
			continue
		}
		for _, s := range c.secrets {
			if strings.Contains(err.Error(), s) {
				t.Errorf("error for %q leaked %q: %v", c.raw, s, err)
			}
		}
	}
}

// TestSelectProxyURL covers the raw selection helper directly.
func TestSelectProxyURL(t *testing.T) {
	if got := selectProxyURL(fakeEnv(map[string]string{})); got != "" {
		t.Errorf("empty env -> %q, want \"\"", got)
	}
	if got := selectProxyURL(fakeEnv(map[string]string{"http_proxy": "http://x:1"})); got != "http://x:1" {
		t.Errorf("http_proxy fallback -> %q", got)
	}
}
