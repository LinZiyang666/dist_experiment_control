// Package proxydial makes tether's control-plane NATS connection proxy-aware.
//
// tether is a static (CGO_ENABLED=0) binary that resolves the broker hostname
// and dials it directly; it honors no proxy env on its own. On a host whose
// transparent TUN cannot carry the connection (e.g. WSL mirrored-network TUN +
// Clash fake-ip), the direct dial hangs. This package lets the ctl/agent route
// the NATS connection through a local proxy (HTTP CONNECT or SOCKS5) when a
// proxy env var is set.
//
// Two things make the bypass actually work, both load-bearing (see Options):
//
//   - nats.SetCustomDialer: route the underlying TCP dial through the proxy.
//     nats.go uses the CustomDialer for the TCP connection under ALL schemes
//     (nats/tls/ws/wss) BEFORE any TLS/WebSocket handshake, and does its own
//     TLS afterwards (we never implement SkipTLSHandshake), so TLS/SNI/cert
//     validation stay end-to-end through the proxy — the proxy only carries
//     raw TCP.
//   - nats.SkipHostLookup: by default nats.go pre-resolves the hostname with
//     net.LookupHost BEFORE calling the dialer (nats.go createConn). On a
//     fake-ip host that yields a dead 198.18.x.x address. SkipHostLookup makes
//     nats.go hand the dialer the hostname instead, so the dialer can give the
//     hostname to the proxy for REMOTE DNS resolution — which is the whole
//     point. The dialer therefore always passes the hostname to the proxy
//     (HTTP CONNECT by name / SOCKS5 ATYP=domainname).
//
// Scope: control plane only. The reverse data-plane tunnel (agent->broker:7000)
// is NOT proxied here.
package proxydial

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/nats-io/nats.go"
)

// Env abstracts env lookup so tests can inject a fake map without t.Setenv
// serialization.
type Env func(key string) (string, bool)

// OSEnv is the production Env backed by the process environment.
func OSEnv(key string) (string, bool) { return os.LookupEnv(key) }

// Options returns the nats.Options that make the connection proxy-aware, based
// ONLY on the presence of a proxy env var. It is the single decision point for
// "attach a proxy dialer or not":
//
//   - No proxy env set -> returns (nil, nil). The caller appends nothing, so
//     the nats.Options are byte-identical to the no-proxy build (zero
//     regression — nats.go keeps its native multi-IP fan-out + local DNS).
//   - A proxy env set but unparseable / unsupported scheme -> returns an error
//     (FAIL-CLOSED: a user who set a proxy must not silently fall back to a
//     direct dial that will just hang again on a fake-ip host).
//   - A usable proxy env set -> returns [SetCustomDialer, SkipHostLookup].
//
// The per-target NO_PROXY decision happens inside the dialer at Dial-time on the
// real address nats.go hands it (a clean host:port with path/query stripped),
// so multi-URL (comma) and URL-path broker URLs are handled correctly without
// this function ever parsing the broker URL.
//
// dialTimeout bounds each proxy dial; thread the call site's budget here (a
// normal ~10s for ctl/agent, a short one for shell completion so a ctx-cancel
// can't leave a goroutine lingering past the tab-completion budget).
func Options(env Env, dialTimeout time.Duration) ([]nats.Option, error) {
	raw := selectProxyURL(env)
	if raw == "" {
		return nil, nil // zero-regression path
	}
	pu, err := infratransport.ParseProxy(raw)
	if err != nil {
		return nil, err
	}
	d := &dialer{
		proxy:   pu,
		noProxy: firstEnv(env, "NO_PROXY", "no_proxy"),
		timeout: dialTimeout,
	}
	return []nats.Option{nats.SetCustomDialer(d), nats.SkipHostLookup()}, nil
}

// selectProxyURL picks the proxy for the control plane. The control plane is
// always a TLS target (wss/tls broker on :443), so HTTPS_PROXY/ALL_PROXY are
// the right vars; HTTP_PROXY is consulted last for convenience (the dialer only
// sees host:port and cannot tell a plaintext target apart, and proxy setups
// commonly export all three to the same value). Uppercase beats lowercase.
func selectProxyURL(env Env) string {
	return infratransport.SelectProxyURL(env)
}

// firstEnv returns the first non-empty value among keys.
func firstEnv(env Env, keys ...string) string {
	return infratransport.FirstProxyEnv(env, keys...)
}

// dialer implements nats.CustomDialer. It deliberately does NOT implement
// SkipTLSHandshake, so nats.go performs the TLS handshake on the conn we return
// (TLS stays end-to-end through the proxy).
type dialer struct {
	proxy   *url.URL
	noProxy string
	timeout time.Duration
	hop     *pendingConn
	budget  *admissionBudget
	lookup  func(context.Context, string) ([]string, error)
	ctx     context.Context
	joinRaw func()
}

// Dial routes a single TCP connection to address through the configured proxy,
// unless NO_PROXY (or the built-in loopback bypass) exempts the target host, in
// which case it dials directly. address is the clean host:port nats.go derived
// from the broker URL.
func (d *dialer) Dial(network, address string) (net.Conn, error) {
	if d.budget != nil {
		if err := d.budget.terminalError(); err != nil {
			return nil, err
		}
	}
	if d.budget != nil && d.budget.active() {
		return d.dialWithin(network, address)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("proxydial: bad target address %q: %w", address, err)
	}
	// NO_PROXY (incl. the built-in loopback bypass) is evaluated ONLY against
	// the target host, never against the proxy hop (the proxy itself is dialed
	// directly below — e.g. Clash on 127.0.0.1:7897 is normal).
	if d.proxy == nil || bypassProxy(host, d.noProxy) {
		if d.ctx != nil {
			return (&net.Dialer{Resolver: &net.Resolver{PreferGo: true}, Timeout: d.timeout}).DialContext(d.ctx, network, address)
		}
		return (&net.Dialer{Timeout: d.timeout}).Dial(network, address)
	}
	// ONE end-to-end deadline for the whole proxy dial (proxy-hop TCP dial +
	// CONNECT/SOCKS handshake), so the call site's budget (e.g. completion's
	// 750ms) is not stacked twice into ~2x.
	var deadline time.Time
	if d.timeout > 0 {
		deadline = time.Now().Add(d.timeout)
	}
	if d.ctx != nil {
		if end, ok := d.ctx.Deadline(); ok && (deadline.IsZero() || end.Before(deadline)) {
			deadline = end
		}
	}
	switch d.proxy.Scheme {
	case "http", "https":
		return d.dialHTTPConnect(address, deadline)
	case "socks5", "socks5h":
		return d.dialSOCKS5(address, deadline)
	default:
		// Unreachable: Options validated the scheme. Fail closed anyway.
		return nil, fmt.Errorf("proxydial: unsupported proxy scheme %q", d.proxy.Scheme)
	}
}

// proxyAuth returns (user, pass, ok) from the proxy URL userinfo.
func (d *dialer) proxyAuth() (user, pass string, ok bool) {
	return infratransport.ProxyCredentials(d.proxy)
}

// dialProxyHop dials the proxy server itself (direct, never via NO_PROXY),
// bounded by the shared end-to-end deadline.
func (d *dialer) dialProxyHop(deadline time.Time) (net.Conn, error) {
	addr := d.proxy.Host
	if _, _, err := net.SplitHostPort(addr); err != nil {
		// No port: default per scheme.
		switch d.proxy.Scheme {
		case "http":
			addr = net.JoinHostPort(addr, "80")
		case "https":
			addr = net.JoinHostPort(addr, "443")
		default: // socks5(h)
			addr = net.JoinHostPort(addr, "1080")
		}
	}
	var c net.Conn
	var err error
	if d.budget != nil && d.budget.active() {
		c, err = d.dialDirectWithin("tcp", addr, deadline)
		d.hop, _ = c.(*pendingConn)
	} else if d.ctx != nil {
		c, err = (&net.Dialer{Resolver: &net.Resolver{PreferGo: true}, Deadline: deadline}).DialContext(d.ctx, "tcp", addr)
	} else {
		c, err = (&net.Dialer{Deadline: deadline}).Dial("tcp", addr)
	}
	if err != nil {
		// addr is host:port only — url.URL.Host never carries userinfo (that's
		// in url.URL.User), so there's nothing to redact here.
		return nil, fmt.Errorf("proxydial: dial proxy %s: %w", addr, err)
	}
	if d.ctx != nil {
		done := make(chan struct{})
		stop := context.AfterFunc(d.ctx, func() { _ = c.Close(); close(done) })
		d.joinRaw = func() {
			if !stop() {
				<-done
			}
		}
	}
	if !deadline.IsZero() {
		_ = c.SetDeadline(deadline)
	}
	return c, nil
}
