package proxydial

import (
	"bufio"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/LinZiyang666/tether/internal/infratransport"
)

// dialHTTPConnect opens a tunnel to target via an HTTP(S) CONNECT proxy,
// bounded by the shared end-to-end deadline.
func (d *dialer) dialHTTPConnect(target string, deadline time.Time) (net.Conn, error) {
	conn, err := d.dialProxyHop(deadline)
	if err != nil {
		return nil, err
	}
	// For an https:// proxy the hop to the proxy itself is TLS (ServerName =
	// proxy host). The end-to-end TLS to the broker is still done later by
	// nats.go over this tunnel.
	if d.proxy.Scheme == "https" {
		tconn := tls.Client(conn, &tls.Config{ServerName: d.proxy.Hostname()})
		//nolint:noctx // nats.go's CustomDialer interface is Dial(network, address) -- no ctx exists to
		// thread, and the handshake is already bounded by the shared end-to-end deadline this dialer
		// carries on the conn.
		if err := tconn.Handshake(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("proxydial: TLS handshake to https proxy: %w", err)
		}
		conn = tconn
	}

	req := "CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n"
	if user, pass, ok := d.proxyAuth(); ok {
		cred := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
		req += "Proxy-Authorization: Basic " + cred + "\r\n"
	}
	req += "\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("proxydial: write CONNECT: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("proxydial: read CONNECT response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// A rejection MAY carry an explanatory body; close it before dropping the connection.
		_ = resp.Body.Close()
		_ = conn.Close()
		// Report only the status — never echo the Proxy-Authorization header.
		return nil, fmt.Errorf("proxydial: proxy CONNECT to %s rejected: %s", target, resp.Status)
	}
	// On 2xx the connection BECOMES the tunnel, so resp.Body is deliberately not closed here: Go gives
	// a successful CONNECT response no body (http.NoBody), and closing a body layered over `br` would
	// mean closing the very connection we are about to hand back to the caller.
	// Legacy callers clear the dial deadline so it does not bleed into later I/O.
	// origin: hot-update S6 plan §8.2 / T34Proxy
	// CONNECT/SOCKS success is only the proxy hop: TLS/WSS and NATS
	// CONNECT still owe the same deadline. Legacy callers detach here.
	if d.budget == nil || !d.budget.active() {
		_ = conn.SetDeadline(time.Time{})
	}
	return infratransport.Buffered(conn, br), nil
}
