package proxydial

import (
	"bufio"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"
)

// bufConn wraps a conn whose initial bytes may have been buffered by the
// bufio.Reader used to parse the CONNECT response. Read drains the buffer first.
//
// LOAD-BEARING: http.ReadResponse can read ahead past the response's blank line
// into the first tunnel bytes, and a NATS server sends its INFO line IMMEDIATELY
// on connect (very likely in the same TCP segment as the 200 CONNECT response),
// so returning a bare conn would silently swallow INFO and hang the handshake.
type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufConn) Read(p []byte) (int, error) { return b.r.Read(p) }

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
	resp, err := http.ReadResponse(br, &http.Request{Method: "CONNECT"})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("proxydial: read CONNECT response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		// Report only the status — never echo the Proxy-Authorization header.
		return nil, fmt.Errorf("proxydial: proxy CONNECT to %s rejected: %s", target, resp.Status)
	}
	// Clear the dial deadline so it does not bleed into nats.go's later I/O.
	_ = conn.SetDeadline(time.Time{})
	return &bufConn{Conn: conn, r: br}, nil
}
