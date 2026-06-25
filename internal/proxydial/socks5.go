package proxydial

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Hand-written SOCKS5 client (no new dependency — same tradition as the project's
// in-Go yamux tunnel and P13 shadowsocks). It ALWAYS sends the target as a
// domain name (ATYP=0x03), i.e. socks5h remote-resolution semantics, which is
// the whole point: the proxy resolves the broker name, bypassing the local
// fake-ip. socks5:// and socks5h:// are therefore equivalent here.
const (
	socks5Ver      = 0x05
	socks5NoAuth   = 0x00
	socks5UserPass = 0x02
	socks5NoAccept = 0xFF
	socks5CmdConn  = 0x01
	socks5ATYPv4   = 0x01
	socks5ATYPDom  = 0x03
	socks5ATYPv6   = 0x04
	socks5RepOK    = 0x00
	socks5AuthRFC  = 0x01 // RFC1929 username/password sub-negotiation version
)

func (d *dialer) dialSOCKS5(target string, deadline time.Time) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("proxydial: socks5 bad target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("proxydial: socks5 bad target port %q", portStr)
	}
	if net.ParseIP(host) == nil && len(host) > 255 {
		return nil, fmt.Errorf("proxydial: socks5 hostname too long (%d > 255)", len(host))
	}

	conn, err := d.dialProxyHop(deadline)
	if err != nil {
		return nil, err
	}
	if err := d.socks5Handshake(conn, host, port); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func (d *dialer) socks5Handshake(conn net.Conn, host string, port int) error {
	user, pass, haveAuth := d.proxyAuth()

	methods := []byte{socks5NoAuth}
	if haveAuth {
		methods = []byte{socks5NoAuth, socks5UserPass}
	}
	greeting := append([]byte{socks5Ver, byte(len(methods))}, methods...)
	if _, err := conn.Write(greeting); err != nil {
		return fmt.Errorf("proxydial: socks5 greeting: %w", err)
	}
	sel := make([]byte, 2)
	if _, err := io.ReadFull(conn, sel); err != nil {
		return fmt.Errorf("proxydial: socks5 method reply: %w", err)
	}
	if sel[0] != socks5Ver {
		return fmt.Errorf("proxydial: socks5 bad version 0x%02x", sel[0])
	}
	switch sel[1] {
	case socks5NoAuth:
		// no sub-negotiation
	case socks5UserPass:
		if !haveAuth {
			return fmt.Errorf("proxydial: socks5 proxy requires authentication but no credentials are set")
		}
		if err := d.socks5Auth(conn, user, pass); err != nil {
			return err
		}
	case socks5NoAccept:
		return fmt.Errorf("proxydial: socks5 proxy rejected all offered auth methods")
	default:
		return fmt.Errorf("proxydial: socks5 unsupported auth method 0x%02x", sel[1])
	}

	// CONNECT. Hostnames go as ATYP=domain (remote resolution = the fake-ip
	// 命门); IP literals go as proper ATYP=IPv4/IPv6 (RFC1928 — remote resolution
	// is meaningless for an IP and a strict server may reject a domain-wrapped IP).
	req := []byte{socks5Ver, socks5CmdConn, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			req = append(req, socks5ATYPv4)
			req = append(req, v4...)
		} else {
			req = append(req, socks5ATYPv6)
			req = append(req, ip.To16()...)
		}
	} else {
		hb := []byte(host)
		req = append(req, socks5ATYPDom, byte(len(hb)))
		req = append(req, hb...)
	}
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("proxydial: socks5 connect request: %w", err)
	}

	head := make([]byte, 4) // VER REP RSV ATYP
	if _, err := io.ReadFull(conn, head); err != nil {
		return fmt.Errorf("proxydial: socks5 connect reply: %w", err)
	}
	if head[0] != socks5Ver {
		return fmt.Errorf("proxydial: socks5 reply bad version 0x%02x", head[0])
	}
	if head[1] != socks5RepOK {
		return fmt.Errorf("proxydial: socks5 connect failed: %s", socks5RepError(head[1]))
	}
	// Consume BND.ADDR (by ATYP) + BND.PORT so the stream stays aligned for the
	// real protocol bytes that follow.
	if err := socks5DrainBND(conn, head[3]); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
		return fmt.Errorf("proxydial: socks5 reply bnd.port: %w", err)
	}
	return nil
}

func (d *dialer) socks5Auth(conn net.Conn, user, pass string) error {
	if len(user) > 255 || len(pass) > 255 {
		return fmt.Errorf("proxydial: socks5 username/password too long (max 255 bytes each)")
	}
	msg := []byte{socks5AuthRFC, byte(len(user))}
	msg = append(msg, user...)
	msg = append(msg, byte(len(pass)))
	msg = append(msg, pass...)
	if _, err := conn.Write(msg); err != nil {
		return fmt.Errorf("proxydial: socks5 auth: %w", err)
	}
	rep := make([]byte, 2)
	if _, err := io.ReadFull(conn, rep); err != nil {
		return fmt.Errorf("proxydial: socks5 auth reply: %w", err)
	}
	if rep[1] != 0x00 {
		// Never echo the password.
		return fmt.Errorf("proxydial: socks5 proxy authentication failed")
	}
	return nil
}

func socks5DrainBND(conn net.Conn, atyp byte) error {
	var n int
	switch atyp {
	case socks5ATYPv4:
		n = 4
	case socks5ATYPv6:
		n = 16
	case socks5ATYPDom:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return fmt.Errorf("proxydial: socks5 reply domain length: %w", err)
		}
		n = int(l[0])
	default:
		return fmt.Errorf("proxydial: socks5 reply bad atyp 0x%02x", atyp)
	}
	if _, err := io.ReadFull(conn, make([]byte, n)); err != nil {
		return fmt.Errorf("proxydial: socks5 reply bnd.addr: %w", err)
	}
	return nil
}

func socks5RepError(rep byte) string {
	switch rep {
	case 0x01:
		return "general SOCKS server failure"
	case 0x02:
		return "connection not allowed by ruleset"
	case 0x03:
		return "network unreachable"
	case 0x04:
		return "host unreachable"
	case 0x05:
		return "connection refused"
	case 0x06:
		return "TTL expired"
	case 0x07:
		return "command not supported"
	case 0x08:
		return "address type not supported"
	default:
		return fmt.Sprintf("SOCKS error 0x%02x", rep)
	}
}
