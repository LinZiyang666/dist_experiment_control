package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Protocol readiness and physical attribution are separate. The caller keeps
// this exact connection open while P02 joins its accepted socket to main.
// origin: hot-update S9 plan §2.3 / §5.1; S9 INSTRUMENTS-3.
func serviceProbe(ctx context.Context, p probe, deadline c.Deadline) (net.Conn, error) {
	host, _, err := net.SplitHostPort(p.Address)
	if err != nil || net.ParseIP(host) == nil {
		return nil, c.ErrIncomplete
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	held, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.Address)
	if err != nil {
		return nil, c.ErrIncomplete
	}
	accepted := false
	defer func() {
		if !accepted {
			_ = held.Close()
		}
	}()
	if err := held.SetDeadline(time.Now().Add(remaining)); err != nil {
		return nil, c.ErrIncomplete
	}
	switch p.Protocol {
	case "nats":
		// TCP may split the greeting at any byte. A complete bounded INFO with
		// an actual serverID is required; a prefix or unrelated port is not ready.
		line, readErr := bufio.NewReader(io.LimitReader(held, 4096)).ReadString('\n')
		var info struct {
			ServerID string `json:"server_id"`
		}
		if readErr != nil || !strings.HasPrefix(line, "INFO ") ||
			json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "INFO "))), &info) != nil || info.ServerID == "" {
			return nil, c.ErrIncomplete
		}
	case "tls":
		ca, readErr := os.ReadFile(p.CAFile)
		roots := x509.NewCertPool()
		if readErr != nil || !roots.AppendCertsFromPEM(ca) || p.ServerName == "" {
			return nil, c.ErrIncomplete
		}
		secure := tls.Client(held, &tls.Config{RootCAs: roots, ServerName: p.ServerName, MinVersion: tls.VersionTLS12})
		if err := secure.HandshakeContext(ctx); err != nil {
			return nil, c.ErrIncomplete
		}
		held = secure
	default:
		return nil, c.ErrUnsupported
	}
	accepted = true
	return held, nil
}
