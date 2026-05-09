package adminsock

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Client is a one-shot Unix-socket admin client. Each Call opens a
// new connection (matching the server's one-request-per-conn
// protocol). For a CLI tool that runs once per invocation that's
// the right shape; a long-lived admin daemon would want to pool.
type Client struct {
	Path    string
	Timeout time.Duration
}

// Call sends req over the socket and decodes one Response. The
// connection is closed before Call returns. Errors come from
// connect / write / read / decode; protocol-level failures (e.g.
// "unknown op") arrive in Response.Error.
func (c *Client) Call(req Request) (*Response, error) {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	conn, err := net.DialTimeout("unix", c.Path, timeout)
	if err != nil {
		return nil, fmt.Errorf("adminsock client: dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	body, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("adminsock client: marshal: %w", err)
	}
	body = append(body, '\n')
	_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(body); err != nil {
		return nil, fmt.Errorf("adminsock client: write: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("adminsock client: read: %w", err)
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("adminsock client: decode: %w", err)
	}
	return &resp, nil
}
