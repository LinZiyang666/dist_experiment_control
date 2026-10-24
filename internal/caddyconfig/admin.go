package caddyconfig

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

var (
	ErrAdmin          = errors.New("infra_permission_denied")
	ErrOutcomeUnknown = errors.New("infra_result_unknown")
)

// PeerVerifier is the P03 credential adapter. It MUST read SO_PEERCRED from
// this conn, check the policy UID, and independently observe/revalidate the
// entire expected Process (including invocation, boot, ticks and running exe).
// A cached PID or pathname observation is insufficient. This injection keeps
// Linux credential/proc code in its platform owner and the root dependency
// closure free of HTTP/config code. Nil never enables an unverified connection.
type PeerVerifier func(conn *net.UnixConn, expected c.Process, uid uint32) error

// Admin owns one already-verified connection. It has no transport pool, dial
// callback or /stop verb: a reused pathname must never retire its successor.
// Retirement belongs to P02's expected-pidfd SignalGraceful implementation.
type Admin struct {
	mu       sync.Mutex
	conn     *net.UnixConn
	reader   *bufio.Reader
	expected c.Process
	uid      uint32
	verify   PeerVerifier
	socket   string
	closed   bool
}

func (a *Admin) String() string             { return "caddy-admin" }
func (a *Admin) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, a.String()) }

// OpenAdmin binds a per-invocation 0600 socket under a 0700 directory. Shared
// group access is deliberately unsupported until an explicit root policy and
// verifier authorize that mode; localhost and HTTP Origin never authorize UID.
// origin: hot-update S9 plan §5.1 / T19
func OpenAdmin(ctx context.Context, socket string, expected c.Process, uid uint32, verify PeerVerifier) (*Admin, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return nil, c.ErrUnsupported
	}
	if ctx == nil || !privatePath(socket) || verify == nil || uid == 0 || !completeProcess(expected) {
		return nil, ErrAdmin
	}
	ctx, cancel := context.WithTimeout(ctx, upgradebudget.RequestLatency)
	defer cancel()
	parent, err := os.Lstat(filepath.Dir(socket))
	if err != nil || !parent.IsDir() || parent.Mode().Perm() != 0700 {
		return nil, ErrAdmin
	}
	before, err := os.Lstat(socket)
	if err != nil || before.Mode()&os.ModeSocket == 0 || before.Mode().Perm() != 0600 {
		return nil, ErrAdmin
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, ErrAdmin
	}
	u, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, ErrAdmin
	}
	after, err := os.Lstat(socket)
	if err != nil || !os.SameFile(before, after) || verify(u, expected, uid) != nil {
		_ = u.Close()
		return nil, ErrAdmin
	}
	return &Admin{conn: u, reader: bufio.NewReader(u), expected: expected, uid: uid, verify: verify, socket: socket}, nil
}

func completeProcess(p c.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" && p.ExeDevice > 0 && p.ExeInode > 0 && p.ExeSHA256 != (c.Digest{}) && p.Cgroup != ""
}

func (a *Admin) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.close()
}

func (a *Admin) close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	return a.conn.Close()
}

// Current returns only the canonical configuration digest, never private paths
// or response text. After a lost /load reply, a freshly verified binding to the
// SAME expected invocation must query this before making another load decision.
func (a *Admin) Current(ctx context.Context) (c.Digest, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current(ctx)
}

func (a *Admin) current(ctx context.Context) (c.Digest, error) {
	body, err := a.exchange(ctx, http.MethodGet, "/config/", nil)
	if err != nil {
		return c.Digest{}, err
	}
	root, err := decodeObject(body)
	if err != nil {
		_ = a.close()
		return c.Digest{}, ErrAdmin
	}
	canonical, err := json.Marshal(root)
	if err != nil {
		return c.Digest{}, ErrAdmin
	}
	return sha256.Sum256(canonical), nil
}

func (a *Admin) exchange(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	// origin: hot-update S9 plan §5.1 / T19
	// The verifier rechecks policy and the original peer for every exchange.
	// A previously valid UID/PID observation cannot authorize a late load after
	// an invocation or policy transition. No filesystem lookup replaces this FD.
	if ctx == nil || a.closed || ctx.Err() != nil || a.verify(a.conn, a.expected, a.uid) != nil {
		return nil, ErrAdmin
	}
	deadline := time.Now().Add(upgradebudget.RequestLatency)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := a.conn.SetDeadline(deadline); err != nil {
		_ = a.close()
		return nil, ErrAdmin
	}
	// Cancellation closes the original socket even during a blocked read/write.
	// Join the callback before the next exchange can reuse or clear its deadline.
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = a.conn.SetDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	req, err := http.NewRequestWithContext(ctx, method, "http://127.0.0.1"+path, bytes.NewReader(body))
	if err != nil {
		return nil, ErrAdmin
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if err := req.Write(a.conn); err != nil {
		_ = a.close()
		return nil, adminFailure(method)
	}
	resp, err := http.ReadResponse(a.reader, req)
	if err != nil {
		_ = a.close()
		return nil, adminFailure(method)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxConfigBytes+1))
	_ = resp.Body.Close()
	if err != nil || len(data) > maxConfigBytes || resp.Close {
		_ = a.close()
		return nil, adminFailure(method)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrAdmin
	}
	return data, nil
}

func adminFailure(method string) error {
	if method == http.MethodPost {
		return ErrOutcomeUnknown
	}
	return ErrAdmin
}
