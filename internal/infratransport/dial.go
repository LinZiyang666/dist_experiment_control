// Package infratransport owns interruptible NATS raw generations, independently
// of the flow, exec, PTY and transfer owners that survive a planned move.
// origin: hot-update S9 plan §4.5 / T32–T33
package infratransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

// PlannedDialer promises to cancel DNS and close the proxy's raw socket before
// joining its handshake workers. A legacy Dial alone cannot make that promise.
// Implementations must join before returning, including on cancellation.
type PlannedDialer interface {
	DialWithin(network, address string, deadline infracontract.Deadline, stop infracontract.Stop) (net.Conn, error)
}

// Transport is single-generation and single-NC. Interrupt is sticky; a successor
// gets a new owner rather than reviving this one's socket registry or deadline.
type Transport struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	watchDone  chan struct{}
	workers    sync.WaitGroup
	stopped    bool
	connecting bool
	generation uint64
	sequence   uint64
	deadline   infracontract.Deadline
	dialer     PlannedDialer
	resolver   *net.Resolver
	conns      map[*Connection]struct{}
}

// New refuses an uninterruptible custom dialer before any socket is changed.
// nil selects Go's context-aware resolver and TCP dialer, bypassing libc DNS.
// The supplied deadline bounds initial CONNECT, TLS and websocket work together.
func New(generation uint64, deadline infracontract.Deadline, stop infracontract.Stop, dialer nats.CustomDialer) (*Transport, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, infracontract.ErrIncomplete
	}
	var planned PlannedDialer
	if dialer != nil {
		var ok bool
		planned, ok = dialer.(PlannedDialer)
		if !ok {
			return nil, infracontract.ErrUnsupported
		}
	}
	select {
	case <-stop:
		return nil, context.Canceled
	default:
	}
	if remaining > upgradebudget.RequestLatency {
		remaining = upgradebudget.RequestLatency
	}
	limit, err := controlbudget.After(remaining)
	if err != nil {
		return nil, err
	}
	if deadline > limit {
		deadline = limit
	}
	// ctx-root: this raw generation owns cancellation and joins its DNS/proxy workers at retirement.
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{ctx: ctx, cancel: cancel, generation: generation, deadline: deadline, dialer: planned,
		resolver: &net.Resolver{PreferGo: true}, conns: make(map[*Connection]struct{}), watchDone: make(chan struct{})}
	if stop == nil {
		close(t.watchDone)
	} else {
		go func() {
			defer close(t.watchDone)
			select {
			case <-stop:
				t.interrupt()
			case <-ctx.Done():
			}
		}()
	}
	return t, nil
}

func (t *Transport) begin() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return context.Canceled
	}
	t.workers.Add(1)
	return nil
}

// Dial implements nats.CustomDialer without exposing the generation's ctx to
// business owners. Registration checks cancellation again after the raw dial.
func (t *Transport) Dial(network, address string) (net.Conn, error) {
	if err := t.begin(); err != nil {
		return nil, err
	}
	defer t.workers.Done()
	t.mu.Lock()
	deadline := t.deadline
	t.mu.Unlock()
	if deadline == 0 {
		var err error
		deadline, err = controlbudget.After(upgradebudget.RequestLatency)
		if err != nil {
			return nil, err
		}
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(t.ctx, remaining)
	defer cancel()
	var raw net.Conn
	if t.dialer == nil {
		raw, err = (&net.Dialer{Resolver: t.resolver}).DialContext(ctx, network, address)
	} else {
		raw, err = t.dialer.DialWithin(network, address, deadline, ctx.Done())
	}
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, err
	}
	if raw == nil {
		return nil, infracontract.ErrIncomplete
	}
	ioDeadline, _ := ctx.Deadline()
	c := &Connection{Conn: raw, owner: t, deadline: ioDeadline}
	t.mu.Lock()
	// origin: hot-update S9 plan §4.5 / T32 — late raw arrivals cannot resurrect the retired generation.
	if t.stopped || ctx.Err() != nil {
		t.mu.Unlock()
		_ = raw.Close()
		return nil, context.Canceled
	}
	t.sequence++
	c.Generation, c.Sequence = t.generation, t.sequence
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	if err := c.SetDeadline(ioDeadline); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// RawConnections returns the actual owned wrappers in dial order. The census
// adapter unwraps RawConn and independently obtains kernel socket identity;
// an empty retired registry is not a migration ACK for a successor.
func (t *Transport) RawConnections() []*Connection {
	t.mu.Lock()
	defer t.mu.Unlock()
	conns := make([]*Connection, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	sort.Slice(conns, func(i, j int) bool { return conns[i].Sequence < conns[j].Sequence })
	return conns
}

// NATSOptions snapshots opaque SDK options from a context-free supplier.
// The returned SDK object is configuration, not the raw generation lifetime.
// origin: S7 internal review INTEG-M F4
// NATSOptions applies opaque SDK options for both proxy factories. Keeping
// this in one place avoids a different custom-dialer selection in the two cuts.
func NATSOptions(options func() []nats.Option) (nats.Options, error) {
	probe := nats.GetDefaultOptions()
	if options == nil {
		return probe, nil
	}
	for _, opt := range options() {
		if err := opt(&probe); err != nil {
			return probe, err
		}
	}
	return probe, nil
}

// ParseProxy preserves the proxy credential redaction contract for all callers.
func ParseProxy(raw string) (*url.URL, error) {
	pu, err := url.Parse(raw)
	if err != nil {
		// NEVER echo the raw proxy env value — it can carry credentials
		// (socks5://user:pass@host) and this error flows to CLI/agent error
		// paths (the systemd journal for agents).
		return nil, fmt.Errorf("proxydial: could not parse the proxy URL from the proxy env var " +
			"(set HTTPS_PROXY/ALL_PROXY to http(s)://host:port or socks5(h)://host:port)")
	}
	if pu.Host == "" || !supportedProxyScheme(pu.Scheme) {
		// Report only the scheme (the actionable part); never the host or
		// userinfo, which may be sensitive (same leak concern as above).
		return nil, fmt.Errorf("proxydial: unsupported or malformed proxy URL (scheme %q) "+
			"— set HTTPS_PROXY/ALL_PROXY to http(s)://host:port or socks5(h)://host:port", pu.Scheme)
	}
	return pu, nil
}

// supportedProxyScheme lists the proxy URL schemes this package understands:
// the HTTP CONNECT pair and the SOCKS5 pair.
func supportedProxyScheme(scheme string) bool {
	switch scheme {
	case "http", "https", "socks5", "socks5h":
		return true
	}
	return false
}

// SelectProxyURL picks the proxy for the control plane. The control plane is
// always a TLS target (wss/tls broker on :443), so HTTPS_PROXY/ALL_PROXY are
// the right vars; HTTP_PROXY is consulted last for convenience (the dialer only
// sees host:port and cannot tell a plaintext target apart, and proxy setups
// commonly export all three to the same value). Uppercase beats lowercase.
func SelectProxyURL(env func(string) (string, bool)) string {
	return FirstProxyEnv(env, "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy")
}

// FirstProxyEnv returns the first non-empty value among keys.
func FirstProxyEnv(env func(string) (string, bool), keys ...string) string {
	for _, key := range keys {
		if value, ok := env(key); ok && value != "" {
			return value
		}
	}
	return ""
}

// RecordAttempt preserves S6's closed diagnostic classes while the socket
// owner stays independent of its admission observer's sticky error policy.
func RecordAttempt(observer controlbudget.Observer, a controlbudget.Attempt, sent bool, cause error) error {
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	a.FinishedMonoNS, a.Sent, a.Class = now, sent, "ok"
	if cause != nil {
		a.Class = "error"
		if errors.Is(cause, context.Canceled) {
			a.Class = "canceled"
		}
		var ne net.Error
		if errors.Is(cause, context.DeadlineExceeded) || (errors.As(cause, &ne) && ne.Timeout()) {
			a.Class = "timeout"
		}
	}
	// Free-form transport errors may contain proxy credentials, hostnames or
	// server -ERR text. Only the closed diagnostic classes reach the receipt.
	return observer.Record(a)
}

// AdmissionRemaining preserves the legacy proxy admission error semantics
// while sharing the BOOTTIME clock with the new raw generation owner.
func AdmissionRemaining(deadline controlbudget.Deadline) (time.Duration, error) {
	now, err := controlbudget.Now()
	if err != nil {
		return 0, err
	}
	if deadline <= 0 || int64(deadline) <= now {
		return 0, context.DeadlineExceeded
	}
	return time.Duration(int64(deadline) - now), nil
}

// ProxyCredentials returns credentials only to the in-memory handshake owner.
func ProxyCredentials(proxy *url.URL) (user, pass string, ok bool) {
	if proxy.User == nil {
		return "", "", false
	}
	user = proxy.User.Username()
	pass, _ = proxy.User.Password()
	return user, pass, true
}

// ServerSchemes normalizes the NATS pool's TLS/WS admission scopes. This is
// shared with S6 proxy admission; moving it does not change the observer keys.
// origin: hot-update S9 plan §10.1 / S6 T34Proxy
func ServerSchemes(servers []string, secure bool) (map[string]string, error) {
	schemes := make(map[string]string)
	for _, raw := range servers {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, errors.New("proxydial: invalid broker URL")
		}
		address := u.Host
		if u.Port() == "" {
			port := "4222"
			if u.Scheme == "ws" {
				port = "80"
			}
			if u.Scheme == "wss" {
				port = "443"
			}
			address = net.JoinHostPort(u.Hostname(), port)
		}
		scheme := u.Scheme
		if secure {
			if scheme == "nats" {
				scheme = "tls"
			}
			if scheme == "ws" {
				scheme = "wss"
			}
		}
		schemes[address] = scheme
	}
	return schemes, nil
}

// The supplier carries SDK configuration without exposing its nested context
// callbacks as a public caller lifetime. This owner retains Deadline and Stop.
// origin: S7 internal review INTEG-M F4
// Connect owns the entire synchronous NATS handshake, including req.Write
// outside Dial. Interrupt can reach its raw connection before waiting for it.
func (t *Transport) Connect(server string, options func() []nats.Option) (*nats.Conn, error) {
	if err := t.begin(); err != nil {
		return nil, err
	}
	defer t.workers.Done()
	var opts []nats.Option
	if options != nil {
		opts = append(opts, options()...)
	}
	t.mu.Lock()
	if t.connecting {
		t.mu.Unlock()
		return nil, infracontract.ErrIncomplete
	}
	t.connecting = true
	t.mu.Unlock()
	// Apply last: a caller cannot restore unbounded hostname lookup or replace
	// the raw owner while claiming this generation's retirement proof.
	opts = append(opts, func(o *nats.Options) error {
		// origin: hot-update S9 plan §4.5 / T32 — a background retry cannot masquerade as the joined CONNECT receipt.
		if o.InProcessServer != nil || o.RetryOnFailedConnect {
			return infracontract.ErrUnsupported
		}
		o.CustomDialer, o.SkipHostLookup = t, true
		previous := o.ReconnectedCB
		o.ReconnectedCB = func(nc *nats.Conn) {
			t.mu.Lock()
			var err error
			if !t.stopped {
				for c := range t.conns {
					err = errors.Join(err, c.admit())
				}
			}
			t.mu.Unlock()
			if err != nil {
				t.interrupt()
				nc.Close()
				return
			}
			if previous != nil {
				previous(nc)
			}
		}
		if o.Timeout <= 0 || o.Timeout > upgradebudget.RequestLatency {
			o.Timeout = upgradebudget.RequestLatency
		}
		return nil
	},
		// origin: hot-update S9 plan §10.2 / T83 — the raw successor retains the same explicit liveness contract.
		nats.PingInterval(20*time.Second), nats.MaxPingsOutstanding(2))
	nc, err := nats.Connect(server, opts...)
	t.mu.Lock()
	stopped := t.stopped
	if err == nil && !stopped {
		for c := range t.conns {
			err = errors.Join(err, c.admit())
		}
		t.deadline = 0
	}
	t.mu.Unlock()
	if stopped {
		err = errors.Join(err, context.Canceled)
	}
	if err != nil {
		t.interrupt()
		if nc != nil {
			nc.Close()
		}
		return nil, err
	}
	return nc, nil
}
