package proxydial

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/nats-io/nats.go"
)

// OptionsWithin installs a bounded dialer even without a proxy. Skipping nats.go's
// unbounded pre-resolution is mandatory for direct and NO_PROXY targets too.
// The observer must be bound to the caller's logical operation: Begin("", phase)
// means the bound identity, never a new identity for a transport attempt.
// origin: hot-update S6 plan §8.2–§8.3 / T34Proxy
func OptionsWithin(env Env, deadline controlbudget.Deadline, observer controlbudget.Observer) ([]nats.Option, error) {
	remaining, err := infratransport.AdmissionRemaining(deadline)
	if err != nil {
		return nil, err
	}
	opts, err := Options(env, remaining)
	if err != nil {
		return nil, err
	}
	probe, err := infratransport.NATSOptions(func() []nats.Option { return opts })
	if err != nil {
		return nil, err
	}
	d, _ := probe.CustomDialer.(*dialer)
	if d == nil {
		d = &dialer{timeout: remaining}
	}
	d.lookup = (&net.Resolver{PreferGo: true}).LookupHost
	d.budget = &admissionBudget{deadline: time.Now().Add(remaining), observer: observer, conns: make(map[*admissionConn]struct{}), schemes: make(map[string]string), pending: make(map[*pendingConn]struct{}), dials: make(map[context.Context]context.CancelFunc)}
	return []nats.Option{func(o *nats.Options) error {
		o.CustomDialer = d
		o.SkipHostLookup = true
		if o.Timeout <= 0 || o.Timeout > remaining {
			o.Timeout = remaining
		}
		schemes, err := infratransport.ServerSchemes(o.Servers, o.Secure || o.TLSConfig != nil)
		if err != nil {
			return err
		}
		d.budget.mu.Lock()
		for address, scheme := range schemes {
			d.budget.schemes[address] = scheme
		}
		d.budget.mu.Unlock()
		previous := o.ConnectedCB
		o.ConnectedCB = func(nc *nats.Conn) {
			// This callback is a safety net. Callers also FinishWithin synchronously,
			// because nats.go dispatches callbacks asynchronously after Connect returns.
			d.budget.mu.Lock()
			owned := d.budget.connectOwned
			d.budget.mu.Unlock()
			if !owned {
				if err := d.budget.finish(nil); err != nil {
					nc.Close()
					return
				}
			}
			if previous != nil {
				previous(nc)
			}
		}
		return nil
	}}, nil
}

// DialWithin implements the planned raw cancellation contract independently of
// the S6 admission observer. Each attempt owns its ctx and proxy Close callback;
// neither a canceled resolver nor req.Write can survive this method's return.
// origin: hot-update S9 plan §4.5 / T32–T33
func (d *dialer) DialWithin(network, address string, deadline controlbudget.Deadline, stop infracontract.Stop) (net.Conn, error) {
	remaining, err := infratransport.AdmissionRemaining(deadline)
	if err != nil {
		return nil, err
	}
	// ctx-none: NATS CustomDialer has no ctx; the raw generation supplies Stop and the absolute deadline.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); <-watchDone }()
	attempt := *d
	attempt.ctx, attempt.budget, attempt.joinRaw = ctx, nil, nil
	conn, err := attempt.Dial(network, address)
	if attempt.joinRaw != nil {
		attempt.joinRaw()
	}
	if ctx.Err() != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, ctx.Err()
	}
	return conn, err
}

// Planned owns proxy-aware generations even when the environment has no proxy.
// Legacy third-party dialers must be rejected by infratransport.New before cut.
func Planned(env Env, generation uint64, deadline controlbudget.Deadline, stop <-chan struct{}) (*infratransport.Transport, error) {
	opts, err := Options(env, 0)
	if err != nil {
		return nil, err
	}
	probe, err := infratransport.NATSOptions(func() []nats.Option { return opts })
	if err != nil {
		return nil, err
	}
	return infratransport.New(generation, deadline, stop, probe.CustomDialer)
}

// FinishWithin must run immediately after each nats.Connect return, including
// errors. It clears the admission deadline on success and returns observer errors
// that nats.go may have ignored while closing a failed transport. Callers close
// any returned NATS connection if this reports an error. Reconnects then use the
// ordinary per-dial timeout, independent of the completed admission deadline.
// origin: hot-update S6 plan §8.2–§8.3 / T34Proxy / T57Proxy
func FinishWithin(opts []nats.Option, connectErr error) error {
	probe := nats.GetDefaultOptions()
	for _, opt := range opts {
		if err := opt(&probe); err != nil {
			return errors.Join(connectErr, err)
		}
	}
	d, ok := probe.CustomDialer.(*dialer)
	if !ok || d.budget == nil {
		return errors.Join(connectErr, errors.New("proxydial: missing admission dialer"))
	}
	if errors.Is(connectErr, context.Canceled) {
		a, e := d.budget.begin(controlbudget.PhaseCancel)
		connectErr = errors.Join(connectErr, e)
		if e == nil {
			connectErr = errors.Join(connectErr, d.budget.record(a, false, context.Canceled))
		}
	}
	return d.budget.finish(connectErr)
}

// There is no timer/handshake goroutine to abandon. DialContext owns cancellation
// until it returns; socket deadlines bound synchronous proxy/TLS/WS/CONNECT IO.
// A single budget owns every socket tried across the whole NATS server pool.
type admissionBudget struct {
	finishMu        sync.Mutex
	mu              sync.Mutex
	deadline        time.Time
	connectDeadline time.Time
	connectOwned    bool
	observer        controlbudget.Observer
	detached        bool
	err             error
	conns           map[*admissionConn]struct{}
	schemes         map[string]string
	pending         map[*pendingConn]struct{}
	dials           map[context.Context]context.CancelFunc
	workers         sync.WaitGroup
	failure         error
}

func (b *admissionBudget) terminalError() error { b.mu.Lock(); defer b.mu.Unlock(); return b.failure }

func (b *admissionBudget) active() bool { b.mu.Lock(); defer b.mu.Unlock(); return !b.detached }

func (b *admissionBudget) begin(phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	b.mu.Lock()
	delegated := phase == controlbudget.PhaseConnect && b.connectOwned
	b.mu.Unlock()
	if delegated {
		return controlbudget.Attempt{}, nil
	}
	if b.observer == nil {
		return controlbudget.Attempt{}, nil
	}
	a, err := b.observer.Begin("", phase)
	if err != nil {
		// nats.go may replace a dial error while walking its URL pool. Keep
		// observer overflow sticky so FinishWithin cannot report false success.
		b.mu.Lock()
		b.err = errors.Join(b.err, err)
		b.mu.Unlock()
	}
	return a, err
}

func (b *admissionBudget) record(a controlbudget.Attempt, sent bool, cause error) error {
	if a.Phase == "" && a.Ordinal == 0 {
		return nil // The enclosing NATS owner records the delegated CONNECT.
	}
	if b.observer == nil {
		return nil
	}
	err := infratransport.RecordAttempt(b.observer, a, sent, cause)
	if err != nil {
		b.mu.Lock()
		b.err = errors.Join(b.err, err)
		b.mu.Unlock()
	}
	return err
}

// SetConnectDeadline binds one URL's DNS/proxy/INFO/TLS/auth work to its share
// of the original admission. The NATS owner records that CONNECT's final auth
// outcome; standalone dialers continue to record their own CONNECT scope.
// origin: hot-update S6 internal review rv-ctl-4 / rv-ctl-8
func (d *dialer) SetConnectDeadline(deadline time.Time) error {
	if d.budget == nil {
		return errors.New("proxydial: missing admission budget")
	}
	d.budget.mu.Lock()
	defer d.budget.mu.Unlock()
	if d.budget.detached {
		return context.Canceled
	}
	if deadline.After(d.budget.deadline) {
		deadline = d.budget.deadline
	}
	d.budget.connectDeadline, d.budget.connectOwned = deadline, true
	return nil
}

// FinishConnect joins the composite NATS/inbox owner at its actual boundary.
// A standalone Options dialer has no admission owner to join, so the caller's
// outcome passes through unchanged. infraattach discovers this hook by
// interface on whatever inner dialer it wraps, so the nil budget must not be
// dereferenced: every HTTPS_PROXY agent panicked on its first connect.
// origin: hot-update S8 FIX-AGENTSLIM report (proxydial nil-budget panic)
func (d *dialer) FinishConnect(cause error) error {
	if d.budget == nil {
		return cause
	}
	return d.budget.finish(cause)
}

// AdmissionOwned reports whether FinishConnect actually joins an admission
// owner. A standalone dialer exposes the same hook as a pass-through, so its
// FinishConnect(context.Canceled) cancels nothing: a wrapper must not rely on
// it to bound an attempt, and dials through DialWithin's own stop instead.
// origin: S9 P22 resume — inner-dialer hook sweep (proxydial nil-owner class)
func (d *dialer) AdmissionOwned() bool { return d.budget != nil }

func (b *admissionBudget) finish(cause error) error {
	b.finishMu.Lock()
	defer b.finishMu.Unlock()
	b.mu.Lock()
	conns := make([]*admissionConn, 0, len(b.conns))
	if !b.detached {
		for c := range b.conns {
			conns = append(conns, c)
		}
		b.detached = true
	}
	var pending []*pendingConn
	var cancels []context.CancelFunc
	if cause != nil {
		b.failure = cause
		for c := range b.pending {
			pending = append(pending, c)
		}
		for _, cancel := range b.dials {
			cancels = append(cancels, cancel)
		}
	}
	b.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	for _, c := range pending {
		_ = c.Close()
	}
	// Cancellation closes the IO source before joining. No timeout wrapper
	// leaves a resolver or a proxy handshake running behind a returned error.
	b.workers.Wait()
	var err error
	for _, c := range conns {
		c.mu.Lock()
		c.detached = true
		if cause == nil {
			// Bypass our sticky SetDeadline override only at successful admission.
			err = errors.Join(err, c.Conn.SetDeadline(time.Time{}))
		}
		c.mu.Unlock()
		err = errors.Join(err, c.complete(cause))
		if cause != nil {
			err = errors.Join(err, c.Close())
		}
	}
	b.mu.Lock()
	b.err = errors.Join(b.err, err)
	err = b.err
	b.mu.Unlock()
	return errors.Join(cause, err)
}

func (d *dialer) dialWithin(network, address string) (net.Conn, error) {
	b := d.budget
	b.mu.Lock()
	if b.detached {
		cause := b.failure
		b.mu.Unlock()
		return nil, errors.Join(context.Canceled, cause)
	}
	b.workers.Add(1)
	deadline := b.deadline
	if !b.connectDeadline.IsZero() && b.connectDeadline.Before(deadline) {
		deadline = b.connectDeadline
	}
	b.mu.Unlock()
	defer b.workers.Done()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("proxydial: invalid target address")
	}
	a, err := b.begin(controlbudget.PhaseConnect)
	if err != nil {
		return nil, err
	}
	if !time.Now().Before(deadline) {
		return nil, errors.Join(context.DeadlineExceeded, b.record(a, false, context.DeadlineExceeded))
	}
	var c net.Conn
	if d.proxy == nil || bypassProxy(host, d.noProxy) {
		c, err = d.dialDirectWithin(network, address, deadline)
	} else {
		p, pe := b.begin(controlbudget.PhaseProxy)
		if pe != nil {
			_ = b.record(a, false, pe)
			return nil, pe
		}
		attemptDialer := *d
		switch d.proxy.Scheme {
		case "http", "https":
			c, err = attemptDialer.dialHTTPConnect(address, deadline)
		case "socks5", "socks5h":
			c, err = attemptDialer.dialSOCKS5(address, deadline)
		default:
			err = errors.New("proxydial: invalid proxy scheme")
		}
		sent := false
		if attemptDialer.hop != nil {
			sent = attemptDialer.hop.wrote()
		}
		err = errors.Join(err, b.record(p, sent, err))
	}
	if err != nil {
		if c != nil {
			_ = c.Close()
		}
		return nil, errors.Join(err, b.record(a, false, err))
	}
	wrapped := &admissionConn{Conn: c, budget: b, deadline: deadline, attempts: []controlbudget.Attempt{a}}
	// These are admission scopes, including the encrypted CONNECT response. The
	// dialer cannot inspect TLS plaintext; it never treats TCP success as auth.
	b.mu.Lock()
	scheme := b.schemes[address]
	b.mu.Unlock()
	phases := []controlbudget.AttemptPhase{}
	if scheme == "tls" || scheme == "wss" {
		phases = append(phases, controlbudget.PhaseTLS)
	}
	if scheme == "ws" || scheme == "wss" {
		phases = append(phases, controlbudget.PhaseWS)
	}
	for _, phase := range phases {
		scope, e := b.begin(phase)
		if e != nil {
			_ = wrapped.Close()
			return nil, e
		}
		wrapped.attempts = append(wrapped.attempts, scope)
	}
	if err = wrapped.SetDeadline(deadline); err != nil {
		_ = wrapped.Close()
		return nil, err
	}
	b.mu.Lock()
	if b.detached {
		b.mu.Unlock()
		_ = wrapped.Close()
		return nil, context.Canceled
	}
	b.conns[wrapped] = struct{}{}
	b.mu.Unlock()
	return wrapped, nil
}

func (d *dialer) dialDirectWithin(network, address string, deadline time.Time) (net.Conn, error) {
	// ctx-none: nats.CustomDialer exposes no caller context; the absolute operation deadline owns this dial.
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	d.budget.mu.Lock()
	if d.budget.detached {
		d.budget.mu.Unlock()
		cancel()
		return nil, context.Canceled
	}
	d.budget.dials[ctx] = cancel
	d.budget.mu.Unlock()
	defer func() { cancel(); d.budget.mu.Lock(); delete(d.budget.dials, ctx); d.budget.mu.Unlock() }()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("proxydial: invalid dial address")
	}
	hosts := []string{host}
	if net.ParseIP(host) == nil {
		a, e := d.budget.begin(controlbudget.PhaseDNS)
		if e != nil {
			return nil, e
		}
		hosts, err = d.lookup(ctx, host)
		err = errors.Join(err, d.budget.record(a, true, err))
		if err != nil {
			return nil, err
		}
	}
	var last error
	for _, ip := range hosts {
		c, e := (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(ip, port))
		if e == nil {
			pc := &pendingConn{Conn: c, budget: d.budget}
			d.budget.mu.Lock()
			if d.budget.detached {
				d.budget.mu.Unlock()
				_ = c.Close()
				return nil, context.Canceled
			}
			d.budget.pending[pc] = struct{}{}
			d.budget.mu.Unlock()
			return pc, nil
		}
		last = e
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if last == nil {
		last = errors.New("proxydial: resolver returned no addresses")
	}
	return nil, last
}

// nats.go resets and clears deadlines during websocket and CONNECT setup. The
// admission wrapper clamps all such requests, including zero, until FinishWithin.
// origin: hot-update S6 plan §8.2 / T34Proxy
type admissionConn struct {
	net.Conn
	mu        sync.Mutex
	budget    *admissionBudget
	deadline  time.Time
	detached  bool
	completed bool
	sent      bool
	attempts  []controlbudget.Attempt
}

func (c *admissionConn) clamp(t time.Time) time.Time {
	if !c.detached && (t.IsZero() || t.After(c.deadline)) {
		return c.deadline
	}
	return t
}
func (c *admissionConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.SetDeadline(c.clamp(t))
}
func (c *admissionConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.SetReadDeadline(c.clamp(t))
}
func (c *admissionConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.SetWriteDeadline(c.clamp(t))
}
func (c *admissionConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.mu.Lock()
	c.sent = c.sent || n > 0
	c.mu.Unlock()
	if err != nil {
		_ = c.complete(err)
	}
	return n, err
}
func (c *admissionConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		_ = c.complete(err)
	}
	return n, err
}
func (c *admissionConn) complete(cause error) error {
	c.mu.Lock()
	if c.completed {
		c.mu.Unlock()
		return nil
	}
	c.completed = true
	attempts, sent := c.attempts, c.sent
	c.mu.Unlock()
	var err error
	for _, a := range attempts {
		err = errors.Join(err, c.budget.record(a, sent, cause))
	}
	return err
}
func (c *admissionConn) Close() error {
	err := c.Conn.Close()
	_ = c.complete(net.ErrClosed)
	c.budget.mu.Lock()
	delete(c.budget.conns, c)
	c.budget.mu.Unlock()
	return err
}

// pendingConn is registered as soon as TCP exists, before a blocking proxy
// handshake. Cancellation can therefore close the source and join dialWithin.
type pendingConn struct {
	net.Conn
	mu     sync.Mutex
	budget *admissionBudget
	sent   bool
}

func (c *pendingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.mu.Lock()
	c.sent = c.sent || n > 0
	c.mu.Unlock()
	return n, err
}
func (c *pendingConn) wrote() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.sent }
func (c *pendingConn) Close() error {
	err := c.Conn.Close()
	c.budget.mu.Lock()
	delete(c.budget.pending, c)
	c.budget.mu.Unlock()
	return err
}
