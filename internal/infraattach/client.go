// Package infraattach composes client NC admission, raw retirement and the
// signed route bridge. Business owners never belong to a connection's lifetime.
// origin: hot-update S9 plan §4.4–§4.6 / P23 / T50 / T51
package infraattach

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/inframaint"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/ingressmove"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

// Config is an in-memory composition port, never an image or public DTO.
// Authentication supplies P17's LOCAL normal-queue proof; Recovery performs
// P18's ACK-prefix read before a raw business dial can replay any subscription.
// Observe joins the actual raw socket to P02's accepted ingress witness. Rebind
// finishes normal REGISTER/private RPC, retaining the original home baseline.
// Missing ports cannot install or advertise infra movement capability.
type Config struct {
	Registry       *ingressmove.Registry
	Role           string
	Authentication func(string, c.Deadline, c.Stop) (infraauth.Receipt, error)
	Recovery       func(infraauth.Receipt, c.Deadline, c.Stop) error
	Observe        func(*nats.Conn, net.Conn, c.ID, uint64, c.Deadline) (c.Attachment, error)
	Rebind         func(*nats.Conn, c.Deadline) error
	Domain         c.ID
	KeyID          string
	Key            ed25519.PublicKey
	View           func() inframaint.View
	Clock          func() (string, int64, error)
	Busy           func() bool
}

// Invocation composition stays in the command's control context. The raw
// transport and every business owner receive Deadline/Stop, never this context.
type configKey struct{}

func WithConfig(ctx context.Context, config Config) context.Context {
	return context.WithValue(ctx, configKey{}, config)
}
func FromContext(ctx context.Context) (Config, bool) {
	config, ok := ctx.Value(configKey{}).(Config)
	return config, ok
}
func Inherit(nc *nats.Conn, role string) nats.Option {
	config := Config{}
	if client := For(nc); client != nil {
		config = client.config
		config.Role = role
	}
	return Configure(config)
}

// configuration is a construction-only option. Connect owners extract it
// BEFORE proxy options overwrite CustomDialer. It can never open a socket.
type configuration struct{ config Config }

func (*configuration) Dial(string, string) (net.Conn, error) { return nil, c.ErrUnsupported }
func Configure(config Config) nats.Option {
	config.Key = append(ed25519.PublicKey(nil), config.Key...)
	return nats.SetCustomDialer(&configuration{config})
}

// Configuration is read before proxy assembly, since an SDK Option has only
// one CustomDialer slot. The marker is never used as an actual network dialer.
// The real proxy and the immutable descriptor have separate responsibilities:
// copying the descriptor must not cause a data NC to share the control raw set.
func Configuration(dialer nats.CustomDialer) (Config, bool) {
	v, ok := dialer.(*configuration)
	if !ok {
		return Config{}, false
	}
	return v.config, true
}

// Client has three distinct inventories: admission identifies this NC in the
// complete cohort; transport owns interruptible raw workers; hold contains the
// receiver's immutable clock challenge and signed old/new route authorization.
// None owns a flow socket, PTY master, child process or transfer source FD.
// Closing a socket therefore cannot implicitly settle a cohort transaction.
// Keeping these references memory-only is part of the image exclusion contract:
// the host must provide Busy from the same upgrade domain even after ncBox is
// replaced, until retained migration obligations are actually discharged.
type Client struct {
	mu                   sync.Mutex
	active               sync.WaitGroup
	op                   chan struct{}
	config               Config
	id                   c.ID
	admission            ingressmove.Admission
	transport            *infratransport.Transport
	nc                   *nats.Conn
	raw                  *admittedRaw
	registered           bool
	registeredGeneration uint64
	generation           uint64
	authGeneration       uint64
	authServer           string
	authDigests          [2]c.Digest
	dialDeadline         c.Deadline
	initialDeadline      c.Deadline
	initialDone          bool
	inner                nats.CustomDialer
	closed               bool
	moveCanceled         bool
	pending              bool
	bound                bool
	target               c.RouteTarget
	hold                 *inframaint.Hold
	challenge            inframaint.Challenge
	ready                chan struct{}
	err                  error
}

var _ ingressmove.Connection = (*Client)(nil)

// Prepare installs the same raw owner on initial, ordinary and planned dials.
// A partial enabled descriptor is an error before any effect, not legacy mode.
// Unconfigured N-1 clients retain their cold connection without advertising S9.
// origin: hot-update S9 plan §4.5 / T50 / T51; rv-business F2 / rv-infra F10
func Prepare(options []nats.Option, deadline c.Deadline, config Config) (*Client, []nats.Option, error) {
	config.Key = append(ed25519.PublicKey(nil), config.Key...)
	probe, err := infratransport.NATSOptions(func() []nats.Option { return options })
	if err != nil {
		return nil, nil, err
	}
	if config.Registry != nil && (!complete(config) || runtime.GOOS != "linux" || runtime.GOARCH != "amd64") {
		return nil, nil, c.ErrUnsupported
	}
	if config.Registry == nil && (config.Authentication != nil || config.Recovery != nil || config.Observe != nil || config.Rebind != nil) {
		return nil, nil, c.ErrUnsupported
	}
	client := &Client{config: config, initialDeadline: deadline, op: make(chan struct{}, 1), ready: make(chan struct{})}
	client.op <- struct{}{}
	if _, err = rand.Read(client.id[:]); err != nil {
		return nil, nil, err
	}
	if config.Registry != nil {
		client.admission, err = config.Registry.Admit(client.id, config.Role, deadline)
		if err != nil {
			return nil, nil, err
		}
	}
	dialer := &gatedDialer{client: client, inner: probe.CustomDialer}
	if dialer.inner != nil {
		if _, ok := dialer.inner.(infratransport.PlannedDialer); !ok {
			return nil, nil, c.ErrUnsupported
		}
	}
	client.inner = dialer.inner
	client.transport, err = infratransport.New(1, deadline, nil, dialer)
	if err != nil {
		return nil, nil, err
	}
	all := append([]nats.Option(nil), options...)
	all = append(all, func(o *nats.Options) error {
		if o.InProcessServer != nil || o.RetryOnFailedConnect {
			return c.ErrUnsupported
		}
		previousReconnect, previousClose := o.ReconnectedCB, o.ClosedCB
		o.CustomDialer, o.SkipHostLookup = client, true
		if client.admission.Target != (c.RouteTarget{}) {
			o.Servers = []string{client.admission.Target.URL}
		}
		o.ReconnectedCB = func(nc *nats.Conn) {
			d, e := controlbudget.After(upgradebudget.RequestLatency)
			if e == nil {
				e = client.finish(nc, d, true)
			}
			if e != nil {
				_ = client.Close(d)
				return
			}
			if previousReconnect != nil {
				previousReconnect(nc)
			}
		}
		o.ClosedCB = func(nc *nats.Conn) {
			client.mu.Lock()
			// A caller-owned Close is already joining this NC; its asynchronous
			// notification must not race a second Registry.Remove of that owner.
			// origin: hot-update S9 plan §4.5; s9-instr4 Request 1
			owned := client.nc == nc && !client.closed
			client.mu.Unlock()
			if owned {
				d, _ := controlbudget.After(upgradebudget.RequestLatency)
				_ = client.Close(d)
			}
			if previousClose != nil {
				previousClose(nc)
			}
		}
		return nil
	})
	return client, all, nil
}

// Presence is an installation precondition, not a readiness receipt. Each
// actual dial obtains fresh auth evidence and executes Recovery, while each
// registration and move re-reads the actual connection and physical witness.
// Factories cannot advertise capability from a compile-time port alone.
func complete(config Config) bool {
	return config.Registry != nil && (config.Role == "control" || config.Role == "data" || config.Role == "consumer") &&
		config.Authentication != nil && config.Recovery != nil && config.Observe != nil && config.Rebind != nil &&
		config.Domain != (c.ID{}) && config.KeyID != "" && len(config.Key) == ed25519.PublicKeySize && config.View != nil && config.Clock != nil && config.Busy != nil
}

// For reads only immutable construction options, never nc.mu or a global
// identity map. Multiple invocations using one nkey therefore remain separate.
func For(nc *nats.Conn) *Client {
	if nc == nil {
		return nil
	}
	client, _ := nc.Opts.CustomDialer.(*Client)
	return client
}

// Finish is the construction barrier after private-inbox negotiation. A
// ConnectedCB is asynchronous and can run before the N-1 fallback decision;
// publishing capability there would sign an NC the caller is about to discard.
// The construction owner calls this synchronously for the final selected NC.
func Finish(nc *nats.Conn, deadline c.Deadline) error {
	if client := For(nc); client != nil {
		return client.finish(nc, deadline, false)
	}
	return nil
}

// The SDK receives the underlying socket through a small admission wrapper.
// P14 retains raw retirement custody; this wrapper also covers the synchronous
// natsinbox negotiation, whose FinishConnect runs after a possible N-1 redial.
type admittedRaw struct {
	net.Conn
	owned      *infratransport.Connection
	mu         sync.Mutex
	limit      time.Time
	generation uint64
}

func (r *admittedRaw) SetDeadline(d time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.limit.IsZero() && (d.IsZero() || d.After(r.limit)) {
		d = r.limit
	}
	return r.Conn.SetDeadline(d)
}
func (r *admittedRaw) SetReadDeadline(d time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.limit.IsZero() && (d.IsZero() || d.After(r.limit)) {
		d = r.limit
	}
	return r.Conn.SetReadDeadline(d)
}
func (r *admittedRaw) SetWriteDeadline(d time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.limit.IsZero() && (d.IsZero() || d.After(r.limit)) {
		d = r.limit
	}
	return r.Conn.SetWriteDeadline(d)
}
func (r *admittedRaw) Close() error { return r.owned.Close() }
func (r *admittedRaw) admit() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.limit = time.Time{}
	return r.Conn.SetDeadline(time.Time{})
}

func (client *Client) Dial(network, address string) (net.Conn, error) {
	client.mu.Lock()
	old, d, closed := client.transport, client.initialDeadline, client.closed || client.moveCanceled
	client.mu.Unlock()
	if closed {
		return nil, context.Canceled
	}
	if d == 0 {
		var err error
		d, err = controlbudget.After(upgradebudget.RequestLatency)
		if err != nil {
			return nil, err
		}
	}
	// SDK dials are serialized per NC. Retire the previous raw owner before
	// publishing a fresh, bounded attempt; initial fallback keeps the original
	// budget until FinishConnect, ordinary reconnect does not inherit it.
	// origin: hot-update S9 plan §4.5 / T51
	old.Interrupt()
	transport, err := infratransport.New(1, d, nil, &gatedDialer{client: client, inner: client.inner})
	if err != nil {
		return nil, err
	}
	client.mu.Lock()
	if client.closed || client.moveCanceled {
		client.mu.Unlock()
		transport.Interrupt()
		return nil, context.Canceled
	}
	client.transport = transport
	client.mu.Unlock()
	raw, err := transport.Dial(network, address)
	if err != nil {
		return nil, err
	}
	owned := raw.(*infratransport.Connection) //nolint:forcetypeassert // Transport.Dial constructs only its concrete Connection on success.
	client.mu.Lock()
	d = client.dialDeadline
	client.mu.Unlock()
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		_ = owned.Close()
		return nil, err
	}
	r := &admittedRaw{Conn: owned.RawConn(), owned: owned, limit: time.Now().Add(remaining)}
	client.mu.Lock()
	if client.closed || client.moveCanceled {
		client.mu.Unlock()
		_ = r.Close()
		return nil, context.Canceled
	}
	client.generation++
	r.generation = client.generation
	client.raw = r
	client.mu.Unlock()
	return r, nil
}

// This hook is called synchronously by natsinbox, after its private inbox gate.
// It never claims physical attachment or normal REGISTER from a bare PONG.
func (client *Client) FinishConnect(err error) error {
	client.mu.Lock()
	initial := !client.initialDone
	client.initialDone = true
	client.mu.Unlock()
	if initial {
		if finisher, ok := client.inner.(interface{ FinishConnect(error) error }); ok {
			err = finisher.FinishConnect(err)
		}
	}
	if err != nil {
		client.mu.Lock()
		transport := client.transport
		client.mu.Unlock()
		transport.Interrupt()
		return err
	}
	client.mu.Lock()
	raw := client.raw
	client.initialDeadline = 0
	client.mu.Unlock()
	if raw == nil {
		return c.ErrIncomplete
	}
	return raw.admit()
}

type gatedDialer struct {
	client *Client
	inner  nats.CustomDialer
}

func (*gatedDialer) Dial(string, string) (net.Conn, error) { return nil, c.ErrUnsupported }
func (g *gatedDialer) DialWithin(network, address string, d c.Deadline, stop c.Stop) (net.Conn, error) {
	client := g.client
	client.mu.Lock()
	closed, target, late := client.closed || client.moveCanceled, client.target, client.admission.Target
	registered, pending, generation := client.registered, client.pending, client.registeredGeneration
	client.dialDeadline = d
	client.mu.Unlock()
	if closed {
		return nil, context.Canceled
	}
	if target == (c.RouteTarget{}) {
		target = late
	}
	// Prepare authorizes intent; it cannot admit an automatic old-pool
	// reconnect before Attach records the one permitted successor dial.
	// origin: hot-update S9 plan §4.6 / M25
	if pending && target == (c.RouteTarget{}) {
		return nil, c.ErrIncomplete
	}
	if target != (c.RouteTarget{}) {
		u, e := url.Parse(target.URL)
		if e != nil || u.Host != address {
			return nil, c.ErrIncomplete
		}
	}
	if registered && !pending {
		if e := client.config.Registry.Remove(client.id, generation, d); e != nil {
			return nil, e
		}
		a, e := client.config.Registry.Admit(client.id, client.config.Role, d)
		if e != nil {
			return nil, e
		}
		client.mu.Lock()
		client.admission, client.registered = a, false
		client.mu.Unlock()
	}
	if client.config.Registry != nil {
		// SDK automatic SUB replay happens after this returns. Neither the
		// reconnect callback nor an old receipt can grant that earlier admission.
		// origin: hot-update S9 plan §4.4 / M25; rv-business F2 / rv-infra F10
		r, e := client.config.Authentication(address, d, stop)
		if e != nil {
			return nil, e
		}
		client.mu.Lock()
		previousAuth, previousServer, previousDigests := client.authGeneration, client.authServer, client.authDigests
		client.mu.Unlock()
		if !authReady(r) || (r.ServerID == previousServer && (r.Generation < previousAuth || r.Probes[0].RequestDigest == previousDigests[0] || r.Probes[1].RequestDigest == previousDigests[1])) {
			return nil, c.ErrIncomplete
		}
		if e = client.config.Recovery(r, d, stop); e != nil {
			return nil, e
		}
		client.mu.Lock()
		client.authGeneration, client.authServer, client.authDigests = r.Generation, r.ServerID, [2]c.Digest{r.Probes[0].RequestDigest, r.Probes[1].RequestDigest}
		client.mu.Unlock()
	}
	client.mu.Lock()
	initial := !client.initialDone
	client.mu.Unlock()
	if initial && g.inner != nil {
		// Only an owned hook bounds this path: a standalone proxydial dialer has
		// the same FinishConnect as a pass-through and would hold the dial to its
		// own timeout past d and stop (S9 P22 resume, inner-dialer hook sweep).
		owned, _ := g.inner.(interface{ AdmissionOwned() bool })
		if finisher, ok := g.inner.(interface{ FinishConnect(error) error }); ok && owned != nil && owned.AdmissionOwned() {
			// Preserve the existing S6 DNS/TCP/proxy attempt observer on initial
			// admission. Its composite budget is itself cancellable and joined;
			// ordinary/planned dials use P14's independent deadline afterwards.
			// origin: hot-update S9 plan §4.5 / T51; S6 T30 receipt continuity
			done, watched := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(watched)
				select {
				case <-stop:
					_ = finisher.FinishConnect(context.Canceled)
				case <-done:
				}
			}()
			defer func() { close(done); <-watched }()
			return g.inner.Dial(network, address)
		}
	}
	if g.inner != nil {
		dialer := g.inner.(infratransport.PlannedDialer) //nolint:forcetypeassert // Prepare validates every non-nil inner dialer before publishing this immutable field.
		return dialer.DialWithin(network, address, d, stop)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, err
	}
	// ctx-none: CustomDialer has no ctx; P14 supplies the raw generation Stop and deadline.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); <-done }()
	return (&net.Dialer{Resolver: &net.Resolver{PreferGo: true}}).DialContext(ctx, network, address)
}

// Validate the closed P17 receipt rather than treating successful TCP or a
// logged ready event as authentication. Both probes must have ordinary signed
// responses from the local queue and complete private-inbox RPC timestamps.
// These times share the responder's BOOTTIME only; no cross-host subtraction
// or wall-clock comparison can establish the subscription replay barrier.
func authReady(r infraauth.Receipt) bool {
	normal := false
	for _, s := range r.AuthSUBs {
		if s.Subject == "$SYS.REQ.USER.AUTH" && s.Queue == "tether-authcallout" {
			normal = true
		}
	}
	if !normal || r.Phase != infraauth.ControlReady || r.Generation == 0 || r.ServerID == "" || r.AuthCID == 0 || r.BusinessCID == 0 || r.AuthPONG <= 0 || r.AuthUsable <= r.AuthPONG || r.BusinessPONG <= r.AuthUsable || r.ControlReady <= r.BusinessPONG {
		return false
	}
	for _, p := range r.Probes {
		if p.Generation != r.Generation || p.ServerID != r.ServerID || p.CID == 0 || p.RequestDigest == (c.Digest{}) || p.ReplyBootNS <= r.AuthPONG || p.PONGBootNS <= p.ReplyBootNS || p.PONGBootNS >= r.AuthUsable || p.RPCBootNS <= r.BusinessPONG || p.RPCBootNS >= r.ControlReady {
			return false
		}
	}
	return true
}

func (client *Client) finish(nc *nats.Conn, d c.Deadline, _ bool) error {
	// Registration borrows the host's normal authority ports. Closing the raw
	// carrier must also join these borrowers before host teardown can proceed.
	// origin: hot-update S9 plan §4.5 / TestClientCloseJoinsNormalRegistration
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return context.Canceled
	}
	client.active.Add(1)
	client.mu.Unlock()
	defer client.active.Done()
	if err := client.FinishConnect(nil); err != nil {
		return err
	}
	client.mu.Lock()
	client.nc = nc
	registered := client.registered
	client.mu.Unlock()
	var err error
	if client.config.Registry != nil {
		err = client.config.Rebind(nc, d)
		if err == nil && !registered {
			err = client.config.Registry.Register(client.admission, client, d)
		}

	}
	client.mu.Lock()
	client.err = err
	if err == nil {
		client.registered = client.config.Registry != nil
		if client.registered {
			client.registeredGeneration = client.raw.generation
		}
	}
	select {
	case <-client.ready:
	default:
		close(client.ready)
	}
	registerClientObservation(client, err)
	client.mu.Unlock()
	return err
}

// Challenge must precede the signed Prepare request. Receiving a proof does
// not create a new local t0 and grant another eight seconds.
func (client *Client) Challenge(tx c.ID, direction c.RouteDirection) (inframaint.Challenge, error) {
	if !complete(client.config) {
		return inframaint.Challenge{}, c.ErrUnsupported
	}
	boot, now, err := client.config.Clock()
	if err != nil {
		return inframaint.Challenge{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.hold != nil && client.challenge.Tx == tx && client.challenge.Direction == direction {
		return client.challenge, nil
	}
	if client.pending && (!client.bound || direction != c.RouteReturn || client.challenge.Tx != tx) {
		return inframaint.Challenge{}, errors.New("infra_busy")
	}
	var nonce c.ID
	if _, err = rand.Read(nonce[:]); err != nil {
		return inframaint.Challenge{}, err
	}
	challenge := inframaint.Challenge{Domain: client.config.Domain, Tx: tx, Participant: client.id, Direction: direction, Nonce: nonce, BootID: boot, StartedNS: now}
	client.hold, err = inframaint.NewHold(challenge)
	if err == nil {
		client.challenge, client.pending, client.bound, client.target = challenge, true, false, c.RouteTarget{}
	}
	return challenge, err
}

func (client *Client) Install(p c.RouteProof, d c.Deadline) (c.Attachment, error) {
	if p.Purpose != c.RoutePrepare {
		return c.Attachment{}, c.ErrIncomplete
	}
	return client.install(p, d)
}
func (client *Client) Bind(p c.RouteProof, d c.Deadline) (c.Attachment, error) {
	if p.Purpose != c.RouteBind {
		return c.Attachment{}, c.ErrIncomplete
	}
	return client.install(p, d)
}
func (client *Client) install(p c.RouteProof, d c.Deadline) (c.Attachment, error) {
	a, err := client.Observe(d)
	if err != nil {
		return c.Attachment{}, err
	}
	client.mu.Lock()
	hold, challenge := client.hold, client.challenge
	client.mu.Unlock()
	if hold == nil {
		return c.Attachment{}, c.ErrIncomplete
	}
	boot, now, err := client.config.Clock()
	if err != nil {
		return c.Attachment{}, err
	}
	v := client.config.View()
	if v.Attachment != a {
		return c.Attachment{}, c.ErrIncomplete
	}
	if _, err = hold.Install(p, client.config.KeyID, client.config.Key, v, boot, challenge.StartedNS, now); err != nil {
		return c.Attachment{}, err
	}
	client.mu.Lock()
	client.pending = true
	client.bound = p.Purpose == c.RouteBind
	client.mu.Unlock()
	return a, nil
}

// Attach consumes installed intent, retires the original raw generation and
// requests a reconnect on the SAME SDK object. That preserves a ctl invocation
// and its subscriptions while changing the transport attachment underneath.
// Prepare is not a move receipt; only Observe after normal Rebind supplies the
// actual new CID/socket. Bind later authenticates that result independently.
// A restricted target is installed before nc.mu-taking SDK calls, so concurrent
// reconnect attempts cannot escape onto the old pool. The request watchdog
// interrupts the currently published raw owner and is joined before returning;
// its cancellation never becomes a flow, SS, PTY or transfer lifetime root.
func (client *Client) Attach(target c.RouteTarget, d c.Deadline, stop c.Stop) (c.Attachment, error) {
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return c.Attachment{}, err
	}
	limit, err := controlbudget.After(min(remaining, upgradebudget.RequestLatency))
	if err != nil {
		return c.Attachment{}, err
	}
	d = min(d, limit)
	remaining, _ = controlbudget.Remaining(d)
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-client.op:
	case <-stop:
		return c.Attachment{}, context.Canceled
	case <-timer.C:
		return c.Attachment{}, controlbudget.ErrExpired
	}
	defer func() { client.op <- struct{}{} }()
	client.mu.Lock()
	hold, nc, raw, transport := client.hold, client.nc, client.raw, client.transport
	client.mu.Unlock()
	if hold == nil || nc == nil || raw == nil {
		return c.Attachment{}, c.ErrUnsupported
	}
	boot, now, err := client.config.Clock()
	if err != nil {
		return c.Attachment{}, err
	}
	if err = hold.BeginDial(inframaint.Pending{Participant: client.id, Generation: raw.generation + 1, Target: target}, client.config.View(), boot, now); err != nil {
		return c.Attachment{}, err
	}
	client.mu.Lock()
	client.target, client.ready, client.err, client.initialDeadline = target, make(chan struct{}), nil, d
	ready := client.ready
	client.mu.Unlock()
	// Break raw DNS/TCP/WS I/O before SetServerPool/ForceReconnect take nc.mu.
	// The gate's restricted target prevents an old-pool retry from escaping.
	// origin: hot-update S9 plan §4.5 / M25
	done, watched, aborted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watched)
		var cause error
		select {
		case <-stop:
			cause = context.Canceled
		case <-timer.C:
			cause = controlbudget.ErrExpired
		case <-done:
			return
		}
		client.mu.Lock()
		client.moveCanceled, client.err = true, cause
		current := client.transport
		client.mu.Unlock()
		current.Interrupt()
		close(aborted)
	}()
	defer func() { close(done); <-watched }()
	transport.Interrupt()
	if err = nc.SetServerPool([]string{target.URL}); err == nil {
		err = nc.ForceReconnect()
	}
	if err != nil {
		return c.Attachment{}, err
	}
	select {
	case <-ready:
	case <-aborted:
		_ = client.Close(d)
	}
	client.mu.Lock()
	err = client.err
	client.mu.Unlock()
	if err != nil {
		return c.Attachment{}, err
	}
	return client.Observe(d)
}

// Protects is consulted inline by the normal roster/reaper decision. It
// cannot make a route healthy or reset dwell, and it does not cover another
// participant/home. A stale, revoked or expired challenge permanently stops
// granting protection; retries cannot resurrect the original local deadline.
func (client *Client) Protects() bool {
	if client == nil || !complete(client.config) {
		return false
	}
	client.mu.Lock()
	hold := client.hold
	client.mu.Unlock()
	if hold == nil {
		return false
	}
	boot, now, err := client.config.Clock()
	return err == nil && hold.Protects(client.config.View(), boot, now)
}

// Pending retains responsibility beyond the protection deadline. Expiring
// a route hold permits normal failure handling, but cannot authorize encoding
// live S9 objects in an image or erase an unfinished cohort from its domain.
func (client *Client) Pending() bool {
	if client == nil {
		return false
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.pending
}

// Installed describes this NC's completed local composition. Hosts publish
// public capability only after installing their normal subscription/REGISTER
// surface and every independent data root; farewell is never that installation
// path. An unconfigured cold client cannot acquire capability from a raw dial.
func (client *Client) Installed() bool {
	if client == nil {
		return false
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.registered && !client.closed && client.err == nil
}

// Release is called after the cohort's own release barrier, never on expiry.
// Expiry stops protection but cannot erase retained transaction responsibilities.
func (client *Client) Release() {
	client.mu.Lock()
	hold := client.hold
	client.pending, client.bound = false, false
	client.target = c.RouteTarget{}
	client.mu.Unlock()
	if hold != nil {
		hold.Release()
	}
}

// Close fences publication before interrupting any raw worker. WaitGroup
// admission uses that same mutex, so no late REGISTER borrower can race Wait.
// Removal uses the last acknowledged registry generation, not whichever raw
// socket happens to be newest. A sealed registry retains its obligation and
// returns an error; closure cannot stand in for a migration/return ACK.
func (client *Client) Close(d c.Deadline) error {
	client.mu.Lock()
	client.closed = true
	unregisterClientObservation(client)
	nc, registered, generation, transport := client.nc, client.registered, client.registeredGeneration, client.transport
	client.mu.Unlock()
	// An expired admission cannot abandon raw workers. Interrupt and SDK
	// close still join, while the expired removal receipt remains an error.
	// origin: hot-update S9 plan §4.5 / TestInfraAttachmentLifecycle
	transport.Interrupt()
	if nc != nil {
		nc.Close()
	}
	client.active.Wait()
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	if registered {
		if err := client.config.Registry.Remove(client.id, generation, d); err != nil {
			return err
		}
		client.mu.Lock()
		client.registered = false
		client.mu.Unlock()
	}
	return nil
}
