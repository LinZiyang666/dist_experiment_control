package infraauth

import (
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const authSubject = "$SYS.REQ.USER.AUTH"
const authQueue = "tether-authcallout"

// Config is the single cold/hot/ordinary descriptor consumed by P22. Authority
// contains only the minimal responders needed by the normal auth handler;
// Business is installed after ordinary CONNECT proofs and the recovery gate.
// Signers and handlers remain caller-owned until Close has actually joined.
type Config struct {
	URL              string
	AccountPublicKey string
	// SDK configuration is snapshotted once; Deadline and Stop own the lifetime.
	// origin: S7 internal review INTEG-M F4
	Options    func() []nats.Option
	Transport  TransportFactory
	HandleAuth func(string) (string, error)
	Authority  []Binding
	Business   []Binding
	Member     Identity
	Agent      Identity
	Recovery   func(uint64, infracontract.Deadline, infracontract.Stop) error
	// Replied observes an answer the normal queue actually delivered (S6's
	// reply witnesses). It is optional, and the probe's in-process decision of
	// a request a peer answered never calls it: deciding is not replying.
	// origin: hot-update S9 P22 resume — design finding (callout queue selection)
	Replied func(request, response string)
}

type Owner struct {
	// op serializes complete generations, while mu protects only publication.
	// Network I/O never holds mu: disconnect and Close must reach the raw
	// cancellation owner even when CONNECT or Flush is waiting on nc.mu.
	mu      sync.Mutex
	op      sync.Mutex
	config  Config
	options []nats.Option
	current *generation
	next    uint64
}

// New freezes the binding slices. A caller cannot mutate the SUB set between
// proof and replay, or smuggle the auth endpoint onto the business connection.
// origin: hot-update S9 plan §4.4 / T38
func New(config Config) (*Owner, error) {
	u, err := url.Parse(config.URL)
	if err != nil || u == nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Contains(config.URL, ",") {
		return nil, ErrBinding
	}
	switch u.Scheme {
	case "nats", "tls", "ws", "wss":
	default:
		return nil, ErrBinding
	}
	if !nkeys.IsValidPublicAccountKey(config.AccountPublicKey) || config.Transport == nil || config.HandleAuth == nil || len(config.Business) == 0 ||
		!validIdentity(config.Member, false) || !validIdentity(config.Agent, true) || config.Member.PublicKey == config.Agent.PublicKey {
		return nil, ErrBinding
	}
	seen := make(map[string]bool)
	for _, bindings := range [][]Binding{config.Authority, config.Business} {
		for _, binding := range bindings {
			if binding.Subject == "" || strings.HasPrefix(binding.Subject, "$SYS.") || binding.Handle == nil || seen[binding.Subject] {
				return nil, ErrBinding
			}
			seen[binding.Subject] = true
		}
	}
	var options []nats.Option
	if config.Options != nil {
		options = append(options, config.Options()...)
	}
	config.Options = nil
	config.Authority = append([]Binding(nil), config.Authority...)
	config.Business = append([]Binding(nil), config.Business...)
	config.Member = cloneIdentity(config.Member)
	config.Agent = cloneIdentity(config.Agent)
	return &Owner{config: config, options: options}, nil
}

// Open retires the previous generation before installing a new auth-only NC.
// The caller's ordinary reconnect loop invokes exactly this same entry point as
// cold startup and hot restore. A failed Open never leaves business admission.
// origin: hot-update S9 plan §4.4 / T38
func (o *Owner) Open(deadline infracontract.Deadline, stop infracontract.Stop) (Receipt, error) {
	o.op.Lock()
	defer o.op.Unlock()
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return Receipt{}, err
	}
	select {
	case <-stop:
		return Receipt{}, ErrFenced
	default:
	}
	if err := o.closeCurrent(deadline); err != nil {
		return Receipt{}, err
	}
	// Do not borrow the old queue's proof. Even the same local server has a new
	// CID after reconnect; every generation must send its own SUB and receive
	// its own PONG and ordinary auth responses before business can be rebuilt.
	// origin: hot-update S9 plan §4.4 / T38
	o.mu.Lock()
	if o.next == ^uint64(0) {
		o.mu.Unlock()
		return Receipt{}, ErrBinding
	}
	o.next++
	g := &generation{receipt: Receipt{Generation: o.next, Phase: AuthBinding}, lost: make(chan struct{}), built: make(chan struct{}), done: make(chan struct{})}
	for index := range g.responseReady {
		g.responseReady[index] = make(chan struct{})
	}
	o.current = g
	o.mu.Unlock()
	go retire(g)
	// This watcher lasts for the generation, not merely the Open call. A
	// successful admission deadline is not a transport lifetime deadline, and
	// stopping the caller must still fence already-ready business admission.
	go func() {
		select {
		case <-stop:
			fence(g)
		case <-g.done:
		}
	}()
	err := o.bind(g, deadline)
	// Retire waits for construction to finish so it sees every late-created
	// raw owner. connect checks the sticky fence after registering each owner,
	// preventing a dial completing after Close from escaping the census.
	close(g.built)
	if err != nil {
		fence(g)
		err = errors.Join(err, waitDone(g, deadline))
	}
	return snapshot(g), err
}

func (o *Owner) bind(g *generation, deadline infracontract.Deadline) error {
	nc, err := o.connect(g, o.options, "tetherd-auth", true, deadline)
	if err != nil {
		return err
	}
	g.auth = nc
	// Server and CID come from this actual INFO exchange. A configured URL or
	// a statically admitted broker CONNECT says nothing about ordinary auth.
	serverID, cid, err := connectionIdentity(nc)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.receipt.ServerID, g.receipt.AuthCID = serverID, cid
	g.mu.Unlock()
	if err := install(g, nc, o.config.Authority, true); err != nil {
		return err
	}
	// Leader PIN verification and other authority responders can themselves be
	// prerequisites of auth. Their SUBs share this first PONG, rather than
	// waiting for the business table that ordinary authentication must unlock.
	// Only this normal queue answers callouts. Probe code never subscribes an
	// auxiliary responder; its non-responding witness (probe.go) cannot sign a
	// success unless this very subscription is still the acknowledged member.
	// origin: hot-update S9 plan §4.4 / T39
	g.normalQueue, err = nc.QueueSubscribe(authSubject, authQueue, o.normalHandler(g))
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.receipt.AuthSUBs = append(g.receipt.AuthSUBs, Subscription{Subject: authSubject, Queue: authQueue})
	g.mu.Unlock()
	if err := flush(g, nc, deadline); err != nil {
		return err
	}
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.receipt.AuthPONG = now
	g.mu.Unlock()
	identities := [2]Identity{o.config.Member, o.config.Agent}
	var probes [2]*nats.Conn
	// Both identities are required. A member grant can succeed while agent
	// provisioning is missing, and a static broker credential bypasses callout
	// entirely. Retain these very connections for the later real RPCs.
	for index, identity := range identities {
		probes[index], err = o.authenticate(g, identity, index, deadline)
		if err != nil {
			return err
		}
	}
	if err := live(g, deadline); err != nil {
		return err
	}
	now, err = controlbudget.Now()
	if err != nil {
		return err
	}
	g.mu.Lock()
	if g.fenced {
		g.mu.Unlock()
		return ErrFenced
	}
	g.receipt.AuthUsable, g.receipt.Phase = now, AuthUsable
	g.mu.Unlock()
	if o.config.Recovery != nil {
		// Auth authority is available for P18's read-only recovery validation.
		// No business SUB, dial or repair producer exists while it is blocked;
		// its generation is the one whose original ACK prefix it must release.
		if err := o.config.Recovery(g.receipt.Generation, deadline, g.lost); err != nil {
			return err
		}
	}
	if err := live(g, deadline); err != nil {
		return err
	}
	g.mu.Lock()
	if g.fenced {
		g.mu.Unlock()
		return ErrFenced
	}
	g.receipt.Phase = BusinessBinding
	g.mu.Unlock()
	business, err := o.connect(g, o.options, "tetherd-business", true, deadline)
	if err != nil {
		return err
	}
	g.business = business
	// Reusing the auth server's identity constrains both broker connections to
	// the same local service. A healthy peer cannot become the business owner
	// merely because it accepts the static broker credential.
	actualServer, businessCID, err := connectionIdentity(business)
	if err != nil || actualServer != serverID {
		return ErrNormalQueue
	}
	if err := install(g, business, o.config.Business, false); err != nil {
		return err
	}
	if err := flush(g, business, deadline); err != nil {
		return err
	}
	now, err = controlbudget.Now()
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.receipt.BusinessCID, g.receipt.BusinessPONG = businessCID, now
	g.mu.Unlock()
	for index, identity := range identities {
		if err := probeRPC(g, probes[index], identity, index, deadline); err != nil {
			return err
		}
	}
	// RPC completion is a separate barrier from subscription PONG. It proves
	// the ordinary grants can use private replies and the rebound application
	// handlers can answer; neither auth_usable nor a broker PONG implies that.
	if err := live(g, deadline); err != nil {
		return err
	}
	now, err = controlbudget.Now()
	if err != nil {
		return err
	}
	g.mu.Lock()
	if g.fenced {
		g.mu.Unlock()
		return ErrFenced
	}
	g.receipt.ControlReady, g.receipt.Phase = now, ControlReady
	g.mu.Unlock()
	for _, probe := range probes {
		// Ordinary probe clients are temporary clients, never responders. Their
		// success receipts remain, but their sockets must not accumulate across
		// ordinary reconnects or outlive the generation's cleanup ownership.
		probe.Close()
	}
	return nil
}

func (o *Owner) connect(g *generation, options []nats.Option, name string, guarded bool, deadline infracontract.Deadline) (*nats.Conn, error) {
	if err := live(g, deadline); err != nil {
		return nil, err
	}
	transport, err := o.config.Transport(deadline, g.lost)
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, ErrBinding
	}
	g.mu.Lock()
	g.transports = append(g.transports, transport)
	fenced := g.fenced
	g.mu.Unlock()
	if fenced {
		transport.Cancel()
		return nil, ErrFenced
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	// Options supplied by existing credential/TLS builders cannot reopen the
	// discovered peer pool or enable automatic business SUB replay. Clear an
	// explicitly supplied Servers list too: IgnoreDiscoveredServers only covers
	// INFO additions, not caller options. Raw cancellation stays with P14.
	// origin: hot-update S9 plan §4.4 / T38 / T39
	opts := append([]nats.Option(nil), options...)
	opts = append(opts, func(opts *nats.Options) error {
		opts.Servers = []string{o.config.URL}
		opts.Url = o.config.URL
		opts.InProcessServer = nil
		opts.Dialer = nil
		return nil
	}, nats.Name(name), nats.SetCustomDialer(transport), nats.Timeout(remaining), nats.NoReconnect(), nats.RetryOnFailedConnect(false), nats.IgnoreDiscoveredServers(), nats.DontRandomize(), nats.ReconnectBufSize(0),
		// origin: hot-update S9 plan §10.2 / T83 — caller defaults cannot lengthen the server's liveness contract.
		nats.PingInterval(20*time.Second), nats.MaxPingsOutstanding(2),
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
			if guarded {
				fence(g)
			}
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			if guarded {
				fence(g)
			}
		}), nats.ReconnectHandler(nil))
	nc, err := nats.Connect(o.config.URL, opts...)
	if nc != nil {
		g.connections = append(g.connections, nc)
	}
	if err != nil {
		// Network errors may contain private URLs or caller credential errors.
		return nil, ErrFenced
	}
	if err := live(g, deadline); err != nil {
		return nil, err
	}
	// Observation borrows this actual NC; a second dial cannot attest its CID.
	// origin: hot-update S9 plan §2.3 / §4.4; s9-instr4 Request 1
	if observer, ok := transport.(interface{ Connected(*nats.Conn) }); ok && guarded {
		observer.Connected(nc)
	}
	return nc, nil
}

func install(g *generation, nc *nats.Conn, bindings []Binding, authority bool) error {
	// Every installed application handler uses the same fence and borrower
	// accounting as auth. Queued messages cannot enter old-generation code
	// after disconnect, and admitted callbacks must join before Close succeeds.
	for _, binding := range bindings {
		var err error
		if binding.Queue == "" {
			_, err = nc.Subscribe(binding.Subject, guardedHandler(g, binding.Handle))
		} else {
			_, err = nc.QueueSubscribe(binding.Subject, binding.Queue, guardedHandler(g, binding.Handle))
		}
		if err != nil {
			return ErrBinding
		}
		g.mu.Lock()
		sub := Subscription{Subject: binding.Subject, Queue: binding.Queue}
		if authority {
			g.receipt.AuthSUBs = append(g.receipt.AuthSUBs, sub)
		} else {
			g.receipt.BusinessSUBs = append(g.receipt.BusinessSUBs, sub)
		}
		g.mu.Unlock()
	}
	return nil
}

func flush(g *generation, nc *nats.Conn, deadline infracontract.Deadline) error {
	// SUB writes are buffered by nats.go. Only the server's PONG closes this
	// installation barrier; a successful Subscribe call is not a server effect.
	if err := live(g, deadline); err != nil {
		return err
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	if nc.FlushTimeout(remaining) != nil || !nc.IsConnected() {
		return ErrFenced
	}
	return live(g, deadline)
}

func connectionIdentity(nc *nats.Conn) (string, uint64, error) {
	cid, err := nc.GetClientID()
	serverID := nc.ConnectedServerId()
	if err != nil || cid == 0 || serverID == "" || !nc.IsConnected() {
		return "", 0, ErrNormalQueue
	}
	return serverID, cid, nil
}

func (o *Owner) closeCurrent(deadline infracontract.Deadline) error {
	o.mu.Lock()
	g := o.current
	o.mu.Unlock()
	if g == nil {
		return nil
	}
	fence(g)
	return waitDone(g, deadline)
}

func (o *Owner) Close(deadline infracontract.Deadline) error {
	o.mu.Lock()
	g := o.current
	o.mu.Unlock()
	if g == nil {
		return nil
	}
	// No operation lock here: Close must interrupt an Open blocked in raw dial.
	fence(g)
	return waitDone(g, deadline)
}

// Business borrows the current ready NC. Its handlers remain fenced even if a
// caller retained this pointer across a disconnect. Callers must request a new
// borrow for the next generation instead of re-enabling reconnect on this NC.
func (o *Owner) Business() (*nats.Conn, Receipt, error) {
	o.mu.Lock()
	g := o.current
	o.mu.Unlock()
	if g == nil {
		return nil, Receipt{}, ErrFenced
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fenced || g.receipt.Phase != ControlReady || g.business == nil || !g.business.IsConnected() || !g.auth.IsConnected() {
		return nil, Receipt{}, ErrFenced
	}
	r := g.receipt
	r.AuthSUBs = append([]Subscription(nil), r.AuthSUBs...)
	r.BusinessSUBs = append([]Subscription(nil), r.BusinessSUBs...)
	return g.business, r, nil
}
