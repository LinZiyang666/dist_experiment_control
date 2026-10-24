package infraattach

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

const (
	// brokerAttemptBudget bounds ONE ordinary auth-first admission attempt
	// (auth-only CONNECT, normal-queue SUB/PONG, both ordinary identities and
	// the private-inbox RPCs). nats-server picks a callout's queue member
	// uniformly across local and ROUTED members (server/client.go selectQSub),
	// so in a cluster a peer broker usually answers a probe CONNECT; P17's
	// probe witness makes the LOCAL proof independent of that draw, so the
	// budget is sized for one real attempt, not for retrying until the local
	// member happens to be picked. A failed attempt is retried by the next
	// gated dial and never inherits the previous attempt's budget.
	// origin: hot-update S9 P22 resume — design finding (callout queue selection)
	brokerAttemptBudget = time.Second
	// brokerRecoveryAttempt replaces it while a recovery seal is held: the
	// attempt then also reads back every original ACK before business.
	brokerRecoveryAttempt = upgradebudget.RequestLatency
	// brokerAdmissionBudget bounds a cold or terminal wait for admission when
	// the caller has no budget of its own: many attempts, not one.
	brokerAdmissionBudget = 12 * upgradebudget.RequestLatency
	// brokerReconnectWait paces the business NC's gated redial. The gate
	// answers a not-yet-admitted dial at once, so this is a poll interval for
	// the admission worker's result, not a socket timeout.
	brokerReconnectWait = 250 * time.Millisecond
	// brokerACKWindow bounds the original-ACK ledger. Only the most recent
	// receipts are at risk across a same-boot process restart; older ones may
	// also be removed legitimately by stream limits, which a ledger of every
	// ACK since boot would misreport as loss.
	brokerACKWindow = 1024
)

var (
	// ErrBrokerAdmission is the gate's refusal while no proven auth generation
	// is available. nats.go retries the dial; no business SUB is replayed.
	ErrBrokerAdmission = errors.New("infraattach: broker business admission pending")
	// ErrInfraPending refuses an S6 image cut while the same upgrade domain
	// still owns an S9 responsibility. It is public as the stable cut refusal.
	ErrInfraPending = errors.New("infra_busy: an infrastructure upgrade owns this upgrade domain")
)

// BrokerAuth is the deployment's ordinary-identity descriptor. Member and
// Agent are an already activated member and an already provisioned agent; the
// normal handler decides their admission exactly as for any other client.
// Business holds the ordinary handlers that answer those identities' RPCs on
// the auth owner's own business NC. They must be exact probe-identity subjects
// and must not use the broker's long-lived business NC. Authority lists the
// minimal responders ordinary authentication itself depends on.
// origin: hot-update S9 plan §3.3 / §4.4 / P22
type BrokerAuth struct {
	AccountPublicKey string
	Member           infraauth.Identity
	Agent            infraauth.Identity
	Authority        []infraauth.Binding
	Business         []infraauth.Binding
}

// BrokerRecovery binds P18's gate for this upgrade domain. Assets is P11's
// complete inventory projection, Exclusive the deployment's writer policy and
// Transfers P19's original S7 seal.
type BrokerRecovery struct {
	Domain    c.ID
	Assets    func(c.Deadline, c.Stop) (c.AssetSeal, error)
	Exclusive func(c.Deadline, c.Stop) error
	Transfers func(c.Deadline) (c.TransferSeal, error)
}

// BrokerInfra is broker.Config.Infra: memory-only composition ports, never an
// image, wire or persistent record. Nil keeps the legacy N-1 connection and
// never advertises infra capability. Pending reports an S9 transaction this
// domain still owns; the Service composition binds Broker.Recovery itself.
type BrokerInfra struct {
	Auth       *BrokerAuth
	Recovery   *BrokerRecovery
	Pending    func() bool
	Service    *ServiceConfig
	Identities *IdentitySupply
	// Observe supplies the independent raw/accepted-ingress witness to private
	// drill custody. Missing evidence refuses a sample, never invents a CID.
	Observe func(*nats.Conn, net.Conn, c.ID, uint64, c.Deadline) (c.Attachment, error)
}

// BrokerConfig is the single cold/hot/ordinary descriptor. Cold Run, the S6
// restored graph and every ordinary reconnect dial through one Broker built
// from it; there is no second path that is only correct during an image cut.
// Options is the broker's static credential builder (BrokerOptions with its
// seed) and returns the broker's public key. AuditCursor is the clustered
// audit publisher's durable cursor read from the broker's CURRENT Node;
// ok=false means this broker runs no audit replay.
// origin: hot-update S9 plan §4.4 / T48; rv-business F2 / rv-infra F10
type BrokerConfig struct {
	URL         string
	Logger      *slog.Logger
	Options     func() ([]nats.Option, string, error)
	Infra       *BrokerInfra
	AuditCursor func() (uint64, bool, error)
}

// Broker owns the broker's NATS connections. In legacy mode it is only the
// historical connect. Configured, it owns the auth-only generations (P17),
// the gated long-lived business NC, the read-only recovery NC and P18's gate.
//
// Lock order is nc.mu → o.mu: Dial runs under the SDK's connection lock, so no
// code here calls a nats.Conn method while holding o.mu.
type Broker struct {
	cfg      BrokerConfig
	options  []nats.Option
	public   string
	address  string
	auth     *infraauth.Owner
	service  *serviceRuntime
	admitted atomic.Bool

	mu              sync.Mutex
	workers         sync.WaitGroup
	gate            *infrarepair.Gate
	mgmtJS          jetstream.JetStream
	mgmt            *nats.Conn
	ledger          []infrarepair.ACK
	handle          func(string) (string, error)
	replied         func(string, string)
	nc              *nats.Conn
	transport       *infratransport.Transport
	raw             *brokerRaw
	ready           *brokerGeneration
	bound           *brokerGeneration
	lost            <-chan struct{}
	armStop         chan struct{}
	pending         c.Checkpoint
	sequence        uint64
	working         bool
	retired         bool
	closed          bool
	lastErr         string
	observation     *Client
	observationNext map[string]uint64
	// retiredPass marks a RetireUnknown pass whose shared transport retirement
	// already ran after the gate closed (broker_recovery.go retireUnknowns).
	retiredPass bool
}

// A proven generation is consumed by exactly one business dial. Its lost
// channel is P17's generation fence, the same Stop each raw owner watches.
type brokerGeneration struct {
	receipt infraauth.Receipt
	lost    <-chan struct{}
}

func (g *brokerGeneration) live() bool {
	select {
	case <-g.lost:
		return false
	default:
		return true
	}
}

// NewBroker validates the descriptor without I/O. A partial S9 descriptor is
// an error before any effect, never a silent fallback to the legacy path.
func NewBroker(cfg BrokerConfig) (*Broker, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Options == nil {
		return nil, infraauth.ErrBinding
	}
	options, public, err := cfg.Options()
	if err != nil {
		return nil, err
	}
	o := &Broker{cfg: cfg, options: options, public: public}
	if cfg.Infra == nil || (cfg.Infra.Auth == nil && cfg.Infra.Recovery == nil && cfg.Infra.Service == nil && cfg.Infra.Identities == nil) {
		return o, nil
	}
	a := cfg.Infra.Auth
	if a == nil || public == "" {
		return nil, fmt.Errorf("infraattach: S9 broker admission requires ordinary identities and the static auth_callout credential")
	}
	if r := cfg.Infra.Recovery; r != nil && (r.Domain == (c.ID{}) || r.Assets == nil || r.Exclusive == nil || r.Transfers == nil) {
		return nil, c.ErrIncomplete
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" {
		return nil, infraauth.ErrBinding
	}
	o.address = u.Host
	recovery := func(_ uint64, d c.Deadline, stop c.Stop) error { return o.beforeBusiness(d, stop) }
	o.auth, err = infraauth.New(infraauth.Config{URL: cfg.URL, AccountPublicKey: a.AccountPublicKey,
		Options: func() []nats.Option { return o.options }, Transport: o.authTransport, HandleAuth: o.handleAuth, Replied: o.handleReplied,
		Authority: a.Authority, Business: a.Business, Member: a.Member, Agent: a.Agent, Recovery: recovery})
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Configured reports whether the auth-only descriptor owns the connection.
func (o *Broker) Configured() bool { return o != nil && o.auth != nil }

// BrokerOptions builds the nats.Options the broker uses for its own
// connection. With auth_callout enabled, the broker presents its
// pre-configured nkey credential.
//
// It returns the broker's own public key alongside the options, empty when there is no
// auth_callout credential to derive one from. The caller needs it because the private
// inbox is no longer a plain option append — it is a connect-and-probe (natsinbox.Connect),
// and only the caller owns the dial.
// origin: hot-update S9 plan §4.4 / P22 — moved verbatim from broker.brokerConnectOptions
func BrokerOptions(logger *slog.Logger, seed []byte, callout bool) ([]nats.Option, string, error) {
	opts := []nats.Option{
		nats.Name("tetherd"),
		nats.MaxReconnects(-1),
		// THE BROKER NEVER ROAMS OFF ITS OWN NATS SERVER (gotcha #89; simcluster-speed round 2).
		//
		// nats.go adds every server the cluster advertises in INFO to the reconnect pool and,
		// on a disconnect, tries the OTHER pool entries before the one it was on. A broker whose
		// local nats-server is bounced — the topology reconciler's staggered hard restart for a
		// non-reloadable route delta, an operator restart, an upgrade — therefore came back on a
		// PEER's server and stayed there for the rest of its life, silently: its own server was
		// left with no auth_callout responder and no ctl-queue member (a ctl dialing that server
		// black-holed on auth — the "minority cannot authenticate, rc=69 50/50" R6 measured), its
		// forwards and JS calls rode a network path its own server was not on, and drill 96.D's
		// "isolated minority" premise was false three times under load: the partitioned brk1's
		// broker was on brk2's NATS, forwarded the ctl's create to the live leader through it,
		// and logged `session created` while brk1's routes and raft were cut — read as a raft
		// safety violation (#65) by the committer-attribution probe. The broker's server is the
		// one clients reach it through; a reconnect anywhere else is not resilience, it is the
		// broker leaving its post. Ignore the advertised pool and keep dialing the configured
		// URL(s) in order — MaxReconnects(-1) already retries forever, so a bounced local server
		// is rejoined the moment it is back.
		nats.IgnoreDiscoveredServers(),
		nats.DontRandomize(),
		// Every disconnect/reconnect is logged WITH the server it landed on: the roam above was
		// invisible for months because the broker had no reconnect handler at all.
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("broker: NATS disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("broker: NATS reconnected", "url", nc.ConnectedUrlRedacted(), "server", nc.ConnectedServerName())
		}),
	}
	if !callout {
		return opts, "", nil
	}
	pub, err := nkeys.FromSeed(seed)
	if err != nil {
		return nil, "", fmt.Errorf("broker: parse own nkey seed: %w", err)
	}
	pubKey, err := pub.PublicKey()
	pub.Wipe()
	if err != nil {
		return nil, "", fmt.Errorf("broker: derive own pub: %w", err)
	}
	seed = append([]byte(nil), seed...)
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
	opts = append(opts, nats.Nkey(pubKey, sigCB))
	// THE BROKER GETS A PRIVATE REPLY INBOX TOO — origin: prerelease audit round 2.
	//
	// This was missed when the per-identity inbox landed: only ctl and agent set it, so
	// every reply to a broker-originated request (expose forwards, upgrade dispatch, the
	// cluster.apply forward) went to `_INBOX.<nuid>` in the shared space — which every
	// agent on the deployment could subscribe to. The broker is the principal with the
	// most to leak and it was the one still publishing into the commons.
	//
	// WHERE THE BROKER'S GRANT COMES FROM, and why the caller probes. Unlike ctl and
	// agent — whose grants auth_callout mints fresh on every CONNECT — the broker
	// authenticates as a STATIC `users:` entry in nats.conf and bypasses the callout
	// entirely (it is in auth_callout.auth_users). So its reach over auth.InboxRoot is
	// whatever that FILE says:
	//
	//   - a single broker's conf is hand-written per broker-ops §3.4 and carries no
	//     permissions block at all → unrestricted → works, no operator action;
	//   - a clustered broker's conf is rendered by natsconf.Render from
	//     PermissionsForBroker and re-rendered by the topology reconciler → works once
	//     that pass runs, which is long before any client upgrades;
	//   - a broker whose conf WAS rendered and then went force-single is the gap: the
	//     file is frozen with the old list and nothing re-renders it
	//     (topology_reconcile.go returns early when b.cl is nil).
	//
	// That last case is why the dial goes through natsinbox.Connect instead of appending
	// these options here. A refused inbox subscription would otherwise make every
	// broker-originated reply vanish with no diagnostic; instead the broker falls back to
	// the shared space and SAYS SO, which is a line an operator can act on.
	return opts, pubKey, nil
}

// Connect is the cold and hot entry. Legacy mode keeps the historical dial,
// including the private-inbox probe and its loud shared-inbox fallback. The
// configured business NC is only constructed here: its first dial is refused
// until Arm installs the normal handler and one auth generation is proven, so
// even the initial SUB batch is sent after auth_usable and the ACK-prefix read.
// origin: hot-update S9 plan §4.4 / T48; rv-business F2 / rv-infra F10
func (o *Broker) Connect(deadline c.Deadline) (*nats.Conn, error) {
	if o.auth == nil {
		opts := append([]nats.Option(nil), o.options...)
		if deadline != 0 {
			left, err := controlbudget.Remaining(deadline)
			if err != nil {
				return nil, err
			}
			opts = append(opts, nats.Timeout(left))
		}
		if o.public == "" {
			return nats.Connect(o.cfg.URL, opts...)
		}
		nc, sharedInbox, err := natsinbox.Connect(o.cfg.URL, o.public, opts)
		if err == nil && sharedInbox {
			// The one failure mode brokerConnectOptions enumerates that an operator has
			// to fix by hand: a conf that was rendered while clustered and then frozen by
			// force-single. Loud on purpose — the broker still works, so nothing else
			// will ever complain, and what is lost is the confidentiality of every reply
			// the broker receives.
			o.cfg.Logger.Error("broker: this nats.conf does not grant the broker its private reply inbox; "+
				"falling back to the SHARED inbox space, where every agent on this deployment can read "+
				"the broker's replies. Re-render nats.conf (cluster natsconf) so the broker's static "+
				"users: entry carries the current PermissionsForBroker, then restart",
				"inbox_root", auth.InboxRoot)
		}
		return nc, err
	}
	o.mu.Lock()
	closed, previous := o.closed, o.nc
	o.mu.Unlock()
	if closed || (previous != nil && !previous.IsClosed()) {
		return nil, ErrBrokerAdmission
	}
	if err := o.bindRecovery(); err != nil {
		return nil, err
	}
	if err := o.bindService(); err != nil {
		return nil, err
	}
	// S9 mode requires the current static grant: no shared-inbox fallback can
	// precede auth, and the business NC never leaves its own local server.
	nc, err := nats.Connect(o.cfg.URL, o.local("tetherd", o)...)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.nc = nc
	o.mu.Unlock()
	registerBrokerObservation(o, nc)
	return nc, nil
}

func (o *Broker) local(name string, dialer nats.CustomDialer) []nats.Option {
	opts := append([]nats.Option(nil), o.options...)
	opts = append(opts, natsinbox.InboxConnectOptions(o.public)...)
	return append(opts, func(opts *nats.Options) error {
		opts.Servers, opts.Url, opts.InProcessServer = []string{o.cfg.URL}, o.cfg.URL, nil
		if dialer != nil {
			opts.CustomDialer, opts.SkipHostLookup = dialer, true
		}
		return nil
	}, nats.Name(name), nats.RetryOnFailedConnect(true), nats.ReconnectWait(brokerReconnectWait),
		// origin: hot-update S9 plan §10.2 / T83 — S9 broker NCs keep the server's liveness contract.
		nats.PingInterval(20*time.Second), nats.MaxPingsOutstanding(2))
}

// Admit wraps a business handler with the synchronous admission fence. A
// legacy broker returns the handler unchanged.
func Admit(o *Broker, handler nats.MsgHandler) nats.MsgHandler {
	if !o.Configured() {
		return handler
	}
	return func(msg *nats.Msg) {
		if o.admitted.Load() {
			handler(msg)
		}
	}
}

// Admitted reports whether business admission is open for the current dial.
func (o *Broker) Admitted() bool { return !o.Configured() || o.admitted.Load() }

func (o *Broker) handleAuth(request string) (string, error) {
	o.mu.Lock()
	handle := o.handle
	o.mu.Unlock()
	if handle == nil {
		return "", ErrBrokerAdmission
	}
	return handle(request)
}

func (o *Broker) handleReplied(request, response string) {
	o.mu.Lock()
	replied := o.replied
	o.mu.Unlock()
	if replied != nil {
		replied(request, response)
	}
}

// AuthHandle adapts the broker's normal callout owner to P17's handler port:
// the same store-user admission (Enter/Leave borrower accounting) as the
// legacy queue callback, with infraauth owning Respond. P17 also runs it on
// a local request a routed peer answered, so it records no reply witness.
func AuthHandle(enter func() bool, leave func(), handle func(string) (string, error)) func(string) (string, error) {
	return func(request string) (string, error) {
		if !enter() {
			return "", ErrBrokerAdmission
		}
		defer leave()
		return handle(request)
	}
}

// AuthReplied records S6's witnesses — the signed-response observation and
// the reply time (replied, guarded by mu) — for an answer P17's normal queue
// actually delivered. Deciding a request a peer answered is not a reply.
// origin: hot-update S9 P22 resume — design finding (callout queue selection)
func AuthReplied(observe func(string, string), mu *sync.Mutex, replied *int64) func(string, string) {
	return func(request, response string) {
		observe(request, response)
		if now, clockErr := controlbudget.Now(); clockErr == nil {
			mu.Lock()
			*replied = now
			mu.Unlock()
		}
	}
}

// Arm installs the normal handler (and the witnesses of its delivered
// answers), starts admission and waits until the business NC is connected
// under a freshly proven generation, the deadline expires or the caller stops
// (a cold Run cancelled while admission is still being proven must not wait
// out the whole budget). A connected NC left over from a reversible S6 fence
// is redialled through the same gate.
// origin: hot-update S9 plan §4.4 / T48
func (o *Broker) Arm(handle func(string) (string, error), replied func(string, string), deadline c.Deadline, stop c.Stop) (infraauth.Receipt, error) {
	if !o.Configured() || handle == nil {
		return infraauth.Receipt{}, c.ErrUnsupported
	}
	var err error
	if deadline == 0 {
		if deadline, err = controlbudget.After(brokerAdmissionBudget); err != nil {
			return infraauth.Receipt{}, err
		}
	}
	o.mu.Lock()
	nc, raw := o.nc, o.raw
	if o.closed || nc == nil {
		o.mu.Unlock()
		return infraauth.Receipt{}, ErrBrokerAdmission
	}
	o.handle, o.replied, o.retired = handle, replied, false
	if o.armStop == nil {
		o.armStop = make(chan struct{})
	}
	o.mu.Unlock()
	if raw != nil && !o.admitted.Load() {
		raw.interrupt()
	}
	o.kick(deadline)
	for {
		o.mu.Lock()
		bound, last := o.bound, o.lastErr
		o.mu.Unlock()
		if bound != nil && o.admitted.Load() && nc.IsConnected() {
			return bound.receipt, nil
		}
		if _, err := controlbudget.Remaining(deadline); err != nil {
			return infraauth.Receipt{}, fmt.Errorf("%w: %w (last admission: %s)", ErrBrokerAdmission, err, last)
		}
		select {
		case <-stop:
			return infraauth.Receipt{}, fmt.Errorf("%w: stopped (last admission: %s)", ErrBrokerAdmission, last)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (o *Broker) kick(limit c.Deadline) {
	o.mu.Lock()
	if o.working || o.closed || o.retired || o.handle == nil || o.armStop == nil {
		o.mu.Unlock()
		return
	}
	o.working = true
	stop, budget := o.armStop, brokerAttemptBudget
	if o.pending != (c.Checkpoint{}) {
		budget = brokerRecoveryAttempt
	}
	o.workers.Add(1)
	o.mu.Unlock()
	go o.admit(budget, limit, stop)
}

// admit runs one ordinary auth-first Open outside nc.mu, within its own fresh
// attempt budget clamped to the caller's limit. Its result is only a
// candidate: the next gated dial consumes it if the generation is still live.
func (o *Broker) admit(budget time.Duration, limit c.Deadline, stop chan struct{}) {
	defer o.workers.Done()
	deadline, err := controlbudget.After(budget)
	if limit != 0 && limit < deadline {
		deadline = limit
	}
	var receipt infraauth.Receipt
	if err == nil {
		receipt, err = o.auth.Open(deadline, stop)
	}
	text := ""
	if err != nil {
		text = err.Error()
	}
	o.mu.Lock()
	o.working = false
	if err == nil && !o.closed && !o.retired && o.armStop == stop {
		o.ready = &brokerGeneration{receipt: receipt, lost: o.lost}
	}
	changed := text != o.lastErr
	o.lastErr = text
	o.mu.Unlock()
	if changed && text != "" {
		o.cfg.Logger.Warn("broker: ordinary auth-first admission failed; business stays fenced", "err", text)
	} else if changed {
		o.cfg.Logger.Info("broker: ordinary auth-first admission proved", "generation", receipt.Generation, "server", receipt.ServerID)
	}
}

// Dial is the business NC's CustomDialer, and so the only place an SDK
// redial can begin. nats.go replays every SUB after Dial returns and before
// ReconnectedCB, so this gate — not a callback — owns the ordering: no raw
// socket exists until P17 proved the local normal queue, both ordinary
// identities, P18's ACK prefix and the private-inbox RPCs for a live
// generation, and each generation admits exactly one business socket.
// origin: hot-update S9 plan §4.4 / T48 / M24; rv-business F2 / rv-infra F10
func (o *Broker) Dial(network, address string) (net.Conn, error) {
	if address != o.address {
		return nil, c.ErrIncomplete // gotcha #89: the broker never roams to a peer
	}
	o.mu.Lock()
	if o.closed || o.retired || o.handle == nil {
		o.mu.Unlock()
		return nil, ErrBrokerAdmission
	}
	g, stop, old := o.ready, o.armStop, o.transport
	o.ready = nil
	o.sequence++
	sequence := o.sequence
	o.mu.Unlock()
	if g == nil || !g.live() || !o.current(g) {
		o.kick(0)
		return nil, ErrBrokerAdmission
	}
	if old != nil {
		old.Interrupt()
	}
	deadline, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		return nil, err
	}
	transport, err := infratransport.New(sequence, deadline, nil, nil)
	if err != nil {
		return nil, err
	}
	raw, err := dialRaw(transport, network, address, deadline, o.fence)
	if err != nil {
		transport.Interrupt()
		return nil, err
	}
	o.mu.Lock()
	if o.closed || o.retired || o.armStop != stop {
		o.mu.Unlock()
		_ = raw.Close()
		transport.Interrupt()
		return nil, ErrBrokerAdmission
	}
	o.transport, o.raw, o.bound = transport, raw, g
	o.admitted.Store(true)
	o.workers.Add(1)
	o.mu.Unlock()
	go o.watch(g, stop)
	return raw, nil
}

func (o *Broker) current(g *brokerGeneration) bool {
	_, receipt, err := o.auth.Business()
	return err == nil && receipt.Generation == g.receipt.Generation
}

// fence closes admission synchronously, inside the raw Read/Close that makes
// nats.go notice a disconnect, before any queued message or redial can run.
func (o *Broker) fence(raw *brokerRaw) {
	o.mu.Lock()
	if o.raw == raw {
		o.admitted.Store(false)
	}
	o.mu.Unlock()
}

// watch retires the business socket when its auth generation is lost, so an
// auth-only disconnect cannot leave business admitted without a live queue.
func (o *Broker) watch(g *brokerGeneration, stop chan struct{}) {
	defer o.workers.Done()
	select {
	case <-g.lost:
	case <-stop:
		return
	}
	o.mu.Lock()
	var raw *brokerRaw
	if o.bound == g {
		o.admitted.Store(false)
		raw = o.raw
	}
	o.mu.Unlock()
	if raw != nil {
		raw.interrupt()
	}
}

// retireBusiness retires the business transport generation that carried a
// sent-unknown producer request (P18 RetireUnknown): the raw socket closes, so
// no further byte can leave on it; the SDK re-dials through the same auth-first
// gate under a fresh generation; and a PONG on the new socket proves every byte
// the SDK still held — including its reconnect-buffer replay — reached the
// server before retirement is reported. A late server-side effect of that
// request stays possible; P18's asset and ACK read-back then reports it as a
// detected loss, never as a clean seal.
// origin: S9 P18 request "transport retirement"; S9 P22 resume
func (o *Broker) retireBusiness(d c.Deadline, stop c.Stop) error {
	o.mu.Lock()
	old, nc := o.raw, o.nc
	o.mu.Unlock()
	if nc == nil || !o.Configured() {
		return ErrBrokerAdmission
	}
	if old != nil {
		old.interrupt()
	}
	for {
		o.mu.Lock()
		current := o.raw
		o.mu.Unlock()
		if current != nil && current != old && o.admitted.Load() && nc.IsConnected() {
			break
		}
		if _, err := controlbudget.Remaining(d); err != nil {
			return err
		}
		select {
		case <-stop:
			return ErrBrokerAdmission
		case <-time.After(5 * time.Millisecond):
		}
	}
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return err
	}
	return nc.FlushTimeout(left)
}

// Retire is the S6 Fence step: admission closes and the auth generation is
// joined without reopening. The business NC stays with its transport owner.
func (o *Broker) Retire(d c.Deadline) error {
	if !o.Configured() {
		return nil
	}
	o.mu.Lock()
	o.retired, o.handle, o.replied, o.ready, o.bound = true, nil, nil, nil, nil
	o.admitted.Store(false)
	stop := o.armStop
	o.armStop = nil
	o.mu.Unlock()
	if stop != nil {
		close(stop)
	}
	o.workers.Wait()
	return o.auth.Close(d)
}

// Close is terminal: it retires admission, joins every worker and raw owner
// and closes the management NC. The caller drains the business NC first. A
// zero deadline (cold terminal stop) takes one request budget for the join.
func (o *Broker) Close(d c.Deadline) error {
	if !o.Configured() {
		return nil
	}
	var err error
	if d == 0 {
		d, err = controlbudget.After(upgradebudget.RequestLatency)
	}
	err = errors.Join(err, o.Retire(d))
	o.mu.Lock()
	o.closed = true
	if o.observation != nil {
		o.observation.mu.Lock()
		o.observation.closed = true
		unregisterClientObservation(o.observation)
		o.observation.mu.Unlock()
	}
	transport, mgmt, observation := o.transport, o.mgmt, o.observation
	o.mu.Unlock()
	if transport != nil {
		transport.Interrupt()
	}
	if observation != nil {
		observation.active.Wait()
	}
	if mgmt != nil {
		mgmt.Close()
	}
	return err
}

// authTransport binds one P14 raw generation to P17's Transport port. Cancel
// closes the raw sockets before joining dial workers; it never waits on nc.mu.
func (o *Broker) authTransport(d c.Deadline, stop c.Stop) (infraauth.Transport, error) {
	transport, err := infratransport.New(1, d, stop, nil)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.lost = stop
	o.mu.Unlock()
	return newAuthTransport(o, transport, d), nil
}

// CalloutOptions selects the existing static broker credential without making
// the host duplicate the connection owner's credential branches.
func CalloutOptions(logger *slog.Logger, config *xferauth.CalloutConfig) ([]nats.Option, string, error) {
	if config == nil {
		return BrokerOptions(logger, nil, false)
	}
	return BrokerOptions(logger, config.BrokerNkeySeed, true)
}
