package connrecovery

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Recovery owns bounded raw-connection teardown; callers supply their close and escalation policy.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
type Intent int

const (
	Rebuild Intent = iota
	Shutdown
)

// teardownDialTimeout replaces the per-dial timeout nats.go skips when a CustomDialer is set
// (nats.go connects via the dialer without applying Options.Timeout or its host-count split).

// trackerConnCap bounds the per-session conn registry (internal review CT-5). nats.go holds at
// most one live connection at a time; the extra slots cover a reconnect storm's in-flight
// stragglers with room to spare, and anything older than that cannot still be blocking it.

const DialTimeout = 2 * time.Second
const TrackerCapacity = 8

type Config struct {
	Logger                   *slog.Logger
	CloseBudget, PoisonGrace time.Duration
	Release, Close           func()
	Poison                   func() int
	Escalate                 func(Intent)
}

// Tracker wraps the dialer nats.go uses so the raw net.Conns behind a *nats.Conn stay reachable.
// It is BOTH the production instrument (poisoning) and the test seam (a fake dialer can hand back a
// conn whose Write blocks forever, reproducing the field hang deterministically).
//
// KNOWN LIMIT (internal review CT-7): nats.go probes a CustomDialer for the optional
// SkipTLSHandshake interface; this wrapper does not forward it, so a wrapped dialer implementing it
// would be silently ignored. Inert today — proxydial deliberately does not implement it — and
// recorded here rather than pre-solved because forwarding an interface nobody implements is
// untestable code.
//
// Sticky by design: once poisoned, conns registered LATER are poisoned on arrival and new dials are
// refused. A one-shot sweep would race nats.go's reconnect loop dialing a fresh socket and blocking
// on that one instead.
type Tracker struct {
	mu       sync.Mutex
	conns    []net.Conn
	poisoned bool
	// dialCtx bounds every dial. HONEST SCOPE (internal review CT-4): connectNATS passes the
	// AGENT-level ctx, not the per-session runCtx, so cancelling a session does NOT abort an
	// in-flight dial — poisoning and the ladder's budget are what bound teardown; this ctx only
	// carries process shutdown plus the per-dial timeout applied below. Nothing depends on the
	// stronger property, and wiring runCtx here would need the tracker built after the session ctx
	// exists (it is built inside connectNATS, before that point).
	dialCtx context.Context
	// dialFn is the underlying dialer (proxy-aware in production, a fake in tests).
	dialFn func(ctx context.Context, network, address string) (net.Conn, error)
}

func NewTracker(ctx context.Context, dialFn func(context.Context, string, string) (net.Conn, error)) *Tracker {
	if dialFn == nil {
		dialFn = (&net.Dialer{}).DialContext
	}
	return &Tracker{dialCtx: ctx, dialFn: dialFn}
}

// Dial implements nats.CustomDialer.
func (t *Tracker) Dial(network, address string) (net.Conn, error) {
	t.mu.Lock()
	poisoned := t.poisoned
	base := t.dialCtx
	t.mu.Unlock()
	if poisoned {
		return nil, errors.New("agent: connection teardown in progress; refusing to dial")
	}
	if base == nil {
		// ctx-none: a tracker built without a ctx (defensive/test construction only — connectNATS
		// always passes the agent ctx). origin: external review F8, CLAUDE.md §5 requires every new
		// context.Background() site to name its class. This creates no uncancellable root in
		// production: the WithTimeout below always bounds the dial, and the teardown ladder's
		// poisoning is what bounds an in-flight one.
		// ctx-none: defensive construction without a caller context; the dial timeout still applies.
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, DialTimeout)
	defer cancel()
	c, err := t.dialFn(ctx, network, address)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.poisoned {
		// Poisoned while this dial was in flight: kill it on arrival rather than handing nats.go a
		// socket it could block on after teardown started.
		t.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("agent: connection teardown in progress; discarding late dial")
	}
	// origin: internal review CT-5 (S11) — bound the registry. A long-lived session on a flapping
	// link redials indefinitely (MaxReconnects(-1)), and an append-only slice would grow a dead
	// conn reference per reconnect for as long as the process runs. Poisoning only ever needs the
	// conns that could still be blocking nats.go, so keep the newest TrackerCapacity.
	t.conns = append(t.conns, c)
	if len(t.conns) > TrackerCapacity {
		drop := len(t.conns) - TrackerCapacity
		t.conns = append([]net.Conn(nil), t.conns[drop:]...)
	}
	t.mu.Unlock()
	return c, nil
}

// poison makes every tracked conn fail fast and keeps that property for conns dialed later. The
// deadline in the past forces a blocked read/write to return immediately; Close covers the rest.
func (t *Tracker) Poison() int {
	t.mu.Lock()
	t.poisoned = true
	conns := append([]net.Conn(nil), t.conns...)
	t.mu.Unlock()
	// Kept local (same body as infratransport.InterruptRaw): this leaf is
	// standard-library-only below every product owner, so it cannot import
	// the S9 transport that sits above it (layering row connrecovery).
	// origin: hot-update S5 plan §10 / AGENT-SLIM-S5; S9 plan §4.5 / T32 (R10)
	for _, c := range conns {
		_ = c.SetDeadline(time.Unix(1, 0))
		_ = c.Close()
	}
	return len(conns)
}

// finalizeConn runs the whole S1–S5 ladder for one connection. Single-flight per session via
// sync.Once in the caller (sessionFinalizer) so the fireRedial path and the session's own deferred
// teardown cannot both drive it.
//
// Returns when the closer goroutine has been JOINED — never before, and never by abandoning it.
func Finalize(parent context.Context, cfg Config, intent Intent, cancel context.CancelFunc, cleanups []func()) {
	// S2 — cancel FIRST. Pure pointer/channel work: heartbeatLoop, roster refresh and every ctx-
	// aware producer stop here, regardless of what the socket does.
	if cancel != nil {
		cancel()
	}

	// S3 — the real close, off the recovery path.
	done := make(chan struct{})
	closeIntent := intent
	go func() {
		defer close(done)
		// The farewell goes INSIDE this goroutine, under the same budget as the
		// close.
		//
		// It was briefly on the caller's side, and that reintroduced exactly the
		// hang S2/S3 exist to prevent: nc.Publish takes nc.mu, and on a
		// RECONNECTING conn doReconnect holds that mutex across a dial with no
		// deadline of its own. A stopping agent then blocked indefinitely on a
		// courtesy message — gotcha #72's shape, on the shutdown path, where the
		// whole point of the ladder is that a stop can never be held hostage.
		// Here the select below bounds it like everything else, and the
		// escalation fires if it wedges.
		//
		// Only on a real stop: a rebuild keeps the same process and the same
		// claim, so releasing there would invite a straggler clone to take the
		// name from a still-running agent.
		if closeIntent == Shutdown {
			cfg.Release()
		}
		// nc.mu-touching cleanups first (subscription teardown before the close), all inside the
		// same budget — see addBoundedCleanup.
		for _, fn := range cleanups {
			fn()
		}
		if cfg.Close != nil {
			// Drain would be nicer, but on a RECONNECTING conn nats.go converts it to Close anyway
			// (and takes the same mutex): Close is the honest call.
			cfg.Close()
		}
	}()

	select {
	case <-done:
		return
	case <-time.After(cfg.CloseBudget):
	}

	// S4 — poison. This is what makes the join below bounded.
	n := 0
	if cfg.Poison != nil {
		n = cfg.Poison()
	}
	cfg.Logger.Warn("agent: NATS close exceeded its budget; poisoning the underlying connection(s)",
		"budget", cfg.CloseBudget.String(), "conns_poisoned", n, "intent", intentName(intent))

	select {
	case <-done:
		cfg.Logger.Warn("agent: NATS close returned after poisoning (teardown recovered within budget+grace)")
		return
	case <-time.After(cfg.PoisonGrace):
	}

	// S5 — escalate. We are wedged somewhere poisoning cannot reach (the nc.mu-holding DNS lookup
	// is the known such layer: no fd exists yet to poison). The closer goroutine is still live and
	// we are deliberately NOT pretending otherwise — the process is about to be replaced or exit.
	// Parent shutdown wins over an earlier redial callback's rebuild request: otherwise a
	// systemctl stop racing this 20s ladder can self-exec and resurrect the daemon it is stopping.
	escalationIntent := intent
	if escalationIntent == Rebuild && parent != nil && parent.Err() != nil {
		escalationIntent = Shutdown
	}
	cfg.Logger.Error("agent: NATS teardown WEDGED past its budget and poison grace; escalating",
		"close_budget", cfg.CloseBudget.String(), "poison_grace", cfg.PoisonGrace.String(),
		"intent", intentName(escalationIntent))
	cfg.Escalate(escalationIntent)
}

func intentName(i Intent) string {
	if i == Shutdown {
		return "shutdown"
	}
	return "rebuild"
}

// The planned path that bypasses the legacy 10+10 escalation ladder is
// infratransport.Move itself (the former FinalizePlanned here only forwarded to
// it): only the raw transport generation is retired there, and business
// cleanups belong to their owners. Keeping a forwarder in this leaf made it
// import the transport above it.
// origin: hot-update S9 plan §4.5 / T32–T33 (R10 layering)
