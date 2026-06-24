package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// TunnelExposeAdapter is the production ExposeAdapter that backs
// `tether expose` with a live reverse-TCP tunnel via internal/tunnel.
//
// It owns one tunnel.Client per agent process (= per (machine, sid)).
// Each AddProxy opens a new (publicPort → localPort) yamux session to
// the broker; RemoveProxy(name, publicPort) tears it down by port.
//
// The tunnel client's LocalPortLookup callback is satisfied by the
// in-memory map populated at AddProxy time, so we never need to
// re-read state.json on the data path.
type TunnelExposeAdapter struct {
	client *tunnel.Client
	logger *slog.Logger

	opMu     sync.Mutex
	mu       sync.RWMutex
	localFor map[int]int // publicPort → localPort
}

// Compile-time contracts: the production adapter MUST satisfy ExposeAdapter AND the
// OPTIONAL homeApplier + homeSessionChecker (D6 §7.4 / audit M3). The agent type-asserts
// the optionals at runtime, so a missing method is otherwise a SILENT production no-op
// (the agent-rehome F1 / openHomeFromState-dead-branch bug). These assertions turn that
// divergence into a compile error instead of a field-only failure.
var (
	_ ExposeAdapter      = (*TunnelExposeAdapter)(nil)
	_ homeApplier        = (*TunnelExposeAdapter)(nil)
	_ homeSessionChecker = (*TunnelExposeAdapter)(nil)
)

// NewTunnelExposeAdapter wires a tunnel.Client at brokerAddr and
// returns the adapter. Caller MUST call Start(ctx) once before any
// AddProxy / RemoveProxy.
func NewTunnelExposeAdapter(brokerAddr, sid, nid string, logger *slog.Logger) *TunnelExposeAdapter {
	a := &TunnelExposeAdapter{
		logger:   logger,
		localFor: map[int]int{},
	}
	a.client = tunnel.NewClient(brokerAddr, sid, nid, a.lookupLocal, logger)
	return a
}

// Start anchors the underlying tunnel.Client to ctx.
func (a *TunnelExposeAdapter) Start(ctx context.Context) {
	a.client.Start(ctx)
}

// SetSessionStateHook forwards a data-plane up/down callback to the tunnel
// client. The agent wires this (in agent.New) to publish proxy ready/unready
// so /sub tracks real liveness. Pass-through: policy (the proxy-port filter)
// lives in the agent, not here.
func (a *TunnelExposeAdapter) SetSessionStateHook(fn func(publicPort int, up bool)) {
	a.client.SetSessionStateHook(fn)
}

// AddProxy opens a tunnel session. Failure → caller (agent.handle
// ExposeForwarded) rolls back state.json + replies frpc_failed to the
// broker so the SQLite row is freed.
func (a *TunnelExposeAdapter) AddProxy(p PortToken) error {
	a.opMu.Lock()
	defer a.opMu.Unlock()

	a.setLocal(p.Port, p.LocalPort)
	// D6 §7.5: dial THIS expose's home (p.HomeBrokerAddr, "" ⇒ the Client's single
	// --tunnel-addr fallback) at its home epoch, pinned by the directive's certs.
	// A clustered home (non-empty addr) with no pins returns ErrHomePinsRequired;
	// the caller (replay) defers until a register/expose reply re-delivers them.
	if err := a.client.OpenHome(p.Port, p.LocalPort, p.Token, p.HomeBrokerAddr, p.Epoch, p.CertPins); err != nil {
		a.deleteLocal(p.Port)
		return fmt.Errorf("tunnel adapter AddProxy: %w", err)
	}
	return nil
}

// ApplyHome performs an epoch-ordered rehome of an already-open expose (D6
// §7.4): the tunnel client Open-replaces the session against the new home iff
// the directive epoch is newer. It implements the optional homeApplier interface
// the agent type-asserts. A transient home_catching_up surfaces here for the
// caller (applyReconciliation) to retry.
func (a *TunnelExposeAdapter) ApplyHome(publicPort int, brokerAddr string, epoch int64, certPins proto.CertPins) error {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	return a.client.ApplyHome(publicPort, brokerAddr, epoch, certPins)
}

// HasSession reports whether a live tunnel session is currently open for
// publicPort. It satisfies the homeSessionChecker interface the agent
// type-asserts in applyOneHome (audit M3 / agent-rehome F1): without it the
// production adapter never matched homeSessionChecker, so the
// `!HasSession -> openHomeFromState` recovery branch was DEAD in production —
// a clustered expose whose tunnel was not yet (re)opened (e.g. the boot-order
// race where applyReconciliation runs before replayPortsFromState) fell through
// to ApplyHome, found no session, returned a no-op nil, and reported a FALSE
// "rehomed" success while the expose stayed DOWN until the next NATS reconnect.
// Delegating to the tunnel client makes the openHomeFromState recovery path live.
func (a *TunnelExposeAdapter) HasSession(publicPort int) bool {
	return a.client.HasSession(publicPort)
}

// RemoveProxy closes the tunnel session for publicPort. Name is
// unused (the broker keys by name, the tunnel keys by port).
func (a *TunnelExposeAdapter) RemoveProxy(_ string, publicPort int) error {
	a.opMu.Lock()
	defer a.opMu.Unlock()

	a.client.Close(publicPort)
	a.deleteLocal(publicPort)
	return nil
}

func (a *TunnelExposeAdapter) lookupLocal(publicPort int) (int, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if local, ok := a.localFor[publicPort]; ok {
		return local, nil
	}
	return 0, fmt.Errorf("tunnel adapter: no local mapping for public port %d", publicPort)
}

func (a *TunnelExposeAdapter) setLocal(publicPort, localPort int) {
	a.mu.Lock()
	a.localFor[publicPort] = localPort
	a.mu.Unlock()
}

func (a *TunnelExposeAdapter) deleteLocal(publicPort int) {
	a.mu.Lock()
	delete(a.localFor, publicPort)
	a.mu.Unlock()
}
