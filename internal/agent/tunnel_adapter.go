package agent

import (
	"context"
	"fmt"
	"log/slog"

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

	localFor map[int]int // publicPort → localPort
}

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

// AddProxy opens a tunnel session. Failure → caller (agent.handle
// ExposeForwarded) rolls back state.json + replies frpc_failed to the
// broker so the SQLite row is freed.
func (a *TunnelExposeAdapter) AddProxy(p PortToken) error {
	a.localFor[p.Port] = p.LocalPort
	if err := a.client.Open(p.Port, p.LocalPort, p.Token); err != nil {
		delete(a.localFor, p.Port)
		return fmt.Errorf("tunnel adapter AddProxy: %w", err)
	}
	return nil
}

// RemoveProxy closes the tunnel session for publicPort. Name is
// unused (the broker keys by name, the tunnel keys by port).
func (a *TunnelExposeAdapter) RemoveProxy(_ string, publicPort int) error {
	a.client.Close(publicPort)
	delete(a.localFor, publicPort)
	return nil
}

func (a *TunnelExposeAdapter) lookupLocal(publicPort int) (int, error) {
	if local, ok := a.localFor[publicPort]; ok {
		return local, nil
	}
	return 0, fmt.Errorf("tunnel adapter: no local mapping for public port %d", publicPort)
}
