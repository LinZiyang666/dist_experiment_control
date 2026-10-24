package broker

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// reconcileTunnelSessions closes locally installed public listeners that no longer
// match the authoritative allocation row. It is the backstop for lossy core-NATS
// tunnel-close broadcasts and for home/epoch changes that leave an ex-home listener
// alive until the next broker tick.
func (b *Broker) reconcileTunnelSessions() int {
	srv := b.tunnelSrv.Load()
	if srv == nil {
		return 0
	}
	closed := 0
	for _, info := range srv.SessionInfos() {
		a, err := port.LookupByTokenHash(b.cfg.DB, info.TokenHash)
		switch {
		case errors.Is(err, port.ErrNotFound):
			if b.closeTunnelProxyIfStale(info, "row_missing") {
				closed++
			}
			continue
		case err != nil:
			b.cfg.Logger.Warn("broker: tunnel reconcile lookup", "err", err, "port", info.PublicPort, "sid", info.SID, "nid", info.NID)
			continue
		}
		switch {
		case a.Port != info.PublicPort || a.SID != info.SID || a.NID != info.NID:
			if b.closeTunnelProxyIfStale(info, "identity_mismatch") {
				closed++
			}
		case a.HomeBroker != "" && a.HomeBroker != b.selfNodeID():
			if b.closeTunnelProxyIfStale(info, "not_home") {
				closed++
			}
		case a.HomeBroker != "" && a.Epoch != info.Epoch:
			if b.closeTunnelProxyIfStale(info, "epoch_mismatch") {
				closed++
			}
		}
	}
	return closed
}

func (b *Broker) closeTunnelProxyIfStale(info tunnel.SessionInfo, reason string) bool {
	srv := b.tunnelSrv.Load()
	if srv == nil {
		return false
	}
	if ok := srv.CloseProxyIf(info); ok {
		b.cfg.Logger.Info("broker: stale tunnel proxy closed", "reason", reason, "port", info.PublicPort, "sid", info.SID, "nid", info.NID)
		return true
	}
	return false
}

// Revalidate only imported allocations. Cold boot's port/liveness cleanup is
// inappropriate here: a broker cut does not imply an agent restart. Every grant
// uses the current bounded SQL snapshot, including home/epoch and proxy fences;
// transient reads block activation rather than making an old positive row usable.
// CloseProxyIf carries the complete identity so a late A close cannot remove B.
// origin: hot-update S6 plan §6.3 / T28
func reconcileBrokerImageTunnel(b *Broker, srv *tunnel.Server) (int, error) {
	if b == nil || b.read().SQL() == nil || srv == nil {
		return 0, errors.New("broker: missing imported tunnel authority")
	}
	closed := 0
	for _, info := range srv.SessionInfos() {
		_, err := tunnelAuthorityLookup(b, tunnel.AuthRequest{Allocation: tunnel.Allocation{
			Port: info.PublicPort, SID: info.SID, NID: info.NID,
			TokenHash: info.TokenHash, Epoch: info.Epoch,
		}})
		if err == nil {
			continue
		}
		if !errors.Is(err, errTunnelAuthorityRevoked) {
			return closed, err
		}
		if srv.CloseProxyIf(info) {
			closed++
		}
	}
	return closed, nil
}
