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
	if b.tunnelSrv == nil {
		return 0
	}
	closed := 0
	for _, info := range b.tunnelSrv.SessionInfos() {
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
	if b.tunnelSrv == nil {
		return false
	}
	if ok := b.tunnelSrv.CloseProxyIf(info); ok {
		b.cfg.Logger.Info("broker: stale tunnel proxy closed", "reason", reason, "port", info.PublicPort, "sid", info.SID, "nid", info.NID)
		return true
	}
	return false
}
