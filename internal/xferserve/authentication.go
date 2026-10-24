package xferserve

import (
	"database/sql"
	"errors"
	"github.com/LinZiyang666/tether/internal/node"
	"log/slog"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// AuthenticateAgent anchors a signed incarnation binding in the same
// provisioned credential the auth callout accepted. A real suffixed device's
// own provisioning wins; only an absent row can use its lease basename.
// origin: hot-update S7 plan §6.3 / T39
func AuthenticateAgent(db *sql.DB, sid, nid, pub string) error {
	if db == nil {
		return errors.New(x.CodeOwnerRecovering)
	}
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		return errors.New(x.CodeNotOwner)
	}
	bound, err := agentprov.Lookup(db, sid, nid)
	if errors.Is(err, agentprov.ErrNotProvisioned) {
		if base, _, leased := proto.SplitLeaseName(nid); leased {
			bound, err = agentprov.Lookup(db, sid, base)
		}
	}
	if err != nil || bound != fp {
		return errors.New(x.CodeNotOwner)
	}
	return nil
}

// NodeGate is only the liveness part of admission. The controller must
// check session/settlement authority first, including revocation of an accepted
// entry. STALE is a heartbeat gap and cannot revoke retained commit ownership.
// Newly created permits/prepares remain ONLINE-only; read-only terminal status
// does not need this gate at all.
// origin: hot-update S7 plan §6.1 / T39
func NodeGate(db *sql.DB, logger *slog.Logger, sid, nid string, accepted bool) string {
	status, err := node.LookupStatus(db, sid, nid)
	if errors.Is(err, node.ErrNotFound) {
		return "node_not_found"
	}
	if err != nil {
		if logger != nil {
			logger.Warn("broker: transfer node gate storage error", "sid", sid, "nid", nid, "err", err)
		}
		return "store_error"
	}
	if status == node.StateOnline || (accepted && status == node.StateStale) {
		return ""
	}
	return "node_offline"
}

// OriginGate selects a responder, not an absence verdict. Instance,
// boot and entry authority are verified against retained state by the controller;
// rejecting them here would silence the only broker able to report recovery.
// A status query lacking Origin needs the controller's complete origin census.
// origin: hot-update S7 plan §6.1 / §6.3 / T39
func OriginGate(clustered bool, selfID string, origin x.Origin) bool {
	if origin.BrokerID == "" {
		return false
	}
	// Single brokers have no cluster selfID. Their controller still checks
	// exact persisted origin authority; this gate only selects a cluster peer.
	if !clustered && selfID == "" {
		return true
	}
	return origin.BrokerID == selfID
}
