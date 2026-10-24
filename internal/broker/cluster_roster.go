package broker

import (
	"database/sql"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/proto"
)

// cluster_roster.go — B7 DOC#3 broker side: build + sign the account-signed broker roster and
// attach it on the register reply (the discovery delivery). Byte-equivalence comes from the
// selfID=="" gate (the Home/Proxy precedent) — a single broker NEVER emits a roster, so the reply
// is byte-identical to today. The roster is signed with the cluster ACCOUNT seed (the existing
// trust anchor) and is DISCOVERY-ONLY (zero membership authority).

// rosterForRegister returns the signed roster to attach to a register reply, or nil. It is nil ONLY
// in single mode (selfID=="") or when the account seed is unavailable — i.e. the inertness is
// CONSTRUCTION-SITE-anchored (selfID), never generation-anchored.
//
// Stage-C M1: it deliberately does NOT suppress on req.RosterGen. The derived generation is a
// wall-clock max over membership timestamps and can REGRESS (a force-single/recover that prunes the
// max-stamp peer, or retiring the newest-stamped node). Gating delivery on `req.RosterGen >= gen`
// would then let a stale agent (cached a higher pre-recovery gen) pin itself to the now-dead broker
// set and have the broker REFUSE to send the corrected roster. So the broker always emits the
// current roster (a few extra bytes on the reply); generation is an ADVISORY tie-breaker only,
// never the broker's suppression decision. (A DELETE-resistant monotone cluster_meta counter is the
// post-v2 hardening, b7-plan §2 DOC#3.)
func (b *Broker) rosterForRegister(req proto.NodeRegisterReq) *proto.ClusterRoster {
	if b.selfID == "" {
		return nil // single broker / unwired → byte-identical reply (no roster key)
	}
	return b.buildSignedRoster() // nil when no account seed
}

// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func (b *Broker) buildSignedRoster() *proto.ClusterRoster {
	if b.cfg.AuthCallout == nil { // no account seed available → cannot sign (defensive; set in cluster mode)
		return nil
	}
	return brokercontrol.BuildSignedRoster(b.cfg.DB, b.cfg.AuthCallout.AccountSeed, b.cfg.Now, rosterTTL)
}

// rosterTTL bounds how long a signed roster a captured agent could replay stays valid. The roster
// is re-pushed on every register, so this is generous-but-finite (the consumer caches it only as a
// short-lived fallback; External-review F14 / re-review).
const rosterTTL = 24 * time.Hour

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
func readRosterBrokers(ro *sql.DB) ([]proto.RosterBroker, uint64, error) {
	return brokercontrol.ReadRosterBrokers(ro)
}
