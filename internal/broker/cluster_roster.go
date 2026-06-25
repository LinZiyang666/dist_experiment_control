package broker

import (
	"database/sql"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
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

// buildSignedRoster reads the replicated roster off RODB() and account-signs it. Returns nil if
// the account seed is unavailable (signing impossible) or no brokers are present.
func (b *Broker) buildSignedRoster() *proto.ClusterRoster {
	if b.cfg.AuthCallout == nil { // no account seed available → cannot sign (defensive; set in cluster mode)
		return nil
	}
	seed := b.cfg.AuthCallout.AccountSeed
	if len(seed) == 0 {
		return nil
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil
	}
	brokers, gen, err := readRosterBrokers(b.cfg.DB)
	if err != nil || len(brokers) == 0 {
		return nil
	}
	// External-review re-review: STAMP a TTL so the anti-replay seam (clusterroster.VerifyAt) is
	// fully armed when the deferred agent consumer lands — a captured old roster cannot then be
	// replayed indefinitely. The roster is re-pushed on every register (frequent), so a short TTL is
	// safe; the agent treats a cached roster only as a short-lived fallback.
	now := b.cfg.Now().UTC()
	expires := now.Add(rosterTTL)
	r, err := clusterroster.Build(seed, accountPub, gen, brokers, now.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano))
	if err != nil {
		return nil
	}
	return r
}

// rosterTTL bounds how long a signed roster a captured agent could replay stays valid. The roster
// is re-pushed on every register, so this is generous-but-finite (the consumer caches it only as a
// short-lived fallback; External-review F14 / re-review).
const rosterTTL = 24 * time.Hour

// readRosterBrokers projects the public broker fields off cluster_nodes + derives an ADVISORY
// generation (the max of the leader-baked added_at/phase_changed_at times, as unix-nano). It
// advances on most membership changes but is NOT strictly monotone (Stage-C M1: it can regress when
// the max-stamp node is pruned/retired) — so delivery never gates on it (rosterForRegister always
// emits); it is only an advisory tie-breaker. NOT the raft commit index (which resets on recover).
func readRosterBrokers(ro *sql.DB) ([]proto.RosterBroker, uint64, error) {
	rows, err := ro.Query(`SELECT node_id, nats_route, public_host, tunnel_addr, cert_fp, phase,
	    COALESCE(added_at,''), COALESCE(phase_changed_at,'') FROM cluster_nodes ORDER BY node_id`)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var out []proto.RosterBroker
	var gen int64
	for rows.Next() {
		var br proto.RosterBroker
		var addedAt, changedAt string
		if err := rows.Scan(&br.NodeID, &br.NatsRoute, &br.PublicHost, &br.TunnelAddr, &br.CertFP, &br.Phase, &addedAt, &changedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, br)
		if v := tsUnixNano(addedAt); v > gen {
			gen = v
		}
		if v := tsUnixNano(changedAt); v > gen {
			gen = v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if gen < 0 {
		gen = 0
	}
	return out, uint64(gen), nil
}

// tsUnixNano parses a leader-baked timestamp (RFC3339Nano OR Go's time.String() form) to unix
// nanos; 0 on any parse failure (the signature is the real anchor; generation is rollback-detection
// and degrades safely to 0).
func tsUnixNano(s string) int64 {
	if s == "" {
		return 0
	}
	// Stage-C m5: a Go time.String() with a monotonic-clock reading carries a trailing " m=+…"
	// token the layouts below don't cover — strip it so such a stamp still parses (else it would
	// silently yield 0).
	if i := strings.Index(s, " m="); i >= 0 {
		s = s[:i]
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano()
		}
	}
	return 0
}
