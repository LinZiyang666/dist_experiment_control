// transfer_home.go is the D8a (§9) BUILD-AND-PROVE mechanism file for distributing file
// transfer over the cluster: the tier-B replica target seam (xferTargetReplicas) and — added
// in later steps — the home-routing gate and the home-gated orphan reaper filter.
//
// Everything here is INERT in production (cutover=D9): serve.go never calls AttachXferReplicas
// nor the home seam, so xferTargetReplicas() returns jsstream.ReplicasSingle (byte-identical
// R=1 bucket) and the transfer handlers run on whichever broker received the request, exactly
// as today.
//
// EXCLUDED from the TestD8ProductionWiresNoCluster guard scan: this is where the
// `b.xferReplicasFn =` write lives (the guard bans that token from the SCANNED production
// files so the seam can never be wired there).
package broker

import "github.com/LinZiyang666/tether/internal/jsstream"

// xferTargetReplicas returns the replica factor a freshly-created or reconciled OBJ_xfer
// bucket should target. In production (xferReplicasFn==nil, build-and-prove) it is
// jsstream.ReplicasSingle (R=1, byte-identical to the prior literal). When the harness has
// attached the cluster seam it is ReplicasFor(NumVoters) so a completed tier-B object lands
// on a quorum of brokers and survives a home-broker kill at N>=3 (§9 EXIT gate). Reading the
// nil seam is the only production-visible change; this method is called from the scanned
// ensureXferBucket call sites (b.xferTargetReplicas() is not a banned token).
func (b *Broker) xferTargetReplicas() int {
	if b.xferReplicasFn != nil {
		return b.xferReplicasFn()
	}
	return jsstream.ReplicasSingle
}

// AttachXferReplicas wires the D8a tier-B replica seam (TEST/HARNESS ONLY). fn typically
// closes over a cluster.Node: func() int { nv, _ := node.NumVoters(); return
// jsstream.ReplicasFor(nv) }. Call before Run. Production never calls it.
func (b *Broker) AttachXferReplicas(fn func() int) { b.xferReplicasFn = fn }

// transferHomeGate decides whether THIS broker should START a transfer for (sid,nid). In
// production (selfID=="", build-and-prove) it ALWAYS proceeds — the request path is
// byte-identical to today. In clustered mode only the agent's HOME broker proceeds: every
// transfer subject is a plain broadcast subscription (broker.go), so in a routed NATS every
// broker receives every push/pull request — without this gate N brokers would each ensure a
// bucket, register a tracker entry, forward to the agent and emit audit (N-way fan-out, §9).
// A non-home broker returns false; the START handlers then return SILENTLY (no reply) so the
// home broker's answer is the only one ctl sees. Home unresolved/ineligible also returns
// false: no broker can match an absent binding, so every broker stays silent and ctl times
// out + retries until the D6 binding converges (the home_catching_up posture; emitting an
// error from "some" broker is impossible without consensus, §9 D8).
//
// Only the START handlers (push.req / pull.req) consult this gate. The continuation/terminal
// handlers (push-commit / ev.transfer / finalize) are already routed by TRACKER PRESENCE —
// the broker that started the transfer is the only one holding its entry (transfers.get /
// claimFinalize return nothing elsewhere), so a transfer's whole lifecycle stays on its
// origin broker even across a mid-transfer rehome, and per-invocation transfer_ids keep two
// brokers from ever binding the same object (no epoch-fence needed).
func (b *Broker) transferHomeGate(sid, nid string) bool {
	if b.selfID == "" {
		return true // production: inert, byte-identical
	}
	home := b.resolveHomeForAgent(sid, nid)
	if home == nil {
		return false // unresolved/ineligible: stay silent, ctl retries
	}
	return home.NodeID == b.selfID
}

// homeOwnsXferBucket decides whether THIS broker may reap orphan objects from the shared,
// replicated OBJ_xfer-<sid> bucket during boot reconciliation (§9 D8). In production
// (selfID=="", build-and-prove) it is always true — a single broker owns every bucket, so
// the boot reaper's reap-everything behavior is byte-identical. In clustered mode the bucket
// is REPLICATED across brokers, so reaping every object (as the single-broker logic does,
// since the in-memory tracker is empty after boot) would delete a LIVE in-flight object that
// ANOTHER broker — the actual home of its target node — is mid-transferring (its tracker, not
// ours, protects it). To stay safe without per-object attribution (the object name is the
// transfer_id, which does not encode the target nid), we reap a bucket ONLY when self is the
// home of EVERY node currently bound to the session: then no other broker transfers into it,
// and self's empty post-boot tracker means none of self's own objects are resumable (the
// receiver-finalization invariant). A mixed-home or other-homed session is skipped here; a
// leader-driven cross-home GC is D9.
func (b *Broker) homeOwnsXferBucket(sid string) bool {
	if b.selfID == "" {
		return true // production: single broker owns every bucket
	}
	// Collect the session's node ids FIRST, then close the rows BEFORE resolving each home
	// via a nested query: the cluster DB is SetMaxOpenConns(1), so a resolveHomeForAgent
	// issued while `rows` still holds the single connection would DEADLOCK (same hazard
	// homeForRegister guards against).
	rows, err := b.cfg.DB.Query(`SELECT nid FROM nodes WHERE sid=?`, sid)
	if err != nil {
		return false
	}
	var nids []string
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			_ = rows.Close()
			return false
		}
		nids = append(nids, nid)
	}
	cerr := rows.Err()
	_ = rows.Close()
	if cerr != nil || len(nids) == 0 {
		return false // no bound nodes (or scan error): cannot attribute — skip conservatively
	}
	for _, nid := range nids {
		home := b.resolveHomeForAgent(sid, nid)
		if home == nil || home.NodeID != b.selfID {
			return false // some node is unresolved or homed elsewhere: not ours to reap
		}
	}
	return true
}

// xferBucketOrphanedEverywhere reports whether NO broker can EVER reap the OBJ_xfer-<sid> bucket:
// the session has zero bound nodes, or its bound nodes resolve to no single common home (an unresolved
// node, or two distinct homes = a split-home session). Since homeOwnsXferBucket requires self to be the
// home of EVERY bound node, in each of those cases it returns false on EVERY broker and the bucket is
// immortal (the #58 split-home residual / racknerd small-disk-fill class). A bucket whose bound nodes
// all share ONE home elsewhere is NOT orphaned-everywhere — that broker will reap it. Read-only, used
// only for the N-6 unreapable-bucket gauge; callers gate on cluster mode (selfID!=""). Same collect-
// then-close discipline as homeOwnsXferBucket (SetMaxOpenConns(1)). A read error returns false —
// observability must not fabricate a defect it cannot confirm.
func (b *Broker) xferBucketOrphanedEverywhere(sid string) bool {
	rows, err := b.cfg.DB.Query(`SELECT nid FROM nodes WHERE sid=?`, sid)
	if err != nil {
		return false
	}
	var nids []string
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			_ = rows.Close()
			return false
		}
		nids = append(nids, nid)
	}
	cerr := rows.Err()
	_ = rows.Close()
	if cerr != nil {
		return false
	}
	if len(nids) == 0 {
		return true // zero-node session bucket: no broker is its home, ever
	}
	commonHome := ""
	for _, nid := range nids {
		home := b.resolveHomeForAgent(sid, nid)
		if home == nil {
			return true // an unresolved node: homeOwnsXferBucket fails on every broker
		}
		if commonHome == "" {
			commonHome = home.NodeID
		} else if home.NodeID != commonHome {
			return true // split home: no single broker owns all bound nodes
		}
	}
	return false // all bound nodes share one home — that broker will reap this bucket
}
