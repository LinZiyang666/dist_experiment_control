package cluster

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/nats-io/nkeys"
)

// membership_ops.go — D7 §8.1 cluster-membership ops (Plan + the one custom
// applier). These are cluster's OWN ops, so their Plan helpers live HERE (like
// D1's clusterMetaPlanner), NOT in a mutator package: internal/clusternodes is the
// pure-SQL home-resolution leaf and must NOT import internal/cluster (L-2), so it
// cannot host writers that build *cluster.Command.

// joinDomainSep domain-separates the join-PoP signed message so a signature minted
// for some other tether protocol/context can never be replayed as a join proof.
// The "-v2" tracks proto v2; a future wire break bumps it.
const joinDomainSep = "tether-cluster-join-v2"

// JoinSignBytes builds the exact bytes the joining node signs and every replica
// re-verifies: domain-sep || NUL || node_id || NUL || node_ident_pub || NUL ||
// join_nonce. It binds node_id (the load-bearing identity) and node_ident_pub to
// the leader-issued single-use nonce. It deliberately does NOT bind a cluster-id:
// the nonce is leader-issued and fresh, so a signature for cluster A (over A's
// nonce) can never satisfy cluster B's challenge (B issues its own nonce). The
// NUL separators are unambiguous because LitText/the token layer reject embedded
// NULs in any field.
func JoinSignBytes(nodeID, nodeIdentPub, joinNonce string) []byte {
	var b strings.Builder
	b.WriteString(joinDomainSep)
	b.WriteByte(0)
	b.WriteString(nodeID)
	b.WriteByte(0)
	b.WriteString(nodeIdentPub)
	b.WriteByte(0)
	b.WriteString(joinNonce)
	return []byte(b.String())
}

// clusterNodeUpsertAux is the apply-inert payload carried in Command.Aux for
// OpClusterNodeUpsert. It lets every replica re-derive the canonical join message
// and re-verify the PoP signature WITHOUT parsing the baked SQL literals (an
// applier cannot introspect Body to recover the bytes it must verify). The same
// values are ALSO baked into the roster row columns; clusterNodeUpsertApplier
// cross-checks Aux against the baked literals so a leader cannot sign over one
// pubkey while writing a different one into the row.
type clusterNodeUpsertAux struct {
	NodeID       string `json:"node_id"`
	Name         string `json:"name"` // carried so the cross-check can pin name at its column (anti-alias, review M8)
	NodeIdentPub string `json:"ident_pub"`
	JoinNonce    string `json:"nonce"`
	JoinSigHex   string `json:"sig"` // hex(ed25519 sig), same value as the join_sig column
}

// ClusterNodeUpsertInput is the leader's request to admit (or refresh) a roster
// row. All values originate from the operator's `cluster add` + the joiner's
// signed token; the leader bakes them as SQL literals and re-verifies the PoP
// before proposing.
type ClusterNodeUpsertInput struct {
	NodeID       string
	Name         string
	NodeIdentPub string
	NatsServerID string
	RaftAddr     string
	NatsRoute    string
	TunnelAddr   string
	PublicHost   string
	CertFP       string
	BusNkey      string // NATS bus nkey pub, baked at admission to break the learner backfill deadlock (audit A)
	JoinNonce    string
	JoinSigHex   string
	Now          time.Time
}

// PlanClusterNodeUpsert renders OpClusterNodeUpsert (§8.1 phase-1). The leader
// re-verifies the join PoP BEFORE proposing (fail-closed: never spend a raft
// round-trip + applied_index advance on an entry every follower would reject), and
// the follower re-verifies in Apply (the cross-node proof). It bakes an
// all-literal INSERT ... ON CONFLICT(node_id) DO UPDATE whose UPDATE is guarded by
// a phase-PREDECESSOR predicate `WHERE cluster_nodes.phase IN
// ('JOIN_VERIFIED_PENDING_VOTER','VOTER_ADD_FAILED')` — so a stale re-add of an
// already-advanced node (CATCHING_UP/VOTER) or a draining node is a deterministic
// RowsAffected==0 no-op (the membership analogue of PlanReassignHome's epoch CAS),
// never a regression to PENDING. The verify inputs travel in cmd.Aux for the
// follower re-verify. NO reqID (leader-driven; the phase guard is the idempotency
// anchor).
//
// Pre-propose validation (leader-local): the operator-keyed node_ident_pub, name
// uniqueness, and node_id format are the orchestrator's responsibility; this Plan
// fails closed on an unverifiable signature and on any value LitText rejects (NUL/
// non-UTF-8), so a malformed admission never reaches the log.
func PlanClusterNodeUpsert(in ClusterNodeUpsertInput) (*Command, error) {
	sig, err := hex.DecodeString(in.JoinSigHex)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-upsert: bad sig hex: %w", err)
	}
	msg := JoinSignBytes(in.NodeID, in.NodeIdentPub, in.JoinNonce)
	if verr := auth.VerifySignature(in.NodeIdentPub, msg, sig); verr != nil {
		return nil, fmt.Errorf("cluster: plan node-upsert: join PoP did not verify: %w", verr)
	}

	lits, err := LitTextAll(in.NodeID, in.Name, in.NodeIdentPub, in.NatsServerID,
		in.RaftAddr, in.NatsRoute, in.TunnelAddr, in.PublicHost, in.CertFP,
		in.JoinNonce, in.JoinSigHex, in.BusNkey)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-upsert: bake literal: %w", err)
	}
	nodeID, name, identPub, natsSrv := lits[0], lits[1], lits[2], lits[3]
	raftAddr, natsRoute, tunnelAddr := lits[4], lits[5], lits[6]
	publicHost, certFP, nonceLit, sigLit := lits[7], lits[8], lits[9], lits[10]
	busNkey := lits[11]
	tsLit := LitTime(in.Now.UTC())
	const pending = "'JOIN_VERIFIED_PENDING_VOTER'"

	// bus_nkey_pub is baked at column 10 (right after cert_fp) so the positional Aux cross-check in
	// clusterNodeUpsertApplier — which matches the contiguous identFrag (cols 1-3) and joinFrag
	// (nonce/sig/voter_add_error) — stays intact (audit A; the splice is order-sensitive).
	sqlStr := `INSERT INTO cluster_nodes(node_id, name, node_ident_pub, nats_server_id, ` +
		`raft_addr, nats_route, tunnel_addr, public_host, cert_fp, bus_nkey_pub, cert_fp_prev, ` +
		`cert_fp_valid_until, phase, added_at, join_nonce, join_sig, voter_add_error, phase_changed_at) VALUES(` +
		nodeID + `, ` + name + `, ` + identPub + `, ` + natsSrv + `, ` +
		raftAddr + `, ` + natsRoute + `, ` + tunnelAddr + `, ` + publicHost + `, ` + certFP + `, ` + busNkey + `, NULL, ` +
		`NULL, ` + pending + `, ` + tsLit + `, ` + nonceLit + `, ` + sigLit + `, NULL, ` + tsLit + `) ` +
		`ON CONFLICT(node_id) DO UPDATE SET name=excluded.name, node_ident_pub=excluded.node_ident_pub, ` +
		`nats_server_id=excluded.nats_server_id, raft_addr=excluded.raft_addr, nats_route=excluded.nats_route, ` +
		`tunnel_addr=excluded.tunnel_addr, public_host=excluded.public_host, ` +
		// v0.4.4 review F2: empty-PRESERVE cert_fp + bus_nkey_pub. An idempotent grow-retry re-approve that
		// carries an empty bundle must NOT clobber a previously-good value back to '' — that re-arms the
		// learner self-backfill deadlock (bus_nkey) / crash-loop (cert_fp) with zero runtime signal.
		`cert_fp=CASE WHEN excluded.cert_fp != '' THEN excluded.cert_fp ELSE cluster_nodes.cert_fp END, ` +
		`bus_nkey_pub=CASE WHEN excluded.bus_nkey_pub != '' THEN excluded.bus_nkey_pub ELSE cluster_nodes.bus_nkey_pub END, ` +
		`phase=` + pending + `, join_nonce=excluded.join_nonce, join_sig=excluded.join_sig, ` +
		`voter_add_error=NULL, phase_changed_at=excluded.phase_changed_at ` +
		`WHERE cluster_nodes.phase IN ('JOIN_VERIFIED_PENDING_VOTER','VOTER_ADD_FAILED')`

	cmd := NewCommand(OpClusterNodeUpsert, Stmt(sqlStr))
	aux, err := json.Marshal(clusterNodeUpsertAux{
		NodeID:       in.NodeID,
		Name:         in.Name,
		NodeIdentPub: in.NodeIdentPub,
		JoinNonce:    in.JoinNonce,
		JoinSigHex:   in.JoinSigHex,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-upsert: marshal aux: %w", err)
	}
	cmd.Aux = aux
	return cmd, nil
}

// clusterNodeUpsertApplier is the ONE custom Applier (D7 §8.1): it re-verifies the
// join PoP signature on EVERY replica before execing the baked roster UPSERT. A
// verify failure returns errAppliedRejected — the FSM advances applied_index, runs
// no op SQL, and never panics (so a forged committed entry cannot brick the
// cluster on log replay; §2.8). The verdict is a pure function of committed bytes,
// so every replica rejects identically (no fork).
type clusterNodeUpsertApplier struct{}

var _ Applier = clusterNodeUpsertApplier{}

func (clusterNodeUpsertApplier) ApplyTx(tx *sql.Tx, cmd *Command) error {
	var aux clusterNodeUpsertAux
	if err := json.Unmarshal(cmd.Aux, &aux); err != nil {
		// A committed OpClusterNodeUpsert with no/garbled Aux cannot be verified;
		// reject it deterministically (every replica fails the same Unmarshal).
		return fmt.Errorf("%w: decode aux: %v", errAppliedRejected, err)
	}
	sig, err := hex.DecodeString(aux.JoinSigHex)
	if err != nil {
		return fmt.Errorf("%w: bad sig hex: %v", errAppliedRejected, err)
	}
	// POSITIONAL cross-check (review M8): the verified identity (node_id, name,
	// node_ident_pub) and the join proof (nonce, sig) must be baked AT THEIR columns,
	// not merely appear somewhere — otherwise a leader could alias name=<auxPub> so a
	// bare substring match passes while node_ident_pub holds a DIFFERENT key. We
	// reconstruct the exact contiguous VALUES fragments PlanClusterNodeUpsert bakes.
	// (Same package, so the format stays in sync.)
	if len(cmd.Body) != 1 {
		return fmt.Errorf("%w: expected exactly 1 baked statement, got %d", errAppliedRejected, len(cmd.Body))
	}
	body := cmd.Body[0].SQL
	idLit, e1 := LitText(aux.NodeID)
	nameLit, e2 := LitText(aux.Name)
	pubLit, e3 := LitText(aux.NodeIdentPub)
	nonceLit, e4 := LitText(aux.JoinNonce)
	sigLit, e5 := LitText(aux.JoinSigHex)
	if err := firstErr(e1, e2, e3, e4, e5); err != nil {
		return fmt.Errorf("%w: aux literal: %v", errAppliedRejected, err)
	}
	identFrag := "VALUES(" + idLit + ", " + nameLit + ", " + pubLit + ", "
	joinFrag := ", " + nonceLit + ", " + sigLit + ", NULL, "
	if !strings.Contains(body, identFrag) || !strings.Contains(body, joinFrag) {
		return fmt.Errorf("%w: aux identity/join not baked at their columns (splice)", errAppliedRejected)
	}
	msg := JoinSignBytes(aux.NodeID, aux.NodeIdentPub, aux.JoinNonce)
	if verr := auth.VerifySignature(aux.NodeIdentPub, msg, sig); verr != nil {
		return fmt.Errorf("%w: %v", errAppliedRejected, verr)
	}
	// Verified — exec the baked roster UPSERT. A SQL CONSTRAINT failure here
	// (UNIQUE(name) / CHECK / NOT NULL) is DETERMINISTIC across replicas (identical
	// baked SQL + schema), so treat it as a poison-skip too (errAppliedRejected),
	// NEVER a plain error — else the §3.7 retry loop turns it into a cluster-wide
	// panic on every log replay (review B2 / never-wedge). The leader should have
	// validated, but a forged/buggy proposal must not brick honest replicas.
	if err := (genericExecApplier{}).ApplyTx(tx, cmd); err != nil {
		return fmt.Errorf("%w: roster upsert: %v", errAppliedRejected, err)
	}
	return nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// clusterPhaseRank orders the phases for documentation/validation. The Apply-side
// guard is a phase-predecessor IN-set (deterministic CAS), not this rank; this is
// the canonical enumeration the predecessor sets are drawn from.
var clusterPhases = map[string]bool{
	"JOIN_VERIFIED_PENDING_VOTER": true,
	"CATCHING_UP":                 true,
	"VOTER":                       true,
	"VOTER_ADD_FAILED":            true,
	"DRAINING":                    true,
	"RETIRING":                    true,
}

// PlanClusterNodePhase renders OpClusterNodePhase (§8.1 half-success transitions):
// UPDATE phase with a baked `WHERE node_id=<lit> AND phase IN (<allowed
// predecessors>)` guard, so a stale ex-leader's disallowed transition is a
// deterministic RowsAffected==0 no-op. voterAddErr is written into voter_add_error
// (empty string => NULL — clears any prior error / catch_up_stalled hint). Rides
// genericExecApplier.
func PlanClusterNodePhase(nodeID, newPhase string, allowedPreds []string, voterAddErr string, now time.Time) (*Command, error) {
	if !clusterPhases[newPhase] {
		return nil, fmt.Errorf("cluster: plan node-phase: unknown target phase %q", newPhase)
	}
	if len(allowedPreds) == 0 {
		return nil, fmt.Errorf("cluster: plan node-phase: empty predecessor set")
	}
	nodeLit, err := LitText(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-phase: node literal: %w", err)
	}
	preds := make([]string, 0, len(allowedPreds))
	for _, p := range allowedPreds {
		if !clusterPhases[p] {
			return nil, fmt.Errorf("cluster: plan node-phase: unknown predecessor %q", p)
		}
		preds = append(preds, MustLitText(p))
	}
	errLit := LitNull()
	if voterAddErr != "" {
		errLit, err = LitText(voterAddErr)
		if err != nil {
			return nil, fmt.Errorf("cluster: plan node-phase: error literal: %w", err)
		}
	}
	sqlStr := `UPDATE cluster_nodes SET phase=` + MustLitText(newPhase) +
		`, voter_add_error=` + errLit +
		`, phase_changed_at=` + LitTime(now.UTC()) +
		` WHERE node_id=` + nodeLit +
		` AND phase IN (` + strings.Join(preds, ",") + `)`
	// C1 §D-2: bump the monotone roster_generation IN THE SAME Apply txn, change-gated
	// (changes()>0) so a stale-leader predecessor-guard no-op does not inflate it.
	// C1 §D-2: roster gen bump (change-gated). C3 §D-G: ALSO bump topology_generation, but ONLY on a
	// mesh-ENTER (→CATCHING_UP) — within-mesh transitions (→VOTER/→DRAINING/→RETIRING) don't change the
	// rendered nats.conf, so they must not flap the cluster to DEGRADED. Appended LAST so its change()
	// gate reads rosterGenBumpStmt's RowsAffected (1 iff the phase UPDATE matched) → truth-preserving.
	stmts := []Statement{Stmt(sqlStr), rosterGenBumpStmt(now)}
	if newPhase == "CATCHING_UP" {
		stmts = append(stmts, topologyGenBumpStmt(now))
	}
	return NewCommand(OpClusterNodePhase, stmts...), nil
}

// PlanClusterNodeRemove renders OpClusterNodeRemove (§8.1 removal order): DELETE a
// roster row ONLY when it has walked through removal (RETIRING) or failed to join
// (VOTER_ADD_FAILED). A live VOTER is structurally undeletable here. Idempotent
// (RowsAffected==0 re-delete). Rides genericExecApplier.
func PlanClusterNodeRemove(nodeID string, now time.Time) (*Command, error) {
	nodeLit, err := LitText(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-remove: node literal: %w", err)
	}
	// C3-m1: split the remove by phase so topology_generation bumps ONLY for a RETIRING node (a mesh
	// member LEAVING). A VOTER_ADD_FAILED row was NEVER in the route mesh (∉ topoMeshPhases), so its
	// removal must NOT flap the cluster to DEGRADED. roster_generation still bumps for EITHER (any
	// membership change). A node is exactly one of the two phases, so at most one DELETE matches.
	delFailed := `DELETE FROM cluster_nodes WHERE node_id=` + nodeLit + ` AND phase='VOTER_ADD_FAILED'`
	delRetiring := `DELETE FROM cluster_nodes WHERE node_id=` + nodeLit + ` AND phase='RETIRING'`
	// review F1: a CATCHING_UP node (a STAGED NONVOTER left behind by a failed/aborted grow) DID enter
	// the route mesh (∈ topoMeshPhases, rendered into the nats.conf routes), so cleaning it up is a mesh
	// LEAVE and must bump BOTH generations — exactly like a RETIRING voter, unlike a never-meshed
	// VOTER_ADD_FAILED. A node is exactly one phase, so at most one DELETE matches; the gen bumps are
	// change-gated off the immediately-preceding statement, so the other phases are unaffected.
	delCatchingUp := `DELETE FROM cluster_nodes WHERE node_id=` + nodeLit + ` AND phase='CATCHING_UP'`
	return NewCommand(OpClusterNodeRemove,
		Stmt(delFailed), rosterGenBumpStmt(now), // non-mesh cleanup: roster bumps, topology does not
		Stmt(delRetiring), rosterGenBumpStmt(now), topologyGenBumpStmt(now), // mesh leave: both bump
		Stmt(delCatchingUp), rosterGenBumpStmt(now), topologyGenBumpStmt(now), // staged nonvoter, mesh leave: both bump
	), nil
}

// PlanClusterNodePrune renders OpClusterNodeRemove for the G2 force-single abandoned set AND the
// membership-aware ghost passthrough (recovery node remove of a VOTER-not-in-committed-config ghost):
// DELETE the given roster rows UNCONDITIONALLY by node_id, regardless of phase. Unlike
// PlanClusterNodeRemove — which only finishes RETIRING/VOTER_ADD_FAILED/CATCHING_UP rows and
// STRUCTURALLY refuses a live VOTER (the roster/raft anti-fork guard) — this is the force-single /
// ghost-removal path: the CALLER has ALREADY proven the node is OUT of the committed raft config
// (force-single's RecoverToSelf* moved it, or RemoveNode read the LEADER-committed config), so deleting
// its roster row cannot fork quorum. It rides OpClusterNodeRemove's genericExecApplier (a DELETE is a
// DELETE — no new op code, so a mixed-version fleet cannot decode-poison it). Both gen counters bump (a
// prune is BOTH a roster change AND a mesh leave), change-gated off the DELETE's RowsAffected via the
// standard rosterGen→topologyGen chain, so a re-prune of already-absent rows is a RowsAffected==0 no-op
// that inflates neither counter (idempotent). ids must be non-empty and LitText-safe.
func PlanClusterNodePrune(ids []string, now time.Time) (*Command, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("cluster: plan node-prune: empty id set")
	}
	lits, err := LitTextAll(ids...)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-prune: id literal: %w", err)
	}
	del := `DELETE FROM cluster_nodes WHERE node_id IN (` + strings.Join(lits, ",") + `)`
	return NewCommand(OpClusterNodeRemove,
		Stmt(del), rosterGenBumpStmt(now), topologyGenBumpStmt(now),
	), nil
}

// PlanClusterBusNkeySet renders OpClusterBusNkeySet (C3 §D-F): record a broker's NATS bus nkey public
// key into cluster_nodes.bus_nkey_pub so the topology reconciler can render that peer into
// auth_callout.auth_users + the static users{} ACL. Leader-local fail-fast rejects a non-user-key
// (a garbage nkey would break every replica's `nats-server -t`). The UPDATE is guarded `bus_nkey_pub
// != <lit>` so an idempotent re-propose is a RowsAffected==0 no-op → the change-gated
// topology_generation bump does not advance. Rides genericExecApplier.
func PlanClusterBusNkeySet(nodeID, busNkeyPub string, now time.Time) (*Command, error) {
	if !nkeys.IsValidPublicUserKey(busNkeyPub) {
		return nil, fmt.Errorf("cluster: plan bus-nkey-set: %q is not a valid NATS user public key", busNkeyPub)
	}
	nodeLit, err := LitText(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan bus-nkey-set: node literal: %w", err)
	}
	nkLit, err := LitText(busNkeyPub)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan bus-nkey-set: nkey literal: %w", err)
	}
	// C3-m2: only a MESH node's bus nkey affects the rendered conf — gate the UPDATE on a mesh phase so
	// filling a PENDING/VOTER_ADD_FAILED node's bus nkey does not spuriously bump topology_generation.
	// (A joiner sets its nkey once it reaches CATCHING_UP; the mesh-ENTER already gates the render.)
	sqlStr := `UPDATE cluster_nodes SET bus_nkey_pub=` + nkLit +
		` WHERE node_id=` + nodeLit + ` AND bus_nkey_pub != ` + nkLit +
		` AND phase IN ('CATCHING_UP','VOTER','DRAINING','RETIRING')`
	return NewCommand(OpClusterBusNkeySet, Stmt(sqlStr), topologyGenBumpStmt(now)), nil
}

// MetaKeyForceSingle is the cluster_meta key the offline force-single tool sets and
// PlanClearForceSingle clears. Shared so the writer (internal/clusteroffline) and the
// clearer agree on the key.
const MetaKeyForceSingle = "force_single_active"

// PlanClearForceSingle renders OpClusterMetaClear: DELETE the replicated
// force_single_active marker (review M3 — clear it once HA is restored so a regrown
// cluster stops reporting exit 3). Fixed-key DELETE, no external input.
func PlanClearForceSingle() (*Command, error) {
	keyLit := MustLitText(MetaKeyForceSingle)
	return NewCommand(OpClusterMetaClear, Stmt(`DELETE FROM cluster_meta WHERE key=`+keyLit)), nil
}

// MetaKeyUpgradeActive is the cluster_meta key a `cluster upgrade` roll sets for its whole duration
// (External-review B2). Membership ops (join/retire) refuse while it is set, so a concurrent grow/retire
// cannot cross a rolling broker restart (which could turn "temporarily down one voter" into "permanently
// down one + restarting one"). A marker (not an operation) so it never self-blocks the roll's own reloads.
const MetaKeyUpgradeActive = "cluster_upgrade_active"

// PlanSetUpgradeActive / PlanClearUpgradeActive acquire/DELETE the roll-lock marker (mirrors force-single).
//
// R7b: acquiring also stamps the lock's LEASE (cluster_upgrade_lease) in the SAME command, i.e. the same
// FSM transaction. Marker-without-lease must be reachable ONLY as "acquired by a pre-R7b broker" — if a
// crash could interleave the two writes, a brand-new lock could land in the fail-closed never-expires
// bucket and reintroduce the exact permanent fence this batch removes.
//
// External review H1: the acquire is CONDITIONAL on no grow marker being held (grow and upgrade are ONE
// mutual-exclusion domain). The leader's preflight read cannot make check-then-write atomic — two
// concurrent callbacks can both pass the read before either commits — so the mutex is enforced HERE, at
// the replicated write: Raft serializes the two commands and the second one's guard sees the first's
// marker and no-ops. The caller read-backs the marker after Propose and refuses on a no-op acquire.
func PlanSetUpgradeActive(now time.Time) (*Command, error) {
	valLit := MustLitText(now.UTC().Format(time.RFC3339Nano))
	guard := `NOT ` + growMarkerExists()
	stmts := markerAcquireStmts(MetaKeyUpgradeActive, valLit, guard)
	stmts = append(stmts, leaseAcquireStmts(MetaKeyUpgradeLease, now.Add(LockLeaseTTL), guard)...)
	return NewCommand(OpClusterMetaSet, stmts...), nil
}

func PlanClearUpgradeActive() (*Command, error) {
	keyLit := MustLitText(MetaKeyUpgradeActive)
	return NewCommand(OpClusterMetaClear,
		Stmt(`DELETE FROM cluster_meta WHERE key=`+keyLit),
		// Guarded on the marker being gone — see leaseClearStmt. For the upgrade lock the marker DELETE is
		// unconditional, so this always fires; the guard keeps the two lock shapes textually identical.
		leaseClearStmt(MetaKeyUpgradeLease, MetaKeyUpgradeActive),
	), nil
}

// MetaKeyGrowActive is the cluster_meta key a `cluster add` grow sets for its whole duration (G4 §B). Its
// VALUE is the joiner's node_id, so the leader can refuse a lock for a DIFFERENT joiner while one grow is in
// flight (strict serialization) and a re-run of the SAME joiner recognizes the marker as its own (resume).
// Symmetric with the upgrade lock: a grow refuses while upgrade is set and vice-versa. A marker (not an
// operation) so it never self-blocks the grow's own OpKindJoin op (see the driveInFlightOperations carve-out).
const MetaKeyGrowActive = "cluster_grow_active"

// PlanSetGrowActive acquires the grow marker with the joiner id as the value (mirrors force-single/upgrade),
// and (R7b) stamps the lock's LEASE in the same command/transaction — see PlanSetUpgradeActive on why the
// two writes must be atomic.
//
// External review H1: the acquire is CONDITIONAL on (a) no upgrade marker held AND (b) no grow marker held
// by a DIFFERENT joiner. (a) enforces the grow⇄upgrade mutex at the replicated layer; (b) closes the same
// check-then-write race for grow-vs-grow (two `cluster add`s of different joiners both passing the leader's
// preflight). Idempotent for THIS joiner (resume). The caller read-backs growActiveJoiner after Propose and
// refuses on a no-op acquire.
func PlanSetGrowActive(joinerID string, now time.Time) (*Command, error) {
	valLit, err := LitText(joinerID)
	if err != nil {
		return nil, fmt.Errorf("cluster: grow-lock acquire: literal: %w", err)
	}
	otherGrow, err := growMarkerHeldByOther(joinerID)
	if err != nil {
		return nil, fmt.Errorf("cluster: grow-lock acquire: literal: %w", err)
	}
	guard := `NOT ` + upgradeMarkerExists() + ` AND NOT ` + otherGrow
	stmts := markerAcquireStmts(MetaKeyGrowActive, valLit, guard)
	stmts = append(stmts, leaseAcquireStmts(MetaKeyGrowLease, now.Add(LockLeaseTTL), guard)...)
	return NewCommand(OpClusterMetaSet, stmts...), nil
}

// PlanClearGrowActive DELETEs the grow marker (release). External review M1: when joinerID is non-empty the
// clear is CONDITIONAL on the marker's value matching it (`AND value=<joiner>`), so a completed/aborted re-run of
// `cluster add <joinerA>` can NEVER wipe a DIFFERENT grow's in-flight marker (<joinerB>) — that would drop the
// strict-serialize mutex mid-grow. An empty joinerID is a break-glass unconditional clear (a stale marker whose
// joiner the caller no longer knows); the release path always passes the joiner it is releasing.
func PlanClearGrowActive(joinerID string) (*Command, error) {
	keyLit := MustLitText(MetaKeyGrowActive)
	where := `key=` + keyLit
	if joinerID != "" {
		lit, err := LitText(joinerID)
		if err != nil {
			return nil, fmt.Errorf("cluster: grow-lock release: literal: %w", err)
		}
		where += ` AND value=` + lit
	}
	return NewCommand(OpClusterMetaClear,
		Stmt(`DELETE FROM cluster_meta WHERE `+where),
		// R7b: the lease dies with its marker — but ONLY if the marker actually went away. When the
		// value predicate above no-ops (this release belongs to a DIFFERENT joiner), the guard inside
		// leaseClearStmt no-ops too, so joiner B's live lease survives joiner A's release. Without that
		// guard, a same-transaction unconditional lease DELETE would leave B's marker leaseless and the
		// reaper would treat it as pre-R7b (never expire) — silently re-creating the permanent fence.
		leaseClearStmt(MetaKeyGrowLease, MetaKeyGrowActive),
	), nil
}

// MetaKeyForceSingleEpoch is the cluster_meta key holding the per-recovery epoch token. The
// post-recover split-brain detector compares it across the cluster-health broadcast: hearing a peer
// advertise force_single_active with a DIFFERENT epoch means two divergent single-voter timelines.
const MetaKeyForceSingleEpoch = "force_single_epoch"

// PlanSetForceSingle renders OpClusterMetaSet: UPSERT the replicated force_single_active marker
// (value = now, RFC3339Nano) — the ONLINE force-single counterpart to the offline raiseForceSingleMarker.
// The leader bakes the timestamp as a literal, so every replica applies the identical value (deterministic).
func PlanSetForceSingle(now time.Time) (*Command, error) {
	keyLit := MustLitText(MetaKeyForceSingle)
	valLit := MustLitText(now.UTC().Format(time.RFC3339Nano))
	return NewCommand(OpClusterMetaSet, Stmt(
		`INSERT INTO cluster_meta(key, value) VALUES(`+keyLit+`, `+valLit+`) `+
			`ON CONFLICT(key) DO UPDATE SET value = excluded.value`)), nil
}

// PlanForceSingleEpoch renders OpClusterMetaSet: UPSERT the per-recovery epoch token (baked literal).
func PlanForceSingleEpoch(epoch string) (*Command, error) {
	keyLit := MustLitText(MetaKeyForceSingleEpoch)
	valLit := MustLitText(epoch)
	return NewCommand(OpClusterMetaSet, Stmt(
		`INSERT INTO cluster_meta(key, value) VALUES(`+keyLit+`, `+valLit+`) `+
			`ON CONFLICT(key) DO UPDATE SET value = excluded.value`)), nil
}

// PlanClusterCertRotate renders OpClusterCertRotate (§15 RF3 / external review F2):
// rotate node's stable tunnel cert pin. It moves the current cert_fp to cert_fp_prev
// (a deterministic SQL self-reference — every replica's row holds the same current
// value), installs newFP as cert_fp, and sets cert_fp_valid_until so the D6 cert-pin
// VerifyConnection accepts the previous pin until the window closes. NOT a join
// (no PoP); just a cert update on an existing roster row. genericExecApplier.
func PlanClusterCertRotate(nodeID, newFP string, validUntil, now time.Time) (*Command, error) {
	nodeLit, err := LitText(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan cert-rotate: node literal: %w", err)
	}
	fpLit, err := LitText(newFP)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan cert-rotate: fp literal: %w", err)
	}
	sqlStr := `UPDATE cluster_nodes SET cert_fp_prev=cert_fp, cert_fp=` + fpLit +
		`, cert_fp_valid_until=` + LitTime(validUntil.UTC()) +
		` WHERE node_id=` + nodeLit + ` AND cert_fp != ` + fpLit
	// C1 §D-2: CertFP is a signed-roster field → bump roster_generation atomically;
	// change-gated so a cert_fp-unchanged (RowsAffected==0) no-op does not advance it.
	return NewCommand(OpClusterCertRotate, Stmt(sqlStr), rosterGenBumpStmt(now)), nil
}

// PlanClusterNodeReaddr renders OpClusterNodeReaddr (v0.4.2 phase-fluidity): rewrite a node's
// cluster_nodes.raft_addr — the replicated copy of its raft advertise address. The raft
// Configuration self-address is rewritten separately by node.AddVoter (an online in-place
// address update on an existing voter); this op keeps the SQLite roster copy in sync so status,
// the :7400 liveness probe, and a future force-single read the same address. Change-gated
// (RowsAffected==0 no-op on re-propose) and — critically — NO generation bump: raft_addr is
// absent from the agent-facing roster SELECT (cluster_roster.go) and the rendered nats.conf, so
// a raft_addr change must not spuriously recompute the roster or push agents. The
// loopback/unspecified policy lives at the admin/CLI boundary (SetRaftAddr + --allow-loopback);
// here we only structurally validate host:port + LitText safety. genericExecApplier.
func PlanClusterNodeReaddr(nodeID, newRaftAddr string) (*Command, error) {
	if _, _, err := net.SplitHostPort(newRaftAddr); err != nil {
		return nil, fmt.Errorf("cluster: plan node-readdr: invalid raft addr %q: %w", newRaftAddr, err)
	}
	nodeLit, err := LitText(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-readdr: node literal: %w", err)
	}
	addrLit, err := LitText(newRaftAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-readdr: addr literal: %w", err)
	}
	sqlStr := `UPDATE cluster_nodes SET raft_addr=` + addrLit +
		` WHERE node_id=` + nodeLit + ` AND raft_addr != ` + addrLit
	return NewCommand(OpClusterNodeReaddr, Stmt(sqlStr)), nil
}

// PlanClusterNodeRoute renders OpClusterNodeRoute (v0.4.2 phase-fluidity): rewrite a node's
// cluster_nodes.nats_route — the NATS route-mesh advertise (the twin of raft_addr for the :6222
// cluster{} routes). UNLIKE raft_addr, nats_route IS rendered into nats.conf
// (topology_reconcile.go builds natscluster.Broker.RouteURL from it), so the change bumps
// topology_generation (change-gated, so an unchanged re-propose does not advance it) to drive a
// re-render + reload. URL-format validation lives at the admin/CLI boundary; here we LitText-guard
// only. genericExecApplier.
// ValidateBareNatsRoute is the SINGLE shared grammar for a broker NATS route (review F6/R4), called by
// BOTH the CLI/admin validator (broker.validateNatsRoute) and the replicated planner
// (PlanClusterNodeRoute) so the two admission boundaries cannot drift. A route must be a BARE
// nats://host:port authority: the nats scheme, a host, a numeric 1-65535 port, and NO userinfo (a URL
// credential would be distributed to every agent via the signed roster), path (incl. a lone "/"),
// query, or fragment. Errors are REDACTED — never echo the route (it may carry a secret).
func ValidateBareNatsRoute(route string) error {
	u, err := url.Parse(route)
	if err != nil {
		return fmt.Errorf("nats route is not a parseable URL")
	}
	if u.Scheme != "nats" {
		return fmt.Errorf("nats route must use the nats:// scheme")
	}
	if u.User != nil {
		return fmt.Errorf("nats route must not carry credentials (userinfo) — broker routes authenticate by mTLS")
	}
	if u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("nats route must be a bare nats://host:port authority (no path/query/fragment)")
	}
	if u.Hostname() == "" {
		return fmt.Errorf("nats route has no host")
	}
	if p, perr := strconv.Atoi(u.Port()); perr != nil || p < 1 || p > 65535 {
		return fmt.Errorf("nats route port must be a number 1-65535")
	}
	return nil
}

func PlanClusterNodeRoute(nodeID, newNatsRoute string, now time.Time) (*Command, error) {
	// review m9: structurally validate the route (a nats:// URL with a host), mirroring readdr's
	// host:port check — a non-CLI caller bypassing clusterdrain.validateNatsRoute must not
	// deterministically commit a garbage route that renders a broken cluster{} routes URL on every
	// replica.
	// SECURITY (review F6/R4): nats_route is folded into the account-signed agent roster body, so it
	// must be a BARE nats://host:port authority. This replicated planner is its OWN trust boundary (a
	// non-CLI caller can reach it without broker.validateNatsRoute), so it enforces the SAME shared
	// grammar — including the 1-65535 numeric port range and a rejection of ANY path (incl. "/").
	if err := ValidateBareNatsRoute(newNatsRoute); err != nil {
		return nil, fmt.Errorf("cluster: plan node-route: %w", err)
	}
	nodeLit, err := LitText(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-route: node literal: %w", err)
	}
	routeLit, err := LitText(newNatsRoute)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-route: route literal: %w", err)
	}
	sqlStr := `UPDATE cluster_nodes SET nats_route=` + routeLit +
		` WHERE node_id=` + nodeLit + ` AND nats_route != ` + routeLit
	// nats_route is BOTH rendered into nats.conf (topology) AND part of the account-signed roster
	// body (review m4 — it is in cluster_roster.go's SELECT), so bump BOTH generations. Change-gated:
	// an unchanged re-propose (RowsAffected==0) advances neither.
	return NewCommand(OpClusterNodeRoute, Stmt(sqlStr), topologyGenBumpStmt(now), rosterGenBumpStmt(now)), nil
}

// PlanClusterDrainSet renders OpClusterDrainSet (§8.3): set or clear a broker's
// drain deadline as a cluster_meta KV ('draining:'+node_id). A non-nil deadline
// UPSERTs with a MONOTONIC guard (later deadline wins; the value is the deadline's
// UnixNano as text, CAST-compared so a stale ex-leader's earlier deadline is a
// no-op). A nil deadline DELETEs the row (clear). Rides genericExecApplier.
func PlanClusterDrainSet(nodeID string, deadline *time.Time) (*Command, error) {
	keyLit, err := LitText("draining:" + nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan drain-set: key literal: %w", err)
	}
	if deadline == nil {
		return NewCommand(OpClusterDrainSet, Stmt(`DELETE FROM cluster_meta WHERE key=`+keyLit)), nil
	}
	valLit := LitInt(deadline.UTC().UnixNano())
	sqlStr := `INSERT INTO cluster_meta(key, value) VALUES(` + keyLit + `, ` + valLit + `) ` +
		`ON CONFLICT(key) DO UPDATE SET value=excluded.value ` +
		`WHERE CAST(excluded.value AS INTEGER) > CAST(cluster_meta.value AS INTEGER)`
	return NewCommand(OpClusterDrainSet, Stmt(sqlStr)), nil
}
