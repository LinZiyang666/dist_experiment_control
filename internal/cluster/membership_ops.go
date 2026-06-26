package cluster

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
		in.JoinNonce, in.JoinSigHex)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan node-upsert: bake literal: %w", err)
	}
	nodeID, name, identPub, natsSrv := lits[0], lits[1], lits[2], lits[3]
	raftAddr, natsRoute, tunnelAddr := lits[4], lits[5], lits[6]
	publicHost, certFP, nonceLit, sigLit := lits[7], lits[8], lits[9], lits[10]
	tsLit := LitTime(in.Now.UTC())
	const pending = "'JOIN_VERIFIED_PENDING_VOTER'"

	sqlStr := `INSERT INTO cluster_nodes(node_id, name, node_ident_pub, nats_server_id, ` +
		`raft_addr, nats_route, tunnel_addr, public_host, cert_fp, cert_fp_prev, ` +
		`cert_fp_valid_until, phase, added_at, join_nonce, join_sig, voter_add_error, phase_changed_at) VALUES(` +
		nodeID + `, ` + name + `, ` + identPub + `, ` + natsSrv + `, ` +
		raftAddr + `, ` + natsRoute + `, ` + tunnelAddr + `, ` + publicHost + `, ` + certFP + `, NULL, ` +
		`NULL, ` + pending + `, ` + tsLit + `, ` + nonceLit + `, ` + sigLit + `, NULL, ` + tsLit + `) ` +
		`ON CONFLICT(node_id) DO UPDATE SET name=excluded.name, node_ident_pub=excluded.node_ident_pub, ` +
		`nats_server_id=excluded.nats_server_id, raft_addr=excluded.raft_addr, nats_route=excluded.nats_route, ` +
		`tunnel_addr=excluded.tunnel_addr, public_host=excluded.public_host, cert_fp=excluded.cert_fp, ` +
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
	return NewCommand(OpClusterNodeRemove,
		Stmt(delFailed), rosterGenBumpStmt(now), // non-mesh cleanup: roster bumps, topology does not
		Stmt(delRetiring), rosterGenBumpStmt(now), topologyGenBumpStmt(now), // mesh leave: both bump
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
