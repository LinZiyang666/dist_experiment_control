package cluster

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

func JoinSignBytes(nodeID, nodeIdentPub, joinNonce string) []byte {
	return state.JoinSignBytes(nodeID, nodeIdentPub, joinNonce)
}

type clusterNodeUpsertAux = state.ClusterNodeUpsertAux

type ClusterNodeUpsertInput = state.ClusterNodeUpsertInput

func PlanClusterNodeUpsert(in ClusterNodeUpsertInput) (*Command, error) {
	cmd, err := state.PlanClusterNodeUpsert(in)
	return (*Command)(cmd), err
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
	return state.FirstErr(errs...)
}

func PlanClusterNodePhase(nodeID, newPhase string, allowedPreds []string, voterAddErr string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterNodePhase(nodeID, newPhase, allowedPreds, voterAddErr, now)
	return (*Command)(cmd), err
}

func PlanClusterNodeRemove(nodeID string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterNodeRemove(nodeID, now)
	return (*Command)(cmd), err
}

func PlanClusterNodePrune(ids []string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterNodePrune(ids, now)
	return (*Command)(cmd), err
}

func PlanClusterBusNkeySet(nodeID, busNkeyPub string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterBusNkeySet(nodeID, busNkeyPub, now)
	return (*Command)(cmd), err
}

const MetaKeyForceSingle = state.MetaKeyForceSingle

func PlanClearForceSingle() (*Command, error) {
	cmd, err := state.PlanClearForceSingle()
	return (*Command)(cmd), err
}

const MetaKeyUpgradeActive = state.MetaKeyUpgradeActive

func PlanSetUpgradeActive(now time.Time) (*Command, error) {
	cmd, err := state.PlanSetUpgradeActive(now)
	return (*Command)(cmd), err
}

func UpgradeMarkerValue(db *sql.DB) string {
	return state.UpgradeMarkerValue(db)
}

func UpgradeMarkerStamp(now time.Time) string {
	return state.UpgradeMarkerStamp(now)
}

func PlanClearUpgradeActive() (*Command, error) {
	cmd, err := state.PlanClearUpgradeActive()
	return (*Command)(cmd), err
}

const MetaKeyGrowActive = state.MetaKeyGrowActive

func PlanSetGrowActive(joinerID string, now time.Time) (*Command, error) {
	cmd, err := state.PlanSetGrowActive(joinerID, now)
	return (*Command)(cmd), err
}

func PlanClearGrowActive(joinerID string) (*Command, error) {
	cmd, err := state.PlanClearGrowActive(joinerID)
	return (*Command)(cmd), err
}

const MetaKeyForceSingleEpoch = state.MetaKeyForceSingleEpoch

func PlanSetForceSingle(now time.Time) (*Command, error) {
	cmd, err := state.PlanSetForceSingle(now)
	return (*Command)(cmd), err
}

func PlanForceSingleEpoch(epoch string) (*Command, error) {
	cmd, err := state.PlanForceSingleEpoch(epoch)
	return (*Command)(cmd), err
}

func PlanClusterCertRotate(nodeID, newFP string, validUntil, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterCertRotate(nodeID, newFP, validUntil, now)
	return (*Command)(cmd), err
}

func PlanClusterNodeReaddr(nodeID, newRaftAddr string) (*Command, error) {
	cmd, err := state.PlanClusterNodeReaddr(nodeID, newRaftAddr)
	return (*Command)(cmd), err
}

func ValidateBareNatsRoute(route string) error {
	return state.ValidateBareNatsRoute(route)
}

func PlanClusterNodeRoute(nodeID, newNatsRoute string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterNodeRoute(nodeID, newNatsRoute, now)
	return (*Command)(cmd), err
}

func PlanClusterDrainSet(nodeID string, deadline *time.Time) (*Command, error) {
	cmd, err := state.PlanClusterDrainSet(nodeID, deadline)
	return (*Command)(cmd), err
}
