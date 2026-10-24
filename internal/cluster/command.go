package cluster

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

type OpType = state.OpType

const OpClusterMetaSet = state.OpClusterMetaSet

const OpSessionCreate = state.OpSessionCreate

const OpSessionTombstone = state.OpSessionTombstone

const OpSessionHardDelete = state.OpSessionHardDelete

const OpMemberJoin = state.OpMemberJoin

const OpNodeRegister = state.OpNodeRegister

const OpNodeEvict = state.OpNodeEvict

const OpSessionCreatorSet = state.OpSessionCreatorSet

const OpProcCreate = state.OpProcCreate

const OpProcMarkExited = state.OpProcMarkExited

const OpReconcileBatch = state.OpReconcileBatch

const OpPortAllocate = state.OpPortAllocate

const OpPortFree = state.OpPortFree

const OpPortRevoke = state.OpPortRevoke

const OpAgentProvision = state.OpAgentProvision

const OpProcGC = state.OpProcGC

const OpPortGC = state.OpPortGC

const OpAuditCheckpointSet = state.OpAuditCheckpointSet

const OpPortReassignHome = state.OpPortReassignHome

const OpClusterNodeUpsert = state.OpClusterNodeUpsert

const OpClusterNodePhase = state.OpClusterNodePhase

const OpClusterNodeRemove = state.OpClusterNodeRemove

const OpClusterDrainSet = state.OpClusterDrainSet

const OpClusterMetaClear = state.OpClusterMetaClear

const OpClusterBusNkeySet = state.OpClusterBusNkeySet

const OpClusterOpStart = state.OpClusterOpStart

const OpClusterOpTransition = state.OpClusterOpTransition

const OpClusterOpConfirm = state.OpClusterOpConfirm

const OpProxySetEnabled = state.OpProxySetEnabled

const OpProxySubCreate = state.OpProxySubCreate

const OpProxySubRevoke = state.OpProxySubRevoke

const OpProxyAllocate = state.OpProxyAllocate

const OpClusterSeedsPublish = state.OpClusterSeedsPublish

const OpClusterCertRotate = state.OpClusterCertRotate

const OpClusterNodeReaddr = state.OpClusterNodeReaddr

const OpClusterNodeRoute = state.OpClusterNodeRoute

const OpTransferAudit = state.OpTransferAudit

const OpAlertRaise = state.OpAlertRaise

const OpAlertClear = state.OpAlertClear

const OpAlertAck = state.OpAlertAck

var ErrLogTruncated = state.ErrLogTruncated

var ErrLogNonCommand = state.ErrLogNonCommand

var ErrLogPoison = state.ErrLogPoison

const metaTestKeyPrefix = state.MetaTestKeyPrefix

const commandVersion = state.EncodingVersion

func CommandVersion() int {
	return state.CommandVersion()
}

type Statement = state.Statement

type Command state.Command

func NewCommand(op OpType, body ...Statement) *Command {
	return (*Command)(state.NewCommand(op, body...))
}

func Stmt(sql string) Statement {
	return state.Stmt(sql)
}

func ExecCommand(db *sql.DB, cmd *Command) error { return state.ExecCommand(db, (*state.Command)(cmd)) }

func (c *Command) encode() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode command: %w", err)
	}
	return b, nil
}
func decodeCommand(data []byte) (*Command, error) {
	cmd, err := state.DecodeCommand(data)
	return (*Command)(cmd), err
}

func validReqID(s string) bool {
	return state.ValidReqID(s)
}

var knownOps = state.KnownOps

func KnownOpsForDocs() map[OpType]bool {
	return state.KnownOpsForDocs()
}

func HasPhaseFluidityOps() bool {
	return state.HasPhaseFluidityOps()
}

func HasSessionCreatorOps() bool {
	return state.HasSessionCreatorOps()
}
