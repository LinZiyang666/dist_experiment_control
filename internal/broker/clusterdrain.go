package broker

import (
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
type QuorumProjection = brokercontrol.QuorumProjection

func ProjectQuorum(currentVoters int, retire bool) QuorumProjection {
	return brokercontrol.ProjectQuorum(currentVoters, retire)
}

type ErrQuorumConfirmRequired = brokercontrol.ErrQuorumConfirmRequired

var ErrNoMigrationTarget = brokercontrol.ErrNoMigrationTarget

type ErrLeadershipTransferred = brokercontrol.ErrLeadershipTransferred
type ErrRemoveOwnsResources = brokercontrol.ErrRemoveOwnsResources

type opCapability = brokercontrol.OpCapability

func checkVotersSupportOps(opName string, cfg []cluster.ServerInfo, self string, health map[string]proto.ClusterHealthResp, cap opCapability) error {
	return brokercontrol.CheckVotersSupportOps(opName, cfg, self, health, cap)
}
func assertAllVotersSupportSessionCreatorOps(a *ClusterAdmin, opName string) error {
	return brokercontrol.AssertAllVotersSupportSessionCreatorOps(a, opName)
}

var sessionCreatorCapability = brokercontrol.SessionCreatorCapability

func validateAdvertiseHostPort(addr string, allowLoopback bool) error {
	return brokercontrol.ValidateAdvertiseHostPort(addr, allowLoopback)
}
func validateNatsRoute(route string, allowLoopback bool) error {
	return brokercontrol.ValidateNatsRoute(route, allowLoopback)
}
func isLoopbackAdvertiseHostPort(addr string) bool {
	return brokercontrol.IsLoopbackAdvertiseHostPort(addr)
}
func isLoopbackNatsRoute(route string) bool { return brokercontrol.IsLoopbackNatsRoute(route) }

type ErrRebuildOffExposes = brokercontrol.ErrRebuildOffExposes
