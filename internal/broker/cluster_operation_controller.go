package broker

import (
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM

const opTimelineCap = brokercontrol.OpTimelineCap
const opCatchupTimeout = brokercontrol.OpCatchupTimeout
const opMaxAttempts = brokercontrol.OpMaxAttempts
const opTopoConvergeTimeout = brokercontrol.OpTopoConvergeTimeout
const opRehomeConvergeTimeout = brokercontrol.OpRehomeConvergeTimeout
const opCatchupMaxWindow = brokercontrol.OpCatchupMaxWindow

type substrate = brokercontrol.Substrate

func opFrozenByUpgradeLock(upgradeLockHeld bool, kind string) bool {
	return brokercontrol.OpFrozenByUpgradeLock(upgradeLockHeld, kind)
}

const OpBlockedCatchupDeadlineMsg = brokercontrol.OpBlockedCatchupDeadlineMsg

func retireStepFailed(a *ClusterAdmin, op *cluster.Operation, what string, stepErr error) {
	brokercontrol.RetireStepFailed(a, op, what, stepErr)
}

type opAttemptKey = brokercontrol.OpAttemptKey

const jsGateExpiryReserve = brokercontrol.JsGateExpiryReserve
