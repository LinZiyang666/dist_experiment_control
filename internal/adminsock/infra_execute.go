package adminsock

import (
	"encoding/hex"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
)

// ExecuteInfra is shared by authenticated local admin and the signed remote
// responder. Both select the same Service and enforce the same public selectors,
// result sanitization and completion evidence. The caller owns transport auth.
// origin: hot-update S9 plan §7.1 / WIRE-2 R10 / R14
func ExecuteInfra(b *InfraBindings, command proto.InfraCommand, deadline c.Deadline, stop c.Stop) *proto.InfraUpgradeResp {
	op := ""
	for _, candidate := range []string{OpInfraUpgrade, OpInfraStatus, OpInfraRecover, OpInfraProbe} {
		if infraOperation(candidate) == command.Op {
			op = candidate
		}
	}
	if err := proto.ValidateInfraCommand(command); err != nil {
		return infraAdminError(op, proto.InfraCode(err)).Infra
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return infraAdminError(op, proto.CodeInfraBudgetExceeded).Infra
	}
	select {
	case <-stop:
		return infraAdminError(op, proto.CodeInfraBudgetExceeded).Infra
	default:
	}
	if b == nil || b.Service == nil || b.Plan == nil || b.Capability == nil {
		return infraAdminError(op, proto.CodeInfraUnsupported).Infra
	}
	if command.Op == "probe" {
		capability := b.Capability()
		if capability == nil || capability.Version != proto.InfraRequestVersion || (!capability.NATS && !capability.Caddy) || (command.Component == "nats" && !capability.NATS) || (command.Component == "caddy" && !capability.Caddy) {
			return infraAdminError(op, proto.CodeInfraUnsupported).Infra
		}
		copy := *capability
		return &proto.InfraUpgradeResp{OK: true, Capability: &copy}
	}
	var err error
	var status c.Status
	txBytes, _ := hex.DecodeString(command.TxID)
	var tx c.ID
	copy(tx[:], txBytes)
	switch command.Op {
	case "status":
		status, err = b.Service.Status(tx, deadline)
	case "recover":
		status, err = b.Service.Resume(tx, deadline, stop)
	case "start":
		var plan c.Plan
		plan, err = b.Plan(command)
		if err == nil {
			err = validateInfraPlan(command, plan)
		}
		if err == nil {
			status, err = b.Service.Start(plan, deadline, stop)
		}
	}
	return infraAdminStatus(op, tx, status, err).Infra
}
