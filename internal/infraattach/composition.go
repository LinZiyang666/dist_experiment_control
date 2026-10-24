package infraattach

import (
	"github.com/LinZiyang666/tether/internal/infracompose"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// ReadSeal exposes the same gate that owns every broker producer; composition
// cannot silently substitute another recovery owner for readiness's short fence.
// origin: hot-update S9 plan §3.2 / §4.3 / P22
func (r brokerRecovery) ReadSeal(p c.Plan, d c.Deadline, stop c.Stop) (c.AssetSeal, error) {
	return r.gate.ReadSeal(p, d, stop)
}

// bindService runs after bindRecovery in the shared cold/hot connection root.
// Ordinary reconnect retains this graph and its original worker/ACK identities.
// Legacy explicit Dependencies remain useful for narrowly scoped leaf fixtures;
// deployments use Composition to construct the real registered graph here.
func (o *Broker) bindService() error {
	if o.cfg.Infra == nil || o.cfg.Infra.Service == nil || o.service != nil {
		return nil
	}
	config := o.cfg.Infra.Service
	if !o.Configured() || o.Recovery() == nil || config.Plan == nil || config.Capability == nil || o.cfg.Infra.Pending == nil {
		return c.ErrContractStub
	}
	deps := config.Dependencies
	if config.Composition != nil {
		recovery, ok := o.Recovery().(infracompose.Recovery)
		if !ok {
			return c.ErrIncomplete
		}
		graph, err := infracompose.New(*config.Composition, recovery)
		if err != nil {
			return err
		}
		deps = graph.Dependencies
	}
	deps.Recovery = o.Recovery()
	service, err := infracompose.Service(deps)
	if err != nil {
		return err
	}
	o.service = &serviceRuntime{Service: service}
	return nil
}
