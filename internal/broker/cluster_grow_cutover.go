package broker

import (
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/proto"
)

// Bind cutover IO to the broker's current cluster and shared loopback monitor.
// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func brokerGrowCutover(b *Broker) brokerruntime.GrowCutover {
	c := brokerruntime.GrowCutover{
		SelfID: b.selfID, NatsConfPath: b.cfg.NatsConfPath, NatsServerBin: b.cfg.NatsServerBin,
		ClusterDataDir: b.cfg.ClusterDataDir, MonitorListen: topoMonitorListen, ReloadTimeout: topoReloadTimeout,
		ProbeClient: topoProbeClient, Now: b.cfg.Now, Logger: b.cfg.Logger, SysEvent: b.pubSysEvent,
		TopologyInputs: func() (natsconf.Inputs, bool) { return b.buildTopologyInputs(1) },
	}
	if b.cl != nil {
		c.Node = b.cl.node
	}
	return c
}

func (b *Broker) performGrowCutover(req *proto.ClusterGrowReq) *proto.ClusterGrowResp {
	return brokerruntime.PerformGrowCutover(brokerGrowCutover(b), req)
}
func (b *Broker) renderClusteredCutoverConf(own *natsconf.Ownership) (string, error) {
	return brokerruntime.RenderClusteredCutoverConf(brokerGrowCutover(b), own)
}
func (b *Broker) growCutoverThisEpochEvidence(storeDir, epoch string) bool {
	return brokerruntime.GrowCutoverThisEpochEvidence(brokerGrowCutover(b), storeDir, epoch)
}
func (b *Broker) markGrowCutoverEpoch(epoch string) {
	brokerruntime.MarkGrowCutoverEpoch(brokerGrowCutover(b), epoch)
}

type cutoverAction = brokerruntime.CutoverAction

const (
	cutoverAlreadyClustered = brokerruntime.CutoverAlreadyClustered
	cutoverSIGKILLToRevive  = brokerruntime.CutoverSIGKILLToRevive
	cutoverAwaitRevival     = brokerruntime.CutoverAwaitRevival
)

func cutoverRestartDecision(clustered, reachable bool) cutoverAction {
	return brokerruntime.CutoverRestartDecision(clustered, reachable)
}

func (b *Broker) moveAsideJetStreamStore(storeDir, epoch string, ack bool) (string, error) {
	return brokerruntime.MoveAsideJetStreamStore(brokerGrowCutover(b), storeDir, epoch, ack)
}
func (b *Broker) hardRestartNatsServer() error {
	return brokerruntime.HardRestartNatsServer(brokerGrowCutover(b))
}
