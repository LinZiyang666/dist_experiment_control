package broker

import (
	"github.com/LinZiyang666/tether/internal/brokercontrol"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM

type ClusterReader = brokercontrol.ClusterReader
type AuditPublisherConfig = brokercontrol.AuditPublisherConfig
type AuditPublisher = brokercontrol.AuditPublisher

func NewAuditPublisher(cfg AuditPublisherConfig) *AuditPublisher {
	return brokercontrol.NewAuditPublisher(cfg)
}

type ReplicaReport = brokercontrol.ReplicaReport

func replicaReportAllAtLeast(r ReplicaReport, target int) bool {
	return brokercontrol.ReplicaReportAllAtLeast(r, target)
}
func replicaReportSurvivesRemoval(r ReplicaReport, server string, target int) bool {
	return brokercontrol.ReplicaReportSurvivesRemoval(r, server, target)
}
