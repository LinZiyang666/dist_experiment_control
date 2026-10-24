package broker

import (
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/proto"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
func DeriveSeedEndpoints(prev []string, brokers []proto.RosterBroker) []string {
	return brokercontrol.DeriveSeedEndpoints(prev, brokers)
}

func seedSetEqual(a, b []string) bool { return brokercontrol.SeedSetEqual(a, b) }
func seedHostsMatchAnyBroker(endpoints []string, brokers []proto.RosterBroker) bool {
	return brokercontrol.SeedHostsMatchAnyBroker(endpoints, brokers)
}
