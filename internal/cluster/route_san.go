package cluster

import "github.com/LinZiyang666/tether/internal/cluster/routecert"

// The leaf owns this responsibility; the facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

func RouteCertSANMatches(secretsDir, routeHost string) error {
	return routecert.RouteCertSANMatches(secretsDir, routeHost)
}
