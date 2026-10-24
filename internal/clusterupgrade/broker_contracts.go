// origin: hot-update S6 plan §2.1 / §12.1 P00
package clusterupgrade

// PlanScope selects the product target independently of Step.BrokerOnly, which
// still describes whether a host has an agent. Compute keeps whole-host default.
type PlanScope string

const (
	ScopeWholeHost    PlanScope = "whole-host"
	ScopeBrokerDaemon PlanScope = "broker-daemon"
)
