package upgradetx

import "strings"

// OrchestrationKey identifies the daemon's separate bookkeeping journals.
// Registered component endpoints never accept these logical keys: their exact
// registered Key remains the image's only Current slot. A host/parent census
// describes several independently owned children, rather than claiming their
// live resources were moved into the coordinator process.
// origin: hot-update S8 plan §3.1 / §5.1; S8 internal review cluster-roll F4
func OrchestrationKey(key Key) bool {
	return (key.Component == ComponentBroker || key.Component == ComponentAgent) && (strings.HasSuffix(key.ConfiguredNID, "@roll") || strings.HasSuffix(key.ConfiguredNID, "@host"))
}
