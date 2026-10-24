package transferharness

import "strings"

// BoundaryCell is the frozen S7 tier/direction/window denominator. Both the
// real fixture and matrix planners enumerate it, so a new window cannot be
// added to the fixture while silently remaining outside every selected unit.
// origin: hot-update S7 plan §10.2 / lead L17 / T35 / T36 / T46
type BoundaryCell struct{ Tier, Direction, Window string }

// BoundaryCells returns a fresh complete set, keeping callers from mutating
// another fixture or planner's denominator.
func BoundaryCells() []BoundaryCell {
	var cells []BoundaryCell
	for _, tier := range []string{"a", "b"} {
		for _, direction := range []string{"push", "pull"} {
			for _, window := range []string{"prepare-accepted", "source-pinned", "sender-chunk-pending",
				"receiver-write", "Sync-pending", "synced-prefix", "grant-reply-lost", "pre-primitive",
				"post-primitive-pre-outcome", "terminal-pre-history-ACK", "cleanup-pending"} {
				if tier == "a" && window == "sender-chunk-pending" {
					window = "inline-send-unconfirmed"
				}
				cells = append(cells, BoundaryCell{tier, direction, window})
			}
		}
	}
	return cells
}

// BoundarySelectors partitions only these measured S7 matrices. All five
// rounds still execute the original top-level Test and its direct leak asserts.
// Each selector owns one existing cell; neither timeout nor cut window changes.
// Other packages and explicit prefiltered commands keep their existing owners.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F5 / hot-update S7 plan lead L17
func BoundarySelectors(pkg, name, tags string) []string {
	if strings.TrimSuffix(pkg, "/...") != "./test/hotupdate" || tags != "hotupdate_integration" {
		return nil
	}
	prefixes := []string{""}
	switch name {
	case "TestTransferAgentExecAcrossEveryBoundary":
	case "TestTransferBrokerExecKeepsTrackerBeforeReaper":
		prefixes = []string{"single", "sole-voter", "quorum"}
	case "TestTransferCarrierAndJSRestartReceiptsAreDistinct":
		prefixes = []string{"carrier", "data-connection", "same-store-js"}
	default:
		return nil
	}
	var selectors []string
	for _, prefix := range prefixes {
		for _, cell := range BoundaryCells() {
			if name == "TestTransferCarrierAndJSRestartReceiptsAreDistinct" &&
				(prefix != "same-store-js" && cell.Window != "synced-prefix" || prefix == "data-connection" && cell.Tier == "a") {
				continue
			}
			selector := "^" + name + "$/^rounds$/^round-[0-4]$"
			if prefix != "" {
				selector += "/^" + prefix + "$"
			}
			selectors = append(selectors, selector+"/^tier-"+cell.Tier+"$/^"+cell.Direction+"$/^"+cell.Window+"$")
		}
	}
	return selectors
}
