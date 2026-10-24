package cli

import (
	"github.com/LinZiyang666/tether/internal/xferclient"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// WithTransferWireExitClasses installs the finite transfer supplement to the command's
// broker-code classifier. Phase-sensitive sent uncertainty remains in xferclient.
// origin: hot-update S7 plan §7 / T34 / INTEG-D
func WithTransferWireExitClasses(classes map[string]int) map[string]int {
	for code, class := range transferWireExitClasses {
		classes[code] = class
	}
	return classes
}

var transferWireExitClasses = map[string]int{
	// origin: hot-update S7 plan §7 / T34. Main invocation sent-unknown
	// classification stays in P25; these are the finite wire refusal classes.
	// The accepted-result class 74 (result_unknown, terminal_conflict,
	// commit_inflight, durability_unknown, reconcile_required) is presentation
	// only: the engine's phase-aware xferclient.ExitCode assigns it after a send,
	// and a broker-code table entry would let a pre-accept refusal claim it
	// (TestExitUnsettledIsPresentationOnly / TestBrokerCodeExitClassesNeverReserved).
	// origin: INTEG-D cmd/tether race suite
	xfercontract.CodeIntentConflict:        exitUsage,
	xfercontract.CodePermitExpired:         exitUsage,
	xfercontract.CodeOwnerRecovering:       exitTransient,
	xfercontract.CodeObjectConflict:        exitUsage,
	xfercontract.CodeObjectVersionMismatch: exitUsage,
	xfercontract.CodeSourceChanged:         exitUsage,
	xfercontract.CodeStorageHorizon:        exitTransient,
	xfercontract.CodePayloadUnsupported:    exitUsage,
	xfercontract.CodeCapacityExceeded:      exitTransient,
	xfercontract.CodeCanceled:              exitUsage,
}

func ClassifyTransferError(err error) (string, int) {
	return xferclient.ErrorCode(err), xferclient.ExitCode(err)
}
