// Read-only image policy retains the closed vocabulary and legacy reader gates.
// origin: hot-update S8 plan §12.1 / P14.
package agentupgrade

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
)

func PreflightDeferrals(checks []agentimage.BusyCheck) []agentimage.DeferredError {
	// origin: hot-update S5 plan §7.3 / T37. Multiple legacy owners share
	// one reason, and receipts retain the frozen reason order.
	byReason := make(map[string]string)
	for _, check := range checks {
		if reason, detail, busy := check(); busy {
			if existing := byReason[reason]; existing != "" {
				detail = existing + "; " + detail
			}
			byReason[reason] = detail
		}
	}
	var reasons []agentimage.DeferredError
	for _, reason := range agentimage.DeferredReasons {
		if detail, busy := byReason[reason]; busy {
			reasons = append(reasons, agentimage.DeferredError{Reason: reason, Detail: detail})
		}
	}
	return reasons
}

func DeferredText(logger *slog.Logger, reasons []agentimage.DeferredError) string {
	parts := make([]string, 0, len(reasons))
	codes := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, reason.Reason+" ("+reason.Detail+")")
		codes = append(codes, reason.Reason)
	}
	logger.Warn("agent: upgrade deferred", "reasons", codes)
	return "deferred: " + strings.Join(parts, "; ")
}

// CutFailureClass, RedispatchExec and OwnsImage moved verbatim to
// internal/agentupgradeport/image_dispatch.go (S8 FIX-DESCRIBE structural move).

// origin: hot-update S4 plan §5.2 / T30. A live summary includes settled
// request records and uncertain-clock revocations, not just live task PIDs.
func CheckCandidateABI(logger *slog.Logger, empty func() bool, required func() []string, candidate agentimage.Capabilities) error {
	// origin: hot-update S3 plan §2.1 Q1. No handoff descriptors or secret
	// may enter a binary that does not know how to consume them.
	// origin: hot-update S8 plan §6.1; S8 review rv-n1 F5. A numeric ABI
	// ceiling is not a reader declaration: it refused a real adjacent reader
	// and admitted nothing the declared epochs did not. CheckDomains decides
	// every positive ABI from both readers' explicit epoch sets.
	if candidate.ABI < 0 || (candidate.ABI == 0 && !empty()) {
		return fmt.Errorf("%w: candidate image-abi v%d, agent supports v%d", agentimage.ErrABIUnsupported, candidate.ABI, proto.ImageABIVersion)
	}
	if candidate.ABI == 0 {
		logger.Warn("agent: candidate has no image ABI; empty image will use bare exec")
		return nil
	}
	domains := required()
	return agentimage.CheckDomains(candidate, domains)
}
