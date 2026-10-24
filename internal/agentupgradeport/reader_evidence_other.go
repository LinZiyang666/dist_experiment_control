//go:build !linux

package agentupgradeport

import "errors"

// Without a change-time identity witness, every gate retains full verification.
// origin: hot-update S8 plan §6.1 / §10; S8 internal review FREEZE-WINDOW R6
type evidenceStamp struct{}

func readEvidenceStamp(string) (evidenceStamp, error) {
	return evidenceStamp{}, errors.New("reader evidence metadata is unavailable")
}
