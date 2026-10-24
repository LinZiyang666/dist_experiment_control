//go:build !linux && !darwin

package transferpath

import (
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
)

// Refuse before acquiring files where POSIX renameat/linkat and directory fsync
// cannot provide the atomic receiver's effect and durability receipts.
// origin: hot-update S7 plan §3 / T10–T11 / S7-GATES lead decision 4
func NewAtomicReceiver(*ValidatedPath, string, bool) (AtomicReceiver, error) {
	return nil, handoff.ErrUnsupported
}

func ThawAtomic(AtomicReceiver, controlbudget.Deadline) error    { return handoff.ErrUnsupported }
func DiscardAtomic(AtomicReceiver, controlbudget.Deadline) error { return handoff.ErrUnsupported }
