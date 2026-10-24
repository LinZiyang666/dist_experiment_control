package upgradecut

import (
	"os"

	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Take moves the original lease OFD out of the auxiliary close list. The callback
// must remove a returned FD from that list, even when it returns an error. P17
// calls this before closeAuxiliary/Thaw and after comparing the state with the
// actual image and descriptor table. Adoption must never re-open/re-flock it.
//
// A nonnil lease with an error retains custody for salvage: the caller must keep
// it for pristine repack or close it during terminal cleanup. Closing here could
// release the reservation while a partially imported generation is still live.
// origin: hot-update S8 plan §5.1 / §6.1 / §8.1 / M08
func Take(state State, takeFD func(uint32) (uintptr, error)) (upgradelock.Lease, error) {
	if err := validateState(state); err != nil {
		return nil, err
	}
	if takeFD == nil {
		return nil, invalidAdjunct()
	}
	fd, takeErr := takeFD(state.LockFD)
	// Auxiliary indexes start at zero, but OS descriptors 0..2 are never lease
	// custody. Reject sentinels/overflow before turning a number into an owner.
	if fd < 3 || fd > 1<<31-1 {
		return nil, invalidAdjunct()
	}
	if takeErr != nil {
		return retainTakenFD(fd, state.Attempt.Key.DomainID), custodyError()
	}
	lease, err := upgradelock.Adopt(fd, state.Attempt.Key, state.ParentToken)
	if err != nil {
		if lease == nil {
			lease = retainTakenFD(fd, state.Attempt.Key.DomainID)
		}
		return lease, custodyError()
	}
	if lease == nil {
		return retainTakenFD(fd, state.Attempt.Key.DomainID), custodyError()
	}
	return lease, nil
}

func custodyError() error {
	// Callback diagnostics can contain private metadata; preserve custody and
	// classify the uncertainty without copying arbitrary input into errors.
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "upgrade lease custody requires recovery", Accepted: true}
}

// takenFD is the closer for a partial Take that could not be adopted. It has no
// execution authority, only descriptor custody. os.File owns this FD only on a
// refused path: constructing a second wrapper on success would leave a finalizer
// able to close the adopted lease later. Close never issues LOCK_UN, preserving
// a parent's reservation if this FD is a borrowed duplicate of the parent's OFD.
type takenFD struct {
	file     *os.File
	domainID string
}

func retainTakenFD(fd uintptr, domainID string) upgradelock.Lease {
	return &takenFD{file: os.NewFile(fd, "upgrade-lease-custody"), domainID: domainID}
}

func (lease *takenFD) FD() uintptr      { return lease.file.Fd() }
func (lease *takenFD) DomainID() string { return lease.domainID }
func (lease *takenFD) Close() error {
	// Even cleanup failures retain the frozen error classification; a raw OS
	// error must not escape as an unmapped result from an accepted cut.
	// origin: hot-update S8 plan §2.2 / §7.1
	if err := lease.file.Close(); err != nil {
		return custodyError()
	}
	return nil
}
