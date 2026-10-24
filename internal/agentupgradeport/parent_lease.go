package agentupgradeport

import (
	"context"
	"os"
	"sync"

	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// BorrowRuntimeLease duplicates custody of the already held OFD. Adopt checks
// its key, inode, declaration and kernel lock without opening/flocking it again.
// Closing a failed reverse's duplicate must never release the pending forward.
// The caller holds the commit owner's lease mutex through this operation.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §5.1
func BorrowRuntimeLease(lease upgradelock.Lease, key upgradetx.Key, parent string) (upgradelock.Lease, error) {
	if lease == nil || lease.DomainID() != key.DomainID {
		return nil, deferral(ReasonDomainLeaseHeld)
	}
	fd, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, deferral(ReasonDomainLeaseHeld)
	}
	borrowed, err := upgradelock.Adopt(uintptr(fd), key, parent)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return borrowed, nil
}

// ParentLeaseAccess gives concurrent original-parent couriers one fence for
// lease observation and release. Component boot never releases a parent lease;
// two resumed drivers may still query and release that same host concurrently.
// Closing only its final owner preserves the inherited OFD until host release.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
func ParentLeaseAccess(lease *upgradelock.Lease, mu *sync.Mutex) (func() bool, func() error) {
	has := func() bool { mu.Lock(); defer mu.Unlock(); return *lease != nil }
	release := func() error {
		mu.Lock()
		defer mu.Unlock()
		if *lease == nil {
			return nil
		}
		err := (*lease).Close()
		if err == nil {
			*lease = nil
		}
		return err
	}
	return has, release
}

// AcquireRuntimeLease carries a pending forward's reservation into its adjacent
// reverse. Reopening the same inode would self-contend while completion retains
// the imported lease. The immutable cut/owner/generation fence prevents a new
// forward or an unrelated component from borrowing that execution authority.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §5.1
func AcquireRuntimeLease(ctx context.Context, key upgradetx.Key, journal upgradetx.Store, inherited *upgradecut.State, lease *upgradelock.Lease, mu *sync.Mutex) (upgradelock.Lease, error) {
	if journal == nil || lease == nil || mu == nil || ctx == nil || ctx.Err() != nil {
		return nil, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: ReasonDomainLeaseHeld}
	}
	mu.Lock()
	defer mu.Unlock()
	view, err := journal.Load(key, "")
	if err != nil {
		return nil, err
	}
	a, cut := current(view.Historical), inherited
	parent := view.Historical.Grant.ParentTxID
	if *lease == nil {
		return upgradelock.Acquire(ctx, key, parent)
	}
	if cut == nil || a.Direction != upgradetx.DirectionReverse || a.Handle.Key != cut.Attempt.Key ||
		a.SourceGeneration != cut.Attempt.Generation || a.Handle.Generation != cut.Attempt.Generation+1 ||
		a.SourceSHA != cut.TargetSHA || view.Historical.Grant.Issuer != cut.Owner {
		return nil, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: ReasonDomainLeaseHeld}
	}
	return BorrowRuntimeLease(*lease, key, parent)
}

// BindInheritedLease connects the final dependency copy, before publication,
// to the runtime's cut owner. An unresolved target still holds its reservation
// when its watchdog exports a reverse; opening the inode again would self-lock.
// Only that accepted reverse may borrow the inherited OFD. A completed target
// has released it and acquires a new reservation by the usual path.
// origin: S8 internal review activation-and-route-migration F10; S8 plan §5.1
func BindInheritedLease(runtime upgradeport.RuntimePort, inherited *upgradelock.Lease, mu *sync.Mutex) {
	p, ok := runtime.(*runtimePort)
	if !ok || p.host.Lease != nil || p.host.Inherited == nil {
		return
	}
	p.host.Lease = func(ctx context.Context, key upgradetx.Key) (upgradelock.Lease, error) {
		if ctx == nil || ctx.Err() != nil || key != p.host.Key || p.host.Journal == nil {
			return nil, deferral(ReasonDomainLeaseHeld)
		}
		view, err := p.host.Journal.Load(key, "")
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		if *inherited == nil {
			return upgradelock.Acquire(ctx, key, view.Historical.Grant.ParentTxID)
		}
		a, cut := current(view.Historical), p.host.Inherited
		if (*inherited).DomainID() != key.DomainID || !upgradetx.SameCut(view.Historical.Forward.Handle, cut.Attempt) ||
			a.Direction != upgradetx.DirectionReverse || a.Handle.TxID != cut.Attempt.TxID || a.Handle.Generation != cut.Attempt.Generation+1 ||
			a.SourceSHA != cut.TargetSHA || a.TargetSHA != cut.SourceSHA {
			return nil, deferral(ReasonDomainLeaseHeld)
		}
		duplicate, err := dupLease(*inherited)
		if err != nil {
			return nil, err
		}
		return inheritedLeaseDuplicate{duplicate, key.DomainID}, nil
	}
}

// Closing a borrowed descriptor must never LOCK_UN its original owner's OFD.
type inheritedLeaseDuplicate struct {
	*os.File
	domain string
}

func (l inheritedLeaseDuplicate) DomainID() string { return l.domain }
func (l inheritedLeaseDuplicate) FD() uintptr      { return l.Fd() }
