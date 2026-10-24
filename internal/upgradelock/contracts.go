// Package upgradelock owns domain reservations across image replacement.
// The long lease and short journal guard use different inodes. An inherited
// lease adopts the original OFD; borrowing never unlocks the parent's lease.
// origin: hot-update S8 plan §2.2 / §5.1 / P00
package upgradelock

import (
	"context"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type Lease interface {
	FD() uintptr
	DomainID() string
	Close() error
}

// P04 supplies the real functions. Only acquisition may wait on the caller's
// context; the local adoption and short file guard remain context-free.
type AcquireFunc = func(ctx context.Context, key upgradetx.Key, parentToken string) (Lease, error)
type AdoptFunc = func(fd uintptr, key upgradetx.Key, parentToken string) (Lease, error)
type WithGuardFunc = func(key upgradetx.Key, fn func() error) error
