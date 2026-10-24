// Package xfercapacity is one process's transfer capacity account. It shares
// the frozen whole-cut ceilings (public 16MiB, sealed 32MiB, non-replay
// 64MiB, shared replay 256MiB, handoff 64MiB / 16384 descriptors); there is no
// separate transfer pool. Reservation IDs are never reused, and an imported
// image's live reservations are adopted rather than charged twice.
// origin: hot-update S7 plan §8.3 / L8
package xfercapacity

import (
	"errors"
	"strconv"
	"sync"

	x "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// Limits are the process-wide ceilings of one account.
var Limits = x.Reservation{NonReplayBytes: 64 << 20, ReplayBytes: 256 << 20, SealedBytes: 32 << 20,
	PublicBytes: 16 << 20, HandoffBytes: 64 << 20, FDs: 16384, Resources: 16384}

// Account implements xfercontract.CapacityAccount.
type Account struct {
	mu     sync.Mutex
	prefix string
	next   uint64
	held   map[x.ReservationID]x.Reservation
	used   x.Reservation
}

// New returns an empty account whose IDs carry prefix, so two processes'
// reservations cannot collide after an image adoption.
func New(prefix string) *Account {
	return &Account{prefix: prefix, held: map[x.ReservationID]x.Reservation{}}
}

func (a *Account) Reserve(r x.Reservation) (x.ReservationID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.fits(r); err != nil {
		return "", err
	}
	var id x.ReservationID
	for {
		a.next++
		if a.next == 0 {
			return "", errors.New(x.CodeCapacityExceeded)
		}
		id = x.ReservationID(a.prefix + strconv.FormatUint(a.next, 10))
		if _, exists := a.held[id]; !exists {
			break
		}
	}
	a.held[id] = r
	a.add(r, 1)
	return id, nil
}

func (a *Account) Release(id x.ReservationID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.held[id]
	if !ok {
		return errors.New("xfercapacity: unknown transfer reservation")
	}
	delete(a.held, id)
	a.add(r, -1)
	return nil
}

// Adopt installs reservations retained by an imported image under their
// original IDs. A duplicate or an over-limit adoption refuses the whole set:
// the import must not silently double-count or lose a live reservation.
func (a *Account) Adopt(ids []x.ReservationID, rs []x.Reservation) error {
	if len(ids) != len(rs) {
		return errors.New("xfercapacity: adoption shape mismatch")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var total x.Reservation
	seen := map[x.ReservationID]bool{}
	for i, id := range ids {
		_, exists := a.held[id]
		if id == "" || exists || seen[id] {
			return errors.New("xfercapacity: duplicate adopted reservation")
		}
		if err := a.fits(rs[i]); err != nil {
			return err
		}
		seen[id] = true
		total = sum(total, rs[i], 1)
	}
	if err := a.fits(total); err != nil {
		return err
	}
	for i, id := range ids {
		a.held[id] = rs[i]
		a.add(rs[i], 1)
	}
	return nil
}

func (a *Account) fits(r x.Reservation) error {
	u := a.used
	if r.NonReplayBytes < 0 || r.ReplayBytes < 0 || r.SealedBytes < 0 || r.PublicBytes < 0 || r.HandoffBytes < 0 ||
		r.NonReplayBytes > Limits.NonReplayBytes-u.NonReplayBytes || r.ReplayBytes > Limits.ReplayBytes-u.ReplayBytes ||
		r.SealedBytes > Limits.SealedBytes-u.SealedBytes || r.PublicBytes > Limits.PublicBytes-u.PublicBytes ||
		r.HandoffBytes > Limits.HandoffBytes-u.HandoffBytes || r.FDs > Limits.FDs-u.FDs || r.Resources > Limits.Resources-u.Resources {
		return errors.New(x.CodeCapacityExceeded)
	}
	return nil
}

func (a *Account) add(r x.Reservation, sign int64) { a.used = sum(a.used, r, sign) }

func sum(a, r x.Reservation, sign int64) x.Reservation {
	a.NonReplayBytes += sign * r.NonReplayBytes
	a.ReplayBytes += sign * r.ReplayBytes
	a.SealedBytes += sign * r.SealedBytes
	a.PublicBytes += sign * r.PublicBytes
	a.HandoffBytes += sign * r.HandoffBytes
	a.FDs = uint32(int64(a.FDs) + sign*int64(r.FDs))
	a.Resources = uint32(int64(a.Resources) + sign*int64(r.Resources))
	return a
}
