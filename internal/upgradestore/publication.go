// Package upgradestore accepts exact-grant publication evidence at the sole
// journal writer. Artifact receipts locate immutable files; SHA strings are
// never paths. Closing rollback authority preserves the current boot pins.
// origin: S8 internal review rv-txn F06
package upgradestore

import (
	"os"
	"path/filepath"
	"reflect"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// BindPublication is an additive typed operation on a prepared grant. It is
// inert: no selector change or execution is authorized before Start's CAS.
func (s *Store) BindPublication(grant upgradetx.Grant, receipt upgradeartifact.Receipt) error {
	if err := upgradeartifact.Verify(receipt, grant); err != nil {
		return err
	}
	return s.locked(func(d *diskFile, e *diskEntry) error {
		for i := range e.Grants {
			p := &e.Grants[i]
			if p.Grant != grant {
				continue
			}
			if err := s.liveGrant(grant); err != nil {
				return err
			}
			pins := upgradeartifact.Pins(receipt)
			if len(p.Pins) != 0 && !reflect.DeepEqual(p.Pins, pins) {
				return failure(upgradetx.CodeRejected, "publication already bound", false)
			}
			p.Pins = pins
			if err := bump(e); err != nil {
				return err
			}
			return s.write(d, false)
		}
		return failure(upgradetx.CodeGrantUnknown, "publication grant is not prepared", false)
	})
}

// A stage publishes an immutable receipt under its exact grant in the control
// directory. Start may read that receipt after a lost stage reply; this never
// scans SHA slots or adopts another grant's otherwise identical publication.
func (s *Store) publicationPins(p preparedGrant) ([]string, error) {
	if len(p.Pins) != 0 {
		return p.Pins, nil
	}
	receipt, err := upgradeartifact.ReadPublication(filepath.Dir(s.options.GuardPath), p.Grant)
	if os.IsNotExist(err) {
		return nil, nil
	} // A bare store also hosts inert/non-image transactions.
	if err != nil {
		return nil, err
	}
	return upgradeartifact.Pins(receipt), nil
}

// RecordForwardRelease persists salvage proof in its actual direction before
// reverse activation. The expected reverse handle still owns the write CAS.
func (s *Store) RecordForwardRelease(expected upgradetx.Handle, receipt upgradetx.Receipt) (out upgradetx.Record, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		if err := currentCAS(e, expected); err != nil {
			return err
		}
		var err error
		out, err = upgradetx.RecordForwardRelease(*e.Current, receipt)
		if err != nil {
			return err
		}
		e.Current = &out
		if err = bump(e); err != nil {
			return err
		}
		return s.write(d, true)
	})
	if err != nil {
		out = upgradetx.Record{}
	}
	return
}
