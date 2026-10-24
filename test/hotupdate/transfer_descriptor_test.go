//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"golang.org/x/sys/unix"
)

func transferCtlExitAllowed(cell transferCell, done transferharness.Receipt, released bool) error {
	if released || transferCtlMayComplete(cell, done) {
		return nil
	}
	return fmt.Errorf("INCOMPLETE: original ctl exited before %s cut", cell.window)
}

// originalInodes compares every original OFD while its producer is parked and
// remembers which file each one names. Equal inodes are never identity
// evidence; they only find later descriptors that must be that same OFD.
// origin: S7 FIX-CUT deterministic original-FD sampling / hot-update S7 plan §10.4 / M09
func (c *transferCollector) originalInodes(row x.Observation) (map[string][2]uint64, error) {
	if err := c.compareOFDs(row); err != nil {
		return nil, err
	}
	inodes := make(map[string][2]uint64)
	err := c.eachOriginalFD(row, func(key string, fd int, same bool, ino [2]uint64) error {
		if same {
			inodes[key] = ino
		}
		return nil
	})
	return inodes, err
}

// checkNoReopen accepts an original that was retired after its release, but
// rejects any descriptor on the original file that is another OFD.
func (c *transferCollector) checkNoReopen(row x.Observation, inodes map[string][2]uint64) error {
	return c.eachOriginalFD(row, func(key string, fd int, same bool, ino [2]uint64) error {
		if want, ok := inodes[key]; ok && ino == want && !same {
			return errors.New("original transfer file was reopened as another open file description (kernel comparison)")
		}
		return nil
	})
}

func (c *transferCollector) eachOriginalFD(row x.Observation, visit func(key string, fd int, same bool, ino [2]uint64) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", row.PID))
	if err != nil {
		// /proc can deny fd access while a just-exited process awaits reaping.
		// An absent path alone is not retirement proof: join the exact original
		// producer before accepting ENOENT/EACCES, so no live survivor is hidden.
		// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
		if transferProducerRetired(c, row, err) {
			// The producer exited after its release: nothing remains open.
			return nil
		}
		return err
	}
	for _, ref := range []x.ResourceRef{row.Source, row.Tmp, row.Parent} {
		if ref.ResourceID == "" {
			continue
		}
		key := fmt.Sprintf("%d/%s/%s", row.PID, ref.ResourceID, ref.Name)
		w := c.witnesses[key]
		if w == nil {
			return errors.New("original transfer OFD custody missing")
		}
		for _, f := range fds {
			fd, err := strconv.Atoi(f.Name())
			if err != nil {
				return err
			}
			var st, again unix.Stat_t
			link := fmt.Sprintf("/proc/%d/fd/%d", row.PID, fd)
			if unix.Stat(link, &st) != nil {
				continue // closed between listing and stat
			}
			same := w.Compare(row.PID, row.ProcessStart, fd) == nil
			// A descriptor closed during the comparison is a retirement, never a
			// reopen; only one still naming the same file after kcmp counts.
			if !same && (unix.Stat(link, &again) != nil || again.Dev != st.Dev || again.Ino != st.Ino) {
				continue
			}
			if err := visit(key, fd, same, [2]uint64{st.Dev, st.Ino}); err != nil {
				return err
			}
		}
	}
	return nil
}

func transferProducerRetired(c *transferCollector, row x.Observation, procErr error) bool {
	if (!errors.Is(procErr, os.ErrNotExist) && !errors.Is(procErr, os.ErrPermission)) || c.process == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(c.t.Context(), 5*time.Second)
	defer cancel()
	done, waitErr := c.process.Wait(ctx)
	return waitErr == nil && done.NativeRC == 0 && done.PID == row.PID && done.ProcessStart == row.ProcessStart && done.BootID == row.BootID
}
