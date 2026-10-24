//go:build linux && amd64

package infralaunch

import (
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// origin: S9 internal review rv-infra F04
func TestLaunchAckRequiresReceiverOwnedLockDescription(t *testing.T) {
	r, selection, _ := launchFixture(t, stockBinary(t))
	// An identity observer alone cannot prove custody. Even a claimed stock
	// identity must be rejected when its target has no inherited descriptor.
	r.Expected.PID = 2147483000
	census, observed := emptyCensus(), r.Expected
	var denied error
	backend := fixtureBackend(t, r, selection, &census, &observed, &denied)
	if _, err := backend.Do(r, launchDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(selection.Executable.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	observed.ExeDevice, observed.ExeInode, observed.ExeSHA256 = uint64(stat.Dev), stat.Ino, r.Candidate.BinarySHA256
	ack := r
	ack.Op, ack.Sequence, ack.BeforeCensus, ack.Expected = c.LaunchAck, r.Sequence+1, census.Digest, observed
	if receipt, err := backend.Do(ack, launchDeadline(t), nil); err == nil || receipt.Applied {
		t.Fatal("stock identity without receiver OFD acknowledged")
	}
	// Refusal must leave the helper's reference intact for fenced recovery.
	assertStoreBusy(t, selection.Directory, selection.Locks)
}
