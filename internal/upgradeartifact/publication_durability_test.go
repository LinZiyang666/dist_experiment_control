package upgradeartifact

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradelock"
)

// origin: S8 internal review rv-txn F06
func TestGrantPublicationReceiptRequiresFileAndDirectorySync(t *testing.T) {
	for _, failAt := range []int{0, 7, 8} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			f := newPublicationFixture(t)
			ctx := WithPublicationPort(t.Context(), f.port)
			calls := 0
			r, err := publishWithReceipt(ctx, f.grant, bytes.NewReader(f.source), bytes.NewReader(f.target), func(file *os.File) error {
				calls++
				if !f.port.lease.held {
					t.Fatal("publication receipt escaped the reservation")
				}
				if calls == failAt {
					return errors.New("receipt sync interrupted")
				}
				return file.Sync()
			}, true)
			if failAt != 0 {
				if err == nil || r != (Receipt{}) || calls != failAt {
					t.Fatal("incomplete receipt durability reported success")
				}
			} else if err != nil || calls != 8 {
				t.Fatalf("receipt did not sync both boundaries: %d %v", calls, err)
			}
			reg, err := upgradelock.RegistrationFor(f.grant.Handle.Key)
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := ReadPublication(reg.ControlDir, f.grant)
			if failAt == 7 {
				if readErr == nil {
					t.Fatal("unsynced receipt became readable authority")
				}
			} else if readErr != nil || got.Grant != f.grant {
				t.Fatalf("durable or post-rename readback lost exact grant: %v", readErr)
			}
			if f.port.lease.held {
				t.Fatal("receipt failure leaked the publication lease")
			}
		})
	}
}
