//go:build linux && amd64

package infraroot

import (
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
)

// origin: hot-update S10 lint-L1 / nilerr records.go journal recovery
func TestInstalledFencePreservesJournalReadFailures(t *testing.T) {
	for _, component := range []c.Component{c.NATS, c.Caddy} {
		for _, failure := range []string{"expired", "corrupt", "missing"} {
			t.Run(componentText(component)+"/"+failure, func(t *testing.T) {
				i, _, _ := installedFixture(t)
				if _, err := i.bootstrap(rootDeadline(t), nil); err != nil {
					t.Fatal(err)
				}
				root, err := i.worker()
				if err != nil {
					t.Fatal(err)
				}
				defer closeTestResource(t, root)
				records, ok := root.files.(*installedRecords)
				if !ok {
					t.Fatal("worker has no installed record owner")
				}
				fence := infrahost.StartupFence{Tx: c.ID{1}, Domain: records.policy.Domain, Step: 1, Component: component, Closed: true, Revision: 1}
				// Absence of a matching operation in a valid journal keeps the
				// fence closed; a broken read must not masquerade as that absence.
				if got, err := records.advanceFence(fence, rootDeadline(t)); err != nil || !reflect.DeepEqual(got, fence) {
					t.Fatalf("valid journal without an intent changed the fence: %+v %v", got, err)
				}
				d := rootDeadline(t)
				want := infrahost.ErrJournal
				switch failure {
				case "expired":
					d = 1
				case "corrupt":
					if err := i.files.Replace(stateDirectory+"/journal.json", []byte("corrupt journal"), 0600, i.gid); err != nil {
						t.Fatal(err)
					}
				case "missing":
					// Missing on-disk state must remain an error after bootstrap.
					if err := os.Remove(i.files.Path(stateDirectory + "/journal.json")); err != nil {
						t.Fatal(err)
					}
				}
				got, err := records.advanceFence(fence, d)
				if failure == "expired" {
					if !controlbudget.IsExpired(err) {
						t.Fatalf("journal deadline failure was discarded: %v", err)
					}
				} else if !errors.Is(err, want) {
					t.Fatalf("journal read failure was discarded: %v", err)
				}
				if !reflect.DeepEqual(got, fence) {
					t.Fatalf("failed journal read advanced the fence: %+v", got)
				}
			})
		}
	}
}
