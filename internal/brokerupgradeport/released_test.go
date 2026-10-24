package brokerupgradeport

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review P13-COHORT F2; hot-update S8 plan §4.2 / M06
func TestImportedCensusKeepsRevokedOriginalsAsProvenTerminals(t *testing.T) {
	f := newCutFixture(t)
	view := f.journal.view
	row := upgradetx.Resource{Domain: upgradetx.DomainBrokerRuntime, ID: "tunnel:4100", Origin: "allocation:lab/agent@0", Owner: f.owner, Generation: 1, State: "active", Custody: "source"}
	view.Historical.Forward.Scope.Expected = append(view.Historical.Forward.Scope.Expected, row)
	stamp := upgradetx.Stamp{Attempt: f.grant.Handle, Producer: f.owner}
	port := *f.runtime.port.Load()
	port.Released = func(domain, id, origin string) bool {
		return domain == string(row.Domain) && id == row.ID && origin == row.Origin
	}
	f.runtime.port.Store(&port)
	if got := f.runtime.releasedRows(stamp, nil, view, true); len(got) != 0 {
		t.Fatal("the source signed its own target retirement")
	}
	f.runtime.host.Inherited = &upgradecut.State{Attempt: f.grant.Handle}
	census, err := f.runtime.census(stamp, nil, view, true)
	if err != nil {
		t.Fatal(err)
	}
	got := census.Resources
	if len(got) != 1 || got[0].Origin != row.Origin || got[0].Generation != stamp.Attempt.Generation || !upgradetx.ResourceReleased(got[0]) {
		t.Fatalf("original revocation vanished from the imported census: %+v", got)
	}
	if got := f.runtime.releasedRows(stamp, []upgradetx.Resource{row}, view, true); len(got) != 1 || got[0] != row {
		t.Fatal("an existing owner was replaced by terminal evidence")
	}
	port.Released = func(string, string, string) bool { return false }
	if got := f.runtime.releasedRows(stamp, nil, view, true); len(got) != 0 {
		t.Fatal("a missing original was released without a positive terminal proof")
	}
	port.Released = func(string, string, string) bool { return true }
	f.runtime.host.Inherited.Attempt.TxID = "another-cut"
	if got := f.runtime.releasedRows(stamp, nil, view, true); len(got) != 0 {
		t.Fatal("a different imported cut signed the original retirement")
	}
	f.runtime.host.Inherited.Attempt = f.grant.Handle
	port.Released = nil
	if got := f.runtime.releasedRows(stamp, nil, view, true); len(got) != 0 {
		t.Fatal("absence became retirement without a positive observer")
	}
}
