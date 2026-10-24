package xfercourier

import (
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/schema"
)

// The real history/auth owners consume kernel BOOTTIME deadlines and the flat
// continuation identity written by the broker. The isolated courier unit fixture
// deliberately uses a small synthetic clock and a legacy audit payload; neither
// can reach those real ports. Keep the same failed outcome and lost-ACK oracle,
// but capture a genuine canonical continuation before attempting publication.
// origin: S10 internal review NO-SKIP-AS-PASS F3
func newHistoryCourierFixture(t *testing.T) *courierFixture {
	t.Helper()
	f := newCourierFixture(t, "failed")
	if err := f.owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	now, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	f.now.Store(now)
	var row schema.AuditTransfer
	if err := json.Unmarshal(f.record.Bytes, &row); err != nil {
		t.Fatal(err)
	}
	id := f.config.Identity
	row.AgentInstance, row.EntryNonce, row.IntentDigest, row.ObjectVersion = id.AgentInstance, id.EntryNonce, id.IntentDigest, id.ObjectVersion
	row.TerminalID = f.receipt.TerminalID
	f.record, err = schema.CanonicalTransferAudit(row)
	if err != nil {
		t.Fatal(err)
	}
	f.receipt.CanonicalDigest, f.receipt.ReqID = f.record.Digest, f.record.ReqID
	owner, err := New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	f.owner = owner
	if err := BindCanonical(f.owner, f.record); err != nil {
		t.Fatal(err)
	}
	return f
}
