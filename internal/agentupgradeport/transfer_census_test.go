package agentupgradeport

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// noTransfers is a host whose capable-transfer worker holds nothing.
func noTransfers() ([]TransferEntry, error) { return nil, nil }

// foreignWorker is a worker manager xferworker did not build.
type foreignWorker struct{ x.WorkerManager }

// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census).
// A live capable transfer is retained work the cut carries, so it must be
// named by identity — its registry key, and its EntryNonce as the origin a
// replacement slot cannot share — with the sample's owner and generation;
// a settled entry keeps only its retained result; a cleanup duty is retained.
// An incomplete identity, a missing enumerator, an unreadable worker, or a
// frozen image whose sealed entry count differs from the census all refuse
// (domain_census_unavailable) instead of describing a partial scope.
func TestTransferCensusNamesEveryRetainedEntry(t *testing.T) {
	owner := upgradetx.Owner{BootID: strings.Repeat("b", 32), PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("9", 32)}
	entries := []TransferEntry{{SID: "lab", TransferID: "live", EntryNonce: "n1"}, {SID: "lab", TransferID: "done", EntryNonce: "n2", Settled: true}, {Cleanup: "duty"}}
	rows, err := transferRows(entries, owner, 4, "target")
	if err != nil {
		t.Fatal(err)
	}
	want := []upgradetx.Resource{
		{Domain: upgradetx.DomainTransferWorker, ID: "transfer:lab/live", Origin: "entry:n1", Owner: owner, Generation: 4, State: "running", Custody: "target"},
		{Domain: upgradetx.DomainTransferWorker, ID: "transfer:lab/done", Origin: "entry:n2", Owner: owner, Generation: 4, State: "terminal", Custody: "retained"},
		{Domain: upgradetx.DomainTransferWorker, ID: "cleanup:duty", Origin: "cleanup:duty", Owner: owner, Generation: 4, State: "retained", Custody: "retained"},
	}
	if len(rows) != len(want) {
		t.Fatalf("census rows = %+v", rows)
	}
	for i := range want {
		if rows[i] != want[i] || upgradetx.ValidateResource(rows[i]) != nil {
			t.Fatalf("row %d = %+v, want %+v", i, rows[i], want[i])
		}
	}
	for name, bad := range map[string]TransferEntry{
		"no_slot":         {SID: "lab", TransferID: "t"},
		"no_transfer":     {SID: "lab", EntryNonce: "n"},
		"no_session":      {TransferID: "t", EntryNonce: "n"},
		"cleanup_and_key": {SID: "lab", TransferID: "t", EntryNonce: "n", Cleanup: "duty"},
		"settled_cleanup": {Cleanup: "duty", Settled: true},
		"empty":           {},
	} {
		if _, err := transferRows([]TransferEntry{bad}, owner, 4, "source"); !censusRefused(err) {
			t.Fatalf("%s: an incomplete identity became a row: %v", name, err)
		}
	}

	live := func() ([]TransferEntry, error) { return entries, nil }
	frozen := func(count uint32) *agentimage.Bundle {
		return &agentimage.Bundle{Transfers: &xfervalue.ImageRef{Schema: "transfer", ResourceID: "transfers", Count: count}}
	}
	for _, tc := range []struct {
		name   string
		census func() ([]TransferEntry, error)
		bundle *agentimage.Bundle
		ok     bool
	}{
		{"live_sample", live, nil, true},
		{"frozen_exact_count", live, frozen(2), true},
		{"frozen_cleanup_only", func() ([]TransferEntry, error) { return []TransferEntry{{Cleanup: "duty"}}, nil }, frozen(0), true},
		{"idle_without_image", noTransfers, &agentimage.Bundle{}, true},
		{"frozen_count_missing_entry", live, frozen(3), false},
		{"frozen_count_extra_entry", live, frozen(1), false},
		{"entries_without_frozen_image", live, &agentimage.Bundle{}, false},
		{"unreadable_worker", func() ([]TransferEntry, error) { return nil, errors.New("private worker failure") }, nil, false},
		{"missing_enumerator", nil, nil, false},
	} {
		port := NewRuntime(Host{Transfers: tc.census}).(*runtimePort)
		got, err := port.transfers(tc.bundle)
		if tc.ok != (err == nil) || !tc.ok && !censusRefused(err) {
			t.Fatalf("%s: transfers = %+v err=%v", tc.name, got, err)
		}
	}

	if got, err := TransferCensus(nil); got != nil || err != nil {
		t.Fatalf("no bound worker is not an empty census: %+v %v", got, err)
	}
	if _, err := TransferCensus(foreignWorker{}); !censusRefused(err) {
		t.Fatalf("an unreadable worker read as an empty census: %v", err)
	}
}

func censusRefused(err error) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == upgradetx.CodeDeferred && typed.Detail == ReasonDomainCensus
}
