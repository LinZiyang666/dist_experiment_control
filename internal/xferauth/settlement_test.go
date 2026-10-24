package xferauth

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// noTransferPorts answers the session census from a fixed table and records
// every transfer-port call. A session with no continuation transfer must be
// released by the census alone: any cancel/advance/update would mean the
// barrier invented work for a transfer that does not exist.
type noTransferPorts struct {
	*settlementPorts
	censuses []xfercontract.LedgerCensus
	err      error
	calls    []string
}

func (p *noTransferPorts) SessionCensus(string, controlbudget.Deadline) ([]xfercontract.LedgerCensus, error) {
	p.calls = append(p.calls, "census")
	return p.censuses, p.err
}
func (p *noTransferPorts) Cancel(xfercontract.Identity, xfercontract.CancelRequest, controlbudget.Deadline) (xfercontract.Status, error) {
	p.calls = append(p.calls, "cancel")
	return xfercontract.Status{}, nil
}
func (p *noTransferPorts) Advance(xfercontract.Identity, controlbudget.Deadline) (xfercontract.Status, error) {
	p.calls = append(p.calls, "advance")
	return xfercontract.Status{}, nil
}
func (p *noTransferPorts) Update(xfercontract.Key, uint64, xfercontract.LedgerMutation, controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	p.calls = append(p.calls, "update")
	return xfercontract.LedgerRow{}, errors.New("unexpected update")
}

// LQ1 keeps a session DELETING with rc75 only while a real continuation
// transfer is unsettled. A complete, ready census from every original origin
// that holds no row for the session is the "nothing to settle" answer and must
// release removal — rows of OTHER sessions included — while a missing,
// errored, incomplete or entry-only census still fails closed. This pins the
// barrier half of the no-transfer semantics; the broker half (no continuation
// capability at all ⇒ no barrier) is TestSessionRemovalWithoutContinuationRunsLegacyCascade.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31; S7 lead integration — make test on the merged tree
func TestSettlementWithoutSessionTransfersReleasesRemoval(t *testing.T) {
	_, _, fixture, _ := authFixture(t)
	origin := fixture.Origin
	second := xfercontract.Origin{BrokerID: "broker-2", InstanceID: "instance-2", BootID: "boot-2"}
	foreign := fixture.Rows[0]
	foreign.Identity.SID = "other"
	empty := func(o xfercontract.Origin) xfercontract.LedgerCensus {
		return xfercontract.LedgerCensus{Origin: o, Ready: true, Complete: true}
	}
	withForeign := empty(origin)
	withForeign.Rows = []xfercontract.LedgerRow{foreign}
	incomplete, notReady, coded, unrowed := empty(origin), empty(origin), empty(origin), empty(origin)
	incomplete.Complete = false
	notReady.Ready = false
	coded.Code = xfercontract.CodeOwnerRecovering
	unrowed.Entries = []xfercontract.Protection{{Identity: fixture.Rows[0].Identity}}

	cases := []struct {
		name     string
		censuses []xfercontract.LedgerCensus
		err      error
		reason   string // empty: removal is released
	}{
		{"one_origin_without_rows", []xfercontract.LedgerCensus{empty(origin)}, nil, ""},
		{"only_other_session_rows", []xfercontract.LedgerCensus{withForeign}, nil, ""},
		{"every_origin_without_rows", []xfercontract.LedgerCensus{empty(origin), empty(second)}, nil, ""},
		{"no_origin_listed", nil, nil, "origin_census_unavailable"},
		{"census_error", []xfercontract.LedgerCensus{empty(origin)}, errors.New("owner unreachable"), "origin_census_unavailable"},
		{"incomplete_origin", []xfercontract.LedgerCensus{empty(origin), incomplete}, nil, "origin_census_incomplete"},
		{"origin_not_ready", []xfercontract.LedgerCensus{notReady}, nil, "origin_census_incomplete"},
		{"origin_refused", []xfercontract.LedgerCensus{coded}, nil, "origin_census_incomplete"},
		{"durable_entry_without_row", []xfercontract.LedgerCensus{unrowed}, nil, "durable_entry_unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ports := &noTransferPorts{settlementPorts: &settlementPorts{}, censuses: tc.censuses, err: tc.err}
			s, err := NewSettlement(SettlementConfig{Owners: ports, Tracker: ports, Ledger: ports, Finalizer: ports})
			if err != nil {
				t.Fatal(err)
			}
			err = s.Settle("lab", false, authDeadline(t))
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("a session without continuation transfers stayed pending: %v", err)
				}
				// Exactly the initial census and the post-settlement re-read.
				if want := []string{"census", "census"}; !reflect.DeepEqual(ports.calls, want) {
					t.Fatalf("barrier touched transfer ports for a session without transfers: %v", ports.calls)
				}
				return
			}
			var pending *PendingError
			if !errors.As(err, &pending) || pending.ExitCode() != 75 || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("unprovable census released removal or lost its reason %q: %v", tc.reason, err)
			}
			for _, call := range ports.calls {
				if call != "census" {
					t.Fatalf("refused census still drove transfer work: %v", ports.calls)
				}
			}
		})
	}
}
