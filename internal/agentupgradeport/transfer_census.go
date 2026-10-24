// transfer_census.go names the agent's capable-transfer responsibility in
// the S8 census. A live transfer is retained work a cut carries (the worker
// entry, its retained result and its consumer-cleanup duties), so the
// original scope must enumerate it by identity; before this census existed a
// single live transfer made every describe, preflight and cut refuse with
// domain_census_unavailable, and reading it as zero would have let a grant
// hide it (M06/M14).
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census)
package agentupgradeport

import (
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferworker"
)

// TransferEntry is one transfer_worker responsibility the agent holds, as the
// worker's own registry names it (xferworker.Census). An entry has its
// business key and the EntryNonce of its slot; Cleanup instead names one
// retained consumer-cleanup duty and leaves the entry fields empty.
type TransferEntry struct {
	SID, TransferID, EntryNonce string
	Settled                     bool
	Cleanup                     string
}

// TransferCensus names every responsibility of the agent's bound
// capable-transfer worker by its own registry identity: each entry (settled
// or not) and each retained consumer-cleanup duty. It is the worker the image
// cut freezes, read without a fence: xferworker.Census takes only the
// registry mutex, so a parked byte actor cannot stall a describe or a
// preflight. No bound worker holds no capable transfer (legacy transfers stay
// a transfers_in_flight deferral of the busy checks); a worker the census
// cannot read refuses, never reads as zero.
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census)
func TransferCensus(worker x.WorkerManager) ([]TransferEntry, error) {
	if worker == nil {
		return nil, nil
	}
	entries, cleanup, ok := xferworker.Census(worker)
	if !ok {
		return nil, deferral(ReasonDomainCensus)
	}
	out := make([]TransferEntry, 0, len(entries)+len(cleanup))
	for _, e := range entries {
		out = append(out, TransferEntry{SID: e.Identity.SID, TransferID: e.Identity.TransferID, EntryNonce: e.Identity.EntryNonce, Settled: e.Settled})
	}
	for _, duty := range cleanup {
		out = append(out, TransferEntry{Cleanup: duty})
	}
	return out, nil
}

// transferRows maps the worker census to transfer_worker rows. The row ID is
// the registry key (session and transfer), and Origin is the slot's
// EntryNonce, so a replacement slot with the same key can never stand in for
// the original across the cut (resourceIdentity compares Domain, ID and
// Origin). A settled entry is terminal and keeps only its retained result.
// Any incomplete identity refuses: an unnamed row cannot be followed.
func transferRows(entries []TransferEntry, owner upgradetx.Owner, generation uint64, custody string) ([]upgradetx.Resource, error) {
	rows := make([]upgradetx.Resource, 0, len(entries))
	for _, e := range entries {
		row := upgradetx.Resource{Domain: upgradetx.DomainTransferWorker, Owner: owner, Generation: generation}
		switch {
		case e.Cleanup != "" && e.SID == "" && e.TransferID == "" && e.EntryNonce == "" && !e.Settled:
			row.ID, row.Origin, row.State, row.Custody = "cleanup:"+e.Cleanup, "cleanup:"+e.Cleanup, "retained", "retained"
		case e.Cleanup == "" && e.SID != "" && e.TransferID != "" && e.EntryNonce != "":
			row.ID, row.Origin, row.State, row.Custody = "transfer:"+e.SID+"/"+e.TransferID, "entry:"+e.EntryNonce, "running", custody
			if e.Settled {
				row.State, row.Custody = "terminal", "retained"
			}
		default:
			return nil, deferral(ReasonDomainCensus)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// transfers reads the worker census for one sample. The enumerator is a
// required callback: a host that cannot say what it holds refuses. Under a
// frozen cut the census must name exactly the entries the sealed transfer
// image carries; a mismatch means the frozen and the live registries differ,
// and neither may then be signed.
func (p *runtimePort) transfers(bundle *agentimage.Bundle) ([]TransferEntry, error) {
	if p.host.Transfers == nil {
		return nil, deferral(ReasonDomainCensus)
	}
	entries, err := p.host.Transfers()
	if err != nil {
		return nil, deferredError(err)
	}
	if bundle == nil {
		return entries, nil
	}
	carried := 0
	for _, e := range entries {
		if e.Cleanup == "" {
			carried++
		}
	}
	frozen := 0
	if ref := bundle.Transfers; ref != nil {
		frozen = int(ref.Count)
	}
	if carried != frozen || bundle.Transfers == nil && len(entries) != 0 {
		return nil, deferral(ReasonDomainCensus)
	}
	return entries, nil
}
