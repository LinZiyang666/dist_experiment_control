// Runtime census binds retained work and authority to the same cut.
// origin: hot-update S8 plan §4.2 / §6.2
package agentupgradeport

import (
	"sort"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// rows enumerates retained responsibility by identity: tasks, settled
// records, tombstones, revocation history, courier events and capable
// transfer entries. Flow/SS rows come from their actual typed owners; an
// unknown owner still refuses rather than reading as zero.
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census)
func (p *runtimePort) rows(bundle *agentimage.Bundle, owner upgradetx.Owner, generation uint64, custody string) ([]upgradetx.Resource, error) {
	var image execsession.Image
	var courier agentimage.CourierState
	if bundle != nil {
		image, courier = bundle.Runtime, bundle.Courier
	} else {
		var err error
		if image, courier, err = p.host.Snapshot(); err != nil {
			return nil, deferredError(err)
		}
	}
	entries, err := p.transfers(bundle)
	if err != nil {
		return nil, err
	}
	transfers, err := transferRows(entries, owner, generation, custody)
	if err != nil {
		return nil, err
	}
	domains, err := p.domainRows(bundle, owner, generation, custody)
	if err != nil {
		return nil, err
	}
	transfers = append(transfers, domains...)
	// origin: S8 internal review remaining-orchestration F2 (R2)
	// Authority is retained responsibility even when no process is running.
	// Sampling it on every path lets reverse carry the current creation fence;
	// an imported cut cannot silently adopt a replacement session's authority.
	if p.host.Incarnation == nil {
		return nil, deferral(ReasonIncarnationUnknown)
	}
	incarnation := p.host.Incarnation()
	if !hexValue(incarnation, 64) {
		return nil, deferral(ReasonIncarnationUnknown)
	}
	if inherited := p.host.Inherited; inherited != nil {
		for _, authority := range inherited.Authorities {
			if authority.SID == p.host.Key.SID && authority.Incarnation != incarnation {
				return nil, deferral(ReasonIncarnationUnknown)
			}
		}
	}
	transfers = append(transfers, upgradetx.Resource{Domain: upgradetx.DomainAuthority,
		ID: incarnation, Origin: "session:" + p.host.Key.SID, Owner: owner,
		Generation: generation, State: "active", Custody: custody})
	rows, err := BundleRows(image, courier, transfers, owner, generation, custody)
	if err != nil {
		return nil, err
	}
	return p.retainReleased(rows)
}

// BundleRows maps the runtime image, courier outbox and the transfer census
// rows to one sorted census. IDs are the original object identities; Origin
// binds each to its creation lease (a transfer row to its entry slot).
func BundleRows(image execsession.Image, courier agentimage.CourierState, transfers []upgradetx.Resource, owner upgradetx.Owner, generation uint64, custody string) ([]upgradetx.Resource, error) {
	rows := append([]upgradetx.Resource(nil), transfers...)
	add := func(kind, id, origin, state, rowCustody string) {
		domain := upgradetx.DomainExec
		if kind == proto.ProcessKindPTY {
			domain = upgradetx.DomainPTY
		}
		rows = append(rows, upgradetx.Resource{Domain: domain, ID: id, Origin: origin, Owner: owner, Generation: generation, State: state, Custody: rowCustody})
	}
	for _, t := range image.Tasks {
		kind := t.Kind
		if t.PTY != nil {
			kind = proto.ProcessKindPTY
		}
		add(kind, "task:"+t.PID, "lease:"+t.Handle.Lease.LeaseID+"/request:"+t.Handle.RequestID, "running", custody)
	}
	for _, t := range image.Tombstones {
		kind := t.Kind
		if t.PTY != nil {
			kind = proto.ProcessKindPTY
		}
		add(kind, "task:"+t.PID, "lease:"+t.Handle.Lease.LeaseID+"/request:"+t.Handle.RequestID, "terminal", "retained")
	}
	for _, r := range image.Registry.Records {
		state, disposition := "retained", "retained"
		// Settlement is authenticated terminal ACK, not mere process exit.
		// Its dedupe record no longer has execution rights; expiry may collect
		// it without invalidating this cut's original identity proof.
		// origin: S8 internal review activation-and-route-migration F8
		settled := r.Phase == "settled" && r.Terminal != nil && r.Terminal.ID != ""
		if settled {
			state, disposition = "terminal", "released"
		}
		origin := "lease:" + r.Handle.Lease.LeaseID + "/request:" + r.Handle.RequestID
		add(r.Kind, "record:"+r.PID+"/"+r.Handle.RequestID, origin, state, disposition)
		if settled {
			present := false
			for _, t := range image.Tasks {
				present = present || t.PID == r.PID
			}
			for _, t := range image.Tombstones {
				present = present || t.PID == r.PID
			}
			if !present {
				add(r.Kind, "task:"+r.PID, origin, state, disposition)
			}
		}
	}
	for _, r := range image.Revocations {
		kind := ""
		if r.ClockUnknown {
			kind = proto.ProcessKindPTY
		}
		add(kind, "revocation:"+r.SID+"@"+strconv.FormatInt(r.AtBootNS, 10), "session:"+r.SID, "retained", "retained")
	}
	for _, e := range courier.Events {
		add("", "courier:"+e.PID+"/"+e.Kind, "courier:"+e.PID+"@"+strconv.FormatInt(e.EnqueuedAtUnixNano, 10), "retained", "retained")
	}
	sort.Slice(rows, func(i, j int) bool {
		return string(rows[i].Domain)+"\x00"+rows[i].ID < string(rows[j].Domain)+"\x00"+rows[j].ID
	})
	for i := range rows {
		if err := upgradetx.ValidateResource(rows[i]); err != nil || i > 0 && rows[i-1].Domain == rows[i].Domain && rows[i-1].ID == rows[i].ID {
			return nil, deferral(ReasonDomainCensus)
		}
	}
	return rows, nil
}

// Incarnation is the automatic-recovery authority port. Historical A cannot
// authorize a reverse while B is Current, and a recreated session cannot lend
// its creation identity to an accepted cut of the old session.
// origin: S8 internal review activation-and-route-migration F2
func (p *runtimePort) Incarnation(handle upgradetx.Handle) (string, error) {
	if p.host.Journal == nil || p.host.Incarnation == nil || handle.Key != p.host.Key {
		return "", deferral(ReasonIncarnationUnknown)
	}
	view, started := p.record(handle)
	latest, err := p.host.Journal.Load(p.host.Key, "")
	if !started || err != nil || view.Historical.Key != p.host.Key || !upgradetx.SameCut(current(latest.Historical).Handle, handle) ||
		view.Historical.Revision != handle.Revision || latest.Historical.Revision != handle.Revision {
		return "", deferral(ReasonIncarnationUnknown)
	}
	incarnation := p.host.Incarnation()
	if !hexValue(incarnation, 64) {
		return "", deferral(ReasonIncarnationUnknown)
	}
	matched := false
	for _, row := range current(view.Historical).Scope.Expected {
		if row.Domain == upgradetx.DomainAuthority && row.Origin == "session:"+handle.Key.SID {
			if row.ID != incarnation || matched {
				return "", deferral(ReasonIncarnationUnknown)
			}
			matched = true
		}
	}
	if !matched {
		return "", deferral(ReasonIncarnationUnknown)
	}
	if cut := p.host.Inherited; cut != nil {
		if !upgradetx.SameCut(cut.Attempt, handle) {
			return "", deferral(ReasonIncarnationUnknown)
		}
		found := false
		for _, authority := range cut.Authorities {
			if authority.SID == handle.Key.SID {
				found = authority.Incarnation == incarnation
			}
		}
		if !found {
			return "", deferral(ReasonIncarnationUnknown)
		}
	}
	return incarnation, nil
}

// retainReleased keeps monotonic release evidence after the runtime legally
// collects an expired settled result. Absence alone never creates a release:
// rows originate in a validated current export or the sealed imported cut.
// This ledger also rides the next final export, so reverse does not resurrect
// a collected result or forget an original resource when the runtime GC runs.
// origin: S8 internal review activation-and-route-migration F8
func (p *runtimePort) retainReleased(rows []upgradetx.Resource) ([]upgradetx.Resource, error) {
	identity := func(r upgradetx.Resource) string { return string(r.Domain) + "\x00" + r.ID + "\x00" + r.Origin }
	released := func(r upgradetx.Resource) bool {
		return (r.Domain == upgradetx.DomainExec || r.Domain == upgradetx.DomainPTY) && upgradetx.ResourceReleased(r)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Older S8 exporters named tombstones separately. Preserve that sealed
	// alias only when this current terminal proves the same lease/request;
	// an absent original never acquires a fabricated terminal disposition.
	// origin: S8 internal review activation-and-route-migration F8
	if cut := p.host.Inherited; cut != nil {
		live := make(map[string]upgradetx.Resource, len(rows))
		for _, row := range rows {
			live[identity(row)] = row
		}
		for _, original := range cut.Scope.Expected {
			if !strings.HasPrefix(original.ID, "tombstone:") {
				continue
			}
			task := original
			task.ID = "task:" + strings.TrimPrefix(original.ID, "tombstone:")
			if row, ok := live[identity(task)]; ok && row.State == "terminal" {
				if _, already := live[identity(original)]; !already {
					row.ID = original.ID
					rows = append(rows, row)
				}
			}
		}
	}
	if p.released == nil {
		p.released = make(map[string]upgradetx.Resource)
		if cut := p.host.Inherited; cut != nil {
			for _, r := range cut.Scope.Expected {
				if released(r) {
					p.released[identity(r)] = r
				}
			}
		}
	}
	// Retain at most the canonical scope ceiling. A full pending ledger
	// refuses another census instead of dropping evidence or growing forever.
	// origin: S8 internal review activation-and-route-migration F8
	if len(p.released) > 4096 {
		return nil, deferral(ReasonDomainCensus)
	}
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		seen[identity(r)] = true
		if released(r) {
			if _, known := p.released[identity(r)]; !known && len(p.released) == 4096 {
				return nil, deferral(ReasonDomainCensus)
			}
			p.released[identity(r)] = r
		}
	}
	for id, r := range p.released {
		if !seen[id] {
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return identity(rows[i]) < identity(rows[j]) })
	return rows, nil
}

// An automatic reverse retains its forward commitment in the SAME record.
// Current now names Reverse, so an ordinary current-only lookup rightly refuses
// the old handle. Retirement alone may read that forward scope while the exact
// adjacent reverse holds the current export. This projection is local and read
// only: it never changes Current, grants authority or substitutes old image data.
// origin: S8 internal review activation-and-route-migration F10; S8 plan §8.1
func retiringForwardRecord(p *runtimePort, handle upgradetx.Handle) (upgradetx.View, bool) {
	h := p.host
	if h.Inherited == nil || h.Journal == nil || h.Owner == nil || h.RunningSHA == nil || handle.Key != h.Key ||
		!upgradetx.SameCut(h.Inherited.Attempt, handle) {
		return upgradetx.View{}, false
	}
	view, err := h.Journal.Load(h.Key, handle.TxID)
	if err != nil || view.Historical.Key != h.Key || !upgradetx.SameCut(view.Historical.Forward.Handle, handle) ||
		view.Historical.Reverse.Handle.TxID != handle.TxID {
		return upgradetx.View{}, false
	}
	latest, err := h.Journal.Load(h.Key, "")
	if err != nil || !upgradetx.SameCut(current(latest.Historical).Handle, view.Historical.Reverse.Handle) {
		return upgradetx.View{}, false
	}
	owner, ownerErr := h.Owner()
	running, runningErr := h.RunningSHA()
	if ownerErr != nil || runningErr != nil || owner != h.Inherited.Owner || running != h.Inherited.TargetSHA {
		return upgradetx.View{}, false
	}
	p.mu.Lock()
	held := p.held
	available := held != nil && !held.activating && upgradetx.SameCut(held.handle, view.Historical.Reverse.Handle)
	p.mu.Unlock()
	if !available || !heldImageMatches(p, held, handle, running) {
		return upgradetx.View{}, false
	}
	view.Historical.Reverse = upgradetx.Attempt{}
	return view, true
}
