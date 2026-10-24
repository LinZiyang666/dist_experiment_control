// freeze.go — the cut: every domain frozen and exported in one order, or
// nothing frozen at all.
package agentimage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// BusyCheck lets an owner outside this package (the agent's bounded home
// read, a PTY session, a transfer) veto the cut without a schema change:
// it reports a deferral reason while its work is in flight. Checks run
// before anything is frozen.
type BusyCheck func() (reason, detail string, busy bool)

// Owners is everything the cut touches. Runtime and Manager are required;
// Spawn and Courier are optional (nil exports nothing for that domain).
type Owners struct {
	Runtime *execsession.Runtime
	Manager *workload.Manager
	Spawn   *spawnsafe.Policy
	Courier CourierExporter
	Busy    []BusyCheck
	Lineage Lineage
	// Flow must have bounded v1 configuration; Lineage.Generation names the
	// next owner. SS includes retained listener/material/timer responsibility.
	Flow *tunnel.Client
	SS   *ssproxy.Server
	// P20 installs the persistent worker; its adapter carries the P10 codec
	// and the current bindings. No compile stub is a valid image owner or an
	// implicit empty-state fallback.
	// origin: hot-update S7 plan §8.1–§8.2 / P21 — R9 NATS-free image ports
	Transfers TransferWorker
	// FrozenAtBootNS is the admission-close instant, if the caller closed its
	// outer admission before entering Freeze. Zero samples this entry instead.
	FrozenAtBootNS int64
}

// Frozen is a successful cut: the bundle plus the borrowed descriptors, and
// the two ways out — Prepare (pack it for exec), Thaw (the switch did not
// happen), Discard (the in-process switch happened and the new owner holds
// its own descriptors).
type Frozen struct {
	owners         Owners
	bundle         Bundle
	resources      []handoff.Resource
	done           bool
	flow           *tunnel.ClientFrozen
	ss             *ssproxy.Frozen
	snapshots      []*sealedstate.Snapshot
	frozenAtBootNS int64
	transfers      TransferFrozen
	transferUntil  controlbudget.Deadline
}

// Freeze performs the cut in the load-bearing order:
//
//  1. Busy checks (nothing frozen yet);
//  2. spawn-safety census (abandoned spawns or probes in flight defer;
//     ordinary in-time slots are drained after creation admission closes);
//  3. a read-only foreign-row precheck of the process table — a legacy
//     exec or PTY child shares the table and must defer BEFORE the runtime
//     is frozen (freezing first and rolling back would let a legacy
//     AdoptDraining in that window meet ErrFrozen and kill its child);
//  4. Runtime.Freeze (quiesce, join creations, registry, then pumps);
//  5. Spawn.Freeze closes slot/probe admission and drains non-Runtime slots
//     under the same absolute deadline, then Manager.Freeze (the table, after
//     the runtime so an in-flight spawn is
//     never adopted into a frozen table), rows re-checked for foreign ids;
//  6. exports: process table, runtime image, spawn verdicts, courier
//     outbox — the courier LAST, after the fence: no starting record means
//     no enqueueStarted in flight, and a gated observe means no
//     enqueueExit, so the outbox is complete;
//  7. the bundle is assembled and size-checked.
//
// Every failure requests rollback in reverse. A rollback that cannot finish
// within the deadline remains owned with closed admission and a visible error.
func Freeze(o Owners, deadline time.Time) (_ *Frozen, retErr error) {
	frozenAt := o.FrozenAtBootNS
	if frozenAt == 0 {
		frozenAt, _ = procrequest.BootNow()
	}

	if err := checkFreezeOwners(o, deadline); err != nil {
		return nil, err
	}
	// PREPARE runs while business is still live, before either safe-point fence.
	// Both fences are requested together: serial SS/flow waits can deadlock on
	// each other's raw socket. Every wait spends the original absolute deadline.
	// origin: hot-update S5 plan §5.3
	f := &Frozen{owners: o, frozenAtBootNS: frozenAt}
	if err := freezeTransfers(f, deadline); err != nil {
		return nil, err
	}
	transferSuccess := false
	defer func() {
		if !transferSuccess && f.transfers != nil {
			retErr = errors.Join(retErr, thawTransfers(f))
		}
	}()
	if err := prepareFlowHandoff(o, deadline); err != nil {
		return nil, err
	}
	if err := freezeFlowSS(f, deadline); err != nil {
		return nil, err
	}
	domainSuccess := false
	defer func() {
		if !domainSuccess {
			retErr = errors.Join(retErr, closeSnapshots(f.snapshots), thawFlowSS(f))
		}
	}()
	if err := o.Runtime.Freeze(deadline); err != nil {
		switch {
		case errors.Is(err, procrequest.ErrBusy):
			return nil, &DeferredError{Reason: ReasonSpawnInFlight, Detail: "registry: a creation is still starting", Cause: err}
		case errors.Is(err, execsession.ErrClosed):
			return nil, &DeferredError{Reason: ReasonRuntimeClosed, Detail: "runtime closed", Cause: err}
		}
		return nil, fmt.Errorf("agent image: freeze runtime: %w", err)
	}
	// Runtime must finish every admitted startup before closing the policy
	// fence: a reserved request may not have acquired its spawn slot yet.
	// The policy then closes acquisition and drains atomically with it, covering
	// boundedTouch/resolution and any other caller outside the runtime gate.
	// Keep it closed through Prepare/exec; a census followed by an unlocked
	// drain would admit a helper the exported image could never own.
	// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT tests 1–4.
	spawnFrozen := false
	if o.Spawn != nil {
		if err := o.Spawn.Freeze(deadline); err != nil {
			reason := ReasonSpawnInFlight
			if errors.Is(err, spawnsafe.ErrProbeInFlight) {
				reason = ReasonRemoteFSProbeInFlight
			}
			cause := error(&DeferredError{Reason: reason, Detail: err.Error(), Cause: err})
			if terr := execsession.ThawUntil(o.Runtime, deadline, false); terr != nil {
				cause = errors.Join(cause, fmt.Errorf("agent image: runtime rollback incomplete: %w", terr))
			}
			return nil, cause
		}
		spawnFrozen = true
	}
	// A failed cut has no image to dispatch. Open the policy even if runtime
	// rollback retains a recovery owner; that runtime still owns its admission.
	// Success transfers this fence to Frozen.Thaw or the same-PID exec.
	success := false
	defer func() {
		if spawnFrozen && !success {
			o.Spawn.Thaw()
		}
	}()
	// origin: hot-update S4 plan §4.1 — control admission shares the pump
	// deadline; starting a fresh wait here could make the overall cut unbounded.
	rows, err := o.Manager.FreezeUntil(deadline)
	if err != nil {
		// A table someone else froze is not ours to export. The runtime's
		// own thaw can itself be refused while that foreign freeze holds
		// (a revoked live task's deferred kill needs the table); say so
		// rather than leave a silently frozen runtime behind.
		err = fmt.Errorf("agent image: freeze process table: %w", err)
		if terr := execsession.ThawUntil(o.Runtime, deadline, false); terr != nil {
			err = errors.Join(err, fmt.Errorf("agent image: runtime still frozen: %w", terr))
		}
		return nil, err
	}
	rollback := func(cause error) error {
		if err := execsession.ThawUntil(o.Runtime, deadline, true); err != nil {
			return errors.Join(cause, fmt.Errorf("agent image: runtime rollback incomplete: %w", err))
		}
		return cause
	}
	if err := foreignRows(rows, o.Runtime.TaskPIDs()); err != nil {
		return nil, rollback(err)
	}
	processes, err := o.Manager.ExportProcesses()
	if err != nil {
		if errors.Is(err, handoff.ErrUnsupported) {
			return nil, rollback(&DeferredError{Reason: ReasonPlatformUnsupported, Detail: "process table export", Cause: err})
		}
		return nil, rollback(fmt.Errorf("agent image: export process table: %w", err))
	}
	image, resources, err := o.Runtime.Export()
	if err != nil {
		return nil, rollback(fmt.Errorf("agent image: export runtime: %w", err))
	}
	f.resources = resources
	f.bundle = Bundle{Version: BundleVersion, Instance: image.Instance, Lineage: o.Lineage, Processes: processes, Runtime: image,
		Courier: CourierState{Version: CourierStateVersion}}
	f.bundle.Lineage.BootID, f.bundle.Lineage.ProtoVersion, f.bundle.Lineage.ExporterRelease = processes.BootID, proto.ProtoVersion, proto.ReleaseVersion
	if f.bundle.Lineage.Generation == 0 {
		f.bundle.Lineage.Generation = 1
	}
	next, merged, snapshots, err := exportDomains(f)
	if err != nil {
		return nil, rollback(err)
	}
	f.bundle, f.resources, f.snapshots = next, merged, snapshots
	if err := exportFencedOwners(f); err != nil {
		return nil, rollback(err)
	}
	if err := f.bundle.Validate(); err != nil {
		return nil, rollback(err)
	}
	if _, err := encode(f.bundle); err != nil {
		return nil, rollback(err)
	}
	success, domainSuccess, transferSuccess = true, true, true
	return f, nil
}

// freezeTransfers is the first fence of the cut. On refusal it has already
// thawed whatever it froze; on success Freeze owns the rollback.
func freezeTransfers(f *Frozen, deadline time.Time) error {
	o := f.owners
	// Close worker admission and join IO/primitive/courier before any other
	// domain can be exported. Rollback uses this same absolute cut deadline.
	// origin: hot-update S7 plan §8.2 / P21
	if o.Transfers != nil {
		var err error
		f.transferUntil, err = transferCutDeadline(deadline)
		if err != nil {
			return err
		}
		f.transfers, err = o.Transfers.Freeze(f.transferUntil)
		if err != nil {
			return errors.Join(domainDeferral("freeze transfers", err), thawTransfers(f))
		}
		if f.transfers == nil {
			return errors.Join(errors.New("agent image: transfer freeze returned no owner"), thawTransfers(f))
		}
	}
	return nil
}

// prepareFlowHandoff mints this cut's identity for the flow carrier while
// business is still live; it freezes nothing.
func prepareFlowHandoff(o Owners, deadline time.Time) error {
	if o.Flow != nil {
		if o.Lineage.Generation < 2 {
			return errors.New("agent image: flow handoff requires the next owner generation")
		}
		var cutID [16]byte
		if _, err := rand.Read(cutID[:]); err != nil {
			return err
		}
		if err := o.Flow.PrepareHandoff(o.Lineage.Generation, cutID, deadline); err != nil {
			return domainDeferral("prepare flow", err)
		}
	}
	return nil
}

// exportFencedOwners exports the spawn verdicts and the courier outbox, the
// courier last: only after every fence is it known to be complete. The caller
// rolls the runtime back on any refusal.
func exportFencedOwners(f *Frozen) error {
	o := f.owners
	if o.Spawn != nil {
		st, err := o.Spawn.Export()
		if err != nil {
			reason := ReasonSpawnInFlight
			if errors.Is(err, spawnsafe.ErrProbeInFlight) {
				reason = ReasonRemoteFSProbeInFlight
			}
			return &DeferredError{Reason: reason, Detail: "spawnsafe export", Cause: err}
		}
		f.bundle.SpawnSafe = &st
	}
	if o.Courier != nil {
		st, err := o.Courier.ExportOutbox()
		if err != nil {
			return fmt.Errorf("agent image: export courier outbox: %w", err)
		}
		f.bundle.Courier = st
	}
	return nil
}

// checkFreezeOwners rejects foreign rows before any domain acquires a fence.
// origin: hot-update S5 plan §5.3
func checkFreezeOwners(o Owners, deadline time.Time) error {
	if o.Runtime == nil || o.Manager == nil {
		return errors.New("agent image: runtime and process table are required")
	}
	for _, check := range o.Busy {
		if reason, detail, busy := check(); busy {
			return &DeferredError{Reason: reason, Detail: detail}
		}
	}
	if o.Spawn != nil {
		if c := o.Spawn.Census(); c.Wedged > 0 {
			return &DeferredError{Reason: ReasonSpawnInFlight, Detail: fmt.Sprintf("spawnsafe: abandoned=%d", c.Wedged)}
		} else if c.ProbesInFlight > 0 {
			return &DeferredError{Reason: ReasonRemoteFSProbeInFlight, Detail: fmt.Sprintf("spawnsafe: probes in flight=%d", c.ProbesInFlight)}
		}
	}
	// origin: hot-update S4 internal review rv-fd rv-fd-3
	// Keep the legacy adoption precheck first, but spend the same cut deadline.
	rows, err := workload.SnapshotUntil(o.Manager, deadline)
	if err != nil {
		return err
	}
	pids, err := execsession.TaskPIDsUntil(o.Runtime, deadline)
	if err != nil {
		return err
	}
	if err := foreignRows(rows, pids); err != nil {
		return err
	}
	return nil
}

// foreignRows refuses a process table with a row no runtime task owns.
func foreignRows(rows []workload.Record, pids []string) error {
	owned := make(map[string]bool, len(pids))
	for _, pid := range pids {
		owned[pid] = true
	}
	for _, row := range rows {
		if !owned[row.ID] {
			return &DeferredError{Reason: ReasonForeignProcessRows, Detail: "process row " + row.ID + " belongs to no reliable task"}
		}
	}
	return nil
}

// Bundle returns the assembled image (a copy; the caller may not print it).
func (f *Frozen) Bundle() Bundle { return f.bundle }

// Prepare packs the cut for exec: the bundle as handoff.State.Data under
// Role, one resource per task plus any extra resources the caller carries
// (a bootstrap's control descriptors). The returned Prepared owns
// independent descriptor duplicates; the cut's originals stay with their
// frozen owners until Thaw or Discard.
func (f *Frozen) Prepare(extra ...handoff.Resource) (*handoff.Prepared, error) {
	return f.PrepareChecked(nil, extra...)
}

// PrepareChecked checks the exact re-export that will be encoded. The callback
// is a pure compatibility check: a late revocation may require a domain the
// reader could not have needed when Freeze first assembled the bundle.
// origin: hot-update S4 plan §2.3 / §5.2
func (f *Frozen) PrepareChecked(validate func(Bundle) error, extra ...handoff.Resource) (*handoff.Prepared, error) {
	if f.done {
		return nil, errors.New("agent image: cut already thawed or discarded")
	}
	// Every mutable data-plane domain contributes its current fenced decisions.
	// Courier keeps its late-delivery contract and is never re-read here.
	// A failed replacement closes only the new sealed files, retaining the
	// previous cut and all borrowed originals for a retry or rollback.
	// origin: hot-update S5 plan §5.2 / §5.3 / T33
	next, resources, snapshots, err := exportDomains(f)
	if err != nil {
		return nil, err
	}
	installed := false
	defer func() {
		if !installed {
			_ = closeSnapshots(snapshots)
		}
	}()
	if err := next.Validate(); err != nil {
		return nil, err
	}
	if validate != nil {
		if err := validate(next); err != nil {
			return nil, err
		}
	}
	data, err := encode(next)
	if err != nil {
		return nil, err
	}
	merged, err := handoff.MergeResources(resources, extra)
	if err != nil {
		return nil, err
	}
	// Agent auxiliary state arrives through extra rather than a domain's
	// Snapshot.Owned. It spends the same sealed allowance before duplication.
	// origin: hot-update S7 plan §8.3 / P21
	if err := validateSealedResources(merged); err != nil {
		return nil, err
	}
	prepared, err := handoff.Prepare(handoff.State{Role: Role, Instance: next.Instance, Generation: next.Lineage.Generation, Data: data}, merged)
	if err != nil {
		return nil, err
	}
	previous := f.snapshots
	f.bundle, f.resources, f.snapshots = next, resources, snapshots
	installed = true
	if err := closeSnapshots(previous); err != nil {
		_ = prepared.Close()
		return nil, err
	}
	return prepared, nil
}

// Thaw is the rollback: the switch did not happen (exec failed or was
// abandoned). Manager first, then the runtime (its deferred revocation kill
// needs a thawed table). The cut is spent afterwards: a new attempt must
// re-export the current state.
func (f *Frozen) Thaw() error {
	if f.done {
		return errors.New("agent image: cut already thawed or discarded")
	}
	// Keep the other owners fenced if the worker cannot rebind/thaw. Retrying
	// a spent Flow/SS thaw would otherwise strand transfer recovery forever.
	// origin: hot-update S7 plan §8.2 / P21
	if err := thawTransfers(f); err != nil {
		return err
	}
	f.done = true
	f.owners.Manager.Thaw()
	err := errors.Join(f.owners.Runtime.Thaw(), thawFlowSS(f), closeSnapshots(f.snapshots))
	if f.owners.Spawn != nil {
		f.owners.Spawn.Thaw()
	}
	return err
}

// Discard is the in-process switch's epilogue: the imported owner holds its
// own duplicates and the shared table, so only the old runtime's copies
// are closed. The table is NOT thawed here (the new owner thaws it).
func (f *Frozen) Discard() error {
	if f.done {
		return errors.New("agent image: cut already thawed or discarded")
	}
	f.done = true
	errs := []error{f.owners.Runtime.Discard()}
	if f.flow != nil {
		errs = append(errs, f.flow.Discard())
	}
	if f.ss != nil {
		errs = append(errs, f.ss.Discard())
	}
	if f.transfers != nil {
		// The in-process replacement owns duplicates now. Pristine detach
		// retires only this frozen manager; close the originals it transfers.
		// origin: hot-update S7 plan §8.2–§8.3 / P21
		salvage, err := f.transfers.DetachPristine()
		errs = append(errs, err, closeTransferResources(salvage.Resources))
	}
	errs = append(errs, closeSnapshots(f.snapshots))
	// An in-process switch can retain the same policy for background callers.
	// Retire the old runtime before releasing its slot fence; the new runtime
	// has its own frozen creation admission until activation finishes.
	// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT fence ownership.
	if f.owners.Spawn != nil {
		f.owners.Spawn.Thaw()
	}
	return errors.Join(errs...)
}

// freezeFlowSS joins both independent fences before rolling either one back.
// A successful half must never be abandoned when the other owner refuses.
func freezeFlowSS(f *Frozen, deadline time.Time) error {
	results := make(chan error, 2)
	pending := 0
	if f.owners.Flow != nil {
		pending++
		go func() {
			var err error
			f.flow, err = f.owners.Flow.FreezeFlows(deadline)
			results <- domainDeferral("freeze flow", err)
		}()
	}
	if f.owners.SS != nil {
		pending++
		go func() {
			var err error
			f.ss, err = f.owners.SS.Freeze(deadline)
			results <- domainDeferral("freeze SS", err)
		}()
	}
	var errs []error
	for n := 0; n < pending; n++ {
		errs = append(errs, <-results)
	}
	if err := errors.Join(errs...); err != nil {
		return errors.Join(err, thawFlowSS(f))
	}
	return nil
}

func thawFlowSS(f *Frozen) error {
	var errs []error
	if f.flow != nil {
		errs = append(errs, f.flow.Thaw())
	}
	if f.ss != nil {
		errs = append(errs, f.ss.Thaw())
	}
	return errors.Join(errs...)
}

// exportDomains constructs a candidate cut without mutating the current one.
// Snapshot.Resources borrows business FDs, Snapshot.Owned owns sealed FDs.
// Only the latter may be closed on a failed replacement.
// origin: hot-update S5 plan §5.3 / T33
func exportDomains(f *Frozen) (next Bundle, resources []handoff.Resource, snapshots []*sealedstate.Snapshot, retErr error) {
	// A retained domain may not silently fall back to runtime-only export.
	// origin: hot-update S5 plan §5.2 / T33
	if (f.bundle.Flow != nil || f.owners.Flow != nil) && f.flow == nil || (f.bundle.SS != nil || f.owners.SS != nil) && f.ss == nil ||
		(f.bundle.Transfers != nil || f.owners.Transfers != nil) && f.transfers == nil {
		return next, nil, nil, fmt.Errorf("%w: retained domain has no frozen owner", ErrBundle)
	}
	next = f.bundle
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, closeSnapshots(snapshots))
		}
	}()
	var err error
	next.Runtime, resources, err = f.owners.Runtime.Export()
	if err != nil {
		return next, nil, nil, fmt.Errorf("agent image: re-export runtime: %w", err)
	}
	// Runtime's cut remains the accounting instant understood by S3. The
	// earlier admission instant is calibration metadata, never a task bound.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility
	next.admissionAt = f.frozenAtBootNS
	calibration, err := freezeCalibration(next)
	if err != nil {
		return next, nil, snapshots, err
	}
	snapshots = append(snapshots, calibration)
	if f.flow != nil {
		im, snapshot, err := f.flow.Export()
		if err != nil {
			return next, nil, snapshots, domainDeferral("export flow", err)
		}
		if snapshot == nil {
			return next, nil, snapshots, errors.New("agent image: flow export has no resource snapshot")
		}
		retained, err := retainDomainSnapshot(snapshot)
		if err != nil {
			return next, nil, snapshots, domainDeferral("retain flow", err)
		}
		snapshots = append(snapshots, retained)
		next.Flow = &im
	}
	if f.ss != nil {
		im, snapshot, err := f.ss.Export()
		if err != nil {
			return next, nil, snapshots, domainDeferral("export SS", err)
		}
		if snapshot == nil {
			return next, nil, snapshots, errors.New("agent image: SS export has no resource snapshot")
		}
		retained, err := retainDomainSnapshot(snapshot)
		if err != nil {
			return next, nil, snapshots, domainDeferral("retain SS", err)
		}
		snapshots = append(snapshots, retained)
		next.SS = &im
	}
	if f.transfers != nil {
		state, borrowed, err := f.transfers.Export()
		if err != nil {
			return next, nil, snapshots, domainDeferral("export transfers", err)
		}
		if err := validateTransferState(next, state, borrowed); err != nil {
			return next, nil, snapshots, err
		}
		data := state.Encoded
		digest := sha256.Sum256(data)
		ref := xfervalue.ImageRef{Schema: xfervalue.StateSchema, Digest: hex.EncodeToString(digest[:]),
			Count: state.Entries, EncodedBytes: uint64(len(data))}
		ref.ResourceID = "xfer-state." + ref.Digest[:32]
		snapshot, err := sealedstate.New(ref.ResourceID, data)
		if err != nil {
			return next, nil, snapshots, domainDeferral("seal transfers", err)
		}
		snapshot.Resources[0].Kind = xfervalue.ResourceState
		snapshot.Resources = append(snapshot.Resources, borrowed...)
		snapshots = append(snapshots, snapshot)
		next.Transfers = &ref
	}
	resources, err = mergeDomainResources(resources, snapshots)
	if err != nil {
		return next, nil, snapshots, err
	}
	if err := validateCutResources(next, resources); err != nil {
		return next, nil, snapshots, err
	}
	return next, resources, snapshots, nil
}

func mergeDomainResources(runtime []handoff.Resource, snapshots []*sealedstate.Snapshot) ([]handoff.Resource, error) {
	// The private-state bound applies to the whole cut, not independently
	// to each domain. Count owned files once without reading secret bytes.
	var privateBytes int64
	seen := make(map[*os.File]bool)
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		for _, file := range snapshot.Owned {
			if file == nil || seen[file] {
				continue
			}
			seen[file] = true
			info, err := file.Stat()
			if err != nil {
				return nil, fmt.Errorf("%w: sealed snapshot is unavailable", ErrBundle)
			}
			if info.Size() < 0 || info.Size() > sealedstate.MaxStateBytes-privateBytes {
				return nil, &DeferredError{Reason: ReasonImageTooLarge, Detail: "combined sealed state exceeds the cut bound"}
			}
			privateBytes += info.Size()
		}
	}
	groups := [][]handoff.Resource{runtime}
	for _, snapshot := range snapshots {
		if snapshot != nil {
			groups = append(groups, snapshot.Resources)
		}
	}
	resources, err := handoff.MergeResources(groups...)
	if err != nil {
		return nil, err
	}
	files := 0
	for _, resource := range resources {
		files += len(resource.Files)
	}
	if files > maxHandoffFiles {
		return nil, &DeferredError{Reason: ReasonImageTooLarge, Detail: "composed cut exceeds the descriptor bound"}
	}
	return resources, nil
}

func validateSealedResources(resources []handoff.Resource) error {
	var total int64
	seen := make(map[*os.File]bool)
	for _, resource := range resources {
		file := resource.Files[xfervalue.StateFileName]
		if file == nil || seen[file] {
			continue
		}
		seen[file] = true
		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("%w: sealed snapshot is unavailable", ErrBundle)
		}
		if info.Size() < 0 || info.Size() > sealedstate.MaxStateBytes-total {
			return &DeferredError{Reason: ReasonImageTooLarge, Detail: "combined sealed state exceeds the cut bound"}
		}
		total += info.Size()
	}
	return nil
}

// Closing Owned directly makes the cut's responsibility explicit even on an
// export error. Borrowed raw/replay files are never closed by this helper.
func closeSnapshots(snapshots []*sealedstate.Snapshot) error {
	var errs []error
	seen := make(map[*os.File]bool)
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		for _, file := range snapshot.Owned {
			if file != nil && !seen[file] {
				seen[file] = true
				errs = append(errs, file.Close())
			}
		}
		snapshot.Owned = nil
	}
	return errors.Join(errs...)
}

func domainDeferral(operation string, err error) error {
	if err == nil {
		return nil
	}
	var deferred *DeferredError
	switch {
	case errors.As(err, &deferred):
		// An owner's own deferral (e.g. transfers in flight) keeps its reason.
		return deferred
	case errors.Is(err, ssproxy.ErrStateUnrepresentable):
		return &DeferredError{Reason: ReasonSSStateUnrepresentable, Detail: operation, Cause: ssproxy.ErrStateUnrepresentable}
	case errors.Is(err, ssproxy.ErrCapacity), errors.Is(err, sealedstate.ErrCapacity):
		return &DeferredError{Reason: ReasonImageTooLarge, Detail: operation}
	case errors.Is(err, sealedstate.ErrUnsupported):
		return &DeferredError{Reason: ReasonPlatformUnsupported, Detail: operation}
	default:
		// Owner errors may carry private parser state. Expose a bounded operation,
		// preserving error identity without rendering the owner's error text.
		return &domainError{operation: operation, cause: err}
	}
}

type domainError struct {
	operation string
	cause     error
}

func (e domainError) Error() string { return "agent image: " + e.operation + " refused" }
func (e domainError) Unwrap() error { return e.cause }

// Formatting must not expose a value-type private error through %#v.
// origin: hot-update S5 plan §6.6
func (e domainError) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(e.Error())) }

// Metadata and descriptors must describe the same closed cut. Counting files
// alone cannot detect a resource replacement that drops an entire domain.
// origin: hot-update S5 plan §5.1 / §5.3 / T33
func validateCutResources(b Bundle, resources []handoff.Resource) error {
	files := make(map[string]map[string]*os.File, len(resources))
	for _, resource := range resources {
		files[resource.ID] = resource.Files
	}
	need := func(id, name string) error {
		if id == "" || files[id][name] == nil {
			return fmt.Errorf("%w: retained domain resource is missing", ErrBundle)
		}
		return nil
	}
	if im := b.Flow; im != nil {
		if err := need(im.StateRef, "state"); err != nil {
			return err
		}
		if err := need(im.Flows.StateRef, "state"); err != nil {
			return err
		}
		for _, endpoint := range im.Endpoints {
			if err := need(endpoint.StateRef, "state"); err != nil {
				return err
			}
		}
		for _, record := range im.Flows.Records {
			if record.RawRef != "" {
				if err := need(record.RawRef, "raw"); err != nil {
					return err
				}
			}
			if record.ReplayRef != "" {
				if err := need(record.ReplayRef, "replay"); err != nil {
					return err
				}
			}
		}
	}
	if im := b.SS; im != nil {
		if err := need(im.StateRef, "state"); err != nil {
			return err
		}
		if im.ListenerRef != "" {
			if err := need(im.ListenerRef, "listener"); err != nil {
				return err
			}
		}
	}
	if b.Transfers != nil {
		if err := need(b.Transfers.ResourceID, xfervalue.StateFileName); err != nil {
			return err
		}
	}
	return nil
}

// thawTransfers rebinds through the adapter, which resolves the backend that
// is current at thaw: stale NC/authority is never captured in Owners.
// origin: hot-update S7 plan §8.2 / P21
func thawTransfers(f *Frozen) error {
	if f.owners.Transfers == nil {
		return nil
	}
	return domainDeferral("thaw transfers", f.owners.Transfers.Thaw(f.transferUntil))
}

func transferCutDeadline(deadline time.Time) (controlbudget.Deadline, error) {
	now, err := controlbudget.Now()
	remaining := time.Until(deadline)
	if err != nil || remaining <= 0 || now <= 0 || int64(remaining) > int64(^uint64(0)>>1)-now {
		return 0, errors.New("agent image: transfer cut deadline is unavailable")
	}
	return controlbudget.Deadline(now + int64(remaining)), nil
}
