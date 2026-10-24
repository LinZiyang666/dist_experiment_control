// restore.go — the other side of the cut: decode, validate every domain,
// take the descriptors, build everything frozen, install the process table
// last, and give it all back (Salvage) if any step refuses.
package agentimage

import (
	"bytes"
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
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// Target is where a bundle is restored into. Runtime.Instance is left empty
// (the image supplies it) and Runtime.Manager must be the table that will
// hold the exported rows — unused for a real exec, or the still-frozen
// shared table for an in-process switch. Spawn and Courier are optional.
type Target struct {
	Runtime execsession.Config
	Spawn   *spawnsafe.Policy
	Courier CourierImporter
	Flow    tunnel.FlowConfig
	// The outer agent's adapter supplies the real P06/P10 ports and resolves
	// current bindings at activation. Import remains sleeping until the
	// installed current owner is activated.
	// origin: hot-update S7 plan §8.1–§8.2 / P21 — R9 NATS-free image ports
	Transfers        TransferImporter
	TransferDeadline controlbudget.Deadline
}

// Salvage is what a refused or abandoned import hands back: the envelope,
// every descriptor that was taken (open and untouched), and the bundle
// itself — decoded when Decode succeeded, the dispatched bytes when it did
// not — so that Prepare can re-dispatch the SAME bundle to the previous
// image. Files still inside the *handoff.Restored are not here; the caller
// closes or re-packs that bundle itself. The bundle carries the registry
// key: Salvage renders as a summary under every fmt verb and State.Data is
// always nil (S2 internal review I1 / import-V1).
type Salvage struct {
	State     handoff.State
	Resources []handoff.Resource
	bundle    *Bundle
	raw       []byte
}

// Prepare packs the salvage for a re-dispatch to the previous image: the
// same bundle (re-encoded, or the very bytes that were dispatched when they
// could not be decoded) under Role, with the salvaged descriptors plus any
// extra resources the caller still holds (the ones left inside the source
// bundle, control descriptors). It returns an error when no bundle survived
// (nothing was dispatched to begin with).
func (s *Salvage) Prepare(extra ...handoff.Resource) (*handoff.Prepared, error) {
	if s == nil {
		return nil, errors.New("agent image: no salvage")
	}
	data := s.raw
	if s.bundle != nil {
		encoded, err := encode(*s.bundle)
		if err != nil {
			return nil, err
		}
		data = encoded
	}
	if len(data) == 0 {
		return nil, errors.New("agent image: salvage carries no bundle to re-dispatch")
	}
	state := handoff.State{Role: Role, Instance: s.State.Instance, Generation: s.State.Generation, Data: data}
	// The salvage holds the files the import took; the caller's extra
	// resources hold the ones it never took (handoff.Restored.Borrow) and its
	// control descriptors. Both halves name the same task resource, so they
	// are merged by ID — a plain append would be refused by handoff.Prepare
	// as a duplicate resource (S3 plan §1.4 P4).
	resources, err := handoff.MergeResources(s.Resources, extra)
	if err != nil {
		return nil, err
	}
	return handoff.Prepare(state, resources)
}

// Bundle returns the decoded bundle, or nil when the salvage stems from a
// Decode failure (then only the raw bytes exist, for Prepare).
func (s *Salvage) Bundle() *Bundle {
	if s == nil {
		return nil
	}
	return s.bundle
}

// String is a summary: never the bundle. Value receiver on purpose: a
// dereferenced Salvage under %+v would otherwise dump raw as a byte list
// (the key, spelled in decimal) — fmt only consults pointer-receiver
// methods on the pointer.
func (s Salvage) String() string {
	files := 0
	for _, res := range s.Resources {
		files += len(res.Files)
	}
	return fmt.Sprintf("agentimage.Salvage{instance=%s generation=%d resources=%d files=%d decoded=%t raw=%d}",
		s.State.Instance, s.State.Generation, len(s.Resources), files, s.bundle != nil, len(s.raw))
}

// Format routes every verb (%v, %+v, %s, %#v, …) to the summary: a
// bootstrap error log that renders the salvage must not print the key.
func (s Salvage) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(s.String())) }

// Close releases every salvaged descriptor (when no re-dispatch follows).
func (s *Salvage) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	for _, res := range s.Resources {
		for name, f := range res.Files {
			errs = append(errs, f.Close())
			delete(res.Files, name)
		}
	}
	return errors.Join(errs...)
}

// Imported is a restored, still-frozen agent image.
type Imported struct {
	Bundle    Bundle
	Runtime   *execsession.Runtime
	Flow      *tunnel.Client
	SS        *ssproxy.Server
	Transfers TransferWorker
	Manager   *workload.Manager
	FrozenAt  int64
	// SpawnVerdicts counts the mount verdicts the policy installed (the
	// exported ones whose mount is still the same instance).
	SpawnVerdicts int
	thawedAt      int64
	thawed        bool
	prepared      bool
	active        bool // responsibility boundary, even when Thaw only partially succeeds
	detached      bool
	// restoredTable is true when this import installed the process table
	// (a real exec); an in-process switch shares the exporter's table and
	// must never discard it.
	restoredTable    bool
	target           Target
	runtimeReady     bool
	flowRawReady     bool
	ssRawReady       bool
	rawReady         bool
	carrierReady     bool
	ssReady          bool
	rawActivatedAt   int64
	carrierResumedAt int64
	ssAuthorizedAt   int64
	transferReady    bool
	transferState    []handoff.Resource // taken sealed metadata, owned until detach/activation
	transferTaken    []handoff.Resource // custody evidence for every leaf TakeFile, including partial errors
}

// Restore decodes the dispatched bundle and imports every domain. Order:
// pure validation of all domains first (nothing taken); then the runtime
// import (descriptors taken and checked, actors built paused); then the
// process table (RestoreProcesses is the last failable step: it has no side
// effect on failure and the kernel checks it performs are the only ones
// that cannot be done from data); then the spawn verdicts and the courier
// outbox (already validated; a refusal here is a caller error and is
// treated as fatal for the import). On any failure the taken descriptors
// come back as Salvage and the source bundle is left for the caller.
func Restore(src *handoff.Restored, tgt Target) (*Imported, *Salvage, error) {
	if src == nil {
		return nil, nil, errors.New("agent image: no dispatched bundle")
	}
	raw := append([]byte(nil), src.State.Data...)
	envelope := src.State
	envelope.Data = nil // the raw JSON carries the registry secret; it lives only inside the Salvage
	b, err := Decode(src.State)
	src.State.Data = nil
	if err != nil {
		return nil, &Salvage{State: envelope, raw: raw}, err
	}
	if err := restoreCalibration(&b, src); err != nil {
		return nil, &Salvage{State: envelope, raw: raw}, err
	}
	rows := b.Processes.Records
	if tgt.Runtime.Manager != nil && len(tgt.Runtime.Manager.Snapshot()) != 0 {
		// In-process switch over a shared table: it must be the frozen table
		// the bundle was cut from.
		if !tgt.Runtime.Manager.Frozen() {
			return nil, &Salvage{State: envelope}, fmt.Errorf("%w: shared process table is not frozen", ErrBundle)
		}
		rows = tgt.Runtime.Manager.Snapshot()
		if err := sameRows(rows, b.Processes.Records); err != nil {
			return nil, &Salvage{State: envelope, bundle: &b}, err
		}
	}
	imp, salvage, err := importDomains(b, rows, src, tgt, envelope)
	if err != nil {
		return nil, salvage, err
	}
	return imp, nil, nil
}

// Import is the in-process half of Restore for callers that already hold a
// decoded bundle and a FileSource (tests and the in-process switch).
func Import(b Bundle, rows []workload.Record, files execsession.FileSource, tgt Target) (*Imported, *Salvage, error) {
	if err := b.Validate(); err != nil {
		return nil, &Salvage{State: handoff.State{Role: Role, Instance: b.Instance, Generation: b.Lineage.Generation}, bundle: &b}, err
	}
	envelope := handoff.State{Role: Role, Instance: b.Instance, Generation: b.Lineage.Generation}
	imp, salvage, err := importDomains(b, rows, files, tgt, envelope)
	if err != nil {
		return nil, salvage, err
	}
	return imp, nil, nil
}

func importDomains(b Bundle, rows []workload.Record, files execsession.FileSource, tgt Target, envelope handoff.State) (*Imported, *Salvage, error) {
	salvageOf := func(resources []handoff.Resource) *Salvage {
		return &Salvage{State: envelope, Resources: resources, bundle: &b}
	}
	if tgt.Runtime.Manager == nil {
		return nil, salvageOf(nil), errors.New("agent image: target has no process table")
	}
	if b.Flow != nil {
		var err error
		tgt, err = shareTargetReplayBudget(tgt)
		if err != nil {
			return nil, salvageOf(nil), err
		}
	}
	// Every refusal the courier and spawn importers can make is asked BEFORE
	// anything is installed (S2 internal review I5): after this point only a
	// race with the caller's own use of those objects can fail, and both
	// installs are then all-or-nothing with the runtime and the table.
	if tgt.Courier != nil {
		if err := tgt.Courier.Check(b.Courier); err != nil {
			return nil, salvageOf(nil), fmt.Errorf("%w: courier: %v", ErrBundle, err)
		}
	}
	if tgt.Spawn != nil && b.SpawnSafe != nil {
		if err := tgt.Spawn.ImportReady(); err != nil {
			return nil, salvageOf(nil), fmt.Errorf("%w: spawn safety: %v", ErrBundle, err)
		}
	}
	// Refuse before Take unless each owner can return its frozen wrappers intact;
	// Discard would close the only descriptors available for redispatch.
	// origin: hot-update S5 plan §5.3 / §5.4
	if err := requireDomainDetach(b); err != nil {
		return nil, salvageOf(nil), err
	}
	cfg := tgt.Runtime
	cfg.Instance = ""
	var workerState TransferState
	var transferResources []handoff.Resource
	if b.Transfers != nil {
		var err error
		workerState, transferResources, err = readTransferState(b, files, tgt)
		if err != nil {
			return nil, salvageOf(transferResources), err
		}
	}
	rt, salvaged, err := execsession.Import(cfg, b.Runtime, rows, files)
	if err != nil {
		if errors.Is(err, execsession.ErrForeignProcess) {
			err = &DeferredError{Reason: ReasonForeignProcessRows, Detail: "runtime import", Cause: err}
		}
		resources, mergeErr := handoff.MergeResources(salvaged, transferResources)
		return nil, salvageOf(resources), errors.Join(err, mergeErr)
	}
	imp := &Imported{Bundle: b, Runtime: rt, Manager: tgt.Runtime.Manager, FrozenAt: admissionInstant(b), target: tgt, transferState: transferResources}
	fail := func(err error) (*Imported, *Salvage, error) {
		resources, detachErr := detachDomains(imp)
		return nil, salvageOf(resources), errors.Join(err, detachErr)
	}
	if b.Transfers != nil {
		source := &transferImportSource{files: files, refs: transferResourceRefs(workerState)}
		manager, salvage, err := tgt.Transfers.Import(workerState, source)
		imp.Transfers = manager // partial+error also retains explicit ownership
		imp.transferTaken = source.resources
		err = errors.Join(err, source.cause)
		if err == nil && len(source.taken) != len(source.refs) {
			err = fmt.Errorf("%w: transfer importer did not take its resource closure", ErrBundle)
		}
		if err != nil || manager == nil {
			_, recovered, cause := fail(errors.Join(domainDeferral("restore transfers", err), transferOwnerMissing(manager)))
			resources, mergeErr := mergeTransferSalvage(recovered.Resources, salvage.Resources, source.resources)
			recovered.Resources = resources
			return nil, recovered, errors.Join(cause, mergeErr)
		}
	}
	if b.Flow != nil {
		cfg := tgt.Flow
		cfg.OwnerGen, cfg.BootID = b.Lineage.Generation, b.Lineage.BootID
		if b.Lineage.InstanceID != "" {
			cfg.InstanceID = b.Lineage.InstanceID
		} else if cfg.InstanceID == "" {
			cfg.InstanceID = b.Instance
		}
		client, resources, err := tunnel.RestoreClient(cfg, *b.Flow, files)
		if err != nil {
			_, salvage, cause := fail(domainDeferral("restore flow", err))
			salvage.Resources, err = handoff.MergeResources(salvage.Resources, resources)
			return nil, salvage, errors.Join(cause, err)
		}
		imp.Flow = client
	}
	if b.SS != nil {
		server, resources, err := ssproxy.Restore(*b.SS, files)
		if err != nil {
			_, salvage, cause := fail(domainDeferral("restore SS", err))
			salvage.Resources, err = handoff.MergeResources(salvage.Resources, resources)
			return nil, salvage, errors.Join(cause, err)
		}
		imp.SS = server
	}
	if len(tgt.Runtime.Manager.Snapshot()) == 0 && len(b.Processes.Records) != 0 {
		if err := tgt.Runtime.Manager.RestoreProcesses(b.Processes); err != nil {
			if errors.Is(err, handoff.ErrUnsupported) {
				return fail(&DeferredError{Reason: ReasonPlatformUnsupported, Detail: "process table import", Cause: err})
			}
			return fail(fmt.Errorf("%w: process table: %v", ErrBundle, err))
		}
		imp.restoredTable = true
	}
	// Courier before spawn verdicts: neither can be un-imported, and the
	// courier's install is the one with a second precondition (an empty
	// outbox) that a caller race could still break.
	if tgt.Courier != nil {
		if err := tgt.Courier.ImportOutbox(b.Courier); err != nil {
			_ = imp.discardTable()
			return fail(fmt.Errorf("agent image: courier import: %w", err))
		}
	}
	if tgt.Spawn != nil && b.SpawnSafe != nil {
		installed, err := tgt.Spawn.Import(*b.SpawnSafe)
		if err != nil {
			_ = imp.discardTable()
			return fail(fmt.Errorf("agent image: spawn safety import: %w", err))
		}
		imp.SpawnVerdicts = installed
	}
	return imp, nil, nil
}

// Record TakeFile before inspecting its error: a leaf can receive ownership
// together with an error. The composed salvage cannot depend on it remembering
// that FD on every partial-constructor path.
// origin: hot-update S7 plan §8.2–§8.3 / P21
type transferImportSource struct {
	files     execsession.FileSource
	refs      map[string]xfervalue.ResourceRef
	taken     map[string]bool
	resources []handoff.Resource
	cause     error
}

func (s *transferImportSource) TakeFile(id, name string) (*os.File, error) {
	key := id + "/" + name
	ref, ok := s.refs[key]
	if !ok || s.taken[key] {
		err := fmt.Errorf("%w: transfer importer requested an undeclared resource", ErrBundle)
		s.cause = errors.Join(s.cause, err)
		return nil, err
	}
	file, err := s.files.TakeFile(id, name)
	if file != nil {
		if s.taken == nil {
			s.taken = make(map[string]bool)
		}
		s.taken[key] = true
		s.resources = append(s.resources, handoff.Resource{ID: id, Kind: ref.Kind, Files: map[string]*os.File{name: file}})
	} else if err == nil {
		err = fmt.Errorf("%w: transfer source returned no descriptor", ErrBundle)
	}
	s.cause = errors.Join(s.cause, err)
	return file, err
}

func mergeTransferSalvage(groups ...[]handoff.Resource) ([]handoff.Resource, error) {
	// Repeated observations of the exact same taken FD are one responsibility;
	// another FD at that ID/name remains a conflict rather than being discarded.
	var unique []handoff.Resource
	seen := make(map[string]*os.File)
	var conflict error
	for _, group := range groups {
		for _, resource := range group {
			copyResource := resource
			copyResource.Files = make(map[string]*os.File)
			for name, file := range resource.Files {
				key := resource.ID + "/" + name
				if previous := seen[key]; previous != nil {
					if previous != file {
						// Refuse repacking, but retain both files for explicit Close.
						conflict = fmt.Errorf("%w: transfer salvage has conflicting ownership", ErrBundle)
						copyResource.Files[name] = file
					}
					continue
				}
				seen[key], copyResource.Files[name] = file, file
			}
			if len(copyResource.Files) != 0 {
				unique = append(unique, copyResource)
			}
		}
	}
	merged, err := handoff.MergeResources(unique)
	if err != nil || conflict != nil {
		return unique, errors.Join(conflict, err)
	}
	return merged, nil
}

// Resolve the node owner before either importer takes files. Otherwise a
// default runtime creates private backing storage while flow reserves against
// a separate pointer, allowing each domain to consume the full node limit.
// Conflicting explicit owners are a caller error, not permission to abandon
// one owner's existing reservations. Exported Limit metadata is never adopted.
// origin: hot-update S5 plan §3.4 / §5.3 / T30
func shareTargetReplayBudget(tgt Target) (Target, error) {
	budget := tgt.Runtime.SharedReplayBudget
	if budget != nil && tgt.Flow.Budget != nil && budget != tgt.Flow.Budget {
		return tgt, fmt.Errorf("%w: target replay reservation owners differ", ErrBundle)
	}
	if budget == nil {
		budget = tgt.Flow.Budget
	}
	if budget == nil {
		if tgt.Runtime.ReplayBudget != 0 && tgt.Runtime.ReplayBudget != workload.DefaultReplayBudget {
			return tgt, fmt.Errorf("%w: flow requires the node replay limit", ErrBundle)
		}
		budget = &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	}
	if budget.Limit != 0 && budget.Limit != workload.DefaultReplayBudget {
		return tgt, fmt.Errorf("%w: flow requires the node replay limit", ErrBundle)
	}
	tgt.Runtime.SharedReplayBudget, tgt.Flow.Budget = budget, budget
	return tgt, nil
}

// sameRows proves a shared frozen table is the one the bundle was cut from.
func sameRows(table, exported []workload.Record) error {
	if len(table) != len(exported) {
		return fmt.Errorf("%w: shared table has %d rows, bundle %d", ErrBundle, len(table), len(exported))
	}
	byID := make(map[string]workload.Record, len(exported))
	for _, row := range exported {
		byID[row.ID] = row
	}
	for _, row := range table {
		want, ok := byID[row.ID]
		if !ok || want.OSPID != row.OSPID || (want.Exit == nil) != (row.Exit == nil) || want.ReapHeld != row.ReapHeld {
			return fmt.Errorf("%w: shared table row %s differs from the bundle", ErrBundle, row.ID)
		}
	}
	return nil
}

// discardTable releases a table this import restored (never a shared one).
func (i *Imported) discardTable() error {
	if !i.restoredTable {
		return nil
	}
	i.restoredTable = false
	return i.Manager.Discard()
}

// PreActivate completes all preparation that may fail while the initial
// bundle is still safe to detach. It does not thaw the process table.
// origin: hot-update S4 plan §4.2 / T23
func (i *Imported) PreActivate() error {
	if i.thawed || i.detached {
		return errors.New("agent image: already thawed or detached")
	}
	if i.prepared || i.active {
		return nil
	}
	if err := i.Runtime.PreActivate(); err != nil {
		return fmt.Errorf("agent image: prepare activation: %w", err)
	}
	i.prepared = true
	return nil
}

// Active reports resource responsibility, not successful activation. Once the
// process table may have moved, recovery must cut the current owner set.
func (i *Imported) Active() bool { return i.active }

// Thaw activates the image: preactivation, process table, then runtime. The
// thaw instant is kept so FreezeWindow can report the cut's duration on the
// same CLOCK_BOOTTIME the exporter stamped. A partial failure retains ownership
// and may be retried; it never makes the initial bundle replayable again.
func (i *Imported) Thaw() error {
	if err := i.PreActivate(); err != nil {
		return err
	}
	// origin: hot-update S4 plan §4.2 / T23 — even a failed Runtime.Thaw
	// follows a live Manager, whose reaper can already change process state.
	if i.Bundle.Flow != nil && i.Flow == nil || i.Bundle.SS != nil && i.SS == nil || i.Bundle.Transfers != nil && i.Transfers == nil {
		return errors.New("agent image: retained data-plane owner is unavailable")
	}
	i.active = true
	i.Manager.Thaw()
	if !i.runtimeReady {
		if err := i.Runtime.Thaw(); err != nil {
			return fmt.Errorf("agent image: thaw runtime: %w", err)
		}
		i.runtimeReady = true
	}
	if i.Flow != nil && !i.flowRawReady {
		if err := i.Flow.ActivateRaw(); err != nil {
			return domainDeferral("activate flow raw", err)
		}
		i.flowRawReady = true
	}
	if i.SS != nil && !i.ssRawReady {
		if err := i.SS.ActivateRaw(); err != nil {
			return domainDeferral("activate SS raw", err)
		}
		i.ssRawReady = true
	}
	if i.Transfers != nil && !i.transferReady {
		// Install precedes callbacks; bind the current data connection and
		// authority before any worker, courier or cancellation actor wakes.
		// origin: hot-update S7 plan §8.2 / P21
		if err := i.Transfers.Thaw(i.target.TransferDeadline); err != nil {
			return domainDeferral("activate transfers", err)
		}
		i.transferReady = true
		if err := closeTransferResources(i.transferState); err != nil {
			return err
		}
		i.transferState = nil
		i.transferTaken = nil // the ACTIVE manager now owns the business closure
	}
	i.rawReady = true
	i.rawActivatedAt, _ = procrequest.BootNow()
	i.thawed = true
	finishActivation(i)
	return nil
}

// FreezeWindow is the time between the exporter's cut and this image's
// complete domain activation (zero until raw, carriers and SS grant are ready).
// The 500 ms target is calibration; NATS register is a separate interval.
func (i *Imported) FreezeWindow() time.Duration {
	if !i.thawed || i.thawedAt == 0 || i.FrozenAt == 0 {
		return 0
	}
	return time.Duration(i.thawedAt - i.FrozenAt)
}

// Detach gives everything back before Thaw: the runtime's descriptors as
// Prepare-ready resources and, for a table this import restored, its
// handles released without reaping. It is the bootstrap's path when a step
// AFTER a successful import fails and the previous image must be re-run
// with the same bundle.
func (i *Imported) Detach() (*Salvage, error) {
	if i.active || i.thawed || i.detached {
		return nil, errors.New("agent image: already thawed or detached")
	}
	i.detached = true
	resources, err := detachDomains(i)
	envelope := handoff.State{Role: Role, Instance: i.Bundle.Instance, Generation: i.Bundle.Lineage.Generation}
	bundle := i.Bundle
	return &Salvage{State: envelope, Resources: resources, bundle: &bundle}, errors.Join(err, i.discardTable())
}

// domainDetacher transfers every descriptor out of an import that has never
// activated. It must release wrappers/budget without shutdown or Close of
// transferred business files. The concrete owners provide this capability.
// origin: hot-update S5 plan §5.3 / §5.4
type domainDetacher interface {
	Detach() ([]handoff.Resource, error)
}

func requireDomainDetach(b Bundle) error {
	if b.Flow != nil {
		if _, ok := any((*tunnel.Client)(nil)).(domainDetacher); !ok {
			return errors.New("agent image: flow owner cannot detach a frozen import")
		}
	}
	if b.SS != nil {
		if _, ok := any((*ssproxy.Server)(nil)).(domainDetacher); !ok {
			return errors.New("agent image: SS owner cannot detach a frozen import")
		}
	}
	return nil
}

func detachDomains(i *Imported) ([]handoff.Resource, error) {
	runtime, err := i.Runtime.Detach()
	groups := [][]handoff.Resource{runtime}
	var errs []error
	errs = append(errs, err)
	if i.Flow != nil {
		resources, err := i.Flow.Detach()
		groups = append(groups, resources)
		errs = append(errs, err)
	}
	if i.SS != nil {
		resources, err := i.SS.Detach()
		groups = append(groups, resources)
		errs = append(errs, err)
	}
	if i.Transfers != nil {
		frozen, err := i.Transfers.Freeze(i.target.TransferDeadline)
		errs = append(errs, domainDeferral("detach transfers", err))
		if err == nil && frozen != nil {
			salvage, err := frozen.DetachPristine()
			groups = append(groups, salvage.Resources)
			errs = append(errs, domainDeferral("detach transfers", err))
		} else if err == nil {
			errs = append(errs, errors.New("agent image: transfer detach returned no frozen owner"))
		}
	}
	groups = append(groups, i.transferState, i.transferTaken)
	i.transferState = nil
	i.transferTaken = nil
	resources, err := mergeTransferSalvage(groups...)
	return resources, errors.Join(append(errs, err)...)
}

// ResumeCarriers observes actual RESUME and data binding completion. A failed
// attempt keeps active responsibility and an unfinished calibration window.
// origin: hot-update S5 plan §7.1 / TestDomainCompleteActivationWindow
func (i *Imported) ResumeCarriers(deadline time.Time) error {
	if !i.rawReady {
		return errors.New("agent image: raw activation is incomplete")
	}
	if i.Bundle.Flow != nil && i.Flow == nil {
		return errors.New("agent image: flow owner is unavailable")
	}
	if i.carrierReady {
		return nil
	}
	if i.Flow != nil {
		if err := i.Flow.ResumeCarriers(deadline); err != nil {
			return domainDeferral("resume carriers", err)
		}
	}
	i.carrierReady = true
	i.carrierResumedAt, _ = procrequest.BootNow()
	finishActivation(i)
	return nil
}

// AuthorizeSS installs the fresh grant after carrier authorization. The bundle
// contains no cached keys that could implicitly open this permission gate.
// origin: hot-update S5 plan §6.5 / §7.1
func (i *Imported) AuthorizeSS(grant ssproxy.Grant) error {
	if !i.rawReady || i.Bundle.Flow != nil && !i.carrierReady {
		return errors.New("agent image: carriers are not authorized")
	}
	if i.SS == nil && i.Bundle.SS != nil {
		return errors.New("agent image: SS owner is unavailable")
	}
	if i.SS != nil {
		if err := i.SS.Authorize(grant); err != nil {
			return domainDeferral("authorize SS", err)
		}
	}
	i.ssReady = true
	i.ssAuthorizedAt, _ = procrequest.BootNow()
	finishActivation(i)
	return nil
}

// Only actual success receipts close the window. Retained metadata, an idle
// listener, or zero active flows still require their domain's permission gate.
func finishActivation(i *Imported) {
	if !i.ActivationComplete() {
		return
	}
	if i.thawedAt == 0 {
		i.thawedAt, _ = procrequest.BootNow()
	}
}

// ActivationComplete is data-plane readiness, independent of NATS ready or a
// durable upgrade commit. A partial raw activation has already become Active
// but cannot publish this completed receipt.
// origin: hot-update S5 plan §7.1
func (i *Imported) ActivationComplete() bool {
	return i.rawReady && (i.Bundle.Flow == nil || i.carrierReady) && (i.Bundle.SS == nil || i.ssReady) &&
		(i.Bundle.Transfers == nil || i.transferReady)
}

func transferOwnerMissing(manager TransferWorker) error {
	if manager == nil {
		return errors.New("agent image: transfer import returned no owner")
	}
	return nil
}

func readTransferState(b Bundle, files execsession.FileSource, tgt Target) (state TransferState, resources []handoff.Resource, err error) {
	if tgt.Transfers == nil || tgt.TransferDeadline <= 0 {
		return state, nil, errors.New("agent image: transfer import ports are unavailable")
	}
	source, ok := files.(interface{ Borrow() []handoff.Resource })
	if !ok {
		return state, nil, errors.New("agent image: transfer import requires a resource census")
	}
	ref := b.Transfers
	var borrowed []handoff.Resource
	found := false
	for _, resource := range source.Borrow() {
		if resource.ID == ref.ResourceID {
			if found || resource.Kind != xfervalue.ResourceState || len(resource.Files) != 1 || resource.Files[xfervalue.StateFileName] == nil || len(resource.Data) != 0 {
				return state, nil, fmt.Errorf("%w: inconsistent transfer state resource", ErrBundle)
			}
			found = true
		} else if resource.Kind == xfervalue.ResourceSource || resource.Kind == xfervalue.ResourceTmp || resource.Kind == xfervalue.ResourceParent {
			borrowed = append(borrowed, resource)
		}
	}
	if !found {
		return state, nil, fmt.Errorf("%w: transfer state resource is missing", ErrBundle)
	}
	data, file, err := sealedstate.Read(files, ref.ResourceID, xfervalue.StateFileName, int64(ref.EncodedBytes))
	if file != nil {
		resources = []handoff.Resource{{ID: ref.ResourceID, Kind: xfervalue.ResourceState, Files: map[string]*os.File{xfervalue.StateFileName: file}}}
	}
	if err != nil {
		return state, resources, domainDeferral("read transfers", err)
	}
	digest := sha256.Sum256(data)
	if uint64(len(data)) != ref.EncodedBytes || hex.EncodeToString(digest[:]) != ref.Digest {
		return state, resources, fmt.Errorf("%w: transfer digest or size differs", ErrBundle)
	}
	state, err = tgt.Transfers.Decode(data)
	if err != nil {
		return state, resources, domainDeferral("decode transfers", err)
	}
	if !bytes.Equal(state.Encoded, data) {
		// The projection must be of these exact sealed bytes.
		return state, resources, fmt.Errorf("%w: transfer projection differs from its sealed state", ErrBundle)
	}
	if state.Entries != ref.Count {
		return state, resources, fmt.Errorf("%w: transfer count differs", ErrBundle)
	}
	return state, resources, validateTransferState(b, state, borrowed)
}

func closeTransferResources(resources []handoff.Resource) error {
	var errs []error
	seen := make(map[*os.File]bool)
	for _, resource := range resources {
		for _, file := range resource.Files {
			if file != nil && !seen[file] {
				seen[file] = true
				errs = append(errs, file.Close())
			}
		}
	}
	return errors.Join(errs...)
}

// ActivationWindows reports CLOCK_BOOTTIME milestones since admission closed;
// zero means that stage has not completed. NATS registration is timed by the
// caller separately, after these data-plane milestones.
func (i *Imported) ActivationWindows() (raw, carrier, authority time.Duration) {
	elapsed := func(at int64) time.Duration {
		if at == 0 || i.FrozenAt == 0 {
			return 0
		}
		return time.Duration(at - i.FrozenAt)
	}
	return elapsed(i.rawActivatedAt), elapsed(i.carrierResumedAt), elapsed(i.ssAuthorizedAt)
}
