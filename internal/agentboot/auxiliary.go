// auxiliary.go owns the image's auxiliary handoff custody: the optional
// sealed private metadata (S5) and the required S8 upgrade cut with its
// inherited domain lease. Neither is a business domain; both survive a
// pristine repack and are released only by activation commit or abandon.
// Split from image.go by responsibility.
// origin: hot-update S5 plan §5.1 / §7.2; hot-update S8 plan §5.1 / §6.1 / §11.2 P17
package agentboot

import (
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

var (
	errUpgradeCutNotImported = errors.New("agentboot: required upgrade cut was not imported")
	errUpgradeCutRefused     = errors.New("agentboot: upgrade cut refused")
)

// ReadSealed takes optional private owner metadata while the image is held.
// Taken files remain borrowed by pristine repacks; only activation or abandon
// closes them. Absence preserves compatibility with exec/PTY-only exporters.
// origin: hot-update S5 plan §5.1 / §7.2
func (i *Image) ReadSealed(id string, limit int64) ([]byte, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != PhaseHeld {
		return nil, errors.New("agentboot: private metadata read outside held phase")
	}
	resources, source := i.resources, i.files
	if i.restored != nil {
		resources, source = i.restored.Resources, i.restored
	}
	for _, resource := range resources {
		if resource.ID != id {
			continue
		}
		data, file, err := sealedstate.Read(source, id, "state", limit)
		if file != nil {
			i.auxiliary = append(i.auxiliary, handoff.Resource{ID: id, Kind: resource.Kind,
				Files: map[string]*os.File{"state": file}})
		}
		return data, err
	}
	return nil, nil
}

func closeAuxiliary(i *Image) {
	for _, resource := range i.auxiliary {
		for _, file := range resource.Files {
			_ = file.Close()
		}
	}
	i.auxiliary = nil
}

// ImportUpgradeCut verifies the required S8 adjunct while the image is held,
// before Restore takes one business descriptor; (nil, nil) is a legacy
// dispatch. The agent role and the same-PID origin are intrinsic. expect
// supplies the facts only the caller has (configured Key, running image SHA,
// the journal's accepted cut and incarnation); without it the adjunct cannot
// be accepted. Every refusal leaves the image held for pristine redispatch.
// expect runs without the image lock and must not call back into the image.
// origin: hot-update S8 plan §6.1 / §11.2 P17
func (i *Image) ImportUpgradeCut(expect func(upgradecut.State) error) (*upgradecut.State, error) {
	i.mu.Lock()
	resources, held := borrowedResources(i), i.phase == PhaseHeld && i.upgrade == nil
	i.mu.Unlock()
	if !held {
		return nil, errUpgradeCutRefused
	}
	custody, err := upgradecut.Admit(resources, upgradetx.ComponentAgent, expect)
	if custody == nil || err != nil {
		return nil, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != PhaseHeld || i.upgrade != nil {
		return nil, errUpgradeCutRefused
	}
	i.upgrade = custody
	return custody.State(), nil
}

// CarriedUpgradeCut reports whether this dispatch carries the S8 adjunct,
// imported or not, without taking a file. A present but unreadable adjunct is
// (nil, true): FinishBoot must keep any S8 dispatch out of the v1 recovery.
// origin: hot-update S8 plan §3.3 / §8.1; S8 FIX-AGENTWIRE
func (i *Image) CarriedUpgradeCut() (*upgradecut.State, bool) {
	if i == nil {
		return nil, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.upgrade != nil {
		return i.upgrade.State(), true
	}
	if i.phase != PhaseHeld && i.phase != PhaseRefused && i.phase != PhaseImported {
		return nil, false
	}
	state, present, err := upgradecut.Find(borrowedResources(i))
	if err != nil || !present {
		return nil, present
	}
	return &state, true
}

// UpgradeCut returns a copy of the imported adjunct, or nil without one.
func (i *Image) UpgradeCut() *upgradecut.State {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.upgrade.State()
}

// TakeUpgradeLease transfers the adopted domain lease to the S8 commit owner.
// Before activation committed responsibility, and for refused custody, the
// image keeps it: pristine salvage must still hand the same OFD onward.
// origin: hot-update S8 plan §5.1 / M08 / §11.2 P17
func (i *Image) TakeUpgradeLease() upgradelock.Lease {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.upgrade.Release(i.phase == PhaseActive || i.phase == PhaseReady)
}

// CutStage maps the boot phases onto the S8 activation stages. Import and
// preactivation are pristine; committing PhaseActive before the first pump
// moves is partial_active until every direction is activated. From that point
// recovery exports the current owners and never repacks the original bundle.
// origin: hot-update S8 plan §4.1 / §8.1 / §11.2 P17 mutant "partial uses old snapshot"
func (i *Image) CutStage() upgradetx.CutStage {
	if i == nil {
		return upgradetx.CutNone
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	switch i.phase {
	case PhaseHeld, PhaseRefused, PhaseImported:
		return upgradetx.CutPristine
	case PhaseActive:
		if i.activated {
			return upgradetx.CutActive
		}
		return upgradetx.CutPartialActive
	case PhaseReady:
		return upgradetx.CutActive
	case PhaseNone, PhaseLost:
	}
	return upgradetx.CutNone
}

// borrowedResources is the untaken handoff view; the caller holds i.mu.
func borrowedResources(i *Image) []handoff.Resource {
	if i.restored != nil {
		return i.restored.Borrow()
	}
	if src, ok := i.files.(remainderSource); ok {
		return src.Borrow()
	}
	return i.resources
}

// takeUpgradeLease adopts the inherited lease once (upgradecut.Custody keeps a
// refused Take final, so a retry cannot double-own the same descriptor).
func takeUpgradeLease(i *Image) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	switch {
	case i.upgrade == nil:
		return nil
	case i.restored != nil:
		return i.upgrade.Take(i.restored.TakeFile)
	case i.files != nil:
		return i.upgrade.Take(i.files.TakeFile)
	}
	return errUpgradeCutRefused
}
