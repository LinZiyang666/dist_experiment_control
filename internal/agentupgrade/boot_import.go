// boot_import.go is the target side of an S8 cut before control-ready: the
// adjunct is verified against the configured key, the running bytes and the
// accepted journal before Restore takes a business descriptor; the inherited
// lease moves to the commit owner only after activation; a constructor that
// fails before activation redispatches the pristine image to the recorded
// source bytes and never enters the v1 marker recovery.
// origin: hot-update S8 plan §3.3 / §5.1 / §6.1 / §8.1 / §11.2 P14 P17; S8 FIX-AGENTWIRE
package agentupgrade

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradehost"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ImportCut verifies a carried adjunct while the image is still held. It
// returns the ports unchanged for a legacy dispatch or an unconfigured agent;
// Restore then refuses an S8 dispatch it could not verify. A forward arrival
// also captures the watchdog's immutable fence and absolute deadline from the
// accepted journal, so a late timer can never adopt another transaction.
func ImportCut(img *agentboot.Image, ports *Dependencies, running func() (string, error)) (*Dependencies, error) {
	if img == nil || ports == nil || ports.Store == nil {
		return ports, nil
	}
	var record upgradetx.Record
	var forward bool
	cut, err := img.ImportUpgradeCut(func(state upgradecut.State) error {
		sha, err := running()
		if err != nil || state.Attempt.Key != ports.Key {
			return refusal(upgradetx.CodeRejected, "upgrade cut does not belong to this agent", false)
		}
		// A pristine redispatch returns the cut to its exporter's bytes; that
		// return is imported so its business survives, never certified.
		forward = sha == state.TargetSHA
		if !forward && sha != state.SourceSHA {
			return refusal(upgradetx.CodeRejected, "upgrade cut names other image bytes", false)
		}
		view, err := ports.Store.Load(ports.Key, state.Attempt.TxID)
		if err != nil {
			return err
		}
		a := current(view.Historical)
		// Only an accepted, unsettled attempt with a durable activation
		// intent dispatched this cut; a settled or pristine record did not.
		if !upgradetx.SameCut(a.Handle, state.Attempt) || state.Attempt.Revision > view.Historical.Revision ||
			a.SourceSHA != state.SourceSHA || a.TargetSHA != state.TargetSHA ||
			(a.CutStage != upgradetx.CutPartialActive && a.CutStage != upgradetx.CutActive) ||
			a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack || a.Phase == upgradetx.PhaseCancelled || a.Phase == upgradetx.PhaseAbandoned {
			return refusal(upgradetx.CodeRejected, "upgrade cut is not the accepted activation", false)
		}
		record = view.Historical
		return nil
	})
	if err != nil || cut == nil {
		return ports, err
	}
	// The sampler and kernel OFD witnesses must name this exact imported cut.
	// origin: S8 internal review activation-and-route-migration F4
	if drillobserve.Hook != nil {
		if err := drillobserve.BindRuntimeCut(cut.Attempt.CutID); err != nil {
			return ports, err
		}
	}
	deps := *ports
	deps.Cut = cut
	if forward {
		captureWatchdog(&deps, record, *cut)
	}
	return &deps, nil
}

// Capture only the accepted journal's absolute deadline and immutable cut.
// Boot/import duration cannot grant a second handover window.
// origin: S8 internal review activation-and-route-migration F10; S8 plan §3.3
func captureWatchdog(deps *Dependencies, record upgradetx.Record, cut upgradecut.State) {
	deps.WatchdogFence, deps.WatchdogDeadline, deps.WatchdogReverse = agentupgradeport.CaptureWatchdog(record, cut)
}

// OpenJournal opens an agent's canonical journal with the legacy-projection
// reader: P17 made every v1 writer refuse a v2 journal, so a terminal v1
// marker is admitted as history while a pending or blocked one still refuses.
// origin: hot-update S8 plan §3.1 / §3.3; upgradestore/codec.go precondition
func OpenJournal(path, guardPath string, key upgradetx.Key, owner upgradetx.Owner, sourceGeneration uint64) (*upgradestore.Store, error) {
	return upgradestore.Open(upgradestore.Options{Path: path, GuardPath: guardPath, AllowLegacyMigration: true, Key: key,
		Owner: owner, SourceGeneration: sourceGeneration, Transition: upgradetx.Transition, ParentReservation: func(parent, slot string) (int64, error) { return upgradehost.ParentDeadline(key, owner, parent, slot) }})
}

// Complete supplies the adapters the integration leaves to the agent: its own
// runtime port and, only for an imported cut, that cut's completion registry.
// Ports already configured are kept; a store is required to supply anything.
func Complete(ports *Dependencies, host func(*Dependencies) agentupgradeport.Host) (*Dependencies, error) {
	if ports == nil || ports.Store == nil || ports.Runtime != nil && ports.Registry != nil {
		return ports, nil
	}
	deps := *ports
	if deps.Runtime == nil {
		deps.Runtime = agentupgradeport.NewRuntime(host(&deps))
	}
	if deps.Registry == nil {
		deps.Registry = agentupgradeport.UnwiredRegistry()
		if deps.Cut != nil {
			registry, err := agentupgradeport.CutRegistry(deps.Runtime, deps.Store, *deps.Cut)
			if err != nil {
				return nil, err
			}
			deps.Registry = registry
		}
	}
	return &deps, nil
}

// AdoptLease moves the activated image's lease to the commit owner. Before
// activation committed, the image keeps it for pristine salvage.
func AdoptLease(img *agentboot.Image, ports *Dependencies) {
	if img == nil || ports == nil || ports.Lease != nil {
		return
	}
	ports.Lease = img.TakeUpgradeLease()
}

// FinishTransactionBoot is FinishBoot's S8 branch for a held, refused or
// imported image. The v1 marker recovery refuses a v2 journal and would
// abandon the carried tasks; instead the target redispatches the SAME image
// (Repack carries the adjunct and lease) to the recorded source bytes. With no
// verifiable source pin, or when this process already IS the source, it can
// only abandon (crash window) and leaves the journal to recovery.
func FinishTransactionBoot(img *agentboot.Image, state *upgradecut.State, ports *Dependencies, running string, logger *slog.Logger, class string, redispatch func(*handoff.Prepared) func(string) error) {
	source := ""
	if state != nil && ports != nil && ports.Store != nil && state.Attempt.Key == ports.Key && running == state.TargetSHA && running != state.SourceSHA {
		source = SourcePin(ports.Store, *state)
	}
	if source == "" {
		img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, class)
		return
	}
	prepared, err := img.Repack()
	if err != nil {
		img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, class)
		return
	}
	if logger != nil {
		logger.Error("agent: transaction boot failed before activation; returning the pristine image to its source", "reason", class)
	}
	_ = redispatch(prepared)(source) // returns only on failure
	_ = prepared.Close()
	img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, class)
}

// SourcePin is the accepted attempt's published source slot, verified by its
// bytes. A matching SHA elsewhere on disk is never adopted.
func SourcePin(journal upgradetx.Store, state upgradecut.State) string {
	view, err := journal.Load(state.Attempt.Key, state.Attempt.TxID)
	if err != nil || !upgradetx.SameCut(current(view.Historical).Handle, state.Attempt) {
		return ""
	}
	for _, pin := range view.Metadata.Pins {
		if filepath.IsAbs(pin) && filepath.Base(pin) == state.SourceSHA {
			if sha, err := agentimage.SHA256OfFile(pin); err == nil && sha == state.SourceSHA {
				if info, err := os.Stat(pin); err == nil && info.Mode().IsRegular() {
					return pin
				}
			}
		}
	}
	return ""
}

// BindRuntime completes missing runtime owners before binding the configured
// component identity; callers keep import refusals at the image boundary.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
func BindRuntime(ports *Dependencies, sid, nid string, uid uint32, host func(*Dependencies) agentupgradeport.Host) (*Dependencies, upgradeport.Endpoint, string, error) {
	completed, err := Complete(ports, host)
	if err != nil {
		return nil, nil, "", err
	}
	return Bind(completed, sid, nid, uid)
}
