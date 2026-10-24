// Package agentboot holds the image a process was dispatched with from the
// first line of main() until it is activated, re-dispatched or abandoned.
// It is the ONE place that knows whether this process owns live business
// resources it has not yet activated, which is what every exit path between
// exec and control-ready has to consult before it lets the process die:
// a bundle that is neither activated nor handed back is a crash window
// (architecture §22.6).
//
// The package owns the order — held → imported → active → ready, with
// refused and lost as the ways out — and nothing else: decoding, validation
// and the domains are internal/agentimage, the descriptors are
// internal/handoff. It does not import internal/agent (layering gate): the
// agent hands it a Target and reads its phase; main hands it a logger.
//
// Every exported entry point is ctx-free (dataplane_lifetime gate ④): the
// image is data-plane state and a control context must never bound it.
// origin: docs/reviews/hot-update-agent-bootstrap-plan.md §1.3.
package agentboot

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Phase is where the dispatched image is on its way to control-ready. A
// string, not an iota family: it is logged and stamped into the upgrade
// marker, and the enum-switch gate has nothing to protect here (every
// switch below lists the members it acts on and treats the rest alike).
type Phase string

const (
	// PhaseNone: this process was not dispatched with an image.
	PhaseNone Phase = ""
	// PhaseHeld: handoff.Restore succeeded; no file has been taken.
	PhaseHeld Phase = "held"
	// PhaseRefused: the import was refused or a pristine import was detached;
	// the salvage holds every taken file.
	PhaseRefused Phase = "refused"
	// PhaseImported: every domain is built and still frozen.
	PhaseImported Phase = "imported"
	// PhaseActive: activation responsibility committed; a partial thaw failure
	// still owns its resources and can only roll back through a current cut.
	PhaseActive Phase = "active"
	// PhaseReady: the first register succeeded (control-ready).
	PhaseReady Phase = "ready"
	// PhaseLost: the image was abandoned; its tasks are lost (crash window).
	PhaseLost Phase = "lost"
)

// CrashWindow names why an image could not be carried (architecture §22.6
// table W1–W3); it is logged and stamped, never a wire value.
const (
	CrashWindowHandoffRestore   = "handoff_restore"
	CrashWindowNonDaemon        = "non_daemon_invocation"
	CrashWindowNoRollbackTarget = "bootstrap_no_rollback_target"
)

// remainderSource is what an in-process (FromCut) file source may implement
// so a refused import can still be repacked with the files it never took.
type remainderSource interface {
	Borrow() []handoff.Resource
}

// Image is the dispatched image and its phase. All methods are safe for
// concurrent use; the phase moves under the mutex, the heavy work (import,
// thaw) runs outside it on the single goroutine that drives the boot.
type Image struct {
	mu    sync.Mutex
	phase Phase
	// restored is the real dispatch (exec path); the fields below it are the
	// in-process source (FromCut: tests and the same-PID harness).
	restored  *handoff.Restored
	bundle    *agentimage.Bundle
	rows      []workload.Record
	files     execsession.FileSource
	resources []handoff.Resource
	auxiliary []handoff.Resource // taken private metadata, retained until activation or salvage
	// upgrade holds the verified S8 adjunct and its domain lease (or refused
	// custody) until activation commits or the image is abandoned.
	// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17
	upgrade *upgradecut.Custody

	imported *agentimage.Imported
	salvage  *agentimage.Salvage

	env         map[string]string
	crashWindow string
	summary     string
	generation  uint64
	tasks       int
	beganAt     int64 // CLOCK_BOOTTIME ns at Begin (the exec landed)
	restoredAt  int64 // when Restore started
	importedAt  int64 // when Restore returned
	activatedAt int64
	activated   bool // all directions ready; phase alone also covers partial activation
	activating  bool // a retry cannot overlap the previous activation attempt
}

var current struct {
	mu      sync.Mutex
	img     *Image
	claimed bool
}

// Begin consumes the dispatch environment as the FIRST thing a process does
// (before any goroutine, subprocess or environment consumer): with no
// dispatch it returns nil; with one it returns the held image and records
// it as the process's current image. envKeys are the lineage variables the
// caller will later consume (the instance id and routing name): they are
// snapshotted here, BEFORE anything consumes them, so a re-dispatch after a
// failure deep inside the agent's constructor still carries the lineage.
//
// A dispatch that cannot be read (W1) is a crash window, not a fatal error:
// the descriptors it left behind are closed (they are the only non-CLOEXEC
// files >=3 this process can inherit at this point; socket activation
// is unsupported and no other descriptor owner may run before Begin). The
// loss is logged with the signature the drills grep, and the process continues as an ordinary
// start. The returned image is in PhaseLost so the marker can say so.
func Begin(logger *slog.Logger, envKeys ...string) *Image {
	img := &Image{env: make(map[string]string, len(envKeys))}
	for _, key := range envKeys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			img.env[key] = v
		}
	}
	if now, err := procrequest.BootNow(); err == nil {
		img.beganAt = now
	}
	// Capture the inherited set before validation can protect some of it with
	// CLOEXEC and then refuse. Ordinary starts need no descriptor census.
	var foreign []foreignDescriptor
	var censusErr error
	if _, dispatched := os.LookupEnv("_TETHER_HANDOFF"); dispatched {
		foreign, censusErr = foreignDescriptors()
	}
	restored, err := handoff.Restore()
	if err == nil && restored == nil {
		return nil
	}
	// origin: hot-update S6 plan §4.1 / T03
	// The sealed role is authoritative. An unknown owner cannot be handed to
	// either constructor, even when argv or a chooser hint names a daemon.
	if err == nil && restored.State.Role != agentimage.Role && restored.State.Role != "broker" {
		_ = restored.Close()
		restored, err = nil, errors.New("agentboot: unsupported dispatch role")
	}
	if err != nil {
		closed := closeForeignDescriptors(foreign)
		img.phase, img.crashWindow = PhaseLost, CrashWindowHandoffRestore
		img.summary = "unreadable dispatch"
		if logger != nil {
			logger.Error("agent: dispatched image unreadable — tasks of the previous image are lost",
				"crash_window", img.crashWindow, "closed_fds", closed, "reason", "handoff_rejected",
				"fd_cleanup_error", censusErr)
		}
		current.mu.Lock()
		current.img = img
		current.mu.Unlock()
		return img
	}
	img.restored = restored
	img.phase = PhaseHeld
	img.generation = restored.State.Generation
	img.summary = fmt.Sprintf("role=%s instance=%s generation=%d resources=%d",
		restored.State.Role, restored.State.Instance, restored.State.Generation, len(restored.Resources))
	if logger != nil {
		logger.Info("agent: bootstrap holds a dispatched image", "image", img.summary)
	}
	current.mu.Lock()
	current.img = img
	current.mu.Unlock()
	return img
}

// Current returns the process's dispatched image (nil without one). It
// never consumes it: FinishBoot reads it after the agent has run.
func Current() *Image {
	current.mu.Lock()
	defer current.mu.Unlock()
	return current.img
}

// Claim hands the held image to the ONE constructor that will restore it;
// a second Claim (a second agent in the same process) gets nil, and so does
// any caller when the image is not held (lost, or already claimed).
func Claim() *Image {
	current.mu.Lock()
	defer current.mu.Unlock()
	i := current.img
	if i == nil || current.claimed {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != PhaseHeld {
		return nil
	}
	// origin: hot-update S6 plan §4.1 / T03
	// A wrong argv must not let an agent constructor consume broker files.
	if i.restored != nil && i.restored.State.Role != agentimage.Role {
		return nil
	}
	current.claimed = true
	return i
}

// TakeRole transfers the envelope Begin already validated to its sole role
// owner. A role mismatch leaves it held for Claim or the matching dispatcher;
// neither argv nor an environment hint can authorize a second Restore.
// origin: hot-update S6 plan §2.1 / §4.1 / T03
func TakeRole(role string) (*handoff.Restored, error) {
	if role != agentimage.Role && role != "broker" {
		return nil, errors.New("agentboot: unsupported requested role")
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	i := current.img
	if i == nil || current.claimed {
		return nil, nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != PhaseHeld || i.restored == nil || i.restored.State.Role != role {
		return nil, nil
	}
	// Removing the agent wrapper prevents its FinishBoot/Abandon path from
	// closing files now owned by broker boot, including pristine salvage.
	restored := i.restored
	i.restored = nil
	i.phase = PhaseNone
	current.claimed, current.img = true, nil
	return restored, nil
}

// FromCut wraps an in-process cut (a bundle, its rows, a file source and the
// cut's resources — borrowed, never closed by the image) as a held image:
// tests and the same-PID harness use it to drive the exact bootstrap code
// the exec path runs.
func FromCut(b agentimage.Bundle, rows []workload.Record, files execsession.FileSource, resources []handoff.Resource) *Image {
	img := &Image{phase: PhaseHeld, bundle: &b, rows: rows, files: files, resources: resources,
		generation: b.Lineage.Generation, env: map[string]string{}}
	if now, err := procrequest.BootNow(); err == nil {
		img.beganAt = now
	}
	img.summary = fmt.Sprintf("in-process instance=%s generation=%d tasks=%d", b.Instance, b.Lineage.Generation, len(b.Runtime.Tasks))
	return img
}

// Env is the lineage snapshot Begin took before anything consumed it.
func (i *Image) Env() map[string]string {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make(map[string]string, len(i.env))
	for k, v := range i.env {
		out[k] = v
	}
	return out
}

// Phase reports where the image is.
func (i *Image) Phase() Phase {
	if i == nil {
		return PhaseNone
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.phase
}

// Activated distinguishes completed pump activation from active resource
// responsibility. The latter already forbids replaying the pristine bundle.
// origin: hot-update S4 internal review rv-fd rv-fd-4
func (i *Image) Activated() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.activated
}

// Generation is the image's generation (0 before Begin).
func (i *Image) Generation() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.generation
}

// Tasks is the number of exec tasks the image carries (known after Restore).
func (i *Image) Tasks() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.tasks
}

// CrashWindow names the crash window the image fell into ("" otherwise).
func (i *Image) CrashWindow() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.crashWindow
}

// Summary is what a log line may say about the image: never the bundle.
func (i *Image) Summary() string {
	if i == nil {
		return ""
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.summary
}

// Imported returns the frozen import (nil unless imported/active/ready).
func (i *Image) Imported() *agentimage.Imported {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.imported
}

// Restore imports every domain into tgt: held → imported. On refusal the
// salvage keeps the taken files and the phase becomes refused; the caller
// re-dispatches with Repack. Decoding is pure and runs first, so a bundle
// this binary cannot read is refused before a single file is taken.
func (i *Image) Restore(tgt agentimage.Target) (*agentimage.Imported, error) {
	i.mu.Lock()
	if i.phase != PhaseHeld {
		i.mu.Unlock()
		return nil, fmt.Errorf("agentboot: restore in phase %q", i.phase)
	}
	// origin: hot-update S8 plan §6.1 / §11.2 P17. The adjunct is required: a
	// reader that has not verified it cannot take one business descriptor.
	if _, present, err := upgradecut.Find(borrowedResources(i)); err != nil || present && i.upgrade == nil {
		i.mu.Unlock()
		return nil, errors.Join(errUpgradeCutNotImported, err)
	}
	if now, err := procrequest.BootNow(); err == nil {
		i.restoredAt = now
	}
	restored, bundle, rows, files := i.restored, i.bundle, i.rows, i.files
	i.mu.Unlock()
	var (
		imp     *agentimage.Imported
		salvage *agentimage.Salvage
		err     error
	)
	if restored != nil {
		imp, salvage, err = agentimage.Restore(restored, tgt)
	} else {
		imp, salvage, err = agentimage.Import(*bundle, rows, files, tgt)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if now, err := procrequest.BootNow(); err == nil {
		i.importedAt = now
	}
	if err != nil {
		i.phase, i.salvage = PhaseRefused, salvage
		return nil, err
	}
	i.phase, i.imported = PhaseImported, imp
	i.tasks, i.generation = len(imp.Bundle.Runtime.Tasks), imp.Bundle.Lineage.Generation
	return imp, nil
}

// Activate thaws the import: imported → active. It returns the freeze
// window (the exporter's cut to this thaw, on the exporter's clock), the
// time the process spent before Restore (exec, Go init, boot shim, Cobra —
// "shim") and the import itself, for the calibration log line. A failed
// preactivation keeps a pristine frozen import. Once the Manager may move,
// even a failed thaw keeps active ownership; callers retry or cut current state.
func (i *Image) Activate() (window, shim, imported time.Duration, err error) {
	i.mu.Lock()
	if (i.phase != PhaseImported && i.phase != PhaseActive) || i.activated || i.activating {
		phase := i.phase
		i.mu.Unlock()
		return 0, 0, 0, fmt.Errorf("agentboot: activate in phase %q", phase)
	}
	imp := i.imported
	// origin: hot-update S4 plan §4.2 — active also permits a later retry,
	// but never a concurrent one while the first actor set is still moving.
	i.activating = true
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		i.activating = false
		i.mu.Unlock()
	}()
	// origin: hot-update S8 plan §5.1 / IL05 / DC05 / M08. Adopt the inherited
	// domain lease while the import is still pristine and before
	// closeAuxiliary; a refused Take keeps its custody for salvage.
	if err := takeUpgradeLease(i); err != nil {
		return 0, 0, 0, fmt.Errorf("agentboot: take upgrade lease: %w", err)
	}
	if err := imp.PreActivate(); err != nil {
		return 0, 0, 0, fmt.Errorf("agentboot: prepare activation: %w", err)
	}
	// origin: hot-update S4 plan §4.2 / T23; hot-update S5 plan §5.4 / T34
	// Commit before Manager.Thaw or any flow/SS actor can advance applied
	// bytes or nonces. A later actor or carrier failure cannot restore the
	// original bundle; only a new cut of the current owner set can recover.
	i.mu.Lock()
	i.phase = PhaseActive
	// Private callback metadata has already been restored. Commit its closure
	// with active responsibility, even if a later actor refuses activation:
	// that failure must cut current owners and cannot repack the old envelope.
	// origin: hot-update S5 plan §5.4 / §7.2 / T34
	closeAuxiliary(i)
	i.mu.Unlock()
	if terr := imp.Thaw(); terr != nil {
		return 0, 0, 0, fmt.Errorf("agentboot: thaw: %w", terr)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	// origin: hot-update S3 plan §1.3 / §1.6. Auxiliary resources are
	// accepted by handoff, but only the imported domains acquire ownership.
	// Preserve the remainder for held/refused/imported rollback; after Thaw
	// the same bundle cannot be replayed, so release every untaken file now.
	if i.restored != nil {
		_ = i.restored.Close()
		i.restored = nil
	}
	i.phase = PhaseActive
	i.activated = true
	if now, err := procrequest.BootNow(); err == nil {
		i.activatedAt = now
	}
	shim = time.Duration(i.restoredAt - i.beganAt)
	imported = time.Duration(i.importedAt - i.restoredAt)
	return imp.FreezeWindow(), shim, imported, nil
}

// MarkReady records control-ready (the first register succeeded).
func (i *Image) MarkReady() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase == PhaseActive && i.activated {
		i.phase = PhaseReady
	}
}

// Repack prepares a re-dispatch of the SAME bundle to another image (the
// previous binary): from held, every file is still inside the handoff
// bundle; from refused, the salvage's taken files are merged with the
// bundle's untaken remainder; from imported (never thawed), the runtime is
// detached first. Ownership is borrowed throughout — an exec that fails
// leaves the image exactly as it was. Active and later phases cannot be
// repacked: the running agent cuts its current state instead.
func (i *Image) Repack(extra ...handoff.Resource) (*handoff.Prepared, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17. A pristine redispatch
	// carries an adopted lease and its adjunct onward: it never unlocks them.
	custody, err := i.upgrade.Resources()
	defer upgradecut.CloseResources(custody)
	if err != nil {
		return nil, err
	}
	extra, err = handoff.MergeResources(i.auxiliary, custody, extra)
	if err != nil {
		return nil, err
	}
	// origin: hot-update S5 plan §5.4 / T34
	// PreActivate runs outside this mutex while the phase is still imported.
	// Detaching then would transfer files out from under the activation owner.
	if i.activating {
		return nil, errors.New("agentboot: repack during activation")
	}
	switch i.phase {
	case PhaseHeld:
		if i.restored != nil {
			return i.restored.Repack(extra...)
		}
		resources, err := handoff.MergeResources(i.resources, extra)
		if err != nil {
			return nil, err
		}
		data, err := agentimage.Encode(*i.bundle)
		if err != nil {
			return nil, err
		}
		return handoff.Prepare(handoff.State{Role: agentimage.Role, Instance: i.bundle.Instance, Generation: i.bundle.Lineage.Generation, Data: data}, resources)
	case PhaseImported:
		salvage, err := i.imported.Detach()
		i.imported = nil
		i.phase, i.salvage = PhaseRefused, salvage
		if err != nil {
			return nil, fmt.Errorf("agentboot: detach before repack: %w", err)
		}
		return i.repackRefusedLocked(extra)
	case PhaseRefused:
		return i.repackRefusedLocked(extra)
	case PhaseNone, PhaseActive, PhaseReady, PhaseLost:
	}
	return nil, fmt.Errorf("agentboot: repack in phase %q", i.phase)
}

// PrepareCurrent prepares an active image's next cut for re-dispatch. The
// caller freezes the complete current owner set and retains the cut so an
// exec failure can thaw it. The boot image supplies only the lineage check:
// its original bundle is never a source of offsets, nonces or descriptors.
// validate retains the caller's final reader/domain check on that same export.
// origin: hot-update S5 plan §5.4 / §7.1 / T34
func PrepareCurrent(i *Image, cut *agentimage.Frozen, validate func(agentimage.Bundle) error, extra ...handoff.Resource) (*handoff.Prepared, error) {
	if i == nil || cut == nil {
		return nil, errors.New("agentboot: current re-dispatch requires an image and cut")
	}
	i.mu.Lock()
	phase, activating, generation, imp := i.phase, i.activating, i.generation, i.imported
	i.mu.Unlock()
	if (phase != PhaseActive && phase != PhaseReady) || activating || imp == nil {
		return nil, fmt.Errorf("agentboot: current re-dispatch in phase %q", phase)
	}
	if generation == math.MaxUint64 {
		return nil, errors.New("agentboot: current re-dispatch generation exhausted")
	}
	instance := imp.Bundle.Instance
	// Validate the final re-export, not a Bundle cached at Freeze. Late revoke
	// and cleanup decisions belong to the current cut right up to Prepare.
	return cut.PrepareChecked(func(b agentimage.Bundle) error {
		if b.Instance != instance || b.Lineage.Generation != generation+1 {
			return errors.New("agentboot: current re-dispatch lineage mismatch")
		}
		if validate != nil {
			return validate(b)
		}
		return nil
	}, extra...)
}

func (i *Image) repackRefusedLocked(extra []handoff.Resource) (*handoff.Prepared, error) {
	if i.salvage == nil {
		return nil, errors.New("agentboot: refused image without a salvage")
	}
	var remainder []handoff.Resource
	switch {
	case i.restored != nil:
		remainder = i.restored.Borrow()
	case i.files != nil:
		if src, ok := i.files.(remainderSource); ok {
			remainder = src.Borrow()
		}
	}
	return i.salvage.Prepare(append(remainder, extra...)...)
}

// Abandon gives up on the image (crash window): every descriptor this
// process holds for it is closed so the previous image's children see EOF
// and EPIPE now instead of hanging on a pipe nobody drains. The log line
// carries the summary and the reason, never the bundle.
func (i *Image) Abandon(logger *slog.Logger, window, reason string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase == PhaseLost {
		return
	}
	// origin: hot-update S5 plan §5.4 / T34
	// The activation driver owns even a still-pristine preactivation attempt;
	// its eventual result decides whether salvage or current-cut recovery is safe.
	if i.activating {
		return
	}
	// origin: hot-update S4 plan §4.2 — bootstrap cleanup cannot drop the
	// only owner after a partial activation. The agent owns current-cut
	// recovery and eventual active-runtime cleanup from this boundary on.
	if i.phase == PhaseActive || i.phase == PhaseReady {
		return
	}
	if i.imported != nil {
		if salvage, err := i.imported.Detach(); err == nil && salvage != nil {
			i.salvage = salvage
		}
		i.imported = nil
	}
	closeAuxiliary(i)
	_ = i.upgrade.Close() // terminal cleanup is the lease's final owner
	if i.salvage != nil {
		_ = i.salvage.Close()
	}
	if i.restored != nil {
		_ = i.restored.Close()
	}
	// FromCut's resources are borrowed from the cut (Repack only duplicates
	// them); their owner closes them.
	i.phase, i.crashWindow = PhaseLost, window
	if logger != nil {
		logger.Error("agent: image abandoned — tasks of the previous image are lost",
			"crash_window", window, "reason", reason, "image", i.summary)
	}
}

// Reset forgets the process's current image (tests only; production
// processes dispatch at most once per exec).
func Reset() {
	current.mu.Lock()
	defer current.mu.Unlock()
	current.img, current.claimed = nil, false
}
