// Legacy writer adapters keep v1 evidence isolated from the sole v2 writer.
// They capture marker identity before waiting and retain all historical guards.
// origin: hot-update S8 plan §3.3 / §12.1 / P14
package agentupgrade

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// LegacyPort exposes concrete writer responsibilities without importing Agent.
// Mu and Stop are shared with Run; callbacks retain the data owners' own locks.
type LegacyPort struct {
	SID, NID, InstanceID, BootProof string
	ExePath                         func() (string, error)
	RunningSHA                      func() (string, error)
	Mu                              *sync.Mutex
	Store                           binaryupgrade.Store
	Logger                          *slog.Logger
	AdmissionPending                func(string) bool
	ReleaseAdmission                func(string)
	Stop                            *context.CancelFunc
	RollbackInFlight                *atomic.Bool
	ImagePhase                      func() agentboot.Phase
	RollbackImage                   func(string, *binaryupgrade.Marker, string, bool) bool
	Now                             func() time.Time
	Exec                            func(string) error
	NoExit, CustomExec              bool
}

func SameLegacyMarker(expected, actual *binaryupgrade.Marker) bool {
	return expected != nil && actual != nil && expected.UpgradeID == actual.UpgradeID &&
		expected.NewSHA == actual.NewSHA && expected.PrevSHA == actual.PrevSHA &&
		expected.TargetSID == actual.TargetSID && expected.TargetNID == actual.TargetNID && expected.TargetInstance == actual.TargetInstance
}

// markerTargetsThisAgent is the instance-identity half of the commit proof
// (external review F1): a shared-binary sibling with a different sid/nid must
// neither report nor transition an upgrade that targeted someone else.
func TargetsLegacy(p LegacyPort, m *binaryupgrade.Marker) bool {
	// When the marker names an instance, only THAT instance may claim it. This
	// is what keeps the F1 sibling guard discriminating now that one image can
	// run several times under one basename: without it every clone answers
	// "yes", so the first one to boot reports someone else's upgrade outcome
	// and clears the marker belonging to the instance that actually upgraded.
	//
	// An empty TargetInstance is a marker written before this field existed;
	// falling back to the (sid, nid) comparison is exactly today's behaviour.
	// THE BOOT PROOF IS THE OTHER ADMISSIBLE LINEAGE, and it is what keeps a
	// legitimate upgrade from being stranded (external review F7 / round-2
	// B7). TargetInstance pins a PROCESS lineage, which survives
	// syscall.Exec and nothing else: a host reboot, `systemctl restart`, an
	// OOM kill or a fatal Run inside the upgrade window starts the successor
	// with a clean environment and a new id, and it would then disown a
	// marker it is in fact the continuation of — no watchdog armed, no
	// outcome reported, nothing to GC it, and some unrelated boot weeks
	// later silently rolling the node back.
	//
	// The boot proof is per-TRANSACTION (the marker's own UpgradeID) and is
	// only held by a process that actually traversed the pending-boot
	// decision for it. It is deliberately weaker than the instance id: on a
	// SHARED binary a sibling can also traverse it, which is the residual
	// limitation F7 identified and which no value available here can close.
	// It is documented as a limitation of shared-binary multi-instance
	// deployments rather than hidden behind a comment claiming otherwise.
	return m.TargetSID == p.SID && m.TargetNID == p.NID &&
		(m.TargetInstance == "" || m.TargetInstance == p.InstanceID ||
			p.BootProof != "" && p.BootProof == m.UpgradeID)
}

// The target, boot count, process-local transaction proof and executing image
// are independent guards. Both report and commit use this exact conjunction.
// origin: hot-update S8 plan §3.3; upgrade-safety external review F1/F9.
func LegacyCommitProof(p LegacyPort, m *binaryupgrade.Marker) bool {
	if !TargetsLegacy(p, m) || m.BootCount == 0 || m.UpgradeID == "" || p.BootProof != m.UpgradeID {
		return false
	}
	sha, err := p.RunningSHA()
	return err == nil && sha == m.NewSHA
}

func LegacyReport(p LegacyPort, m *binaryupgrade.Marker) (state, detail string) {
	if m == nil {
		return "", ""
	}
	switch m.State {
	case binaryupgrade.StatePending:
		// Four-part commit proof (internal review S1 + external reviews F1/F9),
		// each closing a distinct impersonation:
		//   1. target identity — a co-hosted sibling with another sid/nid
		//      must not claim an upgrade that targeted someone else;
		//   2. BootCount > 0 — the staged image went through decideBoot at
		//      least once (the install-time marker is always written with 0,
		//      so a register racing the flip→exec window proves nothing);
		//   3. process-local boot proof == UpgradeID — this process traversed
		//      the pending boot shim for this exact transaction;
		//   4. RUNNING-IMAGE sha == NewSHA — this very process executes the
		//      staged bytes. Hashing the on-disk path here would be
		//      borrowable: after the flip EVERY process on the host sees
		//      NewSHA at the path while still running the old image.
		if !LegacyCommitProof(p, m) {
			return "", ""
		}
		detail := fmt.Sprintf("upgraded %s -> %s", m.PrevVersion, m.NewVersion)
		if m.Generation != 0 || m.ResourcePhase != "" {
			detail += fmt.Sprintf("; image gen %d: %d tasks carried, freeze window %dms",
				m.Generation, m.ImportedResources, m.FreezeWindowMS)
		}
		return binaryupgrade.StateCommitted, detail
	case binaryupgrade.StateRolledBack, binaryupgrade.StateRollbackFailed:
		if !TargetsLegacy(p, m) {
			return "", ""
		}
		return m.State, m.Detail
	default:
		return "", ""
	}
}

func CommitLegacyReport(p LegacyPort, reported string, expected *binaryupgrade.Marker) {
	if drillobserve.Hook != nil {
		drillobserve.Hook(drillobserve.Event{Kind: "register-applied", Domain: "agent"})
	}
	if reported == "" || expected == nil {
		return
	}
	exePath, err := p.ExePath()
	if err != nil {
		return
	}
	p.Mu.Lock()
	defer p.Mu.Unlock()
	// The host-wide flock (external review F1) serializes this
	// read-decide-write against SIBLING PROCESSES sharing the binary; the
	// in-process upgradeMu above serializes it against our own watchdog.
	lerr := binaryupgrade.WithFileLock(exePath, true, func() error {
		markerPath := binaryupgrade.MarkerPath(exePath)
		m, merr := ReadLegacyMarker(markerPath)
		if merr != nil || m == nil || !SameLegacyMarker(expected, m) {
			//nolint:nilerr // The closure's error return propagates LOCK failures only; a corrupt
			// marker is by contract "idle" — there is nothing for this register to commit or clear.
			return nil
		}
		switch {
		// A failed directory fsync may leave a visible committed marker while
		// PTY admission remains closed. Retry the durable write on register;
		// the visible state alone cannot release this instance's restriction.
		// origin: hot-update S4 plan §5.2 / T31.
		case (m.State == binaryupgrade.StatePending || m.State == binaryupgrade.StateCommitted && p.AdmissionPending(m.UpgradeID)) && reported == binaryupgrade.StateCommitted:
			// Same four-part proof as upgradeRegisterReport (S1 + F1/F9):
			// target identity, boot count, process-local upgrade ID, running-image sha.
			if !LegacyCommitProof(p, m) {
				return nil
			}
			m.State = binaryupgrade.StateCommitted
			if err := WriteLegacyMarker(p.Store, markerPath, m); err != nil {
				// origin: internal review S14 — honest failure note: the marker
				// stays pending and the watchdog stays armed, so a healthy,
				// registered upgrade may still be rolled back at the deadline.
				// That is the SAFE direction (old binary, node stays reachable);
				// deliberately not stopping the watchdog here, because a pending
				// marker with no watchdog has no owner left to converge it.
				// origin: hot-update S5 plan §9.2 / DRILL-OBSERVER. Capture the actual filesystem error.
				if drillobserve.Hook != nil {
					e := drillobserve.Event{Kind: "commit-write-failed", Domain: "agent", Values: [16]uint64{uint64(m.Generation)}}
					if errors.Is(err, os.ErrPermission) {
						e.Values[1] = 1
					}
					if (*p.Stop) != nil {
						e.Values[2] = 1
					}
					drillobserve.Hook(e)
				}
				p.Logger.Error("agent: upgrade commit: marker write failed — watchdog stays armed and may roll back at the deadline", "err", err)
				return nil
			}
			// A ready image or visible rename is insufficient: the full durable
			// marker write above must succeed before PTY state can be created.
			// origin: hot-update S4 plan §5.2 / T31.
			p.ReleaseAdmission(m.UpgradeID)
			// origin: hot-update S5 plan §5.2 / T36; hot-update S7 plan §8.4 / P21.
			// Transfer creation shares this durable release. MarkReady never reaches
			// this release; the durable marker write is its only authority.

			if (*p.Stop) != nil {
				(*p.Stop)()
				(*p.Stop) = nil
			}
			// origin: hot-update S3 plan §2.6. A durable commit resolves any
			// deferred watchdog cut belonging to this transaction.
			p.RollbackInFlight.Store(false)
			p.Logger.Info("agent: legacy upgrade COMMITTED (control-ready on the new binary)",
				"prev_version", m.PrevVersion, "new_version", m.NewVersion, "release", proto.ReleaseVersion)
		case m.State == reported && (m.State == binaryupgrade.StateRolledBack || m.State == binaryupgrade.StateRollbackFailed):
			// The register that just succeeded carried this terminal outcome —
			// delivered; clear it so later registers stop re-reporting.
			// (A COMMITTED marker is deliberately NOT cleared, by contrast: it is
			// the operator's on-host breadcrumb of the last successful upgrade
			// and is replaced wholesale by the next install — internal review
			// S24. The asymmetry is intentional: terminal failure states are
			// one-shot reports, success is a durable record.)
			if !TargetsLegacy(p, m) {
				return nil
			}
			if err := os.Remove(markerPath); err != nil {
				p.Logger.Warn("agent: upgrade: could not clear delivered terminal marker", "err", err)
			}
			p.Store.SyncDir(filepath.Dir(markerPath))
			p.RollbackInFlight.Store(false)
		}
		return nil
	})
	if lerr != nil {
		p.Logger.Warn("agent: upgrade commit: host-wide lock unavailable", "err", lerr)
	}
}

func WatchdogLegacy(p LegacyPort, exePath string, expected *binaryupgrade.Marker) bool {
	result := false
	p.Mu.Lock()
	defer p.Mu.Unlock()
	// Host-wide flock (external review F1): the rollback's read-decide-write
	// must exclude sibling PROCESSES, not just our own goroutines.
	lerr := binaryupgrade.WithFileLock(exePath, true, func() error {
		m, err := ReadLegacyMarker(binaryupgrade.MarkerPath(exePath))
		if err != nil || m == nil || m.State != binaryupgrade.StatePending || !SameLegacyMarker(expected, m) || !TargetsLegacy(p, m) {
			// origin: hot-update S3 plan §2.6. No owned pending transaction
			// remains for this retry to reserve upgrade admission against.
			p.RollbackInFlight.Store(false)
			//nolint:nilerr // The closure's error return propagates LOCK failures only; a corrupt
			// marker is by contract "idle" — no pending upgrade of ours means the watchdog has no work.
			return nil
		}
		if p.ImagePhase() == agentboot.PhaseActive || p.ImagePhase() == agentboot.PhaseReady {
			if drillobserve.Hook != nil {
				e := drillobserve.Event{Kind: "watchdog-current", Domain: "agent", Values: [16]uint64{uint64(m.Generation), uint64(max(0, upgradebudget.ComponentHandover+p.Now().Sub(m.Deadline)))}}
				if !p.Now().Before(m.Deadline) {
					e.Values[2] = 1
				}
				drillobserve.Hook(e)
			}
			result = p.RollbackImage(exePath, m,
				fmt.Sprintf("register deadline exceeded (%s) without a successful register", upgradebudget.ComponentHandover), true)
			return nil
		}
		// origin: hot-update S4 internal review rv-fd rv-fd-4
		// Run now arms the watchdog before activation. A pristine import must
		// never fall through to bare exec while its Manager remains frozen.
		if p.ImagePhase() == agentboot.PhaseImported {
			result = true
			return nil
		}
		execFn := func(path string) error { return p.Exec(path) }
		if p.NoExit && !p.CustomExec {
			execFn = nil // in-process test harness: record the rollback, don't exec
		}
		ExecuteLegacyRollback(p.Store, exePath, m,
			fmt.Sprintf("register deadline exceeded (%s) without a successful register", upgradebudget.ComponentHandover),
			p.Logger, true, execFn)
		p.RollbackInFlight.Store(false)
		return nil
	})
	if lerr != nil {
		p.Logger.Error("agent: upgrade watchdog: host-wide lock unavailable; rollback was not performed",
			"err", lerr)
	}
	return result
}

func RecoverLegacyExec(p LegacyPort, exePath string, expected *binaryupgrade.Marker, execErr error) bool {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	handled := false
	// Host-wide flock (external review F1): exclude sibling processes from
	// the marker/prev/dst read-modify-write.
	lerr := binaryupgrade.WithFileLock(exePath, true, func() error {
		m, merr := ReadLegacyMarker(binaryupgrade.MarkerPath(exePath))
		if merr != nil || m == nil || m.State != binaryupgrade.StatePending || !SameLegacyMarker(expected, m) {
			//nolint:nilerr // The closure's error return propagates LOCK failures only; a corrupt
			// marker is by contract "idle" (loud elsewhere, never a block) and means: nothing to recover.
			return nil
		}
		m.ResourcePhase = binaryupgrade.PhaseThawed
		ExecuteLegacyRollback(p.Store, exePath, m, fmt.Sprintf("syscall.Exec of the new binary failed: %v", execErr),
			p.Logger, false, nil)
		p.RollbackInFlight.Store(false)
		handled = true
		return nil
	})
	if lerr != nil {
		// We cannot safely prove that this was the untracked re-exec-only
		// path without the lock. Keep the only known-live old process rather
		// than exit and risk leaving an unsupervised node permanently down.
		p.Logger.Error("agent: re-exec recovery: host-wide lock unavailable; keeping the old process alive",
			"err", lerr)
		return true
	}
	return handled
}

// Marker ownership and cancellation decide whether recovery runs. They do not
// replace Run's original error or masquerade as a failure to acquire the lock.
func RecoverLegacyBeforeControlLocked(ctx context.Context, p LegacyPort, exePath string) {
	m, readErr := ReadLegacyMarker(binaryupgrade.MarkerPath(exePath))
	if readErr != nil || m == nil || m.State != binaryupgrade.StatePending || !TargetsLegacy(p, m) {
		return
	}
	// Recheck after waiting for the watchdog: cancellation wins even if
	// this call originally entered on an authoritative register refusal.
	if ctx != nil && ctx.Err() != nil {
		return
	}
	p.Logger.Error("agent: run failed before control-ready; re-dispatching current state")
	p.RollbackImage(exePath, m, "bootstrap failed before control-ready", false)
}
