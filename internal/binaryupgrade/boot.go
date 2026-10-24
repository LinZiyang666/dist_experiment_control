// boot.go owns host-scoped marker convergence and crash-loop decisions.
// origin: hot-update S4 plan §10.
package binaryupgrade

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
)

func (s Store) CheckBoot(exePath string, logger *slog.Logger, now time.Time, img *agentboot.Image, redispatch func(*handoff.Prepared, *agentboot.Image, func(string) error) func(string) error, execFn func(string) error) string {
	markerPath := MarkerPath(exePath)
	m, err := ReadMarker(markerPath)
	if err != nil {
		logger.Warn("agent: upgrade marker unreadable — treating as idle", "err", err)
		return ""
	}
	if m == nil || m.State != StatePending {
		return ""
	}
	selfSHA, err := agentimage.SHA256OfFile(exePath)
	if err != nil {
		logger.Warn("agent: boot upgrade check: cannot hash own binary — leaving marker untouched", "err", err)
		return ""
	}
	switch DecideBoot(m, selfSHA, now) {
	case BootProceed:
		return ""
	case BootContinuePending:
		m.BootCount++
		// NO OWNERSHIP TAKEOVER HERE. A round-2 revision re-stamped
		// m.TargetInstance from this arm, on the theory that reaching it proved
		// ownership. external review F7 refuted that: a SIBLING clone shares the
		// binary and the home, so it runs these same staged bytes and reaches
		// this same arm — the takeover let any sibling restart steal the marker,
		// spend the target's boot budget, and report an outcome for an upgrade
		// it was not part of.
		//
		// The honest position is that a marker sitting next to a SHARED binary
		// is host-scoped, not instance-scoped, and cannot be made
		// instance-scoped by anything derivable here: a process lineage does not
		// survive a supervisor restart, and everything that survives one is
		// shared with the siblings. So ownership is anchored to the boot proof
		// below (per-transaction, and only held by a process that actually
		// traversed this decision), and the residual limitation is documented
		// rather than papered over.
		//
		// TargetInstance is otherwise pinned to a process lineage, and a lineage
		// only survives syscall.Exec. Any supervisor-mediated restart inside the
		// upgrade window — a host reboot, `systemctl restart`, an OOM kill, Run
		// returning fatally — starts the successor with a clean environment and
		// therefore a NEW instance id. That successor then disowns the marker:
		// no watchdog is armed, no outcome is reported, and nothing in the repo
		// ever GCs it. The marker sits pending forever until some unrelated boot
		// weeks later finds the deadline long past and SILENTLY ROLLS THE NODE
		// BACK — an upgrade that looked successful (the restarted process really
		// did report the new version) quietly undone at a moment no one connects
		// to it.
		//
		// Re-stamping here is what makes the marker survive a restart the way
		// its three sibling proofs (boot count, boot proof, running-image sha)
		// already do, while keeping the field's real job: a SIBLING clone that
		// never ran this transaction cannot reach this arm, so it still cannot
		// claim the outcome.
		if err := s.WriteMarker(markerPath, m); err != nil {
			logger.Warn("agent: boot upgrade check: boot_count persist failed", "err", err)
			return ""
		}
		logger.Info("agent: booting a staged upgrade (pending commit)",
			"new_version", m.NewVersion, "boot_count", m.BootCount, "boot_budget", m.BootBudget,
			"deadline", m.Deadline.UTC().Format(time.RFC3339))
		return m.UpgradeID
	case BootRollback:
		// origin: hot-update S3 plan §1.6. Borrow keeps the held image usable
		// if exec fails and S12 continues in this process after restoring disk.
		if img != nil && img.Phase() == agentboot.PhaseHeld {
			prepared, err := img.Repack()
			if err != nil {
				m.ResourcePhase = PhaseLost
				img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, "bundle_rejected")
			} else {
				defer func() { _ = prepared.Close() }()
				m.ResourcePhase = PhaseRedispatched
				execFn = redispatch(prepared, img, execFn)
				logger.Info("agent: image re-dispatched", "image", img.Summary())
			}
		}
		s.ExecuteRollback(exePath, m, fmt.Sprintf("boot: budget/deadline exhausted (boot_count=%d/%d, deadline=%s)",
			m.BootCount, m.BootBudget, m.Deadline.UTC().Format(time.RFC3339)), logger, true, execFn)
	case BootConvergeRolledBack:
		m.State = StateRolledBack
		if m.Detail == "" {
			m.Detail = "rollback interrupted; converged at boot (running binary matches prev sha)"
		}
		if err := s.WriteMarker(markerPath, m); err != nil {
			logger.Warn("agent: boot upgrade check: converge write failed", "err", err)
		}
		logger.Warn("agent: converged an interrupted upgrade rollback at boot",
			"prev_version", m.PrevVersion, "new_version", m.NewVersion)
	case BootMarkOrphan:
		m.State = StateRollbackFailed
		m.Detail = "running binary matches neither prev nor new sha (replaced by hand mid-upgrade?)"
		if err := s.WriteMarker(markerPath, m); err != nil {
			logger.Warn("agent: boot upgrade check: orphan write failed", "err", err)
		}
		logger.Error("agent: upgrade marker orphaned — running binary matches neither recorded sha; continuing")
	}
	return ""
}
