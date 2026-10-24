// rollback.go owns the v1 rollback execution and its marker record. Only a
// running previous image may record rolled_back; a pre-exec candidate keeps
// recovery intent. Split from marker.go by responsibility.
// origin: hot-update S4 plan §10; hot-update S8 plan §1 / §11.2 P17
package binaryupgrade

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/agentimage"
)

// executeRollback restores the prev slot over the live binary path and records
// the outcome. When reexec is true (boot rollback, watchdog rollback) it then
// execs the restored binary via execFn; when false (syscall.Exec failure — the
// still-running OLD process image is itself the restored code) it just
// returns. Rollback failure (prev slot missing or not the recorded sha) is a
// TERMINAL record: log loudly, keep running whatever we are — a retry loop
// here would recreate the crash-loop this machinery exists to end.
func (s Store) ExecuteRollback(exePath string, m *Marker, reason string, logger *slog.Logger, reexec bool, execFn func(string) error) {
	markerPath := MarkerPath(exePath)
	prev := PrevPath(exePath)

	prevSHA, err := agentimage.SHA256OfFile(prev)
	if err != nil || prevSHA != m.PrevSHA {
		s.record(markerPath, m, StateRollbackFailed, fmt.Sprintf("prev slot unusable (err=%v, sha=%s, want=%s) — running what is on disk", err, prevSHA, m.PrevSHA),
			logger, "agent: upgrade rollback: marker write failed on top of an unusable prev slot")
		logger.Error("agent: UPGRADE ROLLBACK FAILED — prev slot is missing or corrupt; continuing on the current binary",
			"reason", reason, "detail", m.Detail)
		return
	}
	if err := os.Rename(prev, exePath); err != nil {
		s.record(markerPath, m, StateRollbackFailed, fmt.Sprintf("restore rename failed: %v", err),
			logger, "agent: upgrade rollback: marker write failed after a failed restore")
		logger.Error("agent: UPGRADE ROLLBACK FAILED — could not restore prev slot; continuing on the current binary",
			"reason", reason, "err", err)
		return
	}
	s.SyncDir(filepath.Dir(exePath)) // F6: make the restore rename durable before recording it
	// The restore itself landed; a stale pending marker converges at the
	// next boot via decideBoot's prev-sha branch.
	landed := "agent: upgrade rollback: restore landed but marker write failed"
	// origin: hot-update S8 plan §1 / §3.3 / §11.2 P17; S8 review rv-fault F7.
	// Before the exec this process still runs the candidate, so rolled_back
	// would be false: an exec error keeps the candidate serving new work. The
	// pending marker with its reason is the recovery intent; the restored
	// binary converges it at boot (decideBoot's prev-sha branch), the first
	// moment the previous image provably runs. A nil execFn is the in-process
	// harness's explicit "record, do not replace the image" contract, and the
	// non-reexec caller IS the restored code, so both record the outcome now.
	if reexec && execFn != nil {
		s.record(markerPath, m, StatePending, reason, logger, landed)
		logger.Warn("agent: upgrade rollback restored the previous binary; executing it",
			"reason", reason, "prev_version", m.PrevVersion, "new_version", m.NewVersion)
		if err := execFn(exePath); err != nil {
			// origin: internal review S12. Do NOT exit here: on setsid-nohup
			// there is no supervisor to relaunch anything, and this running
			// image — whatever its flaws — is the only live process the
			// NAT'd host has. The restore has landed on disk (dst = old
			// binary), so any later restart, manual or supervised, comes
			// back as the known-good binary. Keep serving. The marker says
			// rollback_failed, not pending: a pending marker would let this
			// still-running candidate commit itself over the restored disk.
			// Detail keeps the caller's sanitized reason; the exec error is
			// logged below and the state alone says the reverse did not run.
			s.record(markerPath, m, StateRollbackFailed, reason, logger, landed)
			logger.Error("agent: upgrade rollback: exec of restored binary failed; "+
				"continuing on the current process image (disk already restored — the next restart boots the old binary)",
				"err", err)
			return
		}
		// A real exec never returns success. An injected one stands for the
		// restored image's own boot convergence and records the same outcome.
	}
	s.record(markerPath, m, StateRolledBack, reason, logger, landed)
	logger.Warn("agent: upgrade ROLLED BACK to previous binary",
		"reason", reason, "prev_version", m.PrevVersion, "new_version", m.NewVersion)
}

// record writes one marker transition. A write failure is logged with the
// caller's context rather than returned: every rollback path keeps serving on
// whatever image is running, and decideBoot converges a stale record at boot.
func (s Store) record(path string, m *Marker, state, detail string, logger *slog.Logger, failure string) {
	m.State, m.Detail = state, detail
	if err := s.WriteMarker(path, m); err != nil {
		logger.Error(failure, "err", err)
	}
}
