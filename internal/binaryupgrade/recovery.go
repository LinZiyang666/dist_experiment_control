// recovery.go owns pre-control-ready disk rollback while the bootstrap retains its image.
// origin: hot-update S4 plan §10.
package binaryupgrade

import (
	"log/slog"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/handoff"
)

func (s Store) FinishBoot(img *agentboot.Image, exe string, logger *slog.Logger, classify func() string, owns func(*Marker, *agentboot.Image) bool, redispatch func(*handoff.Prepared, *agentboot.Image, func(string) error) func(string) error, execFn func(string) error) {
	switch img.Phase() {
	case agentboot.PhaseNone, agentboot.PhaseReady, agentboot.PhaseLost, agentboot.PhaseActive:
		return
	case agentboot.PhaseHeld, agentboot.PhaseRefused, agentboot.PhaseImported:
	}
	if err := WithFileLock(exe, true, func() error {
		s.finishBootLocked(img, exe, logger, classify, owns, redispatch, execFn)
		return nil
	}); err != nil {
		// Lock failure is also W2: returning to main with carried descriptors
		// must never silently turn an unrecorded loss into a successful boot.
		img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, classify())
	}
}

// Recovery handles marker/repack failures itself; only file-lock failures return
// through finishBoot's lock callback and require abandonment there.
func (s Store) finishBootLocked(img *agentboot.Image, exe string, logger *slog.Logger, classify func() string, owns func(*Marker, *agentboot.Image) bool, redispatch func(*handoff.Prepared, *agentboot.Image, func(string) error) func(string) error, execFn func(string) error) {
	m, err := ReadMarker(MarkerPath(exe))
	if err != nil || !owns(m, img) {
		img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, classify())
		return
	}
	reason := "bootstrap failed before control-ready: " + classify()
	prepared, err := img.Repack()
	if err != nil {
		m.ResourcePhase = PhaseLost
		img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, "bundle_rejected")
		s.ExecuteRollback(exe, m, reason, logger, true, execFn)
		return
	}
	m.ResourcePhase = PhaseRedispatched
	logger.Warn("agent: bootstrap failed before control-ready; re-dispatching", "reason", classify(), "image", img.Summary())
	dispatch := redispatch(prepared, img, execFn)
	s.ExecuteRollback(exe, m, reason, logger, true, func(path string) error {
		// origin: hot-update S3 plan §0 exit 3. The same receipt covers
		// failures before Run as well as its active-image recovery.
		logger.Info("agent: image re-dispatched", "image", img.Summary(), "generation", img.Generation())
		return dispatch(path)
	})
	_ = prepared.Close()
	img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, "exec_failed")
}
