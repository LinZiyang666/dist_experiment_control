// image_dispatch.go holds the agent's pure image-cut helpers: the closed
// marker failure vocabulary, the bundle-carrying redispatch exec and image
// ownership. Moved verbatim from internal/agentupgrade/image_policy.go by
// S8 FIX-DESCRIBE so agentupgrade stays under its structural threshold
// (pkg-code-lines > 2000); the agent remains their only caller.
// origin: hot-update S8 plan §12.1 / P14; S8 FIX-DESCRIBE (structural move)
package agentupgradeport

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S3 plan §2.1 step 9. Marker details are a fixed
// vocabulary: wrapped export/validation errors can contain private data.
func CutFailureClass(err error) string {
	switch {
	case errors.Is(err, workload.ErrFrozen):
		return "frozen_table"
	case errors.Is(err, agentimage.ErrABIUnsupported):
		return "abi_late"
	case errors.Is(err, agentimage.ErrBundle):
		return "bundle"
	default:
		return "image_cut"
	}
}

// RedispatchExec turns a prepared bundle into the execFn the rollback
// machinery calls (executeRollback's reexec arm): the previous binary
// receives the SAME bundle instead of a bare exec that would let the kernel
// close every carried descriptor. A nil prepared falls back to the bare
// exec (nothing to carry). env is the lineage-bearing environment: the
// boot path passes the snapshot the bootstrap took before New consumed the
// variables (execEnvFor over agentboot.Image.Env), a running agent passes
// execEnv().
func RedispatchExec(prepared *handoff.Prepared, env []string, fallback func(string) error) func(string) error {
	return func(path string) error {
		if prepared == nil {
			return fallback(path)
		}
		argv := append([]string(nil), os.Args...)
		argv[0] = path
		return prepared.Exec(path, argv, env)
	}
}

// origin: hot-update S3 plan §2.6. Register readiness does not prove the
// commit reached disk: pending + ready still owns the imported runtime.
func OwnsImage(img *agentboot.Image) bool {
	return img != nil && (img.Phase() == agentboot.PhaseActive || img.Phase() == agentboot.PhaseReady)
}

// origin: S10 internal review MAKETEST-GATES F4
// smokeVersion is the smoke gate (upgrade-safety plan §3.1): run
// `<staged> version` and STRICTLY parse its first non-empty line against the
// frozen format `tether <release> (proto v<N>)` (proto.VersionLine — the
// cross-version seam both emitter and this parser derive from). Refuses on
// exec failure, unparsable output, or an epoch N different from this
// agent's own ProtoVersion (external review F5). The returned tag is
// normalized to EXACTLY what the new binary reports as ReleaseVersion on
// register — ctl's --wait equality check depends on that (pinned by test).
// The optional ABI result preserves existing release-only callers.
func SmokeVersion(binPath string, candidateABI ...*int) (string, error) {
	release, capabilities, err := agentimage.ProbeCapabilities(binPath, agentboot.RunVersionProbe)
	if err == nil && len(candidateABI) > 0 {
		*candidateABI[0] = capabilities.ABI
	}
	return release, err
}

// SS owns salts/material after its last connection closes. Only its image
// owner can prove representability, including parser/DNS/cleanup phases.
// origin: hot-update S5 plan §6.1 / §7.3 / T37.
func CheckSSImageState(retained bool, snapshot func() error) error {
	if !retained {
		return nil
	}
	if err := snapshot(); err != nil {
		return err
	}
	// The actor's Freeze validates parser/DNS ownership before export. With
	// no server, only the validated sealed agent metadata remains to carry.
	return nil
}

// SHA256OfBytes names the staged artifact using the same lowercase encoding as
// the source and rollback reader checks.
// origin: S10 internal review MAKETEST-GATES F4
func SHA256OfBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
