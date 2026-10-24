// install.go owns staging, reader validation, prev-slot durability and the atomic flip.
// origin: hot-update S4 plan §5.2 / §10.
package binaryupgrade

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// InstallInputs contains the caller's image admission and immutable target identity.
// The clock and generation are sampled at the original marker construction point.
type InstallInputs struct {
	CandidateABI         *int
	Readers              *[]agentimage.Capabilities
	RunningSHA           string
	Now                  func() time.Time
	Generation           func() uint64
	SID, NID, InstanceID string
	Logger               *slog.Logger
	CheckCandidate       func(agentimage.Capabilities) error
}

var ErrSmokeFailed = errors.New("upgrade: staged binary failed the smoke gate")

// MaxTarballBytes caps the upgrade download size. v1 tether
// release tarballs are ~10MiB; 64MiB gives 6× headroom while
// preventing a malicious or misconfigured URL from OOM-ing the
// agent (audit Sec F1: io.ReadAll has no size limit, `--all`
// could OOM the entire fleet at once).
const MaxTarballBytes = 64 * 1024 * 1024

// installNewBinary stages and atomically installs the `tether` binary
// contained in the just-downloaded gzipped tar. The release tarball
// layout (build/goreleaser.yaml § archives) carries a single file
// named `tether` at the archive root plus README/LICENSE; we
// extract just `tether` into a sibling tmp file, chmod +x, then
// (upgrade-safety plan §3.1) run the SMOKE GATE, hardlink the current
// binary into the prev slot, write the pending marker, and finally
// rename onto dst. Rename is atomic on the same fs; running
// processes keep their open inode handle (the new binary only
// loads on the next exec), so this is safe to do while the agent
// is still serving the upgrade reply. dst itself is IN PLACE at every
// interruption point — the prev slot is a hardlink plus a single
// rename, never a remove-then-replace window (plan §10.2: the
// two-rename variant would strand a crashed host on ENOENT).
//
// Uses archive/tar + compress/gzip directly (audit Sec F1: shelling
// out to host tar opens path-traversal exposure under non-GNU tar
// implementations). Each tar header.Name is checked: must be
// exactly "tether" — anything with a path separator, leading slash,
// or "..", or any other filename, is refused.
//
// Returns the release tag the smoke gate parsed out of `<staged> version` —
// GUARANTEED non-empty and in the same format the new binary will report as
// ReleaseVersion on register (ctl --wait compares the two). A failure
// wrapping errUpgradeSmokeFailed means the artifact is bad everywhere and the
// disk was untouched; any other failure is a local IO problem
// (install_failed).
func (s Store) Install(tarball []byte, dst string, inputs InstallInputs) (string, error) {
	tmpDir, err := os.MkdirTemp(filepath.Dir(dst), ".tether-upgrade-*")
	if err != nil {
		return "", fmt.Errorf("mkdir tmp: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	binPath := filepath.Join(tmpDir, "tether")
	if err := s.extractBinary(tarball, binPath); err != nil {
		return "", err
	}
	prevSHA, err := agentimage.SHA256OfFile(dst)
	if err != nil {
		return "", fmt.Errorf("hash current binary: %w", err)
	}
	newSHA, err := agentimage.SHA256OfFile(binPath)
	if err != nil {
		return "", fmt.Errorf("hash staged binary: %w", err)
	}
	// origin: hot-update S3 plan §2.1; drill 33 C2, first real S3 run 2026-10-02.
	// The prev slot must accept every image a rollback can hand it. Exactly two
	// binaries provably do: the exporter (this running image) and the candidate
	// byte for byte — a boot rollback re-dispatches a bundle the candidate was
	// admitted to import (checkCandidateABI), and a watchdog rollback re-exports
	// from the candidate itself. The second case is the shared-binary sibling:
	// a co-hosted agent already installed this very artifact, so dst ==
	// candidate != running. The plan's first form (running == dst, checked
	// before the download) refused that sibling forever — every retry saw the
	// same disk — and stranded it on the old image short of a restart. Any
	// other dst (installed by hand while this process ran) is refused here,
	// before the prev slot, the marker or dst change.
	if inputs.RunningSHA != "" && prevSHA != inputs.RunningSHA && prevSHA != newSHA {
		return "", agentimage.ErrRunningImageDiffers
	}

	// Smoke gate: the staged binary must exec and answer `version` with a
	// parsable release tag BEFORE anything on disk changes. Catches wrong
	// arch, truncation, and not-a-tether-binary — the failure classes that
	// used to be discovered only by syscall.Exec, after the flip.
	newVersion, candidate, err := probeCapabilities(binPath)
	if err != nil {
		return "", fmt.Errorf("staged binary failed the smoke gate (disk unchanged): %w: %w", ErrSmokeFailed, err)
	}

	// origin: hot-update S4 plan §5.2 / T30. Select capabilities by the
	// actual prev SHA, preserving the shared sibling candidate case.
	if candidate.SHA256 != newSHA {
		return "", fmt.Errorf("%w: candidate changed before version probe", ErrSmokeFailed)
	}
	previous, err := agentimage.SelectPreviousCapabilities(inputs.RunningSHA, prevSHA, candidate)
	if err != nil {
		return "", err
	}
	for _, reader := range []agentimage.Capabilities{candidate, previous} {
		if err := inputs.CheckCandidate(reader); err != nil {
			return "", err
		}
	}
	if err := agentimage.VerifyCapabilitySHA(dst, previous); err != nil {
		return "", err
	}
	if inputs.CandidateABI != nil {
		*inputs.CandidateABI = candidate.ABI
	}
	if inputs.Readers != nil {
		*inputs.Readers = []agentimage.Capabilities{candidate, previous}
	}

	// Prev slot: hardlink the CURRENT binary aside (same dir ⇒ same fs ⇒
	// zero-copy; a hardlink shares the old inode, so the later rename of the
	// new binary over dst cannot touch it). Some filesystems refuse links —
	// fall back to a copy. Old prev from a previous upgrade is replaced.
	prevPath := PrevPath(dst)
	var upgradeNonce [16]byte
	if _, err := rand.Read(upgradeNonce[:]); err != nil {
		return "", fmt.Errorf("generate upgrade transaction id: %w", err)
	}
	if err := os.Remove(prevPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("clear old prev slot: %w", err)
	}
	if err := os.Link(dst, prevPath); err != nil {
		if cerr := s.copyBinary(dst, prevPath); cerr != nil {
			return "", fmt.Errorf("prev slot: link failed (%v) and copy failed: %w", err, cerr)
		}
	}
	// F6 ordered durability: the prev slot's directory entry must be on
	// stable storage BEFORE the marker that references it exists — a power
	// cut between marker and prev would otherwise leave a rollback promise
	// with nothing to roll back to.
	if err := s.SyncDirE(filepath.Dir(dst)); err != nil {
		_ = os.Remove(prevPath)
		return "", fmt.Errorf("sync dir after prev slot: %w", err)
	}

	// Marker BEFORE the flip: if the flip below fails, the error path clears
	// it back to a terminal state; if that clearing is itself lost (process
	// death mid-install), the pending marker goes stale at the deadline and
	// the install entry gate stops honoring it.
	deadline := inputs.Now().Add(upgradebudget.ComponentHandover)
	marker := &Marker{
		CandidateImageABI:     candidate.ABI,
		CandidateImageDomains: append([]string(nil), candidate.Domains...),
		CandidateImageSHA256:  candidate.SHA256,
		PrevImageABI:          previous.ABI,
		PrevImageDomains:      append([]string(nil), previous.Domains...),
		PrevImageSHA256:       previous.SHA256,
		Generation:            inputs.Generation(),
		ResourcePhase:         PhaseStaged,
		State:                 StatePending,
		PrevSHA:               prevSHA,
		NewSHA:                newSHA,
		PrevVersion:           proto.ReleaseVersion,
		NewVersion:            newVersion,
		Deadline:              deadline,
		BootBudget:            upgradebudget.BootBudget,
		// external review F1: pin the upgrade to the instance `node upgrade
		// <nid>` targeted — a co-hosted sibling must neither claim nor clear
		// it. UpgradeID is also the process-local boot proof, so it must be
		// unique even for rapid same-artifact retries.
		TargetSID: inputs.SID,
		TargetNID: inputs.NID,
		// Stamp WHICH instance started this upgrade. TargetNID is the basename
		// and every clone of one image shares it, so without this the marker
		// cannot tell the upgrading instance from its siblings — and the first
		// sibling to boot would report this upgrade's outcome and clear the
		// marker out from under the instance that actually ran it.
		TargetInstance: inputs.InstanceID,
		UpgradeID:      hex.EncodeToString(upgradeNonce[:]),
	}
	// The caller holds the host-wide flock, which serializes this marker write
	// against commit/watchdog in this process and every sibling process. Do
	// not take upgradeMu here: those transitions take upgradeMu before the
	// flock, and reversing that order here creates an ABBA deadlock.
	markerPath := MarkerPath(dst)
	werr := s.WriteMarker(markerPath, marker)
	if werr != nil {
		// external re-review F11: writeUpgradeMarker can fail AFTER its
		// rename landed (the directory-fsync step) — the transaction is
		// aborting, but a visible pending marker would make this healthy
		// old process reject every retry as upgrade_in_progress until the
		// 120s deadline. Compensate under the same locks (host flock is
		// held by the caller): remove whatever landed and re-sync.
		if landed, rerr := ReadMarker(markerPath); rerr == nil && landed != nil &&
			landed.UpgradeID == marker.UpgradeID {
			_ = os.Remove(markerPath)
			s.SyncDir(filepath.Dir(markerPath))
		}
	}
	if werr != nil {
		_ = os.Remove(prevPath)
		s.SyncDir(filepath.Dir(prevPath))
		return "", fmt.Errorf("write upgrade marker: %w", werr)
	}
	if err := os.Rename(binPath, dst); err != nil {
		// dst is untouched (the rename failed) — record the aborted attempt
		// so the entry gate does not block on a flip that never happened.
		marker.State = StateRolledBack
		marker.Detail = fmt.Sprintf("install aborted before the flip: %v", err)
		werr := s.WriteMarker(markerPath, marker)
		if werr != nil {
			inputs.Logger.Warn("agent: could not record aborted install; pending marker will go stale at its deadline", "err", werr)
		}
		_ = os.Remove(prevPath)
		return "", fmt.Errorf("atomic rename: %w", err)
	}
	// F6: make the flip itself durable. Past this point the transaction is
	// complete on stable storage; a failure here is logged, not unwound —
	// the runtime state (reply, re-exec) matches the disk either way.
	if err := s.SyncDirE(filepath.Dir(dst)); err != nil {
		inputs.Logger.Warn("agent: dir fsync after the flip failed — a power cut may resurface the old binary (marker converges it at boot)", "err", err)
	}
	return newVersion, nil
}

func (s Store) copyBinary(src, dst string) error { return agentimage.CopyBinary(src, dst, s.SyncFile) }
func (s Store) extractBinary(tarball []byte, outPath string) error {
	return agentimage.ExtractBinary(tarball, outPath, MaxTarballBytes, s.SyncFile)
}
func probeCapabilities(binPath string) (string, agentimage.Capabilities, error) {
	return agentimage.ProbeCapabilities(binPath, agentboot.RunVersionProbe)
}
