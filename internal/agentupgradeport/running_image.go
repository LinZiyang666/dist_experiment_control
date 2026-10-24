package agentupgradeport

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"github.com/LinZiyang666/tether/internal/agentidentity"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"os"
	"runtime"
)

// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.2
// The running-byte reader stays with image evidence; the engine keeps its API.
// runningImageSHA hashes THIS PROCESS'S executing image — not the on-disk
// path (external review F1): after the install flip, the path holds the new
// binary for EVERY process on the host, but only the process that re-exec'd
// is actually running it. /proc/self/exe reads the running image's bytes even
// when the path it once came from has been renamed over; that makes this the
// one commit proof a co-hosted sibling cannot borrow.
func RunningImageSHA(path string, executable func() (string, error)) (string, error) {
	if path == "" && runtime.GOOS == "linux" {
		return ProcessImageDigest()
	}
	if path == "" {
		var err error
		path, err = executable()
		if err != nil {
			return "", err
		}
	}
	return agentimage.SHA256OfFile(path)
}

// Nonce shares the observation challenge generator with control admission.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §4.2
func Nonce() string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(nonce[:])
}

// ProcessOwner derives the S8 owner identity of this process. The 128-bit
// instance is bound to the lineage instance ID that same-PID exec carries, so
// the journal's issuer and the runtime's producer are one identity; it is not
// a new random per call and never the raw lineage spelling.
// origin: hot-update S8 plan §2.1 / §5.1; S8 FIX-AGENTWIRE
func ProcessOwner(instanceID string) (upgradetx.Owner, error) {
	boot := agentidentity.ReadBootID()
	ticks, err := agentidentity.ReadStartTimeTicks(os.Getpid())
	if instanceID == "" || boot == "" || err != nil || ticks <= 0 {
		return upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: ReasonActivationUnavailable}
	}
	sum := sha256.Sum256([]byte("tether-agent-upgrade-owner\x00" + instanceID))
	return upgradetx.Owner{BootID: boot, PID: os.Getpid(), StartTicks: uint64(ticks), InstanceID: hex.EncodeToString(sum[:16])}, nil
}
