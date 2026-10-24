//go:build !linux

package agentupgradeport

import "errors"

// This cache requires Linux's immutable executing inode. Other platforms keep
// the fresh-path check in agentupgrade.RunningImageSHA.
// origin: hot-update S8 plan §6.1 / §10; S8 internal review FREEZE-WINDOW R6
func ProcessImageDigest() (string, error) {
	return "", errors.New("immutable executing image digest is unavailable")
}
