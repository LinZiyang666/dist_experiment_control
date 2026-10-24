package upgradeauthority

import (
	"crypto/subtle"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Match permits an ordinary first register to learn a known incarnation. A
// carried image already has a nonempty expected value: a replacement session
// or an old writer's empty reply must never replace that inherited identity.
// The register adapter retains its original expected value across the call.
// origin: hot-update S8 plan §6.2 / P08
func Match(expected, actual string) error {
	if !incarnationValid(actual) || (expected != "" && !incarnationValid(expected)) {
		return incarnationMismatch()
	}
	if expected != "" && subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) != 1 {
		return incarnationMismatch()
	}
	return nil
}

func incarnationValid(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := range value {
		if (value[i] < '0' || value[i] > '9') && (value[i] < 'a' || value[i] > 'f') {
			return false
		}
	}
	return true
}

func incarnationMismatch() error {
	return &upgradetx.Error{Code: upgradetx.CodeSessionIncarnationMismatch, Detail: "session incarnation does not match"}
}
