//go:build !linux

package workload

import (
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
)

func newReplayFile(_ int64) (*os.File, error) {
	return nil, handoff.ErrUnsupported
}

func validateReplayFile(_ *os.File, _ int64) error {
	return handoff.ErrUnsupported
}
