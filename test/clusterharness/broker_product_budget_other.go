//go:build !linux

package clusterharness

import (
	"context"
	"errors"
	"testing"
)

// Product broker custody and same-PID store reload fixtures require Linux.
// An unsupported platform must never issue a timing acceptance receipt.
// origin: hot-update S10 lead decision D-A / platform boundary
func productCandidateBuild(_ *testing.T, _ context.Context, _, _, _, _ string) error {
	return errors.New("broker product timing fixtures require Linux")
}
