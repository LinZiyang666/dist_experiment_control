//go:build !linux

package clusterharness

import (
	"github.com/LinZiyang666/tether/internal/handoff"
	"testing"
)

// Unsupported platforms refuse before process, store or descriptor side effects.
// origin: hot-update S6 plan §11 T48
func StartBrokerCohortFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return nil, handoff.ErrUnsupported
}

// origin: hot-update S6 plan §8.3 / T50
func StartBrokerControlFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return nil, handoff.ErrUnsupported
}
