//go:build !linux

package clusterharness

import (
	"github.com/LinZiyang666/tether/internal/handoff"
	"testing"
)

// The unsupported boundary precedes process, store and descriptor side effects.
// origin: hot-update S6 plan §13 / T54
func StartBrokerRunFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return nil, handoff.ErrUnsupported
}

func StartBrokerStoreRunFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return nil, handoff.ErrUnsupported
}

func StartBrokerLifecycleFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return nil, handoff.ErrUnsupported
}
