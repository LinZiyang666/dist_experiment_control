// origin: hot-update S6 plan §2.1 / §12.1 P00
package clusterharness

import (
	"testing"
)

type BrokerImageFixtureConfig struct {
	Topology          string // single | sole-voter | quorum
	CandidatePath     string
	PreviousPath      string
	FaultArtifactPath string
}
type BrokerImageReceipt struct {
	Label  string `json:"label"`
	JSON   []byte `json:"json"`
	Digest string `json:"digest"`
}

type BrokerImageFault struct {
	Point   string `json:"point"`
	Ordinal uint32 `json:"ordinal"`
	Once    bool   `json:"once"`
}

// The unique P26 fixture supplies independent process/store receipts.
type BrokerImageFixtureAPI interface {
	Checkpoint(string) (BrokerImageReceipt, error)
	ArmFault(BrokerImageFault) error
	Close() error
}
type BrokerImageFixture struct{ api BrokerImageFixtureAPI }
type StartBrokerImageFixtureFunc = func(*testing.T, BrokerImageFixtureConfig) (*BrokerImageFixture, error)
type BrokerImageCheckpointFunc = func(*BrokerImageFixture, string) (BrokerImageReceipt, error)
type BrokerImageArmFaultFunc = func(*BrokerImageFixture, BrokerImageFault) error
type BrokerImageCloseFunc = func(*BrokerImageFixture) error
