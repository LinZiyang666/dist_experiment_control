package upgradestage

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// SourceReader binds this binary's own reader declarations to the
// running bytes; ImageReader and BrokerCandidate probe another executable's
// bytes through the isolated version probe (the agent smoke path), so a
// broker never trusts a pathname's declarations without its SHA.
// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11
func SourceReader(sha string) (upgradecompat.Reader, error) {
	return agentimage.CurrentCapabilities(sha).Reader(proto.ReleaseVersion, upgradetx.ComponentBroker), nil
}

func ImageReader(path string) (upgradecompat.Reader, error) {
	release, caps, err := agentimage.ProbeCapabilities(path, spawnsafe.RunVersionProbe)
	if err != nil {
		return upgradecompat.Reader{}, err
	}
	return caps.Reader(release, upgradetx.ComponentBroker), nil
}

func BrokerCandidate(path string) (*upgradetx.CandidateDescription, error) {
	release, caps, err := agentimage.ProbeCapabilities(path, spawnsafe.RunVersionProbe)
	if err != nil || release == "" {
		return nil, errors.Join(err, reject("staged candidate failed its version probe"))
	}
	return &upgradetx.CandidateDescription{SHA: caps.SHA256, Release: release, CapabilityDigest: CapabilityDigest(upgradetx.ComponentBroker, caps)}, nil
}

// The common reader probe needs the same typed refusal on every platform.
// origin: hot-update S8 plan §6.1 / §11.2 P19
func reject(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: detail}
}
