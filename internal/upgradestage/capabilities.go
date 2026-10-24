// Package upgradestage owns the local publication helpers beneath CLI wiring.
package upgradestage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// CapabilityDigest binds a grant to the exact reader declarations of the
// candidate image (ABI epoch, read epochs, domains, features). The root stage
// recomputes it from the published bytes; a candidate whose `version` output
// changed after Prepare therefore cannot be published under this grant.
// origin: hot-update S8 plan §5.2 / §6.1 / §11.2 P19
func CapabilityDigest(component upgradetx.Component, caps agentimage.Capabilities) string {
	domains := append([]string(nil), caps.Domains...)
	features := append([]string(nil), caps.Features...)
	epochs := append([]uint64(nil), caps.ReadEpochs...)
	slices.Sort(domains)
	slices.Sort(features)
	slices.Sort(epochs)
	data, _ := json.Marshal(struct {
		Component  upgradetx.Component `json:"component"`
		ABI        int                 `json:"abi"`
		ReadEpochs []uint64            `json:"read_epochs"`
		Domains    []string            `json:"domains"`
		Features   []string            `json:"features"`
	}{component, caps.ABI, epochs, domains, features})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
