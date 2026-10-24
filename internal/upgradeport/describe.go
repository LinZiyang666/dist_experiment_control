package upgradeport

import (
	"context"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Description and CandidateDescription are the core describe DTOs
// (upgradetx.Description); the aliases keep the control-plane ports in this
// package's vocabulary.
// origin: S8 FIX-DESCRIBE (P19 request R1)
type (
	Description          = upgradetx.Description
	CandidateDescription = upgradetx.CandidateDescription
)

// Describer is implemented by an endpoint that can describe the component it
// serves. It is a separate interface rather than an eighth Endpoint method so
// the frozen Endpoint method set (and every adapter of it) stays unchanged;
// an endpoint without it refuses describe as not wired, never with a guess.
// selector carries only the identity a ctl can know (Component, and for an
// agent SID/ConfiguredNID); nonce is the caller's fresh 32-hex nonce.
// origin: hot-update S8 plan §2.1 / §3.1 / §7.1; S8 FIX-DESCRIBE (P19 request R1)
type Describer interface {
	Describe(ctx context.Context, selector upgradetx.Key, nonce string) (Description, error)
}

// RuntimeDescriber is the runtime half of Describe. The runtime adapter owns
// the running image, its owners and the identity census; the endpoint only
// serializes the call with its other operations and checks the binding.
type RuntimeDescriber interface {
	DescribeRuntime(nonce string) (Description, error)
}
