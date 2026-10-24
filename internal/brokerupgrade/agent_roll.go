package brokerupgrade

import (
	"github.com/LinZiyang666/tether/internal/brokerupgrade/agentroll"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/xferauth"
)

// WithAgentRollAuthority binds roll authentication to the broker's current
// authority at callback time. Capturing a seed here would leave a forwarded
// sample using authority from before a configuration or owner change.
// origin: S10 internal review ARCH-REDS-3 F1; hot-update S8 plan §7.2
func WithAgentRollAuthority(r AgentRoute, authority func() *xferauth.CalloutConfig) AgentRoute {
	r.VerifyRoll = func(request rollcontrol.Request) error {
		if authority() == nil {
			return &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "account authority unavailable"}
		}
		return rollcontrol.VerifyAgentRequest(authority().AccountSeed, request)
	}
	r.SignSample = func(sample rollcontrol.Sample) (rollcontrol.Sample, error) {
		return rollcontrol.SignSample(authority().AccountSeed, sample)
	}
	return r
}

// The existing forwarder owns admission, capacity and joined request lifetimes.
// Only roll-specific policy and sample authentication live in the typed leaf.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §7.2
func rollRoute(r AgentRoute) agentroll.Route {
	return agentroll.Route{Capable: AgentTransactionCapable(r.DB, r.SID, r.NID), DB: r.DB, SID: r.SID, NID: r.NID, Actor: r.Actor, ActorFP: r.ActorFP, Reply: r.Reply, Body: r.Body, VerifyRoll: r.VerifyRoll, SignSample: r.SignSample}
}
func admitAgentRoll(r AgentRoute) (agentForward, error) {
	f, err := agentroll.Admit(rollRoute(r))
	return agentForward{subject: f.Subject, envelope: f.Envelope, read: f.Read, roll: f.Roll}, err
}
