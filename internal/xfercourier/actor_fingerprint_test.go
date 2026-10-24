package xfercourier

import "testing"

// TestTransferCourierAcceptsCanonicalActorFingerprint: a real actor is the
// canonical "SHA256:" fingerprint, raw standard base64 that contains '/' and
// '+'. Requiring it to be a subject token refused every real courier; a
// malformed fingerprint, a wildcard or a subject separator is still refused.
// origin: hot-update S7 plan §6.2 / INTEG-D agent wiring (ActorFP fingerprint)
func TestTransferCourierAcceptsCanonicalActorFingerprint(t *testing.T) {
	f := newCourierFixture(t, "complete")
	for actor, accepted := range map[string]bool{
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf/u0": true,
		"plain-actor-token": true,
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf/u":  false,
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf*u0": false,
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf.u0": false,
		"actor with space": false,
	} {
		cfg := f.config
		cfg.Identity.ActorFP = actor
		cfg.Capsule.Identity = cfg.Identity
		cfg.Outcome.Grant.Identity = cfg.Identity
		if err := validateConfig(cfg); (err == nil) != accepted {
			t.Errorf("actor %q: accepted=%v, want %v (%v)", actor, err == nil, accepted, err)
		}
	}
}
