package ingressswitch

import (
	"crypto/sha256"
	"encoding/json"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Resolve the ACTIVE kernel generation, not a caller's remembered switch
// receipt. RetirementSeal checks both address families, accepts, SYN, and
// conntrack with the rule barrier held across the independent census.
// origin: hot-update S9 plan §5.2 / WIRE-2 R11
func VerifyCurrent(s *Switch, inspect func(c.ID, c.Deadline) (c.Census, error), q c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if s == nil || inspect == nil {
		return c.HostReceipt{}, c.ErrContractStub
	}
	if q.Op != c.VerifyIngress || q.Component != c.Caddy {
		return c.HostReceipt{}, c.ErrUnsupported
	}
	rules, err := s.backend.Read(d, stop)
	if err != nil {
		return c.HostReceipt{}, err
	}
	change, err := s.records.Load(rules.Digest)
	if err != nil || change.After != rules || change.Domain != q.Domain || change.Tx != q.Tx || change.Step != q.Step {
		return c.HostReceipt{}, c.ErrIncomplete
	}
	old, target := change.Primary, change.Shadow
	if !rules.Redirect {
		old, target = change.Shadow, change.Primary
	}
	encoded, err := json.Marshal(target)
	if err != nil || old != q.Expected || sha256.Sum256(encoded) != q.Payload {
		return c.HostReceipt{}, c.ErrIncomplete
	}
	seal, err := s.RetirementSeal(change, d, stop)
	if err != nil {
		return c.HostReceipt{}, err
	}
	census, err := inspect(q.Domain, d)
	if err != nil || !census.Complete || census.Generation == 0 || census.Digest != q.BeforeCensus ||
		seal.Owner != old || seal.Census.OldOwnerPending != 0 {
		return c.HostReceipt{}, c.ErrIncomplete
	}
	found := false
	for _, process := range census.Processes {
		found = found || process == target
	}
	// Re-read after the full census as well: a sealed earlier generation is no
	// authority for a later canonical route, even when its counts were all zero.
	after, err := s.backend.Read(d, stop)
	if err != nil || after != rules || !found {
		return c.HostReceipt{}, c.ErrIncomplete
	}
	return c.HostReceipt{Current: target, Census: census}, nil
}
