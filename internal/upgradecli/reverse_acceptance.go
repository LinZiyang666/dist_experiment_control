package upgradecli

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Only an accepted reverse bound to this exact request may turn its immediate
// unsettled reply into Wait. Missing identity or a lost reply cannot authorize
// another reverse, a guessed TxID, or a completion claim.
// origin: hot-update S8 plan §3.1 / §7.1 / §8.1; S8 internal review upgrade-maketest F8
func acceptedReverseReply(v upgradetx.View, err error, f upgradetx.Fence, r upgradetx.ReverseRequest) bool {
	var typed *upgradetx.Error
	if !errors.As(err, &typed) || !typed.Accepted || typed.Code != upgradetx.CodeUnsettled {
		return false
	}
	g := v.Historical.Grant
	a := v.Historical.Forward
	if v.Historical.Reverse.Handle.TxID != "" {
		a = v.Historical.Reverse
	}
	scope, e := upgradetx.ScopeDigest(r.Scope)
	return e == nil && g.Handle.Key == f.Current.Key && v.Historical.Key == f.Current.Key &&
		hexValue(g.Handle.TxID, 32) && hexValue(g.Handle.CutID, 32) && g.Handle.CutID != f.Current.CutID &&
		f.Current.Generation != ^uint64(0) && g.Handle.Generation == f.Current.Generation+1 &&
		g.ActorFP == r.ActorFP && g.ClientNonce == r.ClientNonce && g.Issuer == f.Owner &&
		g.SourceSHA == f.RunningSHA && g.TargetSHA == r.TargetSHA && g.ScopeDigest == scope &&
		upgradetx.SameCut(a.Handle, g.Handle) && a.Direction == upgradetx.DirectionReverse &&
		a.Phase == upgradetx.PhaseActivating && a.CutStage == upgradetx.CutPartialActive &&
		a.SourceGeneration == f.Current.Generation && a.SourceSHA == g.SourceSHA && a.TargetSHA == g.TargetSHA
}

// Keep the original authority and retained identities, then seal every current
// identity before issuing the reverse grant. A failed census never means zero.
// origin: hot-update S8 plan §8.1 R2; S8 internal review upgrade-maketest F9
func currentReverseScope(original upgradetx.Scope, census upgradetx.Census) (upgradetx.Scope, error) {
	if !census.Known || !census.Complete {
		return upgradetx.Scope{}, unsettled("reverse resource census is unknown")
	}
	scope := original
	scope.Expected = append([]upgradetx.Resource{}, original.Expected...)
	seen := make(map[string]bool, len(scope.Expected))
	for _, r := range scope.Expected {
		seen[string(r.Domain)+"\x00"+r.ID] = true
	}
	for _, r := range census.Resources {
		if !seen[string(r.Domain)+"\x00"+r.ID] {
			scope.Expected = append(scope.Expected, r)
			seen[string(r.Domain)+"\x00"+r.ID] = true
		}
	}
	return upgradetx.CanonicalScope(scope)
}
