// registry.go binds the S8 admission registry to the one cut this
// broker imported. The registry is constructed from the accepted journal
// attempt's original scope, never from a later live roster, and its domain
// observers are the runtime's identity census filtered by domain.
// origin: hot-update S8 plan §2.2 / §4.2 / P03 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
package brokerupgradeport

import (
	"sync"

	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// CutRegistry is the importing target's completion registry. A domain the
// scope marks not_applicable is bound as such; every other domain is observed
// through one census-only sample per stamp, split by domain (eleven fresh
// samples per seal would each read every owner again).
func CutRegistry(runtime upgradeport.RuntimePort, journal upgradetx.Store, cut upgradecut.State) (upgradeobserve.Registry, error) {
	view, err := journal.Load(cut.Attempt.Key, cut.Attempt.TxID)
	if err != nil {
		return nil, err
	}
	a := active(view.Historical)
	if !upgradetx.SameCut(a.Handle, cut.Attempt) {
		return nil, refusal(upgradetx.CodeRejected, "inherited cut is not the accepted attempt")
	}
	expected, err := upgradetx.ExpectedDigest(a.Scope)
	if err != nil {
		return nil, err
	}
	var cache struct {
		sync.Mutex
		stamp upgradetx.Stamp
		o     upgradetx.Observation
		err   error
		valid bool
	}
	sample := func(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
		cache.Lock()
		defer cache.Unlock()
		if !cache.valid || cache.stamp != stamp {
			cache.o, cache.err = observeCensus(runtime, stamp)
			cache.stamp, cache.valid = stamp, true
		}
		return cache.o, cache.err
	}
	observers := make([]upgradeobserve.DomainObserver, 0, len(allDomains()))
	for _, domain := range allDomains() {
		binding := upgradeobserve.DomainObserver{Domain: domain, CapabilityDigest: digest(struct {
			Domain upgradetx.Domain
			Target string
		}{domain, cut.TargetSHA})}
		for _, row := range a.Scope.Expected {
			binding.NotApplicable = binding.NotApplicable || row.Domain == domain && upgradetx.ResourceNotApplicable(row)
		}
		if !binding.NotApplicable {
			binding.Observe = func(stamp upgradetx.Stamp) (upgradetx.Census, error) {
				o, err := sample(stamp)
				if err != nil || !o.Census.Known {
					return upgradetx.Census{}, accepted(refusal(upgradetx.CodeUnsettled, ReasonDomainCensus))
				}
				census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, ExpectedDigest: expected}
				for _, row := range o.Census.Resources {
					if row.Domain == domain {
						census.Resources = append(census.Resources, row)
					}
				}
				return census, nil
			}
		}
		observers = append(observers, binding)
	}
	// The registry's probe contract echoes the requested stamp (nonce,
	// sequence and sampling time) in Census.Stamp and At; the registry then
	// supplies the census itself.
	probe := func(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
		o, err := runtime.Observe(stamp)
		if err != nil {
			return o, err
		}
		o.Census.Stamp, o.At = stamp, stamp.At
		return o, nil
	}
	registry, err := upgradeobserve.New(upgradeobserve.Config{Attempt: a, Producer: cut.Owner, Observers: observers, Probe: probe})
	if err != nil {
		return nil, err
	}
	// The sealed adjunct names every identity the final export carried. Those
	// created after the grant's original scope (Prepare) — a transfer, a tunnel
	// listener, a session — were admitted by the source before it had a
	// registry; they are registered here, before any completion sample, as the
	// admissions they are. An identity in neither set still refuses at Seal.
	// origin: hot-update S8 plan §4.2 ("scope 外迟到创建也必须纳入共享登记序列"); S8 BROKER-EXPORTER R-11
	original := make(map[string]bool, len(a.Scope.Expected))
	for _, row := range a.Scope.Expected {
		original[string(row.Domain)+"\x00"+row.ID+"\x00"+row.Origin] = true
	}
	for _, row := range cut.Scope.Expected {
		if original[string(row.Domain)+"\x00"+row.ID+"\x00"+row.Origin] {
			continue
		}
		sequence, err := registry.Begin(row)
		if err == nil {
			err = registry.Finish(sequence, row)
		}
		if err != nil {
			return nil, err
		}
	}
	// The importing runtime stamps its own samples with this registry's
	// admission sequence (brokerRuntime.stamp).
	if rt, ok := runtime.(*brokerRuntime); ok {
		rt.mu.Lock()
		rt.registry = registry
		rt.mu.Unlock()
	}
	return registry, nil
}

// observeCensus is a census-only sample: the production adapter skips its
// control probe; any other runtime is observed in full.
func observeCensus(runtime upgradeport.RuntimePort, stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	if rt, ok := runtime.(*brokerRuntime); ok {
		return rt.observe(stamp, false)
	}
	return runtime.Observe(stamp)
}
