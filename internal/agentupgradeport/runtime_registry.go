// runtime_registry.go binds the S8 admission registry to the one cut this
// process imported. The registry is constructed from the accepted journal
// attempt's original scope, never from a later live roster, and its domain
// observers are the runtime adapter's identity census filtered by domain.
// origin: hot-update S8 plan §2.2 / §4.2 / P03; S8 FIX-AGENTWIRE
package agentupgradeport

import (
	"sync"

	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

var registryDomains = [...]upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore,
	upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS,
	upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}

// CutRegistry is the target's completion registry. A domain the scope marks
// not_applicable is bound as such; every other domain is observed through the
// same fresh census as Observe, echoing the registry's stamp.
func CutRegistry(runtime upgradeport.RuntimePort, journal upgradetx.Store, cut upgradecut.State) (upgradeobserve.Registry, error) {
	view, err := journal.Load(cut.Attempt.Key, cut.Attempt.TxID)
	if err != nil {
		return nil, err
	}
	a := current(view.Historical)
	if !upgradetx.SameCut(a.Handle, cut.Attempt) {
		return nil, refusal(upgradetx.CodeRejected, "inherited cut is not the accepted attempt", true)
	}
	expected, err := upgradetx.ExpectedDigest(a.Scope)
	if err != nil {
		return nil, err
	}
	// The registry asks every domain observer for the same stamp in turn: one
	// census sample at that stamp is split by domain rather than re-taken per
	// domain, and it needs no control probe (only the registry's own probe
	// signs control). Eleven fresh samples per seal each froze the runtime.
	// origin: hot-update S8 plan §4.2; S8 FIX-DESCRIBE (real-cut finding)
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
	observers := make([]upgradeobserve.DomainObserver, 0, len(registryDomains))
	for _, domain := range registryDomains {
		binding := upgradeobserve.DomainObserver{Domain: domain, CapabilityDigest: sha256Hex(struct {
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
					return upgradetx.Census{}, refusal(upgradetx.CodeUnsettled, ReasonDomainCensus, true)
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
	// supplies the census itself. The runtime's own stamp carries its private
	// sequence, so passing Observe through unchanged failed every Seal with
	// "running image observation failed" after a real cut.
	// origin: hot-update S8 plan §4.2 / P03 contract; S8 FIX-DESCRIBE (real-cut finding)
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
	// The importing runtime stamps its own samples with this registry's
	// admission sequence (see runtimePort.stamp).
	// origin: hot-update S8 plan §4.2 / M04; S8 FIX-DESCRIBE (real-cut finding)
	if port, ok := runtime.(*runtimePort); ok {
		port.mu.Lock()
		port.registry = registry
		port.mu.Unlock()
	}
	return registry, nil
}

// observeCensus is a census-only sample: the production adapter skips its
// control probe; any other runtime is observed in full.
func observeCensus(runtime upgradeport.RuntimePort, stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	if port, ok := runtime.(*runtimePort); ok {
		return port.observe(stamp, false)
	}
	return runtime.Observe(stamp)
}

// unwiredRegistry is the source process's registry before any cut was
// imported: Prepare/Start need none, and completion can never be signed here.
type unwiredRegistry struct{}

// UnwiredRegistry refuses every completion operation with upgrade_not_wired.
func UnwiredRegistry() upgradeobserve.Registry { return unwiredRegistry{} }

func notWired() error {
	return &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "completion registry belongs to the importing process"}
}

func (unwiredRegistry) Begin(upgradetx.Resource) (uint64, error) { return 0, notWired() }
func (unwiredRegistry) Finish(uint64, upgradetx.Resource) error  { return notWired() }
func (unwiredRegistry) Snapshot(upgradetx.Stamp) (upgradetx.Census, error) {
	return upgradetx.Census{}, notWired()
}
func (unwiredRegistry) Seal(upgradetx.Attempt, string) (upgradetx.Receipt, error) {
	return upgradetx.Receipt{}, notWired()
}
