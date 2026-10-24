//go:build linux && amd64

package infrahost

import (
	"errors"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// The cgroup/conntrack enumeration boundary is a sandbox. Reduction, domain
// admission and generation publication are production code; no privileged host
// operation runs and no production cgroup scope is widened for the fixture.
func censusBackendFixture(t *testing.T) (*identityBackend, c.Process) {
	t.Helper()
	old := c.Process{BootID: "boot", PID: 100, StartTicks: 1, InvocationID: identityInvocation,
		ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{1}, Cgroup: "/system.slice/caddy.service"}
	host, err := NewIdentityBackend(CensusScope{Domain: c.ID{2}, Generation: 7,
		Cgroups: []string{old.Cgroup}, Before: []c.Process{old}, Addresses: []string{"0.0.0.0:443"}})
	if err != nil {
		t.Fatal(err)
	}
	return host.(*identityBackend), old
}

// origin: S9 internal review rv-infra F05
func TestUnitStartupFenceInspectsPhysicalDomain(t *testing.T) {
	for _, component := range []c.Component{c.NATS, c.Caddy} {
		host, old := censusBackendFixture(t)
		host.read = func(scope CensusScope, _ c.Deadline) (c.Census, error) {
			return assembleCensus(scope, []c.Process{old}, nil, nil, nil)
		}
		f, request := unitTestFence(t)
		f.Component, f.Closed = component, false
		request.Component, request.Op, request.Expected, request.HostBootID = component, c.FenceStart, old, old.BootID
		request.BeforeCensus = c.Digest{8}
		request.Candidate = c.Artifact{}
		request.Config = c.Digest{}
		if request.Tx == request.Domain {
			t.Fatal("fixture aliases transaction and domain")
		}
		target := UnitCaddy
		if component == c.NATS {
			target = UnitNATS
		}
		policy := unitFixture(target, false)
		manager := &unitTestManager{}
		manager.units[target] = policy.Expected
		state := &unitTestState{f: f}
		units, err := NewUnitHost(UnitDependencies{Manager: manager, State: state, Physical: host, Policies: []UnitPolicy{policy}, Retire: host.Do})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := host.Inspect(request.Tx, f.Deadline); !errors.Is(err, c.ErrIncomplete) {
			t.Fatal("physical backend accepted Tx as domain")
		}
		receipt, err := units.Do(request, f.Deadline, nil)
		if err != nil || !receipt.Applied || !state.f.Closed {
			t.Fatalf("component %d cannot fence its domain: %v", component, err)
		}
	}
}

// origin: S9 internal review rv-infra F06
func TestPhysicalCensusGenerationTracksCompleteObservations(t *testing.T) {
	h, old := censusBackendFixture(t)
	warm := old
	warm.PID, warm.StartTicks, warm.ExeInode = 101, 2, 3
	processes := []c.Process{old}
	listener := diagSocket{socket: c.Socket{NetNS: 1, Device: 1, Inode: 8, Cookie: 9, Local: "0.0.0.0:443", Remote: "0.0.0.0:0"}, state: 10}
	accepted := diagSocket{socket: c.Socket{NetNS: 1, Device: 1, Inode: 10, Cookie: 11, Local: "127.0.0.1:443", Remote: "127.0.0.1:9000"}, state: 1}
	diag := []diagSocket{listener, accepted}
	owners := map[uint64][]c.Process{8: {old}, 10: {old}}
	incomplete := false
	h.read = func(scope CensusScope, _ c.Deadline) (c.Census, error) {
		if incomplete {
			return c.Census{}, c.ErrIncomplete
		}
		return assembleCensus(scope, processes, owners, diag, nil)
	}
	read := func() c.Census {
		t.Helper()
		v, err := h.Inspect(h.scope.Domain, identityDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	before := read()
	if repeat := read(); repeat.Generation != before.Generation || repeat.Digest != before.Digest {
		t.Fatal("unchanged observation invalidated the signed signal census")
	}
	for _, change := range []func(){
		func() { processes = append(processes, warm) },
		func() { diag = diag[:1]; delete(owners, 10) },
		func() { processes = []c.Process{warm}; diag = nil; owners = nil },
	} {
		change()
		next := read()
		if next.Generation <= before.Generation || next.Digest == before.Digest {
			t.Fatal("complete physical change reused a generation/digest")
		}
		before = next
	}
	incomplete = true
	if _, err := h.Inspect(h.scope.Domain, identityDeadline(t)); err == nil {
		t.Fatal("failed sample published a complete observation")
	}
	incomplete = false
	if next := read(); next.Generation != before.Generation || next.Digest != before.Digest {
		t.Fatal("incomplete observation advanced the trusted producer")
	}
}

// origin: S9 internal review rv-infra F07
func TestPhysicalListenerResponsibilityKeepsAcceptedAndPendingWork(t *testing.T) {
	h, old := censusBackendFixture(t)
	listener := diagSocket{socket: c.Socket{NetNS: 1, Device: 1, Inode: 8, Cookie: 9, Local: "0.0.0.0:443", Remote: "0.0.0.0:0"}, state: 10}
	for _, name := range []string{"empty", "accepted_TLS", "idle_HTTP", "slow_HTTP", "late_Upgrade", "SYN", "backlog", "conntrack", "unknown_UDP"} {
		t.Run(name, func(t *testing.T) {
			diag := []diagSocket{listener}
			owners := map[uint64][]c.Process{8: {old}}
			var flows []trackedFlow
			switch name {
			case "empty":
			case "accepted_TLS", "idle_HTTP", "slow_HTTP", "late_Upgrade":
				accepted := listener
				accepted.state, accepted.socket.Inode, accepted.socket.Cookie, accepted.socket.Remote = 1, 10, 11, "127.0.0.1:9000"
				diag = append(diag, accepted)
				owners[10] = []c.Process{old}
			case "SYN":
				pending := listener
				pending.state, pending.socket.Inode = 3, 0
				diag = append(diag, pending)
			case "backlog":
				diag[0].queue = 1
			case "conntrack":
				flows = []trackedFlow{{local: "127.0.0.1:443", remote: "127.0.0.1:9000", protocol: 6, state: 3}}
			case "unknown_UDP":
				diag[0].state = 7
			}
			census, err := assembleCensus(h.scope, []c.Process{old}, owners, diag, flows)
			if err != nil {
				t.Fatal(err)
			}
			if len(census.Sockets) == 0 {
				t.Fatal("producer hid the listener")
			}
			if (census.OldOwnerPending == 0) != (name == "empty") {
				t.Fatal("listener and live/pending responsibilities were conflated")
			}
		})
	}
}
