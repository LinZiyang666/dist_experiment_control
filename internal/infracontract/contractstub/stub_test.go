package contractstub

import (
	"errors"
	"reflect"
	"runtime"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// No adapter may claim readiness, completion or release, even for zero inputs or
// an already stopped call. Five rounds expose an accidental per-call worker/FD.
// origin: hot-update S9 plan §12 P00 / T01 / M02
func TestInfraContractStubLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseContractStub(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra contract stubs", beforeG)
	testharness.AssertNoFDLeak(t, "infra contract stubs", beforeFD, 0)
}

func exerciseContractStub(t *testing.T) {
	t.Helper()
	deps := NewDependencies()
	for i := 0; i < reflect.TypeFor[c.Dependencies]().NumField(); i++ {
		if reflect.ValueOf(deps).Field(i).IsNil() {
			t.Fatalf("missing inert dependency %s", reflect.TypeFor[c.Dependencies]().Field(i).Name)
		}
	}
	for _, d := range []c.Dependencies{deps, {}} {
		got, err := NewDriver(d)
		assertFailure(t, got, err, "P10", "P13")
		svc, err := NewService(d, driver{}, driver{})
		assertFailure(t, svc, err, "P20")
		svc, err = NewService(d, nil, nil)
		assertFailure(t, svc, err, "P20")
	}
	closed := make(chan struct{})
	close(closed)
	for _, stop := range []c.Stop{nil, closed} {
		r, err := deps.Host.Do(c.HostRequest{}, 0, stop)
		assertFailure(t, r, err, "P04")
		// This independent owner list detects a stub silently naming itself or
		// the coordinator instead of the backend that must implement the verb.
		owners := []string{"P02", "P04", "P05", "P04", "P02", "P02", "P02", "P05", "P07", "P09", "P05", "P05", "P04", "P03", "P04", "P06", "P06"}
		for i, owner := range owners {
			r, err = deps.Host.Do(c.HostRequest{Op: c.HostOp(i + 1)}, 1, stop)
			assertFailure(t, r, err, owner)
		}
		census, err := deps.Host.Inspect(c.ID{}, 1)
		assertFailure(t, census, err, "P02")
		a, err := deps.Artifacts.Resolve(c.NATSBinary, "2.14.6", "linux", "amd64")
		assertFailure(t, a, err, "P01")
		assertFailure(t, nil, deps.Artifacts.Verify(a, 1, stop), "P01")
		assertFailure(t, nil, deps.Artifacts.Compatible(a, a, c.Digest{}, c.Store{}, 1), "P01")
		ready, err := deps.Ready.Read(c.Plan{}, 1, stop)
		assertFailure(t, ready, err, "P11")
		cp, err := deps.Cohort.Seal(c.Plan{}, 1)
		assertFailure(t, cp, err, "P15")
		assertFailure(t, nil, deps.Cohort.Install(cp, c.RouteProof{}, 1), "P15")
		move, err := deps.Cohort.Move(cp, 1, stop)
		assertFailure(t, move, err, "P15")
		move, err = deps.Cohort.Return(cp, 1, stop)
		assertFailure(t, move, err, "P15")
		census, err = deps.Cohort.Bind(cp, nil, 1)
		assertFailure(t, census, err, "P15")
		assertFailure(t, nil, deps.Cohort.Release(cp, 1), "P15")
		proofs, err := deps.Routes.Prepare(c.Plan{}, cp, c.RouteOutbound, 1)
		assertFailure(t, proofs, err, "P16")
		proofs, err = deps.Routes.Bind(c.Plan{}, move, c.RouteReturn, 1)
		assertFailure(t, proofs, err, "P16")
		seal, err := deps.Transfers.Fence(c.ID{}, 1)
		assertFailure(t, seal, err, "P19")
		assertFailure(t, nil, deps.Transfers.Rebind(seal, 1), "P19")
		assertFailure(t, nil, deps.Transfers.Release(seal, 1), "P19")
		cp, err = deps.Recovery.Seal(c.Plan{}, 1)
		assertFailure(t, cp, err, "P18")
		assertFailure(t, nil, deps.Recovery.VerifyBeforeReplay(cp, 1, stop), "P18")
		assertFailure(t, nil, deps.Recovery.Release(cp, 1), "P18")
		status, err := deps.Tx.Acquire(c.Plan{}, 1)
		assertFailure(t, status, err, "P20")
		status, err = deps.Tx.CompareAndSwap(status, c.Completed, c.Digest{}, 1)
		assertFailure(t, status, err, "P20")
		status, err = deps.Tx.Load(c.ID{}, 1)
		assertFailure(t, status, err, "P20")
		assertFailure(t, nil, deps.Observe.Record(c.Observation{Kind: "completed"}), "P02")
		for _, p := range []c.Plan{{Component: c.NATS}, {Component: c.Caddy}, {}} {
			owners := []string{"P10", "P13"}
			if p.Component == c.NATS {
				owners = []string{"P13"}
			}
			if p.Component == c.Caddy {
				owners = []string{"P10"}
			}
			status, err = (driver{}).Advance(p, 1, stop)
			assertFailure(t, status, err, owners...)
			status, err = (driver{}).Recover(p, 1, stop)
			assertFailure(t, status, err, owners...)
		}
		status, err = (service{}).Start(c.Plan{}, 1, stop)
		assertFailure(t, status, err, "P20")
		status, err = (service{}).Resume(c.ID{}, 1, stop)
		assertFailure(t, status, err, "P20")
		status, err = (service{}).Status(c.ID{}, 1)
		assertFailure(t, status, err, "P20")
	}
}

func assertFailure(t *testing.T, result interface{}, err error, owners ...string) {
	t.Helper()
	want := ""
	for _, owner := range owners {
		if want != "" {
			want += "\n"
		}
		want += "not implemented (S9 " + owner + ")"
	}
	if !errors.Is(err, c.ErrContractStub) || err.Error() != want {
		t.Fatalf("stub error = %v; want %q wrapping ErrContractStub", err, want)
	}
	if result != nil && !reflect.ValueOf(result).IsZero() {
		t.Fatalf("stub returned nonzero receipt: %T", result)
	}
}
