//go:build linux

package clusterharness

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S8 internal review P13-COHORT F2; hot-update S8 plan §5.1 / L21
func TestBrokerCohortCleanupRequiresItsOriginalParentSettlement(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseCohortParentSettlement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "cohort parent cleanup x5", g)
	testharness.AssertNoFDLeak(t, "cohort parent cleanup x5", fds, 0)
}

func exerciseCohortParentSettlement(t *testing.T) {
	t.Helper()
	failure := errors.New("accepted parent still owns its reservation")
	q := &productQuorum{cutStarted: true, cutDone: make(chan struct{}), cutErr: failure}
	close(q.cutDone)
	if err := q.finish(); !errors.Is(err, failure) {
		t.Fatal("a successful cohort hid its unsettled parent")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	q = &productQuorum{cutDone: make(chan struct{}), cutCancel: cancel}
	go func() { <-ctx.Done(); close(q.cutDone) }()
	if err := q.finish(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.cutDone:
	default:
		t.Fatal("aborted preparation left its driver unjoined")
	}
}
