package broker

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S7 plan §8.2 / T39
func TestTransferImageJoinPropagatesBorrowerDeadline(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferImageJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "image transfer borrowers", beforeG)
	testharness.AssertNoFDLeak(t, "image transfer borrowers", beforeFD, 0)
}
func exerciseTransferImageJoin(t *testing.T) {
	t.Helper()
	for _, audit := range []bool{true, false} {
		b := &Broker{cl: &clusterRuntime{transferRPC: &TransferClusterForward{}}}
		c := &brokerImageControl{broker: b}
		users := &b.cl.transferRPC.users
		if audit {
			users = &b.cl.transferAuditUsers
		}
		if !users.Begin() {
			t.Fatal("initial borrower rejected")
		}
		deadline, err := controlbudget.After(10 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		err = c.JoinStoreUsers(deadline)
		users.End()
		if !controlbudget.IsExpired(err) || time.Since(start) > time.Second {
			t.Fatalf("audit=%v cut ignored actual borrower: %v", audit, err)
		}
		if users.Begin() {
			users.End()
			t.Fatal("refused join silently reopened before backend restore")
		}
		if err := c.ResumeStoreUsers(); err != nil {
			t.Fatal(err)
		}
		if !users.Begin() {
			t.Fatal("restored binding did not resume its retained borrower")
		}
		users.End()
		deadline, err = controlbudget.After(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.JoinStoreUsers(deadline); err != nil {
			t.Fatal(err)
		}
	}
}
