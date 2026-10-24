//go:build linux

package clusterharness

import (
	"crypto/sha256"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Fixed hashes cover both directions, period wrap, the half-close tail and the
// final declared record. Concurrent callers must receive private writable bytes.
// origin: S10 internal review FINAL-REAL-FAILS F6 / S6 plan §6.3 / T48
func TestBrokerCohortPayloadKeepsItsIndependentBytePattern(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerCohortPayload(t)
	}
	testharness.AssertNoGoroutineLeak(t, "cohort payload periods x5", g)
	testharness.AssertNoFDLeak(t, "cohort payload periods x5", fd, 0)
}

func exerciseBrokerCohortPayload(t *testing.T) {
	t.Helper()
	cases := []struct {
		id        string
		direction uint8
		sequence  uint64
		sha       string
	}{
		{"ssh", 0, 0, "38103f530b7f0a6b6d7a7c029d578b56d607ac0c4f2929fa17a00a4f324d0e33"},
		{"ssh", 1, 0, "4dd74ce7344c6f111c00c56c6eef26eeffc39734337f01a69b1e451cbed811fa"},
		{"tcp-active", 0, 255, "bcf6a288a61b13a7d63b27d7beb26d416ddf32311011578fad875b2362be3d1e"},
		{"tcp-active", 1, 255, "45f17c35fbc5b7a0bcdb601c6eb9f6ef55c055a9d464bb4a65bc58fe74867d0a"},
		{"tcp-active", 0, 256, "65546d079db3b7673fae31904da2e28a14e609a8455148583c451f16c9d0a255"},
		{"tcp-half", 1, 1200, "615b71efe089974b4f988e3ba99ce40c5ada585595c9ebd5ebec045236a5dd59"},
		{"ss-bob", 0, 2047, "e96dcea13d2adb1c7c52996db753f6792c0795da2f3915294e73e77dfff64fb0"},
		{"ss-bob", 1, 2047, "3da62fb1d4f17b821797f2d7d89f460b07fcd9b9f6a3c9c23b02b7233beca242"},
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		workers.Go(func() {
			for _, c := range cases {
				body := cohortPayload(c.id, c.direction, c.sequence)
				if got := fmt.Sprintf("%x", sha256.Sum256(body)); got != c.sha {
					t.Errorf("%s direction=%d sequence=%d payload SHA=%s want=%s", c.id, c.direction, c.sequence, got, c.sha)
				}
				body[0] ^= 0xff
			}
		})
	}
	workers.Wait()
}
