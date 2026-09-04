package authcallout

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestVerifyPINWithBudgetCheckAndChargeIsAtomic covers the concurrent leader-local
// and cluster.apply subscriptions. A pure TokensAt check followed by a separate AllowN
// charge lets every waiter observe the same last token before any of them consumes it.
func TestVerifyPINWithBudgetCheckAndChargeIsAtomic(t *testing.T) {
	oldRate, oldBurst := pinGlobalPerSecond, pinGlobalBurst
	pinGlobalPerSecond, pinGlobalBurst = 0, 1
	t.Cleanup(func() { pinGlobalPerSecond, pinGlobalBurst = oldRate, oldBurst })

	const workers = 64
	var clockCalls atomic.Int32
	firstWaveReady := make(chan struct{})
	secondWaveReady := make(chan struct{})
	releaseFirstWave := make(chan struct{})
	releaseSecondWave := make(chan struct{})
	h := &Handler{Now: func() time.Time {
		n := clockCalls.Add(1)
		if n <= workers {
			if n == workers {
				close(firstWaveReady)
			}
			<-releaseFirstWave
		} else {
			// In the current split implementation this is spendPINBudget's
			// second clock read. Hold every spender here until all checkers
			// have observed the last token.
			if n == 2*workers {
				close(secondWaveReady)
			}
			<-releaseSecondWave
		}
		return time.Unix(1, 0)
	}}
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- h.VerifyPINWithBudget("pin", "not-a-phc")
		}()
	}
	close(start)
	select {
	case <-firstWaveReady:
	case <-time.After(5 * time.Second):
		close(releaseFirstWave)
		close(releaseSecondWave)
		t.Fatalf("only %d/%d callers reached the first budget check", clockCalls.Load(), workers)
	}
	close(releaseFirstWave)
	// A correct atomic check-and-charge admits one caller, so at most one second
	// clock read occurs and this times out. The split implementation admits every
	// checker into its charge step and closes secondWaveReady.
	select {
	case <-secondWaveReady:
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseSecondWave)
	wg.Wait()
	close(results)

	admitted := 0
	for err := range results {
		if !errors.Is(err, ErrPINRateLimited) {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("burst=1 admitted %d concurrent verifies, want 1; the leader budget check and charge are not atomic", admitted)
	}
}
