// The legacy timer retains its captured marker and is joined on every Run exit.
// origin: hot-update S8 plan §3.3 / §12.1 / P14.
package agentupgrade

import (
	"context"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

func ArmLegacyWatchdog(ctx context.Context, wait time.Duration, joined *sync.WaitGroup, rollback func() bool) {
	timer := time.NewTimer(wait)
	joined.Add(1)
	go func() {
		defer joined.Done()
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if rollback() {
					timer.Reset(upgradebudget.RollbackCutRetry)
					continue
				}
				return
			}
		}
	}()
}
