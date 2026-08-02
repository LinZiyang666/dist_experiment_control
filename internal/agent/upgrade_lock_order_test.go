package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
)

const upgradeLockOrderHelperEnv = "TETHER_TEST_UPGRADE_LOCK_ORDER_HELPER"

// origin: upgrade-safety external re-review F8 — install enters with the host
// flock and later takes upgradeMu, while watchdog/commit take upgradeMu and
// then block on the host flock. At the stale-deadline boundary a retrying
// install can therefore deadlock permanently with the prior watchdog or a
// late register. Run the exact lock cycle in a subprocess so the expected
// current deadlock can be killed without leaking goroutines into this suite.
func TestUpgradeHostLockOrderDoesNotDeadlock(t *testing.T) {
	if os.Getenv(upgradeLockOrderHelperEnv) == "1" {
		runUpgradeLockOrderHelper(t)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUpgradeHostLockOrderDoesNotDeadlock$")
	cmd.Env = append(os.Environ(), upgradeLockOrderHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("install/watchdog lock-order cycle deadlocked past 4s")
	}
	if err != nil {
		t.Fatalf("lock-order helper: %v\n%s", err, out)
	}
}

func runUpgradeLockOrderHelper(t *testing.T) {
	a, dst := installFixtureAgent(t)
	var stopped atomic.Bool
	installAtPrev := make(chan struct{})
	allowInstallToWriteMarker := make(chan struct{})
	upgradeSyncObserver = func(kind, _ string) error {
		if kind == "dir" && stopped.CompareAndSwap(false, true) {
			close(installAtPrev) // install holds host flock; prev is durable
			<-allowInstallToWriteMarker
		}
		return nil
	}
	t.Cleanup(func() { upgradeSyncObserver = nil })

	installDone := make(chan error, 1)
	go func() {
		installDone <- withUpgradeFileLock(dst, true, func() error {
			_, err := a.installNewBinary(testTarball(t, fakeVersionScript(t)), dst)
			return err
		})
	}()
	<-installAtPrev

	transitionHasMu := make(chan struct{})
	transitionDone := make(chan error, 1)
	go func() {
		a.upgradeMu.Lock()
		close(transitionHasMu)
		err := withUpgradeFileLock(dst, true, func() error { return nil })
		a.upgradeMu.Unlock()
		transitionDone <- err
	}()
	<-transitionHasMu
	close(allowInstallToWriteMarker)

	if err := <-installDone; err != nil {
		t.Fatal(err)
	}
	if err := <-transitionDone; err != nil {
		t.Fatal(err)
	}
}
