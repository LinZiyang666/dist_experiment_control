//go:build linux

package main

import (
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"strings"
	"testing"
	"time"
)

// The live command uses its durable parent and the real signed reservation.
// A negative preflight port holds execution briefly and refuses it; no image
// completion is synthesized. The command must join its keeper on that HALT.
// origin: hot-update S8 plan §5.1 / §6.3; S8 internal review upgrade-maketest-2 F1
func exerciseSignedRollKeeper(t *testing.T, binary string) {
	t.Helper()
	f := startSignedRollFixture(t, binary, false)
	plan := clusterupgrade.Plan{Scope: clusterupgrade.ScopeBrokerDaemon}
	for _, p := range f.processes {
		plan.Steps = append(plan.Steps, clusterupgrade.Step{Kind: clusterupgrade.StepUpgrade, NodeID: p.id})
	}
	var out syncBuffer
	cmd := f.command(t)
	cmd.SetOut(&out)
	err := driveUpgrade(cmd, f.nc, f.actor, f.seed, plan, signedRollRelease, f.sha, "")
	if err == nil || !strings.Contains(err.Error(), "injected preflight refusal") {
		t.Fatalf("the injected refusal must HALT the live roll: %v\n%s", err, out.String())
	}
	acquired, renews := false, 0
	holder := ""
	for _, r := range f.wire() {
		if r.Inner == "roll.reserve_cluster" {
			acquired = true
			holder = r.Holder
		}
		if r.Op == "renew-lock" {
			renews++
			if holder != "" && r.Holder != holder {
				t.Fatal("renewal changed the original parent holder")
			}
		}
	}
	if !acquired {
		t.Fatal("fixture precondition: the roll never took the lock")
	}
	if renews == 0 {
		t.Fatal("driveUpgrade took the roll lock and NEVER RENEWED IT; the keeper exists but nothing calls it")
	}
	if err = f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	settled := len(f.wire())
	// As in the original counterexample, this is an observation of forbidden
	// post-HALT traffic, not a readiness sleep or a shortened product lease.
	time.Sleep(30 * time.Millisecond)
	for _, r := range f.wire()[settled:] {
		if r.Op == "renew-lock" {
			t.Fatal("renewals arrived AFTER driveUpgrade returned; the keeper outlived HALT")
		}
	}
}
