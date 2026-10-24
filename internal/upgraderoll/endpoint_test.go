package upgraderoll

import (
	"context"
	"github.com/LinZiyang666/tether/internal/testharness"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// The existing driver boundary model supplies holder/fleet facts only. These
// tests exercise the real endpoint's immutable-plan and original-token checks;
// signed trigger delivery and the broker's live collectors are integration work.
type controlModel struct {
	port     *rollPort
	lease    *rollReservation
	deadline int64
}

func (c *controlModel) Reserve(ctx context.Context, p upgradetx.Plan) error {
	_, err := c.port.Reserve(ctx, p)
	return err
}
func (c *controlModel) Check(ctx context.Context, p upgradetx.Plan, n string) (FleetProof, error) {
	return c.lease.Check(ctx, p, n)
}
func (c *controlModel) Renew(ctx context.Context, h upgradetx.Handle, d int64) error {
	c.deadline = d
	return c.lease.Renew(ctx, h)
}
func (c *controlModel) Release(ctx context.Context, _ upgradetx.Plan, s upgradetx.Step, n string) (upgradetx.Receipt, error) {
	return c.lease.Release(ctx, s, n)
}
func (c *controlModel) Artifact(ctx context.Context, g upgradetx.Grant) (upgradeartifact.Receipt, error) {
	return c.port.Artifact(ctx, g)
}
func (c *controlModel) Converge(ctx context.Context, s upgradetx.Step, n string) (upgradetx.Receipt, error) {
	return c.port.Converge(ctx, s, n)
}
func (c *controlModel) ObserveParent(ctx context.Context, p upgradetx.Plan, n string) (upgradetx.Observation, error) {
	return c.port.ObserveParent(ctx, p, n)
}
func (c *controlModel) RenewalInterval() time.Duration { return c.lease.RenewalInterval() }

// origin: S8 internal review remaining-orchestration F6 (R6)
func TestRollEndpointRetainsTheOriginalReservation(t *testing.T) {
	model, port := newRollModel(t)
	plan := clonePlan(model.plan)
	if _, err := model.SealPlan(plan); err != nil {
		t.Fatal(err)
	}
	control := &controlModel{port: port, lease: &rollReservation{model: model}}
	endpoint, err := BindEndpoint(model, port, control, plan.Parent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := endpoint.Reserve(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Renew(t.Context(), plan.Parent); err != nil || control.deadline != plan.Deadline {
		t.Fatalf("renew changed absolute deadline: %d %v", control.deadline, err)
	}
	if err := lease.Stop(); err != nil || !model.holder {
		t.Fatalf("driver stop released daemon holder: %v", err)
	}
	changed := clonePlan(plan)
	changed.TargetManifestDigest = testSHA(999)
	if _, err := endpoint.Reserve(t.Context(), changed); err == nil {
		t.Fatal("changed plan acquired the original holder")
	}
	if _, err := lease.Check(t.Context(), changed, testID(999)); err == nil {
		t.Fatal("changed plan obtained fleet proof")
	}
	wrong := plan.Parent
	wrong.CutID = testID(999)
	if err := lease.Renew(t.Context(), wrong); err == nil {
		t.Fatal("another cut renewed the holder")
	}
	if _, err := lease.Release(t.Context(), plan.Steps[0], testID(999)); err == nil {
		t.Fatal("component cut reached holder release")
	}
	release := plan.Steps[len(plan.Steps)-1]
	release.DependsOn = nil
	if _, err := lease.Release(t.Context(), release, testID(999)); err == nil {
		t.Fatal("changed release step reached holder CAS")
	}
	home := plan.Steps[2]
	home.DependsOn = nil
	if _, err := endpoint.Converge(t.Context(), home, testID(999)); err == nil {
		t.Fatal("changed home step reached collector")
	}
	grant := plan.Steps[0].Grant
	grant.Handle.TxID = testID(999)
	if _, err := endpoint.Artifact(t.Context(), grant); err == nil {
		t.Fatal("unstaged child adopted another artifact")
	}
	if _, err := endpoint.ObserveParent(t.Context(), changed, testID(999)); err == nil {
		t.Fatal("parent observer accepted a different snapshot")
	}
	if model.reserveCalls != 1 || !model.holder {
		t.Fatal("rejected operations mutated holder")
	}
}

// origin: S8 internal review remaining-orchestration F6 (R6)
func TestRollEndpointRunsOnlyTheSealedSteps(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBoundRoll(t)
	}
	testharness.AssertNoGoroutineLeak(t, "bound roll endpoint x5", g)
	testharness.AssertNoFDLeak(t, "bound roll endpoint x5", fd, 0)
}

func exerciseBoundRoll(t *testing.T) {
	t.Helper()
	model, port := newRollModel(t)
	control := &controlModel{port: port, lease: &rollReservation{model: model}}
	endpoint, err := BindEndpoint(model, port, control, model.plan.Parent)
	if err != nil {
		t.Fatal(err)
	}
	view, err := Run(t.Context(), model, endpoint, model.plan)
	if err != nil || view.Historical.Forward.Phase != upgradetx.PhaseCompleted {
		t.Fatalf("bound roll: %v", err)
	}
	original := clonePlan(*model.sealed)
	starts := model.startCalls
	_, err = Run(t.Context(), model, endpoint, original)
	if err != nil || model.startCalls != starts || !reflect.DeepEqual(original, *model.sealed) {
		t.Fatalf("resume re-planned or restarted a child: %v", err)
	}
}
