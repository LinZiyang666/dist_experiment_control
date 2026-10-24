// Package contractstub supplies permanent, inert S10 compile witnesses.
// A factory returns no owner: callers must fail before starting a real fixture.
// No stub opens files, starts workers, touches input, or fabricates a receipt.
// origin: hot-update S10 plan §5.1 / P00
package contractstub

import "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"

// The frozen sentinel text and the owner diagnostic serve different callers.
// Keep exact owner text while retaining errors.Is without a second sentinel.
type failure string

func (e failure) Error() string { return string(e) }
func (failure) Unwrap() error   { return contract.ErrContractStub }

func NewSnapshotter(contract.Profile) (contract.Snapshotter, error) {
	return nil, failure("not implemented (S10 P04)")
}
func NewDriver(contract.Profile) (contract.Driver, error) {
	return nil, failure("not implemented (S10 P18)")
}
func NewFixture(contract.Profile) (contract.Fixture, error) {
	return nil, failure("not implemented (S10 P19)")
}
func Plan(contract.Profile) ([]contract.Step, error) {
	return nil, failure("not implemented (S10 P01)")
}
func Judge(contract.Evidence) (contract.Result, error) {
	return contract.Result{}, failure("not implemented (S10 P01)")
}
func JudgeResources(contract.Evidence) (contract.Result, error) {
	return contract.Result{}, failure("not implemented (S10 P03)")
}
func Quantiles([]contract.Attempt, []contract.Slot) (contract.Distribution, error) {
	return contract.Distribution{}, failure("not implemented (S10 P02)")
}

var (
	_ contract.ReaderFactory  = NewSnapshotter
	_ contract.DriverFactory  = NewDriver
	_ contract.FixtureFactory = NewFixture
	_ contract.PlanFunc       = Plan
	_ contract.JudgeFunc      = Judge
	_ contract.JudgeFunc      = JudgeResources
	_ contract.QuantilesFunc  = Quantiles
)
