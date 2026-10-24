// ports.go fixes the acceptance boundaries before their implementations land.
// Contexts bound collection and command attempts, never live business ownership.
// Factories are injected by the implementation owners; the permanent failure
// stubs must remain unreachable from successful fixture and product paths.
// origin: hot-update S10 plan §5.1 / P00
package contract

import (
	"context"
	"errors"
)

type Snapshotter interface {
	Sample(context.Context, Key, string) (Sample, error)
	Drain(context.Context, Key, string) (RawRef, error)
	Close() error
	Join(context.Context) error
}
type Driver interface {
	Stage(context.Context, Step) (View, error)
	Apply(context.Context, Step) (View, error)
	Query(context.Context, Key, string, string) (View, error)
}
type Fixture interface {
	Start(context.Context, Profile) (RawRef, error)
	Sample(context.Context, string) (Evidence, error)
	Drain(context.Context, string) (RawRef, error)
	Stop(context.Context) error
	Join(context.Context) error
}
type PlanFunc func(Profile) ([]Step, error)
type JudgeFunc func(Evidence) (Result, error)
type QuantilesFunc func([]Attempt, []Slot) (Distribution, error)
type ReaderFactory func(Profile) (Snapshotter, error)
type DriverFactory func(Profile) (Driver, error)
type FixtureFactory func(Profile) (Fixture, error)

var ErrContractStub = errors.New("fullstack contract not wired")
