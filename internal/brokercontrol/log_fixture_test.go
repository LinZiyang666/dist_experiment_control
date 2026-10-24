package brokercontrol

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"
)

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeClock is a deterministic, monotonically advanced clock. There is no real
// time anywhere in this file: the equivalence proof simulates ~30 minutes of
// broker uptime in microseconds, which is the only reason it can afford to
// compare tick-for-tick over thousands of ticks.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t0 time.Time) *fakeClock { return &fakeClock{now: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

type levelRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *levelRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}
func (r *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *levelRecorder) WithGroup(string) slog.Handler      { return r }
func (r *levelRecorder) levels() []slog.Level {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]slog.Level, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec.Level)
	}
	return out
}

var passEpoch = time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
