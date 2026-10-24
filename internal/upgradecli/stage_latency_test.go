package upgradecli

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// The two images have independent readers, immutable names and probes. A
// rendezvous inside each probe detects serialization without making machine
// throughput part of the assertion. Refusal and cancellation must still join
// both checks before publication can return or release its reservation.
// Real: registered domain, live supervised issuer, two durable image slots.
// Simulated: bounded decoder probes; not covered: same-PID exec or transfer.
// origin: S8 internal review PREPARE-LATENCY R-10
func TestStagePublicationOverlapsImagesAndJoinsChecks(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStagePublicationOverlap(t)
	}
	testharness.AssertNoGoroutineLeak(t, "concurrent stage checks", g)
	testharness.AssertNoFDLeak(t, "concurrent stage checks", fd, 0)
}

func exerciseStagePublicationOverlap(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"success", "copy_overlap", "source_refused", "target_refused", "cancelled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f := newStageFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.RequestLatency)
			defer cancel()
			entered := make(chan string, 2)
			finished := make(chan string, 2)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			slow := make(chan struct{})
			var slowOnce sync.Once
			unblockSlow := func() { slowOnce.Do(func() { close(slow) }) }
			defer unblockSlow()
			// Serialize only the fixture's counter. Waiting occurs outside the
			// mutex, so the two production checks must actually overlap.
			var probeMu sync.Mutex
			f.port.Probe = func(path string) (string, agentimage.Capabilities, error) {
				sha, err := upgradeartifact.FileSHA(path)
				if err != nil {
					return "", agentimage.Capabilities{}, err
				}
				entered <- sha
				defer func() { finished <- sha }()
				select {
				case <-release:
				case <-ctx.Done():
					return "", agentimage.Capabilities{}, ctx.Err()
				}
				// A failing target must not let publication return while its
				// source probe still owns a running child or the domain lease.
				if mode == "target_refused" && sha == f.grant.SourceSHA {
					select {
					case <-slow:
					case <-ctx.Done():
						return "", agentimage.Capabilities{}, ctx.Err()
					}
				}
				if mode == "source_refused" && sha == f.grant.SourceSHA || mode == "target_refused" && sha == f.grant.TargetSHA {
					return "", agentimage.Capabilities{}, errors.New("decoder refuses this image")
				}
				probeMu.Lock()
				defer probeMu.Unlock()
				return f.probe(path)
			}
			type result struct {
				receipt upgradeartifact.Receipt
				err     error
			}
			done := make(chan result, 1)
			reading := make(chan struct{}, 2)
			readRelease := make(chan struct{})
			var readOnce sync.Once
			unblockReaders := func() { readOnce.Do(func() { close(readRelease) }) }
			defer unblockReaders()
			go func() {
				publisher := upgradeartifact.WithPublicationPort(ctx, f.port)
				if mode == "copy_overlap" {
					source, err := os.Open(f.opts.Source)
					if err != nil {
						done <- result{err: err}
						return
					}
					defer func() { _ = source.Close() }()
					target, err := os.Open(f.opts.Candidate)
					if err != nil {
						done <- result{err: err}
						return
					}
					defer func() { _ = target.Close() }()
					receipt, err := upgradeartifact.Publish(publisher, f.grant,
						&stageReadBarrier{reader: source, entered: reading, release: readRelease, ctx: ctx},
						&stageReadBarrier{reader: target, entered: reading, release: readRelease, ctx: ctx})
					done <- result{receipt, err}
					return
				}
				receipt, err := Stage(publisher, f.opts, upgradeartifact.Publish)
				done <- result{receipt, err}
			}()
			if mode == "copy_overlap" {
				for range 2 {
					select {
					case <-reading:
					case <-ctx.Done():
						unblockReaders()
						unblock()
						<-done
						t.Fatal("stage serialized independent image copies")
					}
				}
				unblockReaders()
			}
			seen := make(map[string]bool)
			for len(seen) < 2 {
				select {
				case sha := <-entered:
					seen[sha] = true
				case <-ctx.Done():
					unblock()
					<-done
					t.Fatal("stage serialized independent image checks")
				}
			}
			if !seen[f.grant.SourceSHA] || !seen[f.grant.TargetSHA] {
				t.Fatal("stage checked another SHA pair")
			}
			if mode == "cancelled" {
				cancel()
			}
			if mode == "expired" {
				f.port.Now = func() time.Time { return time.Unix(0, f.grant.ExpiresAt+1) }
			}
			unblock()
			if mode == "target_refused" {
				select {
				case <-done:
					t.Fatal("publication returned while the source decoder still owned its check")
				case <-time.After(30 * time.Millisecond):
					// The source remains live throughout this observation window.
					// Release it before joining the valid refusal result.
				}
				// The deferred release also runs if an early-return assertion
				// fails; a probe that timed out cannot strand this test's sender.
				unblockSlow()
			}
			got := <-done
			if len(finished) != 2 {
				t.Fatal("publication returned before both decoder checks joined")
			}
			if mode == "success" || mode == "copy_overlap" {
				if got.err != nil {
					t.Fatal(got.err)
				}
				if err := upgradeartifact.Verify(got.receipt, f.grant); err != nil {
					t.Fatal(err)
				}
			} else {
				if got.err == nil || got.receipt != (upgradeartifact.Receipt{}) {
					t.Fatalf("%s published without both valid checks and fresh authority: %v", mode, got.err)
				}
				if _, err := os.Stat(f.opts.ReceiptOut); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refused publication left an artifact receipt")
				}
			}
		})
	}
}

type stageReadBarrier struct {
	reader  io.Reader
	entered chan<- struct{}
	release <-chan struct{}
	ctx     context.Context
	started bool
}

func (r *stageReadBarrier) Read(data []byte) (int, error) {
	if !r.started {
		r.started = true
		r.entered <- struct{}{}
		select {
		case <-r.release:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	return r.reader.Read(data)
}
