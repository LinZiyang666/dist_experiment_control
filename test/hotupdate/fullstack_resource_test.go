//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/testharness"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// Each round owns one real handoff, not another complete seven-row rollout.
// Six workspace builds are shared; mutable homes, observers and stores are not.
// The counters subtests can run while S8 transaction wiring is still pending.
// origin: hot-update S10 plan §4.1 / §10.2 P25 / §11.1–§11.2
func TestFiveGenerationResourceBaseline(t *testing.T) {
	t.Run("contracts", fullstackResourceControls)
	parent := t
	var images []hotUpdateArtifact
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			if images == nil {
				images = buildFullstackResourceImages(parent)
			}
			exerciseGenerationResourceCycle(t, images[round], images[round+1])
		})
	}
	testharness.AssertNoGoroutineLeak(t, "generation fixture and custody collectors x5", g)
	testharness.AssertNoFDLeak(t, "generation fixture and custody collectors x5", fd, 0)
}

func buildFullstackResourceImages(t *testing.T) []hotUpdateArtifact {
	t.Helper()
	begin := time.Now()
	var images []hotUpdateArtifact
	seen := map[string]bool{}
	for generation := 0; generation < 6; generation++ {
		a := buildHotUpdateArtifact(t, fmt.Sprintf("s10-resource-T%d", generation))
		if seen[a.SHA256] || len(images) > 0 && (a.SourceDigest != images[0].SourceDigest || a.SourceCommit != images[0].SourceCommit) {
			t.Fatal("six mechanism artifacts require distinct SHA and one source candidate")
		}
		seen[a.SHA256] = true
		images = append(images, a)
		t.Logf("artifact=T%d sha=%s source=%s tree=%s flags=%q", generation, a.SHA256, a.SourceCommit, a.SourceDigest, a.BuildArgs)
	}
	t.Logf("startup six-image-build=%s GOFLAGS=%q", time.Since(begin), os.Getenv("GOFLAGS"))
	return images
}

func exerciseGenerationResourceCycle(t *testing.T, original, target hotUpdateArtifact) {
	t.Helper()
	begin := time.Now()
	a := transferArtifacts{a: original.Path, b: target.Path, shaA: original.SHA256, shaB: target.SHA256,
		archiveA: original.Tarball, archiveB: target.Tarball, source: original.SourceCommit, dirty: original.SourceDigest}
	s := newFullstackTaskStack(t, a)
	defer func() {
		start := time.Now()
		s.close()
		t.Logf("supervisor-join=%s", time.Since(start))
	}()
	t.Logf("startup=%s", time.Since(begin))
	t.Run("counters", func(t *testing.T) { fullstackLiveResourceCycles(t, s) })
	t.Run("cut", func(t *testing.T) {
		// Re-measure immediately before each old image leaves; a separately
		// selected counters subtest is useful evidence but cannot replace this.
		fullstackLiveResourceCycles(t, s)
		start := time.Now()
		fullstackTaskCut(t, s, "agent", false)
		fullstackLiveResourceCycles(t, s)
		fullstackTaskCut(t, s, "broker", false)
		fullstackLiveResourceCycles(t, s)
		t.Logf("cut=%s", time.Since(start))
		// Exact retention/consumer/temp ownership is a separate axis. Unknown
		// producer fields must not be turned into zeros by the kernel sampler.
		sample := fullstackRuntimeSample(t, s)
		if sample.Unknown != 0 {
			t.Fatal("SETUP-RED: NOT_RUN pending S10 P07 live resource getters and P16 classified owner census")
		}
	})
}

type fullstackLiveFloor struct {
	PID         int
	Ticks       uint64
	SHA         string
	Generation  uint64
	G, FD       int64
	Descriptors []string
}

// Both counters come from the observed process. /proc is used for classified
// descriptor identities, never as a Threads-for-goroutines substitute.
func fullstackLiveFloors(t *testing.T, s *fullstackTaskStack) map[string]fullstackLiveFloor {
	t.Helper()
	a := fullstackRuntimeSample(t, s)
	b, err := s.broker.SampleIdentity()
	transferMust(t, err)
	r, err := (&adminsock.Client{Path: s.admin, Timeout: time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime})
	transferMust(t, err)
	if !r.OK || r.Runtime == nil || r.Runtime.Goroutines <= 0 || r.Runtime.OpenFDs < 0 {
		t.Fatal("SETUP-RED: broker did not report its live goroutine and descriptor counts")
	}
	after, err := s.broker.SampleIdentity()
	transferMust(t, err)
	if b.PID != after.PID || b.ProcessStart != after.ProcessStart || b.RunningSHA != after.RunningSHA {
		t.Fatal("broker image changed during resource observation")
	}
	return map[string]fullstackLiveFloor{
		"agent": {int(a.PID), a.StartTicks, fmt.Sprintf("%x", a.ExeSHA), a.Binding.Generation,
			int64(a.Goroutines), int64(a.FDs), fullstackDescriptorCensus(t, int(a.PID))},
		"broker": {b.PID, b.ProcessStart, b.RunningSHA, 0, int64(r.Runtime.Goroutines), int64(r.Runtime.OpenFDs), fullstackDescriptorCensus(t, b.PID)},
	}
}

func fullstackDescriptorCensus(t *testing.T, pid int) []string {
	t.Helper()
	root := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(root)
	transferMust(t, err)
	var rows []string
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		transferMust(t, err)
		link, err := os.Readlink(filepath.Join(root, entry.Name()))
		if os.IsNotExist(err) {
			continue // An in-flight admin request owns a short-lived socket.
		}
		transferMust(t, err)
		rows = append(rows, fmt.Sprintf("%d:%s", fd, link))
	}
	slices.Sort(rows)
	return rows
}

// origin: hot-update S10 plan §4.1 / §8 M08E/M08P/M08A/M08B/M08D
func fullstackFloorWithinImage(before, after fullstackLiveFloor) error {
	if before.PID <= 0 || before.Ticks == 0 || before.SHA == "" || before.G <= 0 || before.FD < 0 ||
		before.PID != after.PID || before.Ticks != after.Ticks || before.SHA != after.SHA || before.Generation != after.Generation {
		return fmt.Errorf("same-image resource floor crossed a process or image boundary")
	}
	if after.G > before.G+int64(testharness.GoroutineLeakTolerance) || after.FD != before.FD ||
		!slices.Equal(before.Descriptors, after.Descriptors) {
		return fmt.Errorf("same-image resource floor leaked: G=%d/%d FD=%d/%d classified=%t",
			before.G, after.G, before.FD, after.FD, slices.Equal(before.Descriptors, after.Descriptors))
	}
	return nil
}

func fullstackLiveResourceCycles(t *testing.T, s *fullstackTaskStack) {
	t.Helper()
	start := time.Now()
	// Warm the actual exec and PTY paths before sealing their fixed costs.
	fullstackResourceBusiness(t, s)
	before := fullstackLiveFloors(t, s)
	for cycle := 0; cycle < 5; cycle++ {
		ioStart := time.Now()
		fullstackResourceBusiness(t, s)
		ioTime := time.Since(ioStart)
		settle := time.Now()
		var problem error
		if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
			after := fullstackLiveFloors(t, s)
			for _, role := range []string{"agent", "broker"} {
				if problem = fullstackFloorWithinImage(before[role], after[role]); problem != nil {
					return false
				}
			}
			return true
		}) {
			// Overlay hits are reported without printing private product logs.
			// Their presence is evidence of injection, never a success criterion.
			t.Logf("mutation-hit agent=%t broker=%t", strings.Contains(s.output(s.agent), "S10_RESOURCE_MUTATION_HIT"),
				strings.Contains(s.output(s.broker), "S10_RESOURCE_MUTATION_HIT"))
			t.Fatalf("cycle %d failed before next exec or fixture cleanup: %v", cycle, problem)
		}
		t.Logf("cycle=%d I/O=%s settle=%s agent_pid=%d broker_pid=%d", cycle, ioTime, time.Since(settle), before["agent"].PID, before["broker"].PID)
	}
	t.Logf("five-cycle=%s agent_sha=%s broker_sha=%s", time.Since(start), before["agent"].SHA, before["broker"].SHA)
}

func fullstackResourceBusiness(t *testing.T, s *fullstackTaskStack) {
	t.Helper()
	for _, kind := range []string{"", "pty"} {
		w := newFullstackTaskPeer(t, s, kind)
		fullstackCreate(t, w, []string{"/usr/bin/python3", "-I", "-c", "import os; os.write(1,b'fullstack-resource-cycle'); raise SystemExit(23)"})
		fullstackFinish(t, w, []byte("fullstack-resource-cycle"), nil, 23)
		w.close()
	}
}

// A pre-cut floor still contains the original long workload. P03's final-drain
// predicate cannot run here: it correctly rejects those still-live resources.
// Count create/IO/retire cycles against this image's unchanged background;
// the complete P03 retention/classification judge runs after natural drain.
// origin: hot-update S10 plan §4.1 / §8 M08
func fullstackImageCycles(e c.Evidence, key c.Key) error {
	last, ok := fullstackLatestSample(e, key)
	if !ok {
		return fmt.Errorf("missing still-live resource census")
	}
	var samples []c.Sample
	for _, row := range e.Samples {
		if row.Stamp.Process == last.Stamp.Process &&
			row.ExeSHA == last.ExeSHA && row.Stamp.CutID == last.Stamp.CutID && row.Stamp.Generation == last.Stamp.Generation {
			samples = append(samples, row)
		}
	}
	base := samples[0]
	rounds, start := 0, base.Stamp.AtBootNS
	known, pending, nonces := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, resource := range base.Resources {
		known[resource.ID] = true
	}
	for index, row := range samples {
		if !row.Complete || row.ExeSHA == (c.Digest{}) || row.Stamp.QueryNonce == "" || nonces[row.Stamp.QueryNonce] ||
			index > 0 && (row.Stamp.Sequence <= samples[index-1].Stamp.Sequence || row.Stamp.AtBootNS <= samples[index-1].Stamp.AtBootNS) {
			return fmt.Errorf("incomplete or replayed same-image resource sample")
		}
		nonces[row.Stamp.QueryNonce] = true
		for _, resource := range row.Resources {
			if resource.Owner.Process != row.Stamp.Process || resource.Owner.Generation != row.Stamp.Generation {
				return fmt.Errorf("foreign resource owner")
			}
			if !known[resource.ID] {
				pending[resource.ID] = true
			}
		}
		floor := reflect.DeepEqual(fullstackActiveResources(base), fullstackActiveResources(row)) &&
			slices.Equal(base.Consumers, row.Consumers) && slices.Equal(base.Temps, row.Temps) && slices.Equal(base.Locks, row.Locks) && slices.Equal(base.OldImages, row.OldImages)
		if !floor {
			continue
		}
		if row.Goroutines > base.Goroutines+int64(testharness.GoroutineLeakTolerance) || row.FDs != base.FDs {
			return fmt.Errorf("same-image cycle has a growing resource floor")
		}
		if len(pending) == 0 {
			continue
		}
		for id := range pending {
			var effect, retired int64
			for _, event := range e.Intervals {
				if event.ResourceID != id || event.Stamp.Process != row.Stamp.Process || event.Stamp.Generation != row.Stamp.Generation ||
					event.Stamp.CutID != row.Stamp.CutID || event.AtNS <= start || event.AtNS > row.Stamp.AtBootNS || event.Raw.Bytes <= 0 {
					continue
				}
				switch event.Event {
				case "read", "write", "wait", "accept", "primitive":
					effect = event.AtNS
				case "retire":
					retired = event.AtNS
				}
			}
			if effect == 0 || retired < effect {
				return fmt.Errorf("cycle lacks nonempty IO followed by owner retirement")
			}
			known[id] = true
		}
		rounds++
		start, pending = row.Stamp.AtBootNS, map[string]bool{}
	}
	if len(pending) != 0 || key.Role != "nats" && key.Role != "caddy" && rounds < 5 {
		return fmt.Errorf("image cannot leave before five settled resource cycles")
	}
	return nil
}

func fullstackActiveResources(sample c.Sample) []c.Resource {
	var rows []c.Resource
	for _, resource := range sample.Resources {
		if resource.FD < 0 && resource.Bytes == 0 && resource.ReleaseAfterNS > sample.Stamp.AtBootNS &&
			(resource.State == "settled-unexpired" || resource.State == "settled") {
			continue
		}
		// Query identity advances, resource ownership does not. Keep every
		// physical reference, multiplicity and owner generation in the floor.
		resource.Owner.QueryNonce, resource.Owner.Sequence, resource.Owner.AtBootNS, resource.Owner.Revision = "", 0, 0, 0
		rows = append(rows, resource)
	}
	slices.SortFunc(rows, func(a, b c.Resource) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	return rows
}

func fullstackResourceControls(t *testing.T) {
	base := fullstackLiveFloor{PID: 10, Ticks: 20, SHA: "original", Generation: 1, G: 20, FD: 2,
		Descriptors: []string{"3:socket:[10]", "4:pipe:[11]"}}
	transferMust(t, fullstackFloorWithinImage(base, base))
	for _, name := range []string{"M08E", "M08P", "M08A", "M08B", "M08D", "M08X", "M08C", "M08O", "exec-reset", "close-original-leave-dup"} {
		t.Run(name, func(t *testing.T) {
			after := base
			switch name {
			case "M08E", "M08P", "M08B", "M08O":
				for cycle := 0; cycle < 5; cycle++ {
					after.G++
				}
			case "M08A", "M08D", "M08X", "M08C":
				for cycle := 0; cycle < 5; cycle++ {
					after.FD++
				}
			case "exec-reset":
				after.SHA, after.G = "replacement", 1
			case "close-original-leave-dup":
				after.Descriptors = []string{"3:socket:[10]", "5:pipe:[11]"}
			}
			if err := fullstackFloorWithinImage(base, after); err == nil {
				t.Fatal("resource defect accepted before image retirement")
			}
		})
	}
	for _, role := range []string{"agent", "broker", "nats", "caddy"} {
		t.Run("M07-"+role, func(t *testing.T) {
			p, step, before, after := fullstackControlCut(role)
			transferMust(t, fullstackCutIdentity(step, p, before, after))
			after.Samples[0].ExeSHA = before.Samples[0].ExeSHA
			if err := fullstackCutIdentity(step, p, before, after); err == nil {
				t.Fatal("disk-only replacement accepted as executing image")
			}
		})
	}
	t.Run("image-cycle-missing", func(t *testing.T) {
		if err := fullstackImageCycles(c.Evidence{}, c.Key{Role: "agent"}); err == nil {
			t.Fatal("missing child census accepted")
		}
	})
	t.Run("image-cycles", func(t *testing.T) {
		e, key := fullstackControlResourceCycles()
		transferMust(t, fullstackImageCycles(e, key))
		for _, defect := range []string{"missing-cycle", "missing-effect", "missing-retirement", "old-image-floor", "M08-growing-floor", "unsettled"} {
			t.Run(defect, func(t *testing.T) {
				e, key := fullstackControlResourceCycles()
				switch defect {
				case "missing-cycle":
					e.Samples = e.Samples[:9]
				case "missing-effect":
					e.Intervals = e.Intervals[1:]
				case "missing-retirement":
					e.Intervals = e.Intervals[:len(e.Intervals)-1]
				case "old-image-floor":
					e.Samples[len(e.Samples)-1].Stamp.Generation++
				case "M08-growing-floor":
					e.Samples[len(e.Samples)-1].Goroutines += 5
				case "unsettled":
					e.Samples = e.Samples[:len(e.Samples)-1]
				}
				if fullstackImageCycles(e, key) == nil {
					t.Fatal("invalid pre-cut resource cycles accepted")
				}
			})
		}
	})
	if !reflect.DeepEqual(base.Descriptors, []string{"3:socket:[10]", "4:pipe:[11]"}) {
		t.Fatal("resource controls mutated the baseline")
	}
}

func fullstackControlResourceCycles() (c.Evidence, c.Key) {
	key := c.Key{Role: "agent", Host: "local", Unit: "agent", Domain: "agent"}
	stamp := c.Stamp{Process: c.Process{Key: key, PID: 10, StartTicks: 20}, Generation: 1, CutID: "cut", QueryNonce: "0", Sequence: 1, AtBootNS: 1}
	base := c.Sample{Stamp: stamp, ExeSHA: fullstackControlRaw().SHA, Goroutines: 10, FDs: 0, Complete: true}
	e := c.Evidence{Samples: []c.Sample{base}}
	for cycle := 0; cycle < 5; cycle++ {
		live, idle := base, base
		live.Stamp.AtBootNS, live.Stamp.Sequence, live.Stamp.QueryNonce = int64(10*cycle+2), uint64(2*cycle+2), fmt.Sprintf("live-%d", cycle)
		id := fmt.Sprintf("cycle-%d", cycle)
		live.FDs = 1
		live.Resources = []c.Resource{{ID: id, Kind: "pipe", Domain: "exec", State: "live", FD: 3, Owner: live.Stamp}}
		idle.Stamp.AtBootNS, idle.Stamp.Sequence, idle.Stamp.QueryNonce = int64(10*cycle+9), uint64(2*cycle+3), fmt.Sprintf("idle-%d", cycle)
		e.Samples = append(e.Samples, live, idle)
		e.Intervals = append(e.Intervals, c.Interval{Stamp: live.Stamp, ResourceID: id, Event: "write", AtNS: int64(10*cycle + 4), Raw: fullstackControlRaw()},
			c.Interval{Stamp: idle.Stamp, ResourceID: id, Event: "retire", AtNS: int64(10*cycle + 8), Raw: fullstackControlRaw()})
	}
	return e, key
}
