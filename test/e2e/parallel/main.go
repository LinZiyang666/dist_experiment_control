package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type result struct {
	test   string
	worker int
	cpus   string
	dur    time.Duration
	err    error
	output string
}

func main() {
	var (
		workers     = flag.Int("workers", 0, "parallel workers (0 = auto: min(#tests, #physical cores/2, 20))")
		runRe       = flag.String("run", "", "only run tests matching this regexp (default: all top-level tests)")
		timeout     = flag.Duration("timeout", minDeadline, "overall deadline (unset = derived from units/workers; see defaultDeadline)")
		repeat      = flag.Int("repeat", 1, "run the whole set N times (flake hunting)")
		dryRun      = flag.Bool("dry-run", false, "print the plan and exit")
		verbose     = flag.Bool("v", false, "stream failing output in full")
		tagsFlag    = flag.String("tags", "e2e_matrix", "build tags")
		noAvoidBusy = flag.Bool("no-avoid-busy", false, "do not exclude cores already carrying a foreign workload")
		shards      = flag.Int("shards", 0, "split the dominant packages into N name-based shards (0 = off)")
		split       = flag.Bool("split", false, "schedule one unit PER PACKAGE instead of per matrix (lowers the max(matrix) ceiling)")
	)
	flag.Parse()

	topo, err := discoverTopology()
	if err != nil {
		fatal("discover topology: %v", err)
	}
	if !*noAvoidBusy {
		if dropped := topo.excludeBusy(runningHeavyCPUs(os.Getpid())); dropped > 0 {
			fmt.Printf("excluded %d physical core(s) carrying a foreign workload\n", dropped)
		}
	}
	fmt.Printf("topology: %d physical cores across %d NUMA node(s)\n", topo.total, len(topo.nodes))
	for _, n := range topo.nodeIDs() {
		fmt.Printf("  node %d: %d cores\n", n, len(topo.nodes[n]))
	}

	var tests []string
	var units []unit
	if *split {
		root, rerr := repoRoot()
		if rerr != nil {
			fatal("repo root: %v", rerr)
		}
		var unparsed []string
		units, unparsed, err = splitMatrices(root)
		if err != nil {
			fatal("split matrices: %v", err)
		}
		// A matrix whose command shape we could not parse still has to run, or
		// the split mode would silently cover less than the serial gate.
		for _, m := range unparsed {
			units = append(units, unit{matrix: m, name: m, whole: true})
		}
		if *runRe != "" {
			re, cerr := regexp.Compile(*runRe)
			if cerr != nil {
				fatal("bad -run: %v", cerr)
			}
			var kept []unit
			for _, u := range units {
				if re.MatchString(u.matrix) || re.MatchString(u.name) {
					kept = append(kept, u)
				}
			}
			units = kept
		}
		if len(units) == 0 {
			fatal("no units matched %q", *runRe)
		}
		// Coverage self-check. Split mode adds a source-parsing layer between the
		// matrices and what actually runs, and a parser that silently misses a
		// matrix produces a fast, green, INCOMPLETE run — strictly worse than
		// being slow. This has already bitten twice (a non-literal -timeout
		// argument shifted the parse and swallowed a package path; a "Matrix"
		// name filter on the run-whole fallback dropped TestAllPhases and with it
		// all 11 phase suites), so the invariant is asserted rather than assumed:
		// every top-level test the serial gate would run must have a unit here.
		//
		// Only in a FULL run. Under -run the operator has deliberately asked for a
		// subset — that is the supported way to check one matrix without paying for
		// all of them — so demanding full coverage there would make the check the
		// thing that forbids targeted runs. The distinction is what the check is
		// for: it catches coverage lost by ACCIDENT, never coverage declined on
		// purpose. A partial run says so on its own line, so a subset result is
		// never mistaken for a gate.
		if *runRe != "" {
			fmt.Printf("PARTIAL RUN (-run %q): coverage self-check skipped — this is a targeted "+
				"run, not a full gate\n", *runRe)
		} else {
			declared, lerr := listTests("e2e_matrix", "")
			if lerr != nil {
				fatal("coverage self-check: list tests: %v", lerr)
			}
			covered := map[string]bool{}
			for _, u := range units {
				covered[u.matrix] = true
			}
			var uncovered []string
			for _, d := range declared {
				if !covered[d] {
					uncovered = append(uncovered, d)
				}
			}
			if len(uncovered) > 0 {
				fatal("coverage self-check FAILED: %d top-level test(s) in the serial gate have no unit here: %v\n"+
					"Running fewer tests faster is not an optimisation. Fix the parser or add a matrix-level fallback.",
					len(uncovered), uncovered)
			}
			fmt.Printf("coverage self-check: %d/%d top-level tests represented\n", len(declared), len(declared))
		}

		if *shards > 1 {
			before := len(units)
			units, err = shardPackages(units, *shards)
			if err != nil {
				fatal("shard: %v", err)
			}
			fmt.Printf("units: %d -> %d (package split, then %d-way name sharding of the dominant packages)\n",
				before, len(units), *shards)
		} else {
			fmt.Printf("units: %d (split by package; %d matrix-level fallback)\n", len(units), len(unparsed))
		}
	} else {
		tests, err = listTests(*tagsFlag, *runRe)
		if err != nil {
			fatal("list tests: %v", err)
		}
		if len(tests) == 0 {
			fatal("no tests matched %q", *runRe)
		}
		fmt.Printf("tests: %d\n", len(tests))
	}
	workItems := len(tests)
	if *split {
		workItems = len(units)
	}

	w := *workers
	if w == 0 {
		w = autoWorkerCount(topo.total, workItems)
	}

	plan, err := allocate(topo, w)
	if err != nil {
		fatal("allocate: %v", err)
	}
	// Reserve a wide worker for unsplittable units when the run is wide enough to
	// spare the slices (external review B1: weight/admission control). Three
	// merged slices is what takes TestAllPhases from 2 cores to 6 on this box —
	// enough for one phase subprocess to behave like it does in isolation.
	var heavy *assignment
	if *split {
		plan, heavy = reserveHeavyWorker(plan, 3)
	}
	workerCount := len(plan)
	if heavy != nil {
		workerCount++
	}
	fmt.Printf("plan: %d worker(s)\n", workerCount)
	for _, a := range plan {
		fmt.Printf("  %s\n", a)
	}
	if heavy != nil {
		fmt.Printf("  [wide, unsplittable units] %s\n", *heavy)
	}
	// The budget is part of the PLAN, so it is computed and printed before the dry-run return —
	// `-dry-run` exists to answer "what would this host do", and on a small host the answer that
	// matters most is "it would give itself 100 minutes, not 25".
	budget := *timeout
	if timeoutWasSet() {
		fmt.Printf("deadline: %s (explicit -timeout)\n", budget)
	} else {
		budget = defaultDeadline(workItems, workerCount)
		fmt.Printf("deadline: %s (derived: %d unit(s) / %d worker(s) = %d slot(s) deep; "+
			"override with -timeout)\n", budget, workItems, workerCount, slotsPerWorker(workItems, workerCount))
	}
	if *dryRun {
		return
	}

	// ctx-root: the runner's own root; this process IS the round.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	overallStart := time.Now()
	failures := 0
	for round := 1; round <= *repeat; round++ {
		if *repeat > 1 {
			fmt.Printf("\n=== round %d/%d ===\n", round, *repeat)
		}
		results := runRound(ctx, plan, heavy, tests, units, *tagsFlag, *verbose)
		// Reconcile what was SCHEDULED against what reported back. A work item
		// that produces no result did not run, and an absent result is
		// indistinguishable from a passing one in report() — same failure shape
		// as the dropped TestAllPhases: fast, green, incomplete.
		//
		// External review B4: this compared against len(units), which is empty in
		// the default non-split mode, so every successful default run reported
		// "-1 unit(s) produced no result" and exited 1. A guard that fails closed
		// on the correct outcome is not a guard, it is an outage — and it shipped
		// because the Makefile always passes -split, so nothing exercised the
		// advertised default.
		//
		// External review M4: comparing COUNTS cannot see an item that was
		// duplicated while another was dropped, or one silently substituted for
		// another. The identities are compared as a multiset instead.
		// A blown deadline is NOT a test failure, and saying so is the whole point of this block.
		//
		// origin: a 2-physical-core GitHub runner. It seated ONE worker, ran all 99 units serially,
		// hit the 25m default at unit 85, and reported `FAIL D5:test/d5[1/8]: signal: killed` plus
		// "17 scheduled item(s) produced no result". Every word of that is true and all of it points
		// at the tests. Nothing said the host was too small for the budget, so the only way to learn
		// what happened was to notice that the wall clock was 25m0.002s.
		//
		// The deadline exists to stop a runaway round, not to enforce a performance budget. When it
		// fires, the round is still FAILED — an incomplete round must never read as a pass — but it
		// is failed for a reason the operator can act on, and the action is not "debug test/d5".
		if ctx.Err() != nil {
			fmt.Printf("\nDEADLINE EXCEEDED: the round hit its %s budget. THIS IS NOT A TEST FAILURE.\n"+
				"  the unit(s) reported killed below were killed BY this deadline, and any unit listed as\n"+
				"  producing no result never started.\n\n"+
				"  this host seated %d worker(s) from %d physical core(s), so %d unit(s) ran %d-at-a-time;\n"+
				"  the round needed more wall clock than the budget allowed.\n\n"+
				"  fix the BUDGET or the HOST, not the tests:\n"+
				"    -timeout %s        (roughly what this shape needs, with headroom)\n"+
				"    or run on a host with more physical cores — workers = min(units, cores/2, %d)\n",
				budget.Round(time.Second), workerCount, topo.total, workItems, workerCount,
				suggestDeadline(time.Since(overallStart)), maxAutoWorkers)
		}
		if missing, extra := reconcile(scheduledNames(*split, units, tests), results); len(missing) > 0 || len(extra) > 0 {
			if len(missing) > 0 {
				fmt.Printf("FAILURES: %d scheduled item(s) produced no result: %v — "+
					"treat this round as FAILED, not as a pass with fewer tests\n", len(missing), missing)
			}
			if len(extra) > 0 {
				fmt.Printf("FAILURES: %d unexpected/duplicated result(s): %v — "+
					"the set that ran is not the set that was scheduled\n", len(extra), extra)
			}
			failures++
		}
		failures += report(results, *verbose)
	}

	fmt.Printf("\ntotal wall clock: %s\n", time.Since(overallStart).Round(time.Millisecond))
	if failures > 0 {
		fmt.Printf("FAILURES: %d\n", failures)
		// os.Exit skips deferred calls, so the `defer cancel()` above never runs on the failure path.
		cancel()
		//nolint:gocritic // exitAfterDefer: cancel() runs explicitly above; this is a test runner
		// whose exit code IS its result.
		os.Exit(1)
	}
	fmt.Println("ALL PASS")
}

// runRound feeds every test through a channel; each worker takes the next one
// and runs it pinned to its own cores. Work-stealing rather than a static
// test->core map: matrix runtimes differ by 30x (D4 ~280s, ProxyDial ~3s), so a
// fixed assignment would leave most of the machine idle behind one long tail.
func runRound(ctx context.Context, plan []assignment, heavy *assignment, tests []string, units []unit, tags string, verbose bool) []result {
	type job struct {
		test  string
		u     unit
		byPkg bool
	}
	queue := make(chan job)
	// Units that cannot be split get their own queue served by the wide worker.
	// An unsplittable unit is not one big test, it is a whole matrix running its
	// subprocesses serially inside one slice of the machine — starving it while
	// twenty single-package units each hold an equal slice is what turned a 0.9s
	// assertion into a 15s timeout. See reserveHeavyWorker.
	// Buffer all candidates so publishing several whole-matrix fallbacks does
	// not block the producer behind the first long-running heavy unit and leave
	// every light worker idle until the heavy queue is almost drained.
	heavyQueue := make(chan job, len(units))
	go func() {
		defer close(queue)
		defer close(heavyQueue)
		emit := func(ch chan job, j job) bool {
			select {
			case ch <- j:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if len(units) > 0 {
			// Heavy first: it is the critical path, so it must start at t=0 rather
			// than after the light units have drained.
			if heavy != nil {
				for _, u := range units {
					if u.isWhole() && !emit(heavyQueue, job{u: u, byPkg: true}) {
						return
					}
				}
			}
			for _, u := range units {
				if heavy != nil && u.isWhole() {
					continue
				}
				if !emit(queue, job{u: u, byPkg: true}) {
					return
				}
			}
			return
		}
		for _, t := range tests {
			if !emit(queue, job{test: t}) {
				return
			}
		}
	}()

	var mu sync.Mutex
	var results []result
	var wg sync.WaitGroup
	run := func(a assignment, j job) {
		test := j.test
		var r result
		if j.byPkg {
			test = j.u.name
			r = runUnit(ctx, a, j.u)
		} else {
			r = runOne(ctx, a, test, tags)
		}
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
		status := "PASS"
		if r.err != nil {
			status = "FAIL"
		}
		fmt.Printf("  [w%d node%d %dc] %-32s %-4s %s\n",
			a.worker, a.node, len(a.cores), test, status, r.dur.Round(time.Millisecond))
	}
	for _, a := range plan {
		wg.Add(1)
		go func(a assignment) {
			defer wg.Done()
			for j := range queue {
				run(a, j)
			}
		}(a)
	}
	if heavy != nil {
		wg.Add(1)
		go func(a assignment) {
			defer wg.Done()
			// Drain the heavy queue first, then help with the light one. A wide
			// worker sitting idle after its own queue empties would waste the
			// largest slice of the machine.
			for j := range heavyQueue {
				run(a, j)
			}
			for j := range queue {
				run(a, j)
			}
		}(*heavy)
	} else {
		// No wide worker: heavyQueue must still be drained or the producer blocks
		// forever on its first unsplittable unit. It is empty by construction here
		// (the producer routes everything to `queue` when heavy is nil), but
		// relying on that silently is how a deadlock ships.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range heavyQueue { //nolint:revive // documented drain of an empty channel
			}
		}()
	}
	wg.Wait()
	return results
}

// runOne executes one top-level test pinned to the worker's cpu set.
//
// taskset supplies the affinity, numactl the memory binding. Both are applied to
// the `go test` process, and the phase subprocesses it forks inherit the mask —
// which is the point: TestAllPhases forks a child per phase, and an unpinned
// child would wander back across the machine.
// pinnedCommand builds the child process, binding it to the worker's slice when
// numactl is available and running it unpinned when it is not.
//
// numactl is absent on plenty of machines (CI images, macOS, minimal containers).
// Hard-failing there would make this runner unusable exactly where it is now the
// only gate, and silently pinning nothing would be worse — so the degradation is
// explicit and announced once. Without pinning the units still run in parallel;
// what is lost is NUMA locality, which costs latency, not coverage.
var numactlOnce sync.Once
var numactlOK bool

func pinnedCommand(ctx context.Context, a assignment, goArgs []string) *exec.Cmd {
	numactlOnce.Do(func() {
		_, err := exec.LookPath("numactl")
		numactlOK = err == nil
		if !numactlOK {
			fmt.Println("numactl not found: running unpinned (parallelism kept, NUMA locality lost)")
		}
	})
	if !numactlOK {
		return exec.CommandContext(ctx, "go", goArgs...)
	}
	args := append([]string{
		"--cpunodebind=" + itoa(a.node), "--membind=" + itoa(a.node),
		"--physcpubind=" + a.cpuList, "--", "go",
	}, goArgs...)
	return exec.CommandContext(ctx, "numactl", args...)
}

func runOne(ctx context.Context, a assignment, test, tags string) result {
	cmd := pinnedCommand(ctx, a, []string{
		"test", "-count=1", "-tags", tags, "-timeout", "20m",
		"-run", "^" + test + "$", "-v", "./test/e2e",
	})
	cmd.Env = append(os.Environ(), "GOMAXPROCS="+itoa(a.procs))
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	start := time.Now()
	err := cmd.Run()
	return result{test: test, worker: a.worker, cpus: a.cpuList, dur: time.Since(start), err: err, output: buf.String()}
}

// runUnit executes one package (or one whole matrix, for unparsed shapes)
// pinned to the worker's cpu set.
func runUnit(ctx context.Context, a assignment, u unit) result {
	var goArgs []string
	if u.isWhole() {
		goArgs = []string{"test", "-count=1", "-tags", "e2e_matrix", "-timeout", "20m",
			"-run", "^" + u.matrix + "$", "./test/e2e/..."}
	} else {
		goArgs = u.goTestArgs()
	}
	cmd := pinnedCommand(ctx, a, goArgs)
	cmd.Env = append(os.Environ(), "GOMAXPROCS="+itoa(a.procs))
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	start := time.Now()
	err := cmd.Run()
	return result{test: u.name, worker: a.worker, cpus: a.cpuList, dur: time.Since(start), err: err, output: buf.String()}
}

// listTests asks the compiled test binary what it contains. Hardcoding the
// matrix names would rot the first time someone adds one — and the failure mode
// is silent under-coverage, which is worse than a build error.
func listTests(tags, filter string) ([]string, error) {
	// ./test/e2e — NOT ./test/e2e/... — because the serial gate is exactly the
	// tests in that one package. The recursive form also listed this runner's own
	// package, and since `go test -list` prints bare test names with no package
	// attribution, every _test.go added here showed up as an "uncovered top-level
	// test of the serial gate" and the coverage self-check refused to start
	// (external review B5).
	//
	// That is why the plan could claim this runner "has no unit tests" as a mere
	// limitation: the check made having them impossible. A parser that decides
	// what the release gate covers is the last thing that should be untestable.
	cmd := exec.Command("go", "test", "-tags", tags, "-list", ".*", "./test/e2e")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err, out)
	}
	var re *regexp.Regexp
	if filter != "" {
		re, err = regexp.Compile(filter)
		if err != nil {
			return nil, err
		}
	}
	var tests []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Test") {
			continue
		}
		if re != nil && !re.MatchString(line) {
			continue
		}
		tests = append(tests, line)
	}
	sort.Strings(tests)
	return tests, nil
}

// scheduledNames returns the identity of every work item this round was asked to
// run. The two modes carry work in different places — split mode in units, the
// default in tests — and reading the wrong one is exactly how the reconciliation
// below came to fail every non-split run (external review B4).
func scheduledNames(split bool, units []unit, tests []string) []string {
	if split {
		out := make([]string, 0, len(units))
		for _, u := range units {
			out = append(out, u.name)
		}
		return out
	}
	return append([]string(nil), tests...)
}

// reconcile compares scheduled identities against reported ones as a MULTISET,
// so a duplicate that masks a drop still shows up (external review M4: equal
// counts do not imply equal sets). Returns what never reported, and what
// reported without being scheduled or reported more than once.
func reconcile(scheduled []string, results []result) (missing, extra []string) {
	want := map[string]int{}
	for _, s := range scheduled {
		want[s]++
	}
	for _, r := range results {
		want[r.test]--
	}
	for name, n := range want {
		for ; n > 0; n-- {
			missing = append(missing, name)
		}
		for ; n < 0; n++ {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func report(results []result, verbose bool) int {
	sort.Slice(results, func(i, j int) bool { return results[i].dur > results[j].dur })
	fmt.Println("\nslowest:")
	for i, r := range results {
		if i >= 5 {
			break
		}
		fmt.Printf("  %-32s %s\n", r.test, r.dur.Round(time.Millisecond))
	}
	failures := 0
	for _, r := range results {
		if r.err == nil {
			continue
		}
		failures++
		fmt.Printf("\nFAIL %s (worker %d, cpus %s): %v\n", r.test, r.worker, r.cpus, r.err)
		out := r.output
		if !verbose && len(out) > 4000 {
			out = out[len(out)-4000:]
		}
		fmt.Println(out)
	}
	return failures
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }

// maxAutoWorkers caps the automatic worker count. Named because the deadline
// derivation and the operator-facing advice both have to quote it, and a third
// copy of `20` is how those two drift apart.
const maxAutoWorkers = 20

// autoWorkerCount keeps the default usable on both the 44-core development
// host and small CI runners. Hard-coding the development value in the Makefile
// made a 2- or 4-core runner fail allocation before it ran a single test.
func autoWorkerCount(physicalCores, workItems int) int {
	w := physicalCores / 2
	if w > maxAutoWorkers {
		w = maxAutoWorkers
	}
	if w > workItems {
		w = workItems
	}
	if w < 1 {
		w = 1
	}
	return w
}

// timeoutWasSet reports whether -timeout appeared on the command line. Without this the runner
// cannot tell "the operator chose 25m" from "25m is the flag's default", and it must not silently
// override an explicit choice.
func timeoutWasSet() bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "timeout" {
			set = true
		}
	})
	return set
}

// slotsPerWorker is how many units one worker runs back-to-back. THIS, not the unit count, is what
// a round's wall clock tracks: 99 units across 20 workers is a 5-deep queue, and across 1 worker it
// is a 99-deep one.
func slotsPerWorker(workItems, workers int) int {
	if workers < 1 {
		workers = 1
	}
	n := workItems / workers
	if workItems%workers != 0 {
		n++
	}
	if n < 1 {
		n = 1
	}
	return n
}

// DEADLINE DERIVATION
//
// origin: a 2-physical-core GitHub runner blew a hard-coded 25m and reported it as a test failure.
// 25m was never a property of the SUITE — it was a property of the 44-core box the runner was
// written on, where 20 workers make the queue 5 deep. On a host that seats one worker the same
// suite is a 99-deep queue and no fixed number can be right for both.
//
// So it is derived from the queue depth, anchored on two MEASURED points rather than a guess:
//
//	44-core dev host: 99 units / 20 workers =  5 slots -> 3m27s observed  (~41s per slot)
//	2 physical cores: 99 units /  1 worker  = 99 slots -> 42m22s observed (~26s per slot), ALL PASS
//
// The 2-core figure was measured by restricting this repo's dev box to two physical cores with
// `taskset -c 0,1,44,45`, the closest reproduction of a GitHub `ubuntu-latest` runner available
// here. It matters that the round PASSED: nothing in the suite is fragile at one worker, so the CI
// failure really was the budget and nothing else.
//
// Per-slot cost is NOT constant across those points — a worker with 2 cores to itself finishes a
// slot faster than one of 20 workers fighting for memory bandwidth — so the constant is rounded up
// from the LARGER of the two (41s -> 30s is below both; see the factor). A deadline that is
// generous on a big host costs nothing: the deadline is a runaway-stopper, and the per-unit
// `-timeout 20m` inside each `go test` is what actually catches a hung test.
//
// 30s x 2 puts the 1-worker budget at 1h39m against a measured 42m — 2.3x headroom, which is what
// a CI runner slower than this box needs.
const (
	perSlotBudget  = 30 * time.Second // between the two measured per-slot costs (26s small, 41s loaded-big)
	deadlineFactor = 2                // headroom: a loaded or slower runner must not trip it
	minDeadline    = 25 * time.Minute // never go BELOW the historical default on a big host
)

func defaultDeadline(workItems, workers int) time.Duration {
	d := time.Duration(slotsPerWorker(workItems, workers)) * perSlotBudget * deadlineFactor
	if d < minDeadline {
		d = minDeadline
	}
	return d.Round(time.Minute)
}

// suggestDeadline turns "how far it got before the budget ran out" into a number the operator can
// paste back. Deliberately crude: it is a starting point, not a promise, and it says so by being a
// round number well above what was observed.
func suggestDeadline(elapsed time.Duration) time.Duration {
	d := (elapsed * 2).Round(10 * time.Minute)
	if d < 30*time.Minute {
		d = 30 * time.Minute
	}
	return d
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "e2epar: "+format+"\n", a...)
	os.Exit(2)
}
