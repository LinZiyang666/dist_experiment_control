// Command e2epar runs the e2e matrices in parallel, each pinned to its own
// exclusive set of physical cores on a single NUMA node.
//
// # WHY THIS EXISTS
//
// The full matrix used to run serially, and the note in all_phases_test.go said parallelism was
// "tried + measured and REVERTED" because the heavy clustered-JetStream matrices
// starve their meta-group formation ("routed JS server not ready") when a
// concurrent matrix shares the machine — flaking at -parallel 2.
//
// That measurement is real, but the attribution is worth re-testing. On this
// host an entire serial e2e run leaves the CPU 97.5% idle, the disks at 0%
// util, and 196 GB of RAM free. Nothing is starved of a resource; the run is
// dominated by wall-clock waits (raft elections, JetStream readiness polls,
// heartbeat intervals). What -parallel actually changes is SCHEDULING: with
// GOMAXPROCS capped but no affinity, two test processes still interleave across
// both sockets, and this box's cross-node latency is 2.1x local (10 vs 21).
// Lock-heavy raft/JS handshakes with hard deadlines are exactly what that hurts.
//
// GOMAXPROCS bounds how many threads run; it does not bound WHERE. This runner
// supplies the missing half.
//
// The repo has a precedent for this kind of misattribution: simcluster's
// "drills must run serially" was believed for a long time and turned out to be
// fs.inotify.max_user_instances exhaustion — unrelated to CPU, and fixable.
//
// # WHAT IT DOES NOT DO
//
// Nothing here changes test code. It schedules the same matrices the serial binary
// used to, and asserts it is scheduling all of them.
//
// This runner IS the gate now. The full serial target has been deleted outright rather
// than deprecated: a target that exists gets run, and that one spent eighteen minutes
// finding none of the four defect classes a loaded parallel run exposed. `make e2e-one
// T=<matrix>` isolates a single matrix when this runner flags one.
//
// (The placeholder is spelled <matrix> rather than with a Test-prefixed word, because
// TestPromisedGuardTestsExist reads any such word in a comment as a promise that the
// named test exists — and a placeholder is not a promise. The first version of this
// very sentence tripped it, then so did the version explaining why. Worth the awkward
// wording: the alternative is teaching the gate to guess, and refusing to guess is how
// it caught four real broken promises.)
package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// physCore is one physical core: its SMT siblings, and the NUMA node it lives on.
type physCore struct {
	id       int   // lowest sibling id, used as the stable name
	siblings []int // every logical CPU on this core
	node     int
}

// topology is what the runner discovered about this machine. Nothing here is
// hardcoded: a different box, a different container cpuset, or a kernel that
// reports no NUMA all produce a valid (smaller) topology.
type topology struct {
	nodes map[int][]physCore // node id -> its physical cores
	total int
}

func readCPUList(path string) ([]int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseCPUList(strings.TrimSpace(string(b)))
}

// parseCPUList handles the kernel's "0-21,44-65" range syntax.
func parseCPUList(s string) ([]int, error) {
	var out []int
	if s == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi := part, part
		if i := strings.IndexByte(part, '-'); i >= 0 {
			lo, hi = part[:i], part[i+1:]
		}
		a, err := strconv.Atoi(lo)
		if err != nil {
			return nil, fmt.Errorf("bad cpu list %q: %w", s, err)
		}
		b, err := strconv.Atoi(hi)
		if err != nil {
			return nil, fmt.Errorf("bad cpu list %q: %w", s, err)
		}
		for c := a; c <= b; c++ {
			out = append(out, c)
		}
	}
	return out, nil
}

// discoverTopology reads sysfs. It deliberately reads the CPUs this process is
// ALLOWED to use (sched_getaffinity via /proc/self/status Cpus_allowed_list)
// rather than every CPU the kernel knows about — inside a container or under an
// existing taskset, the difference is the whole point.
func discoverTopology() (*topology, error) {
	allowed, err := allowedCPUs()
	if err != nil {
		return nil, err
	}
	allowedSet := map[int]bool{}
	for _, c := range allowed {
		allowedSet[c] = true
	}

	seen := map[int]bool{} // logical cpu -> already folded into a physCore
	t := &topology{nodes: map[int][]physCore{}}

	for _, cpu := range allowed {
		if seen[cpu] {
			continue
		}
		sibs, err := readCPUList(fmt.Sprintf("/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list", cpu))
		if err != nil || len(sibs) == 0 {
			sibs = []int{cpu} // no SMT info: treat as a single-thread core
		}
		var usable []int
		for _, s := range sibs {
			if allowedSet[s] {
				usable = append(usable, s)
			}
			seen[s] = true
		}
		if len(usable) == 0 {
			continue
		}
		sort.Ints(usable)
		node := nodeOf(cpu)
		t.nodes[node] = append(t.nodes[node], physCore{id: usable[0], siblings: usable, node: node})
		t.total++
	}
	for n := range t.nodes {
		sort.Slice(t.nodes[n], func(i, j int) bool { return t.nodes[n][i].id < t.nodes[n][j].id })
	}
	return t, nil
}

func allowedCPUs() ([]int, error) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Cpus_allowed_list:") {
			return parseCPUList(strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:")))
		}
	}
	return nil, fmt.Errorf("no Cpus_allowed_list in /proc/self/status")
}

// nodeOf returns the NUMA node for a logical CPU, or 0 when the machine reports
// no NUMA topology at all (single-node boxes, most containers).
func nodeOf(cpu int) int {
	ents, err := os.ReadDir("/sys/devices/system/node")
	if err != nil {
		return 0
	}
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "node") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(e.Name(), "node"))
		if err != nil {
			continue
		}
		if _, err := os.Stat(fmt.Sprintf("/sys/devices/system/node/node%d/cpu%d", n, cpu)); err == nil {
			return n
		}
	}
	return 0
}

// nodeIDs returns the NUMA nodes in a stable order.
func (t *topology) nodeIDs() []int {
	var ids []int
	for n := range t.nodes {
		ids = append(ids, n)
	}
	sort.Ints(ids)
	return ids
}

// runningHeavyCPUs returns logical CPUs on which a foreign process that has
// accumulated significant CPU time is CURRENTLY resident (state R).
//
// It is deliberately NOT a utilisation measurement, and the name says so after
// external review M2: the previous signature took a `pct` threshold it never
// used (the body literally read `_ = pct`) while the docs around it claimed the
// runner "automatically avoids cores carrying an external workload above 50%".
// One instantaneous /proc sample cannot support that claim — a long-lived
// process that happens to be scheduled here right now looks busy, and a process
// hammering this core in short bursts looks idle.
//
// What it is actually good for: this box runs unrelated training jobs, and a
// core with a big process sitting on it right now is a decent guess at a core
// worth skipping. Skipping is cheap; the cost of being wrong is one fewer
// worker, never a wrong result.
//
// Best-effort by construction: a process that starts after the sample is not
// seen, and any sampling error yields an empty set (pin everything, as before).
// Never fails the run.
func runningHeavyCPUs(self int) map[int]bool {
	out := map[int]bool{}
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			continue
		}
		// The comm field can contain spaces and parens; fields after the final
		// ')' are positionally stable.
		i := strings.LastIndexByte(string(b), ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(string(b)[i+1:])
		// After comm: state(0) ppid(1) ... utime(11) stime(12) ... processor(36)
		if len(f) < 37 {
			continue
		}
		utime, e1 := strconv.ParseFloat(f[11], 64)
		stime, e2 := strconv.ParseFloat(f[12], 64)
		cpu, e3 := strconv.Atoi(f[36])
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		// A process that has accumulated real CPU time AND is currently resident
		// on this core. Coarse on purpose — the goal is "something big lives
		// here", not precise accounting. See the doc comment for why this is not
		// dressed up as a percentage.
		if (utime+stime) > 1000 && f[0] == "R" {
			out[cpu] = true
		}
	}
	return out
}

// excludeBusy drops physical cores whose siblings overlap a busy CPU. Whole
// cores are dropped, never half: leaving one sibling of a contended core is the
// same false-isolation trap as splitting a core across workers.
func (t *topology) excludeBusy(busy map[int]bool) (dropped int) {
	if len(busy) == 0 {
		return 0
	}
	for n, cores := range t.nodes {
		kept := cores[:0]
		for _, c := range cores {
			hot := false
			for _, s := range c.siblings {
				if busy[s] {
					hot = true
					break
				}
			}
			if hot {
				dropped++
				continue
			}
			kept = append(kept, c)
		}
		t.nodes[n] = kept
	}
	t.total -= dropped
	return dropped
}
