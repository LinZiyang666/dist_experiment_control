package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// assignment is one worker's exclusive slice of the machine.
type assignment struct {
	worker  int
	node    int
	cores   []physCore
	cpuList string // taskset -c argument
	procs   int    // GOMAXPROCS
}

// allocate divides the discovered topology among `workers`.
//
// Three properties, in priority order:
//
//  1. NO SHARING. Two workers never receive the same logical CPU. Overlap is
//     what -parallel already does, and what the serial decision was reacting to.
//
//  2. NODE LOCALITY. Every core a worker gets is on ONE NUMA node, and its
//     memory is bound to that node. On this host cross-node access costs 2.1x
//     (distance 21 vs 10); a raft handshake with a hard deadline is precisely
//     the workload that notices.
//
//  3. WHOLE PHYSICAL CORES. A worker gets both SMT siblings or neither. Handing
//     one hyperthread to worker A and its sibling to worker B looks like two
//     cores and behaves like one under contention — the kind of "we gave it
//     enough CPU" reasoning that produced the original misattribution.
//
// Workers are dealt round-robin across nodes so that a 2-worker run uses both
// sockets rather than crowding one.
//
// Nothing about the split is fixed: it is recomputed from the live topology and
// the requested worker count on every run. Pinning a matrix to "core 5" forever
// would be worse than not pinning at all — it would survive into machines where
// core 5 means something entirely different.
func allocate(t *topology, workers int) ([]assignment, error) {
	if workers < 1 {
		return nil, fmt.Errorf("workers must be >= 1, got %d", workers)
	}
	if t.total == 0 {
		return nil, fmt.Errorf("no usable physical cores discovered")
	}
	if workers > t.total {
		return nil, fmt.Errorf("%d workers requested but only %d physical cores are available; "+
			"oversubscribing would reintroduce exactly the contention this runner exists to remove",
			workers, t.total)
	}

	// Nodes with no usable cores are dropped outright: round-robin over a node
	// that can host nothing hands it workers it cannot seat. This is not
	// hypothetical — excludeBusy() leaves a fully-drained node in the map with an
	// empty slice (external review M1).
	var nodes []int
	for _, n := range t.nodeIDs() {
		if len(t.nodes[n]) > 0 {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no NUMA node has a usable physical core")
	}
	// Per-node cursors so cores are handed out without repetition.
	cursor := map[int]int{}

	// How many workers land on each node. Plain round-robin ignores CAPACITY, so
	// an uneven topology (1 core on node0, 3 on node1) refuses 3 workers it can
	// actually seat: node0 gets asked for 2. Handing each worker to the node with
	// the most cores per already-assigned worker keeps the split proportional to
	// what each node can carry, and only fails when the machine genuinely cannot
	// seat the request — which the t.total check above has already ruled out.
	perNode := map[int]int{}
	for i := 0; i < workers; i++ {
		best, bestScore := -1, -1.0
		for _, n := range nodes {
			if perNode[n] >= len(t.nodes[n]) {
				continue // already one worker per core here
			}
			score := float64(len(t.nodes[n])) / float64(perNode[n]+1)
			if score > bestScore {
				best, bestScore = n, score
			}
		}
		if best < 0 {
			return nil, fmt.Errorf("cannot seat %d workers: no node has a free physical core "+
				"(total %d cores across %d node(s))", workers, t.total, len(nodes))
		}
		perNode[best]++
	}

	var out []assignment
	worker := 0
	for _, n := range nodes {
		w := perNode[n]
		if w == 0 {
			continue
		}
		avail := len(t.nodes[n])
		if avail < w {
			return nil, fmt.Errorf("node %d has %d cores for %d workers; reduce -workers", n, avail, w)
		}
		share := avail / w
		for i := 0; i < w; i++ {
			cores := t.nodes[n][cursor[n] : cursor[n]+share]
			cursor[n] += share
			out = append(out, assignment{
				worker:  worker,
				node:    n,
				cores:   cores,
				cpuList: cpuListOf(cores),
				procs:   len(cores), // one P per physical core, not per hyperthread
			})
			worker++
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].worker < out[j].worker })
	return out, nil
}

// cpuListOf renders every logical CPU of the given cores as a taskset list.
func cpuListOf(cores []physCore) string {
	var cpus []int
	for _, c := range cores {
		cpus = append(cpus, c.siblings...)
	}
	sort.Ints(cpus)
	parts := make([]string, 0, len(cpus))
	for _, c := range cpus {
		parts = append(parts, strconv.Itoa(c))
	}
	return strings.Join(parts, ",")
}

// String renders an assignment for the run log — an operator reading a flake
// report needs to know which slice of the machine that worker actually had.
func (a assignment) String() string {
	return fmt.Sprintf("worker %d: node %d, %d physical cores (GOMAXPROCS=%d), cpus %s",
		a.worker, a.node, len(a.cores), a.procs, a.cpuList)
}

// reserveHeavyWorker merges `merge` adjacent same-node workers into one WIDE
// worker, reserved for units that cannot be split.
//
// Why this exists (external review B1's "weight/admission control, not just CPU
// pinning"). Every worker gets an equal slice, which is right for a unit that is
// one `go test` on one package. It is wrong for TestAllPhases: that unit runs 11
// phase subprocesses SERIALLY, each one a -race binary with an embedded NATS
// server and many goroutines. An equal slice at 20 workers is 2 physical cores,
// and a phase that finishes in 0.9s with the machine to itself then takes over
// 15s — long enough to blow a 15s "eventually clears" assertion in test/p13 and
// look like a product defect (proxy_ready not clearing on tunnel drop).
//
// Measured: 8 isolated runs of that test span 0.81-1.38s. The same test inside a
// 2-core TestAllPhases unit exceeded 15s. The work did not get slower; it got
// less machine.
func reserveHeavyWorker(plan []assignment, merge int) ([]assignment, *assignment) {
	if merge < 2 || len(plan) <= merge {
		return plan, nil
	}
	// Merge within ONE NUMA node — a wide worker spanning both sockets would
	// reintroduce exactly the cross-node traffic this runner exists to avoid.
	// Select the node that can contribute the most cores. Looking only at the
	// final assignment made an uneven topology return no wide worker when the
	// last node was small even though another node had enough slices.
	byNode := map[int][]int{}
	for i := range plan {
		byNode[plan[i].node] = append(byNode[plan[i].node], i)
	}
	node := -1
	bestCores := -1
	for n, candidates := range byNode {
		if len(candidates) < merge {
			continue
		}
		cores := 0
		for _, i := range candidates[len(candidates)-merge:] {
			cores += len(plan[i].cores)
		}
		if cores > bestCores || (cores == bestCores && (node < 0 || n < node)) {
			node, bestCores = n, cores
		}
	}
	if node < 0 {
		return plan, nil
	}
	candidates := byNode[node]
	idx := candidates[len(candidates)-merge:]
	keep := map[int]bool{}
	var cores []physCore
	for _, i := range idx {
		keep[i] = true
		cores = append(cores, plan[i].cores...)
	}
	heavy := assignment{
		worker:  plan[idx[len(idx)-1]].worker,
		node:    node,
		cores:   cores,
		cpuList: cpuListOf(cores),
		procs:   len(cores),
	}
	var rest []assignment
	for i, a := range plan {
		if !keep[i] {
			rest = append(rest, a)
		}
	}
	return rest, &heavy
}
