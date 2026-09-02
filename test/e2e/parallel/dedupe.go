package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// dedupe.go — fold units that build the SAME binary and run the SAME selection, proved per run.
//
// WHY THIS EXISTS, AND WHY IT IS HERE AND NOT IN all_phases_test.go
// -----------------------------------------------------------------
// The D matrices list internal/cluster five times and internal/broker twice (all_phases_test.go's
// KNOWN REDUNDANCY note). The reason de-duplication was refused — "different -tags, different
// binary" — was measured false on 2026-09-01 for those packages (split.go's note). But it is TRUE for
// at least one pair: phasefluidity_integration adds a file to internal/broker. So the fold cannot
// be a static list of "known-identical" pairs; it has to be proved on the tree the run sees, and the
// only judge that cannot be wrong about "same binary" is the build closure itself:
//
//	go list -deps -test [-race] [-tags T] -f '{{.ImportPath}} {{.GoFiles}} {{.Imports}}' ./pkg
//
// hashed. Equal closures ⇒ equal test binaries ⇒ the second run proves nothing the first did not.
// Different closures, or a go list error, ⇒ both units are kept (fail-open: the cost of a wrong
// fold is a silently narrower matrix, the cost of a wrong keep is a few minutes) — and the KEEP is
// reported with its reason, because a hasher that is broken for every group looks exactly like a
// tree with no duplicates, and "dedupe: 38 -> 38" must not be the only tell (internal review L4-F7).
//
// The plan (docs/reviews/test-system-overhaul-plan.md B6) wrote the template with the two
// test-file fields listed separately after GoFiles. With -deps -test, go list emits the
// `pkg [pkg.test]` and `pkg_test [pkg.test]` variants whose GoFiles ALREADY hold the _test.go
// files, so GoFiles + Imports over the closure is the same identity with the import edges added; the
// implementation uses that form (internal review L4-F9 registered the deviation).
//
// `-race` is part of the identity: it selects the `race` build tag, so a `//go:build race && …`
// file would make the -race binary differ from the plain one while an un-flagged go list said
// "identical" (shard.go's listPackageTests passes -race for the same reason; internal review L4-F6).
// Groups are race-homogeneous, so the flag costs nothing to carry.
//
// The matrix source is not touched: `make e2e-one T=TestD5Matrix` still means the whole D5
// surface, and a deleted glob there would still be silent. Folding lives in the runner, is printed
// in the plan with its hashes, and is one flag away (-dedupe=false).
//
// origin: docs/reviews/test-system-overhaul-plan.md B6 (§0 A8; infra I2 + conservative A9).

// foldNote records one candidate group for the plan printout and the receipt. A group that was
// folded has kept + dropped; a group that was KEPT APART has kept (the group's first unit), no
// dropped and a reason — it is printed too, so the receipt distinguishes "no duplicates" from
// "duplicates the hasher could not prove".
type foldNote struct {
	kept            string
	dropped         []string
	droppedMatrices []string // matrices the dropped units belonged to (coverage self-check input)
	hash            string
	reason          string // non-empty only for a group kept apart
}

// closureHasher returns the build-identity hash of (pkg, tags, race). Injected so the fold logic is
// unit-testable without go list.
type closureHasher func(pkg, tags string, race bool) (string, error)

// goListArgs is the exact go list invocation that defines build identity. Pure; pinned by
// TestClosureHashArgsCarryTheRaceBuildTag.
func goListArgs(pkg, tags string, race bool) []string {
	args := []string{"list", "-deps", "-test"}
	if race {
		args = append(args, "-race")
	}
	if tags != "" {
		args = append(args, "-tags", tags)
	}
	return append(args, "-f", "{{.ImportPath}} {{.GoFiles}} {{.Imports}}", pkg)
}

// goListClosureHash is the production hasher. Only STDOUT is hashed: stderr carries diagnostics
// (a read-only module cache makes go list print a random temp-file name there, which turned equal
// closures into unequal hashes — fail-open, so safe, but the fold silently stopped happening;
// external review suggestion 3). Stderr is kept for the error message only.
func goListClosureHash(pkg, tags string, race bool) (string, error) {
	cmd := exec.Command("go", goListArgs(pkg, tags, race)...)
	// The runner is invoked from the repo root by the Makefile; this package's own tests are not.
	// Pin the working directory so `./internal/...` patterns resolve in both.
	if root, rerr := repoRoot(); rerr == nil {
		cmd.Dir = root
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go list %s (tags %q, race %v): %v: %s", pkg, tags, race, err, strings.TrimSpace(stderr.String()))
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:8]), nil
}

// dedupeUnits folds duplicate package units. Units are grouped by (pkg, race, hasRun, runFilter,
// extra); whole-matrix units are never grouped. Inside a group, every distinct tags value is hashed;
// only if ALL hashes are equal is the group folded to its alphabetically-first unit, whose -timeout
// becomes the group's maximum. Returns the surviving units (input order preserved) and one note per
// candidate group — folded or kept apart.
func dedupeUnits(units []unit, hash closureHasher) ([]unit, []foldNote, error) {
	type groupKey struct {
		pkg, runFilter, extra string
		race, hasRun          bool
	}
	groups := map[groupKey][]int{}
	var order []groupKey
	for i, u := range units {
		if u.isWhole() {
			continue
		}
		k := groupKey{u.pkg, u.runFilter, u.extra, u.race, u.hasRun}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], i)
	}
	drop := map[int]bool{}
	var notes []foldNote
	for _, k := range order {
		idx := groups[k]
		if len(idx) < 2 {
			continue
		}
		// Keep the alphabetically-first NAME; it is the group's representative in both outcomes.
		sort.Slice(idx, func(a, b int) bool { return units[idx[a]].name < units[idx[b]].name })
		keep := idx[0]
		hashes := map[string]string{} // tags -> hash
		var first string
		reason := ""
		for _, i := range idx {
			tags := units[i].tags
			if _, done := hashes[tags]; done {
				continue
			}
			h, err := hash(units[i].pkg, tags, units[i].race)
			if err != nil {
				// Fail-open: an unhashable unit is kept, and so is everything grouped with it.
				reason = "go list error: " + err.Error()
				break
			}
			hashes[tags] = h
			if first == "" {
				first = h
			} else if h != first {
				reason = fmt.Sprintf("closure differs: tags %q %s vs %q %s", tagsOfHash(hashes, first), first, tags, h)
			}
		}
		if reason != "" {
			notes = append(notes, foldNote{kept: units[keep].name, reason: reason})
			continue
		}
		maxTimeo := units[keep].timeo
		var dropped, droppedMatrices []string
		for _, i := range idx[1:] {
			drop[i] = true
			dropped = append(dropped, units[i].name)
			droppedMatrices = append(droppedMatrices, units[i].matrix)
			if parseTimeout(units[i].timeo) > parseTimeout(maxTimeo) {
				maxTimeo = units[i].timeo
			}
		}
		if maxTimeo != units[keep].timeo {
			units[keep].timeo = maxTimeo
			units[keep].baseArgs = replaceTimeout(units[keep].baseArgs, maxTimeo)
		}
		notes = append(notes, foldNote{kept: units[keep].name, dropped: dropped, droppedMatrices: droppedMatrices, hash: first})
	}
	var kept []unit
	for i, u := range units {
		if !drop[i] {
			kept = append(kept, u)
		}
	}
	return kept, notes, nil
}

// tagsOfHash returns the tags value that produced hash h (for the kept-apart reason text).
func tagsOfHash(hashes map[string]string, h string) string {
	for tags, hh := range hashes {
		if hh == h {
			return tags
		}
	}
	return "?"
}

// coveredMatrices is the set of matrices represented by the surviving units PLUS the matrices whose
// units were folded away under another matrix's name. A matrix every unit of which was folded is
// still covered — its work runs, once — and the coverage self-check must not report it as lost
// (internal review L4-F8: the first version would have refused to start on a correct fold).
func coveredMatrices(units []unit, notes []foldNote) map[string]bool {
	covered := map[string]bool{}
	for _, u := range units {
		covered[u.matrix] = true
	}
	for _, n := range notes {
		for _, m := range n.droppedMatrices {
			covered[m] = true
		}
	}
	return covered
}

func parseTimeout(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// replaceTimeout rewrites the -timeout value inside a parsed baseArgs slice (both `-timeout X` and
// `-timeout=X` spellings), so the kept unit really runs with the group's longest budget.
func replaceTimeout(args []string, timeo string) []string {
	out := append([]string(nil), args...)
	for i := 0; i < len(out); i++ {
		switch {
		case out[i] == "-timeout" && i+1 < len(out):
			out[i+1] = timeo
			return out
		case strings.HasPrefix(out[i], "-timeout="):
			out[i] = "-timeout=" + timeo
			return out
		}
	}
	return append(out, "-timeout", timeo)
}

// wholeUnitEnv is the environment for a WHOLE-matrix unit. The runner's -shuffle rides on the outer
// `go test` as a flag, but a whole matrix forks its own `go test` children (runPhase, the run
// helpers), and a flag on the parent never reaches them. GOFLAGS does: every `go` invocation reads
// it, and non-test subcommands ignore -shuffle. Existing GOFLAGS are kept, not replaced
// (internal review L4-F5: -shuffle was a no-op for the five whole units, which hold the eleven
// phase suites — the part of the tree with the most tests).
func wholeUnitEnv(env []string, shuffle string) []string {
	out := make([]string, 0, len(env)+1)
	if shuffle == "" {
		return append(out, env...)
	}
	found := false
	for _, kv := range env {
		if strings.HasPrefix(kv, "GOFLAGS=") {
			found = true
			kv = strings.TrimRight(kv, " ") + " -shuffle=" + shuffle
		}
		out = append(out, kv)
	}
	if !found {
		out = append(out, "GOFLAGS=-shuffle="+shuffle)
	}
	return out
}
