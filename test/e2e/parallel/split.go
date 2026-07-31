package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// placeholderArg marks an argv element we could not read as a string literal.
// It is recorded rather than skipped so later arguments keep their POSITIONS:
// dropping one shifts everything left, and the first casualty is
// `-timeout phaseTimeout.String()` — the flag then swallows the package path
// that follows it. Wherever a placeholder lands on something load-bearing
// (argv[0], a -run value) the honest answer is "this command was not parsed",
// which routes the matrix to the whole-run fallback.
const placeholderArg = "<expr>"

// unit is one schedulable piece of work: a single package built with one
// matrix's flags.
type unit struct {
	matrix    string // TestD5Matrix
	pkg       string // ./internal/broker/...
	tags      string // d5_integration ("" when the matrix passes none)
	race      bool
	timeo     string
	name      string // display label
	whole     bool   // run the whole matrix (unparsed command shape)
	runFilter string // -run regexp when this unit is one shard of a package
	baseArgs  []string
	hasRun    bool // baseArgs already contains the matrix's original -run
}

// splitMatrices reads all_phases_test.go and turns each D-matrix into one unit
// PER PACKAGE.
//
// Why this is where the remaining time goes. A matrix is not a test suite; it is
// one `go test` invocation listing several packages, and the lists overlap
// heavily: internal/cluster appears in five matrices, internal/broker in two
// (~100s each on its own). D4 and D5 both run at ~4m45s and both are dominated
// by internal/broker, so the whole parallel run is pinned at ~4m48s no matter
// how many cores are idle — max(matrix), not sum(work), is the ceiling.
//
// De-duplicating those repeats is NOT what this does, and deliberately so: the
// matrices build the same package under different tags (d3_integration,
// d5_integration, none), so "the same package" is not the same binary. The
// quality audit already proposed de-duplication and it was rejected for exactly
// that reason. Splitting is the safe half: identical work, more units to
// schedule.
//
// Parsing the source rather than hardcoding the lists means a new matrix, or a
// package added to an existing one, is picked up automatically. A matrix whose
// shape this cannot parse is reported, not silently dropped.
func splitMatrices(root string) ([]unit, []string, error) {
	path := filepath.Join(root, "test", "e2e", "all_phases_test.go")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("parse %s: %w", path, err)
	}

	var units []unit
	var unparsed []string
	funcs := map[string]*ast.FuncDecl{}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
			funcs[fd.Name.Name] = fd
		}
	}
	// A command hidden in a same-file helper cannot be safely combined with
	// units parsed from the Test body: the helper may be dynamic or called in a
	// loop. Treat the entire matrix as unparsed instead of guessing how many
	// times the helper executes.
	var helperMayExec func(string, map[string]bool) bool
	helperMayExec = func(name string, visiting map[string]bool) bool {
		fd := funcs[name]
		if fd == nil || visiting[name] {
			return false
		}
		visiting[name] = true
		defer delete(visiting, name)
		found := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if found {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Command" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "exec" {
					found = true
					return false
				}
			}
			if id, ok := call.Fun.(*ast.Ident); ok && helperMayExec(id.Name, visiting) {
				found = true
				return false
			}
			return true
		})
		return found
	}

	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil || !strings.HasPrefix(fd.Name.Name, "Test") {
			continue
		}
		name := fd.Name.Name
		// External review B2. `found` used to be one bool for the whole function,
		// so a matrix with two commands — one parseable, one dynamic — set it on
		// the first and ran only that half, silently. Counting is the fix: units
		// are accepted only when EVERY command in the function parsed, and the
		// partial units are thrown away otherwise. Under-coverage must never be
		// the cheaper branch.
		var fnUnits []unit
		targets, parsed := 0, 0
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name != name &&
				helperMayExec(id.Name, map[string]bool{name: true}) {
				targets++
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Command" {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "exec" {
				return true
			}
			// Non-literal arguments become a placeholder rather than being
			// dropped. Skipping them shifts every later argument left, and the
			// first casualty is `-timeout phaseTimeout.String()`: the flag then
			// consumes the package path that follows it, and go test rejects the
			// whole command. Position matters more than the value here.
			var args []string
			for _, a := range call.Args {
				lit, ok := a.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					args = append(args, placeholderArg)
					continue
				}
				if s, err := strconv.Unquote(lit.Value); err == nil {
					args = append(args, s)
				} else {
					args = append(args, placeholderArg)
				}
			}
			if len(args) == 0 {
				return true
			}
			if args[0] != "go" {
				// A literal non-go command (git, sh, ...) is not a target and does
				// not make the matrix unparseable. A PLACEHOLDER argv[0] does: we
				// cannot tell whether it was a `go test`, so it counts as a target
				// we failed to read, which forces the whole-matrix fallback.
				if args[0] == placeholderArg {
					targets++
				}
				return true
			}
			targets++
			u := parseGoTestArgs(name, args)
			if len(u) == 0 {
				return true
			}
			parsed++
			fnUnits = append(fnUnits, u...)
			return true
		})
		if targets > 0 && parsed == targets {
			units = append(units, fnUnits...)
			continue
		}
		// Reaching here means either no command was found at all, or at least one
		// target command could not be fully parsed. Both fall back to running the
		// matrix whole, and any partial units built above are discarded.
		//
		// This used to additionally require a "Matrix" name suffix, and that
		// filter silently dropped TestAllPhases — whose exec.Command lives in
		// runPhase(), a different function, so nothing parsed — taking all 11
		// phase suites (p1-p10, p13) out of every parallel run. The runs were
		// fast and green and missing a third of the gate. Never name-filter the
		// fallback: not-parsed must mean run-it-whole, for everything.
		unparsed = append(unparsed, name)
	}
	sort.Slice(units, func(i, j int) bool { return units[i].name < units[j].name })
	return units, unparsed, nil
}

// parseGoTestArgs pulls the tags, -race, timeout and package list out of one
// `go test ...` argument vector.
func parseGoTestArgs(matrix string, args []string) []unit {
	if len(args) < 2 || args[0] != "go" || args[1] != "test" {
		return nil
	}
	var tags, timeo, runf string
	race := false
	var pkgs []string
	base := []string{"test"}
	hasCount := false
	for i := 2; i < len(args); i++ {
		if args[i] == placeholderArg {
			return nil
		}
		switch a := args[i]; {
		case a == "-race":
			race = true
			base = append(base, a)
		case a == "-v" || a == "-short" || a == "-failfast":
			base = append(base, a)
		case a == "-count" || a == "-tags" || a == "-timeout" || a == "-run":
			if i+1 >= len(args) || args[i+1] == placeholderArg {
				return nil
			}
			v := args[i+1]
			base = append(base, a, v)
			switch a {
			case "-count":
				hasCount = true
			case "-tags":
				tags = v
			case "-timeout":
				timeo = v
			case "-run":
				runf = v
			}
			i++
		case strings.HasPrefix(a, "-count="):
			hasCount = true
			base = append(base, a)
		case strings.HasPrefix(a, "-tags="):
			tags = strings.TrimPrefix(a, "-tags=")
			base = append(base, a)
		case strings.HasPrefix(a, "-timeout="):
			timeo = strings.TrimPrefix(a, "-timeout=")
			base = append(base, a)
		case strings.HasPrefix(a, "-run="):
			runf = strings.TrimPrefix(a, "-run=")
			base = append(base, a)
		case strings.HasPrefix(a, "./"):
			pkgs = append(pkgs, a)
		default:
			// Dropping an unfamiliar flag changes the command. Whole-matrix
			// fallback is slower but preserves semantics and makes future Go
			// flags safe by default.
			return nil
		}
	}
	if len(pkgs) == 0 {
		return nil
	}
	if !hasCount {
		base = append(base, "-count=1")
	}
	if timeo == "" {
		timeo = "300s"
		base = append(base, "-timeout", timeo)
	}
	out := make([]unit, 0, len(pkgs))
	for _, p := range pkgs {
		short := strings.TrimSuffix(strings.TrimPrefix(p, "./"), "/...")
		out = append(out, unit{
			matrix: matrix, pkg: p, tags: tags, race: race, timeo: timeo,
			runFilter: runf,
			baseArgs:  append([]string(nil), base...),
			hasRun:    runf != "",
			name:      strings.TrimSuffix(strings.TrimPrefix(matrix, "Test"), "Matrix") + ":" + short,
		})
	}
	return out
}

// goTestArgs renders the command for one unit.
func (u unit) goTestArgs() []string {
	args := append([]string(nil), u.baseArgs...)
	if len(args) == 0 {
		// Backwards-compatible construction for unit tests and callers that
		// build a unit directly instead of through parseGoTestArgs.
		args = []string{"test", "-count=1"}
		if u.race {
			args = append(args, "-race")
		}
		if u.tags != "" {
			args = append(args, "-tags", u.tags)
		}
		// A local, not a write to the value receiver: assigning to u.timeo here mutates a COPY, so the
		// value would be silently discarded if anything downstream ever read it back.
		timeo := u.timeo
		if timeo == "" {
			timeo = "300s"
		}
		args = append(args, "-timeout", timeo)
	}
	if u.runFilter != "" && !u.hasRun {
		args = append(args, "-run", u.runFilter)
	}
	args = append(args, u.pkg)
	return args
}

// whole marks a unit that must run as a whole matrix (its command shape could
// not be parsed into packages). Split mode never drops coverage silently.
func (u unit) isWhole() bool { return u.whole }

// repoRoot walks up from the working directory to the module root.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}
