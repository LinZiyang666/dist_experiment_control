package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// spawn_stall_evidence_test.go — every execve/resolution watchdog that gives up must
// first expire the cached mount-health verdicts, and no invalidation may run under p.mu.
//
// origin: docs/deploy-tier-gotchas.md #81 (timan107, 2026-08-29)
// origin: docs/reviews/remote-fs-stale-health-review.md F-4, F-8, F-15, F-16, F-17, F-18
//
// # WHY THIS IS A GATE
//
// spawnsafe caches "this network mount answers" per mountpoint. Before #81 a HEALTHY
// verdict was terminal, so an 18-day-old agent whose NFS died mid-life kept a dead dir
// at the head of $PATH forever and every command failed with remote_fs_spawn_timeout.
// The fix hangs re-validation off the watchdogs: a spawn that blew its deadline IS the
// proof that some cached verdict is wrong.
//
// That makes the invalidation call a cross-cutting obligation on a set of sites, which
// is exactly the failure shape that has cost this repo three review rounds before —
// internal/tunnel's fence had to be rediscovered in CloseProxy, then CloseSession, then
// ForgetSession. So the sites are enumerated here rather than trusted to review.
//
// WHAT IS A "MINT SITE"
//
// Not "mentions the sentinel" — the two Reason strings and two Err vars appear in
// declarations, comments, errors.Is comparisons and the ctl-facing classification
// switch, none of which mint anything. A mint site is either:
//
//	&FSError{Code: Reason…}          — constructing the fail-fast error,
//	return Err…                      — returning the sentinel var, or
//	return fmt.Errorf("%w: …", Err…) — returning it wrapped.
//
// The predicate is deliberately DISCOVERY-based (walk internal/ + cmd/ and find them)
// rather than "check these files": a new watchdog added anywhere in the tree changes the
// count and turns this red, which is the whole point.
//
// KNOWN BLIND SPOTS (measured, not guessed — the first version of this block named a
// hole that does not exist and missed the ones that do; internal review F-18)
//
//   - A sentinel routed through an intermediate variable (`e := ErrSpawnTimeout; return e`)
//     or assigned to a NAMED RESULT is not seen.
//   - `Code:` written as a bare string literal ("remote_fs_spawn_timeout") instead of the
//     constant is not seen. Both are caught by the behavioural cases instead.
//   - NOT a blind spot, contrary to the first draft: import aliases and dot-imports are
//     seen, because matching is on the selector's Sel name, not on the package qualifier.
//   - The gate proves the CALL is in the right timeout arm and not under the lock. It
//     cannot prove the call does the right thing; that is what the behavioural cases in
//     internal/spawnsafe/spawnsafe_test.go and internal/agent/remotefs_test.go are for.
//
// NOT COVERED BY THIS GATE AT ALL: the fourth evidence site,
// Agent.boundedHomeRead's timeout arm. It returns (nil,false) and mints no sentinel, so
// no ledger of mint sites can reach it. Its guard is
// TestBoundedHomeRead_stallExpiresCachedMountHealth in internal/agent/remotefs_test.go.
// Stated because the earlier version of this file claimed to pin "all four".
const invalidateFn = "invalidateHealthy"

const invalidateExportedFn = "InvalidateHealthy"

// spawnStallMintLedger is the exact set of mint sites, with whether each must be wired
// to the invalidation. Keyed by "<repo-relative file>:<enclosing func>:<sentinel>" so a
// site that MOVES between functions is a change, not a silent pass.
//
// Wired=false is a decision, not an oversight, and it is enforced in BOTH directions:
// the ceiling sites report that too many spawns are ALREADY abandoned. That is a
// consequence of earlier timeouts, not new evidence about any mount, and a saturated
// ceiling produces no evidence at all — which is one of the two reasons the fix also
// carries a TTL (see DefaultHealthTTL). Wiring one of them would silently reverse that
// reasoning, so it fails here too.
var spawnStallMintLedger = map[string]bool{
	"internal/spawnsafe/spawnsafe.go:boundedResolveInDirs:ReasonSpawnTimeout":  true,
	"internal/spawnsafe/spawnsafe.go:RunStartWithCleanup:ErrSpawnTimeout":      true,
	"internal/spawnsafe/spawnsafe.go:boundedResolveInDirs:ReasonTooManyWedged": false,
	"internal/spawnsafe/spawnsafe.go:RunStartWithCleanup:ErrTooManyWedged":     false,
}

var spawnStallSentinels = map[string]bool{
	"ReasonSpawnTimeout":  true,
	"ReasonTooManyWedged": true,
	"ErrSpawnTimeout":     true,
	"ErrTooManyWedged":    true,
}

type mintSite struct {
	key string
	// inTimeoutCase reports whether the site sits inside a deadline select arm, and
	// invalidated whether THAT arm calls the invalidation. Both are needed: a call
	// anywhere else in the function would satisfy a naive "does the function mention
	// it" check while doing nothing on the path that matters.
	inTimeoutCase bool
	invalidated   bool
	// blockInvalidated is the same question asked of the mint's nearest enclosing
	// block rather than of a deadline arm. The unwired ceiling sites are plain early
	// returns with no arm at all, so without this the "must NOT be wired" half of the
	// ledger could never fail (internal review, second gate mutation round).
	blockInvalidated bool
}

// lockedInvalidation is an invalidation call that runs while p.mu is held.
type lockedInvalidation struct {
	file string
	fn   string
}

func TestSpawnTimeoutMintSitesNoteEvidence(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()

	var sites []mintSite
	var locked []lockedInvalidation
	invalidateFnDeclared := false

	// Pass 1 collects every function so the lock check can follow ONE indirection.
	// Matching only direct calls was defeated by the single cheapest edit there is —
	// extract `func (p *Policy) mountHealthyReArm() { p.invalidateHealthy() }` and call
	// that under the lock: gate green, agent hard-deadlocked (measured).
	type fnDecl struct {
		rel string
		fn  *ast.FuncDecl
	}
	var allFuncs []fnDecl

	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			rel, rerr := filepath.Rel(root, path)
			if rerr != nil {
				t.Fatalf("rel %s: %v", path, rerr)
			}
			rel = filepath.ToSlash(rel)

			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				if fn.Name.Name == invalidateFn || fn.Name.Name == invalidateExportedFn {
					invalidateFnDeclared = true
					continue // its own body legitimately takes p.mu
				}
				allFuncs = append(allFuncs, fnDecl{rel: rel, fn: fn})
				sites = append(sites, mintSitesIn(fn, rel)...)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	// A rename must make this gate FAIL LOUDLY rather than quietly find nothing to
	// check — a gate that blinds itself is worse than no gate. Either spelling counts,
	// so dropping the unexported wrapper is a refactor, not a gate break.
	if !invalidateFnDeclared {
		t.Fatalf("no %s/%s function found: the gate can no longer see what it claims to check",
			invalidateFn, invalidateExportedFn)
	}

	// Pass 2: names that reach the invalidation, to a fixed point. One hop would be
	// enough for the demonstrated bypass; the closure costs nothing and does not have
	// to be re-derived the next time someone adds a wrapper.
	invalidating := map[string]bool{invalidateFn: true, invalidateExportedFn: true}
	for changed := true; changed; {
		changed = false
		for _, f := range allFuncs {
			if invalidating[f.fn.Name.Name] {
				continue
			}
			if callsAnyOf(f.fn.Body, invalidating) {
				invalidating[f.fn.Name.Name] = true
				changed = true
			}
		}
	}
	// Scoped to internal/spawnsafe: Policy.mu is the mutex invalidateHealthy takes, and
	// no other package can hold it. Matching any field named `mu` tree-wide would flag
	// unrelated critical sections (internal/agent holds several while calling helpers
	// that transitively reach the invalidation) — false positives that would get the
	// gate deleted rather than obeyed.
	for _, f := range allFuncs {
		if !strings.HasPrefix(f.rel, "internal/spawnsafe/") {
			continue
		}
		if lockedInvalidateIn(f.fn, invalidating) {
			locked = append(locked, lockedInvalidation{file: f.rel, fn: f.fn.Name.Name})
		}
	}

	// Clause ③ — no invalidation may run while p.mu is held. p.mu is NOT reentrant and
	// invalidateHealthy takes it, so such a call deadlocks every spawn on the agent —
	// strictly worse than the bug being fixed.
	//
	// This is a lock-domination check, not a name check. The first version matched the
	// literal function name "mountHealthy"; extracting any part of that 100+ line
	// function into a helper (the lowest-friction next edit, and funlen is deliberately
	// off in .golangci.yml) blinded it completely while the code hard-deadlocked.
	for _, l := range locked {
		t.Errorf("%s: %s calls %s while p.mu is held — p.mu is not reentrant, this deadlocks "+
			"every spawn. Release the lock first, or hoist the call out of the critical section.",
			l.file, l.fn, invalidateFn)
	}

	// Clause ① — the mint-site set must equal the ledger exactly.
	got := map[string]mintSite{}
	conflicts := map[string]bool{}
	for _, s := range sites {
		prev, dup := got[s.key]
		if dup && (prev.inTimeoutCase != s.inTimeoutCase || prev.invalidated != s.invalidated) {
			// Two mints of the same sentinel in one function that disagree about wiring
			// used to collapse in the LENIENT direction, hiding an unwired early return
			// behind a wired one (internal review F-9).
			conflicts[s.key] = true
		}
		if !dup || (!prev.invalidated && s.invalidated) {
			got[s.key] = s
		}
	}
	for key := range conflicts {
		t.Errorf("%s: this function mints the same sentinel from two places that disagree about "+
			"whether they expire cached mount health. Split them, or give the ledger both.", key)
	}
	var gotKeys, wantKeys []string
	for k := range got {
		gotKeys = append(gotKeys, k)
	}
	for k := range spawnStallMintLedger {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	if strings.Join(gotKeys, "\n") != strings.Join(wantKeys, "\n") {
		t.Errorf("spawn-stall mint sites drifted.\n got:\n  %s\nwant:\n  %s\n\n"+
			"A NEW site is not automatically wrong — decide whether it is evidence that a cached\n"+
			"mount-health verdict is stale, wire it (or not), and update spawnStallMintLedger with\n"+
			"the reason. Do not widen the ledger to silence this.",
			strings.Join(gotKeys, "\n  "), strings.Join(wantKeys, "\n  "))
	}

	// Clause ② — the ledger is enforced in both directions, and a wired site's
	// invalidation must live in the SAME deadline arm. Checking the function body
	// instead would let someone move the call into the success branch with the gate
	// still green and the fix completely inert.
	for key, wantWired := range spawnStallMintLedger {
		s, ok := got[key]
		if !ok {
			continue // already reported by clause ①
		}
		if !wantWired {
			if s.invalidated || s.blockInvalidated {
				t.Errorf("%s: the ledger records this site as NOT evidence (a saturated ceiling "+
					"reports earlier abandonments, not a new fact about any mount). Wiring it "+
					"reverses that decision silently — change the ledger and say why.", key)
			}
			continue
		}
		if !s.inTimeoutCase {
			t.Errorf("%s: ledger says wired, but the mint is not inside a deadline select arm", key)
			continue
		}
		if !s.invalidated {
			t.Errorf("%s: timeout arm returns the sentinel without calling %s — "+
				"a stalled spawn is the evidence that some cached healthy verdict is stale (#81)",
				key, invalidateFn)
		}
	}
}

// origin: remote-fs stale-health external review F1
// A timeout arm may contain more than one return. The invalidation must dominate
// each mint site; merely appearing somewhere in the same arm is insufficient.
func TestSpawnStallEvidenceGateRejectsReturnBeforeInvalidation(t *testing.T) {
	const src = `package sample
func (p *Policy) watchdog(skip bool) error {
	select {
	case <-time.After(timeout):
		if skip {
			return ErrSpawnTimeout
		}
		p.invalidateHealthy()
		return ErrSpawnTimeout
	}
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	sites := mintSitesIn(fn, "sample.go")
	if len(sites) != 2 {
		t.Fatalf("found %d mint sites, want 2", len(sites))
	}
	wired := 0
	for _, site := range sites {
		if site.invalidated {
			wired++
		}
	}
	if wired != 1 {
		t.Fatalf("gate classified %d/2 sites as invalidated, want 1/2: the early return "+
			"does not execute the later invalidation and must make the ledger fail", wired)
	}
}

// origin: remote-fs stale-health external review F2
// Source-order Lock/Unlock counting is not a lock-domination check when the
// Unlock is conditional: the false branch reaches invalidation with p.mu held.
func TestSpawnStallEvidenceGateRejectsConditionalUnlockBeforeInvalidation(t *testing.T) {
	const src = `package sample
func (p *Policy) bad(unlock bool) {
	p.mu.Lock()
	if unlock {
		p.mu.Unlock()
	}
	p.invalidateHealthy()
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if !lockedInvalidateIn(fn, map[string]bool{invalidateFn: true}) {
		t.Fatal("gate accepted conditional Unlock before invalidation; unlock=false deadlocks on p.mu")
	}
}

// origin: remote-fs stale-health external re-review F1
// A plain go statement is asynchronous even without a function literal. The returning
// watchdog cannot claim that the next command will observe an invalidated generation.
func TestSpawnStallEvidenceGateRejectsAsyncDirectInvalidation(t *testing.T) {
	const src = `package sample
func (p *Policy) watchdog() error {
	select {
	case <-time.After(timeout):
		go p.invalidateHealthy()
		return ErrSpawnTimeout
	}
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	sites := mintSitesIn(fn, "sample.go")
	if len(sites) != 1 {
		t.Fatalf("found %d mint sites, want 1", len(sites))
	}
	if sites[0].invalidated {
		t.Fatal("gate accepted go p.invalidateHealthy() as synchronous evidence wiring")
	}
}

// origin: remote-fs stale-health external re-review F2
// IfStmt.Init is a real statement on every path. Ignoring it lets the cheapest
// syntactic relocation of Lock hide a deterministic re-entrant deadlock.
func TestSpawnStallEvidenceGateRejectsLockInIfInitializer(t *testing.T) {
	const src = `package sample
func (p *Policy) bad(ok bool) {
	if p.mu.Lock(); ok {
	}
	p.invalidateHealthy()
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if !lockedInvalidateIn(fn, map[string]bool{invalidateFn: true}) {
		t.Fatal("gate ignored p.mu.Lock() in if initializer and accepted a locked invalidation")
	}
}

// origin: remote-fs stale-health external re-review F3
// Sequential AST order is not control-flow order: goto can jump over the call that the
// scanner would otherwise see before the mint.
func TestSpawnStallEvidenceGateRejectsGotoAroundInvalidation(t *testing.T) {
	const src = `package sample
func (p *Policy) watchdog(skip bool) error {
	select {
	case <-time.After(timeout):
		if skip {
			goto done
		}
		p.invalidateHealthy()
	done:
		return ErrSpawnTimeout
	}
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	sites := mintSitesIn(fn, "sample.go")
	if len(sites) != 1 {
		t.Fatalf("found %d mint sites, want 1", len(sites))
	}
	if sites[0].invalidated {
		t.Fatal("gate accepted a goto path that jumps over invalidation before the mint")
	}
}

// origin: remote-fs stale-health external re-review 2
// The lock walker must not credit an Unlock that goto jumps over. This is the lock
// analogue of the mint-wiring bypass above and deterministically deadlocks when skip
// is true.
func TestSpawnStallEvidenceGateRejectsGotoAroundUnlock(t *testing.T) {
	const src = `package sample
func (p *Policy) bad(skip bool) {
	p.mu.Lock()
	if skip {
		goto invalidate
	}
	p.mu.Unlock()
invalidate:
	p.invalidateHealthy()
}`
	file, err := parser.ParseFile(token.NewFileSet(), "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if !lockedInvalidateIn(fn, map[string]bool{invalidateFn: true}) {
		t.Fatal("gate accepted goto around Unlock; skip=true reaches invalidation with p.mu held")
	}
}

// origin: remote-fs stale-health external re-review 2
// Defers execute LIFO. Here invalidateHealthy runs before Unlock at return and
// re-enters p.mu. Ignoring every DeferStmt made this deterministic deadlock invisible.
func TestSpawnStallEvidenceGateRejectsDeferredInvalidationUnderLock(t *testing.T) {
	const src = `package sample
func (p *Policy) bad() {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.invalidateHealthy()
}`
	file, err := parser.ParseFile(token.NewFileSet(), "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if !lockedInvalidateIn(fn, map[string]bool{invalidateFn: true}) {
		t.Fatal("gate ignored deferred invalidation that executes before deferred Unlock")
	}
}

// lockState is a three-valued lock fact: the analysis must be able to say "I do not
// know", and must treat that as held.
type lockState int

const (
	lockFree lockState = iota
	lockHeld
	lockUnknown
)

func joinLockState(a, b lockState) lockState {
	if a == b {
		return a
	}
	return lockUnknown
}

// lockedInvalidateIn reports whether fn can reach the invalidation with p.mu held.
//
// PATH-SENSITIVE. The first version counted Lock/Unlock occurrences in source order,
// which is not a domination check at all — a conditional unlock decremented the counter
// on behalf of a path that never ran:
//
//	p.mu.Lock()
//	if unlock {
//	    p.mu.Unlock()
//	}
//	p.invalidateHealthy()   // unlock=false ⇒ re-enters a non-reentrant mutex, deadlock
//
// The gate accepted that while CLAUDE.md and gotcha #81 advertised it as "any call under
// p.mu is red" — a documented guarantee the code did not provide (external review F3).
// Branches now JOIN: if two paths disagree about the lock, the result is lockUnknown and
// lockUnknown counts as held. Being wrong in that direction produces a loud gate failure;
// being wrong the other way produces an agent-wide spawn deadlock.
//
// SCOPE: callers apply this only to internal/spawnsafe. The mutex that
// invalidateHealthy takes is Policy.mu, which no other package can hold, and matching
// any field named `mu` across the tree would flag unrelated critical sections.
func lockedInvalidateIn(fn *ast.FuncDecl, invalidating map[string]bool) bool {
	violation := false

	// Refuse two control-flow shapes that the lock-state interpreter below cannot
	// model faithfully. A goto can jump over an Unlock that a source-order walk
	// credits. A deferred invalidation runs at function exit in LIFO order, not at
	// the defer statement, so proving it lock-free requires exit-path/defer-stack
	// analysis. Fail closed only when the function also uses Policy.mu and reaches
	// an invalidating call; unrelated labels and defers remain outside this gate.
	hasPolicyLock := false
	hasInvalidation := false
	hasDeferredInvalidation := false
	hasJumpOrLabel := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.FuncLit:
			return false // another function's control flow
		case *ast.DeferStmt:
			if callsAnyOf(v.Call, invalidating) {
				hasInvalidation = true
				hasDeferredInvalidation = true
			}
			return false // never credit a deferred Unlock as immediate
		case *ast.BranchStmt:
			if v.Tok == token.GOTO {
				hasJumpOrLabel = true
			}
		case *ast.LabeledStmt:
			hasJumpOrLabel = true
		case *ast.CallExpr:
			switch f := v.Fun.(type) {
			case *ast.SelectorExpr:
				if isPolicyMutex(f.X) && f.Sel.Name == "Lock" {
					hasPolicyLock = true
				}
				hasInvalidation = hasInvalidation || invalidating[f.Sel.Name]
			case *ast.Ident:
				hasInvalidation = hasInvalidation || invalidating[f.Name]
			}
		}
		return true
	})
	if hasPolicyLock && hasInvalidation && (hasDeferredInvalidation || hasJumpOrLabel) {
		return true
	}

	// classify returns what a single non-compound statement does to the lock, and
	// whether it invalidates.
	classify := func(n ast.Node) (delta lockState, changes bool, invalidates bool) {
		ast.Inspect(n, func(c ast.Node) bool {
			if _, isDefer := c.(*ast.DeferStmt); isDefer {
				// A deferred unlock keeps the lock until return: it is not an unlock
				// at this point, and its body must not be read as one.
				return false
			}
			if _, isLit := c.(*ast.FuncLit); isLit {
				// A closure runs later and/or on another goroutine; its locking is not
				// this path's locking.
				return false
			}
			call, ok := c.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch f := call.Fun.(type) {
			case *ast.SelectorExpr:
				if isPolicyMutex(f.X) {
					switch f.Sel.Name {
					case "Lock":
						delta, changes = lockHeld, true
					case "Unlock":
						delta, changes = lockFree, true
					}
					return true
				}
				if invalidating[f.Sel.Name] {
					invalidates = true
				}
			case *ast.Ident:
				if invalidating[f.Name] {
					invalidates = true
				}
			}
			return true
		})
		return delta, changes, invalidates
	}

	// applyInit runs a compound statement's initializer (and any other sub-statements
	// that execute unconditionally before the body) in Go execution order. Skipping
	// these was RR-F2: `if p.mu.Lock(); ok {}` holds the lock on EVERY path, and the
	// analysis walked straight past it into a "safe" verdict for the invalidation that
	// followed. Every compound statement that can carry an Init must pass through here.
	var walk func(list []ast.Stmt, in lockState) lockState
	applyStmt := func(stmt ast.Stmt, cur lockState) lockState {
		if stmt == nil {
			return cur
		}
		delta, chg, inv := classify(stmt)
		if inv && cur != lockFree {
			violation = true
		}
		if chg {
			return delta
		}
		return cur
	}
	walk = func(list []ast.Stmt, in lockState) lockState {
		cur := in
		for _, stmt := range list {
			switch v := stmt.(type) {
			case *ast.IfStmt:
				cur = applyStmt(v.Init, cur) // executes on every path
				_, chg, inv := classify(v.Cond)
				if inv && cur != lockFree {
					violation = true
				}
				if chg {
					cur = lockUnknown // a lock op in a condition: give up precision safely
				}
				thenOut := walk(v.Body.List, cur)
				elseOut := cur
				if v.Else != nil {
					switch e := v.Else.(type) {
					case *ast.BlockStmt:
						elseOut = walk(e.List, cur)
					case *ast.IfStmt:
						elseOut = walk([]ast.Stmt{e}, cur)
					}
				}
				cur = joinLockState(thenOut, elseOut)
			case *ast.BlockStmt:
				cur = walk(v.List, cur)
			case *ast.ForStmt:
				cur = applyStmt(v.Init, cur)
				if _, chg, inv := classify(v.Cond); v.Cond != nil {
					if inv && cur != lockFree {
						violation = true
					}
					if chg {
						cur = lockUnknown
					}
				}
				// The body and post may run 0..n times; anything they change is unknown.
				body := walk(v.Body.List, cur)
				post := applyStmt(v.Post, body)
				if body != cur || post != cur {
					cur = lockUnknown
				}
			case *ast.RangeStmt:
				if out := walk(v.Body.List, cur); out != cur {
					cur = lockUnknown
				}
			case *ast.SwitchStmt:
				cur = applyStmt(v.Init, cur)
				if v.Tag != nil {
					if _, chg, inv := classify(v.Tag); true {
						if inv && cur != lockFree {
							violation = true
						}
						if chg {
							cur = lockUnknown
						}
					}
				}
				cur = walkClauses(v.Body, cur, walk)
			case *ast.TypeSwitchStmt:
				cur = applyStmt(v.Init, cur)
				cur = applyStmt(v.Assign, cur)
				cur = walkClauses(v.Body, cur, walk)
			case *ast.SelectStmt:
				cur = walkClauses(v.Body, cur, walk)
			case *ast.LabeledStmt:
				cur = walk([]ast.Stmt{v.Stmt}, cur)
			default:
				cur = applyStmt(stmt, cur)
			}
		}
		return cur
	}
	walk(fn.Body.List, lockFree)
	return violation
}

// callsAnyOf reports whether n contains a call to any of the named functions.
func callsAnyOf(n ast.Node, names map[string]bool) bool {
	found := false
	ast.Inspect(n, func(c ast.Node) bool {
		if found {
			return false
		}
		call, ok := c.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch f := call.Fun.(type) {
		case *ast.Ident:
			found = names[f.Name]
		case *ast.SelectorExpr:
			found = names[f.Sel.Name]
		}
		return !found
	})
	return found
}

// isPolicyMutex matches the `p.mu` receiver-field selector the package uses.
func isPolicyMutex(x ast.Expr) bool {
	sel, ok := x.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return sel.Sel.Name == "mu"
}

// mintSitesIn finds the mint sites inside one function and, for each, whether an
// invalidation is guaranteed to have run before it.
//
// PATH-SENSITIVE, not arm-level. The first version asked "does this deadline arm
// mention the invalidation anywhere" and reused that one boolean for every mint in the
// arm, so this shape passed with 2/2 wired when the truth is 1/2:
//
//	case <-time.After(timeout):
//	    if skip {
//	        return ErrSpawnTimeout   // ← reached without invalidating
//	    }
//	    p.invalidateHealthy()
//	    return ErrSpawnTimeout
//
// That is exactly the regression the gate exists to stop (external review F2), and it is
// the shape someone would introduce by adding a fast-path early return to an existing
// timeout arm. The walk below carries an "already invalidated on THIS path" flag and only
// propagates it past a branch when EVERY branch invalidates.
//
// Loops and switches are treated conservatively: a mint inside one is wired only if the
// invalidation preceded the whole construct, because a loop body may execute zero times
// and a switch may take an arm that does not invalidate. Under-approximating "wired" can
// only produce a loud failure, never a silent pass.
func mintSitesIn(fn *ast.FuncDecl, rel string) []mintSite {
	var out []mintSite

	// walkStmts runs one statement list in order, threading the invalidation flag.
	// It returns whether the invalidation is guaranteed on EVERY path that falls out
	// of the list (a list that always returns/breaks reports true vacuously via done).
	var walkStmts func(list []ast.Stmt, arm *ast.CommClause, seen bool) (endSeen bool)

	// walkExprMints records mints that are not in a return statement (composite
	// literals assigned or passed elsewhere) with the flag as of that point.
	walkExprMints := func(n ast.Node, arm *ast.CommClause, seen bool) {
		ast.Inspect(n, func(c ast.Node) bool {
			lit, ok := c.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if name, ok := fsErrorCodeSentinel(lit); ok {
				out = append(out, newMintSite(rel, fn.Name.Name, name, arm, seen))
			}
			return true
		})
	}

	walkStmts = func(list []ast.Stmt, arm *ast.CommClause, seen bool) bool {
		// FAIL CLOSED on control flow this walk does not model. A `goto` can jump over
		// the invalidation the linear scan already credited, so any jump or label in the
		// same statement list makes every mint from here on unwired (external re-review
		// RR-F1). The alternative — modelling labels and jumps — means writing a real
		// CFG interpreter inside a gate, which is how this check keeps acquiring new
		// blind spots. Refusing to analyse is the honest answer, and it is loud: the
		// ledger's wired sites go red until the arm is written without jumps.
		for _, stmt := range list {
			switch stmt.(type) {
			case *ast.BranchStmt, *ast.LabeledStmt:
				seen = false
			}
		}
		for _, stmt := range list {
			switch v := stmt.(type) {
			case *ast.BranchStmt, *ast.LabeledStmt:
				// Unmodelled: contribute mints (so the ledger still sees them) but never
				// credit an invalidation across the jump.
				ast.Inspect(v, func(c ast.Node) bool {
					if ret, ok := c.(*ast.ReturnStmt); ok {
						for _, name := range returnedSentinels(ret) {
							out = append(out, newMintSite(rel, fn.Name.Name, name, arm, false))
						}
					}
					return true
				})
				walkExprMints(v, arm, false)
				seen = false
			case *ast.ReturnStmt:
				for _, name := range returnedSentinels(v) {
					out = append(out, newMintSite(rel, fn.Name.Name, name, arm, seen))
				}
				walkExprMints(v, arm, seen)
				return true // nothing after a return on this path
			case *ast.IfStmt:
				if callsInvalidateDirectly(v.Cond) {
					seen = true
				}
				walkExprMints(v.Cond, arm, seen)
				thenSeen := walkStmts(v.Body.List, arm, seen)
				elseSeen := seen
				if v.Else != nil {
					switch e := v.Else.(type) {
					case *ast.BlockStmt:
						elseSeen = walkStmts(e.List, arm, seen)
					case *ast.IfStmt:
						elseSeen = walkStmts([]ast.Stmt{e}, arm, seen)
					}
				}
				// Only both-branches-invalidate propagates past the if.
				seen = thenSeen && elseSeen
			case *ast.BlockStmt:
				seen = walkStmts(v.List, arm, seen)
			case *ast.SelectStmt:
				for _, cc := range v.Body.List {
					clause, ok := cc.(*ast.CommClause)
					if !ok {
						continue
					}
					next := arm
					if isDeadlineArm(clause) {
						next = clause
					}
					// A select arm starts from the state at the select, not from a
					// sibling arm's state.
					walkStmts(clause.Body, next, seen)
				}
			case *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt:
				// Conservative: descend for mints, never let the construct set seen.
				ast.Inspect(v, func(c ast.Node) bool {
					if ret, ok := c.(*ast.ReturnStmt); ok {
						for _, name := range returnedSentinels(ret) {
							out = append(out, newMintSite(rel, fn.Name.Name, name, arm, seen))
						}
					}
					if sel, ok := c.(*ast.SelectStmt); ok && sel != nil {
						for _, cc := range sel.Body.List {
							if clause, ok := cc.(*ast.CommClause); ok && isDeadlineArm(clause) {
								walkStmts(clause.Body, clause, seen)
								return false
							}
						}
					}
					return true
				})
				walkExprMints(v, arm, seen)
			default:
				if callsInvalidateDirectly(stmt) {
					seen = true
				}
				walkExprMints(stmt, arm, seen)
			}
		}
		return seen
	}

	walkStmts(fn.Body.List, nil, false)
	return out
}

func newMintSite(rel, fnName, sentinel string, arm *ast.CommClause, invalidatedBefore bool) mintSite {
	s := mintSite{key: rel + ":" + fnName + ":" + sentinel}
	if arm != nil {
		s.inTimeoutCase = true
		s.invalidated = invalidatedBefore
	}
	s.blockInvalidated = invalidatedBefore
	return s
}

// isDeadlineArm reports whether a select arm is a deadline arm: `case <-time.After(...)`
// or `case <-someTimer.C`. The timer form is accepted because converting to
// time.NewTimer + defer Stop() is the standard fix for the leaked-timer smell, and a
// gate that reddens on a correct refactor teaches people to delete the gate.
func isDeadlineArm(cc *ast.CommClause) bool {
	expr, ok := cc.Comm.(*ast.ExprStmt)
	if !ok {
		return false
	}
	unary, ok := expr.X.(*ast.UnaryExpr)
	if !ok || unary.Op != token.ARROW {
		return false
	}
	switch v := unary.X.(type) {
	case *ast.CallExpr:
		sel, ok := v.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "After" {
			return false
		}
		pkg, ok := sel.X.(*ast.Ident)
		return ok && pkg.Name == "time"
	case *ast.SelectorExpr:
		return v.Sel.Name == "C" // <-tm.C
	}
	return false
}

// callsInvalidateDirectly reports whether a statement invalidates on the spot. It stops
// at function literals on purpose: a call hidden inside a `go func(){…}()` or a `defer`
// closure in the timeout arm runs later (or on another goroutine) and does not make the
// returning spawn's evidence available to the next one (internal review F-16).
func callsInvalidateDirectly(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(c ast.Node) bool {
		if found {
			return false
		}
		// Asynchronous forms never count as evidence wiring: the watchdog can return
		// before the goroutine runs (or, for defer, before the function returns), so the
		// NEXT command can still read a stale generation. `go func(){…}()` was already
		// excluded via FuncLit, but the shorter `go p.invalidateHealthy()` has no literal
		// at all and was accepted as synchronous (external re-review RR-F1).
		switch c.(type) {
		case *ast.FuncLit, *ast.GoStmt, *ast.DeferStmt:
			return false
		}
		call, ok := c.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			if fn.Name == invalidateFn || fn.Name == invalidateExportedFn {
				found = true
			}
		case *ast.SelectorExpr:
			if fn.Sel.Name == invalidateFn || fn.Sel.Name == invalidateExportedFn {
				found = true
			}
		}
		return !found
	})
	return found
}

// returnedSentinels lists the Err… sentinels a return statement hands back, including
// wrapped forms (`fmt.Errorf("%w: …", ErrSpawnTimeout)`), which the first version missed
// entirely — a whole extra watchdog could be added that way with the gate still green
// (internal review F-8). `errors.Is(err, Err…)` comparisons are excluded: they read the
// sentinel, they do not mint it.
func returnedSentinels(ret *ast.ReturnStmt) []string {
	var out []string
	for _, res := range ret.Results {
		ast.Inspect(res, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Is" {
					return false // errors.Is(err, sentinel) — a comparison, not a mint
				}
			}
			if name, ok := sentinelName(n); ok && strings.HasPrefix(name, "Err") {
				out = append(out, name)
				return false
			}
			return true
		})
	}
	return out
}

// sentinelName returns the sentinel identifier an expression names, handling both the
// in-package (ErrSpawnTimeout) and qualified (spawnsafe.ErrSpawnTimeout) spellings. It
// matches on the selector's Sel, so an import alias or dot-import cannot hide a site.
func sentinelName(e ast.Node) (string, bool) {
	switch v := e.(type) {
	case *ast.Ident:
		if spawnStallSentinels[v.Name] {
			return v.Name, true
		}
	case *ast.SelectorExpr:
		if spawnStallSentinels[v.Sel.Name] {
			return v.Sel.Name, true
		}
	}
	return "", false
}

// fsErrorCodeSentinel matches `FSError{Code: Reason…}` (with or without &, in or out of
// package). RunChunk/ExecChunk literals that merely CARRY the reason string to ctl are
// classification, not minting, so keying on the FSError type name is load-bearing.
func fsErrorCodeSentinel(lit *ast.CompositeLit) (string, bool) {
	typeName := ""
	switch t := lit.Type.(type) {
	case *ast.Ident:
		typeName = t.Name
	case *ast.SelectorExpr:
		typeName = t.Sel.Name
	}
	if typeName != "FSError" {
		return "", false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "Code" {
			continue
		}
		if name, ok := sentinelName(kv.Value); ok {
			return name, true
		}
	}
	return "", false
}

// walkClauses joins the lock state across every clause of a switch/select body. Each
// clause starts from the state at the construct, not from a sibling clause's state.
func walkClauses(body *ast.BlockStmt, cur lockState, walk func([]ast.Stmt, lockState) lockState) lockState {
	if body == nil {
		return cur
	}
	out := cur
	for _, c := range body.List {
		switch cl := c.(type) {
		case *ast.CommClause:
			out = joinLockState(out, walk(cl.Body, cur))
		case *ast.CaseClause:
			out = joinLockState(out, walk(cl.Body, cur))
		}
	}
	return out
}

// The re-review asked for same-family coverage rather than three point fixes: the two
// walks now FAIL CLOSED on constructs they do not model, so each family needs a negative
// control (the bypass must be rejected) AND a positive control (correct code must stay
// green, or the gate teaches people to delete it).
//
// origin: remote-fs stale-health external re-review RR-F1, RR-F2
func TestSpawnStallEvidenceGateControlFlowFamilies(t *testing.T) {
	parse := func(t *testing.T, src string) *ast.FuncDecl {
		t.Helper()
		file, err := parser.ParseFile(token.NewFileSet(), "sample.go", "package sample\n"+src, 0)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return file.Decls[0].(*ast.FuncDecl)
	}

	t.Run("mint wiring", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			src      string
			wantWire bool
		}{
			{"plain sequential invalidation is wired", `
func (p *Policy) f() error {
	select {
	case <-time.After(timeout):
		p.invalidateHealthy()
		return ErrSpawnTimeout
	}
}`, true},
			{"both if branches invalidate", `
func (p *Policy) f(x bool) error {
	select {
	case <-time.After(timeout):
		if x {
			p.invalidateHealthy()
		} else {
			p.invalidateHealthy()
		}
		return ErrSpawnTimeout
	}
}`, true},
			{"only one branch invalidates", `
func (p *Policy) f(x bool) error {
	select {
	case <-time.After(timeout):
		if x {
			p.invalidateHealthy()
		}
		return ErrSpawnTimeout
	}
}`, false},
			{"go statement is async", `
func (p *Policy) f() error {
	select {
	case <-time.After(timeout):
		go p.invalidateHealthy()
		return ErrSpawnTimeout
	}
}`, false},
			{"defer runs too late", `
func (p *Policy) f() error {
	select {
	case <-time.After(timeout):
		defer p.invalidateHealthy()
		return ErrSpawnTimeout
	}
}`, false},
			{"goto may skip the invalidation", `
func (p *Policy) f(skip bool) error {
	select {
	case <-time.After(timeout):
		if skip {
			goto done
		}
		p.invalidateHealthy()
	done:
		return ErrSpawnTimeout
	}
}`, false},
			{"loop body may run zero times", `
func (p *Policy) f(n int) error {
	select {
	case <-time.After(timeout):
		for i := 0; i < n; i++ {
			p.invalidateHealthy()
		}
		return ErrSpawnTimeout
	}
}`, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				sites := mintSitesIn(parse(t, tc.src), "sample.go")
				if len(sites) != 1 {
					t.Fatalf("found %d mint sites, want 1", len(sites))
				}
				if sites[0].invalidated != tc.wantWire {
					t.Errorf("invalidated=%v, want %v", sites[0].invalidated, tc.wantWire)
				}
			})
		}
	})

	t.Run("lock domination", func(t *testing.T) {
		for _, tc := range []struct {
			name          string
			src           string
			wantViolation bool
		}{
			{"unlocked call is fine", `
func (p *Policy) f() {
	p.invalidateHealthy()
}`, false},
			{"unlock before the call is fine", `
func (p *Policy) f() {
	p.mu.Lock()
	p.mu.Unlock()
	p.invalidateHealthy()
}`, false},
			{"plain locked call", `
func (p *Policy) f() {
	p.mu.Lock()
	p.invalidateHealthy()
}`, true},
			{"lock in if initializer", `
func (p *Policy) f(ok bool) {
	if p.mu.Lock(); ok {
	}
	p.invalidateHealthy()
}`, true},
			{"lock in for initializer", `
func (p *Policy) f() {
	for p.mu.Lock(); false; {
	}
	p.invalidateHealthy()
}`, true},
			{"lock in switch initializer", `
func (p *Policy) f(x int) {
	switch p.mu.Lock(); x {
	}
	p.invalidateHealthy()
}`, true},
			{"conditional unlock leaves it unknown", `
func (p *Policy) f(unlock bool) {
	p.mu.Lock()
	if unlock {
		p.mu.Unlock()
	}
	p.invalidateHealthy()
}`, true},
			{"deferred unlock still holds it", `
func (p *Policy) f() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invalidateHealthy()
}`, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got := lockedInvalidateIn(parse(t, tc.src), map[string]bool{invalidateFn: true})
				if got != tc.wantViolation {
					t.Errorf("violation=%v, want %v", got, tc.wantViolation)
				}
			})
		}
	})
}
