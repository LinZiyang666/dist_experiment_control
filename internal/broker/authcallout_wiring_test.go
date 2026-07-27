package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// authcallout_wiring_test.go (batch B, B3) — the three clustered auth_callout assignments must stay
// in ONE block.
//
// authcallout.Handler.ClusterMode makes the PIN-bootstrap seam-nil fallbacks fail closed
// (internal/authcallout/seam_failclosed_test.go proves the refusal). But that whole mechanism is
// worth nothing if ClusterMode is not SET, and it is set in exactly one place: the
// `if b.clusterMode { … }` block in installAuthCallout, next to the two seams it guards.
//
// The failure this pins is a silent one in the most damaging direction. Move ClusterMode out of the
// block (or add a third write seam inside a different `if`) and:
//
//   - internal/authcallout's tests stay green — they set ClusterMode by hand;
//   - the clustered e2e matrices stay green — the seams are still wired, so nothing falls back;
//   - and the fail-closed guard is now dead code that will never fire, so the day someone DOES
//     forget a seam, the read-only FSM handle gets written on the authentication path again.
//
// Nothing behavioural can catch that: the guard's whole purpose is to fire in a state no test
// deliberately constructs. So this is a structural check, and testing-standards §S1 applies — it
// does not substitute for the behavioural tests in internal/authcallout, it covers what they
// cannot see.

// clusteredHandlerFields are the Handler fields installAuthCallout must set together. Every one of
// them is meaningless — or dangerous — without the others:
//
//	ProvisionAgentWrite / JoinMemberWrite : route the write through raft
//	ClusterMode                           : refuse if either of the above is missing
//
// LeaderContactStale is deliberately NOT required here. It is a read-path fence (ErrFenced), not
// part of the write-seam contract, and pinning it would make this guard fire on an unrelated change.
var clusteredHandlerFields = []string{
	"ProvisionAgentWrite",
	"JoinMemberWrite",
	"ClusterMode",
}

// TestClusterModeIsWiredBesideTheSeams walks installAuthCallout and requires all three assignments
// to live in the body of the same `if b.clusterMode` statement.
func TestClusterModeIsWiredBesideTheSeams(t *testing.T) {
	fn := findFuncDecl(t, "authcallout.go", "installAuthCallout")

	blocks := clusterModeGuardedBlocks(fn)
	if len(blocks) == 0 {
		t.Fatal("installAuthCallout has no `if b.clusterMode { … }` block. Either the clustered " +
			"wiring moved somewhere this guard cannot see, or the handler is now wired the same " +
			"way in single and cluster mode — in which case the seam-nil fallback writes the " +
			"read-only FSM handle again.")
	}
	if len(blocks) > 1 {
		t.Fatalf("installAuthCallout has %d separate `if b.clusterMode` blocks. Split wiring is "+
			"exactly the shape this guard exists to prevent: one block can be moved, deleted or "+
			"made conditional without the other noticing.", len(blocks))
	}

	assigned := handlerFieldsAssignedIn(blocks[0])
	for _, want := range clusteredHandlerFields {
		if !assigned[want] {
			t.Errorf("`h.%s` is not assigned inside the `if b.clusterMode` block. All of %v must "+
				"be set together: ClusterMode is what turns a MISSING seam into a named refusal, "+
				"so a seam wired without it (or it without a seam) reintroduces the direct write "+
				"to the read-only FSM handle on the authentication path.", want, clusteredHandlerFields)
		}
	}
}

// TestNoClusterSeamIsAssignedOutsideTheGuard catches the other half: a seam assigned
// UNCONDITIONALLY would make single-mode brokers route PIN writes through a nil cluster node.
// racknerd is single-broker, so that is a fleet-wide auth outage.
func TestNoClusterSeamIsAssignedOutsideTheGuard(t *testing.T) {
	fn := findFuncDecl(t, "authcallout.go", "installAuthCallout")
	blocks := clusterModeGuardedBlocks(fn)
	if len(blocks) != 1 {
		t.Skip("covered by TestClusterModeIsWiredBesideTheSeams") // keep one failure, not two
	}
	guarded := blocks[0]

	var outside []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if n == guarded {
			return false // skip the guarded block itself
		}
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, lhs := range as.Lhs {
			if name, ok := handlerFieldName(lhs); ok {
				for _, f := range clusteredHandlerFields {
					if name == f {
						outside = append(outside, name)
					}
				}
			}
		}
		return true
	})
	if len(outside) > 0 {
		t.Errorf("%v assigned OUTSIDE the `if b.clusterMode` block. A cluster seam set "+
			"unconditionally sends every single-broker deployment's PIN writes through a nil "+
			"cluster node, and ClusterMode set unconditionally makes single-mode refuse PIN "+
			"bootstrap outright — either one is a fleet-wide auth outage.", outside)
	}
}

// TestWiringGuardIsNotVacuous proves the AST walk actually resolves the assignments it claims to
// check. Both tests above are satisfied by a `guarded` block that the walker simply failed to
// recognise (it would report zero fields, but so would a renamed field), so this asserts the
// positive: the walk finds a known-present assignment, and rejects a field name that is not there.
func TestWiringGuardIsNotVacuous(t *testing.T) {
	fn := findFuncDecl(t, "authcallout.go", "installAuthCallout")
	blocks := clusterModeGuardedBlocks(fn)
	if len(blocks) != 1 {
		t.Fatalf("expected exactly 1 guarded block, got %d", len(blocks))
	}
	assigned := handlerFieldsAssignedIn(blocks[0])
	if len(assigned) < len(clusteredHandlerFields) {
		t.Fatalf("the walker resolved only %d field assignments (%v) in a block that must contain "+
			"at least %d — it is not reading the assignments, so the checks above pass vacuously",
			len(assigned), assigned, len(clusteredHandlerFields))
	}
	if assigned["NoSuchFieldOnHandler"] {
		t.Fatal("the walker reports a field that does not exist — it is matching something other " +
			"than the assignment's selector name")
	}
	// And the `h` in `h.ClusterMode` must really be the authcallout.Handler this function builds,
	// not some other local. Pin the composite literal's type so a future rename cannot leave the
	// walker matching a different struct's fields.
	if !buildsAuthcalloutHandler(fn) {
		t.Fatal("installAuthCallout no longer constructs an &authcallout.Handler{…}, so the " +
			"`h.<field>` assignments this file inspects may belong to a different type entirely")
	}
}

// adminBackendClusterFields are the adminsock.Backend fields the broker must set together inside
// Run's `if b.clusterMode`. Same contract as clusteredHandlerFields, second package:
//
//	EvictWrite  : routes `admin evict` deletes through raft
//	ClusterMode : refuses if EvictWrite is missing, instead of doing the direct tx
//
// Cluster (the D7 admin orchestrator) is deliberately NOT listed. It is set in the same block, but
// it gates a different family of verbs and is nil-tolerant by design ("cluster mode not enabled"),
// so requiring it here would couple this guard to an unrelated decision.
var adminBackendClusterFields = []string{
	"EvictWrite",
	"ClusterMode",
}

// TestAdminBackendClusterModeIsWiredBesideTheSeam is the adminsock twin of the test above. The
// stakes are the same and so is the invisibility: wire EvictWrite without ClusterMode and every
// clustered evict still works (the seam is there), so nothing fails until the day a seam is missed —
// at which point the direct tx deletes agent_provisioning/nodes rows outside raft.
func TestAdminBackendClusterModeIsWiredBesideTheSeam(t *testing.T) {
	fn := findFuncDecl(t, "broker.go", "Run")

	blocks := clusterModeGuardedBlocks(fn)
	if len(blocks) == 0 {
		t.Fatal("Run has no `if b.clusterMode { … }` block at all — the clustered adminsock " +
			"wiring moved somewhere this guard cannot see")
	}

	// Run has several `if b.clusterMode` blocks (the admin socket is one of many clustered
	// subsystems), so locate the one that wires the adminsock Backend rather than assuming there is
	// exactly one. Identify it by the seam itself.
	var target *ast.BlockStmt
	for _, blk := range blocks {
		if backendFieldsAssignedIn(blk)["EvictWrite"] {
			if target != nil {
				t.Fatal("`backend.EvictWrite` is assigned in more than one `if b.clusterMode` " +
					"block; split wiring is the shape this guard exists to prevent")
			}
			target = blk
		}
	}
	if target == nil {
		t.Fatal("no `if b.clusterMode` block assigns `backend.EvictWrite`. Either the evict seam " +
			"is now wired unconditionally — which sends single-broker evicts through a nil cluster " +
			"node — or it moved out of Run and this guard no longer covers it.")
	}

	assigned := backendFieldsAssignedIn(target)
	for _, want := range adminBackendClusterFields {
		if !assigned[want] {
			t.Errorf("`backend.%s` is not assigned in the same `if b.clusterMode` block as the "+
				"others (%v). ClusterMode is what turns a MISSING EvictWrite into a refusal; "+
				"without it a clustered broker silently falls back to the direct tx on the "+
				"read-only FSM handle.", want, adminBackendClusterFields)
		}
	}
}

// TestAdminWiringGuardIsNotVacuous mirrors TestWiringGuardIsNotVacuous: prove the walker resolves
// real assignments in a real block, and that the local it is reading is the adminsock Backend.
func TestAdminWiringGuardIsNotVacuous(t *testing.T) {
	fn := findFuncDecl(t, "broker.go", "Run")
	if !buildsCompositeLit(fn, "adminsock", "Backend") {
		t.Fatal("Run no longer constructs an adminsock.Backend{…}, so the `backend.<field>` " +
			"assignments this guard inspects may belong to a different type")
	}
	total := map[string]bool{}
	for _, blk := range clusterModeGuardedBlocks(fn) {
		for f := range backendFieldsAssignedIn(blk) {
			total[f] = true
		}
	}
	if len(total) < len(adminBackendClusterFields) {
		t.Fatalf("the walker resolved only %d `backend.<field>` assignments (%v) across every "+
			"clustered block — it is not reading the assignments, so the check above passes "+
			"vacuously", len(total), total)
	}
	if total["NoSuchFieldOnBackend"] {
		t.Fatal("the walker reports a field that does not exist")
	}
}

// --- AST helpers -----------------------------------------------------------------------------

func findFuncDecl(t *testing.T, file, name string) *ast.FuncDecl {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name && fd.Body != nil {
			return fd
		}
	}
	t.Fatalf("%s: no func %s with a body — it was renamed or moved, and every assertion in this "+
		"file silently stops covering the clustered auth_callout wiring", file, name)
	return nil
}

// clusterModeGuardedBlocks returns the bodies of every `if b.clusterMode { … }` in fn (no else
// branch expected; an `if !b.clusterMode` is deliberately NOT matched — inverted wiring is a
// different shape and should trip the "no guarded block" failure loudly).
func clusterModeGuardedBlocks(fn *ast.FuncDecl) []*ast.BlockStmt {
	var out []*ast.BlockStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		sel, ok := is.Cond.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "clusterMode" {
			return true
		}
		if ident, ok := sel.X.(*ast.Ident); !ok || ident.Name != "b" {
			return true
		}
		out = append(out, is.Body)
		return true
	})
	return out
}

// handlerFieldsAssignedIn collects the `h.<Field>` selector names assigned anywhere in blk.
func handlerFieldsAssignedIn(blk *ast.BlockStmt) map[string]bool {
	return fieldsAssignedIn(blk, "h")
}

// backendFieldsAssignedIn collects the `backend.<Field>` selector names assigned anywhere in blk.
func backendFieldsAssignedIn(blk *ast.BlockStmt) map[string]bool {
	return fieldsAssignedIn(blk, "backend")
}

// fieldsAssignedIn collects the `<recv>.<Field>` selector names assigned anywhere in blk.
func fieldsAssignedIn(blk *ast.BlockStmt, recv string) map[string]bool {
	got := map[string]bool{}
	ast.Inspect(blk, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, lhs := range as.Lhs {
			if name, ok := selectorFieldName(lhs, recv); ok {
				got[name] = true
			}
		}
		return true
	})
	return got
}

// handlerFieldName reports the field name of an `h.<Field>` selector expression.
func handlerFieldName(e ast.Expr) (string, bool) { return selectorFieldName(e, "h") }

// selectorFieldName reports the field name of a `<recv>.<Field>` selector expression.
func selectorFieldName(e ast.Expr, recv string) (string, bool) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok || ident.Name != recv {
		return "", false
	}
	return sel.Sel.Name, true
}

// buildsAuthcalloutHandler reports whether fn contains an &authcallout.Handler{…} literal.
func buildsAuthcalloutHandler(fn *ast.FuncDecl) bool {
	return buildsCompositeLit(fn, "authcallout", "Handler")
}

// buildsCompositeLit reports whether fn contains a `<pkg>.<name>{…}` composite literal.
func buildsCompositeLit(fn *ast.FuncDecl, pkg, name string) bool {
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		x, ok := sel.X.(*ast.Ident)
		if ok && x.Name == pkg && sel.Sel.Name == name {
			found = true
			return false
		}
		return true
	})
	return found
}
