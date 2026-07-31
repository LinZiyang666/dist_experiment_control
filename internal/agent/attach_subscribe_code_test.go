package agent

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// attach_subscribe_code_test.go — attach_subscribe_failed must stay attached to the SUBSCRIBE failure.
//
// origin: line-2 external review M18 / PC-3, which found the four Y2 codes had no emitting-side test.
// Three of the four are covered behaviourally in upgrade_download_errors_test.go. This one is not
// reachable that way: emitting it requires a PTY that allocates successfully AND a NATS connection whose
// SubscribeSync then fails, and the reply travels back over that same broken connection, so there is
// nothing to observe. Faking it would mean faking the very thing under test.
//
// What IS assertable, and is the defect Y2 actually fixed, is WHERE the code is emitted. Before Y2 this
// branch reported pty_alloc_failed — a lie about which thing failed, and a lie with a retry policy
// attached: the PTY had allocated fine (the code closes the session on the way out), so a host that can
// NEVER allocate a PTY got the same retry-forever treatment as a NATS hiccup that clears on its own.
//
// The regression to guard against is someone folding this branch back into the allocate error path, or
// deleting the sess.Close() that proves allocation had succeeded. Both are positional, so the test is
// positional. It fails if the shape stops holding, which is more than the wire-code coverage gate can
// say: that gate only checks the literal exists somewhere in the package.

func TestAttachSubscribeFailureIsItsOwnCode(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "run.go", nil, 0)
	if err != nil {
		t.Fatalf("parse run.go: %v", err)
	}

	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if ok && fd.Name.Name == "handleRunForwarded" {
			fn = fd
		}
	}
	if fn == nil {
		t.Fatal("handleRunForwarded is gone from run.go — this gate names the wrong function now")
	}

	// Collect the positions of the four landmarks, by walking the function body in source order.
	var allocLine, subscribeLine, closeAfterSubLine, codeLine int
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		// ast.Inspect calls back with nil when it leaves a node.
		if n == nil {
			return false
		}
		line := fset.Position(n.Pos()).Line
		switch node := n.(type) {
		case *ast.CallExpr:
			switch callee := node.Fun.(type) {
			case *ast.SelectorExpr:
				switch callee.Sel.Name {
				case "Allocate":
					if allocLine == 0 {
						allocLine = line
					}
				case "SubscribeSync":
					if subscribeLine == 0 {
						subscribeLine = line
					}
				case "Close":
					// The FIRST Close after the SubscribeSync call is the one that proves the PTY had
					// already been allocated when this branch was taken.
					if subscribeLine != 0 && closeAfterSubLine == 0 && line > subscribeLine {
						closeAfterSubLine = line
					}
				}
			}
		case *ast.BasicLit:
			if node.Kind == token.STRING && strings.Contains(node.Value, "attach_subscribe_failed") {
				if codeLine == 0 {
					codeLine = line
				}
			}
		}
		return true
	})

	if allocLine == 0 {
		t.Fatal("no pty.Allocate call found in handleRunForwarded — the landmarks this gate reasons about " +
			"are gone, so its verdict would be meaningless")
	}
	if subscribeLine == 0 {
		t.Fatal("no SubscribeSync call found in handleRunForwarded")
	}
	if codeLine == 0 {
		t.Fatal("handleRunForwarded no longer contains the literal \"attach_subscribe_failed\". If the " +
			"attach-subscribe failure moved elsewhere, move this gate with it; if it was folded back into " +
			"another code, that is the Y2 regression — a NATS hiccup and a host with no /dev/ptmx would " +
			"once again be indistinguishable to a monitor.")
	}

	if codeLine <= allocLine {
		t.Errorf("attach_subscribe_failed is emitted at line %d, at or BEFORE pty.Allocate at line %d — it "+
			"is back in the allocation failure path. That is exactly the pre-Y2 defect: reporting an "+
			"allocation failure for a subscription failure, which attaches the wrong retry semantics.",
			codeLine, allocLine)
	}
	if codeLine <= subscribeLine {
		t.Errorf("attach_subscribe_failed is emitted at line %d, before the SubscribeSync at line %d — it "+
			"cannot be describing that call's failure", codeLine, subscribeLine)
	}
	if closeAfterSubLine == 0 || closeAfterSubLine > codeLine {
		t.Errorf("the attach_subscribe_failed branch does not close the PTY session before replying "+
			"(Close after SubscribeSync: line %d, code emitted: line %d).\n\n"+
			"That Close is what makes the code honest: the session EXISTS, so allocation succeeded, so the "+
			"failure is the subscription. Dropping it also leaks a PTY per occurrence.",
			closeAfterSubLine, codeLine)
	}
}

// TestPTYBranchActuallyConsultsTheTransientPredicate closes M18's real gap.
//
// origin: line-2 closure verification. This file's sibling (upgrade_download_errors_test.go) tests
// ptyFailureIsTransient thoroughly — but it calls the predicate DIRECTLY, so it says nothing about whether
// handleRunForwarded still calls it. The header of that file claimed the review's "mutation B" (severing
// the branch at the call site with `if false && …`) reddens those tests. It does not: the predicate keeps
// answering correctly while the branch that consults it goes dead, and every package stays green. The claim
// was wrong and this is the assertion that makes an equivalent claim true.
//
// Positional rather than behavioural for the same reason as the gate above: reaching the emit requires a
// PTY that allocates and a NATS conn that then fails, and the reply travels back over that conn. What is
// checkable is that the condition guarding pty_alloc_failed is a call to the predicate and nothing else —
// so `if false && ptyFailureIsTransient(err)`, `if someOtherThing(err)`, or an inlined errno comparison all
// fail here.
func TestPTYBranchActuallyConsultsTheTransientPredicate(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "run.go", nil, 0)
	if err != nil {
		t.Fatalf("parse run.go: %v", err)
	}

	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "handleRunForwarded" {
			fn = fd
		}
	}
	if fn == nil {
		t.Fatal("handleRunForwarded is gone from run.go — this gate names the wrong function now")
	}

	// Find the INNERMOST `if` whose body emits pty_alloc_failed. Innermost matters: the outer
	// `if err != nil` from pty.Allocate encloses the whole failure block, so a naive first-match walk
	// inspects that one's condition (`err != nil`) and reports a false positive. The deepest enclosing `if`
	// is the branch that actually decides between the two codes.
	var target *ast.IfStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if n == nil {
			return false
		}
		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		emitsAllocFailed := false
		ast.Inspect(ifStmt.Body, func(inner ast.Node) bool {
			if lit, ok := inner.(*ast.BasicLit); ok && lit.Kind == token.STRING &&
				strings.Contains(lit.Value, "pty_alloc_failed") {
				emitsAllocFailed = true
			}
			return true
		})
		if emitsAllocFailed && (target == nil || ifStmt.Pos() > target.Pos()) {
			target = ifStmt
		}
		return true
	})

	found := target != nil
	if found {
		ifStmt := target
		call, ok := ifStmt.Cond.(*ast.CallExpr)
		if !ok {
			t.Errorf("the branch emitting pty_alloc_failed is guarded by a %T, not by a bare call to "+
				"ptyFailureIsTransient.\n\n"+
				"That predicate is where the errno criterion lives and where its tests point. Any other "+
				"condition — an inlined errors.Is chain, an added `&&`, a different helper — means the "+
				"tested criterion and the shipped criterion are two different things, which is the state "+
				"review mutation B produced with every package green.", ifStmt.Cond)
		} else if ident, isIdent := call.Fun.(*ast.Ident); !isIdent || ident.Name != "ptyFailureIsTransient" {
			t.Errorf("the pty_alloc_failed branch calls %v, not ptyFailureIsTransient — see above", call.Fun)
		}
	}
	if !found {
		t.Fatal("no `if` in handleRunForwarded emits pty_alloc_failed, so this gate asserted nothing. " +
			"Either the code moved (move the gate with it) or the transient PTY branch is gone, which " +
			"would mean fd/pty exhaustion now reports the terminal code and tells automation to stop " +
			"retrying something that clears in seconds.")
	}
}
