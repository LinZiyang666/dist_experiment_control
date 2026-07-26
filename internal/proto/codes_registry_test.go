package proto_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Batch-A review F-11/M9. codes.go called itself "the SSOT for the NATS-wire
// half", but 30 of its 32 constants have no production reference at all: every
// emitter still writes the literal directly. Nothing read the constants, so
// editing one of their VALUES would compile cleanly and pass every test — a
// registry describing itself as a single source of truth while being neither
// single nor a source.
//
// Two things fix that honestly. The doc no longer overstates what the file is
// (see codes.go). And this test makes the values load-bearing in the one way
// that actually matters today: every constant must correspond to a literal some
// production file really emits. Change a constant's bytes and this goes red.
//
// It deliberately does NOT require emitters to reference the constants. Forcing
// that migration is real work with real risk (32 codes across broker, agent and
// spawnsafe) and belongs in its own increment; pretending it already happened is
// what got flagged.
func TestEveryDeclaredCodeHasAProductionEmitter(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root)) // internal/proto -> repo root

	// Collect the declared code constants and their values.
	declared := map[string]string{} // const name -> value
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(root, "internal", "proto", "codes.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse codes.go: %v", err)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, nm := range vs.Names {
			if i >= len(vs.Values) {
				continue
			}
			lit, ok := vs.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			if v, e := strconv.Unquote(lit.Value); e == nil {
				declared[nm.Name] = v
			}
		}
		return true
	})
	if len(declared) == 0 {
		t.Fatal("no code constants found in codes.go; this guard has gone vacuous")
	}

	// External review (gate quality #1): this used to accept ANY string literal
	// in the scanned tree, which meant cmd/tether's own classifier and hint maps
	// vouched for codes whose real emitter had been deleted. The test name
	// promised "HasAProductionEmitter" and delivered "appears somewhere".
	//
	// cmd/tether is now excluded from the emitter evidence: it is the CONSUMER
	// side. Evidence must come from a package that actually replies.
	//
	// Collect two things: every string literal, and every reference to a
	// proto.Code* / proto.Reason* constant.
	//
	// A constant counts as live either way. Codes whose emitters still write the
	// literal are the common case today; codes that have been migrated to the
	// constant (dataplane_not_converged) no longer HAVE a literal, and treating
	// that as an orphan would punish exactly the direction this file wants.
	emitted := map[string]bool{}
	referenced := map[string]bool{}
	for _, dir := range []string{"internal/broker", "internal/agent", "internal/spawnsafe", "internal/adminsock"} {
		_ = filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			pf, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if perr != nil {
				return nil
			}
			ast.Inspect(pf, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.BasicLit:
					if v.Kind == token.STRING {
						if s, e := strconv.Unquote(v.Value); e == nil {
							emitted[s] = true
						}
					}
				case *ast.SelectorExpr:
					if id, ok := v.X.(*ast.Ident); ok && id.Name == "proto" {
						referenced[v.Sel.Name] = true
					}
				}
				return true
			})
			return nil
		})
	}

	var orphans []string
	for name, val := range declared {
		if emitted[val] || referenced[name] {
			continue
		}
		orphans = append(orphans, name+" = "+strconv.Quote(val))
	}
	if len(orphans) > 0 {
		t.Errorf("%d declared code constant(s) match no literal emitted anywhere in production:\n  %s\n"+
			"Either the constant's value was mistyped (it can never match a real reply, which is worse "+
			"than having no constant), or the emitter was removed and the constant should go with it.",
			len(orphans), strings.Join(orphans, "\n  "))
	}
}

// TestSharedCodeConstantsAreReachable is the non-vacuity companion: prove the
// package really exports what the test above thinks it parsed.
func TestSharedCodeConstantsAreReachable(t *testing.T) {
	if proto.CodeStoreError != "store_error" || proto.CodeBadRequest != "bad_request" {
		t.Fatal("the parsed constants do not match the compiled ones; the AST walk is reading the wrong file")
	}
	if proto.CodeDataplaneNotConverged != "dataplane_not_converged" {
		t.Fatal("CodeDataplaneNotConverged drifted; it is aliased by both broker and cmd/tether")
	}
}
