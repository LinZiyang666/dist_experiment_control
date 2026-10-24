package proto

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

// Every operation is a named decision at the wire/controller boundary. A
// default refusal remains safe for future bytes but cannot silently absorb an
// existing broker-only verb or a terminal phase after a source move.
// origin: hot-update S7 plan §6.2 / §11.4 / INTEG-D2 closed dispatch
func TestTransferDispatchNamesEveryClosedMember(t *testing.T) {
	operations := []string{"OpProbe", "OpPermit", "OpPrepare", "OpStatus", "OpSource", "OpReserve", "OpCommit", "OpTerminal", "OpCancel", "OpFinalize", "OpCensus", "OpBucketConfig"}
	phases := []string{"PhaseOpen", "PhasePrepared", "PhaseSending", "PhaseReceiving", "PhaseCommitReserved", "PhaseDecided", "PhaseTerminalPending", "PhaseHistoryConfirmed", "PhaseCleanupPending", "PhaseSettled"}
	for _, tc := range []struct {
		path, name string
		members    []string
	}{
		{"transfer_codec.go", "DecodeTransferResponse", operations},
		{"transfer_codec.go", "EncodeTransferRequest", operations},
		{"transfer_codec.go", "DecodeTransferRequest", operations},
		{"transfer_codec.go", "EncodeTransferResponse", operations},
		{"../xfercontrol/controller.go", "continueTransfer", operations},
		{"../xfercontrol/controller.go", "livePhase", phases},
		{"../xfercontrol/routing.go", "DecodeWire", operations},
		{"../xfercontrol/routing.go", "WireResponse", operations},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Clean(tc.path))
			if err != nil {
				t.Fatal(err)
			}
			f, err := parser.ParseFile(token.NewFileSet(), tc.path, source, 0)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			found := false
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != tc.name {
					continue
				}
				found = true
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					clause, ok := n.(*ast.CaseClause)
					if !ok {
						return true
					}
					for _, expr := range clause.List {
						if sel, ok := expr.(*ast.SelectorExpr); ok {
							if name, ok := sel.X.(*ast.Ident); ok && name.Name == "x" {
								seen[sel.Sel.Name] = true
							}
						}
					}
					return true
				})
			}
			if !found {
				t.Fatal("dispatch root moved without its guard")
			}
			for _, member := range tc.members {
				if !seen[member] {
					t.Errorf("%s omitted %s", tc.name, member)
				}
			}
		})
	}
}
