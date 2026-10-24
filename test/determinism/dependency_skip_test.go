package determinism

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// A missing product implementation or live receipt is SETUP-RED. testing.Skip
// exits successfully, so a NOT_RUN skip can silently certify an unexercised
// contract in both the ordinary suite and the parallel matrix. Scan source,
// including tagged tests and Go helpers, rather than the host's build subset.
// gate-control: TestDependencySkipScannerSeesTheShapes
// origin: S10 internal review NO-SKIP-AS-PASS F3
var missingDependencyMarker = regexp.MustCompile(`(?i)\bNOT_RUN\b|\bpending\s+(?:S[0-9]+|P[0-9]+|dependency)\b`)

// dependencySkipSites uses the same predicate for the repository and controls.
// Comments on the call or up to three preceding lines cover SkipNow and dynamic
// errors; distant comments cannot accidentally classify an unrelated platform
// skip. String bindings cover messages factored out of the call, and parsing
// keeps synthetic Go snippets inside string literals outside the scan.
func dependencySkipSites(name string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	bindings := make(map[string][]ast.Expr)
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ValueSpec:
			for i, ident := range n.Names {
				if i < len(n.Values) {
					bindings[ident.Name] = append(bindings[ident.Name], n.Values[i])
				}
			}
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok && i < len(n.Rhs) {
					bindings[ident.Name] = append(bindings[ident.Name], n.Rhs[i])
				}
			}
		}
		return true
	})
	var messageText func(ast.Expr, map[string]bool) string
	messageText = func(expr ast.Expr, visiting map[string]bool) string {
		switch value := expr.(type) {
		case *ast.BasicLit:
			if value.Kind == token.STRING {
				text, _ := strconv.Unquote(value.Value)
				return text
			}
		case *ast.ParenExpr:
			return messageText(value.X, visiting)
		case *ast.BinaryExpr:
			if value.Op == token.ADD {
				return messageText(value.X, visiting) + messageText(value.Y, visiting)
			}
		case *ast.Ident:
			if !visiting[value.Name] {
				visiting[value.Name] = true
				var out strings.Builder
				for _, binding := range bindings[value.Name] {
					out.WriteString(messageText(binding, visiting))
				}
				delete(visiting, value.Name)
				return out.String()
			}
		case *ast.CallExpr:
			// Formatting can wrap a literal message. Do not chase call argument
			// bindings: they include unrelated error/data graphs in large files.
			var out strings.Builder
			for _, arg := range value.Args {
				if _, literal := arg.(*ast.BasicLit); literal {
					out.WriteString(messageText(arg, visiting))
				}
			}
			return out.String()
		}
		return ""
	}
	var sites []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "Skip" && selector.Sel.Name != "Skipf" && selector.Sel.Name != "SkipNow") {
			return true
		}
		line, end := fset.Position(call.Pos()).Line, fset.Position(call.End()).Line
		var evidence strings.Builder
		for _, arg := range call.Args {
			evidence.WriteString(messageText(arg, make(map[string]bool)))
			evidence.WriteByte('\n')
		}
		for _, group := range file.Comments {
			for _, comment := range group.List {
				first, last := fset.Position(comment.Pos()).Line, fset.Position(comment.End()).Line
				if first <= end && last >= line-3 {
					evidence.WriteString(comment.Text)
					evidence.WriteByte('\n')
				}
			}
		}
		if missingDependencyMarker.MatchString(evidence.String()) {
			sites = append(sites, fmt.Sprintf("%s:%d: %s hides missing evidence; use t.Fatal(\"SETUP-RED: NOT_RUN pending <owner>: ...\")", name, line, selector.Sel.Name))
		}
		return true
	})
	return sites, nil
}

func collectDependencySkipSites(root string) ([]string, int, error) {
	var sites []string
	files := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		// An identifier must contain these bytes even with arbitrary whitespace
		// around its selector. Avoid parsing thousands of unrelated Go files.
		if bytes.Contains(src, []byte("Skip")) {
			found, err := dependencySkipSites(filepath.ToSlash(rel), src)
			if err != nil {
				return err
			}
			sites = append(sites, found...)
		}
		files++
		return nil
	})
	return sites, files, err
}

// origin: S10 internal review NO-SKIP-AS-PASS F3
func TestMissingDependencyNeverSkips(t *testing.T) {
	sites, files, err := collectDependencySkipSites(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	// Count scanned files, not violations: a correct tree has zero bad skips.
	if files < 100 {
		t.Fatalf("dependency skip scan saw only %d Go files", files)
	}
	if len(sites) != 0 {
		t.Fatalf("missing dependency cannot pass by skipping:\n%s", strings.Join(sites, "\n"))
	}
}

// origin: S10 internal review NO-SKIP-AS-PASS F3
func TestDependencySkipScannerSeesTheShapes(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"not run", `t.Skip("NOT_RUN pending S10 P00: real supply")`, 1},
		{"pending owner", `t.Skipf("pending P03: %v", err)`, 1},
		{"stage owner", `t.Skip("pending S7 P05/P06")`, 1},
		{"previous comment", "// NOT_RUN pending P05/P06\nif missing { t.SkipNow() }", 1},
		{"trailing comment", "t.SkipNow() // pending P01", 1},
		{"block comment", "/* NOT_RUN\n pending P26 */\nt.SkipNow()", 1},
		{"split message", `t.Skip("NOT_" + "RUN pending P03")`, 1},
		{"constant message", `const message = "NOT_RUN pending P01"; t.Skip(message)`, 1},
		{"variable message", `message := "pending S8 P01"; t.Skipf(message, err)`, 1},
		{"test helper receiver", `fixture.t.Skip("NOT_RUN pending P26")`, 1},
		{"platform", `if runtime.GOOS != "linux" { t.Skip("requires Linux") }`, 0},
		{"short", `if testing.Short() { t.Skip("slow in short mode") }`, 0},
		{"fatal", `t.Fatal("SETUP-RED: NOT_RUN pending P26: real supply")`, 0},
		{"inert snippet", "_ = `t.Skip(\"NOT_RUN pending P01\")`", 0},
		{"distant comment", "// pending P01\n\n\n\n\nt.Skip(\"requires Linux\")", 0},
		{"different word", `t.Skip("no pending request on this platform")`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := []byte("package fixture\nfunc exercise(t *testing.T) {\n" + tc.body + "\n}\n")
			sites, err := dependencySkipSites("fixture_test.go", src)
			if err != nil || len(sites) != tc.want {
				t.Fatalf("skip classification got %v, %v; want %d sites", sites, err, tc.want)
			}
		})
	}
	if _, err := dependencySkipSites("broken_test.go", []byte("package broken\nfunc")); err == nil {
		t.Fatal("malformed source was accepted")
	}
	// The walk must see tagged tests and helper files through the same scanner.
	root := t.TempDir()
	for _, name := range []string{"tagged_test.go", "helper.go"} {
		src := "//go:build absent_tag\n\npackage fixture\nfunc exercise() { t.Skip(\"NOT_RUN pending P01\") }\n"
		if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sites, files, err := collectDependencySkipSites(root)
	if err != nil || files != 2 || len(sites) != 2 {
		t.Fatalf("source walk lost tagged/helper coverage: files=%d sites=%v err=%v", files, sites, err)
	}
}
