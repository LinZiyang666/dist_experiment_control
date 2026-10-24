package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os/exec"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// The candidate/previous reader probe must never open the live Raft Bolt store
// (plan §3.1 / T06: "it must not import/open Node/SQLite/Bolt"). A cluster
// daemon always holds raft.db's bolt flock, so any open either times out (the
// probe then refuses every cluster reload) or, when the flock is momentarily
// free (pristine Boot.Finish probe of `.prev`), runs a read-write bolt
// transaction from a second process against the live Raft store.
// origin: hot-update S6 internal review rv-writer
func TestBrokerPreflightClusterProbeNeverOpensRaftStore(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker reader probes require Linux image support")
	}
	artifact := filepath.Join(testharness.GuardProcessDirectory(t, t.TempDir()), "reader-probe")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", artifact, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build reader probe: %v %s", err, output)
	}
	artifactBytes, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifactBytes)
	artifactSHA := hex.EncodeToString(digest[:])
	fixture := filepath.Join(t.TempDir(), "schema.db")
	db, err := storage.Open(fixture)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := storage.CanonicalStoreIdentity(fixture, "")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	stamp, err := storage.ReadStoreStamp(db, identity)
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatalf("schema fixture: %v / %v", err, closeErr)
	}
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseLiveStoreReaderProbe(t, artifact, artifactSHA, stamp.Schema) })
	}
	testharness.AssertNoGoroutineLeak(t, "live store reader probe", gb)
	testharness.AssertNoFDLeak(t, "live store reader probe", fb, 0)
}

func exerciseLiveStoreReaderProbe(t *testing.T, artifact, artifactSHA string, schema storage.SchemaStamp) {
	t.Helper()
	for _, held := range []bool{true, false} {
		name := "flock-free"
		if held {
			name = "live-daemon-holds-bolt"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "raft"), 0o700); err != nil {
				t.Fatal(err)
			}
			boltPath := filepath.Join(dir, "raft", "raft.db")
			live, err := raftboltdb.New(raftboltdb.Options{Path: boltPath})
			if err != nil {
				t.Fatal(err)
			}
			if !held {
				if err := live.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Cleanup(func() { _ = live.Close() })
			}
			old := time.Now().Add(-time.Hour)
			if err := os.Chtimes(boltPath, old, old); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(boltPath)
			if err != nil {
				t.Fatal(err)
			}
			absentDB := filepath.Join(dir, "absent.db")
			args := []string{"tether", "serve", "--cluster-data-dir", dir, "--db", absentDB}
			start := time.Now()
			probeErr := validateBrokerProbeConfig(nil, args)
			elapsed := time.Since(start)
			if probeErr != nil {
				t.Fatalf("pure cluster config probe refused: %v", probeErr)
			}
			input, err := json.Marshal(brokerimage.ProbeRequest{AllowedArgs: args, Schema: schema, RequiredDomains: []string{brokerimage.Domain}})
			if err != nil {
				t.Fatal(err)
			}
			// The production dispatcher must also complete while the parent holds Bolt.
			// CombinedOutput joins the actual process on both success and cancellation.
			process := exec.CommandContext(t.Context(), artifact, brokerimage.PreflightProbeCommand)
			process.Stdin = bytes.NewReader(input)
			output, err := process.CombinedOutput()
			if err != nil {
				t.Fatalf("real reader probe: %v %s", err, output)
			}
			var reply brokerimage.ProbeReply
			if err := json.Unmarshal(output, &reply); err != nil || reply.Proof.SHA256 != artifactSHA {
				t.Fatalf("reader omitted its actual artifact proof: %v %s", err, output)
			}
			if _, err := os.Stat(absentDB); !os.IsNotExist(err) {
				t.Fatalf("reader created SQLite: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, storage.StoreLockFile)); !os.IsNotExist(err) {
				t.Fatalf("reader acquired production lock: %v", err)
			}
			after, err := os.Stat(boltPath)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) || !after.ModTime().Equal(before.ModTime()) || before.Size() != after.Size() {
				t.Fatalf("reader probe wrote the live raft.db (mtime %v -> %v, probe err=%v)", before.ModTime(), after.ModTime(), probeErr)
			}
			if held && elapsed >= 300*time.Millisecond {
				t.Fatalf("reader probe blocked %v on the live daemon's bolt flock (err=%v)", elapsed, probeErr)
			}
		})
	}
}

// Trace the project call graph reached by the actual candidate/previous reader,
// including serve's RunE closure with config-check enabled. A gate limited to
// brokerimage/preflight.go misses helpers which reopen SQLite/Bolt transitively.
// Runtime mtime/flock witnesses above complement this conservative source gate.
// origin: hot-update S6 plan §3.1 / T06; internal review rv-writer-5
func TestBrokerPreflightCallGraphHasNoStoreOpener(t *testing.T) {
	gate := preflightSourceGate{t: t, root: filepath.Join("..", ".."), seen: make(map[string]bool), packages: make(map[string][]preflightFunction)}
	gate.visit("cmd/tether", "validateBrokerProbeConfig")
	gate.visit("cmd/tether", "newBrokerImagePreflightCmd")
	gate.visit("internal/brokerimage", "Preflight")
	gate.visit("internal/brokerimage", "ReaderProbe")
}

type preflightFunction struct {
	body    *ast.BlockStmt
	imports map[string]string
	name    string
}

type preflightSourceGate struct {
	t        *testing.T
	root     string
	seen     map[string]bool
	packages map[string][]preflightFunction
}

func (g *preflightSourceGate) visit(pkg, name string) {
	key := pkg + "." + name
	if g.seen[key] {
		return
	}
	g.seen[key] = true
	functions, loaded := g.packages[pkg]
	if !loaded {
		paths, err := filepath.Glob(filepath.Join(g.root, pkg, "*.go"))
		if err != nil {
			g.t.Fatal(err)
		}
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			tree, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				g.t.Fatal(err)
			}
			imports := make(map[string]string)
			for _, spec := range tree.Imports {
				path, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					g.t.Fatal(err)
				}
				alias := filepath.Base(path)
				if spec.Name != nil {
					alias = spec.Name.Name
				}
				imports[alias] = path
			}
			for _, decl := range tree.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok {
					functions = append(functions, preflightFunction{body: fn.Body, imports: imports, name: fn.Name.Name})
				}
			}
		}
		g.packages[pkg] = functions
	}
	for _, fn := range functions {
		if fn.name == name && fn.body != nil {
			g.block(pkg, fn, fn.body)
		}
	}
}

// Only the proven configCheck branch can prune a path. Unknown conditions are
// both inspected, so an error path cannot hide an opener from this audit.
func (g *preflightSourceGate) block(pkg string, fn preflightFunction, block *ast.BlockStmt) bool {
	for _, stmt := range block.List {
		if branch, ok := stmt.(*ast.IfStmt); ok {
			g.node(pkg, fn, branch.Init)
			if enabled, known := preflightConfigCheck(branch.Cond); known {
				if enabled && g.block(pkg, fn, branch.Body) {
					return true
				}
				if !enabled {
					g.node(pkg, fn, branch.Else)
				}
				continue
			}
			g.node(pkg, fn, branch.Cond)
			g.block(pkg, fn, branch.Body)
			g.node(pkg, fn, branch.Else)
			continue
		}
		g.node(pkg, fn, stmt)
		if _, ok := stmt.(*ast.ReturnStmt); ok {
			return true
		}
	}
	return false
}

func preflightConfigCheck(expr ast.Expr) (bool, bool) {
	if ident, ok := expr.(*ast.Ident); ok && ident.Name == "configCheck" {
		return true, true
	}
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.NOT {
		value, known := preflightConfigCheck(unary.X)
		return !value, known
	}
	return false, false
}

func (g *preflightSourceGate) node(pkg string, fn preflightFunction, node ast.Node) {
	if node == nil {
		return
	}
	ast.Inspect(node, func(node ast.Node) bool {
		if closure, ok := node.(*ast.FuncLit); ok {
			g.block(pkg, fn, closure.Body)
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok {
			g.visit(pkg, ident.Name)
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			if owner, ok := selector.X.(*ast.Ident); ok {
				path := fn.imports[owner.Name]
				name := selector.Sel.Name
				if (path == "database/sql" && name == "Open") ||
					(strings.Contains(path, "bbolt") && name == "Open") ||
					(strings.Contains(path, "raft-boltdb") && (name == "New" || name == "NewBoltStore")) ||
					(strings.HasSuffix(path, "/internal/storage") && (strings.HasPrefix(name, "Open") || name == "ApplyMigrations")) ||
					(strings.HasSuffix(path, "/internal/cluster") && (strings.HasPrefix(name, "New") || strings.HasPrefix(name, "Acquire") || name == "RaftStateExists")) {
					g.t.Errorf("preflight reaches live store opener %s.%s through %s.%s", path, name, pkg, fn.name)
					return false
				}
				const project = "github.com/LinZiyang666/tether/"
				if strings.HasPrefix(path, project) {
					g.visit(strings.TrimPrefix(path, project), name)
				} else if path == "" {
					// The receiver may be a value returned by a project package
					// (for example the decoded serve configuration). Resolve its
					// method conservatively in this package and project imports.
					g.visit(pkg, name)
					for _, imported := range fn.imports {
						if strings.HasPrefix(imported, project) {
							g.visit(strings.TrimPrefix(imported, project), name)
						}
					}
				}
			}
		}
		return true
	})
}
