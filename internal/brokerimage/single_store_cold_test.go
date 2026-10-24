package brokerimage

import (
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Cold compatibility and terminal cleanup must survive image-ineligible stores.
// In particular an N-1 rollback retains the newer migration ledger and the same
// flock until the actual writer closes; a refused image close leaves it usable.
// origin: hot-update S6 plan §3.1 / §3.2; internal review rv-writer-7/8/12 and rv-fd-9
func TestColdSingleStoreKeepsImageIneligibleDatabasesUsable(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseColdSingleCompatibility(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "cold single compatibility", gb)
	testharness.AssertNoFDLeak(t, "cold single compatibility", fb, 0)
}

func exerciseColdSingleCompatibility(t *testing.T) {
	t.Helper()
	if err := InstallColdSingleStoreSupport(); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"newer-ledger", "truncate", "persist", "memory", "memory-uri"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.db")
			memory := kind == "memory" || kind == "memory-uri"
			if memory {
				path = ":memory:"
				if kind == "memory-uri" {
					path = "file::memory:"
				}
			} else {
				db, err := storage.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "newer-ledger" {
					_, err = db.Exec(`INSERT INTO schema_migrations(version) VALUES('9999_newer.sql')`)
				} else {
					_, err = db.Exec("PRAGMA journal_mode=" + kind)
				}
				if err := errors.Join(err, db.Close()); err != nil {
					t.Fatal(err)
				}
			}
			owner, db, err := OpenSingleStore(SingleStoreConfig{DBPath: path, Registry: storage.DefaultDriverRegistry()})
			if owner != nil {
				t.Cleanup(func() { _ = CloseStoreOwner(owner) })
			}
			if err != nil || db == nil {
				t.Fatalf("cold %s refused: %v", kind, err)
			}
			if kind == "truncate" || kind == "persist" {
				// These rollback modes are connection-local; set them on the
				// serving writer rather than assuming they survive a pool Close.
				if _, err := db.Exec("PRAGMA journal_mode=" + kind); err != nil {
					t.Fatal(err)
				}
			}
			_, stampErr := owner.Stamp()
			if stampErr == nil {
				t.Fatal("ineligible cold store certified an image stamp")
			}
			if memory && runtime.GOOS == "linux" && !strings.Contains(stampErr.Error(), "in-memory") {
				t.Fatalf("memory owner lost explicit image-ineligible reason: %v", stampErr)
			}
			if _, err := owner.CloseForImage(controlbudget.Deadline(1)); err == nil {
				t.Fatal("image close accepted an ineligible store")
			}
			var value int
			if err := db.QueryRow("SELECT 1").Scan(&value); err != nil || value != 1 {
				t.Fatalf("image refusal damaged the cold writer: %v", err)
			}
			if !memory {
				if release, err := cluster.AcquireDataDirLock(dir); err == nil {
					release()
					t.Fatal("live cold writer lost its canonical flock")
				}
			}
			if err := CloseStoreOwner(owner); err != nil {
				t.Fatalf("cold terminal close: %v", err)
			}
			if memory {
				if _, err := os.Stat(filepath.Join(dir, storage.StoreLockFile)); !os.IsNotExist(err) {
					t.Fatalf("in-memory owner created a filesystem interlock: %v", err)
				}
			} else {
				release, err := cluster.AcquireDataDirLock(dir)
				if err != nil {
					t.Fatalf("terminal owner retained flock: %v", err)
				}
				release()
				id, err := storage.CanonicalStoreIdentity(path, "")
				if err != nil {
					t.Fatal(err)
				}
				if w, r := storage.DefaultDriverRegistry().Connections(id); w != 0 || r != 0 {
					t.Fatalf("terminal close retained drivers: %d/%d", w, r)
				}
			}
		})
	}
}

// The native Darwin test above is meaningful only if cold operations never
// consult the Linux image clock. Cross-building alone cannot catch this runtime
// refusal; retain a source control as well as the all-OS behavioral regression.
// origin: hot-update S6 plan §9.3; internal review rv-writer-8
func TestColdSingleStoreDoesNotRequireLinuxImageClock(t *testing.T) {
	tree, err := parser.ParseFile(token.NewFileSet(), "single_store.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range tree.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "OpenSingleStore" && fn.Name.Name != "Close") {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok {
				if owner, ok := selector.X.(*ast.Ident); ok && owner.Name == "rawsocket" && strings.HasPrefix(selector.Sel.Name, "Boot") {
					t.Errorf("cold %s requires Linux image clock", fn.Name.Name)
				}
			}
			return true
		})
	}
}

// A constructor can return a live pool alongside its error. Losing its identity
// then hides that pool from driver-zero checks and strands the canonical flock.
// origin: hot-update S6 plan §3.2 / T10; internal review rv-writer-7
func TestColdSinglePartialOwnerRetainsCanonicalIdentity(t *testing.T) {
	if err := InstallColdSingleStoreSupport(); err != nil {
		t.Fatal(err)
	}
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePartialColdSingleOwner(t)
	}
	testharness.AssertNoGoroutineLeak(t, "partial cold single owner", gb)
	testharness.AssertNoFDLeak(t, "partial cold single owner", fb, 0)
}

func exercisePartialColdSingleOwner(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := storage.CanonicalStoreIdentity(path, "")
	if err != nil {
		t.Fatal(err)
	}
	// No package test runs in parallel. Preserve the real lock/opener while
	// injecting only the documented nonnil-pool/zero-stamp constructor failure.
	singleStoreSupport.Lock()
	previous := singleStoreSupport.support
	injected := *previous
	injected.OpenCold = func(cfg SingleStoreConfig) (*sql.DB, storage.StoreStamp, error) {
		db, _, err := previous.OpenCold(cfg)
		return db, storage.StoreStamp{}, errors.Join(err, errors.New("cold constructor refused after open"))
	}
	singleStoreSupport.support = &injected
	singleStoreSupport.Unlock()
	defer func() {
		singleStoreSupport.Lock()
		singleStoreSupport.support = previous
		singleStoreSupport.Unlock()
	}()
	owner, db, err := OpenSingleStore(SingleStoreConfig{DBPath: path, Registry: storage.DefaultDriverRegistry()})
	if owner != nil {
		defer func() { _ = CloseStoreOwner(owner) }()
	}
	if err == nil || owner == nil || db == nil {
		t.Fatalf("missing partial constructor owner: %v", err)
	}
	if got := owner.(*singleStoreOwner).stamp.Identity; got != identity {
		t.Fatalf("partial owner lost identity: %+v != %+v", got, identity)
	}
	if err := CloseStoreOwner(owner); err != nil {
		t.Fatal(err)
	}
	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("partial terminal owner retained flock: %v", err)
	}
	release()
	if writers, readers := storage.DefaultDriverRegistry().Connections(identity); writers != 0 || readers != 0 {
		t.Fatalf("partial owner retained actual drivers: %d/%d", writers, readers)
	}
}
