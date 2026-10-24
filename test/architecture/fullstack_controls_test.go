package architecture

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fullstack_controls_test.go — the shared S10 tree reader, §10 manifest parser
// and on-disk mutation tool behind the fullstack source gates.
//
// It owns no gate and carries no gate-control anchor. Plan §12.1 requires every
// gate file to keep its own same-file control: a shared control would let a gate
// whose predicate stopped matching hide behind a sibling's red. What lives here is
// only what a gate and its control must share so that the control cannot pass
// through a reader the real scan does not use — the disk reader, the line
// counters, the manifest parser and the mutate→red→restore→green tool. Source
// gates read the disk, so `go test -overlay` is invisible to them; every control
// therefore mutates a copied real tree on disk.
// origin: hot-update S10 plan §12.1 / §10.3 / P28

// fullstackRoots are the directories S10 created. Every code file below them is
// new S10 code whether or not a manifest row names it: owners split files by
// responsibility (completion_test.go, ofd_handoff_linux_test.go) beside their
// manifest destinations, and a manifest-only scan would never see those splits.
var fullstackRoots = []string{"internal/storewitness", "test/fullstackharness", "test/hotupdate/acceptance", "test/hotupdate/evidencecmd"}

const (
	fullstackPlanPath      = "docs/reviews/hot-update-fullstack-acceptance-plan.md"
	fullstackOwnershipPath = "test/architecture/testdata/fullstack_ownership.tsv"
	fullstackSchemaPath    = "test/hotupdate/acceptance/contract/schema.json"
	fullstackPackageCount  = 36 // P00..P35, plan §10
)

// fullstackTree is one disk snapshot. files holds every code file (Go, Python,
// shell, or an extensionless shebang tool) under internal/, cmd/ and test/;
// docs holds the non-code inputs the gates consume. Absent docs are "".
type fullstackTree struct {
	root  string
	files map[string]string
	docs  map[string]string
}

func readFullstackTree(t *testing.T, root string) fullstackTree {
	t.Helper()
	tree := fullstackTree{root: root, files: map[string]string{}, docs: map[string]string{}}
	for _, top := range []string{"internal", "cmd", "test"} {
		base := filepath.Join(root, top)
		if _, err := os.Stat(base); errors.Is(err, fs.ErrNotExist) {
			continue // a control fixture need not contain every top-level tree
		}
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				switch entry.Name() {
				case "testdata", "vendor", "node_modules", "__pycache__":
					return filepath.SkipDir
				}
				return nil
			}
			if !entry.Type().IsRegular() {
				return nil
			}
			ext := filepath.Ext(path)
			if ext != ".go" && ext != ".py" && ext != ".sh" && ext != "" {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if ext == "" && !bytes.HasPrefix(raw, []byte("#!")) {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			tree.files[filepath.ToSlash(rel)] = string(raw)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, doc := range []string{fullstackPlanPath, fullstackOwnershipPath, fullstackSchemaPath, "CLAUDE.md", "Makefile"} {
		raw, err := os.ReadFile(filepath.Join(root, doc))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		tree.docs[doc] = string(raw)
	}
	return tree
}

var fullstackRepoCache struct {
	sync.Mutex
	tree *fullstackTree
}

// fullstackRepoTree reads the repository once per test binary. Gates and
// controls only read this snapshot; every mutation goes to a fixture copy.
func fullstackRepoTree(t *testing.T) fullstackTree {
	t.Helper()
	fullstackRepoCache.Lock()
	defer fullstackRepoCache.Unlock()
	if fullstackRepoCache.tree == nil {
		tree := readFullstackTree(t, repoRoot(t))
		fullstackRepoCache.tree = &tree
	}
	return *fullstackRepoCache.tree
}

// exists covers manifest destinations that are not code (Markdown, JSON, TSV).
func (tree fullstackTree) exists(path string) bool {
	if _, ok := tree.files[path]; ok {
		return true
	}
	info, err := os.Stat(filepath.Join(tree.root, filepath.FromSlash(path)))
	return err == nil && info.Mode().IsRegular()
}

func fullstackUnder(path string, dirs ...string) bool {
	for _, dir := range dirs {
		if strings.HasPrefix(path, dir+"/") {
			return true
		}
	}
	return false
}

// fullstackFamily recognizes S10's responsibility-named file families outside
// the new directories (drill 99/100, hermetic wrappers, fixture libraries and
// these gates themselves), so a new sibling cannot escape by skipping the manifest.
func fullstackFamily(path string) bool {
	base := filepath.Base(path)
	return strings.HasPrefix(base, "fullstack_") || strings.HasPrefix(base, "fullstack-") ||
		strings.Contains(base, "hot-update-fullstack") || strings.Contains(base, "hot_update_fullstack")
}

// fullstackSize applies plan §12.1. Go counts comment-stripped token lines with
// the structural gate's own scanner. Scripts count non-blank lines that are not
// pure comments, and the physical count is a second conservative ceiling, so
// neither comment packing nor comment padding changes the verdict.
type fullstackSize struct{ physical, code int }

// Controls re-measure a copied tree after every one-file mutation; sizes are
// memoized by exact path and content so an unchanged file is never re-scanned.
var fullstackSizeMemo = struct {
	sync.Mutex
	sizes map[string]fullstackSize
}{sizes: map[string]fullstackSize{}}

func measureFullstackFile(t *testing.T, path, source string) fullstackSize {
	t.Helper()
	key := path + "\x00" + source
	fullstackSizeMemo.Lock()
	size, known := fullstackSizeMemo.sizes[key]
	fullstackSizeMemo.Unlock()
	if known {
		return size
	}
	defer func() {
		fullstackSizeMemo.Lock()
		fullstackSizeMemo.sizes[key] = size
		fullstackSizeMemo.Unlock()
	}()
	if source != "" {
		size.physical = strings.Count(strings.TrimSuffix(source, "\n"), "\n") + 1
	}
	if strings.HasSuffix(path, ".go") {
		size.code = countCodeLines(t, path, []byte(source))
		return size
	}
	for _, line := range strings.Split(source, "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			size.code++
		}
	}
	return size
}

// fullstackManifest is plan §10's unique file-ownership table. Each row's
// declared file count must equal its listed paths, so a dropped backtick is red.
type fullstackManifest struct {
	owner    map[string]string   // path -> package
	packages map[string][]string // package -> paths
}

var (
	fullstackManifestRowRe = regexp.MustCompile(`^\| (P[0-9]{2}) [^|]+\| (?:HARD|NORMAL) \| ([0-9]+) \|[^|]*\|(.*)\|$`)
	fullstackBacktickRe    = regexp.MustCompile("`([^`]+)`")
	fullstackOwnerRe       = regexp.MustCompile(`^P[0-9]{2}$`)
)

func parseFullstackManifest(plan string) (fullstackManifest, error) {
	m := fullstackManifest{owner: map[string]string{}, packages: map[string][]string{}}
	for _, line := range strings.Split(plan, "\n") {
		row := fullstackManifestRowRe.FindStringSubmatch(line)
		if row == nil {
			continue
		}
		if _, duplicate := m.packages[row[1]]; duplicate {
			return m, fmt.Errorf("duplicate S10 manifest package %s", row[1])
		}
		paths := fullstackBacktickRe.FindAllStringSubmatch(row[3], -1)
		count, err := strconv.Atoi(row[2])
		if err != nil || len(paths) == 0 || len(paths) != count {
			return m, fmt.Errorf("%s: manifest declares %s files but lists %d", row[1], row[2], len(paths))
		}
		for _, match := range paths {
			if prior := m.owner[match[1]]; prior != "" {
				return m, fmt.Errorf("%s: two S10 manifest owners %s/%s", match[1], prior, row[1])
			}
			m.owner[match[1]] = row[1]
			m.packages[row[1]] = append(m.packages[row[1]], match[1])
		}
	}
	if len(m.packages) != fullstackPackageCount {
		return m, fmt.Errorf("S10 manifest lists %d packages, want P00..P%02d", len(m.packages), fullstackPackageCount-1)
	}
	for i := 0; i < fullstackPackageCount; i++ {
		if _, ok := m.packages[fmt.Sprintf("P%02d", i)]; !ok {
			return m, fmt.Errorf("S10 manifest is missing package P%02d", i)
		}
	}
	return m, nil
}

// fullstackOwnedRows reads P29's actual-ownership ledger, keeping only S10
// package owners; S8/S9 handoff rows keep their own stage gates.
func fullstackOwnedRows(ledger string) (map[string]string, error) {
	rows := map[string]string{}
	for i, line := range strings.Split(ledger, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 2 || fields[0] == "" {
			return nil, fmt.Errorf("%s:%d: malformed ownership row", fullstackOwnershipPath, i+1)
		}
		if fullstackOwnerRe.MatchString(fields[1]) {
			rows[fields[0]] = fields[1]
		}
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("empty S10 ownership root %s", fullstackOwnershipPath)
	}
	return rows, nil
}

// fullstackFixture copies real files into a temporary tree: every code file
// under the given directory prefixes (a trailing * matches a file-name prefix)
// plus all gate inputs. Controls then mutate those copies on disk and re-read
// them with readFullstackTree.
func fullstackFixture(t *testing.T, real fullstackTree, prefixes ...string) string {
	t.Helper()
	root := t.TempDir()
	for path, source := range real.files {
		for _, prefix := range prefixes {
			if path == prefix || strings.HasPrefix(path, prefix+"/") ||
				strings.HasSuffix(prefix, "*") && strings.HasPrefix(path, strings.TrimSuffix(prefix, "*")) {
				transferWriteFixture(t, root, path, source)
				break
			}
		}
	}
	for path, source := range real.docs {
		if source != "" {
			transferWriteFixture(t, root, path, source)
		}
	}
	return root
}

// fullstackMutation edits (old→new, or appends when old is empty), creates (on
// an absent path) or removes one fixture file. want is the substring the red
// verdict must carry: a control red for the wrong reason proves nothing.
type fullstackMutation struct {
	path, old, new, want string
	remove               bool
}

// proveFullstackMutation is G1 on disk: the baseline is green, the defect is
// written to the file the real reader scans, the verdict turns red for the named
// reason, the original bytes return and the verdict is green again. It returns
// errors instead of failing so that its own refusals are testable.
func proveFullstackMutation(root string, m fullstackMutation, check func() error) error {
	if err := check(); err != nil {
		return fmt.Errorf("baseline red before mutating %s: %w", m.path, err)
	}
	path := filepath.Join(root, filepath.FromSlash(m.path))
	original, err := os.ReadFile(path)
	created := errors.Is(err, fs.ErrNotExist)
	if err != nil && !created {
		return err
	}
	switch {
	case created && (m.old != "" || m.remove):
		return fmt.Errorf("%s: mutation target is missing", m.path)
	case m.remove:
		err = os.Remove(path)
	default:
		mutated := m.new
		if !created && m.old == "" {
			mutated = string(original) + m.new
		} else if !created {
			if strings.Count(string(original), m.old) != 1 {
				return fmt.Errorf("%s: mutation anchor %q must occur exactly once", m.path, m.old)
			}
			mutated = strings.Replace(string(original), m.old, m.new, 1)
		}
		if !created && mutated == string(original) {
			return fmt.Errorf("%s: vacuous mutation", m.path)
		}
		if err = os.MkdirAll(filepath.Dir(path), 0o700); err == nil {
			err = os.WriteFile(path, []byte(mutated), 0o600)
		}
	}
	if err != nil {
		return err
	}
	verdict := check()
	if created {
		err = os.Remove(path)
	} else if err = os.WriteFile(path, original, 0o600); err == nil {
		var restored []byte
		if restored, err = os.ReadFile(path); err == nil && !bytes.Equal(restored, original) {
			err = fmt.Errorf("%s: restore changed bytes", m.path)
		}
	}
	if err != nil {
		return err
	}
	if verdict == nil {
		return fmt.Errorf("%s: on-disk mutation escaped the gate (%q→%q)", m.path, m.old, m.new)
	}
	if !strings.Contains(verdict.Error(), m.want) {
		return fmt.Errorf("%s: red for the wrong reason: %v (want %q)", m.path, verdict, m.want)
	}
	if err := check(); err != nil {
		return fmt.Errorf("restored fixture stays red after %s: %w", m.path, err)
	}
	return nil
}

func proveFullstackMutations(t *testing.T, root string, check func() error, mutations ...fullstackMutation) {
	t.Helper()
	for _, m := range mutations {
		if err := proveFullstackMutation(root, m, check); err != nil {
			t.Error(err)
		}
	}
}

// The tool is the shared half of every fullstack control, so its own refusals
// are proven here: a vacuous, escaping, wrong-reason or red-baseline control
// must never be reported as a kill, and every path restores the original bytes.
// origin: hot-update S10 plan §12.1 / P28
func TestFullstackMutationToolRejectsVacuousControls(t *testing.T) {
	root := t.TempDir()
	const path = "test/tool/file.txt"
	transferWriteFixture(t, root, path, "status ok\n")
	check := func() error {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return fmt.Errorf("missing tool fixture: %w", err)
		}
		if extra, err := os.ReadFile(filepath.Join(root, "test/tool/extra.txt")); err == nil && len(extra) > 0 {
			return errors.New("extra file appeared")
		}
		if strings.Contains(string(raw), "bad") {
			return errors.New("bad marker")
		}
		return nil
	}
	for _, m := range []fullstackMutation{
		{path: path, old: "ok", new: "bad", want: "bad marker"},
		{path: path, new: "bad tail", want: "bad marker"},
		{path: path, remove: true, want: "missing tool fixture"},
		{path: "test/tool/extra.txt", new: "x", want: "extra file"},
	} {
		if err := proveFullstackMutation(root, m, check); err != nil {
			t.Fatalf("valid control refused: %v", err)
		}
		if raw, err := os.ReadFile(filepath.Join(root, path)); err != nil || string(raw) != "status ok\n" {
			t.Fatalf("tool did not restore the original bytes: %q %v", raw, err)
		}
		if _, err := os.Stat(filepath.Join(root, "test/tool/extra.txt")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("tool left a created file behind")
		}
	}
	for name, m := range map[string]fullstackMutation{
		"vacuous":      {path: path, old: "absent", new: "bad", want: "bad marker"},
		"identity":     {path: path, old: "ok", new: "ok", want: "bad marker"},
		"escaping":     {path: path, old: "ok", new: "fine", want: "bad marker"},
		"wrong reason": {path: path, old: "ok", new: "bad", want: "other reason"},
		"missing edit": {path: "test/tool/absent.txt", old: "ok", new: "bad", want: "bad marker"},
	} {
		if proveFullstackMutation(root, m, check) == nil {
			t.Fatalf("%s control reported as a kill", name)
		}
	}
	transferWriteFixture(t, root, path, "status bad\n")
	if proveFullstackMutation(root, fullstackMutation{path: path, old: "status", new: "state", want: "bad marker"}, check) == nil {
		t.Fatal("red baseline accepted as a control")
	}
}
