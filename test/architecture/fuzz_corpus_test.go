package architecture

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// fuzz_corpus_test.go — the committed fuzz corpus (testdata/fuzz/<Fuzz*>/) stays small and well-formed.
//
// WHY THIS EXISTS
// ---------------
// `go test -fuzz` writes only CRASHERS into testdata/fuzz; the "interesting" inputs it discovers live in
// $GOCACHE/fuzz and are never in the tree. Committing a corpus is therefore always a deliberate copy
// (docs/testing-standards.md G7), and the failure mode of a deliberate copy is copying too much: a
// few hundred JSON variants per target, each a file, each replayed by every `make test` forever.
// The budget is per target (≤ fuzzCorpusMaxFiles files) and global (≤ fuzzCorpusMaxBytes), and every
// file must carry the `go test fuzz v1` header — a file without it is silently ignored by the
// toolchain, which is the worst kind of committed test input.
//
// The one corpus on the day this landed: internal/proxydial/testdata/fuzz/FuzzSOCKS5Reply, 23 files,
// the saturated enumeration of every REP code and ATYP branch of the hand-written SOCKS5 parser.
//
// gate-control: TestFuzzCorpusBudgetSeesEveryShape

const (
	fuzzCorpusMaxFiles = 64
	fuzzCorpusMaxBytes = 256 * 1024
	fuzzCorpusHeader   = "go test fuzz v1"
)

// fuzzCorpusEntry is one testdata/fuzz/<Fuzz*> directory.
type fuzzCorpusEntry struct {
	dir   string // repo-relative
	files int
	bytes int64
	bad   []string // files whose first line is not the header
}

// collectFuzzCorpora walks root for testdata/fuzz/<Fuzz*> directories. Pure; shared with the self-check.
func collectFuzzCorpora(root string) ([]fuzzCorpusEntry, error) {
	byDir := map[string]*fuzzCorpusEntry{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		dir := filepath.Dir(p)
		parent := filepath.Dir(dir)
		if filepath.Base(parent) != "fuzz" || filepath.Base(filepath.Dir(parent)) != "testdata" ||
			!strings.HasPrefix(filepath.Base(dir), "Fuzz") {
			return nil
		}
		rel, rerr := filepath.Rel(root, dir)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		e := byDir[rel]
		if e == nil {
			e = &fuzzCorpusEntry{dir: rel}
			byDir[rel] = e
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		e.files++
		e.bytes += info.Size()
		fh, oerr := os.Open(p)
		if oerr != nil {
			return oerr
		}
		first, _ := bufio.NewReader(fh).ReadString('\n')
		_ = fh.Close()
		if strings.TrimRight(first, "\r\n") != fuzzCorpusHeader {
			e.bad = append(e.bad, filepath.Base(p))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]fuzzCorpusEntry, 0, len(byDir))
	for _, e := range byDir {
		sort.Strings(e.bad)
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dir < out[j].dir })
	return out, nil
}

// fuzzCorpusViolations applies the budget. Pure; shared with the self-check.
func fuzzCorpusViolations(entries []fuzzCorpusEntry) []string {
	var v []string
	var total int64
	for _, e := range entries {
		total += e.bytes
		if e.files > fuzzCorpusMaxFiles {
			v = append(v, e.dir+": "+itoa(e.files)+" files (max "+itoa(fuzzCorpusMaxFiles)+")")
		}
		for _, b := range e.bad {
			v = append(v, e.dir+"/"+b+": first line is not "+fuzzCorpusHeader)
		}
	}
	if total > fuzzCorpusMaxBytes {
		v = append(v, "committed fuzz corpus totals "+itoa(int(total))+" bytes (max "+itoa(fuzzCorpusMaxBytes)+")")
	}
	return v
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func TestFuzzCorpusStaysWithinBudget(t *testing.T) {
	entries, err := collectFuzzCorpora(repoRoot(t))
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Live-tree floor (G2b-legal: the success state keeps at least the SOCKS5 corpus).
	if len(entries) == 0 {
		t.Fatal("no testdata/fuzz/<Fuzz*> directory found anywhere — the walk is broken or the SOCKS5 corpus was deleted")
	}
	if v := fuzzCorpusViolations(entries); len(v) > 0 {
		t.Errorf("%d fuzz corpus budget violation(s):\n  %s\n\n"+
			"Commit crashers and hand-picked seeds only (docs/testing-standards.md G7); bulk GOCACHE "+
			"corpora stay out of the tree.", len(v), strings.Join(v, "\n  "))
	}
}

// orphanedCorpora returns every corpus directory whose name matches no `func <Name>(` in a `_test.go`
// of the package that owns it. The toolchain replays testdata/fuzz/<F>/ only while a Fuzz function
// called F exists in that package; rename the function and the directory is silently ignored —
// the same failure class as a file without the header, which this gate already treats as the worst
// kind of committed input (internal review L1-F9). Pure; shared with the self-check.
func orphanedCorpora(root string, entries []fuzzCorpusEntry) ([]string, error) {
	var orphans []string
	for _, e := range entries {
		// <pkg>/testdata/fuzz/<Fuzz> -> <pkg>
		pkgDir := filepath.Join(root, filepath.FromSlash(filepath.Dir(filepath.Dir(filepath.Dir(e.dir)))))
		name := filepath.Base(e.dir)
		re := regexp.MustCompile(`(?m)^func ` + regexp.QuoteMeta(name) + `\(`)
		files, err := filepath.Glob(filepath.Join(pkgDir, "*_test.go"))
		if err != nil {
			return nil, err
		}
		found := false
		for _, f := range files {
			b, rerr := os.ReadFile(f)
			if rerr != nil {
				return nil, rerr
			}
			if re.Match(b) {
				found = true
				break
			}
		}
		if !found {
			orphans = append(orphans, e.dir+": no `func "+name+"(` in "+filepath.ToSlash(filepath.Dir(filepath.Dir(filepath.Dir(e.dir))))+" — the toolchain replays nothing from this directory")
		}
	}
	return orphans, nil
}

func TestEveryCommittedCorpusNamesALiveFuzzTarget(t *testing.T) {
	root := repoRoot(t)
	entries, err := collectFuzzCorpora(root)
	if err != nil {
		t.Fatal(err)
	}
	orphans, err := orphanedCorpora(root, entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) > 0 {
		t.Errorf("%d orphaned fuzz corpus director(y/ies):\n  %s\n\nRename the directory with the function, or delete it.",
			len(orphans), strings.Join(orphans, "\n  "))
	}
}

// TestFuzzCorpusBudgetSeesEveryShape is the G2 self-check: a synthetic tree exercising the walk (a
// directory that is NOT a fuzz corpus must be ignored) and every violation class through the same
// two functions the gate uses.
func TestFuzzCorpusBudgetSeesEveryShape(t *testing.T) {
	root := t.TempDir()
	mk := func(rel, content string) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	good := fuzzCorpusHeader + "\n[]byte(\"x\")\n"
	mk("a/testdata/fuzz/FuzzOK/1", good)
	mk("a/testdata/fuzz/FuzzOK/2", good)
	mk("a/testdata/fuzz/FuzzBadHeader/1", "not a corpus file\n")
	mk("a/testdata/fuzz/notfuzz/1", good) // no Fuzz prefix: not a corpus dir
	mk("a/testdata/other/FuzzX/1", good)  // not under fuzz/: ignored
	for i := 0; i <= fuzzCorpusMaxFiles; i++ {
		mk("b/testdata/fuzz/FuzzTooMany/"+itoa(i), good)
	}
	entries, err := collectFuzzCorpora(root)
	if err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, e := range entries {
		dirs = append(dirs, e.dir)
	}
	if strings.Join(dirs, ",") != "a/testdata/fuzz/FuzzBadHeader,a/testdata/fuzz/FuzzOK,b/testdata/fuzz/FuzzTooMany" {
		t.Fatalf("walk saw %v", dirs)
	}
	v := fuzzCorpusViolations(entries)
	want := []string{
		"a/testdata/fuzz/FuzzBadHeader/1: first line is not " + fuzzCorpusHeader,
		"b/testdata/fuzz/FuzzTooMany: 65 files (max 64)",
	}
	if strings.Join(v, "\n") != strings.Join(want, "\n") {
		t.Fatalf("violations drifted:\n got  %v\n want %v", v, want)
	}
	// Byte budget, on its own synthetic set.
	over := []fuzzCorpusEntry{{dir: "x", files: 1, bytes: fuzzCorpusMaxBytes + 1}}
	if v := fuzzCorpusViolations(over); len(v) != 1 || !strings.Contains(v[0], "totals") {
		t.Fatalf("byte budget not enforced: %v", v)
	}
	// Orphans: FuzzOK has a live target in package a, FuzzBadHeader does not, package b has no
	// test file at all.
	mk("a/a_test.go", "package a\nfunc FuzzOK(f *testing.F) {}\nfunc TestFuzzBadHeaderIsNotAFuzzTarget(t *testing.T) {}\n")
	orphans, err := orphanedCorpora(root, entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 2 || !strings.HasPrefix(orphans[0], "a/testdata/fuzz/FuzzBadHeader:") || !strings.HasPrefix(orphans[1], "b/testdata/fuzz/FuzzTooMany:") {
		t.Fatalf("orphan detection drifted: %v", orphans)
	}
}
