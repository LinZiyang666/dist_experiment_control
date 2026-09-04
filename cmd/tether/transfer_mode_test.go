package main

import (
	"os"
	"path/filepath"
	"testing"
)

// transfer_mode_test.go — a forced overwrite replaces CONTENT, not permissions.
//
// origin: prerelease audit round 2, I-F3 / I-F7 / G9 / J8.
//
// The atomic-replace shape creates its temp at 0600 and renames it over the
// destination, so without an explicit carry a `--force` overwrite silently reset the
// destination's mode: a 0755 script came back non-executable, a 0644 config unreadable
// to the group that had been reading it. The operator asked to replace the bytes.
//
// The FIRST fix landed on the inline path only and left the tier-B path — every file
// over the 8 MiB tier-A ceiling, i.e. exactly what `--force` is usually used for —
// still resetting the mode. It survived all four gates because the change had no test
// at all on either side. This file is that test.

func TestForcedLocalWriteKeepsTheDestinationsMode(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "app.sh")
	if err := os.WriteFile(dst, []byte("old\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Defeat umask: WriteFile's mode is masked, and the whole point is the mode we
	// observe on disk, not the one we asked for.
	if err := os.Chmod(dst, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := writeLocalAtomic(dst, []byte("new content\n"), true); err != nil {
		t.Fatalf("writeLocalAtomic: %v", err)
	}

	st, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o755 {
		t.Fatalf("a forced overwrite reset the destination's mode to %o, want 0755.\n\n"+
			"The operator asked to replace the CONTENT. A 0755 script that comes back 0600 is "+
			"not executable any more, and nothing in the output says so.", st.Mode().Perm())
	}
	body, _ := os.ReadFile(dst)
	if string(body) != "new content\n" {
		t.Errorf("content not replaced: %q", string(body))
	}
}

// TestAFreshLocalWriteDoesNotInventAMode: with no destination to inherit from, the
// temp file's own 0600 stands. Without this the test above is satisfied by a chmod
// that fires unconditionally against a stat of a file that does not exist.
func TestAFreshLocalWriteDoesNotInventAMode(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "brand-new.bin")

	if err := writeLocalAtomic(dst, []byte("x"), false); err != nil {
		t.Fatalf("writeLocalAtomic: %v", err)
	}
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("a first-time write landed at %o; with no destination to inherit from it must "+
			"keep the temp's own 0600", st.Mode().Perm())
	}
}

// TestBothPullPathsCarryTheMode is the WIRING half, and the reason it exists is that
// I-F3 was a missing call site rather than a broken helper: the inline path had the
// carry and the tier-B path did not, and no behavioural test could tell, because
// neither path had one.
//
// Driving the tier-B lander needs an ObjectStore result stream; asserting the call site
// in the source is what is affordable here, and it is exactly what a future edit would
// drop.
func TestBothPullPathsCarryTheMode(t *testing.T) {
	src, err := os.ReadFile("transfer.go")
	if err != nil {
		t.Fatal(err)
	}
	const carry = "f.Chmod(st.Mode().Perm())"
	if n := countAll(string(src), carry); n < 2 {
		t.Fatalf("only %d of the two local-write paths carries the destination's mode.\n\n"+
			"finishPullTierA writes inline through writeLocalAtomic; the tier-B lander streams "+
			"into its own temp. A fix on one of them leaves `pull --force` resetting the mode for "+
			"every file over the 8 MiB tier-A ceiling — which is most of the files anyone uses "+
			"--force on.", n)
	}
}

func countAll(hay, needle string) int {
	n, i := 0, 0
	for {
		j := indexFrom(hay, needle, i)
		if j < 0 {
			return n
		}
		n++
		i = j + len(needle)
	}
}

func indexFrom(hay, needle string, from int) int {
	if from >= len(hay) {
		return -1
	}
	idx := -1
	for k := from; k+len(needle) <= len(hay); k++ {
		if hay[k:k+len(needle)] == needle {
			idx = k
			break
		}
	}
	return idx
}
