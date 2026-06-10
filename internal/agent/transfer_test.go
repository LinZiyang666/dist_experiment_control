package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// canonDir resolves symlinks in a test dir path. t.TempDir() is not
// guaranteed canonical (macOS: /var → /private/var), while
// CanonAllowRoots / ValidateFor* return EvalSymlinks-resolved paths —
// expectations must compare against the resolved form.
func canonDir(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", dir, err)
	}
	return resolved
}

func TestValidate_TransferDisabledOnEmptyAllowRoots(t *testing.T) {
	for _, fn := range []func(string, []string) (*ValidatedPath, error){
		ValidateForRead, ValidateForWrite,
	} {
		_, err := fn("/etc/passwd", nil)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "transfer_disabled" {
			t.Errorf("got %v, want transfer_disabled", err)
		}
	}
}

func TestValidate_RejectsNonAbsolute(t *testing.T) {
	roots := CanonAllowRoots([]string{t.TempDir()})
	for _, fn := range []func(string, []string) (*ValidatedPath, error){
		ValidateForRead, ValidateForWrite,
	} {
		_, err := fn("relative/path", roots)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_not_absolute" {
			t.Errorf("got %v, want path_not_absolute", err)
		}
	}
}

// Push: ../-style escapes get caught by EvalSymlinks(parent) +
// containment check. filepath.Clean alone DOES strip the .. so by
// itself it's not enough; this exercises the full chain.
func TestValidateForWrite_RejectsDotDotEscape(t *testing.T) {
	root := t.TempDir()
	// /tmp/<root>/sub/../../escape — Clean gives /tmp/escape which is
	// outside <root>. Containment check rejects.
	_, err := ValidateForWrite(filepath.Join(root, "sub", "..", "..", "escape"), CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) {
		t.Fatalf("got %v, want PathValidationError", err)
	}
	// Could be path_outside_roots OR path_parent_missing (if the parent
	// doesn't exist). Either is correct; both prevent the write.
	if pve.Code != "path_outside_roots" && pve.Code != "path_parent_missing" {
		t.Errorf("got code=%q, want path_outside_roots|path_parent_missing", pve.Code)
	}
}

// Pull: a symlink at the leaf must be rejected by lstat, before any
// open with O_NOFOLLOW (which would also catch it but with worse error
// attribution).
func TestValidateForRead_RejectsSymlinkAtLeaf(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, err := ValidateForRead(link, CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// Push: same — symlink at leaf rejected even when target is regular.
func TestValidateForWrite_RejectsSymlinkAtLeaf(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, err := ValidateForWrite(link, CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// A symlink in the dir CHAIN is followed (EvalSymlinks of parent), but
// the resolved path must still land inside an allow_root. If the link
// points OUT of allow_roots, containment rejects.
func TestValidateForWrite_RejectsDirSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	// /<root>/escape -> /<outside>
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	// Try to write /<root>/escape/x — resolves parent to /<outside> →
	// outside the allow_root.
	_, err := ValidateForWrite(filepath.Join(link, "x"), CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
		t.Errorf("got %v, want path_outside_roots", err)
	}
}

// A symlink in the dir CHAIN that points INSIDE allow_roots must
// succeed (the leaf is what matters; intermediate symlinks just
// resolve to a real path inside the root).
func TestValidateForWrite_AcceptsDirSymlinkInsideRoot(t *testing.T) {
	root := t.TempDir()
	innerReal := filepath.Join(root, "real-dir")
	if err := os.Mkdir(innerReal, 0o755); err != nil {
		t.Fatal(err)
	}
	innerLink := filepath.Join(root, "alias")
	if err := os.Symlink(innerReal, innerLink); err != nil {
		t.Fatal(err)
	}
	vp, err := ValidateForWrite(filepath.Join(innerLink, "newfile.bin"), CanonAllowRoots([]string{root}))
	if err != nil {
		t.Fatalf("got %v, want OK", err)
	}
	if want := canonDir(t, innerReal); !strings.HasPrefix(vp.Abs, want+"/") {
		t.Errorf("vp.Abs=%q does not start with resolved %q/", vp.Abs, want)
	}
}

// Push: parent dir missing → path_parent_missing (we don't auto-mkdir).
func TestValidateForWrite_ParentMissing(t *testing.T) {
	root := t.TempDir()
	_, err := ValidateForWrite(filepath.Join(root, "no-such-dir", "file"), CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_parent_missing" {
		t.Errorf("got %v, want path_parent_missing", err)
	}
}

// Pull: source missing → path_not_found.
func TestValidateForRead_NotFound(t *testing.T) {
	root := t.TempDir()
	_, err := ValidateForRead(filepath.Join(root, "no-such-file"), CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_not_found" {
		t.Errorf("got %v, want path_not_found", err)
	}
}

// Pull: directory at the leaf → not_a_regular_file.
func TestValidateForRead_RejectsDirectoryLeaf(t *testing.T) {
	root := t.TempDir()
	d := filepath.Join(root, "subdir")
	if err := os.Mkdir(d, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := ValidateForRead(d, CanonAllowRoots([]string{root}))
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// CanonAllowRoots: longest-prefix-wins ordering so /srv/local/alice
// shadows /srv when both are listed.
func TestCanonicalAllowRoots_LongestWins(t *testing.T) {
	srv := t.TempDir()
	deeper := filepath.Join(srv, "alice")
	if err := os.Mkdir(deeper, 0o755); err != nil {
		t.Fatal(err)
	}
	roots := CanonAllowRoots([]string{srv, deeper})
	if len(roots) != 2 {
		t.Fatalf("want 2 roots, got %v", roots)
	}
	if want := canonDir(t, deeper); roots[0] != want {
		t.Errorf("longest first violation: roots[0]=%q want %q", roots[0], want)
	}
}

// CanonAllowRoots silently drops non-existent + non-directory + relative.
func TestCanonicalAllowRoots_DropsBad(t *testing.T) {
	root := t.TempDir()
	regular := filepath.Join(root, "file.txt")
	if err := os.WriteFile(regular, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	roots := CanonAllowRoots([]string{
		"", "relative", "/no/such/path",
		regular, // file, not a dir
		root,    // good
	})
	if want := canonDir(t, root); len(roots) != 1 || roots[0] != want {
		t.Errorf("got %v, want exactly [%q]", roots, want)
	}
}

// Round-trip: ValidateForWrite + OpenForWriteAtomic + RenameForWriteAtomic
// produces a regular file in place; refusing --force when dest exists
// returns dst_exists.
func TestWriteAtomic_DstExistsHonored(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(root, "out.bin")
	if err := os.WriteFile(dst, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	vp, err := ValidateForWrite(dst, CanonAllowRoots([]string{root}))
	if err != nil {
		t.Fatal(err)
	}
	f, tmp, err := OpenForWriteAtomic(vp, "abc")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("new"))
	_ = f.Close()

	// force=false → dst_exists, tmp left for caller cleanup.
	err = RenameForWriteAtomic(vp, tmp, false)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "dst_exists" {
		t.Errorf("got %v, want dst_exists", err)
	}

	// force=true → overwrite.
	if err := RenameForWriteAtomic(vp, tmp, true); err != nil {
		t.Fatalf("force rename: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "new" {
		t.Errorf("dst contents = %q, want %q", got, "new")
	}
}

// OpenForReadAtomic returns ELOOP / not_a_regular_file when the path
// is a leaf symlink (defence-in-depth on top of the lstat check in
// ValidateForRead).
func TestOpenForReadAtomic_RejectsSymlinkLeaf(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	// Spoof a ValidatedPath as if validation had not seen the symlink
	// (simulates TOCTOU: an attacker swaps file→symlink between
	// lstat and open). OpenForReadAtomic should still refuse.
	vp := &ValidatedPath{Abs: link, AllowRoot: root}
	_, err := OpenForReadAtomic(vp)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}
