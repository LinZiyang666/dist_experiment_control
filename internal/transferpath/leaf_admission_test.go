package transferpath

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor leaf-symlink-validation).
func TestLeafValidationRejectsSymlinkWithoutRegularFileGate(t *testing.T) {
	for _, targetKind := range []string{"regular", "directory", "missing"} {
		t.Run(targetKind, func(t *testing.T) {
			leaf := symlinkAdmissionFixture(t, targetKind)
			st, err := os.Lstat(leaf)
			if err != nil {
				t.Fatal(err)
			}
			// Exercise the production symlink decision directly. The adjacent
			// IsRegular rejection otherwise hides removal of this defence.
			if err := rejectLeafSymlink(leaf, st); err == nil {
				t.Fatal("leaf validation admitted a symlink without the regular-file gate")
			} else {
				requireSymlinkAdmissionDenied(t, err)
			}
		})
	}
	regular := filepath.Join(t.TempDir(), "regular")
	if err := os.WriteFile(regular, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := os.Lstat(regular)
	if err != nil {
		t.Fatal(err)
	}
	if err := rejectLeafSymlink(regular, st); err != nil {
		t.Fatalf("symlink admission rejected a regular leaf: %v", err)
	}
}

// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor open-symlink-guard).
func TestAtomicReadRejectsSymlinkWithoutRegularFileGate(t *testing.T) {
	for _, targetKind := range []string{"regular", "directory", "missing"} {
		t.Run(targetKind, func(t *testing.T) {
			leaf := symlinkAdmissionFixture(t, targetKind)
			parent := canonDir(t, filepath.Dir(leaf))
			dirFD, err := openDirNoSymlinks(parent)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = unix.Close(dirFD) }()
			var pre unix.Stat_t
			if err := unix.Fstatat(dirFD, filepath.Base(leaf), &pre, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				t.Fatal(err)
			}
			// The input comes from the actual pinned-directory stat, without
			// invoking the regular-file or kernel-open rejection layers.
			if err := rejectReadSymlink(leaf, pre); err == nil {
				t.Fatal("atomic read admitted a symlink without the regular-file or O_NOFOLLOW gates")
			} else {
				requireSymlinkAdmissionDenied(t, err)
			}
		})
	}
	regular := filepath.Join(t.TempDir(), "regular")
	if err := os.WriteFile(regular, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	var pre unix.Stat_t
	if err := unix.Lstat(regular, &pre); err != nil {
		t.Fatal(err)
	}
	if err := rejectReadSymlink(regular, pre); err != nil {
		t.Fatalf("atomic symlink admission rejected a regular leaf: %v", err)
	}
}

func symlinkAdmissionFixture(t *testing.T, targetKind string) string {
	t.Helper()
	root := t.TempDir()
	target := filepath.Join(root, "target")
	switch targetKind {
	case "regular":
		if err := os.WriteFile(target, []byte("protected target"), 0o600); err != nil {
			t.Fatal(err)
		}
	case "directory":
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
	case "missing":
	default:
		t.Fatalf("unknown symlink target kind %q", targetKind)
	}
	leaf := filepath.Join(root, "leaf")
	if err := os.Symlink(target, leaf); err != nil {
		t.Fatal(err)
	}
	return leaf
}

func requireSymlinkAdmissionDenied(t *testing.T, err error) {
	t.Helper()
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Fatalf("symlink denial was not a permanent leaf rejection: %v", err)
	}
}
