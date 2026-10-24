package transferpath

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor open-parent-current-identity).
func TestAtomicReadRejectsParentReplacementAfterPinning(t *testing.T) {
	for _, replacement := range []string{"stable", "directory", "symlink", "absent"} {
		t.Run(replacement, func(t *testing.T) {
			// Five complete lifecycles also exercise descriptor cleanup on every
			// rejection. No scheduler race is needed: the swap precedes the
			// borrowed-fd open, but follows the production directory traversal.
			for range 5 {
				root := canonDir(t, t.TempDir())
				parent := filepath.Join(root, "parent")
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
				src := filepath.Join(parent, "source")
				if err := os.WriteFile(src, []byte("original directory bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
				vp, err := ValidateForRead(src, nil, ModeOpen)
				if err != nil {
					t.Fatal(err)
				}
				dirFD, err := openDirNoSymlinks(vp.Parent)
				if err != nil {
					t.Fatal(err)
				}
				// Use cleanup even if fixture setup fails; the returned file is a
				// separate descriptor and is closed immediately below.
				ownedDir := os.NewFile(uintptr(dirFD), vp.Parent)
				t.Cleanup(func() { _ = ownedDir.Close() })
				held := filepath.Join(root, "held")
				if replacement != "stable" {
					if err := os.Rename(parent, held); err != nil {
						t.Fatal(err)
					}
				}
				switch replacement {
				case "directory":
					if err := os.Mkdir(parent, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(src, []byte("replacement directory bytes"), 0o600); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					// Even a symlink back to the same directory must be refused:
					// the canonical pathname no longer names the pinned parent.
					if err := os.Symlink(held, parent); err != nil {
						t.Fatal(err)
					}
				case "stable", "absent":
				}
				f, err := openForReadAtomicAt(vp, vp.Parent, vp.Base, dirFD)
				if closeErr := ownedDir.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				var body []byte
				if f != nil {
					body, _ = io.ReadAll(f)
					if closeErr := f.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				if replacement == "stable" {
					if err != nil || string(body) != "original directory bytes" {
						t.Fatalf("stable pinned parent refused: body=%q err=%v", body, err)
					}
					continue
				}
				if f != nil || err == nil {
					t.Fatalf("read admitted bytes from a parent replaced after pinning: replacement=%s body=%q err=%v", replacement, body, err)
				}
				var pve *PathValidationError
				if !errors.As(err, &pve) || pve.Code != "path_race" {
					t.Fatalf("parent replacement was not reported as a race: %v", err)
				}
			}
		})
	}
}
