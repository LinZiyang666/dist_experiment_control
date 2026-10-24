//go:build linux

package installfs

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// Keep the filesystem owner exercised after moving it out of root composition:
// untrusted ancestors, aliases and noncanonical records must still be refused.
// origin: S9 internal review GATE-CONVERGE-sliminfra F1
func TestInstalledFilesLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInstalledFiles(t)
	}
	testharness.AssertNoGoroutineLeak(t, "pinned installation namespace", beforeG)
	testharness.AssertNoFDLeak(t, "installation records and directory custody", beforeFD, 0)
}

func exerciseInstalledFiles(t *testing.T) {
	t.Helper()
	base := t.TempDir()
	owner, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	files, err := Open(base, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	directory, err := files.Directory("/records", true, 0700, gid)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	if files.Owner() != owner || files.Path("/records") != filepath.Join(base, "records") {
		t.Fatal("namespace projection changed")
	}
	type record struct {
		Value string `json:"value"`
	}
	want := record{"verified"}
	if err := files.Encode("/records/current", want, 0600, gid); err != nil {
		t.Fatal(err)
	}
	var got record
	if err := files.Decode("/records/current", &got); err != nil || got != want {
		t.Fatalf("canonical record: %v %v", got, err)
	}
	if _, err := files.Read("/records/current", 1); !errors.Is(err, c.ErrIncomplete) {
		t.Fatalf("oversized record accepted: %v", err)
	}
	if _, err := files.Directory("/records/../records", false, 0, 0); !errors.Is(err, infrapriv.ErrPermission) {
		t.Fatalf("noncanonical directory accepted: %v", err)
	}
	if err := os.Chmod(files.Path("/records"), 0777); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Read("/records/current", 100); !errors.Is(err, infrapriv.ErrPermission) {
		t.Fatalf("service-writable ancestor accepted: %v", err)
	}
	if err := os.Chmod(files.Path("/records"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("current", files.Path("/records/alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Read("/records/alias", 100); err == nil {
		t.Fatal("symlink record accepted")
	}
	if err := os.Link(files.Path("/records/current"), files.Path("/records/shared")); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Read("/records/shared", 100); !errors.Is(err, infrapriv.ErrPermission) {
		t.Fatalf("multiply linked record accepted: %v", err)
	}
	if err := os.Remove(files.Path("/records/shared")); err != nil {
		t.Fatal(err)
	}
	if err := files.Replace("/records/current", []byte(`{"value":"verified","value":"verified"}`), 0600, gid); err != nil {
		t.Fatal(err)
	}
	if err := files.Decode("/records/current", &got); !errors.Is(err, c.ErrIncomplete) {
		t.Fatalf("duplicate JSON fields accepted: %v", err)
	}
	exerciseInstalledDescriptorCustody(t, files, gid)
	unlock, err := files.Lock("/records/lock")
	if err != nil {
		t.Fatal(err)
	}
	if second, err := files.Lock("/records/lock"); !errors.Is(err, infrapriv.ErrBusy) {
		if second != nil {
			second()
		}
		unlock()
		t.Fatalf("second namespace lock accepted: %v", err)
	}
	unlock()
	if err := os.Mkdir(files.Path("/service"), 0700); err != nil {
		t.Fatal(err)
	}
	service, err := files.ServiceDirectory("/service", owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("service", files.Path("/service-alias")); err != nil {
		t.Fatal(err)
	}
	if dir, err := files.ServiceDirectory("/service-alias", owner); err == nil {
		closeTestResource(t, dir)
		t.Fatal("symlink service directory accepted")
	}
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}

// The composition root lends exact descriptors: hashing cannot consume their
// offsets, and Stage custody cannot reopen a pathname after its replacement.
// origin: hot-update S10 lint-L1 / structural cap preservation
func exerciseInstalledDescriptorCustody(t *testing.T, files *Files, gid uint32) {
	t.Helper()
	original := []byte("original immutable stage bytes")
	if err := files.Replace("/records/custody", original, 0600, gid); err != nil {
		t.Fatal(err)
	}
	input, err := files.Open("/records/custody", 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, input)
	if _, err := input.Seek(2, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := files.Replace("/records/custody", []byte("replacement pathname bytes"), 0600, gid); err != nil {
		t.Fatal(err)
	}
	duplicate, err := Duplicate(input)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, duplicate)
	flags, err := unix.FcntlInt(duplicate.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("duplicate lost CLOEXEC custody: flags=%d err=%v", flags, err)
	}
	if _, err := duplicate.Seek(5, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if offset, err := input.Seek(0, io.SeekCurrent); err != nil || offset != 5 {
		t.Fatalf("duplicate reopened an independent description: offset=%d err=%v", offset, err)
	}
	if digest, err := HashOpened(duplicate); err != nil || digest != sha256.Sum256(original) {
		t.Fatalf("hash did not use the complete original descriptor: %x err=%v", digest, err)
	}
	if offset, err := input.Seek(0, io.SeekCurrent); err != nil || offset != 5 {
		t.Fatalf("hash consumed the shared offset: offset=%d err=%v", offset, err)
	}
}
