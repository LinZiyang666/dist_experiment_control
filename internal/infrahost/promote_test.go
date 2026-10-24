//go:build linux

package infrahost

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

type artifactOpener func(infracontract.Artifact, infracontract.Digest, infracontract.Deadline) (*os.File, error)

func (open artifactOpener) Open(a infracontract.Artifact, p infracontract.Digest, d infracontract.Deadline) (*os.File, error) {
	return open(a, p, d)
}

// origin: hot-update S9 plan §6.2 / T10
func TestInfraPromotionCopiesBeforeHash(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := openPrivateDirectory(t.TempDir()); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatal("unsupported platform accepted", err)
		}
		return
	}
	t.Run("retained writable source never aliases executable", func(t *testing.T) {
		f := newHostFixture(t)
		r := f.request(t, infracontract.Stage, 1)
		source, err := f.backend.source.Open(r.Candidate, r.Payload, testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		path := source.Name()
		_ = source.Close()
		writer, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		// The retained attacker descriptor; each WriteAt below is checked where it
		// matters, and its close has no bearing on the installed copy.
		defer func() { _ = writer.Close() }()
		f.run(t, r)
		name := "binary-" + fmtDigest(r.Candidate.BinarySHA256)
		slot := filepath.Join(f.path, name)
		before, err := os.Stat(slot)
		if err != nil {
			t.Fatal(err)
		}
		src, err := writer.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if os.SameFile(before, src) || before.Mode().Perm() != 0755 {
			t.Fatal("source inode promoted")
		}
		if _, err := writer.WriteAt([]byte("attacker mutable bytes!!!!!"), 0); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(slot)
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(data) != r.Candidate.BinarySHA256 {
			t.Fatal("retained source fd changed installed bytes")
		}
		if src.Mode().Perm() != 0600 {
			t.Fatal("staging modified source mode")
		}
		f.run(t, f.request(t, infracontract.FenceStart, 2))
		promote := f.request(t, infracontract.Promote, 3)
		f.run(t, promote)
		s, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if s.Slots[infracontract.CaddyBinary].Active != r.Candidate || s.Slots[infracontract.CaddyBinary].Previous.BinarySHA256 != digestOf("old") || !s.StartupFence {
			t.Fatal("promotion lost selector or startup fence")
		}
	})
	t.Run("source write race cannot authenticate different copied bytes", func(t *testing.T) {
		f := newHostFixture(t)
		data := bytes.Repeat([]byte{0xa1}, 8<<20)
		path := filepath.Join(t.TempDir(), "racing-source")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		f.backend.source = testSource{path}
		f.artifact.BinarySHA256 = sha256.Sum256(data)
		writer, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = writer.Close() }() // Retained attacker descriptor; see above.
		stop, started := make(chan struct{}), make(chan struct{})
		var joined sync.WaitGroup
		joined.Add(1)
		go func() {
			defer joined.Done()
			dirty := bytes.Repeat([]byte{0xb2}, 64<<10)
			_, _ = writer.WriteAt(dirty, 0)
			close(started)
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = writer.WriteAt(dirty, 0)
				}
			}
		}()
		<-started
		r := f.request(t, infracontract.Stage, 1)
		_, err = f.worker.Do(r, testDeadline(t), nil)
		close(stop)
		joined.Wait()
		if !errors.Is(err, ErrArtifact) {
			t.Fatal("racing corrupt source accepted", err)
		}
		s, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if s.Slots[infracontract.CaddyBinary].Pending != (infracontract.Artifact{}) {
			t.Fatal("failed hash selected candidate")
		}
	})
	t.Run("writable FD changes after candidate copy starts", func(t *testing.T) {
		f := newHostFixture(t)
		path := filepath.Join(t.TempDir(), "retained-fd")
		{
			// Block-scoped so the 64 MiB source image is visibly dead before the
			// race starts; a trailing `data = nil` was a no-op store.
			data := bytes.Repeat([]byte{0xa1}, 64<<20)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			f.artifact.BinarySHA256 = sha256.Sum256(data)
		}
		writer, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = writer.Close() }() // Retained attacker descriptor; see above.
		opened, armed := make(chan struct{}), make(chan struct{})
		f.backend.source = artifactOpener(func(infracontract.Artifact, infracontract.Digest, infracontract.Deadline) (*os.File, error) {
			close(opened)
			<-armed
			return os.Open(path)
		})
		r := f.request(t, infracontract.Stage, 1)
		r.RemainingNS = uint64(10 * time.Second)
		r = f.sign(t, r)
		d, err := controlbudget.After(10 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := f.worker.Do(r, d, nil); done <- err }()
		<-opened
		// Watch only after the durable intent. The next private inode belongs
		// to the artifact copy, so a hash-before-copy mutant sees clean source
		// bytes and then copies a tail changed through the retained original FD.
		watch, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
		if err != nil {
			close(armed)
			<-done
			t.Fatal(err)
		}
		defer func() { _ = unix.Close(watch) }() // A read-only inotify descriptor.
		if _, err := unix.InotifyAddWatch(watch, f.path, unix.IN_CREATE); err != nil {
			close(armed)
			<-done
			t.Fatal(err)
		}
		close(armed)
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		event := make([]byte, 4096)
	waitCreate:
		for {
			n, readErr := unix.Read(watch, event)
			if n > 0 {
				break
			}
			if readErr != nil && !errors.Is(readErr, unix.EAGAIN) {
				t.Error(readErr)
				break
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Error("artifact inode was not created")
				break waitCreate
			}
		}
		if _, err := writer.WriteAt(bytes.Repeat([]byte{0xb2}, 64<<10), (64<<20)-(64<<10)); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, ErrArtifact) {
			t.Fatal("source prehash authenticated different copied bytes", err)
		}
		entries, err := os.ReadDir(f.path)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Name() == "binary-"+fmtDigest(r.Candidate.BinarySHA256) {
				t.Fatal("TOCTOU installed corrupt bytes")
			}
		}
	})
	t.Run("slot paths cannot redirect privileged writes", func(t *testing.T) {
		for _, kind := range []string{"symlink-directory", "symlink-journal", "hardlink-journal", "symlink-slot", "hardlink-slot", "wrong-mode"} {
			t.Run(kind, func(t *testing.T) {
				f := newHostFixture(t)
				outside := filepath.Join(t.TempDir(), "outside")
				original := []byte("outside must stay unchanged")
				mode := os.FileMode(0600)
				if kind == "symlink-slot" || kind == "hardlink-slot" {
					original = []byte("sandbox executable contents")
					mode = 0755
				}
				if kind == "symlink-journal" || kind == "hardlink-journal" {
					var err error
					original, err = os.ReadFile(filepath.Join(f.path, "journal.json"))
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(outside, original, mode); err != nil {
					t.Fatal(err)
				}
				var err error
				switch kind {
				case "symlink-directory":
					link := filepath.Join(t.TempDir(), "link")
					if err := os.Symlink(f.path, link); err != nil {
						t.Fatal(err)
					}
					j, openErr := OpenJournal(link, "test-boot", infracontract.ID{2}, f.authority)
					err = openErr
					if j != nil {
						_ = j.Close()
					}
				case "symlink-journal", "hardlink-journal":
					target := filepath.Join(f.path, "journal.json")
					if err := os.Remove(target); err != nil {
						t.Fatal(err)
					}
					if kind == "symlink-journal" {
						err = os.Symlink(outside, target)
					} else {
						err = os.Link(outside, target)
					}
					if err != nil {
						t.Fatal(err)
					}
					_, _, err = f.journal.Read(testDeadline(t))
				case "symlink-slot", "hardlink-slot":
					r := f.request(t, infracontract.Stage, 1)
					target := filepath.Join(f.path, "binary-"+fmtDigest(r.Candidate.BinarySHA256))
					if kind == "symlink-slot" {
						err = os.Symlink(outside, target)
					} else {
						err = os.Link(outside, target)
					}
					if err != nil {
						t.Fatal(err)
					}
					_, err = f.worker.Do(r, testDeadline(t), nil)
				case "wrong-mode":
					if err := os.Chmod(f.path, 0777); err != nil {
						t.Fatal(err)
					}
					j, openErr := OpenJournal(f.path, "test-boot", infracontract.ID{2}, f.authority)
					err = openErr
					if j != nil {
						_ = j.Close()
					}
				}
				if err == nil {
					t.Fatal("unsafe path accepted")
				}
				after, readErr := os.ReadFile(outside)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if !bytes.Equal(original, after) {
					t.Fatal("outside file changed")
				}
			})
		}
	})
	t.Run("promote refuses missing fence or corrupt immutable bytes", func(t *testing.T) {
		f := newHostFixture(t)
		f.run(t, f.request(t, infracontract.Stage, 1))
		r := f.request(t, infracontract.Promote, 2)
		if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
			t.Fatal("promotion bypassed startup fence", err)
		}
		f.run(t, f.request(t, infracontract.FenceStart, 2))
		wrong := f.request(t, infracontract.Promote, 3)
		wrong.Expected.ExeSHA256 = digestOf("unrelated incumbent")
		wrong = f.sign(t, wrong)
		if _, err := f.worker.Do(wrong, testDeadline(t), nil); !errors.Is(err, ErrCAS) {
			t.Fatal("promotion displaced an unrelated incumbent", err)
		}
		slot := filepath.Join(f.path, "binary-"+fmtDigest(f.artifact.BinarySHA256))
		if err := os.WriteFile(slot, []byte("bad"), 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := f.worker.Do(f.request(t, infracontract.Promote, 3), testDeadline(t), nil); !errors.Is(err, ErrArtifact) {
			t.Fatal("promotion trusted stale hash", err)
		}
	})
	t.Run("duplicate stage is content immutable and unsupported rejects", func(t *testing.T) {
		f := newHostFixture(t)
		r := f.request(t, infracontract.Stage, 1)
		f.run(t, r)
		slot := filepath.Join(f.path, "binary-"+fmtDigest(r.Candidate.BinarySHA256))
		before, err := os.Stat(slot)
		if err != nil {
			t.Fatal(err)
		}
		f.run(t, f.request(t, infracontract.Stage, 2))
		after, err := os.Stat(slot)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) {
			t.Fatal("idempotent stage replaced inode")
		}
		r = f.request(t, infracontract.Stage, 3)
		r.Candidate.GOARCH = "arm64"
		r = f.sign(t, r)
		if _, err := f.worker.Do(r, testDeadline(t), nil); err == nil {
			t.Fatal("unsupported artifact accepted")
		}
	})
}

func fmtDigest(d infracontract.Digest) string {
	const alphabet = "0123456789abcdef"
	var b [64]byte
	for i, v := range d {
		b[2*i], b[2*i+1] = alphabet[v>>4], alphabet[v&15]
	}
	return string(b[:])
}
