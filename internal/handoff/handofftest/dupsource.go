//go:build linux

// dupsource.go — an in-process stand-in for the descriptors a handoff
// dispatch would hand a new image: independent F_DUPFD_CLOEXEC copies of
// a cut's borrowed files, offered through the execsession.FileSource shape
// (TakeFile) plus Borrow for the re-dispatch path. It lets the exact
// bootstrap code the exec path runs be driven from a test without an exec.
// A subpackage (like net/http/httptest) so handoff's own tests, which import
// internal/testharness, do not import themselves through it.
package handofftest

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"golang.org/x/sys/unix"
)

// DupSource holds duplicated files by resource ID and name.
type DupSource struct {
	mu    sync.Mutex
	kinds map[string]string
	data  map[string][]byte
	order []string
	files map[string]map[string]*os.File
}

// DupResources duplicates every file of resources (CLOEXEC copies; the
// originals stay with their owners) into a DupSource closed at cleanup.
func DupResources(t *testing.T, resources []handoff.Resource) *DupSource {
	t.Helper()
	src := &DupSource{kinds: map[string]string{}, data: map[string][]byte{}, files: make(map[string]map[string]*os.File, len(resources))}
	for _, res := range resources {
		files := make(map[string]*os.File, len(res.Files))
		for name, file := range res.Files {
			raw, err := file.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			dup, dupErr := -1, error(nil)
			if err := raw.Control(func(fd uintptr) { dup, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || dupErr != nil {
				t.Fatalf("dup %s/%s: %v %v", res.ID, name, err, dupErr)
			}
			files[name] = os.NewFile(uintptr(dup), "dup-"+res.Kind+"-"+name)
		}
		src.files[res.ID] = files
		src.kinds[res.ID] = res.Kind
		src.data[res.ID] = append([]byte(nil), res.Data...)
		src.order = append(src.order, res.ID)
	}
	t.Cleanup(src.Close)
	return src
}

// TakeFile transfers one duplicated file to the caller (execsession.FileSource).
func (s *DupSource) TakeFile(id, name string) (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[id][name]
	if f == nil {
		return nil, fmt.Errorf("dup source: %s/%s unavailable", id, name)
	}
	delete(s.files[id], name)
	return f, nil
}

// Borrow returns the files not yet taken, as handoff.Restored.Borrow would.
func (s *DupSource) Borrow() []handoff.Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []handoff.Resource
	for _, id := range s.order {
		files := s.files[id]
		if len(files) == 0 {
			continue
		}
		borrowed := make(map[string]*os.File, len(files))
		for name, f := range files {
			borrowed[name] = f
		}
		out = append(out, handoff.Resource{ID: id, Kind: s.kinds[id], Data: s.data[id], Files: borrowed})
	}
	return out
}

// Remaining counts the files not yet taken.
func (s *DupSource) Remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, files := range s.files {
		n += len(files)
	}
	return n
}

// Swap exchanges two files of one resource (a same-shape identity mismatch).
func (s *DupSource) Swap(id, a, b string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	files := s.files[id]
	files[a], files[b] = files[b], files[a]
}

// Drop closes and forgets one file (a declared file that is absent).
func (s *DupSource) Drop(id, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.files[id][name]; f != nil {
		_ = f.Close()
		delete(s.files[id], name)
	}
}

// Close releases every file not yet taken.
func (s *DupSource) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, files := range s.files {
		for name, f := range files {
			_ = f.Close()
			delete(files, name)
		}
	}
}
