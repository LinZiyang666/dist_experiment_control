package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type failedArchiveCloser struct{ calls int }

func (c *failedArchiveCloser) Close() error {
	c.calls++
	return &os.PathError{Op: "close", Path: "private-archive-path", Err: os.ErrPermission}
}

// A successful read cannot certify evidence when its close failed; neither
// that failure nor a prior guard error may expose a closer's private path.
// origin: hot-update S10 plan §5.1 / LINT-s10
func TestEvidenceCloseFailureRejectsArchive(t *testing.T) {
	prior := errors.New("prior archive guard")
	for _, initial := range []error{nil, prior} {
		closer := &failedArchiveCloser{}
		err := initial
		closeArchiveResource(&err, closer, "cannot close fullstack archive document")
		if closer.calls != 1 || err == nil || !strings.Contains(err.Error(), "cannot close fullstack archive document") {
			t.Fatalf("close failure was lost: calls=%d err=%v", closer.calls, err)
		}
		if initial != nil && !errors.Is(err, prior) {
			t.Fatal("close failure replaced the prior guard failure")
		}
		if strings.Contains(err.Error(), "private-archive-path") {
			t.Fatal("close diagnostic exposed the archive path")
		}
	}
}

// Archive readers own opened files but borrow the confined root, which must
// remain usable after successful reads and rejected paths alike.
// origin: hot-update S10 plan §5.1 / LINT-s10
func TestEvidenceArchiveReadKeepsConfinedRoot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "evidence.json"), []byte("archive bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, name := range []string{"missing", ".", "../outside", "evidence.json"} {
		data, err := readArchiveFile(root, name)
		if name == "evidence.json" {
			if err != nil || string(data) != "archive bytes" {
				t.Fatalf("archive read: %q %v", data, err)
			}
		} else if err == nil {
			t.Fatalf("invalid archive path accepted: %s", name)
		}
	}
	if _, err := root.Stat("evidence.json"); err != nil {
		t.Fatal("archive read closed its borrowed root")
	}
}
