package sealedstate

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
)

// origin: hot-update S5 plan §5.3 / T51
func TestSealedStateCloseKeepsBorrowedResources(t *testing.T) {
	borrowed, peer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(borrowed.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	owned, ownedPeer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(owned.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(ownedPeer.Close)
	s := &Snapshot{Resources: []handoff.Resource{{ID: "flow.cut", Kind: "raw", Files: map[string]*os.File{"raw": borrowed, "state": owned}}}, Owned: []*os.File{nil, owned}}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owned.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("owned descriptor was not closed: %v", err)
	}
	if _, err := borrowed.Stat(); err != nil {
		t.Fatalf("Close closed a borrowed business descriptor: %v", err)
	}
	if _, err := peer.Write([]byte{42}); err != nil {
		t.Fatal(err)
	}
	var got [1]byte
	if _, err := borrowed.Read(got[:]); err != nil || got[0] != 42 {
		t.Fatalf("borrowed pipe stopped carrying business bytes: %v", err)
	}
	if s.Owned != nil || len(s.Resources) != 1 || s.Resources[0].Files["raw"] != borrowed {
		t.Fatal("Close did not retire Owned independently of Resources")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if err := (*Snapshot)(nil).Close(); err != nil {
		t.Fatalf("nil Close = %v", err)
	}
}

// origin: hot-update S5 plan §5.3 / §6.6
func TestSealedStateCloseContinuesAfterRedactedFailure(t *testing.T) {
	failed, err := os.CreateTemp(t.TempDir(), "private-high-entropy-material-")
	if err != nil {
		t.Fatal(err)
	}
	if err := failed.Close(); err != nil {
		t.Fatal(err)
	}
	live, peer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(live.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	s := &Snapshot{Owned: []*os.File{failed, live}}
	err = s.Close()
	if !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), failed.Name()) {
		t.Fatalf("Close error did not preserve the redacted failure class: %v", err)
	}
	if _, err := live.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close stopped before releasing the next descriptor: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed Close retained ownership: %v", err)
	}
}

type stateFileSource struct {
	file  *os.File
	err   error
	calls int
}

func (s *stateFileSource) TakeFile(id, name string) (*os.File, error) {
	s.calls++
	file := s.file
	s.file = nil
	return file, s.err
}

// On portable systems rejection must precede even an invalid resource's Take.
// Linux coverage of these bodies uses a build overlay, separately from Darwin run.
// origin: hot-update S5 plan §7.4
func TestSealedStatePortableRefusesBeforeOwnership(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("portable bodies are exercised through a Linux build overlay")
	}
	src := &stateFileSource{}
	if data, file, err := Read(src, "ss-state.cut", "state", MaxStateBytes); data != nil || file != nil || !errors.Is(err, ErrUnsupported) || src.calls != 0 {
		t.Fatalf("portable Read granted ownership: calls=%d err=%v", src.calls, err)
	}
	if s, err := New("ss-state.cut", []byte{1}); s != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("portable New granted ownership: %v", err)
	}
}
