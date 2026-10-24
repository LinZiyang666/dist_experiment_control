package sealedstate

import (
	"errors"
	"os"
	"runtime"
	"testing"
)

// Preserve the P0 test identity while checking the implemented owner contract.
// Domain codecs and carrier owners are outside this test's scope.
// origin: hot-update S5 plan §2 / §12.2 P0
func TestSealedStateContractStubsRejectOperations(t *testing.T) {
	want := ErrInvalid
	if runtime.GOOS != "linux" {
		want = ErrUnsupported
	}
	if data, file, err := Read(nil, "flow-state.cut", "state", MaxStateBytes); data != nil || file != nil || !errors.Is(err, want) {
		t.Fatalf("Read invalid source = (%v, %v, %v)", data, file, err)
	}
	s, err := New("flow-state.cut", []byte{1})
	if runtime.GOOS != "linux" {
		if s != nil || !errors.Is(err, ErrUnsupported) {
			t.Fatalf("portable New granted ownership: %v", err)
		}
		return
	}
	if err != nil || s == nil || len(s.Resources) != 1 || len(s.Owned) != 1 || s.Resources[0].Files["state"] != s.Owned[0] {
		t.Fatalf("New did not publish one cut-owned state descriptor: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	owned := s.Owned[0]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owned.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("Close retained the owned state descriptor")
	}
}
