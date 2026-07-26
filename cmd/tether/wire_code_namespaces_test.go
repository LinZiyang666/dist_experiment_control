package main

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// TestWireCodeNamespacesAgree backs the claim internal/proto/codes.go makes in
// its package doc: the codes that travel on BOTH the NATS control plane and the
// local admin socket are declared in both packages and pinned to identical
// values.
//
// Batch-A review B2. That sentence was written in the present tense while no
// such test existed — the same defect A4 had just finished removing from
// proto.RehomeDirective, whose doc claimed "a guard test asserts it has no live
// publisher" when none did. Writing the promise and not the test, in the very
// batch that fixed an instance of it, is why this file exists.
//
// proto cannot import adminsock (the dependency runs the other way), so nothing
// in either package can enforce this. cmd/tether is the only place that imports
// both, which is why the guard lives here rather than next to the constants.
//
// A divergence here is not a compile error and not a runtime error: the broker
// would emit one spelling on NATS while the CLI matched another on the socket,
// and the failure would surface as an exit code quietly reverting to 70.
func TestWireCodeNamespacesAgree(t *testing.T) {
	shared := []struct {
		name  string
		proto string
		admin string
	}{
		{"store_error", proto.CodeStoreError, adminsock.CodeStoreError},
		{"bad_request", proto.CodeBadRequest, adminsock.CodeBadRequest},
	}

	for _, tc := range shared {
		t.Run(tc.name, func(t *testing.T) {
			if tc.proto != tc.admin {
				t.Errorf("wire value diverged: proto=%q adminsock=%q.\n"+
					"Both transports carry this code; a broker emitting one spelling while the CLI "+
					"matches the other silently drops the exit class back to 70.", tc.proto, tc.admin)
			}
			if tc.proto != tc.name {
				t.Errorf("proto constant is %q, expected the wire literal %q — the string itself is "+
					"the protocol, so renaming the constant is free but changing its value is a wire break",
					tc.proto, tc.name)
			}
		})
	}

	// Non-vacuity: if someone deletes the shared block from proto/codes.go, the
	// loop above silently tests nothing. Assert the set is non-empty and that
	// every entry is actually classified, which is the point of sharing them.
	if len(shared) == 0 {
		t.Fatal("no shared codes under test; this guard has become vacuous")
	}
	for _, tc := range shared {
		if _, ok := brokerCodeExitClasses[tc.proto]; !ok {
			t.Errorf("shared code %q has no exit class — a code that travels on both transports and "+
				"classifies on neither is the worst of both", tc.proto)
		}
	}
}
