//go:build linux

package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §6.3 / T28 / TESTFIX-2
func TestFlowRegisterBindFailurePreservesRefusalAndSurvivor(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseFlowRegisterBindFailure(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "flow REGISTER bind refusal", g)
	testharness.AssertNoFDLeak(t, "flow REGISTER bind refusal", fd, 0)
}

func exerciseFlowRegisterBindFailure(t *testing.T) {
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reservation.Close() }()
	port := reservation.Addr().(*net.TCPAddr).Port
	release := sync.OnceValue(reservation.Close)
	var allowBind, revoked atomic.Bool
	f := settlementSetup(t, func(r AuthRequest) error {
		if r.Allocation.Port == port {
			if revoked.Load() {
				return errFlowAuth
			}
			if allowBind.Load() {
				return release()
			}
		}
		return nil
	})
	t.Cleanup(func() { closeClientEndpoint(f.client, port) })
	public, target, _ := settlementConnection(t, f, 1)
	ctx, cancel := context.WithTimeout(t.Context(), ResumeAttempt)
	defer cancel()
	_, _, err = f.client.dialAndRegister(ctx, port, "token", "", 0, proto.CertPins{})
	var deny *DenyError
	if !errors.As(err, &deny) || deny.Reason != "public_port_bind_failed" {
		t.Fatalf("occupied public port became a terminal revocation: %v", err)
	}
	f.server.mu.Lock()
	retained := f.server.flowState.endpoints[port] != nil
	f.server.mu.Unlock()
	if retained {
		t.Fatal("failed bind retained an Endpoint reservation")
	}
	allowBind.Store(true)
	if _, _, err = f.client.dialAndRegister(ctx, port, "token", "", 0, proto.CertPins{}); err != nil {
		t.Fatalf("authorized bind did not recover after collision: %v", err)
	}
	closeClientEndpoint(f.client, port)
	_, _, err = f.client.dialAndRegister(ctx, port, "replacement-token", "", 0, proto.CertPins{})
	if !errors.As(err, &deny) || deny.Reason != "session_closed" {
		t.Fatalf("allocation conflict was mislabeled as a bind collision: %v", err)
	}
	revoked.Store(true)
	_, _, err = f.client.dialAndRegister(ctx, port, "token", "", 0, proto.CertPins{})
	if !errors.As(err, &deny) || deny.Reason != "session_closed" {
		t.Fatalf("authority refusal lost its terminal classification: %v", err)
	}
	if _, err := public.Write([]byte("survivor")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("survivor"))
	if _, err := io.ReadFull(target, got); err != nil || string(got) != "survivor" {
		t.Fatalf("unrelated original flow lost after refused REGISTER: %q / %v", got, err)
	}
}
