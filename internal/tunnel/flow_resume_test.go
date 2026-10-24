//go:build linux

package tunnel

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func initialOwner(t *testing.T, f *endpointFixture) (resumeRequest, resumeResponse, *flowCarrier) {
	t.Helper()
	a := testCarrier(t, f.s.flowState.manager, f.e.id)
	req := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeInitial, RequestID: [16]byte{1}, Owner: 1, Target: 22}
	resp, _, err := resumeEndpoint(f.s, f.e, req, a, time.Now().Add(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	return req, resp, a
}

// origin: hot-update S5 plan §4.3 / T13
func exerciseResumeReRunsTokenLookupAndFence(t *testing.T) {
	for _, which := range []string{"authority", "port", "session", "forgotten", "allocation"} {
		t.Run(which, func(t *testing.T) {
			f := endpointSetup(t)
			defer f.s.Close()
			req, _, a := initialOwner(t, f)
			defer a.Close()
			first := f.lookups.Load()
			if first == 0 {
				t.Fatal("RESUME omitted token lookup")
			}
			if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if f.lookups.Load() != first+1 {
				t.Fatal("cached success bypassed token lookup")
			}
			f.s.mu.Lock()
			switch which {
			case "authority":
				f.allowed.Store(false)
			case "port":
				f.s.killGen[0]++
			case "session":
				f.s.killGenSession["sid"]++
			case "forgotten":
				f.s.forgotten["sid"] = true
				f.s.maybePruneSessionLocked("sid")
				if !f.s.forgotten["sid"] {
					f.s.mu.Unlock()
					t.Fatal("endpoint owner lost forgotten fence")
				}
			case "allocation":
				key := allocationKey(f.e.allocation)
				f.s.closedAllocation[key] = true
				f.s.maybePruneAllocationLocked(key)
				if !f.s.closedAllocation[key] {
					f.s.mu.Unlock()
					t.Fatal("endpoint owner lost allocation fence")
				}
			}
			f.s.mu.Unlock()
			if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(time.Second), nil); err == nil {
				t.Fatal("cached success bypassed fresh authority or fence")
			}
		})
	}
}

// origin: hot-update S5 plan §4.4 / T13
func exerciseCurrentOwnerTicketCoversLongIdleAndFirstDetach(t *testing.T) {
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	expired, err := clientBootDeadline(uint64(now - 1))
	if err != nil || expired.After(time.Now()) {
		t.Fatal("import renewed expired first-detach deadline")
	}
	if _, err = clientBootDeadline(^uint64(0)); err == nil {
		t.Fatal("import accepted overflowing boot deadline")
	}
	f := endpointSetup(t)
	defer f.s.Close()
	_, resp, a := initialOwner(t, f)
	f.e.createdAt = time.Now().Add(-2 * PermitTTL)
	if !bytes.Equal(resp.Ticket, ticketFor(f.s, f.e, 1)) {
		t.Fatal("current-owner ticket acquired idle TTL")
	}
	a.Close()
	a.Join()
	detachEndpoint(f.s, f.e, a)
	f.s.mu.Lock()
	first := f.e.detachDeadline
	f.s.mu.Unlock()
	if first.IsZero() {
		t.Fatal("first detach did not arm grace")
	}
	request := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeReconnect, RequestID: [16]byte{2}, Endpoint: f.e.id, Owner: 1, Target: 22, Ticket: append([]byte(nil), resp.Ticket...)}
	f.allowed.Store(false)
	if _, _, err := resumeEndpoint(f.s, f.e, request, nil, time.Now().Add(time.Second), nil); err == nil {
		t.Fatal("unauthorized reconnect succeeded")
	}
	if _, err := reserveEndpoint(f.s, f.e.allocation, f.e.fence); err != nil {
		t.Fatal(err)
	}
	f.s.mu.Lock()
	after := f.e.detachDeadline
	f.s.mu.Unlock()
	if !first.Equal(after) {
		t.Fatal("failed reconnect renewed first detach grace")
	}
	f.allowed.Store(true)
	next := testCarrier(t, f.s.flowState.manager, f.e.id)
	if _, _, err := resumeEndpoint(f.s, f.e, request, next, time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	next.Close()
	next.Join()
}

// origin: hot-update S5 plan §4.4 / T13
func exercisePrepareActivateLostReplyHasOneOwner(t *testing.T) {
	exerciseActivationResourceDigest(t)
	f := endpointSetup(t)
	defer f.s.Close()
	_, initial, old := initialOwner(t, f)
	prepare := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModePrepare, RequestID: [16]byte{2}, Endpoint: f.e.id, Owner: 1, Next: 2, Cut: [16]byte{3}, Target: 22, Ticket: initial.Ticket}
	if _, _, err := resumeEndpoint(f.s, f.e, prepare, nil, time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if f.e.ownerGen != 1 || f.e.attachment != old {
		t.Fatal("prepare transferred business ownership")
	}
	deadline := f.e.permit.deadline
	if _, _, err := resumeEndpoint(f.s, f.e, prepare, nil, time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !deadline.Equal(f.e.permit.deadline) {
		t.Fatal("prepare retry renewed permit")
	}
	activate := prepare
	activate.Mode = flow.ResumeModeActivate
	activate.RequestID = [16]byte{4}
	activate.Digest = [32]byte{5}
	candidate := testCarrier(t, f.s.flowState.manager, f.e.id)
	f.s.mu.Lock()
	priorGen := f.e.carrierGen
	f.e.carrierGen = ^uint64(0)
	f.s.mu.Unlock()
	if _, _, err := resumeEndpoint(f.s, f.e, activate, candidate, time.Now().Add(time.Second), nil); err == nil || f.e.ownerGen != 1 || f.e.permit.consumed {
		t.Fatal("carrier overflow consumed handoff ownership")
	}
	f.s.mu.Lock()
	f.e.carrierGen = priorGen
	f.s.mu.Unlock()
	first, prior, err := resumeEndpoint(f.s, f.e, activate, candidate, time.Now().Add(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if prior != old || first.Owner != 2 || !f.e.permit.consumed {
		t.Fatal("activate did not consume exactly one permit")
	}
	old.Close()
	old.Join()
	cached, _, err := resumeEndpoint(f.s, f.e, activate, nil, time.Now().Add(time.Second), nil)
	if err != nil || cached.Carrier != first.Carrier || cached.Owner != 2 {
		t.Fatalf("lost reply consumed permit twice: %+v %v", cached, err)
	}
	other := activate
	other.RequestID = [16]byte{8}
	if _, _, err = resumeEndpoint(f.s, f.e, other, nil, time.Now().Add(time.Second), nil); err == nil {
		t.Fatal("second activate consumed used permit")
	}
	f.allowed.Store(false)
	if _, _, err = resumeEndpoint(f.s, f.e, activate, nil, time.Now().Add(time.Second), nil); err == nil {
		t.Fatal("cached activate skipped reauthorization")
	}
	candidate.Close()
	candidate.Join()
}

// origin: hot-update S5 plan §8 / TestResumeReRunsTokenLookupAndFence
func TestResumeReRunsTokenLookupAndFence(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseResumeReRunsTokenLookupAndFence(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestResumeReRunsTokenLookupAndFence", beforeG)
	testharness.AssertNoFDLeak(t, "TestResumeReRunsTokenLookupAndFence", beforeFD, 0)
}

// origin: hot-update S5 plan §8 / TestCurrentOwnerTicketCoversLongIdleAndFirstDetach
func TestCurrentOwnerTicketCoversLongIdleAndFirstDetach(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseCurrentOwnerTicketCoversLongIdleAndFirstDetach(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestCurrentOwnerTicketCoversLongIdleAndFirstDetach", beforeG)
	testharness.AssertNoFDLeak(t, "TestCurrentOwnerTicketCoversLongIdleAndFirstDetach", beforeFD, 0)
}

// origin: hot-update S5 plan §8 / TestPrepareActivateLostReplyHasOneOwner
func TestPrepareActivateLostReplyHasOneOwner(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exercisePrepareActivateLostReplyHasOneOwner(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestPrepareActivateLostReplyHasOneOwner", beforeG)
	testharness.AssertNoFDLeak(t, "TestPrepareActivateLostReplyHasOneOwner", beforeFD, 0)
}

// A complete authority snapshot is indivisible even if a later page is valid
// in isolation. These checks exercise P1 body ownership without a fake P2 codec.
// origin: hot-update S5 plan §2.2 / §4.3 / T13

// Import hashes regular resources without touching their offsets. The sealed
// route and replay content must both participate in the activation CAS digest.
// origin: hot-update S5 plan §4.4 / T13
func exerciseActivationResourceDigest(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "digest-resource")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(file.Close)
	if _, err = file.Write([]byte("retained-bytes")); err != nil {
		t.Fatal(err)
	}
	cut := sealedstate.CutState{Resources: []handoff.Resource{{ID: "resource", Kind: "flow-v1", Files: map[string]*os.File{"replay": file}}}}
	first, err := clientCutDigest(cut)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte("changed-bytes!"), 0); err != nil {
		t.Fatal(err)
	}
	second, err := clientCutDigest(cut)
	if err != nil || first == second {
		t.Fatal("activation digest omitted retained resource")
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil || position != int64(len("retained-bytes")) {
		t.Fatal("digest mutated borrowed file offset")
	}
}
