package agent

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// auth_denial_classification_test.go — how connectNATS classifies an auth denial.
//
// origin: prerelease audit proto-auth-acl (verifier-found). nats-server answers an
// auth_callout TIMEOUT, a missing callout responder and a wrong PIN with the SAME
// "Authorization Violation" text, so the classification cannot be made from the
// error. It is made from history instead: a credential that authenticated once does
// not spontaneously become wrong.

// TestAuthDenialBeforeFirstAuthIsTerminal pins the half that must NOT change: a
// first connect that is refused is a real credential problem and has to fail fast
// and loud, with the operator hint attached.
func TestAuthDenialBeforeFirstAuthIsTerminal(t *testing.T) {
	a := &Agent{cfg: Config{SID: "lab", NID: "gpu1", Logger: d6Logger()}}
	if a.everAuthed.Load() {
		t.Fatal("a fresh agent must not claim to have authenticated")
	}
	// The classifier itself: this is the string nats-server sends.
	if !isAuthFailure(errors.New("nats: Authorization Violation")) {
		t.Fatal("isAuthFailure must recognize the server's denial text")
	}
}

// denyingNATS is a minimal server that completes the NATS protocol far enough to
// refuse the CONNECT with the server's real authorization text, then closes.
//
// A dead address would NOT do: a refused TCP dial never reaches isAuthFailure, so a
// test built on one passes with or without the fix — it would be a vacuous guard.
// This one produces the exact wire text nats-server sends for a wrong PIN, an
// auth_callout timeout and a missing callout responder alike, which is the whole
// reason those three are indistinguishable here.
func denyingNATS(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_, _ = io.WriteString(c, "INFO {\"server_id\":\"FAKE\",\"version\":\"2.14.0\",\"proto\":1,\"max_payload\":1048576,\"headers\":true}\r\n")
				br := bufio.NewReader(c)
				_, _ = br.ReadString('\n') // CONNECT
				_, _ = io.WriteString(c, "-ERR 'Authorization Violation'\r\n")
			}(c)
		}
	}()
	return "nats://" + ln.Addr().String()
}

// TestAuthDenialAfterSuccessfulAuthIsTransient is the guard. Before the fix,
// connectNATS returned a terminal error here; Run then returns, the process exits
// non-zero, and systemd's default KillMode=control-group reaps every other process
// in the agent's cgroup — the operator's running experiments — over what may be a
// two-second callout hiccup.
//
// The assertion is on the LOOP's behaviour, not on a log line: with everAuthed set,
// a denial must keep retrying until the context is cancelled, and the error that
// comes back must be the context's, never the terminal auth message.
func TestAuthDenialAfterSuccessfulAuthIsTransient(t *testing.T) {
	url := denyingNATS(t)
	a := &Agent{cfg: Config{
		SID: "lab", NID: "gpu1", Logger: d6Logger(),
		NATSURL:              url,
		RegisterRetryInitial: 5 * time.Millisecond,
		RegisterRetryMax:     10 * time.Millisecond,
	}}
	a.everAuthed.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := a.connectNATS(ctx)
	if err == nil {
		t.Fatal("connectNATS unexpectedly succeeded against a server that denies every CONNECT")
	}
	if strings.Contains(err.Error(), "auth_callout rejected") {
		t.Fatalf("err = %v; once this process has authenticated once, a denial must NOT become the "+
			"terminal error that ends Run — that exit takes the unit's cgroup, and the operator's "+
			"running workloads, with it", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want the context error, i.e. the loop kept retrying", err)
	}
}

// TestAuthDenialOnFirstConnectStaysTerminal pins the other half: without a prior
// successful authentication the denial IS a credential problem and must fail fast
// with the operator hint, exactly as before.
func TestAuthDenialOnFirstConnectStaysTerminal(t *testing.T) {
	url := denyingNATS(t)
	a := &Agent{cfg: Config{
		SID: "lab", NID: "gpu1", Logger: d6Logger(),
		NATSURL:              url,
		RegisterRetryInitial: 5 * time.Millisecond,
		RegisterRetryMax:     10 * time.Millisecond,
	}}
	// everAuthed deliberately NOT set.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := a.connectNATS(ctx)
	if err == nil {
		t.Fatal("connectNATS unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "auth_callout rejected") {
		t.Fatalf("err = %v; a first-connect denial must stay terminal and carry the operator hint", err)
	}
}

// leaseRetargetSpy is an ExposeAdapter that records the tunnel retarget dropLease
// performs. That retarget is the dangerous half of the degrade: it points this
// process's data plane at a REGISTER line the incumbent owns.
type leaseRetargetSpy struct {
	mu        sync.Mutex
	retargets []string
}

func (s *leaseRetargetSpy) AddProxy(PortToken) error      { return nil }
func (s *leaseRetargetSpy) RemoveProxy(string, int) error { return nil }
func (s *leaseRetargetSpy) SetNID(nid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retargets = append(s.retargets, nid)
}
func (s *leaseRetargetSpy) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.retargets...)
}

// leasedAgent is an agent running under an ASSIGNED lease name — routingNID is the
// suffixed name, cfg.NID the configured basename — which is the only state in
// which the degrade arm is reachable.
func leasedAgent(t *testing.T, url string, backoff time.Duration, spy *leaseRetargetSpy) *Agent {
	t.Helper()
	a := &Agent{cfg: Config{
		SID: "lab", NID: "gpu1", Logger: d6Logger(),
		NATSURL:              url,
		RegisterRetryInitial: backoff,
		RegisterRetryMax:     backoff,
		ExposeAdapter:        spy,
	}}
	leased := "gpu1-02"
	a.routingNID.Store(&leased)
	a.everAuthed.Store(true) // it authenticated to be granted this name in the first place
	return a
}

// origin: prerelease audit round 2, AC-2.
//
// A TRANSIENT DENIAL MUST NOT COST THE LEASE.
//
// "A denial after this process authenticated is ambiguous, resolve it in favour of
// not acting" governed the die-or-retry decision but not the arm that runs BEFORE
// it, which called dropLease unconditionally. That arm is the more dangerous of
// the two: dropLease re-attaches the state store and retargets the tunnel at the
// basename, so on the shared home directory the lease feature exists for, a clone
// gains write access to the INCUMBENT's state.json and aims its data plane at the
// incumbent's REGISTER line. A rolling `cluster upgrade` or a whole-cluster reboot
// makes every broker answer `Authorization Violation` for a few seconds — the
// behaviour buildConnOptions' own IgnoreAuthErrorAbort comment documents — and the
// old code degraded on the FIRST one.
func TestATransientDenialDoesNotSurrenderTheLeaseName(t *testing.T) {
	spy := &leaseRetargetSpy{}
	// A backoff far longer than the test's own budget: exactly ONE denial can be
	// observed before the context expires inside the wait. No timing race — the
	// question is only whether the first denial alone is enough to degrade.
	a := leasedAgent(t, denyingNATS(t), 30*time.Second, spy)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := a.connectNATS(ctx); err == nil {
		t.Fatal("connectNATS unexpectedly succeeded against a server that denies every CONNECT")
	}

	if got := nidOf(a); got != "gpu1-02" {
		t.Fatalf("a single transient denial dropped the lease name (routing nid is now %q).\n\n"+
			"Every broker answers Authorization Violation for a few seconds during a rolling "+
			"restart. Degrading on the first one re-attaches this process's state store over the "+
			"live incumbent's state.json and retargets its tunnel at the incumbent's REGISTER "+
			"line — two clones on one name, which is what the lease exists to prevent.", got)
	}
	if seen := spy.seen(); len(seen) != 0 {
		t.Fatalf("the tunnel was retargeted to %v on a transient denial", seen)
	}
}

// origin: prerelease audit round 2, AC-2.
//
// THE OTHER HALF, and the reason the fix is a persistence threshold rather than a
// bare everAuthed check: an agent holding a lease necessarily authenticated to be
// granted it, so gating the degrade on everAuthed would make it inert in the exact
// case it was written for — a broker rolled back to a build without the suffix
// fallback, which denies the lease name on every retry forever.
func TestAPersistentDenialStillSurrendersTheLeaseName(t *testing.T) {
	spy := &leaseRetargetSpy{}
	a := leasedAgent(t, denyingNATS(t), 2*time.Millisecond, spy)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := a.connectNATS(ctx); err == nil {
		t.Fatal("connectNATS unexpectedly succeeded")
	}

	if got := nidOf(a); got != "gpu1" {
		t.Fatalf("a denial repeated on every retry never degraded: routing nid is still %q.\n\n"+
			"That is a broker rollback, and without the degrade the agent re-presents the name "+
			"auth_callout just refused forever — turning a rollback into a fleet-wide agent "+
			"outage instead of a fall back to the configured basename.", got)
	}
	if seen := spy.seen(); len(seen) == 0 || seen[len(seen)-1] != "gpu1" {
		t.Fatalf("the tunnel was not retargeted at the basename after the degrade (retargets=%v).\n\n"+
			"Dropping the routing name without moving the data plane leaves the agent addressed "+
			"as gpu1 on the control plane and still dialling as gpu1-02.", seen)
	}
}
