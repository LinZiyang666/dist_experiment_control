package agent

import (
	"context"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: docs/reviews/cloned-credential-instances-plan.md
//
// internal/agent re-implements the instance-id acceptance rule that
// proto.ValidateInstanceID already owns (instanceIDLen + isLowerBase32 in
// instance.go), even though this package imports internal/proto two lines away
// in the same file. Two hand-rolled copies of one contract drift, and the drift
// direction is fail-OPEN: if the agent's copy stays wider than the broker's,
// the agent adopts an inherited id the broker rejects, adjudicateLease returns
// leaseReasonBadInstanceID, and handleRegister DEGRADES — granting the
// presented nid, i.e. the clone fan-out returns with a Warn line as the only
// evidence.
//
// This pins the two acceptors to the same language until mintInstanceID is
// changed to call proto.ValidateInstanceID directly, at which point this test
// becomes trivially true and can be deleted with the duplicate.
func TestAgentInstanceIDAcceptorMatchesTheProtoContract(t *testing.T) {
	cases := []string{
		"",
		"abcdefghijklmnopqrstuvwxyz",   // 26, all letters
		"234567234567234567234567ab",   // 26, base32 digits
		"0189abcdefghijklmnopqrstuv",   // 26, digits the encoder can never emit
		"abcdefghijklmnopqrstuvwxy",    // 25
		"abcdefghijklmnopqrstuvwxyz7",  // 27
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ",   // uppercase
		"abcdefghijklmnopqrstuvwxy-",   // dash
		"abcdefghijklmnopqrstuvwx.z",   // subject metacharacter
		"abcdefghijklmnopqrstuvw\nyz",  // newline
		"abcdefghijklmnopqrstuvw\x00z", // NUL
		" bcdefghijklmnopqrstuvwxyz",   // leading space
	}
	for _, s := range cases {
		agentOK := len(s) == instanceIDLen && isLowerBase32(s)
		protoOK := proto.ValidateInstanceID(s) == nil
		if agentOK != protoOK {
			t.Errorf("acceptor drift on %q: agent=%v proto=%v — the agent would present an id the broker rejects (or vice versa), and the broker degrades to granting the presented nid",
				s, agentOK, protoOK)
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md
//
// The two comments that describe the minted alphabet — instance.go's
// "giving [0-9a-z] … the same character class the lowercase ULIDs in
// internal/proc already use for pids" and identifiers.go's "matching the
// lowercase-ULID alphabet already used for pids in internal/proc" — are both
// false. The encoder is RFC 4648 base32 lowercased, [a-z2-7]; a lowercase
// Crockford ULID is [0-9a-hjkmnp-tv-z]. The two alphabets are disjoint in four
// characters each way. This pins what the minter actually emits so a future
// reader tightening instanceCharset "to match the comment" has a fact to work
// from instead of the prose.
func TestMintedInstanceIDsNeverUseTheDigitsTheCommentClaims(t *testing.T) {
	const emittable = "abcdefghijklmnopqrstuvwxyz234567"
	seen := map[rune]bool{}
	for i := 0; i < 400; i++ {
		id, err := newInstanceID()
		if err != nil {
			t.Fatalf("newInstanceID: %v", err)
		}
		if len(id) != instanceIDLen {
			t.Fatalf("newInstanceID length %d, want %d", len(id), instanceIDLen)
		}
		if proto.ValidateInstanceID(id) != nil {
			t.Fatalf("minted id %q is rejected by proto.ValidateInstanceID", id)
		}
		for _, r := range id {
			seen[r] = true
		}
	}
	for _, r := range "0189" {
		if seen[r] {
			t.Errorf("minted an id containing %q — the encoder alphabet is %q and cannot emit it", r, emittable)
		}
	}
	for _, r := range emittable {
		if !seen[r] {
			t.Errorf("400 mints never produced %q; the alphabet claim in the comment is not merely wrong, the encoder changed", r)
		}
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review.md F17
//
// AN UNUSABLE VERDICT MUST BACK OFF, AND EVENTUALLY STOP.
//
// A refusal retires the session (F3), and the natural consequence is a
// reconnect. With nothing damping it an instance that meets a full suffix space
// re-competes at connect speed forever — a full auth_callout round per attempt,
// from every clone in the image, aimed at the broker that just told all of them
// there is no room. Stopping is the honest end state: nothing clears that
// condition except capacity, so an agent that keeps asking is load, not retry.
func TestUnusableLeaseVerdictsBackOffAndThenStop(t *testing.T) {
	// The first refusal is immediate — one retry with no delay is not a storm,
	// and the common case (a transient collision) resolves there.
	if wait, giveUp := leaseRefusalBackoff(1); wait != 2*time.Second || giveUp {
		t.Fatalf("first refusal: wait=%v giveUp=%v, want 2s / false", wait, giveUp)
	}
	// It grows, and it is capped so an operator who frees a slot is not waiting
	// minutes for anyone to notice.
	prev := time.Duration(0)
	for n := int32(1); n < maxLeaseRefusals; n++ {
		wait, giveUp := leaseRefusalBackoff(n)
		if giveUp {
			t.Fatalf("gave up at refusal %d, before the cap", n)
		}
		if wait < prev {
			t.Fatalf("backoff went backwards at refusal %d: %v after %v", n, wait, prev)
		}
		if wait > leaseRefusalBackoffMax {
			t.Fatalf("refusal %d waits %v, past the %v cap", n, wait, leaseRefusalBackoffMax)
		}
		prev = wait
	}
	// And it terminates rather than retrying forever.
	if _, giveUp := leaseRefusalBackoff(maxLeaseRefusals); !giveUp {
		t.Fatalf("after %d consecutive unusable verdicts the agent must stop competing: the suffix "+
			"space is full and nothing it does will change that. Retrying forever turns every clone "+
			"in the image into a load generator aimed at one broker.", maxLeaseRefusals)
	}
}

// The initial register and reconnect-register paths must spend the same
// refusal budget. Reaching the terminal verdict retires the current session
// and leaves the process disconnected until an operator restarts it; it must
// not turn "giving up" into a zero-delay reconnect loop.
func TestInitialLeaseRefusalStopsRecompetitionAtTheBudget(t *testing.T) {
	a := &Agent{cfg: Config{SID: "lab", NID: "gpu1", Logger: testharness.SilentLog()}}
	a.leaseRefusals.Store(maxLeaseRefusals - 1)

	started := time.Now()
	if !applyLeaseVerdict(a, &proto.NodeLease{Basename: "gpu1"}) {
		t.Fatal("an unusable lease verdict must end the current session")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("the initial-register refusal slept before retiring its session")
	}
	if !a.leaseRefusalTerminal.Load() || a.leaseRefusals.Load() != maxLeaseRefusals {
		t.Fatalf("terminal refusal state was not recorded: terminal=%v refusals=%d",
			a.leaseRefusalTerminal.Load(), a.leaseRefusals.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- awaitLeaseRefusalBackoff(ctx, a) }()
	select {
	case <-done:
		t.Fatal("terminal refusal immediately re-entered the dial loop")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("a cancelled terminal wait reported permission to redial")
		}
	case <-time.After(time.Second):
		t.Fatal("terminal refusal wait ignored parent cancellation")
	}
}
