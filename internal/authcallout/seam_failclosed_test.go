package authcallout

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/session"
)

// seam_failclosed_test.go (batch B, B3 — plan §15.2 "adminsock.Backend.DB / authcallout.Handler.DB")
//
// THE DEFECT THIS CLOSES
//
// Both PIN write paths ended in the same shape:
//
//	provision := h.ProvisionAgentWrite
//	if provision == nil {
//	    provision = func(...) error { return agentprov.ProvisionWithPIN(h.DB, ...) }
//	}
//
// In cluster mode h.DB is the READ-ONLY FSM handle (the broker re-points it to node.RODB()), so
// that fallback fails at the SQLite layer with a bare `attempt to write a readonly database` — on
// the AUTHENTICATION path, where the operator sees only "agents cannot join". The safety of the
// whole arrangement rested on a single `if b.clusterMode { … }` in internal/broker/authcallout.go
// wiring both seams, with nothing checking it. And if a future change hands this package the FSM
// WRITE pool instead of the read-only one, the same fallback SUCCEEDS and silently bypasses raft.
//
// WHY THESE TESTS USE A REAL DB
//
// The obvious fixture is `&Handler{DB: nil, ClusterMode: true}` — if the fallback runs it panics,
// which "proves" it did not. That fixture is wrong, and wrong in the direction that makes the test
// useless: ensureMember reads session.IsActive(h.DB, …) BEFORE it ever reaches the seam decision,
// so a nil DB stops the call at a read point and the test passes without the guard existing at all.
// Every fixture here is a fully-populated ACTIVE session with a real PIN, so control genuinely
// arrives at the seam and the SINGLE-mode half can assert the fallback still SUCCEEDS rather than
// merely "panicked, so it ran".

const seamTestPIN = "424242"

// seamHandler builds a handler whose pre-seam checks all pass: a real in-memory DB with an ACTIVE
// session carrying seamTestPIN. logs captures every record so the wire/log split can be asserted
// separately.
func seamHandler(t *testing.T, clusterMode bool) (h *Handler, logs *bytes.Buffer, clientNkey string) {
	t.Helper()
	h, _ = freshHandler(t)
	h.Now = time.Now
	h.ClusterMode = clusterMode
	logs = &bytes.Buffer{}
	h.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	seedSessionWithPin(t, h, "lab", seamTestPIN)
	return h, logs, freshUserPub(t)
}

// TestProvisionSeamFailsClosedInClusterMode is the load-bearing assertion. It checks two things a
// returned error alone cannot distinguish: that the refusal happened, AND that no row was written —
// a fallback that ran and failed at the SQLite layer would also return an error.
func TestProvisionSeamFailsClosedInClusterMode(t *testing.T) {
	h, _, clientNkey := seamHandler(t, true)

	err := h.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, "10.0.0.1")
	if !errors.Is(err, ErrSeamNotWired) {
		t.Fatalf("clustered handler with a nil ProvisionAgentWrite seam returned %v; want "+
			"ErrSeamNotWired. Without the guard this takes the direct-mutator fallback and writes "+
			"h.DB, which is the read-only FSM handle in cluster mode.", err)
	}
	if _, err := agentprov.Lookup(h.DB, "lab", "lab-1"); !errors.Is(err, agentprov.ErrNotProvisioned) {
		t.Fatalf("a row exists for (lab, lab-1) after the refusal (lookup err = %v). The fallback "+
			"RAN — the returned ErrSeamNotWired was not the reason the call failed. In production "+
			"this row would be un-replicated: present on one broker, invisible to raft.", err)
	}
}

// TestJoinSeamFailsClosedInClusterMode is the ctl-side twin. ensureMember has more pre-seam reads
// than ensureAgentProvisioned (IsActive + IsMember), so this also confirms the guard sits after
// them rather than short-circuiting the whole function.
func TestJoinSeamFailsClosedInClusterMode(t *testing.T) {
	h, _, _ := seamHandler(t, true)

	err := h.ensureMember("lab", "fp-ctl", seamTestPIN, "10.0.0.1")
	if !errors.Is(err, ErrSeamNotWired) {
		t.Fatalf("clustered handler with a nil JoinMemberWrite seam returned %v; want ErrSeamNotWired", err)
	}
	member, err := session.IsMember(h.DB, "lab", "fp-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if member {
		t.Fatal("fp-ctl became a member after the refusal — the fallback ran and wrote the FSM " +
			"handle directly, which is the un-replicated write this guard exists to prevent")
	}
}

// TestSeamFallbackStillRunsInSingleMode is the other half, and it is what keeps the guard from
// being a behaviour change for the deployment that actually exists. racknerd runs single-broker:
// there ClusterMode is false, no seam is wired, and the direct mutator is the CORRECT path. A guard
// that refused there would break PIN bootstrap on the entire production fleet.
//
// It asserts SUCCESS plus the written row, not "it did not return ErrSeamNotWired" — the latter
// would still pass if single mode broke for some unrelated reason.
func TestSeamFallbackStillRunsInSingleMode(t *testing.T) {
	h, _, clientNkey := seamHandler(t, false)

	if err := h.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, "10.0.0.1"); err != nil {
		t.Fatalf("single-mode PIN bootstrap failed: %v. The direct mutator MUST remain the "+
			"fallback when ClusterMode is false — every broker in the fleet is single-mode today.", err)
	}
	bound, err := agentprov.Lookup(h.DB, "lab", "lab-1")
	if err != nil {
		t.Fatalf("single mode returned success but wrote no provisioning row: %v", err)
	}
	if bound != "fp-1" {
		t.Fatalf("provisioned row binds %q, want fp-1", bound)
	}

	if err := h.ensureMember("lab", "fp-ctl", seamTestPIN, "10.0.0.1"); err != nil {
		t.Fatalf("single-mode ctl PIN join failed: %v", err)
	}
	member, err := session.IsMember(h.DB, "lab", "fp-ctl")
	if err != nil {
		t.Fatal(err)
	}
	if !member {
		t.Fatal("single-mode ctl join returned success but wrote no member row")
	}
}

// TestSeamWiredTakesPrecedence proves ClusterMode does not disable a WIRED seam. Without this the
// guard could be implemented as "ClusterMode ⇒ refuse", which would break clustered PIN bootstrap
// entirely while both fail-closed tests above stayed green.
func TestSeamWiredTakesPrecedence(t *testing.T) {
	h, _, clientNkey := seamHandler(t, true)
	provisionCalls, joinCalls := 0, 0
	h.ProvisionAgentWrite = func(sid, nid, fp, pin string, now time.Time) error {
		provisionCalls++
		return nil
	}
	h.JoinMemberWrite = func(sid, fp, pin string, now time.Time) error {
		joinCalls++
		return nil
	}

	if err := h.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, "10.0.0.1"); err != nil {
		t.Fatalf("a wired provision seam must be used in cluster mode, got %v", err)
	}
	if err := h.ensureMember("lab", "fp-ctl", seamTestPIN, "10.0.0.1"); err != nil {
		t.Fatalf("a wired join seam must be used in cluster mode, got %v", err)
	}
	if provisionCalls != 1 || joinCalls != 1 {
		t.Fatalf("wired seams called %d/%d times, want 1/1 — ClusterMode short-circuited a seam "+
			"that WAS wired, which disables clustered PIN bootstrap", provisionCalls, joinCalls)
	}
}

// TestSeamNotWiredKeepsDetailOffTheWire and TestSeamNotWiredPutsDetailInTheLog are deliberately
// TWO tests over ONE run of the code. Batch A's M13 finding was a single assertion that conflated
// "the detail is logged" with "the detail is not on the wire"; either can regress alone. Handle
// puts err.Error() on the wire verbatim (h.deny), to a client that has not authenticated.
func TestSeamNotWiredKeepsDetailOffTheWire(t *testing.T) {
	h, _, clientNkey := seamHandler(t, true)
	err := h.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, "10.0.0.1")
	if err == nil {
		t.Fatal("want a refusal")
	}
	wire := err.Error()
	// These tokens describe the broker's internals. An anonymous connector must not learn them.
	for _, leak := range []string{"ProvisionAgentWrite", "JoinMemberWrite", "seam", "cluster", "raft", "readonly", "FSM"} {
		if strings.Contains(strings.ToLower(wire), strings.ToLower(leak)) {
			t.Errorf("the wire text %q contains %q. Handle passes this straight to an "+
				"UNAUTHENTICATED client, so it would disclose that this broker is clustered and "+
				"which internal write path is missing — the same disclosure batch B removed from "+
				"store_error. Put it in the log instead.", wire, leak)
		}
	}
}

func TestSeamNotWiredPutsDetailInTheLog(t *testing.T) {
	h, logs, clientNkey := seamHandler(t, true)
	_ = h.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, "10.0.0.1")

	got := logs.String()
	if !strings.Contains(got, "level=ERROR") {
		t.Errorf("the seam-not-wired refusal must log at ERROR — it is a broker bug an operator "+
			"has to act on, and the wire text deliberately says nothing. Got: %s", got)
	}
	// The operator's whole diagnosis comes from this line, so it must name WHICH seam.
	if !strings.Contains(got, "ProvisionAgentWrite") {
		t.Errorf("the log must name the missing seam; with two seams, %q alone does not tell the "+
			"operator which wiring is absent. Got: %s", "seam is not wired", got)
	}
	if !strings.Contains(got, "sid=lab") || !strings.Contains(got, "nid=lab-1") {
		t.Errorf("the log must carry sid/nid so the refusal can be correlated with the agent that "+
			"hit it. Got: %s", got)
	}

	// The join seam's log must be distinguishable from the provision seam's, or an operator with
	// both paths failing cannot tell them apart.
	h2, logs2, _ := seamHandler(t, true)
	_ = h2.ensureMember("lab", "fp-ctl", seamTestPIN, "10.0.0.1")
	if !strings.Contains(logs2.String(), "JoinMemberWrite") {
		t.Errorf("the join-seam refusal log must name JoinMemberWrite, got: %s", logs2.String())
	}
}

// TestSeamNotWiredDoesNotChargeThePINBudget is the property most likely to be broken by a
// "simplification" that moves the guard, and it has a real consequence.
//
// The seam decision sits AFTER the E.6 per-IP rate limiter. A broker with an unwired seam refuses
// every PIN bootstrap; if those refusals counted as PIN failures, the broker's own bug would
// exhaust the budget of every honest agent's IP and keep denying them for a window after an
// operator fixed the wiring. It must also not emit pin_failed — that event is an alarm for
// credential guessing, and a wiring bug would forge it.
func TestSeamNotWiredDoesNotChargeThePINBudget(t *testing.T) {
	h, _, clientNkey := seamHandler(t, true)
	var events []string
	h.EmitEvent = func(kind string, payload map[string]any) { events = append(events, kind) }

	const ip = "10.0.0.9"
	// Well past the E.6 budget: if each refusal were charged, the limiter would trip and the
	// error would change identity.
	for i := 0; i < 25; i++ {
		if err := h.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, ip); !errors.Is(err, ErrSeamNotWired) {
			t.Fatalf("attempt %d returned %v; want ErrSeamNotWired every time. A rate-limited "+
				"refusal means the broker's own wiring bug is being charged to the client's PIN "+
				"budget, so honest agents stay locked out after the wiring is fixed.", i, err)
		}
	}
	if h.pinRateLimited(ip) {
		t.Error("the IP is now rate-limited purely from seam-not-wired refusals")
	}
	if len(events) != 0 {
		t.Errorf("seam-not-wired emitted %v; pin_failed is a credential-guessing alarm and a "+
			"wiring bug must not forge it", events)
	}
}

// TestSeamGuardIsNotVacuous is the self-check. Every test above asserts about a handler this file
// built, so a rename or a signature change could leave them all green against a code path the
// product no longer takes. This pins the two facts they depend on: that the fixture's pre-seam
// reads really do pass (otherwise the refusals prove nothing), and that ClusterMode is the only
// difference between the failing and succeeding cases.
func TestSeamGuardIsNotVacuous(t *testing.T) {
	// (1) With BOTH ClusterMode false and seams nil, the same arguments SUCCEED. That is what
	// makes "cluster mode refuses" a statement about the guard rather than about the arguments.
	single, _, clientNkey := seamHandler(t, false)
	if err := single.ensureAgentProvisioned("lab", "lab-1", clientNkey, "fp-1", seamTestPIN, "10.0.0.1"); err != nil {
		t.Fatalf("the fixture's arguments do not reach the seam at all (single-mode call failed "+
			"with %v), so every fail-closed assertion in this file could be passing because the "+
			"call stopped at an earlier check", err)
	}

	// (2) The refusal is ClusterMode's doing and nothing else: same fixture shape, one field
	// flipped, opposite outcome.
	clustered, _, clientNkey2 := seamHandler(t, true)
	if err := clustered.ensureAgentProvisioned("lab", "lab-1", clientNkey2, "fp-1", seamTestPIN, "10.0.0.1"); !errors.Is(err, ErrSeamNotWired) {
		t.Fatalf("flipping ClusterMode did not change the outcome (got %v)", err)
	}
}
