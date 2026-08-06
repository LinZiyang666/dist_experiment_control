package broker

import (
	"errors"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentprov"
	nodepkg "github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/session"
)

// forward_error_identity_test.go — a business error must keep its IDENTITY
// across the follower→leader forward, not just its message.
//
// origin: docs/reviews/h1-external-review.md, the "疑惑/低风险残留" list — the
// cluster-mode `unknown_pid` path had no test. Widened on inspection: at the
// time this was written forwardErrKind had NO test at all, so the whole table
// of typed errors that survive the raft boundary was resting on nothing.
//
// # WHY IDENTITY, NOT TEXT
//
// The typed error value does not cross the wire; only a stable Kind code does,
// and ForwardBusinessError.Is reconstitutes the identity by running the SAME
// table on the target. Each entry therefore has a caller that branches on
// errors.Is and does something materially different from its fallback. The
// h1 C3 case is the sharpest: handleProcEvent acks `unknown_pid` (OK=true,
// TERMINAL — stop delivering) on proc.ErrNotFound, and `store_error`
// (OK=false, RETRY) on anything else. Lose the identity across a forward and a
// follower turns a GC'd pid into a courier that retries it forever — a bug
// with no error message anywhere, on the exact path h1 built the courier for.
func TestForwardedBusinessErrorsKeepTheirIdentity(t *testing.T) {
	// Every sentinel forwardErrKind claims to carry, and what breaks if the
	// identity is lost. A sentinel added to the table without a line here is
	// caught by the completeness check below.
	cases := []struct {
		name     string
		sentinel error
		wantKind string
		lost     string
	}{
		{"invalid pin", agentprov.ErrInvalidPIN, "invalid_pin",
			"a forwarded bad PIN stops emitting the pin_failed audit + canonical deny"},
		{"session missing (provision)", agentprov.ErrSessionMissing, "session_missing",
			"a join against a dead session degrades to a generic deny"},
		{"session deleting", agentprov.ErrSessionDeleting, "session_deleting",
			"a join during teardown reads as a generic failure instead of a retryable state"},
		{"session already exists", session.ErrAlreadyExists, "session_already_exists",
			"a duplicate create looks like a store failure rather than an idempotent no-op"},
		{"already provisioned", agentprov.ErrAlreadyProvisioned, "already_provisioned",
			"a re-provision of the same fp stops being recognised as benign"},
		{"node session missing", nodepkg.ErrSessionMissing, "node_session_missing",
			"a forwarded register loses its precise session_not_found guidance"},
		{"node session not active", nodepkg.ErrSessionNotActive, "node_session_not_active",
			"the same, for a session that exists but is not ACTIVE"},
		{"proc node missing", proc.ErrNodeMissing, "proc_node_missing",
			"a proc event for an unregistered node loses its transient classification"},
		{"proc not found (h1 C3)", proc.ErrNotFound, "proc_not_found",
			"handleProcEvent acks store_error instead of unknown_pid, so the agent's courier " +
				"retries a GC'd pid forever — the exact failure h1 C3 exists to prevent"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind := forwardErrKind(tc.sentinel)
			if kind != tc.wantKind {
				t.Fatalf("forwardErrKind = %q, want %q", kind, tc.wantKind)
			}
			// What the forwarder actually holds after the reply comes back.
			fwd := &ForwardBusinessError{Kind: kind, Msg: tc.sentinel.Error()}
			if !errors.Is(fwd, tc.sentinel) {
				t.Fatalf("errors.Is(forwarded, %v) is false — identity did not survive the "+
					"forward. Consequence: %s", tc.sentinel, tc.lost)
			}
			// ...and it must not answer to a DIFFERENT sentinel. Without this
			// half, a table that mapped everything to one kind would pass.
			for _, other := range cases {
				if other.wantKind == tc.wantKind {
					continue
				}
				if errors.Is(fwd, other.sentinel) {
					t.Fatalf("a %q error also matches %v (kind %q) — the kinds are not "+
						"discriminating, so callers branching on errors.Is take the wrong arm",
						tc.wantKind, other.sentinel, other.wantKind)
				}
			}
		})
	}
}

// forwardKindsUnderTest is the set the table above covers, in one place so the
// completeness check and the table cannot drift apart.
func forwardKindsUnderTest() map[string]bool {
	return map[string]bool{
		"invalid_pin": true, "session_missing": true, "session_deleting": true,
		"session_already_exists": true, "already_provisioned": true,
		"node_session_missing": true, "node_session_not_active": true,
		"proc_node_missing": true, "proc_not_found": true,
	}
}

// TestForwardErrKindTableIsFullyCovered is the promise the table above makes,
// kept. A sentinel added to forwardErrKind without a case here would ship an
// untested identity across the raft boundary — and the failure mode of a lost
// identity is silence, not an error, so nothing else would notice.
//
// It reads the FUNCTION BODY rather than calling it, because the only way to
// enumerate a switch's arms from the outside is to already know them.
func TestForwardErrKindTableIsFullyCovered(t *testing.T) {
	src, err := os.ReadFile("cluster_forward.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "func forwardErrKind(")
	if start < 0 {
		t.Fatal("forwardErrKind is gone — this test and the table above both need re-deriving")
	}
	end := strings.Index(body[start:], "\n}\n")
	if end < 0 {
		t.Fatal("could not find the end of forwardErrKind")
	}

	covered := forwardKindsUnderTest()
	var missing []string
	for _, m := range regexp.MustCompile(`return "([a-z_]+)"`).FindAllStringSubmatch(body[start:start+end], -1) {
		if !covered[m[1]] {
			missing = append(missing, m[1])
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("forwardErrKind returns kind(s) %v that TestForwardedBusinessErrorsKeepTheirIdentity "+
			"does not cover. Add a case naming the sentinel AND what breaks when its identity is lost — "+
			"a kind whose round trip is untested crosses the raft boundary on nothing but hope", missing)
	}
	// The reverse: a case for a kind the function no longer produces is a test
	// asserting a contract that does not exist.
	produced := map[string]bool{}
	for _, m := range regexp.MustCompile(`return "([a-z_]+)"`).FindAllStringSubmatch(body[start:start+end], -1) {
		produced[m[1]] = true
	}
	var stale []string
	for k := range covered {
		if !produced[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Fatalf("the identity table covers kind(s) %v that forwardErrKind no longer returns — "+
			"delete them rather than leaving a test that pins nothing", stale)
	}
}

// TestUnknownForwardedErrorCarriesNoIdentity pins the default arm. An unmapped
// error must yield "" and match NOTHING: a ForwardBusinessError with an empty
// Kind that answered errors.Is for any sentinel would silently promote every
// unknown store failure into a typed business outcome — for proc events, into
// the TERMINAL unknown_pid ack, dropping a real exit on the floor.
func TestUnknownForwardedErrorCarriesNoIdentity(t *testing.T) {
	unmapped := errors.New("disk I/O error")
	if kind := forwardErrKind(unmapped); kind != "" {
		t.Fatalf("forwardErrKind(unmapped) = %q, want \"\"", kind)
	}
	fwd := &ForwardBusinessError{Kind: "", Msg: unmapped.Error()}
	for _, sentinel := range []error{proc.ErrNotFound, proc.ErrNodeMissing, agentprov.ErrInvalidPIN} {
		if errors.Is(fwd, sentinel) {
			t.Fatalf("an empty-Kind forwarded error matched %v — an unclassified failure must stay "+
				"a generic permanent deny, identical to the local default branch", sentinel)
		}
	}
}
