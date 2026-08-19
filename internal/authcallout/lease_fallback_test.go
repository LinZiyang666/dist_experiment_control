package authcallout

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/proto"
)

// lease_fallback_test.go — the auth-plane half of the cloned-credential lease.
//
// origin: docs/reviews/cloned-credential-instances-plan.md §3.4
//
// The plan calls the suffix fallback "the single highest-value change in the
// plan" and lists TestSuffixFallbackAuthorizesOnlyTheBasenameFingerprint with
// two required mutations. Before this file the arm had NO test at all: the
// unit suite never reached it, and the new test/p2 end-to-end suite runs
// against testharness.StartNATS, a plain embedded server with no auth_callout
// wired, so it exercises register/lease adjudication while the CONNECT that
// actually adopts the lease name is never authorized by anything.
//
// That gap matters in one specific direction: if this arm were wrong, a
// contested clone would be denied on its adoption reconnect, and connectNATS
// treats an auth failure as FATAL (see internal/agent/agent.go:1247) — so the
// symptom in production is every clone crash-looping at RestartSec=5, which is
// exactly the outcome "DEGRADE, NEVER REFUSE" forbids.

// leaseHandler builds a handler with an ACTIVE "lab" session and returns a
// fresh client nkey. Nothing is provisioned yet.
func leaseHandler(t *testing.T) (*Handler, string) {
	t.Helper()
	h, _ := freshHandler(t)
	h.Now = time.Now
	seedSessionWithPin(t, h, "lab", seamTestPIN)
	return h, freshUserPub(t)
}

const (
	imageFP   = "SHA256:image-credential"
	foreignFP = "SHA256:some-other-device"
)

// TestSuffixFallbackAuthorizesOnlyTheBasenameFingerprint is the plan's §5 entry.
//
// MUTATION 1 — delete the `bound == fp` comparison in handler.go's fallback
// (allow on any successful basename lookup): the "foreign fingerprint" case
// below flips to allow, i.e. any provisioned agent in the session could
// authenticate as any other agent's `-NN` name.
//
// MUTATION 2 — replace proto.ValidateNID at handler.go:312 with a lax variant
// (e.g. only a length check): the "subject metacharacter" case below flips to
// allow and the minted ACL literal stops being a single subject token.
func TestSuffixFallbackAuthorizesOnlyTheBasenameFingerprint(t *testing.T) {
	h, nkey := leaseHandler(t)
	if err := agentprov.Provision(h.DB, "lab", "gpu", imageFP, time.Now()); err != nil {
		t.Fatal(err)
	}

	// The credential that owns the basename may adopt a lease name with no PIN.
	// This is the whole point of the arm: without it the adoption reconnect is
	// denied and connectNATS makes that fatal.
	leaseName := proto.LeaseNameFor("gpu", proto.FirstLeaseSuffix)
	if err := h.ensureAgentProvisioned("lab", leaseName, nkey, imageFP, "", "10.0.0.1"); err != nil {
		t.Fatalf("the basename's own credential was denied its lease name %q: %v\n"+
			"In production this is a crash loop: the adoption reconnect is an INITIAL connect and "+
			"connectNATS treats an auth failure as fatal.", leaseName, err)
	}

	// A DIFFERENT fingerprint must not ride the same grammar.
	if err := h.ensureAgentProvisioned("lab", leaseName, nkey, foreignFP, "", "10.0.0.1"); err == nil {
		t.Fatal("a foreign fingerprint was authorized for gpu-02 — the fallback must honour a " +
			"lease ONLY for the fingerprint bound to the basename")
	}

	// No basename row at all ⇒ no fallback, ordinary PIN-bootstrap deny.
	if err := h.ensureAgentProvisioned("lab", "unrelated-02", nkey, imageFP, "", "10.0.0.1"); err == nil {
		t.Fatal("a lease-shaped name whose basename is not provisioned was authorized")
	}

	// The lease name already belongs to somebody else ⇒ the ordinary
	// bound-to-a-different-identity deny wins; the fallback never runs.
	if err := agentprov.Provision(h.DB, "lab", "gpu-07", foreignFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	err := h.ensureAgentProvisioned("lab", "gpu-07", nkey, imageFP, "", "10.0.0.1")
	if err == nil || !strings.Contains(err.Error(), "different agent identity") {
		t.Fatalf("a lease-shaped name provisioned to another fingerprint returned %v; want the "+
			"bound-to-a-different-identity deny", err)
	}

	// The auth path keeps the STRICT nid validator: a name carrying a subject
	// metacharacter is rejected before any lookup, so the ACL literal minted
	// from nid can never be more than one subject token.
	for _, bad := range []string{"gpu.evil-02", "gpu*-02", "gpu>-02", "GPU-02", strings.Repeat("g", 31) + "-02"} {
		if err := h.ensureAgentProvisioned("lab", bad, nkey, imageFP, "", "10.0.0.1"); err == nil {
			t.Fatalf("nid %q was authorized; ValidateNID must still gate the auth path", bad)
		}
	}
}

// TestSuffixFallbackHonoursSessionStateAndFencing pins that the new arm reuses
// the same two refusals as the already-provisioned path rather than becoming a
// shortcut around them.
//
// MUTATION — drop either the session.IsActive check or the h.fenced() check
// from the fallback: the corresponding case below flips to allow, and a
// tombstoned session or a broker that lost leader contact starts admitting
// agents on a read of a possibly-stale local replica.
func TestSuffixFallbackHonoursSessionStateAndFencing(t *testing.T) {
	h, nkey := leaseHandler(t)
	if err := agentprov.Provision(h.DB, "lab", "gpu", imageFP, time.Now()); err != nil {
		t.Fatal(err)
	}

	h.LeaderContactStale = func(time.Time) bool { return true }
	if err := h.ensureAgentProvisioned("lab", "gpu-02", nkey, imageFP, "", "10.0.0.1"); !errors.Is(err, ErrFenced) {
		t.Fatalf("fenced node authorized a lease name: got %v, want ErrFenced", err)
	}
	h.LeaderContactStale = nil

	if _, err := h.DB.Exec(`UPDATE sessions SET state='DELETING' WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	if err := h.ensureAgentProvisioned("lab", "gpu-02", nkey, imageFP, "", "10.0.0.1"); err == nil {
		t.Fatal("a tombstoned session authorized a lease name")
	}
}

// TestSuffixFallbackScopeIsTheWholeSuffixNamespace CHARACTERIZES current
// behaviour — it is not an endorsement.
//
// FINDING (internal review, auth_callout lane): the arm is stateless. It never
// asks whether the broker actually issued this lease, so the basename's
// credential is authorized for EVERY unprovisioned `<basename>-NN`, including
// a name the operator intends for a different physical device and a name the
// operator has just evicted. internal/node/lease.go:44-49 reasons explicitly
// about the reverse direction ("`foo-02` is an ordinary ValidateNID-legal name
// that an operator may already own as a real device") and refuses to hand such
// a name out; the auth path has no counterpart, while handler.go:350 asserts
// "It creates no new trust edge".
//
// If the main process fixes this, these assertions invert.
func TestSuffixFallbackScopeIsTheWholeSuffixNamespace(t *testing.T) {
	h, nkey := leaseHandler(t)
	if err := agentprov.Provision(h.DB, "lab", "gpu", imageFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"gpu-02", "gpu-07", "gpu-42", "gpu-99"} {
		if err := h.ensureAgentProvisioned("lab", nid, nkey, imageFP, "", "10.0.0.1"); err != nil {
			t.Fatalf("characterization drifted: %q now denied (%v) — if the fallback was "+
				"narrowed to leases the broker actually issued, invert this test", nid, err)
		}
	}
}

// TestEvictOfALeaseNameDoesNotRevokeTheCredential CHARACTERIZES the operator
// consequence of the above.
//
// FINDING (internal review, auth_callout lane): `tether admin evict <sid>
// <lease-name>` deletes the agent_provisioning row (there is none) and the
// nodes row, then returns OK — and the very next CONNECT under that name is
// authorized again by the fallback. Revocation is basename-granular only, and
// nothing in the code, the CLI reply or docs/usage.md says so. Compounding it,
// the agent's agent_evicted matcher compares ev.NID against cfg.NID (the
// BASENAME, internal/agent/agent.go:1066), so the broadcast for a lease name
// matches no running agent either.
func TestEvictOfALeaseNameDoesNotRevokeTheCredential(t *testing.T) {
	h, nkey := leaseHandler(t)
	if err := agentprov.Provision(h.DB, "lab", "gpu", imageFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Exactly what adminsock.handleEvict does for (lab, gpu-02).
	if _, err := h.DB.Exec(`DELETE FROM agent_provisioning WHERE sid=? AND nid=?`, "lab", "gpu-02"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.DB.Exec(`DELETE FROM nodes WHERE sid=? AND nid=?`, "lab", "gpu-02"); err != nil {
		t.Fatal(err)
	}
	if err := h.ensureAgentProvisioned("lab", "gpu-02", nkey, imageFP, "", "10.0.0.1"); err != nil {
		t.Fatalf("characterization drifted: evict now sticks (%v). If that is intentional, this "+
			"test should assert the deny instead.", err)
	}
}
