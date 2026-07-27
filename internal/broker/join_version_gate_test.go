package broker

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// join_version_gate_test.go (batch B, commits 4-5) — the net for B4.
//
// WHAT WAS WRONG
//
// The proto-skew gate existed and was tested, but it was wired to `handleAdd`, reachable only
// through adminsock.OpClusterAdd — which internal/adminsock/protocol.go:150 has DELIBERATELY not
// routed since v0.4.2. So the product could not reach the gate, while b6_skew_test.go called it
// directly and stayed permanently green: a test covering a path the CLI cannot take.
//
// Meanwhile the LIVE grow path (`cluster join approve` -> StartJoinOperation -> driveJoin)
// carried a JoinBundle with no version fields at all and did no version check whatsoever.
//
// WHAT THE FIX IS WORTH — stated honestly, because the roadmap overstated it
//
// The roadmap's failure story was "a v3 broker joins a v2 cluster, serves its agents on
// tether.v3.* and loses them, while cluster status still says HEALTHY_HA". That is FALSE, and
// this file does not claim it: clusterCaughtUp polls proto.SubjClusterCursor, whose subject
// carries the proto token, so a foreign-proto joiner never answers, never satisfies the
// catch-up gate, and never promotes to VOTER — computeHealth marks it degraded at
// phaseCatchingUp.
//
// The real benefit is smaller and worth having anyway: the join is refused in milliseconds with
// a NAMED cause and no state, instead of grinding through a multi-minute catch-up poll and
// timing out with a last_error that says "check the joining broker" — having left an operation
// row and a nonvoter behind for the operator to clean up.

// TestVersionSkewRefusalDecisions is the decision table for the extracted, transport-free gate.
func TestVersionSkewRefusalDecisions(t *testing.T) {
	log := silentLogger()
	// EXTERNAL REVIEW B3: the two "release skew is allowed" rows below used to assert the opposite.
	// They pinned an implementation choice that contradicted docs/requirements.md §6.7 ("三端强制同
	// 版本 … 不一致 → 拒绝连接"), which is the layer-1 WHAT authority under CLAUDE.md §1. A test can
	// pin a policy; it cannot confer authority on one. Release mismatch now refuses.
	//
	// ABSENT declarations also refuse: they cannot prove the exact equality required by §6.7.
	// Decoding remains additive so an old bundle receives an actionable typed error rather than a
	// parse failure; the operator complies by upgrading and regenerating the bundle.
	cases := []struct {
		name          string
		joinerProto   int
		joinerRelease string
		wantRefused   bool
	}{
		{"matching proto and release", proto.ProtoVersion, proto.ReleaseVersion, false},
		{"undeclared proto and release", 0, "", true},
		{"undeclared proto, declared MATCHING release", 0, proto.ReleaseVersion, true},
		{"declared proto, undeclared release", proto.ProtoVersion, "", true},
		{"undeclared proto but MISMATCHED release", 0, "v9.9.9-other", true},
		{"release skew is refused (requirements §6.7)", proto.ProtoVersion, "v9.9.9-other", true},
		{"release skew one patch off is still a skew", proto.ProtoVersion, proto.ReleaseVersion + "1", true},
		{"proto one ahead", proto.ProtoVersion + 1, proto.ReleaseVersion, true},
		{"proto one behind", proto.ProtoVersion - 1, proto.ReleaseVersion, true},
		{"proto far ahead", proto.ProtoVersion + 99, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := versionSkewRefusal(c.joinerProto, c.joinerRelease, "br-joiner", log)
			if c.wantRefused && err == nil {
				t.Fatalf("(proto v%d, release %q) must be refused against cluster (proto v%d, release %s)",
					c.joinerProto, c.joinerRelease, proto.ProtoVersion, proto.ReleaseVersion)
			}
			if !c.wantRefused && err != nil {
				t.Fatalf("must be allowed, got %v", err)
			}
			if err == nil {
				return
			}
			var skew *ErrJoinVersionSkew
			if !errors.As(err, &skew) {
				t.Fatalf("refusal must be *ErrJoinVersionSkew (clusterCodeFor classifies by TYPE, "+
					"not prose); got %T", err)
			}
			if c.joinerProto == 0 {
				if skew.MissingField != "protocol version" {
					t.Errorf("missing proto refusal carries MissingField=%q, want protocol version",
						skew.MissingField)
				}
				return
			}
			// A proto mismatch is reported as a proto mismatch; a release mismatch as a release one.
			// Collapsing them would send the operator to reinstall for the wrong reason.
			protoMismatch := c.joinerProto != 0 && c.joinerProto != proto.ProtoVersion
			if protoMismatch {
				if skew.JoinerProto != c.joinerProto || skew.ClusterProto != proto.ProtoVersion {
					t.Errorf("typed error carries proto (%d,%d), want (%d,%d)",
						skew.JoinerProto, skew.ClusterProto, c.joinerProto, proto.ProtoVersion)
				}
				return
			}
			if c.joinerRelease == "" {
				if skew.MissingField != "release version" {
					t.Errorf("missing release refusal carries MissingField=%q, want release version",
						skew.MissingField)
				}
				return
			}
			if skew.JoinerRelease != c.joinerRelease || skew.ClusterRelease != proto.ReleaseVersion {
				t.Errorf("typed error carries release (%q,%q), want (%q,%q)",
					skew.JoinerRelease, skew.ClusterRelease, c.joinerRelease, proto.ReleaseVersion)
			}
			if !strings.Contains(err.Error(), "§6.7") {
				t.Errorf("a release refusal must cite the requirement it enforces so the operator "+
					"can see it is policy, not a bug; got %q", err.Error())
			}
		})
	}
}

// TestReleaseSkewRefusalIsUnsignedAndSaysSo pins the honest scope of the B3 gate. The declared
// version fields are NOT inside JoinSignBytes (extending it would invalidate every already-issued
// bundle, including the ones DR rejoin replays), so a joiner can declare whatever it likes. That
// makes this an operator-error guard, and the code must not be documented as a security boundary —
// the reviewer's own caveat, kept as a test so a future edit cannot quietly upgrade the claim.
func TestReleaseSkewRefusalIsUnsignedAndSaysSo(t *testing.T) {
	// A forged matching release is accepted: proof the check is advice about the declaration, not
	// authentication of it.
	if err := versionSkewRefusal(proto.ProtoVersion, proto.ReleaseVersion, "br-liar", silentLogger()); err != nil {
		t.Fatalf("a declared-matching joiner must be admitted by this gate: %v", err)
	}
	src, err := readSourceFile("clusterstatus.go")
	if err != nil {
		t.Fatal(err)
	}
	// The godoc must state the limitation. Without it the next reader reasonably concludes the join
	// path authenticates release equality, and builds something else on that assumption.
	for _, want := range []string{"UNSIGNED", "OPERATOR-ERROR guard"} {
		if !strings.Contains(src, want) {
			t.Errorf("versionSkewRefusal's godoc no longer states that the declaration is %q — the "+
				"gate would then read as an authenticated compatibility guarantee, which it is not", want)
		}
	}
}

// TestVersionSkewRefusalToleratesNilLogger pins the fixture reality that made logSkew necessary:
// StartJoinOperation's ClusterAdmin gets its logger from wireClusterLate, and several unit
// fixtures build one without. Logging must not replace a typed refusal with a panic.
func TestVersionSkewRefusalToleratesNilLogger(t *testing.T) {
	if err := versionSkewRefusal(0, "", "br-joiner", nil); err == nil {
		t.Fatal("an undeclared joiner must fail closed even with a nil logger")
	}
	if err := versionSkewRefusal(proto.ProtoVersion+1, "", "br-joiner", nil); err == nil {
		t.Fatal("a real proto skew must still be refused with a nil logger")
	}
	// And the B3 release path logs on the REFUSE branch, which is the one a nil logger would now
	// panic through — the allow-path-only check above would not have caught it.
	if err := versionSkewRefusal(proto.ProtoVersion, "v9.9.9-other", "br-joiner", nil); err == nil {
		t.Fatal("a release skew must be refused with a nil logger")
	}
}

// TestJoinVersionSkewMapsToTerminalExitCode is the assertion that keeps this gate from becoming
// an A1-class defect of its own. clusterCodeFor's default is "", which the CLI classifies exit
// 70 — and docs/usage.md §9.13 tells automation to treat 70 as retryable. A version-skewed
// joiner will NEVER succeed on retry, so it must carry a terminal code.
func TestJoinVersionSkewMapsToTerminalExitCode(t *testing.T) {
	err := &ErrJoinVersionSkew{JoinerProto: proto.ProtoVersion + 1, ClusterProto: proto.ProtoVersion}
	if got := clusterCodeFor(err); got != adminsock.CodeVersionSkew {
		t.Fatalf("clusterCodeFor gave %q, want %q — without the typed branch this falls to the "+
			"default \"\" and the CLI retries a join that can never succeed", got, adminsock.CodeVersionSkew)
	}
	// And through a wrapping layer, since StartJoinOperation's callers wrap.
	if got := clusterCodeFor(errors.Join(errors.New("cluster join:"), err)); got != adminsock.CodeVersionSkew {
		t.Errorf("clusterCodeFor loses the classification through a wrap: got %q", got)
	}
}

// TestJoinVersionSkewMessageIsUnchanged pins the operator-facing string. handleAdd emitted this
// exact sentence before the extraction; keeping it byte-identical means the socket path's
// behaviour did not move while the decision was being relocated.
func TestJoinVersionSkewMessageIsUnchanged(t *testing.T) {
	err := &ErrJoinVersionSkew{JoinerProto: 7, ClusterProto: 2}
	const want = "version skew: joiner speaks proto v7 but this cluster is proto v2 — " +
		"reinstall the joiner on a matching release before adding it"
	if got := err.Error(); got != want {
		t.Fatalf("refusal text changed:\n got: %q\nwant: %q", got, want)
	}
}

// --- the bundle wire -------------------------------------------------------

func signedBundle(t *testing.T, nodeID string, mutate func(*cluster.JoinBundle)) string {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	const nonce = "nonce-abcdef"
	sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	b := cluster.JoinBundle{
		NodeID: nodeID, Name: nodeID, NodeIdentPub: pub, NatsServerID: nodeID,
		RaftAddr: "10.0.0.9:7400", NatsRoute: "nats://10.0.0.9:6222",
		TunnelAddr: "10.0.0.9:7000", PublicHost: "10.0.0.9",
		CertFP: "SHA256:cert", BusNkey: "UBUS",
		JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig),
	}
	if mutate != nil {
		mutate(&b)
	}
	s, err := cluster.EncodeJoinBundle(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return s
}

// TestJoinBundleVersionFieldsAreAdditive proves the wire claim in both directions: a bundle
// minted WITHOUT the fields still decodes and lands on zero values (then receives a typed,
// actionable fail-closed admission error), and one minted WITH them round-trips.
func TestJoinBundleVersionFieldsAreAdditive(t *testing.T) {
	old := signedBundle(t, "br-old", nil)
	b, err := cluster.DecodeJoinBundle(old)
	if err != nil {
		t.Fatalf("a bundle without the new fields must still decode: %v", err)
	}
	if b.ProtoVer != 0 || b.ReleaseVersion != "" {
		t.Fatalf("absent fields must decode to the zero value, got (%d,%q)", b.ProtoVer, b.ReleaseVersion)
	}
	err = versionSkewRefusal(b.ProtoVer, b.ReleaseVersion, b.NodeID, silentLogger())
	var skew *ErrJoinVersionSkew
	if !errors.As(err, &skew) || skew.MissingField != "protocol version" {
		t.Fatalf("an older bundle must decode, then fail with an actionable missing-version error; got %T: %v",
			err, err)
	}

	newer := signedBundle(t, "br-new", func(jb *cluster.JoinBundle) {
		jb.ProtoVer = proto.ProtoVersion
		jb.ReleaseVersion = proto.ReleaseVersion
	})
	nb, err := cluster.DecodeJoinBundle(newer)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nb.ProtoVer != proto.ProtoVersion || nb.ReleaseVersion != proto.ReleaseVersion {
		t.Fatalf("round trip lost the version fields: (%d,%q)", nb.ProtoVer, nb.ReleaseVersion)
	}

	// The encoded prefix is part of the wire and must not have moved.
	if !strings.HasPrefix(newer, "tether-join:v1:") {
		t.Errorf("bundle prefix changed — every existing `cluster join approve` invocation and the "+
			"DecodeJoinBundle prefix check depend on it: %q", newer[:20])
	}
}

// TestJoinBundleVersionFieldsAreNotSigned pins the honesty of the doc comment. The fields are
// ADVISORY: they are outside JoinSignBytes, so a hand-crafted bundle can lie about them. If a
// future change folded them into the signed bytes, every bundle prepared by an older `cluster
// join prepare` would fail PoP verification on an upgraded leader — a silent break of the DR
// path, which is exactly when bundles are old.
func TestJoinBundleVersionFieldsAreNotSigned(t *testing.T) {
	// The direct proof: take a correctly signed bundle, rewrite its version fields to
	// something else, and require the PoP to STILL verify. That is what "advisory and
	// unsigned" means operationally — the fields are forgeable, and they are allowed to be,
	// because the PoP over (domain‖node_id‖ident_pub‖nonce) is the actual trust boundary.
	//
	// The inverse is what must never happen: if a future change folded these into
	// JoinSignBytes, every bundle produced by an older `cluster join prepare` would fail PoP
	// on an upgraded leader. Those old bundles are exactly what a DR rejoin reaches for.
	signed := signedBundle(t, "br-1", func(jb *cluster.JoinBundle) {
		jb.ProtoVer = proto.ProtoVersion
		jb.ReleaseVersion = proto.ReleaseVersion
	})
	b, err := cluster.DecodeJoinBundle(signed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := cluster.VerifyBundlePoP(b); err != nil {
		t.Fatalf("baseline PoP must verify: %v", err)
	}

	b.ProtoVer = proto.ProtoVersion + 42
	b.ReleaseVersion = "v0.0.0-forged"
	if err := cluster.VerifyBundlePoP(b); err != nil {
		t.Fatalf("rewriting the version fields broke the PoP (%v) — they have been pulled INTO "+
			"the signed bytes. Every bundle an older `cluster join prepare` minted now fails "+
			"verification on an upgraded leader, and a DR rejoin is precisely when an old bundle "+
			"gets used.", err)
	}

	// Non-vacuity: the PoP must still reject a forged IDENTITY, else the check above proves
	// only that VerifyBundlePoP accepts everything.
	b.NodeIdentPub = strings.Replace(b.NodeIdentPub, b.NodeIdentPub[3:6], "AAA", 1)
	if err := cluster.VerifyBundlePoP(b); err == nil {
		t.Fatal("VerifyBundlePoP accepted a bundle whose ident_pub was tampered with — the " +
			"assertion above is vacuous because nothing is actually being verified")
	}
}

// NOTE: the tripwire asserting OpClusterAdd stays UNROUTED (and OpClusterJoinApprove stays
// routed) lives in internal/adminsock/routing_tripwire_test.go, because `clusterOps` is
// unexported and the routing table is that package's contract, not this one's.

// TestJoinPrepareStampsTheDeclaredVersion guards the MINT side. Everything else in this file
// tests what the leader does with a bundle that already declares a version — delete the two
// stamping lines from `cluster join prepare` and every other test here still passes, because
// they construct their own bundles. Then the gate is live but nothing real ever declares a
// version. Without the stamps every genuine join is rejected as missing compatibility evidence,
// making grow unusable. Internal review flagged exactly this mint-side gap.
//
// It is a source assertion rather than a CLI invocation because `cluster join prepare` needs a
// seed file, a secrets dir and a live tunnel cert; the property worth pinning is simply that the
// mint site passes the real constants and not, say, a hardcoded 0.
func TestJoinPrepareStampsTheDeclaredVersion(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "cmd", "tether", "cluster_join.go"))
	if err != nil {
		t.Fatalf("read cluster_join.go: %v", err)
	}
	text := string(src)
	for _, want := range []string{
		"ProtoVer: proto.ProtoVersion",
		"ReleaseVersion: proto.ReleaseVersion",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("`cluster join prepare` no longer stamps %q into the JoinBundle.\n"+
				"Without it every real bundle lacks mandatory compatibility evidence and is "+
				"rejected. The gate would be correct but current tooling could never satisfy it.",
				want)
		}
	}
	// Non-vacuity: the file must actually contain a bundle mint, else the checks above pass by
	// reading the wrong file.
	if !strings.Contains(text, "cluster.EncodeJoinBundle(cluster.JoinBundle{") {
		t.Fatal("cluster_join.go no longer mints a JoinBundle — this guard is reading a file that " +
			"no longer holds the mint site, so its assertions mean nothing")
	}
}

// TestJoinVersionGateRefusesBeforeAnyStateIsCreated is the behavioural assertion: on a real raft
// node, a skewed or version-undeclared bundle must be refused with NO operation row and NO roster
// row. The gate's whole value is that a rejected joiner leaves nothing behind to clean up.
func TestJoinVersionGateRefusesBeforeAnyStateIsCreated(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*cluster.JoinBundle)
	}{
		{"declared mismatch", func(jb *cluster.JoinBundle) {
			jb.ProtoVer = proto.ProtoVersion + 1
			jb.ReleaseVersion = "v9.9.9-other"
		}},
		{"missing declarations", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, _ := d7SingleNode(t, "br-1")
			admin := &ClusterAdmin{node: n, logger: silentLogger(), now: time.Now}
			bundle := signedBundle(t, "br-joiner", c.mutate)

			opID, err := admin.StartJoinOperation(bundle)
			if err == nil {
				t.Fatalf("incompatible bundle was ADMITTED (op %s)", opID)
			}
			var skew *ErrJoinVersionSkew
			if !errors.As(err, &skew) {
				t.Fatalf("refusal must be typed for a terminal CLI exit; got %T: %v", err, err)
			}
			if opID != "" {
				t.Errorf("refusal returned non-empty op id %q", opID)
			}

			var ops, nodes int
			if err := n.RODB().QueryRow(`SELECT COUNT(*) FROM cluster_operations`).Scan(&ops); err != nil {
				t.Fatalf("count operations: %v", err)
			}
			if ops != 0 {
				t.Errorf("refusal left %d operation row(s) behind", ops)
			}
			if err := n.RODB().QueryRow(
				`SELECT COUNT(*) FROM cluster_nodes WHERE node_id=?`, "br-joiner",
			).Scan(&nodes); err != nil {
				t.Fatalf("count cluster_nodes: %v", err)
			}
			if nodes != 0 {
				t.Errorf("refusal admitted %d roster row(s)", nodes)
			}
		})
	}
}

// TestJoinVersionGateAdmitsAMatchingJoiner is the other half: the gate must not have broken the
// happy path. Without this, a gate that refused EVERYTHING would pass every test above.
func TestJoinVersionGateAdmitsAMatchingJoiner(t *testing.T) {
	n, _ := d7SingleNode(t, "br-1")
	admin := &ClusterAdmin{node: n, logger: silentLogger(), now: time.Now}

	bundle := signedBundle(t, "br-joiner", func(jb *cluster.JoinBundle) {
		jb.ProtoVer = proto.ProtoVersion
		jb.ReleaseVersion = proto.ReleaseVersion
	})
	if _, err := admin.StartJoinOperation(bundle); err != nil {
		var skew *ErrJoinVersionSkew
		if errors.As(err, &skew) {
			t.Fatalf("a MATCHING joiner was refused by the version gate: %v — the gate is "+
				"refusing everything, which would make every assertion above vacuous", err)
		}
		// Any other error is a pre-existing admission concern, not this gate's business.
		t.Logf("admission failed for a non-version reason (acceptable here): %v", err)
	}
}
