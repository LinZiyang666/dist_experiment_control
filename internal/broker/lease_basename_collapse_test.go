package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: docs/reviews/cloned-credential-instances-external-review.md F2/F15
//
// A CONFIGURED NAME THAT ALREADY ENDS IN `-NN` IS ITS OWN CREDENTIAL FAMILY.
//
// docs/usage.md's own example fleet is `gpu-01 gpu-02 gpu-03`, so this is the
// ordinary case, not a corner. Folding it (the earlier proto.BasenameOf call)
// offered gpu-02's clone a lease of "gpu" — a name in a DIFFERENT family, which
// gpu-02's provisioning row does not cover. Both sides then agreed on a name
// auth would reject, and the agent's refuse arm rebuilt the session and
// re-registered forever: a healthy pod that never subscribes, never appears in
// `node ls`, at one full connect+auth_callout per RegisterRetryInitial.
//
// This file previously CHARACTERIZED that bug (it asserted the fold and logged
// the consequence, staying green). external review F15 is right that a green
// characterization of a defect is a trap for whoever fixes it, so it now pins
// the invariant instead.
//
// MUTATION: fold the presented name with proto.BasenameOf in assignLeaseName
// and this goes red.
func TestLeaseSuffixCollapsesAnAlreadySuffixedBasename(t *testing.T) {
	b := leaseBroker(t)
	for _, nid := range []string{"gpu-01", "gpu-02", "gpu-03"} {
		seedBeat(t, b, nid, 0)
		if _, err := b.cfg.DB.Exec(
			`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES (?,?,?,datetime('now'))`,
			"lab", nid, "SHA256:img"); err != nil {
			t.Fatalf("seed prov %s: %v", nid, err)
		}
	}
	// gpu-02's image is cloned; the clone presents the same configured nid AND
	// says what its configured root is, which is the whole point — the broker
	// must not have to guess it from the string.
	lease, code, err := adjudicated(t, b, "lab", "gpu-02",
		&proto.NodeRegisterReq{InstanceID: testInstanceB, ConfiguredNID: "gpu-02"})
	if lease == nil {
		t.Fatalf("expected a contested verdict; got nil (code=%q err=%v)", code, err)
	}
	if lease.Basename != "gpu-02" {
		t.Fatalf("the configured name was folded into another credential family: basename=%q "+
			"(assigned %q). gpu-02's provisioning row does not cover that family, so the offer is a "+
			"name the agent can never authenticate under.", lease.Basename, lease.AssignedNID)
	}
	base, _, leased := proto.SplitLeaseName(lease.AssignedNID)
	if !leased || base != "gpu-02" {
		t.Fatalf("assigned %q is not a direct lease of gpu-02; the agent refuses it and rebuilds "+
			"forever, so a healthy pod never reaches `node ls`", lease.AssignedNID)
	}
}

// A leased instance does NOT keep its name across a restart: its own row is
// still ONLINE for OfflineAfter (60s), so claimedLeaseNames counts the name it
// just vacated as taken and it is handed the NEXT suffix.
func TestReviewDemoRestartedLeasedInstanceGetsANewSuffixEveryTime(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1", 0)    // the basename holder, alive
	seedBeat(t, b, "gpu1-02", 0) // the ghost row of the clone that just restarted
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceC})
	if lease == nil {
		t.Fatalf("expected contested; code=%q err=%v", code, err)
	}
	if lease.AssignedNID == "gpu1-02" {
		t.Fatal("it kept its name — this demo is obsolete")
	}
	t.Logf("OPERATOR-VISIBLE RESULT: the instance that was gpu1-02 five seconds ago comes back as %q. "+
		"`tether exec gpu1-02 …` now hits a ghost row that stays ONLINE for up to 60s and then "+
		"STALE/OFFLINE forever, and every expose made against gpu1-02 is stranded.",
		lease.AssignedNID)
}

// Suffix exhaustion is answered with the SAME refuse shape as an unacceptable
// name, so it lands in the same uncapped rebuild loop.
func TestReviewDemoExhaustedSuffixSpaceYieldsAnEmptyAssignedNID(t *testing.T) {
	b := leaseBroker(t)
	b.cfg.MaxInstancesPerBasename = 3
	seedBeat(t, b, "gpu1", 0)
	seedBeat(t, b, "gpu1-02", 0)
	seedBeat(t, b, "gpu1-03", 0)
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceC})
	if lease == nil || lease.AssignedNID != "" {
		t.Fatalf("expected the refusal shape; got %+v code=%q err=%v", lease, code, err)
	}
	t.Logf("OPERATOR-VISIBLE RESULT: assigned=%q code=%q — assignLeaseName's comment says the agent "+
		"\"keeps running under the name it presented\", but applyLeaseVerdict's refuse arm rebuilds "+
		"the session instead, so the instance loops connect→register→refused forever.",
		lease.AssignedNID, code)
}

// origin: docs/reviews/cloned-credential-instances-external-review-tasklist.md B2
//
// A configured nid is an opaque operator identity, even when its final token
// looks like the lease grammar. Its clone family must stay rooted at that exact
// configured name. Besides preserving addressability, this is required by
// auth_callout: the permanent provisioning row is keyed by "gpu-02", so a
// lease of "gpu-02-02" can fall back to it while a lease of "gpu-03" cannot.
func TestConfiguredNumericSuffixRemainsTheLiteralLeaseBasename(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu-02", 0)
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES (?,?,?,datetime('now'))`,
		"lab", "gpu-02", "SHA256:img"); err != nil {
		t.Fatalf("seed provisioning: %v", err)
	}

	lease, code, err := adjudicated(t, b, "lab", "gpu-02", &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" || lease == nil {
		t.Fatalf("adjudicate numeric-suffix nid: lease=%+v code=%q err=%v", lease, code, err)
	}
	if lease.Basename != "gpu-02" || lease.AssignedNID != "gpu-02-02" {
		t.Fatalf("configured nid was collapsed into another credential family: lease=%+v", lease)
	}
}

// origin: docs/reviews/cloned-credential-instances-external-rereview.md R5
//
// ConfiguredNID is network input, not an authority. An agent authenticated for
// gpu1 may state gpu1 as its opaque root, but it may not redirect allocation
// into another credential family's suffix space. Without this relationship
// check, repeated contested requests can reserve victim-02, victim-03, ... in
// leaseHolder and temporarily exhaust the victim family without ever being
// able to authenticate as one of those names.
func TestRegisterCannotAllocateAnotherCredentialFamily(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu1", 0)
	if _, _, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{
		InstanceID: testInstanceA, ConfiguredNID: "gpu1",
	}); err != nil {
		t.Fatalf("incumbent: %v", err)
	}

	lease, _, _ := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{
		InstanceID: testInstanceB, ConfiguredNID: "victim",
	})
	if lease != nil && lease.Basename == "victim" {
		t.Fatalf("an agent presenting gpu1 redirected lease allocation into another credential family: %+v", lease)
	}
}

// A syntactic `<base>-NN` is not necessarily a lease: it may be a real,
// independently provisioned device. Such a device cannot use ConfiguredNID to
// enter the shorter base's credential family merely because its name parses as
// a suffix.
func TestProvisionedNumericSuffixCannotClaimTheShorterFamily(t *testing.T) {
	b := leaseBroker(t)
	seedBeat(t, b, "gpu-02", 0)
	for _, nid := range []string{"gpu", "gpu-02"} {
		if _, err := b.cfg.DB.Exec(
			`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES (?,?,?,datetime('now'))`,
			"lab", nid, "SHA256:"+nid); err != nil {
			t.Fatalf("seed provisioning %s: %v", nid, err)
		}
	}
	if _, _, err := adjudicated(t, b, "lab", "gpu-02", &proto.NodeRegisterReq{
		InstanceID: testInstanceA, ConfiguredNID: "gpu-02",
	}); err != nil {
		t.Fatalf("incumbent: %v", err)
	}

	lease, _, _ := adjudicated(t, b, "lab", "gpu-02", &proto.NodeRegisterReq{
		InstanceID: testInstanceB, ConfiguredNID: "gpu",
	})
	if lease != nil && lease.Basename == "gpu" {
		t.Fatalf("provisioned device gpu-02 crossed into gpu's credential family: %+v", lease)
	}
}

func TestPreviousNIDCannotCrossTheProvisionedCredentialFamily(t *testing.T) {
	b := leaseBroker(t)
	for _, nid := range []string{"gpu", "gpu-02"} {
		if _, err := b.cfg.DB.Exec(
			`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES (?,?,?,datetime('now'))`,
			"lab", nid, "SHA256:"+nid); err != nil {
			t.Fatalf("seed provisioning %s: %v", nid, err)
		}
	}
	req := &proto.NodeRegisterReq{ConfiguredNID: "gpu", PreviousNID: "gpu"}
	if got := trustedPreviousNID(b, "lab", "gpu-02", req); got != "" {
		t.Fatalf("independently provisioned gpu-02 was allowed to carry rows from %q", got)
	}

	if _, err := b.cfg.DB.Exec(`DELETE FROM agent_provisioning WHERE sid='lab' AND nid='gpu-02'`); err != nil {
		t.Fatal(err)
	}
	if got := trustedPreviousNID(b, "lab", "gpu-02", req); got != "gpu" {
		t.Fatalf("real lease adoption lost its valid previous name: got %q", got)
	}
	req.PreviousNID = "other-device"
	if got := trustedPreviousNID(b, "lab", "gpu-02", req); got != "" {
		t.Fatalf("lease adoption crossed to unrelated previous name %q", got)
	}
}
